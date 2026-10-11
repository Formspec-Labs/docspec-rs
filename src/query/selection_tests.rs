//! Query selection checks with independently specified source values.

use super::*;
use crate::{
    Engine,
    budget::Limits,
    model::{Id, RunSpec},
};
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::collect;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn budget() -> Budget {
    Budget::new(Limits {
        compute_threads: 2,
        batch_rows: 2,
        max_batch_bytes: 32 << 10,
        max_metadata_bytes: 256 << 10,
        memory_bytes: 2 << 20,
        ..Limits::default()
    })
    .unwrap()
}

async fn fixture() -> (TempDir, Engine, Table) {
    let pin_budget = budget();
    let root = TempDir::new().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 0,
            compute_threads: 2,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let spec = RunSpec {
        base: None,
        relationship: None,
        dataset: Id::new("selection").unwrap(),
        operation: Id::new("initial").unwrap(),
        input: input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&input).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    let release = engine.run(&spec).await.unwrap();
    let native = storage::open_pin(&release.dataset.groups["source"], &pin_budget)
        .await
        .unwrap();
    (root, engine, native)
}

fn values(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect()
}

#[tokio::test]
async fn direct_table_scan_preserves_repeats_empty_rows_and_statistics() {
    let (_root, _engine, native) = fixture().await;
    let budget = budget();
    let context = budget.context();
    let provider = table(native, budget, Some(3)).unwrap();
    let state = context.state();
    let indices = vec![2, 0, 2];
    let plan = provider
        .scan(&state, Some(&indices), &[], None)
        .await
        .unwrap();
    assert!(plan.as_any().is::<DataSourceExec>());
    assert_eq!(
        plan.schema().as_ref(),
        &provider.schema().project(&indices).unwrap()
    );
    let statistics = plan.partition_statistics(None).unwrap();
    assert_eq!(statistics.num_rows, Precision::Inexact(3));
    assert_eq!(statistics.column_statistics.len(), 3);
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(values(&result, 0), [Some(100), Some(200), Some(300)]);
    assert_eq!(values(&result, 1), [Some(1), None, Some(3)]);
    assert_eq!(values(&result, 2), [Some(100), Some(200), Some(300)]);

    let empty = Vec::new();
    let plan = provider
        .scan(&state, Some(&empty), &[], None)
        .await
        .unwrap();
    assert!(plan.schema().fields().is_empty());
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(result.iter().all(|batch| batch.num_columns() == 0));
    assert!(
        provider
            .scan(&state, Some(&vec![3]), &[], None)
            .await
            .is_err()
    );

    let plan = provider
        .scan(&state, Some(&indices), &[], Some(1))
        .await
        .unwrap();
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(values(&result, 0), [Some(100)]);
}

#[tokio::test]
async fn captured_provider_composes_indices_and_keeps_sql_behavior() {
    let (_root, _engine, native) = fixture().await;
    let budget = budget();
    let inventory = storage::snapshot_files(&native, &budget).await.unwrap();
    let scan = Arc::new(
        storage::OccurrenceScan::values(&native, &budget, inventory)
            .await
            .unwrap(),
    );
    let names = vec!["c".to_owned(), "a".to_owned()];
    let provider = files(scan, Some(&names), budget.clone()).unwrap();
    let context = budget.context();
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![1, 0, 1]), &[], None)
        .await
        .unwrap();
    assert_eq!(
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["a", "c", "a"]
    );
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(values(&result, 0), [Some(1), None, Some(3)]);
    assert_eq!(values(&result, 1), [Some(100), Some(200), Some(300)]);
    assert_eq!(values(&result, 2), [Some(1), None, Some(3)]);
    assert!(
        provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .is_err()
    );
    let plan = provider
        .scan(&state, Some(&vec![]), &[], None)
        .await
        .unwrap();
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(result.iter().all(|batch| batch.num_columns() == 0));

    context.register_table("retained", provider).unwrap();
    let result = context
        .sql("SELECT c AS renamed, a AS key FROM retained WHERE a IS NULL OR a = 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(result[0].schema().field(0).name(), "renamed");
    // SQL without ORDER BY may return partitions in either order. Preserve
    // column association while checking the exact values and nulls.
    let mut rows: Vec<_> = values(&result, 0)
        .into_iter()
        .zip(values(&result, 1))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, [(Some(200), None), (Some(300), Some(3))]);
    let result = context
        .sql("SELECT 9 AS constant FROM retained")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(values(&result, 0), [Some(9), Some(9), Some(9)]);
    let result = context
        .sql("SELECT COUNT(*) FROM retained")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(values(&result, 0), [Some(3)]);
}

#[tokio::test]
async fn captured_occurrence_selection_composes_synthetic_row_indices() {
    let (_root, _engine, native) = fixture().await;
    let budget = budget();
    let scan = Arc::new(
        storage::OccurrenceScan::open(&native, &budget)
            .await
            .unwrap(),
    );
    let names = vec!["__docspec_row".to_owned(), "c".to_owned()];
    let provider = files(scan, Some(&names), budget.clone()).unwrap();
    let context = budget.context();
    let plan = provider
        .scan(&context.state(), Some(&vec![1, 0, 1]), &[], None)
        .await
        .unwrap();
    let result = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(values(&result, 0), [Some(100), Some(200), Some(300)]);
    assert_eq!(values(&result, 1), [Some(0), Some(1), Some(2)]);
    assert_eq!(values(&result, 2), [Some(100), Some(200), Some(300)]);
}

#[tokio::test]
async fn projected_mapping_admission_is_owned_by_plan_and_stream() {
    let (_root, _engine, native) = fixture().await;
    let budget = budget();
    let provider = table(native, budget.clone(), Some(3)).unwrap();
    let context = budget.context();
    let plan = provider
        .scan(&context.state(), Some(&vec![2, 0, 2]), &[], None)
        .await
        .unwrap();
    assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    drop(plan);
    assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
    assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 2);
    drop(stream);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    let memory = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
    let error = provider
        .scan(&context.state(), Some(&vec![2, 0, 2]), &[], None)
        .await
        .unwrap_err();
    assert_eq!(crate::Error::from(error).code(), "capacity");
    drop(memory);
    let tiny = Budget::new(Limits {
        max_metadata_bytes: 1,
        ..budget.limits.clone()
    })
    .unwrap();
    let source = provider.as_any().downcast_ref::<NativeTable>().unwrap();
    let partition = NativePartition {
        source: source.0.source.clone(),
        schema: source.schema(),
        budget: tiny,
        rows: Some(3),
    };
    assert_eq!(
        partition.project(&[2, 0, 2]).unwrap_err().code(),
        "metadata_capacity"
    );
}

#[tokio::test]
async fn empty_projection_admits_cloned_schema_metadata_before_allocation() {
    let (_root, _engine, native) = fixture().await;
    let budget = budget();
    let provider = table(native, budget.clone(), Some(3)).unwrap();
    let source = provider.as_any().downcast_ref::<NativeTable>().unwrap();
    let metadata = std::collections::HashMap::from([("provenance".to_owned(), "x".repeat(8192))]);
    let schema = Arc::new(source.schema().as_ref().clone().with_metadata(metadata));
    let small = Budget::new(Limits {
        max_metadata_bytes: 4096,
        ..budget.limits.clone()
    })
    .unwrap();
    let repeated = NativePartition {
        source: source.0.source.clone(),
        schema: source.schema(),
        budget: small.clone(),
        rows: Some(3),
    };
    drop(repeated.project(&[0]).unwrap());
    // Repetition shares Field values but still needs result-schema handles.
    // The index vector alone fits this allowance; the whole selection does not.
    assert_eq!(
        repeated.project(&vec![0; 256]).unwrap_err().code(),
        "metadata_capacity"
    );
    let partition = NativePartition {
        source: source.0.source.clone(),
        schema: schema.clone(),
        budget: small.clone(),
        rows: Some(3),
    };
    assert_eq!(
        partition.project(&[]).unwrap_err().code(),
        "metadata_capacity"
    );
    drop(small.reserve(small.limits.memory_bytes / 2).unwrap());

    let admitted = NativePartition {
        budget: budget.clone(),
        ..partition
    }
    .project(&[])
    .unwrap();
    assert!(admitted.schema.fields().is_empty());
    assert_eq!(admitted.schema.metadata(), schema.metadata());
    assert!(
        budget
            .reserve(budget.limits.memory_bytes / 2 - 4096)
            .is_err()
    );
    drop(admitted);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}
