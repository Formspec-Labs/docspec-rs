//! Caller-scoped join inventory replay and admission lifetime checks.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

async fn fixture() -> (TempDir, Engine, ReleasePin, JoinTransform) {
    let root = TempDir::new().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            compute_threads: 2,
            memory_bytes: 512 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            base: None,
            relationship: None,
            dataset: Id::new("source").unwrap(),
            operation: Id::new("initial").unwrap(),
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let join = JoinTransform {
        left: release.version_ref(),
        right: release.version_ref(),
        join: JoinDefinition {
            keys: vec![JoinKey {
                left: "id".into(),
                right: "id".into(),
            }],
            kind: JoinKind::Inner,
            nulls: NullMatching::Equal,
            cardinality: JoinCardinality::OneToOne,
            fields: vec![JoinField {
                side: JoinSide::Left,
                field: "id".into(),
                name: "id".into(),
            }],
        },
    };
    (root, engine, release, join)
}

fn budget() -> Budget {
    Budget::new(Limits {
        compute_threads: 2,
        memory_bytes: 16 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 64 << 10,
        batch_rows: 2,
        ..Limits::default()
    })
    .unwrap()
}

#[tokio::test]
async fn cardinality_and_main_join_replay_without_manifest_and_share_self_join_admission() {
    let (_root, _engine, release, join) = fixture().await;
    let budget = budget();
    let context = budget.context();
    let releases = BTreeMap::from([(release.dataset.dataset.clone(), &release)]);
    register_join_inputs(&context, &join, &releases, &budget, true)
        .await
        .unwrap();
    // Exactly one captured inventory owns the existing double-metadata allowance.
    let remaining = budget.limits.memory_bytes / 2 - budget.limits.max_metadata_bytes * 2;
    drop(budget.reserve(remaining).unwrap());
    assert!(budget.reserve(remaining + 1).is_err());

    let native = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let manifest = native
        .metadata()
        .current_snapshot()
        .unwrap()
        .manifest_list();
    std::fs::remove_file(manifest).unwrap();
    assert!(storage::snapshot_files(&native, &budget).await.is_err());
    // Each iteration executes both cardinality probes and the main join.
    for _ in 0..2 {
        let batches = join_plan(&context, &join, true)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
            })
            .collect();
        values.sort();
        assert_eq!(values, [None, Some(1), Some(2)]);
        for batch in &batches {
            for name in TUPLE {
                assert_eq!(batch.column_by_name(name).unwrap().null_count(), 0);
            }
        }
    }
    context.deregister_table("join_left").unwrap();
    assert!(budget.reserve(remaining + 1).is_err());
    context.deregister_table("join_right").unwrap();
    drop(context);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}

#[tokio::test]
async fn one_shot_join_inputs_keep_inventory_lazy() {
    let (_root, _engine, release, mut join) = fixture().await;
    let releases = BTreeMap::from([(release.dataset.dataset.clone(), &release)]);
    for (cardinality, enforce) in [
        (JoinCardinality::OneToOne, false),
        (JoinCardinality::ManyToMany, true),
    ] {
        join.join.cardinality = cardinality;
        let budget = budget();
        let context = budget.context();
        register_join_inputs(&context, &join, &releases, &budget, enforce)
            .await
            .unwrap();
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }
}
