//! Independent physical row correspondence and scan refusal qualification.

use super::*;
use crate::{budget::Limits, storage};
use arrow::array::Array;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{collections::HashMap, fs::File};
use tempfile::TempDir;

type Rows = BTreeMap<(String, i64), Option<i64>>;

async fn fixture() -> (TempDir, Arc<dyn Catalog>, Table, Table) {
    let root = TempDir::new().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let namespace = NamespaceIdent::new("occurrences".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let native_schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let empty = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("source".into())
                .schema(native_schema)
                .location(root.path().join("source").to_string_lossy().into_owned())
                .properties(storage::table_properties())
                .build(),
        )
        .await
        .unwrap();
    let budget = Budget::new(Limits {
        batch_rows: 2,
        ..Limits::default()
    })
    .unwrap();
    let mut table = empty.clone();
    for values in [
        vec![Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)],
        vec![Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)],
    ] {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let files = storage::write_batches(&table, futures::stream::iter([Ok(batch)]), &budget)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        table = storage::append(&table, catalog.as_ref(), files)
            .await
            .unwrap();
    }
    (root, catalog, table, empty)
}

fn budget(batch_rows: usize) -> Budget {
    Budget::new(Limits {
        batch_rows,
        max_batch_bytes: 64 << 10,
        max_metadata_bytes: 1 << 20,
        memory_bytes: ((2 << 20) + (128 << 10)) * 2,
        ..Limits::default()
    })
    .unwrap()
}

fn insert(batch: &RecordBatch, rows: &mut Rows) {
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let paths = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let positions = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(paths.null_count(), 0);
    assert_eq!(positions.null_count(), 0);
    for index in 0..batch.num_rows() {
        assert!(
            rows.insert(
                (paths.value(index).to_owned(), positions.value(index)),
                (!values.is_null(index)).then(|| values.value(index))
            )
            .is_none()
        );
    }
}

#[tokio::test]
async fn locators_match_independent_parquet_order_across_files_row_groups_and_batch_sizes() {
    let (_root, _catalog, table, empty) = fixture().await;
    let inventory = storage::snapshot_files(&table, &Budget::new(Limits::default()).unwrap())
        .await
        .unwrap();
    let mut expected = Rows::new();
    for file in &inventory.data {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path()).unwrap())
                .unwrap();
        assert!(builder.metadata().num_row_groups() > 1);
        let mut position = 0;
        for batch in builder.with_batch_size(3).build().unwrap() {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for value in values.iter() {
                expected.insert((file.file_path().to_owned(), position), value);
                position += 1;
            }
        }
    }
    assert_eq!(expected.len(), 14);
    assert_ne!(inventory.data[0].file_path(), inventory.data[1].file_path());
    assert_eq!(
        storage::digest(std::path::Path::new(inventory.data[0].file_path())).unwrap(),
        storage::digest(std::path::Path::new(inventory.data[1].file_path())).unwrap()
    );
    for size in [1, 2, 5, 100] {
        let stream = read_occurrences(&table, &budget(size)).await.unwrap();
        futures::pin_mut!(stream);
        let mut observed = Rows::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() <= size);
            insert(&batch, &mut observed);
        }
        assert_eq!(observed, expected);
    }
    assert_eq!(occurrence_schema(&empty).unwrap().fields().len(), 3);
    assert!(
        read_occurrences(&empty, &budget(2))
            .await
            .unwrap()
            .boxed()
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn reservations_survive_partial_reads_and_release_on_drop_eof_and_error() {
    let (_root, _catalog, table, _) = fixture().await;
    let budget = budget(2);
    let mut stream = read_occurrences(&table, &budget).await.unwrap().boxed();
    stream.try_next().await.unwrap().unwrap();
    assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
    assert_eq!(
        read_occurrences(&table, &budget)
            .await
            .err()
            .unwrap()
            .code(),
        "capacity"
    );
    drop(stream);
    let all = budget.limits.memory_bytes / 2;
    drop(budget.reserve(all).unwrap());
    let mut stream = read_occurrences(&table, &budget).await.unwrap().boxed();
    while stream.try_next().await.unwrap().is_some() {}
    drop(budget.reserve(all).unwrap());
    let mut tight = budget.clone();
    tight.limits.max_batch_bytes = 1;
    let mut stream = read_occurrences(&table, &tight).await.unwrap().boxed();
    assert_eq!(
        stream.try_next().await.unwrap_err().code(),
        "oversized_batch"
    );
    drop(budget.reserve(all).unwrap());
}

#[tokio::test]
async fn locator_columns_participate_in_batch_admission() {
    let (_root, _catalog, table, _) = fixture().await;
    let mut budget = budget(2);
    let mut plain = storage::read_batches(&table, &budget)
        .await
        .unwrap()
        .boxed();
    let mut largest = 0;
    while let Some(batch) = plain.try_next().await.unwrap() {
        largest = largest.max(batch.get_array_memory_size());
    }
    budget.limits.max_batch_bytes = largest;
    let mut plain = storage::read_batches(&table, &budget)
        .await
        .unwrap()
        .boxed();
    while plain.try_next().await.unwrap().is_some() {}
    let mut located = read_occurrences(&table, &budget).await.unwrap().boxed();
    assert_eq!(
        located.try_next().await.unwrap_err().code(),
        "oversized_batch"
    );
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}

#[tokio::test]
async fn selective_scan_skips_unrequested_payloads_and_keeps_stream_admission() {
    let (_root, _catalog, table, _) = fixture().await;
    let budget = budget(2);
    let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
    let paths: Vec<_> = scan
        .files()
        .map(|file| file.file_path().to_owned())
        .collect();
    std::fs::remove_file(&paths[0]).unwrap();
    let mut stream = scan.read_file(&paths[1], &budget).unwrap().boxed();
    drop(scan);
    assert!(stream.try_next().await.unwrap().is_some());
    assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
    let mut rows = 2;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 7);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}

#[tokio::test]
async fn malformed_incomplete_and_repeated_native_tasks_cannot_establish_coverage() {
    let (_root, _catalog, table, _) = fixture().await;
    let tasks: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let task = tasks[0].clone();
    let state = Occurrences::open(&table, &budget(2)).await.unwrap();
    let file = &state.expected[&task.data_file_path];
    for change in 0..8 {
        let mut invalid = task.clone();
        match change {
            0 => invalid.start = 1,
            1 => invalid.length -= 1,
            2 => invalid.record_count = None,
            3 => invalid.file_size_in_bytes += 1,
            4 => invalid.record_count = Some(file.record_count() + 1),
            5 => invalid.predicate = Some(iceberg::expr::BoundPredicate::AlwaysTrue),
            6 => invalid.data_file_format = DataFileFormat::Avro,
            _ => invalid.deletes.push(iceberg::scan::FileScanTaskDeleteFile {
                file_path: "delete.parquet".into(),
                file_size_in_bytes: 10,
                file_type: iceberg::spec::DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }),
        }
        assert_eq!(
            check_task(&invalid, file).unwrap_err().code(),
            "unsupported_occurrence_scan"
        );
    }
    for replacement in [Vec::new(), vec![task.clone(), task.clone()]] {
        let mut state = Occurrences::open(&table, &budget(2)).await.unwrap();
        state.tasks = futures::stream::iter(replacement.into_iter().map(Ok)).boxed();
        loop {
            match state.next().await {
                Err(error) => {
                    assert_eq!(error.code(), "occurrence_tasks");
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("incomplete task set incorrectly accepted"),
            }
        }
    }
    for offset in [-1_i64, 1] {
        let mut state = Occurrences::open(&table, &budget(2)).await.unwrap();
        state.next().await.unwrap().unwrap();
        let file = state.current.as_mut().unwrap();
        file.rows = (file.rows as i64 + offset) as u64;
        loop {
            match state.next().await {
                Err(error) => {
                    assert_eq!(error.code(), "occurrence_count");
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("incorrect file count accepted"),
            }
        }
    }
}

#[tokio::test]
async fn reserved_names_and_unqualified_schema_changes_refuse() {
    let (_root, _catalog, table, empty) = fixture().await;
    for (base, name, code) in [
        (&table, "renamed", "unsupported_schema_evolution"),
        (&empty, "__docspec_file", "reserved_field"),
        (&empty, "__DOCSPEC_ROW", "reserved_field"),
    ] {
        let schema = Schema::new(vec![Field::new(name, DataType::Int64, true)]);
        let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let changed = iceberg::table::StaticTable::from_metadata(
            metadata,
            base.identifier().clone(),
            base.file_io().clone(),
        )
        .await
        .unwrap()
        .into_table();
        assert_eq!(occurrence_schema(&changed).unwrap_err().code(), code);
    }
}
