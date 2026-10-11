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
async fn observed_reader_error_keeps_attempts_sampleable_without_new_ranges() {
    let (_root, _catalog, table, _) = fixture().await;
    let mut budget = budget(2);
    budget.limits.max_batch_bytes = 1;
    let (stream, observation) = storage::read_occurrences_observed(&table, &budget, None)
        .await
        .unwrap();
    assert_eq!(observation.sample(), storage::NativeScanSample::default());
    let mut stream = stream.boxed();
    // This refusal occurs after a real native read, when decoded batch admission
    // fails. No injected error or timed cancellation is needed.
    assert_eq!(
        stream.try_next().await.unwrap_err().code(),
        "oversized_batch"
    );
    let failed = observation.sample();
    assert!(failed.requested_range_bytes > 0);
    assert_eq!(failed.scans_started, 1);
    drop(stream);
    let after_drop = observation.sample();
    assert!(after_drop.requested_range_bytes >= failed.requested_range_bytes);
    assert_eq!(after_drop.scans_started, failed.scans_started);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());

    // Retained counters work even after the temporary payload files disappear.
    // Repeated synchronous samples neither start a scan nor request more ranges.
    let inventory = storage::snapshot_files(&table, &budget).await.unwrap();
    for file in &inventory.data {
        std::fs::remove_file(file.file_path()).unwrap();
    }
    let retained = observation.sample();
    assert!(retained.requested_range_bytes >= after_drop.requested_range_bytes);
    assert_eq!(retained.scans_started, 1);
    assert_eq!(observation.clone().sample(), retained);
    assert_eq!(observation.sample(), retained);
}

#[tokio::test]
async fn locator_columns_participate_in_batch_admission() {
    let (_root, _catalog, table, _) = fixture().await;
    let mut budget = budget(2);
    let tasks = storage::schema::plan_files(&table).await.unwrap();
    let mut plain = table
        .reader_builder()
        .with_batch_size(2)
        .with_data_file_concurrency_limit(1)
        .build()
        .read(tasks)
        .unwrap()
        .stream();
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
    let tasks: Vec<_> = storage::schema::plan_files(&table)
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
    let (_root, _catalog, _table, empty) = fixture().await;
    for (base, name, code) in [
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

mod defaults;
mod nested_selection;
mod schema;
mod selection;

#[tokio::test]
async fn observed_projection_requests_fewer_ranges_with_exact_values_and_occurrences() {
    // Files exceed Iceberg's 512 KiB footer prefetch, and distinct hashed text
    // avoids a tiny dictionary hiding the physical cost of the wide column.
    let root = TempDir::new().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let namespace = NamespaceIdent::new("scan_observation".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, false),
    ]));
    let native = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let mut table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("wide".into())
                .schema(native)
                .location(root.path().join("wide").to_string_lossy().into_owned())
                .properties(storage::table_properties())
                .build(),
        )
        .await
        .unwrap();
    let budget = Budget::new(Limits {
        batch_rows: 32,
        ..Limits::default()
    })
    .unwrap();
    let mut expected_values = BTreeMap::new();
    for file in 0..2 {
        let mut ids = Vec::new();
        let mut texts = Vec::new();
        for row in 0..256 {
            let id = (row % 5 != 0).then_some((row % 7) as i64);
            let mut text = String::new();
            for chunk in 0..256 {
                text.push_str(&blake3::hash(format!("{file}:{row}:{chunk}").as_bytes()).to_hex());
            }
            expected_values.insert(text.clone(), id);
            ids.push(id);
            texts.push(text);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
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
    let full_schema = super::super::schema::read_schema(&table).unwrap();
    let (full, full_observation) = storage::read_batches_observed(&table, &budget, None)
        .await
        .unwrap();
    let mut full = full.boxed();
    let mut actual_values = BTreeMap::new();
    while let Some(batch) = full.try_next().await.unwrap() {
        assert_eq!(batch.schema(), full_schema);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (text, id) in texts.iter().zip(ids.iter()) {
            assert!(actual_values.insert(text.unwrap().to_owned(), id).is_none());
        }
    }
    assert_eq!(actual_values, expected_values);
    let expected_ids = expected_values.values().copied().collect::<Vec<_>>();
    let (narrow, narrow_observation) = storage::read_batches_observed(&table, &budget, Some(&[0]))
        .await
        .unwrap();
    let mut narrow = narrow.boxed();
    let mut actual_ids = Vec::new();
    while let Some(batch) = narrow.try_next().await.unwrap() {
        assert_eq!(batch.schema(), Arc::new(full_schema.project(&[0]).unwrap()));
        actual_ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    let mut expected_ids = expected_ids;
    expected_ids.sort();
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(full_observation.sample().scans_started, 1);
    assert_eq!(narrow_observation.sample().scans_started, 1);
    assert!(
        narrow_observation.sample().requested_range_bytes
            < full_observation.sample().requested_range_bytes
    );

    // Expected physical locations come from a separate synchronous Parquet reader.
    let inventory = storage::snapshot_files(&table, &budget).await.unwrap();
    let mut expected = Rows::new();
    for file in &inventory.data {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path()).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let mut position = 0;
        for batch in reader {
            let batch = batch.unwrap();
            for id in batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
            {
                expected.insert((file.file_path().to_owned(), position), id);
                position += 1;
            }
        }
    }
    let occurrence_schema = occurrence_schema(&table).unwrap();
    let (full, occurrence_full) = storage::read_occurrences_observed(&table, &budget, None)
        .await
        .unwrap();
    let mut full = full.boxed();
    let mut full_rows = Rows::new();
    while let Some(batch) = full.try_next().await.unwrap() {
        assert_eq!(batch.schema(), occurrence_schema);
        insert(&batch.project(&[0, 2, 3]).unwrap(), &mut full_rows);
    }
    let (narrow, occurrence_narrow) =
        storage::read_occurrences_observed(&table, &budget, Some(&[0, 2, 3]))
            .await
            .unwrap();
    let mut narrow = narrow.boxed();
    let mut narrow_rows = Rows::new();
    while let Some(batch) = narrow.try_next().await.unwrap() {
        assert_eq!(
            batch.schema(),
            Arc::new(occurrence_schema.project(&[0, 2, 3]).unwrap())
        );
        insert(&batch, &mut narrow_rows);
    }
    assert_eq!(full_rows, expected);
    assert_eq!(narrow_rows, expected);
    assert_eq!(
        occurrence_full.sample().scans_started,
        inventory.data.len() as u64
    );
    assert_eq!(
        occurrence_narrow.sample().scans_started,
        inventory.data.len() as u64
    );
    assert!(
        occurrence_narrow.sample().requested_range_bytes
            < occurrence_full.sample().requested_range_bytes
    );

    let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
    for file in scan.files() {
        let (full, projected, scalar_full) = scan
            .read_scalar_file_observed(file.file_path(), &budget, &[0, 1, 2, 3])
            .unwrap();
        assert!(projected);
        let mut full = full.boxed();
        let mut full_rows = Rows::new();
        while let Some(batch) = full.try_next().await.unwrap() {
            assert_eq!(batch.schema(), occurrence_schema);
            insert(&batch.project(&[0, 2, 3]).unwrap(), &mut full_rows);
        }
        let (narrow, projected, scalar_narrow) = scan
            .read_scalar_file_observed(file.file_path(), &budget, &[0, 2, 3])
            .unwrap();
        assert!(projected);
        let mut narrow = narrow.boxed();
        let mut narrow_rows = Rows::new();
        while let Some(batch) = narrow.try_next().await.unwrap() {
            assert_eq!(
                batch.schema(),
                Arc::new(occurrence_schema.project(&[0, 2, 3]).unwrap())
            );
            insert(&batch, &mut narrow_rows);
        }
        let expected_file = expected
            .iter()
            .filter(|((path, _), _)| path == file.file_path())
            .map(|(key, value)| (key.clone(), *value))
            .collect::<Rows>();
        assert_eq!(full_rows, expected_file);
        assert_eq!(narrow_rows, expected_file);
        assert_eq!(scalar_full.sample().scans_started, 1);
        assert_eq!(scalar_narrow.sample().scans_started, 1);
        assert!(
            scalar_narrow.sample().requested_range_bytes
                < scalar_full.sample().requested_range_bytes
        );
        eprintln!(
            "scalar requested ranges: full={:?} narrow={:?}",
            scalar_full.sample(),
            scalar_narrow.sample()
        );
    }
    eprintln!(
        "native requested ranges: full={:?} narrow={:?}; occurrences full={:?} narrow={:?}",
        full_observation.sample(),
        narrow_observation.sample(),
        occurrence_full.sample(),
        occurrence_narrow.sample()
    );

    // A dropped stream exposes an observation, not a canceled-I/O completion.
    let (partial, observation) = storage::read_occurrences_observed(&table, &budget, Some(&[0]))
        .await
        .unwrap();
    assert_eq!(observation.sample(), storage::NativeScanSample::default());
    let mut partial = partial.boxed();
    assert!(partial.try_next().await.unwrap().is_some());
    let before_drop = observation.sample();
    assert!(before_drop.requested_range_bytes > 0);
    drop(partial);
    assert!(observation.sample().requested_range_bytes >= before_drop.requested_range_bytes);
    assert_eq!(observation.sample().scans_started, 1);
}
