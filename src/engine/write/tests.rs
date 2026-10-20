//! Native metadata and applied-row oracles for schema-preserving replacements.

use super::*;
use arrow::array::{Array, Int64Array, StringArray, StructArray};
use iceberg::spec::{
    NestedField, PrimitiveType, SnapshotReference, SnapshotRetention, StructType, Type,
};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use tempfile::TempDir;

type Row = (Option<i64>, Option<String>, Option<String>, Option<i64>);

fn field(id: i32, name: &str, kind: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, Type::Primitive(kind)))
}

fn source_schema() -> Schema {
    Schema::builder()
        .with_fields([
            field(10, "old key", PrimitiveType::Long),
            field(30, "text", PrimitiveType::String),
            Arc::new(NestedField::optional(
                40,
                "details",
                Type::Struct(StructType::new(vec![field(
                    45,
                    "number",
                    PrimitiveType::Long,
                )])),
            )),
        ])
        .build()
        .unwrap()
}

#[tokio::test]
async fn captured_source_requires_logical_identity_count_and_public_schema() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            compute_threads: 2,
            memory_bytes: 64 << 20,
            scratch_bytes: 0,
            max_metadata_bytes: 1 << 20,
            max_batch_bytes: 64 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let owner = Id::new("capture").unwrap();
    let native = engine
        .create_output(&owner, &root.path().join("canonical"), source_schema())
        .await
        .unwrap();
    let canonical = storage::pin(&owner, &native, 0, &engine.inner.lifecycle.budget, &[])
        .await
        .unwrap();
    let with_origins = engine
        .source_with_origins(&owner, &canonical, &root.path().join("origins"))
        .await
        .unwrap();
    let accepted = open_captured_source(&canonical, &with_origins, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    assert!(storage::identity::has_origins(accepted.metadata().current_schema()).unwrap());

    for change_count in [false, true] {
        let mut invalid = with_origins.clone();
        if change_count {
            invalid.rows += 1;
        } else {
            invalid.logical_metadata.as_mut().unwrap().digest = "different-logical-source".into();
        }
        assert_eq!(
            open_captured_source(&canonical, &invalid, &engine.inner.lifecycle.budget)
                .await
                .unwrap_err()
                .code(),
            "invalid_admission_dependencies"
        );
    }
    // Matching logical metadata alone cannot bless another schema as equivalent.
    let foreign_schema = Schema::builder()
        .with_fields([field(10, "old key", PrimitiveType::Int)])
        .build()
        .unwrap();
    let foreign = engine
        .create_output(&owner, &root.path().join("foreign"), foreign_schema)
        .await
        .unwrap();
    let mut foreign = storage::pin(&owner, &foreign, 0, &engine.inner.lifecycle.budget, &[])
        .await
        .unwrap();
    foreign.logical_metadata = Some(canonical.logical_metadata().clone());
    assert_eq!(
        open_captured_source(&canonical, &foreign, &engine.inner.lifecycle.budget)
            .await
            .unwrap_err()
            .code(),
        "invalid_admission_dependencies"
    );
}

fn batch(schema: &Schema, rows: &[Row], evolved: bool) -> RecordBatch {
    let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema).unwrap());
    let keys = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
    ));
    let text = Arc::new(StringArray::from(
        rows.iter().map(|r| r.1.as_deref()).collect::<Vec<_>>(),
    ));
    let optional = Arc::new(StringArray::from(
        rows.iter().map(|r| r.2.as_deref()).collect::<Vec<_>>(),
    ));
    let details_index = if evolved { 3 } else { 2 };
    let arrow::datatypes::DataType::Struct(fields) = arrow.field(details_index).data_type() else {
        panic!("struct");
    };
    let details = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(Int64Array::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        ))],
        None,
    ));
    RecordBatch::try_new(
        arrow,
        if evolved {
            vec![text, keys, optional, details]
        } else {
            vec![keys, text, details]
        },
    )
    .unwrap()
}

async fn rows(pin: &TablePin, budget: &Budget) -> Vec<Row> {
    let native = storage::open_pin(pin, budget).await.unwrap();
    let mut stream = storage::read_batches(&native, budget)
        .await
        .unwrap()
        .boxed();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let optional = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let details = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let number = details
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                (!keys.is_null(row)).then(|| keys.value(row)),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
                (!optional.is_null(row)).then(|| optional.value(row).to_owned()),
                (!number.is_null(row)).then(|| number.value(row)),
            ));
        }
    }
    rows.sort();
    rows
}

async fn fixture() -> (TempDir, Engine, Id, Table, TablePin, Vec<Row>, String) {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            batch_rows: 2,
            max_batch_bytes: 64 << 10,
            max_metadata_bytes: 1 << 20,
            memory_bytes: 64 << 20,
            scratch_bytes: 0,
            compute_threads: 2,
        },
    )
    .await
    .unwrap();
    let owner = Id::new("schema-write").unwrap();
    let mut table = engine
        .create_output(&owner, &root.path().join("source"), source_schema())
        .await
        .unwrap();
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_name("details.number")
            .unwrap()
            .id,
        45
    );
    let all: Vec<Row> = vec![
        (Some(1), Some("α".into()), None, Some(11)),
        (Some(1), None, None, None),
        (Some(2), Some(String::new()), None, Some(22)),
        (None, Some("null key".into()), None, Some(0)),
        (Some(7), Some("keep7".into()), None, Some(77)),
        (Some(8), Some("keep8".into()), None, Some(88)),
    ];
    let budget = &engine.inner.lifecycle.budget;
    let mut stable = String::new();
    for values in [&all[..4], &all[4..]] {
        let batch = batch(table.metadata().current_schema(), values, false);
        let files = storage::write_batches(&table, futures::stream::iter([Ok(batch)]), budget)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        stable = files[0].file_path().to_owned();
        table = storage::append(&table, engine.inner.lifecycle.catalog.as_ref(), files)
            .await
            .unwrap();
    }
    let original = storage::pin(&owner, &table, all.len() as u64, budget, &[])
        .await
        .unwrap();
    let original_schema = table.metadata().current_schema().clone();
    // Allocate then remove an ID beyond every retained schema's maximum.
    let high = Schema::builder()
        .with_fields([field(900, "retired", PrimitiveType::Long)])
        .build()
        .unwrap();
    let high = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_current_schema(high)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let high_id = high.current_schema_id();
    let fields = original_schema.as_struct().fields();
    let mut renamed = fields[0].as_ref().clone();
    renamed.name = "key".into();
    let evolved = Schema::builder()
        .with_fields([
            fields[1].clone(),
            Arc::new(renamed),
            field(60, "optional", PrimitiveType::String),
            fields[2].clone(),
        ])
        .build()
        .unwrap();
    let metadata = high
        .into_builder(None)
        .set_current_schema(original_schema.schema_id())
        .unwrap()
        .remove_schemas(&[high_id])
        .unwrap()
        .add_current_schema(evolved)
        .unwrap()
        .set_properties(std::collections::HashMap::from([
            (
                "write.data.path".into(),
                root.path()
                    .join("wrong-data")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "write.folder-storage.path".into(),
                root.path()
                    .join("wrong-folder")
                    .to_string_lossy()
                    .into_owned(),
            ),
        ]))
        .unwrap()
        .set_ref(
            "audit",
            SnapshotReference::new(
                table.metadata().current_snapshot_id().unwrap(),
                SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            ),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert_eq!(metadata.last_column_id(), 900);
    assert!(metadata.schemas_iter().all(|s| s.highest_field_id() < 900));
    let identifier =
        iceberg::TableIdent::new(table.identifier().namespace().clone(), "evolved".into());
    let table = storage::register_candidate(
        engine.inner.lifecycle.catalog.as_ref(),
        &identifier,
        storage::CandidateWriter::Local,
        move || Ok(metadata),
        budget,
    )
    .await
    .unwrap();
    let pin = storage::pin(&owner, &table, all.len() as u64, budget, &original.objects)
        .await
        .unwrap();
    (root, engine, owner, table, pin, all, stable)
}

#[tokio::test]
async fn renamed_reordered_replacements_preserve_ids_highwater_rows_and_shared_files() {
    let (root, engine, owner, table, before, original, stable) = fixture().await;
    let budget = &engine.inner.lifecycle.budget;
    let old_objects = before.objects.clone();
    let stable_before = storage::digest(Path::new(&stable)).unwrap();
    let stable_time = std::fs::metadata(&stable).unwrap().modified().unwrap();
    let replacement_rows: Vec<Row> = vec![
        (Some(1), Some("new1".into()), Some(String::new()), Some(101)),
        (Some(1), None, Some("present".into()), None),
        (Some(3), Some("new3".into()), None, Some(303)),
    ];
    let context = engine.context();
    let replacement = context
        .read_batch(batch(
            table.metadata().current_schema(),
            &replacement_rows,
            true,
        ))
        .unwrap();
    let scope = context
        .sql("SELECT 1::BIGINT AS key UNION ALL SELECT 3::BIGINT UNION ALL SELECT NULL::BIGINT")
        .await
        .unwrap();
    let directory = root.path().join("replacement");
    let registered_before = engine
        .inner
        .lifecycle
        .catalog
        .list_tables(table.identifier().namespace())
        .await
        .unwrap()
        .len();
    let result = engine
        .replace_keys(Replacement {
            dataset: &owner,
            directory: locations::Location::Local(directory.clone()),
            before: &before,
            keys: &["key".into()],
            scope,
            replacement,
            replacement_bytes: None,
        })
        .await
        .unwrap();
    assert!(!directory.join("keys/metadata").exists());
    assert_eq!(
        engine
            .inner
            .lifecycle
            .catalog
            .list_tables(table.identifier().namespace())
            .await
            .unwrap()
            .len(),
        registered_before + 3,
        "only result, removed and added tables need retained native metadata"
    );
    let mut expected = original
        .iter()
        .filter(|r| !matches!(r.0, None | Some(1) | Some(3)))
        .cloned()
        .collect::<Vec<_>>();
    expected.extend(replacement_rows.clone());
    expected.sort();
    assert_eq!(rows(&result.table, budget).await, expected);
    let mut removed = original
        .iter()
        .filter(|r| matches!(r.0, None | Some(1)))
        .cloned()
        .collect::<Vec<_>>();
    removed.sort();
    assert_eq!(rows(&result.removed, budget).await, removed);
    let mut added = replacement_rows;
    added.sort();
    assert_eq!(rows(&result.added, budget).await, added);
    assert_eq!(result.metrics.files_reused, 1);
    assert_eq!(result.metrics.rows_removed, 3);
    assert_eq!(storage::digest(Path::new(&stable)).unwrap(), stable_before);
    assert_eq!(
        std::fs::metadata(&stable).unwrap().modified().unwrap(),
        stable_time
    );
    assert!(result.table.objects.iter().any(|o| o.path == stable));
    for pin in [&result.table, &result.removed, &result.added] {
        let native = storage::open_pin(pin, budget).await.unwrap();
        assert_eq!(
            native.metadata().current_schema(),
            table.metadata().current_schema()
        );
        assert_eq!(native.metadata().last_column_id(), 900);
        assert_eq!(
            native
                .metadata()
                .current_schema()
                .field_by_name("details.number")
                .unwrap()
                .id,
            45
        );
        assert!(
            native
                .metadata()
                .properties()
                .get("write.data.path")
                .is_none()
        );
        assert!(
            native
                .metadata()
                .properties()
                .get("write.folder-storage.path")
                .is_none()
        );
        for object in &pin.objects {
            assert!(
                Path::new(&object.path).starts_with(&directory) || old_objects.contains(object)
            );
        }
        crate::engine::verify_table(pin, budget).await.unwrap();
    }
    storage::verify_objects(old_objects, budget).await.unwrap();
    let candidate = engine
        .inner
        .lifecycle
        .catalog
        .load_table(&iceberg::TableIdent::new(
            table.identifier().namespace().clone(),
            result.table.table.clone(),
        ))
        .await
        .unwrap();
    let tx = Transaction::new(&candidate);
    let updated = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "next",
            Type::Primitive(PrimitiveType::Long),
        ))
        .apply(tx)
        .unwrap()
        .commit(engine.inner.lifecycle.catalog.as_ref())
        .await
        .unwrap();
    assert_eq!(
        updated
            .metadata()
            .current_schema()
            .field_by_name("next")
            .unwrap()
            .id,
        901
    );
    assert!(!root.path().join("wrong-data").exists());
    assert!(!root.path().join("wrong-folder").exists());
}

#[tokio::test]
async fn empty_candidate_clears_snapshot_state_but_preserves_native_schema_history() {
    let (root, engine, owner, source, _, _, _) = fixture().await;
    let snapshot = source.metadata().current_snapshot_id().unwrap();
    let metadata = source
        .metadata()
        .clone()
        .into_builder(None)
        .set_statistics(iceberg::spec::StatisticsFile {
            snapshot_id: snapshot,
            statistics_path: "unused.puffin".into(),
            file_size_in_bytes: 0,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: vec![],
        })
        .set_partition_statistics(iceberg::spec::PartitionStatisticsFile {
            snapshot_id: snapshot,
            statistics_path: "unused.parquet".into(),
            file_size_in_bytes: 0,
        })
        .build()
        .unwrap()
        .metadata;
    let native = iceberg::table::StaticTable::from_metadata(
        metadata,
        source.identifier().clone(),
        source.file_io().clone(),
    )
    .await
    .unwrap()
    .into_table();
    let candidate = engine
        .replacement_output(&owner, &root.path().join("empty"), &native)
        .await
        .unwrap();
    assert_ne!(candidate.metadata().uuid(), native.metadata().uuid());
    assert_eq!(
        candidate.metadata().current_schema(),
        native.metadata().current_schema()
    );
    assert_eq!(candidate.metadata().last_column_id(), 900);
    assert_eq!(candidate.metadata().current_snapshot_id(), None);
    assert_eq!(candidate.metadata().snapshots().count(), 0);
    assert!(candidate.metadata().history().is_empty());
    assert!(candidate.metadata().snapshot_for_ref("audit").is_none());
    assert_eq!(candidate.metadata().statistics_iter().count(), 0);
    assert_eq!(candidate.metadata().partition_statistics_iter().count(), 0);
    assert_eq!(
        candidate.metadata().metadata_log(),
        native.metadata().metadata_log()
    );
    assert!(!candidate.metadata().metadata_log().is_empty());
    assert_eq!(candidate.metadata().properties()["gc.enabled"], "false");
    assert_eq!(
        candidate.metadata().properties()["write.metadata.delete-after-commit.enabled"],
        "false"
    );
    assert_eq!(native.metadata().current_snapshot_id(), Some(snapshot));
    assert_eq!(native.metadata().statistics_iter().count(), 1);
}

#[tokio::test]
async fn value_only_key_probes_skip_wide_fields_and_keep_reader_admission() {
    use arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, Schema as ArrowSchema},
    };
    use std::io::{Seek, SeekFrom, Write};
    let root = TempDir::new().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            batch_rows: 1,
            max_batch_bytes: 64 << 10,
            max_metadata_bytes: 1 << 20,
            memory_bytes: 64 << 20,
            scratch_bytes: 0,
            compute_threads: 2,
        },
    )
    .await
    .unwrap();
    let owner = Id::new("wide-probe").unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Int64, true),
        // Ordinary values are allowed to use names reserved by occurrence views.
        Field::new(storage::OCCURRENCE_FILE, DataType::Utf8, true),
    ]));
    let table = engine
        .create_output(
            &owner,
            &root.path().join("source"),
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&schema).unwrap(),
        )
        .await
        .unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec![
            "x".repeat(16_000),
            "y".repeat(16_000),
        ])),
    ];
    let input = RecordBatch::try_new(schema, columns).unwrap();
    let budget = &engine.inner.lifecycle.budget;
    let files = storage::write_batches(&table, futures::stream::iter([Ok(input)]), budget)
        .await
        .unwrap();
    let path = files[0].file_path().to_owned();
    let table = storage::append(&table, engine.inner.lifecycle.catalog.as_ref(), files)
        .await
        .unwrap();
    assert_eq!(
        storage::occurrence_schema(&table).unwrap_err().code(),
        "reserved_field"
    );
    let inventory = storage::snapshot_files(&table, budget).await.unwrap();
    let scan = Arc::new(
        storage::OccurrenceScan::values(&table, budget, inventory)
            .await
            .unwrap(),
    );
    let mut small = budget.clone();
    small.limits.max_batch_bytes = 2 << 10;
    let context = small.context();
    context
        .register_table(
            "keys",
            crate::query::file(
                scan.clone(),
                path.clone(),
                Some(&["key".into()]),
                small.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    let values = context
        .sql("SELECT key FROM keys ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(values.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(
        values[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    let mut full = scan.read_file(&path, &small).unwrap().boxed();
    assert_eq!(full.try_next().await.unwrap_err().code(), "oversized_batch");
    drop(full);
    // Leave one reader and a KiB for its small selection/schema mappings.
    // A second reader cannot reserve its batch buffers under this ceiling.
    let spare = small
        .reserve(
            small.limits.memory_bytes / 2
                - small.limits.max_metadata_bytes * 2
                - small.limits.max_batch_bytes * 2
                - 1024,
        )
        .unwrap();
    let mut first = scan
        .read_projected_file(&path, &small, Some(&[0]))
        .unwrap()
        .boxed();
    assert!(first.try_next().await.unwrap().is_some());
    assert_eq!(
        scan.read_projected_file(&path, &small, Some(&[0]))
            .err()
            .unwrap()
            .code(),
        "capacity"
    );
    drop(first);
    drop(small.reserve(small.limits.max_batch_bytes * 2).unwrap());
    drop(spare);
    // Damage only an unselected payload page. Selected key decoding still succeeds.
    let file = std::fs::File::open(&path).unwrap();
    let parquet =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let offset = parquet.metadata().row_group(0).column(1).data_page_offset();
    drop(parquet);
    let mut corrupt = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    corrupt.seek(SeekFrom::Start(offset as u64)).unwrap();
    corrupt.write_all(&[255; 8]).unwrap();
    corrupt.sync_all().unwrap();
    drop(corrupt);
    let mut selected = scan
        .read_projected_file(&path, &small, Some(&[0]))
        .unwrap()
        .boxed();
    let mut keys: Vec<i64> = Vec::new();
    while let Some(batch) = selected.try_next().await.unwrap() {
        keys.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
    }
    assert_eq!(keys, vec![1, 2]);
    let mut full = scan.read_file(&path, budget).unwrap().boxed();
    assert!(full.try_next().await.is_err());
}

#[tokio::test]
async fn oversized_candidate_metadata_refuses_before_writing_any_object() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            max_metadata_bytes: 1024,
            max_batch_bytes: 64 << 10,
            memory_bytes: 32 << 20,
            scratch_bytes: 0,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let field = NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long))
        .with_doc("x".repeat(2048));
    let schema = Schema::builder()
        .with_fields([Arc::new(field)])
        .build()
        .unwrap();
    let directory = root.path().join("oversized");
    assert_eq!(
        engine
            .create_output(&Id::new("size").unwrap(), &directory, schema)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    assert!(!directory.exists());
}

#[tokio::test]
async fn reused_key_name_with_new_id_matches_absence_and_empty_noop_results() {
    let (root, engine, owner, source, original_pin, original_rows, _) = fixture().await;
    let budget = &engine.inner.lifecycle.budget;
    let mut fields = source
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields[1] = field(70, "key", PrimitiveType::Long);
    let schema = Schema::builder().with_fields(fields).build().unwrap();
    let metadata = source
        .metadata()
        .clone()
        .into_builder(None)
        .add_current_schema(schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let identifier =
        iceberg::TableIdent::new(source.identifier().namespace().clone(), "reused".into());
    let source = storage::register_candidate(
        engine.inner.lifecycle.catalog.as_ref(),
        &identifier,
        storage::CandidateWriter::Local,
        move || Ok(metadata),
        budget,
    )
    .await
    .unwrap();
    let before = storage::pin(
        &owner,
        &source,
        original_pin.rows,
        budget,
        &original_pin.objects,
    )
    .await
    .unwrap();
    let mut absent = original_rows
        .into_iter()
        .map(|r| (None, r.1, r.2, r.3))
        .collect::<Vec<_>>();
    absent.sort();
    assert_eq!(rows(&before, budget).await, absent);
    let context = engine.context();
    let new_rows = vec![(
        Some(1),
        Some("actually supplied".into()),
        Some(String::new()),
        Some(123),
    )];
    let added = engine
        .replace_keys(Replacement {
            dataset: &owner,
            directory: locations::Location::Local(root.path().join("new-key")),
            before: &before,
            keys: &["key".into()],
            scope: context.sql("SELECT 1::BIGINT AS key").await.unwrap(),
            replacement: context
                .read_batch(batch(source.metadata().current_schema(), &new_rows, true))
                .unwrap(),
            replacement_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(added.removed.rows, 0);
    assert_eq!(added.metrics.files_reused, 2);
    let mut expected = absent.clone();
    expected.extend(new_rows.clone());
    expected.sort();
    assert_eq!(rows(&added.table, budget).await, expected);
    let empty = context
        .read_batch(batch(source.metadata().current_schema(), &[], true))
        .unwrap();
    let deleted = engine
        .replace_keys(Replacement {
            dataset: &owner,
            directory: locations::Location::Local(root.path().join("delete-null-key")),
            before: &added.table,
            keys: &["key".into()],
            scope: context.sql("SELECT NULL::BIGINT AS key").await.unwrap(),
            replacement: empty.clone(),
            replacement_bytes: Some(0),
        })
        .await
        .unwrap();
    assert_eq!(rows(&deleted.table, budget).await, new_rows);
    assert_eq!(rows(&deleted.removed, budget).await, absent);
    assert_eq!(deleted.added.rows, 0);
    assert_eq!(deleted.metrics.files_reused, 1);
    let noop = engine
        .replace_keys(Replacement {
            dataset: &owner,
            directory: locations::Location::Local(root.path().join("noop")),
            before: &deleted.table,
            keys: &["key".into()],
            scope: context
                .sql("SELECT NULL::BIGINT AS key WHERE false")
                .await
                .unwrap(),
            replacement: empty.clone(),
            replacement_bytes: Some(0),
        })
        .await
        .unwrap();
    assert_eq!(noop.table, deleted.table);
    assert_eq!(noop.added.rows, 0);
    assert_eq!(noop.removed.rows, 0);
    let all = engine
        .replace_keys(Replacement {
            dataset: &owner,
            directory: locations::Location::Local(root.path().join("delete-all")),
            before: &noop.table,
            keys: &["key".into()],
            scope: context.sql("SELECT 1::BIGINT AS key").await.unwrap(),
            replacement: empty,
            replacement_bytes: Some(0),
        })
        .await
        .unwrap();
    assert_eq!(all.table.rows, 0);
    assert!(rows(&all.table, budget).await.is_empty());
    for pin in [&all.table, &all.removed, &all.added] {
        let table = storage::open_pin(pin, budget).await.unwrap();
        assert_eq!(
            table.metadata().current_schema(),
            source.metadata().current_schema()
        );
        assert_eq!(table.metadata().last_column_id(), 900);
        crate::engine::verify_table(pin, budget).await.unwrap();
    }
}

#[cfg(feature = "remote")]
mod resources;

#[tokio::test]
#[ignore = "qualification-only observation of prepared-key scan work"]
async fn prepared_scope_view_scan_observations() {
    use datafusion::physical_plan::ExecutionPlan;
    use parquet::arrow::ArrowWriter;

    fn scanned(plan: &dyn ExecutionPlan) -> Option<usize> {
        let mut bytes = plan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("bytes_scanned"))
            .map(|value| value.as_usize());
        for child in plan.children() {
            if let Some(child) = scanned(child.as_ref()) {
                bytes = Some(bytes.unwrap_or(0) + child);
            }
        }
        bytes
    }

    let (root, engine, _, _, _, _, _) = fixture().await;
    let context = engine.context();
    let values = RecordBatch::try_from_iter([(
        "key",
        Arc::new(Int64Array::from(vec![Some(1), None])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let path = root.path().join("prepared-scope.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), values.schema(), None).unwrap();
    writer.write(&values).unwrap();
    writer.close().unwrap();
    let prepared = context
        .read_parquet(
            path.to_string_lossy().as_ref(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    context
        .register_table("affected_keys", prepared.into_view())
        .unwrap();
    for probes in [1, 10, 100] {
        let mut bytes = 0;
        let mut scans = 0;
        for _ in 0..probes {
            // Isolate the prepared-key side. The actual replacement loop creates
            // one such plan per source-file probe and per changed-file rewrite.
            let left = context.sql("SELECT 1::BIGINT AS key").await.unwrap();
            let frame = key_join(
                left,
                context.table("affected_keys").await.unwrap(),
                &["key".into()],
                datafusion::logical_expr::JoinType::Inner,
            )
            .unwrap()
            .select(vec![lit(1)])
            .unwrap()
            .limit(0, Some(1))
            .unwrap();
            let task = Arc::new(frame.task_ctx());
            let plan = frame.create_physical_plan().await.unwrap();
            let output = datafusion::physical_plan::collect(plan.clone(), task)
                .await
                .unwrap();
            assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let observed = scanned(plan.as_ref()).expect("prepared key reader has scan metrics");
            bytes += observed;
            scans += usize::from(observed > 0);
        }
        eprintln!(
            "DOCSPEC_SCOPE_PROBE {}",
            serde_json::json!({"probes": probes, "scope_scan_executions": scans,
                "requested_scope_bytes": bytes})
        );
    }
}
