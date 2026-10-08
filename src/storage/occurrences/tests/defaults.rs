//! Native v3 default and presence qualification before public schema additions.

use super::*;
use arrow::array::BooleanArray;
use iceberg::spec::{FormatVersion, Literal, NestedField, PrimitiveType, Type};

type Row = (Option<i64>, Option<i64>, bool);

fn fields(write_default: i64) -> iceberg::spec::Schema {
    iceberg::spec::Schema::builder()
        .with_fields([
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(7, "value", Type::Primitive(PrimitiveType::Long))
                .with_initial_default(Literal::long(7))
                .with_write_default(Literal::long(write_default))
                .into(),
            NestedField::required(9, "supplied", Type::Primitive(PrimitiveType::Boolean))
                .with_initial_default(Literal::bool(false))
                .with_write_default(Literal::bool(false))
                .into(),
        ])
        .build()
        .unwrap()
}

async fn register(
    base: &Table,
    catalog: &dyn Catalog,
    metadata: iceberg::spec::TableMetadata,
    name: &str,
    budget: &Budget,
) -> Table {
    storage::register_candidate(
        catalog,
        &iceberg::TableIdent::new(base.identifier().namespace().clone(), name.into()),
        move || Ok(metadata),
        budget,
    )
    .await
    .unwrap_or_else(|error| panic!("register {name}: {error}"))
}

// Probe the pinned native reader without relaxing the application's qualification
// guard. Match its ID-aware projection, including the transient metadata column.
async fn native_batches(table: &Table) -> Vec<RecordBatch> {
    let schema = table.metadata().current_schema().clone();
    let mut ids = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    ids.push(iceberg::metadata_columns::RESERVED_FIELD_ID_FILE);
    let tasks = table
        .scan()
        .with_concurrency_limit(1)
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .map_ok(move |mut task| {
            task.schema = schema.clone();
            task.project_field_ids = ids.clone();
            task
        })
        .boxed();
    table
        .reader_builder()
        .with_batch_size(2)
        .with_data_file_concurrency_limit(1)
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build()
        .read(tasks)
        .unwrap()
        .stream()
        .map(|batch| storage::schema::strip_file_column(batch.unwrap(), 64 << 10))
        .try_collect()
        .await
        .unwrap()
}

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let supplied = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(supplied.null_count(), 0);
        for row in 0..batch.num_rows() {
            rows.push((
                (!ids.is_null(row)).then(|| ids.value(row)),
                (!values.is_null(row)).then(|| values.value(row)),
                supplied.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

fn batch(table: &Table, rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
        ),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn native_v3_defaults_and_presence_survive_upgrade_append_rewrite_and_reopen() {
    let (root, catalog, original, _) = fixture().await;
    // Registration reserves metadata and a separate blocking I/O buffer.
    let budget = Budget::new(Limits {
        memory_bytes: 16 << 20,
        ..budget(2).limits
    })
    .unwrap();
    assert_eq!(original.metadata().format_version(), FormatVersion::V2);
    let inventory = storage::snapshot_files(&original, &budget).await.unwrap();
    let originals = inventory
        .data
        .iter()
        .map(|file| {
            (
                file.file_path().to_owned(),
                storage::digest(std::path::Path::new(file.file_path())).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    drop(inventory);
    let metadata = original
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(root.path().join("defaulted").to_string_lossy().into_owned())
        .upgrade_format_version(FormatVersion::V3)
        .unwrap()
        .add_current_schema(fields(9))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let defaulted = register(&original, catalog.as_ref(), metadata, "defaulted", &budget).await;
    assert_eq!(
        defaulted.metadata().current_snapshot_id(),
        original.metadata().current_snapshot_id()
    );
    assert_eq!(defaulted.metadata().last_column_id(), 9);
    // No public default/absence support follows merely from this native probe.
    assert_eq!(
        storage::occurrence_schema(&defaulted).unwrap_err().code(),
        "unsupported_schema_evolution"
    );
    let mut expected = vec![vec![Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)]; 2]
        .concat()
        .into_iter()
        .map(|id| (id, Some(7), false))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(rows(&native_batches(&defaulted).await), expected);
    let old_expected = expected.clone();

    // A future admission normalizer must materialize write-default=9 for omission;
    // the native writer receives complete batches and does not perform that job.
    let supplied = [
        (Some(100), Some(9), false),
        (Some(101), None, true),
        (Some(102), Some(7), true),
        (Some(103), Some(0), true),
    ];
    let files = storage::write_batches(
        &defaulted,
        futures::stream::iter([Ok(batch(&defaulted, &supplied))]),
        &budget,
    )
    .await
    .unwrap();
    let appended = storage::append(&defaulted, catalog.as_ref(), files)
        .await
        .unwrap();
    expected.extend(supplied);
    expected.sort();
    assert_eq!(rows(&native_batches(&appended).await), expected);
    let appended_expected = expected.clone();
    assert!(
        appended
            .metadata()
            .current_snapshot()
            .unwrap()
            .row_range()
            .is_some()
    );

    let metadata = appended
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(
            root.path()
                .join("later-default")
                .to_string_lossy()
                .into_owned(),
        )
        .add_current_schema(fields(11))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let later = register(&appended, catalog.as_ref(), metadata, "later", &budget).await;
    // Changing write-default must not reinterpret either old absent/defaulted rows
    // or supplied nulls. Only newly normalized omissions receive the new value.
    assert_eq!(rows(&native_batches(&later).await), expected);
    let omitted = [(Some(104), Some(11), false)];
    let files = storage::write_batches(
        &later,
        futures::stream::iter([Ok(batch(&later, &omitted))]),
        &budget,
    )
    .await
    .unwrap();
    let later = storage::append(&later, catalog.as_ref(), files)
        .await
        .unwrap();
    expected.extend(omitted);
    expected.sort();

    // Exercise the native replacement mechanism before widening the production
    // format gate. Keep its schema/history/high-water and clear only snapshots.
    let snapshots = later
        .metadata()
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<Vec<_>>();
    let metadata = later
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(root.path().join("rewritten").to_string_lossy().into_owned())
        .remove_ref("main")
        .remove_snapshots(&snapshots)
        .build()
        .unwrap()
        .metadata;
    let rewritten = register(&later, catalog.as_ref(), metadata, "rewritten", &budget).await;
    let mut inventory = storage::snapshot_files(&later, &budget).await.unwrap();
    // One untouched original file remains shared; every other row is materialized.
    let shared = inventory
        .data
        .iter()
        .position(|file| originals.contains_key(file.file_path()))
        .unwrap();
    let shared = inventory.data.remove(shared);
    let mut rewritten_rows = rows(&native_batches(&later).await);
    for id in [Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)] {
        let index = rewritten_rows
            .iter()
            .position(|row| *row == (id, Some(7), false))
            .unwrap();
        rewritten_rows.remove(index);
    }
    let mut files = storage::write_batches(
        &rewritten,
        futures::stream::iter([Ok(batch(&rewritten, &rewritten_rows))]),
        &budget,
    )
    .await
    .unwrap();
    files.push(shared);
    let rewritten = storage::append(&rewritten, catalog.as_ref(), files)
        .await
        .unwrap();
    let reopened = catalog.load_table(rewritten.identifier()).await.unwrap();
    assert_eq!(reopened.metadata().format_version(), FormatVersion::V3);
    assert_eq!(reopened.metadata().last_column_id(), 9);
    assert_eq!(rows(&native_batches(&reopened).await), expected);
    assert_eq!(rows(&native_batches(&defaulted).await), old_expected);
    assert_eq!(rows(&native_batches(&appended).await), appended_expected);
    for (path, digest) in originals {
        assert_eq!(
            storage::digest(std::path::Path::new(&path)).unwrap(),
            digest
        );
    }
}
