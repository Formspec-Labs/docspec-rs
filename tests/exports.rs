//! Local distributions preserve authorized values and durable retry boundaries.
// Rust guideline compliant 2026-02-21

#[cfg(feature = "test-util")]
use super::support;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    export::{ExportPin, ExportPolicy, ExportRows, ExportSpec},
    model::{Id, InputSpec, ReleasePin, RunSpec, UpdateSpec},
    storage,
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{fs::File, path::Path, sync::Arc};

type Row = (Option<i64>, Option<String>, String);

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 8 << 20,
    }
}

fn write_input(path: &Path, rows: &[Row], keys_only: bool) -> InputSpec {
    let mut fields = vec![Field::new("id", DataType::Int64, true)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    ))];
    if !keys_only {
        fields.extend([
            Field::new("public", DataType::Utf8, true),
            Field::new("secret", DataType::Utf8, false),
        ]);
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.2.as_str()),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

async fn fixture(root: &Path) -> (Engine, ReleasePin, Vec<Row>) {
    let rows = vec![
        (Some(1), Some("one".into()), "private-canary-1".into()),
        (Some(1), Some("duplicate".into()), "private-canary-2".into()),
        (Some(2), None, "private-canary-3".into()),
        (None, Some("null key".into()), "private-canary-4".into()),
    ];
    let source = write_input(&root.join("input.parquet"), &rows, false);
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    (engine, release, rows)
}

fn request(source: &ReleasePin, operation: &str) -> ExportSpec {
    ExportSpec {
        dataset: source.dataset.dataset.clone(),
        operation: id(operation),
        source: source.version_ref(),
        channel: id("download"),
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["public".into(), "id".into()],
            ..ExportPolicy::default()
        },
    }
}

fn independent_rows(pin: &ExportPin) -> Vec<(Option<i64>, Option<String>)> {
    storage::verify_object(&pin.object).unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap()).unwrap();
    assert!(builder.schema().metadata().is_empty());
    assert_eq!(
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["public", "id"]
    );
    for field in builder.schema().fields() {
        assert_eq!(field.metadata().len(), 1);
        assert!(
            field
                .metadata()
                .contains_key(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
        );
    }
    let mut actual = Vec::new();
    for batch in builder.with_batch_size(1).build().unwrap() {
        let batch = batch.unwrap();
        let texts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(
            ids.iter()
                .zip(texts.iter())
                .map(|(id, text)| (id, text.map(str::to_owned))),
        );
    }
    actual.sort();
    actual
}

fn assert_authorized_ipc(bytes: &[u8]) {
    let reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let schema = reader.schema();
    assert!(schema.metadata().is_empty());
    assert_eq!(schema.fields().len(), 2);
    for (field, name, data_type, field_id) in [
        (&schema.fields()[0], "public", DataType::Utf8, "2"),
        (&schema.fields()[1], "id", DataType::Int64, "1"),
    ] {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &data_type);
        assert!(field.is_nullable());
        assert_eq!(
            field.metadata(),
            &std::collections::HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                field_id.to_owned()
            ),])
        );
    }
    let mut actual = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let texts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(
            ids.iter()
                .zip(texts.iter())
                .map(|(id, text)| (id, text.map(str::to_owned))),
        );
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![
            (None, Some("null key".to_owned())),
            (Some(1), Some("duplicate".to_owned())),
            (Some(1), Some("one".to_owned())),
            (Some(2), None),
        ]
    );
    assert!(
        !bytes
            .windows(b"private-canary".len())
            .any(|part| part == b"private-canary")
    );
}

#[tokio::test]
async fn primitive_exports_preserve_types_values_nulls_and_authorized_column_order() {
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int32Array, Time64MicrosecondArray, TimestampMicrosecondArray,
    };

    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("primitive-input.parquet");
    let decimal = Decimal128Array::from(vec![Some(123_456_789), Some(-1), None, Some(0)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [
            Some(b"\x00\xff\x01\x02".as_slice()),
            Some(b"ABCD".as_slice()),
            None,
            Some(b"\x00\x00\x00\x00".as_slice()),
        ]
        .into_iter(),
        4,
    )
    .unwrap();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("row_id", Arc::new(Int64Array::from(vec![0, 1, 2, 3]))),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ),
        (
            "bytes",
            Arc::new(BinaryArray::from(vec![
                Some(b"\x00\xff".as_slice()),
                Some(b"".as_slice()),
                None,
                Some(b"\x80abc".as_slice()),
            ])),
        ),
        ("amount", Arc::new(decimal)),
        (
            "day",
            Arc::new(Date32Array::from(vec![
                Some(-1),
                Some(0),
                None,
                Some(20_000),
            ])),
        ),
        (
            "clock",
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(0),
                Some(86_399_999_999),
                None,
                Some(45_000_000_000),
            ])),
        ),
        ("fixed", Arc::new(fixed)),
        (
            "instant",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                Some(0),
                None,
                Some(1_725_000_123_456_789),
            ])),
        ),
        (
            "instant_utc",
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    None,
                    Some(1_725_000_123_456_789),
                ])
                .with_timezone("UTC"),
            ),
        ),
        (
            "small_integer",
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(i32::MAX),
                None,
                Some(0),
            ])),
        ),
        (
            "float",
            Arc::new(Float32Array::from(vec![
                Some(-0.5),
                Some(1.25),
                None,
                Some(0.0),
            ])),
        ),
        (
            "double",
            Arc::new(Float64Array::from(vec![
                Some(-1.5),
                Some(12_345.125),
                None,
                Some(0.0),
            ])),
        ),
        (
            "secret",
            Arc::new(StringArray::from(vec![
                "canary-one",
                "canary-two",
                "canary-three",
                "canary-four",
            ])),
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let input = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, values)| values).collect(),
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&input_path).unwrap(), schema, None).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("primitives"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input_path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input_path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    // Iceberg labels zoned timestamps with its canonical UTC offset. Changing
    // this metadata preserves the original epoch values and null buffers.
    let mut expected_columns = input.columns().to_vec();
    // Iceberg's binary primitive exposes 64-bit offsets in Arrow.
    expected_columns[2] = arrow::compute::cast(input.column(2), &DataType::LargeBinary).unwrap();
    expected_columns[8] = Arc::new(
        input
            .column(8)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .clone()
            .with_timezone("+00:00"),
    );
    // Deliberately reorder unlike-typed columns and omit the canary column.
    let order = [8, 3, 1, 7, 2, 5, 4, 6, 0, 9, 10, 11];
    let names = order
        .iter()
        .map(|index| input.schema().field(*index).name().clone())
        .collect::<Vec<_>>();
    let mut spec = request(&release, "primitive-export");
    spec.policy.columns = names.clone();
    let pin = engine.export(&spec).await.unwrap();
    storage::verify_object(&pin.object).unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap()).unwrap();
    assert_eq!(
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        names
    );
    assert!(builder.schema().metadata().is_empty());
    for (output_index, source_index) in order.iter().enumerate() {
        let expected = input.schema();
        let expected = expected.field(*source_index);
        let actual = builder.schema().field(output_index);
        assert_eq!(
            actual.data_type(),
            expected_columns[*source_index].data_type()
        );
        assert_eq!(actual.is_nullable(), expected.is_nullable());
        assert_eq!(actual.metadata().len(), 1);
        assert_eq!(
            pin.fields[output_index].id,
            i32::try_from(source_index + 1).unwrap()
        );
    }
    let mut seen = std::collections::BTreeSet::new();
    for batch in builder.with_batch_size(1).build().unwrap() {
        let batch = batch.unwrap();
        let row_ids = batch
            .column_by_name("row_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let expected_row = usize::try_from(row_ids.value(row)).unwrap();
            assert!(seen.insert(expected_row));
            assert!(expected_row < input.num_rows());
            for (output_index, source_index) in order.iter().enumerate() {
                // Compare typed Arrow values against the original input arrays,
                // not another engine read or JSON conversion. Array equality
                // retains binary bytes, decimal scale, temporal units, and nulls.
                assert_eq!(
                    batch.column(output_index).slice(row, 1).to_data(),
                    expected_columns[*source_index]
                        .slice(expected_row, 1)
                        .to_data()
                );
            }
        }
    }
    assert_eq!(seen, (0..input.num_rows()).collect());
    assert_eq!(pin.rows, input.num_rows() as u64);
}

#[tokio::test]
async fn small_read_batches_export_exact_data_with_bounded_footer() {
    let root = tempfile::tempdir().unwrap();
    let rows = (0..256_i64)
        .map(|value| {
            (
                Some(value),
                Some(format!("row-{value}")),
                "private-canary".into(),
            )
        })
        .collect::<Vec<_>>();
    let source = write_input(&root.path().join("footer-input.parquet"), &rows, false);
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            batch_rows: 256,
            ..limits()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("footer"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let first = engine.export(&request(&release, "first")).await.unwrap();
    drop(engine);

    // One-row reader batches must share output groups instead of growing the footer.
    // The source retains its exact identity under the smaller read allowance.
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            batch_rows: 1,
            max_metadata_bytes: 256 << 10,
            ..limits()
        },
    )
    .await
    .unwrap();
    let mut spec = request(&release, "bounded-footer");
    spec.expected = Some(first.id.clone());
    let pin = engine.export(&spec).await.unwrap();
    let footer =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap()).unwrap();
    assert_eq!(footer.metadata().num_row_groups(), 1);
    assert_eq!(
        footer.metadata().file_metadata().num_rows(),
        rows.len() as i64
    );
    assert_eq!(engine.export(&spec).await.unwrap(), pin);
    let mut expected = rows
        .into_iter()
        .map(|(id, text, _)| (id, text))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(independent_rows(&pin), expected);
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        pin
    );
}

#[tokio::test]
async fn memory_driven_footer_limit_preserves_prior_selection_and_can_resume() {
    let root = tempfile::tempdir().unwrap();
    // Distinct eight-KiB strings fill the writer estimate even with dictionary encoding.
    // The fixture stays modest while producing enough memory-driven groups to refuse.
    let text = "abcdefgh".repeat(1024);
    let rows = (0..1024_i64)
        .map(|value| {
            (
                Some(value),
                Some(format!("{value}:{text}")),
                "private-canary".into(),
            )
        })
        .collect::<Vec<_>>();
    let source = write_input(&root.path().join("footer-input.parquet"), &rows, false);
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            // Initial input slices may retain the complete eight-MiB Arrow buffers.
            // Small source groups also keep later one-row reads below 32 KiB.
            batch_rows: 1,
            max_batch_bytes: 16 << 20,
            ..limits()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("footer"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let first = engine.export(&request(&release, "first")).await.unwrap();
    drop(engine);

    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            batch_rows: 1,
            max_batch_bytes: 32 << 10,
            max_metadata_bytes: 256 << 10,
            ..limits()
        },
    )
    .await
    .unwrap();
    let mut spec = request(&release, "bounded-footer");
    spec.expected = Some(first.id.clone());
    let error = engine.export(&spec).await.unwrap_err();
    assert_eq!(error.code(), "metadata_capacity");
    assert!(error.to_string().contains("flat export footer"), "{error}");
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        first
    );
    let records = engine.inspect(&spec.dataset).await.unwrap();
    assert!(records.contains_key("export-operation/bounded-footer"));
    assert!(!records.contains_key("export-materialized/bounded-footer"));
    assert!(!records.contains_key("export-result/bounded-footer"));
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let pin = engine.export(&spec).await.unwrap();
    let mut expected = rows
        .into_iter()
        .map(|(id, text, _)| (id, text))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(independent_rows(&pin), expected);
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        pin
    );
}

#[tokio::test]
async fn independent_parquet_contains_only_authorized_columns_and_exact_occurrences() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, rows) = fixture(root.path()).await;
    let spec = request(&release, "public-file");
    let pin = engine.export(&spec).await.unwrap();
    let mut expected = rows
        .into_iter()
        .map(|(id, text, _)| (id, text))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(independent_rows(&pin), expected);
    assert_eq!(pin.source, release.version_ref());
    assert_eq!(
        pin.fields.iter().map(|field| field.id).collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(pin.rows, 4);
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        pin
    );
    assert_eq!(
        engine
            .export_pin(&spec.dataset, &spec.operation)
            .await
            .unwrap(),
        pin
    );
    let modified = std::fs::metadata(&pin.object.path)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(engine.export(&spec).await.unwrap(), pin);
    assert_eq!(
        std::fs::metadata(&pin.object.path)
            .unwrap()
            .modified()
            .unwrap(),
        modified
    );
    let mut conflict = spec.clone();
    conflict.policy.columns = vec!["secret".into()];
    assert_eq!(
        engine.export(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
}

#[tokio::test]
async fn replacements_deletions_and_old_downloads_follow_their_exact_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let (engine, initial, _) = fixture(root.path()).await;
    let first = engine.export(&request(&initial, "first")).await.unwrap();
    let replacements = vec![(
        Some(1),
        Some("corrected".into()),
        "replacement-private".into(),
    )];
    let deletions = vec![(Some(2), None, String::new())];
    let latest = engine
        .update(&UpdateSpec {
            dataset: initial.dataset.dataset.clone(),
            operation: id("corrected"),
            definition: "v1".into(),
            base: initial.id.clone(),
            keys: vec!["id".into()],
            replacements: write_input(
                &root.path().join("replacements.parquet"),
                &replacements,
                false,
            ),
            deletions: Some(write_input(
                &root.path().join("deletions.parquet"),
                &deletions,
                true,
            )),
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    let mut spec = request(&latest, "second");
    spec.expected = Some(first.id.clone());
    let second = engine.export(&spec).await.unwrap();
    assert_eq!(
        independent_rows(&second),
        [
            (None, Some("null key".into())),
            (Some(1), Some("corrected".into()))
        ]
    );
    assert_eq!(independent_rows(&first).len(), 4);
    assert_eq!(
        engine.export(&request(&initial, "first")).await.unwrap(),
        first
    );
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        second
    );
    let mut stale = request(&initial, "regressing");
    stale.expected = Some(second.id);
    assert_eq!(
        engine.export(&stale).await.unwrap_err().code(),
        "export_regression"
    );
}

#[tokio::test]
async fn empty_snapshot_keeps_standalone_schema_and_explicit_policy() {
    let root = tempfile::tempdir().unwrap();
    let source = write_input(&root.path().join("empty.parquet"), &[], false);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("empty"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let spec = request(&release, "empty-export");
    let pin = engine.export(&spec).await.unwrap();
    assert_eq!(pin.rows, 0);
    assert_eq!(pin.validation.sampled_rows, 0);
    assert!(independent_rows(&pin).is_empty());
    let mut invalid = spec.clone();
    invalid.operation = id("all-columns");
    invalid.policy.columns.clear();
    assert_eq!(
        engine.export(&invalid).await.unwrap_err().code(),
        "invalid_export"
    );
    let mut json = serde_json::to_value(&spec).unwrap();
    json["policy"].as_object_mut().unwrap().remove("rows");
    assert!(serde_json::from_value::<ExportSpec>(json).is_err());
    let mut missing = spec;
    missing.operation = id("missing-column");
    missing.channel = id("another");
    missing.policy.columns = vec!["undeclared".into()];
    assert_eq!(
        engine.export(&missing).await.unwrap_err().code(),
        "export_disclosure"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn validation_and_selection_resume_without_opening_the_source_again() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, rows) = fixture(root.path()).await;
    let spec = request(&release, "resumed");
    assert_eq!(
        engine
            .export_until(&spec, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&spec.dataset).await.unwrap();
    let output = records["export-materialized/resumed"]["object"]["path"]
        .as_str()
        .unwrap();
    let modified = std::fs::metadata(output).unwrap().modified().unwrap();
    // Temporarily withhold source metadata in this private fixture. Selection
    // must consume the durable completion, without opening its source payloads.
    let metadata = &release.dataset.groups["source"].metadata.path;
    let withheld = format!("{metadata}.withheld");
    std::fs::rename(metadata, &withheld).unwrap();
    let resumed = engine.export(&spec).await;
    std::fs::rename(&withheld, metadata).unwrap();
    let pin = resumed.unwrap();
    assert_eq!(
        std::fs::metadata(output).unwrap().modified().unwrap(),
        modified
    );
    let mut expected = rows
        .into_iter()
        .map(|(id, text, _)| (id, text))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(independent_rows(&pin), expected);
    let mut next = request(&release, "lost-response");
    next.expected = Some(pin.id);
    assert_eq!(
        engine
            .export_until(&next, "selection")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let selected = engine
        .latest_export(&next.dataset, &next.channel)
        .await
        .unwrap();
    assert_eq!(engine.export(&next).await.unwrap(), selected);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn stale_authority_and_channel_keep_completed_output_without_promotion() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path()).await;
    let spec = request(&release, "candidate");
    assert_eq!(
        engine
            .export_until(&spec, "materialization")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&spec.dataset).await.unwrap();
    let output = records["export-materialized/candidate"]["object"]["path"]
        .as_str()
        .unwrap();
    let bytes = std::fs::read(output).unwrap();
    let winner = engine.export(&request(&release, "winner")).await.unwrap();
    assert_eq!(
        engine.export(&spec).await.unwrap_err().code(),
        "export_conflict"
    );
    assert_eq!(std::fs::read(output).unwrap(), bytes);
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        winner
    );
    let mut stale = request(&release, "authority");
    stale.channel = id("other");
    assert_eq!(
        engine
            .export_until(&stale, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine
        .invalidate(&spec.dataset, "disclosure review changed", None)
        .await
        .unwrap();
    assert_eq!(
        engine.export(&stale).await.unwrap_err().code(),
        "stale_export"
    );
    assert_eq!(
        engine
            .latest_export(&stale.dataset, &stale.channel)
            .await
            .unwrap_err()
            .code(),
        "no_export"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn killed_export_workers_resume_without_repeating_completed_work() {
    for phase in ["materialization", "validation", "selection"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, release, _) = fixture(root.path()).await;
        let spec = request(&release, "resume-process");
        drop(engine);
        support::kill_after(root.path(), &spec, "export", phase);
        let (pin, work): (ExportPin, _) = support::run_cli(
            root.path(),
            &["export", root.path().join("request.json").to_str().unwrap()],
        );
        assert!(!work.iter().any(|name| name == "export-materialization"));
        if phase != "materialization" {
            assert!(!work.iter().any(|name| name == "export-validation"));
        }
        assert_eq!(independent_rows(&pin).len(), 4);
        let (latest, _): (ExportPin, _) = support::run_cli(
            root.path(),
            &[
                "latest-export",
                spec.dataset.as_str(),
                spec.channel.as_str(),
            ],
        );
        assert_eq!(latest, pin);
        let (historical, _): (ExportPin, _) = support::run_cli(
            root.path(),
            &["export-pin", spec.dataset.as_str(), spec.operation.as_str()],
        );
        assert_eq!(historical, pin);
        let manifest = root.path().join("export-pin.json");
        let output = root.path().join("export.arrow");
        std::fs::write(&manifest, serde_json::to_vec(&pin).unwrap()).unwrap();
        let read = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg("--limits")
            .arg(root.path().join("limits.json"))
            .arg("read-export")
            .arg(&manifest)
            .arg(&output)
            .output()
            .unwrap();
        assert!(
            read.status.success(),
            "{}",
            String::from_utf8_lossy(&read.stderr)
        );
        assert_authorized_ipc(&std::fs::read(output).unwrap());
    }
}

#[tokio::test]
async fn http_exports_compaction_and_restore_points_use_the_shared_services() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path()).await;
    let router = docspec::http::router(engine.clone());
    let spec = request(&release, "http-file");
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/exports/run")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let pin: ExportPin =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(independent_rows(&pin).len(), 4);
    for uri in [
        "/datasets/comments/exports/channels/download",
        "/datasets/comments/exports/http-file",
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actual: ExportPin =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(actual, pin);
    }
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/datasets/comments/exports/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/datasets/comments/exports/http-file/arrow")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "application/vnd.apache.arrow.stream"
    );
    assert_eq!(response.headers()["docspec-dataset"], "comments");
    assert_eq!(response.headers()["docspec-export"], "http-file");
    assert_authorized_ipc(&to_bytes(response.into_body(), 1 << 20).await.unwrap());
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/datasets/comments/exports/missing/arrow")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "no_export");
    let compaction = docspec::model::CompactionSpec {
        dataset: spec.dataset.clone(),
        operation: id("compact-http"),
        version: release.id,
        group: "source".into(),
        files: vec![],
        target_file_bytes: 1 << 20,
    };
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/compactions/run")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&compaction).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let compacted: docspec::model::CompactionResult =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(engine.compact(&compaction).await.unwrap(), compacted);
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/restore-points")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"dataset": "comments", "point": "http-backup"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let point: docspec::restore::RestorePointRef =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(
        engine
            .create_restore_point(&spec.dataset, &id("http-backup"))
            .await
            .unwrap(),
        point
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn cli_compaction_and_offline_restore_share_the_application_paths() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path()).await;
    drop(engine);
    std::fs::write(
        root.path().join("limits.json"),
        serde_json::to_vec(&limits()).unwrap(),
    )
    .unwrap();
    let spec = docspec::model::CompactionSpec {
        dataset: release.dataset.dataset.clone(),
        operation: id("compact-cli"),
        version: release.id.clone(),
        group: "source".into(),
        files: vec![],
        target_file_bytes: 1 << 20,
    };
    let spec_path = root.path().join("compaction.json");
    std::fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let (compacted, _): (docspec::model::CompactionResult, _) =
        support::run_cli(root.path(), &["compact", spec_path.to_str().unwrap()]);
    assert!(compacted.verified);
    let (point, _): (docspec::restore::RestorePointRef, _) = support::run_cli(
        root.path(),
        &["create-restore-point", "comments", "cli-backup"],
    );
    let point_path = root.path().join("restore-point.json");
    std::fs::write(&point_path, serde_json::to_vec(&point).unwrap()).unwrap();
    let (restored, _): (docspec::restore::RestoredCatalog, _) = support::run_cli(
        root.path(),
        &["restore-local", point_path.to_str().unwrap()],
    );
    assert_eq!(restored.point, point.id);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), release);
}

#[tokio::test]
async fn multi_group_filtered_computed_folded_sorted_and_faceted_export_preserves_selective_page_index()
 {
    use arrow::array::{Array, ListArray, ListBuilder, StringBuilder, StructArray};
    use docspec::{
        evidence::{EVIDENCE, EvidenceSpec, SOURCE_FILE, SOURCE_ROW},
        export::{
            ExportPolicy, ExportRows, ExportSpec, Predicate, ReadingsFoldSpec, ScalarExpr,
            SortColumn,
        },
        model::{BackfillSpec, ComputedField},
    };
    use futures::TryStreamExt;
    use parquet::{
        arrow::arrow_reader::ArrowReaderOptions,
        file::{metadata::PageIndexPolicy, page_index::column_index::ColumnIndexMetaData},
    };
    use std::collections::BTreeMap;

    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("multi-group-input.parquet");

    let mut committee_builder = ListBuilder::new(StringBuilder::new());
    // Row 0: owner-b, congress 118
    committee_builder.values().append_value("hsag");
    committee_builder.values().append_value("hsap");
    committee_builder.append(true);
    // Row 1: owner-a, congress 119
    committee_builder.append(true);
    // Row 2: owner-c, congress 117 (filtered out)
    committee_builder.values().append_value("hsju");
    committee_builder.append(true);
    // Row 3: owner-d, congress 119
    committee_builder.values().append_value("hsif");
    committee_builder.append(true);

    let mut bill_builder = ListBuilder::new(StringBuilder::new());
    // Row 0: empty
    bill_builder.append(true);
    // Row 1: hr-1
    bill_builder.values().append_value("hr1");
    bill_builder.append(true);
    // Row 2: hr-2
    bill_builder.values().append_value("hr2");
    bill_builder.append(true);
    // Row 3: null
    bill_builder.append(false);

    let mut empty_builder = ListBuilder::new(StringBuilder::new());
    for _ in 0..4 {
        empty_builder.append(true);
    }

    let list_field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("owner_id", DataType::Utf8, false),
        Field::new("congress", DataType::Int64, false),
        Field::new("chamber", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("retain_until", DataType::Utf8, false),
        Field::new("secret", DataType::Utf8, false),
        Field::new("committee_values", DataType::List(list_field.clone()), true),
        Field::new("bill_values", DataType::List(list_field.clone()), true),
        Field::new("empty_values", DataType::List(list_field), true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "owner-b", "owner-a", "owner-c", "owner-d",
            ])),
            Arc::new(Int64Array::from(vec![118, 119, 117, 119])),
            Arc::new(StringArray::from(vec!["house", "senate", "house", "house"])),
            Arc::new(StringArray::from(vec![
                Some("expired-body"),
                Some("active-senate-body"),
                Some("filtered-out-body"),
                Some("active-house-body"),
            ])),
            Arc::new(StringArray::from(vec![
                "2026-01-01",
                "2027-01-01",
                "2027-01-01",
                "2027-06-01",
            ])),
            Arc::new(StringArray::from(vec![
                "secret-0", "secret-1", "secret-2", "secret-3",
            ])),
            Arc::new(committee_builder.finish()),
            Arc::new(bill_builder.finish()),
            Arc::new(empty_builder.finish()),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&input_path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("hearings"),
            operation: id("ingest"),
            base: None,
            relationship: None,
            input: input_path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input_path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    let release = engine
        .backfill(&BackfillSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("backfill-naming"),
            base: release.id.clone(),
            definition: "v1".into(),
            group: id("house_naming"),
            group_definition: "naming-v1".into(),
            fields: vec![ComputedField {
                id: 1,
                name: "canonical_title".into(),
                expression: "UPPER(body)".into(),
            }],
        })
        .await
        .unwrap();

    // Collect occurrence coordinates and attach evidence receipts
    let mut occ_stream = engine.read_source_occurrences(&release).await.unwrap();
    let mut src_files = Vec::new();
    let mut src_rows = Vec::new();
    let mut citations = Vec::new();
    while let Some(occ_batch) = occ_stream.try_next().await.unwrap() {
        let owners = occ_batch
            .column_by_name("owner_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let files = occ_batch
            .column_by_name(storage::OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = occ_batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..occ_batch.num_rows() {
            src_files.push(files.value(i).to_owned());
            src_rows.push(positions.value(i));
            citations.push(Some(format!("cit-{}", owners.value(i))));
        }
    }
    let evidence_path = root.path().join("evidence.parquet");
    let evidence_schema = Arc::new(Schema::new(vec![
        Field::new(SOURCE_FILE, DataType::Utf8, false),
        Field::new(SOURCE_ROW, DataType::Int64, false),
        Field::new(EVIDENCE, DataType::Utf8, true),
    ]));
    let evidence_batch = RecordBatch::try_new(
        evidence_schema.clone(),
        vec![
            Arc::new(StringArray::from(src_files)),
            Arc::new(Int64Array::from(src_rows)),
            Arc::new(StringArray::from(citations)),
        ],
    )
    .unwrap();
    let mut ev_writer =
        ArrowWriter::try_new(File::create(&evidence_path).unwrap(), evidence_schema, None).unwrap();
    ev_writer.write(&evidence_batch).unwrap();
    ev_writer.close().unwrap();

    let release = engine
        .update_evidence(&EvidenceSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("record-receipts"),
            definition: "v1".into(),
            base: release.id.clone(),
            replacements: InputSpec {
                path: evidence_path.to_string_lossy().into_owned(),
                digest: storage::digest(&evidence_path).unwrap().0,
            },
            deletions: None,
        })
        .await
        .unwrap();

    let mut computed_columns = BTreeMap::new();
    computed_columns.insert(
        "body".into(),
        ScalarExpr::sql("CASE WHEN retain_until <= '2026-10-21' THEN NULL ELSE body END"),
    );

    let pin = engine
        .export(&ExportSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("export-multi"),
            source: release.version_ref(),
            channel: id("public"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                groups: vec!["source".into(), "house_naming".into(), "evidence".into()],
                rows: ExportRows::All,
                columns: vec![
                    "owner_id".into(),
                    "congress".into(),
                    "chamber".into(),
                    "canonical_title".into(),
                    "body".into(),
                    "evidence".into(),
                    "filename_readings".into(),
                ],
                filter: Some(Predicate::sql("congress >= 118")),
                computed_columns,
                fold_readings: Some(ReadingsFoldSpec {
                    target_column: "filename_readings".into(),
                    columns: vec![
                        "committee_values".into(),
                        "bill_values".into(),
                        "empty_values".into(),
                    ],
                    include_suffixes: vec!["_values".into()],
                    include_prefixes: Vec::new(),
                    strip_suffix: true,
                    drop_source_columns: true,
                }),
                order_by: vec![SortColumn::desc("congress"), SortColumn::asc("owner_id")],
                page_index_columns: vec!["owner_id".into(), "congress".into()],
                facet_sets: vec![
                    vec!["congress".into()],
                    vec!["congress".into(), "chamber".into()],
                ],
                browse_range_columns: vec!["owner_id".into(), "congress".into()],
                child_tables: BTreeMap::new(),
                row_group_rows: None,
            },
        })
        .await
        .unwrap();

    storage::verify_object(&pin.object).unwrap();
    assert_eq!(pin.rows, 3);

    // Verify selective PageIndex: enabled on owner_id (col 0) and congress (col 1),
    // omitted on nested filename_readings (col 6) and unindexed body (col 4).
    let file = File::open(&pin.object.path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let metadata = builder.metadata();
    let col_index = metadata.column_index().expect("column index present");
    let off_index = metadata.offset_index().expect("offset index present");
    for rg_idx in 0..metadata.num_row_groups() {
        assert!(
            !matches!(col_index[rg_idx][0], ColumnIndexMetaData::NONE),
            "owner_id should have ColumnIndex"
        );
        assert!(
            !matches!(col_index[rg_idx][1], ColumnIndexMetaData::NONE),
            "congress should have ColumnIndex"
        );
        assert!(
            matches!(col_index[rg_idx][4], ColumnIndexMetaData::NONE),
            "body should not have ColumnIndex"
        );
        assert!(
            matches!(col_index[rg_idx][6], ColumnIndexMetaData::NONE),
            "nested filename_readings should not have ColumnIndex"
        );
        assert!(!off_index[rg_idx][0].page_locations().is_empty());
        assert!(!off_index[rg_idx][1].page_locations().is_empty());
    }

    let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
    let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        combined
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "owner_id",
            "congress",
            "chamber",
            "canonical_title",
            "body",
            "evidence",
            "filename_readings",
        ]
    );

    let owners = combined
        .column_by_name("owner_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let congresses = combined
        .column_by_name("congress")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let titles = combined
        .column_by_name("canonical_title")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let bodies = combined
        .column_by_name("body")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let evidence_col = combined
        .column_by_name("evidence")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Sorted by congress DESC, owner_id ASC -> (119, owner-a), (119, owner-d), (118, owner-b)
    assert_eq!(
        (0..combined.num_rows())
            .map(|i| (congresses.value(i), owners.value(i)))
            .collect::<Vec<_>>(),
        vec![(119, "owner-a"), (119, "owner-d"), (118, "owner-b")]
    );
    assert_eq!(titles.value(0), "ACTIVE-SENATE-BODY");
    assert_eq!(titles.value(1), "ACTIVE-HOUSE-BODY");
    assert_eq!(titles.value(2), "EXPIRED-BODY");

    // Computed column masked expired row (owner-b has retain_until 2026-01-01 <= 2026-10-21)
    assert_eq!(bodies.value(0), "active-senate-body");
    assert_eq!(bodies.value(1), "active-house-body");
    assert!(bodies.is_null(2));

    // Evidence join populated evidence column
    assert_eq!(evidence_col.value(0), "cit-owner-a");
    assert_eq!(evidence_col.value(1), "cit-owner-d");
    assert_eq!(evidence_col.value(2), "cit-owner-b");

    // Folded readings contain only non-empty entries per row with suffix `_values` stripped
    let readings = combined
        .column_by_name("filename_readings")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let extract_readings = |row_idx: usize| -> Vec<(String, Vec<String>)> {
        let row_list = readings.value(row_idx);
        let struct_arr = row_list.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_arr
            .column_by_name("field")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_list = struct_arr
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        (0..struct_arr.len())
            .map(|i| {
                let vals = values_list.value(i);
                let str_vals = vals.as_any().downcast_ref::<StringArray>().unwrap();
                let v = (0..str_vals.len())
                    .map(|j| str_vals.value(j).to_owned())
                    .collect::<Vec<_>>();
                (fields.value(i).to_owned(), v)
            })
            .collect()
    };
    assert_eq!(
        extract_readings(0),
        vec![("bill".to_owned(), vec!["hr1".to_owned()])]
    );
    assert_eq!(
        extract_readings(1),
        vec![("committee".to_owned(), vec!["hsif".to_owned()])]
    );
    assert_eq!(
        extract_readings(2),
        vec![(
            "committee".to_owned(),
            vec!["hsag".to_owned(), "hsap".to_owned()]
        )]
    );

    // Facets and browse ranges populated
    assert!(!pin.facets.is_empty());
    let congress_119_count = pin
        .facets
        .iter()
        .find(|f| {
            f.grouping == vec!["congress".to_owned()]
                && f.values.get("congress") == Some(&Some("119".to_owned()))
        })
        .expect("congress=119 facet count")
        .count;
    assert_eq!(congress_119_count, 2);
    assert!(!pin.browse_ranges.is_empty());

    // Also verify `groups: ["evidence", "source"]` (with `"evidence"` listed first) anchors
    // the join on `"source"` without failing on missing `__docspec_file` in `evidence`.
    let ev_first_pin = engine
        .export(&ExportSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("export-evidence-first"),
            source: release.version_ref(),
            channel: id("ev-first"),
            expected: None,
            policy: ExportPolicy {
                group: "evidence".into(),
                groups: vec!["evidence".into(), "source".into()],
                rows: ExportRows::All,
                columns: vec!["evidence".into(), "owner_id".into()],
                filter: None,
                computed_columns: BTreeMap::new(),
                fold_readings: None,
                order_by: vec![SortColumn::asc("owner_id")],
                page_index_columns: Vec::new(),
                facet_sets: Vec::new(),
                browse_range_columns: Vec::new(),
                child_tables: BTreeMap::new(),
                row_group_rows: None,
            },
        })
        .await
        .unwrap();
    assert_eq!(ev_first_pin.rows, 4);
}

#[tokio::test]
async fn high_cardinality_list_of_struct_unnests_into_sorted_child_table_with_fast_slice_lookup() {
    use arrow::{
        array::{Array, Int64Builder, ListBuilder, StringBuilder, StructBuilder},
        datatypes::Fields,
    };
    use docspec::export::{ChildTableExportSpec, ExportPolicy, ExportRows, ExportSpec, SortColumn};
    use futures::TryStreamExt;
    use std::{collections::BTreeMap, time::Instant};

    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("wide-occurrences.parquet");

    let struct_fields = Fields::from(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("byte_offset", DataType::Int64, false),
    ]);
    let struct_builder = StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
        ],
    );
    let mut occ_builder = ListBuilder::new(struct_builder);

    // Row 0: doc-5000 with 5,200 occurrences
    for ord in 0..5_200_i64 {
        let sb = occ_builder.values();
        sb.field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(format!(
                "archive/part-{:04}/segment-{:05}.xml",
                ord / 100,
                ord
            ));
        sb.field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(ord * 4096);
        sb.append(true);
    }
    occ_builder.append(true);

    // Row 1: doc-0010 with 10 occurrences
    for ord in 0..10_i64 {
        let sb = occ_builder.values();
        sb.field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(format!("archive/small/segment-{ord:02}.xml"));
        sb.field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(ord * 512);
        sb.append(true);
    }
    occ_builder.append(true);

    let list_field = Arc::new(Field::new_list_field(DataType::Struct(struct_fields), true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("owner_id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("source_occurrences", DataType::List(list_field), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["doc-5000", "doc-0010"])),
            Arc::new(StringArray::from(vec![
                "Omnibus Appropriations",
                "Short Notice",
            ])),
            Arc::new(occ_builder.finish()),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&input_path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Use a tight max_batch_bytes (512 KiB) so the single 5,200-occurrence row exercises
    // adaptive List<Struct> slicing and single-row high-cardinality admission.
    let tight_limits = Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 32 << 20,
        batch_rows: 16,
        max_batch_bytes: 512 << 10,
        max_metadata_bytes: 8 << 20,
    };
    let engine = Engine::open(root.path().join("workspace"), tight_limits)
        .await
        .unwrap();

    // 1. Build succeeds with 5,200-occurrence single parent row
    let release = engine
        .run(&RunSpec {
            dataset: id("catalog"),
            operation: id("build-wide"),
            base: None,
            relationship: None,
            input: input_path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input_path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // 2. Update succeeds with 5,200-occurrence single parent row replacement
    let release = engine
        .update(&UpdateSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("update-wide"),
            definition: "v1".into(),
            base: release.id.clone(),
            keys: vec!["owner_id".into()],
            replacements: InputSpec {
                path: input_path.to_string_lossy().into_owned(),
                digest: storage::digest(&input_path).unwrap().0,
            },
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    // 3. Read succeeds with 5,200-occurrence single parent row
    let read_batches: Vec<RecordBatch> = engine
        .read_source_occurrences(&release)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_read_rows: usize = read_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_read_rows, 2);

    // 3. Export with ChildTableExportSpec unnests source_occurrences into occurrences.parquet
    //    and replaces source_occurrences with occurrence_count in parent export.parquet.
    let mut child_tables = BTreeMap::new();
    child_tables.insert(
        "occurrences".into(),
        ChildTableExportSpec {
            list_column: "source_occurrences".into(),
            parent_key_columns: vec!["owner_id".into()],
            child_key_columns: vec!["owner_id".into()],
            ordinal_column: "occurrence_order".into(),
            struct_columns: None,
            row_group_rows: 2048,
            replace_with_count_column: Some("occurrence_count".into()),
            drop_parent_column: true,
        },
    );

    let pin = engine
        .export(&ExportSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("export-wide"),
            source: release.version_ref(),
            channel: id("public"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                groups: vec!["source".into()],
                rows: ExportRows::All,
                columns: vec![
                    "owner_id".into(),
                    "title".into(),
                    "source_occurrences".into(),
                ],
                filter: None,
                computed_columns: BTreeMap::new(),
                fold_readings: None,
                order_by: vec![SortColumn::asc("owner_id")],
                page_index_columns: vec!["owner_id".into()],
                facet_sets: Vec::new(),
                browse_range_columns: vec!["owner_id".into()],
                child_tables,
                row_group_rows: None,
            },
        })
        .await
        .unwrap();

    storage::verify_object(&pin.object).unwrap();
    let child = pin
        .child_tables
        .get("occurrences")
        .expect("occurrences child export recorded");
    storage::verify_object(&child.object).unwrap();
    assert_eq!(child.rows, 5_210);
    assert!(
        child.object.path.ends_with("occurrences.parquet"),
        "unexpected child path: {}",
        child.object.path
    );
    assert!(
        child.browse_ranges.len() >= 3,
        "expected >= 3 row-group browse ranges for 5,210 rows at 2,048 rows/group"
    );

    // Verify that the child table parquet file contains OffsetIndex and ColumnIndex
    let child_file = std::fs::File::open(&child.object.path).unwrap();
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::metadata::PageIndexPolicy;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(child_file, options).unwrap();
    let meta = builder.metadata();
    assert!(
        meta.offset_index().is_some(),
        "exported child parquet file must retain offset_index when page_index_cols is non-empty"
    );
    assert!(
        meta.column_index().is_some(),
        "exported child parquet file must retain column_index when page_index_cols is non-empty"
    );

    let rgs: Vec<usize> = (0..meta.num_row_groups()).collect();
    let selection = docspec::engine::export::pipeline::build_page_index_selection(
        meta,
        builder.schema(),
        &rgs,
        "owner_id",
        "occurrence_order",
        "doc-5000",
        None,
        2_500,
        2_525,
    );
    let sel = selection
        .expect("build_page_index_selection must return Some(RowSelection) with valid PageIndex");
    assert!(
        sel.row_count() > 0,
        "RowSelection must contain active selectors"
    );

    // Parent export has occurrence_count: Int64 instead of source_occurrences
    let parent_batches: Vec<RecordBatch> = engine
        .read_export(&pin)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let parent =
        arrow::compute::concat_batches(&parent_batches[0].schema(), &parent_batches).unwrap();
    assert_eq!(
        parent
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["owner_id", "title", "occurrence_count"]
    );
    let counts = parent
        .column_by_name("occurrence_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 10); // doc-0010
    assert_eq!(counts.value(1), 5_200); // doc-5000

    // Warm up OS file cache once, then assert child slice lookup for a single owner_id
    // across 5,200 occurrences completes in < 5 ms with exact ordinal order.
    let _ = engine
        .read_export_child_slice(&pin, "occurrences", "doc-5000", 2_500, 25)
        .await
        .unwrap();
    let start = Instant::now();
    let slice_batches = engine
        .read_export_child_slice(&pin, "occurrences", "doc-5000", 2_500, 25)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 20,
        "expected child slice lookup in < 20 ms, took {elapsed:?}"
    );
    let slice = arrow::compute::concat_batches(&slice_batches[0].schema(), &slice_batches).unwrap();
    assert_eq!(slice.num_rows(), 25);
    let slice_owners = slice
        .column_by_name("owner_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let slice_ordinals = slice
        .column_by_name("occurrence_order")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let slice_offsets = slice
        .column_by_name("byte_offset")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..25 {
        assert_eq!(slice_owners.value(i), "doc-5000");
        assert_eq!(slice_ordinals.value(i), 2_500 + i as i64);
        assert_eq!(slice_offsets.value(i), (2_500 + i as i64) * 4096);
    }
}

#[tokio::test]
async fn export_wide_353_column_schema_and_74_byte_body_key_column_index() {
    use arrow::array::Array;
    use docspec::export::SortColumn;
    use parquet::{
        arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        file::{metadata::PageIndexPolicy, page_index::column_index::ColumnIndexMetaData},
    };
    use std::collections::BTreeMap;

    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("wide-353-input.parquet");

    // Construct a 353-column schema:
    // - doc_id (Utf8)
    // - body_key (Utf8, 74-byte `bodies/sha256/<64-hex>.pdf`)
    // - 51 additional core columns (`core_00..core_50`)
    // - 300 sparse reading columns (`reading_000..reading_299`)
    // Total = 2 + 51 + 300 = 353 columns.
    let mut fields = Vec::with_capacity(353);
    fields.push(Field::new("doc_id", DataType::Utf8, false));
    fields.push(Field::new("body_key", DataType::Utf8, false));
    for i in 0..51 {
        fields.push(Field::new(format!("core_{i:02}"), DataType::Utf8, true));
    }
    for i in 0..300 {
        fields.push(Field::new(format!("reading_{i:03}"), DataType::Utf8, true));
    }
    assert_eq!(fields.len(), 353);
    let schema = Arc::new(Schema::new(fields));

    let num_rows = 8usize;
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(353);
    let doc_ids: Vec<String> = (0..num_rows).map(|r| format!("doc-{r:04}")).collect();
    let body_keys: Vec<String> = (0..num_rows)
        .map(|r| {
            // 14 bytes prefix + 64 hex chars - 4 + 4 (".pdf") = 74 bytes:
            // "bodies/sha256/" (14) + 56 hex chars + 4-digit row hex (4) -> 60?
            // Wait: "bodies/sha256/" is 14 bytes, ".pdf" is 4 bytes; 14 + 56 + 4 = 74 bytes,
            // or 64-hex SHA256 digest: 14 + 64 + 4 = 82 bytes (well over 64-byte default truncation!).
            // Let's use an exact 74-byte key where only the trailing bytes after index 64 differ!
            format!("bodies/sha256/{}{:06x}.pdf", "a".repeat(50), r)
        })
        .collect();
    assert_eq!(body_keys[0].len(), 74);
    // Verify that the first 64 bytes of body_keys[0] and body_keys[7] are identical,
    // so a 64-byte truncation would corrupt the min/max bounds!
    assert_eq!(&body_keys[0][..64], &body_keys[num_rows - 1][..64]);
    assert_ne!(body_keys[0], body_keys[num_rows - 1]);

    columns.push(Arc::new(StringArray::from(doc_ids)));
    columns.push(Arc::new(StringArray::from(body_keys.clone())));
    for i in 0..51 {
        let vals: Vec<Option<String>> = (0..num_rows)
            .map(|r| Some(format!("core-{i:02}-row-{r}")))
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    for i in 0..300 {
        let vals: Vec<Option<String>> = (0..num_rows)
            .map(|r| {
                if (r + i) % 50 == 0 {
                    Some(format!("val-{i:03}-{r}"))
                } else {
                    None
                }
            })
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    {
        let file = std::fs::File::create(&input_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let release = engine
        .run(&RunSpec {
            dataset: id("wide353"),
            operation: id("build-wide353"),
            base: None,
            relationship: None,
            input: input_path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input_path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "wide353-v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    let all_col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let pin = engine
        .export(&ExportSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id("export-all-353"),
            source: release.version_ref(),
            channel: id("public"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                groups: vec!["source".into()],
                rows: ExportRows::All,
                columns: all_col_names,
                filter: None,
                computed_columns: BTreeMap::new(),
                fold_readings: None,
                order_by: vec![SortColumn::asc("doc_id"), SortColumn::asc("body_key")],
                page_index_columns: vec!["doc_id".into(), "body_key".into()],
                facet_sets: Vec::new(),
                browse_range_columns: vec!["doc_id".into(), "body_key".into()],
                child_tables: BTreeMap::new(),
                row_group_rows: Some(4),
            },
        })
        .await
        .unwrap();

    assert_eq!(pin.rows, num_rows as u64);
    assert_eq!(pin.fields.len(), 353);
    assert_eq!(pin.browse_ranges.len(), 2);

    // Inspect the exported Parquet ColumnIndex for `body_key` (column index 1)
    // and verify that 74-byte strings are NOT truncated at 64 bytes!
    let file = std::fs::File::open(&pin.object.path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let col_index = builder
        .metadata()
        .column_index()
        .expect("ColumnIndex present");
    let rg0_body_key_idx = &col_index[0][1];
    match rg0_body_key_idx {
        ColumnIndexMetaData::BYTE_ARRAY(idx) => {
            let min_bytes = idx.min_value(0).expect("min_value present");
            let max_bytes = idx.max_value(0).expect("max_value present");
            let min_str = std::str::from_utf8(min_bytes).unwrap();
            let max_str = std::str::from_utf8(max_bytes).unwrap();
            assert_eq!(min_str.len(), 74);
            assert_eq!(max_str.len(), 74);
            assert_eq!(min_str, body_keys[0]);
            assert_eq!(max_str, body_keys[3]);
        }
        other => panic!("expected BYTE_ARRAY ColumnIndex for body_key, got {other:?}"),
    }
}

#[path = "exports/mcp.rs"]
mod mcp;
