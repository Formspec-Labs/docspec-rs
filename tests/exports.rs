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

#[path = "exports/mcp.rs"]
mod mcp;
