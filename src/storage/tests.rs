use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};

fn data_file_month_partition(
    spec: &iceberg::spec::PartitionSpec,
    file: &DataFile,
) -> Result<Option<i32>> {
    use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
    validate_partition_tuple(spec, file)?;
    let mut months = spec
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.transform == Transform::Month);
    let Some((idx, _)) = months.next() else {
        return Ok(None);
    };
    if months.next().is_some() {
        return Err(Error::refusal(
            "unsupported_write_layout",
            "month pruning requires exactly one month partition field",
        ));
    }
    Ok(match &file.partition()[idx] {
        Some(Literal::Primitive(PrimitiveLiteral::Int(month))) => Some(*month),
        _ => None,
    })
}

fn assert_json_digest<T: serde::Serialize + ?Sized>(value: &T) {
    let bytes = serde_json::to_vec(value).unwrap();
    assert_eq!(json_digest(value).unwrap(), blake3::hash(&bytes));
}

#[test]
fn json_digest_preserves_typed_request_serialization() {
    use crate::{export::ExportSpec, model::GroupDefinition};

    // Freeze compact struct ordering as well as compare the previous digest path.
    let definition_json = r#"{"group":"classification","definition":"révision-東京","fields":[{"id":10,"name":"label\nquoted\"","expression":"coalesce(text, '🦀')"}]}"#;
    let definition: GroupDefinition = serde_json::from_str(definition_json).unwrap();
    assert_eq!(
        serde_json::to_vec(&definition).unwrap(),
        definition_json.as_bytes()
    );
    assert_json_digest(&definition);
    let processor = "docspec.scalar-backfill.v2";
    assert_json_digest(&(processor, &definition));
    let bindings = BTreeMap::from([("text", 2), ("language", 3)]);
    assert_json_digest(&(
        processor,
        &definition,
        bindings,
        vec![serde_json::json!({"name": "évidence", "nested": [null, true, 3]})],
    ));

    let export_json = r#"{"dataset":"comments","operation":"export-1","source":{"dataset":"comments","version":"v1","manifest":{"owner":"comments","path":"/retained/東京/manifest.json","digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bytes":123}},"channel":"download","expected":null,"policy":{"group":"source","rows":"all","columns":["id","résumé"]}}"#;
    let request: ExportSpec = serde_json::from_str(export_json).unwrap();
    assert_eq!(
        serde_json::to_vec(&request).unwrap(),
        export_json.as_bytes()
    );
    assert_json_digest(&request);
    assert_json_digest(&(&request.source.manifest, "dependency-digest"));
}

#[test]
fn json_digest_preserves_nested_maps_unicode_and_unsized_values() {
    let nested = BTreeMap::from([
        (
            "z-last",
            serde_json::json!({"array": [null, true, -42, 1.25]}),
        ),
        (
            "a-first",
            serde_json::json!({"text": "é東京🦀\n\t\"\\", "empty": {}}),
        ),
    ]);
    assert_json_digest(&nested);
    assert_json_digest(&serde_json::Value::Null);
    assert_json_digest(&[] as &[u64]);
    assert_json_digest(&[0, 1, u64::MAX] as &[u64]);
    assert_json_digest("é東京🦀\n\t\"\\");
    // Cross BLAKE3 chunk boundaries without depending on serializer write sizes.
    assert_json_digest(&"é東京🦀".repeat(1024));
}

#[test]
fn json_digest_preserves_serialization_errors() {
    struct FailAfterField;

    impl serde::Serialize for FailAfterField {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeStruct;
            let mut fields = serializer.serialize_struct("FailAfterField", 2)?;
            fields.serialize_field("first", "already serialized")?;
            Err(serde::ser::Error::custom("injected serialization failure"))
        }
    }

    fn compare_errors(value: &impl serde::Serialize) {
        let buffered = serde_json::to_vec(value).unwrap_err();
        let streamed = json_digest(value).unwrap_err();
        assert_eq!(streamed.classify(), buffered.classify());
        assert_eq!(streamed.line(), buffered.line());
        assert_eq!(streamed.column(), buffered.column());
        assert_eq!(streamed.to_string(), buffered.to_string());
        let buffered = Error::from(buffered);
        let streamed = Error::from(streamed);
        assert_eq!(streamed.code(), buffered.code());
        assert_eq!(streamed.code(), "invalid_json");
        assert_eq!(
            std::error::Error::source(&streamed).unwrap().to_string(),
            std::error::Error::source(&buffered).unwrap().to_string()
        );
    }

    compare_errors(&FailAfterField);
    compare_errors(&BTreeMap::from([((1, 2), "invalid JSON map key")]));
}

#[test]
fn digest_matches_complete_bytes_for_empty_and_multi_buffer_files() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bytes");
    // Cross both the maintained reader's buffer and the previous loop's buffer.
    for size in [0, 1, 65_535, 65_536, 65_537, 2 * 1024 * 1024 + 17] {
        let bytes: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            digest(&path).unwrap(),
            (blake3::hash(&bytes).to_hex().to_string(), size as u64)
        );
    }
}

#[test]
fn digest_reader_retries_interruptions_and_accepts_short_reads() {
    struct InterruptedReader<'a> {
        remaining: &'a [u8],
        interrupt: bool,
    }

    impl Read for InterruptedReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.interrupt {
                self.interrupt = false;
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            self.interrupt = true;
            let limit = buffer.len().min(7);
            self.remaining.read(&mut buffer[..limit])
        }
    }

    let bytes = b"short reads and repeated interruptions preserve every byte";
    let reader = InterruptedReader {
        remaining: bytes,
        interrupt: true,
    };
    assert_eq!(
        digest_reader(reader).unwrap(),
        (blake3::hash(bytes).to_hex().to_string(), bytes.len() as u64)
    );
}

#[test]
fn digest_reader_preserves_io_failure_after_partial_input() {
    struct FailedReader;

    impl Read for FailedReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected read failure",
            ))
        }
    }

    let reader = std::io::Cursor::new(b"partial bytes").chain(FailedReader);
    let error = digest_reader(reader).unwrap_err();
    assert_eq!(error.code(), "io");
    let cause = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(cause.to_string(), "injected read failure");
}

#[test]
fn digest_preserves_file_open_failure() {
    let root = tempfile::tempdir().unwrap();
    let error = digest(&root.path().join("missing")).unwrap_err();
    assert_eq!(error.code(), "io");
    let cause = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn canceled_inventory_checks_keep_worker_owned_metadata_admission() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        for full in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("retained");
            std::fs::write(&path, b"retained bytes").unwrap();
            let object = object(&Id::new("owner").unwrap(), &path).unwrap();
            let budget = Budget::new(crate::budget::Limits {
                memory_bytes: 8 << 20,
                max_metadata_bytes: 1 << 20,
                compute_threads: 1,
                ..crate::budget::Limits::default()
            })
            .unwrap();
            let (started, ready) = tokio::sync::oneshot::channel();
            let (resume, paused) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = paused.recv();
            });
            ready.await.unwrap();
            let worker_budget = budget.clone();
            let caller = tokio::spawn(async move {
                if full {
                    verify_objects(vec![object], &worker_budget).await
                } else {
                    check_available(vec![object], &worker_budget).await
                }
            });
            let except_io_buffer = budget.limits.memory_bytes / 2 - (1 << 20);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while budget.reserve(except_io_buffer).is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert!(
                budget.reserve(except_io_buffer).is_err(),
                "queued worker still owns inventory beyond its fixed I/O buffer"
            );
            resume.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while budget.reserve(budget.limits.memory_bytes / 2).is_err() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    });
}

fn batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}

async fn values(pin: &TablePin, budget: &Budget) -> Vec<Option<i64>> {
    let table = open_pin(pin, budget).await.unwrap();
    let stream = read_batches(&table, budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    values.sort();
    values
}

#[tokio::test]
async fn candidate_snapshots_share_unchanged_files_with_complete_owned_inventories() {
    let root = tempfile::TempDir::new().unwrap();
    let catalog = local_catalog(root.path()).await.unwrap();
    let namespace = iceberg::NamespaceIdent::new("sharing".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let owner = Id::new("source").unwrap();
    let derived_owner = Id::new("derived").unwrap();
    let first = batch(vec![Some(1), Some(1)]);
    let schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(first.schema().as_ref()).unwrap();
    let source = catalog
        .create_table(
            &namespace,
            iceberg::TableCreation::builder()
                .name("source".into())
                .location(root.path().join("source").to_string_lossy().into_owned())
                .schema(schema.clone())
                .properties(table_properties())
                .build(),
        )
        .await
        .unwrap();
    let original_files = write_batches(&source, futures::stream::iter([Ok(first)]), &budget)
        .await
        .unwrap();
    let source = append(&source, catalog.as_ref(), original_files)
        .await
        .unwrap();
    let second = batch(vec![Some(2), None]);
    let stable = write_batches(&source, futures::stream::iter([Ok(second)]), &budget)
        .await
        .unwrap();
    assert_eq!(stable.len(), 1);
    let stable_path = stable[0].file_path().to_owned();
    let stable_before = digest(Path::new(&stable_path)).unwrap();
    let stable_modified = std::fs::metadata(&stable_path).unwrap().modified().unwrap();
    let source = append(&source, catalog.as_ref(), stable.clone())
        .await
        .unwrap();
    let original = pin(&owner, &source, 4, &budget, &[]).await.unwrap();
    let unique_metadata_bytes = original
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".avro"))
        .map(|object| object.bytes as usize)
        .sum::<usize>();
    let inventory_limit = unique_metadata_bytes.max(original.objects.len() * 512);
    let tight = Budget::new(crate::budget::Limits {
        max_metadata_bytes: inventory_limit,
        ..crate::budget::Limits::default()
    })
    .unwrap();
    // The earlier snapshot repeats a manifest. Charge/read each immutable
    // manifest once, while preserving every retained snapshot's references.
    assert_eq!(snapshot_files(&source, &tight).await.unwrap().data.len(), 2);

    let candidate = catalog
        .create_table(
            &namespace,
            iceberg::TableCreation::builder()
                .name("candidate".into())
                .location(root.path().join("candidate").to_string_lossy().into_owned())
                .schema(schema)
                .properties(table_properties())
                .build(),
        )
        .await
        .unwrap();
    let replacement = batch(vec![Some(3), Some(3)]);
    let mut candidate_files = write_batches(
        &candidate,
        futures::stream::iter([Ok(replacement)]),
        &budget,
    )
    .await
    .unwrap();
    candidate_files.extend(stable);
    let candidate = append(&candidate, catalog.as_ref(), candidate_files)
        .await
        .unwrap();
    assert_eq!(
        pin(&derived_owner, &candidate, 4, &budget, &[])
            .await
            .unwrap_err()
            .code(),
        "unprotected_reference"
    );
    let mut conflicting = original.objects.clone();
    let mut conflict = original
        .objects
        .iter()
        .find(|object| object.path == stable_path)
        .unwrap()
        .clone();
    conflict.owner = derived_owner.clone();
    conflicting.push(conflict);
    assert_eq!(
        pin(&derived_owner, &candidate, 4, &budget, &conflicting)
            .await
            .unwrap_err()
            .code(),
        "conflicting_owner"
    );
    let result = pin(&derived_owner, &candidate, 4, &budget, &original.objects)
        .await
        .unwrap();
    assert_ne!(result.snapshot, original.snapshot);
    assert_eq!(result.schema_id, original.schema_id);
    assert_eq!(
        result
            .objects
            .iter()
            .find(|object| object.path == stable_path)
            .unwrap()
            .owner,
        owner
    );
    assert_eq!(digest(Path::new(&stable_path)).unwrap(), stable_before);
    assert_eq!(
        std::fs::metadata(&stable_path).unwrap().modified().unwrap(),
        stable_modified
    );
    let original_data = snapshot_files(&source, &budget).await.unwrap();
    let replaced = original_data
        .data
        .iter()
        .find(|file| file.file_path() != stable_path)
        .unwrap();
    assert!(
        !result
            .objects
            .iter()
            .any(|object| object.path == replaced.file_path())
    );
    // An opaque statistics sidecar tests reference closure only; this does
    // not claim qualification of the Puffin statistics format itself.
    let statistics_path = root.path().join("external-statistics.puffin");
    write_new(&statistics_path, b"opaque sidecar").unwrap();
    let statistics = object(&owner, &statistics_path).unwrap();
    let tx = Transaction::new(&candidate);
    let with_statistics = tx
        .update_statistics()
        .set_statistics(iceberg::spec::StatisticsFile {
            snapshot_id: candidate.metadata().current_snapshot_id().unwrap(),
            statistics_path: statistics.path.clone(),
            file_size_in_bytes: statistics.bytes as i64,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: vec![],
        })
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    assert_eq!(
        pin(
            &derived_owner,
            &with_statistics,
            4,
            &budget,
            &original.objects
        )
        .await
        .unwrap_err()
        .code(),
        "unprotected_reference"
    );
    let mut inherited = original.objects.clone();
    inherited.push(statistics.clone());
    let mut statistics_pin = pin(&derived_owner, &with_statistics, 4, &budget, &inherited)
        .await
        .unwrap();
    assert!(statistics_pin.objects.contains(&statistics));
    statistics_pin
        .objects
        .retain(|object| object.path != statistics.path);
    assert_eq!(
        crate::engine::verify_table(&statistics_pin, &budget)
            .await
            .unwrap_err()
            .code(),
        "incomplete_inventory"
    );
    drop(catalog);
    std::fs::rename(
        root.path().join("catalog.sqlite"),
        root.path().join("catalog.unavailable"),
    )
    .unwrap();
    crate::engine::verify_table(&original, &budget)
        .await
        .unwrap();
    crate::engine::verify_table(&result, &budget).await.unwrap();
    assert_eq!(
        values(&original, &budget).await,
        vec![None, Some(1), Some(1), Some(2)]
    );
    assert_eq!(
        values(&result, &budget).await,
        vec![None, Some(2), Some(3), Some(3)]
    );

    let mut incomplete = result.clone();
    incomplete
        .objects
        .retain(|object| object.path != stable_path);
    assert_eq!(
        crate::engine::verify_table(&incomplete, &budget)
            .await
            .unwrap_err()
            .code(),
        "incomplete_inventory"
    );
}

#[test]
fn month_pruning_refuses_ambiguous_specs_and_validates_every_partition_field() {
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, NestedField, PartitionSpec,
        PrimitiveType, Struct, Transform, Type,
    };
    let schema = Arc::new(
        iceberg::spec::Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Date),
                )),
            ])
            .build()
            .unwrap(),
    );
    let two_months = PartitionSpec::builder(schema.clone())
        .add_partition_field("a", "a_month", Transform::Month)
        .unwrap()
        .add_partition_field("b", "b_month", Transform::Month)
        .unwrap()
        .build()
        .unwrap();
    let file = |partition: Struct| {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path("memory://f.parquet".into())
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(partition)
            .build()
            .unwrap()
    };
    let valid = file(Struct::from_iter([
        Some(Literal::int(648)),
        Some(Literal::int(649)),
    ]));
    validate_partition_tuple(&two_months, &valid).unwrap();
    // Choosing the first month field would prune by an arbitrary column.
    assert_eq!(
        data_file_month_partition(&two_months, &valid)
            .unwrap_err()
            .code(),
        "unsupported_write_layout"
    );
    // The second month literal was previously never inspected.
    let malformed = file(Struct::from_iter([
        Some(Literal::int(648)),
        Some(Literal::string("x")),
    ]));
    assert_eq!(
        validate_partition_tuple(&two_months, &malformed)
            .unwrap_err()
            .code(),
        "unsupported_write_layout"
    );
    let one_month = PartitionSpec::builder(schema)
        .add_partition_field("a", "a_month", Transform::Month)
        .unwrap()
        .build()
        .unwrap();
    let single = file(Struct::from_iter([Some(Literal::int(648))]));
    assert_eq!(
        data_file_month_partition(&one_month, &single).unwrap(),
        Some(648)
    );
}

#[tokio::test]
async fn interleaved_partitions_close_open_writers_within_fanout_allowance() {
    use arrow::array::{Date32Array, StringArray};
    use iceberg::{
        TableIdent,
        spec::{NestedField, PrimitiveType, Transform, Type, UnboundPartitionSpec},
    };

    let root = tempfile::TempDir::new().unwrap();
    let catalog = local_catalog(root.path()).await.unwrap();
    let namespace = iceberg::NamespaceIdent::new("fanout".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let limits = crate::budget::Limits {
        max_batch_bytes: 256 << 10,
        ..crate::budget::Limits::default()
    };
    let budget = Budget::new(limits).unwrap();
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields([
            Arc::new(NestedField::required(
                10,
                "observed_at",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                20,
                "payload",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                40,
                identity::ORIGIN_FILE,
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                41,
                identity::ORIGIN_ROW,
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(10, "observed_at_day", Transform::Day)
        .unwrap()
        .build();
    let table_dir = root.path().join("events");
    let table = register_candidate(
        catalog.as_ref(),
        &TableIdent::new(namespace.clone(), "events".into()),
        CandidateWriter::Local,
        move || fresh_partitioned_metadata(&schema, &table_dir, spec),
        &budget,
    )
    .await
    .unwrap();
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("observed_at", DataType::Date32, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new(identity::ORIGIN_FILE, DataType::Utf8, true),
        Field::new(identity::ORIGIN_ROW, DataType::Int64, true),
    ]));
    // Every batch interleaves two days, so both partition writers stay open.
    // Eight ~110 KiB batches exceed the 512 KiB (2 x batch) fanout allowance.
    let rows_per_batch = 1000;
    let batches = (0..8)
        .map(|_| {
            let days = (0..rows_per_batch)
                .map(|row| 19_723 + (row % 2) as i32)
                .collect::<Vec<_>>();
            let payload = (0..rows_per_batch)
                .map(|row| format!("{row:0>100}"))
                .collect::<Vec<_>>();
            Ok(RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Date32Array::from(days)),
                    Arc::new(StringArray::from(payload)),
                    Arc::new(StringArray::new_null(rows_per_batch)),
                    Arc::new(arrow::array::Int64Array::new_null(rows_per_batch)),
                ],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let files = write_batches(&table, futures::stream::iter(batches), &budget)
        .await
        .unwrap();
    let rows: u64 = files.iter().map(DataFile::record_count).sum();
    assert_eq!(rows, 8 * rows_per_batch as u64);
    // Without the allowance one writer per day stays open for the whole stream
    // (exactly two files). Bounded fanout finishes files part-way through.
    assert!(files.len() > 2, "{} files", files.len());
    for file in &files {
        assert_eq!(file.partition().fields().len(), 1);
    }
}

#[tokio::test]
async fn month_partitioned_batches_write_distinct_data_files_prune_by_month_and_preserve_occurrences()
 {
    use arrow::{
        array::{Date32Array, StringArray, TimestampMicrosecondArray},
        datatypes::TimeUnit,
    };
    use iceberg::{
        TableIdent,
        spec::{
            Literal, NestedField, PrimitiveType, Struct, Transform, Type, UnboundPartitionSpec,
        },
    };

    // Verify days_to_months_since_epoch across epoch, leap years, and negative dates.
    assert_eq!(days_to_months_since_epoch(0), 0); // 1970-01-01
    assert_eq!(days_to_months_since_epoch(-1), -1); // 1969-12-31
    assert_eq!(days_to_months_since_epoch(19_723), 648); // 2024-01-01
    assert_eq!(days_to_months_since_epoch(19_782), 649); // 2024-02-29 (leap day)
    assert_eq!(days_to_months_since_epoch(19_783), 650); // 2024-03-01

    let root = tempfile::TempDir::new().unwrap();
    let catalog = local_catalog(root.path()).await.unwrap();
    let namespace = iceberg::NamespaceIdent::new("news_lake".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields([
            Arc::new(NestedField::required(
                10,
                "article_id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                20,
                "observed_at",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                30,
                "recorded_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
            Arc::new(NestedField::optional(
                40,
                identity::ORIGIN_FILE,
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                41,
                identity::ORIGIN_ROW,
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(20, "observed_at_month", Transform::Month)
        .unwrap()
        .build();
    let table_dir = root.path().join("articles");
    let schema_for_create = schema.clone();
    let spec_for_create = spec.clone();
    let dir_for_create = table_dir.clone();
    let table = register_candidate(
        catalog.as_ref(),
        &TableIdent::new(namespace.clone(), "articles".into()),
        CandidateWriter::Local,
        move || fresh_partitioned_metadata(&schema_for_create, &dir_for_create, spec_for_create),
        &budget,
    )
    .await
    .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("article_id", DataType::Int64, false),
        Field::new("observed_at", DataType::Date32, false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(identity::ORIGIN_FILE, DataType::Utf8, true),
        Field::new(identity::ORIGIN_ROW, DataType::Int64, true),
    ]));
    // Interleave rows from 2024-01 (days 19723, 19730 -> month 648),
    // 2024-02 (day 19782 -> month 649), and 2024-03 (day 19783 -> month 650)
    // in a single batch to exercise per-partition splitting and occurrence retention.
    let mixed_batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![101, 201, 102, 301])),
            Arc::new(Date32Array::from(vec![19_723, 19_782, 19_730, 19_783])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(19_723_i64 * 86_400_000_000),
                    Some(19_782_i64 * 86_400_000_000),
                    Some(19_730_i64 * 86_400_000_000),
                    Some(19_783_i64 * 86_400_000_000),
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec![
                Some("origin:jan-0"),
                None,
                None,
                Some("origin:mar-0"),
            ])),
            Arc::new(Int64Array::from(vec![Some(7), None, None, Some(99)])),
        ],
    )
    .unwrap();

    let written = write_batches(&table, futures::stream::iter([Ok(mixed_batch)]), &budget)
        .await
        .unwrap();
    assert_eq!(written.len(), 3);
    let mut by_month = BTreeMap::new();
    for file in &written {
        let month = data_file_month_partition(table.metadata().default_partition_spec(), file)
            .unwrap()
            .unwrap();
        assert_eq!(
            file.partition(),
            &Struct::from_iter([Some(Literal::int(month))])
        );
        by_month.insert(month, file.record_count());
    }
    assert_eq!(by_month, BTreeMap::from([(648, 2), (649, 1), (650, 1)]));

    let table = append(&table, catalog.as_ref(), written).await.unwrap();
    let owner = Id::new("news_lake").unwrap();
    let pinned = pin(&owner, &table, 4, &budget, &[]).await.unwrap();
    assert_eq!(pinned.rows, 4);

    // Verify OccurrenceScan and read_occurrences preserve exact values and occurrence identities.
    let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
    assert_eq!(scan.files().count(), 3);
    let stream = read_occurrences(&table, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut rows = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("article_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dates = batch
            .column_by_name("observed_at")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let files = batch
            .column_by_name(OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = batch
            .column_by_name(OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.insert(
                ids.value(i),
                (
                    dates.value(i),
                    files.value(i).to_owned(),
                    positions.value(i),
                ),
            );
        }
    }
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[&101].0, 19_723);
    assert_eq!(rows[&101].1, "origin:jan-0");
    assert_eq!(rows[&101].2, 7);
    assert_eq!(rows[&301].0, 19_783);
    assert_eq!(rows[&301].1, "origin:mar-0");
    assert_eq!(rows[&301].2, 99);
    // Fallback rows 201 (input index 1) and 102 (input index 2) share the same generated
    // writer origin and keep their exact pre-split input positions 1 and 2.
    assert_eq!(rows[&201].0, 19_782);
    assert_eq!(rows[&102].0, 19_730);
    assert_eq!(rows[&201].1, rows[&102].1);
    assert!(rows[&201].1.starts_with("docspec:"));
    assert_eq!(rows[&201].2, 1);
    assert_eq!(rows[&102].2, 2);

    // Verify replacement candidate preserves the Month partition spec and shares unchanged month files.
    let replacement_dir = root.path().join("articles_replacement");
    let base_meta = table.metadata().clone();
    let rep_dir = replacement_dir.clone();
    let candidate = register_candidate(
        catalog.as_ref(),
        &TableIdent::new(namespace, "articles_replacement".into()),
        CandidateWriter::Local,
        move || replacement_metadata(&base_meta, &rep_dir),
        &budget,
    )
    .await
    .unwrap();
    assert_eq!(
        candidate.metadata().default_partition_spec(),
        table.metadata().default_partition_spec()
    );
    let jan_feb: Vec<_> = current_snapshot_files(&table, &budget)
        .await
        .unwrap()
        .data
        .into_iter()
        .filter(|file| {
            data_file_month_partition(table.metadata().default_partition_spec(), file)
                .unwrap()
                .unwrap()
                <= 649
        })
        .collect();
    assert_eq!(jan_feb.len(), 2);
    let candidate = append(&candidate, catalog.as_ref(), jan_feb).await.unwrap();
    let candidate_pin = pin(&owner, &candidate, 3, &budget, &pinned.objects)
        .await
        .unwrap();
    assert_eq!(candidate_pin.rows, 3);
}

#[tokio::test]
async fn occurrence_scan_reuses_admitted_current_snapshot_across_retained_history() {
    let root = tempfile::TempDir::new().unwrap();
    let catalog = local_catalog(root.path()).await.unwrap();
    let namespace = iceberg::NamespaceIdent::new("planning_reuse".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let owner = Id::new("planning_reuse").unwrap();
    let sample = batch(vec![Some(10), Some(20)]);
    let native_schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(sample.schema().as_ref()).unwrap();

    for historical_snapshots in [0usize, 8, 32] {
        let location = root.path().join(format!("history_{historical_snapshots}"));
        let mut historical = Vec::with_capacity(historical_snapshots);
        for index in 0..historical_snapshots {
            let stage_meta = fresh_metadata(native_schema.clone(), &location).unwrap();
            let stage = register_candidate(
                catalog.as_ref(),
                &iceberg::TableIdent::new(
                    namespace.clone(),
                    format!("stage_{historical_snapshots}_{index}"),
                ),
                CandidateWriter::Local,
                move || Ok(stage_meta),
                &budget,
            )
            .await
            .unwrap();
            let files = write_batches(
                &stage,
                futures::stream::iter([Ok(batch(vec![Some(index as i64)]))]),
                &budget,
            )
            .await
            .unwrap();
            let stage = append(&stage, catalog.as_ref(), files).await.unwrap();
            let snap = stage.metadata().current_snapshot().unwrap();
            historical.push(
                iceberg::spec::Snapshot::builder()
                    .with_snapshot_id((index + 1) as i64)
                    .with_sequence_number((index + 1) as i64)
                    .with_timestamp_ms(snap.timestamp_ms() + index as i64)
                    .with_manifest_list(snap.manifest_list().to_owned())
                    .with_summary(snap.summary().clone())
                    .with_schema_id(snap.schema_id().unwrap())
                    .build(),
            );
        }

        let base_meta = fresh_metadata(native_schema.clone(), &location).unwrap();
        let mut builder = base_meta.into_builder(None);
        for snap in historical {
            builder = builder.add_snapshot(snap).unwrap();
        }
        let seeded_meta = builder.build().unwrap().metadata;
        let table = register_candidate(
            catalog.as_ref(),
            &iceberg::TableIdent::new(namespace.clone(), format!("table_{historical_snapshots}")),
            CandidateWriter::Local,
            move || Ok(seeded_meta),
            &budget,
        )
        .await
        .unwrap();

        // Write 2 fixed current files (2 rows each = 4 rows total) in 1 current manifest.
        let mut current_files = Vec::new();
        for values in [vec![Some(1), Some(2)], vec![Some(3), None]] {
            let files = write_batches(&table, futures::stream::iter([Ok(batch(values))]), &budget)
                .await
                .unwrap();
            assert_eq!(files.len(), 1);
            current_files.extend(files);
        }
        let table = append(&table, catalog.as_ref(), current_files)
            .await
            .unwrap();
        assert_eq!(
            table.metadata().snapshots().count(),
            historical_snapshots + 1
        );

        // 1. Creation/pinning/retention boundary (`snapshot_files`) still verifies
        // full object closure across all retained snapshots (1 + historical_snapshots).
        reset_manifest_decode_count(table.metadata().uuid());
        let closure_start = std::time::Instant::now();
        let inventory = snapshot_files(&table, &budget).await.unwrap();
        let closure_elapsed = closure_start.elapsed();
        let closure_counts = manifest_decode_count(table.metadata().uuid());
        assert_eq!(
            closure_counts,
            ManifestDecodeCount {
                manifest_lists: (historical_snapshots + 1) as u64,
                manifests: (historical_snapshots + 1) as u64,
            }
        );
        assert_eq!(inventory.data.len(), 2);
        let pinned = pin(&owner, &table, 4, &budget, &[]).await.unwrap();
        assert_eq!(pinned.rows, 4);

        // 2. OccurrenceScan::open decodes ONLY the current snapshot's manifest list (1)
        // and manifest (1) once, regardless of retained snapshot history (0 / 8 / 32).
        reset_manifest_decode_count(table.metadata().uuid());
        let open_start = std::time::Instant::now();
        let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
        let open_elapsed = open_start.elapsed();
        let open_counts = manifest_decode_count(table.metadata().uuid());
        assert_eq!(
            open_counts,
            ManifestDecodeCount {
                manifest_lists: 1,
                manifests: 1,
            }
        );
        assert_eq!(scan.files().count(), 2);

        // 3. read_occurrences also decodes only the current snapshot once (1 list, 1 manifest).
        reset_manifest_decode_count(table.metadata().uuid());
        let stream = read_occurrences(&table, &budget).await.unwrap();
        futures::pin_mut!(stream);
        let mut total_rows = 0usize;
        while let Some(batch) = stream.try_next().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 4);
        assert_eq!(
            manifest_decode_count(table.metadata().uuid()),
            ManifestDecodeCount {
                manifest_lists: 1,
                manifests: 1,
            }
        );

        // 4. OccurrenceScan::values reuses an already-admitted SnapshotFiles with
        // zero additional manifest-list or manifest decodes, even after the current
        // snapshot's manifest list is removed from disk.
        let current_manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .manifest_list()
            .to_owned();
        std::fs::remove_file(&current_manifest_list).unwrap();
        reset_manifest_decode_count(table.metadata().uuid());
        let values_start = std::time::Instant::now();
        let values_scan = OccurrenceScan::values(&table, &budget, inventory)
            .await
            .unwrap();
        let values_elapsed = values_start.elapsed();
        assert_eq!(
            manifest_decode_count(table.metadata().uuid()),
            ManifestDecodeCount {
                manifest_lists: 0,
                manifests: 0,
            }
        );
        let mut values_rows = 0usize;
        for file in values_scan.files() {
            let stream = values_scan.read_file(file.file_path(), &budget).unwrap();
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await.unwrap() {
                values_rows += batch.num_rows();
            }
        }
        assert_eq!(values_rows, 4);
        eprintln!(
            "history={historical_snapshots}: closure_counts={closure_counts:?} ({closure_elapsed:?}), open_counts={open_counts:?} ({open_elapsed:?}), values_elapsed={values_elapsed:?}"
        );
    }
}

#[test]
fn split_oversized_batch_splits_skewed_utf8_and_compacts_single_row_slices() {
    use arrow::array::StringArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let limit = 4 * 1024 * 1024; // 4 MiB
    let big_a = "A".repeat(2_500_000);
    let big_b = "B".repeat(2_500_000);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
            Arc::new(StringArray::from(vec![
                "small-0", &big_a, "small-2", &big_b,
            ])),
        ],
    )
    .unwrap();
    assert!(batch.get_array_memory_size() > limit);

    // 1. Multi-row batch with two 2.5 MB Utf8 cells splits cleanly under 4 MiB per slice.
    let slices = split_oversized_batch(batch.clone(), limit).unwrap();
    assert!(slices.len() >= 2);
    let total_rows: usize = slices.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4);
    for slice in &slices {
        assert!(
            slice.get_array_memory_size() <= limit,
            "slice memory {} exceeded limit {}",
            slice.get_array_memory_size(),
            limit
        );
    }

    // 2. A 1-row zero-copy slice of the 5+ MB batch still shares the parent buffer
    // until split_oversized_batch compacts it via compact_batch_slice.
    let uncompacted_one = batch.slice(1, 1);
    assert!(uncompacted_one.get_array_memory_size() > limit);
    let compacted_slices = split_oversized_batch(uncompacted_one, limit).unwrap();
    assert_eq!(compacted_slices.len(), 1);
    assert_eq!(compacted_slices[0].num_rows(), 1);
    assert!(compacted_slices[0].get_array_memory_size() <= limit);
}
