//! Source-only updates through the configured writer, selection and reader.
use super::*;
use crate::export::{ExportField, ExportPin, ExportPolicy, ExportRows, ExportSpec};
use datafusion::physical_plan::SendableRecordBatchStream;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

type Values = Vec<(i64, Option<String>)>;

fn input(path: &Path, rows: &[(i64, Option<&str>)], keys_only: bool) -> Result<InputSpec> {
    let fields = if keys_only {
        vec![Field::new("counter", DataType::Int64, false)]
    } else {
        vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("counter", DataType::Int64, false),
            Field::new("private_canary", DataType::Utf8, false),
        ]
    };
    let schema = Arc::new(Schema::new(fields));
    let count: arrow::array::ArrayRef =
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0)));
    let arrays = if keys_only {
        vec![count]
    } else {
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef,
            count,
            Arc::new(StringArray::from(vec![
                "synthetic-private-canary";
                rows.len()
            ])) as arrow::array::ArrayRef,
        ]
    };
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path)?.0,
    })
}

fn expected(rows: &[(i64, Option<&str>)]) -> Values {
    let mut rows = rows
        .iter()
        .map(|(key, text)| (*key, text.map(str::to_owned)))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn values(mut stream: SendableRecordBatchStream, exported: bool) -> Result<Values> {
    let schema = stream.schema();
    let fields = if exported {
        vec![
            ("counter", 2, DataType::Int64, false),
            ("text", 1, DataType::Utf8, true),
        ]
    } else {
        vec![
            ("text", 1, DataType::Utf8, true),
            ("counter", 2, DataType::Int64, false),
            ("private_canary", 3, DataType::Utf8, false),
        ]
    };
    assert_eq!(schema.fields().len(), fields.len());
    for (field, (name, id, datatype, nullable)) in schema.fields().iter().zip(fields) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &datatype);
        assert_eq!(field.is_nullable(), nullable);
        assert_eq!(field.metadata()[PARQUET_FIELD_ID_META_KEY], id.to_string());
    }
    let counter = schema.index_of("counter")?;
    let text = schema.index_of("text")?;
    let mut output = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if output.len().saturating_add(batch.num_rows()) > 5 {
            return Err(Error::refusal(
                "provider_evidence",
                "synthetic update read exceeded five rows",
            ));
        }
        let count = batch
            .column(counter)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(batch.column(text), &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(count.null_count(), 0);
        for row in 0..batch.num_rows() {
            output.push((
                count.value(row),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
            ));
        }
    }
    output.sort();
    Ok(output)
}

async fn processing(engine: &Engine, spec: &UpdateSpec) -> Result<(i64, PhaseResult)> {
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&spec.dataset).await?;
    let operation: Operation = lifecycle
        .get(&view, &format!("operation/{}", spec.operation))
        .await?
        .unwrap();
    let key = phase_key(
        &OperationRequest::Update(spec.clone()),
        &operation.request_digest,
        operation.eligibility,
        "processing",
        operation.base_manifest.as_ref(),
        operation.schema_key.as_ref(),
    )?;
    let rows = lifecycle.rows(&view).await?;
    let (revision, _, phase) = rows.get_record(&key)?.unwrap();
    Ok((revision, phase))
}

struct Prepared {
    initial: ReleasePin,
    first: ReleasePin,
    second: ReleasePin,
    first_request: UpdateSpec,
    second_request: UpdateSpec,
    export: ExportPin,
    first_processing: (i64, PhaseResult),
}

async fn prepare(engine: &Engine, dataset: &Id, producer: &Path) -> Result<Prepared> {
    let original = input(
        &producer.join("initial.parquet"),
        &[
            (1, Some("same")),
            (1, Some("same")),
            (2, None),
            (3, Some("")),
            (4, Some("keep")),
        ],
        false,
    )?;
    let initial = engine
        .run(&RunSpec {
            dataset: dataset.clone(),
            operation: Id::new("initial")?,
            base: None,
            relationship: None,
            input: original.path.clone(),
            expected_digest: original.digest,
            sql: "SELECT * FROM input".into(),
            definition: "source-update-v1".into(),
        })
        .await?;
    std::fs::remove_file(&original.path)?;
    let first_request = UpdateSpec {
        dataset: dataset.clone(),
        operation: Id::new("update-one")?,
        definition: "source-update-v1".into(),
        base: initial.id.clone(),
        keys: vec!["counter".into()],
        replacements: input(
            &producer.join("first.parquet"),
            &[(1, Some("changed")), (1, Some("changed")), (1, None)],
            false,
        )?,
        deletions: Some(input(
            &producer.join("deleted.parquet"),
            &[(2, None)],
            true,
        )?),
        relationship: None,
    };
    let stopped = engine
        .update_until(&first_request, "processing")
        .await
        .unwrap_err();
    if stopped.code() != "injected_failure" {
        return Err(stopped);
    }
    let first_processing = processing(engine, &first_request).await?;
    assert_eq!(first_processing.1.metrics.rows_removed, 3);
    assert_eq!(first_processing.1.metrics.rows_added, 3);
    let changes = first_processing.1.changes.as_ref().unwrap();
    assert_eq!(changes.key_field_ids, vec![2]);
    assert_eq!(changes.base_manifest, initial.manifest);
    assert_eq!(
        changes.before_metadata,
        initial.dataset.groups["source"].metadata
    );
    let budget = &engine.inner.lifecycle.budget;
    assert_eq!(
        values(
            read::read_pin_with_objects(&engine.inner.objects, &changes.removed, budget).await?,
            false
        )
        .await?,
        expected(&[(1, Some("same")), (1, Some("same")), (2, None)])
    );
    assert_eq!(
        values(
            read::read_pin_with_objects(&engine.inner.objects, &changes.added, budget).await?,
            false
        )
        .await?,
        expected(&[(1, Some("changed")), (1, Some("changed")), (1, None)])
    );
    std::fs::remove_file(&first_request.replacements.path)?;
    std::fs::remove_file(&first_request.deletions.as_ref().unwrap().path)?;
    let namespace = crate::catalog::lifecycle_ident(dataset).namespace().clone();
    let tables = engine
        .inner
        .lifecycle
        .catalog
        .list_tables(&namespace)
        .await?
        .into_iter()
        .map(|table| table.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let stopped = engine
        .update_until(&first_request, "processing")
        .await
        .unwrap_err();
    if stopped.code() != "injected_failure" {
        return Err(stopped);
    }
    // Repeating a writer would create fresh role tables. The unchanged namespace
    // and exact phase revision/pin falsify repeating acknowledged processing.
    assert_eq!(
        engine
            .inner
            .lifecycle
            .catalog
            .list_tables(&namespace)
            .await?
            .into_iter()
            .map(|table| table.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        tables
    );
    assert_eq!(processing(engine, &first_request).await?, first_processing);
    let first = engine.update(&first_request).await?;
    assert_eq!(
        first.dataset.groups["source"],
        *first_processing.1.table.as_ref().unwrap()
    );
    assert_eq!(
        values(engine.read_group(&first, "source").await?, false).await?,
        expected(&[
            (1, Some("changed")),
            (1, Some("changed")),
            (1, None),
            (3, Some("")),
            (4, Some("keep"))
        ])
    );
    let second_request = UpdateSpec {
        dataset: dataset.clone(),
        operation: Id::new("update-two")?,
        base: first.id.clone(),
        replacements: input(
            &producer.join("second.parquet"),
            &[(1, Some("last")), (1, Some("last")), (1, None)],
            false,
        )?,
        deletions: None,
        relationship: None,
        ..first_request.clone()
    };
    let second = engine.update(&second_request).await?;
    std::fs::remove_file(&second_request.replacements.path)?;
    let second_processing = processing(engine, &second_request).await?;
    assert!(second_processing.1.metrics.files_reused > 0);
    assert!(second_processing.1.metrics.bytes_reused > 0);
    assert_eq!(second_processing.1.metrics.rows_removed, 3);
    assert_eq!(second_processing.1.metrics.rows_added, 3);
    assert_eq!(
        second_processing.1.changes.as_ref().unwrap().key_field_ids,
        vec![2]
    );
    assert!(
        second.dataset.groups["source"]
            .objects
            .iter()
            .any(|object| object.path.ends_with(".parquet")
                && first.dataset.groups["source"].objects.contains(object))
    );
    assert_eq!(
        second.dataset.parent_manifest.as_ref(),
        Some(&first.manifest)
    );
    let export = engine
        .export(&ExportSpec {
            dataset: dataset.clone(),
            operation: Id::new("download")?,
            source: second.version_ref(),
            channel: Id::new("public")?,
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec!["counter".into(), "text".into()],
            },
        })
        .await?;
    assert_eq!(export.rows, 5);
    assert_eq!(
        export.fields,
        vec![
            ExportField {
                id: 2,
                name: "counter".into(),
                data_type: "Int64".into(),
                nullable: false
            },
            ExportField {
                id: 1,
                name: "text".into(),
                data_type: "Utf8".into(),
                nullable: true
            }
        ]
    );
    let mut known = BTreeMap::new();
    for release in [&initial, &first, &second] {
        for object in release_objects(release) {
            known.insert(object.path.clone(), object.bytes);
        }
    }
    for phase in [&first_processing.1, &second_processing.1] {
        for object in &phase.outputs {
            known.insert(object.path.clone(), object.bytes);
        }
    }
    known.insert(export.object.path.clone(), export.object.bytes);
    let known_retained_bytes: u64 = known.values().sum();
    println!(
        "{}",
        json!({"phase": "updates_prepared", "dataset": dataset, "initial": initial.manifest, "first": first.manifest, "second": second.manifest, "metrics": second_processing.1.metrics, "export": export.object, "known_retained_bytes": known_retained_bytes, "unlisted_attempt_and_catalog_bytes_excluded": true, "remote_cleanup": false})
    );
    Ok(Prepared {
        initial,
        first,
        second,
        first_request,
        second_request,
        export,
        first_processing,
    })
}

async fn read_back(engine: &Engine, prepared: &Prepared) -> Result<()> {
    let dataset = &prepared.second.dataset.dataset;
    assert_eq!(
        engine.update(&prepared.first_request).await?,
        prepared.first
    );
    assert_eq!(
        engine.update(&prepared.second_request).await?,
        prepared.second
    );
    assert_eq!(engine.latest(dataset).await?, prepared.second);
    assert_eq!(
        processing(engine, &prepared.first_request).await?,
        prepared.first_processing
    );
    assert_eq!(
        values(engine.read_group(&prepared.initial, "source").await?, false).await?,
        expected(&[
            (1, Some("same")),
            (1, Some("same")),
            (2, None),
            (3, Some("")),
            (4, Some("keep"))
        ])
    );
    let final_values = expected(&[
        (1, Some("last")),
        (1, Some("last")),
        (1, None),
        (3, Some("")),
        (4, Some("keep")),
    ]);
    assert_eq!(
        values(engine.read_group(&prepared.second, "source").await?, false).await?,
        final_values
    );
    assert_eq!(
        engine.export_pin(dataset, &prepared.export.id).await?,
        prepared.export
    );
    assert_eq!(
        values(engine.read_export(&prepared.export).await?, true).await?,
        final_values
    );
    compare_versions(engine, prepared).await?;
    Ok(())
}

async fn compare_versions(engine: &Engine, prepared: &Prepared) -> Result<()> {
    let started = std::time::Instant::now();
    let selected = engine.latest(&prepared.second.dataset.dataset).await?;
    for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
        for (before, after, added, removed) in [
            (&prepared.initial, &prepared.second, 3, 3),
            (&prepared.second, &prepared.initial, 3, 3),
            (
                &prepared.first,
                &prepared.second,
                if identity == ComparisonIdentity::Values {
                    2
                } else {
                    3
                },
                if identity == ComparisonIdentity::Values {
                    2
                } else {
                    3
                },
            ),
            (&prepared.second, &prepared.second, 0, 0),
        ] {
            let mut reference = None;
            for force_full in [true, false] {
                let options = ComparisonOptions {
                    identity,
                    force_full,
                };
                let counts = engine
                    .compare_with_options(before, after, "source", &options)
                    .await?;
                assert_eq!(counts.added_rows, Some(added));
                assert_eq!(counts.removed_rows, Some(removed));
                let mut stream = engine.changes(before, after, "source", &options).await?;
                let descriptor: ChangeDescriptor =
                    serde_json::from_str(&stream.schema().metadata()["docspec.change"])?;
                assert_eq!(descriptor, counts.descriptor);
                let mut members = Vec::new();
                while let Some(batch) = stream.try_next().await? {
                    if members.len() + batch.num_rows() > 10 {
                        return Err(Error::refusal(
                            "provider_evidence",
                            "comparison fixture exceeded ten changed occurrences",
                        ));
                    }
                    let fields = batch
                        .column_by_name("values")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .unwrap();
                    let side = batch
                        .column_by_name("side")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let file = batch
                        .column_by_name("file")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let row = batch
                        .column_by_name("row")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let key = fields
                        .column_by_name("counter")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let text = fields
                        .column_by_name("text")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for index in 0..batch.num_rows() {
                        members.push((
                            side.value(index).to_owned(),
                            file.value(index).to_owned(),
                            row.value(index),
                            key.value(index),
                            (!text.is_null(index)).then(|| text.value(index).to_owned()),
                        ));
                    }
                }
                members.sort();
                assert_eq!(members.len() as u64, added + removed);
                if let Some(reference) = &reference {
                    assert_eq!(&members, reference);
                } else {
                    reference = Some(members);
                }
                if !force_full {
                    let expected = if before == after {
                        ComparisonPath::Unchanged
                    } else if before.id == prepared.second.id {
                        ComparisonPath::Full
                    } else {
                        ComparisonPath::ForwardChanges
                    };
                    assert_eq!(descriptor.path, expected);
                }
            }
        }
    }
    assert_eq!(
        engine.latest(&prepared.second.dataset.dataset).await?,
        selected
    );
    println!(
        "{}",
        json!({
            "phase": "source_comparisons_passed",
            "seconds": started.elapsed().as_secs_f64(),
            "exact_count_and_addressed_stream_agreement": true,
            "duplicate_null_and_reverse_cases": true,
            "request_counts_measured": false,
            "selection_unchanged": true
        })
    );
    Ok(())
}

fn limits() -> Limits {
    Limits {
        compute_threads: 1,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
    }
}

#[tokio::test]
async fn source_update_location_path_reuses_processing_and_shares_files() {
    let root = tempfile::tempdir().unwrap();
    let producer = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let prepared = prepare(
        &engine,
        &Id::new("source-updates").unwrap(),
        producer.path(),
    )
    .await
    .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    drop(producer);
    let cold = Engine::open(root.path(), limits()).await.unwrap();
    read_back(&cold, &prepared).await.unwrap();
    cold.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a fresh qualification UUID and isolated live R2/Basin configuration"]
async fn native_remote_updates_export_and_read_from_a_fresh_host() {
    provider_updates(false).await;
}

#[tokio::test]
#[ignore = "requires the exact retained failed qualification UUID and isolated live R2/Basin configuration"]
async fn retained_remote_update_resumes_then_exports_and_reads_from_a_fresh_host() {
    provider_updates(true).await;
}

async fn provider_updates(resume: bool) {
    if let Err(error) = qualify_updates(resume).await {
        eprintln!(
            "{}",
            json!({"phase": "update_failed", "code": error.code()})
        );
        panic!("isolated update qualification failed; retain its references");
    }
}

async fn qualify_updates(resume: bool) -> Result<()> {
    let name = env("DOCSPEC_QUALIFICATION_ID")?;
    let suffix = name
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("provider_scope", "fresh qualification UUID required"))?;
    if uuid::Uuid::parse_str(suffix)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(suffix)
    {
        return Err(Error::refusal(
            "provider_scope",
            "canonical qualification UUID required",
        ));
    }
    let dataset = Id::new(&name)?;
    let prefix = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{name}/engine-update",
        env("DOCSPEC_S3_BUCKET")?
    );
    let remote = connect(&prefix).await?;
    let lifecycle = crate::catalog::lifecycle_ident(&dataset);
    let exists = remote
        .catalog
        .namespace_exists(lifecycle.namespace())
        .await?;
    if exists != resume {
        return Err(Error::refusal(
            "provider_scope",
            "qualification namespace does not match explicit fresh or retained mode",
        ));
    }
    println!(
        "{}",
        json!({"phase": "updates_start", "dataset": dataset, "prefix": prefix, "resume_retained": resume})
    );
    let host = tempfile::tempdir()?;
    let producer = tempfile::tempdir()?;
    let engine = Engine::with_remote(host.path(), remote, limits()).await?;
    let retained_processing = if resume {
        // This continuation is limited to the recorded first-update failure.
        // Check exact retained state before creating any new provider objects.
        assert_eq!(engine.latest(&dataset).await?.id, Id::new("initial")?);
        let view = engine.inner.lifecycle.open(&dataset).await?;
        let request: OperationRequest = engine
            .inner
            .lifecycle
            .get(&view, "request/update-one")
            .await?
            .ok_or_else(|| Error::refusal("provider_scope", "retained update request is absent"))?;
        let OperationRequest::Update(request) = request else {
            return Err(Error::refusal(
                "provider_scope",
                "retained request is not a source update",
            ));
        };
        Some(processing(&engine, &request).await?)
    } else {
        None
    };
    let prepared = prepare(&engine, &dataset, producer.path()).await?;
    if let Some(retained) = retained_processing {
        assert_eq!(retained, prepared.first_processing);
    }
    engine.shutdown().await?;
    drop(engine);
    drop(producer);
    // Fresh local custody shares only the configured remote catalog and objects.
    let cold_host = tempfile::tempdir()?;
    let cold = Engine::with_remote(cold_host.path(), connect(&prefix).await?, limits()).await?;
    read_back(&cold, &prepared).await?;
    assert_eq!(
        cold.inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    cold.shutdown().await?;
    println!(
        "{}",
        json!({"phase": "updates_passed", "dataset": dataset, "manifest": prepared.second.manifest, "export": prepared.export.object, "fresh_host_initial_build_queries": 0, "remote_cleanup": false, "request_counts_measured": false})
    );
    Ok(())
}
