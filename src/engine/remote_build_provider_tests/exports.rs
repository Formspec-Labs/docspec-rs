//! Opt-in exact configured export readback; retain every remote object.

use super::*;
use crate::export::{ExportField, ExportPin, ExportPolicy, ExportRows, ExportSpec};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[tokio::test]
#[ignore = "requires a separate fresh qualification UUID and isolated live R2/Basin configuration"]
async fn native_remote_export_recovers_missing_stage_and_reads_from_a_fresh_host() {
    // Never render upstream error/config Debug: native errors can carry URLs.
    if let Err(error) = qualify_export().await {
        eprintln!(
            "{}",
            json!({"phase": "export_failed", "code": error.code()})
        );
        panic!("isolated export qualification failed; retain its recorded references");
    }
}

fn expected_fields() -> Vec<ExportField> {
    vec![
        ExportField {
            id: 2,
            name: "counter".into(),
            data_type: "Int64".into(),
            nullable: false,
        },
        ExportField {
            id: 1,
            name: "text".into(),
            data_type: "Utf8".into(),
            nullable: true,
        },
    ]
}

pub(super) async fn assert_values(engine: &Engine, pin: &ExportPin) -> Result<()> {
    assert_eq!(pin.rows, 4);
    assert_eq!(pin.fields, expected_fields());
    let mut stream = engine.read_export(pin).await?;
    let schema = stream.schema();
    assert!(schema.metadata().is_empty());
    assert_eq!(schema.fields().len(), 2);
    for (field, expected) in schema.fields().iter().zip(expected_fields()) {
        assert_eq!(field.name(), &expected.name);
        assert_eq!(format!("{:?}", field.data_type()), expected.data_type);
        assert_eq!(field.is_nullable(), expected.nullable);
        assert_eq!(field.metadata().len(), 1);
        assert_eq!(
            field.metadata()[PARQUET_FIELD_ID_META_KEY],
            expected.id.to_string()
        );
    }
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if values.len().saturating_add(batch.num_rows()) > 4 {
            return Err(Error::refusal(
                "provider_evidence",
                "export exceeded the synthetic row bound",
            ));
        }
        assert_eq!(batch.num_columns(), 2);
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(batch.column(1), &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(count.null_count(), 0);
        for row in 0..batch.num_rows() {
            values.push((
                count.value(row),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
            ));
        }
    }
    values.sort();
    // Literal independent oracle: duplicate, null and empty are distinct values.
    assert_eq!(
        values,
        vec![
            (1, Some("same".into())),
            (1, Some("same".into())),
            (2, None),
            (2, Some(String::new()))
        ]
    );
    Ok(())
}

async fn qualify_export() -> Result<()> {
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
        "s3://{}/__r2_data_catalog/docspec-qualification/{name}/engine-export",
        env("DOCSPEC_S3_BUCKET")?
    );
    let remote = connect(&prefix).await?;
    let lifecycle = crate::catalog::lifecycle_ident(&dataset);
    if remote
        .catalog
        .namespace_exists(lifecycle.namespace())
        .await?
    {
        return Err(Error::refusal(
            "provider_scope",
            "qualification namespace already exists",
        ));
    }
    println!(
        "{}",
        json!({"phase": "export_start", "dataset": dataset, "prefix": prefix, "lifecycle": lifecycle})
    );
    let producer = tempfile::tempdir()?;
    let source = producer.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("counter", DataType::Int64, false),
        Field::new("private_canary", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("same"),
                Some("same"),
                None,
                Some(""),
                Some("excluded"),
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 1, 2, 2, 3])),
            Arc::new(StringArray::from(vec!["synthetic-private-canary"; 5])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&source)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let build = RunSpec {
        dataset: dataset.clone(),
        operation: Id::new("initial")?,
        base: None,
        relationship: None,
        input: source.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&source)?.0,
        sql: "SELECT * FROM input WHERE counter < 3".into(),
        definition: "synthetic-export-v1".into(),
        partition_by: Vec::new(),
    };
    let host = tempfile::tempdir()?;
    let engine = compose(host.path(), remote)?;
    let selected = engine.run(&build).await?;
    std::fs::remove_file(&source)?;
    let spec = ExportSpec {
        dataset: dataset.clone(),
        operation: Id::new("download")?,
        source: selected.version_ref(),
        channel: Id::new("public")?,
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["counter".into(), "text".into()],
            ..ExportPolicy::default()
        },
    };
    let stopped = match engine.export_until(&spec, "transfer-uncommitted").await {
        Err(error) => error,
        Ok(_) => {
            return Err(Error::refusal(
                "provider_evidence",
                "transfer interruption was not reached",
            ));
        }
    };
    if stopped.code() != "injected_failure" {
        return Err(stopped);
    }
    let view = engine.inner.lifecycle.open(&dataset).await?;
    let records = engine.inner.lifecycle.records(&view).await?;
    let (stage_key, stage_value) = records
        .iter()
        .find(|(key, _)| key.starts_with("export-staged/download/"))
        .ok_or_else(|| Error::refusal("provider_evidence", "staged export is missing"))?;
    let stage: crate::engine::export::ExportMaterialized =
        serde_json::from_value(stage_value.clone())?;
    let validation_key = stage_key.replacen("export-staged/", "export-stage-validation/", 1);
    let stage_validation = records
        .get(&validation_key)
        .ok_or_else(|| Error::refusal("provider_evidence", "stage validation is missing"))?;
    let checked: crate::export::ExportValidation =
        serde_json::from_value(stage_validation.clone())?;
    assert_eq!(checked.object, stage.object);
    assert_eq!(checked.rows, 4);
    let (attempt_key, attempt) = records
        .iter()
        .find(|(key, _)| key.starts_with("export-attempt/download/"))
        .ok_or_else(|| {
            Error::refusal("provider_evidence", "protected export attempt is missing")
        })?;
    let destination = attempt["destination"].as_str().ok_or_else(|| {
        Error::refusal("provider_evidence", "exact export destination is missing")
    })?;
    let expected = ObjectRef {
        path: destination.to_owned(),
        ..stage.object.clone()
    };
    engine
        .inner
        .objects
        .validate_retained_path(&expected.path)?;
    assert_eq!(stage.fields, expected_fields());
    assert_eq!(stage.rows, 4);
    assert!(!records.contains_key("export-materialized/download"));
    assert!(!records.contains_key("export-validation/download"));
    assert!(!records.contains_key("export-result/download"));
    let canonical_host = std::fs::canonicalize(host.path())?;
    let stage_path = Path::new(&stage.object.path);
    let attempt_id = Id::new(attempt["id"].as_str().ok_or_else(|| {
        Error::refusal("provider_evidence", "export attempt identity is missing")
    })?)?;
    let stage_directory = canonical_host
        .join("objects")
        .join(dataset.as_str())
        .join(attempt_id.as_str());
    let host_prefix = format!("{}/", stage_directory.display());
    let stage_uuid = stage_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("stage-"))
        .and_then(|name| name.strip_suffix(".parquet"));
    if attempt["host_prefix"].as_str() != Some(host_prefix.as_str())
        || stage_path.parent() != Some(stage_directory.as_path())
        || stage_uuid.is_none_or(|name| {
            uuid::Uuid::parse_str(name)
                .map(|id| id.to_string())
                .ok()
                .as_deref()
                != Some(name)
        })
    {
        return Err(Error::refusal(
            "provider_evidence",
            "stage is not owned by this exact synthetic attempt",
        ));
    }
    println!(
        "{}",
        json!({"phase": "export_transfer_without_completion", "attempt": attempt_key, "object": expected, "rows": stage.rows})
    );
    // Controlled local fault injection after verified transfer; retain remote bytes.
    std::fs::remove_file(stage_path)?;
    let pin = engine.export(&spec).await?;
    assert_eq!(pin.object, expected);
    assert_eq!(pin.fields, expected_fields());
    assert!(!stage_path.exists());
    let after = engine
        .inner
        .lifecycle
        .records(&engine.inner.lifecycle.open(&dataset).await?)
        .await?;
    assert_eq!(after[stage_key], *stage_value);
    assert_eq!(after[&validation_key], *stage_validation);
    assert_eq!(after[attempt_key], *attempt);
    assert_eq!(engine.export(&spec).await?, pin);
    assert_eq!(engine.export_pin(&dataset, &spec.operation).await?, pin);
    assert_values(&engine, &pin).await?;

    let later_spec = ExportSpec {
        operation: Id::new("download-next")?,
        expected: Some(pin.id.clone()),
        ..spec.clone()
    };
    let later = engine.export(&later_spec).await?;
    assert_eq!(engine.latest_export(&dataset, &spec.channel).await?, later);
    assert_eq!(engine.export(&spec).await?, pin);
    assert_eq!(engine.latest_export(&dataset, &spec.channel).await?, later);
    assert!(!stage_path.exists());
    assert_eq!(
        engine
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    println!(
        "{}",
        json!({"phase": "export_recovered_and_channel_advanced", "pin": pin, "later": later, "stage_missing": true, "initial_build_queries": 1})
    );
    engine.shutdown().await?;
    drop(engine);
    drop(host);
    drop(producer);

    let cold_host = tempfile::tempdir()?;
    let cold = compose(cold_host.path(), connect(&prefix).await?)?;
    assert_eq!(cold.export_pin(&dataset, &pin.id).await?, pin);
    assert_eq!(cold.latest_export(&dataset, &spec.channel).await?, later);
    assert_values(&cold, &pin).await?;
    let mut wrong = pin.clone();
    wrong.object.path = format!(
        "s3://{}/outside-qualified-prefix/export.parquet",
        env("DOCSPEC_S3_BUCKET")?
    );
    let refused = match cold.read_export(&wrong).await {
        Err(error) => error,
        Ok(_) => {
            return Err(Error::refusal(
                "provider_evidence",
                "changed export pin was accepted",
            ));
        }
    };
    assert_eq!(refused.code(), "invalid_export");
    assert_eq!(
        cold.inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    cold.shutdown().await?;
    println!(
        "{}",
        json!({"phase": "export_passed", "dataset": dataset, "pin": pin, "later": later, "fresh_host_queries": 0, "rows": 4, "remote_cleanup": false, "transfer_request_counts_measured": false})
    );
    Ok(())
}

// Rust guideline compliant 2026-02-21
