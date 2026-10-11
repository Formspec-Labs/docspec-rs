//! Opt-in synthetic Engine qualification; all remote objects remain retained.

use super::*;

mod exports;
mod publication_race;
mod updates;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::collections::HashMap;

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| Error::refusal("provider_config", "probe configuration is missing"))
}

async fn connect(prefix: &str) -> Result<storage::remote::RemoteStorage> {
    storage::remote::remote_storage(
        prefix,
        HashMap::from([
            ("uri".into(), env("DOCSPEC_CATALOG_URI")?),
            ("warehouse".into(), env("DOCSPEC_CATALOG_WAREHOUSE")?),
            ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::from([
            ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_S3_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await
}

fn compose(host: &Path, remote: storage::remote::RemoteStorage) -> Result<Engine> {
    let ownership = worker::Host::acquire(host)?;
    Engine::compose(
        std::fs::canonicalize(host)?,
        remote.catalog,
        storage::Objects::Remote(remote.objects),
        Limits {
            compute_threads: 1,
            memory_bytes: 256 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 1 << 20,
            max_metadata_bytes: 4 << 20,
        },
        ownership,
        false,
    )
}

#[tokio::test]
#[ignore = "requires explicit fresh qualification UUID and isolated live R2/Basin configuration"]
async fn native_remote_build_recovers_then_reads_from_a_fresh_host() {
    // Never render upstream error/config Debug: native errors can carry URLs.
    if let Err(error) = qualify().await {
        eprintln!("{}", json!({"phase": "failed", "code": error.code()}));
        panic!("isolated provider qualification failed; retain its recorded references");
    }
}

async fn qualify() -> Result<()> {
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
        "s3://{}/__r2_data_catalog/docspec-qualification/{name}/engine-build",
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
        json!({"phase": "start", "dataset": dataset, "prefix": prefix, "lifecycle": lifecycle})
    );
    let producer = tempfile::tempdir()?;
    let source = producer.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("counter", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("same"),
                None,
                Some("same"),
                Some(""),
                Some("excluded"),
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 1, 2, 3])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&source)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let spec = RunSpec {
        dataset: dataset.clone(),
        operation: Id::new("initial")?,
        base: None,
        relationship: None,
        input: source.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&source)?.0,
        sql: "SELECT text, counter FROM input WHERE counter < 3".into(),
        definition: "synthetic-v1".into(),
    };
    let host = tempfile::tempdir()?;
    let engine = compose(host.path(), remote)?;
    let stopped = engine.run_until(&spec, "admission").await.unwrap_err();
    if stopped.code() != "injected_failure" {
        return Err(stopped);
    }
    let request = OperationRequest::Build(spec.clone());
    let view = engine.inner.lifecycle.open(&dataset).await?;
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, "operation/initial")
        .await?
        .ok_or_else(|| Error::refusal("provider_evidence", "admitted operation is missing"))?;
    let admission = engine
        .phase(&request, &operation, "admission")
        .await?
        .ok_or_else(|| Error::refusal("provider_evidence", "admission is missing"))?;
    // Exercise the real writer, then leave the acknowledged native snapshot
    // without a lifecycle processing completion. This is a controlled gap, not a kill test.
    let prepared = engine
        .process_initial_build(&spec, &operation, &admission)
        .await?;
    println!(
        "{}",
        json!({"phase": "native_append_without_phase_ack", "table": prepared.table, "metrics": prepared.metrics})
    );
    std::fs::remove_file(&source)?;
    let stopped = engine.run_until(&spec, "readback").await.unwrap_err();
    if stopped.code() != "injected_failure" {
        return Err(stopped);
    }
    let view = engine.inner.lifecycle.open(&dataset).await?;
    let release: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, "readback/initial")
        .await?
        .ok_or_else(|| Error::refusal("provider_evidence", "readback is missing"))?;
    assert_eq!(release.dataset.groups["source"], prepared.table);
    assert_eq!(
        engine
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    println!(
        "{}",
        json!({"phase": "readback_before_selection", "release": release})
    );
    engine.shutdown().await?;
    drop(engine);
    drop(host);
    drop(producer);

    let cold_host = tempfile::tempdir()?;
    let cold = compose(cold_host.path(), connect(&prefix).await?)?;
    let selected = cold.run(&spec).await?;
    assert_eq!(selected, release);
    assert_eq!(cold.latest(&dataset).await?, selected);
    let mut stream = cold.read_group(&selected, "source").await?;
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if values.len().saturating_add(batch.num_rows()) > 4 {
            return Err(Error::refusal(
                "provider_evidence",
                "synthetic read exceeded its row bound",
            ));
        }
        let text = arrow::compute::cast(batch.column(0), &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.null_count(), 0);
        for row in 0..batch.num_rows() {
            values.push((
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
                count.value(row),
            ));
        }
    }
    drop(stream);
    values.sort();
    assert_eq!(
        values,
        vec![
            (None, 2),
            (Some("".into()), 2),
            (Some("same".into()), 1),
            (Some("same".into()), 1)
        ]
    );
    assert_eq!(
        cold.inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    cold.shutdown().await?;
    println!(
        "{}",
        json!({"phase": "passed", "dataset": dataset, "manifest": selected.manifest, "rows": values.len(), "fresh_host_queries": 0, "remote_cleanup": false})
    );
    Ok(())
}
