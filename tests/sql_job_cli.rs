//! The CLI and embedded runner execute the same declarative SQL job.
// Rust guideline compliant 2026-02-21
use arrow::datatypes::{DataType, Field, Schema};
use docspec::{budget::Limits, job::JobSpec};
use flate2::{Compression, write::GzEncoder};
use serde_json::json;
use std::{io::Write, process::Command};

#[tokio::test]
async fn cli_and_library_export_equal_bytes_without_a_lifecycle_workspace() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input.parquet");
    super::scalar::input(
        &input,
        &[
            (Some(2), Some("second".into())),
            (Some(1), Some("first".into())),
        ],
    );
    let receipt = root.path().join("events.gz");
    let mut gzip = GzEncoder::new(
        std::fs::File::create(&receipt).unwrap(),
        Compression::fast(),
    );
    gzip.write_all(b"{\"id\":1,\"value\":7}\n{\"id\":2,\"value\":\"old shape\"}\n")
        .unwrap();
    gzip.finish().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]);
    let mut specification = json!({
        "scratch":root.path().join("scratch"),
        "inputs":[{"name":"items","kind":"parquet","path":input},{"name":"events","kind":"json_lines","files":[{"key":"history/one","path":receipt}],"schema":schema}],
        "steps":[{"name":"named","sql":"SELECT i.id,coalesce(cast(e.value AS VARCHAR),i.text) AS text,'a' AS kind FROM items i JOIN events e ON i.id=e.id","materialize":false},{"name":"settled","sql":"SELECT * FROM named","materialize":true}],
        "queries":[{"name":"counts","sql":"SELECT count(*) AS rows,cast(NULL AS VARCHAR) AS unknown FROM settled"}],
        "outputs":[{"name":"result.parquet","path":root.path().join("cli.parquet"),"sql":"SELECT * FROM settled","settings":{"profile":"ranged","row_group_rows":2,"order_by":[{"field":"id","direction":"asc","nulls":"nulls_last"}],"columns":[],"dictionary_columns":["kind"],"delta_columns":["text"],"key_column":null,"created_by":"job CLI fixture v1","footer_limit":524288}}]
    });
    let spec = root.path().join("job.json");
    std::fs::write(&spec, serde_json::to_vec(&specification).unwrap()).unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 32 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 32 << 20,
        ..Limits::default()
    };
    let limit_path = root.path().join("limits.json");
    std::fs::write(&limit_path, serde_json::to_vec(&limits).unwrap()).unwrap();
    let workspace = root.path().join("unused-workspace");
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(limit_path)
        .args(["job", "--spec"])
        .arg(&spec)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!workspace.exists());
    let cli: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    specification["outputs"][0]["path"] = json!(root.path().join("library.parquet"));
    let typed: JobSpec = serde_json::from_value(specification).unwrap();
    let library = typed.run(limits).await.unwrap();
    assert_eq!(cli, serde_json::to_value(library).unwrap());
    assert_eq!(
        std::fs::read(root.path().join("cli.parquet")).unwrap(),
        std::fs::read(root.path().join("library.parquet")).unwrap()
    );
    assert_eq!(cli["outputs"][0]["rows"], 2);
    assert_eq!(
        cli["queries"][0]["data"],
        json!([{"rows":2,"unknown":null}])
    );
}

#[tokio::test]
async fn invalid_names_refuse_before_opening_inputs_or_scratch() {
    let root = tempfile::tempdir().unwrap();
    let scratch = root.path().join("not-created");
    let spec:JobSpec=serde_json::from_value(json!({"scratch":scratch,"inputs":[{"name":"same","kind":"parquet","path":"missing"},{"name":"same","kind":"parquet","path":"also-missing"}],"steps":[],"outputs":[]})).unwrap();
    let error = spec.run(Limits::default()).await.unwrap_err();
    assert!(error.to_string().contains("duplicate"), "{error}");
    assert!(!scratch.exists());
}

#[test]
fn cli_requires_explicit_host_limits_before_opening_the_spec() {
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["job", "--spec", "nonexistent-job.json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --limits"));
}

#[tokio::test]
async fn later_failure_retains_identities_for_an_explicit_remaining_output_retry() {
    let root = tempfile::tempdir().unwrap();
    let settings = json!({"profile":"whole","row_group_rows":65536,"order_by":[{"field":"id"}],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"retry fixture v1","footer_limit":524288});
    let mut spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("scratch"),"inputs":[],"steps":[],"outputs":[
        {"name":"one.parquet","path":root.path().join("one.parquet"),"sql":"SELECT 1 AS id","settings":settings},
        {"name":"two.parquet","path":root.path().join("two.parquet"),"sql":"SELECT absent_column","settings":settings}
    ]})).unwrap();
    let failure = spec.run(Limits::default()).await.unwrap_err();
    assert_eq!(failure.completed.outputs.len(), 1);
    assert_eq!(failure.completed.outputs[0].name, "one.parquet");
    assert_eq!(failure.report()["status"], "failed");
    let first = std::fs::read(root.path().join("one.parquet")).unwrap();
    assert!(!root.path().join("two.parquet").exists());
    // Keep returned identities and retry only unfinished output declarations.
    spec.outputs.remove(0);
    spec.outputs[0].sql = "SELECT 2 AS id".into();
    let retry = spec.run(Limits::default()).await.unwrap();
    assert_eq!(retry.outputs.len(), 1);
    assert_eq!(
        first,
        std::fs::read(root.path().join("one.parquet")).unwrap()
    );
    assert!(root.path().join("two.parquet").exists());
}

#[tokio::test]
async fn sql_metadata_result_has_a_byte_bound() {
    let root = tempfile::tempdir().unwrap();
    let sql = format!("SELECT '{}' AS text", "x".repeat(8192));
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("scratch"),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"too_big","sql":sql}]})).unwrap();
    let error = spec
        .run(Limits {
            max_metadata_bytes: 1024,
            ..Limits::default()
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("metadata allowance"));
    assert!(error.completed.queries.is_empty());
}

#[tokio::test]
async fn named_step_release_is_validated_before_io_and_happens_after_success() {
    let root = tempfile::tempdir().unwrap();
    for releases in [
        json!(["unknown"]),
        json!(["later"]),
        json!(["input", "input"]),
    ] {
        let scratch = root.path().join("absent");
        let spec: JobSpec = serde_json::from_value(json!({"scratch":scratch,
            "inputs":[{"kind":"parquet","name":"input","path":"missing.parquet"}],
            "steps":[{"name":"first","sql":"SELECT * FROM input","release_after":releases},
                     {"name":"later","sql":"SELECT * FROM first"}],"outputs":[]}))
        .unwrap();
        assert_eq!(
            spec.run(Limits::default()).await.unwrap_err().error.code(),
            "invalid_spec"
        );
        assert!(!scratch.exists());
    }
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("success"),"inputs":[],
        "steps":[{"name":"first","sql":"SELECT 7 AS value","materialize":true},
                 {"name":"second","sql":"SELECT * FROM first","materialize":true,"release_after":["first"]}],
        "outputs":[],"queries":[{"name":"kept","sql":"SELECT * FROM second"}]})).unwrap();
    assert_eq!(
        spec.run(Limits::default()).await.unwrap().queries[0].data,
        json!([{"value":7}])
    );
}

#[cfg(feature = "remote")]
#[test]
fn explicit_remote_sql_job_starts_without_a_lifecycle_workspace_or_catalog_query() {
    let root = tempfile::tempdir().unwrap();
    let spec = root.path().join("job.json");
    std::fs::write(&spec,serde_json::to_vec(&json!({"scratch":root.path().join("scratch"),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"one","sql":"SELECT 1 AS n"}]})).unwrap()).unwrap();
    let remote = root.path().join("remote.json");
    std::fs::write(&remote,serde_json::to_vec(&json!({"owned_prefix":"s3://bucket/owned/","properties":{"uri":"http://127.0.0.1:9","warehouse":"fixture","s3.endpoint":"http://127.0.0.1:9","s3.region":"auto","s3.access-key-id":"dummy","s3.secret-access-key":"dummy","s3.path-style-access":"true"}})).unwrap()).unwrap();
    let ceiling = root.path().join("limits.json");
    std::fs::write(
        &ceiling,
        serde_json::to_vec(&Limits {
            compute_threads: 2,
            memory_bytes: 32 << 20,
            max_batch_bytes: 2 << 20,
            max_metadata_bytes: 2 << 20,
            scratch_bytes: 8 << 20,
            ..Limits::default()
        })
        .unwrap(),
    )
    .unwrap();
    let workspace = root.path().join("unused-workspace");
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--remote")
        .arg(remote)
        .arg("--limits")
        .arg(ceiling)
        .args(["job", "--spec"])
        .arg(spec)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!workspace.exists());
    let output: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output["queries"][0]["data"], json!([{"n":1}]));
}

#[cfg(feature = "remote")]
#[test]
fn create_file_refuses_changed_source_without_catalog_configuration_or_workspace() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::write(&source, b"changed").unwrap();
    let spec = root.path().join("upload.json");
    std::fs::write(&spec, serde_json::to_vec(&json!({"source":source,"destination":"s3://bucket/owned/release.parquet","sha256":"0".repeat(64),"md5":"0".repeat(32),"size":7,"content_type":"application/vnd.apache.parquet","cache_control":"public,max-age=31536000,immutable"})).unwrap()).unwrap();
    let remote = root.path().join("remote.json");
    std::fs::write(&remote, serde_json::to_vec(&json!({"owned_prefix":"s3://bucket/owned/","properties":{"s3.endpoint":"http://127.0.0.1:9","s3.region":"auto","s3.access-key-id":"dummy","s3.secret-access-key":"dummy","s3.path-style-access":"true"}})).unwrap()).unwrap();
    let ceiling = root.path().join("limits.json");
    std::fs::write(&ceiling, serde_json::to_vec(&Limits::default()).unwrap()).unwrap();
    let workspace = root.path().join("unused-workspace");
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--remote")
        .arg(remote)
        .arg("--limits")
        .arg(ceiling)
        .args(["create-file", "--spec"])
        .arg(spec)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("object_changed"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!workspace.exists());
}
