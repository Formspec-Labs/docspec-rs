//! Exact source placement, bounded transport and offline native custody.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::storage::remote::{
    SnapshotSourceConfig,
    objects::tests::{ReadFixture, pinning::native_inventory_at},
};
use std::{collections::HashMap, time::Duration};

const METADATA: &str = "s3://bucket/owned/candidate/metadata/00001.metadata.json";
const LIST: &str = "s3://bucket/owned/candidate/metadata/list.avro";
const MANIFEST: &str = "s3://bucket/owned/candidate/metadata/manifest.avro";
const DATA: &str = "s3://bucket/owned/candidate/data/one.parquet";

async fn snapshot_request(
    root: &Path,
    operation: &str,
    data: &str,
) -> (ManagedJobPrepare, HashMap<String, Vec<u8>>) {
    let (metadata, mut objects) = native_inventory_at(LIST, MANIFEST, data).await;
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let metadata_sha256 = sha256(&bytes);
    let metadata_size = bytes.len() as u64;
    objects.insert(METADATA.into(), bytes);
    let report = "SELECT COUNT(*) - 3 AS invalid_rows FROM raw";
    let job = artifact(
        root.join("job.json"),
        &json!({
            "scratch": root.join("caller-scratch"),
            "inputs": [{
                "kind": "native_table", "name": "raw", "metadata_location": METADATA,
                "table_uuid": metadata.uuid(), "snapshot": metadata.current_snapshot_id(),
                "schema_id": metadata.current_schema_id()
            }],
            "steps": [], "outputs": [],
            "queries": [
                {"name":"invariants", "sql":report},
                {"name":"values", "sql":"SELECT id FROM raw ORDER BY id ASC NULLS LAST"}
            ]
        }),
    );
    let schema = Schema::new(vec![Field::new("invalid_rows", DataType::Int64, false)]);
    let schema =
        crate::job::retained::encode_schema(&schema, &Budget::new(limits()).unwrap()).unwrap();
    let policy = artifact(
        root.join("policy.json"),
        &json!({
            "id": "policy-one", "profile": COUNTERS,
            "evaluator": {"id":"urn:docspec:evaluator:native-job-reports","version":"1"},
            "semantics": {"id":"urn:docspec:semantics:complete-integer-zero-row","revision":"1"},
            "job_sha256": job.sha256,
            "view_names": {"inputs":["raw"],"steps":[],"outputs":[],"reports":["invariants","values"]},
            "required_reports": [{"name":"invariants","sql_sha256":sha256(report.as_bytes()),
                "schema_ipc_base64":STANDARD.encode(&schema.bytes)}]
        }),
    );
    (
        ManagedJobPrepare {
            dataset: Id::new("native-s3").unwrap(),
            operation: Id::new(operation).unwrap(),
            definition: "selected-s3-fixture".into(),
            job,
            file_inputs: Vec::new(),
            snapshot_inputs: vec![JobSnapshotInput {
                name: "raw".into(),
                metadata_sha256,
                metadata_size,
                reuse_from: None,
            }],
            policy_id: Id::new("policy-one").unwrap(),
            policy,
            expected_epoch: 0,
            expected_selection: None,
            producer_adoption: None,
            producer_records: Vec::new(),
            prerequisites: Vec::new(),
        },
        objects,
    )
}

#[tokio::test]
async fn managed_s3_snapshot_resumes_exact_placement_and_reads_sealed_custody_offline() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("host");
    let (request, objects) = snapshot_request(root.path(), "admitted-source", DATA).await;
    let command = ManagedJobCommand::Prepare(request.clone());
    let mut incomplete = objects.clone();
    let missing = incomplete.remove(MANIFEST).unwrap();
    let (fixture, reached, release) = ReadFixture::held_inventory(incomplete);

    let mut engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "snapshot_source_unavailable"
    );
    assert!(
        !engine
            .inspect(&request.dataset)
            .await
            .unwrap()
            .contains_key("job-operation/admitted-source")
    );
    assert!(fixture.observed_paths().is_empty());
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));
    let budget = engine.inner.lifecycle.budget.clone();
    let available = || budget.reserve(0).unwrap().semaphore().available_permits();
    let initial = available();
    let caller = tokio::spawn({
        let engine = engine.clone();
        let command = command.clone();
        async move { engine.managed_job(command).await }
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    let in_flight = available();
    assert!(in_flight < initial);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(available(), in_flight);
    let operation = engine
        .job_operation_lock(&request.dataset, &request.operation)
        .await;
    release.send(()).unwrap();
    // Caller cancellation leaves the native attempt running. Let it seal the
    // earlier members and reach the planted missing manifest before shutdown.
    drop(
        tokio::time::timeout(Duration::from_secs(30), operation.lock())
            .await
            .unwrap(),
    );
    let attempt = engine
        .job_record::<Attempt>(&request.dataset, "job-attempt/admitted-source")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "failed");
    assert_eq!(
        attempt.error.as_deref(),
        Some("remote_read: independent remote object read failed")
    );
    assert!(
        fixture
            .observed_paths()
            .iter()
            .any(|(method, uri)| method == "HEAD" && uri == MANIFEST)
    );
    drop(attempt);
    engine.shutdown().await.unwrap();
    drop(budget);
    drop(engine);

    let other = ReadFixture::inventory(objects.clone());
    let mut engine = Engine::open(&workspace, limits()).await.unwrap();
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert!(before["job-operation/admitted-source"]["snapshot_source"].is_object());
    assert!(!before.contains_key("job-result/admitted-source"));
    assert_eq!(
        before
            .keys()
            .filter(|key| key.starts_with("job-snapshot-member/"))
            .count(),
        2
    );
    engine.snapshot_source = Some(Arc::new(other.snapshot_source(None)));
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "snapshot_source_placement"
    );
    assert!(other.finish().is_empty());
    drop(engine);

    fixture.insert(MANIFEST.into(), missing);
    let earlier_requests = fixture.observed_paths().len();
    let mut engine = Engine::open(&workspace, limits()).await.unwrap();
    engine.snapshot_source = Some(Arc::new(
        fixture.snapshot_source(Some("fixture-session-token")),
    ));
    *engine.inner.job_stop_after.lock().await = Some("job-input/admitted-source/0".into());
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let paths = fixture.observed_paths();
    assert!(fixture.session_token_observed());
    assert!(
        paths[earlier_requests..]
            .iter()
            .any(|(method, uri)| method == "GET" && uri == DATA)
    );
    assert!(
        paths[earlier_requests..]
            .iter()
            .all(|(method, uri)| matches!(method.as_str(), "HEAD" | "GET")
                && matches!(uri.as_str(), MANIFEST | DATA))
    );
    assert!(!fixture.finish().is_empty());
    drop(engine);

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let complete = engine.managed_job(command.clone()).await.unwrap();
    assert_eq!(
        complete.completed_reports[1]["data"],
        json!([{"id":11},{"id":42},{"id":null}])
    );
    let manifest: serde_json::Value = serde_json::from_slice(
        &STANDARD
            .decode(&complete.completion_manifest.as_ref().unwrap().bytes_base64)
            .unwrap(),
    )
    .unwrap();
    let input = &manifest["inputs"][0];
    let held = input["snapshot_custody"]["objects"].as_array().unwrap();
    assert_eq!(held.len(), objects.len());
    for member in held {
        let uri = member["uri"].as_str().unwrap();
        let expected = &objects[uri];
        assert_eq!(member["sha256"], sha256(expected));
        let path = member["retained"]["path"].as_str().unwrap();
        assert!(Path::new(path).starts_with(workspace.canonicalize().unwrap()));
        assert_eq!(&std::fs::read(path).unwrap(), expected);
    }
    let schema = STANDARD
        .decode(
            manifest["schemas"][input["schema_sha256"].as_str().unwrap()]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        try_schema_from_ipc_buffer(&schema)
            .unwrap()
            .field_with_name("id")
            .unwrap()
            .metadata()["PARQUET:field_id"],
        "7"
    );
    let mut reuse = request.clone();
    reuse.operation = Id::new("reuse-source").unwrap();
    reuse.snapshot_inputs[0].reuse_from = Some(complete.input_custody[0].clone());
    let receiver = engine
        .managed_job(ManagedJobCommand::Prepare(reuse))
        .await
        .unwrap();
    assert_eq!(receiver.completed_reports, complete.completed_reports);
    let receiver_manifest: serde_json::Value = serde_json::from_slice(
        &STANDARD
            .decode(&receiver.completion_manifest.as_ref().unwrap().bytes_base64)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        receiver_manifest["inputs"][0]["snapshot_custody"]["objects"],
        input["snapshot_custody"]["objects"]
    );
    let rows = engine.inspect(&request.dataset).await.unwrap();
    assert!(
        rows["job-operation/reuse-source"]
            .get("snapshot_source")
            .is_none()
    );
    std::fs::remove_file(&request.job.path).unwrap();
    std::fs::remove_file(&request.policy.path).unwrap();
    let replay = engine.managed_job(command).await.unwrap();
    assert_eq!(replay.candidate, complete.candidate);
    assert_eq!(replay.completed_reports, complete.completed_reports);
}

#[tokio::test]
async fn managed_s3_snapshot_refuses_remote_controlled_out_of_scope_data() {
    for outside in ["s3://other/owned/one.parquet", "/local/hidden.parquet"] {
        let root = tempfile::tempdir().unwrap();
        let (request, objects) = snapshot_request(root.path(), "outside-source", outside).await;
        let fixture = ReadFixture::inventory(objects);
        let mut engine = Engine::open(root.path().join("host"), limits())
            .await
            .unwrap();
        engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));
        assert_eq!(
            engine
                .managed_job(ManagedJobCommand::Prepare(request.clone()))
                .await
                .unwrap_err()
                .code(),
            "snapshot_source_scope"
        );
        assert!(
            fixture
                .observed_paths()
                .iter()
                .all(|(_, uri)| matches!(uri.as_str(), METADATA | LIST | MANIFEST))
        );
        assert!(!fixture.finish().is_empty());
        assert!(
            !engine
                .inspect(&request.dataset)
                .await
                .unwrap()
                .contains_key("job-result/outside-source")
        );
    }
}

#[tokio::test]
async fn managed_s3_snapshot_refuses_changed_metadata_before_manifest_reads() {
    let root = tempfile::tempdir().unwrap();
    let (mut request, objects) = snapshot_request(root.path(), "wrong-metadata", DATA).await;
    request.snapshot_inputs[0].metadata_sha256 = "0".repeat(64);
    let fixture = ReadFixture::inventory(objects);
    let mut engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(request))
            .await
            .unwrap_err()
            .code(),
        "managed_job_identity"
    );
    assert!(
        fixture
            .observed_paths()
            .iter()
            .all(|(_, uri)| uri == METADATA)
    );
    assert!(!fixture.finish().is_empty());
}

#[tokio::test]
async fn managed_s3_source_configuration_refuses_secret_components_before_host_open() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("not-opened");
    let secret_env = HashMap::from([
        ("s3.access-key-id".into(), "FIXTURE_ACCESS".into()),
        ("s3.secret-access-key".into(), "FIXTURE_SECRET".into()),
    ]);
    let base = SnapshotSourceConfig {
        prefix: "s3://bucket/owned".into(),
        endpoint: "https://s3.invalid".into(),
        region: "auto".into(),
        path_style: true,
        secret_env,
    };
    for endpoint in [
        "https://fixture-sensitive:sensitive@s3.invalid",
        "https://s3.invalid/?fixture-sensitive",
        "https://s3.invalid/#fixture-sensitive",
        "https://s3.invalid/fixture-sensitive",
        "ftp://s3.invalid",
        "http://s3.invalid",
    ] {
        let error = Engine::open_with_snapshot_source(
            &workspace,
            SnapshotSourceConfig {
                endpoint: endpoint.into(),
                ..base.clone()
            },
            limits(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "snapshot_source_config");
        assert!(!format!("{error:?}").contains("fixture-sensitive"));
    }
    let mut config = base;
    config
        .secret_env
        .insert("s3.endpoint".into(), "FIXTURE_ENDPOINT".into());
    let error = Engine::open_with_snapshot_source(&workspace, config, limits())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "snapshot_source_config");
    assert!(!workspace.exists());
}
