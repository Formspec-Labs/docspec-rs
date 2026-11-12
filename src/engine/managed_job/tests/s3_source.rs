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

fn uncompressed_id_parquet(values: &[i64]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "7".into())])),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from_iter_values(
            values.iter().copied(),
        ))],
    )
    .unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut payload = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut payload, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    payload
}

async fn write_manifest_bytes(
    base: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    manifest_uri: &str,
    files: &[(&str, u64, u64)],
) -> (iceberg::spec::ManifestFile, Vec<u8>) {
    let temp = tempfile::tempdir().unwrap();
    let io = iceberg::io::FileIO::new_with_fs();
    let manifest_path = temp
        .path()
        .join("manifest.avro")
        .to_string_lossy()
        .into_owned();
    let mut writer = iceberg::spec::ManifestWriterBuilder::new(
        io.new_output(&manifest_path).unwrap(),
        Some(snapshot_id),
        base.current_schema().clone(),
        base.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    for &(uri, size, rows) in files {
        writer
            .add_file(
                iceberg::spec::DataFileBuilder::default()
                    .content(iceberg::spec::DataContentType::Data)
                    .file_format(iceberg::spec::DataFileFormat::Parquet)
                    .file_path(uri.into())
                    .file_size_in_bytes(size)
                    .record_count(rows)
                    .partition_spec_id(base.default_partition_spec_id())
                    .build()
                    .unwrap(),
                snapshot_id,
            )
            .unwrap();
    }
    let mut manifest = writer.write_manifest_file().await.unwrap();
    manifest.manifest_path = manifest_uri.into();
    (manifest, std::fs::read(manifest_path).unwrap())
}

async fn write_snapshot_version(
    base: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    list_uri: &str,
    manifests: Vec<iceberg::spec::ManifestFile>,
) -> (iceberg::spec::TableMetadata, Vec<u8>) {
    let temp = tempfile::tempdir().unwrap();
    let io = iceberg::io::FileIO::new_with_fs();
    let list_path = temp.path().join("list.avro").to_string_lossy().into_owned();
    let mut writer = iceberg::spec::ManifestListWriter::v2(
        io.new_output(&list_path).unwrap().writer().await.unwrap(),
        snapshot_id,
        None,
        snapshot_id,
    );
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
    let snapshot = iceberg::spec::Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_sequence_number(snapshot_id)
        .with_timestamp_ms(base.last_updated_ms() + snapshot_id)
        .with_schema_id(base.current_schema_id())
        .with_manifest_list(list_uri)
        .with_summary(iceberg::spec::Summary {
            operation: iceberg::spec::Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    let metadata = base
        .clone()
        .into_builder(None)
        .set_branch_snapshot(snapshot, "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    (metadata, std::fs::read(list_path).unwrap())
}

fn s3_job_prepare(
    root: &Path,
    dataset: &str,
    operation: &str,
    metadata_uri: &str,
    metadata: &iceberg::spec::TableMetadata,
    metadata_bytes: &[u8],
    expected_rows: usize,
    reuse_from: Option<JobInputCustodyRef>,
) -> ManagedJobPrepare {
    let report = format!("SELECT COUNT(*) - {expected_rows} AS invalid_rows FROM raw");
    let job = artifact(
        root.join(format!("job-{operation}.json")),
        &json!({
            "scratch": root.join(format!("caller-scratch-{operation}")),
            "inputs": [{
                "kind": "native_table",
                "name": "raw",
                "metadata_location": metadata_uri,
                "table_uuid": metadata.uuid(),
                "snapshot": metadata.current_snapshot_id(),
                "schema_id": metadata.current_schema_id(),
            }],
            "steps": [],
            "outputs": [],
            "queries": [
                {"name": "invariants", "sql": report},
                {"name": "summary", "sql": "SELECT CAST(COALESCE(SUM(id), 0) AS BIGINT) AS total_id FROM raw"},
            ],
        }),
    );
    let schema = Schema::new(vec![Field::new("invalid_rows", DataType::Int64, false)]);
    let schema =
        crate::job::retained::encode_schema(&schema, &Budget::new(limits()).unwrap()).unwrap();
    let policy = artifact(
        root.join(format!("policy-{operation}.json")),
        &json!({
            "id": "policy-one",
            "profile": COUNTERS,
            "evaluator": {"id": "urn:docspec:evaluator:native-job-reports", "version": "1"},
            "semantics": {"id": "urn:docspec:semantics:complete-integer-zero-row", "revision": "1"},
            "job_sha256": job.sha256,
            "view_names": {"inputs": ["raw"], "steps": [], "outputs": [], "reports": ["invariants", "summary"]},
            "required_reports": [{
                "name": "invariants",
                "sql_sha256": sha256(report.as_bytes()),
                "schema_ipc_base64": STANDARD.encode(&schema.bytes),
            }],
        }),
    );
    ManagedJobPrepare {
        dataset: Id::new(dataset).unwrap(),
        operation: Id::new(operation).unwrap(),
        definition: "selected-s3-fixture".into(),
        job,
        file_inputs: Vec::new(),
        snapshot_inputs: vec![JobSnapshotInput {
            name: "raw".into(),
            metadata_sha256: sha256(metadata_bytes),
            metadata_size: metadata_bytes.len() as u64,
            reuse_from,
        }],
        policy_id: Id::new("policy-one").unwrap(),
        policy,
        expected_epoch: 0,
        expected_selection: None,
        producer_adoption: None,
        producer_records: Vec::new(),
        prerequisites: Vec::new(),
    }
}

fn base_s3_table_metadata() -> iceberg::spec::TableMetadata {
    crate::storage::fresh_metadata(
        iceberg::spec::Schema::builder()
            .with_fields([Arc::new(iceberg::spec::NestedField::optional(
                7,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        Path::new("s3://bucket/owned/candidate"),
    )
    .unwrap()
}

#[tokio::test]
async fn s3_snapshot_copies_large_member_in_8mib_ranges_without_redundant_heads() {
    let root = tempfile::tempdir().unwrap();
    let base = base_s3_table_metadata();
    let rows = 1_250_000_usize;
    let values = (0..rows as i64).collect::<Vec<_>>();
    let payload = uncompressed_id_parquet(&values);
    let data_len = payload.len();
    assert!(data_len > (8 << 20) && data_len < (16 << 20));

    let data_uri = "s3://bucket/owned/candidate/data/large.parquet";
    let (manifest_file, manifest_bytes) = write_manifest_bytes(
        &base,
        1,
        MANIFEST,
        &[(data_uri, data_len as u64, rows as u64)],
    )
    .await;
    let (metadata, list_bytes) = write_snapshot_version(&base, 1, LIST, vec![manifest_file]).await;
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();

    let prepare = s3_job_prepare(
        root.path(),
        "native-s3",
        "large-8mib",
        METADATA,
        &metadata,
        &metadata_bytes,
        rows,
        None,
    );
    let objects = HashMap::from([
        (METADATA.into(), metadata_bytes),
        (LIST.into(), list_bytes),
        (MANIFEST.into(), manifest_bytes),
        (data_uri.into(), payload),
    ]);
    let fixture = ReadFixture::inventory(objects);
    let bounds = Limits {
        compute_threads: 2,
        memory_bytes: 128 << 20,
        scratch_bytes: 64 << 20,
        batch_rows: 8192,
        max_batch_bytes: 8 << 20,
        max_metadata_bytes: 2 << 20,
    };
    let mut engine = Engine::open(root.path().join("host"), bounds)
        .await
        .unwrap();
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));
    let receipt = engine
        .managed_job(ManagedJobCommand::Prepare(prepare))
        .await
        .unwrap();
    assert_eq!(receipt.status, "completed");
    let expected_sum = ((rows as i64 - 1) * rows as i64) / 2;
    assert_eq!(
        receipt.completed_reports[1]["data"],
        json!([{"total_id": expected_sum}])
    );

    let data_requests = fixture
        .observed_requests()
        .into_iter()
        .filter(|(_, uri, _)| uri == data_uri)
        .collect::<Vec<_>>();
    assert_eq!(
        data_requests,
        vec![
            (
                "GET".into(),
                data_uri.into(),
                Some("bytes=0-8388607".into()),
            ),
            (
                "GET".into(),
                data_uri.into(),
                Some(format!("bytes=8388608-{}", data_len - 1)),
            ),
        ],
        "non-empty S3 data member must copy in 8 MiB ranges with 0 HEAD requests"
    );
    let _ = fixture.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_snapshot_copies_members_concurrently_within_memory_budget() {
    let root = tempfile::tempdir().unwrap();
    let base = base_s3_table_metadata();
    let file_count = 24_usize;
    let rows_per_file = 16_384_usize;
    let values = (0..rows_per_file as i64).collect::<Vec<_>>();
    let shared_parquet = Arc::new(uncompressed_id_parquet(&values));

    let mut manifest_entries = Vec::with_capacity(file_count);
    let mut objects = HashMap::<String, Arc<Vec<u8>>>::new();
    for idx in 0..file_count {
        let uri = format!("s3://bucket/owned/candidate/data/part-{idx:02}.parquet");
        objects.insert(uri.clone(), shared_parquet.clone());
        manifest_entries.push((uri, shared_parquet.len() as u64, rows_per_file as u64));
    }
    let borrowed = manifest_entries
        .iter()
        .map(|(uri, size, rows)| (uri.as_str(), *size, *rows))
        .collect::<Vec<_>>();
    let (manifest_file, manifest_bytes) = write_manifest_bytes(&base, 1, MANIFEST, &borrowed).await;
    let (metadata, list_bytes) = write_snapshot_version(&base, 1, LIST, vec![manifest_file]).await;
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
    objects.insert(METADATA.into(), Arc::new(metadata_bytes.clone()));
    objects.insert(LIST.into(), Arc::new(list_bytes));
    objects.insert(MANIFEST.into(), Arc::new(manifest_bytes));

    let prepare = s3_job_prepare(
        root.path(),
        "native-s3",
        "concurrent-copy",
        METADATA,
        &metadata,
        &metadata_bytes,
        file_count * rows_per_file,
        None,
    );
    let fixture = ReadFixture::concurrent_shared_inventory(objects, Duration::from_millis(25));
    let bounds = Limits {
        compute_threads: 2,
        memory_bytes: 16 << 20,
        scratch_bytes: 32 << 20,
        batch_rows: 1024,
        max_batch_bytes: 256 << 10,
        max_metadata_bytes: 512 << 10,
    };
    let mut engine = Engine::open(root.path().join("host"), bounds)
        .await
        .unwrap();
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));
    let receipt = engine
        .managed_job(ManagedJobCommand::Prepare(prepare.clone()))
        .await
        .unwrap();
    assert_eq!(receipt.status, "completed");

    let peak = fixture.peak_in_flight();
    assert!(
        peak >= 2,
        "expected concurrent S3 member transfers, got peak_in_flight={peak}"
    );
    assert!(
        peak <= 7,
        "expected memory budget to bound concurrent S3 transfers below 16, got peak_in_flight={peak}"
    );
    assert!(
        fixture
            .observed_paths()
            .iter()
            .all(|(method, uri)| !(method == "HEAD" && uri.contains("/data/"))),
        "data files must not incur redundant HEAD requests"
    );
    let _ = fixture.finish();
}

#[tokio::test]
async fn cross_snapshot_custody_reuses_unchanged_table_files_and_survives_donor_retirement() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("host");
    let base = base_s3_table_metadata();

    let meta_1 = "s3://bucket/owned/candidate/metadata/00001.metadata.json";
    let meta_2 = "s3://bucket/owned/candidate/metadata/00002.metadata.json";
    let meta_3 = "s3://bucket/owned/candidate/metadata/00003.metadata.json";
    let list_1 = "s3://bucket/owned/candidate/metadata/list-1.avro";
    let list_2 = "s3://bucket/owned/candidate/metadata/list-2.avro";
    let list_3 = "s3://bucket/owned/candidate/metadata/list-3.avro";
    let manifest_1 = "s3://bucket/owned/candidate/metadata/manifest-1.avro";
    let manifest_2 = "s3://bucket/owned/candidate/metadata/manifest-2.avro";
    let manifest_3 = "s3://bucket/owned/candidate/metadata/manifest-3.avro";
    let data_1 = "s3://bucket/owned/candidate/data/day1-a.parquet";
    let data_2 = "s3://bucket/owned/candidate/data/day1-b.parquet";
    let data_3 = "s3://bucket/owned/candidate/data/day2-c.parquet";
    let data_4 = "s3://bucket/owned/candidate/data/day3-d.parquet";

    let bytes_d1 = uncompressed_id_parquet(&[10, 20]);
    let bytes_d2 = uncompressed_id_parquet(&[30]);
    let bytes_d3 = uncompressed_id_parquet(&[40, 50]);
    let bytes_d4 = uncompressed_id_parquet(&[60]);

    let (m1_file, m1_bytes) = write_manifest_bytes(
        &base,
        1,
        manifest_1,
        &[
            (data_1, bytes_d1.len() as u64, 2),
            (data_2, bytes_d2.len() as u64, 1),
        ],
    )
    .await;
    let (m2_file, m2_bytes) =
        write_manifest_bytes(&base, 2, manifest_2, &[(data_3, bytes_d3.len() as u64, 2)]).await;
    let (m3_file, m3_bytes) =
        write_manifest_bytes(&base, 3, manifest_3, &[(data_4, bytes_d4.len() as u64, 1)]).await;

    let (s1_meta, l1_bytes) = write_snapshot_version(&base, 1, list_1, vec![m1_file.clone()]).await;
    let s1_meta_bytes = serde_json::to_vec(&s1_meta).unwrap();
    let (s2_meta, l2_bytes) =
        write_snapshot_version(&base, 2, list_2, vec![m1_file.clone(), m2_file.clone()]).await;
    let s2_meta_bytes = serde_json::to_vec(&s2_meta).unwrap();
    let (s3_meta, l3_bytes) =
        write_snapshot_version(&base, 3, list_3, vec![m1_file, m2_file, m3_file]).await;
    let s3_meta_bytes = serde_json::to_vec(&s3_meta).unwrap();

    // Day 1: cold snapshot S1.
    let fixture_1 = ReadFixture::inventory(HashMap::from([
        (meta_1.into(), s1_meta_bytes.clone()),
        (list_1.into(), l1_bytes),
        (manifest_1.into(), m1_bytes),
        (data_1.into(), bytes_d1),
        (data_2.into(), bytes_d2),
    ]));
    let mut engine = Engine::open(&workspace, limits()).await.unwrap();
    engine.snapshot_source = Some(Arc::new(fixture_1.snapshot_source(None)));
    let day1_prepare = s3_job_prepare(
        root.path(),
        "native-s3",
        "op-day1",
        meta_1,
        &s1_meta,
        &s1_meta_bytes,
        3,
        None,
    );
    let day1 = engine
        .managed_job(ManagedJobCommand::Prepare(day1_prepare.clone()))
        .await
        .unwrap();
    assert_eq!(day1.completed_reports[1]["data"], json!([{"total_id": 60}]));
    let day1_ref = day1.input_custody[0].clone();
    let _ = fixture_1.finish();

    let day1_input = engine
        .job_record::<execution::InputAdmission>(&day1_prepare.dataset, "job-input/op-day1/0")
        .await
        .unwrap()
        .unwrap()
        .value;
    let day1_objects = day1_input
        .snapshot_custody
        .as_ref()
        .unwrap()
        .objects
        .clone();
    let day1_reused_refs = day1_objects
        .iter()
        .filter(
            |obj| matches!(obj.uri.as_str(), u if u == manifest_1 || u == data_1 || u == data_2),
        )
        .map(|obj| obj.retained.clone())
        .collect::<Vec<_>>();
    let day1_superseded_refs = day1_objects
        .iter()
        .filter(|obj| matches!(obj.uri.as_str(), u if u == meta_1 || u == list_1))
        .map(|obj| obj.retained.clone())
        .collect::<Vec<_>>();

    // Refusal 1: different table_uuid must be refused.
    let other_base = base_s3_table_metadata();
    assert_ne!(other_base.uuid(), base.uuid());
    let (other_m, _) = write_manifest_bytes(&other_base, 1, manifest_1, &[]).await;
    let (other_meta, _) = write_snapshot_version(&other_base, 1, list_1, vec![other_m]).await;
    let other_meta_bytes = serde_json::to_vec(&other_meta).unwrap();
    let bad_uuid = s3_job_prepare(
        root.path(),
        "native-s3",
        "refuse-other-uuid",
        meta_2,
        &other_meta,
        &other_meta_bytes,
        0,
        Some(day1_ref.clone()),
    );
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(bad_uuid))
            .await
            .unwrap_err()
            .code(),
        mismatch().code()
    );

    // Refusal 2: same metadata_location with mutated metadata_sha256 must be refused.
    let mut bad_same_loc = s3_job_prepare(
        root.path(),
        "native-s3",
        "refuse-same-loc",
        meta_1,
        &s2_meta,
        &s2_meta_bytes,
        5,
        Some(day1_ref.clone()),
    );
    bad_same_loc.snapshot_inputs[0].metadata_sha256 = "f".repeat(64);
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(bad_same_loc))
            .await
            .unwrap_err()
            .code(),
        mismatch().code()
    );

    // Day 2: S2 reuses manifest_1, data_1, data_2 (omitted from S3!) and fetches only new S2 files.
    let fixture_2 = ReadFixture::inventory(HashMap::from([
        (meta_2.into(), s2_meta_bytes.clone()),
        (list_2.into(), l2_bytes),
        (manifest_2.into(), m2_bytes),
        (data_3.into(), bytes_d3),
    ]));
    engine.snapshot_source = Some(Arc::new(fixture_2.snapshot_source(None)));
    let day2_prepare = s3_job_prepare(
        root.path(),
        "native-s3",
        "op-day2",
        meta_2,
        &s2_meta,
        &s2_meta_bytes,
        5,
        Some(day1_ref.clone()),
    );
    *engine.inner.job_stop_after.lock().await = Some("job-grant/op-day2".into());
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(day2_prepare.clone()))
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );

    // Retire Day 1 completely BEFORE Day 2 seals `job-input/op-day2/0`.
    retire_producer_fixture_job(&engine, &day1_prepare, true).await;
    engine
        .compact_lifecycle(&day1_prepare.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inspect_retention(&day1_reused_refs)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some()),
        "unsealed receiver admission must retain donor files across donor retirement"
    );

    let day2 = engine
        .managed_job(ManagedJobCommand::Prepare(day2_prepare.clone()))
        .await
        .unwrap();
    assert_eq!(day2.status, "completed");
    assert_eq!(
        day2.completed_reports[1]["data"],
        json!([{"total_id": 150}])
    );
    assert!(
        fixture_2
            .observed_paths()
            .iter()
            .all(|(_, uri)| matches!(uri.as_str(), u if u == meta_2 || u == list_2 || u == manifest_2 || u == data_3)),
        "Day 2 must not issue HEAD or GET for reused Day 1 manifest or data files"
    );
    let _ = fixture_2.finish();

    let day2_dir = workspace.join("objects/native-s3/job-op-day2");
    let day2_copied = std::fs::read_dir(&day2_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
        .count();
    assert_eq!(
        day2_copied, 4,
        "Day 2 must only write the 4 newly fetched snapshot objects"
    );

    // Day 3: multi-hop reuse from Day 2 after Day 1 was retired and compacted.
    let fixture_3 = ReadFixture::inventory(HashMap::from([
        (meta_3.into(), s3_meta_bytes.clone()),
        (list_3.into(), l3_bytes),
        (manifest_3.into(), m3_bytes),
        (data_4.into(), bytes_d4),
    ]));
    engine.snapshot_source = Some(Arc::new(fixture_3.snapshot_source(None)));
    let day3_prepare = s3_job_prepare(
        root.path(),
        "native-s3",
        "op-day3",
        meta_3,
        &s3_meta,
        &s3_meta_bytes,
        6,
        Some(day2.input_custody[0].clone()),
    );
    let day3 = engine
        .managed_job(ManagedJobCommand::Prepare(day3_prepare.clone()))
        .await
        .unwrap();
    assert_eq!(day3.status, "completed");
    assert_eq!(
        day3.completed_reports[1]["data"],
        json!([{"total_id": 210}])
    );
    assert!(
        fixture_3
            .observed_paths()
            .iter()
            .all(|(_, uri)| matches!(uri.as_str(), u if u == meta_3 || u == list_3 || u == manifest_3 || u == data_4)),
        "Day 3 must reuse both Day 1 and Day 2 manifests and data files without S3 I/O"
    );
    let _ = fixture_3.finish();

    // Once Day 2's operation recovery retires, Day 1's superseded metadata/list become reclaimable,
    // while Day 1's reused manifest/data remain protected by Day 2 & Day 3 results.
    retire_producer_fixture_job(&engine, &day2_prepare, true).await;
    engine
        .compact_lifecycle(&day2_prepare.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inspect_retention(&day1_superseded_refs)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none()),
        "superseded Day 1 metadata/manifest_list must be unblocked once Day 2 retires"
    );
    assert!(
        engine
            .inspect_retention(&day1_reused_refs)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some()),
        "Day 1 manifest/data reused by Day 3 must remain protected while Day 3 is active"
    );

    retire_producer_fixture_job(&engine, &day3_prepare, true).await;
    engine
        .compact_lifecycle(&day3_prepare.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inspect_retention(&day1_reused_refs)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
}

#[tokio::test]
async fn compacted_donor_superseded_files_do_not_exhaust_receiver_scratch_budget() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("host");
    let base = base_s3_table_metadata();

    // S1 has ~1.5 MiB of data; S2 replaces S1 completely after upstream compaction (~1.5 MiB).
    // With `scratch_bytes` = 3 MiB, `size(S1) + size(S2) > scratch_bytes`, while each snapshot
    // individually fits cleanly within `scratch_bytes`.
    let rows = 200_000_usize;
    let d1_bytes = uncompressed_id_parquet(&(0..rows as i64).collect::<Vec<_>>());
    let d2_bytes = uncompressed_id_parquet(&(1..=rows as i64).collect::<Vec<_>>());
    assert!(d1_bytes.len() > (3 << 19) && d2_bytes.len() > (3 << 19));

    let meta_1 = "s3://bucket/owned/candidate/metadata/compact-00001.metadata.json";
    let meta_2 = "s3://bucket/owned/candidate/metadata/compact-00002.metadata.json";
    let list_1 = "s3://bucket/owned/candidate/metadata/compact-list-1.avro";
    let list_2 = "s3://bucket/owned/candidate/metadata/compact-list-2.avro";
    let manifest_1 = "s3://bucket/owned/candidate/metadata/compact-manifest-1.avro";
    let manifest_2 = "s3://bucket/owned/candidate/metadata/compact-manifest-2.avro";
    let data_1 = "s3://bucket/owned/candidate/data/uncompacted.parquet";
    let data_2 = "s3://bucket/owned/candidate/data/compacted.parquet";

    let (m1_file, m1_bytes) = write_manifest_bytes(
        &base,
        1,
        manifest_1,
        &[(data_1, d1_bytes.len() as u64, rows as u64)],
    )
    .await;
    let (m2_file, m2_bytes) = write_manifest_bytes(
        &base,
        2,
        manifest_2,
        &[(data_2, d2_bytes.len() as u64, rows as u64)],
    )
    .await;
    let (s1_meta, l1_bytes) = write_snapshot_version(&base, 1, list_1, vec![m1_file]).await;
    let s1_meta_bytes = serde_json::to_vec(&s1_meta).unwrap();
    let (s2_meta, l2_bytes) = write_snapshot_version(&base, 2, list_2, vec![m2_file]).await;
    let s2_meta_bytes = serde_json::to_vec(&s2_meta).unwrap();

    let bounds = Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        scratch_bytes: (d1_bytes.len() + d2_bytes.len() - (256 << 10)) as u64,
        batch_rows: 4096,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 1 << 20,
    };
    let fixture = ReadFixture::inventory(HashMap::from([
        (meta_1.into(), s1_meta_bytes.clone()),
        (list_1.into(), l1_bytes),
        (manifest_1.into(), m1_bytes),
        (data_1.into(), d1_bytes),
        (meta_2.into(), s2_meta_bytes.clone()),
        (list_2.into(), l2_bytes),
        (manifest_2.into(), m2_bytes),
        (data_2.into(), d2_bytes),
    ]));
    let mut engine = Engine::open(&workspace, bounds).await.unwrap();
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));

    let day1 = engine
        .managed_job(ManagedJobCommand::Prepare(s3_job_prepare(
            root.path(),
            "native-s3",
            "compact-day1",
            meta_1,
            &s1_meta,
            &s1_meta_bytes,
            rows,
            None,
        )))
        .await
        .unwrap();
    assert_eq!(day1.status, "completed");

    let day2 = engine
        .managed_job(ManagedJobCommand::Prepare(s3_job_prepare(
            root.path(),
            "native-s3",
            "compact-day2",
            meta_2,
            &s2_meta,
            &s2_meta_bytes,
            rows,
            Some(day1.input_custody[0].clone()),
        )))
        .await
        .unwrap();
    assert_eq!(day2.status, "completed");
    let _ = fixture.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "756-member 488 MiB Stage 1b throughput benchmark; run explicitly with --ignored"]
async fn benchmark_756_member_488mib_snapshot_copy_and_day2_reuse() {
    let root = tempfile::tempdir().unwrap();
    let base = base_s3_table_metadata();
    let file_count = 756_usize;
    let rows_per_file = 84_500_usize;
    let values = (0..rows_per_file as i64).collect::<Vec<_>>();
    let shared_parquet = Arc::new(uncompressed_id_parquet(&values));
    let total_mib = (file_count * shared_parquet.len()) as f64 / (1024.0 * 1024.0);

    let meta_1 = "s3://bucket/owned/candidate/metadata/bench-00001.metadata.json";
    let meta_2 = "s3://bucket/owned/candidate/metadata/bench-00002.metadata.json";
    let list_1 = "s3://bucket/owned/candidate/metadata/bench-list-1.avro";
    let list_2 = "s3://bucket/owned/candidate/metadata/bench-list-2.avro";
    let manifest_1 = "s3://bucket/owned/candidate/metadata/bench-manifest-1.avro";
    let manifest_2 = "s3://bucket/owned/candidate/metadata/bench-manifest-2.avro";

    let mut day1_entries = Vec::with_capacity(file_count);
    let mut objects = HashMap::<String, Arc<Vec<u8>>>::new();
    for idx in 0..file_count {
        let uri = format!("s3://bucket/owned/candidate/data/bench-d1-{idx:03}.parquet");
        objects.insert(uri.clone(), shared_parquet.clone());
        day1_entries.push((uri, shared_parquet.len() as u64, rows_per_file as u64));
    }
    let borrowed_d1 = day1_entries
        .iter()
        .map(|(uri, size, rows)| (uri.as_str(), *size, *rows))
        .collect::<Vec<_>>();
    let (m1_file, m1_bytes) = write_manifest_bytes(&base, 1, manifest_1, &borrowed_d1[..740]).await;
    let (m1_tail, m1_tail_bytes) = write_manifest_bytes(
        &base,
        1,
        "s3://bucket/owned/candidate/metadata/bench-manifest-1b.avro",
        &borrowed_d1[740..],
    )
    .await;
    let (s1_meta, l1_bytes) =
        write_snapshot_version(&base, 1, list_1, vec![m1_file.clone(), m1_tail]).await;
    let s1_meta_bytes = serde_json::to_vec(&s1_meta).unwrap();
    objects.insert(meta_1.into(), Arc::new(s1_meta_bytes.clone()));
    objects.insert(list_1.into(), Arc::new(l1_bytes));
    objects.insert(manifest_1.into(), Arc::new(m1_bytes));
    objects.insert(
        "s3://bucket/owned/candidate/metadata/bench-manifest-1b.avro".into(),
        Arc::new(m1_tail_bytes),
    );

    let mut day2_new = Vec::with_capacity(16);
    for idx in 0..16_usize {
        let uri = format!("s3://bucket/owned/candidate/data/bench-d2-{idx:02}.parquet");
        objects.insert(uri.clone(), shared_parquet.clone());
        day2_new.push((uri, shared_parquet.len() as u64, rows_per_file as u64));
    }
    let borrowed_d2 = day2_new
        .iter()
        .map(|(uri, size, rows)| (uri.as_str(), *size, *rows))
        .collect::<Vec<_>>();
    let (m2_file, m2_bytes) = write_manifest_bytes(&base, 2, manifest_2, &borrowed_d2).await;
    let (s2_meta, l2_bytes) =
        write_snapshot_version(&base, 2, list_2, vec![m1_file, m2_file]).await;
    let s2_meta_bytes = serde_json::to_vec(&s2_meta).unwrap();
    objects.insert(meta_2.into(), Arc::new(s2_meta_bytes.clone()));
    objects.insert(list_2.into(), Arc::new(l2_bytes));
    objects.insert(manifest_2.into(), Arc::new(m2_bytes));

    let fixture = ReadFixture::concurrent_shared_inventory(objects, Duration::from_millis(2));
    let bounds = Limits {
        compute_threads: 4,
        memory_bytes: 512 << 20,
        scratch_bytes: 2 << 30,
        batch_rows: 8192,
        max_batch_bytes: 8 << 20,
        max_metadata_bytes: 16 << 20,
    };
    let mut engine = Engine::open(root.path().join("host"), bounds)
        .await
        .unwrap();
    engine.snapshot_source = Some(Arc::new(fixture.snapshot_source(None)));

    let t0 = std::time::Instant::now();
    let day1 = engine
        .managed_job(ManagedJobCommand::Prepare(s3_job_prepare(
            root.path(),
            "native-s3",
            "bench-day1",
            meta_1,
            &s1_meta,
            &s1_meta_bytes,
            file_count * rows_per_file,
            None,
        )))
        .await
        .unwrap();
    let day1_secs = t0.elapsed().as_secs_f64();
    let reqs_after_d1 = fixture.observed_paths();
    let d1_data_heads = reqs_after_d1
        .iter()
        .filter(|(m, u)| m == "HEAD" && u.contains("/data/"))
        .count();
    let d1_data_gets = reqs_after_d1
        .iter()
        .filter(|(m, u)| m == "GET" && u.contains("/data/"))
        .count();
    let d1_peak = fixture.peak_in_flight();

    let t1 = std::time::Instant::now();
    let day2 = engine
        .managed_job(ManagedJobCommand::Prepare(s3_job_prepare(
            root.path(),
            "native-s3",
            "bench-day2",
            meta_2,
            &s2_meta,
            &s2_meta_bytes,
            file_count * rows_per_file,
            Some(day1.input_custody[0].clone()),
        )))
        .await
        .unwrap();
    let day2_secs = t1.elapsed().as_secs_f64();
    assert_eq!(day2.status, "completed");
    let reqs_after_d2 = fixture.observed_paths();
    let d2_slice = &reqs_after_d2[reqs_after_d1.len()..];
    let d2_data_heads = d2_slice
        .iter()
        .filter(|(m, u)| m == "HEAD" && u.contains("/data/"))
        .count();
    let d2_data_gets = d2_slice
        .iter()
        .filter(|(m, u)| m == "GET" && u.contains("/data/"))
        .count();
    println!(
        "BENCH_STAGE_1B files={file_count} total_mib={total_mib:.2} \
         day1_secs={day1_secs:.3} day1_mib_s={:.1} day1_total_reqs={} day1_data_heads={d1_data_heads} day1_data_gets={d1_data_gets} peak_in_flight={d1_peak} \
         day2_secs={day2_secs:.3} day2_total_reqs={} day2_data_heads={d2_data_heads} day2_data_gets={d2_data_gets}",
        total_mib / day1_secs,
        reqs_after_d1.len(),
        d2_slice.len(),
    );
    let _ = fixture.finish();
}
