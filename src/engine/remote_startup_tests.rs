//! Public startup keeps explicit configuration and refuses unqualified mutations.
use super::*;
use std::collections::HashMap;

#[tokio::test]
async fn remote_constructor_keeps_host_custody_and_refuses_before_catalog_effects() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    // Construction is network-free: no client may contact this endpoint.
    let objects = storage::remote::remote_objects(
        "s3://bucket/owned",
        HashMap::from([
            ("s3.endpoint".into(), "http://127.0.0.1:9".into()),
            ("s3.region".into(), "auto".into()),
            ("s3.access-key-id".into(), "explicit-test-access".into()),
            ("s3.secret-access-key".into(), "explicit-test-secret".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::new(),
    )
    .unwrap();
    let remote = storage::remote::RemoteStorage {
        maintenance_generation: None,
        catalog: catalog.clone(),
        objects,
    };
    let host = root.path().join("host");
    let limits = Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 64 << 10,
        ..Limits::default()
    };
    let mut engine = Engine::with_remote(&host, remote.clone(), limits.clone())
        .await
        .unwrap();
    // This hybrid fixture uses a local filesystem lifecycle catalog; only
    // payload I/O is remote. Keep catalog files in its temporary warehouse.
    let inner = Arc::get_mut(&mut engine.inner).unwrap();
    inner.lifecycle = inner.lifecycle.clone().with_retained_prefix(None);
    assert!(!engine.inner.retained.is_local());
    assert!(!format!("{engine:?}").contains("explicit-test-secret"));
    assert!(Engine::with_remote(&host, remote, limits).await.is_err());
    let dataset = Id::new("remote-refusal").unwrap();
    let operation = Id::new("rebuild").unwrap();
    let base = Id::new("initial").unwrap();
    let build = RunSpec {
        dataset: dataset.clone(),
        operation: operation.clone(),
        base: Some(base.clone()),
        relationship: None,
        input: "/missing.parquet".into(),
        expected_digest: "0".repeat(64),
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let refused = |error: Error| assert_eq!(error.code(), "remote_build_unavailable");
    refused(engine.plan(&build).await.unwrap_err());
    refused(engine.run(&build).await.unwrap_err());
    let backfill = BackfillSpec {
        dataset: dataset.clone(),
        operation: operation.clone(),
        base,
        definition: "v1".into(),
        group: Id::new("computed").unwrap(),
        group_definition: "v1".into(),
        fields: vec![ComputedField {
            id: 1,
            name: "x".into(),
            expression: "id".into(),
        }],
    };
    assert_eq!(
        engine.plan_backfill(&backfill).await.unwrap_err().code(),
        "no_release"
    );
    assert_eq!(
        engine.backfill(&backfill).await.unwrap_err().code(),
        "no_release"
    );
    let continuous = ContinuousBackfillSpec {
        backfill,
        optimistic_rounds: 1,
        fence_millis: 1000,
    };
    refused(engine.continuous_backfill(&continuous).await.unwrap_err());
    refused(
        engine
            .plan_continuous_backfill(&continuous)
            .await
            .unwrap_err(),
    );
    refused(
        engine
            .invalidate(&dataset, "synthetic", None)
            .await
            .unwrap_err(),
    );
    assert_eq!(
        engine
            .abandon(&dataset, &operation, "synthetic")
            .await
            .unwrap_err()
            .code(),
        "no_operation"
    );
    assert_eq!(
        engine
            .capture_inputs(&BTreeMap::new())
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
    assert_eq!(
        engine.inspect(&dataset).await.unwrap_err().code(),
        "no_release"
    );
    assert_eq!(
        engine.latest(&dataset).await.unwrap_err().code(),
        "no_release"
    );
    assert_eq!(
        engine
            .release(&dataset, &operation)
            .await
            .unwrap_err()
            .code(),
        "no_release"
    );
    assert!(
        !catalog
            .namespace_exists(crate::catalog::lifecycle_ident(&dataset).namespace())
            .await
            .unwrap()
    );
    let update = UpdateSpec {
        dataset: dataset.clone(),
        operation: Id::new("unknown-update").unwrap(),
        definition: "v1".into(),
        base: Id::new("missing").unwrap(),
        keys: vec!["id".into()],
        replacements: InputSpec {
            path: "/missing-update.parquet".into(),
            digest: "0".repeat(64),
        },
        deletions: None,
        merge: None,
        relationship: None,
    };
    assert_eq!(
        engine.plan_update(&update).await.unwrap_err().code(),
        "no_release"
    );
    assert_eq!(
        engine.update(&update).await.unwrap_err().code(),
        "no_release"
    );
    assert!(
        !catalog
            .namespace_exists(crate::catalog::lifecycle_ident(&dataset).namespace())
            .await
            .unwrap()
    );
    // Existing selections cannot enter the unqualified unbased rebuild path.
    let mut initial = build.clone();
    initial.base = None;
    let selected = DatasetPin {
        format: FORMAT,
        dataset: dataset.clone(),
        version: Id::new("selected").unwrap(),
        groups: BTreeMap::new(),
        computed: BTreeMap::new(),
        inputs: BTreeMap::new(),
        relationship: SourceRelationship::Independent,
        related: None,
        evidence: None,
        definition: "v1".into(),
        eligibility: 0,
        boundary: 0,
        request: OperationRequest::Build(initial.clone()),
        parent_manifest: None,
        processor: PROCESSOR.into(),
        validation: VerificationLevel::Routine,
        partition_by: Vec::new(),
    };
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record(
                    "authority",
                    Authority {
                        selected: Some(selected),
                        definition: "v1".into(),
                        ..Authority::default()
                    },
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    refused(engine.plan(&initial).await.unwrap_err());
    refused(engine.run(&initial).await.unwrap_err());
    // A present but unsupported source shape also refuses before retaining the request.
    refused(engine.plan_update(&update).await.unwrap_err());
    refused(engine.update(&update).await.unwrap_err());
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<Operation>(&view, &format!("operation/{operation}"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .inner
            .lifecycle
            .get::<Operation>(&view, "operation/unknown-update")
            .await
            .unwrap()
            .is_none()
    );
    engine.shutdown().await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn remote_selection_refuses_second_prepared_initial_but_reuses_completed_first() {
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let limits = Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 64 << 10,
        ..Limits::default()
    };
    let local = Engine::with_catalog(
        root.path().join("prepare-host"),
        catalog.clone(),
        limits.clone(),
    )
    .await
    .unwrap();
    let source = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&source).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let first = RunSpec {
        dataset: Id::new("selection-race").unwrap(),
        operation: Id::new("first").unwrap(),
        base: None,
        relationship: None,
        input: source.to_str().unwrap().into(),
        expected_digest: storage::digest(&source).unwrap().0,
        sql: "SELECT value FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let mut second = first.clone();
    second.operation = Id::new("second").unwrap();
    second.sql = "SELECT value + 1 AS value FROM input".into();
    for spec in [&first, &second] {
        assert_eq!(
            local.run_until(spec, "readback").await.unwrap_err().code(),
            "injected_failure"
        );
    }
    let view = local.inner.lifecycle.open(&first.dataset).await.unwrap();
    let operation: Operation = local
        .inner
        .lifecycle
        .get(&view, "operation/second")
        .await
        .unwrap()
        .unwrap();
    let second_release: ReleasePin = local
        .inner
        .lifecycle
        .get(&view, "readback/second")
        .await
        .unwrap()
        .unwrap();
    let protections = local
        .retained_protections(&view, "readback/second")
        .await
        .unwrap();
    let selected = local.run(&first).await.unwrap();
    assert!(second_release.dataset.boundary > selected.dataset.boundary);
    assert_ne!(
        second_release.dataset.groups["source"].metadata,
        selected.dataset.groups["source"].metadata
    );
    local.shutdown().await.unwrap();
    drop(local);
    std::fs::remove_file(source).unwrap();
    // The candidates are real maintained native Local outputs; only selection
    // control is exercised remotely here. No remote payload client may be used.
    let objects = storage::remote::remote_objects(
        "s3://bucket/owned",
        HashMap::from([
            ("s3.endpoint".into(), "http://127.0.0.1:9".into()),
            ("s3.region".into(), "auto".into()),
            ("s3.access-key-id".into(), "explicit-test-access".into()),
            ("s3.secret-access-key".into(), "explicit-test-secret".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::new(),
    )
    .unwrap();
    let remote = Engine::with_remote(
        root.path().join("selection-host"),
        storage::remote::RemoteStorage {
            catalog,
            objects,
            maintenance_generation: None,
        },
        limits,
    )
    .await
    .unwrap();
    let error = remote
        .select_prepared(
            &OperationRequest::Build(second.clone()),
            &operation,
            Retained {
                value: second_release.clone(),
                protections,
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "remote_build_unavailable");
    let view = remote.inner.lifecycle.open(&first.dataset).await.unwrap();
    assert!(
        remote
            .inner
            .lifecycle
            .get::<ReleasePin>(&view, "result/second")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        remote
            .inner
            .lifecycle
            .get::<ReleasePin>(&view, "readback/second")
            .await
            .unwrap()
            .unwrap(),
        second_release
    );
    assert_eq!(remote.latest(&first.dataset).await.unwrap(), selected);
    assert_eq!(remote.run(&first).await.unwrap(), selected);
    assert_eq!(
        remote
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    remote.shutdown().await.unwrap();
}

struct MockS3 {
    endpoint: String,
    stop: Arc<std::sync::atomic::AtomicBool>,
    address: std::net::SocketAddr,
    server: Option<std::thread::JoinHandle<()>>,
}

impl MockS3 {
    fn start() -> Self {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        type UploadParts = BTreeMap<String, BTreeMap<u64, Vec<u8>>>;
        let objects: Arc<std::sync::Mutex<BTreeMap<String, Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        let uploads: Arc<std::sync::Mutex<UploadParts>> =
            Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        let next_upload = Arc::new(std::sync::atomic::AtomicU64::new(1));

        let server = std::thread::spawn(move || {
            loop {
                let Ok((mut socket, _)) = listener.accept() else {
                    break;
                };
                if server_stop.load(Ordering::Acquire) {
                    break;
                }
                let objects = objects.clone();
                let uploads = uploads.clone();
                let next_upload = next_upload.clone();
                std::thread::spawn(move || {
                    let _ = socket.set_nonblocking(false);
                    let _ = socket.set_read_timeout(Some(Duration::from_secs(30)));
                    let _ = socket.set_write_timeout(Some(Duration::from_secs(30)));
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        match socket.read(&mut byte) {
                            Ok(1) => header.push(byte[0]),
                            _ => return,
                        }
                    }
                    let header_str = String::from_utf8_lossy(&header);
                    let mut first_line = header_str.lines().next().unwrap_or("").split_whitespace();
                    let method = first_line.next().unwrap_or("").to_owned();
                    let target = first_line.next().unwrap_or("").to_owned();
                    let find_header = |name: &str| -> Option<String> {
                        header_str.lines().find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
                        })
                    };
                    let length = find_header("content-length")
                        .and_then(|v| v.parse::<usize>().ok())
                        .unwrap_or(0);
                    let mut body = vec![0; length];
                    if length > 0 && socket.read_exact(&mut body).is_err() {
                        return;
                    }
                    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                    let query_val = |key: &str| -> Option<String> {
                        query.split('&').find_map(|item| {
                            let (k, v) = item.split_once('=')?;
                            (k == key).then(|| v.to_owned())
                        })
                    };
                    let create_only = find_header("if-none-match").as_deref() == Some("*");
                    let range = find_header("range");

                    match method.as_str() {
                        "POST"
                            if query
                                .split('&')
                                .any(|item| item == "uploads" || item == "uploads=") =>
                        {
                            let id = format!("u-{}", next_upload.fetch_add(1, Ordering::Relaxed));
                            uploads.lock().unwrap().insert(id.clone(), BTreeMap::new());
                            let xml = format!(
                                "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>{}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
                                path.trim_start_matches("/bucket/")
                            );
                            let _ = write!(
                                socket,
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                xml.len()
                            );
                            let _ = socket.write_all(xml.as_bytes());
                        }
                        "PUT" if query_val("uploadId").is_some() => {
                            let id = query_val("uploadId").unwrap();
                            let part = query_val("partNumber")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(1);
                            if let Some(parts) = uploads.lock().unwrap().get_mut(&id) {
                                parts.insert(part, body);
                            }
                            let _ = write!(
                                socket,
                                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"part\"\r\nConnection: close\r\n\r\n"
                            );
                        }
                        "POST" if query_val("uploadId").is_some() => {
                            let id = query_val("uploadId").unwrap();
                            let parts = uploads.lock().unwrap().remove(&id).unwrap_or_default();
                            let full: Vec<u8> = parts.into_values().flatten().collect();
                            let mut store = objects.lock().unwrap();
                            if create_only && store.contains_key(path) {
                                let err = b"<Error><Code>PreconditionFailed</Code></Error>";
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 412 Precondition Failed\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    err.len()
                                );
                                let _ = socket.write_all(err);
                                return;
                            }
                            store.insert(path.to_owned(), full);
                            let xml = "<CompleteMultipartUploadResult><ETag>\"obj\"</ETag></CompleteMultipartUploadResult>";
                            let _ = write!(
                                socket,
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                xml.len()
                            );
                            let _ = socket.write_all(xml.as_bytes());
                        }
                        "PUT" => {
                            let mut store = objects.lock().unwrap();
                            if create_only && store.contains_key(path) {
                                let err = b"<Error><Code>PreconditionFailed</Code></Error>";
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 412 Precondition Failed\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    err.len()
                                );
                                let _ = socket.write_all(err);
                                return;
                            }
                            store.insert(path.to_owned(), body);
                            let _ = write!(
                                socket,
                                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"obj\"\r\nConnection: close\r\n\r\n"
                            );
                        }
                        "DELETE" => {
                            if let Some(id) = query_val("uploadId") {
                                uploads.lock().unwrap().remove(&id);
                            } else {
                                objects.lock().unwrap().remove(path);
                            }
                            let _ = write!(
                                socket,
                                "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            );
                        }
                        "HEAD" => {
                            let store = objects.lock().unwrap();
                            if let Some(bytes) = store.get(path) {
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"obj\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n",
                                    bytes.len()
                                );
                            } else {
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                );
                            }
                        }
                        "GET" => {
                            let store = objects.lock().unwrap();
                            let Some(bytes) = store.get(path).cloned() else {
                                let err = b"<Error><Code>NoSuchKey</Code></Error>";
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    err.len()
                                );
                                let _ = socket.write_all(err);
                                return;
                            };
                            drop(store);
                            if let Some(range_hdr) = range {
                                let (start, end) = range_hdr
                                    .strip_prefix("bytes=")
                                    .and_then(|r| r.split_once('-'))
                                    .map(|(s, e)| {
                                        if s.is_empty() {
                                            (
                                                bytes.len().saturating_sub(
                                                    e.parse::<usize>().unwrap_or(0),
                                                ),
                                                bytes.len(),
                                            )
                                        } else {
                                            let st = s.parse::<usize>().unwrap_or(0);
                                            let en = if e.is_empty() {
                                                bytes.len()
                                            } else {
                                                (e.parse::<usize>().unwrap_or(0) + 1)
                                                    .min(bytes.len())
                                            };
                                            (st, en)
                                        }
                                    })
                                    .unwrap_or((0, bytes.len()));
                                let slice = &bytes[start..end];
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{}\r\nETag: \"obj\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n",
                                    slice.len(),
                                    end.saturating_sub(1),
                                    bytes.len()
                                );
                                let _ = socket.write_all(slice);
                            } else {
                                let _ = write!(
                                    socket,
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"obj\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n",
                                    bytes.len()
                                );
                                let _ = socket.write_all(&bytes);
                            }
                        }
                        _ => {}
                    }
                });
            }
        });

        Self {
            endpoint,
            stop,
            address,
            server: Some(server),
        }
    }

    fn properties(&self) -> HashMap<String, String> {
        HashMap::from([
            ("s3.endpoint".into(), self.endpoint.clone()),
            ("s3.region".into(), "auto".into()),
            ("s3.access-key-id".into(), "test-access".into()),
            ("s3.secret-access-key".into(), "test-secret".into()),
            ("s3.path-style-access".into(), "true".into()),
        ])
    }
}

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let _ =
            std::net::TcpStream::connect_timeout(&self.address, std::time::Duration::from_secs(2));
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

#[tokio::test]
async fn remote_lifecycle_refresh_backfill_evidence_and_maintenance_verify_deep() {
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use parquet::arrow::ArrowWriter;

    let root = tempfile::tempdir().unwrap();
    let s3 = MockS3::start();
    let props = s3.properties();
    let sqlite_path = root.path().join("catalog.sqlite");
    let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        SqlCatalogBuilder::default()
            .uri(format!("sqlite://{}?mode=rwc", sqlite_path.display()))
            .warehouse_location("s3://bucket/owned-deep")
            .sql_bind_style(SqlBindStyle::QMark)
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("docspec", props.clone())
            .await
            .unwrap(),
    );
    let objects =
        storage::remote::remote_objects("s3://bucket/owned-deep", props, HashMap::new()).unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 128 << 20,
        scratch_bytes: 32 << 20,
        max_metadata_bytes: 4 << 20,
        max_batch_bytes: 1 << 20,
        ..Limits::default()
    };
    let engine = Engine::with_remote(
        root.path().join("host"),
        storage::remote::RemoteStorage {
            catalog,
            objects,
            maintenance_generation: None,
        },
        limits,
    )
    .await
    .unwrap();

    // 1. Coupled Build + Seal Generation + Capture Inputs
    let write_parquet = |name: &str, ids: &[i64], labels: &[&str]| -> (String, String) {
        let path = root.path().join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let digest = storage::digest(&path).unwrap().0;
        (path.to_string_lossy().into_owned(), digest)
    };

    let (left_path, left_digest) = write_parquet("left1.parquet", &[1, 2], &["a", "b"]);
    let (right_path, right_digest) = write_parquet("right1.parquet", &[1, 2], &["x", "y"]);
    let family = Id::new("fam").unwrap();
    let gen1 = Id::new("gen-1").unwrap();
    let left_ds = Id::new("left-ds").unwrap();
    let right_ds = Id::new("right-ds").unwrap();

    let (left_ev, right_rel1) = Box::pin(async {
        let left_rel1 = engine
            .run(&RunSpec {
                dataset: left_ds.clone(),
                operation: Id::new("b1").unwrap(),
                base: None,
                relationship: Some(SourceRelationship::Coupled {
                    family: family.clone(),
                    generation: gen1.clone(),
                }),
                input: left_path,
                expected_digest: left_digest,
                sql: "SELECT id, label FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();

        let right_rel1 = engine
            .run(&RunSpec {
                dataset: right_ds.clone(),
                operation: Id::new("b1").unwrap(),
                base: None,
                relationship: Some(SourceRelationship::Coupled {
                    family: family.clone(),
                    generation: gen1.clone(),
                }),
                input: right_path,
                expected_digest: right_digest,
                sql: "SELECT id, label FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();

        // 2. BackfillSpec + read_group on computed group + read_combined
        let backfill_spec = BackfillSpec {
            dataset: left_ds.clone(),
            operation: Id::new("bf1").unwrap(),
            base: left_rel1.id.clone(),
            definition: "v1".into(),
            group: Id::new("metrics").unwrap(),
            group_definition: "m-v1".into(),
            fields: vec![ComputedField {
                id: 1,
                name: "id_plus_ten".into(),
                expression: "id + 10".into(),
            }],
        };
        let bf_plan = engine.plan_backfill(&backfill_spec).await.unwrap();
        assert!(!bf_plan.completed);
        let left_bf = engine.backfill(&backfill_spec).await.unwrap();
        let mut comp_stream = engine.read_group(&left_bf, "metrics").await.unwrap();
        let comp_batch = comp_stream.try_next().await.unwrap().unwrap();
        assert_eq!(comp_batch.num_rows(), 2);
        drop(comp_stream);

        let mut comb_stream = engine.read_combined(&left_bf).await.unwrap();
        let comb_batch = comb_stream.try_next().await.unwrap().unwrap();
        assert_eq!(comb_batch.num_rows(), 2);
        assert!(comb_batch.schema().index_of("id_plus_ten").is_ok());
        drop(comb_stream);

        // 3. EvidenceSpec + read_source_occurrences + read_receipts
        let mut occ_stream = engine.read_source_occurrences(&left_bf).await.unwrap();
        let occ_batch = occ_stream.try_next().await.unwrap().unwrap();
        let source_file = occ_batch
            .column_by_name(storage::OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned();
        let source_row = occ_batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        drop(occ_stream);

        let ev_path = root.path().join("evidence1.parquet");
        let ev_schema = Arc::new(Schema::new(vec![
            Field::new(crate::evidence::SOURCE_FILE, DataType::Utf8, false),
            Field::new(crate::evidence::SOURCE_ROW, DataType::Int64, false),
            Field::new(crate::evidence::EVIDENCE, DataType::Utf8, true),
        ]));
        let ev_batch = RecordBatch::try_new(
            ev_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![source_file])),
                Arc::new(Int64Array::from(vec![source_row])),
                Arc::new(StringArray::from(vec![Some("receipt-ok")])),
            ],
        )
        .unwrap();
        let mut ev_writer =
            ArrowWriter::try_new(std::fs::File::create(&ev_path).unwrap(), ev_schema, None)
                .unwrap();
        ev_writer.write(&ev_batch).unwrap();
        ev_writer.close().unwrap();
        let ev_spec = crate::evidence::EvidenceSpec {
            dataset: left_ds.clone(),
            operation: Id::new("ev1").unwrap(),
            definition: "v1".into(),
            base: left_bf.id.clone(),
            replacements: InputSpec {
                path: ev_path.to_string_lossy().into_owned(),
                digest: storage::digest(&ev_path).unwrap().0,
            },
            deletions: None,
        };
        let ev_plan = engine.plan_evidence(&ev_spec).await.unwrap();
        assert!(!ev_plan.completed);
        let left_ev = engine.update_evidence(&ev_spec).await.unwrap();
        let mut rec_stream = engine.read_receipts(&left_ev).await.unwrap();
        let rec_batch = rec_stream.try_next().await.unwrap().unwrap();
        assert_eq!(rec_batch.num_rows(), 1);
        drop(rec_stream);

        let seal1 = GenerationSeal {
            family: family.clone(),
            generation: gen1.clone(),
            members: BTreeMap::from([
                (left_ds.clone(), left_ev.version_ref()),
                (right_ds.clone(), right_rel1.version_ref()),
            ]),
        };
        assert_eq!(engine.seal_generation(&seal1).await.unwrap(), seal1);

        let captured = engine
            .capture_inputs(&BTreeMap::from([
                (left_ds.clone(), left_ev.version_ref()),
                (right_ds.clone(), right_rel1.version_ref()),
            ]))
            .await
            .unwrap();
        assert_eq!(captured.inputs.len(), 2);
        (left_ev, right_rel1)
    })
    .await;

    // 4. RefreshSpec (Join Full + Incremental, Aggregate Full + Incremental) + read_evidence
    let (joined_ds, joined_rel1, joined_rel2, agg_ds, agg_rel2) = Box::pin(async {
        let joined_ds = Id::new("joined-ds").unwrap();
        let join_def = JoinDefinition {
            keys: vec![JoinKey {
                left: "id".into(),
                right: "id".into(),
            }],
            kind: JoinKind::Inner,
            nulls: NullMatching::Never,
            cardinality: JoinCardinality::OneToOne,
            fields: vec![
                JoinField {
                    side: JoinSide::Left,
                    field: "id".into(),
                    name: "id".into(),
                },
                JoinField {
                    side: JoinSide::Left,
                    field: "label".into(),
                    name: "left_label".into(),
                },
                JoinField {
                    side: JoinSide::Right,
                    field: "label".into(),
                    name: "right_label".into(),
                },
            ],
            filter: None,
            projections: BTreeMap::new(),
        };
        let join_full = RefreshSpec {
            dataset: joined_ds.clone(),
            operation: Id::new("j-full").unwrap(),
            base: None,
            definition: "j-v1".into(),
            transform: RefreshTransform::Join(JoinTransform {
                left: left_ev.version_ref(),
                right: right_rel1.version_ref(),
                join: join_def.clone(),
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
        };
        let joined_rel1 = engine.refresh(&join_full).await.unwrap();
        let mut j_ev_stream = engine.read_evidence(&joined_rel1).await.unwrap();
        let j_ev_batch = j_ev_stream.try_next().await.unwrap().unwrap();
        assert_eq!(j_ev_batch.num_rows(), 2);
        drop(j_ev_stream);

        let agg_ds = Id::new("agg-ds").unwrap();
        let agg_def = AggregationDefinition {
            keys: vec!["left_label".into()],
            aggregates: vec![
                AggregateField {
                    name: "n".into(),
                    function: AggregateFunction::CountRows,
                },
                AggregateField {
                    name: "total_id".into(),
                    function: AggregateFunction::Sum { field: "id".into() },
                },
            ],
            filter: None,
            projections: BTreeMap::new(),
        };
        let agg_full = RefreshSpec {
            dataset: agg_ds.clone(),
            operation: Id::new("a-full").unwrap(),
            base: None,
            definition: "a-v1".into(),
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: joined_rel1.version_ref(),
                grouping: agg_def.clone(),
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
        };
        let agg_rel1 = engine.refresh(&agg_full).await.unwrap();

        // Update left_ds (Coupled gen-2) and right_ds (Coupled gen-2), seal gen-2, then incremental refresh!
        let gen2 = Id::new("gen-2").unwrap();
        let (left_upd_path, left_upd_digest) = write_parquet("left2.parquet", &[2], &["b2"]);
        let left_rel2 = engine
            .update(&UpdateSpec {
                dataset: left_ds.clone(),
                operation: Id::new("u1").unwrap(),
                definition: "v1".into(),
                base: left_ev.id.clone(),
                keys: vec!["id".into()],
                replacements: InputSpec {
                    path: left_upd_path,
                    digest: left_upd_digest,
                },
                deletions: None,
                merge: None,
                relationship: Some(SourceRelationship::Coupled {
                    family: family.clone(),
                    generation: gen2.clone(),
                }),
            })
            .await
            .unwrap();
        let (right_upd_path, right_upd_digest) = write_parquet("right2.parquet", &[2], &["y2"]);
        let right_rel2 = engine
            .update(&UpdateSpec {
                dataset: right_ds.clone(),
                operation: Id::new("u1").unwrap(),
                definition: "v1".into(),
                base: right_rel1.id.clone(),
                keys: vec!["id".into()],
                replacements: InputSpec {
                    path: right_upd_path,
                    digest: right_upd_digest,
                },
                deletions: None,
                merge: None,
                relationship: Some(SourceRelationship::Coupled {
                    family: family.clone(),
                    generation: gen2.clone(),
                }),
            })
            .await
            .unwrap();
        let seal2 = GenerationSeal {
            family: family.clone(),
            generation: gen2.clone(),
            members: BTreeMap::from([
                (left_ds.clone(), left_rel2.version_ref()),
                (right_ds.clone(), right_rel2.version_ref()),
            ]),
        };
        engine.seal_generation(&seal2).await.unwrap();

        let join_inc = RefreshSpec {
            dataset: joined_ds.clone(),
            operation: Id::new("j-inc").unwrap(),
            base: Some(joined_rel1.id.clone()),
            definition: "j-v1".into(),
            transform: RefreshTransform::Join(JoinTransform {
                left: left_rel2.version_ref(),
                right: right_rel2.version_ref(),
                join: join_def,
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Incremental,
            relationship: None,
        };
        let joined_rel2 = engine.refresh(&join_inc).await.unwrap();

        let agg_inc = RefreshSpec {
            dataset: agg_ds.clone(),
            operation: Id::new("a-inc").unwrap(),
            base: Some(agg_rel1.id.clone()),
            definition: "a-v1".into(),
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: joined_rel2.version_ref(),
                grouping: agg_def,
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Incremental,
            relationship: None,
        };
        let agg_rel2 = engine.refresh(&agg_inc).await.unwrap();
        (joined_ds, joined_rel1, joined_rel2, agg_ds, agg_rel2)
    })
    .await;

    // 5. prove_succession + compact_lifecycle + abandon + Deep verification
    let proof = engine
        .prove_succession(&joined_rel1.version_ref(), &joined_rel2.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 1);

    for ds in [&left_ds, &right_ds, &joined_ds, &agg_ds] {
        let rel = engine.latest(ds).await.unwrap();
        let report = verify_release_at(
            &rel,
            engine.inner.lifecycle.budget.limits.clone(),
            VerificationLevel::Deep,
        )
        .await
        .unwrap();
        assert_eq!(report.level, VerificationLevel::Deep);
        let compacted = engine.compact_lifecycle(ds).await.unwrap();
        assert_eq!(compacted.revisions_after, 1);
        let rel_after = engine.latest(ds).await.unwrap();
        let report_after = verify_release_at(
            &rel_after,
            engine.inner.lifecycle.budget.limits.clone(),
            VerificationLevel::Deep,
        )
        .await
        .unwrap();
        assert_eq!(report_after.level, VerificationLevel::Deep);
    }
    assert_eq!(engine.latest(&agg_ds).await.unwrap(), agg_rel2);

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn fair_spill_pool_512mb_stress_test_completes_and_cleans_scratch() {
    use crate::export::{ExportPolicy, ExportRows, ExportSpec};
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::execution::memory_pool::MemoryConsumer;
    use parquet::arrow::ArrowWriter;

    let root = tempfile::tempdir().unwrap();
    let host = root.path().join("host");
    // 1024 MiB total budget -> 512 MiB FairSpillPool (`limits.memory_bytes / 2`).
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 1024 << 20,
        scratch_bytes: 512 << 20,
        batch_rows: 512,
        max_batch_bytes: 8 << 20,
        max_metadata_bytes: 4 << 20,
    };
    let engine = Engine::open(&host, limits).await.unwrap();
    let runtime_env = engine.inner.lifecycle.budget.runtime_env();

    // Reserve all but 16 MiB of the 512 MiB FairSpillPool to stress bounded memory & spilling.
    let ballast = MemoryConsumer::new("512mb-fair-spill-stress")
        .with_can_spill(false)
        .register(&runtime_env.memory_pool);
    ballast.try_grow((512 << 20) - (16 << 20)).unwrap();

    let write_large_parquet = |name: &str, count: i64, start: i64, tag: &str| -> (String, String) {
        let path = root.path().join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("grp", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (start..start + count).collect();
        let grps: Vec<String> = ids.iter().map(|i| format!("g-{:03}", i % 64)).collect();
        let payloads: Vec<String> = ids
            .iter()
            .map(|i| format!("{tag}-{i:06}-{}", "x".repeat(128)))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(grps)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let digest = storage::digest(&path).unwrap().0;
        (path.to_string_lossy().into_owned(), digest)
    };

    let (left_p1, left_d1) = write_large_parquet("left_init.parquet", 2048, 0, "L1");
    let (right_p1, right_d1) = write_large_parquet("right_init.parquet", 2048, 0, "R1");
    let left_ds = Id::new("stress-left").unwrap();
    let right_ds = Id::new("stress-right").unwrap();

    let left_r1 = engine
        .run(&RunSpec {
            dataset: left_ds.clone(),
            operation: Id::new("b1").unwrap(),
            base: None,
            relationship: None,
            input: left_p1,
            expected_digest: left_d1,
            sql: "SELECT id, grp, payload FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let right_r1 = engine
        .run(&RunSpec {
            dataset: right_ds.clone(),
            operation: Id::new("b1").unwrap(),
            base: None,
            relationship: None,
            input: right_p1,
            expected_digest: right_d1,
            sql: "SELECT id, grp, payload FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // 1. replace_keys via UpdateSpec
    let (left_p2, left_d2) = write_large_parquet("left_upd.parquet", 512, 256, "L2");
    let left_r2 = engine
        .update(&UpdateSpec {
            dataset: left_ds.clone(),
            operation: Id::new("u1").unwrap(),
            definition: "v1".into(),
            base: left_r1.id.clone(),
            keys: vec!["id".into()],
            replacements: InputSpec {
                path: left_p2,
                digest: left_d2,
            },
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    // 2. RefreshTransform::Join
    let join_ds = Id::new("stress-join").unwrap();
    let join_rel = engine
        .refresh(&RefreshSpec {
            dataset: join_ds.clone(),
            operation: Id::new("j1").unwrap(),
            base: None,
            definition: "jv1".into(),
            transform: RefreshTransform::Join(JoinTransform {
                left: left_r2.version_ref(),
                right: right_r1.version_ref(),
                join: JoinDefinition {
                    keys: vec![JoinKey {
                        left: "id".into(),
                        right: "id".into(),
                    }],
                    kind: JoinKind::Inner,
                    nulls: NullMatching::Never,
                    cardinality: JoinCardinality::OneToOne,
                    fields: vec![
                        JoinField {
                            side: JoinSide::Left,
                            field: "id".into(),
                            name: "id".into(),
                        },
                        JoinField {
                            side: JoinSide::Left,
                            field: "grp".into(),
                            name: "grp".into(),
                        },
                        JoinField {
                            side: JoinSide::Left,
                            field: "payload".into(),
                            name: "l_payload".into(),
                        },
                        JoinField {
                            side: JoinSide::Right,
                            field: "payload".into(),
                            name: "r_payload".into(),
                        },
                    ],
                    filter: None,
                    projections: BTreeMap::new(),
                },
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
        })
        .await
        .unwrap();

    // 3. RefreshTransform::Aggregate
    let agg_ds = Id::new("stress-agg").unwrap();
    let agg_rel = engine
        .refresh(&RefreshSpec {
            dataset: agg_ds.clone(),
            operation: Id::new("a1").unwrap(),
            base: None,
            definition: "av1".into(),
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: join_rel.version_ref(),
                grouping: AggregationDefinition {
                    keys: vec!["grp".into()],
                    aggregates: vec![
                        AggregateField {
                            name: "cnt".into(),
                            function: AggregateFunction::CountRows,
                        },
                        AggregateField {
                            name: "sum_id".into(),
                            function: AggregateFunction::Sum { field: "id".into() },
                        },
                    ],
                    filter: None,
                    projections: BTreeMap::new(),
                },
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
        })
        .await
        .unwrap();

    // 4. ExportSpec
    let exported = engine
        .export(&ExportSpec {
            dataset: agg_ds.clone(),
            operation: Id::new("exp1").unwrap(),
            source: agg_rel.version_ref(),
            channel: Id::new("main").unwrap(),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec!["grp".into(), "cnt".into(), "sum_id".into()],
                ..ExportPolicy::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(exported.rows, 64);

    drop(ballast);
    assert_eq!(runtime_env.memory_pool.reserved(), 0);
    assert_eq!(runtime_env.disk_manager.used_disk_space(), 0);
    assert_eq!(
        runtime_env
            .disk_manager
            .spilling_progress()
            .active_files_count,
        0
    );
    let scratch_dir = host.join("scratch");
    if scratch_dir.exists() {
        for entry in std::fs::read_dir(&scratch_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
        {
            let path = entry.path();
            if path.is_dir() {
                let inner: Vec<_> = std::fs::read_dir(&path)
                    .unwrap()
                    .filter_map(std::result::Result::ok)
                    .collect();
                assert!(
                    inner.is_empty(),
                    "expected no leftover spill files in {}, found: {inner:?}",
                    path.display()
                );
            } else {
                panic!("unexpected leftover file in scratch: {}", path.display());
            }
        }
    }
    engine.shutdown().await.unwrap();
    drop(engine);
    if scratch_dir.exists() {
        let leftover: Vec<_> = std::fs::read_dir(&scratch_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .collect();
        assert!(
            leftover.is_empty(),
            "expected empty scratch directory after engine drop, found: {leftover:?}"
        );
    }
}
