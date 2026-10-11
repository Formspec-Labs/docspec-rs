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
    };
    let refused = |error: Error| assert_eq!(error.code(), "remote_build_unavailable");
    refused(engine.plan(&build).await.unwrap_err());
    refused(engine.run(&build).await.unwrap_err());
    let mut coupled = build.clone();
    coupled.base = None;
    coupled.relationship = Some(SourceRelationship::Coupled {
        family: Id::new("family").unwrap(),
        generation: Id::new("generation").unwrap(),
    });
    refused(engine.run(&coupled).await.unwrap_err());
    let backfill = BackfillSpec {
        dataset: dataset.clone(),
        operation: operation.clone(),
        base,
        definition: "v1".into(),
        group: Id::new("computed").unwrap(),
        group_definition: "v1".into(),
        fields: Vec::new(),
    };
    refused(engine.plan_backfill(&backfill).await.unwrap_err());
    refused(engine.backfill(&backfill).await.unwrap_err());
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
    refused(
        engine
            .abandon(&dataset, &operation, "synthetic")
            .await
            .unwrap_err(),
    );
    refused(engine.capture_inputs(&BTreeMap::new()).await.unwrap_err());
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
