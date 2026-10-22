//! Actual Engine release interruption and exact destination recovery.
use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

#[tokio::test]
async fn interrupted_release_reuses_exact_uri_and_completed_readback_without_second_decode() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("producer.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(1)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&source).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("host"),
        Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            max_metadata_bytes: 1 << 20,
            max_batch_bytes: 256 << 10,
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let spec = RunSpec {
        dataset: Id::new("dataset").unwrap(),
        operation: Id::new("initial").unwrap(),
        base: None,
        relationship: None,
        input: source.to_str().unwrap().into(),
        expected_digest: storage::digest(&source).unwrap().0,
        sql: "SELECT value FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let error = engine.run_until(&spec, "release-create").await.unwrap_err();
    assert_eq!(error.code(), "injected_failure");
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (revision, _, _) = rows
        .get_record::<Operation>(&format!("operation/{}", spec.operation))
        .unwrap()
        .unwrap();
    let pending_prefix = format!("release-attempt/{}/{revision}/", spec.operation);
    let (pending_key, _, json) = rows.prefix(&pending_prefix).next().unwrap();
    let pending: ReleaseAttempt = serde_json::from_str(json).unwrap();
    validate(
        pending_key,
        &pending,
        &rows,
        Some(&engine.inner.lifecycle.budget),
    )
    .unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<ReleasePin>(&view, &format!("readback/{}", spec.operation))
            .await
            .unwrap()
            .is_none()
    );
    let bytes = std::fs::read(&pending.manifest.path).unwrap();
    assert_eq!(bytes.len() as u64, pending.manifest.bytes);
    assert_eq!(
        engine
            .inner
            .release_decodes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    let query_count = engine
        .inner
        .initial_build_queries
        .load(std::sync::atomic::Ordering::Relaxed);
    std::fs::remove_file(&source).unwrap();
    // A present wrong digest never becomes absence and never gets replaced.
    let mut wrong = bytes.clone();
    wrong[0] ^= 1;
    std::fs::write(&pending.manifest.path, &wrong).unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "corrupt_object"
    );
    assert_eq!(std::fs::read(&pending.manifest.path).unwrap(), wrong);
    std::fs::write(&pending.manifest.path, &bytes).unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let release = engine
        .inner
        .lifecycle
        .get::<ReleasePin>(&view, &format!("readback/{}", spec.operation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(release.manifest, pending.manifest);
    let decode_count = engine
        .inner
        .release_decodes
        .load(std::sync::atomic::Ordering::Relaxed);
    let modified = std::fs::metadata(&release.manifest.path)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .inner
            .release_decodes
            .load(std::sync::atomic::Ordering::Relaxed),
        decode_count
    );
    assert_eq!(
        engine
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        query_count
    );
    assert_eq!(
        std::fs::metadata(&release.manifest.path)
            .unwrap()
            .modified()
            .unwrap(),
        modified
    );
    let selected = engine.run(&spec).await.unwrap();
    assert_eq!(selected.manifest, pending.manifest);
    assert_eq!(selected.dataset.groups["source"].rows, 3);
    engine.shutdown().await.unwrap();
}

#[cfg(feature = "remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_release_absence_is_only_initial_head_not_permission_error() {
    use crate::storage::remote::objects::tests::writes::{Behavior, WriteFixture};
    for (behavior, absent) in [(Behavior::RejectPut, true), (Behavior::HeadDenied, false)] {
        let fixture = WriteFixture::for_path(behavior, "/bucket/owned/release.json".into());
        let objects = storage::Objects::Remote(fixture.objects());
        let expected = ObjectRef {
            owner: Id::new("owner").unwrap(),
            path: "s3://bucket/owned/release.json".into(),
            digest: "0".repeat(64),
            bytes: 16,
        };
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        })
        .unwrap();
        let memory = Arc::new(budget.reserve(4096).unwrap());
        let result = inputs::read_manifest_if_present::<DatasetPin>(
            &objects, &expected, &budget, &mut 0, &memory,
        )
        .await;
        if absent {
            assert!(result.unwrap().is_none());
        } else {
            assert_eq!(result.unwrap_err().code(), "remote_read");
        }
        let report = fixture.finish();
        assert_eq!(report.methods("HEAD"), 1);
        assert_eq!(report.methods("GET"), 0);
        assert_eq!(report.methods("PUT"), 0);
    }
}
