//! Actual remote source admission through the owned Engine service and native lifecycle.

use super::*;
use crate::storage::remote::objects::tests::writes::{Behavior, WriteFixture};
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::sync::atomic::{AtomicUsize, Ordering};

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

pub(in crate::engine) async fn fixture(
    objects: storage::remote::RemoteObjects,
) -> (tempfile::TempDir, Engine, RunSpec, Vec<u8>) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("producer.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(1),
            Some(2),
        ]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = std::fs::read(&input).unwrap();
    let host = root.path().join("host");
    let ownership = worker::Host::acquire(&host).unwrap();
    let catalog = storage::local_catalog(&host).await.unwrap();
    let mut engine = Engine::compose(
        std::fs::canonicalize(&host).unwrap(),
        catalog,
        storage::Objects::Remote(objects),
        limits(),
        ownership,
        false,
    )
    .unwrap();
    // This hybrid fixture uses a local filesystem lifecycle catalog; only
    // payload I/O is remote. Keep catalog files in its temporary warehouse.
    let inner = Arc::get_mut(&mut engine.inner).unwrap();
    inner.lifecycle = inner.lifecycle.clone().with_retained_prefix(None);
    let spec = RunSpec {
        dataset: Id::new("dataset").unwrap(),
        operation: Id::new("initial").unwrap(),
        base: None,
        relationship: None,
        input: input.to_str().unwrap().into(),
        expected_digest: blake3::hash(&bytes).to_hex().to_string(),
        sql: "SELECT value FROM input".into(),
        definition: "v1".into(),
    };
    (root, engine, spec, bytes)
}

pub(in crate::engine) async fn saved(
    engine: &Engine,
    spec: &RunSpec,
) -> (SourceAttempt, Option<PhaseResult>) {
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let (revision, _, operation) = rows
        .get_record::<Operation>(&format!("operation/{}", spec.operation))
        .unwrap()
        .unwrap();
    let key = source_attempt_key(&spec.operation, revision);
    let (_, _, pending) = rows.get_record::<SourceAttempt>(&key).unwrap().unwrap();
    validate_source_attempt(&key, &pending, &rows).unwrap();
    let phase = phase_key(
        &OperationRequest::Build(spec.clone()),
        &operation.request_digest,
        operation.eligibility,
        "admission",
        operation.base_manifest.as_ref(),
        operation.schema_key.as_ref(),
    )
    .unwrap();
    (pending, lifecycle.get(&view, &phase).await.unwrap())
}

fn assert_protection_before_put(
    fixture: &WriteFixture,
    engine: &Engine,
    spec: &RunSpec,
) -> Arc<AtomicUsize> {
    let checked = Arc::new(AtomicUsize::new(0));
    let checks = checked.clone();
    let engine = engine.clone();
    let spec = spec.clone();
    let handle = tokio::runtime::Handle::current();
    fixture.check_put(Arc::new(move |path| {
        handle.block_on(async {
            let (pending, _) = saved(&engine, &spec).await;
            let expected = pending.inputs.get("input").unwrap();
            assert_eq!(
                expected.path,
                format!("s3://{}", path.strip_prefix('/').unwrap())
            );
            assert_eq!(expected.owner, spec.dataset);
            assert_eq!(expected.digest, spec.expected_digest);
            let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
            let reservation: Protection = engine
                .inner
                .lifecycle
                .get(&view, &format!("protection/{}", pending.attempt))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reservation.paths, [pending.prefix]);
            assert!(!reservation.retired);
            checks.fetch_add(1, Ordering::SeqCst);
        });
    }));
    checked
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_remote_admission_resumes_same_destination_then_reuses_completed_phase() {
    let mut server = WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let (_root, engine, spec, bytes) = fixture(server.objects()).await;
    let checks = assert_protection_before_put(&server, &engine, &spec);
    assert_eq!(
        engine
            .run_until(&spec, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (reserved, incomplete) = saved(&engine, &spec).await;
    assert!(incomplete.is_none());
    assert_eq!(
        checks.load(Ordering::SeqCst),
        0,
        "interruption precedes PUT"
    );
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (resumed, completed) = saved(&engine, &spec).await;
    assert_eq!(resumed.attempt, reserved.attempt);
    assert_eq!(resumed.inputs, reserved.inputs);
    assert_eq!(
        completed.unwrap().outputs,
        reserved.inputs.into_values().collect::<Vec<_>>()
    );
    std::fs::remove_file(&spec.input).unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(checks.load(Ordering::SeqCst), 1);
    assert_eq!(report.methods("PUT"), 1);
    assert_eq!(report.methods("GET"), 1);
    assert_eq!(
        report.methods("HEAD"),
        4,
        "one initial absence, qualified readback, then completed-phase availability"
    );
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
    assert_eq!(report.methods("DELETE"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transferred_remote_admission_recovers_without_original_source_or_second_put() {
    let mut server = WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let (_root, engine, spec, bytes) = fixture(server.objects()).await;
    assert_protection_before_put(&server, &engine, &spec);
    assert_eq!(
        engine
            .run_until(&spec, "source-transfer")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (original, phase) = saved(&engine, &spec).await;
    assert!(phase.is_none());
    std::fs::remove_file(&spec.input).unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (recovered, phase) = saved(&engine, &spec).await;
    assert_eq!(recovered.inputs, original.inputs);
    assert_eq!(
        phase.unwrap().outputs,
        original.inputs.into_values().collect::<Vec<_>>()
    );
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(report.methods("PUT"), 1);
    assert_eq!(
        report.methods("GET"),
        2,
        "one initial verification and one unacknowledged-phase reconciliation"
    );
    assert_eq!(report.methods("HEAD"), 5);
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
    assert_eq!(report.methods("DELETE"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_remote_put_response_reconciles_exact_intent_and_completes_admission() {
    let server = WriteFixture::for_path(Behavior::LosePutResponse, "/bucket/owned/".into());
    let (_root, engine, spec, bytes) = fixture(server.objects()).await;
    assert_protection_before_put(&server, &engine, &spec);
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (intent, phase) = saved(&engine, &spec).await;
    assert_eq!(
        phase.unwrap().outputs,
        intent.inputs.into_values().collect::<Vec<_>>()
    );
    std::fs::remove_file(&spec.input).unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(report.methods("PUT"), 1);
    assert_eq!(report.methods("GET"), 1);
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
    assert_eq!(report.methods("DELETE"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saved_remote_intent_does_not_turn_auth_failure_into_absence() {
    let server = WriteFixture::for_path(Behavior::HeadDenied, "/bucket/owned/".into());
    let (_root, engine, spec, _) = fixture(server.objects()).await;
    assert_eq!(
        engine
            .run_until(&spec, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (original, _) = saved(&engine, &spec).await;
    assert_eq!(engine.run(&spec).await.unwrap_err().code(), "remote_read");
    let (retained, phase) = saved(&engine, &spec).await;
    assert_eq!(retained.inputs, original.inputs);
    assert!(phase.is_none());
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(report.methods("PUT"), 0);
    assert_eq!(report.methods("GET"), 0);
    assert_eq!(report.methods("HEAD"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_remote_intent_does_not_poison_another_operation_with_same_material_request() {
    let mut server = WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let (_root, engine, mut spec, _) = fixture(server.objects()).await;
    assert_eq!(
        engine
            .run_until(&spec, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (first, _) = saved(&engine, &spec).await;
    spec.operation = Id::new("other").unwrap();
    assert_protection_before_put(&server, &engine, &spec);
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (other, phase) = saved(&engine, &spec).await;
    assert_eq!(first.request_digest, other.request_digest);
    assert_ne!(first.operation, other.operation);
    assert_ne!(first.operation_revision, other.operation_revision);
    assert_ne!(first.prefix, other.prefix);
    assert_eq!(
        phase.unwrap().outputs,
        other.inputs.into_values().collect::<Vec<_>>()
    );
    engine.shutdown().await.unwrap();
    assert_eq!(server.finish().methods("PUT"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readable_in_scope_remote_input_has_no_ownership_without_creation_evidence() {
    let mut server = WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let objects = server.objects();
    let (_root, engine, mut spec, bytes) = fixture(objects.clone()).await;
    let outside = ObjectRef {
        owner: Id::new("unowned").unwrap(),
        path: "s3://bucket/owned/unowned/attempt/input.parquet".into(),
        digest: spec.expected_digest.clone(),
        bytes: bytes.len() as u64,
    };
    objects
        .create_bytes(&outside, bytes, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    assert_eq!(
        objects
            .verify(&outside, &engine.inner.lifecycle.budget)
            .await
            .unwrap(),
        outside
    );
    spec.input = outside.path.clone();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "unqualified_owner_reference"
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    assert!(
        !engine
            .inner
            .lifecycle
            .rows(&view)
            .await
            .unwrap()
            .contains_key(&format!("operation/{}", spec.operation))
    );
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(
        report.methods("PUT"),
        1,
        "only fixture-created unqualified bytes"
    );
    assert_eq!(
        report.methods("GET"),
        1,
        "only explicit transport readability proof"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn present_remote_destination_with_wrong_digest_never_triggers_upload() {
    let mut server = WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let objects = server.objects();
    let (_root, engine, spec, mut bytes) = fixture(objects.clone()).await;
    assert_eq!(
        engine
            .run_until(&spec, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (pending, _) = saved(&engine, &spec).await;
    let mut wrong = pending.inputs["input"].clone();
    bytes[0] ^= 1;
    wrong.digest = blake3::hash(&bytes).to_hex().to_string();
    objects
        .create_bytes(&wrong, bytes, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    std::fs::remove_file(&spec.input).unwrap();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "object_changed"
    );
    assert!(saved(&engine, &spec).await.1.is_none());
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(
        report.methods("PUT"),
        1,
        "only fixture-created mismatching bytes"
    );
    assert_eq!(report.methods("GET"), 1);
    assert_eq!(report.methods("DELETE"), 0);
}
