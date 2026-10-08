//! Deterministic ownership, cancellation, and maintenance-admission qualification.

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, time::Duration};
use tokio::sync::oneshot;

// Correctness fixtures fail at a lost handoff instead of hanging the test runner.
// This deadline is not a performance acceptance threshold.
const HANDOFF_DEADLINE: Duration = Duration::from_secs(30);

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
        ..Limits::default()
    }
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(HANDOFF_DEADLINE, future)
        .await
        .expect("worker handoff exceeded fixture deadline")
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_caller_keeps_blocking_work_gate_and_host_until_real_exit() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let gate = engine.inner.maintenance.clone();
    let host = Arc::downgrade(&engine.inner.host);
    let (started, entered) = oneshot::channel();
    let (release, parked) = std::sync::mpsc::channel();
    let owner = engine.clone();
    let caller = tokio::spawn(async move {
        let memory = owner.request_memory(&()).unwrap();
        owner
            .admit_work(memory, move |worker| async move {
                worker
                    .inner
                    .lifecycle
                    .budget
                    .blocking(move || {
                        started.send(()).unwrap();
                        parked.recv().unwrap();
                        Ok(())
                    })
                    .await
            })
            .await
    });
    within(entered).await.unwrap();
    caller.abort();
    assert!(within(caller).await.unwrap_err().is_cancelled());
    drop(engine);

    assert!(host.upgrade().is_some());
    assert_eq!(
        Engine::open(&workspace, limits()).await.unwrap_err().code(),
        "workspace_busy"
    );
    let mut exclusive = Box::pin(gate.write_owned());
    assert!(futures::poll!(&mut exclusive).is_pending());

    release.send(()).unwrap();
    let exclusive = within(exclusive).await;
    assert!(
        host.upgrade().is_none(),
        "only the private worker owned the host"
    );
    drop(exclusive);
    let reopened = Engine::open(&workspace, limits()).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_closes_admission_and_resumes_drain_after_its_wait_is_dropped() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let (started, entered) = oneshot::channel();
    let (release, parked) = oneshot::channel();
    let owner = engine.clone();
    let worker = tokio::spawn(async move {
        let memory = owner.request_memory(&()).unwrap();
        owner
            .admit_work(memory, move |_| async move {
                started.send(()).unwrap();
                parked.await.unwrap();
                Ok(())
            })
            .await
    });
    within(entered).await.unwrap();
    let mut shutdown = Box::pin(engine.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    assert_eq!(
        engine
            .inspect(&Id::new("closed").unwrap())
            .await
            .unwrap_err()
            .code(),
        "host_closing"
    );
    drop(shutdown);
    assert!(engine.inner.maintenance.try_write().is_err());
    assert_eq!(
        engine
            .latest(&Id::new("closed").unwrap())
            .await
            .unwrap_err()
            .code(),
        "host_closing"
    );
    let mut shutdown = Box::pin(engine.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    within(worker).await.unwrap().unwrap();
    within(shutdown).await.unwrap();
    assert_eq!(
        WorkspaceLock::acquire(root.path()).unwrap_err().code(),
        "workspace_busy"
    );
    drop(engine);
    let reopened = Engine::open(root.path(), limits()).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_shared_work_can_enter_before_either_operation_finishes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let mut workers = Vec::new();
    let mut releases = Vec::new();
    for value in [1, 2] {
        let owner = engine.clone();
        let (started, entered) = oneshot::channel();
        let (release, parked) = oneshot::channel();
        workers.push(tokio::spawn(async move {
            let memory = owner.request_memory(&value).unwrap();
            owner
                .admit_work(memory, move |_| async move {
                    started.send(()).unwrap();
                    parked.await.unwrap();
                    Ok(value)
                })
                .await
        }));
        within(entered).await.unwrap();
        releases.push(release);
    }
    assert!(engine.inner.maintenance.try_write().is_err());
    for release in releases {
        release.send(()).unwrap();
    }
    for (worker, expected) in workers.into_iter().zip([1, 2]) {
        assert_eq!(within(worker).await.unwrap().unwrap(), expected);
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_callers_cannot_accumulate_unbounded_owned_requests() {
    let root = tempfile::tempdir().unwrap();
    let small = Limits {
        memory_bytes: 128 << 10,
        max_metadata_bytes: 8 << 10,
        max_batch_bytes: 8 << 10,
        ..limits()
    };
    let engine = Engine::open(root.path(), small.clone()).await.unwrap();
    let payload = "x".repeat(1024);
    let mut releases = Vec::new();
    let mut refused = false;
    // The shared pool is 64 KiB. A 1 KiB owned request is charged at least 8 KiB,
    // plus the task state, so this bound must encounter capacity before its end.
    for _ in 0..16 {
        let memory = match engine.request_memory(&payload) {
            Ok(memory) => memory,
            Err(error) => {
                assert_eq!(error.code(), "capacity");
                refused = true;
                break;
            }
        };
        let owned_payload = payload.clone();
        let owner = engine.clone();
        let (started, entered) = oneshot::channel();
        let (release, parked) = oneshot::channel();
        let caller = tokio::spawn(async move {
            owner
                .admit_work(memory, move |_| async move {
                    started.send(()).unwrap();
                    parked.await.unwrap();
                    Ok(owned_payload.len())
                })
                .await
        });
        if within(entered).await.is_err() {
            assert_eq!(
                within(caller).await.unwrap().unwrap_err().code(),
                "capacity"
            );
            refused = true;
            break;
        }
        caller.abort();
        assert!(within(caller).await.unwrap_err().is_cancelled());
        releases.push(release);
    }
    assert!(releases.len() >= 2, "the pool admits concurrent small work");
    assert!(
        refused,
        "detached workers must retain their request/task charges"
    );
    assert!(
        engine
            .inner
            .lifecycle
            .budget
            .reserve(small.memory_bytes / 2)
            .is_err()
    );
    for release in releases {
        release.send(()).unwrap();
    }
    let exclusive = within(engine.inner.maintenance.clone().write_owned()).await;
    drop(exclusive);
    let all = engine
        .inner
        .lifecycle
        .budget
        .reserve(small.memory_bytes / 2)
        .unwrap();
    drop(all);
    let memory = engine.request_memory(&payload).unwrap();
    engine
        .admit_work(memory, |_| async { Ok(()) })
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
}

fn input(root: &Path) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let path = root.join("input.parquet");
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(&path).unwrap().0,
    }
}

fn build(input: &InputSpec, dataset: &str) -> RunSpec {
    RunSpec {
        dataset: Id::new(dataset).unwrap(),
        operation: Id::new("initial").unwrap(),
        base: None,
        relationship: None,
        input: input.path.clone(),
        expected_digest: input.digest.clone(),
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

async fn with_queued_writer<T>(
    engine: &Engine,
    dataset: &Id,
    operation: impl Future<Output = Result<T>>,
) -> T {
    let lock = engine.lock(dataset).await;
    let held = lock.lock().await;
    let mut operation = Box::pin(operation);
    // The public call has spawned its admitted worker, which cannot pass the
    // held dataset lock. No scheduling delay or catalog timing is assumed.
    assert!(futures::poll!(&mut operation).is_pending());
    assert!(engine.inner.maintenance.try_write().is_err());
    let mut exclusive = Box::pin(engine.inner.maintenance.clone().write_owned());
    assert!(futures::poll!(&mut exclusive).is_pending());
    drop(held);
    let result = within(operation).await.unwrap();
    drop(within(exclusive).await);
    result
}

#[tokio::test(flavor = "current_thread")]
async fn queued_writer_does_not_deadlock_public_build_or_refresh_internal_reads() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let input = input(root.path());
    let mut left_request = build(&input, "left");
    let left = engine.run(&left_request).await.unwrap();
    let right = engine.run(&build(&input, "right")).await.unwrap();
    left_request.operation = Id::new("rebuilt").unwrap();
    left_request.base = Some(left.id);
    left_request.sql = "SELECT id + CAST(0 AS BIGINT) AS id FROM input".into();
    let left = with_queued_writer(&engine, &left_request.dataset, engine.run(&left_request)).await;
    assert_eq!(left.dataset.groups["source"].rows, 1);
    let refresh = RefreshSpec {
        dataset: Id::new("joined").unwrap(),
        operation: Id::new("initial").unwrap(),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
                fields: vec![JoinField {
                    side: JoinSide::Left,
                    field: "id".into(),
                    name: "joined_id".into(),
                }],
            },
        }),
    };
    let joined = with_queued_writer(&engine, &refresh.dataset, engine.refresh(&refresh)).await;
    assert_eq!(joined.dataset.groups["source"].rows, 1);
    assert_eq!(
        joined.dataset.related.unwrap().coverage.inputs,
        refresh.inputs()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn private_worker_panic_quarantines_host_and_prevents_reopen() {
    async fn panic_inside_worker(_: Engine) -> Result<()> {
        panic!("intentional private worker panic for host-custody qualification")
    }
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let memory = engine.request_memory(&()).unwrap();
    assert_eq!(
        engine
            .admit_work(memory, panic_inside_worker)
            .await
            .unwrap_err()
            .code(),
        "worker"
    );
    assert_eq!(
        engine
            .inspect(&Id::new("poisoned").unwrap())
            .await
            .unwrap_err()
            .code(),
        "host_uncertain"
    );
    assert_eq!(
        within(engine.shutdown()).await.unwrap_err().code(),
        "host_uncertain"
    );
    drop(engine);
    assert_eq!(
        Engine::open(root.path(), limits())
            .await
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
}
