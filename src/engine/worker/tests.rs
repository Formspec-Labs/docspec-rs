//! Deterministic ownership, cancellation, and maintenance-admission qualification.

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, io::Write, time::Duration};
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

#[tokio::test]
async fn a_detached_native_disk_owner_prevents_a_second_full_disk_allowance() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let memory = engine.request_memory(&()).unwrap();
    let manager = engine
        .admit_disk_work(memory, DiskAdmission::Exclusive, |owner| async move {
            let budget = owner.inner.lifecycle.budget.with_retained_disk(1024)?;
            let manager = budget.runtime_env().disk_manager.clone();
            *owner.inner.retained_disk.lock().await = Arc::downgrade(&manager);
            let mut file = manager.create_tmp_file("detached native child")?;
            file.inner().as_file().set_len(4096)?;
            file.update_disk_usage()?;
            Ok((manager, file))
        })
        .await
        .unwrap();
    for admission in [DiskAdmission::Shared, DiskAdmission::Exclusive] {
        let memory = engine.request_memory(&()).unwrap();
        assert_eq!(
            engine
                .admit_disk_work(memory, admission, |_| async { Ok(()) })
                .await
                .unwrap_err()
                .code(),
            "disk_draining"
        );
    }
    let memory = engine.request_memory(&()).unwrap();
    engine
        .admit_disk_work(memory, DiskAdmission::Metadata, |_| async { Ok(()) })
        .await
        .unwrap();
    drop(manager);
    let memory = engine.request_memory(&()).unwrap();
    engine
        .admit_disk_work(memory, DiskAdmission::Exclusive, |owner| async move {
            let budget = owner.inner.lifecycle.budget.with_retained_disk(0)?;
            assert_eq!(
                budget.runtime_env().disk_manager.max_temp_directory_size(),
                owner.inner.lifecycle.budget.limits.scratch_bytes
            );
            Ok(())
        })
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cold_reopen_reclaims_private_scratch_aliases_after_process_death() {
    use std::os::unix::fs::MetadataExt;
    const CHILD_ROOT: &str = "DOCSPEC_CRASHED_SCRATCH_FIXTURE";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        let engine = Engine::open(&root, limits()).await.unwrap();
        let budget = engine.inner.lifecycle.budget.with_retained_disk(0).unwrap();
        for (name, acknowledge) in [("sealed", true), ("publication-window", false)] {
            let file = budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("crash custody probe")
                .unwrap();
            let mut writer = budget.scratch_writer(&file).unwrap();
            writer.write_all(&vec![0x5a; 1 << 20]).unwrap();
            drop(writer);
            std::fs::hard_link(file.path(), root.join(name)).unwrap();
            if acknowledge {
                budget.retain_temporary(file).unwrap();
            } else {
                std::mem::forget(file);
            }
        }
        std::fs::write(root.join("ready"), b"sealed").unwrap();
        loop {
            std::thread::park();
        }
    }
    let root = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "engine::worker::tests::cold_reopen_reclaims_private_scratch_aliases_after_process_death", "--nocapture"])
        .env(CHILD_ROOT, root.path()).spawn().unwrap();
    within(async {
        while !root.path().join("ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("custody child exited early: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        std::fs::metadata(root.path().join("sealed"))
            .unwrap()
            .nlink(),
        1
    );
    assert_eq!(
        std::fs::metadata(root.path().join("publication-window"))
            .unwrap()
            .nlink(),
        2
    );
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    for name in ["sealed", "publication-window"] {
        let path = root.path().join(name);
        assert_eq!(
            std::fs::metadata(&path).unwrap().nlink(),
            1,
            "cold scratch reclamation removes every surviving alias"
        );
        std::fs::remove_file(path).unwrap();
    }
    fn scratch_files(root: &Path) -> usize {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    scratch_files(&entry.path())
                } else {
                    1
                }
            })
            .sum()
    }
    let files = scratch_files(&root.path().join("scratch"));
    assert_eq!(
        files, 0,
        "removing the native durable path leaves no scratch disk root"
    );
    drop(engine);
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

fn one_batch_stream() -> SendableRecordBatchStream {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]),
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn stream_retains_maintenance_admission_and_host_until_eof_or_drop() {
    for consume in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let gate = engine.inner.maintenance.clone();
        let host = Arc::downgrade(&engine.inner.host);
        let memory = engine.request_memory(&()).unwrap();
        let mut stream = engine
            .admit_stream(memory, |_| async { Ok(one_batch_stream()) })
            .await
            .unwrap();
        drop(engine);
        assert!(host.upgrade().is_some());
        assert_eq!(
            WorkspaceLock::acquire(root.path()).unwrap_err().code(),
            "workspace_busy"
        );
        let mut exclusive = Box::pin(gate.write_owned());
        assert!(futures::poll!(&mut exclusive).is_pending());
        if consume {
            assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
            assert!(futures::poll!(&mut exclusive).is_pending());
            assert!(stream.next().await.is_none());
            // EOF releases custody even while the exhausted stream is retained.
            assert!(host.upgrade().is_none());
        }
        drop(stream);
        drop(within(exclusive).await);
        assert!(host.upgrade().is_none());
        drop(WorkspaceLock::acquire(root.path()).unwrap());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_stream_open_keeps_custody_until_opening_really_finishes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let gate = engine.inner.maintenance.clone();
    let host = Arc::downgrade(&engine.inner.host);
    let (started, entered) = oneshot::channel();
    let (release, parked) = oneshot::channel();
    let owner = engine.clone();
    let caller = tokio::spawn(async move {
        let memory = owner.request_memory(&()).unwrap();
        owner
            .admit_stream(memory, move |_| async move {
                started.send(()).unwrap();
                parked.await.unwrap();
                Ok(one_batch_stream())
            })
            .await
    });
    within(entered).await.unwrap();
    let mut exclusive = Box::pin(gate.write_owned());
    assert!(futures::poll!(&mut exclusive).is_pending());
    caller.abort();
    match within(caller).await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("canceled stream caller completed"),
    }
    drop(engine);
    assert!(host.upgrade().is_some());
    assert!(futures::poll!(&mut exclusive).is_pending());
    release.send(()).unwrap();
    drop(within(exclusive).await);
    assert!(host.upgrade().is_none());
    drop(WorkspaceLock::acquire(root.path()).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn engine_reads_block_maintenance_and_shutdown_refuses_new_streams() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&build(&input(root.path()), "reads"))
        .await
        .unwrap();
    let group = engine.read_group(&release, "source").await.unwrap();
    let combined = engine.read_combined(&release).await.unwrap();
    let memory = engine.request_memory(&()).unwrap();
    let mut maintenance = Box::pin(engine.maintain_work(memory, |_| async { Ok(()) }));
    assert!(futures::poll!(&mut maintenance).is_pending());
    drop(group);
    assert!(futures::poll!(&mut maintenance).is_pending());
    drop(combined);
    within(maintenance).await.unwrap();

    let mut stream = engine.read_group(&release, "source").await.unwrap();
    let mut shutdown = Box::pin(engine.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    assert_eq!(
        engine
            .read_group(&release, "source")
            .await
            .err()
            .unwrap()
            .code(),
        "host_closing"
    );
    assert_eq!(
        engine.read_evidence(&release).await.err().unwrap().code(),
        "host_closing"
    );
    assert_eq!(
        engine.read_combined(&release).await.err().unwrap().code(),
        "host_closing"
    );
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(stream.next().await.is_none());
    within(shutdown).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn engine_reads_require_this_hosts_exact_available_result() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&build(&input(root.path()), "reads"))
        .await
        .unwrap();
    let other = Engine::open(root.path().join("other"), limits())
        .await
        .unwrap();
    assert!(other.read_group(&release, "source").await.is_err());
    assert!(other.read_evidence(&release).await.is_err());
    assert!(other.read_combined(&release).await.is_err());
    assert!(
        !other
            .inner
            .lifecycle
            .catalog
            .table_exists(&crate::catalog::lifecycle_ident(&release.dataset.dataset))
            .await
            .unwrap(),
        "wrong-host reads must not create a producer lifecycle table"
    );
    let mut changed = release;
    changed.manifest.digest.push('0');
    for error in [
        engine.read_group(&changed, "source").await.err().unwrap(),
        engine.read_evidence(&changed).await.err().unwrap(),
        engine.read_combined(&changed).await.err().unwrap(),
    ] {
        assert_eq!(error.code(), "release_mismatch");
    }
    other.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn availability_retirement_waits_for_reader_and_then_refuses_new_engine_reads() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let first_request = build(&input(root.path()), "reads");
    let first = engine.run(&first_request).await.unwrap();
    let replacement = engine
        .run(&RunSpec {
            operation: Id::new("replacement").unwrap(),
            ..first_request.clone()
        })
        .await
        .unwrap();
    let mut stream = engine.read_group(&first, "source").await.unwrap();
    let plan = engine
        .plan_availability_retirement(
            &first_request.dataset,
            &Id::new("retire_initial").unwrap(),
            &first_request.operation,
            "end standalone engine access",
        )
        .await
        .unwrap();
    let mut retirement = Box::pin(engine.retire_availability(&plan));
    assert!(futures::poll!(&mut retirement).is_pending());
    let batch = stream.next().await.unwrap().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[1]);
    assert!(futures::poll!(&mut retirement).is_pending());
    assert!(stream.next().await.is_none());
    within(retirement).await.unwrap();

    for error in [
        engine.read_group(&first, "source").await.err().unwrap(),
        engine.read_evidence(&first).await.err().unwrap(),
        engine.read_combined(&first).await.err().unwrap(),
        engine
            .compare(&first, &replacement, "source")
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), "availability_retired");
    }
    // This caller explicitly retains the files. Catalog retirement does not
    // delete them or turn a serialized identity into an implicit lease.
    let mut standalone = read_group(&first, "source", &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    let batch = standalone.next().await.unwrap().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[1]);
    assert!(standalone.next().await.is_none());
    assert!(Path::new(&first.manifest.path).exists());
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_maintenance_keeps_exclusion_until_its_native_decision_finishes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let shared = engine.inner.maintenance.clone().read_owned().await;
    let (started, entered) = oneshot::channel();
    let (release, parked) = oneshot::channel();
    let owner = engine.clone();
    let mut maintenance = Box::pin(async move {
        let memory = owner.request_memory(&()).unwrap();
        owner
            .maintain_work(memory, move |worker| async move {
                started.send(()).unwrap();
                parked.await.unwrap();
                let lifecycle = &worker.inner.lifecycle;
                let view = lifecycle.open(&Id::new("maintenance").unwrap()).await?;
                lifecycle
                    .commit(&view, vec![record("finished", true)?])
                    .await?;
                Ok(())
            })
            .await
    });
    assert!(futures::poll!(&mut maintenance).is_pending());
    let caller = tokio::spawn(maintenance);
    let dataset = Id::new("maintenance").unwrap();
    let mut reader = Box::pin(engine.inspect(&dataset));
    assert!(
        futures::poll!(&mut reader).is_pending(),
        "queued writer fences later readers"
    );
    drop(shared);
    within(entered).await.unwrap();
    caller.abort();
    assert!(within(caller).await.unwrap_err().is_cancelled());
    assert!(
        futures::poll!(&mut reader).is_pending(),
        "caller cancellation must not release exclusive custody"
    );
    release.send(()).unwrap();
    assert_eq!(within(reader).await.unwrap()["finished"], true);
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
        partition_by: Vec::new(),
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
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };
    let joined = with_queued_writer(&engine, &refresh.dataset, engine.refresh(&refresh)).await;
    assert_eq!(joined.dataset.groups["source"].rows, 1);
    let mut evidence = engine.read_evidence(&joined).await.unwrap();
    assert!(engine.inner.maintenance.try_write().is_err());
    assert!(evidence.next().await.unwrap().unwrap().num_rows() > 0);
    drop(evidence);
    assert!(engine.inner.maintenance.try_write().is_ok());
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
