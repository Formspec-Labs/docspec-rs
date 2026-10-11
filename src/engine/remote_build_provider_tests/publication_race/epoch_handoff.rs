//! Opt-in publication authority handoff against an existing synthetic provider dataset.
// Rust guideline compliant 2026-02-21
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    process::{Child, Command, Stdio},
    time::Instant,
};

const DATASET: &str = "docspec-rs-qualification-2ab6f450-a0ae-4b5e-a70d-8757375d5c79";
const CHILD: &str =
    "engine::remote_build_provider_tests::publication_race::epoch_handoff::handoff_child";
const DEADLINE: Duration = Duration::from_secs(120);

fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path)
        .map_err(|error| Error::from(error.error))?;
    Ok(())
}

async fn wait(path: &Path) -> Result<()> {
    let started = Instant::now();
    while !path.is_file() {
        if started.elapsed() >= DEADLINE {
            return Err(Error::refusal(
                "provider_evidence",
                "epoch handoff rendezvous deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

pub(super) async fn hold(directory: &Path, metadata: &str) -> iceberg::Result<()> {
    async {
        save(
            &directory.join("a-held.json"),
            &json!({"pid":std::process::id(),"metadata":metadata}),
        )?;
        wait(&directory.join("a-release.json")).await
    }
    .await
    .map_err(|_: Error| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "epoch handoff rendezvous refused",
        )
        .with_retryable(false)
    })
}

pub(super) async fn hold_selected(directory: &Path, table: &Table) -> iceberg::Result<()> {
    let result: Result<()> = async {
        save(
            &directory.join("b-selected.json"),
            &json!({"pid":std::process::id(),
            "metadata":table.metadata_location_result()?}),
        )?;
        // Actual accepted native response is withheld until the parent kills us.
        tokio::time::sleep(DEADLINE).await;
        Err(Error::refusal(
            "provider_evidence",
            "selected worker was not killed",
        ))
    }
    .await;
    result.map_err(|_| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "selected worker acknowledgment deadline",
        )
        .with_retryable(false)
    })
}

pub(super) fn native_outcome(
    directory: &Path,
    outcome: &iceberg::Result<Table>,
) -> iceberg::Result<()> {
    // Only the maintained provider response kind is recorded; never its body/config.
    let conflict = outcome
        .as_ref()
        .err()
        .is_some_and(|error| error.kind() == iceberg::ErrorKind::CatalogCommitConflicts);
    save(
        &directory.join("a-native.json"),
        &json!({"accepted":outcome.is_ok(),"native_conflict":conflict}),
    )
    .map_err(|_| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "native handoff receipt could not be retained",
        )
        .with_retryable(false)
    })
}

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn spawn(directory: &Path, mode: &str, spec: &ExportSpec) -> Result<Worker> {
    save(&directory.join(format!("{mode}-request.json")), spec)?;
    let log = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(format!("{mode}.log")))?;
    Ok(Worker(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                CHILD,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("DOCSPEC_EPOCH_CHILD", directory)
            .env("DOCSPEC_EPOCH_MODE", mode)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    ))
}

async fn finish(worker: &mut Worker) -> Result<()> {
    let started = Instant::now();
    loop {
        if let Some(status) = worker.0.try_wait()? {
            if !status.success() {
                return Err(Error::refusal(
                    "provider_evidence",
                    "epoch child failed; retain its log",
                ));
            }
            return Ok(());
        }
        if started.elapsed() >= DEADLINE {
            return Err(Error::refusal(
                "provider_evidence",
                "epoch child exit deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn scope() -> Result<(Id, String, Id, Id, Id)> {
    if env("DOCSPEC_QUALIFICATION_ID")? != DATASET {
        return Err(Error::refusal(
            "provider_scope",
            "this probe requires the exact retained publication dataset",
        ));
    }
    let attempt = env("DOCSPEC_EPOCH_HANDOFF_ID")?;
    if uuid::Uuid::parse_str(&attempt)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(attempt.as_str())
    {
        return Err(Error::refusal(
            "provider_scope",
            "fresh canonical handoff UUID required",
        ));
    }
    let prefix = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{DATASET}/publication-race",
        env("DOCSPEC_S3_BUCKET")?
    );
    Ok((
        Id::new(DATASET)?,
        prefix,
        Id::new(format!("handoff-{attempt}"))?,
        Id::new(format!("handoff-a-{attempt}"))?,
        Id::new(format!("handoff-b-{attempt}"))?,
    ))
}

async fn open(host: &Path, prefix: &str) -> Result<Engine> {
    Engine::with_remote(host, connect(prefix).await?, limits()).await
}

#[derive(Serialize, Deserialize)]
struct ChildReport {
    pid: u32,
    initial_code: String,
    retry_code: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the exact retained provider dataset and a fresh handoff UUID"]
async fn independent_process_epoch_handoff_preserves_export_work_and_recovery() {
    if let Err(error) = qualify().await {
        eprintln!(
            "{}",
            json!({"phase":"epoch_handoff_failed","code":error.code()})
        );
        panic!("epoch handoff refused; retain its exact receipts and remote objects");
    }
}

async fn qualify() -> Result<()> {
    let (dataset, prefix, channel, a_id, b_id) = scope()?;
    let directory = tempfile::tempdir()?.keep();
    println!(
        "{}",
        json!({"phase":"epoch_handoff_start","dataset":dataset,"prefix":prefix,
        "channel":channel,"a":a_id,"b":b_id,"control_directory":directory,"parent_pid":std::process::id()})
    );
    let controller = open(&directory.join("controller"), &prefix).await?;
    let source = controller.latest(&dataset).await?;
    let public = controller
        .latest_export(&dataset, &Id::new("public")?)
        .await?;
    assert_eq!(public.source, source.version_ref());
    assert_eq!(public.rows, 4);
    assert_eq!(source.dataset.request.operation().as_str(), "updated");
    let initial = controller.inspect(&dataset).await?;
    let authority: Authority = serde_json::from_value(
        initial
            .get("authority")
            .cloned()
            .ok_or_else(|| Error::refusal("provider_scope", "retained authority is missing"))?,
    )?;
    if authority.epoch != 0
        || initial.contains_key(&format!("export-channel/{channel}"))
        || initial
            .keys()
            .any(|key| key.contains(a_id.as_str()) || key.contains(b_id.as_str()))
    {
        return Err(Error::refusal(
            "provider_scope",
            "handoff epoch/channel/operations are not fresh",
        ));
    }
    let a = ExportSpec {
        operation: a_id,
        channel: channel.clone(),
        ..request(&source, "unused", None)?
    };
    let b = ExportSpec {
        operation: b_id,
        ..a.clone()
    };
    assert_eq!(
        controller
            .export_until(&a, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let a_completed = completion(&controller, &a).await?;
    let before_fence = controller.inspect(&dataset).await?;
    let mut worker_a = spawn(&directory, "a", &a)?;
    wait(&directory.join("a-held.json")).await?;
    let held: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("a-held.json"))?)?;
    assert_eq!(held["pid"], worker_a.0.id());
    assert_ne!(worker_a.0.id(), std::process::id());
    let head = controller.existing_lifecycle_view(&dataset).await?;
    assert_eq!(held["metadata"], head.table.metadata_location_result()?);
    let old_revision = head.revision;
    assert_eq!(controller.fence_operations(&dataset, 0).await?, 1);
    let fenced = controller.inspect(&dataset).await?;
    for (key, value) in &before_fence {
        if key != "authority" {
            assert_eq!(fenced.get(key), Some(value));
        }
    }
    let mut expected_authority: Authority =
        serde_json::from_value(before_fence["authority"].clone())?;
    expected_authority.epoch = 1;
    assert_eq!(
        fenced["authority"],
        serde_json::to_value(expected_authority)?
    );
    let new_head = controller.existing_lifecycle_view(&dataset).await?;
    assert!(new_head.revision > old_revision);
    assert_ne!(
        new_head.table.metadata_location_result()?,
        head.table.metadata_location_result()?
    );
    assert_eq!(
        controller
            .latest_export(&dataset, &Id::new("public")?)
            .await?,
        public
    );
    assert!(!fenced.contains_key(&format!("export-channel/{channel}")));
    save(&directory.join("a-release.json"), &json!({"epoch":1}))?;
    finish(&mut worker_a).await?;
    let native: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("a-native.json"))?)?;
    assert_eq!(native, json!({"accepted":false,"native_conflict":true}));
    let child: ChildReport =
        serde_json::from_slice(&std::fs::read(directory.join("a-result.json"))?)?;
    assert_eq!(child.pid, worker_a.0.id());
    assert_eq!(child.retry_code, "stale_export");
    assert_eq!(completion(&controller, &a).await?, a_completed);
    assert_eq!(controller.inspect(&dataset).await?, fenced);
    assert_eq!(
        controller
            .existing_lifecycle_view(&dataset)
            .await?
            .table
            .metadata_location_result()?,
        new_head.table.metadata_location_result()?
    );
    // B is admitted after the fence. Its own tiny output is prepared once;
    // cross-operation export materialization has no supported alias shortcut.
    assert_eq!(
        controller
            .export_until(&b, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let b_completed = completion(&controller, &b).await?;
    let mut worker_b = spawn(&directory, "b", &b)?;
    wait(&directory.join("b-selected.json")).await?;
    let selected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("b-selected.json"))?)?;
    assert_eq!(selected["pid"], worker_b.0.id());
    assert_ne!(worker_a.0.id(), worker_b.0.id());
    let winner = controller.export_pin(&dataset, &b.operation).await?;
    assert_eq!(
        selected["metadata"],
        controller
            .existing_lifecycle_view(&dataset)
            .await?
            .table
            .metadata_location_result()?
    );
    assert_eq!(winner.object, b_completed.materialized.object);
    assert_eq!(winner.validation, b_completed.validation);
    // Kill before the child returns its service acknowledgment to its caller.
    worker_b.0.kill()?;
    let status = worker_b.0.wait()?;
    assert!(!status.success());
    let before_retry = controller.inspect(&dataset).await?;
    let retry_head = controller.existing_lifecycle_view(&dataset).await?;
    let cold = open(&directory.join("cold"), &prefix).await?;
    assert_eq!(cold.export(&b).await?, winner);
    assert_eq!(completion(&cold, &a).await?, a_completed);
    assert_eq!(completion(&cold, &b).await?, b_completed);
    assert_eq!(cold.inspect(&dataset).await?, before_retry);
    assert_eq!(cold.fence_operations(&dataset, 0).await?, 1);
    let after_retry = cold.existing_lifecycle_view(&dataset).await?;
    assert_eq!(after_retry.revision, retry_head.revision);
    assert_eq!(
        after_retry.table.metadata_location_result()?,
        retry_head.table.metadata_location_result()?
    );
    assert_eq!(cold.latest_export(&dataset, &channel).await?, winner);
    assert_eq!(
        cold.latest_export(&dataset, &Id::new("public")?).await?,
        public
    );
    assert_eq!(cold.latest(&dataset).await?, source);
    read_values(&cold, &winner, true).await?;
    println!(
        "{}",
        json!({"phase":"epoch_handoff_passed","held_metadata":held["metadata"],
        "a_pid":child.pid,"b_pid":worker_b.0.id(),"a_initial_code":child.initial_code,
        "a_retry_code":child.retry_code,"native_conflict":native["native_conflict"],
        "winner":winner,"source":source.version_ref(),"epoch":1,"remote_delete":false,
        "source_rebuilt":false,"request_counts_measured":false})
    );
    cold.shutdown().await?;
    controller.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by the retained provider epoch-handoff parent"]
async fn handoff_child() {
    if let Err(error) = child().await {
        eprintln!(
            "{}",
            json!({"phase":"epoch_child_failed","code":error.code()})
        );
        panic!("epoch child failed; retain its control log");
    }
}

async fn child() -> Result<()> {
    let (dataset, prefix, _, a_id, b_id) = scope()?;
    let directory = std::path::PathBuf::from(env("DOCSPEC_EPOCH_CHILD")?);
    if !directory.is_absolute() {
        return Err(Error::refusal(
            "provider_scope",
            "absolute control directory required",
        ));
    }
    let mode = env("DOCSPEC_EPOCH_MODE")?;
    if !matches!(mode.as_str(), "a" | "b") {
        return Err(Error::refusal("provider_scope", "unknown epoch child mode"));
    }
    let spec: ExportSpec = serde_json::from_slice(&std::fs::read(
        directory.join(format!("{mode}-request.json")),
    )?)?;
    assert_eq!(spec.dataset, dataset);
    assert_eq!(spec.operation, if mode == "a" { a_id } else { b_id });
    let mut remote = connect(&prefix).await?;
    let catalog = Arc::new(RaceCatalog {
        inner: remote.catalog,
        lifecycle: crate::catalog::lifecycle_ident(&dataset),
        armed: AtomicBool::new(false),
        loaded: StdMutex::new(None),
        rendezvous: Arc::new(Rendezvous {
            barrier: tokio::sync::Barrier::new(2),
            metadata: StdMutex::new(Vec::new()),
            files: Some((
                directory.clone(),
                if mode == "a" { "epoch-a" } else { "epoch-b" }.into(),
            )),
        }),
    });
    remote.catalog = catalog.clone();
    let engine =
        Engine::with_remote(directory.join(format!("child-{mode}")), remote, limits()).await?;
    if mode == "a" {
        let before = completion(&engine, &spec).await?;
        catalog.arm();
        let initial = engine
            .export(&spec)
            .await
            .expect_err("fenced original native commit must refuse");
        let retry = engine
            .export(&spec)
            .await
            .expect_err("old export must remain stale");
        assert_eq!(retry.code(), "stale_export");
        assert_eq!(completion(&engine, &spec).await?, before);
        save(
            &directory.join("a-result.json"),
            &ChildReport {
                pid: std::process::id(),
                initial_code: initial.code().into(),
                retry_code: retry.code().into(),
            },
        )?;
        engine.shutdown().await?;
    } else {
        catalog.arm();
        engine.export(&spec).await?;
        return Err(Error::refusal(
            "provider_evidence",
            "selected acknowledgment unexpectedly returned",
        ));
    }
    Ok(())
}
