//! Local restore preserves exact values and excludes live old workspace actors.
// Rust guideline compliant 2026-02-21
#![cfg(unix)]

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "shared scalar fixtures also support unrelated operations"
)]
mod scalar;

use arrow::array::{Int64Array, StringArray};
use docspec::{
    Engine,
    budget::Limits,
    model::{ReleasePin, RunSpec},
    restore::{RestorePoint, RestorePointRef},
    storage,
};
use futures::TryStreamExt;
use scalar::{Row, id, input};
use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

fn capture_files(
    directory: &Path,
) -> std::collections::BTreeMap<String, (u64, std::time::SystemTime)> {
    if !directory.exists() {
        return std::collections::BTreeMap::new();
    }
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                (metadata.len(), metadata.modified().unwrap()),
            )
        })
        .collect()
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn failed_capture_retries_reuse_one_reservation_and_bounded_partial_files() {
    for boundary in [
        "restore-reservation",
        "restore-reuse",
        "restore-catalog",
        "restore-objects",
    ] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        engine
            .run(&spec(root.path(), "original", &[(Some(1), None)]))
            .await
            .unwrap();
        let mut previous = None;
        let mut attempt = None;
        let mut count = None;
        for _ in 0..3 {
            assert_eq!(
                engine
                    .create_restore_point_until(&id("comments"), &id("point"), boundary)
                    .await
                    .unwrap_err()
                    .code(),
                "injected_failure"
            );
            let records = engine.inspect(&id("comments")).await.unwrap();
            let protections = records
                .iter()
                .filter_map(|(key, value)| {
                    if !key.starts_with("protection/") {
                        return None;
                    }
                    let protection: docspec::model::Protection =
                        serde_json::from_value(value.clone()).unwrap();
                    (protection.purpose == "point/restore").then_some(protection)
                })
                .collect::<Vec<_>>();
            assert_eq!(protections.len(), 1, "{boundary}");
            if let Some(expected) = &attempt {
                assert_eq!(&protections[0].id, expected);
            }
            attempt = Some(protections[0].id.clone());
            let directory = workspace
                .join("objects/comments")
                .join(protections[0].id.as_str())
                .join("restore");
            let files = capture_files(&directory);
            if let Some(expected) = count {
                assert_eq!(files.len(), expected, "{boundary}");
            }
            count = Some(files.len());
            if let Some(expected) = previous {
                assert_eq!(
                    records, expected,
                    "retry changed durable records at {boundary}"
                );
            }
            previous = Some(records);
        }
        let reference = engine
            .create_restore_point(&id("comments"), &id("point"))
            .await
            .unwrap();
        assert_eq!(
            Path::new(&reference.manifest.path)
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .file_name()
                .unwrap(),
            attempt.unwrap().as_str()
        );
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn completed_capture_reconciles_manifest_and_commit_response_without_recopying() {
    for boundary in ["restore-manifest", "restore-committed"] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        engine
            .run(&spec(root.path(), "original", &[(Some(1), None)]))
            .await
            .unwrap();
        assert_eq!(
            engine
                .create_restore_point_until(&id("comments"), &id("point"), boundary)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let records = engine.inspect(&id("comments")).await.unwrap();
        let protection: docspec::model::Protection = records
            .iter()
            .filter(|(key, _)| key.starts_with("protection/"))
            .map(|(_, value)| {
                serde_json::from_value::<docspec::model::Protection>(value.clone()).unwrap()
            })
            .find(|protection| protection.purpose == "point/restore")
            .unwrap();
        let directory = workspace
            .join("objects/comments")
            .join(protection.id.as_str())
            .join("restore");
        let before = capture_files(&directory);
        assert!(before.contains_key("point.json"));
        let reference = engine
            .create_restore_point(&id("comments"), &id("point"))
            .await
            .unwrap();
        assert_eq!(
            capture_files(&directory),
            before,
            "{boundary} replay changed immutable files"
        );
        assert_eq!(
            engine
                .create_restore_point(&id("comments"), &id("point"))
                .await
                .unwrap(),
            reference
        );
        assert_eq!(capture_files(&directory), before);
    }
}

#[tokio::test]
async fn repeated_points_reuse_source_backups_and_do_not_clone_backup_trees() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let original = engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let source = original.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let mut first_backup = None;
    let mut sizes = Vec::new();
    for index in 0..6 {
        // A later checkpoint owner reuses the first owner's exact backup objects.
        let owner = if index == 0 {
            id("comments")
        } else {
            id("checkpoints")
        };
        let reference = engine
            .create_restore_point(&owner, &id(&format!("point{index}")))
            .await
            .unwrap();
        let saved = point(&reference);
        let backup = saved
            .objects
            .iter()
            .find(|object| object.original.path == source.path)
            .unwrap()
            .backup
            .clone();
        if let Some(expected) = &first_backup {
            assert_eq!(&backup, expected);
            assert!(saved.reused_bytes > 0);
        } else {
            first_backup = Some(backup);
        }
        let records = engine.inspect(&owner).await.unwrap();
        let created: docspec::restore::RestoreMaterialized =
            serde_json::from_value(records[&format!("restore-materialized/point{index}")].clone())
                .unwrap();
        assert!(created.backups.iter().all(|object| object.owner == owner
            && Path::new(&object.path).parent() == Path::new(&reference.manifest.path).parent()));
        sizes.push(saved.objects.len());
    }
    // This is a file-count growth assertion for the fixed fixture, not a timing
    // claim. Recursive copying would approximately double each point's closure.
    assert!(
        sizes[5] <= sizes[0] * 11,
        "checkpoint closure grew recursively: {sizes:?}"
    );
    let retirement = engine
        .plan_restore_point_retirement(
            &id("comments"),
            &id("retire-first"),
            &[id("point0")],
            "later points retain shared backup bytes",
        )
        .await
        .unwrap();
    engine.retire_restore_points(&retirement).await.unwrap();
    let first_backup = first_backup.unwrap();
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&first_backup))
        .await
        .unwrap();
    assert!(inspection.objects[0].blocker.is_some());
    storage::verify_object(&first_backup).unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn interrupted_shared_capture_protects_reuse_after_older_point_retires() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let first = engine
        .create_restore_point(&id("comments"), &id("first"))
        .await
        .unwrap();
    let shared = point(&first).objects[0].backup.clone();
    assert_eq!(
        engine
            .create_restore_point_until(&id("checkpoints"), &id("second"), "restore-manifest")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let plan = engine
        .plan_restore_point_retirement(
            &id("comments"),
            &id("retire-first"),
            &[id("first")],
            "temporary capture root preserves reused bytes",
        )
        .await
        .unwrap();
    engine.retire_restore_points(&plan).await.unwrap();
    let protected = engine
        .inspect_retention(std::slice::from_ref(&shared))
        .await
        .unwrap();
    assert!(protected.objects[0].blocker.is_some());
    let second = engine
        .create_restore_point(&id("checkpoints"), &id("second"))
        .await
        .unwrap();
    assert!(
        point(&second)
            .objects
            .iter()
            .any(|object| object.backup == shared)
    );
    let records = engine.inspect(&id("checkpoints")).await.unwrap();
    let reuse: docspec::restore::RestoreReuse =
        serde_json::from_value(records["restore-reuse/second"].clone()).unwrap();
    assert!(reuse.objects.is_empty());
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn explicitly_retired_shared_preparation_refuses_same_point_retry() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    engine
        .create_restore_point(&id("comments"), &id("first"))
        .await
        .unwrap();
    assert_eq!(
        engine
            .create_restore_point_until(&id("checkpoints"), &id("second"), "restore-manifest")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&id("checkpoints")).await.unwrap();
    let reuse: docspec::restore::RestoreReuse =
        serde_json::from_value(before["restore-reuse/second"].clone()).unwrap();
    assert!(!reuse.objects.is_empty());
    let plan = engine
        .plan_phase_retirement(
            &id("checkpoints"),
            &id("abandon-second"),
            &["restore-reuse/second".into()],
            "explicitly end shared preparation recovery",
        )
        .await
        .unwrap();
    let receipt = engine.retire_phases(&plan).await.unwrap();
    assert_eq!(engine.retire_phases(&plan).await.unwrap(), receipt);
    let retired = engine.inspect(&id("checkpoints")).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            engine
                .create_restore_point(&id("checkpoints"), &id("second"))
                .await
                .unwrap_err()
                .code(),
            "restore_attempt_retired"
        );
        assert_eq!(engine.inspect(&id("checkpoints")).await.unwrap(), retired);
    }
    assert!(!retired.contains_key("restore-point/second"));
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
        batch_rows: 8,
    }
}

fn spec(root: &Path, operation: &str, rows: &[Row]) -> RunSpec {
    let source = input(&root.join(format!("{operation}.parquet")), rows);
    RunSpec {
        dataset: id("comments"),
        operation: id(operation),
        base: None,
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

fn point(reference: &RestorePointRef) -> RestorePoint {
    storage::verify_object(&reference.manifest).unwrap();
    serde_json::from_reader(std::fs::File::open(&reference.manifest.path).unwrap()).unwrap()
}

async fn rows(engine: &Engine, release: &ReleasePin) -> Vec<Row> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        result.extend(
            ids.iter()
                .zip(text.iter())
                .map(|(key, text)| (key, text.map(str::to_owned))),
        );
    }
    result
}

#[tokio::test]
async fn restores_exact_catalog_and_missing_originals_from_retained_copies() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let expected = vec![
        (Some(1), Some("same".into())),
        (Some(1), Some("same".into())),
        (None, None),
        (Some(2), Some("é🎉".into())),
    ];
    let original = engine
        .run(&spec(root.path(), "original", &expected))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("before-correction"))
        .await
        .unwrap();
    assert_eq!(
        engine
            .create_restore_point(&id("comments"), &id("before-correction"))
            .await
            .unwrap(),
        reference
    );
    let captured = point(&reference);
    assert!(!captured.objects.is_empty());
    let backup_objects = captured
        .objects
        .iter()
        .map(|object| object.backup.clone())
        .collect::<Vec<_>>();
    let inspection = engine.inspect_retention(&backup_objects).await.unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let newer = engine
        .run(&spec(root.path(), "correction", &[(Some(9), None)]))
        .await
        .unwrap();
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), newer);
    let missing = captured
        .objects
        .iter()
        .find(|object| {
            original.dataset.groups["source"]
                .objects
                .iter()
                .any(|value| value == &object.original)
                && object.original.path.ends_with(".parquet")
        })
        .unwrap();
    let missing_path = missing.original.path.clone();
    let expected_bytes = missing.original.bytes;
    drop(engine);
    // This fixture represents a later authorized unlink. The point's independent
    // backup path must survive even when a same-inode original name disappears.
    std::fs::remove_file(&missing_path).unwrap();
    storage::verify_object(&missing.backup).unwrap();
    let restored = Engine::restore_local(&workspace, &reference, limits(), false)
        .await
        .unwrap();
    assert_eq!(restored.restored_bytes, expected_bytes);
    assert!(Path::new(&missing_path).exists());
    assert!(workspace.join("catalog.sqlite").exists());
    assert!(Path::new(&restored.path).exists());
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), original);
    assert_eq!(rows(&engine, &original).await, expected);
    assert_eq!(
        engine
            .create_restore_point(&id("comments"), &id("before-correction"))
            .await
            .unwrap(),
        reference
    );
    assert!(
        !engine
            .inspect(&id("comments"))
            .await
            .unwrap()
            .contains_key("result/correction")
    );
    // The discarded future is retained physically; restoring a point never sweeps.
    storage::verify_object(&newer.manifest).unwrap();
}

#[tokio::test]
async fn corruption_and_foreign_workspace_refuse_before_catalog_selection() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let original = engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("protected"))
        .await
        .unwrap();
    let captured = point(&reference);
    let saved = captured
        .objects
        .iter()
        .find(|object| {
            original.dataset.groups["source"]
                .objects
                .iter()
                .any(|value| value == &object.original)
                && object.original.path.ends_with(".parquet")
        })
        .unwrap();
    // Replace the backup directory entry, preserving the independently named source.
    std::fs::remove_file(&saved.backup.path).unwrap();
    std::fs::write(&saved.backup.path, vec![0; saved.backup.bytes as usize]).unwrap();
    storage::verify_object(&saved.original).unwrap();
    drop(engine);
    assert_eq!(
        Engine::restore_local(&workspace, &reference, limits(), false)
            .await
            .unwrap_err()
            .code(),
        "corrupt_object"
    );
    assert!(!workspace.join("catalog-current.json").exists());
    let other = root.path().join("foreign-workspace");
    assert_eq!(
        Engine::restore_local(&other, &reference, limits(), true)
            .await
            .unwrap_err()
            .code(),
        "invalid_restore_point"
    );
    assert!(!other.join("catalog-current.json").exists());
}

#[tokio::test]
async fn live_engine_and_held_stream_keep_the_storage_fence() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let release = engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("point"))
        .await
        .unwrap();
    assert_eq!(
        Engine::restore_local(&workspace, &reference, limits(), false)
            .await
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
    let stream = engine.read_group(&release, "source").await.unwrap();
    drop(engine);
    assert_eq!(
        Engine::restore_local(&workspace, &reference, limits(), false)
            .await
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
    drop(stream);
    Engine::restore_local(&workspace, &reference, limits(), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn catalog_loss_requires_explicit_artifact_import() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let release = engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("point"))
        .await
        .unwrap();
    drop(engine);
    std::fs::rename(
        workspace.join("catalog.sqlite"),
        workspace.join("lost-catalog.sqlite"),
    )
    .unwrap();
    assert_eq!(
        Engine::restore_local(&workspace, &reference, limits(), false)
            .await
            .unwrap_err()
            .code(),
        "restore_point_unregistered"
    );
    assert!(!workspace.join("catalog-current.json").exists());
    Engine::restore_local(&workspace, &reference, limits(), true)
        .await
        .unwrap();
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), release);
}

#[test]
fn delayed_writer_child() {
    let Some(root) = std::env::var_os("DOCSPEC_RESTORE_TEST_ROOT") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = Path::new(&root);
        let engine = Engine::open(root.join("workspace"), limits())
            .await
            .unwrap();
        println!("RESTORE_WRITER_READY");
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        assert_eq!(command.trim(), "continue");
        let request: RunSpec =
            serde_json::from_reader(std::fs::File::open(root.join("delayed.json")).unwrap())
                .unwrap();
        engine.run(&request).await.unwrap();
    });
}

#[tokio::test]
async fn explicit_retirement_ends_restore_admission_and_preserves_creation_evidence() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("point"))
        .await
        .unwrap();
    let captured = point(&reference);
    let backups = std::iter::once(reference.manifest.clone())
        .chain(std::iter::once(captured.catalog))
        .chain(captured.objects.into_iter().map(|object| object.backup))
        .collect::<Vec<_>>();
    assert!(
        engine
            .inspect_retention(&backups)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let plan = engine
        .plan_restore_point_retirement(
            &id("comments"),
            &id("end-recovery"),
            &[id("point")],
            "end fixture recovery promise",
        )
        .await
        .unwrap();
    let retired = engine.retire_restore_points(&plan).await.unwrap();
    assert_eq!(engine.retire_restore_points(&plan).await.unwrap(), retired);
    assert!(
        engine
            .inspect_retention(&backups)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
    assert!(
        engine
            .inspect(&id("comments"))
            .await
            .unwrap()
            .contains_key("restore-materialized/point")
    );
    assert_eq!(
        engine
            .create_restore_point(&id("comments"), &id("point"))
            .await
            .unwrap_err()
            .code(),
        "restore_point_retired"
    );
    drop(engine);
    for import in [false, true] {
        assert_eq!(
            Engine::restore_local(&workspace, &reference, limits(), import)
                .await
                .unwrap_err()
                .code(),
            "restore_point_retired"
        );
    }
    assert!(!workspace.join("catalog-current.json").exists());
    for backup in backups {
        storage::verify_object(&backup).unwrap();
    }
}

#[tokio::test]
async fn independent_delayed_writer_must_finish_before_restore_can_select() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let original = engine
        .run(&spec(root.path(), "original", &[(Some(1), None)]))
        .await
        .unwrap();
    let reference = engine
        .create_restore_point(&id("comments"), &id("point"))
        .await
        .unwrap();
    let delayed = spec(root.path(), "delayed", &[(Some(99), None)]);
    std::fs::write(
        root.path().join("delayed.json"),
        serde_json::to_vec(&delayed).unwrap(),
    )
    .unwrap();
    drop(engine);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "delayed_writer_child", "--nocapture"])
        .env("DOCSPEC_RESTORE_TEST_ROOT", root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line == "RESTORE_WRITER_READY" {
                let _ = send.send(());
            }
        }
    });
    if receive.recv_timeout(Duration::from_secs(30)).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        panic!("independent writer did not acknowledge its workspace fence");
    }
    assert_eq!(
        Engine::restore_local(&workspace, &reference, limits(), false)
            .await
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
    assert!(!workspace.join("catalog-current.json").exists());
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"continue\n")
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("independent writer failed to finish within the fixture deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    reader.join().unwrap();
    assert!(status.success());
    Engine::restore_local(&workspace, &reference, limits(), false)
        .await
        .unwrap();
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), original);
    assert_eq!(rows(&engine, &original).await, vec![(Some(1), None)]);
}
