//! Automatic workspace lifecycle baseline compaction across high-step-count jobs.
// Rust guideline compliant 2026-02-21

use super::*;
use std::os::unix::fs::MetadataExt;

const COMPACT_SNAPSHOTS: usize = 100;

fn count_lifecycle_metadata_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0;
    for entry in entries.flatten() {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            count += count_lifecycle_metadata_files(&entry_path);
        } else if entry_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".metadata.json"))
        {
            count += 1;
        }
    }
    count
}

fn multi_step_request(root: &Path, operation: &str, step_count: usize) -> ManagedJobCommand {
    let report = "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"";
    let ManagedJobCommand::Prepare(mut prepare) = request(root, operation, report, None) else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.job.path).unwrap()).unwrap();
    let mut steps = Vec::with_capacity(step_count);
    let mut step_names = Vec::with_capacity(step_count);
    for idx in 0..step_count {
        let name = format!("step_{idx}");
        let sql = if idx == 0 {
            "SELECT id, amount + CAST(1 AS BIGINT) AS amount FROM raw".to_owned()
        } else {
            format!(
                "SELECT id, amount + CAST(1 AS BIGINT) AS amount FROM step_{}",
                idx - 1
            )
        };
        steps.push(json!({
            "name": name,
            "sql": sql,
            "materialize": true,
        }));
        step_names.push(format!("step_{idx}"));
    }
    job["steps"] = json!(steps);
    job["outputs"][0]["sql"] = json!(format!("SELECT * FROM step_{}", step_count - 1));
    job["outputs"][0]["path"] = json!(root.join(format!("caller-output-{operation}.parquet")));
    job["queries"][1] = json!({
        "name": "total",
        "sql": "SELECT CAST(SUM(amount) AS BIGINT) AS total_amount FROM outputs.\"values\"",
    });
    prepare.job = artifact(prepare.job.path, &job);

    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(prepare.job.sha256);
    policy["view_names"]["steps"] = json!(step_names);
    policy["view_names"]["reports"] = json!(["invariants", "total"]);
    prepare.policy = artifact(prepare.policy.path, &policy);
    ManagedJobCommand::Prepare(prepare)
}

#[tokio::test]
async fn auto_compacts_workspace_lifecycle_and_preserves_recovery_reuse_and_retirement() {
    let root = tempfile::tempdir().unwrap();
    let raw_bytes = b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":20}\n";
    let raw_path = root.path().join("input.jsonl");
    std::fs::write(&raw_path, raw_bytes).unwrap();

    let host = root.path().join("host");
    let engine = Engine::open(&host, limits()).await.unwrap();
    let dataset = Id::new("native-job").unwrap();

    // 1. Commit one complete retirement receipt before auto-compaction runs.
    let early_cmd = request(
        root.path(),
        "retired-early",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    let ManagedJobCommand::Prepare(early_prepare) = early_cmd.clone() else {
        unreachable!()
    };
    engine.managed_job(early_cmd).await.unwrap();
    retire_producer_fixture_job(&engine, &early_prepare, true).await;

    // 2. Capture one uncommitted retirement plan before auto-compaction expires its snapshot.
    let prior_cmd = request(
        root.path(),
        "prior",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    let ManagedJobCommand::Prepare(prior_prepare) = prior_cmd.clone() else {
        unreachable!()
    };
    engine.managed_job(prior_cmd).await.unwrap();
    let stale_plan = engine
        .plan_availability_retirement(
            &dataset,
            &Id::new("prior-stale").unwrap(),
            &prior_prepare.operation,
            "captured before baseline compaction",
        )
        .await
        .unwrap();

    // 3. Run a 110-step job (~229 lifecycle commits) with an injected crash at step 104.
    let multi_cmd = multi_step_request(root.path(), "multi-110", 110);
    let ManagedJobCommand::Prepare(multi_prepare) = multi_cmd.clone() else {
        unreachable!()
    };
    *engine.inner.job_stop_after.lock().await = Some("job-phase/multi-110/step/104".into());
    assert_eq!(
        engine
            .managed_job(multi_cmd.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );

    let mid_view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    let mid_snapshots = mid_view.table.metadata().snapshots().count();
    let mid_files = count_lifecycle_metadata_files(&host.join("warehouse"));
    assert!(
        mid_snapshots < COMPACT_SNAPSHOTS,
        "expected < {COMPACT_SNAPSHOTS} snapshots after >200 commits, got {mid_snapshots}"
    );
    assert!(
        mid_files < COMPACT_SNAPSHOTS,
        "expected < {COMPACT_SNAPSHOTS} .metadata.json files on disk, got {mid_files}"
    );

    let step0_before = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-110/step/0")
        .await
        .unwrap()
        .unwrap()
        .value;
    let step104_before = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-110/step/104")
        .await
        .unwrap()
        .unwrap()
        .value;
    let step0_obj = step0_before.object.clone().unwrap();
    let step104_obj = step104_before.object.clone().unwrap();
    let step0_ino = std::fs::metadata(&step0_obj.path).unwrap().ino();
    let step104_ino = std::fs::metadata(&step104_obj.path).unwrap().ino();

    // 4. Resume without caller input file; sealed steps 0..=104 must not recompute.
    std::fs::remove_file(&raw_path).unwrap();
    let completed = engine.managed_job(multi_cmd).await.unwrap();
    assert_eq!(completed.status, "completed");
    assert_eq!(
        completed.completed_reports[1]["data"],
        json!([{"total_amount": 250}])
    );

    let step0_after = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-110/step/0")
        .await
        .unwrap()
        .unwrap()
        .value;
    let step104_after = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-110/step/104")
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(step0_after.object, Some(step0_obj.clone()));
    assert_eq!(step104_after.object, Some(step104_obj.clone()));
    assert_eq!(std::fs::metadata(&step0_obj.path).unwrap().ino(), step0_ino);
    assert_eq!(
        std::fs::metadata(&step104_obj.path).unwrap().ino(),
        step104_ino
    );

    let post_view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    assert!(post_view.table.metadata().snapshots().count() < COMPACT_SNAPSHOTS);
    assert!(count_lifecycle_metadata_files(&host.join("warehouse")) < COMPACT_SNAPSHOTS);

    // 5. Uncommitted pre-baseline retirement plan is refused; a freshly captured plan succeeds.
    assert_eq!(
        engine
            .retire_availability(&stale_plan)
            .await
            .unwrap_err()
            .code(),
        "invalid_retirement"
    );
    retire_producer_fixture_job(&engine, &prior_prepare, true).await;

    // 6. Second job with identical SQL reuses all 110 `job-material/*` steps across compaction.
    std::fs::write(&raw_path, raw_bytes).unwrap();
    let reuse_cmd = multi_step_request(root.path(), "multi-reuse", 110);
    let ManagedJobCommand::Prepare(reuse_prepare) = reuse_cmd.clone() else {
        unreachable!()
    };
    let reused = engine.managed_job(reuse_cmd).await.unwrap();
    assert_eq!(reused.status, "completed");
    assert_eq!(
        reused.completed_reports[1]["data"],
        json!([{"total_amount": 250}])
    );
    let reuse_step0 = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-reuse/step/0")
        .await
        .unwrap()
        .unwrap()
        .value;
    let reuse_step109 = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-reuse/step/109")
        .await
        .unwrap()
        .unwrap()
        .value;
    let orig_step109 = engine
        .job_record::<Phase>(&dataset, "job-phase/multi-110/step/109")
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(reuse_step0.object, Some(step0_obj.clone()));
    assert_eq!(reuse_step109.object, orig_step109.object);
    let reuse_dir = host.join("objects/native-job/job-multi-reuse");
    assert!(
        !std::fs::read_dir(&reuse_dir).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("step-")),
        "reused steps must not write duplicate step Parquet files"
    );
    let reuse_view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    assert!(reuse_view.table.metadata().snapshots().count() < COMPACT_SNAPSHOTS);
    assert!(count_lifecycle_metadata_files(&host.join("warehouse")) < COMPACT_SNAPSHOTS);

    // 7. Full retirement + compact_lifecycle succeeds across pre- and post-baseline receipts.
    let records = engine.inspect(&dataset).await.unwrap();
    let remaining_keys = records
        .iter()
        .filter(|(key, value)| {
            super::super::retention::retirement::completion_key(key)
                && !key.contains("/retired-early/")
                && !key.contains("/prior/")
                && value
                    .get("operation")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|op| op != "retired-early" && op != "prior")
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let phases = engine
        .plan_phase_retirement(
            &dataset,
            &Id::new("multi-all-phases").unwrap(),
            &remaining_keys,
            "retire remaining phases after auto-compaction",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    for op_prepare in [&multi_prepare, &reuse_prepare] {
        let availability = engine
            .plan_availability_retirement(
                &dataset,
                &Id::new(format!("{}-availability", op_prepare.operation)).unwrap(),
                &op_prepare.operation,
                "retire multi availability",
            )
            .await
            .unwrap();
        engine.retire_availability(&availability).await.unwrap();
        let recovery = engine
            .plan_operation_retirement(
                &dataset,
                &Id::new(format!("{}-recovery", op_prepare.operation)).unwrap(),
                &op_prepare.operation,
                "retire multi operation recovery",
            )
            .await
            .unwrap();
        engine.retire_operation(&recovery).await.unwrap();
    }
    engine.compact_lifecycle(&dataset).await.unwrap();
    assert!(
        engine
            .inspect_retention(&[step0_obj, step104_obj])
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
}
