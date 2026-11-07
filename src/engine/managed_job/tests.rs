//! Independent small-value and failure probes for the retained native job owner.
// Rust guideline compliant 2026-02-21
use super::*;
use arrow::{datatypes::Field, ipc::convert::try_schema_from_ipc_buffer};
use serde_json::json;

fn limits() -> Limits {
    Limits {
        compute_threads: 1,
        memory_bytes: 256 << 20,
        scratch_bytes: 32 << 20,
        batch_rows: 128,
        max_batch_bytes: 128 << 10,
        max_metadata_bytes: 2 << 20,
    }
}

fn artifact(path: PathBuf, value: &impl Serialize) -> JobArtifact {
    let bytes = serde_json::to_vec(value).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    JobArtifact {
        path,
        sha256: sha256(&bytes),
    }
}

fn request(
    root: &Path,
    operation: &str,
    report: &str,
    supplied: Option<JobFileInput>,
) -> ManagedJobCommand {
    let source = root.join("input.jsonl");
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]);
    let input = JobInput::JsonLines {
        name: "raw".into(),
        files: vec![crate::job::JsonLinesFile {
            key: "original".into(),
            path: source.clone(),
        }],
        schema,
    };
    let job: JobSpec = serde_json::from_value(json!({
        "scratch":root.join("caller-scratch"), "inputs":[input],
        "steps":[{"name":"material","sql":"SELECT id, amount FROM raw","materialize":true}],
        "outputs":[{"name":"values","path":root.join("caller-output.parquet"),"sql":"SELECT * FROM material","settings":{
            "profile":"whole","row_group_rows":128,"order_by":[{"field":"id"}],"columns":[],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"independent-native-job-test","footer_limit":65536
        }}], "queries":[{"name":"invariants","sql":report},{"name":"metadata","sql":"SELECT COUNT(*) AS positive_rows FROM outputs.\"values\""}]
    })).unwrap();
    let job = artifact(root.join(format!("job-{operation}.json")), &job);
    let schema = Schema::new(vec![Field::new("invalid_rows", DataType::Int64, false)]);
    let bytes =
        crate::job::retained::encode_schema(&schema, &Budget::new(limits()).unwrap()).unwrap();
    let policy = json!({"id":"policy-one","profile":COUNTERS,
        "evaluator":{"id":"urn:docspec:evaluator:native-job-reports","version":"1"},
        "semantics":{"id":"urn:docspec:semantics:complete-integer-zero-row","revision":"1"},
        "job_sha256":job.sha256,"view_names":{"inputs":["raw"],"steps":["material"],"outputs":["values"],"reports":["invariants","metadata"]},
        "required_reports":[{"name":"invariants","sql_sha256":sha256(report.as_bytes()),"schema_ipc_base64":STANDARD.encode(&bytes.bytes)}]
    });
    let policy = artifact(root.join(format!("policy-{operation}.json")), &policy);
    let binding = supplied.unwrap_or_else(|| {
        let bytes = std::fs::read(source).unwrap();
        JobFileInput {
            name: "raw".into(),
            members: vec![JobFileMember {
                key: Some("original".into()),
                sha256: sha256(&bytes),
                size: bytes.len() as u64,
            }],
        }
    });
    ManagedJobCommand::Prepare(ManagedJobPrepare {
        dataset: Id::new("native-job").unwrap(),
        operation: Id::new(operation).unwrap(),
        definition: "independent-job-v1".into(),
        job,
        file_inputs: vec![binding],
        policy_id: Id::new("policy-one").unwrap(),
        policy,
        expected_epoch: 0,
        expected_selection: None,
        prerequisites: Vec::new(),
    })
}

fn action(receipt: &ManagedJobReceipt, action: &str) -> ManagedJobCommand {
    let mut value = json!({"action":action,"dataset":receipt.dataset,"operation":receipt.operation,"request_digest":receipt.request_digest,"candidate":receipt.candidate,"policy_id":receipt.policy_id,
        "accepted_qualification":if action == "select" { receipt.accepted_qualification.as_ref().unwrap()["identity"].clone() } else { serde_json::Value::Null }
    });
    if action != "select" {
        value
            .as_object_mut()
            .unwrap()
            .remove("accepted_qualification");
    }
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
async fn fresh_admission_snapshot_ends_before_retained_phase_work() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let command = request(
        root.path(),
        "fresh-owner",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    let complete = engine.managed_job(command).await.unwrap();
    assert_eq!(complete.status, "completed");
    assert_eq!(complete.completed_outputs[0]["rows"], 2);
    assert!(
        !engine
            .inner
            .job_admission_read_alive_during_work
            .load(std::sync::atomic::Ordering::Acquire),
        "the obsolete authority snapshot must end before the first durable phase"
    );
    assert!(
        engine
            .inner
            .job_admission_rows
            .lock()
            .await
            .as_ref()
            .unwrap()
            .upgrade()
            .is_none()
    );
}

#[tokio::test]
async fn sealed_phases_reopen_without_original_files_and_successor_reuses_material_work() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let first = request(
        root.path(),
        "original",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    *engine.inner.job_stop_after.lock().await = Some("job-phase/original/report/0".into());
    assert_eq!(
        engine.managed_job(first.clone()).await.unwrap_err().code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .job_status(
                &Id::new("native-job").unwrap(),
                &Id::new("original").unwrap(),
                None
            )
            .await
            .unwrap()
            .status,
        "failed"
    );
    let raw = engine
        .job_record::<Phase>(
            &Id::new("native-job").unwrap(),
            "job-phase/original/report/0",
        )
        .await
        .unwrap()
        .unwrap();
    let material = engine
        .job_record::<Phase>(&Id::new("native-job").unwrap(), "job-phase/original/step/0")
        .await
        .unwrap()
        .unwrap();
    let ManagedJobCommand::Prepare(ref original) = first else {
        unreachable!()
    };
    let binding = original.file_inputs[0].clone();
    for path in [
        &original.job.path,
        &original.policy.path,
        &root.path().join("input.jsonl"),
    ] {
        std::fs::remove_file(path).unwrap();
    }
    let partial = engine
        .job_status(
            &Id::new("native-job").unwrap(),
            &Id::new("original").unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(partial.status, "failed");
    assert!(partial.candidate.is_none());
    assert_eq!(partial.completed_outputs.len(), 1);
    assert_eq!(partial.completed_outputs[0]["rows"], 2);
    assert_eq!(partial.completed_reports.len(), 1);
    assert_eq!(partial.completed_reports[0]["data_status"], "completed");
    assert_eq!(
        partial.completed_reports[0]["data"],
        json!([{"invalid_rows":0}])
    );
    assert_eq!(
        partial.result.as_ref().unwrap()["outputs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let complete = engine.managed_job(first.clone()).await.unwrap();
    assert_eq!(complete.status, "completed");
    assert_eq!(complete.completed_outputs[0]["rows"], 2);
    assert_eq!(
        complete.result.as_ref().unwrap()["queries"][0]["data"],
        json!([{"invalid_rows":0}])
    );
    assert_eq!(
        complete.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"positive_rows":2}])
    );
    let same_raw = engine
        .job_record::<Phase>(&complete.dataset, "job-phase/original/report/0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.object, same_raw.object);
    let accepted = engine
        .managed_job(action(&complete, "accept"))
        .await
        .unwrap();
    assert!(accepted.accepted_qualification.is_some());
    let selected = engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap();
    assert_eq!(selected.selection, "selected");
    let successor = request(
        root.path(),
        "successor",
        "SELECT COUNT(*) - 2 + 0 AS invalid_rows FROM outputs.\"values\"",
        Some(binding),
    );
    let ManagedJobCommand::Prepare(mut successor) = successor else {
        unreachable!()
    };
    successor.expected_selection = selected.candidate.clone();
    let successor = engine
        .managed_job(ManagedJobCommand::Prepare(successor))
        .await
        .unwrap();
    let reused = engine
        .job_record::<Phase>(&complete.dataset, "job-phase/successor/step/0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        material.object, reused.object,
        "changed report SQL must reuse the unchanged materialized relation"
    );
    let successor = engine
        .managed_job(action(&successor, "accept"))
        .await
        .unwrap();
    engine
        .managed_job(action(&successor, "select"))
        .await
        .unwrap();
    engine.compact_lifecycle(&complete.dataset).await.unwrap();
    let historical = engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap();
    assert_eq!(historical.candidate, complete.candidate);
    let authority = engine
        .job_record::<Authority>(&complete.dataset, "authority")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.job_selected, successor.candidate);
}

#[tokio::test]
async fn native_counter_refusal_keeps_completed_computation_and_blocks_selection() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let prepared = engine
        .managed_job(request(
            root.path(),
            "nonzero",
            "SELECT COUNT(*) - 1 AS invalid_rows FROM outputs.\"values\"",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(
        engine
            .managed_job(action(&prepared, "accept"))
            .await
            .unwrap_err()
            .code(),
        "job_report_failed"
    );
    let inspected = engine
        .managed_job(ManagedJobCommand::Inspect {
            dataset: prepared.dataset,
            operation: prepared.operation,
        })
        .await
        .unwrap();
    assert_eq!(inspected.status, "completed");
    assert!(inspected.accepted_qualification.is_none());
    assert_eq!(inspected.selection, "pending");
}

#[tokio::test]
async fn an_unaccepted_candidate_requalifies_exact_native_scalar_report_schemas() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let sql = "SELECT (SELECT COUNT(*) FROM outputs.\"values\" WHERE id < 0) AS negative_ids, (SELECT COUNT(*) FROM outputs.\"values\" WHERE amount > 10) AS high_amounts, (SELECT COUNT(*) FROM outputs.\"values\" WHERE id > 2) AS high_ids";
    let ManagedJobCommand::Prepare(mut original) =
        request(root.path(), "unaccepted-schema", sql, None)
    else {
        unreachable!()
    };
    let names = ["negative_ids", "high_amounts", "high_ids"];
    let initial_schema = Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    );
    let mut initial_policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&original.policy.path).unwrap()).unwrap();
    initial_policy["required_reports"][0]["schema_ipc_base64"] = json!(
        STANDARD.encode(
            crate::job::retained::encode_schema(&initial_schema, &Budget::new(limits()).unwrap())
                .unwrap()
                .bytes
        )
    );
    original.policy = artifact(original.policy.path, &initial_policy);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let completed = engine
        .managed_job(ManagedJobCommand::Prepare(original.clone()))
        .await
        .unwrap();
    assert_eq!(completed.status, "completed");
    assert!(completed.accepted_qualification.is_none());
    assert_eq!(
        engine
            .managed_job(action(&completed, "accept"))
            .await
            .unwrap_err()
            .code(),
        "job_report_schema"
    );
    let phase = engine
        .job_record::<Phase>(&completed.dataset, "job-phase/unaccepted-schema/report/0")
        .await
        .unwrap()
        .unwrap();
    let actual = Schema::new(vec![
        Field::new(names[0], DataType::Int64, false),
        Field::new(names[1], DataType::Int64, true),
        Field::new(names[2], DataType::Int64, true),
    ]);
    let bytes = std::fs::read(&phase.object.as_ref().unwrap().path).unwrap();
    assert_eq!(
        StreamReader::try_new(Cursor::new(&bytes), None)
            .unwrap()
            .schema()
            .as_ref(),
        &actual
    );
    assert_eq!(
        try_schema_from_ipc_buffer(&STANDARD.decode(&phase.schema_ipc_base64).unwrap()).unwrap(),
        actual,
        "retained report inventory records actual IPC nullability"
    );
    let candidate = completed.candidate.as_ref().unwrap();
    let original_manifest = std::fs::read(&candidate.manifest.path).unwrap();
    for path in [
        &original.job.path,
        &original.policy.path,
        &root.path().join("input.jsonl"),
    ] {
        std::fs::remove_file(path).unwrap();
    }
    let mut next_policy = initial_policy.clone();
    next_policy["id"] = json!("policy-two");
    next_policy["required_reports"][0]["schema_ipc_base64"] = json!(
        STANDARD.encode(
            crate::job::retained::encode_schema(&actual, &Budget::new(limits()).unwrap())
                .unwrap()
                .bytes
        )
    );
    let mut stale_policy = next_policy.clone();
    stale_policy["id"] = json!("policy-stale");
    let stale_policy = artifact(root.path().join("policy-stale.json"), &stale_policy);
    let stale:ManagedJobCommand = serde_json::from_value(json!({"action":"requalify","dataset":completed.dataset,"operation":completed.operation,"request_digest":completed.request_digest,"candidate":candidate,"previous_policy_id":"policy-one","policy_id":"policy-stale","policy":stale_policy,"expected_epoch":1,"expected_selection":null})).unwrap();
    assert_eq!(
        engine.managed_job(stale).await.unwrap_err().code(),
        "stale_authority"
    );
    assert!(
        engine
            .job_record::<Decision>(
                &completed.dataset,
                "job-policy/unaccepted-schema/policy-stale"
            )
            .await
            .unwrap()
            .is_none()
    );
    let policy = artifact(root.path().join("policy-two.json"), &next_policy);
    let requalified = engine.managed_job(serde_json::from_value(json!({"action":"requalify","dataset":completed.dataset,"operation":completed.operation,"request_digest":completed.request_digest,"candidate":candidate,"previous_policy_id":"policy-one","policy_id":"policy-two","policy":policy,"expected_epoch":0,"expected_selection":null})).unwrap()).await.unwrap();
    assert!(
        requalified.accepted_qualification.is_none(),
        "requalification never invents a prior accepted record"
    );
    assert_eq!(requalified.candidate, completed.candidate);
    let accepted = engine
        .managed_job(action(&requalified, "accept"))
        .await
        .unwrap();
    assert!(accepted.accepted_qualification.is_some());
    assert_eq!(accepted.candidate, completed.candidate);
    let selected = engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap();
    assert_eq!(selected.selection, "selected");
    assert_eq!(selected.candidate, completed.candidate);
    assert_eq!(
        std::fs::read(&candidate.manifest.path).unwrap(),
        original_manifest
    );
    assert!(
        engine
            .job_record::<qualification::Accepted>(
                &completed.dataset,
                "job-accepted/unaccepted-schema/policy-one"
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine
            .job_record::<Decision>(
                &completed.dataset,
                "job-policy/unaccepted-schema/policy-one"
            )
            .await
            .unwrap()
            .unwrap()
            .policy
            .sha256,
        original.policy.sha256,
        "the original policy remains pinned"
    );
}

#[tokio::test]
async fn an_unsealed_report_retries_with_a_fresh_artifact_after_batch_size_changes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let ManagedJobCommand::Prepare(mut command) = request(
        root.path(),
        "report-window",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    ) else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&command.job.path).unwrap()).unwrap();
    job["queries"][1]["sql"] = json!("SELECT id, amount FROM outputs.\"values\" ORDER BY id");
    command.job = artifact(command.job.path, &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&command.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(command.job.sha256);
    command.policy = artifact(command.policy.path, &policy);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    *engine.inner.job_stop_after.lock().await =
        Some("before:job-phase/report-window/report/1".into());
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(command.clone()))
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let admitted = engine
        .job_record::<Admission>(&command.dataset, "job-operation/report-window")
        .await
        .unwrap()
        .unwrap();
    let unsealed = std::fs::read_dir(&admitted.directory)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("report-1-"))
        .unwrap()
        .path();
    let old_bytes = std::fs::read(&unsealed).unwrap();
    assert!(
        engine
            .job_record::<Phase>(&command.dataset, "job-phase/report-window/report/1")
            .await
            .unwrap()
            .is_none()
    );
    for path in [
        &command.job.path,
        &command.policy.path,
        &root.path().join("input.jsonl"),
    ] {
        std::fs::remove_file(path).unwrap();
    }
    drop(admitted);
    drop(engine);
    let mut changed_limits = limits();
    changed_limits.batch_rows = 1;
    let engine = Engine::open(root.path().join("host"), changed_limits)
        .await
        .unwrap();
    let complete = engine
        .managed_job(ManagedJobCommand::Prepare(command))
        .await
        .unwrap();
    assert_eq!(
        complete.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"id":1,"amount":10},{"id":2,"amount":null}])
    );
    let phase = engine
        .job_record::<Phase>(&complete.dataset, "job-phase/report-window/report/1")
        .await
        .unwrap()
        .unwrap();
    let final_path = Path::new(&phase.object.as_ref().unwrap().path);
    assert_ne!(final_path, unsealed);
    assert_ne!(
        std::fs::read(final_path).unwrap(),
        old_bytes,
        "independent batch boundaries must not collide at an immutable unsealed report path"
    );
}

fn alias_request(
    root: &Path,
    operation: &str,
    supplied: Option<Vec<JobFileInput>>,
) -> ManagedJobCommand {
    let base = request(
        root,
        operation,
        "SELECT COUNT(*) - 1 AS invalid_rows FROM outputs.\"values\"",
        Some(JobFileInput {
            name: "raw".into(),
            members: Vec::new(),
        }),
    );
    let ManagedJobCommand::Prepare(mut request) = base else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.job.path).unwrap()).unwrap();
    let template = job["inputs"][0].clone();
    job["inputs"] = json!(
        ["source_a", "source_b"]
            .iter()
            .map(|name| {
                let mut input = template.clone();
                input["name"] = json!(name);
                input["files"] = json!([{"key":name,"path":root.join(format!("{name}.jsonl"))}]);
                input
            })
            .collect::<Vec<_>>()
    );
    job["steps"][0]["sql"] = json!("SELECT source_a.id, source_a.amount FROM source_b AS source_a");
    job["queries"][1]["sql"] =
        json!("SELECT SUM(amount) AS observed_total FROM outputs.\"values\"");
    request.job = artifact(request.job.path, &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(request.job.sha256);
    policy["view_names"]["inputs"] = json!(["source_a", "source_b"]);
    request.policy = artifact(request.policy.path, &policy);
    request.file_inputs = supplied.unwrap_or_else(|| {
        ["source_a", "source_b"]
            .into_iter()
            .map(|name| {
                let bytes = std::fs::read(root.join(format!("{name}.jsonl"))).unwrap();
                JobFileInput {
                    name: name.into(),
                    members: vec![JobFileMember {
                        key: Some(name.into()),
                        sha256: sha256(&bytes),
                        size: bytes.len() as u64,
                    }],
                }
            })
            .collect()
    });
    ManagedJobCommand::Prepare(request)
}

#[tokio::test]
async fn caller_alias_cannot_hide_the_source_that_invalidates_a_materialization() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("source_a.jsonl"),
        b"{\"id\":1,\"amount\":10}\n",
    )
    .unwrap();
    std::fs::write(
        root.path().join("source_b.jsonl"),
        b"{\"id\":1,\"amount\":20}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let first_request = alias_request(root.path(), "alias-original", None);
    let ManagedJobCommand::Prepare(ref initial) = first_request else {
        unreachable!()
    };
    let mut bindings = initial.file_inputs.clone();
    let first = engine.managed_job(first_request).await.unwrap();
    assert_eq!(
        first.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"observed_total":20}])
    );
    let old = engine
        .job_record::<Phase>(&first.dataset, "job-phase/alias-original/step/0")
        .await
        .unwrap()
        .unwrap();
    assert!(
        old.dependencies
            .iter()
            .any(|dependency| dependency.starts_with("public.\"source_b\":"))
    );
    assert!(
        !old.dependencies
            .iter()
            .any(|dependency| dependency.starts_with("public.\"source_a\":"))
    );
    std::fs::remove_file(root.path().join("source_a.jsonl")).unwrap();
    let changed = b"{\"id\":1,\"amount\":30}\n";
    std::fs::write(root.path().join("source_b.jsonl"), changed).unwrap();
    bindings[1].members[0].sha256 = sha256(changed);
    bindings[1].members[0].size = changed.len() as u64;
    let second = engine
        .managed_job(alias_request(
            root.path(),
            "alias-successor",
            Some(bindings),
        ))
        .await
        .unwrap();
    assert_eq!(
        second.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"observed_total":30}])
    );
    let new = engine
        .job_record::<Phase>(&first.dataset, "job-phase/alias-successor/step/0")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        old.object, new.object,
        "a caller alias must not authorize stale material reuse"
    );
}

fn subquery_request(
    root: &Path,
    operation: &str,
    sql: &str,
    bindings: Option<Vec<JobFileInput>>,
) -> ManagedJobCommand {
    let ManagedJobCommand::Prepare(mut request) = alias_request(root, operation, bindings) else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.job.path).unwrap()).unwrap();
    job["steps"][0]["sql"] = json!(sql);
    job["queries"][1]["sql"] = json!(
        "SELECT COALESCE(SUM(amount), CAST(0 AS BIGINT)) AS observed_total FROM outputs.\"values\""
    );
    request.job = artifact(request.job.path, &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(request.job.sha256);
    request.policy = artifact(request.policy.path, &policy);
    ManagedJobCommand::Prepare(request)
}

async fn subquery_change_is_bound(sql: &str, initial_value: i64, changed_value: i64) {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("source_a.jsonl"),
        b"{\"id\":1,\"amount\":30}\n",
    )
    .unwrap();
    std::fs::write(
        root.path().join("source_b.jsonl"),
        b"{\"id\":1,\"amount\":20}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let original = subquery_request(root.path(), "subquery-original", sql, None);
    let ManagedJobCommand::Prepare(ref initial) = original else {
        unreachable!()
    };
    let mut bindings = initial.file_inputs.clone();
    let first = engine.managed_job(original).await.unwrap();
    assert_eq!(
        first.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"observed_total":initial_value}])
    );
    let old = engine
        .job_record::<Phase>(&first.dataset, "job-phase/subquery-original/step/0")
        .await
        .unwrap()
        .unwrap();
    for name in ["source_a", "source_b"] {
        assert!(
            old.dependencies
                .iter()
                .any(|dependency| dependency.starts_with(&format!("public.\"{name}\":"))),
            "subquery input {name} must have an exact native dependency"
        );
        std::fs::remove_file(root.path().join(format!("{name}.jsonl"))).unwrap();
    }
    let unchanged = engine
        .managed_job(subquery_request(
            root.path(),
            "subquery-unchanged",
            sql,
            Some(bindings.clone()),
        ))
        .await
        .unwrap();
    assert_eq!(
        unchanged.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"observed_total":initial_value}])
    );
    let reused = engine
        .job_record::<Phase>(&first.dataset, "job-phase/subquery-unchanged/step/0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old.object, reused.object,
        "unchanged inputs reuse the sealed material without caller files"
    );
    let changed = b"{\"id\":1,\"amount\":30}\n";
    std::fs::write(root.path().join("source_b.jsonl"), changed).unwrap();
    bindings[1].members[0].sha256 = sha256(changed);
    bindings[1].members[0].size = changed.len() as u64;
    let next = engine
        .managed_job(subquery_request(
            root.path(),
            "subquery-successor",
            sql,
            Some(bindings),
        ))
        .await
        .unwrap();
    assert_eq!(
        next.result.as_ref().unwrap()["queries"][1]["data"],
        json!([{"observed_total":changed_value}])
    );
    let new = engine
        .job_record::<Phase>(&first.dataset, "job-phase/subquery-successor/step/0")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        old.object, new.object,
        "changing only a subquery input invalidates the material"
    );
    assert_ne!(old.dependencies, new.dependencies);
}

#[tokio::test]
async fn scalar_subquery_changes_invalidate_material_and_unchanged_inputs_reuse_it() {
    subquery_change_is_bound(
        "SELECT a.id, (SELECT SUM(amount) FROM source_b) AS amount FROM source_a a",
        20,
        30,
    )
    .await;
}

#[tokio::test]
async fn exists_subquery_changes_invalidate_material_and_unchanged_inputs_reuse_it() {
    subquery_change_is_bound("SELECT a.id, CAST(1 AS BIGINT) AS amount FROM source_a a WHERE EXISTS (SELECT 1 FROM source_b b WHERE b.amount = a.amount)", 0, 1).await;
}

#[tokio::test]
async fn in_subquery_changes_invalidate_material_and_unchanged_inputs_reuse_it() {
    subquery_change_is_bound("SELECT a.id, CAST(1 AS BIGINT) AS amount FROM source_a a WHERE a.amount IN (SELECT amount FROM source_b)", 0, 1).await;
}

#[tokio::test]
async fn a_late_missing_input_member_reuses_the_first_sealed_copy() {
    use std::os::unix::fs::MetadataExt;
    let root = tempfile::tempdir().unwrap();
    let first_bytes = b"{\"id\":1,\"amount\":10}\n";
    let second_bytes = b"{\"id\":2,\"amount\":null}\n";
    let first_path = root.path().join("first.jsonl");
    let second_path = root.path().join("second.jsonl");
    std::fs::write(&first_path, first_bytes).unwrap();
    let members = vec![
        JobFileMember {
            key: Some("first".into()),
            sha256: sha256(first_bytes),
            size: first_bytes.len() as u64,
        },
        JobFileMember {
            key: Some("second".into()),
            sha256: sha256(second_bytes),
            size: second_bytes.len() as u64,
        },
    ];
    let ManagedJobCommand::Prepare(mut prepare) = request(
        root.path(),
        "member-retry",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        Some(JobFileInput {
            name: "raw".into(),
            members,
        }),
    ) else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.job.path).unwrap()).unwrap();
    job["inputs"][0]["files"] =
        json!([{"key":"first","path":first_path},{"key":"second","path":second_path}]);
    prepare.job = artifact(prepare.job.path, &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(prepare.job.sha256);
    prepare.policy = artifact(prepare.policy.path, &policy);
    let command = ManagedJobCommand::Prepare(prepare.clone());
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "io"
    );
    let before = engine
        .job_record::<execution::MemberCompletion>(&prepare.dataset, "job-member/member-retry/0/0")
        .await
        .unwrap()
        .unwrap();
    let object = before.member.retained.clone();
    let inode = std::fs::metadata(&object.path).unwrap().ino();
    std::fs::remove_file(first_path).unwrap();
    std::fs::remove_file(&prepare.job.path).unwrap();
    std::fs::remove_file(&prepare.policy.path).unwrap();
    std::fs::write(second_path, second_bytes).unwrap();
    let completed = engine.managed_job(command).await.unwrap();
    assert_eq!(completed.status, "completed");
    assert_eq!(
        completed.result.as_ref().unwrap()["queries"][0]["data"],
        json!([{"invalid_rows":0}])
    );
    let after = engine
        .job_record::<execution::MemberCompletion>(&prepare.dataset, "job-member/member-retry/0/0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(object, after.member.retained);
    assert_eq!(inode, std::fs::metadata(&object.path).unwrap().ino());
    let directory = Path::new(&object.path).parent().unwrap();
    let copied = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with("input-0-"))
        .collect::<Vec<_>>();
    assert_eq!(
        copied.len(),
        2,
        "the completed first member must not consume duplicate retained allocation"
    );
}

#[tokio::test]
async fn a_phase_with_another_origin_runtime_cannot_be_relabelled() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let command = request(
        root.path(),
        "origin-refusal",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    *engine.inner.job_stop_after.lock().await = Some("job-phase/origin-refusal/step/0".into());
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let dataset = Id::new("native-job").unwrap();
    let key = "job-phase/origin-refusal/step/0";
    let saved = engine
        .job_record::<Phase>(&dataset, key)
        .await
        .unwrap()
        .unwrap();
    let mut prior_runtime = saved.value;
    prior_runtime.origin_runtime.binary_sha256 = "f".repeat(64);
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .commit(&view, vec![record(key, prior_runtime).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        engine.managed_job(command).await.unwrap_err().code(),
        "unqualified_job_runtime"
    );
    assert!(
        engine
            .job_status(&dataset, &Id::new("origin-refusal").unwrap(), None)
            .await
            .unwrap()
            .candidate
            .is_none()
    );
}
