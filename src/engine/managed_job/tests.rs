//! Independent small-value and failure probes for the retained native job owner.
// Rust guideline compliant 2026-02-21
use super::publication::{PublicationIntentReceipt, PublicationIntentStatus};
use super::*;
use arrow::{datatypes::Field, ipc::convert::try_schema_from_ipc_buffer};
use serde_json::json;

async fn select_publication_fixture(
    engine: &Engine,
    root: &Path,
    operation: &str,
    previous: Option<JobCandidateRef>,
) -> ManagedJobReceipt {
    let ManagedJobCommand::Prepare(mut prepare) = request(
        root,
        operation,
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    ) else {
        unreachable!()
    };
    prepare.expected_selection = previous;
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.job.path).unwrap()).unwrap();
    let mut private = job["outputs"][0].clone();
    private["name"] = json!("private");
    private["path"] = json!(root.join("private.parquet"));
    private["sql"] = json!("SELECT * FROM material WHERE id = 2");
    job["outputs"].as_array_mut().unwrap().push(private);
    prepare.job = artifact(prepare.job.path, &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&prepare.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(prepare.job.sha256);
    policy["view_names"]["outputs"] = json!(["values", "private"]);
    prepare.policy = artifact(prepare.policy.path, &policy);
    let completed = engine
        .managed_job(ManagedJobCommand::Prepare(prepare))
        .await
        .unwrap();
    let accepted = engine
        .managed_job(action(&completed, "accept"))
        .await
        .unwrap();
    engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap()
}

async fn publication_freeze_spec(
    engine: &Engine,
    root: &Path,
    selected: &ManagedJobReceipt,
) -> serde_json::Value {
    let authority = engine
        .job_record::<Authority>(&selected.dataset, "authority")
        .await
        .unwrap()
        .unwrap();
    let manifest = artifact(root.join("manifest.json"), &json!({"fixture": "complete"}));
    let catalog = artifact(
        root.join("catalog.json"),
        &json!({"fixture": "authored catalog"}),
    );
    let outputs = selected.completed_outputs.iter().map(|output| {
        let disposition = if output["name"] == "values" {
            json!({"kind":"published","name":"rows.parquet","content_type":"application/octet-stream"})
        } else {
            json!({"kind":"private"})
        };
        json!({"name":output["name"],"object":output["completion"],"sha256":output["sha256"],
               "md5":output["md5"],"rows":output["rows"],"disposition":disposition})
    }).collect::<Vec<_>>();
    json!({
        "action":"freeze","dataset":selected.dataset,"operation":selected.operation,
        "candidate":selected.candidate,"job_request_digest":selected.request_digest,
        "policy_id":selected.policy_id,
        "accepted_qualification":selected.accepted_qualification.as_ref().unwrap()["identity"],
        "expected_epoch":authority.epoch,"expected_selection_revision":authority.selection_revision,
        "scope":{"kind":"local_fixture","publisher":"fixture","bucket":"test",
                 "release_prefix":"assets/release","pointer_key":"selected/manifest.json"},
        "release_id":"a".repeat(64),"outputs":outputs,
        "controls":[
            {"name":"manifest.json","content_type":"application/json","size":std::fs::metadata(&manifest.path).unwrap().len(),"artifact":manifest},
            {"name":"catalog.json","content_type":"application/json","size":std::fs::metadata(&catalog.path).unwrap().len(),"artifact":catalog}
        ],
        "expected_pointer":{"kind":"present","etag":"\"original-etag\"","bytes_base64":STANDARD.encode(b"{ \"old\": true }\n")},
        "pointer":{"content_type":"application/json","cache_control":"no-store","bytes_base64":STANDARD.encode(b"{ \"release\": \"chosen\" }\n")}
    })
}

async fn publication_command(
    engine: &Engine,
    spec: &serde_json::Value,
) -> Result<PublicationIntentReceipt> {
    engine
        .job_publication_intent_bytes(serde_json::to_vec(spec)?)
        .await
}

async fn assert_publication_replay(
    engine: &Engine,
    spec: &serde_json::Value,
    saved: &PublicationIntentReceipt,
) {
    let actual = publication_command(engine, spec).await.unwrap();
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(saved).unwrap()
    );
}

#[tokio::test]
async fn publication_intent_replays_and_protects_only_its_live_delivery() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":20}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let missing = publication_command(
        &engine,
        &json!({
            "action":"inspect","dataset":"no-such-dataset","operation":"no-intent"
        }),
    )
    .await
    .unwrap();
    assert_eq!(missing.status, PublicationIntentStatus::NotFound);
    assert!(missing.intent.is_none() && missing.payload.is_none());

    let prior = select_publication_fixture(&engine, root.path(), "prior-release", None).await;
    let prior_spec = publication_freeze_spec(&engine, root.path(), &prior).await;
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":21}\n{\"id\":2,\"amount\":22}\n",
    )
    .unwrap();
    let selected = select_publication_fixture(
        &engine,
        root.path(),
        "first-release",
        prior.candidate.clone(),
    )
    .await;
    assert_eq!(
        publication_command(&engine, &prior_spec)
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
    assert!(
        !engine
            .inspect(&prior.dataset)
            .await
            .unwrap()
            .keys()
            .any(|key| publication::is_record(key))
    );
    let spec = publication_freeze_spec(&engine, root.path(), &selected).await;
    assert_eq!(spec["outputs"][0]["rows"], 2);
    assert_eq!(spec["outputs"][1]["rows"], 1);
    let inspect =
        json!({"action":"inspect","dataset":selected.dataset,"operation":selected.operation});
    for (path, code) in [
        ("epoch", "stale_authority"),
        ("acceptance", "stale_authority"),
        ("revision", "stale_authority"),
        ("rows", "publication_intent_mismatch"),
        ("pointer", "publication_intent_mismatch"),
    ] {
        let mut changed = spec.clone();
        match path {
            "epoch" => changed["expected_epoch"] = json!(1),
            "acceptance" => changed["accepted_qualification"] = json!("0".repeat(64)),
            "revision" => changed["expected_selection_revision"] = json!(1),
            "rows" => changed["outputs"][0]["rows"] = json!(3),
            "pointer" => changed["scope"]["pointer_key"] = json!("assets/release/manifest.json"),
            _ => unreachable!(),
        }
        assert_eq!(
            publication_command(&engine, &changed)
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            publication_command(&engine, &inspect).await.unwrap().status,
            PublicationIntentStatus::NotFound
        );
        assert!(
            !engine
                .inspect(&selected.dataset)
                .await
                .unwrap()
                .keys()
                .any(|key| publication::is_record(key))
        );
    }
    let mut unsupported = spec.clone();
    unsupported["scope"]["kind"] = json!("r2");
    assert!(publication_command(&engine, &unsupported).await.is_err());
    assert_eq!(
        publication_command(&engine, &inspect).await.unwrap().status,
        PublicationIntentStatus::NotFound
    );
    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let frozen = publication_command(&engine, &spec).await.unwrap();
    assert_eq!(frozen.status, PublicationIntentStatus::Retained);
    assert_eq!(frozen.external_authorization, "not_run");
    assert_eq!(frozen.dispatch, "not_run");
    let audit = frozen.intent.as_ref().unwrap();
    assert_eq!(audit.job_request_digest, selected.request_digest);
    assert_ne!(audit.reference.intent_digest, audit.job_request_digest);
    assert_eq!(
        STANDARD
            .decode(&frozen.payload.as_ref().unwrap().pointer_bytes_base64)
            .unwrap(),
        b"{ \"release\": \"chosen\" }\n",
    );
    assert_eq!(
        STANDARD
            .decode(
                frozen
                    .payload
                    .as_ref()
                    .unwrap()
                    .predecessor_bytes_base64
                    .as_ref()
                    .unwrap()
            )
            .unwrap(),
        b"{ \"old\": true }\n",
    );
    let published: ObjectRef =
        serde_json::from_value(spec["outputs"][0]["object"].clone()).unwrap();
    let private: ObjectRef = serde_json::from_value(spec["outputs"][1]["object"].clone()).unwrap();
    let manifest = audit.candidate.manifest.clone();
    assert_eq!(
        frozen.objects().collect::<Vec<_>>(),
        [&manifest, &published]
    );

    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":40}\n{\"id\":2,\"amount\":50}\n",
    )
    .unwrap();
    let _successor = select_publication_fixture(
        &engine,
        root.path(),
        "next-release",
        selected.candidate.clone(),
    )
    .await;
    for control in spec["controls"].as_array().unwrap() {
        std::fs::remove_file(control["artifact"]["path"].as_str().unwrap()).unwrap();
    }
    let mut changed = spec.clone();
    changed["release_id"] = json!("b".repeat(64));
    assert_eq!(
        publication_command(&engine, &changed)
            .await
            .unwrap_err()
            .code(),
        "publication_intent_mismatch"
    );
    assert_publication_replay(&engine, &spec, &frozen).await;
    assert_publication_replay(&engine, &inspect, &frozen).await;

    let original: ManagedJobCommand = serde_json::from_slice(
        &STANDARD
            .decode(&selected.admitted_request.bytes_base64)
            .unwrap(),
    )
    .unwrap();
    let ManagedJobCommand::Prepare(original) = original else {
        unreachable!()
    };
    retire_producer_fixture_job(&engine, &original, true).await;
    engine.compact_lifecycle(&selected.dataset).await.unwrap();
    let targets = [published, private, manifest];
    let inspected = engine.inspect_retention(&targets).await.unwrap();
    for protected in inspected.objects {
        if protected.object == targets[1] {
            assert!(protected.blocker.is_none());
        } else {
            let blocker = protected.blocker.unwrap();
            assert_eq!(blocker.key, audit.reference.native_record.key);
            assert_eq!(blocker.revision, audit.reference.native_record.revision);
            assert!(Path::new(&protected.object.path).is_file());
        }
    }
    assert_publication_replay(&engine, &spec, &frozen).await;
    let plan = engine
        .plan_publication_intent_retirement(
            &selected.dataset,
            &Id::new("retire-release").unwrap(),
            &selected.operation,
            "end fixture delivery",
        )
        .await
        .unwrap();
    let mut wrong = plan.clone();
    wrong.records[0].revision += 1;
    assert_eq!(
        engine
            .retire_publication_intent(&wrong)
            .await
            .unwrap_err()
            .code(),
        "invalid_retirement"
    );
    let retired = engine.retire_publication_intent(&plan).await.unwrap();
    engine.compact_lifecycle(&selected.dataset).await.unwrap();
    assert_eq!(
        engine.retire_publication_intent(&plan).await.unwrap(),
        retired
    );
    let audit_only = publication_command(&engine, &inspect).await.unwrap();
    assert_eq!(audit_only.status, PublicationIntentStatus::Retired);
    assert_eq!(audit_only.intent.as_ref(), Some(audit));
    assert!(audit_only.payload.is_none());
    assert_publication_replay(&engine, &spec, &audit_only).await;
    assert!(
        !engine
            .inspect(&selected.dataset)
            .await
            .unwrap()
            .contains_key(&audit.reference.native_record.key)
    );
    assert!(
        engine
            .inspect_retention(&targets)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
}

async fn fill_publication_history(engine: &Engine, dataset: &Id, target: usize) {
    let lifecycle = &engine.inner.lifecycle;
    let mut view = lifecycle.open(dataset).await.unwrap();
    let key = "job-attempt/capacity-filler";
    let mut filler = Attempt {
        request_digest: "f".repeat(64),
        id: Id::new("padding").unwrap(),
        status: "failed".into(),
        error: Some(String::new()),
    };
    let base = crate::catalog::history_row_bytes(key, &serde_json::to_string(&filler).unwrap());
    loop {
        let rows = lifecycle.rows(&view).await.unwrap();
        let remaining = target.checked_sub(rows.history_bytes()).unwrap();
        if remaining < base {
            break;
        }
        // Leave room for one final complete row while keeping every batch small.
        let charge = if remaining <= (24 << 10) + base {
            remaining
        } else {
            24 << 10
        };
        filler.error = Some("p".repeat(charge - base));
        drop(rows);
        view = lifecycle
            .commit(&view, vec![record(key, &filler).unwrap()])
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn publication_intent_reserves_retirement_room_through_a_cold_open() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":20}\n",
    )
    .unwrap();
    let mut bounds = limits();
    bounds.max_metadata_bytes = 256 << 10;
    let host = root.path().join("host");
    let engine = Engine::open(host.clone(), bounds.clone()).await.unwrap();
    let selected = select_publication_fixture(&engine, root.path(), "near-cap", None).await;
    let mut spec = publication_freeze_spec(&engine, root.path(), &selected).await;
    spec["expected_pointer"] = json!({"kind":"absent"});
    fill_publication_history(
        &engine,
        &selected.dataset,
        bounds.max_metadata_bytes - (48 << 10),
    )
    .await;
    let frozen = publication_command(&engine, &spec).await.unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&selected.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let reserve = crate::engine::retention::publication::future_bytes(
        &rows.values,
        &view,
        view.revision,
        &lifecycle.budget,
    )
    .unwrap();
    assert!(reserve > 24 << 10 && reserve < 32 << 10);
    drop(rows);
    fill_publication_history(
        &engine,
        &selected.dataset,
        bounds.max_metadata_bytes - reserve,
    )
    .await;
    let view = lifecycle.open(&selected.dataset).await.unwrap();
    let ordinary = record("job-active/extra", Id::new("a".repeat(128)).unwrap()).unwrap();
    assert_eq!(
        lifecycle
            .commit(&view, vec![ordinary])
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    engine.shutdown().await.unwrap();
    drop(engine);

    let engine = Engine::open(host, bounds).await.unwrap();
    let inspect =
        json!({"action":"inspect","dataset":selected.dataset,"operation":selected.operation});
    assert_publication_replay(&engine, &inspect, &frozen).await;
    let plan = engine
        .plan_publication_intent_retirement(
            &selected.dataset,
            &Id::new("near-cap-retirement").unwrap(),
            &selected.operation,
            "end near-cap fixture delivery",
        )
        .await
        .unwrap();
    engine.retire_publication_intent(&plan).await.unwrap();
    engine.compact_lifecycle(&selected.dataset).await.unwrap();
    let after = publication_command(&engine, &inspect).await.unwrap();
    assert_eq!(after.status, PublicationIntentStatus::Retired);
    assert_eq!(after.intent, frozen.intent);
    assert!(after.payload.is_none());
}

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
        snapshot_inputs: Vec::new(),
        policy_id: Id::new("policy-one").unwrap(),
        policy,
        expected_epoch: 0,
        expected_selection: None,
        producer_adoption: None,
        producer_records: Vec::new(),
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

pub(super) fn producer_definition(name: &str) -> serde_json::Value {
    let pin = json!({"sha256":"a".repeat(64),"size":17});
    json!({
        "profile":"urn:docspec:whole-job-producer-definition:1",
        "definition":name,
        "repository":{"identity":"fixture-repository","head":"a".repeat(40)},
        "interpretation":{"model":pin,"compatibility_registry":pin},
        "proof":{"job_sha256":"1".repeat(64),"policy_id":"proof","policy_sha256":"2".repeat(64)},
        "model":{"job_sha256":"3".repeat(64),"policy_id":"model","policy_sha256":"4".repeat(64)},
        "model_file_inputs":[],
        "inputs":[{
            "name":"raw","selector":{"kind":"native_table","metadata_location":"file:///original/metadata.json",
                "table_uuid":"d9fdc74e-23c3-49a0-ae9c-dd71f9c0abb2","snapshot":31,"schema_id":0},
            "metadata":pin,"record_namespace":"fixture-records",
            "head_record":{"source":{"namespace":"fixture-records","key":"producer-31.json"},"sha256":"5".repeat(64),"size":29},
            "import_anchors":[],"daily_jobs":[],"import_jobs":[],"expected_versions":{"parser":"v1"}
        }],
        "producers":[{
            "input":"raw","commit":"b".repeat(40),"job":"jobs/build.py","job_blob":"c".repeat(40),
            "source_files":[{"path":"jobs/parse.py","blob":"d".repeat(40),"bytes":pin}],
            "algorithm":"fixture-version-1","versions":{"parser":"v1"}
        }]
    })
}

#[tokio::test]
async fn guarded_producer_adoption_replays_without_reopening_or_reselecting() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("input.jsonl"),
        b"{\"id\":1,\"amount\":10}\n{\"id\":2,\"amount\":null}\n",
    )
    .unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let original = request(
        root.path(),
        "prior-model",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    );
    let completed = engine.managed_job(original.clone()).await.unwrap();
    let accepted = engine
        .managed_job(action(&completed, "accept"))
        .await
        .unwrap();
    let selected = engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap();
    assert_eq!(selected.selection, "selected");
    let dataset = selected.dataset.clone();
    let mut pending = Vec::new();
    for operation in ["prior-unaccepted", "prior-accepted"] {
        let ManagedJobCommand::Prepare(mut prepared) = request(
            root.path(),
            operation,
            "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
            None,
        ) else {
            unreachable!()
        };
        prepared.expected_selection = selected.candidate.clone();
        pending.push(
            engine
                .managed_job(ManagedJobCommand::Prepare(prepared))
                .await
                .unwrap(),
        );
    }
    let prior_accepted = engine
        .managed_job(action(&pending[1], "accept"))
        .await
        .unwrap();
    let mut requalification_policy: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("policy-prior-accepted.json")).unwrap(),
    )
    .unwrap();
    requalification_policy["id"] = json!("new-policy");
    let new_policy = artifact(root.path().join("new-policy.json"), &requalification_policy);
    let before = engine
        .job_record::<Authority>(&dataset, "authority")
        .await
        .unwrap()
        .unwrap();
    let definition = producer_definition("adopted-v1");
    let first = JobProducerAdoption {
        dataset: dataset.clone(),
        adoption: Id::new("first-definition").unwrap(),
        reason: "Adopt the authored fixture interpretation".into(),
        expected_definition: before.definition.clone(),
        expected_epoch: before.epoch,
        expected_selection: selected.candidate.clone(),
        expected_adoption: None,
        definition: artifact(root.path().join("definition-first.json"), &definition),
    };
    let first_bytes = std::fs::read(&first.definition.path).unwrap();
    for field in ["definition", "epoch", "selection"] {
        let mut stale = first.clone();
        stale.definition.path = root.path().join("not-opened.json");
        match field {
            "definition" => stale.expected_definition = "wrong".into(),
            "epoch" => stale.expected_epoch += 1,
            "selection" => stale.expected_selection = None,
            _ => unreachable!(),
        }
        assert_eq!(
            engine
                .adopt_job_producer_definition(&stale)
                .await
                .unwrap_err()
                .code(),
            "stale_authority"
        );
    }
    *engine.inner.job_stop_after.lock().await =
        Some("job-producer-adoption/first-definition".into());
    assert_eq!(
        engine
            .adopt_job_producer_definition(&first)
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    std::fs::remove_file(&first.definition.path).unwrap();
    let first_ref = engine.adopt_job_producer_definition(&first).await.unwrap();
    assert_eq!(first_ref.definition_sha256, first.definition.sha256);
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    assert_eq!(
        first_ref.native_record.table_uuid,
        view.table.metadata().uuid().to_string()
    );
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (_, _, active): (_, _, serde_json::Value) =
        rows.get_record("job-producer-definition").unwrap().unwrap();
    assert_eq!(
        rows.revision("job-producer-definition"),
        Some(first_ref.native_record.revision)
    );
    assert_eq!(
        rows.revision("authority"),
        Some(first_ref.native_record.revision)
    );
    assert_eq!(
        STANDARD
            .decode(active["bytes_base64"].as_str().unwrap())
            .unwrap(),
        first_bytes
    );
    drop(rows);
    drop(view);
    drop(before);

    let mut second = first.clone();
    second.adoption = Id::new("second-definition").unwrap();
    second.expected_definition = "adopted-v1".into();
    let mut invalid = producer_definition("adopted-v2");
    invalid["model"]["job_sha256"] = invalid["proof"]["job_sha256"].clone();
    second.definition = artifact(root.path().join("definition-second.json"), &invalid);
    assert_eq!(
        engine
            .adopt_job_producer_definition(&second)
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
    second.expected_adoption = Some(first_ref.clone());
    assert_eq!(
        engine
            .adopt_job_producer_definition(&second)
            .await
            .unwrap_err()
            .code(),
        "invalid_producer_definition"
    );
    second.definition = artifact(
        root.path().join("definition-second.json"),
        &producer_definition("adopted-v2"),
    );
    let second_ref = engine.adopt_job_producer_definition(&second).await.unwrap();
    std::fs::remove_file(&second.definition.path).unwrap();
    engine.compact_lifecycle(&dataset).await.unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine.adopt_job_producer_definition(&first).await.unwrap(),
        first_ref
    );
    assert_eq!(
        engine.adopt_job_producer_definition(&second).await.unwrap(),
        second_ref
    );
    let mut collision = first;
    collision.reason = "Different decision under the same ID".into();
    assert_eq!(
        engine
            .adopt_job_producer_definition(&collision)
            .await
            .unwrap_err()
            .code(),
        "managed_job_identity"
    );
    let authority = engine
        .job_record::<Authority>(&dataset, "authority")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.definition, "adopted-v2");
    assert_eq!(authority.job_producer_adoption, Some(second_ref));
    assert_eq!(authority.job_selected, selected.candidate);
    assert_eq!(
        engine.managed_job(original).await.unwrap().candidate,
        selected.candidate
    );
    assert_eq!(
        engine
            .managed_job(action(&accepted, "select"))
            .await
            .unwrap()
            .candidate,
        selected.candidate
    );
    assert_eq!(
        engine
            .managed_job(action(&pending[0], "accept"))
            .await
            .unwrap_err()
            .code(),
        "unsupported_job_prerequisite"
    );
    assert_eq!(
        engine
            .managed_job(action(&prior_accepted, "select"))
            .await
            .unwrap_err()
            .code(),
        "unsupported_job_prerequisite"
    );
    let changed_policy = ManagedJobCommand::Requalify {
        dataset: dataset.clone(),
        operation: prior_accepted.operation.clone(),
        request_digest: prior_accepted.request_digest.clone(),
        candidate: prior_accepted.candidate.clone().unwrap(),
        previous_policy_id: Id::new("policy-one").unwrap(),
        policy_id: Id::new("new-policy").unwrap(),
        policy: new_policy,
        expected_epoch: 0,
        expected_selection: selected.candidate.clone(),
    };
    assert_eq!(
        engine.managed_job(changed_policy).await.unwrap_err().code(),
        "unsupported_job_prerequisite"
    );
    assert!(
        engine
            .job_record::<Decision>(&dataset, "job-policy/prior-accepted/new-policy")
            .await
            .unwrap()
            .is_none()
    );

    let ManagedJobCommand::Prepare(mut next) = request(
        root.path(),
        "not-admitted",
        "SELECT COUNT(*) - 2 AS invalid_rows FROM outputs.\"values\"",
        None,
    ) else {
        unreachable!()
    };
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(next.clone()))
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
    next.definition = authority.definition.clone();
    next.expected_epoch = authority.epoch;
    next.expected_selection = authority.job_selected.clone();
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(next))
            .await
            .unwrap_err()
            .code(),
        "unsupported_job_prerequisite"
    );
    assert!(
        engine
            .job_record::<Admission>(&dataset, "job-operation/not-admitted")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cold_lifecycle_reader_refuses_one_oversized_record() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Id::new("oversized-lifecycle").unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&dataset).await.unwrap();
    let original = "q".repeat(16 << 10);
    let written = lifecycle
        .commit(&view, vec![record("diagnostic", &original).unwrap()])
        .await
        .unwrap();
    let warm = lifecycle.rows(&written).await.unwrap();
    assert_eq!(
        warm.get_record::<String>("diagnostic").unwrap().unwrap().2,
        original
    );
    drop(warm);
    engine.shutdown().await.unwrap();
    drop(engine);

    let smaller = Limits {
        max_batch_bytes: 8 << 10,
        ..limits()
    };
    let reopened = Engine::open(root.path(), smaller).await.unwrap();
    let view = reopened.inner.lifecycle.open(&dataset).await.unwrap();
    let error = reopened.inner.lifecycle.rows(&view).await.err().unwrap();
    assert_eq!(error.code(), "oversized_batch");
}

async fn snapshot_request(root: &Path, operation: &str) -> ManagedJobCommand {
    let native_schema = iceberg::spec::Schema::builder()
        .with_fields([
            Arc::new(iceberg::spec::NestedField::required(
                1,
                "occurrence",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
            Arc::new(iceberg::spec::NestedField::required(
                2,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
            Arc::new(iceberg::spec::NestedField::optional(
                3,
                "amount",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
        ])
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    let producer: JobSpec = serde_json::from_value(json!({
        "scratch":root.join("producer/scratch"), "catalog_root":root.join("producer/catalog"), "inputs":[],
        "steps":[{"name":"incoming","sql":"SELECT CAST(occurrence AS BIGINT) AS occurrence,CAST(id AS BIGINT) AS id,CAST(amount AS BIGINT) AS amount FROM (VALUES (1,1,7),(2,1,7),(3,2,NULL)) AS t(occurrence,id,amount)"}],
        "outputs":[], "table_operations":[
            {"kind":"create_namespace","name":"namespace","namespace":["archive"],"properties":{}},
            {"kind":"create","name":"create","table":{"namespace":["archive"],"name":"original"},"location":root.join("producer/data"),"schema":native_schema,"format_version":2,"properties":{"gc.enabled":"true"}},
            {"kind":"append","name":"append","table":{"namespace":["archive"],"name":"original"},"current_table":"old","work_table":"pending","sql":"SELECT * FROM incoming","identity_sql":"SELECT 'authored-three-occurrences' AS identity FROM pending LIMIT 1","layout":{"file_bytes":1048576,"row_group_rows":64,"row_group_bytes":1048576}}
        ]
    })).unwrap();
    let produced = producer.run(limits()).await.unwrap();
    let head = &produced.table_operations[2];
    let input = json!({"kind":"native_table","name":"raw","metadata_location":head.metadata_location,"table_uuid":head.table_uuid,"snapshot":head.snapshot,"schema_id":head.schema_id});
    let report = "SELECT COUNT(*) - 3 AS invalid_rows FROM raw";
    let job = artifact(
        root.join(format!("snapshot-job-{operation}.json")),
        &json!({
            "scratch":root.join("caller-scratch"), "inputs":[input], "steps":[], "outputs":[],
            "queries":[{"name":"invariants","sql":report},{"name":"values","sql":"SELECT id,amount FROM raw ORDER BY occurrence"}]
        }),
    );
    let schema = Schema::new(vec![Field::new("invalid_rows", DataType::Int64, false)]);
    let schema =
        crate::job::retained::encode_schema(&schema, &Budget::new(limits()).unwrap()).unwrap();
    let policy = artifact(
        root.join(format!("snapshot-policy-{operation}.json")),
        &json!({
            "id":"policy-one", "profile":COUNTERS,
            "evaluator":{"id":"urn:docspec:evaluator:native-job-reports","version":"1"},
            "semantics":{"id":"urn:docspec:semantics:complete-integer-zero-row","revision":"1"},
            "job_sha256":job.sha256,"view_names":{"inputs":["raw"],"steps":[],"outputs":[],"reports":["invariants","values"]},
            "required_reports":[{"name":"invariants","sql_sha256":sha256(report.as_bytes()),"schema_ipc_base64":STANDARD.encode(&schema.bytes)}]
        }),
    );
    let metadata = std::fs::read(head.metadata_location.as_ref().unwrap()).unwrap();
    ManagedJobCommand::Prepare(ManagedJobPrepare {
        dataset: Id::new("native-job").unwrap(),
        operation: Id::new(operation).unwrap(),
        definition: "native-snapshot-fixture-v1".into(),
        job,
        file_inputs: Vec::new(),
        snapshot_inputs: vec![JobSnapshotInput {
            name: "raw".into(),
            metadata_sha256: sha256(&metadata),
            metadata_size: metadata.len() as u64,
            reuse_from: None,
        }],
        policy_id: Id::new("policy-one").unwrap(),
        policy,
        expected_epoch: 0,
        expected_selection: None,
        producer_adoption: None,
        producer_records: Vec::new(),
        prerequisites: Vec::new(),
    })
}

struct ProducerFixture {
    adoption_request: JobProducerAdoption,
    definition: serde_json::Value,
    proof: ManagedJobPrepare,
    model: ManagedJobPrepare,
}

async fn producer_fixture(root: &Path, engine: &Engine) -> ProducerFixture {
    let ManagedJobCommand::Prepare(mut proof) = snapshot_request(root, "producer-proof").await
    else {
        unreachable!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&proof.job.path).unwrap()).unwrap();
    let mut selector = job["inputs"][0].clone();
    selector.as_object_mut().unwrap().remove("name");
    let original = artifact(
        root.join("original-producer-record.json"),
        &json!({
            "version":1,"input":"raw","selector":selector,
            "job":"jobs/build.py","commit":"b".repeat(40),"lockfile_digest":"e".repeat(64)
        }),
    );
    let source = JobProducerRecordLocation {
        namespace: "fixture-records".into(),
        key: "head.json".into(),
    };
    let size = std::fs::metadata(&original.path).unwrap().len();
    proof.producer_records = vec![JobProducerRecord {
        source: source.clone(),
        sha256: original.sha256.clone(),
        size,
        path: original.path,
    }];
    let mut model = proof.clone();
    model.operation = Id::new("producer-model").unwrap();
    model.producer_records.clear();
    job["queries"][1]["sql"] = json!("SELECT id,amount FROM raw ORDER BY occurrence DESC");
    model.job = artifact(root.join("model-job.json"), &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&proof.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(model.job.sha256);
    model.policy = artifact(root.join("model-policy.json"), &policy);
    let mut definition = producer_definition(&proof.definition);
    definition["inputs"][0]["selector"] = selector;
    definition["inputs"][0]["metadata"] = json!({
        "sha256":proof.snapshot_inputs[0].metadata_sha256,
        "size":proof.snapshot_inputs[0].metadata_size
    });
    definition["inputs"][0]["head_record"] =
        json!({"source":source,"sha256":original.sha256,"size":size});
    definition["proof"] = json!({
        "job_sha256":proof.job.sha256,"policy_id":proof.policy_id,"policy_sha256":proof.policy.sha256
    });
    definition["model"] = json!({
        "job_sha256":model.job.sha256,"policy_id":model.policy_id,"policy_sha256":model.policy.sha256
    });
    let adoption_request = JobProducerAdoption {
        dataset: proof.dataset.clone(),
        adoption: Id::new("producer-definition").unwrap(),
        reason: "adopt fixture's original producer definition".into(),
        expected_definition: String::new(),
        expected_epoch: 0,
        expected_selection: None,
        expected_adoption: None,
        definition: artifact(root.join("adopted-producer-definition.json"), &definition),
    };
    let adoption = engine
        .adopt_job_producer_definition(&adoption_request)
        .await
        .unwrap();
    proof.producer_adoption = Some(adoption.clone());
    model.producer_adoption = Some(adoption);
    ProducerFixture {
        adoption_request,
        definition,
        proof,
        model,
    }
}

#[tokio::test]
async fn producer_proof_preseal_retry_keeps_original_definition_after_adoption_moves() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let mut fixture = producer_fixture(root.path(), &engine).await;
    *engine.inner.job_stop_after.lock().await = Some("job-grant/producer-proof".into());
    let request = ManagedJobCommand::Prepare(fixture.proof.clone());
    assert_eq!(
        engine
            .managed_job(request.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert!(
        engine
            .job_record::<serde_json::Value>(
                &fixture.proof.dataset,
                "job-producer-static/producer-proof"
            )
            .await
            .unwrap()
            .is_none()
    );
    fixture.definition["definition"] = json!("successor-interpretation");
    let mut next = fixture.adoption_request.clone();
    next.adoption = Id::new("successor-adoption").unwrap();
    next.expected_definition = fixture.proof.definition.clone();
    next.expected_adoption = fixture.proof.producer_adoption.clone();
    next.definition = artifact(
        root.path().join("successor-definition.json"),
        &fixture.definition,
    );
    engine.adopt_job_producer_definition(&next).await.unwrap();
    for path in [
        &fixture.adoption_request.definition.path,
        &fixture.proof.job.path,
        &fixture.proof.policy.path,
        &fixture.proof.producer_records[0].path,
    ] {
        std::fs::remove_file(path).unwrap();
    }
    drop(engine);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let completed = engine.managed_job(request).await.unwrap();
    assert_eq!(
        completed.completed_reports[0]["data"],
        json!([{"invalid_rows":0}])
    );
    let manifest = witness_bytes(
        completed.completion_manifest.as_ref().unwrap(),
        &engine.inner.lifecycle.budget,
    )
    .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest.value).unwrap();
    assert_eq!(
        manifest["producer_evidence"]["adoption"],
        serde_json::to_value(&fixture.proof.producer_adoption).unwrap()
    );
    assert_eq!(
        engine
            .managed_job(action(&completed, "accept"))
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
}

async fn retire_producer_fixture_job(
    engine: &Engine,
    request: &ManagedJobPrepare,
    availability: bool,
) {
    let records = engine.inspect(&request.dataset).await.unwrap();
    let input_prefix = format!("job-input/{}/", request.operation);
    let keys = records
        .iter()
        .filter(|(key, value)| {
            super::super::retention::retirement::completion_key(key)
                && (key.starts_with(&input_prefix)
                    || value["operation"] == request.operation.as_str())
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    if !keys.is_empty() {
        let phases = engine
            .plan_phase_retirement(
                &request.dataset,
                &Id::new(format!("{}-phases", request.operation)).unwrap(),
                &keys,
                "end fixture phase recovery",
            )
            .await
            .unwrap();
        engine.retire_phases(&phases).await.unwrap();
    }
    let recovery = engine
        .plan_operation_retirement(
            &request.dataset,
            &Id::new(format!("{}-recovery", request.operation)).unwrap(),
            &request.operation,
            "end fixture operation recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    if availability {
        retire_producer_fixture_result(engine, request).await;
    }
}

async fn retire_producer_fixture_result(engine: &Engine, request: &ManagedJobPrepare) {
    let plan = engine
        .plan_availability_retirement(
            &request.dataset,
            &Id::new(format!("{}-availability", request.operation)).unwrap(),
            &request.operation,
            "end fixture result availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&plan).await.unwrap();
}

#[tokio::test]
async fn producer_model_grant_survives_donor_retirement_before_model_work() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let mut fixture = producer_fixture(root.path(), &engine).await;
    let proof = engine
        .managed_job(ManagedJobCommand::Prepare(fixture.proof.clone()))
        .await
        .unwrap();
    let proof = engine.managed_job(action(&proof, "accept")).await.unwrap();
    fixture.model.snapshot_inputs[0].reuse_from = Some(proof.input_custody[0].clone());
    fixture.model.prerequisites = vec![JobPrerequisite {
        profile: "urn:docspec:whole-job-producer-prerequisite:1".into(),
        candidate: proof.candidate.clone().unwrap(),
        accepted_qualification: proof.accepted_qualification.as_ref().unwrap()["identity"]
            .as_str()
            .unwrap()
            .into(),
        inputs: vec!["raw".into()],
    }];
    let request = ManagedJobCommand::Prepare(fixture.model.clone());
    *engine.inner.job_stop_after.lock().await = Some("job-grant/producer-model".into());
    assert_eq!(
        engine
            .managed_job(request.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let rows = engine.inspect(&fixture.model.dataset).await.unwrap();
    assert!(!rows.contains_key("job-input/producer-model/0"));
    let admitted: Admission =
        serde_json::from_value(rows["job-operation/producer-model"].clone()).unwrap();
    let retained = producer::inherited_objects(&admitted).to_vec();
    assert!(
        retained
            .iter()
            .any(|object| object.path.ends_with("/producer-static.json"))
    );
    assert!(retained.contains(&proof.candidate.as_ref().unwrap().manifest));
    retire_producer_fixture_job(&engine, &fixture.proof, true).await;
    assert!(
        engine
            .inspect_retention(&retained)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    drop(engine);
    std::fs::remove_dir_all(root.path().join("producer")).unwrap();
    for path in [
        &fixture.adoption_request.definition.path,
        &fixture.proof.producer_records[0].path,
        &fixture.proof.job.path,
        &fixture.proof.policy.path,
        &fixture.model.job.path,
        &fixture.model.policy.path,
    ] {
        std::fs::remove_file(path).unwrap();
    }
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let model = engine.managed_job(request).await.unwrap();
    assert_eq!(
        model.completed_reports[1]["data"],
        json!([{"id":2,"amount":null},{"id":1,"amount":7},{"id":1,"amount":7}])
    );
    engine.managed_job(action(&model, "accept")).await.unwrap();
    assert!(
        !std::fs::read_dir(&admitted.directory)
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("snapshot-"))
    );
    retire_producer_fixture_job(&engine, &fixture.model, false).await;
    assert!(
        engine
            .inspect_retention(&retained)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    retire_producer_fixture_result(&engine, &fixture.model).await;
    assert!(
        engine
            .inspect_retention(&retained)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
}

#[tokio::test]
async fn selected_snapshot_custody_preserves_values_and_cold_original_removal() {
    let root = tempfile::tempdir().unwrap();
    let command = snapshot_request(root.path(), "snapshot-cold").await;
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let complete = engine.managed_job(command.clone()).await.unwrap();
    assert_eq!(complete.status, "completed");
    assert_eq!(
        complete.completed_reports[1]["data"],
        json!([{"id":1,"amount":7},{"id":1,"amount":7},{"id":2,"amount":null}])
    );
    let witness = complete.completion_manifest.as_ref().unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&STANDARD.decode(&witness.bytes_base64).unwrap()).unwrap();
    assert_eq!(manifest["profile"], SNAPSHOT_PROFILE);
    let input = &manifest["inputs"][0];
    assert_eq!(input["files"], json!([]));
    let custody = &input["snapshot_custody"];
    assert_eq!(custody["scope"], "selected_snapshot");
    assert!(
        custody["objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|object| object["role"] == "data")
    );
    let schema_bytes = STANDARD
        .decode(
            manifest["schemas"][input["schema_sha256"].as_str().unwrap()]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    let observed = try_schema_from_ipc_buffer(&schema_bytes).unwrap();
    assert_eq!(
        observed.field_with_name("id").unwrap().metadata()["PARQUET:field_id"],
        "2"
    );
    assert_eq!(
        observed.field_with_name("amount").unwrap().metadata()["PARQUET:field_id"],
        "3"
    );
    let accepted = engine
        .managed_job(action(&complete, "accept"))
        .await
        .unwrap();
    drop(engine);
    std::fs::remove_dir_all(root.path().join("producer")).unwrap();
    if let ManagedJobCommand::Prepare(request) = &command {
        std::fs::remove_file(&request.job.path).unwrap();
        std::fs::remove_file(&request.policy.path).unwrap();
    }
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let retried = engine.managed_job(command).await.unwrap();
    assert_eq!(retried.candidate, complete.candidate);
    assert_eq!(retried.completed_reports, complete.completed_reports);
    let selected = engine
        .managed_job(action(&accepted, "select"))
        .await
        .unwrap();
    assert_eq!(selected.selection, "selected");
}

#[tokio::test]
async fn selected_snapshot_custody_reopens_payload_after_input_seal_and_caller_loss() {
    let root = tempfile::tempdir().unwrap();
    let command = snapshot_request(root.path(), "snapshot-pending").await;
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    *engine.inner.job_stop_after.lock().await = Some("job-input/snapshot-pending/0".into());
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine
        .inspect(&Id::new("native-job").unwrap())
        .await
        .unwrap();
    assert!(!before.contains_key("job-result/snapshot-pending"));
    let members = before
        .iter()
        .filter(|(key, _)| key.starts_with("job-snapshot-member/"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(!members.is_empty());
    drop(engine);
    std::fs::remove_dir_all(root.path().join("producer")).unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let complete = engine.managed_job(command).await.unwrap();
    assert_eq!(
        complete.completed_reports[1]["data"],
        json!([{"id":1,"amount":7},{"id":1,"amount":7},{"id":2,"amount":null}])
    );
    let after = engine
        .inspect(&Id::new("native-job").unwrap())
        .await
        .unwrap();
    for (key, value) in members {
        assert_eq!(
            after[&key], value,
            "sealed native members are reused without original opens or copies"
        );
    }
}

#[tokio::test]
async fn selected_snapshot_metadata_seals_survive_a_later_missing_manifest() {
    let root = tempfile::tempdir().unwrap();
    let command = snapshot_request(root.path(), "snapshot-manifest").await;
    let ManagedJobCommand::Prepare(request) = &command else {
        panic!()
    };
    let spec: JobSpec = serde_json::from_slice(&std::fs::read(&request.job.path).unwrap()).unwrap();
    let selected = snapshots::selection(&spec.inputs[0]).unwrap();
    let table = crate::storage::open_snapshot_with_io(
        &selected,
        iceberg::io::FileIO::new_with_fs(),
        &Budget::new(limits()).unwrap(),
    )
    .await
    .unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let list_path = snapshot.manifest_list().to_owned();
    let list = table.manifest_list_reader(snapshot).load().await.unwrap();
    let manifest = list.entries()[0].manifest_path.clone();
    drop(table);
    let backup = root.path().join("manifest-backup");
    std::fs::rename(&manifest, &backup).unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    assert!(engine.managed_job(command.clone()).await.is_err());
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert!(
        !before
            .keys()
            .any(|key| key.starts_with("job-snapshot-plan/"))
    );
    assert_eq!(
        before
            .keys()
            .filter(|key| key.starts_with("job-snapshot-member/"))
            .count(),
        2
    );
    drop(engine);
    std::fs::rename(&backup, &manifest).unwrap();
    std::fs::remove_file(&selected.metadata_location).unwrap();
    std::fs::remove_file(&list_path).unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let complete = engine.managed_job(command.clone()).await.unwrap();
    assert_eq!(
        complete.completed_reports[1]["data"],
        json!([{"id":1,"amount":7},{"id":1,"amount":7},{"id":2,"amount":null}])
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in before
        .iter()
        .filter(|(key, _)| key.starts_with("job-snapshot-member/"))
    {
        assert_eq!(&after[key], value);
    }
}

#[tokio::test]
async fn selected_snapshot_custody_roots_end_at_existing_phase_and_operation_retirement() {
    let root = tempfile::tempdir().unwrap();
    let command = snapshot_request(root.path(), "snapshot-retire").await;
    let ManagedJobCommand::Prepare(request) = &command else {
        panic!()
    };
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    *engine.inner.job_stop_after.lock().await = Some("job-input/snapshot-retire/0".into());
    assert_eq!(
        engine
            .managed_job(command.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&request.dataset).await.unwrap();
    let members = records
        .iter()
        .filter(|(key, _)| key.starts_with("job-snapshot-member/"))
        .map(|(_, value)| {
            serde_json::from_value::<ObjectRef>(value["object"]["retained"].clone()).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .inspect_retention(&members)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    engine
        .abandon(
            &request.dataset,
            &request.operation,
            "end deliberately interrupted snapshot fixture",
        )
        .await
        .unwrap();
    let keys = records
        .keys()
        .filter(|key| super::super::retention::retirement::completion_key(key))
        .cloned()
        .collect::<Vec<_>>();
    assert!(keys.iter().any(|key| key.starts_with("job-snapshot-plan/")));
    assert!(
        keys.iter()
            .any(|key| key.starts_with("job-snapshot-member/"))
    );
    let phases = engine
        .plan_phase_retirement(
            &request.dataset,
            &Id::new("snapshot-phases").unwrap(),
            &keys,
            "end input phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let operation = engine
        .plan_operation_retirement(
            &request.dataset,
            &Id::new("snapshot-operation").unwrap(),
            &request.operation,
            "end abandoned operation recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&operation).await.unwrap();
    assert!(
        engine
            .inspect_retention(&members)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
    assert_eq!(
        engine.managed_job(command).await.unwrap_err().code(),
        "operation_abandoned"
    );
}

#[tokio::test]
async fn selected_snapshot_custody_refuses_wrong_native_pins_and_original_metadata_bytes() {
    let root = tempfile::tempdir().unwrap();
    let base = snapshot_request(root.path(), "pin-source").await;
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    for (index, field, value) in [
        (
            0,
            "table_uuid",
            json!("00000000-0000-4000-8000-000000000099"),
        ),
        (1, "schema_id", json!(999)),
        (2, "snapshot", serde_json::Value::Null),
    ] {
        let ManagedJobCommand::Prepare(mut request) = base.clone() else {
            panic!()
        };
        request.operation = Id::new(format!("wrong-pin-{index}")).unwrap();
        let mut job: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&request.job.path).unwrap()).unwrap();
        job["inputs"][0][field] = value;
        request.job = artifact(root.path().join(format!("wrong-job-{index}.json")), &job);
        let mut policy: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&request.policy.path).unwrap()).unwrap();
        policy["job_sha256"] = json!(request.job.sha256);
        request.policy = artifact(
            root.path().join(format!("wrong-policy-{index}.json")),
            &policy,
        );
        assert_eq!(
            engine
                .managed_job(ManagedJobCommand::Prepare(request))
                .await
                .unwrap_err()
                .code(),
            "pin_mismatch"
        );
    }
    let ManagedJobCommand::Prepare(mut request) = base else {
        panic!()
    };
    request.operation = Id::new("wrong-metadata").unwrap();
    request.snapshot_inputs[0].metadata_sha256 = "0".repeat(64);
    assert_eq!(
        engine
            .managed_job(ManagedJobCommand::Prepare(request))
            .await
            .unwrap_err()
            .code(),
        mismatch().code()
    );
}

#[tokio::test]
async fn input_custody_grant_survives_donor_retirement_before_input_seal() {
    let root = tempfile::tempdir().unwrap();
    let original = snapshot_request(root.path(), "custody-donor").await;
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let complete = engine.managed_job(original.clone()).await.unwrap();
    let reference = complete.input_custody[0].clone();
    assert_eq!(reference.operation, Id::new("custody-donor").unwrap());
    let ManagedJobCommand::Prepare(mut receiving) = original else {
        panic!()
    };
    receiving.operation = Id::new("custody-receiver").unwrap();
    receiving.snapshot_inputs[0].reuse_from = Some(reference.clone());
    *engine.inner.job_stop_after.lock().await = Some("job-grant/custody-receiver".into());
    let request = ManagedJobCommand::Prepare(receiving.clone());
    assert_eq!(
        engine
            .managed_job(request.clone())
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&receiving.dataset).await.unwrap();
    assert!(!records.contains_key("job-input/custody-receiver/0"));
    let admitted: Admission =
        serde_json::from_value(records["job-operation/custody-receiver"].clone()).unwrap();
    let source_objects = input_custody::input_objects(&admitted.input_grants["raw"]);
    let keys = records
        .keys()
        .filter(|key| super::super::retention::retirement::completion_key(key))
        .cloned()
        .collect::<Vec<_>>();
    let phases = engine
        .plan_phase_retirement(
            &receiving.dataset,
            &Id::new("donor-phases").unwrap(),
            &keys,
            "end donor phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let availability = engine
        .plan_availability_retirement(
            &receiving.dataset,
            &Id::new("donor-availability").unwrap(),
            &reference.operation,
            "end donor result availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &receiving.dataset,
            &Id::new("donor-recovery").unwrap(),
            &reference.operation,
            "end donor operation recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    assert!(
        engine
            .inspect_retention(&source_objects)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    drop(engine);
    std::fs::remove_dir_all(root.path().join("producer")).unwrap();
    std::fs::remove_file(&receiving.job.path).unwrap();
    std::fs::remove_file(&receiving.policy.path).unwrap();
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let finished = engine.managed_job(request).await.unwrap();
    assert_eq!(
        finished.completed_reports[1]["data"],
        json!([{"id":1,"amount":7},{"id":1,"amount":7},{"id":2,"amount":null}])
    );
    assert_eq!(finished.input_custody[0].operation, receiving.operation);
    let manifest: serde_json::Value = serde_json::from_slice(
        &STANDARD
            .decode(&finished.completion_manifest.as_ref().unwrap().bytes_base64)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["inputs"][0]["custody_grant"]["source"],
        serde_json::to_value(reference).unwrap()
    );
    assert_eq!(
        manifest["inputs"][0]["snapshot_custody"]["objects"],
        serde_json::to_value(
            admitted.input_grants["raw"]
                .snapshot_custody
                .as_ref()
                .unwrap()
                .objects
                .clone()
        )
        .unwrap()
    );
    assert!(
        !std::fs::read_dir(&admitted.directory)
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("snapshot-")),
        "receiving operation must not copy raw snapshot objects"
    );
    let records = engine.inspect(&receiving.dataset).await.unwrap();
    let keys = records
        .iter()
        .filter(|(key, value)| {
            super::super::retention::retirement::completion_key(key)
                && (key.starts_with("job-input/custody-receiver/")
                    || value
                        .get("operation")
                        .is_some_and(|value| value == "custody-receiver"))
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let phases = engine
        .plan_phase_retirement(
            &receiving.dataset,
            &Id::new("receiver-phases").unwrap(),
            &keys,
            "end receiving phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &receiving.dataset,
            &Id::new("receiver-recovery").unwrap(),
            &receiving.operation,
            "end receiving operation recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    assert!(
        engine
            .inspect_retention(&source_objects)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some()),
        "result availability independently owns data and original provenance"
    );
    let availability = engine
        .plan_availability_retirement(
            &receiving.dataset,
            &Id::new("receiver-availability").unwrap(),
            &receiving.operation,
            "end receiving result availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    assert!(
        engine
            .inspect_retention(&source_objects)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
}

#[tokio::test]
async fn input_custody_reference_refuses_changed_incarnation_and_admission_digest() {
    let root = tempfile::tempdir().unwrap();
    let original = snapshot_request(root.path(), "reference-donor").await;
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let complete = engine.managed_job(original.clone()).await.unwrap();
    for index in 0..3 {
        let ManagedJobCommand::Prepare(mut request) = original.clone() else {
            panic!()
        };
        request.operation = Id::new(format!("reference-refusal-{index}")).unwrap();
        let mut source = complete.input_custody[0].clone();
        match index {
            0 => source.native_record.table_uuid = "00000000-0000-4000-8000-000000000099".into(),
            1 => source.admission_digest.value = "0".repeat(64),
            _ => source.input = "absent".into(),
        }
        request.snapshot_inputs[0].reuse_from = Some(source);
        assert_eq!(
            engine
                .managed_job(ManagedJobCommand::Prepare(request))
                .await
                .unwrap_err()
                .code(),
            mismatch().code()
        );
    }
}

#[tokio::test]
async fn input_custody_rule_change_reuses_native_material_without_raw_copies() {
    let root = tempfile::tempdir().unwrap();
    let ManagedJobCommand::Prepare(mut source) =
        snapshot_request(root.path(), "material-donor").await
    else {
        panic!()
    };
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&source.job.path).unwrap()).unwrap();
    job["steps"] = json!([{"name":"material","sql":"SELECT * FROM raw","materialize":true}]);
    source.job = artifact(root.path().join("material-job-original.json"), &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&source.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(source.job.sha256);
    policy["view_names"]["steps"] = json!(["material"]);
    source.policy = artifact(root.path().join("material-policy-original.json"), &policy);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let original = engine
        .managed_job(ManagedJobCommand::Prepare(source.clone()))
        .await
        .unwrap();
    let old_phase = engine
        .job_record::<Phase>(&source.dataset, "job-phase/material-donor/step/0")
        .await
        .unwrap()
        .unwrap();
    let mut receiving = source;
    receiving.operation = Id::new("material-receiver").unwrap();
    receiving.snapshot_inputs[0].reuse_from = Some(original.input_custody[0].clone());
    job["queries"][1]["sql"] = json!("SELECT id,amount FROM material ORDER BY occurrence");
    receiving.job = artifact(root.path().join("material-job-new-report.json"), &job);
    policy["job_sha256"] = json!(receiving.job.sha256);
    receiving.policy = artifact(root.path().join("material-policy-new-report.json"), &policy);
    std::fs::remove_dir_all(root.path().join("producer")).unwrap();
    let current = engine
        .managed_job(ManagedJobCommand::Prepare(receiving.clone()))
        .await
        .unwrap();
    assert_eq!(
        current.completed_reports[1]["data"],
        json!([{"id":1,"amount":7},{"id":1,"amount":7},{"id":2,"amount":null}])
    );
    let new_phase = engine
        .job_record::<Phase>(&receiving.dataset, "job-phase/material-receiver/step/0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        new_phase.object, old_phase.object,
        "different custody references and report SQL preserve native material reuse"
    );
    assert_eq!(new_phase.material_identity, old_phase.material_identity);
    assert_eq!(new_phase.origin_runtime, old_phase.origin_runtime);
    let directory = root
        .path()
        .join("host/objects/native-job/job-material-receiver");
    assert!(!std::fs::read_dir(directory).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("snapshot-")
    }));
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
