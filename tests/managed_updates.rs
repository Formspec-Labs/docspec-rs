//! Managed updates preserve based replacement semantics and native quality guards.
// Rust guideline compliant 2026-02-21

use std::{collections::BTreeMap, fs::File, io::Cursor, path::Path, sync::Arc};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    engine::ManagedBuildCommand,
    job::{JobInput, JobSpec, SqlQuery},
    model::{Id, InputSpec, ReleasePin, RunSpec, UpdateSpec, VersionRef},
    storage,
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 32 << 20,
        max_metadata_bytes: 4 << 20,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
    }
}

fn input(path: &Path, rows: &[(&str, Option<i64>)], keys_only: bool) -> InputSpec {
    let mut fields = vec![Field::new("id", DataType::Utf8, true)];
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(StringArray::from(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    ))];
    if !keys_only {
        fields.push(Field::new("amount_cents", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

async fn fixture(root: &Path) -> (Engine, VersionRef) {
    let source = input(
        &root.join("base.parquet"),
        &[("A", Some(100)), ("B", Some(200)), ("D", Some(50))],
        false,
    );
    let engine = Engine::open(root.join("host"), limits()).await.unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: id("amounts"),
            operation: id("initial"),
            base: None,
            relationship: None,
            definition: "v1".into(),
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    (engine, base.version_ref())
}

fn policy(base: &VersionRef) -> Value {
    json!({
        "id": "quality-v1", "rules_sha256": "a".repeat(64),
        "evaluator": {
            "id": "urn:ddk:quality:datacontract-cli", "version": "1.2.9",
            "distribution_sha256": "b".repeat(64),
        },
        "semantics": {"id": "urn:ddk:quality:duckdb-direct-view", "revision": "1"},
        "required_checks": ["amounts.nonnegative"],
        "expected_epoch": 0, "expected_selection": base,
    })
}

async fn managed(engine: &Engine, request: Value) -> docspec::Result<Value> {
    let command: ManagedBuildCommand = serde_json::from_value(request).unwrap();
    let receipt = engine.managed_build(command).await?;
    Ok(serde_json::to_value(receipt).unwrap())
}

async fn rows(engine: &Engine, version: &VersionRef) -> BTreeMap<String, Option<i64>> {
    let mut bytes = Vec::new();
    engine
        .write_source_version(version, &mut bytes)
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for batch in StreamReader::try_new(Cursor::new(bytes), None).unwrap() {
        let batch = batch.unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            let old = rows.insert(
                keys.value(index).into(),
                (!amounts.is_null(index)).then(|| amounts.value(index)),
            );
            assert_eq!(old, None, "duplicate key in native candidate");
        }
    }
    rows
}

#[tokio::test]
async fn protected_version_observes_pending_and_compacted_source_without_payload() {
    let root = TempDir::new().unwrap();
    let (engine, selected) = fixture(root.path()).await;
    let source = input(&root.path().join("import.parquet"), &[("I", None)], false);
    let build = RunSpec {
        dataset: id("imports"),
        operation: id("import"),
        base: None,
        relationship: None,
        definition: "v1".into(),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        partition_by: Vec::new(),
    };
    let mut policy = policy(&selected);
    policy["expected_selection"] = Value::Null;
    let prepared = managed(
        &engine,
        json!({"action":"prepare", "build":build, "policy":policy}),
    )
    .await
    .unwrap();
    let pending: VersionRef = serde_json::from_value(prepared["candidate"].clone()).unwrap();
    assert_eq!(prepared["selection"], "pending");
    assert_eq!(
        rows(&engine, &pending).await,
        BTreeMap::from([("I".into(), None)])
    );
    let records = engine.inspect(&pending.dataset).await.unwrap();
    let candidate: ReleasePin = serde_json::from_value(records["readback/import"].clone()).unwrap();
    let pending_pin = &candidate.dataset.groups["source"];
    let selected_release = engine
        .release(&selected.dataset, &selected.version)
        .await
        .unwrap();
    let logical_pin = &selected_release.dataset.groups["source"];
    let compaction = engine
        .compact(&super::compaction::request(&selected_release, "rewrite"))
        .await
        .unwrap();
    let replacement = &compaction.replacement;
    assert_ne!(replacement.metadata, logical_pin.metadata);
    assert_eq!(
        engine
            .latest(&selected.dataset)
            .await
            .unwrap()
            .version_ref(),
        selected
    );
    drop(engine);

    let data = pending_pin
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let hidden = root.path().join("hidden.parquet");
    std::fs::rename(&data.path, &hidden).unwrap();
    let workspace = root.path().join("host");
    let mut job = JobSpec {
        scratch: root.path().join("scratch"),
        catalog_root: None,
        table_operations: Vec::new(),
        inputs: vec![
            JobInput::Version {
                name: "pending".into(),
                workspace: workspace.clone(),
                version: pending.clone(),
            },
            JobInput::Version {
                name: "selected".into(),
                workspace,
                version: selected.clone(),
            },
        ],
        steps: Vec::new(),
        outputs: Vec::new(),
        queries: Vec::new(),
    };
    let result = job.run(limits()).await.unwrap();
    assert_eq!(result.native_versions.len(), 2);
    for (observation, (name, version, canonical_schema, pin)) in
        result.native_versions.iter().zip([
            ("pending", &pending, pending_pin.schema_id, pending_pin),
            ("selected", &selected, logical_pin.schema_id, replacement),
        ])
    {
        let bytes = std::fs::read(&pin.metadata.path).unwrap();
        let native: Value = serde_json::from_slice(&bytes).unwrap();
        let physical = observation.physical_snapshot.as_ref().unwrap();
        assert_eq!(observation.name, name);
        assert_eq!(&observation.version, version);
        assert_eq!(observation.schema_id, canonical_schema);
        assert_eq!(physical.selector.metadata_location, pin.metadata.path);
        assert_eq!(json!(physical.selector.table_uuid), native["table-uuid"]);
        assert_eq!(
            json!(physical.selector.snapshot),
            native["current-snapshot-id"]
        );
        assert_eq!(
            json!(physical.selector.schema_id),
            native["current-schema-id"]
        );
        assert_eq!(physical.metadata_size, bytes.len() as u64);
        assert_eq!(
            physical.metadata_sha256,
            format!("{:x}", Sha256::digest(&bytes))
        );
        let field = observation.schema.field_with_name("amount_cents").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(field.is_nullable());
        assert_eq!(field.metadata()["PARQUET:field_id"], "2");
    }
    let complete = serde_json::to_value(&result.native_versions).unwrap();
    job.queries.push(SqlQuery {
        name: "late_failure".into(),
        sql: "SELECT missing_observation_column".into(),
    });
    let failure = job.run(limits()).await.unwrap_err();
    assert!(
        failure
            .error
            .to_string()
            .contains("missing_observation_column")
    );
    assert_eq!(failure.report()["completed"]["native_versions"], complete);
    std::fs::rename(hidden, &data.path).unwrap();
    let JobInput::Version { version, .. } = &mut job.inputs[0] else {
        unreachable!()
    };
    version.manifest.digest = "0".repeat(64);
    let refusal = job.run(limits()).await.unwrap_err();
    assert_eq!(refusal.error.code(), "release_mismatch");
    assert!(refusal.completed.native_versions.is_empty());
}

fn update(root: &Path, operation: &str, replacements: &[(&str, Option<i64>)]) -> UpdateSpec {
    UpdateSpec {
        dataset: id("amounts"),
        operation: id(operation),
        definition: "v1".into(),
        base: id("initial"),
        keys: vec!["id".into()],
        replacements: input(
            &root.join(format!("{operation}.parquet")),
            replacements,
            false,
        ),
        deletions: Some(input(
            &root.join(format!("{operation}-delete.parquet")),
            &[("B", None)],
            true,
        )),
        relationship: None,
        merge: None,
    }
}

#[tokio::test]
async fn based_correction_delete_and_rule_only_reuse_keep_history() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let spec = update(
        root.path(),
        "correction",
        &[("A", Some(120)), ("C", Some(0))],
    );
    let request = json!({"action": "prepare_update", "update": spec, "policy": policy(&base)});
    let prepared = managed(&engine, request.clone()).await.unwrap();
    let candidate: VersionRef = serde_json::from_value(prepared["candidate"].clone()).unwrap();
    assert_eq!(prepared["selection"], "pending");
    assert_eq!(
        engine.latest(&id("amounts")).await.unwrap().version_ref(),
        base
    );
    assert_eq!(
        rows(&engine, &candidate).await,
        BTreeMap::from([
            ("A".into(), Some(120)),
            ("C".into(), Some(0)),
            ("D".into(), Some(50)),
        ])
    );
    assert_eq!(
        rows(&engine, &base).await,
        BTreeMap::from([
            ("A".into(), Some(100)),
            ("B".into(), Some(200)),
            ("D".into(), Some(50)),
        ])
    );
    let pending = managed(&engine, json!({
        "action": "select", "dataset": "amounts", "operation": "correction", "policy_id": "quality-v1",
    })).await.unwrap_err();
    assert_eq!(pending.code(), "qualification_required");
    assert_eq!(
        engine.update(&spec).await.unwrap_err().code(),
        "qualification_policy_mismatch"
    );

    let mut changed = request.clone();
    changed["update"]["replacements"]["digest"] = json!("0".repeat(64));
    assert_eq!(
        managed(&engine, changed).await.unwrap_err().code(),
        "operation_conflict"
    );

    let mut next_policy = policy(&base);
    next_policy["id"] = json!("quality-v2");
    next_policy["rules_sha256"] = json!("c".repeat(64));
    let before = engine.inspect(&id("amounts")).await.unwrap();
    let amended = managed(
        &engine,
        json!({
            "action": "requalify", "dataset": "amounts", "operation": "correction",
            "previous_policy_id": "quality-v1", "policy": next_policy,
        }),
    )
    .await
    .unwrap();
    assert_eq!(amended["candidate"], prepared["candidate"]);
    assert_eq!(
        amended["computation_identity"],
        prepared["computation_identity"]
    );
    assert_eq!(amended["accepted_qualification"], Value::Null);
    let after = engine.inspect(&id("amounts")).await.unwrap();
    for (key, value) in before.iter().filter(|(key, _)| key.starts_with("phase/")) {
        assert_eq!(after.get(key), Some(value));
    }
    engine.shutdown().await.unwrap();
    drop(engine);

    let cold = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let resumed = managed(&cold, request).await.unwrap();
    assert_eq!(resumed["candidate"], prepared["candidate"]);
    assert_eq!(
        resumed["computation_identity"],
        prepared["computation_identity"]
    );
    assert_eq!(resumed["policy"]["id"], "quality-v2");
}

#[tokio::test]
async fn explicit_null_replaces_while_declared_coalescing_preserves_prior() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let replace = update(root.path(), "replace-null", &[("A", None)]);
    let mut coalesce = update(root.path(), "coalesce-null", &[("A", None)]);
    coalesce.merge = Some(
        serde_json::from_value(json!({
            "columns": {"amount_cents": "latest_or_backfill"},
        }))
        .unwrap(),
    );
    for (spec, expected) in [(replace, None), (coalesce, Some(100))] {
        let receipt = managed(
            &engine,
            json!({
                "action": "prepare_update", "update": spec, "policy": policy(&base),
            }),
        )
        .await
        .unwrap();
        let candidate: VersionRef = serde_json::from_value(receipt["candidate"].clone()).unwrap();
        assert_eq!(
            rows(&engine, &candidate).await,
            BTreeMap::from([("A".into(), expected), ("D".into(), Some(50)),])
        );
    }
}

#[tokio::test]
async fn successful_empty_scope_deletes_only_explicitly_read_keys() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let spec = update(root.path(), "empty-scope", &[]);
    let receipt = managed(
        &engine,
        json!({
            "action": "prepare_update", "update": spec, "policy": policy(&base),
        }),
    )
    .await
    .unwrap();
    let candidate: VersionRef = serde_json::from_value(receipt["candidate"].clone()).unwrap();
    assert_eq!(
        rows(&engine, &candidate).await,
        BTreeMap::from([("A".into(), Some(100)), ("D".into(), Some(50)),])
    );
}

#[tokio::test]
async fn computed_base_refuses_before_managed_update_admission() {
    let root = TempDir::new().unwrap();
    let (engine, _) = fixture(root.path()).await;
    let grouped = engine
        .backfill(
            &serde_json::from_value(json!({
                "dataset": "amounts", "operation": "with-extra", "base": "initial",
                "definition": "v1", "group": "extra", "group_definition": "double-v1",
                "fields": [{"id": 1, "name": "doubled", "expression": "amount_cents * 2"}],
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut spec = update(root.path(), "unsupported", &[("A", Some(120))]);
    spec.base = grouped.id.clone();
    let refused = managed(
        &engine,
        json!({
            "action": "prepare_update", "update": spec, "policy": policy(&grouped.version_ref()),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code(), "unsupported_native_mapping");
    let retained = engine.inspect(&id("amounts")).await.unwrap();
    assert!(!retained.contains_key("operation/unsupported"));
    assert_eq!(engine.latest(&id("amounts")).await.unwrap(), grouped);
}

#[tokio::test]
async fn pending_quality_policy_and_completed_read_survive_lifecycle_compaction() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let spec = update(root.path(), "pending", &[("A", Some(120))]);
    let prepared = managed(
        &engine,
        json!({
            "action": "prepare_update", "update": spec, "policy": policy(&base),
        }),
    )
    .await
    .unwrap();
    let candidate: VersionRef = serde_json::from_value(prepared["candidate"].clone()).unwrap();
    let expected = BTreeMap::from([("A".into(), Some(120)), ("D".into(), Some(50))]);
    assert_eq!(rows(&engine, &candidate).await, expected);
    engine.compact_lifecycle(&id("amounts")).await.unwrap();
    let retained = managed(
        &engine,
        json!({
            "action": "inspect", "dataset": "amounts", "operation": "pending",
        }),
    )
    .await
    .unwrap();
    assert_eq!(retained["candidate"], prepared["candidate"]);
    assert_eq!(retained["selection"], "pending");
    assert_eq!(rows(&engine, &candidate).await, expected);
    assert_eq!(
        engine.latest(&id("amounts")).await.unwrap().version_ref(),
        base
    );
}

#[tokio::test]
async fn quality_mapping_revisions_are_explicit_and_cannot_relabel_a_policy() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let spec = update(root.path(), "mapping-two", &[("A", Some(120))]);
    let mut policy_two = policy(&base);
    policy_two["semantics"]["revision"] = json!("2");
    let missing_fact = policy_two.clone();
    let missing_request = json!({"action":"prepare_update","update":spec,"policy":missing_fact});
    assert_eq!(
        managed(&engine, missing_request).await.unwrap_err().code(),
        "invalid_qualification_policy"
    );
    policy_two["required_checks"]
        .as_array_mut()
        .unwrap()
        .push(json!("ddk:quality-execution-complete"));
    let request = json!({"action":"prepare_update", "update":spec, "policy":policy_two});
    let prepared = managed(&engine, request.clone()).await.unwrap();
    assert_eq!(prepared["policy"]["semantics"]["revision"], "2");
    assert_eq!(prepared["accepted_qualification"], Value::Null);
    for crossed in ["1", "3"] {
        let mut changed = request.clone();
        changed["policy"]["semantics"]["revision"] = json!(crossed);
        let error = managed(&engine, changed).await.unwrap_err();
        assert_eq!(
            error.code(),
            if crossed == "3" {
                "invalid_qualification_policy"
            } else {
                "qualification_policy_mismatch"
            }
        );
    }
    let one = update(root.path(), "mapping-one", &[("A", Some(121))]);
    let prepared_one = managed(
        &engine,
        json!({"action":"prepare_update", "update":one, "policy":policy(&base)}),
    )
    .await
    .unwrap();
    assert_eq!(prepared_one["policy"]["semantics"]["revision"], "1");
    assert_eq!(prepared_one["accepted_qualification"], Value::Null);
}
