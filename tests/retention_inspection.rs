//! Shared protection discovery reads bounded metadata, not retained data payloads.
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    model::{GenerationSeal, ObjectRef, RetentionInspection},
    storage,
};
use scalar::{fixture, id, limits, spec};
use std::collections::BTreeMap;

async fn lifecycle(root: &std::path::Path) -> Lifecycle {
    Lifecycle::new(
        storage::local_catalog(&root.join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    )
}

fn object(owner: &docspec::model::Id, path: &std::path::Path) -> ObjectRef {
    let (digest, bytes) = storage::digest(path).unwrap();
    ObjectRef {
        owner: owner.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    }
}

#[tokio::test]
async fn completed_and_interrupted_roots_protect_without_opening_payloads() {
    for empty in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, _) = fixture(root.path(), empty).await;
        let request = spec(&base, "prepared", "length", "text_length", "length(text)");
        assert_eq!(
            engine
                .backfill_until(&request, "validation")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        // Admission and invalidation rewrite authority without selecting another
        // result. Retention must resolve the original immutable selection after
        // both rewrites, without a second persisted authority binding.
        let docspec::model::OperationRequest::Build(mut pending) = base.dataset.request.clone()
        else {
            panic!("fixture must build its initial selection");
        };
        pending.operation = id("pending-input");
        assert_eq!(
            engine
                .run_until(&pending, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        engine
            .invalidate(&request.dataset, "new qualification required", None)
            .await
            .unwrap();
        let records = engine.inspect(&request.dataset).await.unwrap();
        let authority: docspec::model::Authority =
            serde_json::from_value(records["authority"].clone()).unwrap();
        assert_eq!(authority.release.as_ref(), Some(&base));
        let view = lifecycle(root.path())
            .await
            .open(&request.dataset)
            .await
            .unwrap();
        assert!(authority.selection_revision.unwrap() < view.revision);
        let prepared: docspec::model::TablePin = serde_json::from_value(
            records
                .iter()
                .find(|(key, _)| key.starts_with("backfill-prepared/"))
                .unwrap()
                .1
                .clone(),
        )
        .unwrap();
        let mut candidates = base.dataset.inputs.values().cloned().collect::<Vec<_>>();
        candidates.extend(prepared.objects.clone());
        candidates.extend(base.dataset.groups["source"].objects.clone());
        // Missing source payloads must not trigger data verification or prevent
        // discovery from the recorded native inventories and manifest metadata.
        for object in &candidates {
            if object.path.ends_with(".parquet") {
                let _ = std::fs::remove_file(&object.path);
            }
        }
        let result = engine.inspect_retention(&candidates).await.unwrap();
        assert!(result.objects.iter().all(|object| object.blocker.is_some()));
        assert_eq!(result.unmatched_bytes, 0);
        assert_eq!(
            result.metadata_objects_read,
            if empty { 1 } else { 2 },
            "read the shared origins and any declaration once; complete pins need no native reread"
        );
        let expected: BTreeMap<_, _> = candidates.iter().map(|o| (&o.path, o.bytes)).collect();
        assert_eq!(result.protected_bytes, expected.values().sum::<u64>());
        assert_eq!(result.boundaries.len(), 1);
    }
}

async fn isolated(
    key: &str,
    value: &serde_json::Value,
    candidates: &[ObjectRef],
) -> docspec::Result<RetentionInspection> {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let catalog = lifecycle(root.path()).await;
    let view = catalog.open(&id("isolated")).await.unwrap();
    catalog
        .commit(&view, vec![record(key, value).unwrap()])
        .await
        .unwrap();
    engine.inspect_retention(candidates).await
}

#[tokio::test]
async fn selected_and_prepared_roots_retain_original_executions_without_old_phases() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "derived", "length", "text_length", "length(text)");
    let selected = engine.backfill(&request).await.unwrap();
    let reference = selected.dataset.computed["length"]
        .origins
        .as_ref()
        .unwrap();
    let origins: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&reference.path).unwrap()).unwrap();
    let mut candidates = BTreeMap::from([(reference.path.clone(), reference.clone())]);
    for file in origins["files"].as_array().unwrap() {
        let completion = &file["completion"];
        for value in [
            &completion["source"],
            &completion["source_metadata"],
            &completion["descriptor"],
            &completion["execution"]["declaration"],
        ]
        .into_iter()
        .chain(completion["outputs"].as_array().unwrap())
        {
            let object: ObjectRef = serde_json::from_value(value.clone()).unwrap();
            candidates.insert(object.path.clone(), object);
        }
    }
    let candidates = candidates.into_values().collect::<Vec<_>>();
    let records = engine.inspect(&request.dataset).await.unwrap();
    let (prepared_key, prepared) = records
        .iter()
        .find(|(key, _)| key.starts_with("backfill-prepared/"))
        .unwrap();
    // Each isolated catalog has only this one root, without file completions,
    // operation recovery, or grants that could mask a missing transitive reference.
    for (key, value) in [
        (prepared_key.clone(), prepared.clone()),
        (
            format!("result/{}", selected.id),
            serde_json::to_value(&selected).unwrap(),
        ),
    ] {
        let inspected = isolated(&key, &value, &candidates).await.unwrap();
        assert!(
            inspected
                .objects
                .iter()
                .all(|entry| entry.blocker.is_some()),
            "{key}"
        );
        assert_eq!(inspected.unmatched_bytes, 0);
    }
    for candidate in &candidates {
        if candidate.path.ends_with(".parquet") {
            std::fs::remove_file(&candidate.path).unwrap();
        }
    }
    assert!(
        isolated(prepared_key, prepared, &candidates).await.is_ok(),
        "protection discovery must not open original or computed values"
    );
}

#[tokio::test]
async fn each_backfill_completion_independently_protects_its_consumed_source() {
    for empty in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, _) = fixture(root.path(), empty).await;
        let request = spec(&base, "prepared", "length", "text_length", "length(text)");
        assert!(engine.backfill_until(&request, "validation").await.is_err());
        let records = engine.inspect(&request.dataset).await.unwrap();
        let source = &base.dataset.groups["source"];
        let candidates = source
            .objects
            .iter()
            .filter(|object| empty || object.path.ends_with(".parquet"))
            .cloned()
            .collect::<Vec<_>>();
        assert!(!candidates.is_empty());
        for (key, value) in records.iter().filter(|(key, _)| {
            key.starts_with("backfill-prepared/") || key.starts_with("backfill-validation/")
        }) {
            let result = isolated(key, value, &candidates).await.unwrap();
            assert!(
                result
                    .objects
                    .iter()
                    .all(|object| object.blocker.as_ref().unwrap().key == *key)
            );
            let mut unknown = value.clone();
            unknown["future_input"] = serde_json::to_value(&source.metadata).unwrap();
            assert!(isolated(key, &unknown, &candidates).await.is_err());
            let mut legacy = value.clone();
            legacy.as_object_mut().unwrap().remove("dependencies");
            assert_eq!(
                isolated(key, &legacy, &candidates)
                    .await
                    .unwrap_err()
                    .code(),
                "retention_unknown_dependencies"
            );
            let mut mismatch = value.clone();
            mismatch["dependencies"]["source"]["schema_id"] =
                serde_json::json!(source.schema_id + 100);
            assert_eq!(
                isolated(key, &mismatch, &candidates)
                    .await
                    .unwrap_err()
                    .code(),
                "retention_inventory"
            );
            mismatch["dependencies"]["source"]["schema_id"] = serde_json::json!(source.schema_id);
            mismatch["dependencies"]["source"]["snapshot"] = serde_json::json!(i64::MAX);
            assert_eq!(
                isolated(key, &mismatch, &candidates)
                    .await
                    .unwrap_err()
                    .code(),
                "retention_inventory"
            );
        }
        if let Some((key, value)) = records
            .iter()
            .find(|(key, _)| key.starts_with("phase/backfill/"))
        {
            let consumed: ObjectRef = serde_json::from_value(value["source"].clone()).unwrap();
            let result = isolated(key, value, &[consumed]).await.unwrap();
            assert_eq!(result.objects[0].blocker.as_ref().unwrap().key, *key);
            assert_eq!(
                result.metadata_objects_read, 1,
                "source is bound directly; the original declaration is a typed metadata leaf"
            );
        }
        let (key, value) = records
            .iter()
            .find(|(key, _)| key.starts_with("backfill-prepared/"))
            .unwrap();
        let mut native: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&source.metadata.path).unwrap()).unwrap();
        native["future-dependency"] = serde_json::json!({"path":"unrecognized"});
        let path = root.path().join("future.metadata.json");
        std::fs::write(&path, serde_json::to_vec(&native).unwrap()).unwrap();
        let mut altered = value.clone();
        altered["dependencies"]["source"]["metadata"] =
            serde_json::to_value(object(&source.metadata.owner, &path)).unwrap();
        assert!(
            isolated(key, &altered, &candidates).await.is_err(),
            "unknown native dependencies must refuse"
        );
    }
}

#[tokio::test]
async fn schema_admission_and_evolved_group_metadata_preserve_native_interpretation() {
    use docspec::model::{
        ColumnAddition, ColumnType, PhaseResult, RunSpec, SchemaField, SchemaSpec,
    };
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let evolved = engine
        .evolve_schema(&SchemaSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("evolved"),
            base: base.id.clone(),
            definition: "v1".into(),
            next_definition: "v2".into(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "id".into(),
                },
                SchemaField {
                    id: 2,
                    name: "text".into(),
                },
            ],
            additions: vec![ColumnAddition {
                name: "flag".into(),
                data_type: ColumnType::Boolean,
                default: serde_json::json!(true),
            }],
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap();
    let mut group = spec(
        &evolved,
        "prepared",
        "derived",
        "observed",
        "CASE WHEN flag THEN id ELSE -1 END",
    );
    group.definition = "v2".into();
    let completed = engine.backfill(&group).await.unwrap();
    let incoming = scalar::input(&root.path().join("replacement.parquet"), &[(Some(7), None)]);
    let request = RunSpec {
        dataset: group.dataset.clone(),
        operation: id("rebuild"),
        base: Some(completed.id),
        definition: "v2".into(),
        input: incoming.path,
        expected_digest: incoming.digest,
        sql: "SELECT * FROM input".into(),
        relationship: None,
    };
    assert!(engine.run_until(&request, "groups").await.is_err());
    let records = engine.inspect(&group.dataset).await.unwrap();
    let candidate = base.dataset.inputs.values().next().unwrap().clone();
    let (key, admission) = records
        .iter()
        .find(|(_, value)| {
            serde_json::from_value::<PhaseResult>((*value).clone())
                .is_ok_and(|phase| phase.operation == id("evolved") && phase.phase == "admission")
        })
        .unwrap();
    let result = isolated(key, admission, &[candidate]).await.unwrap();
    assert!(
        result.objects[0].blocker.is_some(),
        "schema admission retains the whole base release"
    );
    let source = &evolved.dataset.groups["source"];
    let (key, prepared) = records
        .iter()
        .find(|(key, _)| {
            key.starts_with("backfill-prepared/")
                && serde_json::from_value::<ObjectRef>(
                    records[*key]["dependencies"]["source"]["metadata"].clone(),
                )
                .is_ok_and(|object| object == source.metadata)
        })
        .unwrap();
    // Reorder native map-backed collections explicitly to exercise stable ID
    // matching regardless of the randomized HashMap order inside Iceberg.
    let mut native: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&source.metadata.path).unwrap()).unwrap();
    native["schemas"].as_array_mut().unwrap().reverse();
    let path = root.path().join("reordered.metadata.json");
    std::fs::write(&path, serde_json::to_vec(&native).unwrap()).unwrap();
    let mut prepared = prepared.clone();
    prepared["dependencies"]["source"]["metadata"] =
        serde_json::to_value(object(&source.metadata.owner, &path)).unwrap();
    let candidates = source
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        isolated(key, &prepared, &candidates)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|o| o.blocker.is_some())
    );
    let groups = &records["groups/rebuild"];
    let processing = records
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|phase| phase.operation == request.operation && phase.phase == "processing")
        .unwrap();
    let source = processing.table.unwrap();
    let candidate = source
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let result = isolated("groups/rebuild", groups, std::slice::from_ref(candidate))
        .await
        .unwrap();
    assert!(result.objects[0].blocker.is_some());
}

#[tokio::test]
async fn family_only_namespace_retains_external_release_dependencies_once() {
    let producer = tempfile::tempdir().unwrap();
    let (_, release, _) = fixture(producer.path(), false).await;
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let catalog = lifecycle(root.path()).await;
    let seal = GenerationSeal {
        family: id("family"),
        generation: id("batch"),
        members: BTreeMap::from([(release.dataset.dataset.clone(), release.version_ref())]),
    };
    let view = catalog.open(&seal.family).await.unwrap();
    catalog
        .commit(
            &view,
            vec![
                record("generation/first", &seal).unwrap(),
                record("generation/second", &seal).unwrap(),
            ],
        )
        .await
        .unwrap();
    let candidates = release
        .dataset
        .inputs
        .values()
        .cloned()
        .chain(release.dataset.groups["source"].objects.clone())
        .collect::<Vec<_>>();
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.as_ref().unwrap().key == "generation/first")
    );
    assert_eq!(
        inspection.metadata_objects_read, 1,
        "shared manifest is decoded once"
    );
    assert_eq!(inspection.boundaries.len(), 1);
}

#[tokio::test]
async fn admitted_refresh_request_protects_inputs_before_any_phase_completes() {
    let root = tempfile::tempdir().unwrap();
    let (_, release, _) = fixture(root.path(), false).await;
    let request = serde_json::json!({"kind":"refresh", "spec": {
        "dataset":"summary", "operation":"pending", "base":null, "definition":"v1",
        "transform": {"kind":"aggregate", "input":release.version_ref(), "grouping": {
            "keys":["id"], "aggregates":[{"name":"total", "function":{"kind":"count_rows"}}]
        }}, "policy":"captured", "mode":"full", "relationship":null
    }});
    let candidate = release.dataset.inputs.values().next().unwrap().clone();
    let report = isolated("request/pending", &request, &[candidate])
        .await
        .unwrap();
    assert_eq!(
        report.objects[0].blocker.as_ref().unwrap().key,
        "request/pending"
    );
    assert_eq!(report.metadata_objects_read, 1);
}

#[tokio::test]
async fn unknown_and_legacy_roots_refuse_instead_of_reporting_unprotected() {
    for mode in ["unknown_family", "unknown_field", "legacy"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, release, _) = fixture(root.path(), true).await;
        let catalog = lifecycle(root.path()).await;
        let view = catalog.open(&release.dataset.dataset).await.unwrap();
        let records = catalog.records(&view).await.unwrap();
        let change = if mode == "unknown_family" {
            record("future-work/one", serde_json::json!({})).unwrap()
        } else {
            let (key, value) = records
                .iter()
                .find(|(key, _)| key.starts_with("phase/"))
                .unwrap();
            let mut value = value.clone();
            if mode == "legacy" {
                value.as_object_mut().unwrap().remove("dependencies");
            } else {
                value["future_input"] = serde_json::to_value(&release.manifest).unwrap();
            }
            record(key, value).unwrap()
        };
        catalog.commit(&view, vec![change]).await.unwrap();
        assert!(
            engine
                .inspect_retention(std::slice::from_ref(&release.manifest))
                .await
                .is_err(),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn candidate_identity_capacity_and_transport_boundaries_are_explicit() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path(), false).await;
    let missing = ObjectRef {
        owner: id("comments"),
        path: root
            .path()
            .join("unreferenced")
            .to_string_lossy()
            .into_owned(),
        digest: "0".repeat(64),
        bytes: 123,
    };
    let candidates = vec![
        release.manifest.clone(),
        release.manifest.clone(),
        missing.clone(),
    ];
    let expected = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(expected.objects.len(), 2);
    assert_eq!(expected.unmatched_bytes, 123);
    assert!(
        expected
            .objects
            .iter()
            .find(|object| object.object == missing)
            .unwrap()
            .blocker
            .is_none()
    );
    let response = docspec::http::router(engine.clone())
        .oneshot(
            Request::post("/retention/inspect")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&candidates).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let actual: RetentionInspection =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(actual, expected);
    let mut conflict = release.manifest.clone();
    conflict.bytes += 1;
    assert_eq!(
        engine
            .inspect_retention(&[conflict])
            .await
            .unwrap_err()
            .code(),
        "retention_inventory"
    );
    let input = root.path().join("objects.json");
    std::fs::write(&input, serde_json::to_vec(&candidates).unwrap()).unwrap();
    drop(engine);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args([
            "--workspace",
            root.path().join("workspace").to_str().unwrap(),
            "inspect-retention",
            input.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<RetentionInspection>(&output.stdout).unwrap(),
        expected
    );
    let tight = docspec::budget::Limits {
        max_metadata_bytes: 128,
        ..limits()
    };
    let engine = Engine::open(root.path().join("workspace"), tight)
        .await
        .unwrap();
    assert_eq!(
        engine
            .inspect_retention(&candidates)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}
