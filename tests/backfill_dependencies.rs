//! Independent backfill roots retain exact source/output snapshots across recovery.
#![cfg(feature = "test-util")]

use super::scalar;
use super::support;

use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    engine::verify_release,
    model::{NativeSnapshotRef, ObjectRef, TablePin},
    storage,
};
use scalar::{fixture, id, limits, spec};
use serde_json::Value;
use std::collections::BTreeMap;

fn completions(records: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    records
        .iter()
        .filter(|(key, _)| {
            key.starts_with("backfill-prepared/") || key.starts_with("backfill-validation/")
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn assert_dependencies(record: &Value, source: &TablePin, output: &TablePin) {
    assert_eq!(
        record["dependencies"]["source"],
        serde_json::to_value(NativeSnapshotRef::from(source)).unwrap()
    );
    assert_eq!(
        record["dependencies"]["output"],
        serde_json::to_value(NativeSnapshotRef::from(output)).unwrap()
    );
}

async fn recover(empty: bool, phase: &str, legacy: bool) {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), empty).await;
    let request = spec(&base, "derived", "length", "text_length", "length(text)");
    drop(engine);
    support::kill_after(root.path(), &request, "backfill", phase);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let before = engine.inspect(&request.dataset).await.unwrap();
    if empty {
        assert_eq!(base.dataset.groups["source"].rows, 0);
        assert!(before.keys().all(|key| !key.starts_with("phase/backfill/")));
    }
    let mut retained = completions(&before);
    let prepared_value = retained
        .iter()
        .find(|(key, _)| key.starts_with("backfill-prepared/"))
        .unwrap()
        .1;
    // The additive shape remains readable as the original TablePin/ObjectRef.
    let prepared: TablePin = serde_json::from_value(prepared_value.clone()).unwrap();
    assert_dependencies(prepared_value, &base.dataset.groups["source"], &prepared);
    let origins_ref: ObjectRef = serde_json::from_value(prepared_value["origins"].clone()).unwrap();
    let origins_bytes = std::fs::read(&origins_ref.path).unwrap();
    let origins: Value = serde_json::from_slice(&origins_bytes).unwrap();
    assert_eq!(origins["files"].as_array().unwrap().is_empty(), empty);
    assert_dependencies(&origins, &base.dataset.groups["source"], &prepared);
    let validation = retained
        .iter()
        .find(|(key, _)| key.starts_with("backfill-validation/"));
    assert_eq!(validation.is_some(), phase == "validation");
    if let Some((_, value)) = validation {
        let output: ObjectRef = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(output, prepared.metadata);
        assert_dependencies(value, &base.dataset.groups["source"], &prepared);
        assert_eq!(value["origins"], prepared_value["origins"]);
    }
    if legacy {
        // Retire the modern generations before replacing their record shapes;
        // their existing protection bindings must not qualify the legacy values.
        let plan = engine
            .plan_phase_retirement(
                &request.dataset,
                &id("legacy-shapes"),
                &retained.keys().cloned().collect::<Vec<_>>(),
                "replace fixture completions with unknown legacy generations",
            )
            .await
            .unwrap();
        engine.retire_phases(&plan).await.unwrap();
    }
    drop(engine);

    if legacy {
        // Reconstruct the older serialized record shapes, without inventing
        // dependency information from their material hashes or current tables.
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&request.dataset).await.unwrap();
        let changes = retained
            .iter_mut()
            .map(|(key, value)| {
                value.as_object_mut().unwrap().remove("dependencies");
                value.as_object_mut().unwrap().remove("origins");
                record(key, value).unwrap()
            })
            .collect();
        lifecycle.commit(&view, changes).await.unwrap();
    }

    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    let (release, work) = support::resume(root.path(), "backfill");
    let expected = if phase == "processing" {
        vec!["backfill-validation", "backfill-readback"]
    } else {
        vec!["backfill-readback"]
    };
    assert_eq!(work, expected, "completed work must not run again");
    assert_eq!(release.dataset.groups[request.group.as_str()], prepared);
    assert_eq!(
        release.dataset.computed[request.group.as_str()]
            .origins
            .as_ref(),
        if legacy { None } else { Some(&origins_ref) }
    );
    assert_eq!(std::fs::read(&origins_ref.path).unwrap(), origins_bytes);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in retained {
        assert_eq!(
            after[&key], value,
            "first completion provenance must remain"
        );
        if legacy {
            assert!(after[&key].get("dependencies").is_none());
            assert!(after[&key].get("origins").is_none());
        } else {
            assert_eq!(
                after[&key]["origins"],
                serde_json::to_value(&origins_ref).unwrap()
            );
        }
    }
    if phase == "processing" {
        // A new validation knows the snapshots it actually checked even when
        // the separate prepared root has older, unknown dependency metadata.
        let validation = after
            .iter()
            .find(|(key, _)| key.starts_with("backfill-validation/"))
            .unwrap()
            .1;
        assert_dependencies(validation, &base.dataset.groups["source"], &prepared);
        assert_eq!(
            validation["origins"],
            if legacy {
                Value::Null
            } else {
                serde_json::to_value(&origins_ref).unwrap()
            }
        );
    }
    verify_release(&release, limits()).await.unwrap();
    assert_eq!(engine.backfill(&request).await.unwrap(), release);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_source_completions_retain_dependencies_without_file_work() {
    for phase in ["processing", "validation"] {
        recover(true, phase, false).await;
    }
    recover(false, "validation", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_group_completions_resume_and_keep_unknown_dependencies() {
    for phase in ["processing", "validation"] {
        recover(true, phase, true).await;
    }
    recover(false, "validation", true).await;
}

#[tokio::test]
async fn legacy_file_reuse_keeps_original_execution_unknown() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(
        &base,
        "legacy-files",
        "length",
        "text_length",
        "length(text)",
    );
    assert_eq!(
        engine
            .backfill_until(&request, "files")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let mut retained: BTreeMap<_, _> = engine
        .inspect(&request.dataset)
        .await
        .unwrap()
        .into_iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
        .collect();
    assert!(!retained.is_empty());
    let plan = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("legacy-file-shapes"),
            &retained.keys().cloned().collect::<Vec<_>>(),
            "replace fixture file completions with unknown legacy executions",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    drop(engine);
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.path().join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let view = lifecycle.open(&request.dataset).await.unwrap();
    let changes = retained
        .iter_mut()
        .map(|(key, value)| {
            value.as_object_mut().unwrap().remove("execution");
            value.as_object_mut().unwrap().remove("source_metadata");
            record(key, value).unwrap()
        })
        .collect();
    lifecycle.commit(&view, changes).await.unwrap();
    drop(lifecycle);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.files_completed, Some(retained.len()));
    assert_eq!(plan.files_pending, Some(0));
    let release = engine.backfill(&request).await.unwrap();
    let origins_ref = release.dataset.computed["length"].origins.as_ref().unwrap();
    let origins: Value =
        serde_json::from_slice(&std::fs::read(&origins_ref.path).unwrap()).unwrap();
    let files = origins["files"].as_array().unwrap();
    assert_eq!(files.len(), retained.len());
    for origin in files {
        let key = origin["generation"]["key"].as_str().unwrap();
        let mut expected = retained[key].clone();
        // Reading an older shape supplies unknown optional fields, never a
        // declaration or operation inferred from the adopting request.
        expected["execution"] = Value::Null;
        expected["source_metadata"] = Value::Null;
        assert_eq!(origin["completion"], expected);
    }
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in retained {
        assert_eq!(after[&key], value);
    }
    assert_eq!(engine.backfill(&request).await.unwrap(), release);
}
