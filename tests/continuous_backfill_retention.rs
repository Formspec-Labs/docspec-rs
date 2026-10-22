//! Superseded capture retirement releases only work no longer promised elsewhere.
#![cfg(all(feature = "test-util", unix))]

use super::{
    continuous_backfill::{assert_retained, assert_values, completed_work, continuous},
    scalar::{fixture, id, input, limits},
};
use docspec::{
    Engine,
    model::{ObjectRef, ReleasePin, RemovalOutcome, UpdateSpec},
};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

fn file_outputs(records: &BTreeMap<String, Value>) -> Vec<ObjectRef> {
    records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
        .flat_map(|(_, value)| value["outputs"].as_array().unwrap())
        .map(|value| serde_json::from_value(value.clone()).unwrap())
        .collect()
}

fn selected_outputs(release: &ReleasePin) -> Vec<ObjectRef> {
    let origins = release.dataset.computed["length"].origins.as_ref().unwrap();
    let origins: Value = serde_json::from_slice(&std::fs::read(&origins.path).unwrap()).unwrap();
    origins["files"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|file| file["completion"]["outputs"].as_array().unwrap())
        .map(|value| serde_json::from_value(value.clone()).unwrap())
        .collect()
}

#[tokio::test]
async fn superseded_child_promises_retire_independently_before_exact_removal() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, base, rows) = fixture(root.path(), false).await;
    let request = continuous(&base);
    let dataset = &base.dataset.dataset;
    assert_eq!(
        engine
            .continuous_backfill_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let child = engine
        .plan_continuous_backfill(&request)
        .await
        .unwrap()
        .child
        .unwrap();
    let before = engine.inspect(dataset).await.unwrap();
    let work = completed_work(&before);
    let old_outputs = file_outputs(&work);
    assert!(
        old_outputs.len() > 1,
        "fixture needs shared and replaced files"
    );
    assert_eq!(
        engine
            .plan_operation_retirement(dataset, &id("active"), &child, "not terminal")
            .await
            .unwrap_err()
            .code(),
        "operation_active"
    );
    let replacement = vec![(Some(0), Some("changed é🎉".into()))];
    engine
        .update(&UpdateSpec {
            dataset: dataset.clone(),
            operation: id("source-edit"),
            definition: "v1".into(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: input(&root.path().join("replacement.parquet"), &replacement),
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    let selected = engine.continuous_backfill(&request).await.unwrap();
    assert_ne!(selected.id, child);
    let after = engine.inspect(dataset).await.unwrap();
    assert_retained(&work, &after);
    let abandoned = &after[&format!("abandoned/{child}")];
    assert_eq!(abandoned["operation"], child.as_str());
    assert_eq!(abandoned["reason"], "superseded continuous capture");
    let selected_outputs = selected_outputs(&selected);
    let shared = old_outputs
        .iter()
        .find(|file| selected_outputs.contains(file))
        .unwrap()
        .clone();
    let obsolete = old_outputs
        .iter()
        .find(|file| !selected_outputs.contains(file))
        .unwrap()
        .clone();
    let candidates = [obsolete.clone(), shared.clone()];
    let initial = engine.inspect_retention(&candidates).await.unwrap();
    assert!(initial.objects.iter().all(|entry| entry.blocker.is_some()));

    let recovery = engine
        .plan_operation_retirement(
            dataset,
            &id("old-recovery"),
            &child,
            "end superseded capture recovery",
        )
        .await
        .unwrap();
    let retired_recovery = engine.retire_operation(&recovery).await.unwrap();
    assert_eq!(
        engine.retire_operation(&recovery).await.unwrap(),
        retired_recovery
    );
    let phases = work
        .keys()
        .filter(|key| !key.starts_with("backfill-readback/"))
        .cloned()
        .collect::<Vec<_>>();
    let phases = engine
        .plan_phase_retirement(
            dataset,
            &id("old-phases"),
            &phases,
            "end original phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    // A completed unselected readback promises availability independently of
    // both its abandoned operation and its retired upstream phase generations.
    let held = engine
        .inspect_retention(std::slice::from_ref(&obsolete))
        .await
        .unwrap();
    assert!(
        held.objects[0]
            .blocker
            .as_ref()
            .unwrap()
            .key
            .starts_with(&format!("backfill-readback/{child}/"))
    );
    let availability = engine
        .plan_availability_retirement(
            dataset,
            &id("old-readback"),
            &child,
            "end superseded readback availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let plan = engine
        .plan_removal(
            dataset,
            &id("remove-obsolete"),
            &candidates,
            "reclaim only unused superseded scalar output",
        )
        .await
        .unwrap();
    assert!(
        plan.inspection
            .objects
            .iter()
            .find(|entry| entry.object == obsolete)
            .unwrap()
            .blocker
            .is_none()
    );
    assert!(
        plan.inspection
            .objects
            .iter()
            .find(|entry| entry.object == shared)
            .unwrap()
            .blocker
            .is_some()
    );
    assert!(
        Path::new(&obsolete.path).exists(),
        "retirement does not delete bytes"
    );
    let removed = engine.remove_objects(&plan).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.progress.objects, vec![RemovalOutcome::Removed]);
    assert_eq!(removed.confirmed_unlinked_bytes, obsolete.bytes);
    assert_eq!(removed.reconciled_absent_bytes, 0);
    assert!(!Path::new(&obsolete.path).exists());
    assert!(Path::new(&shared.path).exists());
    let after_removal = engine.inspect(dataset).await.unwrap();
    assert_retained(&work, &after_removal);
    assert_eq!(after_removal[&format!("abandoned/{child}")], *abandoned);
    drop(engine);

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.remove_objects(&plan).await.unwrap(), removed);
    assert_eq!(
        engine.continuous_backfill(&request).await.unwrap(),
        selected
    );
    assert_eq!(engine.latest(dataset).await.unwrap(), selected);
    let mut expected = rows
        .into_iter()
        .filter(|row| row.0 != Some(0))
        .collect::<Vec<_>>();
    expected.extend(replacement);
    assert_values(&engine, &selected, expected).await;
    assert_eq!(
        docspec::storage::digest(Path::new(&shared.path)).unwrap().0,
        shared.digest
    );
}
