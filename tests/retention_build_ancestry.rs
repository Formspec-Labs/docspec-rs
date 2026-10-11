//! Full reconstruction releases obsolete payloads while retaining field history.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;
use arrow::array::{Array, Int64Array, StringArray};
use docspec::{
    Engine,
    engine::verify_release,
    model::{DependencyCorrection, ObjectRef, ReleasePin, RunSpec},
};
use futures::TryStreamExt;
use scalar::{id, input, spec};
use std::{collections::BTreeMap, path::Path};

fn limits() -> docspec::budget::Limits {
    docspec::budget::Limits {
        memory_bytes: 512 << 20,
        max_metadata_bytes: 16 << 20,
        ..scalar::limits()
    }
}

async fn check_values(engine: &Engine, selected: &ReleasePin, id_aware: bool) {
    let mut stream = engine.read_combined(selected).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(
            batch.column_by_name("text").unwrap(),
            &arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        let lengths = arrow::compute::cast(
            batch.column_by_name("text_length").unwrap(),
            &arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            actual.push((
                (!ids.is_null(row)).then(|| ids.value(row)),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
                (!lengths.is_null(row)).then(|| lengths.value(row)),
            ));
        }
    }
    actual.sort();
    let expected_length = if id_aware { 4 } else { 3 };
    assert_eq!(
        actual,
        vec![
            (None, None, None),
            (Some(9), Some("new".into()), Some(expected_length)),
            (Some(9), Some("new".into()), Some(expected_length))
        ]
    );
}

async fn lifetime(qualified_history: bool) {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let source = input(
        &root.path().join("old.parquet"),
        &[(Some(1), Some("old context".into())), (Some(2), None)],
    );
    let dataset = id("comments");
    let initial = engine
        .run(&RunSpec {
            dataset: dataset.clone(),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let original = engine
        .backfill(&spec(
            &initial,
            "length",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let mut operations = vec!["initial", "length"];
    let base = if qualified_history {
        engine
            .correct_dependencies(&DependencyCorrection {
                dataset: dataset.clone(),
                id: id("omitted-id"),
                definition: original.dataset.computed["length"].definition.clone(),
                fields: [1].into_iter().collect(),
                reason: "retain the original omitted context".into(),
            })
            .await
            .unwrap();
        operations.push("sibling");
        let selected = engine
            .backfill(&spec(&original, "sibling", "next", "next_id", "id + 1"))
            .await
            .unwrap();
        assert!(selected.dataset.computed["length"].qualification.is_some());
        // Preserve this owning selection's original inline proof in history.
        // Fresh work under its deficient definition must not evade the correction.
        let mut corrected = spec(
            &selected,
            "corrected-length",
            "length",
            "text_length",
            "CASE WHEN id IS NULL THEN length(text) ELSE length(text) + 1 END",
        );
        corrected.group_definition = "id-aware-length-v2".into();
        let corrected = engine.backfill(&corrected).await.unwrap();
        assert_eq!(
            corrected.dataset.parent_manifest.as_ref(),
            Some(&selected.manifest)
        );
        assert_eq!(corrected.dataset.computed["length"].input_fields["id"], 1);
        assert_eq!(corrected.dataset.computed["length"].input_fields["text"], 2);
        assert!(corrected.dataset.computed["length"].qualification.is_none());
        operations.push("corrected-length");
        corrected
    } else {
        original.clone()
    };
    let mut old_payloads = BTreeMap::new();
    for name in ["source", "length"] {
        for object in &original.dataset.groups[name].objects {
            if object.path.ends_with(".parquet") {
                old_payloads.insert(object.path.clone(), object.clone());
            }
        }
    }
    assert!(!old_payloads.is_empty());
    if !qualified_history {
        for object in initial.dataset.inputs.values() {
            old_payloads.insert(object.path.clone(), object.clone());
        }
    }
    let old_payloads = old_payloads.into_values().collect::<Vec<ObjectRef>>();
    let source = input(
        &root.path().join("new.parquet"),
        &[
            (Some(9), Some("new".into())),
            (Some(9), Some("new".into())),
            (None, None),
        ],
    );
    let request = RunSpec {
        dataset: dataset.clone(),
        operation: id("rebuilt"),
        base: Some(base.id.clone()),
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    let selected = engine.run(&request).await.unwrap();
    assert_eq!(
        selected.dataset.parent_manifest.as_ref(),
        Some(&base.manifest)
    );
    assert!(
        selected
            .dataset
            .computed
            .values()
            .all(|group| group.qualification.is_none())
    );
    assert_ne!(
        selected.dataset.computed["length"].origins,
        original.dataset.computed["length"].origins
    );
    assert!(
        selected.dataset.inputs.values().all(|object| !initial
            .dataset
            .inputs
            .values()
            .any(|old| old == object))
    );
    assert!(old_payloads.iter().all(|old| {
        !selected
            .dataset
            .groups
            .values()
            .any(|table| table.objects.contains(old))
    }));
    check_values(&engine, &selected, qualified_history).await;

    // An old retained availability root wins over the new interpretation edge.
    assert!(
        engine
            .inspect_retention(&old_payloads)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|entry| entry.blocker.is_some())
    );
    for operation in &operations {
        let plan = engine
            .plan_availability_retirement(
                &dataset,
                &id(&format!("{operation}-availability")),
                &id(operation),
                "end superseded result availability",
            )
            .await
            .unwrap();
        engine.retire_availability(&plan).await.unwrap();
    }
    // New completed work also binds its captured base. End all phase recovery
    // promises explicitly; the selected result still protects current outputs.
    let records = engine.inspect(&dataset).await.unwrap();
    let phases = records
        .keys()
        .filter(|key| {
            key.starts_with("phase/")
                || key.starts_with("groups/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("prepared-changes/")
                || key.starts_with("dependency-qualification/")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!phases.is_empty());
    let plan = engine
        .plan_phase_retirement(
            &dataset,
            &id("completions"),
            &phases,
            "end completed recovery independently of selected availability",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    for operation in &operations {
        let plan = engine
            .plan_operation_retirement(
                &dataset,
                &id(&format!("{operation}-recovery")),
                &id(operation),
                "end superseded terminal recovery",
            )
            .await
            .unwrap();
        engine.retire_operation(&plan).await.unwrap();
    }
    // Rebuilt operation admission/recovery deliberately keeps the full base.
    assert!(
        engine
            .inspect_retention(&old_payloads)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|entry| entry.blocker.is_some())
    );
    let recovery = engine
        .plan_operation_retirement(
            &dataset,
            &id("rebuilt-recovery"),
            &request.operation,
            "end terminal rebuild recovery while retaining its selected result",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    // This fixture has no cross-owner inputs; no grant lifetime is hidden.
    assert!(
        !engine
            .inspect(&dataset)
            .await
            .unwrap()
            .keys()
            .any(|key| key.starts_with("owner-protection/"))
    );

    let inspection = engine.inspect_retention(&old_payloads).await.unwrap();
    if qualified_history {
        // The selected history still verifies the old inline proof against its
        // exact originals, even after its independent completion is retired.
        assert!(
            inspection
                .objects
                .iter()
                .all(|entry| entry.blocker.is_some())
        );
        drop(engine);
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        check_values(&engine, &selected, qualified_history).await;
        verify_release(&selected, limits()).await.unwrap();
        return;
    }
    assert!(
        inspection
            .objects
            .iter()
            .all(|entry| entry.blocker.is_none())
    );
    let removal = engine
        .plan_removal(
            &dataset,
            &id("obsolete-payloads"),
            &old_payloads,
            "remove exact obsolete payloads after all independent promises end",
        )
        .await
        .unwrap();
    assert_eq!(removal.inspection.protected_bytes, 0);
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(
        removed.confirmed_unlinked_bytes,
        old_payloads.iter().map(|object| object.bytes).sum::<u64>()
    );
    assert!(
        old_payloads
            .iter()
            .all(|object| !Path::new(&object.path).exists())
    );
    // The immutable interpretation chain remains; do not erase field history.
    for object in [
        &initial.manifest,
        &original.manifest,
        &initial.dataset.groups["source"].metadata,
        &original.dataset.groups["length"].metadata,
    ] {
        assert!(Path::new(&object.path).exists());
        assert!(
            engine
                .inspect_retention(std::slice::from_ref(object))
                .await
                .unwrap()
                .objects[0]
                .blocker
                .is_some()
        );
    }
    // Only the external fixture producer disappears. The retained admitted
    // input stays authoritative, while a retired phase cannot run admission again.
    assert!(
        selected
            .dataset
            .inputs
            .values()
            .all(|object| object.path != request.input)
    );
    std::fs::remove_file(&request.input).unwrap();
    drop(engine);
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&dataset).await.unwrap(), selected);
    check_values(&engine, &selected, qualified_history).await;
    verify_release(&selected, limits()).await.unwrap();
    assert_eq!(engine.run(&request).await.unwrap(), selected);
    assert_eq!(engine.remove_objects(&removal).await.unwrap(), removed);
}

#[tokio::test]
async fn based_full_build_reclaims_obsolete_payloads_after_recovery_retirement() {
    lifetime(false).await;
}

#[tokio::test]
async fn based_full_build_keeps_historical_inline_qualification_payloads() {
    lifetime(true).await;
}
