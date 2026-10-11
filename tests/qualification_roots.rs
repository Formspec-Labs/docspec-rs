//! Isolated qualification roots retain native evidence and original creation witnesses.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;
use arrow::array::Array;
use futures::TryStreamExt;

use docspec::{
    Engine,
    budget::{Budget, Limits},
    catalog::{Lifecycle, record},
    model::{
        Authority, DependencyCorrection, DependencyQualification, NativeSnapshotRef, ObjectRef,
        ReleasePin, RunSpec, TablePin,
    },
    storage,
};
use scalar::{id, input, spec};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

fn limits() -> Limits {
    Limits {
        memory_bytes: 512 << 20,
        max_metadata_bytes: 16 << 20,
        ..scalar::limits()
    }
}

type Reservations = Vec<(String, Value)>;

async fn qualified(
    root: &Path,
) -> (
    DependencyQualification,
    TablePin,
    Vec<ObjectRef>,
    Reservations,
) {
    let source = input(
        &root.join("input.parquet"),
        &[(Some(1), Some("original".into())), (Some(2), None)],
    );
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: id("comments"),
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
    let request = spec(&base, "original", "length", "text_length", "length(text)");
    let original = engine.backfill(&request).await.unwrap();
    engine
        .correct_dependencies(&DependencyCorrection {
            dataset: request.dataset.clone(),
            id: id("omitted-id"),
            definition: request.group_definition(),
            fields: [1].into_iter().collect(),
            reason: "fixture correction identifies omitted source context".into(),
        })
        .await
        .unwrap();
    let selected = engine
        .backfill(&spec(&original, "sibling", "next", "next_id", "id + 1"))
        .await
        .unwrap();
    let proof = selected.dataset.computed["length"]
        .qualification
        .as_ref()
        .unwrap()
        .as_ref()
        .clone();
    let origins: Value =
        serde_json::from_slice(&std::fs::read(&proof.origins.path).unwrap()).unwrap();
    let mut creations = Vec::new();
    for file in origins["files"].as_array().unwrap() {
        let completion = &file["completion"];
        for value in [
            &completion["descriptor"],
            &completion["execution"]["declaration"],
        ]
        .into_iter()
        .chain(completion["outputs"].as_array().unwrap())
        {
            creations.push(serde_json::from_value(value.clone()).unwrap());
        }
    }
    assert!(!creations.is_empty());
    let target_only = selected.dataset.groups["next"].clone();
    let reservations = engine
        .inspect(&proof.origins.owner)
        .await
        .unwrap()
        .into_iter()
        .filter(|(key, _)| key.starts_with("protection/"))
        .collect();
    drop(engine);
    (proof, target_only, creations, reservations)
}

async fn retain_only(lifecycle: &Lifecycle, proof: &DependencyQualification) -> String {
    let mut view = lifecycle.open(&proof.origins.owner).await.unwrap();
    // This is synthetic catalog isolation, not a supported retirement transition.
    // Keep the actual proof's first-correction revision valid without other roots.
    while view.revision + 1 < proof.first_revision {
        view = lifecycle
            .commit(
                &view,
                vec![record("authority", Authority::default()).unwrap()],
            )
            .await
            .unwrap();
    }
    let key = format!(
        "dependency-qualification/{}",
        blake3::hash(&serde_json::to_vec(proof).unwrap()).to_hex()
    );
    lifecycle
        .commit(&view, vec![record(&key, proof).unwrap()])
        .await
        .unwrap();
    let view = lifecycle.open(&proof.origins.owner).await.unwrap();
    let records = lifecycle.records(&view).await.unwrap();
    assert!(
        records
            .keys()
            .all(|retained| retained == &key || retained == "authority")
    );
    key
}

#[tokio::test]
async fn sole_qualification_root_protects_original_creations_and_target_native_closure() {
    let root = tempfile::tempdir().unwrap();
    let (proof, distinct_target, creations, _) = qualified(root.path()).await;
    let distinct_payload = distinct_target
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    assert_ne!(distinct_target.metadata, proof.target.metadata);
    // The real proof checks correspondence. This second synthetic variant only
    // tests inventory dispatch: its target is not already reachable from origins.
    for distinct in [false, true] {
        let isolated = tempfile::tempdir().unwrap();
        let workspace = isolated.path().join("workspace");
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&workspace).await.unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let mut retained = proof.clone();
        if distinct {
            retained.target = NativeSnapshotRef::from(&distinct_target);
        }
        let key = retain_only(&lifecycle, &retained).await;
        let mut candidates = BTreeMap::from([
            (retained.origins.path.clone(), retained.origins.clone()),
            (
                retained.target.metadata.path.clone(),
                retained.target.metadata.clone(),
            ),
        ]);
        for object in &creations {
            candidates.insert(object.path.clone(), object.clone());
        }
        if distinct {
            candidates.insert(distinct_payload.path.clone(), distinct_payload.clone());
        }
        let candidates = candidates.into_values().collect::<Vec<_>>();
        let inspected = engine.inspect_retention(&candidates).await.unwrap();
        assert_eq!(inspected.unmatched_bytes, 0);
        assert!(inspected.objects.iter().all(|entry| {
            entry
                .blocker
                .as_ref()
                .is_some_and(|blocker| blocker.key == key)
        }));
        assert_eq!(
            inspected.protected_bytes,
            candidates.iter().map(|object| object.bytes).sum::<u64>()
        );
        assert!(inspected.metadata_objects_read > 0);
    }
}

#[tokio::test]
async fn qualification_only_owner_acquires_original_creations_without_direct_phase_rows() {
    let root = tempfile::tempdir().unwrap();
    let (proof, _, creations, reservations) = qualified(root.path()).await;
    for (index, object) in creations.iter().enumerate() {
        let catalog =
            storage::local_catalog(&root.path().join(format!("isolated-catalog-{index}")))
                .await
                .unwrap();
        let lifecycle = Lifecycle::new(catalog.clone(), Budget::new(limits()).unwrap());
        let key = retain_only(&lifecycle, &proof).await;
        let view = lifecycle.open(&proof.origins.owner).await.unwrap();
        // Reservations prove path ownership, but contain no completed creations.
        lifecycle.commit(&view, reservations.clone()).await.unwrap();
        let engine = Engine::with_catalog(root.path().join("workspace"), catalog, limits())
            .await
            .unwrap();
        let consumer = id(&format!("consumer-{index}"));
        let request = RunSpec {
            dataset: consumer.clone(),
            operation: id("admitted"),
            base: None,
            relationship: None,
            input: object.path.clone(),
            expected_digest: object.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        };
        let mut mismatched = request.clone();
        mismatched.expected_digest = "f".repeat(64);
        assert_ne!(mismatched.expected_digest, object.digest);
        assert_eq!(
            engine
                .run_until(&mismatched, "admission")
                .await
                .unwrap_err()
                .code(),
            "invalid_owner_protection"
        );
        assert!(
            !engine
                .inspect(&proof.origins.owner)
                .await
                .unwrap()
                .keys()
                .any(|record_key| record_key.starts_with("owner-protection/"))
        );
        // Stop before processing: declarations and descriptors are metadata,
        // while owner acquisition and exact input binding are shared behavior.
        assert_eq!(
            engine
                .run_until(&request, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let records = engine.inspect(&consumer).await.unwrap();
        assert_eq!(
            records["request-dependencies/admitted"]["inputs"]["input"],
            serde_json::to_value(object).unwrap()
        );
        let owner_records = engine.inspect(&proof.origins.owner).await.unwrap();
        let grants = owner_records
            .iter()
            .filter(|(record_key, _)| record_key.starts_with("owner-protection/"))
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        assert!(
            grants
                .iter()
                .any(|grant| grant["consumer"] == consumer.as_str()
                    && grant["objects"] == serde_json::to_value(vec![object]).unwrap())
        );
        assert_eq!(
            grants.len(),
            1,
            "fresh catalog prevents an earlier grant masking fallback"
        );
        assert_eq!(owner_records[&key], serde_json::to_value(&proof).unwrap());
        assert!(
            !owner_records
                .keys()
                .any(|record_key| record_key.starts_with("phase/backfill/")
                    || record_key.starts_with("backfill-prepared/")
                    || record_key.starts_with("backfill-validation/")
                    || record_key.starts_with("result/")
                    || record_key.starts_with("readback/")
                    || record_key.starts_with("backfill-readback/"))
        );
        drop(engine);
    }
}

#[tokio::test]
async fn unselected_qualification_promise_ends_after_explicit_retirement() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let source = input(
        &root.path().join("input.parquet"),
        &[(Some(1), Some("original".into())), (Some(2), None)],
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
    let original = spec(
        &initial,
        "original",
        "length",
        "text_length",
        "length(text)",
    );
    assert_eq!(
        engine
            .backfill_until(&original, "files")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine
        .correct_dependencies(&DependencyCorrection {
            dataset: dataset.clone(),
            id: id("omitted-id"),
            definition: original.group_definition(),
            fields: [1].into_iter().collect(),
            reason: "retain the omitted source context before publishing derived work".into(),
        })
        .await
        .unwrap();
    let qualified = spec(
        &initial,
        "qualified",
        "length",
        "text_length",
        "length(text)",
    );
    assert_eq!(
        engine
            .backfill_until(&qualified, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&dataset).await.unwrap();
    let retained: ReleasePin = serde_json::from_value(
        records
            .iter()
            .find(|(key, _)| key.starts_with("backfill-readback/qualified/"))
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    let proof = retained.dataset.computed["length"]
        .qualification
        .as_ref()
        .unwrap()
        .as_ref()
        .clone();
    assert_eq!(engine.latest(&dataset).await.unwrap(), initial);
    assert!(initial.dataset.computed.is_empty());
    let origins: Value =
        serde_json::from_slice(&std::fs::read(&proof.origins.path).unwrap()).unwrap();
    let mut creations: Vec<ObjectRef> = Vec::new();
    for file in origins["files"].as_array().unwrap() {
        let completion = &file["completion"];
        for value in [
            &completion["descriptor"],
            &completion["execution"]["declaration"],
        ]
        .into_iter()
        .chain(completion["outputs"].as_array().unwrap())
        {
            creations.push(serde_json::from_value(value.clone()).unwrap());
        }
    }
    assert!(!creations.is_empty());
    let old = engine.inspect(&dataset).await.unwrap();
    let qualification_key = old
        .iter()
        .find(|(key, value)| {
            key.starts_with("dependency-qualification/")
                && **value == serde_json::to_value(&proof).unwrap()
        })
        .unwrap()
        .0
        .clone();
    let mut candidates = BTreeMap::from([
        (proof.origins.path.clone(), proof.origins.clone()),
        (
            proof.target.metadata.path.clone(),
            proof.target.metadata.clone(),
        ),
    ]);
    for object in creations {
        candidates.insert(object.path.clone(), object);
    }
    let candidates = candidates.into_values().collect::<Vec<_>>();
    assert!(
        candidates
            .iter()
            .any(|object| object.path.ends_with(".parquet"))
    );
    let protected = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(protected.unmatched_bytes, 0);
    assert!(
        protected
            .objects
            .iter()
            .all(|entry| entry.blocker.is_some())
    );

    // Withdraw the unselected derived operations. The selected source has no
    // computed descendants, so a new independent full Build is permitted.
    for operation in ["original", "qualified"] {
        engine
            .abandon(
                &dataset,
                &id(operation),
                "withdraw the unselected derived work",
            )
            .await
            .unwrap();
    }
    // Different bytes prevent material phase reuse; no required parent is erased.
    let expected = vec![(Some(9), Some("replacement".into())), (Some(10), None)];
    let source = input(&root.path().join("replacement.parquet"), &expected);
    let request = RunSpec {
        dataset: dataset.clone(),
        operation: id("replacement"),
        base: None,
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    let selected = engine.run(&request).await.unwrap();
    assert!(selected.dataset.parent_manifest.is_none());
    assert!(selected.dataset.computed.is_empty());
    assert_ne!(
        selected.dataset.groups["source"].metadata,
        proof.target.metadata
    );

    // Retire the old selected result and withdrawn readback independently.
    // Keep the qualification completion active as the sole final evidence root.
    for operation in ["initial", "qualified"] {
        let plan = engine
            .plan_availability_retirement(
                &dataset,
                &id(&format!("{operation}-availability")),
                &id(operation),
                "end the old selected-result availability promise",
            )
            .await
            .unwrap();
        engine.retire_availability(&plan).await.unwrap();
    }
    let completions = old
        .keys()
        .filter(|key| {
            key.starts_with("phase/")
                || key.starts_with("groups/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("prepared-changes/")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!completions.is_empty());
    let phases = engine
        .plan_phase_retirement(
            &dataset,
            &id("old-completions"),
            &completions,
            "end old computation recovery while keeping qualification evidence",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    for operation in ["initial", "original", "qualified"] {
        let plan = engine
            .plan_operation_retirement(
                &dataset,
                &id(&format!("{operation}-recovery")),
                &id(operation),
                "end terminal operation recovery and owned attempts",
            )
            .await
            .unwrap();
        engine.retire_operation(&plan).await.unwrap();
    }
    let protected = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(protected.unmatched_bytes, 0);
    assert!(protected.objects.iter().all(|entry| {
        entry
            .blocker
            .as_ref()
            .is_some_and(|root| root.key == qualification_key)
    }));
    assert_eq!(
        protected.protected_bytes,
        candidates.iter().map(|object| object.bytes).sum::<u64>()
    );

    let plan = engine
        .plan_phase_retirement(
            &dataset,
            &id("qualification-evidence"),
            std::slice::from_ref(&qualification_key),
            "end the last independent qualification evidence promise",
        )
        .await
        .unwrap();
    let retired = engine.retire_phases(&plan).await.unwrap();
    assert_eq!(engine.retire_phases(&plan).await.unwrap(), retired);
    assert_eq!(
        engine.inspect(&dataset).await.unwrap()[&qualification_key],
        old[&qualification_key]
    );
    let removal = engine
        .plan_removal(
            &dataset,
            &id("qualified-old-material"),
            &candidates,
            "remove exact old qualification material after all promises end",
        )
        .await
        .unwrap();
    // Unmatched means no active protection root; exact ownership and creation
    // evidence were checked separately by plan_removal before this report.
    assert_eq!(removal.inspection.protected_bytes, 0);
    assert_eq!(
        removal.inspection.unmatched_bytes,
        candidates.iter().map(|object| object.bytes).sum::<u64>()
    );
    assert!(
        removal
            .inspection
            .objects
            .iter()
            .all(|entry| entry.blocker.is_none())
    );
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.intent.objects.len(), candidates.len());
    assert_eq!(
        removed.confirmed_unlinked_bytes,
        candidates.iter().map(|object| object.bytes).sum::<u64>()
    );
    assert!(
        candidates
            .iter()
            .all(|object| !Path::new(&object.path).exists())
    );
    drop(engine);

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.retire_phases(&plan).await.unwrap(), retired);
    assert_eq!(engine.remove_objects(&removal).await.unwrap(), removed);
    assert_eq!(engine.latest(&dataset).await.unwrap(), selected);
    let batches = engine
        .read_group(&selected, "source")
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(
            batch.column_by_name("text").unwrap(),
            &arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        let text = text
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push((
                (!ids.is_null(row)).then(|| ids.value(row)),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
            ));
        }
    }
    values.sort();
    assert_eq!(values, expected);
    assert_eq!(engine.run(&request).await.unwrap(), selected);
}
