//! Isolated qualification roots retain native evidence and original creation witnesses.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    budget::{Budget, Limits},
    catalog::{Lifecycle, record},
    model::{
        Authority, DependencyCorrection, DependencyQualification, NativeSnapshotRef, ObjectRef,
        RunSpec, TablePin,
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
