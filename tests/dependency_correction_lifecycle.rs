//! Dependency corrections guard exports and preserve qualified scalar recovery evidence.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;

use arrow::array::Int32Array;
use docspec::{
    Engine,
    budget::Limits,
    export::{ExportPolicy, ExportRows, ExportSpec},
    model::{BackfillSpec, DependencyCorrection, ObjectRef, ReleasePin, RunSpec},
    restore::RestorePoint,
    storage,
};
use futures::TryStreamExt;
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

async fn classified(root: &Path) -> (Engine, BackfillSpec, ReleasePin) {
    let source = input(
        &root.join("input.parquet"),
        &[
            (Some(1), Some("é🎉".into())),
            (Some(1), Some("é🎉".into())),
            (Some(2), None),
            (None, Some(String::new())),
        ],
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
    let request = spec(&base, "lengths", "length", "text_length", "length(text)");
    let release = engine.backfill(&request).await.unwrap();
    (engine, request, release)
}

fn correction(request: &BackfillSpec) -> DependencyCorrection {
    DependencyCorrection {
        dataset: request.dataset.clone(),
        id: id("identifier-omission"),
        definition: request.group_definition(),
        fields: [1].into_iter().collect(),
        reason: "review identified omitted source identifier context".into(),
    }
}

fn export(source: &ReleasePin, operation: &str, group: &str) -> ExportSpec {
    ExportSpec {
        dataset: source.dataset.dataset.clone(),
        operation: id(operation),
        source: source.version_ref(),
        channel: id(operation),
        expected: None,
        policy: ExportPolicy {
            group: group.into(),
            rows: ExportRows::All,
            columns: if group == "source" {
                vec!["id".into(), "text".into()]
            } else {
                vec!["text_length".into()]
            },
        },
    }
}

#[tokio::test]
async fn corrected_computed_export_admission_preserves_completed_replay_and_source_exports() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, historical) = classified(root.path()).await;
    let completed_request = export(&historical, "completed", "length");
    let completed = engine.export(&completed_request).await.unwrap();
    engine
        .correct_dependencies(&correction(&request))
        .await
        .unwrap();

    let blocked = export(&historical, "blocked", "length");
    assert_eq!(
        engine.export(&blocked).await.unwrap_err().code(),
        "correction_required"
    );
    let records = engine.inspect(&request.dataset).await.unwrap();
    assert!(!records.contains_key("export-operation/blocked"));
    assert!(!records.contains_key("export-result/blocked"));
    assert_eq!(engine.export(&completed_request).await.unwrap(), completed);
    assert_eq!(
        engine
            .latest_export(&request.dataset, &completed_request.channel)
            .await
            .unwrap(),
        completed
    );
    storage::verify_object(&completed.object).unwrap();

    let source = engine
        .export(&export(&historical, "source-only", "source"))
        .await
        .unwrap();
    assert_eq!(source.rows, historical.dataset.groups["source"].rows);
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), historical);
}

#[tokio::test]
async fn correction_after_export_validation_blocks_selection_and_keeps_completed_output() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, historical) = classified(root.path()).await;
    let pending = export(&historical, "pending", "length");
    assert_eq!(
        engine
            .export_until(&pending, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let materialized = before["export-materialized/pending"].clone();
    let validation = before["export-validation/pending"].clone();
    let object: ObjectRef = serde_json::from_value(materialized["object"].clone()).unwrap();
    let bytes = std::fs::read(&object.path).unwrap();
    engine
        .correct_dependencies(&correction(&request))
        .await
        .unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    assert_eq!(
        engine.export(&pending).await.unwrap_err().code(),
        "correction_required"
    );
    assert_eq!(
        engine
            .latest_export(&request.dataset, &pending.channel)
            .await
            .unwrap_err()
            .code(),
        "no_export"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(after["export-materialized/pending"], materialized);
    assert_eq!(after["export-validation/pending"], validation);
    assert!(!after.contains_key("export-result/pending"));
    assert_eq!(std::fs::read(&object.path).unwrap(), bytes);
    storage::verify_object(&object).unwrap();
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), historical);
}

#[cfg(unix)]
#[tokio::test]
async fn qualified_inherited_group_restores_original_evidence_after_recovery_retirement() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, request, historical) = classified(root.path()).await;
    let decision = engine
        .correct_dependencies(&correction(&request))
        .await
        .unwrap();
    let sibling = engine
        .backfill(&spec(
            &historical,
            "sibling",
            "sibling",
            "next_id",
            "id + 1",
        ))
        .await
        .unwrap();
    let original = &historical.dataset.computed["length"];
    let inherited = &sibling.dataset.computed["length"];
    let proof = inherited.qualification.as_ref().unwrap();
    assert_eq!(proof.first_revision, decision.revision);
    assert_eq!(proof.fields, [1].into_iter().collect());
    assert_eq!(inherited.origins, original.origins);
    assert_eq!(inherited.request, original.request);
    assert_eq!(inherited.input_fields, original.input_fields);
    assert_eq!(
        sibling.dataset.groups["length"],
        historical.dataset.groups["length"]
    );

    let origins_ref = inherited.origins.as_ref().unwrap();
    let origins: Value =
        serde_json::from_slice(&std::fs::read(&origins_ref.path).unwrap()).unwrap();
    let mut evidence = BTreeMap::from([(origins_ref.path.clone(), origins_ref.clone())]);
    evidence.insert(
        proof.target.metadata.path.clone(),
        proof.target.metadata.clone(),
    );
    for dependency in ["source", "output"] {
        let object: ObjectRef =
            serde_json::from_value(origins["dependencies"][dependency]["metadata"].clone())
                .unwrap();
        evidence.insert(object.path.clone(), object);
    }
    assert!(!origins["files"].as_array().unwrap().is_empty());
    for origin in origins["files"].as_array().unwrap() {
        let completion = &origin["completion"];
        assert_eq!(
            completion["execution"]["operation"],
            request.operation.as_str()
        );
        for value in [
            &completion["descriptor"],
            &completion["source_metadata"],
            &completion["execution"]["declaration"],
        ] {
            let object: ObjectRef = serde_json::from_value(value.clone()).unwrap();
            evidence.insert(object.path.clone(), object);
        }
    }
    let records = engine.inspect(&request.dataset).await.unwrap();
    let keys = records
        .keys()
        .filter(|key| {
            [
                "phase/backfill/",
                "backfill-prepared/",
                "backfill-validation/",
                "dependency-qualification/",
            ]
            .iter()
            .any(|prefix| key.starts_with(prefix))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        keys.iter()
            .any(|key| key.starts_with("dependency-qualification/"))
    );
    let retirement = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("retire-recovery"),
            &keys,
            "qualified selected result retains original execution evidence",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    let candidates = evidence.values().cloned().collect::<Vec<_>>();
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );

    let reference = engine
        .create_restore_point(&request.dataset, &id("qualified-point"))
        .await
        .unwrap();
    let captured: RestorePoint =
        serde_json::from_slice(&std::fs::read(&reference.manifest.path).unwrap()).unwrap();
    for required in &candidates {
        let saved = captured
            .objects
            .iter()
            .find(|saved| saved.original == *required)
            .unwrap_or_else(|| {
                panic!(
                    "restore point omitted original scalar evidence {}",
                    required.path
                )
            });
        storage::verify_object(&saved.backup).unwrap();
    }
    let expected_bytes = candidates.iter().map(|object| object.bytes).sum::<u64>();
    drop(engine);
    // Private fixture unlink models missing originals; independent restore names retain their bytes.
    for object in &candidates {
        std::fs::remove_file(&object.path).unwrap();
    }
    let restored = Engine::restore_local(&workspace, &reference, limits(), false)
        .await
        .unwrap();
    assert_eq!(restored.restored_bytes, expected_bytes);
    for object in &candidates {
        storage::verify_object(object).unwrap();
    }
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let selected = engine.latest(&request.dataset).await.unwrap();
    assert_eq!(selected, sibling);
    let records = engine.inspect(&request.dataset).await.unwrap();
    assert!(
        records
            .values()
            .any(|value| value == &serde_json::to_value(&decision).unwrap())
    );
    let mut stream = engine.read_group(&selected, "length").await.unwrap();
    let mut lengths = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let values = batch
            .column_by_name("text_length")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        lengths.extend(values.iter());
    }
    lengths.sort();
    assert_eq!(lengths, [None, Some(0), Some(2), Some(2)]);
    let exported = engine
        .export(&export(&selected, "qualified-export", "length"))
        .await
        .unwrap();
    assert_eq!(exported.rows, 4);
    assert_eq!(
        engine
            .export(&export(&historical, "old-after-restore", "length"))
            .await
            .unwrap_err()
            .code(),
        "correction_required"
    );
}
