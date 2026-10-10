//! Scoped corrections qualify actual scalar reuse while preserving historical results.
#![cfg(feature = "test-util")]

#[path = "support/scalar.rs"]
mod scalar;

use arrow::array::StringArray;
use docspec::{
    Engine,
    model::{
        BackfillSpec, ColumnAddition, ColumnType, DependencyCorrection, ObjectRef, ReleasePin,
        SchemaField, SchemaSpec,
    },
    storage,
};
use futures::TryStreamExt;
use scalar::{fixture, id, spec};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

// Rust guideline compliant 2026-02-21

fn classifier(base: &ReleasePin, operation: &str) -> BackfillSpec {
    let mut request = spec(
        base,
        operation,
        "classification",
        "label",
        "CASE WHEN text IS NULL THEN NULL ELSE 'english' END",
    );
    request.definition = base.dataset.definition.clone();
    request.group_definition = "text-classifier-v1".into();
    request
}

fn correction(request: &BackfillSpec, name: &str, fields: &[i32]) -> DependencyCorrection {
    DependencyCorrection {
        dataset: request.dataset.clone(),
        id: id(name),
        definition: request.group_definition(),
        fields: fields.iter().copied().collect(),
        reason: "classification depended on the source language".into(),
    }
}

fn language_edit(base: &ReleasePin, operation: &str, language: &str) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        next_definition: operation.into(),
        // Omitting an earlier language identity and adding its name assigns a fresh ID.
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
            name: "language".into(),
            data_type: ColumnType::String,
            default: language.into(),
        }],
        write_defaults: BTreeMap::new(),
    }
}

async fn language_id(release: &ReleasePin) -> i32 {
    storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap()
        .metadata()
        .current_schema()
        .field_by_name("language")
        .unwrap()
        .id
}

fn payloads(release: &ReleasePin, group: &str) -> Vec<ObjectRef> {
    release.dataset.groups[group]
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .cloned()
        .collect()
}

fn file_completions(records: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

async fn assert_language(engine: &Engine, release: &ReleasePin, expected: &str) {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        let values = batch
            .column_by_name("language")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(values.iter().all(|value| value == Some(expected)));
        rows += batch.num_rows();
    }
    assert_eq!(rows as u64, release.dataset.groups["source"].rows);
    assert!(rows > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn correction_committed_at_actual_file_reuse_blocks_changed_language_context() {
    for apply_correction in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let english = engine
            .evolve_schema(&language_edit(&base, "english-schema", "en"))
            .await
            .unwrap();
        let old_language = language_id(&english).await;
        assert_language(&engine, &english, "en").await;
        let original = classifier(&english, "english-files");
        assert_eq!(
            engine
                .backfill_until(&original, "files")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let completed = file_completions(&engine.inspect(&original.dataset).await.unwrap());
        assert!(!completed.is_empty());
        assert_eq!(engine.latest(&original.dataset).await.unwrap(), english);

        let french = engine
            .evolve_schema(&language_edit(&english, "french-schema", "fr"))
            .await
            .unwrap();
        assert_ne!(language_id(&french).await, old_language);
        assert_eq!(payloads(&base, "source"), payloads(&english, "source"));
        assert_eq!(payloads(&english, "source"), payloads(&french, "source"));
        assert_language(&engine, &french, "fr").await;
        let adopted = classifier(&french, "adopted-files");
        let (reached, release) = engine
            .pause_scalar_file_reuse(adopted.dataset.clone(), adopted.operation.clone())
            .await;
        let worker_engine = engine.clone();
        let worker_request = adopted.clone();
        let mut worker = tokio::spawn(async move { worker_engine.backfill(&worker_request).await });
        // A bounded failure diagnoses a missing real cache hit without hanging the suite.
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                reached = reached => reached.expect("actual file cache hit must signal its pause"),
                result = &mut worker => panic!("worker returned before actual cache reuse: {result:?}"),
            }
        }).await.expect("actual scalar reuse pause was not reached");
        let before = engine.inspect(&adopted.dataset).await.unwrap();
        if apply_correction {
            engine
                .correct_dependencies(&correction(&original, "language-omission", &[old_language]))
                .await
                .unwrap();
            let after = engine.inspect(&adopted.dataset).await.unwrap();
            assert_eq!(after["authority"], before["authority"]);
            assert_eq!(file_completions(&after), completed);
            assert_eq!(engine.latest(&adopted.dataset).await.unwrap(), french);
        }
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .unwrap()
            .unwrap();
        if apply_correction {
            assert_eq!(result.unwrap_err().code(), "correction_mismatch");
            assert_eq!(engine.latest(&adopted.dataset).await.unwrap(), french);
            assert_eq!(
                file_completions(&engine.inspect(&adopted.dataset).await.unwrap()),
                completed
            );
            let mut fixed = classifier(&french, "explicitly-corrected");
            fixed.group_definition = "language-aware-classifier-v2".into();
            fixed.fields[0].expression = "CASE WHEN language = 'en' THEN 'english' WHEN language = 'fr' THEN 'french' ELSE 'unknown' END".into();
            let corrected = engine.backfill(&fixed).await.unwrap();
            let mut stream = engine
                .read_group(&corrected, "classification")
                .await
                .unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let labels = batch
                    .column_by_name("label")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(labels.iter().all(|value| value == Some("french")));
            }
        } else {
            let selected = result.unwrap();
            assert_eq!(
                selected.dataset.groups["source"],
                french.dataset.groups["source"]
            );
            assert_eq!(
                file_completions(&engine.inspect(&adopted.dataset).await.unwrap()),
                completed
            );
            let origins = selected.dataset.computed["classification"]
                .origins
                .as_ref()
                .unwrap();
            let originals: Value =
                serde_json::from_slice(&std::fs::read(&origins.path).unwrap()).unwrap();
            for file in originals["files"].as_array().unwrap() {
                assert_eq!(
                    file["completion"],
                    completed[file["generation"]["key"].as_str().unwrap()]
                );
            }
        }
    }
}

#[tokio::test]
async fn inherited_sibling_selection_adds_cumulative_proof_without_rewriting_values() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let source = engine
        .evolve_schema(&language_edit(&base, "language-schema", "en"))
        .await
        .unwrap();
    let language = language_id(&source).await;
    let original_request = classifier(&source, "classified");
    let original = engine.backfill(&original_request).await.unwrap();
    let original_group = original.dataset.computed["classification"].clone();
    let classified_table = original.dataset.groups["classification"].clone();
    let completions = file_completions(&engine.inspect(&original_request.dataset).await.unwrap());
    let first = engine
        .correct_dependencies(&correction(
            &original_request,
            "language-omission",
            &[language],
        ))
        .await
        .unwrap();
    assert_eq!(engine.backfill(&original_request).await.unwrap(), original);
    let mut sibling_request = spec(&original, "sibling", "sibling", "next_id", "id + 1");
    sibling_request.definition = original.dataset.definition.clone();
    let sibling = engine.backfill(&sibling_request).await.unwrap();
    let supplemented = &sibling.dataset.computed["classification"];
    assert_eq!(supplemented.origins, original_group.origins);
    assert_eq!(supplemented.input_fields, original_group.input_fields);
    assert_eq!(supplemented.request, original_group.request);
    assert_eq!(sibling.dataset.groups["classification"], classified_table);
    let proof = supplemented.qualification.as_ref().unwrap();
    assert_eq!(proof.fields, BTreeSet::from([language]));
    assert_eq!(proof.first_revision, first.revision);
    assert!(sibling.dataset.computed["sibling"].qualification.is_none());

    engine
        .correct_dependencies(&correction(&original_request, "identifier-omission", &[1]))
        .await
        .unwrap();
    let mut second_request = spec(&sibling, "second-sibling", "second", "double_id", "id * 2");
    second_request.definition = sibling.dataset.definition.clone();
    let second = engine.backfill(&second_request).await.unwrap();
    let proof = second.dataset.computed["classification"]
        .qualification
        .as_ref()
        .unwrap();
    assert_eq!(proof.fields, BTreeSet::from([1, language]));
    assert_eq!(proof.first_revision, first.revision);
    assert_eq!(
        second.dataset.computed["classification"].origins,
        original_group.origins
    );
    assert_eq!(second.dataset.groups["classification"], classified_table);
    assert_eq!(
        second.dataset.groups["sibling"],
        sibling.dataset.groups["sibling"]
    );
    let after = engine.inspect(&original_request.dataset).await.unwrap();
    for (key, value) in completions {
        assert_eq!(after[&key], value);
    }
}

#[tokio::test]
async fn correction_after_readback_requires_new_operation_and_reuses_originals() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let source = engine
        .evolve_schema(&language_edit(&base, "language-schema", "en"))
        .await
        .unwrap();
    let pending = classifier(&source, "readback-before-correction");
    assert_eq!(
        engine
            .backfill_until(&pending, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&pending.dataset).await.unwrap();
    let (readback_key, saved) = before
        .iter()
        .find(|(key, _)| key.starts_with("backfill-readback/readback-before-correction/"))
        .unwrap();
    let old: ReleasePin = serde_json::from_value(saved.clone()).unwrap();
    engine
        .correct_dependencies(&correction(
            &pending,
            "language-omission",
            &[language_id(&source).await],
        ))
        .await
        .unwrap();
    assert_eq!(
        engine
            .plan_backfill(&pending)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("correction_required")
    );
    assert_eq!(
        engine.backfill(&pending).await.unwrap_err().code(),
        "correction_required"
    );
    assert_eq!(engine.latest(&pending.dataset).await.unwrap(), source);
    let mut fresh = pending.clone();
    fresh.operation = id("qualified-association");
    let plan = engine.plan_backfill(&fresh).await.unwrap();
    assert_eq!(plan.files_pending, Some(0));
    assert_eq!(
        plan.promotion_refusal.as_deref(),
        Some("correction_required")
    );
    let qualified = engine.backfill(&fresh).await.unwrap();
    assert_eq!(
        qualified.dataset.groups["classification"],
        old.dataset.groups["classification"]
    );
    assert_eq!(
        qualified.dataset.computed["classification"].origins,
        old.dataset.computed["classification"].origins
    );
    assert!(
        qualified.dataset.computed["classification"]
            .qualification
            .is_some()
    );
    let after = engine.inspect(&pending.dataset).await.unwrap();
    assert_eq!(after[readback_key], *saved);
    assert_eq!(file_completions(&after), file_completions(&before));
}

#[tokio::test]
async fn missing_original_field_is_unknown_and_new_deficient_execution_is_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let historical = classifier(&base, "files-without-language");
    assert_eq!(
        engine
            .backfill_until(&historical, "files")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let source = engine
        .evolve_schema(&language_edit(&base, "language-schema", "en"))
        .await
        .unwrap();
    let language = language_id(&source).await;
    engine
        .correct_dependencies(&correction(&historical, "language-omission", &[language]))
        .await
        .unwrap();
    let adopted = classifier(&source, "unknown-original-language");
    assert_eq!(
        engine.backfill(&adopted).await.unwrap_err().code(),
        "correction_unknown"
    );
    assert_eq!(engine.latest(&adopted.dataset).await.unwrap(), source);

    let mut deficient = classifier(&source, "new-deficient-execution");
    deficient.group_definition = "another-text-only-classifier".into();
    engine
        .correct_dependencies(&correction(
            &deficient,
            "new-definition-omission",
            &[language],
        ))
        .await
        .unwrap();
    assert_eq!(
        engine.backfill(&deficient).await.unwrap_err().code(),
        "correction_mismatch"
    );
    assert_eq!(engine.latest(&deficient.dataset).await.unwrap(), source);
    assert!(!file_completions(&engine.inspect(&deficient.dataset).await.unwrap()).is_empty());
}
