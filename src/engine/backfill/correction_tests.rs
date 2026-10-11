//! Retained scalar targets remain immutable when corrections race with adoption.
#![cfg(feature = "test-util")]

use super::*;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

// Rust guideline compliant 2026-02-21

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

fn input(root: &Path) -> RunSpec {
    let path = root.join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("language", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["en", "en"])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: id("readback-correction"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT value, language FROM input".into(),
        definition: "v1".into(),
    }
}

#[tokio::test]
async fn fresh_readback_view_fences_target_proof_changes_but_allows_siblings() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let build = input(root.path());
    let base = engine.run(&build).await.unwrap();
    let spec = BackfillSpec {
        dataset: build.dataset.clone(),
        operation: id("target"),
        base: base.id.clone(),
        definition: build.definition.clone(),
        group: id("calculated"),
        group_definition: "scalar-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "next_value".into(),
            expression: "value + 1".into(),
        }],
    };
    let lifecycle = &engine.inner.lifecycle;
    // Two attempts can both observe no readback before either acknowledges one.
    let before_readback = lifecycle.open(&spec.dataset).await.unwrap();
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained_view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&retained_view).await.unwrap();
    let prefix = format!("backfill-readback/{}/", spec.operation);
    let (key, revision, json) = rows.prefix(&prefix).next().unwrap();
    let saved_key = key.to_owned();
    let saved_json = json.to_owned();
    let saved_revision = revision;
    let saved: ReleasePin = serde_json::from_str(json).unwrap();
    let saved_manifest = std::fs::read(&saved.manifest.path).unwrap();
    drop(rows);
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );

    // A real sibling selection changes composition without replacing the target.
    let sibling = BackfillSpec {
        operation: id("sibling"),
        group: id("additional"),
        fields: vec![ComputedField {
            id: 11,
            name: "another_value".into(),
            expression: "value + 2".into(),
        }],
        ..spec.clone()
    };
    let sibling_release = engine.backfill(&sibling).await.unwrap();
    let mut composed = saved.dataset.clone();
    composed.parent_manifest = Some(sibling_release.manifest.clone());
    composed.groups.insert(
        sibling.group.to_string(),
        sibling_release.dataset.groups[sibling.group.as_str()].clone(),
    );
    composed.computed.insert(
        sibling.group.to_string(),
        sibling_release.dataset.computed[sibling.group.as_str()].clone(),
    );
    {
        let lock = engine.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        engine
            .check_backfill_target(&view, &spec, &composed)
            .await
            .unwrap();
    }

    let source = &saved.dataset.groups["source"];
    let native = storage::open_pin(source, &lifecycle.budget).await.unwrap();
    let language_id = native
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|field| field.name == "language")
        .unwrap()
        .id;
    let correction = DependencyCorrection {
        dataset: spec.dataset.clone(),
        id: id("language-omission"),
        definition: spec.group_definition(),
        fields: BTreeSet::from([language_id]),
        reason: "language is a material applicability condition".into(),
    };
    let decision = engine.correct_dependencies(&correction).await.unwrap();
    let mut changed_target = saved.dataset.clone();
    let target = changed_target
        .computed
        .get_mut(spec.group.as_str())
        .unwrap();
    target.qualification = Some(Box::new(DependencyQualification {
        checker: super::super::corrections::CHECKER.into(),
        scope: super::super::corrections::records::scope_key(&correction.definition).unwrap(),
        fields: correction.fields.clone(),
        first_revision: decision.revision,
        origins: target.origins.clone().unwrap(),
        target: source.into(),
    }));
    // The candidate differs only in supplemental proof. The proof is valid for
    // the old files, so this is a readback-identity refusal, not a field mismatch.
    let mut checks = super::super::verification::Checks::composition();
    checks
        .scalar_qualification(
            &changed_target.computed[spec.group.as_str()],
            source,
            &changed_target.groups[spec.group.as_str()],
            &lifecycle.budget,
        )
        .await
        .unwrap();
    engine
        .check_backfill_target(&before_readback, &spec, &changed_target)
        .await
        .unwrap();
    let final_revision = {
        let lock = engine.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        engine
            .guard_corrections(&view, &changed_target, None)
            .await
            .unwrap();
        assert_eq!(
            engine
                .check_backfill_target(&view, &spec, &changed_target)
                .await
                .unwrap_err()
                .code(),
            "correction_required"
        );
        engine
            .check_backfill_target(&view, &spec, &composed)
            .await
            .unwrap();
        view.revision
    };
    let final_view = lifecycle.open(&spec.dataset).await.unwrap();
    assert_eq!(final_view.revision, final_revision);
    let rows = lifecycle.rows(&final_view).await.unwrap();
    let readbacks: Vec<_> = rows.prefix(&prefix).collect();
    assert_eq!(readbacks.len(), 1);
    assert_eq!(
        readbacks[0],
        (saved_key.as_str(), saved_revision, saved_json.as_str())
    );
    assert_eq!(std::fs::read(&saved.manifest.path).unwrap(), saved_manifest);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), sibling_release);
}
