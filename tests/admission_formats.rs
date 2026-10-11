//! Retained format-10 admissions keep their meaning across current-format cutovers.
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    engine::verify_release,
    model::{ReleasePin, RunSpec, SchemaField, SchemaSpec},
    storage,
};
use scalar::{fixture, id, limits, spec};
use std::{collections::BTreeMap, path::Path};

fn build(base: &ReleasePin, operation: &str) -> RunSpec {
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: None,
        relationship: None,
        input: base.dataset.inputs["input"].path.clone(),
        expected_digest: base.dataset.inputs["input"].digest.clone(),
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
    }
}

fn schema(base: &ReleasePin, operation: &str, next: &str) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        next_definition: next.into(),
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".into(),
            },
            SchemaField {
                id: 2,
                name: "body".into(),
            },
        ],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    }
}

// Reproduce the original serialized admission shape, not a historical binary.
// Operation and request still share their atomic commit revision.
pub(super) async fn original_admission(root: &Path, operation: &str, old_schema: bool) {
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let view = lifecycle.open(&id("comments")).await.unwrap();
    let records = lifecycle.records(&view).await.unwrap();
    let key = format!("operation/{operation}");
    let request_key = format!("request/{operation}");
    let mut admission = records[&key].clone();
    let object = admission.as_object_mut().unwrap();
    object.remove("result_format");
    object.remove("admission_fence");
    let mut changes = vec![];
    if old_schema {
        let mut authority = records["authority"].clone();
        let boundary = authority["boundary"].as_u64().unwrap() + 1;
        authority["boundary"] = boundary.into();
        admission["boundary"] = boundary.into();
        changes.push(record("authority", authority).unwrap());
    }
    changes.push(record(key, admission).unwrap());
    changes.push(record(&request_key, &records[&request_key]).unwrap());
    lifecycle.commit(&view, changes).await.unwrap();
}

#[tokio::test]
async fn original_admissions_resume_and_mixed_history_stays_readable() {
    let root = tempfile::tempdir().unwrap();
    let (engine, first, _) = fixture(root.path(), false).await;
    let pending = build(&first, "hidden-pending");
    assert_eq!(
        engine
            .run_until(&pending, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let request = build(&first, "old-build");
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    original_admission(root.path(), "old-build", false).await;
    let old = engine.run(&request).await.unwrap();
    assert_eq!(old.dataset.format, 10);

    let edit = schema(&old, "old-schema", "v2");
    assert_eq!(
        engine
            .schema_until(&edit, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    original_admission(root.path(), "old-schema", true).await;
    assert_eq!(
        engine
            .schema_until(&edit, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&edit.dataset).await.unwrap();
    let readback: ReleasePin =
        serde_json::from_value(before["readback/old-schema"].clone()).unwrap();
    assert_eq!(readback.dataset.format, 10);
    assert_eq!(readback.dataset.boundary, old.dataset.boundary + 1);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine.evolve_schema(&edit).await.unwrap_err().code(),
        "pending_source",
        "format-10 Schema must exclude its own admission but retain unrelated pending inputs"
    );
    assert_eq!(engine.latest(&edit.dataset).await.unwrap(), old);
    let refused = engine.inspect(&edit.dataset).await.unwrap();
    assert_eq!(refused["authority"]["definition"], "v1");
    assert!(!refused.contains_key("result/old-schema"));
    assert_eq!(
        refused["readback/old-schema"],
        before["readback/old-schema"]
    );
    engine
        .abandon(
            &pending.dataset,
            &pending.operation,
            "withdraw hidden input",
        )
        .await
        .unwrap();
    let old_schema = engine.evolve_schema(&edit).await.unwrap();
    assert_eq!(
        old_schema, readback,
        "resume must preserve the serialized interpretation"
    );

    let resumed = engine.inspect(&edit.dataset).await.unwrap();
    for (key, value) in &before {
        if key.starts_with("phase/") {
            assert_eq!(resumed[key], *value, "legacy Schema recomputed {key}");
        }
    }

    let mut backfill = spec(
        &old_schema,
        "new-backfill",
        "lengths",
        "text_length",
        "length(body)",
    );
    backfill.definition = "v2".into();
    let group = engine.backfill(&backfill).await.unwrap();
    assert_eq!(
        group.dataset.format, 12,
        "cloning an old pin must not inherit its release format"
    );
    assert_eq!(group.dataset.boundary, old_schema.dataset.boundary);
    let new = engine
        .evolve_schema(&schema(&group, "new-schema", "v3"))
        .await
        .unwrap();
    assert_eq!(new.dataset.format, 12);
    assert_eq!(new.dataset.boundary, group.dataset.boundary);
    for release in [&old, &old_schema, &group, &new] {
        assert_eq!(
            engine
                .release(&release.dataset.dataset, &release.id)
                .await
                .unwrap(),
            *release
        );
        verify_release(release, limits()).await.unwrap();
    }
}

#[tokio::test]
async fn original_backfill_does_not_silently_adopt_new_gap_rules() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let pending = build(&base, "pending");
    assert_eq!(
        engine
            .run_until(&pending, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let mut request = spec(
        &base,
        "old-backfill",
        "lengths",
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
    original_admission(root.path(), "old-backfill", false).await;
    engine
        .abandon(&pending.dataset, &pending.operation, "withdrawn")
        .await
        .unwrap();
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    assert_eq!(
        engine
            .plan_backfill(&request)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("stale_source_boundary")
    );
    request.operation = id("new-backfill");
    assert_eq!(
        engine.plan_backfill(&request).await.unwrap().files_pending,
        Some(0)
    );
    let release = engine.backfill(&request).await.unwrap();
    assert_eq!(release.dataset.format, 12);
    assert_eq!(release.dataset.boundary, base.dataset.boundary);
}
