//! Compaction retirement reconciles uncertain commits and rejects forged scope.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::engine::retention::recovery::tests::fixture;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn request(release: &ReleasePin) -> CompactionSpec {
    CompactionSpec {
        dataset: release.dataset.dataset.clone(),
        operation: id("layout"),
        version: release.id.clone(),
        group: "source".into(),
        files: vec![],
        target_file_bytes: 16 << 20,
    }
}

async fn plan(engine: &Engine, spec: &CompactionSpec) -> RetentionPlan {
    engine
        .plan_compaction_retirement(
            &spec.dataset,
            &id("retire"),
            &spec.operation,
            "end rewrite recovery",
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn lost_retirement_reply_and_concurrent_retry_keep_one_decision_and_current_layout() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _, release) = fixture(root.path()).await;
    let spec = request(&release);
    let compacted = engine.compact(&spec).await.unwrap();
    let plan = plan(&engine, &spec).await;
    let before = engine.inspect(&spec.dataset).await.unwrap();
    let lifecycle = &engine.inner.lifecycle;
    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (first, retry) = tokio::join!(
        engine.retire_compaction(&plan),
        engine.retire_compaction(&plan)
    );
    let decision = first.unwrap();
    assert_eq!(retry.unwrap(), decision);
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    assert_eq!(view.revision, plan.revision + 1);
    let after = engine.inspect(&spec.dataset).await.unwrap();
    for (key, value) in before {
        assert_eq!(after[&key], value);
    }
    assert_eq!(
        engine.compact(&spec).await.unwrap_err().code(),
        "compaction_retired"
    );
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), release);
    assert_eq!(
        engine
            .representation_pin(
                &view,
                &release.id,
                "source",
                &release.dataset.groups["source"]
            )
            .await
            .unwrap(),
        compacted.replacement
    );
    let mut changed = plan.clone();
    changed.reason = "different decision".into();
    assert_eq!(
        engine.retire_compaction(&changed).await.unwrap_err().code(),
        "retirement_conflict"
    );
}

#[tokio::test]
async fn forged_compaction_receipt_cannot_end_another_attempt_or_omit_completed_work() {
    for foreign_attempt in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, _, release) = fixture(root.path()).await;
        let spec = request(&release);
        engine.compact(&spec).await.unwrap();
        let mut plan = plan(&engine, &spec).await;
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        if foreign_attempt {
            let (key, revision, _) = rows
                .prefix("protection/")
                .find(|(_, _, json)| {
                    let attempt: Protection = serde_json::from_str(json).unwrap();
                    attempt.purpose == "initial/processing"
                })
                .unwrap();
            plan.records.push(RetainedGeneration {
                key: key.into(),
                revision,
            });
            plan.records.sort();
        } else {
            plan.records
                .retain(|record| record.key != "compaction-prepared/layout");
        }
        assert_eq!(
            engine.retire_compaction(&plan).await.unwrap_err().code(),
            "stale_retirement"
        );
        // Simulate a malformed stored receipt with internally matching markers.
        // Scope-specific validation must still refuse it before any cleanup.
        let decision = Retirement {
            plan: plan.clone(),
            revision: view.revision + 1,
        };
        let mut records = vec![record("retirement/retire", &decision).unwrap()];
        for generation in &plan.records {
            records.push(
                record(
                    retirement::marker_key(generation),
                    retirement::RetiredRecord {
                        dataset: spec.dataset.clone(),
                        table_uuid: plan.table_uuid.clone(),
                        record: generation.clone(),
                        retirement: plan.retirement.clone(),
                    },
                )
                .unwrap(),
            );
        }
        lifecycle.commit(&view, records).await.unwrap();
        assert_eq!(
            engine
                .inspect_retention(std::slice::from_ref(&release.manifest))
                .await
                .unwrap_err()
                .code(),
            "invalid_retirement"
        );
        assert!(Path::new(&release.manifest.path).exists());
    }
}

#[tokio::test]
async fn compaction_helper_grants_logical_metadata_without_obsolete_payload_in_selection_scope() {
    use crate::engine::retention::owners::OwnerProtection;
    use std::collections::BTreeMap;

    let root = tempfile::tempdir().unwrap();
    let (engine, _, release) = fixture(root.path()).await;
    let source = &release.dataset.groups["source"];
    let compacted = engine.compact(&request(&release)).await.unwrap();
    assert_eq!(compacted.source, *source);
    assert_eq!(compacted.replacement.logical_metadata(), &source.metadata);
    assert!(!compacted.replacement.objects.contains(&source.metadata));
    assert!(compacted.input_files.iter().all(|path| {
        !compacted
            .replacement
            .objects
            .iter()
            .any(|object| &object.path == path)
    }));

    // This directly qualifies the helper's grants from genuine native pins.
    // It does not claim a consumer workflow has selected a foreign source.
    let recovery_consumer = id("recovery-reader");
    let selection_consumer = id("selection-reader");
    let recovery = engine
        .protect_compaction_table(&recovery_consumer, source, Some(&release.manifest))
        .await
        .unwrap();
    let selection = engine
        .protect_compaction_table(&selection_consumer, &compacted.replacement, None)
        .await
        .unwrap();
    let records = engine.inspect(&release.dataset.dataset).await.unwrap();
    let granted = |consumer: &Id| {
        records
            .iter()
            .filter(|(key, _)| key.starts_with("owner-protection/"))
            .map(|(_, value)| serde_json::from_value::<OwnerProtection>(value.clone()).unwrap())
            .filter(|grant| &grant.consumer == consumer)
            .flat_map(|grant| {
                assert_eq!(grant.owner, release.dataset.dataset);
                grant.objects
            })
            .map(|object| (object.path.clone(), object))
            .collect::<BTreeMap<_, _>>()
    };
    let mut expected_recovery = source
        .objects
        .iter()
        .cloned()
        .map(|object| (object.path.clone(), object))
        .collect::<BTreeMap<_, _>>();
    expected_recovery.insert(source.metadata.path.clone(), source.metadata.clone());
    expected_recovery.insert(release.manifest.path.clone(), release.manifest.clone());
    assert_eq!(granted(&recovery_consumer), expected_recovery);
    let mut expected_selection = compacted
        .replacement
        .objects
        .iter()
        .cloned()
        .map(|object| (object.path.clone(), object))
        .collect::<BTreeMap<_, _>>();
    expected_selection.insert(source.metadata.path.clone(), source.metadata.clone());
    let selected_grant = granted(&selection_consumer);
    assert_eq!(selected_grant, expected_selection);
    assert_eq!(selected_grant[&source.metadata.path], source.metadata);
    assert!(!selected_grant.contains_key(&release.manifest.path));
    for old_payload in &compacted.input_files {
        assert!(!selected_grant.contains_key(old_payload));
        assert!(granted(&recovery_consumer).contains_key(old_payload));
    }
    drop(selection);
    drop(recovery);
}
