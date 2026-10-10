//! Retiring one recovery generation preserves independent results and later work.
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    engine::verify_release,
    model::{
        Id, JoinCardinality, JoinDefinition, JoinField, JoinKey, JoinKind, JoinSide, JoinTransform,
        NullMatching, PhaseResult, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform,
        ReleasePin, Retirement, RunSpec, TablePin,
    },
};
use scalar::{fixture, id, input, limits, spec};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

fn request(root: &Path, dataset: &str) -> RunSpec {
    let source = input(
        &root.join(format!("{dataset}.parquet")),
        &[
            (Some(1), Some("same".into())),
            (Some(1), Some("same".into())),
            (Some(2), None),
            (None, Some(String::new())),
        ],
    );
    RunSpec {
        dataset: id(dataset),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

async fn open(root: &Path) -> Engine {
    Engine::open(root.join("workspace"), limits())
        .await
        .unwrap()
}

fn phase(records: &BTreeMap<String, Value>, name: &str) -> (String, PhaseResult) {
    records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/") && !key.starts_with("phase/backfill/"))
        .find_map(|(key, value)| {
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.phase == name).then(|| (key.clone(), phase))
        })
        .unwrap()
}

fn key_with_prefix(records: &BTreeMap<String, Value>, prefix: &str) -> String {
    records
        .keys()
        .find(|key| key.starts_with(prefix))
        .unwrap()
        .clone()
}

async fn retire(engine: &Engine, dataset: &Id, name: &str, keys: &[String]) -> Retirement {
    let plan = engine
        .plan_phase_retirement(dataset, &id(name), keys, "discard this recovery generation")
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap()
}

#[tokio::test]
async fn processing_recovers_after_admission_retirement_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), "comments");
    let engine = open(root.path()).await;
    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let (admission_key, admission) = phase(&before, "admission");
    let (processing_key, processing) = phase(&before, "processing");
    let decision = retire(
        &engine,
        &request.dataset,
        "admission",
        std::slice::from_ref(&admission_key),
    )
    .await;
    let input = &admission.outputs[0];
    let inspection = engine
        .inspect_retention(std::slice::from_ref(input))
        .await
        .unwrap();
    assert_eq!(
        inspection.objects[0].blocker.as_ref().unwrap().key,
        processing_key
    );
    assert_eq!(inspection.unmatched_bytes, 0);
    std::fs::remove_file(&request.input).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(
        engine.retire_phases(&decision.plan).await.unwrap(),
        decision
    );
    let plan = engine.plan(&request).await.unwrap();
    assert!(
        !plan
            .required_phases
            .iter()
            .any(|phase| phase == "admission")
    );
    assert!(
        plan.reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let release = engine.run(&request).await.unwrap();
    assert_eq!(release.dataset.groups["source"], processing.table.unwrap());
    assert_eq!(release.dataset.inputs["input"], *input);
    let after = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(after[&admission_key], before[&admission_key]);
    assert_eq!(after[&processing_key], before[&processing_key]);
    assert_eq!(
        after["retirement/admission"],
        serde_json::to_value(&decision).unwrap()
    );
    for (key, value) in before
        .iter()
        .filter(|(key, _)| key.starts_with("protection/"))
    {
        assert_eq!(
            after[key], *value,
            "retirement must not close attempt reservations"
        );
    }
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn exact_generation_retries_do_not_retire_recomputed_work() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), "comments");
    let engine = open(root.path()).await;
    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let (key, processing) = phase(&before, "processing");
    let candidate = processing
        .table
        .as_ref()
        .unwrap()
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&candidate))
        .await
        .unwrap();
    assert_eq!(inspection.objects[0].blocker.as_ref().unwrap().key, key);
    let stale = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("stale"),
            std::slice::from_ref(&key),
            "older captured decision",
        )
        .await
        .unwrap();
    let mut forged = stale.clone();
    forged.retirement = id("unknown-native-boundary");
    forged.snapshot_id = i64::MAX;
    assert_eq!(
        engine.retire_phases(&forged).await.unwrap_err().code(),
        "invalid_retirement"
    );
    let retired = retire(
        &engine,
        &request.dataset,
        "retire-first",
        std::slice::from_ref(&key),
    )
    .await;
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&candidate))
        .await
        .unwrap();
    assert!(
        inspection.objects[0]
            .blocker
            .as_ref()
            .unwrap()
            .key
            .starts_with("protection/")
    );
    assert_eq!(
        std::fs::metadata(&candidate.path).unwrap().len(),
        candidate.bytes
    );
    let mut conflict = retired.plan.clone();
    conflict.reason = "a different decision under the same identity".into();
    assert_eq!(
        engine.retire_phases(&conflict).await.unwrap_err().code(),
        "retirement_conflict"
    );

    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let fresh = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("fresh"),
            std::slice::from_ref(&key),
            "inspect new generation",
        )
        .await
        .unwrap();
    assert!(fresh.records[0].revision > retired.plan.records[0].revision);
    assert_eq!(fresh.records[0].key, key);
    assert_eq!(
        engine.retire_phases(&stale).await.unwrap_err().code(),
        "stale_retirement"
    );
    assert_eq!(engine.retire_phases(&retired.plan).await.unwrap(), retired);
    assert!(
        engine
            .plan(&request)
            .await
            .unwrap()
            .reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let regenerated = phase(
        &engine.inspect(&request.dataset).await.unwrap(),
        "processing",
    )
    .1;
    assert_ne!(
        regenerated.table.as_ref().unwrap().metadata,
        processing.table.as_ref().unwrap().metadata
    );
    let release = engine.run(&request).await.unwrap();
    assert_eq!(release.dataset.groups["source"], regenerated.table.unwrap());
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn validation_requires_explicit_retirement_after_processing_replacement() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), "comments");
    let engine = open(root.path()).await;
    assert_eq!(
        engine
            .run_until(&request, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let (processing_key, old_processing) = phase(&before, "processing");
    let (validation_key, old_validation) = phase(&before, "validation");
    retire(&engine, &request.dataset, "processing", &[processing_key]).await;
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "phase_generation_conflict"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    let new_processing = phase(&after, "processing").1;
    assert_ne!(new_processing.table, old_processing.table);
    assert_eq!(phase(&after, "validation").1, old_validation);
    assert!(!after.contains_key("result/initial"));
    retire(&engine, &request.dataset, "validation", &[validation_key]).await;
    let release = engine.run(&request).await.unwrap();
    assert_eq!(
        release.dataset.groups["source"],
        new_processing.table.unwrap()
    );
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn retained_readback_selects_after_upstream_retirement_without_source() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), "comments");
    let engine = open(root.path()).await;
    assert_eq!(
        engine
            .run_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let retained: ReleasePin = serde_json::from_value(before["readback/initial"].clone()).unwrap();
    let keys = before
        .keys()
        .filter(|key| key.starts_with("phase/"))
        .cloned()
        .collect::<Vec<_>>();
    retire(&engine, &request.dataset, "upstream", &keys).await;
    assert_eq!(
        engine
            .plan_phase_retirement(
                &request.dataset,
                &id("readback"),
                &["readback/initial".into()],
                "discard recovery",
            )
            .await
            .unwrap_err()
            .code(),
        "invalid_retirement"
    );
    std::fs::remove_file(&request.input).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert!(
        engine
            .plan(&request)
            .await
            .unwrap()
            .required_phases
            .is_empty()
    );
    assert_eq!(engine.run(&request).await.unwrap(), retained);
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), retained);
    let after = engine.inspect(&request.dataset).await.unwrap();
    for key in &keys {
        assert_eq!(
            after[key], before[key],
            "readback recovery must not repeat upstream work"
        );
    }
    let mut candidates = retained
        .dataset
        .inputs
        .values()
        .cloned()
        .collect::<Vec<_>>();
    candidates.extend(retained.dataset.groups["source"].objects.clone());
    candidates.push(retained.manifest.clone());
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    assert!(
        inspection
            .objects
            .iter()
            .all(|entry| entry.blocker.is_some())
    );
    verify_release(&retained, limits()).await.unwrap();
}

#[tokio::test]
async fn scalar_preparation_cannot_reuse_validation_from_a_retired_generation() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "derived", "length", "text_length", "length(text)");
    assert_eq!(
        engine
            .backfill_until(&request, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let prepared_key = key_with_prefix(&before, "backfill-prepared/");
    let validation_key = key_with_prefix(&before, "backfill-validation/");
    let old_prepared: TablePin = serde_json::from_value(before[&prepared_key].clone()).unwrap();
    retire(
        &engine,
        &request.dataset,
        "prepared",
        std::slice::from_ref(&prepared_key),
    )
    .await;
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "phase_generation_conflict"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    let new_prepared: TablePin = serde_json::from_value(after[&prepared_key].clone()).unwrap();
    assert_ne!(new_prepared.metadata, old_prepared.metadata);
    assert_eq!(after[&validation_key], before[&validation_key]);
    for (key, value) in before
        .iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
    {
        assert_eq!(after[key], *value, "file computation must remain reusable");
    }
    retire(&engine, &request.dataset, "validation", &[validation_key]).await;
    let release = engine.backfill(&request).await.unwrap();
    assert_eq!(release.dataset.groups["length"], new_prepared);
    let after = engine.inspect(&request.dataset).await.unwrap();
    let keys = after
        .keys()
        .filter(|key| {
            key.starts_with("phase/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
        })
        .cloned()
        .collect::<Vec<_>>();
    retire(&engine, &request.dataset, "remaining", &keys).await;
    assert_eq!(
        engine.release(&request.dataset, &base.id).await.unwrap(),
        base
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), release);
    let mut candidates = base.dataset.inputs.values().cloned().collect::<Vec<_>>();
    candidates.extend(base.dataset.groups["source"].objects.clone());
    candidates.extend(release.dataset.groups["length"].objects.clone());
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn evidence_requires_explicit_retirement_after_source_replacement() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let left = engine.run(&request(root.path(), "comments")).await.unwrap();
    let right = engine.run(&request(root.path(), "dockets")).await.unwrap();
    let request = RefreshSpec {
        dataset: id("joined"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Left,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
                fields: vec![JoinField {
                    side: JoinSide::Left,
                    field: "text".into(),
                    name: "text".into(),
                }],
            },
        }),
    };
    assert_eq!(
        engine
            .refresh_until(&request, "evidence")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let (processing_key, old_processing) = phase(&before, "processing");
    let (evidence_key, evidence) = phase(&before, "evidence");
    retire(&engine, &request.dataset, "processing", &[processing_key]).await;
    assert_eq!(
        engine.refresh(&request).await.unwrap_err().code(),
        "phase_generation_conflict"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    let new_processing = phase(&after, "processing").1;
    assert_ne!(new_processing.table, old_processing.table);
    assert_eq!(phase(&after, "evidence").1, evidence);
    retire(&engine, &request.dataset, "evidence", &[evidence_key]).await;
    let release = engine.refresh(&request).await.unwrap();
    assert_eq!(
        release.dataset.groups["source"],
        new_processing.table.unwrap()
    );
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn retained_scalar_readback_recomposes_with_a_new_sibling_after_retirement() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let target = spec(&base, "target", "length", "text_length", "length(text)");
    let sibling = spec(&base, "sibling", "next", "next_id", "id + 1");
    assert_eq!(
        engine
            .backfill_until(&target, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&target.dataset).await.unwrap();
    let readback_key = key_with_prefix(&before, "backfill-readback/target/");
    let retained: ReleasePin = serde_json::from_value(before[&readback_key].clone()).unwrap();
    let keys = before
        .keys()
        .filter(|key| {
            key.starts_with("phase/backfill/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!keys.is_empty());
    retire(&engine, &target.dataset, "target-upstream", &keys).await;
    let target_metadata = &retained.dataset.groups["length"].metadata;
    let protected = engine
        .inspect_retention(std::slice::from_ref(target_metadata))
        .await
        .unwrap();
    assert_eq!(
        protected.objects[0].blocker.as_ref().unwrap().key,
        readback_key
    );
    let sibling_release = engine.backfill(&sibling).await.unwrap();
    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(
        engine.plan_backfill(&target).await.unwrap().files_pending,
        Some(0)
    );
    let completed = engine.backfill(&target).await.unwrap();
    assert_eq!(
        completed.dataset.groups["length"],
        retained.dataset.groups["length"]
    );
    assert_eq!(
        completed.dataset.computed["length"],
        retained.dataset.computed["length"]
    );
    assert_eq!(
        completed.dataset.groups["next"],
        sibling_release.dataset.groups["next"]
    );
    assert_eq!(
        completed.dataset.computed["next"],
        sibling_release.dataset.computed["next"]
    );
    assert_eq!(
        completed.dataset.parent_manifest,
        Some(sibling_release.manifest.clone())
    );
    assert_ne!(
        completed.manifest, retained.manifest,
        "only combined release metadata changes"
    );
    assert_eq!(engine.latest(&target.dataset).await.unwrap(), completed);
    assert_eq!(engine.backfill(&sibling).await.unwrap(), sibling_release);
    let after = engine.inspect(&target.dataset).await.unwrap();
    assert_eq!(after[&readback_key], before[&readback_key]);
    for key in &keys {
        assert_eq!(
            after[key], before[key],
            "retired target computation must not run again"
        );
    }
    verify_release(&completed, limits()).await.unwrap();
}
