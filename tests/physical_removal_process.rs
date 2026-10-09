//! Real worker loss preserves exact removal intent and selected replacement values.
// Rust guideline compliant 2026-02-21

#![cfg(all(feature = "test-util", unix))]

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "shared scalar module also builds backfill fixtures"
)]
mod scalar;
#[expect(dead_code, reason = "shared harness also supports operation resume")]
mod support;

use arrow::array::{Int64Array, StringArray};
use docspec::{
    Engine,
    model::{PhaseResult, ReleasePin, RemovalOutcome, RemovalPlan, RemovalStatus, RunSpec},
};
use futures::TryStreamExt;
use scalar::{Row, id, input, limits};
use std::path::Path;

async fn run(engine: &Engine, root: &Path, operation: &str, rows: &[Row]) -> ReleasePin {
    let source = input(&root.join(format!("{operation}.parquet")), rows);
    engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id(operation),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap()
}

async fn retire(engine: &Engine) {
    let dataset = id("comments");
    let operation = id("old");
    let phases = engine
        .inspect(&dataset)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value).unwrap();
            (phase.operation == operation).then_some(key)
        })
        .collect::<Vec<_>>();
    assert!(!phases.is_empty());
    let availability = engine
        .plan_availability_retirement(
            &dataset,
            &id("availability"),
            &operation,
            "retire old result",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let phases = engine
        .plan_phase_retirement(&dataset, &id("phases"), &phases, "retire old phases")
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(&dataset, &id("recovery"), &operation, "retire old recovery")
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
}

async fn assert_selected(engine: &Engine, selected: &ReleasePin, expected: &[Row]) {
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), *selected);
    let mut stream = engine.read_group(selected, "source").await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        actual.extend(
            keys.iter()
                .zip(text.iter())
                .map(|(key, text)| (key, text.map(str::to_owned))),
        );
    }
    actual.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn sigkill_at_each_removal_boundary_resumes_exact_effects_in_a_fresh_cli() {
    for phase in ["removal-intent", "removal-registrations", "removal-unlink"] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        let old = run(
            &engine,
            root.path(),
            "old",
            &[(Some(1), Some("old".into()))],
        )
        .await;
        let object = old.dataset.groups["source"]
            .objects
            .iter()
            .find(|object| object.path.ends_with(".parquet"))
            .unwrap()
            .clone();
        // Include duplicates, nulls and Unicode so survival means exact values.
        let expected = vec![
            (Some(8), Some("é🎉".into())),
            (Some(8), Some("é🎉".into())),
            (None, None),
            (Some(9), Some(String::new())),
        ];
        let selected = run(&engine, root.path(), "replacement", &expected).await;
        retire(&engine).await;
        let plan: RemovalPlan = engine
            .plan_removal(
                &id("comments"),
                &id("removal"),
                std::slice::from_ref(&object),
                "retired output",
            )
            .await
            .unwrap();
        assert!(plan.inspection.objects[0].blocker.is_none());
        assert!(!plan.registrations.is_empty());
        drop(engine);

        support::kill_after(root.path(), &plan, "removal", phase);
        let (pending, _): (RemovalStatus, _) = support::run_cli(
            root.path(),
            &[
                "inspect-removal",
                "--dataset",
                "comments",
                "--removal",
                "removal",
            ],
        );
        assert!(!pending.complete);
        assert_eq!(pending.intent.plan, plan);
        assert_eq!(pending.progress.objects, vec![RemovalOutcome::Unresolved]);
        assert_eq!(Path::new(&object.path).exists(), phase != "removal-unlink");

        let (resumed, _): (RemovalStatus, _) = support::run_cli(
            root.path(),
            &[
                "resume-removal",
                "--dataset",
                "comments",
                "--removal",
                "removal",
            ],
        );
        assert!(resumed.complete);
        assert_eq!(resumed.intent, pending.intent);
        assert_eq!(resumed.progress.reserved_future_bytes, 0);
        assert!(!Path::new(&object.path).exists());
        if phase == "removal-unlink" {
            assert_eq!(resumed.progress.objects, vec![RemovalOutcome::Absent]);
            assert_eq!(resumed.confirmed_unlinked_bytes, 0);
            assert_eq!(resumed.reconciled_absent_bytes, object.bytes);
        } else {
            assert_eq!(resumed.progress.objects, vec![RemovalOutcome::Removed]);
            assert_eq!(resumed.confirmed_unlinked_bytes, object.bytes);
            assert_eq!(resumed.reconciled_absent_bytes, 0);
        }
        let registration_outcome = if phase == "removal-intent" {
            RemovalOutcome::Removed
        } else {
            RemovalOutcome::Absent
        };
        assert_eq!(
            resumed.progress.registrations,
            vec![registration_outcome; plan.registrations.len()]
        );
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        assert_selected(&engine, &selected, &expected).await;
        drop(engine);

        // A completed retry must return saved outcomes without hashing or unlinking
        // new bytes now occupying the old path.
        std::fs::write(&object.path, b"later unrelated bytes").unwrap();
        let (retried, _): (RemovalStatus, _) = support::run_cli(
            root.path(),
            &[
                "remove-objects",
                "--plan",
                root.path().join("request.json").to_str().unwrap(),
            ],
        );
        assert_eq!(retried, resumed);
        let (retried, _): (RemovalStatus, _) = support::run_cli(
            root.path(),
            &[
                "resume-removal",
                "--dataset",
                "comments",
                "--removal",
                "removal",
            ],
        );
        assert_eq!(retried, resumed);
        assert_eq!(
            std::fs::read(&object.path).unwrap(),
            b"later unrelated bytes"
        );
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        assert_selected(&engine, &selected, &expected).await;
    }
}
