//! Terminal recovery retirement preserves independent results and rejects stale identities.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 8 << 20,
        ..Limits::default()
    }
}

pub(in crate::engine::retention) async fn fixture(root: &Path) -> (Engine, RunSpec, ReleasePin) {
    let input = root.join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    let request = RunSpec {
        dataset: id("comments"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&input).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let result = engine.run(&request).await.unwrap();
    (engine, request, result)
}

async fn plan(engine: &Engine, request: &RunSpec) -> RetentionPlan {
    engine
        .plan_operation_retirement(
            &request.dataset,
            &id("retire"),
            &request.operation,
            "end operation recovery",
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn lost_response_concurrent_retry_and_cold_replay_preserve_the_selected_result() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, result) = fixture(root.path()).await;
    let plan = plan(&engine, &request).await;
    let before = engine.inspect(&request.dataset).await.unwrap();
    let lifecycle = &engine.inner.lifecycle;
    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (left, right) = tokio::join!(
        engine.retire_operation(&plan),
        engine.retire_operation(&plan)
    );
    let decision = left.unwrap();
    assert_eq!(right.unwrap(), decision);
    let view = lifecycle.open(&request.dataset).await.unwrap();
    assert_eq!(view.revision, plan.revision + 1);
    let rows = lifecycle.rows(&view).await.unwrap();
    for generation in &plan.records {
        assert_eq!(rows.revision(&generation.key), Some(generation.revision));
    }
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in before {
        assert_eq!(
            after[&key], value,
            "retirement must append rather than rewrite {key}"
        );
    }
    assert_eq!(
        engine
            .retained_protections(&view, "request-dependencies/initial")
            .await
            .err()
            .unwrap()
            .code(),
        "retained_claim_retired"
    );
    assert!(
        engine
            .retained_protections(&view, "result/initial")
            .await
            .unwrap()
            .is_some()
    );
    drop(rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.retire_operation(&plan).await.unwrap(), decision);
    assert_eq!(engine.run(&request).await.unwrap(), result);
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), result);
    assert_eq!(
        engine
            .plan_operation_retirement(
                &request.dataset,
                &id("another"),
                &request.operation,
                "another decision",
            )
            .await
            .unwrap_err()
            .code(),
        "recovery_retired"
    );
    let inventory = engine
        .inspect_retention(std::slice::from_ref(&result.manifest))
        .await
        .unwrap();
    assert!(inventory.objects[0].blocker.is_some());
}

#[tokio::test]
async fn execution_rechecks_the_complete_attempt_set_and_canonical_owner() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, _) = fixture(root.path()).await;
    let original = plan(&engine, &request).await;
    let mut incomplete = original.clone();
    incomplete
        .records
        .retain(|record| record.key != "request-dependencies/initial");
    assert_eq!(
        engine
            .retire_operation(&incomplete)
            .await
            .unwrap_err()
            .code(),
        "stale_retirement"
    );
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&request.dataset).await.unwrap();
    let attempt = Protection {
        id: id("late-attempt"),
        consumer: request.dataset.clone(),
        purpose: "initial/processing".into(),
        paths: vec![format!(
            "{}/",
            engine
                .inner
                .root
                .join("objects/comments/late-attempt")
                .display()
        )],
        retired: false,
    };
    lifecycle
        .commit(
            &view,
            vec![record("protection/late-attempt", &attempt).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        engine.retire_operation(&original).await.unwrap_err().code(),
        "stale_retirement"
    );
    let fresh = plan(&engine, &request).await;
    assert!(
        fresh
            .records
            .iter()
            .any(|record| record.key == "protection/late-attempt")
    );
    for corruption in ["owner", "path", "id", "purpose", "retired"] {
        let mut bad = attempt.clone();
        match corruption {
            "owner" => bad.consumer = id("foreign"),
            "path" => bad.paths.push("/another/".into()),
            "id" => bad.id = id("different"),
            "purpose" => bad.purpose = "initial/invalid/purpose".into(),
            "retired" => bad.retired = true,
            _ => unreachable!(),
        }
        let view = lifecycle.open(&request.dataset).await.unwrap();
        lifecycle
            .commit(&view, vec![record("protection/late-attempt", bad).unwrap()])
            .await
            .unwrap();
        assert_eq!(
            engine
                .plan_operation_retirement(
                    &request.dataset,
                    &id("invalid"),
                    &request.operation,
                    "invalid attempt",
                )
                .await
                .unwrap_err()
                .code(),
            "invalid_retirement",
            "{corruption}"
        );
    }
}

#[tokio::test]
async fn otherwise_consistent_selection_cannot_complete_a_different_request() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, mut result) = fixture(root.path()).await;
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&request.dataset).await.unwrap();
    let link: Succession = lifecycle
        .get(&view, "succession/initial")
        .await
        .unwrap()
        .unwrap();
    let OperationRequest::Build(ref mut stored) = result.dataset.request else {
        unreachable!()
    };
    stored.sql = "SELECT id + 100 AS id FROM input".into();
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("result/initial", result).unwrap(),
                record("succession/initial", &link).unwrap(),
            ],
        )
        .await
        .unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    // Existing ordering checks intentionally read only ordering identity. Ending
    // operation recovery also needs the exact admitted request.
    super::super::super::history::check_link_receipt(&rows, view.revision, &link).unwrap();
    assert_eq!(
        engine
            .plan_operation_retirement(
                &request.dataset,
                &id("invalid"),
                &request.operation,
                "wrong completion",
            )
            .await
            .unwrap_err()
            .code(),
        "invalid_retirement"
    );
}

#[tokio::test]
async fn saved_recovery_validation_reserves_original_metadata_on_retry() {
    let root = tempfile::tempdir().unwrap();
    let (engine, request, _) = fixture(root.path()).await;
    let plan = plan(&engine, &request).await;
    let decision = engine.retire_operation(&plan).await.unwrap();
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let budget = &engine.inner.lifecycle.budget;
    // Occupy the unused pool in bounded chunks. A metadata retry must refuse
    // before decoding original requests when no allowance remains.
    let mut held = Vec::new();
    for chunk in [1 << 20, 4096, 1] {
        while let Ok(permit) = budget.reserve(chunk) {
            held.push(permit);
        }
    }
    assert_eq!(
        validate_saved(&rows, &view, &decision, &engine)
            .unwrap_err()
            .code(),
        "capacity"
    );
    let revision = rows.revision("operation/initial").unwrap();
    assert_eq!(
        record_retired(&rows, &view, "operation/initial", revision, budget)
            .unwrap_err()
            .code(),
        "capacity"
    );
    // The usual live-record lookup needs no decoding or reservation.
    assert!(
        !record_retired(
            &rows,
            &view,
            "result/initial",
            rows.revision("result/initial").unwrap(),
            budget
        )
        .unwrap()
    );
    drop(held);
    validate_saved(&rows, &view, &decision, &engine).unwrap();
}
