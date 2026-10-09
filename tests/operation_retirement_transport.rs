//! Operation recovery retirement agrees across transports and preserves selected data.
#![cfg(feature = "test-util")]

#[path = "support/scalar.rs"]
mod scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    model::{RetentionPlan, Retirement},
};
use futures::TryStreamExt;
use scalar::{fixture, id, limits, spec};
use tower::ServiceExt;

fn post(path: &str, value: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(value).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn cli_http_retirement_preserves_selected_values_and_reconciles_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, selected, rows) = fixture(root.path(), false).await;
    let expected: Vec<_> = engine
        .read_combined(&selected)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        expected.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        rows.len()
    );
    let pending = spec(
        &selected,
        "pending",
        "length",
        "text_length",
        "length(text)",
    );
    assert_eq!(
        engine
            .backfill_until(&pending, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let limits_path = root.path().join("limits.json");
    let plan_path = root.path().join("operation-retirement.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args([
            "plan-operation-retirement",
            "comments",
            "initial-recovery",
            "initial",
            "end operation recovery",
        ])
        .output()
        .unwrap();
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let cli_plan: RetentionPlan = serde_json::from_slice(&planned.stdout).unwrap();
    std::fs::write(&plan_path, &planned.stdout).unwrap();

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({
        "dataset": "comments",
        "retirement": "initial-recovery",
        "operation": "initial",
        "reason": "end operation recovery",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/operations/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RetentionPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, cli_plan);

    let mut active = request.clone();
    active["retirement"] = serde_json::json!("pending-recovery");
    active["operation"] = serde_json::json!("pending");
    let response = router
        .clone()
        .oneshot(post("/retention/operations/plan", &active))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "operation_active");

    let response = router
        .clone()
        .oneshot(post("/retention/operations/retire", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let mut replanning = request;
    replanning["retirement"] = serde_json::json!("fresh-recovery");
    let response = router
        .clone()
        .oneshot(post("/retention/operations/plan", &replanning))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "recovery_retired");
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), selected);
    assert_eq!(
        engine
            .release(&id("comments"), &id("initial"))
            .await
            .unwrap(),
        selected
    );
    drop(router);
    drop(engine);

    let repeated = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-operation")
        .arg(&plan_path)
        .output()
        .unwrap();
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Retirement>(&repeated.stdout).unwrap(),
        decision
    );

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let available = engine
        .release(&id("comments"), &id("initial"))
        .await
        .unwrap();
    let actual: Vec<_> = engine
        .read_combined(&available)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual, expected);
}
