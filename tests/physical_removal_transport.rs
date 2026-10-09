//! Removal transports share preview decisions, bounded requests, and saved-identity lookup.
// Rust guideline compliant 2026-02-21

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "shared fixture also exposes an unused backfill request builder"
)]
mod scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    model::{RemovalPlan, RemovalStatus},
};
use scalar::{fixture, limits};
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
async fn cli_http_preview_agrees_and_keeps_protected_bytes() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, selected, _rows) = fixture(root.path(), false).await;
    let object = selected.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    let objects_path = root.path().join("objects.json");
    let limits_path = root.path().join("limits.json");
    std::fs::write(&objects_path, serde_json::to_vec(&vec![&object]).unwrap()).unwrap();
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args([
            "plan-removal",
            "--dataset",
            "comments",
            "--removal",
            "transport",
        ])
        .arg("--objects")
        .arg(&objects_path)
        .args(["--reason", "inspect selected file protection"])
        .output()
        .unwrap();
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let cli_plan: RemovalPlan = serde_json::from_slice(&planned.stdout).unwrap();

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({
        "dataset": "comments",
        "removal": "transport",
        "objects": [&object],
        "reason": "inspect selected file protection",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/removal/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RemovalPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(cli_plan, http_plan);
    assert!(http_plan.inspection.objects[0].blocker.is_some());
    assert!(std::path::Path::new(&object.path).exists());

    // Executing a protected preview may complete an empty intersection, but
    // cannot turn an HTTP request into authority to remove selected bytes.
    let response = router
        .clone()
        .oneshot(post("/retention/removal/execute", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status: RemovalStatus =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert!(status.complete);
    assert!(status.intent.objects.is_empty());
    assert_eq!(status.confirmed_unlinked_bytes, 0);
    for (method, path) in [
        ("GET", "/datasets/comments/removals/transport"),
        ("POST", "/datasets/comments/removals/transport/resume"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let saved: RemovalStatus =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(saved, status);
    }
    assert!(std::path::Path::new(&object.path).exists());

    let mut oversized = request.clone();
    oversized["reason"] = serde_json::json!("x".repeat((1 << 20) + 1));
    let response = router
        .clone()
        .oneshot(post("/retention/removal/plan", &oversized))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let mut unknown = request;
    unknown["unexpected"] = serde_json::json!(true);
    let response = router
        .clone()
        .oneshot(post("/retention/removal/plan", &unknown))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    for (method, path) in [
        ("GET", "/datasets/comments/removals/missing"),
        ("POST", "/datasets/comments/removals/missing/resume"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let refusal: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(refusal["code"], "no_removal");
    }
    assert_eq!(
        engine.latest(&selected.dataset.dataset).await.unwrap(),
        selected
    );
}
