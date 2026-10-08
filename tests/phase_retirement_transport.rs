//! CLI and HTTP retain the same exact phase-retirement decision.

#[path = "support/scalar.rs"]
mod scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    model::{PhaseRetirement, PhaseRetirementPlan},
};
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
async fn cli_and_http_share_exact_retirement_and_preserve_the_release() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, base, _) = fixture(root.path(), true).await;
    let selected = engine
        .backfill(&spec(
            &base,
            "derived",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let records = engine.inspect(&id("comments")).await.unwrap();
    let keys = vec![
        records
            .keys()
            .find(|key| key.starts_with("backfill-prepared/"))
            .unwrap()
            .clone(),
    ];
    let keys_path = root.path().join("keys.json");
    let limits_path = root.path().join("limits.json");
    let plan_path = root.path().join("retirement.json");
    std::fs::write(&keys_path, serde_json::to_vec(&keys).unwrap()).unwrap();
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args(["plan-phase-retirement", "comments", "prepared"])
        .arg(&keys_path)
        .arg("recovery no longer needed")
        .output()
        .unwrap();
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let cli_plan: PhaseRetirementPlan = serde_json::from_slice(&planned.stdout).unwrap();
    std::fs::write(&plan_path, &planned.stdout).unwrap();

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.inspect(&id("comments")).await.unwrap(), records);
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({
        "dataset": "comments",
        "retirement": "prepared",
        "keys": keys,
        "reason": "recovery no longer needed",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/phases/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: PhaseRetirementPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, cli_plan);

    let response = router
        .clone()
        .oneshot(post("/retention/phases/retire", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: PhaseRetirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let mut conflict = http_plan.clone();
    conflict.reason = "different decision".into();
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/phases/retire", &conflict))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/phases/plan", &request))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let mut missing = request.clone();
    missing["keys"] = serde_json::json!(["phase/missing/admission"]);
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/phases/plan", &missing))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut oversized = request;
    oversized["reason"] = serde_json::Value::String("x".repeat((1 << 20) + 1));
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/phases/plan", &oversized))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), selected);
    assert!(
        selected
            .dataset
            .groups
            .values()
            .flat_map(|table| &table.objects)
            .all(|object| std::path::Path::new(&object.path).is_file())
    );
    drop(router);
    drop(engine);

    let repeated = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-phases")
        .arg(&plan_path)
        .output()
        .unwrap();
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<PhaseRetirement>(&repeated.stdout).unwrap(),
        decision
    );
}
