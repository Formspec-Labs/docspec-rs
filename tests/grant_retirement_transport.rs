//! CLI and HTTP share exact owner-grant retirement decisions and refusals.

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "This transport fixture does not need scalar backfill specs."
)]
mod scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    model::{ObjectRef, RetentionPlan, Retirement, RetirementScope, RunSpec},
    storage,
};
use scalar::{fixture, id, limits};
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
async fn grant_retirement_matches_cli_http_and_preserves_files_after_cold_retry() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, base, _) = fixture(root.path(), true).await;
    let source = &base.dataset.inputs["input"];
    engine
        .run(&RunSpec {
            dataset: id("consumer"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path.clone(),
            expected_digest: source.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let owner = id("comments");
    let records = engine.inspect(&owner).await.unwrap();
    let (live_key, live_grant) = records
        .iter()
        .find(|(key, _)| key.starts_with("owner-protection/"))
        .unwrap();
    let live_key = live_key.clone();
    let objects: Vec<ObjectRef> = serde_json::from_value(live_grant["objects"].clone()).unwrap();
    let retained_files: Vec<_> = objects
        .iter()
        .map(|object| {
            let metadata = std::fs::metadata(&object.path).unwrap();
            (
                object.path.clone(),
                metadata.len(),
                metadata.modified().unwrap(),
            )
        })
        .collect();

    // Create an unused grant only in this test's private catalog. A real live
    // grant supplies the native table identity and immutable owned objects.
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&workspace).await.unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let unused_consumer = id("unused-consumer");
    let view = lifecycle.open(&owner).await.unwrap();
    let mut unused_grant = live_grant.clone();
    unused_grant["consumer"] = serde_json::json!(unused_consumer);
    unused_grant["revision"] = serde_json::json!(view.revision + 1);
    let grant_digest =
        blake3::hash(&serde_json::to_vec(&(&owner, &unused_consumer, &objects)).unwrap());
    let unused_key = format!("owner-protection/{}", grant_digest.to_hex());
    lifecycle
        .commit(&view, vec![record(&unused_key, unused_grant).unwrap()])
        .await
        .unwrap();
    drop(lifecycle);
    let before = engine.inspect(&owner).await.unwrap();
    let limits_path = root.path().join("limits.json");
    let keys_path = root.path().join("grant-keys.json");
    let plan_path = root.path().join("grant-retirement.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    std::fs::write(&keys_path, serde_json::to_vec(&[&unused_key]).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args(["plan-grant-retirement", "comments", "unused-grant"])
        .arg(&keys_path)
        .arg("consumer no longer needs this grant")
        .output()
        .unwrap();
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let cli_plan: RetentionPlan = serde_json::from_slice(&planned.stdout).unwrap();
    assert_eq!(cli_plan.scope, RetirementScope::OwnerGrants);
    assert_eq!(cli_plan.records.len(), 1);
    assert_eq!(cli_plan.records[0].key, unused_key);
    std::fs::write(&plan_path, &planned.stdout).unwrap();

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.inspect(&owner).await.unwrap(), before);
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({
        "dataset": "comments",
        "retirement": "unused-grant",
        "keys": [unused_key],
        "reason": "consumer no longer needs this grant",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RetentionPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, cli_plan);

    let mut live_request = request.clone();
    live_request["retirement"] = serde_json::json!("live-grant");
    live_request["keys"] = serde_json::json!([live_key]);
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &live_request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "grant_in_use");

    let response = router
        .clone()
        .oneshot(post("/retention/grants/retire", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "grant_retired");
    let mut conflicting = http_plan;
    conflicting.reason = "different decision".into();
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/grants/retire", &conflicting))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(engine.latest(&owner).await.unwrap(), base);
    for (path, bytes, modified) in &retained_files {
        let metadata = std::fs::metadata(path).unwrap();
        assert_eq!(metadata.len(), *bytes);
        assert_eq!(metadata.modified().unwrap(), *modified);
    }
    drop(router);
    drop(engine);

    let repeated = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-grants")
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
}
