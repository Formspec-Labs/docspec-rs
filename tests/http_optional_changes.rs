//! Optional change evidence retires through saved HTTP plans independently of results.

use super::scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::model::{CertifiedChanges, RetentionPlan, Retirement, RetirementScope, UpdateSpec};
use scalar::{fixture, id, input};
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
async fn saved_http_plan_retires_optional_changes_and_reconciles_retry() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), true).await;
    let updated = engine
        .update(&UpdateSpec {
            dataset: id("comments"),
            operation: id("updated"),
            definition: "v1".into(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: input(
                &root.path().join("replacement.parquet"),
                &[(Some(1), Some("new text".into()))],
            ),
            deletions: None,
        })
        .await
        .unwrap();
    let version = updated.version_ref();
    let router = docspec::http::router(engine.clone());

    let response = router
        .clone()
        .oneshot(post("/changes/inspect", &version))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let certified: Option<CertifiedChanges> =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let certified = certified.unwrap();
    assert_eq!(certified.certificate.before, base.version_ref());
    assert_eq!(certified.certificate.after, version);

    let request = serde_json::json!({
        "dataset": "comments",
        "retirement": "optional-history",
        "versions": ["updated"],
        "reason": "end optional acceleration history",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/changes/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let saved_bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let plan_path = root.path().join("change-retirement.json");
    std::fs::write(&plan_path, &saved_bytes).unwrap();
    let saved: RetentionPlan = serde_json::from_slice(&std::fs::read(&plan_path).unwrap()).unwrap();
    assert_eq!(saved.scope, RetirementScope::OptionalChanges);
    assert_eq!(saved.records.len(), 1);
    assert_eq!(saved.records[0].key, "change-certificate/updated");
    assert_eq!(saved.records[0].revision, certified.revision);

    let response = router
        .clone()
        .oneshot(post("/retention/changes/retire", &saved))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let retired: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(retired.plan, saved);

    let response = router
        .clone()
        .oneshot(post("/changes/inspect", &version))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let certified: Option<CertifiedChanges> =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(certified, None);
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), updated);

    let response = router
        .clone()
        .oneshot(post("/retention/changes/retire", &saved))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let repeated: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(repeated, retired);

    let response = router
        .oneshot(post("/retention/changes/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "changes_retired");
}
