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
            merge: None,
            relationship: None,
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

#[tokio::test]
async fn http_lifecycle_compaction_reports_baseline_revisions() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _base, _) = fixture(root.path(), true).await;
    let router = docspec::http::router(engine);

    let request = Request::builder()
        .method("POST")
        .uri("/datasets/comments/lifecycle/compact")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result: docspec::LifecycleCompactionResult =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert!(result.revisions_after <= result.revisions_before);
    assert!(result.rows_after <= result.rows_before);
    assert!(result.current_revision > 0);
}

#[tokio::test]
async fn http_routes_refuse_unknown_fields_with_422() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _base, _) = fixture(root.path(), true).await;
    let router = docspec::http::router(engine);

    for (path, mut payload) in [
        (
            "/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-1",
                "input": "in.parquet",
                "expected_digest": "a".repeat(64),
                "sql": "SELECT * FROM input",
                "definition": "v1",
            }),
        ),
        (
            "/updates/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-2",
                "definition": "v1",
                "base": "initial",
                "keys": ["id"],
                "replacements": {"path": "r.parquet", "digest": "a".repeat(64)},
            }),
        ),
        (
            "/schemas/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-3",
                "base": "initial",
                "definition": "v1",
                "next_definition": "v2",
                "fields": [{"id": 1, "name": "id"}],
            }),
        ),
        (
            "/backfills/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-4",
                "base": "initial",
                "definition": "v1",
                "group": "g1",
                "group_definition": "gd1",
                "fields": [{"id": 1, "name": "f", "expression": "1"}],
            }),
        ),
        (
            "/exports/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-5",
                "source": {
                    "dataset": "comments",
                    "version": "initial",
                    "manifest": {"owner": "comments", "path": "m.json", "digest": "a".repeat(64), "bytes": 10}
                },
                "channel": "public",
                "expected": null,
                "policy": {"group": "source", "rows": "all", "columns": ["id"]},
            }),
        ),
        (
            "/compactions/run",
            serde_json::json!({
                "dataset": "comments",
                "operation": "op-6",
                "version": "initial",
                "group": "source",
                "target_file_bytes": 1048576,
            }),
        ),
    ] {
        payload["unacknowledged_extra_field"] = serde_json::json!("refuse");
        let response = router.clone().oneshot(post(path, &payload)).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "route {path} must reject unknown fields"
        );
    }
}
