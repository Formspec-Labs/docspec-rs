//! A catalog epoch fences prepared work without changing immutable selections.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar::{fixture, id, input};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::model::{Authority, UpdateSpec};
use tower::ServiceExt;

fn post(epoch: u64) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/datasets/comments/authority/fence")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"expected_epoch":epoch}).to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn fence_preserves_selection_and_completions_and_reconciles_exact_retry() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = UpdateSpec {
        dataset: id("comments"),
        operation: id("old-worker"),
        definition: "v1".into(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: input(
            &root.path().join("replacement.parquet"),
            &[(Some(1), Some("corrected".into()))],
        ),
        deletions: None,
    };
    assert_eq!(
        engine
            .update_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let mut expected: Authority = serde_json::from_value(before["authority"].clone()).unwrap();
    assert_eq!(expected.epoch, 0);
    let router = docspec::http::router(engine.clone());
    let response = router.clone().oneshot(post(0)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 32).await.unwrap().as_ref(),
        b"1"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    expected.epoch = 1;
    assert_eq!(after["authority"], serde_json::to_value(expected).unwrap());
    for (key, value) in &before {
        if key != "authority" {
            assert_eq!(after.get(key), Some(value));
        }
    }
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    assert_eq!(
        engine.fence_operations(&request.dataset, 0).await.unwrap(),
        1
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), after);
    assert_eq!(
        engine.update(&request).await.unwrap_err().code(),
        "stale_eligibility"
    );
    let replacement = UpdateSpec {
        operation: id("new-worker"),
        ..request.clone()
    };
    let selected = engine.update(&replacement).await.unwrap();
    assert_ne!(selected.id, base.id);
    assert_eq!(
        engine.release(&request.dataset, &base.id).await.unwrap(),
        base
    );
    // A lost handoff response still reconciles after the new worker publishes.
    assert_eq!(
        engine.fence_operations(&request.dataset, 0).await.unwrap(),
        1
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), selected);
    assert_eq!(
        engine.fence_operations(&request.dataset, 1).await.unwrap(),
        2
    );
    assert_eq!(
        router.oneshot(post(0)).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        engine
            .fence_operations(&request.dataset, u64::MAX)
            .await
            .unwrap_err()
            .code(),
        "revision_exhausted"
    );
    assert_eq!(
        engine
            .fence_operations(&id("absent"), 0)
            .await
            .unwrap_err()
            .code(),
        "no_release"
    );
}
