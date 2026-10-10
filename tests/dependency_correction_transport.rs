//! Dependency correction transports share immutable decisions and structured refusals.

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    budget::Limits,
    model::{ComputedField, CorrectionDecision, DependencyCorrection, GroupDefinition, Id},
};
use tower::ServiceExt;

// Rust guideline compliant 2026-02-21

fn post(value: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/dependencies/corrections")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(value).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn http_and_cli_reconcile_the_same_decision_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
        ..Limits::default()
    };
    let correction = DependencyCorrection {
        dataset: Id::new("comments").unwrap(),
        id: Id::new("language-omission").unwrap(),
        definition: GroupDefinition {
            group: Id::new("classification").unwrap(),
            definition: "text-classifier-v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "label".into(),
                expression: "text".into(),
            }],
        },
        fields: [2].into_iter().collect(),
        reason: "language was material".into(),
    };
    let engine = Engine::open(&workspace, limits.clone()).await.unwrap();
    let router = docspec::http::router(engine.clone());
    let response = router.clone().oneshot(post(&correction)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: CorrectionDecision =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(decision.request, correction);
    assert!(decision.revision > 0);

    let mut conflicting = correction.clone();
    conflicting.fields.insert(3);
    let response = router.clone().oneshot(post(&conflicting)).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "correction_conflict");

    for (fields, reason, code) in [
        (vec![2], " ", "missing_reason"),
        (vec![], "language was material", "invalid_correction"),
        (vec![0], "language was material", "invalid_correction"),
    ] {
        let mut invalid = correction.clone();
        invalid.fields = fields.into_iter().collect();
        invalid.reason = reason.into();
        let response = router.clone().oneshot(post(&invalid)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let refusal: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(refusal["code"], code);
    }
    let mut unknown_field = serde_json::to_value(&correction).unwrap();
    unknown_field["unexpected"] = serde_json::json!(true);
    let response = router.oneshot(post(&unknown_field)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    drop(engine);

    let limits_path = root.path().join("limits.json");
    let correction_path = root.path().join("correction.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits).unwrap()).unwrap();
    std::fs::write(&correction_path, serde_json::to_vec(&correction).unwrap()).unwrap();
    let replay = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("correct-dependencies")
        .arg(&correction_path)
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<CorrectionDecision>(&replay.stdout).unwrap(),
        decision
    );

    std::fs::write(&correction_path, serde_json::to_vec(&conflicting).unwrap()).unwrap();
    let conflict = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("correct-dependencies")
        .arg(&correction_path)
        .output()
        .unwrap();
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("correction_conflict"));
}
