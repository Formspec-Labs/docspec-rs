//! Compaction recovery retirement agrees across transports and preserves selected data.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;

use arrow::array::{Int64Array, StringArray};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    model::{CompactionResult, CompactionSpec, ReleasePin, RetentionPlan, Retirement},
};
use futures::TryStreamExt;
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

async fn selected_rows(engine: &Engine, release: &ReleasePin) -> Vec<scalar::Row> {
    let mut stream = engine.read_combined(release).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        rows.extend(
            ids.iter()
                .zip(text.iter())
                .map(|(id, text)| (id, text.map(str::to_owned))),
        );
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn cli_http_compaction_retirement_preserves_reads_and_reconciles_exact_retry() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, selected, mut expected) = fixture(root.path(), false).await;
    expected.sort();
    let spec = CompactionSpec {
        dataset: id("comments"),
        operation: id("compact"),
        version: selected.id.clone(),
        group: "source".into(),
        files: vec![],
        target_file_bytes: 16 << 20,
    };
    let compacted = engine.compact(&spec).await.unwrap();
    assert!(compacted.verified);
    assert!(compacted.input_files.len() > compacted.output_files.len());
    let limits_path = root.path().join("limits.json");
    let plan_path = root.path().join("compaction-retirement.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args([
            "plan-compaction-retirement",
            "comments",
            "compact-recovery",
            "compact",
            "end compaction recovery",
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
        "retirement": "compact-recovery",
        "operation": "compact",
        "reason": "end compaction recovery",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/compactions/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RetentionPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, cli_plan);

    // Planning preserves the existing recovery result and selected data.
    let response = router
        .clone()
        .oneshot(post("/compactions/run", &spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<CompactionResult>(
            &to_bytes(response.into_body(), 1 << 20).await.unwrap()
        )
        .unwrap(),
        compacted
    );
    assert_eq!(selected_rows(&engine, &selected).await, expected);

    let response = router
        .clone()
        .oneshot(post("/retention/compactions/retire", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    drop(router);
    drop(engine);

    let repeated = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-compaction")
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
    let router = docspec::http::router(engine.clone());
    let response = router
        .oneshot(post("/compactions/run", &spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "compaction_retired");
    let current = engine.latest(&id("comments")).await.unwrap();
    assert_eq!(current, selected);
    assert_eq!(selected_rows(&engine, &current).await, expected);
}
