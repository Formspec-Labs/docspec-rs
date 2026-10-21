//! Explicit abandonment fences retries and preserves independently reusable work.
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    model::{Abandonment, PhaseResult, ReleasePin, RunSpec, SchemaField, SchemaSpec, UpdateSpec},
};
use futures::TryStreamExt;
use scalar::{fixture, id, input, limits, spec};
use std::{collections::BTreeMap, path::Path};
use tempfile::TempDir;

fn build(root: &Path, operation: &str) -> RunSpec {
    let input = input(&root.join("replacement.parquet"), &[(Some(9000), None)]);
    RunSpec {
        dataset: id("comments"),
        operation: id(operation),
        base: None,
        relationship: None,
        input: input.path,
        expected_digest: input.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

fn schema(base: &ReleasePin, operation: &str) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        next_definition: "v2".into(),
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".into(),
            },
            SchemaField {
                id: 2,
                name: "body".into(),
            },
        ],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    }
}

#[tokio::test]
async fn abandonment_survives_reopen_and_preserves_shared_phases_and_readback() {
    for stop in ["admission", "processing", "readback"] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let mut request = build(root.path(), "stopped");
        assert_eq!(
            engine.run_until(&request, stop).await.unwrap_err().code(),
            "injected_failure"
        );
        let before = engine.inspect(&request.dataset).await.unwrap();
        let processing = before.values().find_map(|value| {
            serde_json::from_value::<PhaseResult>(value.clone())
                .ok()
                .filter(|phase| phase.phase == "processing" && phase.operation == request.operation)
                .and_then(|phase| phase.table)
        });
        std::fs::remove_file(&request.input).unwrap();
        let decision = engine
            .abandon(
                &request.dataset,
                &request.operation,
                "input selection withdrawn",
            )
            .await
            .unwrap();
        assert_eq!(decision.operation, request.operation);
        assert!(decision.operation_revision > 0);
        assert_eq!(
            decision.request_digest,
            before["operation/stopped"]["request_digest"]
                .as_str()
                .unwrap()
        );
        let mut after = engine.inspect(&request.dataset).await.unwrap();
        assert_eq!(
            after.remove("abandoned/stopped").unwrap(),
            serde_json::to_value(&decision).unwrap()
        );
        assert_eq!(
            before, after,
            "abandonment must preserve authority, phases, releases and protections"
        );
        assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        assert_eq!(
            engine
                .abandon(&request.dataset, &request.operation, "different decision")
                .await
                .unwrap_err()
                .code(),
            "abandonment_conflict"
        );
        drop(engine);

        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_eq!(
            engine
                .abandon(
                    &request.dataset,
                    &request.operation,
                    "input selection withdrawn"
                )
                .await
                .unwrap(),
            decision
        );
        assert_eq!(
            engine.run(&request).await.unwrap_err().code(),
            "operation_abandoned"
        );
        assert_eq!(
            engine.plan(&request).await.unwrap_err().code(),
            "operation_abandoned"
        );
        if stop == "readback" {
            let retained: ReleasePin =
                serde_json::from_value(before["readback/stopped"].clone()).unwrap();
            let mut stream = engine.read_group(&retained, "source").await.unwrap();
            assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 1);
            assert!(stream.try_next().await.unwrap().is_none());
        }
        request.operation = id("replacement-request");
        let planned = engine.plan(&request).await.unwrap();
        assert!(planned.reusable_phases.contains(&"admission".to_owned()));
        let release = engine.run(&request).await.unwrap();
        assert_eq!(release.dataset.groups["source"].rows, 1);
        if let Some(processing) = processing {
            assert_eq!(release.dataset.groups["source"], processing);
        }
        assert_eq!(
            engine
                .abandon(&request.dataset, &request.operation, "too late")
                .await
                .unwrap_err()
                .code(),
            "operation_completed"
        );
        assert_eq!(engine.run(&request).await.unwrap(), release);
    }
}

#[tokio::test]
async fn abandoned_backfill_preserves_file_reuse_and_refuses_every_retry() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let mut request = spec(&base, "stopped", "lengths", "text_length", "length(text)");
    assert_eq!(
        engine
            .backfill_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    engine
        .abandon(&request.dataset, &request.operation, "use a new request")
        .await
        .unwrap();
    assert_eq!(
        engine.plan_backfill(&request).await.unwrap_err().code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "operation_abandoned"
    );
    request.operation = id("replacement");
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.files_pending, Some(0));
    assert!(plan.files_completed.is_some_and(|files| files > 0));
    let release = engine.backfill(&request).await.unwrap();
    assert!(release.dataset.groups.contains_key("lengths"));
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in before.iter().filter(|(key, _)| {
        key.starts_with("phase/backfill/") || key.starts_with("backfill-prepared/")
    }) {
        assert_eq!(after[key], *value);
    }
}

#[tokio::test]
async fn abandonment_fences_updates_and_schema_without_rewinding_source_admission() {
    for is_schema in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let schema_request = schema(&base, "stopped");
        let update = UpdateSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("stopped"),
            base: base.id.clone(),
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: input(&root.path().join("update.parquet"), &[(Some(9000), None)]),
            deletions: None,
            relationship: None,
        };
        let stopped = if is_schema {
            engine.schema_until(&schema_request, "readback").await
        } else {
            engine.update_until(&update, "readback").await
        };
        assert_eq!(stopped.unwrap_err().code(), "injected_failure");
        let before = engine.inspect(&base.dataset.dataset).await.unwrap();
        engine
            .abandon(&base.dataset.dataset, &id("stopped"), "withdrawn")
            .await
            .unwrap();
        let after = engine.inspect(&base.dataset.dataset).await.unwrap();
        assert_eq!(before["authority"], after["authority"]);
        assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
        let (planned, resumed) = if is_schema {
            (
                engine.plan_schema(&schema_request).await,
                engine.evolve_schema(&schema_request).await,
            )
        } else {
            (
                engine.plan_update(&update).await,
                engine.update(&update).await,
            )
        };
        assert_eq!(planned.unwrap_err().code(), "operation_abandoned");
        assert_eq!(resumed.unwrap_err().code(), "operation_abandoned");
        let mut next_schema = schema_request;
        next_schema.operation = id("next-schema");
        assert!(engine.plan_schema(&next_schema).await.is_ok());
        let release = engine.evolve_schema(&next_schema).await.unwrap();
        assert_eq!(release.dataset.boundary, base.dataset.boundary);
        assert_eq!(release.dataset.inputs, base.dataset.inputs);
        let selected = engine.inspect(&base.dataset.dataset).await.unwrap();
        assert_eq!(
            selected["authority"]["boundary"],
            before["authority"]["boundary"]
        );
        let files = |pin: &ReleasePin| {
            pin.dataset.groups["source"]
                .objects
                .iter()
                .filter(|object| object.path.ends_with(".parquet"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(files(&release), files(&base));
        docspec::engine::verify_release(&release, limits())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn schema_requires_every_uncaptured_source_to_be_abandoned() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    for operation in ["first", "second"] {
        assert_eq!(
            engine
                .run_until(&build(root.path(), operation), "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
    }
    engine
        .abandon(&base.dataset.dataset, &id("first"), "withdrawn")
        .await
        .unwrap();
    let request = schema(&base, "edit");
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(
        engine.evolve_schema(&request).await.unwrap_err().code(),
        "pending_source"
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
    engine
        .abandon(&base.dataset.dataset, &id("second"), "withdrawn")
        .await
        .unwrap();
    let release = engine.evolve_schema(&request).await.unwrap();
    assert_eq!(release.dataset.boundary, base.dataset.boundary);
    assert_eq!(release.dataset.inputs, base.dataset.inputs);
}

#[tokio::test]
async fn backfill_prepares_during_pending_input_and_selects_after_its_abandonment() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let pending = build(root.path(), "pending");
    assert_eq!(
        engine
            .run_until(&pending, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let request = spec(&base, "lengths", "lengths", "text_length", "length(text)");
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.promotion_refusal.as_deref(), Some("pending_source"));
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "pending_source"
    );
    let prepared = engine.inspect(&request.dataset).await.unwrap();
    let completed = prepared
        .iter()
        .filter(|(key, _)| {
            key.starts_with("phase/backfill/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("backfill-readback/")
        })
        .collect::<Vec<_>>();
    assert!(!completed.is_empty());
    assert!(
        completed
            .iter()
            .any(|(key, _)| key.starts_with("backfill-readback/"))
    );
    engine
        .abandon(&request.dataset, &pending.operation, "withdrawn")
        .await
        .unwrap();
    drop(engine);
    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    std::fs::remove_file(&pending.input).unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.promotion_refusal, None);
    assert_eq!(plan.files_pending, Some(0));
    let release = engine.backfill(&request).await.unwrap();
    assert_eq!(release.dataset.boundary, base.dataset.boundary);
    assert_eq!(
        release.dataset.groups["source"],
        base.dataset.groups["source"]
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in completed {
        assert_eq!(&after[key], value);
    }
    docspec::engine::verify_release(&release, limits())
        .await
        .unwrap();
}

#[tokio::test]
async fn abandoning_a_later_admission_does_not_revive_an_old_cutover_capture() {
    for is_schema in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let mut edit = schema(&base, "captured");
        let mut backfill = spec(&base, "captured", "lengths", "text_length", "length(text)");
        let stopped = if is_schema {
            engine.schema_until(&edit, "readback").await
        } else {
            engine.backfill_until(&backfill, "readback").await
        };
        assert_eq!(stopped.unwrap_err().code(), "injected_failure");
        let pending = build(root.path(), "later");
        assert_eq!(
            engine
                .run_until(&pending, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        engine
            .abandon(&pending.dataset, &pending.operation, "withdrawn")
            .await
            .unwrap();
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let stale = if is_schema {
            engine.evolve_schema(&edit).await
        } else {
            engine.backfill(&backfill).await
        };
        assert_eq!(stale.unwrap_err().code(), "stale_source_boundary");
        assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
        edit.operation = id("fresh");
        backfill.operation = id("fresh");
        let release = if is_schema {
            engine.evolve_schema(&edit).await.unwrap()
        } else {
            assert_eq!(
                engine.plan_backfill(&backfill).await.unwrap().files_pending,
                Some(0)
            );
            engine.backfill(&backfill).await.unwrap()
        };
        assert_eq!(release.dataset.boundary, base.dataset.boundary);
    }
}

#[tokio::test]
async fn http_and_cli_share_the_durable_decision() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let (engine, _, _) = fixture(root.path(), false).await;
    let request = build(root.path(), "stopped");
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let router = docspec::http::router(engine.clone());
    let post = |operation: &str, reason: &str| {
        Request::builder()
            .method("POST")
            .uri(format!("/datasets/comments/operations/{operation}/abandon"))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({"reason": reason})).unwrap(),
            ))
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(post("missing", "unused"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        router
            .clone()
            .oneshot(post("stopped", " "))
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let response = router
        .clone()
        .oneshot(post("stopped", "withdrawn"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: Abandonment =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(
        router
            .oneshot(post("stopped", "changed"))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    drop(engine);
    let command = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .args(["abandon", "comments", "stopped", "withdrawn"])
        .output()
        .unwrap();
    assert!(
        command.status.success(),
        "{}",
        String::from_utf8_lossy(&command.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Abandonment>(&command.stdout).unwrap(),
        decision
    );
}
