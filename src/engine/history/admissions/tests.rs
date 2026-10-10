//! Metadata-only fixtures exercise hidden admissions and forged history.
// Rust guideline compliant 2026-02-21

use super::*;
use serde_json::json;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn workspace() -> (tempfile::TempDir, Engine, Id, View) {
    let root = tempfile::TempDir::new().unwrap();
    let engine = Engine::open(
        root.path(),
        Limits {
            compute_threads: 1,
            memory_bytes: 256 << 20,
            scratch_bytes: 1 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 256 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let dataset = id("source");
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    (root, engine, dataset, view)
}

fn admission(dataset: &Id, name: &str, boundary: u64) -> (OperationRequest, Operation) {
    let request = OperationRequest::Build(RunSpec {
        dataset: dataset.clone(),
        operation: id(name),
        base: None,
        relationship: None,
        input: "/absent/source.parquet".into(),
        expected_digest: "0".repeat(64),
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    });
    let operation = Operation {
        request_digest: fingerprint(&request).unwrap(),
        eligibility: 0,
        epoch: 0,
        boundary,
        admission_fence: None,
        result_format: FORMAT,
        definition: "v1".into(),
        base: None,
        base_manifest: None,
        schema_key: None,
        require_current_boundary: false,
    };
    (request, operation)
}

async fn admit(
    engine: &Engine,
    view: &View,
    request: &OperationRequest,
    operation: &Operation,
) -> View {
    engine
        .inner
        .lifecycle
        .commit(
            view,
            vec![
                record(format!("operation/{}", request.operation()), operation).unwrap(),
                record(format!("request/{}", request.operation()), request).unwrap(),
                record(
                    "authority",
                    json!({"boundary": operation.boundary, "eligibility": 0}),
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap()
}

fn result(request: &OperationRequest, operation: &Operation) -> serde_json::Value {
    json!({
        "id": request.operation(), "verified": true,
        "manifest": {"owner": request.dataset(), "path": "/absent/release.json", "digest": "0".repeat(64), "bytes": 1},
        "dataset": {"dataset": request.dataset(), "version": request.operation(),
            "boundary": operation.boundary, "eligibility": operation.eligibility,
            "format": operation.result_format, "request": request}
    })
}

async fn select(
    engine: &Engine,
    view: &View,
    request: &OperationRequest,
    operation: &Operation,
    predecessor: Option<VersionRef>,
) -> View {
    let release = result(request, operation);
    let manifest: ObjectRef = serde_json::from_value(release["manifest"].clone()).unwrap();
    let link = Succession {
        selected: VersionRef {
            dataset: request.dataset().clone(),
            version: request.operation().clone(),
            manifest,
        },
        predecessor,
        boundary: operation.boundary,
        eligibility: operation.eligibility,
    };
    engine.inner.lifecycle.commit(view, vec![
        record(format!("result/{}", request.operation()), &release).unwrap(),
        record(format!("succession/{}", request.operation()), link).unwrap(),
        record("authority", json!({"boundary": operation.boundary, "eligibility": operation.eligibility,
            "selected": release["dataset"], "release": release, "selection_revision": view.revision + 1})).unwrap(),
    ]).await.unwrap()
}

async fn abandon(
    engine: &Engine,
    view: &View,
    request: &OperationRequest,
    operation: &Operation,
    admission_revision: i64,
) -> View {
    engine
        .inner
        .lifecycle
        .commit(
            view,
            vec![
                record(
                    format!("abandoned/{}", request.operation()),
                    Abandonment {
                        dataset: request.dataset().clone(),
                        operation: request.operation().clone(),
                        request_digest: operation.request_digest.clone(),
                        operation_revision: admission_revision,
                        reason: "superseded source".into(),
                    },
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn later_full_selection_does_not_hide_earlier_preparation_or_stale_request() {
    let (_root, engine, dataset, view) = workspace().await;
    let (mut early, mut operation) = admission(&dataset, "early", 1);
    if let OperationRequest::Build(spec) = &mut early {
        spec.base = Some(id("stale"));
    }
    operation.base = Some(id("stale"));
    operation.request_digest = fingerprint(&early).unwrap();
    let view = admit(&engine, &view, &early, &operation).await;
    let admission_revision = view.revision;
    let view = engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record("phase/early", json!({"completed": true})).unwrap(),
                record("readback/early", json!({"verified": true})).unwrap(),
                record("preparation/early", json!({"base": "stale"})).unwrap(),
            ],
        )
        .await
        .unwrap();
    let (late, late_operation) = admission(&dataset, "late", 2);
    let view = admit(&engine, &view, &late, &late_operation).await;
    let view = select(&engine, &view, &late, &late_operation, None).await;
    assert_eq!(
        engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap(),
        vec![id("early")]
    );
    let view = abandon(&engine, &view, &early, &operation, admission_revision).await;
    assert!(
        engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn selected_chain_remains_covered_with_retired_unavailable_payloads() {
    let (_root, engine, dataset, view) = workspace().await;
    let (first, first_operation) = admission(&dataset, "first", 1);
    let view = admit(&engine, &view, &first, &first_operation).await;
    let view = select(&engine, &view, &first, &first_operation, None).await;
    let release = result(&first, &first_operation);
    let previous = VersionRef {
        dataset: dataset.clone(),
        version: id("first"),
        manifest: serde_json::from_value(release["manifest"].clone()).unwrap(),
    };
    let (second, second_operation) = admission(&dataset, "second", 2);
    // Preserve the actual selected head across an admission, as production does.
    let authority = engine
        .inner
        .lifecycle
        .rows(&view)
        .await
        .unwrap()
        .get_record::<serde_json::Value>("authority")
        .unwrap()
        .unwrap()
        .2;
    let view = admit(&engine, &view, &second, &second_operation).await;
    let mut authority = authority;
    authority["boundary"] = json!(2);
    let view = engine
        .inner
        .lifecycle
        .commit(&view, vec![record("authority", authority).unwrap()])
        .await
        .unwrap();
    let view = select(&engine, &view, &second, &second_operation, Some(previous)).await;
    let view = engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![record("availability/first", json!({"retired": true})).unwrap()],
        )
        .await
        .unwrap();
    assert!(
        engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_orphan_result_and_forged_selected_receipt() {
    for selected in [false, true] {
        let (_root, engine, dataset, view) = workspace().await;
        let (request, operation) = admission(&dataset, "first", 1);
        let view = admit(&engine, &view, &request, &operation).await;
        let view = if selected {
            let mut forged = operation.clone();
            forged.result_format = PREVIOUS_FORMAT;
            select(&engine, &view, &request, &forged, None).await
        } else {
            engine
                .inner
                .lifecycle
                .commit(
                    &view,
                    vec![record("result/first", result(&request, &operation)).unwrap()],
                )
                .await
                .unwrap()
        };
        assert_eq!(
            engine
                .pending_source_operations(&view, &dataset)
                .await
                .unwrap_err()
                .code(),
            "invalid_succession"
        );
    }
}

#[tokio::test]
async fn rejects_missing_and_duplicate_source_boundaries() {
    for duplicate in [false, true] {
        let (_root, engine, dataset, view) = workspace().await;
        let (first, first_operation) = admission(&dataset, "first", if duplicate { 1 } else { 2 });
        let view = admit(&engine, &view, &first, &first_operation).await;
        let view = if duplicate {
            let (second, second_operation) = admission(&dataset, "second", 1);
            admit(&engine, &view, &second, &second_operation).await
        } else {
            view
        };
        assert_eq!(
            engine
                .pending_source_operations(&view, &dataset)
                .await
                .unwrap_err()
                .code(),
            "invalid_succession"
        );
    }
}

#[tokio::test]
async fn rejects_abandonment_not_bound_to_original_admission() {
    let (_root, engine, dataset, view) = workspace().await;
    let (request, operation) = admission(&dataset, "first", 1);
    let view = admit(&engine, &view, &request, &operation).await;
    let view = abandon(&engine, &view, &request, &operation, view.revision + 1).await;
    assert_eq!(
        engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap_err()
            .code(),
        "invalid_succession"
    );
}

#[tokio::test]
async fn legacy_schema_counts_as_source_while_current_schema_does_not() {
    for format in [PREVIOUS_FORMAT, FORMAT] {
        let (_root, engine, dataset, view) = workspace().await;
        let (build, mut operation) = admission(
            &dataset,
            "schema",
            if format == PREVIOUS_FORMAT { 1 } else { 0 },
        );
        let request = OperationRequest::Schema(SchemaSpec {
            dataset: dataset.clone(),
            operation: build.operation().clone(),
            base: id("base"),
            definition: "v1".into(),
            next_definition: "v2".into(),
            fields: vec![],
            additions: vec![],
            write_defaults: BTreeMap::new(),
        });
        operation.result_format = format;
        operation.request_digest = fingerprint(&request).unwrap();
        let view = admit(&engine, &view, &request, &operation).await;
        let pending = engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap();
        assert_eq!(
            pending,
            if format == PREVIOUS_FORMAT {
                vec![id("schema")]
            } else {
                vec![]
            }
        );
    }
}

#[tokio::test]
async fn refuses_metadata_before_decoding_an_oversized_request() {
    let (_root, engine, dataset, view) = workspace().await;
    let (mut request, mut operation) = admission(&dataset, "first", 1);
    if let OperationRequest::Build(spec) = &mut request {
        spec.sql = "x".repeat(engine.inner.lifecycle.budget.limits.max_metadata_bytes / 4);
    }
    operation.request_digest = fingerprint(&request).unwrap();
    let view = admit(&engine, &view, &request, &operation).await;
    assert_eq!(
        engine
            .pending_source_operations(&view, &dataset)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}
