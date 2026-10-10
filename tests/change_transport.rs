//! Comparison transports preserve engine results, descriptions and successful EOF.

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "shared fixture includes scalar backfill scenarios"
)]
mod scalar;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    model::{ComparisonIdentity, ComparisonOptions, InputSpec, ReleasePin, UpdateSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{io::Cursor, sync::Arc};
use tempfile::TempDir;
use tower::ServiceExt;

async fn updated(root: &std::path::Path) -> (Engine, ReleasePin, ReleasePin) {
    let (engine, before, _) = scalar::fixture(root, false).await;
    let deletion_path = root.join("deletions.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let deletions = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(2)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&deletion_path).unwrap(), schema, None).unwrap();
    writer.write(&deletions).unwrap();
    writer.close().unwrap();
    let deletions = InputSpec {
        path: deletion_path.to_string_lossy().into_owned(),
        digest: storage::digest(&deletion_path).unwrap().0,
    };
    let after = engine
        .update(&UpdateSpec {
            dataset: before.dataset.dataset.clone(),
            operation: scalar::id("transport-update"),
            definition: "v1".into(),
            base: before.id.clone(),
            keys: vec!["id".into()],
            replacements: scalar::input(
                &root.join("replacements.parquet"),
                &[(Some(0), Some("changed".into())), (Some(2048), None)],
            ),
            deletions: Some(deletions),
        })
        .await
        .unwrap();
    (engine, before, after)
}

fn post(path: &str, request: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&request).unwrap()))
        .unwrap()
}

async fn expected(
    engine: &Engine,
    before: &ReleasePin,
    after: &ReleasePin,
    options: &ComparisonOptions,
) -> (arrow::datatypes::SchemaRef, Vec<RecordBatch>) {
    let stream = engine
        .changes(before, after, "source", options)
        .await
        .unwrap();
    let schema = stream.schema();
    let batches = stream.try_collect().await.unwrap();
    (schema, batches)
}

fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut writer = arrow::json::ArrayWriter::new(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&writer.into_inner()).unwrap();
    let mut rows: Vec<_> = rows.into_iter().map(|row| row.to_string()).collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn http_defaults_and_explicit_options_preserve_engine_comparisons() {
    let root = TempDir::new().unwrap();
    let (engine, before, after) = updated(root.path()).await;
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({"before": before, "after": after});
    let response = router
        .clone()
        .oneshot(post("/compare", request.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let actual: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let summary = engine.compare(&before, &after, "source").await.unwrap();
    assert_eq!(actual, serde_json::to_value(summary).unwrap());

    for options in [
        ComparisonOptions::default(),
        ComparisonOptions {
            identity: ComparisonIdentity::Values,
            force_full: true,
        },
    ] {
        let (schema, batches) = expected(&engine, &before, &after, &options).await;
        assert!(
            !batches.is_empty(),
            "the fixture must contain actual changes"
        );
        let mut body = request.clone();
        if options.force_full {
            body["options"] = serde_json::to_value(&options).unwrap();
        }
        let response = router
            .clone()
            .oneshot(post("/changes", body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/vnd.apache.arrow.stream"
        );
        let bytes = to_bytes(response.into_body(), 4 << 20).await.unwrap();
        assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema(), schema);
        assert!(reader.schema().metadata().contains_key("docspec.change"));
        assert_eq!(
            sorted_rows(&reader.collect::<Result<Vec<_>, _>>().unwrap()),
            sorted_rows(&batches)
        );

        let response = router
            .clone()
            .oneshot(post(
                "/compare",
                serde_json::json!({
                    "before": before, "after": after, "options": options,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actual: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        let summary = engine
            .compare_with_options(&before, &after, "source", &options)
            .await
            .unwrap();
        assert_eq!(actual, serde_json::to_value(summary).unwrap());
    }

    let response = router
        .clone()
        .oneshot(post(
            "/changes",
            serde_json::json!({
                "before": before, "after": before,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
    let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    assert!(reader.schema().metadata().contains_key("docspec.change"));
    assert_eq!(
        reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
        0
    );

    let mut missing = request;
    missing["group"] = "missing".into();
    let response = router
        .clone()
        .oneshot(post("/changes", missing))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "missing_group");

    let oversized = Request::builder()
        .method("POST")
        .uri("/changes")
        .header("content-type", "application/json")
        .body(Body::from(vec![b' '; (1 << 20) + 1]))
        .unwrap();
    let response = router.oneshot(oversized).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn cli_changes_preserves_descriptor_and_values_selection() {
    let root = TempDir::new().unwrap();
    let (engine, before, after) = updated(root.path()).await;
    let before_path = root.path().join("before.json");
    let after_path = root.path().join("after.json");
    std::fs::write(&before_path, serde_json::to_vec(&before).unwrap()).unwrap();
    std::fs::write(&after_path, serde_json::to_vec(&after).unwrap()).unwrap();
    let mut cases = Vec::new();
    for options in [
        ComparisonOptions::default(),
        ComparisonOptions {
            identity: ComparisonIdentity::Values,
            force_full: true,
        },
    ] {
        let result = expected(&engine, &before, &after, &options).await;
        cases.push((options, result));
    }
    drop(engine);
    for (index, (options, (schema, batches))) in cases.into_iter().enumerate() {
        let output = root.path().join(format!("changes-{index}.arrow"));
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"));
        command
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg("changes")
            .arg(&before_path)
            .arg(&after_path)
            .arg(&output);
        if options.identity == ComparisonIdentity::Values {
            command.arg("--values");
        }
        if options.force_full {
            command.arg("--force-full");
        }
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let bytes = std::fs::read(output).unwrap();
        assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema(), schema);
        assert_eq!(
            sorted_rows(&reader.collect::<Result<Vec<_>, _>>().unwrap()),
            sorted_rows(&batches)
        );
    }
}
