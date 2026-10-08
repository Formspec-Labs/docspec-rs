//! Exact-version HTTP reads preserve values and cancellation through native Arrow IPC.

#[path = "support/scalar.rs"]
mod scalar;

use arrow::{
    array::{Array, Int64Array, StringArray},
    ipc::reader::StreamReader,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{Engine, budget::WorkspaceLock};
use futures::TryStreamExt;
use scalar::{fixture, spec};
use std::io::Cursor;
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn http_reads_exact_historical_source_and_combined_values_in_bounded_frames() {
    let root = TempDir::new().unwrap();
    let (engine, base, mut expected) = fixture(root.path(), false).await;
    let enriched = engine
        .backfill(&spec(
            &base,
            "length",
            "stats",
            "length",
            "CAST(length(text) AS BIGINT)",
        ))
        .await
        .unwrap();
    let router = docspec::http::router(engine.clone());
    for (version, suffix, columns) in [(&base, "/source", 2), (&enriched, "", 3)] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/datasets/comments/versions/{}/arrow{suffix}",
                        version.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/vnd.apache.arrow.stream"
        );
        assert_eq!(response.headers()["docspec-version"], version.id.as_str());
        assert_eq!(
            response.headers()["docspec-manifest-blake3"],
            version.manifest.digest
        );
        let mut body = response.into_body().into_data_stream();
        let mut bytes = Vec::new();
        let mut frames = 0;
        while let Some(frame) = body.try_next().await.unwrap() {
            assert!(frame.len() <= 32 << 20);
            frames += 1;
            bytes.extend_from_slice(&frame);
        }
        assert!(
            frames > 2,
            "schema, batches and end must be streamed separately"
        );
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema().fields().len(), columns);
        let mut actual = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let texts = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let text = (!texts.is_null(i)).then(|| texts.value(i).to_owned());
                if columns == 3 {
                    let lengths = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(
                        (!lengths.is_null(i)).then(|| lengths.value(i)),
                        text.as_ref().map(|s| s.chars().count() as i64)
                    );
                }
                actual.push(((!ids.is_null(i)).then(|| ids.value(i)), text));
            }
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/datasets/comments/versions/{}/arrow/stats",
                    enriched.id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let reader = StreamReader::try_new(
        Cursor::new(to_bytes(response.into_body(), 1 << 20).await.unwrap()),
        None,
    )
    .unwrap();
    assert_eq!(
        reader
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            docspec::storage::OCCURRENCE_FILE,
            docspec::storage::OCCURRENCE_ROW,
            "length"
        ]
    );
    let mut lengths = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        lengths.extend(values.iter());
    }
    lengths.sort();
    let mut expected_lengths: Vec<_> = expected
        .iter()
        .map(|(_, s)| s.as_ref().map(|s| s.chars().count() as i64))
        .collect();
    expected_lengths.sort();
    assert_eq!(lengths, expected_lengths);
    assert_eq!(
        engine.latest(&base.dataset.dataset).await.unwrap(),
        enriched
    );
}

#[tokio::test]
async fn empty_streams_keep_schema_and_known_failures_return_structured_errors() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), true).await;
    let router = docspec::http::router(engine);
    for suffix in ["", "/source"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/datasets/comments/versions/{}/arrow{suffix}",
                        base.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let reader = StreamReader::try_new(
            Cursor::new(to_bytes(response.into_body(), 1 << 20).await.unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(reader.schema().fields().len(), 2);
        assert_eq!(
            reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
            0
        );
    }
    for (version, group, code) in [
        (base.id.as_str(), "absent", "missing_group"),
        ("absent", "source", "no_release"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/datasets/comments/versions/{version}/arrow/{group}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(value["code"], code);
    }
    std::fs::write(&base.dataset.inputs["input"].path, b"corrupted").unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/datasets/comments/versions/{}/arrow/source",
                    base.id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Serving the committed result does not revalidate its historical input.
    assert_eq!(response.status(), StatusCode::OK);
    let reader = StreamReader::try_new(
        Cursor::new(to_bytes(response.into_body(), 1 << 20).await.unwrap()),
        None,
    )
    .unwrap();
    assert_eq!(
        reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
        0
    );
    // The explicit integrity diagnostic still discovers that retained corruption.
    assert_eq!(
        docspec::engine::verify_release(&base, scalar::limits())
            .await
            .unwrap_err()
            .code(),
        "corrupt_object"
    );
}

#[tokio::test]
async fn in_flight_response_keeps_workspace_ownership_until_canceled() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let response = docspec::http::router(engine)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/datasets/comments/versions/{}/arrow/source",
                    base.id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        WorkspaceLock::acquire(&root.path().join("workspace"))
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
    drop(response);
    let engine = Engine::open(root.path().join("workspace"), scalar::limits())
        .await
        .unwrap();
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
}

#[tokio::test]
async fn catalog_free_cli_uses_the_shared_group_reader() {
    let root = TempDir::new().unwrap();
    let (engine, base, mut expected) = fixture(root.path(), false).await;
    drop(engine);
    let workspace = root.path().join("workspace");
    std::fs::rename(
        workspace.join("catalog.sqlite"),
        workspace.join("hidden-catalog.sqlite"),
    )
    .unwrap();
    let manifest = root.path().join("pin.json");
    let output = root.path().join("read.arrow");
    std::fs::write(&manifest, serde_json::to_vec(&base).unwrap()).unwrap();
    let command = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("read")
        .arg(manifest)
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        command.status.success(),
        "{}",
        String::from_utf8_lossy(&command.stderr)
    );
    assert!(!workspace.join("catalog.sqlite").exists());
    let reader = StreamReader::try_new(std::fs::File::open(output).unwrap(), None).unwrap();
    let mut actual = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        actual.extend(ids.iter().zip(texts.iter().map(|s| s.map(str::to_owned))));
    }
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn rebound_catalog_results_cannot_change_an_exact_version_url() {
    use docspec::{
        budget::Budget,
        catalog::{Lifecycle, record},
        model::RunSpec,
        storage,
    };
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), true).await;
    let second = engine
        .run(&RunSpec {
            dataset: base.dataset.dataset.clone(),
            operation: scalar::id("second"),
            base: None,
            relationship: None,
            input: base.dataset.inputs["input"].path.clone(),
            expected_digest: base.dataset.inputs["input"].digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.path().join("workspace"))
            .await
            .unwrap(),
        Budget::new(scalar::limits()).unwrap(),
    );
    for kind in [
        "other_version",
        "other_dataset",
        "unverified",
        "different_dataset_version",
    ] {
        let mut rebound = if kind == "other_version" {
            second.clone()
        } else {
            base.clone()
        };
        match kind {
            "other_dataset" => rebound.dataset.dataset = scalar::id("other"),
            "unverified" => rebound.verified = false,
            "different_dataset_version" => rebound.dataset.version = scalar::id("other"),
            _ => {}
        }
        let view = lifecycle.open(&base.dataset.dataset).await.unwrap();
        lifecycle
            .commit(
                &view,
                vec![record(format!("result/{}", base.id), rebound).unwrap()],
            )
            .await
            .unwrap();
        assert_eq!(
            engine
                .release(&base.dataset.dataset, &base.id)
                .await
                .unwrap_err()
                .code(),
            "release_mismatch"
        );
        let response = docspec::http::router(engine.clone())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/datasets/comments/versions/{}/arrow/source",
                        base.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{kind}"
        );
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(error["code"], "release_mismatch");
    }
}
