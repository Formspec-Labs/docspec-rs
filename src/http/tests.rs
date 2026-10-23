// Rust guideline compliant 2026-02-21

use super::*;

#[tokio::test]
async fn correction_refusals_preserve_codes_and_return_conflict() {
    for code in [
        "correction_conflict",
        "correction_required",
        "correction_mismatch",
        "correction_unknown",
    ] {
        let response = HttpError(Error::refusal(code, "correction needs review")).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let refusal: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(refusal["code"], code);
    }
}

#[tokio::test]
async fn worker_termination_returns_internal_server_error() {
    let handle = tokio::spawn(async {
        panic!("simulated worker panic");
    });
    let join_error = handle.await.unwrap_err();
    let error = Error::from(join_error);
    assert_eq!(error.code(), "worker");
    let response = HttpError::from(error).into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], "worker");
}

#[tokio::test]
async fn arrow_ipc_write_streaming_on_run_and_updates_run() {
    use arrow::{
        array::{Array, BinaryArray, Decimal128Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        ipc::{reader::StreamReader, writer::StreamWriter},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        crate::budget::Limits {
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 128,
            max_batch_bytes: 64 << 10,
            max_metadata_bytes: 1 << 20,
            ..crate::budget::Limits::default()
        },
    )
    .await
    .unwrap();
    let app = router(engine.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("blob", DataType::Binary, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1_u64, 2_u64, u64::MAX - 1])),
            Arc::new(StringArray::from(vec![
                Some("one"),
                Some("two"),
                Some("max"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(b"\x01\x02".as_slice()),
                Some(b"\x03\x04".as_slice()),
                Some(b"\xff\x00".as_slice()),
            ])),
        ],
    )
    .unwrap();
    let mut ipc_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let ipc_digest = blake3::hash(&ipc_bytes).to_hex().to_string();

    // 1. POST /run with Arrow IPC stream and X-Docspec-Spec + X-Docspec-Input-Blake3
    let run_spec_json = serde_json::json!({
        "dataset": "ipc_ds",
        "operation": "op_ipc_build",
        "sql": "SELECT * FROM input",
        "definition": "v1"
    })
    .to_string();
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/run")
                .header("content-type", "application/vnd.apache.arrow.stream")
                .header("x-docspec-spec", &run_spec_json)
                .header("x-docspec-input-blake3", &ipc_digest)
                .body(Body::from(ipc_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let release1: ReleasePin = serde_json::from_slice(&body).unwrap();
    assert_eq!(release1.dataset.groups["source"].rows, 3);

    // Idempotent retry via standardized /runs/run alias returns the same release pin
    let retry_response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/runs/run")
                .header("content-type", "application/vnd.apache.arrow.stream")
                .header("x-docspec-spec", &run_spec_json)
                .header("x-docspec-input-blake3", &ipc_digest)
                .body(Body::from(ipc_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retry_response.status(), StatusCode::OK);
    let retry_body = axum::body::to_bytes(retry_response.into_body(), 1 << 20)
        .await
        .unwrap();
    let retry_release: ReleasePin = serde_json::from_slice(&retry_body).unwrap();
    assert_eq!(retry_release, release1);

    // 2. POST /updates/run with Arrow IPC stream
    let update_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![2_u64])),
            Arc::new(StringArray::from(vec![Some("two-updated")])),
            Arc::new(BinaryArray::from(vec![Some(b"\xaa\xbb\xcc".as_slice())])),
        ],
    )
    .unwrap();
    let mut update_ipc_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut update_ipc_bytes, &schema).unwrap();
        writer.write(&update_batch).unwrap();
        writer.finish().unwrap();
    }
    let update_ipc_digest = blake3::hash(&update_ipc_bytes).to_hex().to_string();
    let update_spec_json = serde_json::json!({
        "dataset": "ipc_ds",
        "operation": "op_ipc_update",
        "definition": "v1",
        "base": "op_ipc_build",
        "keys": ["id"]
    })
    .to_string();
    let update_response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/updates/run")
                .header("content-type", "application/vnd.apache.arrow.stream")
                .header("x-docspec-spec", &update_spec_json)
                .header("x-docspec-input-blake3", &update_ipc_digest)
                .body(Body::from(update_ipc_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    let update_status = update_response.status();
    let update_body = axum::body::to_bytes(update_response.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(
        update_status,
        StatusCode::OK,
        "update failed: {}",
        String::from_utf8_lossy(&update_body)
    );
    let release2: ReleasePin = serde_json::from_slice(&update_body).unwrap();
    assert_eq!(release2.dataset.groups["source"].rows, 3);

    // Read back the updated version via Arrow IPC read endpoint
    let read_response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/datasets/ipc_ds/versions/op_ipc_update/arrow/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read_response.status(), StatusCode::OK);
    let read_bytes = axum::body::to_bytes(read_response.into_body(), 1 << 20)
        .await
        .unwrap();
    let reader = StreamReader::try_new(std::io::Cursor::new(read_bytes), None).unwrap();
    let read_batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>().unwrap();
    let mut names = Vec::new();
    for b in &read_batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let ns = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let blobs = b
            .column_by_name("blob")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        for row in 0..b.num_rows() {
            names.push((
                ids.value(row),
                ns.value(row).to_owned(),
                blobs.value(row).to_vec(),
            ));
        }
    }
    names.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        names,
        vec![
            (1_i128, "one".to_owned(), b"\x01\x02".to_vec()),
            (2_i128, "two-updated".to_owned(), b"\xaa\xbb\xcc".to_vec()),
            (
                i128::from(u64::MAX - 1),
                "max".to_owned(),
                b"\xff\x00".to_vec()
            ),
        ]
    );

    // 3. Digest mismatch refuses with input_digest_mismatch
    let bad_digest_response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/run")
                .header("content-type", "application/vnd.apache.arrow.stream")
                .header(
                    "x-docspec-spec",
                    serde_json::json!({
                        "dataset": "ipc_ds",
                        "operation": "op_bad_digest",
                        "sql": "SELECT * FROM input",
                        "definition": "v1"
                    })
                    .to_string(),
                )
                .header("x-docspec-input-blake3", "00000000000000000000000000000000")
                .body(Body::from(ipc_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        bad_digest_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let bad_body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(bad_digest_response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bad_body["code"], "input_digest_mismatch");

    // Verify scratch directory has no leftover ipc-* staged files
    if engine.scratch_dir().exists() {
        let leftover: Vec<_> = std::fs::read_dir(engine.scratch_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("ipc-"))
            .collect();
        assert!(
            leftover.is_empty(),
            "expected no leftover scratch IPC files, found {leftover:?}"
        );
    }
    engine.shutdown().await.unwrap();
}
