//! Compaction admits large managed companions before decoding control records.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::{fs::File, sync::Arc};

#[tokio::test]
async fn quality_compaction_refuses_unadmitted_request_companions() {
    let root = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let input = root.path().join("input.parquet");
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let limits = crate::budget::Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 32 << 20,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 4 << 20,
    };
    let engine = Engine::open(root.path().join("host"), limits.clone())
        .await
        .unwrap();
    // The tiny policy row refers to a valid, much larger admitted SQL request.
    let command = serde_json::from_value(json!({
        "action": "prepare",
        "build": {
            "dataset": "records", "operation": "pending", "definition": "v1",
            "input": input, "expected_digest": crate::storage::digest(&input).unwrap().0,
            "sql": format!("/*{}*/ SELECT * FROM input", "x".repeat(512 << 10)),
        },
        "policy": {
            "id": "quality", "rules_sha256": "a".repeat(64),
            "evaluator": {"id": "urn:ddk:quality:datacontract-cli", "version": "1.2.9",
                          "distribution_sha256": "b".repeat(64)},
            "semantics": {"id": "urn:ddk:quality:duckdb-direct-view", "revision": "1"},
            "required_checks": ["independent-check"], "expected_epoch": 0, "expected_selection": null,
        },
    })).unwrap();
    engine.managed_build(command).await.unwrap();
    let dataset = Id::new("records").unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&dataset).await.unwrap();
    let rows = lifecycle.rows_for_compaction(&view).await.unwrap();
    // Its 4 MiB shared metadata half cannot admit the request's decoded copies.
    let limited = crate::budget::Budget::new(crate::budget::Limits {
        memory_bytes: 8 << 20,
        ..limits
    })
    .unwrap();
    let mut removed = BTreeSet::new();
    let error = prune_obsolete_protections(
        &rows,
        &view.identifier().to_string(),
        &limited,
        &BTreeSet::new(),
        &mut removed,
    )
    .unwrap_err();
    assert_eq!(error.code(), "capacity");
    assert!(removed.is_empty());
    assert!(rows.contains_key("request/pending"));
    assert!(rows.contains_key("readback/pending"));
}
