// Rust guideline compliant 2026-02-21
//! Integration tests for `docspec export-nquads` and `docspec owner-worker`.

use arrow::{
    array::{
        ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int64Array, StringArray,
        StringViewArray, StructArray, TimestampNanosecondArray,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file_hex(path: &Path) -> String {
    sha256_hex(&std::fs::read(path).unwrap())
}

#[test]
fn export_nquads_cli_streams_projected_column_with_exact_digests_and_quad_count() {
    let root = tempfile::tempdir().unwrap();
    let parquet_path = root.path().join("quads.parquet");
    let nq_path = root.path().join("out/graph.nq");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nquad", DataType::Utf8View, true),
        Field::new("custom_quad", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringViewArray::from(vec![
                Some("<urn:s:1> <urn:p:name> \"Alice\" <urn:g:1> ."),
                None,
                Some("<urn:s:2> <urn:p:name> \"Bob\\nLine2\" <urn:g:1> .\n"),
                Some("<urn:s:3> <urn:p:name> \"Café\" <urn:g:1> ."),
            ])),
            Arc::new(StringArray::from(vec![
                Some("<urn:c:1> <urn:p:v> \"one\" ."),
                Some("<urn:c:2> <urn:p:v> \"two\" .\n"),
                None,
                None,
            ])),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&parquet_path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&parquet_path)
        .arg("--output")
        .arg(&nq_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout_str = String::from_utf8(out.stdout).unwrap();
    assert_eq!(
        stdout_str.lines().count(),
        1,
        "expected compact single-line JSON"
    );
    let receipt: Value = serde_json::from_str(&stdout_str).unwrap();

    let expected_nq = concat!(
        "<urn:s:1> <urn:p:name> \"Alice\" <urn:g:1> .\n",
        "<urn:s:2> <urn:p:name> \"Bob\\nLine2\" <urn:g:1> .\n",
        "<urn:s:3> <urn:p:name> \"Café\" <urn:g:1> .\n",
    );
    let actual_nq = std::fs::read(&nq_path).unwrap();
    assert_eq!(actual_nq, expected_nq.as_bytes());
    assert_eq!(receipt["status"], "succeeded");
    assert_eq!(
        receipt["input"]["path"],
        json!(std::fs::canonicalize(&parquet_path).unwrap())
    );
    assert_eq!(receipt["input"]["sha256"], sha256_file_hex(&parquet_path));
    assert_eq!(
        receipt["output"]["path"],
        json!(std::fs::canonicalize(&nq_path).unwrap())
    );
    assert_eq!(receipt["output"]["sha256"], sha256_hex(&actual_nq));
    assert_eq!(receipt["output"]["bytes"], actual_nq.len() as u64);
    assert_eq!(receipt["output"]["quads"], 3);

    // Custom --column flag
    let custom_nq_path = root.path().join("out/custom.nq");
    let custom_out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&parquet_path)
        .arg("--output")
        .arg(&custom_nq_path)
        .args(["--column", "custom_quad"])
        .output()
        .unwrap();
    assert!(custom_out.status.success());
    let custom_receipt: Value = serde_json::from_slice(&custom_out.stdout).unwrap();
    assert_eq!(custom_receipt["output"]["quads"], 2);

    // Non-string column is refused cleanly without leaving temp files
    let bad_nq_path = root.path().join("out/refused.nq");
    let bad_out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&parquet_path)
        .arg("--output")
        .arg(&bad_nq_path)
        .args(["--column", "id"])
        .output()
        .unwrap();
    assert!(!bad_out.status.success());
    assert!(!bad_nq_path.exists());
}

fn build_owner_test_batch(rows_range: std::ops::Range<usize>) -> RecordBatch {
    let nested_fields = Fields::from(vec![
        Field::new("$number", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(24, 4), false),
        Field::new("ratio", DataType::Float64, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Binary, true),
        Field::new("nested", DataType::Struct(nested_fields.clone()), true),
    ]));

    let all_ids: [i64; 5] = [
        9_007_199_254_740_995,
        -9_007_199_254_740_995,
        42,
        9_007_199_254_740_993,
        7,
    ];
    let all_amounts: [i128; 5] = [
        123_456_700,                // 12345.6700
        -50,                        // -0.0050
        1_000_000,                  // 100.0000
        12_345_678_901_234_567_891, // 1234567890123456.7891
        90_071_992_547_409_930_000, // 9007199254740993.0000 (>15 sig digits)
    ];
    let all_ratios: [f64; 5] = [-0.0, 1.5, 0.0, 2.25, -3.5];
    let all_ts: [i64; 5] = [
        1_794_406_516_123_456_789,
        1_794_406_517_000_000_000,
        1_794_406_518_000_000_000,
        1_794_406_519_000_000_001,
        1_794_406_520_000_000_000,
    ];
    let all_payloads: [Option<&[u8]>; 5] = [
        Some(b"alpha"),
        Some(b"beta"),
        None,
        Some(b"delta"),
        Some(b"eps"),
    ];
    let all_nested_num: [Option<&str>; 5] = [
        Some("raw-escape"),
        Some("second"),
        None,
        Some("fourth"),
        Some("fifth"),
    ];
    let all_nested_lbl: [Option<&str>; 5] =
        [Some("row0"), Some("row1"), None, Some("row3"), Some("row4")];
    let all_nested_valid: [bool; 5] = [true, true, false, true, true];

    let ids: Vec<i64> = rows_range.clone().map(|i| all_ids[i]).collect();
    let amounts: Vec<i128> = rows_range.clone().map(|i| all_amounts[i]).collect();
    let ratios: Vec<f64> = rows_range.clone().map(|i| all_ratios[i]).collect();
    let ts: Vec<i64> = rows_range.clone().map(|i| all_ts[i]).collect();
    let payloads: Vec<Option<&[u8]>> = rows_range.clone().map(|i| all_payloads[i]).collect();
    let nested_num: Vec<Option<&str>> = rows_range.clone().map(|i| all_nested_num[i]).collect();
    let nested_lbl: Vec<Option<&str>> = rows_range.clone().map(|i| all_nested_lbl[i]).collect();
    let nested_valid: Vec<bool> = rows_range.map(|i| all_nested_valid[i]).collect();

    let dec_arr = Decimal128Array::from(amounts)
        .with_precision_and_scale(24, 4)
        .unwrap();
    let ts_arr = TimestampNanosecondArray::from(ts).with_timezone("UTC");
    let struct_arr = StructArray::try_new(
        nested_fields,
        vec![
            Arc::new(StringArray::from(nested_num)) as ArrayRef,
            Arc::new(StringArray::from(nested_lbl)) as ArrayRef,
        ],
        Some(NullBuffer::from(nested_valid)),
    )
    .unwrap();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(dec_arr),
            Arc::new(Float64Array::from(ratios)),
            Arc::new(ts_arr),
            Arc::new(BinaryArray::from(payloads)),
            Arc::new(struct_arr),
        ],
    )
    .unwrap()
}

fn write_parquet_with_row_group_size(path: &Path, batch: &RecordBatch, rg_rows: usize) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rg_rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn send_rpc(stdin: &mut impl Write, stdout: &mut impl BufRead, req: Value) -> Value {
    let mut line = serde_json::to_vec(&req).unwrap();
    line.push(b'\n');
    stdin.write_all(&line).unwrap();
    stdin.flush().unwrap();
    let mut resp_line = String::new();
    let n = stdout.read_line(&mut resp_line).unwrap();
    assert!(n > 0, "worker closed stdout unexpectedly");
    serde_json::from_str(&resp_line).unwrap()
}

#[test]
fn owner_worker_stdio_serves_init_paginates_shards_and_runs_exact_queries() {
    let root = tempfile::tempdir().unwrap();
    let root_canon = std::fs::canonicalize(root.path()).unwrap();
    let data_dir = root_canon.join("data");
    let catalog_dir = root_canon.join("catalog");
    let recipes_dir = catalog_dir.join("recipes");
    let receipts_dir = root_canon.join("receipts");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&recipes_dir).unwrap();

    // Write full 5-row Parquet (row_group_size=2) + 3 multi-shard files [0..2), [2..4), [4..5) (row_group_size=1)
    let full_batch = build_owner_test_batch(0..5);
    let full_parquet = data_dir.join("ledger.parquet");
    write_parquet_with_row_group_size(&full_parquet, &full_batch, 2);
    let full_sha = sha256_file_hex(&full_parquet);

    let shard_ranges = [(0usize, 2usize), (2, 4), (4, 5)];
    let mut shards_json = Vec::new();
    for (idx, (lo, hi)) in shard_ranges.into_iter().enumerate() {
        let shard_path = data_dir.join(format!("shard-{idx}.parquet"));
        let shard_batch = build_owner_test_batch(lo..hi);
        write_parquet_with_row_group_size(&shard_path, &shard_batch, 1);
        let bytes = std::fs::read(&shard_path).unwrap();
        shards_json.push(json!({
            "path": shard_path,
            "sha256": sha256_hex(&bytes),
            "byteSize": bytes.len(),
            "rowCount": hi - lo,
            "rowRange": [lo, hi],
        }));
    }

    // Write SQL recipe companion using both positional `?` and named `$min_amount`
    let sql_text = "SELECT id, amount, ratio, iri_encode('hello world') AS encoded_iri \
                    FROM ledger \
                    WHERE id = ? AND amount = $min_amount \
                    ORDER BY id ASC;\n";
    let sql_rel = "recipes/high_precision_filter.sql";
    let sql_path = catalog_dir.join(sql_rel);
    std::fs::write(&sql_path, sql_text.as_bytes()).unwrap();
    let sql_sha = sha256_hex(sql_text.as_bytes());

    let all_sql_text =
        "SELECT id, amount FROM ledger WHERE id >= ? AND amount >= $min_amount ORDER BY id ASC;\n";
    let all_sql_rel = "recipes/all_rows.sql";
    std::fs::write(catalog_dir.join(all_sql_rel), all_sql_text.as_bytes()).unwrap();
    let all_sql_sha = sha256_hex(all_sql_text.as_bytes());

    let catalog_doc = json!({
        "specVersion": "0.1",
        "kind": "catalog",
        "id": "urn:test:owner-catalog",
        "version": "1.0.0",
        "audience": "local",
        "datasets": [{"id": "urn:ds:ledger", "alias": "ledger"}],
        "versions": [{
            "id": "urn:ver:ledger:v1",
            "dataset": "ledger",
            "retention": "present",
            "reference": {
                "scheme": "urn:ddk:version:local-file-sha256",
                "value": {
                    "location": full_parquet,
                    "sha256": full_sha,
                }
            }
        }],
        "releases": [{
            "id": "urn:rel:r1",
            "members": ["urn:ver:ledger:v1"]
        }],
        "relationships": [{
            "id": "urn:relship:same-rel",
            "scope": "same_release",
            "source": {"dataset": "ledger"},
            "target": {"dataset": "ledger"}
        }],
        "recipes": [
            {
                "id": "urn:recipe:high-precision",
                "sql": sql_rel,
                "dialect": "datafusion",
                "inputs": {"ledger": "ledger"},
                "relationships": [],
                "parameterOrder": ["min_id"],
                "parameters": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["min_id", "min_amount"],
                    "properties": {
                        "min_id": {"type": "integer", "minimum": 1},
                        "min_amount": {"type": "number", "minimum": 0}
                    }
                }
            },
            {
                "id": "urn:recipe:requires-release",
                "sql": "recipes/all_rows.sql",
                "dialect": "datafusion",
                "inputs": {"ledger": "ledger"},
                "relationships": ["urn:relship:same-rel"],
                "parameterOrder": ["min_id"],
                "parameters": {
                    "type": "object",
                    "required": ["min_id", "min_amount"],
                    "properties": {
                        "min_id": {"type": "integer"},
                        "min_amount": {"type": "number"}
                    }
                }
            }
        ],
        "files": [
            {"path": sql_rel, "role": "recipe", "sha256": sql_sha},
            {"path": all_sql_rel, "role": "recipe", "sha256": all_sql_sha}
        ]
    });
    let catalog_path = catalog_dir.join("catalog.ddk.json");
    std::fs::write(
        &catalog_path,
        serde_json::to_vec_pretty(&catalog_doc).unwrap(),
    )
    .unwrap();

    let lock_doc = json!({
        "specVersion": "0.1",
        "kind": "lock",
        "id": "urn:test:owner-catalog:lock",
        "files": [
            {"path": sql_rel, "role": "recipe", "sha256": sql_sha},
            {"path": all_sql_rel, "role": "recipe", "sha256": all_sql_sha}
        ]
    });
    let lock_path = catalog_dir.join("catalog.lock.json");
    let lock_bytes = serde_json::to_vec_pretty(&lock_doc).unwrap();
    std::fs::write(&lock_path, &lock_bytes).unwrap();
    let lock_sha = sha256_hex(&lock_bytes);

    let bin_path = Path::new(env!("CARGO_BIN_EXE_docspec"));
    let bin_canon = std::fs::canonicalize(bin_path).unwrap();
    let bin_sha = sha256_file_hex(&bin_canon);

    let mut child = Command::new(bin_path)
        .arg("owner-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // 1. Ping before init
    let ping_resp = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({"id": 1, "method": "ping", "params": {}}),
    );
    assert_eq!(
        ping_resp,
        json!({"id": 1, "ok": true, "result": {"ok": true}})
    );

    // 2. Oversized >256 KiB line returns 413 request_budget without corrupting framing
    let huge = format!(
        "{{\"id\":2,\"method\":\"ping\",\"pad\":\"{}\"}}\n",
        "x".repeat(270 * 1024)
    );
    stdin.write_all(huge.as_bytes()).unwrap();
    stdin.flush().unwrap();
    let mut huge_resp_line = String::new();
    stdout.read_line(&mut huge_resp_line).unwrap();
    let huge_resp: Value = serde_json::from_str(&huge_resp_line).unwrap();
    assert_eq!(huge_resp["ok"], false);
    assert_eq!(huge_resp["error"]["code"], "request_budget");
    assert_eq!(huge_resp["error"]["status"], 413);

    // Follow-up ping immediately succeeds
    let ping_after = send_rpc(&mut stdin, &mut stdout, json!({"id": 3, "method": "ping"}));
    assert_eq!(ping_after["ok"], true);

    // 3. Tampered lock digest fails with integrity_mismatch (409) without initializing worker
    let bad_lock = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 4,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": "0".repeat(64),
            }
        }),
    );
    assert_eq!(bad_lock["ok"], false);
    assert_eq!(bad_lock["error"]["code"], "integrity_mismatch");
    assert_eq!(bad_lock["error"]["status"], 409);

    // 4. Tampered docspecSha256 fails with integrity_mismatch (409)
    let bad_bin = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 5,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
                "docspecBinary": bin_canon,
                "docspecSha256": "f".repeat(64),
            }
        }),
    );
    assert_eq!(bad_bin["ok"], false);
    assert_eq!(bad_bin["error"]["code"], "integrity_mismatch");
    assert_eq!(bad_bin["error"]["status"], 409);

    // 5. Valid init succeeds and pins binarySha256 + lockSha256
    let init_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 6,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
                "docspecBinary": bin_canon,
                "docspecSha256": bin_sha,
                "retainReceiptsDir": receipts_dir,
            }
        }),
    );
    assert_eq!(init_ok["ok"], true, "{init_ok}");
    assert_eq!(init_ok["result"]["runtime"], "docspec-native-rust");
    assert_eq!(init_ok["result"]["lockSha256"], lock_sha);
    assert_eq!(init_ok["result"]["engine"]["id"], "docspec");
    assert_eq!(init_ok["result"]["engine"]["binarySha256"], bin_sha);

    // 6. read_page across 3 shards (row_group_size=1) with offset=0, limit=4
    let page = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 7,
            "method": "read_page",
            "params": {
                "shards": shards_json,
                "offset": 0,
                "limit": 4,
            }
        }),
    );
    assert_eq!(page["ok"], true, "{page}");
    let page_res = &page["result"];
    assert_eq!(page_res["total"], 5);
    assert_eq!(page_res["hasMore"], true);
    assert_eq!(page_res["rowOrder"], "source-row");
    assert_eq!(page_res["rowPositions"], json!([0, 1, 2, 3]));
    let page_rows = page_res["rows"].as_array().unwrap();
    assert_eq!(page_rows.len(), 4);

    // Row 0: Int64 > 2^53-1, Decimal128(24,4) trailing zeros, -0.0, UTC ns timestamp, base64 binary, escaped $number struct
    assert_eq!(page_rows[0]["id"], json!({"$number": "9007199254740995"}));
    assert_eq!(page_rows[0]["amount"], json!({"$number": "12345.6700"}));
    assert_eq!(page_rows[0]["ratio"], json!({"$number": "-0"}));
    assert_eq!(
        page_rows[0]["event_ts"],
        json!("2026-11-11T14:15:16.123456789Z")
    );
    assert_eq!(
        page_rows[0]["payload"],
        json!({"type": "binary", "encoding": "base64", "value": "YWxwaGE="})
    );
    assert_eq!(
        page_rows[0]["nested"],
        json!({"$object": [["$number", "raw-escape"], ["label", "row0"]]})
    );
    // Row 1: negative >2^53-1 Int64 and negative Decimal < 1
    assert_eq!(page_rows[1]["id"], json!({"$number": "-9007199254740995"}));
    assert_eq!(page_rows[1]["amount"], json!({"$number": "-0.0050"}));
    // Row 2: nulls preserved
    assert_eq!(page_rows[2]["payload"], Value::Null);
    assert_eq!(page_rows[2]["nested"], Value::Null);
    // Row 3: 20-digit Decimal128(24, 4)
    assert_eq!(
        page_rows[3]["amount"],
        json!({"$number": "1234567890123456.7891"})
    );

    // 7. read_page slice across shard boundary (offset=1, limit=3 -> rows [1, 2, 3])
    let mid_page = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 8,
            "method": "read_page",
            "params": {
                "shards": shards_json,
                "offset": 1,
                "limit": 3,
            }
        }),
    );
    assert_eq!(mid_page["ok"], true);
    assert_eq!(mid_page["result"]["rowPositions"], json!([1, 2, 3]));
    assert_eq!(mid_page["result"]["hasMore"], true);

    // 8. run_query in-process with 64-bit integer `?` and 20-digit Decimal `$min_amount` + retained receipt
    let query_resp = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 9,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:high-precision",
                "expectedSqlSha256": sql_sha,
                "versions": {"ledger": "urn:ver:ledger:v1"},
                "inputs": {
                    "ledger": {
                        "path": full_parquet,
                        "sha256": full_sha,
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {
                    "min_id": 9007199254740992_i64,
                    "min_amount": 1234567890123456.8_f64
                },
                "rawNumericParameters": {
                    "min_id": "9007199254740993",
                    "min_amount": "1234567890123456.7891"
                },
                "limit": 10
            }
        }),
    );
    assert_eq!(query_resp["ok"], true, "{query_resp}");
    let qres = &query_resp["result"];
    assert_eq!(qres["hasMore"], false);
    assert_eq!(qres["total"], 1);
    assert_eq!(qres["rowPositions"], json!([0]));
    assert_eq!(qres["sqlSha256"], sql_sha);
    assert_eq!(qres["engine"]["id"], "docspec");
    let qrows = qres["rows"].as_array().unwrap();
    assert_eq!(qrows.len(), 1);
    assert_eq!(qrows[0]["id"], json!({"$number": "9007199254740993"}));
    assert_eq!(
        qrows[0]["amount"],
        json!({"$number": "1234567890123456.7891"})
    );
    assert_eq!(qrows[0]["encoded_iri"], json!("hello%20world"));

    // Verify retained receipt exists and matches SHA-256
    let rec_path_str = qres["record"]["path"].as_str().unwrap();
    let rec_sha_str = qres["record"]["sha256"].as_str().unwrap();
    let rec_bytes = std::fs::read(rec_path_str).unwrap();
    assert_eq!(sha256_hex(&rec_bytes), rec_sha_str);
    let rec_json: Value = serde_json::from_slice(&rec_bytes).unwrap();
    assert_eq!(rec_json["kind"], "docspec-owner-query-receipt");
    assert_eq!(rec_json["recipeId"], "urn:recipe:high-precision");
    assert_eq!(rec_json["returned"], 1);

    // 9. same_release relationship without releaseId is refused with native_scope_refused (409)
    let scope_err = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 10,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:requires-release",
                "expectedSqlSha256": all_sql_sha,
                "versions": {"ledger": "urn:ver:ledger:v1"},
                "inputs": {
                    "ledger": {
                        "path": full_parquet,
                        "sha256": full_sha,
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {"min_id": 1, "min_amount": 0},
                "limit": 10
            }
        }),
    );
    assert_eq!(scope_err["ok"], false);
    assert_eq!(scope_err["error"]["code"], "native_scope_refused");
    assert_eq!(scope_err["error"]["status"], 409);

    // Same recipe WITH releaseId succeeds
    let scope_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 11,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:requires-release",
                "expectedSqlSha256": all_sql_sha,
                "releaseId": "urn:rel:r1",
                "inputs": {
                    "ledger": {
                        "path": full_parquet,
                        "sha256": full_sha,
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {"min_id": 1, "min_amount": 0},
                "limit": 2
            }
        }),
    );
    assert_eq!(scope_ok["ok"], true, "{scope_ok}");
    assert_eq!(scope_ok["result"]["hasMore"], true);
    assert!(scope_ok["result"].get("total").is_none());
    assert_eq!(scope_ok["result"]["rows"].as_array().unwrap().len(), 2);

    // 10. Clean exit on stdin EOF
    drop(stdin);
    let exit_status = child.wait().unwrap();
    assert!(exit_status.success());
}
