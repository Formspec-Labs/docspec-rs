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

fn write_single_string_parquet(path: &Path, values: Vec<Option<&str>>) {
    let schema = Arc::new(Schema::new(vec![Field::new("nquad", DataType::Utf8, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
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

    // Pre-existing output destination is refused with structured `existing_output` without overwriting
    let overwrite_out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&parquet_path)
        .arg("--output")
        .arg(&nq_path)
        .output()
        .unwrap();
    assert!(!overwrite_out.status.success());
    let overwrite_json: Value = serde_json::from_slice(&overwrite_out.stdout).unwrap();
    assert_eq!(overwrite_json["status"], "failed");
    assert_eq!(overwrite_json["error"]["code"], "existing_output");
    assert_eq!(std::fs::read(&nq_path).unwrap(), expected_nq.as_bytes());

    // Non-string column is refused cleanly with structured JSON and zero leftover files
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
    let bad_json: Value = serde_json::from_slice(&bad_out.stdout).unwrap();
    assert_eq!(bad_json["status"], "failed");
    assert_eq!(bad_json["error"]["code"], "invalid_export");
    assert!(!bad_nq_path.exists());

    // Embedded newline inside a single cell is refused (`invalid_export`) without publishing output
    let multiline_parquet = root.path().join("multiline.parquet");
    write_single_string_parquet(
        &multiline_parquet,
        vec![Some(
            "<urn:s:1> <urn:p:1> \"a\" .\n<urn:s:2> <urn:p:1> \"injected\" .",
        )],
    );
    let multiline_nq = root.path().join("out/multiline.nq");
    let multiline_out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&multiline_parquet)
        .arg("--output")
        .arg(&multiline_nq)
        .output()
        .unwrap();
    assert!(!multiline_out.status.success());
    let multiline_json: Value = serde_json::from_slice(&multiline_out.stdout).unwrap();
    assert_eq!(multiline_json["status"], "failed");
    assert_eq!(multiline_json["error"]["code"], "invalid_export");
    assert!(!multiline_nq.exists());

    // Empty / whitespace-only quad cell is refused (`invalid_export`)
    let blank_parquet = root.path().join("blank.parquet");
    write_single_string_parquet(&blank_parquet, vec![Some("   \t  ")]);
    let blank_nq = root.path().join("out/blank.nq");
    let blank_out = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["export-nquads", "--input"])
        .arg(&blank_parquet)
        .arg("--output")
        .arg(&blank_nq)
        .output()
        .unwrap();
    assert!(!blank_out.status.success());
    let blank_json: Value = serde_json::from_slice(&blank_out.stdout).unwrap();
    assert_eq!(blank_json["status"], "failed");
    assert_eq!(blank_json["error"]["code"], "invalid_export");
    assert!(!blank_nq.exists());
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
    let receipts_root = root_canon.join("receipts-root");
    let receipts_dir = receipts_root.join("nested");
    let outside_dir = root_canon.join("outside");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&recipes_dir).unwrap();
    std::fs::create_dir_all(&receipts_root).unwrap();
    std::fs::create_dir_all(&outside_dir).unwrap();

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

    // Write a mismatched-schema Parquet shard for cross-shard schema homogeneity test
    let mismatch_shard_path = data_dir.join("mismatch-shard.parquet");
    write_single_string_parquet(&mismatch_shard_path, vec![Some("only-one-col")]);
    let mismatch_bytes = std::fs::read(&mismatch_shard_path).unwrap();
    let mismatch_sha = sha256_hex(&mismatch_bytes);

    // Write SQL recipe companions:
    // 1) positional `?` + named `$min_amount`
    let sql_text = "SELECT id, amount, ratio, iri_encode('hello world') AS encoded_iri \
                    FROM ledger \
                    WHERE id = ? AND amount = $min_amount \
                    ORDER BY id ASC;\n";
    let sql_rel = "recipes/high_precision_filter.sql";
    std::fs::write(catalog_dir.join(sql_rel), sql_text.as_bytes()).unwrap();
    let sql_sha = sha256_hex(sql_text.as_bytes());

    // 2) same_release relationship recipe
    let all_sql_text =
        "SELECT id, amount FROM ledger WHERE id >= ? AND amount >= $min_amount ORDER BY id ASC;\n";
    let all_sql_rel = "recipes/all_rows.sql";
    std::fs::write(catalog_dir.join(all_sql_rel), all_sql_text.as_bytes()).unwrap();
    let all_sql_sha = sha256_hex(all_sql_text.as_bytes());

    // 3) adversarial lexer recipe: nested /* /* */ :inj */, E'a\'b', backticks, $$a'b$$,
    //    exact <=15-digit decimal (:exact_dec = 12345.6700), union schema, and trailing comment after `;`
    let adv_sql_text = "SELECT `id`, amount, E'a\\'b :inj' AS esc_lit, $$a'b $exact_dec$$ AS dollar_lit, :inj AS bound_payload, :code AS bound_code \
                        /* outer /* inner */ :inj */ \
                        FROM ledger \
                        WHERE amount = :exact_dec \
                        ORDER BY id ASC;\n-- trailing comment after semicolon\n";
    let adv_sql_rel = "recipes/adversarial.sql";
    std::fs::write(catalog_dir.join(adv_sql_rel), adv_sql_text.as_bytes()).unwrap();
    let adv_sql_sha = sha256_hex(adv_sql_text.as_bytes());

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
                "sql": all_sql_rel,
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
            },
            {
                "id": "urn:recipe:adversarial",
                "sql": adv_sql_rel,
                "dialect": "datafusion",
                "inputs": {"ledger": "ledger"},
                "relationships": [],
                "parameters": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["inj", "exact_dec", "code"],
                    "properties": {
                        "inj": {"type": "string"},
                        "exact_dec": {"type": "number"},
                        "code": {"type": ["string", "integer"]}
                    }
                }
            }
        ],
        "files": [
            {"path": sql_rel, "role": "recipe", "sha256": sql_sha},
            {"path": all_sql_rel, "role": "recipe", "sha256": all_sql_sha},
            {"path": adv_sql_rel, "role": "recipe", "sha256": adv_sql_sha}
        ]
    });
    let catalog_path = catalog_dir.join("catalog.ddk.json");
    let catalog_bytes = serde_json::to_vec_pretty(&catalog_doc).unwrap();
    std::fs::write(&catalog_path, &catalog_bytes).unwrap();
    let catalog_sha = sha256_hex(&catalog_bytes);

    let lock_doc = json!({
        "specVersion": "0.1",
        "kind": "lock",
        "id": "urn:test:owner-catalog:lock",
        "files": [
            {"path": sql_rel, "role": "recipe", "sha256": sql_sha},
            {"path": all_sql_rel, "role": "recipe", "sha256": all_sql_sha},
            {"path": adv_sql_rel, "role": "recipe", "sha256": adv_sql_sha}
        ],
        "extensions": {
            "urn:ddk:catalog:output-set": [
                {"path": "catalog.ddk.json", "role": "catalog", "sha256": catalog_sha},
                {"path": sql_rel, "role": "recipe", "sha256": sql_sha},
                {"path": all_sql_rel, "role": "recipe", "sha256": all_sql_sha},
                {"path": adv_sql_rel, "role": "recipe", "sha256": adv_sql_sha}
            ]
        }
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

    // 3a. Missing expectedLockSha256 is refused (`400 invalid_registration`)
    let missing_lock_sha = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 40,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
            }
        }),
    );
    assert_eq!(missing_lock_sha["ok"], false);
    assert_eq!(missing_lock_sha["error"]["code"], "invalid_registration");
    assert_eq!(missing_lock_sha["error"]["status"], 400);

    // 3b. Tampered lock digest fails with integrity_mismatch (409) without initializing worker
    let bad_lock = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 4,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": "0".repeat(64),
            }
        }),
    );
    assert_eq!(bad_lock["ok"], false);
    assert_eq!(bad_lock["error"]["code"], "integrity_mismatch");
    assert_eq!(bad_lock["error"]["status"], 409);

    // 3c. catalogPath outside permittedRoots is refused (`409 evidence_refused`)
    let unpermitted_cat = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 41,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
            }
        }),
    );
    assert_eq!(unpermitted_cat["ok"], false);
    assert_eq!(unpermitted_cat["error"]["code"], "evidence_refused");
    assert_eq!(unpermitted_cat["error"]["status"], 409);

    // 3d. retainReceiptsDir outside permittedRoots is refused BEFORE creating directories
    let escaped_receipts = outside_dir.join("should-not-be-created");
    let unpermitted_retain = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 42,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
                "retainReceiptsDir": escaped_receipts,
            }
        }),
    );
    assert_eq!(unpermitted_retain["ok"], false);
    assert_eq!(unpermitted_retain["error"]["code"], "evidence_refused");
    assert_eq!(unpermitted_retain["error"]["status"], 409);
    assert!(!escaped_receipts.exists());

    // 3e. Tampered catalog.ddk.json with valid catalog.lock.json is refused (`409 integrity_mismatch`)
    let tampered_cat_path = catalog_dir.join("tampered-catalog.ddk.json");
    std::fs::write(
        &tampered_cat_path,
        b"{\"kind\":\"catalog\",\"tampered\":true}\n",
    )
    .unwrap();
    let tampered_cat_resp = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 43,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
                "catalogPath": tampered_cat_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
            }
        }),
    );
    assert_eq!(tampered_cat_resp["ok"], false);
    assert_eq!(tampered_cat_resp["error"]["code"], "integrity_mismatch");
    assert_eq!(tampered_cat_resp["error"]["status"], 409);

    // 4a. docspecBinary pointing to an arbitrary host file (not current_exe) is refused (`409 evidence_refused`)
    let non_self_bin = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 44,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
                "catalogPath": catalog_path,
                "lockPath": lock_path,
                "expectedLockSha256": lock_sha,
                "docspecBinary": lock_path,
            }
        }),
    );
    assert_eq!(non_self_bin["ok"], false);
    assert_eq!(non_self_bin["error"]["code"], "evidence_refused");
    assert_eq!(non_self_bin["error"]["status"], 409);

    // 4b. Tampered docspecSha256 fails with integrity_mismatch (409)
    let bad_bin = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 5,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
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

    // 5. Valid init succeeds and pins binarySha256 + lockSha256 + creates nested retainReceiptsDir inside permittedRoots
    let init_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 6,
            "method": "init",
            "params": {
                "permittedRoots": [data_dir, catalog_dir, receipts_root],
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

    // 6a. read_page refuses shard missing sha256 (`409 integrity_mismatch`)
    let missing_shard_sha = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 61,
            "method": "read_page",
            "params": {
                "shards": [{"path": full_parquet}],
                "offset": 0,
                "limit": 2,
            }
        }),
    );
    assert_eq!(missing_shard_sha["ok"], false);
    assert_eq!(missing_shard_sha["error"]["code"], "integrity_mismatch");
    assert_eq!(missing_shard_sha["error"]["status"], 409);

    // 6b. read_page refuses heterogeneous shard schemas (`422 owner_schema_mismatch`)
    let hetero_shards = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 62,
            "method": "read_page",
            "params": {
                "shards": [
                    shards_json[0].clone(),
                    {
                        "path": mismatch_shard_path,
                        "sha256": mismatch_sha,
                        "byteSize": mismatch_bytes.len(),
                        "rowCount": 1,
                        "rowRange": [2, 3]
                    }
                ],
                "offset": 0,
                "limit": 2,
            }
        }),
    );
    assert_eq!(hetero_shards["ok"], false);
    assert_eq!(hetero_shards["error"]["code"], "owner_schema_mismatch");
    assert_eq!(hetero_shards["error"]["status"], 422);

    // 6c. read_page across 3 shards (row_group_size=1) with offset=0, limit=4
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
    assert_eq!(page_rows[1]["id"], json!({"$number": "-9007199254740995"}));
    assert_eq!(page_rows[1]["amount"], json!({"$number": "-0.0050"}));
    assert_eq!(page_rows[2]["payload"], Value::Null);
    assert_eq!(page_rows[2]["nested"], Value::Null);
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

    // 8a. run_query refuses omitted input sha256 (`409 integrity_mismatch`)
    let no_in_sha = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 81,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:high-precision",
                "expectedSqlSha256": sql_sha,
                "versions": {"ledger": "urn:ver:ledger:v1"},
                "inputs": {
                    "ledger": {
                        "path": full_parquet,
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {"min_id": 42, "min_amount": 100},
                "limit": 10
            }
        }),
    );
    assert_eq!(no_in_sha["ok"], false);
    assert_eq!(no_in_sha["error"]["code"], "integrity_mismatch");
    assert_eq!(no_in_sha["error"]["status"], 409);

    // 8b. run_query refuses substituting another permitted Parquet file for a pinned version (`409 binding_mismatch`)
    let swapped_input = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 82,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:high-precision",
                "expectedSqlSha256": sql_sha,
                "versions": {"ledger": "urn:ver:ledger:v1"},
                "inputs": {
                    "ledger": {
                        "path": shards_json[0]["path"],
                        "sha256": shards_json[0]["sha256"],
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {"min_id": 42, "min_amount": 100},
                "limit": 10
            }
        }),
    );
    assert_eq!(swapped_input["ok"], false);
    assert_eq!(swapped_input["error"]["code"], "binding_mismatch");
    assert_eq!(swapped_input["error"]["status"], 409);

    // 8c. run_query refuses non-boolean wrapLimit (`400 invalid_request`) and mismatched rawNumericParameters (`400 invalid_parameters`)
    let bad_wrap = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 83,
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
                "parameters": {"min_id": 42, "min_amount": 100},
                "wrapLimit": "false",
                "limit": 10
            }
        }),
    );
    assert_eq!(bad_wrap["ok"], false);
    assert_eq!(bad_wrap["error"]["code"], "invalid_request");
    assert_eq!(bad_wrap["error"]["status"], 400);

    let mismatched_raw = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 84,
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
                "parameters": {"min_id": 42, "min_amount": 100},
                "rawNumericParameters": {"min_id": "999999"},
                "limit": 10
            }
        }),
    );
    assert_eq!(mismatched_raw["ok"], false);
    assert_eq!(mismatched_raw["error"]["code"], "invalid_parameters");
    assert_eq!(mismatched_raw["error"]["status"], 400);

    // 8d. run_query in-process with 64-bit integer `?` and 20-digit Decimal `$min_amount` + retained receipt
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

    // 8e. Adversarial SQL recipe with nested block comments, E'a\'b', backticks, $$a'b$$,
    //     trailing comment after `;`, <=15-digit exact Decimal128 (`12345.6700`), and union type
    let inj_payload = "*/ UNION ALL SELECT 999, 0, 'x', 'y', 'z', 0 --";
    let adv_resp = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 91,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:adversarial",
                "expectedSqlSha256": adv_sql_sha,
                "versions": {"ledger": "urn:ver:ledger:v1"},
                "inputs": {
                    "ledger": {
                        "path": full_parquet,
                        "sha256": full_sha,
                        "versionId": "urn:ver:ledger:v1"
                    }
                },
                "parameters": {
                    "inj": inj_payload,
                    "exact_dec": {"$number": "12345.6700"},
                    "code": 77
                },
                "limit": 10
            }
        }),
    );
    assert_eq!(adv_resp["ok"], true, "{adv_resp}");
    let adv_rows = adv_resp["result"]["rows"].as_array().unwrap();
    assert_eq!(adv_rows.len(), 1);
    assert_eq!(adv_rows[0]["id"], json!({"$number": "9007199254740995"}));
    assert_eq!(adv_rows[0]["amount"], json!({"$number": "12345.6700"}));
    assert_eq!(adv_rows[0]["esc_lit"], json!("a'b :inj"));
    assert_eq!(adv_rows[0]["dollar_lit"], json!("a'b $exact_dec"));
    assert_eq!(adv_rows[0]["bound_payload"], json!(inj_payload));
    assert_eq!(adv_rows[0]["bound_code"], json!({"$number": "77"}));

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

#[test]
fn owner_worker_handles_tight_budgets_adjacent_tokens_nullable_decimals_symlinks_and_multishards() {
    assert_eq!(
        docspec::ErrorKind::from_code("existing_output"),
        docspec::ErrorKind::Conflict
    );

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let data_dir = root.join("data");
    let catalog_dir = root.join("catalog");
    let locks_dir = catalog_dir.join("locks");
    let receipts_root = root.join("receipts_root");
    let outside_dir = root.join("outside_unpermitted");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(catalog_dir.join("recipes")).unwrap();
    std::fs::create_dir_all(&locks_dir).unwrap();
    std::fs::create_dir_all(&receipts_root).unwrap();
    std::fs::create_dir_all(&outside_dir).unwrap();

    // Write full single-file Parquet + 2 multi-shard Parquet files
    let full_parquet = data_dir.join("full.parquet");
    let full_batch = build_owner_test_batch(0..5);
    write_parquet_with_row_group_size(&full_parquet, &full_batch, 2);
    let full_sha = sha256_file_hex(&full_parquet);

    let shard0 = data_dir.join("multi-0.parquet");
    let shard1 = data_dir.join("multi-1.parquet");
    write_parquet_with_row_group_size(&shard0, &build_owner_test_batch(0..1), 1);
    write_parquet_with_row_group_size(&shard1, &build_owner_test_batch(1..2), 1);
    let shard0_sha = sha256_file_hex(&shard0);
    let shard1_sha = sha256_file_hex(&shard1);
    let shard0_len = std::fs::metadata(&shard0).unwrap().len();

    // Whitespace-free adjacent placeholders (`SELECT?AS`, `THEN:tag`, `id>=?AND`) + nullable number COALESCE on >53-bit Decimal128
    let compact_sql = "SELECT?AS echoed, id, COALESCE(amount,:fallback_num) AS exact_amt, \
                       CASE WHEN id>0 THEN:tag ELSE'none' END AS tag_out \
                       FROM ledger WHERE id>=?AND amount>0 ORDER BY id ASC;";
    let compact_rel = "recipes/compact.sql";
    std::fs::write(catalog_dir.join(compact_rel), compact_sql.as_bytes()).unwrap();
    let compact_sha = sha256_hex(compact_sql.as_bytes());

    let catalog_doc = json!({
        "specVersion": "0.1",
        "kind": "catalog",
        "id": "urn:test:r2-catalog",
        "version": "1.0.0",
        "audience": "local",
        "datasets": [{"id": "urn:ds:ledger", "alias": "ledger"}],
        "versions": [
            {
                "id": "urn:ver:single",
                "dataset": "ledger",
                "retention": "present",
                "reference": {"scheme": "urn:ddk:version:lock-pinned", "value": "single"}
            },
            {
                "id": "urn:ver:multi",
                "dataset": "ledger",
                "retention": "present",
                "reference": {"scheme": "urn:ddk:version:lock-pinned", "value": "multi"}
            }
        ],
        "representations": [
            {"id": "urn:rep:single", "version": "urn:ver:single"},
            {"id": "urn:rep:multi", "version": "urn:ver:multi"}
        ],
        "releases": [],
        "relationships": [],
        "recipes": [{
            "id": "urn:recipe:compact",
            "sql": compact_rel,
            "dialect": "datafusion",
            "inputs": {"ledger": "ledger"},
            "relationships": [],
            "parameterOrder": ["echo_val", "min_id"],
            "parameters": {
                "type": "object",
                "additionalProperties": false,
                "required": ["echo_val", "min_id", "fallback_num", "tag"],
                "properties": {
                    "echo_val": {"type": "string"},
                    "min_id": {"type": "integer"},
                    "fallback_num": {"type": ["number", "null"]},
                    "tag": {"type": ["string", "null"]}
                }
            }
        }],
        "files": [
            {"path": compact_rel, "role": "recipe", "sha256": compact_sha}
        ]
    });
    let catalog_path = catalog_dir.join("catalog.ddk.json");
    let catalog_bytes = serde_json::to_vec_pretty(&catalog_doc).unwrap();
    std::fs::write(&catalog_path, &catalog_bytes).unwrap();
    let catalog_sha = sha256_hex(&catalog_bytes);

    // Put lock in `catalog/locks/catalog.lock.json` with paths relative to `lock_dir` (`../` and `../../data/`)
    let lock_doc = json!({
        "specVersion": "0.1",
        "kind": "lock",
        "id": "urn:test:r2-lock",
        "files": [
            {"role": "data", "identity": "urn:rep:single", "path": "../../data/full.parquet", "sha256": full_sha},
            {"role": "data", "identity": "urn:rep:multi", "path": "../../data/multi-0.parquet", "sha256": shard0_sha},
            {"role": "data", "identity": "urn:rep:multi", "path": "../../data/multi-1.parquet", "sha256": shard1_sha}
        ],
        "extensions": {
            "urn:ddk:catalog:output-set": [
                {"path": "../catalog.ddk.json", "role": "catalog", "sha256": catalog_sha},
                {"path": compact_rel, "role": "recipe", "sha256": compact_sha}
            ]
        }
    });
    let lock_path = locks_dir.join("catalog.lock.json");
    let lock_bytes = serde_json::to_vec_pretty(&lock_doc).unwrap();
    std::fs::write(&lock_path, &lock_bytes).unwrap();
    let lock_sha = sha256_hex(&lock_bytes);

    let bin_path = Path::new(env!("CARGO_BIN_EXE_docspec"));
    let mut child = Command::new(bin_path)
        .arg("owner-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // 1. Dangling symlink inside permitted root pointing outside permittedRoots must NOT create outside directory
    #[cfg(unix)]
    {
        let escaped_target = outside_dir.join("escaped_target");
        let dangling_link = receipts_root.join("dangling_escape");
        std::os::unix::fs::symlink(&escaped_target, &dangling_link).unwrap();
        let symlink_init = send_rpc(
            &mut stdin,
            &mut stdout,
            json!({
                "id": 1,
                "method": "init",
                "params": {
                    "permittedRoots": [&data_dir, &catalog_dir, &receipts_root],
                    "catalogPath": &catalog_path,
                    "lockPath": &lock_path,
                    "expectedLockSha256": &lock_sha,
                    "retainReceiptsDir": dangling_link.join("sub"),
                }
            }),
        );
        assert_eq!(symlink_init["ok"], false);
        assert!(
            !escaped_target.exists(),
            "dangling symlink must never create directories outside permittedRoots"
        );
    }

    // 2. Init with tight Limits (`memory_bytes = 32 MiB`, `max_batch_bytes = 16 MiB` -> `shared_half == max_batch_bytes`)
    let init_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 2,
            "method": "init",
            "params": {
                "permittedRoots": [&data_dir, &catalog_dir, &receipts_root],
                "catalogPath": &catalog_path,
                "lockPath": &lock_path,
                "expectedLockSha256": &lock_sha,
                "retainReceiptsDir": receipts_root.join("audit"),
                "docspecLimits": {
                    "memory_bytes": 33_554_432,
                    "max_batch_bytes": 16_777_216,
                    "max_metadata_bytes": 4_194_304
                }
            }
        }),
    );
    assert_eq!(init_ok["ok"], true, "{init_ok}");

    // 3. `read_page` succeeds under tight `shared_half == max_batch_bytes`
    let page_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 3,
            "method": "read_page",
            "params": {
                "shards": [{
                    "path": &shard0,
                    "sha256": &shard0_sha,
                    "byteSize": shard0_len,
                    "rowCount": 1,
                    "rowRange": [0, 1]
                }],
                "offset": 0,
                "limit": 10
            }
        }),
    );
    assert_eq!(page_ok["ok"], true, "{page_ok}");
    assert_eq!(page_ok["result"]["rows"].as_array().unwrap().len(), 1);

    // 4. `run_query` against multi-shard version (`urn:ver:multi`) refuses partial single-file binding (`409 binding_mismatch`)
    let multi_refused = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 4,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:compact",
                "expectedSqlSha256": &compact_sha,
                "versions": {"ledger": "urn:ver:multi"},
                "inputs": {
                    "ledger": {
                        "path": &shard0,
                        "sha256": &shard0_sha,
                        "versionId": "urn:ver:multi"
                    }
                },
                "parameters": {
                    "echo_val": "hi",
                    "min_id": {"$number": "9007199254740993"},
                    "fallback_num": null,
                    "tag": "ok"
                },
                "limit": 10
            }
        }),
    );
    assert_eq!(multi_refused["ok"], false);
    assert_eq!(multi_refused["error"]["code"], "binding_mismatch");
    assert_eq!(multi_refused["error"]["status"], 409);

    // 5. `run_query` against single-shard lock-pinned version (`urn:ver:single` resolved via `locks/` `../../data/full.parquet`)
    //    with tight budget, adjacent SQL tokens, and `fallback_num: null` preserving exact >53-bit `Decimal128`!
    let query_ok = send_rpc(
        &mut stdin,
        &mut stdout,
        json!({
            "id": 5,
            "method": "run_query",
            "params": {
                "recipeId": "urn:recipe:compact",
                "expectedSqlSha256": &compact_sha,
                "versions": {"ledger": "urn:ver:single"},
                "inputs": {
                    "ledger": {
                        "path": &full_parquet,
                        "sha256": &full_sha,
                        "versionId": "urn:ver:single"
                    }
                },
                "parameters": {
                    "echo_val": "hi",
                    "min_id": {"$number": "9007199254740993"},
                    "fallback_num": null,
                    "tag": "ok"
                },
                "limit": 10
            }
        }),
    );
    assert_eq!(query_ok["ok"], true, "{query_ok}");
    let rows = query_ok["result"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["echoed"], json!("hi"));
    assert_eq!(rows[0]["tag_out"], json!("ok"));
    assert_eq!(
        rows[0]["exact_amt"],
        json!({"$number": "1234567890123456.7891"}),
        "nullable number parameter must not coerce Decimal128 to Float64"
    );

    drop(stdin);
    assert!(child.wait().unwrap().success());
}
