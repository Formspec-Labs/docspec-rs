//! A failed job says which statement or input failed, and keeps the cause.
//!
//! Each statement test runs the failing statement twice: inside a job, and
//! alone through the same `Job` call. The job's failure must be the lone
//! error's code and message behind the statement's kind, position and name.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::Error;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde_json::{Value, json};
use std::sync::Arc;

/// Fails at planning: no relation is in scope.
const BROKEN: &str = "SELECT absent_column";

fn export_settings() -> Value {
    json!({"profile":"whole","row_group_rows":65536,"order_by":[{"field":"id"}],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"evidence fixture v1","footer_limit":524288})
}

/// Runs `spec` with its scratch under `root`; absent lists are empty.
async fn run(
    root: &Path,
    limits: Limits,
    mut spec: Value,
) -> std::result::Result<JobResult, JobFailure> {
    spec["scratch"] = json!(root.join("job-scratch"));
    for list in ["inputs", "steps", "outputs"] {
        if spec.get(list).is_none() {
            spec[list] = json!([]);
        }
    }
    serde_json::from_value::<JobSpec>(spec)
        .unwrap()
        .run(limits)
        .await
}

async fn failure(root: &Path, limits: Limits, spec: Value) -> JobFailure {
    match run(root, limits, spec).await {
        Ok(result) => panic!(
            "the job succeeded: {}",
            serde_json::to_string(&result).unwrap()
        ),
        Err(failure) => failure,
    }
}

/// A job for running one statement alone, with scratch apart from the spec's.
fn lone_job(root: &Path, limits: Limits) -> Job {
    Job::new(limits, root.join("lone-scratch")).unwrap()
}

fn assert_names(failure: &JobFailure, statement: &str, alone: &Error) {
    let message = format!("{statement}: {}", alone.message());
    assert_eq!(
        failure.report()["error"],
        json!({"code": alone.code(), "message": message})
    );
    assert_eq!(failure.to_string(), format!("{}: {message}", alone.code()));
}

fn write_ids(path: &Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn a_failed_step_is_named_whatever_its_kind() {
    let root = tempfile::tempdir().unwrap();
    let unusable = ComponentsSettings {
        sql: "SELECT 'a' AS a, 'b' AS b".into(),
        left: "a".into(),
        right: "b".into(),
        node_limit: 0,
    };
    let job = lone_job(root.path(), Limits::default());
    let view = job.register_view("broken", BROKEN).await.unwrap_err();
    let materialized = job.materialize("broken", BROKEN).await.unwrap_err();
    let components = job
        .connected_components("broken", &unusable)
        .await
        .unwrap_err();
    // A refusal's code must survive the naming, not only the default "query".
    assert_ne!(components.code(), view.code());
    for (step, alone) in [
        (json!({"name":"broken","sql":BROKEN}), view),
        (
            json!({"name":"broken","sql":BROKEN,"materialize":true}),
            materialized,
        ),
        (json!({"name":"broken","components":unusable}), components),
    ] {
        let spec = json!({"steps":[
            {"name":"first","sql":"SELECT 1 AS id"},
            {"name":"kept","sql":"SELECT 1 AS id","materialize":true},
            step,
        ]});
        let failure = failure(root.path(), Limits::default(), spec).await;
        assert_names(&failure, "step 2 \"broken\"", &alone);
        assert_eq!(failure.completed.steps.len(), 2);
    }
}

#[tokio::test]
async fn a_failed_output_is_named() {
    let root = tempfile::tempdir().unwrap();
    let unwritten = root.path().join("two.parquet");
    let alone = lone_job(root.path(), Limits::default())
        .export(
            BROKEN,
            &unwritten,
            &serde_json::from_value(export_settings()).unwrap(),
        )
        .await
        .unwrap_err();
    let spec = json!({"outputs":[
        {"name":"one.parquet","path":root.path().join("one.parquet"),"sql":"SELECT 1 AS id","settings":export_settings()},
        {"name":"two.parquet","path":unwritten,"sql":BROKEN,"settings":export_settings()},
    ]});
    let failure = failure(root.path(), Limits::default(), spec).await;
    assert_names(&failure, "output 1 \"two.parquet\"", &alone);
    assert_eq!(failure.completed.outputs.len(), 1);
}

#[tokio::test]
async fn a_failed_query_is_named_and_keeps_its_own_code() {
    let root = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_metadata_bytes: 1024,
        ..Limits::default()
    };
    let too_big = format!("SELECT '{}' AS text", "x".repeat(8192));
    let alone = query_result::query(&lone_job(root.path(), limits.clone()), "too_big", &too_big)
        .await
        .unwrap_err();
    assert_ne!(alone.code(), "query");
    let spec = json!({"queries":[
        {"name":"small","sql":"SELECT 1 AS id"},
        {"name":"too_big","sql":too_big},
    ]});
    let failure = failure(root.path(), limits, spec).await;
    assert_names(&failure, "query 1 \"too_big\"", &alone);
    assert_eq!(failure.completed.queries.len(), 1);
}

/// Counts rows without naming a column, so it plans against any table.
const COUNT_CAPTURES: &str = "SELECT count(*) AS n FROM captures";

fn captures_job(path: &Path, sql: &str) -> Value {
    json!({
        "inputs":[{"name":"captures","kind":"parquet","path":path}],
        "queries":[{"name":"rows","sql":sql}],
    })
}

#[tokio::test]
async fn a_failure_between_the_statement_lists_is_not_blamed_on_a_statement() {
    let root = tempfile::tempdir().unwrap();
    // Table operations run after the steps; this one reads a table nobody made.
    let spec = json!({
        "catalog_root":root.path().join("catalog"),
        "steps":[{"name":"kept","sql":"SELECT 1 AS id"}],
        "table_operations":[{"kind":"resolve","name":"head","table":{"namespace":["absent"],"name":"none"}}],
    });
    let failure = failure(root.path(), Limits::default(), spec).await;
    assert_eq!(failure.completed.steps.len(), 1);
    let message = failure.error.message();
    assert!(!message.starts_with("step "), "{message}");
}

#[tokio::test]
async fn a_parquet_input_with_no_file_is_refused_before_any_statement() {
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("absent.parquet");
    // The trailing separator makes the listing read it as a folder.
    let absent_directory = root.path().join("absent/");
    let empty_directory = root.path().join("empty");
    std::fs::create_dir(&empty_directory).unwrap();
    // The listing skips a file of no bytes, so a folder holding only one has
    // nothing to read. A lone zero-byte file is refused before the listing.
    let zero_bytes_directory = root.path().join("zero");
    std::fs::create_dir(&zero_bytes_directory).unwrap();
    std::fs::write(zero_bytes_directory.join("part-0.parquet"), b"").unwrap();
    for path in [
        absent,
        absent_directory,
        empty_directory,
        zero_bytes_directory,
    ] {
        let failure = failure(
            root.path(),
            Limits::default(),
            captures_job(&path, COUNT_CAPTURES),
        )
        .await;
        assert_eq!(
            failure.report()["error"],
            json!({"code":"missing_input","message":format!(
                "Parquet input \"captures\" has no readable Parquet file at {}: \
                 the path is missing or holds no non-empty file ending in .parquet",
                path.display()
            )})
        );
        assert!(failure.completed.queries.is_empty());
    }
}

#[tokio::test]
async fn a_parquet_file_with_no_rows_is_an_empty_table() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("empty.parquet");
    write_ids(&file, &[]);
    let directory = root.path().join("parts");
    std::fs::create_dir(&directory).unwrap();
    write_ids(&directory.join("part-0.parquet"), &[]);
    for path in [file, directory] {
        let spec = captures_job(
            &path,
            "SELECT count(*) AS n, count(id) AS ids FROM captures",
        );
        let result = run(root.path(), Limits::default(), spec).await.unwrap();
        assert_eq!(result.queries[0].data, json!([{"n":0,"ids":0}]));
    }
}

#[tokio::test]
async fn an_unreadable_parquet_file_is_refused_naming_its_input_and_path() {
    let root = tempfile::tempdir().unwrap();
    let torn = root.path().join("torn.parquet");
    std::fs::write(&torn, b"not a Parquet file").unwrap();
    let zero_bytes = root.path().join("zero.parquet");
    std::fs::write(&zero_bytes, b"").unwrap();
    // A readable Parquet file that the listing refuses for its name.
    let other_suffix = root.path().join("rows.pq");
    write_ids(&other_suffix, &[1, 2]);
    // What reading each path alone says. A single file's footer is admitted
    // before the listing, so a torn or empty file is refused there, as
    // `export_read` with no cause (admission hides transport detail on
    // purpose); a whole file with another suffix passes it and is refused by
    // the listing, with the listing's message.
    let job = lone_job(root.path(), Limits::default());
    let budget = &job.budget;
    let cases = [
        (
            &torn,
            crate::storage::admit_parquet_footer(&torn, budget)
                .await
                .unwrap_err(),
        ),
        (
            &zero_bytes,
            crate::storage::admit_parquet_footer(&zero_bytes, budget)
                .await
                .unwrap_err(),
        ),
        (
            &other_suffix,
            Error::from(
                SessionContext::new()
                    .register_parquet(
                        "captures",
                        other_suffix.to_string_lossy().as_ref(),
                        ParquetReadOptions::default(),
                    )
                    .await
                    .unwrap_err(),
            ),
        ),
    ];
    for (path, alone) in cases {
        let failure = failure(
            root.path(),
            Limits::default(),
            captures_job(path, COUNT_CAPTURES),
        )
        .await;
        let named = format!("Parquet input \"captures\" at {}", path.display());
        assert_names(&failure, &named, &alone);
    }
}

#[tokio::test]
async fn a_missing_json_lines_source_in_an_anonymous_job_fails_at_the_statement_that_reads_it() {
    let root = tempfile::tempdir().unwrap();
    let absent_path = root.path().join("absent.jsonl.gz");
    let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
    let spec = json!({
        "inputs": [{
            "name": "absent",
            "kind": "json_lines",
            "files": [{"key": "absent", "path": absent_path}],
            "schema": schema
        }],
        "steps": [{"name": "one", "sql": "SELECT 1 AS x"}],
        "queries": [
            {"name": "before", "sql": "SELECT x FROM one"},
            {"name": "q", "sql": "SELECT count(*) AS n FROM absent"}
        ]
    });
    let fail = failure(root.path(), Limits::default(), spec).await;
    assert_eq!(fail.error.code(), "query", "{}", fail.error);
    assert!(
        fail.error
            .message()
            .starts_with("query 1 \"q\": Execution error: absent line 1: source open failed"),
        "{}",
        fail.error
    );
    assert_eq!(fail.completed.queries.len(), 1);
    assert_eq!(fail.completed.queries[0].name, "before");
}

#[tokio::test]
async fn job_metric_events_record_start_join_summaries_shared_floor_and_failure() {
    let root = tempfile::tempdir().unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 256 << 20,
        batch_rows: 1024,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 4 << 20,
    };
    let half = (limits.memory_bytes / 2) as u64;
    let out_path = root.path().join("joined.parquet");
    let mut joined_export = export_settings();
    joined_export["order_by"] = json!([{"field": "k"}]);
    let spec = json!({
        "steps": [
            {"name": "big", "materialize": true, "sql": "SELECT value AS k FROM generate_series(1, 20) AS t(value)"},
            {"name": "few", "materialize": true, "sql": "SELECT value * 2 AS k, value * 10 AS v FROM generate_series(1, 3) AS t(value)"},
            {"name": "joined", "materialize": true, "sql": "SELECT big.k, few.v FROM big LEFT JOIN few ON big.k = few.k"}
        ],
        "outputs": [
            {"name": "joined_out", "path": out_path, "sql": "SELECT k, v FROM joined", "settings": joined_export}
        ],
        "queries": [
            {"name": "summary", "sql": "SELECT count(*) AS n FROM joined"}
        ]
    });
    let (res, events) =
        super::metrics::capture_events(run(root.path(), limits.clone(), spec)).await;
    res.unwrap();

    assert_eq!(events[0]["event"], "start");
    assert_eq!(events[0]["pool_bytes"], half);
    assert_eq!(events[0]["shared_admission_bytes"], half);
    assert_eq!(events[0]["retention_ceiling_bytes"], half / 4);
    assert_eq!(events[0]["target_partitions"], 2);
    assert!(events[0]["open_files_soft"].as_u64().is_some_and(|n| n > 0));
    assert!(events[0].get("start_epoch").is_none());

    let joined_step = events
        .iter()
        .find(|e| e["event"] == "step" && e["name"] == "joined")
        .expect("joined step event");
    assert!(joined_step["process_peak_rss_bytes"].as_u64().unwrap() > 0);
    assert!(joined_step["shared_available_min"].as_u64().unwrap() <= half);
    assert_eq!(
        joined_step["joins"],
        json!([{
            "join_type": "Right",
            "partition_mode": "CollectLeft",
            "build_step": "few",
            "build_rows": 3
        }])
    );

    let output_ev = events
        .iter()
        .find(|e| e["event"] == "output" && e["name"] == "joined_out")
        .expect("output event");
    assert!(output_ev["pool_reserved_peak"].as_u64().unwrap() > 0);
    assert!(
        output_ev["shared_available_min"].as_u64().unwrap() < half,
        "export reserves writer and metadata permits from the shared half: {output_ev}"
    );
    assert_eq!(output_ev["compaction_copied_batches"], 0);
    assert_eq!(output_ev["compaction_refused_copies"], 0);
    assert!(output_ev["process_peak_rss_bytes"].as_u64().unwrap() > 0);

    let query_ev = events
        .iter()
        .find(|e| e["event"] == "step" && e["name"] == "query:summary")
        .expect("query event");
    assert!(query_ev["process_peak_rss_bytes"].as_u64().unwrap() > 0);
    assert!(
        query_ev["shared_available_min"].as_u64().unwrap() < half,
        "query JSON encoding reserves from the shared half: {query_ev}"
    );

    // Failing job emits a final `failed` event naming the statement with live pool bytes.
    let fail_spec = json!({
        "steps": [
            {"name": "kept", "materialize": true, "sql": "SELECT value AS k FROM generate_series(1, 500) AS t(value)"},
            {"name": "broken", "sql": "SELECT missing_column FROM kept"}
        ]
    });
    let (fail_res, fail_events) =
        super::metrics::capture_events(run(root.path(), limits, fail_spec)).await;
    let err = fail_res.unwrap_err();
    let last = fail_events.last().expect("failed event");
    assert_eq!(last["event"], "failed");
    assert_eq!(last["statement_kind"], "step");
    assert_eq!(last["statement_index"], 1);
    assert_eq!(last["name"], "broken");
    assert_eq!(last["code"], err.error.code());
    assert!(last["pool_reserved"].as_u64().unwrap() > 0, "{last}");
    assert!(last["process_peak_rss_bytes"].as_u64().unwrap() > 0);
    assert!(last.get("start_epoch").is_none());
    assert!(last.get("end_epoch").is_none());
}

#[tokio::test]
async fn prepare_job_names_failing_step_output_and_query() {
    use sha2::{Digest, Sha256};

    let root = tempfile::tempdir().unwrap();
    let cases = [
        (
            "step 1 \"broken_step\": ",
            json!({
                "scratch": root.path().join("scratch"),
                "inputs": [],
                "steps": [
                    {"name": "ok_step", "sql": "SELECT 1 AS id"},
                    {"name": "broken_step", "sql": BROKEN}
                ],
                "outputs": []
            }),
        ),
        (
            "output 0 \"broken_out\": ",
            json!({
                "scratch": root.path().join("scratch"),
                "inputs": [],
                "steps": [{"name": "ok_step", "sql": "SELECT 1 AS id"}],
                "outputs": [
                    {"name": "broken_out", "path": root.path().join("out.parquet"), "sql": BROKEN, "settings": export_settings()}
                ]
            }),
        ),
        (
            "query 1 \"broken_query\": ",
            json!({
                "scratch": root.path().join("scratch"),
                "inputs": [],
                "steps": [{"name": "ok_step", "sql": "SELECT 1 AS id"}],
                "outputs": [],
                "queries": [
                    {"name": "ok_query", "sql": "SELECT id FROM ok_step"},
                    {"name": "broken_query", "sql": BROKEN}
                ]
            }),
        ),
    ];

    for (expected_prefix, job_json) in cases {
        let job_path = root.path().join("job.json");
        let bytes = serde_json::to_vec(&job_json).unwrap();
        std::fs::write(&job_path, &bytes).unwrap();
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let prep: PrepareJobSpec = serde_json::from_value(json!({
            "job": {"path": job_path, "sha256": sha256},
            "inputs": []
        }))
        .unwrap();
        let err = prep.prepare(Limits::default()).await.unwrap_err();
        assert_eq!(err.code(), "query", "{err}");
        assert!(
            err.message().starts_with(expected_prefix),
            "expected prefix {expected_prefix:?}, got {:?}",
            err.message()
        );
    }
}
