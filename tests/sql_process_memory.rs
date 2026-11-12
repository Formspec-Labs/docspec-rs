//! Check observed process memory for SQL over shared or decoded string views.
// Rust guideline compliant 2026-02-21
#![cfg(any(target_os = "macos", target_os = "linux"))]

use arrow::{
    array::{Array, ListArray, StringArray, StringViewArray, StructArray, UInt32Array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use docspec::{budget::Limits, job::Job};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{path::Path, process::Command, sync::Arc};

const MEMORY: usize = 128 << 20;
// Allow a fixed 64 MiB for executable/runtime/allocator overhead, separately
// from the configured 128 MiB shared execution and custom-admission allowance.
const OVERHEAD: usize = 64 << 20;
const ROWS: usize = 2048;
const INPUT: &str = "DOCSPEC_TEST_VIEW_MEMORY_INPUT";

// Match the ordinary CLI allocator for this isolated process qualification.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn run_projection(path: &Path) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let job = Job::new(
                Limits {
                    memory_bytes: MEMORY,
                    max_batch_bytes: 4 << 20,
                    batch_rows: ROWS,
                    compute_threads: 1,
                    ..Limits::default()
                },
                root.path().join("scratch"),
            )
            .unwrap();
            job.register_parquet("source", path).await.unwrap();
            let batches = job
                .sql("SELECT json_get_many(raw,paths) AS selected FROM source")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                ROWS
            );
            for batch in batches {
                let selected = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                let values = selected
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(values.len(), batch.num_rows() * 2);
                for row in 0..selected.len() {
                    let offset = selected.value_offsets()[row] as usize;
                    assert_eq!(values.value(offset), "1");
                    assert!(values.is_null(offset + 1));
                }
            }
        });
}

#[test]
fn repeated_dictionary_views_respect_process_memory() {
    if let Some(path) = std::env::var_os(INPUT) {
        run_projection(Path::new(&path));
        return;
    }
    // Prepare the compressed fixture outside the measured child. One shared
    // value represents >2 GiB of logical text, without allocating that copy.
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("repeated.parquet");
    let raw = format!(r#"{{"keep":1,"padding":"{}"}}"#, "x".repeat(1 << 20));
    let one = StringViewArray::from(vec![raw]);
    let values = arrow::compute::take(&one, &UInt32Array::from(vec![0; ROWS]), None).unwrap();
    assert!(values.get_array_memory_size() < 2 << 20);
    let paths = Arc::new(StringViewArray::from_iter_values(
        (0..ROWS).map(|row| format!(r#"[["keep"],["missing_{row}"]]"#)),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("raw", DataType::Utf8View, false),
        Field::new("paths", DataType::Utf8View, false),
    ]));
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(2 << 20)
        .set_max_row_group_row_count(Some(ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, vec![values, paths]).unwrap())
        .unwrap();
    writer.close().unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < 2 << 20);

    measure_child(
        "repeated_dictionary_views_respect_process_memory",
        INPUT,
        &path,
        MEMORY,
        MEMORY + OVERHEAD,
    );
}

/// Runs `test` alone in a child process with `variable` set and returns its peak RSS.
fn measure_child(
    test: &str,
    variable: &str,
    value: impl AsRef<std::ffi::OsStr>,
    configured: usize,
    ceiling: usize,
) -> usize {
    let mut command = Command::new("/usr/bin/time");
    #[cfg(target_os = "macos")]
    command.arg("-l");
    #[cfg(target_os = "linux")]
    command.args(["-f", "DOCSPEC_MAXRSS_KIB=%M"]);
    let output = command
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--include-ignored"])
        .env(variable, value)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    for line in stdout
        .lines()
        .filter(|line| line.contains("json_case") || line.contains("slice_case"))
    {
        println!("{line}");
    }
    for line in stderr
        .lines()
        .filter(|line| line.contains("JSON-lines decoded allocation") || line.contains(" real "))
    {
        println!("{line}");
    }
    #[cfg(target_os = "macos")]
    let peak = stderr
        .lines()
        .find(|line| line.contains("maximum resident set size"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    #[cfg(target_os = "linux")]
    let peak = stderr
        .lines()
        .find_map(|line| line.strip_prefix("DOCSPEC_MAXRSS_KIB="))
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024;
    println!(
        "peak_process_memory_bytes={peak}, configured_memory_bytes={configured}, fixed_overhead_bytes={OVERHEAD}"
    );
    assert!(peak <= ceiling, "{stdout}\n{stderr}");
    #[cfg(target_os = "macos")]
    {
        let footprint = stderr
            .lines()
            .find(|line| line.contains("peak memory footprint"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        println!("peak_physical_footprint_bytes={footprint}");
        assert!(footprint <= ceiling, "{stdout}\n{stderr}");
    }
    peak
}

const SLICE_CASE: &str = "DOCSPEC_TEST_SLICE_MEMORY_CASE";
/// About 150 MiB retained per partition at two partitions.
const SLICE_ROWS: usize = 600_000;

/// One measured child: a payload (`list` or `view`) and an arm. `base` stops
/// after retaining the inputs; `on` and `off` then run the LEFT JOIN chain
/// with and without retained-input compaction.
#[cfg(feature = "test-util")]
fn run_slice_case(case: &str) {
    let (payload, arm) = case.split_once(' ').unwrap();
    let column = match payload {
        "list" => {
            "make_array(lpad(cast(value AS varchar), 150, 'x'), \
             lpad(cast(value AS varchar), 150, 'y'))"
        }
        "view" => "lpad(cast(value AS varchar), 300, 'x')",
        other => unreachable!("{other}"),
    };
    // Retained steps hold text; raw Parquet inputs are read as views.
    let big = if payload == "view" {
        "(SELECT k, arrow_cast(body, 'Utf8View') AS body, \
         arrow_cast(payload, 'Utf8View') AS payload FROM big) AS big"
    } else {
        "big"
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let mut job = Job::new(
                Limits {
                    compute_threads: 2,
                    // Room for the uncompacted arm's inflated charge.
                    memory_bytes: 48 << 30,
                    ..Limits::default()
                },
                root.path().join("scratch"),
            )
            .unwrap();
            if arm == "off" {
                job.without_retained_compaction();
            }
            let (storage, _, retained) = job
                .materialize_detailed(
                    "big",
                    &format!(
                        "SELECT value AS k, lpad(cast(value AS varchar), 200, 'b') AS body, \
                         {column} AS payload FROM generate_series(1, {SLICE_ROWS}) AS t(value)"
                    ),
                )
                .await
                .unwrap();
            assert_eq!(storage, docspec::job::StepStorage::Memory);
            for (name, sql) in [
                (
                    "mid",
                    format!(
                        "SELECT CASE WHEN value <= 2000 THEN value ELSE -value END AS k \
                         FROM generate_series(1, {SLICE_ROWS}) AS t(value)"
                    ),
                ),
                (
                    "tiny",
                    format!(
                        "SELECT CASE WHEN value = 1 THEN 1 ELSE -1000000 - value END AS k \
                         FROM generate_series(1, {SLICE_ROWS}) AS t(value)"
                    ),
                ),
            ] {
                job.materialize(name, &sql).await.unwrap();
            }
            if arm != "base" {
                job.restart_pool_peak();
                let start = std::time::Instant::now();
                let (_, rows, bytes) = job
                    .materialize_detailed(
                        "chain",
                        &format!(
                            "SELECT big.k, big.body, big.payload FROM {big} \
                             LEFT JOIN mid ON big.k = mid.k LEFT JOIN tiny ON big.k = tiny.k"
                        ),
                    )
                    .await
                    .unwrap();
                assert_eq!(rows, SLICE_ROWS);
                println!(
                    "slice_case payload={payload} arm={arm} retained_bytes={retained} \
                     chain_bytes={bytes} pool_reserved_peak={} seconds={:.1}",
                    job.pool_peak(),
                    start.elapsed().as_secs_f64()
                );
            }
        });
}

/// Real and charged memory of a chained LEFT JOIN, with compaction on and off.
///
/// The measurement behind the outer-join wrap: peak RSS of a child that
/// retains about 300 MiB and stops (`base`), against children that then run
/// the chain with the rule (`on`) and without it (`off`), with the pool's
/// peak while the chain ran. Run it alone, since timing shares the machine:
/// `cargo test --all-features --test sql_process_memory -- --ignored --nocapture slice`.
/// On 2026-11-10 each child peaked under 1.8 GiB RSS and the run took 76 s.
///
/// What it measures depends on the plan: `mid` and `tiny` each carry
/// `SLICE_ROWS` keys (only 2,000 and 1 positive matches, the rest negative) so
/// `JoinSelection` sees equal row counts (`left == right`) and keeps `big` as
/// the `Partitioned` `Left` build side of the first join and its sliced
/// unmatched rows as the build input of the second join.
#[cfg(feature = "test-util")]
#[test]
#[ignore = "measurement: six children of up to 2 GiB each"]
fn outer_join_slice_compaction_real_and_charged_memory() {
    if let Some(case) = std::env::var_os(SLICE_CASE) {
        run_slice_case(case.to_str().unwrap());
        return;
    }
    for payload in ["list", "view"] {
        for arm in ["base", "on", "off"] {
            let peak = measure_child(
                "outer_join_slice_compaction_real_and_charged_memory",
                SLICE_CASE,
                format!("{payload} {arm}"),
                48 << 30,
                8 << 30,
            );
            println!("slice_peak payload={payload} arm={arm} peak_rss_bytes={peak}");
        }
    }
}

const AGGREGATE_INPUT: &str = "DOCSPEC_TEST_ORDERED_AGGREGATE_INPUT";
const AGGREGATE_ROWS: usize = 400;

fn committee_code(row: usize) -> String {
    format!(
        "hs{}{:02}",
        if row % 2 == 1 { "cn" } else { "ag" },
        row % 100
    )
}

fn run_ordered_aggregate(path: &Path) {
    // ct-deg8: DataFusion 53.1 types the computed THEN as Utf8View and casts
    // the ELSE into one view buffer shared by the batch.
    let code = "lower(c['system_code'])";
    let sql = format!(
        "WITH r AS (SELECT event_id, unnest(raw) AS c, \
         unnest(range(0, cast(array_length(raw) AS BIGINT))) AS idx FROM source) \
         SELECT event_id, array_agg(named_struct('s', CASE WHEN {code} = 'hsag00' \
         THEN 'hl' || substr({code}, 3) ELSE {code} END, 'r', c['role']) ORDER BY idx) AS a \
         FROM r GROUP BY event_id"
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let job = Job::new(
                Limits {
                    memory_bytes: MEMORY,
                    compute_threads: 2,
                    ..Limits::default()
                },
                root.path().join("scratch"),
            )
            .unwrap();
            job.register_parquet("source", path).await.unwrap();
            let batches = job.sql(&sql).await.unwrap().collect().await.unwrap();
            let mut seen = vec![false; AGGREGATE_ROWS];
            for batch in &batches {
                let events = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let lists = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let event = usize::try_from(events.value(row)).unwrap();
                    assert!(!std::mem::replace(&mut seen[event], true));
                    let item = lists.value(row);
                    let item = item
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .unwrap();
                    assert_eq!(item.len(), 1);
                    let text = |column: usize| {
                        let value =
                            arrow::compute::cast(item.column(column), &DataType::Utf8).unwrap();
                        value
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0)
                            .to_owned()
                    };
                    let code = committee_code(event);
                    let expected = if code == "hsag00" {
                        "hlag00".to_owned()
                    } else {
                        code
                    };
                    assert_eq!(text(0), expected);
                    assert_eq!(text(1), "host");
                }
            }
            assert!(seen.into_iter().all(|seen| seen));
        });
}

#[test]
fn ordered_aggregate_over_shared_views_respects_process_memory() {
    if let Some(path) = std::env::var_os(AGGREGATE_INPUT) {
        run_ordered_aggregate(Path::new(&path));
        return;
    }
    let item = Fields::from(vec![
        Field::new("system_code", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, true),
    ]);
    let codes = StringArray::from_iter_values((0..AGGREGATE_ROWS).map(committee_code));
    let roles = StringArray::from_iter_values((0..AGGREGATE_ROWS).map(|_| "host"));
    let items = StructArray::new(item.clone(), vec![Arc::new(codes), Arc::new(roles)], None);
    let field = Arc::new(Field::new("item", DataType::Struct(item), true));
    let raw = ListArray::new(
        field,
        OffsetBuffer::from_lengths(std::iter::repeat_n(1, AGGREGATE_ROWS)),
        Arc::new(items),
        None,
    );
    let events = arrow::array::Int64Array::from_iter_values(0..AGGREGATE_ROWS as i64);
    let batch = RecordBatch::try_from_iter([
        ("event_id", Arc::new(events) as _),
        ("raw", Arc::new(raw) as _),
    ])
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("committees.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    measure_child(
        "ordered_aggregate_over_shared_views_respects_process_memory",
        AGGREGATE_INPUT,
        &path,
        MEMORY,
        MEMORY + OVERHEAD,
    );
}

const JSON_ROOT: &str = "DOCSPEC_TEST_JSON_LINES_MEMORY_ROOT";
const LARGE_LINE: usize = 187_661_339;

#[test]
fn large_json_lines_fit_decode_and_preserve_the_line_byte_ceiling() {
    if let Some(root) = std::env::var_os(JSON_ROOT) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let root=std::path::PathBuf::from(root);
            let limits=Limits {compute_threads:1,memory_bytes:10<<30,max_batch_bytes:512<<20,..Limits::default()};
            let schema=Arc::new(Schema::new(vec![Field::new("id",DataType::Int64,true),Field::new("payload",DataType::Utf8,true)]));
            {
                let job=Job::new(limits.clone(),root.join("valid-scratch")).unwrap();
                job.register_json_lines("input",vec![docspec::job::JsonLinesFile {key:"large".into(),path:root.join("valid.jsonl")}],schema.clone()).unwrap();
                let batches=job.sql("SELECT id,cast(length(payload) AS BIGINT),cast(length(__raw_line) AS BIGINT),__source_line FROM input").await.unwrap().collect().await.unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(),1);
                let batch=&batches[0];
                for (column,expected) in [7,(LARGE_LINE-b"{\"id\":7,\"payload\":\"".len()-b"\"}\n".len()) as i64,LARGE_LINE as i64,1].into_iter().enumerate() {
                    assert_eq!(batch.column(column).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0),expected);
                }
            }
            let job=Job::new(limits,root.join("oversized-scratch")).unwrap();
            job.register_json_lines("input",vec![docspec::job::JsonLinesFile {key:"oversized".into(),path:root.join("oversized.jsonl")}],schema).unwrap();
            let error=job.sql("SELECT id FROM input").await.unwrap().collect().await.unwrap_err();
            assert!(error.to_string().contains("line exceeds configured allowance"),"{error}");
        });
        return;
    }
    use std::io::Write;
    fn write_line(path: &Path, length: usize) {
        let mut file = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
        let prefix = b"{\"id\":7,\"payload\":\"";
        let suffix = b"\"}\n";
        file.write_all(prefix).unwrap();
        let chunk = [b'x'; 64 << 10];
        let mut remaining = length - prefix.len() - suffix.len();
        while remaining != 0 {
            let n = remaining.min(chunk.len());
            file.write_all(&chunk[..n]).unwrap();
            remaining -= n;
        }
        file.write_all(suffix).unwrap();
        file.flush().unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    write_line(&root.path().join("valid.jsonl"), LARGE_LINE);
    write_line(&root.path().join("oversized.jsonl"), 513 << 20);
    measure_child(
        "large_json_lines_fit_decode_and_preserve_the_line_byte_ceiling",
        JSON_ROOT,
        root.path(),
        10 << 30,
        (2 << 30) + OVERHEAD,
    );
}

const RECEIPT_CHILD: &str = "DOCSPEC_RECEIPT_SCHEMA_CHILD";
const RECEIPT_ROOT: &str = "DOCSPEC_RECEIPT_SCHEMA_REAL_ROOT";

#[test]
fn receipt_schema_charges_measured_allocations_for_both_record_shapes() {
    if let Some(spec) = std::env::var_os(RECEIPT_CHILD) {
        let case: serde_json::Value =
            serde_json::from_slice(&std::fs::read(spec).unwrap()).unwrap();
        tracing_subscriber::fmt()
            .with_env_filter("docspec::job::json_lines=debug")
            .with_writer(std::io::stderr)
            .try_init()
            .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let root = tempfile::tempdir().unwrap();
                let schema: Schema =
                    serde_json::from_str(include_str!("fixtures/json_lines_receipt_schema.json"))
                        .unwrap();
                let limits: Limits = serde_json::from_value(case["limits"].clone()).unwrap();
                let files: Vec<docspec::job::JsonLinesFile> =
                    serde_json::from_value(case["files"].clone()).unwrap();
                let job = Job::new(limits, root.path().join("scratch")).unwrap();
                job.register_json_lines("source", files, Arc::new(schema))
                    .unwrap();
                let batches = job
                    .sql("SELECT * FROM source")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
                let mut seen = std::collections::BTreeSet::new();
                for batch in batches {
                    let keys = batch
                        .column(24)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let lines = batch
                        .column(25)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap();
                    let raw = batch
                        .column(26)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        let key = keys.value(row);
                        assert!(seen.insert(key.to_owned()));
                        assert_eq!(lines.value(row), 1);
                        assert_eq!(
                            raw.value(row).len() as u64,
                            case["lengths"][key].as_u64().unwrap()
                        );
                        println!(
                            "json_case key={key} raw_line_bytes={} decoded_batch_bytes={}",
                            raw.value(row).len(),
                            batch.get_array_memory_size()
                        );
                    }
                }
            });
        return;
    }
    use std::io::{BufRead, Write};
    let root = tempfile::tempdir().unwrap();
    let mut files = Vec::new();
    let mut lengths = serde_json::Map::new();
    let real = std::env::var_os(RECEIPT_ROOT).map(std::path::PathBuf::from);
    for (key, relative, list) in [
        (
            "house",
            "house/meeting-xml/undated/assemble-20260930.jsonl.gz",
            false,
        ),
        (
            "youtube",
            "youtube/api/2026-09-29/assemble-20260930.jsonl.gz",
            true,
        ),
    ] {
        let path = root.path().join(format!("{key}.jsonl.gz"));
        let mut writer = flate2::write::GzEncoder::new(
            std::fs::File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        let length = if let Some(real) = &real {
            let mut reader = std::io::BufReader::new(flate2::read::MultiGzDecoder::new(
                std::fs::File::open(real.join(relative)).unwrap(),
            ));
            let mut length = 0;
            loop {
                let buffer = reader.fill_buf().unwrap();
                assert!(!buffer.is_empty(), "missing retained line1");
                let count = buffer
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(buffer.len(), |index| index + 1);
                let end = buffer[count - 1] == b'\n';
                writer.write_all(&buffer[..count]).unwrap();
                length += count;
                reader.consume(count);
                if end {
                    break;
                }
            }
            if key == "house" {
                assert_eq!(length - 1, 187_661_339);
            } else {
                assert!(length > 50_000_000);
            }
            length
        } else {
            // The real schema's unselected nested records must not multiply
            // admission for its two declared list fields.
            let prefix = if list {
                "{\"record\":["
            } else {
                "{\"record\":{\"unselected\":["
            };
            writer.write_all(prefix.as_bytes()).unwrap();
            let item = b"{\"unused\":0,\"nested\":[0,1,2]}";
            let count = 17_244;
            for index in 0..count {
                if index != 0 {
                    writer.write_all(b",").unwrap();
                }
                writer.write_all(item).unwrap();
            }
            let middle = if list {
                b"],\"padding\":\"".as_slice()
            } else {
                b"]},\"padding\":\"".as_slice()
            };
            writer.write_all(middle).unwrap();
            let suffix = b"\"}\n";
            let length = 8 << 20;
            let mut remaining = length
                - prefix.len()
                - count * item.len()
                - (count - 1)
                - middle.len()
                - suffix.len();
            let chunk = [b'x'; 64 << 10];
            while remaining != 0 {
                let count = remaining.min(chunk.len());
                writer.write_all(&chunk[..count]).unwrap();
                remaining -= count;
            }
            writer.write_all(suffix).unwrap();
            length
        };
        writer.finish().unwrap();
        files.push(docspec::job::JsonLinesFile {
            key: key.into(),
            path,
        });
        lengths.insert(key.into(), serde_json::json!(length));
    }
    let memory = if real.is_some() { 10 << 30 } else { 256 << 20 };
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: memory,
        max_batch_bytes: 512 << 20,
        ..Limits::default()
    };
    let spec = root.path().join("case.json");
    std::fs::write(
        &spec,
        serde_json::to_vec(&serde_json::json!({"files":files,"limits":limits,"lengths":lengths}))
            .unwrap(),
    )
    .unwrap();
    measure_child(
        "receipt_schema_charges_measured_allocations_for_both_record_shapes",
        RECEIPT_CHILD,
        &spec,
        memory,
        (2 << 30) + OVERHEAD,
    );
}
