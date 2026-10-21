//! Realistic-scale harness: hourly native news polls against a growing archive.
//!
//! Simulates the `news.versions` append path: every poll adds `--rows` new rows
//! keyed by uuid5-shaped hashes (production `named_id` keys are SHA-1 name
//! hashes, i.e. uniformly distributed 36-character strings). Two key modes:
//!   `id`       — Update keys `["id"]`, as news_lake commits appends today;
//!   `time-id`  — Update keys `["observed_at", "id"]`, an input-derived poll
//!                instant (congressional-tech ct-xu1o).
//! The lifecycle is compacted once per simulated day (every 24 polls).
//!
//! Usage:
//!   cargo run --release --example news_poll_scale -- <id|time-id> <polls> <workspace> <csv>
//!
//! Per poll it records wall time; every `CHECKPOINT` polls it also records the
//! processing metrics (files scanned/reused), process RSS, workspace disk use and
//! the lifecycle metadata bytes after the latest compaction.

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{Id, InputSpec, PhaseResult, RunSpec, UpdateSpec},
    storage,
};
use parquet::arrow::ArrowWriter;
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Instant,
};

const ROWS_PER_POLL: usize = 2_000;
const BODY_BYTES: usize = 200;
const CHECKPOINT: usize = 60;
const HOUR_MS: i64 = 3_600_000;
const START_MS: i64 = 1_767_225_600_000; // 2026-01-01T00:00:00Z

/// uuid5-shaped key: version nibble 5, RFC 4122 variant, hash-distributed.
fn uuid5_like(name: &str) -> String {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(name.as_bytes()).as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

fn poll_input(dir: &Path, poll: usize) -> InputSpec {
    let observed = START_MS + poll as i64 * HOUR_MS;
    let ids: Vec<String> = (0..ROWS_PER_POLL)
        .map(|row| uuid5_like(&format!("version:item-{poll}-{row}:run-{poll}")))
        .collect();
    let items: Vec<String> = (0..ROWS_PER_POLL)
        .map(|row| {
            uuid5_like(&format!(
                "item:outlet-{}:https://example.com/{poll}/{row}",
                row % 400
            ))
        })
        .collect();
    let body = "x".repeat(BODY_BYTES);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("observed_at", DataType::Int64, false),
        Field::new("item_id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from(vec![observed; ROWS_PER_POLL])),
            Arc::new(StringArray::from(items)),
            Arc::new(StringArray::from(vec![body.as_str(); ROWS_PER_POLL])),
        ],
    )
    .unwrap();
    let path = dir.join(format!("poll-{poll}.parquet"));
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(&path).unwrap().0,
    }
}

fn rss_kib() -> u64 {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn disk_kib(path: &Path) -> u64 {
    let out = Command::new("du").args(["-sk"]).arg(path).output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    let polls: usize = args[2].parse().unwrap();
    let workspace = PathBuf::from(&args[3]);
    let csv_path = PathBuf::from(&args[4]);
    let keys: Vec<String> = match mode {
        "id" => vec!["id".into()],
        "time-id" => vec!["observed_at".into(), "id".into()],
        other => panic!("unknown mode {other}"),
    };
    let inputs = workspace.join("inputs");
    std::fs::create_dir_all(&inputs).unwrap();
    let limits = Limits {
        max_metadata_bytes: 64 << 20,
        scratch_bytes: 4 << 30,
        ..Limits::default()
    };
    let engine = Engine::open(workspace.join("engine"), limits)
        .await
        .unwrap();
    let dataset = Id::new("versions").unwrap();
    let mut csv = File::create(&csv_path).unwrap();
    writeln!(
        csv,
        "poll,wall_ms,files_scanned,files_reused,bytes_reused,bytes_written,rss_kib,disk_kib,lifecycle_bytes"
    )
    .unwrap();

    let first = poll_input(&inputs, 0);
    let mut release = engine
        .run(&RunSpec {
            dataset: dataset.clone(),
            operation: Id::new("poll-0").unwrap(),
            base: None,
            relationship: None,
            input: first.path.clone(),
            expected_digest: first.digest,
            sql: "SELECT * FROM input".into(),
            definition: "versions-v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    std::fs::remove_file(&first.path).ok();
    let mut lifecycle_bytes = 0_u64;
    for poll in 1..polls {
        let input = poll_input(&inputs, poll);
        let started = Instant::now();
        release = engine
            .update(&UpdateSpec {
                dataset: dataset.clone(),
                operation: Id::new(format!("poll-{poll}")).unwrap(),
                definition: "versions-v1".into(),
                base: release.id.clone(),
                keys: keys.clone(),
                replacements: input.clone(),
                relationship: None,
                deletions: None,
            })
            .await
            .unwrap();
        let wall_ms = started.elapsed().as_millis();
        std::fs::remove_file(&input.path).ok();
        let is_checkpoint = poll % CHECKPOINT == 0 || poll + 1 == polls;
        let metrics = if is_checkpoint {
            Some(
                engine
                    .inspect(&dataset)
                    .await
                    .unwrap()
                    .values()
                    .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
                    .find(|phase| phase.operation == release.id && phase.phase == "processing")
                    .map(|phase| phase.metrics)
                    .unwrap_or_default(),
            )
        } else {
            None
        };
        if poll % 6 == 0 || is_checkpoint {
            lifecycle_bytes = engine
                .compact_lifecycle(&dataset)
                .await
                .unwrap()
                .metadata_bytes_after as u64;
        }
        if let Some(metrics) = metrics {
            writeln!(
                csv,
                "{poll},{wall_ms},{},{},{},{},{},{},{lifecycle_bytes}",
                metrics.files_scanned,
                metrics.files_reused,
                metrics.bytes_reused,
                metrics.bytes_written,
                rss_kib(),
                disk_kib(&workspace),
            )
            .unwrap();
            eprintln!(
                "{mode} poll={poll} wall_ms={wall_ms} scanned={} reused={} lifecycle={lifecycle_bytes}",
                metrics.files_scanned, metrics.files_reused
            );
        } else {
            writeln!(csv, "{poll},{wall_ms},,,,,,,").unwrap();
        }
        csv.flush().unwrap();
    }
}
