//! Measurement logging for docspec jobs.
// Rust guideline compliant 2026-02-21
use crate::budget::Limits;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

/// One executed `HashJoinExec` observed in a statement's physical plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSummary {
    pub join_type: String,
    pub partition_mode: String,
    pub build_step: Option<String>,
    pub build_rows: Option<u64>,
}

/// Batches retained-input compaction copied, and view copies it was refused.
///
/// One counter per job session: the compaction wraps planned in it count into
/// it, and the step loop takes the counts after each step for its event.
#[derive(Debug, Default)]
pub(crate) struct Compactions {
    copied: AtomicUsize,
    refused: AtomicUsize,
}

impl Compactions {
    /// Installs a counter in `context`'s session and returns it.
    pub(crate) fn install(context: &datafusion::prelude::SessionContext) -> Arc<Self> {
        let counts = Arc::new(Self::default());
        context
            .state_ref()
            .write()
            .config_mut()
            .set_extension(counts.clone());
        counts
    }

    pub(crate) fn record_copy(&self) {
        self.copied.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_refusal(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns (copied batches, refused copies) since the last take, and restarts both.
    pub(crate) fn take(&self) -> (usize, usize) {
        (
            self.copied.swap(0, Ordering::Relaxed),
            self.refused.swap(0, Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
tokio::task_local! {
    static TEST_SINK: Arc<std::sync::Mutex<Vec<serde_json::Value>>>;
}

/// Captures metric events emitted while `future` runs on the current Tokio task.
#[cfg(test)]
pub(crate) async fn capture_events<F: std::future::Future>(
    future: F,
) -> (F::Output, Vec<serde_json::Value>) {
    let sink = Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = TEST_SINK.scope(sink.clone(), future).await;
    let events = sink.lock().expect("metric test sink").clone();
    (output, events)
}

pub fn now_epoch_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Peak resident set size of the process in bytes, read from mimalloc's process stats.
pub fn process_peak_rss_bytes() -> u64 {
    mimalloc::MiMalloc::stats_json()
        .ok()
        .and_then(|stats| {
            stats
                .to_str()
                .ok()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        })
        .and_then(|value| value["process"]["rss_peak"].as_u64())
        .unwrap_or(0)
}

/// Current `(soft, hard)` open-file descriptor limits (`RLIMIT_NOFILE`), where finite.
pub fn open_file_limits() -> (Option<u64>, Option<u64>) {
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    (limit.current, limit.maximum)
}

pub fn get_log_path() -> Option<String> {
    std::env::var("DOCSPEC_STEP_LOG")
        .or_else(|_| std::env::var("DOCSPEC_METRICS_LOG"))
        .ok()
}

fn write_jsonl(val: &serde_json::Value) {
    #[cfg(test)]
    let _ = TEST_SINK.try_with(|sink| {
        sink.lock().expect("metric test sink").push(val.clone());
    });
    if let Some(path) = get_log_path()
        && let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path)
    {
        let _ = writeln!(f, "{}", val);
    }
    eprintln!("[docspec-metric] {}", val);
}

pub fn log_start(limits: &Limits, scratch: &Path) {
    let (soft, hard) = open_file_limits();
    write_jsonl(&start_event(
        limits,
        scratch,
        (soft, hard),
        now_epoch_secs(),
    ));
}

fn start_event(
    limits: &Limits,
    scratch: &Path,
    (open_files_soft, open_files_hard): (Option<u64>, Option<u64>),
    epoch: f64,
) -> serde_json::Value {
    let half = limits.memory_bytes / 2;
    serde_json::json!({
        "event": "start",
        "limits": limits,
        "pool_bytes": half,
        "shared_admission_bytes": half,
        "retention_ceiling_bytes": half / 4,
        "target_partitions": limits.compute_threads,
        "scratch": scratch.display().to_string(),
        "open_files_soft": open_files_soft,
        "open_files_hard": open_files_hard,
        "epoch": epoch,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn log_step(
    step_index: usize,
    name: &str,
    storage: &str,
    rows: usize,
    charged_bytes: usize,
    start_epoch: f64,
    end_epoch: f64,
    duration_secs: f64,
    pool_before: usize,
    pool_after: usize,
    pool_peak: usize,
    shared_available_min: usize,
    compactions: (usize, usize),
    process_peak_rss_bytes: u64,
    joins: &[JoinSummary],
) {
    write_jsonl(&step_event(
        step_index,
        name,
        storage,
        rows,
        charged_bytes,
        start_epoch,
        end_epoch,
        duration_secs,
        pool_before,
        pool_after,
        pool_peak,
        shared_available_min,
        compactions,
        process_peak_rss_bytes,
        joins,
    ));
}

/// The step event; `compactions` is (copied batches, refused copies) from [`Compactions::take`].
#[allow(clippy::too_many_arguments)]
fn step_event(
    step_index: usize,
    name: &str,
    storage: &str,
    rows: usize,
    charged_bytes: usize,
    start_epoch: f64,
    end_epoch: f64,
    duration_secs: f64,
    pool_before: usize,
    pool_after: usize,
    pool_peak: usize,
    shared_available_min: usize,
    (copied, refused): (usize, usize),
    process_peak_rss_bytes: u64,
    joins: &[JoinSummary],
) -> serde_json::Value {
    serde_json::json!({
        "event": "step",
        "step_index": step_index,
        "name": name,
        "storage": storage,
        "rows": rows,
        "charged_bytes": charged_bytes,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "duration_seconds": duration_secs,
        "pool_reserved_before": pool_before,
        "pool_reserved_after": pool_after,
        // The pool's highest total while the step ran: retained results plus
        // the step's own operators, in charged bytes.
        "pool_reserved_peak": pool_peak,
        // The shared semaphore's lowest available balance while the step ran.
        "shared_available_min": shared_available_min,
        // Batches retained-input compaction copied, and view copies the
        // shared budget refused (those batches went on uncompacted).
        "compaction_copied_batches": copied,
        "compaction_refused_copies": refused,
        "process_peak_rss_bytes": process_peak_rss_bytes,
        "joins": joins,
    })
}

pub fn log_retain(
    name: &str,
    storage: &str,
    rows: usize,
    bytes: usize,
    pool_before: usize,
    pool_after: usize,
) {
    let val = serde_json::json!({
        "event": "retain",
        "name": name,
        "storage": storage,
        "rows": rows,
        "bytes": bytes,
        "pool_reserved_before": pool_before,
        "pool_reserved_after": pool_after,
        "epoch": now_epoch_secs(),
    });
    write_jsonl(&val);
}

pub fn log_release(
    step_index: usize,
    step_name: &str,
    released_name: &str,
    pool_before: usize,
    pool_after: usize,
) {
    let now = now_epoch_secs();
    let released_bytes = pool_before.saturating_sub(pool_after);
    let val = serde_json::json!({
        "event": "release",
        "step_index": step_index,
        "step_name": step_name,
        "released_name": released_name,
        "pool_reserved_before": pool_before,
        "pool_reserved_after": pool_after,
        "bytes_released": released_bytes,
        "epoch": now,
    });
    write_jsonl(&val);
}

#[allow(clippy::too_many_arguments)]
pub fn log_output(
    output_index: usize,
    name: &str,
    rows: u64,
    size: u64,
    start_epoch: f64,
    end_epoch: f64,
    duration_secs: f64,
    pool_before: usize,
    pool_after: usize,
    pool_peak: usize,
    shared_available_min: usize,
    compactions: (usize, usize),
    process_peak_rss_bytes: u64,
) {
    write_jsonl(&output_event(
        output_index,
        name,
        rows,
        size,
        start_epoch,
        end_epoch,
        duration_secs,
        pool_before,
        pool_after,
        pool_peak,
        shared_available_min,
        compactions,
        process_peak_rss_bytes,
    ));
}

#[allow(clippy::too_many_arguments)]
fn output_event(
    output_index: usize,
    name: &str,
    rows: u64,
    size: u64,
    start_epoch: f64,
    end_epoch: f64,
    duration_secs: f64,
    pool_before: usize,
    pool_after: usize,
    pool_peak: usize,
    shared_available_min: usize,
    (copied, refused): (usize, usize),
    process_peak_rss_bytes: u64,
) -> serde_json::Value {
    serde_json::json!({
        "event": "output",
        "output_index": output_index,
        "name": name,
        "rows": rows,
        "size": size,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "duration_seconds": duration_secs,
        "pool_reserved_before": pool_before,
        "pool_reserved_after": pool_after,
        "pool_reserved_peak": pool_peak,
        "shared_available_min": shared_available_min,
        "compaction_copied_batches": copied,
        "compaction_refused_copies": refused,
        "process_peak_rss_bytes": process_peak_rss_bytes,
    })
}

pub fn log_failed(
    statement_kind: Option<&str>,
    statement_index: Option<usize>,
    name: Option<&str>,
    code: &str,
    message: &str,
    budget_snapshot: Option<(usize, usize, usize)>,
) {
    write_jsonl(&failed_event(
        statement_kind,
        statement_index,
        name,
        code,
        message,
        budget_snapshot,
        process_peak_rss_bytes(),
        now_epoch_secs(),
    ));
}

#[allow(clippy::too_many_arguments)]
fn failed_event(
    statement_kind: Option<&str>,
    statement_index: Option<usize>,
    name: Option<&str>,
    code: &str,
    message: &str,
    budget_snapshot: Option<(usize, usize, usize)>,
    process_peak_rss_bytes: u64,
    epoch: f64,
) -> serde_json::Value {
    let (pool_reserved, pool_reserved_peak, shared_available_min) = match budget_snapshot {
        Some((reserved, peak, shared_min)) => (Some(reserved), Some(peak), Some(shared_min)),
        None => (None, None, None),
    };
    serde_json::json!({
        "event": "failed",
        "statement_kind": statement_kind,
        "statement_index": statement_index,
        "name": name,
        "code": code,
        "message": message,
        "pool_reserved": pool_reserved,
        "pool_reserved_peak": pool_reserved_peak,
        "shared_available_min": shared_available_min,
        "process_peak_rss_bytes": process_peak_rss_bytes,
        "epoch": epoch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_peak_rss_and_open_file_limits_are_readable() {
        assert!(process_peak_rss_bytes() > 0);
        let (soft, _) = open_file_limits();
        assert!(soft.is_some_and(|n| n > 0));
    }

    #[test]
    fn start_output_and_failed_events_keep_instantaneous_vs_span_epochs_distinct() {
        let limits = Limits {
            compute_threads: 4,
            memory_bytes: 16 << 30,
            scratch_bytes: 32 << 30,
            batch_rows: 8192,
            max_batch_bytes: 64 << 20,
            max_metadata_bytes: 32 << 20,
        };
        let start = start_event(
            &limits,
            Path::new("/tmp/scratch"),
            (Some(65_536), None),
            10.0,
        );
        assert_eq!(start["event"], "start");
        assert_eq!(start["pool_bytes"], 8u64 << 30);
        assert_eq!(start["shared_admission_bytes"], 8u64 << 30);
        assert_eq!(start["retention_ceiling_bytes"], 2u64 << 30);
        assert_eq!(start["target_partitions"], 4);
        assert_eq!(start["open_files_soft"], 65_536);
        assert!(start["open_files_hard"].is_null());
        assert!(start.get("start_epoch").is_none());
        assert!(start.get("end_epoch").is_none());
        assert_eq!(start["epoch"], 10.0);

        let out = output_event(
            1,
            "list",
            42,
            1024,
            2.0,
            3.5,
            1.5,
            100,
            80,
            250,
            900,
            (3, 1),
            4096,
        );
        assert_eq!(out["event"], "output");
        assert_eq!(out["pool_reserved_peak"], 250);
        assert_eq!(out["shared_available_min"], 900);
        assert_eq!(out["compaction_copied_batches"], 3);
        assert_eq!(out["compaction_refused_copies"], 1);
        assert_eq!(out["process_peak_rss_bytes"], 4096);

        let failed = failed_event(
            Some("query"),
            Some(2),
            Some("bad"),
            "invalid_sql",
            "query 2 \"bad\": duplicate",
            Some((120, 300, 700)),
            8192,
            12.0,
        );
        assert_eq!(failed["event"], "failed");
        assert_eq!(failed["statement_kind"], "query");
        assert_eq!(failed["statement_index"], 2);
        assert_eq!(failed["name"], "bad");
        assert_eq!(failed["code"], "invalid_sql");
        assert_eq!(failed["pool_reserved"], 120);
        assert_eq!(failed["pool_reserved_peak"], 300);
        assert_eq!(failed["shared_available_min"], 700);
        assert_eq!(failed["process_peak_rss_bytes"], 8192);
        assert!(failed.get("start_epoch").is_none());
        assert!(failed.get("end_epoch").is_none());
    }

    #[test]
    fn step_event_adds_memory_shared_floor_and_joins_while_keeping_every_field() {
        let joins = [JoinSummary {
            join_type: "Right".into(),
            partition_mode: "CollectLeft".into(),
            build_step: Some("few".into()),
            build_rows: Some(3),
        }];
        let event = step_event(
            3,
            "base",
            "memory",
            7,
            11,
            1.0,
            2.0,
            1.0,
            5,
            6,
            9,
            512,
            (0, 0),
            2048,
            &joins,
        );
        let fields = event.as_object().unwrap();
        let mut names = fields.keys().map(String::as_str).collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "charged_bytes",
                "compaction_copied_batches",
                "compaction_refused_copies",
                "duration_seconds",
                "end_epoch",
                "event",
                "joins",
                "name",
                "pool_reserved_after",
                "pool_reserved_before",
                "pool_reserved_peak",
                "process_peak_rss_bytes",
                "rows",
                "shared_available_min",
                "start_epoch",
                "step_index",
                "storage",
            ]
        );
        assert_eq!(fields["compaction_copied_batches"], 0);
        assert_eq!(fields["compaction_refused_copies"], 0);
        assert_eq!(fields["pool_reserved_peak"], 9);
        assert_eq!(fields["shared_available_min"], 512);
        assert_eq!(fields["process_peak_rss_bytes"], 2048);
        assert_eq!(fields["joins"][0]["build_step"], "few");
        assert_eq!(fields["joins"][0]["build_rows"], 3);
    }
}
