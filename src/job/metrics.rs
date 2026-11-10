//! Measurement logging for docspec jobs.
use std::fs::OpenOptions;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_epoch_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub fn get_log_path() -> Option<String> {
    std::env::var("DOCSPEC_STEP_LOG")
        .or_else(|_| std::env::var("DOCSPEC_METRICS_LOG"))
        .ok()
}

fn write_jsonl(val: &serde_json::Value) {
    if let Some(path) = get_log_path()
        && let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path)
    {
        let _ = writeln!(f, "{}", val);
    }
    eprintln!("[docspec-metric] {}", val);
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
) {
    let val = serde_json::json!({
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
    });
    write_jsonl(&val);
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
) {
    let val = serde_json::json!({
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
    });
    write_jsonl(&val);
}
