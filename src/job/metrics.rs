//! Measurement logging for docspec jobs.
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

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
    compactions: (usize, usize),
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
        compactions,
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
    (copied, refused): (usize, usize),
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
        // Batches retained-input compaction copied, and view copies the
        // shared budget refused (those batches went on uncompacted).
        "compaction_copied_batches": copied,
        "compaction_refused_copies": refused,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_event_adds_compaction_counts_and_keeps_every_field() {
        let event = step_event(3, "base", "memory", 7, 11, 1.0, 2.0, 1.0, 5, 6, 9, (0, 0));
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
                "name",
                "pool_reserved_after",
                "pool_reserved_before",
                "pool_reserved_peak",
                "rows",
                "start_epoch",
                "step_index",
                "storage",
            ]
        );
        assert_eq!(fields["compaction_copied_batches"], 0);
        assert_eq!(fields["compaction_refused_copies"], 0);
        assert_eq!(fields["pool_reserved_peak"], 9);
        let counted = step_event(3, "base", "memory", 7, 11, 1.0, 2.0, 1.0, 5, 6, 9, (4, 2));
        assert_eq!(
            (
                counted["compaction_copied_batches"].as_u64(),
                counted["compaction_refused_copies"].as_u64()
            ),
            (Some(4), Some(2))
        );
    }
}
