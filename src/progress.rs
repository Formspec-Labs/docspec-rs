//! Live observations from necessary work; none of these events are recovery records.

use crate::{Result, model::Id};
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tracing::Instrument;

// One observation per second keeps a long streaming write visible without
// making event volume proportional to its batch count. First acceptance and
// terminal events are always reported; a blocked writer has no invented heartbeat.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) async fn phase<T>(
    dataset: &Id,
    operation: &Id,
    phase: &'static str,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    async {
        let mut timer = PhaseTimer {
            started: Instant::now(),
            finished: false,
        };
        tracing::info!(event = "phase_work_started", "phase work started");
        let result = work.await;
        timer.finished = true;
        match &result {
            Ok(_) => tracing::info!(
                event = "phase_work_finished",
                elapsed_seconds = timer.started.elapsed().as_secs_f64(),
                "phase work finished"
            ),
            Err(error) => tracing::info!(
                event = "phase_work_failed",
                elapsed_seconds = timer.started.elapsed().as_secs_f64(),
                code = error.code(),
                "phase work failed; consult retained completions before retry"
            ),
        }
        result
    }
    .instrument(
        tracing::info_span!("phase_work", dataset = %dataset, operation = %operation, phase),
    )
    .await
}

struct PhaseTimer {
    started: Instant,
    finished: bool,
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        if !self.finished {
            tracing::info!(
                event = "phase_work_abandoned",
                elapsed_seconds = self.started.elapsed().as_secs_f64(),
                outcome = "unknown",
                "phase future abandoned; durable outcome is unknown"
            );
        }
    }
}

pub(crate) struct Writer<'a> {
    id: &'a str,
    started: Instant,
    last_sample: Option<Instant>,
    rows: u64,
    decoded_bytes: u64,
    finished: bool,
}

impl<'a> Writer<'a> {
    pub(crate) fn new(id: &'a str) -> Self {
        tracing::info!(
            event = "native_writer_started",
            writer = id,
            total_rows = "unknown",
            total_decoded_bytes = "unknown",
            "native writer started"
        );
        Self {
            id,
            started: Instant::now(),
            last_sample: None,
            rows: 0,
            decoded_bytes: 0,
            finished: false,
        }
    }

    // Reuse the existing Arrow batch-size observation. This cumulative measure
    // can count shared buffers again in another batch; it is neither transferred
    // bytes nor output size nor whole-process memory.
    pub(crate) fn accepted(&mut self, rows: u64, decoded_bytes: u64) {
        self.rows = self.rows.saturating_add(rows);
        self.decoded_bytes = self.decoded_bytes.saturating_add(decoded_bytes);
        let now = Instant::now();
        if self
            .last_sample
            .is_none_or(|last| now.duration_since(last) >= SAMPLE_INTERVAL)
        {
            tracing::info!(
                event = "native_writer_progress",
                writer = self.id,
                rows_accepted = self.rows,
                decoded_bytes_accepted = self.decoded_bytes,
                elapsed_seconds = now.duration_since(self.started).as_secs_f64(),
                total_rows = "unknown",
                total_decoded_bytes = "unknown",
                "native writer accepted batches; values are not committed"
            );
            self.last_sample = Some(now);
        }
    }

    pub(crate) fn finish(mut self, result: &Result<Vec<iceberg::spec::DataFile>>) {
        self.finished = true;
        match result {
            Ok(files) => tracing::info!(
                event = "native_writer_closed",
                writer = self.id,
                rows_accepted = self.rows,
                decoded_bytes_accepted = self.decoded_bytes,
                files = files.len(),
                elapsed_seconds = self.started.elapsed().as_secs_f64(),
                "native writer closed; snapshot and phase commits are separate"
            ),
            Err(error) => tracing::info!(
                event = "native_writer_failed",
                writer = self.id,
                rows_accepted = self.rows,
                decoded_bytes_accepted = self.decoded_bytes,
                elapsed_seconds = self.started.elapsed().as_secs_f64(),
                code = error.code(),
                "native writer failed; accepted rows are not committed"
            ),
        }
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        if !self.finished {
            tracing::info!(
                event = "native_writer_abandoned",
                writer = self.id,
                rows_accepted = self.rows,
                decoded_bytes_accepted = self.decoded_bytes,
                elapsed_seconds = self.started.elapsed().as_secs_f64(),
                outcome = "unknown",
                "native writer future abandoned; completion is unknown"
            );
        }
    }
}

#[cfg(test)]
mod tests;
