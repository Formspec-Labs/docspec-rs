//! Bounded observations of requested native data ranges during qualification.

use iceberg::arrow::{ScanMetrics, ScanResult};
use std::sync::{Arc, Mutex};

/// Observes attempted native data ranges without performing additional reads.
///
/// Iceberg 0.10.1 counts each requested range before awaiting its read. These
/// bytes include Parquet footers and delete files, but exclude manifest planning,
/// catalog requests, backend retries, and transport overhead. They are neither
/// successful bytes nor network bytes. Overlapping requests count repeatedly.
///
/// Each returned observer belongs to one serial stream. Memory remains constant
/// across files: completed counters plus at most one active native metric handle.
/// After early stream drop or error, the active handle remains observable; queued
/// or in-flight work can still update it. A sample is not an exact final total of
/// canceled I/O. Successful end-of-stream consumption supplies the complete total.
#[derive(Clone, Debug, Default)]
pub struct NativeScanObservation {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    completed_bytes: u64,
    scans_started: u64,
    active: Option<ScanMetrics>,
}

/// One current sample of attempted data ranges and native reader executions.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeScanSample {
    /// Sum of requested range lengths, including attempts that fail or cancel.
    pub requested_range_bytes: u64,
    /// Calls to the native reader, not file counts or backend request counts.
    pub scans_started: u64,
}

impl NativeScanObservation {
    /// Samples counters without waiting for pending I/O or scanning data.
    ///
    /// Concurrent reads can advance the atomic byte counter during sampling.
    ///
    /// # Panics
    /// Panics if a programming failure has poisoned the observer lock.
    #[cfg(any(test, feature = "test-util"))]
    pub fn sample(&self) -> NativeScanSample {
        let state = self.state.lock().expect("native scan observation poisoned");
        NativeScanSample {
            requested_range_bytes: state
                .completed_bytes
                .saturating_add(state.active.as_ref().map_or(0, ScanMetrics::bytes_read)),
            scans_started: state.scans_started,
        }
    }

    pub(super) fn attach(&self, result: &ScanResult) {
        let mut state = self.state.lock().expect("native scan observation poisoned");
        assert!(state.active.is_none(), "observer requires a serial stream");
        state.active = Some(result.metrics().clone());
        state.scans_started = state.scans_started.saturating_add(1);
    }

    pub(super) fn finish(&self) {
        let mut state = self.state.lock().expect("native scan observation poisoned");
        if let Some(metrics) = state.active.take() {
            state.completed_bytes = state.completed_bytes.saturating_add(metrics.bytes_read());
        }
    }
}
