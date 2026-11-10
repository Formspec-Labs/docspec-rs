//! Remember the native pool's highest total reservation between two readings.
// Rust guideline compliant 2026-02-21
use datafusion::{
    common::Result,
    execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Delegates every reservation to `inner` and keeps the highest total seen.
///
/// The total covers every consumer of the pool, retained step results
/// included. It is charged bytes, not process memory.
#[derive(Debug)]
pub(super) struct PeakPool<P> {
    inner: P,
    peak: AtomicUsize,
}

impl<P: MemoryPool> PeakPool<P> {
    pub(super) fn new(inner: P) -> Self {
        Self {
            inner,
            peak: AtomicUsize::new(0),
        }
    }

    /// Highest total reserved since the last [`Self::restart`].
    pub(super) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Starts a new interval at the current total.
    pub(super) fn restart(&self) {
        self.peak.store(self.inner.reserved(), Ordering::Relaxed);
    }
}

impl<P: MemoryPool> MemoryPool for PeakPool<P> {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::FairSpillPool;
    use std::sync::Arc;

    #[test]
    fn peak_keeps_the_highest_total_until_restarted() {
        let tracked = Arc::new(PeakPool::new(FairSpillPool::new(1000)));
        let pool: Arc<dyn MemoryPool> = tracked.clone();
        let held = MemoryConsumer::new("retained").register(&pool);
        let query = MemoryConsumer::new("query").register(&pool);
        held.try_grow(100).unwrap();
        query.try_grow(600).unwrap();
        // A refused request reserves nothing and must not raise the peak.
        query.try_grow(600).unwrap_err();
        query.shrink(500);
        query.grow(50);
        assert_eq!((pool.reserved(), tracked.peak()), (250, 700));
        tracked.restart();
        assert_eq!(tracked.peak(), 250);
        query.free();
        assert_eq!((pool.reserved(), tracked.peak()), (100, 250));
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1000)));
    }
}
