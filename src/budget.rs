//! Shared byte admission and process-independent workspace ownership.

use crate::{Error, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Declared limits for one workspace execution host.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub compute_threads: usize,
    pub memory_bytes: usize,
    pub scratch_bytes: u64,
    pub batch_rows: usize,
    pub max_batch_bytes: usize,
    pub max_metadata_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        // Initial reference profile from the accepted implementation proposal.
        Self {
            compute_threads: 12,
            memory_bytes: 24 << 30,
            scratch_bytes: 32 << 30,
            batch_rows: 4096,
            max_batch_bytes: 64 << 20,
            max_metadata_bytes: 32 << 20,
        }
    }
}

/// A shared admission pool; native engine limits remain separate from observed RSS.
#[derive(Clone, Debug)]
pub struct Budget {
    pub limits: Limits,
    bytes: Arc<Semaphore>,
    blocking_jobs: Arc<Semaphore>,
}
impl Budget {
    /// Creates a shared pool after validating representable limits.
    ///
    /// # Errors
    /// Refuses zero or unsupported capacities.
    pub fn new(limits: Limits) -> Result<Self> {
        if limits.compute_threads == 0
            || limits.compute_threads > Semaphore::MAX_PERMITS
            || limits.memory_bytes == 0
            || limits.memory_bytes > Semaphore::MAX_PERMITS
            || limits.batch_rows == 0
            || limits.max_metadata_bytes == 0
            || limits.max_batch_bytes == 0
            || limits.max_batch_bytes > u32::MAX as usize / 4
        {
            return Err(Error::refusal(
                "invalid_budget",
                "invalid thread, batch, or memory limits",
            ));
        }
        // The other half belongs to the shared DataFusion memory pool.
        Ok(Self {
            bytes: Arc::new(Semaphore::new(limits.memory_bytes / 2)),
            blocking_jobs: Arc::new(Semaphore::new(limits.compute_threads)),
            limits,
        })
    }

    pub(crate) async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .blocking_jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Error::refusal(
                    "capacity",
                    "blocking I/O capacity exhausted; retry retained work",
                )
            })?;
        // Hash/copy routines use one MiB buffers. The worker owns both permits so
        // canceling its caller cannot admit another job while I/O is still active.
        let memory = self.reserve(1 << 20)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _memory = memory;
            operation()
        })
        .await?
    }
    /// Reserves bytes without waiting while already holding other reservations.
    ///
    /// # Errors
    /// Refuses oversized allocations or exhausted shared capacity.
    pub fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        let count = u32::try_from(bytes)
            .map_err(|_| Error::refusal("capacity", "allocation exceeds admission granularity"))?;
        self.bytes
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| {
                Error::refusal(
                    "capacity",
                    "shared memory budget exhausted; completed phases remain retained",
                )
            })
    }
}

/// An OS-held lock; process death releases ownership without guessing from timestamps.
#[derive(Debug)]
pub struct WorkspaceLock {
    _file: File,
}
impl WorkspaceLock {
    /// Acquires exclusive local workspace ownership.
    ///
    /// # Errors
    /// Refuses another live owner or filesystem errors.
    pub fn acquire(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("host.lock"))?;
        file.try_lock_exclusive().map_err(|_| {
            Error::refusal(
                "workspace_busy",
                "another execution host owns this workspace",
            )
        })?;
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrepresentable_worker_limit_refuses_without_panicking() {
        assert_eq!(
            Budget::new(Limits {
                compute_threads: usize::MAX,
                ..Limits::default()
            })
            .unwrap_err()
            .code(),
            "invalid_budget"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_blocking_work_keeps_admission_without_stalling_the_executor() {
        let budget = Budget::new(Limits {
            compute_threads: 1,
            ..Limits::default()
        })
        .unwrap();
        let worker_budget = budget.clone();
        let (started, received) = tokio::sync::oneshot::channel();
        let (release, stalled) = std::sync::mpsc::channel();
        let worker = tokio::spawn(async move {
            worker_budget
                .blocking(move || {
                    started.send(()).unwrap();
                    stalled.recv().unwrap();
                    Ok(())
                })
                .await
        });
        received.await.unwrap();
        // This task runs on the same single-thread executor while I/O is stalled.
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(
            budget.blocking(|| Ok(())).await.unwrap_err().code(),
            "capacity"
        );
        release.send(()).unwrap();
        // Joining admission proves the detached worker releases its permit on exit.
        let permit = budget.blocking_jobs.clone().acquire_owned().await.unwrap();
        drop(permit);
        budget.blocking(|| Ok(())).await.unwrap();
    }
}
