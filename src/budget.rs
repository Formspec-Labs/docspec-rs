//! Shared byte admission and process-independent workspace ownership.

use crate::{Error, Result};
use datafusion::{
    execution::{
        memory_pool::FairSpillPool,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::{SessionConfig, SessionContext},
};
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
    runtime: Arc<RuntimeEnv>,
}
impl Budget {
    /// Creates a shared pool after validating representable limits.
    ///
    /// # Errors
    /// Refuses zero or unsupported capacities.
    pub fn new(limits: Limits) -> Result<Self> {
        Self::create(limits, None)
    }

    pub(crate) fn in_workspace(limits: Limits, scratch: &Path) -> Result<Self> {
        Self::create(limits, Some(scratch))
    }

    fn create(limits: Limits, scratch: Option<&Path>) -> Result<Self> {
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
        let mut runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(limits.memory_bytes / 2)))
            .with_max_temp_directory_size(limits.scratch_bytes);
        if let Some(scratch) = scratch {
            runtime = runtime.with_temp_file_path(scratch);
        }
        Ok(Self {
            bytes: Arc::new(Semaphore::new(limits.memory_bytes / 2)),
            blocking_jobs: Arc::new(Semaphore::new(limits.compute_threads)),
            runtime: Arc::new(runtime.build()?),
            limits,
        })
    }

    pub(crate) fn context(&self) -> SessionContext {
        SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_batch_size(self.limits.batch_rows)
                .with_target_partitions(self.limits.compute_threads),
            self.runtime.clone(),
        )
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

impl Drop for WorkspaceLock {
    fn drop(&mut self) {
        // A fork may temporarily inherit this open file description before exec.
        // Closing only our descriptor leaves its lock alive in that child.
        if let Err(error) = FileExt::unlock(&self._file) {
            tracing::warn!(error = %error, "workspace lock release failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contexts_share_native_memory_and_spill_accounting() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::in_workspace(
            Limits {
                memory_bytes: 8 << 20,
                scratch_bytes: 1024,
                ..Limits::default()
            },
            root.path(),
        )
        .unwrap();
        let first = budget.context().runtime_env();
        let second = budget.clone().context().runtime_env();
        assert!(Arc::ptr_eq(&first, &second));
        let one = MemoryConsumer::new("first verifier").register(&first.memory_pool);
        let two = MemoryConsumer::new("second verifier").register(&second.memory_pool);
        one.try_grow(4 << 20).unwrap();
        assert!(two.try_grow(1).is_err());
        drop(one);
        two.try_grow(4 << 20).unwrap();
        drop(two);
        assert_eq!(first.memory_pool.reserved(), 0);
        let mut spill = first
            .disk_manager
            .create_tmp_file("shared accounting probe")
            .unwrap();
        assert!(spill.path().starts_with(root.path()));
        spill.inner().as_file().set_len(768).unwrap();
        spill.update_disk_usage().unwrap();
        assert_eq!(second.disk_manager.used_disk_space(), 768);
        let retained = spill.clone();
        let path = spill.path().to_owned();
        drop(spill);
        assert_eq!(first.disk_manager.used_disk_space(), 768);
        drop(retained);
        assert!(!path.exists());
        assert_eq!(second.disk_manager.used_disk_space(), 0);
    }

    #[test]
    fn pinned_native_spill_overrun_leaves_accounting_until_runtime_reopen() {
        // Capability probe for DataFusion 53.1: quota failure updates global
        // usage before the file's own charge, so dropping it cannot refund it.
        // Keep this limitation explicit until a maintained dependency fixes it.
        let limits = Limits {
            scratch_bytes: 16,
            ..Limits::default()
        };
        let budget = Budget::new(limits.clone()).unwrap();
        let manager = &budget.runtime.disk_manager;
        let mut spill = manager.create_tmp_file("quota overrun probe").unwrap();
        spill.inner().as_file().set_len(17).unwrap();
        assert!(spill.update_disk_usage().is_err());
        let path = spill.path().to_owned();
        drop(spill);
        assert!(!path.exists());
        assert_eq!(manager.used_disk_space(), 17);
        assert_eq!(manager.spilling_progress().active_files_count, 0);
        let mut next = manager.create_tmp_file("retry after failure").unwrap();
        next.inner().as_file().set_len(1).unwrap();
        assert!(next.update_disk_usage().is_err());
        drop(next);
        drop(budget);
        let fresh = Budget::new(limits).unwrap();
        let mut spill = fresh
            .runtime
            .disk_manager
            .create_tmp_file("reopened runtime")
            .unwrap();
        spill.inner().as_file().set_len(1).unwrap();
        spill.update_disk_usage().unwrap();
        drop(spill);
        assert_eq!(fresh.runtime.disk_manager.used_disk_space(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn releasing_workspace_ownership_unlocks_inherited_descriptors() {
        let root = tempfile::tempdir().unwrap();
        let owner = WorkspaceLock::acquire(root.path()).unwrap();
        // Unix process creation can briefly duplicate the open file description
        // before close-on-exec takes effect. Keep such a descriptor alive here.
        let inherited = owner._file.try_clone().unwrap();
        assert_eq!(
            WorkspaceLock::acquire(root.path()).unwrap_err().code(),
            "workspace_busy"
        );
        drop(owner);
        let next = WorkspaceLock::acquire(root.path()).unwrap();
        drop(inherited);
        assert_eq!(
            WorkspaceLock::acquire(root.path()).unwrap_err().code(),
            "workspace_busy"
        );
        drop(next);
        WorkspaceLock::acquire(root.path()).unwrap();
    }

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
