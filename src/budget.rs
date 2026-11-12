//! Shared byte admission and process-independent workspace ownership.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use datafusion::{
    execution::{
        SessionStateBuilder,
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool, PeakRecordingPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    physical_optimizer::optimizer::PhysicalOptimizer,
    prelude::{SessionConfig, SessionContext},
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
mod array_admission;
mod distinct;
pub(crate) use array_admission::{array as admit_array, clean_null_views, dirty_view_bytes};
mod metadata_cache;
mod portable_aggregates;
mod schema_metadata;
mod subqueries;
mod view_compaction;
#[cfg(any(test, feature = "test-util"))]
pub(crate) use view_compaction::without as without_retained_compaction;

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
        // `batch_rows` sizes reader batches (kept in the 8,192-65,536 range
        // recommended for streaming Parquet reads) and writer row groups, so
        // it also bounds per-row-group writer buffering inside `max_batch_bytes`.
        Self {
            compute_threads: 12,
            memory_bytes: 24 << 30,
            scratch_bytes: 32 << 30,
            batch_rows: 8192,
            max_batch_bytes: 64 << 20,
            max_metadata_bytes: 32 << 20,
        }
    }
}

/// Shared admission; native limits remain separate from observed process memory.
///
/// Retained SQL batches also have a ceiling of one quarter of the native pool.
/// They remain charged to that pool; this ceiling grants no additional memory.
/// Excess retained data uses workspace scratch instead of starving execution.
#[derive(Clone, Debug)]
pub struct Budget {
    pub limits: Limits,
    bytes: Arc<Semaphore>,
    shared_available_min: Arc<AtomicUsize>,
    blocking_jobs: Arc<Semaphore>,
    runtime: Arc<RuntimeEnv>,
    pool: Arc<PeakRecordingPool>,
    retention_pool: Arc<dyn MemoryPool>,
    retained_files: Option<Arc<std::sync::Mutex<RetainedFiles>>>,
    retained_existing_bytes: u64,
}

#[derive(Debug)]
struct RetainedFiles {
    retained_bytes: u64,
    count: usize,
    memory: OwnedSemaphorePermit,
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
            // The native virtual file column uses signed 32-bit run ends.
            || limits.batch_rows > i32::MAX as usize
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
        let shared_half = limits.memory_bytes / 2;
        let bytes = Arc::new(Semaphore::new(shared_half));
        let shared_available_min = Arc::new(AtomicUsize::new(shared_half));
        let cache = metadata_cache::AdmittedMetadataCache::new(
            limits.max_metadata_bytes,
            bytes.clone(),
            shared_available_min.clone(),
        );
        let cache = datafusion::execution::cache::cache_manager::CacheManagerConfig::default()
            .with_file_metadata_cache(Some(Arc::new(cache)))
            .with_metadata_cache_limit(limits.max_metadata_bytes)
            .with_file_statistics_cache_limit(0)
            .with_list_files_cache_limit(0);
        let pool = Arc::new(PeakRecordingPool::new(Arc::new(FairSpillPool::new(
            shared_half,
        ))));
        let mut runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_cache_manager(cache)
            .with_max_temp_directory_size(limits.scratch_bytes)
            .with_max_spill_merge_fan_in(32);
        if let Some(scratch) = scratch {
            runtime = runtime.with_temp_file_path(scratch);
        }
        Ok(Self {
            bytes,
            shared_available_min,
            blocking_jobs: Arc::new(Semaphore::new(limits.compute_threads)),
            runtime: Arc::new(runtime.build()?),
            pool,
            retention_pool: Arc::new(GreedyMemoryPool::new(limits.memory_bytes / 2 / 4)),
            retained_files: None,
            retained_existing_bytes: 0,
            limits,
        })
    }

    pub(crate) fn runtime_env(&self) -> &RuntimeEnv {
        &self.runtime
    }

    /// Shares memory and execution admission while charging one retained operation's disk.
    pub(crate) fn with_retained_disk(&self, bytes: u64) -> Result<Self> {
        if bytes > self.limits.scratch_bytes {
            return Err(Error::refusal(
                "disk_capacity",
                "retained operation exceeds its disk allowance",
            ));
        }
        use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode};
        let paths = self.runtime.disk_manager.temp_dir_paths();
        let mode = if paths.is_empty() {
            DiskManagerMode::OsTmpDirectory
        } else {
            DiskManagerMode::Directories(paths)
        };
        // The host admits this retained operation exclusively against other disk
        // work. Reuse its memory/cache/store owners, with one native disk counter
        // for new retained files and spills; cold retained files reduce the cap.
        let runtime = RuntimeEnvBuilder::from_runtime_env(&self.runtime)
            .with_disk_manager_builder(
                DiskManager::builder()
                    .with_mode(mode)
                    .with_max_temp_directory_size(self.limits.scratch_bytes - bytes),
            )
            .with_max_spill_merge_fan_in(32)
            .build_arc()?;
        let mut budget = self.clone();
        budget.runtime = runtime;
        budget.retained_existing_bytes = bytes;
        budget.retained_files = Some(Arc::new(std::sync::Mutex::new(RetainedFiles {
            retained_bytes: 0,
            count: 0,
            memory: self.reserve(0)?,
        })));
        Ok(budget)
    }

    #[cfg(test)]
    pub(crate) fn retained_operation_disk_bytes(&self) -> u64 {
        let retained = self
            .retained_files
            .as_ref()
            .and_then(|files| files.lock().ok().map(|files| files.retained_bytes))
            .unwrap_or(0);
        self.retained_existing_bytes
            .saturating_add(retained)
            .saturating_add(self.runtime.disk_manager.used_disk_space())
    }

    pub(crate) fn retain_temporary(
        &self,
        file: Arc<dyn datafusion::execution::SpillFile>,
    ) -> Result<()> {
        if let Some(files) = &self.retained_files {
            let mut files = files
                .lock()
                .map_err(|_| Error::refusal("worker", "retained file admission is poisoned"))?;
            let charge = size_of::<Arc<dyn datafusion::execution::SpillFile>>()
                .saturating_mul(2)
                .saturating_add(128);
            if files.count.saturating_add(1).saturating_mul(charge) > self.limits.max_metadata_bytes
            {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "retained file handles exceed metadata allowance",
                ));
            }
            let file_bytes = file.size().ok_or_else(|| {
                Error::refusal("scratch_path", "scratch file reports no byte size")
            })?;
            let next_bytes = files
                .retained_bytes
                .checked_add(file_bytes)
                .ok_or_else(|| {
                    Error::refusal(
                        "disk_capacity",
                        "retained files and native spill exceed the operation allowance",
                    )
                })?;
            let next_cap = self
                .limits
                .scratch_bytes
                .checked_sub(self.retained_existing_bytes)
                .and_then(|remaining| remaining.checked_sub(next_bytes))
                .ok_or_else(|| {
                    Error::refusal(
                        "disk_capacity",
                        "retained files and native spill exceed the operation allowance",
                    )
                })?;
            let permit = self.reserve(charge)?;
            // The durable hard link owns the bytes; dropping the temporary handle
            // unlinks its private scratch alias, closes its descriptor so snapshots
            // hold O(1) open descriptors, and releases its per-file disk charge
            // while lowering the native disk cap by the same retained byte count.
            drop(file);
            self.runtime
                .disk_manager
                .set_max_temp_directory_size(next_cap)?;
            if self.runtime.disk_manager.used_disk_space() > next_cap {
                return Err(Error::refusal(
                    "disk_capacity",
                    "retained files and native spill exceed the operation allowance",
                ));
            }
            files.retained_bytes = next_bytes;
            files.count += 1;
            files.memory.merge(permit);
        }
        Ok(())
    }

    pub(crate) fn tracks_retained_files(&self) -> bool {
        self.retained_files.is_some()
    }

    pub(crate) fn retention_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.retention_pool
    }

    /// Highest native pool total since [`Self::restart_pool_peak`], in charged bytes.
    ///
    /// It counts every consumer of the shared pool, retained results included.
    pub(crate) fn pool_peak(&self) -> usize {
        self.pool.peak_reserved()
    }

    /// Starts a new peak interval at the pool's current total.
    pub(crate) fn restart_pool_peak(&self) {
        self.pool.reset_peak();
    }

    /// Lowest shared-admission byte balance observed since [`Self::restart_shared_available_min`].
    pub(crate) fn shared_available_min(&self) -> usize {
        self.shared_available_min.load(Ordering::Relaxed)
    }

    /// Starts a new low-water interval at the shared semaphore's current available bytes.
    pub(crate) fn restart_shared_available_min(&self) {
        self.shared_available_min
            .store(self.bytes.available_permits(), Ordering::Relaxed);
    }

    /// Returns a scratch file's local path; this runtime creates only local files.
    ///
    /// The file's own writer (`SpillFile::open_writer`) charges the shared
    /// scratch limit and refuses a write that would exceed it before writing.
    ///
    /// # Errors
    /// Refuses a scratch file without a local path.
    pub(crate) fn scratch_path(
        file: &dyn datafusion::execution::SpillFile,
    ) -> Result<&std::path::Path> {
        file.path()
            .ok_or_else(|| Error::refusal("scratch_path", "scratch file has no local path"))
    }

    pub(crate) fn context(&self) -> SessionContext {
        let mut rules: Vec<
            Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
        > = Vec::new();
        for rule in PhysicalOptimizer::new().rules {
            // Keep admitted evaluation outside scan projection pushdown, after
            // native join/sort planning. Logical reader-column pruning remains.
            if rule.name() == "ProjectionPushdown" {
                rules.push(Arc::new(crate::job::fitting::FitProjections));
            }
            if rule.name() == "SanityCheckPlan" {
                rules.push(Arc::new(crate::job::fitting::FitFunctions));
                rules.push(Arc::new(view_compaction::Planner(self.clone())));
            }
            rules.push(rule);
        }
        let config = SessionConfig::new()
            .with_batch_size(self.limits.batch_rows)
            .with_target_partitions(self.limits.compute_threads);
        // Bound each partition's merge headroom to 1/16 of its native share,
        // leaving room for concurrent sorts and retained steps. Preserve the
        // maintained default ceiling for larger pools and at least one byte.
        let sort_spill_bytes = config
            .options()
            .execution
            .sort_spill_reservation_bytes
            .min((self.limits.memory_bytes / 2 / self.limits.compute_threads / 16).max(1));
        let mut config = config.with_sort_spill_reservation_bytes(sort_spill_bytes);
        // Scale PushdownSort's BufferExec cap (default 1 GiB) to 1/4 of each
        // partition's native pool share so bounded pools cannot starve on sort pushdown.
        let sort_pushdown_bytes = config
            .options()
            .execution
            .sort_pushdown_buffer_capacity
            .min((self.limits.memory_bytes / 2 / self.limits.compute_threads / 4).max(1));
        config.options_mut().execution.sort_pushdown_buffer_capacity = sort_pushdown_bytes;
        // The initial retained profile admits footer/page-index metadata before
        // native inference. Optional Bloom headers/bitsets need a separate
        // bounded admission path; disabling this pruning preserves row results
        // and accepts the native offset-only Bloom metadata form unchanged.
        let config = if self.tracks_retained_files() {
            config.set_bool("datafusion.execution.parquet.bloom_filter_on_read", false)
        } else {
            config
        };
        SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(self.runtime.clone())
                .with_default_features()
                .with_analyzer_rule(Arc::new(schema_metadata::Planner))
                .with_optimizer_rules(subqueries::rules())
                .with_physical_optimizer_rules(rules)
                .with_query_planner(Arc::new(schema_metadata::Planner))
                .build(),
        )
    }

    pub(crate) async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        // Hash/copy routines use one MiB buffers. Keep this reservation in the
        // worker so cancellation cannot release it while I/O is still active.
        let memory = self.reserve(1 << 20)?;
        self.blocking_admitted(move || {
            let _memory = memory;
            operation()
        })
        .await
    }

    /// Runs blocking work whose memory admission is already owned by its closure.
    ///
    /// Unlike copy/hash workers, bounded metadata decoding has no fixed I/O buffer.
    pub(crate) async fn blocking_admitted<T: Send + 'static>(
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
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await?
    }

    /// Runs admitted blocking work queued behind `blocking_jobs` capacity.
    pub(crate) async fn blocking_admitted_queued<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .blocking_jobs
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::refusal("worker", "blocking I/O limiter closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await?
    }
    /// Reserves bytes without waiting while already holding other reservations.
    ///
    /// # Errors
    /// Refuses oversized allocations or exhausted shared capacity.
    #[track_caller]
    pub fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        let caller = std::panic::Location::caller();
        tracing::debug!(
            bytes,
            available = self.bytes.available_permits(),
            native_reserved = self.runtime.memory_pool.reserved(),
            file = caller.file(),
            line = caller.line(),
            "shared admission request"
        );
        let count = u32::try_from(bytes)
            .map_err(|_| Error::refusal("capacity", "allocation exceeds admission granularity"))?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| {
                self.shared_available_min
                    .fetch_min(self.bytes.available_permits(), Ordering::Relaxed);
                Error::refusal(
                    "capacity",
                    format!("shared memory budget exhausted: requested {bytes} bytes, available {} of {}, native reserved {}; {}:{}; completed phases remain retained", self.bytes.available_permits(), self.limits.memory_bytes / 2, self.runtime.memory_pool.reserved(), caller.file(), caller.line()),
                )
            })?;
        self.shared_available_min
            .fetch_min(self.bytes.available_permits(), Ordering::Relaxed);
        Ok(permit)
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

// A counting sink bounds serialized metadata without allocating a second copy.
pub(crate) fn metadata_size(value: &impl serde::Serialize, limit: usize) -> Result<usize> {
    struct Counter {
        remaining: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("metadata capacity"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { remaining: limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| {
        Error::refusal(
            "metadata_capacity",
            "related-input metadata exceeds its combined allowance",
        )
    })?;
    Ok(limit - counter.remaining)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_profile_uses_native_bloom_read_policy_without_changing_the_host() {
        let host = Budget::new(Limits::default()).unwrap();
        assert!(
            host.context()
                .copied_table_options()
                .parquet
                .global
                .bloom_filter_on_read
        );
        let retained = host.with_retained_disk(0).unwrap();
        assert!(
            !retained
                .context()
                .copied_table_options()
                .parquet
                .global
                .bloom_filter_on_read
        );
        assert!(
            host.context()
                .copied_table_options()
                .parquet
                .global
                .bloom_filter_on_read
        );
    }

    #[test]
    fn sort_spill_headroom_scales_to_native_pool_and_partitions() {
        for (memory, threads, expected, expected_pushdown) in [
            (192 << 20, 2, 3 << 20, 12 << 20),
            (192 << 20, 4, 1536 << 10, 6 << 20),
            (24usize << 30, 12, 10 << 20, 256 << 20),
            (32, 64, 1, 1),
        ] {
            let budget = Budget::new(Limits {
                memory_bytes: memory,
                compute_threads: threads,
                ..Limits::default()
            })
            .unwrap();
            let state = budget.context().state();
            let execution = &state.config_options().execution;
            assert_eq!(execution.sort_spill_reservation_bytes, expected);
            assert_eq!(execution.sort_pushdown_buffer_capacity, expected_pushdown);
        }
    }

    #[tokio::test]
    async fn distinct_using_keeps_explicit_selected_columns() {
        let context = Budget::new(Limits::default()).unwrap().context();
        let mut failures = Vec::new();
        for (name, sql) in [
            (
                "later_alias",
                "SELECT DISTINCT z.id FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) USING(id)",
            ),
            (
                "earlier_alias",
                "SELECT DISTINCT a.id FROM (VALUES (1),(2),(2),(3)) a(id) JOIN (VALUES (1),(2),(3)) z(id) USING(id)",
            ),
            (
                "right_side",
                "SELECT DISTINCT z.id FROM (VALUES (1),(2),(3)) a(id) JOIN (VALUES (1),(2),(2),(3)) z(id) USING(id)",
            ),
            (
                "on_join",
                "SELECT DISTINCT z.id FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) ON z.id=a.id",
            ),
            (
                "mixed_columns",
                "SELECT DISTINCT z.id,z.label FROM (VALUES (1,'x'),(2,'x'),(2,'x'),(3,'x')) z(id,label) JOIN (VALUES (1),(2),(3)) a(id) USING(id)",
            ),
            (
                "aliased_column",
                "SELECT DISTINCT z.id AS key FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) USING(id)",
            ),
            (
                "nested",
                "SELECT count(*) FROM (SELECT DISTINCT z.id FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) USING(id)) q",
            ),
        ] {
            let batches = context.sql(sql).await.unwrap().collect().await;
            match batches {
                Ok(batches) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let correct = if name == "nested" {
                        batches[0]
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap()
                            .value(0)
                            == 3
                    } else {
                        rows == 3
                    };
                    if !correct {
                        failures.push(format!("{name}: {batches:?}"));
                    }
                }
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[tokio::test]
    async fn distinct_using_preserves_nulls_empty_inputs_and_distinct_on() {
        let context = Budget::new(Limits::default()).unwrap().context();
        for (sql, expected_rows) in [
            (
                "SELECT DISTINCT z.id FROM (VALUES (1),(2),(2),(NULL)) z(id) FULL JOIN (VALUES (1),(3),(NULL)) a(id) USING(id)",
                3,
            ),
            (
                "SELECT DISTINCT z.id,a.id FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) USING(id)",
                3,
            ),
            (
                "SELECT DISTINCT z.id FROM (VALUES (1),(2)) z(id) JOIN (VALUES (3)) a(id) USING(id)",
                0,
            ),
            (
                "SELECT DISTINCT 1 FROM (VALUES (1),(2)) z(id) JOIN (VALUES (1),(2)) a(id) USING(id)",
                1,
            ),
            (
                "SELECT DISTINCT ON (z.id) z.id FROM (VALUES (1),(2),(2),(3)) z(id) JOIN (VALUES (1),(2),(3)) a(id) USING(id) ORDER BY z.id",
                3,
            ),
        ] {
            let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                expected_rows,
                "{sql}"
            );
            if sql.contains("FULL JOIN") {
                assert_eq!(
                    batches
                        .iter()
                        .map(|b| b.column(0).null_count())
                        .sum::<usize>(),
                    1
                );
            }
        }
    }

    #[test]
    fn scratch_writes_refuse_before_effect_and_reuse_capacity_after_drop() {
        use std::io::Write;
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::in_workspace(
            Limits {
                scratch_bytes: 64,
                ..Limits::default()
            },
            root.path(),
        )
        .unwrap();
        let file = budget
            .runtime
            .disk_manager
            .create_tmp_file("physical guard")
            .unwrap();
        let path = Budget::scratch_path(file.as_ref()).unwrap().to_owned();
        let mut writer = file.open_writer().unwrap();
        writer.write_all(&[1; 60]).unwrap();
        assert!(writer.write_all(&[2; 5]).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 60);
        assert_eq!(budget.runtime.disk_manager.used_disk_space(), 60);
        writer.write_all(&[3; 4]).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            [[1; 60].as_slice(), &[3; 4]].concat()
        );
        drop(writer);
        drop(file);
        assert_eq!(budget.runtime.disk_manager.used_disk_space(), 0);
    }

    #[test]
    fn scratch_writers_share_remaining_capacity() {
        use std::io::Write;
        let budget = Budget::new(Limits {
            scratch_bytes: 100,
            ..Limits::default()
        })
        .unwrap();
        let first = budget
            .runtime
            .disk_manager
            .create_tmp_file("first")
            .unwrap();
        let second = budget
            .runtime
            .disk_manager
            .create_tmp_file("second")
            .unwrap();
        let mut one = first.open_writer().unwrap();
        let mut two = second.open_writer().unwrap();
        one.write_all(&[0; 60]).unwrap();
        assert!(two.write_all(&[0; 41]).is_err());
        assert_eq!(second.size(), Some(0));
        two.write_all(&[0; 40]).unwrap();
        assert!(one.write_all(&[0]).is_err());
    }

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
        let spill = first
            .disk_manager
            .create_tmp_file("shared accounting probe")
            .unwrap();
        let path = Budget::scratch_path(spill.as_ref()).unwrap().to_owned();
        assert!(path.starts_with(root.path()));
        std::io::Write::write_all(&mut spill.open_writer().unwrap(), &[0; 768]).unwrap();
        assert_eq!(second.disk_manager.used_disk_space(), 768);
        let retained = spill.clone();
        drop(spill);
        assert_eq!(first.disk_manager.used_disk_space(), 768);
        drop(retained);
        assert!(!path.exists());
        assert_eq!(second.disk_manager.used_disk_space(), 0);
    }

    #[test]
    fn native_spill_overrun_is_refused_without_a_phantom_charge() {
        // DataFusion 53.1 charged a refused spill globally and never refunded
        // it until the runtime reopened. 55's spill writer checks before it
        // writes and refunds the refused charge; keep that guarantee probed.
        use std::io::Write;
        let budget = Budget::new(Limits {
            scratch_bytes: 16,
            ..Limits::default()
        })
        .unwrap();
        let manager = &budget.runtime.disk_manager;
        let spill = manager.create_tmp_file("quota overrun probe").unwrap();
        let path = Budget::scratch_path(spill.as_ref()).unwrap().to_owned();
        assert!(spill.open_writer().unwrap().write_all(&[0; 17]).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert_eq!(manager.used_disk_space(), 0);
        drop(spill);
        assert!(!path.exists());
        assert_eq!(manager.spilling_progress().active_files_count, 0);
        let next = manager.create_tmp_file("retry after failure").unwrap();
        next.open_writer().unwrap().write_all(&[0; 16]).unwrap();
        assert_eq!(manager.used_disk_space(), 16);
        drop(next);
        assert_eq!(manager.used_disk_space(), 0);
    }

    #[tokio::test]
    async fn native_spills_write_view_columns_by_referenced_bytes() {
        use arrow::{
            array::{ArrayRef, AsArray, BinaryViewArray, Int64Array, StringViewArray},
            compute::cast,
            datatypes::DataType,
            record_batch::RecordBatch,
        };
        use datafusion::{
            datasource::MemTable,
            physical_plan::{ExecutionPlan, collect},
        };
        fn spilled(plan: &Arc<dyn ExecutionPlan>) -> usize {
            let own = plan
                .metrics()
                .and_then(|metrics| metrics.spilled_bytes())
                .unwrap_or(0);
            own + plan.children().into_iter().map(spilled).sum::<usize>()
        }
        // The aggregate and the sort spill each sorted run as batch_rows
        // batches sharing the run's buffers. Arrow IPC writes every data buffer
        // a view column references, so DataFusion 53.1 rewrote the whole run
        // once per batch (ss-vuwa).
        const ROWS: usize = 128 << 10;
        // 28 bytes: out of line in a view.
        let key = |value: usize| format!("receipt-{value:020}");
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 16 << 20,
            batch_rows: 1024,
            ..Limits::default()
        })
        .unwrap();
        let batches = (0..ROWS / 1024)
            .map(|chunk| {
                // Descending, so the sort reorders every row.
                let keys = || (0..1024).map(move |row| key(ROWS - chunk * 1024 - row));
                RecordBatch::try_from_iter([
                    (
                        "k",
                        Arc::new(StringViewArray::from_iter_values(keys())) as ArrayRef,
                    ),
                    ("b", Arc::new(BinaryViewArray::from_iter_values(keys()))),
                    ("n", Arc::new(Int64Array::from_iter_values(0..1024))),
                ])
                .unwrap()
            })
            .collect::<Vec<_>>();
        let input = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let context = budget.context();
        let table = MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
        context.register_table("t", Arc::new(table)).unwrap();
        for (sql, expected) in [
            (
                "SELECT k FROM t GROUP BY k, n HAVING count(*) > 1",
                Vec::new(),
            ),
            ("SELECT b FROM t ORDER BY b", (1..=ROWS).map(key).collect()),
        ] {
            let plan = context
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let output = collect(plan.clone(), context.task_ctx()).await.unwrap();
            let values = output
                .iter()
                .flat_map(|batch| {
                    let values = cast(batch.column(0), &DataType::BinaryView).unwrap();
                    values
                        .as_binary_view()
                        .iter()
                        .map(|value| String::from_utf8(value.unwrap().to_vec()).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert!(values == expected, "{sql} returned other values");
            let spilled = spilled(&plan);
            assert!(spilled > 0, "{sql} did not spill");
            assert!(
                spilled < input,
                "{sql} spilled {spilled} B for {input} B of input"
            );
        }
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

    #[test]
    fn native_virtual_run_end_limit_refuses_oversized_batches() {
        assert_eq!(
            Budget::new(Limits {
                batch_rows: i32::MAX as usize + 1,
                ..Limits::default()
            })
            .unwrap_err()
            .code(),
            "invalid_budget"
        );
        Budget::new(Limits {
            batch_rows: i32::MAX as usize,
            ..Limits::default()
        })
        .unwrap();
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

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_small_worker_keeps_its_owned_bytes_and_worker_slot() {
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 128 << 10,
            ..Limits::default()
        })
        .unwrap();
        let memory = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
        let worker_budget = budget.clone();
        let (started, received) = tokio::sync::oneshot::channel();
        let (release, stalled) = std::sync::mpsc::channel();
        let worker = tokio::spawn(async move {
            worker_budget
                .blocking_admitted(move || {
                    let _memory = memory;
                    started.send(()).unwrap();
                    stalled.recv().unwrap();
                    Ok(())
                })
                .await
        });
        received.await.unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        assert_eq!(
            budget
                .blocking_admitted(|| Ok(()))
                .await
                .unwrap_err()
                .code(),
            "capacity"
        );
        release.send(()).unwrap();
        let permit = budget.blocking_jobs.clone().acquire_owned().await.unwrap();
        drop(permit);
        let _memory = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
        budget.blocking_admitted(|| Ok(())).await.unwrap();
    }

    #[test]
    fn retaining_hundreds_of_files_holds_constant_open_file_descriptors() {
        let root = tempfile::tempdir().unwrap();
        let host = Budget::in_workspace(Limits::default(), &root.path().join("scratch")).unwrap();
        let budget = host.with_retained_disk(4096).unwrap();
        for idx in 0..512_usize {
            let file = budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("fd bound check")
                .unwrap();
            let mut writer = file.open_writer().unwrap();
            std::io::Write::write_all(&mut writer, &[0x3c; 1024]).unwrap();
            drop(writer);
            let scratch = Budget::scratch_path(file.as_ref()).unwrap();
            std::fs::hard_link(scratch, root.path().join(format!("retained-{idx}"))).unwrap();
            budget.retain_temporary(file).unwrap();
        }
        assert_eq!(budget.retained_operation_disk_bytes(), 4096 + 512 * 1024);
        assert_eq!(
            budget
                .runtime_env()
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0,
            "retaining 512 files must release temporary descriptors rather than holding 512 open handles"
        );
    }

    #[test]
    fn runtime_cache_manager_disables_uncharged_statistics_and_file_list_caches() {
        let budget = Budget::new(Limits::default()).unwrap();
        for env in [
            budget.runtime_env(),
            budget.with_retained_disk(0).unwrap().runtime_env(),
        ] {
            assert!(env.cache_manager.get_file_statistic_cache().is_none());
            assert!(env.cache_manager.get_list_files_cache().is_none());
            assert_eq!(
                env.cache_manager.get_file_metadata_cache().name(),
                "AdmittedFileMetadataCache"
            );
            assert_eq!(env.disk_manager.max_spill_merge_fan_in(), 32);
        }
    }

    #[test]
    fn peak_keeps_the_highest_total_until_restarted() {
        use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit};
        let tracked = Arc::new(PeakRecordingPool::new(Arc::new(FairSpillPool::new(1000))));
        let pool: Arc<dyn MemoryPool> = tracked.clone();
        let held = MemoryConsumer::new("retained").register(&pool);
        let query = MemoryConsumer::new("query").register(&pool);
        held.try_grow(100).unwrap();
        query.try_grow(600).unwrap();
        // A refused request reserves nothing and must not raise the peak.
        query.try_grow(600).unwrap_err();
        query.shrink(500);
        query.grow(50);
        assert_eq!((pool.reserved(), tracked.peak_reserved()), (250, 700));
        tracked.reset_peak();
        assert_eq!(tracked.peak_reserved(), 250);
        query.free();
        assert_eq!((pool.reserved(), tracked.peak_reserved()), (100, 250));
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1000)));
    }
}
