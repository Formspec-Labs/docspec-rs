//! Shared byte admission and process-independent workspace ownership.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use datafusion::{
    execution::{
        SessionStateBuilder,
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool},
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
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
mod array_admission;
mod distinct;
mod peak_pool;
pub(crate) use array_admission::{array as admit_array, clean_null_views, dirty_view_bytes};
mod metadata_cache;
mod portable_aggregates;
mod schema_metadata;
mod scratch;
mod subqueries;
mod view_compaction;
pub(crate) use scratch::ScratchWriter;
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
    blocking_jobs: Arc<Semaphore>,
    runtime: Arc<RuntimeEnv>,
    pool: Arc<peak_pool::PeakPool<FairSpillPool>>,
    retention_pool: Arc<dyn MemoryPool>,
    scratch_writes: Arc<std::sync::Mutex<()>>,
    retained_files: Option<Arc<std::sync::Mutex<RetainedFiles>>>,
    retained_existing_bytes: u64,
}

#[derive(Debug)]
struct RetainedFiles {
    files: Vec<datafusion::execution::disk_manager::RefCountedTempFile>,
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
        let bytes = Arc::new(Semaphore::new(limits.memory_bytes / 2));
        let cache =
            metadata_cache::AdmittedMetadataCache::new(limits.max_metadata_bytes, bytes.clone());
        let cache = datafusion::execution::cache::cache_manager::CacheManagerConfig::default()
            .with_file_metadata_cache(Some(Arc::new(cache)))
            .with_metadata_cache_limit(limits.max_metadata_bytes);
        let pool = Arc::new(peak_pool::PeakPool::new(FairSpillPool::new(
            limits.memory_bytes / 2,
        )));
        let mut runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_cache_manager(cache)
            .with_max_temp_directory_size(limits.scratch_bytes);
        if let Some(scratch) = scratch {
            runtime = runtime.with_temp_file_path(scratch);
        }
        Ok(Self {
            bytes,
            blocking_jobs: Arc::new(Semaphore::new(limits.compute_threads)),
            runtime: Arc::new(runtime.build()?),
            pool,
            retention_pool: Arc::new(GreedyMemoryPool::new(limits.memory_bytes / 2 / 4)),
            scratch_writes: Arc::default(),
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
            .build_arc()?;
        let mut budget = self.clone();
        budget.runtime = runtime;
        budget.retained_existing_bytes = bytes;
        budget.retained_files = Some(Arc::new(std::sync::Mutex::new(RetainedFiles {
            files: Vec::new(),
            memory: self.reserve(0)?,
        })));
        Ok(budget)
    }

    pub(crate) fn retained_operation_disk_bytes(&self) -> u64 {
        self.retained_existing_bytes
            .saturating_add(self.runtime.disk_manager.used_disk_space())
    }

    pub(crate) fn retain_temporary(
        &self,
        file: datafusion::execution::disk_manager::RefCountedTempFile,
    ) -> Result<()> {
        if let Some(files) = &self.retained_files {
            if self.retained_operation_disk_bytes() > self.limits.scratch_bytes {
                return Err(Error::refusal(
                    "disk_capacity",
                    "retained files and native spill exceed the operation allowance",
                ));
            }
            let mut files = files
                .lock()
                .map_err(|_| Error::refusal("worker", "retained file admission is poisoned"))?;
            let charge = size_of_val(&file).saturating_mul(2).saturating_add(128);
            if files.files.len().saturating_add(1).saturating_mul(charge)
                > self.limits.max_metadata_bytes
            {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "retained file handles exceed metadata allowance",
                ));
            }
            files.memory.merge(self.reserve(charge)?);
            // The open native handle retains its disk charge. The durable hard
            // link owns the bytes; scratch must not become a second crash root.
            std::fs::remove_file(file.path())?;
            files.files.push(file);
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
        self.pool.peak()
    }

    /// Starts a new peak interval at the pool's current total.
    pub(crate) fn restart_pool_peak(&self) {
        self.pool.restart();
    }

    pub(crate) fn scratch_writer(
        &self,
        file: &datafusion::execution::disk_manager::RefCountedTempFile,
    ) -> Result<ScratchWriter> {
        ScratchWriter::new(file.clone(), self.clone())
    }

    pub(crate) fn context(&self) -> SessionContext {
        // DataFusion 53.1 can promote a filtered row estimate through OFFSET to
        // exact zero, then fold COUNT to an incorrect constant. Retain join cost
        // planning, but disable that aggregate rewrite until the upstream path
        // passes native_statistics_preserve_filtered_offset_counts unguarded.
        let mut rules: Vec<
            Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
        > = Vec::new();
        for rule in PhysicalOptimizer::new()
            .rules
            .into_iter()
            .filter(|rule| rule.name() != "aggregate_statistics")
        {
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
            .with_target_partitions(self.limits.compute_threads)
            // DataFusion 53.1's dynamic join bounds use ordinary comparisons
            // even for null-safe joins, dropping matching partial-null keys.
            // Keep other pruning enabled; qualify an upstream fix before reuse.
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                false,
            )
            // DataFusion 53.1's `push_down_leaf_projections` leaks extracted
            // `__datafusion_extracted_N` columns across aliased outer joins
            // (`apache/datafusion#22971`, `dr-7uke`), failing with ambiguous
            // schema errors on valid views and filters. Top-level column
            // pruning (`OptimizeProjections` + `ProjectionPushdown`) remains
            // active; re-enable once DataFusion >= 55.2 is qualified.
            .set_bool(
                "datafusion.optimizer.enable_leaf_expression_pushdown",
                false,
            );
        // Bound each partition's merge headroom to 1/16 of its native share,
        // leaving room for concurrent sorts and retained steps. Preserve the
        // maintained default ceiling for larger pools and at least one byte.
        let sort_spill_bytes = config
            .options()
            .execution
            .sort_spill_reservation_bytes
            .min((self.limits.memory_bytes / 2 / self.limits.compute_threads / 16).max(1));
        let config = config.with_sort_spill_reservation_bytes(sort_spill_bytes);
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
        self.bytes
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| {
                Error::refusal(
                    "capacity",
                    format!("shared memory budget exhausted: requested {bytes} bytes, available {} of {}, native reserved {}; {}:{}; completed phases remain retained", self.bytes.available_permits(), self.limits.memory_bytes / 2, self.runtime.memory_pool.reserved(), caller.file(), caller.line()),
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
        for (memory, threads, expected) in [
            (192 << 20, 2, 3 << 20),
            (192 << 20, 4, 1536 << 10),
            (24usize << 30, 12, 10 << 20),
            (32, 64, 1),
        ] {
            let budget = Budget::new(Limits {
                memory_bytes: memory,
                compute_threads: threads,
                ..Limits::default()
            })
            .unwrap();
            assert_eq!(
                budget
                    .context()
                    .state()
                    .config_options()
                    .execution
                    .sort_spill_reservation_bytes,
                expected
            );
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
        let mut writer = budget.scratch_writer(&file).unwrap();
        writer.write_all(&[1; 60]).unwrap();
        assert!(writer.write_all(&[2; 5]).is_err());
        assert_eq!(std::fs::metadata(file.path()).unwrap().len(), 60);
        assert_eq!(budget.runtime.disk_manager.used_disk_space(), 60);
        writer.write_all(&[3; 4]).unwrap();
        assert_eq!(
            std::fs::read(file.path()).unwrap(),
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
        let mut one = budget.scratch_writer(&first).unwrap();
        let mut two = budget.scratch_writer(&second).unwrap();
        one.write_all(&[0; 60]).unwrap();
        assert!(two.write_all(&[0; 41]).is_err());
        assert_eq!(std::fs::metadata(second.path()).unwrap().len(), 0);
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
}
