//! Shared local application operations and durable phase handoffs.
//!
//! One process owns the workspace. Short per-dataset sections serialize admission
//! and selection; immutable data preparation can proceed independently.

use crate::{
    Error, Result,
    budget::{Budget, Limits, WorkspaceLock},
    catalog::{Lifecycle, record},
    model::*,
    storage,
};
use datafusion::{
    execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    prelude::*,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::{Catalog, TableCreation};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

mod write;

/// Local execution host sharing catalog, admission, and resource accounting.
#[derive(Clone, Debug)]
pub struct Engine {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    lifecycle: Lifecycle,
    runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    locks: Mutex<BTreeMap<Id, Arc<Mutex<()>>>>,
    _workspace: WorkspaceLock,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Operation {
    request_digest: String,
    eligibility: u64,
    epoch: u64,
    boundary: u64,
    definition: String,
    base: Option<Id>,
}

/// A bounded explanation of reuse and work remaining before selection.
#[derive(Clone, Debug, Serialize)]
pub struct RunPlan {
    pub dataset: Id,
    pub operation: Id,
    pub completed: bool,
    pub reusable_phases: Vec<String>,
    pub required_phases: Vec<String>,
    pub limits: Limits,
}

const PHASES: [&str; 4] = ["admission", "processing", "validation", "readback"];
const PROCESSOR: &str = "docspec.dataset.v2";

impl Engine {
    /// Opens an exclusively owned local workspace with a maintained catalog.
    ///
    /// # Errors
    /// Refuses a live owner, invalid limits, or catalog/storage initialization failure.
    pub async fn open(root: impl AsRef<Path>, limits: Limits) -> Result<Self> {
        let ownership = WorkspaceLock::acquire(root.as_ref())?;
        let root = std::fs::canonicalize(root)?;
        let catalog = storage::local_catalog(&root).await?;
        Self::compose(root, catalog, limits, ownership)
    }

    /// Opens a workspace using an injected maintained catalog.
    ///
    /// # Errors
    /// Refuses a live owner, invalid limits, or runtime configuration failure.
    pub async fn with_catalog(
        root: impl AsRef<Path>,
        catalog: Arc<dyn Catalog>,
        limits: Limits,
    ) -> Result<Self> {
        let ownership = WorkspaceLock::acquire(root.as_ref())?;
        Self::compose(std::fs::canonicalize(root)?, catalog, limits, ownership)
    }

    fn compose(
        root: PathBuf,
        catalog: Arc<dyn Catalog>,
        limits: Limits,
        ownership: WorkspaceLock,
    ) -> Result<Self> {
        let budget = Budget::new(limits)?;
        let scratch = root.join("scratch");
        std::fs::create_dir_all(&scratch)?;
        // Half the declared process allowance is reserved for native execution;
        // readers, writers, metadata and other allocations require the remainder.
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(budget.limits.memory_bytes / 2)))
                .with_temp_file_path(scratch)
                .with_max_temp_directory_size(budget.limits.scratch_bytes)
                .build()?,
        );
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                lifecycle: Lifecycle::new(catalog, budget),
                runtime,
                locks: Mutex::new(BTreeMap::new()),
                _workspace: ownership,
            }),
        })
    }

    async fn lock(&self, dataset: &Id) -> Arc<Mutex<()>> {
        self.inner
            .locks
            .lock()
            .await
            .entry(dataset.clone())
            .or_default()
            .clone()
    }

    fn context(&self) -> SessionContext {
        let limits = &self.inner.lifecycle.budget.limits;
        SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_batch_size(limits.batch_rows)
                .with_target_partitions(limits.compute_threads),
            self.inner.runtime.clone(),
        )
    }

    /// Explains retained phases without opening large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting operation identities or invalid requests.
    pub async fn plan(&self, spec: &RunSpec) -> Result<RunPlan> {
        self.plan_request(&OperationRequest::Build(spec.clone()))
            .await
    }

    /// Plans an update without opening its large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting identities, stale bases or definitions, and invalid requests.
    pub async fn plan_update(&self, spec: &UpdateSpec) -> Result<RunPlan> {
        self.plan_request(&OperationRequest::Update(spec.clone()))
            .await
    }

    async fn plan_request(&self, spec: &OperationRequest) -> Result<RunPlan> {
        validate_request(spec)?;
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(spec.dataset()).await?;
        let request = fingerprint(spec)?;
        let operation: Option<Operation> = lifecycle
            .get(&view, &format!("operation/{}", spec.operation()))
            .await?;
        if operation
            .as_ref()
            .is_some_and(|o| o.request_digest != request)
        {
            return Err(conflicting_operation());
        }
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        let key = format!("result/{}", spec.operation());
        let completed = lifecycle.get::<ReleasePin>(&view, &key).await?.is_some();
        if !completed {
            check_definition(&authority, spec)?;
            check_base(&authority, spec.base())?;
        }
        if !completed && let Some(operation) = &operation {
            check_authority(&authority, operation)?;
        }
        let mut reusable_phases = Vec::new();
        let mut required_phases = Vec::new();
        for phase in PHASES {
            let exists = if phase == "readback" {
                lifecycle
                    .get::<ReleasePin>(&view, &format!("readback/{}", spec.operation()))
                    .await?
                    .is_some()
            } else {
                let eligibility = operation
                    .as_ref()
                    .map_or(authority.eligibility, |o| o.eligibility);
                lifecycle
                    .get::<PhaseResult>(&view, &phase_key(spec, &request, eligibility, phase))
                    .await?
                    .is_some()
            };
            if exists {
                reusable_phases.push(phase.to_owned());
            } else {
                required_phases.push(phase.to_owned());
            }
        }
        Ok(RunPlan {
            dataset: spec.dataset().clone(),
            operation: spec.operation().clone(),
            completed,
            reusable_phases,
            required_phases,
            limits: lifecycle.budget.limits.clone(),
        })
    }

    /// Returns bounded, current lifecycle records for inspection and recovery.
    ///
    /// # Errors
    /// Refuses corrupt or oversized metadata.
    pub async fn inspect(&self, dataset: &Id) -> Result<BTreeMap<String, serde_json::Value>> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.inner.lifecycle.records(&view).await
    }

    /// Resolves the current release once, without consulting independent native heads.
    ///
    /// # Errors
    /// Refuses datasets without a selected release or unavailable metadata.
    pub async fn latest(&self, dataset: &Id) -> Result<ReleasePin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(&view, "authority")
            .await?
            .unwrap_or_default();
        authority
            .release
            .ok_or_else(|| Error::refusal("no_release", "dataset has no selected release"))
    }

    /// Resolves a historical operation's immutable release without changing latest.
    ///
    /// # Errors
    /// Refuses unknown versions or unavailable lifecycle metadata.
    pub async fn release(&self, dataset: &Id, version: &Id) -> Result<ReleasePin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.inner
            .lifecycle
            .get(&view, &format!("result/{version}"))
            .await?
            .ok_or_else(|| Error::refusal("no_release", "operation has no completed release"))
    }

    /// Compares pinned values with duplicate-aware full reconstruction.
    ///
    /// Uses maintained Iceberg providers and grouped occurrence counts. Different
    /// schemas are reported directly; no implicit cast can hide a changed type.
    /// This is a full comparison, not an incremental change journal.
    ///
    /// # Errors
    /// Refuses missing groups, changed evidence, unsupported comparisons, or exhausted capacity.
    pub async fn compare(
        &self,
        before: &ReleasePin,
        after: &ReleasePin,
        group: &str,
    ) -> Result<Comparison> {
        let first = before.dataset.groups.get(group).ok_or_else(|| {
            Error::refusal(
                "missing_group",
                "before release lacks the requested field group",
            )
        })?;
        let second = after.dataset.groups.get(group).ok_or_else(|| {
            Error::refusal(
                "missing_group",
                "after release lacks the requested field group",
            )
        })?;
        verify_release_with_budget(before, &self.inner.lifecycle.budget).await?;
        verify_release_with_budget(after, &self.inner.lifecycle.budget).await?;
        let first = storage::open_pin(first).await?;
        let second = storage::open_pin(second).await?;
        let first_schema =
            iceberg::arrow::schema_to_arrow_schema(first.metadata().current_schema())?;
        let second_schema =
            iceberg::arrow::schema_to_arrow_schema(second.metadata().current_schema())?;
        let same_schema = first_schema == second_schema;
        let mut comparison = Comparison {
            same_schema,
            same_inputs: before.dataset.inputs == after.dataset.inputs,
            added_rows: None,
            removed_rows: None,
        };
        if !same_schema {
            return Ok(comparison);
        }
        let context = self.context();
        for (name, table) in [("before", first), ("after", second)] {
            context.register_table(
                name,
                Arc::new(
                    iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table(table)
                        .await?,
                ),
            )?;
        }
        let columns = first_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| format!("\"{}\" AS c{i}", field.name().replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let group_columns = (0..first_schema.fields().len())
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        // DataFusion 53 EXCEPT ALL uses an anti join and loses matched duplicate
        // differences. Signed occurrence counts preserve exact multiplicity and nulls.
        let dataframe = context
            .sql(&format!(
                "SELECT COALESCE(SUM(CASE WHEN n > 0 THEN n ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN n < 0 THEN -n ELSE 0 END), 0) \
             FROM (SELECT {group_columns}, SUM(weight) AS n FROM \
                   (SELECT {columns}, CAST(1 AS BIGINT) AS weight FROM after UNION ALL \
                    SELECT {columns}, CAST(-1 AS BIGINT) AS weight FROM before) \
                   GROUP BY {group_columns})"
            ))
            .await?;
        // Only the final aggregate scalars cross the application boundary.
        let batches = dataframe.collect().await?;
        let mut counts = Vec::new();
        for column in 0..2 {
            let count = batches
                .first()
                .and_then(|batch| {
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                })
                .filter(|array| !array.is_empty())
                .ok_or_else(|| {
                    Error::refusal("invalid_comparison", "comparison returned no scalar count")
                })?
                .value(0);
            counts.push(
                u64::try_from(count).map_err(|_| {
                    Error::refusal("invalid_comparison", "negative difference count")
                })?,
            );
        }
        comparison.added_rows = Some(counts[0]);
        comparison.removed_rows = Some(counts[1]);
        Ok(comparison)
    }

    /// Advances mutable eligibility without changing the selected output.
    ///
    /// # Errors
    /// Refuses empty reasons or conflicting native commits.
    pub async fn invalidate(
        &self,
        dataset: &Id,
        reason: &str,
        definition: Option<String>,
    ) -> Result<u64> {
        if reason.trim().is_empty() {
            return Err(Error::refusal(
                "missing_reason",
                "dependency corrections require a retained reason",
            ));
        }
        if definition
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(Error::refusal(
                "invalid_request",
                "definition must be nonempty",
            ));
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let mut authority: Authority = self
            .inner
            .lifecycle
            .get(&view, "authority")
            .await?
            .unwrap_or_default();
        authority.eligibility = authority
            .eligibility
            .checked_add(1)
            .ok_or_else(|| Error::refusal("revision_exhausted", "eligibility overflow"))?;
        if let Some(definition) = definition {
            authority.definition = definition;
        }
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record("authority", &authority)?,
                    record(format!("invalidation/{}", authority.eligibility), reason)?,
                ],
            )
            .await?;
        Ok(authority.eligibility)
    }

    /// Runs or resumes a selected input through an independently readable release.
    ///
    /// # Errors
    /// Refuses mismatched inputs, stale eligibility, capacity exhaustion, or corrupt outputs.
    /// Earlier qualified phases remain retained after any failure.
    pub async fn run(&self, spec: &RunSpec) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Build(spec.clone()), None)
            .await
    }

    /// Interrupts after a durable phase for lifecycle fault qualification.
    ///
    /// # Errors
    /// Returns `injected_failure` after the selected completion, or ordinary run errors.
    #[cfg(feature = "test-util")]
    pub async fn run_until(&self, spec: &RunSpec, phase: &str) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Build(spec.clone()), Some(phase))
            .await
    }

    /// Applies complete key replacements and deletions against one exact base.
    ///
    /// # Errors
    /// Refuses stale bases, overlapping inputs, schema mismatch or failed preparation.
    pub async fn update(&self, spec: &UpdateSpec) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Update(spec.clone()), None)
            .await
    }

    /// Interrupts an update after a durable phase for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or ordinary update errors.
    #[cfg(feature = "test-util")]
    pub async fn update_until(&self, spec: &UpdateSpec, phase: &str) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Update(spec.clone()), Some(phase))
            .await
    }

    async fn run_internal(
        &self,
        spec: &OperationRequest,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        validate_request(spec)?;
        let request = fingerprint(spec)?;
        let result = self.prepare_and_select(spec, stop).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code() != "injected_failure")
        {
            // A competing attempt may select this exact operation while this
            // caller is preparing or checking its base. Reconcile durable success.
            let lock = self.lock(spec.dataset()).await;
            let _guard = lock.lock().await;
            let lifecycle = &self.inner.lifecycle;
            let view = lifecycle.open(spec.dataset()).await?;
            let operation: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation()))
                .await?;
            if operation.is_some_and(|operation| operation.request_digest == request)
                && let Some(completed) = lifecycle
                    .get(&view, &format!("result/{}", spec.operation()))
                    .await?
            {
                return Ok(completed);
            }
        }
        result
    }

    async fn prepare_and_select(
        &self,
        spec: &OperationRequest,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        validate_request(spec)?;
        let request = fingerprint(spec)?;
        let lock = self.lock(spec.dataset()).await;
        let lifecycle = &self.inner.lifecycle;
        let operation = {
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            let old: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation()))
                .await?;
            if old.as_ref().is_some_and(|o| o.request_digest != request) {
                return Err(conflicting_operation());
            }
            if let Some(result) = lifecycle
                .get(&view, &format!("result/{}", spec.operation()))
                .await?
            {
                return Ok(result);
            }
            let mut authority: Authority =
                lifecycle.get(&view, "authority").await?.unwrap_or_default();
            if authority.definition.is_empty() {
                authority.definition = spec.definition().to_owned();
            }
            check_definition(&authority, spec)?;
            check_base(&authority, spec.base())?;
            if let Some(old) = old {
                check_authority(&authority, &old)?;
                old
            } else {
                authority.boundary = authority.boundary.checked_add(1).ok_or_else(|| {
                    Error::refusal("revision_exhausted", "source boundary overflow")
                })?;
                let operation = Operation {
                    request_digest: request.clone(),
                    eligibility: authority.eligibility,
                    epoch: authority.epoch,
                    boundary: authority.boundary,
                    definition: authority.definition.clone(),
                    base: spec.base().cloned(),
                };
                lifecycle
                    .commit(
                        &view,
                        vec![
                            record("authority", &authority)?,
                            record(format!("operation/{}", spec.operation()), &operation)?,
                        record(format!("request/{}", spec.operation()), spec)?,
                        record(format!("runtime/{}", spec.operation()), serde_json::json!({
                            "processor": PROCESSOR, "package_version": env!("CARGO_PKG_VERSION"),
                            "dependency_lock_blake3": blake3::hash(include_bytes!("../Cargo.lock")).to_hex().as_str(),
                            "limits": lifecycle.budget.limits,
                        }))?,
                        ],
                    )
                    .await?;
                operation
            }
        };

        let admission = if let Some(phase) = self.phase(spec, &operation, "admission").await? {
            phase
        } else {
            let directory = self.reserve_attempt(spec, &operation, "admission").await?;
            let inputs = spec.inputs();
            let size = inputs
                .values()
                .try_fold(0u64, |sum, input| -> Result<u64> {
                    Ok(sum.saturating_add(std::fs::metadata(&input.path)?.len()))
                })?;
            let available = fs2::available_space(&self.inner.root)?;
            if available
                < size
                    .saturating_mul(3)
                    .saturating_add(lifecycle.budget.limits.scratch_bytes)
            {
                return Err(Error::refusal(
                    "disk_capacity",
                    "input, output and declared scratch do not fit available disk",
                ));
            }
            let mut outputs = Vec::new();
            for (name, input) in inputs {
                let owner = spec.dataset().clone();
                let source = PathBuf::from(input.path);
                let destination = directory.join(format!("{name}.parquet"));
                let retained = lifecycle
                    .budget
                    .blocking(move || {
                        storage::copy_verified(&owner, &source, &destination, &input.digest)
                    })
                    .await?;
                outputs.push(retained);
            }
            let phase = PhaseResult {
                operation: spec.operation().clone(),
                phase: "admission".into(),
                request_digest: request.clone(),
                eligibility: operation.eligibility,
                outputs,
                table: None,
                rows: 0,
                changes: None,
                metrics: WriteMetrics::default(),
            };
            self.complete(spec, &operation, &phase).await?
        };
        stop_after(stop, "admission")?;

        let processing = if let Some(phase) = self.phase(spec, &operation, "processing").await? {
            phase
        } else {
            let directory = self.reserve_attempt(spec, &operation, "processing").await?;
            let prepared = match spec {
                OperationRequest::Build(build) => {
                    self.process_build(build, &directory, &admission).await?
                }
                OperationRequest::Update(update) => {
                    self.process_update(update, &directory, &admission).await?
                }
            };
            let mut outputs = prepared.table.objects.clone();
            if let Some(changes) = &prepared.changes {
                outputs.push(changes.base_manifest.clone());
                outputs.extend(changes.removed.objects.clone());
                outputs.extend(changes.added.objects.clone());
            }
            outputs.sort_by(|left, right| left.path.cmp(&right.path));
            outputs.dedup();
            let phase = PhaseResult {
                operation: spec.operation().clone(),
                phase: "processing".into(),
                request_digest: request.clone(),
                eligibility: operation.eligibility,
                outputs,
                rows: prepared.table.rows,
                table: Some(prepared.table),
                changes: prepared.changes,
                metrics: prepared.metrics,
            };
            self.complete(spec, &operation, &phase).await?
        };
        stop_after(stop, "processing")?;

        let table = processing.table.clone().ok_or_else(|| {
            Error::refusal("corrupt_phase", "processing completion has no table pin")
        })?;
        if self.phase(spec, &operation, "validation").await?.is_none() {
            verify_table(&table, &lifecycle.budget).await?;
            verify_changes(spec, &table, processing.changes.as_ref(), &lifecycle.budget).await?;
            let phase = PhaseResult {
                phase: "validation".into(),
                ..processing.clone()
            };
            self.complete(spec, &operation, &phase).await?;
        }
        stop_after(stop, "validation")?;

        let mut inputs = if let Some(base) = spec.base() {
            self.release(spec.dataset(), base).await?.dataset.inputs
        } else {
            BTreeMap::new()
        };
        for ((name, _), input) in spec.inputs().iter().zip(&admission.outputs) {
            let name = match spec {
                OperationRequest::Build(_) => name.clone(),
                OperationRequest::Update(_) => format!("{}/{name}", spec.operation()),
            };
            inputs.insert(name, input.clone());
        }
        let dataset = DatasetPin {
            format: 2,
            dataset: spec.dataset().clone(),
            version: spec.operation().clone(),
            groups: BTreeMap::from([("source".into(), table)]),
            inputs,
            changes: processing.changes.clone(),
            definition: operation.definition.clone(),
            eligibility: operation.eligibility,
            boundary: operation.boundary,
            request: spec.clone(),
            processor: PROCESSOR.into(),
        };
        // Readback has an operation-specific manifest because coverage can advance
        // while the same expensive materialized values remain reusable.
        let readback_key = format!("readback/{}", spec.operation());
        let release = if let Some(release) = lifecycle
            .get::<ReleasePin>(&lifecycle.open(spec.dataset()).await?, &readback_key)
            .await?
        {
            release
        } else {
            let directory = self.reserve_attempt(spec, &operation, "release").await?;
            let path = directory.join("release.json");
            storage::write_new(&path, &serde_json::to_vec_pretty(&dataset)?)?;
            let manifest = storage::object(spec.dataset(), &path)?;
            // Decode the serialized release independently of the producing context.
            let decoded: DatasetPin = serde_json::from_reader(std::fs::File::open(&path)?)?;
            if decoded != dataset {
                return Err(Error::refusal(
                    "release_mismatch",
                    "serialized release differs from selected result",
                ));
            }
            let release = ReleasePin {
                id: spec.operation().clone(),
                dataset,
                manifest,
                verified: true,
            };
            verify_release_with_budget(&release, &lifecycle.budget).await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            self.check(&view, &operation).await?;
            if let Some(completed) = lifecycle.get::<ReleasePin>(&view, &readback_key).await? {
                completed
            } else {
                lifecycle
                    .commit(&view, vec![record(&readback_key, &release)?])
                    .await?;
                release
            }
        };
        stop_after(stop, "readback")?;
        {
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation()))
                .await?
            {
                return Ok(completed);
            }
            let mut authority = self.check(&view, &operation).await?;
            if authority
                .selected
                .as_ref()
                .is_some_and(|p| p.boundary > operation.boundary)
            {
                return Err(Error::refusal(
                    "coverage_regression",
                    "a newer captured input boundary is already selected",
                ));
            }
            authority.selected = Some(release.dataset.clone());
            authority.release = Some(release.clone());
            lifecycle
                .commit(
                    &view,
                    vec![
                        record("authority", authority)?,
                        record(format!("result/{}", spec.operation()), &release)?,
                    ],
                )
                .await?;
        }
        stop_after(stop, "selection")?;
        Ok(release)
    }

    async fn check(&self, view: &crate::catalog::View, operation: &Operation) -> Result<Authority> {
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        check_authority(&authority, operation)?;
        Ok(authority)
    }

    async fn phase(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &str,
    ) -> Result<Option<PhaseResult>> {
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation).await?;
        let found: Option<PhaseResult> = self
            .inner
            .lifecycle
            .get(
                &view,
                &phase_key(
                    spec,
                    &operation.request_digest,
                    operation.eligibility,
                    phase,
                ),
            )
            .await?;
        if let Some(found) = &found {
            for output in &found.outputs {
                if std::fs::metadata(&output.path)?.len() != output.bytes {
                    return Err(Error::refusal(
                        "unavailable_phase",
                        "retained phase output size changed",
                    ));
                }
            }
            tracing::info!(phase, operation = %spec.operation(), "reusing qualified phase");
        }
        Ok(found)
    }

    async fn reserve_attempt(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        purpose: &str,
    ) -> Result<PathBuf> {
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .root
            .join("objects")
            .join(spec.dataset().as_str())
            .join(id.as_str());
        let reservation = Protection {
            id: id.clone(),
            consumer: spec.dataset().clone(),
            purpose: format!("{}/{}", spec.operation(), purpose),
            paths: vec![format!("{}/", directory.display())],
            retired: false,
        };
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation).await?;
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![record(format!("protection/{id}"), reservation)?],
            )
            .await?;
        std::fs::create_dir_all(&directory)?;
        Ok(directory)
    }

    async fn complete(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &PhaseResult,
    ) -> Result<PhaseResult> {
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation).await?;
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            &phase.phase,
        );
        // Another attempt may have completed the same deterministic phase. Preserve
        // that generation rather than relabeling or replacing its evidence.
        if let Some(completed) = self.inner.lifecycle.get::<PhaseResult>(&view, &key).await? {
            return Ok(completed);
        }
        self.inner
            .lifecycle
            .commit(&view, vec![record(key, phase)?])
            .await?;
        tracing::info!(phase = phase.phase, rows = phase.rows, operation = %spec.operation(), "phase durably complete");
        Ok(phase.clone())
    }
}

fn phase_key(spec: &OperationRequest, request: &str, eligibility: u64, phase: &str) -> String {
    if phase == "admission" {
        let mut hash = blake3::Hasher::new();
        for (name, input) in spec.inputs() {
            hash.update(&(name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash.update(input.digest.as_bytes());
        }
        return format!("phase/input/{}/{phase}", hash.finalize().to_hex());
    }
    format!("phase/{request}/{eligibility}/{phase}")
}
fn conflicting_operation() -> Error {
    Error::refusal(
        "operation_conflict",
        "operation identity already binds a different request",
    )
}
fn check_authority(authority: &Authority, operation: &Operation) -> Result<()> {
    if authority.eligibility != operation.eligibility
        || authority.definition != operation.definition
        || authority.epoch != operation.epoch
    {
        return Err(Error::refusal(
            "stale_eligibility",
            "definition, dependency qualification or authority changed; start a new operation",
        ));
    }
    check_base(authority, operation.base.as_ref())
}
fn check_definition(authority: &Authority, spec: &OperationRequest) -> Result<()> {
    if !authority.definition.is_empty() && authority.definition != spec.definition() {
        return Err(Error::refusal(
            "stale_definition",
            "activate the definition explicitly before running",
        ));
    }
    Ok(())
}
fn check_base(authority: &Authority, expected: Option<&Id>) -> Result<()> {
    if let Some(expected) = expected
        && authority
            .selected
            .as_ref()
            .is_none_or(|selected| &selected.version != expected)
    {
        return Err(Error::refusal(
            "stale_base",
            "the selected version changed; submit a new update against its exact base",
        ));
    }
    Ok(())
}
fn fingerprint(spec: &OperationRequest) -> Result<String> {
    let material = match spec {
        OperationRequest::Build(spec) => {
            serde_json::json!({"build": [&spec.expected_digest, &spec.sql, &spec.definition]})
        }
        OperationRequest::Update(spec) => {
            serde_json::json!({"update": {"base": spec.base, "keys": spec.keys, "replacements": spec.replacements.digest, "deletions": spec.deletions.as_ref().map(|input| &input.digest), "definition": spec.definition}})
        }
    };
    Ok(blake3::hash(&serde_json::to_vec(&(PROCESSOR, material))?)
        .to_hex()
        .to_string())
}
fn validate_request(spec: &OperationRequest) -> Result<()> {
    if let OperationRequest::Build(build) = spec {
        return validate_spec(build);
    }
    if spec.definition().is_empty()
        || spec.inputs().values().any(|input| {
            input.digest.len() != 64 || !input.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(Error::refusal(
            "invalid_request",
            "expected BLAKE3 input digests and a nonempty definition",
        ));
    }
    if let OperationRequest::Update(update) = spec {
        let unique: std::collections::BTreeSet<_> = update.keys.iter().collect();
        if update.keys.is_empty()
            || unique.len() != update.keys.len()
            || update.base == update.operation
        {
            return Err(Error::refusal(
                "invalid_update",
                "update needs distinct key fields and a different base operation",
            ));
        }
    }
    Ok(())
}
fn validate_spec(spec: &RunSpec) -> Result<()> {
    if spec.expected_digest.len() != 64
        || !spec.expected_digest.bytes().all(|c| c.is_ascii_hexdigit())
        || spec.definition.is_empty()
    {
        return Err(Error::refusal(
            "invalid_request",
            "expected a BLAKE3 input digest and nonempty definition",
        ));
    }
    use datafusion::sql::sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
    let statements = Parser::parse_sql(&GenericDialect {}, &spec.sql)
        .map_err(|e| Error::refusal("invalid_sql", e.to_string()))?;
    if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
        return Err(Error::refusal(
            "query_only",
            "processing accepts exactly one read-only query over input",
        ));
    }
    use datafusion::sql::sqlparser::ast::Visit;
    if let std::ops::ControlFlow::Break((code, reason)) = statements[0].visit(&mut SqlQualification)
    {
        return Err(Error::refusal(code, reason));
    }
    Ok(())
}

struct SqlQualification;
impl datafusion::sql::sqlparser::ast::Visitor for SqlQualification {
    type Break = (&'static str, &'static str);
    fn pre_visit_expr(
        &mut self,
        expression: &datafusion::sql::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion::sql::sqlparser::ast::Expr::Function(function) = expression
            && function.over.is_some()
        {
            return std::ops::ControlFlow::Break((
                "unrepeatable_query",
                "window functions require ordering qualification before their results can be reused",
            ));
        }
        std::ops::ControlFlow::Continue(())
    }
    fn pre_visit_query(
        &mut self,
        query: &datafusion::sql::sqlparser::ast::Query,
    ) -> std::ops::ControlFlow<Self::Break> {
        if query.limit_clause.is_some() || query.fetch.is_some() {
            return std::ops::ControlFlow::Break((
                "unrepeatable_query",
                "LIMIT, OFFSET and FETCH require total-order qualification before their results can be reused",
            ));
        }
        use datafusion::sql::sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};
        fn unsupported(body: &SetExpr) -> bool {
            match body {
                SetExpr::SetOperation {
                    op,
                    set_quantifier,
                    left,
                    right,
                } => {
                    (matches!(op, SetOperator::Except | SetOperator::Intersect)
                        && *set_quantifier == SetQuantifier::All)
                        || unsupported(left)
                        || unsupported(right)
                }
                _ => false,
            }
        }
        if unsupported(&query.body) {
            std::ops::ControlFlow::Break((
                "unsupported_sql",
                "EXCEPT ALL and INTERSECT ALL do not preserve required duplicate semantics in the pinned engine",
            ))
        } else {
            std::ops::ControlFlow::Continue(())
        }
    }
}

fn validate_plan(plan: &datafusion::logical_expr::LogicalPlan) -> Result<()> {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        logical_expr::{Expr, LogicalPlan, Volatility},
    };
    let mut refusal = None;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node && scan.table_name.table() != "input" {
            refusal = Some("processing may read only the retained input");
        }
        if matches!(node, LogicalPlan::Limit(_)) {
            refusal = Some("LIMIT, OFFSET and FETCH require total-order qualification before their results can be reused");
        }
        node.apply_expressions(|expression| expression.apply(|part| {
            let volatility = match part {
                Expr::ScalarFunction(f) => Some(f.func.signature().volatility),
                Expr::AggregateFunction(f) => {
                    if f.func.order_sensitivity() != datafusion::logical_expr::utils::AggregateOrderSensitivity::Insensitive {
                        refusal = Some("order-sensitive aggregates require ordering qualification before their results can be reused");
                    }
                    Some(f.func.signature().volatility)
                }
                Expr::WindowFunction(_) => {
                    refusal = Some("window functions require ordering qualification before their results can be reused");
                    None
                }
                _ => None,
            };
            if volatility.is_some_and(|v| v != Volatility::Immutable) {
                refusal = Some("reusable processing requires immutable functions; clock and random functions are unsupported");
            }
            Ok(TreeNodeRecursion::Continue)
        }))?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(reason) = refusal {
        return Err(Error::refusal("unrepeatable_query", reason));
    }
    Ok(())
}
fn stop_after(stop: Option<&str>, phase: &str) -> Result<()> {
    if stop == Some(phase) {
        return Err(Error::refusal(
            "injected_failure",
            format!("interrupted after durable {phase}"),
        ));
    }
    Ok(())
}

/// Verifies exact retained objects and decodes a native result without its catalog.
///
/// # Errors
/// Refuses changed bytes, invalid schemas, missing files, or row-count disagreement.
pub async fn verify_table(pin: &TablePin, budget: &Budget) -> Result<()> {
    storage::verify_objects(pin.objects.clone(), budget).await?;
    let table = storage::open_pin(pin).await?;
    storage::check_inventory(pin, &table, budget).await?;
    let mut rows = 0;
    if pin.snapshot.is_some() {
        let stream = storage::read_batches(&table, budget).await?;
        futures::pin_mut!(stream);
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows() as u64;
        }
    }
    if rows != pin.rows {
        return Err(Error::refusal(
            "row_count_mismatch",
            "independent native reader disagrees with committed writer count",
        ));
    }
    Ok(())
}

/// Independently verifies a serialized release and all retained data and evidence.
///
/// # Errors
/// Refuses modified or missing objects and inconsistent native metadata.
pub async fn verify_release(release: &ReleasePin, limits: Limits) -> Result<()> {
    let budget = Budget::new(limits)?;
    verify_release_with_budget(release, &budget).await
}

async fn verify_release_with_budget(release: &ReleasePin, budget: &Budget) -> Result<()> {
    let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
    if release.manifest.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "release manifest exceeds reader budget",
        ));
    }
    storage::verify_object(&release.manifest)?;
    let decoded: DatasetPin =
        serde_json::from_reader(std::fs::File::open(&release.manifest.path)?)?;
    if decoded != release.dataset {
        return Err(Error::refusal(
            "release_mismatch",
            "release manifest does not match its pin",
        ));
    }
    if decoded.format != 2
        || release.id != decoded.version
        || decoded.request.dataset() != &decoded.dataset
        || decoded.request.operation() != &decoded.version
        || decoded.request.definition() != decoded.definition
        || decoded.request.inputs().iter().any(|(name, input)| {
            let key = match &decoded.request {
                OperationRequest::Build(_) => name.clone(),
                OperationRequest::Update(_) => format!("{}/{name}", decoded.version),
            };
            decoded
                .inputs
                .get(&key)
                .is_none_or(|retained| retained.digest != input.digest)
        })
    {
        return Err(Error::refusal(
            "release_mismatch",
            "release request, version, or input binding differs",
        ));
    }
    storage::verify_objects(decoded.inputs.values().cloned().collect(), budget).await?;
    let source = decoded
        .groups
        .get("source")
        .ok_or_else(|| Error::refusal("release_mismatch", "release has no source table"))?;
    verify_changes(&decoded.request, source, decoded.changes.as_ref(), budget).await?;
    for pin in decoded.groups.values() {
        verify_table(pin, budget).await?;
    }
    Ok(())
}

async fn verify_changes(
    request: &OperationRequest,
    source: &TablePin,
    changes: Option<&ChangePin>,
    budget: &Budget,
) -> Result<()> {
    let invalid = || {
        Error::refusal(
            "invalid_changes",
            "change rows, schemas or endpoints differ from the captured request and result",
        )
    };
    let (spec, changes) = match (request, changes) {
        (OperationRequest::Build(_), None) => return Ok(()),
        (OperationRequest::Update(spec), Some(changes)) => (spec, changes),
        _ => return Err(invalid()),
    };
    let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
    if changes.base_manifest.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "base manifest exceeds reader budget",
        ));
    }
    storage::verify_objects(
        vec![
            changes.base_manifest.clone(),
            changes.before_metadata.clone(),
        ],
        budget,
    )
    .await?;
    let before: DatasetPin =
        serde_json::from_reader(std::fs::File::open(&changes.base_manifest.path)?)?;
    let before_source = before.groups.get("source").ok_or_else(invalid)?;
    if before.format != 2
        || before.dataset != spec.dataset
        || before.version != spec.base
        || changes.base_version != spec.base
        || changes.before_metadata != before_source.metadata
        || changes.after_metadata != source.metadata
        || before_source
            .rows
            .checked_sub(changes.removed.rows)
            .and_then(|rows| rows.checked_add(changes.added.rows))
            != Some(source.rows)
    {
        return Err(invalid());
    }
    let native_before = storage::open_pin(before_source).await?;
    let native_after = storage::open_pin(source).await?;
    let expected_ids: Option<Vec<_>> = spec
        .keys
        .iter()
        .map(|name| {
            native_before
                .metadata()
                .current_schema()
                .field_by_name(name)
                .map(|field| field.id)
        })
        .collect();
    if expected_ids.as_ref() != Some(&changes.key_field_ids)
        || native_before.metadata().current_schema() != native_after.metadata().current_schema()
    {
        return Err(invalid());
    }
    for pin in [&changes.removed, &changes.added] {
        verify_table(pin, budget).await?;
        if storage::open_pin(pin).await?.metadata().current_schema()
            != native_after.metadata().current_schema()
        {
            return Err(invalid());
        }
    }
    Ok(())
}
