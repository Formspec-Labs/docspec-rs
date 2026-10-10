//! Shared local application operations and durable phase handoffs.
//!
//! One process owns the workspace. Short per-dataset sections serialize admission
//! and selection; immutable data preparation can proceed independently.

use crate::{
    Error, Result,
    budget::{Budget, Limits, WorkspaceLock},
    catalog::{Lifecycle, View, record},
    model::*,
    storage,
};
use datafusion::prelude::*;
use futures::{StreamExt, TryStreamExt};
use iceberg::Catalog;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

mod admission;
mod backfill;
#[doc(inline)]
pub use backfill::{BackfillPlan, read_combined};
mod changes;
mod compaction;
mod comparison;
mod corrections;
mod evidence;
mod export;
#[doc(inline)]
pub use evidence::read_receipts;
mod history;
mod restore;
#[doc(inline)]
pub use changes::verify_applied_changes;
use changes::verify_changes;
mod inputs;
#[doc(inline)]
pub use inputs::verify_input_coverage;
mod read;
#[doc(inline)]
pub use read::{read_compaction, read_evidence, read_group};
mod aggregation;
mod incremental;
mod refresh;
pub(crate) mod retention;
use retention::owners::ReferenceProtections;
mod schema;
mod verification;
mod worker;
mod write;

/// Local execution host sharing catalog, admission, and resource accounting.
///
/// Once admitted, catalog operations finish in owned tasks even if their caller
/// disconnects or drops its future. Use [`Self::abandon`] to revoke an unfinished
/// operation's selection authority, and [`Self::shutdown`] before stopping the
/// runtime. Engine read streams retain shared admission until EOF or drop.
#[derive(Clone, Debug)]
pub struct Engine {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    lifecycle: Lifecycle,
    locks: Mutex<BTreeMap<Id, Arc<Mutex<()>>>>,
    host: Arc<worker::Host>,
    maintenance: Arc<RwLock<()>>,
    closing: AtomicBool,
    owned_catalog: bool,
    #[cfg(feature = "test-util")]
    scalar_reuse_pause: Mutex<Option<ScalarReusePause>>,
}

#[cfg(feature = "test-util")]
#[derive(Debug)]
struct ScalarReusePause {
    dataset: Id,
    operation: Id,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

// A retained value and its protection always come from the same native view.
// None means unknown legacy protection, not a qualified empty dependency set.
struct Retained<T> {
    value: T,
    protections: Option<ReferenceProtections>,
}

struct PreparedSource {
    admission: Option<Retained<PhaseResult>>,
    processing: Retained<PhaseResult>,
}

impl<T> std::ops::Deref for Retained<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

fn completion_records<T: Serialize>(
    consumer: &Id,
    view: &View,
    key: &str,
    completion: &T,
    protections: Option<&ReferenceProtections>,
) -> Result<Vec<(String, serde_json::Value)>> {
    let claim = record(key, completion)?;
    let binding = protections
        .map(|scope| scope.binding(consumer, view, key, &claim.1))
        .transpose()?;
    let mut records = vec![claim];
    records.extend(binding);
    Ok(records)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Operation {
    request_digest: String,
    eligibility: u64,
    epoch: u64,
    boundary: u64,
    /// Schema composition keeps source coverage separate from later admissions.
    #[serde(default)]
    admission_fence: Option<u64>,
    /// An interrupted admission retains its original release interpretation.
    #[serde(default = "previous_format")]
    result_format: u32,
    definition: String,
    base: Option<Id>,
    base_manifest: Option<ObjectRef>,
    /// Material native schema/history for full rebuilds with omission rules.
    #[serde(default)]
    schema_key: Option<String>,
    /// Explicit rebuilds fence later source admissions before replacing the selection.
    require_current_boundary: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum CompletedGroups {
    Captured {
        outputs: BTreeMap<String, ObjectRef>,
        source: NativeSnapshotRef,
    },
    // Old progress records remain readable, but their source is unknown to cleanup.
    Legacy(BTreeMap<String, ObjectRef>),
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
const PROCESSOR: &str = "docspec.dataset.v10";
// Format 11 changes schema coverage metadata, not material processing/reuse keys.
pub(crate) const FORMAT: u32 = 11;
const PREVIOUS_FORMAT: u32 = 10;

fn previous_format() -> u32 {
    PREVIOUS_FORMAT
}

fn supported_format(format: u32) -> bool {
    matches!(format, PREVIOUS_FORMAT | FORMAT)
}

impl Engine {
    /// Opens an exclusively owned local workspace with a maintained catalog.
    ///
    /// # Errors
    /// Refuses a live owner, invalid limits, or catalog/storage initialization failure.
    pub async fn open(root: impl AsRef<Path>, limits: Limits) -> Result<Self> {
        let runtime = worker::runtime()?;
        let host = worker::Host::acquire(root.as_ref())?;
        let root = std::fs::canonicalize(root)?;
        let custody = worker::Custody::new(host.clone());
        runtime
            .spawn(async move {
                let result = async {
                    let catalog = storage::local_catalog(&root).await?;
                    Self::compose(root, catalog, limits, host, true)
                }
                .await;
                custody.complete();
                result
            })
            .await?
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
        let ownership = worker::Host::acquire(root.as_ref())?;
        Self::compose(
            std::fs::canonicalize(root)?,
            catalog,
            limits,
            ownership,
            false,
        )
    }

    fn compose(
        root: PathBuf,
        catalog: Arc<dyn Catalog>,
        limits: Limits,
        ownership: Arc<worker::Host>,
        owned_catalog: bool,
    ) -> Result<Self> {
        let scratch = root.join("scratch");
        std::fs::create_dir_all(&scratch)?;
        let budget = Budget::in_workspace(limits, &scratch)?;
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                lifecycle: Lifecycle::new(catalog, budget),
                locks: Mutex::new(BTreeMap::new()),
                host: ownership,
                maintenance: Arc::new(RwLock::new(())),
                closing: AtomicBool::new(false),
                owned_catalog,
                #[cfg(feature = "test-util")]
                scalar_reuse_pause: Mutex::new(None),
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
        self.inner.lifecycle.budget.context()
    }

    /// Pauses the next actual scalar file reuse for this operation in tests.
    ///
    /// The first channel signals the hit; sending or dropping the second resumes
    /// work. Installing a pause replaces any unused pause on this engine.
    #[cfg(feature = "test-util")]
    pub async fn pause_scalar_file_reuse(
        &self,
        dataset: Id,
        operation: Id,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached, receiver) = tokio::sync::oneshot::channel();
        let (sender, release) = tokio::sync::oneshot::channel();
        *self.inner.scalar_reuse_pause.lock().await = Some(ScalarReusePause {
            dataset,
            operation,
            reached,
            release,
        });
        (receiver, sender)
    }

    #[cfg(feature = "test-util")]
    async fn scalar_file_reused(&self, request: &OperationRequest) {
        let pause = {
            let mut pending = self.inner.scalar_reuse_pause.lock().await;
            if pending.as_ref().is_some_and(|pause| {
                &pause.dataset == request.dataset() && &pause.operation == request.operation()
            }) {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = pause.release.await;
        }
    }

    /// Explains retained phases without opening large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting operation identities or invalid requests.
    pub async fn plan(&self, spec: &RunSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Build(spec.clone()), memory)
            .await
    }

    /// Plans an update without opening its large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting identities, stale bases or definitions, and invalid requests.
    pub async fn plan_update(&self, spec: &UpdateSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Update(spec.clone()), memory)
            .await
    }

    async fn plan_request(
        &self,
        spec: OperationRequest,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<RunPlan> {
        self.admit_work(memory, move |engine| async move {
            engine.plan_request_admitted(&spec).await
        })
        .await
    }

    async fn plan_request_admitted(&self, spec: &OperationRequest) -> Result<RunPlan> {
        validate_request(spec)?;
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(spec.dataset()).await?;
        self.check_operation_available(&view, spec.operation())
            .await?;
        self.check_not_abandoned(&view, spec.operation()).await?;
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
            check_update_eligibility(&authority, spec)?;
            check_group_rebuild(&authority, spec)?;
            self.check_incremental(&authority, spec).await?;
            self.check_schema(&view, &authority, spec, operation.is_none())
                .await?;
        }
        if !completed && let Some(operation) = &operation {
            check_authority(&authority, operation)?;
        }
        if lifecycle
            .get::<ReleasePin>(&view, &format!("readback/{}", spec.operation()))
            .await?
            .is_some()
        {
            return Ok(RunPlan {
                dataset: spec.dataset().clone(),
                operation: spec.operation().clone(),
                completed,
                reusable_phases: vec!["readback".into()],
                required_phases: Vec::new(),
                limits: lifecycle.budget.limits.clone(),
            });
        }
        let mut reusable_phases = Vec::new();
        let mut required_phases = Vec::new();
        let mut phases = PHASES.to_vec();
        if matches!(spec, OperationRequest::Refresh(_)) {
            phases.insert(2, "evidence");
        }
        let mut receipt_base = None;
        if let Some(version) = spec.base() {
            let base: ReleasePin = lifecycle
                .get(&view, &format!("result/{version}"))
                .await?
                .ok_or_else(|| {
                    Error::refusal("no_release", "captured source version is unavailable")
                })?;
            if !base.dataset.computed.is_empty() && !matches!(spec, OperationRequest::Evidence(_)) {
                phases.insert(3, "groups");
            }
            if base.dataset.evidence.is_some()
                && !matches!(
                    spec,
                    OperationRequest::Evidence(_) | OperationRequest::Schema(_)
                )
            {
                phases.insert(2, "receipts");
                receipt_base = Some(base.dataset);
            }
        }
        let schema_key = match &operation {
            Some(operation) => operation.schema_key.clone(),
            None => self.schema_key(&authority, spec).await?,
        };
        let mut retained_processing = false;
        let mut adopted_receipts = false;
        for phase in phases {
            if phase == "receipts" && adopted_receipts {
                continue;
            }
            let exists = if phase == "groups" {
                self.active_completion::<CompletedGroups>(
                    &view,
                    &format!("groups/{}", spec.operation()),
                )
                .await?
                .is_some()
            } else if phase == "readback" {
                lifecycle
                    .get::<ReleasePin>(&view, &format!("readback/{}", spec.operation()))
                    .await?
                    .is_some()
            } else {
                let eligibility = operation
                    .as_ref()
                    .map_or(authority.eligibility, |o| o.eligibility);
                let completion = self
                    .active_completion::<PhaseResult>(
                        &view,
                        &phase_key(
                            spec,
                            &request,
                            eligibility,
                            phase,
                            operation
                                .as_ref()
                                .and_then(|o| o.base_manifest.as_ref())
                                .or_else(|| captured_manifest(&authority, spec)),
                            schema_key.as_ref(),
                        )?,
                    )
                    .await?;
                if phase == "processing" {
                    retained_processing = completion
                        .as_ref()
                        .is_some_and(|phase| phase.dependencies.is_some());
                    if let (Some(processing), Some(base)) = (&completion, &receipt_base) {
                        adopted_receipts = evidence::adopts_receipts(spec, processing, base)?;
                    }
                }
                completion.is_some()
            };
            if exists {
                reusable_phases.push(phase.to_owned());
            } else {
                required_phases.push(phase.to_owned());
            }
        }
        if retained_processing {
            required_phases.retain(|phase| phase != "admission");
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
        let memory = self.request_memory(dataset)?;
        let dataset = dataset.clone();
        self.admit_work(memory, move |engine| async move {
            engine.inspect_admitted(&dataset).await
        })
        .await
    }

    async fn inspect_admitted(&self, dataset: &Id) -> Result<BTreeMap<String, serde_json::Value>> {
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
        let memory = self.request_memory(dataset)?;
        let dataset = dataset.clone();
        self.admit_work(memory, move |engine| async move {
            engine.latest_admitted(&dataset).await
        })
        .await
    }

    async fn latest_admitted(&self, dataset: &Id) -> Result<ReleasePin> {
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
    /// Refuses unknown versions, rebound identities, or unavailable lifecycle metadata.
    pub async fn release(&self, dataset: &Id, version: &Id) -> Result<ReleasePin> {
        let memory = self.request_memory(&(dataset, version))?;
        let dataset = dataset.clone();
        let version = version.clone();
        self.admit_work(memory, move |engine| async move {
            engine.available_release_admitted(&dataset, &version).await
        })
        .await
    }

    async fn release_admitted(&self, dataset: &Id, version: &Id) -> Result<ReleasePin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.resolve_release(&view, dataset, version).await
    }

    async fn available_release_admitted(&self, dataset: &Id, version: &Id) -> Result<ReleasePin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.check_operation_available(&view, version).await?;
        self.resolve_release(&view, dataset, version).await
    }

    async fn resolve_release(
        &self,
        view: &crate::catalog::View,
        dataset: &Id,
        version: &Id,
    ) -> Result<ReleasePin> {
        let release: ReleasePin = self
            .inner
            .lifecycle
            .get(view, &format!("result/{version}"))
            .await?
            .ok_or_else(|| Error::refusal("no_release", "operation has no completed release"))?;
        if &release.dataset.dataset != dataset
            || &release.id != version
            || &release.dataset.version != version
            || !release.verified
        {
            return Err(Error::refusal(
                "release_mismatch",
                "catalog result differs from the requested accepted version",
            ));
        }
        Ok(release)
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
        let memory = self.request_memory(&(dataset, reason, &definition))?;
        let dataset = dataset.clone();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine
                .invalidate_admitted(&dataset, &reason, definition)
                .await
        })
        .await
    }

    async fn invalidate_admitted(
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
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Build(spec.clone()), memory, None)
            .await
    }

    /// Interrupts after a durable phase for lifecycle fault qualification.
    ///
    /// # Errors
    /// Returns `injected_failure` after the selected completion, or ordinary run errors.
    #[cfg(feature = "test-util")]
    pub async fn run_until(&self, spec: &RunSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(OperationRequest::Build(spec.clone()), memory, Some(phase))
            .await
    }

    /// Applies complete key replacements and deletions against one exact base.
    ///
    /// # Errors
    /// Refuses stale bases, overlapping inputs, schema mismatch or failed preparation.
    pub async fn update(&self, spec: &UpdateSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Update(spec.clone()), memory, None)
            .await
    }

    /// Interrupts an update after a durable phase for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or ordinary update errors.
    #[cfg(feature = "test-util")]
    pub async fn update_until(&self, spec: &UpdateSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(OperationRequest::Update(spec.clone()), memory, Some(phase))
            .await
    }

    async fn run_internal(
        &self,
        spec: OperationRequest,
        memory: tokio::sync::OwnedSemaphorePermit,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        let stop = stop.map(str::to_owned);
        self.admit_work(memory, move |engine| async move {
            engine.run_admitted(&spec, stop.as_deref()).await
        })
        .await
    }

    async fn run_admitted(
        &self,
        spec: &OperationRequest,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        validate_request(spec)?;
        // Owned-host maintenance waits for this admitted worker. One check before
        // preparation and error reconciliation covers the complete operation.
        self.check_operation_entry(spec.dataset(), spec.operation())
            .await?;
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

    // Source work finishes before composition. Keep its large native futures out
    // of the enclosing poll frame while later groups, checks and selection run.
    fn prepare_source<'a>(
        &'a self,
        spec: &'a OperationRequest,
        operation: &'a Operation,
        request: &'a str,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<PreparedSource>> {
        Box::pin(async move {
            let lifecycle = &self.inner.lifecycle;
            if let Some(processing) = self.phase(spec, operation, "processing").await? {
                let admission = if processing.dependencies.is_none() {
                    let admission = self.phase(spec, operation, "admission").await?.ok_or_else(|| {
                        Error::refusal("phase_dependencies_unknown", "legacy processing needs its original admission; retire that processing generation before rebuilding")
                    })?;
                    let view = lifecycle.open(spec.dataset()).await?;
                    let rows = lifecycle.rows(&view).await?;
                    let revision = |phase| -> Result<i64> {
                        rows.revision(&phase_key(
                            spec,
                            request,
                            operation.eligibility,
                            phase,
                            operation.base_manifest.as_ref(),
                            operation.schema_key.as_ref(),
                        )?)
                        .ok_or_else(|| {
                            Error::refusal(
                                "phase_dependencies_unknown",
                                "original phase generation is unavailable",
                            )
                        })
                    };
                    if revision("admission")? > revision("processing")? {
                        return Err(Error::refusal(
                            "phase_dependencies_unknown",
                            "legacy processing cannot adopt a newer admission generation; retire that processing before rebuilding",
                        ));
                    }
                    Some(admission)
                } else {
                    None
                };
                stop_after(stop, "admission")?;
                stop_after(stop, "processing")?;
                return Ok(PreparedSource {
                    admission,
                    processing,
                });
            }
            let admission = if let Some(phase) = self.phase(spec, operation, "admission").await? {
                phase
            } else if let OperationRequest::Refresh(refresh) = spec {
                Box::pin(self.admit_refresh(refresh, operation)).await?
            } else if let OperationRequest::Schema(schema) = spec {
                self.admit_schema(schema, operation).await?
            } else {
                #[cfg(feature = "test-util")]
                work_started("admission");
                let view = lifecycle.open(spec.dataset()).await?;
                let _memory = self.admission_memory(&view, spec.operation()).await?;
                let captured = self
                    .admitted_dependencies(&view, spec.operation())
                    .await?
                    .unwrap_or_default();
                let inputs = spec.inputs();
                let size = inputs
                    .iter()
                    .try_fold(0u64, |sum, (name, input)| -> Result<u64> {
                        // Reused managed inputs consume no new input-copy space;
                        // retain the existing estimate for output/intermediate work.
                        let bytes = if let Some(object) = captured.inputs.get(name) {
                            object.bytes.saturating_mul(2)
                        } else {
                            std::fs::metadata(&input.path)?.len().saturating_mul(3)
                        };
                        Ok(sum.saturating_add(bytes))
                    })?;
                let available = fs2::available_space(&self.inner.root)?;
                if available < size.saturating_add(lifecycle.budget.limits.scratch_bytes) {
                    return Err(Error::refusal(
                        "disk_capacity",
                        "input, output and declared scratch do not fit available disk",
                    ));
                }
                let directory = if captured.inputs.len() == inputs.len() {
                    None
                } else {
                    Some(self.reserve_attempt(spec, operation, "admission").await?)
                };
                let mut outputs = Vec::new();
                for (name, input) in inputs {
                    if let Some(retained) = captured.inputs.get(&name) {
                        outputs.push(retained.clone());
                        continue;
                    }
                    let owner = spec.dataset().clone();
                    let source = PathBuf::from(input.path);
                    let destination = directory
                        .as_ref()
                        .expect("unretained input reserved a copy directory")
                        .join(format!("{name}.parquet"));
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
                    request_digest: request.to_owned(),
                    eligibility: operation.eligibility,
                    outputs,
                    table: None,
                    rows: 0,
                    changes: None,
                    coverage: None,
                    metrics: WriteMetrics::default(),
                    dependencies: Some(PhaseDependencies::default()),
                };
                let protections = self.request_protections(&view, spec.operation()).await?;
                self.complete(spec, operation, phase, protections).await?
            };
            stop_after(stop, "admission")?;

            let processing = if let Some(phase) = self.phase(spec, operation, "processing").await? {
                phase
            } else {
                #[cfg(feature = "test-util")]
                work_started("processing");
                let directory = self.reserve_attempt(spec, operation, "processing").await?;
                let (_capture_memory, update_source) = if admission::captures_source(spec) {
                    let view = lifecycle.open(spec.dataset()).await?;
                    let memory = self.admission_memory(&view, spec.operation()).await?;
                    let source = self
                        .admitted_dependencies(&view, spec.operation())
                        .await?
                        .and_then(|dependencies| dependencies.update_source);
                    (Some(memory), source)
                } else {
                    (None, None)
                };
                let prepared = match spec {
                    OperationRequest::Build(build) => {
                        self.process_build(build, &directory, &admission).await?
                    }
                    OperationRequest::Update(update) => {
                        self.process_update(update, &directory, &admission, update_source.as_ref())
                            .await?
                    }
                    OperationRequest::Schema(schema) => {
                        self.process_schema(schema, &directory, operation).await?
                    }
                    OperationRequest::Evidence(evidence) => {
                        self.process_receipts(evidence, &directory, &admission)
                            .await?
                    }
                    OperationRequest::Refresh(refresh) => {
                        Box::pin(self.process_refresh(
                            refresh,
                            &directory,
                            &admission,
                            operation,
                            update_source.as_ref(),
                        ))
                        .await?
                    }
                    OperationRequest::Backfill(_) => {
                        return Err(Error::refusal(
                            "invalid_operation",
                            "use the backfill service",
                        ));
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
                    request_digest: request.to_owned(),
                    eligibility: operation.eligibility,
                    outputs,
                    rows: prepared.table.rows,
                    table: Some(prepared.table),
                    changes: prepared.changes,
                    coverage: admission.coverage.clone(),
                    metrics: prepared.metrics,
                    dependencies: Some(PhaseDependencies {
                        admitted_inputs: spec
                            .inputs()
                            .keys()
                            .cloned()
                            .zip(admission.outputs.iter().cloned())
                            .collect(),
                        base_manifest: operation.base_manifest.clone(),
                        schema_metadata: prepared.schema_input.into_iter().collect(),
                        tables: update_source
                            .as_ref()
                            .map(NativeSnapshotRef::from)
                            .into_iter()
                            .collect(),
                    }),
                };
                let view = lifecycle.open(spec.dataset()).await?;
                let mut protections = self.request_protections(&view, spec.operation()).await?;
                ReferenceProtections::inherit(&mut protections, admission.protections.as_ref())?;
                self.complete(spec, operation, phase, protections).await?
            };
            stop_after(stop, "processing")?;

            Ok(PreparedSource {
                admission: Some(admission),
                processing,
            })
        })
    }

    // Construct the large future outside run_internal's poll frame, so its
    // temporary construction storage does not remain on every nested poll.
    fn prepare_and_select<'a>(
        &'a self,
        spec: &'a OperationRequest,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<ReleasePin>> {
        Box::pin(async move {
            validate_request(spec)?;
            let request = fingerprint(spec)?;
            let lock = self.lock(spec.dataset()).await;
            let lifecycle = &self.inner.lifecycle;
            let mut prepared: Option<admission::PreparedAdmission> = None;
            let operation = loop {
                let datasets = prepared.as_ref().map_or_else(
                    || std::collections::BTreeSet::from([spec.dataset().clone()]),
                    |prepared| prepared.datasets(spec.dataset()),
                );
                let guards = self.lock_inputs(datasets).await;
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
                self.check_not_abandoned(&view, spec.operation()).await?;
                let decoding = if old.is_none() {
                    Some(
                        lifecycle
                            .rows(&view)
                            .await?
                            .reserve_decoded(&lifecycle.budget, &["authority"])?,
                    )
                } else {
                    None
                };
                let mut authority: Authority =
                    lifecycle.get(&view, "authority").await?.unwrap_or_default();
                if authority.definition.is_empty() {
                    authority.definition = spec.definition().to_owned();
                }
                check_definition(&authority, spec)?;
                check_base(&authority, spec.base())?;
                check_update_eligibility(&authority, spec)?;
                check_group_rebuild(&authority, spec)?;
                self.check_incremental(&authority, spec).await?;
                self.check_schema(&view, &authority, spec, old.is_none())
                    .await?;
                if let Some(old) = old {
                    check_authority(&authority, &old)?;
                    break old;
                } else {
                    let Some(protected) = &prepared else {
                        // Unbased builds retain only the ordering receipt of the
                        // previous selection; no previous payload is consumed.
                        let base = spec.base().and_then(|_| authority.release.take());
                        let mut decoding = decoding.expect("new admission reserved decoding");
                        let update_source = if admission::captures_source(spec) {
                            let base = base.as_ref().ok_or_else(|| {
                                Error::refusal(
                                    "invalid_base",
                                    "incremental write has no base result",
                                )
                            })?;
                            let canonical = base.dataset.groups.get("source").ok_or_else(|| {
                                Error::refusal("invalid_base", "base has no source table")
                            })?;
                            let key = format!(
                                "representation/{}/source",
                                canonical.logical_metadata().digest
                            );
                            decoding.merge(
                                lifecycle
                                    .rows(&view)
                                    .await?
                                    .reserve_decoded(&lifecycle.budget, &[&key])?,
                            );
                            // The fallback copies the canonical pin; selected records
                            // have their own decoding allowance above.
                            decoding.merge(
                                lifecycle.budget.reserve(
                                    inputs::metadata_size(
                                        canonical,
                                        lifecycle.budget.limits.max_metadata_bytes,
                                    )?
                                    .saturating_mul(4),
                                )?,
                            );
                            Some(
                                self.representation_pin(&view, &base.id, "source", canonical)
                                    .await?,
                            )
                        } else {
                            None
                        };
                        let boundary = view.table.metadata_location_result()?.to_owned();
                        drop(guards);
                        prepared = Some(
                            self.protect_admission(spec, boundary, base, update_source, decoding)
                                .await?,
                        );
                        continue;
                    };
                    if !protected.is_current(&view)? {
                        // Repeat capture and eligibility against fresh state;
                        // committed owner grants remain reusable. A compatible
                        // concurrent operation must not force a caller retry.
                        prepared = None;
                        continue;
                    }
                    if let Some(coverage) = &protected.dependencies.coverage {
                        self.guard_inputs(coverage, None, RefreshPolicy::Captured)
                            .await?;
                    }
                    let composition = matches!(
                        spec,
                        OperationRequest::Schema(_) | OperationRequest::Evidence(_)
                    );
                    let boundary = if composition {
                        authority
                            .selected
                            .as_ref()
                            .ok_or_else(|| {
                                Error::refusal(
                                    "no_release",
                                    "schema operation needs selected source",
                                )
                            })?
                            .boundary
                    } else {
                        authority.boundary =
                            authority.boundary.checked_add(1).ok_or_else(|| {
                                Error::refusal("revision_exhausted", "source boundary overflow")
                            })?;
                        authority.boundary
                    };
                    let operation = Operation {
                        request_digest: request.clone(),
                        eligibility: authority.eligibility,
                        epoch: authority.epoch,
                        boundary,
                        admission_fence: composition.then_some(authority.boundary),
                        result_format: FORMAT,
                        definition: authority.definition.clone(),
                        base: spec.base().cloned(),
                        base_manifest: captured_manifest(&authority, spec).cloned(),
                        schema_key: self.schema_key(&authority, spec).await?,
                        require_current_boundary: matches!(spec, OperationRequest::Schema(_))
                            || matches!(spec, OperationRequest::Build(build) if build.base.is_some()),
                    };
                    let mut records = vec![
                        record(format!("operation/{}", spec.operation()), &operation)?,
                        record(format!("request/{}", spec.operation()), spec)?,
                        record(
                            format!("runtime/{}", spec.operation()),
                            serde_json::json!({
                                "processor": PROCESSOR, "package_version": env!("CARGO_PKG_VERSION"),
                                "dependency_lock_blake3": blake3::hash(include_bytes!("../Cargo.lock")).to_hex().as_str(),
                                "limits": lifecycle.budget.limits,
                            }),
                        )?,
                    ];
                    if !composition {
                        records.push(record("authority", &authority)?);
                    }
                    protected.records(&view, spec, &mut records)?;
                    lifecycle.commit(&view, records).await?;
                    break operation;
                }
            };
            drop(prepared);

            // Locators can change on a material-identical retry. Preserve the admitted
            // request as execution provenance while using caller locators for missing copies.
            let captured_request: OperationRequest = lifecycle
                .get(
                    &lifecycle.open(spec.dataset()).await?,
                    &format!("request/{}", spec.operation()),
                )
                .await?
                .ok_or_else(|| {
                    Error::refusal("corrupt_phase", "operation has no admitted request")
                })?;

            // Readback is an independent retained promise. Earlier phase retirement
            // must not force its already qualified values through preparation again.
            let retained_view = lifecycle.open(spec.dataset()).await?;
            if let Some(release) = lifecycle
                .get::<ReleasePin>(&retained_view, &format!("readback/{}", spec.operation()))
                .await?
            {
                let protections = self
                    .retained_protections(&retained_view, &format!("readback/{}", spec.operation()))
                    .await?;
                if !release.verified
                    || &release.id != spec.operation()
                    || &release.dataset.dataset != spec.dataset()
                    || release.dataset.version != release.id
                    || release.dataset.request != captured_request
                    || release.dataset.definition != captured_request.result_definition()
                    || release.dataset.format != operation.result_format
                    || release.dataset.eligibility != operation.eligibility
                    || release.dataset.boundary != operation.boundary
                {
                    return Err(Error::refusal(
                        "corrupt_phase",
                        "retained readback differs from its admitted operation",
                    ));
                }
                // Refresh selection checks the complete dependency graph below.
                // Other selections check retained availability here, once.
                if !matches!(spec, OperationRequest::Refresh(_)) {
                    check_release_available(&release, &lifecycle.budget).await?;
                }
                stop_after(stop, "readback")?;
                return self
                    .select_prepared(
                        spec,
                        &operation,
                        Retained {
                            value: release,
                            protections,
                        },
                        stop,
                    )
                    .await;
            }

            let PreparedSource {
                admission,
                processing,
            } = self
                .prepare_source(spec, &operation, &request, stop)
                .await?;

            let processed = processing.table.clone().ok_or_else(|| {
                Error::refusal("corrupt_phase", "processing completion has no table pin")
            })?;
            let base = if let Some(base) = spec.base() {
                Some(self.release_admitted(spec.dataset(), base).await?)
            } else {
                None
            };
            let evidence_only = matches!(spec, OperationRequest::Evidence(_));
            let table = if evidence_only {
                base.as_ref()
                    .and_then(|base| base.dataset.groups.get("source"))
                    .cloned()
                    .ok_or_else(|| {
                        Error::refusal("invalid_evidence", "captured result lacks source values")
                    })?
            } else {
                processed.clone()
            };
            let related = if let OperationRequest::Refresh(refresh) = spec {
                Some(Box::pin(self.prepare_evidence(refresh, &operation, &processing, stop)).await?)
            } else if evidence_only {
                base.as_ref()
                    .and_then(|base| base.dataset.related.as_ref())
                    .map(|related| {
                        Ok::<_, Error>(Retained {
                            value: (**related).clone(),
                            protections: processing
                                .protections
                                .as_ref()
                                .map(ReferenceProtections::duplicate)
                                .transpose()?,
                        })
                    })
                    .transpose()?
            } else {
                None
            };
            let receipts = if evidence_only {
                Some(Retained {
                    value: evidence::PreparedReceipts {
                        result: crate::evidence::EvidenceResult {
                            source: (&table).into(),
                            table: processed,
                        },
                        changes: processing.changes.clone(),
                    },
                    protections: processing
                        .protections
                        .as_ref()
                        .map(ReferenceProtections::duplicate)
                        .transpose()?,
                })
            } else if let Some(base) = &base {
                self.inherit_receipts(spec, &operation, &processing, base, stop)
                    .await?
            } else {
                None
            };
            let mut checks = verification::Checks::new(VerificationLevel::Routine);
            if let Some(base) = &base {
                checks.trust(base, &lifecycle.budget).await?;
            }
            let validation_dependencies =
                processing.dependencies.clone().map(|mut dependencies| {
                    if let Some(related) = &related {
                        dependencies.tables.push((&related.evidence).into());
                    }
                    if let Some(receipts) = &receipts {
                        dependencies.tables.push((&receipts.result.table).into());
                    }
                    dependencies
                });
            let validated = if let Some(validated) =
                self.phase(spec, &operation, "validation").await?
            {
                if validated.table != processing.table
                    || validated.outputs != processing.outputs
                    || validated.rows != processing.rows
                    || validated.changes != processing.changes
                    || validated.coverage != processing.coverage
                    || validated.dependencies != validation_dependencies
                {
                    return Err(self.phase_generation_conflict(spec, &operation, "validation")?);
                }
                validated
            } else {
                #[cfg(feature = "test-util")]
                work_started("validation");
                checks.table(&table, &lifecycle.budget).await?;
                verify_changes(
                    spec,
                    &table,
                    if evidence_only {
                        None
                    } else {
                        processing.changes.as_ref()
                    },
                    &lifecycle.budget,
                    &mut checks,
                )
                .await?;
                if let Some(receipts) = &receipts {
                    evidence::verify_prepared(
                        spec,
                        &table,
                        base.as_ref().ok_or_else(|| {
                            Error::refusal("invalid_evidence", "receipts require a captured base")
                        })?,
                        receipts,
                        &lifecycle.budget,
                        &mut checks,
                    )
                    .await?;
                }
                if let Some(related) = &related {
                    Box::pin(refresh::verify_related(
                        &table,
                        related,
                        &lifecycle.budget,
                        &mut checks,
                    ))
                    .await?;
                }
                let phase = PhaseResult {
                    phase: "validation".into(),
                    dependencies: validation_dependencies,
                    ..processing.value.clone()
                };
                let mut protections = processing
                    .protections
                    .as_ref()
                    .map(ReferenceProtections::duplicate)
                    .transpose()?;
                if let Some(related) = &related {
                    ReferenceProtections::inherit(&mut protections, related.protections.as_ref())?;
                }
                if let Some(receipts) = &receipts {
                    ReferenceProtections::inherit(&mut protections, receipts.protections.as_ref())?;
                }
                self.complete(spec, &operation, phase, protections).await?
            };
            stop_after(stop, "validation")?;
            self.prepare_changes(
                spec,
                &operation,
                base.as_ref(),
                if evidence_only {
                    None
                } else {
                    processing.changes.as_ref()
                },
                receipts
                    .as_ref()
                    .and_then(|receipts| receipts.changes.as_ref()),
            )
            .await?;
            let protection_view = lifecycle.open(spec.dataset()).await?;
            let mut readback_protections = self
                .request_protections(&protection_view, spec.operation())
                .await?;
            ReferenceProtections::inherit(
                &mut readback_protections,
                processing.protections.as_ref(),
            )?;
            ReferenceProtections::inherit(
                &mut readback_protections,
                validated.protections.as_ref(),
            )?;
            if let Some(related) = &related {
                ReferenceProtections::inherit(
                    &mut readback_protections,
                    related.protections.as_ref(),
                )?;
            }
            if let Some(receipts) = &receipts {
                ReferenceProtections::inherit(
                    &mut readback_protections,
                    receipts.protections.as_ref(),
                )?;
            }

            // A full rebuild declares a complete new input. Updates retain the prior
            // source evidence because they only replace a subset of its rows.
            let mut inputs = base
                .as_ref()
                .filter(|_| {
                    matches!(
                        spec,
                        OperationRequest::Update(_)
                            | OperationRequest::Schema(_)
                            | OperationRequest::Evidence(_)
                    )
                })
                .map(|base| base.dataset.inputs.clone())
                .unwrap_or_default();
            if let Some(dependencies) = &processing.dependencies {
                for (name, input) in &dependencies.admitted_inputs {
                    inputs.insert(spec.input_key(name), input.clone());
                }
            } else {
                // Earlier completions remain readable while cleanup is disabled.
                // Their inferred input edge is not sufficient removal authority.
                let admission = admission.as_ref().ok_or_else(|| {
                    Error::refusal(
                        "phase_dependencies_unknown",
                        "legacy processing lacks its original admission",
                    )
                })?;
                for ((name, _), input) in spec.inputs().iter().zip(&admission.outputs) {
                    inputs.insert(spec.input_key(name), input.clone());
                }
            }
            let mut groups = BTreeMap::from([("source".to_owned(), table.clone())]);
            let mut computed = BTreeMap::new();
            if evidence_only {
                let base = base
                    .as_ref()
                    .ok_or_else(|| Error::refusal("invalid_evidence", "receipts require a base"))?;
                groups = base.dataset.groups.clone();
                computed = base.dataset.computed.clone();
            } else if let Some(base) = &base {
                for (name, previous) in &base.dataset.computed {
                    self.check(
                        &lifecycle.open(spec.dataset()).await?,
                        &operation,
                        spec.operation(),
                    )
                    .await?;
                    let input_fields = backfill::inherited_inputs(
                        spec,
                        previous,
                        &base.dataset.groups["source"],
                        &table,
                    )
                    .await?;
                    let prepared = Box::pin(self.prepare_group(
                        &captured_request,
                        &operation,
                        &table,
                        readback_protections.as_ref(),
                        &previous.definition,
                        Some(&input_fields),
                        None,
                        &mut checks,
                    ))
                    .await?;
                    ReferenceProtections::inherit(
                        &mut readback_protections,
                        prepared.protections.as_ref(),
                    )?;
                    groups.insert(name.clone(), prepared.table);
                    computed.insert(
                        name.clone(),
                        ComputedGroup {
                            qualification: None,
                            origins: prepared.origins,
                            definition: previous.definition.clone(),
                            input_fields: prepared.input_fields,
                            request: captured_request.clone(),
                            native_field_ids: prepared.native_field_ids,
                            source_metadata: table.metadata.clone(),
                            base_manifest: base.manifest.clone(),
                        },
                    );
                    stop_after(stop, &format!("group/{name}"))?;
                }
                if !computed.is_empty() {
                    let _guard = lock.lock().await;
                    let view = lifecycle.open(spec.dataset()).await?;
                    self.check_completion(&view, &operation, spec.operation())
                        .await?;
                    let key = format!("groups/{}", spec.operation());
                    let completed: BTreeMap<_, _> = groups
                        .iter()
                        .filter(|(name, _)| name.as_str() != "source")
                        .map(|(name, pin)| (name.clone(), pin.metadata.clone()))
                        .collect();
                    let existing = self
                        .active_completion::<CompletedGroups>(&view, &key)
                        .await?;
                    if let Some(existing) = existing {
                        let matches = match existing {
                            CompletedGroups::Captured { outputs, source } => {
                                outputs == completed && source == (&table).into()
                            }
                            CompletedGroups::Legacy(outputs) => outputs == completed,
                        };
                        if !matches {
                            return Err(Error::refusal(
                                "phase_generation_conflict",
                                format!(
                                    "retained completion {key} belongs to another physical generation; explicitly retire it before recomputing its dependencies"
                                ),
                            ));
                        }
                        let protections = self.retained_protections(&view, &key).await?;
                        ReferenceProtections::inherit(
                            &mut readback_protections,
                            protections.as_ref(),
                        )?;
                    } else {
                        let records = completion_records(
                            spec.dataset(),
                            &view,
                            &key,
                            &CompletedGroups::Captured {
                                outputs: completed,
                                source: (&table).into(),
                            },
                            readback_protections.as_ref(),
                        )?;
                        lifecycle.commit(&view, records).await?;
                    }
                }
            }
            stop_after(stop, "groups")?;
            let mut dataset = DatasetPin {
                format: operation.result_format,
                dataset: spec.dataset().clone(),
                version: spec.operation().clone(),
                groups,
                computed,
                inputs,
                relationship: inputs::relationship(spec, base.as_ref().map(|base| &base.dataset)),
                related: related.map(|related| Box::new(related.value)),
                evidence: receipts.map(|receipts| Box::new(receipts.value.result)),
                definition: captured_request.result_definition().to_owned(),
                eligibility: operation.eligibility,
                boundary: operation.boundary,
                request: captured_request.clone(),
                parent_manifest: base.as_ref().map(|base| base.manifest.clone()),
                processor: PROCESSOR.into(),
                validation: VerificationLevel::Routine,
            };
            checks.compose();
            Box::pin(self.qualify_corrections(
                &captured_request,
                &operation,
                &mut dataset,
                &mut readback_protections,
                &mut checks,
            ))
            .await?;
            backfill::verify_groups(&dataset, &lifecycle.budget, &mut checks).await?;
            evidence::verify(&dataset, &lifecycle.budget, &mut checks).await?;
            // Readback has an operation-specific manifest because coverage can advance
            // while the same expensive materialized values remain reusable.
            let readback_key = format!("readback/{}", spec.operation());
            let readback_view = lifecycle.open(spec.dataset()).await?;
            let release = if let Some(release) = lifecycle
                .get::<ReleasePin>(&readback_view, &readback_key)
                .await?
            {
                if release.dataset != dataset {
                    return Err(Error::refusal(
                        "corrupt_phase",
                        "retained readback differs from the prepared result",
                    ));
                }
                check_release_available_with_checks(&release, &lifecycle.budget, &mut checks)
                    .await?;
                Retained {
                    value: release,
                    protections: self
                        .retained_protections(&readback_view, &readback_key)
                        .await?,
                }
            } else {
                #[cfg(feature = "test-util")]
                work_started("readback");
                let directory = self.reserve_attempt(spec, &operation, "release").await?;
                let path = directory.join("release.json");
                let serialized = serde_json::to_vec_pretty(&dataset)?;
                if serialized.len() > lifecycle.budget.limits.max_metadata_bytes {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "combined release exceeds metadata allowance",
                    ));
                }
                storage::write_new(&path, &serialized)?;
                let manifest = ObjectRef {
                    owner: spec.dataset().clone(),
                    path: path.to_string_lossy().into_owned(),
                    digest: blake3::hash(&serialized).to_hex().to_string(),
                    bytes: serialized.len() as u64,
                };
                // Decode the serialized release independently of the producing context.
                let memory = Arc::new(
                    lifecycle
                        .budget
                        .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
                );
                let decoded: DatasetPin =
                    inputs::read_manifest(&manifest, &lifecycle.budget, &mut 0, &memory).await?;
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
                // Prepared tables were validated at their durable phase boundaries.
                // Equality above verifies serialization without scanning them again.
                let _guard = lock.lock().await;
                let view = lifecycle.open(spec.dataset()).await?;
                self.check_completion(&view, &operation, spec.operation())
                    .await?;
                if let Some(completed) = lifecycle.get::<ReleasePin>(&view, &readback_key).await? {
                    Retained {
                        value: completed,
                        protections: self.retained_protections(&view, &readback_key).await?,
                    }
                } else {
                    let records = completion_records(
                        spec.dataset(),
                        &view,
                        &readback_key,
                        &release,
                        readback_protections.as_ref(),
                    )?;
                    lifecycle.commit(&view, records).await?;
                    Retained {
                        value: release,
                        protections: readback_protections,
                    }
                }
            };
            stop_after(stop, "readback")?;
            self.select_prepared(spec, &operation, release, stop).await
        })
    }

    fn select_prepared<'a>(
        &'a self,
        spec: &'a OperationRequest,
        operation: &'a Operation,
        release: Retained<ReleasePin>,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<ReleasePin>> {
        Box::pin(async move {
            if let OperationRequest::Refresh(refresh) = spec {
                let selected = self.select_refresh(refresh, operation, &release).await?;
                stop_after(stop, "selection")?;
                return Ok(selected);
            }
            let dependencies = if release.dataset.related.is_some() {
                Some(verification::available_graph(&release, &self.inner.lifecycle.budget).await?)
            } else {
                None
            };
            let datasets = dependencies.as_ref().map_or_else(
                || std::collections::BTreeSet::from([spec.dataset().clone()]),
                verification::DependencyGraph::datasets,
            );
            let _guards = self.lock_inputs(datasets).await;
            let lifecycle = &self.inner.lifecycle;
            let view = lifecycle.open(spec.dataset()).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation()))
                .await?
            {
                return Ok(completed);
            }
            let authority = self.check(&view, operation, spec.operation()).await?;
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
            check_group_rebuild(&authority, spec)?;
            self.check_incremental(&authority, spec).await?;
            if let Some(dependencies) = &dependencies {
                self.guard_ancestors(dependencies, &release).await?;
            }
            if matches!(spec, OperationRequest::Schema(_)) {
                self.check_cutover_inputs(&view, spec.dataset(), Some(spec.operation()))
                    .await?;
            }
            self.select(&view, authority, &release).await?;
            stop_after(stop, "selection")?;
            Ok(release.value)
        })
    }

    async fn check(
        &self,
        view: &crate::catalog::View,
        operation: &Operation,
        id: &Id,
    ) -> Result<Authority> {
        self.check_not_abandoned(view, id).await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        check_authority(&authority, operation)?;
        Ok(authority)
    }

    async fn check_completion(
        &self,
        view: &crate::catalog::View,
        operation: &Operation,
        id: &Id,
    ) -> Result<()> {
        self.check_not_abandoned(view, id).await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        // Finished work retains its original qualification. Changing the selected
        // source or eligibility fences reuse and promotion, not its acknowledgment.
        check_preparation(&authority, operation)
    }

    async fn phase(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &str,
    ) -> Result<Option<Retained<PhaseResult>>> {
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation, spec.operation()).await?;
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        let found: Option<PhaseResult> = self.active_completion(&view, &key).await?;
        if let Some(found) = found {
            for output in &found.outputs {
                if std::fs::metadata(&output.path)?.len() != output.bytes {
                    return Err(Error::refusal(
                        "unavailable_phase",
                        "retained phase output size changed",
                    ));
                }
            }
            tracing::info!(phase, operation = %spec.operation(), "reusing qualified phase");
            return Ok(Some(Retained {
                value: found,
                protections: self.retained_protections(&view, &key).await?,
            }));
        }
        Ok(None)
    }

    fn phase_generation_conflict(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &str,
    ) -> Result<Error> {
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        Ok(Error::refusal(
            "phase_generation_conflict",
            format!(
                "retained completion {key} belongs to another physical generation; explicitly retire it before recomputing its dependencies"
            ),
        ))
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
        self.check(&view, operation, spec.operation()).await?;
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
        phase: PhaseResult,
        protections: Option<ReferenceProtections>,
    ) -> Result<Retained<PhaseResult>> {
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check_completion(&view, operation, spec.operation())
            .await?;
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            &phase.phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        // Another attempt may have completed the same deterministic phase. Preserve
        // that generation rather than relabeling or replacing its evidence.
        if let Some(completed) = self.active_completion::<PhaseResult>(&view, &key).await? {
            if matches!(phase.phase.as_str(), "validation" | "evidence" | "receipts")
                && (completed.dependencies != phase.dependencies
                    || (phase.phase == "validation" && completed.table != phase.table))
            {
                return Err(self.phase_generation_conflict(spec, operation, &phase.phase)?);
            }
            return Ok(Retained {
                value: completed,
                protections: self.retained_protections(&view, &key).await?,
            });
        }
        let records =
            completion_records(spec.dataset(), &view, &key, &phase, protections.as_ref())?;
        self.inner.lifecycle.commit(&view, records).await?;
        tracing::info!(phase = phase.phase, rows = phase.rows, operation = %spec.operation(), "phase durably complete");
        Ok(Retained {
            value: phase,
            protections,
        })
    }
}

// These operations reuse the exact selected native combination as a material input.
fn captured_manifest<'a>(
    authority: &'a Authority,
    request: &OperationRequest,
) -> Option<&'a ObjectRef> {
    (matches!(request, OperationRequest::Schema(_) | OperationRequest::Evidence(_))
        || matches!(request, OperationRequest::Refresh(spec) if spec.mode == RefreshMode::Incremental))
        .then(|| authority.release.as_ref().map(|release| &release.manifest)).flatten()
}

fn phase_key(
    spec: &OperationRequest,
    request: &str,
    eligibility: u64,
    phase: &str,
    base_manifest: Option<&ObjectRef>,
    schema_key: Option<&String>,
) -> Result<String> {
    if phase == "receipts" {
        return Ok(format!("phase/{request}/{eligibility}/receipts"));
    }
    if let OperationRequest::Refresh(refresh) = spec {
        return refresh::phase_key(refresh, eligibility, phase, base_manifest);
    }
    if phase == "admission" && !matches!(spec, OperationRequest::Schema(_)) {
        let mut hash = blake3::Hasher::new();
        for (name, input) in spec.inputs() {
            hash.update(&(name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash.update(input.digest.as_bytes());
        }
        return Ok(format!("phase/input/{}/{phase}", hash.finalize().to_hex()));
    }
    if let OperationRequest::Build(build) = spec {
        // The selected composition determines admission and promotion, but does
        // not change source SQL results. A rebased rebuild reuses this exact work.
        let mut hash = blake3::Hasher::new();
        for value in [
            PROCESSOR,
            &build.expected_digest,
            &build.sql,
            &build.definition,
        ] {
            hash.update(&(value.len() as u64).to_le_bytes());
            hash.update(value.as_bytes());
        }
        if let Some(source) = schema_key {
            hash.update(b"native-omission-rules-v1");
            hash.update(source.as_bytes());
        }
        return Ok(format!(
            "phase/build/{}/{eligibility}/{phase}",
            hash.finalize().to_hex()
        ));
    }
    Ok(format!("phase/{request}/{eligibility}/{phase}"))
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
    check_base(authority, operation.base.as_ref())?;
    if operation.base_manifest.as_ref().is_some_and(|manifest| {
        authority.release.as_ref().map(|release| &release.manifest) != Some(manifest)
    }) {
        return Err(Error::refusal(
            "stale_base",
            "selected base manifest differs from the captured operation",
        ));
    }
    if operation.require_current_boundary
        && authority.boundary != operation.admission_fence.unwrap_or(operation.boundary)
    {
        return Err(Error::refusal(
            "stale_source_boundary",
            "source admission advanced after capture; start a new operation",
        ));
    }
    Ok(())
}
fn check_preparation(authority: &Authority, operation: &Operation) -> Result<()> {
    if authority.epoch != operation.epoch {
        return Err(Error::refusal(
            "stale_eligibility",
            "execution authority changed",
        ));
    }
    Ok(())
}

fn check_group_rebuild(authority: &Authority, request: &OperationRequest) -> Result<()> {
    inputs::check_rebuild(authority, request)?;
    refresh::check_rebuild(authority, request)?;
    if matches!(request, OperationRequest::Build(build) if build.base.is_none())
        && authority
            .selected
            .as_ref()
            .is_some_and(|selected| !selected.computed.is_empty())
    {
        return Err(Error::refusal(
            "dependent_groups",
            "provide the selected base to rebuild its computed groups together",
        ));
    }
    Ok(())
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
fn check_update_eligibility(authority: &Authority, request: &OperationRequest) -> Result<()> {
    if matches!(
        request,
        OperationRequest::Update(_) | OperationRequest::Schema(_) | OperationRequest::Evidence(_)
    ) && authority
        .selected
        .as_ref()
        .is_some_and(|base| base.eligibility != authority.eligibility)
    {
        return Err(Error::refusal(
            "stale_eligibility",
            "the selected source transformation requires requalification before incremental update",
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
            "the selected version changed; submit a new operation against its exact base",
        ));
    }
    Ok(())
}
fn fingerprint(spec: &OperationRequest) -> Result<String> {
    let material = match spec {
        OperationRequest::Build(spec) => {
            serde_json::json!({"build": {"input": spec.expected_digest, "sql": spec.sql, "definition": spec.definition, "base": spec.base, "relationship": spec.relationship}})
        }
        OperationRequest::Update(spec) => {
            serde_json::json!({"update": {"base": spec.base, "keys": spec.keys, "replacements": spec.replacements.digest, "deletions": spec.deletions.as_ref().map(|input| &input.digest), "definition": spec.definition}})
        }
        OperationRequest::Backfill(spec) => {
            serde_json::json!({"backfill": {"base": spec.base, "definition": spec.definition, "group": spec.group, "group_definition": spec.group_definition, "fields": spec.fields}})
        }
        OperationRequest::Refresh(spec) => {
            serde_json::json!({"refresh": {"base": spec.base, "definition": spec.definition, "transform": spec.transform, "policy": spec.policy, "mode": spec.mode, "relationship": spec.relationship}})
        }
        OperationRequest::Schema(spec) => {
            let mut value = serde_json::json!({"schema": {"base": spec.base, "definition": spec.definition,
                "next_definition": spec.next_definition, "fields": spec.fields}});
            if !spec.additions.is_empty() {
                value["schema"]["additions"] = serde_json::to_value(&spec.additions)?;
            }
            if !spec.write_defaults.is_empty() {
                value["schema"]["write_defaults"] = serde_json::to_value(&spec.write_defaults)?;
            }
            value
        }
        OperationRequest::Evidence(spec) => {
            serde_json::json!({"evidence": {"base": spec.base, "replacements": spec.replacements.digest, "deletions": spec.deletions.as_ref().map(|input| &input.digest), "definition": spec.definition}})
        }
    };
    Ok(storage::json_digest(&(PROCESSOR, material))?
        .to_hex()
        .to_string())
}
fn validate_request(spec: &OperationRequest) -> Result<()> {
    if let OperationRequest::Evidence(spec) = spec {
        return evidence::validate(spec);
    }
    if let OperationRequest::Schema(spec) = spec {
        return schema::validate(spec);
    }
    if let OperationRequest::Refresh(spec) = spec {
        return refresh::validate(spec);
    }
    if let OperationRequest::Backfill(spec) = spec {
        return backfill::validate(spec);
    }
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
    if spec.base.as_ref() == Some(&spec.operation) {
        return Err(Error::refusal(
            "invalid_request",
            "a rebuild requires a different base operation",
        ));
    }
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

// Test-only observations prove actual work entry, independently of retained
// records that deliberately preserve the first successful completion on retry.
#[cfg(feature = "test-util")]
fn work_started(phase: &str) {
    eprintln!("DOCSPEC_TEST_WORK {phase}");
}

/// Verifies exact retained objects and decodes a native result without its catalog.
///
/// The caller retains the files for the check; this does not acquire a host reader.
///
/// # Errors
/// Refuses changed bytes, invalid schemas, missing files, or row-count disagreement.
pub async fn verify_table(pin: &TablePin, budget: &Budget) -> Result<()> {
    #[cfg(feature = "test-util")]
    eprintln!("DOCSPEC_TEST_DEEP_TABLE");
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
/// The caller retains the files or relies on a published availability window.
/// A serialized pin does not extend that availability promise.
///
/// # Errors
/// Refuses modified or missing objects and inconsistent native metadata.
pub async fn verify_release(release: &ReleasePin, limits: Limits) -> Result<()> {
    verify_release_at(release, limits, VerificationLevel::Deep)
        .await
        .map(|_| ())
}

/// Checks a release at the requested scope, reporting the bounded sample.
///
/// Routine checks read metadata and at most three files' first eight rows across
/// the request. Deep checks also hash payloads and reconstruct all derived results.
/// The caller retains the files for the check; this does not acquire a host reader.
///
/// # Errors
/// Refuses inconsistent references, missing objects, failed checks or exhausted capacity.
pub async fn verify_release_at(
    release: &ReleasePin,
    limits: Limits,
    level: VerificationLevel,
) -> Result<VerificationReport> {
    let budget = Budget::new(limits)?;
    let mut checks = verification::Checks::new(level);
    verification::verify_all(&[release], &budget, &mut checks).await?;
    Ok(checks.report)
}

fn release_objects(release: &ReleasePin) -> Vec<ObjectRef> {
    let dataset = &release.dataset;
    let mut objects = vec![release.manifest.clone()];
    objects.extend(dataset.parent_manifest.iter().cloned());
    objects.extend(dataset.inputs.values().cloned());
    if let Some(related) = &dataset.related {
        objects.extend(
            related
                .coverage
                .inputs
                .values()
                .map(|version| version.manifest.clone()),
        );
        objects.extend(related.evidence.objects.iter().cloned());
    }
    objects.extend(
        dataset
            .groups
            .values()
            .flat_map(|pin| pin.objects.iter().cloned()),
    );
    objects.extend(
        dataset
            .computed
            .values()
            .map(|group| group.base_manifest.clone()),
    );
    objects.extend(
        dataset
            .computed
            .values()
            .filter_map(|group| group.origins.clone()),
    );
    if let Some(evidence) = &dataset.evidence {
        objects.push(evidence.source.metadata.clone());
        objects.extend(evidence.table.objects.iter().cloned());
    }
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    objects.dedup();
    objects
}

async fn check_release_available(release: &ReleasePin, budget: &Budget) -> Result<()> {
    check_release_available_with_checks(release, budget, &mut verification::Checks::composition())
        .await
}

async fn check_release_available_with_checks(
    release: &ReleasePin,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    if release.dataset.related.is_some() {
        verification::check_available(release, budget).await
    } else {
        storage::check_available(release_objects(release), budget).await?;
        schema::check_available(&release.dataset, budget).await?;
        backfill::verify_binding_dependencies(&release.dataset, budget, checks).await
    }
}

async fn resolve_release(release: &ReleasePin, budget: &Budget) -> Result<()> {
    // Resolve the immutable selection only. Payloads and dependencies are checked
    // at creation, or by explicit verification; a read opens its requested data.
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let decoded = refresh::read_release(&release.version_ref(), budget, &mut 0, &memory).await?;
    if decoded.dataset != release.dataset || !release.verified {
        return Err(Error::refusal(
            "release_mismatch",
            "release differs from its completed selection",
        ));
    }
    Ok(())
}

async fn verify_local_release(
    release: &ReleasePin,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    let decoded = &release.dataset;
    if !supported_format(decoded.format)
        || release.id != decoded.version
        || decoded.request.dataset() != &decoded.dataset
        || decoded.request.operation() != &decoded.version
        || decoded.request.result_definition() != decoded.definition
        || decoded.request.inputs().iter().any(|(name, input)| {
            let key = decoded.request.input_key(name);
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
    if checks.deep() {
        storage::verify_objects(decoded.inputs.values().cloned().collect(), budget).await?;
    } else if checks.inspects_tables() {
        storage::check_available(decoded.inputs.values().cloned().collect(), budget).await?;
    }
    let source = decoded
        .groups
        .get("source")
        .ok_or_else(|| Error::refusal("release_mismatch", "release has no source table"))?;
    checks.table(source, budget).await?;
    backfill::verify_groups(decoded, budget, checks).await?;
    evidence::verify(decoded, budget, checks).await?;
    Ok(())
}
