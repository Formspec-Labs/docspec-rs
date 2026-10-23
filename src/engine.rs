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
mod aggregation;
mod authority;
mod backfill;
#[doc(inline)]
pub use backfill::{BackfillPlan, read_combined};
mod changes;
#[doc(inline)]
pub use changes::verify_applied_changes;
mod compaction;
mod comparison;
mod connected_components;
mod corrections;
mod evidence;
#[doc(inline)]
pub use evidence::read_receipts;
pub mod export;
mod expr;
mod history;
mod incremental;
mod inputs;
#[doc(inline)]
pub use inputs::verify_input_coverage;
mod intake;
mod key_pruning;
pub(crate) mod locations;
mod pipeline;
mod planner;
#[cfg(test)]
pub(in crate::engine) use planner::material_phase_key;
pub use planner::{PlanPhaseReason, RunPlan};
pub(in crate::engine) use planner::{
    check_definition, check_preparation, conflicting_operation, fingerprint, phase_key,
};
#[cfg(all(test, feature = "test-util"))]
mod profile_tests;
mod read;
#[doc(inline)]
pub use read::{read_compaction, read_evidence, read_group, read_keys};
mod refresh;
mod release_check;
pub(in crate::engine) use release_check::{
    check_release_available_with_checks, release_objects, resolve_release,
    resolve_release_with_objects, verify_local_release,
};
pub use release_check::{verify_release, verify_release_at, verify_table};
#[cfg(all(test, feature = "test-util", feature = "remote"))]
mod remote_build_provider_tests;
#[cfg(all(test, feature = "remote"))]
mod remote_query_tests;
#[cfg(all(test, feature = "remote"))]
mod remote_startup_tests;
mod restore;
pub(crate) mod retention;
use retention::owners::ReferenceProtections;
mod schema;
mod sql_validation;
#[doc(inline)]
pub use sql_validation::is_identity_source_projection;
mod test_hooks;
pub(in crate::engine) use test_hooks::stop_after;
#[cfg(feature = "test-util")]
pub(in crate::engine) use test_hooks::{work_started, work_started_async};
mod validation;
use validation::ValidatedReadback;
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
    retained: locations::Location,
    lifecycle: Lifecycle,
    objects: storage::Objects,
    locks: Mutex<BTreeMap<Id, Arc<Mutex<()>>>>,
    host: Arc<worker::Host>,
    maintenance: Arc<RwLock<()>>,
    closing: AtomicBool,
    owned_catalog: bool,
    #[cfg(feature = "remote")]
    remote_maintenance: Option<uuid::Uuid>,
    test_hooks: test_hooks::TestHooks,
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
    #[cfg(all(not(test), not(feature = "test-util")))]
    freshly_processed: bool,
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
    /// Material native schema/history when a rebuild cannot reuse its exact source.
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

const PHASES: [&str; 4] = ["admission", "processing", "validation", "readback"];
// Bump for material computation/interpretation changes; routine-only checker
// changes use validation::VALIDATOR so qualified value work stays reusable.
const PROCESSOR: &str = "docspec.dataset.v10";
// Format 12 makes explicitly based Builds preserve exact native schema history.
pub(crate) const FORMAT: u32 = 12;
const PREVIOUS_FORMAT: u32 = 10;

fn previous_format() -> u32 {
    PREVIOUS_FORMAT
}

fn supported_format(format: u32) -> bool {
    // Format 11 changed schema coverage; its retained results remain readable.
    matches!(format, PREVIOUS_FORMAT | 11 | FORMAT)
}

/// Backend capability classes decided by one execution-host authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendCapability {
    /// Exact source-group reads, independent Build, and source-only Update.
    SourceGroup,
    /// Computed/related/evidence groups, receipts, schema, backfill, capture and corrections.
    LocalOnly,
}

impl BackendCapability {
    /// Returns the capability required to read one retained field group.
    pub fn for_group(group: &str) -> Self {
        if group == "source" {
            Self::SourceGroup
        } else {
            Self::LocalOnly
        }
    }

    /// Returns whether this capability requires a local workspace.
    pub fn local_only(self) -> bool {
        matches!(self, Self::LocalOnly)
    }
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
                    Self::compose(
                        root,
                        catalog,
                        storage::Objects::Local,
                        limits,
                        host,
                        true,
                        #[cfg(feature = "remote")]
                        None,
                    )
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
            storage::Objects::Local,
            limits,
            ownership,
            false,
            #[cfg(feature = "remote")]
            None,
        )
    }

    /// Opens a local execution host with explicitly configured remote storage.
    ///
    /// The host retains exclusive custody, bounded scratch and execution admission.
    /// The supplied catalog owns lifecycle placement; the supplied object prefix
    /// owns immutable admitted inputs, native candidates and releases. Supports an
    /// initial independent Build (`RunSpec::base` is `None`), source-only updates,
    /// export and selected source-group reads. A completed retry returns its original
    /// result; another Build after selection and updates involving computed groups,
    /// related inputs or receipts refuse before input transfer or processing.
    /// An unacknowledged update restarts in a fresh protected attempt; its partial
    /// objects remain retained. Remote retirement and removal remain unavailable.
    /// CLI and HTTP configuration remain separate from this library constructor.
    /// No ambient credentials are resolved here; use `remote_storage` at startup.
    ///
    /// # Errors
    /// Refuses a live host owner, invalid limits, object-store registration failure,
    /// or local host initialization failure. Catalog connectivity is checked on use.
    #[cfg(feature = "remote")]
    pub async fn with_remote(
        root: impl AsRef<Path>,
        remote: storage::remote::RemoteStorage,
        limits: Limits,
    ) -> Result<Self> {
        let ownership = worker::Host::acquire(root.as_ref())?;
        Self::compose(
            std::fs::canonicalize(root)?,
            remote.catalog,
            storage::Objects::Remote(remote.objects),
            limits,
            ownership,
            false,
            remote.maintenance_generation,
        )
    }

    fn require_data_work(&self) -> Result<()> {
        #[cfg(feature = "remote")]
        if self.inner.remote_maintenance.is_some() {
            return Err(Error::refusal(
                "maintenance_only",
                "quiescent maintenance cannot start data operations",
            ));
        }
        Ok(())
    }

    fn maintenance_catalog(&self) -> bool {
        if self.inner.owned_catalog {
            return true;
        }
        #[cfg(feature = "remote")]
        {
            self.inner.remote_maintenance.is_some()
        }
        #[cfg(not(feature = "remote"))]
        false
    }

    #[cfg(feature = "remote")]
    fn remote_maintenance(&self) -> Result<uuid::Uuid> {
        self.inner.remote_maintenance.ok_or_else(|| {
            Error::refusal(
                "retention_catalog_unqualified",
                "remote maintenance requires an owned generation and explicit quiescence",
            )
        })
    }

    fn retained_owner(&self, location: &str) -> Result<Id> {
        let relative = self
            .inner
            .retained
            .relative(location)
            .ok_or_else(locations::invalid)?;
        let (owner, remainder) = relative.split_once('/').ok_or_else(locations::invalid)?;
        if remainder.is_empty() {
            return Err(locations::invalid());
        }
        Id::new(owner)
    }

    fn require_local(&self) -> Result<()> {
        self.require_capability(BackendCapability::LocalOnly)
    }

    /// Returns whether this host's configured backend supports one capability class.
    ///
    /// This is the single authority for backend capability decisions. CLI, HTTP and
    /// library entry points must route their checks here rather than testing object
    /// storage or field-group names independently.
    pub(crate) fn supports_capability(&self, capability: BackendCapability) -> bool {
        self.inner.retained.is_local() || matches!(capability, BackendCapability::SourceGroup)
    }

    pub(crate) fn require_capability(&self, capability: BackendCapability) -> Result<()> {
        if self.supports_capability(capability) {
            Ok(())
        } else {
            Err(locations::unsupported())
        }
    }

    /// Refuses a non-source field group unless the backend supports local work.
    pub(crate) fn require_group(&self, group: &str) -> Result<()> {
        self.require_capability(BackendCapability::for_group(group))
    }

    fn check_backend_selection(
        &self,
        authority: &Authority,
        request: &OperationRequest,
    ) -> Result<()> {
        if self.inner.retained.is_local() {
            return Ok(());
        }
        match request {
            OperationRequest::Build(_) if authority.selected.is_none() => Ok(()),
            OperationRequest::Update(_)
            | OperationRequest::Refresh(_)
            | OperationRequest::Evidence(_)
            | OperationRequest::Backfill(_)
                if authority
                    .selected
                    .as_ref()
                    .is_none_or(|selected| selected.groups.contains_key("source")) =>
            {
                Ok(())
            }
            _ => Err(locations::unsupported()),
        }
    }

    async fn request_view(&self, request: &OperationRequest) -> Result<View> {
        // Update and Evidence require an existing selected dataset; a remote refusal
        // must not create a namespace or native lifecycle table for an unknown name.
        if matches!(
            request,
            OperationRequest::Update(_) | OperationRequest::Evidence(_)
        ) {
            self.read_view(request.dataset()).await
        } else {
            self.inner.lifecycle.open(request.dataset()).await
        }
    }

    fn check_request_backend(&self, request: &OperationRequest) -> Result<()> {
        self.require_data_work()?;
        if matches!(
            request,
            OperationRequest::Update(_)
                | OperationRequest::Refresh(_)
                | OperationRequest::Evidence(_)
                | OperationRequest::Backfill(_)
        ) || matches!(request, OperationRequest::Build(build) if build.base.is_none())
        {
            Ok(())
        } else {
            self.require_local()
        }
    }

    fn compose(
        root: PathBuf,
        catalog: Arc<dyn Catalog>,
        objects: storage::Objects,
        limits: Limits,
        ownership: Arc<worker::Host>,
        owned_catalog: bool,
        #[cfg(feature = "remote")] remote_maintenance: Option<uuid::Uuid>,
    ) -> Result<Self> {
        let retained = locations::Location::root(&objects, &root);
        let scratch = root.join("scratch");
        std::fs::create_dir_all(&scratch)?;
        let budget = Budget::in_workspace(limits, &scratch)?;
        objects.register_query_store(budget.runtime_env())?;
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                retained,
                lifecycle: Lifecycle::new(catalog, budget)
                    .with_retained_prefix(objects.retained_prefix()),
                objects,
                locks: Mutex::new(BTreeMap::new()),
                host: ownership,
                maintenance: Arc::new(RwLock::new(())),
                closing: AtomicBool::new(false),
                owned_catalog,
                #[cfg(feature = "remote")]
                remote_maintenance,
                test_hooks: test_hooks::TestHooks::default(),
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

    pub fn limits(&self) -> &Limits {
        &self.inner.lifecycle.budget.limits
    }

    fn context(&self) -> SessionContext {
        self.inner.lifecycle.budget.context()
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

    async fn read_view(&self, dataset: &Id) -> Result<View> {
        if self.inner.retained.is_local() {
            self.inner.lifecycle.open(dataset).await
        } else {
            self.existing_lifecycle_view(dataset).await
        }
    }

    async fn existing_lifecycle_view(&self, dataset: &Id) -> Result<View> {
        #[cfg(all(not(test), not(feature = "test-util")))]
        if let Some(cached) = self.inner.lifecycle.cached_view(dataset) {
            return Ok(cached);
        }
        let table = self
            .inner
            .lifecycle
            .catalog
            .load_table(&crate::catalog::lifecycle_ident(dataset))
            .await
            .map_err(|error| {
                if error.kind() == iceberg::ErrorKind::TableNotFound {
                    Error::refusal("no_release", "this host has no retained dataset")
                } else {
                    error.into()
                }
            })?;
        let view = View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: crate::catalog::lifecycle_ident(dataset),
        };
        #[cfg(all(not(test), not(feature = "test-util")))]
        self.inner.lifecycle.set_cached_view(view.clone());
        Ok(view)
    }

    async fn inspect_admitted(&self, dataset: &Id) -> Result<BTreeMap<String, serde_json::Value>> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.read_view(dataset).await?;
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
        let view = self.read_view(dataset).await?;
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
        let view = self.read_view(dataset).await?;
        self.resolve_release(&view, dataset, version).await
    }

    async fn available_release_admitted(&self, dataset: &Id, version: &Id) -> Result<ReleasePin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.read_view(dataset).await?;
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
        self.require_local()?;
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
}
