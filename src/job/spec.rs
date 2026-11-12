//! Declarative job execution uses the embedded SQL service directly.
// Rust guideline compliant 2026-02-21
#[cfg(feature = "substrait")]
use super::substrait::SavedPlan;
use super::{
    ComponentsSettings, ExportSettings, ExportedFile, Job, JsonLinesFile, StepStorage,
    TableOperation, TableOperationResult,
};
use crate::{
    Engine, Error, Result,
    budget::Limits,
    model::{TablePin, VersionRef},
};
use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Explicit data source, including immutable native table pins.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobInput {
    Parquet {
        name: String,
        path: PathBuf,
    },
    JsonLines {
        name: String,
        files: Vec<JsonLinesFile>,
        schema: Schema,
    },
    NativeTable {
        name: String,
        metadata_location: String,
        table_uuid: uuid::Uuid,
        snapshot: Option<i64>,
        schema_id: i32,
    },
    Iceberg {
        name: String,
        pin: TablePin,
    },
    /// A complete protected source-only version in a local DocSpec workspace.
    Version {
        name: String,
        workspace: PathBuf,
        version: VersionRef,
    },
}
impl JobInput {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Parquet { name, .. }
            | Self::JsonLines { name, .. }
            | Self::Iceberg { name, .. }
            | Self::Version { name, .. }
            | Self::NativeTable { name, .. } => name,
        }
    }
}

/// One reusable view or materialized relation, evaluated after earlier steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStep {
    pub name: String,
    pub sql: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub materialize: bool,
    #[serde(default)]
    pub release_after: Vec<String>,
}

/// One materialized component result, with the same named-step lifetime rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentsStep {
    pub name: String,
    pub components: ComponentsSettings,
    #[serde(default)]
    pub release_after: Vec<String>,
}

/// One named relation decoded from a content-addressed Substrait file.
#[cfg(feature = "substrait")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubstraitStep {
    pub name: String,
    pub substrait: SavedPlan,
    #[serde(default)]
    pub release_after: Vec<String>,
}

/// Distinct step shapes prevent mixing SQL-view and graph options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JobStep {
    Sql(SqlStep),
    #[cfg(feature = "substrait")]
    Substrait(SubstraitStep),
    Components(ComponentsStep),
}
impl JobStep {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Sql(step) => &step.name,
            #[cfg(feature = "substrait")]
            Self::Substrait(step) => &step.name,
            Self::Components(step) => &step.name,
        }
    }
    pub(crate) fn release_after(&self) -> &[String] {
        match self {
            Self::Sql(step) => &step.release_after,
            #[cfg(feature = "substrait")]
            Self::Substrait(step) => &step.release_after,
            Self::Components(step) => &step.release_after,
        }
    }
    pub(crate) fn step_result(&self, storage: StepStorage) -> Result<StepResult> {
        let (sql_sha256, parameters_sha256, materialize) = match self {
            Self::Sql(step) => {
                let params = if step.parameters.is_empty() {
                    None
                } else {
                    Some(sha256_hex(&serde_json::to_vec(&step.parameters)?))
                };
                (
                    Some(sha256_hex(step.sql.as_bytes())),
                    params,
                    step.materialize,
                )
            }
            #[cfg(feature = "substrait")]
            Self::Substrait(_) => (None, None, false),
            Self::Components(step) => {
                (Some(sha256_hex(step.components.sql.as_bytes())), None, true)
            }
        };
        Ok(StepResult {
            name: self.name().to_owned(),
            storage,
            sql_sha256,
            parameters_sha256,
            materialize,
        })
    }
}

/// One ordered output, separate from the relations it reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobOutput {
    pub name: String,
    pub path: PathBuf,
    pub sql: String,
    pub settings: ExportSettings,
}

/// One declarative SQL job; limits are supplied by the execution host.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub scratch: PathBuf,
    #[serde(default)]
    pub catalog_root: Option<PathBuf>,
    #[serde(default)]
    pub table_operations: Vec<TableOperation>,
    pub inputs: Vec<JobInput>,
    pub steps: Vec<JobStep>,
    pub outputs: Vec<JobOutput>,
    #[serde(default)]
    pub queries: Vec<SqlQuery>,
}

/// A small SQL metadata result serialized by the engine for its caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQuery {
    pub name: String,
    pub sql: String,
}

/// Exact observed local file member for a file-backed job input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputFileResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub bytes: u64,
}

/// Observed version or file-digest binding for one declared job input.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputResult {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<InputFileResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<VersionRef>,
}

/// Storage and definition digest summary for one named relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub name: String,
    pub storage: StepStorage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters_sha256: Option<String>,
    #[serde(default)]
    pub materialize: bool,
}

/// An exported file's declared name and computed content identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputResult {
    pub name: String,
    #[serde(flatten)]
    pub file: ExportedFile,
}

/// Machine-readable local results; no clock or process identity enters them.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub job_spec_sha256: String,
    /// Summary facts from exact native inputs; unavailable observations remain null.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_inputs: Vec<super::NativeInputObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_versions: Vec<super::NativeVersionObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_operations: Vec<TableOperationResult>,
    #[cfg(feature = "substrait")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub saved_plans: Vec<super::substrait::SubstraitReceipt>,
    #[serde(default)]
    pub inputs: Vec<InputResult>,
    pub steps: Vec<StepResult>,
    pub outputs: Vec<OutputResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries: Vec<super::QueryResult>,
}

/// Failed work with identities of every output already committed by this run.
#[derive(Debug)]
pub struct JobFailure {
    pub completed: JobResult,
    pub error: Error,
}
impl std::fmt::Display for JobFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}
impl std::error::Error for JobFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
impl JobFailure {
    /// Returns a machine-readable failure while preserving completed identities.
    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({"status":"failed","completed":self.completed,"error":{"code":self.error.code(),"message":self.error.message()}})
    }
}

#[derive(Default)]
struct Connections {
    job_spec_sha256: Option<String>,
    catalog: Option<Arc<dyn iceberg::Catalog>>,
    #[cfg(feature = "remote")]
    remote: Option<crate::storage::remote::RemoteStorage>,
}

#[allow(clippy::result_large_err)]
impl JobSpec {
    /// Executes every declared relation and export through one bounded SQL job.
    ///
    /// # Errors
    /// Refuses duplicate names/paths before I/O and propagates input, SQL,
    /// resource and export failures. Completed outputs remain create-only.
    pub async fn run(&self, limits: Limits) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(limits, Connections::default()).await
    }

    /// Executes using the exact SHA-256 hex digest of the raw `JobSpec` JSON bytes.
    ///
    /// # Errors
    /// Same validation, input, SQL, resource and export errors as [`Self::run`].
    pub async fn run_with_spec_sha256(
        &self,
        limits: Limits,
        job_spec_sha256: String,
    ) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(
            limits,
            Connections {
                job_spec_sha256: Some(job_spec_sha256),
                ..Default::default()
            },
        )
        .await
    }

    /// Executes with an explicitly injected native catalog and no lifecycle store.
    ///
    /// # Errors
    /// Refuses conflicting local catalog selection or the same job failures as `run`.
    pub async fn run_with_catalog(
        &self,
        limits: Limits,
        catalog: Arc<dyn iceberg::Catalog>,
    ) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(
            limits,
            Connections {
                catalog: Some(catalog),
                ..Default::default()
            },
        )
        .await
    }

    /// Runs the same job with explicit remote catalog and object connections.
    ///
    /// # Errors
    /// Refuses conflicting catalog selection and the same errors as `run`.
    #[cfg(feature = "remote")]
    pub async fn run_with_remote(
        &self,
        limits: Limits,
        remote: crate::storage::remote::RemoteStorage,
    ) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(
            limits,
            Connections {
                job_spec_sha256: None,
                catalog: Some(remote.catalog.clone()),
                remote: Some(remote),
            },
        )
        .await
    }

    /// Runs with explicit remote connections and the raw `JobSpec` JSON SHA-256 digest.
    ///
    /// # Errors
    /// Refuses conflicting catalog selection and the same errors as `run`.
    #[cfg(feature = "remote")]
    pub async fn run_with_remote_and_spec_sha256(
        &self,
        limits: Limits,
        remote: crate::storage::remote::RemoteStorage,
        job_spec_sha256: String,
    ) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(
            limits,
            Connections {
                job_spec_sha256: Some(job_spec_sha256),
                catalog: Some(remote.catalog.clone()),
                remote: Some(remote),
            },
        )
        .await
    }

    async fn run_injected(
        &self,
        limits: Limits,
        mut connections: Connections,
    ) -> std::result::Result<JobResult, JobFailure> {
        let job_spec_sha256 = connections.job_spec_sha256.take().unwrap_or_else(|| {
            serde_json::to_vec(self).map_or_else(|_| String::new(), |bytes| sha256_hex(&bytes))
        });
        let mut completed = JobResult {
            job_id: self.id.clone(),
            job_spec_sha256,
            native_inputs: Vec::new(),
            native_versions: Vec::new(),
            table_operations: Vec::new(),
            #[cfg(feature = "substrait")]
            saved_plans: Vec::new(),
            inputs: Vec::new(),
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
        };
        let mut running = None;
        let mut failure_budget = None;
        match self
            .execute(
                limits,
                &mut completed,
                connections,
                &mut running,
                &mut failure_budget,
            )
            .await
        {
            Ok(()) => Ok(completed),
            Err(raw_error) => {
                let error = self.named(running, raw_error);
                let (kind, index, name) = self.running_parts(running);
                super::metrics::log_failed(
                    kind,
                    index,
                    name,
                    error.code(),
                    error.message(),
                    failure_budget,
                );
                Err(JobFailure { completed, error })
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.id.as_deref().is_some_and(|id| id.trim().is_empty()) {
            return Err(Error::refusal("invalid_spec", "job id must not be empty"));
        }
        let mut names = BTreeSet::new();
        for name in self
            .inputs
            .iter()
            .map(JobInput::name)
            .chain(self.steps.iter().map(JobStep::name))
        {
            if name.is_empty() || !names.insert(name.to_lowercase()) {
                return Err(Error::refusal(
                    "invalid_spec",
                    "duplicate or empty input/step name",
                ));
            }
        }
        TableOperation::validate_all(&self.table_operations, names)?;
        let mut names = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for output in &self.outputs {
            if output.name.is_empty() || !names.insert(&output.name) || !paths.insert(&output.path)
            {
                return Err(Error::refusal(
                    "invalid_spec",
                    "duplicate or empty output name/path",
                ));
            }
        }
        let mut query_names = BTreeSet::new();
        for query in &self.queries {
            if query.name.is_empty() || !query_names.insert(&query.name) {
                return Err(Error::refusal(
                    "invalid_spec",
                    "duplicate or empty SQL result name",
                ));
            }
        }
        let mut available: BTreeSet<String> = self
            .inputs
            .iter()
            .map(|input| input.name().to_lowercase())
            .collect();
        for step in &self.steps {
            available.insert(step.name().to_lowercase());
            for name in step.release_after() {
                if name.eq_ignore_ascii_case(step.name()) || !available.remove(&name.to_lowercase())
                {
                    return Err(Error::refusal(
                        "invalid_spec",
                        "release names must exist once before their successor",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        limits: Limits,
        result: &mut JobResult,
        connections: Connections,
        running: &mut Option<Running>,
        failure_budget: &mut Option<(usize, usize, usize)>,
    ) -> Result<()> {
        self.validate()?;
        if connections.catalog.is_some() && self.catalog_root.is_some() {
            return Err(Error::refusal(
                "invalid_spec",
                "injected and local catalog authority cannot be combined",
            ));
        }
        if !self.table_operations.is_empty()
            && connections.catalog.is_none()
            && self.catalog_root.is_none()
        {
            return Err(Error::refusal(
                "invalid_spec",
                "table operations require an explicit catalog",
            ));
        }
        #[cfg(feature = "remote")]
        let job = if let Some(remote) = connections.remote {
            Job::with_remote(limits, self.scratch.clone(), remote)?
        } else {
            Job::new(limits, self.scratch.clone())?
        };
        #[cfg(not(feature = "remote"))]
        let job = Job::new(limits, self.scratch.clone())?;
        super::metrics::log_start(&job.budget.limits, &self.scratch);
        let outcome = self
            .execute_with_job(&job, result, connections.catalog, running)
            .await;
        if outcome.is_err() {
            *failure_budget = Some((
                job.budget.runtime_env().memory_pool.reserved(),
                job.budget.pool_peak(),
                job.budget.shared_available_min(),
            ));
        }
        outcome
    }

    async fn execute_with_job(
        &self,
        job: &Job,
        result: &mut JobResult,
        injected_catalog: Option<Arc<dyn iceberg::Catalog>>,
        running: &mut Option<Running>,
    ) -> Result<()> {
        // Hold each producing host's exclusive protection through all output scans.
        let mut native_hosts = BTreeMap::new();
        let compactions = super::metrics::Compactions::install(&job.context);
        let compute_digests = self.id.is_some();
        for input in &self.inputs {
            match input {
                JobInput::Parquet { name, path } => {
                    job.register_parquet(name, path).await?;
                    let name_owned = name.clone();
                    let path_owned = path.clone();
                    let hash_budget = job.budget.clone();
                    let observed = job
                        .budget
                        .blocking_admitted_queued(move || {
                            observe_parquet_input(
                                &name_owned,
                                &path_owned,
                                &hash_budget,
                                compute_digests,
                            )
                        })
                        .await?;
                    result.inputs.push(observed);
                }
                JobInput::JsonLines {
                    name,
                    files,
                    schema,
                } => {
                    job.register_json_lines(name, files.clone(), Arc::new(schema.clone()))?;
                    let name_owned = name.clone();
                    let files_owned = files.clone();
                    let hash_budget = job.budget.clone();
                    let observed = job
                        .budget
                        .blocking_admitted_queued(move || {
                            observe_json_lines_input(
                                &name_owned,
                                &files_owned,
                                &hash_budget,
                                compute_digests,
                            )
                        })
                        .await?;
                    result.inputs.push(observed);
                }
                JobInput::NativeTable {
                    name,
                    metadata_location,
                    table_uuid,
                    snapshot,
                    schema_id,
                } => {
                    job.register_native_snapshot(
                        name,
                        &crate::storage::NativeTableSnapshot {
                            metadata_location: metadata_location.clone(),
                            table_uuid: *table_uuid,
                            snapshot: *snapshot,
                            schema_id: *schema_id,
                        },
                    )
                    .await?;
                    let location_owned = PathBuf::from(metadata_location);
                    let hash_budget = job.budget.clone();
                    let (sha256, bytes) = job
                        .budget
                        .blocking_admitted_queued(move || {
                            observe_optional_local_file(
                                &location_owned,
                                &hash_budget,
                                compute_digests,
                            )
                        })
                        .await?;
                    result.inputs.push(InputResult {
                        name: name.clone(),
                        kind: "native_table".into(),
                        uri: Some(metadata_location.clone()),
                        sha256,
                        bytes,
                        table_uuid: Some(*table_uuid),
                        snapshot_id: *snapshot,
                        schema_id: Some(*schema_id),
                        ..Default::default()
                    });
                }
                JobInput::Iceberg { name, pin } => {
                    job.register_iceberg(name, pin).await?;
                    let location_owned = PathBuf::from(&pin.metadata.path);
                    let hash_budget = job.budget.clone();
                    let (sha256, local_bytes) = job
                        .budget
                        .blocking_admitted_queued(move || {
                            observe_optional_local_file(
                                &location_owned,
                                &hash_budget,
                                compute_digests,
                            )
                        })
                        .await?;
                    result.inputs.push(InputResult {
                        name: name.clone(),
                        kind: "iceberg".into(),
                        uri: Some(pin.metadata.path.clone()),
                        sha256,
                        bytes: Some(local_bytes.unwrap_or(pin.metadata.bytes)),
                        snapshot_id: pin.snapshot,
                        schema_id: Some(pin.schema_id),
                        metadata_digest: Some(pin.metadata.digest.clone()),
                        ..Default::default()
                    });
                }
                JobInput::Version {
                    name,
                    workspace,
                    version,
                } => {
                    let root = std::fs::canonicalize(workspace)?;
                    let host = match native_hosts.entry(root) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            let host = Engine::open(entry.key(), job.budget.limits.clone()).await?;
                            entry.insert(host)
                        }
                    };
                    let source = host.protected_source_table(version, &job.budget).await?;
                    let physical_snapshot = super::NativeVersionSnapshotObservation::from_table(
                        &source.table,
                        source.metadata_sha256,
                        source.metadata_size,
                        &job.budget,
                    )?;
                    let observation =
                        super::NativeInputObservation::from_table(name, &source.table);
                    let provider =
                        crate::query::table(source.table, job.budget.clone(), Some(source.rows))?;
                    let schema = provider.schema().as_ref().clone();
                    job.context.register_table(
                        name.as_str(),
                        Arc::new(super::native::AdmittedTable {
                            provider,
                            memory: Arc::new(source.memory),
                            observation,
                        }),
                    )?;
                    result.inputs.push(InputResult {
                        name: name.clone(),
                        kind: "version".into(),
                        uri: Some(physical_snapshot.selector.metadata_location.clone()),
                        sha256: Some(physical_snapshot.metadata_sha256.clone()),
                        bytes: Some(source.metadata_size),
                        table_uuid: Some(physical_snapshot.selector.table_uuid),
                        snapshot_id: physical_snapshot.selector.snapshot,
                        schema_id: Some(source.schema_id),
                        workspace: Some(workspace.clone()),
                        version: Some(version.clone()),
                        ..Default::default()
                    });
                    result
                        .native_versions
                        .push(super::NativeVersionObservation {
                            name: name.clone(),
                            version: version.clone(),
                            schema_id: source.schema_id,
                            schema,
                            physical_snapshot: Some(physical_snapshot),
                        });
                }
            }
            if matches!(
                input,
                JobInput::NativeTable { .. } | JobInput::Iceberg { .. }
            ) {
                result
                    .native_inputs
                    .push(job.native_input_observation(input.name()).await?);
            }
        }
        #[cfg(feature = "substrait")]
        let mut saved_plan_admission = Vec::new();
        for (step_index, step) in self.steps.iter().enumerate() {
            *running = Some(Running::Step(step_index));
            let start_epoch = super::metrics::now_epoch_secs();
            let start_instant = std::time::Instant::now();
            let pool_before = job.budget.runtime_env().memory_pool.reserved();
            job.budget.restart_pool_peak();
            job.budget.restart_shared_available_min();
            compactions.take();
            let (storage, rows, charged_bytes, joins) = match step {
                JobStep::Sql(step) if step.materialize && step.parameters.is_empty() => {
                    job.materialize_with_joins(&step.name, &step.sql).await?
                }
                JobStep::Sql(step) => {
                    if step.materialize {
                        return Err(Error::refusal(
                            "unsupported_parameters",
                            "parameters are supported for view steps",
                        ));
                    }
                    let frame = job.sql_bound(&step.sql, &step.parameters).await?;
                    job.register_frame(&step.name, frame)?;
                    (StepStorage::View, 0, 0, Vec::new())
                }
                #[cfg(feature = "substrait")]
                JobStep::Substrait(step) => {
                    let (frame, receipt, memory) = job.read_substrait(&step.substrait).await?;
                    job.register_frame(&step.name, frame)?;
                    result.saved_plans.push(receipt);
                    saved_plan_admission.push(memory);
                    (StepStorage::View, 0, 0, Vec::new())
                }
                JobStep::Components(step) => {
                    let (storage, rows, bytes) = job
                        .connected_components_detailed(&step.name, &step.components)
                        .await?;
                    (storage, rows, bytes, Vec::new())
                }
            };
            let pool_after = job.budget.runtime_env().memory_pool.reserved();
            let end_epoch = super::metrics::now_epoch_secs();
            let duration_seconds = start_instant.elapsed().as_secs_f64();
            super::metrics::log_step(
                step_index,
                step.name(),
                storage.label(),
                rows,
                charged_bytes,
                start_epoch,
                end_epoch,
                duration_seconds,
                pool_before,
                pool_after,
                job.budget.pool_peak(),
                job.budget.shared_available_min(),
                compactions.take(),
                super::metrics::process_peak_rss_bytes(),
                &joins,
            );
            for name in step.release_after() {
                job.release_for_step(step_index, step.name(), name)?;
            }
            result.steps.push(step.step_result(storage)?);
        }
        *running = None;
        let catalog = if !self.table_operations.is_empty() {
            match injected_catalog {
                Some(catalog) => Some(catalog),
                None => {
                    let root = self.catalog_root.as_ref().expect("validated local catalog");
                    Some(
                        if self
                            .table_operations
                            .iter()
                            .all(TableOperation::is_read_only)
                        {
                            crate::storage::local_catalog_read_only(root).await?
                        } else {
                            crate::storage::local_catalog(root).await?
                        },
                    )
                }
            }
        } else {
            None
        };
        for operation in &self.table_operations {
            result.table_operations.push(
                operation
                    .execute(
                        job,
                        catalog.as_ref().expect("operation owns catalog").as_ref(),
                    )
                    .await?,
            );
        }
        let mut active_steps: std::collections::BTreeSet<String> =
            self.steps.iter().map(|s| s.name().to_owned()).collect();

        for (output_index, output) in self.outputs.iter().enumerate() {
            *running = Some(Running::Output(output_index));
            let start_epoch = super::metrics::now_epoch_secs();
            let start_instant = std::time::Instant::now();
            let pool_before = job.budget.runtime_env().memory_pool.reserved();
            job.budget.restart_pool_peak();
            job.budget.restart_shared_available_min();
            compactions.take();
            let file = job
                .export(&output.sql, &output.path, &output.settings)
                .await?;
            let pool_after = job.budget.runtime_env().memory_pool.reserved();
            let end_epoch = super::metrics::now_epoch_secs();
            let duration_seconds = start_instant.elapsed().as_secs_f64();
            super::metrics::log_output(
                output_index,
                &output.name,
                file.rows,
                file.size,
                start_epoch,
                end_epoch,
                duration_seconds,
                pool_before,
                pool_after,
                job.budget.pool_peak(),
                job.budget.shared_available_min(),
                compactions.take(),
                super::metrics::process_peak_rss_bytes(),
            );
            result.outputs.push(OutputResult {
                name: output.name.clone(),
                file,
            });
            job.register_completed_output(&output.name, &output.path)
                .await?;

            let remaining_outputs_sql = self.outputs[output_index + 1..]
                .iter()
                .map(|o| o.sql.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let all_queries_sql = self
                .queries
                .iter()
                .map(|q| q.sql.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let to_release: Vec<String> = active_steps
                .iter()
                .filter(|name| {
                    !sql_references_table(&remaining_outputs_sql, name)
                        && !sql_references_table(&all_queries_sql, name)
                })
                .cloned()
                .collect();
            for name in to_release {
                active_steps.remove(&name);
                let _ = job.release(&name);
            }
        }

        for (query_index, query) in self.queries.iter().enumerate() {
            *running = Some(Running::Query(query_index));
            let start_epoch = super::metrics::now_epoch_secs();
            let start_instant = std::time::Instant::now();
            let pool_before = job.budget.runtime_env().memory_pool.reserved();
            job.budget.restart_pool_peak();
            job.budget.restart_shared_available_min();
            compactions.take();
            let (q_res, joins) =
                super::query_result::query_with_joins(job, &query.name, &query.sql).await?;
            let pool_after = job.budget.runtime_env().memory_pool.reserved();
            let end_epoch = super::metrics::now_epoch_secs();
            let duration_seconds = start_instant.elapsed().as_secs_f64();
            super::metrics::log_step(
                1000 + query_index,
                &format!("query:{}", query.name),
                "query",
                q_res.data.as_array().map_or(0, |a| a.len()),
                0,
                start_epoch,
                end_epoch,
                duration_seconds,
                pool_before,
                pool_after,
                job.budget.pool_peak(),
                job.budget.shared_available_min(),
                compactions.take(),
                super::metrics::process_peak_rss_bytes(),
                &joins,
            );
            result.queries.push(q_res);

            let remaining_queries_sql = self.queries[query_index + 1..]
                .iter()
                .map(|q| q.sql.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let to_release: Vec<String> = active_steps
                .iter()
                .filter(|name| !sql_references_table(&remaining_queries_sql, name))
                .cloned()
                .collect();
            for name in to_release {
                active_steps.remove(&name);
                let _ = job.release(&name);
            }
        }
        Ok(())
    }

    /// Puts the running statement's kind, position and name ahead of a failure.
    ///
    /// The code and cause are unchanged. Without it a failure says only what
    /// went wrong, and which of a job's statements did is read off the order
    /// of the log. The name is the statement that was running: a view fails
    /// when a later statement reads it, and is then named as that reader.
    /// Shared across `docspec job`, `prepare-job`, and `managed-job`.
    pub(crate) fn running_parts(
        &self,
        running: Option<Running>,
    ) -> (Option<&'static str>, Option<usize>, Option<&str>) {
        match running {
            None => (None, None, None),
            Some(Running::Step(index)) => {
                (Some("step"), Some(index), Some(self.steps[index].name()))
            }
            Some(Running::Output(index)) => (
                Some("output"),
                Some(index),
                Some(self.outputs[index].name.as_str()),
            ),
            Some(Running::Query(index)) => (
                Some("query"),
                Some(index),
                Some(self.queries[index].name.as_str()),
            ),
        }
    }

    pub(crate) fn named(&self, running: Option<Running>, error: Error) -> Error {
        let (Some(kind), Some(index), Some(name)) = self.running_parts(running) else {
            return error;
        };
        error.context(format_args!("{kind} {index} {name:?}"))
    }
}

/// The statement a job loop is working on, by position in its own list.
///
/// Positions count from zero, as `step_index` and `output_index` do in the
/// step log. Each loop sets it as a statement starts and clears it before
/// non-statement work (such as table operations or directory creation) runs.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Running {
    Step(usize),
    Output(usize),
    Query(usize),
}

fn sql_references_table(sql: &str, table: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = sql[start..].find(table) {
        let abs_pos = start + pos;
        let end_pos = abs_pos + table.len();
        let prefix_ok = abs_pos == 0 || {
            let prev = sql[..abs_pos].chars().last().unwrap();
            !prev.is_alphanumeric() && prev != '_'
        };
        let suffix_ok = end_pos == sql.len() || {
            let next = sql[end_pos..].chars().next().unwrap();
            !next.is_alphanumeric() && next != '_'
        };
        if prefix_ok && suffix_ok {
            return true;
        }
        start = end_pos;
    }
    false
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn hash_local_file(path: &Path, budget: &crate::budget::Budget) -> Result<(String, u64)> {
    const CHUNK_BYTES: usize = 65_536;
    let _memory = budget.reserve(CHUNK_BYTES)?;
    let mut file = std::fs::File::open(path)?;
    let expected_len = file.metadata()?.len();
    let mut buffer = vec![0_u8; CHUNK_BYTES];
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total = total.checked_add(read as u64).ok_or_else(|| {
            Error::refusal("capacity", "local input file size overflowed byte counter")
        })?;
    }
    if total != expected_len {
        return Err(Error::refusal(
            "input_changed",
            format!(
                "local input file size changed while hashing {}",
                path.display()
            ),
        ));
    }
    Ok((format!("{:x}", hasher.finalize()), total))
}

fn observe_local_file(
    path: &Path,
    budget: &crate::budget::Budget,
    compute_digest: bool,
) -> Result<(Option<String>, u64)> {
    if compute_digest {
        let (sha256, bytes) = hash_local_file(path, budget)?;
        Ok((Some(sha256), bytes))
    } else {
        Ok((None, std::fs::metadata(path)?.len()))
    }
}

fn observe_optional_local_file(
    path: &Path,
    budget: &crate::budget::Budget,
    compute_digest: bool,
) -> Result<(Option<String>, Option<u64>)> {
    if path.is_file() {
        let (sha256, bytes) = observe_local_file(path, budget, compute_digest)?;
        Ok((sha256, Some(bytes)))
    } else {
        Ok((None, None))
    }
}

fn collect_parquet_members(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = std::fs::metadata(&path)?;
        if metadata.is_dir() {
            collect_parquet_members(&path, out)?;
        } else if metadata.is_file()
            && metadata.len() > 0
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn observe_parquet_input(
    name: &str,
    path: &Path,
    budget: &crate::budget::Budget,
    compute_digest: bool,
) -> Result<InputResult> {
    let metadata = std::fs::metadata(path)?;
    if metadata.is_file() {
        let (sha256, bytes) = if compute_digest {
            let (digest, len) = hash_local_file(path, budget)?;
            (Some(digest), len)
        } else {
            (None, metadata.len())
        };
        return Ok(InputResult {
            name: name.to_owned(),
            kind: "parquet".into(),
            path: Some(path.to_path_buf()),
            sha256,
            bytes: Some(bytes),
            ..Default::default()
        });
    }
    let mut member_paths = Vec::new();
    collect_parquet_members(path, &mut member_paths)?;
    member_paths.sort();
    let mut files = Vec::with_capacity(member_paths.len());
    let mut total_bytes = 0_u64;
    let mut canonical_members = Vec::with_capacity(if compute_digest {
        member_paths.len()
    } else {
        0
    });
    for member_path in member_paths {
        let (sha256, bytes) = observe_local_file(&member_path, budget, compute_digest)?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::refusal("capacity", "directory Parquet input size overflowed"))?;
        if let Some(digest) = &sha256 {
            let rel = member_path
                .strip_prefix(path)
                .unwrap_or(&member_path)
                .to_string_lossy()
                .into_owned();
            canonical_members.push((rel, digest.clone(), bytes));
        }
        files.push(InputFileResult {
            key: None,
            path: member_path,
            sha256,
            bytes,
        });
    }
    let dir_sha256 = if compute_digest {
        Some(sha256_hex(&serde_json::to_vec(&canonical_members)?))
    } else {
        None
    };
    Ok(InputResult {
        name: name.to_owned(),
        kind: "parquet".into(),
        path: Some(path.to_path_buf()),
        sha256: dir_sha256,
        bytes: Some(total_bytes),
        files,
        ..Default::default()
    })
}

fn observe_json_lines_input(
    name: &str,
    declared_files: &[JsonLinesFile],
    budget: &crate::budget::Budget,
    compute_digest: bool,
) -> Result<InputResult> {
    let mut files = Vec::with_capacity(declared_files.len());
    let mut total_bytes = Some(0_u64);
    for declared in declared_files {
        if compute_digest {
            let (sha256, bytes) = observe_local_file(&declared.path, budget, true)?;
            total_bytes =
                Some(total_bytes.unwrap_or(0).checked_add(bytes).ok_or_else(|| {
                    Error::refusal("capacity", "JSON Lines input size overflowed")
                })?);
            files.push(InputFileResult {
                key: Some(declared.key.clone()),
                path: declared.path.clone(),
                sha256,
                bytes,
            });
        } else {
            let (_, maybe_bytes) = observe_optional_local_file(&declared.path, budget, false)?;
            if let Some(bytes) = maybe_bytes {
                if let Some(running) = total_bytes {
                    total_bytes = Some(running.checked_add(bytes).ok_or_else(|| {
                        Error::refusal("capacity", "JSON Lines input size overflowed")
                    })?);
                }
                files.push(InputFileResult {
                    key: Some(declared.key.clone()),
                    path: declared.path.clone(),
                    sha256: None,
                    bytes,
                });
            } else {
                total_bytes = None;
            }
        }
    }
    let (path, sha256) = if declared_files.len() == 1 {
        let single_sha = files.first().and_then(|f| f.sha256.clone());
        (Some(declared_files[0].path.clone()), single_sha)
    } else if compute_digest {
        let summary = files
            .iter()
            .map(|f| (&f.key, &f.sha256, f.bytes))
            .collect::<Vec<_>>();
        (None, Some(sha256_hex(&serde_json::to_vec(&summary)?)))
    } else {
        (None, None)
    };
    Ok(InputResult {
        name: name.to_owned(),
        kind: "json_lines".into(),
        path,
        sha256,
        bytes: total_bytes,
        files,
        ..Default::default()
    })
}
