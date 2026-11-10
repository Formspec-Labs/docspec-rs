//! Declarative job execution uses the embedded SQL service directly.
// Rust guideline compliant 2026-02-21
use super::{
    ComponentsSettings, ExportSettings, ExportedFile, Job, JsonLinesFile, StepStorage,
    TableOperation, TableOperationResult,
};
use crate::{Error, Result, budget::Limits, model::TablePin};
use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

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
}
impl JobInput {
    fn name(&self) -> &str {
        match self {
            Self::Parquet { name, .. }
            | Self::JsonLines { name, .. }
            | Self::Iceberg { name, .. }
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

/// Distinct step shapes prevent mixing SQL-view and graph options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JobStep {
    Sql(SqlStep),
    Components(ComponentsStep),
}
impl JobStep {
    fn name(&self) -> &str {
        match self {
            Self::Sql(step) => &step.name,
            Self::Components(step) => &step.name,
        }
    }
    fn release_after(&self) -> &[String] {
        match self {
            Self::Sql(step) => &step.release_after,
            Self::Components(step) => &step.release_after,
        }
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

/// Storage selected for one named relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub name: String,
    pub storage: StepStorage,
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
    /// Summary facts from exact native inputs; unavailable observations remain null.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_inputs: Vec<super::NativeInputObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_operations: Vec<TableOperationResult>,
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
    catalog: Option<Arc<dyn iceberg::Catalog>>,
    #[cfg(feature = "remote")]
    remote: Option<crate::storage::remote::RemoteStorage>,
}

impl JobSpec {
    /// Executes every declared relation and export through one bounded SQL job.
    ///
    /// # Errors
    /// Refuses duplicate names/paths before I/O and propagates input, SQL,
    /// resource and export failures. Completed outputs remain create-only.
    pub async fn run(&self, limits: Limits) -> std::result::Result<JobResult, JobFailure> {
        self.run_injected(limits, Connections::default()).await
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
                catalog: Some(remote.catalog.clone()),
                remote: Some(remote),
            },
        )
        .await
    }

    async fn run_injected(
        &self,
        limits: Limits,
        connections: Connections,
    ) -> std::result::Result<JobResult, JobFailure> {
        let mut completed = JobResult {
            native_inputs: Vec::new(),
            table_operations: Vec::new(),
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
        };
        let mut running = None;
        match self
            .execute(limits, &mut completed, connections, &mut running)
            .await
        {
            Ok(()) => Ok(completed),
            Err(error) => Err(JobFailure {
                completed,
                error: self.named(running, error),
            }),
        }
    }

    async fn execute(
        &self,
        limits: Limits,
        result: &mut JobResult,
        connections: Connections,
        running: &mut Option<Running>,
    ) -> Result<()> {
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
        #[cfg(feature = "remote")]
        let job = if let Some(remote) = connections.remote {
            Job::with_remote(limits, self.scratch.clone(), remote)?
        } else {
            Job::new(limits, self.scratch.clone())?
        };
        #[cfg(not(feature = "remote"))]
        let job = Job::new(limits, self.scratch.clone())?;
        for input in &self.inputs {
            match input {
                JobInput::Parquet { name, path } => job.register_parquet(name, path).await?,
                JobInput::JsonLines {
                    name,
                    files,
                    schema,
                } => job.register_json_lines(name, files.clone(), Arc::new(schema.clone()))?,
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
                    .await?
                }
                JobInput::Iceberg { name, pin } => job.register_iceberg(name, pin).await?,
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
        for (step_index, step) in self.steps.iter().enumerate() {
            *running = Some(Running::Step(step_index));
            let start_epoch = super::metrics::now_epoch_secs();
            let start_instant = std::time::Instant::now();
            let pool_before = job.budget.runtime_env().memory_pool.reserved();
            job.budget.restart_pool_peak();
            let (storage, rows, charged_bytes) = match step {
                JobStep::Sql(step) if step.materialize => {
                    job.materialize_detailed(&step.name, &step.sql).await?
                }
                JobStep::Sql(step) => {
                    job.register_view(&step.name, &step.sql).await?;
                    (StepStorage::View, 0, 0)
                }
                JobStep::Components(step) => {
                    job.connected_components_detailed(&step.name, &step.components)
                        .await?
                }
            };
            let pool_after = job.budget.runtime_env().memory_pool.reserved();
            let end_epoch = super::metrics::now_epoch_secs();
            let duration_seconds = start_instant.elapsed().as_secs_f64();
            super::metrics::log_step(
                step_index,
                step.name(),
                match storage {
                    StepStorage::Memory => "memory",
                    StepStorage::Scratch => "scratch",
                    StepStorage::View => "view",
                },
                rows,
                charged_bytes,
                start_epoch,
                end_epoch,
                duration_seconds,
                pool_before,
                pool_after,
                job.budget.pool_peak(),
            );
            for name in step.release_after() {
                job.release_for_step(step_index, step.name(), name)?;
            }
            result.steps.push(StepResult {
                name: step.name().to_owned(),
                storage,
            });
        }
        *running = None;
        let catalog = if !self.table_operations.is_empty() {
            match connections.catalog {
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
                        &job,
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
            );
            result.outputs.push(OutputResult {
                name: output.name.clone(),
                file,
            });

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

        *running = None;
        for (query_index, query) in self.queries.iter().enumerate() {
            *running = Some(Running::Query(query_index));
            let start_epoch = super::metrics::now_epoch_secs();
            let start_instant = std::time::Instant::now();
            let pool_before = job.budget.runtime_env().memory_pool.reserved();
            job.budget.restart_pool_peak();
            let q_res = super::query_result::query(&job, &query.name, &query.sql).await?;
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
    /// of the log.
    fn named(&self, running: Option<Running>, error: Error) -> Error {
        let (kind, index, name) = match running {
            None => return error,
            Some(Running::Step(index)) => ("step", index, self.steps[index].name()),
            Some(Running::Output(index)) => ("output", index, self.outputs[index].name.as_str()),
            Some(Running::Query(index)) => ("query", index, self.queries[index].name.as_str()),
        };
        error.context(format_args!("{kind} {index} {name:?}"))
    }
}

/// The statement `execute` is working on, by position in its own list.
///
/// Positions count from zero, as `step_index` and `output_index` do in the
/// step log. `execute` sets it as each statement starts and clears it before
/// other work follows a list, so that work is not blamed on a statement.
#[derive(Debug, Clone, Copy)]
enum Running {
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
