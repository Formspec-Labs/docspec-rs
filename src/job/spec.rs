//! Declarative job execution uses the embedded SQL service directly.
// Rust guideline compliant 2026-02-21
use super::{ComponentsSettings, ExportSettings, ExportedFile, Job, JsonLinesFile, StepStorage};
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
            | Self::Iceberg { name, .. } => name,
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

impl JobSpec {
    /// Executes every declared relation and export through one bounded SQL job.
    ///
    /// # Errors
    /// Refuses duplicate names/paths before I/O and propagates input, SQL,
    /// resource and export failures. Completed outputs remain create-only.
    pub async fn run(&self, limits: Limits) -> std::result::Result<JobResult, JobFailure> {
        let mut completed = JobResult {
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
        };
        match self.execute(limits, &mut completed).await {
            Ok(()) => Ok(completed),
            Err(error) => Err(JobFailure { completed, error }),
        }
    }

    async fn execute(&self, limits: Limits, result: &mut JobResult) -> Result<()> {
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
        let job = Job::new(limits, self.scratch.clone())?;
        for input in &self.inputs {
            match input {
                JobInput::Parquet { name, path } => job.register_parquet(name, path).await?,
                JobInput::JsonLines {
                    name,
                    files,
                    schema,
                } => job.register_json_lines(name, files.clone(), Arc::new(schema.clone()))?,
                JobInput::Iceberg { name, pin } => job.register_iceberg(name, pin).await?,
            }
        }
        for step in &self.steps {
            let storage = match step {
                JobStep::Sql(step) if step.materialize => {
                    job.materialize(&step.name, &step.sql).await?
                }
                JobStep::Sql(step) => {
                    job.register_view(&step.name, &step.sql).await?;
                    StepStorage::View
                }
                JobStep::Components(step) => {
                    job.connected_components(&step.name, &step.components)
                        .await?
                }
            };
            for name in step.release_after() {
                job.release(name)?;
            }
            result.steps.push(StepResult {
                name: step.name().to_owned(),
                storage,
            });
        }
        for output in &self.outputs {
            let file = job
                .export(&output.sql, &output.path, &output.settings)
                .await?;
            result.outputs.push(OutputResult {
                name: output.name.clone(),
                file,
            });
        }
        for query in &self.queries {
            result
                .queries
                .push(super::query_result::query(&job, &query.name, &query.sql).await?);
        }
        Ok(())
    }
}
