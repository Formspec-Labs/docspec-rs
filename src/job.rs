//! SQL jobs share one bounded execution context; callers supply all domain rules.
// Rust guideline compliant 2026-02-21
mod export;
mod json_lines;
mod materialized;
mod query_result;
mod spec;

use crate::{
    Result,
    budget::{Budget, Limits},
};
use arrow::datatypes::SchemaRef;
use datafusion::{
    dataframe::DataFrame,
    prelude::{ParquetReadOptions, SessionContext},
};
use std::path::{Path, PathBuf};

pub use export::{ExportProfile, ExportSettings, ExportedFile};
pub use json_lines::JsonLinesFile;
pub use materialized::StepStorage;
pub use query_result::QueryResult;
pub use spec::{
    JobFailure, JobInput, JobOutput, JobResult, JobSpec, OutputResult, SqlQuery, SqlStep,
    StepResult,
};

/// An embedded SQL job. Output frames retain their source and memory ownership.
pub struct Job {
    context: SessionContext,
    budget: Budget,
    scratch: PathBuf,
}

impl Job {
    /// Registers a reusable logical view without collecting its rows.
    ///
    /// # Errors
    /// Refuses an existing name or an invalid or volatile SQL expression.
    pub async fn register_view(&self, name: &str, sql: &str) -> Result<()> {
        if self.context.table_exist(name)? {
            return Err(crate::Error::refusal(
                "invalid_spec",
                "named step already exists",
            ));
        }
        let frame = self.sql(sql).await?;
        self.context.register_table(name, frame.into_view())?;
        Ok(())
    }
    /// Removes a finished name while existing plans retain their dependencies.
    ///
    /// # Errors
    /// Refuses unknown names or native catalog failures.
    pub fn release(&self, name: &str) -> Result<()> {
        self.context
            .deregister_table(name)?
            .ok_or_else(|| crate::Error::refusal("invalid_spec", "released step does not exist"))?;
        Ok(())
    }

    /// Creates a job with a caller-owned scratch directory and explicit limits.
    ///
    /// # Errors
    /// Refuses invalid limits or an unavailable scratch directory.
    pub fn new(limits: Limits, scratch: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&scratch)?;
        let budget = Budget::in_workspace(limits, &scratch)?;
        Ok(Self {
            context: budget.context(),
            budget,
            scratch,
        })
    }

    /// Registers a Parquet file or directory with native projection and statistics.
    ///
    /// # Errors
    /// Returns native schema, input I/O, or registration errors.
    pub async fn register_parquet(&self, name: &str, path: &Path) -> Result<()> {
        self.context
            .register_parquet(
                name,
                path.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await?;
        Ok(())
    }

    /// Registers gzip JSON lines with an explicit nullable schema and raw evidence.
    ///
    /// # Errors
    /// Refuses required fields, reserved evidence names, or invalid registration.
    pub fn register_json_lines(
        &self,
        name: &str,
        files: Vec<JsonLinesFile>,
        schema: SchemaRef,
    ) -> Result<()> {
        let provider = json_lines::provider(files, schema, &self.budget.limits)?;
        self.context.register_table(name, provider)?;
        Ok(())
    }

    /// Plans a SQL statement in the same context as every declared input and step.
    ///
    /// # Errors
    /// Returns SQL planning errors; execution errors propagate from the frame.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let frame = self.context.sql(sql).await?;
        crate::query::validate_immutable_functions(frame.logical_plan())?;
        Ok(frame)
    }

    /// Computes a named step once, reserving retained batches or spilling to disk.
    ///
    /// # Errors
    /// Refuses an existing name, exhausted capacity, failed input or invalid SQL.
    pub async fn materialize(&self, name: &str, sql: &str) -> Result<StepStorage> {
        materialized::materialize(&self.context, &self.budget, &self.scratch, name, sql).await
    }

    /// Registers an exact immutable Iceberg pin using the maintained static provider.
    ///
    /// # Errors
    /// Refuses changed metadata, an absent pinned snapshot, or provider failures.
    pub async fn register_iceberg(&self, name: &str, pin: &crate::model::TablePin) -> Result<()> {
        use iceberg_datafusion::table::IcebergStaticTableProvider;
        let table = crate::storage::open_pin(pin, &self.budget).await?;
        let provider = match pin.snapshot {
            Some(snapshot) => {
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot).await?
            }
            None => IcebergStaticTableProvider::try_new_from_table(table).await?,
        };
        self.context
            .register_table(name, std::sync::Arc::new(provider))?;
        Ok(())
    }

    /// Exports an ordered SQL result using explicit reproducible Parquet settings.
    ///
    /// # Errors
    /// Refuses invalid layout, capacity exhaustion, existing output or writer failure.
    pub async fn export(
        &self,
        sql: &str,
        path: &Path,
        settings: &ExportSettings,
    ) -> Result<ExportedFile> {
        export::export(self.sql(sql).await?, path, settings, &self.budget).await
    }
}
