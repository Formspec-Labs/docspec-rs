//! SQL jobs share one bounded execution context; callers supply all domain rules.
// Rust guideline compliant 2026-02-21
mod json_lines;
mod materialized;

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

pub use json_lines::JsonLinesFile;
pub use materialized::StepStorage;

/// An embedded SQL job. Output frames retain their source and memory ownership.
pub struct Job {
    context: SessionContext,
    budget: Budget,
    scratch: PathBuf,
}

impl Job {
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
        Ok(self.context.sql(sql).await?)
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
}
