//! SQL jobs share one bounded execution context; callers supply all domain rules.
// Rust guideline compliant 2026-02-21
mod canonical;
mod components;
mod export;
mod functions;
mod json_lines;
mod json_many;
mod materialized;
mod native;
mod query_result;
mod spec;
mod table_operations;
pub mod tables;

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

pub use components::ComponentsSettings;
pub use export::{ExportProfile, ExportSettings, ExportedFile};
pub use json_lines::JsonLinesFile;
pub use materialized::StepStorage;
pub use query_result::QueryResult;
pub use spec::{
    ComponentsStep, JobFailure, JobInput, JobOutput, JobResult, JobSpec, JobStep, OutputResult,
    SqlQuery, SqlStep, StepResult,
};
pub use table_operations::{TableOperation, TableOperationResult};

/// An embedded SQL job. Output frames retain their source and memory ownership.
pub struct Job {
    context: SessionContext,
    budget: Budget,
    #[cfg(feature = "remote")]
    remote: Option<crate::storage::remote::RemoteStorage>,
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
        let mut context = budget.context();
        functions::register(&mut context, &budget)?;
        Ok(Self {
            context,
            budget,
            #[cfg(feature = "remote")]
            remote: None,
        })
    }

    /// Creates the SQL service with explicit remote catalog and object I/O.
    ///
    /// # Errors
    /// Refuses invalid host limits, scratch or object-store registration.
    #[cfg(feature = "remote")]
    pub fn with_remote(
        limits: Limits,
        scratch: PathBuf,
        remote: crate::storage::remote::RemoteStorage,
    ) -> Result<Self> {
        let mut job = Self::new(limits, scratch)?;
        remote
            .objects
            .register_query_store(job.budget.runtime_env())?;
        job.remote = Some(remote);
        Ok(job)
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

    /// Registers plain or gzip JSON lines with nullable fields and raw evidence.
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
        materialized::materialize(&self.context, &self.budget, name, sql).await
    }

    /// Groups an undirected string edge relation into deterministic components.
    ///
    /// # Errors
    /// Refuses invalid endpoints, existing names, node or byte exhaustion.
    pub async fn connected_components(
        &self,
        name: &str,
        settings: &ComponentsSettings,
    ) -> Result<StepStorage> {
        components::compute(self, name, settings).await
    }

    /// Registers an exact immutable Iceberg pin using the maintained static provider.
    ///
    /// # Errors
    /// Refuses changed metadata, an absent pinned snapshot, or provider failures.
    pub async fn register_iceberg(&self, name: &str, pin: &crate::model::TablePin) -> Result<()> {
        let (table, memory) = crate::storage::open_pin_admitted(
            pin,
            #[cfg(feature = "remote")]
            self.remote.as_ref().map(|x| &x.objects),
            &self.budget,
        )
        .await?;
        self.register_admitted_native_table(name, table, std::sync::Arc::new(memory))
            .await
    }

    /// Registers exact immutable native metadata without consulting a catalog.
    ///
    /// Null snapshot means an empty table. Metadata and its decoded-byte
    /// admission remain owned by every resulting frame, plan and stream.
    ///
    /// # Errors
    /// Refuses unavailable or mismatched metadata, capacity and provider errors.
    pub async fn register_native_snapshot(
        &self,
        name: &str,
        selection: &crate::storage::NativeTableSnapshot,
    ) -> Result<()> {
        let (table, memory) = crate::storage::open_snapshot(
            selection,
            #[cfg(feature = "remote")]
            self.remote.as_ref().map(|x| &x.objects),
            &self.budget,
        )
        .await?;
        self.register_admitted_native_table(name, table, std::sync::Arc::new(memory))
            .await
    }

    pub(super) async fn register_native_table(
        &self,
        name: &str,
        table: iceberg::table::Table,
    ) -> Result<()> {
        // Catalog selection returns parsed metadata without a byte admission.
        // Re-admit that immutable selection once through the bounded reader;
        // upstream Serialize would clone unknown-sized metadata before counting.
        let selection = crate::storage::NativeTableSnapshot {
            metadata_location: table
                .metadata_location()
                .ok_or_else(|| {
                    crate::Error::refusal(
                        "invalid_spec",
                        "selected table has no immutable metadata location",
                    )
                })?
                .to_owned(),
            table_uuid: table.metadata().uuid(),
            snapshot: table.metadata().current_snapshot_id(),
            schema_id: table.metadata().current_schema_id(),
        };
        #[cfg(feature = "remote")]
        let (table, memory) = if self.remote.is_some() {
            crate::storage::open_snapshot(
                &selection,
                self.remote.as_ref().map(|x| &x.objects),
                &self.budget,
            )
            .await?
        } else {
            crate::storage::open_snapshot_with_io_admitted(
                &selection,
                table.file_io().clone(),
                &self.budget,
            )
            .await?
        };
        #[cfg(not(feature = "remote"))]
        let (table, memory) = crate::storage::open_snapshot_with_io_admitted(
            &selection,
            table.file_io().clone(),
            &self.budget,
        )
        .await?;
        let memory = std::sync::Arc::new(memory);
        self.register_admitted_native_table(name, table, memory)
            .await
    }
    async fn register_admitted_native_table(
        &self,
        name: &str,
        table: iceberg::table::Table,
        memory: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<()> {
        use iceberg_datafusion::table::IcebergStaticTableProvider;
        let provider = match table.metadata().current_snapshot_id() {
            Some(snapshot) => {
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot).await?
            }
            None => IcebergStaticTableProvider::try_new_from_table(table).await?,
        };
        self.context.register_table(
            name,
            std::sync::Arc::new(native::AdmittedTable {
                provider: std::sync::Arc::new(provider),
                memory,
            }),
        )?;
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
