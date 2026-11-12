//! SQL jobs share one bounded execution context; callers supply all domain rules.
// Rust guideline compliant 2026-02-21
mod canonical;
mod components;
mod encoding;
mod export;
pub(crate) mod fitting;
mod functions;
mod json_lines;
mod json_many;
mod materialized;
pub(crate) mod metrics;
mod native;
mod plan;
mod prepare;
mod query_result;
pub(crate) mod retained;
mod spec;
#[cfg(feature = "substrait")]
mod substrait;
mod table_operations;
pub mod tables;

#[cfg(test)]
mod evidence_tests;
#[cfg(test)]
mod slice_charge_tests;

use crate::{
    Result,
    budget::{Budget, Limits},
};
use arrow::datatypes::SchemaRef;
use datafusion::{
    dataframe::DataFrame,
    prelude::{ParquetReadOptions, SessionContext},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub use components::ComponentsSettings;
pub use export::{ExportProfile, ExportSettings, ExportedFile};
pub use json_lines::JsonLinesFile;
pub use materialized::StepStorage;
#[doc(inline)]
pub use native::{
    NativeInputObservation, NativeVersionObservation, NativeVersionSnapshotObservation,
};
#[doc(inline)]
pub use plan::JobPlanSpec;
#[doc(inline)]
pub use prepare::{
    DeclaredInputSchema, JobArtifact, PrepareJobSpec, PreparedInput, PreparedJob, PreparedRelation,
    SchemaArtifact, SchemaArtifactFormat,
};
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
    relation_aliases: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
    /// Schema-only planners refuse casts DataFusion adds between type families.
    refuse_implicit_casts: bool,
    #[cfg(feature = "remote")]
    remote: Option<crate::storage::remote::RemoteStorage>,
}

impl Job {
    fn register_output_provider(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<()> {
        let catalog = self.context.catalog("datafusion").ok_or_else(|| {
            crate::Error::refusal("invalid_spec", "native job catalog is missing")
        })?;
        if catalog.schema("outputs").is_none() {
            catalog.register_schema(
                "outputs",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )?;
        }
        let reference = datafusion::common::TableReference::partial("outputs", name);
        if self.context.table_exist(reference.clone())? {
            return Err(crate::Error::refusal(
                "invalid_spec",
                "named export relation already exists",
            ));
        }
        self.context.register_table(reference, provider)?;
        Ok(())
    }

    pub(crate) async fn register_completed_output(&self, name: &str, path: &Path) -> Result<()> {
        // DataFusion's defaults: an export's columns lose their own metadata, as
        // `outputs.<name>` always has, in plain and managed jobs alike.
        let frame = materialized::read_retained(&self.budget, path, |_| {}).await?;
        let frame = self.owned_relation_frame(name, true, frame)?;
        self.register_output_provider(name, frame.into_view())
    }

    fn register_declared_output(&self, name: &str, schema: SchemaRef) -> Result<()> {
        self.register_output_provider(name, Arc::new(plan::SchemaOnlyTable(schema)))
    }
    /// Registers a reusable logical view without collecting its rows.
    ///
    /// # Errors
    /// Refuses an existing name or an invalid or volatile SQL expression.
    pub async fn register_view(&self, name: &str, sql: &str) -> Result<()> {
        let frame = self.sql(sql).await?;
        self.register_frame(name, frame)
    }

    fn register_frame(&self, name: &str, frame: DataFrame) -> Result<()> {
        if self.context.table_exist(name)? {
            return Err(crate::Error::refusal(
                "invalid_spec",
                "named relation already exists",
            ));
        }
        self.context.register_table(name, frame.into_view())?;
        Ok(())
    }
    /// Removes a finished name while existing plans retain their dependencies.
    ///
    /// # Errors
    /// Refuses unknown names or native catalog failures.
    pub fn release(&self, name: &str) -> Result<()> {
        self.release_for_step(0, "", name)
    }

    /// Removes a finished name for a specific step while logging metrics.
    pub fn release_for_step(&self, step_index: usize, step_name: &str, name: &str) -> Result<()> {
        let pool_before = self.budget.runtime_env().memory_pool.reserved();
        self.context
            .deregister_table(name)?
            .ok_or_else(|| crate::Error::refusal("invalid_spec", "released step does not exist"))?;
        let pool_after = self.budget.runtime_env().memory_pool.reserved();
        metrics::log_release(step_index, step_name, name, pool_before, pool_after);
        Ok(())
    }

    /// Creates a job with a caller-owned scratch directory and explicit limits.
    ///
    /// # Errors
    /// Refuses invalid limits or an unavailable scratch directory.
    pub fn new(limits: Limits, scratch: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&scratch)?;
        Self::from_budget(Budget::in_workspace(limits, &scratch)?)
    }

    pub(crate) fn from_budget(budget: Budget) -> Result<Self> {
        let mut context = budget.context();
        functions::register(&mut context, &budget)?;
        plan::constants::register(&context, &budget)?;
        Ok(Self {
            context,
            budget,
            relation_aliases: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            refuse_implicit_casts: false,
            #[cfg(feature = "remote")]
            remote: None,
        })
    }

    /// Makes later SQL planning refuse implicit casts between type families.
    fn refusing_implicit_casts(mut self) -> Self {
        self.refuse_implicit_casts = true;
        self
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

    /// Plans later queries without retained-input compaction, for comparisons in tests.
    ///
    /// Tables stay registered; only the planner rule is removed.
    #[cfg(any(test, feature = "test-util"))]
    pub fn without_retained_compaction(&mut self) {
        self.context = crate::budget::without_retained_compaction(&self.context);
    }

    /// Highest native pool total since [`Self::restart_pool_peak`], for memory tests.
    ///
    /// The step log's `pool_reserved_peak` is this figure for one step.
    #[cfg(any(test, feature = "test-util"))]
    pub fn pool_peak(&self) -> usize {
        self.budget.pool_peak()
    }

    /// Starts a new pool peak interval at the current total, for memory tests.
    #[cfg(any(test, feature = "test-util"))]
    pub fn restart_pool_peak(&self) {
        self.budget.restart_pool_peak();
    }

    /// Registers a Parquet file or directory with native projection and statistics.
    ///
    /// # Errors
    /// Refuses a path where the listing finds no file to read (code
    /// `missing_input`).
    /// Returns native schema, input I/O, or registration errors. Footer
    /// admission and listing failures name the input and its path.
    pub async fn register_parquet(&self, name: &str, path: &Path) -> Result<()> {
        let metadata_memory = if path.is_file() {
            Some(Arc::new(
                crate::storage::admit_parquet_footer(path, &self.budget)
                    .await
                    .map_err(|error| unreadable_parquet(name, path, error))?,
            ))
        } else {
            None
        };
        self.context
            .register_parquet(
                name,
                path.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await
            .map_err(|error| unreadable_parquet(name, path, error))?;
        let provider = self.context.table_provider(name).await?;
        self.require_parquet_file(name, path, &provider.schema())?;
        self.context.deregister_table(name)?;
        self.context.register_table(
            name,
            std::sync::Arc::new(fitting::FittedTable {
                provider,
                metadata_memory,
            }),
        )?;
        Ok(())
    }

    /// Refuses a Parquet input for which the listing found no file to read.
    ///
    /// The listing reads a path it cannot find, and a directory holding no
    /// non-empty file with the suffix (it skips files of zero bytes), as a
    /// table with no columns and no rows. A statement then fails on an
    /// unknown column, or counts zero rows and succeeds. A written Parquet
    /// file brings its columns whether or not it has rows, so `listed`
    /// without columns means nothing was read. A single file's footer is
    /// admitted before the listing, so a lone empty or torn file is refused
    /// there instead.
    ///
    /// It runs after the listing on purpose: a check before it would restate
    /// the listing's rules for what it reads, list the path twice, and refuse
    /// a path given as a URL.
    fn require_parquet_file(
        &self,
        name: &str,
        path: &Path,
        listed: &arrow::datatypes::Schema,
    ) -> Result<()> {
        if !listed.fields().is_empty() {
            return Ok(());
        }
        self.context.deregister_table(name)?;
        Err(crate::Error::refusal(
            "missing_input",
            format!(
                "Parquet input {name:?} has no readable Parquet file at {}: \
                 the path is missing or holds no non-empty file ending in .parquet",
                path.display()
            ),
        ))
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

    /// Plans read-only relational SQL in the shared job context.
    ///
    /// The returned `DataFrame` is a native DataFusion escape hatch. Calling its
    /// `collect` or `create_physical_plan` directly does not include this Job's
    /// cumulative optimizer/stream ownership. `JobSpec::run`, materialization,
    /// exports and managed commands use the maintained admitted consumer paths.
    ///
    /// # Errors
    /// Refuses DDL, DML, session statements, unbound parameters and invalid functions.
    /// Returns planning errors; execution errors propagate from the frame.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.sql_bound(sql, &std::collections::BTreeMap::new())
            .await
    }

    async fn sql_unbound(&self, sql: &str) -> Result<DataFrame> {
        let options = datafusion::execution::context::SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = self.context.sql_with_options(sql, options).await?;
        crate::query::validate_immutable_functions(frame.logical_plan())?;
        Ok(frame)
    }

    /// Computes a named step once, reserving retained batches or spilling to disk.
    ///
    /// # Errors
    /// Refuses an existing name, exhausted capacity, failed input or invalid SQL.
    pub async fn materialize(&self, name: &str, sql: &str) -> Result<StepStorage> {
        let (storage, _, _) = materialized::materialize(self, name, sql).await?;
        Ok(storage)
    }

    /// Computes a named step with detailed metrics (storage, rows, charged bytes).
    pub async fn materialize_detailed(
        &self,
        name: &str,
        sql: &str,
    ) -> Result<(StepStorage, usize, usize)> {
        materialized::materialize(self, name, sql).await
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
        let (storage, _, _) = components::compute(self, name, settings).await?;
        Ok(storage)
    }

    /// Groups an undirected string edge relation into deterministic components with metrics.
    pub async fn connected_components_detailed(
        &self,
        name: &str,
        settings: &ComponentsSettings,
    ) -> Result<(StepStorage, usize, usize)> {
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

    pub(crate) async fn register_retained_snapshot(
        &self,
        name: &str,
        selection: &crate::storage::NativeTableSnapshot,
        paths: std::collections::BTreeMap<String, String>,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        let owner = std::sync::Arc::new(std::sync::Mutex::new(vec![std::sync::Arc::new(memory)]));
        let io = crate::storage::custody::retained_io(paths, &self.budget, Some(owner.clone()))?;
        let (table, metadata_memory) =
            crate::storage::open_snapshot_with_io_admitted(selection, io, &self.budget).await?;
        let table = iceberg::table::Table::builder()
            .metadata(table.metadata_ref())
            .metadata_location(&selection.metadata_location)
            .identifier(iceberg::TableIdent::from_strs(["retained", "snapshot"])?)
            .runtime(iceberg::Runtime::try_current()?)
            .file_io(table.file_io().clone())
            .readonly(true)
            .disable_cache()
            .build()?;
        let metadata_memory = std::sync::Arc::new(metadata_memory);
        // Native manifest tasks may retain FileIO after the outer stream drops.
        // This shared owner contains permits only, so it cannot retain the table.
        owner.lock().unwrap().push(metadata_memory.clone());
        // This retained profile uses the existing serial, byte-fitted native
        // scan rather than the upstream provider's host-wide concurrency defaults.
        let observation = NativeInputObservation::from_table(name, &table);
        let provider = crate::query::table(table, self.budget.clone(), None)?;
        self.context.register_table(
            name,
            std::sync::Arc::new(native::AdmittedTable {
                provider,
                memory: metadata_memory,
                observation,
            }),
        )?;
        Ok(())
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
        let observation = NativeInputObservation::from_table(name, &table);
        let metadata = table.metadata();
        let head = match metadata.current_snapshot() {
            Some(snapshot) => Some((
                snapshot.snapshot_id(),
                crate::storage::schema::appends_nullable_columns(
                    snapshot.schema(metadata)?.as_ref(),
                    metadata.current_schema(),
                ),
            )),
            None => None,
        };
        let provider: std::sync::Arc<dyn datafusion::catalog::TableProvider> = match head {
            // Columns added after the head snapshot belong to the head. The upstream
            // provider scans a snapshot with its own schema, so read this head with
            // the engine's current-schema reader; every other head is unchanged.
            Some((_, true)) => crate::query::table(table, self.budget.clone(), None)?,
            Some((snapshot, false)) => std::sync::Arc::new(
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot).await?,
            ),
            None => {
                std::sync::Arc::new(IcebergStaticTableProvider::try_new_from_table(table).await?)
            }
        };
        self.context.register_table(
            name,
            std::sync::Arc::new(native::AdmittedTable {
                provider,
                memory,
                observation,
            }),
        )?;
        Ok(())
    }

    /// Reads available facts retained from an input's selected native metadata.
    ///
    /// Performs no additional catalog, manifest, planning or data reads.
    ///
    /// # Errors
    /// Refuses unknown bindings and sources not registered from native metadata.
    pub async fn native_input_observation(&self, name: &str) -> Result<NativeInputObservation> {
        let provider = self.context.table_provider(name).await?;
        provider
            .as_any()
            .downcast_ref::<native::AdmittedTable>()
            .map(|table| table.observation.clone())
            .ok_or_else(|| {
                crate::Error::refusal(
                    "invalid_spec",
                    "observation requires a registered native input",
                )
            })
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

/// Puts the input's name and path ahead of a failure to list or read it.
fn unreadable_parquet(name: &str, path: &Path, error: impl Into<crate::Error>) -> crate::Error {
    error
        .into()
        .context(format_args!("Parquet input {name:?} at {}", path.display()))
}

#[cfg(test)]
mod sql_guard_tests {
    use super::Job;
    use crate::budget::Limits;

    #[tokio::test]
    async fn correlated_filters_compare_mixed_string_representations() {
        use arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        for outer in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for inner in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                let root = tempfile::tempdir().unwrap();
                let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
                for (name, kind, values) in [
                    (
                        "candidates",
                        outer.clone(),
                        vec![Some("same"), Some("left"), None],
                    ),
                    ("existing", inner.clone(), vec![Some("same"), None]),
                    ("versions", inner.clone(), vec![Some("same"), None]),
                ] {
                    let values = arrow::compute::cast(&StringArray::from(values), &kind).unwrap();
                    let schema = Arc::new(Schema::new(vec![Field::new("value", kind, true)]));
                    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
                    let table =
                        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
                            .unwrap();
                    job.context.register_table(name, Arc::new(table)).unwrap();
                }
                job.register_view(
                    "combined",
                    "SELECT value FROM existing UNION ALL SELECT value FROM candidates",
                )
                .await
                .unwrap();
                job.register_view(
                    "grouped",
                    "SELECT max(value) AS value FROM combined GROUP BY substr(value,1,1)",
                )
                .await
                .unwrap();
                for sql in [
                    "SELECT value FROM candidates c WHERE c.value IN ('same','left') AND NOT EXISTS (SELECT 1 FROM existing e WHERE e.value=c.value) ORDER BY value",
                    "SELECT value FROM candidates c WHERE c.value IN ('same','left') AND NOT EXISTS (SELECT 1 FROM existing e JOIN versions v ON e.value=v.value WHERE e.value=c.value) ORDER BY value",
                    "SELECT value FROM grouped c WHERE c.value IN ('same','left') AND NOT EXISTS (SELECT 1 FROM existing e JOIN versions v ON e.value=v.value WHERE e.value=c.value) ORDER BY value",
                    "SELECT value FROM candidates c WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.value IS NOT DISTINCT FROM c.value) ORDER BY value",
                ] {
                    let frame = job.sql(sql).await.unwrap();
                    let batches = frame
                        .collect()
                        .await
                        .unwrap_or_else(|error| panic!("{outer:?}/{inner:?}: {sql}: {error}"));
                    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                    let values =
                        arrow::compute::cast(batches[0].column(0), &DataType::Utf8).unwrap();
                    assert_eq!(
                        values.as_any().downcast_ref::<StringArray>().unwrap(),
                        &StringArray::from(vec!["left"])
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn null_url_arguments_preserve_the_other_string_representation() {
        use arrow::{
            array::{ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let root = tempfile::tempdir().unwrap();
            let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
            let values = arrow::compute::cast(
                &StringArray::from(vec![Some("https://example.test/a"), None]),
                &data_type,
            )
            .unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
            let batch = RecordBatch::try_new(schema.clone(), vec![values as ArrayRef]).unwrap();
            let table =
                datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            job.context.register_table("v", Arc::new(table)).unwrap();
            let frame = job
                .sql("SELECT url_resolve(value,NULL) AS absolute,url_resolve(NULL,value) AS absent FROM v")
                .await
                .unwrap();
            let plan = frame
                .clone()
                .into_optimized_plan()
                .unwrap()
                .display_indent()
                .to_string();
            assert!(!plan.contains("CAST(v.value"), "{plan}");
            let batches = frame.collect().await.unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 2);
            let absolute = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                absolute,
                &StringArray::from(vec![Some("https://example.test/a"), None])
            );
            assert_eq!(batches[0].column(1).null_count(), 2);
        }
    }

    #[tokio::test]
    async fn repeated_large_views_are_narrowed_without_an_i32_string_copy() {
        use arrow::{
            array::{Array, ArrayRef, StringViewArray, UInt32Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        let job = Job::new(
            Limits {
                memory_bytes: 128 << 20,
                max_batch_bytes: 4 << 20,
                ..Limits::default()
            },
            root.path().join("scratch"),
        )
        .unwrap();
        let raw = format!(r#"{{"keep":1,"padding":"{}"}}"#, "x".repeat(1 << 20));
        let one = StringViewArray::from(vec![raw]);
        let values = arrow::compute::take(&one, &UInt32Array::from(vec![0; 2048]), None).unwrap();
        assert!(
            values
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .total_bytes_len()
                > i32::MAX as usize
        );
        assert!(values.get_array_memory_size() < 2 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "raw",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values as ArrayRef]).unwrap();
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        job.context
            .register_table("expanded", Arc::new(table))
            .unwrap();
        let frame = job
            .sql("SELECT json_get_many(raw,'[[\"keep\"]]') AS selected FROM expanded")
            .await
            .unwrap();
        let plan = frame
            .clone()
            .into_optimized_plan()
            .unwrap()
            .display_indent()
            .to_string();
        // Fail cheaply before a broken coercion expands the 1 MiB backing buffer.
        assert!(!plan.contains("CAST(expanded.raw AS Utf8)"), "{plan}");
        let batches = frame.collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2048);
        for batch in batches {
            let selected = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .unwrap();
            let values = selected
                .values()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            assert!(values.iter().all(|v| v == Some("1")));
        }
    }

    #[tokio::test]
    async fn caller_sql_refuses_commands_before_session_or_filesystem_effects() {
        for retained in [false, true] {
            for command in ["copy", "ddl", "set"] {
                let root = tempfile::tempdir().unwrap();
                let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
                let target = root.path().join("unrequested.parquet");
                let sql = match command {
                    "copy" => format!(
                        "COPY (SELECT 1 AS id) TO '{}' STORED AS PARQUET",
                        target.display()
                    ),
                    "ddl" => "CREATE TABLE injected AS SELECT 1 AS id".into(),
                    "set" => "SET datafusion.execution.batch_size = 1".into(),
                    _ => unreachable!(),
                };
                let before = job.context.copied_config().batch_size();
                let refused = if retained {
                    job.materialize("attempt", &sql).await.is_err()
                } else {
                    match job.sql(&sql).await {
                        Ok(frame) => {
                            let _ = frame.collect().await;
                            false
                        }
                        Err(_) => true,
                    }
                };
                assert!(refused, "{command}, materialize={retained}");
                assert!(!target.exists(), "COPY made an unrequested file");
                assert!(!job.context.table_exist("injected").unwrap());
                assert_eq!(job.context.copied_config().batch_size(), before);
                assert_eq!(
                    job.sql("SELECT 1 AS id")
                        .await
                        .unwrap()
                        .count()
                        .await
                        .unwrap(),
                    1
                );
            }
        }
    }
}
