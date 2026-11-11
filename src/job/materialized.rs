//! Retained batches own native and retention reservations; either refusal spills once.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::{Session, TableProvider, streaming::StreamingTable},
    common::{Statistics, internal_err, stats::Precision},
    execution::{
        TaskContext,
        disk_manager::RefCountedTempFile,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    logical_expr::{Expr, TableType},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        metrics::MetricsSet, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
        streaming::PartitionStream,
    },
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::{StreamExt, TryStreamExt, stream};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{any::Any, fmt, sync::Arc};

/// Physical storage selected for a reusable SQL step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStorage {
    View,
    Memory,
    Scratch,
}
impl StepStorage {
    /// The name the step and retain logs and query plans print.
    ///
    /// It is the serde name too, which readers of a job's result.json depend on.
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Memory => "memory",
            Self::Scratch => "scratch",
        }
    }
}

#[derive(Debug)]
struct Batches {
    schema: SchemaRef,
    retained: Arc<StepData>,
}

#[derive(Debug)]
enum StepData {
    Memory {
        batches: Vec<RecordBatch>,
        _reservation: MemoryReservation,
        _retention: MemoryReservation,
    },
    Scratch {
        frame: Box<datafusion::dataframe::DataFrame>,
        _file: RefCountedTempFile,
    },
}
impl PartitionStream for Batches {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        // Keep the reservation in the stream, including after the final yield.
        let retained = self.retained.clone();
        match retained.as_ref() {
            StepData::Memory { batches, .. } => {
                let count = batches.len();
                let batches = stream::iter(0..count).map(move |index| {
                    let StepData::Memory { batches, .. } = retained.as_ref() else {
                        unreachable!()
                    };
                    Ok(batches[index].clone())
                });
                Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
            }
            StepData::Scratch { frame, .. } => {
                let frame = frame.as_ref().clone();
                let batches = stream::once(async move { frame.execute_stream().await })
                    .try_flatten()
                    .map(move |batch| {
                        let _keep_alive = &retained;
                        batch
                    });
                Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
            }
        }
    }
}

#[derive(Debug)]
struct ReservedTable {
    table: StreamingTable,
    step: Arc<str>,
    rows: usize,
    bytes: usize,
    storage: StepStorage,
}
#[async_trait::async_trait]
impl TableProvider for ReservedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    fn statistics(&self) -> Option<Statistics> {
        Some(
            Statistics::new_unknown(&self.schema())
                .with_num_rows(Precision::Exact(self.rows))
                .with_total_byte_size(Precision::Exact(self.bytes)),
        )
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RetainedScan {
            input: self.table.scan(session, projection, filters, limit).await?,
            step: self.step.clone(),
            rows: self.rows,
            storage: self.storage,
        }))
    }
}

/// The scan of one retained step, its line in a plan naming the step.
///
/// DataFusion's streaming scan prints no relation, so a plan did not say which
/// of a job's steps a leaf read or how large it was. This leaf is that scan
/// with the step's name, rows and storage appended to its line, as in
/// `StreamingTableExec: partition_sizes=1, projection=[k], step=big,
/// step_rows=300000, storage=memory`. The scan's own name and text stay
/// first, so plan readers that match `StreamingTableExec` still do, and the
/// count is `step_rows`, which no reader can take for a metric such as
/// `output_rows`.
///
/// All else is the scan's: its projection, limit and partition (which keeps
/// the step's reservation or scratch file alive), properties, metrics and
/// statistics. The planner sees what it saw before, and no row count. Given
/// one, DataFusion's join selection swaps a larger left input, which moves
/// build sides and memory; that change is to be made, and measured, on its
/// own. The optimizer sees a leaf, so it can put nothing between the two.
#[derive(Debug)]
struct RetainedScan {
    input: Arc<dyn ExecutionPlan>,
    step: Arc<str>,
    rows: usize,
    storage: StepStorage,
}
impl RetainedScan {
    /// This step's scan again, over `input` with a projection or limit pushed into it.
    fn over(&self, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input,
            step: self.step.clone(),
            rows: self.rows,
            storage: self.storage,
        })
    }
}
impl DisplayAs for RetainedScan {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.input.fmt_as(format, f)?;
        let (step, rows, storage) = (&self.step, self.rows, self.storage.label());
        // The tree format takes one `key=value` a line.
        if matches!(format, DisplayFormatType::TreeRender) {
            write!(f, "\nstep={step}\nstep_rows={rows}\nstorage={storage}")
        } else {
            write!(f, ", step={step}, step_rows={rows}, storage={storage}")
        }
    }
}
impl ExecutionPlan for RetainedScan {
    fn name(&self) -> &str {
        self.input.name()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("a retained step's scan has no inputs to replace")
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        self.input.partition_statistics(partition)
    }
    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(self.over(self.input.with_fetch(limit)?))
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let input = self.input.try_swapping_with_projection(projection)?;
        Ok(input.map(|input| self.over(input)))
    }
}

pub(super) async fn materialize(
    job: &super::Job,
    name: &str,
    sql: &str,
) -> Result<(StepStorage, usize, usize)> {
    if job.context.table_exist(name)? {
        return Err(Error::refusal("invalid_spec", "named step already exists"));
    }
    let frame = job.sql(sql).await?;
    let schema =
        crate::export::portable_schema(&super::plan::inferred_schema(&frame, &job.budget)?);
    let stream = super::plan::execute_stream(frame).await?;
    retain(&job.context, &job.budget, name, schema, stream).await
}

pub(super) async fn retain(
    context: &SessionContext,
    budget: &Budget,
    name: &str,
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
) -> Result<(StepStorage, usize, usize)> {
    let pool_before = budget.runtime_env().memory_pool.reserved();
    let mut stream = crate::export::portable_stream(stream, schema.clone(), budget);
    let reservation =
        MemoryConsumer::new(format!("SQL step {name}")).register(&budget.runtime_env().memory_pool);
    let retention =
        MemoryConsumer::new(format!("SQL retention {name}")).register(budget.retention_pool());
    let mut batches = Vec::new();
    let mut rows = 0;
    let mut bytes = 0;
    let mut file = None;
    let mut writer_memory = None;
    let mut writer: Option<ArrowWriter<crate::budget::ScratchWriter>> = None;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        let size = batch.get_array_memory_size();
        if size > budget.limits.max_batch_bytes {
            return Err(Error::refusal(
                "capacity",
                "named step batch exceeds configured byte allowance",
            ));
        }
        bytes += size;
        if writer.is_none()
            && (retention.try_grow(size).is_err() || reservation.try_grow(size).is_err())
        {
            let mut permit = budget.reserve(budget.limits.max_batch_bytes * 2)?;
            let temporary = budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("SQL named step")?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                // Compute batches need not become individual physical groups.
                // Keep the native default row ceiling and the byte ceiling.
                .set_max_row_group_bytes(Some((budget.limits.max_batch_bytes / 4).max(1)))
                .set_write_batch_size(budget.limits.batch_rows.min(32))
                .set_dictionary_enabled(false)
                // A spill is replayed whole, in order and with no predicate
                // (`Batches::execute`), and the step counts its own rows
                // (`ReservedTable`), so nothing reads statistics or a page index.
                // With them a wide spill's footer and index outweighed its
                // data; `read_spill` asks for no statistics.
                .set_statistics_enabled(EnabledStatistics::None)
                .set_offset_index_disabled(true)
                .build();
            let mut spill = ArrowWriter::try_new(
                budget.scratch_writer(&temporary)?,
                schema.clone(),
                Some(props),
            )?;
            for previous in batches.drain(..) {
                super::encoding::write(
                    &mut spill,
                    &previous,
                    budget,
                    &mut permit,
                    budget.limits.max_batch_bytes * 2,
                    false,
                    true,
                )?;
            }
            writer_memory = Some(permit);
            reservation.free();
            retention.free();
            file = Some(temporary);
            writer = Some(spill);
        }
        if let Some(writer) = &mut writer {
            let permit = writer_memory.as_mut().expect("spill owns writer_memory");
            super::encoding::write(
                writer,
                &batch,
                budget,
                permit,
                budget.limits.max_batch_bytes * 2,
                false,
                true,
            )?;
        } else {
            batches.push(batch);
        }
    }
    let (storage, retained) = if let Some(writer) = writer {
        writer.close()?;
        drop(writer_memory);
        let file = file.expect("spill owns temporary file");
        let frame = read_spill(budget, &file, name).await?;
        (
            StepStorage::Scratch,
            StepData::Scratch {
                frame: Box::new(frame),
                _file: file,
            },
        )
    } else {
        (
            StepStorage::Memory,
            StepData::Memory {
                batches,
                _reservation: reservation,
                _retention: retention,
            },
        )
    };
    let partition = Arc::new(Batches {
        schema: schema.clone(),
        retained: Arc::new(retained),
    });
    let table = ReservedTable {
        table: StreamingTable::try_new(schema, vec![partition])?,
        step: name.into(),
        rows,
        bytes,
        storage,
    };
    context.register_table(name, Arc::new(table))?;
    let pool_after = budget.runtime_env().memory_pool.reserved();
    super::metrics::log_retain(name, storage.label(), rows, bytes, pool_before, pool_after);
    Ok((storage, rows, bytes))
}

/// Reads back step `step`'s spill file through the handle its writer still holds.
///
/// `file` must be the file the step's spill writer has just closed: that, not
/// the handle, is why its footer is charged to the shared budget without being
/// judged by `max_metadata_bytes` (see [`crate::storage::admit_spill_footer`]).
///
/// The scan collects no statistics: a replay is the whole file in order
/// (`Batches::execute`) and the step counts its own rows (`ReservedTable`).
/// No other reader option is set, because none can act:
/// pruning, page-index and filter-pushdown options need a predicate, and none
/// reaches this scan (`ReservedTable` accepts no filters, and the frame runs
/// as built here). The file has no page index either.
///
/// # Errors
/// Refuses with `capacity`, naming the step, when the shared budget cannot
/// hold the footer's charge.
async fn read_spill(
    budget: &Budget,
    file: &RefCountedTempFile,
    step: &str,
) -> Result<datafusion::dataframe::DataFrame> {
    let memory = crate::storage::admit_spill_footer(file, budget)
        .await
        .map_err(|error| match error.code() {
            "capacity" => Error::refusal(
                "capacity",
                format!(
                    "spilled step {step}: the shared budget cannot hold its file's footer: {}",
                    error.message()
                ),
            ),
            _ => error,
        })?;
    let reader = retained_reader(budget);
    reader
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .collect_statistics = false;
    scan_retained(reader, file.path(), Arc::new(memory)).await
}

/// Reads a completed export back, for later outputs and queries.
///
/// Its columns lose their own metadata, a lake column's `PARQUET:field_id`
/// among it, as DataFusion's Parquet reader skips it by default: plain and
/// managed jobs alike have always read `outputs.<name>` so.
pub(super) async fn read_retained(
    budget: &Budget,
    path: &std::path::Path,
) -> Result<datafusion::dataframe::DataFrame> {
    let memory = Arc::new(crate::storage::admit_parquet_footer(path, budget).await?);
    scan_retained(retained_reader(budget), path, memory).await
}

/// Reopens a managed job's retained step with each column's metadata.
///
/// A plain job keeps a step as planned, so its later steps and exports see
/// each column's metadata, a lake column's `PARQUET:field_id` among it. A
/// managed job writes the step to a file and reopens it here. DataFusion's
/// Parquet reader skips top-level column metadata by default
/// (`skip_metadata`), which gave a managed job's exports the plain job's rows
/// without their field ids: different footers, so different files. Read with
/// it, the step's schema is the one written, and later steps plan as in the
/// plain job.
pub(super) async fn read_retained_step(
    budget: &Budget,
    path: &std::path::Path,
) -> Result<datafusion::dataframe::DataFrame> {
    let memory = Arc::new(crate::storage::admit_parquet_footer(path, budget).await?);
    let reader = retained_reader(budget);
    reader
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .parquet
        .skip_metadata = false;
    scan_retained(reader, path, memory).await
}

/// A reader for the engine's own Parquet files.
///
/// A frame from the job's own catalog would retain the table it backs and
/// form a cycle. A fresh context shares the budget's runtime, not a catalog.
fn retained_reader(budget: &Budget) -> SessionContext {
    let reader = budget.context();
    // Private materializations advertise their already normalized portable schema.
    reader
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    reader
}

/// Scans `path` whole through `reader`, keeping its footer admission with the scan.
async fn scan_retained(
    reader: SessionContext,
    path: &std::path::Path,
    memory: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<datafusion::dataframe::DataFrame> {
    let frame = reader
        .read_parquet(
            path.to_string_lossy().as_ref(),
            ParquetReadOptions::default().file_extension(""),
        )
        .await?;
    let datafusion::logical_expr::LogicalPlan::TableScan(scan) = frame.logical_plan() else {
        return Err(Error::refusal(
            "invalid_spec",
            "retained Parquet did not produce a native file scan",
        ));
    };
    let provider = datafusion::datasource::source_as_provider(&scan.source)?;
    Ok(reader.read_table(Arc::new(super::fitting::FittedTable {
        provider,
        metadata_memory: Some(memory),
    }))?)
}

#[cfg(test)]
mod spill_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{budget::Limits, job::Job};
    use arrow::{
        array::{AsArray, Int64Array},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{physical_expr::expressions::col, physical_plan::displayable};

    /// A job on two partitions, as the CLI runs queries, with room to keep steps in memory.
    fn job(root: &tempfile::TempDir) -> Job {
        let limits = Limits {
            compute_threads: 2,
            ..Limits::default()
        };
        Job::new(limits, root.path().join("scratch")).unwrap()
    }

    /// Retains `rows` consecutive keys `k`, each with a value `v`, as step `name`.
    async fn retain_keys(job: &Job, name: &str, rows: usize) {
        let sql = format!(
            "SELECT value AS k, value * 2 AS v FROM generate_series(1, {rows}) AS t(value)"
        );
        assert_eq!(
            job.materialize(name, &sql).await.unwrap(),
            StepStorage::Memory
        );
    }

    async fn physical_plan(job: &Job, sql: &str) -> Arc<dyn ExecutionPlan> {
        let frame = job.sql(sql).await.unwrap();
        frame.create_physical_plan().await.unwrap()
    }

    /// Each query's physical plan as text, every operator with its statistics.
    async fn plans(job: &Job, queries: &[String]) -> Vec<String> {
        let mut plans = Vec::new();
        for sql in queries {
            let plan = physical_plan(job, sql).await;
            let text = displayable(plan.as_ref()).set_show_statistics(true);
            plans.push(text.indent(true).to_string());
        }
        plans
    }

    /// The physical plan of `sql` as `EXPLAIN` returns it.
    async fn explain(job: &Job, sql: &str) -> String {
        let frame = job.sql(&format!("EXPLAIN {sql}")).await.unwrap();
        let batches = frame.collect().await.unwrap();
        let plans = batches.iter().flat_map(|batch| {
            let plan = batch.column(1).as_string::<i32>();
            (0..batch.num_rows()).map(move |row| plan.value(row).to_owned())
        });
        plans.collect()
    }

    /// The scan beneath `plan`, following each operator's first input.
    fn scan(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
        plan.children().first().copied().map_or(plan, scan)
    }

    /// `plan`'s own line of an `EXPLAIN`.
    fn line(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).one_line().to_string()
    }

    /// A retained step that scans as before steps were named: the bare streaming scan.
    #[derive(Debug)]
    struct Unnamed(Arc<dyn TableProvider>);
    #[async_trait::async_trait]
    impl TableProvider for Unnamed {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
        fn table_type(&self) -> TableType {
            self.0.table_type()
        }
        fn statistics(&self) -> Option<Statistics> {
            self.0.statistics()
        }
        async fn scan(
            &self,
            session: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let step = self.0.as_any().downcast_ref::<ReservedTable>();
            let step = step.expect("a retained step");
            step.table.scan(session, projection, filters, limit).await
        }
    }

    #[tokio::test]
    async fn explain_names_the_step_a_scan_reads() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        retain_keys(&job, "few", 3).await;
        retain_keys(&job, "zero", 0).await;
        // The streaming scan's own text comes first: plan readers match it.
        let few = "StreamingTableExec: partition_sizes=1, projection=[v], fetch=2, \
                   step=few, step_rows=3, storage=memory";
        let zero = "StreamingTableExec: partition_sizes=1, projection=[v], \
                    step=zero, step_rows=0, storage=memory";
        for (sql, scan) in [
            ("SELECT v FROM few LIMIT 2", few),
            ("SELECT v FROM zero", zero),
        ] {
            let text = explain(&job, sql).await;
            assert!(text.contains(scan), "{text}");
        }
    }

    #[test]
    fn storage_labels_are_their_serde_names() {
        // Readers of a job's result.json, the pipeline's among them, read these.
        for storage in [StepStorage::View, StepStorage::Memory, StepStorage::Scratch] {
            assert_eq!(serde_json::to_value(storage).unwrap(), storage.label());
        }
    }

    #[tokio::test]
    async fn plans_over_retained_steps_match_the_bare_scans() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        // Given row counts, DataFusion's join selection would swap a strictly
        // larger left input: `big LEFT JOIN mid` would build on mid,
        // partitioned (both are over its 131,072-row single-partition
        // ceiling), and `big LEFT JOIN few` on few, collected. Without them
        // every join builds on its left input as written. The other shapes
        // cover each join operator and type, a window, an aggregate, a sort
        // under a limit and a union. Queries are planned, not run.
        let steps = [
            ("big", 300_000),
            ("twin", 300_000),
            ("mid", 150_000),
            ("few", 3),
            ("trio", 3),
        ];
        for (name, rows) in steps {
            retain_keys(&job, name, rows).await;
        }
        let pairs = [
            ("big", "mid"),
            ("big", "few"),
            ("mid", "big"),
            ("big", "twin"),
            ("few", "trio"),
        ];
        let mut queries = Vec::from(pairs.map(|(left, right)| {
            format!(
                "SELECT {left}.k, {right}.v FROM {left} LEFT JOIN {right} ON {left}.k = {right}.k"
            )
        }));
        for kind in ["INNER", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"] {
            queries.push(format!(
                "SELECT * FROM big {kind} JOIN few ON big.k = few.k"
            ));
        }
        queries.extend(
            [
                "SELECT big.k, few.v FROM big JOIN few ON big.k < few.k",
                "SELECT big.k, few.v FROM big CROSS JOIN few",
                "SELECT k FROM big WHERE k NOT IN (SELECT k FROM few)",
                "SELECT k, row_number() OVER (PARTITION BY k % 7 ORDER BY v) AS n FROM big",
                "SELECT k % 7 AS g, count(*) AS n FROM big GROUP BY k % 7",
                "SELECT k, v FROM big ORDER BY v DESC LIMIT 5",
                "SELECT k FROM big UNION ALL SELECT k FROM few",
            ]
            .map(String::from),
        );
        let named = plans(&job, &queries).await;
        for (name, _) in steps {
            let step = job.context.deregister_table(name).unwrap().unwrap();
            job.context
                .register_table(name, Arc::new(Unnamed(step)))
                .unwrap();
        }
        let bare = plans(&job, &queries).await;
        for ((sql, named), bare) in queries.iter().zip(named).zip(bare) {
            // Every scan names its step, and nothing else in the plan moved,
            // the statistics of every operator included.
            let scans = named.matches("StreamingTableExec:").count();
            assert!(scans > 0, "{sql}\n{named}");
            assert_eq!(named.matches(", step=").count(), scans, "{sql}\n{named}");
            let unnamed = steps.iter().fold(named, |text, (step, rows)| {
                let label = format!(", step={step}, step_rows={rows}, storage=memory");
                text.replace(&label, "")
            });
            assert_eq!(unnamed, bare, "{sql}");
        }
    }

    #[tokio::test]
    async fn pushed_limits_and_projections_keep_the_step_named() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        retain_keys(&job, "few", 3).await;
        // The physical optimizer can push a limit or a projection into a
        // planned scan. The step's name must survive both.
        let plan = physical_plan(&job, "SELECT * FROM few").await;
        let leaf = scan(&plan);
        let limited = leaf.with_fetch(Some(1)).expect("the scan takes a limit");
        let v = (col("v", &leaf.schema()).unwrap(), "v".to_owned());
        let projection = ProjectionExec::try_new([v], leaf.clone()).unwrap();
        let projected = leaf.try_swapping_with_projection(&projection).unwrap();
        let projected = projected.expect("the scan takes a projection");
        for (pushed, text) in [
            (limited, "projection=[k, v], fetch=1"),
            (projected, "projection=[v]"),
        ] {
            let line = line(&pushed);
            let named = format!("{text}, step=few, step_rows=3, storage=memory");
            assert!(line.contains(&named), "{line}");
        }
    }

    #[tokio::test]
    async fn scratch_batches_do_not_reacquire_retained_memory() {
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            memory_bytes: 8 << 20,
            max_batch_bytes: 64 << 10,
            batch_rows: 1024,
            scratch_bytes: 8 << 20,
            ..Limits::default()
        };
        let job = Job::new(limits.clone(), root.path().join("scratch")).unwrap();
        let pool = job.budget.runtime_env().memory_pool.clone();
        // Another operator forces the first batch to scratch, then releases
        // its memory while the rest of the input continues streaming.
        let pressure = MemoryConsumer::new("other operator").register(&pool);
        pressure.try_grow(limits.memory_bytes / 2).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..1024))],
        )
        .unwrap();
        let input = stream::iter(0..3).map(move |index| {
            if index == 1 {
                pressure.free();
            }
            if index > 0 {
                assert_eq!(
                    pool.reserved(),
                    0,
                    "scratch batches must leave native memory available"
                );
            }
            Ok(batch.clone())
        });
        let input = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), input));
        let (storage, rows, _) = retain(&job.context, &job.budget, "spilled", schema, input)
            .await
            .unwrap();
        assert_eq!(storage, StepStorage::Scratch);
        assert_eq!(rows, 3072);
        assert_eq!(
            job.sql("SELECT * FROM spilled")
                .await
                .unwrap()
                .count()
                .await
                .unwrap(),
            rows
        );
        let text = explain(&job, "SELECT * FROM spilled").await;
        let scan = "step=spilled, step_rows=3072, storage=scratch";
        assert!(text.contains(scan), "{text}");
    }
}
