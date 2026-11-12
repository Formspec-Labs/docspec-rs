//! Retained batches own native and retention reservations; either refusal spills once.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::{Session, TableProvider, streaming::StreamingTable},
    common::{Statistics, internal_err, stats::Precision},
    config::ConfigOptions,
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
        Some(Statistics::new_unknown(&self.schema()).with_num_rows(Precision::Exact(self.rows)))
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
/// `partition_statistics` publishes the step's row count (capped by any pushed
/// `fetch`) as `Precision::Inexact` without `total_byte_size`, so DataFusion's
/// `JoinSelection` swaps a larger left input to build on the smaller step while
/// `EnforceDistribution` preserves round-robin repartitioning over small steps.
/// The optimizer sees a leaf, so it can put nothing between the two.
#[derive(Debug)]
pub(super) struct RetainedScan {
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
        if partition.is_some_and(|p| p != 0) {
            return self.input.partition_statistics(partition);
        }
        let rows = self.fetch().map_or(self.rows, |fetch| fetch.min(self.rows));
        Ok(Statistics::new_unknown(&self.schema()).with_num_rows(Precision::Inexact(rows)))
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
    let (storage, rows, bytes, _) = materialize_with_joins(job, name, sql).await?;
    Ok((storage, rows, bytes))
}

pub(super) async fn materialize_with_joins(
    job: &super::Job,
    name: &str,
    sql: &str,
) -> Result<(StepStorage, usize, usize, Vec<super::metrics::JoinSummary>)> {
    if job.context.table_exist(name)? {
        return Err(Error::refusal("invalid_spec", "named step already exists"));
    }
    let frame = job.sql(sql).await?;
    let schema = super::plan::declared_schema(&frame, &job.budget)?;
    let (plan, stream) = super::plan::execute_plan_stream(frame).await?;
    let (storage, rows, bytes) = retain(&job.context, &job.budget, name, schema, stream).await?;
    Ok((storage, rows, bytes, summarize_joins(&plan)))
}

/// Summarizes each executed `HashJoinExec` in `plan`, naming its build step and row count.
pub(super) fn summarize_joins(plan: &Arc<dyn ExecutionPlan>) -> Vec<super::metrics::JoinSummary> {
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    fn retained_scan_under(plan: &Arc<dyn ExecutionPlan>) -> Option<(&str, usize)> {
        fn collect<'a>(plan: &'a Arc<dyn ExecutionPlan>, out: &mut Vec<(&'a str, usize)>) {
            if let Some(scan) = plan.as_any().downcast_ref::<RetainedScan>() {
                out.push((scan.step.as_ref(), scan.rows));
                return;
            }
            for child in plan.children() {
                collect(child, out);
            }
        }
        let mut found = Vec::new();
        collect(plan, &mut found);
        match found.as_slice() {
            [one] => Some(*one),
            _ => None,
        }
    }
    fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<super::metrics::JoinSummary>) {
        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let scanned = retained_scan_under(join.left());
            let build_rows = join
                .metrics()
                .and_then(|metrics| metrics.sum_by_name("build_input_rows"))
                .map(|value| value.as_usize() as u64);
            let partition_mode = match join.partition_mode() {
                PartitionMode::CollectLeft => "CollectLeft",
                PartitionMode::Partitioned => "Partitioned",
                PartitionMode::Auto => "Auto",
            };
            out.push(super::metrics::JoinSummary {
                join_type: join.join_type().to_string(),
                partition_mode: partition_mode.to_owned(),
                build_step: scanned.map(|(step, _)| step.to_owned()),
                build_rows,
            });
        }
        for child in plan.children() {
            walk(child, out);
        }
    }
    let mut out = Vec::new();
    walk(plan, &mut out);
    out
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
    let mut spill_metadata: Option<(usize, tokio::sync::OwnedSemaphorePermit)> = None;
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
            let mut permit = budget
                .reserve(budget.limits.max_batch_bytes * 2)
                .map_err(|error| named_spill_capacity(name, error))?;
            let metadata_unit = spill_metadata_unit(&schema)?;
            let mut metadata_permit = budget
                .reserve(0)
                .map_err(|error| named_spill_capacity(name, error))?;
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
                super::encoding::write_bounded(
                    &mut spill,
                    &previous,
                    budget,
                    &mut permit,
                    budget.limits.max_batch_bytes * 2,
                    false,
                    false,
                    true,
                    Some(super::encoding::MetadataBound::Spill {
                        step: name,
                        unit: metadata_unit,
                        permit: &mut metadata_permit,
                    }),
                )
                .map_err(|error| named_spill_capacity(name, error))?;
            }
            writer_memory = Some(permit);
            spill_metadata = Some((metadata_unit, metadata_permit));
            reservation.free();
            retention.free();
            file = Some(temporary);
            writer = Some(spill);
        }
        if let Some(writer) = &mut writer {
            let permit = writer_memory.as_mut().expect("spill owns writer_memory");
            let (metadata_unit, metadata_permit) =
                spill_metadata.as_mut().expect("spill owns spill_metadata");
            super::encoding::write_bounded(
                writer,
                &batch,
                budget,
                permit,
                budget.limits.max_batch_bytes * 2,
                false,
                false,
                true,
                Some(super::encoding::MetadataBound::Spill {
                    step: name,
                    unit: *metadata_unit,
                    permit: metadata_permit,
                }),
            )
            .map_err(|error| named_spill_capacity(name, error))?;
        } else {
            batches.push(batch);
        }
    }
    let (storage, retained) = if let Some(writer) = writer {
        writer.close()?;
        drop(writer_memory);
        drop(spill_metadata);
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
        storage,
    };
    context.register_table(name, Arc::new(table))?;
    let pool_after = budget.runtime_env().memory_pool.reserved();
    super::metrics::log_retain(name, storage.label(), rows, bytes, pool_before, pool_after);
    Ok((storage, rows, bytes))
}

/// Upper bound on one flushed spill row group's live `RowGroupMetaData`.
///
/// Spills disable statistics and offset indexes (`EnabledStatistics::None`,
/// `set_offset_index_disabled(true)`), and `ColumnDescPtr` is shared across row
/// groups, so each flushed group holds `RowGroupMetaData`, its
/// `Vec<ColumnChunkMetaData>`, and per-column `PageEncodingStats` plus `Vec`
/// growth slack.
fn spill_metadata_unit(schema: &arrow::datatypes::Schema) -> Result<usize> {
    let descriptor = parquet::arrow::ArrowSchemaConverter::new().convert(schema)?;
    Ok(descriptor.columns().iter().fold(
        std::mem::size_of::<parquet::file::metadata::RowGroupMetaData>().saturating_add(256),
        |bytes, _| {
            bytes
                .saturating_add(std::mem::size_of::<
                    parquet::file::metadata::ColumnChunkMetaData,
                >())
                .saturating_add(96)
        },
    ))
}

fn named_spill_capacity(step: &str, error: Error) -> Error {
    match error.code() {
        "capacity" if !error.message().contains("spilled step ") => Error::refusal(
            "capacity",
            format!("spilled step {step}: {}", error.message()),
        ),
        _ => error,
    }
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
    let reader = retained_reader(budget, |options| {
        options.execution.collect_statistics = false;
    });
    scan_retained(reader, file.path(), Arc::new(memory)).await
}

/// Reads a Parquet file the engine keeps beyond one statement: an export or a retained phase.
///
/// `configure` sets the reader's options beyond [`retained_reader`]'s, so each
/// caller says what its read keeps: an export drops each column's own
/// metadata (DataFusion's default, `outputs.<name>` as it has always been read)
/// and a managed job's reopened step keeps it (`Job::reopen_retained`).
pub(super) async fn read_retained(
    budget: &Budget,
    path: &std::path::Path,
    configure: impl FnOnce(&mut ConfigOptions),
) -> Result<datafusion::dataframe::DataFrame> {
    let memory = Arc::new(crate::storage::admit_parquet_footer(path, budget).await?);
    scan_retained(retained_reader(budget, configure), path, memory).await
}

/// A reader for the engine's own Parquet files, its options then set by `configure`.
///
/// A frame from the job's own catalog would retain the table it backs and
/// form a cycle. A fresh context shares the budget's runtime, not a catalog.
fn retained_reader(budget: &Budget, configure: impl FnOnce(&mut ConfigOptions)) -> SessionContext {
    let reader = budget.context();
    {
        let shared = reader.state_ref();
        let mut state = shared.write();
        let options = state.config_mut().options_mut();
        // Private materializations advertise their already normalized portable schema.
        options.execution.parquet.schema_force_view_types = false;
        configure(options);
    }
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

    fn first_hash_join(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> &datafusion::physical_plan::joins::HashJoinExec {
        if let Some(join) = plan
            .as_any()
            .downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>()
        {
            return join;
        }
        for child in plan.children() {
            if let Some(join) = find_hash_join(child) {
                return join;
            }
        }
        panic!(
            "expected HashJoinExec in:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    fn find_hash_join(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<&datafusion::physical_plan::joins::HashJoinExec> {
        if let Some(join) = plan
            .as_any()
            .downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>()
        {
            return Some(join);
        }
        plan.children().into_iter().find_map(find_hash_join)
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
    async fn left_join_builds_on_the_smaller_retained_step() {
        use datafusion::{common::JoinType, physical_plan::joins::PartitionMode};
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        for (name, rows) in [
            ("big", 300_000),
            ("twin", 300_000),
            ("mid", 150_000),
            ("few", 3),
            ("trio", 3),
        ] {
            retain_keys(&job, name, rows).await;
        }
        let cases = [
            (
                "big",
                "mid",
                JoinType::Right,
                PartitionMode::Partitioned,
                "mid",
                150_000,
            ),
            (
                "big",
                "few",
                JoinType::Right,
                PartitionMode::CollectLeft,
                "few",
                3,
            ),
            (
                "mid",
                "big",
                JoinType::Left,
                PartitionMode::Partitioned,
                "mid",
                150_000,
            ),
            (
                "big",
                "twin",
                JoinType::Left,
                PartitionMode::Partitioned,
                "big",
                300_000,
            ),
            (
                "few",
                "trio",
                JoinType::Left,
                PartitionMode::CollectLeft,
                "few",
                3,
            ),
        ];
        for (left, right, expected_type, expected_mode, expected_build, expected_rows) in cases {
            let sql = format!(
                "SELECT {left}.k, {right}.v FROM {left} LEFT JOIN {right} ON {left}.k = {right}.k"
            );
            let plan = physical_plan(&job, &sql).await;
            let join = first_hash_join(&plan);
            assert_eq!(
                (*join.join_type(), *join.partition_mode()),
                (expected_type, expected_mode),
                "{sql}\n{}",
                displayable(plan.as_ref()).indent(true)
            );
            let build_leaf = scan(join.left());
            let expected_label = format!("step={expected_build}, step_rows={expected_rows}");
            assert!(
                line(build_leaf).contains(&expected_label),
                "{sql}\n{}",
                displayable(plan.as_ref()).indent(true)
            );
        }

        // `Inexact` row counts keep round-robin repartition over small steps when
        // parallel operators benefit from partitioning.
        let agg = physical_plan(
            &job,
            "SELECT k % 7 AS g, count(*) AS n FROM few GROUP BY k % 7",
        )
        .await;
        let agg_text = displayable(agg.as_ref()).indent(true).to_string();
        assert!(
            agg_text.contains("RoundRobinBatch(2)"),
            "expected round-robin repartition over `few`:\n{agg_text}"
        );
    }

    #[tokio::test]
    async fn scan_statistics_follow_the_pushed_limit_and_projection() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        retain_keys(&job, "few", 3).await;

        let provider = job.context.table_provider("few").await.unwrap();
        let table_stats = provider
            .statistics()
            .expect("retained table publishes row count");
        assert_eq!(table_stats.num_rows, Precision::Exact(3));
        assert_eq!(table_stats.total_byte_size, Precision::Absent);
        assert_eq!(table_stats.column_statistics.len(), 2);

        let plan = physical_plan(&job, "SELECT * FROM few").await;
        let leaf = scan(&plan);
        let full = leaf.partition_statistics(None).unwrap();
        assert_eq!(full.num_rows, Precision::Inexact(3));
        assert_eq!(full.total_byte_size, Precision::Absent);
        assert_eq!(full.column_statistics.len(), 2);
        assert_eq!(
            leaf.partition_statistics(Some(0)).unwrap().num_rows,
            Precision::Inexact(3)
        );
        assert!(leaf.partition_statistics(Some(1)).is_err());

        let limited = leaf.with_fetch(Some(1)).unwrap();
        let limited_stats = limited.partition_statistics(None).unwrap();
        assert_eq!(limited_stats.num_rows, Precision::Inexact(1));
        assert_eq!(limited_stats.total_byte_size, Precision::Absent);

        let capped = leaf.with_fetch(Some(10)).unwrap();
        assert_eq!(
            capped.partition_statistics(None).unwrap().num_rows,
            Precision::Inexact(3)
        );

        let v = (col("v", &leaf.schema()).unwrap(), "v".to_owned());
        let projection = ProjectionExec::try_new([v], leaf.clone()).unwrap();
        let projected = leaf
            .try_swapping_with_projection(&projection)
            .unwrap()
            .unwrap();
        let projected_stats = projected.partition_statistics(None).unwrap();
        assert_eq!(projected_stats.num_rows, Precision::Inexact(3));
        assert_eq!(projected_stats.total_byte_size, Precision::Absent);
        assert_eq!(projected_stats.column_statistics.len(), 1);
    }

    #[tokio::test]
    async fn swapped_joins_return_the_rows_of_the_join_as_written() {
        use datafusion::{common::JoinType, physical_plan::joins::PartitionMode};
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        job.materialize(
            "lhs",
            "SELECT CASE WHEN value % 5 = 0 THEN NULL ELSE value END AS k, \
             value * 10 AS lv FROM generate_series(1, 20) AS t(value)",
        )
        .await
        .unwrap();
        job.materialize(
            "rhs",
            "SELECT CASE WHEN value = 5 THEN NULL ELSE value * 2 END AS k, \
             value * 100 AS rv FROM generate_series(1, 5) AS t(value)",
        )
        .await
        .unwrap();

        // Build an uncounted reference session (`StreamingTable` directly, so no join swaps).
        let uncounted = Job::new(
            Limits {
                compute_threads: 2,
                ..Limits::default()
            },
            root.path().join("scratch_uncounted"),
        )
        .unwrap();
        for name in ["lhs", "rhs"] {
            let batches = job
                .sql(&format!("SELECT * FROM {name}"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let schema = batches[0].schema();
            let partition = Arc::new(Batches {
                schema: schema.clone(),
                retained: Arc::new(StepData::Memory {
                    batches,
                    _reservation: MemoryConsumer::new("ref")
                        .register(&uncounted.budget.runtime_env().memory_pool),
                    _retention: MemoryConsumer::new("ref_ret")
                        .register(uncounted.budget.retention_pool()),
                }),
            });
            let bare = StreamingTable::try_new(schema, vec![partition]).unwrap();
            uncounted
                .context
                .register_table(name, Arc::new(bare))
                .unwrap();
        }

        let mut queries = Vec::new();
        for (a, b) in [("lhs", "rhs"), ("rhs", "lhs")] {
            for kind in ["INNER", "LEFT", "RIGHT", "FULL"] {
                queries.push(format!(
                    "SELECT {a}.k AS ak, {a}.*, {b}.* FROM {a} {kind} JOIN {b} ON {a}.k = {b}.k"
                ));
            }
            for kind in ["LEFT SEMI", "LEFT ANTI"] {
                queries.push(format!(
                    "SELECT {a}.* FROM {a} {kind} JOIN {b} ON {a}.k = {b}.k"
                ));
            }
            queries.push(format!(
                "SELECT * FROM {a} WHERE k NOT IN (SELECT k FROM {b})"
            ));
        }

        // Null-aware anti join (`NOT IN` over nullable keys) must never swap.
        let null_anti =
            physical_plan(&job, "SELECT * FROM lhs WHERE k NOT IN (SELECT k FROM rhs)").await;
        let join = first_hash_join(&null_anti);
        assert_eq!(
            (*join.join_type(), *join.partition_mode(), join.null_aware),
            (JoinType::LeftAnti, PartitionMode::CollectLeft, true)
        );

        async fn sorted_rows(job: &Job, sql: &str) -> Vec<arrow::row::OwnedRow> {
            let frame = job.sql(sql).await.unwrap();
            let schema = Arc::new(frame.schema().as_arrow().clone());
            let batches = frame.collect().await.unwrap();
            let converter = arrow::row::RowConverter::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| arrow::row::SortField::new(field.data_type().clone()))
                    .collect(),
            )
            .unwrap();
            let mut rows = Vec::new();
            for batch in &batches {
                let converted = converter.convert_columns(batch.columns()).unwrap();
                rows.extend(converted.iter().map(|row| row.owned()));
            }
            rows.sort_unstable();
            rows
        }

        for sql in &queries {
            let actual = sorted_rows(&job, sql).await;
            let expected = sorted_rows(&uncounted, sql).await;
            assert!(actual == expected, "mismatch for {sql}");
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
