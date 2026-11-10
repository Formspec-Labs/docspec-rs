//! Retained batches own native and retention reservations; either refusal spills once.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
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
        display::ProjectSchemaDisplay, metrics::MetricsSet, projection::ProjectionExec,
        stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::{StreamExt, TryStreamExt, stream};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
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
    /// The name the retain log and query plans print.
    const fn label(self) -> &'static str {
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

/// What a retained step tells the planner: its exact rows, capped by a pushed limit.
///
/// No byte size is given, although the one `retain` counts is accurate: 1.00
/// to 1.01 times Arrow's own count on 14 step tables of a pipeline build,
/// since `portable_batch` compacts what it retains. It is a size for whole
/// rows, and a scan usually reads a few columns. Split by column count it
/// came to 0.61 to 2.47 times the join keys' real bytes on three of those
/// tables, and DataFusion's join selection compares byte sizes before row
/// counts when both sides give one, so such a share would overrule the exact
/// count. Publishing bytes needs them kept per column.
fn statistics(schema: &Schema, rows: usize, fetch: Option<usize>) -> Statistics {
    let rows = fetch.map_or(rows, |fetch| fetch.min(rows));
    Statistics::new_unknown(schema).with_num_rows(Precision::Exact(rows))
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
        Some(statistics(&self.schema(), self.rows, None))
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

/// The scan of one retained step, with the row count and name its plan lacked.
///
/// DataFusion's streaming scan reports no statistics and prints no relation.
/// Join selection reads only the physical plan's statistics, so every join
/// built on its left input as written, and a plan did not say which step a
/// leaf read. The scan itself stays `input`'s work: its projection, its limit,
/// and the partition that keeps the step's reservation or scratch file alive.
/// The optimizer sees a leaf, so it can put nothing between the two.
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
        let (step, rows, storage) = (&self.step, self.rows, self.storage.label());
        // The tree format takes one `key=value` a line and prints no node name.
        let tree = matches!(format, DisplayFormatType::TreeRender);
        if tree {
            write!(f, "step={step}\nrows={rows}\nstorage={storage}")?;
        } else {
            write!(
                f,
                "RetainedScan: step={step}, rows={rows}, storage={storage}"
            )?;
            let schema = self.schema();
            if !schema.fields().is_empty() {
                write!(f, ", projection={}", ProjectSchemaDisplay(&schema))?;
            }
        }
        match self.fetch() {
            Some(fetch) if tree => write!(f, "\nfetch={fetch}"),
            Some(fetch) => write!(f, ", fetch={fetch}"),
            None => Ok(()),
        }
    }
}
impl ExecutionPlan for RetainedScan {
    fn name(&self) -> &str {
        "RetainedScan"
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
            internal_err!("RetainedScan has no inputs to replace")
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
    /// One partition holds the whole step, so its statistics are the plan's.
    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        Ok(statistics(&self.schema(), self.rows, self.fetch()))
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
    let schema = crate::export::portable_schema(frame.schema().as_arrow());
    let stream = frame.execute_stream().await?;
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
                .set_max_row_group_row_count(Some(budget.limits.batch_rows))
                .set_max_row_group_bytes(Some((budget.limits.max_batch_bytes / 4).max(1)))
                .set_write_batch_size(budget.limits.batch_rows.min(32))
                .set_dictionary_enabled(false)
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
        // A frame from the job's own catalog would retain this table and form
        // a cycle. A fresh context shares its runtime, not its catalog.
        let reader = budget.context();
        // This private table advertises the already normalized spill schema.
        // Keep the native reader in that representation instead of changing
        // Utf8 fields back to views underneath the streaming table.
        reader
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = false;
        let frame = reader
            .read_parquet(
                file.path().to_string_lossy().as_ref(),
                ParquetReadOptions::default().file_extension(""),
            )
            .await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{budget::Limits, job::Job};
    use arrow::{
        array::{Array, AsArray, Int64Array},
        datatypes::{DataType, Field, Int64Type, Schema},
    };
    use datafusion::{
        common::JoinType,
        physical_expr::expressions::col,
        physical_plan::{
            collect, displayable,
            joins::{HashJoinExec, PartitionMode},
            projection::ProjectionExec,
        },
    };

    /// A key that may be null, and a value that tells rows with equal keys apart.
    type Row = (Option<i64>, i64);
    const BIG: [Row; 7] = [
        (Some(1), 10),
        (Some(2), 20),
        (Some(2), 21),
        (None, 30),
        (Some(4), 40),
        (None, 31),
        (Some(6), 60),
    ];
    const SMALL: [Row; 4] = [(Some(2), 200), (Some(2), 201), (None, 300), (Some(7), 700)];

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

    /// The first operator of type `T` in `plan`, outermost first.
    fn find<T: 'static>(plan: &Arc<dyn ExecutionPlan>) -> Option<&T> {
        let found = plan.as_any().downcast_ref();
        found.or_else(|| plan.children().into_iter().find_map(find))
    }

    /// The scan beneath `plan`, following each operator's first input.
    fn scan(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
        plan.children().first().copied().map_or(plan, scan)
    }

    /// `plan`'s own line of an `EXPLAIN`.
    fn line(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).one_line().to_string()
    }

    /// The rows of `left <kind> JOIN right ON left.k = right.k`, sorted, by nested loop.
    fn joined(kind: &str, left: &[Row], right: &[Row]) -> Vec<Vec<Option<i64>>> {
        let matches = |l: &Row, r: &Row| l.0.is_some() && l.0 == r.0;
        let side = |row: Option<&Row>| row.map_or([None, None], |row| [row.0, Some(row.1)]);
        let pair = |l: Option<&Row>, r: Option<&Row>| [side(l), side(r)].concat();
        let mut rows = Vec::new();
        for l in left {
            let matched = right.iter().filter(|r| matches(l, r)).collect::<Vec<_>>();
            match kind {
                "LEFT SEMI" | "LEFT ANTI" => {
                    if matched.is_empty() == (kind == "LEFT ANTI") {
                        rows.push(side(Some(l)).to_vec());
                    }
                }
                _ => {
                    rows.extend(matched.iter().map(|r| pair(Some(l), Some(*r))));
                    if matched.is_empty() && matches!(kind, "LEFT" | "FULL") {
                        rows.push(pair(Some(l), None));
                    }
                }
            }
        }
        if matches!(kind, "RIGHT" | "FULL") {
            let unmatched = |r: &&Row| !left.iter().any(|l| matches(l, r));
            rows.extend(right.iter().filter(unmatched).map(|r| pair(None, Some(r))));
        }
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn left_join_builds_on_the_smaller_retained_step() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        // `big` and `mid` are over DataFusion's 131,072-row ceiling for a
        // single-partition build, so their join stays partitioned, as the
        // pipeline's large joins do. `few` is under it.
        for (name, rows) in [("big", 300_000), ("mid", 150_000), ("few", 3)] {
            retain_keys(&job, name, rows).await;
        }
        for (small, mode) in [
            ("mid", PartitionMode::Partitioned),
            ("few", PartitionMode::CollectLeft),
        ] {
            let sql =
                format!("SELECT big.k, {small}.v FROM big LEFT JOIN {small} ON big.k = {small}.k");
            let plan = physical_plan(&job, &sql).await;
            let text = displayable(plan.as_ref()).indent(true).to_string();
            let join = find::<HashJoinExec>(&plan).expect(&text);
            // A hash join builds on its left input: the join was swapped.
            assert!(
                line(scan(join.left())).contains(&format!("step={small},")),
                "{text}"
            );
            assert_eq!(
                (join.join_type(), join.partition_mode()),
                (&JoinType::Right, &mode),
                "{text}"
            );
        }
    }

    #[tokio::test]
    async fn swapped_joins_return_the_rows_of_the_join_as_written() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        let steps = [("big", &BIG[..]), ("small", &SMALL[..])];
        for (name, rows) in steps {
            let values = rows.iter().map(|(k, v)| match k {
                Some(k) => format!("({k}, {v})"),
                None => format!("(NULL, {v})"),
            });
            let values = values.collect::<Vec<_>>().join(", ");
            let sql = format!("SELECT * FROM (VALUES {values}) AS t(k, v)");
            job.materialize(name, &sql).await.unwrap();
        }
        for kind in ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"] {
            for (left, right) in [(steps[0], steps[1]), (steps[1], steps[0])] {
                let sql = format!(
                    "SELECT * FROM {} AS l {kind} JOIN {} AS r ON l.k = r.k",
                    left.0, right.0
                );
                let frame = job.sql(&sql).await.unwrap();
                let task = Arc::new(frame.task_ctx());
                let plan = frame.create_physical_plan().await.unwrap();
                let text = displayable(plan.as_ref()).indent(true).to_string();
                let batches = collect(plan.clone(), task).await.unwrap();
                let cell = |batch: &RecordBatch, column: usize, row| {
                    let values = batch.column(column).as_primitive::<Int64Type>();
                    values.is_valid(row).then(|| values.value(row))
                };
                let mut rows = Vec::new();
                for batch in &batches {
                    rows.extend((0..batch.num_rows()).map(|row| {
                        let cells = (0..batch.num_columns()).map(|column| cell(batch, column, row));
                        cells.collect::<Vec<_>>()
                    }));
                }
                rows.sort();
                assert_eq!(rows, joined(kind, left.1, right.1), "{sql}\n{text}");
                // Whichever step is written first, the smaller one is the build side.
                let join = find::<HashJoinExec>(&plan).expect(&text);
                let build = line(scan(join.left()));
                assert!(build.contains("step=small,"), "{sql}\n{text}");
            }
        }
    }

    #[tokio::test]
    async fn scan_statistics_follow_the_pushed_limit_and_projection() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        retain_keys(&job, "few", 3).await;
        let rows_and_columns = |plan: &Arc<dyn ExecutionPlan>| {
            let statistics = plan.partition_statistics(None).unwrap();
            assert_eq!(
                statistics.column_statistics.len(),
                plan.schema().fields().len(),
                "{}",
                line(plan)
            );
            (statistics.num_rows, statistics.column_statistics.len())
        };
        // (query, rows its scan returns, columns its scan returns)
        for (sql, rows, columns) in [
            ("SELECT * FROM few", 3, 2),
            ("SELECT v FROM few", 3, 1),
            ("SELECT v FROM few LIMIT 2", 2, 1),
            ("SELECT * FROM few LIMIT 7", 3, 2),
        ] {
            let plan = physical_plan(&job, sql).await;
            let leaf = scan(&plan);
            assert_eq!(leaf.fetch().is_some(), sql.contains("LIMIT"), "{sql}");
            assert_eq!(
                rows_and_columns(leaf),
                (Precision::Exact(rows), columns),
                "{sql}"
            );
        }
        // The physical optimizer can also push a limit or a projection into a
        // planned scan. The step's name and count must survive both.
        let plan = physical_plan(&job, "SELECT * FROM few").await;
        let leaf = scan(&plan);
        let limited = leaf.with_fetch(Some(1)).expect("the scan takes a limit");
        assert_eq!(rows_and_columns(&limited), (Precision::Exact(1), 2));
        let v = (col("v", &leaf.schema()).unwrap(), "v".to_owned());
        let projection = ProjectionExec::try_new([v], leaf.clone()).unwrap();
        let projected = leaf.try_swapping_with_projection(&projection).unwrap();
        let projected = projected.expect("the scan takes a projection");
        assert_eq!(rows_and_columns(&projected), (Precision::Exact(3), 1));
        for pushed in [limited, projected] {
            let line = line(&pushed);
            assert!(line.contains("step=few, rows=3,"), "{line}");
        }
    }

    #[tokio::test]
    async fn explain_names_the_step_a_scan_reads() {
        let root = tempfile::tempdir().unwrap();
        let job = job(&root);
        retain_keys(&job, "few", 3).await;
        let text = explain(&job, "SELECT v FROM few LIMIT 2").await;
        let scan = "RetainedScan: step=few, rows=3, storage=memory, projection=[v], fetch=2";
        assert!(text.contains(scan), "{text}");
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
        let scan = "RetainedScan: step=spilled, rows=3072, storage=scratch";
        assert!(text.contains(scan), "{text}");
    }
}
