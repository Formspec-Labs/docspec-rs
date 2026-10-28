//! Retained Arrow batches own their pool reservation; a refused grow spills once.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::{Session, TableProvider, streaming::StreamingTable},
    common::{Statistics, stats::Precision},
    execution::{
        TaskContext,
        disk_manager::RefCountedTempFile,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    logical_expr::{Expr, TableType},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::{StreamExt, TryStreamExt, stream};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{any::Any, fmt, sync::Arc};

/// Physical storage selected for a reusable SQL step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStorage {
    View,
    Memory,
    Scratch,
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
    rows: usize,
    bytes: usize,
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
        Ok(Arc::new(StepStatistics {
            input: self.table.scan(session, projection, filters, limit).await?,
            rows: limit.map_or(self.rows, |limit| self.rows.min(limit)),
            bytes: self.bytes,
        }))
    }
}

/// Native StreamingTableExec has no statistics hook. Reuse the counts already
/// collected during retention, without executing the stream or rereading files.
#[derive(Debug)]
struct StepStatistics {
    input: Arc<dyn ExecutionPlan>,
    rows: usize,
    bytes: usize,
}
impl DisplayAs for StepStatistics {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetainedStepStatistics")
    }
}
impl ExecutionPlan for StepStatistics {
    fn name(&self) -> &str {
        "RetainedStepStatistics"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn cardinality_effect(&self) -> datafusion::physical_plan::execution_plan::CardinalityEffect {
        datafusion::physical_plan::execution_plan::CardinalityEffect::Equal
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return datafusion::common::internal_err!("retained step statistics require one input");
        };
        Ok(Arc::new(Self {
            input: input.clone(),
            rows: self.rows,
            bytes: self.bytes,
        }))
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        if partition
            .is_some_and(|index| index >= self.input.output_partitioning().partition_count())
        {
            return datafusion::common::internal_err!("retained step partition is out of range");
        }
        let statistics = self.input.partition_statistics(partition)?;
        if partition.is_some() && self.input.output_partitioning().partition_count() != 1 {
            return Ok(statistics);
        }
        // Byte size is the retained full-row estimate, also when a projection
        // or pushed limit requests fewer bytes. Inexact estimates guide native
        // join choice; they cannot authorize exact aggregate folding.
        Ok(statistics
            .with_num_rows(Precision::Inexact(self.rows))
            .with_total_byte_size(Precision::Inexact(self.bytes)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

pub(super) async fn materialize(job: &super::Job, name: &str, sql: &str) -> Result<StepStorage> {
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
) -> Result<StepStorage> {
    let mut stream = crate::export::portable_stream(stream, schema.clone(), budget);
    let reservation =
        MemoryConsumer::new(format!("SQL step {name}")).register(&budget.runtime_env().memory_pool);
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
        if writer.is_none() && reservation.try_grow(size).is_err() {
            writer_memory = Some(budget.reserve(budget.limits.max_batch_bytes * 2)?);
            let temporary = budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("SQL named step")?;
            let props = WriterProperties::builder()
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
                    budget.limits.max_batch_bytes * 2,
                    false,
                    true,
                )?;
            }
            reservation.free();
            file = Some(temporary);
            writer = Some(spill);
        }
        if let Some(writer) = &mut writer {
            super::encoding::write(
                writer,
                &batch,
                budget,
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
            },
        )
    };
    let partition = Arc::new(Batches {
        schema: schema.clone(),
        retained: Arc::new(retained),
    });
    let table = StreamingTable::try_new(schema, vec![partition])?;
    context.register_table(name, Arc::new(ReservedTable { table, rows, bytes }))?;
    Ok(storage)
}

#[cfg(test)]
mod statistics_tests {
    use super::*;
    use crate::budget::Limits;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::common::stats::Precision;

    async fn check(name: &str, limits: Limits, batches: Vec<RecordBatch>, expected: StepStorage) {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::in_workspace(limits, root.path()).unwrap();
        let context = budget.context();
        let schema = batches[0].schema();
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let bytes = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        ));
        assert_eq!(
            retain(&context, &budget, name, stream.schema(), stream)
                .await
                .unwrap(),
            expected
        );
        let provider = context.table_provider(name).await.unwrap();
        let state = context.state();
        for projection in [None, Some(vec![0])] {
            let plan = provider
                .scan(&state, projection.as_ref(), &[], None)
                .await
                .unwrap();
            for partition in [None, Some(0)] {
                let facts = plan.partition_statistics(partition).unwrap();
                assert_eq!(facts.num_rows, Precision::Inexact(rows));
                assert_eq!(facts.total_byte_size, Precision::Inexact(bytes));
                assert_eq!(
                    facts.column_statistics.len(),
                    projection.as_ref().map_or(2, Vec::len)
                );
            }
            assert!(plan.partition_statistics(Some(1)).is_err());
            let plan = provider
                .scan(&state, projection.as_ref(), &[], Some(1))
                .await
                .unwrap();
            assert_eq!(
                plan.partition_statistics(None).unwrap().num_rows,
                Precision::Inexact(rows.min(1))
            );
        }
        let result = context
            .sql(&format!("SELECT count(*) FROM {name}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            rows as i64
        );
    }
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }
    #[tokio::test]
    async fn retained_memory_and_empty_scans_publish_inexact_rows_and_bytes() {
        let schema = schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap();
        check(
            "retained",
            Limits::default(),
            vec![batch],
            StepStorage::Memory,
        )
        .await;
        check(
            "empty",
            Limits::default(),
            vec![RecordBatch::new_empty(schema)],
            StepStorage::Memory,
        )
        .await;
    }
    #[tokio::test]
    async fn retained_scratch_scan_reuses_materialized_size_estimates() {
        let limits = Limits {
            compute_threads: 1,
            memory_bytes: 8 << 20,
            max_batch_bytes: 1 << 20,
            scratch_bytes: 32 << 20,
            ..Limits::default()
        };
        let schema = schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x".repeat(512 << 10)])),
            ],
        )
        .unwrap();
        check("spilled", limits, vec![batch; 12], StepStorage::Scratch).await;
    }
    #[tokio::test]
    async fn native_join_builds_on_fewer_bytes_even_when_that_side_has_more_rows() {
        use datafusion::{
            common::tree_node::{TreeNode, TreeNodeRecursion},
            physical_plan::joins::HashJoinExec,
        };
        let budget = Budget::new(Limits {
            compute_threads: 1,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        let schema = schema();
        for (name, rows, text) in [
            ("wide", 2, "x".repeat(64 << 10)),
            ("narrow", 64, "n".into()),
        ] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows)),
                    Arc::new(StringArray::from(vec![text; rows as usize])),
                ],
            )
            .unwrap();
            let input = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                stream::iter([Ok(batch)]),
            ));
            retain(&context, &budget, name, schema.clone(), input)
                .await
                .unwrap();
        }
        let frame = context
            .sql("SELECT wide.value FROM wide LEFT JOIN narrow ON wide.id=narrow.id")
            .await
            .unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let mut joins = 0;
        plan.apply(|plan| {
            if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                let left = join.left().partition_statistics(None)?;
                let right = join.right().partition_statistics(None)?;
                assert!(
                    left.total_byte_size.get_value().unwrap()
                        < right.total_byte_size.get_value().unwrap()
                );
                assert!(left.num_rows.get_value().unwrap() > right.num_rows.get_value().unwrap());
                joins += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(joins, 1);
        let result = datafusion::physical_plan::collect(plan, context.task_ctx())
            .await
            .unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in result {
            for value in batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
            {
                assert_eq!(value.unwrap().len(), 64 << 10);
            }
        }
    }
}
