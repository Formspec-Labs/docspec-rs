//! Retained batches own native and retention reservations; either refusal spills once.
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
        ExecutionPlan, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
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
use std::{any::Any, sync::Arc};

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
        self.table.scan(session, projection, filters, limit).await
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
                // (`Batches::execute`), and the step tells the planner its
                // rows itself, so nothing reads statistics or a page index.
                // With them a wide spill's footer and index outweighed its
                // data; `spill_reader` asks for neither.
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
    let table = StreamingTable::try_new(schema, vec![partition])?;
    context.register_table(name, Arc::new(ReservedTable { table, rows, bytes }))?;
    let pool_after = budget.runtime_env().memory_pool.reserved();
    super::metrics::log_retain(
        name,
        match storage {
            StepStorage::Memory => "memory",
            StepStorage::Scratch => "scratch",
            StepStorage::View => "view",
        },
        rows,
        bytes,
        pool_before,
        pool_after,
    );
    Ok((storage, rows, bytes))
}

/// Reads back step `step`'s spill file through the handle its writer still holds.
///
/// `file` must be the file the step's spill writer has just closed: that, not
/// the handle, is why its footer is charged to the shared budget without being
/// judged by `max_metadata_bytes` (see [`crate::storage::admit_spill_footer`]).
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
    scan_retained(spill_reader(budget), file.path(), Arc::new(memory)).await
}

/// Reads a Parquet file the engine keeps beyond one statement: an export or a retained phase.
pub(super) async fn read_retained(
    budget: &Budget,
    path: &std::path::Path,
) -> Result<datafusion::dataframe::DataFrame> {
    let memory = Arc::new(crate::storage::admit_parquet_footer(path, budget).await?);
    scan_retained(retained_reader(budget), path, memory).await
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

/// [`retained_reader`] for a spill file: the one place its read options are set.
///
/// The spill writer leaves no statistics and no page index, and a replay is
/// the whole file with no predicate, so the reader asks for neither.
fn spill_reader(budget: &Budget) -> SessionContext {
    let reader = retained_reader(budget);
    {
        let state = reader.state_ref();
        let mut state = state.write();
        let execution = &mut state.config_mut().options_mut().execution;
        execution.collect_statistics = false;
        execution.parquet.enable_page_index = false;
        execution.parquet.pruning = false;
        execution.parquet.pushdown_filters = false;
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
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

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
    }
}
