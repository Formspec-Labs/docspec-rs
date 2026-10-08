//! Native snapshot streams connected to maintained DataFusion execution.

use crate::{Result, budget::Budget, storage};
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    common::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use futures::{StreamExt, TryStreamExt};
use iceberg::table::Table;
use std::sync::Arc;

/// Uses the same reader admission and native field interpretation as independent reads.
pub(crate) fn table(native: Table, budget: Budget) -> Result<Arc<dyn TableProvider>> {
    provider(native, budget, false)
}

pub(crate) fn occurrences(native: Table, budget: Budget) -> Result<Arc<dyn TableProvider>> {
    provider(native, budget, true)
}

fn provider(native: Table, budget: Budget, located: bool) -> Result<Arc<dyn TableProvider>> {
    let schema = if located {
        storage::occurrence_schema(&native)?
    } else {
        Arc::new(iceberg::arrow::schema_to_arrow_schema(
            native.metadata().current_schema(),
        )?)
    };
    let partition = NativePartition {
        source: NativeSource::Table {
            table: native,
            located,
        },
        schema: schema.clone(),
        budget,
    };
    Ok(Arc::new(StreamingTable::try_new(
        schema,
        vec![Arc::new(partition)],
    )?))
}

/// Replays one captured file, optionally reading only selected root columns.
pub(crate) fn file(
    scan: Arc<storage::OccurrenceScan>,
    path: String,
    columns: Option<&[String]>,
    budget: Budget,
) -> Result<Arc<dyn TableProvider>> {
    let projection = columns
        .map(|columns| {
            columns
                .iter()
                .map(|name| scan.schema().index_of(name))
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .transpose()?;
    let schema = match &projection {
        Some(indices) => Arc::new(scan.schema().project(indices)?),
        None => scan.schema().clone(),
    };
    let partition = NativePartition {
        source: NativeSource::File {
            scan,
            path,
            projection,
        },
        schema: schema.clone(),
        budget,
    };
    Ok(Arc::new(StreamingTable::try_new(
        schema,
        vec![Arc::new(partition)],
    )?))
}

#[derive(Debug, Clone)]
enum NativeSource {
    Table {
        table: Table,
        located: bool,
    },
    File {
        scan: Arc<storage::OccurrenceScan>,
        path: String,
        projection: Option<Vec<usize>>,
    },
}

#[derive(Debug)]
struct NativePartition {
    source: NativeSource,
    schema: SchemaRef,
    budget: Budget,
}

impl PartitionStream for NativePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let source = self.source.clone();
        let budget = self.budget.clone();
        let prepare = async move {
            let stream = match source {
                NativeSource::Table {
                    table,
                    located: true,
                } => storage::read_occurrences(&table, &budget).await?.boxed(),
                NativeSource::Table {
                    table,
                    located: false,
                } => storage::read_batches(&table, &budget).await?.boxed(),
                NativeSource::File {
                    scan,
                    path,
                    projection,
                } => scan
                    .read_projected_file(&path, &budget, projection.as_deref())?
                    .boxed(),
            };
            Ok::<_, crate::Error>(stream)
        };
        let prepare = async move {
            prepare
                .await
                .map(|stream| stream.map_err(external))
                .map_err(external)
        };
        let stream = futures::stream::once(prepare).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

fn external(error: crate::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Applies final consumer admission after joins combine independently bounded inputs.
pub(crate) fn bounded(
    stream: SendableRecordBatchStream,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let schema = stream.schema();
    let reservation = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    let limit = budget.limits.max_batch_bytes;
    let stream = futures::stream::try_unfold(
        (stream, reservation),
        move |(mut stream, reservation)| async move {
            let Some(batch) = stream.try_next().await? else {
                return Ok(None);
            };
            if batch.get_array_memory_size() > limit {
                return Err(external(crate::Error::refusal(
                    "oversized_batch",
                    "combined result exceeds the consumer batch allowance",
                )));
            }
            Ok(Some((batch, (stream, reservation))))
        },
    );
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Engine,
        budget::Limits,
        model::{Id, RunSpec},
    };
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    async fn fixture() -> (TempDir, Engine, RunSpec, Table) {
        let root = TempDir::new().unwrap();
        let input = root.path().join("input.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                None,
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                compute_threads: 2,
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let spec = RunSpec {
            base: None,
            relationship: None,
            dataset: Id::new("query").unwrap(),
            operation: Id::new("initial").unwrap(),
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        };
        let release = engine.run(&spec).await.unwrap();
        let native = storage::open_pin(&release.dataset.groups["source"])
            .await
            .unwrap();
        (root, engine, spec, native)
    }

    fn reader_budget() -> Budget {
        Budget::new(Limits {
            batch_rows: 2,
            max_batch_bytes: 64 << 10,
            memory_bytes: 256 << 10,
            ..Limits::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn native_query_stream_keeps_admission_until_drop_and_enforces_batch_rows() {
        let (_root, _engine, _spec, native) = fixture().await;
        let budget = reader_budget();
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema()).unwrap(),
        );
        let partition = NativePartition {
            source: NativeSource::Table {
                table: native,
                located: false,
            },
            schema,
            budget: budget.clone(),
        };
        let context = SessionContext::new();
        let mut stream = partition.execute(context.task_ctx());
        assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 2);
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        let mut competing = partition.execute(context.task_ctx());
        let error = competing.try_next().await.unwrap_err();
        let error = crate::Error::from(DataFusionError::Context(
            "comparison".into(),
            Box::new(error),
        ));
        assert_eq!(error.code(), "capacity");
        assert!(error.to_string().contains("comparison"));
        drop(competing);
        assert!(budget.reserve(1).is_err());
        drop(stream);
        drop(budget.reserve(128 << 10).unwrap());
        let mut retry = partition.execute(context.task_ctx());
        let mut rows = 0;
        while let Some(batch) = retry.try_next().await.unwrap() {
            assert!(batch.num_rows() <= 2);
            rows += batch.num_rows();
        }
        assert_eq!(rows, 7);
        drop(budget.reserve(128 << 10).unwrap());
    }

    #[tokio::test]
    async fn query_provider_preserves_pins_filters_duplicates_and_empty_schema() {
        let (_root, engine, mut spec, native) = fixture().await;
        let context = SessionContext::new();
        context
            .register_table("retained", table(native, reader_budget()).unwrap())
            .unwrap();
        spec.operation = Id::new("empty").unwrap();
        spec.sql = "SELECT * FROM input WHERE id < 0".into();
        let empty = engine.run(&spec).await.unwrap();
        context
            .register_table(
                "empty",
                table(
                    storage::open_pin(&empty.dataset.groups["source"])
                        .await
                        .unwrap(),
                    reader_budget(),
                )
                .unwrap(),
            )
            .unwrap();
        for _ in 0..2 {
            let batches = context
                .sql("SELECT id AS renamed FROM retained WHERE id IS NULL OR id = 1")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut values: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                })
                .collect();
            values.sort();
            assert_eq!(values, [None, Some(1), Some(1)]);
        }
        let empty = context.sql("SELECT id FROM empty").await.unwrap();
        assert_eq!(empty.schema().fields()[0].name(), "id");
        assert!(
            empty
                .collect()
                .await
                .unwrap()
                .iter()
                .all(|batch| batch.num_rows() == 0)
        );
    }

    #[tokio::test]
    async fn query_reader_refuses_oversized_native_batches_and_releases_capacity() {
        let (_root, _engine, _spec, native) = fixture().await;
        let budget = Budget::new(Limits {
            max_batch_bytes: 1,
            memory_bytes: 256 << 10,
            batch_rows: 2,
            ..Limits::default()
        })
        .unwrap();
        let context = SessionContext::new();
        context
            .register_table("retained", table(native, budget.clone()).unwrap())
            .unwrap();
        let error = context
            .sql("SELECT * FROM retained")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        let error = crate::Error::from(error);
        assert_eq!(error.code(), "oversized_batch");
        assert!(
            error
                .to_string()
                .contains("native reader exceeded its byte allowance")
        );
        drop(budget.reserve(128 << 10).unwrap());
    }

    // Capability probe for the pinned native dependency; see the retained failure log.
    #[tokio::test]
    async fn pinned_native_position_column_is_unavailable() {
        let (_root, _engine, _spec, native) = fixture().await;
        let mut stream = native
            .scan()
            .with_concurrency_limit(1)
            .select(["id", "_file", "_pos"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("field not found")
        );
    }
}
