//! Native snapshot streams connected to maintained DataFusion execution.

use crate::{Result, budget::Budget, storage};
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Statistics, stats::Precision},
    datasource::source::{DataSource, DataSourceExec},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_expr::{
        EquivalenceProperties, Partitioning, expressions::Column, projection::ProjectionExprs,
    },
    physical_plan::{
        DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, coop::make_cooperative,
        limit::GlobalLimitExec, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
        streaming::PartitionStream,
    },
};
use futures::{StreamExt, TryStreamExt};
use iceberg::table::Table;
use std::{any::Any, fmt, sync::Arc};

/// Uses the same reader admission and native field interpretation as independent reads.
/// Supply rows only from the same selected pin; unpinned schema candidates use None.
pub(crate) fn table(
    native: Table,
    budget: Budget,
    rows: Option<u64>,
) -> Result<Arc<dyn TableProvider>> {
    provider(native, budget, false, rows)
}

pub(crate) fn occurrences(
    native: Table,
    budget: Budget,
    rows: Option<u64>,
) -> Result<Arc<dyn TableProvider>> {
    provider(native, budget, true, rows)
}

fn provider(
    native: Table,
    budget: Budget,
    located: bool,
    rows: Option<u64>,
) -> Result<Arc<dyn TableProvider>> {
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
        rows: rows.and_then(|rows| usize::try_from(rows).ok()),
    };
    Ok(Arc::new(NativeTable(Arc::new(partition))))
}

/// Replays one captured file, optionally reading only selected root columns.
pub(crate) fn file(
    scan: Arc<storage::OccurrenceScan>,
    path: String,
    columns: Option<&[String]>,
    budget: Budget,
) -> Result<Arc<dyn TableProvider>> {
    projected_files(scan, Some(path), columns, budget)
}

/// Streams captured files sequentially so reader buffers do not grow with file count.
pub(crate) fn files(
    scan: Arc<storage::OccurrenceScan>,
    columns: Option<&[String]>,
    budget: Budget,
) -> Result<Arc<dyn TableProvider>> {
    projected_files(scan, None, columns, budget)
}

fn projected_files(
    scan: Arc<storage::OccurrenceScan>,
    path: Option<String>,
    columns: Option<&[String]>,
    budget: Budget,
) -> Result<Arc<dyn TableProvider>> {
    let rows = match &path {
        Some(path) => usize::try_from(scan.file_rows(path).ok_or_else(|| {
            crate::Error::refusal("occurrence_tasks", "file is outside the captured source")
        })?)
        .ok(),
        None => scan.files().try_fold(0usize, |rows, file| {
            rows.checked_add(usize::try_from(file.record_count()).ok()?)
        }),
    };
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
        rows,
    };
    Ok(Arc::new(NativeTable(Arc::new(partition))))
}

#[derive(Debug)]
struct NativeTable(Arc<NativePartition>);

#[async_trait::async_trait]
impl TableProvider for NativeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Filters remain unsupported here, so DataFusion applies residual filters
        // before any query limit. Reuse maintained projection and limit operators.
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(self.0.clone()));
        if let Some(indices) = projection {
            let projected = self.0.schema.project(indices)?;
            let columns = indices
                .iter()
                .zip(projected.fields())
                .map(|(index, field)| {
                    (
                        Arc::new(Column::new(field.name(), *index))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        field.name().clone(),
                    )
                });
            plan = Arc::new(ProjectionExec::try_new(columns, plan)?);
        }
        if limit.is_some() {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, limit));
        }
        Ok(plan)
    }
}

#[derive(Debug, Clone)]
enum NativeSource {
    Table {
        table: Table,
        located: bool,
    },
    File {
        scan: Arc<storage::OccurrenceScan>,
        path: Option<String>,
        projection: Option<Vec<usize>>,
    },
}

#[derive(Debug)]
struct NativePartition {
    source: NativeSource,
    schema: SchemaRef,
    budget: Budget,
    /// A count from the selected pin or captured files, never a payload estimate.
    rows: Option<usize>,
}

impl DataSource for NativePartition {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "native scan has one partition".into(),
            ));
        }
        Ok(make_cooperative(PartitionStream::execute(self, context)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _kind: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NativeScan: rows={:?}", self.rows)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.schema.clone())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        if partition.is_some_and(|partition| partition != 0) {
            return Err(DataFusionError::Execution(
                "native scan has one partition".into(),
            ));
        }
        // Counts guide build-side selection. Budget also guards the pinned
        // aggregate-statistics rewrite: inexactness alone cannot prevent later
        // operators from promoting an estimate. Decoded byte sizes stay unknown.
        Ok(Statistics::new_unknown(&self.schema)
            .with_num_rows(self.rows.map_or(Precision::Absent, Precision::Inexact)))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> datafusion::common::Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
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
                    path: Some(path),
                    projection,
                } => scan
                    .read_projected_file(&path, &budget, projection.as_deref())?
                    .boxed(),
                NativeSource::File {
                    scan,
                    path: None,
                    projection,
                } => {
                    let bytes = scan.files().fold(0usize, |bytes, file| {
                        bytes
                            .saturating_add(file.file_path().len())
                            .saturating_add(size_of::<String>())
                    });
                    if bytes > budget.limits.max_metadata_bytes {
                        return Err(crate::Error::refusal(
                            "metadata_capacity",
                            "captured file paths exceed metadata allowance",
                        ));
                    }
                    let memory = budget.reserve(bytes)?;
                    let paths = scan
                        .files()
                        .map(|file| file.file_path().to_owned())
                        .collect::<Vec<_>>();
                    // The next reader opens only after the preceding stream reaches
                    // EOF and releases its buffers. Keep path admission until drop.
                    futures::stream::iter(paths)
                        .map(move |path| {
                            let _memory = &memory;
                            scan.read_projected_file(&path, &budget, projection.as_deref())
                        })
                        .try_flatten()
                        .boxed()
                }
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
            rows: Some(7),
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
            .register_table("retained", table(native, reader_budget(), Some(7)).unwrap())
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
                    Some(0),
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
            .register_table("retained", table(native, budget.clone(), Some(7)).unwrap())
            .unwrap();
        for sql in ["SELECT * FROM retained", "SELECT COUNT(*) FROM retained"] {
            let error = context.sql(sql).await.unwrap().collect().await.unwrap_err();
            let error = crate::Error::from(error);
            assert_eq!(error.code(), "oversized_batch");
            assert!(
                error
                    .to_string()
                    .contains("native reader exceeded its byte allowance")
            );
            drop(budget.reserve(128 << 10).unwrap());
        }
    }

    #[tokio::test]
    async fn native_statistics_preserve_filtered_offset_counts() {
        let (_root, _engine, spec, native) = fixture().await;
        for rows in [None, Some(7)] {
            let budget = reader_budget();
            let context = budget.context();
            context
                .register_table("retained", table(native.clone(), budget, rows).unwrap())
                .unwrap();
            let result = context.sql(
                "SELECT COUNT(*) FROM (SELECT id FROM retained WHERE length(CAST(id AS VARCHAR)) > 0 LIMIT 10 OFFSET 2)",
            ).await.unwrap().collect().await.unwrap();
            let count = result[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count, 4, "source row hint: {rows:?}");
        }
        // The session guard also covers the maintained flat-Parquet provider,
        // whose own exact input counts can become filter estimates downstream.
        let context = reader_budget().context();
        context
            .register_parquet(
                "retained",
                &spec.input,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let result = context.sql(
            "SELECT COUNT(*) FROM (SELECT id FROM retained WHERE length(CAST(id AS VARCHAR)) > 0 LIMIT 10 OFFSET 2)",
        ).await.unwrap().collect().await.unwrap();
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
        );
    }

    #[tokio::test]
    async fn native_statistics_choose_the_smaller_build_in_both_directions() {
        use datafusion::physical_plan::joins::HashJoinExec;
        fn joins(plan: &dyn ExecutionPlan, found: &mut Vec<(Precision<usize>, Precision<usize>)>) {
            if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                found.push((
                    join.left().partition_statistics(None).unwrap().num_rows,
                    join.right().partition_statistics(None).unwrap().num_rows,
                ));
            }
            for child in plan.children() {
                joins(child.as_ref(), found);
            }
        }
        let (_root, engine, mut spec, large) = fixture().await;
        spec.operation = Id::new("small").unwrap();
        spec.sql = "SELECT * FROM input WHERE id < 2".into();
        let small = engine.run(&spec).await.unwrap().dataset.groups["source"].clone();
        assert_eq!(small.rows, 2);
        let context = reader_budget().context();
        context
            .register_table("large", table(large, reader_budget(), Some(7)).unwrap())
            .unwrap();
        context
            .register_table(
                "small",
                table(
                    storage::open_pin(&small).await.unwrap(),
                    reader_budget(),
                    Some(small.rows),
                )
                .unwrap(),
            )
            .unwrap();
        for (left, right) in [("large", "small"), ("small", "large")] {
            let plan = context
                .sql(&format!(
                    "SELECT l.id FROM {left} l JOIN {right} r ON l.id = r.id"
                ))
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let mut found = Vec::new();
            joins(plan.as_ref(), &mut found);
            assert_eq!(found, vec![(Precision::Inexact(2), Precision::Inexact(7))]);
        }
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
