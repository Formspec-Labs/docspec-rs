//! Native snapshot streams connected to maintained DataFusion execution.
// Rust guideline compliant 2026-02-21

mod sequence;
#[doc(inline)]
pub(crate) use sequence::sequence;

use crate::{Result, budget::Budget, storage};
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Statistics, stats::Precision},
    datasource::source::{DataSource, DataSourceExec},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_expr::{EquivalenceProperties, Partitioning, projection::ProjectionExprs},
    physical_plan::{
        DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, coop::make_cooperative,
        limit::GlobalLimitExec, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use futures::{StreamExt, TryStreamExt};
use iceberg::table::Table;
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

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
        source: Arc::new(NativeSource::Table {
            table: native,
            located,
            projection: None,
            _projection_memory: None,
        }),
        schema: schema.clone(),
        budget,
        rows: rows.and_then(|rows| usize::try_from(rows).ok()),
        filters: Vec::new(),
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
    let projection_memory = columns
        .map(|columns| {
            storage::reserve_projection(
                &budget,
                scan.schema(),
                columns.len(),
                columns
                    .len()
                    .saturating_mul(size_of::<usize>())
                    .saturating_add(size_of::<Vec<usize>>()),
            )
        })
        .transpose()?;
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
        source: Arc::new(NativeSource::File {
            scan,
            path,
            projection,
            _projection_memory: projection_memory,
        }),
        schema: schema.clone(),
        budget,
        rows,
        filters: Vec::new(),
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        use datafusion::logical_expr::TableProviderFilterPushDown;
        let status = if self.0.is_located() {
            TableProviderFilterPushDown::Unsupported
        } else {
            TableProviderFilterPushDown::Inexact
        };
        Ok(vec![status; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // DataFusion applies residual filters before query limits. Compose its
        // provider-relative indices before the native reader opens payloads.
        let partition = match projection {
            Some(indices) => Arc::new(self.0.project(indices).map_err(external)?),
            None => self.0.clone(),
        };
        let partition = if !partition.is_located() && !filters.is_empty() {
            Arc::new(partition.with_filters(filters))
        } else {
            partition
        };
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(partition));
        if limit.is_some() {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, limit));
        }
        Ok(plan)
    }
}

#[derive(Debug)]
enum NativeSource {
    Table {
        table: Table,
        located: bool,
        projection: Option<Vec<usize>>,
        _projection_memory: Option<OwnedSemaphorePermit>,
    },
    File {
        scan: Arc<storage::OccurrenceScan>,
        path: Option<String>,
        projection: Option<Vec<usize>>,
        _projection_memory: Option<OwnedSemaphorePermit>,
    },
}

#[derive(Debug)]
struct NativePartition {
    source: Arc<NativeSource>,
    schema: SchemaRef,
    budget: Budget,
    /// A count from the selected pin or captured files, never a payload estimate.
    rows: Option<usize>,
    filters: Vec<Expr>,
}

impl NativePartition {
    fn is_located(&self) -> bool {
        match self.source.as_ref() {
            NativeSource::Table { located, .. } => *located,
            NativeSource::File { scan, .. } => scan.is_located(),
        }
    }

    fn with_filters(&self, filters: &[Expr]) -> Self {
        let mut next = self.filters.clone();
        next.extend(filters.iter().cloned());
        Self {
            source: self.source.clone(),
            schema: self.schema.clone(),
            budget: self.budget.clone(),
            rows: self.rows,
            filters: next,
        }
    }

    fn project(&self, indices: &[usize]) -> Result<Self> {
        let memory = storage::reserve_projection(
            &self.budget,
            &self.schema,
            indices.len(),
            indices
                .len()
                .saturating_mul(size_of::<usize>())
                .saturating_add(size_of::<Vec<usize>>()),
        )?;
        // Arrow validates every provider-relative index before composition.
        let schema = Arc::new(self.schema.project(indices)?);
        let existing = match self.source.as_ref() {
            NativeSource::Table { projection, .. } | NativeSource::File { projection, .. } => {
                projection.as_deref()
            }
        };
        let projection = Some(
            indices
                .iter()
                .map(|index| existing.map_or(*index, |existing| existing[*index]))
                .collect(),
        );
        let source = match self.source.as_ref() {
            NativeSource::Table { table, located, .. } => NativeSource::Table {
                table: table.clone(),
                located: *located,
                projection,
                _projection_memory: Some(memory),
            },
            NativeSource::File { scan, path, .. } => NativeSource::File {
                scan: scan.clone(),
                path: path.clone(),
                projection,
                _projection_memory: Some(memory),
            },
        };
        Ok(Self {
            source: Arc::new(source),
            schema,
            budget: self.budget.clone(),
            rows: self.rows,
            filters: self.filters.clone(),
        })
    }
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
        let projected = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        write!(
            f,
            "NativeScan: rows={:?}, projection=[{}]",
            self.rows,
            projected.join(", ")
        )
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
        let filters = self.filters.clone();
        let prepare = async move {
            let stream = match source.as_ref() {
                NativeSource::Table {
                    table,
                    located: true,
                    projection,
                    ..
                } => storage::read_projected_occurrences(table, &budget, projection.as_deref())
                    .await?
                    .boxed(),
                NativeSource::Table {
                    table,
                    located: false,
                    projection,
                    ..
                } => storage::read_projected_batches(table, &budget, projection.as_deref())
                    .await?
                    .boxed(),
                NativeSource::File {
                    scan,
                    path: Some(path),
                    projection,
                    ..
                } => {
                    // Keyed lookup: one probe per registered file must not walk the
                    // whole captured population (O(files^2) across an Update).
                    if !scan.is_located()
                        && !filters.is_empty()
                        && let Some(file) = scan.file(path)
                        && !file_matches_inexact_filters_with_partition(
                            scan.native_schema(),
                            scan.partition_spec(),
                            file,
                            &filters,
                        )
                    {
                        futures::stream::empty().boxed()
                    } else {
                        scan.read_projected_file(path, &budget, projection.as_deref())?
                            .boxed()
                    }
                }
                NativeSource::File {
                    scan, path: None, ..
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
                        .filter(|file| {
                            scan.is_located()
                                || filters.is_empty()
                                || file_matches_inexact_filters_with_partition(
                                    scan.native_schema(),
                                    scan.partition_spec(),
                                    file,
                                    &filters,
                                )
                        })
                        .map(|file| file.file_path().to_owned())
                        .collect::<Vec<_>>();
                    // The next reader opens only after the preceding stream reaches
                    // EOF and releases its buffers. Keep path admission until drop.
                    let source = source.clone();
                    futures::stream::iter(paths)
                        .map(move |path| {
                            let _memory = &memory;
                            let NativeSource::File {
                                scan, projection, ..
                            } = source.as_ref()
                            else {
                                unreachable!("captured file source remains unchanged");
                            };
                            scan.read_projected_file(&path, &budget, projection.as_deref())
                        })
                        .try_flatten()
                        .boxed()
                }
            };
            // Keep the mapping and its reservation alive until EOF or drop.
            Ok::<_, crate::Error>(
                stream
                    .map(move |batch| {
                        let _source = &source;
                        batch
                    })
                    .boxed(),
            )
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

fn untruncated_file_bounds(
    file: &iceberg::spec::DataFile,
    field_id: i32,
) -> Option<(
    iceberg::spec::PrimitiveLiteral,
    iceberg::spec::PrimitiveLiteral,
)> {
    use iceberg::spec::PrimitiveLiteral;
    let low = PrimitiveLiteral::from(file.lower_bounds().get(&field_id)?.clone());
    let high = PrimitiveLiteral::from(file.upper_bounds().get(&field_id)?.clone());
    for bound in [&low, &high] {
        match bound {
            PrimitiveLiteral::String(s) if s.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => {
                return None;
            }
            PrimitiveLiteral::Binary(b) if b.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => {
                return None;
            }
            PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => return None,
            _ if bound.is_nan() => return None,
            _ => {}
        }
    }
    (low <= high).then_some((low, high))
}

fn scalar_to_full_primitive_literal(
    scalar: &datafusion::common::ScalarValue,
    expected_type: &iceberg::spec::Type,
) -> Option<iceberg::spec::PrimitiveLiteral> {
    use datafusion::common::ScalarValue;
    use iceberg::spec::{PrimitiveLiteral, PrimitiveType, Type};
    let Type::Primitive(prim) = expected_type else {
        return None;
    };
    match (scalar, prim) {
        (ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), PrimitiveType::String) => {
            Some(PrimitiveLiteral::String(s.clone()))
        }
        (ScalarValue::Binary(Some(b)), PrimitiveType::Binary) => {
            Some(PrimitiveLiteral::Binary(b.clone()))
        }
        (ScalarValue::Int32(Some(v)), PrimitiveType::Int) => Some(PrimitiveLiteral::Int(*v)),
        (ScalarValue::Int64(Some(v)), PrimitiveType::Long) => Some(PrimitiveLiteral::Long(*v)),
        (ScalarValue::Date32(Some(v)), PrimitiveType::Date) => Some(PrimitiveLiteral::Int(*v)),
        (
            ScalarValue::TimestampMicrosecond(Some(v), _),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
        ) => Some(PrimitiveLiteral::Long(*v)),
        (ScalarValue::Boolean(Some(v)), PrimitiveType::Boolean) => {
            Some(PrimitiveLiteral::Boolean(*v))
        }
        _ => None,
    }
}

fn scalar_to_primitive_literal(
    scalar: &datafusion::common::ScalarValue,
    expected_type: &iceberg::spec::Type,
) -> Option<iceberg::spec::PrimitiveLiteral> {
    use iceberg::spec::PrimitiveLiteral;
    let lit = scalar_to_full_primitive_literal(scalar, expected_type)?;
    match &lit {
        PrimitiveLiteral::String(s) if s.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        PrimitiveLiteral::Binary(b) if b.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        _ => Some(lit),
    }
}

#[allow(dead_code)]
fn file_matches_inexact_filters(
    native_schema: &iceberg::spec::Schema,
    file: &iceberg::spec::DataFile,
    filters: &[Expr],
) -> bool {
    let unpartitioned = iceberg::spec::PartitionSpec::unpartition_spec();
    file_matches_inexact_filters_with_partition(native_schema, &unpartitioned, file, filters)
}

fn file_matches_inexact_filters_with_partition(
    native_schema: &iceberg::spec::Schema,
    partition_spec: &iceberg::spec::PartitionSpec,
    file: &iceberg::spec::DataFile,
    filters: &[Expr],
) -> bool {
    filters
        .iter()
        .all(|expr| file_matches_inexact_expr(native_schema, partition_spec, file, expr))
}

fn file_matches_inexact_expr(
    native_schema: &iceberg::spec::Schema,
    partition_spec: &iceberg::spec::PartitionSpec,
    file: &iceberg::spec::DataFile,
    expr: &Expr,
) -> bool {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use iceberg::spec::{Literal, Transform};
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                file_matches_inexact_expr(native_schema, partition_spec, file, left)
                    && file_matches_inexact_expr(native_schema, partition_spec, file, right)
            }
            Operator::Or => {
                file_matches_inexact_expr(native_schema, partition_spec, file, left)
                    || file_matches_inexact_expr(native_schema, partition_spec, file, right)
            }
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                let (col, scalar, effective_op) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(scalar, _)) => (col, scalar, *op),
                    (Expr::Literal(scalar, _), Expr::Column(col)) if *op == Operator::Eq => {
                        (col, scalar, Operator::Eq)
                    }
                    _ => return true,
                };
                let Some(field) = native_schema.field_by_name(col.name()) else {
                    return true;
                };
                if let Some(full_val) = scalar_to_full_primitive_literal(scalar, &field.field_type)
                {
                    for (part_idx, pf) in partition_spec.fields().iter().enumerate() {
                        if pf.source_id != field.id {
                            continue;
                        }
                        let Some(slot) = file.partition().fields().get(part_idx) else {
                            continue;
                        };
                        let Some(Literal::Primitive(file_part)) = slot else {
                            return false;
                        };
                        let Some(target_part) = storage::compute_partition_literal(
                            pf.transform,
                            &field.field_type,
                            &full_val,
                        ) else {
                            continue;
                        };
                        let matches_part = match effective_op {
                            Operator::Eq => file_part == &target_part,
                            Operator::Lt if pf.transform == Transform::Identity => {
                                file_part < &target_part
                            }
                            Operator::Lt | Operator::LtEq
                                if storage::is_order_preserving_transform(pf.transform) =>
                            {
                                file_part <= &target_part
                            }
                            Operator::Gt if pf.transform == Transform::Identity => {
                                file_part > &target_part
                            }
                            Operator::Gt | Operator::GtEq
                                if storage::is_order_preserving_transform(pf.transform) =>
                            {
                                file_part >= &target_part
                            }
                            _ => true,
                        };
                        if !matches_part {
                            return false;
                        }
                    }
                }
                let Some((low, high)) = untruncated_file_bounds(file, field.id) else {
                    return true;
                };
                let Some(val) = scalar_to_primitive_literal(scalar, &field.field_type) else {
                    return true;
                };
                match effective_op {
                    Operator::Eq => val >= low && val <= high,
                    Operator::Lt => low < val,
                    Operator::LtEq => low <= val,
                    Operator::Gt => high > val,
                    Operator::GtEq => high >= val,
                    _ => true,
                }
            }
            _ => true,
        },
        Expr::InList(in_list)
            if !in_list.negated && !in_list.list.is_empty() && in_list.list.len() <= 200 =>
        {
            let Expr::Column(col) = in_list.expr.as_ref() else {
                return true;
            };
            let Some(field) = native_schema.field_by_name(col.name()) else {
                return true;
            };
            let mut full_candidates = Vec::with_capacity(in_list.list.len());
            let mut all_full = true;
            for item in &in_list.list {
                if let Expr::Literal(scalar, _) = item
                    && let Some(val) = scalar_to_full_primitive_literal(scalar, &field.field_type)
                {
                    full_candidates.push(val);
                } else {
                    all_full = false;
                    break;
                }
            }
            if all_full {
                for (part_idx, pf) in partition_spec.fields().iter().enumerate() {
                    if pf.source_id != field.id {
                        continue;
                    }
                    let Some(slot) = file.partition().fields().get(part_idx) else {
                        continue;
                    };
                    let Some(Literal::Primitive(file_part)) = slot else {
                        return false;
                    };
                    let mut target_parts = Vec::with_capacity(full_candidates.len());
                    let mut all_mapped = true;
                    for candidate in &full_candidates {
                        if let Some(tp) = storage::compute_partition_literal(
                            pf.transform,
                            &field.field_type,
                            candidate,
                        ) {
                            target_parts.push(tp);
                        } else {
                            all_mapped = false;
                            break;
                        }
                    }
                    if all_mapped && !target_parts.iter().any(|tp| tp == file_part) {
                        return false;
                    }
                }
            }
            let Some((low, high)) = untruncated_file_bounds(file, field.id) else {
                return true;
            };
            let mut candidates = Vec::with_capacity(in_list.list.len());
            for item in &in_list.list {
                let Expr::Literal(scalar, _) = item else {
                    return true;
                };
                let Some(val) = scalar_to_primitive_literal(scalar, &field.field_type) else {
                    return true;
                };
                candidates.push(val);
            }
            candidates.into_iter().any(|val| val >= low && val <= high)
        }
        _ => true,
    }
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
        let pin_budget = reader_budget();
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
            partition_by: Vec::new(),
        };
        let release = engine.run(&spec).await.unwrap();
        let native = storage::open_pin(&release.dataset.groups["source"], &pin_budget)
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

    fn query_budget() -> Budget {
        // Query plans retain column mappings alongside native batch reservations.
        // Keep the same process ceiling and leave room by reducing the batch cap.
        Budget::new(Limits {
            max_batch_bytes: 32 << 10,
            ..reader_budget().limits
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
            source: Arc::new(NativeSource::Table {
                table: native,
                located: false,
                projection: None,
                _projection_memory: None,
            }),
            schema,
            budget: budget.clone(),
            rows: Some(7),
            filters: Vec::new(),
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
        let pin_budget = query_budget();
        let (_root, engine, mut spec, native) = fixture().await;
        let context = SessionContext::new();
        context
            .register_table("retained", table(native, query_budget(), Some(7)).unwrap())
            .unwrap();
        spec.operation = Id::new("empty").unwrap();
        spec.sql = "SELECT * FROM input WHERE id < 0".into();
        let empty = engine.run(&spec).await.unwrap();
        context
            .register_table(
                "empty",
                table(
                    storage::open_pin(&empty.dataset.groups["source"], &pin_budget)
                        .await
                        .unwrap(),
                    query_budget(),
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
            let budget = query_budget();
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
        let context = query_budget().context();
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
        let pin_budget = query_budget();
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
        let context = query_budget().context();
        context
            .register_table("large", table(large, query_budget(), Some(7)).unwrap())
            .unwrap();
        context
            .register_table(
                "small",
                table(
                    storage::open_pin(&small, &pin_budget).await.unwrap(),
                    query_budget(),
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

#[cfg(test)]
mod pruning_tests;
#[cfg(test)]
mod selection_tests;
