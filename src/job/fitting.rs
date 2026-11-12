//! Fit native scalar evaluation before its output admission.
// Rust guideline compliant 2026-02-21
use super::functions;
pub(crate) mod windows;
use arrow::{
    array::{as_boolean_array, builder::BooleanBuilder},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{
        DataFusionError, Result, Statistics,
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    execution::TaskContext,
    logical_expr::{ColumnarValue, Expr, TableProviderFilterPushDown, TableType},
    physical_expr::{
        LexOrdering, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr, expressions::Column,
        projection::Projector,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        SendableRecordBatchStream, StatisticsArgs,
        aggregates::{AggregateExec, AggregateInputMode, PhysicalGroupBy},
        filter::{FilterExec, FilterExecBuilder},
        joins::{
            HashJoinExec, HashJoinExecBuilder, NestedLoopJoinExec, NestedLoopJoinExecBuilder,
            SortMergeJoinExec, utils::JoinFilter,
        },
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        projection::ProjectionExec,
        replace_children_if_necessary,
        sorts::{
            partial_sort::PartialSortExec, sort::SortExec,
            sort_preserving_merge::SortPreservingMergeExec,
        },
        stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug)]
struct BatchSize {
    rows: usize,
}
impl fmt::Display for BatchSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "function output exceeds its byte allowance; fit {} preceding rows",
            self.rows
        )
    }
}
impl std::error::Error for BatchSize {}

pub(super) fn smaller(rows: usize) -> DataFusionError {
    DataFusionError::External(Box::new(BatchSize { rows: rows.max(1) }))
}

pub(super) fn preflight(
    rows: usize,
    limit: usize,
    sizes: impl Iterator<Item = Result<usize>>,
) -> Result<()> {
    if rows <= 1 {
        return Ok(());
    }
    let mut bytes = 0usize;
    for (index, size) in sizes.enumerate() {
        bytes = bytes.saturating_add(size?);
        if bytes > limit {
            return Err(smaller(index));
        }
    }
    Ok(())
}

fn hint(error: &DataFusionError) -> Option<usize> {
    match error.find_root() {
        DataFusionError::External(error) => {
            error.downcast_ref::<BatchSize>().map(|error| error.rows)
        }
        _ => None,
    }
}

fn bounded(expr: &Arc<dyn PhysicalExpr>) -> Result<Option<usize>> {
    let mut limit = None;
    expr.apply(|expr| {
        if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>()
            && let Some(bytes) = functions::byte_limit(function.fun())
        {
            limit = Some(limit.map_or(bytes, |old: usize| old.min(bytes)));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(limit)
}

fn logical_bounded(expr: &Expr) -> Result<bool> {
    let mut found = false;
    expr.apply(|expr| {
        if let Expr::ScalarFunction(function) = expr
            && functions::byte_limit(&function.func).is_some()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

pub(super) fn filter_support(
    provider: &dyn TableProvider,
    filters: &[&Expr],
) -> Result<Vec<TableProviderFilterPushDown>> {
    let mut support = provider.supports_filters_pushdown(filters)?;
    for (support, filter) in support.iter_mut().zip(filters) {
        if logical_bounded(filter)? {
            *support = TableProviderFilterPushDown::Unsupported;
        }
    }
    Ok(support)
}

#[derive(Debug)]
pub(super) struct FittedTable {
    pub provider: Arc<dyn TableProvider>,
    pub metadata_memory: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}
#[async_trait::async_trait]
impl TableProvider for FittedTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.provider.schema()
    }
    fn table_type(&self) -> TableType {
        self.provider.table_type()
    }
    fn statistics(&self) -> Option<Statistics> {
        self.provider.statistics()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filter_support(self.provider.as_ref(), filters)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .provider
            .scan(state, projection, filters, limit)
            .await?;
        Ok(if let Some(memory) = &self.metadata_memory {
            super::native::admit_metadata_plan(plan, memory.clone())
        } else {
            plan
        })
    }
}

#[derive(Debug)]
struct FittedProjection {
    native: Arc<dyn ExecutionPlan>,
    projector: Projector,
    properties: Arc<PlanProperties>,
    limit: usize,
    metrics: ExecutionPlanMetricsSet,
}
impl FittedProjection {
    fn new(native: Arc<dyn ExecutionPlan>, limit: usize) -> Result<Self> {
        let projection = native
            .downcast_ref::<ProjectionExec>()
            .expect("native projection");
        let mut properties = projection.properties().as_ref().clone();
        let input_schema = projection.input().schema();
        let inputs = input_schema.fields().len();
        let preserves_inputs = projection.expr().len() >= inputs
            && projection.expr()[..inputs]
                .iter()
                .enumerate()
                .all(|(index, expr)| {
                    expr.expr
                        .downcast_ref::<Column>()
                        .is_some_and(|column| column.index() == index)
                        && &expr.alias == input_schema.field(index).name()
                });
        if preserves_inputs {
            // Hoisting preserves every input column at its original index.
            // Record the computed value's equality with its new column, so
            // dropping a temporary sort key retains the original ordering.
            for (index, expr) in projection.expr().iter().enumerate().skip(inputs) {
                let mut immutable = true;
                expr.expr.apply(|expr| {
                    if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
                        immutable &= function.fun().signature().volatility
                            == datafusion::logical_expr::Volatility::Immutable;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                if immutable {
                    properties.eq_properties.add_equal_conditions(
                        expr.expr.clone(),
                        Arc::new(Column::new(&expr.alias, index)),
                    )?;
                }
            }
        }
        Ok(Self {
            projector: projection
                .projection_expr()
                .make_projector(&projection.input().schema())?,
            native,
            properties: Arc::new(properties),
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}
impl DisplayAs for FittedProjection {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ByteFittedProjection: ")?;
        self.native.fmt_as(format, f)
    }
}
impl ExecutionPlan for FittedProjection {
    fn name(&self) -> &str {
        "ByteFittedProjection"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.native.apply_expressions(f)
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.native.child_stats_requests(partition)
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.native.statistics_from_inputs(input_stats, args)
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.native.benefits_from_input_partitioning()
    }
    fn supports_limit_pushdown(&self) -> bool {
        self.native.supports_limit_pushdown()
    }
    fn cardinality_effect(&self) -> datafusion::physical_plan::execution_plan::CardinalityEffect {
        self.native.cardinality_effect()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.native.children()
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            replace_children_if_necessary(self.native.clone(), children)?,
            self.limit,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        struct State {
            input: SendableRecordBatchStream,
            projector: Projector,
            pending: Option<RecordBatch>,
            offset: usize,
            target: usize,
            limit: usize,
        }
        let state = State {
            input: self.children()[0].execute(partition, context)?,
            projector: self.projector.with_metrics(&self.metrics, partition),
            pending: None,
            offset: 0,
            target: usize::MAX,
            limit: self.limit,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if state.pending.is_none() {
                    let Some(batch) = state.input.try_next().await? else {
                        return Ok(None);
                    };
                    state.target = batch.num_rows().max(1);
                    state.offset = 0;
                    state.pending = Some(batch);
                }
                let input = state.pending.as_ref().expect("pending input");
                let take = state.target.min(input.num_rows() - state.offset);
                match state
                    .projector
                    .project_batch(&input.slice(state.offset, take))
                {
                    Ok(batch) => {
                        state.offset += take;
                        if state.offset == input.num_rows() {
                            state.pending = None;
                        }
                        if crate::storage::referenced_batch_bytes(&batch) < state.limit / 2 {
                            state.target = state.target.saturating_mul(2);
                        }
                        return Ok::<_, DataFusionError>(Some((batch, state)));
                    }
                    Err(error) => {
                        if let Some(rows) = hint(&error).filter(|_| take > 1) {
                            state.target = rows.min(take - 1).max(1);
                            tokio::task::yield_now().await;
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn fit_projection(native: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let projection = native
        .downcast_ref::<ProjectionExec>()
        .expect("native projection");
    let mut limit = None;
    for expr in projection.expr() {
        if let Some(bytes) = bounded(&expr.expr)? {
            limit = Some(limit.map_or(bytes, |old: usize| old.min(bytes)));
        }
    }
    match limit {
        Some(limit) => Ok(Arc::new(FittedProjection::new(native, limit)?)),
        None => Ok(native),
    }
}

/// Projection pushdown cannot move an admitted evaluator into a scan that
/// cannot respond to sizing hints. Keep only those projections as boundaries.
#[derive(Debug)]
struct FittingInput {
    input: Arc<dyn ExecutionPlan>,
}
impl DisplayAs for FittingInput {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FunctionFittingInput")
    }
}
impl ExecutionPlan for FittingInput {
    fn name(&self) -> &str {
        "FunctionFittingInput"
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
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }
    fn supports_limit_pushdown(&self) -> bool {
        true
    }
    fn cardinality_effect(&self) -> datafusion::physical_plan::execution_plan::CardinalityEffect {
        datafusion::physical_plan::execution_plan::CardinalityEffect::Equal
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        for expr in projection.expr() {
            if bounded(&expr.expr)?.is_some() {
                return Ok(None);
            }
        }
        // Preserve native pruning/computation that needs no fitting. Only own
        // admitted functions remain above this identity boundary.
        let projected = ProjectionExec::try_new(projection.expr().to_vec(), self.input.clone())?;
        Ok(Some(Arc::new(Self {
            input: Arc::new(projected),
        })))
    }
}

#[derive(Debug)]
pub(crate) struct FitProjections;
impl PhysicalOptimizerRule for FitProjections {
    fn name(&self) -> &str {
        "retain_function_output_admission"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|plan| {
                // The native pass first extracts single-side join expressions,
                // then pushes projections into scans in that same invocation.
                if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>()
                    && let Some(filter) = join.filter()
                    && bounded(filter.expression())?.is_some()
                {
                    let mut changed = false;
                    let children = plan
                        .children()
                        .into_iter()
                        .map(|child| {
                            if child.is::<FittingInput>() {
                                child.clone()
                            } else {
                                changed = true;
                                Arc::new(FittingInput {
                                    input: child.clone(),
                                }) as Arc<dyn ExecutionPlan>
                            }
                        })
                        .collect();
                    if changed {
                        return Ok(Transformed::yes(replace_children_if_necessary(
                            plan, children,
                        )?));
                    }
                }
                if plan.is::<ProjectionExec>() {
                    let fitted = fit_projection(plan.clone())?;
                    if !Arc::ptr_eq(&fitted, &plan) {
                        return Ok(Transformed::yes(fitted));
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }
}

/// A join consumes one boolean per candidate pair. Fit the full predicate,
/// retaining only native boolean pieces while temporary UDF buffers are freed.
/// The join keeps responsibility for matches, outer rows and its own memory.
#[derive(Debug, Eq)]
struct FittedPredicate {
    native: Arc<dyn PhysicalExpr>,
}
impl PartialEq for FittedPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.native.eq(&other.native)
    }
}
impl Hash for FittedPredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.native.hash(state);
    }
}
impl fmt::Display for FittedPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.native.fmt(f)
    }
}
impl PhysicalExpr for FittedPredicate {
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.native.data_type(schema)
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.native.nullable(schema)
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.native.fmt_sql(f)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.native]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            native: children[0].clone(),
        }))
    }
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        let mut offset = 0;
        let mut target = input.num_rows().max(1);
        let mut values = BooleanBuilder::with_capacity(input.num_rows());
        loop {
            let take = target.min(input.num_rows() - offset);
            match self.native.evaluate(&input.slice(offset, take)) {
                Ok(value) => {
                    let value = value.into_array(take)?;
                    values.append_array(as_boolean_array(value.as_ref()));
                    offset += take;
                    if offset == input.num_rows() {
                        break;
                    }
                }
                Err(error) => {
                    if let Some(rows) = hint(&error).filter(|_| take > 1) {
                        target = rows.min(take - 1).max(1);
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(values.finish())))
    }
}
fn fit_join_filter(filter: &JoinFilter) -> Result<Option<JoinFilter>> {
    if bounded(filter.expression())?.is_none() {
        return Ok(None);
    }
    Ok(Some(JoinFilter::new(
        Arc::new(FittedPredicate {
            native: filter.expression().clone(),
        }),
        filter.column_indices().to_vec(),
        filter.schema().clone(),
    )))
}

struct Hoist {
    input: Arc<dyn ExecutionPlan>,
    expressions: Vec<(Arc<dyn PhysicalExpr>, String)>,
    changed: bool,
}
impl Hoist {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let expressions = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect();
        Self {
            input,
            expressions,
            changed: false,
        }
    }
    fn expression(&mut self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        if bounded(&expr)?.is_none() {
            return Ok(expr);
        }
        let index = self.expressions.len();
        let mut suffix = index;
        let name = loop {
            let name = format!("__docspec_byte_fit_{suffix}");
            if !self.expressions.iter().any(|(_, old)| old == &name) {
                break name;
            }
            suffix += 1;
        };
        self.expressions.push((expr, name.clone()));
        self.changed = true;
        Ok(Arc::new(Column::new(&name, index)))
    }
    fn finish(self) -> Result<Arc<dyn ExecutionPlan>> {
        fit_projection(Arc::new(ProjectionExec::try_new(
            self.expressions,
            self.input,
        )?))
    }
    fn ordering(&mut self, order: &LexOrdering) -> Result<LexOrdering> {
        let expressions = order
            .iter()
            .map(|e| {
                Ok(PhysicalSortExpr {
                    expr: self.expression(e.expr.clone())?,
                    options: e.options,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LexOrdering::new(expressions).expect("nonempty native ordering"))
    }
}

#[derive(Debug)]
pub(crate) struct FitFunctions;
impl PhysicalOptimizerRule for FitFunctions {
    fn name(&self) -> &str {
        "fit_function_output"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|plan| {
                if plan.is::<ProjectionExec>() {
                    return Ok(Transformed::yes(fit_projection(plan)?));
                }
                if let Some(join) = plan.downcast_ref::<HashJoinExec>()
                    && let Some(filter) = join.filter()
                    && let Some(filter) = fit_join_filter(filter)?
                {
                    return Ok(Transformed::yes(Arc::new(
                        HashJoinExecBuilder::from(join)
                            .with_filter(Some(filter))
                            .build()?,
                    )
                        as Arc<dyn ExecutionPlan>));
                }
                if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>()
                    && let Some(filter) = join.filter()
                    && let Some(filter) = fit_join_filter(filter)?
                {
                    return Ok(Transformed::yes(Arc::new(
                        NestedLoopJoinExecBuilder::from(join)
                            .with_filter(Some(filter))
                            .build()?,
                    )
                        as Arc<dyn ExecutionPlan>));
                }
                if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>()
                    && let Some(filter) = join.filter()
                    && let Some(filter) = fit_join_filter(filter)?
                {
                    return Ok(Transformed::yes(Arc::new(SortMergeJoinExec::try_new(
                        join.left().clone(),
                        join.right().clone(),
                        join.on().to_vec(),
                        Some(filter),
                        join.join_type(),
                        join.sort_options().to_vec(),
                        join.null_equality(),
                    )?)
                        as Arc<dyn ExecutionPlan>));
                }

                if let Some(filter) = plan.downcast_ref::<FilterExec>()
                    && bounded(filter.predicate())?.is_some()
                {
                    let columns = filter.input().schema().fields().len();
                    let mut hoist = Hoist::new(filter.input().clone());
                    let predicate = hoist.expression(filter.predicate().clone())?;
                    let mut builder = FilterExecBuilder::from(filter)
                        .with_input(hoist.finish()?)
                        .with_predicate(predicate);
                    if filter.projection().is_none() {
                        builder = builder.apply_projection(Some((0..columns).collect()))?;
                    }
                    return Ok(Transformed::yes(
                        Arc::new(builder.build()?) as Arc<dyn ExecutionPlan>
                    ));
                }
                if let Some(sort) = plan.downcast_ref::<SortExec>() {
                    let mut hoist = Hoist::new(sort.input().clone());
                    let order = sort
                        .expr()
                        .iter()
                        .map(|expr| {
                            Ok(PhysicalSortExpr {
                                expr: hoist.expression(expr.expr.clone())?,
                                options: expr.options,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if hoist.changed {
                        let columns = hoist.expressions[..sort.schema().fields().len()].to_vec();
                        let native = SortExec::new(
                            LexOrdering::new(order).expect("nonempty native ordering"),
                            hoist.finish()?,
                        )
                        .with_fetch(sort.fetch())
                        .with_preserve_partitioning(sort.preserve_partitioning());
                        return Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                            columns,
                            Arc::new(native),
                        )?)
                            as Arc<dyn ExecutionPlan>));
                    }
                }
                if let Some(sort) = plan.downcast_ref::<SortPreservingMergeExec>() {
                    let mut hoist = Hoist::new(sort.input().clone());
                    let order = hoist.ordering(sort.expr())?;
                    if hoist.changed {
                        let columns = hoist.expressions[..sort.schema().fields().len()].to_vec();
                        let native = SortPreservingMergeExec::new(order, hoist.finish()?)
                            .with_fetch(sort.fetch());
                        return Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                            columns,
                            Arc::new(native),
                        )?)
                            as Arc<dyn ExecutionPlan>));
                    }
                }
                if let Some(sort) = plan.downcast_ref::<PartialSortExec>() {
                    let mut hoist = Hoist::new(sort.input().clone());
                    let order = hoist.ordering(sort.expr())?;
                    if hoist.changed {
                        let columns = hoist.expressions[..sort.schema().fields().len()].to_vec();
                        let native = PartialSortExec::new(
                            order,
                            hoist.finish()?,
                            sort.common_prefix_length(),
                        )
                        .with_fetch(sort.fetch())
                        .with_preserve_partitioning(sort.preserve_partitioning());
                        return Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                            columns,
                            Arc::new(native),
                        )?)
                            as Arc<dyn ExecutionPlan>));
                    }
                }
                if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
                    if aggregate.mode().input_mode() != AggregateInputMode::Raw {
                        return Ok(Transformed::no(plan));
                    }
                    let mut hoist = Hoist::new(aggregate.input().clone());
                    let mut functions = Vec::with_capacity(aggregate.aggr_expr().len());
                    for aggregate_expr in aggregate.aggr_expr() {
                        let args = aggregate_expr
                            .expressions()
                            .into_iter()
                            .map(|expr| hoist.expression(expr))
                            .collect::<Result<Vec<_>>>()?;
                        let order = aggregate_expr
                            .order_bys()
                            .iter()
                            .map(|expr| hoist.expression(expr.expr.clone()))
                            .collect::<Result<Vec<_>>>()?;
                        let expression = aggregate_expr
                            .with_new_expressions(args, order)
                            .ok_or_else(|| {
                                DataFusionError::Plan(
                                    "aggregate cannot preserve fitted input expressions".into(),
                                )
                            })?;
                        functions.push(Arc::new(expression));
                    }
                    let filters = aggregate
                        .filter_expr()
                        .iter()
                        .map(|expr| {
                            expr.as_ref()
                                .map(|expr| hoist.expression(expr.clone()))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let groups = aggregate.group_expr();
                    let expr = groups
                        .expr()
                        .iter()
                        .map(|(expr, name)| Ok((hoist.expression(expr.clone())?, name.clone())))
                        .collect::<Result<Vec<_>>>()?;
                    if hoist.changed {
                        let group = PhysicalGroupBy::new(
                            expr,
                            groups.null_expr().to_vec(),
                            groups.groups().to_vec(),
                            groups.has_grouping_set(),
                        );
                        let rebuilt = AggregateExec::try_new(
                            *aggregate.mode(),
                            group,
                            functions,
                            filters,
                            hoist.finish()?,
                            aggregate.input_schema(),
                        )?
                        .with_limit_options(aggregate.limit_options());
                        return Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>));
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }
}
