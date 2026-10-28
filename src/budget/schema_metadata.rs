//! Keep affected native batches consistent with their declared logical metadata.
// Rust guideline compliant 2026-02-21
use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{
        DFSchemaRef, Result,
        config::ConfigOptions,
        internal_err,
        tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    },
    execution::{SessionState, TaskContext, context::QueryPlanner},
    logical_expr::{
        Expr, LogicalPlan,
        logical_plan::{Extension, Union, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    },
    optimizer::analyzer::AnalyzerRule,
    physical_expr::expressions::Column,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc};

#[derive(Debug)]
pub(super) struct Planner;

fn normalize(plan: LogicalPlan, wrap: bool) -> Result<LogicalPlan> {
    Ok(plan
        .rewrite_with_subqueries(&mut Reconcile {
            wrap,
            input_schemas: Vec::new(),
        })?
        .data)
}

struct Reconcile {
    wrap: bool,
    input_schemas: Vec<Vec<DFSchemaRef>>,
}

impl TreeNodeRewriter for Reconcile {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        self.input_schemas.push(
            plan.inputs()
                .iter()
                .map(|input| input.schema().clone())
                .collect(),
        );
        Ok(Transformed::no(plan))
    }

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let before = self
            .input_schemas
            .pop()
            .expect("paired native plan traversal");
        let changed_input = before
            .iter()
            .zip(plan.inputs())
            .any(|(before, input)| before != input.schema());
        let prior = plan.schema().clone();
        // Native tree rewriting retains cached parent schemas. Reconstruct
        // only ancestors whose child declaration actually changed.
        let plan = if changed_input {
            plan.recompute_schema()?
        } else {
            plan
        };
        let result = self.node(plan)?;
        let changed_schema = result.data.schema() != &prior;
        Ok(Transformed::new(
            result.data,
            result.transformed || changed_schema,
            result.tnr,
        ))
    }
}

impl Reconcile {
    fn node(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let wrap = self.wrap;
        if wrap && let LogicalPlan::Projection(projection) = &plan {
            for expr in &projection.expr {
                if expr.exists(|expr| Ok(matches!(expr, Expr::TryCast(_))))? {
                    // Native TryCastExpr::return_field retains input tags;
                    // logical TRY_CAST declares a new field without them.
                    return Ok(Transformed::yes(wrapped(plan)));
                }
            }
        }
        let LogicalPlan::Union(union) = plan else {
            return Ok(Transformed::no(plan));
        };
        let prior = union.schema.clone();
        // Type coercion in 53.1 merges metadata; subsequent column pruning
        // intersects it. Use the native logical constructor consistently.
        let union = if union.inputs.len() > 1 {
            Union::try_new(union.inputs)?
        } else {
            union
        };
        let needs_adapter = union
            .inputs
            .iter()
            .any(|input| input.schema().as_arrow() != union.schema.as_arrow());
        let changed = union.schema != prior;
        let plan = LogicalPlan::Union(union);
        if wrap && needs_adapter {
            Ok(Transformed::yes(wrapped(plan)))
        } else if changed {
            Ok(Transformed::yes(plan))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

fn wrapped(plan: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(DeclaredSchema {
            input: Arc::new(plan),
        }),
    })
}

impl AnalyzerRule for Planner {
    fn name(&self) -> &str {
        "schema_metadata"
    }
    fn analyze(&self, plan: LogicalPlan, _options: &ConfigOptions) -> Result<LogicalPlan> {
        normalize(plan, false)
    }
}

#[async_trait::async_trait]
impl QueryPlanner for Planner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // This hook runs after native logical optimization. Keep all native
        // physical planning and checks; only bridge the metadata discrepancy.
        let plan = normalize(plan.clone(), true)?;
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(Self)])
            .create_physical_plan(&plan, state)
            .await
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
struct DeclaredSchema {
    input: Arc<LogicalPlan>,
}

impl UserDefinedLogicalNodeCore for DeclaredSchema {
    fn name(&self) -> &str {
        "DeclaredSchema"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeclaredSchema")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!("Declared schema requires one input and no expressions");
        }
        Ok(Self {
            input: Arc::new(inputs.remove(0)),
        })
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for Planner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        inputs: &[Arc<dyn ExecutionPlan>],
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DeclaredSchema>() else {
            return Ok(None);
        };
        let [input] = inputs else {
            return internal_err!("Declared schema requires one physical input");
        };
        let schema = node.input.schema().inner().clone();
        // UNION may advertise the target schema yet emit a child schema.
        // Keep this adapter even when the advertised schemas already agree.
        Ok(Some(Arc::new(MetadataExec::new(input.clone(), schema)?)))
    }
}

#[derive(Debug)]
struct MetadataExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl MetadataExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        let old = input.schema();
        if old.fields().len() != schema.fields().len()
            || old.fields().iter().zip(schema.fields()).any(|(a, b)| {
                a.name() != b.name()
                    || a.data_type() != b.data_type()
                    || (a.is_nullable() && !b.is_nullable())
            })
        {
            return internal_err!(
                "Metadata adapter cannot change names, types, or tighten nullability"
            );
        }
        let eq = input
            .equivalence_properties()
            .clone()
            .with_new_schema(schema)?;
        let properties = Arc::new(input.properties().as_ref().clone().with_eq_properties(eq));
        Ok(Self { input, properties })
    }
}

impl DisplayAs for MetadataExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeclaredSchema")
    }
}

impl ExecutionPlan for MetadataExec {
    fn name(&self) -> &str {
        "DeclaredSchema"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("Declared schema requires one child");
        }
        Ok(Arc::new(Self::new(children.remove(0), self.schema())?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let (_, columns, rows) = batch?.into_parts();
            // Move the column vector; retain every array and buffer unchanged.
            RecordBatch::try_new_with_options(
                schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<datafusion::common::Statistics> {
        self.input.partition_statistics(partition)
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Column pruning is metadata-neutral. Keep expression evaluation in
        // the existing admitted projection path rather than creating it here.
        if !projection
            .expr()
            .iter()
            .all(|p| p.expr.as_any().is::<Column>())
        {
            return Ok(None);
        }
        let input = Arc::new(ProjectionExec::try_new(
            projection.expr().to_vec(),
            self.input.clone(),
        )?);
        Ok(Some(Arc::new(Self::new(input, projection.schema())?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use futures::TryStreamExt;
    use std::collections::HashMap;

    fn batch(value: Option<i64>, origin: &str) -> (RecordBatch, ArrayRef) {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![value]));
        let metadata = HashMap::from([
            ("common".into(), "unit".into()),
            ("origin".into(), origin.into()),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("x", DataType::Int64, value.is_none()).with_metadata(metadata.clone())],
            metadata,
        ));
        (
            RecordBatch::try_new(schema, vec![values.clone()]).unwrap(),
            values,
        )
    }

    #[tokio::test]
    async fn union_reconciles_schema_and_field_tags_without_copying_arrays() {
        for (left_origin, right_origin) in [("left", "right"), ("same", "same")] {
            let context = crate::budget::Budget::new(crate::budget::Limits::default())
                .unwrap()
                .context();
            let (left, values) = batch(Some(7), left_origin);
            let (right, _) = batch(None, right_origin);
            context.register_batch("left_input", left).unwrap();
            context.register_batch("right_input", right).unwrap();
            let frame = context
                .sql("SELECT x FROM left_input UNION ALL SELECT x FROM right_input")
                .await
                .unwrap();
            let declared = frame.schema().inner().clone();
            let mut expected = HashMap::from([("common".into(), "unit".into())]);
            if left_origin == right_origin {
                expected.insert("origin".into(), left_origin.into());
            }
            assert_eq!(declared.metadata(), &expected);
            assert_eq!(declared.field(0).metadata(), &expected);
            assert!(declared.field(0).is_nullable());
            let batches = frame.collect().await.unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            assert!(
                batches
                    .iter()
                    .any(|batch| Arc::ptr_eq(batch.column(0), &values))
            );
            assert_eq!(
                batches
                    .iter()
                    .map(|batch| batch.column(0).null_count())
                    .sum::<usize>(),
                1
            );
            for batch in batches {
                assert_eq!(batch.schema(), declared);
            }
        }
    }

    #[tokio::test]
    async fn metadata_adapter_preserves_zero_column_rows() {
        let original = Arc::new(Schema::new_with_metadata(
            Vec::<Field>::new(),
            HashMap::from([("origin".into(), "source".into())]),
        ));
        let batch = RecordBatch::try_new_with_options(
            original.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], original, None).unwrap();
        let target = Arc::new(Schema::empty());
        let plan = MetadataExec::new(input, target.clone()).unwrap();
        let context = crate::budget::Budget::new(crate::budget::Limits::default())
            .unwrap()
            .context();
        let batches: Vec<_> = plan
            .execute(0, context.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(batches.iter().all(|batch| batch.schema() == target));
    }

    #[test]
    fn metadata_adapter_refuses_value_schema_changes() {
        let (batch, _) = batch(None, "source");
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        for field in [
            Field::new("renamed", DataType::Int64, true),
            Field::new("x", DataType::Utf8, true),
            Field::new("x", DataType::Int64, false),
        ] {
            assert!(MetadataExec::new(input.clone(), Arc::new(Schema::new(vec![field]))).is_err());
        }
    }
}
