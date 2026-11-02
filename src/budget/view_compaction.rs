//! Release unused payload before native operators retain or charge their input.
//!
//! Sorts and hash-join builds retain their input; repartition and the join
//! charge each batch by its buffers' capacity. DataFusion 53.1 emits grouped
//! aggregate output as `batch_rows` slices of one array, so those charges grow
//! with the square of groups per partition (ct-deg8, ss-ud8y). The
//! planner compacts aggregate outputs, sort inputs and hash-join build inputs.
// Rust guideline compliant 2026-02-21

use super::Budget;
use datafusion::{
    common::{
        Result, Statistics,
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    execution::TaskContext,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, StatisticsArgs, aggregates::AggregateExec, joins::HashJoinExec,
        replace_children_if_necessary, sorts::sort::SortExec, stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};

/// Runs after native planning so compaction sees retained inputs, before reservation.
#[derive(Debug)]
pub(super) struct Planner(pub Budget);
impl Planner {
    fn compacted(&self, input: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if input.is::<CompactViews>() {
            input.clone()
        } else {
            Arc::new(CompactViews {
                input: input.clone(),
                budget: self.0.clone(),
            })
        }
    }
}
impl PhysicalOptimizerRule for Planner {
    fn name(&self) -> &str {
        "compact_retained_inputs"
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
                if plan.is::<AggregateExec>() {
                    return Ok(Transformed::yes(self.compacted(&plan)));
                }
                let children = plan.children();
                let children = if plan.is::<SortExec>() {
                    children
                        .into_iter()
                        .map(|input| self.compacted(input))
                        .collect()
                } else if plan.is::<HashJoinExec>() {
                    // The first child is the build side, retained whole.
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, input)| {
                            if index == 0 {
                                self.compacted(input)
                            } else {
                                input.clone()
                            }
                        })
                        .collect()
                } else {
                    return Ok(Transformed::no(plan));
                };
                Ok(Transformed::yes(replace_children_if_necessary(
                    plan, children,
                )?))
            })?
            .data)
    }
}

#[derive(Debug)]
struct CompactViews {
    input: Arc<dyn ExecutionPlan>,
    budget: Budget,
}
impl DisplayAs for CompactViews {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactViews")
    }
}
impl ExecutionPlan for CompactViews {
    fn name(&self) -> &str {
        "CompactViews"
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
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!("CompactViews requires exactly one input");
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            budget: self.budget.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let budget = self.budget.clone();
        let stream =
            input.and_then(move |batch| {
                futures::future::ready(crate::storage::compact_retained(batch, &budget).map_err(
                    |error| datafusion::common::DataFusionError::External(Box::new(error)),
                ))
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Budget, Limits};
    use arrow::{
        array::{Array, ArrayRef, StringViewArray},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn native_sort_fits_small_values_with_large_unused_view_backing() {
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            max_batch_bytes: 1 << 20,
            ..Limits::default()
        })
        .unwrap();
        let unused = "x".repeat(40 << 20);
        let values = Arc::new(StringViewArray::from(vec![
            Some(unused.as_str()),
            Some("z retained long string"),
            None,
            Some("a retained long string"),
        ]));
        let batch =
            RecordBatch::try_from_iter([("value", Arc::new(values.slice(1, 3)) as ArrayRef)])
                .unwrap();
        assert!(batch.get_array_memory_size() > 32 << 20);
        let context = budget.context();
        context.register_batch("source", batch).unwrap();
        let result = context
            .sql("SELECT value FROM source ORDER BY value NULLS FIRST")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                None,
                Some("a retained long string"),
                Some("z retained long string")
            ]
        );
        assert!(
            result
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>()
                < 1 << 20
        );
    }

    /// Runs a DISTINCT that builds a hash join and returns the build charge.
    async fn distinct_join_build_charge(values: ArrayRef) -> (i64, usize) {
        use datafusion::physical_plan::{ExecutionPlan, collect, joins::HashJoinExec};
        let budget = Budget::new(Limits {
            compute_threads: 2,
            // Room for the aggregates' own state (about 60 MB at 200k keys).
            memory_bytes: 512 << 20,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        let rows = values.len();
        // Every key appears twice on the probe side and once after DISTINCT.
        let doubled = arrow::compute::concat(&[values.as_ref(), values.as_ref()]).unwrap();
        context
            .register_batch(
                "probe",
                RecordBatch::try_from_iter([("k", doubled.clone())]).unwrap(),
            )
            .unwrap();
        context
            .register_batch(
                "build",
                RecordBatch::try_from_iter([("k", doubled)]).unwrap(),
            )
            .unwrap();
        let plan = context
            .sql(
                "SELECT count(*) FROM (SELECT DISTINCT k FROM build) d \
                 JOIN probe p ON d.k = p.k",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        fn joins(plan: &Arc<dyn ExecutionPlan>, found: &mut Vec<Arc<dyn ExecutionPlan>>) {
            if plan.is::<HashJoinExec>() {
                found.push(plan.clone());
            }
            for child in plan.children() {
                joins(child, found);
            }
        }
        let mut found = Vec::new();
        joins(&plan, &mut found);
        assert_eq!(found.len(), 1);
        // The reproduced shape: the build side is the DISTINCT's aggregate.
        let mut build = found[0]
            .downcast_ref::<HashJoinExec>()
            .unwrap()
            .left()
            .clone();
        while !build.is::<datafusion::physical_plan::aggregates::AggregateExec>() {
            assert_eq!(build.children().len(), 1, "{}", build.name());
            build = build.children()[0].clone();
        }
        let batches = collect(plan, context.task_ctx()).await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(usize::try_from(count).unwrap(), 2 * rows);
        let charged = found[0]
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("build_mem_used"))
            .map_or(0, |value| value.as_usize());
        (count, charged)
    }

    #[tokio::test]
    async fn hash_join_build_charges_sliced_aggregate_output_once() {
        const ROWS: usize = 200_000;
        // 24-byte keys: Utf8View (Parquet's default read type) and Utf8.
        let keys = || (0..ROWS).map(|row| format!("https://e.example/{row:06}"));
        let mut charges = Vec::new();
        for values in [
            Arc::new(StringViewArray::from_iter_values(keys())) as ArrayRef,
            Arc::new(arrow::array::StringArray::from_iter_values(keys())) as ArrayRef,
        ] {
            let data_type = values.data_type().clone();
            let referenced = values.get_array_memory_size();
            let (_, charged) = distinct_join_build_charge(values).await;
            charges.push((data_type, charged, referenced));
        }
        // Keys plus the join's hash table; DataFusion 53.1 charges every
        // batch_rows slice of the aggregate's output for the whole output.
        assert!(
            charges
                .iter()
                .all(|(_, charged, referenced)| *charged < 4 * *referenced),
            "build charged bytes against referenced bytes: {charges:?}"
        );
    }
}
