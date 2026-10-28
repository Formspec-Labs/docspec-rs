//! Release unused view payload before native operators retain their input.
// Rust guideline compliant 2026-02-21

use super::Budget;
use datafusion::{
    common::{
        Result, Statistics,
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode},
    },
    execution::TaskContext,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        sorts::sort::SortExec, stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use std::{any::Any, fmt, sync::Arc};

/// Runs after native planning so sort sees retained payload, before reservation.
#[derive(Debug)]
pub(super) struct Planner(pub Budget);
impl PhysicalOptimizerRule for Planner {
    fn name(&self) -> &str {
        "compact_sort_view_inputs"
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
                if !plan.as_any().is::<SortExec>() {
                    return Ok(Transformed::no(plan));
                }
                let children = plan
                    .children()
                    .iter()
                    .map(|input| {
                        if input.as_any().is::<CompactViews>() {
                            (*input).clone()
                        } else {
                            Arc::new(CompactViews {
                                input: (*input).clone(),
                                budget: self.0.clone(),
                            }) as Arc<dyn ExecutionPlan>
                        }
                    })
                    .collect();
                Ok(Transformed::yes(plan.with_new_children(children)?))
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
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
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
                futures::future::ready(crate::storage::compact_views(batch, &budget).map_err(
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
}
