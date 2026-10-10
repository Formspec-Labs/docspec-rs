//! Admit sorted batches before maintained causal window execution.

use crate::{Error, Result, budget::Budget};
use datafusion::{
    common::{
        DataFusionError, ScalarValue,
        tree_node::{Transformed, TreeNode},
    },
    execution::TaskContext,
    functions_window::row_number::RowNumber,
    physical_expr::window::StandardWindowExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, InputOrderMode,
        PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
        windows::{BoundedWindowAggExec, WindowAggExec, WindowUDFExpr},
    },
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn admit(
    plan: Arc<dyn ExecutionPlan>,
    budget: &Budget,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|plan| {
            if plan.as_any().is::<WindowAggExec>() {
                return Err(refusal(
                    "comparison_plan",
                    "comparison requires a bounded window",
                ));
            }
            let Some(window) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() else {
                return Ok(Transformed::no(plan));
            };
            let ranking = window
                .window_expr()
                .first()
                .filter(|_| window.window_expr().len() == 1)
                .and_then(|expr| expr.as_any().downcast_ref::<StandardWindowExpr>())
                .and_then(|expr| {
                    expr.get_standard_func_expr()
                        .as_any()
                        .downcast_ref::<WindowUDFExpr>()
                })
                .is_some_and(|expr| expr.fun().inner().as_any().is::<RowNumber>());
            if !ranking || !matches!(window.input_order_mode, InputOrderMode::Sorted) {
                return Err(refusal(
                    "comparison_plan",
                    "comparison requires fully sorted ROW_NUMBER",
                ));
            }
            let input = window.input();
            let workers = input.output_partitioning().partition_count();
            if workers > budget.limits.compute_threads {
                return Err(refusal(
                    "comparison_plan",
                    "window exceeds admitted worker count",
                ));
            }
            // ROW_NUMBER emits every batch and retains only a tail row between
            // batches. Admit four input/output and tail/construction buffers,
            // plus four owned key-payload copies across search, window state,
            // evaluation and pruning. ScalarValue's stack size excludes Utf8/
            // Binary payloads. Four further batches cover sliced backing and
            // old/new concatenation overlap, including a single-row hot key.
            // Add row/column bookkeeping for keys, maps and rank state.
            // This is an allocation estimate, not a whole-process RSS guarantee.
            let bookkeeping = budget
                .limits
                .batch_rows
                .saturating_mul(input.schema().fields().len().saturating_add(4))
                .saturating_mul(size_of::<ScalarValue>())
                .saturating_mul(4);
            let bytes = budget
                .limits
                .max_batch_bytes
                .saturating_mul(12)
                .saturating_add(bookkeeping);
            let memory = (0..workers)
                .map(|_| {
                    budget
                        .reserve(bytes)
                        .map_err(|error| DataFusionError::External(Box::new(error)))
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            let checked = Arc::new(WindowInput {
                input: input.clone(),
                bytes: budget.limits.max_batch_bytes,
                rows: budget.limits.batch_rows,
                memory: Arc::new(memory),
            });
            Ok(Transformed::yes(plan.with_new_children(vec![checked])?))
        })?
        .data)
}

fn refusal(code: &'static str, message: &'static str) -> DataFusionError {
    DataFusionError::External(Box::new(Error::refusal(code, message)))
}

#[derive(Debug)]
struct WindowInput {
    input: Arc<dyn ExecutionPlan>,
    bytes: usize,
    rows: usize,
    memory: Arc<Vec<OwnedSemaphorePermit>>,
}

impl DisplayAs for WindowInput {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            formatter,
            "ComparisonWindowInput: max_bytes={}, max_rows={}",
            self.bytes, self.rows
        )
    }
}

impl ExecutionPlan for WindowInput {
    fn name(&self) -> &str {
        "ComparisonWindowInput"
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
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(refusal(
                "comparison_plan",
                "window admission requires one input",
            ));
        };
        Ok(Arc::new(Self {
            input: input.clone(),
            bytes: self.bytes,
            rows: self.rows,
            memory: self.memory.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let (bytes, rows, memory) = (self.bytes, self.rows, self.memory.clone());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            input.map(move |batch| {
                let _admission = &memory;
                let batch = batch?;
                if batch.num_rows() > rows || batch.get_array_memory_size() > bytes {
                    return Err(refusal(
                        "oversized_batch",
                        "sorted comparison batch exceeds window admission",
                    ));
                }
                Ok(batch)
            }),
        )))
    }
}
