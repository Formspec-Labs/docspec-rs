//! Release unused payload before native operators retain or charge their input.
//!
//! Sorts and hash-join builds retain their input; repartition and the join
//! charge each batch by its buffers' capacity. DataFusion 53.1 emits grouped
//! aggregate output as `batch_rows` slices of one array, so those charges grow
//! with the square of groups per partition (ct-deg8, ss-ud8y). An outer join
//! emits its unmatched build rows the same way. The planner compacts
//! aggregate outputs, sort inputs and hash-join build inputs.
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
        aggregates::AggregateExec, joins::HashJoinExec, sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use std::{any::Any, fmt, sync::Arc};

/// Runs after native planning so compaction sees retained inputs, before reservation.
#[derive(Debug)]
pub(super) struct Planner(pub Budget);
impl Planner {
    fn compacted(&self, input: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if input.as_any().is::<CompactViews>() {
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
                if plan.as_any().is::<AggregateExec>() {
                    return Ok(Transformed::yes(self.compacted(&plan)));
                }
                let children = plan.children();
                let children = if plan.as_any().is::<SortExec>() {
                    children
                        .into_iter()
                        .map(|input| self.compacted(input))
                        .collect()
                } else if plan.as_any().is::<HashJoinExec>() {
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
        datatypes::DataType,
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::{ExecutionPlan, collect, joins::HashJoinExec};
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

    /// Every hash join in `plan`, outermost first.
    fn hash_joins(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
        let mut found = Vec::new();
        if plan.as_any().is::<HashJoinExec>() {
            found.push(plan.clone());
        }
        for child in plan.children() {
            found.extend(hash_joins(child));
        }
        found
    }

    /// Bytes an executed hash join charged the pool for its build side.
    fn build_charge(join: &Arc<dyn ExecutionPlan>) -> usize {
        join.metrics()
            .and_then(|metrics| metrics.sum_by_name("build_mem_used"))
            .map_or(0, |value| value.as_usize())
    }

    /// Runs a DISTINCT that builds a hash join and returns the build charge.
    async fn distinct_join_build_charge(values: ArrayRef) -> (i64, usize) {
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
        let found = hash_joins(&plan);
        assert_eq!(found.len(), 1);
        // The reproduced shape: the build side is the DISTINCT's aggregate.
        let mut build = found[0]
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .unwrap()
            .left()
            .clone();
        while !build
            .as_any()
            .is::<datafusion::physical_plan::aggregates::AggregateExec>()
        {
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
        (count, build_charge(&found[0]))
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

    /// Runs `big LEFT JOIN mid LEFT JOIN tiny` over retained steps.
    ///
    /// Returns the bytes `big` retains and the second join's build charge.
    async fn chained_left_join_build_charge(
        payload: &str,
        expected: fn(&DataType) -> bool,
    ) -> (usize, usize) {
        const ROWS: usize = 200_000;
        const MATCHED: usize = 2000;
        let scratch = tempfile::tempdir().unwrap();
        let job = crate::job::Job::new(
            Limits {
                // Retained steps carry no statistics, so each join builds on
                // its left input, hash-partitioned by key.
                compute_threads: 2,
                // A 4 GiB pool admits an inflated charge so the test reports it.
                memory_bytes: 8 << 30,
                ..Limits::default()
            },
            scratch.path().join("scratch"),
        )
        .unwrap();
        let (storage, rows, retained) = job
            .materialize_detailed(
                "big",
                &format!(
                    "SELECT value AS k, lpad(cast(value AS varchar), 200, 'b') AS body, \
                     {payload} AS payload FROM generate_series(1, {ROWS}) AS t(value)"
                ),
            )
            .await
            .unwrap();
        assert_eq!((storage, rows), (crate::job::StepStorage::Memory, ROWS));
        for (name, keys) in [("mid", MATCHED), ("tiny", 1)] {
            let sql = format!("SELECT value AS k FROM generate_series(1, {keys}) AS t(value)");
            job.materialize(name, &sql).await.unwrap();
        }
        let frame = job
            .sql(
                "SELECT big.k, big.body, big.payload, mid.k AS mid_k, tiny.k AS tiny_k \
                 FROM big LEFT JOIN mid ON big.k = mid.k LEFT JOIN tiny ON big.k = tiny.k",
            )
            .await
            .unwrap();
        let context = Arc::new(frame.task_ctx());
        let plan = frame.create_physical_plan().await.unwrap();
        let joins = hash_joins(&plan);
        assert_eq!(joins.len(), 2);
        // The reproduced shape: the second join builds on the first join's
        // output, whose unmatched rows leave as slices of one batch.
        let second = joins[0].as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(hash_joins(second.left()).len(), 1);
        let batches = collect(plan, context).await.unwrap();
        assert!(expected(batches[0].column(2).data_type()));
        let present = |column: usize| {
            batches
                .iter()
                .map(|batch| batch.num_rows() - batch.column(column).null_count())
                .sum::<usize>()
        };
        assert_eq!([present(0), present(3), present(4)], [ROWS, MATCHED, 1]);
        (retained, build_charge(&joins[0]))
    }

    #[tokio::test]
    async fn hash_join_build_charges_sliced_join_output_once() {
        let mut charges = Vec::new();
        for (name, payload, expected) in [
            (
                "list",
                "make_array(lpad(cast(value AS varchar), 150, 'x'), \
                 lpad(cast(value AS varchar), 150, 'y'))",
                (|data_type| matches!(data_type, DataType::List(item) if item.data_type() == &DataType::Utf8))
                    as fn(&DataType) -> bool,
            ),
            // Control: the same bytes in a column the guard always measured.
            (
                "string",
                "lpad(cast(value AS varchar), 300, 'x')",
                |data_type| data_type == &DataType::Utf8,
            ),
        ] {
            let (retained, charged) = chained_left_join_build_charge(payload, expected).await;
            charges.push((name, charged, retained));
        }
        eprintln!("second build charged bytes against retained step bytes: {charges:?}");
        // The whole left relation plus the join's hash table. Unguarded, every
        // batch_rows slice of the first join's unmatched rows is charged for
        // all of them.
        assert!(
            charges
                .iter()
                .all(|(_, charged, retained)| *charged < 2 * *retained),
            "second build charged bytes against retained step bytes: {charges:?}"
        );
    }
}
