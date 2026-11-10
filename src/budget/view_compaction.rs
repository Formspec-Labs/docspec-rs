//! Release unused payload before native operators retain or charge their input.
//!
//! Sorts and hash-join builds retain their input; repartition and the join
//! charge each batch by its buffers' capacity. DataFusion 53.1 emits grouped
//! aggregate output as `batch_rows` slices of one array, so those charges grow
//! with the square of groups per partition (ct-deg8, ss-ud8y). A hash or
//! nested-loop join that emits build rows after its probe ends hands them on
//! the same way (ct-n5ec). The planner compacts where such slices are made,
//! the output of aggregates and of those joins, so every operator downstream
//! is covered at once, and where input is retained from any source: sort
//! inputs and hash-join build inputs. A batch is copied at most once; a
//! compact batch only pays the measurement again. Compaction is optional: a
//! view copy the shared budget refuses is skipped, and the batch passes on
//! with its views as they came (`compact_retained`).
// Rust guideline compliant 2026-02-21

use super::Budget;
use datafusion::{
    common::{
        JoinType, Result, Statistics,
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode},
    },
    execution::TaskContext,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        aggregates::AggregateExec,
        joins::{HashJoinExec, NestedLoopJoinExec},
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use std::{any::Any, fmt, sync::Arc};

/// The rule's name, which also removes it from a session in tests.
const NAME: &str = "compact_retained_inputs";

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
                metrics: ExecutionPlanMetricsSet::new(),
            })
        }
    }
}

/// A copy of `context` that plans without this rule, to compare results in tests.
///
/// The copy shares the catalog, so tables registered in either are seen by both.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn without(
    context: &datafusion::prelude::SessionContext,
) -> datafusion::prelude::SessionContext {
    let state = context.state();
    let rules = state
        .physical_optimizers()
        .iter()
        .filter(|rule| rule.name() != NAME)
        .cloned()
        .collect();
    datafusion::prelude::SessionContext::new_with_state(
        datafusion::execution::SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(rules)
            .build(),
    )
}

/// Whether `plan`'s output can be `batch_rows` slices of one large batch.
///
/// An aggregate emits its groups that way. So does a join that produces build
/// rows only after its probe ends: a hash join pushes them into its output
/// coalescer as one batch, and a nested-loop join emits unfiltered ranges of
/// its build batch.
fn emits_slices(plan: &dyn ExecutionPlan) -> bool {
    let plan = plan.as_any();
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        emits_build_rows_last(*join.join_type())
    } else if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        emits_build_rows_last(*join.join_type())
    } else {
        plan.is::<AggregateExec>()
    }
}

/// Whether a join of `join_type` emits build rows only after its probe ends.
///
/// This restates the vendored `need_produce_result_in_final`, which DataFusion
/// keeps crate-private. The match names every variant, so an upgrade that adds
/// one stops the build here; `join_types_emitting_build_rows_last_match_datafusion`
/// checks every answer against what the joins themselves allocate.
fn emits_build_rows_last(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Left
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::LeftMark => true,
        JoinType::Inner
        | JoinType::Right
        | JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::RightMark => false,
    }
}

impl PhysicalOptimizerRule for Planner {
    fn name(&self) -> &str {
        NAME
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
                // Children are visited first, so a second pass wraps a slicing
                // operator again beneath its first wrap. Folding the pair keeps
                // the rule idempotent.
                if let Some(wrap) = plan.as_any().downcast_ref::<CompactViews>()
                    && wrap.input.as_any().is::<CompactViews>()
                {
                    return Ok(Transformed::yes(wrap.input.clone()));
                }
                // Leading inputs the operator retains whole: all of a sort's,
                // and a hash join's first child, its build side.
                let retained = if plan.as_any().is::<SortExec>() {
                    usize::MAX
                } else {
                    usize::from(plan.as_any().is::<HashJoinExec>())
                };
                let sliced = emits_slices(plan.as_ref());
                if retained == 0 && !sliced {
                    return Ok(Transformed::no(plan));
                }
                let plan = if retained > 0 {
                    let children = plan
                        .children()
                        .into_iter()
                        .enumerate()
                        .map(|(index, input)| {
                            if index < retained {
                                self.compacted(input)
                            } else {
                                input.clone()
                            }
                        })
                        .collect();
                    plan.with_new_children(children)?
                } else {
                    plan
                };
                Ok(Transformed::yes(if sliced {
                    self.compacted(&plan)
                } else {
                    plan
                }))
            })?
            .data)
    }
}

/// Copies each batch that holds far more than its rows, as `compact_retained` decides.
///
/// A view copy the shared budget refuses leaves the views as they came. EXPLAIN
/// ANALYZE shows `copied_batches`, the batches returned in new buffers, and
/// `refused_copies`, those passed on uncompacted for want of admission; the
/// step metric event carries both for the step.
///
/// A copied view column is charged twice while an operator retains it: its
/// buffers carry the shared-budget permit `compact_views` reserved (payload
/// plus 128 bytes a row), and the operator charges the same buffers (payload
/// plus 16 bytes a row) to the native pool, the other half of the budget.
/// Aggregate output, sort input and hash-join build input were charged so
/// before the outer-join wrap; it adds outer-join output. Copies of other
/// columns hold no permit and are charged once, by the operator.
#[derive(Debug)]
struct CompactViews {
    input: Arc<dyn ExecutionPlan>,
    budget: Budget,
    metrics: ExecutionPlanMetricsSet,
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
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The job's step counts, when its session carries them.
        let step = context
            .session_config()
            .get_extension::<crate::job::metrics::Compactions>();
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let budget = self.budget.clone();
        let copied = MetricBuilder::new(&self.metrics).counter("copied_batches", partition);
        let refused = MetricBuilder::new(&self.metrics).counter("refused_copies", partition);
        let stream = input.and_then(move |batch| {
            let rows = batch.num_rows();
            let kept = crate::storage::compact_retained(batch, &budget)
                .map(|retained| {
                    if retained.copied {
                        copied.add(1);
                        step.iter().for_each(|step| step.record_copy());
                    }
                    if let Some(error) = &retained.refused {
                        refused.add(1);
                        step.iter().for_each(|step| step.record_refusal());
                        tracing::debug!(
                            rows,
                            error = %error,
                            "retained compaction refused; views passed on uncompacted"
                        );
                    }
                    retained.batch
                })
                .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)));
            futures::future::ready(kept)
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
    async fn applying_the_rule_twice_yields_the_same_plan() {
        use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::displayable};
        let budget = Budget::new(Limits {
            compute_threads: 2,
            memory_bytes: 64 << 20,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        // The larger table stays the probe side, so the outer join stays Left.
        for (name, rows) in [("t", 100), ("u", 10_000)] {
            let keys = Arc::new(arrow::array::Int64Array::from_iter_values(0..rows)) as ArrayRef;
            context
                .register_batch(name, RecordBatch::try_from_iter([("k", keys)]).unwrap())
                .unwrap();
        }
        // Wraps over aggregate and outer-join output and under a sort; the
        // aggregate's wrap is also the build input of the join.
        let plan = context
            .sql(
                "SELECT a.k, count(*) AS n FROM (SELECT k, count(*) AS c FROM t GROUP BY k) a \
                 LEFT JOIN u b ON a.k = b.k GROUP BY a.k ORDER BY a.k",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let once = displayable(plan.as_ref()).indent(true).to_string();
        let state = context.state();
        let again = super::Planner(budget.clone())
            .optimize(plan, state.config_options())
            .unwrap();
        let twice = displayable(again.as_ref()).indent(true).to_string();
        // A node's first child is the next line of the indented plan.
        for (parent, child) in [
            ("CompactViews", "AggregateExec"),
            ("CompactViews", "HashJoinExec"),
            ("SortExec", "CompactViews"),
        ] {
            assert!(
                once.lines()
                    .zip(once.lines().skip(1))
                    .any(|(above, below)| above.trim_start().starts_with(parent)
                        && below.trim_start().starts_with(child)
                        && (child != "HashJoinExec" || below.contains("join_type=Left"))),
                "no {parent} over {child}:\n{once}"
            );
        }
        assert_eq!(twice, once);
    }

    #[tokio::test]
    async fn join_types_emitting_build_rows_last_match_datafusion() {
        use datafusion::{
            common::JoinType::*, datasource::memory::MemorySourceConfig, execution::TaskContext,
            physical_plan::joins::NestedLoopJoinExec,
        };
        // Every variant: `emits_build_rows_last` stops compiling on a new one.
        let every = [
            Inner, Left, Right, Full, LeftSemi, RightSemi, LeftAnti, RightAnti, LeftMark, RightMark,
        ];
        let rows = 1000usize;
        let build = RecordBatch::try_from_iter([(
            "b",
            Arc::new(arrow::array::Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
        )])
        .unwrap();
        let probe = RecordBatch::try_from_iter([(
            "p",
            Arc::new(arrow::array::Int64Array::from(vec![0])) as ArrayRef,
        )])
        .unwrap();
        let task = Arc::new(TaskContext::default());
        let mut answers = Vec::new();
        for join_type in every {
            let input = |batch: &RecordBatch| {
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
                    .unwrap() as Arc<dyn ExecutionPlan>
            };
            let join: Arc<dyn ExecutionPlan> = Arc::new(
                NestedLoopJoinExec::try_new(input(&build), input(&probe), None, &join_type, None)
                    .unwrap(),
            );
            collect(join.clone(), task.clone()).await.unwrap();
            // DataFusion charges its build a bitmap of visited rows exactly
            // when it must emit build rows after the probe ends.
            let bitmap = build_charge(&join) - build.get_array_memory_size();
            assert!(
                bitmap == 0 || bitmap == rows.div_ceil(8),
                "{join_type}: {bitmap}"
            );
            answers.push((
                join_type,
                bitmap > 0,
                super::emits_build_rows_last(join_type),
            ));
        }
        assert!(
            answers.iter().all(|(_, native, ours)| native == ours),
            "(join type, DataFusion, emits_build_rows_last): {answers:?}"
        );
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
