//! Keep per-row aggregate state off batch-shared string-view buffers.
//!
//! DataFusion 53.1 keeps one `ScalarValue` per input row for `array_agg` and
//! `string_agg` with ORDER BY or DISTINCT. A view-typed value still references
//! the batch's whole data buffer (a `Utf8 -> Utf8View` cast shares one), state
//! concatenation appends every row's buffer list without deduplication, and
//! the hash aggregate's spill serializes each reference outside the memory
//! pool: rows^2 references and rows^3 bytes. ct-deg8 measured 8.7 GiB in
//! 0.41 s at 1,000 rows under a 1 GiB pool (row_hash.rs spill -> arrow-ipc
//! write_buffer). Upstream #21633 and #21934 (54.0) do not cover the merge
//! path, and 55.2 still multiplies references.
//!
//! Non-view arguments copy only each row's bytes, so the rule casts view-typed
//! arguments of those aggregates to their portable types and casts the result
//! back, leaving the plan schema unchanged. Measured with the same casts in
//! SQL: 3k-30k rows in 0.12-0.14 s at <= 173 MiB; 300k rows within 1.02x of
//! an unaffected ordered aggregate's peak RSS.
//!
//! Remove this rule when a DataFusion release passes this module's tests and
//! `ordered_aggregate_over_shared_views_respects_process_memory` without it.
// Rust guideline compliant 2026-02-21

use crate::export::portable_data_type;
use datafusion::{
    common::{Column, Result, tree_node::Transformed},
    logical_expr::{
        Aggregate, Expr, ExprSchemable, LogicalPlan, Projection, cast, expr::AggregateFunction,
    },
    optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder},
};
use std::sync::Arc;

/// Casts view arguments of per-row ordered or distinct aggregates.
#[derive(Debug)]
pub(super) struct PortableRowAggregateArguments;

impl OptimizerRule for PortableRowAggregateArguments {
    fn name(&self) -> &str {
        "portable_row_aggregate_arguments"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };
        let input = Arc::clone(aggregate.input.schema());
        let mut changed = false;
        let mut aggr_expr = Vec::with_capacity(aggregate.aggr_expr.len());
        for expr in &aggregate.aggr_expr {
            let name = expr.schema_name().to_string();
            let mut function = expr;
            while let Expr::Alias(alias) = function {
                function = &alias.expr;
            }
            let Expr::AggregateFunction(function) = function else {
                aggr_expr.push(expr.clone());
                continue;
            };
            let params = &function.params;
            if params.order_by.is_empty() && !params.distinct {
                aggr_expr.push(expr.clone());
                continue;
            }
            // Arguments without a view type stay as written, which keeps the
            // rule idempotent across optimizer passes.
            let mut args_changed = false;
            let args = params
                .args
                .iter()
                .map(|arg| {
                    let data_type = arg.get_type(&input)?;
                    let portable = portable_data_type(&data_type);
                    if portable == data_type {
                        Ok(arg.clone())
                    } else {
                        args_changed = true;
                        Ok(cast(arg.clone(), portable))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            if !args_changed {
                aggr_expr.push(expr.clone());
                continue;
            }
            changed = true;
            let mut function: AggregateFunction = function.clone();
            // DISTINCT requires ORDER BY keys to appear among the arguments,
            // so a key equal to an argument takes the argument's rewrite.
            for sort in &mut function.params.order_by {
                if let Some(index) = function
                    .params
                    .args
                    .iter()
                    .position(|arg| *arg == sort.expr)
                {
                    sort.expr = args[index].clone();
                }
            }
            function.params.args = args;
            aggr_expr.push(Expr::AggregateFunction(function).alias_if_changed(name)?);
        }
        if !changed {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }
        let portable = Aggregate::try_new(
            Arc::clone(&aggregate.input),
            aggregate.group_expr.clone(),
            aggr_expr,
        )?;
        // Restore every original field type, name and qualifier.
        let output = (0..aggregate.schema.fields().len())
            .map(|index| {
                let (qualifier, field) = aggregate.schema.qualified_field(index);
                let column = Expr::Column(Column::from(portable.schema.qualified_field(index)));
                if portable.schema.field(index).data_type() == field.data_type() {
                    column
                } else {
                    cast(column, field.data_type().clone())
                        .alias_qualified(qualifier.cloned(), field.name())
                }
            })
            .collect();
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(output, Arc::new(LogicalPlan::Aggregate(portable)))?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Budget, Limits};
    use arrow::{
        array::{Array, ArrayRef, AsArray, Int64Array, StringArray},
        compute::cast,
        datatypes::DataType,
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        logical_expr::{Expr, ExprSchemable, LogicalPlan},
        physical_plan::{ExecutionPlan, aggregates::AggregateExec, collect},
        prelude::SessionContext,
    };
    use std::{collections::BTreeMap, sync::Arc};

    const ROWS: i64 = 300;
    const GROUPS: i64 = 10;

    fn value(i: i64) -> String {
        // 30 bytes: longer than the 12-byte inline view, so it uses a buffer.
        format!("shared-view-value-{i:012}")
    }

    fn context() -> SessionContext {
        let budget = Budget::new(Limits {
            memory_bytes: 128 << 20,
            compute_threads: 2,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        // `Utf8 -> Utf8View` makes the whole values buffer every row's data
        // buffer, as DataFusion 53.1 does for a CASE with a computed branch.
        let strings = StringArray::from_iter_values((0..ROWS).map(value));
        let shared = cast(&strings, &DataType::Utf8View).unwrap();
        assert_eq!(shared.as_string_view().data_buffers().len(), 1);
        let batch = RecordBatch::try_from_iter([
            (
                "k",
                Arc::new(Int64Array::from_iter_values((0..ROWS).map(|i| i % GROUPS))) as ArrayRef,
            ),
            ("i", Arc::new(Int64Array::from_iter_values(0..ROWS))),
            ("s", shared),
        ])
        .unwrap();
        context.register_batch("t", batch).unwrap();
        context
    }

    /// Counts view data-buffer references in an array and its children.
    fn view_buffer_references(data: &arrow::array::ArrayData) -> usize {
        let own = match data.data_type() {
            DataType::Utf8View | DataType::BinaryView => data.buffers().len() - 1,
            _ => 0,
        };
        own + data
            .child_data()
            .iter()
            .map(view_buffer_references)
            .sum::<usize>()
    }

    fn spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = if plan.is::<AggregateExec>() {
            let metrics = plan.metrics().unwrap_or_default();
            metrics.spill_count().unwrap_or(0) + metrics.spilled_bytes().unwrap_or(0)
        } else {
            0
        };
        own + plan.children().into_iter().map(spills).sum::<usize>()
    }

    /// Runs `sql`, checks bounded state, and returns strings per group key.
    async fn bounded(sql: &str) -> BTreeMap<i64, Vec<String>> {
        let context = context();
        let plan = context
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
        let references = batches
            .iter()
            .flat_map(|batch| batch.columns())
            .map(|column| view_buffer_references(&column.to_data()))
            .sum::<usize>();
        let bytes = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        // DataFusion 53.1 without the rule: 90,000 references, about 160 MB.
        assert!(references <= ROWS as usize, "{references} view references");
        assert!(bytes < 1 << 20, "{bytes} output bytes");
        assert_eq!(spills(&plan), 0, "aggregate state spilled");
        let mut groups = BTreeMap::new();
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let values = batch.column(1);
            for row in 0..batch.num_rows() {
                let strings = match values.data_type() {
                    DataType::List(_) => {
                        let item = values.as_list::<i32>().value(row);
                        let item = match item.data_type() {
                            DataType::Struct(_) => item.as_struct().column(0).clone(),
                            _ => item,
                        };
                        let item = cast(&item, &DataType::Utf8).unwrap();
                        item.as_string::<i32>()
                            .iter()
                            .map(|v| v.unwrap().to_owned())
                            .collect()
                    }
                    _ => {
                        let one = cast(&values.slice(row, 1), &DataType::Utf8).unwrap();
                        vec![one.as_string::<i32>().value(0).to_owned()]
                    }
                };
                assert!(groups.insert(keys.value(row), strings).is_none());
            }
        }
        groups
    }

    fn expected(join: bool) -> BTreeMap<i64, Vec<String>> {
        (0..GROUPS)
            .map(|k| {
                let values = (k..ROWS).step_by(GROUPS as usize).map(value);
                let values = if join {
                    vec![values.collect::<Vec<_>>().join(",")]
                } else {
                    values.collect()
                };
                (k, values)
            })
            .collect()
    }

    #[tokio::test]
    async fn ordered_aggregate_keeps_shared_view_state_linear() {
        let groups =
            bounded("SELECT k, array_agg(named_struct('s', s) ORDER BY i) AS a FROM t GROUP BY k")
                .await;
        assert_eq!(groups, expected(false));
    }

    #[tokio::test]
    async fn ordered_nested_aggregate_fits_many_small_groups() {
        // Each of two inputs contributes two references per group. A migrated
        // final aggregate charges the shared state repeatedly even with Utf8 leaves.
        const GROUPS: i64 = 4_096;
        let budget = Budget::new(Limits {
            memory_bytes: 1 << 30,
            compute_threads: 2,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        let text = |key, index| format!("{key:05}-{index}-{}", "x".repeat(240));
        for (name, positions) in [("left_input", [4, 2]), ("right_input", [3, 1])] {
            let keys = positions.into_iter().flat_map(|_| 0..GROUPS);
            let indices = positions
                .into_iter()
                .flat_map(|i| (0..GROUPS).map(move |_| i));
            let payloads = StringArray::from_iter(
                keys.clone()
                    .zip(indices.clone())
                    .map(|(key, index)| (key != 0 || index != 1).then(|| text(key, index))),
            );
            let batch = RecordBatch::try_from_iter([
                (
                    "k",
                    Arc::new(Int64Array::from_iter_values(keys)) as ArrayRef,
                ),
                ("i", Arc::new(Int64Array::from_iter_values(indices))),
                ("s", Arc::new(payloads)),
            ])
            .unwrap();
            context.register_batch(name, batch).unwrap();
        }
        let batches = context
            .sql(
                "SELECT k, array_agg(named_struct('s', s, 'i', i) ORDER BY i) AS refs \
                 FROM (SELECT * FROM left_input UNION ALL SELECT * FROM right_input) GROUP BY k",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut seen = vec![false; GROUPS as usize];
        for batch in batches {
            let keys = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let references = batch.column(1).as_list::<i32>();
            for (row, key) in keys.values().iter().copied().enumerate() {
                assert!(!std::mem::replace(&mut seen[key as usize], true));
                let items = references.value(row);
                let items = items.as_struct();
                let indices = items
                    .column(1)
                    .as_primitive::<arrow::datatypes::Int64Type>();
                assert_eq!(indices.values().as_ref(), &[1, 2, 3, 4]);
                let payloads = items.column(0).as_string::<i32>();
                for (index, payload) in (1..=4).zip(payloads.iter()) {
                    let expected = (key != 0 || index != 1).then(|| text(key, index));
                    assert_eq!(payload, expected.as_deref());
                }
            }
        }
        assert!(seen.into_iter().all(|found| found));
    }

    #[tokio::test]
    async fn distinct_aggregate_keeps_shared_view_state_linear() {
        let mut groups =
            bounded("SELECT k, array_agg(DISTINCT named_struct('s', s)) AS a FROM t GROUP BY k")
                .await;
        groups.values_mut().for_each(|values| values.sort());
        assert_eq!(groups, expected(false));
    }

    #[tokio::test]
    async fn ordered_string_agg_keeps_shared_view_state_linear() {
        let groups =
            bounded("SELECT k, string_agg(s, ',' ORDER BY i) AS a FROM t GROUP BY k").await;
        assert_eq!(groups, expected(true));
    }

    #[tokio::test]
    async fn distinct_aggregates_ordered_by_their_argument_keep_that_order() {
        // DataFusion requires DISTINCT ORDER BY keys to appear as arguments.
        let groups =
            bounded("SELECT k, array_agg(DISTINCT s ORDER BY s) AS a FROM t GROUP BY k").await;
        assert_eq!(groups, expected(false));
        let groups =
            bounded("SELECT k, string_agg(DISTINCT s, ',' ORDER BY s) AS a FROM t GROUP BY k")
                .await;
        assert_eq!(groups, expected(true));
    }

    fn aggregate_arguments(plan: &LogicalPlan) -> Vec<DataType> {
        let mut types = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Aggregate(aggregate) = node {
                for expr in &aggregate.aggr_expr {
                    let mut expr = expr;
                    while let Expr::Alias(alias) = expr {
                        expr = &alias.expr;
                    }
                    if let Expr::AggregateFunction(function) = expr {
                        for arg in &function.params.args {
                            types.push(arg.get_type(aggregate.input.schema()).unwrap());
                        }
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        types
    }

    #[tokio::test]
    async fn planner_casts_only_ordered_or_distinct_view_arguments() {
        let context = context();
        for (sql, portable) in [
            (
                "SELECT k, array_agg(named_struct('s', s) ORDER BY i) AS a FROM t GROUP BY k",
                true,
            ),
            (
                "SELECT k, array_agg(named_struct('s', s)) AS a FROM t GROUP BY k",
                false,
            ),
        ] {
            let frame = context.sql(sql).await.unwrap();
            let schema = frame.schema().as_arrow().clone();
            let plan = frame.into_optimized_plan().unwrap();
            assert_eq!(plan.schema().as_arrow(), &schema, "{sql}");
            let types = aggregate_arguments(&plan);
            assert!(!types.is_empty(), "{sql}");
            let views = types
                .iter()
                .any(|t| *t != crate::export::portable_data_type(t));
            assert_eq!(views, !portable, "{sql}: {types:?}");
        }
    }
}
