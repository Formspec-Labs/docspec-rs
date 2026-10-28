//! Preserve selected USING keys when native DISTINCT becomes an aggregate.
// Rust guideline compliant 2026-02-21
use datafusion::{
    common::{Result, tree_node::Transformed},
    logical_expr::{Aggregate, Distinct, Expr, LogicalPlan},
    optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder},
};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct SelectedColumns(pub Arc<dyn OptimizerRule + Send + Sync>);

impl OptimizerRule for SelectedColumns {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.0.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Distinct(Distinct::All(input)) = &plan
            && !input.schema().fields().is_empty()
            && !input.using_columns()?.is_empty()
        {
            // DataFusion 53.1 expands a wildcard over already selected fields.
            // That can discard a USING key whose peer is no longer selected,
            // turning DISTINCT into LIMIT 1 or an incomplete grouping. The
            // selected schema is authoritative: group on every output field.
            let columns = input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            return Ok(Transformed::yes(LogicalPlan::Aggregate(
                Aggregate::try_new(input.clone(), columns, vec![])?,
            )));
        }
        // Retain native DISTINCT ON, zero-column handling and other rewrites.
        self.0.rewrite(plan, config)
    }
}
