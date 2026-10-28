//! Keep native logical rewrites type- and column-consistent.
// Rust guideline compliant 2026-02-21
use datafusion::{
    common::{Result, tree_node::Transformed},
    logical_expr::LogicalPlan,
    optimizer::{
        OptimizerConfig, OptimizerRule,
        analyzer::{AnalyzerRule, type_coercion::TypeCoercion},
        optimizer::{ApplyOrder, Optimizer},
    },
};
use std::sync::Arc;

#[derive(Debug)]
struct TypedSubqueries(Arc<dyn OptimizerRule + Send + Sync>);

impl OptimizerRule for TypedSubqueries {
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
        let rewritten = self.0.rewrite(plan, config)?;
        if rewritten.transformed {
            // DataFusion 53.1 retains an outer reference's earlier type when a
            // named UNION widens underneath it. Once decorrelation replaces
            // that reference with a column, use native coercion before filter
            // inference can move literals to a differently typed join input.
            rewritten.map_data(|plan| TypeCoercion::new().analyze(plan, &config.options()))
        } else {
            Ok(rewritten)
        }
    }
}

pub(super) fn rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Optimizer::new()
        .rules
        .into_iter()
        .map(|rule| {
            if rule.name() == "replace_distinct_aggregate" {
                Arc::new(super::distinct::SelectedColumns(rule))
                    as Arc<dyn OptimizerRule + Send + Sync>
            } else if rule.name() == "decorrelate_predicate_subquery" {
                Arc::new(TypedSubqueries(rule)) as Arc<dyn OptimizerRule + Send + Sync>
            } else {
                rule
            }
        })
        .collect();
    rules.push(Arc::new(crate::job::fitting::windows::FitWindowInputs));
    rules
}
