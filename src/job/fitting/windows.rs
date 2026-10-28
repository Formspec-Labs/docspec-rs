//! Name window inputs before native planning loses their rewriting interface.
// Rust guideline compliant 2026-02-21
use datafusion::{
    common::{
        Column, Result,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{Expr, LogicalPlan, Projection, Window},
    optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder},
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct FitWindowInputs;
impl OptimizerRule for FitWindowInputs {
    fn name(&self) -> &str {
        "fit_window_function_inputs"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Window(window) = plan else {
            return Ok(Transformed::no(plan));
        };
        let mut projected = window
            .input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let original = projected.len();
        let mut fit = |expr: Expr| -> Result<Expr> {
            if !super::logical_bounded(&expr)? {
                return Ok(expr);
            }
            let mut suffix = projected.len();
            let name = loop {
                let name = format!("__docspec_window_fit_{suffix}");
                if !window.schema.fields().iter().any(|f| f.name() == &name)
                    && !projected
                        .iter()
                        .any(|e| e.schema_name().to_string() == name)
                {
                    break name;
                }
                suffix += 1;
            };
            projected.push(expr.alias(name.clone()));
            Ok(Expr::Column(Column::new_unqualified(name)))
        };
        let expressions = window
            .window_expr
            .iter()
            .cloned()
            .map(|expr| {
                let name = expr.schema_name().to_string();
                expr.transform_down(|expr| {
                    let Expr::WindowFunction(mut function) = expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let p = &mut function.params;
                    p.args = std::mem::take(&mut p.args)
                        .into_iter()
                        .map(&mut fit)
                        .collect::<Result<_>>()?;
                    p.partition_by = std::mem::take(&mut p.partition_by)
                        .into_iter()
                        .map(&mut fit)
                        .collect::<Result<_>>()?;
                    for sort in &mut p.order_by {
                        sort.expr = fit(sort.expr.clone())?;
                    }
                    p.filter = p.filter.take().map(|e| fit(*e).map(Box::new)).transpose()?;
                    Ok(Transformed::yes(Expr::WindowFunction(function)))
                })?
                .data
                .alias_if_changed(name)
            })
            .collect::<Result<Vec<_>>>()?;
        if projected.len() == original {
            return Ok(Transformed::no(LogicalPlan::Window(window)));
        }
        let input = LogicalPlan::Projection(Projection::try_new(projected, window.input)?);
        let native = LogicalPlan::Window(Window::try_new(expressions, Arc::new(input))?);
        // Keep all public names, qualifiers, ordering fields and field positions.
        let output = window
            .schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(output, Arc::new(native))?,
        )))
    }
}
