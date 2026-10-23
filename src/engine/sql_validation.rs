//! Request validation, SQL AST/plan qualification, and build-query planning.

use super::{backfill, evidence, refresh, schema};
use crate::{Error, Result, model::*, storage};
use datafusion::prelude::*;

pub(in crate::engine) fn validate_request(spec: &OperationRequest) -> Result<()> {
    if let OperationRequest::Evidence(spec) = spec {
        return evidence::validate(spec);
    }
    if let OperationRequest::Schema(spec) = spec {
        return schema::validate(spec);
    }
    if let OperationRequest::Refresh(spec) = spec {
        return refresh::validate(spec);
    }
    if let OperationRequest::Backfill(spec) = spec {
        return backfill::validate(spec);
    }
    if let OperationRequest::Build(build) = spec {
        return validate_spec(build);
    }
    if spec.definition().is_empty()
        || spec.inputs().values().any(|input| {
            input.digest.len() != 64 || !input.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(Error::refusal(
            "invalid_request",
            "expected BLAKE3 input digests and a nonempty definition",
        ));
    }
    if let OperationRequest::Update(update) = spec {
        let unique: std::collections::BTreeSet<_> = update.keys.iter().collect();
        if update.keys.is_empty()
            || unique.len() != update.keys.len()
            || update.base == update.operation
        {
            return Err(Error::refusal(
                "invalid_update",
                "update needs distinct key fields and a different base operation",
            ));
        }
    }
    Ok(())
}

pub(in crate::engine) fn validate_spec(spec: &RunSpec) -> Result<()> {
    if spec.base.as_ref() == Some(&spec.operation) {
        return Err(Error::refusal(
            "invalid_request",
            "a rebuild requires a different base operation",
        ));
    }
    if spec.expected_digest.len() != 64
        || !spec.expected_digest.bytes().all(|c| c.is_ascii_hexdigit())
        || spec.definition.is_empty()
    {
        return Err(Error::refusal(
            "invalid_request",
            "expected a BLAKE3 input digest and nonempty definition",
        ));
    }
    let mut partition_names = std::collections::BTreeSet::new();
    for pf in &spec.partition_by {
        if pf.column.trim().is_empty()
            || storage::identity::hidden(&pf.column)
            || storage::defaults::supplied_id(&pf.column).is_some()
        {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "partition_by requires a non-empty user source column",
            ));
        }
        if matches!(
            pf.transform,
            PartitionTransformSpec::Bucket { num_buckets: 0 }
                | PartitionTransformSpec::Truncate { width: 0 }
        ) {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "partition bucket count and truncate width must be positive",
            ));
        }
        let field_name = pf
            .name
            .clone()
            .unwrap_or_else(|| storage::default_partition_field_name(&pf.column, &pf.transform));
        if field_name.trim().is_empty() || !partition_names.insert(field_name.to_ascii_lowercase())
        {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "partition field names must be non-empty and distinct",
            ));
        }
    }
    use datafusion::sql::sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
    let statements = Parser::parse_sql(&GenericDialect {}, &spec.sql)
        .map_err(|e| Error::refusal("invalid_sql", e.to_string()))?;
    if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
        return Err(Error::refusal(
            "query_only",
            "processing accepts exactly one read-only query over input",
        ));
    }
    use datafusion::sql::sqlparser::ast::Visit;
    if let std::ops::ControlFlow::Break((code, reason)) = statements[0].visit(&mut SqlQualification)
    {
        return Err(Error::refusal(code, reason));
    }
    Ok(())
}

struct SqlQualification;
impl datafusion::sql::sqlparser::ast::Visitor for SqlQualification {
    type Break = (&'static str, &'static str);
    fn pre_visit_expr(
        &mut self,
        expression: &datafusion::sql::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion::sql::sqlparser::ast::Expr::Function(function) = expression
            && function.over.is_some()
        {
            return std::ops::ControlFlow::Break((
                "unrepeatable_query",
                "window functions require ordering qualification before their results can be reused",
            ));
        }
        std::ops::ControlFlow::Continue(())
    }
    fn pre_visit_query(
        &mut self,
        query: &datafusion::sql::sqlparser::ast::Query,
    ) -> std::ops::ControlFlow<Self::Break> {
        if query.limit_clause.is_some() || query.fetch.is_some() {
            return std::ops::ControlFlow::Break((
                "unrepeatable_query",
                "LIMIT, OFFSET and FETCH require total-order qualification before their results can be reused",
            ));
        }
        use datafusion::sql::sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};
        fn unsupported(body: &SetExpr) -> bool {
            match body {
                SetExpr::SetOperation {
                    op,
                    set_quantifier,
                    left,
                    right,
                } => {
                    (matches!(op, SetOperator::Except | SetOperator::Intersect)
                        && *set_quantifier == SetQuantifier::All)
                        || unsupported(left)
                        || unsupported(right)
                }
                _ => false,
            }
        }
        if unsupported(&query.body) {
            std::ops::ControlFlow::Break((
                "unsupported_sql",
                "EXCEPT ALL and INTERSECT ALL do not preserve required duplicate semantics in the pinned engine",
            ))
        } else {
            std::ops::ControlFlow::Continue(())
        }
    }
}

pub(in crate::engine) fn validate_plan(plan: &datafusion::logical_expr::LogicalPlan) -> Result<()> {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        logical_expr::{Expr, LogicalPlan, Volatility},
    };
    let mut refusal = None;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan.table_name.table() != "input"
        {
            refusal = Some("processing may read only the retained input");
        }
        if matches!(node, LogicalPlan::Limit(_)) {
            refusal = Some(
                "LIMIT, OFFSET and FETCH require total-order qualification before their results can be reused",
            );
        }
        node.apply_expressions(|expression| {
            expression.apply(|part| {
                let volatility = match part {
                    Expr::ScalarFunction(f) => Some(f.func.signature().volatility),
                    Expr::AggregateFunction(f) => {
                        if f.func.order_sensitivity()
                            != datafusion::logical_expr::utils::AggregateOrderSensitivity::Insensitive
                        {
                            refusal = Some(
                                "order-sensitive aggregates require ordering qualification before their results can be reused",
                            );
                        }
                        Some(f.func.signature().volatility)
                    }
                    Expr::WindowFunction(_) => {
                        refusal = Some(
                            "window functions require ordering qualification before their results can be reused",
                        );
                        None
                    }
                    _ => None,
                };
                if volatility.is_some_and(|v| v != Volatility::Immutable) {
                    refusal = Some(
                        "reusable processing requires immutable functions; clock and random functions are unsupported",
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(reason) = refusal {
        return Err(Error::refusal("unrepeatable_query", reason));
    }
    Ok(())
}

pub(in crate::engine) fn check_build_arrow_schema(schema: &arrow::datatypes::Schema) -> Result<()> {
    if schema.fields().iter().any(|f| {
        f.name()
            .to_ascii_lowercase()
            .starts_with(storage::defaults::SUPPLIED_PREFIX)
            || storage::identity::hidden(&f.name().to_ascii_lowercase())
    }) {
        return Err(Error::refusal(
            "reserved_field",
            "supplied-state columns require an evolved source schema",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_build_fields(frame: &DataFrame) -> Result<()> {
    check_build_arrow_schema(frame.schema().as_arrow())
}

pub(in crate::engine) async fn build_query(
    context: &SessionContext,
    sql: &str,
) -> Result<DataFrame> {
    let options = datafusion::execution::context::SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let dataframe = context.sql_with_options(sql, options).await?;
    validate_plan(dataframe.logical_plan())?;
    Ok(dataframe)
}

pub(in crate::engine) async fn plan_build_input(
    context: &SessionContext,
    parquet_path: &str,
    sql: &str,
) -> Result<DataFrame> {
    context
        .register_parquet("input", parquet_path, ParquetReadOptions::default())
        .await?;
    let input_df = context.table("input").await?;
    if is_identity_source_projection(sql, input_df.schema().as_arrow()) {
        validate_plan(input_df.logical_plan())?;
        Ok(input_df)
    } else {
        build_query(context, sql).await
    }
}

/// Returns `true` when `sql` is a pure identity projection over `input`
/// (`SELECT * FROM input` or `SELECT c1, c2, ... FROM input` matching `input_schema`
/// fields in exact order with no filtering, grouping, ordering, aliasing, or expressions).
pub fn is_identity_source_projection(sql: &str, input_schema: &arrow::datatypes::Schema) -> bool {
    use datafusion::sql::sqlparser::{
        ast::{Expr, GroupByExpr, ObjectNamePart, SelectItem, SetExpr, Statement, TableFactor},
        dialect::GenericDialect,
        parser::Parser,
    };
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return false;
    };
    if statements.len() != 1 {
        return false;
    }
    let Statement::Query(query) = &statements[0] else {
        return false;
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return false;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.selection.is_some()
        || !matches!(
            &select.group_by,
            GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty()
        )
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.prewhere.is_some()
        || select.value_table_mode.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || select.from.len() != 1
    {
        return false;
    }
    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return false;
    }
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = &table_with_joins.relation
    else {
        return false;
    };
    if !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
        || name.0.len() != 1
    {
        return false;
    }
    let ObjectNamePart::Identifier(table_ident) = &name.0[0] else {
        return false;
    };
    let is_input_table = if table_ident.quote_style.is_some() {
        table_ident.value == "input"
    } else {
        table_ident.value.eq_ignore_ascii_case("input")
    };
    if !is_input_table {
        return false;
    }
    if select.projection.len() == 1
        && let SelectItem::Wildcard(options) = &select.projection[0]
    {
        return options.opt_ilike.is_none()
            && options.opt_exclude.is_none()
            && options.opt_except.is_none()
            && options.opt_replace.is_none()
            && options.opt_rename.is_none();
    }
    if select.projection.len() != input_schema.fields().len() {
        return false;
    }
    select
        .projection
        .iter()
        .zip(input_schema.fields().iter())
        .all(|(item, field)| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                if ident.quote_style.is_some() {
                    ident.value == *field.name()
                } else {
                    ident.value.eq_ignore_ascii_case(field.name()) && ident.value == *field.name()
                }
            }
            _ => false,
        })
}
