//! Compilation and validation of declarative `Predicate` and `ScalarExpr` ASTs.

use super::*;
use crate::model::{
    AggregateFunction, AggregateTransform, BinaryOp, ComparisonOp, JoinSide, JoinTransform,
    LiteralValue, Predicate, ScalarExpr,
};
use arrow::datatypes::Schema as ArrowSchema;
use std::collections::{BTreeMap, BTreeSet};
use write::identifier as quote;

pub(super) fn is_valid_identifier(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty() && !trimmed.to_ascii_lowercase().starts_with("__docspec_")
}

/// Validates a `Predicate` AST syntactically before schema resolution.
pub(super) fn validate_predicate(predicate: &Predicate) -> Result<()> {
    match predicate {
        Predicate::Comparison { left, right, .. }
        | Predicate::Eq { left, right }
        | Predicate::NotEq { left, right }
        | Predicate::Lt { left, right }
        | Predicate::LtEq { left, right }
        | Predicate::Gt { left, right }
        | Predicate::GtEq { left, right } => {
            validate_scalar_expr(left)?;
            validate_scalar_expr(right)
        }
        Predicate::IsNull { expr } | Predicate::IsNotNull { expr } => validate_scalar_expr(expr),
        Predicate::InList { expr, list, .. } => {
            if list.is_empty() {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "in_list predicate requires at least one candidate value",
                ));
            }
            validate_scalar_expr(expr)?;
            for item in list {
                validate_scalar_expr(item)?;
            }
            Ok(())
        }
        Predicate::And { exprs } | Predicate::Or { exprs } => {
            if exprs.is_empty() {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "logical AND/OR predicate requires at least one clause",
                ));
            }
            for item in exprs {
                validate_predicate(item)?;
            }
            Ok(())
        }
        Predicate::Not { expr } => validate_predicate(expr),
        Predicate::Sql { expr } => validate_sql_fragment(expr),
    }
}

/// Validates a `ScalarExpr` AST syntactically before schema resolution.
pub(super) fn validate_scalar_expr(expr: &ScalarExpr) -> Result<()> {
    match expr {
        ScalarExpr::Column { field } => {
            let check_part = |part: &str| is_valid_identifier(part);
            let valid = if let Some((prefix, rest)) = field.split_once('.') {
                matches!(prefix, "l" | "r" | "left" | "right") && check_part(rest)
            } else {
                check_part(field)
            };
            if !valid {
                return Err(Error::refusal(
                    "invalid_refresh",
                    format!("invalid or reserved column reference '{field}' in expression"),
                ));
            }
            Ok(())
        }
        ScalarExpr::Literal { value } => {
            if let LiteralValue::Float64(f) = value
                && !f.is_finite()
            {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "non-finite float literal in expression",
                ));
            }
            Ok(())
        }
        ScalarExpr::Coalesce { exprs } => {
            if exprs.is_empty() {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "coalesce expression requires at least one argument",
                ));
            }
            for item in exprs {
                validate_scalar_expr(item)?;
            }
            Ok(())
        }
        ScalarExpr::Binary { left, right, .. } => {
            validate_scalar_expr(left)?;
            validate_scalar_expr(right)
        }
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            if when_then.is_empty() {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "case expression requires at least one WHEN/THEN branch",
                ));
            }
            for branch in when_then {
                validate_predicate(&branch.when)?;
                validate_scalar_expr(&branch.then)?;
            }
            if let Some(else_e) = else_expr {
                validate_scalar_expr(else_e)?;
            }
            Ok(())
        }
        ScalarExpr::Cast { expr, data_type } => {
            if data_type.trim().is_empty() {
                return Err(Error::refusal(
                    "invalid_refresh",
                    "cast expression requires a non-empty target type",
                ));
            }
            validate_scalar_expr(expr)
        }
        ScalarExpr::Predicate { predicate } => validate_predicate(predicate),
        ScalarExpr::Sql { expr } => validate_sql_fragment(expr),
    }
}

/// Validates a map of named scalar projections.
pub(super) fn validate_projections(projections: &BTreeMap<String, ScalarExpr>) -> Result<()> {
    let mut lower_names = BTreeSet::new();
    for (name, expr) in projections {
        if !is_valid_identifier(name) || !lower_names.insert(name.to_ascii_lowercase()) {
            return Err(Error::refusal(
                "invalid_refresh",
                format!("projection name '{name}' is empty, reserved, or duplicated"),
            ));
        }
        validate_scalar_expr(expr)?;
    }
    Ok(())
}

fn validate_sql_fragment(sql: &str) -> Result<()> {
    if sql.trim().is_empty() {
        return Err(Error::refusal(
            "invalid_refresh",
            "SQL expression cannot be empty",
        ));
    }
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let wrapped = format!("SELECT {sql} FROM input");
    let statements = Parser::parse_sql(&GenericDialect {}, &wrapped)
        .map_err(|e| Error::refusal("invalid_refresh", format!("invalid SQL expression: {e}")))?;
    if statements.len() != 1 {
        return Err(Error::refusal(
            "invalid_refresh",
            "expected a single scalar SQL expression",
        ));
    }
    let mut refs = BTreeSet::new();
    collect_sql_identifiers(sql, &mut refs)?;
    if refs
        .iter()
        .any(|r| r.to_ascii_lowercase().starts_with("__docspec_"))
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "SQL expression cannot reference reserved __docspec_ columns",
        ));
    }
    Ok(())
}

fn collect_sql_identifiers(sql: &str, out: &mut BTreeSet<String>) -> Result<()> {
    use datafusion::sql::sqlparser::{
        ast::{Expr as SqlExpr, SelectItem, SetExpr, Statement, Visit, Visitor},
        dialect::GenericDialect,
        parser::Parser,
    };
    use std::ops::ControlFlow;

    struct IdentVisitor<'a>(&'a mut BTreeSet<String>, bool);
    impl Visitor for IdentVisitor<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            match expr {
                SqlExpr::Subquery(_) | SqlExpr::InSubquery { .. } | SqlExpr::Exists { .. } => {
                    self.1 = true;
                    return ControlFlow::Break(());
                }
                SqlExpr::Identifier(ident) => {
                    self.0.insert(ident.value.clone());
                }
                SqlExpr::CompoundIdentifier(parts) => {
                    if parts.len() == 2 {
                        self.0
                            .insert(format!("{}.{}", parts[0].value, parts[1].value));
                    } else if let Some(last) = parts.last() {
                        self.0.insert(last.value.clone());
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let wrapped = format!("SELECT {sql} FROM input");
    let statements = Parser::parse_sql(&GenericDialect {}, &wrapped)
        .map_err(|e| Error::refusal("invalid_refresh", format!("invalid SQL expression: {e}")))?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err(Error::refusal("invalid_refresh", "invalid SQL expression"));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(Error::refusal("invalid_refresh", "invalid SQL expression"));
    };
    if select.projection.len() != 1 {
        return Err(Error::refusal(
            "invalid_refresh",
            "expected a single scalar SQL expression",
        ));
    }
    let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
        return Err(Error::refusal(
            "invalid_refresh",
            "SQL expression cannot use wildcards or aliases",
        ));
    };
    let mut visitor = IdentVisitor(out, false);
    let _ = expr.visit(&mut visitor);
    if visitor.1 {
        return Err(Error::refusal(
            "invalid_refresh",
            "subqueries are not permitted in declarative expressions",
        ));
    }
    Ok(())
}

/// Collects all column references from a `ScalarExpr`.
pub(super) fn collect_scalar_refs(expr: &ScalarExpr, out: &mut BTreeSet<String>) -> Result<()> {
    match expr {
        ScalarExpr::Column { field } => {
            out.insert(field.clone());
            Ok(())
        }
        ScalarExpr::Literal { .. } => Ok(()),
        ScalarExpr::Coalesce { exprs } => {
            for e in exprs {
                collect_scalar_refs(e, out)?;
            }
            Ok(())
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_scalar_refs(left, out)?;
            collect_scalar_refs(right, out)
        }
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for branch in when_then {
                collect_predicate_refs(&branch.when, out)?;
                collect_scalar_refs(&branch.then, out)?;
            }
            if let Some(else_e) = else_expr {
                collect_scalar_refs(else_e, out)?;
            }
            Ok(())
        }
        ScalarExpr::Cast { expr, .. } => collect_scalar_refs(expr, out),
        ScalarExpr::Predicate { predicate } => collect_predicate_refs(predicate, out),
        ScalarExpr::Sql { expr } => collect_sql_identifiers(expr, out),
    }
}

/// Collects all column references from a `Predicate`.
pub(super) fn collect_predicate_refs(
    predicate: &Predicate,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    match predicate {
        Predicate::Comparison { left, right, .. }
        | Predicate::Eq { left, right }
        | Predicate::NotEq { left, right }
        | Predicate::Lt { left, right }
        | Predicate::LtEq { left, right }
        | Predicate::Gt { left, right }
        | Predicate::GtEq { left, right } => {
            collect_scalar_refs(left, out)?;
            collect_scalar_refs(right, out)
        }
        Predicate::IsNull { expr } | Predicate::IsNotNull { expr } => {
            collect_scalar_refs(expr, out)
        }
        Predicate::InList { expr, list, .. } => {
            collect_scalar_refs(expr, out)?;
            for item in list {
                collect_scalar_refs(item, out)?;
            }
            Ok(())
        }
        Predicate::And { exprs } | Predicate::Or { exprs } => {
            for item in exprs {
                collect_predicate_refs(item, out)?;
            }
            Ok(())
        }
        Predicate::Not { expr } => collect_predicate_refs(expr, out),
        Predicate::Sql { expr } => collect_sql_identifiers(expr, out),
    }
}

fn render_literal(lit: &LiteralValue) -> String {
    match lit {
        LiteralValue::Null => "NULL".to_owned(),
        LiteralValue::Bool(true) => "TRUE".to_owned(),
        LiteralValue::Bool(false) => "FALSE".to_owned(),
        LiteralValue::Int64(v) => format!("CAST({v} AS BIGINT)"),
        LiteralValue::Float64(v) => {
            let s = format!("{v:?}");
            format!("CAST({s} AS DOUBLE)")
        }
        LiteralValue::String(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

fn render_scalar_with_resolver<F>(expr: &ScalarExpr, resolve: &F) -> Result<String>
where
    F: Fn(&str) -> Result<String>,
{
    match expr {
        ScalarExpr::Column { field } => resolve(field),
        ScalarExpr::Literal { value } => Ok(render_literal(value)),
        ScalarExpr::Coalesce { exprs } => {
            let args = exprs
                .iter()
                .map(|e| render_scalar_with_resolver(e, resolve))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("COALESCE({})", args.join(", ")))
        }
        ScalarExpr::Binary { left, op, right } => {
            let l = render_scalar_with_resolver(left, resolve)?;
            let r = render_scalar_with_resolver(right, resolve)?;
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Subtract => "-",
                BinaryOp::Multiply => "*",
                BinaryOp::Divide => "/",
                BinaryOp::Modulo => "%",
                BinaryOp::Concat => "||",
            };
            Ok(format!("({l} {op_str} {r})"))
        }
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            let mut branches = Vec::with_capacity(when_then.len());
            for branch in when_then {
                let w = render_predicate_with_resolver(&branch.when, resolve)?;
                let t = render_scalar_with_resolver(&branch.then, resolve)?;
                branches.push(format!("WHEN {w} THEN {t}"));
            }
            let else_sql = match else_expr {
                Some(e) => format!(" ELSE {}", render_scalar_with_resolver(e, resolve)?),
                None => String::new(),
            };
            Ok(format!("(CASE {}{else_sql} END)", branches.join(" ")))
        }
        ScalarExpr::Cast { expr, data_type } => {
            let inner = render_scalar_with_resolver(expr, resolve)?;
            Ok(format!("CAST({inner} AS {data_type})"))
        }
        ScalarExpr::Predicate { predicate } => render_predicate_with_resolver(predicate, resolve),
        ScalarExpr::Sql { expr } => rewrite_sql_identifiers(expr, resolve),
    }
}

fn render_predicate_with_resolver<F>(predicate: &Predicate, resolve: &F) -> Result<String>
where
    F: Fn(&str) -> Result<String>,
{
    let cmp = |left: &ScalarExpr, op: ComparisonOp, right: &ScalarExpr| -> Result<String> {
        let l = render_scalar_with_resolver(left, resolve)?;
        let r = render_scalar_with_resolver(right, resolve)?;
        let op_str = match op {
            ComparisonOp::Eq => "=",
            ComparisonOp::NotEq => "<>",
            ComparisonOp::Lt => "<",
            ComparisonOp::LtEq => "<=",
            ComparisonOp::Gt => ">",
            ComparisonOp::GtEq => ">=",
        };
        Ok(format!("({l} {op_str} {r})"))
    };
    match predicate {
        Predicate::Comparison { left, op, right } => cmp(left, *op, right),
        Predicate::Eq { left, right } => cmp(left, ComparisonOp::Eq, right),
        Predicate::NotEq { left, right } => cmp(left, ComparisonOp::NotEq, right),
        Predicate::Lt { left, right } => cmp(left, ComparisonOp::Lt, right),
        Predicate::LtEq { left, right } => cmp(left, ComparisonOp::LtEq, right),
        Predicate::Gt { left, right } => cmp(left, ComparisonOp::Gt, right),
        Predicate::GtEq { left, right } => cmp(left, ComparisonOp::GtEq, right),
        Predicate::IsNull { expr } => {
            let inner = render_scalar_with_resolver(expr, resolve)?;
            Ok(format!("({inner} IS NULL)"))
        }
        Predicate::IsNotNull { expr } => {
            let inner = render_scalar_with_resolver(expr, resolve)?;
            Ok(format!("({inner} IS NOT NULL)"))
        }
        Predicate::InList {
            expr,
            list,
            negated,
        } => {
            let inner = render_scalar_with_resolver(expr, resolve)?;
            let items = list
                .iter()
                .map(|i| render_scalar_with_resolver(i, resolve))
                .collect::<Result<Vec<_>>>()?;
            let kw = if *negated { "NOT IN" } else { "IN" };
            Ok(format!("({inner} {kw} ({}))", items.join(", ")))
        }
        Predicate::And { exprs } => {
            let parts = exprs
                .iter()
                .map(|e| render_predicate_with_resolver(e, resolve))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        Predicate::Or { exprs } => {
            let parts = exprs
                .iter()
                .map(|e| render_predicate_with_resolver(e, resolve))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        Predicate::Not { expr } => {
            let inner = render_predicate_with_resolver(expr, resolve)?;
            Ok(format!("(NOT {inner})"))
        }
        Predicate::Sql { expr } => {
            let s = rewrite_sql_identifiers(expr, resolve)?;
            Ok(format!("({s})"))
        }
    }
}

fn rewrite_sql_identifiers<F>(sql: &str, resolve: &F) -> Result<String>
where
    F: Fn(&str) -> Result<String>,
{
    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Ident, VisitMut, VisitorMut};
    use std::ops::ControlFlow;

    struct Rewriter<'a, F>(&'a F, Option<Error>);
    impl<F> VisitorMut for Rewriter<'_, F>
    where
        F: Fn(&str) -> Result<String>,
    {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
            match expr {
                SqlExpr::Identifier(ident) => match (self.0)(&ident.value) {
                    Ok(replacement) => {
                        if let Ok(parsed) = parse_single_sql_expr(&replacement) {
                            *expr = parsed;
                        } else {
                            *ident = Ident::with_quote('"', replacement);
                        }
                        return ControlFlow::Continue(());
                    }
                    Err(err) => {
                        self.1 = Some(err);
                        return ControlFlow::Break(());
                    }
                },
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    let combined = format!("{}.{}", parts[0].value, parts[1].value);
                    match (self.0)(&combined) {
                        Ok(replacement) => {
                            if let Ok(parsed) = parse_single_sql_expr(&replacement) {
                                *expr = parsed;
                            }
                            return ControlFlow::Continue(());
                        }
                        Err(err) => {
                            self.1 = Some(err);
                            return ControlFlow::Break(());
                        }
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let mut parsed = parse_single_sql_expr(sql)?;
    let mut rewriter = Rewriter(resolve, None);
    let _ = parsed.visit(&mut rewriter);
    if let Some(err) = rewriter.1 {
        return Err(err);
    }
    Ok(parsed.to_string())
}

fn parse_single_sql_expr(sql: &str) -> Result<datafusion::sql::sqlparser::ast::Expr> {
    use datafusion::sql::sqlparser::{
        ast::{SelectItem, SetExpr, Statement},
        dialect::GenericDialect,
        parser::Parser,
    };
    let wrapped = format!("SELECT {sql} FROM input");
    let mut statements = Parser::parse_sql(&GenericDialect {}, &wrapped)
        .map_err(|e| Error::refusal("invalid_refresh", format!("invalid SQL expression: {e}")))?;
    let Some(Statement::Query(query)) = statements.pop() else {
        return Err(Error::refusal("invalid_refresh", "invalid SQL expression"));
    };
    let SetExpr::Select(mut select) = *query.body else {
        return Err(Error::refusal("invalid_refresh", "invalid SQL expression"));
    };
    let Some(SelectItem::UnnamedExpr(expr)) = select.projection.pop() else {
        return Err(Error::refusal("invalid_refresh", "invalid SQL expression"));
    };
    Ok(expr)
}

/// Compiles `JoinTransform` output projections and optional filter predicate into SQL.
pub(super) fn compile_join_sql(
    spec: &JoinTransform,
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
) -> Result<(Vec<String>, Option<String>)> {
    let projections = spec.effective_projections();
    let filter = spec.effective_filter();
    validate_projections(&projections)?;
    if let Some(f) = &filter {
        validate_predicate(f)?;
    }

    let left_public: BTreeSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| !name.starts_with("__docspec_"))
        .collect();
    let right_public: BTreeSet<&str> = right_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| !name.starts_with("__docspec_"))
        .collect();

    let field_map: BTreeMap<&str, (JoinSide, &str)> = spec
        .join
        .fields
        .iter()
        .map(|f| (f.name.as_str(), (f.side, f.field.as_str())))
        .collect();

    let resolve_base = |col_ref: &str| -> Result<String> {
        if let Some((prefix, rest)) = col_ref.split_once('.') {
            match prefix {
                "l" | "left" => {
                    if !left_public.contains(rest) {
                        return Err(Error::refusal(
                            "unknown_field",
                            format!("join left input has no field '{rest}'"),
                        ));
                    }
                    return Ok(format!("l.{}", quote(rest)));
                }
                "r" | "right" => {
                    if !right_public.contains(rest) {
                        return Err(Error::refusal(
                            "unknown_field",
                            format!("join right input has no field '{rest}'"),
                        ));
                    }
                    return Ok(format!("r.{}", quote(rest)));
                }
                _ => {
                    return Err(Error::refusal(
                        "invalid_refresh",
                        format!("unknown table qualifier '{prefix}' in join expression"),
                    ));
                }
            }
        }
        if let Some((side, src_field)) = field_map.get(col_ref) {
            let alias = match side {
                JoinSide::Left => "l",
                JoinSide::Right => "r",
            };
            return Ok(format!("{alias}.{}", quote(src_field)));
        }
        match (
            left_public.contains(col_ref),
            right_public.contains(col_ref),
        ) {
            (true, false) => Ok(format!("l.{}", quote(col_ref))),
            (false, true) => Ok(format!("r.{}", quote(col_ref))),
            (true, true) => Err(Error::refusal(
                "ambiguous_column",
                format!(
                    "column '{col_ref}' exists in both join inputs; qualify as l.{col_ref} or r.{col_ref}"
                ),
            )),
            (false, false) => Err(Error::refusal(
                "unknown_field",
                format!("column '{col_ref}' is absent from join inputs and output fields"),
            )),
        }
    };

    // Compile projections with cycle detection so projections and filters can reference projections.
    let mut compiled_projections: BTreeMap<String, String> = BTreeMap::new();
    fn compile_one_proj<'a>(
        name: &str,
        projections: &'a BTreeMap<String, ScalarExpr>,
        compiled: &mut BTreeMap<String, String>,
        visiting: &mut BTreeSet<String>,
        resolve_base: &dyn Fn(&str) -> Result<String>,
    ) -> Result<String> {
        if let Some(done) = compiled.get(name) {
            return Ok(done.clone());
        }
        if !visiting.insert(name.to_owned()) {
            return Err(Error::refusal(
                "invalid_refresh",
                format!("cyclic projection dependency involving '{name}'"),
            ));
        }
        let expr = projections.get(name).ok_or_else(|| {
            Error::refusal("unknown_field", format!("missing projection '{name}'"))
        })?;
        let mut deps = BTreeSet::new();
        collect_scalar_refs(expr, &mut deps)?;
        for col_ref in deps {
            if col_ref != name && projections.contains_key(&col_ref) {
                compile_one_proj(&col_ref, projections, compiled, visiting, resolve_base)?;
            }
        }
        let rendered = render_scalar_with_resolver(expr, &|col_ref| {
            if col_ref != name
                && let Some(dep_sql) = compiled.get(col_ref)
            {
                Ok(dep_sql.clone())
            } else {
                resolve_base(col_ref)
            }
        })?;
        visiting.remove(name);
        compiled.insert(name.to_owned(), rendered.clone());
        Ok(rendered)
    }

    for name in projections.keys() {
        let mut visiting = BTreeSet::new();
        compile_one_proj(
            name,
            &projections,
            &mut compiled_projections,
            &mut visiting,
            &resolve_base,
        )?;
    }

    let resolve_with_projections = |col_ref: &str| -> Result<String> {
        if let Some(proj_sql) = compiled_projections.get(col_ref) {
            Ok(proj_sql.clone())
        } else {
            resolve_base(col_ref)
        }
    };

    let compiled_filter = match &filter {
        Some(pred) => Some(render_predicate_with_resolver(
            pred,
            &resolve_with_projections,
        )?),
        None => None,
    };

    let mut select_items = Vec::new();
    let mut emitted_projections = BTreeSet::new();
    for field in &spec.join.fields {
        if let Some(proj_sql) = compiled_projections.get(&field.name) {
            select_items.push(format!("{proj_sql} AS {}", quote(&field.name)));
            emitted_projections.insert(field.name.clone());
        } else {
            let side = match field.side {
                JoinSide::Left => "l",
                JoinSide::Right => "r",
            };
            select_items.push(format!(
                "{side}.{} AS {}",
                quote(&field.field),
                quote(&field.name)
            ));
        }
    }
    for (name, proj_sql) in &compiled_projections {
        if !emitted_projections.contains(name) {
            select_items.push(format!("{proj_sql} AS {}", quote(name)));
        }
    }

    Ok((select_items, compiled_filter))
}

/// Compiled SQL components for an `AggregateTransform` with optional filters and projections.
#[derive(Clone, Debug, Default)]
pub(super) struct CompiledAggregationSql {
    /// Pre-aggregation `WHERE` clause over input columns (and pre-projections).
    pub pre_where: Option<String>,
    /// Pre-aggregation computed columns `(alias, sql_expr)` needed before `GROUP BY`.
    pub pre_projections: Vec<(String, String)>,
    /// Post-aggregation `HAVING` filter over group keys and aggregate outputs.
    pub post_having: Option<String>,
    /// Post-aggregation computed columns `(alias, sql_expr)` over group keys and aggregate outputs.
    pub post_projections: Vec<(String, String)>,
}

/// Compiles `AggregateTransform` filters and projections into pre-aggregation and post-aggregation SQL.
pub(super) fn compile_aggregate_sql(
    spec: &AggregateTransform,
    input_schema: &ArrowSchema,
) -> Result<CompiledAggregationSql> {
    let projections = spec.effective_projections();
    let filter = spec.effective_filter();
    if projections.is_empty() && filter.is_none() {
        return Ok(CompiledAggregationSql::default());
    }
    validate_projections(&projections)?;
    if let Some(f) = &filter {
        validate_predicate(f)?;
    }

    let input_public: BTreeSet<String> = input_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| !name.starts_with("__docspec_"))
        .collect();
    let key_set: BTreeSet<String> = spec.grouping.keys.iter().cloned().collect();
    let agg_set: BTreeSet<String> = spec
        .grouping
        .aggregates
        .iter()
        .map(|f| f.name.clone())
        .collect();

    // Determine which projection names are referenced as pre-aggregation inputs by keys or aggregates.
    let mut needed_pre: BTreeSet<String> = BTreeSet::new();
    for key in &spec.grouping.keys {
        if projections.contains_key(key) {
            needed_pre.insert(key.clone());
        }
    }
    for agg in &spec.grouping.aggregates {
        match &agg.function {
            AggregateFunction::CountRows => {}
            AggregateFunction::Count { field }
            | AggregateFunction::CountDistinct { field }
            | AggregateFunction::CollectSet { field }
            | AggregateFunction::Sum { field }
            | AggregateFunction::Min { field }
            | AggregateFunction::Max { field }
            | AggregateFunction::FirstValue { field } => {
                if projections.contains_key(field) {
                    needed_pre.insert(field.clone());
                }
            }
            AggregateFunction::FirstValueBy { field, order_by } => {
                if projections.contains_key(field) {
                    needed_pre.insert(field.clone());
                }
                for ob in order_by {
                    if projections.contains_key(&ob.field) {
                        needed_pre.insert(ob.field.clone());
                    }
                }
            }
            AggregateFunction::CollectDistinctWithProvenance {
                field,
                provenance_fields,
                order_by,
            } => {
                if projections.contains_key(field) {
                    needed_pre.insert(field.clone());
                }
                for pf in provenance_fields {
                    if projections.contains_key(pf) {
                        needed_pre.insert(pf.clone());
                    }
                }
                for ob in order_by {
                    if projections.contains_key(&ob.field) {
                        needed_pre.insert(ob.field.clone());
                    }
                }
            }
        }
    }

    // Separate projections into pre-aggregation vs post-aggregation.
    let mut pre_proj_exprs: BTreeMap<String, ScalarExpr> = BTreeMap::new();
    let mut post_proj_exprs: BTreeMap<String, ScalarExpr> = BTreeMap::new();
    for (name, expr) in &projections {
        let mut refs = BTreeSet::new();
        collect_scalar_refs(expr, &mut refs)?;
        let refs_agg = refs.iter().any(|r| agg_set.contains(r));
        if needed_pre.contains(name) {
            if refs_agg {
                return Err(Error::refusal(
                    "invalid_refresh",
                    format!(
                        "pre-aggregation projection '{name}' cannot reference aggregate outputs"
                    ),
                ));
            }
            pre_proj_exprs.insert(name.clone(), expr.clone());
        } else if refs_agg
            || refs
                .iter()
                .all(|r| key_set.contains(r) || agg_set.contains(r) || projections.contains_key(r))
        {
            post_proj_exprs.insert(name.clone(), expr.clone());
        } else {
            // References a non-key input column directly: can only be evaluated pre-aggregation
            // if used by a filter, not as an unaggregated output column.
            pre_proj_exprs.insert(name.clone(), expr.clone());
        }
    }

    // Compile pre-projections over `input_public`.
    let mut compiled_pre: BTreeMap<String, String> = BTreeMap::new();
    for (name, expr) in &pre_proj_exprs {
        let sql = render_scalar_with_resolver(expr, &|col_ref| {
            if let Some(existing) = compiled_pre.get(col_ref) {
                Ok(existing.clone())
            } else if input_public.contains(col_ref) {
                Ok(quote(col_ref))
            } else {
                Err(Error::refusal(
                    "unknown_field",
                    format!(
                        "unknown input column '{col_ref}' in pre-aggregation projection '{name}'"
                    ),
                ))
            }
        })?;
        compiled_pre.insert(name.clone(), sql);
    }

    // Compile post-projections over `key_set ∪ agg_set`.
    let mut compiled_post: BTreeMap<String, String> = BTreeMap::new();
    let resolve_post_base = |col_ref: &str| -> Result<String> {
        if key_set.contains(col_ref) || agg_set.contains(col_ref) {
            Ok(quote(col_ref))
        } else {
            Err(Error::refusal(
                "unknown_field",
                format!(
                    "post-aggregation expression column '{col_ref}' must be a group key or aggregate output"
                ),
            ))
        }
    };
    for name in post_proj_exprs.keys() {
        let expr = &post_proj_exprs[name];
        let sql = render_scalar_with_resolver(expr, &|col_ref| {
            if col_ref != name
                && let Some(existing) = compiled_post.get(col_ref)
            {
                Ok(existing.clone())
            } else {
                resolve_post_base(col_ref)
            }
        })?;
        compiled_post.insert(name.clone(), sql);
    }

    // Split filter into top-level AND conjuncts and classify each as pre_where or post_having.
    let mut pre_conjuncts = Vec::new();
    let mut post_conjuncts = Vec::new();
    if let Some(pred) = &filter {
        let mut flat = Vec::new();
        flatten_and(pred, &mut flat);
        for clause in flat {
            let mut refs = BTreeSet::new();
            collect_predicate_refs(clause, &mut refs)?;
            let uses_post = refs
                .iter()
                .any(|r| agg_set.contains(r) || compiled_post.contains_key(r));
            if uses_post {
                let sql = render_predicate_with_resolver(clause, &|col_ref| {
                    if let Some(proj_sql) = compiled_post.get(col_ref) {
                        Ok(proj_sql.clone())
                    } else {
                        resolve_post_base(col_ref)
                    }
                })?;
                post_conjuncts.push(sql);
            } else {
                let sql = render_predicate_with_resolver(clause, &|col_ref| {
                    if let Some(proj_sql) = compiled_pre.get(col_ref) {
                        Ok(proj_sql.clone())
                    } else if input_public.contains(col_ref) {
                        Ok(quote(col_ref))
                    } else {
                        Err(Error::refusal(
                            "unknown_field",
                            format!("unknown column '{col_ref}' in aggregate filter"),
                        ))
                    }
                })?;
                pre_conjuncts.push(sql);
            }
        }
    }

    // Any pre_projection that is NOT in needed_pre and NOT referenced by pre_conjuncts
    // was an invalid attempt to project an unaggregated non-key column.
    for name in compiled_pre.keys() {
        if !needed_pre.contains(name) {
            let used_in_filter = filter.as_ref().is_some_and(|f| {
                let mut refs = BTreeSet::new();
                collect_predicate_refs(f, &mut refs).is_ok() && refs.contains(name)
            });
            if !used_in_filter {
                return Err(Error::refusal(
                    "invalid_refresh",
                    format!(
                        "aggregate output projection '{name}' cannot reference unaggregated non-key input columns"
                    ),
                ));
            }
        }
    }

    Ok(CompiledAggregationSql {
        pre_where: (!pre_conjuncts.is_empty()).then(|| pre_conjuncts.join(" AND ")),
        pre_projections: compiled_pre.into_iter().collect(),
        post_having: (!post_conjuncts.is_empty()).then(|| post_conjuncts.join(" AND ")),
        post_projections: compiled_post.into_iter().collect(),
    })
}

fn flatten_and<'a>(pred: &'a Predicate, out: &mut Vec<&'a Predicate>) {
    if let Predicate::And { exprs } = pred {
        for e in exprs {
            flatten_and(e, out);
        }
    } else {
        out.push(pred);
    }
}
