//! Refuse casts DataFusion's type coercion adds between value families.
//!
//! DataFusion makes mismatched SQL plan by casting one side: `integer = text`
//! compares text, and `date >= 20200101` reads the integer as days since 1970.
//! Both plan and run, then return wrong rows. Schema-only planning refuses them
//! and names each cast, so the author writes the conversion they mean.
//!
//! The check reads DataFusion's own analyzer. Its observer sees the plan just
//! before and just after the `type_coercion` rule. Casts present before that
//! rule are written in the SQL (`CAST`, `::`, `DATE '…'`) and stay allowed:
//! marking them as `Expr::TryCast` on a throwaway clone before `TypeCoercion`
//! runs leaves only analyzer-added `Expr::Cast` nodes in the coerced output,
//! because `TypeCoercion` passes `Cast` and `TryCast` through identically and
//! never constructs `TryCast`. Every remaining `Expr::Cast` was added by
//! coercion; it is refused when its input and target belong to different
//! families: text, number, date/time or boolean. Null and other types (binary,
//! duration, interval, struct) belong to no family, and DataFusion's internal
//! `time_to_nanos` lowering inside `Duration(Nanosecond)` arithmetic is ignored.
//!
//! Bound parameters enter the plan as literals of their supplied JSON type, so
//! a text value compared with an integer column is refused like a text literal.
//! The check cannot see casts DataFusion's SQL planner inserts before analysis,
//! such as those unifying rows of an inline `VALUES` list.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    common::{
        DFSchema, ScalarValue,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    execution::SessionState,
    logical_expr::{
        Expr, ExprSchemable, LogicalPlan, Operator,
        expr::{Cast, TryCast},
        utils::merge_schema,
    },
    optimizer::{AnalyzerRule, analyzer::type_coercion::TypeCoercion},
};
use std::collections::{BTreeSet, HashSet};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Text,
    Number,
    Time,
    Boolean,
}

fn family(data_type: &DataType) -> Option<Family> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Family::Text),
        DataType::Boolean => Some(Family::Boolean),
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(..)
        | DataType::Time32(_)
        | DataType::Time64(_) => Some(Family::Time),
        DataType::Dictionary(_, value) => family(value),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            family(item.data_type())
        }
        numeric if numeric.is_numeric() => Some(Family::Number),
        _ => None,
    }
}

/// Marks every written `Expr::Cast` as `Expr::TryCast` prior to `TypeCoercion`.
///
/// `TypeCoercion` never creates `Expr::TryCast` and treats `Cast` and `TryCast`
/// identically for type inference and column naming, so every `Expr::Cast` in
/// the coerced result was added by the analyzer.
fn mark_written_casts(plan: LogicalPlan) -> datafusion::common::Result<LogicalPlan> {
    Ok(plan
        .transform_up_with_subqueries(|node| {
            node.map_expressions(|expression| {
                expression.transform(|part| {
                    Ok(match part {
                        Expr::Cast(Cast { expr, data_type }) => {
                            Transformed::yes(Expr::TryCast(TryCast::new(expr, data_type)))
                        }
                        other => Transformed::no(other),
                    })
                })
            })
        })?
        .data)
}

fn collect_try_casts(plan: &LogicalPlan) -> datafusion::common::Result<HashSet<Expr>> {
    let mut try_casts = HashSet::new();
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expression| {
            expression.apply(|part| {
                if matches!(part, Expr::TryCast(_)) {
                    try_casts.insert(part.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(try_casts)
}

fn restore_written_casts(
    expression: &Expr,
    original_try_casts: &HashSet<Expr>,
) -> datafusion::common::Result<Expr> {
    Ok(expression
        .clone()
        .transform_up(|part| {
            Ok(match part {
                Expr::TryCast(ref try_cast) if !original_try_casts.contains(&part) => {
                    Transformed::yes(Expr::Cast(Cast::new(
                        try_cast.expr.clone(),
                        try_cast.data_type.clone(),
                    )))
                }
                other => Transformed::no(other),
            })
        })?
        .data)
}

/// Recognizes DataFusion's internal `time_to_nanos` lowering inside
/// `Cast(..., Duration(Nanosecond))` for `DATE +/- TIME` and `TIME - TIME`.
fn time_to_nanos_operand<'a>(
    expression: &'a Expr,
    schema: &DFSchema,
) -> datafusion::common::Result<Option<&'a Expr>> {
    let cast_part = match expression {
        Expr::BinaryExpr(binary)
            if binary.op == Operator::Multiply
                && matches!(
                    binary.right.as_ref(),
                    Expr::Literal(
                        ScalarValue::Int64(Some(1_000 | 1_000_000 | 1_000_000_000)),
                        _
                    )
                ) =>
        {
            binary.left.as_ref()
        }
        other => other,
    };
    let Expr::Cast(outer) = cast_part else {
        return Ok(None);
    };
    if outer.data_type != DataType::Int64 {
        return Ok(None);
    }
    let candidate = match outer.expr.as_ref() {
        Expr::Cast(inner) if inner.data_type == DataType::Int32 => inner.expr.as_ref(),
        other => other,
    };
    Ok(matches!(
        candidate.get_type(schema)?,
        DataType::Time32(_) | DataType::Time64(_)
    )
    .then_some(candidate))
}

fn visit_expr(
    expression: &Expr,
    schema: &DFSchema,
    visit: &mut impl FnMut(&Cast, &DFSchema) -> datafusion::common::Result<()>,
) -> datafusion::common::Result<TreeNodeRecursion> {
    expression.apply(|part| {
        if let Expr::Cast(cast) = part {
            if cast.data_type == DataType::Duration(TimeUnit::Nanosecond)
                && let Some(operand) = time_to_nanos_operand(&cast.expr, schema)?
            {
                visit_expr(operand, schema, visit)?;
                return Ok(TreeNodeRecursion::Jump);
            }
            visit(cast, schema)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })
}

fn each_added_cast(
    plan: &LogicalPlan,
    mut visit: impl FnMut(&Cast, &DFSchema) -> datafusion::common::Result<()>,
) -> datafusion::common::Result<()> {
    plan.apply_with_subqueries(|node| {
        // TypeCoercion::coerce_join types equijoin sides against left and right
        // schemas separately; DFSchema::merge would drop unqualified right-side
        // columns that share a name with any left-side column.
        if let LogicalPlan::Join(join) = node {
            for (lhs, rhs) in &join.on {
                visit_expr(lhs, join.left.schema().as_ref(), &mut visit)?;
                visit_expr(rhs, join.right.schema().as_ref(), &mut visit)?;
            }
            if let Some(filter) = &join.filter {
                let schema = merge_schema(&node.inputs());
                visit_expr(filter, &schema, &mut visit)?;
            }
            return Ok(TreeNodeRecursion::Continue);
        }
        let mut schema = merge_schema(&node.inputs());
        if let LogicalPlan::TableScan(scan) = node {
            schema.merge(&DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &scan.source.schema(),
            )?);
        }
        node.apply_expressions(|expression| visit_expr(expression, &schema, &mut visit))
    })
    .map(|_| ())
}

/// Lists casts in `coerced` that are absent from `written` and cross families.
fn added_casts(
    coercion: &TypeCoercion,
    state: &SessionState,
    written: &LogicalPlan,
    analyzed: &LogicalPlan,
) -> datafusion::common::Result<BTreeSet<String>> {
    let original_try_casts = collect_try_casts(analyzed)?;
    let marked = coercion.analyze(mark_written_casts(written.clone())?, state.config_options())?;
    let mut added = BTreeSet::new();
    each_added_cast(&marked, |cast, schema| {
        let from = cast.expr.get_type(schema)?;
        if let (Some(source), Some(target)) = (family(&from), family(&cast.data_type))
            && source != target
        {
            let shown = restore_written_casts(&cast.expr, &original_try_casts)?;
            added.insert(format!("{shown} ({from} to {})", cast.data_type));
        }
        Ok(())
    })?;
    Ok(added)
}

/// Refuses a plan whose analysis would add a cast between type families.
///
/// # Errors
/// Refuses with `implicit_cast` naming each cast, or returns analyzer errors.
pub(super) fn refuse_implicit_casts(state: &SessionState, plan: &LogicalPlan) -> Result<()> {
    let coercion = TypeCoercion::new();
    let mut written = plan.clone();
    let mut added = None;
    state.analyzer().execute_and_check(
        plan.clone(),
        state.config_options(),
        |analyzed, rule| {
            if rule.name() == coercion.name() {
                added = Some(added_casts(&coercion, state, &written, analyzed));
            } else {
                written = analyzed.clone();
            }
        },
    )?;
    // A renamed or removed rule must not turn this check into a silent pass.
    let added = added.ok_or_else(|| {
        datafusion::common::DataFusionError::Internal(
            "DataFusion's type_coercion analyzer rule did not run".into(),
        )
    })??;
    if added.is_empty() {
        return Ok(());
    }
    Err(Error::refusal(
        "implicit_cast",
        format!(
            "SQL mixes text, number, date/time or boolean values without an explicit CAST; \
             write the conversion you mean: {}",
            added.into_iter().collect::<Vec<_>>().join("; ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::SchemaOnlyTable;
    use crate::{budget::Limits, job::Job};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    async fn refusal(sql: &str) -> Option<String> {
        bound_refusal(sql, serde_json::json!({})).await
    }

    async fn bound_refusal(sql: &str, parameters: serde_json::Value) -> Option<String> {
        let root = tempfile::tempdir().unwrap();
        let job = Job::new(Limits::default(), root.path().join("scratch"))
            .unwrap()
            .refusing_implicit_casts();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("small_id", DataType::Int32, true),
            Field::new("code", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
            Field::new("tod", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("tod_sec", DataType::Time32(TimeUnit::Second), true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("flag", DataType::Boolean, true),
        ]);
        job.context
            .register_table("t", Arc::new(SchemaOnlyTable(Arc::new(schema))))
            .unwrap();
        let parameters = serde_json::from_value(parameters).unwrap();
        match job.sql_bound(sql, &parameters).await {
            Ok(_) => None,
            Err(error) => {
                assert_eq!(error.code(), "implicit_cast", "{sql}: {error}");
                Some(error.to_string())
            }
        }
    }

    #[tokio::test]
    async fn refuses_added_casts_between_families_at_every_coercion_site() {
        for sql in [
            "SELECT 1 FROM t AS a JOIN t AS b ON a.id = b.code",
            "SELECT 1 FROM t LEFT SEMI JOIN (SELECT 1 AS code) USING (code)",
            "SELECT 1 FROM t NATURAL LEFT SEMI JOIN (SELECT 1 AS code)",
            "SELECT 1 FROM (SELECT '1' AS code) LEFT SEMI JOIN (SELECT 1 AS code) USING (code)",
            "SELECT 1 FROM t LEFT ANTI JOIN (SELECT 20200101 AS day) USING (day)",
            "SELECT 1 FROM t WHERE day >= 20200101",
            "SELECT 1 FROM t WHERE day >= '2020-01-01'",
            "SELECT 1 FROM t WHERE tod = '12:00:00'",
            "SELECT day + (CASE WHEN flag THEN tod ELSE 'bad' END) FROM t",
            "SELECT 1 FROM t WHERE code IN (1, 2)",
            "SELECT 1 FROM t WHERE id BETWEEN '1' AND '9'",
            "SELECT 1 FROM t WHERE code IN (SELECT id FROM t)",
            "SELECT CASE WHEN flag THEN id ELSE code END FROM t",
            "SELECT CASE code WHEN 1 THEN 'one' END FROM t",
            "SELECT id FROM t UNION ALL SELECT code FROM t",
            "SELECT coalesce(id, code) FROM t",
            "SELECT code || id FROM t",
            "SELECT code || flag FROM t",
            "SELECT 1 FROM t WHERE (SELECT max(id) FROM t) = code",
            "SELECT CAST(code || id AS DATE) FROM t",
            "SELECT make_array(id, code) FROM t",
            "SELECT concat(code, id) FROM t",
        ] {
            assert!(refusal(sql).await.is_some(), "{sql}");
        }
        let text = serde_json::json!({"p": "5"});
        assert!(
            bound_refusal("SELECT 1 FROM t WHERE id = $p", text)
                .await
                .is_some()
        );
        let message = refusal("SELECT 1 FROM t WHERE day >= 20200101")
            .await
            .unwrap();
        assert!(
            message.contains("Int64(20200101) (Int64 to Date32)"),
            "{message}"
        );
        let message = refusal("SELECT CAST(code || id AS DATE) FROM t")
            .await
            .unwrap();
        assert!(message.contains("t.id (Int64 to Utf8)"), "{message}");
        assert!(
            !message.contains("Date32"),
            "the written cast is not reported"
        );
        let message = refusal("SELECT 1 FROM t WHERE code = CAST(id AS INT)")
            .await
            .unwrap();
        assert!(
            message.contains("CAST(t.id AS Int32) (Int32 to Utf8)"),
            "{message}"
        );
        let message = refusal("SELECT 1 FROM t WHERE code = TRY_CAST(id AS INT)")
            .await
            .unwrap();
        assert!(
            message.contains("TRY_CAST(t.id AS Int32) (Int32 to Utf8)"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn written_casts_nulls_and_same_family_coercion_pass() {
        for sql in [
            "SELECT 1 FROM t AS a JOIN t AS b ON CAST(a.id AS VARCHAR) = b.code",
            "SELECT 1 FROM t LEFT SEMI JOIN (SELECT CAST(1 AS INT) AS id) USING (id)",
            "SELECT 1 FROM t WHERE day >= DATE '2020-01-01'",
            "SELECT 1 FROM t WHERE day >= CAST('2020-01-01' AS DATE)",
            "SELECT 1 FROM t WHERE id = code::BIGINT",
            "SELECT 1 FROM t WHERE id = TRY_CAST(code AS BIGINT)",
            "SELECT CAST(id + 1.5 AS VARCHAR) = code FROM t",
            "SELECT CAST((SELECT count(*) FROM t WHERE small_id > 10) AS VARCHAR) FROM t",
            "SELECT CAST(day + 7 AS VARCHAR) FROM t",
            "SELECT day + tod FROM t",
            "SELECT day + tod_sec FROM t",
            "SELECT tod - tod FROM t",
            "SELECT to_date(code || '01', '%Y%m%d') FROM t",
            "SELECT 1 FROM t WHERE at >= day AND id > 1.5 AND id < 2",
            "SELECT CASE WHEN flag THEN NULL ELSE id END FROM t",
            "SELECT CAST(NULL AS VARCHAR) FROM t UNION ALL SELECT code FROM t",
            "SELECT coalesce(id, 0) + count(*) FROM t GROUP BY id",
            "SELECT s FROM (VALUES (1, 'a')) AS v(n, s) WHERE n = 1",
            "WITH c AS (SELECT unnest(make_array(code)) AS item FROM t) SELECT item FROM c",
        ] {
            assert_eq!(refusal(sql).await, None, "{sql}");
        }
        for (sql, value) in [
            ("SELECT 1 FROM t WHERE id = $p", serde_json::json!(5)),
            (
                "SELECT 1 FROM t WHERE day >= CAST($p AS DATE)",
                serde_json::json!("2020-01-01"),
            ),
        ] {
            let parameters = serde_json::json!({ "p": value });
            assert_eq!(bound_refusal(sql, parameters).await, None, "{sql}");
        }
    }
}
