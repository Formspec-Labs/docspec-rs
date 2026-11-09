//! Refuse casts DataFusion's type coercion adds between value families.
//!
//! DataFusion makes mismatched SQL plan by casting one side: `integer = text`
//! compares text, and `date >= 20200101` reads the integer as days since 1970.
//! Both plan and run, then return wrong rows. Schema-only planning refuses them
//! and names each cast, so the author writes the conversion they mean.
//!
//! The check reads DataFusion's own analyzer. Its observer sees the plan just
//! before and just after the `type_coercion` rule. Casts present before that
//! rule are written in the SQL (`CAST`, `::`, `DATE '…'`) and stay allowed.
//! Every other cast was added by coercion; it is refused when its input and
//! target belong to different families: text, number, date/time or boolean.
//! Null and other types (binary, interval, struct) belong to no family.
//!
//! Bound parameters enter the plan as literals of their supplied JSON type, so
//! a text value compared with an integer column is refused like a text literal.
//! The check cannot see casts DataFusion's SQL planner inserts before analysis,
//! such as those unifying a `VALUES` list. A written cast whose input coercion
//! changes beyond added casts (a coerced subquery or window frame inside it) is
//! reported as added; it fails loudly, never silently.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result};
use arrow::datatypes::DataType;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    execution::SessionState,
    logical_expr::{Expr, ExprSchemable, LogicalPlan, expr::Cast, utils::merge_schema},
    optimizer::{AnalyzerRule, analyzer::type_coercion::TypeCoercion},
};
use std::collections::{BTreeSet, HashMap};

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

/// Identifies a cast by target and input, ignoring casts and aliases within.
///
/// Coercion rewrites bottom-up, so a written `CAST(x + 1.5 AS VARCHAR)` gains
/// an inner `CAST(x AS Float64)`; stripping keeps both forms one key.
fn key(cast: &Cast) -> datafusion::common::Result<(DataType, Expr)> {
    let input = (*cast.expr).clone().transform(|part| {
        Ok(match part {
            Expr::Cast(inner) => Transformed::yes(*inner.expr),
            Expr::Alias(alias) => Transformed::yes(*alias.expr),
            other => Transformed::no(other),
        })
    })?;
    Ok((cast.data_type.clone(), input.data))
}

fn each_cast(
    plan: &LogicalPlan,
    mut visit: impl FnMut(&Cast, &LogicalPlan) -> datafusion::common::Result<()>,
) -> datafusion::common::Result<()> {
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expression| {
            expression.apply(|part| {
                if let Expr::Cast(cast) = part {
                    visit(cast, node)?;
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .map(|_| ())
}

/// Lists casts in `coerced` that are absent from `written` and cross families.
fn added_casts(
    written: &LogicalPlan,
    coerced: &LogicalPlan,
) -> datafusion::common::Result<BTreeSet<String>> {
    let mut remaining = HashMap::<_, usize>::new();
    each_cast(written, |cast, _| {
        *remaining.entry(key(cast)?).or_default() += 1;
        Ok(())
    })?;
    let mut added = BTreeSet::new();
    each_cast(coerced, |cast, node| {
        if let Some(count) = remaining.get_mut(&key(cast)?).filter(|count| **count > 0) {
            *count -= 1;
            return Ok(());
        }
        // Coercion types each expression against its node's merged inputs.
        let from = cast.expr.get_type(&merge_schema(&node.inputs()))?;
        if let (Some(source), Some(target)) = (family(&from), family(&cast.data_type))
            && source != target
        {
            added.insert(format!("{} ({from} to {})", cast.expr, cast.data_type));
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
                added = Some(added_casts(&written, analyzed));
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
            Field::new("code", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
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
            "SELECT 1 FROM t WHERE day >= 20200101",
            "SELECT 1 FROM t WHERE day >= '2020-01-01'",
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
    }

    #[tokio::test]
    async fn written_casts_nulls_and_same_family_coercion_pass() {
        for sql in [
            "SELECT 1 FROM t AS a JOIN t AS b ON CAST(a.id AS VARCHAR) = b.code",
            "SELECT 1 FROM t WHERE day >= DATE '2020-01-01'",
            "SELECT 1 FROM t WHERE day >= CAST('2020-01-01' AS DATE)",
            "SELECT 1 FROM t WHERE id = code::BIGINT",
            "SELECT 1 FROM t WHERE id = TRY_CAST(code AS BIGINT)",
            "SELECT CAST(id + 1.5 AS VARCHAR) = code FROM t",
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
