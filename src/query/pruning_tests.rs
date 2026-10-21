//! File-bound pruning for pushed-down values-only filters.
//!
//! `file_matches_inexact_filters` may only return `false` when manifest bounds
//! prove that no row in the file can satisfy the conjunction of filters. A wrong
//! `false` silently drops rows from query results, so every operator is checked
//! against an independent row-level oracle over the exact bound values.

use super::*;
use datafusion::{
    common::ScalarValue,
    logical_expr::{col, lit},
    prelude::Expr,
};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, NestedField, PrimitiveType, Schema,
    Type,
};
use std::collections::HashMap;

const ID: i32 = 1;
const NAME: i32 = 2;
const DAY: i32 = 3;

fn schema() -> Schema {
    Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                ID,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                NAME,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                DAY,
                "day",
                Type::Primitive(PrimitiveType::Date),
            )),
        ])
        .build()
        .unwrap()
}

fn file(lower: Vec<(i32, Datum)>, upper: Vec<(i32, Datum)>) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_format(DataFileFormat::Parquet)
        .file_path("memory://file.parquet".into())
        .file_size_in_bytes(1)
        .record_count(1)
        .lower_bounds(lower.into_iter().collect::<HashMap<_, _>>())
        .upper_bounds(upper.into_iter().collect::<HashMap<_, _>>())
        .build()
        .unwrap()
}

/// A file whose ids lie in [10, 20], names in ["b", "m"], days in [100, 200].
fn bounded() -> iceberg::spec::DataFile {
    file(
        vec![
            (ID, Datum::long(10)),
            (NAME, Datum::string("b")),
            (DAY, Datum::date(100)),
        ],
        vec![
            (ID, Datum::long(20)),
            (NAME, Datum::string("m")),
            (DAY, Datum::date(200)),
        ],
    )
}

fn keeps(expr: Expr) -> bool {
    file_matches_inexact_filters(&schema(), &bounded(), &[expr])
}

#[test]
fn comparisons_prune_only_disjoint_intervals() {
    // Equality: inside, at both edges, and outside.
    assert!(keeps(col("id").eq(lit(15_i64))));
    assert!(keeps(col("id").eq(lit(10_i64))));
    assert!(keeps(col("id").eq(lit(20_i64))));
    assert!(!keeps(col("id").eq(lit(9_i64))));
    assert!(!keeps(col("id").eq(lit(21_i64))));
    // Literal on the left of equality.
    assert!(!keeps(lit(21_i64).eq(col("id"))));
    // Strict and non-strict ranges at the boundaries.
    assert!(!keeps(col("id").lt(lit(10_i64))));
    assert!(keeps(col("id").lt_eq(lit(10_i64))));
    assert!(!keeps(col("id").gt(lit(20_i64))));
    assert!(keeps(col("id").gt_eq(lit(20_i64))));
    assert!(keeps(col("id").lt(lit(11_i64))));
    assert!(keeps(col("id").gt(lit(19_i64))));
    // Strings compare bytewise, as Parquet UTF8 statistics do.
    assert!(keeps(col("name").eq(lit("c"))));
    assert!(!keeps(col("name").eq(lit("a"))));
    assert!(!keeps(col("name").eq(lit("n"))));
    assert!(!keeps(col("name").eq(lit("B"))));
    // Dates.
    assert!(keeps(col("day").eq(lit(ScalarValue::Date32(Some(150))))));
    assert!(!keeps(col("day").gt(lit(ScalarValue::Date32(Some(200))))));
}

#[test]
fn boolean_connectives_and_in_lists_are_sound() {
    assert!(!keeps(
        col("id").eq(lit(1_i64)).and(col("name").eq(lit("c")))
    ));
    assert!(keeps(col("id").eq(lit(1_i64)).or(col("name").eq(lit("c")))));
    assert!(!keeps(
        col("id").eq(lit(1_i64)).or(col("id").eq(lit(30_i64)))
    ));
    assert!(keeps(
        col("id").in_list(vec![lit(1_i64), lit(12_i64)], false)
    ));
    assert!(!keeps(
        col("id").in_list(vec![lit(1_i64), lit(30_i64)], false)
    ));
    // Negated lists and NOT prove nothing from bounds.
    assert!(keeps(col("id").in_list(vec![lit(15_i64)], true)));
    assert!(keeps(Expr::Not(Box::new(col("id").eq(lit(15_i64))))));
    // Several filters form a conjunction.
    assert!(!file_matches_inexact_filters(
        &schema(),
        &bounded(),
        &[col("id").eq(lit(15_i64)), col("name").eq(lit("z"))],
    ));
}

#[test]
fn unprovable_cases_keep_the_file() {
    // Literal type differs from the column type: no coercion is guessed.
    assert!(keeps(col("id").eq(lit(99_i32))));
    assert!(keeps(col("id").eq(lit("99"))));
    // Literal-on-left ordering comparisons are not rewritten.
    assert!(keeps(lit(99_i64).lt(col("id"))));
    // Unknown column, inequality, IS NULL, and arithmetic.
    assert!(keeps(col("missing").eq(lit(1_i64))));
    assert!(keeps(col("id").not_eq(lit(15_i64))));
    assert!(keeps(col("id").is_null()));
    assert!(keeps((col("id") + lit(1_i64)).eq(lit(99_i64))));
    // 64-byte SHA-256 hex digests and 74-byte `body_key` locators are strictly shorter
    // than `STATISTICS_TRUNCATE_BYTES` (128) and therefore prune disjoint files.
    let sha_file = file(
        vec![(NAME, Datum::string("a".repeat(64)))],
        vec![(NAME, Datum::string("f".repeat(64)))],
    );
    assert!(!file_matches_inexact_filters(
        &schema(),
        &sha_file,
        &[col("name").eq(lit("0".repeat(64)))],
    ));
    let body_key_file = file(
        vec![(NAME, Datum::string(format!("text/plain{}", "a".repeat(64))))],
        vec![(NAME, Datum::string(format!("text/plain{}", "f".repeat(64))))],
    );
    assert!(!file_matches_inexact_filters(
        &schema(),
        &body_key_file,
        &[col("name").eq(lit(format!("text/plain{}", "0".repeat(64))))],
    ));
    // Strings at or beyond the 128-byte statistics truncation length are kept conservatively.
    let long = "z".repeat(crate::storage::STATISTICS_TRUNCATE_BYTES);
    assert!(keeps(col("name").eq(lit(long.clone()))));
    let truncated = file(
        vec![(NAME, Datum::string("a".repeat(crate::storage::STATISTICS_TRUNCATE_BYTES)))],
        vec![(NAME, Datum::string("a".repeat(crate::storage::STATISTICS_TRUNCATE_BYTES)))],
    );
    assert!(file_matches_inexact_filters(
        &schema(),
        &truncated,
        &[col("name").eq(lit("b"))],
    ));
    // Missing bounds (e.g. an all-NULL column or a column added after the write).
    let unbounded = file(vec![], vec![]);
    assert!(file_matches_inexact_filters(
        &schema(),
        &unbounded,
        &[col("id").eq(lit(1_i64))],
    ));
}

/// Exhaustive check against a row oracle: for every bound pair in a small domain
/// and every literal, a pruned file must hold no satisfying value.
#[test]
fn pruning_never_drops_a_satisfying_value() {
    type Op = fn(Expr, Expr) -> Expr;
    type Holds = fn(i64, i64) -> bool;
    let ops: [(Op, Holds); 5] = [
        (Expr::eq, |v, l| v == l),
        (Expr::lt, |v, l| v < l),
        (Expr::lt_eq, |v, l| v <= l),
        (Expr::gt, |v, l| v > l),
        (Expr::gt_eq, |v, l| v >= l),
    ];
    for low in -3_i64..=3 {
        for high in low..=3 {
            let file = file(vec![(ID, Datum::long(low))], vec![(ID, Datum::long(high))]);
            for literal in -5_i64..=5 {
                for (build, holds) in ops {
                    let kept = file_matches_inexact_filters(
                        &schema(),
                        &file,
                        &[build(col("id"), lit(literal))],
                    );
                    let satisfiable = (low..=high).any(|value| holds(value, literal));
                    assert!(
                        kept || !satisfiable,
                        "pruned [{low},{high}] for literal {literal}"
                    );
                    // Bounds are exact here, so pruning should also be complete.
                    assert_eq!(kept, satisfiable, "[{low},{high}] literal {literal}");
                }
            }
        }
    }
}
