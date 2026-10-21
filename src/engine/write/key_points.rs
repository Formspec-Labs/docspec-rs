//! Exact affected-key points for manifest-bound file pruning during key Updates.
//!
//! The interval test in `file_may_overlap_key_files` compares one retained file's
//! bounds with the *overall* min/max of the affected keys. Two affected keys at the
//! extremes of the key space therefore overlap every file, so an Update that
//! corrects the oldest and newest rows decodes the whole population. Keeping the
//! distinct affected values (bounded by an explicit memory reservation) lets each
//! file be tested for an actual affected point inside its bounds instead.
//!
//! The test is a necessary condition per key column: a composite key tuple can only
//! occur in a file when every one of its components lies inside that file's bounds.
//! Columns whose values cannot be represented exactly are skipped, never guessed.
//! When the distinct values exceed the reservation the caller falls back to the
//! interval test; pruning stays conservative and correctness never depends on it.

use crate::{Result, budget::Budget};
use arrow::array::{
    Array, BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, UInt64Array,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::spec::{DataFile, PrimitiveLiteral, Schema};
use std::collections::BTreeSet;
use tokio::sync::OwnedSemaphorePermit;

/// One affected key component ordered exactly like its Iceberg bound literal.
///
/// Strings and binaries order bytewise, matching Parquet's unsigned byte ordering
/// for BYTE_ARRAY statistics; integer, decimal, and date columns order numerically.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Point {
    Int(i64),
    Int128(i128),
    Str(String),
    Bin(Vec<u8>),
}

#[derive(Debug, Default)]
struct Column {
    values: BTreeSet<Point>,
    has_null: bool,
}

/// Distinct affected values per key column, retained under one reservation.
#[derive(Debug)]
pub(super) struct KeyPoints {
    /// `None` marks a key column whose type has no exact point representation.
    columns: Vec<Option<Column>>,
    _reservation: OwnedSemaphorePermit,
}

/// Fixed per-entry admission for B-tree node and enum overhead.
const ENTRY_OVERHEAD: usize = 48;

impl KeyPoints {
    /// Streams the registered `affected_keys` table once and keeps its distinct
    /// key values. Returns `Ok(None)` when the reservation is unavailable or the
    /// distinct values outgrow it; the caller then keeps the interval test.
    pub(super) async fn collect(
        context: &SessionContext,
        keys: &[String],
        budget: &Budget,
    ) -> Result<Option<Self>> {
        // One batch allowance bounds the retained set. This is a pruning aid, not
        // a correctness requirement, so contention or overflow declines quietly.
        let allowance = budget.limits.max_batch_bytes;
        let Ok(reservation) = budget.reserve(allowance) else {
            return Ok(None);
        };
        let mut columns: Vec<Option<Column>> =
            keys.iter().map(|_| Some(Column::default())).collect();
        let mut bytes = 0usize;
        let mut stream = context
            .table("affected_keys")
            .await?
            .select_columns(&keys.iter().map(String::as_str).collect::<Vec<_>>())?
            .execute_stream()
            .await?;
        while let Some(batch) = stream.try_next().await? {
            for (index, slot) in columns.iter_mut().enumerate() {
                let Some(column) = slot.as_mut() else {
                    continue;
                };
                let array = batch.column(index);
                let Some(added) = insert(column, array.as_ref()) else {
                    *slot = None;
                    continue;
                };
                bytes = bytes.saturating_add(added);
                if bytes > allowance {
                    return Ok(None);
                }
            }
            if columns.iter().all(Option::is_none) {
                return Ok(None);
            }
        }
        Ok(Some(Self {
            columns,
            _reservation: reservation,
        }))
    }

    /// Returns `false` only when manifest bounds prove that no affected tuple can
    /// occur in `file`. Unknown types, truncated or absent bounds, and possible
    /// NULL-to-NULL matches keep the file.
    pub(super) fn may_contain(
        &self,
        native_schema: &Schema,
        keys: &[String],
        file: &DataFile,
    ) -> bool {
        for (key, column) in keys.iter().zip(&self.columns) {
            let Some(column) = column else {
                continue;
            };
            let Some(field) = native_schema.field_by_name(key) else {
                continue;
            };
            // `key_join` uses `IS NOT DISTINCT FROM`: an affected NULL can match a
            // NULL row, and bounds say nothing about NULLs.
            let file_may_have_null = file
                .null_value_counts()
                .get(&field.id)
                .copied()
                .unwrap_or(1)
                > 0;
            if column.has_null && file_may_have_null {
                continue;
            }
            let (Some(low), Some(high)) = (
                file.lower_bounds().get(&field.id).and_then(point),
                file.upper_bounds().get(&field.id).and_then(point),
            ) else {
                continue;
            };
            if low > high {
                continue;
            }
            if column.values.range(low..=high).next().is_none() {
                return false;
            }
        }
        true
    }
}

/// Adds one array's values; `None` means the type has no exact point form.
fn insert(column: &mut Column, array: &dyn Array) -> Option<usize> {
    let mut added = 0usize;
    macro_rules! each {
        ($array:expr, $convert:expr) => {{
            for row in 0..$array.len() {
                if $array.is_null(row) {
                    column.has_null = true;
                    continue;
                }
                let point = $convert($array.value(row));
                let size = match &point {
                    Point::Str(value) => value.len(),
                    Point::Bin(value) => value.len(),
                    Point::Int(_) | Point::Int128(_) => 0,
                };
                if column.values.insert(point) {
                    added = added.saturating_add(size + ENTRY_OVERHEAD);
                }
            }
        }};
    }
    let any = array.as_any();
    if let Some(array) = any.downcast_ref::<StringArray>() {
        each!(array, |value: &str| Point::Str(value.to_owned()));
    } else if let Some(array) = any.downcast_ref::<LargeStringArray>() {
        each!(array, |value: &str| Point::Str(value.to_owned()));
    } else if let Some(array) = any.downcast_ref::<BinaryArray>() {
        each!(array, |value: &[u8]| Point::Bin(value.to_vec()));
    } else if let Some(array) = any.downcast_ref::<LargeBinaryArray>() {
        each!(array, |value: &[u8]| Point::Bin(value.to_vec()));
    } else if let Some(array) = any.downcast_ref::<FixedSizeBinaryArray>() {
        each!(array, |value: &[u8]| Point::Bin(value.to_vec()));
    } else if let Some(array) = any.downcast_ref::<Int64Array>() {
        each!(array, Point::Int);
    } else if let Some(array) = any.downcast_ref::<Int32Array>() {
        each!(array, |value: i32| Point::Int(i64::from(value)));
    } else if let Some(array) = any.downcast_ref::<Date32Array>() {
        each!(array, |value: i32| Point::Int(i64::from(value)));
    } else if let Some(array) = any.downcast_ref::<UInt64Array>() {
        each!(array, |value: u64| Point::Int128(i128::from(value)));
    } else if let Some(array) = any.downcast_ref::<Decimal128Array>() {
        each!(array, Point::Int128);
    } else {
        return None;
    }
    Some(added)
}

/// Converts an exact (untruncated) bound to a point; see `untruncated_bound`.
fn point(datum: &iceberg::spec::Datum) -> Option<Point> {
    match super::untruncated_bound(datum)? {
        PrimitiveLiteral::String(value) => Some(Point::Str(value)),
        PrimitiveLiteral::Binary(value) => Some(Point::Bin(value)),
        PrimitiveLiteral::Int(value) => Some(Point::Int(i64::from(value))),
        PrimitiveLiteral::Long(value) => Some(Point::Int(value)),
        PrimitiveLiteral::Int128(value) => Some(Point::Int128(value)),
        _ => None,
    }
}
