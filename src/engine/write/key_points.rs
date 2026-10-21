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

use crate::{Result, budget::Budget, storage};
use arrow::array::{
    Array, Date32Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::spec::{DataFile, Literal, PartitionSpec, PrimitiveLiteral, Schema, Transform};
use std::collections::{BTreeSet, HashSet};
use tokio::sync::OwnedSemaphorePermit;

/// One affected key component ordered exactly like its Iceberg bound literal.
///
/// Strings order bytewise (UTF-8), matching Parquet's unsigned byte ordering for
/// UTF8 statistics; integer and date columns share a widened `i64` order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Point {
    Int(i64),
    Str(String),
}

#[derive(Debug, Default)]
struct Column {
    values: BTreeSet<Point>,
    has_null: bool,
}

#[derive(Debug)]
struct PartitionFilter {
    part_idx: usize,
    key_idx: usize,
    transform: Transform,
    values: HashSet<Option<PrimitiveLiteral>>,
}

/// Distinct affected values per key column, retained under one reservation.
#[derive(Debug)]
pub(super) struct KeyPoints {
    /// `None` marks a key column whose type has no exact point representation.
    columns: Vec<Option<Column>>,
    partitions: Vec<Option<PartitionFilter>>,
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
        native_schema: &Schema,
        partition_spec: &PartitionSpec,
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
        let mut partitions: Vec<Option<PartitionFilter>> = partition_spec
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(part_idx, pf)| {
                let key_idx = keys.iter().position(|key| {
                    native_schema
                        .field_by_name(key)
                        .is_some_and(|field| field.id == pf.source_id)
                })?;
                Some(Some(PartitionFilter {
                    part_idx,
                    key_idx,
                    transform: pf.transform,
                    values: HashSet::new(),
                }))
            })
            .collect();
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
            for slot in &mut partitions {
                let Some(filter) = slot.as_mut() else {
                    continue;
                };
                let array = batch.column(filter.key_idx);
                let Ok(computed) = storage::compute_partition_column(filter.transform, array) else {
                    *slot = None;
                    continue;
                };
                for item in computed {
                    let prim = match item {
                        None => None,
                        Some(Literal::Primitive(p)) => Some(p),
                        _ => {
                            *slot = None;
                            break;
                        }
                    };
                    let size = match &prim {
                        Some(PrimitiveLiteral::String(s)) => s.len(),
                        Some(PrimitiveLiteral::Binary(b)) => b.len(),
                        _ => 0,
                    };
                    if filter.values.insert(prim) {
                        bytes = bytes.saturating_add(size + ENTRY_OVERHEAD);
                        if bytes > allowance {
                            return Ok(None);
                        }
                    }
                }
            }
            if columns.iter().all(Option::is_none) && partitions.iter().all(Option::is_none) {
                return Ok(None);
            }
        }
        Ok(Some(Self {
            columns,
            partitions,
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
        let part_fields = file.partition().fields();
        for filter in self.partitions.iter().flatten() {
            let Some(slot) = part_fields.get(filter.part_idx) else {
                continue;
            };
            let file_lit = match slot {
                None => None,
                Some(Literal::Primitive(p)) => Some(p.clone()),
                _ => continue,
            };
            if !filter.values.contains(&file_lit) {
                return false;
            }
        }
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
                    Point::Int(_) => 0,
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
    } else if let Some(array) = any.downcast_ref::<StringViewArray>() {
        each!(array, |value: &str| Point::Str(value.to_owned()));
    } else if let Some(array) = any.downcast_ref::<Int64Array>() {
        each!(array, Point::Int);
    } else if let Some(array) = any.downcast_ref::<Int32Array>() {
        each!(array, |value: i32| Point::Int(i64::from(value)));
    } else if let Some(array) = any.downcast_ref::<Date32Array>() {
        each!(array, |value: i32| Point::Int(i64::from(value)));
    } else {
        return None;
    }
    Some(added)
}

/// Converts an exact (untruncated) bound to a point; see `untruncated_bound`.
fn point(datum: &iceberg::spec::Datum) -> Option<Point> {
    match super::untruncated_bound(datum)? {
        PrimitiveLiteral::String(value) => Some(Point::Str(value)),
        PrimitiveLiteral::Int(value) => Some(Point::Int(i64::from(value))),
        PrimitiveLiteral::Long(value) => Some(Point::Int(value)),
        _ => None,
    }
}
