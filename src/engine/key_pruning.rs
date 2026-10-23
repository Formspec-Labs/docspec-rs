//! Unified manifest-bound, partition-spec, and exact-point key pruning.
//!
//! Consolidates key-point extraction (`KeyPoints`), point-lookup key target parsing
//! and batch filtering (`KeyTargets`), untruncated Iceberg bound decoding, and
//! interval/point overlap pruning across write, incremental join, aggregation, and
//! point-lookup paths.

use crate::{Error, Result, budget::Budget, storage};
use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::spec::{
    DataFile, Datum, Literal, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Transform,
    Type,
};
use std::collections::{BTreeSet, HashSet};
use tokio::sync::OwnedSemaphorePermit;

/// Physical comparison domain of a supported key array.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum KeyArrayKind {
    Utf8,
    Binary,
    Int,
    Int128,
    Date32,
    TimestampMicros,
}

/// Borrowed key value yielded by [`visit_key_array`].
#[derive(Clone, Copy, Debug)]
pub(in crate::engine) enum KeyValueRef<'a> {
    Str(&'a str),
    Bin(&'a [u8]),
    Int(i64),
    Int128(i128),
    Date32(i32),
    TimestampMicros(i64),
}

/// Visits every row of one supported 12-type key array without allocating per row
/// unless requested by `on_row`. Returns `None` if `array` is not one of the 12
/// supported physical key array types.
pub(in crate::engine) fn visit_key_array(
    array: &dyn Array,
    mut on_row: impl FnMut(Option<KeyValueRef<'_>>),
) -> Option<KeyArrayKind> {
    macro_rules! scan {
        ($arr:expr, $kind:expr, $wrap:expr) => {{
            for row in 0..$arr.len() {
                if $arr.is_null(row) {
                    on_row(None);
                } else {
                    on_row(Some($wrap($arr.value(row))));
                }
            }
            Some($kind)
        }};
    }
    let any = array.as_any();
    if let Some(arr) = any.downcast_ref::<StringArray>() {
        scan!(arr, KeyArrayKind::Utf8, KeyValueRef::Str)
    } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
        scan!(arr, KeyArrayKind::Utf8, KeyValueRef::Str)
    } else if let Some(arr) = any.downcast_ref::<StringViewArray>() {
        scan!(arr, KeyArrayKind::Utf8, KeyValueRef::Str)
    } else if let Some(arr) = any.downcast_ref::<BinaryArray>() {
        scan!(arr, KeyArrayKind::Binary, KeyValueRef::Bin)
    } else if let Some(arr) = any.downcast_ref::<LargeBinaryArray>() {
        scan!(arr, KeyArrayKind::Binary, KeyValueRef::Bin)
    } else if let Some(arr) = any.downcast_ref::<FixedSizeBinaryArray>() {
        scan!(arr, KeyArrayKind::Binary, KeyValueRef::Bin)
    } else if let Some(arr) = any.downcast_ref::<Int64Array>() {
        scan!(arr, KeyArrayKind::Int, KeyValueRef::Int)
    } else if let Some(arr) = any.downcast_ref::<Int32Array>() {
        scan!(arr, KeyArrayKind::Int, |v: i32| KeyValueRef::Int(
            i64::from(v)
        ))
    } else if let Some(arr) = any.downcast_ref::<Date32Array>() {
        scan!(arr, KeyArrayKind::Date32, KeyValueRef::Date32)
    } else if let Some(arr) = any.downcast_ref::<UInt64Array>() {
        scan!(arr, KeyArrayKind::Int128, |v: u64| KeyValueRef::Int128(
            i128::from(v)
        ))
    } else if let Some(arr) = any.downcast_ref::<Decimal128Array>() {
        scan!(arr, KeyArrayKind::Int128, KeyValueRef::Int128)
    } else if let Some(arr) = any.downcast_ref::<TimestampMicrosecondArray>() {
        scan!(
            arr,
            KeyArrayKind::TimestampMicros,
            KeyValueRef::TimestampMicros
        )
    } else {
        None
    }
}

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

#[derive(Debug)]
struct PartitionFilter {
    part_idx: usize,
    key_idx: usize,
    transform: Transform,
    source_type: Type,
    values: HashSet<Option<PrimitiveLiteral>>,
}

/// Distinct affected values per key column, retained under one reservation.
#[derive(Debug)]
pub(in crate::engine) struct KeyPoints {
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
    pub(in crate::engine) async fn collect(
        context: &SessionContext,
        native_schema: &Schema,
        partition_spec: &PartitionSpec,
        keys: &[String],
        budget: &Budget,
    ) -> Result<Option<Self>> {
        Self::collect_with_partitions(
            context,
            "affected_keys",
            Some((native_schema, partition_spec)),
            keys,
            budget,
        )
        .await
    }

    /// Streams any registered key table once and keeps its distinct key values.
    pub(in crate::engine) async fn collect_table(
        context: &SessionContext,
        table_name: &str,
        keys: &[String],
        budget: &Budget,
    ) -> Result<Option<Self>> {
        Self::collect_with_partitions(context, table_name, None, keys, budget).await
    }

    async fn collect_with_partitions(
        context: &SessionContext,
        table_name: &str,
        partition_target: Option<(&Schema, &PartitionSpec)>,
        keys: &[String],
        budget: &Budget,
    ) -> Result<Option<Self>> {
        if keys.is_empty() {
            return Ok(None);
        }
        // One batch allowance bounds the retained set. This is a pruning aid, not
        // a correctness requirement, so contention or overflow declines quietly.
        let allowance = budget.limits.max_batch_bytes;
        let Ok(reservation) = budget.reserve(allowance) else {
            return Ok(None);
        };
        let mut columns: Vec<Option<Column>> =
            keys.iter().map(|_| Some(Column::default())).collect();
        let mut partitions: Vec<Option<PartitionFilter>> =
            if let Some((native_schema, partition_spec)) = partition_target {
                partition_spec
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(part_idx, pf)| {
                        let source_field = native_schema.field_by_id(pf.source_id)?;
                        let key_idx = keys.iter().position(|key| key == &source_field.name)?;
                        Some(Some(PartitionFilter {
                            part_idx,
                            key_idx,
                            transform: pf.transform,
                            source_type: (*source_field.field_type).clone(),
                            values: HashSet::new(),
                        }))
                    })
                    .collect()
            } else {
                Vec::new()
            };
        let mut bytes = 0usize;
        let mut stream = context
            .table(table_name)
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
                let Some(computed) = compute_array_partition_literals(
                    filter.transform,
                    &filter.source_type,
                    array.as_ref(),
                ) else {
                    *slot = None;
                    continue;
                };
                for prim in computed {
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
    pub(in crate::engine) fn may_contain(
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
            if column.values.is_empty() && !column.has_null {
                return false;
            }
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

/// Adds one array's values using the shared 12-type visitor; `None` means the
/// type has no exact point form.
fn insert(column: &mut Column, array: &dyn Array) -> Option<usize> {
    let mut added = 0usize;
    visit_key_array(array, |row| {
        let Some(value) = row else {
            column.has_null = true;
            return;
        };
        let (point, size) = match value {
            KeyValueRef::Str(v) => (Point::Str(v.to_owned()), v.len()),
            KeyValueRef::Bin(v) => (Point::Bin(v.to_vec()), v.len()),
            KeyValueRef::Int(v) | KeyValueRef::TimestampMicros(v) => (Point::Int(v), 0),
            KeyValueRef::Date32(v) => (Point::Int(i64::from(v)), 0),
            KeyValueRef::Int128(v) => (Point::Int128(v), 0),
        };
        if column.values.insert(point) {
            added = added.saturating_add(size + ENTRY_OVERHEAD);
        }
    })?;
    Some(added)
}

fn compute_array_partition_literals(
    transform: Transform,
    source_type: &Type,
    array: &dyn Array,
) -> Option<Vec<Option<PrimitiveLiteral>>> {
    let mut out = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            out.push(None);
            continue;
        }
        let lit = array_row_primitive_literal(array, row)?;
        let transformed = storage::compute_partition_literal(transform, source_type, &lit)?;
        out.push(Some(transformed));
    }
    Some(out)
}

fn array_row_primitive_literal(array: &dyn Array, row: usize) -> Option<PrimitiveLiteral> {
    let any = array.as_any();
    if let Some(arr) = any.downcast_ref::<BooleanArray>() {
        Some(PrimitiveLiteral::Boolean(arr.value(row)))
    } else if let Some(arr) = any.downcast_ref::<Int32Array>() {
        Some(PrimitiveLiteral::Int(arr.value(row)))
    } else if let Some(arr) = any.downcast_ref::<Int64Array>() {
        Some(PrimitiveLiteral::Long(arr.value(row)))
    } else if let Some(arr) = any.downcast_ref::<Date32Array>() {
        Some(PrimitiveLiteral::Int(arr.value(row)))
    } else if let Some(arr) = any.downcast_ref::<TimestampMicrosecondArray>() {
        Some(PrimitiveLiteral::Long(arr.value(row)))
    } else if let Some(arr) = any.downcast_ref::<StringArray>() {
        Some(PrimitiveLiteral::String(arr.value(row).to_owned()))
    } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
        Some(PrimitiveLiteral::String(arr.value(row).to_owned()))
    } else if let Some(arr) = any.downcast_ref::<StringViewArray>() {
        Some(PrimitiveLiteral::String(arr.value(row).to_owned()))
    } else if let Some(arr) = any.downcast_ref::<BinaryArray>() {
        Some(PrimitiveLiteral::Binary(arr.value(row).to_vec()))
    } else if let Some(arr) = any.downcast_ref::<LargeBinaryArray>() {
        Some(PrimitiveLiteral::Binary(arr.value(row).to_vec()))
    } else {
        any.downcast_ref::<BinaryViewArray>()
            .map(|arr| PrimitiveLiteral::Binary(arr.value(row).to_vec()))
    }
}

/// Converts an exact (untruncated) bound to a point; see [`untruncated_bound`].
fn point(datum: &Datum) -> Option<Point> {
    match untruncated_bound(datum)? {
        PrimitiveLiteral::String(value) => Some(Point::Str(value)),
        PrimitiveLiteral::Binary(value) => Some(Point::Bin(value)),
        PrimitiveLiteral::Int(value) => Some(Point::Int(i64::from(value))),
        PrimitiveLiteral::Long(value) => Some(Point::Int(value)),
        PrimitiveLiteral::Int128(value) => Some(Point::Int128(value)),
        _ => None,
    }
}

/// Requested key values in the key column's comparison domain for `read_keys`.
#[derive(Debug)]
pub(in crate::engine) enum KeyTargets {
    Strings(BTreeSet<String>),
    Ints(BTreeSet<i64>),
    Int128s(BTreeSet<i128>),
    Binaries(BTreeSet<Vec<u8>>),
    Dates(BTreeSet<i32>),
    Timestamps(BTreeSet<i64>),
}

fn parse_binary_key_candidates(value: &str) -> Vec<Vec<u8>> {
    let mut out = vec![value.as_bytes().to_vec()];
    let hex_str = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .or_else(|| value.strip_prefix("hex:"))
        .or_else(|| value.strip_prefix("HEX:"))
        .unwrap_or(value);
    if !hex_str.is_empty()
        && hex_str.len().is_multiple_of(2)
        && let Some(decoded) = (0..hex_str.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).ok())
            .collect::<Option<Vec<u8>>>()
        && !out.contains(&decoded)
    {
        out.push(decoded);
    }
    out
}

impl KeyTargets {
    pub(in crate::engine) fn parse(data_type: &DataType, values: &[String]) -> Self {
        match data_type {
            DataType::Int64 => Self::Ints(values.iter().filter_map(|v| v.parse().ok()).collect()),
            DataType::Int32 => Self::Ints(
                values
                    .iter()
                    .filter_map(|v| v.parse::<i32>().ok())
                    .map(i64::from)
                    .collect(),
            ),
            DataType::UInt64 | DataType::Decimal128(_, 0) => {
                Self::Int128s(values.iter().filter_map(|v| v.parse().ok()).collect())
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Self::Binaries(
                    values
                        .iter()
                        .flat_map(|v| parse_binary_key_candidates(v))
                        .collect(),
                )
            }
            DataType::Date32 => Self::Dates(
                values
                    .iter()
                    .filter_map(|v| {
                        if let Ok(days) = v.parse::<i32>() {
                            return Some(days);
                        }
                        let src = StringArray::from(vec![v.as_str()]);
                        let casted = arrow::compute::cast(&src, &DataType::Date32).ok()?;
                        let arr = casted.as_any().downcast_ref::<Date32Array>()?;
                        (!arr.is_null(0)).then(|| arr.value(0))
                    })
                    .collect(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::Timestamps(
                values
                    .iter()
                    .filter_map(|v| {
                        if let Ok(micros) = v.parse::<i64>() {
                            return Some(micros);
                        }
                        let src = StringArray::from(vec![v.as_str()]);
                        let casted = arrow::compute::cast(
                            &src,
                            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                        )
                        .or_else(|_| {
                            arrow::compute::cast(
                                &src,
                                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                            )
                        })
                        .ok()?;
                        let arr = casted
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()?;
                        (!arr.is_null(0)).then(|| arr.value(0))
                    })
                    .collect(),
            ),
            _ => Self::Strings(values.iter().cloned().collect()),
        }
    }

    pub(in crate::engine) fn is_empty(&self) -> bool {
        match self {
            Self::Strings(set) => set.is_empty(),
            Self::Ints(set) => set.is_empty(),
            Self::Int128s(set) => set.is_empty(),
            Self::Binaries(set) => set.is_empty(),
            Self::Dates(set) => set.is_empty(),
            Self::Timestamps(set) => set.is_empty(),
        }
    }

    pub(in crate::engine) fn partition_filters(
        &self,
        partition_spec: &PartitionSpec,
        field_id: i32,
        field_type: &Type,
    ) -> Vec<(usize, HashSet<PrimitiveLiteral>)> {
        let Type::Primitive(prim) = field_type else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (part_idx, pf) in partition_spec.fields().iter().enumerate() {
            if pf.source_id != field_id {
                continue;
            }
            let mut set = HashSet::new();
            let mut complete = true;
            match (self, prim) {
                (Self::Strings(values), PrimitiveType::String) => {
                    for value in values {
                        let lit = PrimitiveLiteral::String(value.clone());
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Ints(values), PrimitiveType::Int) => {
                    for value in values {
                        let Ok(v) = i32::try_from(*value) else {
                            continue;
                        };
                        let lit = PrimitiveLiteral::Int(v);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Ints(values), PrimitiveType::Long) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Long(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Dates(values), PrimitiveType::Date) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Int(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (
                    Self::Timestamps(values),
                    PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                ) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Long(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                _ => {
                    complete = false;
                }
            }
            if complete {
                out.push((part_idx, set));
            }
        }
        out
    }

    /// Returns `false` only when partition metadata or exact (untruncated) manifest
    /// bounds prove that no requested value lies inside the file's key range.
    pub(in crate::engine) fn file_may_contain(
        &self,
        file: &DataFile,
        key_field_id: Option<i32>,
        key_partitions: &[(usize, HashSet<PrimitiveLiteral>)],
    ) -> bool {
        for (part_idx, allowed) in key_partitions {
            let Some(slot) = file.partition().fields().get(*part_idx) else {
                continue;
            };
            match slot {
                None => return false,
                Some(Literal::Primitive(p)) => {
                    if !allowed.contains(p) {
                        return false;
                    }
                }
                _ => {}
            }
        }
        let Some(field_id) = key_field_id else {
            return true;
        };
        let (Some(low), Some(high)) = (
            file.lower_bounds()
                .get(&field_id)
                .and_then(untruncated_bound),
            file.upper_bounds()
                .get(&field_id)
                .and_then(untruncated_bound),
        ) else {
            return true;
        };
        match (self, low, high) {
            (Self::Strings(set), PrimitiveLiteral::String(low), PrimitiveLiteral::String(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (Self::Ints(set), low, high) => {
                let as_i64 = |lit| match lit {
                    PrimitiveLiteral::Int(v) => Some(i64::from(v)),
                    PrimitiveLiteral::Long(v) => Some(v),
                    _ => None,
                };
                match (as_i64(low), as_i64(high)) {
                    (Some(low), Some(high)) => low > high || set.range(low..=high).next().is_some(),
                    _ => true,
                }
            }
            (Self::Int128s(set), PrimitiveLiteral::Int128(low), PrimitiveLiteral::Int128(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (
                Self::Binaries(set),
                PrimitiveLiteral::Binary(low),
                PrimitiveLiteral::Binary(high),
            ) => low > high || set.range(low..=high).next().is_some(),
            (Self::Dates(set), PrimitiveLiteral::Int(low), PrimitiveLiteral::Int(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (Self::Timestamps(set), PrimitiveLiteral::Long(low), PrimitiveLiteral::Long(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            _ => true,
        }
    }

    fn expected_kind(&self) -> KeyArrayKind {
        match self {
            Self::Strings(_) => KeyArrayKind::Utf8,
            Self::Ints(_) => KeyArrayKind::Int,
            Self::Int128s(_) => KeyArrayKind::Int128,
            Self::Binaries(_) => KeyArrayKind::Binary,
            Self::Dates(_) => KeyArrayKind::Date32,
            Self::Timestamps(_) => KeyArrayKind::TimestampMicros,
        }
    }

    pub(in crate::engine) fn filter_batch(
        &self,
        batch: &RecordBatch,
        key_idx: usize,
        out_indices: &[usize],
    ) -> Result<RecordBatch> {
        let col = batch.column(key_idx);
        let mut flags = Vec::with_capacity(col.len());
        let kind = visit_key_array(col.as_ref(), |row| {
            let matched = match (self, row) {
                (Self::Strings(set), Some(KeyValueRef::Str(v))) => set.contains(v),
                (Self::Ints(set), Some(KeyValueRef::Int(v))) => set.contains(&v),
                (Self::Int128s(set), Some(KeyValueRef::Int128(v))) => set.contains(&v),
                (Self::Binaries(set), Some(KeyValueRef::Bin(v))) => set.contains(v),
                (Self::Dates(set), Some(KeyValueRef::Date32(v))) => set.contains(&v),
                (Self::Timestamps(set), Some(KeyValueRef::TimestampMicros(v))) => set.contains(&v),
                _ => false,
            };
            flags.push(Some(matched));
        });
        if kind != Some(self.expected_kind()) {
            return Err(Error::refusal(
                "invalid_key_read",
                "unsupported physical array type for key column",
            ));
        }
        let mask: BooleanArray = flags.into_iter().collect();
        let projected = batch.project(out_indices)?;
        Ok(arrow::compute::filter_record_batch(&projected, &mask)?)
    }
}

pub(in crate::engine) fn untruncated_bound(datum: &Datum) -> Option<PrimitiveLiteral> {
    let lit = PrimitiveLiteral::from(datum.clone());
    match &lit {
        // Chunk statistics truncate variable-width bounds at `STATISTICS_TRUNCATE_BYTES`.
        // Only treat bounds strictly shorter than that limit as exact interval endpoints.
        PrimitiveLiteral::String(s) if s.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        PrimitiveLiteral::Binary(b) if b.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => None,
        _ if lit.is_nan() => None,
        _ => Some(lit),
    }
}

pub(in crate::engine) fn file_may_overlap_mapped_key_files(
    native_schema: &Schema,
    partition_spec: &PartitionSpec,
    scope_schema: &Schema,
    native_keys: &[String],
    scope_keys: &[String],
    key_files: &[DataFile],
    file: &DataFile,
) -> bool {
    if key_files.is_empty() {
        return false;
    }
    for (native_key, scope_key) in native_keys.iter().zip(scope_keys) {
        let (Some(native_field), Some(scope_field)) = (
            native_schema.field_by_name(native_key),
            scope_schema.field_by_name(scope_key),
        ) else {
            continue;
        };
        let (native_id, scope_id) = (native_field.id, scope_field.id);
        let file_may_have_null = file
            .null_value_counts()
            .get(&native_id)
            .copied()
            .unwrap_or(1)
            > 0;
        let keys_may_have_null = key_files
            .iter()
            .any(|kf| kf.null_value_counts().get(&scope_id).copied().unwrap_or(1) > 0);
        // Check order-preserving partition tuple bounds first so files whose column
        // bounds are absent or truncated still prune in O(1) from partition metadata.
        for (part_idx, pf) in partition_spec.fields().iter().enumerate() {
            if pf.source_id != native_id || !storage::is_order_preserving_transform(pf.transform) {
                continue;
            }
            let Some(slot) = file.partition().fields().get(part_idx) else {
                continue;
            };
            match slot {
                None => {
                    if !keys_may_have_null {
                        return false;
                    }
                }
                Some(Literal::Primitive(file_part)) => {
                    let all_disjoint = key_files.iter().all(|kf| {
                        let (Some(kf_low), Some(kf_high)) = (
                            kf.lower_bounds().get(&scope_id).and_then(untruncated_bound),
                            kf.upper_bounds().get(&scope_id).and_then(untruncated_bound),
                        ) else {
                            return false;
                        };
                        let (Some(part_low), Some(part_high)) = (
                            storage::compute_partition_literal(
                                pf.transform,
                                &native_field.field_type,
                                &kf_low,
                            ),
                            storage::compute_partition_literal(
                                pf.transform,
                                &native_field.field_type,
                                &kf_high,
                            ),
                        ) else {
                            return false;
                        };
                        part_low <= part_high && (&part_high < file_part || &part_low > file_part)
                    });
                    if all_disjoint {
                        return false;
                    }
                }
                _ => {}
            }
        }
        // `key_join` uses `IsNotDistinctFrom`, so a possible NULL on both sides can match.
        if file_may_have_null && keys_may_have_null {
            continue;
        }
        let (Some(file_low), Some(file_high)) = (
            file.lower_bounds()
                .get(&native_id)
                .and_then(untruncated_bound),
            file.upper_bounds()
                .get(&native_id)
                .and_then(untruncated_bound),
        ) else {
            continue;
        };
        if file_low > file_high {
            continue;
        }
        let all_disjoint = key_files.iter().all(|kf| {
            let (Some(kf_low), Some(kf_high)) = (
                kf.lower_bounds().get(&scope_id).and_then(untruncated_bound),
                kf.upper_bounds().get(&scope_id).and_then(untruncated_bound),
            ) else {
                return false;
            };
            kf_low <= kf_high && (kf_high < file_low || kf_low > file_high)
        });
        if all_disjoint {
            return false;
        }
    }
    true
}

/// Combined interval + exact-point check for a single data file against prepared key files.
#[expect(
    clippy::too_many_arguments,
    reason = "Evaluates manifest bounds, partition spec, and exact point filters across native and scope schemas"
)]
pub(in crate::engine) fn file_matches_pruned_keys(
    native_schema: &Schema,
    partition_spec: &PartitionSpec,
    scope_schema: &Schema,
    native_keys: &[String],
    scope_keys: &[String],
    key_files: &[DataFile],
    points: Option<&KeyPoints>,
    file: &DataFile,
) -> bool {
    file_may_overlap_mapped_key_files(
        native_schema,
        partition_spec,
        scope_schema,
        native_keys,
        scope_keys,
        key_files,
        file,
    ) && points.is_none_or(|p| p.may_contain(native_schema, native_keys, file))
}

/// Filters snapshot data files down to those whose bounds/partitions may overlap
/// `key_files` and `points`, returning `(retained_file_paths, retained_row_count)`.
#[expect(
    clippy::too_many_arguments,
    reason = "Filters snapshot data files against native/scope schemas, partition spec, key files, and point sets"
)]
pub(in crate::engine) fn prune_snapshot_files<'a>(
    files: impl IntoIterator<Item = &'a DataFile>,
    native_schema: &Schema,
    partition_spec: &PartitionSpec,
    scope_schema: &Schema,
    native_keys: &[String],
    scope_keys: &[String],
    key_files: &[DataFile],
    points: Option<&KeyPoints>,
) -> (BTreeSet<String>, u64) {
    let mut retained = BTreeSet::new();
    let mut rows = 0u64;
    for file in files {
        if file_matches_pruned_keys(
            native_schema,
            partition_spec,
            scope_schema,
            native_keys,
            scope_keys,
            key_files,
            points,
            file,
        ) {
            retained.insert(file.file_path().to_owned());
            rows = rows.saturating_add(file.record_count());
        }
    }
    (retained, rows)
}
