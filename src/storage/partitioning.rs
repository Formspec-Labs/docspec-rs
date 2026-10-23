//! Partition key computation, calendar transforms, and partition tuple validation.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFile;
use std::{collections::HashMap, sync::Arc};

pub(crate) fn partition_batch(
    schema: &iceberg::spec::SchemaRef,
    spec: &iceberg::spec::PartitionSpecRef,
    batch: RecordBatch,
) -> Result<Vec<(Option<iceberg::spec::PartitionKey>, RecordBatch)>> {
    if spec.fields().is_empty() {
        return Ok(vec![(None, batch)]);
    }
    let num_rows = batch.num_rows();
    let mut field_values = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema
            .as_struct()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                Error::refusal(
                    "unsupported_write_layout",
                    "partition spec references a missing top-level field",
                )
            })?;
        let col_idx = batch.schema().index_of(&source.name).map_err(|_| {
            Error::refusal(
                "schema_mismatch",
                "writer batch omits a required partition source column",
            )
        })?;
        field_values.push(compute_partition_column(
            field.transform,
            batch.column(col_idx),
        )?);
    }
    let mut groups: Vec<(iceberg::spec::Struct, Vec<u32>)> = Vec::new();
    let mut group_index: HashMap<iceberg::spec::Struct, usize> = HashMap::new();
    for row_idx in 0..num_rows {
        let partition =
            iceberg::spec::Struct::from_iter(field_values.iter().map(|col| col[row_idx].clone()));
        let idx = u32::try_from(row_idx)
            .map_err(|_| Error::refusal("oversized_batch", "batch row count exceeds u32 range"))?;
        if let Some(&pos) = group_index.get(&partition) {
            groups[pos].1.push(idx);
        } else {
            let pos = groups.len();
            group_index.insert(partition.clone(), pos);
            groups.push((partition, vec![idx]));
        }
    }
    if groups.len() == 1 {
        let (partition, _) = groups.pop().expect("single partition group");
        let key =
            iceberg::spec::PartitionKey::new(spec.as_ref().clone(), schema.clone(), partition);
        return Ok(vec![(Some(key), batch)]);
    }
    let mut out = Vec::with_capacity(groups.len());
    for (partition, indices) in groups {
        let indices = arrow::array::UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|col| Ok(arrow::compute::take(col.as_ref(), &indices, None)?))
            .collect::<Result<Vec<_>>>()?;
        let sub_batch = RecordBatch::try_new(batch.schema(), columns)?;
        let key =
            iceberg::spec::PartitionKey::new(spec.as_ref().clone(), schema.clone(), partition);
        out.push((Some(key), sub_batch));
    }
    Ok(out)
}

pub(crate) fn compute_partition_column(
    transform: iceberg::spec::Transform,
    column: &arrow::array::ArrayRef,
) -> Result<Vec<Option<iceberg::spec::Literal>>> {
    use arrow::{
        array::{
            Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Int32Array, Int64Array,
            LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
            TimestampMicrosecondArray,
        },
        datatypes::{DataType, TimeUnit},
    };
    use iceberg::spec::{Literal, Transform};

    let rows = column.len();
    let mut out = Vec::with_capacity(rows);
    match (transform, column.data_type()) {
        (Transform::Year, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push(
                    (!arr.is_null(i))
                        .then(|| Literal::int(days_to_years_since_epoch(arr.value(i)))),
                );
            }
        }
        (Transform::Year, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::int(days_to_years_since_epoch(days))));
                }
            }
        }
        (Transform::Month, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push(
                    (!arr.is_null(i))
                        .then(|| Literal::int(days_to_months_since_epoch(arr.value(i)))),
                );
            }
        }
        (Transform::Month, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::int(days_to_months_since_epoch(days))));
                }
            }
        }
        (Transform::Day, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::date(arr.value(i))));
            }
        }
        (Transform::Day, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::date(days)));
                }
            }
        }
        (Transform::Identity, DataType::Boolean) => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::bool(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::date(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Int32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::int(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Int64) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::long(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Utf8) => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::LargeUtf8) => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Utf8View) => {
            let arr = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Timestamp(TimeUnit::Microsecond, tz)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else if tz.is_some() {
                    out.push(Some(Literal::timestamptz(arr.value(i))));
                } else {
                    out.push(Some(Literal::timestamp(arr.value(i))));
                }
            }
        }
        (Transform::Identity, DataType::Binary) => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Identity, DataType::LargeBinary) => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Identity, DataType::BinaryView) => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Bucket(n), _) if n > 0 => {
            let normalized = match column.data_type() {
                DataType::LargeUtf8 | DataType::Utf8View => {
                    arrow::compute::cast(column, &DataType::Utf8)
                        .map_err(|_| unsupported_partition_column())?
                }
                DataType::LargeBinary | DataType::BinaryView => {
                    arrow::compute::cast(column, &DataType::Binary)
                        .map_err(|_| unsupported_partition_column())?
                }
                _ => column.clone(),
            };
            let tf = iceberg::transform::create_transform_function(&transform)
                .map_err(|_| unsupported_partition_column())?;
            let transformed = tf
                .transform(normalized)
                .map_err(|_| unsupported_partition_column())?;
            let arr = transformed
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push(
                    (!column.is_null(i) && !arr.is_null(i)).then(|| Literal::int(arr.value(i))),
                );
            }
        }
        (Transform::Truncate(w), _) if w > 0 => {
            let normalized = match column.data_type() {
                DataType::LargeUtf8 | DataType::Utf8View => {
                    arrow::compute::cast(column, &DataType::Utf8)
                        .map_err(|_| unsupported_partition_column())?
                }
                DataType::LargeBinary | DataType::BinaryView => {
                    arrow::compute::cast(column, &DataType::Binary)
                        .map_err(|_| unsupported_partition_column())?
                }
                _ => column.clone(),
            };
            let tf = iceberg::transform::create_transform_function(&transform)
                .map_err(|_| unsupported_partition_column())?;
            let transformed = tf
                .transform(normalized)
                .map_err(|_| unsupported_partition_column())?;
            return compute_partition_column(Transform::Identity, &transformed);
        }
        _ => return Err(unsupported_partition_column()),
    }
    Ok(out)
}

fn unsupported_partition_column() -> Error {
    Error::refusal(
        "unsupported_write_layout",
        "partition source column type does not match a qualified Iceberg transform",
    )
}

/// Converts proleptic Gregorian days since `1970-01-01` into `(year, month)` where `month` is `1..=12`.
fn days_to_year_month(days: i32) -> (i64, i64) {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m)
}

/// Converts proleptic Gregorian days since `1970-01-01` into years since `1970`.
fn days_to_years_since_epoch(days: i32) -> i32 {
    let (year, _) = days_to_year_month(days);
    (year - 1970) as i32
}

/// Converts proleptic Gregorian days since `1970-01-01` into months since `1970-01`.
pub(crate) fn days_to_months_since_epoch(days: i32) -> i32 {
    let (year, m) = days_to_year_month(days);
    ((year - 1970) * 12 + (m - 1)) as i32
}

/// Returns whether `transform` preserves inequality ordering (`a <= b => T(a) <= T(b)`).
pub(crate) fn is_order_preserving_transform(transform: iceberg::spec::Transform) -> bool {
    matches!(
        transform,
        iceberg::spec::Transform::Identity
            | iceberg::spec::Transform::Year
            | iceberg::spec::Transform::Month
            | iceberg::spec::Transform::Day
            | iceberg::spec::Transform::Truncate(_)
    )
}

/// Computes the partition literal for a single primitive source value under `transform`.
pub(crate) fn compute_partition_literal(
    transform: iceberg::spec::Transform,
    source_type: &iceberg::spec::Type,
    lit: &iceberg::spec::PrimitiveLiteral,
) -> Option<iceberg::spec::PrimitiveLiteral> {
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type};

    let Type::Primitive(prim) = source_type else {
        return None;
    };
    let array: ArrayRef = match (prim, lit) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => {
            Arc::new(BooleanArray::from(vec![*v]))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Arc::new(Int32Array::from(vec![*v])),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Arc::new(Int64Array::from(vec![*v])),
        (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Arc::new(Date32Array::from(vec![*v])),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]).with_timezone("+00:00"))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(v)) => {
            Arc::new(StringArray::from(vec![v.as_str()]))
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(v)) => {
            Arc::new(BinaryArray::from(vec![v.as_slice()]))
        }
        _ => return None,
    };
    match compute_partition_column(transform, &array)
        .ok()?
        .into_iter()
        .next()
        .flatten()?
    {
        Literal::Primitive(p) => Some(p),
        _ => None,
    }
}

/// Verifies every partition literal of a [`DataFile`] against its qualified transform.
///
/// Year, Month, Day, and Bucket transforms store integer ordinals; Identity and
/// Truncate store primitive values. Every field is checked.
pub(crate) fn validate_partition_tuple(
    spec: &iceberg::spec::PartitionSpec,
    file: &DataFile,
) -> Result<()> {
    use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
    if file.partition().fields().len() != spec.fields().len() {
        return Err(Error::refusal(
            "unsupported_write_layout",
            "snapshot data file partition tuple does not match its partition spec",
        ));
    }
    for (idx, field) in spec.fields().iter().enumerate() {
        let valid = matches!(
            (&field.transform, &file.partition()[idx]),
            (_, None)
                | (
                    Transform::Year | Transform::Month | Transform::Day | Transform::Bucket(_),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(_))),
                )
                | (
                    Transform::Identity | Transform::Truncate(_),
                    Some(Literal::Primitive(_))
                )
        );
        if !valid {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "partition literal does not match its qualified transform",
            ));
        }
    }
    Ok(())
}
