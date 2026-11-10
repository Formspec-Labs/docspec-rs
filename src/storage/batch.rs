//! Arrow batch type coercion, skewed-column detection, and bounded batch splitting.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result, budget::Budget};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
mod views;
pub(crate) use views::{compact_views, referenced_batch_bytes};

#[cfg(test)]
mod alignment_tests;
#[cfg(test)]
mod retained_tests;

pub(crate) fn coerce_data_type(
    data_type: &arrow::datatypes::DataType,
) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::List(field) => DataType::List(Arc::new(coerce_arrow_field(field))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(coerce_arrow_field(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(coerce_arrow_field(field)), *size)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(coerce_arrow_field(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn coerce_arrow_field(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    let coerced = coerce_data_type(field.data_type());
    if &coerced == field.data_type() {
        field.clone()
    } else {
        field.clone().with_data_type(coerced)
    }
}

fn coerce_arrow_schema(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(coerce_arrow_field(field)))
        .collect::<Vec<_>>();
    arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub(crate) fn arrow_schema_to_schema_auto_assign_ids(
    schema: &arrow::datatypes::Schema,
) -> Result<iceberg::spec::Schema> {
    let coerced = coerce_arrow_schema(schema);
    Ok(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
        &coerced,
    )?)
}

pub(crate) fn coerce_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    if schema
        .fields()
        .iter()
        .all(|field| &coerce_data_type(field.data_type()) == field.data_type())
    {
        return Ok(batch);
    }
    let coerced_schema = Arc::new(coerce_arrow_schema(schema.as_ref()));
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (column, target_field) in batch.columns().iter().zip(coerced_schema.fields()) {
        if column.data_type() == target_field.data_type() {
            columns.push(column.clone());
        } else {
            columns.push(arrow::compute::cast(column, target_field.data_type())?);
        }
    }
    Ok(RecordBatch::try_new(coerced_schema, columns)?)
}

/// Returns whether `schema` contains a `List<Struct<...>>` column.
pub(crate) fn has_list_of_struct(schema: &arrow::datatypes::Schema) -> bool {
    schema.fields().iter().any(|field| {
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::List(item)
                if matches!(item.data_type(), arrow::datatypes::DataType::Struct(_))
        )
    })
}

/// Returns the referenced byte size of a `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary`
/// array slice across `num_rows` rows, or `None` for other array types.
fn referenced_variable_bytes(col: &dyn arrow::array::Array, num_rows: usize) -> Option<usize> {
    if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        let offsets = utf8.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i32>()))
    } else if let Some(utf8) = col
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        let offsets = utf8.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i64>()))
    } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        let offsets = bin.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i32>()))
    } else if let Some(bin) = col
        .as_any()
        .downcast_ref::<arrow::array::LargeBinaryArray>()
    {
        let offsets = bin.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i64>()))
    } else {
        views::reference_bytes(col, num_rows)
    }
}

/// Returns whether `batch` contains a `List<Struct>` column, an uncompacted
/// variable-length slice whose backing buffer exceeds its referenced bytes, or a
/// multi-row batch with a skewed multi-megabyte `Utf8`/`Binary` cell (`> limit / 16`).
pub(crate) fn has_splittable_skewed_column(batch: &RecordBatch, limit: usize) -> bool {
    if has_list_of_struct(batch.schema().as_ref()) {
        return true;
    }
    let num_rows = batch.num_rows();
    if num_rows == 0 || limit < (1 << 20) {
        return false;
    }
    let cell_threshold = (limit / 16).max(1 << 20);
    for col in batch.columns() {
        if let Some(ref_bytes) = referenced_variable_bytes(col.as_ref(), num_rows)
            && col.get_array_memory_size() > ref_bytes.saturating_add(cell_threshold)
        {
            return true;
        }
        if num_rows <= 1 {
            continue;
        }
        if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            let offsets = utf8.value_offsets();
            if (0..num_rows)
                .any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold)
            {
                return true;
            }
        } else if let Some(utf8) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            let offsets = utf8.value_offsets();
            if (0..num_rows)
                .any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold)
            {
                return true;
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
            let offsets = bin.value_offsets();
            if (0..num_rows)
                .any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold)
            {
                return true;
            }
        } else if let Some(bin) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
        {
            let offsets = bin.value_offsets();
            if (0..num_rows)
                .any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold)
            {
                return true;
            }
        }
    }
    false
}

/// Computes the effective admission byte size of `batch`.
///
/// For multi-row batches, returns `batch.get_array_memory_size()`.
/// For a 1-row batch:
/// - bounds `List<Struct>` columns by their per-occurrence element size, and
/// - uses referenced slice bytes for `Utf8`/`Binary` columns so uncompacted slices
///   are recognized before deep compaction.
pub(crate) fn effective_batch_bytes(batch: &RecordBatch) -> usize {
    if batch.num_rows() != 1 {
        return batch.get_array_memory_size();
    }
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(col, field)| {
            if let arrow::datatypes::DataType::List(item) = field.data_type()
                && matches!(item.data_type(), arrow::datatypes::DataType::Struct(_))
                && let Some(list) = col.as_any().downcast_ref::<arrow::array::ListArray>()
            {
                let elems = (list.value_length(0).max(1)) as usize;
                col.get_array_memory_size().div_ceil(elems)
            } else if let Some(ref_bytes) = referenced_variable_bytes(col.as_ref(), 1) {
                ref_bytes
            } else {
                col.get_array_memory_size()
            }
        })
        .fold(0usize, usize::saturating_add)
}

/// Produces a deep-compacted slice `[offset..offset + length]` of `batch` whose
/// backing Arrow buffers retain only the values referenced by those rows.
///
/// Note: `arrow::compute::concat_batches` has a `batches.len() == 1` fast path
/// that returns `batches[0].clone()` without reallocating buffers. Passing an
/// trailing 0-row batch forces `MutableArrayData` compaction into fresh buffers.
pub(crate) fn compact_batch_slice(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
    budget: &Budget,
) -> Result<RecordBatch> {
    let sliced = batch.slice(offset, length);
    if sliced.num_columns() == 0 {
        return Ok(sliced);
    }
    let schema = batch.schema();
    let empty = RecordBatch::new_empty(schema.clone());
    let compacted = arrow::compute::concat_batches(&schema, &[sliced, empty])?;
    compact_views(compacted, budget)
}

/// Bytes a batch's rows reference, counting view payloads and slice ranges.
///
/// The export measure owns this: it follows list and map offsets into the
/// child rows a slice references, where Arrow's `get_slice_memory_size`
/// counts the whole child. A batch it cannot measure counts as fully referenced.
fn sliced_batch_bytes(batch: &RecordBatch) -> usize {
    crate::export::portable_batch_bytes(batch).unwrap_or_else(|_| batch.get_array_memory_size())
}

/// Releases buffers a batch retains beyond its own rows before an operator keeps it.
///
/// DataFusion 53.1 emits a grouped aggregate's output as `batch_rows` slices
/// of one array, and repartition, sort and hash-join build inputs charge each
/// slice for the whole array: charges grow with the square of groups per
/// partition (ct-deg8 model run: 5.3 GB charged for 118 MB). An outer join
/// does the same with its unmatched build rows when its output coalescer
/// already holds a few rows. A batch charged at more than four times what its
/// rows reference, and by more than 1 MiB, is copied once into its own
/// buffers; others keep only view compaction.
///
/// # Errors
/// Refuses when the shared memory pool cannot admit a view compaction copy.
pub(crate) fn compact_retained(batch: RecordBatch, budget: &Budget) -> Result<RecordBatch> {
    let charged = datafusion::common::utils::memory::get_record_batch_memory_size(&batch);
    let referenced = sliced_batch_bytes(&batch);
    if charged > referenced.saturating_mul(4) && charged - referenced > 1 << 20 {
        compact_batch_slice(&batch, 0, batch.num_rows(), budget)
    } else {
        compact_views(batch, budget)
    }
}

/// Splits a multi-row `RecordBatch` containing `List<Struct>` columns into
/// deep-compacted row slices that fit within `limit` in a single linear pass
/// by per-row byte weight so each slice is compacted at most once.
pub(crate) fn split_oversized_batch(
    batch: RecordBatch,
    limit: usize,
    budget: &crate::budget::Budget,
) -> Result<Vec<RecordBatch>> {
    if batch.get_array_memory_size() <= limit {
        return Ok(vec![batch]);
    }
    if !has_splittable_skewed_column(&batch, limit) {
        let bytes = batch.get_array_memory_size();
        return Err(Error::refusal(
            "oversized_batch",
            format!(
                "decoded batch uses {bytes} bytes, exceeding the {limit} byte allowance; reduce batch rows or split oversized values"
            ),
        ));
    }
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        let compacted =
            if num_rows == 1 && batch.get_array_memory_size() > effective_batch_bytes(&batch) {
                compact_batch_slice(&batch, 0, 1, budget)?
            } else {
                batch
            };
        let effective = effective_batch_bytes(&compacted);
        if effective > limit {
            return Err(Error::refusal(
                "oversized_batch",
                format!(
                    "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                ),
            ));
        }
        return Ok(vec![compacted]);
    }

    // Estimate per-row byte weights in a single linear pass across columns.
    let mut row_weights = vec![0usize; num_rows];
    for col in batch.columns() {
        if views::add_weights(col.as_ref(), &mut row_weights) {
            continue;
        }
        let col_bytes = col.get_array_memory_size();
        if let Some(list) = col.as_any().downcast_ref::<arrow::array::ListArray>() {
            let offsets = list.value_offsets();
            let total_elems = usize::try_from(offsets[num_rows] - offsets[0])
                .unwrap_or(0)
                .max(1);
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let elems = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                let share = col_bytes.saturating_mul(elems).div_ceil(total_elems);
                *weight = weight.saturating_add(share.saturating_add(8));
            }
        } else if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            let offsets = utf8.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(8));
            }
        } else if let Some(utf8) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            let offsets = utf8.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(12));
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
            let offsets = bin.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(8));
            }
        } else if let Some(bin) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
        {
            let offsets = bin.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(12));
            }
        } else {
            let per_row = col_bytes.div_ceil(num_rows);
            for weight in &mut row_weights {
                *weight = weight.saturating_add(per_row);
            }
        }
    }

    let mut ranges = Vec::new();
    let mut slice_start = 0usize;
    let mut acc_bytes = 0usize;
    for (r, &w) in row_weights.iter().enumerate() {
        if r > slice_start && acc_bytes.saturating_add(w) > limit {
            ranges.push((slice_start, r - slice_start));
            slice_start = r;
            acc_bytes = w;
        } else {
            acc_bytes = acc_bytes.saturating_add(w);
        }
    }
    if slice_start < num_rows {
        ranges.push((slice_start, num_rows - slice_start));
    }

    let mut out = Vec::with_capacity(ranges.len());
    for (start, len) in ranges {
        let slice = compact_batch_slice(&batch, start, len, budget)?;
        if slice.get_array_memory_size() <= limit {
            out.push(slice);
        } else if slice.num_rows() <= 1 {
            let effective = effective_batch_bytes(&slice);
            if effective > limit {
                return Err(Error::refusal(
                    "oversized_batch",
                    format!(
                        "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                    ),
                ));
            }
            out.push(slice);
        } else {
            for r in 0..slice.num_rows() {
                let one = compact_batch_slice(&batch, start + r, 1, budget)?;
                let effective = effective_batch_bytes(&one);
                if effective > limit {
                    return Err(Error::refusal(
                        "oversized_batch",
                        format!(
                            "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                        ),
                    ));
                }
                out.push(one);
            }
        }
    }
    Ok(out)
}

fn aligned_native_type(
    source: &arrow::datatypes::DataType,
    target: &arrow::datatypes::DataType,
) -> Option<arrow::datatypes::DataType> {
    use arrow::datatypes::DataType;
    if source == target {
        return Some(target.clone());
    }
    match (source, target) {
        (DataType::List(a), DataType::List(b)) => {
            Some(DataType::List(Arc::new(b.as_ref().clone().with_data_type(
                aligned_native_type(a.data_type(), b.data_type())?,
            ))))
        }
        (DataType::LargeList(a), DataType::LargeList(b)) => Some(DataType::LargeList(Arc::new(
            b.as_ref()
                .clone()
                .with_data_type(aligned_native_type(a.data_type(), b.data_type())?),
        ))),
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) if n == m => {
            Some(DataType::FixedSizeList(
                Arc::new(
                    b.as_ref()
                        .clone()
                        .with_data_type(aligned_native_type(a.data_type(), b.data_type())?),
                ),
                *m,
            ))
        }
        (DataType::Struct(source), DataType::Struct(target)) => {
            let fields = source
                .iter()
                .map(|field| (field.name(), field.data_type()))
                .collect::<std::collections::BTreeMap<_, _>>();
            if fields.len() != source.len() || source.len() != target.len() {
                return None;
            }
            target
                .iter()
                .map(|field| {
                    let source = fields.get(field.name())?;
                    Some(Arc::new(field.as_ref().clone().with_data_type(
                        aligned_native_type(source, field.data_type())?,
                    )))
                })
                .collect::<Option<Vec<_>>>()
                .map(|fields| DataType::Struct(fields.into()))
        }
        // The maintained writer accepts views directly. Flattening shared views
        // can multiply payload bytes, so preserve storage rather than casting.
        (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8)
        | (DataType::BinaryView, DataType::Binary | DataType::LargeBinary) => Some(source.clone()),
        // Preserve the existing lossless scalar whitelist. Broad Arrow cast
        // admission would also permit truncation and positional struct fallback.
        (DataType::Timestamp(unit, Some(_)), DataType::Timestamp(target_unit, Some(_))) => {
            (unit == target_unit).then(|| target.clone())
        }
        (
            DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16,
            DataType::Int32,
        )
        | (DataType::UInt32, DataType::Int64)
        | (DataType::Binary, DataType::LargeBinary)
        | (DataType::LargeBinary, DataType::Binary)
        | (DataType::Utf8, DataType::LargeUtf8)
        | (DataType::LargeUtf8, DataType::Utf8) => Some(target.clone()),
        _ => None,
    }
}

/// Aligns native field metadata through maintained casts, preserving actual values.
pub(crate) fn align_native_batch(
    batch: RecordBatch,
    expected: &arrow::datatypes::Schema,
    budget: &crate::budget::Budget,
) -> Result<(RecordBatch, Option<tokio::sync::OwnedSemaphorePermit>)> {
    use arrow::datatypes::{DataType, Schema};
    let source = batch.schema();
    let positions = source
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name(), index))
        .collect::<std::collections::HashMap<_, _>>();
    if positions.len() != source.fields().len()
        || positions.len() != expected.fields().len()
        || expected
            .fields()
            .iter()
            .any(|field| !positions.contains_key(field.name()))
    {
        return Err(Error::refusal(
            "schema_mismatch",
            "writer and native schema need the same unique field names",
        ));
    }
    // The maintained Parquet writer consumes arrays in native schema order.
    // Projecting the existing arrays is O(columns), with no payload copy.
    let order = expected
        .fields()
        .iter()
        .map(|field| positions[field.name()])
        .collect::<Vec<_>>();
    let batch = batch.project(&order)?;
    let source = batch.schema();
    let fields = source
        .fields()
        .iter()
        .zip(expected.fields())
        .map(|(field, target)| {
            let aligned =
                aligned_native_type(field.data_type(), target.data_type()).ok_or_else(|| {
                    Error::refusal(
                        "schema_mismatch",
                        format!(
                            "writer field {} has incompatible native types: {:?} -> {:?}",
                            field.name(),
                            field.data_type(),
                            target.data_type()
                        ),
                    )
                })?;
            Ok(target.as_ref().clone().with_data_type(aligned))
        })
        .collect::<Result<Vec<_>>>()?;
    let copies_payload = source.fields().iter().zip(&fields).any(|(a, b)| {
        a.data_type() != b.data_type()
            && !matches!((a.data_type(), b.data_type()),
                    (DataType::Timestamp(unit, Some(_)), DataType::Timestamp(target_unit, Some(_)))
                    if unit == target_unit)
    });
    // Arrow58 reinterprets and clones the same timestamp values/validity when
    // both timezone tags are present and units match. No payload is allocated,
    // so the already admitted input must not be charged again for this cast.
    // The whitelist widens values at most fourfold. Cover retained input,
    // slice, compacted children, casts and aligned buffer rounding before any
    // copy; writer-owned retained buffers have their separate lifetime admission.
    let memory = if copies_payload {
        Some(
            budget.reserve(
                batch
                    .get_array_memory_size()
                    .checked_mul(8)
                    .and_then(|n| n.checked_add(512))
                    .ok_or_else(|| {
                        Error::refusal("capacity", "native alignment allocation overflow")
                    })?,
            )?,
        )
    } else {
        None
    };
    let options = arrow::compute::CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = batch
        .columns()
        .iter()
        .zip(source.fields())
        .zip(&fields)
        .map(|((array, field), target)| {
            if array.data_type() == target.data_type() {
                Ok(array.clone())
            } else {
                // Trim unused child nulls only in a column being aligned;
                // unrelated columns keep their existing buffers.
                let array = if matches!(
                    array.data_type(),
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::Struct(_)
                ) {
                    let one = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![field.clone()])),
                        vec![array.clone()],
                    )?;
                    compact_batch_slice(&one, 0, batch.num_rows(), budget)?
                        .column(0)
                        .clone()
                } else {
                    array.clone()
                };
                Ok(arrow::compute::cast_with_options(
                    &array,
                    target.data_type(),
                    &options,
                )?)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    if batch.get_array_memory_size() > budget.limits.max_batch_bytes {
        return Err(Error::refusal(
            "capacity",
            "aligned native batch exceeds configured byte allowance",
        ));
    }
    Ok((batch, memory))
}
