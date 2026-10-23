//! Arrow batch type coercion, skewed-column detection, and bounded batch splitting.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

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
        None
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
) -> Result<RecordBatch> {
    let sliced = batch.slice(offset, length);
    if sliced.num_columns() == 0 {
        return Ok(sliced);
    }
    let schema = batch.schema();
    let empty = RecordBatch::new_empty(schema.clone());
    Ok(arrow::compute::concat_batches(&schema, &[sliced, empty])?)
}

/// Splits a multi-row `RecordBatch` containing `List<Struct>` columns into
/// deep-compacted row slices that fit within `limit` in a single linear pass
/// by per-row byte weight so each slice is compacted at most once.
pub(crate) fn split_oversized_batch(batch: RecordBatch, limit: usize) -> Result<Vec<RecordBatch>> {
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
                compact_batch_slice(&batch, 0, 1)?
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
        let slice = compact_batch_slice(&batch, start, len)?;
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
                let one = compact_batch_slice(&batch, start + r, 1)?;
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
