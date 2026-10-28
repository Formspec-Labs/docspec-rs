//! Bound copies while releasing unused payloads in Arrow view columns.
// Rust guideline compliant 2026-02-21
use crate::{
    Result,
    budget::{Budget, admit_array, clean_null_views, dirty_view_bytes},
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryViewArray, GenericByteViewArray, StringViewArray,
        types::ByteViewType,
    },
    record_batch::RecordBatch,
};
use std::{collections::HashSet, sync::Arc};

fn row_bytes<T: ByteViewType>(array: &GenericByteViewArray<T>, row: usize) -> usize {
    let length = if array.is_null(row) {
        0
    } else {
        array.views()[row] as u32 as usize
    };
    // Inline bytes already occupy the sixteen-byte view header.
    17usize.saturating_add(if length > 12 { length } else { 0 })
}

fn referenced<T: ByteViewType>(array: &GenericByteViewArray<T>, rows: usize) -> usize {
    (0..rows)
        .map(|row| row_bytes(array, row))
        .fold(std::mem::size_of_val(array), usize::saturating_add)
}

pub(super) fn reference_bytes(array: &dyn Array, rows: usize) -> Option<usize> {
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        Some(referenced(array, rows))
    } else {
        array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .map(|array| referenced(array, rows))
    }
}

pub(crate) fn referenced_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|array| {
            reference_bytes(array.as_ref(), array.len())
                .unwrap_or_else(|| array.get_array_memory_size())
        })
        .fold(0, usize::saturating_add)
}

pub(super) fn add_weights(array: &dyn Array, weights: &mut [usize]) -> bool {
    fn add<T: ByteViewType>(array: &GenericByteViewArray<T>, weights: &mut [usize]) {
        for (row, weight) in weights.iter_mut().enumerate() {
            *weight = weight.saturating_add(row_bytes(array, row));
        }
        if let Some(first) = weights.first_mut() {
            *first = first.saturating_add(std::mem::size_of_val(array));
        }
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        add(array, weights);
        true
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        add(array, weights);
        true
    } else {
        false
    }
}

fn collect<T: ByteViewType>(
    view: &GenericByteViewArray<T>,
    array: ArrayRef,
    budget: &Budget,
) -> Result<ArrayRef> {
    let payload = view
        .views()
        .iter()
        .enumerate()
        .map(|(row, raw)| {
            let length = if view.is_null(row) {
                0
            } else {
                *raw as u32 as usize
            };
            if length > 12 { length } else { 0 }
        })
        .fold(0usize, usize::saturating_add);
    let upper = view
        .data_buffers()
        .iter()
        .map(|buffer| buffer.len())
        .fold(0usize, usize::saturating_add);
    if payload >= upper / 2 {
        return Ok(array);
    }

    // Shared buffer references must not make a small backing allocation look
    // larger than the logical copies gc() would create. Charge the temporary
    // deduplication table before allocating it, then release it before copying.
    let metadata = budget.reserve(
        view.data_buffers()
            .len()
            .saturating_mul(32)
            .saturating_add(1024),
    )?;
    let mut seen = HashSet::with_capacity(view.data_buffers().len());
    let retained = view
        .data_buffers()
        .iter()
        .filter(|buffer| seen.insert(buffer.data_ptr()))
        .map(|buffer| buffer.len())
        .fold(0usize, usize::saturating_add);
    drop(seen);
    drop(metadata);
    if payload >= retained / 2 {
        return Ok(array);
    }

    // Qualify only Arrow58's single-buffer gc path. Its multi-buffer path
    // groups out-of-line values but indexes by all rows, which is unsuitable
    // for mixed inline/null arrays. Compaction is optional; preserve sharing
    // when a copy would require that path.
    if payload >= i32::MAX as usize {
        return Ok(array);
    }

    // Include view descriptors, validity and native gc's temporary grouping.
    // Output admission stays on every returned buffer, including child owners.
    let output = payload
        .saturating_add(view.len().saturating_mul(128))
        .saturating_add(2048);
    let clean = if dirty_view_bytes(array.as_ref()) == 0 {
        0
    } else {
        view.len()
            .saturating_mul(16)
            .saturating_add(
                view.data_buffers()
                    .len()
                    .saturating_mul(std::mem::size_of::<arrow::buffer::Buffer>()),
            )
            .saturating_add(1024)
    };
    let mut memory = budget.reserve(output.saturating_add(clean))?;
    let temporary = memory
        .split(clean)
        .expect("reserved temporary view workspace");
    let cleared = clean_null_views(array)?;
    let view = cleared
        .as_any()
        .downcast_ref::<GenericByteViewArray<T>>()
        .expect("clearing headers preserves the view type");
    let collected = view.gc();
    // Arrow's inline-only fast path shares sliced headers, and every gc path
    // shares validity. Copy those small descriptors before wrapping owners so
    // a tiny slice cannot retain a hidden parent allocation.
    let headers = if payload == 0 {
        collected.views().to_vec().into()
    } else {
        collected.views().clone()
    };
    let nulls = collected
        .nulls()
        .map(|nulls| nulls.iter().collect::<arrow::buffer::NullBuffer>());
    let result = Arc::new(GenericByteViewArray::<T>::try_new(
        headers,
        collected.data_buffers().to_vec(),
        nulls,
    )?);
    drop(collected);
    drop(cleared);
    drop(temporary);
    admit_array(result, memory)
}

pub(super) fn compact(array: &ArrayRef, budget: &Budget) -> Result<ArrayRef> {
    if let Some(view) = array.as_any().downcast_ref::<StringViewArray>() {
        collect(view, array.clone(), budget)
    } else if let Some(view) = array.as_any().downcast_ref::<BinaryViewArray>() {
        collect(view, array.clone(), budget)
    } else {
        let data = array.to_data();
        if data.child_data().is_empty() {
            return Ok(array.clone());
        }
        let mut changed = false;
        let children = data
            .child_data()
            .iter()
            .map(|child| {
                let original = arrow::array::make_array(child.clone());
                let compacted = compact(&original, budget)?;
                changed |= !Arc::ptr_eq(&original, &compacted);
                Ok(compacted.to_data())
            })
            .collect::<Result<Vec<_>>>()?;
        if changed {
            Ok(arrow::array::make_array(
                data.into_builder().child_data(children).build()?,
            ))
        } else {
            Ok(array.clone())
        }
    }
}

/// Releases unused view payload, retaining rows, types, metadata and ordering.
///
/// # Errors
/// Refuses before copying when the shared memory pool cannot admit the copy.
pub(crate) fn compact_views(batch: RecordBatch, budget: &Budget) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| compact(array, budget))
        .collect::<Result<Vec<_>>>()?;
    if columns
        .iter()
        .zip(batch.columns())
        .all(|(left, right)| Arc::ptr_eq(left, right))
    {
        return Ok(batch);
    }
    let options =
        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}
