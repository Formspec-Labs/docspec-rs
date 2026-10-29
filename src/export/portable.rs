//! Normalize Arrow view representations before retaining or encoding batches.
// Rust guideline compliant 2026-02-21
use crate::{
    Error, Result,
    budget::{Budget, clean_null_views, dirty_view_bytes},
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray,
        LargeListArray, ListArray, MapArray, StringViewArray, StructArray,
    },
    compute::BatchCoalescer,
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

struct PortableBatches {
    batch: RecordBatch,
    schema: SchemaRef,
    budget: Budget,
    base: usize,
    batch_bytes: usize,
    oversized: bool,
    next_cost: Option<usize>,
    offset: usize,
    finished: bool,
}

pub(crate) fn batch_bytes(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(0usize, |bytes, column| {
        Ok(bytes.saturating_add(portable_bytes(column.as_ref(), false)?))
    })
}

fn batch_bytes_as(batch: &RecordBatch, schema: &Schema) -> Result<usize> {
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .try_fold(0usize, |bytes, (column, field)| {
            Ok(bytes.saturating_add(portable_bytes_as(
                column.as_ref(),
                false,
                field.data_type(),
            )?))
        })
}

impl PortableBatches {
    fn new(batch: RecordBatch, schema: SchemaRef, budget: Budget) -> Result<Self> {
        validate_types(&batch, &schema)?;
        let batch_bytes = batch_bytes_as(&batch, &schema)?;
        let oversized = batch_bytes > budget.limits.max_batch_bytes
            && batch.num_rows() > 1;
        let base = batch_bytes_as(&RecordBatch::new_empty(batch.schema()), &schema)?;
        Ok(Self {
            batch,
            schema,
            budget,
            base,
            batch_bytes,
            oversized,
            next_cost: None,
            offset: 0,
            finished: false,
        })
    }

    fn cost(&mut self, row: usize) -> Result<usize> {
        if let Some(cost) = self.next_cost.take() {
            return Ok(cost);
        }
        Ok(batch_bytes_as(&self.batch.slice(row, 1), &self.schema)?.saturating_sub(self.base))
    }
}

/// Pack already normalized rows without rebuilding an oversized batch.
pub(crate) fn coalesce_portable_stream(
    input: SendableRecordBatchStream,
    rows: usize,
    budget: &Budget,
) -> SendableRecordBatchStream {
    struct State {
        input: SendableRecordBatchStream,
        coalescer: BatchCoalescer,
        budget: Budget,
        pending: Option<PortableBatches>,
        bytes: usize,
        exhausted: bool,
    }
    let schema = input.schema();
    let state = State {
        input,
        coalescer: BatchCoalescer::new(schema.clone(), rows),
        budget: budget.clone(),
        pending: None,
        bytes: 0,
        exhausted: false,
    };
    let stream = futures::stream::try_unfold(state, move |mut state| async move {
        loop {
            if let Some(batch) = state.coalescer.next_completed_batch() {
                let batch = portable_batch(batch, state.coalescer.schema(), &state.budget)?;
                return Ok::<_, Error>(Some((batch, state)));
            }
            if let Some(pending) = &mut state.pending {
                if pending.offset == pending.batch.num_rows() {
                    state.pending = None;
                    continue;
                }
                if state.coalescer.is_empty() {
                    state.bytes = pending.base;
                }
                let start = pending.offset;
                let available = rows - state.coalescer.get_buffered_rows();
                if start == 0
                    && !pending.oversized
                    && pending.batch.num_rows() <= available
                    && state
                        .bytes
                        .saturating_add(pending.batch_bytes.saturating_sub(pending.base))
                        <= state.budget.limits.max_batch_bytes
                {
                    state.bytes = state
                        .bytes
                        .saturating_add(pending.batch_bytes.saturating_sub(pending.base));
                    let full_batch = pending.batch.clone();
                    pending.offset = pending.batch.num_rows();
                    state.coalescer.push_batch(full_batch)?;
                    continue;
                }
                let mut end = start;
                while end < pending.batch.num_rows() && end - start < available {
                    let cost = pending.cost(end)?;
                    let next = state.bytes.saturating_add(cost);
                    if next > state.budget.limits.max_batch_bytes {
                        pending.next_cost = Some(cost);
                        break;
                    }
                    state.bytes = next;
                    end += 1;
                }
                if end == start {
                    if state.coalescer.is_empty() {
                        return Err(Error::refusal(
                            "capacity",
                            "single portable row exceeds configured byte allowance",
                        ));
                    }
                    state.coalescer.finish_buffered_batch()?;
                    continue;
                }
                state
                    .coalescer
                    .push_batch(pending.batch.slice(start, end - start))?;
                pending.offset = end;
                continue;
            }
            if state.exhausted {
                return Ok(None);
            }
            match state.input.try_next().await? {
                Some(batch) => {
                    state.pending = Some(PortableBatches::new(
                        batch,
                        state.coalescer.schema(),
                        state.budget.clone(),
                    )?);
                }
                None => {
                    state.exhausted = true;
                    state.coalescer.finish_buffered_batch()?;
                }
            }
        }
    })
    .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)));
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

impl Iterator for PortableBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let start = self.offset;
        let end = if self.oversized {
            let mut bytes = self.base;
            let mut end = start;
            while end < self.batch.num_rows() {
                let cost = match self.cost(end) {
                    Ok(cost) => cost,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                let next = bytes.saturating_add(cost);
                if next > self.budget.limits.max_batch_bytes && end > start {
                    self.next_cost = Some(cost);
                    break;
                }
                bytes = next;
                end += 1;
                if bytes > self.budget.limits.max_batch_bytes {
                    break;
                }
            }
            end
        } else {
            self.batch.num_rows()
        };
        self.offset = end;
        self.finished = end == self.batch.num_rows();
        let result = portable_batch(
            self.batch.slice(start, end - start),
            self.schema.clone(),
            &self.budget,
        );
        if result.is_err() {
            self.finished = true;
        }
        Some(result)
    }
}

pub(crate) fn portable_stream(
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    budget: &Budget,
) -> SendableRecordBatchStream {
    let budget = budget.clone();
    let output_schema = schema.clone();
    let stream = input
        .map(move |batch| {
            batch
                .map_err(Error::from)
                .and_then(|batch| PortableBatches::new(batch, schema.clone(), budget.clone()))
                .map(futures::stream::iter)
        })
        .try_flatten()
        .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)));
    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}

pub(crate) fn portable_data_type(data_type: &DataType) -> DataType {
    let field = |field: &Arc<arrow::datatypes::Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(portable_data_type(field.data_type())),
        )
    };
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(item) => DataType::List(field(item)),
        DataType::LargeList(item) => DataType::LargeList(field(item)),
        DataType::FixedSizeList(item, n) => DataType::FixedSizeList(field(item), *n),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(item, sorted) => DataType::Map(field(item), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(portable_data_type(value)))
        }
        other => other.clone(),
    }
}

pub(crate) fn portable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(portable_data_type(field.data_type()))
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn compatible(from: &DataType, to: &DataType) -> bool {
    // Nullability is checked against values by Arrow's strict constructors.
    // Struct labels must match: its cast otherwise reorders fields by name.
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| a.name() == b.name() && compatible(a.data_type(), b.data_type()))
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            compatible(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) => {
            n == m && compatible(a.data_type(), b.data_type())
        }
        (DataType::Map(a, sorted_a), DataType::Map(b, sorted_b)) => {
            sorted_a == sorted_b && compatible(a.data_type(), b.data_type())
        }
        (DataType::Dictionary(a, av), DataType::Dictionary(b, bv)) => a == b && compatible(av, bv),
        (DataType::FixedSizeBinary(a), DataType::FixedSizeBinary(b)) => a == b,
        (a, b) if binary_type(a) && binary_type(b) => true,
        (a, b) => a.equals_datatype(b),
    }
}

fn validate_types(batch: &RecordBatch, schema: &Schema) -> Result<()> {
    if batch.num_columns() != schema.fields().len() {
        return Err(Error::refusal(
            "invalid_export",
            "executed batch column count changed",
        ));
    }
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        if !compatible(&portable_data_type(column.data_type()), field.data_type()) {
            return Err(Error::refusal(
                "invalid_export",
                format!(
                    "executed batch type changed for {}: {:?}; expected {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ),
            ));
        }
    }
    Ok(())
}

fn portable_bytes(array: &dyn Array, view_storage: bool) -> Result<usize> {
    portable_bytes_as(array, view_storage, array.data_type())
}

fn binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
    )
}

fn binary_value_bytes(array: &dyn Array) -> Result<Option<usize>> {
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        let offsets = array.value_offsets();
        return Ok(Some((offsets[array.len()] - offsets[0]) as usize));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        let offsets = array.value_offsets();
        return Ok(Some((offsets[array.len()] - offsets[0]) as usize));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(array.total_bytes_len()));
    }
    if let DataType::FixedSizeBinary(width) = array.data_type() {
        let width = usize::try_from(*width)
            .map_err(|_| Error::refusal("invalid_export", "negative binary width"))?;
        return Ok(Some(width.saturating_mul(array.len() - array.null_count())));
    }
    Ok(None)
}

fn portable_bytes_as(array: &dyn Array, view_storage: bool, target: &DataType) -> Result<usize> {
    // Count logical view values, including repeated references to one buffer.
    let overhead = array.len().div_ceil(8).saturating_add(256);
    if !view_storage
        && binary_type(target)
        && let Some(values) = binary_value_bytes(array)?
    {
        // Fixed arrays allocate bytes beneath nulls; variable arrays need an
        // offset per row. Fit the declared representation before its cast.
        let bytes = if let DataType::FixedSizeBinary(width) = target {
            let width = usize::try_from(*width)
                .map_err(|_| Error::refusal("invalid_export", "negative binary width"))?;
            array.len().saturating_mul(width)
        } else {
            let offset = if matches!(target, DataType::LargeBinary) {
                8
            } else {
                4
            };
            values.saturating_add((array.len() + 1).saturating_mul(offset))
        };
        return Ok(bytes.saturating_add(overhead));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        if view_storage {
            return Ok(a
                .len()
                .saturating_mul(16)
                .saturating_add(
                    a.data_buffers()
                        .len()
                        .saturating_mul(std::mem::size_of::<arrow::buffer::Buffer>()),
                )
                .saturating_add(overhead));
        }
        return Ok(a
            .total_bytes_len()
            .saturating_add((a.len() + 1).saturating_mul(4))
            .saturating_add(overhead));
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryViewArray>() {
        if view_storage {
            return Ok(a
                .len()
                .saturating_mul(16)
                .saturating_add(
                    a.data_buffers()
                        .len()
                        .saturating_mul(std::mem::size_of::<arrow::buffer::Buffer>()),
                )
                .saturating_add(overhead));
        }
        return Ok(a
            .total_bytes_len()
            .saturating_add((a.len() + 1).saturating_mul(4))
            .saturating_add(overhead));
    }
    if let (Some(a), DataType::Struct(fields)) =
        (array.as_any().downcast_ref::<StructArray>(), target)
    {
        return a
            .columns()
            .iter()
            .zip(fields)
            .try_fold(overhead, |bytes, (child, field)| {
                Ok(bytes.saturating_add(portable_bytes_as(
                    child.as_ref(),
                    view_storage,
                    field.data_type(),
                )?))
            });
    }
    if let (Some(a), DataType::List(field)) = (array.as_any().downcast_ref::<ListArray>(), target) {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(portable_bytes_as(
            a.values().slice(start, end - start).as_ref(),
            view_storage,
            field.data_type(),
        )?
        .saturating_add((a.len() + 1).saturating_mul(4))
        .saturating_add(overhead));
    }
    if let (Some(a), DataType::LargeList(field)) =
        (array.as_any().downcast_ref::<LargeListArray>(), target)
    {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(portable_bytes_as(
            a.values().slice(start, end - start).as_ref(),
            view_storage,
            field.data_type(),
        )?
        .saturating_add((a.len() + 1).saturating_mul(8))
        .saturating_add(overhead));
    }
    if let (Some(a), DataType::FixedSizeList(field, _)) =
        (array.as_any().downcast_ref::<FixedSizeListArray>(), target)
    {
        return Ok(
            portable_bytes_as(a.values().as_ref(), view_storage, field.data_type())?
                .saturating_add(overhead),
        );
    }
    if let (Some(a), DataType::Map(field, _)) = (array.as_any().downcast_ref::<MapArray>(), target)
    {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(portable_bytes_as(
            &a.entries().slice(start, end - start),
            view_storage,
            field.data_type(),
        )?
        .saturating_add((a.len() + 1).saturating_mul(4))
        .saturating_add(overhead));
    }
    if let (DataType::Dictionary(key, _), DataType::Dictionary(_, target)) =
        (array.data_type(), target)
    {
        let data = array.to_data();
        let values = arrow::array::make_array(data.child_data()[0].clone());
        let width = key
            .primitive_width()
            .ok_or_else(|| Error::refusal("invalid_export", "dictionary key is not fixed width"))?;
        return Ok(portable_bytes_as(values.as_ref(), view_storage, target)?
            .saturating_add(array.len().saturating_mul(width))
            .saturating_add(overhead));
    }
    Ok(array
        .to_data()
        .get_slice_memory_size()?
        .saturating_sub(if array.nulls().is_some() {
            array.len().div_ceil(8)
        } else {
            0
        })
        .saturating_add(overhead))
}

fn compact(array: &ArrayRef) -> Result<ArrayRef> {
    // A single-array concat deliberately returns a zero-copy slice. Including
    // an empty array uses Arrow's maintained logical-value copy for every type.
    let empty = arrow::array::new_empty_array(array.data_type());
    Ok(arrow::compute::concat(&[array.as_ref(), empty.as_ref()])?)
}

pub(crate) fn portable_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    budget: &Budget,
) -> Result<RecordBatch> {
    validate_types(&batch, &schema)?;
    // Native coalescing reserves row capacity. Trim uniquely owned buffers
    // before deciding that another admitted copy is needed; shared buffers
    // retain their ownership and are compacted below when necessary.
    let (input_schema, mut input_columns, rows) = batch.into_parts();
    input_columns
        .iter_mut()
        .for_each(|column| column.shrink_to_fit());
    let batch = RecordBatch::try_new_with_options(
        input_schema,
        input_columns,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )?;
    let mut bytes = 0usize;
    let mut conversion_bytes = 0usize;
    let over_limit = batch.get_array_memory_size() > budget.limits.max_batch_bytes;
    let mut compact_columns = Vec::with_capacity(batch.num_columns());
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        let logical = portable_bytes_as(column.as_ref(), false, field.data_type())?;
        let needs_cast = column.data_type() != field.data_type();
        let direct_view = matches!(
            column.data_type(),
            DataType::Utf8View | DataType::BinaryView
        );
        // Top-level views cast directly. Nested casts must first trim child
        // ranges, so unused repeated views cannot expand during the cast.
        let needs_compaction = !direct_view
            && (needs_cast
                || over_limit
                || column.get_array_memory_size() > logical.saturating_mul(2));
        if needs_cast || needs_compaction {
            conversion_bytes = conversion_bytes.saturating_add(logical.saturating_mul(2));
        }
        if needs_cast {
            let intermediate = portable_data_type(column.data_type());
            if &intermediate != column.data_type() && &intermediate != field.data_type() {
                conversion_bytes = conversion_bytes
                    .saturating_add(portable_bytes(column.as_ref(), false)?.saturating_mul(2));
            }
            conversion_bytes = conversion_bytes
                .saturating_add(dirty_view_bytes(column.as_ref()).saturating_mul(2));
        }
        if needs_compaction {
            conversion_bytes = conversion_bytes
                .saturating_add(portable_bytes(column.as_ref(), true)?.saturating_mul(2));
        }
        compact_columns.push(needs_compaction);
        bytes = bytes.saturating_add(logical);
    }
    if bytes > budget.limits.max_batch_bytes {
        return Err(Error::refusal(
            "capacity",
            "portable batch exceeds configured byte allowance",
        ));
    }
    let _conversion = if conversion_bytes > 0 {
        // Include the intermediate view representation and portable cast,
        // rather than assuming views have the same byte width as UTF-8.
        Some(budget.reserve(conversion_bytes)?)
    } else {
        None
    };
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .zip(compact_columns)
        .map(|((column, field), needs_compaction)| {
            let column = if needs_compaction {
                compact(column)?
            } else {
                column.clone()
            };
            let mut column = column;
            if column.data_type() != field.data_type() {
                column = clean_null_views(column)?;
                let options = arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                };
                // Normalize views first: native Arrow casts variable binary to
                // a fixed width, but has no direct BinaryView -> fixed cast.
                for target in [
                    portable_data_type(column.data_type()),
                    field.data_type().clone(),
                ] {
                    if column.data_type() != &target {
                        column = arrow::compute::cast_with_options(&column, &target, &options)?;
                    }
                }
            }
            column.shrink_to_fit();
            Ok(column)
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let result = RecordBatch::try_new(schema, columns)?;
    if result.get_array_memory_size() > budget.limits.max_batch_bytes {
        return Err(Error::refusal(
            "capacity",
            format!(
                "portable buffers use {} bytes, exceeding configured byte allowance {}",
                result.get_array_memory_size(),
                budget.limits.max_batch_bytes
            ),
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    mod containers;

    use super::*;
    use crate::budget::Limits;
    use arrow::{
        array::{Int64Array, ListArray, StringArray, StringViewArray, UInt32Array},
        datatypes::{Field, Int64Type},
    };

    fn budget() -> Budget {
        Budget::new(Limits {
            memory_bytes: 32 << 20,
            max_batch_bytes: 4096,
            batch_rows: 16,
            scratch_bytes: 16 << 20,
            ..Limits::default()
        })
        .unwrap()
    }
    fn batch(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap()
    }

    fn declared(data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("value", data_type, true)]))
    }

    #[test]
    fn native_binary_representation_preserves_declared_values() {
        use arrow::array::{BinaryArray, FixedSizeBinaryArray, LargeBinaryArray};
        let first = [7u8; 32];
        let last = [9u8; 32];
        let values = vec![Some(first.as_slice()), None, Some(last.as_slice())];
        let binary: ArrayRef = Arc::new(BinaryArray::from(values.clone()));
        let view = arrow::compute::cast(&binary, &DataType::BinaryView).unwrap();
        for array in [binary, view, Arc::new(LargeBinaryArray::from(values))] {
            let result = portable_batch(
                batch(array),
                declared(DataType::FixedSizeBinary(32)),
                &budget(),
            )
            .unwrap();
            let values = result
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.value(0), first);
            assert!(values.is_null(1));
            assert_eq!(values.value(2), last);
        }
    }

    #[test]
    fn native_nested_nullability_preserves_declared_values() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let nulls = Some(arrow::buffer::NullBuffer::from(vec![true, false, true]));
        let source = ListArray::new(
            Arc::new(Field::new("element", DataType::Utf8, true)),
            offsets.clone(),
            values.clone(),
            nulls.clone(),
        );
        let child = Arc::new(Field::new("item", DataType::Utf8, false));
        let expected = ListArray::new(child.clone(), offsets, values, nulls);
        let result = portable_batch(
            batch(Arc::new(source)),
            declared(DataType::List(child)),
            &budget(),
        )
        .unwrap();
        assert_eq!(result.column(0).to_data(), expected.to_data());
    }

    #[test]
    fn native_representation_conversion_rejects_invalid_values() {
        use arrow::array::{BinaryArray, FixedSizeBinaryArray};
        let wrong_width = batch(Arc::new(BinaryArray::from(vec![Some(&[1u8; 31][..])])));
        assert!(
            portable_batch(
                wrong_width,
                declared(DataType::FixedSizeBinary(32)),
                &budget()
            )
            .is_err()
        );
        let wrong_fixed = batch(Arc::new(
            FixedSizeBinaryArray::try_from_iter([&[1u8; 16][..]].into_iter()).unwrap(),
        ));
        assert!(
            portable_batch(
                wrong_fixed,
                declared(DataType::FixedSizeBinary(32)),
                &budget()
            )
            .is_err()
        );
        let child = Arc::new(Field::new("item", DataType::Int64, false));
        let source = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), None])]);
        assert!(
            portable_batch(
                batch(Arc::new(source)),
                declared(DataType::List(child)),
                &budget()
            )
            .is_err()
        );
        let unrelated = batch(Arc::new(StringArray::from(vec!["12"])));
        assert!(portable_batch(unrelated, declared(DataType::Int64), &budget()).is_err());
    }

    #[test]
    fn native_struct_field_names_cannot_enable_value_coercion() {
        let source = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["12"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![17])) as ArrayRef,
            ),
        ]);
        let target = DataType::Struct(
            vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("a", DataType::Int64, true),
            ]
            .into(),
        );
        // Native Arrow matches struct fields by name, so a broad cast can
        // change values even when positional types originally matched.
        let native = arrow::compute::cast(&source, &target).unwrap();
        assert_eq!(
            native
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            12
        );
        assert!(portable_batch(batch(Arc::new(source)), declared(target), &budget()).is_err());
    }

    #[test]
    fn binary_conversion_fits_target_bytes_before_admission() {
        use arrow::array::{BinaryArray, FixedSizeBinaryArray};
        let empty = batch(Arc::new(BinaryArray::from(vec![None::<&[u8]>; 128])));
        let output = PortableBatches::new(empty, declared(DataType::FixedSizeBinary(64)), budget())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(output.len() > 1);
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 128);
        for batch in output {
            assert!(batch.get_array_memory_size() <= 4096);
            assert_eq!(batch.column(0).null_count(), batch.num_rows());
        }
        let bytes = [5u8; 32];
        let fixed = batch(Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(bytes.as_slice(), 112))
                .unwrap(),
        ));
        let output = PortableBatches::new(fixed, declared(DataType::Binary), budget())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(output.len() > 1);
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 112);
        for batch in output {
            assert!(batch.get_array_memory_size() <= 4096);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            assert!(values.iter().all(|value| value == Some(bytes.as_slice())));
        }
        let large_nulls = arrow::compute::cast(
            &BinaryArray::from(vec![None::<&[u8]>; 128]),
            &DataType::FixedSizeBinary(64),
        )
        .unwrap();
        let output = PortableBatches::new(batch(large_nulls), declared(DataType::Binary), budget())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 128);
        assert_eq!(output[0].column(0).null_count(), 128);
        let oversized = batch(Arc::new(BinaryArray::from(vec![None::<&[u8]>])));
        let error = PortableBatches::new(
            oversized,
            declared(DataType::FixedSizeBinary(8192)),
            budget(),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code(), "capacity");
    }
    #[test]
    fn small_string_slice_compacts_unused_backing_bytes() {
        let values = StringArray::from(vec![
            "x".repeat(1 << 18),
            "kept".into(),
            "z".repeat(1 << 18),
        ]);
        let source = batch(Arc::new(values.slice(1, 1)));
        assert!(source.get_array_memory_size() > budget().limits.max_batch_bytes);
        let output = portable_batch(
            source.clone(),
            portable_schema(source.schema().as_ref()),
            &budget(),
        )
        .unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "kept"
        );
        assert!(output.get_array_memory_size() < 4096);
    }
    #[test]
    fn small_list_slice_compacts_unreferenced_child_values() {
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(7); 1 << 15]),
            Some(vec![Some(9)]),
            Some(vec![Some(8); 1 << 15]),
        ]);
        let source = batch(Arc::new(values.slice(1, 1)));
        let output = portable_batch(
            source.clone(),
            portable_schema(source.schema().as_ref()),
            &budget(),
        )
        .unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[9]
        );
        assert!(output.get_array_memory_size() < 4096);
    }
    #[test]
    fn logical_view_expansion_and_large_values_still_refuse() {
        let one = StringViewArray::from(vec!["v".repeat(1024)]);
        let repeated = arrow::compute::take(&one, &UInt32Array::from(vec![0; 16]), None).unwrap();
        for array in [
            repeated,
            Arc::new(StringArray::from(vec!["x".repeat(8192)])) as ArrayRef,
        ] {
            let source = batch(array);
            let error = portable_batch(
                source.clone(),
                portable_schema(source.schema().as_ref()),
                &budget(),
            )
            .unwrap_err();
            assert_eq!(error.code(), "capacity");
        }
    }
    #[test]
    fn moderate_backing_slack_compacts_when_total_exceeds_the_batch_limit() {
        let columns = (0..2)
            .map(|_| {
                let values = StringArray::from(vec!["p".repeat(900), "k".repeat(1500)]);
                Arc::new(values.slice(1, 1)) as ArrayRef
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let input = RecordBatch::try_new(schema.clone(), columns).unwrap();
        assert!(input.get_array_memory_size() > 4096);
        let output = portable_batch(input, schema, &budget()).unwrap();
        assert!(output.get_array_memory_size() < 4096);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .len(),
            1500
        );
    }
    #[test]
    fn short_views_cast_directly_with_constrained_remaining_admission() {
        let budget = budget();
        let _other_work = budget
            .reserve(budget.limits.memory_bytes / 2 - 6144)
            .unwrap();
        let values = [Some(""), None]
            .into_iter()
            .cycle()
            .take(500)
            .collect::<StringViewArray>();
        let input = batch(Arc::new(values));
        let output = portable_batch(
            input.clone(),
            portable_schema(input.schema().as_ref()),
            &budget,
        )
        .unwrap();
        assert_eq!(output.num_rows(), 500);
        assert_eq!(output.column(0).null_count(), 250);
    }
    #[test]
    fn nested_view_cast_trims_unreferenced_repeated_values_first() {
        let one = StringViewArray::from(vec!["v".repeat(512 << 10)]);
        let repeated = arrow::compute::take(&one, &UInt32Array::from(vec![0; 5]), None).unwrap();
        let values = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8View, true)),
            arrow::buffer::OffsetBuffer::new(vec![0, 1, 5].into()),
            repeated,
            None,
        );
        let input = batch(Arc::new(values.slice(0, 1)));
        let mut limits = budget().limits.clone();
        limits.max_batch_bytes = 2 << 20;
        let output = portable_batch(
            input.clone(),
            portable_schema(input.schema().as_ref()),
            &Budget::new(limits).unwrap(),
        )
        .unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.values().len(), 1);
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .len(),
            512 << 10
        );
    }
}
