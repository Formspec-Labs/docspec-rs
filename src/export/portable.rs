//! Normalize Arrow view representations before retaining or encoding batches.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{
    array::{
        Array, ArrayRef, BinaryViewArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
        StringViewArray, StructArray,
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

impl PortableBatches {
    fn new(batch: RecordBatch, schema: SchemaRef, budget: Budget) -> Result<Self> {
        let oversized =
            batch_bytes(&batch)? > budget.limits.max_batch_bytes && batch.num_rows() > 1;
        let base = batch_bytes(&RecordBatch::new_empty(batch.schema()))?;
        Ok(Self {
            batch,
            schema,
            budget,
            base,
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
        Ok(batch_bytes(&self.batch.slice(row, 1))?.saturating_sub(self.base))
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

fn portable_bytes(array: &dyn Array, view_storage: bool) -> Result<usize> {
    // Count logical view values, including repeated references to one buffer.
    let overhead = array.len().div_ceil(8).saturating_add(256);
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
    if let Some(a) = array.as_any().downcast_ref::<StructArray>() {
        return a.columns().iter().try_fold(overhead, |bytes, child| {
            Ok(bytes.saturating_add(portable_bytes(child.as_ref(), view_storage)?))
        });
    }
    if let Some(a) = array.as_any().downcast_ref::<ListArray>() {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(
            portable_bytes(a.values().slice(start, end - start).as_ref(), view_storage)?
                .saturating_add((a.len() + 1).saturating_mul(4))
                .saturating_add(overhead),
        );
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeListArray>() {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(
            portable_bytes(a.values().slice(start, end - start).as_ref(), view_storage)?
                .saturating_add((a.len() + 1).saturating_mul(8))
                .saturating_add(overhead),
        );
    }
    if let Some(a) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return Ok(portable_bytes(a.values().as_ref(), view_storage)?.saturating_add(overhead));
    }
    if let Some(a) = array.as_any().downcast_ref::<MapArray>() {
        let offsets = a.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[a.len()] as usize;
        return Ok(
            portable_bytes(&a.entries().slice(start, end - start), view_storage)?
                .saturating_add((a.len() + 1).saturating_mul(4))
                .saturating_add(overhead),
        );
    }
    if let DataType::Dictionary(key, _) = array.data_type() {
        let data = array.to_data();
        let values = arrow::array::make_array(data.child_data()[0].clone());
        let width = key
            .primitive_width()
            .ok_or_else(|| Error::refusal("invalid_export", "dictionary key is not fixed width"))?;
        return Ok(portable_bytes(values.as_ref(), view_storage)?
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

fn dirty_view_bytes(array: &dyn Array) -> usize {
    let dirty = |views: &[u128]| {
        if views
            .iter()
            .enumerate()
            .any(|(index, view)| array.is_null(index) && *view != 0)
        {
            views.len().saturating_mul(16)
        } else {
            0
        }
    };
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return dirty(array.views());
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return dirty(array.views());
    }
    array
        .to_data()
        .child_data()
        .iter()
        .fold(0usize, |bytes, child| {
            bytes.saturating_add(dirty_view_bytes(
                arrow::array::make_array(child.clone()).as_ref(),
            ))
        })
}

fn clean_null_views(array: ArrayRef) -> Result<ArrayRef> {
    // Arrow58's view cast reserves using every header, including null rows.
    // Clear only ignored headers before that maintained cast, without copying
    // source payloads or making their unknown bytes meaningful.
    macro_rules! clean {
        ($kind:ty) => {
            if let Some(view) = array.as_any().downcast_ref::<$kind>() {
                let views = view
                    .views()
                    .iter()
                    .enumerate()
                    .map(|(index, value)| if view.is_null(index) { 0 } else { *value })
                    .collect::<Vec<_>>();
                return Ok(Arc::new(<$kind>::try_new(
                    views.into(),
                    view.data_buffers().to_vec(),
                    view.nulls().cloned(),
                )?));
            }
        };
    }
    if dirty_view_bytes(array.as_ref()) == 0 {
        return Ok(array);
    }
    clean!(StringViewArray);
    clean!(BinaryViewArray);
    let data = array.to_data();
    let children = data
        .child_data()
        .iter()
        .map(|child| Ok(clean_null_views(arrow::array::make_array(child.clone()))?.to_data()))
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow::array::make_array(
        data.into_builder().child_data(children).build()?,
    ))
}

pub(crate) fn portable_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    budget: &Budget,
) -> Result<RecordBatch> {
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
    if batch.num_columns() != schema.fields().len() {
        return Err(Error::refusal(
            "invalid_export",
            "executed batch column count changed",
        ));
    }
    let mut bytes = 0usize;
    let mut conversion_bytes = 0usize;
    let over_limit = batch.get_array_memory_size() > budget.limits.max_batch_bytes;
    let mut compact_columns = Vec::with_capacity(batch.num_columns());
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        let logical = portable_bytes(column.as_ref(), false)?;
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
            conversion_bytes = conversion_bytes
                .saturating_add(dirty_view_bytes(column.as_ref()).saturating_mul(2));
        }
        if needs_compaction {
            conversion_bytes = conversion_bytes
                .saturating_add(portable_bytes(column.as_ref(), true)?.saturating_mul(2));
        }
        compact_columns.push(needs_compaction);
        bytes = bytes.saturating_add(logical);
        if !portable_data_type(column.data_type()).equals_datatype(field.data_type()) {
            return Err(Error::refusal(
                "invalid_export",
                "executed batch type changed",
            ));
        }
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
            let mut column = if column.data_type() == field.data_type() {
                column
            } else {
                arrow::compute::cast(&clean_null_views(column)?, field.data_type())?
            };
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
