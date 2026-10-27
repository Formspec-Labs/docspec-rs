//! Normalize Arrow view representations before retaining or encoding batches.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::{
    array::{
        Array, ArrayRef, BinaryViewArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
        StringViewArray, StructArray,
    },
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use std::sync::Arc;

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
            if column.data_type() == field.data_type() {
                Ok(column)
            } else {
                Ok(arrow::compute::cast(&column, field.data_type())?)
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let result = RecordBatch::try_new(schema, columns)?;
    if result.get_array_memory_size() > budget.limits.max_batch_bytes {
        return Err(Error::refusal(
            "capacity",
            "portable buffers exceed configured byte allowance",
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
