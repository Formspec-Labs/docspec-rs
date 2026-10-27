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

fn portable_bytes(array: &dyn Array) -> usize {
    // Views can repeat one backing value many times. Count logical bytes before
    // casting, so a small shared buffer cannot expand past batch admission.
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return a
            .total_bytes_len()
            .saturating_add((a.len() + 1).saturating_mul(4))
            .saturating_add(a.len().div_ceil(8) + 256);
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return a
            .total_bytes_len()
            .saturating_add((a.len() + 1).saturating_mul(4))
            .saturating_add(a.len().div_ceil(8) + 256);
    }
    if let Some(a) = array.as_any().downcast_ref::<StructArray>() {
        return a
            .columns()
            .iter()
            .map(|c| portable_bytes(c.as_ref()))
            .fold(a.len().div_ceil(8) + 256, usize::saturating_add);
    }
    if let Some(a) = array.as_any().downcast_ref::<ListArray>() {
        return portable_bytes(a.values().as_ref())
            .saturating_add((a.len() + 1).saturating_mul(4) + 256);
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeListArray>() {
        return portable_bytes(a.values().as_ref())
            .saturating_add((a.len() + 1).saturating_mul(8) + 256);
    }
    if let Some(a) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return portable_bytes(a.values().as_ref()).saturating_add(256);
    }
    if let Some(a) = array.as_any().downcast_ref::<MapArray>() {
        return portable_bytes(a.entries()).saturating_add((a.len() + 1).saturating_mul(4) + 256);
    }
    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let data = array.to_data();
        let values = arrow::array::make_array(data.child_data()[0].clone());
        return array
            .get_array_memory_size()
            .saturating_add(portable_bytes(values.as_ref()));
    }
    array.get_array_memory_size()
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
    let mut changed = false;
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        bytes = bytes.saturating_add(portable_bytes(column.as_ref()));
        changed |= column.data_type() != field.data_type();
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
    let _conversion = if changed {
        Some(budget.reserve(bytes.saturating_mul(2))?)
    } else {
        None
    };
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                Ok(arrow::compute::cast(column, field.data_type())?)
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::{Int64Array, ListArray, StringArray, StringViewArray, UInt32Array}, datatypes::{Field, Int64Type}};
    use crate::budget::Limits;

    fn budget() -> Budget {
        Budget::new(Limits { memory_bytes: 32 << 20, max_batch_bytes: 4096,
            batch_rows: 16, scratch_bytes: 16 << 20, ..Limits::default() }).unwrap()
    }
    fn batch(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("value", array.data_type().clone(), true)])),vec![array]).unwrap()
    }
    #[test]
    fn small_string_slice_compacts_unused_backing_bytes() {
        let values = StringArray::from(vec!["x".repeat(1 << 18), "kept".into(), "z".repeat(1 << 18)]);
        let source = batch(Arc::new(values.slice(1,1)));
        assert!(source.get_array_memory_size() > budget().limits.max_batch_bytes);
        let output = portable_batch(source.clone(), portable_schema(source.schema().as_ref()), &budget()).unwrap();
        assert_eq!(output.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "kept");
        assert!(output.get_array_memory_size() < 4096);
    }
    #[test]
    fn small_list_slice_compacts_unreferenced_child_values() {
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(7);1 << 15]),Some(vec![Some(9)]),Some(vec![Some(8);1 << 15])]);
        let source = batch(Arc::new(values.slice(1,1)));
        let output = portable_batch(source.clone(), portable_schema(source.schema().as_ref()), &budget()).unwrap();
        let list = output.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.values().as_any().downcast_ref::<Int64Array>().unwrap().values(), &[9]);
        assert!(output.get_array_memory_size() < 4096);
    }
    #[test]
    fn logical_view_expansion_and_large_values_still_refuse() {
        let one = StringViewArray::from(vec!["v".repeat(1024)]);
        let repeated = arrow::compute::take(&one, &UInt32Array::from(vec![0;16]), None).unwrap();
        for array in [repeated, Arc::new(StringArray::from(vec!["x".repeat(8192)])) as ArrayRef] {
            let source=batch(array);
            let error=portable_batch(source.clone(),portable_schema(source.schema().as_ref()),&budget()).unwrap_err();
            assert_eq!(error.code(),"capacity");
        }
    }
}
