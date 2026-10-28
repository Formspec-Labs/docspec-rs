//! Exercise declared child types through every supported recursive container.
use super::*;
use arrow::{
    array::{BinaryArray, DictionaryArray, FixedSizeBinaryArray, Int8Array, MapArray},
    buffer::OffsetBuffer,
    datatypes::Int8Type,
};
use std::collections::HashMap;

fn wrap(kind: usize, values: ArrayRef, declared: bool, rows: Option<usize>) -> ArrayRef {
    let metadata = HashMap::from([(
        "leaf".into(),
        if declared { "declared" } else { "source" }.into(),
    )]);
    let field = Arc::new(
        Field::new(
            if declared { "item" } else { "element" },
            values.data_type().clone(),
            true,
        )
        .with_metadata(metadata.clone()),
    );
    let len = values.len();
    match kind {
        0 => Arc::new(LargeListArray::new(
            field,
            OffsetBuffer::new((0..=len as i64).collect::<Vec<_>>().into()),
            values,
            None,
        )),
        1 => Arc::new(FixedSizeListArray::new(field, 1, values, None)),
        2 => {
            let keys: ArrayRef = Arc::new(StringArray::from(vec!["key"; len]));
            let fields = vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", values.data_type().clone(), true).with_metadata(metadata),
            ]
            .into();
            let entries = StructArray::new(fields, vec![keys, values], None);
            let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
            Arc::new(MapArray::new(
                field,
                OffsetBuffer::new((0..=len as i32).collect::<Vec<_>>().into()),
                entries,
                None,
                false,
            ))
        }
        3 => {
            let keys = if let Some(rows) = rows {
                Int8Array::from_iter((0..rows).map(|i| (i % 17 != 0).then_some((i % len) as i8)))
            } else {
                Int8Array::from_iter((0..=len).map(|i| (i < len).then_some(i as i8)))
            };
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap())
        }
        _ => unreachable!(),
    }
}

fn schema(array: &ArrayRef) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("value", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("root".into(), "declared".into())])),
    ]))
}

#[test]
fn recursive_declared_binary_types_keep_values_nulls_and_metadata() {
    let a = [7u8; 32];
    let b = [9u8; 32];
    for kind in 0..4 {
        let source: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(a.as_slice()),
            None,
            Some(b.as_slice()),
        ]));
        let expected: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(a.as_slice()), None, Some(b.as_slice())].into_iter(),
                32,
            )
            .unwrap(),
        );
        let source = wrap(kind, source, false, None);
        let expected = wrap(kind, expected, true, None);
        let target = schema(&expected);
        assert!(
            portable_bytes_as(source.as_ref(), false, expected.data_type()).unwrap()
                >= expected.get_array_memory_size()
        );
        let result = portable_batch(batch(source), target.clone(), &budget()).unwrap();
        assert_eq!(result.schema(), target);
        assert_eq!(result.column(0).data_type(), expected.data_type());
        let (actual, expected) = if kind == 3 {
            (
                arrow::compute::cast(result.column(0), &DataType::FixedSizeBinary(32)).unwrap(),
                arrow::compute::cast(&expected, &DataType::FixedSizeBinary(32)).unwrap(),
            )
        } else {
            (result.column(0).clone(), expected)
        };
        assert_eq!(actual.to_data(), expected.to_data());
        let invalid: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[1u8; 31][..]),
            None,
            Some(b.as_slice()),
        ]));
        assert!(
            portable_batch(batch(wrap(kind, invalid, false, None)), target, &budget()).is_err()
        );
    }
}

#[test]
fn recursive_sizing_counts_fixed_bytes_beneath_nulls() {
    for kind in 0..4 {
        let count = if kind == 3 { 24 } else { 96 };
        let rows = (kind == 3).then_some(1024);
        let source: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>; count]));
        let expected: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                std::iter::repeat_n(None::<&[u8]>, count),
                64,
            )
            .unwrap(),
        );
        let source = wrap(kind, source, false, rows);
        let expected = wrap(kind, expected, true, rows);
        assert!(
            portable_bytes_as(source.as_ref(), false, expected.data_type()).unwrap()
                >= expected.get_array_memory_size()
        );
        let expected_rows = source.len();
        let batches = PortableBatches::new(batch(source), schema(&expected), budget())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows
        );
        if kind != 3 {
            assert!(batches.len() > 1);
        }
        for batch in batches {
            assert!(batch.get_array_memory_size() <= budget().limits.max_batch_bytes);
            assert_eq!(batch.column(0).data_type(), expected.data_type());
        }
    }
}
