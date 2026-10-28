//! Admission counts every referenced element, including one-row large lists.
// Rust guideline compliant 2026-02-21
use super::*;

#[test]
fn inline_view_compaction_releases_sliced_headers_and_null_bitmap() {
    use arrow::{
        array::{Array, StringViewArray},
        buffer::NullBuffer,
    };
    let source = StringViewArray::from_iter_values(
        std::iter::once("x".repeat(2 << 20)).chain(std::iter::repeat_n("inline".into(), 200_000)),
    );
    let values = Arc::new(
        StringViewArray::try_new(
            source.views().clone(),
            source.data_buffers().to_vec(),
            Some(NullBuffer::new_valid(source.len())),
        )
        .unwrap(),
    );
    drop(source);
    let headers = values.views().inner().clone();
    let validity = values.nulls().unwrap().inner().inner().clone();
    let batch = RecordBatch::try_from_iter([("value", values.clone() as ArrayRef)]).unwrap();
    let budget = crate::budget::Budget::new(crate::budget::Limits::default()).unwrap();
    let compacted = compact_views(batch.slice(1, 3), &budget).unwrap();
    drop(batch);
    drop(values);
    assert_eq!(
        headers.strong_count(),
        1,
        "compaction must release original header allocation"
    );
    assert_eq!(
        validity.strong_count(),
        1,
        "compaction must release original validity allocation"
    );
    let values = compacted
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some("inline"); 3]);
}

#[test]
fn compacted_view_slices_release_unused_payload_and_keep_admission() {
    use arrow::array::{Array, StringViewArray};
    let unused = "x".repeat(2 << 20);
    let source = Arc::new(StringViewArray::from(vec![
        Some(unused.as_str()),
        Some("first retained long value"),
        None,
        Some("second retained long value"),
    ]));
    let batch = RecordBatch::try_from_iter([("value", source as ArrayRef)]).unwrap();
    let budget = crate::budget::Budget::new(crate::budget::Limits {
        memory_bytes: 64 << 20,
        max_batch_bytes: 1 << 20,
        ..crate::budget::Limits::default()
    })
    .unwrap();
    let compacted = compact_batch_slice(&batch, 1, 3, &budget).unwrap();
    let view = compacted
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(
        view.iter().collect::<Vec<_>>(),
        vec![
            Some("first retained long value"),
            None,
            Some("second retained long value")
        ]
    );
    assert!(
        view.data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>()
            < 1024
    );
    assert_eq!(compacted.schema(), batch.schema());
    let kept = compacted.column(0).to_data();
    drop(compacted);
    assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
    drop(kept);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}

#[test]
fn compacting_views_preserves_sharing_when_logical_bytes_are_large() {
    use arrow::array::{Array, StringViewArray, UInt32Array};
    let text = "x".repeat(1 << 20);
    let source = StringViewArray::from(vec![Some(text), None]);
    let indices = UInt32Array::from(
        (0..2048)
            .map(|row| if row % 17 == 0 { 1 } else { 0 })
            .collect::<Vec<_>>(),
    );
    let values = arrow::compute::take(&source, &indices, None).unwrap();
    let batch = RecordBatch::try_from_iter([("value", values)]).unwrap();
    let budget = crate::budget::Budget::new(crate::budget::Limits {
        memory_bytes: 64 << 20,
        max_batch_bytes: 4 << 20,
        ..crate::budget::Limits::default()
    })
    .unwrap();
    let compacted = compact_batch_slice(&batch, 0, 2048, &budget).unwrap();
    let view = compacted
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(view.len(), 2048);
    assert_eq!(
        view.null_count(),
        (0..2048).filter(|row| row % 17 == 0).count()
    );
    assert_eq!(view.value(1).len(), 1 << 20);
    assert!(view.get_array_memory_size() < 2 << 20);
}

#[test]
fn view_compaction_ignores_large_headers_in_null_slots() {
    use arrow::{
        array::{Array, StringViewArray},
        buffer::NullBuffer,
    };
    let source = StringViewArray::from(vec!["x".repeat(2 << 20), "retained noninline text".into()]);
    let array = Arc::new(
        StringViewArray::try_new(
            vec![source.views()[0], source.views()[1]].into(),
            source.data_buffers().to_vec(),
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_from_iter([("value", array as ArrayRef)]).unwrap();
    let budget = crate::budget::Budget::new(crate::budget::Limits {
        memory_bytes: 64 << 20,
        max_batch_bytes: 4 << 20,
        ..crate::budget::Limits::default()
    })
    .unwrap();
    let compacted = compact_batch_slice(&batch, 0, 2, &budget).unwrap();
    let array = compacted
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert!(array.is_null(0));
    assert_eq!(array.value(1), "retained noninline text");
    assert!(
        array
            .data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>()
            < 1024
    );
}

#[test]
fn compaction_releases_unused_payload_inside_list_values() {
    use arrow::{
        array::{Array, ListArray, StringViewArray},
        buffer::OffsetBuffer,
    };
    let values = StringViewArray::from(vec![
        "x".repeat(2 << 20),
        "retained nested long string".into(),
    ]);
    let values = Arc::new(values.slice(1, 1));
    let list = ListArray::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Utf8View,
            true,
        )),
        OffsetBuffer::new(vec![0i32, 1].into()),
        values,
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter([("items", Arc::new(list) as ArrayRef)]).unwrap();
    let budget = crate::budget::Budget::new(crate::budget::Limits::default()).unwrap();
    let compacted = compact_batch_slice(&batch, 0, 1, &budget).unwrap();
    assert!(compacted.get_array_memory_size() < 4096);
    assert_eq!(compacted.schema(), batch.schema());
    let list = compacted
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(values.value(0), "retained nested long string");
}

#[test]
fn native_alignment_reorders_columns_without_copying_view_buffers() {
    use arrow::array::{Date32Array, StringViewArray};
    let first = Arc::new(StringViewArray::from(vec![
        "first string in its own buffer",
    ]));
    let second = Arc::new(StringViewArray::from(vec![
        "second string with distinct values",
    ]));
    let day = Arc::new(Date32Array::from(vec![20753]));
    let source = RecordBatch::try_from_iter([
        ("second", second.clone() as ArrayRef),
        ("day", day.clone() as ArrayRef),
        ("first", first.clone() as ArrayRef),
    ])
    .unwrap();
    let target = Schema::new(vec![
        Field::new("first", DataType::Utf8, false)
            .with_metadata([("PARQUET:field_id".into(), "17".into())].into()),
        Field::new("second", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
    ]);
    let budget = crate::budget::Budget::new(crate::budget::Limits::default()).unwrap();
    let (aligned, _) = align_native_batch(source, &target, &budget).unwrap();
    assert_eq!(
        aligned
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second", "day"]
    );
    assert_eq!(
        aligned.schema().field(0).metadata()["PARQUET:field_id"],
        "17"
    );
    for (index, original) in [first as ArrayRef, second as ArrayRef, day as ArrayRef]
        .iter()
        .enumerate()
    {
        assert_eq!(aligned.column(index).to_data(), original.to_data());
        assert_eq!(
            aligned.column(index).to_data().buffers()[0].as_ptr(),
            original.to_data().buffers()[0].as_ptr()
        );
    }
}

#[test]
fn native_alignment_refuses_incomplete_or_ambiguous_column_names() {
    let budget = crate::budget::Budget::new(crate::budget::Limits::default()).unwrap();
    let target = Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    for names in [vec!["a"], vec!["a", "foreign"], vec!["a", "a"]] {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(
                names
                    .iter()
                    .map(|name| Field::new(*name, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )),
            names
                .iter()
                .map(|_| Arc::new(StringArray::from(vec!["value"])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        assert_eq!(
            align_native_batch(source, &target, &budget)
                .unwrap_err()
                .code(),
            "schema_mismatch"
        );
    }
}

#[test]
fn native_alignment_preserves_view_storage_and_nested_field_identity() {
    use arrow::array::{Array, BinaryViewArray, ListArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let text = Arc::new(StringViewArray::from(vec![
        "a long string requiring a backing buffer",
    ]));
    let binary = Arc::new(BinaryViewArray::from(vec![
        b"a long binary backing buffer".as_slice(),
    ]));
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8View, true)),
        arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into()),
        text.clone(),
        None,
    ));
    let source = RecordBatch::try_from_iter([
        ("text", text.clone() as Arc<dyn Array>),
        ("binary", binary as Arc<dyn Array>),
        ("list", list as Arc<dyn Array>),
    ])
    .unwrap();
    let child = Field::new("element", DataType::Utf8, false)
        .with_metadata([("PARQUET:field_id".into(), "41".into())].into());
    let target = Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("binary", DataType::Binary, false),
        Field::new("list", DataType::List(Arc::new(child)), false),
    ]);
    let budget = crate::budget::Budget::new(crate::budget::Limits::default()).unwrap();
    let (aligned, _) = align_native_batch(source, &target, &budget).unwrap();
    assert_eq!(aligned.column(0).data_type(), &DataType::Utf8View);
    assert_eq!(aligned.column(1).data_type(), &DataType::BinaryView);
    assert_eq!(
        aligned.column(0).to_data().buffers()[0].as_ptr(),
        text.to_data().buffers()[0].as_ptr()
    );
    let DataType::List(child) = aligned.schema().field(2).data_type().clone() else {
        panic!("list required")
    };
    assert_eq!(child.data_type(), &DataType::Utf8View);
    assert_eq!(child.name(), "element");
    assert!(!child.is_nullable());
    assert_eq!(child.metadata()["PARQUET:field_id"], "41");
}
use arrow::{
    array::{Array, ArrayRef, ListArray, StringArray, StructArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Schema},
};
#[test]
fn many_struct_elements_are_admitted_before_compaction_and_cast() {
    let value = "x".repeat(1024);
    let child = Arc::new(Field::new("value", DataType::Utf8, true));
    let structure = StructArray::new(
        vec![child.clone()].into(),
        vec![Arc::new(StringArray::from(vec![value.as_str(); 10000]))],
        None,
    );
    let item = Arc::new(Field::new("item", structure.data_type().clone(), true));
    let list = ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 10000])),
        Arc::new(structure),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "values",
            list.data_type().clone(),
            true,
        )])),
        vec![Arc::new(list) as ArrayRef],
    )
    .unwrap();
    let target = Schema::new(vec![Field::new(
        "values",
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(vec![Field::new("value", DataType::Utf8, false)].into()),
            false,
        ))),
        true,
    )]);
    let budget = crate::budget::Budget::new(crate::budget::Limits {
        compute_threads: 1,
        memory_bytes: 32 << 20,
        max_batch_bytes: 64 << 20,
        ..Default::default()
    })
    .unwrap();
    let result = align_native_batch(batch, &target, &budget);
    assert!(
        result.is_err(),
        "large nested copy was admitted beyond its pool"
    );
    let error = result.expect_err("checked refusal");
    assert_eq!(error.code(), "capacity");
    // Refusal must release every partial reservation.
    assert!(budget.reserve(16 << 20).is_ok());
}
