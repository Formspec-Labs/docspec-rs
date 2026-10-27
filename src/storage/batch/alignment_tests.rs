//! Admission counts every referenced element, including one-row large lists.
// Rust guideline compliant 2026-02-21
use super::*;

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
