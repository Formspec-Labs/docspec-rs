//! Admission counts every referenced element, including one-row large lists.
// Rust guideline compliant 2026-02-21
use super::*;
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
    assert!(result.is_err(), "large nested copy was admitted beyond its pool");
    let error = result.err().expect("checked refusal");
    assert_eq!(error.code(), "capacity");
    // Refusal must release every partial reservation.
    assert!(budget.reserve(16 << 20).is_ok());
}
