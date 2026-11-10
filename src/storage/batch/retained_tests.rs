//! A retained slice is measured by the rows it references, nested columns included.
// Rust guideline compliant 2026-02-21
use super::*;
use arrow::{
    array::{
        Array, ArrayRef, FixedSizeListArray, Int64Array, LargeListArray, ListArray, MapArray,
        OffsetSizeTrait, StringArray, StringViewArray, StructArray,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Fields},
};
use datafusion::common::utils::memory::get_record_batch_memory_size;

/// Rows in the parent batch; a join emits its unmatched build rows this way.
const ROWS: usize = 100_000;
/// DataFusion's default output batch, cut from the parent without copying.
const SLICE: usize = 8192;
/// Each row holds two items of this many bytes.
const ITEM: usize = 150;

const NESTED: [&str; 6] = [
    "list",
    "large_list",
    "fixed_size_list",
    "struct_of_list",
    "map",
    "list_of_views",
];

fn text(items: usize, width: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..items).map(|item| format!("{item:0width$}")),
    ))
}

fn pairs<O: OffsetSizeTrait>() -> OffsetBuffer<O> {
    OffsetBuffer::from_lengths(std::iter::repeat_n(2, ROWS))
}

fn item_field(values: &ArrayRef) -> Arc<Field> {
    Arc::new(Field::new("item", values.data_type().clone(), false))
}

fn list(values: ArrayRef) -> ArrayRef {
    Arc::new(ListArray::new(item_field(&values), pairs(), values, None))
}

/// A nested column of `ROWS` rows, each referencing `2 * ITEM` payload bytes.
fn nested(name: &str) -> ArrayRef {
    let items = text(2 * ROWS, ITEM);
    match name {
        "list" => list(items),
        "large_list" => Arc::new(LargeListArray::new(
            item_field(&items),
            pairs(),
            items,
            None,
        )),
        "fixed_size_list" => Arc::new(FixedSizeListArray::new(item_field(&items), 2, items, None)),
        "struct_of_list" => {
            let inner = list(items);
            Arc::new(StructArray::new(
                Fields::from(vec![Field::new("inner", inner.data_type().clone(), false)]),
                vec![inner],
                None,
            ))
        }
        "map" => {
            // One entry a row; short keys leave the payload in the values.
            let entries = StructArray::new(
                Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, false),
                ]),
                vec![text(ROWS, 8), text(ROWS, 2 * ITEM)],
                None,
            );
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, ROWS)),
                entries,
                None,
                false,
            ))
        }
        "list_of_views" => list(Arc::new(StringViewArray::from_iter_values(
            (0..2 * ROWS).map(|item| format!("{item:0ITEM$}")),
        ))),
        other => unreachable!("{other}"),
    }
}

#[test]
fn retained_slice_of_nested_column_is_measured_and_copied_by_referenced_rows() {
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let mut oversized = Vec::new();
    for name in NESTED {
        let keys = Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef;
        let parent = RecordBatch::try_from_iter([("k", keys), ("nested", nested(name))]).unwrap();
        // An interior slice: its offsets do not start at the child's first row.
        let slice = parent.slice(3 * SLICE, SLICE);
        drop(parent);
        let charged = get_record_batch_memory_size(&slice);
        // Arrow's own copy of these rows is the independent size to compare with.
        let owned =
            get_record_batch_memory_size(&compact_batch_slice(&slice, 0, SLICE, &budget).unwrap());
        let payload = SLICE * 2 * ITEM;
        assert!(
            payload <= owned && 4 * owned < charged,
            "{name}: fixture must retain far more than its rows: \
             payload {payload}, owned {owned}, charged {charged}"
        );
        let referenced = sliced_batch_bytes(&slice);
        let kept = compact_retained(slice.clone(), &budget).unwrap();
        let kept_charge = get_record_batch_memory_size(&kept);
        assert_eq!(kept, slice, "{name}: compaction must keep every value");
        if referenced < payload || referenced > 2 * owned || kept_charge > 2 * owned {
            oversized.push(format!(
                "{name}: referenced {referenced}, kept charge {kept_charge}, \
                 owned copy {owned}, payload {payload}, slice charge {charged}"
            ));
        }
    }
    assert!(
        oversized.is_empty(),
        "slices measured or kept at their parent's size:\n{}",
        oversized.join("\n")
    );
}
