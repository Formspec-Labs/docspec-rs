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
    Arc::new(Field::new(
        "item",
        values.data_type().clone(),
        values.null_count() > 0,
    ))
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

/// Whether `kept` holds the very arrays of `given`, with nothing copied.
fn same_buffers(kept: &RecordBatch, given: &RecordBatch) -> bool {
    kept.columns()
        .iter()
        .zip(given.columns())
        .all(|(kept, given)| Arc::ptr_eq(kept, given))
}

/// Columns of `ROWS` short values, where a view costs most against its text.
fn short_columns() -> Vec<(&'static str, ArrayRef)> {
    let views = |value: fn(usize) -> Option<String>| {
        Arc::new((0..2 * ROWS).map(value).collect::<StringViewArray>()) as ArrayRef
    };
    let rows = |column: ArrayRef| column.slice(0, ROWS);
    vec![
        ("text", text(ROWS, 8)),
        ("empty_views", rows(views(|_| Some(String::new())))),
        ("null_views", rows(views(|_| None))),
        // Twelve bytes is the longest value a view holds inline.
        (
            "inline_views",
            rows(views(|item| Some(format!("{item:012}")))),
        ),
        // Its own pool of strings. A take of half of a pool's rows uses under
        // half of the pool's block capacity, which view compaction copies.
        (
            "long_views",
            Arc::new(StringViewArray::from_iter_values(
                (0..ROWS).map(|item| format!("{item:013}")),
            )) as ArrayRef,
        ),
        ("list", list(text(2 * ROWS, 8))),
        ("list_of_empty_views", list(views(|_| Some(String::new())))),
        ("list_of_null_views", list(views(|_| None))),
    ]
}

/// Arrow's own copy and a join's `take` size their view buffers exactly, so
/// this control also passes under the flattened-text measure fb50a24
/// replaced: there an exact view column sits just under four times its text
/// size. fb50a24's message says the control was not run against that measure;
/// it was, and passed. The layout that separates the two measures is in
/// `views_a_builder_left_room_in_are_not_copied`.
#[test]
fn compact_batch_is_returned_with_the_buffers_it_arrived_with() {
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let columns = short_columns();
    let keys = Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef;
    let together =
        RecordBatch::try_from_iter([("k", keys)].into_iter().chain(columns.clone())).unwrap();
    let batches = columns
        .into_iter()
        .map(|(name, column)| (name, RecordBatch::try_from_iter([(name, column)]).unwrap()))
        .chain([("together", together)]);
    let every_row = arrow::array::UInt32Array::from_iter_values(0..ROWS as u32);
    let mut copied = Vec::new();
    for (name, batch) in batches {
        // The two ways the engine receives compact batches: Arrow's own copy,
        // which is what the guard returns, and a join's `take`.
        let compact = [
            (
                "copy",
                compact_batch_slice(&batch, 0, ROWS, &budget).unwrap(),
            ),
            (
                "take",
                arrow::compute::take_record_batch(&batch, &every_row).unwrap(),
            ),
        ];
        for (source, given) in compact {
            let charged = get_record_batch_memory_size(&given);
            let referenced = sliced_batch_bytes(&given);
            // Large enough that only the ratio keeps the guard silent.
            assert!(charged > 1 << 20, "{name} {source}: {charged}");
            let kept = compact_retained(given.clone(), &budget).unwrap();
            if !same_buffers(&kept, &given) {
                copied.push(format!(
                    "{name} {source}: charged {charged}, referenced {referenced}"
                ));
            }
        }
    }
    assert!(
        copied.is_empty(),
        "compact batches were copied:\n{}",
        copied.join("\n")
    );
}

/// Views a builder grew by doubling: 140,000 rows in 262,144 slots.
const GROWN: usize = 140_000;
/// Views per row of the list and fixed-size-list columns.
const PER_ROW: usize = 8;

/// Empty and null strings as a builder leaves them, alone and nested.
///
/// Builders, Parquet's view decoder and DataFusion's group values grow their
/// view vector by doubling and keep the spare slots, which DataFusion charges.
fn grown_columns() -> Vec<(&'static str, ArrayRef)> {
    let views = || {
        let mut builder = arrow::array::StringViewBuilder::new();
        for row in 0..GROWN {
            if row % 2 == 0 {
                builder.append_value("");
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    };
    let lists = OffsetBuffer::from_lengths(std::iter::repeat_n(PER_ROW, GROWN / PER_ROW));
    let item = |values: &ArrayRef| Arc::new(Field::new("item", values.data_type().clone(), true));
    let (list_items, fixed_items) = (views(), views());
    vec![
        ("views", views()),
        ("struct_of_views", {
            let field = Field::new("text", DataType::Utf8View, true);
            Arc::new(StructArray::new(
                Fields::from(vec![field]),
                vec![views()],
                None,
            ))
        }),
        (
            "list_of_views",
            Arc::new(ListArray::new(item(&list_items), lists, list_items, None)),
        ),
        (
            "fixed_size_list_of_views",
            Arc::new(FixedSizeListArray::new(
                item(&fixed_items),
                PER_ROW as i32,
                fixed_items,
                None,
            )),
        ),
    ]
}

/// fb50a24's reason, as a failing case for the measure it replaced.
///
/// The guard copies a batch charged at more than four times a compact copy.
/// The export measure it replaced sized each view as flattened text, 4.1
/// bytes for an empty or null value against the 16 a copy holds, so these
/// columns, charged under two times Arrow's own copy, looked about seven
/// times oversized and were copied. Measured as views they are kept.
#[test]
fn views_a_builder_left_room_in_are_not_copied() {
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let mut copied = Vec::new();
    for (name, column) in grown_columns() {
        let given = RecordBatch::try_from_iter([(name, column)]).unwrap();
        let charged = get_record_batch_memory_size(&given);
        let owned = get_record_batch_memory_size(
            &compact_batch_slice(&given, 0, given.num_rows(), &budget).unwrap(),
        );
        // Under the 4x contract, and past the guard's 1 MiB floor, so only
        // the measure decides.
        assert!(
            charged < 4 * owned && charged > owned + (1 << 20),
            "{name}: charged {charged}, Arrow's own copy {owned}"
        );
        let kept = compact_retained(given.clone(), &budget).unwrap();
        if !same_buffers(&kept, &given) {
            copied.push(format!(
                "{name}: charged {charged}, own copy {owned}, measured {}",
                sliced_batch_bytes(&given)
            ));
        }
    }
    assert!(
        copied.is_empty(),
        "batches under four times their own copy were copied:\n{}",
        copied.join("\n")
    );
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
        // A second guard on the same path measures the copy and leaves it alone.
        assert!(
            same_buffers(&compact_retained(kept.clone(), &budget).unwrap(), &kept),
            "{name}: a compacted slice must not be copied again"
        );
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

/// Views Arrow's output coalescer copied into blocks sized for its whole input.
///
/// When a pushed view array uses under half of its buffers, arrow-select
/// 58.4's `BatchCoalescer` copies its strings, allocating each output batch a
/// block sized for every row it was pushed (coalesce/byte_view.rs:341, :162).
/// An 8,192-row batch cut from 30,000 rows then holds 2.5 MB in a 9 MB block,
/// and DataFusion charges the 9 MB. Joins and repartition emit through this
/// coalescer, so their view output arrives this way.
#[test]
fn views_in_a_mostly_spare_block_are_copied() {
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    // Every third of 90,000 300-byte strings: a third of the buffers used.
    let text = StringViewArray::from_iter_values((0..90_000).map(|row| format!("{row:0300}")));
    let every_third = arrow::array::UInt32Array::from_iter_values((0..90_000).step_by(3));
    let sparse = arrow::compute::take(&text, &every_third, None).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "text",
        DataType::Utf8View,
        false,
    )]));
    let mut coalescer = arrow::compute::BatchCoalescer::new(schema.clone(), SLICE);
    coalescer
        .push_batch(RecordBatch::try_new(schema, vec![sparse]).unwrap())
        .unwrap();
    coalescer.finish_buffered_batch().unwrap();
    let mut oversized = Vec::new();
    while let Some(given) = coalescer.next_completed_batch() {
        // A view and its string for each row.
        let data = given.num_rows() * (16 + 300);
        let charged = get_record_batch_memory_size(&given);
        let kept = get_record_batch_memory_size(&compact_retained(given, &budget).unwrap());
        if kept > 2 * data {
            oversized.push(format!("charged {charged}, kept {kept} for {data}"));
        }
    }
    assert!(
        oversized.is_empty(),
        "batches kept in their coalescer blocks:\n{}",
        oversized.join("\n")
    );
}
