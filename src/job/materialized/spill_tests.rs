//! A spilled step's private file: what it carries and how it is read back.
//!
//! The limits are the ones congressional-tech's bounded-memory comparison
//! tests run under (`test_capture_comparison_memory.py`, `limited_engine`),
//! and the rows have their shape: two texts of about 8 KiB and many narrow,
//! mostly empty columns. At those limits the spill writer starts a row group
//! every 256 KiB, so a few thousand rows leave thousands of column chunks and
//! a footer larger than `max_metadata_bytes`.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::{budget::Limits, job::Job};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray, new_null_array},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::metadata::ParquetMetaData,
};
use std::path::{Path, PathBuf};

/// Rows per input batch: the limits' `batch_rows`.
const BATCH: usize = 32;
/// Narrow columns beside the key and the two texts, 61 leaf columns in all.
const NARROW: usize = 58;
/// Bytes of each of a row's two texts.
const TEXT: usize = 8 << 10;

fn tight() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 192 << 20,
        scratch_bytes: 256 << 20,
        batch_rows: BATCH,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 1 << 20,
    }
}

fn wide_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("old_text", DataType::Utf8, true),
        Field::new("new_text", DataType::Utf8, true),
    ];
    fields.extend(
        (0..NARROW).map(|index| Field::new(format!("narrow_{index}"), DataType::Utf8, true)),
    );
    Arc::new(Schema::new(fields))
}

/// Rows `first..first + rows`: a key, two distinct texts, and narrow columns
/// of which every third holds a short value and the rest are null.
fn wide_batch(schema: &SchemaRef, first: usize, rows: usize) -> RecordBatch {
    let text = |side: &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values((first..first + rows).map(
            |row| {
                let head = format!("{side}-{row:08}-");
                format!("{head}{}", "x".repeat(TEXT - head.len()))
            },
        )))
    };
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            (first..first + rows).map(|row| row as i64),
        )),
        text("old"),
        text("new"),
    ];
    columns.extend((0..NARROW).map(|index| -> ArrayRef {
        if index % 3 == 0 {
            Arc::new(StringArray::from_iter_values(
                (first..first + rows).map(|row| format!("value-{index}-{row}")),
            ))
        } else {
            new_null_array(&DataType::Utf8, rows)
        }
    }));
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn wide_batches(rows: usize) -> Vec<RecordBatch> {
    let schema = wide_schema();
    (0..rows)
        .step_by(BATCH)
        .map(|first| wide_batch(&schema, first, BATCH.min(rows - first)))
        .collect()
}

/// Retains `rows` wide rows as step `name`, in `BATCH`-row batches.
async fn retain_wide(job: &Job, name: &str, rows: usize) -> Result<(StepStorage, usize, usize)> {
    let schema = wide_schema();
    let batches = stream::iter(wide_batches(rows).into_iter().map(Ok));
    let input = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), batches));
    retain(&job.context, &job.budget, name, schema, input).await
}

/// Every Parquet file under `root`: the job's spill files while their steps live.
fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(parquet_files(&path));
        } else if std::fs::read(&path).is_ok_and(|bytes| bytes.ends_with(b"PAR1")) {
            files.push(path);
        }
    }
    files
}

fn metadata(path: &Path) -> Arc<ParquetMetaData> {
    ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
        .unwrap()
        .metadata()
        .clone()
}

/// The serialized footer's length, from the file's last eight bytes.
fn footer_bytes(path: &Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    let tail = &bytes[bytes.len() - 8..bytes.len() - 4];
    u32::from_le_bytes(tail.try_into().unwrap()) as usize
}

async fn rows_of(job: &Job, name: &str) -> RecordBatch {
    let frame = job.sql(&format!("SELECT * FROM {name}")).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    concat_batches(&schema, &frame.collect().await.unwrap()).unwrap()
}

#[tokio::test]
async fn a_spilled_step_is_read_back_whatever_its_footer_weighs() {
    // 8,192 rows: about 500 row groups of 61 columns, a footer near 2 MiB
    // even with no statistics in it, against a 1 MiB allowance.
    const ROWS: usize = 8192;
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(tight(), root.path().join("scratch")).unwrap();
    let (storage, rows, _) = retain_wide(&job, "wide", ROWS)
        .await
        .expect("the engine reads back the spill file it has just written");
    assert_eq!((storage, rows), (StepStorage::Scratch, ROWS));
    let files = parquet_files(&root.path().join("scratch"));
    assert_eq!(files.len(), 1);
    assert!(
        footer_bytes(&files[0]) > tight().max_metadata_bytes,
        "the footer must exceed the allowance, or this test shows nothing: {} bytes",
        footer_bytes(&files[0])
    );
    let counted = job
        .sql("SELECT count(*) AS n, count(DISTINCT id) AS ids, sum(length(new_text)) AS bytes FROM wide")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for (column, expected) in [ROWS, ROWS, ROWS * TEXT].into_iter().enumerate() {
        let values = counted[0]
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>();
        assert_eq!(values.unwrap().value(0), expected as i64);
    }
}

#[tokio::test]
async fn a_spill_file_carries_no_statistics_and_no_page_index_and_replays_its_rows() {
    // Nothing may depend on them: a replay is the whole file with no
    // predicate, and the step itself tells the planner its row count.
    const ROWS: usize = 2048;
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            // Room for this smaller file's footer under either rule.
            max_metadata_bytes: 32 << 20,
            ..tight()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    let (storage, _, _) = retain_wide(&job, "wide", ROWS).await.unwrap();
    assert_eq!(storage, StepStorage::Scratch);
    let files = parquet_files(&root.path().join("scratch"));
    assert_eq!(files.len(), 1);
    let metadata = metadata(&files[0]);
    assert!(metadata.num_row_groups() > 1);
    for group in metadata.row_groups() {
        for column in group.columns() {
            let path = column.column_path().string();
            assert!(column.statistics().is_none(), "{path} has statistics");
            assert!(
                column.column_index_offset().is_none(),
                "{path} has a column index"
            );
            assert!(
                column.offset_index_offset().is_none(),
                "{path} has an offset index"
            );
        }
    }
    let expected = concat_batches(&wide_schema(), &wide_batches(ROWS)).unwrap();
    let replayed = rows_of(&job, "wide").await;
    assert_eq!(replayed.columns(), expected.columns());
}

#[tokio::test]
async fn a_spill_whose_footer_the_budget_cannot_hold_is_refused_by_name() {
    const ROWS: usize = 8192;
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(tight(), root.path().join("scratch")).unwrap();
    // Leave the spill writer and the batch conversions their working room,
    // and not the sixteen-fold charge of a footer of about 2 MiB.
    let spare = 16 << 20;
    let available = job
        .budget
        .reserve(0)
        .unwrap()
        .semaphore()
        .available_permits();
    let _held = job.budget.reserve(available - spare).unwrap();
    let error = retain_wide(&job, "wide", ROWS).await.unwrap_err();
    assert_eq!(error.code(), "capacity", "{error}");
    assert!(error.message().contains("spilled step wide"), "{error}");
}

/// A file the engine did not write, with the spill writer's old shape:
/// statistics, page index and a row group every 256 KiB.
fn write_foreign(path: &Path, rows: usize) {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_bytes(Some(256 << 10))
        .set_write_batch_size(BATCH)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        wide_schema(),
        Some(props),
    )
    .unwrap();
    for batch in wide_batches(rows) {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

#[tokio::test]
async fn a_foreign_file_over_the_allowance_is_refused_wherever_it_lies() {
    const ROWS: usize = 4096;
    let root = tempfile::tempdir().unwrap();
    let scratch = root.path().join("scratch");
    let job = Job::new(tight(), scratch.clone()).unwrap();
    let outside = root.path().join("foreign.parquet");
    write_foreign(&outside, ROWS);
    assert!(footer_bytes(&outside) > tight().max_metadata_bytes);
    // Lying in the scratch directory does not make a file the engine's own.
    let planted = scratch.join("planted.parquet");
    std::fs::copy(&outside, &planted).unwrap();
    for (name, path) in [("outside", &outside), ("planted", &planted)] {
        let error = job.register_parquet(name, path).await.unwrap_err();
        assert_eq!(error.code(), "metadata_capacity", "{name}: {error}");
        assert!(
            error.message().contains("exceeds metadata allowance"),
            "{name}: {error}"
        );
    }
}
