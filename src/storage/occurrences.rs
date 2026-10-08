//! Source-pinned physical row locations using unfiltered native file reads.

use crate::{Error, Result, budget::Budget};
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::{
    arrow::ArrowReader,
    scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream},
    spec::{DataFile, DataFileFormat},
    table::Table,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

/// Exact native data-file path; meaningful only with its verified source pin.
pub const OCCURRENCE_FILE: &str = "__docspec_file";
/// Zero-based physical row position inside the named immutable data file.
pub const OCCURRENCE_ROW: &str = "__docspec_row";

/// Adds physical occurrence fields to the captured native schema.
///
/// These are source-pinned locations, not logical IDs stable across rewrites.
///
/// # Errors
/// Refuses reserved names or an unqualified snapshot/current-schema difference.
pub fn occurrence_schema(table: &Table) -> Result<SchemaRef> {
    let metadata = table.metadata();
    if let Some(snapshot) = metadata.current_snapshot()
        && snapshot.schema(metadata)? != *metadata.current_schema()
    {
        return Err(Error::refusal(
            "unsupported_schema_evolution",
            "occurrence reads require the selected snapshot and current schema to agree",
        ));
    }
    let schema = iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())?;
    if schema.fields().iter().any(|field| {
        field.name().eq_ignore_ascii_case(OCCURRENCE_FILE)
            || field.name().eq_ignore_ascii_case(OCCURRENCE_ROW)
    }) {
        return Err(Error::refusal(
            "reserved_field",
            "source schema uses a reserved occurrence field name",
        ));
    }
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(OCCURRENCE_FILE, DataType::Utf8, false)));
    fields.push(Arc::new(Field::new(OCCURRENCE_ROW, DataType::Int64, false)));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Reads every source occurrence before any relational filtering.
///
/// Verifies one complete task per live native file and exact decoded row counts.
/// The caller must bind paths to verified immutable objects in its source pin.
/// Coverage is established only after successful end-of-stream consumption.
///
/// # Errors
/// Refuses partial/deleted/filtered tasks, incompatible schemas, invalid counts,
/// exhausted capacity, oversized batches, or native read failures.
pub async fn read_occurrences(
    table: &Table,
    budget: &Budget,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    let state = Occurrences::open(table, budget).await?;
    Ok(futures::stream::try_unfold(state, |mut state| async move {
        Ok(state.next().await?.map(|batch| (batch, state)))
    }))
}

struct FileRows {
    stream: ArrowRecordBatchStream,
    path: String,
    next: u64,
    rows: u64,
}

struct Occurrences {
    tasks: FileScanTaskStream,
    reader: ArrowReader,
    expected: BTreeMap<String, DataFile>,
    current: Option<FileRows>,
    schema: SchemaRef,
    limit: usize,
    _batches: OwnedSemaphorePermit,
    _metadata: OwnedSemaphorePermit,
}

impl Occurrences {
    async fn open(table: &Table, budget: &Budget) -> Result<Self> {
        let schema = occurrence_schema(table)?;
        let batches = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
        let inventory = super::snapshot_files(table, budget).await?;
        // Retain the existing inventory reservation while its live descriptors are used.
        let expected = inventory
            .data
            .into_iter()
            .map(|file| (file.file_path().to_owned(), file))
            .collect();
        let tasks = table
            .scan()
            .with_concurrency_limit(1)
            .build()?
            .plan_files()
            .await?;
        let reader = table
            .reader_builder()
            .with_data_file_concurrency_limit(1)
            .with_batch_size(budget.limits.batch_rows)
            .with_row_group_filtering_enabled(false)
            .with_row_selection_enabled(false)
            .build();
        Ok(Self {
            tasks,
            reader,
            expected,
            current: None,
            schema,
            limit: budget.limits.max_batch_bytes,
            _batches: batches,
            _metadata: inventory._reservation,
        })
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(file) = &mut self.current {
                if let Some(batch) = file.stream.try_next().await? {
                    let end = file
                        .next
                        .checked_add(batch.num_rows() as u64)
                        .filter(|end| *end <= file.rows && *end <= i64::MAX as u64)
                        .ok_or_else(|| {
                            Error::refusal(
                                "occurrence_count",
                                "decoded rows exceed the captured native count",
                            )
                        })?;
                    let batch = append_locations(
                        batch,
                        &file.path,
                        file.next,
                        end,
                        &self.schema,
                        self.limit,
                    )?;
                    file.next = end;
                    return Ok(Some(batch));
                }
                if file.next != file.rows {
                    return Err(Error::refusal(
                        "occurrence_count",
                        "decoded rows differ from the captured native count",
                    ));
                }
                self.current = None;
            }
            let Some(task) = self.tasks.try_next().await? else {
                if !self.expected.is_empty() {
                    return Err(Error::refusal(
                        "occurrence_tasks",
                        "native scan omitted a captured live data file",
                    ));
                }
                return Ok(None);
            };
            let file = self.expected.remove(&task.data_file_path).ok_or_else(|| {
                Error::refusal(
                    "occurrence_tasks",
                    "native scan repeats a file or includes an unknown file",
                )
            })?;
            check_task(&task, &file)?;
            let path = task.data_file_path.clone();
            let rows = file.record_count();
            let stream = self
                .reader
                .clone()
                .read(futures::stream::once(async move { Ok(task) }).boxed())?
                .stream();
            self.current = Some(FileRows {
                stream,
                path,
                next: 0,
                rows,
            });
            // An empty-file population still yields cooperatively between tasks.
            tokio::task::yield_now().await;
        }
    }
}

fn check_task(task: &FileScanTask, file: &DataFile) -> Result<()> {
    if task.start != 0
        || task.length != file.file_size_in_bytes()
        || task.file_size_in_bytes != file.file_size_in_bytes()
        || task.record_count != Some(file.record_count())
        || file.record_count() > i64::MAX as u64
        || task.data_file_format != DataFileFormat::Parquet
        || task.predicate.is_some()
        || !task.deletes.is_empty()
    {
        return Err(Error::refusal(
            "unsupported_occurrence_scan",
            "occurrence reads require unfiltered, complete Parquet files with exact native counts",
        ));
    }
    Ok(())
}

fn append_locations(
    batch: RecordBatch,
    path: &str,
    start: u64,
    end: u64,
    schema: &SchemaRef,
    limit: usize,
) -> Result<RecordBatch> {
    // Bound path replication and position buffers before allocating them. The
    // final Arrow memory check also accounts for buffer padding and array structs.
    let added = path
        .len()
        .saturating_add(size_of::<i64>() + size_of::<i32>())
        .saturating_mul(batch.num_rows())
        .saturating_add(size_of::<i32>());
    if batch.get_array_memory_size().saturating_add(added) > limit {
        return Err(oversized());
    }
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from_iter_values(
        std::iter::repeat_n(path, batch.num_rows()),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        start as i64..end as i64,
    )));
    let result = RecordBatch::try_new(schema.clone(), columns)?;
    if result.get_array_memory_size() > limit {
        return Err(oversized());
    }
    Ok(result)
}

fn oversized() -> Error {
    Error::refusal(
        "oversized_batch",
        "source values and occurrence locations exceed the reader byte allowance",
    )
}

#[cfg(test)]
mod tests;
