//! Stable source occurrences using stored origins or initial physical row locations.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result, budget::Budget};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
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

/// Immutable birth-file or writer origin identifying a source occurrence.
pub const OCCURRENCE_FILE: &str = "__docspec_file";
/// Zero-based occurrence position inside its immutable origin.
pub const OCCURRENCE_ROW: &str = "__docspec_row";

/// Adds stable occurrence fields to the captured native schema.
///
/// # Errors
/// Refuses reserved names or unqualified changes in native value interpretation.
pub fn occurrence_schema(table: &Table) -> Result<SchemaRef> {
    super::identity::has_origins(table.metadata().current_schema())?;
    let schema = super::schema::read_schema(table)?;
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
    read_projected_occurrences(table, budget, None).await
}

pub(crate) async fn read_projected_occurrences(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    read_projected_occurrences_inner(table, budget, projection, None, None).await
}

pub(crate) async fn read_projected_occurrences_pruned(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
    retained_files: Option<&std::collections::BTreeSet<String>>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    read_projected_occurrences_inner(table, budget, projection, None, retained_files).await
}

/// Reads selected occurrences while observing attempted native data ranges.
///
/// `None` selects every value and both occurrence fields. Successful complete
/// consumption retains the same coverage checks as `read_occurrences`.
///
/// # Errors
/// Refuses invalid scans/selections, exhausted capacity, or oversized batches.
#[cfg(any(test, feature = "test-util"))]
pub async fn read_occurrences_observed(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
) -> Result<(
    impl Stream<Item = Result<RecordBatch>> + Send + use<>,
    super::scan_metrics::NativeScanObservation,
)> {
    let observation = super::scan_metrics::NativeScanObservation::default();
    let stream = read_projected_occurrences_inner(
        table,
        budget,
        projection,
        Some(observation.clone()),
        None,
    )
    .await?;
    Ok((stream, observation))
}

async fn read_projected_occurrences_inner(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
    observation: Option<super::scan_metrics::NativeScanObservation>,
    retained_files: Option<&std::collections::BTreeSet<String>>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    let mut inventory = super::current_snapshot_files(table, budget).await?;
    if let Some(retained) = retained_files {
        inventory
            .data
            .retain(|file| retained.contains(file.file_path()));
        inventory.paths.retain(|path| retained.contains(path));
    }
    let mut state =
        Occurrences::capture_projected(table, budget, inventory, true, projection).await?;
    state.observation = observation;
    Ok(futures::stream::try_unfold(state, |mut state| {
        Box::pin(async move { Ok(state.next().await?.map(|batch| (batch, state))) })
    }))
}

struct FileRows {
    stream: ArrowRecordBatchStream,
    pending: std::collections::VecDeque<RecordBatch>,
    path: String,
    next: u64,
    rows: u64,
}

struct Occurrences {
    tasks: FileScanTaskStream,
    reader: ArrowReader,
    expected: BTreeMap<String, DataFile>,
    current: Option<FileRows>,
    observation: Option<super::scan_metrics::NativeScanObservation>,
    schema: SchemaRef,
    native_schema: iceberg::spec::SchemaRef,
    partition_spec: iceberg::spec::PartitionSpecRef,
    selection: Option<super::selection::Selection>,
    located: bool,
    limit: usize,
    _batches: OwnedSemaphorePermit,
    _metadata: Arc<OwnedSemaphorePermit>,
}

impl Occurrences {
    async fn open(table: &Table, budget: &Budget) -> Result<Self> {
        let inventory = super::current_snapshot_files(table, budget).await?;
        Self::capture(table, budget, inventory, true).await
    }

    async fn capture(
        table: &Table,
        budget: &Budget,
        inventory: super::SnapshotFiles,
        located: bool,
    ) -> Result<Self> {
        Self::capture_projected(table, budget, inventory, located, None).await
    }

    async fn capture_projected(
        table: &Table,
        budget: &Budget,
        inventory: super::SnapshotFiles,
        located: bool,
        projection: Option<&[usize]>,
    ) -> Result<Self> {
        super::defaults::check_expansion(table.metadata().current_schema(), &budget.limits, false)?;
        let schema = if located {
            occurrence_schema(table)?
        } else {
            super::schema::read_schema(table)?
        };
        if let Some(snapshot) = table.metadata().current_snapshot() {
            snapshot.schema(table.metadata())?;
        }
        let native_schema = table.metadata().current_schema().clone();
        let selection = projection
            .map(|indices| {
                super::selection::Selection::new(
                    &native_schema,
                    &schema,
                    indices,
                    located,
                    true,
                    budget,
                )
            })
            .transpose()?;
        let batches = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
        let fields = match &selection {
            Some(selection) => selection.fields.clone(),
            None => {
                let mut fields = native_schema
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| field.id)
                    .collect::<Vec<_>>();
                fields.push(iceberg::metadata_columns::RESERVED_FIELD_ID_FILE);
                fields
            }
        };
        let name_mapping = table
            .metadata()
            .properties()
            .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
            .map(|raw| serde_json::from_str::<iceberg::spec::NameMapping>(raw).map(Arc::new))
            .transpose()?;
        // Build scan tasks directly from the already-admitted current-snapshot
        // descriptors instead of decoding the current snapshot's manifests again.
        let tasks = futures::stream::iter(
            inventory
                .data
                .iter()
                .map(|file| {
                    Ok(FileScanTask::builder()
                        .with_file_size_in_bytes(file.file_size_in_bytes())
                        .with_start(0)
                        .with_length(file.file_size_in_bytes())
                        .with_record_count(Some(file.record_count()))
                        .with_data_file_path(file.file_path().to_owned())
                        .with_data_file_format(file.file_format())
                        .with_schema(native_schema.clone())
                        .with_project_field_ids(fields.clone())
                        .with_partition(Some(file.partition().clone()))
                        .with_partition_spec(None)
                        .with_name_mapping(name_mapping.clone())
                        .with_case_sensitive(true)
                        .build())
                })
                .collect::<Vec<_>>(),
        )
        .boxed();
        // Retain the existing inventory reservation while its live descriptors are used.
        let expected = inventory
            .data
            .into_iter()
            .map(|file| (file.file_path().to_owned(), file))
            .collect();
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
            observation: None,
            schema,
            native_schema,
            partition_spec: table.metadata().default_partition_spec().clone(),
            selection,
            located,
            limit: budget.limits.max_batch_bytes,
            _batches: batches,
            _metadata: Arc::new(inventory._reservation),
        })
    }

    fn next<'a>(&'a mut self) -> futures::future::BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            loop {
                if let Some(file) = &mut self.current {
                    if file.pending.is_empty()
                        && let Some(raw_batch) = file.stream.try_next().await?
                    {
                        let stripped = super::schema::strip_file_column(raw_batch, self.limit)?;
                        for slice in super::split_oversized_batch(stripped, self.limit)? {
                            file.pending.push_back(slice);
                        }
                    }
                    if let Some(batch) = file.pending.pop_front() {
                        let batch =
                            super::identity::materialize(batch, &file.path, file.next, self.limit)?;
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
                        let batch = if let Some(selection) = &self.selection {
                            selection.apply(
                                batch,
                                Some((&file.path, file.next, end)),
                                self.limit,
                            )?
                        } else if self.located {
                            append_locations(
                                batch,
                                &file.path,
                                file.next,
                                end,
                                &self.schema,
                                self.limit,
                            )?
                        } else {
                            batch
                        };
                        file.next = end;
                        return Ok(Some(batch));
                    }
                    if file.next != file.rows {
                        return Err(Error::refusal(
                            "occurrence_count",
                            "decoded rows differ from the captured native count",
                        ));
                    }
                    if let Some(observation) = &self.observation {
                        observation.finish();
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
                let result = self
                    .reader
                    .clone()
                    .read(futures::stream::once(async move { Ok(task) }).boxed())?;
                if let Some(observation) = &self.observation {
                    observation.attach(&result);
                }
                let stream = result.stream();
                self.current = Some(FileRows {
                    stream,
                    pending: std::collections::VecDeque::new(),
                    path,
                    next: 0,
                    rows,
                });
                // An empty-file population still yields cooperatively between tasks.
                tokio::task::yield_now().await;
            }
        })
    }
}

/// Captures native file tasks once; payloads open only for requested files.
pub(crate) struct OccurrenceScan {
    tasks: BTreeMap<String, (FileScanTask, DataFile)>,
    reader: ArrowReader,
    schema: SchemaRef,
    native_schema: iceberg::spec::SchemaRef,
    partition_spec: iceberg::spec::PartitionSpecRef,
    located: bool,
    _metadata: Arc<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for OccurrenceScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OccurrenceScan")
            .field("files", &self.tasks.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl OccurrenceScan {
    pub async fn open(table: &Table, budget: &Budget) -> Result<Self> {
        Self::capture(
            Occurrences::open(table, budget).await?,
            budget.limits.max_metadata_bytes,
        )
        .await
    }

    /// Transfers an admitted inventory into a values-only selected-file reader.
    pub async fn values(
        table: &Table,
        budget: &Budget,
        inventory: super::SnapshotFiles,
    ) -> Result<Self> {
        Self::capture(
            Occurrences::capture(table, budget, inventory, false).await?,
            budget.limits.max_metadata_bytes,
        )
        .await
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn native_schema(&self) -> &iceberg::spec::SchemaRef {
        &self.native_schema
    }

    pub fn partition_spec(&self) -> &iceberg::spec::PartitionSpecRef {
        &self.partition_spec
    }

    pub fn is_located(&self) -> bool {
        self.located
    }

    async fn capture(mut state: Occurrences, metadata_limit: usize) -> Result<Self> {
        let mut tasks = BTreeMap::new();
        let mut bytes = 0usize;
        while let Some(task) = state.tasks.try_next().await? {
            let file = state.expected.remove(&task.data_file_path).ok_or_else(|| {
                Error::refusal(
                    "occurrence_tasks",
                    "native scan repeats or introduces a file",
                )
            })?;
            check_task(&task, &file)?;
            // Bound retained task details as well as the native inventory. Schema
            // objects are shared; field projections and paths are task-owned.
            bytes = bytes
                .saturating_add(task.data_file_path.len().saturating_mul(3))
                .saturating_add(
                    task.project_field_ids
                        .len()
                        .saturating_mul(size_of::<i32>()),
                )
                .saturating_add(512);
            if bytes > metadata_limit {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "captured file tasks exceed metadata allowance",
                ));
            }
            tasks.insert(task.data_file_path.clone(), (task, file));
        }
        if !state.expected.is_empty() {
            return Err(Error::refusal(
                "occurrence_tasks",
                "native scan omitted a captured file",
            ));
        }
        Ok(Self {
            tasks,
            reader: state.reader,
            schema: state.schema,
            native_schema: state.native_schema,
            partition_spec: state.partition_spec,
            located: state.located,
            _metadata: state._metadata,
        })
    }

    pub fn contains_file(&self, path: &str) -> bool {
        self.tasks.contains_key(path)
    }

    /// Looks up the row count without walking other files.
    pub fn file_rows(&self, path: &str) -> Option<u64> {
        self.tasks.get(path).map(|(_, file)| file.record_count())
    }

    pub fn files(&self) -> impl Iterator<Item = &DataFile> {
        self.tasks.values().map(|(_, file)| file)
    }

    /// Looks up one captured file descriptor without walking the population.
    pub fn file(&self, path: &str) -> Option<&DataFile> {
        self.tasks.get(path).map(|(_, file)| file)
    }

    pub fn read_file(
        &self,
        path: &str,
        budget: &Budget,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
        self.read_projected_file(path, budget, None)
    }

    pub fn read_projected_file(
        &self,
        path: &str,
        budget: &Budget,
        projection: Option<&[usize]>,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
        let (task, file) = self.tasks.get(path).ok_or_else(|| {
            Error::refusal("occurrence_tasks", "file is outside the captured source")
        })?;
        let selection = projection
            .map(|indices| {
                super::selection::Selection::new(
                    &self.native_schema,
                    &self.schema,
                    indices,
                    self.located,
                    true,
                    budget,
                )
            })
            .transpose()?;
        self.read_selection_file(task, file, budget, selection, None)
    }

    /// Selects scalar columns only when a requested physical column is evidenced.
    ///
    /// The boolean tells the caller whether batches already have its projection.
    /// Missing metrics or only absent/nested fields retain full-batch admission.
    ///
    /// # Errors
    /// Refuses invalid selections, uncaptured files, or exhausted capacity.
    pub(crate) fn read_scalar_file(
        &self,
        path: &str,
        budget: &Budget,
        projection: &[usize],
    ) -> Result<(impl Stream<Item = Result<RecordBatch>> + Send + use<>, bool)> {
        self.read_scalar_file_inner(path, budget, projection, None)
    }

    #[cfg(test)]
    fn read_scalar_file_observed(
        &self,
        path: &str,
        budget: &Budget,
        projection: &[usize],
    ) -> Result<(
        impl Stream<Item = Result<RecordBatch>> + Send + use<>,
        bool,
        super::scan_metrics::NativeScanObservation,
    )> {
        let observation = super::scan_metrics::NativeScanObservation::default();
        let (stream, projected) =
            self.read_scalar_file_inner(path, budget, projection, Some(observation.clone()))?;
        Ok((stream, projected, observation))
    }

    fn read_scalar_file_inner(
        &self,
        path: &str,
        budget: &Budget,
        projection: &[usize],
        observation: Option<super::scan_metrics::NativeScanObservation>,
    ) -> Result<(impl Stream<Item = Result<RecordBatch>> + Send + use<>, bool)> {
        let (task, file) = self.tasks.get(path).ok_or_else(|| {
            Error::refusal("occurrence_tasks", "file is outside the captured source")
        })?;
        let selection = super::selection::Selection::new(
            &self.native_schema,
            &self.schema,
            projection,
            self.located,
            true,
            budget,
        )?;
        // Iceberg falls back to decoding all columns when every requested ID
        // is absent. Only admitted native primitive-column metrics establish
        // a nonempty physical mask; compressed sizes do not estimate memory.
        let projected = self.native_schema.as_struct().fields().iter().any(|field| {
            field.field_type.is_primitive()
                && selection.fields.contains(&field.id)
                && file.column_sizes().contains_key(&field.id)
        });
        let stream = self.read_selection_file(
            task,
            file,
            budget,
            projected.then_some(selection),
            observation,
        )?;
        Ok((stream, projected))
    }

    fn read_selection_file(
        &self,
        task: &FileScanTask,
        file: &DataFile,
        budget: &Budget,
        selection: Option<super::selection::Selection>,
        observation: Option<super::scan_metrics::NativeScanObservation>,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
        let mut task = task.clone();
        if let Some(selection) = &selection {
            task.project_field_ids = selection.fields.clone();
        }
        let expected = BTreeMap::from([(task.data_file_path.clone(), file.clone())]);
        let state = Occurrences {
            tasks: futures::stream::iter([Ok(task)]).boxed(),
            reader: self.reader.clone(),
            schema: self.schema.clone(),
            native_schema: self.native_schema.clone(),
            partition_spec: self.partition_spec.clone(),
            selection,
            located: self.located,
            expected,
            current: None,
            observation,
            limit: budget.limits.max_batch_bytes,
            _batches: budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?,
            _metadata: self._metadata.clone(),
        };
        Ok(futures::stream::try_unfold(state, |mut state| {
            Box::pin(async move { Ok(state.next().await?.map(|batch| (batch, state))) })
        }))
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
    let (file, row) = location_columns(&batch, path, start, end, true, true, limit)?;
    let mut columns = batch.columns().to_vec();
    columns.push(file.expect("requested file column"));
    columns.push(row.expect("requested row column"));
    let result = RecordBatch::try_new(schema.clone(), columns)?;
    if super::effective_batch_bytes(&result) > limit {
        return Err(oversized());
    }
    Ok(result)
}

pub(super) fn location_columns(
    batch: &RecordBatch,
    path: &str,
    start: u64,
    end: u64,
    want_file: bool,
    want_row: bool,
    limit: usize,
) -> Result<(Option<ArrayRef>, Option<ArrayRef>)> {
    let native_schema = batch.schema();
    if let (Ok(file), Ok(row)) = (
        native_schema.index_of(super::identity::ORIGIN_FILE),
        native_schema.index_of(super::identity::ORIGIN_ROW),
    ) {
        return Ok((
            want_file.then(|| batch.column(file).clone()),
            want_row.then(|| batch.column(row).clone()),
        ));
    }
    // Bound path replication and position buffers before allocating them. The
    // final Arrow memory check also accounts for buffer padding and array structs.
    let file_bytes = if want_file {
        path.len().saturating_add(size_of::<i32>())
    } else {
        0
    };
    let row_bytes = if want_row { size_of::<i64>() } else { 0 };
    let added = file_bytes
        .saturating_add(row_bytes)
        .saturating_mul(batch.num_rows())
        .saturating_add(if want_file { size_of::<i32>() } else { 0 });
    if super::effective_batch_bytes(batch).saturating_add(added) > limit {
        return Err(oversized());
    }
    let file = want_file.then(|| {
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            path,
            batch.num_rows(),
        ))) as ArrayRef
    });
    let row = want_row
        .then(|| Arc::new(Int64Array::from_iter_values(start as i64..end as i64)) as ArrayRef);
    Ok((file, row))
}

fn oversized() -> Error {
    Error::refusal(
        "oversized_batch",
        "source values and occurrence locations exceed the reader byte allowance",
    )
}

#[cfg(test)]
mod tests;
