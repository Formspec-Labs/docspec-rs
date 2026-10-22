//! Bounded local objects and maintained Iceberg readers, writers, and commit actions.
// Rust guideline compliant 2026-02-21

pub(crate) mod defaults;
mod flat;
pub(crate) mod identity;
mod inspection;
mod object_io;
mod occurrences;
pub(crate) use object_io::{Objects, PinTable, valid_object_locator};
mod pinned;
pub(crate) use pinned::decode as decode_native_metadata;
#[doc(inline)]
pub use pinned::{open_pin, open_pin_with_io};
#[cfg(feature = "remote")]
pub mod remote;
pub(crate) mod removal;
mod scan_metrics;
#[cfg(any(test, feature = "test-util"))]
#[doc(inline)]
pub use scan_metrics::{NativeScanObservation, NativeScanSample};
pub(crate) mod schema;
mod selection;
pub(crate) use selection::reserve_projection;
mod tables;
pub(crate) use inspection::{CheckedFile, inspect_table, schema_identity};
#[cfg(any(test, feature = "test-util"))]
#[doc(inline)]
pub use occurrences::read_occurrences_observed;
#[doc(inline)]
pub use occurrences::{OCCURRENCE_FILE, OCCURRENCE_ROW, occurrence_schema, read_occurrences};
pub(crate) use occurrences::{OccurrenceScan, read_projected_occurrences_pruned};
#[allow(unused_imports)]
pub(crate) use tables::{
    CandidateWriter, bounded_json, build_unbound_partition_spec, check_partition_by_matches,
    default_partition_field_name, fresh_metadata, fresh_metadata_at, fresh_partitioned_metadata,
    fresh_partitioned_metadata_at, from_iceberg_transform, partition_specs_from_metadata,
    register_candidate, replacement_metadata, replacement_metadata_at, to_iceberg_transform,
    validate_partition_spec, validate_unbound_partition_spec,
};

use crate::{
    Error, Result,
    budget::Budget,
    model::{Id, ObjectRef, TablePin},
};
use arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::{
    base_writer::data_file_writer::DataFileWriterBuilder,
    file_writer::ParquetWriterBuilder,
    file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
    file_writer::rolling_writer::RollingFileWriterBuilder,
    partitioning::unpartitioned_writer::UnpartitionedWriter,
};
use iceberg::{
    Catalog, CatalogBuilder,
    io::LocalFsStorageFactory,
    spec::{DataFile, DataFileFormat},
    table::Table,
};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

/// Maximum byte length written into Parquet column chunk min/max statistics.
///
/// Variable-width bounds strictly shorter than this threshold are guaranteed
/// untruncated and can be used as exact interval endpoints for file pruning
/// (including 36-byte UUIDs, 64-byte SHA-256 hex digests, and 74-byte `body_key`
/// locators).
pub(crate) const STATISTICS_TRUNCATE_BYTES: usize = 128;

/// Hashes the exact compact JSON serialization without retaining its bytes.
///
/// # Errors
/// Returns the same serialization errors as `serde_json::to_vec`.
pub(crate) fn json_digest<T: serde::Serialize + ?Sized>(
    value: &T,
) -> serde_json::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    serde_json::to_writer(&mut hasher, value)?;
    Ok(hasher.finalize())
}

/// Creates the maintained embedded catalog; application rows remain in Iceberg.
///
/// # Errors
/// Returns filesystem, SQLite driver, or catalog initialization errors.
pub async fn local_catalog(root: &Path) -> Result<Arc<dyn Catalog>> {
    local_catalog_at(root, &crate::restore::catalog_path(root)?).await
}

/// Opens one explicitly selected native catalog during fenced local recovery.
pub(crate) async fn local_catalog_at(root: &Path, file: &Path) -> Result<Arc<dyn Catalog>> {
    std::fs::create_dir_all(root.join("warehouse"))?;
    let catalog = SqlCatalogBuilder::default()
        .uri(format!("sqlite://{}?mode=rwc", file.display()))
        .warehouse_location(root.join("warehouse").to_string_lossy())
        .sql_bind_style(SqlBindStyle::QMark)
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("docspec", HashMap::new())
        .await?;
    Ok(Arc::new(catalog))
}

pub(crate) fn table_properties() -> HashMap<String, String> {
    HashMap::from([
        ("commit.retry.num-retries".into(), "0".into()),
        (
            "write.metadata.delete-after-commit.enabled".into(),
            "false".into(),
        ),
        ("gc.enabled".into(), "false".into()),
    ])
}

/// Creates a serial native reader with capacity retained until stream drop.
///
/// # Errors
/// Refuses exhausted reader capacity, invalid scans, or oversized decoded batches.
pub async fn read_batches(
    table: &Table,
    budget: &Budget,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    read_projected_batches(table, budget, None).await
}

pub(crate) async fn read_projected_batches(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    read_projected_batches_inner(table, budget, projection, None).await
}

/// Reads selected columns while observing attempted native data ranges.
///
/// `None` selects all values; indices preserve requested output order.
/// Use the returned observer during qualification without another payload pass.
///
/// # Errors
/// Refuses invalid selections, exhausted capacity, or oversized decoded batches.
#[cfg(any(test, feature = "test-util"))]
pub async fn read_batches_observed(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
) -> Result<(
    impl Stream<Item = Result<RecordBatch>> + Send + use<>,
    NativeScanObservation,
)> {
    let observation = NativeScanObservation::default();
    let stream =
        read_projected_batches_inner(table, budget, projection, Some(observation.clone())).await?;
    Ok((stream, observation))
}

async fn read_projected_batches_inner(
    table: &Table,
    budget: &Budget,
    projection: Option<&[usize]>,
    observation: Option<scan_metrics::NativeScanObservation>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + use<>> {
    defaults::check_expansion(table.metadata().current_schema(), &budget.limits, false)?;
    let reservation = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    let limit = budget.limits.max_batch_bytes;
    let selection = projection
        .map(|indices| {
            selection::Selection::new(
                table.metadata().current_schema(),
                &schema::read_schema(table)?,
                indices,
                false,
                false,
                budget,
            )
        })
        .transpose()?;
    let tasks = match &selection {
        Some(selection) => schema::plan_selected_files(table, selection.fields.clone()).await?,
        None => schema::plan_files(table).await?,
    };
    let result = table
        .reader_builder()
        .with_data_file_concurrency_limit(1)
        .with_batch_size(budget.limits.batch_rows)
        .build()
        .read(tasks)?;
    if let Some(observation) = &observation {
        observation.attach(&result);
    }
    let stream = result.stream();
    let pending = std::collections::VecDeque::<RecordBatch>::new();
    Ok(futures::stream::try_unfold(
        (stream, pending, reservation, selection, observation),
        move |(mut stream, mut pending, reservation, selection, observation)| async move {
            loop {
                if let Some(sub_batch) = pending.pop_front() {
                    let sub_batch = match &selection {
                        Some(selection) => selection.apply(sub_batch, None, limit)?,
                        None => sub_batch,
                    };
                    return Ok(Some((
                        sub_batch,
                        (stream, pending, reservation, selection, observation),
                    )));
                }
                let Some(batch) = stream.try_next().await? else {
                    if let Some(observation) = &observation {
                        observation.finish();
                    }
                    return Ok(None);
                };
                let batch = schema::strip_file_column(batch, limit)?;
                for slice in split_oversized_batch(batch, limit)? {
                    pending.push_back(slice);
                }
            }
        },
    ))
}

pub(crate) async fn verify_objects(objects: Vec<ObjectRef>, budget: &Budget) -> Result<()> {
    check_objects(objects, budget, verify_object).await
}

pub(crate) async fn check_available(objects: Vec<ObjectRef>, budget: &Budget) -> Result<()> {
    check_objects(objects, budget, check_object_available).await
}

fn check_object_available(object: &ObjectRef) -> Result<()> {
    #[cfg(feature = "remote")]
    if let Some(remote) = remote::RemoteObjects::for_uri(&object.path) {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::refusal(
                "runtime_required",
                "remote object availability check requires a Tokio runtime",
            )
        })?;
        return handle.block_on(remote.check_available_admitted(object));
    }
    let metadata = std::fs::metadata(&object.path).map_err(|_| {
        Error::refusal(
            "unavailable_phase",
            "retained completion references an unavailable object",
        )
    })?;
    if !metadata.is_file() || metadata.len() != object.bytes {
        return Err(Error::refusal(
            "unavailable_phase",
            "retained completion object size changed",
        ));
    }
    Ok(())
}

async fn check_objects(
    objects: Vec<ObjectRef>,
    budget: &Budget,
    check: impl Fn(&ObjectRef) -> Result<()> + Send + 'static,
) -> Result<()> {
    let bytes = objects.iter().fold(
        objects
            .capacity()
            .saturating_mul(std::mem::size_of::<ObjectRef>()),
        |total, object| {
            total
                .saturating_add(object.path.capacity())
                .saturating_add(object.digest.capacity())
                .saturating_add(object.owner.as_str().len())
        },
    );
    if bytes > budget.limits.max_metadata_bytes {
        return Err(Error::refusal(
            "metadata_capacity",
            "worker object inventory exceeds metadata allowance",
        ));
    }
    let inventory = budget.reserve(bytes)?;
    budget
        .blocking(move || {
            // Cancellation cannot release this owned inventory's admission while its
            // blocking worker is still queued or reading retained files.
            let _inventory = inventory;
            for object in objects {
                check(&object)?;
            }
            Ok(())
        })
        .await
}

pub(crate) fn coerce_data_type(
    data_type: &arrow::datatypes::DataType,
) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::List(field) => DataType::List(Arc::new(coerce_arrow_field(field))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(coerce_arrow_field(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(coerce_arrow_field(field)), *size)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(coerce_arrow_field(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(crate) fn coerce_arrow_field(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    let coerced = coerce_data_type(field.data_type());
    if &coerced == field.data_type() {
        field.clone()
    } else {
        field.clone().with_data_type(coerced)
    }
}

pub(crate) fn coerce_arrow_schema(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(coerce_arrow_field(field)))
        .collect::<Vec<_>>();
    arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub(crate) fn arrow_schema_to_schema_auto_assign_ids(
    schema: &arrow::datatypes::Schema,
) -> Result<iceberg::spec::Schema> {
    let coerced = coerce_arrow_schema(schema);
    Ok(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
        &coerced,
    )?)
}

pub(crate) fn coerce_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    if schema
        .fields()
        .iter()
        .all(|field| &coerce_data_type(field.data_type()) == field.data_type())
    {
        return Ok(batch);
    }
    let coerced_schema = Arc::new(coerce_arrow_schema(schema.as_ref()));
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (column, target_field) in batch.columns().iter().zip(coerced_schema.fields()) {
        if column.data_type() == target_field.data_type() {
            columns.push(column.clone());
        } else {
            columns.push(arrow::compute::cast(column, target_field.data_type())?);
        }
    }
    Ok(RecordBatch::try_new(coerced_schema, columns)?)
}

// Keep the large maintained writer future out of each caller's poll frame.
// This boundary covers every writer, including both halves of paired changes.
pub(crate) fn write_batches<'a, S>(
    table: &'a Table,
    batches: S,
    budget: &'a Budget,
) -> futures::future::BoxFuture<'a, Result<Vec<DataFile>>>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'a,
{
    write_batches_sized(table, batches, budget, budget.limits.max_batch_bytes)
}

pub(crate) fn write_batches_sized<'a, S>(
    table: &'a Table,
    batches: S,
    budget: &'a Budget,
    target_file_bytes: usize,
) -> futures::future::BoxFuture<'a, Result<Vec<DataFile>>>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'a,
{
    write_batches_inner(table, batches, budget, target_file_bytes, true)
}

// Lifecycle commits use the same bounded writer without data-progress noise.
pub(crate) fn write_catalog_batches<'a, S>(
    table: &'a Table,
    batches: S,
    budget: &'a Budget,
) -> futures::future::BoxFuture<'a, Result<Vec<DataFile>>>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'a,
{
    write_batches_inner(table, batches, budget, budget.limits.max_batch_bytes, false)
}

fn write_batches_inner<'a, S>(
    table: &'a Table,
    batches: S,
    budget: &'a Budget,
    target_file_bytes: usize,
    report_progress: bool,
) -> futures::future::BoxFuture<'a, Result<Vec<DataFile>>>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'a,
{
    Box::pin(async move {
        if target_file_bytes == 0 {
            return Err(Error::refusal(
                "invalid_compaction",
                "output file size must be positive",
            ));
        }
        let partition_spec = table.metadata().default_partition_spec().clone();
        tables::validate_partition_spec(table.metadata().current_schema(), &partition_spec)?;
        let is_partitioned = !partition_spec.fields().is_empty();
        // Parquet and rolling-file buffers survive individual writes. Keep their
        // admission for the entire writer lifetime, including close and flush.
        let _reservation = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(4))?;
        let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
        // The maintained rolling writer retains completed file descriptors and the
        // open Parquet footer. Bound both before admitting another batch. Chunk-only
        // truncated statistics keep variable-size values and page indexes from
        // turning a small descriptor count into an unbounded metadata allocation.
        let unit_metadata_bytes = table
            .metadata()
            .current_schema()
            .field_id_to_name_map()
            .values()
            .fold(2048_usize, |bytes, name| {
                bytes
                    .saturating_add(2048)
                    .saturating_add(name.len().saturating_mul(4))
            });
        let mut metadata_bytes = 0usize;
        let mut retained_bytes = 0usize;
        let mut completed = Vec::new();
        // `FanoutWriter` keeps one open Parquet writer, with its in-progress row
        // group, per partition touched since the last close. The reservation above
        // covers a bounded number of buffered batches, not one row group per
        // partition: a Day/Identity backfill over unsorted input could otherwise
        // buffer thousands of row groups. Close (finishing files) once more than
        // one partition is open and the bytes accepted since the last close exceed
        // two batch allowances. A single open partition is already bounded by its
        // row-group limit, so sorted or single-partition input never closes early.
        let fanout_bytes_limit = budget.limits.max_batch_bytes.saturating_mul(2);
        let mut open_partitions: std::collections::HashSet<iceberg::spec::Struct> =
            std::collections::HashSet::new();
        let mut open_bytes = 0usize;
        // Row groups track `batch_rows` so writer buffering stays inside the batch
        // admission reserved above; the 1,048,576-row writer default would buffer a
        // whole row group far beyond `max_batch_bytes`. One admitted batch is at
        // most `max_batch_bytes` (64 MiB) of decoded values, so a row group stays
        // under a ~128 MiB encoded target by admission; `set_max_row_group_bytes`
        // stays unset because a byte cap could otherwise exceed the rolling file
        // target and skew output file sizing. Level 3 is the recommended
        // production Zstandard default: smaller output than the crate level-1
        // default at still-modest CPU on hourly ingestion cadences.
        let expected_arrow =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
        let props_builder = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(budget.limits.batch_rows))
            .set_max_row_group_bytes(Some((budget.limits.max_batch_bytes / 2).max(1)))
            // One mebibyte data pages keep a page decodable per read request
            // without inflating the per-row-group page count admitted above.
            .set_data_page_size_limit(1024 * 1024)
            // Dictionary pages collapse repeated low-cardinality values; the
            // writer falls back transparently once its page budget is exceeded.
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .set_statistics_truncate_length(Some(STATISTICS_TRUNCATE_BYTES))
            .set_column_index_truncate_length(Some(STATISTICS_TRUNCATE_BYTES))
            .set_offset_index_disabled(true);
        let props = props_builder.build();
        let parquet = ParquetWriterBuilder::new(props, table.metadata().current_schema().clone())
            .with_match_mode(iceberg::arrow::FieldMatchMode::Name);
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            target_file_bytes,
            table.file_io().clone(),
            DataLocationGenerator(DefaultLocationGenerator::new(table.metadata())?),
            DefaultFileNameGenerator::new(
                uuid::Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let spec_id = partition_spec.spec_id();
        let new_writer = || {
            ActiveWriter::new(
                DataFileWriterBuilder::new(rolling.clone()),
                is_partitioned,
                spec_id,
            )
        };
        let mut writer = new_writer();
        let origin = format!("docspec:{}", uuid::Uuid::new_v4());
        let mut next_row = 0u64;
        let mut progress = report_progress.then(|| crate::progress::Writer::new(&origin));
        let result = async {
            futures::pin_mut!(batches);
            while let Some(raw_batch) = batches.try_next().await? {
                if raw_batch.num_rows() == 0 {
                    continue;
                }
                let raw_batch = coerce_batch(raw_batch)?;
                let write_split_limit = if budget.limits.max_batch_bytes >= (1 << 20)
                    && raw_batch.num_rows() > 1
                    && has_splittable_skewed_column(&raw_batch, budget.limits.max_batch_bytes)
                {
                    budget.limits.max_batch_bytes / 2
                } else {
                    budget.limits.max_batch_bytes
                };
                for batch in split_oversized_batch(raw_batch, write_split_limit)? {
                    let batch =
                        identity::materialize(batch, &origin, next_row, budget.limits.max_batch_bytes)?;
                    next_row = next_row
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| {
                            Error::refusal(
                                "occurrence_count",
                                "writer occurrence positions exceed the supported range",
                            )
                        })?;
                    let bytes = effective_batch_bytes(&batch);
                    if bytes > budget.limits.max_batch_bytes {
                        return Err(Error::refusal(
                            "oversized_batch",
                            format!(
                                "decoded batch uses {bytes} bytes, exceeding the {} byte allowance; reduce batch rows or split oversized values",
                                budget.limits.max_batch_bytes
                            ),
                        ));
                    }
                    // Literal expressions may be non-nullable even for optional native
                    // fields. Preserve the native physical nullability, without copying values.
                    let expected = table.metadata().current_schema();
                    let mut columns = batch.columns().to_vec();
                    let fields = batch
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            let native = expected.field_by_name(field.name()).ok_or_else(|| {
                                Error::refusal(
                                    "schema_mismatch",
                                    "writer field is absent from native schema",
                                )
                            })?;
                            let mut field = field.as_ref().clone().with_nullable(!native.required);
                            let target = expected_arrow.field_with_name(field.name())?.data_type();
                            // Iceberg stores zoned timestamps as UTC. Arrow can label that
                            // same instant with "UTC", "+00:00", or another zone; normalize
                            // only zone metadata, retaining the exact unit and epoch values.
                            let compatible_cast = match (field.data_type(), target) {
                                (
                                    arrow::datatypes::DataType::Timestamp(unit, Some(_)),
                                    arrow::datatypes::DataType::Timestamp(target_unit, Some(_)),
                                ) => unit == target_unit && field.data_type() != target,
                                (
                                    arrow::datatypes::DataType::Binary,
                                    arrow::datatypes::DataType::LargeBinary,
                                )
                                | (
                                    arrow::datatypes::DataType::LargeBinary,
                                    arrow::datatypes::DataType::Binary,
                                )
                                | (
                                    arrow::datatypes::DataType::Utf8,
                                    arrow::datatypes::DataType::LargeUtf8,
                                )
                                | (
                                    arrow::datatypes::DataType::LargeUtf8,
                                    arrow::datatypes::DataType::Utf8,
                                ) => true,
                                _ => false,
                            };
                            if compatible_cast {
                                columns[index] = arrow::compute::cast(&columns[index], target)?;
                                field = field.with_data_type(target.clone());
                            }
                            Ok(field)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let batch =
                        RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
                    let rows = batch.num_rows() as u64;
                    let slices = partition_batch(expected, &partition_spec, batch)?;
                    for (partition_key, sub_batch) in slices {
                        // A write can finish the preceding row group, create new groups, and
                        // roll a file. This conservative total also covers descriptors retained
                        // after the underlying file closes. It is admission, not an RSS estimate.
                        let units = sub_batch
                            .num_rows()
                            .div_ceil(budget.limits.batch_rows)
                            .saturating_add(1);
                        let incoming = units.saturating_mul(unit_metadata_bytes);
                        // Periodically finish the maintained writer, releasing its accumulated
                        // footers. Keep only bounded file descriptors, so this admission grows
                        // with output files instead of every row group in the entire population.
                        let slice_bytes = effective_batch_bytes(&sub_batch);
                        let fanout_pressure = partition_key.as_ref().is_some_and(|key| {
                            !open_partitions.is_empty()
                                && (open_partitions.len() > 1 || !open_partitions.contains(key.data()))
                                && open_bytes.saturating_add(slice_bytes) > fanout_bytes_limit
                        });
                        if metadata_bytes != 0
                            && (fanout_pressure
                                || metadata_bytes.saturating_add(incoming)
                                    > budget.limits.max_metadata_bytes / 2)
                        {
                            let files = writer.close().await?;
                            for file in &files {
                                retained_bytes = retained_bytes
                                    .saturating_add(unit_metadata_bytes)
                                    .saturating_add(file.file_path().len().saturating_mul(4))
                                    .saturating_add(
                                        file.split_offsets()
                                            .map_or(0, |offsets| offsets.len().saturating_mul(16)),
                                    );
                            }
                            completed.extend(files);
                            writer = new_writer();
                            metadata_bytes = 0;
                            open_partitions.clear();
                            open_bytes = 0;
                        }
                        if let Some(key) = &partition_key {
                            open_partitions.insert(key.data().clone());
                            open_bytes = open_bytes.saturating_add(slice_bytes);
                        }
                        metadata_bytes = metadata_bytes.saturating_add(incoming);
                        if retained_bytes.saturating_add(metadata_bytes)
                            > budget.limits.max_metadata_bytes
                        {
                            return Err(Error::refusal(
                                "metadata_capacity",
                                "native writer file and row-group metadata exceed the declared allowance",
                            ));
                        }
                        writer.write(expected, partition_key, sub_batch).await?;
                    }
                    if let Some(progress) = &mut progress {
                        progress.accepted(rows, bytes as u64);
                    }
                }
            }
            completed.extend(writer.close().await?);
            Ok(completed)
        }.await;
        if let Some(progress) = progress {
            progress.finish(&result);
        }
        result
    })
}

/// Returns whether `schema` contains a `List<Struct<...>>` column.
pub(crate) fn has_list_of_struct(schema: &arrow::datatypes::Schema) -> bool {
    schema.fields().iter().any(|field| {
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::List(item)
                if matches!(item.data_type(), arrow::datatypes::DataType::Struct(_))
        )
    })
}

/// Returns the referenced byte size of a `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary`
/// array slice across `num_rows` rows, or `None` for other array types.
fn referenced_variable_bytes(col: &dyn arrow::array::Array, num_rows: usize) -> Option<usize> {
    if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        let offsets = utf8.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i32>()))
    } else if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
        let offsets = utf8.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i64>()))
    } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        let offsets = bin.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i32>()))
    } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::LargeBinaryArray>() {
        let offsets = bin.value_offsets();
        let data = usize::try_from(offsets[num_rows] - offsets[0]).unwrap_or(0);
        Some(data.saturating_add((num_rows + 1) * std::mem::size_of::<i64>()))
    } else {
        None
    }
}

/// Returns whether `batch` contains a `List<Struct>` column, an uncompacted
/// variable-length slice whose backing buffer exceeds its referenced bytes, or a
/// multi-row batch with a skewed multi-megabyte `Utf8`/`Binary` cell (`> limit / 16`).
pub(crate) fn has_splittable_skewed_column(batch: &RecordBatch, limit: usize) -> bool {
    if has_list_of_struct(batch.schema().as_ref()) {
        return true;
    }
    let num_rows = batch.num_rows();
    if num_rows == 0 || limit < (1 << 20) {
        return false;
    }
    let cell_threshold = (limit / 16).max(1 << 20);
    for col in batch.columns() {
        if let Some(ref_bytes) = referenced_variable_bytes(col.as_ref(), num_rows)
            && col.get_array_memory_size() > ref_bytes.saturating_add(cell_threshold)
        {
            return true;
        }
        if num_rows <= 1 {
            continue;
        }
        if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            let offsets = utf8.value_offsets();
            if (0..num_rows).any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold) {
                return true;
            }
        } else if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
            let offsets = utf8.value_offsets();
            if (0..num_rows).any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold) {
                return true;
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
            let offsets = bin.value_offsets();
            if (0..num_rows).any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold) {
                return true;
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::LargeBinaryArray>() {
            let offsets = bin.value_offsets();
            if (0..num_rows).any(|r| usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0) > cell_threshold) {
                return true;
            }
        }
    }
    false
}

/// Computes the effective admission byte size of `batch`.
///
/// For multi-row batches, returns `batch.get_array_memory_size()`.
/// For a 1-row batch:
/// - bounds `List<Struct>` columns by their per-occurrence element size, and
/// - uses referenced slice bytes for `Utf8`/`Binary` columns so uncompacted slices
///   are recognized before deep compaction.
pub(crate) fn effective_batch_bytes(batch: &RecordBatch) -> usize {
    if batch.num_rows() != 1 {
        return batch.get_array_memory_size();
    }
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(col, field)| {
            if let arrow::datatypes::DataType::List(item) = field.data_type()
                && matches!(item.data_type(), arrow::datatypes::DataType::Struct(_))
                && let Some(list) = col.as_any().downcast_ref::<arrow::array::ListArray>()
            {
                let elems = (list.value_length(0).max(1)) as usize;
                col.get_array_memory_size().div_ceil(elems)
            } else if let Some(ref_bytes) = referenced_variable_bytes(col.as_ref(), 1) {
                ref_bytes
            } else {
                col.get_array_memory_size()
            }
        })
        .fold(0usize, usize::saturating_add)
}

/// Produces a deep-compacted slice `[offset..offset + length]` of `batch` whose
/// backing Arrow buffers retain only the values referenced by those rows.
///
/// Note: `arrow::compute::concat_batches` has a `batches.len() == 1` fast path
/// that returns `batches[0].clone()` without reallocating buffers. Passing an
/// trailing 0-row batch forces `MutableArrayData` compaction into fresh buffers.
pub(crate) fn compact_batch_slice(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> Result<RecordBatch> {
    let sliced = batch.slice(offset, length);
    if sliced.num_columns() == 0 {
        return Ok(sliced);
    }
    let schema = batch.schema();
    let empty = RecordBatch::new_empty(schema.clone());
    Ok(arrow::compute::concat_batches(&schema, &[sliced, empty])?)
}

/// Splits a multi-row `RecordBatch` containing `List<Struct>` columns into
/// deep-compacted row slices that fit within `limit` in a single linear pass
/// by per-row byte weight so each slice is compacted at most once.
pub(crate) fn split_oversized_batch(
    batch: RecordBatch,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    if batch.get_array_memory_size() <= limit {
        return Ok(vec![batch]);
    }
    if !has_splittable_skewed_column(&batch, limit) {
        let bytes = batch.get_array_memory_size();
        return Err(Error::refusal(
            "oversized_batch",
            format!(
                "decoded batch uses {bytes} bytes, exceeding the {limit} byte allowance; reduce batch rows or split oversized values"
            ),
        ));
    }
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        let compacted = if num_rows == 1
            && batch.get_array_memory_size() > effective_batch_bytes(&batch)
        {
            compact_batch_slice(&batch, 0, 1)?
        } else {
            batch
        };
        let effective = effective_batch_bytes(&compacted);
        if effective > limit {
            return Err(Error::refusal(
                "oversized_batch",
                format!(
                    "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                ),
            ));
        }
        return Ok(vec![compacted]);
    }

    // Estimate per-row byte weights in a single linear pass across columns.
    let mut row_weights = vec![0usize; num_rows];
    for col in batch.columns() {
        let col_bytes = col.get_array_memory_size();
        if let Some(list) = col.as_any().downcast_ref::<arrow::array::ListArray>() {
            let offsets = list.value_offsets();
            let total_elems = usize::try_from(offsets[num_rows] - offsets[0])
                .unwrap_or(0)
                .max(1);
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let elems = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                let share = col_bytes.saturating_mul(elems).div_ceil(total_elems);
                *weight = weight.saturating_add(share.saturating_add(8));
            }
        } else if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            let offsets = utf8.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(8));
            }
        } else if let Some(utf8) = col.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
            let offsets = utf8.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(12));
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
            let offsets = bin.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(8));
            }
        } else if let Some(bin) = col.as_any().downcast_ref::<arrow::array::LargeBinaryArray>() {
            let offsets = bin.value_offsets();
            for (r, weight) in row_weights.iter_mut().enumerate() {
                let bytes = usize::try_from(offsets[r + 1] - offsets[r]).unwrap_or(0);
                *weight = weight.saturating_add(bytes.saturating_add(12));
            }
        } else {
            let per_row = col_bytes.div_ceil(num_rows);
            for weight in &mut row_weights {
                *weight = weight.saturating_add(per_row);
            }
        }
    }

    let mut ranges = Vec::new();
    let mut slice_start = 0usize;
    let mut acc_bytes = 0usize;
    for (r, &w) in row_weights.iter().enumerate() {
        if r > slice_start && acc_bytes.saturating_add(w) > limit {
            ranges.push((slice_start, r - slice_start));
            slice_start = r;
            acc_bytes = w;
        } else {
            acc_bytes = acc_bytes.saturating_add(w);
        }
    }
    if slice_start < num_rows {
        ranges.push((slice_start, num_rows - slice_start));
    }

    let mut out = Vec::with_capacity(ranges.len());
    for (start, len) in ranges {
        let slice = compact_batch_slice(&batch, start, len)?;
        if slice.get_array_memory_size() <= limit {
            out.push(slice);
        } else if slice.num_rows() <= 1 {
            let effective = effective_batch_bytes(&slice);
            if effective > limit {
                return Err(Error::refusal(
                    "oversized_batch",
                    format!(
                        "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                    ),
                ));
            }
            out.push(slice);
        } else {
            for r in 0..slice.num_rows() {
                let one = compact_batch_slice(&batch, start + r, 1)?;
                let effective = effective_batch_bytes(&one);
                if effective > limit {
                    return Err(Error::refusal(
                        "oversized_batch",
                        format!(
                            "single-row batch uses {effective} effective bytes, exceeding the {limit} byte allowance"
                        ),
                    ));
                }
                out.push(one);
            }
        }
    }
    Ok(out)
}

// Keep physical data file paths flat beneath `data/` while recording exact
// partition tuples in each DataFile manifest descriptor. This preserves
// canonical object locator and local removal path invariants for all values.
#[derive(Clone, Debug)]
struct DataLocationGenerator(DefaultLocationGenerator);

impl iceberg::writer::file_writer::location_generator::LocationGenerator for DataLocationGenerator {
    fn generate_location(
        &self,
        _partition_key: Option<&iceberg::spec::PartitionKey>,
        file_name: &str,
    ) -> String {
        self.0.generate_location(None, file_name)
    }
}

enum ActiveWriterInner<B: iceberg::writer::IcebergWriterBuilder> {
    Unpartitioned(UnpartitionedWriter<B>),
    Partitioned(iceberg::writer::partitioning::fanout_writer::FanoutWriter<B>),
}

struct ActiveWriter<B: iceberg::writer::IcebergWriterBuilder> {
    inner: ActiveWriterInner<B>,
    truncated_fields: BTreeSet<i32>,
    spec_id: i32,
}

impl<B: iceberg::writer::IcebergWriterBuilder> ActiveWriter<B> {
    fn new(builder: B, partitioned: bool, spec_id: i32) -> Self {
        let inner = if partitioned {
            ActiveWriterInner::Partitioned(
                iceberg::writer::partitioning::fanout_writer::FanoutWriter::new(builder),
            )
        } else {
            ActiveWriterInner::Unpartitioned(UnpartitionedWriter::new(builder))
        };
        Self {
            inner,
            truncated_fields: BTreeSet::new(),
            spec_id,
        }
    }

    async fn write(
        &mut self,
        schema: &iceberg::spec::Schema,
        partition_key: Option<iceberg::spec::PartitionKey>,
        batch: RecordBatch,
    ) -> iceberg::Result<()> {
        use iceberg::writer::partitioning::PartitioningWriter;
        for (index, field) in batch.schema().fields().iter().enumerate() {
            if let Some(native_field) = schema.field_by_name(field.name())
                && !self.truncated_fields.contains(&native_field.id)
                && column_has_truncated_stat_value(batch.column(index).as_ref())
            {
                self.truncated_fields.insert(native_field.id);
            }
        }
        match (&mut self.inner, partition_key) {
            (ActiveWriterInner::Unpartitioned(writer), None) => writer.write(batch).await,
            (ActiveWriterInner::Partitioned(writer), Some(key)) => writer.write(key, batch).await,
            _ => unreachable!("writer mode matches partition key presence"),
        }
    }

    async fn close(self) -> iceberg::Result<Vec<DataFile>> {
        use iceberg::writer::partitioning::PartitioningWriter;
        let files = match self.inner {
            ActiveWriterInner::Unpartitioned(writer) => writer.close().await?,
            ActiveWriterInner::Partitioned(writer) => writer.close().await?,
        };
        if self.truncated_fields.is_empty() {
            return Ok(files);
        }
        files
            .into_iter()
            .map(|file| strip_truncated_bounds(file, &self.truncated_fields, self.spec_id))
            .collect()
    }
}

fn column_has_truncated_stat_value(array: &dyn arrow::array::Array) -> bool {
    use arrow::array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
    let any = array.as_any();
    if let Some(arr) = any.downcast_ref::<StringArray>() {
        (0..arr.len()).any(|i| !arr.is_null(i) && arr.value(i).len() >= STATISTICS_TRUNCATE_BYTES)
    } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
        (0..arr.len()).any(|i| !arr.is_null(i) && arr.value(i).len() >= STATISTICS_TRUNCATE_BYTES)
    } else if let Some(arr) = any.downcast_ref::<BinaryArray>() {
        (0..arr.len()).any(|i| !arr.is_null(i) && arr.value(i).len() >= STATISTICS_TRUNCATE_BYTES)
    } else if let Some(arr) = any.downcast_ref::<LargeBinaryArray>() {
        (0..arr.len()).any(|i| !arr.is_null(i) && arr.value(i).len() >= STATISTICS_TRUNCATE_BYTES)
    } else {
        false
    }
}

fn strip_truncated_bounds(
    file: DataFile,
    truncated_fields: &BTreeSet<i32>,
    spec_id: i32,
) -> iceberg::Result<DataFile> {
    if !file
        .lower_bounds()
        .keys()
        .any(|id| truncated_fields.contains(id))
        && !file
            .upper_bounds()
            .keys()
            .any(|id| truncated_fields.contains(id))
    {
        return Ok(file);
    }
    let lower_bounds = file
        .lower_bounds()
        .iter()
        .filter(|(id, _)| !truncated_fields.contains(id))
        .map(|(id, datum)| (*id, datum.clone()))
        .collect();
    let upper_bounds = file
        .upper_bounds()
        .iter()
        .filter(|(id, _)| !truncated_fields.contains(id))
        .map(|(id, datum)| (*id, datum.clone()))
        .collect();
    let mut builder = iceberg::spec::DataFileBuilder::default();
    builder
        .content(file.content_type())
        .file_path(file.file_path().to_owned())
        .file_format(file.file_format())
        .partition(file.partition().clone())
        .record_count(file.record_count())
        .file_size_in_bytes(file.file_size_in_bytes())
        .column_sizes(file.column_sizes().clone())
        .value_counts(file.value_counts().clone())
        .null_value_counts(file.null_value_counts().clone())
        .nan_value_counts(file.nan_value_counts().clone())
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .key_metadata(file.key_metadata().map(ToOwned::to_owned))
        .split_offsets(file.split_offsets().map(ToOwned::to_owned))
        .equality_ids(file.equality_ids())
        .first_row_id(file.first_row_id())
        .partition_spec_id(spec_id)
        .referenced_data_file(file.referenced_data_file())
        .content_offset(file.content_offset())
        .content_size_in_bytes(file.content_size_in_bytes());
    if let Some(sort_order_id) = file.sort_order_id() {
        builder.sort_order_id(sort_order_id);
    }
    builder.build().map_err(|err| {
        iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("failed to rebuild data file descriptor: {err}"),
        )
    })
}

fn partition_batch(
    schema: &iceberg::spec::SchemaRef,
    spec: &iceberg::spec::PartitionSpecRef,
    batch: RecordBatch,
) -> Result<Vec<(Option<iceberg::spec::PartitionKey>, RecordBatch)>> {
    if spec.fields().is_empty() {
        return Ok(vec![(None, batch)]);
    }
    let num_rows = batch.num_rows();
    let mut field_values = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema
            .as_struct()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                Error::refusal(
                    "unsupported_write_layout",
                    "partition spec references a missing top-level field",
                )
            })?;
        let col_idx = batch.schema().index_of(&source.name).map_err(|_| {
            Error::refusal(
                "schema_mismatch",
                "writer batch omits a required partition source column",
            )
        })?;
        field_values.push(compute_partition_column(
            field.transform,
            batch.column(col_idx),
        )?);
    }
    let mut groups: Vec<(iceberg::spec::Struct, Vec<u32>)> = Vec::new();
    let mut group_index: HashMap<iceberg::spec::Struct, usize> = HashMap::new();
    for row_idx in 0..num_rows {
        let partition =
            iceberg::spec::Struct::from_iter(field_values.iter().map(|col| col[row_idx].clone()));
        let idx = u32::try_from(row_idx)
            .map_err(|_| Error::refusal("oversized_batch", "batch row count exceeds u32 range"))?;
        if let Some(&pos) = group_index.get(&partition) {
            groups[pos].1.push(idx);
        } else {
            let pos = groups.len();
            group_index.insert(partition.clone(), pos);
            groups.push((partition, vec![idx]));
        }
    }
    if groups.len() == 1 {
        let (partition, _) = groups.pop().expect("single partition group");
        let key =
            iceberg::spec::PartitionKey::new(spec.as_ref().clone(), schema.clone(), partition);
        return Ok(vec![(Some(key), batch)]);
    }
    let mut out = Vec::with_capacity(groups.len());
    for (partition, indices) in groups {
        let indices = arrow::array::UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|col| Ok(arrow::compute::take(col.as_ref(), &indices, None)?))
            .collect::<Result<Vec<_>>>()?;
        let sub_batch = RecordBatch::try_new(batch.schema(), columns)?;
        let key =
            iceberg::spec::PartitionKey::new(spec.as_ref().clone(), schema.clone(), partition);
        out.push((Some(key), sub_batch));
    }
    Ok(out)
}

pub(crate) fn compute_partition_column(
    transform: iceberg::spec::Transform,
    column: &arrow::array::ArrayRef,
) -> Result<Vec<Option<iceberg::spec::Literal>>> {
    use arrow::{
        array::{
            Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Int32Array, Int64Array,
            LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
            TimestampMicrosecondArray,
        },
        datatypes::{DataType, TimeUnit},
    };
    use iceberg::spec::{Literal, Transform};

    let rows = column.len();
    let mut out = Vec::with_capacity(rows);
    match (transform, column.data_type()) {
        (Transform::Year, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push(
                    (!arr.is_null(i))
                        .then(|| Literal::int(days_to_years_since_epoch(arr.value(i)))),
                );
            }
        }
        (Transform::Year, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::int(days_to_years_since_epoch(days))));
                }
            }
        }
        (Transform::Month, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push(
                    (!arr.is_null(i))
                        .then(|| Literal::int(days_to_months_since_epoch(arr.value(i)))),
                );
            }
        }
        (Transform::Month, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::int(days_to_months_since_epoch(days))));
                }
            }
        }
        (Transform::Day, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::date(arr.value(i))));
            }
        }
        (Transform::Day, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let days = i32::try_from(arr.value(i).div_euclid(86_400_000_000))
                        .map_err(|_| unsupported_partition_column())?;
                    out.push(Some(Literal::date(days)));
                }
            }
        }
        (Transform::Identity, DataType::Boolean) => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::bool(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Date32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::date(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Int32) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::int(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Int64) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::long(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Utf8) => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::LargeUtf8) => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Utf8View) => {
            let arr = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::string(arr.value(i))));
            }
        }
        (Transform::Identity, DataType::Timestamp(TimeUnit::Microsecond, tz)) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                if arr.is_null(i) {
                    out.push(None);
                } else if tz.is_some() {
                    out.push(Some(Literal::timestamptz(arr.value(i))));
                } else {
                    out.push(Some(Literal::timestamp(arr.value(i))));
                }
            }
        }
        (Transform::Identity, DataType::Binary) => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Identity, DataType::LargeBinary) => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Identity, DataType::BinaryView) => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!arr.is_null(i)).then(|| Literal::binary(arr.value(i).to_vec())));
            }
        }
        (Transform::Bucket(n), _) if n > 0 => {
            let normalized = match column.data_type() {
                DataType::LargeUtf8 | DataType::Utf8View => arrow::compute::cast(column, &DataType::Utf8)
                    .map_err(|_| unsupported_partition_column())?,
                DataType::LargeBinary | DataType::BinaryView => {
                    arrow::compute::cast(column, &DataType::Binary)
                        .map_err(|_| unsupported_partition_column())?
                }
                _ => column.clone(),
            };
            let tf = iceberg::transform::create_transform_function(&transform)
                .map_err(|_| unsupported_partition_column())?;
            let transformed = tf
                .transform(normalized)
                .map_err(|_| unsupported_partition_column())?;
            let arr = transformed
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(unsupported_partition_column)?;
            for i in 0..rows {
                out.push((!column.is_null(i) && !arr.is_null(i)).then(|| Literal::int(arr.value(i))));
            }
        }
        (Transform::Truncate(w), _) if w > 0 => {
            let normalized = match column.data_type() {
                DataType::LargeUtf8 | DataType::Utf8View => arrow::compute::cast(column, &DataType::Utf8)
                    .map_err(|_| unsupported_partition_column())?,
                DataType::LargeBinary | DataType::BinaryView => {
                    arrow::compute::cast(column, &DataType::Binary)
                        .map_err(|_| unsupported_partition_column())?
                }
                _ => column.clone(),
            };
            let tf = iceberg::transform::create_transform_function(&transform)
                .map_err(|_| unsupported_partition_column())?;
            let transformed = tf
                .transform(normalized)
                .map_err(|_| unsupported_partition_column())?;
            return compute_partition_column(Transform::Identity, &transformed);
        }
        _ => return Err(unsupported_partition_column()),
    }
    Ok(out)
}

fn unsupported_partition_column() -> Error {
    Error::refusal(
        "unsupported_write_layout",
        "partition source column type does not match a qualified Iceberg transform",
    )
}

/// Converts proleptic Gregorian days since `1970-01-01` into years since `1970`.
pub(crate) fn days_to_years_since_epoch(days: i32) -> i32 {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year - 1970) as i32
}

/// Converts proleptic Gregorian days since `1970-01-01` into months since `1970-01`.
pub(crate) fn days_to_months_since_epoch(days: i32) -> i32 {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    ((year - 1970) * 12 + (m - 1)) as i32
}

/// Returns whether `transform` preserves inequality ordering (`a <= b => T(a) <= T(b)`).
pub(crate) fn is_order_preserving_transform(transform: iceberg::spec::Transform) -> bool {
    matches!(
        transform,
        iceberg::spec::Transform::Identity
            | iceberg::spec::Transform::Year
            | iceberg::spec::Transform::Month
            | iceberg::spec::Transform::Day
            | iceberg::spec::Transform::Truncate(_)
    )
}

/// Computes the partition literal for a single primitive source value under `transform`.
pub(crate) fn compute_partition_literal(
    transform: iceberg::spec::Transform,
    source_type: &iceberg::spec::Type,
    lit: &iceberg::spec::PrimitiveLiteral,
) -> Option<iceberg::spec::PrimitiveLiteral> {
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type};

    let Type::Primitive(prim) = source_type else {
        return None;
    };
    let array: ArrayRef = match (prim, lit) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => {
            Arc::new(BooleanArray::from(vec![*v]))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Arc::new(Int32Array::from(vec![*v])),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Arc::new(Int64Array::from(vec![*v])),
        (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Arc::new(Date32Array::from(vec![*v])),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]).with_timezone("+00:00"))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(v)) => {
            Arc::new(StringArray::from(vec![v.as_str()]))
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(v)) => {
            Arc::new(BinaryArray::from(vec![v.as_slice()]))
        }
        _ => return None,
    };
    match compute_partition_column(transform, &array)
        .ok()?
        .into_iter()
        .next()
        .flatten()?
    {
        Literal::Primitive(p) => Some(p),
        _ => None,
    }
}

pub(crate) async fn append(
    table: &Table,
    catalog: &dyn Catalog,
    files: Vec<DataFile>,
) -> Result<Table> {
    if files.is_empty() {
        return Ok(table.clone());
    }
    // Reuse the lifecycle boundary: native refresh must never adopt a newer
    // candidate and append the same computed output to it a second time.
    let guard = crate::catalog::GuardedCatalog::new(catalog, table);
    let tx = Transaction::new(table);
    Ok(tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)?
        .commit(&guard)
        .await?)
}

/// Hashes immutable bytes using bounded reads.
///
/// Retries interrupted reads through the maintained BLAKE3 reader.
///
/// # Errors
/// Returns filesystem failures without interpreting partial files as complete.
pub fn digest(path: &Path) -> Result<(String, u64)> {
    digest_reader(File::open(path)?)
}

fn digest_reader(input: impl Read) -> Result<(String, u64)> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(input)?;
    Ok((hasher.finalize().to_hex().to_string(), hasher.count()))
}

pub(crate) fn object(owner: &Id, path: &Path) -> Result<ObjectRef> {
    let (digest, bytes) = digest(path)?;
    Ok(ObjectRef {
        owner: owner.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    })
}

/// Verifies exact identity; file existence or a readable footer is insufficient.
///
/// # Errors
/// Refuses changed, missing, or unreadable bytes.
pub fn verify_object(object: &ObjectRef) -> Result<()> {
    #[cfg(feature = "remote")]
    if let Some(remote) = remote::RemoteObjects::for_uri(&object.path) {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::refusal(
                "runtime_required",
                "remote object verification requires a Tokio runtime",
            )
        })?;
        let actual = handle
            .block_on(remote.identity_admitted(&object.owner, &object.path))
            .map_err(|_| {
                Error::refusal(
                    "corrupt_object",
                    format!("object identity differs for owner {}", object.owner),
                )
            })?;
        if actual.digest != object.digest || actual.bytes != object.bytes {
            return Err(Error::refusal(
                "corrupt_object",
                format!("object identity differs for owner {}", object.owner),
            ));
        }
        return Ok(());
    }
    let (digest, bytes) = digest(Path::new(&object.path))?;
    if digest != object.digest || bytes != object.bytes {
        return Err(Error::refusal(
            "corrupt_object",
            format!("object identity differs for owner {}", object.owner),
        ));
    }
    Ok(())
}

pub(crate) fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = OpenOptions::new().write(true).create_new(true).open(path)?;
    out.write_all(bytes)?;
    out.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(crate) fn copy_verified(
    owner: &Id,
    source: &Path,
    destination: &Path,
    expected: &str,
) -> Result<ObjectRef> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)?;
    let mut hash = blake3::Hasher::new();
    let mut bytes = 0;
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        output.write_all(&buffer[..n])?;
        bytes += n as u64;
    }
    output.sync_all()?;
    if let Some(parent) = destination.parent() {
        File::open(parent)?.sync_all()?;
    }
    let digest = hash.finalize().to_hex().to_string();
    if digest != expected {
        return Err(Error::refusal(
            "input_digest_mismatch",
            "supplied bytes do not match the selected immutable input",
        ));
    }
    Ok(ObjectRef {
        owner: owner.clone(),
        path: destination.to_string_lossy().into_owned(),
        digest,
        bytes,
    })
}

/// Native metadata and live file descriptors for an exact snapshot.
#[derive(Debug)]
pub(crate) struct SnapshotFiles {
    pub paths: BTreeSet<String>,
    pub data: Vec<DataFile>,
    _reservation: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) async fn snapshot_files(table: &Table, budget: &Budget) -> Result<SnapshotFiles> {
    let memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    snapshot_files_guarded(table, budget, None, memory).await
}

pub(crate) async fn current_snapshot_files(
    table: &Table,
    budget: &Budget,
) -> Result<SnapshotFiles> {
    let memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    snapshot_files_guarded_with_scope(table, budget, None, memory, false).await
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ManifestDecodeCount {
    pub manifest_lists: u64,
    pub manifests: u64,
}

#[cfg(test)]
fn manifest_decode_tracker() -> &'static std::sync::Mutex<BTreeMap<uuid::Uuid, ManifestDecodeCount>>
{
    static TRACKER: std::sync::OnceLock<
        std::sync::Mutex<BTreeMap<uuid::Uuid, ManifestDecodeCount>>,
    > = std::sync::OnceLock::new();
    TRACKER.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn reset_manifest_decode_count(table_uuid: uuid::Uuid) {
    manifest_decode_tracker()
        .lock()
        .expect("manifest decode tracker poisoned")
        .insert(table_uuid, ManifestDecodeCount::default());
}

#[cfg(test)]
pub(crate) fn manifest_decode_count(table_uuid: uuid::Uuid) -> ManifestDecodeCount {
    manifest_decode_tracker()
        .lock()
        .expect("manifest decode tracker poisoned")
        .get(&table_uuid)
        .copied()
        .unwrap_or_default()
}

/// Verifies every partition literal of a [`DataFile`] against its qualified transform.
///
/// Year, Month, Day, and Bucket transforms store integer ordinals; Identity and
/// Truncate store primitive values. Every field is checked.
pub(crate) fn validate_partition_tuple(
    spec: &iceberg::spec::PartitionSpec,
    file: &DataFile,
) -> Result<()> {
    use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
    if file.partition().fields().len() != spec.fields().len() {
        return Err(Error::refusal(
            "unsupported_write_layout",
            "snapshot data file partition tuple does not match its partition spec",
        ));
    }
    for (idx, field) in spec.fields().iter().enumerate() {
        let valid = matches!(
            (&field.transform, &file.partition()[idx]),
            (_, None)
                | (
                    Transform::Year | Transform::Month | Transform::Day | Transform::Bucket(_),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(_))),
                )
                | (
                    Transform::Identity | Transform::Truncate(_),
                    Some(Literal::Primitive(_))
                )
        );
        if !valid {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "partition literal does not match its qualified transform",
            ));
        }
    }
    Ok(())
}

async fn snapshot_files_guarded(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
) -> Result<SnapshotFiles> {
    snapshot_files_guarded_with_scope(table, budget, guard, reservation, true).await
}

async fn snapshot_files_guarded_with_scope(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
    include_retained_history: bool,
) -> Result<SnapshotFiles> {
    tables::validate_partition_spec(
        table.metadata().current_schema(),
        table.metadata().default_partition_spec(),
    )?;
    let check = |path: &str| -> Result<()> {
        if let Some(guard) = guard {
            guard.check(path)?;
        }
        Ok(())
    };
    check(table.metadata_location_result()?)?;
    let mut paths = BTreeSet::from([table.metadata_location_result()?.to_owned()]);
    let mut current_data = BTreeMap::new();
    let mut metadata_bytes = 0u64;
    let mut visited_manifests = BTreeSet::new();
    for statistics in table.metadata().statistics_iter() {
        check(&statistics.statistics_path)?;
        paths.insert(statistics.statistics_path.clone());
    }
    for statistics in table.metadata().partition_statistics_iter() {
        check(&statistics.statistics_path)?;
        paths.insert(statistics.statistics_path.clone());
    }
    // Retain every snapshot referenced by this exact metadata file at creation
    // and retention boundaries; current-snapshot occurrence capture inspects
    // only the live current snapshot.
    let snapshots =
        table
            .metadata()
            .current_snapshot()
            .into_iter()
            .chain(table.metadata().snapshots().filter(move |snapshot| {
                include_retained_history
                    && Some(snapshot.snapshot_id()) != table.metadata().current_snapshot_id()
            }));
    for snapshot in snapshots {
        let is_current = Some(snapshot.snapshot_id()) == table.metadata().current_snapshot_id();
        let list_path = snapshot.manifest_list();
        check(list_path)?;
        metadata_bytes = metadata_bytes
            .saturating_add(table.file_io().new_input(list_path)?.metadata().await?.size);
        check_inventory_size(&paths, metadata_bytes, budget)?;
        paths.insert(list_path.to_owned());
        let list = table.manifest_list_reader(snapshot).load().await?;
        #[cfg(test)]
        {
            manifest_decode_tracker()
                .lock()
                .expect("manifest decode tracker poisoned")
                .entry(table.metadata().uuid())
                .or_default()
                .manifest_lists += 1;
        }
        for entry in list.entries() {
            check(&entry.manifest_path)?;
            if !visited_manifests.insert(entry.manifest_path.clone()) {
                if is_current {
                    return Err(Error::refusal(
                        "duplicate_manifest",
                        "current snapshot repeats a manifest",
                    ));
                }
                continue;
            }
            metadata_bytes = metadata_bytes.saturating_add(
                table
                    .file_io()
                    .new_input(&entry.manifest_path)?
                    .metadata()
                    .await?
                    .size,
            );
            check_inventory_size(&paths, metadata_bytes, budget)?;
            paths.insert(entry.manifest_path.clone());
            let manifest = entry.load_manifest(table.file_io()).await?;
            #[cfg(test)]
            {
                manifest_decode_tracker()
                    .lock()
                    .expect("manifest decode tracker poisoned")
                    .entry(table.metadata().uuid())
                    .or_default()
                    .manifests += 1;
            }
            let current_spec = if is_current {
                let Some(spec) = table
                    .metadata()
                    .partition_spec_by_id(entry.partition_spec_id)
                else {
                    return Err(Error::refusal(
                        "unsupported_write_layout",
                        "snapshot manifest references an unknown partition spec",
                    ));
                };
                tables::validate_partition_spec(table.metadata().current_schema(), spec)?;
                tables::validate_partition_spec(
                    manifest.metadata().schema(),
                    manifest.metadata().partition_spec(),
                )?;
                Some(spec)
            } else {
                None
            };
            for item in manifest.entries().iter().filter(|item| item.is_alive()) {
                let file = item.data_file();
                check(file.file_path())?;
                paths.insert(file.file_path().to_owned());
                if let Some(spec) = current_spec {
                    if file.content_type() != iceberg::spec::DataContentType::Data {
                        return Err(Error::refusal(
                            "unsupported_deletes",
                            "candidate file sharing requires materialized membership without native delete files",
                        ));
                    }
                    if file.partition().fields().len() != spec.fields().len() {
                        return Err(Error::refusal(
                            "unsupported_write_layout",
                            "snapshot data file partition tuple does not match its partition spec",
                        ));
                    }
                    validate_partition_tuple(spec, file)?;
                    if current_data
                        .insert(file.file_path().to_owned(), file.clone())
                        .is_some()
                    {
                        return Err(Error::refusal(
                            "duplicate_file",
                            "snapshot contains the same live data file more than once",
                        ));
                    }
                }
                check_inventory_size(&paths, metadata_bytes, budget)?;
            }
        }
    }
    check_inventory_size(&paths, metadata_bytes, budget)?;
    Ok(SnapshotFiles {
        paths,
        data: current_data.into_values().collect(),
        _reservation: reservation,
    })
}

fn check_inventory_size(
    paths: &BTreeSet<String>,
    metadata_bytes: u64,
    budget: &Budget,
) -> Result<()> {
    // Native Avro decoding has additional allocations; this is bounded metadata
    // admission, not a qualified RSS ceiling. Account conservatively per object.
    let entries = paths.len().saturating_mul(512);
    if metadata_bytes > budget.limits.max_metadata_bytes as u64
        || entries > budget.limits.max_metadata_bytes
    {
        return Err(Error::refusal(
            "metadata_capacity",
            "snapshot inventory exceeds metadata admission",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn pin(
    owner: &Id,
    table: &Table,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
) -> Result<TablePin> {
    pin_bound(owner, &PinTable::local(table), rows, budget, shared).await
}

pub(crate) async fn pin_bound(
    owner: &Id,
    binding: &PinTable,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
) -> Result<TablePin> {
    let table = &binding.table;
    // Admit the complete inventory before copying captured shared identities.
    let memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    let object_io = binding.objects.clone();
    let mut inherited = BTreeMap::new();
    for object in shared {
        if inherited
            .insert(object.path.clone(), object.clone())
            .is_some_and(|previous| previous != *object)
        {
            return Err(Error::refusal(
                "conflicting_owner",
                "one shared path binds conflicting owner or content identities",
            ));
        }
    }
    let remote_root = if object_io.is_remote() {
        Some(object_io.pin_root(table.metadata().location())?)
    } else {
        None
    };
    let guard = remote_root.as_ref().map(|root| object_io::PinPaths {
        objects: &object_io,
        root,
        inherited: &inherited,
    });
    let inventory = snapshot_files_guarded(table, budget, guard.as_ref(), memory).await?;
    let native_rows = inventory
        .data
        .iter()
        .try_fold(0u64, |sum, file| sum.checked_add(file.record_count()))
        .ok_or_else(|| {
            Error::refusal(
                "row_count_overflow",
                "native record counts exceed the supported range",
            )
        })?;
    if native_rows != rows {
        return Err(Error::refusal(
            "row_count_mismatch",
            "native live file counts differ from prepared output",
        ));
    }
    let owner = owner.clone();
    let location = table.metadata().location().to_owned();
    let metadata_path = table.metadata_location_result()?.to_owned();
    let name = table.identifier().name().to_owned();
    let snapshot = table.metadata().current_snapshot_id();
    let schema_id = table.metadata().current_schema_id();
    let memory = budget.reserve(object_io.read_allowance())?;
    let handle = tokio::runtime::Handle::current();
    budget
        .blocking_admitted(move || {
            let _memory = memory;
            let inventory = inventory;
            let root = match remote_root {
                Some(root) => root,
                None => object_io.pin_root(&location)?,
            };
            let mut objects = Vec::new();
            for path in &inventory.paths {
                if let Some(shared) = inherited.get(path) {
                    object_io.check_available_admitted(shared, &handle)?;
                    objects.push(shared.clone());
                } else {
                    root.check_new_path(path)?;
                    objects.push(object_io.identity_admitted(&owner, path, &handle)?);
                }
            }
            let metadata = objects
                .iter()
                .find(|object| object.path == metadata_path)
                .cloned()
                .ok_or_else(|| {
                    Error::refusal(
                        "incomplete_inventory",
                        "native metadata is absent from snapshot inventory",
                    )
                })?;
            Ok(TablePin {
                table: name,
                metadata,
                snapshot,
                schema_id,
                objects,
                rows,
                logical_metadata: None,
            })
        })
        .await
}

pub(crate) async fn check_inventory(pin: &TablePin, table: &Table, budget: &Budget) -> Result<()> {
    let objects = Objects::for_path(&pin.metadata.path);
    if objects.is_remote() {
        let (_, inventory) = objects.open_snapshot(pin, budget).await?;
        return check_inventory_files(pin, &inventory);
    }
    let inventory = snapshot_files(table, budget).await?;
    check_inventory_files(pin, &inventory)
}

pub(crate) fn check_inventory_files(pin: &TablePin, inventory: &SnapshotFiles) -> Result<()> {
    let pinned = pin
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect::<BTreeSet<_>>();
    if !pin.objects.contains(&pin.metadata)
        || inventory
            .paths
            .iter()
            .any(|path| !pinned.contains(path.as_str()))
    {
        return Err(Error::refusal(
            "incomplete_inventory",
            "release omits an object referenced by its native metadata",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    fn data_file_month_partition(
        spec: &iceberg::spec::PartitionSpec,
        file: &DataFile,
    ) -> Result<Option<i32>> {
        use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
        validate_partition_tuple(spec, file)?;
        let mut months = spec
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.transform == Transform::Month);
        let Some((idx, _)) = months.next() else {
            return Ok(None);
        };
        if months.next().is_some() {
            return Err(Error::refusal(
                "unsupported_write_layout",
                "month pruning requires exactly one month partition field",
            ));
        }
        Ok(match &file.partition()[idx] {
            Some(Literal::Primitive(PrimitiveLiteral::Int(month))) => Some(*month),
            _ => None,
        })
    }

    fn assert_json_digest<T: serde::Serialize + ?Sized>(value: &T) {
        let bytes = serde_json::to_vec(value).unwrap();
        assert_eq!(json_digest(value).unwrap(), blake3::hash(&bytes));
    }

    #[test]
    fn json_digest_preserves_typed_request_serialization() {
        use crate::{export::ExportSpec, model::GroupDefinition};

        // Freeze compact struct ordering as well as compare the previous digest path.
        let definition_json = r#"{"group":"classification","definition":"révision-東京","fields":[{"id":10,"name":"label\nquoted\"","expression":"coalesce(text, '🦀')"}]}"#;
        let definition: GroupDefinition = serde_json::from_str(definition_json).unwrap();
        assert_eq!(
            serde_json::to_vec(&definition).unwrap(),
            definition_json.as_bytes()
        );
        assert_json_digest(&definition);
        let processor = "docspec.scalar-backfill.v2";
        assert_json_digest(&(processor, &definition));
        let bindings = BTreeMap::from([("text", 2), ("language", 3)]);
        assert_json_digest(&(
            processor,
            &definition,
            bindings,
            vec![serde_json::json!({"name": "évidence", "nested": [null, true, 3]})],
        ));

        let export_json = r#"{"dataset":"comments","operation":"export-1","source":{"dataset":"comments","version":"v1","manifest":{"owner":"comments","path":"/retained/東京/manifest.json","digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bytes":123}},"channel":"download","expected":null,"policy":{"group":"source","rows":"all","columns":["id","résumé"]}}"#;
        let request: ExportSpec = serde_json::from_str(export_json).unwrap();
        assert_eq!(
            serde_json::to_vec(&request).unwrap(),
            export_json.as_bytes()
        );
        assert_json_digest(&request);
        assert_json_digest(&(&request.source.manifest, "dependency-digest"));
    }

    #[test]
    fn json_digest_preserves_nested_maps_unicode_and_unsized_values() {
        let nested = BTreeMap::from([
            (
                "z-last",
                serde_json::json!({"array": [null, true, -42, 1.25]}),
            ),
            (
                "a-first",
                serde_json::json!({"text": "é東京🦀\n\t\"\\", "empty": {}}),
            ),
        ]);
        assert_json_digest(&nested);
        assert_json_digest(&serde_json::Value::Null);
        assert_json_digest(&[] as &[u64]);
        assert_json_digest(&[0, 1, u64::MAX] as &[u64]);
        assert_json_digest("é東京🦀\n\t\"\\");
        // Cross BLAKE3 chunk boundaries without depending on serializer write sizes.
        assert_json_digest(&"é東京🦀".repeat(1024));
    }

    #[test]
    fn json_digest_preserves_serialization_errors() {
        struct FailAfterField;

        impl serde::Serialize for FailAfterField {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeStruct;
                let mut fields = serializer.serialize_struct("FailAfterField", 2)?;
                fields.serialize_field("first", "already serialized")?;
                Err(serde::ser::Error::custom("injected serialization failure"))
            }
        }

        fn compare_errors(value: &impl serde::Serialize) {
            let buffered = serde_json::to_vec(value).unwrap_err();
            let streamed = json_digest(value).unwrap_err();
            assert_eq!(streamed.classify(), buffered.classify());
            assert_eq!(streamed.line(), buffered.line());
            assert_eq!(streamed.column(), buffered.column());
            assert_eq!(streamed.to_string(), buffered.to_string());
            let buffered = Error::from(buffered);
            let streamed = Error::from(streamed);
            assert_eq!(streamed.code(), buffered.code());
            assert_eq!(streamed.code(), "invalid_json");
            assert_eq!(
                std::error::Error::source(&streamed).unwrap().to_string(),
                std::error::Error::source(&buffered).unwrap().to_string()
            );
        }

        compare_errors(&FailAfterField);
        compare_errors(&BTreeMap::from([((1, 2), "invalid JSON map key")]));
    }

    #[test]
    fn digest_matches_complete_bytes_for_empty_and_multi_buffer_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("bytes");
        // Cross both the maintained reader's buffer and the previous loop's buffer.
        for size in [0, 1, 65_535, 65_536, 65_537, 2 * 1024 * 1024 + 17] {
            let bytes: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                digest(&path).unwrap(),
                (blake3::hash(&bytes).to_hex().to_string(), size as u64)
            );
        }
    }

    #[test]
    fn digest_reader_retries_interruptions_and_accepts_short_reads() {
        struct InterruptedReader<'a> {
            remaining: &'a [u8],
            interrupt: bool,
        }

        impl Read for InterruptedReader<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if self.interrupt {
                    self.interrupt = false;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                self.interrupt = true;
                let limit = buffer.len().min(7);
                self.remaining.read(&mut buffer[..limit])
            }
        }

        let bytes = b"short reads and repeated interruptions preserve every byte";
        let reader = InterruptedReader {
            remaining: bytes,
            interrupt: true,
        };
        assert_eq!(
            digest_reader(reader).unwrap(),
            (blake3::hash(bytes).to_hex().to_string(), bytes.len() as u64)
        );
    }

    #[test]
    fn digest_reader_preserves_io_failure_after_partial_input() {
        struct FailedReader;

        impl Read for FailedReader {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected read failure",
                ))
            }
        }

        let reader = std::io::Cursor::new(b"partial bytes").chain(FailedReader);
        let error = digest_reader(reader).unwrap_err();
        assert_eq!(error.code(), "io");
        let cause = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(cause.to_string(), "injected read failure");
    }

    #[test]
    fn digest_preserves_file_open_failure() {
        let root = tempfile::tempdir().unwrap();
        let error = digest(&root.path().join("missing")).unwrap_err();
        assert_eq!(error.code(), "io");
        let cause = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn canceled_inventory_checks_keep_worker_owned_metadata_admission() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            for full in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("retained");
                std::fs::write(&path, b"retained bytes").unwrap();
                let object = object(&Id::new("owner").unwrap(), &path).unwrap();
                let budget = Budget::new(crate::budget::Limits {
                    memory_bytes: 8 << 20,
                    max_metadata_bytes: 1 << 20,
                    compute_threads: 1,
                    ..crate::budget::Limits::default()
                })
                .unwrap();
                let (started, ready) = tokio::sync::oneshot::channel();
                let (resume, paused) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    started.send(()).unwrap();
                    let _ = paused.recv();
                });
                ready.await.unwrap();
                let worker_budget = budget.clone();
                let caller = tokio::spawn(async move {
                    if full {
                        verify_objects(vec![object], &worker_budget).await
                    } else {
                        check_available(vec![object], &worker_budget).await
                    }
                });
                let except_io_buffer = budget.limits.memory_bytes / 2 - (1 << 20);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while budget.reserve(except_io_buffer).is_ok() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
                assert!(
                    budget.reserve(except_io_buffer).is_err(),
                    "queued worker still owns inventory beyond its fixed I/O buffer"
                );
                resume.send(()).unwrap();
                blocker.await.unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while budget.reserve(budget.limits.memory_bytes / 2).is_err() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
        });
    }

    fn batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    async fn values(pin: &TablePin, budget: &Budget) -> Vec<Option<i64>> {
        let table = open_pin(pin, budget).await.unwrap();
        let stream = read_batches(&table, budget).await.unwrap();
        futures::pin_mut!(stream);
        let mut values = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter(),
            );
        }
        values.sort();
        values
    }

    #[tokio::test]
    async fn candidate_snapshots_share_unchanged_files_with_complete_owned_inventories() {
        let root = tempfile::TempDir::new().unwrap();
        let catalog = local_catalog(root.path()).await.unwrap();
        let namespace = iceberg::NamespaceIdent::new("sharing".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let budget = Budget::new(crate::budget::Limits::default()).unwrap();
        let owner = Id::new("source").unwrap();
        let derived_owner = Id::new("derived").unwrap();
        let first = batch(vec![Some(1), Some(1)]);
        let schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(first.schema().as_ref())
                .unwrap();
        let source = catalog
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name("source".into())
                    .location(root.path().join("source").to_string_lossy().into_owned())
                    .schema(schema.clone())
                    .properties(table_properties())
                    .build(),
            )
            .await
            .unwrap();
        let original_files = write_batches(&source, futures::stream::iter([Ok(first)]), &budget)
            .await
            .unwrap();
        let source = append(&source, catalog.as_ref(), original_files)
            .await
            .unwrap();
        let second = batch(vec![Some(2), None]);
        let stable = write_batches(&source, futures::stream::iter([Ok(second)]), &budget)
            .await
            .unwrap();
        assert_eq!(stable.len(), 1);
        let stable_path = stable[0].file_path().to_owned();
        let stable_before = digest(Path::new(&stable_path)).unwrap();
        let stable_modified = std::fs::metadata(&stable_path).unwrap().modified().unwrap();
        let source = append(&source, catalog.as_ref(), stable.clone())
            .await
            .unwrap();
        let original = pin(&owner, &source, 4, &budget, &[]).await.unwrap();
        let unique_metadata_bytes = original
            .objects
            .iter()
            .filter(|object| object.path.ends_with(".avro"))
            .map(|object| object.bytes as usize)
            .sum::<usize>();
        let inventory_limit = unique_metadata_bytes.max(original.objects.len() * 512);
        let tight = Budget::new(crate::budget::Limits {
            max_metadata_bytes: inventory_limit,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        // The earlier snapshot repeats a manifest. Charge/read each immutable
        // manifest once, while preserving every retained snapshot's references.
        assert_eq!(snapshot_files(&source, &tight).await.unwrap().data.len(), 2);

        let candidate = catalog
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name("candidate".into())
                    .location(root.path().join("candidate").to_string_lossy().into_owned())
                    .schema(schema)
                    .properties(table_properties())
                    .build(),
            )
            .await
            .unwrap();
        let replacement = batch(vec![Some(3), Some(3)]);
        let mut candidate_files = write_batches(
            &candidate,
            futures::stream::iter([Ok(replacement)]),
            &budget,
        )
        .await
        .unwrap();
        candidate_files.extend(stable);
        let candidate = append(&candidate, catalog.as_ref(), candidate_files)
            .await
            .unwrap();
        assert_eq!(
            pin(&derived_owner, &candidate, 4, &budget, &[])
                .await
                .unwrap_err()
                .code(),
            "unprotected_reference"
        );
        let mut conflicting = original.objects.clone();
        let mut conflict = original
            .objects
            .iter()
            .find(|object| object.path == stable_path)
            .unwrap()
            .clone();
        conflict.owner = derived_owner.clone();
        conflicting.push(conflict);
        assert_eq!(
            pin(&derived_owner, &candidate, 4, &budget, &conflicting)
                .await
                .unwrap_err()
                .code(),
            "conflicting_owner"
        );
        let result = pin(&derived_owner, &candidate, 4, &budget, &original.objects)
            .await
            .unwrap();
        assert_ne!(result.snapshot, original.snapshot);
        assert_eq!(result.schema_id, original.schema_id);
        assert_eq!(
            result
                .objects
                .iter()
                .find(|object| object.path == stable_path)
                .unwrap()
                .owner,
            owner
        );
        assert_eq!(digest(Path::new(&stable_path)).unwrap(), stable_before);
        assert_eq!(
            std::fs::metadata(&stable_path).unwrap().modified().unwrap(),
            stable_modified
        );
        let original_data = snapshot_files(&source, &budget).await.unwrap();
        let replaced = original_data
            .data
            .iter()
            .find(|file| file.file_path() != stable_path)
            .unwrap();
        assert!(
            !result
                .objects
                .iter()
                .any(|object| object.path == replaced.file_path())
        );
        // An opaque statistics sidecar tests reference closure only; this does
        // not claim qualification of the Puffin statistics format itself.
        let statistics_path = root.path().join("external-statistics.puffin");
        write_new(&statistics_path, b"opaque sidecar").unwrap();
        let statistics = object(&owner, &statistics_path).unwrap();
        let tx = Transaction::new(&candidate);
        let with_statistics = tx
            .update_statistics()
            .set_statistics(iceberg::spec::StatisticsFile {
                snapshot_id: candidate.metadata().current_snapshot_id().unwrap(),
                statistics_path: statistics.path.clone(),
                file_size_in_bytes: statistics.bytes as i64,
                file_footer_size_in_bytes: 0,
                key_metadata: None,
                blob_metadata: vec![],
            })
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
        assert_eq!(
            pin(
                &derived_owner,
                &with_statistics,
                4,
                &budget,
                &original.objects
            )
            .await
            .unwrap_err()
            .code(),
            "unprotected_reference"
        );
        let mut inherited = original.objects.clone();
        inherited.push(statistics.clone());
        let mut statistics_pin = pin(&derived_owner, &with_statistics, 4, &budget, &inherited)
            .await
            .unwrap();
        assert!(statistics_pin.objects.contains(&statistics));
        statistics_pin
            .objects
            .retain(|object| object.path != statistics.path);
        assert_eq!(
            crate::engine::verify_table(&statistics_pin, &budget)
                .await
                .unwrap_err()
                .code(),
            "incomplete_inventory"
        );
        drop(catalog);
        std::fs::rename(
            root.path().join("catalog.sqlite"),
            root.path().join("catalog.unavailable"),
        )
        .unwrap();
        crate::engine::verify_table(&original, &budget)
            .await
            .unwrap();
        crate::engine::verify_table(&result, &budget).await.unwrap();
        assert_eq!(
            values(&original, &budget).await,
            vec![None, Some(1), Some(1), Some(2)]
        );
        assert_eq!(
            values(&result, &budget).await,
            vec![None, Some(2), Some(3), Some(3)]
        );

        let mut incomplete = result.clone();
        incomplete
            .objects
            .retain(|object| object.path != stable_path);
        assert_eq!(
            crate::engine::verify_table(&incomplete, &budget)
                .await
                .unwrap_err()
                .code(),
            "incomplete_inventory"
        );
    }

    #[test]
    fn month_pruning_refuses_ambiguous_specs_and_validates_every_partition_field() {
        use iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, Literal, NestedField, PartitionSpec,
            PrimitiveType, Struct, Transform, Type,
        };
        let schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_fields([
                    Arc::new(NestedField::required(
                        1,
                        "a",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "b",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let two_months = PartitionSpec::builder(schema.clone())
            .add_partition_field("a", "a_month", Transform::Month)
            .unwrap()
            .add_partition_field("b", "b_month", Transform::Month)
            .unwrap()
            .build()
            .unwrap();
        let file = |partition: Struct| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("memory://f.parquet".into())
                .file_size_in_bytes(1)
                .record_count(1)
                .partition(partition)
                .build()
                .unwrap()
        };
        let valid = file(Struct::from_iter([
            Some(Literal::int(648)),
            Some(Literal::int(649)),
        ]));
        validate_partition_tuple(&two_months, &valid).unwrap();
        // Choosing the first month field would prune by an arbitrary column.
        assert_eq!(
            data_file_month_partition(&two_months, &valid)
                .unwrap_err()
                .code(),
            "unsupported_write_layout"
        );
        // The second month literal was previously never inspected.
        let malformed = file(Struct::from_iter([
            Some(Literal::int(648)),
            Some(Literal::string("x")),
        ]));
        assert_eq!(
            validate_partition_tuple(&two_months, &malformed)
                .unwrap_err()
                .code(),
            "unsupported_write_layout"
        );
        let one_month = PartitionSpec::builder(schema)
            .add_partition_field("a", "a_month", Transform::Month)
            .unwrap()
            .build()
            .unwrap();
        let single = file(Struct::from_iter([Some(Literal::int(648))]));
        assert_eq!(
            data_file_month_partition(&one_month, &single).unwrap(),
            Some(648)
        );
    }

    #[tokio::test]
    async fn interleaved_partitions_close_open_writers_within_fanout_allowance() {
        use arrow::array::{Date32Array, StringArray};
        use iceberg::{
            TableIdent,
            spec::{NestedField, PrimitiveType, Transform, Type, UnboundPartitionSpec},
        };

        let root = tempfile::TempDir::new().unwrap();
        let catalog = local_catalog(root.path()).await.unwrap();
        let namespace = iceberg::NamespaceIdent::new("fanout".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let limits = crate::budget::Limits {
            max_batch_bytes: 256 << 10,
            ..crate::budget::Limits::default()
        };
        let budget = Budget::new(limits).unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields([
                Arc::new(NestedField::required(
                    10,
                    "observed_at",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    20,
                    "payload",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    40,
                    identity::ORIGIN_FILE,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    41,
                    identity::ORIGIN_ROW,
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(10, "observed_at_day", Transform::Day)
            .unwrap()
            .build();
        let table_dir = root.path().join("events");
        let table = register_candidate(
            catalog.as_ref(),
            &TableIdent::new(namespace.clone(), "events".into()),
            CandidateWriter::Local,
            move || fresh_partitioned_metadata(&schema, &table_dir, spec),
            &budget,
        )
        .await
        .unwrap();
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("observed_at", DataType::Date32, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new(identity::ORIGIN_FILE, DataType::Utf8, true),
            Field::new(identity::ORIGIN_ROW, DataType::Int64, true),
        ]));
        // Every batch interleaves two days, so both partition writers stay open.
        // Eight ~110 KiB batches exceed the 512 KiB (2 x batch) fanout allowance.
        let rows_per_batch = 1000;
        let batches = (0..8)
            .map(|_| {
                let days = (0..rows_per_batch)
                    .map(|row| 19_723 + (row % 2) as i32)
                    .collect::<Vec<_>>();
                let payload = (0..rows_per_batch)
                    .map(|row| format!("{row:0>100}"))
                    .collect::<Vec<_>>();
                Ok(RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![
                        Arc::new(Date32Array::from(days)),
                        Arc::new(StringArray::from(payload)),
                        Arc::new(StringArray::new_null(rows_per_batch)),
                        Arc::new(arrow::array::Int64Array::new_null(rows_per_batch)),
                    ],
                )
                .unwrap())
            })
            .collect::<Vec<_>>();
        let files = write_batches(&table, futures::stream::iter(batches), &budget)
            .await
            .unwrap();
        let rows: u64 = files.iter().map(DataFile::record_count).sum();
        assert_eq!(rows, 8 * rows_per_batch as u64);
        // Without the allowance one writer per day stays open for the whole stream
        // (exactly two files). Bounded fanout finishes files part-way through.
        assert!(files.len() > 2, "{} files", files.len());
        for file in &files {
            assert_eq!(file.partition().fields().len(), 1);
        }
    }

    #[tokio::test]
    async fn month_partitioned_batches_write_distinct_data_files_prune_by_month_and_preserve_occurrences()
     {
        use arrow::{
            array::{Date32Array, StringArray, TimestampMicrosecondArray},
            datatypes::TimeUnit,
        };
        use iceberg::{
            TableIdent,
            spec::{
                Literal, NestedField, PrimitiveType, Struct, Transform, Type, UnboundPartitionSpec,
            },
        };

        // Verify days_to_months_since_epoch across epoch, leap years, and negative dates.
        assert_eq!(days_to_months_since_epoch(0), 0); // 1970-01-01
        assert_eq!(days_to_months_since_epoch(-1), -1); // 1969-12-31
        assert_eq!(days_to_months_since_epoch(19_723), 648); // 2024-01-01
        assert_eq!(days_to_months_since_epoch(19_782), 649); // 2024-02-29 (leap day)
        assert_eq!(days_to_months_since_epoch(19_783), 650); // 2024-03-01

        let root = tempfile::TempDir::new().unwrap();
        let catalog = local_catalog(root.path()).await.unwrap();
        let namespace = iceberg::NamespaceIdent::new("news_lake".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let budget = Budget::new(crate::budget::Limits::default()).unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields([
                Arc::new(NestedField::required(
                    10,
                    "article_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    20,
                    "observed_at",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    30,
                    "recorded_at",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::optional(
                    40,
                    identity::ORIGIN_FILE,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    41,
                    identity::ORIGIN_ROW,
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(20, "observed_at_month", Transform::Month)
            .unwrap()
            .build();
        let table_dir = root.path().join("articles");
        let schema_for_create = schema.clone();
        let spec_for_create = spec.clone();
        let dir_for_create = table_dir.clone();
        let table = register_candidate(
            catalog.as_ref(),
            &TableIdent::new(namespace.clone(), "articles".into()),
            CandidateWriter::Local,
            move || {
                fresh_partitioned_metadata(&schema_for_create, &dir_for_create, spec_for_create)
            },
            &budget,
        )
        .await
        .unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("article_id", DataType::Int64, false),
            Field::new("observed_at", DataType::Date32, false),
            Field::new(
                "recorded_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(identity::ORIGIN_FILE, DataType::Utf8, true),
            Field::new(identity::ORIGIN_ROW, DataType::Int64, true),
        ]));
        // Interleave rows from 2024-01 (days 19723, 19730 -> month 648),
        // 2024-02 (day 19782 -> month 649), and 2024-03 (day 19783 -> month 650)
        // in a single batch to exercise per-partition splitting and occurrence retention.
        let mixed_batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![101, 201, 102, 301])),
                Arc::new(Date32Array::from(vec![19_723, 19_782, 19_730, 19_783])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(19_723_i64 * 86_400_000_000),
                        Some(19_782_i64 * 86_400_000_000),
                        Some(19_730_i64 * 86_400_000_000),
                        Some(19_783_i64 * 86_400_000_000),
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![
                    Some("origin:jan-0"),
                    None,
                    None,
                    Some("origin:mar-0"),
                ])),
                Arc::new(Int64Array::from(vec![Some(7), None, None, Some(99)])),
            ],
        )
        .unwrap();

        let written = write_batches(&table, futures::stream::iter([Ok(mixed_batch)]), &budget)
            .await
            .unwrap();
        assert_eq!(written.len(), 3);
        let mut by_month = BTreeMap::new();
        for file in &written {
            let month = data_file_month_partition(table.metadata().default_partition_spec(), file)
                .unwrap()
                .unwrap();
            assert_eq!(
                file.partition(),
                &Struct::from_iter([Some(Literal::int(month))])
            );
            by_month.insert(month, file.record_count());
        }
        assert_eq!(by_month, BTreeMap::from([(648, 2), (649, 1), (650, 1)]));

        let table = append(&table, catalog.as_ref(), written).await.unwrap();
        let owner = Id::new("news_lake").unwrap();
        let pinned = pin(&owner, &table, 4, &budget, &[]).await.unwrap();
        assert_eq!(pinned.rows, 4);

        // Verify OccurrenceScan and read_occurrences preserve exact values and occurrence identities.
        let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
        assert_eq!(scan.files().count(), 3);
        let stream = read_occurrences(&table, &budget).await.unwrap();
        futures::pin_mut!(stream);
        let mut rows = BTreeMap::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let ids = batch
                .column_by_name("article_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let dates = batch
                .column_by_name("observed_at")
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            let files = batch
                .column_by_name(OCCURRENCE_FILE)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let positions = batch
                .column_by_name(OCCURRENCE_ROW)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.insert(
                    ids.value(i),
                    (
                        dates.value(i),
                        files.value(i).to_owned(),
                        positions.value(i),
                    ),
                );
            }
        }
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[&101].0, 19_723);
        assert_eq!(rows[&101].1, "origin:jan-0");
        assert_eq!(rows[&101].2, 7);
        assert_eq!(rows[&301].0, 19_783);
        assert_eq!(rows[&301].1, "origin:mar-0");
        assert_eq!(rows[&301].2, 99);
        // Fallback rows 201 (input index 1) and 102 (input index 2) share the same generated
        // writer origin and keep their exact pre-split input positions 1 and 2.
        assert_eq!(rows[&201].0, 19_782);
        assert_eq!(rows[&102].0, 19_730);
        assert_eq!(rows[&201].1, rows[&102].1);
        assert!(rows[&201].1.starts_with("docspec:"));
        assert_eq!(rows[&201].2, 1);
        assert_eq!(rows[&102].2, 2);

        // Verify replacement candidate preserves the Month partition spec and shares unchanged month files.
        let replacement_dir = root.path().join("articles_replacement");
        let base_meta = table.metadata().clone();
        let rep_dir = replacement_dir.clone();
        let candidate = register_candidate(
            catalog.as_ref(),
            &TableIdent::new(namespace, "articles_replacement".into()),
            CandidateWriter::Local,
            move || replacement_metadata(&base_meta, &rep_dir),
            &budget,
        )
        .await
        .unwrap();
        assert_eq!(
            candidate.metadata().default_partition_spec(),
            table.metadata().default_partition_spec()
        );
        let jan_feb: Vec<_> = current_snapshot_files(&table, &budget)
            .await
            .unwrap()
            .data
            .into_iter()
            .filter(|file| {
                data_file_month_partition(table.metadata().default_partition_spec(), file)
                    .unwrap()
                    .unwrap()
                    <= 649
            })
            .collect();
        assert_eq!(jan_feb.len(), 2);
        let candidate = append(&candidate, catalog.as_ref(), jan_feb).await.unwrap();
        let candidate_pin = pin(&owner, &candidate, 3, &budget, &pinned.objects)
            .await
            .unwrap();
        assert_eq!(candidate_pin.rows, 3);
    }

    #[tokio::test]
    async fn occurrence_scan_reuses_admitted_current_snapshot_across_retained_history() {
        let root = tempfile::TempDir::new().unwrap();
        let catalog = local_catalog(root.path()).await.unwrap();
        let namespace = iceberg::NamespaceIdent::new("planning_reuse".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let budget = Budget::new(crate::budget::Limits::default()).unwrap();
        let owner = Id::new("planning_reuse").unwrap();
        let sample = batch(vec![Some(10), Some(20)]);
        let native_schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(sample.schema().as_ref())
                .unwrap();

        for historical_snapshots in [0usize, 8, 32] {
            let location = root.path().join(format!("history_{historical_snapshots}"));
            let mut historical = Vec::with_capacity(historical_snapshots);
            for index in 0..historical_snapshots {
                let stage_meta = fresh_metadata(native_schema.clone(), &location).unwrap();
                let stage = register_candidate(
                    catalog.as_ref(),
                    &iceberg::TableIdent::new(
                        namespace.clone(),
                        format!("stage_{historical_snapshots}_{index}"),
                    ),
                    CandidateWriter::Local,
                    move || Ok(stage_meta),
                    &budget,
                )
                .await
                .unwrap();
                let files = write_batches(
                    &stage,
                    futures::stream::iter([Ok(batch(vec![Some(index as i64)]))]),
                    &budget,
                )
                .await
                .unwrap();
                let stage = append(&stage, catalog.as_ref(), files).await.unwrap();
                let snap = stage.metadata().current_snapshot().unwrap();
                historical.push(
                    iceberg::spec::Snapshot::builder()
                        .with_snapshot_id((index + 1) as i64)
                        .with_sequence_number((index + 1) as i64)
                        .with_timestamp_ms(snap.timestamp_ms() + index as i64)
                        .with_manifest_list(snap.manifest_list().to_owned())
                        .with_summary(snap.summary().clone())
                        .with_schema_id(snap.schema_id().unwrap())
                        .build(),
                );
            }

            let base_meta = fresh_metadata(native_schema.clone(), &location).unwrap();
            let mut builder = base_meta.into_builder(None);
            for snap in historical {
                builder = builder.add_snapshot(snap).unwrap();
            }
            let seeded_meta = builder.build().unwrap().metadata;
            let table = register_candidate(
                catalog.as_ref(),
                &iceberg::TableIdent::new(
                    namespace.clone(),
                    format!("table_{historical_snapshots}"),
                ),
                CandidateWriter::Local,
                move || Ok(seeded_meta),
                &budget,
            )
            .await
            .unwrap();

            // Write 2 fixed current files (2 rows each = 4 rows total) in 1 current manifest.
            let mut current_files = Vec::new();
            for values in [vec![Some(1), Some(2)], vec![Some(3), None]] {
                let files =
                    write_batches(&table, futures::stream::iter([Ok(batch(values))]), &budget)
                        .await
                        .unwrap();
                assert_eq!(files.len(), 1);
                current_files.extend(files);
            }
            let table = append(&table, catalog.as_ref(), current_files)
                .await
                .unwrap();
            assert_eq!(
                table.metadata().snapshots().count(),
                historical_snapshots + 1
            );

            // 1. Creation/pinning/retention boundary (`snapshot_files`) still verifies
            // full object closure across all retained snapshots (1 + historical_snapshots).
            reset_manifest_decode_count(table.metadata().uuid());
            let closure_start = std::time::Instant::now();
            let inventory = snapshot_files(&table, &budget).await.unwrap();
            let closure_elapsed = closure_start.elapsed();
            let closure_counts = manifest_decode_count(table.metadata().uuid());
            assert_eq!(
                closure_counts,
                ManifestDecodeCount {
                    manifest_lists: (historical_snapshots + 1) as u64,
                    manifests: (historical_snapshots + 1) as u64,
                }
            );
            assert_eq!(inventory.data.len(), 2);
            let pinned = pin(&owner, &table, 4, &budget, &[]).await.unwrap();
            assert_eq!(pinned.rows, 4);

            // 2. OccurrenceScan::open decodes ONLY the current snapshot's manifest list (1)
            // and manifest (1) once, regardless of retained snapshot history (0 / 8 / 32).
            reset_manifest_decode_count(table.metadata().uuid());
            let open_start = std::time::Instant::now();
            let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
            let open_elapsed = open_start.elapsed();
            let open_counts = manifest_decode_count(table.metadata().uuid());
            assert_eq!(
                open_counts,
                ManifestDecodeCount {
                    manifest_lists: 1,
                    manifests: 1,
                }
            );
            assert_eq!(scan.files().count(), 2);

            // 3. read_occurrences also decodes only the current snapshot once (1 list, 1 manifest).
            reset_manifest_decode_count(table.metadata().uuid());
            let stream = read_occurrences(&table, &budget).await.unwrap();
            futures::pin_mut!(stream);
            let mut total_rows = 0usize;
            while let Some(batch) = stream.try_next().await.unwrap() {
                total_rows += batch.num_rows();
            }
            assert_eq!(total_rows, 4);
            assert_eq!(
                manifest_decode_count(table.metadata().uuid()),
                ManifestDecodeCount {
                    manifest_lists: 1,
                    manifests: 1,
                }
            );

            // 4. OccurrenceScan::values reuses an already-admitted SnapshotFiles with
            // zero additional manifest-list or manifest decodes, even after the current
            // snapshot's manifest list is removed from disk.
            let current_manifest_list = table
                .metadata()
                .current_snapshot()
                .unwrap()
                .manifest_list()
                .to_owned();
            std::fs::remove_file(&current_manifest_list).unwrap();
            reset_manifest_decode_count(table.metadata().uuid());
            let values_start = std::time::Instant::now();
            let values_scan = OccurrenceScan::values(&table, &budget, inventory)
                .await
                .unwrap();
            let values_elapsed = values_start.elapsed();
            assert_eq!(
                manifest_decode_count(table.metadata().uuid()),
                ManifestDecodeCount {
                    manifest_lists: 0,
                    manifests: 0,
                }
            );
            let mut values_rows = 0usize;
            for file in values_scan.files() {
                let stream = values_scan.read_file(file.file_path(), &budget).unwrap();
                futures::pin_mut!(stream);
                while let Some(batch) = stream.try_next().await.unwrap() {
                    values_rows += batch.num_rows();
                }
            }
            assert_eq!(values_rows, 4);
            eprintln!(
                "history={historical_snapshots}: closure_counts={closure_counts:?} ({closure_elapsed:?}), open_counts={open_counts:?} ({open_elapsed:?}), values_elapsed={values_elapsed:?}"
            );
        }
    }

    #[test]
    fn split_oversized_batch_splits_skewed_utf8_and_compacts_single_row_slices() {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let limit = 4 * 1024 * 1024; // 4 MiB
        let big_a = "A".repeat(2_500_000);
        let big_b = "B".repeat(2_500_000);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
                Arc::new(StringArray::from(vec!["small-0", &big_a, "small-2", &big_b])),
            ],
        )
        .unwrap();
        assert!(batch.get_array_memory_size() > limit);

        // 1. Multi-row batch with two 2.5 MB Utf8 cells splits cleanly under 4 MiB per slice.
        let slices = split_oversized_batch(batch.clone(), limit).unwrap();
        assert!(slices.len() >= 2);
        let total_rows: usize = slices.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 4);
        for slice in &slices {
            assert!(
                slice.get_array_memory_size() <= limit,
                "slice memory {} exceeded limit {}",
                slice.get_array_memory_size(),
                limit
            );
        }

        // 2. A 1-row zero-copy slice of the 5+ MB batch still shares the parent buffer
        // until split_oversized_batch compacts it via compact_batch_slice.
        let uncompacted_one = batch.slice(1, 1);
        assert!(uncompacted_one.get_array_memory_size() > limit);
        let compacted_slices = split_oversized_batch(uncompacted_one, limit).unwrap();
        assert_eq!(compacted_slices.len(), 1);
        assert_eq!(compacted_slices[0].num_rows(), 1);
        assert!(compacted_slices[0].get_array_memory_size() <= limit);
    }
}
