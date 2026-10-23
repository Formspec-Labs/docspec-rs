//! Bounded local objects and maintained Iceberg readers, writers, and commit actions.
// Rust guideline compliant 2026-02-21

mod batch;
pub(crate) use batch::{
    arrow_schema_to_schema_auto_assign_ids, coerce_batch, coerce_data_type, compact_batch_slice,
    effective_batch_bytes, has_list_of_struct, has_splittable_skewed_column, split_oversized_batch,
};
pub(crate) mod defaults;
mod flat;
pub(crate) mod identity;
mod inspection;
mod object_io;
mod occurrences;
pub(crate) use object_io::{Objects, PinTable, valid_object_locator};
mod partitioning;
#[cfg(test)]
pub(crate) use partitioning::days_to_months_since_epoch;
pub(crate) use partitioning::{
    compute_partition_literal, is_order_preserving_transform, partition_batch,
    validate_partition_tuple,
};
mod pinned;
#[cfg(test)]
pub(crate) use pinned::pin;
pub(crate) use pinned::{
    check_inventory, check_inventory_files, decode as decode_native_metadata, pin_bound,
    pin_bound_with_files,
};
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
pub(crate) use tables::{
    CandidateWriter, bounded_json, build_unbound_partition_spec, check_partition_by_matches,
    default_partition_field_name, fresh_metadata, fresh_metadata_at, fresh_partitioned_metadata,
    fresh_partitioned_metadata_at, partition_specs_from_metadata, register_candidate,
    replacement_metadata, replacement_metadata_at, validate_partition_spec,
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
    if object_io::is_remote_uri(&object.path) {
        return Err(object_io::unregistered_remote_uri());
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
        let is_remote = table.metadata().location().starts_with("s3://");
        let target_file_bytes = if is_remote {
            target_file_bytes.min(2 << 20)
        } else {
            target_file_bytes
        };
        let effective_batch_rows = if is_remote {
            budget.limits.batch_rows.clamp(1, 2048)
        } else {
            budget.limits.batch_rows
        };
        let max_row_group_bytes = if is_remote {
            (budget.limits.max_batch_bytes / 2).clamp(1, 2 << 20)
        } else {
            (budget.limits.max_batch_bytes / 2).max(1)
        };
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
            .set_max_row_group_row_count(Some(effective_batch_rows))
            .set_max_row_group_bytes(Some(max_row_group_bytes))
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
                                    arrow::datatypes::DataType::Int8
                                    | arrow::datatypes::DataType::Int16
                                    | arrow::datatypes::DataType::UInt8
                                    | arrow::datatypes::DataType::UInt16,
                                    arrow::datatypes::DataType::Int32,
                                )
                                | (
                                    arrow::datatypes::DataType::UInt32,
                                    arrow::datatypes::DataType::Int64,
                                )
                                | (
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
                        if is_remote && sub_batch.num_rows() > effective_batch_rows {
                            let mut offset = 0usize;
                            while offset < sub_batch.num_rows() {
                                let len = (sub_batch.num_rows() - offset).min(effective_batch_rows);
                                let chunk = sub_batch.slice(offset, len);
                                writer.write(expected, partition_key.clone(), chunk).await?;
                                offset += len;
                            }
                        } else {
                            writer.write(expected, partition_key, sub_batch).await?;
                        }
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

fn is_transient_gateway_commit_error(error: &iceberg::Error) -> bool {
    let msg = error.to_string().to_ascii_lowercase();
    msg.contains("commit state is unknown")
        || msg.contains("gateway or proxy received an invalid response")
        || msg.contains("429")
        || msg.contains("toomanyrequestsexception")
        || msg.contains("rate limit")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
}

async fn current_snapshot_added_files_match(table: &Table, expected: &[DataFile]) -> bool {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return false;
    };
    let Ok(list) = table.manifest_list_reader(snapshot).load().await else {
        return false;
    };
    let expected_paths: BTreeSet<&str> = expected.iter().map(DataFile::file_path).collect();
    let mut added_paths = BTreeSet::new();
    for entry in list
        .entries()
        .iter()
        .filter(|entry| entry.added_snapshot_id == snapshot.snapshot_id())
    {
        let Ok(manifest) = entry.load_manifest(table.file_io()).await else {
            return false;
        };
        for item in manifest.entries().iter().filter(|item| item.is_alive()) {
            added_paths.insert(item.data_file().file_path().to_owned());
        }
    }
    added_paths.len() == expected_paths.len()
        && expected_paths
            .iter()
            .all(|path| added_paths.contains(*path))
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
    let mut last_err = match tx
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(files.clone())
        .apply(tx)?
        .commit(&guard)
        .await
    {
        Ok(committed) => return Ok(committed),
        Err(err) if is_transient_gateway_commit_error(&err) => err,
        Err(err) => return Err(err.into()),
    };
    for attempt in 0..8_u32 {
        let is_429 = {
            let msg = last_err.to_string().to_ascii_lowercase();
            msg.contains("429")
                || msg.contains("toomanyrequestsexception")
                || msg.contains("rate limit")
        };
        let sleep_dur = if is_429 {
            std::time::Duration::from_secs(10 * u64::from(attempt + 1))
        } else {
            std::time::Duration::from_millis(500 * (1_u64 << attempt.min(5)))
        };
        tokio::time::sleep(sleep_dur).await;
        let latest = match catalog.load_table(table.identifier()).await {
            Ok(loaded) => loaded,
            Err(load_err) if is_transient_gateway_commit_error(&load_err) && attempt < 7 => {
                last_err = load_err;
                continue;
            }
            Err(_) => return Err(last_err.into()),
        };
        if latest.metadata_location() == table.metadata_location() {
            let retry_tx = Transaction::new(table);
            match retry_tx
                .fast_append()
                .with_check_duplicate(false)
                .add_data_files(files.clone())
                .apply(retry_tx)?
                .commit(&guard)
                .await
            {
                Ok(committed) => return Ok(committed),
                Err(retry_err) if is_transient_gateway_commit_error(&retry_err) && attempt < 7 => {
                    last_err = retry_err;
                    continue;
                }
                Err(retry_err) => return Err(retry_err.into()),
            }
        }
        if latest.metadata().last_sequence_number() == table.metadata().last_sequence_number() + 1
            && current_snapshot_added_files_match(&latest, &files).await
        {
            return Ok(latest);
        }
        return Err(last_err.into());
    }
    Err(last_err.into())
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
    if object_io::is_remote_uri(&object.path) {
        return Err(object_io::unregistered_remote_uri());
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

/// Flushes directory metadata to durable storage after creating, renaming, or removing an entry.
pub(crate) fn sync_directory(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
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
        sync_directory(parent)?;
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
        sync_directory(parent)?;
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
    #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
    pub known_sizes: BTreeMap<String, u64>,
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

async fn snapshot_files_guarded(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
) -> Result<SnapshotFiles> {
    snapshot_files_guarded_with_scope_and_files(table, budget, guard, reservation, true, None).await
}

async fn snapshot_files_guarded_with_scope(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
    include_retained_history: bool,
) -> Result<SnapshotFiles> {
    snapshot_files_guarded_with_scope_and_files(
        table,
        budget,
        guard,
        reservation,
        include_retained_history,
        None,
    )
    .await
}

async fn snapshot_files_guarded_with_scope_and_files(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
    include_retained_history: bool,
    #[allow(unused_variables)] known_files: Option<&[DataFile]>,
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
    #[cfg(all(not(test), not(feature = "test-util")))]
    if let Some(known) = known_files
        && table.metadata().snapshots().count() == 1
        && table.metadata().statistics_iter().next().is_none()
        && table
            .metadata()
            .partition_statistics_iter()
            .next()
            .is_none()
        && let Some(snapshot) = table.metadata().current_snapshot()
    {
        let list_path = snapshot.manifest_list();
        check(list_path)?;
        paths.insert(list_path.to_owned());
        let list = table.manifest_list_reader(snapshot).load().await?;
        #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
        let mut known_sizes = BTreeMap::new();
        for entry in list.entries() {
            check(&entry.manifest_path)?;
            if !visited_manifests.insert(entry.manifest_path.clone()) {
                return Err(Error::refusal(
                    "duplicate_manifest",
                    "current snapshot repeats a manifest",
                ));
            }
            if let Ok(len) = u64::try_from(entry.manifest_length) {
                metadata_bytes = metadata_bytes.saturating_add(len);
                #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
                known_sizes.insert(entry.manifest_path.clone(), len);
            }
            paths.insert(entry.manifest_path.clone());
        }
        let spec = table.metadata().default_partition_spec();
        for file in known {
            check(file.file_path())?;
            paths.insert(file.file_path().to_owned());
            validate_partition_tuple(spec, file)?;
            #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
            known_sizes.insert(file.file_path().to_owned(), file.file_size_in_bytes());
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
        return Ok(SnapshotFiles {
            paths,
            data: current_data.into_values().collect(),
            #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
            known_sizes,
            _reservation: reservation,
        });
    }
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
    #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
    let known_sizes = current_data
        .values()
        .map(|f: &DataFile| (f.file_path().to_owned(), f.file_size_in_bytes()))
        .collect();
    Ok(SnapshotFiles {
        paths,
        data: current_data.into_values().collect(),
        #[cfg(all(feature = "remote", not(test), not(feature = "test-util")))]
        known_sizes,
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
mod tests;
