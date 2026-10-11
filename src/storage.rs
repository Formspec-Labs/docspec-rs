//! Bounded local objects and maintained Iceberg readers, writers, and commit actions.

pub(crate) mod defaults;
pub(crate) mod identity;
mod inspection;
mod object_io;
mod occurrences;
pub(crate) use object_io::{Objects, PinTable, valid_object_locator};
mod pinned;
#[doc(inline)]
pub use pinned::{open_pin, open_pin_with_io};
#[cfg(feature = "remote")]
pub mod remote;
pub(crate) mod removal;
mod scan_metrics;
#[cfg(any(test, feature = "test-util"))]
#[doc(inline)]
pub use scan_metrics::{NativeScanObservation, NativeScanSample};
mod schema;
mod selection;
pub(crate) use selection::reserve_projection;
mod tables;
pub(crate) use inspection::{CheckedFile, inspect_table, schema_identity};
#[cfg(any(test, feature = "test-util"))]
#[doc(inline)]
pub use occurrences::read_occurrences_observed;
#[doc(inline)]
pub use occurrences::{OCCURRENCE_FILE, OCCURRENCE_ROW, occurrence_schema, read_occurrences};
pub(crate) use occurrences::{OccurrenceScan, read_projected_occurrences};
pub(crate) use tables::{
    CandidateWriter, bounded_json, fresh_metadata, register_candidate, replacement_metadata,
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
    file::properties::WriterProperties,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

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
    Ok(futures::stream::try_unfold(
        (stream, reservation, selection, observation),
        move |(mut stream, reservation, selection, observation)| async move {
            let Some(batch) = stream.try_next().await? else {
                if let Some(observation) = &observation {
                    observation.finish();
                }
                return Ok(None);
            };
            let batch = schema::strip_file_column(batch, limit)?;
            let batch = match &selection {
                Some(selection) => selection.apply(batch, None, limit)?,
                None => batch,
            };
            Ok(Some((batch, (stream, reservation, selection, observation))))
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
    check: fn(&ObjectRef) -> Result<()>,
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
    Box::pin(async move {
        if target_file_bytes == 0 {
            return Err(Error::refusal(
                "invalid_compaction",
                "output file size must be positive",
            ));
        }
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
        let props = WriterProperties::builder()
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .set_statistics_truncate_length(Some(64))
            .set_offset_index_disabled(true)
            .set_max_row_group_row_count(Some(budget.limits.batch_rows))
            // The retained Comments probe found large avoidable uncompressed output.
            // Keep the maintained default Zstandard level; see the codec experiment.
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let parquet = ParquetWriterBuilder::new(props, table.metadata().current_schema().clone())
            .with_match_mode(iceberg::arrow::FieldMatchMode::Name);
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            target_file_bytes,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata())?,
            DefaultFileNameGenerator::new(
                uuid::Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let new_writer = || UnpartitionedWriter::new(DataFileWriterBuilder::new(rolling.clone()));
        let mut writer = new_writer();
        let expected_arrow =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
        let origin = format!("docspec:{}", uuid::Uuid::new_v4());
        let mut next_row = 0u64;
        futures::pin_mut!(batches);
        while let Some(batch) = batches.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
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
            let bytes = batch.get_array_memory_size();
            if bytes > budget.limits.max_batch_bytes {
                return Err(Error::refusal(
                    "oversized_batch",
                    format!(
                        "decoded batch uses {bytes} bytes, exceeding the {} byte allowance; reduce batch rows or split oversized values",
                        budget.limits.max_batch_bytes
                    ),
                ));
            }
            // A write can finish the preceding row group, create new groups, and
            // roll a file. This conservative total also covers descriptors retained
            // after the underlying file closes. It is admission, not an RSS estimate.
            let units = batch
                .num_rows()
                .div_ceil(budget.limits.batch_rows)
                .saturating_add(1);
            let incoming = units.saturating_mul(unit_metadata_bytes);
            // Periodically finish the maintained writer, releasing its accumulated
            // footers. Keep only bounded file descriptors, so this admission grows
            // with output files instead of every row group in the entire population.
            if metadata_bytes != 0
                && metadata_bytes.saturating_add(incoming) > budget.limits.max_metadata_bytes / 2
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
            }
            metadata_bytes = metadata_bytes.saturating_add(incoming);
            if retained_bytes.saturating_add(metadata_bytes) > budget.limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "native writer file and row-group metadata exceed the declared allowance",
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
                    if let (
                        arrow::datatypes::DataType::Timestamp(unit, Some(_)),
                        arrow::datatypes::DataType::Timestamp(target_unit, Some(_)),
                    ) = (field.data_type(), target)
                        && unit == target_unit
                        && field.data_type() != target
                    {
                        columns[index] = arrow::compute::cast(&columns[index], target)?;
                        field = field.with_data_type(target.clone());
                    }
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>()?;
            let batch =
                RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
            writer.write(batch).await?;
        }
        completed.extend(writer.close().await?);
        Ok(completed)
    })
}

pub(crate) async fn append(
    table: &Table,
    catalog: &dyn Catalog,
    files: Vec<DataFile>,
) -> Result<Table> {
    if files.is_empty() {
        return Ok(table.clone());
    }
    let tx = Transaction::new(table);
    Ok(tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)?
        .commit(catalog)
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

async fn snapshot_files_guarded(
    table: &Table,
    budget: &Budget,
    guard: Option<&object_io::PinPaths<'_>>,
    reservation: tokio::sync::OwnedSemaphorePermit,
) -> Result<SnapshotFiles> {
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
    // Retain every snapshot referenced by this exact metadata file. Earlier
    // metadata versions have their own pins; directory contents are not authority.
    let snapshots =
        table
            .metadata()
            .current_snapshot()
            .into_iter()
            .chain(table.metadata().snapshots().filter(|snapshot| {
                Some(snapshot.snapshot_id()) != table.metadata().current_snapshot_id()
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
            for item in manifest.entries().iter().filter(|item| item.is_alive()) {
                let file = item.data_file();
                check(file.file_path())?;
                paths.insert(file.file_path().to_owned());
                if is_current {
                    if file.content_type() != iceberg::spec::DataContentType::Data {
                        return Err(Error::refusal(
                            "unsupported_deletes",
                            "candidate file sharing requires materialized membership without native delete files",
                        ));
                    }
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
}
