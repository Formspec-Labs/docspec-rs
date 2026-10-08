//! Bounded local objects and maintained Iceberg readers, writers, and commit actions.

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
    table::{StaticTable, Table},
};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
use parquet::file::properties::WriterProperties;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

/// Creates the maintained embedded catalog; application rows remain in Iceberg.
///
/// # Errors
/// Returns filesystem, SQLite driver, or catalog initialization errors.
pub async fn local_catalog(root: &Path) -> Result<Arc<dyn Catalog>> {
    std::fs::create_dir_all(root.join("warehouse"))?;
    let catalog = SqlCatalogBuilder::default()
        .uri(format!(
            "sqlite://{}?mode=rwc",
            root.join("catalog.sqlite").display()
        ))
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
    let reservation = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    let limit = budget.limits.max_batch_bytes;
    let stream = table
        .scan()
        .with_concurrency_limit(1)
        .with_batch_size(Some(budget.limits.batch_rows))
        .build()?
        .to_arrow()
        .await?;
    Ok(futures::stream::try_unfold(
        (stream, reservation),
        move |(mut stream, reservation)| async move {
            let Some(batch) = stream.try_next().await? else {
                return Ok(None);
            };
            if batch.get_array_memory_size() > limit {
                return Err(Error::refusal(
                    "oversized_batch",
                    "native reader exceeded its byte allowance",
                ));
            }
            Ok(Some((batch, (stream, reservation))))
        },
    ))
}

pub(crate) async fn verify_objects(objects: Vec<ObjectRef>, budget: &Budget) -> Result<()> {
    budget
        .blocking(move || {
            for object in objects {
                verify_object(&object)?;
            }
            Ok(())
        })
        .await
}

pub(crate) async fn write_batches<S>(
    table: &Table,
    batches: S,
    budget: &Budget,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Send,
{
    // Parquet and rolling-file buffers survive individual writes. Keep their
    // admission for the entire writer lifetime, including close and flush.
    let _reservation = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(4))?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(budget.limits.batch_rows))
        .build();
    let parquet = ParquetWriterBuilder::new(props, table.metadata().current_schema().clone())
        .with_match_mode(iceberg::arrow::FieldMatchMode::Name);
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        budget.limits.max_batch_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(
            uuid::Uuid::new_v4().to_string(),
            None,
            DataFileFormat::Parquet,
        ),
    );
    let mut writer = UnpartitionedWriter::new(DataFileWriterBuilder::new(rolling));
    futures::pin_mut!(batches);
    while let Some(batch) = batches.try_next().await? {
        let bytes = batch.get_array_memory_size();
        if bytes > budget.limits.max_batch_bytes {
            return Err(Error::refusal(
                "oversized_batch",
                "decoded batch exceeds byte admission; reduce batch rows or split oversized values",
            ));
        }
        writer.write(batch).await?;
    }
    Ok(writer.close().await?)
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
/// # Errors
/// Returns filesystem failures without interpreting partial files as complete.
pub fn digest(path: &Path) -> Result<(String, u64)> {
    let mut input = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0; 1024 * 1024];
    let mut bytes = 0;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        bytes += n as u64;
    }
    Ok((hasher.finalize().to_hex().to_string(), bytes))
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
    let reservation = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    let mut paths = BTreeSet::from([table.metadata_location_result()?.to_owned()]);
    let mut current_data = BTreeMap::new();
    let mut metadata_bytes = 0u64;
    let mut visited_manifests = BTreeSet::new();
    for statistics in table.metadata().statistics_iter() {
        paths.insert(statistics.statistics_path.clone());
    }
    for statistics in table.metadata().partition_statistics_iter() {
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
        metadata_bytes = metadata_bytes
            .saturating_add(table.file_io().new_input(list_path)?.metadata().await?.size);
        check_inventory_size(&paths, metadata_bytes, budget)?;
        paths.insert(list_path.to_owned());
        let list = table.manifest_list_reader(snapshot).load().await?;
        for entry in list.entries() {
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

pub(crate) async fn pin(
    owner: &Id,
    table: &Table,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
) -> Result<TablePin> {
    let inventory = snapshot_files(table, budget).await?;
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
    let owner = owner.clone();
    let location = table.metadata().location().to_owned();
    let metadata_path = table.metadata_location_result()?.to_owned();
    let name = table.identifier().name().to_owned();
    let snapshot = table.metadata().current_snapshot_id();
    let schema_id = table.metadata().current_schema_id();
    budget.blocking(move || {
        let inventory = inventory;
        let root = std::fs::canonicalize(location)?;
        let mut objects = Vec::new();
        for path in &inventory.paths {
            if let Some(shared) = inherited.get(path) {
                verify_object(shared)?;
                objects.push(shared.clone());
            } else {
                let canonical = std::fs::canonicalize(path)?;
                if !canonical.starts_with(&root) || std::fs::symlink_metadata(path)?.file_type().is_symlink() {
                    return Err(Error::refusal("unprotected_reference", "native snapshot references an object without an inherited protection owner"));
                }
                objects.push(object(&owner, Path::new(path))?);
            }
        }
        let metadata = objects.iter().find(|object| object.path == metadata_path).cloned()
            .ok_or_else(|| Error::refusal("incomplete_inventory", "native metadata is absent from snapshot inventory"))?;
        Ok(TablePin { table: name, metadata, snapshot, schema_id, objects, rows })
    }).await
}

pub(crate) async fn check_inventory(pin: &TablePin, table: &Table, budget: &Budget) -> Result<()> {
    let inventory = snapshot_files(table, budget).await?;
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

/// Opens a native snapshot without the original catalog or producer callbacks.
///
/// # Errors
/// Refuses corrupted metadata or incompatible snapshot/schema bindings.
pub async fn open_pin(pin: &TablePin) -> Result<Table> {
    verify_object(&pin.metadata)?;
    let ident = iceberg::TableIdent::from_strs(["release", &pin.table])?;
    let table = StaticTable::from_metadata_file(
        &pin.metadata.path,
        ident,
        iceberg::io::FileIO::new_with_fs(),
    )
    .await?
    .into_table();
    if table.metadata().current_snapshot_id() != pin.snapshot
        || table.metadata().current_schema_id() != pin.schema_id
    {
        return Err(Error::refusal(
            "pin_mismatch",
            "snapshot or schema differs from release pin",
        ));
    }
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    fn batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    async fn values(pin: &TablePin, budget: &Budget) -> Vec<Option<i64>> {
        let table = open_pin(pin).await.unwrap();
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
