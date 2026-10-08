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
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
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

pub(crate) fn files_under(root: &Path, limit: usize) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(Error::refusal(
                    "symlink",
                    "owned storage cannot contain symlinks",
                ));
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
            if files.len() + pending.len() > limit {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "object inventory exceeds the declared entry budget",
                ));
            }
        }
    }
    files.sort();
    Ok(files)
}

pub(crate) fn pin(owner: &Id, table: &Table, rows: u64, max_entries: usize) -> Result<TablePin> {
    let metadata = object(owner, Path::new(table.metadata_location_result()?))?;
    let mut objects = Vec::new();
    for path in files_under(Path::new(table.metadata().location()), max_entries)? {
        objects.push(object(owner, &path)?);
    }
    Ok(TablePin {
        table: table.identifier().name().into(),
        metadata,
        snapshot: table.metadata().current_snapshot_id(),
        schema_id: table.metadata().current_schema_id(),
        objects,
        rows,
    })
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
