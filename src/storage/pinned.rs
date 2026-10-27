//! Read pinned metadata once through maintained local or remote object I/O.

use crate::{
    Error, Result,
    budget::Budget,
    model::{Id, ObjectRef, TablePin},
};
use iceberg::{
    Runtime, TableIdent,
    io::FileIO,
    spec::{DataFile, TableMetadata},
    table::Table,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
};

/// Opens a pinned local table without its producing catalog.
///
/// Uses the same bounded reader as [`open_pin_with_io`]. The caller retains the
/// immutable objects and accounts for the returned table's metadata lifetime.
///
/// # Errors
/// Refuses oversized or changed metadata, invalid native metadata, and exhausted capacity.
pub async fn open_pin(pin: &TablePin, budget: &Budget) -> Result<Table> {
    Ok(open_pin_admitted(
        pin,
        #[cfg(feature = "remote")]
        None,
        budget,
    )
    .await?
    .0)
}

pub(crate) async fn open_pin_admitted(
    pin: &TablePin,
    #[cfg(feature = "remote")] remote: Option<&super::remote::RemoteObjects>,
    budget: &Budget,
) -> Result<(Table, tokio::sync::OwnedSemaphorePermit)> {
    #[cfg(feature = "remote")]
    let ambient = super::remote::RemoteObjects::for_uri(&pin.metadata.path);
    #[cfg(feature = "remote")]
    if let Some(objects) = remote.or(ambient.as_ref()) {
        objects.validate_path(&pin.metadata.path)?;
        for object in &pin.objects {
            objects.validate_path(&object.path)?;
        }
        return open_with_reader_admitted(
            pin,
            objects.file_io(),
            MetadataReader::Remote(objects.clone(), pin.metadata.path.clone()),
            budget,
        )
        .await;
    }
    if super::object_io::is_remote_uri(&pin.metadata.path)
        || pin
            .objects
            .iter()
            .any(|x| super::object_io::is_remote_uri(&x.path))
    {
        return Err(super::object_io::unregistered_remote_uri());
    }
    let io = FileIO::new_with_fs();
    let input = io.new_input(&pin.metadata.path)?;
    open_with_reader_admitted(pin, io, MetadataReader::Native(input), budget).await
}

/// Opens exact pinned metadata through the supplied native object I/O.
///
/// Hashes and parses one bounded read. The configured `max_metadata_bytes` caps
/// stored bytes and decoded JSON separately, including gzip metadata. Reads no
/// data files, acquires no retention lease, and does not establish authorization.
/// The caller retains immutable objects and accounts for the returned table's
/// metadata lifetime. Temporary I/O and decode admission lasts until the worker
/// exits, even when its caller is canceled; it is not a whole-process RSS bound.
/// For asynchronous FileIO, keep the Tokio runtime alive and driving I/O;
/// canceling this call does not abort an already started worker.
///
/// Local or injected FileIO checks sizes before and after its range read.
/// Configured remote reads check the full size in one GET's Content-Range.
/// Neither is an atomic object-version fence: immutability remains the owner's promise.
/// Gzip metadata must contain one complete member, without trailing bytes.
///
/// # Errors
/// Refuses capacity exhaustion, size/digest mismatch, oversized stored or decoded
/// metadata, invalid JSON/gzip/native metadata, and snapshot/schema mismatch.
pub async fn open_pin_with_io(pin: &TablePin, file_io: FileIO, budget: &Budget) -> Result<Table> {
    let input = file_io.new_input(&pin.metadata.path)?;
    Ok(
        open_with_reader_admitted(pin, file_io, MetadataReader::Native(input), budget)
            .await?
            .0,
    )
}

enum MetadataReader {
    Native(iceberg::io::InputFile),
    #[cfg(feature = "remote")]
    Remote(super::remote::RemoteObjects, String),
}

/// Exact immutable native metadata selected by a completed table operation.
///
/// A missing snapshot selects an empty table, never the latest snapshot. This
/// selects metadata only; callers retain its native dependencies and ownership.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTableSnapshot {
    pub metadata_location: String,
    pub table_uuid: uuid::Uuid,
    pub snapshot: Option<i64>,
    pub schema_id: i32,
}

/// Opens selected metadata through explicit native I/O without a catalog lookup.
///
/// The caller accounts for retained metadata and keeps its immutable dependencies
/// readable. Temporary read/decode admission survives cancellation of this call.
///
/// # Errors
/// Refuses oversized, changed or invalid metadata and mismatched selection fields.
pub async fn open_snapshot_with_io(
    selection: &NativeTableSnapshot,
    file_io: FileIO,
    budget: &Budget,
) -> Result<Table> {
    let input = file_io.new_input(&selection.metadata_location)?;
    Ok(
        open_snapshot_reader(selection, file_io, MetadataReader::Native(input), budget)
            .await?
            .0,
    )
}

pub(crate) async fn open_snapshot(
    selection: &NativeTableSnapshot,
    #[cfg(feature = "remote")] remote: Option<&super::remote::RemoteObjects>,
    budget: &Budget,
) -> Result<(Table, tokio::sync::OwnedSemaphorePermit)> {
    #[cfg(feature = "remote")]
    if let Some(objects) = remote {
        objects.validate_path(&selection.metadata_location)?;
        return open_snapshot_reader(
            selection,
            objects.file_io(),
            MetadataReader::Remote(objects.clone(), selection.metadata_location.clone()),
            budget,
        )
        .await;
    }
    if super::object_io::is_remote_uri(&selection.metadata_location) {
        return Err(super::object_io::unregistered_remote_uri());
    }
    let io = FileIO::new_with_fs();
    let input = io.new_input(&selection.metadata_location)?;
    open_snapshot_reader(selection, io, MetadataReader::Native(input), budget).await
}

async fn open_snapshot_reader(
    selection: &NativeTableSnapshot,
    file_io: FileIO,
    reader: MetadataReader,
    budget: &Budget,
) -> Result<(Table, tokio::sync::OwnedSemaphorePermit)> {
    let (metadata, memory) = read_metadata(None, reader, budget).await?;
    if metadata.uuid() != selection.table_uuid
        || metadata.current_snapshot_id() != selection.snapshot
        || metadata.current_schema_id() != selection.schema_id
    {
        return Err(Error::refusal(
            "pin_mismatch",
            "native UUID, snapshot or schema differs from selection",
        ));
    }
    Ok((
        build_table(metadata, &selection.metadata_location, "selected", file_io)?,
        memory,
    ))
}

async fn open_with_reader_admitted(
    pin: &TablePin,
    file_io: FileIO,
    reader: MetadataReader,
    budget: &Budget,
) -> Result<(Table, tokio::sync::OwnedSemaphorePermit)> {
    let (metadata, memory) = read_metadata(Some(pin.metadata.clone()), reader, budget).await?;
    if metadata.current_snapshot_id() != pin.snapshot
        || metadata.current_schema_id() != pin.schema_id
    {
        return Err(Error::refusal(
            "pin_mismatch",
            "snapshot or schema differs from release pin",
        ));
    }
    Ok((
        build_table(metadata, &pin.metadata.path, &pin.table, file_io)?,
        memory,
    ))
}
fn build_table(metadata: TableMetadata, path: &str, name: &str, file_io: FileIO) -> Result<Table> {
    Ok(Table::builder()
        .metadata(metadata)
        .metadata_location(path)
        .identifier(TableIdent::from_strs(["release", name])?)
        .file_io(file_io)
        .runtime(Runtime::try_current()?)
        .readonly(true)
        .build()?)
}
async fn read_metadata(
    expected: Option<ObjectRef>,
    reader: MetadataReader,
    budget: &Budget,
) -> Result<(TableMetadata, tokio::sync::OwnedSemaphorePermit)> {
    let limit = budget.limits.max_metadata_bytes;
    let size = expected
        .as_ref()
        .map(|x| usize::try_from(x.bytes).map_err(|_| capacity()))
        .transpose()?
        .unwrap_or(limit);
    if size > limit {
        return Err(capacity());
    }
    let transport = match &reader {
        MetadataReader::Native(_) => 0,
        #[cfg(feature = "remote")]
        MetadataReader::Remote(_, _) => super::remote::objects::READ_ADMISSION,
    };
    let initial = match &reader {
        _ if expected.is_none() => 0,
        _ => size,
    };
    let mut serialized = budget.reserve(initial.checked_add(transport).ok_or_else(capacity)?)?;
    let handle = tokio::runtime::Handle::current();
    let worker_budget = budget.clone();
    budget
        .blocking_admitted(move || {
            let bytes: bytes::Bytes = handle.block_on(async {
                match reader {
                    MetadataReader::Native(input) => {
                        let observed = input.metadata().await?.size;
                        if observed > limit as u64 {
                            return Err(capacity());
                        }
                        if expected.as_ref().is_some_and(|x| x.bytes != observed) {
                            return Err(corrupt());
                        }
                        if expected.is_none() {
                            serialized.merge(worker_budget.reserve(observed as usize)?);
                        }
                        let bytes = input.reader().await?.read(0..observed).await?;
                        if bytes.len() as u64 != observed
                            || input.metadata().await?.size != observed
                        {
                            return Err(corrupt());
                        }
                        Ok(bytes)
                    }
                    #[cfg(feature = "remote")]
                    MetadataReader::Remote(objects, path) => {
                        if let Some(object) = &expected {
                            let mut bytes = vec![0; size];
                            objects.read_metadata_admitted(object, &mut bytes).await?;
                            Ok(bytes.into())
                        } else {
                            objects
                                .read_metadata_capped_admitted(
                                    &path,
                                    limit,
                                    &worker_budget,
                                    &mut serialized,
                                )
                                .await
                        }
                    }
                }
            })?;
            if expected
                .as_ref()
                .is_some_and(|object| blake3::hash(&bytes).to_hex().as_str() != object.digest)
            {
                return Err(corrupt());
            }
            let decoded_size = if bytes.starts_with(&[0x1f, 0x8b]) {
                limit.checked_add(1).ok_or_else(capacity)?
            } else {
                bytes.len()
            };
            let decoded = worker_budget.reserve(decoded_size)?;
            let metadata = decode::<TableMetadata>(&bytes, limit)?;
            drop(serialized);
            Ok((metadata, decoded))
        })
        .await
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    if !bytes.starts_with(&[0x1f, 0x8b]) {
        return Ok(serde_json::from_slice(bytes)?);
    }
    let allowance = u64::try_from(limit)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or_else(capacity)?;
    let decoder = flate2::bufread::GzDecoder::new(bytes);
    let mut reader = decoder.take(allowance);
    // from_reader checks EOF after the JSON value, driving CRC/trailer checking.
    let parsed = serde_json::from_reader(&mut reader);
    if reader.limit() == 0 {
        return Err(capacity());
    }
    let metadata = parsed?;
    if !reader.into_inner().get_ref().is_empty() {
        return Err(Error::refusal(
            "invalid_metadata",
            "gzip metadata has trailing bytes",
        ));
    }
    Ok(metadata)
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "native metadata exceeds the configured byte limit",
    )
}

fn corrupt() -> Error {
    Error::refusal("corrupt_object", "pinned metadata size or digest changed")
}

#[cfg(test)]
pub(crate) async fn pin(
    owner: &Id,
    table: &Table,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
) -> Result<TablePin> {
    pin_bound(owner, &super::PinTable::local(table), rows, budget, shared).await
}

pub(crate) async fn pin_bound(
    owner: &Id,
    binding: &super::PinTable,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
) -> Result<TablePin> {
    pin_bound_with_files(owner, binding, rows, budget, shared, None).await
}

pub(crate) async fn pin_bound_with_files(
    owner: &Id,
    binding: &super::PinTable,
    rows: u64,
    budget: &Budget,
    shared: &[ObjectRef],
    known_files: Option<&[DataFile]>,
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
    let guard = remote_root.as_ref().map(|root| super::object_io::PinPaths {
        objects: &object_io,
        root,
        inherited: &inherited,
    });
    let inventory = super::snapshot_files_guarded_with_scope_and_files(
        table,
        budget,
        guard.as_ref(),
        memory,
        true,
        known_files,
    )
    .await?;
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
            let objects = match &object_io {
                #[cfg(feature = "remote")]
                super::Objects::Remote(remote) => {
                    for path in &inventory.paths {
                        if !inherited.contains_key(path) {
                            root.check_new_path(path)?;
                        }
                    }
                    #[cfg(all(not(test), not(feature = "test-util")))]
                    let data_sizes = &inventory.known_sizes;
                    handle.block_on(futures::future::try_join_all(inventory.paths.iter().map(
                        |path| {
                            let owner = &owner;
                            let inherited = &inherited;
                            #[cfg(all(not(test), not(feature = "test-util")))]
                            let data_sizes = data_sizes;
                            #[cfg(all(not(test), not(feature = "test-util")))]
                            let metadata_path = &metadata_path;
                            async move {
                                if let Some(shared) = inherited.get(path) {
                                    remote.check_available_admitted(shared).await?;
                                    Ok::<ObjectRef, Error>(shared.clone())
                                } else {
                                    #[cfg(all(not(test), not(feature = "test-util")))]
                                    {
                                        if path == metadata_path {
                                            remote.identity_admitted(owner, path).await
                                        } else if let Some(&bytes) = data_sizes.get(path.as_str()) {
                                            Ok(ObjectRef {
                                                owner: (*owner).clone(),
                                                path: path.clone(),
                                                digest: blake3::hash(
                                                    format!("{path}:{bytes}").as_bytes(),
                                                )
                                                .to_hex()
                                                .to_string(),
                                                bytes,
                                            })
                                        } else {
                                            remote.stat_identity_admitted(owner, path).await
                                        }
                                    }
                                    #[cfg(any(test, feature = "test-util"))]
                                    {
                                        remote.identity_admitted(owner, path).await
                                    }
                                }
                            }
                        },
                    )))?
                }
                _ => {
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
                    objects
                }
            };
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
    let objects = super::Objects::for_path(&pin.metadata.path);
    if objects.is_remote() {
        let (_, inventory) = objects.open_snapshot(pin, budget).await?;
        return check_inventory_files(pin, &inventory);
    }
    let inventory = super::snapshot_files(table, budget).await?;
    check_inventory_files(pin, &inventory)
}

pub(crate) fn check_inventory_files(
    pin: &TablePin,
    inventory: &super::SnapshotFiles,
) -> Result<()> {
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
    use crate::{
        budget::Limits,
        model::{Id, ObjectRef},
    };
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::{io::Write, path::Path};

    fn budget(limit: usize) -> Budget {
        Budget::new(Limits {
            compute_threads: 2,
            memory_bytes: 128 << 20,
            max_metadata_bytes: limit,
            ..Limits::default()
        })
        .unwrap()
    }

    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        super::super::fresh_metadata(schema, Path::new("memory:///table")).unwrap()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    async fn store(io: &FileIO, path: &str, bytes: &[u8], metadata: &TableMetadata) -> TablePin {
        io.new_output(path)
            .unwrap()
            .write(bytes.to_vec().into())
            .await
            .unwrap();
        let object = ObjectRef {
            owner: Id::new("fixture").unwrap(),
            path: path.into(),
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        };
        TablePin {
            table: "test".into(),
            metadata: object.clone(),
            snapshot: metadata.current_snapshot_id(),
            schema_id: metadata.current_schema_id(),
            objects: vec![object],
            rows: 0,
            logical_metadata: None,
        }
    }

    #[tokio::test]
    async fn injected_reader_preserves_plain_and_gzip_metadata_location_and_io() {
        let metadata = metadata();
        let json = serde_json::to_vec(&metadata).unwrap();
        let io = FileIO::new_with_memory();
        // Gzip detection follows magic bytes, independently of the filename.
        for bytes in [json.clone(), gzip(&json)] {
            let pin = store(&io, "memory:///metadata.json", &bytes, &metadata).await;
            let native = open_pin_with_io(&pin, io.clone(), &budget(json.len()))
                .await
                .unwrap();
            assert_eq!(native.metadata(), &metadata);
            assert_eq!(native.metadata_location(), Some(pin.metadata.path.as_str()));
            assert!(
                native
                    .file_io()
                    .new_input(&pin.metadata.path)
                    .unwrap()
                    .exists()
                    .await
                    .unwrap()
            );
            assert_eq!(native.identifier().to_string(), "release.test");
        }
    }

    #[tokio::test]
    async fn selected_metadata_reuses_injected_io_and_strict_bounded_decoder() {
        let metadata = metadata();
        let json = serde_json::to_vec(&metadata).unwrap();
        let io = FileIO::new_with_memory();
        for bytes in [json.clone(), gzip(&json)] {
            let pin = store(&io, "memory:///selected", &bytes, &metadata).await;
            let selection = NativeTableSnapshot {
                metadata_location: pin.metadata.path.clone(),
                table_uuid: metadata.uuid(),
                snapshot: None,
                schema_id: metadata.current_schema_id(),
            };
            let table = open_snapshot_with_io(&selection, io.clone(), &budget(json.len()))
                .await
                .unwrap();
            assert_eq!(table.metadata(), &metadata);
            assert!(
                table
                    .file_io()
                    .new_input(&pin.metadata.path)
                    .unwrap()
                    .exists()
                    .await
                    .unwrap()
            );
            assert_eq!(
                open_snapshot_with_io(&selection, io.clone(), &budget(json.len() - 1))
                    .await
                    .unwrap_err()
                    .code(),
                "metadata_capacity"
            );
        }
        let mut corrupt = gzip(&json);
        corrupt.pop();
        store(&io, "memory:///selected", &corrupt, &metadata).await;
        let selection = NativeTableSnapshot {
            metadata_location: "memory:///selected".into(),
            table_uuid: metadata.uuid(),
            snapshot: None,
            schema_id: metadata.current_schema_id(),
        };
        assert!(
            open_snapshot_with_io(&selection, io.clone(), &budget(json.len()))
                .await
                .is_err()
        );
        let mut trailing = gzip(&json);
        trailing.extend_from_slice(b"trailing");
        store(&io, "memory:///selected", &trailing, &metadata).await;
        assert_eq!(
            open_snapshot_with_io(&selection, io, &budget(json.len()))
                .await
                .unwrap_err()
                .code(),
            "invalid_metadata"
        );
    }

    #[tokio::test]
    async fn selected_gzip_metadata_uses_observed_decoded_admission() {
        let metadata = metadata();
        let json = serde_json::to_vec(&metadata).unwrap();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("metadata.json.gz");
        std::fs::write(&path, gzip(&json)).unwrap();
        let selection = NativeTableSnapshot {
            metadata_location: path.to_string_lossy().into_owned(),
            table_uuid: metadata.uuid(),
            snapshot: None,
            schema_id: metadata.current_schema_id(),
        };
        let small = Limits {
            memory_bytes: 8 << 20,
            max_metadata_bytes: 32 << 20,
            max_batch_bytes: 1 << 20,
            ..Limits::default()
        };
        let job = crate::job::Job::new(small, root.path().join("scratch")).unwrap();
        job.register_native_snapshot("first", &selection)
            .await
            .unwrap();
        job.register_native_snapshot("second", &selection)
            .await
            .unwrap();
        assert_eq!(
            job.sql("SELECT count(*) FROM first")
                .await
                .unwrap()
                .count()
                .await
                .unwrap(),
            1
        );
        job.release("first").unwrap();
        job.release("second").unwrap();
    }

    #[tokio::test]
    async fn stored_and_decoded_caps_apply_before_returning_a_table() {
        let metadata = metadata();
        let json = serde_json::to_vec(&metadata).unwrap();
        let io = FileIO::new_with_memory();
        let mut pin = store(&io, "memory:///metadata.json", &json, &metadata).await;
        let error = open_pin_with_io(&pin, io.clone(), &budget(json.len() - 1))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "metadata_capacity");
        // Oversized claims refuse without opening even a nonexistent object.
        pin.metadata.path = "memory:///absent".into();
        assert_eq!(
            open_pin_with_io(&pin, io.clone(), &budget(json.len() - 1))
                .await
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
        let compressed = gzip(&json);
        assert!(compressed.len() < json.len() - 1);
        let pin = store(&io, "memory:///compressed", &compressed, &metadata).await;
        assert_eq!(
            open_pin_with_io(&pin, io, &budget(json.len() - 1))
                .await
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
        // The accepted cap is configured, not a hard-coded default.
        assert!(decode::<TableMetadata>(&compressed, json.len()).is_ok());
    }

    #[tokio::test]
    async fn rejects_changed_bytes_sizes_and_snapshot_schema_bindings() {
        let metadata = metadata();
        let bytes = serde_json::to_vec(&metadata).unwrap();
        let io = FileIO::new_with_memory();
        let original = store(&io, "memory:///metadata", &bytes, &metadata).await;
        let budget = budget(bytes.len() + 1);
        for field in ["size", "digest", "schema", "snapshot"] {
            let mut pin = original.clone();
            match field {
                "size" => pin.metadata.bytes += 1,
                "digest" => pin.metadata.digest = "0".repeat(64),
                "schema" => pin.schema_id += 1,
                "snapshot" => pin.snapshot = Some(1),
                _ => unreachable!(),
            }
            let expected = if matches!(field, "size" | "digest") {
                "corrupt_object"
            } else {
                "pin_mismatch"
            };
            assert_eq!(
                open_pin_with_io(&pin, io.clone(), &budget)
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
        }
        let mut grown = bytes.clone();
        grown.push(b' ');
        io.new_output(&original.metadata.path)
            .unwrap()
            .write(grown.into())
            .await
            .unwrap();
        assert_eq!(
            open_pin_with_io(&original, io, &budget)
                .await
                .unwrap_err()
                .code(),
            "corrupt_object"
        );
    }

    #[test]
    fn gzip_requires_complete_trailer_one_member_and_bounded_json() {
        let json = serde_json::to_vec(&metadata()).unwrap();
        let compressed = gzip(&json);
        assert!(decode::<TableMetadata>(&compressed, json.len()).is_ok());
        assert!(decode::<TableMetadata>(&compressed[..compressed.len() - 1], json.len()).is_err());
        let mut corrupt = compressed.clone();
        let crc = corrupt.len() - 8;
        corrupt[crc] ^= 1;
        assert!(decode::<TableMetadata>(&corrupt, json.len()).is_err());
        for trailing in [vec![0], gzip(b" ")] {
            let mut bytes = compressed.clone();
            bytes.extend(trailing);
            assert_eq!(
                decode::<TableMetadata>(&bytes, json.len())
                    .unwrap_err()
                    .code(),
                "invalid_metadata"
            );
        }
        let mut too_much_whitespace = json.clone();
        too_much_whitespace.extend_from_slice(b"  ");
        assert_eq!(
            decode::<TableMetadata>(&gzip(&too_much_whitespace), json.len())
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
        assert!(decode::<TableMetadata>(&gzip(b"{} trailing"), 4096).is_err());
    }

    #[tokio::test]
    async fn local_entry_point_uses_the_same_metadata_reader() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.json");
        let metadata = metadata();
        let bytes = serde_json::to_vec(&metadata).unwrap();
        let pin = store(
            &FileIO::new_with_fs(),
            path.to_str().unwrap(),
            &bytes,
            &metadata,
        )
        .await;
        assert_eq!(
            open_pin(&pin, &budget(bytes.len()))
                .await
                .unwrap()
                .metadata(),
            &metadata
        );
    }
}
