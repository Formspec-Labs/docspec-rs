//! Read pinned metadata once through maintained local or remote object I/O.

use crate::{Error, Result, budget::Budget, model::TablePin};
use iceberg::{Runtime, TableIdent, io::FileIO, spec::TableMetadata, table::Table};
use std::io::Read;

/// Opens a pinned local table without its producing catalog.
///
/// Uses the same bounded reader as [`open_pin_with_io`]. The caller retains the
/// immutable objects and accounts for the returned table's metadata lifetime.
///
/// # Errors
/// Refuses oversized or changed metadata, invalid native metadata, and exhausted capacity.
pub async fn open_pin(pin: &TablePin, budget: &Budget) -> Result<Table> {
    open_pin_with_io(pin, FileIO::new_with_fs(), budget).await
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
/// Size checks before and after the range read detect observed changes. They are
/// not an atomic object-version fence: immutability remains the owner's promise.
/// Gzip metadata must contain one complete member, without trailing bytes.
///
/// # Errors
/// Refuses capacity exhaustion, size/digest mismatch, oversized stored or decoded
/// metadata, invalid JSON/gzip/native metadata, and snapshot/schema mismatch.
pub async fn open_pin_with_io(pin: &TablePin, file_io: FileIO, budget: &Budget) -> Result<Table> {
    let limit = budget.limits.max_metadata_bytes;
    let size = usize::try_from(pin.metadata.bytes).map_err(|_| capacity())?;
    if size > limit {
        return Err(capacity());
    }
    let serialized = budget.reserve(size)?;
    let runtime = Runtime::try_current()?;
    let handle = tokio::runtime::Handle::current();
    let input = file_io.new_input(&pin.metadata.path)?;
    let object = pin.metadata.clone();
    let worker_budget = budget.clone();
    let metadata = budget
        .blocking_admitted(move || {
            let _serialized = serialized;
            // Native filesystem FileIO blocks internally. One admitted worker keeps
            // this off runtime threads and also bounds concurrent remote opens.
            let bytes = handle.block_on(async {
                if input.metadata().await?.size != object.bytes {
                    return Err(corrupt());
                }
                let bytes = input.reader().await?.read(0..object.bytes).await?;
                if bytes.len() != size || input.metadata().await?.size != object.bytes {
                    return Err(corrupt());
                }
                Ok(bytes)
            })?;
            if blake3::hash(&bytes).to_hex().as_str() != object.digest {
                return Err(corrupt());
            }
            // Native decoded structs can differ from JSON size; this is shared
            // admission, not an exact allocator/RSS measurement. No decoded Vec copy.
            let decoded_size = if bytes.starts_with(&[0x1f, 0x8b]) {
                limit.checked_add(1).ok_or_else(capacity)?
            } else {
                size
            };
            let _decoded = worker_budget.reserve(decoded_size)?;
            decode::<TableMetadata>(&bytes, limit)
        })
        .await?;
    if metadata.current_snapshot_id() != pin.snapshot
        || metadata.current_schema_id() != pin.schema_id
    {
        return Err(Error::refusal(
            "pin_mismatch",
            "snapshot or schema differs from release pin",
        ));
    }
    Ok(Table::builder()
        .metadata(metadata)
        .metadata_location(&pin.metadata.path)
        .identifier(TableIdent::from_strs(["release", &pin.table])?)
        .file_io(file_io)
        .runtime(runtime)
        .readonly(true)
        .build()?)
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
