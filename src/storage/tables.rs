//! Candidate metadata preserving native field identity without copying value files.

use crate::{Error, Result, budget::Budget};
use iceberg::{
    Catalog, MetadataLocation, TableIdent,
    spec::{FormatVersion, PartitionSpec, Schema, SortOrder, TableMetadata, TableMetadataBuilder},
    table::Table,
};
use std::path::Path;

pub(crate) fn fresh_metadata(schema: Schema, directory: &Path) -> Result<TableMetadata> {
    fresh_metadata_at(schema, &directory.to_string_lossy())
}

pub(crate) fn fresh_metadata_at(schema: Schema, location: &str) -> Result<TableMetadata> {
    // Native creation reassigns IDs. Reinstall the supplied schema before any
    // files exist, then discard only the unused provisional schema.
    let metadata = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        location.to_owned(),
        FormatVersion::V2,
        super::table_properties(),
    )?
    .add_current_schema(schema)?
    .build()?
    .metadata;
    let unused = metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .filter(|id| *id != metadata.current_schema_id())
        .collect::<Vec<_>>();
    Ok(metadata
        .into_builder(None)
        .remove_schemas(&unused)?
        .build()?
        .metadata)
}

pub(crate) fn replacement_metadata(
    base: &TableMetadata,
    directory: &Path,
) -> Result<TableMetadata> {
    replacement_metadata_at(base, &directory.to_string_lossy())
}

pub(crate) fn replacement_metadata_at(
    base: &TableMetadata,
    location: &str,
) -> Result<TableMetadata> {
    if !matches!(base.format_version(), FormatVersion::V2 | FormatVersion::V3)
        || !base.default_partition_spec().is_unpartitioned()
        || !base.default_sort_order().is_unsorted()
        || base.encryption_keys_iter().next().is_some()
    {
        return Err(Error::refusal(
            "unsupported_write_layout",
            "replacement candidates require unpartitioned, unsorted, unencrypted Iceberg v2/v3 metadata",
        ));
    }
    let snapshots = base
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect::<Vec<_>>();
    let mut builder = base
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(location.to_owned())
        .remove_ref("main")
        .remove_snapshots(&snapshots)
        // These writer overrides would escape the new protected attempt directory.
        .remove_properties(&[
            "write.data.path".into(),
            "write.folder-storage.path".into(),
            "write.metadata.compression-codec".into(),
        ])?
        .set_properties(super::table_properties())?;
    for statistics in base.statistics_iter() {
        builder = builder.remove_statistics(statistics.snapshot_id);
    }
    for statistics in base.partition_statistics_iter() {
        builder = builder.remove_partition_statistics(statistics.snapshot_id);
    }
    // The maintained builder preserves the highest allocated field ID, even when
    // no remaining schema contains it. Existing metadata-log entries remain;
    // they do not select results or add snapshots to the new candidate.
    Ok(builder.build()?.metadata)
}

/// Concrete create-new metadata I/O for the shared candidate worker.
#[derive(Clone)]
pub(crate) enum CandidateWriter {
    Local,
    #[cfg(feature = "remote")]
    Remote(Box<super::remote::RemoteObjects>, Option<MetadataLocation>),
}

impl CandidateWriter {
    pub(crate) fn configured(objects: &super::Objects) -> Self {
        match objects {
            super::Objects::Local => Self::Local,
            #[cfg(feature = "remote")]
            super::Objects::Remote(objects) => Self::Remote(Box::new(objects.clone()), None),
        }
    }

    fn metadata_path(&self, metadata: &TableMetadata) -> Result<String> {
        if metadata.metadata_compression_codec()? != iceberg::compression::CompressionCodec::None {
            return Err(Error::refusal(
                "metadata_compression",
                "candidate metadata requires uncompressed JSON",
            ));
        }
        match self {
            Self::Local => {
                Ok(MetadataLocation::new_with_metadata(metadata.location(), metadata).to_string())
            }
            #[cfg(feature = "remote")]
            Self::Remote(_, selected) => {
                let location = selected.clone().unwrap_or_else(|| {
                    MetadataLocation::new_with_metadata(metadata.location(), metadata)
                });
                let path = location.to_string();
                let prefix = format!("{}/metadata/", metadata.location());
                let filename = path.strip_prefix(&prefix);
                let canonical = path
                    .parse::<MetadataLocation>()
                    .is_ok_and(|parsed| parsed == location && parsed.to_string() == path);
                if location.compression_codec() != iceberg::compression::CompressionCodec::None
                    || filename.is_none_or(|name| name.is_empty() || name.contains('/'))
                    || !canonical
                {
                    return Err(Error::refusal(
                        "metadata_location",
                        "candidate location must name canonical native metadata beneath its table location",
                    ));
                }
                Ok(path)
            }
        }
    }

    fn bind_candidate(self, table: Table) -> Result<Table> {
        match self {
            Self::Local => Ok(table),
            #[cfg(feature = "remote")]
            Self::Remote(objects, _) => Ok(super::Objects::Remote(*objects)
                .bind_table(&table)?
                .into_native()),
        }
    }

    fn create_bytes(
        self,
        path: &str,
        bytes: Vec<u8>,
        _handle: &tokio::runtime::Handle,
    ) -> Result<()> {
        let objects = match self {
            Self::Local => super::Objects::Local,
            #[cfg(feature = "remote")]
            Self::Remote(objects, _) => super::Objects::Remote(*objects),
        };
        objects.create_bytes_admitted(path, bytes, _handle)
    }
}

pub(crate) async fn register_candidate(
    catalog: &dyn Catalog,
    identifier: &TableIdent,
    writer: CandidateWriter,
    prepare: impl FnOnce() -> Result<TableMetadata> + Send + 'static,
    budget: &Budget,
) -> Result<Table> {
    let reservation =
        std::sync::Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?);
    let worker_reservation = reservation.clone();
    let limit = budget.limits.max_metadata_bytes;
    let worker_writer = writer.clone();
    let handle = tokio::runtime::Handle::current();
    let path = budget
        .blocking(move || {
            // The worker keeps admission if its caller is canceled during durable I/O.
            let _reservation = worker_reservation;
            let metadata = prepare()?;
            let path = worker_writer.metadata_path(&metadata)?;
            let bytes = bounded_json(&metadata, limit)?;
            worker_writer.create_bytes(&path, bytes, &handle)?;
            Ok(path)
        })
        .await?;
    // Keep the same admission while the native catalog parses the retained metadata.
    let table = catalog.register_table(identifier, path).await?;
    writer.bind_candidate(table)
}

pub(crate) fn bounded_json(value: &impl serde::Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut output = BoundedJson {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    match serde_json::to_writer(&mut output, value) {
        Ok(()) => Ok(output.bytes),
        Err(_) if output.exceeded => Err(Error::refusal(
            "metadata_capacity",
            "candidate metadata exceeds its byte allowance",
        )),
        Err(error) => Err(error.into()),
    }
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|needed| *needed <= self.limit);
        let Some(needed) = needed else {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "metadata allowance exceeded",
            ));
        };
        if needed > self.bytes.capacity() {
            // Geometric growth avoids reallocating for every JSON token; its
            // requested capacity never exceeds the declared metadata allowance.
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Match the existing restore publication boundary: a partial temporary never
// becomes the final metadata selected by a retained candidate registration.
pub(super) fn publish_candidate_metadata(
    expected: &crate::model::ObjectRef,
    bytes: &[u8],
) -> Result<()> {
    if !publish_metadata_new(expected, bytes)? {
        super::verify_object(expected)?;
    }
    Ok(())
}

pub(super) fn publish_metadata_new(
    expected: &crate::model::ObjectRef,
    bytes: &[u8],
) -> Result<bool> {
    let path = Path::new(&expected.path);
    let parent = path
        .parent()
        .ok_or_else(|| Error::refusal("metadata_location", "candidate metadata needs a parent"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{}.candidate", uuid::Uuid::new_v4().simple()));
    super::write_new(&temporary, bytes)?;
    match std::fs::hard_link(&temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(&temporary)?;
            sync_candidate_parent(expected)?;
            return Ok(false);
        }
        Err(error) => return Err(error.into()),
    }
    std::fs::remove_file(&temporary)?;
    sync_candidate_parent(expected)?;
    Ok(true)
}

pub(super) fn sync_candidate_parent(expected: &crate::model::ObjectRef) -> Result<()> {
    let parent = Path::new(&expected.path)
        .parent()
        .ok_or_else(|| Error::refusal("metadata_location", "candidate metadata needs a parent"))?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn metadata_serialization_stops_at_exact_byte_limit() {
        let value = vec!["a\"b", "unicode α", "last"];
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(bounded_json(&value, expected.len()).unwrap(), expected);
        assert_eq!(
            bounded_json(&value, expected.len() - 1).unwrap_err().code(),
            "metadata_capacity"
        );
        assert_eq!(
            bounded_json(&value, 0).unwrap_err().code(),
            "metadata_capacity"
        );
        let mut output = BoundedJson {
            bytes: Vec::new(),
            limit: 17,
            exceeded: false,
        };
        for _ in 0..17 {
            output.write_all(b"a").unwrap();
            assert!(output.bytes.capacity() <= 17);
        }
        assert!(output.write_all(b"b").is_err());
        assert_eq!(output.bytes.len(), 17);
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use crate::budget::Limits;
    use iceberg::{
        NamespaceIdent,
        spec::{NestedField, PrimitiveType, Type},
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_metadata_worker_keeps_admission_until_durable_write_finishes() {
        let root = tempfile::TempDir::new().unwrap();
        let catalog = crate::storage::local_catalog(root.path()).await.unwrap();
        let namespace = NamespaceIdent::new("cancel".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let identifier = TableIdent::new(namespace, "candidate".into());
        let budget = Budget::new(Limits {
            max_metadata_bytes: 4096,
            // The wrapper half covers candidate metadata plus the blocking
            // worker's one-MiB allowance, with no spare admission.
            memory_bytes: 2 * (8192 + (1 << 20)),
            compute_threads: 1,
            ..Limits::default()
        })
        .unwrap();
        let directory = root.path().join("candidate");
        let destination = directory.clone();
        let worker_budget = budget.clone();
        let worker_catalog = catalog.clone();
        let worker_id = identifier.clone();
        let (entered, received) = tokio::sync::oneshot::channel();
        let (release, stalled) = std::sync::mpsc::channel();
        let worker = tokio::spawn(async move {
            register_candidate(
                worker_catalog.as_ref(),
                &worker_id,
                CandidateWriter::Local,
                move || {
                    entered.send(()).unwrap();
                    stalled.recv().unwrap();
                    fresh_metadata(
                        Schema::builder()
                            .with_fields([Arc::new(NestedField::optional(
                                1,
                                "id",
                                Type::Primitive(PrimitiveType::Long),
                            ))])
                            .build()
                            .unwrap(),
                        &destination,
                    )
                },
                &worker_budget,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(permit) = budget.reserve(budget.limits.memory_bytes / 2) {
                    drop(permit);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let objects = std::fs::read_dir(directory.join("metadata"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(objects.len(), 1);
        let _: TableMetadata =
            serde_json::from_slice(&std::fs::read(&objects[0]).unwrap()).unwrap();
        assert!(!catalog.table_exists(&identifier).await.unwrap());
    }
}
