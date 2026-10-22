//! Candidate metadata preserving native field identity without copying value files.
// Rust guideline compliant 2026-02-21

use crate::{
    Error, Result,
    budget::Budget,
    model::{PartitionFieldSpec, PartitionTransformSpec},
};
use iceberg::{
    Catalog, MetadataLocation, TableIdent,
    spec::{
        FormatVersion, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadata,
        TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
    },
    table::Table,
};
use std::{collections::BTreeSet, path::Path, sync::Arc};

pub(crate) fn to_iceberg_transform(spec: &PartitionTransformSpec) -> Transform {
    match spec {
        PartitionTransformSpec::Identity => Transform::Identity,
        PartitionTransformSpec::Bucket { num_buckets } => Transform::Bucket(*num_buckets),
        PartitionTransformSpec::Year => Transform::Year,
        PartitionTransformSpec::Month => Transform::Month,
        PartitionTransformSpec::Day => Transform::Day,
        PartitionTransformSpec::Truncate { width } => Transform::Truncate(*width),
    }
}

pub(crate) fn from_iceberg_transform(transform: Transform) -> Option<PartitionTransformSpec> {
    match transform {
        Transform::Identity => Some(PartitionTransformSpec::Identity),
        Transform::Bucket(num_buckets) => Some(PartitionTransformSpec::Bucket { num_buckets }),
        Transform::Year => Some(PartitionTransformSpec::Year),
        Transform::Month => Some(PartitionTransformSpec::Month),
        Transform::Day => Some(PartitionTransformSpec::Day),
        Transform::Truncate(width) => Some(PartitionTransformSpec::Truncate { width }),
        _ => None,
    }
}

pub(crate) fn default_partition_field_name(
    column: &str,
    transform: &PartitionTransformSpec,
) -> String {
    match transform {
        PartitionTransformSpec::Identity => column.to_owned(),
        PartitionTransformSpec::Bucket { num_buckets } => {
            format!("{column}_bucket_{num_buckets}")
        }
        PartitionTransformSpec::Year => format!("{column}_year"),
        PartitionTransformSpec::Month => format!("{column}_month"),
        PartitionTransformSpec::Day => format!("{column}_day"),
        PartitionTransformSpec::Truncate { width } => format!("{column}_trunc_{width}"),
    }
}

pub(crate) fn build_unbound_partition_spec(
    schema: &Schema,
    specs: &[PartitionFieldSpec],
) -> Result<UnboundPartitionSpec> {
    if specs.is_empty() {
        return Ok(UnboundPartitionSpec::builder().build());
    }
    let mut builder = UnboundPartitionSpec::builder();
    let mut names = BTreeSet::new();
    for spec in specs {
        let Some(source) = schema.field_by_name(&spec.column) else {
            return Err(unsupported_partition_spec(
                "partition spec references an unknown source column",
            ));
        };
        if schema.as_struct().field_by_id(source.id).is_none() {
            return Err(unsupported_partition_spec(
                "partition spec must reference a top-level schema field",
            ));
        }
        let field_name = spec
            .name
            .clone()
            .unwrap_or_else(|| default_partition_field_name(&spec.column, &spec.transform));
        if field_name.trim().is_empty() || !names.insert(field_name.to_ascii_lowercase()) {
            return Err(unsupported_partition_spec(
                "partition spec field names must be non-empty and distinct",
            ));
        }
        builder = builder
            .add_partition_field(source.id, field_name, to_iceberg_transform(&spec.transform))
            .map_err(|_| {
                unsupported_partition_spec("partition spec could not be constructed")
            })?;
    }
    let unbound = builder.build();
    validate_unbound_partition_spec(schema, &unbound)?;
    Ok(unbound)
}

pub(crate) fn check_partition_by_matches(
    schema: &Schema,
    actual: &PartitionSpec,
    expected: &[PartitionFieldSpec],
) -> Result<()> {
    let unbound = build_unbound_partition_spec(schema, expected)?;
    let bound = unbound.bind(Arc::new(schema.clone())).map_err(|_| {
        unsupported_partition_spec("partition spec is incompatible with table schema")
    })?;
    let same = actual.fields().len() == bound.fields().len()
        && actual
            .fields()
            .iter()
            .zip(bound.fields().iter())
            .all(|(a, b)| {
                a.source_id == b.source_id && a.name == b.name && a.transform == b.transform
            });
    if !same {
        return Err(Error::refusal(
            "partition_spec_mismatch",
            "declared partition_by does not match the selected base partition spec",
        ));
    }
    Ok(())
}

pub(crate) fn partition_specs_from_metadata(
    metadata: &TableMetadata,
    declared: Option<&[PartitionFieldSpec]>,
) -> Vec<PartitionFieldSpec> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    spec.fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, pf)| {
            let source = schema.field_by_id(pf.source_id)?;
            let transform = from_iceberg_transform(pf.transform)?;
            let default_name = default_partition_field_name(&source.name, &transform);
            let name = if let Some(decl) = declared.and_then(|d| d.get(idx))
                && to_iceberg_transform(&decl.transform) == pf.transform
            {
                decl.name.clone()
            } else if pf.name == default_name {
                None
            } else {
                Some(pf.name.clone())
            };
            Some(PartitionFieldSpec {
                column: source.name.clone(),
                name,
                transform,
            })
        })
        .collect()
}

pub(crate) fn validate_partition_spec(schema: &Schema, spec: &PartitionSpec) -> Result<()> {
    if spec.fields().is_empty() {
        return Ok(());
    }
    for field in spec.fields() {
        let Some(source) = schema.as_struct().field_by_id(field.source_id) else {
            return Err(unsupported_partition_spec(
                "partition spec must reference a top-level schema field",
            ));
        };
        if super::identity::hidden(&source.name)
            || super::defaults::supplied_id(&source.name).is_some()
        {
            return Err(unsupported_partition_spec(
                "partition spec cannot reference internal occurrence identity fields",
            ));
        }
        let Type::Primitive(primitive) = source.field_type.as_ref() else {
            return Err(unsupported_partition_spec(
                "partition spec requires a primitive source field",
            ));
        };
        let supported = match (field.transform, primitive) {
            (
                Transform::Year | Transform::Month | Transform::Day,
                PrimitiveType::Date | PrimitiveType::Timestamptz | PrimitiveType::Timestamp,
            ) => true,
            (
                Transform::Identity,
                PrimitiveType::Boolean
                | PrimitiveType::Date
                | PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::String
                | PrimitiveType::Timestamptz
                | PrimitiveType::Timestamp
                | PrimitiveType::Uuid
                | PrimitiveType::Binary,
            ) => true,
            (
                Transform::Bucket(n),
                PrimitiveType::Date
                | PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::String
                | PrimitiveType::Timestamptz
                | PrimitiveType::Timestamp
                | PrimitiveType::Uuid
                | PrimitiveType::Binary,
            ) => n > 0,
            (
                Transform::Truncate(w),
                PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::String
                | PrimitiveType::Binary,
            ) => w > 0,
            _ => false,
        };
        if !supported {
            return Err(unsupported_partition_spec(
                "partition spec uses an unqualified transform or source type",
            ));
        }
    }
    spec.clone()
        .into_unbound()
        .bind(Arc::new(schema.clone()))
        .map_err(|_| {
            unsupported_partition_spec("partition spec is incompatible with table schema")
        })?;
    Ok(())
}

pub(crate) fn validate_unbound_partition_spec(
    schema: &Schema,
    spec: &UnboundPartitionSpec,
) -> Result<()> {
    if spec.fields().is_empty() {
        return Ok(());
    }
    let bound = spec.clone().bind(Arc::new(schema.clone())).map_err(|_| {
        unsupported_partition_spec("partition spec is incompatible with table schema")
    })?;
    validate_partition_spec(schema, &bound)
}

fn unsupported_partition_spec(message: &'static str) -> Error {
    Error::refusal("unsupported_write_layout", message)
}

pub(crate) fn fresh_metadata(schema: Schema, directory: &Path) -> Result<TableMetadata> {
    fresh_metadata_at(schema, &directory.to_string_lossy())
}

pub(crate) fn fresh_metadata_at(schema: Schema, location: &str) -> Result<TableMetadata> {
    fresh_partitioned_metadata_at(&schema, location, UnboundPartitionSpec::builder().build())
}

#[allow(dead_code)]
pub(crate) fn fresh_partitioned_metadata(
    schema: &Schema,
    directory: &Path,
    partition_spec: UnboundPartitionSpec,
) -> Result<TableMetadata> {
    fresh_partitioned_metadata_at(schema, directory.to_string_lossy(), partition_spec)
}

pub(crate) fn fresh_partitioned_metadata_at(
    schema: &Schema,
    location: impl AsRef<str>,
    partition_spec: UnboundPartitionSpec,
) -> Result<TableMetadata> {
    validate_unbound_partition_spec(schema, &partition_spec)?;
    // Native creation reassigns IDs. Reinstall the supplied schema before any
    // files exist, then bind any declared partition spec to those preserved IDs
    // and discard only the unused provisional schema and partition spec.
    let builder = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        location.as_ref().to_owned(),
        FormatVersion::V2,
        super::table_properties(),
    )?
    .add_current_schema(schema.clone())?;
    let builder = if partition_spec.fields().is_empty() {
        builder
    } else {
        builder.add_default_partition_spec(partition_spec)?
    };
    let metadata = builder.build()?.metadata;
    let unused_schemas = metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .filter(|id| *id != metadata.current_schema_id())
        .collect::<Vec<_>>();
    let unused_specs = metadata
        .partition_specs_iter()
        .map(|spec| spec.spec_id())
        .filter(|id| *id != metadata.default_partition_spec_id())
        .collect::<Vec<_>>();
    Ok(metadata
        .into_builder(None)
        .remove_schemas(&unused_schemas)?
        .remove_partition_specs(&unused_specs)?
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
        || !base.default_sort_order().is_unsorted()
        || base.encryption_keys_iter().next().is_some()
    {
        return Err(Error::refusal(
            "unsupported_write_layout",
            "replacement candidates require unsorted, unencrypted Iceberg v2/v3 metadata",
        ));
    }
    validate_partition_spec(base.current_schema(), base.default_partition_spec())?;
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
    use iceberg::spec::{NestedField, StructType, UnboundPartitionField};
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

    #[test]
    fn month_and_qualified_partition_specs_preserve_field_ids_through_fresh_and_replacement_metadata()
     {
        let schema = Schema::builder()
            .with_schema_id(7)
            .with_fields([
                Arc::new(NestedField::required(
                    10,
                    "observed_at",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    20,
                    "recorded_at",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::optional(
                    30,
                    "source_feed",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(10, "observed_at_month", Transform::Month)
            .unwrap()
            .add_partition_field(20, "recorded_at_month", Transform::Month)
            .unwrap()
            .add_partition_field(30, "source_feed", Transform::Identity)
            .unwrap()
            .build();
        let fresh = fresh_partitioned_metadata_at(&schema, "/tmp/news-lake-source", spec).unwrap();
        assert_eq!(fresh.current_schema().as_struct(), schema.as_struct());
        assert_eq!(fresh.current_schema().highest_field_id(), 30);
        assert_eq!(fresh.schemas_iter().count(), 1);
        assert_eq!(fresh.partition_specs_iter().count(), 1);
        let bound = fresh.default_partition_spec();
        assert_eq!(bound.fields().len(), 3);
        assert_eq!(bound.fields()[0].source_id, 10);
        assert_eq!(bound.fields()[0].name, "observed_at_month");
        assert_eq!(bound.fields()[0].transform, Transform::Month);
        assert_eq!(bound.fields()[1].source_id, 20);
        assert_eq!(bound.fields()[1].name, "recorded_at_month");
        assert_eq!(bound.fields()[1].transform, Transform::Month);
        assert_eq!(bound.fields()[2].source_id, 30);
        assert_eq!(bound.fields()[2].name, "source_feed");
        assert_eq!(bound.fields()[2].transform, Transform::Identity);

        let replaced = replacement_metadata_at(&fresh, "/tmp/news-lake-replacement").unwrap();
        assert_eq!(replaced.current_schema(), fresh.current_schema());
        assert_eq!(
            replaced.default_partition_spec(),
            fresh.default_partition_spec()
        );
        assert_eq!(replaced.location(), "/tmp/news-lake-replacement");
    }

    #[test]
    fn unqualified_transforms_and_source_types_are_refused_with_unsupported_write_layout() {
        let nested_child = Arc::new(NestedField::optional(
            99,
            "nested_date",
            Type::Primitive(PrimitiveType::Date),
        ));
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    10,
                    "observed_at",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    20,
                    "title",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    30,
                    "weight",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    40,
                    "nested",
                    Type::Struct(StructType::new(vec![nested_child])),
                )),
                Arc::new(NestedField::optional(
                    50,
                    super::super::identity::ORIGIN_FILE,
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        for valid in [
            UnboundPartitionSpec::builder()
                .add_partition_field(10, "observed_bucket", Transform::Bucket(16))
                .unwrap()
                .build(),
            UnboundPartitionSpec::builder()
                .add_partition_field(20, "title_trunc", Transform::Truncate(4))
                .unwrap()
                .build(),
            UnboundPartitionSpec::builder()
                .add_partition_field(10, "observed_year", Transform::Year)
                .unwrap()
                .build(),
        ] {
            validate_unbound_partition_spec(&schema, &valid).unwrap();
            fresh_partitioned_metadata_at(&schema, "/tmp/accepted", valid).unwrap();
        }

        for invalid in [
            // Zero-bucket transform is not qualified.
            UnboundPartitionSpec::builder()
                .add_partition_field(10, "observed_bucket_zero", Transform::Bucket(0))
                .unwrap()
                .build(),
            // Zero-width truncate transform is not qualified.
            UnboundPartitionSpec::builder()
                .add_partition_field(20, "title_trunc_zero", Transform::Truncate(0))
                .unwrap()
                .build(),
            // Truncate on a Date column is not qualified.
            UnboundPartitionSpec::builder()
                .add_partition_field(10, "observed_trunc", Transform::Truncate(4))
                .unwrap()
                .build(),
            // Void transform must be rejected even though Iceberg treats all-void as is_unpartitioned().
            UnboundPartitionSpec::builder()
                .add_partition_field(10, "observed_void", Transform::Void)
                .unwrap()
                .build(),
            // Month on a String column is invalid.
            UnboundPartitionSpec::builder()
                .add_partition_field(20, "title_month", Transform::Month)
                .unwrap()
                .build(),
            // Identity on a Double column is not qualified.
            UnboundPartitionSpec::builder()
                .add_partition_field(30, "weight", Transform::Identity)
                .unwrap()
                .build(),
            // Nested field source_id is not a top-level field.
            UnboundPartitionSpec::builder()
                .add_partition_field(99, "nested_month", Transform::Month)
                .unwrap()
                .build(),
            // Internal occurrence identity field cannot be partitioned on.
            UnboundPartitionSpec::builder()
                .add_partition_field(50, super::super::identity::ORIGIN_FILE, Transform::Identity)
                .unwrap()
                .build(),
        ] {
            assert_eq!(
                validate_unbound_partition_spec(&schema, &invalid)
                    .unwrap_err()
                    .code(),
                "unsupported_write_layout"
            );
            assert_eq!(
                fresh_partitioned_metadata_at(&schema, "/tmp/rejected", invalid)
                    .unwrap_err()
                    .code(),
                "unsupported_write_layout"
            );
        }

        // Also verify replacement_metadata_at rejects an unqualified Bucket(0) or Void partition spec.
        for bad_transform in [Transform::Bucket(0), Transform::Void] {
            let unvalidated = TableMetadataBuilder::new(
                schema.clone(),
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                "/tmp/bad-base".to_owned(),
                FormatVersion::V2,
                super::super::table_properties(),
            )
            .unwrap()
            .add_current_schema(schema.clone())
            .unwrap()
            .add_default_partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_fields([UnboundPartitionField::builder()
                        .source_id(10)
                        .name("bad_part".to_owned())
                        .transform(bad_transform)
                        .build()])
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            assert_eq!(
                replacement_metadata_at(&unvalidated, "/tmp/bad-replacement")
                    .unwrap_err()
                    .code(),
                "unsupported_write_layout"
            );
        }
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
