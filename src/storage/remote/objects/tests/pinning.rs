//! Populated native inventory reads and object identities share configured I/O.
use super::*;
use crate::storage::{Objects, pin_bound};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
    Operation, Snapshot, Summary, TableMetadata,
};

const ROOT: &str = "s3://bucket/owned/candidate";
const LIST: &str = "s3://bucket/owned/candidate/metadata/list.avro";
const MANIFEST: &str = "s3://bucket/owned/candidate/metadata/manifest.avro";
const DATA: &str = "s3://bucket/owned/candidate/data/one.parquet";

async fn native_inventory(
    list_uri: &str,
    manifest_uri: &str,
) -> (TableMetadata, HashMap<String, Vec<u8>>) {
    let temp = tempfile::tempdir().unwrap();
    let metadata = crate::storage::fresh_metadata(
        iceberg::spec::Schema::builder()
            .with_fields([Arc::new(iceberg::spec::NestedField::optional(
                7,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        std::path::Path::new(ROOT),
    )
    .unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "7".into())])),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![
            Some(11),
            None,
            Some(42),
        ]))],
    )
    .unwrap();
    let mut payload = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut payload, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let io = iceberg::io::FileIO::new_with_fs();
    let manifest_path = temp
        .path()
        .join("manifest.avro")
        .to_string_lossy()
        .into_owned();
    let mut writer = ManifestWriterBuilder::new(
        io.new_output(&manifest_path).unwrap(),
        Some(1),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    writer
        .add_file(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path(DATA.into())
                .file_size_in_bytes(payload.len() as u64)
                .record_count(3)
                .partition_spec_id(metadata.default_partition_spec_id())
                .build()
                .unwrap(),
            1,
        )
        .unwrap();
    let mut manifest = writer.write_manifest_file().await.unwrap();
    manifest.manifest_path = manifest_uri.into();
    let list_path = temp.path().join("list.avro").to_string_lossy().into_owned();
    let mut writer = ManifestListWriter::v2(
        io.new_output(&list_path).unwrap().writer().await.unwrap(),
        1,
        None,
        1,
    );
    writer.add_manifests(std::iter::once(manifest)).unwrap();
    writer.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_sequence_number(1)
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(list_uri)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    let metadata = metadata
        .into_builder(None)
        .set_branch_snapshot(snapshot, "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    (
        metadata,
        HashMap::from([
            (list_uri.into(), std::fs::read(list_path).unwrap()),
            (manifest_uri.into(), std::fs::read(manifest_path).unwrap()),
            (DATA.into(), payload),
        ]),
    )
}

fn candidate_table(
    metadata: TableMetadata,
    path: &str,
    native_objects: &RemoteObjects,
) -> iceberg::table::Table {
    iceberg::table::Table::builder()
        .metadata(metadata)
        .metadata_location(path)
        .identifier(iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("pin".into()),
            "candidate".into(),
        ))
        .file_io(native_objects.file_io())
        .runtime(iceberg::Runtime::try_current().unwrap())
        .readonly(true)
        .build()
        .unwrap()
}

#[tokio::test]
async fn populated_pin_binds_native_inventory_and_identities_to_configured_backend() {
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let expected = contents
        .iter()
        .map(|(path, bytes)| ObjectRef {
            owner: Id::new("candidate-owner").unwrap(),
            path: path.clone(),
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        })
        .collect::<Vec<_>>();
    // Arbitrary native FileIO at endpoint A must never feed the bound inventory.
    let endpoint_a = ReadFixture::inventory(HashMap::new());
    let endpoint_b = ReadFixture::inventory(contents);
    let table = candidate_table(metadata, &location, &endpoint_a.objects());
    let binding = Objects::Remote(endpoint_b.objects())
        .bind_table(&table)
        .unwrap();
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let pin = pin_bound(
        &Id::new("candidate-owner").unwrap(),
        &binding,
        3,
        &budget,
        &[],
    )
    .await
    .unwrap();
    assert_eq!(pin.rows, 3);
    assert_eq!(pin.snapshot, Some(1));
    assert_eq!(pin.schema_id, table.metadata().current_schema_id());
    for identity in &expected {
        assert!(pin.objects.contains(identity));
    }
    assert_eq!(pin.objects.len(), expected.len());
    assert_eq!(pin.metadata.path, location);
    assert!(
        endpoint_a.finish().is_empty(),
        "no inventory read through arbitrary native FileIO"
    );
    let paths = endpoint_b.paths.lock().unwrap().clone();
    endpoint_b.finish();
    assert_eq!(
        paths
            .iter()
            .filter(|(method, path)| method == "GET" && path == DATA)
            .count(),
        1,
        "bulk payload has one identity GET"
    );
    // Reuse the complete exact closure with an original owner. Native list and
    // manifest decoding still occurs once each; there are no identity GETs.
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let shared = contents
        .iter()
        .map(|(path, bytes)| ObjectRef {
            owner: Id::new("original-owner").unwrap(),
            path: path.clone(),
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        })
        .collect::<Vec<_>>();
    let fixture = ReadFixture::inventory(contents);
    let objects = fixture.objects();
    let table = candidate_table(metadata, &location, &objects);
    let binding = Objects::Remote(objects).bind_table(&table).unwrap();
    let pin = pin_bound(
        &Id::new("new-owner").unwrap(),
        &binding,
        3,
        &budget,
        &shared,
    )
    .await
    .unwrap();
    for identity in &shared {
        assert!(pin.objects.contains(identity));
    }
    let paths = fixture.paths.lock().unwrap().clone();
    fixture.finish();
    let gets = paths
        .iter()
        .filter(|(method, _)| method == "GET")
        .map(|(_, path)| path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        gets,
        vec![LIST, MANIFEST],
        "inherited payload and metadata never rehash"
    );
}

#[tokio::test]
async fn populated_pin_guards_manifest_list_and_manifest_before_native_read() {
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    for (list, manifest, rejected, inherited) in [
        (
            "s3://bucket/elsewhere/list.avro",
            MANIFEST,
            "s3://bucket/elsewhere/list.avro",
            false,
        ),
        (
            "s3://bucket/elsewhere/list.avro",
            MANIFEST,
            "s3://bucket/elsewhere/list.avro",
            true,
        ),
        (
            LIST,
            "s3://bucket/elsewhere/manifest.avro",
            "s3://bucket/elsewhere/manifest.avro",
            false,
        ),
        (
            LIST,
            "s3://bucket/elsewhere/manifest.avro",
            "s3://bucket/elsewhere/manifest.avro",
            true,
        ),
        (
            "s3://bucket/owned/other/list.avro",
            MANIFEST,
            "s3://bucket/owned/other/list.avro",
            false,
        ),
        (
            LIST,
            "s3://bucket/owned/other/manifest.avro",
            "s3://bucket/owned/other/manifest.avro",
            false,
        ),
    ] {
        let (metadata, mut contents) = native_inventory(list, manifest).await;
        let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
        contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
        let shared = if inherited {
            let bytes = contents.get(rejected).unwrap();
            vec![ObjectRef {
                owner: Id::new("original-owner").unwrap(),
                path: rejected.into(),
                digest: blake3::hash(bytes).to_hex().to_string(),
                bytes: bytes.len() as u64,
            }]
        } else {
            Vec::new()
        };
        let fixture = ReadFixture::inventory(contents);
        let objects = fixture.objects();
        let table = candidate_table(metadata, &location, &objects);
        let binding = Objects::Remote(objects).bind_table(&table).unwrap();
        let code = if rejected.starts_with("s3://bucket/elsewhere/") {
            "remote_scope"
        } else {
            "unprotected_reference"
        };
        assert_eq!(
            pin_bound(
                &Id::new("candidate-owner").unwrap(),
                &binding,
                3,
                &budget,
                &shared
            )
            .await
            .unwrap_err()
            .code(),
            code
        );
        let paths = fixture.paths.lock().unwrap().clone();
        fixture.finish();
        assert!(
            paths.iter().all(|(_, path)| path != rejected),
            "rejected URI received no HEAD or GET"
        );
        if rejected == list {
            assert!(paths.is_empty());
        } else {
            assert!(
                paths.iter().all(|(_, path)| path == list),
                "only allowed list was decoded"
            );
        }
    }
}

// Rust guideline compliant 2026-02-21

#[tokio::test]
async fn populated_gzip_selected_metadata_preserves_absent_write_property_and_shared_identities() {
    use std::io::Write;
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    let metadata = metadata
        .into_builder(None)
        .remove_properties(&["write.metadata.compression-codec".into()])
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert!(
        !metadata
            .properties()
            .contains_key("write.metadata.compression-codec")
    );
    assert_eq!(
        metadata.metadata_compression_codec().unwrap(),
        iceberg::compression::CompressionCodec::None
    );
    let plain_location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    let location = format!(
        "{}.gz.metadata.json",
        plain_location.strip_suffix(".metadata.json").unwrap()
    );
    let parsed: iceberg::MetadataLocation = location.parse().unwrap();
    assert_eq!(parsed.to_string(), location);
    assert_ne!(
        parsed.compression_codec(),
        metadata.metadata_compression_codec().unwrap()
    );
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(&serde_json::to_vec(&metadata).unwrap())
        .unwrap();
    let bytes = encoder.finish().unwrap();
    assert!(bytes.starts_with(&[0x1f, 0x8b]));
    contents.insert(location.clone(), bytes.clone());
    let fixture = ReadFixture::inventory(contents);
    let objects = fixture.objects();
    let table = candidate_table(metadata.clone(), &location, &objects);
    let binding = Objects::Remote(objects.clone()).bind_table(&table).unwrap();
    let owner = Id::new("original-owner").unwrap();
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let pin = pin_bound(&owner, &binding, 3, &budget, &[]).await.unwrap();
    assert_eq!(
        pin.metadata,
        ObjectRef {
            owner: owner.clone(),
            path: location.clone(),
            bytes: bytes.len() as u64,
            digest: blake3::hash(&bytes).to_hex().to_string(),
        }
    );
    assert_eq!(pin.snapshot, Some(1));
    assert_eq!(pin.schema_id, metadata.current_schema_id());
    assert_eq!(pin.rows, 3);
    assert!(pin.objects.iter().any(|object| object.path == DATA));
    let initial = fixture.paths.lock().unwrap().clone();
    assert_eq!(
        initial
            .iter()
            .filter(|(method, path)| method == "GET" && path == &location)
            .count(),
        1
    );
    assert_eq!(
        initial
            .iter()
            .filter(|(method, path)| method == "GET" && path == DATA)
            .count(),
        1
    );
    // Known compressed identity remains owned by the original producer. Native
    // manifests decode once; inherited metadata and payload receive no GET/hash.
    let reused = pin_bound(
        &Id::new("consumer").unwrap(),
        &binding,
        3,
        &budget,
        &pin.objects,
    )
    .await
    .unwrap();
    assert_eq!(reused.metadata, pin.metadata);
    assert_eq!(reused.objects, pin.objects);
    let after_reuse = fixture.paths.lock().unwrap().clone();
    assert_eq!(
        after_reuse
            .iter()
            .filter(|(method, path)| method == "GET" && path == &location)
            .count(),
        1
    );
    assert_eq!(
        after_reuse
            .iter()
            .filter(|(method, path)| method == "GET" && path == DATA)
            .count(),
        1
    );
    // Independent pinned open consumes its one bounded compressed metadata read,
    // preserving URI/property meaning without reopening data or producer catalog.
    let reopened = crate::storage::open_pin_with_io(&reused, objects.file_io(), &budget)
        .await
        .unwrap();
    assert_eq!(reopened.metadata(), &metadata);
    assert_eq!(reopened.metadata_location(), Some(location.as_str()));
    assert!(
        !reopened
            .metadata()
            .properties()
            .contains_key("write.metadata.compression-codec")
    );
    let final_paths = fixture.paths.lock().unwrap().clone();
    fixture.finish();
    assert_eq!(
        final_paths
            .iter()
            .filter(|(method, path)| method == "GET" && path == &location)
            .count(),
        2
    );
    assert_eq!(
        final_paths
            .iter()
            .filter(|(method, path)| method == "GET" && path == DATA)
            .count(),
        1
    );
}

#[tokio::test]
async fn configured_inspection_reuses_one_inventory_and_bounded_samples_then_streams_values() {
    use futures::TryStreamExt;
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let fixture = ReadFixture::inventory(contents);
    let objects = Objects::Remote(fixture.objects());
    let table = candidate_table(metadata, &location, &fixture.objects());
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let pin = pin_bound(
        &Id::new("owner").unwrap(),
        &objects.bind_table(&table).unwrap(),
        3,
        &budget,
        &[],
    )
    .await
    .unwrap();
    let before = fixture.paths.lock().unwrap().len();
    let (inspection, known) = crate::storage::inspect_table(
        &objects,
        &pin,
        &budget,
        1,
        budget.limits.max_batch_bytes,
        std::collections::BTreeMap::new(),
        Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(inspection.files, 1);
    assert_eq!(inspection.sampled_files, 1);
    assert_eq!(inspection.sampled_rows, 3);
    let after_inspection = fixture.paths.lock().unwrap().clone();
    for path in [LIST, MANIFEST, location.as_str()] {
        assert_eq!(
            after_inspection[before..]
                .iter()
                .filter(|(method, uri)| method == "GET" && uri == path)
                .count(),
            1,
            "routine inspection reads each native metadata object once"
        );
    }
    // Reuse known physical evidence: native inventory may be decoded, but data
    // receives neither footer checks, sampling nor another whole-file hash.
    let before_reuse = after_inspection.len();
    let (reused, _) = crate::storage::inspect_table(
        &objects,
        &pin,
        &budget,
        1,
        budget.limits.max_batch_bytes,
        known,
        Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(reused.files, 0);
    assert_eq!(reused.sampled_files, 0);
    let after_reuse = fixture.paths.lock().unwrap().clone();
    assert!(
        after_reuse[before_reuse..]
            .iter()
            .all(|(method, uri)| method != "GET" || uri != DATA)
    );
    let before_read = after_reuse.len();
    // Same exact-pin open used by Engine read_group; no producing catalog or
    // original source path is consulted and no creation inspection runs.
    let opened = objects.open_pin(&pin, &budget).await.unwrap();
    assert_eq!(
        opened.metadata().current_schema().as_struct().fields()[0].id,
        7
    );
    let batches = crate::storage::read_batches(&opened, &budget)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(11), None, Some(42)]);
    let paths = fixture.paths.lock().unwrap().clone();
    for path in [LIST, MANIFEST, location.as_str()] {
        assert_eq!(
            paths[before_read..]
                .iter()
                .filter(|(method, uri)| method == "GET" && uri == path)
                .count(),
            1
        );
    }
    let before_available = paths.len();
    objects
        .check_available(pin.objects.clone(), &budget)
        .await
        .unwrap();
    let paths = fixture.paths.lock().unwrap().clone();
    assert!(
        paths[before_available..]
            .iter()
            .all(|(method, _)| method == "HEAD")
    );
    assert_eq!(paths[before_available..].len(), pin.objects.len());
    fixture.finish();
}

#[tokio::test]
async fn configured_pin_open_and_availability_refuse_entire_scope_before_io() {
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let fixture = ReadFixture::inventory(contents);
    let objects = Objects::Remote(fixture.objects());
    let table = candidate_table(metadata, &location, &fixture.objects());
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let mut pin = pin_bound(
        &Id::new("owner").unwrap(),
        &objects.bind_table(&table).unwrap(),
        3,
        &budget,
        &[],
    )
    .await
    .unwrap();
    let before = fixture.paths.lock().unwrap().len();
    pin.objects.last_mut().unwrap().path = "s3://bucket/elsewhere/unqualified.parquet".into();
    assert_eq!(
        objects.open_pin(&pin, &budget).await.unwrap_err().code(),
        "remote_scope"
    );
    assert_eq!(
        objects
            .check_available(pin.objects.clone(), &budget)
            .await
            .unwrap_err()
            .code(),
        "remote_scope"
    );
    assert_eq!(fixture.paths.lock().unwrap().len(), before);
    fixture.finish();
}

#[tokio::test]
async fn configured_inspection_preserves_evolved_nullable_and_default_field_ids() {
    use futures::TryStreamExt;
    use iceberg::spec::{FormatVersion, Literal, NestedField, PrimitiveType, Type};
    let (metadata, mut contents) = native_inventory(LIST, MANIFEST).await;
    // Retain the physical original schema while the selected interpretation adds
    // a nullable column and an initial default under distinct sparse native IDs.
    let evolved = iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields([
            metadata.current_schema().as_struct().fields()[0].clone(),
            Arc::new(NestedField::optional(
                42,
                "missing",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(
                NestedField::optional(73, "defaulted", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(Literal::long(99))
                    .with_write_default(Literal::long(99)),
            ),
        ])
        .build()
        .unwrap();
    let metadata = metadata
        .into_builder(None)
        .upgrade_format_version(FormatVersion::V3)
        .unwrap()
        .add_current_schema(evolved)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let fixture = ReadFixture::inventory(contents);
    let objects = Objects::Remote(fixture.objects());
    let table = candidate_table(metadata, &location, &fixture.objects());
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let pin = pin_bound(
        &Id::new("owner").unwrap(),
        &objects.bind_table(&table).unwrap(),
        3,
        &budget,
        &[],
    )
    .await
    .unwrap();
    let before = fixture.paths.lock().unwrap().len();
    let (inspection, _) = crate::storage::inspect_table(
        &objects,
        &pin,
        &budget,
        1,
        budget.limits.max_batch_bytes,
        std::collections::BTreeMap::new(),
        Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(inspection.sampled_rows, 3);
    let inspected = fixture.paths.lock().unwrap().clone();
    for path in [LIST, MANIFEST] {
        assert_eq!(
            inspected[before..]
                .iter()
                .filter(|(method, uri)| method == "GET" && uri == path)
                .count(),
            1
        );
    }
    let table = objects.open_pin(&pin, &budget).await.unwrap();
    let ids = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![7, 42, 73]);
    let batches = crate::storage::read_batches(&table, &budget)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let values = |column| {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values(0), vec![Some(11), None, Some(42)]);
    assert_eq!(values(1), vec![None, None, None]);
    assert_eq!(values(2), vec![Some(99), Some(99), Some(99)]);
    fixture.finish();
}

#[tokio::test]
async fn configured_inspection_refuses_manifest_scope_before_native_decode() {
    let rejected = "s3://bucket/elsewhere/list.avro";
    let (metadata, mut contents) = native_inventory(rejected, MANIFEST).await;
    let location = iceberg::MetadataLocation::new_with_metadata(ROOT, &metadata).to_string();
    contents.insert(location.clone(), serde_json::to_vec(&metadata).unwrap());
    let identities = contents
        .iter()
        .filter(|(path, _)| path.as_str() != rejected)
        .map(|(path, bytes)| ObjectRef {
            owner: Id::new("owner").unwrap(),
            path: path.clone(),
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        })
        .collect::<Vec<_>>();
    let pin = crate::model::TablePin {
        table: "candidate".into(),
        metadata: identities
            .iter()
            .find(|object| object.path == location)
            .unwrap()
            .clone(),
        snapshot: Some(1),
        schema_id: metadata.current_schema_id(),
        objects: identities,
        rows: 3,
        logical_metadata: None,
    };
    let fixture = ReadFixture::inventory(contents);
    let objects = Objects::Remote(fixture.objects());
    let budget = Budget::new(Limits {
        compute_threads: 1,
        ..Limits::default()
    })
    .unwrap();
    let error = crate::storage::inspect_table(
        &objects,
        &pin,
        &budget,
        1,
        budget.limits.max_batch_bytes,
        std::collections::BTreeMap::new(),
        Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap()),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.code(), "remote_scope");
    let paths = fixture.paths.lock().unwrap().clone();
    assert!(paths.iter().all(|(_, path)| path != rejected));
    assert!(
        paths
            .iter()
            .all(|(method, path)| method != "GET" || path == &location)
    );
    fixture.finish();
}
