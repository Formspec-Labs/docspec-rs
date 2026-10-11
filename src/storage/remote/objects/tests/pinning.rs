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
    let payload = b"immutable synthetic payload; pinning does not decode Parquet".to_vec();
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
