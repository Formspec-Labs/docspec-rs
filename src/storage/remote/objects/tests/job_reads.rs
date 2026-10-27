//! A pinned immutable metadata response carries complete size without HEAD.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::model::TablePin;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
fn metadata() -> iceberg::spec::TableMetadata {
    crate::storage::fresh_metadata(
        Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "value",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        std::path::Path::new("s3://bucket/owned/table"),
    )
    .unwrap()
}
fn pin(bytes: &[u8], metadata: &iceberg::spec::TableMetadata) -> TablePin {
    let object = ObjectRef {
        owner: Id::new("read-fixture").unwrap(),
        path: "s3://bucket/owned/table/metadata.json".into(),
        digest: blake3::hash(bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    };
    TablePin {
        table: "table".into(),
        metadata: object.clone(),
        snapshot: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        objects: vec![object],
        rows: 0,
        logical_metadata: None,
    }
}
#[tokio::test]
async fn pinned_metadata_is_one_get_without_head_for_plain_and_gzip() {
    let metadata = metadata();
    let plain = serde_json::to_vec(&metadata).unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&plain).unwrap();
    let gzip = gzip.finish().unwrap();
    for bytes in [plain, gzip] {
        let pin = pin(&bytes, &metadata);
        let fixture = ReadFixture::inventory(HashMap::from([(pin.metadata.path.clone(), bytes)]));
        let _objects = fixture.objects();
        let budget = Budget::new(Limits::default()).unwrap();
        let loaded = crate::storage::open_pin(&pin, &budget).await.unwrap();
        assert_eq!(loaded.metadata().uuid(), metadata.uuid());
        assert_eq!(
            fixture.finish(),
            vec![(
                "GET".into(),
                Some(format!("bytes=0-{}", pin.metadata.bytes - 1))
            )]
        );
    }
}
#[tokio::test]
async fn pinned_get_refuses_observed_suffix_truncation_and_digest_change() {
    let metadata = metadata();
    let plain = serde_json::to_vec(&metadata).unwrap();
    let pin = pin(&plain, &metadata);
    let mut suffix = plain.clone();
    suffix.extend_from_slice(b"unexpected");
    let mut changed = plain.clone();
    changed[0] = b'[';
    for bytes in [suffix, plain[..3].to_vec(), changed] {
        let fixture = ReadFixture::inventory(HashMap::from([(pin.metadata.path.clone(), bytes)]));
        let _objects = fixture.objects();
        assert!(
            crate::storage::open_pin(&pin, &Budget::new(Limits::default()).unwrap())
                .await
                .is_err()
        );
        let requests = fixture.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "GET");
    }
}

#[tokio::test]
async fn explicit_remote_job_reuses_its_connection_without_lifecycle() {
    let metadata = metadata();
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let pin = pin(&bytes, &metadata);
    let fixture = ReadFixture::inventory(HashMap::from([(pin.metadata.path.clone(), bytes)]));
    let root = tempfile::tempdir().unwrap();
    let catalog = crate::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let remote = crate::storage::remote::RemoteStorage {
        catalog,
        objects: fixture.objects(),
        maintenance_generation: None,
    };
    let spec:crate::job::JobSpec=serde_json::from_value(serde_json::json!({"scratch":root.path().join("scratch"),"inputs":[{"kind":"iceberg","name":"pinned","pin":pin}],"steps":[],"outputs":[],"queries":[{"name":"count","sql":"SELECT count(*) AS n FROM pinned"}]})).unwrap();
    let result = spec
        .run_with_remote(Limits::default(), remote)
        .await
        .unwrap();
    assert_eq!(result.queries[0].data, serde_json::json!([{"n":0}]));
    assert!(!root.path().join("workspace").exists());
    assert_eq!(
        fixture.finish(),
        vec![(
            "GET".into(),
            Some(format!("bytes=0-{}", pin.metadata.bytes - 1))
        )]
    );
}
