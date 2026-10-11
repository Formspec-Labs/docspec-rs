//! Qualifies remote registration of DocSpec-owned immutable native candidate metadata.
//!
//! Requires a fresh synthetic namespace and retains its metadata and registration.
//! This probes registration, schema preservation, fresh catalog loading and an
//! independent catalog-free native metadata read; it does not append data or
//! qualify a remote dataset flow. If the catalog rewrites metadata, one bounded
//! read binds the returned object's identity before the independent native open.
// Rust guideline compliant 2026-02-21

use docspec::{
    Error, Result,
    budget::{Budget, Limits},
    model::{Id, ObjectRef, TablePin},
    storage::{
        open_pin_with_io,
        remote::{RemoteStorage, remote_storage},
    },
};
use iceberg::{
    MetadataLocation, NamespaceIdent, TableIdent,
    spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadata,
        TableMetadataBuilder, Type,
    },
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("r2_candidate_probe failed: {}", error.code());
        std::process::exit(1);
    }
}

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| Error::refusal("remote_config", "required probe configuration is missing"))
}

async fn connect(root: &str) -> Result<RemoteStorage> {
    remote_storage(
        root,
        HashMap::from([
            ("uri".into(), env("DOCSPEC_CATALOG_URI")?),
            ("warehouse".into(), env("DOCSPEC_CATALOG_WAREHOUSE")?),
            ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::from([
            ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_S3_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await
}

fn provider<T>(phase: &str, result: iceberg::Result<T>) -> Result<T> {
    result.map_err(|error| {
        eprintln!("{}", json!({"phase": phase, "kind": error.kind().to_string(), "retryable": error.retryable()}));
        Error::refusal("probe_provider", "provider operation failed; retain exact probe references")
    })
}

fn object(owner: &Id, path: String, bytes: &[u8]) -> ObjectRef {
    ObjectRef {
        owner: owner.clone(),
        path,
        digest: blake3::hash(bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    }
}

fn candidate(root: &str) -> Result<TableMetadata> {
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                7,
                "text",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                42,
                "counter",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()?;
    // Match fresh_metadata: reinstall the supplied IDs after native provisional
    // creation, then remove only the unused provisional schema.
    let metadata = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        root.into(),
        FormatVersion::V2,
        HashMap::from([
            ("commit.retry.num-retries".into(), "0".into()),
            (
                "write.metadata.delete-after-commit.enabled".into(),
                "false".into(),
            ),
            ("gc.enabled".into(), "false".into()),
        ]),
    )?
    .add_current_schema(schema)?
    .build()?
    .metadata;
    let unused = metadata
        .schemas_iter()
        .map(|schema| schema.schema_id())
        .filter(|id| *id != metadata.current_schema_id())
        .collect::<Vec<_>>();
    Ok(metadata
        .into_builder(None)
        .remove_schemas(&unused)?
        .build()?
        .metadata)
}

async fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 || args[0] != "--allow-synthetic-writes" {
        return Err(Error::refusal(
            "probe_opt_in",
            "supply --allow-synthetic-writes and a fresh qualification UUID",
        ));
    }
    let suffix = args[1]
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("probe_prefix", "qualification prefix required"))?;
    if uuid::Uuid::parse_str(suffix)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(suffix)
    {
        return Err(Error::refusal(
            "probe_prefix",
            "canonical qualification UUID required",
        ));
    }
    let owner = Id::new(args[1].clone())?;
    // Basin registration requires locations beneath its catalog storage profile.
    let root = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{owner}/candidate-probe",
        env("DOCSPEC_S3_BUCKET")?
    );
    let namespace = NamespaceIdent::new(format!("candidate_{}", suffix.replace('-', "_")));
    let ident = TableIdent::new(namespace.clone(), "candidate".into());
    let expected = candidate(&root)?;
    let metadata_location = MetadataLocation::new_with_metadata(&root, &expected);
    let path = metadata_location.to_string();
    let bytes = serde_json::to_vec(&expected)?;
    let expected_object = object(&owner, path.clone(), &bytes);
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 16,
        max_batch_bytes: 4096,
        max_metadata_bytes: 1 << 20,
    })?;
    if bytes.len() > budget.limits.max_metadata_bytes {
        return Err(Error::refusal(
            "probe_metadata_capacity",
            "synthetic metadata exceeds the declared allowance",
        ));
    }
    let remote = connect(&root).await?;
    if provider(
        "namespace_exists",
        remote.catalog.namespace_exists(&namespace).await,
    )? {
        return Err(Error::refusal(
            "probe_existing_namespace",
            "use a fresh qualification UUID",
        ));
    }
    println!(
        "{}",
        json!({"event": "start", "root": root, "namespace": namespace.to_string(),
        "table": ident.to_string(), "metadata": expected_object, "uuid": expected.uuid().to_string(),
        "current_schema_id": expected.current_schema_id(), "field_ids": [7, 42],
        "scope": "candidate metadata registration only; remote objects and namespace retained"})
    );
    provider(
        "create_namespace",
        remote
            .catalog
            .create_namespace(&namespace, HashMap::new())
            .await,
    )?;
    println!(
        "{}",
        json!({"event": "object_create_start", "namespace": namespace.to_string(), "expected": expected_object})
    );
    println!(
        "{}",
        json!({"event": "register_start", "table": ident.to_string(), "metadata": expected_object})
    );
    let registered = remote
        .register_candidate(&ident, metadata_location, expected.clone(), &budget)
        .await
        .map_err(|error| {
            if let Some(provider_error) = std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<iceberg::Error>())
            {
                eprintln!("{}", json!({"phase": "register_candidate", "kind": provider_error.kind().to_string(), "retryable": provider_error.retryable()}));
                Error::refusal("probe_provider", "provider operation failed; retain exact probe references")
            } else {
                error
            }
        })?;
    let metadata_path = provider(
        "registered_metadata_location",
        registered.metadata_location_result(),
    )?
    .to_owned();
    let actual = registered.metadata();
    let field_ids = actual
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    println!(
        "{}",
        json!({"event": "registered", "table_location": actual.location(),
        "uuid": actual.uuid().to_string(), "field_ids": field_ids,
        "current_schema_id": actual.current_schema_id(), "metadata_location": metadata_path,
        "metadata_rewritten": metadata_path != path})
    );
    if registered.identifier() != &ident
        || actual.location() != expected.location()
        || actual.uuid() != expected.uuid()
        || actual.format_version() != expected.format_version()
        || actual.current_schema() != expected.current_schema()
        || actual.last_column_id() != expected.last_column_id()
        || actual.current_snapshot_id() != expected.current_snapshot_id()
        || expected
            .properties()
            .iter()
            .any(|(key, value)| actual.properties().get(key) != Some(value))
    {
        return Err(Error::refusal(
            "probe_registration_changed",
            "registered candidate changed intended native meaning",
        ));
    }
    let fresh = connect(&root).await?;
    let loaded = provider("fresh_load_table", fresh.catalog.load_table(&ident).await)?;
    if loaded.metadata() != actual
        || provider("fresh_metadata_location", loaded.metadata_location_result())? != metadata_path
    {
        return Err(Error::refusal(
            "probe_fresh_load",
            "fresh catalog load differs from acknowledged registration",
        ));
    }
    let retained = if metadata_path == path {
        expected_object
    } else {
        // Reads may inspect the returned immutable metadata, but no writable
        // scope is expanded. Refuse a returned path outside this owned prefix.
        if !metadata_path.starts_with(&format!("{root}/")) {
            return Err(Error::refusal(
                "probe_metadata_scope",
                "returned metadata lies outside the qualification prefix",
            ));
        }
        let input = provider(
            "returned_metadata_input",
            remote.objects.file_io().new_input(&metadata_path),
        )?;
        let size = provider("returned_metadata_size", input.metadata().await)?.size;
        if size > budget.limits.max_metadata_bytes as u64 {
            return Err(Error::refusal(
                "probe_metadata_capacity",
                "returned metadata exceeds the declared allowance",
            ));
        }
        let _memory = budget.reserve(usize::try_from(size).map_err(|_| {
            Error::refusal(
                "probe_metadata_capacity",
                "returned metadata size cannot be admitted",
            )
        })?)?;
        println!(
            "{}",
            json!({"event": "returned_metadata_identity_start", "path": metadata_path, "bytes": size})
        );
        let reader = provider("returned_metadata_reader", input.reader().await)?;
        let bytes = provider("returned_metadata_read", reader.read(0..size).await)?;
        if bytes.len() as u64 != size
            || provider("returned_metadata_size_after", input.metadata().await)?.size != size
        {
            return Err(Error::refusal(
                "probe_metadata_changed",
                "returned metadata changed during identity capture",
            ));
        }
        object(&owner, metadata_path, &bytes)
    };
    let pin = TablePin {
        table: ident.to_string(),
        metadata: retained.clone(),
        snapshot: actual.current_snapshot_id(),
        schema_id: actual.current_schema_id(),
        objects: vec![retained],
        rows: 0,
        logical_metadata: None,
    };
    println!("{}", json!({"event": "native_open_start", "pin": pin}));
    let opened = open_pin_with_io(&pin, remote.objects.file_io(), &budget).await?;
    if opened.metadata() != actual {
        return Err(Error::refusal(
            "probe_native_readback",
            "catalog-free native read differs from registered metadata",
        ));
    }
    println!(
        "{}",
        json!({"event": "complete", "passed": true, "pin": pin,
        "fresh_catalog_readback": true, "catalog_free_native_readback": true,
        "scope": "metadata registration and preservation only; no payload or remote engine qualification"})
    );
    Ok(())
}
