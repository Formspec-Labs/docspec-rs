//! Qualifies remote registration of DocSpec-owned immutable native candidate metadata.
//!
//! Requires a fresh synthetic namespace and retains its metadata and registration.
//! This probes registration, schema preservation, fresh catalog loading and an
//! independent catalog-free native metadata read. Optional `--append-tiny` adds
//! one native data append and exact value readback; neither qualifies an Engine flow. If the catalog rewrites metadata, one bounded
//! read binds the returned object's identity before the independent native open.
// Rust guideline compliant 2026-02-21

use arrow::{
    array::{Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use docspec::{
    Error, Result,
    budget::{Budget, Limits},
    model::{Id, ObjectRef, TablePin},
    storage::{
        open_pin_with_io,
        remote::{RemoteStorage, remote_storage},
    },
};
use futures::TryStreamExt;
use iceberg::{
    MetadataLocation, NamespaceIdent, TableIdent,
    spec::{
        DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema,
        SortOrder, TableMetadata, TableMetadataBuilder, Type,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        file_writer::{ParquetWriterBuilder, rolling_writer::RollingFileWriterBuilder},
        partitioning::unpartitioned_writer::UnpartitionedWriter,
    },
};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

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
    let append = args.get(2).is_some_and(|flag| flag == "--append-tiny");
    if !(args.len() == 2 || (args.len() == 3 && append)) || args[0] != "--allow-synthetic-writes" {
        return Err(Error::refusal(
            "probe_opt_in",
            "supply --allow-synthetic-writes, a fresh qualification UUID, and optional --append-tiny",
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
        json!({"event": if append { "registration_complete" } else { "complete" }, "passed": true, "pin": pin,
        "fresh_catalog_readback": true, "catalog_free_native_readback": true,
        "scope": "metadata registration and preservation only; no payload or remote engine qualification"})
    );
    if append {
        append_tiny(&remote, &registered, &owner, &root, &budget).await?;
    }
    Ok(())
}

fn scoped(root: &str, path: &str) -> Result<()> {
    if !path.strip_prefix(&format!("{root}/")).is_some_and(|tail| {
        !tail.is_empty()
            && !tail.contains('%')
            && !tail.contains('?')
            && !tail.contains('#')
            && !tail.contains('\\')
            && tail
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
    }) {
        return Err(Error::refusal(
            "probe_metadata_scope",
            "native append reference escapes its retained qualification prefix",
        ));
    }
    Ok(())
}

async fn exact_object(
    remote: &RemoteStorage,
    owner: &Id,
    root: &str,
    path: &str,
    budget: &Budget,
) -> Result<ObjectRef> {
    scoped(root, path)?;
    let input = provider(
        "append_object_input",
        remote.objects.file_io().new_input(path),
    )?;
    let size = provider("append_object_size", input.metadata().await)?.size;
    // This fixture writes three rows; refuse unexpected object growth before reading.
    if size > 64 << 10 {
        return Err(Error::refusal(
            "probe_object_capacity",
            "tiny append object exceeds 64 KiB fixture allowance",
        ));
    }
    let _memory = budget.reserve(size as usize)?;
    let reader = provider("append_object_reader", input.reader().await)?;
    let bytes = provider("append_object_read", reader.read(0..size).await)?;
    if bytes.len() as u64 != size
        || provider("append_object_size_after", input.metadata().await)?.size != size
    {
        return Err(Error::refusal(
            "probe_metadata_changed",
            "append object changed during bounded identity capture",
        ));
    }
    Ok(object(owner, path.into(), &bytes))
}

async fn append_tiny(
    remote: &RemoteStorage,
    registered: &Table,
    owner: &Id,
    root: &str,
    budget: &Budget,
) -> Result<()> {
    if registered.metadata().location() != root
        || registered.metadata().current_snapshot_id().is_some()
    {
        return Err(Error::refusal(
            "probe_append_base",
            "tiny append requires the fresh exact empty registered candidate",
        ));
    }
    scoped(root, registered.metadata_location_result()?)?;
    // Native writers and manifest producers retain this exact table location.
    // A trusted REST catalog chooses its own new metadata filename; record and
    // check its actual placement after acknowledgement before reading that object.
    for key in [
        "write.data.path",
        "write.folder-storage.path",
        "write.metadata.path",
    ] {
        if let Some(path) = registered.metadata().properties().get(key) {
            scoped(root, path)?;
        }
    }
    let _memory = budget.reserve(4 << 20)?;
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        registered.metadata().current_schema(),
    )?);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("same"), None, Some("same")])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 1])),
        ],
    )?;
    let data_root = format!("{root}/data");
    scoped(root, &data_root)?;
    let parquet = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        registered.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        4096,
        remote.objects.file_io(),
        DefaultLocationGenerator::with_data_location(data_root.clone()),
        DefaultFileNameGenerator::new(
            uuid::Uuid::new_v4().to_string(),
            None,
            DataFileFormat::Parquet,
        ),
    );
    let mut writer = UnpartitionedWriter::new(DataFileWriterBuilder::new(rolling));
    println!(
        "{}",
        json!({"event": "native_append_start", "table": registered.identifier().to_string(),
        "before_metadata": registered.metadata_location_result()?, "table_location": root,
        "data_location": data_root, "rows": 3, "includes_duplicate_and_null": true,
        "scope": "trusted catalog placement probe; all resulting objects retained"})
    );
    provider("append_write_batch", writer.write(batch).await)?;
    let files = provider("append_close_writer", writer.close().await)?;
    let mut expected_files = BTreeMap::new();
    for file in &files {
        scoped(root, file.file_path())?;
        expected_files.insert(file.file_path().to_owned(), file.record_count());
    }
    if expected_files.values().sum::<u64>() != 3 {
        return Err(Error::refusal(
            "probe_append_rows",
            "native writer count differs from tiny fixture",
        ));
    }
    println!(
        "{}",
        json!({"event": "native_append_commit_start", "before_metadata": registered.metadata_location_result()?, "data_files": expected_files})
    );
    let tx = Transaction::new(registered);
    let appended = provider(
        "append_apply",
        tx.fast_append().add_data_files(files).apply(tx),
    )?;
    let appended = provider(
        "append_commit",
        appended.commit(remote.catalog.as_ref()).await,
    )?;
    let after = appended.metadata_location_result()?.to_owned();
    println!(
        "{}",
        json!({"event": "native_append_acknowledged", "before_metadata": registered.metadata_location_result()?,
        "after_metadata": after, "table_location": appended.metadata().location(), "snapshot": appended.metadata().current_snapshot_id()})
    );
    scoped(root, &after)?;
    if appended.metadata().location() != root
        || appended.metadata().current_schema() != registered.metadata().current_schema()
        || appended.metadata().uuid() != registered.metadata().uuid()
        || after == registered.metadata_location_result()?
    {
        return Err(Error::refusal(
            "probe_append_identity",
            "native append changed table identity/schema or failed to advance metadata",
        ));
    }
    // Never reuse a catalog-returned native FileIO configuration for independent
    // reads. The probe's configured object connection owns all decoded references.
    let native = Table::builder()
        .identifier(registered.identifier().clone())
        .metadata(appended.metadata_ref())
        .metadata_location(&after)
        .file_io(remote.objects.file_io())
        .runtime(iceberg::Runtime::try_current()?)
        .readonly(true)
        .build()?;
    let mut paths = BTreeSet::from([after.clone()]);
    let snapshot = native
        .metadata()
        .current_snapshot()
        .ok_or_else(|| Error::refusal("probe_append_snapshot", "append returned no snapshot"))?;
    scoped(root, snapshot.manifest_list())?;
    paths.insert(snapshot.manifest_list().to_owned());
    let list_input = remote
        .objects
        .file_io()
        .new_input(snapshot.manifest_list())?;
    if list_input.metadata().await?.size > 64 << 10 {
        return Err(Error::refusal(
            "probe_object_capacity",
            "tiny manifest list exceeds fixture allowance",
        ));
    }
    let list = provider(
        "append_manifest_list",
        native.manifest_list_reader(snapshot).load().await,
    )?;
    for manifest in list.entries() {
        scoped(root, &manifest.manifest_path)?;
        if manifest.manifest_length > 64 << 10 {
            return Err(Error::refusal(
                "probe_object_capacity",
                "tiny manifest exceeds fixture allowance",
            ));
        }
        let input = remote
            .objects
            .file_io()
            .new_input(&manifest.manifest_path)?;
        if input.metadata().await?.size > 64 << 10 {
            return Err(Error::refusal(
                "probe_object_capacity",
                "tiny manifest object exceeds fixture allowance",
            ));
        }
        paths.insert(manifest.manifest_path.clone());
    }
    // Native file planning validates the maintained manifest interpretation;
    // compare its exact live file paths/counts with the writer's returned files.
    let scan = provider("append_scan_build", native.scan().build())?;
    let tasks = provider("append_plan_files", scan.plan_files().await)?;
    futures::pin_mut!(tasks);
    let mut observed_files = BTreeMap::new();
    while let Some(task) = provider("append_file_task", tasks.try_next().await)? {
        scoped(root, &task.data_file_path)?;
        paths.insert(task.data_file_path.clone());
        let rows = task.record_count.ok_or_else(|| {
            Error::refusal("probe_append_inventory", "native task has no row count")
        })?;
        if observed_files.insert(task.data_file_path, rows).is_some() {
            return Err(Error::refusal(
                "probe_append_inventory",
                "native task repeats a live file",
            ));
        }
    }
    if observed_files != expected_files {
        return Err(Error::refusal(
            "probe_append_inventory",
            "native live files differ from completed writer files",
        ));
    }
    let mut objects = Vec::new();
    for path in paths {
        objects.push(exact_object(remote, owner, root, &path, budget).await?);
    }
    let metadata = objects
        .iter()
        .find(|object| object.path == after)
        .cloned()
        .ok_or_else(|| Error::refusal("probe_append_inventory", "metadata absent from closure"))?;
    let pin = TablePin {
        table: registered.identifier().to_string(),
        metadata,
        snapshot: native.metadata().current_snapshot_id(),
        schema_id: native.metadata().current_schema_id(),
        objects,
        rows: 3,
        logical_metadata: None,
    };
    println!("{}", json!({"event": "native_append_closure", "pin": pin}));
    let fresh = connect(root).await?;
    let loaded = provider(
        "append_fresh_load",
        fresh.catalog.load_table(registered.identifier()).await,
    )?;
    if loaded.metadata() != appended.metadata() || loaded.metadata_location_result()? != after {
        return Err(Error::refusal(
            "probe_fresh_load",
            "fresh catalog differs from acknowledged append",
        ));
    }
    let opened = open_pin_with_io(&pin, fresh.objects.file_io(), budget).await?;
    if opened.metadata() != appended.metadata() {
        return Err(Error::refusal(
            "probe_native_readback",
            "catalog-free read differs from acknowledged append",
        ));
    }
    let scan = provider("append_value_scan", opened.scan().build())?;
    let batches = provider("append_value_reader", scan.to_arrow().await)?;
    futures::pin_mut!(batches);
    let mut values = Vec::new();
    while let Some(batch) = provider("append_value_batch", batches.try_next().await)? {
        if batch.num_rows() > 3 - values.len() {
            return Err(Error::refusal(
                "probe_append_values",
                "native readback exceeds the three-row fixture",
            ));
        }
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::refusal("probe_value_type", "native text type differs"))?;
        let counter = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::refusal("probe_value_type", "native counter type differs"))?;
        for row in 0..batch.num_rows() {
            if counter.is_null(row) {
                return Err(Error::refusal(
                    "probe_append_values",
                    "native readback has a null required counter",
                ));
            }
            values.push((
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
                counter.value(row),
            ));
        }
    }
    values.sort();
    let mut expected = vec![
        (Some("same".to_owned()), 1),
        (None, 2),
        (Some("same".to_owned()), 1),
    ];
    expected.sort();
    if values != expected {
        return Err(Error::refusal(
            "probe_append_values",
            "fresh native values differ from duplicate/null fixture",
        ));
    }
    println!(
        "{}",
        json!({"event": "complete", "passed": true, "pin": pin,
        "before_metadata": registered.metadata_location_result()?, "after_metadata": after,
        "fresh_catalog_readback": true, "catalog_free_native_readback": true, "exact_values": true,
        "scope": "one tiny maintained native append and server placement; no remote Engine or recovery qualification"})
    );
    Ok(())
}
