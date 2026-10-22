//! Initial Build candidate identity retained before native writes.

use super::*;
use iceberg::{MetadataLocation, TableIdent, spec::TableMetadata};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct CandidateAttempt {
    attempt: Id,
    consumer: Id,
    operation: Id,
    operation_revision: i64,
    processing_revision: i64,
    request_digest: String,
    eligibility: u64,
    prefix: String,
    input: ObjectRef,
    identifier: TableIdent,
    metadata: ObjectRef,
    // The exact bounded serialization is both the durable creation intent and
    // the eventual object payload. Never regenerate metadata on a retry.
    metadata_json: String,
}

struct RetainedCandidate {
    pending: CandidateAttempt,
    resuming: bool,
    native: TableMetadata,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_candidate_attempt",
        "candidate intent differs from its protected operation generation",
    )
}

fn key(
    operation: &Id,
    revision: i64,
    processing_revision: i64,
    input: &ObjectRef,
) -> Result<String> {
    Ok(format!(
        "candidate-attempt/{operation}/{revision}/{processing_revision}/{}/processing",
        storage::json_digest(input)?.to_hex()
    ))
}

fn processing_revision(
    spec: &RunSpec,
    operation: &Operation,
    rows: &crate::catalog::SnapshotRows,
) -> Result<i64> {
    let phase = phase_key(
        &OperationRequest::Build(spec.clone()),
        &operation.request_digest,
        operation.eligibility,
        "processing",
        operation.base_manifest.as_ref(),
        operation.schema_key.as_ref(),
    )?;
    Ok(rows.revision(&phase).unwrap_or(0))
}

pub(in crate::engine) fn validate_record(
    key: &str,
    json: &str,
    rows: &crate::catalog::SnapshotRows,
    budget: Option<&Budget>,
) -> Result<()> {
    let pending: CandidateAttempt = serde_json::from_str(json)?;
    // Inventory already admits the complete decoded snapshot. Recovery callers
    // admit this candidate separately and also need its referenced small records.
    let _memory = budget
        .map(|budget| {
            rows.reserve_decoded(
                budget,
                &[
                    &format!("operation/{}", pending.operation),
                    &format!("request/{}", pending.operation),
                    &format!("protection/{}", pending.attempt),
                ],
            )
        })
        .transpose()?;
    validate(key, &pending, rows).map(|_| ())
}

fn validate(
    key_value: &str,
    pending: &CandidateAttempt,
    rows: &crate::catalog::SnapshotRows,
) -> Result<TableMetadata> {
    let (revision, _, operation) = rows
        .get_record::<Operation>(&format!("operation/{}", pending.operation))?
        .ok_or_else(invalid)?;
    let (request_revision, _, request) = rows
        .get_record::<OperationRequest>(&format!("request/{}", pending.operation))?
        .ok_or_else(invalid)?;
    let intent_revision = rows.revision(key_value).ok_or_else(invalid)?;
    let (protection_revision, _, protection) = rows
        .get_record::<Protection>(&format!("protection/{}", pending.attempt))?
        .ok_or_else(invalid)?;
    if key_value
        != key(
            &pending.operation,
            pending.operation_revision,
            pending.processing_revision,
            &pending.input,
        )?
        || pending.processing_revision < 0
        || pending.processing_revision >= intent_revision
        || revision != pending.operation_revision
        || request_revision != revision
        || pending.consumer != *request.dataset()
        || pending.operation != *request.operation()
        || pending.request_digest != operation.request_digest
        || pending.request_digest != fingerprint(&request)?
        || pending.eligibility != operation.eligibility
        || intent_revision <= revision
        || protection_revision != intent_revision
        || protection.id != pending.attempt
        || protection.consumer != pending.consumer
        || protection.paths != [pending.prefix.clone()]
        || protection.purpose != format!("{}/processing", pending.operation)
        || protection.retired
        || pending.metadata.owner != pending.consumer
        || pending.metadata.bytes != pending.metadata_json.len() as u64
        || blake3::hash(pending.metadata_json.as_bytes())
            .to_hex()
            .as_str()
            != pending.metadata.digest
    {
        return Err(invalid());
    }
    let native: TableMetadata = serde_json::from_str(&pending.metadata_json)?;
    let location: MetadataLocation = pending.metadata.path.parse()?;
    let root = format!("{}/metadata/", native.location());
    let filename = pending
        .metadata
        .path
        .strip_prefix(&root)
        .ok_or_else(invalid)?;
    if native.current_snapshot_id().is_some()
        || native.snapshots().len() != 0
        || pending.prefix != format!("{}/", native.location())
        || pending.identifier.name() != format!("data_{}", pending.attempt)
        || location.to_string() != pending.metadata.path
        || location.compression_codec() != iceberg::compression::CompressionCodec::None
        || !filename.starts_with("00000-")
        || filename.contains('/')
        || native.properties() != &storage::table_properties()
    {
        return Err(invalid());
    }
    storage::validate_partition_spec(native.current_schema(), native.default_partition_spec())
        .map_err(|_| invalid())?;
    if let OperationRequest::Build(build) = &request {
        storage::check_partition_by_matches(
            native.current_schema(),
            native.default_partition_spec(),
            &build.partition_by,
        )
        .map_err(|_| invalid())?;
    }
    Ok(native)
}

enum InitialSource {
    DirectParquet(datafusion::physical_plan::SendableRecordBatchStream),
    DataFrame(Box<DataFrame>),
}

impl InitialSource {
    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        match self {
            Self::DirectParquet(stream) => stream.schema(),
            Self::DataFrame(frame) => Arc::new(frame.schema().as_arrow().clone()),
        }
    }
}

impl Engine {
    pub(in crate::engine) fn process_initial_build<'a>(
        &'a self,
        spec: &'a RunSpec,
        operation: &'a Operation,
        admission: &'a PhaseResult,
    ) -> futures::future::BoxFuture<'a, Result<Prepared>> {
        Box::pin(async move {
        let input = admission
            .outputs
            .first()
            .ok_or_else(|| Error::refusal("corrupt_phase", "admission has no retained input"))?;
        let budget = &self.inner.lifecycle.budget;
        // Resolve and admit the saved intent before opening Parquet or planning
        // the query. Its actual retained size, not the configured ceiling, stays
        // charged while native processing uses it.
        let mut planned = None;
        let RetainedCandidate {
            pending,
            resuming,
            native: initial,
            _memory,
        } = if let Some(saved) = self.existing_candidate(spec, operation, input).await? {
            saved
        } else {
            let source = self.initial_source(spec, input).await?;
            let schema =
                storage::arrow_schema_to_schema_auto_assign_ids(source.arrow_schema().as_ref())?;
            let pending = self
                .reserve_candidate(spec, operation, input, schema)
                .await?;
            planned = Some(source);
            pending
        };
        let catalog = self.inner.lifecycle.catalog.as_ref();
        let table = if catalog.table_exists(&pending.identifier).await? {
            catalog.load_table(&pending.identifier).await?
        } else {
            self.inner
                .objects
                .create_candidate_metadata(
                    &pending.metadata,
                    pending.metadata_json.into_bytes(),
                    budget,
                    resuming,
                )
                .await?;
            match catalog
                .register_table(&pending.identifier, pending.metadata.path.clone())
                .await
            {
                Ok(table) => table,
                Err(error) => {
                    // Lost response or competing exact registration: reconcile only
                    // the retained identifier. An absent table preserves the error.
                    if !catalog.table_exists(&pending.identifier).await? {
                        return Err(error.into());
                    }
                    catalog.load_table(&pending.identifier).await?
                }
            }
        };
        let table = self.inner.objects.bind_table(&table)?.into_native();
        if let Some(prepared) = self
            .recover_initial_output(&spec.dataset, &table, &initial, &pending.metadata)
            .await?
        {
            return Ok(prepared);
        }
        let result = async {
            crate::catalog::GuardedCatalog::new(catalog, &table)
                .load_table(table.identifier())
                .await?;
            let source = match planned {
                Some(source) => source,
                None => self.initial_source(spec, input).await?,
            };
            self.write_initial_source(&spec.dataset, table, source).await
        }
        .await;
        match result {
            Ok(prepared) => Ok(prepared),
            Err(error) => {
                // A competing exact retry or lost append response can commit
                // before this writer fails. Reload only the retained candidate;
                // never append again or allocate another output identity.
                let table = catalog.load_table(&pending.identifier).await?;
                let table = self.inner.objects.bind_table(&table)?.into_native();
                match self
                    .recover_initial_output(&spec.dataset, &table, &initial, &pending.metadata)
                    .await?
                {
                    Some(prepared) => Ok(prepared),
                    None => Err(error),
                }
            }
        }
        })
    }

    async fn recover_initial_output(
        &self,
        dataset: &Id,
        table: &Table,
        initial: &TableMetadata,
        expected: &ObjectRef,
    ) -> Result<Option<Prepared>> {
        check_candidate(table, initial, expected)?;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(None);
        };
        // An acknowledged native output can precede the lifecycle phase commit.
        // Both ordinary resumption and a losing concurrent append reuse it.
        let summary = &snapshot.summary().additional_properties;
        let count = |name: &str| -> Result<u64> {
            summary
                .get(name)
                .and_then(|v| v.parse().ok())
                .ok_or_else(invalid)
        };
        let rows = count("added-records")?;
        let metrics = WriteMetrics {
            data_files_written: count("added-data-files")?,
            bytes_written: count("added-files-size")?,
            rows_added: rows,
            ..WriteMetrics::default()
        };
        let table = self.pin_output(dataset, table, rows, &[]).await?;
        Ok(Some(Prepared {
            table,
            changes: None,
            metrics,
            schema_input: None,
        }))
    }

    async fn write_initial_source(
        &self,
        dataset: &Id,
        table: Table,
        source: InitialSource,
    ) -> Result<Prepared> {
        match source {
            InitialSource::DirectParquet(stream) => {
                let stream = stream.map_err(Error::from);
                self.write_batch_stream(dataset, table, stream).await
            }
            InitialSource::DataFrame(frame) => self.write_output(dataset, table, *frame).await,
        }
    }

    async fn initial_source(&self, spec: &RunSpec, input: &ObjectRef) -> Result<InitialSource> {
        #[cfg(test)]
        self.inner
            .initial_build_queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stream = self
            .inner
            .objects
            .read_flat_parquet(input, &self.inner.lifecycle.budget)
            .await?;
        let schema = stream.schema();
        if is_identity_source_projection(&spec.sql, schema.as_ref()) {
            super::check_build_arrow_schema(schema.as_ref())?;
            #[cfg(test)]
            self.inner
                .identity_build_fast_path
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(InitialSource::DirectParquet(stream));
        }
        drop(stream);
        let context = self.context();
        context
            .register_parquet("input", &input.path, ParquetReadOptions::default())
            .await?;
        let input_df = context.table("input").await?;
        let frame = if is_identity_source_projection(&spec.sql, input_df.schema().as_arrow()) {
            validate_plan(input_df.logical_plan())?;
            input_df
        } else {
            build_query(&context, &spec.sql).await?
        };
        check_build_fields(&frame)?;
        Ok(InitialSource::DataFrame(Box::new(frame)))
    }

    async fn existing_candidate(
        &self,
        spec: &RunSpec,
        operation: &Operation,
        input: &ObjectRef,
    ) -> Result<Option<RetainedCandidate>> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        self.check(&view, operation, &spec.operation).await?;
        let rows = lifecycle.rows(&view).await?;
        let revision = rows
            .revision(&format!("operation/{}", spec.operation))
            .ok_or_else(invalid)?;
        let prior_processing = processing_revision(spec, operation, &rows)?;
        let key_value = key(&spec.operation, revision, prior_processing, input)?;
        self.decode_candidate(spec, input, &view, &key_value, &rows)
    }

    fn decode_candidate(
        &self,
        spec: &RunSpec,
        input: &ObjectRef,
        view: &View,
        key_value: &str,
        rows: &crate::catalog::SnapshotRows,
    ) -> Result<Option<RetainedCandidate>> {
        if !rows.contains_key(key_value) {
            return Ok(None);
        }
        let budget = &self.inner.lifecycle.budget;
        let mut memory = rows.reserve_decoded(
            budget,
            &[
                key_value,
                &format!("operation/{}", spec.operation),
                &format!("request/{}", spec.operation),
            ],
        )?;
        let (_, _, saved) = rows
            .get_record::<CandidateAttempt>(key_value)?
            .ok_or_else(invalid)?;
        memory.merge(rows.reserve_decoded(budget, &[&format!("protection/{}", saved.attempt)])?);
        let native = self.check_candidate_intent(spec, input, view, key_value, &saved, rows)?;
        Ok(Some(RetainedCandidate {
            pending: saved,
            resuming: true,
            native,
            _memory: memory,
        }))
    }

    fn check_candidate_intent(
        &self,
        spec: &RunSpec,
        input: &ObjectRef,
        view: &View,
        key_value: &str,
        saved: &CandidateAttempt,
        rows: &crate::catalog::SnapshotRows,
    ) -> Result<TableMetadata> {
        let native = validate(key_value, saved, rows)?;
        if saved.input != *input || saved.identifier.namespace() != view.identifier().namespace() {
            return Err(invalid());
        }
        let directory = self
            .inner
            .retained
            .child(spec.dataset.as_str())?
            .child(saved.attempt.as_str())?;
        if directory.protection_prefix()? != saved.prefix {
            return Err(invalid());
        }
        self.inner
            .objects
            .validate_retained_path(&saved.metadata.path)?;
        Ok(native)
    }

    async fn reserve_candidate(
        &self,
        spec: &RunSpec,
        operation: &Operation,
        input: &ObjectRef,
        schema: Schema,
    ) -> Result<RetainedCandidate> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let revision = rows
            .revision(&format!("operation/{}", spec.operation))
            .ok_or_else(invalid)?;
        let prior_processing = processing_revision(spec, operation, &rows)?;
        let key_value = key(&spec.operation, revision, prior_processing, input)?;
        if let Some(saved) = self.decode_candidate(spec, input, &view, &key_value, &rows)? {
            self.check(&view, operation, &spec.operation).await?;
            return Ok(saved);
        }
        let attempt = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .retained
            .child(spec.dataset.as_str())?
            .child(attempt.as_str())?;
        let location = directory.as_string()?;
        let memory = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(2))?;
        let owner = spec.dataset.clone();
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let partition_by = spec.partition_by.clone();
        let (metadata_ref, metadata_json, native, mut memory) = lifecycle
            .budget
            .blocking_admitted(move || {
                let unbound = storage::build_unbound_partition_spec(&schema, &partition_by)?;
                let metadata = storage::fresh_partitioned_metadata_at(&schema, &location, unbound)?;
                let bytes = storage::bounded_json(&metadata, limit)?;
                let metadata_ref = ObjectRef {
                    owner,
                    path: MetadataLocation::new_with_metadata(&location, &metadata).to_string(),
                    digest: blake3::hash(&bytes).to_hex().to_string(),
                    bytes: bytes.len() as u64,
                };
                Ok((
                    metadata_ref,
                    String::from_utf8(bytes).map_err(|_| invalid())?,
                    metadata,
                    memory,
                ))
            })
            .await?;
        // Match retained-record accounting for decoded native schema, strings
        // and transient serialization copies. Release the unused ceiling before
        // lifecycle commit and native writer allocations overlap this intent.
        let retained_bytes = metadata_json
            .capacity()
            .saturating_mul(8)
            .saturating_add(input.path.len().saturating_mul(8))
            .saturating_add(4096);
        if retained_bytes > memory.num_permits() {
            memory.merge(
                lifecycle
                    .budget
                    .reserve(retained_bytes - memory.num_permits())?,
            );
        } else {
            let excess = memory.num_permits() - retained_bytes;
            drop(memory.split(excess));
        }
        self.inner
            .objects
            .validate_retained_path(&metadata_ref.path)?;
        let pending = CandidateAttempt {
            attempt: attempt.clone(),
            consumer: spec.dataset.clone(),
            operation: spec.operation.clone(),
            operation_revision: revision,
            processing_revision: prior_processing,
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            prefix: directory.protection_prefix()?,
            input: input.clone(),
            identifier: TableIdent::new(
                view.identifier().namespace().clone(),
                format!("data_{}", attempt.as_str()),
            ),
            metadata: metadata_ref,
            metadata_json,
        };
        let protection = Protection {
            id: attempt.clone(),
            consumer: spec.dataset.clone(),
            purpose: format!("{}/processing", spec.operation),
            paths: vec![pending.prefix.clone()],
            retired: false,
        };
        let lock = self.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let fresh = lifecycle.open(&spec.dataset).await?;
        self.check(&fresh, operation, &spec.operation).await?;
        let rows = lifecycle.rows(&fresh).await?;
        if rows.revision(&format!("operation/{}", spec.operation)) != Some(revision)
            || processing_revision(spec, operation, &rows)? != prior_processing
        {
            return Err(invalid());
        }
        if rows.contains_key(&key_value) {
            drop((pending, native, memory));
            return self
                .decode_candidate(spec, input, &fresh, &key_value, &rows)?
                .ok_or_else(invalid);
        }
        let records = vec![
            record(format!("protection/{attempt}"), protection)?,
            record(&key_value, &pending)?,
        ];
        if let Err(error) = lifecycle.commit(&fresh, records).await {
            let after = lifecycle.open(&spec.dataset).await?;
            self.check(&after, operation, &spec.operation).await?;
            let rows = lifecycle.rows(&after).await?;
            if processing_revision(spec, operation, &rows)? != prior_processing {
                return Err(invalid());
            }
            if rows.contains_key(&key_value) {
                drop((pending, native, memory));
                return self
                    .decode_candidate(spec, input, &after, &key_value, &rows)?
                    .ok_or_else(invalid);
            }
            return Err(error);
        }
        Ok(RetainedCandidate {
            pending,
            resuming: false,
            native,
            _memory: memory,
        })
    }
}

fn check_candidate(table: &Table, initial: &TableMetadata, expected: &ObjectRef) -> Result<()> {
    let metadata = table.metadata();
    if metadata.uuid() != initial.uuid()
        || metadata.location() != initial.location()
        || metadata.current_schema() != initial.current_schema()
        || metadata.properties() != initial.properties()
        || metadata.default_partition_spec() != initial.default_partition_spec()
        || metadata.default_sort_order() != initial.default_sort_order()
        || metadata.statistics_iter().next().is_some()
        || metadata.partition_statistics_iter().next().is_some()
    {
        return Err(invalid());
    }
    if let Some(snapshot) = metadata.current_snapshot() {
        if metadata.snapshots().len() != 1
            || snapshot.parent_snapshot_id().is_some()
            || snapshot.summary().operation != iceberg::spec::Operation::Append
        {
            return Err(invalid());
        }
    } else if metadata != initial || table.metadata_location_result()? != expected.path {
        return Err(invalid());
    }
    // Native writer paths are derived only from the retained table location and
    // unchanged fresh properties. Bound pinning checks each catalog-owned path
    // before reading manifests or payloads.
    Ok(())
}

#[cfg(all(test, feature = "test-util"))]
mod tests;
