//! Durable authorized flat exports using retained native reads and Parquet writes.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::export::{ExportField, ExportPin, ExportPolicy, ExportSpec, ExportValidation};
use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder,
    },
    basic::{Compression, ZstdLevel},
    file::{
        metadata::FooterTail,
        properties::{EnabledStatistics, WriterProperties},
    },
};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

/// The source release is a durable recovery dependency, including foreign owners.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportOperation {
    pub request: ExportSpec,
    pub request_digest: String,
    pub source: ReleasePin,
    pub source_revision: i64,
    pub eligibility: u64,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportMaterialized {
    pub request_digest: String,
    pub object: ObjectRef,
    pub rows: u64,
    pub fields: Vec<ExportField>,
}

impl Engine {
    /// Creates, validates, and selects an immutable authorized local Parquet file.
    ///
    /// Completed phases survive caller cancellation and retry. A completed retry
    /// returns its original pin without changing a channel that advanced later.
    ///
    /// # Errors
    /// Refuses conflicting identities, unsupported disclosure, unavailable sources,
    /// stale channel/authority decisions, regressing sources, or exhausted capacity.
    pub async fn export(&self, spec: &ExportSpec) -> Result<ExportPin> {
        self.export_request(spec, None).await
    }

    /// Interrupts after a durable export boundary for recovery qualification.
    ///
    /// # Errors
    /// Returns the export refusal or an injected failure after the named phase.
    #[cfg(feature = "test-util")]
    pub async fn export_until(&self, spec: &ExportSpec, phase: &str) -> Result<ExportPin> {
        self.export_request(spec, Some(phase.to_owned())).await
    }

    async fn export_request(&self, spec: &ExportSpec, stop: Option<String>) -> Result<ExportPin> {
        self.require_local()?;
        let memory = self.request_memory(&(spec, &stop))?;
        validate_request(spec)?;
        let spec = spec.clone();
        self.admit_work(memory, move |engine| async move {
            engine.export_admitted(&spec, stop.as_deref()).await
        })
        .await
    }

    /// Resolves a channel once to its catalog-selected immutable export.
    ///
    /// # Errors
    /// Refuses an unknown channel or inconsistent retained selection metadata.
    pub async fn latest_export(&self, dataset: &Id, channel: &Id) -> Result<ExportPin> {
        self.lookup_export(dataset, &format!("export-channel/{channel}"))
            .await
    }

    /// Resolves an export's immutable result without advancing its channel.
    ///
    /// # Errors
    /// Refuses an unknown export or inconsistent retained selection metadata.
    pub async fn export_pin(&self, dataset: &Id, operation: &Id) -> Result<ExportPin> {
        self.lookup_export(dataset, &format!("export-result/{operation}"))
            .await
    }

    async fn lookup_export(&self, dataset: &Id, key: &str) -> Result<ExportPin> {
        self.require_local()?;
        let memory = self.request_memory(&(dataset, key))?;
        let dataset = dataset.clone();
        let key = key.to_owned();
        self.admit_work(memory, move |engine| async move {
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            let view = engine.inner.lifecycle.open(&dataset).await?;
            let pin: ExportPin = engine
                .inner
                .lifecycle
                .get(&view, &key)
                .await?
                .ok_or_else(|| Error::refusal("no_export", "no completed export is selected"))?;
            let result: ExportPin = engine
                .inner
                .lifecycle
                .get(&view, &format!("export-result/{}", pin.id))
                .await?
                .ok_or_else(invalid_export)?;
            if result != pin || pin.dataset != dataset {
                return Err(invalid_export());
            }
            check_pin(&pin)?;
            let operation: ExportOperation = engine
                .inner
                .lifecycle
                .get(&view, &format!("export-operation/{}", pin.id))
                .await?
                .ok_or_else(invalid_export)?;
            check_result(&pin, &operation)?;
            let expected_key = if key.starts_with("export-result/") {
                format!("export-result/{}", pin.id)
            } else {
                format!("export-channel/{}", operation.request.channel)
            };
            if key != expected_key {
                return Err(invalid_export());
            }
            Ok(pin)
        })
        .await
    }

    async fn export_admitted(&self, spec: &ExportSpec, stop: Option<&str>) -> Result<ExportPin> {
        let digest = storage::json_digest(spec)?.to_hex().to_string();
        let operation = self.admit_export(spec, &digest).await?;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(pin) = lifecycle
            .get::<ExportPin>(&view, &format!("export-result/{}", spec.operation))
            .await?
        {
            check_result(&pin, &operation)?;
            return Ok(pin);
        }
        self.check_export_authority(&view, &operation).await?;
        self.check_export_channel(&view, spec, operation.source_revision)
            .await?;
        let materialized_key = format!("export-materialized/{}", spec.operation);
        let materialized = match lifecycle
            .get::<ExportMaterialized>(&view, &materialized_key)
            .await?
        {
            Some(value) => value,
            None => {
                let value = self.materialize_export(&operation).await?;
                self.retain_export_record(&operation, &materialized_key, &value)
                    .await?
            }
        };
        check_materialized(&materialized, &operation)?;
        stop_after(stop, "materialization")?;
        let validation_key = format!("export-validation/{}", spec.operation);
        let view = lifecycle.open(&spec.dataset).await?;
        let validation = match lifecycle
            .get::<ExportValidation>(&view, &validation_key)
            .await?
        {
            Some(value) => value,
            None => {
                #[cfg(feature = "test-util")]
                work_started("export-validation");
                let value = validate_output(&materialized, &lifecycle.budget).await?;
                self.retain_export_record(&operation, &validation_key, &value)
                    .await?
            }
        };
        if validation.object != materialized.object || validation.rows != materialized.rows {
            return Err(invalid_export());
        }
        stop_after(stop, "validation")?;
        let pin = ExportPin {
            format: 1,
            id: spec.operation.clone(),
            dataset: spec.dataset.clone(),
            source: spec.source.clone(),
            source_revision: operation.source_revision,
            policy: spec.policy.clone(),
            object: materialized.object,
            rows: materialized.rows,
            fields: materialized.fields,
            validation,
        };
        let lock = self.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(selected) = lifecycle
            .get::<ExportPin>(&view, &format!("export-result/{}", spec.operation))
            .await?
        {
            check_result(&selected, &operation)?;
            return Ok(selected);
        }
        self.check_export_authority(&view, &operation).await?;
        self.check_export_channel(&view, spec, operation.source_revision)
            .await?;
        check_result(&pin, &operation)?;
        let committed = lifecycle
            .commit(
                &view,
                vec![
                    record(format!("export-result/{}", spec.operation), &pin)?,
                    record(format!("export-channel/{}", spec.channel), &pin)?,
                ],
            )
            .await?;
        stop_after(stop, "selection")?;
        let selected: ExportPin = lifecycle
            .get(&committed, &format!("export-result/{}", spec.operation))
            .await?
            .ok_or_else(invalid_export)?;
        if selected != pin {
            return Err(invalid_export());
        }
        Ok(selected)
    }

    async fn admit_export(&self, spec: &ExportSpec, digest: &str) -> Result<ExportOperation> {
        let key = format!("export-operation/{}", spec.operation);
        let lock = self.lock(&spec.dataset).await;
        {
            let _guard = lock.lock().await;
            let view = self.inner.lifecycle.open(&spec.dataset).await?;
            if let Some(saved) = self
                .inner
                .lifecycle
                .get::<ExportOperation>(&view, &key)
                .await?
            {
                check_operation(&saved, spec, digest)?;
                return Ok(saved);
            }
        }
        // Foreign owners are protected before taking the consumer lock.
        let source = self
            .available_release_admitted(&spec.dataset, &spec.source.version)
            .await?;
        if source.version_ref() != spec.source {
            return Err(invalid_export());
        }
        let protections = self
            .protect_releases(&spec.dataset, std::slice::from_ref(&source))
            .await?;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(&spec.dataset).await?;
        if let Some(saved) = self
            .inner
            .lifecycle
            .get::<ExportOperation>(&view, &key)
            .await?
        {
            check_operation(&saved, spec, digest)?;
            return Ok(saved);
        }
        self.check_operation_available(&view, &source.id).await?;
        self.guard_corrections(&view, &source.dataset, Some(&spec.policy.group))
            .await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let source_revision = rows
            .revision(&format!("result/{}", source.id))
            .ok_or_else(invalid_export)?;
        self.check_export_channel(&view, spec, source_revision)
            .await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(&view, "authority")
            .await?
            .unwrap_or_default();
        let operation = ExportOperation {
            request: spec.clone(),
            request_digest: digest.into(),
            source,
            source_revision,
            eligibility: authority.eligibility,
            epoch: authority.epoch,
        };
        self.inner
            .lifecycle
            .commit(
                &view,
                completion_records(&spec.dataset, &view, &key, &operation, Some(&protections))?,
            )
            .await?;
        Ok(operation)
    }

    async fn check_export_authority(&self, view: &View, operation: &ExportOperation) -> Result<()> {
        self.guard_corrections(
            view,
            &operation.source.dataset,
            Some(&operation.request.policy.group),
        )
        .await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        if authority.eligibility != operation.eligibility || authority.epoch != operation.epoch {
            return Err(Error::refusal(
                "stale_export",
                "export authority changed; completed output remains retained",
            ));
        }
        Ok(())
    }

    async fn check_export_channel(
        &self,
        view: &View,
        spec: &ExportSpec,
        revision: i64,
    ) -> Result<()> {
        let selected: Option<ExportPin> = self
            .inner
            .lifecycle
            .get(view, &format!("export-channel/{}", spec.channel))
            .await?;
        if selected.as_ref().map(|pin| &pin.id) != spec.expected.as_ref() {
            return Err(Error::refusal(
                "export_conflict",
                "export channel differs from the expected selection",
            ));
        }
        if let Some(previous) = selected {
            check_pin(&previous)?;
            if previous.dataset != spec.dataset || previous.source_revision > revision {
                return Err(Error::refusal(
                    "export_regression",
                    "export would replace newer accepted source coverage",
                ));
            }
        }
        Ok(())
    }

    async fn retain_export_record<T>(
        &self,
        operation: &ExportOperation,
        key: &str,
        value: &T,
    ) -> Result<T>
    where
        T: Serialize + serde::de::DeserializeOwned + Clone,
    {
        let dataset = &operation.request.dataset;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        if let Some(saved) = self.inner.lifecycle.get::<T>(&view, key).await? {
            // Concurrent attempts retain the first completed immutable output.
            return Ok(saved);
        }
        self.inner
            .lifecycle
            .commit(&view, vec![record(key, value)?])
            .await?;
        Ok(value.clone())
    }

    async fn materialize_export(&self, operation: &ExportOperation) -> Result<ExportMaterialized> {
        let spec = &operation.request;
        let budget = &self.inner.lifecycle.budget;
        let table_pin = operation
            .source
            .dataset
            .groups
            .get(&spec.policy.group)
            .ok_or_else(|| {
                Error::refusal("missing_group", "export policy names an absent field group")
            })?;
        let table = storage::open_pin(table_pin, &self.inner.lifecycle.budget).await?;
        let inventory = storage::snapshot_files(&table, budget).await?;
        // This maintained shared reader projects by native field ID and refuses
        // unqualified native delete files instead of globbing historical data.
        let scan = storage::OccurrenceScan::values(&table, budget, inventory).await?;
        let (indices, schema, fields) = authorized_schema(scan.schema(), &spec.policy)?;
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .root
            .join("objects")
            .join(spec.dataset.as_str())
            .join(id.as_str());
        {
            let lock = self.lock(&spec.dataset).await;
            let _guard = lock.lock().await;
            let view = self.inner.lifecycle.open(&spec.dataset).await?;
            self.check_export_authority(&view, operation).await?;
            self.inner
                .lifecycle
                .commit(
                    &view,
                    vec![record(
                        format!("protection/{id}"),
                        Protection {
                            id,
                            consumer: spec.dataset.clone(),
                            purpose: format!("{}/export", spec.operation),
                            paths: vec![format!("{}/", directory.display())],
                            retired: false,
                        },
                    )?],
                )
                .await?;
        }
        let memory = budget.reserve(
            budget
                .limits
                .max_batch_bytes
                .saturating_mul(4)
                .saturating_add(budget.limits.max_metadata_bytes),
        )?;
        let path = directory.join("export.parquet");
        let initial_path = path.clone();
        let output_schema = schema.clone();
        let mut writer = budget
            .blocking(move || {
                let _memory = memory;
                std::fs::create_dir_all(&directory)?;
                let output = HashingFile::create(&initial_path)?;
                let properties = WriterProperties::builder()
                    .set_compression(Compression::ZSTD(ZstdLevel::default()))
                    .set_statistics_enabled(EnabledStatistics::None)
                    .set_offset_index_disabled(true)
                    .build();
                Ok((
                    ArrowWriter::try_new(output, output_schema, Some(properties))?,
                    _memory,
                ))
            })
            .await?;
        #[cfg(feature = "test-util")]
        work_started("export-materialization");
        let mut rows = 0_u64;
        // A conservative per-column allowance bounds accumulated footer records.
        // Statistics/indexes are disabled; names and type descriptions add their
        // actual size. This is admission accounting, not a measured RSS claim.
        let footer_group_bytes = fields.iter().fold(1024_usize, |size, field| {
            size.saturating_add(1024 + field.name.len() + field.data_type.len())
        });
        for file in scan.files() {
            let stream = scan.read_projected_file(file.file_path(), budget, Some(&indices))?;
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await? {
                let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?;
                rows = rows
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(invalid_export)?;
                let limits = budget.limits.clone();
                let root = self.inner.root.clone();
                writer = budget
                    .blocking(move || {
                        let reserve = limits
                            .scratch_bytes
                            .saturating_add((limits.max_batch_bytes as u64).saturating_mul(4));
                        if fs2::available_space(&root)? < reserve {
                            return Err(Error::refusal(
                                "disk_capacity",
                                "export output and declared scratch no longer fit available disk",
                            ));
                        }
                        writer.0.write(&batch)?;
                        // Reader batch size does not determine physical row groups.
                        // Flush when estimated writer memory reaches the batch allowance.
                        if writer.0.memory_size() >= limits.max_batch_bytes {
                            writer.0.flush()?;
                        }
                        if writer
                            .0
                            .flushed_row_groups()
                            .len()
                            .saturating_mul(footer_group_bytes)
                            > limits.max_metadata_bytes
                        {
                            return Err(Error::refusal(
                                "metadata_capacity",
                                "estimated flat export footer exceeds the declared metadata allowance",
                            ));
                        }
                        Ok(writer)
                    })
                    .await?;
            }
        }
        if rows != table_pin.rows {
            return Err(Error::refusal(
                "row_count_mismatch",
                "export rows differ from the retained native group",
            ));
        }
        let owner = spec.dataset.clone();
        let max_metadata_bytes = budget.limits.max_metadata_bytes;
        let object = budget
            .blocking(move || {
                let (mut output, _memory) = writer;
                // Finish flushes the pending group and consumes the accumulated records.
                // Admit its footer before that boundary, including the final partial group.
                let groups = output
                    .flushed_row_groups()
                    .len()
                    .saturating_add(usize::from(output.in_progress_rows() != 0));
                if groups.saturating_mul(footer_group_bytes) > max_metadata_bytes {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "estimated flat export footer exceeds the declared metadata allowance",
                    ));
                }
                output.finish()?;
                output.inner().finish(&owner, &path)
            })
            .await?;
        Ok(ExportMaterialized {
            request_digest: operation.request_digest.clone(),
            object,
            rows,
            fields,
        })
    }
}

fn validate_request(spec: &ExportSpec) -> Result<()> {
    let columns = &spec.policy.columns;
    if spec.source.dataset != spec.dataset
        || spec.policy.group.is_empty()
        || columns.is_empty()
        || columns.iter().any(|name| name.is_empty())
        || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
    {
        return Err(Error::refusal(
            "invalid_export",
            "export requires one exact local source and distinct explicit columns",
        ));
    }
    Ok(())
}

fn check_operation(operation: &ExportOperation, spec: &ExportSpec, digest: &str) -> Result<()> {
    if operation.request != *spec || operation.request_digest != digest {
        return Err(Error::refusal(
            "operation_conflict",
            "export identity belongs to a different request",
        ));
    }
    if operation.source.version_ref() != spec.source
        || operation.source_revision <= 0
        || !operation.source.verified
        || operation.source.id != operation.source.dataset.version
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(super) fn validate_operation_record(operation: &ExportOperation) -> Result<()> {
    validate_request(&operation.request)?;
    let digest = storage::json_digest(&operation.request)?
        .to_hex()
        .to_string();
    check_operation(operation, &operation.request, &digest)
}

pub(super) fn check_materialized(
    value: &ExportMaterialized,
    operation: &ExportOperation,
) -> Result<()> {
    let policy = &operation.request.policy;
    if value.request_digest != operation.request_digest
        || value.object.owner != operation.request.dataset
        || value
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(policy.columns.iter())
        || operation
            .source
            .dataset
            .groups
            .get(&policy.group)
            .is_none_or(|group| group.rows != value.rows)
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(super) fn check_pin(pin: &ExportPin) -> Result<()> {
    if pin.format != 1
        || pin.dataset != pin.source.dataset
        || pin.dataset != pin.object.owner
        || pin.validation.object != pin.object
        || pin.validation.rows != pin.rows
        || pin
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(pin.policy.columns.iter())
        || pin.source_revision <= 0
        || pin.fields.is_empty()
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(super) fn check_result(pin: &ExportPin, operation: &ExportOperation) -> Result<()> {
    check_pin(pin)?;
    let spec = &operation.request;
    if pin.id != spec.operation
        || pin.dataset != spec.dataset
        || pin.source != spec.source
        || pin.policy != spec.policy
        || pin.source_revision != operation.source_revision
    {
        return Err(invalid_export());
    }
    Ok(())
}

fn invalid_export() -> Error {
    Error::refusal(
        "invalid_export",
        "retained export metadata does not identify one authorized immutable result",
    )
}

fn authorized_schema(
    source: &SchemaRef,
    policy: &ExportPolicy,
) -> Result<(Vec<usize>, SchemaRef, Vec<ExportField>)> {
    let mut indices = Vec::with_capacity(policy.columns.len());
    let mut fields = Vec::with_capacity(policy.columns.len());
    for name in &policy.columns {
        if name.to_ascii_lowercase().starts_with("__docspec_") {
            return Err(Error::refusal(
                "export_disclosure",
                "internal occurrence and evidence fields cannot be exported",
            ));
        }
        let index = source.index_of(name).map_err(|_| {
            Error::refusal(
                "export_disclosure",
                "authorized column is absent from the exact source schema",
            )
        })?;
        let field = source.field(index);
        if !matches!(
            field.data_type(),
            DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Date32
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Decimal128(_, _)
        ) {
            return Err(Error::refusal(
                "unsupported_export_type",
                format!(
                    "local flat export column {name} has unsupported type {}",
                    field.data_type()
                ),
            ));
        }
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(invalid_export)?;
        // Rebuild each field: source documentation/defaults and arbitrary metadata
        // never reach the public Parquet footer or its encoded Arrow schema.
        fields.push(
            Field::new(name, field.data_type().clone(), field.is_nullable()).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), id.clone())]),
            ),
        );
        indices.push(index);
    }
    let schema = Arc::new(Schema::new(fields));
    let fields = describe_schema(&schema)?;
    Ok((indices, schema, fields))
}

fn describe_schema(schema: &Schema) -> Result<Vec<ExportField>> {
    if !schema.metadata().is_empty() {
        return Err(invalid_export());
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().len() != 1 {
                return Err(invalid_export());
            }
            let id = field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|id| *id > 0)
                .ok_or_else(invalid_export)?;
            Ok(ExportField {
                id,
                name: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
            })
        })
        .collect()
}

async fn validate_output(
    materialized: &ExportMaterialized,
    budget: &Budget,
) -> Result<ExportValidation> {
    let value = materialized.clone();
    let limits = budget.limits.clone();
    let memory = budget.reserve(
        limits
            .max_metadata_bytes
            .saturating_mul(4)
            .saturating_add(limits.max_batch_bytes.saturating_mul(2)),
    )?;
    budget
        .blocking(move || {
            let _memory = memory;
            let mut file = File::open(&value.object.path)?;
            if file.metadata()?.len() != value.object.bytes {
                return Err(invalid_export());
            }
            file.seek(SeekFrom::End(-8))?;
            let mut tail = [0; 8];
            file.read_exact(&mut tail)?;
            if FooterTail::try_from(tail)?.metadata_length() > limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "export footer exceeds metadata allowance",
                ));
            }
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            if describe_schema(builder.schema())? != value.fields
                || u64::try_from(builder.metadata().file_metadata().num_rows()).ok()
                    != Some(value.rows)
            {
                return Err(invalid_export());
            }
            // One small shape probe. The reader may decode compressed pages beyond
            // the returned rows; this is not a compressed-I/O or total-memory claim.
            let mut reader = builder.with_batch_size(8.min(limits.batch_rows)).build()?;
            let (sampled_rows, sampled_decoded_bytes) = match reader.next().transpose()? {
                None => (0, 0),
                Some(batch) => {
                    if batch.get_array_memory_size() > limits.max_batch_bytes {
                        return Err(Error::refusal(
                            "oversized_batch",
                            "export sample exceeds the byte allowance",
                        ));
                    }
                    if batch
                        .columns()
                        .iter()
                        .zip(batch.schema().fields())
                        .any(|(array, field)| !field.is_nullable() && array.null_count() != 0)
                    {
                        return Err(invalid_export());
                    }
                    (batch.num_rows(), batch.get_array_memory_size())
                }
            };
            if (sampled_rows == 0) != (value.rows == 0) {
                return Err(invalid_export());
            }
            Ok(ExportValidation {
                object: value.object,
                rows: value.rows,
                sampled_rows,
                sampled_decoded_bytes,
            })
        })
        .await
}

/// Hashes the exact byte stream accepted by the maintained Parquet writer.
struct HashingFile {
    file: File,
    hash: blake3::Hasher,
    bytes: u64,
}

impl HashingFile {
    fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create_new(true).write(true).open(path)?,
            hash: blake3::Hasher::new(),
            bytes: 0,
        })
    }

    fn finish(&self, owner: &Id, path: &Path) -> Result<ObjectRef> {
        self.file.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(ObjectRef {
            owner: owner.clone(),
            path: path.to_string_lossy().into_owned(),
            digest: self.hash.finalize().to_hex().to_string(),
            bytes: self.bytes,
        })
    }
}

impl Write for HashingFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.hash.update(&bytes[..written]);
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::ExportRows;

    #[test]
    fn schema_authorization_removes_metadata_and_refuses_internal_or_nested_columns() {
        let field = |name, datatype, id: &str| {
            Field::new(name, datatype, true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.into(), id.into()),
                (
                    "source_documentation".into(),
                    "undisclosed-metadata-canary".into(),
                ),
            ]))
        };
        let source = Arc::new(Schema::new_with_metadata(
            vec![
                field("public", DataType::Utf8, "7"),
                field("secret", DataType::Utf8, "9"),
                field("__docspec_left_file", DataType::Utf8, "11"),
                field(
                    "nested",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    "13",
                ),
            ],
            HashMap::from([("source_path".into(), "private-evidence-path".into())]),
        ));
        let mut policy = ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["public".into()],
        };
        let (indices, selected, fields) = authorized_schema(&source, &policy).unwrap();
        assert_eq!(indices, [0]);
        assert!(selected.metadata().is_empty());
        assert_eq!(
            selected.field(0).metadata(),
            &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "7".into())])
        );
        assert_eq!(fields[0].id, 7);
        policy.columns = vec!["__docspec_left_file".into()];
        assert_eq!(
            authorized_schema(&source, &policy).unwrap_err().code(),
            "export_disclosure"
        );
        policy.columns = vec!["nested".into()];
        assert_eq!(
            authorized_schema(&source, &policy).unwrap_err().code(),
            "unsupported_export_type"
        );
    }
    #[tokio::test]
    async fn retained_lookup_binds_requested_identity_and_output_is_protected() {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("input.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![
                Some(1),
                None,
            ]))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let id = |name| Id::new(name).unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                // This two-row identity fixture does not need the production scratch budget.
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let source = engine
            .run(&RunSpec {
                dataset: id("data"),
                operation: id("initial"),
                base: None,
                relationship: None,
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
            })
            .await
            .unwrap();
        let spec = ExportSpec {
            dataset: id("data"),
            operation: id("public"),
            source: source.version_ref(),
            channel: id("download"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec!["value".into()],
            },
        };
        let pin = engine.export(&spec).await.unwrap();
        let inspection = engine
            .inspect_retention(std::slice::from_ref(&pin.object))
            .await
            .unwrap();
        assert!(inspection.objects[0].blocker.is_some());
        let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
        engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record("export-result/wrong", &pin).unwrap(),
                    record("export-channel/wrong", &pin).unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            engine
                .export_pin(&spec.dataset, &id("wrong"))
                .await
                .unwrap_err()
                .code(),
            "invalid_export"
        );
        assert_eq!(
            engine
                .latest_export(&spec.dataset, &id("wrong"))
                .await
                .unwrap_err()
                .code(),
            "invalid_export"
        );
        assert_eq!(
            engine.export_pin(&spec.dataset, &pin.id).await.unwrap(),
            pin
        );
    }
}
