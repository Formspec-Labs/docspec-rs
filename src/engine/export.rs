//! Durable authorized flat exports using retained native reads and Parquet writes.
// Rust guideline compliant 2026-02-21

use super::*;

pub(in crate::engine) mod host;
pub mod pipeline;
pub(super) mod remote;
use crate::export::{
    ChildTableExport, ExportField, ExportKeyRange, ExportPin, ExportPolicy, ExportSpec,
    ExportValidation, FacetCountEntry,
};
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use parquet::{
    arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder,
    },
    basic::{Compression, ZstdLevel},
    file::{
        metadata::FooterTail,
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facets: Vec<FacetCountEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub browse_ranges: Vec<ExportKeyRange>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub child_tables: BTreeMap<String, ChildTableExport>,
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
        self.require_data_work()?;
        for group in spec.policy.effective_groups() {
            self.require_group(&group)?;
        }
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

    /// Streams an exact retained primitive export with configured object credentials.
    ///
    /// The pin fixes values and disclosure independently of a later channel selection.
    /// Ordinary reads check availability and schema, without a full payload digest pass.
    ///
    /// # Errors
    /// Refuses inconsistent pins, unavailable objects, unqualified scope, or exhausted capacity.
    pub async fn read_export(&self, pin: &ExportPin) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(pin)?;
        let pin = pin.clone();
        self.admit_stream(memory, move |engine| async move {
            let retained = engine
                .load_export_pin(&pin.dataset, &format!("export-result/{}", pin.id))
                .await?;
            if retained != pin {
                return Err(invalid_export());
            }
            engine.open_export_stream(&retained).await
        })
        .await
    }

    /// Streams a catalog-retained export by its immutable operation identity.
    ///
    /// # Errors
    /// Refuses unknown or inconsistent exports, unavailable objects, or exhausted capacity.
    pub async fn read_export_id(
        &self,
        dataset: &Id,
        operation: &Id,
    ) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(&(dataset, operation))?;
        let dataset = dataset.clone();
        let operation = operation.clone();
        self.admit_stream(memory, move |engine| async move {
            let pin = engine
                .load_export_pin(&dataset, &format!("export-result/{operation}"))
                .await?;
            engine.open_export_stream(&pin).await
        })
        .await
    }

    async fn open_export_stream(&self, pin: &ExportPin) -> Result<SendableRecordBatchStream> {
        let stream = self
            .inner
            .objects
            .read_flat_parquet(&pin.object, &self.inner.lifecycle.budget)
            .await?;
        if describe_schema(&stream.schema())? != pin.fields {
            return Err(invalid_export());
        }
        Ok(stream)
    }

    async fn lookup_export(&self, dataset: &Id, key: &str) -> Result<ExportPin> {
        let memory = self.request_memory(&(dataset, key))?;
        let dataset = dataset.clone();
        let key = key.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.load_export_pin(&dataset, &key).await
        })
        .await
    }

    async fn load_export_pin(&self, dataset: &Id, key: &str) -> Result<ExportPin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.read_view(dataset).await?;
        let pin: ExportPin = self
            .inner
            .lifecycle
            .get(&view, key)
            .await?
            .ok_or_else(|| Error::refusal("no_export", "no completed export is selected"))?;
        if pin.dataset != *dataset {
            return Err(invalid_export());
        }
        if key != format!("export-result/{}", pin.id) {
            let result: ExportPin = self
                .inner
                .lifecycle
                .get(&view, &format!("export-result/{}", pin.id))
                .await?
                .ok_or_else(invalid_export)?;
            if result != pin {
                return Err(invalid_export());
            }
        }
        check_pin(&pin)?;
        let operation: ExportOperation = self
            .inner
            .lifecycle
            .get(&view, &format!("export-operation/{}", pin.id))
            .await?
            .ok_or_else(invalid_export)?;
        check_result(&pin, &operation)?;
        self.check_export_backend(&operation.source, &operation.request.policy)?;
        self.inner
            .objects
            .validate_retained_path(&pin.object.path)?;
        for child in pin.child_tables.values() {
            self.inner
                .objects
                .validate_retained_path(&child.object.path)?;
        }
        if !self.inner.retained.is_local() {
            remote::check_pin_binding(
                &pin,
                &operation,
                self.inner.lifecycle.rows(&view).await?.as_ref(),
                Some(&self.inner.lifecycle.budget),
            )?;
        }
        let expected_key = if key.starts_with("export-result/") {
            format!("export-result/{}", pin.id)
        } else {
            format!("export-channel/{}", operation.request.channel)
        };
        if key != expected_key {
            return Err(invalid_export());
        }
        Ok(pin)
    }

    async fn export_admitted(&self, spec: &ExportSpec, stop: Option<&str>) -> Result<ExportPin> {
        let digest = storage::json_digest(spec)?.to_hex().to_string();
        let operation = self.admit_export(spec, &digest).await?;
        self.check_export_backend(&operation.source, &spec.policy)?;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(pin) = lifecycle
            .get::<ExportPin>(&view, &format!("export-result/{}", spec.operation))
            .await?
        {
            check_result(&pin, &operation)?;
            if !self.inner.retained.is_local() {
                remote::check_pin_binding(
                    &pin,
                    &operation,
                    lifecycle.rows(&view).await?.as_ref(),
                    Some(&lifecycle.budget),
                )?;
            }
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
                let value = if self.inner.retained.is_local() {
                    self.materialize_export(&operation, None, stop).await?
                } else {
                    self.remote_materialize_export(&operation, stop).await?
                };
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
                if !self.inner.retained.is_local() {
                    return Err(invalid_export());
                }
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
        if !self.inner.retained.is_local() {
            self.inner
                .objects
                .validate_retained_path(&materialized.object.path)?;
            let view = lifecycle.open(&spec.dataset).await?;
            remote::check_completed(
                &operation,
                &materialized,
                &validation,
                lifecycle.rows(&view).await?.as_ref(),
                Some(&lifecycle.budget),
            )?;
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
            facets: materialized.facets,
            browse_ranges: materialized.browse_ranges,
            child_tables: materialized.child_tables,
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
        self.check_export_backend(&source, &spec.policy)?;
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
        for group in spec.policy.effective_groups() {
            let guard_group = if group == "evidence" || group == "receipts" {
                "source"
            } else {
                group.as_str()
            };
            self.guard_corrections(&view, &source.dataset, Some(guard_group))
                .await?;
        }
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
        let rows = self.inner.lifecycle.rows(view).await?;
        self.check_export_recovery(&rows, view, &operation.request.operation)?;
        drop(rows);
        for group in operation.request.policy.effective_groups() {
            let guard_group = if group == "evidence" || group == "receipts" {
                "source"
            } else {
                group.as_str()
            };
            self.guard_corrections(view, &operation.source.dataset, Some(guard_group))
                .await?;
        }
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
        let rows = self.inner.lifecycle.rows(&view).await?;
        self.check_export_recovery(&rows, &view, &operation.request.operation)?;
        drop(rows);
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

    fn check_export_backend(&self, source: &ReleasePin, policy: &ExportPolicy) -> Result<()> {
        if !self.inner.retained.is_local()
            && (policy
                .effective_groups()
                .iter()
                .any(|g| BackendCapability::for_group(g).local_only())
                || !source.dataset.groups.contains_key("source"))
        {
            return Err(locations::unsupported());
        }
        Ok(())
    }

    async fn materialize_export(
        &self,
        operation: &ExportOperation,
        staged_directory: Option<PathBuf>,
        stop: Option<&str>,
    ) -> Result<ExportMaterialized> {
        let spec = &operation.request;
        if spec.policy.has_extended_features() {
            return self
                .materialize_extended_export(operation, staged_directory, stop)
                .await;
        }
        let budget = &self.inner.lifecycle.budget;
        let table_pin = operation
            .source
            .dataset
            .groups
            .get(&spec.policy.group)
            .ok_or_else(|| {
                Error::refusal("missing_group", "export policy names an absent field group")
            })?;
        // This maintained shared reader projects by native field ID and refuses
        // unqualified native delete files instead of globbing historical data.
        let scan = self.inner.objects.export_scan(table_pin, budget).await?;
        let (indices, schema, fields) = authorized_schema(scan.schema(), &spec.policy)?;
        let is_staged = staged_directory.is_some();
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = staged_directory.unwrap_or_else(|| {
            self.inner
                .root
                .join("objects")
                .join(spec.dataset.as_str())
                .join(id.as_str())
        });
        if !is_staged {
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
        let path = directory.join(if is_staged {
            format!("stage-{}.parquet", uuid::Uuid::new_v4())
        } else {
            "export.parquet".into()
        });
        let initial_path = path.clone();
        let output_schema = schema.clone();
        let max_row_group_bytes = budget.limits.max_batch_bytes.max(1);
        let (output, properties, memory, native) = budget
            .blocking(move || {
                let _memory = memory;
                std::fs::create_dir_all(&directory)?;
                let output = HashingFile::create(&initial_path)?;
                // Exports are consumed by foreign engines, so truncated
                // chunk-level statistics give their readers row-group pruning
                // while the admitted footer stays bounded. Offset indexes and
                // per-page statistics stay disabled: page indexes would
                // multiply the footer past the declared metadata allowance on
                // wide or long exports. Level 3 matches the native writer.
                // Row groups bound by encoded bytes rather than reader batch
                // rows: the writer flushes at `max_batch_bytes` of buffered
                // memory and enforces the same caller byte ceiling on encoded
                // row-group size.
                let properties = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                    .set_max_row_group_row_count(None)
                    .set_max_row_group_bytes(Some(max_row_group_bytes))
                    .set_data_page_size_limit(1024 * 1024)
                    .set_dictionary_enabled(true)
                    .set_statistics_enabled(EnabledStatistics::Chunk)
                    .set_statistics_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES))
                    .set_column_index_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES))
                    .set_offset_index_disabled(true)
                    .build();
                let native = if cfg!(unix) {
                    output.file.sync_all()?;
                    File::open(initial_path.parent().ok_or_else(invalid_export)?)?.sync_all()?;
                    Some(host::native_identity(&output.file)?)
                } else {
                    None
                };
                Ok((output, properties, _memory, native))
            })
            .await?;
        let custody = if let Some((device, inode)) = native {
            Some(
                self.retain_host_file(operation, &path, device, inode)
                    .await?,
            )
        } else {
            None
        };
        stop_after(stop, "host-created")?;
        let mut writer = budget
            .blocking(move || {
                Ok((
                    ArrowWriter::try_new(output, output_schema, Some(properties))?,
                    memory,
                ))
            })
            .await?;
        #[cfg(feature = "test-util")]
        work_started("export-materialization");
        let mut rows = 0_u64;
        // A conservative per-leaf-column allowance bounds accumulated footer
        // records across both flat and nested schemas. Truncated chunk
        // statistics, names, and type descriptions add their actual size. This
        // is admission accounting, not a measured RSS claim.
        let primitive_leaves = count_primitive_leaves(&schema);
        let footer_group_bytes = fields.iter().fold(
            1024_usize.saturating_add(primitive_leaves.saturating_mul(1024)),
            |size, field| size.saturating_add(field.name.len() + field.data_type.len()),
        );
        for file in scan.files() {
            let stream = scan.read_projected_file(file.file_path(), budget, Some(&indices))?;
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await? {
                let columns = batch
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(column, field)| sanitize_export_array(column, field.data_type()))
                    .collect::<Result<Vec<_>>>()?;
                let batch = RecordBatch::try_new(schema.clone(), columns)?;
                rows = rows
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(invalid_export)?;
                let limits = budget.limits.clone();
                let root = self.inner.root.clone();
                #[cfg(feature = "test-util")]
                let stop_payload = stop == Some("host-payload");
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
                        #[cfg(feature = "test-util")]
                        if stop_payload { writer.0.flush()?; }
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
                stop_after(stop, "host-payload")?;
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
        let materialized = ExportMaterialized {
            request_digest: operation.request_digest.clone(),
            object,
            rows,
            fields,
            facets: Vec::new(),
            browse_ranges: Vec::new(),
            child_tables: BTreeMap::new(),
        };
        if let Some(custody) = custody {
            self.retain_export_record(
                operation,
                &format!(
                    "export-host-closed/{}/{}/{}",
                    custody.operation, custody.operation_revision, custody.id
                ),
                &materialized,
            )
            .await?;
        }
        Ok(materialized)
    }
}

fn validate_request(spec: &ExportSpec) -> Result<()> {
    let policy = &spec.policy;
    let columns = &policy.columns;
    let groups = policy.effective_groups();
    if spec.source.dataset != spec.dataset
        || groups.is_empty()
        || groups.iter().any(|g| g.is_empty())
        || groups.iter().collect::<BTreeSet<_>>().len() != groups.len()
        || columns.is_empty()
        || columns.iter().any(|name| name.is_empty())
        || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
        || policy.computed_columns.keys().any(|name| name.is_empty())
        || policy.order_by.iter().any(|sc| sc.column.is_empty())
        || policy.page_index_columns.iter().any(|c| c.is_empty())
        || policy.browse_range_columns.iter().any(|c| c.is_empty())
        || policy
            .facet_sets
            .iter()
            .any(|set| set.is_empty() || set.iter().any(|c| c.is_empty()))
        || policy.row_group_rows == Some(0)
    {
        return Err(Error::refusal(
            "invalid_export",
            "export requires one exact source and distinct explicit columns",
        ));
    }
    if let Some(fold) = &policy.fold_readings
        && (fold.target_column.is_empty() || fold.columns.iter().any(|c| c.is_empty()))
    {
        return Err(Error::refusal(
            "invalid_export",
            "export fold_readings requires a nonempty target_column",
        ));
    }
    for (child_name, child_spec) in &policy.child_tables {
        if Id::new(child_name).is_err()
            || child_name == "export"
            || child_spec.list_column.is_empty()
            || child_spec.parent_key_columns.is_empty()
            || child_spec.parent_key_columns.iter().any(|c| c.is_empty())
            || (!child_spec.child_key_columns.is_empty()
                && child_spec.child_key_columns.len() != child_spec.parent_key_columns.len())
            || child_spec.ordinal_column.is_empty()
            || child_spec.row_group_rows == 0
        {
            return Err(Error::refusal(
                "invalid_export",
                "invalid child_tables specification in export policy",
            ));
        }
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
    let expected_cols = policy.expected_output_columns();
    let groups = policy.effective_groups();
    let anchor_group = groups
        .iter()
        .find(|g| *g != "evidence" && *g != "receipts")
        .map_or_else(|| policy.primary_group(), String::as_str);
    let expected_rows = if anchor_group == "evidence" || anchor_group == "receipts" {
        operation
            .source
            .dataset
            .evidence
            .as_ref()
            .map(|ev| ev.table.rows)
    } else {
        operation
            .source
            .dataset
            .groups
            .get(anchor_group)
            .map(|g| g.rows)
    };
    let rows_ok = match expected_rows {
        Some(group_rows) if policy.filter.is_some() => value.rows <= group_rows,
        Some(group_rows) => value.rows == group_rows,
        None => false,
    };
    if value.request_digest != operation.request_digest
        || value.object.owner != operation.request.dataset
        || value
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(expected_cols.iter())
        || !rows_ok
        || value.child_tables.keys().ne(policy.child_tables.keys())
        || value.child_tables.values().any(|child| {
            child.object.owner != operation.request.dataset
                || child.validation.object != child.object
                || child.validation.rows != child.rows
                || child.fields.is_empty()
        })
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(super) fn check_pin(pin: &ExportPin) -> Result<()> {
    let expected_cols = pin.policy.expected_output_columns();
    if pin.format != 1
        || pin.dataset != pin.source.dataset
        || pin.dataset != pin.object.owner
        || pin.validation.object != pin.object
        || pin.validation.rows != pin.rows
        || pin
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(expected_cols.iter())
        || pin.source_revision <= 0
        || pin.fields.is_empty()
        || pin.child_tables.keys().ne(pin.policy.child_tables.keys())
        || pin.child_tables.values().any(|child| {
            child.object.owner != pin.dataset
                || child.validation.object != child.object
                || child.validation.rows != child.rows
                || child.fields.is_empty()
        })
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
    let mut seen_ids = BTreeSet::new();
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
        fields.push(sanitize_export_field(name, field, &mut seen_ids)?);
        indices.push(index);
    }
    let schema = Arc::new(Schema::new(fields));
    let fields = describe_schema(&schema)?;
    Ok((indices, schema, fields))
}

fn sanitize_export_field(
    root_name: &str,
    field: &Field,
    seen_ids: &mut BTreeSet<i32>,
) -> Result<Field> {
    if field.name().to_ascii_lowercase().starts_with("__docspec_") {
        return Err(Error::refusal(
            "export_disclosure",
            "internal occurrence and evidence fields cannot be exported",
        ));
    }
    let data_type = match field.data_type() {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Decimal128(_, _) => field.data_type().clone(),
        DataType::List(item) => {
            DataType::List(Arc::new(sanitize_export_field(root_name, item, seen_ids)?))
        }
        DataType::Struct(fields) if !fields.is_empty() => {
            let mut sanitized = Vec::with_capacity(fields.len());
            for child in fields {
                sanitized.push(Arc::new(sanitize_export_field(root_name, child, seen_ids)?));
            }
            DataType::Struct(sanitized.into())
        }
        unsupported => {
            return Err(Error::refusal(
                "unsupported_export_type",
                format!("local flat export column {root_name} has unsupported type {unsupported}"),
            ));
        }
    };
    let id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|id| *id > 0)
        .ok_or_else(invalid_export)?;
    if !seen_ids.insert(id) {
        return Err(invalid_export());
    }
    // Rebuild every root and descendant field: source documentation/defaults
    // and arbitrary metadata never reach the public Parquet footer or its
    // encoded Arrow schema.
    Ok(
        Field::new(field.name(), data_type, field.is_nullable()).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            id.to_string(),
        )])),
    )
}

fn count_primitive_leaves(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| data_type_primitive_leaves(field.data_type()))
        .fold(0_usize, usize::saturating_add)
}

fn data_type_primitive_leaves(data_type: &DataType) -> usize {
    match data_type {
        DataType::List(item) => data_type_primitive_leaves(item.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| data_type_primitive_leaves(field.data_type()))
            .fold(0_usize, usize::saturating_add),
        _ => 1,
    }
}

fn sanitize_export_array(
    array: &arrow::array::ArrayRef,
    data_type: &DataType,
) -> Result<arrow::array::ArrayRef> {
    if array.data_type() == data_type {
        return Ok(array.clone());
    }
    match data_type {
        DataType::List(item) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(invalid_export)?;
            let values = sanitize_export_array(list.values(), item.data_type())?;
            Ok(Arc::new(arrow::array::ListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        DataType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(invalid_export)?;
            if structure.columns().len() != fields.len() {
                return Err(invalid_export());
            }
            let columns = structure
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(child, field)| sanitize_export_array(child, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(arrow::array::StructArray::try_new(
                fields.clone(),
                columns,
                structure.nulls().cloned(),
            )?))
        }
        _ => Err(invalid_export()),
    }
}

fn describe_schema(schema: &Schema) -> Result<Vec<ExportField>> {
    if !schema.metadata().is_empty() {
        return Err(invalid_export());
    }
    let mut seen_ids = BTreeSet::new();
    schema
        .fields()
        .iter()
        .map(|field| {
            let sanitized = sanitize_export_field(field.name(), field, &mut seen_ids)
                .map_err(|_| invalid_export())?;
            if &sanitized != field.as_ref() {
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
    for child in materialized.child_tables.values() {
        let child_val =
            validate_output_object(&child.object, child.rows, &child.fields, budget).await?;
        if child_val.object != child.object || child_val.rows != child.rows {
            return Err(invalid_export());
        }
    }
    validate_output_object(
        &materialized.object,
        materialized.rows,
        &materialized.fields,
        budget,
    )
    .await
}

pub(super) async fn validate_output_object(
    object: &ObjectRef,
    rows: u64,
    fields: &[ExportField],
    budget: &Budget,
) -> Result<ExportValidation> {
    let object = object.clone();
    let fields = fields.to_vec();
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
            let mut file = File::open(&object.path)?;
            if file.metadata()?.len() != object.bytes {
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
            if describe_schema(builder.schema())? != fields
                || u64::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(rows)
            {
                return Err(invalid_export());
            }
            // One small shape probe. The reader may decode compressed pages beyond
            // the returned rows; this is not a compressed-I/O or total-memory claim.
            let mut reader = builder.with_batch_size(8.min(limits.batch_rows)).build()?;
            let (sampled_rows, sampled_decoded_bytes) = match reader.next().transpose()? {
                None => (0, 0),
                Some(batch) => {
                    if storage::effective_batch_bytes(&batch) > limits.max_batch_bytes {
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
            if (sampled_rows == 0) != (rows == 0) {
                return Err(invalid_export());
            }
            Ok(ExportValidation {
                object,
                rows,
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
    fn schema_authorization_removes_metadata_allows_nested_and_refuses_internal_or_unsupported_columns()
     {
        let field = |name, datatype, id: &str| {
            Field::new(name, datatype, true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.into(), id.into()),
                ("doc".into(), "private-child-doc".into()),
                (
                    "source_documentation".into(),
                    "undisclosed-metadata-canary".into(),
                ),
            ]))
        };
        let map_entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );
        let source = Arc::new(Schema::new_with_metadata(
            vec![
                field("public", DataType::Utf8, "7"),
                field("secret", DataType::Utf8, "9"),
                field("__docspec_left_file", DataType::Utf8, "11"),
                field(
                    "nested_list",
                    DataType::List(Arc::new(field("item", DataType::Utf8, "14"))),
                    "13",
                ),
                field(
                    "nested_struct",
                    DataType::Struct(
                        vec![
                            field("start", DataType::Int32, "16"),
                            field("end", DataType::Int64, "18"),
                        ]
                        .into(),
                    ),
                    "15",
                ),
                field(
                    "unsupported",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    "17",
                ),
                field(
                    "nested_internal",
                    DataType::Struct(vec![field("__docspec_row", DataType::Int64, "20")].into()),
                    "19",
                ),
                field(
                    "nested_unsupported_struct",
                    DataType::Struct(
                        vec![field(
                            "bad_dict",
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            "22",
                        )]
                        .into(),
                    ),
                    "21",
                ),
                field(
                    "nested_unsupported_list",
                    DataType::List(Arc::new(field(
                        "item",
                        DataType::Map(Arc::new(map_entries), false),
                        "24",
                    ))),
                    "23",
                ),
                field(
                    "nested_empty_struct",
                    DataType::Struct(Vec::<Field>::new().into()),
                    "25",
                ),
                field(
                    "nested_missing_id",
                    DataType::Struct(vec![Field::new("untagged", DataType::Int32, true)].into()),
                    "27",
                ),
                field(
                    "nested_duplicate_id",
                    DataType::Struct(vec![field("dup", DataType::Int32, "29")].into()),
                    "29",
                ),
            ],
            HashMap::from([("source_path".into(), "private-evidence-path".into())]),
        ));
        let mut policy = ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec![
                "public".into(),
                "nested_list".into(),
                "nested_struct".into(),
            ],
            ..ExportPolicy::default()
        };
        let (indices, selected, fields) = authorized_schema(&source, &policy).unwrap();
        assert_eq!(indices, [0, 3, 4]);
        assert_eq!(count_primitive_leaves(&selected), 4);
        assert!(selected.metadata().is_empty());
        assert_eq!(
            selected.field(0).metadata(),
            &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "7".into())])
        );
        let DataType::List(list_item) = selected.field(1).data_type() else {
            panic!("expected list data type");
        };
        assert_eq!(
            list_item.metadata(),
            &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "14".into())])
        );
        let DataType::Struct(struct_fields) = selected.field(2).data_type() else {
            panic!("expected struct data type");
        };
        assert_eq!(
            struct_fields[0].metadata(),
            &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "16".into())])
        );
        assert_eq!(
            struct_fields[1].metadata(),
            &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "18".into())])
        );
        for exported in &fields {
            assert!(!exported.data_type.contains("undisclosed-metadata-canary"));
            assert!(!exported.data_type.contains("private-child-doc"));
        }
        assert_eq!(fields[0].id, 7);
        assert_eq!(fields[1].id, 13);
        assert_eq!(fields[2].id, 15);
        for internal in ["__docspec_left_file", "nested_internal"] {
            policy.columns = vec![internal.into()];
            assert_eq!(
                authorized_schema(&source, &policy).unwrap_err().code(),
                "export_disclosure"
            );
        }
        for bad_type in [
            "unsupported",
            "nested_unsupported_struct",
            "nested_unsupported_list",
            "nested_empty_struct",
        ] {
            policy.columns = vec![bad_type.into()];
            assert_eq!(
                authorized_schema(&source, &policy).unwrap_err().code(),
                "unsupported_export_type"
            );
        }
        for bad_id in ["nested_missing_id", "nested_duplicate_id"] {
            policy.columns = vec![bad_id.into()];
            assert_eq!(
                authorized_schema(&source, &policy).unwrap_err().code(),
                "invalid_export"
            );
        }
    }

    #[tokio::test]
    async fn nested_list_and_struct_columns_export_and_read_back() {
        use arrow::{
            array::{
                Int32Array, Int32Builder, ListBuilder, StringBuilder, StructArray, StructBuilder,
            },
            buffer::NullBuffer,
        };
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("nested.parquet");
        let child_meta = |doc: &str| {
            HashMap::from([
                ("doc".into(), doc.into()),
                (
                    "custom_secret".into(),
                    "private-descendant-custom-canary".into(),
                ),
            ])
        };
        let span_fields: arrow::datatypes::Fields = vec![
            Field::new("start", DataType::Int32, false)
                .with_metadata(child_meta("private-descendant-doc-canary-start")),
            Field::new("end", DataType::Int32, true)
                .with_metadata(child_meta("private-descendant-doc-canary-end")),
        ]
        .into();
        let mut aliases_builder = ListBuilder::new(StringBuilder::new()).with_field(
            Field::new("item", DataType::Utf8, true).with_metadata(HashMap::from([(
                "custom_secret".into(),
                "private-descendant-custom-canary".into(),
            )])),
        );
        aliases_builder.values().append_value("https://a.example");
        aliases_builder.values().append_null();
        aliases_builder.values().append_value("https://b.example");
        aliases_builder.append(true);
        aliases_builder.append(false);
        aliases_builder.append(true);
        let aliases = Arc::new(aliases_builder.finish());

        let mut spans_builder = ListBuilder::new(StructBuilder::new(
            span_fields.clone(),
            vec![Box::new(Int32Builder::new()), Box::new(Int32Builder::new())],
        ))
        .with_field(
            Field::new("item", DataType::Struct(span_fields.clone()), true).with_metadata(
                HashMap::from([(
                    "custom_secret".into(),
                    "private-descendant-custom-canary".into(),
                )]),
            ),
        );
        spans_builder
            .values()
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        spans_builder
            .values()
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(4);
        spans_builder.values().append(true);
        spans_builder
            .values()
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(5);
        spans_builder
            .values()
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null();
        spans_builder.values().append(true);
        spans_builder.append(true);
        spans_builder.append(true);
        spans_builder.append(false);
        let spans = Arc::new(spans_builder.finish());

        let details = Arc::new(StructArray::new(
            span_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 10, 20])),
                Arc::new(Int32Array::from(vec![Some(5), None, Some(25)])),
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("url_aliases", aliases.data_type().clone(), true),
            Field::new("spans", spans.data_type().clone(), true),
            Field::new("span", DataType::Struct(span_fields), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                aliases.clone(),
                spans.clone(),
                details.clone(),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let id = |name| Id::new(name).unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let source = engine
            .run(&RunSpec {
                dataset: id("docs"),
                operation: id("initial"),
                base: None,
                relationship: None,
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let spec = ExportSpec {
            dataset: id("docs"),
            operation: id("nested-export"),
            source: source.version_ref(),
            channel: id("download"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec![
                    "id".into(),
                    "url_aliases".into(),
                    "spans".into(),
                    "span".into(),
                ],
                ..ExportPolicy::default()
            },
        };
        let pin = engine.export(&spec).await.unwrap();
        assert_eq!(pin.rows, 3);
        assert_eq!(pin.fields.len(), 4);
        for field in &pin.fields {
            assert!(!field.data_type.contains("private-descendant-doc-canary"));
            assert!(!field.data_type.contains("private-descendant-custom-canary"));
        }
        let raw_bytes = std::fs::read(&pin.object.path).unwrap();
        assert!(
            !raw_bytes
                .windows(b"private-descendant".len())
                .any(|window| window == b"private-descendant")
        );
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap())
                .unwrap();
        assert!(builder.schema().metadata().is_empty());
        let read = engine.read_export(&pin).await.unwrap();
        let batches = datafusion::physical_plan::common::collect(read)
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let exported = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let ids = exported
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for row in 0..exported.num_rows() {
            let source_row = usize::try_from(ids.value(row) - 1).unwrap();
            for col in 0..4 {
                let expected = sanitize_export_array(
                    batch.column(col),
                    exported.schema().field(col).data_type(),
                )
                .unwrap();
                assert_eq!(
                    exported.column(col).slice(row, 1).to_data(),
                    expected.slice(source_row, 1).to_data()
                );
            }
        }
    }

    #[tokio::test]
    async fn wide_nested_struct_exceeding_metadata_budget_refuses_export() {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("wide-nested.parquet");
        let leaf_fields: arrow::datatypes::Fields = (0..40)
            .map(|index| Field::new(format!("f{index}"), DataType::Int32, false))
            .collect::<Vec<_>>()
            .into();
        let leaf_arrays = (0..40)
            .map(|index| {
                Arc::new(arrow::array::Int32Array::from(vec![index])) as arrow::array::ArrayRef
            })
            .collect::<Vec<_>>();
        let wide_struct = Arc::new(arrow::array::StructArray::new(
            leaf_fields.clone(),
            leaf_arrays,
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("wide", DataType::Struct(leaf_fields), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1_i64])),
                wide_struct,
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let id = |name| Id::new(name).unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let release = engine
            .run(&RunSpec {
                dataset: id("wide"),
                operation: id("initial"),
                base: None,
                relationship: None,
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        drop(engine);

        // 41 primitive leaves (id + 40 struct children) require >42 KiB of
        // per-row-group footer admission, whereas root-column counting would
        // estimate <6 KiB and mistakenly admit under a 32 KiB metadata cap.
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                max_metadata_bytes: 32 << 10,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let spec = ExportSpec {
            dataset: id("wide"),
            operation: id("wide-export"),
            source: release.version_ref(),
            channel: id("download"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec!["id".into(), "wide".into()],
                ..ExportPolicy::default()
            },
        };
        let err = engine.export(&spec).await.unwrap_err();
        assert_eq!(err.code(), "metadata_capacity");
    }

    #[tokio::test]
    async fn caller_limits_bound_row_groups_and_preserve_global_dictionary_encoding() {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("limits-input.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("custom_category", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let row_count = 160_i64;
        let ids = (0..row_count).collect::<Vec<_>>();
        let categories = (0..row_count)
            .map(|index| if index % 2 == 0 { "alpha" } else { "beta" })
            .collect::<Vec<_>>();
        let payloads = (0..row_count)
            .map(|index| format!("payload-{index:04}-{}", "x".repeat(64)))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids)),
                Arc::new(arrow::array::StringArray::from(categories)),
                Arc::new(arrow::array::StringArray::from(payloads)),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let id = |name| Id::new(name).unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                batch_rows: 64,
                max_batch_bytes: 64 << 10,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let release = engine
            .run(&RunSpec {
                dataset: id("bounded"),
                operation: id("initial"),
                base: None,
                relationship: None,
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let native_parquet = release.dataset.groups["source"]
            .objects
            .iter()
            .find(|object| object.path.ends_with(".parquet"))
            .unwrap();
        let native_reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&native_parquet.path).unwrap())
                .unwrap();
        assert!(native_reader.metadata().num_row_groups() >= 3);
        for group in native_reader.metadata().row_groups() {
            assert!(group.num_rows() <= 64);
            let category_col = group
                .columns()
                .iter()
                .find(|col| col.column_path().string() == "custom_category")
                .unwrap();
            assert!(category_col.dictionary_page_offset().is_some());
        }
        drop(engine);

        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                batch_rows: 8,
                max_batch_bytes: 16 << 10,
                ..Limits::default()
            },
        )
        .await
        .unwrap();

        let pin = engine
            .export(&ExportSpec {
                dataset: id("bounded"),
                operation: id("export-bounded"),
                source: release.version_ref(),
                channel: id("download"),
                expected: None,
                policy: ExportPolicy {
                    group: "source".into(),
                    rows: ExportRows::All,
                    columns: vec!["id".into(), "custom_category".into(), "payload".into()],
                    ..ExportPolicy::default()
                },
            })
            .await
            .unwrap();
        let export_reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap())
                .unwrap();
        assert!(export_reader.metadata().num_row_groups() >= 2);
        for group in export_reader.metadata().row_groups() {
            let category_col = group
                .columns()
                .iter()
                .find(|col| col.column_path().string() == "custom_category")
                .unwrap();
            assert!(category_col.dictionary_page_offset().is_some());
        }
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
                partition_by: Vec::new(),
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
                ..ExportPolicy::default()
            },
        };
        let pin = engine.export(&spec).await.unwrap();
        // Both transport entrypoints resolve the same immutable retained export.
        let read = engine
            .read_export_id(&spec.dataset, &spec.operation)
            .await
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(read)
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
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(1), None]);
        let mut wrong_pin = pin.clone();
        wrong_pin.object.digest = "0".repeat(64);
        assert_eq!(
            engine.read_export(&wrong_pin).await.err().unwrap().code(),
            "invalid_export"
        );
        let read = engine.read_export(&pin).await.unwrap();
        let batches = datafusion::physical_plan::common::collect(read)
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
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
        #[cfg(feature = "remote")]
        remote::test_retained_phase_revision_bindings(&engine, &spec, &pin).await;
        #[cfg(not(feature = "remote"))]
        let _ = remote::test_retained_phase_revision_bindings;
    }
}
