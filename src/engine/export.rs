//! Durable authorized flat exports using retained native reads and Parquet writes.
// Rust guideline compliant 2026-02-21

use super::*;

pub(in crate::engine) mod host;
pub mod pipeline;
mod read;
pub(super) mod remote;
mod validation;

#[cfg(test)]
mod tests;

#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
use validation::{
    authorized_schema, check_operation, count_primitive_leaves, describe_schema, invalid_export,
    sanitize_export_array, validate_output, validate_output_object, validate_request,
};
pub(in crate::engine) use validation::{
    check_materialized, check_pin, check_result, validate_operation_record,
};

use crate::export::{
    ChildTableExport, ExportField, ExportKeyRange, ExportPin, ExportPolicy, ExportSpec,
    ExportValidation, FacetCountEntry,
};
use arrow::record_batch::RecordBatch;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use std::{
    fs::{File, OpenOptions},
    io::Write,
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

    pub(super) async fn check_export_authority(
        &self,
        view: &View,
        operation: &ExportOperation,
    ) -> Result<()> {
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

    pub(super) async fn retain_export_record<T>(
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

    pub(super) fn check_export_backend(
        &self,
        source: &ReleasePin,
        policy: &ExportPolicy,
    ) -> Result<()> {
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

    pub(super) async fn materialize_export(
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
                        if stop_payload {
                            writer.0.flush()?;
                        }
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

/// Hashes the exact byte stream accepted by the maintained Parquet writer.
pub(super) struct HashingFile {
    pub(super) file: File,
    hash: blake3::Hasher,
    bytes: u64,
}

impl HashingFile {
    pub(super) fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create_new(true).write(true).open(path)?,
            hash: blake3::Hasher::new(),
            bytes: 0,
        })
    }

    pub(super) fn finish(&self, owner: &Id, path: &Path) -> Result<ObjectRef> {
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
