//! Shared retained scalar preparation over selected or candidate source snapshots.

use super::*;

#[cfg(all(test, feature = "test-util"))]
#[path = "file_completion_tests.rs"]
mod tests;

/// A canonical file completion and the exact retained generation that won.
pub(in crate::engine) struct AcknowledgedFile {
    pub(in crate::engine) completion: FileCompletion,
    pub(in crate::engine) generation: RetainedGeneration,
    pub(in crate::engine) protections: Option<Arc<ReferenceProtections>>,
}

struct FileAssembly {
    files: Vec<DataFile>,
    inherited: Vec<ObjectRef>,
    origins: Vec<FileOrigin>,
    protections: Option<ReferenceProtections>,
    bytes: usize,
}

impl FileAssembly {
    async fn append(
        &mut self,
        acknowledged: Vec<AcknowledgedFile>,
        expressions: &Expressions,
        eligibility: u64,
        budget: &Budget,
        checks: &mut super::super::verification::Checks,
    ) -> Result<()> {
        for file in acknowledged {
            if file.generation.key != file_key(&file.completion.source, expressions, eligibility)? {
                return Err(invalid_group());
            }
            self.bytes = charge_assembly(
                self.bytes,
                &file.completion,
                budget.limits.max_metadata_bytes,
            )?;
            self.bytes = self.bytes.saturating_add(inputs::metadata_size(
                &file.generation,
                budget.limits.max_metadata_bytes.saturating_sub(self.bytes),
            )?);
            let files = checks
                .scalar_file(&file.completion, &expressions.native, budget)
                .await?;
            self.files.extend(files.iter().cloned());
            self.inherited
                .extend(file.completion.outputs.iter().cloned());
            ReferenceProtections::inherit(&mut self.protections, file.protections.as_deref())?;
            self.origins.push(FileOrigin {
                generation: file.generation,
                completion: file.completion,
            });
        }
        Ok(())
    }
}

impl Engine {
    #[expect(
        clippy::too_many_arguments,
        reason = "One operation shares validation and sampling state across field groups"
    )]
    pub(in crate::engine) async fn prepare_group(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        source: &TablePin,
        source_protections: Option<&ReferenceProtections>,
        definition: &GroupDefinition,
        input_fields: Option<&BTreeMap<String, i32>>,
        stop: Option<&str>,
        checks: &mut super::super::verification::Checks,
    ) -> Result<PreparedGroup> {
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(request.dataset()).await;
        let view = lifecycle.open(request.dataset()).await?;
        self.check_not_abandoned(&view, request.operation()).await?;
        let native = storage::open_pin(source, &self.inner.lifecycle.budget).await?;
        self.check_continuous_source_schema(
            &view,
            request.operation(),
            native.metadata().current_schema(),
        )
        .await?;
        let expressions = Arc::new(Expressions::compile(
            &self.context(),
            native.metadata().current_schema(),
            definition,
            input_fields,
        )?);
        let digest = storage::json_digest(&(
            PROCESSOR,
            source.logical_metadata(),
            definition,
            &expressions.input_fields,
        ))?
        .to_hex()
        .to_string();
        let prepared_key = format!("backfill-prepared/{}/{digest}", operation.eligibility);
        let prepared: Option<PreparedCompletion> =
            self.active_completion(&view, &prepared_key).await?;
        let (prepared, origins, mut protections) = if let Some(prepared) = prepared {
            check_group_dependencies(
                &prepared_key,
                prepared.dependencies.as_ref(),
                source,
                &prepared.table,
            )?;
            storage::check_available(prepared.table.objects.clone(), &lifecycle.budget).await?;
            (
                prepared.table,
                prepared.origins,
                self.retained_protections(&view, &prepared_key).await?,
            )
        } else {
            #[cfg(feature = "test-util")]
            work_started("backfill-assembly");
            let directory = self.reserve_group(request, operation, "processing").await?;
            let output = self
                .create_output_at(request.dataset(), &directory, expressions.native.clone())
                .await?;
            let scan = storage::OccurrenceScan::open(&native, &lifecycle.budget).await?;
            let objects = source_objects(source);
            let mut completions = BTreeMap::new();
            let mut assembly = FileAssembly {
                files: Vec::new(),
                inherited: Vec::new(),
                origins: Vec::new(),
                protections: source_protections
                    .map(ReferenceProtections::duplicate)
                    .transpose()?,
                bytes: 0,
            };
            let _assembly = lifecycle
                .budget
                .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(2))?;
            // One declaration serves every newly computed file in this preparation.
            // The reserved prefix protects it before any file acknowledgment.
            let declaration = origins::write_at(
                &self.inner.objects,
                request.dataset(),
                &directory.child("declaration.json")?,
                &OriginalDeclaration {
                    processor: PROCESSOR.into(),
                    definition: definition.clone(),
                    input_fields: expressions.input_fields.clone(),
                },
                &lifecycle.budget,
            )
            .await?;
            let mut completion_view = lifecycle.open(request.dataset()).await?;
            for file in scan.files() {
                let object = objects.get(file.file_path()).ok_or_else(invalid_group)?;
                let key = file_key(object, &expressions, operation.eligibility)?;
                let existing: Option<FileCompletion> =
                    self.active_completion(&completion_view, &key).await?;
                if let Some(existing) = existing {
                    if existing.source != **object || existing.rows != file.record_count() {
                        return Err(invalid_group());
                    }
                    let rows = lifecycle.rows(&completion_view).await?;
                    let retained = AcknowledgedFile {
                        completion: existing,
                        generation: RetainedGeneration {
                            key: key.clone(),
                            revision: rows.revision(&key).ok_or_else(invalid_group)?,
                        },
                        protections: self
                            .retained_protections(&completion_view, &key)
                            .await?
                            .map(Arc::new),
                    };
                    #[cfg(feature = "test-util")]
                    self.scalar_file_reused(request).await;
                    if let Err(error) = assembly
                        .append(
                            vec![retained],
                            &expressions,
                            operation.eligibility,
                            &lifecycle.budget,
                            checks,
                        )
                        .await
                    {
                        self.acknowledge_files(
                            request,
                            operation,
                            &mut completions,
                            source_protections,
                        )
                        .await?;
                        return Err(error);
                    }
                    continue;
                }
                // A new execution of the same deficient definition cannot create
                // a qualifying historical baseline after its omission is known.
                let correction_view = lifecycle.open(request.dataset()).await?;
                if self
                    .requirements(&correction_view, definition)
                    .await?
                    .is_some()
                {
                    self.acknowledge_files(
                        request,
                        operation,
                        &mut completions,
                        source_protections,
                    )
                    .await?;
                    return Err(super::super::corrections::mismatch());
                }
                let result = self
                    .compute_file(
                        request.dataset(),
                        &directory,
                        &scan,
                        object,
                        file.record_count(),
                        &output,
                        expressions.clone(),
                    )
                    .await;
                let completion = match result {
                    Ok(mut completion) => {
                        completion.source_metadata = Some(source.metadata.clone());
                        completion.execution = Some(OriginalExecution {
                            operation: request.operation().clone(),
                            declaration: declaration.clone(),
                        });
                        completion
                    }
                    Err(error) => {
                        self.acknowledge_files(
                            request,
                            operation,
                            &mut completions,
                            source_protections,
                        )
                        .await?;
                        let _guard = lock.lock().await;
                        let view = lifecycle.open(request.dataset()).await?;
                        self.check_not_abandoned(&view, request.operation()).await?;
                        lifecycle
                            .commit(
                                &view,
                                vec![record(
                                    format!("failure/{}/{key}", request.operation()),
                                    error.to_string(),
                                )?],
                            )
                            .await?;
                        return Err(error);
                    }
                };
                if completion.source != **object || completion.rows != file.record_count() {
                    return Err(invalid_group());
                }
                completions.insert(key, completion);
                let bytes = serde_json::to_vec(&completions)?.len();
                if completions.len() >= COMPLETION_BUNDLE
                    || bytes >= lifecycle.budget.limits.max_metadata_bytes / 8
                {
                    let acknowledged = self
                        .acknowledge_files(request, operation, &mut completions, source_protections)
                        .await?;
                    stop_after(stop, "files")?;
                    assembly
                        .append(
                            acknowledged,
                            &expressions,
                            operation.eligibility,
                            &lifecycle.budget,
                            checks,
                        )
                        .await?;
                    completion_view = lifecycle.open(request.dataset()).await?;
                }
            }
            let acknowledged = self
                .acknowledge_files(request, operation, &mut completions, source_protections)
                .await?;
            stop_after(stop, "files")?;
            assembly
                .append(
                    acknowledged,
                    &expressions,
                    operation.eligibility,
                    &lifecycle.budget,
                    checks,
                )
                .await?;
            let output =
                storage::append(&output, lifecycle.catalog.as_ref(), assembly.files).await?;
            let prepared = self
                .pin_output(request.dataset(), &output, source.rows, &assembly.inherited)
                .await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(request.dataset()).await?;
            self.check_completion(&view, operation, request.operation())
                .await?;
            if let Some(existing) = self
                .active_completion::<PreparedCompletion>(&view, &prepared_key)
                .await?
            {
                check_group_dependencies(
                    &prepared_key,
                    existing.dependencies.as_ref(),
                    source,
                    &existing.table,
                )?;
                (
                    existing.table,
                    existing.origins,
                    self.retained_protections(&view, &prepared_key).await?,
                )
            } else {
                let origins = origins::write_at(
                    &self.inner.objects,
                    request.dataset(),
                    &directory.child("origins.json")?,
                    &ScalarOrigins {
                        processor: PROCESSOR.into(),
                        definition: definition.clone(),
                        input_fields: expressions.input_fields.clone(),
                        dependencies: GroupDependencies {
                            source: source.into(),
                            source_logical_metadata: source.logical_metadata.clone(),
                            output: (&prepared).into(),
                        },
                        files: assembly.origins,
                    },
                    &lifecycle.budget,
                )
                .await?;
                let completed = PreparedCompletion {
                    origins: Some(origins),
                    dependencies: Some(GroupDependencies {
                        source: source.into(),
                        source_logical_metadata: source.logical_metadata.clone(),
                        output: (&prepared).into(),
                    }),
                    table: prepared,
                };
                let records = completion_records(
                    request.dataset(),
                    &view,
                    &prepared_key,
                    &completed,
                    assembly.protections.as_ref(),
                )?;
                lifecycle.commit(&view, records).await?;
                (completed.table, completed.origins, assembly.protections)
            }
        };
        checks
            .scalar_origins(
                origins.as_ref(),
                source,
                &prepared,
                definition,
                &expressions.input_fields,
                &expressions.native_field_ids,
                &lifecycle.budget,
            )
            .await?;
        let correction_view = lifecycle.open(request.dataset()).await?;
        self.check_prepared_correction(
            &correction_view,
            origins.as_ref(),
            source,
            definition,
            checks,
        )
        .await?;
        stop_after(stop, "processing")?;
        let validation_protections = self
            .validate_scalar_group(
                request,
                operation,
                source,
                source_protections,
                definition,
                &expressions.input_fields,
                &expressions.native_field_ids,
                &prepared,
                origins.as_ref(),
                protections.as_ref(),
                checks,
            )
            .await?;
        ReferenceProtections::inherit(&mut protections, validation_protections.as_ref())?;
        stop_after(stop, "validation")?;
        Ok(PreparedGroup {
            origins,
            table: prepared,
            native_field_ids: expressions.native_field_ids.clone(),
            input_fields: expressions.input_fields.clone(),
            protections,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "One checker binds exact source, target interpretation and inherited protections"
    )]
    pub(super) async fn validate_scalar_group(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        source: &TablePin,
        source_protections: Option<&ReferenceProtections>,
        definition: &GroupDefinition,
        input_fields: &BTreeMap<String, i32>,
        native_field_ids: &BTreeMap<i32, i32>,
        output: &TablePin,
        origins: Option<&ObjectRef>,
        output_protections: Option<&ReferenceProtections>,
        checks: &mut super::super::verification::Checks,
    ) -> Result<Option<ReferenceProtections>> {
        let lifecycle = &self.inner.lifecycle;
        let validation_key =
            scalar_validation_key(operation.eligibility, source, definition, input_fields)?;
        let view = lifecycle.open(request.dataset()).await?;
        let validated: Option<ValidationCompletion> =
            self.active_completion(&view, &validation_key).await?;
        let validation_protections = if let Some(validated) = validated {
            check_validation(&validation_key, &validated, source, output, origins)?;
            self.retained_protections(&view, &validation_key).await?
        } else {
            let mut validation_protections = output_protections
                .map(ReferenceProtections::duplicate)
                .transpose()?;
            ReferenceProtections::inherit(&mut validation_protections, source_protections)?;
            #[cfg(feature = "test-util")]
            work_started("backfill-validation");
            // Composition-only readback checked interpretation, not native files.
            // A new validator acknowledgment needs its own bounded routine check.
            if !checks.inspects_tables() {
                *checks = super::super::verification::Checks::routine(self.inner.objects.clone());
            }
            verification::verify_group_data(
                source,
                output,
                definition,
                input_fields,
                native_field_ids,
                &lifecycle.budget,
                checks,
            )
            .await?;
            let lock = self.lock(request.dataset()).await;
            let _guard = lock.lock().await;
            let view = lifecycle.open(request.dataset()).await?;
            self.check_completion(&view, operation, request.operation())
                .await?;
            let existing: Option<ValidationCompletion> =
                self.active_completion(&view, &validation_key).await?;
            if let Some(existing) = existing {
                check_validation(&validation_key, &existing, source, output, origins)?;
                self.retained_protections(&view, &validation_key).await?
            } else {
                let completed = ValidationCompletion {
                    validator: Some(VALIDATOR.into()),
                    origins: origins.cloned(),
                    output: output.metadata.clone(),
                    dependencies: Some(GroupDependencies {
                        source: source.into(),
                        source_logical_metadata: source.logical_metadata.clone(),
                        output: output.into(),
                    }),
                };
                let records = completion_records(
                    request.dataset(),
                    &view,
                    &validation_key,
                    &completed,
                    validation_protections.as_ref(),
                )?;
                lifecycle.commit(&view, records).await?;
                validation_protections
            }
        };
        Ok(validation_protections)
    }

    pub(super) async fn reserve_group(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        purpose: &str,
    ) -> Result<locations::Location> {
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .retained
            .child(request.dataset().as_str())?
            .child(id.as_str())?;
        let lock = self.lock(request.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(request.dataset()).await?;
        self.check_completion(&view, operation, request.operation())
            .await?;
        lifecycle
            .commit(
                &view,
                vec![record(
                    format!("protection/{id}"),
                    Protection {
                        id,
                        consumer: request.dataset().clone(),
                        purpose: format!("{}/{purpose}", request.operation()),
                        paths: vec![directory.protection_prefix()?],
                        retired: false,
                    },
                )?],
            )
            .await?;
        match &directory {
            locations::Location::Local(path) => std::fs::create_dir_all(path)?,
            #[cfg(feature = "remote")]
            locations::Location::Remote(_) => {}
        }
        Ok(directory)
    }

    async fn acknowledge_files(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        pending: &mut BTreeMap<String, FileCompletion>,
        source_protections: Option<&ReferenceProtections>,
    ) -> Result<Vec<AcknowledgedFile>> {
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        let lock = self.lock(request.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(request.dataset()).await?;
        self.check_completion(&view, operation, request.operation())
            .await?;
        let rows = lifecycle.rows(&view).await?;
        let mut changes = Vec::new();
        let mut acknowledged = Vec::new();
        let mut new_keys = BTreeSet::new();
        let mut new_protections = None;
        let mut bytes = 0usize;
        let mut completion_bytes = 0usize;
        for (key, candidate) in pending.iter() {
            let (completion, revision, protections) = if let Some(existing) =
                self.active_completion::<FileCompletion>(&view, key).await?
            {
                if existing.source != candidate.source || existing.rows != candidate.rows {
                    return Err(invalid_group());
                }
                (
                    existing,
                    rows.revision(key).ok_or_else(invalid_group)?,
                    self.retained_protections(&view, key).await?.map(Arc::new),
                )
            } else {
                // New files share one acquired scope; serialized bindings remain
                // separate copies and count against the combined commit bound.
                if new_keys.is_empty() {
                    new_protections = source_protections
                        .map(ReferenceProtections::duplicate)
                        .transpose()?
                        .map(Arc::new);
                }
                let protections = new_protections.clone();
                let records = completion_records(
                    request.dataset(),
                    &view,
                    key,
                    candidate,
                    protections.as_deref(),
                )?;
                bytes = bytes.saturating_add(inputs::metadata_size(
                    &records,
                    lifecycle
                        .budget
                        .limits
                        .max_metadata_bytes
                        .saturating_sub(bytes),
                )?);
                changes.extend(records);
                new_keys.insert(key.clone());
                (candidate.clone(), view.revision, protections)
            };
            completion_bytes = completion_bytes.saturating_add(inputs::metadata_size(
                &(key, &completion),
                lifecycle
                    .budget
                    .limits
                    .max_metadata_bytes
                    .saturating_sub(completion_bytes),
            )?);
            acknowledged.push(AcknowledgedFile {
                completion,
                generation: RetainedGeneration {
                    key: key.clone(),
                    revision,
                },
                protections,
            });
        }
        if !changes.is_empty() {
            let committed = lifecycle.commit(&view, changes).await?;
            for file in &mut acknowledged {
                if new_keys.contains(&file.generation.key) {
                    file.generation.revision = committed.revision;
                }
            }
        }
        pending.clear();
        Ok(acknowledged)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "File execution binds its protected directory alongside scan, source and output"
    )]
    async fn compute_file(
        &self,
        dataset: &Id,
        directory: &locations::Location,
        scan: &storage::OccurrenceScan,
        source: &ObjectRef,
        rows: u64,
        output: &iceberg::table::Table,
        expressions: Arc<Expressions>,
    ) -> Result<FileCompletion> {
        #[cfg(feature = "test-util")]
        work_started("backfill-file");
        let budget = &self.inner.lifecycle.budget;
        let required_disk = if self.inner.retained.is_local() {
            source
                .bytes
                .saturating_mul(2)
                .saturating_add(budget.limits.scratch_bytes)
        } else {
            budget.limits.scratch_bytes
        };
        if fs2::available_space(&self.inner.root)? < required_disk {
            return Err(Error::refusal(
                "disk_capacity",
                "computed output and scratch do not fit available disk",
            ));
        }
        let _execution = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
        let limit = budget.limits.max_batch_bytes;
        let (stream, projected) =
            scan.read_scalar_file(&source.path, budget, &expressions.projection)?;
        let stream = stream.map(move |batch| {
            let batch = batch?;
            // The conservative fallback checks the full native batch first.
            // Physical expressions still require their compiled column order.
            let batch = if projected {
                batch
            } else {
                batch.project(&expressions.projection)?
            };
            expressions.evaluate(batch, limit)
        });
        let files = storage::write_batches(output, stream, budget).await?;
        if files.iter().map(DataFile::record_count).sum::<u64>() != rows {
            return Err(invalid_group());
        }
        let descriptor_location =
            directory.child(&format!("{}.avro", uuid::Uuid::new_v4().simple()))?;
        let mut bytes = Vec::new();
        iceberg::spec::write_data_files_to_avro(
            &mut bytes,
            files.clone(),
            &StructType::new(vec![]),
            FormatVersion::V2,
        )?;
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "file completion descriptor exceeds metadata allowance",
            ));
        }
        let descriptor = ObjectRef {
            owner: dataset.clone(),
            path: descriptor_location.as_string()?,
            digest: blake3::hash(&bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        };
        self.inner
            .objects
            .create_candidate_metadata(&descriptor, bytes, budget, false)
            .await?;
        let mut outputs = Vec::with_capacity(files.len());
        for file in &files {
            outputs.push(
                self.inner
                    .objects
                    .identify(dataset, file.file_path(), budget)
                    .await?,
            );
        }
        Ok(FileCompletion {
            execution: None,
            source_metadata: None,
            source: source.clone(),
            rows,
            descriptor,
            outputs,
        })
    }
}

pub(super) fn check_group_dependencies(
    key: &str,
    dependencies: Option<&GroupDependencies>,
    source: &TablePin,
    output: &TablePin,
) -> Result<()> {
    // The key binds the logical source. Retain the exact consumed snapshot for
    // recovery, and require its captured logical proof before crossing layouts.
    if let Some(dependencies) = dependencies {
        let same_source = if dependencies.source.metadata == source.metadata {
            dependencies.source.snapshot == source.snapshot
                && dependencies.source.schema_id == source.schema_id
                && dependencies
                    .source_logical_metadata
                    .as_ref()
                    .is_none_or(|logical| logical == source.logical_metadata())
        } else {
            dependencies
                .source_logical_metadata
                .as_ref()
                .unwrap_or(&dependencies.source.metadata)
                == source.logical_metadata()
        };
        if !same_source
            || dependencies.output.metadata != output.metadata
            || dependencies.output.snapshot != output.snapshot
            || dependencies.output.schema_id != output.schema_id
        {
            return Err(generation_conflict(key));
        }
    }
    Ok(())
}

fn check_validation(
    key: &str,
    completion: &ValidationCompletion,
    source: &TablePin,
    output: &TablePin,
    origins: Option<&ObjectRef>,
) -> Result<()> {
    if completion.validator.as_deref() != Some(VALIDATOR)
        || completion.output != output.metadata
        || completion.origins.as_ref() != origins
    {
        return Err(generation_conflict(key));
    }
    check_group_dependencies(key, completion.dependencies.as_ref(), source, output)
}

fn generation_conflict(key: &str) -> Error {
    Error::refusal(
        "phase_generation_conflict",
        format!("active completion {key} binds a different source or output generation"),
    )
}

/// Keeps validator changes out of material preparation and per-file execution keys.
pub(super) fn scalar_validation_key(
    eligibility: u64,
    source: &TablePin,
    definition: &GroupDefinition,
    input_fields: &BTreeMap<String, i32>,
) -> Result<String> {
    let digest = storage::json_digest(&(
        VALIDATOR,
        PROCESSOR,
        source.logical_metadata(),
        definition,
        input_fields,
    ))?;
    Ok(format!(
        "backfill-validation/{eligibility}/{}",
        digest.to_hex()
    ))
}
