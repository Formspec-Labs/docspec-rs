//! Shared retained scalar preparation over selected or candidate source snapshots.

use super::*;

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
        definition: &GroupDefinition,
        input_fields: Option<&BTreeMap<String, i32>>,
        stop: Option<&str>,
        checks: &mut super::super::verification::Checks,
    ) -> Result<PreparedGroup> {
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(request.dataset()).await;
        let view = lifecycle.open(request.dataset()).await?;
        self.check_not_abandoned(&view, request.operation()).await?;
        let native = storage::open_pin(source).await?;
        let expressions = Arc::new(Expressions::compile(
            &self.context(),
            native.metadata().current_schema(),
            definition,
            input_fields,
        )?);
        let digest = blake3::hash(&serde_json::to_vec(&(
            PROCESSOR,
            &source.metadata,
            definition,
            &expressions.input_fields,
        ))?)
        .to_hex()
        .to_string();
        let prepared_key = format!("backfill-prepared/{}/{digest}", operation.eligibility);
        let prepared: Option<TablePin> = lifecycle.get(&view, &prepared_key).await?;
        let prepared = if let Some(prepared) = prepared {
            storage::check_available(prepared.objects.clone(), &lifecycle.budget).await?;
            prepared
        } else {
            #[cfg(feature = "test-util")]
            work_started("backfill-assembly");
            let directory = self.reserve_group(request, operation, "processing").await?;
            let output = self
                .create_output(request.dataset(), &directory, expressions.native.clone())
                .await?;
            let scan = storage::OccurrenceScan::open(&native, &lifecycle.budget).await?;
            let objects = source_objects(source);
            let mut completions = BTreeMap::new();
            let mut files = Vec::new();
            let mut inherited = Vec::new();
            let _assembly = lifecycle
                .budget
                .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(2))?;
            let mut assembly_bytes = 0usize;
            let mut completion_view = lifecycle.open(request.dataset()).await?;
            for file in scan.files() {
                let object = objects.get(file.file_path()).ok_or_else(invalid_group)?;
                let key = file_key(object, &expressions, operation.eligibility)?;
                let existing: Option<FileCompletion> =
                    lifecycle.get(&completion_view, &key).await?;
                let completion = if let Some(existing) = existing {
                    existing
                } else {
                    let result = self
                        .compute_file(
                            request.dataset(),
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
                            completion
                        }
                        Err(error) => {
                            self.acknowledge_files(request, operation, &mut completions)
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
                    completions.insert(key, completion.clone());
                    completion
                };
                if completion.source != **object || completion.rows != file.record_count() {
                    return Err(invalid_group());
                }
                match charge_assembly(
                    assembly_bytes,
                    &completion,
                    lifecycle.budget.limits.max_metadata_bytes,
                ) {
                    Ok(bytes) => assembly_bytes = bytes,
                    Err(error) => {
                        self.acknowledge_files(request, operation, &mut completions)
                            .await?;
                        return Err(error);
                    }
                }
                files.extend(
                    read_completion(&completion, &expressions.native, &lifecycle.budget).await?,
                );
                inherited.extend(completion.outputs.clone());
                let bytes = serde_json::to_vec(&completions)?.len();
                if completions.len() >= COMPLETION_BUNDLE
                    || bytes >= lifecycle.budget.limits.max_metadata_bytes / 8
                {
                    self.acknowledge_files(request, operation, &mut completions)
                        .await?;
                    stop_after(stop, "files")?;
                    completion_view = lifecycle.open(request.dataset()).await?;
                }
            }
            self.acknowledge_files(request, operation, &mut completions)
                .await?;
            stop_after(stop, "files")?;
            let output = storage::append(&output, lifecycle.catalog.as_ref(), files).await?;
            let prepared = storage::pin(
                request.dataset(),
                &output,
                source.rows,
                &lifecycle.budget,
                &inherited,
            )
            .await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(request.dataset()).await?;
            self.check_completion(&view, operation, request.operation())
                .await?;
            if let Some(existing) = lifecycle.get::<TablePin>(&view, &prepared_key).await? {
                existing
            } else {
                lifecycle
                    .commit(&view, vec![record(&prepared_key, &prepared)?])
                    .await?;
                prepared
            }
        };
        stop_after(stop, "processing")?;
        let validation_key = format!("backfill-validation/{}/{digest}", operation.eligibility);
        let validated: Option<ObjectRef> = lifecycle
            .get(&lifecycle.open(request.dataset()).await?, &validation_key)
            .await?;
        if validated.as_ref() != Some(&prepared.metadata) {
            #[cfg(feature = "test-util")]
            work_started("backfill-validation");
            verification::verify_group_data(
                source,
                &prepared,
                definition,
                &expressions.input_fields,
                &expressions.native_field_ids,
                &lifecycle.budget,
                checks,
            )
            .await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(request.dataset()).await?;
            self.check_completion(&view, operation, request.operation())
                .await?;
            lifecycle
                .commit(&view, vec![record(&validation_key, &prepared.metadata)?])
                .await?;
        }
        stop_after(stop, "validation")?;
        Ok(PreparedGroup {
            table: prepared,
            native_field_ids: expressions.native_field_ids.clone(),
            input_fields: expressions.input_fields.clone(),
        })
    }

    pub(super) async fn reserve_group(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        purpose: &str,
    ) -> Result<PathBuf> {
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .root
            .join("objects")
            .join(request.dataset().as_str())
            .join(id.as_str());
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
                        paths: vec![format!("{}/", directory.display())],
                        retired: false,
                    },
                )?],
            )
            .await?;
        std::fs::create_dir_all(&directory)?;
        Ok(directory)
    }

    async fn acknowledge_files(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        pending: &mut BTreeMap<String, FileCompletion>,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let lock = self.lock(request.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(request.dataset()).await?;
        self.check_completion(&view, operation, request.operation())
            .await?;
        let mut changes = Vec::new();
        for (key, completion) in pending.iter() {
            if lifecycle.get::<FileCompletion>(&view, key).await?.is_none() {
                changes.push(record(key, completion)?);
            }
        }
        if !changes.is_empty() {
            lifecycle.commit(&view, changes).await?;
        }
        pending.clear();
        Ok(())
    }

    async fn compute_file(
        &self,
        dataset: &Id,
        scan: &storage::OccurrenceScan,
        source: &ObjectRef,
        rows: u64,
        output: &iceberg::table::Table,
        expressions: Arc<Expressions>,
    ) -> Result<FileCompletion> {
        #[cfg(feature = "test-util")]
        work_started("backfill-file");
        let budget = &self.inner.lifecycle.budget;
        if fs2::available_space(&self.inner.root)?
            < source
                .bytes
                .saturating_mul(2)
                .saturating_add(budget.limits.scratch_bytes)
        {
            return Err(Error::refusal(
                "disk_capacity",
                "computed output and scratch do not fit available disk",
            ));
        }
        let _execution = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
        let limit = budget.limits.max_batch_bytes;
        let stream = scan
            .read_file(&source.path, budget)?
            .map(move |batch| expressions.evaluate(batch?, limit));
        let files = storage::write_batches(output, stream, budget).await?;
        if files.iter().map(DataFile::record_count).sum::<u64>() != rows {
            return Err(invalid_group());
        }
        let descriptor_path = Path::new(output.metadata().location())
            .join(format!("{}.avro", uuid::Uuid::new_v4().simple()));
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
        storage::write_new(&descriptor_path, &bytes)?;
        let owner = dataset.clone();
        let outputs = budget
            .blocking(move || {
                files
                    .iter()
                    .map(|file| storage::object(&owner, Path::new(file.file_path())))
                    .collect()
            })
            .await?;
        Ok(FileCompletion {
            source_metadata: None,
            source: source.clone(),
            rows,
            descriptor: storage::object(dataset, &descriptor_path)?,
            outputs,
        })
    }
}
