//! Recoverable row-local field groups, selected against an exact source boundary.

use super::*;
use iceberg::spec::{DataFile, FormatVersion, StructType};
use std::collections::BTreeSet;

mod expressions;
mod verification;
use expressions::Expressions;
pub(super) use verification::verify_groups;

// Batches acknowledgments without retaining payloads or creating one lifecycle
// transaction per row/file. A crash may repeat only the unacknowledged bundle.
const COMPLETION_BUNDLE: usize = 16;
const PROCESSOR: &str = "docspec.scalar-backfill.v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Capture {
    admission_fence: u64,
    target: Option<ObjectRef>,
    base_manifest: ObjectRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileCompletion {
    source: ObjectRef,
    rows: u64,
    descriptor: ObjectRef,
    outputs: Vec<ObjectRef>,
}

/// Progress against a captured source; completed files remain usable after stale promotion.
#[derive(Clone, Debug, Serialize)]
pub struct BackfillPlan {
    pub completed: bool,
    pub files_completed: usize,
    pub files_pending: usize,
    pub rows: u64,
    pub failures: BTreeMap<String, String>,
    pub promotion_refusal: Option<String>,
}

pub(super) fn validate(spec: &BackfillSpec) -> Result<()> {
    let ids: BTreeSet<_> = spec.fields.iter().map(|field| field.id).collect();
    let names: BTreeSet<_> = spec
        .fields
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
    if spec.operation == spec.base
        || spec.group.as_str() == "source"
        || spec.fields.is_empty()
        || spec.definition.trim().is_empty()
        || spec.group_definition.trim().is_empty()
        || ids.len() != spec.fields.len()
        || names.len() != spec.fields.len()
        || spec.fields.iter().any(|field| {
            field.id < 1
                || field.name.trim().is_empty()
                || field.name.to_ascii_lowercase().starts_with("__docspec_")
                || field.expression.trim().is_empty()
        })
    {
        return Err(Error::refusal(
            "invalid_backfill",
            "backfill needs a distinct operation, named group/definition, and distinct fields with positive IDs",
        ));
    }
    Ok(())
}

impl Engine {
    /// Streams source values with their exactly covered computed fields.
    ///
    /// # Errors
    /// Refuses invalid pins or coverage, conflicting names, or exhausted capacity.
    pub async fn read_combined(
        &self,
        release: &ReleasePin,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let budget = &self.inner.lifecycle.budget;
        verify_release_with_budget(release, budget).await?;
        let source = storage::open_pin(&release.dataset.groups["source"]).await?;
        let context = self.context();
        let mut columns = source
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| format!("s.{}", quote(&field.name)))
            .collect::<Vec<_>>();
        context.register_table("source", crate::query::occurrences(source, budget.clone())?)?;
        let mut from = "source s".to_owned();
        for (index, (name, binding)) in release.dataset.computed.iter().enumerate() {
            let alias = format!("g{index}");
            let native = storage::open_pin(&release.dataset.groups[name]).await?;
            context.register_table(&alias, crate::query::table(native, budget.clone())?)?;
            from.push_str(&format!(
                " INNER JOIN {alias} ON s.{} = {alias}.{} AND s.{} = {alias}.{}",
                quote(storage::OCCURRENCE_FILE),
                quote(storage::OCCURRENCE_FILE),
                quote(storage::OCCURRENCE_ROW),
                quote(storage::OCCURRENCE_ROW)
            ));
            columns.extend(
                binding
                    .request
                    .fields
                    .iter()
                    .map(|field| format!("{alias}.{}", quote(&field.name))),
            );
        }
        crate::query::bounded(
            context
                .sql(&format!("SELECT {} FROM {from}", columns.join(", ")))
                .await?
                .execute_stream()
                .await?,
            budget,
        )
    }

    /// Plans file reuse without opening source payloads.
    ///
    /// # Errors
    /// Refuses unknown bases, conflicting retry identities, or invalid definitions.
    pub async fn plan_backfill(&self, spec: &BackfillSpec) -> Result<BackfillPlan> {
        validate(spec)?;
        let request = fingerprint(&OperationRequest::Backfill(spec.clone()))?;
        let base = self.release(&spec.dataset, &spec.base).await?;
        let native = storage::open_pin(&base.dataset.groups["source"]).await?;
        Expressions::compile(&self.context(), native.metadata().current_schema(), spec)?;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        let old: Option<Operation> = lifecycle
            .get(&view, &format!("operation/{}", spec.operation))
            .await?;
        if old
            .as_ref()
            .is_some_and(|old| old.request_digest != request)
        {
            return Err(conflicting_operation());
        }
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        let eligibility = old
            .as_ref()
            .map_or(authority.eligibility, |old| old.eligibility);
        let scan = storage::OccurrenceScan::open(&native, &lifecycle.budget).await?;
        let mut plan = BackfillPlan {
            completed: lifecycle
                .get::<ReleasePin>(&view, &format!("result/{}", spec.operation))
                .await?
                .is_some(),
            files_completed: 0,
            files_pending: 0,
            rows: base.dataset.groups["source"].rows,
            failures: BTreeMap::new(),
            promotion_refusal: None,
        };
        if !plan.completed {
            let operation = old.clone().unwrap_or(Operation {
                request_digest: request,
                eligibility,
                epoch: authority.epoch,
                boundary: base.dataset.boundary,
                definition: spec.definition.clone(),
                base: Some(spec.base.clone()),
            });
            let capture = lifecycle
                .get::<Capture>(&view, &format!("backfill/{}", spec.operation))
                .await?
                .unwrap_or(Capture {
                    admission_fence: authority.boundary,
                    target: base
                        .dataset
                        .groups
                        .get(spec.group.as_str())
                        .map(|pin| pin.metadata.clone()),
                    base_manifest: base.manifest.clone(),
                });
            plan.promotion_refusal =
                check_definition(&authority, &OperationRequest::Backfill(spec.clone()))
                    .and_then(|()| {
                        promotion(
                            &authority,
                            &operation,
                            &capture,
                            &base.dataset.groups["source"],
                            spec,
                        )
                    })
                    .err()
                    .map(|error| error.code().to_owned());
        }
        let objects = source_objects(&base.dataset.groups["source"]);
        for file in scan.files() {
            let object = objects.get(file.file_path()).ok_or_else(invalid_group)?;
            let key = file_key(
                spec,
                object,
                native.metadata().current_schema(),
                eligibility,
            )?;
            if lifecycle
                .get::<FileCompletion>(&view, &key)
                .await?
                .is_some()
            {
                plan.files_completed += 1;
            } else {
                plan.files_pending += 1;
                if let Some(error) = lifecycle
                    .get::<String>(&view, &format!("failure/{}/{key}", spec.operation))
                    .await?
                {
                    plan.failures.insert(object.path.clone(), error);
                }
            }
        }
        Ok(plan)
    }

    /// Computes, verifies, and selects one independently stored field group.
    ///
    /// # Errors
    /// Refuses stale source/target/definition selection while retaining completed files.
    pub async fn backfill(&self, spec: &BackfillSpec) -> Result<ReleasePin> {
        self.backfill_internal(spec, None).await
    }

    /// Interrupts after durable backfill work for recovery qualification.
    ///
    /// # Errors
    /// Returns `injected_failure` after files, processing, validation, readback, or selection.
    #[cfg(feature = "test-util")]
    pub async fn backfill_until(&self, spec: &BackfillSpec, phase: &str) -> Result<ReleasePin> {
        self.backfill_internal(spec, Some(phase)).await
    }

    async fn backfill_internal(
        &self,
        spec: &BackfillSpec,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        validate(spec)?;
        let digest = fingerprint(&OperationRequest::Backfill(spec.clone()))?;
        // Native preparation has a large async state. Keep it on the heap rather
        // than nesting another copy in the reconciliation future's stack frame.
        let result = Box::pin(self.prepare_backfill(spec, stop)).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code() != "injected_failure")
        {
            let lock = self.lock(&spec.dataset).await;
            let _guard = lock.lock().await;
            let lifecycle = &self.inner.lifecycle;
            let view = lifecycle.open(&spec.dataset).await?;
            let operation: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation))
                .await?;
            if operation.is_some_and(|operation| operation.request_digest == digest)
                && let Some(completed) = lifecycle
                    .get(&view, &format!("result/{}", spec.operation))
                    .await?
            {
                return Ok(completed);
            }
        }
        result
    }

    async fn prepare_backfill(
        &self,
        spec: &BackfillSpec,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        validate(spec)?;
        let request = OperationRequest::Backfill(spec.clone());
        let digest = fingerprint(&request)?;
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&spec.dataset).await;
        let (operation, capture, base) = {
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            let old: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation))
                .await?;
            if old.as_ref().is_some_and(|old| old.request_digest != digest) {
                return Err(conflicting_operation());
            }
            if let Some(result) = lifecycle
                .get(&view, &format!("result/{}", spec.operation))
                .await?
            {
                return Ok(result);
            }
            let base: ReleasePin = lifecycle
                .get(&view, &format!("result/{}", spec.base))
                .await?
                .ok_or_else(|| {
                    Error::refusal("no_release", "backfill source version is unavailable")
                })?;
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            check_definition(&authority, &request)?;
            if old.is_none() && base.dataset.eligibility != authority.eligibility {
                return Err(Error::refusal(
                    "stale_eligibility",
                    "captured source selection requires revalidation under current eligibility",
                ));
            }
            if let Some(operation) = old {
                check_preparation(&authority, &operation)?;
                let capture: Capture = lifecycle
                    .get(&view, &format!("backfill/{}", spec.operation))
                    .await?
                    .ok_or_else(invalid_group)?;
                (operation, capture, base)
            } else {
                let operation = Operation {
                    request_digest: digest.clone(),
                    eligibility: authority.eligibility,
                    epoch: authority.epoch,
                    boundary: base.dataset.boundary,
                    definition: spec.definition.clone(),
                    base: Some(spec.base.clone()),
                };
                let capture = Capture {
                    admission_fence: authority.boundary,
                    target: base
                        .dataset
                        .groups
                        .get(spec.group.as_str())
                        .map(|pin| pin.metadata.clone()),
                    base_manifest: base.manifest.clone(),
                };
                lifecycle.commit(&view, vec![record(format!("operation/{}", spec.operation), &operation)?,
                    record(format!("request/{}", spec.operation), &request)?, record(format!("backfill/{}", spec.operation), &capture)?,
                    record(format!("runtime/{}", spec.operation), serde_json::json!({"processor": PROCESSOR, "dependency_lock_blake3": blake3::hash(include_bytes!("../../Cargo.lock")).to_hex().as_str(), "limits": lifecycle.budget.limits}))?]).await?;
                (operation, capture, base)
            }
        };
        let source = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(invalid_group)?;
        // Resolve durable computation before opening source payloads. Fresh file work
        // verifies its source bytes; final readback verifies the complete selection.
        if base.manifest.bytes > lifecycle.budget.limits.max_metadata_bytes as u64 {
            return Err(Error::refusal(
                "metadata_capacity",
                "source manifest exceeds metadata allowance",
            ));
        }
        storage::verify_objects(vec![base.manifest.clone()], &lifecycle.budget).await?;
        let decoded: DatasetPin =
            serde_json::from_reader(std::fs::File::open(&base.manifest.path)?)?;
        if decoded != base.dataset {
            return Err(invalid_group());
        }
        let native = storage::open_pin(source).await?;
        let expressions = Arc::new(Expressions::compile(
            &self.context(),
            native.metadata().current_schema(),
            spec,
        )?);
        let prepared_key = format!("backfill-prepared/{}/{digest}", operation.eligibility);
        let prepared: Option<TablePin> = lifecycle
            .get(&lifecycle.open(&spec.dataset).await?, &prepared_key)
            .await?;
        let prepared = if let Some(prepared) = prepared {
            storage::check_available(prepared.objects.clone(), &lifecycle.budget).await?;
            prepared
        } else {
            let directory = self
                .reserve_backfill(spec, &operation, "processing")
                .await?;
            let output = self
                .create_output(&spec.dataset, &directory, expressions.native.clone())
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
            let mut completion_view = lifecycle.open(&spec.dataset).await?;
            for file in scan.files() {
                let object = objects.get(file.file_path()).ok_or_else(invalid_group)?;
                let key = file_key(
                    spec,
                    object,
                    native.metadata().current_schema(),
                    operation.eligibility,
                )?;
                let existing: Option<FileCompletion> =
                    lifecycle.get(&completion_view, &key).await?;
                let completion = if let Some(existing) = existing {
                    existing
                } else {
                    let result = self
                        .compute_file(
                            spec,
                            &scan,
                            object,
                            file.record_count(),
                            &output,
                            expressions.clone(),
                        )
                        .await;
                    let completion = match result {
                        Ok(completion) => completion,
                        Err(error) => {
                            self.acknowledge_files(spec, &operation, &mut completions)
                                .await?;
                            let _guard = lock.lock().await;
                            let view = lifecycle.open(&spec.dataset).await?;
                            lifecycle
                                .commit(
                                    &view,
                                    vec![record(
                                        format!("failure/{}/{key}", spec.operation),
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
                        self.acknowledge_files(spec, &operation, &mut completions)
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
                    self.acknowledge_files(spec, &operation, &mut completions)
                        .await?;
                    stop_after(stop, "files")?;
                    completion_view = lifecycle.open(&spec.dataset).await?;
                }
            }
            self.acknowledge_files(spec, &operation, &mut completions)
                .await?;
            stop_after(stop, "files")?;
            let output = storage::append(&output, lifecycle.catalog.as_ref(), files).await?;
            let prepared = storage::pin(
                &spec.dataset,
                &output,
                source.rows,
                &lifecycle.budget,
                &inherited,
            )
            .await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            check_preparation(&authority, &operation)?;
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
        let binding = ComputedGroup {
            request: spec.clone(),
            native_field_ids: expressions.native_field_ids.clone(),
            source_metadata: source.metadata.clone(),
            base_manifest: capture.base_manifest.clone(),
        };
        let validation_key = format!("backfill-validation/{}/{digest}", operation.eligibility);
        let validated: Option<ObjectRef> = lifecycle
            .get(&lifecycle.open(&spec.dataset).await?, &validation_key)
            .await?;
        if validated.as_ref() != Some(&prepared.metadata) {
            verification::verify_group(source, &prepared, &binding, &lifecycle.budget).await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            check_preparation(&authority, &operation)?;
            lifecycle
                .commit(&view, vec![record(&validation_key, &prepared.metadata)?])
                .await?;
        }
        stop_after(stop, "validation")?;
        // Only composition is retried after a compatible sibling selection. Expensive
        // file work is retained; no stale whole-dataset manifest can win the retry.
        loop {
            let current = {
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                if let Some(completed) = lifecycle
                    .get(&view, &format!("result/{}", spec.operation))
                    .await?
                {
                    return Ok(completed);
                }
                let authority: Authority =
                    lifecycle.get(&view, "authority").await?.unwrap_or_default();
                promotion(&authority, &operation, &capture, source, spec)?;
                authority.selected.ok_or_else(invalid_group)?
            };
            let mut dataset = current.clone();
            dataset.version = spec.operation.clone();
            dataset.request = request.clone();
            dataset.changes = None;
            dataset.processor = PROCESSOR.into();
            dataset
                .groups
                .insert(spec.group.to_string(), prepared.clone());
            dataset
                .computed
                .insert(spec.group.to_string(), binding.clone());
            check_names(&dataset)?;
            let serialized = serde_json::to_vec_pretty(&dataset)?;
            if serialized.len() > lifecycle.budget.limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "combined release exceeds metadata allowance",
                ));
            }
            let readback_key = format!(
                "backfill-readback/{}/{}",
                spec.operation,
                blake3::hash(&serialized).to_hex()
            );
            let recorded: Option<ReleasePin> = lifecycle
                .get(&lifecycle.open(&spec.dataset).await?, &readback_key)
                .await?;
            let release = if let Some(recorded) = recorded {
                if recorded.dataset != dataset {
                    return Err(invalid_group());
                }
                let mut objects = vec![recorded.manifest.clone()];
                objects.extend(recorded.dataset.inputs.values().cloned());
                objects.extend(
                    recorded
                        .dataset
                        .groups
                        .values()
                        .flat_map(|pin| pin.objects.iter().cloned()),
                );
                objects.extend(
                    recorded
                        .dataset
                        .computed
                        .values()
                        .map(|group| group.base_manifest.clone()),
                );
                storage::check_available(objects, &lifecycle.budget).await?;
                recorded
            } else {
                let directory = self.reserve_backfill(spec, &operation, "release").await?;
                let manifest_path = directory.join("release.json");
                storage::write_new(&manifest_path, &serialized)?;
                let release = ReleasePin {
                    id: spec.operation.clone(),
                    dataset,
                    manifest: storage::object(&spec.dataset, &manifest_path)?,
                    verified: true,
                };
                verify_release_with_budget(&release, &lifecycle.budget).await?;
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                let authority: Authority =
                    lifecycle.get(&view, "authority").await?.unwrap_or_default();
                check_preparation(&authority, &operation)?;
                if let Some(recorded) = lifecycle.get::<ReleasePin>(&view, &readback_key).await? {
                    recorded
                } else {
                    lifecycle
                        .commit(&view, vec![record(&readback_key, &release)?])
                        .await?;
                    release
                }
            };
            stop_after(stop, "readback")?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation))
                .await?
            {
                return Ok(completed);
            }
            let mut authority: Authority =
                lifecycle.get(&view, "authority").await?.unwrap_or_default();
            promotion(&authority, &operation, &capture, source, spec)?;
            if authority.selected.as_ref() != Some(&current) {
                continue;
            }
            authority.selected = Some(release.dataset.clone());
            authority.release = Some(release.clone());
            lifecycle
                .commit(
                    &view,
                    vec![
                        record("authority", authority)?,
                        record(format!("result/{}", spec.operation), &release)?,
                    ],
                )
                .await?;
            stop_after(stop, "selection")?;
            return Ok(release);
        }
    }

    async fn reserve_backfill(
        &self,
        spec: &BackfillSpec,
        operation: &Operation,
        purpose: &str,
    ) -> Result<PathBuf> {
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .root
            .join("objects")
            .join(spec.dataset.as_str())
            .join(id.as_str());
        let lock = self.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        check_preparation(&authority, operation)?;
        lifecycle
            .commit(
                &view,
                vec![record(
                    format!("protection/{id}"),
                    Protection {
                        id,
                        consumer: spec.dataset.clone(),
                        purpose: format!("{}/{purpose}", spec.operation),
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
        spec: &BackfillSpec,
        operation: &Operation,
        pending: &mut BTreeMap<String, FileCompletion>,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let lock = self.lock(&spec.dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        check_preparation(&authority, operation)?;
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
        spec: &BackfillSpec,
        scan: &storage::OccurrenceScan,
        source: &ObjectRef,
        rows: u64,
        output: &iceberg::table::Table,
        expressions: Arc<Expressions>,
    ) -> Result<FileCompletion> {
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
        storage::verify_objects(vec![source.clone()], budget).await?;
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
        let owner = spec.dataset.clone();
        let outputs = budget
            .blocking(move || {
                files
                    .iter()
                    .map(|file| storage::object(&owner, Path::new(file.file_path())))
                    .collect()
            })
            .await?;
        Ok(FileCompletion {
            source: source.clone(),
            rows,
            descriptor: storage::object(&spec.dataset, &descriptor_path)?,
            outputs,
        })
    }
}

fn check_preparation(authority: &Authority, operation: &Operation) -> Result<()> {
    if authority.epoch != operation.epoch {
        return Err(Error::refusal(
            "stale_eligibility",
            "execution authority changed",
        ));
    }
    Ok(())
}

fn promotion(
    authority: &Authority,
    operation: &Operation,
    capture: &Capture,
    source: &TablePin,
    spec: &BackfillSpec,
) -> Result<()> {
    check_preparation(authority, operation)?;
    if authority.eligibility != operation.eligibility
        || authority.definition != operation.definition
    {
        return Err(Error::refusal(
            "stale_eligibility",
            "backfill definition or dependency eligibility changed",
        ));
    }
    let current = authority.selected.as_ref().ok_or_else(invalid_group)?;
    if current.eligibility != operation.eligibility {
        return Err(Error::refusal(
            "stale_eligibility",
            "selected source or sibling groups require revalidation",
        ));
    }
    if current.groups.get("source") != Some(source)
        || current.boundary != operation.boundary
        || authority.boundary != capture.admission_fence
        || authority.boundary != current.boundary
    {
        return Err(Error::refusal(
            "stale_source_boundary",
            "source changed or admitted work remains beyond the captured selection; catch up in a new operation",
        ));
    }
    if current
        .groups
        .get(spec.group.as_str())
        .map(|pin| &pin.metadata)
        != capture.target.as_ref()
    {
        return Err(Error::refusal(
            "stale_group",
            "target field group changed; completed computation remains retained",
        ));
    }
    Ok(())
}

fn source_objects(source: &TablePin) -> BTreeMap<&str, &ObjectRef> {
    source
        .objects
        .iter()
        .map(|object| (object.path.as_str(), object))
        .collect()
}

fn file_key(
    spec: &BackfillSpec,
    source: &ObjectRef,
    schema: &iceberg::spec::Schema,
    eligibility: u64,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        PROCESSOR,
        source,
        schema,
        &spec.group,
        &spec.group_definition,
        &spec.fields,
    ))?;
    Ok(format!(
        "phase/backfill/{eligibility}/{}",
        blake3::hash(&bytes).to_hex()
    ))
}

async fn read_completion(
    completion: &FileCompletion,
    schema: &iceberg::spec::Schema,
    budget: &Budget,
) -> Result<Vec<DataFile>> {
    if completion.descriptor.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "file descriptor exceeds reader allowance",
        ));
    }
    let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
    let mut objects = completion.outputs.clone();
    objects.push(completion.descriptor.clone());
    storage::verify_objects(objects, budget).await?;
    let files = iceberg::spec::read_data_files_from_avro(
        &mut std::fs::File::open(&completion.descriptor.path)?,
        schema,
        0,
        &StructType::new(vec![]),
        FormatVersion::V2,
    )?;
    let known: BTreeMap<_, _> = completion
        .outputs
        .iter()
        .map(|object| (object.path.as_str(), object.bytes))
        .collect();
    let paths: BTreeSet<_> = files.iter().map(DataFile::file_path).collect();
    if paths.len() != files.len()
        || paths.len() != known.len()
        || files
            .iter()
            .any(|file| known.get(file.file_path()) != Some(&file.file_size_in_bytes()))
        || files.iter().map(DataFile::record_count).sum::<u64>() != completion.rows
    {
        return Err(invalid_group());
    }
    Ok(files)
}

fn invalid_group() -> Error {
    Error::refusal(
        "invalid_group",
        "computed group identity, schema or coverage differs from its captured source",
    )
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn check_names(dataset: &DatasetPin) -> Result<()> {
    let mut names = BTreeSet::new();
    for group in dataset.computed.values() {
        for field in &group.request.fields {
            if !names.insert(field.name.to_ascii_lowercase()) {
                return Err(Error::refusal(
                    "field_conflict",
                    "independent groups use the same display name",
                ));
            }
        }
    }
    Ok(())
}

fn charge_assembly(bytes: usize, completion: &FileCompletion, limit: usize) -> Result<usize> {
    let bytes = bytes
        .saturating_add(completion.descriptor.bytes as usize)
        .saturating_add(serde_json::to_vec(&completion.outputs)?.len())
        .saturating_add(completion.outputs.len().saturating_mul(512));
    if bytes > limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "assembled group descriptors exceed metadata allowance",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aggregate_completion_descriptors_cannot_exceed_assembly_allowance() {
        let object = ObjectRef {
            owner: Id::new("test").unwrap(),
            path: "descriptor".into(),
            digest: "a".repeat(64),
            bytes: 600,
        };
        let completion = FileCompletion {
            source: object.clone(),
            rows: 0,
            descriptor: object,
            outputs: vec![],
        };
        let admitted = charge_assembly(0, &completion, 1024).unwrap();
        assert!(admitted < 1024);
        assert_eq!(
            charge_assembly(admitted, &completion, 1024)
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
    }
}
