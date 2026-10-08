//! Recoverable row-local field groups, selected against an exact source boundary.

use super::*;
use iceberg::spec::{DataFile, FormatVersion, StructType};
use std::collections::BTreeSet;

mod expressions;
mod preparation;
mod verification;
use expressions::Expressions;
pub(super) use verification::{verify_binding_dependencies, verify_groups};

// Batches acknowledgments without retaining payloads or creating one lifecycle
// transaction per row/file. A crash may repeat only the unacknowledged bundle.
const COMPLETION_BUNDLE: usize = 16;
const PROCESSOR: &str = "docspec.scalar-backfill.v2";

pub(super) struct PreparedGroup {
    pub table: TablePin,
    pub native_field_ids: BTreeMap<i32, i32>,
    pub input_fields: BTreeMap<String, i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Capture {
    admission_fence: u64,
    target: Option<ObjectRef>,
    target_binding: Option<ComputedGroup>,
    base_manifest: ObjectRef,
    related: Option<Box<RelatedResult>>,
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
    if spec.operation == spec.base || spec.definition.trim().is_empty() {
        return Err(Error::refusal(
            "invalid_backfill",
            "backfill needs a distinct operation and active source definition",
        ));
    }
    validate_definition(&spec.group_definition())
}

fn validate_definition(spec: &GroupDefinition) -> Result<()> {
    let ids: BTreeSet<_> = spec.fields.iter().map(|field| field.id).collect();
    let names: BTreeSet<_> = spec
        .fields
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
    if spec.group.as_str() == "source"
        || spec.fields.is_empty()
        || spec.definition.trim().is_empty()
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
            "computed groups need a definition and distinct fields with positive IDs",
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
        resolve_release(release, budget).await?;
        let source = storage::open_pin(&release.dataset.groups["source"]).await?;
        let context = self.context();
        let mut columns = source
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .filter(|field| {
                storage::defaults::supplied_id(&field.name).is_none()
                    && (release.dataset.related.is_none() || !refresh::hidden_field(&field.name))
            })
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
                    .definition
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
        let expressions = Expressions::compile(
            &self.context(),
            native.metadata().current_schema(),
            &spec.group_definition(),
            None,
        )?;
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
                base_manifest: None,
                schema_key: None,
                require_current_boundary: false,
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
                    target_binding: base.dataset.computed.get(spec.group.as_str()).cloned(),
                    base_manifest: base.manifest.clone(),
                    related: base.dataset.related.clone(),
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
            let key = file_key(object, &expressions, eligibility)?;
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
                    base_manifest: None,
                    schema_key: None,
                    require_current_boundary: false,
                };
                let capture = Capture {
                    admission_fence: authority.boundary,
                    target: base
                        .dataset
                        .groups
                        .get(spec.group.as_str())
                        .map(|pin| pin.metadata.clone()),
                    target_binding: base.dataset.computed.get(spec.group.as_str()).cloned(),
                    base_manifest: base.manifest.clone(),
                    related: base.dataset.related.clone(),
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
        let definition = spec.group_definition();
        let mut checks = super::verification::Checks::new(VerificationLevel::Routine);
        checks.trust(&base, &lifecycle.budget).await?;
        let prepared = Box::pin(self.prepare_group(
            &request,
            &operation,
            source,
            &definition,
            None,
            stop,
            &mut checks,
        ))
        .await?;
        let binding = ComputedGroup {
            definition,
            input_fields: prepared.input_fields,
            request: request.clone(),
            native_field_ids: prepared.native_field_ids,
            source_metadata: source.metadata.clone(),
            base_manifest: capture.base_manifest.clone(),
        };
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
                authority.release.ok_or_else(invalid_group)?
            };
            let mut dataset = current.dataset.clone();
            dataset.parent_manifest = Some(current.manifest.clone());
            dataset.version = spec.operation.clone();
            dataset.request = request.clone();
            dataset.changes = None;
            dataset.processor = PROCESSOR.into();
            dataset
                .groups
                .insert(spec.group.to_string(), prepared.table.clone());
            dataset
                .computed
                .insert(spec.group.to_string(), binding.clone());
            check_names(&dataset)?;
            verify_groups(
                &dataset,
                &lifecycle.budget,
                &mut super::verification::Checks::composition(),
            )
            .await?;
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
                check_release_available(&recorded, &lifecycle.budget).await?;
                recorded
            } else {
                #[cfg(feature = "test-util")]
                work_started("backfill-readback");
                let directory = self.reserve_group(&request, &operation, "release").await?;
                let manifest_path = directory.join("release.json");
                storage::write_new(&manifest_path, &serialized)?;
                let release = ReleasePin {
                    id: spec.operation.clone(),
                    dataset,
                    manifest: ObjectRef {
                        owner: spec.dataset.clone(),
                        path: manifest_path.to_string_lossy().into_owned(),
                        digest: blake3::hash(&serialized).to_hex().to_string(),
                        bytes: serialized.len() as u64,
                    },
                    verified: true,
                };
                let memory = Arc::new(
                    lifecycle
                        .budget
                        .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
                );
                let decoded: DatasetPin =
                    inputs::read_manifest(&release.manifest, &lifecycle.budget, &mut 0, &memory)
                        .await?;
                if decoded != release.dataset {
                    return Err(invalid_group());
                }
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
            let dependencies = if release.dataset.related.is_some() {
                Some(super::verification::available_graph(&release, &lifecycle.budget).await?)
            } else {
                None
            };
            let datasets = dependencies.as_ref().map_or_else(
                || BTreeSet::from([spec.dataset.clone()]),
                super::verification::DependencyGraph::datasets,
            );
            let _guards = self.lock_inputs(datasets).await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation))
                .await?
            {
                return Ok(completed);
            }
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            promotion(&authority, &operation, &capture, source, spec)?;
            if authority.release.as_ref() != Some(&current) {
                continue;
            }
            if let Some(dependencies) = &dependencies {
                Box::pin(self.guard_ancestors(dependencies, &release)).await?;
            }
            Box::pin(self.select(&view, authority, &release)).await?;
            stop_after(stop, "selection")?;
            return Ok(release);
        }
    }
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
        || current.related != capture.related
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
        || current.computed.get(spec.group.as_str()) != capture.target_binding.as_ref()
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

/// Full source reconstructions declare correspondence by the prior current name.
/// Updates retain exact IDs; they never reinterpret an old name under a new ID.
pub(super) async fn inherited_inputs(
    request: &OperationRequest,
    previous: &ComputedGroup,
    before: &TablePin,
    after: &TablePin,
) -> Result<BTreeMap<String, i32>> {
    let rebuilt = match request {
        OperationRequest::Build(_) => true,
        OperationRequest::Refresh(spec) => spec.mode == RefreshMode::Full,
        _ => false,
    };
    if !rebuilt {
        return Ok(previous.input_fields.clone());
    }
    let before = storage::open_pin(before).await?;
    let after = storage::open_pin(after).await?;
    previous
        .input_fields
        .iter()
        .map(|(alias, id)| {
            let old = before
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .find(|field| field.id == *id)
                .ok_or_else(expressions::invalid_binding)?;
            let new = after
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .find(|field| field.name == old.name)
                .ok_or_else(expressions::invalid_binding)?;
            Ok((alias.clone(), new.id))
        })
        .collect()
}

fn file_key(source: &ObjectRef, expressions: &Expressions, eligibility: u64) -> Result<String> {
    let bytes = serde_json::to_vec(&(source, &expressions.dependencies))?;
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
    storage::check_available(completion.outputs.clone(), budget).await?;
    storage::verify_objects(vec![completion.descriptor.clone()], budget).await?;
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
        for field in &group.definition.fields {
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

    #[tokio::test]
    async fn native_rename_reuses_file_completions_but_name_reuse_changes_input_meaning() {
        let root = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let dataset = Id::new("cache").unwrap();
        let prepared = engine
            .write_frame(
                &dataset,
                &root.path().join("source"),
                engine
                    .context()
                    .sql("SELECT CAST(1 AS BIGINT) AS old, CAST(100 AS BIGINT) AS spare")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let source = prepared.table;
        let native = storage::open_pin(&source).await.unwrap();
        let fields = native.metadata().current_schema().as_struct().fields();
        let old_id = fields[0].id;
        let spec = BackfillSpec {
            dataset: dataset.clone(),
            operation: Id::new("backfill").unwrap(),
            base: Id::new("base").unwrap(),
            definition: "v1".into(),
            group: Id::new("derived").unwrap(),
            group_definition: "scalar-v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "answer".into(),
                expression: "old + 1".into(),
            }],
        };
        let operation = Operation {
            request_digest: "fixture".into(),
            eligibility: 0,
            epoch: 0,
            boundary: 0,
            definition: "v1".into(),
            base: Some(spec.base.clone()),
            base_manifest: None,
            schema_key: None,
            require_current_boundary: false,
        };
        let request = OperationRequest::Backfill(spec.clone());
        let first = engine
            .prepare_group(
                &request,
                &operation,
                &source,
                &spec.group_definition(),
                None,
                None,
                &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
            )
            .await
            .unwrap();
        let before = engine.inspect(&dataset).await.unwrap();
        let completions = before
            .iter()
            .filter(|(key, _)| key.starts_with("phase/backfill/"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        assert!(!completions.is_empty());
        let mut renamed = fields[0].as_ref().clone();
        renamed.name = "new name".into();
        renamed.doc = Some("presentation only".into());
        let new_schema = iceberg::spec::Schema::builder()
            .with_fields([
                fields[1].clone(),
                Arc::new(renamed),
                Arc::new(iceberg::spec::NestedField::optional(
                    30,
                    "old",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let directory = root.path().join("evolved");
        let metadata = native
            .metadata()
            .clone()
            .into_builder(None)
            .set_location(directory.to_string_lossy().into_owned())
            .assign_uuid(uuid::Uuid::new_v4())
            .add_current_schema(new_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let identifier =
            iceberg::TableIdent::new(native.identifier().namespace().clone(), "evolved".into());
        let evolved = storage::register_candidate(
            engine.inner.lifecycle.catalog.as_ref(),
            &identifier,
            move || Ok(metadata),
            &engine.inner.lifecycle.budget,
        )
        .await
        .unwrap();
        let evolved = storage::pin(
            &dataset,
            &evolved,
            source.rows,
            &engine.inner.lifecycle.budget,
            &source.objects,
        )
        .await
        .unwrap();
        let bound = engine
            .prepare_group(
                &request,
                &operation,
                &evolved,
                &spec.group_definition(),
                Some(&first.input_fields),
                None,
                &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
            )
            .await
            .unwrap();
        assert_eq!(bound.input_fields["old"], old_id);
        let after = engine.inspect(&dataset).await.unwrap();
        assert_eq!(
            completions,
            after
                .iter()
                .filter(|(key, _)| key.starts_with("phase/backfill/"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        );
        let data = |pin: &TablePin| {
            pin.objects
                .iter()
                .filter(|object| object.path.ends_with(".parquet"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(data(&first.table), data(&bound.table));
        assert_ne!(first.table.metadata, bound.table.metadata);
        // Same SQL and current source pin, but fresh name resolution consumes
        // ID30 rather than ID1. Neither file nor whole-table reuse may collide.
        let fresh = engine
            .prepare_group(
                &request,
                &operation,
                &evolved,
                &spec.group_definition(),
                None,
                None,
                &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
            )
            .await
            .unwrap();
        assert_eq!(fresh.input_fields["old"], 30);
        assert_ne!(data(&fresh.table), data(&bound.table));
        for (pin, expected) in [(&bound.table, Some(2)), (&fresh.table, None)] {
            let table = storage::open_pin(pin).await.unwrap();
            let batches = storage::read_batches(&table, &engine.inner.lifecycle.budget)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let values = batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(values.iter().collect::<Vec<_>>(), vec![expected]);
        }
    }

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
