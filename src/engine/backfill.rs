//! Recoverable row-local field groups, selected against an exact source boundary.

use super::retention::owners::ReferenceProtections;
use super::{expr::is_valid_identifier, write::identifier as quote, *};
use iceberg::spec::{DataFile, FormatVersion, StructType};
use std::collections::BTreeSet;

pub(in crate::engine) mod continuous;
#[cfg(all(test, feature = "test-util"))]
mod correction_tests;
pub(in crate::engine) mod expressions;
pub(in crate::engine) mod origins;
mod preparation;
#[cfg(all(test, feature = "test-util"))]
mod protection_tests;
#[cfg(all(test, feature = "test-util"))]
mod validator_tests;
pub(in crate::engine) mod verification;
use expressions::Expressions;
use origins::{
    FileOrigin, OriginalDeclaration, OriginalExecution, ScalarOriginBinding, ScalarOrigins,
};
pub(super) use verification::{verify_binding_dependencies, verify_groups};

// Batches acknowledgments without retaining payloads or creating one lifecycle
// transaction per row/file. A crash may repeat only the unacknowledged bundle.
const COMPLETION_BUNDLE: usize = 16;
pub(in crate::engine) const PROCESSOR: &str = "docspec.scalar-backfill.v2";
// Version the routine scalar checker independently from value computation.
// Bump this when its schema/field mapping or native inspection rules change.
const VALIDATOR: &str = "docspec.scalar-validation.v1";

pub(super) struct PreparedGroup {
    pub origins: Option<ObjectRef>,
    pub table: TablePin,
    pub native_field_ids: BTreeMap<i32, i32>,
    pub input_fields: BTreeMap<String, i32>,
    pub protections: Option<ReferenceProtections>,
}

/// Alias retained for internal callers referencing `backfill::Capture`.
pub(in crate::engine) type Capture = BackfillCapture;

/// Captured source boundary, target group state, and admission fence for a backfill operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct BackfillCapture {
    pub(in crate::engine) admission_fence: u64,
    pub(in crate::engine) target: Option<ObjectRef>,
    pub(in crate::engine) target_binding: Option<ComputedGroup>,
    pub(in crate::engine) base_manifest: ObjectRef,
    pub(in crate::engine) related: Option<Box<RelatedResult>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct FileCompletion {
    /// Original execution identity; missing historical facts remain unknown.
    #[serde(default)]
    pub(in crate::engine) execution: Option<OriginalExecution>,
    pub(in crate::engine) source: ObjectRef,
    /// Exact schema/default interpretation used for this physical source file.
    /// Missing on older completions; not part of the material reuse key.
    #[serde(default)]
    pub(in crate::engine) source_metadata: Option<ObjectRef>,
    pub(in crate::engine) rows: u64,
    pub(in crate::engine) descriptor: ObjectRef,
    pub(in crate::engine) outputs: Vec<ObjectRef>,
}

/// Exact source and output consumed by group assembly or validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::engine) struct GroupDependencies {
    pub(in crate::engine) source: NativeSnapshotRef,
    /// Verified logical identity when the consumed source is a physical rewrite.
    /// Without this proof, only the captured native metadata identifies its state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::engine) source_logical_metadata: Option<ObjectRef>,
    pub(in crate::engine) output: NativeSnapshotRef,
}

/// A prepared group with independently readable recovery dependencies.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct PreparedCompletion {
    /// Immutable canonical file executions; absence remains unknown.
    #[serde(default)]
    pub(in crate::engine) origins: Option<ObjectRef>,
    // Preserve the original TablePin shape for retained records and readers.
    #[serde(flatten)]
    pub(in crate::engine) table: TablePin,
    /// Absence on an older completion remains unknown to retention.
    #[serde(default)]
    pub(in crate::engine) dependencies: Option<GroupDependencies>,
}

/// A validation acknowledgment with the exact snapshots it checked.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct ValidationCompletion {
    /// Missing legacy identity is unknown; it cannot qualify the current checker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::engine) validator: Option<String>,
    /// Immutable canonical file executions; absence remains unknown.
    #[serde(default)]
    pub(in crate::engine) origins: Option<ObjectRef>,
    // Preserve the original ObjectRef shape; its identity still decides reuse.
    #[serde(flatten)]
    pub(in crate::engine) output: ObjectRef,
    /// Absence on an older acknowledgment remains unknown to retention.
    #[serde(default)]
    pub(in crate::engine) dependencies: Option<GroupDependencies>,
}

/// Readback retains its qualification independently of upstream phase recovery.
#[derive(Clone, Debug, Serialize)]
pub(in crate::engine) struct ReadbackCompletion {
    // Keep the public ReleasePin and its immutable manifest identity unchanged.
    #[serde(flatten)]
    pub(in crate::engine) release: ReleasePin,
    /// Missing legacy identity remains unknown, requiring the current checker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scalar_validator: Option<String>,
}

// Flattened Serde Content does not support JSON's integer map keys inside the
// native field-ID maps. Decode the unchanged public pin through the maintained
// JSON Value deserializer instead, while typing the one private field explicitly.
impl<'de> Deserialize<'de> for ReadbackCompletion {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let (release, scalar_validator) =
            super::validation::deserialize_stamped_release(deserializer, "scalar_validator")?;
        Ok(Self {
            release,
            scalar_validator,
        })
    }
}

/// Progress against a captured source; completed files remain usable after stale promotion.
#[derive(Clone, Debug, Serialize)]
pub struct BackfillPlan {
    pub completed: bool,
    /// Exact recovery diagnostics; absent when the completed result needs no file scan.
    pub files_completed: Option<usize>,
    /// Exact recovery diagnostics; absent when the completed result needs no file scan.
    pub files_pending: Option<usize>,
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

pub(in crate::engine) fn validate_definition(spec: &GroupDefinition) -> Result<()> {
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
            field.id < 1 || !is_valid_identifier(&field.name) || field.expression.trim().is_empty()
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
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses invalid pins or coverage, conflicting names, or exhausted capacity.
    pub async fn read_combined(
        &self,
        release: &ReleasePin,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.require_data_work()?;
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            engine.check_read_release(&release).await?;
            resolve_release(&release, &engine.inner.lifecycle.budget).await?;
            let lock = engine.lock(&release.dataset.dataset).await;
            let mut dataset = release.dataset.clone();
            {
                let _guard = lock.lock().await;
                let view = engine.inner.lifecycle.open(&dataset.dataset).await?;
                for (group, pin) in &mut dataset.groups {
                    *pin = engine.resolve_representation_pin(&view, group, pin).await?;
                }
            }
            combined_dataset(&dataset, &engine.inner.lifecycle.budget).await
        })
        .await
    }
}

/// Streams source and computed fields without requiring their producing catalog.
///
/// The caller retains the files or relies on a published availability window.
/// A serialized pin creates no lease or host protection. Engine reads reuse
/// this implementation inside their admitted reader lifetime.
///
/// # Errors
/// Refuses invalid pins or coverage, conflicting names, or exhausted capacity.
pub async fn read_combined(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    resolve_release(release, budget).await?;
    combined_dataset(&release.dataset, budget).await
}

async fn combined_dataset(
    dataset: &DatasetPin,
    budget: &Budget,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let source_pin = dataset
        .groups
        .get("source")
        .ok_or_else(|| invalid_group("dataset is missing source group"))?;
    let objects = storage::Objects::for_path(&source_pin.metadata.path);
    objects.register_query_store(budget.runtime_env())?;
    let source = objects.open_pin(source_pin, budget).await?;
    let context = budget.context();
    let mut columns = source
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|field| {
            storage::defaults::supplied_id(&field.name).is_none()
                && !storage::identity::hidden(&field.name)
                && (dataset.related.is_none() || !refresh::hidden_field(&field.name))
        })
        .map(|field| format!("s.{}", quote(&field.name)))
        .collect::<Vec<_>>();
    context.register_table(
        "source",
        crate::query::occurrences(source, budget.clone(), Some(source_pin.rows))?,
    )?;
    let mut from = "source s".to_owned();
    for (index, (name, binding)) in dataset.computed.iter().enumerate() {
        let alias = format!("g{index}");
        let group_pin = dataset
            .groups
            .get(name)
            .ok_or_else(|| invalid_group("dataset is missing table pin for computed group"))?;
        let native = storage::open_pin(group_pin, budget).await?;
        context.register_table(
            &alias,
            crate::query::table(native, budget.clone(), Some(group_pin.rows))?,
        )?;
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

impl Engine {
    /// Plans file reuse without opening source payloads.
    ///
    /// # Errors
    /// Refuses unknown bases, conflicting retry identities, or invalid definitions.
    pub async fn plan_backfill(&self, spec: &BackfillSpec) -> Result<BackfillPlan> {
        let memory = self.request_memory(spec)?;
        let spec = spec.clone();
        self.admit_work(memory, move |engine| async move {
            engine.plan_backfill_admitted(&spec).await
        })
        .await
    }

    async fn plan_backfill_admitted(&self, spec: &BackfillSpec) -> Result<BackfillPlan> {
        self.require_data_work()?;
        validate(spec)?;
        let request = fingerprint(&OperationRequest::Backfill(spec.clone()))?;
        let lifecycle = &self.inner.lifecycle;
        let view = self.read_view(&spec.dataset).await?;
        self.check_operation_available(&view, &spec.operation)
            .await?;
        self.check_not_abandoned(&view, &spec.operation).await?;
        let old: Option<Operation> = lifecycle
            .get(&view, &format!("operation/{}", spec.operation))
            .await?;
        if old
            .as_ref()
            .is_some_and(|old| old.request_digest != request)
        {
            return Err(conflicting_operation());
        }
        self.check_continuous_child(&view, spec, None).await?;
        if let Some(result) = lifecycle
            .get::<ReleasePin>(&view, &format!("result/{}", spec.operation))
            .await?
        {
            if old.is_none()
                || result.dataset.dataset != spec.dataset
                || result.id != spec.operation
                || result.dataset.version != spec.operation
                || !matches!(&result.dataset.request, OperationRequest::Backfill(recorded) if recorded == spec)
                || !result.verified
            {
                return Err(Error::refusal(
                    "release_mismatch",
                    "completed Backfill lacks matching admission or result identity",
                ));
            }
            let source = result.dataset.groups.get("source").ok_or_else(|| {
                Error::refusal("release_mismatch", "completed Backfill has no source group")
            })?;
            // Completion preserves row metadata, not historical file diagnostics.
            // Recovery inputs may have independently ended their availability.
            return Ok(BackfillPlan {
                completed: true,
                files_completed: None,
                files_pending: None,
                rows: source.rows,
                failures: BTreeMap::new(),
                promotion_refusal: None,
            });
        }
        if old.is_none() {
            self.check_operation_available(&view, &spec.base).await?;
        }
        let base = self.release_admitted(&spec.dataset, &spec.base).await?;
        let base_source = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid_group("backfill base release is missing source group"))?;
        let native = storage::open_pin(base_source, &self.inner.lifecycle.budget).await?;
        let expressions = Expressions::compile(
            &self.context(),
            native.metadata().current_schema(),
            &spec.group_definition(),
            None,
        )?;
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        let eligibility = old
            .as_ref()
            .map_or(authority.eligibility, |old| old.eligibility);
        let scan = storage::OccurrenceScan::open(&native, &lifecycle.budget).await?;
        let mut plan = BackfillPlan {
            completed: false,
            files_completed: Some(0),
            files_pending: Some(0),
            rows: base_source.rows,
            failures: BTreeMap::new(),
            promotion_refusal: None,
        };
        let operation = old.clone().unwrap_or(Operation {
            request_digest: request,
            eligibility,
            epoch: authority.epoch,
            boundary: base.dataset.boundary,
            admission_fence: None,
            result_format: FORMAT,
            definition: spec.definition.clone(),
            base: Some(spec.base.clone()),
            base_manifest: None,
            schema_key: None,
            require_current_boundary: false,
        });
        let capture = lifecycle
            .get::<BackfillCapture>(&view, &format!("backfill/{}", spec.operation))
            .await?
            .unwrap_or(BackfillCapture {
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
        let (retained_readback, _) = if old.is_some() {
            self.retained_backfill_readback(
                spec,
                &operation,
                &capture,
                &base,
                &mut super::verification::Checks::composition_with_objects(
                    self.inner.objects.clone(),
                ),
            )
            .await?
        } else {
            (None, false)
        };
        let retained_target = retained_readback.is_some();
        plan.promotion_refusal =
            check_definition(&authority, &OperationRequest::Backfill(spec.clone()))
                .and_then(|()| promotion(&authority, &operation, &capture, base_source, spec))
                .err()
                .map(|error| error.code().to_owned());
        if plan.promotion_refusal.is_none() {
            plan.promotion_refusal = self
                .check_cutover_inputs(&view, &spec.dataset, None)
                .await
                .err()
                .map(|error| error.code().to_owned());
        }
        if plan.promotion_refusal.is_none() {
            plan.promotion_refusal = if let Some(readback) = &retained_readback {
                self.guard_corrections(&view, &readback.dataset, None)
                    .await
                    .err()
                    .map(|error| error.code().to_owned())
            } else if self
                .requirements(&view, &spec.group_definition())
                .await?
                .is_some()
            {
                Some("correction_required".into())
            } else {
                None
            };
        }
        let objects = source_objects(base_source);
        let mut files_completed = 0;
        let mut files_pending = 0;
        for file in scan.files() {
            let object = objects.get(file.file_path()).ok_or_else(|| {
                invalid_group("source data file is missing from source pin objects")
            })?;
            let key = file_key(object, &expressions, eligibility)?;
            if retained_target
                || self
                    .active_completion::<FileCompletion>(&view, &key)
                    .await?
                    .is_some()
            {
                files_completed += 1;
            } else {
                files_pending += 1;
                if let Some(error) = lifecycle
                    .get::<String>(&view, &format!("failure/{}/{key}", spec.operation))
                    .await?
                {
                    plan.failures.insert(object.path.clone(), error);
                }
            }
        }
        plan.files_completed = Some(files_completed);
        plan.files_pending = Some(files_pending);
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
        let memory = self.request_memory(&(spec, stop))?;
        let spec = spec.clone();
        let stop = stop.map(str::to_owned);
        self.admit_work(memory, move |engine| async move {
            engine.backfill_admitted(&spec, stop.as_deref()).await
        })
        .await
    }

    async fn backfill_admitted(
        &self,
        spec: &BackfillSpec,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        self.require_data_work()?;
        validate(spec)?;
        let digest = fingerprint(&OperationRequest::Backfill(spec.clone()))?;
        // Native preparation has a large async state. Keep it on the heap rather
        // than nesting another copy in the reconciliation future's stack frame.
        let result = Box::pin(self.prepare_backfill(spec, stop, None)).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code() != "injected_failure")
        {
            let lock = self.lock(&spec.dataset).await;
            let _guard = lock.lock().await;
            let lifecycle = &self.inner.lifecycle;
            if let Ok(view) = self.read_view(&spec.dataset).await {
                self.check_operation_available(&view, &spec.operation)
                    .await?;
                let operation: Option<Operation> = lifecycle
                    .get(&view, &format!("operation/{}", spec.operation))
                    .await?;
                self.check_continuous_child(&view, spec, None).await?;
                if operation.is_some_and(|operation| operation.request_digest == digest)
                    && let Some(completed) = lifecycle
                        .get(&view, &format!("result/{}", spec.operation))
                        .await?
                {
                    return Ok(completed);
                }
            }
        }
        result
    }

    async fn prepare_backfill(
        &self,
        spec: &BackfillSpec,
        stop: Option<&str>,
        continuous: Option<&continuous::Attempt>,
    ) -> Result<ReleasePin> {
        validate(spec)?;
        let request = OperationRequest::Backfill(spec.clone());
        let digest = fingerprint(&request)?;
        let lifecycle = &self.inner.lifecycle;
        let (operation, capture, base, stale_capture) = match self
            .admit_backfill_operation(spec, &request, &digest, continuous)
            .await?
        {
            BackfillAdmission::Completed(result) => return Ok(result),
            BackfillAdmission::Admitted(admitted) => admitted,
        };
        if continuous.is_some() {
            stop_after(stop, "continuous-admission")?;
        }
        if stale_capture {
            // Preserve protected first admission before retrying. Otherwise a
            // hot source can exhaust optimistic attempts with no retained parent
            // eligible to acquire the bounded intake fence. Skip obsolete work.
            return Err(Error::refusal(
                "stale_source_boundary",
                "source changed before protected child admission; recapture before processing",
            ));
        }
        let source = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid_group("backfill base release is missing source group"))?;
        // Issued readbacks retain their qualified target even when earlier phase
        // promises retire. Resolve them before reopening those preparation paths.
        let mut checks = super::verification::Checks::with_objects(
            VerificationLevel::Routine,
            self.inner.objects.clone(),
        );
        let (retained_readback, current_readback_validator) = self
            .retained_backfill_readback(spec, &operation, &capture, &base, &mut checks)
            .await?;
        if let Some(readback) = &retained_readback {
            let view = lifecycle.open(&spec.dataset).await?;
            self.guard_corrections(&view, &readback.dataset, None)
                .await?;
        }
        let (prepared, binding, prepared_protections) = self
            .prepare_or_recover_backfill_group(
                spec,
                &request,
                &operation,
                &capture,
                &base,
                source,
                continuous,
                stop,
                retained_readback.as_ref(),
                current_readback_validator,
                &mut checks,
            )
            .await?;
        checks.compose();
        self.promote_backfill_composition(
            spec,
            &request,
            &operation,
            &capture,
            source,
            prepared,
            binding,
            prepared_protections,
            retained_readback,
            stop,
            continuous,
            &mut checks,
        )
        .await
    }

    async fn admit_backfill_operation(
        &self,
        spec: &BackfillSpec,
        request: &OperationRequest,
        digest: &str,
        continuous: Option<&continuous::Attempt>,
    ) -> Result<BackfillAdmission> {
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&spec.dataset).await;
        let mut admission: Option<super::admission::PreparedAdmission> = None;
        loop {
            let guard = lock.lock().await;
            let view = self.read_view(&spec.dataset).await?;
            self.check_operation_available(&view, &spec.operation)
                .await?;
            let old: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation))
                .await?;
            if old.as_ref().is_some_and(|old| old.request_digest != digest) {
                return Err(conflicting_operation());
            }
            self.check_continuous_child(&view, spec, continuous).await?;
            if let Some(result) = lifecycle
                .get(&view, &format!("result/{}", spec.operation))
                .await?
            {
                return Ok(BackfillAdmission::Completed(result));
            }
            self.check_not_abandoned(&view, &spec.operation).await?;
            if old.is_none() {
                self.check_operation_available(&view, &spec.base).await?;
            }
            let decoding = if old.is_none() {
                Some(lifecycle.rows(&view).await?.reserve_decoded(
                    &lifecycle.budget,
                    &["authority", &format!("result/{}", spec.base)],
                )?)
            } else {
                None
            };
            let base: ReleasePin = lifecycle
                .get(&view, &format!("result/{}", spec.base))
                .await?
                .ok_or_else(|| {
                    Error::refusal("no_release", "backfill source version is unavailable")
                })?;
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            check_definition(&authority, request)?;
            if old.is_none() && base.dataset.eligibility != authority.eligibility {
                return Err(Error::refusal(
                    "stale_eligibility",
                    "captured source selection requires revalidation under current eligibility",
                ));
            }
            if let Some(operation) = old {
                check_preparation(&authority, &operation)?;
                let capture: BackfillCapture = lifecycle
                    .get(&view, &format!("backfill/{}", spec.operation))
                    .await?
                    .ok_or_else(|| invalid_group("admitted backfill is missing capture record"))?;
                return Ok(BackfillAdmission::Admitted((
                    operation, capture, base, false,
                )));
            }
            let Some(protected) = &admission else {
                let boundary = view.table.metadata_location_result()?.to_owned();
                drop(guard);
                admission = Some(
                    self.protect_admission(
                        request,
                        boundary,
                        Some(base),
                        None,
                        decoding.expect("new backfill admission reserved decoding"),
                    )
                    .await?,
                );
                continue;
            };
            if !protected.is_current(&view)? {
                // Another independent group may have advanced the catalog.
                // Re-evaluate admission; do not replay the stale decision.
                admission = None;
                continue;
            }
            let operation = Operation {
                request_digest: digest.to_owned(),
                eligibility: authority.eligibility,
                epoch: authority.epoch,
                boundary: base.dataset.boundary,
                admission_fence: None,
                result_format: FORMAT,
                definition: spec.definition.clone(),
                base: Some(spec.base.clone()),
                base_manifest: None,
                schema_key: None,
                require_current_boundary: false,
            };
            let capture = BackfillCapture {
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
            let mut records = vec![
                record(format!("operation/{}", spec.operation), &operation)?,
                record(format!("request/{}", spec.operation), request)?,
                record(format!("backfill/{}", spec.operation), &capture)?,
                record(
                    format!("runtime/{}", spec.operation),
                    serde_json::json!({"processor": PROCESSOR, "dependency_lock_blake3": blake3::hash(include_bytes!("../../Cargo.lock")).to_hex().as_str(), "limits": lifecycle.budget.limits}),
                )?,
            ];
            let stale_capture = self
                .continuous_admission_records(&view, spec, continuous, &mut records)
                .await?;
            protected.records(&view, request, &mut records)?;
            lifecycle.commit(&view, records).await?;
            return Ok(BackfillAdmission::Admitted((
                operation,
                capture,
                base,
                stale_capture,
            )));
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Recovery or preparation binds operation, source, continuous intent, and shared verification checks"
    )]
    async fn prepare_or_recover_backfill_group(
        &self,
        spec: &BackfillSpec,
        request: &OperationRequest,
        operation: &Operation,
        capture: &BackfillCapture,
        base: &ReleasePin,
        source: &TablePin,
        continuous: Option<&continuous::Attempt>,
        stop: Option<&str>,
        retained_readback: Option<&Retained<ReleasePin>>,
        current_readback_validator: bool,
        checks: &mut super::verification::Checks,
    ) -> Result<(TablePin, ComputedGroup, Option<ReferenceProtections>)> {
        let lifecycle = &self.inner.lifecycle;
        let (prepared, binding, mut prepared_protections) =
            if let Some(readback) = retained_readback {
                (
                    readback
                        .dataset
                        .groups
                        .get(spec.group.as_str())
                        .ok_or_else(|| {
                            invalid_group("retained readback is missing target group table pin")
                        })?
                        .clone(),
                    readback
                        .dataset
                        .computed
                        .get(spec.group.as_str())
                        .ok_or_else(|| {
                            invalid_group("retained readback is missing target computed group")
                        })?
                        .clone(),
                    readback
                        .protections
                        .as_ref()
                        .map(ReferenceProtections::duplicate)
                        .transpose()?,
                )
            } else {
                if base.manifest.bytes > lifecycle.budget.limits.max_metadata_bytes as u64 {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "source manifest exceeds metadata allowance",
                    ));
                }
                let memory = Arc::new(
                    lifecycle
                        .budget
                        .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
                );
                let decoded: DatasetPin = inputs::read_manifest(
                    &self.inner.objects,
                    &base.manifest,
                    &lifecycle.budget,
                    &mut 0,
                    &memory,
                )
                .await?;
                if decoded != base.dataset {
                    return Err(invalid_group(
                        "decoded base manifest differs from retained base dataset pin",
                    ));
                }
                let definition = spec.group_definition();
                checks.trust(base, &lifecycle.budget).await?;
                let view = lifecycle.open(&spec.dataset).await?;
                let source_protections = self.request_protections(&view, &spec.operation).await?;
                let prepared = Box::pin(self.prepare_group(
                    request,
                    operation,
                    source,
                    source_protections.as_ref(),
                    &definition,
                    continuous.map(|attempt| &attempt.intent.input_fields),
                    stop,
                    checks,
                ))
                .await?;
                let binding = ComputedGroup {
                    origins: prepared.origins,
                    qualification: None,
                    definition,
                    input_fields: prepared.input_fields,
                    request: request.clone(),
                    native_field_ids: prepared.native_field_ids,
                    source_metadata: source.metadata.clone(),
                    base_manifest: capture.base_manifest.clone(),
                };
                (prepared.table, binding, prepared.protections)
            };
        if retained_readback.is_some() {
            if !current_readback_validator {
                let view = lifecycle.open(&spec.dataset).await?;
                let source_protections = self.request_protections(&view, &spec.operation).await?;
                let validation_protections = self
                    .validate_scalar_group(
                        request,
                        operation,
                        ScalarOriginBinding {
                            source,
                            group: &prepared,
                            definition: &binding.definition,
                            input_fields: &binding.input_fields,
                            native_field_ids: &binding.native_field_ids,
                        },
                        source_protections.as_ref(),
                        binding.origins.as_ref(),
                        prepared_protections.as_ref(),
                        checks,
                    )
                    .await?;
                ReferenceProtections::inherit(
                    &mut prepared_protections,
                    validation_protections.as_ref(),
                )?;
            }
            stop_after(stop, "validation")?;
        }
        Ok((prepared, binding, prepared_protections))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Composition retry loop binds prepared group, binding, protections, readback, and continuous attempt"
    )]
    async fn promote_backfill_composition(
        &self,
        spec: &BackfillSpec,
        request: &OperationRequest,
        operation: &Operation,
        capture: &BackfillCapture,
        source: &TablePin,
        prepared: TablePin,
        binding: ComputedGroup,
        prepared_protections: Option<ReferenceProtections>,
        mut retained_readback: Option<Retained<ReleasePin>>,
        stop: Option<&str>,
        continuous: Option<&continuous::Attempt>,
        checks: &mut super::verification::Checks,
    ) -> Result<ReleasePin> {
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&spec.dataset).await;
        // Only composition is retried after a compatible sibling selection. Expensive
        // file work is retained; no stale whole-dataset manifest can win the retry.
        loop {
            let (current, current_protections) = {
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                self.check_continuous_child(&view, spec, continuous).await?;
                if let Some(completed) = lifecycle
                    .get(&view, &format!("result/{}", spec.operation))
                    .await?
                {
                    return Ok(completed);
                }
                let authority: Authority =
                    lifecycle.get(&view, "authority").await?.unwrap_or_default();
                self.check_not_abandoned(&view, &spec.operation).await?;
                promotion(&authority, operation, capture, source, spec)?;
                let current = authority
                    .release
                    .ok_or_else(|| invalid_group("authority is missing selected release"))?;
                let key = format!("result/{}", current.id);
                let protections = self.retained_protections(&view, &key).await?;
                if protections.is_some() {
                    let rows = lifecycle.rows(&view).await?;
                    let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
                    let recorded: ReleasePin = lifecycle
                        .get(&view, &key)
                        .await?
                        .ok_or_else(|| invalid_group("selected release record is missing"))?;
                    if recorded != current {
                        return Err(invalid_group(
                            "recorded selected release differs from authority release",
                        ));
                    }
                }
                (current, protections)
            };
            let mut readback_protections = prepared_protections
                .as_ref()
                .map(ReferenceProtections::duplicate)
                .transpose()?;
            ReferenceProtections::inherit(&mut readback_protections, current_protections.as_ref())?;
            let mut dataset = current.dataset.clone();
            dataset.format = operation.result_format;
            dataset.parent_manifest = Some(current.manifest.clone());
            dataset.version = spec.operation.clone();
            dataset.request = request.clone();
            dataset.processor = PROCESSOR.into();
            dataset
                .groups
                .insert(spec.group.to_string(), prepared.clone());
            dataset
                .computed
                .insert(spec.group.to_string(), binding.clone());
            check_names(&dataset)?;
            Box::pin(self.qualify_corrections(
                request,
                operation,
                &mut dataset,
                &mut readback_protections,
                checks,
            ))
            .await?;
            if let Some(readback) = &retained_readback {
                preparation::check_retained_target(
                    &readback.dataset,
                    &dataset,
                    spec.group.as_str(),
                )?;
            }
            verify_groups(&dataset, &lifecycle.budget, checks).await?;
            super::evidence::verify(&dataset, &lifecycle.budget, checks).await?;
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
            let recorded: Option<Retained<ReleasePin>> = if retained_readback
                .as_ref()
                .is_some_and(|readback| readback.dataset == dataset)
            {
                retained_readback.take()
            } else {
                let view = lifecycle.open(&spec.dataset).await?;
                if let Some(value) = lifecycle.get(&view, &readback_key).await? {
                    Some(Retained {
                        value,
                        protections: self.retained_protections(&view, &readback_key).await?,
                    })
                } else {
                    None
                }
            };
            let release = if let Some(recorded) = recorded {
                if recorded.dataset != dataset {
                    return Err(invalid_group(
                        "recorded backfill readback dataset disagrees with candidate dataset",
                    ));
                }
                check_release_available_with_checks(
                    &self.inner.objects,
                    &recorded,
                    &lifecycle.budget,
                    checks,
                )
                .await?;
                recorded
            } else {
                if let Some(readback) = retained_readback.take() {
                    // A sibling changed composition, not this completed target.
                    check_release_available_with_checks(
                        &self.inner.objects,
                        &readback,
                        &lifecycle.budget,
                        checks,
                    )
                    .await?;
                }
                #[cfg(feature = "test-util")]
                work_started("backfill-readback");
                let directory = self.reserve_group(request, operation, "release").await?;
                let manifest_path = directory.child("release.json")?.as_string()?;
                let manifest = ObjectRef {
                    owner: spec.dataset.clone(),
                    path: manifest_path,
                    digest: blake3::hash(&serialized).to_hex().to_string(),
                    bytes: serialized.len() as u64,
                };
                self.inner
                    .objects
                    .create_candidate_metadata(&manifest, serialized, &lifecycle.budget, false)
                    .await?;
                let release = ReleasePin {
                    id: spec.operation.clone(),
                    dataset,
                    manifest,
                    verified: true,
                };
                let memory = Arc::new(
                    lifecycle
                        .budget
                        .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
                );
                let decoded: DatasetPin = inputs::read_manifest(
                    &self.inner.objects,
                    &release.manifest,
                    &lifecycle.budget,
                    &mut 0,
                    &memory,
                )
                .await?;
                if decoded != release.dataset {
                    return Err(invalid_group(
                        "decoded candidate release manifest differs from candidate dataset",
                    ));
                }
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                self.check_completion(&view, operation, &spec.operation)
                    .await?;
                // Other workers or a prior loop iteration may already have
                // retained this operation's target. Check in the commit view so
                // a late correction cannot silently change that target.
                self.check_backfill_target(&view, spec, &release.dataset)
                    .await?;
                if let Some(value) = lifecycle.get::<ReleasePin>(&view, &readback_key).await? {
                    Retained {
                        value,
                        protections: self.retained_protections(&view, &readback_key).await?,
                    }
                } else {
                    let records = completion_records(
                        &spec.dataset,
                        &view,
                        &readback_key,
                        &ReadbackCompletion {
                            release: release.clone(),
                            scalar_validator: Some(VALIDATOR.into()),
                        },
                        readback_protections.as_ref(),
                    )?;
                    lifecycle.commit(&view, records).await?;
                    Retained {
                        value: release,
                        protections: readback_protections,
                    }
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
            self.check_continuous_child(&view, spec, continuous).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation))
                .await?
            {
                return Ok(completed);
            }
            let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
            promotion(&authority, operation, capture, source, spec)?;
            if authority.release.as_ref() != Some(&current) {
                continue;
            }
            self.check_cutover_inputs(&view, &spec.dataset, None)
                .await?;
            if let Some(dependencies) = &dependencies {
                Box::pin(self.guard_ancestors(dependencies, &release)).await?;
            }
            if let Some(attempt) = continuous {
                Box::pin(self.select_continuous_backfill(&view, authority, &release, attempt))
                    .await?;
            } else {
                Box::pin(self.select(&view, authority, &release)).await?;
            }
            stop_after(stop, "selection")?;
            return Ok(release.value);
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Single-use return value from backfill admission phase"
)]
enum BackfillAdmission {
    Completed(ReleasePin),
    Admitted((Operation, BackfillCapture, ReleasePin, bool)),
}

fn promotion(
    authority: &Authority,
    operation: &Operation,
    capture: &BackfillCapture,
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
    let current = authority
        .selected
        .as_ref()
        .ok_or_else(|| invalid_group("authority is missing selected dataset pin"))?;
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
        || (operation.result_format == PREVIOUS_FORMAT && authority.boundary != current.boundary)
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
    budget: &Budget,
) -> Result<BTreeMap<String, i32>> {
    let rebuilt = match request {
        OperationRequest::Build(_) => true,
        OperationRequest::Refresh(spec) => spec.mode == RefreshMode::Full,
        _ => false,
    };
    if !rebuilt {
        return Ok(previous.input_fields.clone());
    }
    let before = storage::open_pin(before, budget).await?;
    let after = storage::open_pin(after, budget).await?;
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
    Ok(format!(
        "phase/backfill/{eligibility}/{}",
        storage::json_digest(&(source, &expressions.dependencies))?.to_hex()
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
    let metadata = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    storage::check_available(completion.outputs.clone(), budget).await?;
    let descriptor = completion.descriptor.clone();
    let schema = schema.clone();
    let size = usize::try_from(descriptor.bytes)
        .map_err(|_| invalid_group("file completion descriptor byte length overflows usize"))?;
    let objects = storage::Objects::for_path(&descriptor.path);
    let buffer = budget.reserve(
        size.saturating_add(1)
            .saturating_add(objects.metadata_read_allowance()),
    )?;
    let handle = tokio::runtime::Handle::current();
    let files = budget
        .blocking_admitted(move || {
            // Keep input and decoder admission with the worker after cancellation.
            let (_metadata, _buffer) = (metadata, buffer);
            let mut bytes = vec![0u8; size];
            objects
                .read_metadata_admitted(&descriptor, &mut bytes, &handle)
                .map_err(|error| {
                    if error.code() == "corrupt_object" {
                        Error::refusal(
                            "corrupt_object",
                            "scalar completion descriptor differs from its retained identity",
                        )
                    } else {
                        error
                    }
                })?;
            Ok(iceberg::spec::read_data_files_from_avro(
                &mut bytes.as_slice(),
                &schema,
                0,
                &StructType::new(vec![]),
                FormatVersion::V2,
            )?)
        })
        .await?;
    let known: BTreeMap<_, _> = completion
        .outputs
        .iter()
        .map(|object| (object.path.as_str(), object.bytes))
        .collect();
    let paths: BTreeSet<_> = files.iter().map(DataFile::file_path).collect();
    let rows = files.iter().try_fold(0u64, |rows, file| {
        rows.checked_add(file.record_count())
            .ok_or_else(|| invalid_group("file completion record counts overflow"))
    })?;
    if known.len() != completion.outputs.len()
        || paths.len() != files.len()
        || paths.len() != known.len()
        || files
            .iter()
            .any(|file| known.get(file.file_path()) != Some(&file.file_size_in_bytes()))
        || rows != completion.rows
    {
        return Err(invalid_group(
            "file completion descriptor paths, byte sizes, or row counts disagree with completion record",
        ));
    }
    Ok(files)
}

pub(in crate::engine) fn invalid_group(reason: impl Into<String>) -> Error {
    Error::refusal("invalid_group", reason)
}

pub(in crate::engine) fn check_names(dataset: &DatasetPin) -> Result<()> {
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
        .saturating_add(inputs::metadata_size(
            completion,
            limit.saturating_sub(bytes),
        )?)
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
mod tests;
