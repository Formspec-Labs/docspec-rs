//! Recoverable row-local field groups, selected against an exact source boundary.

use super::retention::owners::ReferenceProtections;
use super::*;
use iceberg::spec::{DataFile, FormatVersion, StructType};
use std::collections::BTreeSet;

mod expressions;
mod preparation;
#[cfg(all(test, feature = "test-util"))]
mod protection_tests;
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
    pub protections: Option<ReferenceProtections>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct Capture {
    pub(in crate::engine) admission_fence: u64,
    pub(in crate::engine) target: Option<ObjectRef>,
    pub(in crate::engine) target_binding: Option<ComputedGroup>,
    pub(in crate::engine) base_manifest: ObjectRef,
    pub(in crate::engine) related: Option<Box<RelatedResult>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct FileCompletion {
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
    pub(in crate::engine) output: NativeSnapshotRef,
}

/// A prepared group with independently readable recovery dependencies.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(in crate::engine) struct PreparedCompletion {
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
    // Preserve the original ObjectRef shape; its identity still decides reuse.
    #[serde(flatten)]
    pub(in crate::engine) output: ObjectRef,
    /// Absence on an older acknowledgment remains unknown to retention.
    #[serde(default)]
    pub(in crate::engine) dependencies: Option<GroupDependencies>,
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
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses invalid pins or coverage, conflicting names, or exhausted capacity.
    pub async fn read_combined(
        &self,
        release: &ReleasePin,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            engine.check_read_release(&release).await?;
            read_combined(&release, &engine.inner.lifecycle.budget).await
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
    let source = storage::open_pin(&release.dataset.groups["source"]).await?;
    let context = budget.context();
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
        validate(spec)?;
        let request = fingerprint(&OperationRequest::Backfill(spec.clone()))?;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
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
        let native = storage::open_pin(&base.dataset.groups["source"]).await?;
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
            rows: base.dataset.groups["source"].rows,
            failures: BTreeMap::new(),
            promotion_refusal: None,
        };
        let mut retained_target = false;
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
        if old.is_some() {
            retained_target = self
                .retained_backfill_readback(spec, &operation, &capture, &base)
                .await?
                .is_some();
        }
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
        if plan.promotion_refusal.is_none() && operation.result_format == FORMAT {
            plan.promotion_refusal = self
                .check_abandoned_gap(
                    &view,
                    &spec.dataset,
                    operation.boundary,
                    capture.admission_fence,
                )
                .await
                .err()
                .map(|error| error.code().to_owned());
        }
        let objects = source_objects(&base.dataset.groups["source"]);
        let mut files_completed = 0;
        let mut files_pending = 0;
        for file in scan.files() {
            let object = objects.get(file.file_path()).ok_or_else(invalid_group)?;
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
        validate(spec)?;
        self.check_operation_entry(&spec.dataset, &spec.operation)
            .await?;
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
        let mut admission: Option<super::admission::PreparedAdmission> = None;
        let (operation, capture, base) = loop {
            let guard = lock.lock().await;
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
                break (operation, capture, base);
            } else {
                let Some(protected) = &admission else {
                    let boundary = view.table.metadata_location_result()?.to_owned();
                    drop(guard);
                    admission = Some(
                        self.protect_admission(
                            &request,
                            boundary,
                            Some(base),
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
                    request_digest: digest.clone(),
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
                let mut records = vec![
                    record(format!("operation/{}", spec.operation), &operation)?,
                    record(format!("request/{}", spec.operation), &request)?,
                    record(format!("backfill/{}", spec.operation), &capture)?,
                    record(
                        format!("runtime/{}", spec.operation),
                        serde_json::json!({"processor": PROCESSOR, "dependency_lock_blake3": blake3::hash(include_bytes!("../../Cargo.lock")).to_hex().as_str(), "limits": lifecycle.budget.limits}),
                    )?,
                ];
                protected.records(&view, &request, &mut records)?;
                lifecycle.commit(&view, records).await?;
                break (operation, capture, base);
            }
        };
        drop(admission);
        let source = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(invalid_group)?;
        // Issued readbacks retain their qualified target even when earlier phase
        // promises retire. Resolve them before reopening those preparation paths.
        let mut retained_readback = self
            .retained_backfill_readback(spec, &operation, &capture, &base)
            .await?;
        let (prepared, binding, prepared_protections) = if let Some(readback) = &retained_readback {
            (
                readback
                    .dataset
                    .groups
                    .get(spec.group.as_str())
                    .ok_or_else(invalid_group)?
                    .clone(),
                readback
                    .dataset
                    .computed
                    .get(spec.group.as_str())
                    .ok_or_else(invalid_group)?
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
            storage::verify_objects(vec![base.manifest.clone()], &lifecycle.budget).await?;
            let decoded: DatasetPin =
                serde_json::from_reader(std::fs::File::open(&base.manifest.path)?)?;
            if decoded != base.dataset {
                return Err(invalid_group());
            }
            let definition = spec.group_definition();
            let mut checks = super::verification::Checks::new(VerificationLevel::Routine);
            checks.trust(&base, &lifecycle.budget).await?;
            let view = lifecycle.open(&spec.dataset).await?;
            let source_protections = self.request_protections(&view, &spec.operation).await?;
            let prepared = Box::pin(self.prepare_group(
                &request,
                &operation,
                source,
                source_protections.as_ref(),
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
            (prepared.table, binding, prepared.protections)
        };
        // Only composition is retried after a compatible sibling selection. Expensive
        // file work is retained; no stale whole-dataset manifest can win the retry.
        loop {
            let (current, current_protections) = {
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
                self.check_not_abandoned(&view, &spec.operation).await?;
                promotion(&authority, &operation, &capture, source, spec)?;
                let current = authority.release.ok_or_else(invalid_group)?;
                let key = format!("result/{}", current.id);
                let protections = self.retained_protections(&view, &key).await?;
                if protections.is_some() {
                    let rows = lifecycle.rows(&view).await?;
                    let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
                    let recorded: ReleasePin = lifecycle
                        .get(&view, &key)
                        .await?
                        .ok_or_else(invalid_group)?;
                    if recorded != current {
                        return Err(invalid_group());
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
            dataset.changes = None;
            dataset.processor = PROCESSOR.into();
            dataset
                .groups
                .insert(spec.group.to_string(), prepared.clone());
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
                    return Err(invalid_group());
                }
                check_release_available(&recorded, &lifecycle.budget).await?;
                recorded
            } else {
                if let Some(readback) = retained_readback.take() {
                    // A sibling changed composition, not this completed target.
                    check_release_available(&readback, &lifecycle.budget).await?;
                }
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
                self.check_completion(&view, &operation, &spec.operation)
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
                        &release,
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
            if operation.result_format == FORMAT {
                self.check_abandoned_gap(
                    &view,
                    &spec.dataset,
                    operation.boundary,
                    capture.admission_fence,
                )
                .await?;
            }
            if let Some(dependencies) = &dependencies {
                Box::pin(self.guard_ancestors(dependencies, &release)).await?;
            }
            Box::pin(self.select(&view, authority, &release)).await?;
            stop_after(stop, "selection")?;
            return Ok(release.value);
        }
    }

    async fn retained_backfill_readback(
        &self,
        spec: &BackfillSpec,
        operation: &Operation,
        capture: &Capture,
        base: &ReleasePin,
    ) -> Result<Option<Retained<ReleasePin>>> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let prefix = format!("backfill-readback/{}/", spec.operation);
        let mut readbacks = rows.prefix(&prefix).peekable();
        if readbacks.peek().is_none() {
            return Ok(None);
        }
        let _memory = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(2))?;
        let request = OperationRequest::Backfill(spec.clone());
        let definition = spec.group_definition();
        let source = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(invalid_group)?;
        let mut retained: Option<(i64, &str, ReleasePin)> = None;
        for (key, revision, json) in readbacks {
            tokio::task::consume_budget().await;
            let readback: ReleasePin = serde_json::from_str(json)?;
            let dataset = &readback.dataset;
            let table = dataset
                .groups
                .get(spec.group.as_str())
                .ok_or_else(invalid_group)?;
            let binding = dataset
                .computed
                .get(spec.group.as_str())
                .ok_or_else(invalid_group)?;
            if !readback.verified
                || readback.id != spec.operation
                || dataset.dataset != spec.dataset
                || dataset.version != spec.operation
                || dataset.request != request
                || dataset.format != operation.result_format
                || dataset.eligibility != operation.eligibility
                || dataset.boundary != operation.boundary
                || dataset.definition != operation.definition
                || dataset.processor != PROCESSOR
                || dataset.groups.get("source") != Some(source)
                || dataset.related != capture.related
                || dataset.inputs != base.dataset.inputs
                || dataset.relationship != base.dataset.relationship
                || dataset.changes.is_some()
                || dataset.parent_manifest.is_none()
                || binding.definition != definition
                || binding.request != request
                || binding.source_metadata != source.metadata
                || binding.base_manifest != capture.base_manifest
                || capture.base_manifest != base.manifest
                || readback.manifest.owner != spec.dataset
                || key != format!("{prefix}{}", readback.manifest.digest)
            {
                return Err(invalid_group());
            }
            if let Some((_, _, previous)) = &retained
                && (previous.dataset.groups.get(spec.group.as_str()) != Some(table)
                    || previous.dataset.computed.get(spec.group.as_str()) != Some(binding))
            {
                return Err(Error::refusal(
                    "phase_generation_conflict",
                    format!("retained readback {key} binds a different target generation"),
                ));
            }
            if retained
                .as_ref()
                .is_none_or(|(prior, _, _)| revision > *prior)
            {
                retained = Some((revision, key, readback));
            }
        }
        if let Some((_, key, value)) = retained {
            Ok(Some(Retained {
                value,
                protections: self.retained_protections(&view, key).await?,
            }))
        } else {
            Ok(None)
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
    async fn completed_plan_qualifies_admission_and_result_without_opening_inputs() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            root.path().join("workspace"),
            Limits {
                scratch_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        for fault in [
            "none",
            "missing-admission",
            "conflicting-admission",
            "dataset",
            "id",
            "version",
            "request",
            "unverified",
            "missing-source",
        ] {
            let spec = BackfillSpec {
                dataset: Id::new(fault).unwrap(),
                operation: Id::new("derived").unwrap(),
                base: Id::new("base").unwrap(),
                definition: "v1".into(),
                group: Id::new("length").unwrap(),
                group_definition: "scalar-v1".into(),
                fields: vec![ComputedField {
                    id: 10,
                    name: "text_length".into(),
                    expression: "length(text)".into(),
                }],
            };
            let request = OperationRequest::Backfill(spec.clone());
            let operation = Operation {
                request_digest: if fault == "conflicting-admission" {
                    "another-request".into()
                } else {
                    fingerprint(&request).unwrap()
                },
                eligibility: 0,
                epoch: 0,
                boundary: 0,
                admission_fence: None,
                result_format: FORMAT,
                definition: spec.definition.clone(),
                base: Some(spec.base.clone()),
                base_manifest: None,
                schema_key: None,
                require_current_boundary: false,
            };
            // No source, manifest or native metadata objects exist. Only the
            // lifecycle receipt is needed to report a completed plan.
            let missing = ObjectRef {
                owner: spec.dataset.clone(),
                path: root.path().join("absent").to_string_lossy().into_owned(),
                digest: "a".repeat(64),
                bytes: 0,
            };
            let mut result = ReleasePin {
                id: spec.operation.clone(),
                dataset: DatasetPin {
                    format: FORMAT,
                    dataset: spec.dataset.clone(),
                    version: spec.operation.clone(),
                    groups: BTreeMap::from([(
                        "source".into(),
                        TablePin {
                            table: "source".into(),
                            metadata: missing.clone(),
                            snapshot: None,
                            schema_id: 0,
                            objects: vec![],
                            rows: 7,
                        },
                    )]),
                    computed: BTreeMap::new(),
                    inputs: BTreeMap::new(),
                    relationship: SourceRelationship::Independent,
                    related: None,
                    definition: spec.definition.clone(),
                    eligibility: 0,
                    boundary: 0,
                    request,
                    parent_manifest: None,
                    changes: None,
                    processor: PROCESSOR.into(),
                    validation: VerificationLevel::Routine,
                },
                manifest: missing,
                verified: true,
            };
            match fault {
                "dataset" => result.dataset.dataset = Id::new("other").unwrap(),
                "id" => result.id = Id::new("other").unwrap(),
                "version" => result.dataset.version = Id::new("other").unwrap(),
                "request" => {
                    let OperationRequest::Backfill(ref mut stored) = result.dataset.request else {
                        unreachable!()
                    };
                    stored.group_definition = "different".into();
                }
                "unverified" => result.verified = false,
                "missing-source" => result.dataset.groups.clear(),
                _ => {}
            }
            let lifecycle = &engine.inner.lifecycle;
            let view = lifecycle.open(&spec.dataset).await.unwrap();
            let mut changes = vec![record(format!("result/{}", spec.operation), result).unwrap()];
            if fault != "missing-admission" {
                changes.push(record(format!("operation/{}", spec.operation), operation).unwrap());
            }
            lifecycle.commit(&view, changes).await.unwrap();
            let plan = engine.plan_backfill(&spec).await;
            match fault {
                "none" => {
                    let plan = plan.unwrap();
                    assert!(plan.completed);
                    assert_eq!(plan.rows, 7);
                    assert_eq!(plan.files_completed, None);
                    assert_eq!(plan.files_pending, None);
                }
                "conflicting-admission" => {
                    assert_eq!(plan.unwrap_err().code(), "operation_conflict");
                }
                _ => assert_eq!(plan.unwrap_err().code(), "release_mismatch"),
            }
        }
    }

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
            admission_fence: None,
            result_format: FORMAT,
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
                None,
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
                None,
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
                None,
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
            source_metadata: None,
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
