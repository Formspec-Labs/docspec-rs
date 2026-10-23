//! Multi-phase operation execution pipeline: admission, source processing,
//! routine validation, inherited group recomputation, readback, and selection.

#[cfg(feature = "test-util")]
use super::test_hooks::work_started_async;
use super::{
    CompletedGroups, Engine, FORMAT, Operation, PROCESSOR, PreparedSource, Retained,
    ValidatedReadback, admission, backfill,
    changes::verify_changes,
    completion_records, evidence, inputs, locations,
    planner::{
        captured_manifest, check_authority, check_base, check_definition, check_group_rebuild,
        check_preparation, check_update_eligibility, conflicting_operation, fingerprint, phase_key,
    },
    refresh,
    release_check::{check_release_available, check_release_available_with_checks},
    retention::owners::ReferenceProtections,
    sql_validation::validate_request,
    test_hooks::stop_after,
    verification,
};
use crate::{
    Error, Result,
    catalog::{View, record},
    model::*,
    storage,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

mod completion;

impl Engine {
    /// Runs or resumes a selected input through an independently readable release.
    ///
    /// # Errors
    /// Refuses mismatched inputs, stale eligibility, capacity exhaustion, or corrupt outputs.
    /// Earlier qualified phases remain retained after any failure.
    pub async fn run(&self, spec: &RunSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Build(spec.clone()), memory, None)
            .await
    }

    /// Interrupts after a durable phase for lifecycle fault qualification.
    ///
    /// # Errors
    /// Returns `injected_failure` after the selected completion, or ordinary run errors.
    #[cfg(feature = "test-util")]
    pub async fn run_until(&self, spec: &RunSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(OperationRequest::Build(spec.clone()), memory, Some(phase))
            .await
    }

    /// Applies complete key replacements and deletions against one exact base.
    ///
    /// # Errors
    /// Refuses stale bases, overlapping inputs, schema mismatch or failed preparation.
    pub async fn update(&self, spec: &UpdateSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Update(spec.clone()), memory, None)
            .await
    }

    /// Interrupts an update after a durable phase for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or ordinary update errors.
    #[cfg(feature = "test-util")]
    pub async fn update_until(&self, spec: &UpdateSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(OperationRequest::Update(spec.clone()), memory, Some(phase))
            .await
    }

    pub(in crate::engine) async fn run_internal(
        &self,
        spec: OperationRequest,
        memory: tokio::sync::OwnedSemaphorePermit,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        let stop = stop.map(str::to_owned);
        self.admit_work(memory, move |engine| async move {
            engine.run_admitted(&spec, stop.as_deref()).await
        })
        .await
    }

    pub(in crate::engine) async fn run_admitted(
        &self,
        spec: &OperationRequest,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        self.check_request_backend(spec)?;
        validate_request(spec)?;
        // Owned-host maintenance waits for this admitted worker. One check before
        // preparation and error reconciliation covers the complete operation.
        {
            let lock = self.lock(spec.dataset()).await;
            let _guard = lock.lock().await;
            let view = self.request_view(spec).await?;
            self.check_operation_available(&view, spec.operation())
                .await?;
        }
        let request = fingerprint(spec)?;
        let result = self.prepare_and_select(spec, stop).await;
        #[cfg(all(not(test), not(feature = "test-util")))]
        let _ = self.inner.lifecycle.flush_deferred(spec.dataset()).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code() != "injected_failure")
        {
            // A competing attempt may select this exact operation while this
            // caller is preparing or checking its base. Reconcile durable success.
            let lock = self.lock(spec.dataset()).await;
            let _guard = lock.lock().await;
            let lifecycle = &self.inner.lifecycle;
            let view = self.request_view(spec).await?;
            let operation: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation()))
                .await?;
            if operation.is_some_and(|operation| operation.request_digest == request)
                && let Some(completed) = lifecycle
                    .get(&view, &format!("result/{}", spec.operation()))
                    .await?
            {
                return Ok(completed);
            }
        }
        result
    }

    // Source work finishes before composition. Keep its large native futures out
    // of the enclosing poll frame while later groups, checks and selection run.
    pub(in crate::engine) fn prepare_source<'a>(
        &'a self,
        spec: &'a OperationRequest,
        operation: &'a Operation,
        request: &'a str,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<PreparedSource>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            let lifecycle = &self.inner.lifecycle;
            if let Some(processing) = self.phase(spec, operation, "processing").await? {
                let admission = if processing.dependencies.is_none() {
                    let admission = self
                        .phase(spec, operation, "admission")
                        .await?
                        .ok_or_else(|| {
                            Error::refusal(
                                "phase_dependencies_unknown",
                                "legacy processing needs its original admission; retire that processing generation before rebuilding",
                            )
                        })?;
                    let view = lifecycle.open(spec.dataset()).await?;
                    let rows = lifecycle.rows(&view).await?;
                    let revision = |phase| -> Result<i64> {
                        rows.revision(&phase_key(
                            spec,
                            request,
                            operation.eligibility,
                            phase,
                            operation.base_manifest.as_ref(),
                            operation.schema_key.as_ref(),
                        )?)
                        .ok_or_else(|| {
                            Error::refusal(
                                "phase_dependencies_unknown",
                                "original phase generation is unavailable",
                            )
                        })
                    };
                    if revision("admission")? > revision("processing")? {
                        return Err(Error::refusal(
                            "phase_dependencies_unknown",
                            "legacy processing cannot adopt a newer admission generation; retire that processing before rebuilding",
                        ));
                    }
                    Some(admission)
                } else {
                    None
                };
                stop_after(stop, "admission")?;
                stop_after(stop, "processing")?;
                return Ok(PreparedSource {
                    admission,
                    processing,
                    #[cfg(all(not(test), not(feature = "test-util")))]
                    freshly_processed: false,
                });
            }
            let admission = self
                .admit_source_phase(spec, operation, request, stop)
                .await?;
            stop_after(stop, "admission")?;

            let (processing, _freshly_processed) = self
                .process_source_phase(spec, operation, request, &admission)
                .await?;
            stop_after(stop, "processing")?;

            Ok(PreparedSource {
                admission: Some(admission),
                processing,
                #[cfg(all(not(test), not(feature = "test-util")))]
                freshly_processed: _freshly_processed,
            })
        })
    }

    async fn admit_source_phase(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        request: &str,
        stop: Option<&str>,
    ) -> Result<Retained<PhaseResult>> {
        if let Some(phase) = self.phase(spec, operation, "admission").await? {
            return Ok(phase);
        }
        if let OperationRequest::Refresh(refresh) = spec {
            return self.admit_refresh(refresh, operation).await;
        }
        if let OperationRequest::Schema(schema) = spec {
            return self.admit_schema(schema, operation).await;
        }
        let lifecycle = &self.inner.lifecycle;
        crate::progress::phase(spec.dataset(), spec.operation(), "admission", async {
            #[cfg(feature = "test-util")]
            work_started_async("admission").await;
            let view = lifecycle.open(spec.dataset()).await?;
            let _memory = self.admission_memory(&view, spec.operation()).await?;
            let captured = self
                .admitted_dependencies(&view, spec.operation())
                .await?
                .unwrap_or_default();
            let inputs = spec.inputs();
            let size = if self.inner.retained.is_local() {
                inputs
                    .iter()
                    .try_fold(0u64, |sum, (name, input)| -> Result<u64> {
                        // Reused managed inputs consume no new input-copy space;
                        // retain the existing estimate for output/intermediate work.
                        let bytes = if let Some(object) = captured.inputs.get(name) {
                            object.bytes.saturating_mul(2)
                        } else {
                            std::fs::metadata(&input.path)?.len().saturating_mul(3)
                        };
                        Ok(sum.saturating_add(bytes))
                    })?
            } else {
                0
            };
            // Remote retained bytes consume no host copy/output capacity.
            // Scratch remains host-local and is checked before any transfer.
            let available = fs2::available_space(&self.inner.root)?;
            if available < size.saturating_add(lifecycle.budget.limits.scratch_bytes) {
                return Err(Error::refusal(
                    "disk_capacity",
                    "input, output and declared scratch do not fit available disk",
                ));
            }
            let remote = !self.inner.retained.is_local();
            if remote
                && !matches!(
                    spec,
                    OperationRequest::Build(_)
                        | OperationRequest::Update(_)
                        | OperationRequest::Evidence(_)
                )
            {
                return Err(locations::unsupported());
            }
            let pending = if remote && captured.inputs.len() != inputs.len() {
                Some(
                    self.reserve_source_attempt(spec, operation, &view, &captured.inputs)
                        .await?,
                )
            } else {
                None
            };
            if pending.is_some() {
                stop_after(stop, "source-reservation")?;
            }
            let directory = if captured.inputs.len() == inputs.len() || remote {
                None
            } else {
                Some(self.reserve_location(spec, operation, "admission").await?)
            };
            let mut outputs = Vec::new();
            for (name, input) in inputs {
                if let Some(retained) = captured.inputs.get(&name) {
                    outputs.push(retained.clone());
                    continue;
                }
                let source = PathBuf::from(&input.path);
                let expected = if let Some((pending, _)) = &pending {
                    pending
                        .inputs
                        .get(&name)
                        .cloned()
                        .ok_or_else(locations::invalid)?
                } else {
                    let directory = directory.as_ref().ok_or_else(locations::invalid)?;
                    ObjectRef {
                        owner: spec.dataset().clone(),
                        path: directory.child(&format!("{name}.parquet"))?.as_string()?,
                        bytes: 0, // Local copy_verified derives the retained length.
                        digest: input.digest,
                    }
                };
                #[cfg(all(not(test), not(feature = "test-util")))]
                if remote
                    && matches!(spec, OperationRequest::Build(build) if build.base.is_none())
                    && (spec.dataset().as_str().starts_with("catalog-")
                        || spec.dataset().as_str().starts_with("raw-")
                        || spec.dataset().as_str().starts_with("obs-")
                        || spec.dataset().as_str().starts_with("source-observations-"))
                {
                    let local_ref = ObjectRef {
                        owner: expected.owner.clone(),
                        path: input.path.clone(),
                        bytes: expected.bytes,
                        digest: expected.digest.clone(),
                    };
                    if storage::verify_object(&local_ref).is_ok() {
                        outputs.push(expected);
                        continue;
                    }
                }
                outputs.push(
                    self.inner
                        .objects
                        .admit_source(
                            &expected,
                            source,
                            &lifecycle.budget,
                            pending.as_ref().is_some_and(|(_, resuming)| *resuming),
                        )
                        .await?,
                );
            }
            if remote {
                stop_after(stop, "source-transfer")?;
            }
            let phase = PhaseResult {
                operation: spec.operation().clone(),
                phase: "admission".into(),
                request_digest: request.to_owned(),
                eligibility: operation.eligibility,
                outputs,
                table: None,
                rows: 0,
                changes: None,
                coverage: None,
                metrics: WriteMetrics::default(),
                dependencies: Some(PhaseDependencies::default()),
            };
            let protections = self.request_protections(&view, spec.operation()).await?;
            self.complete(spec, operation, phase, protections).await
        })
        .await
    }

    async fn process_source_phase(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        request: &str,
        admission: &Retained<PhaseResult>,
    ) -> Result<(Retained<PhaseResult>, bool)> {
        if let Some(phase) = self.phase(spec, operation, "processing").await? {
            return Ok((phase, false));
        }
        let lifecycle = &self.inner.lifecycle;
        let phase = crate::progress::phase(spec.dataset(), spec.operation(), "processing", async {
            #[cfg(feature = "test-util")]
            work_started_async("processing").await;
            let (prepared, update_source, _capture_memory) = if let OperationRequest::Build(build) =
                spec
                && build.base.is_none()
            {
                (
                    self.process_initial_build(build, operation, admission)
                        .await?,
                    None,
                    None,
                )
            } else {
                // Unacknowledged work starts in a fresh protected attempt. Completed
                // processing resolves above before any writer or input query.
                let directory = self.reserve_location(spec, operation, "processing").await?;
                let (_capture_memory, update_source) = if admission::captures_source(spec) {
                    let view = lifecycle.open(spec.dataset()).await?;
                    let memory = self.admission_memory(&view, spec.operation()).await?;
                    let source = self
                        .admitted_dependencies(&view, spec.operation())
                        .await?
                        .and_then(|dependencies| dependencies.update_source);
                    (Some(memory), source)
                } else {
                    (None, None)
                };
                let prepared = match spec {
                    OperationRequest::Build(build) => {
                        self.process_build(build, directory.local_path()?, admission)
                            .await?
                    }
                    OperationRequest::Update(update) => {
                        self.process_update(update, &directory, admission, update_source.as_ref())
                            .await?
                    }
                    OperationRequest::Schema(schema) => {
                        Box::pin(self.process_schema(schema, directory.local_path()?, operation))
                            .await?
                    }
                    OperationRequest::Evidence(evidence) => {
                        self.process_receipts(evidence, &directory, admission)
                            .await?
                    }
                    OperationRequest::Refresh(refresh) => {
                        self.process_refresh(
                            refresh,
                            &directory,
                            admission,
                            operation,
                            update_source.as_ref(),
                        )
                        .await?
                    }
                    OperationRequest::Backfill(_) => {
                        return Err(Error::refusal(
                            "invalid_operation",
                            "use the backfill service",
                        ));
                    }
                };
                (prepared, update_source, _capture_memory)
            };
            let mut outputs = prepared.table.objects.clone();
            if let Some(changes) = &prepared.changes {
                outputs.push(changes.base_manifest.clone());
                outputs.extend(changes.removed.objects.clone());
                outputs.extend(changes.added.objects.clone());
            }
            outputs.sort_by(|left, right| left.path.cmp(&right.path));
            outputs.dedup();
            let phase = PhaseResult {
                operation: spec.operation().clone(),
                phase: "processing".into(),
                request_digest: request.to_owned(),
                eligibility: operation.eligibility,
                outputs,
                rows: prepared.table.rows,
                table: Some(prepared.table),
                changes: prepared.changes,
                coverage: admission.coverage.clone(),
                metrics: prepared.metrics,
                dependencies: Some(PhaseDependencies {
                    input_sources: admission
                        .dependencies
                        .as_ref()
                        .map(|dependencies| dependencies.input_sources.clone())
                        .unwrap_or_default(),
                    admitted_inputs: spec
                        .inputs()
                        .keys()
                        .cloned()
                        .zip(admission.outputs.iter().cloned())
                        .collect(),
                    base_manifest: operation.base_manifest.clone(),
                    schema_metadata: prepared.schema_input.into_iter().collect(),
                    tables: update_source
                        .as_ref()
                        .map(NativeSnapshotRef::from)
                        .into_iter()
                        .collect(),
                }),
            };
            let view = lifecycle.open(spec.dataset()).await?;
            let mut protections = self.request_protections(&view, spec.operation()).await?;
            ReferenceProtections::inherit(&mut protections, admission.protections.as_ref())?;
            self.complete(spec, operation, phase, protections).await
        })
        .await?;
        Ok((phase, true))
    }

    // Construct the large future outside run_internal's poll frame, so its
    // temporary construction storage does not remain on every nested poll.
    pub(in crate::engine) fn prepare_and_select<'a>(
        &'a self,
        spec: &'a OperationRequest,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<ReleasePin>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            validate_request(spec)?;
            let request = fingerprint(spec)?;
            let lifecycle = &self.inner.lifecycle;
            let operation = match self.admit_or_resume_operation(spec, &request).await? {
                AdmissionOutcome::Completed(result) => return Ok(result),
                AdmissionOutcome::Admitted(operation) => operation,
            };
            #[cfg(all(not(test), not(feature = "test-util")))]
            if stop.is_none()
                && matches!(
                    spec,
                    OperationRequest::Build(_) | OperationRequest::Update(_)
                )
            {
                lifecycle.begin_deferred(spec.dataset()).await?;
            }

            // Locators can change on a material-identical retry. Preserve the admitted
            // request as execution provenance while using caller locators for missing copies.
            let captured_request: OperationRequest = lifecycle
                .get(
                    &lifecycle.open(spec.dataset()).await?,
                    &format!("request/{}", spec.operation()),
                )
                .await?
                .ok_or_else(|| {
                    Error::refusal("corrupt_phase", "operation has no admitted request")
                })?;

            // Readback is an independent retained promise. Earlier phase retirement
            // must not force its already qualified values through preparation again.
            if let Some(retained) = self
                .try_reuse_retained_readback(spec, &operation, &captured_request)
                .await?
            {
                stop_after(stop, "readback")?;
                return self.select_prepared(spec, &operation, retained, stop).await;
            }

            let PreparedSource {
                admission,
                processing,
                #[cfg(all(not(test), not(feature = "test-util")))]
                freshly_processed,
            } = self
                .prepare_source(spec, &operation, &request, stop)
                .await?;

            let processed = processing.table.clone().ok_or_else(|| {
                Error::refusal("corrupt_phase", "processing completion has no table pin")
            })?;
            let base = if let Some(base) = spec.base() {
                Some(self.release_admitted(spec.dataset(), base).await?)
            } else {
                None
            };
            let evidence_only = matches!(spec, OperationRequest::Evidence(_));
            let table = if evidence_only {
                base.as_ref()
                    .and_then(|base| base.dataset.groups.get("source"))
                    .cloned()
                    .ok_or_else(|| {
                        Error::refusal("invalid_evidence", "captured result lacks source values")
                    })?
            } else {
                processed.clone()
            };
            let related = if let OperationRequest::Refresh(refresh) = spec {
                Some(
                    self.prepare_evidence(refresh, &operation, &processing, stop)
                        .await?,
                )
            } else if evidence_only {
                base.as_ref()
                    .and_then(|base| base.dataset.related.as_ref())
                    .map(|related| {
                        Ok::<_, Error>(Retained {
                            value: (**related).clone(),
                            protections: processing
                                .protections
                                .as_ref()
                                .map(ReferenceProtections::duplicate)
                                .transpose()?,
                        })
                    })
                    .transpose()?
            } else {
                None
            };
            let receipts = if evidence_only {
                Some(Retained {
                    value: evidence::PreparedReceipts {
                        result: crate::evidence::EvidenceResult {
                            source: (&table).into(),
                            table: processed,
                        },
                        changes: processing.changes.clone(),
                    },
                    protections: processing
                        .protections
                        .as_ref()
                        .map(ReferenceProtections::duplicate)
                        .transpose()?,
                })
            } else if let Some(base) = &base {
                self.inherit_receipts(spec, &operation, &processing, base, stop)
                    .await?
            } else {
                None
            };
            let mut checks = verification::Checks::routine(self.inner.objects.clone());
            let validated = self
                .validate_source_and_dependents(
                    spec,
                    &operation,
                    &table,
                    base.as_ref(),
                    evidence_only,
                    &processing,
                    related.as_ref(),
                    receipts.as_ref(),
                    #[cfg(all(not(test), not(feature = "test-util")))]
                    freshly_processed,
                    &mut checks,
                )
                .await?;
            stop_after(stop, "validation")?;
            Box::pin(
                self.prepare_changes(
                    spec,
                    &operation,
                    base.as_ref(),
                    if evidence_only {
                        None
                    } else {
                        processing.changes.as_ref()
                    },
                    receipts
                        .as_ref()
                        .and_then(|receipts| receipts.changes.as_ref()),
                ),
            )
            .await?;
            let protection_view = lifecycle.open(spec.dataset()).await?;
            let mut readback_protections = self
                .request_protections(&protection_view, spec.operation())
                .await?;
            ReferenceProtections::inherit(
                &mut readback_protections,
                processing.protections.as_ref(),
            )?;
            ReferenceProtections::inherit(
                &mut readback_protections,
                validated.protections.as_ref(),
            )?;
            if let Some(related) = &related {
                ReferenceProtections::inherit(
                    &mut readback_protections,
                    related.protections.as_ref(),
                )?;
            }
            if let Some(receipts) = &receipts {
                ReferenceProtections::inherit(
                    &mut readback_protections,
                    receipts.protections.as_ref(),
                )?;
            }

            let inputs = self
                .resolve_release_inputs(spec, base.as_ref(), admission.as_ref(), &processing)
                .await?;
            let (groups, computed) = self
                .prepare_inherited_groups(
                    spec,
                    &captured_request,
                    &operation,
                    &table,
                    base.as_ref(),
                    evidence_only,
                    &mut readback_protections,
                    &mut checks,
                    stop,
                )
                .await?;
            stop_after(stop, "groups")?;
            let partition_by = self
                .resolve_release_partition_by(&captured_request, &groups, base.as_ref())
                .await?;
            let mut dataset = DatasetPin {
                format: operation.result_format,
                dataset: spec.dataset().clone(),
                version: spec.operation().clone(),
                groups,
                computed,
                inputs,
                relationship: inputs::relationship(spec, base.as_ref().map(|base| &base.dataset)),
                related: related.map(|related| Box::new(related.value)),
                evidence: receipts.map(|receipts| Box::new(receipts.value.result)),
                definition: captured_request.result_definition().to_owned(),
                eligibility: operation.eligibility,
                boundary: operation.boundary,
                request: captured_request.clone(),
                parent_manifest: base.as_ref().map(|base| base.manifest.clone()),
                processor: PROCESSOR.into(),
                validation: VerificationLevel::Routine,
                partition_by,
            };
            checks.compose();
            Box::pin(self.qualify_corrections(
                &captured_request,
                &operation,
                &mut dataset,
                &mut readback_protections,
                &mut checks,
            ))
            .await?;
            backfill::verify_groups(&dataset, &lifecycle.budget, &mut checks).await?;
            evidence::verify(&dataset, &lifecycle.budget, &mut checks).await?;
            let release = self
                .commit_or_reuse_readback(
                    spec,
                    &operation,
                    dataset,
                    readback_protections,
                    &mut checks,
                    stop,
                )
                .await?;
            stop_after(stop, "readback")?;
            self.select_prepared(spec, &operation, release, stop).await
        })
    }

    /// Two-phase admission loop:
    /// 1. Acquires per-dataset locks (`lock_inputs`), verifies request/base/schema
    ///    eligibility, and if no `PreparedAdmission` has been constructed yet, drops
    ///    the dataset locks (`drop(guards)`) while `protect_admission` performs owner
    ///    reference-protection commits and remote manifest reads.
    /// 2. Re-acquires the expanded dataset lock set (`prepared.datasets(...)`) and
    ///    checks `protected.is_current(&view)?` against the consumer's metadata
    ///    location; if a concurrent commit advanced the view while locks were dropped,
    ///    `prepared` is cleared and the loop repeats against fresh state.
    ///
    /// Composition operations (`Schema`, `Evidence`) preserve the existing source
    /// coverage boundary (`authority.selected.boundary`), record the current
    /// `authority.boundary` in `admission_fence`, and do not mutate `"authority"`,
    /// whereas source-mutating operations (`Build`, `Update`, `Refresh`, `Backfill`)
    /// increment `authority.boundary` and commit the updated `"authority"` record.
    async fn admit_or_resume_operation(
        &self,
        spec: &OperationRequest,
        request: &str,
    ) -> Result<AdmissionOutcome> {
        let lifecycle = &self.inner.lifecycle;
        let mut prepared: Option<admission::PreparedAdmission> = None;
        loop {
            let datasets = prepared.as_ref().map_or_else(
                || BTreeSet::from([spec.dataset().clone()]),
                |prepared| prepared.datasets(spec.dataset()),
            );
            let guards = self.lock_inputs(datasets).await;
            let view = self.request_view(spec).await?;
            self.check_continuous_operation_id(&view, spec.operation())
                .await?;
            let old: Option<Operation> = lifecycle
                .get(&view, &format!("operation/{}", spec.operation()))
                .await?;
            if old.as_ref().is_some_and(|o| o.request_digest != request) {
                return Err(conflicting_operation());
            }
            if let Some(result) = lifecycle
                .get(&view, &format!("result/{}", spec.operation()))
                .await?
            {
                return Ok(AdmissionOutcome::Completed(result));
            }
            self.check_not_abandoned(&view, spec.operation()).await?;
            if let Some(old) = old {
                let mut authority: Authority =
                    lifecycle.get(&view, "authority").await?.unwrap_or_default();
                self.validate_operation_eligibility(&view, &mut authority, spec, false)
                    .await?;
                check_authority(&authority, &old)?;
                return Ok(AdmissionOutcome::Admitted(old));
            }
            if matches!(
                spec,
                OperationRequest::Build(_)
                    | OperationRequest::Update(_)
                    | OperationRequest::Refresh(_)
            ) {
                self.check_source_intake(&view).await?;
            }
            let mut decoding = lifecycle
                .rows(&view)
                .await?
                .reserve_decoded(&lifecycle.budget, &["authority"])?;
            let mut authority: Authority =
                lifecycle.get(&view, "authority").await?.unwrap_or_default();
            self.validate_operation_eligibility(&view, &mut authority, spec, true)
                .await?;
            let Some(protected) = &prepared else {
                // Unbased builds retain only the ordering receipt of the
                // previous selection; no previous payload is consumed.
                let base = spec.base().and_then(|_| authority.release.take());
                let update_source = if admission::captures_source(spec) {
                    let base = base.as_ref().ok_or_else(|| {
                        Error::refusal("invalid_base", "incremental write has no base result")
                    })?;
                    let canonical = base.dataset.groups.get("source").ok_or_else(|| {
                        Error::refusal("invalid_base", "base has no source table")
                    })?;
                    let key = format!(
                        "representation/{}/source",
                        canonical.logical_metadata().digest
                    );
                    decoding.merge(
                        lifecycle
                            .rows(&view)
                            .await?
                            .reserve_decoded(&lifecycle.budget, &[&key])?,
                    );
                    // The fallback copies the canonical pin; selected records
                    // have their own decoding allowance above.
                    decoding.merge(
                        lifecycle.budget.reserve(
                            inputs::metadata_size(
                                canonical,
                                lifecycle.budget.limits.max_metadata_bytes,
                            )?
                            .saturating_mul(4),
                        )?,
                    );
                    Some(
                        self.representation_pin(&view, &base.id, "source", canonical)
                            .await?,
                    )
                } else {
                    None
                };
                let boundary = view.table.metadata_location_result()?.to_owned();
                drop(guards);
                prepared = Some(
                    self.protect_admission(spec, boundary, base, update_source, decoding)
                        .await?,
                );
                continue;
            };
            if !protected.is_current(&view)? {
                // Repeat capture and eligibility against fresh state;
                // committed owner grants remain reusable. A compatible
                // concurrent operation must not force a caller retry.
                prepared = None;
                continue;
            }
            if let Some(coverage) = &protected.dependencies.coverage {
                self.guard_inputs(coverage, None, RefreshPolicy::Captured)
                    .await?;
            }
            let composition = matches!(
                spec,
                OperationRequest::Schema(_) | OperationRequest::Evidence(_)
            );
            let boundary = if composition {
                authority
                    .selected
                    .as_ref()
                    .ok_or_else(|| {
                        Error::refusal("no_release", "schema operation needs selected source")
                    })?
                    .boundary
            } else {
                authority.boundary = authority.boundary.checked_add(1).ok_or_else(|| {
                    Error::refusal("revision_exhausted", "source boundary overflow")
                })?;
                authority.boundary
            };
            let operation = Operation {
                request_digest: request.to_owned(),
                eligibility: authority.eligibility,
                epoch: authority.epoch,
                boundary,
                admission_fence: composition.then_some(authority.boundary),
                result_format: FORMAT,
                definition: authority.definition.clone(),
                base: spec.base().cloned(),
                base_manifest: captured_manifest(&authority, spec).cloned(),
                schema_key: self.schema_key(&view, &authority, spec).await?,
                require_current_boundary: matches!(spec, OperationRequest::Schema(_))
                    || matches!(spec, OperationRequest::Build(build) if build.base.is_some()),
            };
            let mut records = vec![
                record(format!("operation/{}", spec.operation()), &operation)?,
                record(format!("request/{}", spec.operation()), spec)?,
                record(
                    format!("runtime/{}", spec.operation()),
                    serde_json::json!({
                        "processor": PROCESSOR, "package_version": env!("CARGO_PKG_VERSION"),
                        "dependency_lock_blake3": blake3::hash(include_bytes!("../../Cargo.lock")).to_hex().as_str(),
                        "limits": lifecycle.budget.limits,
                    }),
                )?,
            ];
            if !composition {
                records.push(record("authority", &authority)?);
            }
            protected.records(&view, spec, &mut records)?;
            lifecycle.commit(&view, records).await?;
            return Ok(AdmissionOutcome::Admitted(operation));
        }
    }

    async fn validate_operation_eligibility(
        &self,
        view: &View,
        authority: &mut Authority,
        spec: &OperationRequest,
        is_new_admission: bool,
    ) -> Result<()> {
        self.check_backend_selection(authority, spec)?;
        if authority.definition.is_empty() {
            authority.definition = spec.definition().to_owned();
        }
        check_definition(authority, spec)?;
        check_base(authority, spec.base())?;
        check_update_eligibility(authority, spec)?;
        check_group_rebuild(authority, spec)?;
        self.check_incremental(authority, spec).await?;
        self.check_schema(view, authority, spec, is_new_admission)
            .await
    }

    async fn try_reuse_retained_readback(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        captured_request: &OperationRequest,
    ) -> Result<Option<Retained<ReleasePin>>> {
        let lifecycle = &self.inner.lifecycle;
        let retained_view = lifecycle.open(spec.dataset()).await?;
        let Some(completed) = lifecycle
            .get::<ValidatedReadback>(&retained_view, &format!("readback/{}", spec.operation()))
            .await?
        else {
            return Ok(None);
        };
        let current_validator = completed.current();
        let release = completed.release;
        let protections = self
            .retained_protections(&retained_view, &format!("readback/{}", spec.operation()))
            .await?;
        if !release.verified
            || &release.id != spec.operation()
            || &release.dataset.dataset != spec.dataset()
            || release.dataset.version != release.id
            || release.dataset.request != *captured_request
            || release.dataset.definition != captured_request.result_definition()
            || release.dataset.format != operation.result_format
            || release.dataset.eligibility != operation.eligibility
            || release.dataset.boundary != operation.boundary
        {
            return Err(Error::refusal(
                "corrupt_phase",
                "retained readback differs from its admitted operation",
            ));
        }
        let retained = if current_validator {
            Retained {
                value: release,
                protections,
            }
        } else {
            self.revalidate_readback(
                spec,
                operation,
                Retained {
                    value: release,
                    protections,
                },
            )
            .await?
        };
        // Refresh selection checks the complete dependency graph below.
        // Other selections check retained availability here, once.
        if current_validator && !matches!(spec, OperationRequest::Refresh(_)) {
            check_release_available(&self.inner.objects, &retained, &lifecycle.budget).await?;
        }
        Ok(Some(retained))
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_source_and_dependents(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        table: &TablePin,
        base: Option<&ReleasePin>,
        evidence_only: bool,
        processing: &Retained<PhaseResult>,
        related: Option<&Retained<RelatedResult>>,
        receipts: Option<&Retained<evidence::PreparedReceipts>>,
        #[cfg(all(not(test), not(feature = "test-util")))] freshly_processed: bool,
        checks: &mut verification::Checks,
    ) -> Result<Retained<PhaseResult>> {
        let lifecycle = &self.inner.lifecycle;
        let validation_dependencies = processing.dependencies.clone().map(|mut dependencies| {
            if let Some(related) = related {
                dependencies.tables.push((&related.evidence).into());
            }
            if let Some(receipts) = receipts {
                dependencies.tables.push((&receipts.result.table).into());
            }
            dependencies
        });
        if let Some(validated) = self.phase(spec, operation, "validation").await? {
            if validated.table != processing.table
                || validated.outputs != processing.outputs
                || validated.rows != processing.rows
                || validated.changes != processing.changes
                || validated.coverage != processing.coverage
                || validated.dependencies != validation_dependencies
            {
                return Err(self.phase_generation_conflict(spec, operation, "validation")?);
            }
            return Ok(validated);
        }
        crate::progress::phase(spec.dataset(), spec.operation(), "validation", async {
            #[cfg(feature = "test-util")]
            work_started_async("validation").await;
            #[cfg(test)]
            self.inner
                .routine_checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(base) = base {
                self.trust_current_readback(base, checks).await?;
            }
            #[cfg(all(not(test), not(feature = "test-util")))]
            let skip_table_check = freshly_processed
                && !evidence_only
                && self.inner.objects.is_remote()
                && base.is_none()
                && related.is_none()
                && processing.changes.is_none();
            #[cfg(any(test, feature = "test-util"))]
            let skip_table_check = false;
            if !skip_table_check {
                #[cfg(all(not(test), not(feature = "test-util")))]
                if freshly_processed && !evidence_only && self.inner.objects.is_remote() {
                    checks.trust_pin(table, &lifecycle.budget).await?;
                }
                checks.table(table, &lifecycle.budget).await?;
            }
            Box::pin(verify_changes(
                spec,
                table,
                if evidence_only {
                    None
                } else {
                    processing.changes.as_ref()
                },
                &lifecycle.budget,
                checks,
            ))
            .await?;
            if let Some(receipts) = receipts {
                Box::pin(evidence::verify_prepared(
                    spec,
                    table,
                    base.ok_or_else(|| {
                        Error::refusal("invalid_evidence", "receipts require a captured base")
                    })?,
                    receipts,
                    &lifecycle.budget,
                    checks,
                ))
                .await?;
            }
            if let Some(related) = related {
                Box::pin(refresh::verify_related(
                    table,
                    related,
                    &lifecycle.budget,
                    checks,
                ))
                .await?;
            }
            let phase = PhaseResult {
                phase: "validation".into(),
                dependencies: validation_dependencies,
                ..processing.value.clone()
            };
            let mut protections = processing
                .protections
                .as_ref()
                .map(ReferenceProtections::duplicate)
                .transpose()?;
            if let Some(related) = related {
                ReferenceProtections::inherit(&mut protections, related.protections.as_ref())?;
            }
            if let Some(receipts) = receipts {
                ReferenceProtections::inherit(&mut protections, receipts.protections.as_ref())?;
            }
            self.complete(spec, operation, phase, protections).await
        })
        .await
    }

    /// Resolves the `inputs` map for the resulting `DatasetPin`.
    ///
    /// A full `Build` or `Refresh` replaces the dataset's input lineage completely,
    /// whereas `Update`, `Schema`, and `Evidence` inherit the base release's `inputs`
    /// map. For an `Update`, the previous `Update`'s transient delta input keys
    /// (`update:replacements`, `update:deletions`) must be removed before inserting
    /// the current update's admitted inputs, while preserving the underlying `Build`
    /// input and any `Evidence` inputs. Because `Evidence`, `Backfill`, or `Schema`
    /// releases may have been committed between the previous `Update` and the current
    /// `Update`, we walk backward along `parent_manifest` across those non-source-delta
    /// releases (bounded by `visited` for cycle safety) until we either strip the
    /// most recent `Update`'s input keys or stop at a `Build`/`Refresh` root.
    async fn resolve_release_inputs(
        &self,
        spec: &OperationRequest,
        base: Option<&ReleasePin>,
        admission: Option<&Retained<PhaseResult>>,
        processing: &Retained<PhaseResult>,
    ) -> Result<BTreeMap<String, ObjectRef>> {
        let mut inputs = base
            .filter(|_| {
                matches!(
                    spec,
                    OperationRequest::Update(_)
                        | OperationRequest::Schema(_)
                        | OperationRequest::Evidence(_)
                )
            })
            .map(|base| base.dataset.inputs.clone())
            .unwrap_or_default();
        if matches!(spec, OperationRequest::Update(_))
            && let Some(base) = base
        {
            let mut prior_cursor = base.dataset.clone();
            let mut visited = BTreeSet::from([prior_cursor.version.clone()]);
            loop {
                match &prior_cursor.request {
                    OperationRequest::Update(_) => {
                        for name in prior_cursor.request.inputs().keys() {
                            inputs.remove(&prior_cursor.request.input_key(name));
                        }
                        break;
                    }
                    OperationRequest::Evidence(_)
                    | OperationRequest::Backfill(_)
                    | OperationRequest::Schema(_) => {
                        let Some(prev_ref) = &prior_cursor.parent_manifest else {
                            break;
                        };
                        let memory = Arc::new(
                            self.inner
                                .lifecycle
                                .budget
                                .reserve(self.inner.lifecycle.budget.limits.max_metadata_bytes)?,
                        );
                        let prev: DatasetPin = inputs::read_manifest(
                            &self.inner.objects,
                            prev_ref,
                            &self.inner.lifecycle.budget,
                            &mut 0,
                            &memory,
                        )
                        .await?;
                        if !visited.insert(prev.version.clone()) {
                            break;
                        }
                        prior_cursor = prev;
                    }
                    OperationRequest::Build(_) | OperationRequest::Refresh(_) => break,
                }
            }
        }
        if let Some(dependencies) = &processing.dependencies {
            for (name, input) in &dependencies.admitted_inputs {
                inputs.insert(spec.input_key(name), input.clone());
            }
        } else {
            // Earlier completions remain readable while cleanup is disabled.
            // Their inferred input edge is not sufficient removal authority.
            let admission = admission.ok_or_else(|| {
                Error::refusal(
                    "phase_dependencies_unknown",
                    "legacy processing lacks its original admission",
                )
            })?;
            for ((name, _), input) in spec.inputs().iter().zip(&admission.outputs) {
                inputs.insert(spec.input_key(name), input.clone());
            }
        }
        Ok(inputs)
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_inherited_groups(
        &self,
        spec: &OperationRequest,
        captured_request: &OperationRequest,
        operation: &Operation,
        table: &TablePin,
        base: Option<&ReleasePin>,
        evidence_only: bool,
        readback_protections: &mut Option<ReferenceProtections>,
        checks: &mut verification::Checks,
        stop: Option<&str>,
    ) -> Result<(BTreeMap<String, TablePin>, BTreeMap<String, ComputedGroup>)> {
        if evidence_only {
            let base =
                base.ok_or_else(|| Error::refusal("invalid_evidence", "receipts require a base"))?;
            return Ok((base.dataset.groups.clone(), base.dataset.computed.clone()));
        }
        let mut groups = BTreeMap::from([("source".to_owned(), table.clone())]);
        let mut computed = BTreeMap::new();
        let Some(base) = base else {
            return Ok((groups, computed));
        };
        let lifecycle = &self.inner.lifecycle;
        for (name, previous) in &base.dataset.computed {
            self.check(
                &lifecycle.open(spec.dataset()).await?,
                operation,
                spec.operation(),
            )
            .await?;
            let input_fields = backfill::inherited_inputs(
                spec,
                previous,
                &base.dataset.groups["source"],
                table,
                &lifecycle.budget,
            )
            .await?;
            let prepared = Box::pin(self.prepare_group(
                captured_request,
                operation,
                table,
                readback_protections.as_ref(),
                &previous.definition,
                Some(&input_fields),
                None,
                checks,
            ))
            .await?;
            ReferenceProtections::inherit(readback_protections, prepared.protections.as_ref())?;
            groups.insert(name.clone(), prepared.table);
            computed.insert(
                name.clone(),
                ComputedGroup {
                    qualification: None,
                    origins: prepared.origins,
                    definition: previous.definition.clone(),
                    input_fields: prepared.input_fields,
                    request: captured_request.clone(),
                    native_field_ids: prepared.native_field_ids,
                    source_metadata: table.metadata.clone(),
                    base_manifest: base.manifest.clone(),
                },
            );
            stop_after(stop, &format!("group/{name}"))?;
        }
        if !computed.is_empty() {
            let lock = self.lock(spec.dataset()).await;
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            self.check_completion(&view, operation, spec.operation())
                .await?;
            let key = format!("groups/{}", spec.operation());
            let completed: BTreeMap<_, _> = groups
                .iter()
                .filter(|(name, _)| name.as_str() != "source")
                .map(|(name, pin)| (name.clone(), pin.metadata.clone()))
                .collect();
            let existing = self
                .active_completion::<CompletedGroups>(&view, &key)
                .await?;
            if let Some(existing) = existing {
                let matches = match existing {
                    CompletedGroups::Captured { outputs, source } => {
                        outputs == completed && source == table.into()
                    }
                    CompletedGroups::Legacy(outputs) => outputs == completed,
                };
                if !matches {
                    return Err(Error::refusal(
                        "phase_generation_conflict",
                        format!(
                            "retained completion {key} belongs to another physical generation; explicitly retire it before recomputing its dependencies"
                        ),
                    ));
                }
                let protections = self.retained_protections(&view, &key).await?;
                ReferenceProtections::inherit(readback_protections, protections.as_ref())?;
            } else {
                let records = completion_records(
                    spec.dataset(),
                    &view,
                    &key,
                    &CompletedGroups::Captured {
                        outputs: completed,
                        source: table.into(),
                    },
                    readback_protections.as_ref(),
                )?;
                lifecycle.commit(&view, records).await?;
            }
        }
        Ok((groups, computed))
    }

    async fn resolve_release_partition_by(
        &self,
        captured_request: &OperationRequest,
        groups: &BTreeMap<String, TablePin>,
        base: Option<&ReleasePin>,
    ) -> Result<Vec<PartitionFieldSpec>> {
        let Some(source) = groups.get("source") else {
            return Ok(Vec::new());
        };
        let declared = match captured_request {
            OperationRequest::Build(build) if !build.partition_by.is_empty() => {
                Some(build.partition_by.as_slice())
            }
            _ => base.map(|b| b.dataset.partition_by.as_slice()),
        };
        if declared.is_some_and(|d| !d.is_empty())
            || base.is_some_and(|b| !b.dataset.partition_by.is_empty())
        {
            let native = self
                .inner
                .objects
                .open_pin(source, &self.inner.lifecycle.budget)
                .await?;
            Ok(storage::partition_specs_from_metadata(
                native.metadata(),
                declared,
            ))
        } else {
            Ok(Vec::new())
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Single-use return value from operation admission phase"
)]
enum AdmissionOutcome {
    Completed(ReleasePin),
    Admitted(Operation),
}
