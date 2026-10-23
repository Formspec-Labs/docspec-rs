//! Readback persistence, selection promotion, and durable phase completion.

use super::*;

impl Engine {
    pub(super) async fn commit_or_reuse_readback(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        dataset: DatasetPin,
        readback_protections: Option<ReferenceProtections>,
        checks: &mut verification::Checks,
        stop: Option<&str>,
    ) -> Result<Retained<ReleasePin>> {
        // Readback has an operation-specific manifest because coverage can advance
        // while the same expensive materialized values remain reusable.
        let lifecycle = &self.inner.lifecycle;
        let readback_key = format!("readback/{}", spec.operation());
        let readback_view = lifecycle.open(spec.dataset()).await?;
        if let Some(completed) = lifecycle
            .get::<ValidatedReadback>(&readback_view, &readback_key)
            .await?
        {
            completed.require_current()?;
            let release = completed.release;
            if release.dataset != dataset {
                return Err(Error::refusal(
                    "corrupt_phase",
                    "retained readback differs from the prepared result",
                ));
            }
            check_release_available_with_checks(
                &self.inner.objects,
                &release,
                &lifecycle.budget,
                checks,
            )
            .await?;
            return Ok(Retained {
                value: release,
                protections: self
                    .retained_protections(&readback_view, &readback_key)
                    .await?,
            });
        }
        let lock = self.lock(spec.dataset()).await;
        crate::progress::phase(spec.dataset(), spec.operation(), "readback", async {
            #[cfg(feature = "test-util")]
            work_started_async("readback").await;
            let manifest = self.create_release(spec, operation, &dataset, stop).await?;
            let release = ReleasePin {
                id: spec.operation().clone(),
                dataset,
                manifest,
                verified: true,
            };
            // Full refresh result availability owns its exact dependencies,
            // separately from execution's retained captured-base grants.
            // Acquire owner grants before taking the consumer commit lock.
            let readback_protections = if matches!(spec, OperationRequest::Refresh(refresh)
                if refresh.mode == RefreshMode::Full)
                && release.dataset.evidence.is_none()
            {
                Some(
                    self.protect_releases(spec.dataset(), std::slice::from_ref(&release))
                        .await?,
                )
            } else {
                readback_protections
            };
            // Prepared tables were validated at their durable phase boundaries.
            // Equality above verifies serialization without scanning them again.
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            self.check_completion(&view, operation, spec.operation())
                .await?;
            Ok(
                if let Some(completed) = lifecycle
                    .get::<ValidatedReadback>(&view, &readback_key)
                    .await?
                {
                    completed.require_current()?;
                    Retained {
                        value: completed.release,
                        protections: self.retained_protections(&view, &readback_key).await?,
                    }
                } else {
                    let records = completion_records(
                        spec.dataset(),
                        &view,
                        &readback_key,
                        &ValidatedReadback::stamp(&release),
                        readback_protections.as_ref(),
                    )?;
                    lifecycle.commit(&view, records).await?;
                    Retained {
                        value: release,
                        protections: readback_protections,
                    }
                },
            )
        })
        .await
    }

    pub(in crate::engine) fn select_prepared<'a>(
        &'a self,
        spec: &'a OperationRequest,
        operation: &'a Operation,
        release: Retained<ReleasePin>,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<ReleasePin>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            if let OperationRequest::Refresh(refresh) = spec {
                let selected = self.select_refresh(refresh, operation, &release).await?;
                stop_after(stop, "selection")?;
                return Ok(selected);
            }
            let dependencies = if release.dataset.related.is_some() {
                Some(verification::available_graph(&release, &self.inner.lifecycle.budget).await?)
            } else {
                None
            };
            let datasets = dependencies.as_ref().map_or_else(
                || BTreeSet::from([spec.dataset().clone()]),
                verification::DependencyGraph::datasets,
            );
            let _guards = self.lock_inputs(datasets).await;
            let lifecycle = &self.inner.lifecycle;
            let view = lifecycle.open(spec.dataset()).await?;
            if let Some(completed) = lifecycle
                .get(&view, &format!("result/{}", spec.operation()))
                .await?
            {
                return Ok(completed);
            }
            let authority = self.check(&view, operation, spec.operation()).await?;
            self.check_backend_selection(&authority, spec)?;
            if authority
                .selected
                .as_ref()
                .is_some_and(|p| p.boundary > operation.boundary)
            {
                return Err(Error::refusal(
                    "coverage_regression",
                    "a newer captured input boundary is already selected",
                ));
            }
            check_group_rebuild(&authority, spec)?;
            self.check_incremental(&authority, spec).await?;
            if let Some(dependencies) = &dependencies {
                self.guard_ancestors(dependencies, &release).await?;
            }
            if matches!(spec, OperationRequest::Schema(_)) {
                self.check_cutover_inputs(&view, spec.dataset(), Some(spec.operation()))
                    .await?;
            }
            self.select(&view, authority, &release).await?;
            stop_after(stop, "selection")?;
            Ok(release.value)
        })
    }

    pub(in crate::engine) async fn check(
        &self,
        view: &View,
        operation: &Operation,
        id: &Id,
    ) -> Result<Authority> {
        self.check_not_abandoned(view, id).await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        check_authority(&authority, operation)?;
        Ok(authority)
    }

    pub(in crate::engine) async fn check_completion(
        &self,
        view: &View,
        operation: &Operation,
        id: &Id,
    ) -> Result<()> {
        self.check_not_abandoned(view, id).await?;
        let authority: Authority = self
            .inner
            .lifecycle
            .get(view, "authority")
            .await?
            .unwrap_or_default();
        // Finished work retains its original qualification. Changing the selected
        // source or eligibility fences reuse and promotion, not its acknowledgment.
        check_preparation(&authority, operation)
    }

    pub(in crate::engine) async fn phase(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &str,
    ) -> Result<Option<Retained<PhaseResult>>> {
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation, spec.operation()).await?;
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        let found: Option<PhaseResult> = self.active_completion(&view, &key).await?;
        if let Some(found) = found {
            if phase == "processing" {
                self.check_reused_build_schema(
                    &view,
                    spec,
                    operation.result_format,
                    operation.schema_key.as_ref(),
                    &found,
                )
                .await?;
            }
            let objects = self.inner.objects.clone();
            let handle = tokio::runtime::Handle::current();
            #[cfg(all(not(test), not(feature = "test-util")))]
            let skip_output_check = phase == "admission"
                && objects.is_remote()
                && matches!(spec, OperationRequest::Build(build) if build.base.is_none())
                && (spec.dataset().as_str().starts_with("catalog-")
                    || spec.dataset().as_str().starts_with("raw-")
                    || spec.dataset().as_str().starts_with("obs-")
                    || spec.dataset().as_str().starts_with("source-observations-"));
            #[cfg(any(test, feature = "test-util"))]
            let skip_output_check = false;
            // The worker owns the retained completion until its size checks finish,
            // including when the awaiting caller is canceled. No payload is read.
            let found = if skip_output_check {
                found
            } else {
                self.inner
                    .lifecycle
                    .budget
                    .blocking_admitted(move || {
                        for output in &found.outputs {
                            objects.check_available_admitted(output, &handle)?;
                        }
                        Ok(found)
                    })
                    .await?
            };
            tracing::info!(phase, operation = %spec.operation(), "reusing qualified phase");
            return Ok(Some(Retained {
                value: found,
                protections: self.retained_protections(&view, &key).await?,
            }));
        }
        Ok(None)
    }

    pub(in crate::engine) fn phase_generation_conflict(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: &str,
    ) -> Result<Error> {
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        Ok(Error::refusal(
            "phase_generation_conflict",
            format!(
                "retained completion {key} belongs to another physical generation; explicitly retire it before recomputing its dependencies"
            ),
        ))
    }

    pub(in crate::engine) async fn complete(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        phase: PhaseResult,
        protections: Option<ReferenceProtections>,
    ) -> Result<Retained<PhaseResult>> {
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check_completion(&view, operation, spec.operation())
            .await?;
        let key = phase_key(
            spec,
            &operation.request_digest,
            operation.eligibility,
            &phase.phase,
            operation.base_manifest.as_ref(),
            operation.schema_key.as_ref(),
        )?;
        // Another attempt may have completed the same deterministic phase. Preserve
        // that generation rather than relabeling or replacing its evidence.
        if let Some(completed) = self.active_completion::<PhaseResult>(&view, &key).await? {
            if matches!(phase.phase.as_str(), "validation" | "evidence" | "receipts")
                && (completed.dependencies != phase.dependencies
                    || (phase.phase == "validation" && completed.table != phase.table))
            {
                return Err(self.phase_generation_conflict(spec, operation, &phase.phase)?);
            }
            return Ok(Retained {
                value: completed,
                protections: self.retained_protections(&view, &key).await?,
            });
        }
        let records =
            completion_records(spec.dataset(), &view, &key, &phase, protections.as_ref())?;
        self.inner.lifecycle.commit(&view, records).await?;
        tracing::info!(phase = phase.phase, rows = phase.rows, operation = %spec.operation(), "phase durably complete");
        Ok(Retained {
            value: phase,
            protections,
        })
    }
}
