//! Native related results, retained contributors, and guarded forward input coverage.

use super::write::{Prepared, identifier as quote};
use super::*;
use datafusion::{common::Column, prelude::col};
use std::collections::BTreeSet;

mod verification;
pub(super) use verification::{verify_related, verify_related_with_inputs};

#[cfg(test)]
mod cardinality_tests;
#[cfg(test)]
mod inventory_tests;
#[cfg(test)]
mod plan_probe;

pub(super) const TUPLE: [&str; 4] = [
    "__docspec_left_file",
    "__docspec_left_row",
    "__docspec_right_file",
    "__docspec_right_row",
];

impl Engine {
    /// Plans retained related work over exact native inputs without reading their payloads.
    ///
    /// # Errors
    /// Refuses malformed recipes, conflicting operation identities, or stale output bases.
    pub async fn plan_refresh(&self, spec: &RefreshSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Refresh(Box::new(spec.clone())), memory)
            .await
    }

    /// Runs or resumes native joins or summaries with retained contributor evidence.
    ///
    /// # Errors
    /// Refuses inadmissible inputs, invalid recipes, exhausted capacity, and stale selection guards.
    pub async fn refresh(&self, spec: &RefreshSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(
            OperationRequest::Refresh(Box::new(spec.clone())),
            memory,
            None,
        )
        .await
    }

    /// Interrupts after a durable refresh handoff for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or an ordinary refresh refusal.
    #[cfg(feature = "test-util")]
    pub async fn refresh_until(&self, spec: &RefreshSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(
            OperationRequest::Refresh(Box::new(spec.clone())),
            memory,
            Some(phase),
        )
        .await
    }

    pub(super) fn admit_refresh<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        operation: &'a Operation,
    ) -> futures::future::BoxFuture<'a, Result<Retained<PhaseResult>>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            #[cfg(feature = "test-util")]
            work_started_async("admission").await;
            let budget = &self.inner.lifecycle.budget;
            let view = self.inner.lifecycle.open(&spec.dataset).await?;
            let _memory = self.admission_memory(&view, &spec.operation).await?;
            let (coverage, input_sources) =
                if let Some(captured) = self.admitted_dependencies(&view, &spec.operation).await? {
                    (
                        captured
                            .coverage
                            .ok_or_else(|| invalid("admitted refresh is missing input coverage"))?,
                        captured.input_sources,
                    )
                } else {
                    // Earlier requests predate protected admission and retain their
                    // existing capture behavior while physical cleanup stays disabled.
                    (
                        self.capture_inputs_admitted(&spec.inputs(), false).await?,
                        BTreeMap::new(),
                    )
                };
            let loaded = load_inputs(&coverage, &input_sources, budget).await?;
            match &spec.transform {
                RefreshTransform::Join(_) => {}
                RefreshTransform::Aggregate(aggregate) => {
                    aggregation::register_input(
                        &self.context(),
                        aggregate,
                        &loaded.borrowed(),
                        budget,
                    )
                    .await?;
                }
                RefreshTransform::ConnectedComponents(cc) => {
                    connected_components::register_input(
                        &self.context(),
                        cc,
                        &loaded.borrowed(),
                        budget,
                    )
                    .await?;
                }
            }
            let phase = PhaseResult {
                operation: spec.operation.clone(),
                phase: "admission".into(),
                request_digest: operation.request_digest.clone(),
                eligibility: operation.eligibility,
                outputs: coverage
                    .inputs
                    .values()
                    .map(|version| version.manifest.clone())
                    .collect(),
                table: None,
                rows: 0,
                changes: None,
                coverage: Some(coverage),
                metrics: WriteMetrics::default(),
                dependencies: Some(PhaseDependencies {
                    input_sources,
                    ..PhaseDependencies::default()
                }),
            };
            let protections = self.request_protections(&view, &spec.operation).await?;
            self.complete(
                &OperationRequest::Refresh(Box::new(spec.clone())),
                operation,
                phase,
                protections,
            )
            .await
        })
    }

    pub(super) fn process_refresh<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        admission: &'a PhaseResult,
        operation: &'a Operation,
        captured_source: Option<&'a TablePin>,
    ) -> futures::future::BoxFuture<'a, Result<Prepared>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            if spec.mode == RefreshMode::Incremental {
                return self
                    .process_incremental(spec, directory, admission, operation, captured_source)
                    .await;
            }
            let coverage = admission
                .coverage
                .as_ref()
                .ok_or_else(|| invalid("refresh admission phase is missing input coverage"))?;
            if coverage.inputs != spec.inputs() {
                return Err(invalid(
                    "admitted input coverage disagrees with refresh request inputs",
                ));
            }
            let input_sources = admission
                .dependencies
                .as_ref()
                .map(|dependencies| &dependencies.input_sources);
            let loaded = load_inputs(
                coverage,
                input_sources.unwrap_or(&BTreeMap::new()),
                &self.inner.lifecycle.budget,
            )
            .await?;
            let context = self.context();
            let frame = source_frame(
                &context,
                &spec.transform,
                &loaded.borrowed(),
                &self.inner.lifecycle.budget,
                true,
            )
            .await?;
            self.write_frame_at(&spec.dataset, directory, frame).await
        })
    }

    pub(super) fn prepare_evidence<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        operation: &'a Operation,
        processing: &'a Retained<PhaseResult>,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<Retained<RelatedResult>>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            let request = OperationRequest::Refresh(Box::new(spec.clone()));
            let dependencies = processing.dependencies.clone().map(|mut dependencies| {
                if spec.mode == RefreshMode::Full
                    && matches!(spec.transform, RefreshTransform::Join(_))
                    && let Some(table) = &processing.table
                {
                    dependencies.tables.push(table.into());
                }
                dependencies
            });
            let evidence = if let Some(phase) = self.phase(&request, operation, "evidence").await? {
                if phase.dependencies != dependencies
                    || phase.coverage != processing.coverage
                    || phase.changes != processing.changes
                {
                    return Err(self.phase_generation_conflict(&request, operation, "evidence")?);
                }
                phase
            } else {
                #[cfg(feature = "test-util")]
                work_started_async("evidence").await;
                let directory = self
                    .reserve_location(&request, operation, "evidence")
                    .await?;
                let prepared = if spec.mode == RefreshMode::Incremental {
                    self.incremental_evidence(spec, &directory, processing, operation)
                        .await?
                } else {
                    let context = self.context();
                    match &spec.transform {
                        RefreshTransform::Join(_) => {
                            let pin = processing.table.as_ref().ok_or_else(|| {
                                invalid("join processing phase is missing its output table pin")
                            })?;
                            context.register_table(
                                "matches",
                                crate::query::table(
                                    storage::open_pin(pin, &self.inner.lifecycle.budget).await?,
                                    self.inner.lifecycle.budget.clone(),
                                    Some(pin.rows),
                                )?,
                            )?;
                        }
                        RefreshTransform::Aggregate(aggregate) => {
                            let loaded = load_inputs(
                                processing.coverage.as_ref().ok_or_else(|| {
                                    invalid(
                                        "aggregate processing phase is missing its input coverage",
                                    )
                                })?,
                                processing
                                    .dependencies
                                    .as_ref()
                                    .map(|dependencies| &dependencies.input_sources)
                                    .unwrap_or(&BTreeMap::new()),
                                &self.inner.lifecycle.budget,
                            )
                            .await?;
                            aggregation::register_input(
                                &context,
                                aggregate,
                                &loaded.borrowed(),
                                &self.inner.lifecycle.budget,
                            )
                            .await?;
                        }
                        RefreshTransform::ConnectedComponents(cc) => {
                            let loaded = load_inputs(
                                processing.coverage.as_ref().ok_or_else(|| {
                                    invalid(
                                        "connected-components processing phase is missing its input coverage",
                                    )
                                })?,
                                processing
                                    .dependencies
                                    .as_ref()
                                    .map(|dependencies| &dependencies.input_sources)
                                    .unwrap_or(&BTreeMap::new()),
                                &self.inner.lifecycle.budget,
                            )
                            .await?;
                            connected_components::register_input(
                                &context,
                                cc,
                                &loaded.borrowed(),
                                &self.inner.lifecycle.budget,
                            )
                            .await?;
                        }
                    }
                    let frame = evidence_frame(
                        &context,
                        &spec.transform,
                        "matches",
                        &self.inner.lifecycle.budget,
                    )
                    .await?;
                    self.write_frame_at(&spec.dataset, &directory, frame)
                        .await?
                };
                let phase = PhaseResult {
                    phase: "evidence".into(),
                    outputs: prepared.table.objects.clone(),
                    table: Some(prepared.table),
                    metrics: prepared.metrics,
                    dependencies,
                    ..processing.value.clone()
                };
                let protections = processing
                    .protections
                    .as_ref()
                    .map(ReferenceProtections::duplicate)
                    .transpose()?;
                self.complete(&request, operation, phase, protections)
                    .await?
            };
            stop_after(stop, "evidence")?;
            Ok(Retained {
                value: RelatedResult {
                    input_sources: processing
                        .dependencies
                        .as_ref()
                        .map(|dependencies| dependencies.input_sources.clone())
                        .unwrap_or_default(),
                    request: spec.clone(),
                    coverage: processing.coverage.clone().ok_or_else(|| {
                        invalid("completed refresh processing phase is missing input coverage")
                    })?,
                    evidence: evidence.value.table.ok_or_else(|| {
                        invalid("completed refresh evidence phase is missing its table pin")
                    })?,
                },
                protections: evidence.protections,
            })
        })
    }

    pub(super) async fn select_refresh(
        &self,
        spec: &RefreshSpec,
        operation: &Operation,
        release: &Retained<ReleasePin>,
    ) -> Result<ReleasePin> {
        let related = release
            .dataset
            .related
            .as_ref()
            .ok_or_else(|| invalid("refresh release is missing related result metadata"))?;
        let dependencies =
            super::verification::available_graph(release, &self.inner.lifecycle.budget).await?;
        let _guards = self.lock_inputs(dependencies.datasets()).await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(selected) = lifecycle
            .get::<ReleasePin>(&view, &format!("result/{}", spec.operation))
            .await?
        {
            return Ok(selected);
        }
        let authority = self.check(&view, operation, &spec.operation).await?;
        check_rebuild(
            &authority,
            &OperationRequest::Refresh(Box::new(spec.clone())),
        )?;
        self.guard_ancestors(&dependencies, release).await?;
        let previous = authority
            .selected
            .as_ref()
            .and_then(|dataset| dataset.related.as_ref());
        if let Some(previous) = previous
            && (!previous.request.transform.same_roles(&spec.transform))
        {
            return Err(Error::refusal(
                "incomparable_inputs",
                "changing transform kind or input roles requires explicit reconciliation",
            ));
        }
        let progress = self
            .guard_inputs(
                &related.coverage,
                previous.map(|previous| &previous.coverage),
                spec.policy,
            )
            .await?;
        self.select_with_progress(&view, authority, release, Some(&progress))
            .await?;
        Ok(release.value.clone())
    }

    // Caller owns every participating source/family lock plus the output lock.
    pub(super) async fn guard_ancestors(
        &self,
        graph: &super::verification::DependencyGraph,
        release: &ReleasePin,
    ) -> Result<()> {
        for coverage in graph.ancestor_coverages(release) {
            self.guard_inputs(coverage, None, RefreshPolicy::Captured)
                .await?;
        }
        Ok(())
    }

    // Caller owns every participating source/family lock plus the output lock.
    pub(super) async fn guard_inputs(
        &self,
        coverage: &InputCoverage,
        previous: Option<&InputCoverage>,
        policy: RefreshPolicy,
    ) -> Result<BTreeMap<Id, SuccessionProof>> {
        let lifecycle = &self.inner.lifecycle;
        if previous.is_some_and(|previous| !previous.inputs.keys().eq(coverage.inputs.keys())) {
            return Err(Error::refusal(
                "incomparable_inputs",
                "refresh cannot omit or replace prior input datasets",
            ));
        }
        let mut relationships = BTreeMap::new();
        let mut progress = BTreeMap::new();
        for (dataset, target) in &coverage.inputs {
            relationships.insert(dataset.clone(), self.qualify_input(target).await?);
            let view = lifecycle.open(dataset).await?;
            let prior = previous
                .and_then(|previous| previous.inputs.get(dataset))
                .unwrap_or(target);
            let proof = self.follows(&view, prior, target).await?;
            if policy == RefreshPolicy::Latest && proof.observed_head != *target {
                return Err(Error::refusal(
                    "stale_input",
                    "latest-input policy requires every captured version to remain selected",
                ));
            }
            progress.insert(dataset.clone(), proof);
        }
        for (family, seal) in &coverage.generations {
            let view = lifecycle.open(family).await?;
            let current: Option<GenerationSeal> = lifecycle
                .get(&view, &format!("generation/{}", seal.generation))
                .await?;
            let members: Option<BTreeSet<Id>> = lifecycle.get(&view, "family-members").await?;
            if current.as_ref() != Some(seal) || members.as_ref() != coverage.families.get(family) {
                return Err(inputs::invalid_relationship());
            }
        }
        inputs::check_relationships(coverage, &relationships)?;
        Ok(progress)
    }
}

pub(super) fn validate(spec: &RefreshSpec) -> Result<()> {
    if spec.definition.trim().is_empty()
        || spec.base.as_ref() == Some(&spec.operation)
        || spec
            .transform
            .versions()
            .iter()
            .any(|v| v.dataset == spec.dataset)
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "refresh needs exact external inputs and distinct output identity",
        ));
    }
    let spec = match &spec.transform {
        RefreshTransform::Join(spec) => spec,
        RefreshTransform::Aggregate(spec) => return aggregation::validate_transform(spec),
        RefreshTransform::ConnectedComponents(spec) => {
            return connected_components::validate(spec);
        }
    };
    if (spec.left.dataset == spec.right.dataset && spec.left != spec.right)
        || spec.join.keys.is_empty()
        || spec.join.fields.is_empty()
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "join needs consistent input roles, keys and fields",
        ));
    }
    let names: BTreeSet<_> = spec
        .join
        .fields
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
    let keys: BTreeSet<_> = spec
        .join
        .keys
        .iter()
        .map(|key| (&key.left, &key.right))
        .collect();
    if names.len() != spec.join.fields.len()
        || keys.len() != spec.join.keys.len()
        || spec.join.fields.iter().any(|field| {
            field.name.trim().is_empty()
                || field.field.trim().is_empty()
                || field.name.to_ascii_lowercase().starts_with("__docspec_")
                || field.field.to_ascii_lowercase().starts_with("__docspec_")
        })
        || spec.join.keys.iter().any(|key| {
            [&key.left, &key.right].iter().any(|key| {
                key.trim().is_empty() || key.to_ascii_lowercase().starts_with("__docspec_")
            })
        })
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "join fields and keys must be distinct, named, and outside reserved occurrence fields",
        ));
    }
    expr::validate_projections(&spec.join.projections)?;
    expr::validate_projections(&spec.projections)?;
    expr::validate_projections(&spec.effective_projections())?;
    if let Some(filter) = &spec.effective_filter() {
        expr::validate_predicate(filter)?;
    }
    Ok(())
}

pub(super) fn check_rebuild(authority: &Authority, request: &OperationRequest) -> Result<()> {
    if matches!(request, OperationRequest::Refresh(spec) if spec.base.is_none())
        && authority.selected.is_some()
    {
        return Err(Error::refusal(
            "stale_base",
            "an initial refresh requires an absent output; provide the selected base",
        ));
    }
    if authority
        .selected
        .as_ref()
        .is_some_and(|selected| selected.related.is_some())
        && matches!(
            request,
            OperationRequest::Build(_) | OperationRequest::Update(_) | OperationRequest::Schema(_)
        )
    {
        return Err(Error::refusal(
            "related_source",
            "use refresh to preserve related input coverage and contributors",
        ));
    }
    Ok(())
}

pub(super) fn phase_key(
    spec: &RefreshSpec,
    eligibility: u64,
    phase: &str,
    base_manifest: Option<&ObjectRef>,
) -> Result<String> {
    let digest = if spec.mode == RefreshMode::Incremental {
        storage::json_digest(&(
            PROCESSOR,
            spec.mode,
            base_manifest
                .ok_or_else(|| invalid("incremental refresh requires a base manifest digest"))?,
            &spec.transform,
            &spec.definition,
        ))?
    } else if phase == "admission" {
        storage::json_digest(&(PROCESSOR, spec.inputs()))?
    } else {
        storage::json_digest(&(PROCESSOR, &spec.transform, &spec.definition))?
    };
    Ok(format!(
        "phase/refresh/{}/{eligibility}/{phase}",
        digest.to_hex()
    ))
}

pub(super) struct LoadedInputs {
    pub releases: BTreeMap<Id, ReleasePin>,
    canonical_sources: BTreeMap<Id, TablePin>,
    _metadata: Arc<tokio::sync::OwnedSemaphorePermit>,
}
impl LoadedInputs {
    pub(super) fn canonical_source(&self, dataset: &Id) -> Result<&TablePin> {
        self.canonical_sources
            .get(dataset)
            .or_else(|| self.releases.get(dataset)?.dataset.groups.get("source"))
            .ok_or_else(|| invalid("loaded input release is missing its canonical source pin"))
    }
    pub(super) fn borrowed(&self) -> BTreeMap<Id, &ReleasePin> {
        self.releases
            .iter()
            .map(|(dataset, release)| (dataset.clone(), release))
            .collect()
    }
}

pub(super) async fn load_inputs(
    coverage: &InputCoverage,
    input_sources: &BTreeMap<Id, TablePin>,
    budget: &Budget,
) -> Result<LoadedInputs> {
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut bytes = inputs::metadata_size(coverage, budget.limits.max_metadata_bytes)?;
    let mut releases = BTreeMap::new();
    for (dataset, version) in &coverage.inputs {
        let release = read_release(version, budget, &mut bytes, &memory).await?;
        if dataset != &release.dataset.dataset {
            return Err(invalid(
                "input coverage dataset disagrees with release dataset identity",
            ));
        }
        releases.insert(dataset.clone(), release);
    }
    let mut canonical_sources = BTreeMap::new();
    for (dataset, captured) in input_sources {
        bytes = bytes.saturating_add(inputs::metadata_size(
            captured,
            budget.limits.max_metadata_bytes,
        )?);
        if bytes > budget.limits.max_metadata_bytes || captured.metadata.owner != *dataset {
            return Err(invalid(
                "captured input source metadata exceeds allowance or disagrees with dataset owner",
            ));
        }
        let release = releases
            .get_mut(dataset)
            .ok_or_else(|| invalid("captured input source references an undeclared dataset"))?;
        let canonical = release
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid("input release is missing its canonical source group"))?;
        write::open_captured_source(canonical, captured, budget).await?;
        canonical_sources.insert(dataset.clone(), canonical.clone());
        release
            .dataset
            .groups
            .insert("source".into(), captured.clone());
    }
    let relationships = releases
        .iter()
        .map(|(dataset, release)| (dataset.clone(), release.dataset.relationship.clone()))
        .collect();
    inputs::check_relationships(coverage, &relationships)?;
    Ok(LoadedInputs {
        releases,
        canonical_sources,
        _metadata: memory,
    })
}

pub(super) async fn read_release(
    version: &VersionRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<ReleasePin> {
    let objects = storage::Objects::for_path(&version.manifest.path);
    read_release_with_objects(&objects, version, budget, bytes, memory).await
}

pub(super) async fn read_release_with_objects(
    objects: &storage::Objects,
    version: &VersionRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<ReleasePin> {
    let dataset: DatasetPin =
        inputs::read_manifest(objects, &version.manifest, budget, bytes, memory).await?;
    if dataset.dataset != version.dataset
        || dataset.version != version.version
        || version.manifest.owner != dataset.dataset
        || !supported_format(dataset.format)
    {
        return Err(invalid(
            "input manifest identity or format disagrees with version reference",
        ));
    }
    Ok(ReleasePin {
        id: version.version.clone(),
        dataset,
        manifest: version.manifest.clone(),
        verified: true,
    })
}

async fn join_frame(
    context: &SessionContext,
    spec: &JoinTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
    enforce_cardinality: bool,
) -> Result<DataFrame> {
    register_join_inputs(context, spec, releases, budget, enforce_cardinality).await?;
    join_plan(context, spec, enforce_cardinality).await
}

async fn register_join_inputs(
    context: &SessionContext,
    spec: &JoinTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
    enforce_cardinality: bool,
) -> Result<()> {
    // Cardinality probes and the main join replay these same immutable inputs.
    // One-shot joins keep their existing lazy inventory loading.
    let repeated = enforce_cardinality && spec.join.cardinality != JoinCardinality::ManyToMany;
    let mut captured: Option<(&VersionRef, Arc<storage::OccurrenceScan>)> = None;
    for (alias, version) in [("join_left", &spec.left), ("join_right", &spec.right)] {
        let release = releases
            .get(&version.dataset)
            .ok_or_else(|| invalid("missing loaded release for join input"))?;
        if release.version_ref() != *version {
            return Err(invalid(
                "loaded join input release version disagrees with transform specification",
            ));
        }
        let pin = release
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid("join input release is missing its source pin"))?;
        let provider = if repeated {
            let scan = match &captured {
                Some((previous, scan)) if *previous == version => scan.clone(),
                _ => {
                    let native = storage::open_pin(pin, budget).await?;
                    Arc::new(storage::OccurrenceScan::open(&native, budget).await?)
                }
            };
            let provider = crate::query::files(scan.clone(), None, budget.clone())?;
            captured = Some((version, scan));
            provider
        } else {
            let native = storage::open_pin(pin, budget).await?;
            crate::query::occurrences(native, budget.clone(), Some(pin.rows))?
        };
        context.register_table(alias, provider)?;
    }
    Ok(())
}

pub(super) async fn join_registered(
    context: &SessionContext,
    spec: &JoinTransform,
) -> Result<DataFrame> {
    join_plan(context, spec, true).await
}

async fn join_plan(
    context: &SessionContext,
    spec: &JoinTransform,
    enforce_cardinality: bool,
) -> Result<DataFrame> {
    let (probes, main) = join_queries(context, spec, enforce_cardinality).await?;
    for (side, sql) in probes {
        // Unique matchable keys cannot fan out, regardless of the other input.
        // Only duplicate keys need the existing matched-key proof, which keeps
        // unmatched duplicates legal without scanning the other input normally.
        if write::any_rows(context.sql(&duplicate_keys_query(spec, side)).await?).await?
            && write::any_rows(context.sql(&sql).await?).await?
        {
            return Err(Error::refusal(
                "join_cardinality",
                "matching source keys exceed the declared cardinality",
            ));
        }
    }
    Ok(context.sql(&main).await?)
}

fn duplicate_keys_query(spec: &JoinTransform, side: JoinSide) -> String {
    let (table, keys) = match side {
        JoinSide::Left => (
            "join_left",
            spec.join
                .keys
                .iter()
                .map(|key| quote(&key.left))
                .collect::<Vec<_>>(),
        ),
        JoinSide::Right => (
            "join_right",
            spec.join
                .keys
                .iter()
                .map(|key| quote(&key.right))
                .collect::<Vec<_>>(),
        ),
    };
    let filter = match spec.join.nulls {
        NullMatching::Never => format!(
            " WHERE {}",
            keys.iter()
                .map(|key| format!("{key} IS NOT NULL"))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        NullMatching::Equal => String::new(),
    };
    format!(
        "SELECT 1 FROM {table}{filter} GROUP BY {} HAVING COUNT(*) > 1 LIMIT 1",
        keys.join(", ")
    )
}

async fn join_queries(
    context: &SessionContext,
    spec: &JoinTransform,
    enforce_cardinality: bool,
) -> Result<(Vec<(JoinSide, String)>, String)> {
    let schemas = [
        context
            .table("join_left")
            .await?
            .schema()
            .as_arrow()
            .clone(),
        context
            .table("join_right")
            .await?
            .schema()
            .as_arrow()
            .clone(),
    ];
    for key in &spec.join.keys {
        let left = schemas[0].field_with_name(&key.left)?;
        let right = schemas[1].field_with_name(&key.right)?;
        if left.data_type() != right.data_type()
            || left.data_type().is_nested()
            || matches!(
                left.data_type(),
                arrow::datatypes::DataType::Null | arrow::datatypes::DataType::Dictionary(_, _)
            )
        {
            return Err(Error::refusal(
                "join_key_type",
                "join keys need identical qualified scalar types; implicit coercion is refused",
            ));
        }
    }
    for field in &spec.join.fields {
        let index = match field.side {
            JoinSide::Left => 0,
            JoinSide::Right => 1,
        };
        schemas[index].field_with_name(&field.field)?;
    }
    let mut columns = vec![
        format!(
            "l.{} AS {}",
            quote(storage::OCCURRENCE_FILE),
            quote(TUPLE[0])
        ),
        format!(
            "l.{} AS {}",
            quote(storage::OCCURRENCE_ROW),
            quote(TUPLE[1])
        ),
        format!(
            "r.{} AS {}",
            quote(storage::OCCURRENCE_FILE),
            quote(TUPLE[2])
        ),
        format!(
            "r.{} AS {}",
            quote(storage::OCCURRENCE_ROW),
            quote(TUPLE[3])
        ),
    ];
    let (projected_columns, filter_clause) =
        expr::compile_join_sql(spec, &schemas[0], &schemas[1])?;
    columns.extend(projected_columns);
    let comparison = match spec.join.nulls {
        NullMatching::Never => "=",
        NullMatching::Equal => "IS NOT DISTINCT FROM",
    };
    let predicate = spec
        .join
        .keys
        .iter()
        .map(|key| {
            format!(
                "(l.{} {comparison} r.{})",
                quote(&key.left),
                quote(&key.right)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let kind = match spec.join.kind {
        JoinKind::Inner => "INNER",
        JoinKind::Left => "LEFT",
        JoinKind::Full => "FULL",
    };
    // A semi join preserves each candidate partner once, so malformed declared
    // cardinality refuses before a duplicate-key Cartesian result is written.
    let mut probes = Vec::new();
    for side in if !enforce_cardinality {
        vec![]
    } else {
        match spec.join.cardinality {
            JoinCardinality::ManyToMany => vec![],
            JoinCardinality::ManyToOne => vec![JoinSide::Right],
            JoinCardinality::OneToOne => vec![JoinSide::Right, JoinSide::Left],
        }
    } {
        let (from, keys) = match side {
            JoinSide::Right => (
                "join_right r LEFT SEMI JOIN join_left l",
                spec.join
                    .keys
                    .iter()
                    .map(|key| format!("r.{}", quote(&key.right)))
                    .collect::<Vec<_>>(),
            ),
            JoinSide::Left => (
                "join_left l LEFT SEMI JOIN join_right r",
                spec.join
                    .keys
                    .iter()
                    .map(|key| format!("l.{}", quote(&key.left)))
                    .collect::<Vec<_>>(),
            ),
        };
        probes.push((
            side,
            format!(
                "SELECT 1 FROM {from} ON {predicate} GROUP BY {} HAVING COUNT(*) > 1 LIMIT 1",
                keys.join(", ")
            ),
        ));
    }
    let where_sql = match filter_clause {
        Some(f) => format!(" WHERE {f}"),
        None => String::new(),
    };
    Ok((
        probes,
        format!(
            "SELECT {} FROM join_left l {kind} JOIN join_right r ON {predicate}{where_sql}",
            columns.join(", ")
        ),
    ))
}

pub(super) fn tuple_columns(alias: Option<&str>) -> String {
    TUPLE
        .iter()
        .map(|name| match alias {
            Some(alias) => format!("{alias}.{}", quote(name)),
            None => quote(name),
        })
        .collect::<Vec<_>>()
        .join(", ")
}
pub(super) fn invalid(reason: &'static str) -> Error {
    Error::refusal("invalid_related_result", reason)
}

async fn source_frame(
    context: &SessionContext,
    transform: &RefreshTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
    enforce_cardinality: bool,
) -> Result<DataFrame> {
    match transform {
        RefreshTransform::Join(join) => {
            join_frame(context, join, releases, budget, enforce_cardinality).await
        }
        RefreshTransform::Aggregate(aggregate) => {
            aggregation::register_input(context, aggregate, releases, budget).await?;
            aggregation::aggregate_frame_with_transform(context, aggregate, "aggregate_input").await
        }
        RefreshTransform::ConnectedComponents(cc) => {
            connected_components::register_input(context, cc, releases, budget).await?;
            connected_components::source_frame(context, cc, "cc_input", budget).await
        }
    }
}
async fn evidence_frame(
    context: &SessionContext,
    transform: &RefreshTransform,
    source: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    match transform {
        RefreshTransform::Join(_) => Ok(context.table(source).await?.select(
            TUPLE
                .iter()
                .map(|&name| col(Column::from_name(name)))
                .collect::<Vec<_>>(),
        )?),
        RefreshTransform::Aggregate(aggregate) => {
            aggregation::evidence_frame_with_transform(context, aggregate, "aggregate_input").await
        }
        RefreshTransform::ConnectedComponents(cc) => {
            connected_components::evidence_frame(context, cc, "cc_input", budget).await
        }
    }
}
pub(super) fn output_keys(transform: &RefreshTransform) -> Vec<String> {
    match transform {
        RefreshTransform::Join(_) => TUPLE.map(str::to_owned).to_vec(),
        RefreshTransform::Aggregate(aggregate) => aggregation::keys(&aggregate.grouping),
        RefreshTransform::ConnectedComponents(cc) => vec![cc.document_id_column.clone()],
    }
}
pub(super) fn hidden_field(name: &str) -> bool {
    TUPLE.contains(&name) || name == aggregation::GLOBAL
}
