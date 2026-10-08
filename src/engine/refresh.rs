//! Native related results, retained contributors, and guarded forward input coverage.

use super::write::{Prepared, identifier as quote};
use super::*;
use std::collections::BTreeSet;

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
        self.plan_request(&OperationRequest::Refresh(Box::new(spec.clone())))
            .await
    }

    /// Runs or resumes native joins or summaries with retained contributor evidence.
    ///
    /// # Errors
    /// Refuses inadmissible inputs, invalid recipes, exhausted capacity, and stale selection guards.
    pub async fn refresh(&self, spec: &RefreshSpec) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Refresh(Box::new(spec.clone())), None)
            .await
    }

    /// Interrupts after a durable refresh handoff for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or an ordinary refresh refusal.
    #[cfg(feature = "test-util")]
    pub async fn refresh_until(&self, spec: &RefreshSpec, phase: &str) -> Result<ReleasePin> {
        self.run_internal(
            &OperationRequest::Refresh(Box::new(spec.clone())),
            Some(phase),
        )
        .await
    }

    pub(super) async fn admit_refresh(
        &self,
        spec: &RefreshSpec,
        operation: &Operation,
    ) -> Result<PhaseResult> {
        #[cfg(feature = "test-util")]
        work_started("admission");
        let coverage = self.capture_inputs(&spec.inputs()).await?;
        let budget = &self.inner.lifecycle.budget;
        let loaded = load_inputs(&coverage, budget).await?;
        verification::verify_all(&loaded.releases.values().collect::<Vec<_>>(), budget).await?;
        if let RefreshTransform::Aggregate(aggregate) = &spec.transform {
            aggregation::register_input(&self.context(), aggregate, &loaded.borrowed(), budget)
                .await?;
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
        };
        self.complete(
            &OperationRequest::Refresh(Box::new(spec.clone())),
            operation,
            &phase,
        )
        .await
    }

    pub(super) async fn process_refresh(
        &self,
        spec: &RefreshSpec,
        directory: &Path,
        admission: &PhaseResult,
        operation: &Operation,
    ) -> Result<Prepared> {
        if spec.mode == RefreshMode::Incremental {
            return Box::pin(self.process_incremental(spec, directory, admission, operation)).await;
        }
        let coverage = admission.coverage.as_ref().ok_or_else(invalid)?;
        if coverage.inputs != spec.inputs() {
            return Err(invalid());
        }
        let loaded = load_inputs(coverage, &self.inner.lifecycle.budget).await?;
        let context = self.context();
        let frame = source_frame(
            &context,
            &spec.transform,
            &loaded.borrowed(),
            &self.inner.lifecycle.budget,
        )
        .await?;
        self.write_frame(&spec.dataset, directory, frame).await
    }

    pub(super) async fn prepare_evidence(
        &self,
        spec: &RefreshSpec,
        operation: &Operation,
        processing: &PhaseResult,
        stop: Option<&str>,
    ) -> Result<RelatedResult> {
        let request = OperationRequest::Refresh(Box::new(spec.clone()));
        let evidence = if let Some(phase) = self.phase(&request, operation, "evidence").await? {
            phase
        } else {
            #[cfg(feature = "test-util")]
            work_started("evidence");
            let directory = self
                .reserve_attempt(&request, operation, "evidence")
                .await?;
            let prepared = if spec.mode == RefreshMode::Incremental {
                Box::pin(self.incremental_evidence(spec, &directory, processing, operation)).await?
            } else {
                let context = self.context();
                match &spec.transform {
                    RefreshTransform::Join(_) => {
                        context.register_table(
                            "matches",
                            crate::query::table(
                                storage::open_pin(processing.table.as_ref().ok_or_else(invalid)?)
                                    .await?,
                                self.inner.lifecycle.budget.clone(),
                            )?,
                        )?;
                    }
                    RefreshTransform::Aggregate(aggregate) => {
                        let loaded = load_inputs(
                            processing.coverage.as_ref().ok_or_else(invalid)?,
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
                }
                let frame = evidence_frame(&context, &spec.transform, "matches").await?;
                self.write_frame(&spec.dataset, &directory, frame).await?
            };
            let phase = PhaseResult {
                phase: "evidence".into(),
                outputs: prepared.table.objects.clone(),
                table: Some(prepared.table),
                metrics: prepared.metrics,
                ..processing.clone()
            };
            self.complete(&request, operation, &phase).await?
        };
        stop_after(stop, "evidence")?;
        Ok(RelatedResult {
            request: spec.clone(),
            coverage: processing.coverage.clone().ok_or_else(invalid)?,
            evidence: evidence.table.ok_or_else(invalid)?,
        })
    }

    pub(super) async fn select_refresh(
        &self,
        spec: &RefreshSpec,
        operation: &Operation,
        release: &ReleasePin,
    ) -> Result<ReleasePin> {
        let related = release.dataset.related.as_ref().ok_or_else(invalid)?;
        let dependencies =
            verification::available_graph(release, &self.inner.lifecycle.budget).await?;
        let _guards = self.lock_inputs(dependencies.datasets()).await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(selected) = lifecycle
            .get::<ReleasePin>(&view, &format!("result/{}", spec.operation))
            .await?
        {
            return Ok(selected);
        }
        let authority = self.check(&view, operation).await?;
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
        Ok(release.clone())
    }

    // Caller owns every participating source/family lock plus the output lock.
    pub(super) async fn guard_ancestors(
        &self,
        graph: &verification::DependencyGraph,
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
        RefreshTransform::Aggregate(spec) => return aggregation::validate(&spec.grouping),
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
            OperationRequest::Build(_) | OperationRequest::Update(_)
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
    let material = if spec.mode == RefreshMode::Incremental {
        serde_json::to_vec(&(
            PROCESSOR,
            spec.mode,
            base_manifest.ok_or_else(invalid)?,
            &spec.transform,
            &spec.definition,
        ))?
    } else if phase == "admission" {
        serde_json::to_vec(&(PROCESSOR, spec.inputs()))?
    } else {
        serde_json::to_vec(&(PROCESSOR, &spec.transform, &spec.definition))?
    };
    Ok(format!(
        "phase/refresh/{}/{eligibility}/{phase}",
        blake3::hash(&material).to_hex()
    ))
}

pub(super) struct LoadedInputs {
    pub releases: BTreeMap<Id, ReleasePin>,
    _metadata: Arc<tokio::sync::OwnedSemaphorePermit>,
}
impl LoadedInputs {
    pub(super) fn borrowed(&self) -> BTreeMap<Id, &ReleasePin> {
        self.releases
            .iter()
            .map(|(dataset, release)| (dataset.clone(), release))
            .collect()
    }
}

pub(super) async fn load_inputs(coverage: &InputCoverage, budget: &Budget) -> Result<LoadedInputs> {
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut bytes = inputs::metadata_size(coverage, budget.limits.max_metadata_bytes)?;
    let mut releases = BTreeMap::new();
    for (dataset, version) in &coverage.inputs {
        let release = read_release(version, budget, &mut bytes, &memory).await?;
        if dataset != &release.dataset.dataset {
            return Err(invalid());
        }
        releases.insert(dataset.clone(), release);
    }
    let relationships = releases
        .iter()
        .map(|(dataset, release)| (dataset.clone(), release.dataset.relationship.clone()))
        .collect();
    inputs::check_relationships(coverage, &relationships)?;
    Ok(LoadedInputs {
        releases,
        _metadata: memory,
    })
}

pub(super) async fn read_release(
    version: &VersionRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<ReleasePin> {
    let dataset: DatasetPin =
        inputs::read_manifest(&version.manifest, budget, bytes, memory).await?;
    if dataset.dataset != version.dataset
        || dataset.version != version.version
        || version.manifest.owner != dataset.dataset
        || dataset.format != FORMAT
    {
        return Err(invalid());
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
) -> Result<DataFrame> {
    for (alias, version) in [("join_left", &spec.left), ("join_right", &spec.right)] {
        let release = releases.get(&version.dataset).ok_or_else(invalid)?;
        if release.version_ref() != *version {
            return Err(invalid());
        }
        let native =
            storage::open_pin(release.dataset.groups.get("source").ok_or_else(invalid)?).await?;
        context.register_table(alias, crate::query::occurrences(native, budget.clone())?)?;
    }
    join_registered(context, spec).await
}

pub(super) async fn join_registered(
    context: &SessionContext,
    spec: &JoinTransform,
) -> Result<DataFrame> {
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
    columns.extend(spec.join.fields.iter().map(|field| {
        let side = match field.side {
            JoinSide::Left => "l",
            JoinSide::Right => "r",
        };
        format!("{side}.{} AS {}", quote(&field.field), quote(&field.name))
    }));
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
    for side in match spec.join.cardinality {
        JoinCardinality::ManyToMany => vec![],
        JoinCardinality::ManyToOne => vec![JoinSide::Right],
        JoinCardinality::OneToOne => vec![JoinSide::Right, JoinSide::Left],
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
        if has_rows(
            context,
            &format!(
                "SELECT 1 FROM {from} ON {predicate} GROUP BY {} HAVING COUNT(*) > 1 LIMIT 1",
                keys.join(", ")
            ),
        )
        .await?
        {
            return Err(Error::refusal(
                "join_cardinality",
                "matching source keys exceed the declared cardinality",
            ));
        }
    }
    Ok(context
        .sql(&format!(
            "SELECT {} FROM join_left l {kind} JOIN join_right r ON {predicate}",
            columns.join(", ")
        ))
        .await?)
}

pub(super) async fn verify_related(
    source: &TablePin,
    related: &RelatedResult,
    budget: &Budget,
) -> Result<()> {
    let loaded = load_inputs(&related.coverage, budget).await?;
    verify_related_with_inputs(source, related, &loaded.borrowed(), budget).await
}

pub(super) async fn verify_related_with_inputs(
    source: &TablePin,
    related: &RelatedResult,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
) -> Result<()> {
    if related.coverage.inputs != related.request.inputs() {
        return Err(invalid());
    }
    let relationships = related
        .coverage
        .inputs
        .keys()
        .map(|dataset| {
            releases
                .get(dataset)
                .map(|release| (dataset.clone(), release.dataset.relationship.clone()))
                .ok_or_else(invalid)
        })
        .collect::<Result<_>>()?;
    inputs::check_relationships(&related.coverage, &relationships)?;
    let context = budget.context();
    validate(&related.request)?;
    let expected = source_frame(&context, &related.request.transform, releases, budget).await?;
    let expected_schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(expected.schema().as_arrow())?;
    let native = storage::open_pin(source).await?;
    if *native.metadata().current_schema().as_ref() != expected_schema {
        return Err(Error::refusal(
            "invalid_related_result",
            "native result schema differs from the declared computation",
        ));
    }
    context.register_table("expected", expected.into_view())?;
    context.register_table("actual", crate::query::table(native, budget.clone())?)?;
    let columns = context
        .table("expected")
        .await?
        .schema()
        .fields()
        .iter()
        .map(|field| quote(field.name()))
        .collect::<Vec<_>>();
    equal_multisets(&context, "expected", "actual", &columns.join(", ")).await?;
    verify_table(&related.evidence, budget).await?;
    let evidence = storage::open_pin(&related.evidence).await?;
    let expected_evidence = evidence_frame(&context, &related.request.transform, "actual").await?;
    let evidence_columns = expected_evidence
        .schema()
        .fields()
        .iter()
        .map(|field| quote(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let evidence_schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
        expected_evidence.schema().as_arrow(),
    )?;
    context.register_table("expected_evidence", expected_evidence.into_view())?;
    if *evidence.metadata().current_schema().as_ref() != evidence_schema {
        return Err(Error::refusal(
            "invalid_related_result",
            "contributor evidence schema differs from the selected native result",
        ));
    }
    context.register_table("evidence", crate::query::table(evidence, budget.clone())?)?;
    equal_multisets(&context, "expected_evidence", "evidence", &evidence_columns).await?;
    let RefreshTransform::Join(join) = &related.request.transform else {
        return Ok(());
    };
    for side in match join.join.cardinality {
        JoinCardinality::ManyToMany => vec![],
        JoinCardinality::ManyToOne => vec![0],
        JoinCardinality::OneToOne => vec![0, 2],
    } {
        let sql = format!(
            "SELECT 1 FROM actual WHERE {} IS NOT NULL AND {} IS NOT NULL GROUP BY {}, {} HAVING COUNT(*) > 1 LIMIT 1",
            quote(TUPLE[0]),
            quote(TUPLE[2]),
            quote(TUPLE[side]),
            quote(TUPLE[side + 1])
        );
        if has_rows(&context, &sql).await? {
            return Err(Error::refusal(
                "join_cardinality",
                "an occurrence has more matching partners than the declared rule permits",
            ));
        }
    }
    Ok(())
}

async fn equal_multisets(
    context: &SessionContext,
    left: &str,
    right: &str,
    columns: &str,
) -> Result<()> {
    let sql = format!(
        "SELECT 1 FROM (SELECT {columns}, SUM(__docspec_weight) AS __docspec_total FROM (SELECT {columns}, CAST(1 AS BIGINT) AS __docspec_weight FROM {left} UNION ALL SELECT {columns}, CAST(-1 AS BIGINT) AS __docspec_weight FROM {right}) GROUP BY {columns}) WHERE __docspec_total <> 0 LIMIT 1"
    );
    if has_rows(context, &sql).await? {
        return Err(Error::refusal(
            "invalid_related_result",
            format!("typed row multiplicities differ between {left} and {right}"),
        ));
    }
    Ok(())
}
async fn has_rows(context: &SessionContext, sql: &str) -> Result<bool> {
    let mut stream = context.sql(sql).await?.execute_stream().await?;
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() != 0 {
            return Ok(true);
        }
    }
    Ok(false)
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
fn invalid() -> Error {
    Error::refusal(
        "invalid_related_result",
        "related values, contributor evidence or captured input identities disagree",
    )
}

async fn source_frame(
    context: &SessionContext,
    transform: &RefreshTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
) -> Result<DataFrame> {
    match transform {
        RefreshTransform::Join(join) => join_frame(context, join, releases, budget).await,
        RefreshTransform::Aggregate(aggregate) => {
            aggregation::register_input(context, aggregate, releases, budget).await?;
            aggregation::aggregate_frame(context, &aggregate.grouping, "aggregate_input").await
        }
    }
}
async fn evidence_frame(
    context: &SessionContext,
    transform: &RefreshTransform,
    source: &str,
) -> Result<DataFrame> {
    match transform {
        RefreshTransform::Join(_) => Ok(context
            .sql(&format!("SELECT {} FROM {source}", tuple_columns(None)))
            .await?),
        RefreshTransform::Aggregate(aggregate) => {
            aggregation::evidence_frame(context, &aggregate.grouping, "aggregate_input").await
        }
    }
}
pub(super) fn output_keys(transform: &RefreshTransform) -> Vec<String> {
    match transform {
        RefreshTransform::Join(_) => TUPLE.map(str::to_owned).to_vec(),
        RefreshTransform::Aggregate(aggregate) => aggregation::keys(&aggregate.grouping),
    }
}
pub(super) fn hidden_field(name: &str) -> bool {
    TUPLE.contains(&name) || name == aggregation::GLOBAL
}
