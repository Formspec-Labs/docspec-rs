//! Phase planning (`plan` / `plan_update`), phase keys, fingerprints, and authority checks.

use super::{
    CompletedGroups, Engine, FORMAT, Operation, PHASES, PROCESSOR, ValidatedReadback, inputs,
    refresh, sql_validation::validate_request, validation,
};
use crate::{Error, Result, catalog::View, model::*, storage};
use serde::Serialize;
use std::collections::BTreeMap;

/// Why a durable phase has no completion available for this request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanPhaseReason {
    /// No completion is retained under this request's exact phase key.
    NoRetainedCompletion,
    /// The exact retained completion's recovery promise was explicitly retired.
    RecoveryRetired,
    /// Retained readback predates the current built-in routine checker.
    ValidatorChanged,
}

/// A bounded explanation of reuse and work remaining before selection.
#[derive(Clone, Debug, Serialize)]
pub struct RunPlan {
    pub dataset: Id,
    pub operation: Id,
    /// This operation has a retained selection result, even after a later selection.
    pub completed: bool,
    pub reusable_phases: Vec<String>,
    /// Exact retained records supporting the reusable phases at this observation.
    /// Keys locate consumed references and original operation identity in `inspect`.
    /// Readback fast paths do not revisit earlier phases; this grants no protection.
    pub reusable_phase_origins: BTreeMap<String, RetainedGeneration>,
    /// Missing durable handoffs, including final selection when not completed.
    pub required_phases: Vec<String>,
    /// Known reasons for required durable phases; final selection is a separate handoff.
    /// Absence does not infer failure, running work, or changed input contents.
    pub required_phase_reasons: BTreeMap<String, PlanPhaseReason>,
    pub limits: crate::budget::Limits,
}

impl Engine {
    /// Explains retained phases without opening large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting operation identities or invalid requests.
    pub async fn plan(&self, spec: &RunSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Build(spec.clone()), memory)
            .await
    }

    /// Plans an update without opening its large input payloads.
    ///
    /// # Errors
    /// Refuses conflicting identities, stale bases or definitions, and invalid requests.
    pub async fn plan_update(&self, spec: &UpdateSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Update(spec.clone()), memory)
            .await
    }

    pub(in crate::engine) async fn plan_request(
        &self,
        spec: OperationRequest,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<RunPlan> {
        self.admit_work(memory, move |engine| async move {
            engine.plan_request_admitted(&spec).await
        })
        .await
    }

    pub(in crate::engine) async fn plan_request_admitted(
        &self,
        spec: &OperationRequest,
    ) -> Result<RunPlan> {
        self.check_request_backend(spec)?;
        validate_request(spec)?;
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = self.request_view(spec).await?;
        self.check_operation_available(&view, spec.operation())
            .await?;
        self.check_not_abandoned(&view, spec.operation()).await?;
        let request = fingerprint(spec)?;
        let operation: Option<Operation> = lifecycle
            .get(&view, &format!("operation/{}", spec.operation()))
            .await?;
        if operation
            .as_ref()
            .is_some_and(|o| o.request_digest != request)
        {
            return Err(conflicting_operation());
        }
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        let key = format!("result/{}", spec.operation());
        let completed = lifecycle.get::<ReleasePin>(&view, &key).await?.is_some();
        if !completed {
            self.check_backend_selection(&authority, spec)?;
            check_definition(&authority, spec)?;
            check_base(&authority, spec.base())?;
            check_update_eligibility(&authority, spec)?;
            check_group_rebuild(&authority, spec)?;
            self.check_incremental(&authority, spec).await?;
            self.check_schema(&view, &authority, spec, operation.is_none())
                .await?;
        }
        if !completed && let Some(operation) = &operation {
            check_authority(&authority, operation)?;
        }
        let readback_key = format!("readback/{}", spec.operation());
        let readback = lifecycle
            .rows(&view)
            .await?
            .get_record::<ValidatedReadback>(&readback_key)?
            .map(|(revision, _, completed)| {
                (
                    RetainedGeneration {
                        key: readback_key,
                        revision,
                    },
                    completed.current(),
                )
            });
        // A readback preserves expensive work but still needs guarded selection.
        // A retained result completes this operation independently of today's head.
        if completed || readback.is_some() {
            return Ok(self.plan_from_readback(spec, completed, readback));
        }
        let (phases, receipt_base) = self.ordered_plan_phases(&view, spec).await?;
        let schema_key = match &operation {
            Some(operation) => operation.schema_key.clone(),
            None => self.schema_key(&view, &authority, spec).await?,
        };
        let mut reusable_phases = Vec::new();
        let mut reusable_phase_origins = BTreeMap::new();
        let mut required_phases = Vec::new();
        let mut required_phase_reasons = BTreeMap::new();
        let mut retained_processing = false;
        let mut adopted_receipts = false;
        for phase in phases {
            if phase == "receipts" && adopted_receipts {
                continue;
            }
            let (origin, reason) = self
                .resolve_plan_phase(
                    &view,
                    spec,
                    &request,
                    &authority,
                    operation.as_ref(),
                    schema_key.as_ref(),
                    receipt_base.as_ref(),
                    phase,
                    &mut retained_processing,
                    &mut adopted_receipts,
                )
                .await?;
            if let Some(origin) = origin {
                reusable_phases.push(phase.to_owned());
                reusable_phase_origins.insert(phase.to_owned(), origin);
            } else {
                required_phases.push(phase.to_owned());
                if let Some(reason) = reason {
                    required_phase_reasons.insert(phase.to_owned(), reason);
                }
            }
        }
        if retained_processing {
            required_phases.retain(|phase| phase != "admission");
            required_phase_reasons.remove("admission");
        }
        required_phases.push("selection".into());
        Ok(RunPlan {
            dataset: spec.dataset().clone(),
            operation: spec.operation().clone(),
            completed,
            reusable_phases,
            reusable_phase_origins,
            required_phases,
            required_phase_reasons,
            limits: lifecycle.budget.limits.clone(),
        })
    }

    fn plan_from_readback(
        &self,
        spec: &OperationRequest,
        completed: bool,
        readback: Option<(RetainedGeneration, bool)>,
    ) -> RunPlan {
        let current_readback = readback.as_ref().is_some_and(|(_, current)| *current);
        RunPlan {
            dataset: spec.dataset().clone(),
            operation: spec.operation().clone(),
            completed,
            reusable_phases: if readback.is_some() && (completed || current_readback) {
                vec!["readback".into()]
            } else {
                Vec::new()
            },
            reusable_phase_origins: readback
                .filter(|(_, current)| completed || *current)
                .map(|(origin, _)| BTreeMap::from([("readback".into(), origin)]))
                .unwrap_or_default(),
            required_phases: if completed {
                Vec::new()
            } else if current_readback {
                vec!["selection".into()]
            } else {
                vec!["validation".into(), "readback".into(), "selection".into()]
            },
            required_phase_reasons: if !completed && !current_readback {
                ["validation", "readback"]
                    .into_iter()
                    .map(|phase| (phase.into(), PlanPhaseReason::ValidatorChanged))
                    .collect()
            } else {
                BTreeMap::new()
            },
            limits: self.inner.lifecycle.budget.limits.clone(),
        }
    }

    async fn ordered_plan_phases(
        &self,
        view: &View,
        spec: &OperationRequest,
    ) -> Result<(Vec<&'static str>, Option<DatasetPin>)> {
        let mut phases = PHASES.to_vec();
        if matches!(spec, OperationRequest::Refresh(_)) {
            phases.insert(2, "evidence");
        }
        let mut receipt_base = None;
        if let Some(version) = spec.base() {
            let base: ReleasePin = self
                .inner
                .lifecycle
                .get(view, &format!("result/{version}"))
                .await?
                .ok_or_else(|| {
                    Error::refusal("no_release", "captured source version is unavailable")
                })?;
            if !base.dataset.computed.is_empty() && !matches!(spec, OperationRequest::Evidence(_)) {
                phases.insert(3, "groups");
            }
            if base.dataset.evidence.is_some()
                && !matches!(
                    spec,
                    OperationRequest::Evidence(_) | OperationRequest::Schema(_)
                )
            {
                phases.insert(2, "receipts");
                receipt_base = Some(base.dataset);
            }
        }
        Ok((phases, receipt_base))
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_plan_phase(
        &self,
        view: &View,
        spec: &OperationRequest,
        request: &str,
        authority: &Authority,
        operation: Option<&Operation>,
        schema_key: Option<&String>,
        receipt_base: Option<&DatasetPin>,
        phase: &str,
        retained_processing: &mut bool,
        adopted_receipts: &mut bool,
    ) -> Result<(Option<RetainedGeneration>, Option<PlanPhaseReason>)> {
        if phase == "groups" {
            let key = format!("groups/{}", spec.operation());
            let (completion, reason) = self
                .completion_with_reason::<CompletedGroups>(view, &key)
                .await?;
            return Ok((
                completion.map(|(revision, _)| RetainedGeneration { key, revision }),
                reason,
            ));
        }
        if phase == "readback" {
            // The same immutable view's readback was already resolved above.
            return Ok((None, Some(PlanPhaseReason::NoRetainedCompletion)));
        }
        let eligibility = operation.map_or(authority.eligibility, |o| o.eligibility);
        let key = phase_key(
            spec,
            request,
            eligibility,
            phase,
            operation
                .and_then(|o| o.base_manifest.as_ref())
                .or_else(|| captured_manifest(authority, spec)),
            schema_key,
        )?;
        let (completion, reason) = self
            .completion_with_reason::<PhaseResult>(view, &key)
            .await?;
        if phase == "processing" {
            if let Some((_, completed)) = &completion {
                self.check_reused_build_schema(
                    view,
                    spec,
                    operation.map_or(FORMAT, |o| o.result_format),
                    schema_key,
                    completed,
                )
                .await?;
            }
            *retained_processing = completion
                .as_ref()
                .is_some_and(|(_, phase)| phase.dependencies.is_some());
            if let (Some((_, processing)), Some(base)) = (&completion, receipt_base) {
                *adopted_receipts = super::evidence::adopts_receipts(spec, processing, base)?;
            }
        }
        Ok((
            completion.map(|(revision, _)| RetainedGeneration { key, revision }),
            reason,
        ))
    }
}

// These operations reuse the exact selected native combination as a material input.
pub(in crate::engine) fn captured_manifest<'a>(
    authority: &'a Authority,
    request: &OperationRequest,
) -> Option<&'a ObjectRef> {
    (matches!(request, OperationRequest::Schema(_) | OperationRequest::Evidence(_))
        || matches!(request, OperationRequest::Refresh(spec) if spec.mode == RefreshMode::Incremental))
        .then(|| authority.release.as_ref().map(|release| &release.manifest)).flatten()
}

pub(in crate::engine) fn phase_key(
    spec: &OperationRequest,
    request: &str,
    eligibility: u64,
    phase: &str,
    base_manifest: Option<&ObjectRef>,
    schema_key: Option<&String>,
) -> Result<String> {
    let key = material_phase_key(spec, request, eligibility, phase, base_manifest, schema_key)?;
    if phase == "validation" {
        return validation::phase_key(&key);
    }
    Ok(key)
}

pub(in crate::engine) fn material_phase_key(
    spec: &OperationRequest,
    request: &str,
    eligibility: u64,
    phase: &str,
    base_manifest: Option<&ObjectRef>,
    schema_key: Option<&String>,
) -> Result<String> {
    if phase == "receipts" {
        return Ok(format!("phase/{request}/{eligibility}/receipts"));
    }
    if let OperationRequest::Refresh(refresh) = spec {
        return refresh::phase_key(refresh, eligibility, phase, base_manifest);
    }
    if phase == "admission" && !matches!(spec, OperationRequest::Schema(_)) {
        let mut hash = blake3::Hasher::new();
        for (name, input) in spec.inputs() {
            hash.update(&(name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash.update(input.digest.as_bytes());
        }
        return Ok(format!("phase/input/{}/{phase}", hash.finalize().to_hex()));
    }
    if let OperationRequest::Build(build) = spec {
        // Input bytes, SQL and retained field history determine source work.
        // Compatible selections may reuse it; promotion still checks authority.
        let mut hash = blake3::Hasher::new();
        for value in [
            PROCESSOR,
            &build.expected_digest,
            &build.sql,
            &build.definition,
        ] {
            hash.update(&(value.len() as u64).to_le_bytes());
            hash.update(value.as_bytes());
        }
        if let Some(source) = schema_key {
            hash.update(b"native-omission-rules-v1");
            hash.update(source.as_bytes());
        }
        if !build.partition_by.is_empty() {
            hash.update(b"partition-by-v1");
            hash.update(&serde_json::to_vec(&build.partition_by)?);
        }
        return Ok(format!(
            "phase/build/{}/{eligibility}/{phase}",
            hash.finalize().to_hex()
        ));
    }
    Ok(format!("phase/{request}/{eligibility}/{phase}"))
}

pub(in crate::engine) fn conflicting_operation() -> Error {
    Error::refusal(
        "operation_conflict",
        "operation identity already binds a different request",
    )
}

pub(in crate::engine) fn check_authority(
    authority: &Authority,
    operation: &Operation,
) -> Result<()> {
    if authority.eligibility != operation.eligibility
        || authority.definition != operation.definition
        || authority.epoch != operation.epoch
    {
        return Err(Error::refusal(
            "stale_eligibility",
            "definition, dependency qualification or authority changed; start a new operation",
        ));
    }
    check_base(authority, operation.base.as_ref())?;
    if operation.base_manifest.as_ref().is_some_and(|manifest| {
        authority.release.as_ref().map(|release| &release.manifest) != Some(manifest)
    }) {
        return Err(Error::refusal(
            "stale_base",
            "selected base manifest differs from the captured operation",
        ));
    }
    if operation.require_current_boundary
        && authority.boundary != operation.admission_fence.unwrap_or(operation.boundary)
    {
        return Err(Error::refusal(
            "stale_source_boundary",
            "source admission advanced after capture; start a new operation",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_preparation(
    authority: &Authority,
    operation: &Operation,
) -> Result<()> {
    if authority.epoch != operation.epoch {
        return Err(Error::refusal(
            "stale_eligibility",
            "execution authority changed",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_group_rebuild(
    authority: &Authority,
    request: &OperationRequest,
) -> Result<()> {
    inputs::check_rebuild(authority, request)?;
    refresh::check_rebuild(authority, request)?;
    if matches!(request, OperationRequest::Build(build) if build.base.is_none())
        && authority
            .selected
            .as_ref()
            .is_some_and(|selected| !selected.computed.is_empty())
    {
        return Err(Error::refusal(
            "dependent_groups",
            "provide the selected base to rebuild its computed groups together",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_definition(
    authority: &Authority,
    spec: &OperationRequest,
) -> Result<()> {
    if !authority.definition.is_empty() && authority.definition != spec.definition() {
        return Err(Error::refusal(
            "stale_definition",
            "activate the definition explicitly before running",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_update_eligibility(
    authority: &Authority,
    request: &OperationRequest,
) -> Result<()> {
    if matches!(
        request,
        OperationRequest::Update(_) | OperationRequest::Schema(_) | OperationRequest::Evidence(_)
    ) && authority
        .selected
        .as_ref()
        .is_some_and(|base| base.eligibility != authority.eligibility)
    {
        return Err(Error::refusal(
            "stale_eligibility",
            "the selected source transformation requires requalification before incremental update",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn check_base(authority: &Authority, expected: Option<&Id>) -> Result<()> {
    if let Some(expected) = expected
        && authority
            .selected
            .as_ref()
            .is_none_or(|selected| &selected.version != expected)
    {
        return Err(Error::refusal(
            "stale_base",
            "the selected version changed; submit a new operation against its exact base",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn fingerprint(spec: &OperationRequest) -> Result<String> {
    let material = match spec {
        OperationRequest::Build(spec) => {
            let mut value = serde_json::json!({"build": {"input": spec.expected_digest, "sql": spec.sql, "definition": spec.definition, "base": spec.base, "relationship": spec.relationship}});
            if !spec.partition_by.is_empty() {
                value["build"]["partition_by"] = serde_json::to_value(&spec.partition_by)?;
            }
            value
        }
        OperationRequest::Update(spec) => {
            let mut value = serde_json::json!({"update": {"base": spec.base, "keys": spec.keys, "replacements": spec.replacements.digest, "deletions": spec.deletions.as_ref().map(|input| &input.digest), "definition": spec.definition, "relationship": spec.relationship}});
            if let Some(merge) = &spec.merge {
                value["update"]["merge"] = serde_json::to_value(merge)?;
            }
            value
        }
        OperationRequest::Backfill(spec) => {
            serde_json::json!({"backfill": {"base": spec.base, "definition": spec.definition, "group": spec.group, "group_definition": spec.group_definition, "fields": spec.fields}})
        }
        OperationRequest::Refresh(spec) => {
            serde_json::json!({"refresh": {"base": spec.base, "definition": spec.definition, "transform": spec.transform, "policy": spec.policy, "mode": spec.mode, "relationship": spec.relationship}})
        }
        OperationRequest::Schema(spec) => {
            let mut value = serde_json::json!({"schema": {"base": spec.base, "definition": spec.definition,
                "next_definition": spec.next_definition, "fields": spec.fields}});
            if !spec.additions.is_empty() {
                value["schema"]["additions"] = serde_json::to_value(&spec.additions)?;
            }
            if !spec.write_defaults.is_empty() {
                value["schema"]["write_defaults"] = serde_json::to_value(&spec.write_defaults)?;
            }
            value
        }
        OperationRequest::Evidence(spec) => {
            serde_json::json!({"evidence": {"base": spec.base, "replacements": spec.replacements.digest, "deletions": spec.deletions.as_ref().map(|input| &input.digest), "definition": spec.definition}})
        }
    };
    Ok(storage::json_digest(&(PROCESSOR, material))?
        .to_hex()
        .to_string())
}
