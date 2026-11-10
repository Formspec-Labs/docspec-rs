//! External quality gates on the source-only local Build and Update lifecycle.
// Rust guideline compliant 2026-02-21
use super::*;
use arrow::datatypes::Schema;
use datafusion::physical_plan::SendableRecordBatchStream;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Evaluator {
    id: String,
    version: String,
    distribution_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Semantics {
    id: String,
    revision: String,
}

/// Immutable externally evaluated requirements and native selection preconditions.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationPolicy {
    id: Id,
    rules_sha256: String,
    evaluator: Evaluator,
    semantics: Semantics,
    required_checks: Vec<String>,
    expected_epoch: u64,
    expected_selection: Option<VersionRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckOutcome {
    id: String,
    status: CheckStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Passed,
    Failed,
    Unknown,
    NotRun,
}

/// One evaluator observation, tied to the exact protected read and rules.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalEvidence {
    policy_id: Id,
    candidate: VersionRef,
    rules: Artifact,
    evaluator: Evaluator,
    semantics: Semantics,
    checks: Vec<CheckOutcome>,
    native_report: Artifact,
    native_read: Artifact,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluatedQuality {
    status: String,
    target: EvaluatedTarget,
    evaluator: EvaluatedBy,
    rules: Artifact,
    mapping_semantics: Semantics,
    required_check_ids: Vec<String>,
    checks: Vec<EvaluatedCheck>,
    native_report: Artifact,
    native_read: ObservedArtifact,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluatedTarget {
    kind: String,
    native_version: VersionRef,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluatedBy {
    id: String,
    version: String,
    distribution_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluatedCheck {
    id: String,
    status: CheckStatus,
    native_check_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolvedRules {
    document: RuleDocument,
    required_check_ids: Vec<String>,
    mapping_semantics: Semantics,
}

#[derive(Debug, Deserialize)]
struct RuleDocument {
    id: String,
    version: String,
    schema: Vec<RuleDataset>,
}

#[derive(Debug, Deserialize)]
struct RuleDataset {
    properties: Vec<RuleField>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuleField {
    name: String,
    physical_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DccReport {
    data_contract_id: String,
    data_contract_version: String,
    datacontract_cli_version: String,
    dry_run: bool,
    result: String,
    checks: Vec<DccCheck>,
}

#[derive(Debug, Deserialize)]
struct DccCheck {
    id: String,
    key: String,
    result: String,
}

#[derive(Debug, Deserialize)]
struct ObservedArtifact {
    path: PathBuf,
    sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct AcceptedQualification {
    identity: String,
    evidence: ExternalEvidence,
    /// Original verified bytes; legacy acceptances did not retain their rule artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolved_rules_json: Option<String>,
    native_report: serde_json::Value,
    evaluator_report: serde_json::Value,
    native_read: NativeReadReceipt,
}

#[derive(Deserialize)]
struct AcceptedIdentity {
    identity: String,
}

#[derive(Deserialize)]
struct AcceptedReplay {
    identity: String,
    evidence: ExternalEvidence,
}

/// Commands for local, source-only, quality-gated Build and Update operations.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedBuildCommand {
    Prepare {
        build: RunSpec,
        policy: QualificationPolicy,
    },
    PrepareUpdate {
        update: UpdateSpec,
        policy: QualificationPolicy,
    },
    Inspect {
        dataset: Id,
        operation: Id,
    },
    Requalify {
        dataset: Id,
        operation: Id,
        previous_policy_id: Id,
        policy: QualificationPolicy,
    },
    Accept {
        dataset: Id,
        operation: Id,
        evidence: Box<ExternalEvidence>,
    },
    Select {
        dataset: Id,
        operation: Id,
        policy_id: Id,
    },
}

/// Current retained facts for one operation, including completed reconciliation.
#[derive(Debug, Serialize)]
pub struct ManagedBuildReceipt {
    dataset: Id,
    operation: Id,
    request_digest: String,
    computation_identity: String,
    policy: QualificationPolicy,
    candidate: Option<VersionRef>,
    accepted_qualification: Option<String>,
    selection: &'static str,
}

/// Exact native source version to stream through the local workspace owner.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeVersionRead {
    pub version: VersionRef,
    pub receipt_path: PathBuf,
}

/// Observations emitted after an exact protected Arrow stream reaches EOF.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReadReceipt {
    #[serde(default, skip_serializing_if = "uuid::Uuid::is_nil")]
    pub read_id: uuid::Uuid,
    pub version: VersionRef,
    pub schema_id: i32,
    pub schema: Schema,
    pub rows: u64,
    pub ipc_sha256: String,
}

pub(crate) struct ProtectedSourceTable {
    pub table: iceberg::table::Table,
    pub memory: tokio::sync::OwnedSemaphorePermit,
    pub schema_id: i32,
    pub rows: u64,
    pub metadata_sha256: sha2::digest::Output<Sha256>,
    pub metadata_size: u64,
}

pub(super) fn initial_policy_key(operation: &Id) -> String {
    format!("managed-quality-policy/{operation}")
}

fn active_policy_key(operation: &Id) -> String {
    format!("managed-quality-current/{operation}")
}

fn policy_key(operation: &Id, policy: &Id) -> String {
    format!("managed-quality-policy/{operation}/{policy}")
}

fn read_key(receipt: &NativeReadReceipt) -> Result<String> {
    // Serialize through a Value so Arrow field metadata key ordering is stable.
    let canonical = serde_json::to_value(receipt)?;
    Ok(format!(
        "managed-quality-read/{}/{}",
        receipt.version.version,
        storage::json_digest(&canonical)?.to_hex(),
    ))
}

/// Names companion records decoded while validating quality control metadata.
///
/// # Errors
/// Refuses malformed operation keys or policy identifiers.
pub(in crate::engine) fn retention_companions(
    key: &str,
    json: &str,
    rows: &crate::catalog::SnapshotRows,
) -> Result<Vec<String>> {
    let operation = Id::new(key.split('/').nth(1).ok_or_else(unknown_operation)?)?;
    let mut keys = vec![
        format!("request/{operation}"),
        initial_policy_key(&operation),
    ];
    let policy = if key == active_policy_key(&operation) {
        Some(serde_json::from_str::<Id>(json)?)
    } else if let Some(id) = key.strip_prefix(&format!("managed-quality-evidence/{operation}/")) {
        Some(Id::new(id)?)
    } else {
        None
    };
    if let Some(policy) = policy {
        let key = policy_key(&operation, &policy);
        if rows.contains_key(&key) {
            keys.push(key);
        }
    }
    Ok(keys)
}

/// Validates quality control metadata without adding a payload lifetime.
pub(in crate::engine) fn retention_record(
    key: &str,
    json: &str,
    rows: &crate::catalog::SnapshotRows,
    table: &str,
) -> Result<Option<serde_json::Value>> {
    if !key.starts_with("managed-quality-") {
        return Ok(None);
    }
    let invalid = || {
        Error::refusal(
            "retention_inventory",
            format!("managed quality identity differs for {key}"),
        )
    };
    let operation = Id::new(key.split('/').nth(1).ok_or_else(invalid)?)?;
    let (_, _, request): (_, _, OperationRequest) = rows
        .get_record(&format!("request/{operation}"))?
        .ok_or_else(invalid)?;
    let dataset = request.dataset();
    if request.operation() != &operation
        || !matches!(
            &request,
            OperationRequest::Build(_) | OperationRequest::Update(_)
        )
        || crate::catalog::lifecycle_ident(dataset).to_string() != table
    {
        return Err(invalid());
    }
    let original: QualificationPolicy = rows
        .get_record(&initial_policy_key(&operation))?
        .map(|(_, _, policy)| policy)
        .ok_or_else(invalid)?;
    let policy_for = |id: &Id| -> Result<QualificationPolicy> {
        if &original.id == id {
            return Ok(original.clone());
        }
        rows.get_record(&policy_key(&operation, id))?
            .map(|(_, _, policy)| policy)
            .ok_or_else(invalid)
    };
    let known = if key.starts_with("managed-quality-policy/") {
        let policy: QualificationPolicy = serde_json::from_str(json)?;
        policy.validate()?;
        if key != initial_policy_key(&operation) && key != policy_key(&operation, &policy.id) {
            return Err(invalid());
        }
        serde_json::to_value(policy)?
    } else if key == active_policy_key(&operation) {
        let active: Id = serde_json::from_str(json)?;
        if policy_for(&active)?.id != active {
            return Err(invalid());
        }
        serde_json::to_value(active)?
    } else if key.starts_with("managed-quality-evidence/") {
        let accepted: AcceptedQualification = serde_json::from_str(json)?;
        let evidence = &accepted.evidence;
        if key != evidence_key(&operation, &evidence.policy_id) {
            return Err(invalid());
        }
        let policy = policy_for(&evidence.policy_id)?;
        if evidence.candidate.dataset != *dataset
            || evidence.candidate.version != operation
            || evidence.candidate.manifest.owner != *dataset
            || evidence.rules.sha256 != policy.rules_sha256
            || evidence.evaluator != policy.evaluator
            || evidence.semantics != policy.semantics
            || accepted.native_read.version != evidence.candidate
            || accepted.identity != qualification_identity(&policy, evidence)?
            || accepted.resolved_rules_json.as_ref().is_some_and(|rules| {
                format!("{:x}", Sha256::digest(rules.as_bytes())) != evidence.rules.sha256
            })
        {
            return Err(invalid());
        }
        serde_json::to_value(accepted)?
    } else if key.starts_with("managed-quality-read/") {
        let read: NativeReadReceipt = serde_json::from_str(json)?;
        if key != read_key(&read)?
            || read.version.dataset != *dataset
            || read.version.version != operation
            || read.version.manifest.owner != *dataset
            || !valid_digest(&read.ipc_sha256)
        {
            return Err(invalid());
        }
        serde_json::to_value(read)?
    } else {
        return Err(invalid());
    };
    Ok(Some(known))
}

fn evidence_key(operation: &Id, policy: &Id) -> String {
    format!("managed-quality-evidence/{operation}/{policy}")
}

fn qualification_identity(
    policy: &QualificationPolicy,
    evidence: &ExternalEvidence,
) -> Result<String> {
    Ok(storage::json_digest(&(
        policy,
        &evidence.candidate,
        &evidence.checks,
        &evidence.native_report.sha256,
        &evidence.native_read.sha256,
    ))?
    .to_hex()
    .to_string())
}

fn invalid_policy() -> Error {
    Error::refusal(
        "invalid_qualification_policy",
        "invalid external quality policy",
    )
}

fn quality_pending() -> Error {
    Error::refusal(
        "qualification_required",
        "current exact candidate lacks accepted quality evidence",
    )
}

fn quality_mismatch() -> Error {
    Error::refusal(
        "qualification_mismatch",
        "quality evidence does not match the exact candidate, rules, evaluator, or required outcomes",
    )
}

fn unknown_operation() -> Error {
    Error::refusal(
        "unknown_operation",
        "managed operation has no retained admission",
    )
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn source_only(release: &ReleasePin) -> Result<&TablePin> {
    let dataset = &release.dataset;
    if dataset.groups.len() == 1
        && dataset.computed.is_empty()
        && dataset.related.is_none()
        && let Some(source) = dataset.groups.get("source")
    {
        return Ok(source);
    }
    Err(Error::refusal(
        "unsupported_native_mapping",
        "the local exact-version profile requires a single source group without computed groups",
    ))
}

pub(super) fn check_quality_base(authority: &Authority) -> Result<()> {
    let base = authority.release.as_ref().ok_or_else(|| {
        Error::refusal(
            "invalid_base",
            "managed Update requires a selected source base",
        )
    })?;
    source_only(base)?;
    if base.dataset.relationship != SourceRelationship::Independent {
        return Err(Error::refusal(
            "unsupported_managed_update",
            "managed Update requires an independent source base",
        ));
    }
    Ok(())
}

// Revision 2 records completed bounded evaluator execution separately from raw DCC checks.
const QUALITY_EXECUTION_COMPLETE: &str = "ddk:quality-execution-complete";

impl QualificationPolicy {
    fn validate(&self) -> Result<()> {
        if !valid_digest(&self.rules_sha256)
            || self.evaluator.id != "urn:ddk:quality:datacontract-cli"
            || self.evaluator.version != "1.2.9"
            || !valid_digest(&self.evaluator.distribution_sha256)
            || self.semantics.id != "urn:ddk:quality:duckdb-direct-view"
            || !matches!(self.semantics.revision.as_str(), "1" | "2")
            || (self.semantics.revision == "2"
                && !self
                    .required_checks
                    .iter()
                    .any(|id| id == QUALITY_EXECUTION_COMPLETE))
            || self.required_checks.is_empty()
            || self.required_checks.iter().any(String::is_empty)
            || self.required_checks.iter().collect::<BTreeSet<_>>().len()
                != self.required_checks.len()
        {
            return Err(invalid_policy());
        }
        Ok(())
    }

    pub(super) fn check_authority(&self, authority: &Authority) -> Result<()> {
        if authority.epoch != self.expected_epoch
            || authority.release.as_ref().map(ReleasePin::version_ref) != self.expected_selection
        {
            return Err(Error::refusal(
                "stale_qualification_authority",
                "selection or authority epoch differs from the admitted quality policy",
            ));
        }
        Ok(())
    }
}

impl ManagedBuildCommand {
    /// Returns the dataset and operation carried by every managed action.
    pub fn identity(&self) -> (&Id, &Id) {
        match self {
            Self::Prepare { build, .. } => (&build.dataset, &build.operation),
            Self::PrepareUpdate { update, .. } => (&update.dataset, &update.operation),
            Self::Inspect { dataset, operation }
            | Self::Requalify {
                dataset, operation, ..
            }
            | Self::Accept {
                dataset, operation, ..
            }
            | Self::Select {
                dataset, operation, ..
            } => (dataset, operation),
        }
    }
}

impl Engine {
    /// Applies an explicit request to the local Build or Update lifecycle.
    ///
    /// # Errors
    /// Refuses incompatible requests, unavailable candidate evidence, or stale authority.
    pub async fn managed_build(&self, command: ManagedBuildCommand) -> Result<ManagedBuildReceipt> {
        if self.inner.objects.is_remote() || !self.inner.owned_catalog {
            return Err(Error::refusal(
                "unsupported_backend",
                "managed quality supports the local catalog",
            ));
        }
        let memory = self.request_memory(&command)?;
        self.admit_work(memory, move |engine| async move {
            match command {
                ManagedBuildCommand::Prepare { build, policy } => {
                    if build.base.is_some() || !matches!(build.relationship, None | Some(SourceRelationship::Independent)) {
                        return Err(Error::refusal(
                            "unsupported_managed_build",
                            "managed quality supports an independent source-only Build with no inherited base",
                        ));
                    }
                    engine.prepare_qualified(OperationRequest::Build(build), &policy).await
                }
                ManagedBuildCommand::PrepareUpdate { update, policy } => {
                    if !matches!(update.relationship, None | Some(SourceRelationship::Independent)) {
                        return Err(Error::refusal(
                            "unsupported_managed_update",
                            "managed quality supports an independent source-only Update",
                        ));
                    }
                    engine.prepare_qualified(OperationRequest::Update(update), &policy).await
                }
                ManagedBuildCommand::Inspect { dataset, operation } => {
                    engine.managed_status(&dataset, &operation).await
                }
                ManagedBuildCommand::Requalify { dataset, operation, previous_policy_id, policy } => {
                    policy.validate()?;
                    engine.change_quality_policy(&dataset, &operation, &previous_policy_id, &policy).await?;
                    engine.managed_status(&dataset, &operation).await
                }
                ManagedBuildCommand::Accept { dataset, operation, evidence } => {
                    engine.accept_quality(&dataset, &operation, *evidence).await?;
                    engine.managed_status(&dataset, &operation).await
                }
                ManagedBuildCommand::Select { dataset, operation, policy_id } => {
                    engine.select_qualified(&dataset, &operation, &policy_id).await?;
                    engine.managed_status(&dataset, &operation).await
                }
            }
        }).await
    }

    async fn prepare_qualified(
        &self,
        spec: OperationRequest,
        policy: &QualificationPolicy,
    ) -> Result<ManagedBuildReceipt> {
        policy.validate()?;
        self.check_request_backend(&spec)?;
        let candidate = self.prepare_and_select(&spec, None, Some(policy)).await?;
        source_only(&candidate)?;
        self.managed_status(spec.dataset(), spec.operation()).await
    }

    pub(super) async fn initial_qualification(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<Option<QualificationPolicy>> {
        self.inner
            .lifecycle
            .get(view, &initial_policy_key(operation))
            .await
    }

    async fn quality_policy(&self, view: &View, operation: &Id) -> Result<QualificationPolicy> {
        let initial = self
            .initial_qualification(view, operation)
            .await?
            .ok_or_else(unknown_operation)?;
        let active: Option<Id> = self
            .inner
            .lifecycle
            .get(view, &active_policy_key(operation))
            .await?;
        match active {
            None => Ok(initial),
            Some(id) => self
                .inner
                .lifecycle
                .get(view, &policy_key(operation, &id))
                .await?
                .ok_or_else(|| {
                    Error::refusal("corrupt_phase", "active qualification policy is absent")
                }),
        }
    }

    async fn quality_candidate(&self, view: &View, operation: &Id) -> Result<Option<ReleasePin>> {
        let readback: Option<ValidatedReadback> = self
            .inner
            .lifecycle
            .get(view, &format!("readback/{operation}"))
            .await?;
        readback
            .map(|record| {
                record.require_current()?;
                source_only(&record.release)?;
                Ok(record.release)
            })
            .transpose()
    }

    async fn managed_status(&self, dataset: &Id, operation: &Id) -> Result<ManagedBuildReceipt> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(dataset).await?;
        let lifecycle = &self.inner.lifecycle;
        let admission: Operation = lifecycle
            .get(&view, &format!("operation/{operation}"))
            .await?
            .ok_or_else(unknown_operation)?;
        let policy = self.quality_policy(&view, operation).await?;
        let result: Option<ReleasePin> =
            lifecycle.get(&view, &format!("result/{operation}")).await?;
        let (candidate, selection) = if let Some(release) = result {
            (Some(release), "selected")
        } else {
            self.check_operation_available(&view, operation).await?;
            let selection = match self.check_not_abandoned(&view, operation).await {
                Ok(()) => "pending",
                Err(error) if error.code() == "operation_abandoned" => "refused",
                Err(error) => return Err(error),
            };
            (self.quality_candidate(&view, operation).await?, selection)
        };
        let accepted_key = evidence_key(operation, &policy.id);
        let _accepted_memory = lifecycle
            .rows(&view)
            .await?
            .reserve_decoded(&lifecycle.budget, &[&accepted_key])?;
        let accepted: Option<AcceptedIdentity> = lifecycle.get(&view, &accepted_key).await?;
        let spec: OperationRequest = lifecycle
            .get(&view, &format!("request/{operation}"))
            .await?
            .ok_or_else(unknown_operation)?;
        let initial_policy = self
            .initial_qualification(&view, operation)
            .await?
            .ok_or_else(unknown_operation)?;
        let request_digest = storage::json_digest(&(
            dataset,
            operation,
            &admission.request_digest,
            &initial_policy,
        ))?
        .to_hex()
        .to_string();
        let computation_identity = phase_key(
            &spec,
            &admission.request_digest,
            admission.eligibility,
            "processing",
            admission.base_manifest.as_ref(),
            admission.schema_key.as_ref(),
        )?;
        Ok(ManagedBuildReceipt {
            dataset: dataset.clone(),
            operation: operation.clone(),
            request_digest,
            computation_identity,
            policy,
            candidate: candidate.map(|release| release.version_ref()),
            accepted_qualification: accepted.map(|record| record.identity),
            selection,
        })
    }

    async fn change_quality_policy(
        &self,
        dataset: &Id,
        operation: &Id,
        previous: &Id,
        policy: &QualificationPolicy,
    ) -> Result<()> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(dataset).await?;
        let lifecycle = &self.inner.lifecycle;
        self.check_operation_available(&view, operation).await?;
        self.check_not_abandoned(&view, operation).await?;
        let active = self.quality_policy(&view, operation).await?;
        if &active == policy {
            return Ok(());
        }
        let initial = self
            .initial_qualification(&view, operation)
            .await?
            .ok_or_else(unknown_operation)?;
        let key = policy_key(operation, &policy.id);
        if &active.id != previous
            || policy.id == initial.id
            || policy.expected_epoch != initial.expected_epoch
            || policy.expected_selection != initial.expected_selection
            || lifecycle
                .get::<QualificationPolicy>(&view, &key)
                .await?
                .is_some()
            || lifecycle
                .get::<ReleasePin>(&view, &format!("result/{operation}"))
                .await?
                .is_some()
        {
            return Err(Error::refusal(
                "stale_qualification_policy",
                "cannot replace this managed policy",
            ));
        }
        self.quality_candidate(&view, operation)
            .await?
            .ok_or_else(quality_pending)?;
        lifecycle
            .commit(
                &view,
                vec![
                    record(key, policy)?,
                    record(active_policy_key(operation), &policy.id)?,
                ],
            )
            .await?;
        Ok(())
    }

    async fn reuse_accepted_quality(
        &self,
        dataset: &Id,
        operation: &Id,
        evidence: &ExternalEvidence,
    ) -> Result<bool> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(dataset).await?;
        self.check_operation_available(&view, operation).await?;
        self.check_not_abandoned(&view, operation).await?;
        let policy = self.quality_policy(&view, operation).await?;
        let lifecycle = &self.inner.lifecycle;
        let accepted_key = evidence_key(operation, &policy.id);
        let result_key = format!("result/{operation}");
        let readback_key = format!("readback/{operation}");
        let rows = lifecycle.rows(&view).await?;
        let _accepted_memory = rows.reserve_decoded(&lifecycle.budget, &[&accepted_key])?;
        let accepted: Option<AcceptedReplay> = lifecycle.get(&view, &accepted_key).await?;
        let Some(accepted) = accepted else {
            return Ok(false);
        };
        if accepted.evidence != *evidence || evidence.policy_id != policy.id {
            return Ok(false);
        }
        let _candidate_memory = rows.reserve_decoded(
            &lifecycle.budget,
            &[if rows.contains_key(&result_key) {
                &result_key
            } else {
                &readback_key
            }],
        )?;
        let release = match lifecycle.get::<ReleasePin>(&view, &result_key).await? {
            Some(release) => release,
            None => self
                .quality_candidate(&view, operation)
                .await?
                .ok_or_else(quality_pending)?,
        };
        source_only(&release)?;
        if evidence.candidate != release.version_ref()
            || accepted.identity != qualification_identity(&policy, evidence)?
        {
            return Err(quality_mismatch());
        }
        // This acknowledges existing acceptance, including after a successor
        // selection. Mutable selection authority still guards any new selection.
        Ok(true)
    }

    async fn accept_quality(
        &self,
        dataset: &Id,
        operation: &Id,
        evidence: ExternalEvidence,
    ) -> Result<()> {
        if self
            .reuse_accepted_quality(dataset, operation, &evidence)
            .await?
        {
            return Ok(());
        }
        let lifecycle = &self.inner.lifecycle;
        // Keep each verified artifact and its decoder allowance alive through acceptance.
        let (rules, _rules_memory) = read_artifact(&evidence.rules, &lifecycle.budget)?;
        let (report, _report_memory) = read_artifact(&evidence.native_report, &lifecycle.budget)?;
        let (read, _read_memory) = read_artifact(&evidence.native_read, &lifecycle.budget)?;
        let evaluated: EvaluatedQuality = serde_json::from_slice(&report)?;
        let (evaluator_report, _evaluator_memory) =
            read_artifact(&evaluated.native_report, &lifecycle.budget)?;
        let native_read: NativeReadReceipt = serde_json::from_slice(&read)?;
        let native_report = serde_json::from_slice(&report)?;
        let evaluator_report = serde_json::from_slice(&evaluator_report)?;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(dataset).await?;
        self.check_operation_available(&view, operation).await?;
        self.check_not_abandoned(&view, operation).await?;
        let policy = self.quality_policy(&view, operation).await?;
        let release = self
            .quality_candidate(&view, operation)
            .await?
            .ok_or_else(quality_pending)?;
        let pin = source_only(&release)?;
        let recorded_read: Option<NativeReadReceipt> =
            lifecycle.get(&view, &read_key(&native_read)?).await?;
        if evidence.policy_id != policy.id
            || evaluated.target.kind != "native"
            || evaluated.target.native_version != evidence.candidate
            || evaluated.evaluator.id != evidence.evaluator.id
            || evaluated.evaluator.version != evidence.evaluator.version
            || evaluated.evaluator.distribution_sha256 != evidence.evaluator.distribution_sha256
            || evaluated.rules != evidence.rules
            || evaluated.mapping_semantics != evidence.semantics
            || evaluated.native_read.path != evidence.native_read.path
            || evaluated.native_read.sha256 != evidence.native_read.sha256
            || evidence.candidate != release.version_ref()
            || evidence.candidate != native_read.version
            || pin.schema_id != native_read.schema_id
            || pin.rows != native_read.rows
            || !valid_digest(&native_read.ipc_sha256)
            || evidence.rules.sha256 != policy.rules_sha256
            || evidence.evaluator != policy.evaluator
            || evidence.semantics != policy.semantics
        {
            return Err(quality_mismatch());
        }
        if recorded_read.as_ref() != Some(&native_read) {
            if lifecycle
                .get::<ReleasePin>(&view, &format!("result/{operation}"))
                .await?
                .is_some()
            {
                return Err(Error::refusal(
                    "qualification_audit_only",
                    "an exact read after selection is audit evidence",
                ));
            }
            return Err(quality_mismatch());
        }
        let declared_required = policy.required_checks.iter().collect::<BTreeSet<_>>();
        let evaluated_required = evaluated.required_check_ids.iter().collect::<BTreeSet<_>>();
        let submitted = evidence
            .checks
            .iter()
            .map(|check| (&check.id, check.status))
            .collect::<BTreeMap<_, _>>();
        let observed = evaluated
            .checks
            .iter()
            .map(|check| (&check.id, check.status))
            .collect::<BTreeMap<_, _>>();
        if submitted.len() != evidence.checks.len()
            || observed.len() != evaluated.checks.len()
            || evaluated_required != declared_required
            || evaluated_required.len() != evaluated.required_check_ids.len()
            || declared_required
                .iter()
                .any(|id| !submitted.contains_key(id))
        {
            return Err(Error::refusal(
                "qualification_coverage",
                "qualification requires one outcome for every admitted required check",
            ));
        }
        if submitted != observed {
            return Err(quality_mismatch());
        }
        if declared_required
            .iter()
            .any(|id| submitted.get(id) == Some(&CheckStatus::Failed))
        {
            return Err(Error::refusal(
                "qualification_failed",
                "a required evaluator check failed",
            ));
        }
        if declared_required.iter().any(|id| {
            matches!(
                submitted.get(id),
                Some(CheckStatus::Unknown | CheckStatus::NotRun)
            )
        }) {
            return Err(Error::refusal(
                "qualification_unknown",
                "the evaluator could not establish all required outcomes",
            ));
        }
        validate_dcc(
            &evaluated,
            &serde_json::from_slice(&rules)?,
            DccReport::deserialize(&evaluator_report)?,
            &native_read,
        )?;
        let key = evidence_key(operation, &policy.id);
        let identity = qualification_identity(&policy, &evidence)?;
        let _accepted_memory = lifecycle
            .rows(&view)
            .await?
            .reserve_decoded(&lifecycle.budget, &[&key])?;
        if let Some(accepted) = lifecycle.get::<AcceptedIdentity>(&view, &key).await? {
            if accepted.identity == identity {
                return Ok(());
            }
            return Err(Error::refusal(
                "qualification_conflict",
                "a different result is already accepted for the policy",
            ));
        }
        if lifecycle
            .get::<ReleasePin>(&view, &format!("result/{operation}"))
            .await?
            .is_some()
        {
            return Err(Error::refusal(
                "qualification_conflict",
                "cannot change the selected qualification",
            ));
        }
        let accepted = AcceptedQualification {
            identity,
            evidence,
            resolved_rules_json: Some(
                std::str::from_utf8(&rules)
                    .map_err(|_| quality_mismatch())?
                    .to_owned(),
            ),
            native_report,
            evaluator_report,
            native_read,
        };
        lifecycle
            .commit(&view, vec![record(key, accepted)?])
            .await?;
        Ok(())
    }

    async fn select_qualified(&self, dataset: &Id, operation: &Id, policy_id: &Id) -> Result<()> {
        let lifecycle = &self.inner.lifecycle;
        let (spec, admission, release) = {
            let lock = self.lock(dataset).await;
            let _guard = lock.lock().await;
            let view = self.existing_lifecycle_view(dataset).await?;
            let policy = self.quality_policy(&view, operation).await?;
            if &policy.id != policy_id {
                return Err(Error::refusal(
                    "stale_qualification_policy",
                    "selection requested a stale quality policy",
                ));
            }
            if lifecycle
                .get::<ReleasePin>(&view, &format!("result/{operation}"))
                .await?
                .is_some()
            {
                return Ok(());
            }
            self.check_operation_available(&view, operation).await?;
            let spec: OperationRequest = lifecycle
                .get(&view, &format!("request/{operation}"))
                .await?
                .ok_or_else(unknown_operation)?;
            let admission: Operation = lifecycle
                .get(&view, &format!("operation/{operation}"))
                .await?
                .ok_or_else(unknown_operation)?;
            let release = Retained {
                value: self
                    .quality_candidate(&view, operation)
                    .await?
                    .ok_or_else(quality_pending)?,
                protections: self
                    .retained_protections(&view, &format!("readback/{operation}"))
                    .await?,
            };
            (spec, admission, release)
        };
        self.select_prepared_with_policy(&spec, &admission, release, None, Some(policy_id))
            .await?;
        Ok(())
    }

    pub(super) async fn guard_completed_policy(
        &self,
        view: &View,
        operation: &Id,
        requested: &Id,
    ) -> Result<()> {
        if &self.quality_policy(view, operation).await?.id != requested {
            return Err(Error::refusal(
                "stale_qualification_policy",
                "selection requested a stale quality policy",
            ));
        }
        Ok(())
    }

    pub(super) async fn guard_external_qualification(
        &self,
        view: &View,
        authority: &Authority,
        operation: &Id,
        release: &ReleasePin,
        requested: Option<&Id>,
    ) -> Result<()> {
        if self.initial_qualification(view, operation).await?.is_none() {
            return Ok(());
        }
        let policy = self.quality_policy(view, operation).await?;
        if requested != Some(&policy.id) {
            return Err(Error::refusal(
                "stale_qualification_policy",
                "selection requires the active quality policy",
            ));
        }
        policy.check_authority(authority)?;
        self.check_operation_available(view, operation).await?;
        let lifecycle = &self.inner.lifecycle;
        let accepted_key = evidence_key(operation, &policy.id);
        let _accepted_memory = lifecycle
            .rows(view)
            .await?
            .reserve_decoded(&lifecycle.budget, &[&accepted_key])?;
        let accepted: AcceptedReplay = lifecycle
            .get(view, &accepted_key)
            .await?
            .ok_or_else(quality_pending)?;
        if accepted.evidence.candidate != release.version_ref() {
            return Err(quality_pending());
        }
        Ok(())
    }

    async fn read_source_version(
        &self,
        version: &VersionRef,
    ) -> Result<(i32, SendableRecordBatchStream)> {
        if self.inner.objects.is_remote() || !self.inner.owned_catalog {
            return Err(Error::refusal(
                "unsupported_backend",
                "exact source version requires the local catalog",
            ));
        }
        let release = self.exact_source_release(version).await?;
        let schema_id = source_only(&release)?.schema_id;
        let stream = self.read_group(&release, "source").await?;
        Ok((schema_id, stream))
    }

    /// Streams the complete exact local source view and records its identity.
    ///
    /// # Errors
    /// Refuses unavailable versions, ambiguous mappings, capacity or write failures.
    pub async fn write_source_version(
        &self,
        version: &VersionRef,
        target: impl Write,
    ) -> Result<NativeReadReceipt> {
        let (schema_id, mut stream) = self.read_source_version(version).await?;
        let schema = stream.schema().as_ref().clone();
        let output = IpcDigest {
            writer: target,
            digest: Sha256::new(),
        };
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(output, &schema)?;
        let mut rows = 0_u64;
        while let Some(batch) = stream.try_next().await? {
            rows = rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                Error::refusal("metadata_capacity", "native read row count overflow")
            })?;
            writer.write(&batch)?;
        }
        let mut output = writer.into_inner()?;
        output.flush()?;
        let receipt = NativeReadReceipt {
            read_id: uuid::Uuid::new_v4(),
            version: version.clone(),
            schema_id,
            schema,
            rows,
            ipc_sha256: format!("{:x}", output.digest.finalize()),
        };
        self.record_quality_read(&receipt).await?;
        Ok(receipt)
    }

    async fn record_quality_read(&self, receipt: &NativeReadReceipt) -> Result<()> {
        let version = &receipt.version;
        let lock = self.lock(&version.dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(&version.dataset).await?;
        let lifecycle = &self.inner.lifecycle;
        if self
            .initial_qualification(&view, &version.version)
            .await?
            .is_none()
            || lifecycle
                .get::<ReleasePin>(&view, &format!("result/{}", version.version))
                .await?
                .is_some()
        {
            return Ok(());
        }
        self.check_operation_available(&view, &version.version)
            .await?;
        self.check_not_abandoned(&view, &version.version).await?;
        let release = self
            .quality_candidate(&view, &version.version)
            .await?
            .ok_or_else(quality_pending)?;
        let pin = source_only(&release)?;
        if release.version_ref() != *version
            || pin.rows != receipt.rows
            || pin.schema_id != receipt.schema_id
            || !valid_digest(&receipt.ipc_sha256)
        {
            return Err(Error::refusal(
                "release_mismatch",
                "completed read disagrees with the retained candidate",
            ));
        }
        let key = read_key(receipt)?;
        if lifecycle
            .get::<NativeReadReceipt>(&view, &key)
            .await?
            .is_none()
        {
            lifecycle.commit(&view, vec![record(key, receipt)?]).await?;
        }
        Ok(())
    }

    pub(crate) async fn protected_source_table(
        &self,
        version: &VersionRef,
        budget: &Budget,
    ) -> Result<ProtectedSourceTable> {
        if self.inner.objects.is_remote() || !self.inner.owned_catalog {
            return Err(Error::refusal(
                "unsupported_backend",
                "exact source version requires the local catalog",
            ));
        }
        let release = self.exact_source_release(version).await?;
        let view = self.check_read_release(&release).await?;
        let canonical = source_only(&release)?;
        let pin = self
            .representation_pin(&view, &release.id, "source", canonical)
            .await?;
        let (table, memory, metadata_sha256) =
            storage::open_local_pin_observed(&pin, budget).await?;
        Ok(ProtectedSourceTable {
            table,
            memory,
            schema_id: canonical.schema_id,
            rows: pin.rows,
            metadata_sha256,
            metadata_size: pin.metadata.bytes,
        })
    }

    async fn exact_source_release(&self, version: &VersionRef) -> Result<ReleasePin> {
        let lock = self.lock(&version.dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_lifecycle_view(&version.dataset).await?;
        self.check_operation_available(&view, &version.version)
            .await?;
        let lifecycle = &self.inner.lifecycle;
        let selected: Option<ReleasePin> = lifecycle
            .get(&view, &format!("result/{}", version.version))
            .await?;
        let release = match selected {
            Some(release) => release,
            None => self
                .quality_candidate(&view, &version.version)
                .await?
                .ok_or_else(|| {
                    Error::refusal("no_release", "exact source version is unavailable")
                })?,
        };
        if release.version_ref() != *version {
            return Err(Error::refusal(
                "release_mismatch",
                "exact source version differs from retained release",
            ));
        }
        source_only(&release)?;
        Ok(release)
    }
}

fn validate_dcc(
    evaluated: &EvaluatedQuality,
    rules: &ResolvedRules,
    raw: DccReport,
    read: &NativeReadReceipt,
) -> Result<()> {
    let [dataset] = rules.document.schema.as_slice() else {
        return Err(quality_mismatch());
    };
    if evaluated.status != "passed"
        || raw.result != "passed"
        || raw.dry_run
        || raw.data_contract_id != rules.document.id
        || raw.data_contract_version != rules.document.version
        || raw.datacontract_cli_version != evaluated.evaluator.version
        || rules.required_check_ids != evaluated.required_check_ids
        || rules.mapping_semantics != evaluated.mapping_semantics
    {
        return Err(quality_mismatch());
    }
    if rules.mapping_semantics.revision == "2"
        && (!rules
            .required_check_ids
            .iter()
            .any(|id| id == QUALITY_EXECUTION_COMPLETE)
            || evaluated
                .checks
                .iter()
                .filter(|check| {
                    check.id == QUALITY_EXECUTION_COMPLETE
                        && check.status == CheckStatus::Passed
                        && check.native_check_ids.is_empty()
                })
                .count()
                != 1)
    {
        return Err(quality_mismatch());
    }
    let expected_count = raw.checks.len();
    let mut native = raw
        .checks
        .into_iter()
        .map(|check| (check.id, (check.key, check.result)))
        .collect::<BTreeMap<_, _>>();
    if native.len() != expected_count {
        return Err(quality_mismatch());
    }
    for observed in &evaluated.checks {
        if observed.status != CheckStatus::Passed {
            return Err(quality_mismatch());
        }
        match observed.native_check_ids.as_slice() {
            [id] if native
                .remove(id)
                .is_some_and(|(key, result)| key == observed.id && result == "passed") => {}
            [] if observed.id == QUALITY_EXECUTION_COMPLETE
                && evaluated.mapping_semantics.revision == "2" => {}
            [] if observed.id == "ddk:exact-field-names"
                && dataset.properties.iter().all(|field| {
                    let name = field.physical_name.as_deref().unwrap_or(&field.name);
                    read.schema
                        .fields()
                        .iter()
                        .any(|native| native.name() == name)
                }) => {}
            _ => return Err(quality_mismatch()),
        }
    }
    if !native.is_empty() {
        return Err(quality_mismatch());
    }
    Ok(())
}

fn read_artifact(
    artifact: &Artifact,
    budget: &Budget,
) -> Result<(Vec<u8>, tokio::sync::OwnedSemaphorePermit)> {
    let file = File::open(&artifact.path)?;
    let size = file.metadata()?.len();
    let invalid = || {
        Error::refusal(
            "qualification_artifact",
            "quality artifact exceeds limits or its SHA-256 differs",
        )
    };
    if size > budget.limits.max_metadata_bytes as u64 {
        return Err(invalid());
    }
    // JSON tree, typed receipt and encoded bytes coexist during validation.
    const DECODE_ALLOWANCE: usize = 16;
    let memory = budget.reserve((size as usize).saturating_mul(DECODE_ALLOWANCE))?;
    let mut bytes = Vec::with_capacity(size as usize);
    file.take(size.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size || format!("{:x}", Sha256::digest(&bytes)) != artifact.sha256 {
        return Err(invalid());
    }
    Ok((bytes, memory))
}

struct IpcDigest<W> {
    writer: W,
    digest: Sha256,
}

impl<W: Write> Write for IpcDigest<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.writer.write(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod semantics_tests;
