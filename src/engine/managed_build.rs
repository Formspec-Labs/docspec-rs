//! External quality gates on the existing source-only local Build lifecycle.
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
    native_report: serde_json::Value,
    evaluator_report: serde_json::Value,
    native_read: NativeReadReceipt,
}

/// Commands for the local, source-only, quality-gated Build.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedBuildCommand {
    Prepare {
        build: RunSpec,
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
    pub version: VersionRef,
    pub schema_id: i32,
    pub schema: Schema,
    pub rows: u64,
    pub ipc_sha256: String,
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

fn evidence_key(operation: &Id, policy: &Id) -> String {
    format!("managed-quality-evidence/{operation}/{policy}")
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
        "managed Build has no retained admission",
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

impl QualificationPolicy {
    fn validate(&self) -> Result<()> {
        if !valid_digest(&self.rules_sha256)
            || self.evaluator.id != "urn:ddk:quality:datacontract-cli"
            || self.evaluator.version != "1.2.9"
            || !valid_digest(&self.evaluator.distribution_sha256)
            || self.semantics.id != "urn:ddk:quality:duckdb-direct-view"
            || self.semantics.revision != "1"
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
    /// Applies an explicit request to the existing local Build lifecycle.
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
                    policy.validate()?;
                    if build.base.is_some() || !matches!(build.relationship, None | Some(SourceRelationship::Independent)) {
                        return Err(Error::refusal(
                            "unsupported_managed_build",
                            "managed quality supports an independent source-only Build with no inherited base",
                        ));
                    }
                    let dataset = build.dataset.clone();
                    let operation = build.operation.clone();
                    let spec = OperationRequest::Build(build);
                    engine.check_request_backend(&spec)?;
                    let candidate = engine.prepare_and_select(&spec, None, Some(&policy)).await?;
                    source_only(&candidate)?;
                    engine.managed_status(&dataset, &operation).await
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
        let accepted: Option<AcceptedQualification> = lifecycle
            .get(&view, &evidence_key(operation, &policy.id))
            .await?;
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

    async fn accept_quality(
        &self,
        dataset: &Id,
        operation: &Id,
        evidence: ExternalEvidence,
    ) -> Result<()> {
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
        validate_dcc(
            &evaluated,
            &serde_json::from_slice(&rules)?,
            DccReport::deserialize(&evaluator_report)?,
            &native_read,
        )?;
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
        let mut checks = BTreeSet::new();
        let evaluated_required = evaluated.required_check_ids.iter().collect::<BTreeSet<_>>();
        let declared_required = policy.required_checks.iter().collect::<BTreeSet<_>>();
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
            || evaluated_required != declared_required
            || evaluated_required.len() != evaluated.required_check_ids.len()
            || evaluated.checks.len() != evidence.checks.len()
            || evaluated
                .checks
                .iter()
                .map(|check| &check.id)
                .collect::<BTreeSet<_>>()
                .len()
                != evaluated.checks.len()
            || evaluated.checks.iter().any(|observed| {
                !evidence
                    .checks
                    .iter()
                    .any(|check| check.id == observed.id && check.status == observed.status)
            })
            || evidence.candidate != release.version_ref()
            || evidence.candidate != native_read.version
            || recorded_read.as_ref() != Some(&native_read)
            || pin.schema_id != native_read.schema_id
            || pin.rows != native_read.rows
            || !valid_digest(&native_read.ipc_sha256)
            || evidence.rules.sha256 != policy.rules_sha256
            || evidence.evaluator != policy.evaluator
            || evidence.semantics != policy.semantics
            || evidence
                .checks
                .iter()
                .any(|check| !checks.insert(&check.id))
            || policy.required_checks.iter().any(|id| {
                !evidence
                    .checks
                    .iter()
                    .any(|check| &check.id == id && check.status == CheckStatus::Passed)
            })
        {
            return Err(quality_mismatch());
        }
        let key = evidence_key(operation, &policy.id);
        let identity = storage::json_digest(&(
            &policy,
            &evidence.candidate,
            &evidence.checks,
            &evidence.native_report.sha256,
            &evidence.native_read.sha256,
        ))?
        .to_hex()
        .to_string();
        if let Some(accepted) = lifecycle.get::<AcceptedQualification>(&view, &key).await? {
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
        let accepted: AcceptedQualification = self
            .inner
            .lifecycle
            .get(view, &evidence_key(operation, &policy.id))
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
    ) -> Result<(
        iceberg::table::Table,
        tokio::sync::OwnedSemaphorePermit,
        i32,
        u64,
    )> {
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
        let (table, memory) = storage::open_pin_admitted(
            &pin,
            #[cfg(feature = "remote")]
            None,
            budget,
        )
        .await?;
        Ok((table, memory, canonical.schema_id, pin.rows))
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
