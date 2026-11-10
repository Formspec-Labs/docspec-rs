//! Freeze the declared delivery of an exact selected whole-job candidate.
//!
//! The local fixture scope records native custody only. External authorization
//! and dispatch have not run. One audit and one retireable payload share the
//! original guarded lifecycle commit.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::catalog::{SnapshotRows, View, history_row_bytes};
use crate::engine::retention::{publication::publication_plan_limits, retirement::record_retired};

pub(crate) const INTENT_PREFIX: &str = "job-publication-intent/";
pub(crate) const PAYLOAD_PREFIX: &str = "job-publication-payload/";

pub(crate) fn is_record(key: &str) -> bool {
    key.starts_with(INTENT_PREFIX) || key.starts_with(PAYLOAD_PREFIX)
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Freeze(Box<Freeze>),
    Inspect { dataset: Id, operation: Id },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Freeze {
    dataset: Id,
    operation: Id,
    candidate: JobCandidateRef,
    job_request_digest: String,
    policy_id: Id,
    accepted_qualification: String,
    expected_epoch: u64,
    expected_selection_revision: i64,
    scope: PublicationScope,
    release_id: String,
    outputs: Vec<PublicationOutput>,
    controls: Vec<Control>,
    expected_pointer: ExpectedPointer,
    pointer: Pointer,
}

/// The declared local fixture destination and its pointer key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicationScope {
    LocalFixture {
        publisher: Id,
        bucket: Id,
        release_prefix: String,
        pointer_key: String,
    },
}

/// Whether a native output belongs in the declared release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicationDisposition {
    Private,
    Published { name: String, content_type: String },
}

/// One output identified by its sealed native completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationOutput {
    pub name: String,
    pub object: ObjectRef,
    pub sha256: String,
    pub md5: String,
    pub rows: u64,
    pub disposition: PublicationDisposition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    name: String,
    content_type: String,
    size: u64,
    artifact: JobArtifact,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ExpectedPointer {
    Absent,
    Present { etag: String, bytes_base64: String },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pointer {
    content_type: String,
    cache_control: String,
    bytes_base64: String,
}

/// A SHA-256 digest and exact byte count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationBytePin {
    pub sha256: String,
    pub size: u64,
}

/// A declared immutable control file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationControlPin {
    pub name: String,
    pub content_type: String,
    pub bytes: PublicationBytePin,
}

/// The original pointer condition and predecessor identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicationPointerCondition {
    Absent,
    Present {
        etag: String,
        bytes: PublicationBytePin,
    },
}

/// The declared replacement pointer and its exact byte identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationPointerPin {
    pub content_type: String,
    pub cache_control: String,
    pub bytes: PublicationBytePin,
}

/// The immutable native identity of one accepted publication intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntentRef {
    pub dataset: Id,
    pub operation: Id,
    pub intent_digest: String,
    pub native_record: JobInputRecordRef,
}

/// The compact original decision, available after payload retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntentAudit {
    pub reference: PublicationIntentRef,
    pub candidate: JobCandidateRef,
    pub job_request_digest: String,
    pub policy_id: Id,
    pub accepted_qualification: String,
    pub epoch: u64,
    pub selection: JobInputRecordRef,
    pub acceptance: JobInputRecordRef,
    pub completion: JobInputRecordRef,
    pub scope: PublicationScope,
    pub release_id: String,
    pub inventory_digest: String,
    pub controls: Vec<PublicationControlPin>,
    pub expected_pointer: PublicationPointerCondition,
    pub pointer: PublicationPointerPin,
    pub payload_digest: String,
}

/// The exact delivery metadata available while the intent remains protected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntentPayload {
    pub outputs: Vec<PublicationOutput>,
    /// Canonical base64 bytes in the order of the audit's control pins.
    pub controls_base64: Vec<String>,
    pub predecessor_bytes_base64: Option<String>,
    pub pointer_bytes_base64: String,
}

/// The native intent's availability at inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationIntentStatus {
    NotFound,
    Retained,
    Retired,
}

/// The observed native publication intent and protection state.
#[derive(Debug, Serialize)]
pub struct PublicationIntentReceipt {
    pub dataset: Id,
    pub operation: Id,
    pub status: PublicationIntentStatus,
    pub intent: Option<PublicationIntentAudit>,
    pub payload: Option<PublicationIntentPayload>,
    pub external_authorization: &'static str,
    pub dispatch: &'static str,
    #[serde(skip)]
    _memory: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PublicationIntentReceipt {
    fn new(
        dataset: Id,
        operation: Id,
        intent: Option<PublicationIntentAudit>,
        payload: Option<PublicationIntentPayload>,
    ) -> Self {
        let status = if payload.is_some() {
            PublicationIntentStatus::Retained
        } else if intent.is_some() {
            PublicationIntentStatus::Retired
        } else {
            PublicationIntentStatus::NotFound
        };
        Self {
            dataset,
            operation,
            status,
            intent,
            payload,
            external_authorization: "not_run",
            dispatch: "not_run",
            _memory: None,
        }
    }

    pub(in crate::engine) fn objects(&self) -> impl Iterator<Item = &ObjectRef> {
        self.intent
            .iter()
            .zip(self.payload.iter())
            .flat_map(|(intent, payload)| {
                std::iter::once(&intent.candidate.manifest).chain(
                    payload
                        .outputs
                        .iter()
                        .filter(|output| {
                            matches!(output.disposition, PublicationDisposition::Published { .. })
                        })
                        .map(|output| &output.object),
                )
            })
    }
}

impl Held<PublicationIntentReceipt> {
    fn into_receipt(self) -> PublicationIntentReceipt {
        let mut receipt = self.value;
        receipt._memory = Some(self._memory);
        receipt
    }
}

#[derive(Deserialize)]
struct Manifest {
    profile: String,
    dataset: Id,
    operation: Id,
    request_digest: String,
    execution: String,
    view_set_digest: String,
    outputs: Vec<CompletedOutput>,
}

#[derive(Deserialize)]
struct CompletedOutput {
    name: String,
    path: String,
    sha256: String,
    md5: String,
    size: u64,
    rows: u64,
    completion: ObjectRef,
}

fn invalid() -> Error {
    Error::refusal(
        "publication_intent_mismatch",
        "publication intent identity or inventory differs",
    )
}

fn stale() -> Error {
    Error::refusal(
        "stale_authority",
        "publication freeze requires the current selected whole-job acceptance",
    )
}

fn component(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn header(value: &str) -> bool {
    !value.trim().is_empty()
        && value
            .bytes()
            .all(|byte| byte == b' ' || byte.is_ascii_graphic())
}

fn capture_pointer(bytes_base64: &str) -> Result<PublicationBytePin> {
    let bytes = STANDARD.decode(bytes_base64).map_err(|_| invalid())?;
    if bytes.is_empty() || STANDARD.encode(&bytes) != bytes_base64 {
        return Err(invalid());
    }
    Ok(PublicationBytePin {
        sha256: sha256(&bytes),
        size: bytes.len() as u64,
    })
}

impl Freeze {
    fn validate(&self, budget: &Budget) -> Result<()> {
        let PublicationScope::LocalFixture {
            release_prefix,
            pointer_key,
            ..
        } = &self.scope;
        if !release_prefix.split('/').all(component)
            || !pointer_key.split('/').all(component)
            || !valid_sha256(&self.release_id)
            || !header(&self.pointer.content_type)
            || !header(&self.pointer.cache_control)
        {
            return Err(invalid());
        }
        let mut names = BTreeSet::new();
        let mut native = BTreeSet::new();
        for output in &self.outputs {
            if !native.insert(&output.name) {
                return Err(invalid());
            }
            if let PublicationDisposition::Published { name, content_type } = &output.disposition
                && (!component(name) || !header(content_type) || !names.insert(name.as_str()))
            {
                return Err(invalid());
            }
        }
        let mut inline_bytes = self.pointer.bytes_base64.len();
        if let ExpectedPointer::Present { bytes_base64, .. } = &self.expected_pointer {
            inline_bytes = inline_bytes
                .checked_add(bytes_base64.len())
                .ok_or_else(capacity)?;
        }
        for control in &self.controls {
            if !component(&control.name)
                || !header(&control.content_type)
                || !valid_sha256(&control.artifact.sha256)
                || !names.insert(&control.name)
            {
                return Err(invalid());
            }
            let size = usize::try_from(control.size).map_err(|_| capacity())?;
            inline_bytes = inline_bytes
                .checked_add(size.div_ceil(3).checked_mul(4).ok_or_else(capacity)?)
                .ok_or_else(capacity)?;
        }
        if !self
            .controls
            .iter()
            .any(|control| control.name == "manifest.json")
            || names
                .iter()
                .any(|name| *pointer_key == format!("{release_prefix}/{name}"))
        {
            return Err(invalid());
        }
        if inline_bytes
            > budget
                .limits
                .max_metadata_bytes
                .min(budget.limits.max_batch_bytes / 2)
        {
            return Err(capacity());
        }
        Ok(())
    }

    fn check_outputs(&self, manifest: &Manifest) -> Result<()> {
        if !known_job_profile(&manifest.profile)
            || manifest.dataset != self.dataset
            || manifest.operation != self.operation
            || manifest.request_digest != self.job_request_digest
            || manifest.execution != "completed"
            || manifest.outputs.len() != self.outputs.len()
        {
            return Err(invalid());
        }
        let expected: BTreeMap<_, _> = manifest
            .outputs
            .iter()
            .map(|output| (&output.name, output))
            .collect();
        if expected.len() != self.outputs.len() {
            return Err(invalid());
        }
        for output in &self.outputs {
            let exact = expected.get(&output.name).ok_or_else(invalid)?;
            if output.object != exact.completion
                || output.object.owner != self.dataset
                || output.object.path != exact.path
                || output.object.bytes != exact.size
                || output.rows != exact.rows
                || output.sha256 != exact.sha256
                || output.md5 != exact.md5
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

fn native_record(view: &View, key: String, revision: i64) -> JobInputRecordRef {
    JobInputRecordRef {
        table: view.identifier().to_string(),
        table_uuid: view.table.metadata().uuid().to_string(),
        key,
        revision,
    }
}

pub(crate) fn validate_audit(
    audit: &PublicationIntentAudit,
    view: &View,
    key: &str,
    revision: i64,
    ceiling: i64,
) -> Result<()> {
    let reference = &audit.reference;
    let operation = &reference.operation;
    if key != format!("{INTENT_PREFIX}{operation}")
        || !valid_sha256(&reference.intent_digest)
        || !valid_sha256(&audit.job_request_digest)
        || !valid_sha256(&audit.payload_digest)
        || &crate::catalog::lifecycle_ident(&reference.dataset) != view.identifier()
        || audit.candidate.dataset != reference.dataset
        || audit.candidate.operation != *operation
        || audit.candidate.manifest.owner != reference.dataset
        || audit.candidate.scheme != "urn:docspec:whole-job-candidate"
        || audit.candidate.scheme_version != "1"
        || revision > ceiling
        || reference.native_record
            != native_record(view, format!("{PAYLOAD_PREFIX}{operation}"), revision)
        || audit.selection
            != native_record(
                view,
                format!("job-selected/{operation}"),
                audit.selection.revision,
            )
        || audit.acceptance
            != native_record(
                view,
                format!("job-accepted/{operation}/{}", audit.policy_id),
                audit.acceptance.revision,
            )
        || audit.completion
            != native_record(
                view,
                format!("job-result/{operation}"),
                audit.completion.revision,
            )
        || !(0 < audit.completion.revision
            && audit.completion.revision < audit.acceptance.revision
            && audit.acceptance.revision < audit.selection.revision
            && audit.selection.revision < revision)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(in crate::engine) fn retained_intent(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    budget: &Budget,
) -> Result<Option<Held<PublicationIntentReceipt>>> {
    let key = format!("{INTENT_PREFIX}{operation}");
    let mut memory = rows.reserve_decoded(budget, &[&key])?;
    let Some((revision, _, audit)) = rows.get_record::<PublicationIntentAudit>(&key)? else {
        if rows.contains_key(&format!("{PAYLOAD_PREFIX}{operation}")) {
            return Err(invalid());
        }
        return Ok(None);
    };
    validate_audit(&audit, view, &key, revision, view.revision)?;
    let payload_key = &audit.reference.native_record.key;
    let payload_revision = rows.revision(payload_key);
    if payload_revision.is_some_and(|actual| actual != revision) {
        return Err(invalid());
    }
    let payload = if record_retired(rows, view, payload_key, revision, budget)? {
        None
    } else {
        memory.merge(rows.reserve_decoded(budget, &[payload_key])?);
        let (_, _, payload): (_, _, PublicationIntentPayload) =
            rows.get_record(payload_key)?.ok_or_else(invalid)?;
        if json_identity(&payload)? != audit.payload_digest
            || json_identity(&payload.outputs)? != audit.inventory_digest
        {
            return Err(invalid());
        }
        Some(payload)
    };
    Ok(Some(Held {
        value: PublicationIntentReceipt::new(
            audit.reference.dataset.clone(),
            operation.clone(),
            Some(audit),
            payload,
        ),
        _memory: memory,
    }))
}

pub(in crate::engine) fn retained_intent_record(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    budget: &Budget,
) -> Result<Option<Held<PublicationIntentReceipt>>> {
    if let Some(operation) = key.strip_prefix(INTENT_PREFIX) {
        return retained_intent(rows, view, &Id::new(operation)?, budget)?
            .map(Some)
            .ok_or_else(invalid);
    }
    let operation = key.strip_prefix(PAYLOAD_PREFIX).ok_or_else(invalid)?;
    if !rows.contains_key(&format!("{INTENT_PREFIX}{operation}")) {
        return Err(invalid());
    }
    Ok(None)
}

fn replay(
    retained: Held<PublicationIntentReceipt>,
    intent_digest: &str,
) -> Result<PublicationIntentReceipt> {
    if retained
        .intent
        .as_ref()
        .is_none_or(|audit| audit.reference.intent_digest != intent_digest)
    {
        return Err(invalid());
    }
    Ok(retained.into_receipt())
}

impl Engine {
    /// Freezes or inspects one local fixture publication intent.
    ///
    /// Freeze resolves the current native acceptance and protects its exact
    /// delivery objects. Inspection reports the saved audit even after retirement.
    ///
    /// # Errors
    /// Refuses unsupported scopes, stale selection, changed requests, inconsistent
    /// inventory or retired result availability, and metadata capacity exhaustion.
    pub async fn job_publication_intent_bytes(
        &self,
        bytes: Vec<u8>,
    ) -> Result<PublicationIntentReceipt> {
        self.require_local()?;
        let budget = &self.inner.lifecycle.budget;
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let memory = budget.reserve(bytes.len().saturating_mul(32).saturating_add(4096))?;
        let command: Command = serde_json::from_slice(&bytes)?;
        drop(bytes);
        let disk = match &command {
            Command::Freeze(_) => worker::DiskAdmission::Shared,
            Command::Inspect { .. } => worker::DiskAdmission::Metadata,
        };
        self.admit_disk_work(memory, disk, move |engine| async move {
            match command {
                Command::Freeze(request) => engine.freeze_publication(*request).await,
                Command::Inspect { dataset, operation } => {
                    engine.inspect_publication(dataset, operation).await
                }
            }
        })
        .await
    }

    async fn inspect_publication(
        &self,
        dataset: Id,
        operation: Id,
    ) -> Result<PublicationIntentReceipt> {
        let view = match self.existing_lifecycle_view(&dataset).await {
            Ok(view) => view,
            Err(error) if error.code() == "no_release" => {
                return Ok(PublicationIntentReceipt::new(
                    dataset, operation, None, None,
                ));
            }
            Err(error) => return Err(error),
        };
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(&view).await?;
        Ok(
            match retained_intent(&rows, &view, &operation, &lifecycle.budget)? {
                Some(saved) => saved.into_receipt(),
                None => PublicationIntentReceipt::new(dataset, operation, None, None),
            },
        )
    }

    async fn freeze_publication(&self, request: Freeze) -> Result<PublicationIntentReceipt> {
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let digest = json_identity(&request)?;
        let lock = self.lock(&request.dataset).await;
        let _guard = lock.lock().await;
        let view = self
            .existing_lifecycle_view(&request.dataset)
            .await
            .map_err(|error| {
                if error.code() == "no_release" {
                    stale()
                } else {
                    error
                }
            })?;
        let rows = lifecycle.rows(&view).await?;
        if let Some(saved) = retained_intent(&rows, &view, &request.operation, budget)? {
            return replay(saved, &digest);
        }

        let operation = &request.operation;
        let intent_key = format!("{INTENT_PREFIX}{operation}");
        let payload_key = format!("{PAYLOAD_PREFIX}{operation}");
        let selected_key = format!("job-selected/{operation}");
        let accepted_key = format!("job-accepted/{operation}/{}", request.policy_id);
        let result_key = format!("job-result/{operation}");
        let decoded = rows.reserve_decoded(
            budget,
            &["authority", &selected_key, &accepted_key, &result_key],
        )?;
        let (_, _, authority): (_, _, Authority) =
            rows.get_record("authority")?.ok_or_else(stale)?;
        if authority.epoch != request.expected_epoch
            || !authority
                .job_profile
                .as_deref()
                .is_some_and(known_job_profile)
            || authority.job_selected.as_ref() != Some(&request.candidate)
            || authority.selection_revision != Some(request.expected_selection_revision)
            || request.candidate.dataset != request.dataset
            || request.candidate.operation != *operation
            || request.candidate.scheme != "urn:docspec:whole-job-candidate"
            || request.candidate.scheme_version != "1"
            || authority.selected.is_some()
            || authority.release.is_some()
        {
            return Err(stale());
        }
        let (selected_revision, _, selected): (_, _, qualification::Selected) =
            rows.get_record(&selected_key)?.ok_or_else(stale)?;
        if selected_revision != request.expected_selection_revision
            || selected.request_digest != request.job_request_digest
            || selected.candidate != request.candidate
            || selected.policy_id != request.policy_id
            || selected.accepted_qualification != request.accepted_qualification
        {
            return Err(stale());
        }
        let (accepted_revision, _, accepted): (_, _, qualification::Accepted) =
            rows.get_record(&accepted_key)?.ok_or_else(stale)?;
        self.check_operation_available(&view, operation).await?;
        let (result_revision, _, completed): (_, _, execution::Completion) =
            rows.get_record(&result_key)?.ok_or_else(stale)?;
        if completed.candidate != request.candidate
            || completed.manifest.reference != request.candidate.manifest
        {
            return Err(invalid());
        }
        let manifest_bytes = witness_bytes(&completed.manifest, budget)?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes.value)?;
        qualification::validate_publication_acceptance(
            &accepted,
            &selected,
            &native_record(&view, accepted_key.clone(), accepted_revision),
            &manifest.view_set_digest,
        )?;
        request.validate(budget)?;
        request.check_outputs(&manifest)?;
        self.check_job_evidence_object(&rows, &view, &request.candidate.manifest)?;
        for output in &request.outputs {
            if matches!(output.disposition, PublicationDisposition::Published { .. }) {
                self.check_job_evidence_object(&rows, &view, &output.object)?;
            }
        }
        drop(manifest);
        drop(manifest_bytes);
        drop(accepted);
        drop(completed);
        drop(selected);
        drop(authority);
        drop(decoded);
        drop(rows);
        publication_plan_limits(view.table.metadata_location_result()?, "")?;

        let Freeze {
            dataset,
            operation,
            candidate,
            job_request_digest,
            policy_id,
            accepted_qualification,
            expected_epoch,
            scope,
            release_id,
            outputs,
            controls,
            expected_pointer,
            pointer,
            ..
        } = request;
        let (expected_pointer, predecessor_bytes_base64) = match expected_pointer {
            ExpectedPointer::Absent => (PublicationPointerCondition::Absent, None),
            ExpectedPointer::Present { etag, bytes_base64 } => {
                if !header(&etag) {
                    return Err(invalid());
                }
                let bytes = capture_pointer(&bytes_base64)?;
                (
                    PublicationPointerCondition::Present { etag, bytes },
                    Some(bytes_base64),
                )
            }
        };
        let pointer_pin = PublicationPointerPin {
            content_type: pointer.content_type,
            cache_control: pointer.cache_control,
            bytes: capture_pointer(&pointer.bytes_base64)?,
        };
        let mut pins = Vec::with_capacity(controls.len());
        let mut controls_base64 = Vec::with_capacity(controls.len());
        let mut capture_memory = budget.reserve(0)?;
        for control in controls {
            let (artifact, size) = (control.artifact, control.size);
            let file_budget = budget.clone();
            let (held, sha256) = budget
                .blocking_admitted(move || {
                    let bytes = read_artifact_sized(&artifact, Some(size), &file_budget)?;
                    Ok((bytes, artifact.sha256))
                })
                .await?;
            pins.push(PublicationControlPin {
                name: control.name,
                content_type: control.content_type,
                bytes: PublicationBytePin { sha256, size },
            });
            controls_base64.push(STANDARD.encode(&held.value));
            capture_memory.merge(held._memory);
        }
        let inventory_digest = json_identity(&outputs)?;
        let payload = PublicationIntentPayload {
            outputs,
            controls_base64,
            predecessor_bytes_base64,
            pointer_bytes_base64: pointer.bytes_base64,
        };
        let revision = view.revision.checked_add(1).ok_or_else(capacity)?;
        let audit = PublicationIntentAudit {
            reference: PublicationIntentRef {
                dataset: dataset.clone(),
                operation: operation.clone(),
                intent_digest: digest.clone(),
                native_record: native_record(&view, payload_key.clone(), revision),
            },
            candidate,
            job_request_digest,
            policy_id,
            accepted_qualification,
            epoch: expected_epoch,
            selection: native_record(&view, selected_key, selected_revision),
            acceptance: native_record(&view, accepted_key, accepted_revision),
            completion: native_record(&view, result_key, result_revision),
            scope,
            release_id,
            inventory_digest,
            controls: pins,
            expected_pointer,
            pointer: pointer_pin,
            payload_digest: json_identity(&payload)?,
        };
        validate_audit(&audit, &view, &intent_key, revision, revision)?;
        let mut receipt =
            PublicationIntentReceipt::new(dataset, operation, Some(audit), Some(payload));
        let response_memory = self.request_memory(&receipt)?;
        let changes = vec![
            record(intent_key, receipt.intent.as_ref().ok_or_else(invalid)?)?,
            record(payload_key, receipt.payload.as_ref().ok_or_else(invalid)?)?,
        ];
        let encoding_memory = self.request_memory(&changes)?;
        for (key, value) in &changes {
            let encoded = crate::budget::metadata_size(value, budget.limits.max_batch_bytes / 2)?;
            if history_row_bytes(key, "").saturating_add(encoded)
                > budget.limits.max_batch_bytes / 2
            {
                return Err(capacity());
            }
        }
        let committed = lifecycle.commit(&view, changes).await;
        drop(encoding_memory);
        drop(capture_memory);
        match committed {
            Ok(_) => {
                receipt._memory = Some(response_memory);
                Ok(receipt)
            }
            Err(error) => {
                let dataset = receipt.dataset.clone();
                let operation = receipt.operation.clone();
                drop(receipt);
                drop(response_memory);
                let after = self.existing_lifecycle_view(&dataset).await?;
                let current = lifecycle.rows(&after).await?;
                match retained_intent(&current, &after, &operation, budget)? {
                    Some(saved) => replay(saved, &digest),
                    None => Err(error),
                }
            }
        }
    }
}
