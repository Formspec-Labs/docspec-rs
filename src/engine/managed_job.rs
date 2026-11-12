//! Native retained phases and exact SQL-report qualification for whole jobs.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::job::{
    Job, JobArtifact, JobInput, JobResult, JobSpec, JobStep, OutputResult, StepResult, StepStorage,
};
use arrow::{
    array::Array,
    datatypes::{DataType, Schema},
    ipc::reader::StreamReader,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Cursor, Read, Write},
};

mod execution;
mod input_custody;
pub(super) mod producer;
pub mod publication;
mod qualification;
pub(in crate::engine) mod retention;
mod snapshots;
#[cfg(test)]
mod tests;

const PROFILE: &str = "urn:docspec:whole-job-completion:1";
const SNAPSHOT_PROFILE: &str = "urn:docspec:whole-job-completion:2";
const PRODUCER_PROFILE: &str = "urn:docspec:whole-job-completion:3";
const COUNTERS: &str = "urn:docspec:quality:native-sql-zero-counters:1";

fn completion_profile(request: &ManagedJobPrepare) -> &'static str {
    if request.producer_adoption.is_some() {
        PRODUCER_PROFILE
    } else if request.snapshot_inputs.is_empty() {
        PROFILE
    } else {
        SNAPSHOT_PROFILE
    }
}

fn known_job_profile(profile: &str) -> bool {
    matches!(profile, PROFILE | SNAPSHOT_PROFILE | PRODUCER_PROFILE)
}

/// An exact caller-declared local input member, verified during native admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFileMember {
    pub key: Option<String>,
    pub sha256: String,
    pub size: u64,
}

/// Complete named member inventory for one file-backed input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFileInput {
    pub name: String,
    pub members: Vec<JobFileMember>,
}

/// Exact original metadata bytes selecting one native snapshot to retain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSnapshotInput {
    pub name: String,
    pub metadata_sha256: String,
    pub metadata_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reuse_from: Option<JobInputCustodyRef>,
}

/// Native typed-admission identity, returned without requiring client canonicalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobInputAdmissionDigest {
    pub algorithm: String,
    pub value: String,
}

/// References sealed native custody; it grants no producer or population qualification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobInputCustodyRef {
    pub scheme: String,
    pub scheme_version: String,
    pub dataset: Id,
    pub operation: Id,
    pub request_digest: String,
    pub input: String,
    pub native_record: JobInputRecordRef,
    pub admission_digest: JobInputAdmissionDigest,
}

/// A required native proof completion scoped to the main job's exact inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobPrerequisite {
    pub profile: String,
    pub candidate: JobCandidateRef,
    pub accepted_qualification: String,
    pub inputs: Vec<String>,
}

/// The immutable computation request; later qualification authority is separate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedJobPrepare {
    pub dataset: Id,
    pub operation: Id,
    pub definition: String,
    pub job: JobArtifact,
    pub file_inputs: Vec<JobFileInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_inputs: Vec<JobSnapshotInput>,
    pub policy_id: Id,
    pub policy: JobArtifact,
    pub expected_epoch: u64,
    pub expected_selection: Option<JobCandidateRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_adoption: Option<JobProducerAdoptionRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub producer_records: Vec<JobProducerRecord>,
    #[serde(default)]
    pub prerequisites: Vec<JobPrerequisite>,
}

/// Commands use one original operation and never accept caller-computed reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedJobCommand {
    Prepare(ManagedJobPrepare),
    Inspect {
        dataset: Id,
        operation: Id,
    },
    Accept {
        dataset: Id,
        operation: Id,
        request_digest: String,
        candidate: JobCandidateRef,
        policy_id: Id,
    },
    Select {
        dataset: Id,
        operation: Id,
        request_digest: String,
        candidate: JobCandidateRef,
        policy_id: Id,
        accepted_qualification: String,
    },
    Requalify {
        dataset: Id,
        operation: Id,
        request_digest: String,
        candidate: JobCandidateRef,
        previous_policy_id: Id,
        policy_id: Id,
        policy: JobArtifact,
        expected_epoch: u64,
        expected_selection: Option<JobCandidateRef>,
    },
}
impl ManagedJobCommand {
    /// Returns the original native recovery identity for every action.
    pub fn identity(&self) -> (&Id, &Id) {
        match self {
            Self::Prepare(request) => (&request.dataset, &request.operation),
            Self::Inspect { dataset, operation }
            | Self::Accept {
                dataset, operation, ..
            }
            | Self::Select {
                dataset, operation, ..
            }
            | Self::Requalify {
                dataset, operation, ..
            } => (dataset, operation),
        }
    }
}

/// Original native artifact bytes, independently readable by metadata-only consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobArtifactWitness {
    pub reference: ObjectRef,
    pub sha256: String,
    pub bytes_base64: String,
}

/// Native retained result; computation and selection have independent status.
#[derive(Debug, Serialize)]
pub struct ManagedJobReceipt {
    pub dataset: Id,
    pub operation: Id,
    pub request_digest: String,
    pub status: String,
    pub candidate: Option<JobCandidateRef>,
    pub view_set_digest: Option<String>,
    pub policy_id: Id,
    pub active_policy_id: Id,
    pub accepted_qualification: Option<serde_json::Value>,
    pub selection: String,
    pub completed_outputs: Vec<serde_json::Value>,
    pub completed_reports: Vec<serde_json::Value>,
    pub completion_manifest: Option<JobArtifactWitness>,
    pub admitted_request: JobArtifactWitness,
    pub policy: JobArtifactWitness,
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub input_custody: Vec<JobInputCustodyRef>,
    #[serde(skip)]
    _memory: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    request: ManagedJobPrepare,
    request_digest: String,
    admitted_request: JobArtifactWitness,
    job: JobArtifactWitness,
    initial_policy: JobArtifactWitness,
    directory: PathBuf,
    protection: Id,
    base_definition: String,
    runtime: PhaseRuntime,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    input_grants: BTreeMap<String, execution::InputAdmission>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    input_donors: BTreeMap<String, execution::InputAdmission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot_source: Option<snapshots::SnapshotSourcePlacement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer: Option<producer::ProducerAdmission>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    policy_id: Id,
    policy: JobArtifactWitness,
    expected_epoch: u64,
    expected_selection: Option<JobCandidateRef>,
    action_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    request_digest: String,
    id: Id,
    status: String,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Phase {
    operation: Id,
    request_digest: String,
    name: String,
    material_identity: String,
    schema_sha256: String,
    schema_ipc_base64: String,
    object: Option<ObjectRef>,
    sha256: Option<String>,
    rows: u64,
    file: Option<crate::job::ExportedFile>,
    dependencies: Vec<String>,
    origin_runtime: PhaseRuntime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhaseRuntime {
    format: u32,
    processor: String,
    binary_sha256: String,
    package_version: String,
    dependency_lock_blake3: String,
}

pub(in crate::engine) struct Held<T> {
    value: T,
    _memory: tokio::sync::OwnedSemaphorePermit,
}
impl<T> std::ops::Deref for Held<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl Engine {
    async fn job_runtime(&self) -> Result<PhaseRuntime> {
        Ok(PhaseRuntime {
            format: super::FORMAT,
            processor: "docspec.whole-job.v1".into(),
            binary_sha256: self.job_runtime_sha256().await?,
            package_version: env!("CARGO_PKG_VERSION").into(),
            dependency_lock_blake3: blake3::hash(include_bytes!("../../Cargo.lock"))
                .to_hex()
                .to_string(),
        })
    }

    async fn check_job_runtime(&self, admitted: &Admission) -> Result<()> {
        if admitted.runtime != self.job_runtime().await? {
            return Err(Error::refusal(
                "unqualified_job_runtime",
                "this original computation requires its retained native runtime; cross-runtime compatibility has not been qualified",
            ));
        }
        Ok(())
    }
    async fn job_operation_lock(&self, dataset: &Id, operation: &Id) -> Arc<Mutex<()>> {
        let mut locks = self.inner.job_operations.lock().await;
        if locks.len() >= 256 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        let key = (dataset.clone(), operation.clone());
        if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }

    async fn run_job_attempt(&self, admitted: &Admission) -> Result<()> {
        self.check_job_recovery(admitted).await?;
        let request = &admitted.request;
        if self
            .job_record::<execution::Completion>(
                &request.dataset,
                &format!("job-result/{}", request.operation),
            )
            .await?
            .is_some()
        {
            return Ok(());
        }
        let key = format!("job-attempt/{}", request.operation);
        let mut attempt = Attempt {
            request_digest: admitted.request_digest.clone(),
            id: Id::new(uuid::Uuid::new_v4().simple().to_string())?,
            status: "started".into(),
            error: None,
        };
        self.record_job_attempt(&request.dataset, &key, &attempt)
            .await?;
        let result = self.resume_job(admitted).await;
        if let Err(error) = &result {
            attempt.status = "failed".into();
            let text = format!("{}: {}", error.code(), error.message());
            if text.len() <= self.inner.lifecycle.budget.limits.max_metadata_bytes / 4 {
                attempt.error = Some(text);
                self.record_job_attempt(&request.dataset, &key, &attempt)
                    .await?;
            }
        }
        result
    }

    pub(super) const JOB_LIFECYCLE_COMPACT_SNAPSHOTS: usize = 100;

    async fn record_job_attempt(&self, dataset: &Id, key: &str, attempt: &Attempt) -> Result<()> {
        let _memory = self.request_memory(attempt)?;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let changes = vec![record(key, attempt)?];
        self.commit_or_compact_job_changes(dataset, &view, changes)
            .await
    }
    async fn job_runtime_sha256(&self) -> Result<String> {
        if let Some(identity) = self.inner.job_runtime_sha256.get() {
            return Ok(identity.clone());
        }
        let memory = self.inner.lifecycle.budget.reserve(64 << 10)?;
        let identity = self
            .inner
            .lifecycle
            .budget
            .blocking_admitted(move || {
                let _memory = memory;
                let mut file = File::open(std::env::current_exe()?)?;
                let mut buffer = vec![0; 64 << 10];
                let mut digest = Sha256::new();
                loop {
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    digest.update(&buffer[..count]);
                }
                Ok(format!("{:x}", digest.finalize()))
            })
            .await?;
        let _ = self.inner.job_runtime_sha256.set(identity.clone());
        Ok(identity)
    }
    async fn job_phase_sealed(&self, _key: &str) -> Result<()> {
        #[cfg(test)]
        {
            if self
                .inner
                .job_admission_rows
                .lock()
                .await
                .as_ref()
                .is_some_and(|rows| rows.upgrade().is_some())
            {
                self.inner
                    .job_admission_read_alive_during_work
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            let mut stop = self.inner.job_stop_after.lock().await;
            if stop.as_deref() == Some(_key) {
                stop.take();
                return Err(Error::refusal(
                    "injected_failure",
                    "stopped at the native whole-job phase checkpoint",
                ));
            }
        }
        Ok(())
    }
    /// Runs one retained whole-job command in the host-owned execution service.
    ///
    /// # Errors
    /// Refuses mismatched retries, unsupported custody, incomplete reports or stale selection authority.
    pub async fn managed_job(&self, command: ManagedJobCommand) -> Result<ManagedJobReceipt> {
        let bytes = serde_json::to_vec(&command)?;
        self.managed_job_bytes(bytes).await
    }

    /// Captures exact submitted metadata before parsing or opening caller artifacts.
    ///
    /// # Errors
    /// Refuses metadata capacity, malformed commands and the same native failures as `managed_job`.
    pub async fn managed_job_bytes(&self, bytes: Vec<u8>) -> Result<ManagedJobReceipt> {
        let budget = &self.inner.lifecycle.budget;
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let memory = budget.reserve(bytes.len().saturating_mul(32).saturating_add(4096))?;
        let command: ManagedJobCommand = serde_json::from_slice(&bytes)?;
        let disk = match &command {
            ManagedJobCommand::Prepare(_) => worker::DiskAdmission::Exclusive,
            ManagedJobCommand::Inspect { .. } => worker::DiskAdmission::Metadata,
            _ => worker::DiskAdmission::Shared,
        };
        self.admit_disk_work(memory, disk, move |engine| async move {
            engine.require_local()?;
            let (dataset, operation) = command.identity();
            let lock = engine.job_operation_lock(dataset, operation).await;
            let _operation = if matches!(command, ManagedJobCommand::Inspect { .. }) {
                None
            } else {
                Some(lock.lock_owned().await)
            };
            match command {
                ManagedJobCommand::Prepare(request) => {
                    engine.prepare_job_retained(request, bytes).await
                }
                ManagedJobCommand::Inspect { dataset, operation } => {
                    engine.job_status(&dataset, &operation, None).await
                }
                ManagedJobCommand::Accept {
                    dataset,
                    operation,
                    request_digest,
                    candidate,
                    policy_id,
                } => {
                    engine
                        .accept_job(
                            &dataset,
                            &operation,
                            &request_digest,
                            &candidate,
                            &policy_id,
                        )
                        .await?;
                    engine
                        .job_status(&dataset, &operation, Some(&policy_id))
                        .await
                }
                ManagedJobCommand::Select {
                    dataset,
                    operation,
                    request_digest,
                    candidate,
                    policy_id,
                    accepted_qualification,
                } => {
                    engine
                        .select_job(
                            &dataset,
                            &operation,
                            &request_digest,
                            &candidate,
                            &policy_id,
                            &accepted_qualification,
                        )
                        .await?;
                    engine
                        .job_status(&dataset, &operation, Some(&policy_id))
                        .await
                }
                command @ ManagedJobCommand::Requalify { .. } => {
                    engine.requalify_job(command).await
                }
            }
        })
        .await
    }

    async fn job_record<T: DeserializeOwned>(
        &self,
        dataset: &Id,
        key: &str,
    ) -> Result<Option<Held<T>>> {
        let view = self.read_view(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[key])?;
        Ok(rows.get_record(key)?.map(|(_, _, value)| Held {
            value,
            _memory: memory,
        }))
    }

    async fn commit_job_record<T: Serialize>(
        &self,
        dataset: &Id,
        key: &str,
        value: &T,
    ) -> Result<()> {
        self.commit_job_records(dataset, &[(key, value)]).await
    }

    pub(super) async fn commit_job_records<T: Serialize>(
        &self,
        dataset: &Id,
        entries: &[(&str, &T)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let _value_memory = entries
            .iter()
            .map(|(_, value)| self.request_memory(*value))
            .collect::<Result<Vec<_>>>()?;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let pending = {
            let rows = self.inner.lifecycle.rows(&view).await?;
            let keys = entries.iter().map(|(key, _)| *key).collect::<Vec<_>>();
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &keys)?;
            let mut seen = BTreeSet::new();
            let mut pending = Vec::with_capacity(entries.len());
            for &(key, value) in entries {
                if !seen.insert(key) {
                    return Err(mismatch());
                }
                let encoded = record(key, value)?;
                if let Some((revision, _, old)) = rows.get_record::<serde_json::Value>(key)? {
                    if old == encoded.1 {
                        continue;
                    }
                    let reusable =
                        key.starts_with("job-material/") || key.starts_with("job-input-material/");
                    if !reusable
                        || !super::retention::retirement::record_retired(
                            &rows,
                            &view,
                            key,
                            revision,
                            &self.inner.lifecycle.budget,
                        )?
                    {
                        return Err(mismatch());
                    }
                }
                if let Some(operation) = key
                    .strip_prefix("job-phase/")
                    .or_else(|| key.strip_prefix("job-input/"))
                    .or_else(|| key.strip_prefix("job-member/"))
                    .or_else(|| key.strip_prefix("job-snapshot-member/"))
                    .or_else(|| key.strip_prefix("job-snapshot-plan/"))
                    .or_else(|| key.strip_prefix("job-producer-static/"))
                    .or_else(|| key.strip_prefix("job-result/"))
                {
                    let operation = operation.split('/').next().ok_or_else(mismatch)?;
                    if rows.contains_key(&format!("abandoned/{operation}")) {
                        return Err(Error::refusal(
                            "operation_abandoned",
                            "whole-job operation was abandoned before completion",
                        ));
                    }
                }
                pending.push(encoded);
            }
            pending
        };
        if pending.is_empty() {
            return Ok(());
        }
        self.commit_or_compact_job_changes(dataset, &view, pending)
            .await
    }

    async fn commit_or_compact_job_changes(
        &self,
        dataset: &Id,
        view: &View,
        pending: Vec<(String, serde_json::Value)>,
    ) -> Result<()> {
        let committed =
            if view.table.metadata().snapshots().count() >= Self::JOB_LIFECYCLE_COMPACT_SNAPSHOTS {
                self.inner
                    .lifecycle
                    .compact_baseline_with_records(view, &BTreeSet::new(), &pending)
                    .await
                    .map(|(next, _)| next)
            } else {
                self.inner.lifecycle.commit(view, pending.clone()).await
            };
        if let Err(error) = committed {
            let after = self.inner.lifecycle.open(dataset).await?;
            let rows = self.inner.lifecycle.rows(&after).await?;
            let keys = pending
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>();
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &keys)?;
            if pending.iter().all(|(key, expected)| {
                rows.get_record::<serde_json::Value>(key)
                    .ok()
                    .flatten()
                    .is_some_and(|(_, _, old)| old == *expected)
            }) {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    async fn reusable_job_record<T: DeserializeOwned>(
        &self,
        dataset: &Id,
        key: &str,
    ) -> Result<Option<Held<T>>> {
        let view = self.read_view(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[key])?;
        let Some((revision, _, value)) = rows.get_record(key)? else {
            return Ok(None);
        };
        if super::retention::retirement::record_retired(
            &rows,
            &view,
            key,
            revision,
            &self.inner.lifecycle.budget,
        )? {
            return Ok(None);
        }
        Ok(Some(Held {
            value,
            _memory: memory,
        }))
    }

    async fn retain_job_bytes(
        &self,
        owner: &Id,
        directory: &Path,
        name: &str,
        bytes: Vec<u8>,
        budget: &Budget,
    ) -> Result<JobArtifactWitness> {
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let sha256 = sha256(&bytes);
        let reference = ObjectRef {
            owner: owner.clone(),
            path: directory.join(name).to_string_lossy().into_owned(),
            digest: blake3::hash(&bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        };
        let bytes_base64 = STANDARD.encode(&bytes);
        self.inner
            .objects
            .create_candidate_metadata(&reference, bytes, budget, true)
            .await?;
        Ok(JobArtifactWitness {
            reference,
            sha256,
            bytes_base64,
        })
    }

    async fn prepare_job_retained(
        &self,
        request: ManagedJobPrepare,
        submitted: Vec<u8>,
    ) -> Result<ManagedJobReceipt> {
        let key = format!("job-operation/{}", request.operation);
        let digest = json_identity(&request)?;
        if let Some(saved) = self.job_record::<Admission>(&request.dataset, &key).await? {
            if saved.request_digest != digest {
                return Err(mismatch());
            }
            self.run_job_attempt(&saved).await?;
            return self
                .job_status(&request.dataset, &request.operation, None)
                .await;
        }
        if request.definition.is_empty() {
            return Err(mismatch());
        }
        if request.producer_adoption.is_none()
            && (!request.producer_records.is_empty() || !request.prerequisites.is_empty())
        {
            return Err(Error::refusal(
                "unsupported_job_prerequisite",
                "this native profile has not admitted the required producer interpretation proof",
            ));
        }
        let budget = &self.inner.lifecycle.budget;
        let job_bytes = read_artifact(&request.job, budget)?;
        let spec: JobSpec = serde_json::from_slice(&job_bytes.value)?;
        spec.validate()?;
        if spec
            .inputs
            .iter()
            .map(JobInput::name)
            .chain(spec.steps.iter().map(JobStep::name))
            .chain(spec.outputs.iter().map(|output| output.name.as_str()))
            .any(|name| name.to_ascii_lowercase().starts_with("__docspec_relation_"))
        {
            return Err(Error::refusal(
                "invalid_spec",
                "caller names cannot use the reserved native relation prefix",
            ));
        }
        if !spec.table_operations.is_empty() {
            return Err(Error::refusal(
                "unsupported_managed_job",
                "managed jobs refuse table operations",
            ));
        }
        #[cfg(feature = "substrait")]
        if spec
            .steps
            .iter()
            .any(|step| matches!(step, JobStep::Substrait(_)))
        {
            return Err(Error::refusal(
                "unsupported_managed_job",
                "retained Substrait artifact admission is outside this initial whole-job profile",
            ));
        }
        validate_file_inventory(&spec, &request.file_inputs, &request.snapshot_inputs)?;
        let policy_bytes = read_artifact(&request.policy, budget)?;
        let policy: qualification::CounterPolicy = serde_json::from_slice(&policy_bytes.value)?;
        policy.validate(&request.policy_id, &request.job.sha256, &spec)?;
        let directory = self
            .inner
            .root
            .join("objects")
            .join(request.dataset.as_str())
            .join(format!("job-{}", request.operation));
        let total_input = request
            .file_inputs
            .iter()
            .flat_map(|input| &input.members)
            .try_fold(0u64, |sum, member| {
                sum.checked_add(member.size).ok_or_else(capacity)
            })?;
        let required = total_input
            .checked_add(budget.limits.scratch_bytes)
            .and_then(|bytes| {
                bytes.checked_add((budget.limits.max_metadata_bytes as u64).saturating_mul(4))
            })
            .ok_or_else(capacity)?;
        if total_input > budget.limits.scratch_bytes
            || fs2::available_space(&self.inner.root)? < required
        {
            return Err(Error::refusal(
                "disk_capacity",
                "all admitted inputs, retained outputs, partial work and scratch must fit the operation allowance",
            ));
        }
        let protection = Id::new(format!("job-{}", request.operation))?;
        let runtime = self.job_runtime().await?;
        // This existing native protection is committed before creating the owned prefix.
        let lock = self.lock(&request.dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(&request.dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        #[cfg(test)]
        {
            *self.inner.job_admission_rows.lock().await = Some(Arc::downgrade(&rows));
        }
        let _memory = rows.reserve_decoded(budget, &["authority", &key])?;
        let mut authority: Authority = rows
            .get_record("authority")?
            .map_or_else(Authority::default, |(_, _, value)| value);
        if rows.contains_key(&key) {
            return Err(Error::refusal(
                "operation_conflict",
                "operation admission changed; reconcile its original identity",
            ));
        }
        check_authority(
            &authority,
            &authority.definition,
            request.expected_epoch,
            &request.expected_selection,
        )?;
        let producer = self
            .prepare_producer_admission(&request, &spec, &directory, &authority, &view, &rows)
            .await?;
        let (producer, _producer_memory) = match producer {
            Some(Held { value, _memory }) => (Some(value), Some(_memory)),
            None => (None, None),
        };
        authority.job_profile = Some(completion_profile(&request).into());
        let admitted_request =
            planned_witness(&request.dataset, &directory, "request.json", &submitted);
        let job = planned_witness(&request.dataset, &directory, "job.json", &job_bytes.value);
        let initial_policy = planned_witness(
            &request.dataset,
            &directory,
            "policy-initial.json",
            &policy_bytes.value,
        );
        // Eligibility and this receiving root use the same locked native view.
        let Held {
            value: (input_grants, input_donors),
            _memory: _input_reuse_memory,
        } = self
            .prepare_input_reuse(&request, &spec, &view, &rows)
            .await?;
        let snapshot_source = self.admit_snapshot_source(&spec, &input_grants)?;
        let _source_memory = snapshot_source
            .as_ref()
            .map(|placement| self.request_memory(placement))
            .transpose()?;
        let saved = Admission {
            request: request.clone(),
            request_digest: digest,
            admitted_request,
            job,
            initial_policy: initial_policy.clone(),
            directory,
            protection: protection.clone(),
            base_definition: authority.definition.clone(),
            runtime,
            input_grants,
            input_donors,
            snapshot_source,
            producer,
        };
        let decision = Decision {
            policy_id: request.policy_id.clone(),
            policy: initial_policy,
            expected_epoch: request.expected_epoch,
            expected_selection: request.expected_selection.clone(),
            action_digest: saved.request_digest.clone(),
        };
        let committed = self
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record("authority", authority)?,
                    record(
                        format!("protection/{protection}"),
                        Protection {
                            id: protection,
                            consumer: request.dataset.clone(),
                            purpose: format!("{}/job", request.operation),
                            paths: vec![format!("{}/", saved.directory.display())],
                            retired: false,
                        },
                    )?,
                    record(&key, &saved)?,
                    record(
                        format!("job-policy/{}/{}", request.operation, request.policy_id),
                        &decision,
                    )?,
                    record(
                        format!("job-active/{}", request.operation),
                        &request.policy_id,
                    )?,
                ],
            )
            .await;
        drop(_guard);
        // This authority read is obsolete after the guarded admission. Keeping
        // its snapshot across execution retains a separate cache reservation.
        drop(_memory);
        drop(rows);
        drop(view);
        if let Err(error) = committed {
            let retained = self.job_record::<Admission>(&request.dataset, &key).await?;
            if retained.is_none_or(|retained| retained.request_digest != saved.request_digest) {
                return Err(error);
            }
        }
        self.job_phase_sealed(&format!("job-grant/{}", request.operation))
            .await?;
        self.run_job_attempt(&saved).await?;
        self.job_status(&request.dataset, &request.operation, None)
            .await
    }
}

fn check_authority(
    authority: &Authority,
    definition: &str,
    epoch: u64,
    expected: &Option<JobCandidateRef>,
) -> Result<()> {
    if authority.selected.is_some()
        || authority.release.is_some()
        || authority
            .job_profile
            .as_deref()
            .is_some_and(|profile| !known_job_profile(profile))
        || (!authority.definition.is_empty() && authority.definition != definition)
        || authority.epoch != epoch
        || &authority.job_selected != expected
    {
        return Err(Error::refusal(
            "stale_authority",
            "whole-job definition, epoch or expected selection differs",
        ));
    }
    Ok(())
}

fn validate_file_inventory(
    spec: &JobSpec,
    supplied: &[JobFileInput],
    snapshots: &[JobSnapshotInput],
) -> Result<()> {
    let expected: BTreeMap<_, Vec<Option<String>>> = spec
        .inputs
        .iter()
        .filter_map(|input| match input {
            JobInput::Parquet { name, .. } => Some(Ok((name.as_str(), vec![None]))),
            JobInput::JsonLines { name, files, .. } => Some(Ok((
                name.as_str(),
                files.iter().map(|file| Some(file.key.clone())).collect(),
            ))),
            JobInput::NativeTable { .. } => None,
            _ => Some(Err(Error::refusal(
                "unsupported_job_custody",
                "this selector has no qualified retained input admission",
            ))),
        })
        .collect::<Result<_>>()?;
    if supplied.len() != expected.len() {
        return Err(mismatch());
    }
    let mut names = BTreeSet::new();
    for input in supplied {
        if !names.insert(input.name.as_str())
            || expected.get(input.name.as_str())
                != Some(
                    &input
                        .members
                        .iter()
                        .map(|member| member.key.clone())
                        .collect(),
                )
            || input
                .members
                .iter()
                .any(|member| !valid_sha256(&member.sha256))
        {
            return Err(mismatch());
        }
    }
    let expected: BTreeSet<_> = spec
        .inputs
        .iter()
        .filter_map(|input| matches!(input, JobInput::NativeTable { .. }).then_some(input.name()))
        .collect();
    if snapshots.len() != expected.len()
        || snapshots.iter().any(|input| {
            !expected.contains(input.name.as_str())
                || !names.insert(input.name.as_str())
                || !valid_sha256(&input.metadata_sha256)
        })
    {
        return Err(mismatch());
    }
    Ok(())
}

fn read_artifact(artifact: &JobArtifact, budget: &Budget) -> Result<Held<Vec<u8>>> {
    read_artifact_sized(artifact, None, budget)
}

fn read_artifact_sized(
    artifact: &JobArtifact,
    expected_size: Option<u64>,
    budget: &Budget,
) -> Result<Held<Vec<u8>>> {
    let file = File::open(&artifact.path)?;
    let size = file.metadata()?.len();
    if expected_size.is_some_and(|expected| size != expected) {
        return Err(mismatch());
    }
    if size > budget.limits.max_metadata_bytes as u64 {
        return Err(capacity());
    }
    let memory = budget.reserve((size as usize).saturating_mul(32))?;
    let mut bytes = Vec::with_capacity(size as usize);
    file.take(size.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size || sha256(&bytes) != artifact.sha256 {
        return Err(mismatch());
    }
    Ok(Held {
        value: bytes,
        _memory: memory,
    })
}

fn witness_bytes(witness: &JobArtifactWitness, budget: &Budget) -> Result<Held<Vec<u8>>> {
    let size = usize::try_from(witness.reference.bytes).map_err(|_| capacity())?;
    if size > budget.limits.max_metadata_bytes
        || witness.bytes_base64.len() > size.div_ceil(3).saturating_mul(4)
    {
        return Err(capacity());
    }
    let memory = budget.reserve(size.saturating_mul(32))?;
    let bytes = STANDARD
        .decode(&witness.bytes_base64)
        .map_err(|_| mismatch())?;
    if bytes.len() != size
        || sha256(&bytes) != witness.sha256
        || blake3::hash(&bytes).to_hex().as_str() != witness.reference.digest
    {
        return Err(mismatch());
    }
    Ok(Held {
        value: bytes,
        _memory: memory,
    })
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn planned_witness(owner: &Id, directory: &Path, name: &str, bytes: &[u8]) -> JobArtifactWitness {
    JobArtifactWitness {
        reference: ObjectRef {
            owner: owner.clone(),
            path: directory.join(name).to_string_lossy().into_owned(),
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        },
        sha256: sha256(bytes),
        bytes_base64: STANDARD.encode(bytes),
    }
}
fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn mismatch() -> Error {
    Error::refusal(
        "managed_job_identity",
        "retained whole-job request, artifact, policy or candidate differs",
    )
}
fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "whole-job metadata exceeds its admitted allowance",
    )
}

fn json_identity(value: &impl Serialize) -> Result<String> {
    let mut value = serde_json::to_value(value)?;
    value.sort_all_objects();
    Ok(storage::json_digest(&value)?.to_hex().to_string())
}

fn encode_metadata(value: &impl Serialize, budget: &Budget) -> Result<Held<Vec<u8>>> {
    let size = super::inputs::metadata_size(value, budget.limits.max_metadata_bytes)?;
    let memory = budget.reserve(size.saturating_mul(32))?;
    Ok(Held {
        value: serde_json::to_vec(value)?,
        _memory: memory,
    })
}
