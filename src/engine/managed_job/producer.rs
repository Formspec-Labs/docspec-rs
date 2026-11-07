//! Guarded adoption of exact producer-definition metadata by the native owner.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::catalog::SnapshotRows;

mod admission;
pub(super) use admission::ProducerAdmission;
mod evidence;
pub(super) use evidence::{ProducerEvidence, Role};
mod prerequisite;
pub(super) mod retention;
mod static_proof;

const SCHEME: &str = "urn:docspec:whole-job-producer-adoption";
const PROFILE: &str = "urn:docspec:whole-job-producer-definition:1";
const CURRENT: &str = "job-producer-definition";
const PREFIX: &str = "job-producer-adoption/";

/// Adopts one exact producer definition without changing the selected result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProducerAdoption {
    pub dataset: Id,
    pub adoption: Id,
    pub reason: String,
    pub expected_definition: String,
    pub expected_epoch: u64,
    pub expected_selection: Option<JobCandidateRef>,
    pub expected_adoption: Option<JobProducerAdoptionRef>,
    pub definition: JobArtifact,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BytePin {
    sha256: String,
    size: u64,
}

impl BytePin {
    fn valid(&self) -> bool {
        self.size > 0 && valid_sha256(&self.sha256)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Repository {
    identity: String,
    head: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Interpretation {
    model: BytePin,
    compatibility_registry: BytePin,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobPolicy {
    job_sha256: String,
    policy_id: Id,
    policy_sha256: String,
}

impl JobPolicy {
    fn valid(&self) -> bool {
        !self.policy_id.as_str().is_empty()
            && valid_sha256(&self.job_sha256)
            && valid_sha256(&self.policy_sha256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Original producer record address, independent of its local transport path.
pub struct JobProducerRecordLocation {
    pub namespace: String,
    pub key: String,
}

/// Exact original producer record bytes supplied to the proof operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProducerRecord {
    pub source: JobProducerRecordLocation,
    pub sha256: String,
    pub size: u64,
    pub path: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordPin {
    source: JobProducerRecordLocation,
    sha256: String,
    size: u64,
}

impl RecordPin {
    fn valid(&self) -> bool {
        !self.source.namespace.trim().is_empty()
            && !self.source.key.trim().is_empty()
            && self.size > 0
            && self.size <= 1 << 20
            && valid_sha256(&self.sha256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum NativeSelector {
    NativeTable {
        metadata_location: String,
        table_uuid: uuid::Uuid,
        snapshot: Option<i64>,
        schema_id: i32,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Anchor {
    anchor: String,
    record: RecordPin,
    blob: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    name: String,
    selector: NativeSelector,
    metadata: BytePin,
    record_namespace: String,
    head_record: RecordPin,
    import_anchors: Vec<Anchor>,
    daily_jobs: Vec<String>,
    import_jobs: Vec<String>,
    expected_versions: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFile {
    path: String,
    blob: String,
    bytes: BytePin,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Producer {
    input: String,
    commit: String,
    job: String,
    job_blob: String,
    source_files: Vec<SourceFile>,
    algorithm: String,
    versions: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    profile: String,
    definition: String,
    repository: Repository,
    interpretation: Interpretation,
    proof: JobPolicy,
    model: JobPolicy,
    model_file_inputs: Vec<JobFileInput>,
    inputs: Vec<Input>,
    producers: Vec<Producer>,
}

fn git_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn git_path(value: &str) -> bool {
    !value.contains(['\\', '\0'])
        && value
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."))
}

fn version_terms(versions: &BTreeMap<String, String>) -> bool {
    versions
        .iter()
        .all(|(term, value)| !term.trim().is_empty() && !value.trim().is_empty())
}

impl Input {
    fn valid(&self) -> bool {
        let NativeSelector::NativeTable {
            metadata_location,
            table_uuid,
            snapshot,
            schema_id,
        } = &self.selector;
        let mut anchors = BTreeSet::new();
        let mut locations = BTreeSet::new();
        let mut role_paths = BTreeSet::new();
        !self.name.trim().is_empty()
            && !metadata_location.trim().is_empty()
            && !table_uuid.is_nil()
            && snapshot.is_some_and(|snapshot| snapshot > 0)
            && *schema_id >= 0
            && self.metadata.valid()
            && !self.record_namespace.trim().is_empty()
            && self.head_record.valid()
            && !self.expected_versions.is_empty()
            && version_terms(&self.expected_versions)
            && self.import_anchors.iter().all(|anchor| {
                !anchor.anchor.trim().is_empty()
                    && anchor.record.valid()
                    && anchor.record.source.namespace != self.record_namespace
                    && git_path(&anchor.record.source.key)
                    && git_sha(&anchor.blob)
                    && anchors.insert(&anchor.anchor)
                    && locations
                        .insert((&anchor.record.source.namespace, &anchor.record.source.key))
            })
            && self
                .daily_jobs
                .iter()
                .chain(&self.import_jobs)
                .all(|path| git_path(path) && role_paths.insert(path))
            && (self.head_record.source.namespace == self.record_namespace
                || self.import_anchors.iter().any(|anchor| {
                    let record = &anchor.record;
                    record.source.namespace == self.head_record.source.namespace
                        && record.source.key == self.head_record.source.key
                        && record.sha256 == self.head_record.sha256
                        && record.size == self.head_record.size
                }))
    }
}

impl Producer {
    fn valid(&self) -> bool {
        let mut paths = BTreeSet::new();
        git_sha(&self.commit)
            && git_path(&self.job)
            && git_sha(&self.job_blob)
            && !self.algorithm.trim().is_empty()
            && !self.source_files.is_empty()
            && version_terms(&self.versions)
            && self.source_files.iter().all(|file| {
                git_path(&file.path)
                    && git_sha(&file.blob)
                    && valid_sha256(&file.bytes.sha256)
                    && paths.insert(&file.path)
            })
    }
}

impl Definition {
    fn validate(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        let mut producer_keys = BTreeSet::new();
        let mut covered = BTreeSet::new();
        if self.profile != PROFILE
            || self.definition.trim().is_empty()
            || self.repository.identity.trim().is_empty()
            || !git_sha(&self.repository.head)
            || !self.interpretation.model.valid()
            || !self.interpretation.compatibility_registry.valid()
            || !self.proof.valid()
            || !self.model.valid()
            || self.proof.job_sha256 == self.model.job_sha256
            || self.inputs.is_empty()
            || self
                .inputs
                .iter()
                .any(|input| !input.valid() || !names.insert(&input.name))
        {
            return Err(invalid_definition());
        }
        for producer in &self.producers {
            if !names.contains(&producer.input)
                || !producer.valid()
                || !producer_keys.insert((&producer.input, &producer.commit, &producer.job))
            {
                return Err(invalid_definition());
            }
            covered.insert(&producer.input);
        }
        if names != covered {
            return Err(invalid_definition());
        }
        for input in &self.model_file_inputs {
            let mut keys = BTreeSet::new();
            if input.name.trim().is_empty()
                || !names.insert(&input.name)
                || input.members.iter().any(|member| {
                    !valid_sha256(&member.sha256)
                        || !keys.insert(&member.key)
                        || match &member.key {
                            None => input.members.len() != 1,
                            Some(key) => key.is_empty(),
                        }
                })
            {
                return Err(invalid_definition());
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdoptionDecision {
    reference: JobProducerAdoptionRef,
    request: JobProducerAdoption,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentDefinition {
    reference: JobProducerAdoptionRef,
    size: u64,
    bytes_base64: String,
}

fn decode_definition(active: &CurrentDefinition, budget: &Budget) -> Result<Held<Definition>> {
    let size = usize::try_from(active.size).map_err(|_| capacity())?;
    if size > budget.limits.max_metadata_bytes
        || active.bytes_base64.len() != size.div_ceil(3).saturating_mul(4)
    {
        return Err(mismatch());
    }
    let memory = budget.reserve(size.saturating_mul(32))?;
    let bytes = STANDARD
        .decode(&active.bytes_base64)
        .map_err(|_| mismatch())?;
    if bytes.len() != size || sha256(&bytes) != active.reference.definition_sha256 {
        return Err(mismatch());
    }
    let value: Definition = serde_json::from_slice(&bytes)?;
    value.validate()?;
    Ok(Held {
        value,
        _memory: memory,
    })
}

impl Engine {
    pub(super) async fn seal_job_producer_evidence(
        &self,
        admitted: &Admission,
        spec: &JobSpec,
        budget: &Budget,
    ) -> Result<Option<Held<(ProducerEvidence, JobArtifactWitness)>>> {
        match &admitted.producer {
            Some(producer) => match &producer.binding {
                admission::Binding::Proof(proof) => self
                    .seal_static_proof(admitted, spec, proof, budget)
                    .await
                    .map(Some),
                admission::Binding::Model(grant) => self
                    .seal_model_grant(admitted, grant, budget)
                    .await
                    .map(Some),
            },
            None if admitted.request.producer_adoption.is_none() => Ok(None),
            None => Err(mismatch()),
        }
    }
}

pub(super) fn inherited_objects(admitted: &Admission) -> &[ObjectRef] {
    match admitted.producer.as_ref().map(|producer| &producer.binding) {
        Some(admission::Binding::Model(grant)) => &grant.objects,
        _ => &[],
    }
}

pub(super) fn check_input_schema(
    admitted: &Admission,
    input: &JobInput,
    digest: &str,
) -> Result<()> {
    if let Some(admission::Binding::Model(grant)) =
        admitted.producer.as_ref().map(|producer| &producer.binding)
        && matches!(input, JobInput::NativeTable { .. })
        && grant.input_schemas.get(input.name()).map(String::as_str) != Some(digest)
    {
        return Err(Error::refusal(
            "producer_prerequisite_mismatch",
            "model input Arrow schema differs from the accepted proof",
        ));
    }
    Ok(())
}

pub(super) fn check_decision_authority(
    authority: &Authority,
    admitted: &Admission,
    view: &View,
    rows: &SnapshotRows,
    budget: &Budget,
) -> Result<()> {
    if admitted.producer.is_none() {
        return require_unbound(authority);
    }
    if authority.job_producer_adoption != admitted.request.producer_adoption
        || authority.definition != admitted.request.definition
    {
        return Err(Error::refusal(
            "stale_authority",
            "producer adoption or definition changed before the decision",
        ));
    }
    check_current(rows, view, authority, budget)
}

pub(super) fn check_selection(admitted: &Admission) -> Result<()> {
    if admitted.producer.as_ref().map(ProducerAdmission::role) == Some(Role::Proof) {
        return Err(Error::refusal(
            "producer_proof_not_selectable",
            "accepted producer proof is a prerequisite, not a model selection",
        ));
    }
    Ok(())
}

pub(super) fn check_policy(
    admitted: &Admission,
    decision: &Decision,
    policy_id: &Id,
) -> Result<()> {
    if admitted.producer.is_some()
        && (policy_id != &admitted.request.policy_id
            || decision.policy_id != admitted.request.policy_id
            || decision.policy.sha256 != admitted.request.policy.sha256)
    {
        return Err(Error::refusal(
            "producer_policy_mismatch",
            "the decision must use the exact adopted role policy",
        ));
    }
    Ok(())
}

pub(super) fn check_candidate_evidence(
    admitted: &Admission,
    document: &serde_json::Value,
    budget: &Budget,
) -> Result<()> {
    let verified = evidence::completion_evidence(document, &admitted.request, budget)?;
    match (&admitted.producer, verified) {
        (None, None) => Ok(()),
        (Some(producer), Some(verified)) => match &producer.binding {
            admission::Binding::Proof(proof) => {
                let body: evidence::StaticProof = serde_json::from_slice(&verified.bytes)?;
                evidence::validate_static(
                    &body,
                    &verified.evidence,
                    &admitted.request.dataset,
                    &admitted.request.operation,
                    &admitted.request_digest,
                    budget,
                )?;
                if body.definition != proof.definition
                    || body.records.len() != proof.records.len()
                    || body
                        .records
                        .iter()
                        .zip(&proof.records)
                        .any(|(actual, expected)| {
                            actual.retained != expected.reference
                                || actual.sha256 != expected.sha256
                        })
                {
                    return Err(mismatch());
                }
                Ok(())
            }
            admission::Binding::Model(grant) => {
                let body: prerequisite::ModelGrant = serde_json::from_slice(&verified.bytes)?;
                if &body != grant.as_ref() {
                    return Err(mismatch());
                }
                body.validate(&admitted.request, &admitted.request_digest, budget)
            }
        },
        _ => Err(mismatch()),
    }
}

fn adoption_key(adoption: &Id) -> String {
    format!("{PREFIX}{adoption}")
}

fn invalid_definition() -> Error {
    Error::refusal(
        "invalid_producer_definition",
        "adopted producer definition has an invalid or incomplete identity",
    )
}

fn valid_reference(reference: &JobProducerAdoptionRef, table: &str) -> bool {
    reference.scheme == SCHEME
        && reference.scheme_version == "1"
        && valid_sha256(&reference.definition_sha256)
        && valid_sha256(&reference.request_digest)
        && reference.native_record.table == table
        && crate::catalog::lifecycle_ident(&reference.dataset).to_string() == table
        && reference.native_record.key == adoption_key(&reference.adoption)
        && reference.native_record.revision > 0
        && uuid::Uuid::parse_str(&reference.native_record.table_uuid).is_ok_and(|uuid| {
            !uuid.is_nil() && uuid.to_string() == reference.native_record.table_uuid
        })
}

fn validate_decision(decision: &AdoptionDecision, key: &str, table: &str) -> Result<()> {
    if !valid_reference(&decision.reference, table)
        || key != decision.reference.native_record.key
        || decision.reference.dataset != decision.request.dataset
        || decision.reference.adoption != decision.request.adoption
        || decision.reference.definition_sha256 != decision.request.definition.sha256
        || decision.reference.request_digest != json_identity(&decision.request)?
        || decision.request.reason.trim().is_empty()
    {
        return Err(mismatch());
    }
    Ok(())
}

fn replay(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    digest: &str,
) -> Result<Option<JobProducerAdoptionRef>> {
    let Some((revision, _, saved)) = rows.get_record::<AdoptionDecision>(key)? else {
        return Ok(None);
    };
    validate_decision(&saved, key, &view.identifier().to_string())?;
    if saved.reference.native_record.revision != revision
        || saved.reference.native_record.table_uuid != view.table.metadata().uuid().to_string()
        || saved.reference.request_digest != digest
    {
        return Err(mismatch());
    }
    Ok(Some(saved.reference))
}

fn check_current(
    rows: &SnapshotRows,
    view: &View,
    authority: &Authority,
    budget: &Budget,
) -> Result<()> {
    let Some(reference) = &authority.job_producer_adoption else {
        return if rows.contains_key(CURRENT) {
            Err(mismatch())
        } else {
            Ok(())
        };
    };
    let _memory = rows.reserve_decoded(budget, &[CURRENT, &reference.native_record.key])?;
    // Changing authority needs the active identity, not another decoded copy
    // of the previous definition. Static proof admission retains its own bytes.
    #[derive(Deserialize)]
    struct ActiveIdentity {
        reference: JobProducerAdoptionRef,
    }
    let (revision, _, current): (_, _, ActiveIdentity) =
        rows.get_record(CURRENT)?.ok_or_else(mismatch)?;
    if current.reference != *reference
        || revision != reference.native_record.revision
        || replay(
            rows,
            view,
            &reference.native_record.key,
            &reference.request_digest,
        )?
        .as_ref()
            != Some(reference)
    {
        return Err(mismatch());
    }
    Ok(())
}

pub(super) fn require_unbound(authority: &Authority) -> Result<()> {
    if authority.job_producer_adoption.is_some() {
        return Err(Error::refusal(
            "unsupported_job_prerequisite",
            "adopted producer definitions require native proof admission and prerequisites",
        ));
    }
    Ok(())
}

pub(super) fn retention_record(
    key: &str,
    json: &str,
    table: &str,
    budget: &Budget,
) -> Result<bool> {
    if key.starts_with(PREFIX) {
        validate_decision(&serde_json::from_str(json)?, key, table)?;
    } else if key == CURRENT {
        let active: CurrentDefinition = serde_json::from_str(json)?;
        if !valid_reference(&active.reference, table) {
            return Err(mismatch());
        }
        let _definition = decode_definition(&active, budget)?;
    } else {
        return Ok(false);
    }
    // These lifecycle values contain metadata only, never payload roots.
    Ok(true)
}

impl Engine {
    /// Adopts bounded producer-definition request bytes before decoding their fields.
    ///
    /// # Errors
    ///
    /// Refuses oversized or invalid metadata and the same authority or identity
    /// conflicts as `adopt_job_producer_definition`.
    pub async fn adopt_job_producer_definition_bytes(
        &self,
        bytes: Vec<u8>,
    ) -> Result<JobProducerAdoptionRef> {
        self.require_local()?;
        let budget = &self.inner.lifecycle.budget;
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let memory = budget.reserve(bytes.len().saturating_mul(32))?;
        self.admit_work(memory, move |engine| async move {
            let request = serde_json::from_slice(&bytes)?;
            engine.adopt_job_producer_definition_admitted(request).await
        })
        .await
    }

    /// Commits an exact producer interpretation under the local dataset authority.
    ///
    /// A repeated request returns its original decision without reopening the
    /// definition file or changing the current selection or adoption.
    ///
    /// # Errors
    ///
    /// Refuses stale authority, changed retries, invalid definitions, and any
    /// workspace that does not support the local authority action.
    pub async fn adopt_job_producer_definition(
        &self,
        request: &JobProducerAdoption,
    ) -> Result<JobProducerAdoptionRef> {
        self.require_local()?;
        let memory = self.request_memory(request)?;
        let request = request.clone();
        self.admit_work(memory, move |engine| async move {
            engine.adopt_job_producer_definition_admitted(request).await
        })
        .await
    }

    async fn adopt_job_producer_definition_admitted(
        &self,
        request: JobProducerAdoption,
    ) -> Result<JobProducerAdoptionRef> {
        if request.reason.trim().is_empty() || !valid_sha256(&request.definition.sha256) {
            return Err(invalid_definition());
        }
        let lifecycle = &self.inner.lifecycle;
        let key = adoption_key(&request.adoption);
        let digest = json_identity(&request)?;
        let lock = self.lock(&request.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&request.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let _memory = rows.reserve_decoded(&lifecycle.budget, &["authority", &key])?;
        if let Some(saved) = replay(&rows, &view, &key, &digest)? {
            return Ok(saved);
        }
        let mut authority = rows
            .get_record::<Authority>("authority")?
            .map_or_else(Authority::default, |(_, _, value)| value);
        check_authority(
            &authority,
            &request.expected_definition,
            request.expected_epoch,
            &request.expected_selection,
        )?;
        if authority.definition != request.expected_definition
            || authority.job_producer_adoption != request.expected_adoption
        {
            return Err(Error::refusal(
                "stale_authority",
                "current producer adoption or definition differs",
            ));
        }
        check_current(&rows, &view, &authority, &lifecycle.budget)?;
        let bytes = {
            let budget = lifecycle.budget.clone();
            let artifact = request.definition.clone();
            lifecycle
                .budget
                .blocking_admitted(move || read_artifact(&artifact, &budget))
                .await?
        };
        let definition_name = {
            let definition: Definition = serde_json::from_slice(&bytes.value)?;
            definition.validate()?;
            definition.definition
        };
        let revision = view.revision.checked_add(1).ok_or_else(capacity)?;
        let reference = JobProducerAdoptionRef {
            scheme: SCHEME.into(),
            scheme_version: "1".into(),
            dataset: request.dataset.clone(),
            adoption: request.adoption.clone(),
            request_digest: digest.clone(),
            definition_sha256: request.definition.sha256.clone(),
            native_record: JobInputRecordRef {
                table: view.identifier().to_string(),
                table_uuid: view.table.metadata().uuid().to_string(),
                key: key.clone(),
                revision,
            },
        };
        authority.definition = definition_name;
        authority.job_producer_adoption = Some(reference.clone());
        let active = CurrentDefinition {
            reference: reference.clone(),
            size: bytes.value.len() as u64,
            bytes_base64: STANDARD.encode(&bytes.value),
        };
        let decision = AdoptionDecision {
            reference: reference.clone(),
            request,
        };
        let _records_memory = self.request_memory(&(&authority, &active, &decision))?;
        // The published representations now own their independent admission.
        // Raw validation bytes and their allowance are no longer needed.
        drop(bytes);
        let changes = vec![
            record("authority", authority)?,
            record(CURRENT, active)?,
            record(&key, &decision)?,
        ];
        match lifecycle.commit(&view, changes).await {
            Ok(committed) if committed.revision == revision => {
                self.job_phase_sealed(&key).await?;
                Ok(reference)
            }
            Ok(_) => Err(mismatch()),
            Err(error) => {
                drop(_memory);
                drop(rows);
                let fresh = lifecycle.open(&decision.request.dataset).await?;
                let rows = lifecycle.rows(&fresh).await?;
                let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
                replay(&rows, &fresh, &key, &digest)?.ok_or(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adopted_model_file_inventory_is_complete_and_preserves_member_identity() {
        let mut value = super::super::tests::producer_definition("authored-interpretation");
        value["model_file_inputs"] = json!([
            {"name":"empty-parquet","members":[{"key":null,
                "sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0}]},
            {"name":"empty-jsonl","members":[]},
            {"name":"ordered-jsonl","members":[
                {"key":"second","sha256":"7".repeat(64),"size":19},
                {"key":"first","sha256":"8".repeat(64),"size":23}
            ]}
        ]);
        let valid: Definition = serde_json::from_value(value.clone()).unwrap();
        valid.validate().unwrap();
        assert_eq!(valid.model_file_inputs[0].members[0].size, 0);
        assert!(valid.model_file_inputs[1].members.is_empty());
        assert_eq!(
            valid.model_file_inputs[2]
                .members
                .iter()
                .map(|member| member.key.as_deref())
                .collect::<Vec<_>>(),
            [Some("second"), Some("first")]
        );
        let mut empty_source = value.clone();
        empty_source["producers"][0]["source_files"][0]["bytes"] = json!({
            "sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "size":0
        });
        serde_json::from_value::<Definition>(empty_source.clone())
            .unwrap()
            .validate()
            .unwrap();
        empty_source["interpretation"]["model"] =
            empty_source["producers"][0]["source_files"][0]["bytes"].clone();
        assert_eq!(
            serde_json::from_value::<Definition>(empty_source)
                .unwrap()
                .validate()
                .unwrap_err()
                .code(),
            "invalid_producer_definition"
        );
        for defect in [
            "duplicate-name",
            "native-name",
            "empty-name",
            "duplicate-key",
            "mixed-null",
            "invalid-digest",
        ] {
            let mut bad = value.clone();
            match defect {
                "duplicate-name" => bad["model_file_inputs"][1]["name"] = json!("empty-parquet"),
                "native-name" => bad["model_file_inputs"][1]["name"] = json!("raw"),
                "empty-name" => bad["model_file_inputs"][1]["name"] = json!(""),
                "duplicate-key" => {
                    bad["model_file_inputs"][2]["members"][1]["key"] = json!("second")
                }
                "mixed-null" => bad["model_file_inputs"][2]["members"][0]["key"] = json!(null),
                "invalid-digest" => {
                    bad["model_file_inputs"][0]["members"][0]["sha256"] = json!("unknown")
                }
                _ => unreachable!(),
            }
            assert_eq!(
                serde_json::from_value::<Definition>(bad)
                    .unwrap()
                    .validate()
                    .unwrap_err()
                    .code(),
                "invalid_producer_definition",
                "{defect}"
            );
        }
        value.as_object_mut().unwrap().remove("model_file_inputs");
        assert!(serde_json::from_value::<Definition>(value).is_err());
    }
}
