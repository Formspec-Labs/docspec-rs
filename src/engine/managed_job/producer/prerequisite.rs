//! Resolves an exact accepted proof before granting a dependent model admission.
// Rust guideline compliant 2026-02-21
use super::*;
use evidence::{EvidenceRef, Role, StaticProof};

pub(super) const GRANT_PROFILE: &str = "urn:docspec:whole-job-producer-prerequisite-grant:1";
const PREREQUISITE: &str = "urn:docspec:whole-job-producer-prerequisite:1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeAcceptance {
    native_record: JobInputRecordRef,
    sha256: String,
    size: u64,
    bytes_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ModelGrant {
    pub(super) profile: String,
    pub(super) dataset: Id,
    pub(super) operation: Id,
    pub(super) request_digest: String,
    pub(super) adoption: JobProducerAdoptionRef,
    pub(super) prerequisite: JobPrerequisite,
    proof_request_digest: String,
    proof_static: EvidenceRef,
    accepted: NativeAcceptance,
    pub(super) input_schemas: BTreeMap<String, String>,
    pub(super) objects: Vec<ObjectRef>,
}

fn prerequisite_mismatch() -> Error {
    Error::refusal(
        "producer_prerequisite_mismatch",
        "required native proof, acceptance or input scope differs",
    )
}

impl ModelGrant {
    pub(super) fn validate(
        &self,
        request: &ManagedJobPrepare,
        digest: &str,
        budget: &Budget,
    ) -> Result<()> {
        if self.profile != GRANT_PROFILE
            || self.dataset != request.dataset
            || self.operation != request.operation
            || self.request_digest != digest
            || request.producer_adoption.as_ref() != Some(&self.adoption)
            || !matches!(request.prerequisites.as_slice(), [required] if required == &self.prerequisite)
            || self.prerequisite.profile != PREREQUISITE
            || self.prerequisite.candidate.dataset != self.dataset
            || self.prerequisite.candidate.operation == self.operation
            || self.accepted.native_record.table != self.adoption.native_record.table
            || self.accepted.native_record.table_uuid != self.adoption.native_record.table_uuid
            || self.input_schemas.len() != request.snapshot_inputs.len()
            || request.snapshot_inputs.iter().any(|input| {
                self.input_schemas
                    .get(&input.name)
                    .is_none_or(|digest| !valid_sha256(digest))
            })
            || !valid_sha256(&self.proof_static.sha256)
            || !self.objects.contains(&self.proof_static.reference)
            || !self.objects.contains(&self.prerequisite.candidate.manifest)
            || self
                .objects
                .iter()
                .any(|object| object.owner != self.dataset)
            || self
                .objects
                .windows(2)
                .any(|pair| pair[0].path >= pair[1].path)
        {
            return Err(prerequisite_mismatch());
        }
        let size = usize::try_from(self.accepted.size).map_err(|_| capacity())?;
        if size > budget.limits.max_metadata_bytes
            || self.accepted.bytes_base64.len() != size.div_ceil(3).saturating_mul(4)
        {
            return Err(capacity());
        }
        let _memory = budget.reserve(size.saturating_mul(32))?;
        let bytes = STANDARD
            .decode(&self.accepted.bytes_base64)
            .map_err(|_| prerequisite_mismatch())?;
        if bytes.len() != size || sha256(&bytes) != self.accepted.sha256 {
            return Err(prerequisite_mismatch());
        }
        let accepted: qualification::Accepted = serde_json::from_slice(&bytes)?;
        qualification::validate_grant_acceptance(
            &accepted,
            &self.prerequisite,
            &self.proof_request_digest,
            &self.accepted.native_record,
        )
        .map_err(|_| prerequisite_mismatch())
    }
}

impl Engine {
    pub(super) async fn resolve_model_grant(
        &self,
        request: &ManagedJobPrepare,
        definition: &Definition,
        adoption: &JobProducerAdoptionRef,
        view: &View,
        rows: &SnapshotRows,
    ) -> Result<Held<ModelGrant>> {
        let [prerequisite] = request.prerequisites.as_slice() else {
            return Err(prerequisite_mismatch());
        };
        if !request.producer_records.is_empty()
            || prerequisite.profile != PREREQUISITE
            || prerequisite.candidate.dataset != request.dataset
            || prerequisite.candidate.operation == request.operation
            || !valid_sha256(&prerequisite.accepted_qualification)
            || !prerequisite
                .inputs
                .iter()
                .map(String::as_str)
                .eq(definition.inputs.iter().map(|input| input.name.as_str()))
        {
            return Err(prerequisite_mismatch());
        }
        let budget = &self.inner.lifecycle.budget;
        let operation = &prerequisite.candidate.operation;
        let operation_key = format!("job-operation/{operation}");
        let result_key = format!("job-result/{operation}");
        let accepted_key = format!("job-accepted/{operation}/{}", definition.proof.policy_id);
        let _records_memory =
            rows.reserve_decoded(budget, &[&operation_key, &result_key, &accepted_key])?;
        let (operation_revision, _, proof): (_, _, Admission) = rows
            .get_record(&operation_key)?
            .ok_or_else(prerequisite_mismatch)?;
        let (_, _, completed): (_, _, execution::Completion) = rows
            .get_record(&result_key)?
            .ok_or_else(prerequisite_mismatch)?;
        if proof.request.dataset != request.dataset
            || proof.request.operation != *operation
            || proof.request_digest != json_identity(&proof.request)?
            || proof.request.producer_adoption.as_ref() != Some(adoption)
            || proof.request.job.sha256 != definition.proof.job_sha256
            || proof.request.policy_id != definition.proof.policy_id
            || proof.request.policy.sha256 != definition.proof.policy_sha256
            || completed.candidate != prerequisite.candidate
            || completed.candidate.manifest != completed.manifest.reference
        {
            return Err(prerequisite_mismatch());
        }
        if rows.contains_key(&format!("abandoned/{operation}")) {
            return Err(Error::refusal(
                "operation_abandoned",
                "producer proof was abandoned",
            ));
        }
        if super::super::super::retention::retirement::record_retired(
            rows,
            view,
            &operation_key,
            operation_revision,
            budget,
        )? {
            return Err(Error::refusal(
                "recovery_retired",
                "producer proof recovery was retired",
            ));
        }
        self.check_operation_available(view, operation).await?;
        let (revision, _, accepted): (_, _, qualification::Accepted) =
            rows.get_record(&accepted_key)?.ok_or_else(|| {
                Error::refusal(
                    "producer_prerequisite_unaccepted",
                    "the exact producer proof has no native accepted qualification",
                )
            })?;
        let generation = JobInputRecordRef {
            table: view.identifier().to_string(),
            table_uuid: view.table.metadata().uuid().to_string(),
            key: accepted_key.clone(),
            revision,
        };
        let manifest_bytes = witness_bytes(&completed.manifest, budget)?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes.value)?;
        if manifest["dataset"] != request.dataset.as_str()
            || manifest["operation"] != operation.as_str()
            || manifest["request_digest"] != proof.request_digest
            || manifest["execution"] != "completed"
            || manifest["job"]["sha256"] != definition.proof.job_sha256
            || manifest["outputs"]
                .as_array()
                .is_none_or(|outputs| !outputs.is_empty())
        {
            return Err(prerequisite_mismatch());
        }
        let view_digest = manifest["view_set_digest"]
            .as_str()
            .ok_or_else(prerequisite_mismatch)?;
        let policy = qualification::prerequisite_policy(
            &accepted,
            &proof,
            prerequisite,
            view_digest,
            &generation,
        )
        .map_err(|_| prerequisite_mismatch())?;
        let proof_evidence = evidence::completion_evidence(&manifest, &proof.request, budget)?
            .ok_or_else(prerequisite_mismatch)?;
        if proof_evidence.evidence.role != Role::Proof {
            return Err(prerequisite_mismatch());
        }
        let proof_static: StaticProof = serde_json::from_slice(&proof_evidence.bytes)?;
        evidence::validate_static(
            &proof_static,
            &proof_evidence.evidence,
            &request.dataset,
            operation,
            &proof.request_digest,
            budget,
        )?;
        let inputs = manifest["inputs"]
            .as_array()
            .ok_or_else(prerequisite_mismatch)?;
        if inputs.len() != definition.inputs.len() {
            return Err(prerequisite_mismatch());
        }
        let mut memory = budget.reserve(0)?;
        let mut schemas = BTreeMap::new();
        for input in inputs {
            let name = input["name"].as_str().ok_or_else(prerequisite_mismatch)?;
            let digest = input["schema_sha256"]
                .as_str()
                .filter(|digest| valid_sha256(digest))
                .ok_or_else(prerequisite_mismatch)?;
            let binding = request
                .snapshot_inputs
                .iter()
                .find(|binding| binding.name == name)
                .ok_or_else(prerequisite_mismatch)?;
            let reference: JobInputCustodyRef =
                serde_json::from_value(input["custody_reference"].clone())?;
            if binding.reuse_from.as_ref() != Some(&reference)
                || reference.input != name
                || reference.dataset != request.dataset
                || reference.operation != *operation
                || reference.request_digest != proof.request_digest
                || reference.native_record.table != generation.table
                || reference.native_record.table_uuid != generation.table_uuid
                || input["kind"] != "native_table"
                || manifest["schemas"]
                    .get(digest)
                    .and_then(serde_json::Value::as_str)
                    .is_none()
            {
                return Err(prerequisite_mismatch());
            }
            memory.merge(self.request_memory(&(name, digest))?);
            if schemas.insert(name.to_owned(), digest.to_owned()).is_some() {
                return Err(prerequisite_mismatch());
            }
        }
        let mut objects = BTreeMap::<String, ObjectRef>::new();
        let mut add = |object: &ObjectRef, origin: Option<&Id>| -> Result<()> {
            if object.owner != request.dataset {
                return Err(prerequisite_mismatch());
            }
            if let Some(origin) = origin {
                self.check_custody_object(rows, view, object, origin)?;
            } else {
                self.check_job_evidence_object(rows, view, object)?;
            }
            if let Some(saved) = objects.get(&object.path) {
                return if saved == object {
                    Ok(())
                } else {
                    Err(prerequisite_mismatch())
                };
            }
            memory.merge(self.request_memory(&(object, &object.path))?);
            objects.insert(object.path.clone(), object.clone());
            Ok(())
        };
        let proof_view: ObjectRef =
            serde_json::from_value(manifest["view_set"]["reference"].clone())?;
        for object in [
            &completed.manifest.reference,
            &proof.admitted_request.reference,
            &proof.job.reference,
            policy,
            &proof_view,
            &proof_evidence.evidence.evidence.reference,
        ] {
            add(object, Some(operation))?;
        }
        for report in manifest["reports"]
            .as_array()
            .ok_or_else(prerequisite_mismatch)?
        {
            // A report may use an exact retained phase from an earlier job.
            add(&serde_json::from_value(report["completion"].clone())?, None)?;
        }
        for record in &proof_static.records {
            add(&record.retained, Some(operation))?;
        }
        for metadata in &proof_static.metadata {
            add(
                &metadata.object.retained,
                Some(&metadata.object.origin_operation),
            )?;
        }
        let (_, _, raw_accepted) = rows
            .prefix(&accepted_key)
            .find(|(key, _, _)| *key == accepted_key)
            .ok_or_else(prerequisite_mismatch)?;
        memory.merge(budget.reserve(raw_accepted.len().saturating_mul(8))?);
        let grant = ModelGrant {
            profile: GRANT_PROFILE.into(),
            dataset: request.dataset.clone(),
            operation: request.operation.clone(),
            request_digest: json_identity(request)?,
            adoption: adoption.clone(),
            prerequisite: prerequisite.clone(),
            proof_request_digest: proof.request_digest.clone(),
            proof_static: proof_evidence.evidence.evidence.clone(),
            accepted: NativeAcceptance {
                native_record: generation,
                sha256: sha256(raw_accepted.as_bytes()),
                size: raw_accepted.len() as u64,
                bytes_base64: STANDARD.encode(raw_accepted.as_bytes()),
            },
            input_schemas: schemas,
            objects: objects.into_values().collect(),
        };
        super::super::super::inputs::metadata_size(&grant, budget.limits.max_metadata_bytes)?;
        Ok(Held {
            value: grant,
            _memory: memory,
        })
    }

    pub(super) async fn seal_model_grant(
        &self,
        admitted: &Admission,
        grant: &ModelGrant,
        budget: &Budget,
    ) -> Result<Held<(ProducerEvidence, JobArtifactWitness)>> {
        grant.validate(&admitted.request, &admitted.request_digest, budget)?;
        let _encoded_memory = self.request_memory(grant)?;
        let bytes = serde_json::to_vec(grant)?;
        let witness = self
            .retain_job_bytes(
                &grant.dataset,
                &admitted.directory,
                "producer-prerequisite.json",
                bytes,
                budget,
            )
            .await?;
        let evidence = ProducerEvidence {
            adoption: grant.adoption.clone(),
            role: Role::Model,
            evidence: EvidenceRef::from_witness(&witness),
            prerequisite: Some(grant.prerequisite.clone()),
        };
        let memory = self.request_memory(&(&evidence, &witness))?;
        Ok(Held {
            value: (evidence, witness),
            _memory: memory,
        })
    }
}
