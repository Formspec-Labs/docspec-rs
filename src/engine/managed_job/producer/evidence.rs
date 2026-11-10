//! Sealed producer evidence owned by existing job operation and result roots.
// Rust guideline compliant 2026-02-21
use super::*;
use admission::ProofAdmission;
use static_proof::{Records, record_mismatch};

pub(super) const STATIC_PROFILE: &str = "urn:docspec:whole-job-producer-static:1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::engine::managed_job) enum Role {
    Proof,
    Model,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine::managed_job) struct EvidenceRef {
    pub reference: ObjectRef,
    pub sha256: String,
}

impl EvidenceRef {
    pub(super) fn from_witness(witness: &JobArtifactWitness) -> Self {
        Self {
            reference: witness.reference.clone(),
            sha256: witness.sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine::managed_job) struct ProducerEvidence {
    pub adoption: JobProducerAdoptionRef,
    pub role: Role,
    pub evidence: EvidenceRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prerequisite: Option<JobPrerequisite>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StaticProof {
    pub(super) profile: String,
    pub(super) dataset: Id,
    pub(super) operation: Id,
    pub(super) request_digest: String,
    pub(super) adoption: JobProducerAdoptionRef,
    pub(super) definition: CurrentDefinition,
    pub(super) records: Vec<OriginalRecordObject>,
    pub(super) metadata: Vec<OriginalMetadataObject>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalRecordObject {
    pub(super) source: JobProducerRecordLocation,
    pub(super) sha256: String,
    pub(super) size: u64,
    pub(super) retained: ObjectRef,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalMetadataObject {
    pub(super) name: String,
    pub(super) object: snapshots::SnapshotObject,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StaticSeal {
    pub(super) operation: Id,
    pub(super) request_digest: String,
    pub(super) evidence: ProducerEvidence,
}

pub(in crate::engine::managed_job) struct VerifiedEvidence {
    pub(super) evidence: ProducerEvidence,
    pub(super) bytes: Vec<u8>,
}

pub(in crate::engine::managed_job) fn completion_evidence(
    document: &serde_json::Value,
    request: &ManagedJobPrepare,
    budget: &Budget,
) -> Result<Option<Held<VerifiedEvidence>>> {
    let Some(adoption) = &request.producer_adoption else {
        if document.get("producer_evidence").is_some()
            || document.get("producer_evidence_witness").is_some()
        {
            return Err(mismatch());
        }
        return Ok(None);
    };
    let (role, prerequisite) = match request.prerequisites.as_slice() {
        [] if !request.producer_records.is_empty() && request.file_inputs.is_empty() => {
            (Role::Proof, None)
        }
        [prerequisite] if request.producer_records.is_empty() => (Role::Model, Some(prerequisite)),
        _ => return Err(mismatch()),
    };
    let fields = (
        &document["producer_evidence"],
        &document["producer_evidence_witness"],
        &document["view_set"],
    );
    let bytes =
        super::super::super::inputs::metadata_size(&fields, budget.limits.max_metadata_bytes)?;
    let memory = budget.reserve(bytes.saturating_mul(8))?;
    let evidence: ProducerEvidence = serde_json::from_value(fields.0.clone())?;
    let witness: JobArtifactWitness = serde_json::from_value(fields.1.clone())?;
    let view: JobArtifactWitness = serde_json::from_value(fields.2.clone())?;
    if document["profile"] != PRODUCER_PROFILE
        || evidence.adoption != *adoption
        || evidence.role != role
        || evidence.prerequisite.as_ref() != prerequisite
        || evidence.evidence != EvidenceRef::from_witness(&witness)
        || witness.reference.owner != request.dataset
        || document["view_set_digest"] != view.reference.digest
    {
        return Err(mismatch());
    }
    #[derive(Deserialize)]
    struct ViewBinding {
        profile: String,
        producer_evidence: ProducerEvidence,
    }
    let view_bytes = witness_bytes(&view, budget)?;
    let binding: ViewBinding = serde_json::from_slice(&view_bytes.value)?;
    if binding.profile != "urn:docspec:whole-job-view-set:3"
        || binding.producer_evidence != evidence
    {
        return Err(mismatch());
    }
    let Held {
        value: bytes,
        mut _memory,
    } = witness_bytes(&witness, budget)?;
    _memory.merge(memory);
    Ok(Some(Held {
        value: VerifiedEvidence { evidence, bytes },
        _memory,
    }))
}

pub(super) fn validate_static(
    body: &StaticProof,
    evidence: &ProducerEvidence,
    dataset: &Id,
    operation: &Id,
    digest: &str,
    budget: &Budget,
) -> Result<()> {
    if body.profile != STATIC_PROFILE
        || body.dataset != *dataset
        || body.operation != *operation
        || body.request_digest != digest
        || body.adoption != evidence.adoption
        || body.definition.reference != body.adoption
        || evidence.role != Role::Proof
        || evidence.prerequisite.is_some()
    {
        return Err(mismatch());
    }
    let definition = decode_definition(&body.definition, budget)?;
    if body.metadata.len() != definition.inputs.len()
        || body
            .metadata
            .iter()
            .zip(&definition.inputs)
            .any(|(actual, input)| {
                let NativeSelector::NativeTable {
                    metadata_location, ..
                } = &input.selector;
                actual.name != input.name
                    || actual.object.uri != *metadata_location
                    || actual.object.role != "metadata"
                    || actual.object.sha256 != input.metadata.sha256
                    || actual.object.size != input.metadata.size
                    || actual.object.retained.bytes != input.metadata.size
                    || actual.object.retained.owner != *dataset
            })
        || body.records.is_empty()
    {
        return Err(mismatch());
    }
    let mut seen = BTreeSet::new();
    for record in &body.records {
        if record.size != record.retained.bytes
            || record.retained.owner != *dataset
            || !valid_sha256(&record.sha256)
            || !seen.insert(&record.source)
        {
            return Err(mismatch());
        }
    }
    Ok(())
}

pub(super) fn read_evidence(reference: &EvidenceRef, budget: &Budget) -> Result<Held<Vec<u8>>> {
    let bytes = execution::read_retained(&reference.reference, budget)?;
    if sha256(&bytes.value) != reference.sha256 {
        return Err(mismatch());
    }
    Ok(bytes)
}

pub(super) fn evidence_witness(
    reference: &EvidenceRef,
    budget: &Budget,
) -> Result<Held<JobArtifactWitness>> {
    let Held { value, _memory } = read_evidence(reference, budget)?;
    Ok(Held {
        value: JobArtifactWitness {
            reference: reference.reference.clone(),
            sha256: reference.sha256.clone(),
            bytes_base64: STANDARD.encode(value),
        },
        _memory,
    })
}

impl Engine {
    pub(super) async fn seal_static_proof(
        &self,
        admitted: &Admission,
        spec: &JobSpec,
        proof: &ProofAdmission,
        budget: &Budget,
    ) -> Result<Held<(ProducerEvidence, JobArtifactWitness)>> {
        let request = &admitted.request;
        let adoption = request.producer_adoption.as_ref().ok_or_else(mismatch)?;
        if adoption != &proof.definition.reference
            || proof.records.len() != request.producer_records.len()
        {
            return Err(mismatch());
        }
        let key = format!("job-producer-static/{}", request.operation);
        if let Some(saved) = self
            .job_record::<StaticSeal>(&request.dataset, &key)
            .await?
        {
            if saved.operation != request.operation
                || saved.request_digest != admitted.request_digest
                || saved.evidence.adoption != *adoption
                || saved.evidence.role != Role::Proof
                || saved.evidence.prerequisite.is_some()
            {
                return Err(mismatch());
            }
            let Held {
                value: witness,
                mut _memory,
            } = evidence_witness(&saved.evidence.evidence, budget)?;
            _memory.merge(self.request_memory(&saved.evidence)?);
            return Ok(Held {
                value: (saved.value.evidence, witness),
                _memory,
            });
        }

        let definition = decode_definition(&proof.definition, budget)?;
        let mut records = Records::new();
        let mut records_memory = budget.reserve(0)?;
        let mut evidence_memory = budget.reserve(0)?;
        let mut originals = Vec::new();
        for (original, witness) in request.producer_records.iter().zip(&proof.records) {
            if original.size != witness.reference.bytes
                || original.sha256 != witness.sha256
                || witness.reference.owner != request.dataset
            {
                return Err(mismatch());
            }
            let Held {
                value: bytes,
                _memory,
            } = witness_bytes(witness, budget)?;
            records.add(original, &bytes)?;
            self.inner
                .objects
                .create_candidate_metadata(&witness.reference, bytes, budget, true)
                .await?;
            records_memory.merge(_memory);
            let object = OriginalRecordObject {
                source: original.source.clone(),
                sha256: original.sha256.clone(),
                size: original.size,
                retained: witness.reference.clone(),
            };
            evidence_memory.merge(self.request_memory(&object)?);
            originals.push(object);
        }
        let mut metadata = Vec::new();
        for input in &definition.inputs {
            let selector = spec
                .inputs
                .iter()
                .find(|selector| selector.name() == input.name)
                .ok_or_else(|| record_mismatch("adopted input is absent from the proof job"))?;
            let observed = self
                .producer_snapshot_metadata(admitted, selector, budget)
                .await?;
            if observed.0.sha256 != input.metadata.sha256 || observed.0.size != input.metadata.size
            {
                return Err(record_mismatch(
                    "protected producer metadata differs from the original pin",
                ));
            }
            records.verify_input(&definition, input, &observed.1)?;
            let object = OriginalMetadataObject {
                name: input.name.clone(),
                object: observed.0.clone(),
            };
            evidence_memory.merge(self.request_memory(&object)?);
            metadata.push(object);
        }
        records.require_complete()?;
        drop(records);
        drop(records_memory);
        drop(definition);

        evidence_memory.merge(self.request_memory(&proof.definition)?);
        let body = StaticProof {
            profile: STATIC_PROFILE.into(),
            dataset: request.dataset.clone(),
            operation: request.operation.clone(),
            request_digest: admitted.request_digest.clone(),
            adoption: adoption.clone(),
            definition: proof.definition.clone(),
            records: originals,
            metadata,
        };
        super::super::super::inputs::metadata_size(&body, budget.limits.max_metadata_bytes)?;
        let bytes = serde_json::to_vec(&body)?;
        let witness = self
            .retain_job_bytes(
                &request.dataset,
                &admitted.directory,
                "producer-static.json",
                bytes,
                budget,
            )
            .await?;
        let evidence = ProducerEvidence {
            adoption: adoption.clone(),
            role: Role::Proof,
            evidence: EvidenceRef::from_witness(&witness),
            prerequisite: None,
        };
        let memory = self.request_memory(&(&evidence, &witness))?;
        drop(body);
        drop(evidence_memory);
        self.commit_job_record(
            &request.dataset,
            &key,
            &StaticSeal {
                operation: request.operation.clone(),
                request_digest: admitted.request_digest.clone(),
                evidence: evidence.clone(),
            },
        )
        .await?;
        self.job_phase_sealed(&key).await?;
        Ok(Held {
            value: (evidence, witness),
            _memory: memory,
        })
    }
}
