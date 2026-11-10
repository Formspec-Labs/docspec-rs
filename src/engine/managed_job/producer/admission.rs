//! Binds original producer evidence within the same guarded job admission.
// Rust guideline compliant 2026-02-21
use super::*;
use static_proof::record_mismatch;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine::managed_job) struct ProducerAdmission {
    pub(super) binding: Binding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "role",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum Binding {
    Proof(Box<ProofAdmission>),
    Model(Box<prerequisite::ModelGrant>),
}

impl ProducerAdmission {
    pub(in crate::engine::managed_job) fn role(&self) -> Role {
        match &self.binding {
            Binding::Proof(_) => Role::Proof,
            Binding::Model(_) => Role::Model,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProofAdmission {
    pub(super) definition: CurrentDefinition,
    pub(super) records: Vec<JobArtifactWitness>,
}

fn producer_definition_mismatch() -> Error {
    Error::refusal(
        "producer_definition_mismatch",
        "whole job does not match the exact adopted producer definition",
    )
}

fn policy_matches(policy: &JobPolicy, request: &ManagedJobPrepare) -> Result<()> {
    if policy.policy_id != request.policy_id || policy.policy_sha256 != request.policy.sha256 {
        return Err(Error::refusal(
            "producer_policy_mismatch",
            "whole job must use its exact adopted role policy",
        ));
    }
    Ok(())
}

fn scope_matches(
    request: &ManagedJobPrepare,
    spec: &JobSpec,
    definition: &Definition,
) -> Result<()> {
    if request.snapshot_inputs.len() != definition.inputs.len() {
        return Err(producer_definition_mismatch());
    }
    for input in &definition.inputs {
        let binding = request
            .snapshot_inputs
            .iter()
            .find(|candidate| candidate.name == input.name)
            .ok_or_else(producer_definition_mismatch)?;
        let declared = spec
            .inputs
            .iter()
            .find(|candidate| candidate.name() == input.name)
            .ok_or_else(producer_definition_mismatch)?;
        let selected = snapshots::selection(declared)?;
        let selector = NativeSelector::NativeTable {
            metadata_location: selected.metadata_location,
            table_uuid: selected.table_uuid,
            snapshot: selected.snapshot,
            schema_id: selected.schema_id,
        };
        if selector != input.selector
            || binding.metadata_sha256 != input.metadata.sha256
            || binding.metadata_size != input.metadata.size
        {
            return Err(producer_definition_mismatch());
        }
    }
    Ok(())
}

fn file_inventory_matches(request: &ManagedJobPrepare, definition: &Definition) -> Result<()> {
    let valid =
        request.file_inputs.len() == definition.model_file_inputs.len()
            && definition.model_file_inputs.iter().all(|required| {
                request
                    .file_inputs
                    .iter()
                    .find(|input| input.name == required.name)
                    .is_some_and(|actual| {
                        actual.members.len() == required.members.len()
                            && actual.members.iter().zip(&required.members).all(
                                |(actual, required)| {
                                    actual.key == required.key
                                        && actual.sha256 == required.sha256
                                        && actual.size == required.size
                                },
                            )
                    })
            });
    if !valid {
        return Err(Error::refusal(
            "producer_file_inventory_mismatch",
            "model file members differ from the adopted byte inventory",
        ));
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine::managed_job) async fn prepare_producer_admission(
        &self,
        request: &ManagedJobPrepare,
        spec: &JobSpec,
        directory: &Path,
        authority: &Authority,
        view: &View,
        rows: &SnapshotRows,
    ) -> Result<Option<Held<ProducerAdmission>>> {
        let Some(reference) = &request.producer_adoption else {
            require_unbound(authority)?;
            return Ok(None);
        };
        if authority.job_producer_adoption.as_ref() != Some(reference) {
            return Err(Error::refusal(
                "stale_authority",
                "the supplied producer adoption is not active",
            ));
        }
        let budget = &self.inner.lifecycle.budget;
        check_current(rows, view, authority, budget)?;
        let _active_memory = rows.reserve_decoded(budget, &[CURRENT])?;
        let (_, _, active): (_, _, CurrentDefinition) =
            rows.get_record(CURRENT)?.ok_or_else(mismatch)?;
        let definition = decode_definition(&active, budget)?;
        if definition.definition != request.definition
            || definition.definition != authority.definition
        {
            return Err(producer_definition_mismatch());
        }
        let (role, policy) = if definition.proof.job_sha256 == request.job.sha256 {
            (Role::Proof, &definition.proof)
        } else if definition.model.job_sha256 == request.job.sha256 {
            (Role::Model, &definition.model)
        } else {
            return Err(producer_definition_mismatch());
        };
        policy_matches(policy, request)?;
        scope_matches(request, spec, &definition)?;
        if matches!(role, Role::Model) {
            file_inventory_matches(request, &definition)?;
            let Held { value, _memory } = self
                .resolve_model_grant(request, &definition, reference, view, rows)
                .await?;
            return Ok(Some(Held {
                value: ProducerAdmission {
                    binding: Binding::Model(Box::new(value)),
                },
                _memory,
            }));
        }
        if !spec.outputs.is_empty()
            || !request.file_inputs.is_empty()
            || !request.prerequisites.is_empty()
            || request.producer_records.is_empty()
        {
            return Err(producer_definition_mismatch());
        }
        for input in &definition.inputs {
            if !request.producer_records.iter().any(|record| {
                record.source == input.head_record.source
                    && record.sha256 == input.head_record.sha256
                    && record.size == input.head_record.size
            }) {
                return Err(record_mismatch(
                    "the adopted original head record is missing",
                ));
            }
        }
        drop(definition);
        let mut distinct = BTreeSet::new();
        let mut total = 0u64;
        for record in &request.producer_records {
            if record.source.namespace.trim().is_empty()
                || record.source.key.trim().is_empty()
                || !valid_sha256(&record.sha256)
                || record.size == 0
                || record.size > 1 << 20
                || !distinct.insert(&record.source)
            {
                return Err(record_mismatch(
                    "original record inventory has an invalid pin",
                ));
            }
            total = total.checked_add(record.size).ok_or_else(capacity)?;
        }
        if total > budget.limits.max_metadata_bytes as u64 {
            return Err(capacity());
        }
        let mut memory = self.request_memory(&active)?;
        let mut records = Vec::new();
        for (index, record) in request.producer_records.iter().enumerate() {
            let artifact = JobArtifact {
                path: record.path.clone(),
                sha256: record.sha256.clone(),
            };
            let source_budget = budget.clone();
            let size = record.size;
            let bytes = budget
                .blocking_admitted(move || {
                    read_artifact_sized(&artifact, Some(size), &source_budget)
                })
                .await
                .map_err(|error| {
                    if error.code() == "managed_job_identity" {
                        record_mismatch("original producer record bytes differ from their pin")
                    } else {
                        error
                    }
                })?;
            let witness = planned_witness(
                &request.dataset,
                directory,
                &format!("producer-record-{index}.json"),
                &bytes.value,
            );
            memory.merge(self.request_memory(&witness)?);
            records.push(witness);
        }
        let value = ProducerAdmission {
            binding: Binding::Proof(Box::new(ProofAdmission {
                definition: active,
                records,
            })),
        };
        super::super::super::inputs::metadata_size(&value, budget.limits.max_metadata_bytes)?;
        Ok(Some(Held {
            value,
            _memory: memory,
        }))
    }
}
