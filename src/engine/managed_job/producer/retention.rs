//! Expands producer evidence under existing operation, phase and result scopes.
// Rust guideline compliant 2026-02-21
use super::*;
use evidence::{StaticProof, StaticSeal};

pub(in crate::engine::managed_job) fn admission_roots(
    admitted: &Admission,
    budget: &Budget,
) -> Result<Vec<ObjectRef>> {
    let Some(producer) = &admitted.producer else {
        return if admitted.request.producer_adoption.is_none() {
            Ok(Vec::new())
        } else {
            Err(mismatch())
        };
    };
    match &producer.binding {
        admission::Binding::Proof(proof) => {
            if admitted.request.producer_adoption.as_ref() != Some(&proof.definition.reference)
                || proof.records.len() != admitted.request.producer_records.len()
            {
                return Err(mismatch());
            }
            let mut objects = Vec::new();
            for (record, witness) in admitted.request.producer_records.iter().zip(&proof.records) {
                if witness.sha256 != record.sha256 || witness.reference.bytes != record.size {
                    return Err(mismatch());
                }
                let _verified = witness_bytes(witness, budget)?;
                objects.push(witness.reference.clone());
            }
            Ok(objects)
        }
        admission::Binding::Model(grant) => {
            grant.validate(&admitted.request, &admitted.request_digest, budget)?;
            Ok(grant.objects.clone())
        }
    }
}

fn static_objects(body: StaticProof) -> impl Iterator<Item = ObjectRef> {
    body.records
        .into_iter()
        .map(|record| record.retained)
        .chain(
            body.metadata
                .into_iter()
                .map(|metadata| metadata.object.retained),
        )
}

pub(in crate::engine::managed_job) fn static_roots(
    key: &str,
    json: &str,
    table: &str,
    budget: &Budget,
) -> Result<Vec<ObjectRef>> {
    let seal: StaticSeal = serde_json::from_str(json)?;
    if key != format!("job-producer-static/{}", seal.operation)
        || !valid_sha256(&seal.request_digest)
        || !valid_reference(&seal.evidence.adoption, table)
    {
        return Err(mismatch());
    }
    let bytes = evidence::read_evidence(&seal.evidence.evidence, budget)?;
    let body: StaticProof = serde_json::from_slice(&bytes.value)?;
    evidence::validate_static(
        &body,
        &seal.evidence,
        &seal.evidence.adoption.dataset,
        &seal.operation,
        &seal.request_digest,
        budget,
    )?;
    let mut objects = vec![seal.evidence.evidence.reference];
    objects.extend(static_objects(body));
    Ok(objects)
}

pub(in crate::engine::managed_job) fn completion_roots(
    document: &serde_json::Value,
    request: &ManagedJobPrepare,
    budget: &Budget,
) -> Result<Vec<ObjectRef>> {
    let Some(verified) = evidence::completion_evidence(document, request, budget)? else {
        return Ok(Vec::new());
    };
    let digest = document["request_digest"].as_str().ok_or_else(mismatch)?;
    let mut objects = vec![verified.evidence.evidence.reference.clone()];
    match verified.evidence.role {
        Role::Proof => {
            let body: StaticProof = serde_json::from_slice(&verified.bytes)?;
            evidence::validate_static(
                &body,
                &verified.evidence,
                &request.dataset,
                &request.operation,
                digest,
                budget,
            )?;
            if body.records.len() != request.producer_records.len()
                || body
                    .records
                    .iter()
                    .zip(&request.producer_records)
                    .any(|(actual, original)| {
                        actual.source != original.source
                            || actual.sha256 != original.sha256
                            || actual.size != original.size
                    })
            {
                return Err(mismatch());
            }
            objects.extend(static_objects(body));
        }
        Role::Model => {
            let body: prerequisite::ModelGrant = serde_json::from_slice(&verified.bytes)?;
            body.validate(request, digest, budget)?;
            objects.extend(body.objects);
        }
    }
    Ok(objects)
}
