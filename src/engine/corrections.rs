//! Scoped material-dependency correction and immutable supplemental qualification.

pub(in crate::engine) mod records;

use super::*;
use records::{Requirements, scope_key};

pub(in crate::engine) const CHECKER: &str = "docspec.scalar-field-correspondence.v1";

pub(in crate::engine) fn validate_qualification(proof: &DependencyQualification) -> Result<()> {
    if proof.checker != CHECKER
        || !proof
            .scope
            .strip_prefix("correction-requirements/")
            .is_some_and(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || proof.first_revision <= 0
        || proof.fields.is_empty()
        || proof.fields.iter().any(|id| *id <= 0)
        || proof.origins.owner != proof.target.metadata.owner
    {
        return Err(Error::refusal(
            "invalid_qualification",
            "invalid supplemental dependency evidence",
        ));
    }
    Ok(())
}

pub(in crate::engine) fn qualification_key(proof: &DependencyQualification) -> Result<String> {
    validate_qualification(proof)?;
    Ok(format!(
        "dependency-qualification/{}",
        storage::json_digest(proof)?.to_hex()
    ))
}

pub(in crate::engine) fn check_association(
    proof: &DependencyQualification,
    binding: &ComputedGroup,
    source: &TablePin,
) -> Result<()> {
    validate_qualification(proof)?;
    if binding.origins.as_ref() != Some(&proof.origins)
        || proof.scope != scope_key(&binding.definition)?
        || proof.target != source.into()
        || &proof.origins.owner != binding.request.dataset()
    {
        return Err(Error::refusal(
            "invalid_qualification",
            "supplemental evidence differs from its group or source",
        ));
    }
    Ok(())
}

fn expected(
    requirements: &Requirements,
    binding: &ComputedGroup,
    source: &TablePin,
) -> Result<DependencyQualification> {
    candidate(requirements, binding.origins.as_ref(), source)
}

fn candidate(
    requirements: &Requirements,
    origins: Option<&ObjectRef>,
    source: &TablePin,
) -> Result<DependencyQualification> {
    Ok(DependencyQualification {
        checker: CHECKER.into(),
        scope: scope_key(&requirements.definition)?,
        fields: requirements.fields.clone(),
        first_revision: requirements.first_revision,
        origins: origins.cloned().ok_or_else(unknown)?,
        target: source.into(),
    })
}

/// An inherited binding may add independently checked supplemental requirements.
pub(in crate::engine) fn inherits(child: &ComputedGroup, parent: &ComputedGroup) -> bool {
    if child.origins != parent.origins
        || child.definition != parent.definition
        || child.input_fields != parent.input_fields
        || child.request != parent.request
        || child.native_field_ids != parent.native_field_ids
        || child.source_metadata != parent.source_metadata
        || child.base_manifest != parent.base_manifest
    {
        return false;
    }
    match (&child.qualification, &parent.qualification) {
        (None, None) | (Some(_), None) => true,
        (Some(child), Some(parent)) => {
            child.checker == parent.checker
                && child.scope == parent.scope
                && child.first_revision == parent.first_revision
                && child.origins == parent.origins
                && child.target == parent.target
                && child.fields.is_superset(&parent.fields)
        }
        (None, Some(_)) => false,
    }
}

pub(in crate::engine) fn inherits_groups(
    child: &BTreeMap<String, ComputedGroup>,
    parent: &BTreeMap<String, ComputedGroup>,
) -> bool {
    child.len() == parent.len()
        && child.iter().all(|(name, binding)| {
            parent
                .get(name)
                .is_some_and(|previous| inherits(binding, previous))
        })
}

impl Engine {
    pub(super) async fn check_prepared_correction(
        &self,
        view: &View,
        origins: Option<&ObjectRef>,
        source: &TablePin,
        definition: &GroupDefinition,
        checks: &mut verification::Checks,
    ) -> Result<()> {
        if let Some(requirements) = self.requirements(view, definition).await? {
            let proof = candidate(&requirements, origins, source)?;
            checks
                .scalar_correspondence(&proof, source, &self.inner.lifecycle.budget)
                .await?;
        }
        Ok(())
    }
    /// Prepares new associations before their immutable readback is retained.
    pub(super) async fn qualify_corrections(
        &self,
        request: &OperationRequest,
        operation: &Operation,
        dataset: &mut DatasetPin,
        protections: &mut Option<ReferenceProtections>,
        checks: &mut verification::Checks,
    ) -> Result<()> {
        if dataset.computed.is_empty() {
            return Ok(());
        }
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&dataset.dataset).await?;
        let source = dataset.groups.get("source").ok_or_else(unknown)?;
        for (name, binding) in &mut dataset.computed {
            let Some(requirements) = self.requirements(&view, &binding.definition).await? else {
                continue;
            };
            let proof = expected(&requirements, binding, source)?;
            let key = qualification_key(&proof)?;
            if let Some(retained) = self
                .active_completion::<DependencyQualification>(&view, &key)
                .await?
            {
                if retained != proof {
                    return Err(Error::refusal(
                        "corrupt_metadata",
                        "qualification completion has conflicting content",
                    ));
                }
                ReferenceProtections::inherit(
                    protections,
                    self.retained_protections(&view, &key).await?.as_ref(),
                )?;
                binding.qualification = Some(Box::new(proof));
                continue;
            }
            let previous = binding.qualification.replace(Box::new(proof.clone()));
            let group = dataset.groups.get(name).ok_or_else(unknown)?;
            if let Err(error) = checks
                .scalar_qualification(binding, source, group, &lifecycle.budget)
                .await
            {
                binding.qualification = previous;
                return Err(error);
            }
            let lock = self.lock(&dataset.dataset).await;
            let _guard = lock.lock().await;
            let fresh = lifecycle.open(&dataset.dataset).await?;
            self.check_completion(&fresh, operation, request.operation())
                .await?;
            if let Some(retained) = self
                .active_completion::<DependencyQualification>(&fresh, &key)
                .await?
            {
                if retained != proof {
                    return Err(Error::refusal(
                        "corrupt_metadata",
                        "qualification completion has conflicting content",
                    ));
                }
                ReferenceProtections::inherit(
                    protections,
                    self.retained_protections(&fresh, &key).await?.as_ref(),
                )?;
            } else {
                let records = completion_records(
                    &dataset.dataset,
                    &fresh,
                    &key,
                    &proof,
                    protections.as_ref(),
                )?;
                lifecycle.commit(&fresh, records).await?;
            }
        }
        Ok(())
    }

    /// Checks every newly selected computed value against the same guarded view.
    pub(super) async fn guard_corrections(
        &self,
        view: &View,
        dataset: &DatasetPin,
        only_group: Option<&str>,
    ) -> Result<()> {
        let source = dataset.groups.get("source").ok_or_else(unknown)?;
        for (name, binding) in &dataset.computed {
            if only_group.is_some_and(|group| name != group) {
                continue;
            }
            let Some(requirements) = self.requirements(view, &binding.definition).await? else {
                continue;
            };
            let proof = expected(&requirements, binding, source)?;
            if binding.qualification.as_deref() != Some(&proof) {
                return Err(required());
            }
            check_association(&proof, binding, source)?;
        }
        Ok(())
    }
}

pub(in crate::engine) fn required() -> Error {
    Error::refusal(
        "correction_required",
        "new dependency requirements need a qualified association; use a new operation ID to reuse eligible completed values",
    )
}

pub(in crate::engine) fn mismatch() -> Error {
    Error::refusal(
        "correction_mismatch",
        "original dependency context changed or was not acknowledged before correction; use an explicitly corrected group definition",
    )
}

pub(in crate::engine) fn unknown() -> Error {
    Error::refusal(
        "correction_unknown",
        "original dependency context is unavailable; use an explicitly corrected group definition",
    )
}
