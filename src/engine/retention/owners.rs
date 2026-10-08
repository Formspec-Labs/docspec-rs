//! Owner-first durable protection, before another dataset retains a reference.

use super::*;
use crate::catalog::SnapshotRows;
use crate::engine::backfill::{FileCompletion, PreparedCompletion};
use std::path::Component;

#[cfg(all(test, feature = "test-util"))]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OwnerProtection {
    pub owner: Id,
    pub consumer: Id,
    pub table_uuid: String,
    pub revision: i64,
    pub objects: Vec<ObjectRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OwnerProtectionRef {
    pub owner: Id,
    pub token: String,
    pub table_uuid: String,
    pub revision: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReferenceBinding {
    pub consumer: Id,
    pub key: String,
    pub revision: i64,
    pub digest: String,
    pub protections: Vec<OwnerProtectionRef>,
}

// A future removal intent must reserve this exact owner-side identity before
// effects. Acquisition reads the same guarded state; no deletion is enabled here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnavailableObject {
    pub object: ObjectRef,
    pub intent: Id,
    pub revision: i64,
}

pub(in crate::engine) struct ReferenceProtections {
    references: Vec<OwnerProtectionRef>,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

impl ReferenceProtections {
    pub(in crate::engine) fn binding(
        &self,
        consumer: &Id,
        view: &View,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(String, serde_json::Value)> {
        record(
            binding_key(key),
            ReferenceBinding {
                consumer: consumer.clone(),
                key: key.into(),
                revision: view.revision.checked_add(1).ok_or_else(invalid)?,
                digest: value_digest(value)?,
                protections: self.references.clone(),
            },
        )
    }
}

pub(super) fn binding_key(key: &str) -> String {
    format!(
        "reference-binding/{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}

pub(super) fn unavailable_key(path: &str) -> String {
    format!(
        "object-unavailable/{}",
        blake3::hash(path.as_bytes()).to_hex()
    )
}

fn value_digest(value: &impl Serialize) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}

pub(super) fn protection_key(grant: &OwnerProtection) -> Result<String> {
    Ok(format!(
        "owner-protection/{}",
        value_digest(&(&grant.owner, &grant.consumer, &grant.objects))?
    ))
}

pub(super) fn validate_grant(
    view: &View,
    key: &str,
    revision: i64,
    grant: &OwnerProtection,
) -> Result<()> {
    validate_scope(&grant.owner, &grant.consumer, &grant.objects)?;
    if revision <= 0
        || revision > view.revision
        || grant.revision != revision
        || grant.table_uuid != view.table.metadata().uuid().to_string()
        || &crate::catalog::lifecycle_ident(&grant.owner) != view.table.identifier()
        || key != protection_key(grant)?
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_scope(owner: &Id, consumer: &Id, objects: &[ObjectRef]) -> Result<()> {
    if owner == consumer
        || objects.is_empty()
        || objects.iter().any(|object| &object.owner != owner)
        || objects.windows(2).any(|pair| pair[0].path >= pair[1].path)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn validate_binding(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
    binding: &ReferenceBinding,
) -> Result<Vec<ObjectRef>> {
    let (claim_revision, _, claim) = rows
        .get_record::<serde_json::Value>(&binding.key)?
        .ok_or_else(invalid)?;
    if key != binding_key(&binding.key)
        || binding.revision != revision
        || revision != claim_revision
        || revision <= 0
        || revision > view.revision
        || &crate::catalog::lifecycle_ident(&binding.consumer) != view.table.identifier()
        || binding.digest != value_digest(&claim)?
        || binding
            .protections
            .windows(2)
            .any(|pair| pair[0].owner >= pair[1].owner)
        || binding
            .protections
            .iter()
            .any(|reference| reference.owner == binding.consumer || reference.revision <= 0)
    {
        return Err(invalid());
    }
    // Seals are the first supported consumer. Derive their required roots from
    // the committed claim, never from the supplied protection list itself.
    let seal: GenerationSeal = serde_json::from_value(claim)?;
    if binding.key != format!("generation/{}", seal.generation)
        || binding.consumer != seal.family
        || seal.members.is_empty()
        || seal.members.iter().any(|(dataset, version)| {
            dataset != &version.dataset || dataset != &version.manifest.owner
        })
    {
        return Err(invalid());
    }
    let required = seal
        .members
        .into_values()
        .map(|version| version.manifest)
        .filter(|object| object.owner != binding.consumer)
        .collect::<Vec<_>>();
    if required.iter().any(|object| {
        binding
            .protections
            .binary_search_by(|reference| reference.owner.cmp(&object.owner))
            .is_err()
    }) {
        return Err(invalid());
    }
    Ok(required)
}

pub(super) fn validate_reference<'a>(
    reference: &OwnerProtectionRef,
    grant: &OwnerProtection,
    consumer: &Id,
    required: impl IntoIterator<Item = &'a ObjectRef>,
) -> Result<()> {
    if grant.owner != reference.owner
        || &grant.consumer != consumer
        || grant.table_uuid != reference.table_uuid
        || grant.revision != reference.revision
        || required
            .into_iter()
            .filter(|object| object.owner == reference.owner)
            .any(|object| {
                grant
                    .objects
                    .binary_search_by(|candidate| candidate.path.cmp(&object.path))
                    .ok()
                    .is_none_or(|index| grant.objects[index] != *object)
            })
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn available(rows: &SnapshotRows, view: &View, object: &ObjectRef) -> Result<()> {
    if let Some((revision, _, unavailable)) =
        rows.get_record::<UnavailableObject>(&unavailable_key(&object.path))?
    {
        if revision <= 0
            || revision > view.revision
            || unavailable.revision != revision
            || unavailable.object.path != object.path
            || unavailable.object.owner != object.owner
        {
            return Err(invalid());
        }
        return Err(Error::refusal(
            "object_unavailable",
            "owner has reserved this object for removal",
        ));
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine) async fn check_reference_binding(
        &self,
        view: &View,
        claim: &str,
    ) -> Result<()> {
        let rows = self.inner.lifecycle.rows(view).await?;
        let key = binding_key(claim);
        let Some((revision, _, binding)) = rows.get_record::<ReferenceBinding>(&key)? else {
            // Older immutable promises remain protected by complete inventory;
            // their histories are not rewritten to claim an earlier acquisition.
            return Ok(());
        };
        let required = validate_binding(&rows, view, &key, revision, &binding)?;
        self.check_protection_references(&binding, &required).await
    }

    pub(super) async fn check_protection_references(
        &self,
        binding: &ReferenceBinding,
        required: &[ObjectRef],
    ) -> Result<()> {
        let lifecycle = &self.inner.lifecycle;
        let _memory = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(4))?;
        for reference in &binding.protections {
            let view = lifecycle.open(&reference.owner).await?;
            let rows = lifecycle.rows(&view).await?;
            let (revision, _, grant) = rows
                .get_record::<OwnerProtection>(&reference.token)?
                .ok_or_else(invalid)?;
            validate_grant(&view, &reference.token, revision, &grant)?;
            validate_reference(reference, &grant, &binding.consumer, required)?;
            for object in &grant.objects {
                available(&rows, &view, object)?;
            }
        }
        Ok(())
    }

    // Call before consumer locks. Owner commits precede consumer acknowledgment;
    // a failed handoff keeps extra protection, never a dangling retained root.
    pub(in crate::engine) async fn protect_releases(
        &self,
        consumer: &Id,
        releases: &[ReleasePin],
    ) -> Result<ReferenceProtections> {
        let inventory = self.reference_objects(releases).await?;
        let budget = &self.inner.lifecycle.budget;
        let bytes = inputs::metadata_size(&inventory.objects, budget.limits.max_metadata_bytes)?;
        let memory = budget.reserve(bytes.saturating_mul(4).saturating_add(4096))?;
        let mut owners: BTreeMap<Id, Vec<ObjectRef>> = BTreeMap::new();
        for object in &inventory.objects {
            if &object.owner != consumer {
                owners
                    .entry(object.owner.clone())
                    .or_default()
                    .push(object.clone());
            }
        }
        let mut references = Vec::with_capacity(owners.len());
        for (owner, mut objects) in owners {
            objects.sort_by(|left, right| left.path.cmp(&right.path));
            let lock = self.lock(&owner).await;
            let _guard = lock.lock().await;
            references.push(self.acquire_owner(&owner, consumer, objects).await?);
        }
        Ok(ReferenceProtections {
            references,
            _memory: memory,
        })
    }

    async fn acquire_owner(
        &self,
        owner: &Id,
        consumer: &Id,
        objects: Vec<ObjectRef>,
    ) -> Result<OwnerProtectionRef> {
        validate_scope(owner, consumer, &objects)?;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(owner).await?;
        let rows = lifecycle.rows(&view).await?;
        let grant = OwnerProtection {
            owner: owner.clone(),
            consumer: consumer.clone(),
            table_uuid: view.table.metadata().uuid().to_string(),
            revision: view.revision.checked_add(1).ok_or_else(invalid)?,
            objects,
        };
        let key = protection_key(&grant)?;
        for object in &grant.objects {
            available(&rows, &view, object)?;
            self.check_object_owner(&rows, owner, object)?;
        }
        if let Some((revision, _, previous)) = rows.get_record::<OwnerProtection>(&key)? {
            validate_grant(&view, &key, revision, &previous)?;
            if previous.owner != *owner
                || previous.consumer != *consumer
                || previous.objects != grant.objects
            {
                return Err(invalid());
            }
            return Ok(reference(&key, &previous));
        }
        // Reservations prove who produces at a path; typed completed evidence
        // also proves the exact immutable identity. Provisional files cannot grant
        // foreign references solely because they lie inside a reserved directory.
        let _decoding = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(4))?;
        confirm_creation(&rows, &view, &grant.objects).await?;
        if let Err(error) = lifecycle.commit(&view, vec![record(&key, &grant)?]).await {
            let after = lifecycle.open(owner).await?;
            let rows = lifecycle.rows(&after).await?;
            if let Some((revision, _, previous)) = rows.get_record::<OwnerProtection>(&key)? {
                validate_grant(&after, &key, revision, &previous)?;
                if previous == grant {
                    return Ok(reference(&key, &previous));
                }
                return Err(invalid());
            }
            return Err(error);
        }
        Ok(reference(&key, &grant))
    }

    fn check_object_owner(
        &self,
        rows: &SnapshotRows,
        owner: &Id,
        object: &ObjectRef,
    ) -> Result<()> {
        if object.owner != *owner
            || object.digest.len() != 64
            || !object.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
        let prefix = self.inner.root.join("objects").join(owner.as_str());
        let relative = Path::new(&object.path)
            .strip_prefix(&prefix)
            .map_err(|_| invalid())?;
        let mut components = relative.components();
        let Some(Component::Normal(attempt)) = components.next() else {
            return Err(invalid());
        };
        let attempt = Id::new(attempt.to_str().ok_or_else(invalid)?)?;
        let remainder = components.collect::<Vec<_>>();
        if remainder.is_empty()
            || remainder
                .iter()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(invalid());
        }
        let (_, _, reservation) = rows
            .get_record::<Protection>(&format!("protection/{attempt}"))?
            .ok_or_else(invalid)?;
        if reservation.id != attempt
            || reservation.consumer != *owner
            || reservation.paths != [format!("{}/", prefix.join(attempt.as_str()).display())]
        {
            return Err(invalid());
        }
        Ok(())
    }
}

fn reference(key: &str, grant: &OwnerProtection) -> OwnerProtectionRef {
    OwnerProtectionRef {
        owner: grant.owner.clone(),
        token: key.into(),
        table_uuid: grant.table_uuid.clone(),
        revision: grant.revision,
    }
}

// Only retained typed creation evidence or a prior owner grant qualifies a new
// reference. This scans one owner's cached rows, not every catalog namespace.
async fn confirm_creation(rows: &SnapshotRows, view: &View, objects: &[ObjectRef]) -> Result<()> {
    let expected: BTreeMap<_, _> = objects
        .iter()
        .map(|object| (object.path.as_str(), object))
        .collect();
    let mut missing: BTreeSet<_> = expected.keys().copied().collect();
    let mut observe = |object: &ObjectRef| -> Result<()> {
        if let Some(expected) = expected.get(object.path.as_str()) {
            if **expected != *object {
                return Err(invalid());
            }
            missing.remove(object.path.as_str());
        }
        Ok(())
    };
    for (key, revision, json) in rows.prefix("") {
        tokio::task::consume_budget().await;
        if key.starts_with("phase/backfill/") {
            let phase: FileCompletion = serde_json::from_str(json)?;
            observe(&phase.descriptor)?;
            for object in &phase.outputs {
                observe(object)?;
            }
        } else if key.starts_with("phase/") {
            let phase: PhaseResult = serde_json::from_str(json)?;
            for object in &phase.outputs {
                observe(object)?;
            }
            if let Some(table) = &phase.table {
                for object in &table.objects {
                    observe(object)?;
                }
            }
        } else if key.starts_with("backfill-prepared/") {
            let phase: PreparedCompletion = serde_json::from_str(json)?;
            for object in &phase.table.objects {
                observe(object)?;
            }
        } else if key.starts_with("result/")
            || key.starts_with("readback/")
            || key.starts_with("backfill-readback/")
        {
            let release: ReleasePin = serde_json::from_str(json)?;
            observe(&release.manifest)?;
            for object in release_objects(&release) {
                observe(&object)?;
            }
        } else if key.starts_with("owner-protection/") {
            let grant: OwnerProtection = serde_json::from_str(json)?;
            validate_grant(view, key, revision, &grant)?;
            for object in &grant.objects {
                observe(object)?;
            }
        }
    }
    if !missing.is_empty() {
        return Err(Error::refusal(
            "unqualified_owner_reference",
            "object lacks retained typed creation evidence at its owner",
        ));
    }
    Ok(())
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_owner_protection",
        "owner, immutable object identity or committed protection differs",
    )
}
