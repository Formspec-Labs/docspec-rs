//! Owner-first durable protection, before another dataset retains a reference.

use super::*;
use crate::catalog::SnapshotRows;
use crate::engine::backfill::{
    FileCompletion, PreparedCompletion, ValidationCompletion,
    origins::{self, ScalarOrigins},
};
use std::path::Component;

#[cfg(all(test, feature = "test-util"))]
mod phase_tests;
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    consumer: Id,
    references: Vec<OwnerProtectionRef>,
    limit: usize,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

impl ReferenceProtections {
    pub(in crate::engine) fn duplicate(&self) -> Result<Self> {
        let bytes = inputs::metadata_size(&(&self.consumer, &self.references), self.limit)?
            .saturating_mul(4)
            .saturating_add(4096);
        let count = u32::try_from(bytes).map_err(|_| capacity())?;
        let memory = self
            ._memory
            .semaphore()
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| capacity())?;
        Ok(Self {
            consumer: self.consumer.clone(),
            references: self.references.clone(),
            limit: self.limit,
            _memory: memory,
        })
    }

    /// Unknown provenance cannot become qualified by combining it with known work.
    pub(in crate::engine) fn inherit(
        target: &mut Option<Self>,
        incoming: Option<&Self>,
    ) -> Result<()> {
        match (target.as_mut(), incoming) {
            (Some(target), Some(incoming)) => target.merge(incoming.duplicate()?),
            (_, None) => {
                *target = None;
                Ok(())
            }
            (None, Some(_)) => Ok(()),
        }
    }

    pub(in crate::engine) fn empty(consumer: &Id, budget: &Budget) -> Result<Self> {
        Ok(Self {
            consumer: consumer.clone(),
            references: Vec::new(),
            limit: budget.limits.max_metadata_bytes,
            _memory: budget.reserve(consumer.as_str().len().saturating_add(4096))?,
        })
    }

    /// Combines already acquired scopes without writing grants or reopening metadata.
    pub(in crate::engine) fn merge(&mut self, other: Self) -> Result<()> {
        if self.consumer != other.consumer
            || self.limit != other.limit
            || !Arc::ptr_eq(self._memory.semaphore(), other._memory.semaphore())
        {
            return Err(invalid());
        }
        let mut bytes = inputs::metadata_size(&(&self.consumer, &self.references), self.limit)?;
        for reference in &other.references {
            match self
                .references
                .binary_search_by(|candidate| reference_order(candidate, reference))
            {
                Ok(index) if self.references[index] != *reference => return Err(invalid()),
                Ok(_) => {}
                Err(_) => {
                    bytes = bytes.saturating_add(inputs::metadata_size(reference, self.limit)? + 1);
                    if bytes > self.limit {
                        return Err(capacity());
                    }
                }
            }
        }
        self._memory.merge(other._memory);
        self.references.extend(other.references);
        self.references.sort_unstable_by(reference_order);
        self.references.dedup();
        // Release duplicate/transient state charges after the in-place merge.
        self.retain_memory()?;
        Ok(())
    }

    fn retain_memory(&mut self) -> Result<()> {
        let bytes = inputs::metadata_size(&(&self.consumer, &self.references), self.limit)?
            .saturating_mul(4)
            .saturating_add(4096);
        if self._memory.num_permits() > bytes {
            drop(self._memory.split(self._memory.num_permits() - bytes));
        }
        Ok(())
    }

    pub(in crate::engine) fn binding(
        &self,
        consumer: &Id,
        view: &View,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(String, serde_json::Value)> {
        if consumer != &self.consumer
            || &crate::catalog::lifecycle_ident(consumer) != view.identifier()
        {
            return Err(invalid());
        }
        inputs::metadata_size(value, self.limit)?;
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

fn reference_order(left: &OwnerProtectionRef, right: &OwnerProtectionRef) -> std::cmp::Ordering {
    (&left.owner, &left.token).cmp(&(&right.owner, &right.token))
}

pub(super) fn binding_key(key: &str) -> String {
    format!(
        "reference-binding/{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}

pub(in crate::engine) fn unavailable_key(path: &str) -> String {
    format!(
        "object-unavailable/{}",
        blake3::hash(path.as_bytes()).to_hex()
    )
}

fn value_digest(value: &impl Serialize) -> Result<String> {
    Ok(storage::json_digest(value)?.to_hex().to_string())
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
        || &crate::catalog::lifecycle_ident(&grant.owner) != view.identifier()
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
    limit: usize,
) -> Result<Vec<ObjectRef>> {
    let (claim_revision, claim_json) = rows
        .prefix(&binding.key)
        .find(|(key, _, _)| *key == binding.key)
        .map(|(_, revision, json)| (revision, json))
        .ok_or_else(invalid)?;
    if claim_json.len() > limit {
        return Err(capacity());
    }
    let claim: serde_json::Value = serde_json::from_str(claim_json)?;
    if key != binding_key(&binding.key)
        || binding.revision != revision
        || revision != claim_revision
        || revision <= 0
        || revision > view.revision
        || &crate::catalog::lifecycle_ident(&binding.consumer) != view.identifier()
        || binding.digest != value_digest(&claim)?
        || binding
            .protections
            .windows(2)
            .any(|pair| reference_order(&pair[0], &pair[1]) != std::cmp::Ordering::Less)
        || binding
            .protections
            .iter()
            .any(|reference| reference.owner == binding.consumer || reference.revision <= 0)
    {
        return Err(invalid());
    }
    let origin = RetentionBlocker {
        table: view.identifier().to_string(),
        key: binding.key.clone(),
        revision: claim_revision,
    };
    // One typed root walker serves inspection and local propagation. This mode
    // collects direct declarations only; creation already qualified the closure.
    let required =
        super::inventory::reference_roots(rows, &binding.key, claim_json, &origin, limit)?
            .into_iter()
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

// Replacing a retired completion from unknown legacy inputs may leave its older
// binding row in history. It never protects the new generation. The caller must
// also validate the retirement receipt (or use the inventory's validated set).
pub(super) fn earlier_binding(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
    binding: &ReferenceBinding,
) -> Result<bool> {
    let claim_revision = rows.revision(&binding.key).ok_or_else(invalid)?;
    if claim_revision <= revision {
        return Ok(false);
    }
    if key != binding_key(&binding.key)
        || binding.revision != revision
        || revision <= 0
        || claim_revision > view.revision
        || &crate::catalog::lifecycle_ident(&binding.consumer) != view.identifier()
    {
        return Err(invalid());
    }
    let (retirement_revision, _) =
        super::retirement::retirement_marker(rows, view, &binding.key, revision)?
            .ok_or_else(invalid)?;
    if retirement_revision >= claim_revision {
        return Err(invalid());
    }
    Ok(true)
}

/// Admits a bound claim and any retained base decoded by the shared root walker.
pub(super) fn reserve_claim(
    rows: &SnapshotRows,
    budget: &Budget,
    claim: &str,
) -> Result<tokio::sync::OwnedSemaphorePermit> {
    let mut memory = rows.reserve_decoded(budget, &[claim])?;
    if claim.starts_with("operation/") {
        let (_, operation): (_, Operation) =
            read_retained(rows, claim, budget.limits.max_metadata_bytes)?.ok_or_else(invalid)?;
        if let Some(base) = operation.base {
            memory.merge(rows.reserve_decoded(budget, &[&format!("result/{base}")])?);
        }
    }
    Ok(memory)
}

pub(super) fn validate_reference(
    reference: &OwnerProtectionRef,
    grant: &OwnerProtection,
    consumer: &Id,
) -> Result<()> {
    if grant.owner != reference.owner
        || &grant.consumer != consumer
        || grant.table_uuid != reference.table_uuid
        || grant.revision != reference.revision
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn check_grant_active(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
    budget: &Budget,
) -> Result<()> {
    if super::retirement::record_retired(rows, view, key, revision, budget)? {
        return Err(Error::refusal(
            "grant_retired",
            "owner protection generation was explicitly retired",
        ));
    }
    Ok(())
}

/// Marks the portion of one claim covered by this grant; other grants may add more.
pub(super) fn cover_required<'a>(
    required: impl IntoIterator<Item = &'a mut (ObjectRef, bool)>,
    grant: &OwnerProtection,
) -> Result<()> {
    for (object, covered) in required
        .into_iter()
        .filter(|(object, _)| object.owner == grant.owner)
    {
        if let Ok(index) = grant
            .objects
            .binary_search_by(|candidate| candidate.path.cmp(&object.path))
        {
            if grant.objects[index] != *object {
                return Err(invalid());
            }
            *covered = true;
        }
    }
    Ok(())
}

pub(super) fn check_covered<'a>(
    required: impl IntoIterator<Item = &'a (ObjectRef, bool)>,
) -> Result<()> {
    if required.into_iter().any(|(_, covered)| !covered) {
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
    pub(in crate::engine) async fn request_protections(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<Option<ReferenceProtections>> {
        self.retained_protections(view, &admission::dependencies_key(operation))
            .await
    }

    /// Loads immutable references after checking the exact local claim identity.
    ///
    /// No owner is reopened: retirement excludes admitted propagation and refuses
    /// every live bound claim. Callers propagate only the generations consumed;
    /// newly introduced scope still requires owner acquisition first.
    pub(in crate::engine) async fn retained_protections(
        &self,
        view: &View,
        claim: &str,
    ) -> Result<Option<ReferenceProtections>> {
        let Some((binding, _, memory)) = self.local_binding(view, claim).await? else {
            return Ok(None);
        };
        let mut protections = ReferenceProtections {
            consumer: binding.consumer,
            references: binding.protections,
            limit: self.inner.lifecycle.budget.limits.max_metadata_bytes,
            _memory: memory,
        };
        protections.retain_memory()?;
        Ok(Some(protections))
    }

    async fn local_binding(
        &self,
        view: &View,
        claim: &str,
    ) -> Result<
        Option<(
            ReferenceBinding,
            Vec<ObjectRef>,
            tokio::sync::OwnedSemaphorePermit,
        )>,
    > {
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let rows = lifecycle.rows(view).await?;
        let key = binding_key(claim);
        if !rows.contains_key(&key) {
            // Missing legacy bindings do not claim an earlier acquisition.
            return Ok(None);
        }
        let mut memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
        memory.merge(reserve_claim(&rows, &lifecycle.budget, claim)?);
        let (revision, binding): (_, ReferenceBinding) =
            read_retained(&rows, &key, limit)?.ok_or_else(invalid)?;
        if binding.key != claim {
            return Err(invalid());
        }
        if earlier_binding(&rows, view, &key, revision, &binding)? {
            if !super::retirement::record_retired(&rows, view, claim, revision, &lifecycle.budget)?
            {
                return Err(invalid());
            }
            return Ok(None);
        }
        if super::retirement::record_retired(&rows, view, claim, revision, &lifecycle.budget)? {
            return Err(Error::refusal(
                "retained_claim_retired",
                "retired recovery cannot provide protection to new work",
            ));
        }
        let required = validate_binding(&rows, view, &key, revision, &binding, limit)?;
        Ok(Some((binding, required, memory)))
    }

    pub(in crate::engine) async fn check_reference_binding(
        &self,
        view: &View,
        claim: &str,
    ) -> Result<()> {
        let Some((binding, required, _memory)) = self.local_binding(view, claim).await? else {
            return Ok(());
        };
        self.check_protection_references(&binding, &required).await
    }

    pub(super) async fn check_protection_references(
        &self,
        binding: &ReferenceBinding,
        required: &[ObjectRef],
    ) -> Result<()> {
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let _memory = lifecycle.budget.reserve(limit.saturating_mul(4))?;
        let mut required = required
            .iter()
            .cloned()
            .map(|object| (object, false))
            .collect::<Vec<_>>();
        for references in binding
            .protections
            .chunk_by(|left, right| left.owner == right.owner)
        {
            let view = lifecycle.open(&references[0].owner).await?;
            let rows = lifecycle.rows(&view).await?;
            for reference in references {
                tokio::task::consume_budget().await;
                let (revision, grant): (_, OwnerProtection) =
                    read_retained(&rows, &reference.token, limit)?.ok_or_else(invalid)?;
                validate_grant(&view, &reference.token, revision, &grant)?;
                validate_reference(reference, &grant, &binding.consumer)?;
                check_grant_active(&rows, &view, &reference.token, revision, &lifecycle.budget)?;
                cover_required(&mut required, &grant)?;
                for object in &grant.objects {
                    available(&rows, &view, object)?;
                }
            }
        }
        check_covered(&required)
    }

    // Call before consumer locks. Owner commits precede consumer acknowledgment;
    // a failed handoff keeps extra protection, never a dangling retained root.
    pub(in crate::engine) async fn protect_releases(
        &self,
        consumer: &Id,
        releases: &[ReleasePin],
    ) -> Result<ReferenceProtections> {
        if releases.is_empty() {
            return ReferenceProtections::empty(consumer, &self.inner.lifecycle.budget);
        }
        let inventory = self.reference_objects(releases).await?;
        self.protect_objects(consumer, &inventory.objects).await
    }

    /// Protects already qualified exact objects without walking release metadata.
    pub(in crate::engine) async fn protect_objects(
        &self,
        consumer: &Id,
        objects: &[ObjectRef],
    ) -> Result<ReferenceProtections> {
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        let bytes = inputs::metadata_size(&objects, limit)?;
        let memory = budget.reserve(bytes.saturating_mul(4).saturating_add(4096))?;
        // Deduplication covers own objects too: filtering ownership first would
        // hide a conflicting declaration for the same immutable path.
        let mut exact = BTreeMap::new();
        for object in objects {
            super::inventory::validate_object(object)?;
            if let Some(previous) = exact.insert(object.path.as_str(), object)
                && previous != object
            {
                return Err(invalid());
            }
        }
        let mut owners: BTreeMap<Id, Vec<ObjectRef>> = BTreeMap::new();
        for object in exact
            .into_values()
            .filter(|object| &object.owner != consumer)
        {
            owners
                .entry(object.owner.clone())
                .or_default()
                .push(object.clone());
        }
        let mut references = Vec::with_capacity(owners.len());
        for (owner, objects) in owners {
            let lock = self.lock(&owner).await;
            let _guard = lock.lock().await;
            references.push(self.acquire_owner(&owner, consumer, objects).await?);
        }
        let mut protections = ReferenceProtections {
            consumer: consumer.clone(),
            references,
            limit,
            _memory: memory,
        };
        protections.retain_memory()?;
        Ok(protections)
    }

    /// Resolves an exact managed object from retained creation evidence only.
    ///
    /// Resolution is provisional. Foreign acquisition rechecks under the owner
    /// lock; own-object admission must retain and recheck its consumer boundary.
    pub(in crate::engine) async fn qualified_object(
        &self,
        owner: &Id,
        path: &str,
        digest: &str,
    ) -> Result<ObjectRef> {
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        inputs::metadata_size(&(owner, path, digest), limit)?;
        let _memory = lifecycle.budget.reserve(limit.saturating_mul(4))?;
        let view = lifecycle.open(owner).await?;
        let rows = lifecycle.rows(&view).await?;
        let mut found: Option<ObjectRef> = None;
        {
            let mut observe = |object: &ObjectRef| {
                if object.path == path {
                    if object.owner != *owner
                        || object.digest != digest
                        || found.as_ref().is_some_and(|previous| previous != object)
                    {
                        return Err(invalid());
                    }
                    if found.is_none() {
                        found = Some(object.clone());
                    }
                }
                Ok(())
            };
            visit_creation(&rows, &view, limit, &mut observe).await?;
        }
        if found.is_none() {
            visit_original_creation(&rows, &view, &lifecycle.budget, None, |object| {
                if object.path == path {
                    if object.owner != *owner
                        || object.digest != digest
                        || found.as_ref().is_some_and(|previous| previous != object)
                    {
                        return Err(invalid());
                    }
                    if found.is_none() {
                        found = Some(object.clone());
                    }
                }
                Ok(found.is_some())
            })
            .await?;
        }
        let object = found.ok_or_else(unqualified)?;
        super::inventory::validate_object(&object)?;
        self.check_object_owner(&rows, owner, &object)?;
        available(&rows, &view, &object)?;
        Ok(object)
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
        let _previous_memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
        let known_scope = if let Some((revision, _, previous)) =
            rows.get_record::<OwnerProtection>(&key)?
        {
            validate_grant(&view, &key, revision, &previous)?;
            if previous.owner != *owner
                || previous.consumer != *consumer
                || previous.objects != grant.objects
            {
                return Err(invalid());
            }
            if !super::retirement::record_retired(&rows, &view, &key, revision, &lifecycle.budget)?
            {
                return Ok(reference(&key, &previous));
            }
            true
        } else {
            false
        };
        drop(_previous_memory);
        // Reservations prove who produces at a path; typed completed evidence
        // also proves the exact immutable identity. Provisional files cannot grant
        // foreign references solely because they lie inside a reserved directory.
        if !known_scope {
            let _decoding = lifecycle
                .budget
                .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(4))?;
            confirm_creation(&rows, &view, &grant.objects, &lifecycle.budget).await?;
        }
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

    pub(super) fn check_object_owner(
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
        self.inner.objects.validate_retained_path(&object.path)?;
        let prefix = self.inner.retained.child(owner.as_str())?;
        let relative = prefix.relative(&object.path).ok_or_else(invalid)?;
        let mut components = Path::new(relative).components();
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
        if reservation.id != attempt || reservation.consumer != *owner {
            return Err(invalid());
        }
        let retained_prefix = prefix.child(attempt.as_str())?.protection_prefix()?;
        if reservation.paths.len() == 1 && reservation.paths[0] == retained_prefix {
            return Ok(());
        }
        // Remote exports protect both their host stage and exact remote target.
        // Require the existing typed transfer proof, not merely an extra matching
        // prefix: another file beneath the same reservation is not an export.
        if self.inner.retained.is_local() {
            return Err(invalid());
        }
        let operation = Id::new(
            reservation
                .purpose
                .strip_suffix("/export")
                .ok_or_else(invalid)?,
        )?;
        let completed = crate::engine::export::remote::completed_stage(
            rows,
            &operation,
            Some(&self.inner.lifecycle.budget),
        )?
        .ok_or_else(invalid)?;
        if completed.attempt != attempt
            || completed.remote != *object
            || reservation.paths != [completed.host_prefix, retained_prefix]
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
pub(super) async fn confirm_creation(
    rows: &SnapshotRows,
    view: &View,
    objects: &[ObjectRef],
    budget: &Budget,
) -> Result<()> {
    let limit = budget.limits.max_metadata_bytes;
    let expected: BTreeMap<_, _> = objects
        .iter()
        .map(|object| (object.path.as_str(), object))
        .collect();
    let mut missing: BTreeSet<_> = expected.keys().copied().collect();
    visit_creation(rows, view, limit, |object: &ObjectRef| -> Result<()> {
        if let Some(expected) = expected.get(object.path.as_str()) {
            if **expected != *object {
                return Err(invalid());
            }
            missing.remove(object.path.as_str());
        }
        Ok(())
    })
    .await?;
    if !missing.is_empty() {
        visit_original_creation(rows, view, budget, Some(&expected), |object| {
            if let Some(expected) = expected.get(object.path.as_str()) {
                if **expected != *object {
                    return Err(invalid());
                }
                missing.remove(object.path.as_str());
            }
            Ok(missing.is_empty())
        })
        .await?;
    }
    if !missing.is_empty() {
        return Err(unqualified());
    }
    Ok(())
}

// Original file completions survive cache-key replacement inside immutable
// origins retained by prepared groups, validation, qualifications or releases.
// Consult these only after the direct creation pass misses a requested object,
// and stop once it is found.
async fn visit_original_creation(
    rows: &SnapshotRows,
    view: &View,
    budget: &Budget,
    preferred: Option<&BTreeMap<&str, &ObjectRef>>,
    mut observe: impl FnMut(&ObjectRef) -> Result<bool>,
) -> Result<()> {
    let limit = budget.limits.max_metadata_bytes;
    let _identities = budget.reserve(limit)?;
    let mut anchors: BTreeMap<String, ObjectRef> = BTreeMap::new();
    let mut identity_bytes = 0usize;
    for (key, revision, json) in rows.prefix("") {
        tokio::task::consume_budget().await;
        if !(key.starts_with("backfill-prepared/")
            || key.starts_with("backfill-validation/")
            || key.starts_with("result/")
            || key.starts_with("readback/")
            || key.starts_with("backfill-readback/")
            || key.starts_with("dependency-qualification/"))
        {
            continue;
        }
        if json.len() > limit {
            return Err(capacity());
        }
        if revision <= 0 || revision > view.revision {
            return Err(invalid());
        }
        let retained = if key.starts_with("dependency-qualification/") {
            let qualification: DependencyQualification = serde_json::from_str(json)?;
            crate::engine::corrections::validate_qualification(&qualification)?;
            if key != crate::engine::corrections::qualification_key(&qualification)?
                || crate::catalog::lifecycle_ident(&qualification.origins.owner).to_string()
                    != view.identifier().to_string()
            {
                return Err(invalid());
            }
            vec![qualification.origins]
        } else if key.starts_with("backfill-prepared/") {
            let phase: PreparedCompletion = serde_json::from_str(json)?;
            phase.origins.into_iter().collect::<Vec<_>>()
        } else if key.starts_with("backfill-validation/") {
            let phase: ValidationCompletion = serde_json::from_str(json)?;
            phase.origins.into_iter().collect::<Vec<_>>()
        } else {
            let release: ReleasePin = serde_json::from_str(json)?;
            release
                .dataset
                .computed
                .into_values()
                .flat_map(|group| {
                    group
                        .origins
                        .into_iter()
                        .chain(group.qualification.map(|proof| proof.origins))
                })
                .collect::<Vec<_>>()
        };
        for anchor in retained {
            // Foreign dependencies are not creation facts at this owner.
            if &crate::catalog::lifecycle_ident(&anchor.owner) != view.identifier() {
                continue;
            }
            super::inventory::validate_object(&anchor)?;
            if let Some(previous) = anchors.get(&anchor.path) {
                if previous != &anchor {
                    return Err(invalid());
                }
                continue;
            }
            match available(rows, view, &anchor) {
                Ok(()) => (),
                Err(error) if error.code() == "object_unavailable" => continue,
                Err(error) => return Err(error),
            }
            identity_bytes = identity_bytes.saturating_add(
                anchor
                    .path
                    .len()
                    .saturating_mul(2)
                    .saturating_add(anchor.owner.as_str().len())
                    .saturating_add(anchor.digest.len())
                    .saturating_add(256),
            );
            if identity_bytes > limit {
                return Err(capacity());
            }
            anchors.insert(anchor.path.clone(), anchor);
        }
    }
    let mut metadata_bytes = 0usize;
    // A full requested closure names its own origins. Open those first, so an
    // unrelated old descriptor need not be read to qualify the requested files.
    for preferred_pass in [true, false] {
        for anchor in anchors.values().filter(|anchor| {
            preferred.is_some_and(|objects| objects.contains_key(anchor.path.as_str()))
                == preferred_pass
        }) {
            tokio::task::consume_budget().await;
            let size = usize::try_from(anchor.bytes).map_err(|_| capacity())?;
            if size > limit {
                return Err(capacity());
            }
            let memory = Arc::new(budget.reserve(size.saturating_mul(8).saturating_add(4096))?);
            let originals: ScalarOrigins =
                origins::read(anchor, budget, &mut metadata_bytes, &memory).await?;
            for file in &originals.files {
                // Only actual file creation outputs qualify. Source/schema and
                // assembled dependency references remain inputs, not creations.
                let completion = &file.completion;
                if let Some(execution) = &completion.execution
                    && execution.declaration.owner == anchor.owner
                    && observe(&execution.declaration)?
                {
                    return Ok(());
                }
                if completion.descriptor.owner == anchor.owner && observe(&completion.descriptor)? {
                    return Ok(());
                }
                for output in &completion.outputs {
                    if output.owner == anchor.owner && observe(output)? {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

async fn visit_creation(
    rows: &SnapshotRows,
    view: &View,
    limit: usize,
    mut observe: impl FnMut(&ObjectRef) -> Result<()>,
) -> Result<()> {
    for (key, revision, json) in rows.prefix("") {
        tokio::task::consume_budget().await;
        if json.len() > limit {
            return Err(capacity());
        }
        if revision <= 0 || revision > view.revision {
            return Err(invalid());
        }
        if key.starts_with("prepared-changes/") {
            let prepared: crate::engine::changes::PreparedChanges = serde_json::from_str(json)?;
            crate::engine::changes::check_prepared(&prepared, key, &prepared.before.dataset)?;
            if crate::catalog::lifecycle_ident(&prepared.before.dataset) != *view.identifier() {
                return Err(invalid());
            }
            observe(&prepared.descriptor)?;
        } else if key.starts_with("change-certificate/") {
            let certificate: ChangeCertificate = serde_json::from_str(json)?;
            crate::engine::changes::check_certificate(rows, revision, key, &certificate)?;
            if crate::catalog::lifecycle_ident(&certificate.after.dataset) != *view.identifier() {
                return Err(invalid());
            }
            observe(&certificate.descriptor)?;
        } else if key.starts_with("restore-materialized/") {
            let created: crate::restore::RestoreMaterialized = serde_json::from_str(json)?;
            observe(&created.reference.manifest)?;
            observe(&created.catalog)?;
            for object in &created.backups {
                observe(object)?;
            }
        } else if key.starts_with("export-host-closed/") {
            let created_key = key.replacen("export-host-closed/", "export-host-file/", 1);
            let (created_revision, _, file): (_, _, crate::engine::export::host::HostFile) =
                rows.get_record(&created_key)?.ok_or_else(invalid)?;
            crate::engine::export::host::validate(
                &file,
                &created_key,
                created_revision,
                rows,
                None,
            )?;
            let output: crate::engine::export::ExportMaterialized = serde_json::from_str(json)?;
            let (_, _, operation): (_, _, crate::engine::export::ExportOperation) = rows
                .get_record(&format!("export-operation/{}", file.operation))?
                .ok_or_else(invalid)?;
            crate::engine::export::check_materialized(&output, &operation)?;
            if output.object.path != file.path || revision <= created_revision {
                return Err(invalid());
            }
            observe(&output.object)?;
        } else if let Some(suffix) = key.strip_prefix("export-staged/") {
            crate::engine::export::remote::validate_record(key, json, rows, None)?;
            let (operation, _) = suffix.split_once('/').ok_or_else(invalid)?;
            if let Some(stage) =
                crate::engine::export::remote::completed_stage(rows, &Id::new(operation)?, None)?
            {
                if rows.revision(key) != Some(stage.stage_revision) {
                    return Err(invalid());
                }
                observe(&stage.object)?;
            }
        } else if let Some(id) = key.strip_prefix("export-materialized/") {
            let output: crate::engine::export::ExportMaterialized = serde_json::from_str(json)?;
            if output.object.path.starts_with("s3://") {
                let operation: crate::engine::export::ExportOperation = rows
                    .get_record(&format!("export-operation/{id}"))?
                    .ok_or_else(invalid)?
                    .2;
                let validation: crate::export::ExportValidation = rows
                    .get_record(&format!("export-validation/{id}"))?
                    .ok_or_else(invalid)?
                    .2;
                crate::engine::export::remote::check_completed(
                    &operation,
                    &output,
                    &validation,
                    rows,
                    None,
                )?;
            }
            observe(&output.object)?;
        } else if key.starts_with("compaction-prepared/") {
            let result: CompactionResult = serde_json::from_str(json)?;
            for object in &result.replacement.objects {
                observe(object)?;
            }
        } else if key.starts_with("phase/backfill/") {
            let phase: FileCompletion = serde_json::from_str(json)?;
            if let Some(execution) = &phase.execution {
                observe(&execution.declaration)?;
            }
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
            if let Some(origins) = &phase.origins {
                observe(origins)?;
            }
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
    Ok(())
}

pub(super) fn read_retained<T: serde::de::DeserializeOwned>(
    rows: &SnapshotRows,
    key: &str,
    limit: usize,
) -> Result<Option<(i64, T)>> {
    rows.prefix(key)
        .find(|(candidate, _, _)| *candidate == key)
        .map(|(_, revision, json)| {
            if json.len() > limit {
                return Err(capacity());
            }
            Ok((revision, serde_json::from_str(json)?))
        })
        .transpose()
}

fn unqualified() -> Error {
    Error::refusal(
        "unqualified_owner_reference",
        "object lacks retained typed creation evidence at its owner",
    )
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "owner references exceed the metadata allowance",
    )
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_owner_protection",
        "owner, immutable object identity or committed protection differs",
    )
}
