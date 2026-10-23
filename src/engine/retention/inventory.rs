//! Exact references and candidate protection from retained metadata, never payload scans.

use super::*;
#[cfg(test)]
use crate::engine::backfill::{
    FileCompletion, GroupDependencies, ReadbackCompletion,
    origins::{OriginalDeclaration, ScalarOrigins},
};
use crate::engine::changes::PreparedChanges;
use iceberg::{NamespaceIdent, TableIdent};

mod expand;
mod roots;
mod strict;

pub(in crate::engine) use strict::validate_object;
use strict::{Strict, capacity, decode, invalid_inventory, reference_bytes, unknown_dependencies};

#[cfg(test)]
mod collector_tests;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod legacy_fields_tests;
#[cfg(test)]
mod optional_changes_tests;
#[cfg(test)]
mod tests;

/// Exact references whose collection remains charged until the caller releases it.
#[derive(Debug)]
pub(in crate::engine) struct ReferenceObjects {
    pub(in crate::engine) objects: Vec<ObjectRef>,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

impl Engine {
    /// Collects exact dependencies of producer-anchored retained releases.
    ///
    /// The caller establishes each supplied release against its producer before
    /// calling. This only follows those roots; it neither enumerates the catalog
    /// nor qualifies the supplied roots. Complete retained table inventories avoid
    /// native rereads. Expanded native paths need identities elsewhere in this
    /// same metadata closure; missing owners or digests are never inferred.
    ///
    /// # Errors
    /// Refuses conflicting references, unsupported metadata, missing exact native
    /// identities, unavailable metadata, or exhausted shared/metadata capacity.
    pub(in crate::engine) async fn reference_objects(
        &self,
        releases: &[ReleasePin],
    ) -> Result<ReferenceObjects> {
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        if releases.is_empty() {
            return Ok(ReferenceObjects {
                objects: Vec::new(),
                _memory: budget.reserve(0)?,
            });
        }
        // Bound even a collection of tiny roots before traversal or external
        // decoding. The counting sink admits their complete serialized shape
        // without allocating another copy of these caller-owned pins.
        if releases.len().saturating_mul(size_of::<ReleasePin>()) > limit {
            return Err(capacity());
        }
        inputs::metadata_size(&releases, limit)?;
        // Bound identities and queue here; each external metadata decode admits
        // its known serialized size separately. Tiny roots must not reserve the
        // configured maximum for several decoders that are never used together.
        let mut memory = budget.reserve(limit)?;
        let mut inventory = Inventory::collecting(limit)?;
        for release in releases {
            tokio::task::consume_budget().await;
            // Traversal provenance only; collection returns no lifecycle witness.
            let origin = RetentionBlocker {
                table: release.dataset.dataset.to_string(),
                key: format!("explicit-release/{}", release.id),
                revision: 0,
            };
            inventory.release_pin(release, &origin)?;
        }
        inventory
            .expand(&self.inner.objects, budget, &self.inner.root)
            .await?;
        let objects = inventory.into_references()?;
        let bytes = objects.iter().fold(0usize, |bytes, object| {
            bytes.saturating_add(reference_bytes(object))
        });
        // Split the existing permit instead of briefly admitting another
        // allocation for the same returned objects.
        let retained = memory.split(bytes).ok_or_else(capacity)?;
        Ok(ReferenceObjects {
            objects,
            _memory: retained,
        })
    }

    /// Explains which retained records protect the supplied immutable objects.
    ///
    /// Visits every lifecycle namespace and expands exact release/native metadata
    /// once per request. Returns one protection witness per unique candidate.
    /// No files are removed, no promises are retired, and unmatched objects are
    /// not certified reclaimable. Ordinary reads never call this inspection.
    ///
    /// # Errors
    /// Refuses unknown root formats/dependencies, conflicting identities, missing
    /// metadata, incomplete catalog enumeration, or exhausted metadata capacity.
    pub async fn inspect_retention(&self, objects: &[ObjectRef]) -> Result<RetentionInspection> {
        let memory = self.request_memory(&objects)?;
        let objects = objects.to_vec();
        self.admit_work(memory, move |engine| async move {
            engine.inspect_retention_admitted(&objects).await
        })
        .await
    }

    pub(super) async fn inspect_retention_admitted(
        &self,
        objects: &[ObjectRef],
    ) -> Result<RetentionInspection> {
        self.retirement_catalog()?;
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        // One bounded graph allowance plus decoded lifecycle-row copies.
        // External metadata and native readers now admit separately in expand;
        // do not reserve that decoder's old maximum here as well.
        let _memory = budget.reserve(limit.saturating_mul(5))?;
        let mut inventory = Inventory::new(objects, limit)?;
        let boundaries = self.collect_inventory(&mut inventory, None).await?;
        let mut protected_bytes = 0u64;
        let mut unmatched_bytes = 0u64;
        let objects = inventory.candidates.into_values().collect::<Vec<_>>();
        for object in &objects {
            let total = if object.blocker.is_some() {
                &mut protected_bytes
            } else {
                &mut unmatched_bytes
            };
            *total = total
                .checked_add(object.object.bytes)
                .ok_or_else(capacity)?;
        }
        Ok(RetentionInspection {
            boundaries,
            objects,
            protected_bytes,
            unmatched_bytes,
            metadata_objects_read: inventory.metadata_objects_read,
        })
    }

    /// Captures the same active roots used by removal plus the native catalog's
    /// lifecycle tables. Called only while exclusive maintenance owns the host.
    pub(in crate::engine) async fn restore_dependencies(
        &self,
        owner: &Id,
    ) -> Result<(Vec<RetentionBoundary>, Vec<ObjectRef>)> {
        self.retirement_catalog()?;
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        let _memory = budget.reserve(limit.saturating_mul(5))?;
        let mut inventory = Inventory::collecting(limit)?;
        let boundaries = self.collect_inventory(&mut inventory, Some(owner)).await?;
        let paths = inventory.required_paths.take().unwrap_or_default();
        for path in paths {
            if inventory.candidates.contains_key(&path) {
                continue;
            }
            let object = if self.inner.retained.is_local() {
                let root = self.inner.root.clone();
                let owner = owner.clone();
                budget
                    .blocking(move || {
                        crate::restore::regular_owned_file(&root, Path::new(&path))?;
                        storage::object(&owner, Path::new(&path))
                    })
                    .await?
            } else {
                let owner = self.retained_owner(&path)?;
                self.inner.objects.identify(&owner, &path, budget).await?
            };
            inventory.charge(reference_bytes(&object).saturating_add(object.path.len()))?;
            inventory.candidates.insert(
                object.path.clone(),
                ObjectProtection {
                    object,
                    blocker: None,
                },
            );
        }
        Ok((boundaries, inventory.into_references()?))
    }

    pub(in crate::engine) async fn live_retained_closure(
        &self,
    ) -> Result<(BTreeSet<String>, BTreeSet<String>, BTreeSet<String>)> {
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        let _memory = budget.reserve(limit)?;
        let mut inventory = Inventory::collecting(limit)?;
        self.collect_inventory(&mut inventory, None).await?;
        let mut live_paths = inventory.required_paths.unwrap_or_default();
        live_paths.extend(inventory.candidates.into_keys());
        let mut live_native_metadata = BTreeSet::new();
        for (path, entry) in inventory.visited {
            if entry.kind == MetadataKind::Native {
                live_native_metadata.insert(path.clone());
            }
            live_paths.insert(path);
        }
        Ok((
            live_paths,
            live_native_metadata,
            inventory.protected_prefixes,
        ))
    }

    async fn collect_inventory(
        &self,
        inventory: &mut Inventory,
        restore_owner: Option<&Id>,
    ) -> Result<Vec<RetentionBoundary>> {
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        // Engine::open owns a flat embedded catalog. The maintained SQL adapter
        // scans all namespace rows per listing; recurse here and cost becomes
        // quadratic in dataset count. Injected/hierarchical catalogs need their
        // own qualified enumeration before this inventory can inspect them.
        let mut namespaces = lifecycle.catalog.list_namespaces(None).await?;
        charge_namespaces(inventory, &namespaces)?;
        namespaces.sort();
        let mut visited = BTreeSet::new();
        let mut boundaries = Vec::new();
        while let Some(namespace) = namespaces.pop() {
            tokio::task::consume_budget().await;
            inventory.charge(namespace.to_string().len().saturating_add(128))?;
            if !visited.insert(namespace.clone()) {
                return Err(invalid_inventory("catalog enumeration repeats a namespace"));
            }
            let ident = TableIdent::new(namespace, "lifecycle".into());
            if !lifecycle.catalog.table_exists(&ident).await? {
                return Err(invalid_inventory(
                    "catalog namespace lacks its lifecycle table",
                ));
            }
            let table = lifecycle.catalog.load_table(&ident).await?;
            let view = View {
                revision: table.metadata().last_sequence_number(),
                table,
                logical_identifier: ident.clone(),
            };
            let table_name = ident.to_string();
            let boundary = RetentionBoundary {
                table: table_name.clone(),
                metadata: view.table.metadata_location_result()?.to_owned(),
                revision: view.revision,
            };
            inventory.charge(boundary.table.len() + boundary.metadata.len() + 128)?;
            boundaries.push(boundary);
            if restore_owner.is_some() {
                let origin = RetentionBlocker {
                    table: table_name.clone(),
                    key: "native-catalog".into(),
                    revision: view.revision,
                };
                // The SQLite image includes every registration, even tables
                // whose logical promises have since retired. Retain their native
                // closure too, or restoring that image would create dangling heads.
                let tables = lifecycle.catalog.list_tables(ident.namespace()).await?;
                for native_ident in tables {
                    inventory.charge(native_ident.to_string().len().saturating_add(128))?;
                    let native = if native_ident == ident {
                        view.table.clone()
                    } else {
                        lifecycle.catalog.load_table(&native_ident).await?
                    };
                    if native_ident != ident {
                        let boundary = RetentionBoundary {
                            table: native_ident.to_string(),
                            metadata: native.metadata_location_result()?.to_owned(),
                            revision: native.metadata().last_sequence_number(),
                        };
                        inventory.charge(
                            boundary
                                .table
                                .len()
                                .saturating_add(boundary.metadata.len())
                                .saturating_add(128),
                        )?;
                        boundaries.push(boundary);
                    }
                    let files = self.inner.objects.snapshot_files(&native, budget).await?;
                    for path in &files.paths {
                        inventory.path(path, &origin)?;
                    }
                }
                // Missing identities are resolved after all typed roots were visited,
                // so retained payload identities never incur a second hash pass.
            }
            let rows = lifecycle.rows(&view).await?;
            let removal_markers = self.validate_removal_records(&rows, &view)?;
            if restore_owner.is_some() {
                for (_, _, json) in rows.prefix("removal-state/") {
                    let progress: RemovalProgress = decode(json)?;
                    if progress
                        .objects
                        .iter()
                        .chain(&progress.registrations)
                        .any(|outcome| !outcome.finished())
                    {
                        return Err(Error::refusal(
                            "restore_removal_pending",
                            "finish saved removal decisions before creating a restore point",
                        ));
                    }
                }
            }
            let retired = super::retirement::retired_records(&rows, &view, self, |bytes| {
                inventory.charge(bytes)
            })?;
            let retired_records = &retired.records;
            for (key, revision, json) in rows.prefix("") {
                tokio::task::consume_budget().await;
                if revision <= 0 || revision > view.revision {
                    return Err(invalid_inventory("root has an invalid native revision"));
                }
                let origin = RetentionBlocker {
                    table: table_name.clone(),
                    key: key.into(),
                    revision,
                };
                if key.starts_with("owner-protection/") {
                    let grant: super::owners::OwnerProtection = decode(json)?;
                    super::owners::validate_grant(&view, key, revision, &grant)?;
                    if retired_records.contains(&RetainedGeneration {
                        key: key.into(),
                        revision,
                    }) {
                        continue;
                    }
                    for object in &grant.objects {
                        super::owners::available(&rows, &view, object)?;
                        inventory.object(object, &origin)?;
                    }
                    continue;
                }
                if key.starts_with("reference-binding/") {
                    let binding: super::owners::ReferenceBinding = decode(json)?;
                    if super::owners::earlier_binding(&rows, &view, key, revision, &binding)? {
                        if !retired_records.contains(&RetainedGeneration {
                            key: binding.key,
                            revision,
                        }) {
                            return Err(invalid_inventory(
                                "superseded binding has no exact retirement",
                            ));
                        }
                        continue;
                    }
                    let required = super::owners::validate_binding(
                        &rows, &view, key, revision, &binding, limit,
                    )?;
                    if !retired_records.contains(&RetainedGeneration {
                        key: binding.key.clone(),
                        revision,
                    }) {
                        inventory.binding(binding, required)?;
                    }
                    continue;
                }
                if key.starts_with("object-unavailable/") {
                    let unavailable: super::owners::UnavailableObject = decode(json)?;
                    if !removal_markers.contains(key)
                        || key != super::owners::unavailable_key(&unavailable.object.path)
                        || unavailable.revision != revision
                        || &crate::catalog::lifecycle_ident(&unavailable.object.owner)
                            != view.identifier()
                    {
                        return Err(invalid_inventory("object unavailability identity differs"));
                    }
                    validate_object(&unavailable.object)?;
                    continue;
                }
                if key.starts_with("removal-intent/")
                    || key.starts_with("removal-state/")
                    || key.starts_with("retirement/")
                    || key.starts_with("availability-retired/")
                    || key.starts_with("retired/")
                {
                    continue;
                }
                if super::retirement::retained_key(key)
                    && retired_records.contains(&RetainedGeneration {
                        key: key.into(),
                        revision,
                    })
                {
                    continue;
                }
                inventory.record(key, json, &rows, &origin)?;
                inventory
                    .expand(&self.inner.objects, budget, &self.inner.root)
                    .await?;
            }
        }
        boundaries.sort_by(|left, right| left.table.cmp(&right.table));
        let lifecycle_boundaries = boundaries
            .iter()
            .filter(|boundary| boundary.table.ends_with(".lifecycle"))
            .cloned()
            .collect::<Vec<_>>();
        inventory
            .check_bindings(lifecycle, &lifecycle_boundaries)
            .await?;
        Ok(boundaries)
    }
}

/// Checks the mutable selection's exact immutable receipt without external reads.
/// The caller admits both decoded authority and result metadata.
pub(super) fn validate_selection(
    rows: &crate::catalog::SnapshotRows,
    authority: &Authority,
    authority_revision: i64,
    table: &str,
    limit: usize,
) -> Result<()> {
    if authority.selected.as_ref() != authority.release.as_ref().map(|release| &release.dataset) {
        return Err(invalid_inventory("selected dataset and release disagree"));
    }
    match (&authority.release, authority.selection_revision) {
        (None, None) => Ok(()),
        (Some(release), Some(selection_revision)) => {
            if selection_revision <= 0
                || selection_revision > authority_revision
                || !release.verified
                || release.id != release.dataset.version
                || release.manifest.owner != release.dataset.dataset
                || crate::catalog::lifecycle_ident(&release.dataset.dataset).to_string() != table
                || rows.contains_key(&super::availability::pointer_key(&release.id))
            {
                return Err(invalid_inventory(
                    "selected authority identity or availability differs",
                ));
            }
            let result_key = format!("result/{}", release.id);
            let (_, result_revision, result_json) = rows
                .prefix(&result_key)
                .find(|(key, _, _)| *key == result_key)
                .ok_or_else(|| invalid_inventory("selected result is missing"))?;
            if result_json.len() > limit {
                return Err(capacity());
            }
            let result: ReleasePin = decode(result_json)?;
            if result_revision != selection_revision || &result != release {
                return Err(invalid_inventory(
                    "selected authority differs from its result",
                ));
            }
            Ok(())
        }
        _ => Err(invalid_inventory("selected authority is incomplete")),
    }
}

fn charge_namespaces(inventory: &mut Inventory, namespaces: &[NamespaceIdent]) -> Result<()> {
    for namespace in namespaces {
        inventory.charge(namespace.to_string().len().saturating_add(128))?;
    }
    Ok(())
}

/// Collects only references declared directly by one retained typed claim.
///
/// The caller admits decoding/returned storage. Paths without exact identities
/// are intentionally excluded: request admission supplies qualified managed inputs
/// in its separate typed dependency record. No external metadata is opened here.
pub(super) fn reference_roots(
    rows: &crate::catalog::SnapshotRows,
    key: &str,
    json: &str,
    origin: &RetentionBlocker,
    limit: usize,
) -> Result<Vec<ObjectRef>> {
    if json.len() > limit {
        return Err(capacity());
    }
    let mut inventory = Inventory::collecting(limit)?;
    inventory.shallow = true;
    inventory.record(key, json, rows, origin)?;
    inventory.into_references()
}

pub(super) enum Expansion {
    Origins,
    Declaration,
    Release { payloads: bool },
    Changes(PreparedChanges),
    Restore(crate::restore::RestorePointRef),
    Native(Option<(Option<i64>, i32)>),
}

pub(super) struct Pending {
    pub(super) object: ObjectRef,
    pub(super) kind: Expansion,
    pub(super) origin: RetentionBlocker,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataKind {
    Origins,
    Declaration,
    Release,
    Changes,
    Restore,
    Native,
}

pub(super) struct VisitedMetadata {
    pub(super) object: ObjectRef,
    pub(super) kind: MetadataKind,
    pub(super) binding: Option<(Option<i64>, i32)>,
    pub(super) change_binding: Option<blake3::Hash>,
    pub(super) complete: bool,
    pub(super) release_payloads: bool,
}

struct GrantClaim {
    reference: super::owners::OwnerProtectionRef,
    consumer: Id,
    bindings: Vec<usize>,
}

struct BindingClaim {
    required: BTreeMap<String, (ObjectRef, bool)>,
}

#[derive(Deserialize)]
pub(super) struct ProgressResult {
    pub(super) id: Id,
    pub(super) dataset: ProgressDataset,
    pub(super) manifest: ObjectRef,
    pub(super) verified: bool,
}

#[derive(Deserialize)]
pub(super) struct ProgressDataset {
    pub(super) dataset: Id,
    pub(super) version: Id,
    pub(super) related: Option<ProgressRelated>,
}

#[derive(Deserialize)]
pub(super) struct ProgressRelated {
    pub(super) coverage: ProgressCoverage,
}

#[derive(Deserialize)]
pub(super) struct ProgressCoverage {
    pub(super) inputs: BTreeMap<Id, VersionRef>,
}

pub(super) struct Inventory {
    pub(super) candidates: BTreeMap<String, ObjectProtection>,
    pub(super) required_paths: Option<BTreeSet<String>>,
    pub(super) protected_prefixes: BTreeSet<String>,
    pub(super) shallow: bool,
    pub(super) visited: BTreeMap<String, VisitedMetadata>,
    pub(super) pending: Vec<Pending>,
    pub(super) pending_native: Vec<Pending>,
    claims: BTreeMap<Id, BTreeMap<String, GrantClaim>>,
    bindings: Vec<BindingClaim>,
    binding_scopes: BTreeMap<Id, BTreeMap<Vec<super::owners::OwnerProtectionRef>, usize>>,
    bytes: usize,
    pub(super) limit: usize,
    pub(super) metadata_bytes: usize,
    pub(super) metadata_objects_read: usize,
}

impl Inventory {
    fn new(objects: &[ObjectRef], limit: usize) -> Result<Self> {
        let mut result = Self {
            candidates: BTreeMap::new(),
            required_paths: None,
            protected_prefixes: BTreeSet::new(),
            shallow: false,
            visited: BTreeMap::new(),
            pending: Vec::new(),
            pending_native: Vec::new(),
            claims: BTreeMap::new(),
            bindings: Vec::new(),
            binding_scopes: BTreeMap::new(),
            bytes: 0,
            limit,
            metadata_bytes: 0,
            metadata_objects_read: 0,
        };
        for object in objects {
            validate_object(object)?;
            if let Some(old) = result.candidates.get(&object.path) {
                if old.object != *object {
                    return Err(invalid_inventory(
                        "candidate path has conflicting identities",
                    ));
                }
                continue;
            }
            result.charge(reference_bytes(object).saturating_add(object.path.len()))?;
            result.candidates.insert(
                object.path.clone(),
                ObjectProtection {
                    object: object.clone(),
                    blocker: None,
                },
            );
        }
        Ok(result)
    }

    fn collecting(limit: usize) -> Result<Self> {
        let mut result = Self::new(&[], limit)?;
        result.required_paths = Some(BTreeSet::new());
        Ok(result)
    }

    fn binding(
        &mut self,
        binding: super::owners::ReferenceBinding,
        required: Vec<ObjectRef>,
    ) -> Result<()> {
        let existing = self
            .binding_scopes
            .get(&binding.consumer)
            .and_then(|scopes| scopes.get(&binding.protections))
            .copied();
        let index = if let Some(index) = existing {
            index
        } else {
            let bytes =
                inputs::metadata_size(&(&binding.consumer, &binding.protections), self.limit)?;
            self.charge(bytes.saturating_mul(4).saturating_add(512))?;
            let index = self.bindings.len();
            self.bindings.push(BindingClaim {
                required: BTreeMap::new(),
            });
            self.binding_scopes
                .entry(binding.consumer.clone())
                .or_default()
                .insert(binding.protections.clone(), index);
            index
        };
        for object in required {
            if let Some((previous, _)) = self.bindings[index].required.get(&object.path) {
                if previous != &object {
                    return Err(invalid_inventory("owner claim identity differs"));
                }
            } else {
                self.charge(reference_bytes(&object).saturating_add(object.path.len()))?;
                self.bindings[index]
                    .required
                    .insert(object.path.clone(), (object, false));
            }
        }
        if existing.is_some() {
            return Ok(());
        }
        for reference in binding.protections {
            if self
                .claims
                .get(&reference.owner)
                .is_none_or(|claims| !claims.contains_key(&reference.token))
            {
                let bytes = inputs::metadata_size(&(&reference, &binding.consumer), self.limit)?;
                self.charge(bytes.saturating_mul(4).saturating_add(512))?;
            }
            self.charge(32)?;
            let claim = self
                .claims
                .entry(reference.owner.clone())
                .or_default()
                .entry(reference.token.clone())
                .or_insert_with(|| GrantClaim {
                    reference: reference.clone(),
                    consumer: binding.consumer.clone(),
                    bindings: Vec::new(),
                });
            if claim.reference != reference || claim.consumer != binding.consumer {
                return Err(invalid_inventory(
                    "owner token has conflicting consumer bindings",
                ));
            }
            claim.bindings.push(index);
        }
        Ok(())
    }

    async fn check_bindings(
        &mut self,
        lifecycle: &crate::catalog::Lifecycle,
        boundaries: &[RetentionBoundary],
    ) -> Result<()> {
        for (owner, claims) in &self.claims {
            let ident = crate::catalog::lifecycle_ident(owner);
            let name = ident.to_string();
            let index = boundaries
                .binary_search_by(|boundary| boundary.table.cmp(&name))
                .map_err(|_| invalid_inventory("consumer refers to an unknown owner"))?;
            let table = lifecycle.catalog.load_table(&ident).await?;
            if table.metadata_location_result()? != boundaries[index].metadata {
                return Err(Error::refusal(
                    "stale_authority",
                    "owner changed during protection inspection; retry the inspection",
                ));
            }
            let view = View {
                revision: table.metadata().last_sequence_number(),
                table,
                logical_identifier: ident.clone(),
            };
            let rows = lifecycle.rows(&view).await?;
            for (token, claim) in claims {
                tokio::task::consume_budget().await;
                let (revision, grant) = super::owners::read_retained::<
                    super::owners::OwnerProtection,
                >(&rows, token, self.limit)?
                .ok_or_else(|| invalid_inventory("consumer refers to a missing owner token"))?;
                super::owners::validate_grant(&view, token, revision, &grant)?;
                super::owners::validate_reference(&claim.reference, &grant, &claim.consumer)?;
                super::owners::check_grant_active(
                    &rows,
                    &view,
                    token,
                    revision,
                    &lifecycle.budget,
                )?;
                for index in &claim.bindings {
                    super::owners::cover_required(
                        self.bindings[*index].required.values_mut(),
                        &grant,
                    )?;
                }
            }
        }
        for binding in &self.bindings {
            super::owners::check_covered(binding.required.values())?;
        }
        Ok(())
    }

    fn into_references(self) -> Result<Vec<ObjectRef>> {
        if self
            .required_paths
            .as_ref()
            .is_some_and(|paths| paths.iter().any(|path| !self.candidates.contains_key(path)))
        {
            return Err(Error::refusal(
                "retention_unknown_dependencies",
                "native dependency lacks an exact retained object identity",
            ));
        }
        Ok(self
            .candidates
            .into_values()
            .map(|candidate| candidate.object)
            .collect())
    }

    pub(super) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > self.limit {
            return Err(capacity());
        }
        Ok(())
    }

    pub(super) fn path(&mut self, path: &str, origin: &RetentionBlocker) -> Result<()> {
        if self.shallow {
            return Ok(());
        }
        if let Some(paths) = &self.required_paths {
            if !self.candidates.contains_key(path) && !paths.contains(path) {
                self.charge(path.len().saturating_add(128))?;
                self.required_paths
                    .as_mut()
                    .expect("collection mode was checked")
                    .insert(path.to_owned());
            }
            return Ok(());
        }
        if self
            .candidates
            .get(path)
            .is_some_and(|object| object.blocker.is_none())
        {
            self.charge(origin.table.len() + origin.key.len() + 128)?;
            self.candidates
                .get_mut(path)
                .expect("candidate was just found")
                .blocker = Some(origin.clone());
        }
        Ok(())
    }

    pub(super) fn object(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        validate_object(object)?;
        if let Some(candidate) = self.candidates.get(&object.path)
            && candidate.object != *object
        {
            return Err(invalid_inventory(
                "retained reference conflicts with candidate identity",
            ));
        }
        if self.required_paths.is_some() && !self.candidates.contains_key(&object.path) {
            self.charge(reference_bytes(object).saturating_add(object.path.len()))?;
            self.candidates.insert(
                object.path.clone(),
                ObjectProtection {
                    object: object.clone(),
                    blocker: None,
                },
            );
        }
        self.path(&object.path, origin)
    }
}
