//! Exact references and candidate protection from retained metadata, never payload scans.

use super::*;
use crate::engine::backfill::{
    Capture, FileCompletion, GroupDependencies, PreparedCompletion, ValidationCompletion,
    origins::{OriginalDeclaration, ScalarOrigins},
};
use crate::engine::changes::PreparedChanges;
use iceberg::{NamespaceIdent, TableIdent, spec::TableMetadata, table::Table};
use serde::de::DeserializeOwned;

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
        inventory.expand(budget, &self.inner.root).await?;
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
        if !self.inner.owned_catalog {
            return Err(Error::refusal(
                "retention_catalog_unqualified",
                "protection inspection requires the owned embedded catalog",
            ));
        }
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
            let root = self.inner.root.clone();
            let owner = owner.clone();
            let object = budget
                .blocking(move || {
                    crate::restore::regular_owned_file(&root, Path::new(&path))?;
                    storage::object(&owner, Path::new(&path))
                })
                .await?;
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
                    let files = storage::snapshot_files(&native, budget).await?;
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
                            != view.table.identifier()
                    {
                        return Err(invalid_inventory("object unavailability identity differs"));
                    }
                    validate_object(&unavailable.object)?;
                    continue;
                }
                if key.starts_with("removal-intent/") || key.starts_with("removal-state/") {
                    continue;
                }
                if key.starts_with("retirement/") {
                    continue;
                }
                if key.starts_with("availability-retired/") {
                    continue;
                }
                if key.starts_with("retired/") {
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
                inventory.expand(budget, &self.inner.root).await?;
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
    // The maintained catalog returns a Vec. This bounds admitted application
    // state after return, not that adapter's peak allocation during enumeration.
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

enum Expansion {
    Origins,
    Declaration,
    Release,
    Changes(PreparedChanges),
    Restore(crate::restore::RestorePointRef),
    Native(Option<(Option<i64>, i32)>),
}

struct Pending {
    object: ObjectRef,
    kind: Expansion,
    origin: RetentionBlocker,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataKind {
    Origins,
    Declaration,
    Release,
    Changes,
    Restore,
    Native,
}

struct VisitedMetadata {
    object: ObjectRef,
    kind: MetadataKind,
    binding: Option<(Option<i64>, i32)>,
    change_binding: Option<blake3::Hash>,
    complete: bool,
}

struct GrantClaim {
    reference: super::owners::OwnerProtectionRef,
    consumer: Id,
    bindings: Vec<usize>,
}

struct BindingClaim {
    required: BTreeMap<String, (ObjectRef, bool)>,
}

// Lag checks need identity and captured inputs, not another copy of the result's
// file inventories, recipes, or evidence tables. Decode only those fields.
#[derive(Deserialize)]
struct ProgressResult {
    id: Id,
    dataset: ProgressDataset,
    manifest: ObjectRef,
    verified: bool,
}

#[derive(Deserialize)]
struct ProgressDataset {
    dataset: Id,
    version: Id,
    related: Option<ProgressRelated>,
}

#[derive(Deserialize)]
struct ProgressRelated {
    coverage: ProgressCoverage,
}

#[derive(Deserialize)]
struct ProgressCoverage {
    inputs: BTreeMap<Id, VersionRef>,
}

struct Inventory {
    candidates: BTreeMap<String, ObjectProtection>,
    // Some selects explicit-root collection. Native files only provide paths;
    // these must resolve to retained exact identities after the whole closure is
    // visited, independent of expansion order. None preserves candidate inspection.
    required_paths: Option<BTreeSet<String>>,
    shallow: bool,
    // Metadata identity and interpretation are retained; payload inventories are
    // consumed one at a time. A second use still checks its exact declaration.
    visited: BTreeMap<String, VisitedMetadata>,
    pending: Vec<Pending>,
    // Collection expands releases first so their complete inventories can satisfy
    // compact native roots without reading the same native metadata again.
    pending_native: Vec<Pending>,
    // Group small consumer requirements by owner/token. A second owner pass
    // checks each grant once without retaining every owner's full grant payloads.
    claims: BTreeMap<Id, BTreeMap<String, GrantClaim>>,
    bindings: Vec<BindingClaim>,
    // Only the exact same consumer/token set may share coverage obligations.
    // Claim key/revision/digest have already been checked against their own rows.
    binding_scopes: BTreeMap<Id, BTreeMap<Vec<super::owners::OwnerProtectionRef>, usize>>,
    bytes: usize,
    limit: usize,
    metadata_bytes: usize,
    metadata_objects_read: usize,
}

impl Inventory {
    fn new(objects: &[ObjectRef], limit: usize) -> Result<Self> {
        let mut result = Self {
            candidates: BTreeMap::new(),
            required_paths: None,
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
            // Decode each token once. Only scopes actually naming that token
            // can use it; grants may jointly cover a scope's required objects.
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

    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > self.limit {
            return Err(capacity());
        }
        Ok(())
    }

    fn path(&mut self, path: &str, origin: &RetentionBlocker) -> Result<()> {
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

    fn object(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
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

    fn enqueue(
        &mut self,
        object: &ObjectRef,
        kind: Expansion,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        self.object(object, origin)?;
        let (tag, binding) = match &kind {
            Expansion::Origins => (MetadataKind::Origins, None),
            Expansion::Declaration => (MetadataKind::Declaration, None),
            Expansion::Release => (MetadataKind::Release, None),
            Expansion::Changes(_) => (MetadataKind::Changes, None),
            Expansion::Restore(_) => (MetadataKind::Restore, None),
            Expansion::Native(binding) => (MetadataKind::Native, *binding),
        };
        let change_binding = match &kind {
            Expansion::Changes(prepared) => Some(storage::json_digest(prepared)?),
            _ => None,
        };
        if let Some(old) = self.visited.get_mut(&object.path) {
            if old.object != *object
                || old.kind != tag
                || old.change_binding != change_binding
                || (binding.is_some() && old.binding.is_some() && old.binding != binding)
            {
                return Err(invalid_inventory(
                    "metadata has conflicting identities or interpretations",
                ));
            }
            if old.binding.is_none() {
                old.binding = binding;
            }
            return Ok(());
        }
        // Charge queue plus retained identity once, before allocation. Charges
        // stay for the request, so repeated traversal cannot grow unbounded state.
        self.charge(
            reference_bytes(object)
                .saturating_mul(2)
                .saturating_add(origin.table.len() + origin.key.len() + 256),
        )?;
        if let Expansion::Changes(prepared) = &kind {
            // The queued descriptor binding is bounded metadata, independent
            // of the change tables' potentially large file inventories.
            self.charge(inputs::metadata_size(prepared, self.limit)?.saturating_mul(2))?;
        }
        self.visited.insert(
            object.path.clone(),
            VisitedMetadata {
                object: object.clone(),
                kind: tag,
                binding,
                change_binding,
                complete: false,
            },
        );
        if self.shallow {
            return Ok(());
        }
        let pending = Pending {
            object: object.clone(),
            kind,
            origin: origin.clone(),
        };
        if self.required_paths.is_some() && tag == MetadataKind::Native {
            self.pending_native.push(pending);
        } else {
            self.pending.push(pending);
        }
        Ok(())
    }

    fn release(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(object, Expansion::Release, origin)
    }

    fn snapshot(&mut self, snapshot: &NativeSnapshotRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(
            &snapshot.metadata,
            Expansion::Native(Some((snapshot.snapshot, snapshot.schema_id))),
            origin,
        )
    }

    fn native(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(object, Expansion::Native(None), origin)
    }

    fn table(&mut self, table: &TablePin, origin: &RetentionBlocker) -> Result<()> {
        self.object(&table.metadata, origin)?;
        if let Some(logical) = &table.logical_metadata {
            self.object(logical, origin)?;
        }
        // Pins created by supported writers already retain the native snapshot
        // inventory. No need to read those manifests again for direct table roots.
        if !table.objects.contains(&table.metadata) {
            return Err(invalid_inventory(
                "table inventory omits its native metadata",
            ));
        }
        for object in &table.objects {
            self.object(object, origin)?;
        }
        self.snapshot(&table.into(), origin)?;
        self.visited
            .get_mut(&table.metadata.path)
            .expect("snapshot was registered")
            .complete = true;
        Ok(())
    }

    fn coverage(&mut self, coverage: &InputCoverage, origin: &RetentionBlocker) -> Result<()> {
        for version in coverage.inputs.values() {
            self.release(&version.manifest, origin)?;
        }
        for seal in coverage.generations.values() {
            self.seal(seal, origin)?;
        }
        Ok(())
    }

    fn seal(&mut self, seal: &GenerationSeal, origin: &RetentionBlocker) -> Result<()> {
        for version in seal.members.values() {
            self.release(&version.manifest, origin)?;
        }
        Ok(())
    }

    fn computed(&mut self, computed: &ComputedGroup, origin: &RetentionBlocker) -> Result<()> {
        self.object(&computed.source_metadata, origin)?;
        if let Some(origins) = &computed.origins {
            self.enqueue(origins, Expansion::Origins, origin)?;
        }
        if let Some(qualification) = &computed.qualification {
            self.qualification(qualification, origin)?;
        }
        self.release(&computed.base_manifest, origin)
    }

    fn qualification(
        &mut self,
        qualification: &DependencyQualification,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        crate::engine::corrections::validate_qualification(qualification)?;
        self.enqueue(&qualification.origins, Expansion::Origins, origin)?;
        self.snapshot(&qualification.target, origin)
    }

    fn file_completion(&mut self, phase: &FileCompletion, origin: &RetentionBlocker) -> Result<()> {
        self.object(&phase.source, origin)?;
        // This metadata records one file's schema interpretation. Its old table
        // snapshot may contain unrelated files, so retain only the metadata.
        self.object(
            phase
                .source_metadata
                .as_ref()
                .ok_or_else(unknown_dependencies)?,
            origin,
        )?;
        self.object(&phase.descriptor, origin)?;
        for object in &phase.outputs {
            self.object(object, origin)?;
        }
        if let Some(execution) = &phase.execution {
            self.enqueue(&execution.declaration, Expansion::Declaration, origin)?;
        }
        Ok(())
    }

    fn group_dependencies(
        &mut self,
        dependencies: &GroupDependencies,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        self.snapshot(&dependencies.source, origin)?;
        if let Some(logical) = &dependencies.source_logical_metadata {
            self.object(logical, origin)?;
        }
        self.snapshot(&dependencies.output, origin)
    }

    fn related(&mut self, related: &RelatedResult, origin: &RetentionBlocker) -> Result<()> {
        self.coverage(&related.coverage, origin)?;
        self.table(&related.evidence, origin)
    }

    fn changes(&mut self, changes: &ChangePin, origin: &RetentionBlocker) -> Result<()> {
        self.release(&changes.base_manifest, origin)?;
        self.native(&changes.before_metadata, origin)?;
        self.native(&changes.after_metadata, origin)?;
        self.table(&changes.removed, origin)?;
        self.table(&changes.added, origin)
    }

    fn optional_changes(&mut self, changes: &ChangePin, origin: &RetentionBlocker) -> Result<()> {
        // These metadata objects interpret the applied rows. The whole endpoint
        // snapshots and result manifests have independent availability promises.
        self.object(&changes.before_metadata, origin)?;
        self.object(&changes.after_metadata, origin)?;
        self.table(&changes.removed, origin)?;
        self.table(&changes.added, origin)
    }

    fn prepared_changes(
        &mut self,
        prepared: PreparedChanges,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        let descriptor = prepared.descriptor.clone();
        self.enqueue(&descriptor, Expansion::Changes(prepared), origin)
    }

    fn dataset(&mut self, dataset: &DatasetPin, origin: &RetentionBlocker) -> Result<()> {
        if !supported_format(dataset.format) {
            return Err(invalid_inventory("unsupported release format"));
        }
        for table in dataset.groups.values() {
            self.table(table, origin)?;
        }
        for computed in dataset.computed.values() {
            self.computed(computed, origin)?;
        }
        for input in dataset.inputs.values() {
            self.object(input, origin)?;
        }
        if let Some(related) = &dataset.related {
            self.related(related, origin)?;
        }
        if let Some(evidence) = &dataset.evidence {
            self.snapshot(&evidence.source, origin)?;
            self.table(&evidence.table, origin)?;
        }
        if let Some(parent) = &dataset.parent_manifest {
            self.release(parent, origin)?;
        }
        Ok(())
    }

    fn release_pin(&mut self, release: &ReleasePin, origin: &RetentionBlocker) -> Result<()> {
        if release.id != release.dataset.version
            || release.manifest.owner != release.dataset.dataset
        {
            return Err(invalid_inventory(
                "release identity differs from its selection",
            ));
        }
        self.release(&release.manifest, origin)?;
        self.dataset(&release.dataset, origin)?;
        self.visited
            .get_mut(&release.manifest.path)
            .expect("release was registered")
            .complete = true;
        Ok(())
    }

    fn record(
        &mut self,
        key: &str,
        json: &str,
        rows: &crate::catalog::SnapshotRows,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        // Most-specific families precede their common prefix.
        if key == "source-fence" {
            let fence: crate::engine::intake::SourceFence = decode(json)?;
            crate::engine::intake::validate_record(&fence)?;
            if crate::catalog::lifecycle_ident(&fence.dataset).to_string() != origin.table {
                return Err(invalid_inventory("source fence dataset differs"));
            }
        } else if key.starts_with("prepared-changes/") {
            let prepared: PreparedChanges = decode(json)?;
            crate::engine::changes::check_prepared(&prepared, key, &prepared.before.dataset)?;
            if crate::catalog::lifecycle_ident(&prepared.before.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("prepared change dataset differs"));
            }
            self.prepared_changes(prepared, origin)?;
        } else if key.starts_with("change-certificate/") {
            let certificate: ChangeCertificate = decode(json)?;
            crate::engine::changes::check_certificate(rows, origin.revision, key, &certificate)?;
            if crate::catalog::lifecycle_ident(&certificate.after.dataset).to_string()
                != origin.table
            {
                return Err(invalid_inventory("change certificate dataset differs"));
            }
            self.prepared_changes(PreparedChanges::from(&certificate), origin)?;
        } else if let Some(id) = key.strip_prefix("dependency-correction/") {
            let decision: CorrectionDecision = decode(json)?;
            crate::engine::corrections::records::validate(&decision.request)?;
            if decision.request.id.as_str() != id
                || decision.processor != crate::engine::backfill::PROCESSOR
                || decision.revision != origin.revision
                || decision.revision <= 0
                || crate::catalog::lifecycle_ident(&decision.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("dependency correction identity differs"));
            }
        } else if key.starts_with("correction-requirements/") {
            let requirements: crate::engine::corrections::records::Requirements = decode(json)?;
            let decision_key = format!("dependency-correction/{}", requirements.latest_decision);
            let (_, decision_revision, decision_json) = rows
                .prefix(&decision_key)
                .find(|(key, _, _)| *key == decision_key)
                .ok_or_else(unknown_dependencies)?;
            if decision_json.len() > self.limit {
                return Err(capacity());
            }
            let decision: CorrectionDecision = decode(decision_json)?;
            crate::engine::corrections::records::validate(&decision.request)?;
            if key != crate::engine::corrections::records::scope_key(&requirements.definition)?
                || requirements.processor != crate::engine::backfill::PROCESSOR
                || requirements.revision != origin.revision
                || decision_revision != requirements.revision
                || requirements.first_revision <= 0
                || requirements.first_revision > requirements.revision
                || requirements.fields.is_empty()
                || requirements.fields.iter().any(|field| *field <= 0)
                || decision.processor != requirements.processor
                || decision.request.definition != requirements.definition
                || decision.request.id != requirements.latest_decision
                || decision.revision != requirements.revision
                || !decision.request.fields.is_subset(&requirements.fields)
                || crate::catalog::lifecycle_ident(&decision.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory(
                    "dependency requirements identity differs",
                ));
            }
        } else if key.starts_with("dependency-qualification/") {
            let qualification: DependencyQualification = decode(json)?;
            if key != crate::engine::corrections::qualification_key(&qualification)?
                || crate::catalog::lifecycle_ident(&qualification.origins.owner).to_string()
                    != origin.table
                || qualification.first_revision > origin.revision
            {
                return Err(invalid_inventory(
                    "dependency qualification identity differs",
                ));
            }
            self.qualification(&qualification, origin)?;
        } else if let Some(id) = key.strip_prefix("restore-reuse/") {
            let reuse: crate::restore::RestoreReuse = decode(json)?;
            if reuse.id.as_str() != id
                || crate::catalog::lifecycle_ident(&reuse.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("restore preparation identity differs"));
            }
            for object in &reuse.objects {
                self.object(object, origin)?;
            }
        } else if let Some(id) = key.strip_prefix("restore-point/") {
            let reference: crate::restore::RestorePointRef = decode(json)?;
            if reference.id.as_str() != id
                || reference.manifest.owner != reference.dataset
                || crate::catalog::lifecycle_ident(&reference.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("restore point identity differs"));
            }
            self.enqueue(
                &reference.manifest,
                Expansion::Restore(reference.clone()),
                origin,
            )?;
        } else if let Some(id) = key.strip_prefix("restore-materialized/") {
            let created: crate::restore::RestoreMaterialized = decode(json)?;
            if created.reference.id.as_str() != id
                || created.reference.manifest.owner != created.reference.dataset
                || created.catalog.owner != created.reference.dataset
                || created
                    .backups
                    .iter()
                    .any(|object| object.owner != created.reference.dataset)
                || crate::catalog::lifecycle_ident(&created.reference.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory(
                    "restore creation evidence identity differs",
                ));
            }
            // Creation evidence is retained after explicit retirement. Only the
            // active point promises continued availability of its backup objects.
            for object in std::iter::once(&created.reference.manifest)
                .chain(std::iter::once(&created.catalog))
                .chain(&created.backups)
            {
                validate_object(object)?;
            }
        } else if let Some(id) = key.strip_prefix("compaction-request/") {
            let capture: crate::engine::compaction::Capture = decode(json)?;
            if capture.spec.operation.as_str() != id
                || capture.manifest.owner != capture.spec.dataset
                || crate::catalog::lifecycle_ident(&capture.spec.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("compaction request identity differs"));
            }
            self.release(&capture.manifest, origin)?;
            self.table(&capture.source, origin)?;
        } else if [
            "compaction-prepared/",
            "compaction-verified/",
            "compaction-result/",
            "representation/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
        {
            let result: CompactionResult = decode(json)?;
            let capture: crate::engine::compaction::Capture = required_record(
                rows,
                &format!("compaction-request/{}", result.spec.operation),
            )?;
            crate::engine::compaction::check_result(&capture, &result)?;
            let expected = if key.starts_with("representation/") {
                format!(
                    "representation/{}/{}",
                    result.source.logical_metadata().digest,
                    result.spec.group
                )
            } else {
                format!(
                    "{}/{}",
                    key.split('/').next().unwrap_or_default(),
                    result.spec.operation
                )
            };
            if key != expected
                || (!key.starts_with("compaction-prepared/") && !result.verified)
                || crate::catalog::lifecycle_ident(&result.spec.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("compaction completion identity differs"));
            }
            // Selection promises an equivalent current layout, not recovery of
            // the rewrite that produced it. Recovery records root both sides.
            // The replacement retains logical metadata without its old payload.
            if !key.starts_with("representation/") {
                self.table(&result.source, origin)?;
            }
            self.table(&result.replacement, origin)?;
        } else if let Some(id) = key.strip_prefix("export-operation/") {
            let operation: crate::engine::export::ExportOperation = decode(json)?;
            crate::engine::export::validate_operation_record(&operation)?;
            if operation.request.operation.as_str() != id
                || crate::catalog::lifecycle_ident(&operation.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("export operation identity differs"));
            }
            self.release_pin(&operation.source, origin)?;
        } else if let Some(id) = key.strip_prefix("export-materialized/") {
            let output: crate::engine::export::ExportMaterialized = decode(json)?;
            let operation: crate::engine::export::ExportOperation =
                required_record(rows, &format!("export-operation/{id}"))?;
            crate::engine::export::validate_operation_record(&operation)?;
            crate::engine::export::check_materialized(&output, &operation)?;
            if operation.request.operation.as_str() != id
                || crate::catalog::lifecycle_ident(&operation.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("export creation identity differs"));
            }
            self.object(&output.object, origin)?;
        } else if let Some(id) = key.strip_prefix("export-validation/") {
            let validation: crate::export::ExportValidation = decode(json)?;
            let output: crate::engine::export::ExportMaterialized =
                required_record(rows, &format!("export-materialized/{id}"))?;
            if validation.object != output.object || validation.rows != output.rows {
                return Err(invalid_inventory(
                    "export validation differs from its output",
                ));
            }
            self.object(&validation.object, origin)?;
        } else if key.starts_with("export-result/") || key.starts_with("export-channel/") {
            let pin: crate::export::ExportPin = decode(json)?;
            crate::engine::export::check_pin(&pin)?;
            let operation: crate::engine::export::ExportOperation =
                required_record(rows, &format!("export-operation/{}", pin.id))?;
            crate::engine::export::check_result(&pin, &operation)?;
            let expected = if key.starts_with("export-result/") {
                format!("export-result/{}", pin.id)
            } else {
                format!("export-channel/{}", operation.request.channel)
            };
            if key != expected
                || crate::catalog::lifecycle_ident(&pin.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("export selection identity differs"));
            }
            self.object(&pin.object, origin)?;
            self.release(&pin.source.manifest, origin)?;
        } else if key.starts_with("phase/backfill/") {
            let phase: FileCompletion = decode(json)?;
            self.file_completion(&phase, origin)?;
        } else if key.starts_with("phase/") {
            let phase: PhaseResult = decode(json)?;
            if !matches!(
                phase.phase.as_str(),
                "admission" | "processing" | "validation" | "evidence" | "receipts"
            ) {
                return Err(invalid_inventory("unsupported completed phase kind"));
            }
            for object in &phase.outputs {
                self.object(object, origin)?;
            }
            if let Some(table) = &phase.table {
                self.table(table, origin)?;
            }
            if let Some(changes) = &phase.changes {
                self.changes(changes, origin)?;
            }
            if let Some(coverage) = &phase.coverage {
                self.coverage(coverage, origin)?;
            }
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            for object in dependencies.admitted_inputs.values() {
                self.object(object, origin)?;
            }
            if let Some(base) = &dependencies.base_manifest {
                self.release(base, origin)?;
            }
            for schema in &dependencies.schema_metadata {
                self.object(schema, origin)?;
            }
            for table in &dependencies.tables {
                self.snapshot(table, origin)?;
            }
        } else if key.starts_with("backfill-prepared/") {
            let phase: PreparedCompletion = decode(json)?;
            self.table(&phase.table, origin)?;
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            self.group_dependencies(dependencies, origin)?;
            if let Some(origins) = &phase.origins {
                self.enqueue(origins, Expansion::Origins, origin)?;
            }
        } else if key.starts_with("backfill-validation/") {
            let phase: ValidationCompletion = decode(json)?;
            self.object(&phase.output, origin)?;
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            self.group_dependencies(dependencies, origin)?;
            if let Some(origins) = &phase.origins {
                self.enqueue(origins, Expansion::Origins, origin)?;
            }
        } else if key.starts_with("backfill/") {
            let capture: Capture = decode(json)?;
            self.release(&capture.base_manifest, origin)?;
            if let Some(target) = &capture.target {
                self.native(target, origin)?;
            }
            if let Some(binding) = &capture.target_binding {
                self.computed(binding, origin)?;
            }
            if let Some(related) = &capture.related {
                self.related(related, origin)?;
            }
        } else if key.starts_with("result/")
            || key.starts_with("readback/")
            || key.starts_with("backfill-readback/")
        {
            self.release_pin(&decode(json)?, origin)?;
        } else if key == "authority" {
            let authority: Authority = decode(json)?;
            validate_selection(rows, &authority, origin.revision, &origin.table, self.limit)?;
            if let Some(release) = &authority.release {
                // Authority adds no dependency to its exact immutable result.
                // Its binding is inspected once through that result record.
                self.release_pin(release, origin)?;
            }
        } else if key.starts_with("operation/") {
            let operation: Operation = decode(json)?;
            if !supported_format(operation.result_format) {
                return Err(unknown_dependencies());
            }
            if let Some(base) = &operation.base_manifest {
                self.release(base, origin)?;
            }
            if let Some(base) = &operation.base {
                let (_, _, release) = rows
                    .get_record::<ReleasePin>(&format!("result/{base}"))?
                    .ok_or_else(unknown_dependencies)?;
                self.release_pin(&release, origin)?;
            }
        } else if key.starts_with("request-dependencies/") {
            let dependencies: crate::engine::admission::AdmissionDependencies = decode(json)?;
            if let Some(base) = &dependencies.base {
                self.release(&base.manifest, origin)?;
            }
            if let Some(coverage) = &dependencies.coverage {
                self.coverage(coverage, origin)?;
            }
            for object in dependencies.inputs.values() {
                self.object(object, origin)?;
            }
            if let Some(source) = &dependencies.update_source {
                self.table(source, origin)?;
            }
        } else if key.starts_with("request/") {
            let request: OperationRequest = decode(json)?;
            if self.shallow
                || (!rows.contains_key(&format!("result/{}", request.operation()))
                    && !rows.contains_key(&format!("abandoned/{}", request.operation())))
            {
                for input in request.inputs().values() {
                    self.path(&input.path, origin)?;
                }
                if let OperationRequest::Refresh(refresh) = &request {
                    for input in refresh.inputs().values() {
                        self.release(&input.manifest, origin)?;
                    }
                }
            }
        } else if key.starts_with("groups/") {
            let groups: CompletedGroups = decode(json)?;
            let CompletedGroups::Captured { outputs, source } = groups else {
                return Err(unknown_dependencies());
            };
            self.snapshot(&source, origin)?;
            for object in outputs.values() {
                self.native(object, origin)?;
            }
        } else if key.starts_with("generation/") {
            let seal: GenerationSeal = decode(json)?;
            if self.shallow
                && (key != format!("generation/{}", seal.generation)
                    || origin.table != crate::catalog::lifecycle_ident(&seal.family).to_string()
                    || seal.members.is_empty()
                    || seal.members.iter().any(|(dataset, version)| {
                        dataset != &version.dataset || dataset != &version.manifest.owner
                    }))
            {
                return Err(invalid_inventory("generation seal identity differs"));
            }
            self.seal(&seal, origin)?;
        } else if key == "family-members" {
            let _: BTreeSet<Id> = decode(json)?;
        } else if key.starts_with("succession/") {
            let succession: Succession = decode(json)?;
            if key != format!("succession/{}", succession.selected.version)
                || crate::catalog::lifecycle_ident(&succession.selected.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("succession receipt identity differs"));
            }
            super::super::history::check_link_receipt(rows, origin.revision, &succession)?;
            validate_object(&succession.selected.manifest)?;
            if let Some(previous) = &succession.predecessor {
                validate_object(&previous.manifest)?;
            }
        } else if let Some(version) = key.strip_prefix("input-progress/") {
            let progress: BTreeMap<Id, SuccessionProof> = decode(json)?;
            let (revision, _, result): (_, _, ProgressResult) = rows
                .get_record(&format!("result/{version}"))?
                .ok_or_else(|| invalid_inventory("progress result is missing"))?;
            let related = result
                .dataset
                .related
                .as_ref()
                .ok_or_else(|| invalid_inventory("progress result has no input coverage"))?;
            if revision != origin.revision
                || result.id.as_str() != version
                || result.dataset.version != result.id
                || result.manifest.owner != result.dataset.dataset
                || !result.verified
                || crate::catalog::lifecycle_ident(&result.dataset.dataset).to_string()
                    != origin.table
                || !progress.keys().eq(related.coverage.inputs.keys())
            {
                return Err(invalid_inventory("progress receipt identity differs"));
            }
            for (dataset, proof) in &progress {
                if related.coverage.inputs.get(dataset) != Some(&proof.target)
                    || proof.observed_revision <= 0
                    || (proof.selections_advanced == 0) != (proof.prior == proof.target)
                    || (proof.selections_behind == 0) != (proof.target == proof.observed_head)
                {
                    return Err(invalid_inventory("progress receipt coverage differs"));
                }
                for version in [&proof.prior, &proof.target, &proof.observed_head] {
                    if &version.dataset != dataset || version.manifest.owner != *dataset {
                        return Err(invalid_inventory("progress endpoint identity differs"));
                    }
                    validate_object(&version.manifest)?;
                }
                for (left, right) in [
                    (&proof.prior, &proof.target),
                    (&proof.target, &proof.observed_head),
                    (&proof.prior, &proof.observed_head),
                ] {
                    if left.version == right.version && left != right {
                        return Err(invalid_inventory("progress version identity conflicts"));
                    }
                }
            }
            // Order and lag are catalog evidence. Availability promises come
            // from actual results/phases, not merely observing these identities.
            // Existing owner grants remain independent roots until retired.
        } else if key.starts_with("protection/") {
            let protection: Protection = decode(json)?;
            if !protection.retired {
                for prefix in &protection.paths {
                    if !prefix.ends_with('/') {
                        return Err(invalid_inventory("unsupported protection path"));
                    }
                    let candidates = self
                        .candidates
                        .range(prefix.clone()..)
                        .take_while(|(path, _)| path.starts_with(prefix))
                        .filter(|(_, object)| object.blocker.is_none())
                        .map(|(path, _)| path.clone())
                        .collect::<Vec<_>>();
                    for path in candidates {
                        self.path(&path, origin)?;
                    }
                }
            }
        } else if crate::engine::backfill::continuous::validate_control_record(
            key,
            json,
            rows,
            &origin.table,
            origin.revision,
        )? {
            // Parent/control pointers do not add a second payload lifetime.
        } else if key.starts_with("abandoned/") {
            let _: Abandonment = decode(json)?;
        } else if key.starts_with("runtime/") {
            let _: Runtime = decode(json)?;
        } else if key.starts_with("failure/") || key.starts_with("invalidation/") {
            let _: String = decode(json)?;
        } else {
            return Err(invalid_inventory(format!(
                "unsupported lifecycle root {key}"
            )));
        }
        Ok(())
    }

    async fn expand(&mut self, budget: &Budget, root: &Path) -> Result<()> {
        while let Some(Pending {
            object,
            kind,
            origin,
        }) = self.pending.pop().or_else(|| self.pending_native.pop())
        {
            tokio::task::consume_budget().await;
            if self
                .visited
                .get(&object.path)
                .is_some_and(|entry| entry.complete)
            {
                continue;
            }
            // The immutable descriptor supplies its size before decoding. The
            // worker retains this allowance if its waiting caller is canceled.
            let bytes = usize::try_from(object.bytes).map_err(|_| capacity())?;
            if bytes > budget.limits.max_metadata_bytes {
                return Err(capacity());
            }
            let memory = Arc::new(budget.reserve(bytes.saturating_mul(8).saturating_add(4096))?);
            match kind {
                Expansion::Origins => {
                    let Strict(originals): Strict<ScalarOrigins> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    self.group_dependencies(&originals.dependencies, &origin)?;
                    for file in &originals.files {
                        self.file_completion(&file.completion, &origin)?;
                    }
                }
                Expansion::Declaration => {
                    // Declarations contain field identities and definitions,
                    // with no transitive object references.
                    let _: Strict<OriginalDeclaration> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                }
                Expansion::Release => {
                    let Strict(dataset): Strict<DatasetPin> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    if dataset.dataset != object.owner {
                        return Err(invalid_inventory("release owner differs"));
                    }
                    self.dataset(&dataset, &origin)?;
                }
                Expansion::Changes(prepared) => {
                    let Strict(applied): Strict<AppliedChangeSet> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    crate::engine::changes::check_applied(&applied, &prepared)?;
                    for changes in applied.source.iter().chain(applied.receipts.iter()) {
                        self.optional_changes(changes, &origin)?;
                    }
                }
                Expansion::Restore(reference) => {
                    let point: crate::restore::RestorePoint =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    crate::restore::validate_point(root, &reference, &point)?;
                    self.object(&point.catalog, &origin)?;
                    for retained in &point.objects {
                        self.object(&retained.backup, &origin)?;
                    }
                }
                Expansion::Native(_) => {
                    let Strict(metadata): Strict<TableMetadata> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    let actual = (metadata.current_snapshot_id(), metadata.current_schema_id());
                    let binding = self
                        .visited
                        .get(&object.path)
                        .and_then(|entry| entry.binding);
                    if binding.is_some_and(|expected| expected != actual) {
                        return Err(invalid_inventory("native snapshot or schema differs"));
                    }
                    if let Some(entry) = self.visited.get_mut(&object.path) {
                        entry.binding = Some(actual);
                    }
                    let table = Table::builder()
                        .identifier(TableIdent::from_strs(["retention", "snapshot"])?)
                        .metadata(metadata)
                        .metadata_location(&object.path)
                        .file_io(iceberg::io::FileIO::new_with_fs())
                        .runtime(iceberg::Runtime::try_current()?)
                        .readonly(true)
                        .build()?;
                    let files = storage::snapshot_files(&table, budget).await?;
                    for path in &files.paths {
                        self.path(path, &origin)?;
                    }
                }
            }
            if let Some(entry) = self.visited.get_mut(&object.path) {
                entry.complete = true;
            }
            self.metadata_objects_read += 1;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Runtime {
    processor: String,
    #[serde(default)]
    package_version: Option<String>,
    dependency_lock_blake3: String,
    limits: Limits,
}

struct Strict<T>(T);
impl<'de, T: DeserializeOwned + Serialize + 'static> Deserialize<'de> for Strict<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let mut original = serde_json::Value::deserialize(deserializer)?;
        let value: T =
            serde_json::from_value(original.clone()).map_err(serde::de::Error::custom)?;
        let known = serde_json::to_value(&value).map_err(serde::de::Error::custom)?;
        normalize_legacy_state_fields::<T>(&mut original).map_err(serde::de::Error::custom)?;
        if !known_fields(&original, &known) {
            return Err(serde::de::Error::custom(
                "unsupported retention metadata fields",
            ));
        }
        Ok(Self(value))
    }
}

fn decode<T: DeserializeOwned + Serialize + 'static>(json: &str) -> Result<T> {
    Ok(serde_json::from_str::<Strict<T>>(json)?.0)
}

fn normalize_legacy_state_fields<T: 'static>(
    original: &mut serde_json::Value,
) -> serde_json::Result<()> {
    use std::any::TypeId;

    // Only complete states dropped these two optional members. Typed root
    // selection and exact paths prevent similarly named recovery dependencies
    // or arbitrary nested objects from inheriting this compatibility exception.
    if TypeId::of::<T>() == TypeId::of::<DatasetPin>() {
        normalize_legacy_dataset(original)?;
    } else if TypeId::of::<T>() == TypeId::of::<ReleasePin>() {
        if let Some(dataset) = original.get_mut("dataset") {
            normalize_legacy_dataset(dataset)?;
        }
    } else if TypeId::of::<T>() == TypeId::of::<Authority>() {
        if let Some(dataset) = original.get_mut("selected") {
            normalize_legacy_dataset(dataset)?;
        }
        if let Some(dataset) = original
            .get_mut("release")
            .and_then(|release| release.get_mut("dataset"))
        {
            normalize_legacy_dataset(dataset)?;
        }
    }
    Ok(())
}

fn normalize_legacy_dataset(dataset: &mut serde_json::Value) -> serde_json::Result<()> {
    if !dataset
        .get("format")
        .and_then(serde_json::Value::as_u64)
        .and_then(|format| u32::try_from(format).ok())
        .is_some_and(supported_format)
    {
        return Ok(());
    }
    let Some(fields) = dataset.as_object_mut() else {
        return Ok(());
    };
    if let Some(changes) = fields.remove("changes") {
        check_legacy_changes(changes)?;
    }
    if let Some(changes) = fields
        .get_mut("evidence")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|evidence| evidence.remove("changes"))
    {
        check_legacy_changes(changes)?;
    }
    Ok(())
}

fn check_legacy_changes(changes: serde_json::Value) -> serde_json::Result<()> {
    if !changes.is_null() {
        // Check the former typed shape, including unrecognized dependency fields.
        // These records gain neither retention roots nor certificate trust.
        let _: Strict<ChangePin> = serde_json::from_value(changes)?;
    }
    Ok(())
}

// Serde's flatten and untagged enums buffer unknown fields, bypassing ordinary
// ignored-field callbacks. Require the typed round trip to retain every key.
// Values are interpreted by the maintained deserializers; this checks only
// whether an unknown dependency-bearing field would otherwise disappear.
fn known_fields(original: &serde_json::Value, known: &serde_json::Value) -> bool {
    match original {
        serde_json::Value::Object(fields) => fields.iter().all(|(key, value)| {
            known
                .get(key)
                .is_some_and(|field| known_fields(value, field))
        }),
        serde_json::Value::Array(values) => known.as_array().is_some_and(|fields| {
            if values.len() != fields.len() {
                return false;
            }
            // Iceberg keeps these collections in maps and can reorder them on
            // serialization. Match their stable IDs in O(n log n), not position
            // or pairwise search. Ordered schema fields and other arrays stay positional.
            for id in ["schema-id", "snapshot-id", "spec-id", "order-id"] {
                let indexed: Option<BTreeMap<_, _>> = fields
                    .iter()
                    .map(|field| Some((field.get(id)?.as_i64()?, field)))
                    .collect();
                if let Some(indexed) = indexed
                    && indexed.len() == fields.len()
                    && values
                        .iter()
                        .all(|value| value.get(id).and_then(serde_json::Value::as_i64).is_some())
                {
                    return values.iter().all(|value| {
                        indexed
                            .get(&value[id].as_i64().expect("ID checked above"))
                            .is_some_and(|field| known_fields(value, field))
                    });
                }
            }
            values
                .iter()
                .zip(fields)
                .all(|(value, field)| known_fields(value, field))
        }),
        _ => true,
    }
}

fn reference_bytes(object: &ObjectRef) -> usize {
    object
        .path
        .len()
        .saturating_add(object.digest.len())
        .saturating_add(object.owner.as_str().len())
        .saturating_add(256)
}

pub(super) fn validate_object(object: &ObjectRef) -> Result<()> {
    let normalized: PathBuf = Path::new(&object.path).components().collect();
    if !Path::new(&object.path).is_absolute()
        || normalized.to_str() != Some(object.path.as_str())
        || Path::new(&object.path)
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
        || object.digest.len() != 64
        || !object.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_inventory(
            "object needs an absolute path and a BLAKE3 identity",
        ));
    }
    Ok(())
}

fn invalid_inventory(message: impl Into<String>) -> Error {
    Error::refusal("retention_inventory", message)
}
fn unknown_dependencies() -> Error {
    Error::refusal(
        "retention_unknown_dependencies",
        "retained work lacks exact dependency metadata; cleanup cannot infer it",
    )
}
fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "retention inspection exceeds its metadata allowance",
    )
}

#[cfg(test)]
mod scalar_origins_tests {
    use super::*;
    use crate::engine::backfill::origins::OriginalExecution;

    fn object(name: &str) -> ObjectRef {
        ObjectRef {
            owner: Id::new("owner").unwrap(),
            path: format!("/unavailable/{name}"),
            digest: "0".repeat(64),
            bytes: 17,
        }
    }

    fn origin() -> RetentionBlocker {
        RetentionBlocker {
            table: "owner".into(),
            key: "phase/backfill/file".into(),
            revision: 1,
        }
    }

    #[test]
    fn shallow_original_roots_keep_schema_as_a_leaf_and_do_not_queue_reads() {
        let phase = FileCompletion {
            execution: Some(OriginalExecution {
                operation: Id::new("original-operation").unwrap(),
                declaration: object("declaration.json"),
            }),
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output.json"),
            outputs: vec![object("output.parquet")],
        };
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        inventory.shallow = true;
        inventory.file_completion(&phase, &origin()).unwrap();
        let origins = object("origins.json");
        inventory
            .enqueue(&origins, Expansion::Origins, &origin())
            .unwrap();
        inventory
            .enqueue(&origins, Expansion::Origins, &origin())
            .unwrap();
        assert!(inventory.pending.is_empty());
        assert!(inventory.pending_native.is_empty());
        assert!(
            !inventory
                .visited
                .contains_key("/unavailable/old-schema.json")
        );
        assert_eq!(inventory.visited.len(), 2);
        assert!(
            inventory
                .enqueue(&origins, Expansion::Declaration, &origin())
                .is_err()
        );
        let references = inventory.into_references().unwrap();
        for reference in [
            phase.source,
            phase.source_metadata.unwrap(),
            phase.descriptor,
            phase.outputs[0].clone(),
            phase.execution.unwrap().declaration,
            origins,
        ] {
            assert!(references.contains(&reference));
        }
    }

    #[tokio::test]
    async fn typed_origins_expand_once_and_retain_original_file_closure() {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::new(crate::budget::Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            max_metadata_bytes: 1 << 20,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        let definition = GroupDefinition {
            group: Id::new("computed").unwrap(),
            definition: "v1".into(),
            fields: Vec::new(),
        };
        let declaration = OriginalDeclaration {
            processor: "fixture".into(),
            definition: definition.clone(),
            input_fields: BTreeMap::new(),
        };
        let declaration = crate::engine::backfill::origins::write(
            &Id::new("owner").unwrap(),
            &root.path().join("declaration.json"),
            &declaration,
            &budget,
        )
        .unwrap();
        let mut source = TablePin {
            table: "source".into(),
            metadata: object("source-table.json"),
            logical_metadata: None,
            snapshot: Some(1),
            schema_id: 0,
            objects: vec![object("source-table.json"), object("source.parquet")],
            rows: 1,
        };
        let mut output = source.clone();
        output.table = "output".into();
        output.metadata = object("output-table.json");
        output.objects = vec![output.metadata.clone(), object("output.parquet")];
        source.logical_metadata = Some(object("logical-source.json"));
        let completion = FileCompletion {
            execution: Some(OriginalExecution {
                operation: Id::new("original-operation").unwrap(),
                declaration: declaration.clone(),
            }),
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output-file.json"),
            outputs: vec![object("output.parquet")],
        };
        let originals = ScalarOrigins {
            processor: "fixture".into(),
            definition,
            input_fields: BTreeMap::new(),
            dependencies: GroupDependencies {
                source: (&source).into(),
                source_logical_metadata: source.logical_metadata.clone(),
                output: (&output).into(),
            },
            files: vec![crate::engine::backfill::origins::FileOrigin {
                generation: RetainedGeneration {
                    key: "phase/backfill/file".into(),
                    revision: 1,
                },
                completion: completion.clone(),
            }],
        };
        let originals = crate::engine::backfill::origins::write(
            &Id::new("owner").unwrap(),
            &root.path().join("origins.json"),
            &originals,
            &budget,
        )
        .unwrap();
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        // Complete table inventories avoid opening native metadata or payloads.
        inventory.table(&source, &origin()).unwrap();
        inventory.table(&output, &origin()).unwrap();
        inventory
            .enqueue(&originals, Expansion::Origins, &origin())
            .unwrap();
        inventory
            .enqueue(&originals, Expansion::Origins, &origin())
            .unwrap();
        inventory.expand(&budget, root.path()).await.unwrap();
        assert_eq!(inventory.metadata_objects_read, 2);
        assert!(
            !inventory
                .visited
                .contains_key("/unavailable/old-schema.json")
        );
        assert!(
            inventory
                .enqueue(&originals, Expansion::Declaration, &origin())
                .is_err()
        );
        let references = inventory.into_references().unwrap();
        for reference in [
            originals,
            declaration,
            completion.source,
            completion.source_metadata.unwrap(),
            completion.descriptor,
            completion.outputs[0].clone(),
            source.logical_metadata.unwrap(),
        ] {
            assert!(references.contains(&reference));
        }
    }

    #[test]
    fn legacy_execution_absence_and_null_remain_unknown_with_strict_decoding() {
        let phase = FileCompletion {
            execution: None,
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output.json"),
            outputs: vec![object("output.parquet")],
        };
        let mut json = serde_json::to_value(phase).unwrap();
        let explicit_null: FileCompletion = decode(&json.to_string()).unwrap();
        assert!(explicit_null.execution.is_none());
        json.as_object_mut().unwrap().remove("execution");
        let absent: FileCompletion = decode(&json.to_string()).unwrap();
        assert!(absent.execution.is_none());
        json["unrecognized_origins"] = serde_json::json!({"declaration": object("unknown.json")});
        assert!(decode::<FileCompletion>(&json.to_string()).is_err());
    }
}

#[cfg(test)]
mod collector_tests;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod legacy_fields_tests;
#[cfg(test)]
mod optional_changes_tests;

#[cfg(test)]
mod correction_tests {
    use super::*;

    #[tokio::test]
    async fn correction_registry_is_typed_object_free_and_revision_bound() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            root.path(),
            Limits {
                compute_threads: 1,
                memory_bytes: 64 << 20,
                scratch_bytes: 16 << 20,
                max_batch_bytes: 1 << 20,
                max_metadata_bytes: 1 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let correction = DependencyCorrection {
            dataset: Id::new("comments").unwrap(),
            id: Id::new("language-omission").unwrap(),
            definition: GroupDefinition {
                group: Id::new("classification").unwrap(),
                definition: "text-only-v1".into(),
                fields: vec![ComputedField {
                    id: 10,
                    name: "label".into(),
                    expression: "text".into(),
                }],
            },
            fields: BTreeSet::from([2]),
            reason: "language was material".into(),
        };
        let decision = engine.correct_dependencies(&correction).await.unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&correction.dataset).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        for (key, revision, json) in rows.prefix("") {
            let origin = RetentionBlocker {
                table: view.table.identifier().to_string(),
                key: key.into(),
                revision,
            };
            assert!(
                reference_roots(&rows, key, json, &origin, 1 << 20)
                    .unwrap()
                    .is_empty()
            );
            let mut wrong_revision = origin;
            wrong_revision.revision += 1;
            assert!(reference_roots(&rows, key, json, &wrong_revision, 1 << 20).is_err());
        }
        // Restore follows these registry records without inventing physical objects.
        let inspection = engine.inspect_retention(&[]).await.unwrap();
        assert!(inspection.objects.is_empty());
        let (_, objects) = engine
            .restore_dependencies(&correction.dataset)
            .await
            .unwrap();
        assert!(
            objects
                .iter()
                .all(|object| object.owner == correction.dataset)
        );

        let mut unexpected = serde_json::to_value(&decision).unwrap();
        unexpected["unrecognized_dependency"] = serde_json::json!({"path": "/unknown"});
        assert!(decode::<CorrectionDecision>(&unexpected.to_string()).is_err());
        let key = format!("dependency-correction/{}", correction.id);
        let origin = RetentionBlocker {
            table: view.table.identifier().to_string(),
            key: key.clone(),
            revision: decision.revision,
        };
        let proof = DependencyQualification {
            checker: "docspec.scalar-field-correspondence.v1".into(),
            scope: crate::engine::corrections::records::scope_key(&correction.definition).unwrap(),
            fields: correction.fields.clone(),
            first_revision: decision.revision,
            origins: ObjectRef {
                owner: correction.dataset.clone(),
                path: "/unavailable/origins.json".into(),
                digest: "0".repeat(64),
                bytes: 17,
            },
            target: NativeSnapshotRef {
                metadata: ObjectRef {
                    owner: correction.dataset.clone(),
                    path: "/unavailable/target.json".into(),
                    digest: "0".repeat(64),
                    bytes: 17,
                },
                snapshot: Some(42),
                schema_id: 7,
            },
        };
        let proof_key = crate::engine::corrections::qualification_key(&proof).unwrap();
        let references = reference_roots(
            &rows,
            &proof_key,
            &serde_json::to_string(&proof).unwrap(),
            &origin,
            1 << 20,
        )
        .unwrap();
        assert!(references.contains(&proof.origins));
        assert!(references.contains(&proof.target.metadata));
        assert_eq!(references.len(), 2);
        assert!(
            reference_roots(
                &rows,
                "dependency-qualification/wrong-key",
                &serde_json::to_string(&proof).unwrap(),
                &origin,
                1 << 20,
            )
            .is_err()
        );
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        inventory.shallow = true;
        inventory.qualification(&proof, &origin).unwrap();
        assert_eq!(
            inventory.visited[&proof.target.metadata.path].binding,
            Some((proof.target.snapshot, proof.target.schema_id)),
        );
        assert!(inventory.pending.is_empty());
        assert!(inventory.pending_native.is_empty());
        assert!(
            reference_roots(
                &rows,
                "dependency-correction/different-id",
                &serde_json::to_string(&decision).unwrap(),
                &origin,
                1 << 20,
            )
            .is_err()
        );
    }

    #[test]
    fn qualification_recovery_can_retire_without_retiring_correction_authority() {
        assert!(super::super::retirement::completion_key(
            "dependency-qualification/proof"
        ));
        for key in [
            "dependency-correction/decision",
            "correction-requirements/scope",
        ] {
            assert!(!super::super::retirement::completion_key(key));
            assert!(!super::super::retirement::retained_key(key));
        }
    }
}

fn required_record<T: DeserializeOwned>(
    rows: &crate::catalog::SnapshotRows,
    key: &str,
) -> Result<T> {
    rows.get_record(key)?
        .map(|(_, _, value)| value)
        .ok_or_else(unknown_dependencies)
}
