//! Exact references and candidate protection from retained metadata, never payload scans.

use super::*;
use crate::engine::backfill::{Capture, FileCompletion, PreparedCompletion, ValidationCompletion};
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
        inventory.expand(budget).await?;
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

    async fn inspect_retention_admitted(
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
        // Engine::open owns a flat embedded catalog. The maintained SQL adapter
        // scans all namespace rows per listing; recurse here and cost becomes
        // quadratic in dataset count. Injected/hierarchical catalogs need their
        // own qualified enumeration before this inventory can inspect them.
        let mut namespaces = lifecycle.catalog.list_namespaces(None).await?;
        charge_namespaces(&mut inventory, &namespaces)?;
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
            let rows = lifecycle.rows(&view).await?;
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
                    if key != super::owners::unavailable_key(&unavailable.object.path)
                        || unavailable.revision != revision
                        || &crate::catalog::lifecycle_ident(&unavailable.object.owner)
                            != view.table.identifier()
                    {
                        return Err(invalid_inventory("object unavailability identity differs"));
                    }
                    validate_object(&unavailable.object)?;
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
                inventory.expand(budget).await?;
            }
        }
        boundaries.sort_by(|left, right| left.table.cmp(&right.table));
        inventory.check_bindings(lifecycle, &boundaries).await?;
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
    Release,
    Native(Option<(Option<i64>, i32)>),
}

struct Pending {
    object: ObjectRef,
    kind: Expansion,
    origin: RetentionBlocker,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataKind {
    Release,
    Native,
}

struct VisitedMetadata {
    object: ObjectRef,
    kind: MetadataKind,
    binding: Option<(Option<i64>, i32)>,
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
        let (tag, binding) = match kind {
            Expansion::Release => (MetadataKind::Release, None),
            Expansion::Native(binding) => (MetadataKind::Native, binding),
        };
        if let Some(old) = self.visited.get_mut(&object.path) {
            if old.object != *object
                || old.kind != tag
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
        self.visited.insert(
            object.path.clone(),
            VisitedMetadata {
                object: object.clone(),
                kind: tag,
                binding,
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
        self.release(&computed.base_manifest, origin)
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
        if let Some(parent) = &dataset.parent_manifest {
            self.release(parent, origin)?;
        }
        if let Some(changes) = &dataset.changes {
            self.changes(changes, origin)?;
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
        if key.starts_with("phase/backfill/") {
            let phase: FileCompletion = decode(json)?;
            self.object(&phase.source, origin)?;
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
        } else if key.starts_with("phase/") {
            let phase: PhaseResult = decode(json)?;
            if !matches!(
                phase.phase.as_str(),
                "admission" | "processing" | "validation" | "evidence"
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
            self.snapshot(&dependencies.source, origin)?;
            self.snapshot(&dependencies.output, origin)?;
        } else if key.starts_with("backfill-validation/") {
            let phase: ValidationCompletion = decode(json)?;
            self.object(&phase.output, origin)?;
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            self.snapshot(&dependencies.source, origin)?;
            self.snapshot(&dependencies.output, origin)?;
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

    async fn expand(&mut self, budget: &Budget) -> Result<()> {
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
                Expansion::Release => {
                    let Strict(dataset): Strict<DatasetPin> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, &memory)
                            .await?;
                    if dataset.dataset != object.owner {
                        return Err(invalid_inventory("release owner differs"));
                    }
                    self.dataset(&dataset, &origin)?;
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
impl<'de, T: DeserializeOwned + Serialize> Deserialize<'de> for Strict<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let original = serde_json::Value::deserialize(deserializer)?;
        let value: T =
            serde_json::from_value(original.clone()).map_err(serde::de::Error::custom)?;
        let known = serde_json::to_value(&value).map_err(serde::de::Error::custom)?;
        if !known_fields(&original, &known) {
            return Err(serde::de::Error::custom(
                "unsupported retention metadata fields",
            ));
        }
        Ok(Self(value))
    }
}

fn decode<T: DeserializeOwned + Serialize>(json: &str) -> Result<T> {
    Ok(serde_json::from_str::<Strict<T>>(json)?.0)
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
mod collector_tests;
#[cfg(test)]
mod history_tests;
