//! Exact references and candidate protection from retained metadata, never payload scans.

use super::*;
use crate::engine::backfill::{Capture, FileCompletion, PreparedCompletion, ValidationCompletion};
use iceberg::{NamespaceIdent, TableIdent, spec::TableMetadata, table::Table};
use serde::de::DeserializeOwned;

/// Exact references whose collection remains charged until the caller releases it.
#[derive(Debug)]
pub(in crate::engine) struct ReferenceObjects {
    pub(in crate::engine) objects: Vec<ObjectRef>,
    _memory: Arc<tokio::sync::OwnedSemaphorePermit>,
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
                _memory: Arc::new(budget.reserve(0)?),
            });
        }
        // Bound even a collection of tiny roots before traversal or external
        // decoding. The counting sink admits their complete serialized shape
        // without allocating another copy of these caller-owned pins.
        if releases.len().saturating_mul(size_of::<ReleasePin>()) > limit {
            return Err(capacity());
        }
        inputs::metadata_size(&releases, limit)?;
        // Same admission as inspection: identities/queue, one decoded release,
        // and one decoded native object. Keep only the returned objects' charge
        // afterward, so separate small collections can coexist under this limit.
        let memory = Arc::new(budget.reserve(limit.saturating_mul(8))?);
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
        inventory.expand(budget, &memory).await?;
        let objects = inventory.into_references()?;
        let bytes = objects.iter().fold(0usize, |bytes, object| {
            bytes.saturating_add(reference_bytes(object))
        });
        // Awaited decoders have released their clones. Split the existing permit
        // instead of briefly admitting a second allocation for the same objects.
        let mut memory = Arc::try_unwrap(memory).map_err(|_| capacity())?;
        let retained = memory.split(bytes).ok_or_else(capacity)?;
        Ok(ReferenceObjects {
            objects,
            _memory: Arc::new(retained),
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
        // Candidate/visited state, one decoded row and one decoded external
        // metadata object are admitted together. Native readers admit separately.
        let memory = Arc::new(budget.reserve(limit.saturating_mul(8))?);
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
            // Decode each retirement batch once. The bounded set lets both marker
            // validation and completion filtering share its exact authorization.
            let mut retired_phases = BTreeSet::new();
            for (key, revision, json) in rows.prefix("phase-retirement/") {
                let retired: PhaseRetirement = decode(json)?;
                super::phases::validate_markers(&rows, &retired, &view, key, revision)?;
                for phase in retired.plan.phases {
                    inventory.charge(phase.key.len().saturating_add(128))?;
                    if !retired_phases.insert(phase) {
                        return Err(super::phases::invalid_retirement());
                    }
                }
            }
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
                    for object in &grant.objects {
                        super::owners::available(&rows, &view, object)?;
                        inventory.object(object, &origin)?;
                    }
                    continue;
                }
                if key.starts_with("reference-binding/") {
                    let binding: super::owners::ReferenceBinding = decode(json)?;
                    let required =
                        super::owners::validate_binding(&rows, &view, key, revision, &binding)?;
                    inventory.binding(binding, required)?;
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
                if key.starts_with("phase-retirement/") {
                    continue;
                }
                if key.starts_with("phase-retired/") {
                    let marker: super::phases::RetiredPhase = decode(json)?;
                    if key != super::phases::marker_key(&marker.phase)
                        || !retired_phases.contains(&marker.phase)
                    {
                        return Err(super::phases::invalid_retirement());
                    }
                    continue;
                }
                if completion_key(key)
                    && retired_phases.contains(&PhaseGeneration {
                        key: key.into(),
                        revision,
                    })
                {
                    continue;
                }
                inventory.record(key, json, &rows, &origin)?;
                inventory.expand(budget, &memory).await?;
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

fn charge_namespaces(inventory: &mut Inventory, namespaces: &[NamespaceIdent]) -> Result<()> {
    // The maintained catalog returns a Vec. This bounds admitted application
    // state after return, not that adapter's peak allocation during enumeration.
    for namespace in namespaces {
        inventory.charge(namespace.to_string().len().saturating_add(128))?;
    }
    Ok(())
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
    required: BTreeMap<String, ObjectRef>,
}

struct Inventory {
    candidates: BTreeMap<String, ObjectProtection>,
    // Some selects explicit-root collection. Native files only provide paths;
    // these must resolve to retained exact identities after the whole closure is
    // visited, independent of expansion order. None preserves candidate inspection.
    required_paths: Option<BTreeSet<String>>,
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
            visited: BTreeMap::new(),
            pending: Vec::new(),
            pending_native: Vec::new(),
            claims: BTreeMap::new(),
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
        for reference in binding.protections {
            if self
                .claims
                .get(&reference.owner)
                .is_none_or(|claims| !claims.contains_key(&reference.token))
            {
                let bytes = inputs::metadata_size(&(&reference, &binding.consumer), self.limit)?;
                self.charge(bytes.saturating_mul(4).saturating_add(512))?;
            }
            let mut added = Vec::new();
            for object in required
                .iter()
                .filter(|object| object.owner == reference.owner)
            {
                let old = self
                    .claims
                    .get(&reference.owner)
                    .and_then(|claims| claims.get(&reference.token))
                    .and_then(|claim| claim.required.get(&object.path));
                if let Some(old) = old {
                    if old != object {
                        return Err(invalid_inventory("owner claim identity differs"));
                    }
                } else {
                    self.charge(reference_bytes(object).saturating_add(object.path.len()))?;
                    added.push(object.clone());
                }
            }
            let claim = self
                .claims
                .entry(reference.owner.clone())
                .or_default()
                .entry(reference.token.clone())
                .or_insert_with(|| GrantClaim {
                    reference: reference.clone(),
                    consumer: binding.consumer.clone(),
                    required: BTreeMap::new(),
                });
            if claim.reference != reference || claim.consumer != binding.consumer {
                return Err(invalid_inventory(
                    "owner token has conflicting consumer bindings",
                ));
            }
            for object in added {
                claim.required.insert(object.path.clone(), object);
            }
        }
        Ok(())
    }

    async fn check_bindings(
        &self,
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
                let (revision, _, grant) = rows
                    .get_record::<super::owners::OwnerProtection>(token)?
                    .ok_or_else(|| invalid_inventory("consumer refers to a missing owner token"))?;
                super::owners::validate_grant(&view, token, revision, &grant)?;
                super::owners::validate_reference(
                    &claim.reference,
                    &grant,
                    &claim.consumer,
                    claim.required.values(),
                )?;
            }
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
            if authority.selected.as_ref()
                != authority.release.as_ref().map(|release| &release.dataset)
            {
                return Err(invalid_inventory("selected dataset and release disagree"));
            }
            if let Some(release) = &authority.release {
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
        } else if key.starts_with("request/") {
            let request: OperationRequest = decode(json)?;
            if !rows.contains_key(&format!("result/{}", request.operation()))
                && !rows.contains_key(&format!("abandoned/{}", request.operation()))
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
            self.seal(&decode(json)?, origin)?;
        } else if key == "family-members" {
            let _: BTreeSet<Id> = decode(json)?;
        } else if key.starts_with("succession/") {
            let succession: Succession = decode(json)?;
            self.release(&succession.selected.manifest, origin)?;
            if let Some(previous) = &succession.predecessor {
                self.release(&previous.manifest, origin)?;
            }
        } else if key.starts_with("input-progress/") {
            let progress: BTreeMap<Id, SuccessionProof> = decode(json)?;
            for proof in progress.values() {
                for version in [&proof.prior, &proof.target, &proof.observed_head] {
                    self.release(&version.manifest, origin)?;
                }
            }
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

    async fn expand(
        &mut self,
        budget: &Budget,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<()> {
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
            match kind {
                Expansion::Release => {
                    let Strict(dataset): Strict<DatasetPin> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, memory)
                            .await?;
                    if dataset.dataset != object.owner {
                        return Err(invalid_inventory("release owner differs"));
                    }
                    self.dataset(&dataset, &origin)?;
                }
                Expansion::Native(_) => {
                    let Strict(metadata): Strict<TableMetadata> =
                        inputs::read_manifest(&object, budget, &mut self.metadata_bytes, memory)
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

fn validate_object(object: &ObjectRef) -> Result<()> {
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
