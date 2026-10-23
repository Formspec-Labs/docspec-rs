//! Typed retention root registration on `Inventory`.

use super::{
    Expansion, Inventory, MetadataKind, Pending, VisitedMetadata, invalid_inventory,
    reference_bytes, unknown_dependencies,
};
use crate::{
    Result,
    engine::{
        backfill::{FileCompletion, GroupDependencies},
        changes::PreparedChanges,
        inputs, supported_format,
    },
    model::{
        ChangePin, ComputedGroup, DatasetPin, DependencyQualification, GenerationSeal,
        InputCoverage, NativeSnapshotRef, ObjectRef, OperationRequest, RefreshMode, RelatedResult,
        ReleasePin, RetentionBlocker, TablePin,
    },
    storage,
};

impl Inventory {
    pub(super) fn enqueue(
        &mut self,
        object: &ObjectRef,
        kind: Expansion,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        self.object(object, origin)?;
        let (tag, binding) = match &kind {
            Expansion::Origins => (MetadataKind::Origins, None),
            Expansion::Declaration => (MetadataKind::Declaration, None),
            Expansion::Release { .. } => (MetadataKind::Release, None),
            Expansion::Changes(_) => (MetadataKind::Changes, None),
            Expansion::Restore(_) => (MetadataKind::Restore, None),
            Expansion::Native(binding) => (MetadataKind::Native, *binding),
        };
        let change_binding = match &kind {
            Expansion::Changes(prepared) => Some(storage::json_digest(prepared)?),
            _ => None,
        };
        let release_payloads = matches!(&kind, Expansion::Release { payloads: true });
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
            if release_payloads && !old.release_payloads {
                // An independently retained availability/recovery root wins over
                // interpretation history, even after that history was expanded.
                old.release_payloads = true;
                old.complete = false;
                self.charge(
                    reference_bytes(object)
                        .saturating_add(origin.table.len() + origin.key.len() + 128),
                )?;
                if !self.shallow {
                    self.pending.push(Pending {
                        object: object.clone(),
                        kind,
                        origin: origin.clone(),
                    });
                }
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
                release_payloads,
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

    pub(super) fn release(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(object, Expansion::Release { payloads: true }, origin)
    }

    pub(super) fn snapshot(
        &mut self,
        snapshot: &NativeSnapshotRef,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        self.enqueue(
            &snapshot.metadata,
            Expansion::Native(Some((snapshot.snapshot, snapshot.schema_id))),
            origin,
        )
    }

    pub(super) fn native(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(object, Expansion::Native(None), origin)
    }

    pub(super) fn table(&mut self, table: &TablePin, origin: &RetentionBlocker) -> Result<()> {
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

    pub(super) fn coverage(
        &mut self,
        coverage: &InputCoverage,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        for version in coverage.inputs.values() {
            self.release(&version.manifest, origin)?;
        }
        for seal in coverage.generations.values() {
            // Coverage consumes its explicit inputs. Other cohort members prove
            // exact generation membership, without promising their row payloads.
            for version in seal.members.values() {
                self.history(&version.manifest, origin)?;
            }
        }
        Ok(())
    }

    pub(super) fn seal(&mut self, seal: &GenerationSeal, origin: &RetentionBlocker) -> Result<()> {
        for version in seal.members.values() {
            self.release(&version.manifest, origin)?;
        }
        Ok(())
    }

    pub(super) fn history(&mut self, object: &ObjectRef, origin: &RetentionBlocker) -> Result<()> {
        self.enqueue(object, Expansion::Release { payloads: false }, origin)
    }

    pub(super) fn computed(
        &mut self,
        computed: &ComputedGroup,
        origin: &RetentionBlocker,
        history: bool,
    ) -> Result<()> {
        self.object(&computed.source_metadata, origin)?;
        if let Some(origins) = &computed.origins {
            self.enqueue(origins, Expansion::Origins, origin)?;
        }
        if let Some(qualification) = &computed.qualification {
            self.qualification(qualification, origin)?;
        }
        if history {
            self.history(&computed.base_manifest, origin)
        } else {
            self.release(&computed.base_manifest, origin)
        }
    }

    pub(super) fn qualification(
        &mut self,
        qualification: &DependencyQualification,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        crate::engine::corrections::validate_qualification(qualification)?;
        self.enqueue(&qualification.origins, Expansion::Origins, origin)?;
        self.snapshot(&qualification.target, origin)
    }

    pub(super) fn file_completion(
        &mut self,
        phase: &FileCompletion,
        origin: &RetentionBlocker,
    ) -> Result<()> {
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

    pub(super) fn group_dependencies(
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

    pub(super) fn related(
        &mut self,
        related: &RelatedResult,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        self.coverage(&related.coverage, origin)?;
        for source in related.input_sources.values() {
            self.table(source, origin)?;
        }
        self.table(&related.evidence, origin)
    }

    pub(super) fn changes(&mut self, changes: &ChangePin, origin: &RetentionBlocker) -> Result<()> {
        self.release(&changes.base_manifest, origin)?;
        self.native(&changes.before_metadata, origin)?;
        self.native(&changes.after_metadata, origin)?;
        self.table(&changes.removed, origin)?;
        self.table(&changes.added, origin)
    }

    pub(super) fn optional_changes(
        &mut self,
        changes: &ChangePin,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        // These metadata objects interpret the applied rows. The whole endpoint
        // snapshots and result manifests have independent availability promises.
        self.object(&changes.before_metadata, origin)?;
        self.object(&changes.after_metadata, origin)?;
        self.table(&changes.removed, origin)?;
        self.table(&changes.added, origin)
    }

    pub(super) fn prepared_changes(
        &mut self,
        prepared: PreparedChanges,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        let descriptor = prepared.descriptor.clone();
        self.enqueue(&descriptor, Expansion::Changes(prepared), origin)
    }

    pub(super) fn dataset(
        &mut self,
        dataset: &DatasetPin,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        if !supported_format(dataset.format) {
            return Err(invalid_inventory("unsupported release format"));
        }
        // Complete reconstruction owns fresh values/current contributors. Its
        // predecessor establishes interpretation and ordering, while separate
        // availability, recovery, seals and grants retain stronger payload roots.
        let history = ((matches!(&dataset.request, OperationRequest::Build(spec) if spec.base.is_some())
            && dataset.related.is_none())
            || (matches!(&dataset.request, OperationRequest::Update(_))
                && dataset.computed.is_empty()
                && dataset.related.is_none())
            || matches!(&dataset.request, OperationRequest::Refresh(spec)
                if spec.base.is_some() && spec.mode == RefreshMode::Full && dataset.related.is_some()))
            && dataset.evidence.is_none();
        for table in dataset.groups.values() {
            self.table(table, origin)?;
        }
        for computed in dataset.computed.values() {
            self.computed(computed, origin, history)?;
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
            if history {
                self.history(parent, origin)?;
            } else {
                self.release(parent, origin)?;
            }
        }
        Ok(())
    }

    pub(super) fn interpretation(
        &mut self,
        dataset: &DatasetPin,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        if !supported_format(dataset.format) {
            return Err(invalid_inventory("unsupported release format"));
        }
        // Historical binding checks read exact owning selections and native
        // schema metadata. They do not reconstruct each predecessor's rows.
        for table in dataset.groups.values() {
            self.object(&table.metadata, origin)?;
            if let Some(logical) = &table.logical_metadata {
                self.object(logical, origin)?;
            }
        }
        for computed in dataset.computed.values() {
            self.object(&computed.source_metadata, origin)?;
            if let Some(proof) = &computed.qualification {
                // Historical qualification still validates original executions
                // and their availability; preserve this explicit payload edge.
                self.qualification(proof, origin)?;
            }
            self.history(&computed.base_manifest, origin)?;
        }
        if let Some(related) = &dataset.related {
            // Historical input identities and coupled membership remain exact;
            // fresh full refresh does not reconstruct the prior join's values.
            for version in related.coverage.inputs.values().chain(
                related
                    .coverage
                    .generations
                    .values()
                    .flat_map(|seal| seal.members.values()),
            ) {
                self.history(&version.manifest, origin)?;
            }
            for table in related
                .input_sources
                .values()
                .chain(std::iter::once(&related.evidence))
            {
                self.object(&table.metadata, origin)?;
                if let Some(logical) = &table.logical_metadata {
                    self.object(logical, origin)?;
                }
            }
        }
        if let Some(parent) = &dataset.parent_manifest
            && (!dataset.computed.is_empty()
                || dataset.related.is_some()
                || dataset.evidence.is_some()
                || !matches!(&dataset.request, OperationRequest::Update(_)))
        {
            self.history(parent, origin)?;
        }
        Ok(())
    }

    pub(super) fn release_pin(
        &mut self,
        release: &ReleasePin,
        origin: &RetentionBlocker,
    ) -> Result<()> {
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
}
