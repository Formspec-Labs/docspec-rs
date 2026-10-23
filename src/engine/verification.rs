//! Bounded, request-local verification of semantic input dependencies and operation transitions.

use super::*;
use crate::evidence::EvidenceSpec;
use backfill::{
    check_names, invalid_group,
    origins::{ScalarOriginBinding, ScalarOriginChecks},
    verification::{
        History, check_refreshed_groups, compatible_input_history, verify_computed_groups,
    },
};
use std::collections::BTreeSet;

type Key = (Id, Id);

/// One sampling allowance across all tables and semantic dependencies in a request.
pub(super) struct Checks {
    pub report: VerificationReport,
    inspect_tables: bool,
    objects: storage::Objects,
    scalar_originals: Option<Box<ScalarOriginChecks>>,
    known: BTreeMap<String, storage::CheckedFile>,
    _memory: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl Checks {
    #[cfg(test)]
    pub fn new(level: VerificationLevel) -> Self {
        Self::with_objects(level, storage::Objects::Local)
    }

    pub(super) fn with_objects(level: VerificationLevel, objects: storage::Objects) -> Self {
        Self {
            report: VerificationReport {
                level,
                data_files_checked: (level == VerificationLevel::Routine).then_some(0),
                ..VerificationReport::default()
            },
            inspect_tables: true,
            objects,
            scalar_originals: None,
            known: BTreeMap::new(),
            _memory: None,
        }
    }

    pub(super) fn routine(objects: storage::Objects) -> Self {
        Self::with_objects(VerificationLevel::Routine, objects)
    }

    pub(super) fn objects(&self) -> &storage::Objects {
        &self.objects
    }

    /// Components already passed their durable validation; check only their assembly.
    pub fn composition() -> Self {
        Self::composition_with_objects(storage::Objects::Local)
    }

    pub(super) fn composition_with_objects(objects: storage::Objects) -> Self {
        Self {
            inspect_tables: false,
            ..Self::with_objects(VerificationLevel::Routine, objects)
        }
    }

    /// Keep immutable metadata checks when moving from components to composition.
    pub fn compose(&mut self) {
        self.inspect_tables = false;
    }

    /// Validates only immutable supplemental evidence, never current corrections.
    pub async fn scalar_qualification(
        &mut self,
        binding: &ComputedGroup,
        source: &TablePin,
        group: &TablePin,
        budget: &Budget,
    ) -> Result<()> {
        if let Some(proof) = &binding.qualification {
            self.scalar_originals
                .get_or_insert_with(Default::default)
                .qualification(proof, binding, source, group, budget)
                .await?;
        }
        Ok(())
    }

    /// Shares supplemental metadata checks performed before prepared-value validation.
    pub async fn scalar_correspondence(
        &mut self,
        proof: &DependencyQualification,
        source: &TablePin,
        budget: &Budget,
    ) -> Result<()> {
        self.scalar_originals
            .get_or_insert_with(Default::default)
            .correspondence(proof, source, budget)
            .await
    }

    /// Shares canonical file descriptors between assembly and metadata qualification.
    pub async fn scalar_file(
        &mut self,
        completion: &backfill::FileCompletion,
        schema: &iceberg::spec::Schema,
        budget: &Budget,
    ) -> Result<Arc<Vec<iceberg::spec::DataFile>>> {
        self.scalar_originals
            .get_or_insert_with(Default::default)
            .file(completion, schema, budget)
            .await
    }

    /// Checks one immutable scalar-origins descriptor against its selected source and field group.
    pub async fn scalar_origins(
        &mut self,
        origins: Option<&ObjectRef>,
        binding: ScalarOriginBinding<'_>,
        budget: &Budget,
    ) -> Result<()> {
        if let Some(origins) = origins {
            self.scalar_originals
                .get_or_insert_with(Default::default)
                .check(origins, binding, budget)
                .await?;
        }
        Ok(())
    }

    pub fn deep(&self) -> bool {
        self.report.level == VerificationLevel::Deep
    }

    pub fn inspects_tables(&self) -> bool {
        self.inspect_tables
    }

    /// Trust immutable files from a freshly written table pin in the same request.
    #[cfg(all(not(test), not(feature = "test-util")))]
    pub async fn trust_pin(&mut self, pin: &TablePin, budget: &Budget) -> Result<()> {
        self.admit(budget)?;
        let mut bytes = self.known.values().fold(0usize, |sum, file| {
            sum.saturating_add(file.estimated_bytes())
        });
        let (table, inventory) = self.objects.open_snapshot(pin, budget).await?;
        let schemas = Arc::new(
            table
                .metadata()
                .schemas_iter()
                .map(|schema| storage::schema_identity(schema.as_struct()))
                .collect::<Result<std::collections::BTreeSet<_>>>()?,
        );
        let objects: BTreeMap<_, _> = pin
            .objects
            .iter()
            .map(|object| (object.path.as_str(), object))
            .collect();
        for file in &inventory.data {
            let object = objects
                .get(file.file_path())
                .ok_or_else(|| invalid("completed table has no data-file identity"))?;
            let known = storage::CheckedFile {
                object: (*object).clone(),
                schemas: schemas.clone(),
                rows: file.record_count(),
            };
            bytes = bytes
                .saturating_add(known.estimated_bytes())
                .saturating_sub(
                    self.known
                        .get(&object.path)
                        .map_or(0, storage::CheckedFile::estimated_bytes),
                );
            if bytes > budget.limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "checked-file inventory exceeds metadata allowance",
                ));
            }
            if let Some(previous) = self.known.insert(object.path.clone(), known)
                && (previous.object != **object || previous.rows != file.record_count())
            {
                return Err(invalid(
                    "completed tables disagree about shared file identity",
                ));
            }
        }
        Ok(())
    }

    /// Trust immutable files from the operation's already completed captured base.
    pub async fn trust(&mut self, release: &ReleasePin, budget: &Budget) -> Result<()> {
        self.admit(budget)?;
        let mut bytes = self.known.values().fold(0usize, |sum, file| {
            sum.saturating_add(file.estimated_bytes())
        });
        for pin in release
            .dataset
            .groups
            .values()
            .chain(
                release
                    .dataset
                    .related
                    .iter()
                    .map(|related| &related.evidence),
            )
            .chain(
                release
                    .dataset
                    .evidence
                    .iter()
                    .map(|evidence| &evidence.table),
            )
        {
            let (table, inventory) = self.objects.open_snapshot(pin, budget).await?;
            let schemas = Arc::new(
                table
                    .metadata()
                    .schemas_iter()
                    .map(|schema| storage::schema_identity(schema.as_struct()))
                    .collect::<Result<std::collections::BTreeSet<_>>>()?,
            );
            let objects: BTreeMap<_, _> = pin
                .objects
                .iter()
                .map(|object| (object.path.as_str(), object))
                .collect();
            for file in &inventory.data {
                let object = objects
                    .get(file.file_path())
                    .ok_or_else(|| invalid("completed table has no data-file identity"))?;
                let known = storage::CheckedFile {
                    object: (*object).clone(),
                    schemas: schemas.clone(),
                    rows: file.record_count(),
                };
                bytes = bytes
                    .saturating_add(known.estimated_bytes())
                    .saturating_sub(
                        self.known
                            .get(&object.path)
                            .map_or(0, storage::CheckedFile::estimated_bytes),
                    );
                if bytes > budget.limits.max_metadata_bytes {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "checked-file inventory exceeds metadata allowance",
                    ));
                }
                if let Some(previous) = self.known.insert(object.path.clone(), known)
                    && (previous.object != **object || previous.rows != file.record_count())
                {
                    return Err(invalid(
                        "completed tables disagree about shared file identity",
                    ));
                }
            }
        }
        Ok(())
    }

    fn admit(&mut self, budget: &Budget) -> Result<()> {
        if self._memory.is_none() {
            self._memory = Some(Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?));
        }
        Ok(())
    }

    pub async fn table(&mut self, pin: &TablePin, budget: &Budget) -> Result<()> {
        if !self.inspect_tables {
            return Ok(());
        }
        if self.deep() {
            Box::pin(verify_table(pin, budget)).await?;
        } else {
            self.admit(budget)?;
            let (inspected, known) = storage::inspect_table(
                &self.objects,
                pin,
                budget,
                3usize.saturating_sub(self.report.sampled_files),
                budget
                    .limits
                    .max_batch_bytes
                    .saturating_mul(3)
                    .saturating_sub(self.report.sampled_decoded_bytes),
                std::mem::take(&mut self.known),
                self._memory
                    .as_ref()
                    .ok_or_else(|| invalid("checked files lack admission"))?
                    .clone(),
            )
            .await?;
            self.known = known;
            *self.report.data_files_checked.get_or_insert(0) += inspected.files;
            self.report.sampled_files += inspected.sampled_files;
            self.report.sampled_rows += inspected.sampled_rows;
            self.report.sampled_decoded_bytes += inspected.sampled_bytes;
        }
        self.report.tables_checked += 1;
        self.report.rows_from_metadata = self
            .report
            .rows_from_metadata
            .checked_add(pin.rows)
            .ok_or_else(|| invalid("verified metadata counts overflow"))?;
        Ok(())
    }
}

/// Verifies top-level operation transition invariants (`Build`, `Update`, `Backfill`, `Refresh`,
/// `Schema`, `Evidence`) and every attached computed field group on `dataset`.
pub(crate) async fn verify_groups(
    dataset: &DatasetPin,
    budget: &Budget,
    checks: &mut Checks,
) -> Result<()> {
    if dataset.groups.len() != dataset.computed.len() + 1 {
        return Err(invalid_group(
            "dataset group count does not equal source plus computed group count",
        ));
    }
    check_names(dataset)?;
    let source = dataset
        .groups
        .get("source")
        .ok_or_else(|| invalid_group("dataset is missing source group"))?;
    let mut history = History::new(budget, checks.objects().clone())?;
    match &dataset.request {
        OperationRequest::Build(spec) => {
            verify_build_operation(dataset, spec, source, &mut history, budget).await?;
        }
        OperationRequest::Update(spec) => {
            verify_update_operation(dataset, spec, &mut history, budget, checks).await?;
        }
        OperationRequest::Backfill(spec) => {
            verify_backfill_operation(dataset, spec, source, &mut history).await?;
        }
        OperationRequest::Refresh(spec) => {
            verify_refresh_operation(dataset, spec, &mut history, budget).await?;
        }
        OperationRequest::Schema(spec) => {
            verify_schema_operation(dataset, spec, &mut history, budget).await?;
        }
        OperationRequest::Evidence(spec) => {
            verify_evidence_operation(dataset, spec, &mut history).await?;
        }
    }
    verify_computed_groups(dataset, source, &mut history, budget, checks).await
}

async fn verify_build_operation(
    dataset: &DatasetPin,
    spec: &RunSpec,
    source: &TablePin,
    history: &mut History<'_>,
    budget: &Budget,
) -> Result<()> {
    if dataset.related.is_some() || dataset.inputs.len() != dataset.request.inputs().len() {
        return Err(invalid_group(
            "build release has unexpected related metadata or mismatched input count",
        ));
    }
    if let Some(version) = &spec.base {
        let parent = dataset
            .parent_manifest
            .as_ref()
            .ok_or_else(|| invalid_group("rebuild release is missing parent_manifest"))?;
        let base = history.read(parent).await?;
        if base.dataset != dataset.dataset
            || base.related.is_some()
            || &base.version != version
            || base.version == dataset.version
        {
            return Err(invalid_group(
                "rebuild base dataset, related metadata, or version identity is invalid",
            ));
        }
        if dataset.boundary <= base.boundary
            || dataset.eligibility < base.eligibility
            || (dataset.definition != base.definition && dataset.eligibility == base.eligibility)
        {
            return Err(invalid_group(
                "rebuild boundary, eligibility, or definition progression is invalid",
            ));
        }
        let partition_mismatch = (spec.partition_by.is_empty()
            && dataset.partition_by != base.partition_by)
            || (!spec.partition_by.is_empty()
                && dataset.partition_by.len() != spec.partition_by.len());
        if partition_mismatch {
            return Err(invalid_group(
                "rebuild partition specification disagrees with base or request",
            ));
        }
        check_refreshed_groups(dataset, &base, parent)?;
        let base_source = base
            .groups
            .get("source")
            .ok_or_else(|| invalid_group("rebuild base is missing source group"))?;
        schema::verify_rebuild(base_source, source, dataset.format, budget).await?;
        if dataset.relationship != inputs::relationship(&dataset.request, Some(&base)) {
            return Err(inputs::invalid_relationship());
        }
    } else {
        let invalid_initial_build = !dataset.computed.is_empty()
            || dataset.parent_manifest.is_some()
            || (spec.partition_by.is_empty() && !dataset.partition_by.is_empty())
            || (!spec.partition_by.is_empty()
                && dataset.partition_by.len() != spec.partition_by.len());
        if invalid_initial_build {
            return Err(invalid_group(
                "initial build cannot have computed groups, parent_manifest, or mismatched partitions",
            ));
        }
        if dataset.relationship != inputs::relationship(&dataset.request, None) {
            return Err(inputs::invalid_relationship());
        }
    }
    Ok(())
}

async fn verify_update_operation(
    dataset: &DatasetPin,
    spec: &UpdateSpec,
    history: &mut History<'_>,
    budget: &Budget,
    checks: &mut Checks,
) -> Result<()> {
    if dataset.related.is_some() {
        return Err(invalid_group(
            "update release cannot carry related refresh metadata",
        ));
    }
    let parent = dataset
        .parent_manifest
        .as_ref()
        .ok_or_else(|| invalid_group("update release is missing parent_manifest"))?;
    let base = history.read(parent).await?;
    if base.dataset != spec.dataset
        || base.dataset != dataset.dataset
        || base.version != spec.base
        || base.version == dataset.version
    {
        return Err(invalid_group(
            "update base dataset or version does not match specification",
        ));
    }
    let mut expected_inputs = base.inputs.clone();
    let mut prior_cursor = base.clone();
    let mut visited = BTreeSet::from([prior_cursor.version.clone()]);
    loop {
        match &prior_cursor.request {
            OperationRequest::Update(_) => {
                for name in prior_cursor.request.inputs().keys() {
                    expected_inputs.remove(&prior_cursor.request.input_key(name));
                }
                break;
            }
            OperationRequest::Evidence(_)
            | OperationRequest::Backfill(_)
            | OperationRequest::Schema(_) => {
                let Some(prev_ref) = &prior_cursor.parent_manifest else {
                    break;
                };
                let prev = history.read(prev_ref).await?;
                if !visited.insert(prev.version.clone()) {
                    return Err(invalid_group(
                        "update ancestor history contains a version cycle",
                    ));
                }
                prior_cursor = prev;
            }
            OperationRequest::Build(_) | OperationRequest::Refresh(_) => break,
        }
    }
    let mut inherited_inputs = dataset.inputs.clone();
    for name in dataset.request.inputs().keys() {
        let key = dataset.request.input_key(name);
        if expected_inputs.contains_key(&key) || inherited_inputs.remove(&key).is_none() {
            return Err(invalid_group(
                "update request input key collides with or is missing from dataset inputs",
            ));
        }
    }
    if inherited_inputs != expected_inputs
        || base.related.is_some()
        || dataset.relationship != inputs::relationship(&dataset.request, Some(&base))
        || base.eligibility != dataset.eligibility
        || dataset.boundary <= base.boundary
        || dataset.definition != base.definition
    {
        return Err(invalid_group(
            "update release inputs, relationship, eligibility, boundary, or definition disagrees with base",
        ));
    }
    write::verify_update(dataset, &base, spec, budget, checks).await?;
    check_refreshed_groups(dataset, &base, parent)
}

async fn verify_backfill_operation(
    dataset: &DatasetPin,
    spec: &BackfillSpec,
    source: &TablePin,
    history: &mut History<'_>,
) -> Result<()> {
    let selected = dataset
        .computed
        .get(spec.group.as_str())
        .ok_or_else(|| invalid_group("backfill release is missing target computed group"))?;
    if selected.request != dataset.request || selected.definition != spec.group_definition() {
        return Err(invalid_group(
            "backfill target group request or definition disagrees with backfill specification",
        ));
    }
    let base = history.base(selected).await?;
    let parent = history
        .read(
            dataset
                .parent_manifest
                .as_ref()
                .ok_or_else(|| invalid_group("backfill release is missing parent_manifest"))?,
        )
        .await?;
    if parent.dataset != dataset.dataset
        || parent.version == dataset.version
        || parent.groups.get("source") != Some(source)
        || parent.inputs != dataset.inputs
    {
        return Err(invalid_group(
            "backfill parent dataset, version, source group, or inputs disagree with release",
        ));
    }
    if parent.related != dataset.related
        || base.related != dataset.related
        || parent.relationship != dataset.relationship
        || base.relationship != dataset.relationship
    {
        return Err(invalid_group(
            "backfill parent or base related metadata or source relationship disagrees with release",
        ));
    }
    if parent.boundary != dataset.boundary
        || parent.definition != dataset.definition
        || parent.eligibility != dataset.eligibility
        || base.boundary != dataset.boundary
        || base.eligibility != dataset.eligibility
    {
        return Err(invalid_group(
            "backfill parent or base boundary, definition, or eligibility disagrees with release",
        ));
    }
    if parent.groups.get(spec.group.as_str()) != base.groups.get(spec.group.as_str())
        || parent.computed.get(spec.group.as_str()) != base.computed.get(spec.group.as_str())
        || parent.partition_by != dataset.partition_by
        || base.partition_by != dataset.partition_by
    {
        return Err(invalid_group(
            "backfill target group state or partition_by diverged between base and parent",
        ));
    }
    compatible_input_history(&base, &parent, history).await?;
    let mut inherited = dataset.computed.clone();
    inherited.remove(spec.group.as_str());
    let mut expected = parent.computed.clone();
    expected.remove(spec.group.as_str());
    if !corrections::inherits_groups(&inherited, &expected) {
        return Err(invalid_group(
            "backfill release does not preserve sibling computed group bindings from parent",
        ));
    }
    for name in inherited.keys() {
        if dataset.groups.get(name) != parent.groups.get(name) {
            return Err(invalid_group(
                "backfill release modified a sibling group table pin",
            ));
        }
    }
    Ok(())
}

async fn verify_refresh_operation(
    dataset: &DatasetPin,
    spec: &RefreshSpec,
    history: &mut History<'_>,
    budget: &Budget,
) -> Result<()> {
    let related = dataset
        .related
        .as_ref()
        .ok_or_else(|| invalid_group("refresh release is missing related metadata"))?;
    if &related.request != spec
        || related.coverage.inputs != spec.inputs()
        || !dataset.inputs.is_empty()
        || !dataset.partition_by.is_empty()
    {
        return Err(invalid_group(
            "refresh release related specification, coverage inputs, or partition_by is invalid",
        ));
    }
    if let Some(version) = &spec.base {
        let parent = dataset
            .parent_manifest
            .as_ref()
            .ok_or_else(|| invalid_group("refresh release with base is missing parent_manifest"))?;
        let base = history.read(parent).await?;
        if base.dataset != dataset.dataset
            || &base.version != version
            || base.version == dataset.version
            || dataset.boundary <= base.boundary
            || dataset.eligibility < base.eligibility
            || (dataset.definition != base.definition && dataset.eligibility == base.eligibility)
            || dataset.relationship != inputs::relationship(&dataset.request, Some(&base))
            || base
                .related
                .as_ref()
                .is_some_and(|previous| !previous.request.transform.same_roles(&spec.transform))
        {
            return Err(invalid_group(
                "refresh base version, progression, relationship, or transform roles disagree with release",
            ));
        }
        if spec.mode == RefreshMode::Incremental {
            incremental::qualify(spec, &base, dataset.eligibility, budget).await?;
        }
        check_refreshed_groups(dataset, &base, parent)?;
    } else if spec.mode == RefreshMode::Incremental
        || !dataset.computed.is_empty()
        || dataset.parent_manifest.is_some()
        || dataset.relationship != inputs::relationship(&dataset.request, None)
    {
        return Err(invalid_group(
            "initial refresh cannot be incremental or have computed groups, parent_manifest, or invalid relationship",
        ));
    }
    Ok(())
}

async fn verify_schema_operation(
    dataset: &DatasetPin,
    spec: &SchemaSpec,
    history: &mut History<'_>,
    budget: &Budget,
) -> Result<()> {
    let parent = dataset
        .parent_manifest
        .as_ref()
        .ok_or_else(|| invalid_group("schema release is missing parent_manifest"))?;
    let base = history.read(parent).await?;
    if base.partition_by != dataset.partition_by {
        return Err(invalid_group(
            "schema transition cannot modify dataset partition_by",
        ));
    }
    schema::verify_transition(spec, &base, dataset, budget).await?;
    check_refreshed_groups(dataset, &base, parent)
}

async fn verify_evidence_operation(
    dataset: &DatasetPin,
    spec: &EvidenceSpec,
    history: &mut History<'_>,
) -> Result<()> {
    let parent = dataset
        .parent_manifest
        .as_ref()
        .ok_or_else(|| invalid_group("evidence release is missing parent_manifest"))?;
    let base = history.read(parent).await?;
    let mut inherited_inputs = dataset.inputs.clone();
    for name in dataset.request.inputs().keys() {
        let key = dataset.request.input_key(name);
        if base.inputs.contains_key(&key) || inherited_inputs.remove(&key).is_none() {
            return Err(invalid_group(
                "evidence request input key collides with base or is missing from dataset inputs",
            ));
        }
    }
    if base.dataset != dataset.dataset
        || base.version != spec.base
        || inherited_inputs != base.inputs
        || dataset.groups != base.groups
        || !corrections::inherits_groups(&dataset.computed, &base.computed)
    {
        return Err(invalid_group(
            "evidence release dataset, base version, inputs, or groups disagree with base",
        ));
    }
    if dataset.related != base.related
        || dataset.relationship != base.relationship
        || dataset.boundary != base.boundary
        || dataset.eligibility != base.eligibility
        || dataset.definition != base.definition
        || dataset.partition_by != base.partition_by
    {
        return Err(invalid_group(
            "evidence release modified related metadata, relationship, boundary, eligibility, definition, or partition_by",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Visiting,
    Verified,
}

struct Node {
    release: ReleasePin,
    state: State,
}

pub(super) struct DependencyGraph {
    nodes: BTreeMap<Key, Node>,
    _memory: Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl DependencyGraph {
    pub fn datasets(&self) -> std::collections::BTreeSet<Id> {
        self.nodes
            .values()
            .flat_map(|node| {
                std::iter::once(node.release.dataset.dataset.clone()).chain(
                    node.release
                        .dataset
                        .related
                        .iter()
                        .flat_map(|related| related.coverage.families.keys().cloned()),
                )
            })
            .collect()
    }

    pub fn ancestor_coverages<'a>(
        &'a self,
        release: &'a ReleasePin,
    ) -> impl Iterator<Item = &'a InputCoverage> {
        self.nodes
            .values()
            .filter(move |node| {
                node.release.dataset.dataset != release.dataset.dataset
                    || node.release.id != release.id
            })
            .filter_map(|node| {
                node.release
                    .dataset
                    .related
                    .as_ref()
                    .map(|related| &related.coverage)
            })
    }
}

enum Work {
    Enter(VersionRef),
    Finish(Key),
}

pub(super) async fn verify_all(
    roots: &[&ReleasePin],
    budget: &Budget,
    checks: &mut Checks,
) -> Result<()> {
    Box::pin(walk(roots, budget, Some(checks)))
        .await
        .map(|_| ())
}

pub(super) async fn check_available(release: &ReleasePin, budget: &Budget) -> Result<()> {
    available_graph(release, budget).await.map(|_| ())
}

pub(super) async fn available_graph(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<DependencyGraph> {
    Box::pin(walk(&[release], budget, None)).await
}

async fn walk(
    roots: &[&ReleasePin],
    budget: &Budget,
    mut checks: Option<&mut Checks>,
) -> Result<DependencyGraph> {
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut nodes: BTreeMap<Key, Node> = BTreeMap::new();
    let mut expected = BTreeMap::new();
    let mut work = Vec::new();
    let mut bytes = 0usize;
    for release in roots {
        let key = (release.dataset.dataset.clone(), release.id.clone());
        if let Some(previous) = expected.insert(key, *release)
            && previous != *release
        {
            return Err(invalid("the same root identity has conflicting pins"));
        }
        charge(&mut bytes, reference_bytes(&release.version_ref()), budget)?;
        work.push(Work::Enter(release.version_ref()));
    }
    while let Some(task) = work.pop() {
        match task {
            Work::Enter(version) => {
                let key = (version.dataset.clone(), version.version.clone());
                if let Some(node) = nodes.get(&key) {
                    if node.release.version_ref() != version {
                        return Err(invalid(
                            "a dependency identity resolves to conflicting manifests",
                        ));
                    }
                    if node.state == State::Visiting {
                        return Err(invalid("related input dependencies contain a cycle"));
                    }
                    continue;
                }
                let objects = match checks.as_ref().map(|c| c.objects()) {
                    Some(objects) if objects.is_remote() => objects.clone(),
                    _ => storage::Objects::for_path(&version.manifest.path),
                };
                objects.register_query_store(budget.runtime_env())?;
                charge(&mut bytes, std::mem::size_of::<Node>(), budget)?;
                let release = refresh::read_release_with_objects(
                    &objects, &version, budget, &mut bytes, &memory,
                )
                .await?;
                if let Some(root) = expected.get(&key)
                    && (root.manifest != release.manifest
                        || root.dataset != release.dataset
                        || root.id != release.id)
                {
                    return Err(Error::refusal(
                        "release_mismatch",
                        "release manifest does not match its pin",
                    ));
                }
                // Visit only semantic inputs. Composition parents and optional
                // change history retain their existing shallow consistency checks.
                work.push(Work::Finish(key.clone()));
                if let Some(related) = &release.dataset.related {
                    for child in related.coverage.inputs.values() {
                        charge(&mut bytes, reference_bytes(child), budget)?;
                        work.push(Work::Enter(child.clone()));
                    }
                }
                nodes.insert(
                    key,
                    Node {
                        release,
                        state: State::Visiting,
                    },
                );
            }
            Work::Finish(key) => {
                let node = nodes
                    .get(&key)
                    .ok_or_else(|| invalid("missing verification node"))?;
                let node_objects = storage::Objects::for_path(&node.release.manifest.path);
                let mut composition = Checks::composition_with_objects(node_objects.clone());
                let checks = if let Some(checks) = checks.as_deref_mut() {
                    if !checks.objects.is_remote() {
                        checks.objects = node_objects;
                    }
                    checks
                } else {
                    composition
                        .objects()
                        .check_available(release_objects(&node.release), budget)
                        .await?;
                    &mut composition
                };
                verify_local_release(&node.release, budget, checks).await?;
                if let Some(related) = &node.release.dataset.related {
                    let inputs: BTreeMap<_, _> = related
                        .coverage
                        .inputs
                        .iter()
                        .map(|(dataset, version)| {
                            let child = nodes
                                .get(&(dataset.clone(), version.version.clone()))
                                .ok_or_else(|| invalid("missing semantic dependency"))?;
                            if child.state != State::Verified
                                || child.release.version_ref() != *version
                            {
                                return Err(invalid(
                                    "semantic dependency is not independently verified",
                                ));
                            }
                            Ok((dataset.clone(), &child.release))
                        })
                        .collect::<Result<_>>()?;
                    Box::pin(refresh::verify_related_with_inputs(
                        node.release
                            .dataset
                            .groups
                            .get("source")
                            .ok_or_else(|| invalid("missing source"))?,
                        related,
                        &inputs,
                        budget,
                        checks,
                    ))
                    .await?;
                }
                nodes
                    .get_mut(&key)
                    .ok_or_else(|| invalid("missing verification node"))?
                    .state = State::Verified;
            }
        }
        tokio::task::consume_budget().await;
    }
    Ok(DependencyGraph {
        nodes,
        _memory: memory,
    })
}

fn reference_bytes(version: &VersionRef) -> usize {
    std::mem::size_of::<Work>()
        .saturating_add(version.dataset.as_str().len())
        .saturating_add(version.version.as_str().len())
        .saturating_add(version.manifest.path.len())
        .saturating_add(version.manifest.digest.len())
        .saturating_add(version.manifest.owner.as_str().len())
}
fn charge(bytes: &mut usize, added: usize, budget: &Budget) -> Result<()> {
    *bytes = bytes.saturating_add(added);
    if *bytes > budget.limits.max_metadata_bytes {
        return Err(Error::refusal(
            "metadata_capacity",
            "related release graph exceeds its aggregate metadata allowance",
        ));
    }
    Ok(())
}
fn invalid(reason: &str) -> Error {
    Error::refusal("invalid_dependency_graph", reason)
}
