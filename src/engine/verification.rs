//! Bounded, request-local verification of semantic input dependencies.

use super::*;

type Key = (Id, Id);

/// One sampling allowance across all tables and semantic dependencies in a request.
pub(super) struct Checks {
    pub report: VerificationReport,
    inspect_tables: bool,
    objects: storage::Objects,
    scalar_originals: Option<Box<backfill::origins::Checks>>,
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

    #[expect(
        clippy::too_many_arguments,
        reason = "One descriptor is associated with one selected source and field group"
    )]
    pub async fn scalar_origins(
        &mut self,
        origins: Option<&ObjectRef>,
        source: &TablePin,
        group: &TablePin,
        definition: &GroupDefinition,
        input_fields: &BTreeMap<String, i32>,
        native_field_ids: &BTreeMap<i32, i32>,
        budget: &Budget,
    ) -> Result<()> {
        if let Some(origins) = origins {
            self.scalar_originals
                .get_or_insert_with(Default::default)
                .check(
                    origins,
                    source,
                    group,
                    definition,
                    input_fields,
                    native_field_ids,
                    budget,
                )
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
                let release =
                    refresh::read_release_with_objects(&objects, &version, budget, &mut bytes, &memory)
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
