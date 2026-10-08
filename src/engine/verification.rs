//! Bounded, request-local verification of semantic input dependencies.

use super::*;

type Key = (Id, Id);

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

pub(super) async fn verify_all(roots: &[&ReleasePin], budget: &Budget) -> Result<()> {
    Box::pin(walk(roots, budget, true)).await.map(|_| ())
}

pub(super) async fn check_available(release: &ReleasePin, budget: &Budget) -> Result<()> {
    available_graph(release, budget).await.map(|_| ())
}

pub(super) async fn available_graph(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<DependencyGraph> {
    Box::pin(walk(&[release], budget, false)).await
}

async fn walk(roots: &[&ReleasePin], budget: &Budget, full: bool) -> Result<DependencyGraph> {
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
                charge(&mut bytes, std::mem::size_of::<Node>(), budget)?;
                let release = refresh::read_release(&version, budget, &mut bytes, &memory).await?;
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
                if !full {
                    storage::check_available(release_objects(&node.release), budget).await?;
                    schema::check_available(&node.release.dataset, budget).await?;
                    backfill::verify_binding_dependencies(&node.release.dataset, budget).await?;
                    nodes
                        .get_mut(&key)
                        .ok_or_else(|| invalid("missing verification node"))?
                        .state = State::Verified;
                    continue;
                }
                verify_local_release(&node.release, budget).await?;
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
