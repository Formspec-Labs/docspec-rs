//! Protect exact dependencies before retaining an operation request.

use super::*;
use crate::catalog::View;
use std::collections::BTreeSet;
use tokio::sync::OwnedSemaphorePermit;

#[cfg(all(test, feature = "test-util"))]
mod tests;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct AdmissionDependencies {
    pub base: Option<VersionRef>,
    pub coverage: Option<InputCoverage>,
    pub inputs: BTreeMap<String, ObjectRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_source: Option<TablePin>,
}

pub(super) struct PreparedAdmission {
    boundary: String,
    pub dependencies: AdmissionDependencies,
    pub protections: super::retention::owners::ReferenceProtections,
    _memory: OwnedSemaphorePermit,
}

impl PreparedAdmission {
    pub(super) fn datasets(&self, consumer: &Id) -> BTreeSet<Id> {
        let mut datasets = BTreeSet::from([consumer.clone()]);
        if let Some(coverage) = &self.dependencies.coverage {
            datasets.extend(coverage.inputs.keys().cloned());
            datasets.extend(coverage.generations.keys().cloned());
        }
        datasets
    }

    pub(super) fn is_current(&self, view: &View) -> Result<bool> {
        Ok(self.boundary == view.table.metadata_location_result()?)
    }

    pub(super) fn records(
        &self,
        view: &View,
        spec: &OperationRequest,
        records: &mut Vec<(String, serde_json::Value)>,
    ) -> Result<()> {
        records.push(record(
            dependencies_key(spec.operation()),
            &self.dependencies,
        )?);
        // These are independently retained roots. Bind each in the same guarded
        // commit, reusing the owner references acquired before consumer locking.
        let bindings = records
            .iter()
            .filter(|(key, _)| {
                key.starts_with("operation/")
                    || key.starts_with("request/")
                    || key.starts_with("request-dependencies/")
                    || key.starts_with("backfill/")
            })
            .map(|(key, value)| self.protections.binding(spec.dataset(), view, key, value))
            .collect::<Result<Vec<_>>>()?;
        records.extend(bindings);
        Ok(())
    }
}

pub(super) fn dependencies_key(operation: &Id) -> String {
    format!("request-dependencies/{operation}")
}

impl Engine {
    pub(super) async fn admission_memory(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<OwnedSemaphorePermit> {
        self.inner.lifecycle.rows(view).await?.reserve_decoded(
            &self.inner.lifecycle.budget,
            &[
                &dependencies_key(operation),
                &format!("request/{operation}"),
            ],
        )
    }

    pub(super) async fn admitted_dependencies(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<Option<AdmissionDependencies>> {
        let rows = self.inner.lifecycle.rows(view).await?;
        let key = dependencies_key(operation);
        let Some((revision, _, dependencies)) = rows.get_record::<AdmissionDependencies>(&key)?
        else {
            return Ok(None);
        };
        let (request_revision, _, request) = rows
            .get_record::<OperationRequest>(&format!("request/{operation}"))?
            .ok_or_else(invalid)?;
        if request_revision != revision
            || rows.revision(&format!("operation/{operation}")) != Some(revision)
            || request.operation() != operation
            || &crate::catalog::lifecycle_ident(request.dataset()) != view.table.identifier()
            || dependencies.base.as_ref().is_some_and(|base| {
                &base.dataset != request.dataset()
                    || base.manifest.owner != base.dataset
                    || request
                        .base()
                        .is_some_and(|version| version != &base.version)
            })
            || (request.base().is_some() && dependencies.base.is_none())
        {
            return Err(invalid());
        }
        match (&request, &dependencies.coverage) {
            (OperationRequest::Refresh(refresh), Some(coverage))
                if coverage.inputs == refresh.inputs() => {}
            (OperationRequest::Refresh(_), _) | (_, Some(_)) => return Err(invalid()),
            _ => {}
        }
        if dependencies.update_source.as_ref().is_some_and(|source| {
            !matches!(&request, OperationRequest::Update(_))
                || source.metadata.owner != *request.dataset()
        }) {
            return Err(invalid());
        }
        let inputs = request.inputs();
        if dependencies.inputs.iter().any(|(name, object)| {
            inputs
                .get(name)
                .is_none_or(|input| input.digest != object.digest)
        }) {
            return Err(invalid());
        }
        // This record kind was introduced with atomic protection bindings; its
        // presence with no binding is corruption, not an older legacy format.
        self.retained_protections(view, &key)
            .await?
            .ok_or_else(invalid)?;
        Ok(Some(dependencies))
    }

    /// Caller releases all dataset locks before acquiring this protection.
    pub(super) async fn protect_admission(
        &self,
        spec: &OperationRequest,
        boundary: String,
        base: Option<ReleasePin>,
        update_source: Option<TablePin>,
        mut memory: OwnedSemaphorePermit,
    ) -> Result<PreparedAdmission> {
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        let coverage = if let OperationRequest::Refresh(refresh) = spec {
            let (coverage, capture_memory) = self
                .capture_inputs_with_memory(&refresh.inputs(), true)
                .await?;
            memory.merge(capture_memory);
            Some(coverage)
        } else {
            None
        };
        let mut dependencies = AdmissionDependencies {
            base: base.as_ref().map(ReleasePin::version_ref),
            coverage,
            inputs: BTreeMap::new(),
            update_source,
        };
        let mut releases = base.into_iter().collect::<Vec<_>>();
        let mut release_bytes = inputs::metadata_size(&releases, limit)?;
        let mut versions = BTreeMap::new();
        if let Some(coverage) = &dependencies.coverage {
            for version in coverage.inputs.values().chain(
                coverage
                    .generations
                    .values()
                    .flat_map(|seal| seal.members.values()),
            ) {
                let key = (version.dataset.clone(), version.version.clone());
                if versions
                    .insert(key, version.clone())
                    .is_some_and(|old| old != *version)
                {
                    return Err(inputs::invalid_relationship());
                }
            }
        }
        inputs::metadata_size(&versions.values().collect::<Vec<_>>(), limit)?;
        for version in versions.into_values() {
            if dependencies.base.as_ref() == Some(&version) {
                continue;
            }
            let (release, charge) = {
                let lock = self.lock(&version.dataset).await;
                let _guard = lock.lock().await;
                let view = self.inner.lifecycle.open(&version.dataset).await?;
                self.check_operation_available(&view, &version.version)
                    .await?;
                let charge = self
                    .inner
                    .lifecycle
                    .rows(&view)
                    .await?
                    .reserve_decoded(budget, &[&format!("result/{}", version.version)])?;
                (
                    self.resolve_release(&view, &version.dataset, &version.version)
                        .await?,
                    charge,
                )
            };
            if release.version_ref() != version {
                return Err(inputs::invalid_relationship());
            }
            release_bytes = release_bytes.saturating_add(inputs::metadata_size(&release, limit)?);
            if release_bytes > limit {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "admitted input roots exceed metadata allowance",
                ));
            }
            memory.merge(charge);
            releases.push(release);
        }
        let managed = self.inner.root.join("objects");
        for (name, input) in spec.inputs() {
            // Resolve filesystem aliases once to recognize managed files. A
            // missing external locator can still reuse an existing completed
            // phase; only the path that actually needs copying must open it.
            let path =
                std::fs::canonicalize(&input.path).unwrap_or_else(|_| PathBuf::from(&input.path));
            if let Ok(relative) = path.strip_prefix(&managed) {
                let owner = relative
                    .components()
                    .next()
                    .and_then(|part| {
                        if let std::path::Component::Normal(owner) = part {
                            owner.to_str()
                        } else {
                            None
                        }
                    })
                    .ok_or_else(inputs::invalid_relationship)?;
                let object = self
                    .qualified_object(&Id::new(owner)?, &path.to_string_lossy(), &input.digest)
                    .await?;
                memory.merge(
                    budget.reserve(
                        inputs::metadata_size(&(&name, &object), limit)?
                            .saturating_mul(8)
                            .saturating_add(4096),
                    )?,
                );
                dependencies.inputs.insert(name, object);
            }
        }
        let dependency_bytes = inputs::metadata_size(&dependencies, limit)?;
        if release_bytes.saturating_add(dependency_bytes) > limit {
            return Err(Error::refusal(
                "metadata_capacity",
                "admitted input roots exceed metadata allowance",
            ));
        }
        // Decoding is finished. Do not keep a temporary decoding allowance while
        // the shared collector admits its own temporary metadata work.
        let pins_memory = memory
            .split(release_bytes.saturating_mul(4))
            .ok_or_else(|| Error::refusal("metadata_capacity", "input pin allowance exhausted"))?;
        let retained = memory
            .split(
                dependency_bytes
                    .saturating_mul(4)
                    .saturating_add(boundary.len()),
            )
            .ok_or_else(|| {
                Error::refusal("metadata_capacity", "input dependency allowance exhausted")
            })?;
        drop(memory);
        let mut protections = self.protect_releases(spec.dataset(), &releases).await?;
        drop(releases);
        drop(pins_memory);
        if !dependencies.inputs.is_empty() {
            let objects = dependencies.inputs.values().cloned().collect::<Vec<_>>();
            protections.merge(self.protect_objects(spec.dataset(), &objects).await?)?;
        }
        if let Some(source) = &dependencies.update_source {
            protections.merge(
                self.protect_objects(spec.dataset(), &source.objects)
                    .await?,
            )?;
        }
        Ok(PreparedAdmission {
            boundary,
            dependencies,
            protections,
            _memory: retained,
        })
    }
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_admission_dependencies",
        "retained input capture differs from its admitted request",
    )
}
