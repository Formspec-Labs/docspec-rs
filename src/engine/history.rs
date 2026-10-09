//! Accepted selection order, independent of composition parents and optional change files.

use super::*;
use crate::catalog::{SnapshotRows, View};

impl Engine {
    /// Proves forward coverage using one captured accepted-history snapshot.
    ///
    /// This reads lifecycle metadata only. It does not establish payload availability
    /// or whether the source has acquired every upstream record. Composition-only
    /// selections count as accepted versions, even when source values are unchanged.
    ///
    /// # Errors
    /// Refuses backward or unproven coverage, missing or malformed history, mismatched
    /// identities, and history exceeding the configured metadata allowance.
    pub async fn prove_succession(
        &self,
        prior: &VersionRef,
        target: &VersionRef,
    ) -> Result<SuccessionProof> {
        let memory = self.request_memory(&(prior, target))?;
        let prior = prior.clone();
        let target = target.clone();
        self.admit_work(memory, move |engine| async move {
            engine.prove_succession_admitted(&prior, &target).await
        })
        .await
    }

    async fn prove_succession_admitted(
        &self,
        prior: &VersionRef,
        target: &VersionRef,
    ) -> Result<SuccessionProof> {
        if prior.dataset != target.dataset {
            return Err(invalid("coverage endpoints belong to different datasets"));
        }
        let lock = self.lock(&prior.dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(&prior.dataset).await?;
        self.follows(&view, prior, target).await
    }

    pub(super) async fn follows(
        &self,
        view: &View,
        prior: &VersionRef,
        target: &VersionRef,
    ) -> Result<SuccessionProof> {
        if prior.dataset != target.dataset {
            return Err(invalid("coverage endpoints belong to different datasets"));
        }
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let _metadata = lifecycle.budget.reserve(limit)?;
        // Keep this exact state and its reservation alive across yields. Other
        // datasets may replace the shared cache without making us replay history.
        let rows = lifecycle.rows(view).await?;
        let (selection_revision, head) = read_head(&rows, view.revision)?
            .ok_or_else(|| Error::refusal("no_release", "dataset has no accepted selection"))?;
        if head.dataset.dataset != prior.dataset {
            return Err(invalid("selected head belongs to a different dataset"));
        }
        let observed_head = head.version_ref();
        let mut current = observed_head.clone();
        let mut previous_revision = None;
        let mut previous_boundary = None;
        let mut previous_eligibility = None;
        let mut target_at = None;
        let mut prior_at = None;
        let mut steps = 0;
        let mut bytes = 0usize;
        loop {
            // Serialized bytes bound variable-length identities. This separate
            // in-memory lower bound also caps visited links without retaining a set.
            if steps >= limit / std::mem::size_of::<Succession>() {
                return Err(history_capacity());
            }
            let (revision, charge, link) = read_link(&rows, view.revision, &current)?;
            if steps == 0 {
                check_selected_link(selection_revision, &head, revision, &link)?;
            }
            bytes = bytes.saturating_add(charge);
            if bytes > limit {
                return Err(history_capacity());
            }
            if previous_revision.is_some_and(|child| revision >= child)
                || previous_boundary.is_some_and(|child| link.boundary > child)
                || previous_eligibility.is_some_and(|child| link.eligibility > child)
            {
                return Err(invalid(
                    "accepted predecessor revisions or coverage are inconsistent",
                ));
            }
            // All endpoints, including equal endpoints, must occur on this head's
            // chain. Finding an isolated target-to-prior branch is insufficient.
            for endpoint in [prior, target] {
                if current.version == endpoint.version && current != *endpoint {
                    return Err(invalid(
                        "accepted version has a different manifest identity",
                    ));
                }
            }
            if current == *target {
                target_at = Some(steps);
            }
            if current == *prior {
                prior_at = Some(steps);
            }
            if let (Some(target_at), Some(prior_at)) = (target_at, prior_at) {
                if prior_at < target_at {
                    return Err(Error::refusal(
                        "coverage_regression",
                        "target precedes the prior coverage on accepted history",
                    ));
                }
                return Ok(SuccessionProof {
                    prior: prior.clone(),
                    target: target.clone(),
                    observed_head,
                    observed_revision: view.revision,
                    selections_advanced: prior_at - target_at,
                    selections_behind: target_at,
                });
            }
            let Some(predecessor) = link.predecessor else {
                return Err(Error::refusal(
                    "unproven_succession",
                    "both endpoints must occur on the observed accepted selection chain",
                ));
            };
            current = predecessor;
            previous_revision = Some(revision);
            previous_boundary = Some(link.boundary);
            previous_eligibility = Some(link.eligibility);
            steps += 1;
            tokio::task::consume_budget().await;
        }
    }

    /// Called after guards pass while holding the output and related source/family locks.
    pub(super) async fn select(
        &self,
        view: &View,
        authority: Authority,
        release: &Retained<ReleasePin>,
    ) -> Result<()> {
        let progress = if let Some(related) = &release.dataset.related {
            Some(
                Box::pin(self.guard_inputs(&related.coverage, None, RefreshPolicy::Captured))
                    .await?,
            )
        } else {
            None
        };
        self.select_with_progress(view, authority, release, progress.as_ref())
            .await
    }

    pub(super) async fn select_with_progress(
        &self,
        view: &View,
        mut authority: Authority,
        release: &Retained<ReleasePin>,
        progress: Option<&BTreeMap<Id, SuccessionProof>>,
    ) -> Result<()> {
        let lifecycle = &self.inner.lifecycle;
        self.check_not_abandoned(view, &release.id).await?;
        if authority.selected.as_ref() != authority.release.as_ref().map(|old| &old.dataset) {
            return Err(invalid("selected dataset and release disagree"));
        }
        let key = format!("succession/{}", release.id);
        let rows = lifecycle.rows(view).await?;
        if rows.get_record::<serde_json::Value>(&key)?.is_some() {
            return Err(invalid(
                "an accepted version's succession cannot be replaced",
            ));
        }
        let predecessor = authority.release.as_ref().map(ReleasePin::version_ref);
        let head = read_head(&rows, view.revision)?;
        if head.as_ref().map(|(_, value)| value.version_ref()) != predecessor {
            return Err(invalid("captured authority differs from selected head"));
        }
        if let Some((selected_revision, head)) = head {
            let previous = head.version_ref();
            if previous.dataset != release.dataset.dataset || previous.version == release.id {
                return Err(invalid(
                    "selection cannot replace another dataset or itself",
                ));
            }
            // Selection keeps the required ordering chain intact, even when
            // optional change accelerators or old payloads are no longer needed.
            let (revision, _, link) = read_link(&rows, view.revision, &previous)?;
            check_selected_link(selected_revision, &head, revision, &link)?;
        }
        // The accepted predecessor may have changed since this request was
        // admitted. Inherit its exact retained scope from this guarded view.
        // Unknown older provenance must not borrow the candidate's protection.
        let mut succession_protections = release
            .protections
            .as_ref()
            .map(ReferenceProtections::duplicate)
            .transpose()?;
        if let Some(previous) = &authority.release {
            let previous_key = format!("result/{}", previous.id);
            let previous_scope = self.retained_protections(view, &previous_key).await?;
            if previous_scope.is_some() {
                let _memory = rows.reserve_decoded(&lifecycle.budget, &[&previous_key])?;
                if rows
                    .get_record::<ReleasePin>(&previous_key)?
                    .map(|(_, _, value)| value)
                    .as_ref()
                    != Some(previous)
                {
                    return Err(invalid("selected release differs from its retained result"));
                }
            }
            ReferenceProtections::inherit(&mut succession_protections, previous_scope.as_ref())?;
        }
        let link = Succession {
            selected: release.version_ref(),
            predecessor,
            boundary: release.dataset.boundary,
            eligibility: release.dataset.eligibility,
        };
        if let OperationRequest::Schema(spec) = &release.dataset.request {
            if authority.definition != spec.definition
                || release.dataset.definition != spec.next_definition
            {
                return Err(invalid(
                    "schema selection differs from its writer definitions",
                ));
            }
            authority.definition = spec.next_definition.clone();
        }
        authority.selected = Some(release.dataset.clone());
        authority.release = Some(release.value.clone());
        authority.selection_revision = Some(
            view.revision
                .checked_add(1)
                .ok_or_else(|| Error::refusal("revision_exhausted", "revision overflow"))?,
        );
        let mut records = vec![record("authority", authority)?];
        records.extend(completion_records(
            &release.dataset.dataset,
            view,
            &format!("result/{}", release.id),
            &release.value,
            release.protections.as_ref(),
        )?);
        records.extend(completion_records(
            &release.dataset.dataset,
            view,
            &key,
            &link,
            succession_protections.as_ref(),
        )?);
        if let Some(progress) = progress {
            records.push(record(format!("input-progress/{}", release.id), progress)?);
        }
        lifecycle.commit(view, records).await?;
        Ok(())
    }
}

// Decode only identity fields; traversing history does not materialize every
// release's native file inventory, computed recipe or optional change descriptor.
#[derive(Deserialize)]
struct SelectedRelease {
    id: Id,
    manifest: ObjectRef,
    dataset: SelectedDataset,
    verified: bool,
}
#[derive(PartialEq, Eq, Deserialize)]
struct SelectedDataset {
    dataset: Id,
    version: Id,
    boundary: u64,
    eligibility: u64,
}
#[derive(Default, Deserialize)]
struct SelectedHead {
    selected: Option<SelectedDataset>,
    release: Option<SelectedRelease>,
    selection_revision: Option<i64>,
}
impl SelectedRelease {
    fn version_ref(&self) -> VersionRef {
        VersionRef {
            dataset: self.dataset.dataset.clone(),
            version: self.id.clone(),
            manifest: self.manifest.clone(),
        }
    }
}

fn read_head(
    rows: &SnapshotRows,
    observed_revision: i64,
) -> Result<Option<(i64, SelectedRelease)>> {
    let Some((authority_revision, _, head)): Option<(_, _, SelectedHead)> =
        rows.get_record("authority")?
    else {
        return Ok(None);
    };
    match (head.selected, head.release, head.selection_revision) {
        (None, None, None) => Ok(None),
        (Some(selected), Some(release), Some(revision))
            if revision > 0
                && revision <= authority_revision
                && authority_revision <= observed_revision
                && selected == release.dataset
                && release.dataset.version == release.id
                && release.verified =>
        {
            Ok(Some((revision, release)))
        }
        _ => Err(invalid(
            "selected head has inconsistent commit or version identities",
        )),
    }
}

fn read_link(
    rows: &SnapshotRows,
    observed_revision: i64,
    expected: &VersionRef,
) -> Result<(i64, usize, Succession)> {
    let missing = || {
        Error::refusal(
            "unavailable_succession",
            "required accepted-history evidence is missing",
        )
    };
    let (revision, bytes, value): (_, _, serde_json::Value) = rows
        .get_record(&format!("succession/{}", expected.version))?
        .ok_or_else(missing)?;
    let link: Succession = serde_json::from_value(value)
        .map_err(|_| invalid("accepted-history record is malformed"))?;
    let (result_revision, result_bytes, result): (_, _, SelectedRelease) = rows
        .get_record(&format!("result/{}", expected.version))?
        .ok_or_else(missing)?;
    if revision <= 0
        || revision > observed_revision
        || result_revision != revision
        || link.selected != *expected
        || result.version_ref() != *expected
        || result.dataset.version != expected.version
        || !result.verified
        || link.boundary != result.dataset.boundary
        || link.eligibility != result.dataset.eligibility
        || link.predecessor.as_ref().is_some_and(|previous| {
            previous.dataset != expected.dataset || previous.version == expected.version
        })
    {
        return Err(invalid(
            "accepted-history record differs from its selection commit",
        ));
    }
    Ok((revision, bytes.saturating_add(result_bytes), link))
}

fn check_selected_link(
    selected_revision: i64,
    head: &SelectedRelease,
    revision: i64,
    link: &Succession,
) -> Result<()> {
    if selected_revision != revision
        || head.dataset.boundary != link.boundary
        || head.dataset.eligibility != link.eligibility
    {
        return Err(invalid("selected head differs from its selection commit"));
    }
    Ok(())
}

fn invalid(reason: &str) -> Error {
    Error::refusal("invalid_succession", reason)
}
fn history_capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "accepted-history traversal exceeds its metadata allowance",
    )
}
