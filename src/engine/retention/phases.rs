//! Exact native-revision retirement of independently reusable phase promises.

use super::*;
use crate::catalog::SnapshotRows;
use serde::de::DeserializeOwned;

#[cfg(all(test, feature = "test-util"))]
mod tests;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredPhase {
    pub dataset: Id,
    pub table_uuid: String,
    pub phase: PhaseGeneration,
    pub retirement: Id,
}

pub(in crate::engine) fn completion_key(key: &str) -> bool {
    key.starts_with("phase/backfill/")
        || (key.starts_with("phase/")
            && ["/admission", "/processing", "/validation", "/evidence"]
                .iter()
                .any(|suffix| key.ends_with(suffix)))
        || key.starts_with("backfill-prepared/")
        || key.starts_with("backfill-validation/")
        || key.starts_with("groups/")
}

pub(super) fn marker_key(phase: &PhaseGeneration) -> String {
    format!(
        "phase-retired/{}/{}",
        blake3::hash(phase.key.as_bytes()).to_hex(),
        phase.revision
    )
}

pub(in crate::engine) fn phase_retired(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<bool> {
    let Some((retired_revision, marker)) = retirement_marker(rows, view, key, revision)? else {
        return Ok(false);
    };
    let receipt_key = format!("phase-retirement/{}", marker.retirement);
    let (_, _, receipt) = rows
        .get_record::<PhaseRetirement>(&receipt_key)?
        .ok_or_else(invalid_retirement)?;
    validate_receipt(&receipt, view, &receipt_key, retired_revision)?;
    if receipt.plan.phases.binary_search(&marker.phase).is_err() {
        return Err(invalid_retirement());
    }
    Ok(true)
}

pub(super) fn validate_receipt(
    receipt: &PhaseRetirement,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<()> {
    let plan = &receipt.plan;
    if key != format!("phase-retirement/{}", plan.retirement)
        || receipt.revision != revision
        || revision > view.revision
        || plan.revision <= 0
        || plan.revision >= revision
        || plan.table_uuid != view.table.metadata().uuid().to_string()
        || &crate::catalog::lifecycle_ident(&plan.dataset) != view.table.identifier()
        || plan.reason.trim().is_empty()
        || plan.phases.is_empty()
        || plan
            .phases
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
        || plan.phases.iter().any(|phase| {
            !completion_key(&phase.key) || phase.revision <= 0 || phase.revision > plan.revision
        })
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

pub(super) fn validate_markers(
    rows: &SnapshotRows,
    receipt: &PhaseRetirement,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<()> {
    validate_receipt(receipt, view, key, revision)?;
    for phase in &receipt.plan.phases {
        let (marker_revision, marker) = retirement_marker(rows, view, &phase.key, phase.revision)?
            .ok_or_else(invalid_retirement)?;
        if marker_revision != revision
            || marker.retirement != receipt.plan.retirement
            || marker.dataset != receipt.plan.dataset
            || marker.table_uuid != receipt.plan.table_uuid
        {
            return Err(invalid_retirement());
        }
    }
    Ok(())
}

// Inventory validates each receipt once, then checks its markers without
// repeatedly decoding an entire retirement batch for each completion.
pub(super) fn retirement_marker(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<Option<(i64, RetiredPhase)>> {
    let phase = PhaseGeneration {
        key: key.into(),
        revision,
    };
    let Some((retired_revision, _, marker)) =
        rows.get_record::<RetiredPhase>(&marker_key(&phase))?
    else {
        return Ok(None);
    };
    if marker.phase != phase
        || !completion_key(key)
        || marker.table_uuid != view.table.metadata().uuid().to_string()
        || &crate::catalog::lifecycle_ident(&marker.dataset) != view.table.identifier()
        || revision <= 0
        || retired_revision <= revision
        || retired_revision > view.revision
        || rows.revision(&format!("phase-retirement/{}", marker.retirement))
            != Some(retired_revision)
    {
        return Err(invalid_retirement());
    }
    Ok(Some((retired_revision, marker)))
}

pub(super) fn invalid_retirement() -> Error {
    Error::refusal(
        "invalid_retirement",
        "retirement identity or native generation is inconsistent",
    )
}

impl Engine {
    pub(in crate::engine) async fn active_completion<T: DeserializeOwned>(
        &self,
        view: &View,
        key: &str,
    ) -> Result<Option<T>> {
        if !completion_key(key) {
            return Err(invalid_retirement());
        }
        let rows = self.inner.lifecycle.rows(view).await?;
        let Some(revision) = rows.revision(key) else {
            return Ok(None);
        };
        if phase_retired(&rows, view, key, revision)? {
            return Ok(None);
        }
        Ok(rows.get_record::<T>(key)?.map(|(_, _, value)| value))
    }

    /// Captures exact completion generations for explicit recovery-promise retirement.
    ///
    /// This performs metadata work only. It retires no promise and reports no
    /// reclaimable bytes; independent releases and reservations can still protect
    /// every physical object after these completions are retired.
    ///
    /// # Errors
    /// Refuses unknown or retired completions, duplicate keys, invalid reasons,
    /// unqualified catalogs, or exhausted metadata capacity.
    pub async fn plan_phase_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        keys: &[String],
        reason: &str,
    ) -> Result<PhaseRetirementPlan> {
        let memory = self.request_memory(&(dataset, retirement, keys, reason))?;
        let dataset = dataset.clone();
        let retirement = retirement.clone();
        let keys = keys.to_vec();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine
                .plan_phase_retirement_admitted(dataset, retirement, keys, reason)
                .await
        })
        .await
    }

    async fn plan_phase_retirement_admitted(
        &self,
        dataset: Id,
        retirement: Id,
        mut keys: Vec<String>,
        reason: String,
    ) -> Result<PhaseRetirementPlan> {
        self.retirement_catalog()?;
        keys.sort();
        if reason.trim().is_empty()
            || keys.is_empty()
            || keys.windows(2).any(|pair| pair[0] == pair[1])
            || keys.iter().any(|key| !completion_key(key))
        {
            return Err(invalid_retirement());
        }
        let lock = self.lock(&dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(&dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let mut phases = Vec::with_capacity(keys.len());
        for key in keys {
            let revision = rows
                .revision(&key)
                .ok_or_else(|| Error::refusal("no_phase", "requested completion is unavailable"))?;
            if phase_retired(&rows, &view, &key, revision)? {
                return Err(Error::refusal(
                    "phase_retired",
                    "requested completion is already retired",
                ));
            }
            phases.push(PhaseGeneration { key, revision });
        }
        Ok(PhaseRetirementPlan {
            dataset,
            retirement,
            reason,
            phases,
            table_uuid: view.table.metadata().uuid().to_string(),
            metadata: view.table.metadata_location_result()?.to_owned(),
            snapshot_id: view
                .table
                .metadata()
                .current_snapshot()
                .ok_or_else(invalid_retirement)?
                .snapshot_id(),
            revision: view.revision,
        })
    }

    /// Retires only the exact completion generations named by a captured plan.
    ///
    /// Waits for admitted workers and records one guarded native decision. A later
    /// generation at the same material key is independently reusable. Retained
    /// results, readbacks, physical files and attempt protections stay available.
    ///
    /// # Errors
    /// Refuses changed generations, conflicting retry identities, unknown native
    /// boundaries, unqualified catalogs, or exhausted capacity. No objects are deleted.
    pub async fn retire_phases(&self, plan: &PhaseRetirementPlan) -> Result<PhaseRetirement> {
        let memory = self.request_memory(plan)?;
        let plan = plan.clone();
        self.maintain_work(memory, move |engine| async move {
            engine.retire_phases_admitted(&plan).await
        })
        .await
    }

    async fn retire_phases_admitted(&self, plan: &PhaseRetirementPlan) -> Result<PhaseRetirement> {
        self.retirement_catalog()?;
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&plan.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&plan.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let key = format!("phase-retirement/{}", plan.retirement);
        if let Some((revision, _, previous)) = rows.get_record::<PhaseRetirement>(&key)? {
            if previous.plan != *plan || previous.revision != revision {
                return Err(Error::refusal(
                    "retirement_conflict",
                    "retirement identity already records a different decision",
                ));
            }
            validate_markers(&rows, &previous, &view, &key, revision)?;
            return Ok(previous);
        }
        let metadata = view.table.metadata();
        if plan.reason.trim().is_empty()
            || plan.phases.is_empty()
            || plan
                .phases
                .windows(2)
                .any(|pair| pair[0].key >= pair[1].key)
            || plan.table_uuid != metadata.uuid().to_string()
            || metadata
                .snapshot_by_id(plan.snapshot_id)
                .is_none_or(|snapshot| snapshot.sequence_number() != plan.revision)
            || (plan.metadata != view.table.metadata_location_result()?
                && !metadata
                    .metadata_log()
                    .iter()
                    .any(|entry| entry.metadata_file == plan.metadata))
        {
            return Err(invalid_retirement());
        }
        let mut records = Vec::with_capacity(plan.phases.len() + 1);
        for phase in &plan.phases {
            if !completion_key(&phase.key) || phase.revision <= 0 || phase.revision > plan.revision
            {
                return Err(invalid_retirement());
            }
            if rows.revision(&phase.key) != Some(phase.revision)
                || phase_retired(&rows, &view, &phase.key, phase.revision)?
            {
                return Err(Error::refusal(
                    "stale_retirement",
                    "completion generation changed or was already retired; capture a fresh plan",
                ));
            }
            records.push(record(
                marker_key(phase),
                RetiredPhase {
                    dataset: plan.dataset.clone(),
                    table_uuid: plan.table_uuid.clone(),
                    phase: phase.clone(),
                    retirement: plan.retirement.clone(),
                },
            )?);
        }
        let result = PhaseRetirement {
            plan: plan.clone(),
            revision: view
                .revision
                .checked_add(1)
                .ok_or_else(invalid_retirement)?,
        };
        records.push(record(&key, &result)?);
        if let Err(error) = lifecycle.commit(&view, records).await {
            let after = lifecycle.open(&plan.dataset).await?;
            let rows = lifecycle.rows(&after).await?;
            if let Some((revision, _, previous)) = rows.get_record::<PhaseRetirement>(&key)? {
                if previous == result {
                    validate_markers(&rows, &previous, &after, &key, revision)?;
                    return Ok(previous);
                }
                return Err(Error::refusal(
                    "retirement_conflict",
                    "retirement identity changed after an uncertain commit",
                ));
            }
            return Err(error);
        }
        Ok(result)
    }

    fn retirement_catalog(&self) -> Result<()> {
        if !self.inner.owned_catalog {
            return Err(Error::refusal(
                "retention_catalog_unqualified",
                "phase retirement requires the owned embedded catalog",
            ));
        }
        Ok(())
    }
}
