//! Guarded retirement of exact recovery and result-availability promises.

use super::*;
use crate::catalog::SnapshotRows;
use serde::de::DeserializeOwned;

#[cfg(all(test, feature = "test-util"))]
mod tests;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredRecord {
    pub dataset: Id,
    pub table_uuid: String,
    pub record: RetainedGeneration,
    pub retirement: Id,
}

pub(in crate::engine) fn completion_key(key: &str) -> bool {
    key.starts_with("restore-reuse/")
        || key.starts_with("phase/backfill/")
        || (key.starts_with("phase/")
            && ["/admission", "/processing", "/validation", "/evidence"]
                .iter()
                .any(|suffix| key.ends_with(suffix)))
        || key.starts_with("backfill-prepared/")
        || key.starts_with("backfill-validation/")
        || key.starts_with("groups/")
}

pub(super) fn scope_key(scope: &RetirementScope, key: &str) -> bool {
    match scope {
        RetirementScope::PhaseRecovery => completion_key(key),
        RetirementScope::RestorePoints => key.starts_with("restore-point/"),
        RetirementScope::OwnerGrants => super::grants::grant_key(key),
        RetirementScope::OperationRecovery { operation } => {
            super::recovery::scope_key(operation, key)
        }
        RetirementScope::ResultAvailability { operation } => {
            key == format!("result/{operation}")
                || key == format!("readback/{operation}")
                || key
                    .strip_prefix(&format!("backfill-readback/{operation}/"))
                    .is_some_and(|digest| {
                        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
                    })
        }
    }
}

pub(super) fn retained_key(key: &str) -> bool {
    completion_key(key)
        || key.starts_with("restore-point/")
        || key.starts_with("result/")
        || key.starts_with("readback/")
        || key.starts_with("backfill-readback/")
        || super::grants::grant_key(key)
        || [
            "operation/",
            "request/",
            "request-dependencies/",
            "backfill/",
            "protection/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

pub(super) fn marker_key(phase: &RetainedGeneration) -> String {
    format!(
        "retired/{}/{}",
        blake3::hash(phase.key.as_bytes()).to_hex(),
        phase.revision
    )
}

pub(in crate::engine) fn record_retired(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
    budget: &crate::budget::Budget,
) -> Result<bool> {
    let marker = marker_key(&RetainedGeneration {
        key: key.into(),
        revision,
    });
    if !rows.contains_key(&marker) {
        return Ok(false);
    }
    let _marker_memory = rows.reserve_decoded(budget, &[&marker])?;
    let Some((retired_revision, marker)) = retirement_marker(rows, view, key, revision)? else {
        return Ok(false);
    };
    let receipt_key = format!("retirement/{}", marker.retirement);
    let _receipt_memory = rows.reserve_decoded(budget, &[&receipt_key])?;
    let (_, _, receipt) = rows
        .get_record::<Retirement>(&receipt_key)?
        .ok_or_else(invalid_retirement)?;
    validate_receipt(&receipt, view, &receipt_key, retired_revision)?;
    if receipt.plan.records.binary_search(&marker.record).is_err() {
        return Err(invalid_retirement());
    }
    Ok(true)
}

pub(super) fn validate_receipt(
    receipt: &Retirement,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<()> {
    let plan = &receipt.plan;
    if key != format!("retirement/{}", plan.retirement)
        || receipt.revision != revision
        || revision > view.revision
        || plan.revision <= 0
        || plan.revision >= revision
        || plan.table_uuid != view.table.metadata().uuid().to_string()
        || &crate::catalog::lifecycle_ident(&plan.dataset) != view.table.identifier()
        || plan.reason.trim().is_empty()
        || plan.records.is_empty()
        || plan
            .records
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
        || plan.records.iter().any(|phase| {
            !scope_key(&plan.scope, &phase.key)
                || phase.revision <= 0
                || phase.revision > plan.revision
        })
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

pub(super) fn validate_markers(
    rows: &SnapshotRows,
    receipt: &Retirement,
    view: &View,
    key: &str,
    revision: i64,
    engine: &Engine,
) -> Result<()> {
    validate_receipt(receipt, view, key, revision)?;
    for phase in &receipt.plan.records {
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
    super::availability::validate_pointer(rows, view, receipt, revision)?;
    super::recovery::validate_saved(rows, view, receipt, engine)?;
    super::grants::validate_saved(rows, view, receipt, engine)?;
    Ok(())
}

/// Validated retirement membership shared by inventory and grant eligibility.
pub(super) struct RetiredRecords {
    pub records: BTreeSet<RetainedGeneration>,
}

/// The caller charges retained sets against its admitted graph/state allowance.
pub(super) fn retired_records(
    rows: &SnapshotRows,
    view: &View,
    engine: &Engine,
    mut charge: impl FnMut(usize) -> Result<()>,
) -> Result<RetiredRecords> {
    let budget = &engine.inner.lifecycle.budget;
    let mut records = BTreeSet::new();
    let mut operations = BTreeSet::new();
    for (key, revision, _) in rows.prefix("retirement/") {
        let _memory = rows.reserve_decoded(budget, &[key])?;
        let (_, _, retired): (_, _, Retirement) =
            rows.get_record(key)?.ok_or_else(invalid_retirement)?;
        validate_markers(rows, &retired, view, key, revision, engine)?;
        if let RetirementScope::ResultAvailability { operation } = retired.plan.scope {
            charge(operation.as_str().len().saturating_add(128))?;
            if !operations.insert(operation) {
                return Err(invalid_retirement());
            }
        }
        for record in retired.plan.records {
            charge(record.key.len().saturating_add(128))?;
            if !records.insert(record) {
                return Err(invalid_retirement());
            }
        }
    }
    for (key, _, _) in rows.prefix("retired/") {
        let _memory = rows.reserve_decoded(budget, &[key])?;
        let (_, _, marker): (_, _, RetiredRecord) =
            rows.get_record(key)?.ok_or_else(invalid_retirement)?;
        if key != marker_key(&marker.record) || !records.contains(&marker.record) {
            return Err(invalid_retirement());
        }
    }
    for (key, _, _) in rows.prefix("availability-retired/") {
        let operation = Id::new(
            key.strip_prefix("availability-retired/")
                .ok_or_else(invalid_retirement)?,
        )?;
        if !operations.contains(&operation) {
            return Err(invalid_retirement());
        }
    }
    Ok(RetiredRecords { records })
}

// Inventory validates each receipt once, then checks its markers without
// repeatedly decoding an entire retirement batch for each completion.
pub(super) fn retirement_marker(
    rows: &SnapshotRows,
    view: &View,
    key: &str,
    revision: i64,
) -> Result<Option<(i64, RetiredRecord)>> {
    let phase = RetainedGeneration {
        key: key.into(),
        revision,
    };
    let Some((retired_revision, _, marker)) =
        rows.get_record::<RetiredRecord>(&marker_key(&phase))?
    else {
        return Ok(None);
    };
    if marker.record != phase
        || !retained_key(key)
        || marker.table_uuid != view.table.metadata().uuid().to_string()
        || &crate::catalog::lifecycle_ident(&marker.dataset) != view.table.identifier()
        || revision <= 0
        || retired_revision <= revision
        || retired_revision > view.revision
        || rows.revision(&format!("retirement/{}", marker.retirement)) != Some(retired_revision)
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

pub(super) fn plan(
    view: &View,
    dataset: Id,
    retirement: Id,
    scope: RetirementScope,
    reason: String,
    records: Vec<RetainedGeneration>,
) -> Result<RetentionPlan> {
    Ok(RetentionPlan {
        dataset,
        retirement,
        scope,
        reason,
        records,
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
        if record_retired(&rows, view, key, revision, &self.inner.lifecycle.budget)? {
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
    ) -> Result<RetentionPlan> {
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
    ) -> Result<RetentionPlan> {
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
            if record_retired(&rows, &view, &key, revision, &self.inner.lifecycle.budget)? {
                return Err(Error::refusal(
                    "phase_retired",
                    "requested completion is already retired",
                ));
            }
            phases.push(RetainedGeneration { key, revision });
        }
        plan(
            &view,
            dataset,
            retirement,
            RetirementScope::PhaseRecovery,
            reason,
            phases,
        )
    }

    /// Captures explicit retirement of named local restore promises.
    /// Backup bytes remain until a separately authorized removal becomes eligible.
    ///
    /// # Errors
    /// Refuses missing/retired points, duplicate names, or changed native authority.
    pub async fn plan_restore_point_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        points: &[Id],
        reason: &str,
    ) -> Result<RetentionPlan> {
        let memory = self.request_memory(&(dataset, retirement, points, reason))?;
        let dataset = dataset.clone();
        let retirement = retirement.clone();
        let points = points.to_vec();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.retirement_catalog()?;
            let mut keys = points
                .iter()
                .map(|point| format!("restore-point/{point}"))
                .collect::<Vec<_>>();
            keys.sort();
            if keys.is_empty()
                || reason.trim().is_empty()
                || keys.windows(2).any(|pair| pair[0] == pair[1])
            {
                return Err(invalid_retirement());
            }
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            let view = engine.inner.lifecycle.open(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let mut records = Vec::with_capacity(keys.len());
            for key in keys {
                let (revision, _, point) = rows
                    .get_record::<crate::restore::RestorePointRef>(&key)?
                    .ok_or_else(|| {
                        Error::refusal("no_restore_point", "requested restore point is unavailable")
                    })?;
                if point.dataset != dataset || key != format!("restore-point/{}", point.id) {
                    return Err(invalid_retirement());
                }
                if record_retired(&rows, &view, &key, revision, &engine.inner.lifecycle.budget)? {
                    return Err(Error::refusal(
                        "restore_point_retired",
                        "requested restore point is already retired",
                    ));
                }
                records.push(RetainedGeneration { key, revision });
            }
            plan(
                &view,
                dataset,
                retirement,
                RetirementScope::RestorePoints,
                reason,
                records,
            )
        })
        .await
    }

    /// Ends only the exact restore promises captured by a reviewed plan.
    ///
    /// # Errors
    /// Refuses changed generations, conflicting retries, or another retirement scope.
    pub async fn retire_restore_points(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if plan.scope != RetirementScope::RestorePoints {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
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
    pub async fn retire_phases(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if plan.scope != RetirementScope::PhaseRecovery {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }

    pub(super) async fn retire_retained(&self, plan: &RetentionPlan) -> Result<Retirement> {
        let memory = self.request_memory(plan)?;
        let plan = plan.clone();
        self.maintain_work(memory, move |engine| async move {
            engine.retire_retained_admitted(&plan).await
        })
        .await
    }

    async fn retire_retained_admitted(&self, plan: &RetentionPlan) -> Result<Retirement> {
        self.retirement_catalog()?;
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&plan.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&plan.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let key = format!("retirement/{}", plan.retirement);
        if let Some((revision, _, previous)) = rows.get_record::<Retirement>(&key)? {
            if previous.plan != *plan || previous.revision != revision {
                return Err(Error::refusal(
                    "retirement_conflict",
                    "retirement identity already records a different decision",
                ));
            }
            validate_markers(&rows, &previous, &view, &key, revision, self)?;
            return Ok(previous);
        }
        let metadata = view.table.metadata();
        if plan.reason.trim().is_empty()
            || plan.records.is_empty()
            || plan
                .records
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
        if let RetirementScope::ResultAvailability { operation } = &plan.scope {
            self.check_operation_available(&view, operation).await?;
            let current = super::availability::collect(&rows, &view, operation, &lifecycle.budget)?;
            if current != plan.records {
                return Err(Error::refusal(
                    "stale_retirement",
                    "result or readback generations changed; capture a fresh plan",
                ));
            }
        }
        if let RetirementScope::OperationRecovery { operation } = &plan.scope {
            let current = super::recovery::collect(
                &rows,
                &view,
                operation,
                &self.inner.root,
                &lifecycle.budget,
            )?;
            if current != plan.records {
                return Err(Error::refusal(
                    "stale_retirement",
                    "operation recovery records changed; capture a fresh plan",
                ));
            }
        }
        if plan.scope == RetirementScope::OwnerGrants {
            super::grants::check_eligible(self, &view, &rows, &plan.records).await?;
        }
        let mut records = Vec::with_capacity(plan.records.len() + 1);
        for phase in &plan.records {
            if !scope_key(&plan.scope, &phase.key)
                || phase.revision <= 0
                || phase.revision > plan.revision
            {
                return Err(invalid_retirement());
            }
            if rows.revision(&phase.key) != Some(phase.revision)
                || record_retired(&rows, &view, &phase.key, phase.revision, &lifecycle.budget)?
            {
                return Err(Error::refusal(
                    "stale_retirement",
                    "completion generation changed or was already retired; capture a fresh plan",
                ));
            }
            records.push(record(
                marker_key(phase),
                RetiredRecord {
                    dataset: plan.dataset.clone(),
                    table_uuid: plan.table_uuid.clone(),
                    record: phase.clone(),
                    retirement: plan.retirement.clone(),
                },
            )?);
        }
        let result = Retirement {
            plan: plan.clone(),
            revision: view
                .revision
                .checked_add(1)
                .ok_or_else(invalid_retirement)?,
        };
        records.push(record(&key, &result)?);
        if let RetirementScope::ResultAvailability { operation } = &plan.scope {
            records.push(super::availability::pointer_record(plan, operation)?);
        }
        if let Err(error) = lifecycle.commit(&view, records).await {
            let after = lifecycle.open(&plan.dataset).await?;
            let rows = lifecycle.rows(&after).await?;
            if let Some((revision, _, previous)) = rows.get_record::<Retirement>(&key)? {
                if previous == result {
                    validate_markers(&rows, &previous, &after, &key, revision, self)?;
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

    pub(super) fn retirement_catalog(&self) -> Result<()> {
        if !self.inner.owned_catalog {
            return Err(Error::refusal(
                "retention_catalog_unqualified",
                "retirement requires the owned embedded catalog",
            ));
        }
        Ok(())
    }
}
