//! Explicit export recovery retirement preserves independent immutable export pins.
// Rust guideline compliant 2026-02-21

use super::retirement::{invalid_retirement, record_retired};
use super::*;
use crate::catalog::SnapshotRows;

pub(super) fn scope_key(operation: &Id, key: &str) -> bool {
    [
        "export-operation",
        "export-materialized",
        "export-validation",
    ]
    .iter()
    .any(|family| key == format!("{family}/{operation}"))
        || [
            "export-attempt",
            "export-staged",
            "export-stage-validation",
            "export-host-file",
            "export-host-closed",
        ]
        .iter()
        .any(|family| {
            key.strip_prefix(&format!("{family}/{operation}/"))
                .is_some_and(|revision| {
                    revision
                        .split('/')
                        .next()
                        .is_some_and(|revision| revision.parse::<i64>().is_ok_and(|n| n > 0))
                })
        })
        || key
            .strip_prefix("protection/")
            .is_some_and(|id| Id::new(id).is_ok())
}

fn collect(
    engine: &Engine,
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
) -> Result<Vec<RetainedGeneration>> {
    let budget = &engine.inner.lifecycle.budget;
    let key = format!("export-operation/{operation}");
    let _memory = rows.reserve_decoded(budget, &[&key])?;
    let (revision, _, saved): (_, _, crate::engine::export::ExportOperation) = rows
        .get_record(&key)?
        .ok_or_else(|| Error::refusal("no_export", "export has not been admitted"))?;
    crate::engine::export::validate_operation_record(&saved)?;
    if saved.request.operation != *operation
        || crate::catalog::lifecycle_ident(&saved.request.dataset) != *view.table.identifier()
    {
        return Err(invalid_retirement());
    }
    let mut records = vec![RetainedGeneration { key, revision }];
    for (key, revision, json) in rows.prefix("") {
        if !scope_key(operation, key) || key.starts_with("protection/") {
            continue;
        }
        if key.starts_with("export-operation/") {
            continue;
        }
        let _memory = rows.reserve_decoded(budget, &[key])?;
        if key.starts_with("export-host-file/") || key.starts_with("export-host-closed/") {
            let created_key = key.replacen("export-host-closed/", "export-host-file/", 1);
            let _created_memory = rows.reserve_decoded(budget, &[&created_key])?;
            let (created_revision, _, created): (_, _, crate::engine::export::host::HostFile) =
                rows.get_record(&created_key)?
                    .ok_or_else(invalid_retirement)?;
            crate::engine::export::host::validate(
                &created,
                &created_key,
                created_revision,
                rows,
                Some(budget),
            )?;
            if key.starts_with("export-host-closed/") {
                let materialized: crate::engine::export::ExportMaterialized =
                    serde_json::from_str(json)?;
                crate::engine::export::check_materialized(&materialized, &saved)?;
                if materialized.object.path != created.path || revision <= created_revision {
                    return Err(invalid_retirement());
                }
            }
        } else if key.starts_with("export-attempt/")
            || key.starts_with("export-staged/")
            || key.starts_with("export-stage-validation/")
        {
            crate::engine::export::remote::validate_record(key, json, rows, Some(budget))?;
        } else if key.starts_with("export-materialized/") {
            let materialized: crate::engine::export::ExportMaterialized =
                serde_json::from_str(json)?;
            crate::engine::export::check_materialized(&materialized, &saved)?;
        } else {
            let validation: crate::export::ExportValidation = serde_json::from_str(json)?;
            let materialized_key = format!("export-materialized/{operation}");
            let _materialized_memory = rows.reserve_decoded(budget, &[&materialized_key])?;
            let (_, _, materialized): (_, _, crate::engine::export::ExportMaterialized) = rows
                .get_record(&materialized_key)?
                .ok_or_else(invalid_retirement)?;
            if validation.object != materialized.object || validation.rows != materialized.rows {
                return Err(invalid_retirement());
            }
        }
        records.push(RetainedGeneration {
            key: key.into(),
            revision,
        });
    }
    // Intent and its original protection were committed together. Keep both exact
    // generations immutable; a retirement marker ends only their recovery root.
    let attempt_key = format!("export-attempt/{operation}/{}", records[0].revision);
    if let Some((_, _, protection)) =
        crate::engine::export::remote::attempt_protection(rows, &attempt_key, Some(budget))?
    {
        records.push(protection);
    }
    for (key, _, _) in rows.prefix(&format!("export-host-file/{operation}/")) {
        let _memory = rows.reserve_decoded(budget, &[key])?;
        let (_, _, file): (_, _, crate::engine::export::host::HostFile) =
            rows.get_record(key)?.ok_or_else(invalid_retirement)?;
        let protection_key = format!("protection/{}", file.attempt);
        let protection_revision = rows
            .revision(&protection_key)
            .ok_or_else(invalid_retirement)?;
        records.push(RetainedGeneration {
            key: protection_key,
            revision: protection_revision,
        });
    }
    records.sort();
    records.dedup();
    Ok(records)
}

pub(super) fn validate_saved(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    engine: &Engine,
) -> Result<()> {
    let RetirementScope::ExportRecovery { operation } = &receipt.plan.scope else {
        return Ok(());
    };
    if collect(engine, rows, view, operation)? != receipt.plan.records {
        return Err(invalid_retirement());
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine) fn check_export_recovery(
        &self,
        rows: &SnapshotRows,
        view: &View,
        operation: &Id,
    ) -> Result<()> {
        let key = format!("export-operation/{operation}");
        if let Some(revision) = rows.revision(&key)
            && record_retired(rows, view, &key, revision, &self.inner.lifecycle.budget)?
        {
            return Err(Error::refusal(
                "export_retired",
                "export recovery was explicitly ended; use a new operation identity",
            ));
        }
        Ok(())
    }

    pub(super) fn check_export_retirement(
        &self,
        rows: &SnapshotRows,
        view: &View,
        operation: &Id,
    ) -> Result<Vec<RetainedGeneration>> {
        for (key, _, json) in rows.prefix("export-channel/") {
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[key])?;
            let pin: crate::export::ExportPin = serde_json::from_str(json)?;
            crate::engine::export::check_pin(&pin)?;
            if pin.id == *operation {
                return Err(Error::refusal(
                    "export_selected",
                    "advance the selected export channel before ending its recovery",
                ));
            }
        }
        let records = collect(self, rows, view, operation)?;
        for generation in &records {
            if record_retired(
                rows,
                view,
                &generation.key,
                generation.revision,
                &self.inner.lifecycle.budget,
            )? {
                return Err(Error::refusal(
                    "export_retired",
                    "export recovery is already retired",
                ));
            }
        }
        Ok(records)
    }

    /// Captures exact export recovery generations for explicit terminal retirement.
    ///
    /// Existing immutable export pins keep their separate availability promise.
    /// No objects are deleted and no unrecorded host file is adopted.
    ///
    /// # Errors
    /// Refuses a current channel selection, absent or retired operation, malformed
    /// native bindings or exhausted metadata admission.
    pub async fn plan_export_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        operation: &Id,
        reason: &str,
    ) -> Result<RetentionPlan> {
        let memory = self.request_memory(&(dataset, retirement, operation, reason))?;
        let (dataset, retirement, operation, reason) = (
            dataset.clone(),
            retirement.clone(),
            operation.clone(),
            reason.to_owned(),
        );
        self.admit_work(memory, move |engine| async move {
            if reason.trim().is_empty() {
                return Err(invalid_retirement());
            }
            let view = engine.existing_lifecycle_view(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let records = engine.check_export_retirement(&rows, &view, &operation)?;
            super::retirement::plan(
                &view,
                dataset,
                retirement,
                RetirementScope::ExportRecovery { operation },
                reason,
                records,
            )
        })
        .await
    }

    /// Ends only the export recovery generations in an exact captured plan.
    ///
    /// Waits for admitted work and preserves immutable exported values and pins.
    ///
    /// # Errors
    /// Refuses changed records, a current channel selection, conflicting identities,
    /// malformed native bindings, or exhausted capacity. No physical deletion occurs.
    pub async fn retire_export(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if !matches!(plan.scope, RetirementScope::ExportRecovery { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
