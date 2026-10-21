//! Retire standalone result availability without erasing retained dependency receipts.

use super::retirement::invalid_retirement;
use super::*;
use crate::{budget::Budget, catalog::SnapshotRows};

#[cfg(all(test, feature = "test-util"))]
mod tests;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredAvailability {
    dataset: Id,
    operation: Id,
    retirement: Id,
}

pub(super) fn pointer_key(operation: &Id) -> String {
    format!("availability-retired/{operation}")
}

pub(super) fn pointer_record(
    plan: &RetentionPlan,
    operation: &Id,
) -> Result<(String, serde_json::Value)> {
    record(
        pointer_key(operation),
        RetiredAvailability {
            dataset: plan.dataset.clone(),
            operation: operation.clone(),
            retirement: plan.retirement.clone(),
        },
    )
}

pub(super) fn validate_pointer(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    revision: i64,
) -> Result<()> {
    let RetirementScope::ResultAvailability { operation } = &receipt.plan.scope else {
        return Ok(());
    };
    let (pointer_revision, _, pointer): (_, _, RetiredAvailability) = rows
        .get_record(&pointer_key(operation))?
        .ok_or_else(invalid_retirement)?;
    if pointer_revision != revision
        || pointer.dataset != receipt.plan.dataset
        || pointer.operation != *operation
        || pointer.retirement != receipt.plan.retirement
        || &crate::catalog::lifecycle_ident(&pointer.dataset) != view.identifier()
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

pub(super) fn validate_retired_operation(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    engine: &Engine,
) -> Result<bool> {
    let pointer_key = pointer_key(operation);
    if !rows.contains_key(&pointer_key) {
        return Ok(false);
    }
    let budget = &engine.inner.lifecycle.budget;
    let _pointer_memory = rows.reserve_decoded(budget, &[&pointer_key])?;
    let Some((revision, _, pointer)): Option<(_, _, RetiredAvailability)> =
        rows.get_record(&pointer_key)?
    else {
        return Ok(false);
    };
    let key = format!("retirement/{}", pointer.retirement);
    let _receipt_memory = rows.reserve_decoded(budget, &[&key])?;
    let (receipt_revision, _, receipt): (_, _, Retirement) =
        rows.get_record(&key)?.ok_or_else(invalid_retirement)?;
    if receipt_revision != revision
        || pointer.operation != *operation
        || receipt.plan.scope
            != (RetirementScope::ResultAvailability {
                operation: operation.clone(),
            })
    {
        return Err(invalid_retirement());
    }
    retirement::validate_markers(rows, &receipt, view, &key, revision, engine)?;
    Ok(true)
}

// Only identity is relevant to ending this standalone promise. Do not copy file
// inventories or open manifests to retire the already retained record.
#[derive(Deserialize)]
struct ReadbackIdentity {
    id: Id,
    dataset: ReadbackDataset,
    verified: bool,
}

#[derive(Deserialize)]
struct ReadbackDataset {
    dataset: Id,
    version: Id,
}

/// Captures the complete current set; a stale plan cannot silently expand it.
pub(super) fn collect(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    budget: &Budget,
) -> Result<Vec<RetainedGeneration>> {
    let limit = budget.limits.max_metadata_bytes;
    let _state = budget.reserve(limit)?;
    let op_key = format!("operation/{operation}");
    let request_key = format!("request/{operation}");
    let abandoned_key = format!("abandoned/{operation}");
    let _decoding = rows.reserve_decoded(
        budget,
        &[&op_key, &request_key, &abandoned_key, "authority"],
    )?;
    let (operation_revision, _, admitted): (_, _, Operation) = rows
        .get_record(&op_key)?
        .ok_or_else(|| Error::refusal("no_operation", "operation has not been admitted"))?;
    let (request_revision, _, request): (_, _, OperationRequest) =
        rows.get_record(&request_key)?
            .ok_or_else(invalid_retirement)?;
    if operation_revision <= 0
        || operation_revision > view.revision
        || request_revision != operation_revision
        || request.operation() != operation
        || &crate::catalog::lifecycle_ident(request.dataset()) != view.identifier()
        || fingerprint(&request)? != admitted.request_digest
    {
        return Err(invalid_retirement());
    }
    let (authority_revision, authority): (_, Authority) = rows
        .get_record("authority")?
        .map(|(revision, _, value)| (revision, value))
        .unwrap_or_default();
    let result_key = authority
        .release
        .as_ref()
        .map(|release| format!("result/{}", release.id));
    let _selection_memory = rows.reserve_decoded(
        budget,
        &result_key.as_deref().into_iter().collect::<Vec<_>>(),
    )?;
    super::inventory::validate_selection(
        rows,
        &authority,
        authority_revision,
        &view.identifier().to_string(),
        limit,
    )?;
    if authority
        .release
        .as_ref()
        .is_some_and(|release| &release.id == operation)
    {
        return Err(Error::refusal(
            "selected_result",
            "the current selected result remains available",
        ));
    }
    let result_key = format!("result/{operation}");
    if !rows.contains_key(&result_key) {
        let (revision, _, abandoned): (_, _, Abandonment) =
            rows.get_record(&abandoned_key)?.ok_or_else(|| {
                Error::refusal(
                    "operation_active",
                    "abandon unfinished work before retiring its readbacks",
                )
            })?;
        if revision <= operation_revision
            || abandoned.operation_revision != operation_revision
            || abandoned.operation != *operation
            || abandoned.dataset != *request.dataset()
            || abandoned.request_digest != admitted.request_digest
            || abandoned.reason.trim().is_empty()
        {
            return Err(invalid_retirement());
        }
    }
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut add = |key: &str, revision: i64, json: &str| -> Result<()> {
        bytes = bytes.saturating_add(key.len().saturating_mul(2).saturating_add(128));
        if bytes > limit || json.len() > limit {
            return Err(Error::refusal(
                "metadata_capacity",
                "availability retirement exceeds its metadata allowance",
            ));
        }
        let _decoded = budget.reserve(json.len().saturating_mul(4).saturating_add(512))?;
        let identity: ReadbackIdentity = serde_json::from_str(json)?;
        if revision <= 0
            || revision > view.revision
            || identity.id != *operation
            || identity.dataset.version != *operation
            || identity.dataset.dataset != *request.dataset()
            || !identity.verified
            || !retirement::scope_key(
                &RetirementScope::ResultAvailability {
                    operation: operation.clone(),
                },
                key,
            )
        {
            return Err(invalid_retirement());
        }
        records.push(RetainedGeneration {
            key: key.into(),
            revision,
        });
        Ok(())
    };
    for key in [&result_key, &format!("readback/{operation}")] {
        if let Some((_, revision, json)) = rows
            .prefix(key)
            .find(|(candidate, _, _)| *candidate == key.as_str())
        {
            add(key, revision, json)?;
        }
    }
    for (key, revision, json) in rows.prefix(&format!("backfill-readback/{operation}/")) {
        add(key, revision, json)?;
    }
    if records.is_empty() {
        return Err(Error::refusal(
            "no_release",
            "operation has no retained result or readback",
        ));
    }
    records.sort();
    Ok(records)
}

impl Engine {
    /// Captures a non-current result and all its saved readbacks for retirement.
    ///
    /// Unselected readbacks require an abandoned operation. Original records stay
    /// in the catalog for ordering and independently retained dependencies.
    ///
    /// # Errors
    /// Refuses current selections, active unfinished operations, unknown or already
    /// retired availability, malformed records, and exhausted metadata capacity.
    pub async fn plan_availability_retirement(
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
            engine.retirement_catalog()?;
            if reason.trim().is_empty() {
                return Err(invalid_retirement());
            }
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            let view = engine.inner.lifecycle.open(&dataset).await?;
            engine.check_operation_available(&view, &operation).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let records = collect(&rows, &view, &operation, &engine.inner.lifecycle.budget)?;
            retirement::plan(
                &view,
                dataset,
                retirement,
                RetirementScope::ResultAvailability { operation },
                reason,
                records,
            )
        })
        .await
    }

    /// Retires the exact standalone availability named by a saved plan.
    ///
    /// Rechecks the complete result/readback set under owned-host worker exclusion.
    /// This changes logical availability; independent roots and files remain retained.
    ///
    /// # Errors
    /// Refuses a changed set, current selection, active operation, conflicting retry,
    /// invalid native boundary, or exhausted metadata capacity.
    pub async fn retire_availability(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if !matches!(plan.scope, RetirementScope::ResultAvailability { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }

    pub(in crate::engine) async fn check_operation_available(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<()> {
        let rows = self.inner.lifecycle.rows(view).await?;
        if validate_retired_operation(&rows, view, operation, self)? {
            return Err(Error::refusal(
                "availability_retired",
                "this operation's standalone result and readback availability was explicitly retired",
            ));
        }
        Ok(())
    }
}
