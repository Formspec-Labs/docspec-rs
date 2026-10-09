//! End terminal-operation recovery while preserving independent results and phases.
// Rust guideline compliant 2026-02-21

use super::retirement::{invalid_retirement, record_retired};
use super::*;
use crate::{budget::Budget, catalog::SnapshotRows};

#[cfg(all(test, feature = "test-util"))]
mod tests;

fn keys(operation: &Id) -> [String; 4] {
    ["operation", "request", "request-dependencies", "backfill"]
        .map(|family| format!("{family}/{operation}"))
}

pub(super) fn scope_key(operation: &Id, key: &str) -> bool {
    keys(operation).iter().any(|candidate| candidate == key)
        || key
            .strip_prefix("protection/")
            .is_some_and(|id| Id::new(id).is_ok())
}

// Admission identity stays readable after its recovery contribution ends.
fn admission(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
) -> Result<(i64, OperationRequest, Operation)> {
    let (revision, _, admitted): (_, _, Operation) = rows
        .get_record(&format!("operation/{operation}"))?
        .ok_or_else(|| Error::refusal("no_operation", "operation has not been admitted"))?;
    let (request_revision, _, request): (_, _, OperationRequest) = rows
        .get_record(&format!("request/{operation}"))?
        .ok_or_else(invalid_retirement)?;
    if revision <= 0
        || revision > view.revision
        || request_revision != revision
        || request.operation() != operation
        || &crate::catalog::lifecycle_ident(request.dataset()) != view.table.identifier()
        || !supported_format(admitted.result_format)
        || fingerprint(&request)? != admitted.request_digest
    {
        return Err(invalid_retirement());
    }
    Ok((revision, request, admitted))
}

fn terminal(
    rows: &SnapshotRows,
    operation_revision: i64,
    request: &OperationRequest,
    admitted: &Operation,
    boundary: i64,
) -> Result<()> {
    let operation = request.operation();
    if let Some(revision) = rows.revision(&format!("result/{operation}")) {
        // Decode only the original request, not the result's file inventory.
        #[derive(Deserialize)]
        struct CompletedRequest {
            dataset: RequestDataset,
        }
        #[derive(Deserialize)]
        struct RequestDataset {
            request: OperationRequest,
        }
        let (_, _, completed): (_, _, CompletedRequest) = rows
            .get_record(&format!("result/{operation}"))?
            .ok_or_else(invalid_retirement)?;
        let (link_revision, _, link): (_, _, Succession) = rows
            .get_record(&format!("succession/{operation}"))?
            .ok_or_else(invalid_retirement)?;
        if revision <= operation_revision
            || revision > boundary
            || link_revision != revision
            || link.selected.version != *operation
            || link.selected.dataset != *request.dataset()
            || completed.dataset.request != *request
            || rows.contains_key(&format!("abandoned/{operation}"))
        {
            return Err(invalid_retirement());
        }
        super::super::history::check_link_receipt(rows, revision, &link)?;
        return Ok(());
    }
    let (revision, _, abandoned): (_, _, Abandonment) = rows
        .get_record(&format!("abandoned/{operation}"))?
        .ok_or_else(|| {
            Error::refusal(
                "operation_active",
                "complete or abandon the operation before retiring recovery",
            )
        })?;
    if revision <= operation_revision
        || revision > boundary
        || abandoned.operation_revision != operation_revision
        || abandoned.operation != *operation
        || abandoned.dataset != *request.dataset()
        || abandoned.request_digest != admitted.request_digest
        || abandoned.reason.trim().is_empty()
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

fn reserve_identity(
    rows: &SnapshotRows,
    operation: &Id,
    budget: &Budget,
) -> Result<tokio::sync::OwnedSemaphorePermit> {
    let names = ["operation", "request", "result", "succession", "abandoned"]
        .map(|family| format!("{family}/{operation}"));
    rows.reserve_decoded(
        budget,
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    )
}

fn check_fixed(
    rows: &SnapshotRows,
    request: &OperationRequest,
    revision: i64,
) -> Result<Vec<RetainedGeneration>> {
    let mut fixed = Vec::with_capacity(4);
    for (index, key) in keys(request.operation()).into_iter().enumerate() {
        let actual = rows.revision(&key);
        if (index < 2 && actual.is_none())
            || (index == 3 && actual.is_some() != matches!(request, OperationRequest::Backfill(_)))
            || actual.is_some_and(|actual| actual != revision)
        {
            return Err(invalid_retirement());
        }
        if let Some(revision) = actual {
            fixed.push(RetainedGeneration { key, revision });
        }
    }
    Ok(fixed)
}

fn check_attempt(
    root: &Path,
    request: &OperationRequest,
    operation_revision: i64,
    generation: &RetainedGeneration,
    attempt: &Protection,
    boundary: i64,
) -> Result<()> {
    let Some((operation, purpose)) = attempt.purpose.split_once('/') else {
        return Err(invalid_retirement());
    };
    let directory = root
        .join("objects")
        .join(request.dataset().as_str())
        .join(attempt.id.as_str());
    if generation.key != format!("protection/{}", attempt.id)
        || generation.revision <= operation_revision
        || generation.revision > boundary
        || operation != request.operation().as_str()
        || Id::new(purpose).is_err()
        || attempt.consumer != *request.dataset()
        || attempt.paths != [format!("{}/", directory.display())]
        || attempt.retired
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

/// Metadata-only collection, bounded by record state and per-record decoding.
pub(super) fn collect(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    root: &Path,
    budget: &Budget,
) -> Result<Vec<RetainedGeneration>> {
    let limit = budget.limits.max_metadata_bytes;
    let _state = budget.reserve(limit)?;
    let _decoded = reserve_identity(rows, operation, budget)?;
    let (operation_revision, request, admitted) = admission(rows, view, operation)?;
    terminal(rows, operation_revision, &request, &admitted, view.revision)?;
    let mut records = check_fixed(rows, &request, operation_revision)?;
    // Charge spare vector capacity and per-entry allocation overhead as well as keys.
    let mut bytes = records
        .iter()
        .map(|record| record.key.len().saturating_mul(2).saturating_add(128))
        .sum::<usize>();
    let prefix = format!("{operation}/");
    for (key, revision, json) in rows.prefix("protection/") {
        let _memory = rows.reserve_decoded(budget, &[key])?;
        let attempt: Protection = serde_json::from_str(json)?;
        if !attempt.purpose.starts_with(&prefix) {
            continue;
        }
        let generation = RetainedGeneration {
            key: key.into(),
            revision,
        };
        check_attempt(
            root,
            &request,
            operation_revision,
            &generation,
            &attempt,
            view.revision,
        )?;
        bytes = bytes.saturating_add(key.len().saturating_mul(2).saturating_add(128));
        if bytes > limit {
            return Err(capacity());
        }
        records.push(generation);
    }
    if bytes > limit {
        return Err(capacity());
    }
    records.sort();
    Ok(records)
}

/// Validates only the captured records, never rescanning attempts for every receipt.
pub(super) fn validate_saved(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    engine: &Engine,
) -> Result<()> {
    let RetirementScope::OperationRecovery { operation } = &receipt.plan.scope else {
        return Ok(());
    };
    let budget = &engine.inner.lifecycle.budget;
    let _decoded = reserve_identity(rows, operation, budget)?;
    let (revision, request, admitted) = admission(rows, view, operation)?;
    terminal(rows, revision, &request, &admitted, receipt.plan.revision)?;
    for fixed in check_fixed(rows, &request, revision)? {
        if receipt.plan.records.binary_search(&fixed).is_err() {
            return Err(invalid_retirement());
        }
    }
    for generation in &receipt.plan.records {
        if rows.revision(&generation.key) != Some(generation.revision) {
            return Err(invalid_retirement());
        }
        if generation.key.starts_with("protection/") {
            let _memory = rows.reserve_decoded(budget, &[&generation.key])?;
            let (_, _, attempt) = rows
                .get_record::<Protection>(&generation.key)?
                .ok_or_else(invalid_retirement)?;
            check_attempt(
                &engine.inner.root,
                &request,
                revision,
                generation,
                &attempt,
                receipt.plan.revision,
            )?;
        }
    }
    Ok(())
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "operation retirement exceeds its metadata allowance",
    )
}

impl Engine {
    /// Captures terminal recovery records and owned attempt reservations for retirement.
    ///
    /// Results and completed phases remain independent promises. Original records
    /// stay as identity and creator evidence; this service removes no physical data.
    ///
    /// # Errors
    /// Refuses active or already retired operations, inconsistent records, unqualified
    /// catalogs, empty reasons, or exhausted metadata capacity.
    pub async fn plan_operation_retirement(
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
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let key = format!("operation/{operation}");
            if let Some(revision) = rows.revision(&key)
                && record_retired(&rows, &view, &key, revision, &engine.inner.lifecycle.budget)?
            {
                return Err(Error::refusal(
                    "recovery_retired",
                    "operation recovery is already retired",
                ));
            }
            let records = collect(
                &rows,
                &view,
                &operation,
                &engine.inner.root,
                &engine.inner.lifecycle.budget,
            )?;
            retirement::plan(
                &view,
                dataset,
                retirement,
                RetirementScope::OperationRecovery { operation },
                reason,
                records,
            )
        })
        .await
    }

    /// Retires captured terminal recovery records while preserving independent retained work.
    ///
    /// # Errors
    /// Refuses an incorrect scope, changed record set, active operation, conflicting
    /// retry, malformed metadata, or exhausted capacity. No physical objects are deleted.
    pub async fn retire_operation(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if !matches!(plan.scope, RetirementScope::OperationRecovery { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
