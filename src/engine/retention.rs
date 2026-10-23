//! Explicit operation abandonment; completed phases and every object remain retained.
//!
//! This ends an operation's permission to acknowledge work or select a result.
//! Already-started I/O can finish under its existing protection. Abandonment is
//! neither worker quiescence nor permission to delete a phase or attempt.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::catalog::View;
use std::collections::BTreeSet;

mod availability;
mod compaction;
pub(super) mod compaction_recovery;
mod export_recovery;
mod grants;
pub(in crate::engine) mod inventory;
pub(super) mod owners;
mod recovery;
pub(crate) mod removal;
pub(in crate::engine) mod retirement;
mod seals;
#[cfg(test)]
mod tests;

impl Engine {
    /// Requires a qualified outcome for every accepted source operation.
    ///
    /// A later selected boundary cannot stand in for an earlier request's
    /// selection or explicit abandonment. Call from the locked cutover view.
    /// `selecting` may name only the already checked schema operation selected
    /// by the same guarded commit: format-10 schemas also admitted source input.
    pub(super) async fn check_cutover_inputs(
        &self,
        view: &View,
        dataset: &Id,
        selecting: Option<&Id>,
    ) -> Result<()> {
        if self
            .pending_source_operations(view, dataset)
            .await?
            .iter()
            .any(|operation| Some(operation) != selecting)
        {
            return Err(pending_source());
        }
        Ok(())
    }

    /// Proves that every unselected admission in a captured interval was abandoned.
    /// Caller uses the same locked view for admission or final selection.
    pub(super) async fn check_abandoned_gap(
        &self,
        view: &View,
        dataset: &Id,
        selected: u64,
        fence: u64,
    ) -> Result<()> {
        let gap = fence.checked_sub(selected).ok_or_else(invalid_gap)?;
        if gap == 0 {
            return Ok(());
        }
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        // A conservative application charge for a BTreeSet<u64> entry, including
        // node overhead. Admission does not bound all native allocator behavior.
        let charge = gap
            .checked_mul(64)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(|| {
                Error::refusal(
                    "metadata_capacity",
                    "admission interval exceeds metadata allowance",
                )
            })?;
        let _interval = budget.reserve(charge)?;
        let _decoding = budget.reserve(limit.saturating_mul(4))?;
        let rows = self.inner.lifecycle.rows(view).await?;
        let mut boundaries = BTreeSet::new();
        for (key, revision, json) in rows.prefix("operation/") {
            tokio::task::consume_budget().await;
            let operation: Operation = serde_json::from_str(json)?;
            if operation.boundary > fence {
                return Err(invalid_gap());
            }
            if operation.boundary <= selected {
                continue;
            }
            let id = Id::new(key.strip_prefix("operation/").ok_or_else(invalid_gap)?)?;
            let (request_revision, _, request) = rows
                .get_record::<OperationRequest>(&format!("request/{id}"))?
                .ok_or_else(invalid_gap)?;
            if revision <= 0
                || revision > view.revision
                || request_revision != revision
                || request.dataset() != dataset
                || request.operation() != &id
                || fingerprint(&request)? != operation.request_digest
                || !supported_format(operation.result_format)
            {
                return Err(invalid_gap());
            }
            // Format 10 schema operations advanced the admission counter. New
            // schema and all backfill operations preserve their source coverage.
            if matches!(
                request,
                OperationRequest::Backfill(_) | OperationRequest::Evidence(_)
            ) || (matches!(request, OperationRequest::Schema(_))
                && operation.result_format != PREVIOUS_FORMAT)
            {
                continue;
            }
            let (abandoned_revision, _, abandoned) = rows
                .get_record::<Abandonment>(&format!("abandoned/{id}"))?
                .ok_or_else(pending_source)?;
            if abandoned_revision <= revision
                || abandoned_revision > view.revision
                || abandoned.operation_revision != revision
                || abandoned.operation != id
                || abandoned.dataset != *dataset
                || abandoned.request_digest != operation.request_digest
                || abandoned.reason.trim().is_empty()
                || rows.contains_key(&format!("result/{id}"))
                || !boundaries.insert(operation.boundary)
            {
                return Err(invalid_gap());
            }
        }
        if boundaries.len() as u64 != gap {
            return Err(invalid_gap());
        }
        Ok(())
    }

    /// Stops future completion and selection for one admitted, unfinished operation.
    ///
    /// Identical retries return the retained decision. Completed phases remain
    /// reusable by other operation IDs. No payloads are read or removed.
    ///
    /// # Errors
    /// Refuses missing operations, selected results, conflicting reasons, empty
    /// reasons, or a stale native commit. Running I/O is not forcibly canceled.
    pub async fn abandon(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        let memory = self.request_memory(&(dataset, operation, reason))?;
        let dataset = dataset.clone();
        let operation = operation.clone();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.abandon_admitted(&dataset, &operation, &reason).await
        })
        .await
    }

    async fn abandon_admitted(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        self.require_data_work()?;
        if reason.trim().is_empty() {
            return Err(Error::refusal(
                "missing_reason",
                "abandonment requires a reason",
            ));
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        if reason.len() > lifecycle.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "abandonment reason exceeds metadata allowance",
            ));
        }
        if !lifecycle
            .catalog
            .table_exists(&crate::catalog::lifecycle_ident(dataset))
            .await?
        {
            return Err(Error::refusal(
                "no_operation",
                "operation has not been admitted",
            ));
        }
        let view = lifecycle.open(dataset).await?;
        if let Some(abandoned) = self
            .abandon_continuous_in(&view, dataset, operation, reason)
            .await?
        {
            return Ok(abandoned);
        }
        let rows = lifecycle.rows(&view).await?;
        let (revision, _, admitted) = rows
            .get_record::<Operation>(&format!("operation/{operation}"))?
            .ok_or_else(|| Error::refusal("no_operation", "operation has not been admitted"))?;
        let abandoned = Abandonment {
            dataset: dataset.clone(),
            operation: operation.clone(),
            request_digest: admitted.request_digest,
            operation_revision: revision,
            reason: reason.to_owned(),
        };
        let key = format!("abandoned/{operation}");
        if let Some((_, _, existing)) = rows.get_record::<Abandonment>(&key)? {
            if existing != abandoned {
                return Err(Error::refusal(
                    "abandonment_conflict",
                    "operation already has a different abandonment decision",
                ));
            }
            return Ok(existing);
        }
        if rows.contains_key(&format!("result/{operation}")) {
            return Err(Error::refusal(
                "operation_completed",
                "selected results remain available; abandonment only stops unfinished work",
            ));
        }
        lifecycle
            .commit(&view, vec![record(key, &abandoned)?])
            .await?;
        Ok(abandoned)
    }

    pub(super) async fn check_not_abandoned(&self, view: &View, operation: &Id) -> Result<()> {
        self.check_continuous_capability(view, operation).await?;
        if self
            .inner
            .lifecycle
            .rows(view)
            .await?
            .contains_key(&format!("abandoned/{operation}"))
        {
            return Err(Error::refusal(
                "operation_abandoned",
                "operation was explicitly abandoned; completed work remains retained",
            ));
        }
        Ok(())
    }

    /// Compacts superseded lifecycle revisions and transient records behind the active release.
    ///
    /// Coalesces historical lifecycle appends into one native baseline snapshot while
    /// preserving selected releases, retained results, active reader grants, exports,
    /// restore points, retirement receipts, and unfinished operations.
    ///
    /// # Errors
    /// Refuses remote-only engines without local authority, corrupt metadata, stale
    /// catalog views, or exhausted metadata capacity.
    pub async fn compact_lifecycle(
        &self,
        dataset: &Id,
    ) -> Result<crate::catalog::LifecycleCompactionResult> {
        let memory = self.request_memory(dataset)?;
        let dataset = dataset.clone();
        self.admit_work(memory, move |engine| async move {
            engine.compact_lifecycle_admitted(&dataset).await
        })
        .await
    }
}

fn invalid_gap() -> Error {
    Error::refusal(
        "invalid_admission_history",
        "admission interval or abandonment identities are incomplete or inconsistent",
    )
}

fn pending_source() -> Error {
    Error::refusal(
        "pending_source",
        "source admission is unresolved; select or explicitly abandon it before cutover",
    )
}
