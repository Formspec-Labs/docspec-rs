//! Explicit operation abandonment; completed phases and every object remain retained.
//!
//! This ends an operation's permission to acknowledge work or select a result.
//! Already-started I/O can finish under its existing protection. Abandonment is
//! neither worker quiescence nor permission to delete a phase or attempt.

use super::*;
use crate::catalog::View;

impl Engine {
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
        let view = lifecycle.open(dataset).await?;
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
}
