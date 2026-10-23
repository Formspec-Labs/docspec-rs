//! Explicit retirement of optional change certificates, independent of complete state.

use super::*;
use crate::engine::retention::retirement as retained;

fn invalid(reason: impl Into<String>) -> Error {
    Error::refusal("invalid_retirement", reason)
}

impl Engine {
    /// Captures exact optional change certificates for explicit retirement.
    ///
    /// This checks catalog metadata only. Complete results, recovery phases and
    /// independent consumer protections retain their own availability promises.
    ///
    /// # Errors
    /// Refuses duplicate or missing versions, empty requests or reasons, retired
    /// certificates, inconsistent selections, unqualified catalogs or capacity exhaustion.
    pub async fn plan_change_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        versions: &[Id],
        reason: &str,
    ) -> Result<RetentionPlan> {
        let memory = self.request_memory(&(dataset, retirement, versions, reason))?;
        let dataset = dataset.clone();
        let retirement = retirement.clone();
        let mut versions = versions.to_vec();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.retirement_catalog()?;
            versions.sort();
            if versions.is_empty()
                || reason.trim().is_empty()
                || versions.windows(2).any(|pair| pair[0] == pair[1])
            {
                return Err(invalid(
                    "change retirement requires distinct versions and a non-empty reason",
                ));
            }
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            let lifecycle = &engine.inner.lifecycle;
            let view = lifecycle.open(&dataset).await?;
            let rows = lifecycle.rows(&view).await?;
            let mut records = Vec::with_capacity(versions.len());
            for version in versions {
                let key = format!("change-certificate/{version}");
                let result_key = format!("result/{version}");
                let _decoded = rows.reserve_decoded(&lifecycle.budget, &[&key, &result_key])?;
                let (revision, _, certificate): (_, _, ChangeCertificate) =
                    rows.get_record(&key)?.ok_or_else(|| {
                        Error::refusal("no_changes", "requested optional changes are unavailable")
                    })?;
                if certificate.after.dataset != dataset || revision > view.revision {
                    return Err(invalid(
                        "change certificate dataset or revision does not match the opened catalog view",
                    ));
                }
                check_certificate(&rows, revision, &key, &certificate)?;
                if retained::record_retired(&rows, &view, &key, revision, &lifecycle.budget)? {
                    return Err(Error::refusal(
                        "changes_retired",
                        "requested optional changes are already retired",
                    ));
                }
                records.push(RetainedGeneration { key, revision });
            }
            retained::plan(
                &view,
                dataset,
                retirement,
                RetirementScope::OptionalChanges,
                reason,
                records,
            )
        })
        .await
    }

    /// Retires captured optional change certificates without deleting physical objects.
    ///
    /// Identical retries reconcile the existing guarded decision. Independent
    /// phase, result and consumer protections remain active.
    ///
    /// # Errors
    /// Refuses another retirement scope, changed generations, conflicting retries,
    /// malformed selection metadata, unqualified catalogs or capacity exhaustion.
    pub async fn retire_changes(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if plan.scope != RetirementScope::OptionalChanges {
            return Err(invalid(
                "retire_changes requires an OptionalChanges retention plan",
            ));
        }
        self.retire_retained(plan).await
    }
}
