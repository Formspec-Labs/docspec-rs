//! Explicitly end compaction recovery while preserving the selected physical layout.
// Rust guideline compliant 2026-02-21

use super::retirement::{invalid_retirement, record_retired};
use super::*;
use crate::{budget::Budget, catalog::SnapshotRows, engine::compaction::Capture};

#[cfg(all(test, feature = "test-util"))]
mod tests;

const FAMILIES: [&str; 4] = [
    "compaction-request",
    "compaction-prepared",
    "compaction-verified",
    "compaction-result",
];

fn keys(operation: &Id) -> [String; 4] {
    FAMILIES.map(|family| format!("{family}/{operation}"))
}

pub(super) fn recovery_key(key: &str) -> bool {
    key.split_once('/')
        .is_some_and(|(family, operation)| FAMILIES.contains(&family) && Id::new(operation).is_ok())
}

pub(super) fn scope_key(operation: &Id, key: &str) -> bool {
    keys(operation).iter().any(|candidate| candidate == key)
        || key
            .strip_prefix("protection/")
            .is_some_and(|id| Id::new(id).is_ok())
}

/// Checks the fixed recovery chain using retained metadata only.
fn fixed(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    boundary: i64,
    budget: &Budget,
) -> Result<(i64, Vec<RetainedGeneration>)> {
    let keys = keys(operation);
    let _decoded =
        rows.reserve_decoded(budget, &keys.iter().map(String::as_str).collect::<Vec<_>>())?;
    let (admitted, _, capture): (_, _, Capture) = rows
        .get_record(&keys[0])?
        .ok_or_else(|| Error::refusal("no_compaction", "compaction has not been admitted"))?;
    if admitted <= 0
        || admitted > boundary
        || boundary > view.revision
        || capture.spec.operation != *operation
        || capture.manifest.owner != capture.spec.dataset
        || &crate::catalog::lifecycle_ident(&capture.spec.dataset) != view.identifier()
    {
        return Err(invalid_retirement());
    }
    let mut records = vec![RetainedGeneration {
        key: keys[0].clone(),
        revision: admitted,
    }];
    let mut previous: Option<CompactionResult> = None;
    let mut gap = false;
    for (index, key) in keys.iter().enumerate().skip(1) {
        let Some((current, _, result)) = rows.get_record::<CompactionResult>(key)? else {
            gap = true;
            continue;
        };
        // Concurrent identical workers can acknowledge verification after the
        // winning selection. Exact contents and captured generations matter;
        // these independent acknowledgments need not be revision-sorted.
        if gap || current <= admitted || current > boundary || result.verified != (index > 1) {
            return Err(invalid_retirement());
        }
        crate::engine::compaction::check_result(&capture, &result)?;
        if let Some(mut expected) = previous {
            expected.verified = true;
            if expected != result {
                return Err(invalid_retirement());
            }
        }
        records.push(RetainedGeneration {
            key: key.clone(),
            revision: current,
        });
        previous = Some(result);
    }
    records.sort();
    Ok((admitted, records))
}

fn check_attempt(
    root: &Path,
    view: &View,
    operation: &Id,
    admitted: i64,
    boundary: i64,
    generation: &RetainedGeneration,
    attempt: &Protection,
) -> Result<()> {
    let directory = root
        .join("objects")
        .join(attempt.consumer.as_str())
        .join(attempt.id.as_str());
    if generation.key != format!("protection/{}", attempt.id)
        || generation.revision <= admitted
        || generation.revision > boundary
        || attempt.purpose != format!("{operation}/compaction")
        || &crate::catalog::lifecycle_ident(&attempt.consumer) != view.identifier()
        || attempt.paths != [format!("{}/", directory.display())]
        || attempt.retired
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

pub(super) fn collect(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    root: &Path,
    budget: &Budget,
) -> Result<Vec<RetainedGeneration>> {
    let limit = budget.limits.max_metadata_bytes;
    let _state = budget.reserve(limit)?;
    let (admitted, mut records) = fixed(rows, view, operation, view.revision, budget)?;
    let mut bytes = records
        .iter()
        .map(|record| record.key.len().saturating_mul(2).saturating_add(128))
        .sum::<usize>();
    let purpose = format!("{operation}/compaction");
    for (key, revision, json) in rows.prefix("protection/") {
        let _decoded = rows.reserve_decoded(budget, &[key])?;
        let attempt: Protection = serde_json::from_str(json)?;
        if attempt.purpose != purpose {
            continue;
        }
        let generation = RetainedGeneration {
            key: key.into(),
            revision,
        };
        check_attempt(
            root,
            view,
            operation,
            admitted,
            view.revision,
            &generation,
            &attempt,
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

/// Checks captured identities without rescanning every attempt for each receipt.
pub(super) fn validate_saved(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    engine: &Engine,
) -> Result<()> {
    let RetirementScope::CompactionRecovery { operation } = &receipt.plan.scope else {
        return Ok(());
    };
    let budget = &engine.inner.lifecycle.budget;
    let (admitted, fixed) = fixed(rows, view, operation, receipt.plan.revision, budget)?;
    if fixed
        .iter()
        .any(|record| receipt.plan.records.binary_search(record).is_err())
    {
        return Err(invalid_retirement());
    }
    for generation in &receipt.plan.records {
        if rows.revision(&generation.key) != Some(generation.revision) {
            return Err(invalid_retirement());
        }
        if generation.key.starts_with("protection/") {
            let _decoded = rows.reserve_decoded(budget, &[&generation.key])?;
            let (_, _, attempt) = rows
                .get_record::<Protection>(&generation.key)?
                .ok_or_else(invalid_retirement)?;
            check_attempt(
                &engine.inner.root,
                view,
                operation,
                admitted,
                receipt.plan.revision,
                generation,
                &attempt,
            )?;
        }
    }
    Ok(())
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "compaction retirement exceeds its metadata allowance",
    )
}

impl Engine {
    /// Captures all recovery records for one compaction without ending any promise.
    ///
    /// Execution explicitly ends unfinished work and completed rewrite recovery.
    /// Selected layouts and other users of the same files remain independent roots.
    ///
    /// # Errors
    /// Refuses missing or retired requests, invalid reasons, inconsistent records,
    /// unqualified catalogs, or exhausted metadata capacity.
    pub async fn plan_compaction_retirement(
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
            let scope = RetirementScope::CompactionRecovery {
                operation: operation.clone(),
            };
            engine
                .plan_retirement_in(dataset, retirement, scope, reason, |engine, view, rows| {
                    let key = format!("compaction-request/{operation}");
                    if let Some(revision) = rows.revision(&key)
                        && record_retired(
                            rows,
                            view,
                            &key,
                            revision,
                            &engine.inner.lifecycle.budget,
                        )?
                    {
                        return Err(Error::refusal(
                            "compaction_retired",
                            "compaction recovery is already retired",
                        ));
                    }
                    collect(
                        rows,
                        view,
                        &operation,
                        &engine.inner.root,
                        &engine.inner.lifecycle.budget,
                    )
                })
                .await
        })
        .await
    }

    /// Ends captured compaction recovery while preserving independently promised data.
    ///
    /// Waits for admitted workers and readers, then retires exact generations.
    /// Retained rows remain identity and creation evidence. No files are deleted.
    ///
    /// # Errors
    /// Refuses changed recovery records, malformed scope, conflicting retry,
    /// unsupported catalogs, or exhausted metadata capacity.
    pub async fn retire_compaction(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if !matches!(plan.scope, RetirementScope::CompactionRecovery { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
