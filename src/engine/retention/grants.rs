//! End unused owner protection without changing independently retained consumer promises.
// Rust guideline compliant 2026-02-21

use super::owners::{OwnerProtection, OwnerProtectionRef, ReferenceBinding};
use super::retirement::{invalid_retirement, record_retired};
use super::*;
use crate::catalog::SnapshotRows;

#[cfg(all(test, feature = "test-util"))]
mod tests;

pub(super) fn grant_key(key: &str) -> bool {
    key.strip_prefix("owner-protection/").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

// A retired generation can be replaced at its content-addressed key. The key
// fixes owner, consumer and exact scope; the receipt keeps its original revision.
pub(super) fn validate_saved(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    engine: &Engine,
) -> Result<()> {
    if receipt.plan.scope != RetirementScope::OwnerGrants {
        return Ok(());
    }
    for record in &receipt.plan.records {
        let _memory = rows.reserve_decoded(&engine.inner.lifecycle.budget, &[&record.key])?;
        let (revision, _, grant): (_, _, OwnerProtection) = rows
            .get_record(&record.key)?
            .ok_or_else(invalid_retirement)?;
        owners::validate_grant(view, &record.key, revision, &grant)?;
        if revision < record.revision
            || (revision != record.revision && revision <= receipt.revision)
        {
            return Err(invalid_retirement());
        }
    }
    Ok(())
}

/// Scans each named consumer once, without catalog enumeration or payload expansion.
/// Execution calls this while exclusive admitted-work ownership prevents new claims.
pub(super) async fn check_eligible(
    engine: &Engine,
    view: &View,
    rows: &SnapshotRows,
    records: &[RetainedGeneration],
) -> Result<()> {
    let lifecycle = &engine.inner.lifecycle;
    let budget = &lifecycle.budget;
    let limit = budget.limits.max_metadata_bytes;
    let _targets = budget.reserve(limit)?;
    let mut bytes = 0;
    let mut consumers: BTreeMap<Id, BTreeMap<String, OwnerProtectionRef>> = BTreeMap::new();
    for record in records {
        if !grant_key(&record.key) {
            return Err(invalid_retirement());
        }
        let _memory = rows.reserve_decoded(budget, &[&record.key])?;
        let (revision, _, grant): (_, _, OwnerProtection) = rows
            .get_record(&record.key)?
            .ok_or_else(invalid_retirement)?;
        owners::validate_grant(view, &record.key, revision, &grant)?;
        if revision != record.revision {
            return Err(Error::refusal(
                "stale_retirement",
                "owner grant generation changed; capture a fresh plan",
            ));
        }
        if record_retired(rows, view, &record.key, revision, budget)? {
            return Err(Error::refusal(
                "grant_retired",
                "owner grant generation is already retired",
            ));
        }
        let reference = OwnerProtectionRef {
            owner: grant.owner,
            token: record.key.clone(),
            table_uuid: grant.table_uuid,
            revision,
        };
        // Include map nodes and owned token/consumer strings, not object payloads.
        charge(
            &mut bytes,
            inputs::metadata_size(&reference, limit)?
                .saturating_mul(2)
                .saturating_add(grant.consumer.as_str().len())
                .saturating_add(256),
            limit,
        )?;
        if consumers
            .entry(grant.consumer)
            .or_default()
            .insert(record.key.clone(), reference)
            .is_some()
        {
            return Err(invalid_retirement());
        }
    }
    for (consumer, targets) in consumers {
        tokio::task::consume_budget().await;
        let ident = crate::catalog::lifecycle_ident(&consumer);
        // Failed handoff can leave a grant before the consumer table exists.
        // Planning does not create a table merely to prove no claim exists.
        if !lifecycle.catalog.table_exists(&ident).await? {
            continue;
        }
        let table = lifecycle.catalog.load_table(&ident).await?;
        let consumer_view = View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: ident,
        };
        let consumer_rows = lifecycle.rows(&consumer_view).await?;
        // Keep set state separate from actual per-record decoding, especially
        // when a small operation record refers to a large retained base result.
        let _claims = budget.reserve(limit)?;
        let mut retired_bytes = 0;
        let retired =
            retirement::retired_records(&consumer_rows, &consumer_view, engine, |bytes| {
                charge(&mut retired_bytes, bytes, limit)
            })?;
        for (key, revision, json) in consumer_rows.prefix("reference-binding/") {
            tokio::task::consume_budget().await;
            let mut memory = consumer_rows.reserve_decoded(budget, &[key])?;
            let binding: ReferenceBinding = serde_json::from_str(json)?;
            memory.merge(owners::reserve_claim(&consumer_rows, budget, &binding.key)?);
            let generation = RetainedGeneration {
                key: binding.key.clone(),
                revision,
            };
            if owners::earlier_binding(&consumer_rows, &consumer_view, key, revision, &binding)? {
                if !retired.records.contains(&generation) {
                    return Err(invalid_retirement());
                }
                continue;
            }
            // A malformed or unknown bound claim cannot authorize releasing its tokens.
            owners::validate_binding(
                &consumer_rows,
                &consumer_view,
                key,
                revision,
                &binding,
                limit,
            )?;
            if retired.records.contains(&generation) {
                continue;
            }
            for reference in &binding.protections {
                if let Some(target) = targets.get(&reference.token) {
                    if reference != target {
                        return Err(invalid_retirement());
                    }
                    return Err(Error::refusal(
                        "grant_in_use",
                        format!(
                            "owner grant {} still protects {}/{}",
                            reference.token, consumer, binding.key
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn charge(bytes: &mut usize, added: usize, limit: usize) -> Result<()> {
    *bytes = bytes.saturating_add(added);
    if *bytes > limit {
        return Err(capacity());
    }
    Ok(())
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "owner grant retirement exceeds its metadata allowance",
    )
}

impl Engine {
    /// Captures unused owner grants for explicit, exact-generation retirement.
    ///
    /// Live consumer claims block retirement. Planning reads their lifecycle
    /// metadata only; it neither retires promises nor deletes objects.
    ///
    /// # Errors
    /// Refuses active, missing, retired or malformed grants, duplicate keys,
    /// unsupported consumer bindings, empty reasons, or exhausted capacity.
    pub async fn plan_grant_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        keys: &[String],
        reason: &str,
    ) -> Result<RetentionPlan> {
        let memory = self.request_memory(&(dataset, retirement, keys, reason))?;
        let (dataset, retirement, mut keys, reason) = (
            dataset.clone(),
            retirement.clone(),
            keys.to_vec(),
            reason.to_owned(),
        );
        self.admit_work(memory, move |engine| async move {
            engine.retirement_catalog()?;
            keys.sort();
            if keys.is_empty()
                || keys.windows(2).any(|pair| pair[0] == pair[1])
                || keys.iter().any(|key| !grant_key(key))
            {
                return Err(invalid_retirement());
            }
            let (_guard, view, rows) = engine.open_retirement_view(&dataset, &reason).await?;
            let records = keys
                .into_iter()
                .map(|key| {
                    let revision = rows
                        .revision(&key)
                        .ok_or_else(|| Error::refusal("no_grant", "owner grant is unavailable"))?;
                    Ok(RetainedGeneration { key, revision })
                })
                .collect::<Result<Vec<_>>>()?;
            check_eligible(&engine, &view, &rows, &records).await?;
            retirement::plan(
                &view,
                dataset,
                retirement,
                RetirementScope::OwnerGrants,
                reason,
                records,
            )
        })
        .await
    }

    /// Retires captured owner grants after rechecking their consumer claims.
    ///
    /// Holds exclusive admitted-work ownership through the native decision.
    /// Retired grants remain creation evidence; reacquisition creates a new generation.
    ///
    /// # Errors
    /// Refuses live consumers, changed generations, invalid scope, conflicting
    /// retry identity, malformed metadata, or exhausted capacity.
    pub async fn retire_grants(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if plan.scope != RetirementScope::OwnerGrants {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
