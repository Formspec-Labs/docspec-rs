//! Retire publication payloads through the existing exact-generation owner.
// Rust guideline compliant 2026-02-21

use super::retirement::{RetiredRecord, invalid_retirement, marker_key, validate_receipt};
use super::*;
use crate::catalog::{SnapshotRows, history_row_bytes};
use crate::engine::job_publication::{
    self, INTENT_PREFIX, PAYLOAD_PREFIX, PublicationIntentAudit, PublicationIntentStatus,
};

// Fixture plan fields fit in the reserved history even at maximum JSON expansion.
const REASON_BYTES: usize = 256;
const METADATA_LOCATION_BYTES: usize = 4096;
const JSON_ESCAPE_BYTES_PER_INPUT_BYTE: usize = 6;

pub(in crate::engine) fn publication_plan_limits(metadata: &str, reason: &str) -> Result<()> {
    if metadata.len() > METADATA_LOCATION_BYTES || reason.len() > REASON_BYTES {
        return Err(Error::refusal(
            "publication_retirement_capacity",
            "publication retirement requires a bounded reason and local metadata location",
        ));
    }
    Ok(())
}

fn generation(audit: &PublicationIntentAudit) -> RetainedGeneration {
    RetainedGeneration {
        key: audit.reference.native_record.key.clone(),
        revision: audit.reference.native_record.revision,
    }
}

pub(super) fn scope_key(operation: &Id, key: &str) -> bool {
    key == format!("{PAYLOAD_PREFIX}{operation}")
}

fn future_charge(audit: &PublicationIntentAudit, generation: &RetainedGeneration) -> Result<usize> {
    // Serialize the actual two row types with their largest possible numeric
    // fields. Separately charge worst-case escaping for bounded variable text.
    let retirement = Id::new("x".repeat(128))?;
    let plan = RetentionPlan {
        dataset: audit.reference.dataset.clone(),
        retirement: retirement.clone(),
        scope: RetirementScope::PublicationIntent {
            operation: audit.reference.operation.clone(),
        },
        reason: String::new(),
        table_uuid: audit.reference.native_record.table_uuid.clone(),
        metadata: String::new(),
        snapshot_id: i64::MIN,
        revision: i64::MAX,
        records: vec![generation.clone()],
    };
    let marker = RetiredRecord {
        dataset: plan.dataset.clone(),
        table_uuid: plan.table_uuid.clone(),
        record: generation.clone(),
        retirement: retirement.clone(),
    };
    let receipt = Retirement {
        plan,
        revision: i64::MAX,
    };
    let receipt_bytes = history_row_bytes(
        &format!("retirement/{retirement}"),
        &serde_json::to_string(&receipt)?,
    );
    let marker_bytes = history_row_bytes(&marker_key(generation), &serde_json::to_string(&marker)?);
    let text = (REASON_BYTES + METADATA_LOCATION_BYTES)
        .checked_mul(JSON_ESCAPE_BYTES_PER_INPUT_BYTE)
        .ok_or_else(invalid_retirement)?;
    receipt_bytes
        .checked_add(marker_bytes)
        .and_then(|bytes| bytes.checked_add(text))
        .ok_or_else(invalid_retirement)
}

/// Charges live intents in warm, cold and compacted native lifecycle views.
pub(crate) fn future_bytes(
    rows: &BTreeMap<String, (i64, Arc<str>)>,
    view: &View,
    ceiling: i64,
    budget: &crate::budget::Budget,
) -> Result<usize> {
    let mut total = 0_usize;
    let mut prospective = view.clone();
    prospective.revision = ceiling;
    for (key, (revision, json)) in rows
        .range(INTENT_PREFIX.to_owned()..)
        .take_while(|(key, _)| key.starts_with(INTENT_PREFIX))
    {
        let _decoded = budget.reserve(json.len().saturating_mul(32).saturating_add(4096))?;
        let audit: PublicationIntentAudit = serde_json::from_str(json)?;
        job_publication::validate_audit(&audit, view, key, *revision, ceiling)?;
        let payload = generation(&audit);
        let present = rows.get(&payload.key).map(|(actual, _)| *actual);
        if present.is_some_and(|actual| actual != payload.revision) {
            return Err(invalid_retirement());
        }
        let Some((marker_revision, marker_json)) = rows.get(&marker_key(&payload)) else {
            if present.is_none() {
                return Err(invalid_retirement());
            }
            total = total
                .checked_add(future_charge(&audit, &payload)?)
                .ok_or_else(invalid_retirement)?;
            continue;
        };
        let _marker = budget.reserve(marker_json.len().saturating_mul(32).saturating_add(4096))?;
        let marker: RetiredRecord = serde_json::from_str(marker_json)?;
        let receipt_key = format!("retirement/{}", marker.retirement);
        let (receipt_revision, receipt_json) =
            rows.get(&receipt_key).ok_or_else(invalid_retirement)?;
        let _receipt =
            budget.reserve(receipt_json.len().saturating_mul(32).saturating_add(4096))?;
        let receipt: Retirement = serde_json::from_str(receipt_json)?;
        validate_receipt(&receipt, &prospective, &receipt_key, *receipt_revision)?;
        if marker.record != payload
            || marker.dataset != audit.reference.dataset
            || marker.table_uuid != audit.reference.native_record.table_uuid
            || marker_revision != receipt_revision
            || receipt.plan.scope
                != (RetirementScope::PublicationIntent {
                    operation: audit.reference.operation,
                })
            || receipt.plan.records != [payload]
        {
            return Err(invalid_retirement());
        }
        publication_plan_limits(&receipt.plan.metadata, &receipt.plan.reason)?;
    }
    for (key, _) in rows
        .range(PAYLOAD_PREFIX.to_owned()..)
        .take_while(|(key, _)| key.starts_with(PAYLOAD_PREFIX))
    {
        let operation = key
            .strip_prefix(PAYLOAD_PREFIX)
            .ok_or_else(invalid_retirement)?;
        if !rows.contains_key(&format!("{INTENT_PREFIX}{operation}")) {
            return Err(invalid_retirement());
        }
    }
    Ok(total)
}

fn collect(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    budget: &crate::budget::Budget,
) -> Result<Vec<RetainedGeneration>> {
    let saved = job_publication::retained_intent(rows, view, operation, budget)?
        .ok_or_else(invalid_retirement)?;
    if saved.status != PublicationIntentStatus::Retained {
        return Err(Error::refusal(
            "publication_intent_retired",
            "publication intent payload was already retired",
        ));
    }
    Ok(vec![generation(
        saved.intent.as_ref().ok_or_else(invalid_retirement)?,
    )])
}

pub(super) fn validate_plan(
    rows: &SnapshotRows,
    view: &View,
    plan: &RetentionPlan,
    operation: &Id,
    budget: &crate::budget::Budget,
) -> Result<()> {
    publication_plan_limits(&plan.metadata, &plan.reason)?;
    if plan.records != collect(rows, view, operation, budget)? {
        return Err(invalid_retirement());
    }
    Ok(())
}

pub(super) fn validate_saved(
    rows: &SnapshotRows,
    view: &View,
    receipt: &Retirement,
    budget: &crate::budget::Budget,
) -> Result<()> {
    if let RetirementScope::PublicationIntent { operation } = &receipt.plan.scope {
        publication_plan_limits(&receipt.plan.metadata, &receipt.plan.reason)?;
        let saved = job_publication::retained_intent(rows, view, operation, budget)?
            .ok_or_else(invalid_retirement)?;
        if saved.status != PublicationIntentStatus::Retired
            || receipt.plan.records
                != [generation(
                    saved.intent.as_ref().ok_or_else(invalid_retirement)?,
                )]
        {
            return Err(invalid_retirement());
        }
    }
    Ok(())
}

impl Engine {
    /// Captures one exact publication payload generation for retirement.
    ///
    /// # Errors
    /// Refuses a missing or retired intent, invalid reason, or exhausted capacity.
    pub async fn plan_publication_intent_retirement(
        &self,
        dataset: &Id,
        retirement: &Id,
        operation: &Id,
        reason: &str,
    ) -> Result<RetentionPlan> {
        self.require_local()?;
        publication_plan_limits("", reason)?;
        let memory = self.request_memory(&(dataset, retirement, operation, reason))?;
        let dataset = dataset.clone();
        let retirement = retirement.clone();
        let operation = operation.clone();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            let scope = RetirementScope::PublicationIntent {
                operation: operation.clone(),
            };
            engine
                .plan_retirement_in(dataset, retirement, scope, reason, |engine, view, rows| {
                    publication_plan_limits(view.table.metadata_location_result()?, "")?;
                    collect(rows, view, &operation, &engine.inner.lifecycle.budget)
                })
                .await
        })
        .await
    }

    /// Ends one publication payload's protection and preserves its audit.
    ///
    /// # Errors
    /// Refuses a stale, conflicting or invalid native retirement plan.
    pub async fn retire_publication_intent(&self, plan: &RetentionPlan) -> Result<Retirement> {
        self.require_local()?;
        if !matches!(plan.scope, RetirementScope::PublicationIntent { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
