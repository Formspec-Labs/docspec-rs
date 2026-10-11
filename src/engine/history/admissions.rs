//! Qualify every source admission against one captured accepted history.
// Rust guideline compliant 2026-02-21

use super::*;
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

#[derive(Default, Deserialize)]
struct AdmissionAuthority {
    boundary: u64,
    eligibility: u64,
}

#[derive(Deserialize)]
struct AdmissionResult {
    dataset: AdmissionDataset,
}

#[derive(Deserialize)]
struct AdmissionDataset {
    format: u32,
    request: OperationRequest,
}

// Conservatively charge decoded strings, map nodes, serialization copies and
// temporary keys before allocation. This is admission accounting, not heap RSS.
fn charge(used: &mut usize, serialized: usize, limit: usize) -> Result<()> {
    *used = used
        .checked_add(serialized.checked_mul(8).ok_or_else(history_capacity)?)
        .and_then(|bytes| bytes.checked_add(512))
        .filter(|bytes| *bytes <= limit)
        .ok_or_else(history_capacity)?;
    Ok(())
}

impl Engine {
    /// Returns source admissions requiring selection or explicit abandonment.
    ///
    /// Uses the captured lifecycle view and reads metadata only. Selected history
    /// remains valid after result availability retirement. Composition operations
    /// do not create a source backlog.
    ///
    /// # Errors
    /// Refuses malformed history, unmatched receipts, incomplete source admission
    /// boundaries and metadata exceeding the configured allowance.
    pub(in crate::engine) async fn pending_source_operations(
        &self,
        view: &View,
        dataset: &Id,
    ) -> Result<Vec<Id>> {
        if &crate::catalog::lifecycle_ident(dataset) != view.identifier() {
            return Err(invalid("admission view belongs to a different dataset"));
        }
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let _metadata = lifecycle.budget.reserve(limit)?;
        let rows = lifecycle.rows(view).await?;
        let mut used = 0;
        charge(&mut used, rows.record_bytes("authority"), limit)?;
        let authority = rows
            .get_record::<AdmissionAuthority>("authority")?
            .map(|(_, _, value)| value)
            .unwrap_or_default();
        let head = read_head(&rows, view.revision)?;
        let mut selected = BTreeMap::new();
        if let Some((selection_revision, head)) = head {
            if head.dataset.dataset != *dataset {
                return Err(invalid("selected head belongs to a different dataset"));
            }
            let mut current = head.version_ref();
            let mut previous_revision = None;
            let mut previous_boundary = authority.boundary;
            let mut previous_eligibility = authority.eligibility;
            loop {
                // Admit both decodes, including read_link's temporary JSON value,
                // before invoking the shared ordering and result receipt checks.
                let link_key = format!("succession/{}", current.version);
                let result_key = format!("result/{}", current.version);
                charge(&mut used, rows.record_bytes(&link_key), limit)?;
                charge(&mut used, rows.record_bytes(&result_key), limit)?;
                let (revision, _, link) = read_link(&rows, view.revision, &current)?;
                if previous_revision.is_none() {
                    check_selected_link(selection_revision, &head, revision, &link)?;
                }
                if previous_revision.is_some_and(|child| revision >= child)
                    || link.boundary > previous_boundary
                    || link.eligibility > previous_eligibility
                {
                    return Err(invalid(
                        "accepted predecessor revisions or coverage are inconsistent",
                    ));
                }
                previous_revision = Some(revision);
                previous_boundary = link.boundary;
                previous_eligibility = link.eligibility;
                let predecessor = link.predecessor.clone();
                if selected.insert(current.version, (revision, link)).is_some() {
                    return Err(invalid("accepted history repeats an operation"));
                }
                let Some(predecessor) = predecessor else {
                    break;
                };
                current = predecessor;
                tokio::task::consume_budget().await;
            }
        }
        let mut boundaries = BTreeSet::new();
        let mut pending = Vec::new();
        for (key, revision, json) in rows.prefix("operation/") {
            charge(&mut used, json.len().saturating_add(key.len()), limit)?;
            let id = Id::new(
                key.strip_prefix("operation/")
                    .ok_or_else(|| invalid("invalid operation key"))?,
            )?;
            let request_key = format!("request/{id}");
            charge(&mut used, rows.record_bytes(&request_key), limit)?;
            let operation: Operation = serde_json::from_str(json)?;
            let (request_revision, _, request) = rows
                .get_record::<OperationRequest>(&request_key)?
                .ok_or_else(|| invalid("admitted request is missing"))?;
            if revision <= 0
                || revision > view.revision
                || request_revision != revision
                || request.operation() != &id
                || request.dataset() != dataset
                || fingerprint(&request)? != operation.request_digest
                || !supported_format(operation.result_format)
                || operation.boundary > authority.boundary
                || operation.eligibility > authority.eligibility
            {
                return Err(invalid("source admission identity or revision differs"));
            }
            let source = matches!(
                request,
                OperationRequest::Build(_)
                    | OperationRequest::Update(_)
                    | OperationRequest::Refresh(_)
            ) || (matches!(request, OperationRequest::Schema(_))
                && operation.result_format == PREVIOUS_FORMAT);
            if source && (operation.boundary == 0 || !boundaries.insert(operation.boundary)) {
                return Err(invalid("source admission boundary is zero or duplicated"));
            }
            let result_key = format!("result/{id}");
            let abandoned_key = format!("abandoned/{id}");
            if let Some((selection_revision, link)) = selected.remove(&id) {
                let result = rows
                    .get_record::<AdmissionResult>(&result_key)?
                    .ok_or_else(|| invalid("selected admission result is missing"))?
                    .2;
                if selection_revision <= revision
                    || link.boundary != operation.boundary
                    || link.eligibility != operation.eligibility
                    || result.dataset.format != operation.result_format
                    || result.dataset.request != request
                    || rows.contains_key(&abandoned_key)
                {
                    return Err(invalid("selected result differs from its source admission"));
                }
            } else if rows.contains_key(&result_key) {
                return Err(invalid("result lacks qualified accepted succession"));
            } else if source {
                charge(&mut used, rows.record_bytes(&abandoned_key), limit)?;
                if let Some((abandoned_revision, _, abandoned)) =
                    rows.get_record::<Abandonment>(&abandoned_key)?
                {
                    if abandoned_revision <= revision
                        || abandoned_revision > view.revision
                        || abandoned.operation_revision != revision
                        || abandoned.operation != id
                        || abandoned.dataset != *dataset
                        || abandoned.request_digest != operation.request_digest
                        || abandoned.reason.trim().is_empty()
                    {
                        return Err(invalid("abandonment differs from its source admission"));
                    }
                } else {
                    pending.push(id);
                }
            }
            tokio::task::consume_budget().await;
        }
        if !selected.is_empty() || boundaries.len() as u64 != authority.boundary {
            return Err(invalid(
                "accepted history or source admission boundaries are incomplete",
            ));
        }
        Ok(pending)
    }
}
