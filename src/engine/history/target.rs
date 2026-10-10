//! Prove a captured computed target's meaning through accepted selections.
// Rust guideline compliant 2026-02-21

use super::*;

#[cfg(test)]
mod tests;

// Skip native file inventories and unrelated evidence payload descriptions.
#[derive(Deserialize)]
struct TargetRelease {
    dataset: TargetDataset,
}

#[derive(Deserialize)]
struct TargetDataset {
    format: u32,
    definition: String,
    request: OperationRequest,
    parent_manifest: Option<ObjectRef>,
    groups: BTreeMap<String, NativeSnapshotRef>,
    computed: BTreeMap<String, ComputedGroup>,
}

fn charge(used: &mut usize, serialized: usize, limit: usize) -> Result<()> {
    // Bound decoded maps/strings, temporary JSON, and keys before decoding.
    *used = used
        .checked_add(serialized.checked_mul(8).ok_or_else(history_capacity)?)
        .and_then(|bytes| bytes.checked_add(512))
        .filter(|bytes| *bytes <= limit)
        .ok_or_else(history_capacity)?;
    Ok(())
}

fn conflict(reason: &str) -> Error {
    Error::refusal("backfill_target_conflict", reason)
}

impl Engine {
    /// Proves unchanged target intent across a captured accepted selection chain.
    ///
    /// Reads retained lifecycle metadata only, including after payload retirement.
    /// Source operations may rebuild the target together with the source; sibling
    /// backfills and evidence edits may preserve it. Every competing same-group
    /// backfill conflicts, even if it restores an earlier definition. Authored
    /// input field IDs must remain exact; full rebuild ID remapping is unsupported.
    ///
    /// # Errors
    /// Refuses changed target meaning, missing or malformed accepted history,
    /// mismatched endpoint/request receipts, and metadata allowance exhaustion.
    pub(in crate::engine) async fn check_backfill_target_lineage(
        &self,
        view: &View,
        original: &ReleasePin,
        current: &ReleasePin,
        group: &Id,
    ) -> Result<()> {
        let dataset = &original.dataset.dataset;
        if current.dataset.dataset != *dataset
            || &crate::catalog::lifecycle_ident(dataset) != view.table.identifier()
            || group.as_str() == "source"
        {
            return Err(invalid(
                "target endpoints or lifecycle view belong to different datasets",
            ));
        }
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let _metadata = lifecycle.budget.reserve(limit)?;
        let rows = lifecycle.rows(view).await?;
        let mut used = 0;
        charge(&mut used, rows.record_bytes("authority"), limit)?;
        let (selection_revision, head) = read_head(&rows, view.revision)?
            .ok_or_else(|| invalid("target lineage requires an accepted selection"))?;
        if head.dataset.dataset != *dataset {
            return Err(invalid("selected head belongs to a different dataset"));
        }
        let original_ref = original.version_ref();
        let current_ref = current.version_ref();
        let mut cursor = head.version_ref();
        let mut previous_revision = None;
        let mut previous_boundary = None;
        let mut previous_eligibility = None;
        let mut found_current = false;
        let mut child: Option<(VersionRef, TargetDataset)> = None;
        loop {
            let result_key = format!("result/{}", cursor.version);
            charge(
                &mut used,
                rows.record_bytes(&format!("succession/{}", cursor.version)),
                limit,
            )?;
            charge(&mut used, rows.record_bytes(&result_key), limit)?;
            let (revision, _, link) = read_link(&rows, view.revision, &cursor)?;
            if previous_revision.is_none() {
                check_selected_link(selection_revision, &head, revision, &link)?;
            }
            if previous_revision.is_some_and(|child| revision >= child)
                || previous_boundary.is_some_and(|child| link.boundary > child)
                || previous_eligibility.is_some_and(|child| link.eligibility > child)
            {
                return Err(invalid(
                    "accepted predecessor revisions or coverage are inconsistent",
                ));
            }
            for endpoint in [&original_ref, &current_ref] {
                if cursor.version == endpoint.version && cursor != *endpoint {
                    return Err(invalid("target endpoint has a different manifest identity"));
                }
            }
            for endpoint in [original, current] {
                if cursor == endpoint.version_ref()
                    && (endpoint.dataset.boundary != link.boundary
                        || endpoint.dataset.eligibility != link.eligibility)
                {
                    return Err(invalid("captured target endpoint has different coverage"));
                }
            }
            if cursor == current_ref {
                found_current = true;
            }
            if cursor == original_ref && !found_current {
                return Err(invalid(
                    "current target endpoint precedes its original capture",
                ));
            }
            let value = read_target(&rows, revision, &link, &mut used, limit)?;
            if found_current {
                if cursor == current_ref {
                    check_endpoint(current, &value, group)?;
                }
                if cursor == original_ref {
                    check_endpoint(original, &value, group)?;
                }
                check_meaning(&original.dataset, &value, group)?;
                if let Some((child_ref, child_value)) = &child {
                    check_step(&cursor, &value, child_ref, child_value, group)?;
                }
                if cursor == original_ref {
                    return Ok(());
                }
                if matches!(&value.request, OperationRequest::Backfill(spec) if spec.group == *group)
                {
                    return Err(conflict("another backfill selected this target group"));
                }
                child = Some((cursor.clone(), value));
            }
            let Some(predecessor) = link.predecessor else {
                return Err(invalid(
                    "both target endpoints must belong to the observed accepted chain",
                ));
            };
            cursor = predecessor;
            previous_revision = Some(revision);
            previous_boundary = Some(link.boundary);
            previous_eligibility = Some(link.eligibility);
            tokio::task::consume_budget().await;
        }
    }
}

fn read_target(
    rows: &SnapshotRows,
    selection_revision: i64,
    link: &Succession,
    used: &mut usize,
    limit: usize,
) -> Result<TargetDataset> {
    let expected = &link.selected;
    let operation_key = format!("operation/{}", expected.version);
    let request_key = format!("request/{}", expected.version);
    charge(used, rows.record_bytes(&operation_key), limit)?;
    charge(used, rows.record_bytes(&request_key), limit)?;
    let (admission_revision, _, operation) = rows
        .get_record::<Operation>(&operation_key)?
        .ok_or_else(|| invalid("selected target operation is missing"))?;
    let (request_revision, _, request) = rows
        .get_record::<OperationRequest>(&request_key)?
        .ok_or_else(|| invalid("selected target request is missing"))?;
    let value = rows
        .get_record::<TargetRelease>(&format!("result/{}", expected.version))?
        .ok_or_else(|| invalid("selected target result is missing"))?
        .2
        .dataset;
    if admission_revision <= 0
        || admission_revision >= selection_revision
        || request_revision != admission_revision
        || request.dataset() != &expected.dataset
        || request.operation() != &expected.version
        || fingerprint(&request)? != operation.request_digest
        || operation.boundary != link.boundary
        || operation.eligibility != link.eligibility
        || !supported_format(operation.result_format)
        || value.format != operation.result_format
        || value.request.dataset() != &expected.dataset
        || value.request.operation() != &expected.version
        // Source admission relocates inputs into retained storage. Their byte
        // identity, rather than the original external path, binds the request.
        || fingerprint(&value.request)? != operation.request_digest
        || value.definition != request.result_definition()
        || operation.definition != request.definition()
        || operation.base.as_ref() != request.base()
        || rows.contains_key(&format!("abandoned/{}", expected.version))
    {
        return Err(invalid("target result differs from its admitted request"));
    }
    if !matches!(&request, OperationRequest::Backfill(_)) {
        // Build/update bind their base by operation ID; only some composition
        // requests also retain Operation::base_manifest. Resolve the exact
        // accepted receipt rather than requiring that optional field everywhere.
        let expected_parent = if let Some(base) = request.base() {
            let key = format!("result/{base}");
            charge(used, rows.record_bytes(&key), limit)?;
            let (revision, _, parent) = rows
                .get_record::<SelectedRelease>(&key)?
                .ok_or_else(|| invalid("target request base receipt is missing"))?;
            if revision >= admission_revision
                || parent.id != *base
                || parent.dataset.version != *base
                || parent.dataset.dataset != expected.dataset
                || !parent.verified
            {
                return Err(invalid(
                    "target request base is not an earlier accepted result",
                ));
            }
            Some(parent.manifest)
        } else {
            None
        };
        if value.parent_manifest != expected_parent
            || operation
                .base_manifest
                .as_ref()
                .is_some_and(|manifest| Some(manifest) != expected_parent.as_ref())
        {
            return Err(invalid(
                "target composition parent differs from its admitted capture",
            ));
        }
    }
    if !value.groups.contains_key("source") {
        return Err(invalid("target history has no source metadata"));
    }
    Ok(value)
}

fn check_endpoint(endpoint: &ReleasePin, value: &TargetDataset, group: &Id) -> Result<()> {
    let name = group.as_str();
    if !endpoint.verified
        || endpoint.id != endpoint.dataset.version
        || endpoint.dataset.format != value.format
        || endpoint.dataset.definition != value.definition
        || endpoint.dataset.request != value.request
        || endpoint.dataset.parent_manifest != value.parent_manifest
        || endpoint.dataset.computed.get(name) != value.computed.get(name)
        || endpoint
            .dataset
            .groups
            .get(name)
            .map(NativeSnapshotRef::from)
            != value.groups.get(name).cloned()
        || endpoint
            .dataset
            .groups
            .get("source")
            .map(NativeSnapshotRef::from)
            != value.groups.get("source").cloned()
    {
        return Err(invalid(
            "captured target endpoint differs from its retained result",
        ));
    }
    Ok(())
}

fn check_meaning(original: &DatasetPin, value: &TargetDataset, group: &Id) -> Result<()> {
    let name = group.as_str();
    if let Some(binding) = value.computed.get(name) {
        let source = value
            .groups
            .get("source")
            .ok_or_else(|| invalid("target history has no source metadata"))?;
        if binding.request.dataset() != &original.dataset
            || binding.source_metadata != source.metadata
            || binding.input_fields.values().any(|field| *field <= 0)
            || matches!(&binding.request, OperationRequest::Backfill(spec)
                if spec.group_definition() != binding.definition)
        {
            return Err(invalid(
                "target group has inconsistent source or request bindings",
            ));
        }
    }
    match (original.computed.get(name), value.computed.get(name)) {
        (None, None) if !original.groups.contains_key(name) && !value.groups.contains_key(name) => {
            Ok(())
        }
        (Some(before), Some(after))
            if value.groups.contains_key(name)
                && original.groups.contains_key(name)
                && before.definition == after.definition
                && before.input_fields == after.input_fields
                && after.definition.group == *group =>
        {
            Ok(())
        }
        _ => Err(conflict(
            "target presence, definition or authored input field IDs changed",
        )),
    }
}

fn check_step(
    parent_ref: &VersionRef,
    parent: &TargetDataset,
    child_ref: &VersionRef,
    child: &TargetDataset,
    group: &Id,
) -> Result<()> {
    let name = group.as_str();
    let source = child
        .groups
        .get("source")
        .ok_or_else(|| invalid("target history has no source metadata"))?;
    let unbased_absent = matches!(
        &child.request,
        OperationRequest::Build(_) | OperationRequest::Refresh(_)
    ) && child.request.base().is_none()
        && child.parent_manifest.is_none()
        && !parent.computed.contains_key(name)
        && !child.computed.contains_key(name);
    if !unbased_absent && child.parent_manifest.as_ref() != Some(&parent_ref.manifest) {
        return Err(invalid(
            "target selection composition parent differs from accepted predecessor",
        ));
    }
    match &child.request {
        OperationRequest::Backfill(spec) if spec.group == *group => {
            Err(conflict("another backfill selected this target group"))
        }
        OperationRequest::Backfill(_) | OperationRequest::Evidence(_) => {
            // Correction qualification may advance while original execution bindings stay exact.
            let before = parent.computed.get(name);
            let after = child.computed.get(name);
            let same = match (before, after) {
                (None, None) => true,
                (Some(before), Some(after)) => {
                    let mut before = before.clone();
                    before.qualification = after.qualification.clone();
                    before == *after
                }
                _ => false,
            };
            if !same
                || parent.groups.get(name) != child.groups.get(name)
                || parent.groups.get("source") != Some(source)
            {
                return Err(invalid(
                    "composition selection changed another target or source",
                ));
            }
            Ok(())
        }
        OperationRequest::Build(_)
        | OperationRequest::Update(_)
        | OperationRequest::Refresh(_)
        | OperationRequest::Schema(_) => {
            if !unbased_absent && child.request.base() != Some(&parent_ref.version) {
                return Err(invalid(
                    "coordinated target update did not capture the accepted predecessor",
                ));
            }
            if let Some(binding) = child.computed.get(name)
                && (binding.request != child.request
                    || binding.request.operation() != &child_ref.version
                    || binding.source_metadata != source.metadata
                    || binding.base_manifest != parent_ref.manifest)
            {
                return Err(invalid(
                    "coordinated target binding differs from its source request",
                ));
            }
            Ok(())
        }
    }
}
