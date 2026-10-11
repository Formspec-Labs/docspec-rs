//! Independent membership and reconstruction checks for applied native changes.

use super::write::identifier as quote;
use super::*;

mod certificates;
mod chain;
pub(super) use chain::{Chain, Forward, MAX_LINKS};
mod retirement;
pub(super) use certificates::{
    PreparedChanges, WRITE_RULES, change_selection, check_applied, check_certificate,
    check_prepared, local_certificate,
};

/// Independently reconstructs an optional applied transition from exact endpoints.
///
/// This explicit audit scans the applied rows and endpoint values. It does not
/// grant local certificate authority or extend any objects' retention lifetime.
/// The caller keeps the referenced objects available throughout the check.
///
/// # Errors
/// Refuses incorrect bindings, changed objects, invalid schemas or applied values,
/// and exhausted execution capacity.
pub async fn verify_applied_changes(
    before: &ReleasePin,
    after: &ReleasePin,
    changes: &AppliedChangeSet,
    limits: Limits,
) -> Result<()> {
    certificates::check_bindings(changes)?;
    let invalid = || {
        Error::refusal(
            "invalid_changes",
            "applied transition differs from its exact endpoints",
        )
    };
    if changes.before != before.version_ref()
        || changes.operation != after.id
        || changes.before.dataset != after.dataset.dataset
        || after.dataset.parent_manifest.as_ref() != Some(&before.manifest)
        || after.dataset.request.base() != Some(&before.id)
        || changes.format != 1
        || changes.rules != WRITE_RULES
        || (changes.source.is_none() && changes.receipts.is_none())
    {
        return Err(invalid());
    }
    let budget = Budget::new(limits)?;
    resolve_release(before, &budget).await?;
    resolve_release(after, &budget).await?;
    if let OperationRequest::Refresh(spec) = &after.dataset.request
        && spec.mode == RefreshMode::Incremental
    {
        incremental::qualify(spec, &before.dataset, after.dataset.eligibility, &budget).await?;
    }
    let mut checks = verification::Checks::new(VerificationLevel::Deep);
    let source = after.dataset.groups.get("source").ok_or_else(invalid)?;
    checks.table(source, &budget).await?;
    verify_changes(
        &after.dataset.request,
        source,
        changes.source.as_ref(),
        &budget,
        &mut checks,
    )
    .await?;
    match (&after.dataset.evidence, &changes.receipts) {
        (Some(result), receipt_changes) => {
            evidence::verify_prepared(
                &after.dataset.request,
                source,
                before,
                &evidence::PreparedReceipts {
                    result: (**result).clone(),
                    changes: receipt_changes.clone(),
                },
                &budget,
                &mut checks,
            )
            .await?;
        }
        (None, Some(_)) => return Err(invalid()),
        (None, None) => {}
    }
    Ok(())
}

pub(super) async fn verify_changes(
    request: &OperationRequest,
    source: &TablePin,
    changes: Option<&ChangePin>,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    let invalid = || {
        Error::refusal(
            "invalid_changes",
            "change rows, schemas or endpoints differ from the captured request and result",
        )
    };
    let (dataset, base, keys, changes) = match (request, changes) {
        (
            OperationRequest::Build(_)
            | OperationRequest::Backfill(_)
            | OperationRequest::Schema(_)
            | OperationRequest::Evidence(_),
            None,
        ) => return Ok(()),
        (OperationRequest::Refresh(spec), None) if spec.mode == RefreshMode::Full => return Ok(()),
        (OperationRequest::Update(spec), Some(changes)) => {
            (&spec.dataset, &spec.base, spec.keys.clone(), changes)
        }
        (OperationRequest::Refresh(spec), Some(changes))
            if spec.mode == RefreshMode::Incremental =>
        {
            (
                &spec.dataset,
                spec.base.as_ref().ok_or_else(invalid)?,
                refresh::output_keys(&spec.transform),
                changes,
            )
        }
        _ => return Err(invalid()),
    };
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut bytes = 0;
    let before = refresh::read_release_with_objects(
        checks.objects(),
        &VersionRef {
            dataset: dataset.clone(),
            version: base.clone(),
            manifest: changes.base_manifest.clone(),
        },
        budget,
        &mut bytes,
        &memory,
    )
    .await
    .map_err(|error| {
        if error.code() == "invalid_related_result" {
            invalid()
        } else {
            error
        }
    })?;
    let before_source = before.dataset.groups.get("source").ok_or_else(invalid)?;
    if changes.base_version != *base
        || changes.before_metadata != before_source.metadata
        || changes.after_metadata != source.metadata
        || before_source
            .rows
            .checked_sub(changes.removed.rows)
            .and_then(|rows| rows.checked_add(changes.added.rows))
            != Some(source.rows)
    {
        return Err(invalid());
    }
    verify_native_changes(before_source, source, &keys, changes, budget, checks).await
}

/// Shared applied-write proof for source values and occurrence-associated receipts.
pub(super) async fn verify_native_changes(
    before_source: &TablePin,
    source: &TablePin,
    keys: &[String],
    changes: &ChangePin,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    let invalid = || {
        Error::refusal(
            "invalid_changes",
            "applied rows, native schemas or endpoint counts disagree",
        )
    };
    if before_source
        .rows
        .checked_sub(changes.removed.rows)
        .and_then(|rows| rows.checked_add(changes.added.rows))
        != Some(source.rows)
    {
        return Err(invalid());
    }
    // Shallow verification of the exact prior source avoids recursively scanning
    // every historical change chain while establishing the delta's before state.
    if checks.deep() {
        checks.table(before_source, budget).await?;
    }
    let native_before = storage::open_pin(before_source, budget).await?;
    let native_after = storage::open_pin(source, budget).await?;
    let expected_ids: Option<Vec<_>> = keys
        .iter()
        .map(|name| {
            native_before
                .metadata()
                .current_schema()
                .field_by_name(name)
                .map(|field| field.id)
        })
        .collect();
    if expected_ids.as_ref() != Some(&changes.key_field_ids)
        || !storage::identity::preserves_schema(
            native_before.metadata().current_schema(),
            native_after.metadata().current_schema(),
        )?
    {
        return Err(invalid());
    }
    let context = budget.context();
    // Positional aliases keep arbitrary user field names separate from helpers.
    let fields = storage::identity::public_schema(&native_before)?;
    let located = storage::identity::has_origins(native_after.metadata().current_schema())?;
    let mut names = fields
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    // A rewritten source preserves occurrence membership, not only equal values.
    // Legacy sources without stored identities retain the value-only check.
    if located {
        names.extend([storage::OCCURRENCE_FILE, storage::OCCURRENCE_ROW]);
    }
    let selected = names
        .iter()
        .enumerate()
        .map(|(index, name)| format!("{} AS c{index}", quote(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = (0..names.len())
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    for (name, pin) in [
        ("before", before_source),
        ("after", source),
        ("removed", &changes.removed),
        ("added", &changes.added),
    ] {
        if name == "removed" || name == "added" {
            checks.table(pin, budget).await?;
        }
        let native = storage::open_pin(pin, budget).await?;
        if name != "before"
            && native.metadata().current_schema() != native_after.metadata().current_schema()
        {
            return Err(invalid());
        }
        if !checks.deep() {
            continue;
        }
        context.register_table(
            format!("raw_{name}"),
            if located {
                crate::query::occurrences(native, budget.clone(), Some(pin.rows))?
            } else {
                crate::query::table(native, budget.clone(), Some(pin.rows))?
            },
        )?;
        context.register_table(
            name,
            context
                .sql(&format!("SELECT {selected} FROM raw_{name}"))
                .await?
                .into_view(),
        )?;
    }
    if !checks.deep() {
        return Ok(());
    }
    // One aggregate checks both invariants without decoding before/removed twice.
    let union = [("before", 1, 1), ("removed", -1, -1), ("added", 0, 1), ("after", 0, -1)]
        .iter()
        .map(|(name, remaining, delta)| format!("SELECT {columns}, CAST({remaining} AS BIGINT) AS remaining, CAST({delta} AS BIGINT) AS delta FROM {name}"))
        .collect::<Vec<_>>().join(" UNION ALL ");
    let sql = format!(
        "SELECT 1 FROM ({union}) GROUP BY {columns} HAVING SUM(remaining) < 0 OR SUM(delta) <> 0 LIMIT 1"
    );
    let mut stream = context.sql(&sql).await?.execute_stream().await?;
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() != 0 {
            return Err(invalid());
        }
    }
    Ok(())
}
