//! Independent membership and reconstruction checks for applied native changes.

use super::write::identifier as quote;
use super::*;

pub(super) async fn verify_changes(
    request: &OperationRequest,
    source: &TablePin,
    changes: Option<&ChangePin>,
    budget: &Budget,
) -> Result<()> {
    let invalid = || {
        Error::refusal(
            "invalid_changes",
            "change rows, schemas or endpoints differ from the captured request and result",
        )
    };
    let (dataset, base, keys, changes) = match (request, changes) {
        (OperationRequest::Build(_) | OperationRequest::Backfill(_), None) => return Ok(()),
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
    let before = refresh::read_release(
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
    // Shallow verification of the exact prior source avoids recursively scanning
    // every historical change chain while establishing the delta's before state.
    verify_table(before_source, budget).await?;
    let native_before = storage::open_pin(before_source).await?;
    let native_after = storage::open_pin(source).await?;
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
        || native_before.metadata().current_schema() != native_after.metadata().current_schema()
    {
        return Err(invalid());
    }
    let context = budget.context();
    // Positional aliases keep arbitrary user field names separate from helpers.
    let fields = iceberg::arrow::schema_to_arrow_schema(native_before.metadata().current_schema())?;
    let selected = fields
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| format!("{} AS c{index}", quote(field.name())))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = (0..fields.fields().len())
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
            verify_table(pin, budget).await?;
        }
        let native = storage::open_pin(pin).await?;
        if native.metadata().current_schema() != native_after.metadata().current_schema() {
            return Err(invalid());
        }
        context.register_table(
            format!("raw_{name}"),
            crate::query::table(native, budget.clone())?,
        )?;
        context.register_table(
            name,
            context
                .sql(&format!("SELECT {selected} FROM raw_{name}"))
                .await?
                .into_view(),
        )?;
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
