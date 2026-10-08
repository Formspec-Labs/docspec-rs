//! Exact fixed-state summaries and their complete contributor relations.

use super::{incremental::source, refresh::LoadedInputs, write::Replacement, *};
use arrow::datatypes::DataType;
use write::identifier as quote;

pub(super) const GLOBAL: &str = "__docspec_global";
pub(super) const CONTRIBUTOR: [&str; 2] = ["__docspec_input_file", "__docspec_input_row"];

pub(super) fn validate(definition: &AggregationDefinition) -> Result<()> {
    let valid = |name: &str| {
        !name.trim().is_empty() && !name.to_ascii_lowercase().starts_with("__docspec_")
    };
    let names = definition
        .keys
        .iter()
        .chain(definition.aggregates.iter().map(|field| &field.name));
    let mut distinct = std::collections::BTreeSet::new();
    if definition.aggregates.is_empty()
        || names
            .into_iter()
            .any(|name| !valid(name) || !distinct.insert(name.to_ascii_lowercase()))
        || definition
            .aggregates
            .iter()
            .any(|field| argument(&field.function).is_some_and(|name| !valid(name)))
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "summary needs distinct named keys and aggregates outside reserved fields",
        ));
    }
    Ok(())
}
fn argument(function: &AggregateFunction) -> Option<&str> {
    match function {
        AggregateFunction::CountRows => None,
        AggregateFunction::Count { field }
        | AggregateFunction::Sum { field }
        | AggregateFunction::Min { field }
        | AggregateFunction::Max { field } => Some(field),
    }
}
pub(super) fn keys(definition: &AggregationDefinition) -> Vec<String> {
    if definition.keys.is_empty() {
        vec![GLOBAL.into()]
    } else {
        definition.keys.clone()
    }
}
fn columns(keys: &[String]) -> String {
    keys.iter()
        .map(|key| quote(key))
        .collect::<Vec<_>>()
        .join(", ")
}
fn scope_predicate(keys: &[String]) -> String {
    keys.iter()
        .enumerate()
        .map(|(i, key)| format!("(s.{} IS NOT DISTINCT FROM k.\"k{i}\")", quote(key)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub(super) async fn register_input(
    context: &SessionContext,
    spec: &AggregateTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
) -> Result<()> {
    let release = releases.get(&spec.input.dataset).ok_or_else(invalid)?;
    if release.version_ref() != spec.input {
        return Err(invalid());
    }
    let source = source(release)?;
    check_row_count(source.rows)?;
    context.register_table(
        "aggregate_input",
        crate::query::occurrences(storage::open_pin(source).await?, budget.clone())?,
    )?;
    qualify(context, &spec.grouping, "aggregate_input").await
}
async fn qualify(
    context: &SessionContext,
    definition: &AggregationDefinition,
    input: &str,
) -> Result<()> {
    validate(definition)?;
    let frame = context.table(input).await?;
    let schema = frame.schema().as_arrow();
    for key in &definition.keys {
        if !matches!(
            schema.field_with_name(key)?.data_type(),
            DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::Utf8
        ) {
            return Err(Error::refusal(
                "aggregate_key_type",
                "summary keys currently support Boolean, Int32, Int64 and Utf8",
            ));
        }
    }
    for aggregate in &definition.aggregates {
        if let Some(field) = argument(&aggregate.function) {
            let data_type = schema.field_with_name(field)?.data_type();
            if !matches!(aggregate.function, AggregateFunction::Count { .. })
                && !matches!(data_type, DataType::Int32 | DataType::Int64)
            {
                return Err(Error::refusal(
                    "aggregate_value_type",
                    "sum, min and max currently require signed Int32 or Int64 operands",
                ));
            }
        }
    }
    Ok(())
}
pub(super) async fn aggregate_frame(
    context: &SessionContext,
    definition: &AggregationDefinition,
    input: &str,
) -> Result<DataFrame> {
    qualify(context, definition, input).await?;
    let mut selected = definition
        .keys
        .iter()
        .map(|key| quote(key))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        selected.push(format!("CAST(0 AS BIGINT) AS {}", quote(GLOBAL)));
    }
    selected.extend(definition.aggregates.iter().map(|field| {
        let expression = match &field.function {
            AggregateFunction::CountRows => "COUNT(*)".into(),
            AggregateFunction::Count { field } => format!("COUNT({})", quote(field)),
            AggregateFunction::Sum { field } => {
                format!("SUM(CAST({} AS DECIMAL(38, 0)))", quote(field))
            }
            AggregateFunction::Min { field } => format!("MIN({})", quote(field)),
            AggregateFunction::Max { field } => format!("MAX({})", quote(field)),
        };
        format!("{expression} AS {}", quote(&field.name))
    }));
    let grouping = if definition.keys.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", columns(&definition.keys))
    };
    Ok(context
        .sql(&format!(
            "SELECT {} FROM {input}{grouping}",
            selected.join(", ")
        ))
        .await?)
}
pub(super) async fn evidence_frame(
    context: &SessionContext,
    definition: &AggregationDefinition,
    input: &str,
) -> Result<DataFrame> {
    let mut selected = definition
        .keys
        .iter()
        .map(|key| quote(key))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        selected.push(format!("CAST(0 AS BIGINT) AS {}", quote(GLOBAL)));
    }
    for (field, name) in [storage::OCCURRENCE_FILE, storage::OCCURRENCE_ROW]
        .iter()
        .zip(CONTRIBUTOR)
    {
        selected.push(format!("{} AS {}", quote(field), quote(name)));
    }
    Ok(context
        .sql(&format!("SELECT {} FROM {input}", selected.join(", ")))
        .await?)
}

impl Engine {
    pub(super) async fn process_aggregation(
        &self,
        spec: &RefreshSpec,
        directory: &Path,
        base: &ReleasePin,
        previous: &LoadedInputs,
        current: &LoadedInputs,
    ) -> Result<write::Prepared> {
        let RefreshTransform::Aggregate(aggregate) = &spec.transform else {
            return Err(invalid());
        };
        let budget = &self.inner.lifecycle.budget;
        let before = source(base)?;
        let context = self.context();
        register_input(&context, aggregate, &current.borrowed(), budget).await?;
        let old = source(&previous.releases[&aggregate.input.dataset])?;
        let new = source(&current.releases[&aggregate.input.dataset])?;
        let (affected, scanned) = self
            .changed_keys(
                &spec.dataset,
                &directory.join("affected"),
                old,
                new,
                &aggregate.grouping.keys,
            )
            .await?;
        let native = storage::open_pin(&affected).await?;
        let (discovery_files, discovery_bytes) = {
            let inventory = storage::snapshot_files(&native, budget).await?;
            (
                inventory.data.len() as u64,
                inventory
                    .data
                    .iter()
                    .map(|file| file.file_size_in_bytes())
                    .sum::<u64>(),
            )
        };
        context.register_table("affected", crate::query::table(native, budget.clone())?)?;
        let key_names = keys(&aggregate.grouping);
        let selected = key_names
            .iter()
            .enumerate()
            .map(|(i, key)| format!("\"k{i}\" AS {}", quote(key)))
            .collect::<Vec<_>>()
            .join(", ");
        let scope = context
            .sql(&format!("SELECT DISTINCT {selected} FROM affected"))
            .await?;
        let replacement = if aggregate.grouping.keys.is_empty() {
            let frame = aggregate_frame(&context, &aggregate.grouping, "aggregate_input").await?;
            // Unchanged empty input must not fabricate an empty-input aggregate replacement.
            if affected.rows == 0 {
                frame.limit(0, Some(0))?
            } else {
                frame
            }
        } else {
            let scoped = context
                .sql(&format!(
                    "SELECT s.* FROM aggregate_input s LEFT SEMI JOIN affected k ON {}",
                    scope_predicate(&aggregate.grouping.keys)
                ))
                .await?;
            context.register_table("scoped", scoped.into_view())?;
            aggregate_frame(&context, &aggregate.grouping, "scoped").await?
        };
        let native = storage::open_pin(before).await?;
        let key_field_ids = key_names
            .iter()
            .map(|key| {
                native
                    .metadata()
                    .current_schema()
                    .field_by_name(key)
                    .map(|field| field.id)
                    .ok_or_else(invalid)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut applied = Box::pin(self.replace_keys(Replacement {
            dataset: &spec.dataset,
            directory: &directory.join("replacement"),
            before,
            keys: &key_names,
            scope,
            replacement,
            replacement_bytes: None,
        }))
        .await?;
        applied.metrics.files_scanned += scanned;
        applied.metrics.data_files_written += discovery_files;
        applied.metrics.bytes_written += discovery_bytes;
        Ok(write::Prepared {
            changes: Some(ChangePin {
                base_version: base.id.clone(),
                base_manifest: base.manifest.clone(),
                before_metadata: before.metadata.clone(),
                after_metadata: applied.table.metadata.clone(),
                key_field_ids,
                removed: applied.removed,
                added: applied.added,
            }),
            table: applied.table,
            metrics: applied.metrics,
        })
    }
    pub(super) async fn aggregate_evidence(
        &self,
        spec: &RefreshSpec,
        directory: &Path,
        processing: &PhaseResult,
        before: &TablePin,
        context: &SessionContext,
    ) -> Result<write::Prepared> {
        let RefreshTransform::Aggregate(aggregate) = &spec.transform else {
            return Err(invalid());
        };
        let key_names = keys(&aggregate.grouping);
        let selected = columns(&key_names);
        // Deliberately replaced groups remain in the applied delta even when values
        // are equal. Union both sides includes newly created and deleted groups.
        let scope = context
            .sql(&format!(
                "SELECT {selected} FROM removed UNION SELECT {selected} FROM added"
            ))
            .await?;
        context.register_table("affected_groups", scope.clone().into_view())?;
        let loaded = refresh::load_inputs(
            processing.coverage.as_ref().ok_or_else(invalid)?,
            &self.inner.lifecycle.budget,
        )
        .await?;
        register_input(
            context,
            aggregate,
            &loaded.borrowed(),
            &self.inner.lifecycle.budget,
        )
        .await?;
        let evidence = evidence_frame(context, &aggregate.grouping, "aggregate_input").await?;
        context.register_table("contributors", evidence.into_view())?;
        let predicate = key_names
            .iter()
            .map(|key| format!("(s.{} IS NOT DISTINCT FROM k.{})", quote(key), quote(key)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let replacement = context
            .sql(&format!(
                "SELECT s.* FROM contributors s LEFT SEMI JOIN affected_groups k ON {predicate}"
            ))
            .await?;
        let applied = Box::pin(self.replace_keys(Replacement {
            dataset: &spec.dataset,
            directory,
            before,
            keys: &key_names,
            scope,
            replacement,
            replacement_bytes: None,
        }))
        .await?;
        Ok(write::Prepared {
            table: applied.table,
            changes: None,
            metrics: applied.metrics,
        })
    }
}
fn invalid() -> Error {
    Error::refusal(
        "invalid_related_result",
        "summary inputs, grouping or contributor evidence disagree",
    )
}

fn check_row_count(rows: u64) -> Result<()> {
    // Native SUM wraps, including Decimal128. Signed operands cast BEFORE SUM
    // and at most i64::MAX occurrences bound every partial sum below 10^38.
    if rows > i64::MAX as u64 {
        return Err(Error::refusal(
            "aggregate_range",
            "exact counts and integer sums require at most i64::MAX verified input rows",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn qualified_row_limit_bounds_every_signed_partial_sum() {
        let rows = i64::MAX as u64;
        check_row_count(rows).unwrap();
        assert_eq!(
            check_row_count(rows + 1).unwrap_err().code(),
            "aggregate_range"
        );
        // Worst possible signed operand magnitude, even for an all-negative partition.
        let worst = i128::from(i64::MIN)
            .abs()
            .checked_mul(i128::from(rows))
            .unwrap();
        assert!(worst < 10_i128.pow(38));
    }
}
