//! Exact fixed-state summaries and their complete contributor relations.
// Rust guideline compliant 2026-02-21

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
        | AggregateFunction::CountDistinct { field }
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
        crate::query::occurrences(
            storage::open_pin(source, budget).await?,
            budget.clone(),
            Some(source.rows),
        )?,
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
            DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Utf8
                | DataType::Date32
        ) {
            return Err(Error::refusal(
                "aggregate_key_type",
                "summary keys currently support Boolean, Int32, Int64, Utf8 and Date32",
            ));
        }
    }
    for aggregate in &definition.aggregates {
        if let Some(field) = argument(&aggregate.function) {
            let data_type = schema.field_with_name(field)?.data_type();
            if !matches!(
                aggregate.function,
                AggregateFunction::Count { .. } | AggregateFunction::CountDistinct { .. }
            ) && !matches!(data_type, DataType::Int32 | DataType::Int64)
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
            AggregateFunction::CountDistinct { field } => {
                format!("COUNT(DISTINCT {})", quote(field))
            }
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
        captured: (&TablePin, &iceberg::table::Table),
    ) -> Result<write::Prepared> {
        let (captured_source, captured_native) = captured;
        let RefreshTransform::Aggregate(aggregate) = &spec.transform else {
            return Err(invalid());
        };
        let budget = &self.inner.lifecycle.budget;
        let before = source(base)?;
        let context = self.context();
        register_input(&context, aggregate, &current.borrowed(), budget).await?;
        let (affected, scanned) = self
            .changed_keys(
                &spec.dataset,
                &directory.join("affected"),
                (
                    &previous.releases[&aggregate.input.dataset],
                    &current.releases[&aggregate.input.dataset],
                ),
                (
                    previous.canonical_source(&aggregate.input.dataset)?,
                    current.canonical_source(&aggregate.input.dataset)?,
                ),
                &aggregate.grouping.keys,
            )
            .await?;
        let native = storage::open_pin(&affected, &self.inner.lifecycle.budget).await?;
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
        context.register_table(
            "affected",
            crate::query::table(native, budget.clone(), Some(affected.rows))?,
        )?;
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
        let key_field_ids = key_names
            .iter()
            .map(|key| {
                captured_native
                    .metadata()
                    .current_schema()
                    .field_by_name(key)
                    .map(|field| field.id)
                    .ok_or_else(invalid)
            })
            .collect::<Result<Vec<_>>>()?;
        let (write_before, replacement) = self
            .source_replacement(&spec.dataset, captured, directory, replacement, &key_names)
            .await?;
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: locations::Location::Local(directory.join("replacement")),
                before: &write_before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        let mut applied = self
            .finish_source_replacement(&spec.dataset, directory, captured_source, applied)
            .await?;
        applied.metrics.files_scanned += scanned;
        applied.metrics.data_files_written += discovery_files;
        applied.metrics.bytes_written += discovery_bytes;
        Ok(write::Prepared {
            schema_input: None,
            changes: Some(ChangePin {
                rules: super::changes::WRITE_RULES.into(),
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
            processing
                .dependencies
                .as_ref()
                .map(|dependencies| &dependencies.input_sources)
                .unwrap_or(&BTreeMap::new()),
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
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: locations::Location::Local(directory.to_owned()),
                before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        Ok(write::Prepared {
            table: applied.table,
            changes: None,
            metrics: applied.metrics,
            schema_input: None,
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
    use arrow::{
        array::{Array, Date32Array, Int64Array, StringArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

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

    #[tokio::test]
    async fn date32_keys_and_count_distinct_refresh_full_and_incrementally() {
        let root = tempfile::tempdir().unwrap();
        let write_input = |name: &str, rows: &[(i64, Option<i32>, Option<&str>)]| {
            let path = root.path().join(name);
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("day", DataType::Date32, true),
                Field::new("author", DataType::Utf8, true),
                Field::new("resolution", DataType::Utf8, true),
                Field::new("headline", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(Date32Array::from_iter(rows.iter().map(|r| r.1))),
                    Arc::new(StringArray::from_iter(rows.iter().map(|r| r.2))),
                    Arc::new(StringArray::from_iter(
                        rows.iter().map(|_| Some("exact_match")),
                    )),
                    Arc::new(StringArray::from_iter(
                        rows.iter().map(|_| Some("unneeded payload body")),
                    )),
                ],
            )
            .unwrap();
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            InputSpec {
                path: path.to_string_lossy().into_owned(),
                digest: storage::digest(&path).unwrap().0,
            }
        };
        let id = |name| Id::new(name).unwrap();
        let limits = Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        };
        let engine = Engine::open(root.path().join("workspace"), limits.clone())
            .await
            .unwrap();
        let initial_input = write_input(
            "initial.parquet",
            &[
                (1, Some(19_000), Some("alice")),
                (2, Some(19_000), Some("alice")),
                (3, Some(19_000), Some("bob")),
                (4, Some(19_001), None),
                (5, None, Some("carol")),
            ],
        );
        let source = engine
            .run(&RunSpec {
                dataset: id("events"),
                operation: id("initial"),
                base: None,
                relationship: None,
                input: initial_input.path,
                expected_digest: initial_input.digest,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
            })
            .await
            .unwrap();
        let mut spec = RefreshSpec {
            dataset: id("daily-authors"),
            operation: id("initial"),
            base: None,
            definition: "v1".into(),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: source.version_ref(),
                grouping: AggregationDefinition {
                    keys: vec!["day".into()],
                    aggregates: vec![
                        AggregateField {
                            name: "rows".into(),
                            function: AggregateFunction::CountRows,
                        },
                        AggregateField {
                            name: "distinct_authors".into(),
                            function: AggregateFunction::CountDistinct {
                                field: "author".into(),
                            },
                        },
                    ],
                },
            }),
        };
        let first = engine.refresh(&spec).await.unwrap();
        verify_release(&first, limits.clone()).await.unwrap();

        let RefreshTransform::Aggregate(aggregate_spec) = &spec.transform else {
            unreachable!()
        };
        let budget = &engine.inner.lifecycle.budget;
        let context = engine.context();
        let releases = BTreeMap::from([(id("events"), &source)]);
        register_input(&context, aggregate_spec, &releases, budget)
            .await
            .unwrap();
        let full_frame = aggregate_frame(&context, &aggregate_spec.grouping, "aggregate_input")
            .await
            .unwrap();
        let full_plan = full_frame.create_physical_plan().await.unwrap();
        let full_rendered = datafusion::physical_plan::displayable(full_plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            full_rendered.contains("AggregateExec: mode=Partial")
                && full_rendered.contains("AggregateExec: mode=Final"),
            "expected hash AggregateExec in plan:\n{full_rendered}"
        );
        assert!(
            !full_rendered.contains("SortExec"),
            "unexpected SortExec in hash aggregate plan:\n{full_rendered}"
        );
        assert!(
            full_rendered.contains("NativeScan: rows=Some(5), projection=[day, author]"),
            "expected projected NativeScan of only [day, author] in plan:\n{full_rendered}"
        );
        for omitted in [
            "id",
            "resolution",
            "headline",
            storage::OCCURRENCE_FILE,
            storage::OCCURRENCE_ROW,
        ] {
            assert!(
                !full_rendered.contains(omitted),
                "unneeded column {omitted} leaked into aggregate physical plan:\n{full_rendered}"
            );
        }

        let updated = engine
            .update(&UpdateSpec {
                dataset: id("events"),
                operation: id("updated"),
                base: source.id.clone(),
                definition: "v1".into(),
                keys: vec!["id".into()],
                replacements: write_input(
                    "replacements.parquet",
                    &[
                        (2, Some(19_000), Some("dave")),
                        (4, Some(19_001), Some("erin")),
                    ],
                ),
                deletions: None,
            })
            .await
            .unwrap();
        spec.operation = id("incremental");
        spec.base = Some(first.id.clone());
        spec.mode = RefreshMode::Incremental;
        let RefreshTransform::Aggregate(aggregate) = &mut spec.transform else {
            unreachable!()
        };
        aggregate.input = updated.version_ref();
        let second = engine.refresh(&spec).await.unwrap();
        verify_release(&second, limits).await.unwrap();

        let stream = engine.read_group(&second, "source").await.unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        let mut actual = BTreeMap::new();
        for batch in batches {
            let days = batch
                .column_by_name("day")
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            let rows = batch
                .column_by_name("rows")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let authors = batch
                .column_by_name("distinct_authors")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let day = (!days.is_null(row)).then(|| days.value(row));
                actual.insert(day, (rows.value(row), authors.value(row)));
            }
        }
        assert_eq!(
            actual,
            BTreeMap::from([
                (None, (1, 1)),
                (Some(19_000), (3, 3)),
                (Some(19_001), (1, 1))
            ])
        );
    }
}
