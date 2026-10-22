//! Exact fixed-state summaries and their complete contributor relations.
// Rust guideline compliant 2026-02-21

mod udaf;

use super::{incremental::source, refresh::LoadedInputs, write::Replacement, *};
use arrow::datatypes::DataType;
use std::collections::BTreeSet;
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
    let mut distinct = BTreeSet::new();
    if definition.aggregates.is_empty()
        || names
            .into_iter()
            .any(|name| !valid(name) || !distinct.insert(name.to_ascii_lowercase()))
        || definition
            .aggregates
            .iter()
            .any(|field| match &field.function {
                AggregateFunction::CollectSet { field } => {
                    !valid(field)
                        || definition
                            .keys
                            .iter()
                            .any(|key| key.eq_ignore_ascii_case(field))
                }
                AggregateFunction::FirstValueBy { field, order_by } => {
                    !valid(field)
                        || order_by.is_empty()
                        || order_by.iter().any(|ob| !valid(&ob.field))
                }
                AggregateFunction::CollectDistinctWithProvenance {
                    field,
                    provenance_fields,
                    order_by,
                } => {
                    let mut prov_distinct = BTreeSet::new();
                    !valid(field)
                        || provenance_fields.is_empty()
                        || provenance_fields.iter().any(|pf| {
                            !valid(pf)
                                || pf.eq_ignore_ascii_case("value")
                                || !prov_distinct.insert(pf.to_ascii_lowercase())
                        })
                        || order_by.iter().any(|ob| !valid(&ob.field))
                }
                function => argument(function).is_some_and(|name| !valid(name)),
            })
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "summary needs distinct named keys and aggregates outside reserved fields",
        ));
    }
    expr::validate_projections(&definition.projections)?;
    if let Some(filter) = &definition.filter {
        expr::validate_predicate(filter)?;
    }
    Ok(())
}

pub(super) fn validate_transform(spec: &AggregateTransform) -> Result<()> {
    validate(&spec.grouping)?;
    expr::validate_projections(&spec.effective_projections())?;
    if let Some(filter) = &spec.effective_filter() {
        expr::validate_predicate(filter)?;
    }
    Ok(())
}

fn argument(function: &AggregateFunction) -> Option<&str> {
    match function {
        AggregateFunction::CountRows => None,
        AggregateFunction::Count { field }
        | AggregateFunction::CountDistinct { field }
        | AggregateFunction::CollectSet { field }
        | AggregateFunction::FirstValueBy { field, .. }
        | AggregateFunction::CollectDistinctWithProvenance { field, .. }
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
    qualify_transform(context, spec, "aggregate_input").await?;
    Ok(())
}

fn is_supported_distinct_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Decimal128(_, 0)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
    ) || matches!(
        data_type,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
    )
}

fn is_supported_order_scalar(data_type: &DataType) -> bool {
    !data_type.is_nested() && !matches!(data_type, DataType::Null | DataType::Dictionary(_, _))
}

async fn qualify_transform(
    context: &SessionContext,
    spec: &AggregateTransform,
    input: &str,
) -> Result<expr::CompiledAggregationSql> {
    validate_transform(spec)?;
    let frame = context.table(input).await?;
    let raw_schema = frame.schema().as_arrow();
    let compiled = expr::compile_aggregate_sql(spec, raw_schema)?;
    let pre_schema = if compiled.pre_projections.is_empty() {
        raw_schema.clone()
    } else {
        let mut items = vec!["*".to_owned()];
        for (name, sql) in &compiled.pre_projections {
            items.push(format!("{sql} AS {}", quote(name)));
        }
        let pre_df = context
            .sql(&format!(
                "SELECT {} FROM {input} LIMIT 0",
                items.join(", ")
            ))
            .await?;
        pre_df.schema().as_arrow().clone()
    };

    let definition = &spec.grouping;
    for key in &definition.keys {
        if !matches!(
            pre_schema.field_with_name(key)?.data_type(),
            DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt64
                | DataType::Decimal128(_, 0)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
        ) {
            return Err(Error::refusal(
                "aggregate_key_type",
                "summary keys currently support Boolean, Int32, Int64, UInt64/Decimal128, Utf8, Binary, Date32 and Timestamp(Microsecond)",
            ));
        }
    }
    for aggregate in &definition.aggregates {
        match &aggregate.function {
            AggregateFunction::CountRows => {}
            AggregateFunction::Count { field } | AggregateFunction::CountDistinct { field } => {
                pre_schema.field_with_name(field)?;
            }
            AggregateFunction::CollectSet { field } => {
                let dt = pre_schema.field_with_name(field)?.data_type();
                let valid = matches!(
                    dt,
                    DataType::Boolean
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt64
                        | DataType::Decimal128(_, 0)
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::FixedSizeBinary(_)
                        | DataType::Date32
                ) || matches!(
                    dt,
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(zone))
                        if zone.as_ref() == "UTC" || zone.as_ref() == "+00:00"
                );
                if !valid {
                    return Err(Error::refusal(
                        "aggregate_value_type",
                        "sum, min, max and collect_set require qualified scalar operands",
                    ));
                }
            }
            AggregateFunction::FirstValueBy { field, order_by } => {
                let dt = pre_schema.field_with_name(field)?.data_type();
                if matches!(dt, DataType::Null | DataType::Dictionary(_, _)) {
                    return Err(Error::refusal(
                        "aggregate_value_type",
                        "first_value_by field must have a concrete non-null data type",
                    ));
                }
                for ob in order_by {
                    let ob_dt = pre_schema.field_with_name(&ob.field)?.data_type();
                    if !is_supported_order_scalar(ob_dt) {
                        return Err(Error::refusal(
                            "aggregate_value_type",
                            format!(
                                "first_value_by order_by column '{}' must have an orderable scalar type",
                                ob.field
                            ),
                        ));
                    }
                }
            }
            AggregateFunction::CollectDistinctWithProvenance {
                field,
                provenance_fields,
                order_by,
            } => {
                let dt = pre_schema.field_with_name(field)?.data_type();
                if !is_supported_distinct_scalar(dt) {
                    return Err(Error::refusal(
                        "aggregate_value_type",
                        "collect_distinct_with_provenance field requires a qualified scalar type",
                    ));
                }
                for pf in provenance_fields {
                    let pf_dt = pre_schema.field_with_name(pf)?.data_type();
                    if matches!(pf_dt, DataType::Null | DataType::Dictionary(_, _)) {
                        return Err(Error::refusal(
                            "aggregate_value_type",
                            format!("invalid provenance field type for '{pf}'"),
                        ));
                    }
                }
                for ob in order_by {
                    let ob_dt = pre_schema.field_with_name(&ob.field)?.data_type();
                    if !is_supported_order_scalar(ob_dt) {
                        return Err(Error::refusal(
                            "aggregate_value_type",
                            format!(
                                "collect_distinct_with_provenance order_by column '{}' must have an orderable scalar type",
                                ob.field
                            ),
                        ));
                    }
                }
            }
            AggregateFunction::Sum { field } => {
                let dt = pre_schema.field_with_name(field)?.data_type();
                if !matches!(
                    dt,
                    DataType::Int32
                        | DataType::Int64
                        | DataType::UInt64
                        | DataType::Decimal128(_, 0)
                ) {
                    return Err(Error::refusal(
                        "aggregate_value_type",
                        "sum, min, max and collect_set require qualified scalar operands",
                    ));
                }
            }
            AggregateFunction::Min { field } | AggregateFunction::Max { field } => {
                let dt = pre_schema.field_with_name(field)?.data_type();
                if !matches!(
                    dt,
                    DataType::Int32
                        | DataType::Int64
                        | DataType::UInt64
                        | DataType::Decimal128(_, 0)
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::FixedSizeBinary(_)
                ) {
                    return Err(Error::refusal(
                        "aggregate_value_type",
                        "sum, min, max and collect_set require qualified scalar operands",
                    ));
                }
            }
        }
    }
    Ok(compiled)
}

pub(super) async fn aggregate_frame_with_transform(
    context: &SessionContext,
    spec: &AggregateTransform,
    input: &str,
) -> Result<DataFrame> {
    let compiled = qualify_transform(context, spec, input).await?;
    build_aggregate_dataframe(context, &spec.grouping, input, &compiled).await
}

fn build_aggregate_dataframe<'a>(
    context: &'a SessionContext,
    definition: &'a AggregationDefinition,
    input: &'a str,
    compiled: &'a expr::CompiledAggregationSql,
) -> futures::future::BoxFuture<'a, Result<DataFrame>> {
    Box::pin(async move {
        let mut selected = definition
            .keys
            .iter()
            .map(|key| quote(key))
            .collect::<Vec<_>>();
        if selected.is_empty() {
            selected.push(format!("CAST(0 AS BIGINT) AS {}", quote(GLOBAL)));
        }
        for (idx, field) in definition.aggregates.iter().enumerate() {
            let expression = match &field.function {
                AggregateFunction::CountRows => "COUNT(*)".into(),
                AggregateFunction::Count { field } => format!("COUNT({})", quote(field)),
                AggregateFunction::CountDistinct { field } => {
                    format!("COUNT(DISTINCT {})", quote(field))
                }
                AggregateFunction::CollectSet { field } => {
                    let col = quote(field);
                    format!(
                        "COALESCE(array_sort(ARRAY_AGG(DISTINCT {col}) FILTER (WHERE {col} IS NOT NULL)), make_array())"
                    )
                }
                AggregateFunction::FirstValueBy {
                    field: val_col,
                    order_by,
                } => {
                    let fn_name = format!("__docspec_first_value_by_{idx}");
                    let order_specs = order_by.iter().map(udaf::OrderSpec::from).collect();
                    context.register_udaf(udaf::make_first_value_by_udaf(
                        fn_name.clone(),
                        order_specs,
                    ));
                    let mut args = vec![quote(val_col)];
                    args.extend(order_by.iter().map(|ob| quote(&ob.field)));
                    format!("{fn_name}({})", args.join(", "))
                }
                AggregateFunction::CollectDistinctWithProvenance {
                    field: val_col,
                    provenance_fields,
                    order_by,
                } => {
                    let fn_name = format!("__docspec_collect_distinct_prov_{idx}");
                    let order_specs = order_by.iter().map(udaf::OrderSpec::from).collect();
                    context.register_udaf(udaf::make_collect_distinct_with_provenance_udaf(
                        fn_name.clone(),
                        provenance_fields.clone(),
                        order_specs,
                    ));
                    let mut args = vec![quote(val_col)];
                    args.extend(provenance_fields.iter().map(|pf| quote(pf)));
                    args.extend(order_by.iter().map(|ob| quote(&ob.field)));
                    format!("COALESCE({fn_name}({}), make_array())", args.join(", "))
                }
                AggregateFunction::Sum { field } => {
                    format!("SUM(CAST({} AS DECIMAL(38, 0)))", quote(field))
                }
                AggregateFunction::Min { field } => format!("MIN({})", quote(field)),
                AggregateFunction::Max { field } => format!("MAX({})", quote(field)),
            };
            selected.push(format!("{expression} AS {}", quote(&field.name)));
        }

        let from_source = if compiled.pre_projections.is_empty() {
            input.to_owned()
        } else {
            let mut pre_items = vec!["*".to_owned()];
            for (name, sql) in &compiled.pre_projections {
                pre_items.push(format!("{sql} AS {}", quote(name)));
            }
            let pre_df = context
                .sql(&format!("SELECT {} FROM {input}", pre_items.join(", ")))
                .await?;
            let _ = context.deregister_table("__docspec_agg_pre");
            context.register_table("__docspec_agg_pre", pre_df.into_view())?;
            "__docspec_agg_pre".to_owned()
        };
        let where_clause = match &compiled.pre_where {
            Some(pred) => format!(" WHERE {pred}"),
            None => String::new(),
        };
        let grouping = if definition.keys.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", columns(&definition.keys))
        };

        let base_sql = format!(
            "SELECT {} FROM {from_source}{where_clause}{grouping}",
            selected.join(", ")
        );
        let base_df = context.sql(&base_sql).await?;
        if compiled.post_having.is_none() && compiled.post_projections.is_empty() {
            return Ok(base_df);
        }

        let _ = context.deregister_table("__docspec_agg_base");
        context.register_table("__docspec_agg_base", base_df.into_view())?;

        let post_map: BTreeMap<&str, &str> = compiled
            .post_projections
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let mut outer_items = Vec::new();
        let mut emitted_post = BTreeSet::new();
        let base_output_names: Vec<String> = if definition.keys.is_empty() {
            std::iter::once(GLOBAL.to_owned())
                .chain(definition.aggregates.iter().map(|f| f.name.clone()))
                .collect()
        } else {
            definition
                .keys
                .iter()
                .cloned()
                .chain(definition.aggregates.iter().map(|f| f.name.clone()))
                .collect()
        };
        for col_name in &base_output_names {
            if let Some(proj_sql) = post_map.get(col_name.as_str()) {
                outer_items.push(format!("{proj_sql} AS {}", quote(col_name)));
                emitted_post.insert(col_name.clone());
            } else {
                outer_items.push(quote(col_name));
            }
        }
        for (proj_name, proj_sql) in &compiled.post_projections {
            if !emitted_post.contains(proj_name) {
                outer_items.push(format!("{proj_sql} AS {}", quote(proj_name)));
            }
        }
        let outer_where = match &compiled.post_having {
            Some(having) => format!(" WHERE {having}"),
            None => String::new(),
        };
        let final_sql = format!(
            "SELECT {} FROM __docspec_agg_base{outer_where}",
            outer_items.join(", ")
        );
        Ok(context.sql(&final_sql).await?)
    })
}

pub(super) async fn evidence_frame_with_transform(
    context: &SessionContext,
    spec: &AggregateTransform,
    input: &str,
) -> Result<DataFrame> {
    let compiled = qualify_transform(context, spec, input).await?;
    build_evidence_dataframe(context, &spec.grouping, input, &compiled).await
}

fn build_evidence_dataframe<'a>(
    context: &'a SessionContext,
    definition: &'a AggregationDefinition,
    input: &'a str,
    compiled: &'a expr::CompiledAggregationSql,
) -> futures::future::BoxFuture<'a, Result<DataFrame>> {
    Box::pin(async move {
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
        let from_source = if compiled.pre_projections.is_empty() {
            input.to_owned()
        } else {
            let mut pre_items = vec!["*".to_owned()];
            for (name, sql) in &compiled.pre_projections {
                pre_items.push(format!("{sql} AS {}", quote(name)));
            }
            let pre_df = context
                .sql(&format!("SELECT {} FROM {input}", pre_items.join(", ")))
                .await?;
            let _ = context.deregister_table("__docspec_ev_pre");
            context.register_table("__docspec_ev_pre", pre_df.into_view())?;
            "__docspec_ev_pre".to_owned()
        };
        let where_clause = match &compiled.pre_where {
            Some(pred) => format!(" WHERE {pred}"),
            None => String::new(),
        };
        let base_contributors = format!(
            "SELECT {} FROM {from_source}{where_clause}",
            selected.join(", ")
        );
        let base_df = context.sql(&base_contributors).await?;
        if compiled.post_having.is_none() {
            return Ok(base_df);
        }
        let _ = context.deregister_table("__docspec_base_contributors");
        context.register_table("__docspec_base_contributors", base_df.into_view())?;
        let surviving = build_aggregate_dataframe(context, definition, input, compiled).await?;
        let _ = context.deregister_table("__docspec_surviving_groups");
        context.register_table("__docspec_surviving_groups", surviving.into_view())?;
        let key_names = keys(definition);
        let join_pred = key_names
            .iter()
            .map(|k| format!("(c.{} IS NOT DISTINCT FROM g.{})", quote(k), quote(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(context
            .sql(&format!(
                "SELECT c.* FROM __docspec_base_contributors c LEFT SEMI JOIN __docspec_surviving_groups g ON {join_pred}"
            ))
            .await?)
    })
}

impl Engine {
    pub(super) fn process_aggregation<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        base: &'a ReleasePin,
        previous: &'a LoadedInputs,
        current: &'a LoadedInputs,
        captured: (&'a TablePin, &'a iceberg::table::Table),
    ) -> futures::future::BoxFuture<'a, Result<write::Prepared>> {
        Box::pin(async move {
        let (captured_source, captured_native) = captured;
        let RefreshTransform::Aggregate(aggregate) = &spec.transform else {
            return Err(invalid());
        };
        let budget = &self.inner.lifecycle.budget;
        let before = source(base)?;
        let context = self.context();
        register_input(&context, aggregate, &current.borrowed(), budget).await?;
        let compiled = qualify_transform(&context, aggregate, "aggregate_input").await?;
        let pre_proj_names: BTreeSet<&str> = compiled
            .pre_projections
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        let physical_keys_only = aggregate
            .grouping
            .keys
            .iter()
            .all(|k| !pre_proj_names.contains(k.as_str()));
        let discovery_keys: Vec<String> = if physical_keys_only {
            aggregate.grouping.keys.clone()
        } else {
            Vec::new()
        };
        let affected_dir = directory.child("affected")?;
        let (affected, scanned) = self
            .changed_keys(
                &spec.dataset,
                &affected_dir,
                (
                    &previous.releases[&aggregate.input.dataset],
                    &current.releases[&aggregate.input.dataset],
                ),
                (
                    previous.canonical_source(&aggregate.input.dataset)?,
                    current.canonical_source(&aggregate.input.dataset)?,
                ),
                &discovery_keys,
            )
            .await?;
        let native = storage::open_pin(&affected, &self.inner.lifecycle.budget).await?;
        let (discovery_files, discovery_bytes, affected_data) = {
            let inv = storage::current_snapshot_files(&native, budget).await?;
            let files = inv.data.len() as u64;
            let bytes = inv
                .data
                .iter()
                .map(|file| file.file_size_in_bytes())
                .sum::<u64>();
            (files, bytes, inv.data)
        };
        let affected_schema = native.metadata().current_schema().clone();
        context.register_table(
            "affected",
            crate::query::table(native, budget.clone(), Some(affected.rows))?,
        )?;
        let key_names = keys(&aggregate.grouping);
        let mut source_skipped = 0u64;
        let mut source_probed = 0u64;
        let (scope, replacement) = if affected.rows == 0 {
            let empty_key_cols = key_names
                .iter()
                .map(|key| format!("s.{}", quote(key)))
                .collect::<Vec<_>>()
                .join(", ");
            let scope = context
                .sql(&format!(
                    "SELECT {empty_key_cols} FROM aggregate_input s LIMIT 0"
                ))
                .await?;
            let frame =
                aggregate_frame_with_transform(&context, aggregate, "aggregate_input").await?;
            (scope, frame.limit(0, Some(0))?)
        } else if aggregate.grouping.keys.is_empty() || !physical_keys_only {
            let frame =
                aggregate_frame_with_transform(&context, aggregate, "aggregate_input").await?;
            let scope = if aggregate.grouping.keys.is_empty() {
                let selected = key_names
                    .iter()
                    .enumerate()
                    .map(|(i, key)| format!("\"k{i}\" AS {}", quote(key)))
                    .collect::<Vec<_>>()
                    .join(", ");
                context
                    .sql(&format!("SELECT DISTINCT {selected} FROM affected"))
                    .await?
            } else {
                // Pre-projected grouping keys: recompute full groups and union with before keys.
                context.register_table(
                    "__docspec_agg_before",
                    crate::query::table(
                        captured_native.clone(),
                        budget.clone(),
                        Some(captured_source.rows),
                    )?,
                )?;
                let cols = columns(&key_names);
                let _ = context.deregister_table("__docspec_agg_new");
                context.register_table("__docspec_agg_new", frame.clone().into_view())?;
                context
                    .sql(&format!(
                        "SELECT DISTINCT {cols} FROM __docspec_agg_before UNION SELECT DISTINCT {cols} FROM __docspec_agg_new"
                    ))
                    .await?
            };
            (scope, frame)
        } else {
            let selected = key_names
                .iter()
                .enumerate()
                .map(|(i, key)| format!("\"k{i}\" AS {}", quote(key)))
                .collect::<Vec<_>>()
                .join(", ");
            let scope = context
                .sql(&format!("SELECT DISTINCT {selected} FROM affected"))
                .await?;
            let k_cols: Vec<String> = (0..aggregate.grouping.keys.len())
                .map(|i| format!("k{i}"))
                .collect();
            let points = write::key_points::KeyPoints::collect_table(
                &context, "affected", &k_cols, budget,
            )
            .await?;
            let input_release = &current.releases[&aggregate.input.dataset];
            let input_pin = source(input_release)?;
            let input_native = storage::open_pin(input_pin, budget).await?;
            let input_schema = input_native.metadata().current_schema().clone();
            let (retained, retained_rows) = {
                let inv = storage::current_snapshot_files(&input_native, budget).await?;
                let mut set = BTreeSet::new();
                let mut rows = 0u64;
                for file in &inv.data {
                    if write::file_may_overlap_mapped_key_files(
                        &input_schema,
                        &affected_schema,
                        &aggregate.grouping.keys,
                        &k_cols,
                        &affected_data,
                        file,
                    ) && points.as_ref().is_none_or(|p| {
                        p.may_contain(&input_schema, &aggregate.grouping.keys, file)
                    }) {
                        source_probed += 1;
                        set.insert(file.file_path().to_owned());
                        rows = rows.saturating_add(file.record_count());
                    } else {
                        source_skipped += 1;
                    }
                }
                (set, rows)
            };
            context.register_table(
                "pruned_aggregate_input",
                crate::query::pruned_occurrences(
                    input_native,
                    budget.clone(),
                    Some(retained_rows),
                    Arc::new(retained),
                )?,
            )?;
            let scoped = context
                .sql(&format!(
                    "SELECT s.* FROM pruned_aggregate_input s LEFT SEMI JOIN affected k ON {}",
                    scope_predicate(&aggregate.grouping.keys)
                ))
                .await?;
            context.register_table("scoped", scoped.into_view())?;
            let replacement =
                aggregate_frame_with_transform(&context, aggregate, "scoped").await?;
            (scope, replacement)
        };
        drop(affected_data);
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
        let applied = Box::pin(self.replace_keys(Replacement {
            dataset: &spec.dataset,
            directory: directory.child("replacement")?,
            before: &write_before,
            keys: &key_names,
            scope,
            replacement,
            replacement_bytes: None,
        }))
        .await?;
        let mut applied = self
            .finish_source_replacement_at(&spec.dataset, directory, captured_source, applied)
            .await?;
        applied.metrics.files_scanned += scanned;
        applied.metrics.data_files_written += discovery_files;
        applied.metrics.bytes_written += discovery_bytes;
        applied.metrics.files_skipped_by_pruning += source_skipped;
        applied.metrics.candidate_files_probed += source_probed;
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
        })
    }
    pub(super) fn aggregate_evidence<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        processing: &'a PhaseResult,
        before: &'a TablePin,
        context: &'a SessionContext,
    ) -> futures::future::BoxFuture<'a, Result<write::Prepared>> {
        Box::pin(async move {
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
            let evidence =
                evidence_frame_with_transform(context, aggregate, "aggregate_input").await?;
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
                    directory: directory.clone(),
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
                    filter: None,
                    projections: BTreeMap::new(),
                },
                filter: None,
                projections: BTreeMap::new(),
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
        let full_frame =
            aggregate_frame_with_transform(&context, aggregate_spec, "aggregate_input")
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
                merge: None,
                relationship: None,
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

    #[tokio::test]
    async fn hot_group_distinct_and_collect_set_respect_memory_and_incremental_refresh() {
        use crate::{
            Engine,
            budget::Limits,
            engine::{verify_applied_changes, verify_release},
            model::{
                AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, Id,
                InputSpec, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform, RunSpec,
                UpdateSpec,
            },
            storage,
        };
        use arrow::{
            array::{Array, Date32Array, Int64Array, ListArray, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;
        use std::{collections::BTreeMap, fs::File, sync::Arc};

        let root = tempfile::TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("day", DataType::Date32, true),
            Field::new("author", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let write_input = |name: &str, rows: &[(i64, Option<i32>, Option<String>)]| {
            let path = root.path().join(name);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(Date32Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.2.as_deref()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|r| format!("payload-{}", r.0)),
                    )),
                ],
            )
            .unwrap();
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            InputSpec {
                path: path.to_string_lossy().into_owned(),
                digest: storage::digest(&path).unwrap().0,
            }
        };
        let id = |name| Id::new(name).unwrap();
        let limits = Limits {
            compute_threads: 2,
            memory_bytes: 512 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 64,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 16 << 20,
        };
        let engine = Engine::open(root.path().join("workspace"), limits.clone())
            .await
            .unwrap();

        let mut initial_rows = Vec::new();
        for index in 0..128i64 {
            let author = format!("author-{index:03}");
            initial_rows.push((index * 2 + 1, Some(19_000), Some(author.clone())));
            initial_rows.push((index * 2 + 2, Some(19_000), Some(author)));
        }
        initial_rows.push((300, Some(19_000), None));
        initial_rows.push((301, Some(19_001), None));
        initial_rows.push((302, None, Some("zoe".into())));
        initial_rows.push((303, None, Some("amy".into())));
        initial_rows.push((304, None, Some("zoe".into())));

        let initial_input = write_input("initial.parquet", &initial_rows);
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
                        AggregateField {
                            name: "author_set".into(),
                            function: AggregateFunction::CollectSet {
                                field: "author".into(),
                            },
                        },
                    ],
                    filter: None,
                    projections: BTreeMap::new(),
                },
                filter: None,
                projections: BTreeMap::new(),
            }),
        };

        let RefreshTransform::Aggregate(aggregate_spec) = &spec.transform else {
            unreachable!()
        };
        let budget = &engine.inner.lifecycle.budget;
        let context = engine.context();
        let releases = BTreeMap::from([(id("events"), &source)]);
        register_input(&context, aggregate_spec, &releases, budget)
            .await
            .unwrap();
        let full_frame =
            aggregate_frame_with_transform(&context, aggregate_spec, "aggregate_input")
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
            "unexpected SortExec in CollectSet + CountDistinct plan:\n{full_rendered}"
        );
        assert!(
            full_rendered.contains("projection=[day, author]"),
            "expected projected NativeScan of only [day, author] in plan:\n{full_rendered}"
        );
        assert!(
            !full_rendered.contains("payload"),
            "unneeded payload column leaked into aggregate physical plan:\n{full_rendered}"
        );
        drop((full_plan, context));

        let first = engine.refresh(&spec).await.unwrap();
        verify_release(&first, limits.clone()).await.unwrap();

        let read_summary = |engine: &Engine, release: &crate::model::ReleasePin| {
            let engine = engine.clone();
            let release = release.clone();
            async move {
                let stream = engine.read_group(&release, "source").await.unwrap();
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
                    let distinct = batch
                        .column_by_name("distinct_authors")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let author_set = batch
                        .column_by_name("author_set")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        assert!(!author_set.is_null(row));
                        let values = author_set.value(row);
                        let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
                        let list: Vec<String> = (0..strings.len())
                            .map(|idx| {
                                assert!(!strings.is_null(idx));
                                strings.value(idx).to_owned()
                            })
                            .collect();
                        let day = (!days.is_null(row)).then(|| days.value(row));
                        actual.insert(day, (rows.value(row), distinct.value(row), list));
                    }
                }
                actual
            }
        };

        let expected_hot: Vec<String> = (0..128).map(|idx| format!("author-{idx:03}")).collect();
        assert_eq!(
            read_summary(&engine, &first).await,
            BTreeMap::from([
                (None, (3, 2, vec!["amy".to_owned(), "zoe".to_owned()])),
                (Some(19_000), (257, 128, expected_hot.clone())),
                (Some(19_001), (1, 0, Vec::new())),
            ])
        );

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
                        (1, Some(19_000), Some("author-999".into())),
                        (2, Some(19_000), None),
                        (301, Some(19_001), Some("bea".into())),
                    ],
                ),
                deletions: None,
                merge: None,
                relationship: None,
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
        verify_release(&second, limits.clone()).await.unwrap();
        let cert = engine
            .change_certificate(&second.version_ref())
            .await
            .unwrap()
            .unwrap();
        verify_applied_changes(&first, &second, &cert.changes, limits.clone())
            .await
            .unwrap();

        let mut expected_hot_after: Vec<String> =
            (1..128).map(|idx| format!("author-{idx:03}")).collect();
        expected_hot_after.push("author-999".into());
        assert_eq!(
            read_summary(&engine, &second).await,
            BTreeMap::from([
                (None, (3, 2, vec!["amy".to_owned(), "zoe".to_owned()])),
                (Some(19_000), (257, 128, expected_hot_after)),
                (Some(19_001), (1, 1, vec!["bea".to_owned()])),
            ])
        );

        // Exhausting either the byte admission semaphore or the shared DataFusion spill
        // pool during hot-group distinct accumulation refuses without advancing selection.
        let mut held = Vec::new();
        while let Ok(permit) = engine.inner.lifecycle.budget.reserve(16 << 20) {
            held.push(permit);
        }
        let mut refused = spec.clone();
        refused.operation = id("tight-memory-refusal");
        refused.base = Some(second.id.clone());
        refused.mode = RefreshMode::Full;
        assert_eq!(
            engine.refresh(&refused).await.unwrap_err().code(),
            "capacity"
        );
        drop(held);
        assert_eq!(engine.latest(&spec.dataset).await.unwrap(), second);

        let pool_reservation =
            datafusion::execution::memory_pool::MemoryConsumer::new("hot-group-ceiling")
                .register(&engine.inner.lifecycle.budget.runtime_env().memory_pool);
        pool_reservation
            .try_grow((limits.memory_bytes / 2) - 256)
            .unwrap();
        let mut refused_pool = spec.clone();
        refused_pool.operation = id("tight-pool-refusal");
        refused_pool.base = Some(second.id.clone());
        refused_pool.mode = RefreshMode::Full;
        assert_eq!(
            engine.refresh(&refused_pool).await.unwrap_err().code(),
            "query"
        );
        drop(pool_reservation);
        assert_eq!(engine.latest(&spec.dataset).await.unwrap(), second);
    }
}
