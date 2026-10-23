//! Complete update-state checks independent of optional applied-change history.

use super::*;
use crate::engine::verification::Checks;
use datafusion::{
    common::{Column, ScalarValue},
    functions_aggregate::sum::sum,
    logical_expr::{JoinType, when},
};

/// Checks schema continuity and optionally reconstructs a complete source update.
pub(in crate::engine) async fn verify_update(
    dataset: &DatasetPin,
    base: &DatasetPin,
    spec: &UpdateSpec,
    budget: &Budget,
    checks: &mut Checks,
) -> Result<()> {
    let before = base
        .groups
        .get("source")
        .ok_or_else(|| invalid("base release has no source table"))?;
    let after = dataset
        .groups
        .get("source")
        .ok_or_else(|| invalid("updated release has no source table"))?;
    let native_before = checks.objects().open_pin(before, budget).await?;
    let native_after = checks.objects().open_pin(after, budget).await?;
    if !storage::identity::preserves_schema(
        native_before.metadata().current_schema(),
        native_after.metadata().current_schema(),
    )? || native_before.metadata().default_partition_spec()
        != native_after.metadata().default_partition_spec()
        || base.partition_by != dataset.partition_by
    {
        return Err(invalid(
            "source update schema or partition specification differs from base",
        ));
    }
    let public = storage::identity::public_schema(&native_after)?;
    let unique: std::collections::BTreeSet<_> = spec.keys.iter().collect();
    if spec.keys.is_empty()
        || unique.len() != spec.keys.len()
        || spec.keys.iter().any(|key| {
            public.field_with_name(key).is_err() || storage::defaults::supplied_id(key).is_some()
        })
    {
        return Err(invalid(
            "update key fields are empty, duplicated, or absent from public schema",
        ));
    }
    if let Some(merge_spec) = &spec.merge {
        super::merge::validate(
            merge_spec,
            native_after.metadata().current_schema(),
            &spec.keys,
        )
        .map_err(|_| invalid("update merge specification is invalid for source schema"))?;
    }
    if !checks.deep() {
        return Ok(());
    }
    if before != after {
        checks.table(before, budget).await?;
    }
    let context = budget.context();
    let (replacement, scope) =
        replacement_inputs(&context, dataset, spec, &native_after, budget).await?;
    let after_ice_schema = native_after.metadata().current_schema().clone();
    let located = storage::identity::has_origins(&after_ice_schema)?;
    for (name, native, pin) in [
        ("before", native_before, before),
        ("after", native_after, after),
    ] {
        context.register_table(
            name,
            if located {
                crate::query::occurrences(native, budget.clone(), Some(pin.rows))?
            } else {
                crate::query::table(native, budget.clone(), Some(pin.rows))?
            },
        )?;
    }
    let fields: Vec<_> = public
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let kept = key_join(
        context.table("before").await?,
        scope.clone(),
        &spec.keys,
        JoinType::LeftAnti,
    )?;
    let replacement = if let Some(merge_spec) = &spec.merge {
        let prior = key_join(
            context.table("before").await?,
            scope.clone(),
            &spec.keys,
            JoinType::LeftSemi,
        )?;
        super::merge::apply_merge(
            &context,
            prior,
            replacement,
            &after_ice_schema,
            &spec.keys,
            merge_spec,
            budget,
        )
        .await
        .map_err(|_| invalid("merged replacement reconstruction failed"))?
    } else {
        replacement
    };
    let kept = members(
        kept,
        &fields,
        if located {
            MemberProjection::RetainedOrigins
        } else {
            MemberProjection::Unlocated
        },
        1,
    )?;
    let added = members(
        replacement,
        &fields,
        if located {
            MemberProjection::AddedOriginsNull
        } else {
            MemberProjection::Unlocated
        },
        1,
    )?;
    let actual = context.table("after").await?;
    let actual = if located {
        // A scope marker distinguishes an actual null key match from no match.
        let mut marker = "__docspec_update_scope".to_owned();
        while scope.schema().has_column_with_unqualified_name(&marker) {
            marker.push('_');
        }
        let scope = scope.with_column(&marker, lit(true))?;
        let current = key_join(actual, scope, &spec.keys, JoinType::Left)?;
        let mut selected = values(&fields, Some("l"));
        selected.push(
            when(
                qualified("r", &marker).is_null(),
                qualified("l", storage::OCCURRENCE_FILE),
            )
            .otherwise(lit(ScalarValue::Utf8(None)))?
            .alias("file"),
        );
        selected.push(
            when(
                qualified("r", &marker).is_null(),
                qualified("l", storage::OCCURRENCE_ROW),
            )
            .otherwise(lit(ScalarValue::Int64(None)))?
            .alias("row"),
        );
        selected.push(lit(-1_i64).alias("weight"));
        current.select(selected)?
    } else {
        members(actual, &fields, MemberProjection::Unlocated, -1)?
    };
    let mut group: Vec<_> = (0..fields.len())
        .map(|index| col(format!("c{index}")))
        .collect();
    if located {
        group.extend([col("file"), col("row")]);
    }
    let difference = kept
        .union(added)?
        .union(actual)?
        .aggregate(group, vec![sum(col("weight")).alias("net")])?
        .filter(col("net").not_eq(lit(0_i64)))?;
    if any_rows(difference).await? {
        return Err(invalid(
            "reconstructed update rows differ from committed source table",
        ));
    }
    Ok(())
}

async fn replacement_inputs(
    context: &SessionContext,
    dataset: &DatasetPin,
    spec: &UpdateSpec,
    native: &Table,
    budget: &Budget,
) -> Result<(DataFrame, DataFrame)> {
    for (name, input) in dataset.request.inputs() {
        let object = dataset
            .inputs
            .get(&dataset.request.input_key(&name))
            .filter(|object| object.digest == input.digest)
            .ok_or_else(|| invalid("update input binding is missing or digest differs"))?;
        context
            .register_parquet(name.as_str(), &object.path, ParquetReadOptions::default())
            .await?;
    }
    let map_input_err = |error: Error| match error.code() {
        "schema_mismatch" | "conflicting_update" => {
            invalid("source update schema, keys or complete values differ from retained inputs")
        }
        _ => error,
    };
    let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
    let replacement = super::normalize_replacement_input(context, native, &spec.keys, budget)
        .await
        .map_err(map_input_err)?;
    let scope = super::build_update_scope(context, &replacement, spec, &schema)
        .await
        .map_err(map_input_err)?;
    Ok((replacement, scope))
}

fn values(fields: &[&str], relation: Option<&str>) -> Vec<Expr> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            relation
                .map_or_else(
                    || col(Column::from_name(*field)),
                    |name| qualified(name, field),
                )
                .alias(format!("c{index}"))
        })
        .collect()
}

#[derive(Clone, Copy)]
enum MemberProjection {
    Unlocated,
    RetainedOrigins,
    AddedOriginsNull,
}

fn members(
    frame: DataFrame,
    fields: &[&str],
    projection: MemberProjection,
    weight: i64,
) -> Result<DataFrame> {
    let mut selected = values(fields, None);
    match projection {
        MemberProjection::Unlocated => {}
        MemberProjection::RetainedOrigins => {
            selected.push(col(storage::OCCURRENCE_FILE).alias("file"));
            selected.push(col(storage::OCCURRENCE_ROW).alias("row"));
        }
        MemberProjection::AddedOriginsNull => {
            selected.push(lit(ScalarValue::Utf8(None)).alias("file"));
            selected.push(lit(ScalarValue::Int64(None)).alias("row"));
        }
    }
    selected.push(lit(weight).alias("weight"));
    Ok(frame.select(selected)?)
}

fn invalid(reason: &'static str) -> Error {
    Error::refusal("invalid_changes", reason)
}
