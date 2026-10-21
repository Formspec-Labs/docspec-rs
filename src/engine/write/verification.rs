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
    let before = base.groups.get("source").ok_or_else(invalid)?;
    let after = dataset.groups.get("source").ok_or_else(invalid)?;
    let native_before = checks.objects().open_pin(before, budget).await?;
    let native_after = checks.objects().open_pin(after, budget).await?;
    if !storage::identity::preserves_schema(
        native_before.metadata().current_schema(),
        native_after.metadata().current_schema(),
    )? {
        return Err(invalid());
    }
    let public = storage::identity::public_schema(&native_after)?;
    let unique: std::collections::BTreeSet<_> = spec.keys.iter().collect();
    if spec.keys.is_empty()
        || unique.len() != spec.keys.len()
        || spec.keys.iter().any(|key| {
            public.field_with_name(key).is_err() || storage::defaults::supplied_id(key).is_some()
        })
    {
        return Err(invalid());
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
    let located = storage::identity::has_origins(native_after.metadata().current_schema())?;
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
    let kept = members(kept, &fields, located, false, 1)?;
    let added = members(replacement, &fields, located, true, 1)?;
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
        members(actual, &fields, false, false, -1)?
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
        return Err(invalid());
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
            .ok_or_else(invalid)?;
        context
            .register_parquet(name.as_str(), &object.path, ParquetReadOptions::default())
            .await?;
    }
    let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
    let replacement = coerce_dataframe_for_schema(context.table("replacements").await?, &schema)?;
    let replacement = schema::normalize(
        replacement,
        native.metadata().current_schema(),
        &spec.keys,
        budget,
    )
    .await?;
    if !same_fields(&schema, replacement.schema().as_arrow())? {
        return Err(invalid());
    }
    let key_columns = || {
        spec.keys
            .iter()
            .map(|key| col(Column::from_name(key)))
            .collect::<Vec<_>>()
    };
    let mut scope = replacement.clone().select(key_columns())?.distinct()?;
    if spec.deletions.is_some() {
        let deletions = coerce_dataframe_for_schema(context.table("deletions").await?, &schema)?;
        let expected = spec
            .keys
            .iter()
            .map(|key| {
                schema
                    .field_with_name(key)
                    .map(|field| field.clone().with_nullable(true))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let supplied = spec
            .keys
            .iter()
            .map(|key| {
                deletions
                    .schema()
                    .as_arrow()
                    .field_with_name(key)
                    .ok()
                    .map(|field| field.clone().with_nullable(true))
            })
            .collect::<Option<Vec<_>>>();
        if deletions.schema().fields().len() != spec.keys.len()
            || match supplied {
                Some(fields) => !same_fields(
                    &arrow::datatypes::Schema::new(expected),
                    &arrow::datatypes::Schema::new(fields),
                )?,
                None => true,
            }
        {
            return Err(invalid());
        }
        if any_rows(key_join(
            replacement.clone(),
            deletions.clone(),
            &spec.keys,
            JoinType::Inner,
        )?)
        .await?
        {
            return Err(invalid());
        }
        scope = scope.union(deletions.select(key_columns())?)?.distinct()?;
    }
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

fn members(
    frame: DataFrame,
    fields: &[&str],
    located: bool,
    added: bool,
    weight: i64,
) -> Result<DataFrame> {
    let mut selected = values(fields, None);
    if located {
        selected.push(
            if added {
                lit(ScalarValue::Utf8(None))
            } else {
                col(storage::OCCURRENCE_FILE)
            }
            .alias("file"),
        );
        selected.push(
            if added {
                lit(ScalarValue::Int64(None))
            } else {
                col(storage::OCCURRENCE_ROW)
            }
            .alias("row"),
        );
    }
    selected.push(lit(weight).alias("weight"));
    Ok(frame.select(selected)?)
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_changes",
        "source update schema, keys or complete values differ from retained inputs",
    )
}
