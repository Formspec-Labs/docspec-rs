//! Verification of related results, contributor evidence, and captured input equivalence.

use super::{TUPLE, evidence_frame, invalid, load_inputs, source_frame, validate};
use crate::{
    Error, Result,
    budget::Budget,
    engine::{inputs, verification, write},
    model::{Id, JoinCardinality, RefreshTransform, RelatedResult, ReleasePin, TablePin},
    storage,
};
use datafusion::{
    common::Column,
    functions_aggregate::expr_fn::{count, sum},
    prelude::{Expr, SessionContext, col, lit},
};
use std::collections::BTreeMap;

pub(in crate::engine) fn verify_related<'a>(
    source: &'a TablePin,
    related: &'a RelatedResult,
    budget: &'a Budget,
    checks: &'a mut verification::Checks,
) -> futures::future::BoxFuture<'a, Result<()>> {
    // Heap-box state machine to bound poll-frame stack size and worker admission
    Box::pin(async move {
        let loaded = load_inputs(&related.coverage, &BTreeMap::new(), budget).await?;
        verify_related_with_inputs(source, related, &loaded.borrowed(), budget, checks).await
    })
}

pub(in crate::engine) fn verify_related_with_inputs<'a>(
    source: &'a TablePin,
    related: &'a RelatedResult,
    releases: &'a BTreeMap<Id, &'a ReleasePin>,
    budget: &'a Budget,
    checks: &'a mut verification::Checks,
) -> futures::future::BoxFuture<'a, Result<()>> {
    // Heap-box state machine to bound poll-frame stack size and worker admission
    Box::pin(async move {
        if related.coverage.inputs != related.request.inputs() {
            return Err(invalid(
                "related result input coverage disagrees with request inputs",
            ));
        }
        let relationships = related
            .coverage
            .inputs
            .keys()
            .map(|dataset| {
                releases
                    .get(dataset)
                    .map(|release| (dataset.clone(), release.dataset.relationship.clone()))
                    .ok_or_else(|| invalid("missing input release for related coverage dataset"))
            })
            .collect::<Result<_>>()?;
        inputs::check_relationships(&related.coverage, &relationships)?;
        // Reconstruction retains the canonical producer interpretation. The physical
        // capture is checked separately so its logical label cannot certify itself.
        for (dataset, captured) in &related.input_sources {
            if !related.coverage.inputs.contains_key(dataset) || captured.metadata.owner != *dataset
            {
                return Err(invalid(
                    "captured input source owner or coverage membership disagrees",
                ));
            }
            let canonical = releases
                .get(dataset)
                .ok_or_else(|| invalid("missing release for captured input source"))?
                .dataset
                .groups
                .get("source")
                .ok_or_else(|| invalid("input release is missing its canonical source pin"))?;
            let selected = write::open_captured_source(canonical, captured, budget).await?;
            if checks.deep() {
                checks.table(captured, budget).await?;
                let context = budget.context();
                for (alias, native, rows) in [
                    (
                        "canonical_input",
                        storage::open_pin(canonical, budget).await?,
                        canonical.rows,
                    ),
                    ("captured_input", selected, captured.rows),
                ] {
                    context.register_table(
                        alias,
                        crate::query::occurrences(native, budget.clone(), Some(rows))?,
                    )?;
                }
                equal_multisets(&context, "canonical_input", "captured_input").await?;
            }
        }
        let context = budget.context();
        validate(&related.request)?;
        let expected = source_frame(
            &context,
            &related.request.transform,
            releases,
            budget,
            false,
        )
        .await?;
        let expected_schema =
            storage::arrow_schema_to_schema_auto_assign_ids(expected.schema().as_arrow())?;
        let native = storage::open_pin(source, budget).await?;
        if !storage::identity::preserves_schema(
            &expected_schema,
            native.metadata().current_schema(),
        )? {
            return Err(Error::refusal(
                "invalid_related_result",
                "native result schema differs from the declared computation",
            ));
        }
        context.register_table("expected", expected.into_view())?;
        context.register_table(
            "actual",
            crate::query::table(native, budget.clone(), Some(source.rows))?,
        )?;
        if checks.deep() {
            equal_multisets(&context, "expected", "actual").await?;
        }
        checks.table(&related.evidence, budget).await?;
        let evidence = storage::open_pin(&related.evidence, budget).await?;
        let expected_evidence =
            evidence_frame(&context, &related.request.transform, "actual", budget).await?;
        let evidence_schema =
            storage::arrow_schema_to_schema_auto_assign_ids(expected_evidence.schema().as_arrow())?;
        context.register_table("expected_evidence", expected_evidence.into_view())?;
        if *evidence.metadata().current_schema().as_ref() != evidence_schema {
            return Err(Error::refusal(
                "invalid_related_result",
                "contributor evidence schema differs from the selected native result",
            ));
        }
        if !checks.deep() {
            return Ok(());
        }
        context.register_table(
            "evidence",
            crate::query::table(evidence, budget.clone(), Some(related.evidence.rows))?,
        )?;
        equal_multisets(&context, "expected_evidence", "evidence").await?;
        let RefreshTransform::Join(join) = &related.request.transform else {
            return Ok(());
        };
        for side in match join.join.cardinality {
            JoinCardinality::ManyToMany => vec![],
            JoinCardinality::ManyToOne => vec![0],
            JoinCardinality::OneToOne => vec![0, 2],
        } {
            let duplicates = context
                .table("actual")
                .await?
                .filter(
                    col(Column::from_name(TUPLE[0]))
                        .is_not_null()
                        .and(col(Column::from_name(TUPLE[2])).is_not_null()),
                )?
                .aggregate(
                    vec![
                        col(Column::from_name(TUPLE[side])),
                        col(Column::from_name(TUPLE[side + 1])),
                    ],
                    vec![count(lit(1_i64)).alias("__docspec_partners")],
                )?
                .filter(col(Column::from_name("__docspec_partners")).gt(lit(1_i64)))?
                .limit(0, Some(1))?;
            if write::any_rows(duplicates).await? {
                return Err(Error::refusal(
                    "join_cardinality",
                    "an occurrence has more matching partners than the declared rule permits",
                ));
            }
        }
        Ok(())
    })
}

async fn equal_multisets(context: &SessionContext, left: &str, right: &str) -> Result<()> {
    let left_frame = context.table(left).await?;
    let columns: Vec<Expr> = left_frame
        .schema()
        .fields()
        .iter()
        .map(|field| col(Column::from_name(field.name())))
        .collect();
    let weighted_left = left_frame.select(
        columns
            .iter()
            .cloned()
            .chain(std::iter::once(lit(1_i64).alias("__docspec_weight")))
            .collect::<Vec<_>>(),
    )?;
    let weighted_right = context.table(right).await?.select(
        columns
            .iter()
            .cloned()
            .chain(std::iter::once(lit(-1_i64).alias("__docspec_weight")))
            .collect::<Vec<_>>(),
    )?;
    let differences = weighted_left
        .union(weighted_right)?
        .aggregate(
            columns,
            vec![sum(col(Column::from_name("__docspec_weight"))).alias("__docspec_total")],
        )?
        .filter(col(Column::from_name("__docspec_total")).not_eq(lit(0_i64)))?
        .limit(0, Some(1))?;
    if write::any_rows(differences).await? {
        return Err(Error::refusal(
            "invalid_related_result",
            format!("typed row multiplicities differ between {left} and {right}"),
        ));
    }
    Ok(())
}
