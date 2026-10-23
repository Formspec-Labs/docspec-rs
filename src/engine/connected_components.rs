//! Bipartite connected-components clustering and representative aggregation.

mod eval;
mod incremental;
mod lineage;
mod sha256;
mod topology;

pub(super) use lineage::{evidence_frame, source_frame};
pub(crate) use sha256::sha256_hex;

use super::{
    aggregation::{CONTRIBUTOR, argument},
    expr::is_valid_identifier,
    incremental::source,
    write::identifier as quote,
    *,
};
use arrow::datatypes::DataType;
use eval::is_supported_scalar;
use std::collections::{BTreeMap, BTreeSet};

/// Computes the deterministic 64-char SHA-256 hex `document_id` for a component.
///
/// Prefers body tokens (`["bodies", [...]]`) when present; otherwise hashes sorted source
/// keys (`["sources", [...]]`).
pub(crate) fn compute_document_id(bodies: &BTreeSet<String>, sources: &BTreeSet<String>) -> String {
    let json = if !bodies.is_empty() {
        let list: Vec<&str> = bodies.iter().map(String::as_str).collect();
        serde_json::to_vec(&("bodies", list)).expect("tuple serializes")
    } else {
        let list: Vec<&str> = sources.iter().map(String::as_str).collect();
        serde_json::to_vec(&("sources", list)).expect("tuple serializes")
    };
    sha256_hex(&json)
}

/// Validates syntactic invariants of a [`ConnectedComponentsTransform`] specification.
pub(super) fn validate(spec: &ConnectedComponentsTransform) -> Result<()> {
    if !is_valid_identifier(&spec.row_key)
        || !is_valid_identifier(&spec.document_id_column)
        || spec.edges.is_empty()
        || spec.aggregates.is_empty()
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "connected_components requires row_key, document_id_column, edges, and aggregates",
        ));
    }
    let mut edge_pairs = BTreeSet::new();
    for edge in &spec.edges {
        if edge.namespace.trim().is_empty()
            || !is_valid_identifier(&edge.column)
            || !edge_pairs.insert((
                edge.namespace.to_ascii_lowercase(),
                edge.column.to_ascii_lowercase(),
            ))
        {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components edges require distinct non-empty namespaces and columns",
            ));
        }
    }
    let mut order_cols = BTreeSet::new();
    for sort in &spec.order_by {
        if !is_valid_identifier(&sort.field) || !order_cols.insert(sort.field.to_ascii_lowercase())
        {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components order_by columns must be distinct and non-empty",
            ));
        }
    }
    let mut output_names = BTreeSet::new();
    output_names.insert(spec.document_id_column.to_ascii_lowercase());
    for agg in &spec.aggregates {
        if !is_valid_identifier(&agg.name)
            || !output_names.insert(agg.name.to_ascii_lowercase())
            || argument(&agg.function).is_some_and(|arg| !is_valid_identifier(arg))
        {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components output fields and aggregate operands must be valid and distinct",
            ));
        }
    }
    Ok(())
}

/// Registers the verified input release as `cc_input` with physical occurrence columns and
/// validates its schema against `spec`.
pub(super) async fn register_input(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
) -> Result<()> {
    let release = releases
        .get(&spec.input.dataset)
        .ok_or_else(|| invalid("connected_components input dataset release is missing"))?;
    if release.version_ref() != spec.input {
        return Err(invalid(
            "connected_components input release version does not match specification",
        ));
    }
    let src = source(release)?;
    if src.rows > i64::MAX as u64 {
        return Err(Error::refusal(
            "aggregate_range",
            "connected_components requires at most i64::MAX verified input rows",
        ));
    }
    let _ = context.deregister_table("cc_input");
    context.register_table(
        "cc_input",
        crate::query::occurrences(
            storage::open_pin(src, budget).await?,
            budget.clone(),
            Some(src.rows),
        )?,
    )?;
    qualify(context, spec, "cc_input").await
}

pub(super) async fn qualify(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
) -> Result<()> {
    validate(spec)?;
    let frame = context.table(input).await?;
    let schema = frame.schema().as_arrow();
    let row_key_dt = schema.field_with_name(&spec.row_key)?.data_type();
    if !matches!(row_key_dt, DataType::Utf8 | DataType::LargeUtf8) {
        return Err(Error::refusal(
            "invalid_refresh",
            "connected_components row_key column must be Utf8",
        ));
    }
    for edge in &spec.edges {
        let dt = schema.field_with_name(&edge.column)?.data_type();
        let valid_edge = matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
            || matches!(
                dt,
                DataType::List(f) | DataType::LargeList(f)
                    if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8)
            );
        if !valid_edge {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components edge columns must be Utf8 or List<Utf8>",
            ));
        }
    }
    for sort in &spec.order_by {
        let dt = schema.field_with_name(&sort.field)?.data_type();
        if !is_supported_scalar(dt) {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components order_by column must be a supported scalar type",
            ));
        }
    }
    for agg in &spec.aggregates {
        if let Some(field) = argument(&agg.function) {
            let dt = schema.field_with_name(field)?.data_type();
            let valid = match &agg.function {
                AggregateFunction::CountRows => true,
                AggregateFunction::Count { .. } | AggregateFunction::CountDistinct { .. } => {
                    is_supported_scalar(dt)
                }
                AggregateFunction::FirstValue { .. } => is_supported_scalar(dt),
                AggregateFunction::CollectSet { .. } => {
                    is_supported_scalar(dt)
                        || matches!(
                            dt,
                            DataType::List(f) | DataType::LargeList(f)
                                if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                        )
                }
                AggregateFunction::Sum { .. } => matches!(dt, DataType::Int32 | DataType::Int64),
                AggregateFunction::Min { .. } | AggregateFunction::Max { .. } => {
                    is_supported_scalar(dt)
                }
                AggregateFunction::FirstValueBy { .. }
                | AggregateFunction::CollectDistinctWithProvenance { .. } => false,
            };
            if !valid {
                return Err(Error::refusal(
                    "aggregate_value_type",
                    "connected_components aggregate operand has an unsupported data type",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn invalid(reason: impl Into<String>) -> Error {
    Error::refusal("invalid_related_result", reason)
}
