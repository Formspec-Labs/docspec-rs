//! Relational transforms: equality joins, aggregations, connected components, and refreshes.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{
    Id, InputCoverage, Predicate, ScalarExpr, SortColumn, SourceRelationship, TablePin, VersionRef,
};

fn combine_filters(base: &Option<Predicate>, extra: &Option<Predicate>) -> Option<Predicate> {
    match (base, extra) {
        (None, None) => None,
        (Some(p), None) | (None, Some(p)) => Some(p.clone()),
        (Some(a), Some(b)) => Some(Predicate::And {
            exprs: vec![a.clone(), b.clone()],
        }),
    }
}

fn merge_projections(
    base: &BTreeMap<String, ScalarExpr>,
    extra: &BTreeMap<String, ScalarExpr>,
) -> BTreeMap<String, ScalarExpr> {
    let mut out = base.clone();
    for (k, v) in extra {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Which source supplies a field in a typed join.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinSide {
    Left,
    Right,
}

/// Unmatched occurrence behavior for an equality join.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Full,
}

/// Whether null join-key components match other null components.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullMatching {
    Never,
    Equal,
}

/// Maximum matching partners per occurrence; duplicate business keys remain distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinCardinality {
    ManyToMany,
    ManyToOne,
    OneToOne,
}

/// One equality condition between exact, equally typed source fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinKey {
    pub left: String,
    pub right: String,
}

/// One output field copied without coercion from a declared input role.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinField {
    pub side: JoinSide,
    pub field: String,
    pub name: String,
}

/// A qualified equality join with explicit multiplicity and unmatched behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinDefinition {
    pub keys: Vec<JoinKey>,
    pub kind: JoinKind,
    pub nulls: NullMatching,
    pub cardinality: JoinCardinality,
    pub fields: Vec<JoinField>,
    /// Optional row-local filter predicate evaluated on joined rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    /// Optional named scalar projections appended to (or overriding) output fields.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<String, ScalarExpr>,
}

/// Selection policy while live producer heads continue advancing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPolicy {
    /// Accept complete forward coverage and report the observed remaining lag.
    #[default]
    Captured,
    /// Require each consumed version to remain its producer's selected head.
    Latest,
}

/// Work selection for refreshing a related result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    /// Reconstruct the complete result from captured inputs.
    #[default]
    Full,
    /// Replace complete match groups affected by changed native files.
    Incremental,
}

/// Equality join inputs and their declared matching rules.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinTransform {
    pub left: VersionRef,
    pub right: VersionRef,
    pub join: JoinDefinition,
    /// Optional transform-level row filter predicate combined with `join.filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    /// Optional transform-level scalar projections merged with `join.projections`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<String, ScalarExpr>,
}

impl JoinTransform {
    /// Returns the combined filter predicate from `join.filter` and `self.filter`.
    pub fn effective_filter(&self) -> Option<Predicate> {
        combine_filters(&self.join.filter, &self.filter)
    }

    /// Returns the merged scalar projections from `join.projections` and `self.projections`.
    pub fn effective_projections(&self) -> BTreeMap<String, ScalarExpr> {
        merge_projections(&self.join.projections, &self.projections)
    }
}

/// Fixed-state aggregate functions with exact integer sums.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AggregateFunction {
    CountRows,
    Count {
        #[serde(alias = "column")]
        field: String,
    },
    CountDistinct {
        #[serde(alias = "column")]
        field: String,
    },
    /// Collects sorted distinct non-null values into a single list row per group.
    ///
    /// Materializes one group's set in a single output row (`O(distinct)` per
    /// group), unlike [`Self::CountDistinct`], which emits only a scalar count.
    /// Unbounded-cardinality groups whose materialized row or state exceeds the
    /// configured batch or memory allowance (`Limits::max_batch_bytes` /
    /// `Limits::memory_bytes`) refuse with `capacity`, `query`, or
    /// `oversized_batch`.
    CollectSet {
        #[serde(alias = "column")]
        field: String,
    },
    /// Selects the non-null value of `field` from the row that sorts first by `order_by`.
    FirstValueBy {
        #[serde(alias = "column")]
        field: String,
        order_by: Vec<SortColumn>,
    },
    /// Collects sorted distinct non-null values of `field` into a list of structs
    /// pairing each distinct `value` with representative `provenance_fields` chosen by `order_by`.
    CollectDistinctWithProvenance {
        #[serde(alias = "column")]
        field: String,
        #[serde(alias = "provenance_columns", alias = "provenance")]
        provenance_fields: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        order_by: Vec<SortColumn>,
    },
    Sum {
        #[serde(alias = "column")]
        field: String,
    },
    Min {
        #[serde(alias = "column")]
        field: String,
    },
    Max {
        #[serde(alias = "column")]
        field: String,
    },
    /// Selects the deterministic first value according to the transform's row order.
    FirstValue {
        field: String,
    },
}

/// One named aggregate output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateField {
    pub name: String,
    pub function: AggregateFunction,
}

/// Complete group recomputation; an empty key list denotes one global group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregationDefinition {
    pub keys: Vec<String>,
    pub aggregates: Vec<AggregateField>,
    /// Optional filter predicate (evaluated before grouping when referencing input columns,
    /// or after grouping as HAVING when referencing aggregate output names).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    /// Optional scalar projections (evaluated before grouping when referenced by keys/aggregates,
    /// or after grouping when computed over group keys and aggregate outputs).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<String, ScalarExpr>,
}

/// Aggregation over exact native input occurrences, including joined results.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateTransform {
    pub input: VersionRef,
    pub grouping: AggregationDefinition,
    /// Optional transform-level filter predicate combined with `grouping.filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    /// Optional transform-level scalar projections merged with `grouping.projections`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<String, ScalarExpr>,
}

impl AggregateTransform {
    /// Returns the combined filter predicate from `grouping.filter` and `self.filter`.
    pub fn effective_filter(&self) -> Option<Predicate> {
        combine_filters(&self.grouping.filter, &self.filter)
    }

    /// Returns the merged scalar projections from `grouping.projections` and `self.projections`.
    pub fn effective_projections(&self) -> BTreeMap<String, ScalarExpr> {
        merge_projections(&self.grouping.projections, &self.projections)
    }
}

/// Bipartite edge namespace and source column for connected-components clustering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectedEdgeSpec {
    pub namespace: String,
    pub column: String,
}

/// Bipartite connected-components clustering and representative aggregation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectedComponentsTransform {
    pub input: VersionRef,
    pub row_key: String,
    pub edges: Vec<ConnectedEdgeSpec>,
    #[serde(default)]
    pub order_by: Vec<SortColumn>,
    pub document_id_column: String,
    pub aggregates: Vec<AggregateField>,
}

/// Qualified related computations sharing the same refresh lifecycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RefreshTransform {
    Join(JoinTransform),
    Aggregate(AggregateTransform),
    ConnectedComponents(ConnectedComponentsTransform),
}
impl RefreshTransform {
    pub(crate) fn versions(&self) -> Vec<&VersionRef> {
        match self {
            Self::Join(join) => vec![&join.left, &join.right],
            Self::Aggregate(aggregate) => vec![&aggregate.input],
            Self::ConnectedComponents(cc) => vec![&cc.input],
        }
    }
    pub(crate) fn same_roles(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
            && self
                .versions()
                .iter()
                .map(|v| &v.dataset)
                .eq(other.versions().iter().map(|v| &v.dataset))
    }
    pub(crate) fn same_definition(&self, other: &Self) -> bool {
        self.same_roles(other)
            && match (self, other) {
                (Self::Join(a), Self::Join(b)) => {
                    a.join == b.join && a.filter == b.filter && a.projections == b.projections
                }
                (Self::Aggregate(a), Self::Aggregate(b)) => {
                    a.grouping == b.grouping
                        && a.filter == b.filter
                        && a.projections == b.projections
                }
                (Self::ConnectedComponents(a), Self::ConnectedComponents(b)) => {
                    a.row_key == b.row_key
                        && a.edges == b.edges
                        && a.order_by == b.order_by
                        && a.document_id_column == b.document_id_column
                        && a.aggregates == b.aggregates
                }
                _ => false,
            }
    }
}

/// A reproducible refresh over exact native input versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshSpec {
    pub dataset: Id,
    pub operation: Id,
    /// None requires an absent output; otherwise capture the exact selected output.
    pub base: Option<Id>,
    pub definition: String,
    pub transform: RefreshTransform,
    #[serde(default)]
    pub policy: RefreshPolicy,
    #[serde(default)]
    pub mode: RefreshMode,
    #[serde(default)]
    pub relationship: Option<SourceRelationship>,
}

impl RefreshSpec {
    /// Returns the exact input vector, independent of processing roles.
    pub fn inputs(&self) -> BTreeMap<Id, VersionRef> {
        self.transform
            .versions()
            .into_iter()
            .map(|version| (version.dataset.clone(), version.clone()))
            .collect()
    }
}

/// Retained contributor evidence bound to a complete related-source computation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelatedResult {
    /// Exact equivalent source layouts consumed while retaining logical input coverage.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_sources: BTreeMap<Id, TablePin>,
    pub request: RefreshSpec,
    pub coverage: InputCoverage,
    pub evidence: TablePin,
}
