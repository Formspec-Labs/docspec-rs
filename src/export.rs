//! Explicit disclosure policies and immutable primitive Parquet distributions.
// Rust guideline compliant 2026-02-21

use crate::model::{Id, ObjectRef, OperationId, VersionRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn default_group() -> String {
    "source".into()
}

fn default_true() -> bool {
    true
}

fn default_ordinal_column() -> String {
    "occurrence_order".into()
}

fn default_child_row_group_rows() -> usize {
    2048
}

/// Explicitly authorizes the active row population in the selected native snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportRows {
    /// Include every active row; row restrictions must be prepared upstream or via `filter`.
    #[default]
    All,
}

/// Sort direction for an exported column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// One sort key with explicit direction and null placement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortColumn {
    pub column: String,
    #[serde(default)]
    pub direction: SortDirection,
    /// When true, NULL values sort before non-null values; defaults to false (NULLS LAST).
    #[serde(default)]
    pub nulls_first: bool,
}

impl SortColumn {
    /// Ascending sort with NULLS LAST.
    pub fn asc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Asc,
            nulls_first: false,
        }
    }

    /// Descending sort with NULLS LAST.
    pub fn desc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Desc,
            nulls_first: false,
        }
    }
}

/// One `WHEN condition THEN result` branch in a [`TaggedScalarExpr::CaseWhen`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhenThen {
    pub when: Predicate,
    pub then: ScalarExpr,
}

/// Structured AST variants for a deterministic row-local scalar expression.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaggedScalarExpr {
    /// A SQL scalar expression parsed and validated for determinism against the input schema.
    Sql {
        expression: String,
    },
    /// Reference an existing column by name.
    Column {
        name: String,
    },
    /// A typed literal constant (or `null`).
    Literal {
        #[serde(default)]
        value: serde_json::Value,
    },
    /// `CASE WHEN ... THEN ... ELSE ... END`.
    CaseWhen {
        branches: Vec<WhenThen>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        else_expr: Option<Box<ScalarExpr>>,
    },
    /// `COALESCE(expr1, expr2, ...)`.
    Coalesce {
        exprs: Vec<ScalarExpr>,
    },
    /// `CONCAT(expr1, expr2, ...)`.
    Concat {
        exprs: Vec<ScalarExpr>,
    },
    /// `LOWER(expr)`.
    Lower {
        expr: Box<ScalarExpr>,
    },
    /// `UPPER(expr)`.
    Upper {
        expr: Box<ScalarExpr>,
    },
    /// `TRIM(expr)`.
    Trim {
        expr: Box<ScalarExpr>,
    },
}

/// Deterministic scalar expression accepted as either a SQL string or a structured AST.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarExpr {
    SqlString(String),
    Ast(TaggedScalarExpr),
}

impl ScalarExpr {
    /// Creates a SQL scalar expression.
    pub fn sql(expression: impl Into<String>) -> Self {
        Self::SqlString(expression.into())
    }

    /// References a column by name.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Ast(TaggedScalarExpr::Column { name: name.into() })
    }

    /// Creates a JSON literal constant.
    pub fn literal(value: impl Into<serde_json::Value>) -> Self {
        Self::Ast(TaggedScalarExpr::Literal {
            value: value.into(),
        })
    }

    /// Creates a `NULL` literal constant.
    pub fn null() -> Self {
        Self::Ast(TaggedScalarExpr::Literal {
            value: serde_json::Value::Null,
        })
    }

    /// Creates a `CASE WHEN ... THEN ... ELSE ... END` expression.
    pub fn case_when(branches: Vec<WhenThen>, else_expr: Option<ScalarExpr>) -> Self {
        Self::Ast(TaggedScalarExpr::CaseWhen {
            branches,
            else_expr: else_expr.map(Box::new),
        })
    }

    /// Creates a `COALESCE(...)` expression.
    pub fn coalesce(exprs: Vec<ScalarExpr>) -> Self {
        Self::Ast(TaggedScalarExpr::Coalesce { exprs })
    }
}

/// Structured AST variants for a deterministic row-level boolean predicate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaggedPredicate {
    /// A SQL boolean predicate parsed and validated for determinism against the input schema.
    Sql {
        expression: String,
    },
    Eq {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    NotEq {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    Lt {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    LtEq {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    Gt {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    GtEq {
        left: ScalarExpr,
        right: ScalarExpr,
    },
    IsNull {
        expr: ScalarExpr,
    },
    IsNotNull {
        expr: ScalarExpr,
    },
    InList {
        expr: ScalarExpr,
        list: Vec<ScalarExpr>,
        #[serde(default)]
        negated: bool,
    },
    And {
        predicates: Vec<Predicate>,
    },
    Or {
        predicates: Vec<Predicate>,
    },
    Not {
        predicate: Box<Predicate>,
    },
}

/// Deterministic row-level filter predicate accepted as either a SQL string or a structured AST.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Predicate {
    SqlString(String),
    Ast(TaggedPredicate),
}

impl Predicate {
    /// Creates a SQL boolean predicate.
    pub fn sql(expression: impl Into<String>) -> Self {
        Self::SqlString(expression.into())
    }

    pub fn eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::Eq { left, right })
    }

    pub fn neq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::NotEq { left, right })
    }

    pub fn lt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::Lt { left, right })
    }

    pub fn lte(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::LtEq { left, right })
    }

    pub fn gt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::Gt { left, right })
    }

    pub fn gte(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::GtEq { left, right })
    }

    pub fn is_null(expr: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::IsNull { expr })
    }

    pub fn is_not_null(expr: ScalarExpr) -> Self {
        Self::Ast(TaggedPredicate::IsNotNull { expr })
    }

    pub fn and(predicates: Vec<Predicate>) -> Self {
        Self::Ast(TaggedPredicate::And { predicates })
    }

    pub fn or(predicates: Vec<Predicate>) -> Self {
        Self::Ast(TaggedPredicate::Or { predicates })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: Predicate) -> Self {
        Self::Ast(TaggedPredicate::Not {
            predicate: Box::new(predicate),
        })
    }
}

/// Folds a sparse set of string or string-list columns into a single compact
/// `List<Struct<field: Utf8, values: List<Utf8>>>` column containing only non-empty
/// entries per row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadingsFoldSpec {
    /// Name of the output `List<Struct<field: Utf8, values: List<Utf8>>>` column
    /// (for example, `"filename_readings"`).
    pub target_column: String,
    /// Explicit source columns to fold. When empty, columns matching `include_suffixes`
    /// or `include_prefixes` in the source schema are selected in schema order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Optional column name suffixes to match (for example, `[""_values", "_raw"]`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_suffixes: Vec<String>,
    /// Optional column name prefixes to match.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_prefixes: Vec<String>,
    /// When true, strips a matched suffix in `include_suffixes` from the recorded `field` name.
    #[serde(default)]
    pub strip_suffix: bool,
    /// Whether to omit the folded source columns from the exported parent Parquet file.
    #[serde(default = "default_true")]
    pub drop_source_columns: bool,
}

impl ReadingsFoldSpec {
    /// Returns whether `column_name` is matched by this fold specification.
    pub fn matches_source_column(&self, column_name: &str) -> bool {
        if column_name == self.target_column
            || column_name.to_ascii_lowercase().starts_with("__docspec_")
        {
            return false;
        }
        if !self.columns.is_empty() {
            return self.columns.iter().any(|c| c == column_name);
        }
        let has_prefix = self
            .include_prefixes
            .iter()
            .any(|p| !p.is_empty() && column_name.starts_with(p.as_str()));
        let has_suffix = self
            .include_suffixes
            .iter()
            .any(|s| !s.is_empty() && column_name.ends_with(s.as_str()));
        has_prefix || has_suffix
    }

    /// Formats the `field` label stored in each non-empty reading struct.
    pub fn field_label<'a>(&self, column_name: &'a str) -> &'a str {
        if self.strip_suffix {
            for suffix in &self.include_suffixes {
                if !suffix.is_empty()
                    && let Some(stripped) = column_name.strip_suffix(suffix.as_str())
                    && !stripped.is_empty()
                {
                    return stripped;
                }
            }
        }
        column_name
    }
}

/// Declarative specification for unnesting a `List<Struct>` column into a flat,
/// sorted child Parquet table during [`ExportSpec`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildTableExportSpec {
    /// Name of the source `List<Struct<...>>` column to unnest (e.g. `"source_occurrences"`).
    pub list_column: String,
    /// Parent scalar key column(s) copied into every child row (e.g. `["owner_id"]`).
    pub parent_key_columns: Vec<String>,
    /// Optional alias for the parent key column(s) in the child table (when empty, uses
    /// `parent_key_columns` names as-is).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub child_key_columns: Vec<String>,
    /// Name of the 0-based ordinal column added to each unnested child row (defaults to `"occurrence_order"`).
    #[serde(default = "default_ordinal_column")]
    pub ordinal_column: String,
    /// Optional subset of struct subfield names to include in the child table (when `None`, all
    /// subfields of the `List<Struct>` item are exported in struct order).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub struct_columns: Option<Vec<String>>,
    /// Target rows per row group in the child Parquet file (defaults to `2048`).
    #[serde(default = "default_child_row_group_rows")]
    pub row_group_rows: usize,
    /// Optional scalar column name (`Int64`) added to the parent export recording the
    /// number of elements in `list_column` for each parent row (e.g. `Some("occurrence_count")`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_with_count_column: Option<String>,
    /// Whether to omit `list_column` from the parent exported Parquet file (defaults to `true`).
    #[serde(default = "default_true")]
    pub drop_parent_column: bool,
}

impl ChildTableExportSpec {
    /// Returns the effective key column names in the child table.
    pub fn effective_child_key_columns(&self) -> &[String] {
        if self.child_key_columns.is_empty() {
            &self.parent_key_columns
        } else {
            &self.child_key_columns
        }
    }
}

/// One grouped count row produced by an [`ExportPolicy::facet_sets`] grouping set.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FacetCountEntry {
    /// Grouping column names in this grouping set (in declared order).
    pub grouping: Vec<String>,
    /// Key-value map of column name to stringified scalar value (`None` for SQL `NULL`).
    pub values: BTreeMap<String, Option<String>>,
    /// Exact row count matching this facet tuple in the exported dataset.
    pub count: u64,
}

/// Per-row-group min/max key bounds for exported browse/key columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportKeyRange {
    /// Zero-based row group index within the exported Parquet file.
    pub row_group: usize,
    /// Zero-based starting row offset of this row group in the exported Parquet file.
    pub start_row: u64,
    /// Number of rows in this row group.
    pub num_rows: u64,
    /// Minimum non-null scalar value per tracked key/sort column in this row group.
    pub min_values: BTreeMap<String, String>,
    /// Maximum non-null scalar value per tracked key/sort column in this row group.
    pub max_values: BTreeMap<String, String>,
}

/// Materialized and validated flat child Parquet table produced alongside a parent [`ExportPin`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildTableExport {
    pub object: ObjectRef,
    pub rows: u64,
    pub fields: Vec<ExportField>,
    pub validation: ExportValidation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub browse_ranges: Vec<ExportKeyRange>,
}

/// Authorizes named columns from active rows of one or more retained field groups.
///
/// The local format supports primitive, list, and struct columns, optional multi-group
/// occurrence joins, deterministic row filters, computed/redacted scalar columns,
/// sparse column-group folding (`fold_readings`), global sorting with selective scalar
/// `PageIndex`, `GROUPING SETS` facet summaries, browse key ranges, and unnested child
/// column-group tables (`child_tables`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPolicy {
    #[serde(default = "default_group")]
    pub group: String,
    /// Optional list of field groups (`"source"`, computed `BackfillSpec` groups, and/or
    /// `"evidence"` / `"receipts"`) joined by occurrence identity before projection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    /// Caller-supplied row authorization; the engine never infers restrictions.
    pub rows: ExportRows,
    /// Exact names in the requested output order; an empty list is refused.
    pub columns: Vec<String>,
    /// Optional deterministic row filter applied before projection and sorting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    /// Optional deterministic scalar expressions replacing or adding named columns.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub computed_columns: BTreeMap<String, ScalarExpr>,
    /// Optional folding of sparse string/list columns into a compact `List<Struct<field, values>>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fold_readings: Option<ReadingsFoldSpec>,
    /// Optional global sort order applied to the exported rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<SortColumn>,
    /// Scalar columns on which to enable Parquet `PageIndex` (`ColumnIndex` + `OffsetIndex`).
    /// When empty and `order_by` is non-empty, scalar `order_by` columns automatically receive a `PageIndex`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub page_index_columns: Vec<String>,
    /// Optional `GROUPING SETS` column lists whose exact counts are recorded in [`ExportPin::facets`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facet_sets: Vec<Vec<String>>,
    /// Optional scalar columns whose per-row-group min/max bounds are recorded in [`ExportPin::browse_ranges`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub browse_range_columns: Vec<String>,
    /// Optional unnested flat child Parquet tables (such as `"occurrences"` from `source_occurrences`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub child_tables: BTreeMap<String, ChildTableExportSpec>,
    /// Optional target rows per row group for the parent Parquet file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_rows: Option<usize>,
}

impl Default for ExportPolicy {
    fn default() -> Self {
        Self {
            group: default_group(),
            groups: Vec::new(),
            rows: ExportRows::All,
            columns: Vec::new(),
            filter: None,
            computed_columns: BTreeMap::new(),
            fold_readings: None,
            order_by: Vec::new(),
            page_index_columns: Vec::new(),
            facet_sets: Vec::new(),
            browse_range_columns: Vec::new(),
            child_tables: BTreeMap::new(),
            row_group_rows: None,
        }
    }
}

impl ExportPolicy {
    /// Returns the ordered list of field groups to read and join.
    pub fn effective_groups(&self) -> Vec<String> {
        if self.groups.is_empty() {
            vec![self.group.clone()]
        } else {
            self.groups.clone()
        }
    }

    /// Returns the primary field group name.
    pub fn primary_group(&self) -> &str {
        self.groups.first().map_or(self.group.as_str(), String::as_str)
    }

    /// Returns whether this policy uses any extended multi-group, relational, or child-table features.
    pub fn has_extended_features(&self) -> bool {
        !self.groups.is_empty()
            || self.filter.is_some()
            || !self.computed_columns.is_empty()
            || self.fold_readings.is_some()
            || !self.order_by.is_empty()
            || !self.page_index_columns.is_empty()
            || !self.facet_sets.is_empty()
            || !self.browse_range_columns.is_empty()
            || !self.child_tables.is_empty()
            || self.row_group_rows.is_some()
    }

    /// Computes the exact expected parent output column names in order after applying
    /// `computed_columns`, `fold_readings`, and `child_tables` replacements/drops.
    pub fn expected_output_columns(&self) -> Vec<String> {
        let mut cols = self.columns.clone();
        if let Some(fold) = &self.fold_readings {
            if fold.drop_source_columns {
                cols.retain(|c| c == &fold.target_column || !fold.matches_source_column(c));
            }
            if !cols.iter().any(|c| c == &fold.target_column) {
                cols.push(fold.target_column.clone());
            }
        }
        for name in self.computed_columns.keys() {
            if !cols.iter().any(|c| c == name) {
                cols.push(name.clone());
            }
        }
        for child in self.child_tables.values() {
            if let Some(count_col) = &child.replace_with_count_column {
                if child.drop_parent_column {
                    if let Some(pos) = cols.iter().position(|c| c == &child.list_column) {
                        if !cols.iter().any(|c| c == count_col) {
                            cols[pos] = count_col.clone();
                        } else {
                            cols.remove(pos);
                        }
                    } else if !cols.iter().any(|c| c == count_col) {
                        cols.push(count_col.clone());
                    }
                } else if !cols.iter().any(|c| c == count_col) {
                    cols.push(count_col.clone());
                }
            } else if child.drop_parent_column {
                cols.retain(|c| c != &child.list_column);
            }
        }
        // Deduplicate while preserving first-seen order.
        let mut seen = BTreeSet::new();
        cols.into_iter().filter(|c| seen.insert(c.clone())).collect()
    }
}

/// Creates and selects one authorized export using a stable retry identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub source: VersionRef,
    pub channel: Id,
    /// Exact export currently selected by the channel, or an unoccupied channel.
    pub expected: Option<Id>,
    pub policy: ExportPolicy,
}

/// Records the retained native identity and physical interpretation of one column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportField {
    pub id: i32,
    pub name: String,
    /// Arrow's precise primitive type description, including units and precision.
    pub data_type: String,
    pub nullable: bool,
}

/// Retains creation checks so retries need not reopen the output payload.
///
/// The writer hashes bytes as it writes. Validation independently checks the
/// file size, readable footer, field identities/types, row count, and a bounded
/// decoded sample. These checks do not establish full independent reconstruction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportValidation {
    pub object: ObjectRef,
    pub rows: u64,
    pub sampled_rows: usize,
    pub sampled_decoded_bytes: usize,
}

/// Identifies an independently readable file selected through the dataset catalog.
///
/// Resolve a channel once and retain this pin while reading its immutable file.
/// Host staging stays protected until verified remote completion permits explicit cleanup.
/// Neither placement establishes anonymous public hosting or application evidence access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPin {
    pub format: u32,
    pub id: Id,
    pub dataset: Id,
    pub source: VersionRef,
    /// Native lifecycle revision accepting the exact source release.
    pub source_revision: i64,
    pub policy: ExportPolicy,
    pub object: ObjectRef,
    pub rows: u64,
    pub fields: Vec<ExportField>,
    pub validation: ExportValidation,
    /// Grouped row counts computed when [`ExportPolicy::facet_sets`] is non-empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facets: Vec<FacetCountEntry>,
    /// Per-row-group key min/max bounds for browse and range pruning.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub browse_ranges: Vec<ExportKeyRange>,
    /// Unnested child Parquet tables keyed by their declared name in [`ExportPolicy::child_tables`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub child_tables: BTreeMap<String, ChildTableExport>,
}
