//! Versioned identities and lifecycle records; no storage or execution dependencies.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A validated identifier, safe in catalog names and workspace paths.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Id(String);

impl Id {
    /// Validates a bounded ASCII identifier.
    ///
    /// # Errors
    /// Refuses empty, long, traversal, or non-identifier values.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Error::refusal(
                "invalid_id",
                "expected 1–128 ASCII letters, digits, underscores or hyphens",
            ));
        }
        Ok(Self(value))
    }
    /// Returns the exact identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Stable retry identity; conflicting requests cannot reuse an operation.
pub type OperationId = Id;

/// An explicit decision to stop an operation while retaining its completed work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Abandonment {
    pub dataset: Id,
    pub operation: OperationId,
    pub request_digest: String,
    /// Revision of the original admission, not the abandonment commit.
    pub operation_revision: i64,
    pub reason: String,
}

/// An immutable object bound to its protection owner and exact bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub owner: Id,
    pub path: String,
    pub digest: String,
    pub bytes: u64,
}

/// An independently readable, exact native snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePin {
    pub table: String,
    pub metadata: ObjectRef,
    pub snapshot: Option<i64>,
    pub schema_id: i32,
    pub objects: Vec<ObjectRef>,
    pub rows: u64,
    /// Original logical state when this pin is a verified equivalent representation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_metadata: Option<ObjectRef>,
}

impl TablePin {
    /// Returns the original metadata identity of this unchanged logical state.
    pub fn logical_metadata(&self) -> &ObjectRef {
        self.logical_metadata.as_ref().unwrap_or(&self.metadata)
    }
}

/// Rewrites selected live files while preserving their logical group state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionSpec {
    pub dataset: Id,
    pub operation: Id,
    pub version: Id,
    pub group: String,
    /// Empty selects every live data file; supplied paths must be distinct and current.
    #[serde(default)]
    pub files: Vec<String>,
    pub target_file_bytes: usize,
}

/// Exact equivalent physical representation and measured rewrite sizes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionResult {
    pub spec: CompactionSpec,
    pub source: TablePin,
    pub replacement: TablePin,
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub rows: u64,
    pub verified: bool,
}

/// Exact native interpretation without copying the table's physical object inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSnapshotRef {
    pub metadata: ObjectRef,
    pub snapshot: Option<i64>,
    pub schema_id: i32,
}

impl From<&TablePin> for NativeSnapshotRef {
    fn from(table: &TablePin) -> Self {
        Self {
            metadata: table.metadata.clone(),
            snapshot: table.snapshot,
            schema_id: table.schema_id,
        }
    }
}

/// A complete data, schema, evidence, and input-coverage selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPin {
    pub format: u32,
    pub dataset: Id,
    pub version: Id,
    pub groups: BTreeMap<String, TablePin>,
    pub computed: BTreeMap<String, ComputedGroup>,
    pub inputs: BTreeMap<String, ObjectRef>,
    /// Producer-declared relationship retained with this exact version.
    pub relationship: SourceRelationship,
    /// Exact related inputs and contributor relation establishing this source table.
    pub related: Option<Box<RelatedResult>>,
    /// Opaque receipt values associated with exact surviving source occurrences.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<Box<crate::evidence::EvidenceResult>>,
    pub definition: String,
    pub eligibility: u64,
    pub boundary: u64,
    pub request: OperationRequest,
    /// Exact selected combination replaced or extended, including concurrent sibling groups.
    pub parent_manifest: Option<ObjectRef>,
    pub processor: String,
    /// Validation scope completed before selection. Older releases meet at least routine scope.
    #[serde(default)]
    pub validation: VerificationLevel,
    /// Active partition layout for the source group, empty when unpartitioned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<PartitionFieldSpec>,
}

/// An immutable consumer release selected through the lifecycle table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePin {
    pub id: Id,
    pub dataset: DatasetPin,
    pub manifest: ObjectRef,
    /// Routine validation/readback completed; this does not certify every payload value.
    pub verified: bool,
}

/// The amount of payload work requested by an independent verifier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationLevel {
    /// Check references, schemas, sizes, footer counts and a bounded row sample.
    #[default]
    Routine,
    /// Also hash payloads and reconstruct complete derived values and evidence.
    Deep,
}

/// Reports the scope of completed checks; routine counts come from metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub level: VerificationLevel,
    pub tables_checked: usize,
    /// Unique files whose footers were inspected; not collected by the deep verifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_files_checked: Option<usize>,
    pub rows_from_metadata: u64,
    pub sampled_files: usize,
    pub sampled_rows: usize,
    pub sampled_decoded_bytes: usize,
}

/// Exact identity of one accepted release, without embedding its payload inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRef {
    pub dataset: Id,
    pub version: Id,
    pub manifest: ObjectRef,
}

/// Producer-declared admissibility of a version alongside related datasets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceRelationship {
    /// This version imposes no cross-dataset generation requirement.
    #[default]
    Independent,
    /// Consumed members of this family must match one sealed generation.
    Coupled { family: Id, generation: Id },
}

/// Create-once producer selection of exact members belonging to one generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationSeal {
    pub family: Id,
    pub generation: Id,
    pub members: BTreeMap<Id, VersionRef>,
}

/// Exact consumed versions and the producer seals required to combine them.
///
/// Retaining this metadata permits independent relationship checks. It does not
/// promise payload availability or certify current eligibility after capture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputCoverage {
    pub inputs: BTreeMap<Id, VersionRef>,
    /// Immutable member set established by each family's first seal.
    pub families: BTreeMap<Id, std::collections::BTreeSet<Id>>,
    pub generations: BTreeMap<Id, GenerationSeal>,
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

/// Scalar literal values used in declarative predicates and projections.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
}

impl PartialEq for LiteralValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for LiteralValue {}

impl From<bool> for LiteralValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for LiteralValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<i32> for LiteralValue {
    fn from(value: i32) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<f64> for LiteralValue {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<&str> for LiteralValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for LiteralValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<'de> Deserialize<'de> for LiteralValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(d)?;
        Self::from_json_value(value).map_err(serde::de::Error::custom)
    }
}

impl LiteralValue {
    fn from_json_value(value: serde_json::Value) -> std::result::Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::Null),
            serde_json::Value::Bool(b) => Ok(Self::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Self::Int64(i))
                } else if let Some(u) = n.as_u64() {
                    i64::try_from(u)
                        .map(Self::Int64)
                        .map_err(|_| format!("integer literal out of i64 range: {u}"))
                } else if let Some(f) = n.as_f64() {
                    Ok(Self::Float64(f))
                } else {
                    Err(format!("unsupported numeric literal: {n}"))
                }
            }
            serde_json::Value::String(s) => Ok(Self::String(s)),
            serde_json::Value::Object(mut map) => {
                let Some(kind) = map.remove("kind").and_then(|v| v.as_str().map(str::to_owned))
                else {
                    return Err("literal object must specify a string 'kind'".into());
                };
                let val = map.remove("value").unwrap_or(serde_json::Value::Null);
                if !map.is_empty() {
                    return Err(format!(
                        "unknown field(s) in literal object: {}",
                        map.keys().cloned().collect::<Vec<_>>().join(", ")
                    ));
                }
                match kind.as_str() {
                    "null" => Ok(Self::Null),
                    "bool" | "boolean" => val
                        .as_bool()
                        .map(Self::Bool)
                        .ok_or_else(|| "expected boolean literal 'value'".into()),
                    "int" | "int64" | "long" | "integer" => val
                        .as_i64()
                        .map(Self::Int64)
                        .ok_or_else(|| "expected integer literal 'value'".into()),
                    "float" | "float64" | "double" => val
                        .as_f64()
                        .map(Self::Float64)
                        .ok_or_else(|| "expected float literal 'value'".into()),
                    "string" | "utf8" => val
                        .as_str()
                        .map(|s| Self::String(s.to_owned()))
                        .ok_or_else(|| "expected string literal 'value'".into()),
                    other => Err(format!("unknown literal kind '{other}'")),
                }
            }
            serde_json::Value::Array(_) => Err("array literals are not supported".into()),
        }
    }
}

/// Binary scalar operator in declarative projection expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    #[serde(alias = "+")]
    Add,
    #[serde(alias = "sub", alias = "-")]
    Subtract,
    #[serde(alias = "mul", alias = "*")]
    Multiply,
    #[serde(alias = "div", alias = "/")]
    Divide,
    #[serde(alias = "mod", alias = "%")]
    Modulo,
    #[serde(alias = "string_concat", alias = "||")]
    Concat,
}

/// Comparison operator for declarative filter predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    #[serde(alias = "=")]
    Eq,
    #[serde(alias = "ne", alias = "neq", alias = "!=", alias = "<>")]
    NotEq,
    #[serde(alias = "<")]
    Lt,
    #[serde(alias = "le", alias = "lte", alias = "<=")]
    LtEq,
    #[serde(alias = ">")]
    Gt,
    #[serde(alias = "ge", alias = "gte", alias = ">=")]
    GtEq,
}

/// One `WHEN ... THEN ...` branch of a declarative `CASE` expression.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseWhen {
    pub when: Predicate,
    pub then: ScalarExpr,
}

/// Declarative row-local scalar expression for computed projections and filter operands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScalarExpr {
    Column {
        field: String,
    },
    Literal {
        value: LiteralValue,
    },
    Coalesce {
        exprs: Vec<ScalarExpr>,
    },
    Binary {
        left: Box<ScalarExpr>,
        op: BinaryOp,
        right: Box<ScalarExpr>,
    },
    Case {
        when_then: Vec<CaseWhen>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        else_expr: Option<Box<ScalarExpr>>,
    },
    Cast {
        expr: Box<ScalarExpr>,
        data_type: String,
    },
    Predicate {
        predicate: Box<Predicate>,
    },
    Sql {
        expr: String,
    },
}

impl ScalarExpr {
    /// Creates a column reference expression.
    pub fn column(field: impl Into<String>) -> Self {
        Self::Column {
            field: field.into(),
        }
    }

    /// Shorthand for [`Self::column`].
    pub fn col(field: impl Into<String>) -> Self {
        Self::column(field)
    }

    /// Creates a literal value expression.
    pub fn literal(value: impl Into<LiteralValue>) -> Self {
        Self::Literal {
            value: value.into(),
        }
    }

    /// Shorthand for [`Self::literal`].
    pub fn lit(value: impl Into<LiteralValue>) -> Self {
        Self::literal(value)
    }

    /// Creates a typed `NULL` literal expression.
    pub fn null() -> Self {
        Self::Literal {
            value: LiteralValue::Null,
        }
    }

    /// Creates a `COALESCE(...)` expression.
    pub fn coalesce(exprs: Vec<ScalarExpr>) -> Self {
        Self::Coalesce { exprs }
    }

    /// Creates a binary operator expression.
    pub fn binary(left: ScalarExpr, op: BinaryOp, right: ScalarExpr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Creates a `CASE WHEN ... THEN ... ELSE ... END` expression.
    pub fn case(when_then: Vec<CaseWhen>, else_expr: Option<ScalarExpr>) -> Self {
        Self::Case {
            when_then,
            else_expr: else_expr.map(Box::new),
        }
    }

    /// Creates a type cast expression.
    pub fn cast(expr: ScalarExpr, data_type: impl Into<String>) -> Self {
        Self::Cast {
            expr: Box::new(expr),
            data_type: data_type.into(),
        }
    }

    /// Creates a SQL scalar expression.
    pub fn sql(expr: impl Into<String>) -> Self {
        Self::Sql { expr: expr.into() }
    }
}

impl<'de> Deserialize<'de> for ScalarExpr {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum TaggedScalarExpr {
            #[serde(alias = "col")]
            Column {
                #[serde(alias = "column", alias = "name")]
                field: String,
            },
            #[serde(alias = "lit")]
            Literal {
                #[serde(default = "default_null_literal")]
                value: LiteralValue,
            },
            Coalesce {
                #[serde(alias = "args", alias = "values")]
                exprs: Vec<ScalarExpr>,
            },
            Binary {
                left: Box<ScalarExpr>,
                op: BinaryOp,
                right: Box<ScalarExpr>,
            },
            Case {
                #[serde(alias = "branches", alias = "cases")]
                when_then: Vec<CaseWhen>,
                #[serde(
                    default,
                    alias = "else",
                    alias = "otherwise"
                )]
                else_expr: Option<Box<ScalarExpr>>,
            },
            Cast {
                expr: Box<ScalarExpr>,
                #[serde(alias = "target_type", alias = "to")]
                data_type: String,
            },
            Predicate {
                predicate: Box<Predicate>,
            },
            Sql {
                #[serde(alias = "sql", alias = "expression")]
                expr: String,
            },
        }

        fn default_null_literal() -> LiteralValue {
            LiteralValue::Null
        }

        let raw = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(s) = raw {
            return Ok(Self::Sql { expr: s });
        }
        let tagged: TaggedScalarExpr =
            serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        Ok(match tagged {
            TaggedScalarExpr::Column { field } => Self::Column { field },
            TaggedScalarExpr::Literal { value } => Self::Literal { value },
            TaggedScalarExpr::Coalesce { exprs } => Self::Coalesce { exprs },
            TaggedScalarExpr::Binary { left, op, right } => Self::Binary { left, op, right },
            TaggedScalarExpr::Case {
                when_then,
                else_expr,
            } => Self::Case {
                when_then,
                else_expr,
            },
            TaggedScalarExpr::Cast { expr, data_type } => Self::Cast { expr, data_type },
            TaggedScalarExpr::Predicate { predicate } => Self::Predicate { predicate },
            TaggedScalarExpr::Sql { expr } => Self::Sql { expr },
        })
    }
}

/// Declarative boolean predicate for filtering join or aggregation rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    Comparison {
        left: ScalarExpr,
        op: ComparisonOp,
        right: ScalarExpr,
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
        exprs: Vec<Predicate>,
    },
    Or {
        exprs: Vec<Predicate>,
    },
    Not {
        expr: Box<Predicate>,
    },
    Sql {
        expr: String,
    },
}

impl Predicate {
    /// Creates an equality predicate (`left = right`).
    pub fn eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Eq { left, right }
    }

    /// Creates an inequality predicate (`left != right`).
    pub fn not_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::NotEq { left, right }
    }

    /// Creates a less-than predicate (`left < right`).
    pub fn lt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Lt { left, right }
    }

    /// Creates a less-than-or-equal predicate (`left <= right`).
    pub fn lt_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::LtEq { left, right }
    }

    /// Creates a greater-than predicate (`left > right`).
    pub fn gt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Gt { left, right }
    }

    /// Creates a greater-than-or-equal predicate (`left >= right`).
    pub fn gt_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::GtEq { left, right }
    }

    /// Creates an `IS NULL` predicate.
    pub fn is_null(expr: ScalarExpr) -> Self {
        Self::IsNull { expr }
    }

    /// Creates an `IS NOT NULL` predicate.
    pub fn is_not_null(expr: ScalarExpr) -> Self {
        Self::IsNotNull { expr }
    }

    /// Creates an `IN (...)` predicate.
    pub fn in_list(expr: ScalarExpr, list: Vec<ScalarExpr>) -> Self {
        Self::InList {
            expr,
            list,
            negated: false,
        }
    }

    /// Creates a `NOT IN (...)` predicate.
    pub fn not_in_list(expr: ScalarExpr, list: Vec<ScalarExpr>) -> Self {
        Self::InList {
            expr,
            list,
            negated: true,
        }
    }

    /// Creates a conjunction (`AND`) of predicates.
    pub fn and(exprs: Vec<Predicate>) -> Self {
        Self::And { exprs }
    }

    /// Creates a disjunction (`OR`) of predicates.
    pub fn or(exprs: Vec<Predicate>) -> Self {
        Self::Or { exprs }
    }

    /// Negates a predicate (`NOT`).
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Predicate) -> Self {
        Self::Not {
            expr: Box::new(expr),
        }
    }

    /// Creates a SQL boolean predicate expression.
    pub fn sql(expr: impl Into<String>) -> Self {
        Self::Sql { expr: expr.into() }
    }
}

impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct BinaryEndpoints {
            #[serde(default)]
            left: Option<ScalarExpr>,
            #[serde(default)]
            right: Option<ScalarExpr>,
            #[serde(default, alias = "column")]
            field: Option<String>,
            #[serde(default)]
            value: Option<LiteralValue>,
            #[serde(default)]
            op: Option<ComparisonOp>,
        }

        impl BinaryEndpoints {
            fn into_pair(self) -> std::result::Result<(ScalarExpr, ScalarExpr), String> {
                let left = match (self.left, self.field) {
                    (Some(l), None) => l,
                    (None, Some(f)) => ScalarExpr::Column { field: f },
                    (Some(_), Some(_)) => {
                        return Err("specify either 'left' or 'field', not both".into());
                    }
                    (None, None) => return Err("missing 'left' (or 'field') in predicate".into()),
                };
                let right = match (self.right, self.value) {
                    (Some(r), None) => r,
                    (None, Some(v)) => ScalarExpr::Literal { value: v },
                    (Some(_), Some(_)) => {
                        return Err("specify either 'right' or 'value', not both".into());
                    }
                    (None, None) => {
                        return Err("missing 'right' (or 'value') in predicate".into());
                    }
                };
                Ok((left, right))
            }
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UnaryEndpoint {
            #[serde(default)]
            expr: Option<ScalarExpr>,
            #[serde(default, alias = "column")]
            field: Option<String>,
        }

        impl UnaryEndpoint {
            fn into_expr(self) -> std::result::Result<ScalarExpr, String> {
                match (self.expr, self.field) {
                    (Some(e), None) => Ok(e),
                    (None, Some(f)) => Ok(ScalarExpr::Column { field: f }),
                    (Some(_), Some(_)) => Err("specify either 'expr' or 'field', not both".into()),
                    (None, None) => Err("missing 'expr' (or 'field') in predicate".into()),
                }
            }
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct InListPayload {
            #[serde(default)]
            expr: Option<ScalarExpr>,
            #[serde(default, alias = "column")]
            field: Option<String>,
            #[serde(default)]
            list: Option<Vec<ScalarExpr>>,
            #[serde(default)]
            values: Option<Vec<LiteralValue>>,
            #[serde(default)]
            negated: bool,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LogicalPayload {
            #[serde(default, alias = "predicates", alias = "clauses")]
            exprs: Option<Vec<Predicate>>,
            #[serde(default)]
            left: Option<Box<Predicate>>,
            #[serde(default)]
            right: Option<Box<Predicate>>,
        }

        impl LogicalPayload {
            fn into_vec(self) -> std::result::Result<Vec<Predicate>, String> {
                match (self.exprs, self.left, self.right) {
                    (Some(v), None, None) => Ok(v),
                    (None, Some(l), Some(r)) => Ok(vec![*l, *r]),
                    _ => Err("logical predicate requires either 'exprs' or ('left' and 'right')".into()),
                }
            }
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct NotPayload {
            #[serde(alias = "predicate")]
            expr: Box<Predicate>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SqlPayload {
            #[serde(alias = "sql", alias = "expression")]
            expr: String,
        }

        let raw = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(s) = raw {
            return Ok(Self::Sql { expr: s });
        }
        let serde_json::Value::Object(mut map) = raw else {
            return Err(serde::de::Error::custom(
                "expected predicate object or SQL string",
            ));
        };
        let Some(kind) = map.remove("kind").and_then(|v| v.as_str().map(str::to_owned)) else {
            return Err(serde::de::Error::custom("predicate object missing 'kind'"));
        };
        let rest = serde_json::Value::Object(map);
        match kind.as_str() {
            "comparison" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let op = payload
                    .op
                    .ok_or_else(|| serde::de::Error::custom("comparison predicate missing 'op'"))?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::Comparison { left, op, right })
            }
            "eq" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::Eq { left, right })
            }
            "not_eq" | "ne" | "neq" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::NotEq { left, right })
            }
            "lt" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::Lt { left, right })
            }
            "lt_eq" | "le" | "lte" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::LtEq { left, right })
            }
            "gt" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::Gt { left, right })
            }
            "gt_eq" | "ge" | "gte" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(Self::GtEq { left, right })
            }
            "is_null" => {
                let payload: UnaryEndpoint =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let expr = payload.into_expr().map_err(serde::de::Error::custom)?;
                Ok(Self::IsNull { expr })
            }
            "is_not_null" => {
                let payload: UnaryEndpoint =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let expr = payload.into_expr().map_err(serde::de::Error::custom)?;
                Ok(Self::IsNotNull { expr })
            }
            "in_list" | "in" | "not_in_list" | "not_in" => {
                let negated_by_kind = matches!(kind.as_str(), "not_in_list" | "not_in");
                let payload: InListPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let expr = UnaryEndpoint {
                    expr: payload.expr,
                    field: payload.field,
                }
                .into_expr()
                .map_err(serde::de::Error::custom)?;
                let list = match (payload.list, payload.values) {
                    (Some(l), None) => l,
                    (None, Some(vals)) => vals
                        .into_iter()
                        .map(|value| ScalarExpr::Literal { value })
                        .collect(),
                    (Some(_), Some(_)) => {
                        return Err(serde::de::Error::custom(
                            "specify either 'list' or 'values' in in_list predicate",
                        ));
                    }
                    (None, None) => {
                        return Err(serde::de::Error::custom(
                            "missing 'list' (or 'values') in in_list predicate",
                        ));
                    }
                };
                Ok(Self::InList {
                    expr,
                    list,
                    negated: payload.negated || negated_by_kind,
                })
            }
            "and" => {
                let payload: LogicalPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let exprs = payload.into_vec().map_err(serde::de::Error::custom)?;
                Ok(Self::And { exprs })
            }
            "or" => {
                let payload: LogicalPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let exprs = payload.into_vec().map_err(serde::de::Error::custom)?;
                Ok(Self::Or { exprs })
            }
            "not" => {
                let payload: NotPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                Ok(Self::Not { expr: payload.expr })
            }
            "sql" => {
                let payload: SqlPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                Ok(Self::Sql { expr: payload.expr })
            }
            other => Err(serde::de::Error::custom(format!(
                "unknown predicate kind '{other}'"
            ))),
        }
    }
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
        match (&self.join.filter, &self.filter) {
            (None, None) => None,
            (Some(p), None) | (None, Some(p)) => Some(p.clone()),
            (Some(a), Some(b)) => Some(Predicate::And {
                exprs: vec![a.clone(), b.clone()],
            }),
        }
    }

    /// Returns the merged scalar projections from `join.projections` and `self.projections`.
    pub fn effective_projections(&self) -> BTreeMap<String, ScalarExpr> {
        let mut out = self.join.projections.clone();
        for (k, v) in &self.projections {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

/// Sort direction for ordered aggregate selection.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Null placement for ordered aggregate selection.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NullOrdering {
    NullsFirst,
    #[default]
    NullsLast,
}

/// One ordering column for deterministic aggregate representative selection.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SortColumn {
    pub field: String,
    #[serde(default)]
    pub direction: SortDirection,
    #[serde(default)]
    pub nulls: NullOrdering,
}

impl SortColumn {
    /// Ascending sort order with `NULLS LAST`.
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsLast,
        }
    }

    /// Descending sort order with `NULLS LAST`.
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }
    }
}

impl<'de> Deserialize<'de> for SortColumn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawSortColumn {
            #[serde(alias = "column")]
            field: String,
            #[serde(default)]
            direction: Option<SortDirection>,
            #[serde(default)]
            nulls: Option<NullOrdering>,
            #[serde(default)]
            descending: Option<bool>,
            #[serde(default)]
            nulls_first: Option<bool>,
        }

        let raw = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(field) = raw {
            return Ok(Self::asc(field));
        }
        let parsed: RawSortColumn =
            serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        let direction = match (parsed.direction, parsed.descending) {
            (Some(dir), None) => dir,
            (None, Some(true)) => SortDirection::Desc,
            (None, Some(false) | None) => SortDirection::Asc,
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "specify either 'direction' or 'descending', not both",
                ));
            }
        };
        let nulls = match (parsed.nulls, parsed.nulls_first) {
            (Some(n), None) => n,
            (None, Some(true)) => NullOrdering::NullsFirst,
            (None, Some(false) | None) => NullOrdering::NullsLast,
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "specify either 'nulls' or 'nulls_first', not both",
                ));
            }
        };
        Ok(Self {
            field: parsed.field,
            direction,
            nulls,
        })
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
        match (&self.grouping.filter, &self.filter) {
            (None, None) => None,
            (Some(p), None) | (None, Some(p)) => Some(p.clone()),
            (Some(a), Some(b)) => Some(Predicate::And {
                exprs: vec![a.clone(), b.clone()],
            }),
        }
    }

    /// Returns the merged scalar projections from `grouping.projections` and `self.projections`.
    pub fn effective_projections(&self) -> BTreeMap<String, ScalarExpr> {
        let mut out = self.grouping.projections.clone();
        for (k, v) in &self.projections {
            out.insert(k.clone(), v.clone());
        }
        out
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

impl ReleasePin {
    /// Returns the exact identity used for accepted-history checks.
    pub fn version_ref(&self) -> VersionRef {
        VersionRef {
            dataset: self.dataset.dataset.clone(),
            version: self.id.clone(),
            manifest: self.manifest.clone(),
        }
    }
}

/// Accepted predecessor, committed atomically with the selected result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Succession {
    pub selected: VersionRef,
    pub predecessor: Option<VersionRef>,
    /// Descriptive source admission boundary; ordering comes from predecessor links.
    pub boundary: u64,
    pub eligibility: u64,
}

/// Evidence that target coverage follows prior coverage on the observed selected chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessionProof {
    pub prior: VersionRef,
    pub target: VersionRef,
    pub observed_head: VersionRef,
    pub observed_revision: i64,
    /// Accepted selections between prior and target, including composition-only changes.
    pub selections_advanced: usize,
    /// Accepted selections after target at this observation, not elapsed time or acquisition lag.
    pub selections_behind: usize,
}

/// An expensive completed phase retained independently of its parent operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub operation: OperationId,
    pub phase: String,
    pub request_digest: String,
    pub eligibility: u64,
    pub outputs: Vec<ObjectRef>,
    pub table: Option<TablePin>,
    pub rows: u64,
    pub changes: Option<ChangePin>,
    pub coverage: Option<InputCoverage>,
    pub metrics: WriteMetrics,
    /// Exact retained dependencies used by this completion. Absence marks an older
    /// record whose recovery dependencies cannot authorize cleanup by inference.
    #[serde(default)]
    pub dependencies: Option<PhaseDependencies>,
}

/// Small immutable references needed to recover or reuse a completed phase.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseDependencies {
    /// Equivalent related-input sources actually consumed by this completion.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_sources: BTreeMap<Id, TablePin>,
    /// Input names before the operation-specific release prefix is applied.
    pub admitted_inputs: BTreeMap<String, ObjectRef>,
    /// Selected schema/history consumed while preparing this phase.
    pub base_manifest: Option<ObjectRef>,
    /// Metadata consumed for schema/default interpretation, without its old data rows.
    pub schema_metadata: Vec<ObjectRef>,
    /// Native tables consumed by later processing or validation.
    pub tables: Vec<NativeSnapshotRef>,
}

/// Mutable eligibility and coverage facts checked by the same lifecycle commit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub definition: String,
    pub eligibility: u64,
    pub epoch: u64,
    pub boundary: u64,
    pub selected: Option<DatasetPin>,
    pub release: Option<ReleasePin>,
    /// Native revision that selected this release, preserved across later admissions.
    pub selection_revision: Option<i64>,
}

/// Objects protected by an owner before a consumer can promise their availability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Protection {
    pub id: Id,
    pub consumer: Id,
    pub purpose: String,
    pub paths: Vec<String>,
    pub retired: bool,
}

/// Exact lifecycle snapshot observed during a read-only protection inspection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionBoundary {
    pub table: String,
    pub metadata: String,
    pub revision: i64,
}

/// One exact native generation of a retained lifecycle record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedGeneration {
    pub key: String,
    pub revision: i64,
}

/// The independent availability promise ended by a retirement decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetirementScope {
    PhaseRecovery,
    OptionalChanges,
    ResultAvailability {
        operation: OperationId,
    },
    OperationRecovery {
        operation: OperationId,
    },
    CompactionRecovery {
        operation: OperationId,
    },
    /// Recovery of one export operation, separate from immutable pin availability.
    ExportRecovery {
        operation: OperationId,
    },
    /// Standalone availability of one immutable producer generation seal.
    GenerationSeal {
        generation: Id,
    },
    OwnerGrants,
    RestorePoints,
}

/// Explicit retirement of retained promises, without deleting physical objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPlan {
    pub dataset: Id,
    pub retirement: Id,
    pub scope: RetirementScope,
    pub reason: String,
    pub table_uuid: String,
    pub metadata: String,
    pub snapshot_id: i64,
    pub revision: i64,
    pub records: Vec<RetainedGeneration>,
}

/// A durable retirement decision and its native lifecycle revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retirement {
    pub plan: RetentionPlan,
    pub revision: i64,
}

/// One retained record sufficient to protect a candidate object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionBlocker {
    pub table: String,
    pub key: String,
    pub revision: i64,
}

/// A candidate's first protection witness, not an exhaustive list of owners.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectProtection {
    pub object: ObjectRef,
    pub blocker: Option<RetentionBlocker>,
}

/// Read-only protection evidence; unmatched objects are not authorized for removal.
///
/// Each table has its own observed boundary. Concurrent work can add references
/// during or after inspection. Removal requires fresh eligibility, worker
/// exclusion and a durable exact intent, none of which this report supplies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInspection {
    pub boundaries: Vec<RetentionBoundary>,
    pub objects: Vec<ObjectProtection>,
    pub protected_bytes: u64,
    pub unmatched_bytes: u64,
    /// Unique release or native metadata files decoded, excluding lifecycle rows.
    pub metadata_objects_read: usize,
}

/// One exact owned native registration, detached without purging its files.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalRegistration {
    pub namespace: String,
    pub name: String,
    pub table_uuid: String,
    pub metadata: String,
}

/// Exact supported authority for a removal plan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemovalScope {
    /// Owned embedded-catalog inventory and native registrations.
    #[default]
    Objects,
    /// One original host stage whose verified remote completion is durable.
    ExportHostStage {
        operation: Id,
        operation_revision: i64,
        stage_revision: i64,
        completion_revision: i64,
        host_root: String,
    },
    /// One exactly created host child after terminal export recovery retirement.
    ExportHostFile {
        operation: Id,
        operation_revision: i64,
        file: Id,
        creation_revision: i64,
        host_root: String,
    },
}

impl RemovalScope {
    fn is_objects(&self) -> bool {
        matches!(self, Self::Objects)
    }
}

/// Bounded explicit removal candidates and their observed protection boundaries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalPlan {
    #[serde(default, skip_serializing_if = "RemovalScope::is_objects")]
    pub scope: RemovalScope,
    pub dataset: Id,
    pub removal: Id,
    pub reason: String,
    pub table_uuid: String,
    pub inspection: RetentionInspection,
    pub registrations: Vec<RemovalRegistration>,
}

/// Immutable removal scope chosen from a preview by fresh eligibility checks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalIntent {
    pub plan: RemovalPlan,
    pub revision: i64,
    /// Fresh inventory boundaries captured before reserving effects.
    pub boundaries: Vec<RetentionBoundary>,
    /// Originally eligible objects excluded by new protections.
    pub blocked: Vec<ObjectProtection>,
    /// Indices into the preview's candidates; no later attempt may expand these.
    pub objects: Vec<usize>,
    /// Indices into the preview's exact native registrations.
    pub registrations: Vec<usize>,
}

/// A durable effect acknowledgment; unresolved or failed effects may already have occurred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemovalOutcome {
    Unresolved,
    Removed,
    Absent,
    Failed { code: String },
}

impl RemovalOutcome {
    pub(crate) fn finished(&self) -> bool {
        matches!(self, Self::Removed | Self::Absent)
    }
}

/// Current acknowledgments and reserved capacity for an immutable removal intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalProgress {
    pub intent_revision: i64,
    /// Lifecycle replay-accounting bytes, not filesystem storage or memory usage.
    pub reserved_future_bytes: u64,
    /// Positions correspond to the intent's registration indices.
    pub registrations: Vec<RemovalOutcome>,
    /// Positions correspond to the intent's object indices.
    pub objects: Vec<RemovalOutcome>,
}

/// Exact saved removal decisions, acknowledgments and cumulative logical byte outcomes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovalStatus {
    pub intent: RemovalIntent,
    pub progress: RemovalProgress,
    pub complete: bool,
    /// Logical file sizes with acknowledged unlink, not reclaimed allocated blocks.
    pub confirmed_unlinked_bytes: u64,
    /// Logical file sizes reconciled absent; no new unlink is claimed for them.
    pub reconciled_absent_bytes: u64,
}

/// Equality used to cancel matching rows in a comparison.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonIdentity {
    /// Compare typed visible values and their multiplicities.
    Values,
    /// Also compare each row's stable occurrence address.
    #[default]
    Occurrences,
}

/// Exact-release values-only lookup for a bounded set of scalar keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyReadSpec {
    pub key: String,
    pub values: Vec<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

/// Selects comparison identity and an explicit full reconstruction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComparisonOptions {
    pub identity: ComparisonIdentity,
    pub force_full: bool,
}

/// The work actually selected for a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonPath {
    /// The exact retained field-group pin is shared by both endpoints.
    Unchanged,
    /// Compose qualified local applied writes from the exact before version.
    ForwardChanges,
    /// Reconstruct differences from both complete endpoint relations.
    Full,
}

/// Why the comparison selected full reconstruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonFallback {
    Forced,
    /// Optional local change evidence is absent or retired.
    HistoryNotQualified,
    /// Endpoints do not form a supported forward relationship.
    NotForward,
    /// This field group has no qualified applied-change reader.
    UnsupportedGroup,
    /// Optional history exceeds the declared link or cumulative metadata bound.
    HistoryLimit,
    /// Differing schemas cannot be compared through an implicit cast.
    SchemaChanged,
}

/// Exact metadata differences, independent of the requested row identity.
///
/// Changed references are conservative signals, not proof of unequal payload values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonEffects {
    pub schema_changed: bool,
    pub input_refs_changed: bool,
    pub coverage_changed: bool,
    pub definition_changed: bool,
    /// Source receipts, contributor references, or computed execution evidence/bindings changed.
    pub evidence_refs_changed: bool,
    pub relationship_changed: bool,
    /// Computed groups whose declared definitions were added, removed or changed.
    pub changed_group_definitions: Vec<String>,
}

/// Describes a streamed difference between two exact retained endpoints.
///
/// Arrow schema metadata `docspec.change` contains this JSON. Each output row has
/// `side` (before/after), `file`, `row`, and a typed `values` struct. The address is
/// an occurrence in the corresponding endpoint, not necessarily a current file
/// location after compaction. Value mode chooses deterministic representatives
/// of surplus equal values; it does not assert which occurrence disappeared.
/// Completeness requires successful end of stream. This is not a portable
/// applied-write certificate or a retained availability promise.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeDescriptor {
    pub format: u32,
    pub before: VersionRef,
    pub after: VersionRef,
    pub group: String,
    pub identity: ComparisonIdentity,
    pub path: ComparisonPath,
    pub fallback: Option<ComparisonFallback>,
    /// Native identifiers of the visible value fields in output order.
    pub field_ids: Vec<i32>,
    /// Whether visible field identities and types permit a comparison without casts.
    pub comparable_values: bool,
    /// Native declared identifier sets, sorted; empty establishes no key here.
    pub before_key_field_ids: Vec<i32>,
    pub after_key_field_ids: Vec<i32>,
    pub effects: ComparisonEffects,
}

/// Multiset comparison counts and the exact comparison semantics.
#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub descriptor: ChangeDescriptor,
    pub same_schema: bool,
    pub same_inputs: bool,
    pub added_rows: Option<u64>,
    pub removed_rows: Option<u64>,
}

/// Partition transform applied to a source column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionTransformSpec {
    Identity,
    Bucket { num_buckets: u32 },
    Year,
    Month,
    Day,
    Truncate { width: u32 },
}

/// One declared partition field on a dataset's source table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionFieldSpec {
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub transform: PartitionTransformSpec,
}

/// A reproducible local run, with trusted expected digests for supplied inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    pub dataset: Id,
    pub operation: OperationId,
    /// Exact selected version to rebuild, preserving its computed group definitions.
    #[serde(default)]
    pub base: Option<Id>,
    /// Omission inherits the exact base, or declares a new source independent.
    /// Changing an existing relationship requires its selected base.
    #[serde(default)]
    pub relationship: Option<SourceRelationship>,
    pub input: String,
    pub expected_digest: String,
    pub sql: String,
    pub definition: String,
    /// Optional Iceberg partition layout for the source table.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<PartitionFieldSpec>,
}

/// A selected local input with an expected immutable byte identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub path: String,
    pub digest: String,
}

/// Column-level merge strategy when updating existing keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateMergeStrategy {
    /// Replace with incoming value when incoming wins, else keep prior value.
    #[default]
    Replace,
    /// Take non-null incoming value when incoming wins (falling back to prior),
    /// or keep non-null prior value when prior wins (backfilling from incoming).
    LatestOrBackfill,
    /// Keep the minimum non-null value across prior and incoming.
    Min,
    /// Keep the maximum non-null value across prior and incoming.
    Max,
    /// Deduplicated list union in canonical ascending element order.
    SetUnion,
}

impl UpdateMergeStrategy {
    fn from_kind_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "replace" => Ok(Self::Replace),
            "latest_or_backfill" => Ok(Self::LatestOrBackfill),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "set_union" => Ok(Self::SetUnion),
            other => Err(format!("unknown update merge strategy '{other}'")),
        }
    }
}

impl<'de> Deserialize<'de> for UpdateMergeStrategy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TaggedStrategy {
            #[serde(alias = "strategy")]
            kind: String,
        }

        let raw = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(s) = raw {
            return Self::from_kind_str(&s).map_err(serde::de::Error::custom);
        }
        let tagged: TaggedStrategy =
            serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        Self::from_kind_str(&tagged.kind).map_err(serde::de::Error::custom)
    }
}

/// Declarative column-level merge rules for keyed updates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateMergeSpec {
    /// Optional ordering column determining whether incoming or prior row is newer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<String>,
    /// Per-column merge strategies; omitted non-key columns default to `Replace`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, UpdateMergeStrategy>,
}

/// Complete replacement rows and deletion keys against an exact selected version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub definition: String,
    pub base: Id,
    pub keys: Vec<String>,
    pub replacements: InputSpec,
    /// Omission retains the base relationship; a supplied value replaces it.
    #[serde(default)]
    pub relationship: Option<SourceRelationship>,
    pub deletions: Option<InputSpec>,
    /// Optional column-level merge rules when replacement keys match existing rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge: Option<UpdateMergeSpec>,
}

/// Selects an existing root field's identity and current display name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
}

/// Scalar types qualified for metadata-only source additions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Boolean,
    Long,
    Double,
    String,
}

/// Adds a nullable field; its literal applies to old absence and future omission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnAddition {
    pub name: String,
    pub data_type: ColumnType,
    /// Null when omitted. Explicitly supplied nulls never take this default.
    #[serde(default)]
    pub default: serde_json::Value,
}

/// Edits source fields and defaults without rewriting existing values.
///
/// `fields` lists the complete new root order; omitted fields remain in retained
/// old versions. Additions follow this order and receive fresh native IDs. Type
/// changes and nested edits require other operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub base: Id,
    /// Expected active writer definition while preparing this change.
    pub definition: String,
    /// Writer definition selected atomically with the new schema.
    pub next_definition: String,
    pub fields: Vec<SchemaField>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additions: Vec<ColumnAddition>,
    /// Changes future omission rules by field ID, without changing old values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub write_defaults: BTreeMap<i32, serde_json::Value>,
}

/// A stable field identity and a row-local SQL expression over source values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedField {
    /// Stable logical field ID within its group, independent of native table numbering.
    pub id: i32,
    pub name: String,
    pub expression: String,
}

/// Independently computes a field group against a retained source version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub base: Id,
    /// Required active source definition.
    pub definition: String,
    pub group: Id,
    pub group_definition: String,
    pub fields: Vec<ComputedField>,
}

/// One recoverable scalar-field change while source versions continue advancing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousBackfillSpec {
    /// The operation identifies the parent intent; its base fixes original meaning.
    pub backfill: BackfillSpec,
    /// Attempts before temporarily fencing new source admissions, from 1 through 16.
    pub optimistic_rounds: u32,
    /// Maximum local fence lifetime, from 1 through 300000 milliseconds.
    pub fence_millis: u64,
}

/// Bounded control progress; child phases retain the actual processing evidence.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuousBackfillPlan {
    pub completed: Option<ReleasePin>,
    pub child: Option<Id>,
    pub round: u32,
    pub pending_source: Vec<Id>,
    pub fenced: bool,
}

/// Material scalar computation, independent of its adoption operation and captured source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupDefinition {
    pub group: Id,
    pub definition: String,
    pub fields: Vec<ComputedField>,
}

/// Records omitted source-field dependencies for one exact scalar definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyCorrection {
    pub dataset: Id,
    /// Stable retry identity; changed content requires a new identifier.
    pub id: Id,
    pub definition: GroupDefinition,
    /// Positive stable source-field IDs, separate from the original SQL bindings.
    pub fields: std::collections::BTreeSet<i32>,
    pub reason: String,
}

/// Immutable acknowledgment of a correction and its actual native commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectionDecision {
    pub request: DependencyCorrection,
    /// Exact scalar implementation whose material dependency scope was corrected.
    pub processor: String,
    /// Native lifecycle revision that atomically retained this decision and requirements.
    pub revision: i64,
}

impl BackfillSpec {
    /// Returns the material definition reused across source refreshes.
    pub fn group_definition(&self) -> GroupDefinition {
        GroupDefinition {
            group: self.group.clone(),
            definition: self.group_definition.clone(),
            fields: self.fields.clone(),
        }
    }
}

/// Exact source and expression binding for a completely covered field group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputedGroup {
    /// Immutable original file executions; absence means unknown historical origins.
    #[serde(default)]
    pub origins: Option<ObjectRef>,
    /// Immutable supplemental correspondence; original SQL bindings stay unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualification: Option<Box<DependencyQualification>>,
    pub definition: GroupDefinition,
    /// SQL input names bound to root field IDs in this exact source interpretation.
    pub input_fields: BTreeMap<String, i32>,
    /// Operation establishing complete coverage; reused files retain their original identities.
    pub request: OperationRequest,
    pub native_field_ids: BTreeMap<i32, i32>,
    pub source_metadata: ObjectRef,
    pub base_manifest: ObjectRef,
}

/// Retained correspondence between original execution context and selected source fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyQualification {
    pub checker: String,
    /// Digest-addressed scope of the exact scalar processor and group definition.
    pub scope: String,
    pub fields: std::collections::BTreeSet<i32>,
    /// Original file acknowledgments must precede this dataset lifecycle revision.
    pub first_revision: i64,
    pub origins: ObjectRef,
    pub target: NativeSnapshotRef,
}

/// A typed request sharing the same recovery and guarded selection path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "spec", rename_all = "snake_case")]
pub enum OperationRequest {
    Build(RunSpec),
    Update(UpdateSpec),
    Backfill(BackfillSpec),
    Refresh(Box<RefreshSpec>),
    Schema(SchemaSpec),
    Evidence(crate::evidence::EvidenceSpec),
}

impl OperationRequest {
    /// Returns the dataset whose authority selects this operation.
    pub fn dataset(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.dataset,
            Self::Update(spec) => &spec.dataset,
            Self::Backfill(spec) => &spec.dataset,
            Self::Refresh(spec) => &spec.dataset,
            Self::Schema(spec) => &spec.dataset,
            Self::Evidence(spec) => &spec.dataset,
        }
    }
    /// Returns the stable retry identity.
    pub fn operation(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.operation,
            Self::Update(spec) => &spec.operation,
            Self::Backfill(spec) => &spec.operation,
            Self::Refresh(spec) => &spec.operation,
            Self::Schema(spec) => &spec.operation,
            Self::Evidence(spec) => &spec.operation,
        }
    }
    /// Returns the required active definition.
    pub fn definition(&self) -> &str {
        match self {
            Self::Build(spec) => &spec.definition,
            Self::Update(spec) => &spec.definition,
            Self::Backfill(spec) => &spec.definition,
            Self::Refresh(spec) => &spec.definition,
            Self::Schema(spec) => &spec.definition,
            Self::Evidence(spec) => &spec.definition,
        }
    }
    /// Returns the writer definition selected with this operation's result.
    pub fn result_definition(&self) -> &str {
        match self {
            Self::Schema(spec) => &spec.next_definition,
            _ => self.definition(),
        }
    }
    /// Returns the selected version whose composition this operation captures.
    pub fn base(&self) -> Option<&Id> {
        match self {
            Self::Build(spec) => spec.base.as_ref(),
            Self::Update(spec) => Some(&spec.base),
            Self::Backfill(spec) => Some(&spec.base),
            Self::Refresh(spec) => spec.base.as_ref(),
            Self::Schema(spec) => Some(&spec.base),
            Self::Evidence(spec) => Some(&spec.base),
        }
    }
    /// Lists bounded input declarations, excluding their payloads.
    pub fn inputs(&self) -> BTreeMap<String, InputSpec> {
        match self {
            Self::Build(spec) => BTreeMap::from([(
                "input".into(),
                InputSpec {
                    path: spec.input.clone(),
                    digest: spec.expected_digest.clone(),
                },
            )]),
            Self::Update(spec) => {
                let mut inputs =
                    BTreeMap::from([("replacements".into(), spec.replacements.clone())]);
                if let Some(input) = &spec.deletions {
                    inputs.insert("deletions".into(), input.clone());
                }
                inputs
            }
            Self::Backfill(_) | Self::Refresh(_) | Self::Schema(_) => BTreeMap::new(),
            Self::Evidence(spec) => {
                let mut inputs =
                    BTreeMap::from([("replacements".into(), spec.replacements.clone())]);
                if let Some(input) = &spec.deletions {
                    inputs.insert("deletions".into(), input.clone());
                }
                inputs
            }
        }
    }

    /// Returns the retained input name for this operation's provenance.
    pub(crate) fn input_key(&self, name: &str) -> String {
        match self {
            Self::Build(_) => name.to_owned(),
            Self::Update(_)
            | Self::Backfill(_)
            | Self::Refresh(_)
            | Self::Schema(_)
            | Self::Evidence(_) => {
                format!("{}/{name}", self.operation())
            }
        }
    }
}

/// Typed applied rows retained by a recovery phase or optional change descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePin {
    /// Qualified local production semantics; absent older claims are not certified.
    #[serde(default)]
    pub rules: String,
    pub base_version: Id,
    pub base_manifest: ObjectRef,
    pub before_metadata: ObjectRef,
    pub after_metadata: ObjectRef,
    pub key_field_ids: Vec<i32>,
    pub removed: TablePin,
    pub added: TablePin,
}

/// Applied changes stored independently of the complete result's required evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedChangeSet {
    pub format: u32,
    pub rules: String,
    pub operation: OperationId,
    pub before: VersionRef,
    pub source: Option<ChangePin>,
    pub receipts: Option<ChangePin>,
}

/// Local change qualification selected atomically with its exact result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeCertificate {
    pub format: u32,
    pub rules: String,
    pub before: VersionRef,
    pub after: VersionRef,
    pub descriptor: ObjectRef,
}

/// A captured optional certificate and its bounded typed description.
///
/// This metadata identifies retained evidence but creates no lasting protection.
/// Readers and consumers must acquire their own admission or retention promise.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertifiedChanges {
    pub revision: i64,
    pub certificate: ChangeCertificate,
    pub changes: AppliedChangeSet,
}

/// Counts observed while discovering and writing a candidate; no reporting scan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteMetrics {
    /// Native file decode passes executed (`files_probed + files_rewritten` for keyed
    /// replacements, where each affected file is decoded once for key probing and once
    /// for full-row rewriting).
    pub files_scanned: u64,
    pub files_reused: u64,
    pub data_files_written: u64,
    pub bytes_reused: u64,
    pub bytes_written: u64,
    pub rows_removed: u64,
    pub rows_added: u64,
    /// Candidate files whose key columns were decoded after metadata/zone-map pruning.
    #[serde(default)]
    pub candidate_files_probed: u64,
    /// Files skipped before key decoding via Iceberg column bounds or Parquet row-group statistics.
    #[serde(default)]
    pub files_skipped_by_pruning: u64,
}

pub use crate::export::{
    ChildTableExport, ChildTableExportSpec, ExportKeyRange, FacetCountEntry, ReadingsFoldSpec,
    TaggedPredicate, TaggedScalarExpr, WhenThen,
};

#[cfg(test)]
mod unknown_field_tests {
    use super::*;

    fn update() -> serde_json::Value {
        serde_json::json!({
            "dataset": "news_captures",
            "operation": "append-1",
            "definition": "news-v1",
            "base": "admit-1",
            "keys": ["id"],
            "replacements": {"path": "replacements.parquet", "digest": "a".repeat(64)},
        })
    }

    #[test]
    fn update_rejects_unknown_fields_instead_of_dropping_them() {
        let mut value = update();
        value["unacknowledged"] = serde_json::json!("must refuse");
        let error = serde_json::from_value::<UpdateSpec>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    #[test]
    fn update_relationship_is_an_explicit_optional_field() {
        let spec: UpdateSpec = serde_json::from_value(update()).unwrap();
        assert!(spec.relationship.is_none());

        let mut value = update();
        value["relationship"] = serde_json::json!({
            "kind": "coupled", "family": "news", "generation": "g2"
        });
        let spec: UpdateSpec = serde_json::from_value(value).unwrap();
        assert_eq!(
            spec.relationship,
            Some(SourceRelationship::Coupled {
                family: Id::new("news").unwrap(),
                generation: Id::new("g2").unwrap(),
            })
        );
    }

    #[test]
    fn run_spec_rejects_unknown_fields() {
        let value = serde_json::json!({
            "dataset": "news_captures",
            "operation": "admit-1",
            "input": "source.parquet",
            "expected_digest": "a".repeat(64),
            "sql": "SELECT * FROM input",
            "definition": "news-v1",
            "unexpected": true,
        });
        assert!(serde_json::from_value::<RunSpec>(value).is_err());
    }
}
