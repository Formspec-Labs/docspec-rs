//! Canonical row-local scalar expressions, boolean predicates, and sort specifications.

use serde::{Deserialize, Serialize};

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
    pub(crate) fn from_json_value(value: serde_json::Value) -> std::result::Result<Self, String> {
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
                let Some(kind) = map
                    .remove("kind")
                    .and_then(|v| v.as_str().map(str::to_owned))
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

/// Compatibility alias for [`CaseWhen`].
pub type WhenThen = CaseWhen;

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
    Concat {
        exprs: Vec<ScalarExpr>,
    },
    Lower {
        expr: Box<ScalarExpr>,
    },
    Upper {
        expr: Box<ScalarExpr>,
    },
    Trim {
        expr: Box<ScalarExpr>,
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

/// Compatibility alias for [`ScalarExpr`].
pub type TaggedScalarExpr = ScalarExpr;

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
        enum RawScalarExpr {
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
            Concat {
                #[serde(alias = "args", alias = "values")]
                exprs: Vec<ScalarExpr>,
            },
            Lower {
                expr: Box<ScalarExpr>,
            },
            Upper {
                expr: Box<ScalarExpr>,
            },
            Trim {
                expr: Box<ScalarExpr>,
            },
            Binary {
                left: Box<ScalarExpr>,
                op: BinaryOp,
                right: Box<ScalarExpr>,
            },
            #[serde(alias = "case_when")]
            Case {
                #[serde(alias = "branches", alias = "cases")]
                when_then: Vec<CaseWhen>,
                #[serde(default, alias = "else", alias = "otherwise")]
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
        let tagged: RawScalarExpr =
            serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        Ok(match tagged {
            RawScalarExpr::Column { field } => Self::Column { field },
            RawScalarExpr::Literal { value } => Self::Literal { value },
            RawScalarExpr::Coalesce { exprs } => Self::Coalesce { exprs },
            RawScalarExpr::Concat { exprs } => Self::Concat { exprs },
            RawScalarExpr::Lower { expr } => Self::Lower { expr },
            RawScalarExpr::Upper { expr } => Self::Upper { expr },
            RawScalarExpr::Trim { expr } => Self::Trim { expr },
            RawScalarExpr::Binary { left, op, right } => Self::Binary { left, op, right },
            RawScalarExpr::Case {
                when_then,
                else_expr,
            } => Self::Case {
                when_then,
                else_expr,
            },
            RawScalarExpr::Cast { expr, data_type } => Self::Cast { expr, data_type },
            RawScalarExpr::Predicate { predicate } => Self::Predicate { predicate },
            RawScalarExpr::Sql { expr } => Self::Sql { expr },
        })
    }
}

/// Declarative boolean predicate for filtering join, aggregation, or export rows.
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

/// Compatibility alias for [`Predicate`].
pub type TaggedPredicate = Predicate;

impl Predicate {
    /// Creates an equality predicate (`left = right`).
    pub fn eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Eq { left, right }
    }

    /// Creates an inequality predicate (`left != right`).
    pub fn not_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::NotEq { left, right }
    }

    /// Shorthand for [`Self::not_eq`].
    pub fn neq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::not_eq(left, right)
    }

    /// Creates a less-than predicate (`left < right`).
    pub fn lt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Lt { left, right }
    }

    /// Creates a less-than-or-equal predicate (`left <= right`).
    pub fn lt_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::LtEq { left, right }
    }

    /// Shorthand for [`Self::lt_eq`].
    pub fn lte(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::lt_eq(left, right)
    }

    /// Creates a greater-than predicate (`left > right`).
    pub fn gt(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::Gt { left, right }
    }

    /// Creates a greater-than-or-equal predicate (`left >= right`).
    pub fn gt_eq(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::GtEq { left, right }
    }

    /// Shorthand for [`Self::gt_eq`].
    pub fn gte(left: ScalarExpr, right: ScalarExpr) -> Self {
        Self::gt_eq(left, right)
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
                    _ => Err(
                        "logical predicate requires either 'exprs' or ('left' and 'right')".into(),
                    ),
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
        let Some(kind) = map
            .remove("kind")
            .and_then(|v| v.as_str().map(str::to_owned))
        else {
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
            "eq" | "not_eq" | "ne" | "neq" | "lt" | "lt_eq" | "le" | "lte" | "gt" | "gt_eq"
            | "ge" | "gte" => {
                let payload: BinaryEndpoints =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let (left, right) = payload.into_pair().map_err(serde::de::Error::custom)?;
                Ok(match kind.as_str() {
                    "eq" => Self::Eq { left, right },
                    "not_eq" | "ne" | "neq" => Self::NotEq { left, right },
                    "lt" => Self::Lt { left, right },
                    "lt_eq" | "le" | "lte" => Self::LtEq { left, right },
                    "gt" => Self::Gt { left, right },
                    _ => Self::GtEq { left, right },
                })
            }
            "is_null" | "is_not_null" => {
                let payload: UnaryEndpoint =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let expr = payload.into_expr().map_err(serde::de::Error::custom)?;
                Ok(if kind == "is_null" {
                    Self::IsNull { expr }
                } else {
                    Self::IsNotNull { expr }
                })
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
            "and" | "or" => {
                let payload: LogicalPayload =
                    serde_json::from_value(rest).map_err(serde::de::Error::custom)?;
                let exprs = payload.into_vec().map_err(serde::de::Error::custom)?;
                Ok(if kind == "and" {
                    Self::And { exprs }
                } else {
                    Self::Or { exprs }
                })
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

/// Sort direction for ordered aggregate selection and exported tables.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Null placement for ordered aggregate selection and exported tables.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NullOrdering {
    NullsFirst,
    #[default]
    NullsLast,
}

/// One ordering column for deterministic aggregate representative selection or export sorting.
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

    /// Returns whether `NULL` values sort before non-null values (`NULLS FIRST`).
    pub fn nulls_first(&self) -> bool {
        self.nulls == NullOrdering::NullsFirst
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
