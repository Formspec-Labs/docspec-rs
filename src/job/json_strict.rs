//! Keep typed JSON getters strict: a value converts only from its own JSON token.
//!
//! datafusion-functions-json 0.54 and later convert JSON strings to numbers and
//! booleans (`json_get_int('"7"')` is 7, upstream #110), read negative numbers
//! (#125), read integral floats as integers (#147) and widen long integers
//! (#124). 0.53, which every published congressional-tech output was built with,
//! returned NULL for all of these, and receipt classification depends on it: a
//! MODS descriptor with `"bytes": "45043"` must not qualify. Each wrapper here
//! evaluates the maintained getter, reads the located token with `json_get_json`,
//! and keeps the value only where that token has the form 0.53 accepted.
//!
//! Remove a wrapper only together with a deliberate, versioned output change.
// Rust guideline compliant 2026-02-21

use arrow::array::{ArrayRef, AsArray, BooleanArray};
use datafusion::{
    arrow::datatypes::{DataType, Field, FieldRef},
    common::{DFSchema, Result, ScalarValue, config::ConfigOptions, tree_node::Transformed},
    execution::FunctionRegistry,
    logical_expr::{
        ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
        ScalarUDFImpl, Signature,
        expr::{Cast, Expr, ScalarFunction, TryCast},
        expr_rewriter::FunctionRewrite,
    },
    prelude::SessionContext,
};
use std::sync::Arc;

/// The JSON token forms datafusion-functions-json 0.53 converted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Token {
    /// An unsigned JSON integer within jiter 0.13's 18-digit fast path. Longer
    /// integers decoded as BigInt, which 0.53 refused even when they fit i64.
    Integer,
    /// Any unsigned JSON number; 0.53 refused the leading minus sign.
    Number,
    /// The literals `true` and `false`.
    Boolean,
}

/// Longest integer 0.53 accepted: jiter 0.13 decodes 18 digits without BigInt.
const MAX_INTEGER_DIGITS: usize = 18;

impl Token {
    fn accepts(self, raw: &str) -> bool {
        match self {
            Self::Integer => {
                !raw.is_empty()
                    && raw.len() <= MAX_INTEGER_DIGITS
                    && raw.bytes().all(|b| b.is_ascii_digit())
            }
            Self::Number => raw.bytes().next().is_some_and(|b| b.is_ascii_digit()),
            Self::Boolean => raw == "true" || raw == "false",
        }
    }
}

/// A maintained typed getter restricted to its strict JSON token form.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StrictGetter {
    inner: Arc<ScalarUDF>,
    raw: Arc<ScalarUDF>,
    token: Token,
}

impl ScalarUDFImpl for StrictGetter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.inner().placement(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = args.number_rows;
        let raw_args = ScalarFunctionArgs {
            args: args.args.clone(),
            arg_fields: args.arg_fields.clone(),
            number_rows: rows,
            return_field: Arc::new(Field::new(self.raw.name(), DataType::Utf8, true)),
            config_options: Arc::clone(&args.config_options),
        };
        let raw = self.raw.invoke_with_args(raw_args)?.into_array(rows)?;
        let values = self.inner.invoke_with_args(args)?.into_array(rows)?;
        let refused = refused(&raw, self.token)?;
        let kept: ArrayRef = arrow::compute::nullif(&values, &refused)?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&kept, 0)?)
        } else {
            ColumnarValue::Array(kept)
        })
    }
}

/// True where the located token is missing or not of the strict form.
fn refused(raw: &ArrayRef, token: Token) -> Result<BooleanArray> {
    let raw = arrow::compute::cast(raw, &DataType::Utf8)?;
    Ok(raw
        .as_string::<i32>()
        .iter()
        .map(|value| Some(!value.is_some_and(|value| token.accepts(value))))
        .collect())
}

#[derive(Debug)]
struct StrictJsonRewriter {
    int: Arc<ScalarUDF>,
    float: Arc<ScalarUDF>,
    boolean: Arc<ScalarUDF>,
}

impl StrictJsonRewriter {
    fn strict_udf(&self, func: &Arc<ScalarUDF>) -> Option<Arc<ScalarUDF>> {
        if func.inner().is::<StrictGetter>() {
            return None;
        }
        match func.name() {
            "json_get_int" => Some(Arc::clone(&self.int)),
            "json_get_float" => Some(Arc::clone(&self.float)),
            "json_get_bool" => Some(Arc::clone(&self.boolean)),
            _ => None,
        }
    }

    fn rewrite_scalar(&self, call: &ScalarFunction) -> Option<ScalarFunction> {
        if let Some(func) = self.strict_udf(&call.func) {
            return Some(ScalarFunction {
                func,
                args: call.args.clone(),
            });
        }
        if matches!(call.func.name(), "arrow_cast" | "arrow_try_cast")
            && let [Expr::ScalarFunction(inner), target_type] = call.args.as_slice()
            && let Some(rewritten) = self.rewrite_scalar(inner)
        {
            return Some(ScalarFunction {
                func: Arc::clone(&call.func),
                args: vec![Expr::ScalarFunction(rewritten), target_type.clone()],
            });
        }
        None
    }
}

impl FunctionRewrite for StrictJsonRewriter {
    fn name(&self) -> &'static str {
        "StrictJsonRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let rewritten = match &expr {
            Expr::ScalarFunction(call) => self.rewrite_scalar(call).map(Expr::ScalarFunction),
            Expr::Cast(cast) => {
                if let Expr::ScalarFunction(inner) = cast.expr.as_ref() {
                    self.rewrite_scalar(inner).map(|rewritten| {
                        Expr::Cast(Cast {
                            expr: Box::new(Expr::ScalarFunction(rewritten)),
                            field: Arc::clone(&cast.field),
                        })
                    })
                } else {
                    None
                }
            }
            Expr::TryCast(try_cast) => {
                if let Expr::ScalarFunction(inner) = try_cast.expr.as_ref() {
                    self.rewrite_scalar(inner).map(|rewritten| {
                        Expr::TryCast(TryCast {
                            expr: Box::new(Expr::ScalarFunction(rewritten)),
                            field: Arc::clone(&try_cast.field),
                        })
                    })
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(rewritten.map_or_else(|| Transformed::no(expr), Transformed::yes))
    }
}

/// Replaces the maintained typed getters (and `JsonFunctionRewriter` cast targets)
/// with their strict 0.53 forms.
///
/// Call after `datafusion_functions_json::register_all`, which these override.
pub(super) fn register(context: &mut SessionContext) -> Result<()> {
    use datafusion_functions_json::udfs;
    let raw = udfs::json_get_json_udf();
    let strict = |inner: Arc<ScalarUDF>, token: Token| {
        Arc::new(ScalarUDF::new_from_impl(StrictGetter {
            inner,
            raw: Arc::clone(&raw),
            token,
        }))
    };
    let int = strict(udfs::json_get_int_udf(), Token::Integer);
    let float = strict(udfs::json_get_float_udf(), Token::Number);
    let boolean = strict(udfs::json_get_bool_udf(), Token::Boolean);
    for udf in [&int, &float, &boolean] {
        context.register_udf(Arc::clone(udf))?;
    }
    context.register_function_rewrite(Arc::new(StrictJsonRewriter {
        int,
        float,
        boolean,
    }))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Token;

    #[tokio::test]
    async fn job_sql_getters_refuse_strings_signs_floats_and_long_integers() {
        let root = tempfile::tempdir().unwrap();
        let job =
            crate::job::Job::new(crate::budget::Limits::default(), root.path().join("s")).unwrap();
        let sql = r#"SELECT
            json_get_int('{"a":7}','a') AS int_number,
            json_get_int('{"a":"45043"}','a') AS int_string,
            json_get_int('{"a":-1}','a') AS int_negative,
            json_get_int('{"a":2.0}','a') AS int_float,
            json_get_int('{"a":1234567890123456789}','a') AS int_long,
            CAST(json_get('{"a":7}','a') AS BIGINT) AS cast_int_number,
            CAST(json_get('{"a":"45043"}','a') AS BIGINT) AS cast_int_string,
            CAST(('{"a":"45043"}' -> 'a') AS INT) AS narrow_cast_int_string,
            TRY_CAST(('{"a":"45043"}' -> 'a') AS BIGINT) AS try_cast_int_string,
            arrow_cast(json_get('{"a":"45043"}','a'), 'Int64') AS arrow_cast_int_string,
            CAST(('{"a":"1.5"}' -> 'a') AS DOUBLE) AS cast_float_string,
            CAST(('{"a":"false"}' -> 'a') AS BOOLEAN) AS cast_bool_string,
            json_get_float('{"a":1.5}','a') AS float_number,
            json_get_float('{"a":"1.5"}','a') AS float_string,
            json_get_float('{"a":-1.5}','a') AS float_negative,
            json_get_bool('false') AS bool_literal,
            json_get_bool('"false"') AS bool_string"#;
        let batches = job.sql(sql).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        let value = |name: &str| {
            let column = batch.column(batch.schema().index_of(name).unwrap());
            datafusion::common::ScalarValue::try_from_array(column, 0).unwrap()
        };
        use datafusion::common::ScalarValue;
        assert_eq!(value("int_number"), ScalarValue::Int64(Some(7)));
        assert_eq!(value("cast_int_number"), ScalarValue::Int64(Some(7)));
        assert_eq!(value("float_number"), ScalarValue::Float64(Some(1.5)));
        assert_eq!(value("bool_literal"), ScalarValue::Boolean(Some(false)));
        for refused in [
            "int_string",
            "int_negative",
            "int_float",
            "int_long",
            "cast_int_string",
            "narrow_cast_int_string",
            "try_cast_int_string",
            "arrow_cast_int_string",
            "cast_float_string",
            "cast_bool_string",
            "float_string",
            "float_negative",
            "bool_string",
        ] {
            assert!(value(refused).is_null(), "{refused}: {}", value(refused));
        }
    }

    #[test]
    fn tokens_match_the_forms_functions_json_0_53_converted() {
        assert!(Token::Integer.accepts("0"));
        assert!(Token::Integer.accepts("123456789012345678"));
        for refused in [
            "\"7\"",
            "-1",
            "2.0",
            "1e3",
            "1234567890123456789",
            "null",
            "",
        ] {
            assert!(!Token::Integer.accepts(refused), "{refused}");
        }
        for accepted in ["0", "3", "1.5", "2e3"] {
            assert!(Token::Number.accepts(accepted), "{accepted}");
        }
        for refused in ["-1.5", "\"1.5\"", "true"] {
            assert!(!Token::Number.accepts(refused), "{refused}");
        }
        assert!(Token::Boolean.accepts("false") && Token::Boolean.accepts("true"));
        assert!(!Token::Boolean.accepts("\"false\"") && !Token::Boolean.accepts("0"));
    }
}
