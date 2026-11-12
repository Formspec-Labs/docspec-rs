// Rust guideline compliant 2026-02-21
//! Recipe parameter JSON Schema validation and lexer-safe DataFusion parameter binding.

use super::exact::{BridgeError, valid_integer_lexeme, valid_json_number};
use datafusion::{
    common::ScalarValue,
    sql::sqlparser::{
        dialect::GenericDialect,
        tokenizer::{Location, Token, Tokenizer},
    },
};
use serde_json::{Map, Value};
use std::{cmp::Ordering, collections::BTreeMap};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalDecimal {
    neg: bool,
    /// Significant ASCII digits `'1'..='9'`..`'1'..='9'` (empty for zero).
    sig: Vec<u8>,
    /// Place value of `sig[0]` is `10^(mag - 1)`. Zero uses `mag = 0`.
    mag: i32,
}

fn parse_canonical_decimal(lex: &str) -> Option<CanonicalDecimal> {
    if !valid_json_number(lex) {
        return None;
    }
    let (neg, unsigned) = match lex.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, lex),
    };
    let (coeff, exp_part) = match unsigned.split_once(['e', 'E']) {
        Some((c, e)) => (c, e.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (int_part, frac_part) = match coeff.split_once('.') {
        Some((i, f)) => (i, f),
        None => (coeff, ""),
    };
    let mut digits = Vec::with_capacity(int_part.len() + frac_part.len());
    digits.extend_from_slice(int_part.as_bytes());
    digits.extend_from_slice(frac_part.as_bytes());
    let frac_len = i32::try_from(frac_part.len()).ok()?;
    let base_exp = exp_part.checked_sub(frac_len)?;

    let first_nz = digits.iter().position(|&b| b != b'0');
    let Some(first_idx) = first_nz else {
        return Some(CanonicalDecimal {
            neg: false,
            sig: Vec::new(),
            mag: 0,
        });
    };
    let last_nz = digits.iter().rposition(|&b| b != b'0').unwrap_or(first_idx);
    let trailing_zeros = i32::try_from(digits.len() - 1 - last_nz).ok()?;
    let sig = digits[first_idx..=last_nz].to_vec();
    let sig_len = i32::try_from(sig.len()).ok()?;
    let mag = base_exp.checked_add(trailing_zeros)?.checked_add(sig_len)?;
    Some(CanonicalDecimal { neg, sig, mag })
}

fn compare_numeric_lexemes(left: &str, right: &str) -> Option<Ordering> {
    let a = parse_canonical_decimal(left)?;
    let b = parse_canonical_decimal(right)?;
    if a.sig.is_empty() && b.sig.is_empty() {
        return Some(Ordering::Equal);
    }
    if a.sig.is_empty() {
        return Some(if b.neg {
            Ordering::Greater
        } else {
            Ordering::Less
        });
    }
    if b.sig.is_empty() {
        return Some(if a.neg {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    if a.neg != b.neg {
        return Some(if a.neg {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    let pos_cmp = match a.mag.cmp(&b.mag) {
        Ordering::Equal => a.sig.cmp(&b.sig),
        other => other,
    };
    Some(if a.neg { pos_cmp.reverse() } else { pos_cmp })
}

fn value_numeric_lexeme(value: &Value, raw_override: Option<&str>) -> Option<String> {
    if let Some(raw) = raw_override
        && valid_json_number(raw)
    {
        return Some(raw.to_owned());
    }
    match value {
        Value::Number(n) => {
            let s = n.to_string();
            valid_json_number(&s).then_some(s)
        }
        Value::Object(obj)
            if obj.len() == 1
                && obj
                    .get("$number")
                    .and_then(Value::as_str)
                    .is_some_and(valid_json_number) =>
        {
            obj.get("$number")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }
        _ => None,
    }
}

fn values_equal_for_schema(actual: &Value, actual_lex: Option<&str>, expected: &Value) -> bool {
    if let (Some(a_lex), Some(e_lex)) = (
        value_numeric_lexeme(actual, actual_lex),
        value_numeric_lexeme(expected, None),
    ) {
        return compare_numeric_lexemes(&a_lex, &e_lex) == Some(Ordering::Equal);
    }
    actual == expected
}

/// Normalized parameter map paired with exact raw numeric lexemes.
pub(super) type NormalizedParameters = (BTreeMap<String, Value>, BTreeMap<String, String>);

/// Normalizes incoming `parameters` and `rawNumericParameters`, extracting any
/// `{ "$number": "<lex>" }` wrappers and verifying consistency between both maps.
pub(super) fn normalize_incoming_parameters(
    raw_params: &Map<String, Value>,
    raw_numeric: &Map<String, Value>,
) -> Result<NormalizedParameters, BridgeError> {
    let mut params = BTreeMap::new();
    let mut lexemes = BTreeMap::new();
    for (k, v) in raw_numeric {
        let Some(param_val) = raw_params.get(k) else {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("rawNumericParameters[{k:?}] has no matching entry in parameters."),
                400,
            ));
        };
        let Some(s) = v.as_str() else {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("rawNumericParameters[{k:?}] must be a string."),
                400,
            ));
        };
        if !valid_json_number(s) {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("rawNumericParameters[{k:?}] is not a valid JSON number."),
                400,
            ));
        }
        match param_val {
            Value::Number(n) => {
                let raw_f = s
                    .parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .ok_or_else(|| {
                        BridgeError::new(
                            "invalid_parameters",
                            format!("rawNumericParameters[{k:?}] exceeds finite bounds."),
                            400,
                        )
                    })?;
                let param_f = n.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
                    BridgeError::new(
                        "invalid_parameters",
                        format!("Numeric parameter {k:?} exceeds finite bounds."),
                        400,
                    )
                })?;
                let consistent = raw_f == param_f
                    || ((raw_f - param_f).abs() <= raw_f.abs() * (2.0 * f64::EPSILON));
                if !consistent {
                    return Err(BridgeError::new(
                        "invalid_parameters",
                        format!("rawNumericParameters[{k:?}] disagrees with parameters[{k:?}]."),
                        400,
                    ));
                }
            }
            Value::Object(obj)
                if obj.len() == 1 && obj.get("$number").and_then(Value::as_str).is_some() =>
            {
                let inner = obj.get("$number").and_then(Value::as_str).unwrap_or("");
                if compare_numeric_lexemes(s, inner) != Some(Ordering::Equal) {
                    return Err(BridgeError::new(
                        "invalid_parameters",
                        format!("rawNumericParameters[{k:?}] disagrees with parameters[{k:?}]."),
                        400,
                    ));
                }
            }
            _ => {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!(
                        "rawNumericParameters[{k:?}] conflicts with non-numeric parameter value."
                    ),
                    400,
                ));
            }
        }
        lexemes.insert(k.clone(), s.to_owned());
    }
    for (k, v) in raw_params {
        if let Value::Object(obj) = v
            && obj.len() == 1
            && let Some(Value::String(num_str)) = obj.get("$number")
        {
            if !valid_json_number(num_str) {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Numeric parameter {k:?} is not a valid exact number."),
                    400,
                ));
            }
            lexemes.entry(k.clone()).or_insert_with(|| num_str.clone());
            if let Ok(parsed_i) = num_str.parse::<i64>() {
                params.insert(k.clone(), Value::Number(parsed_i.into()));
            } else if let Ok(parsed_f) = num_str.parse::<f64>()
                && parsed_f.is_finite()
                && let Some(num) = serde_json::Number::from_f64(parsed_f)
            {
                params.insert(k.clone(), Value::Number(num));
            } else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Numeric parameter {k:?} exceeds finite bounds."),
                    400,
                ));
            }
        } else {
            params.insert(k.clone(), v.clone());
        }
    }
    Ok((params, lexemes))
}

fn check_single_type(type_name: &str, val: &Value, raw_lex: Option<&str>) -> bool {
    match type_name {
        "null" => val.is_null() && raw_lex.is_none(),
        "boolean" => val.is_boolean() && raw_lex.is_none(),
        "string" => val.is_string() && raw_lex.is_none(),
        "integer" => {
            if let Some(lex) = raw_lex {
                val.is_number() && valid_integer_lexeme(lex)
            } else if val.is_i64() || val.is_u64() {
                true
            } else if let Some(f) = val.as_f64() {
                f.is_finite() && f.fract() == 0.0
            } else {
                false
            }
        }
        "number" => val.is_number() && value_numeric_lexeme(val, raw_lex).is_some(),
        "array" => val.is_array() && raw_lex.is_none(),
        "object" => val.is_object() && raw_lex.is_none(),
        _ => false,
    }
}

/// Validates recipe `parameters` against the recipe's JSON Schema object.
pub(super) fn validate_parameters_schema(
    parameters: &BTreeMap<String, Value>,
    raw_lexemes: &BTreeMap<String, String>,
    schema: Option<&Value>,
) -> Result<(), BridgeError> {
    let Some(Value::Object(schema_obj)) = schema else {
        return Ok(());
    };

    let props = schema_obj.get("properties").and_then(Value::as_object);

    if let Some(Value::Array(req_list)) = schema_obj.get("required") {
        for req_item in req_list {
            if let Some(req_key) = req_item.as_str()
                && !parameters.contains_key(req_key)
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Missing required recipe parameter: {req_key:?}."),
                    400,
                ));
            }
        }
    }

    if schema_obj.get("additionalProperties") == Some(&Value::Bool(false)) {
        for key in parameters.keys() {
            let allowed = props.is_some_and(|p| p.contains_key(key));
            if !allowed {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Unexpected recipe parameter: {key:?}."),
                    400,
                ));
            }
        }
    }

    for (key, val) in parameters {
        let Some(prop_val) = props.and_then(|p| p.get(key)) else {
            continue;
        };
        let Some(prop) = prop_val.as_object() else {
            continue;
        };
        let raw_lex = raw_lexemes.get(key).map(String::as_str);

        if let Some(type_decl) = prop.get("type") {
            let ok = match type_decl {
                Value::String(t) => check_single_type(t, val, raw_lex),
                Value::Array(ts) => ts
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|t| check_single_type(t, val, raw_lex)),
                _ => true,
            };
            if !ok {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} does not match declared type {type_decl}."),
                    400,
                ));
            }
        }

        if let Some(const_val) = prop.get("const")
            && !values_equal_for_schema(val, raw_lex, const_val)
        {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("Parameter {key:?} does not match declared const value."),
                400,
            ));
        }

        if let Some(Value::Array(enum_vals)) = prop.get("enum")
            && !enum_vals
                .iter()
                .any(|candidate| values_equal_for_schema(val, raw_lex, candidate))
        {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("Parameter {key:?} is not one of the allowed enum values."),
                400,
            ));
        }

        if let Some(val_lex) = value_numeric_lexeme(val, raw_lex) {
            if let Some(min_v) = prop.get("minimum")
                && let Some(min_lex) = value_numeric_lexeme(min_v, None)
                && compare_numeric_lexemes(&val_lex, &min_lex) == Some(Ordering::Less)
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} is below minimum {min_lex}."),
                    400,
                ));
            }
            if let Some(max_v) = prop.get("maximum")
                && let Some(max_lex) = value_numeric_lexeme(max_v, None)
                && compare_numeric_lexemes(&val_lex, &max_lex) == Some(Ordering::Greater)
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} exceeds maximum {max_lex}."),
                    400,
                ));
            }
            if let Some(ex_min_v) = prop.get("exclusiveMinimum")
                && let Some(ex_min_lex) = value_numeric_lexeme(ex_min_v, None)
                && matches!(
                    compare_numeric_lexemes(&val_lex, &ex_min_lex),
                    Some(Ordering::Less | Ordering::Equal)
                )
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} must be strictly greater than {ex_min_lex}."),
                    400,
                ));
            }
            if let Some(ex_max_v) = prop.get("exclusiveMaximum")
                && let Some(ex_max_lex) = value_numeric_lexeme(ex_max_v, None)
                && matches!(
                    compare_numeric_lexemes(&val_lex, &ex_max_lex),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} must be strictly less than {ex_max_lex}."),
                    400,
                ));
            }
        }

        if let Some(s) = val.as_str() {
            let char_len = s.chars().count() as u64;
            if let Some(min_len) = prop.get("minLength").and_then(Value::as_u64)
                && char_len < min_len
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} is shorter than minLength {min_len}."),
                    400,
                ));
            }
            if let Some(max_len) = prop.get("maxLength").and_then(Value::as_u64)
                && char_len > max_len
            {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Parameter {key:?} exceeds maxLength {max_len}."),
                    400,
                ));
            }
        }
    }

    Ok(())
}

fn parameter_scalar_value(
    value: &Value,
    prop_schema: Option<&Map<String, Value>>,
    raw_lexeme: Option<&str>,
) -> Result<ScalarValue, BridgeError> {
    let allowed_types: Vec<&str> = match prop_schema.and_then(|p| p.get("type")) {
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(arr)) => arr.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };

    if value.is_null() {
        return Ok(ScalarValue::Null);
    }

    let resolved_type = if allowed_types.is_empty() {
        if value.is_boolean() {
            "boolean"
        } else if value.is_string() {
            "string"
        } else if raw_lexeme.is_some_and(valid_integer_lexeme)
            || (raw_lexeme.is_none() && (value.is_i64() || value.is_u64()))
        {
            "integer"
        } else if value.is_number() || raw_lexeme.is_some() {
            "number"
        } else {
            return Err(BridgeError::new(
                "invalid_parameters",
                "Unsupported recipe parameter value.",
                400,
            ));
        }
    } else if allowed_types.contains(&"boolean") && check_single_type("boolean", value, raw_lexeme)
    {
        "boolean"
    } else if allowed_types.contains(&"integer") && check_single_type("integer", value, raw_lexeme)
    {
        "integer"
    } else if allowed_types.contains(&"number") && check_single_type("number", value, raw_lexeme) {
        "number"
    } else if allowed_types.contains(&"string") && check_single_type("string", value, raw_lexeme) {
        "string"
    } else {
        return Err(BridgeError::new(
            "invalid_parameters",
            format!("Unsupported or mismatched recipe parameter type: {allowed_types:?}"),
            400,
        ));
    };

    match resolved_type {
        "boolean" => {
            let Some(b) = value.as_bool() else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Expected boolean recipe parameter.",
                    400,
                ));
            };
            Ok(ScalarValue::Boolean(Some(b)))
        }
        "integer" => {
            let lex_owned;
            let lex = if let Some(raw) = raw_lexeme {
                raw
            } else if let Some(i) = value.as_i64() {
                lex_owned = i.to_string();
                &lex_owned
            } else if let Some(u) = value.as_u64() {
                lex_owned = u.to_string();
                &lex_owned
            } else if let Some(f) = value.as_f64()
                && f.is_finite()
                && f.fract() == 0.0
                && f >= (i64::MIN as f64)
                && f < (i64::MAX as f64)
            {
                lex_owned = (f as i64).to_string();
                &lex_owned
            } else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Expected exact integer recipe parameter.",
                    400,
                ));
            };
            if !valid_integer_lexeme(lex) {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Expected exact integer recipe parameter.",
                    400,
                ));
            }
            let val_i64 = lex.parse::<i64>().map_err(|_| {
                BridgeError::new(
                    "invalid_parameters",
                    "Integer recipe parameter overflows signed 64-bit integer range.",
                    400,
                )
            })?;
            Ok(ScalarValue::Int64(Some(val_i64)))
        }
        "number" => {
            let Some(lex) = value_numeric_lexeme(value, raw_lexeme) else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Expected exact numeric recipe parameter.",
                    400,
                ));
            };
            if !lex.contains(['e', 'E']) {
                let neg = lex.starts_with('-');
                let unsigned = lex.strip_prefix('-').unwrap_or(lex.as_str());
                let (int_part, frac_part) = unsigned.split_once('.').unwrap_or((unsigned, ""));
                let scale = frac_part.len();
                let int_sig_len = int_part.trim_start_matches('0').len();
                let precision = (int_sig_len + scale).max(scale).max(1);
                if precision <= 38 && scale <= 38 {
                    let digits_str = format!("{int_part}{frac_part}");
                    let mag = digits_str.parse::<i128>().map_err(|_| {
                        BridgeError::new(
                            "invalid_parameters",
                            "Decimal recipe parameter exceeds 128-bit precision.",
                            400,
                        )
                    })?;
                    let signed = if neg && mag != 0 { -mag } else { mag };
                    return Ok(ScalarValue::Decimal128(
                        Some(signed),
                        precision as u8,
                        scale as i8,
                    ));
                }
            }
            let parsed_f = lex
                .parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .ok_or_else(|| {
                    BridgeError::new(
                        "invalid_parameters",
                        "Numeric recipe parameter exceeds finite IEEE-754 bounds.",
                        400,
                    )
                })?;
            Ok(ScalarValue::Float64(Some(parsed_f)))
        }
        "string" => {
            let Some(text) = value.as_str() else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Expected string recipe parameter.",
                    400,
                ));
            };
            if text.contains('\0') {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "NUL bytes are not permitted in SQL string parameters.",
                    400,
                ));
            }
            Ok(ScalarValue::Utf8(Some(text.to_owned())))
        }
        _ => Err(BridgeError::new(
            "invalid_parameters",
            format!("Unsupported recipe parameter type: {resolved_type:?}"),
            400,
        )),
    }
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn choose_placeholder_prefix(sql_text: &str) -> String {
    let mut candidate = "_ddk_p".to_owned();
    let mut suffix = 0u32;
    while sql_text.contains(&candidate) {
        suffix = suffix.saturating_add(1);
        candidate = format!("_ddk_p{suffix}_");
    }
    candidate
}

struct LocationCursor<'a> {
    sql_len: usize,
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    line: u64,
    col: u64,
}

impl<'a> LocationCursor<'a> {
    fn new(sql_text: &'a str) -> Self {
        Self {
            sql_len: sql_text.len(),
            chars: sql_text.char_indices().peekable(),
            line: 1,
            col: 1,
        }
    }

    fn advance_to(&mut self, target: Location) -> Result<usize, BridgeError> {
        while (self.line, self.col) < (target.line, target.column) {
            let Some((_, ch)) = self.chars.next() else {
                if (self.line, self.col) == (target.line, target.column) {
                    return Ok(self.sql_len);
                }
                return Err(BridgeError::new(
                    "native_read_refused",
                    "SQL tokenizer location exceeded input bounds.",
                    409,
                ));
            };
            if ch == '\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
        if (self.line, self.col) != (target.line, target.column) {
            return Err(BridgeError::new(
                "native_read_refused",
                "Non-monotonic SQL token span encountered.",
                409,
            ));
        }
        Ok(self.chars.peek().map_or(self.sql_len, |&(idx, _)| idx))
    }
}

/// Tokenizes `sql_text` with DataFusion's `GenericDialect`, strips an optional trailing
/// semicolon without mutating literals or comments, and rewrites `?`, `$1..$N`, `:name`,
/// and `$name` tokens into native DataFusion `ScalarValue` parameter bindings.
pub(super) fn prepare_sql_parameters(
    sql_text: &str,
    parameters: &BTreeMap<String, Value>,
    raw_lexemes: &BTreeMap<String, String>,
    param_schema: Option<&Value>,
    parameter_order: Option<&[String]>,
) -> Result<(String, Vec<(String, ScalarValue)>), BridgeError> {
    let props = param_schema
        .and_then(Value::as_object)
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object);

    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_text)
        .tokenize_with_location()
        .map_err(|err| {
            BridgeError::new(
                "native_read_refused",
                format!("Invalid recipe SQL syntax: {err}"),
                409,
            )
        })?;

    let last_non_ws = tokens
        .iter()
        .rposition(|t| !matches!(t.token, Token::Whitespace(_)));

    let prefix = choose_placeholder_prefix(sql_text);
    let mut replacements: Vec<(Location, Location, String)> = Vec::new();
    let mut bound_values: Vec<(String, ScalarValue)> = Vec::new();
    let mut qmark_index = 0usize;

    let bind_named = |name: &str,
                      start: Location,
                      end: Location,
                      replacements: &mut Vec<(Location, Location, String)>,
                      bound_values: &mut Vec<(String, ScalarValue)>|
     -> Result<(), BridgeError> {
        let Some(val) = parameters.get(name) else {
            return Err(BridgeError::new(
                "invalid_parameters",
                format!("Missing value for parameter {name:?}."),
                400,
            ));
        };
        let prop_map = props.and_then(|p| p.get(name)).and_then(Value::as_object);
        let scalar =
            parameter_scalar_value(val, prop_map, raw_lexemes.get(name).map(String::as_str))?;
        let slot_id = format!("{prefix}{}", bound_values.len() + 1);
        replacements.push((start, end, format!(" ${slot_id} ")));
        bound_values.push((slot_id, scalar));
        Ok(())
    };

    let mut idx = 0usize;
    while idx < tokens.len() {
        let tok = &tokens[idx];
        match &tok.token {
            Token::SemiColon => {
                if Some(idx) != last_non_ws {
                    return Err(BridgeError::new(
                        "native_read_refused",
                        "Multiple SQL statements are not permitted in owner recipes.",
                        409,
                    ));
                }
                replacements.push((tok.span.start, tok.span.end, String::new()));
                idx += 1;
            }
            Token::Placeholder(raw_ph) => {
                if raw_ph == "?" {
                    let Some(order) = parameter_order.filter(|o| qmark_index < o.len()) else {
                        return Err(BridgeError::new(
                            "invalid_parameters",
                            "Positional SQL placeholder has no matching parameterOrder entry.",
                            400,
                        ));
                    };
                    let name = &order[qmark_index];
                    qmark_index += 1;
                    bind_named(
                        name,
                        tok.span.start,
                        tok.span.end,
                        &mut replacements,
                        &mut bound_values,
                    )?;
                } else if let Some(rest) = raw_ph.strip_prefix('$') {
                    if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) {
                        let pos_1 = rest.parse::<usize>().map_err(|_| {
                            BridgeError::new(
                                "invalid_parameters",
                                "Positional parameter index overflow.",
                                400,
                            )
                        })?;
                        if pos_1 == 0 {
                            return Err(BridgeError::new(
                                "invalid_parameters",
                                "Positional parameter $0 is out of range.",
                                400,
                            ));
                        }
                        let order_idx = pos_1 - 1;
                        let Some(order) = parameter_order.filter(|o| order_idx < o.len()) else {
                            return Err(BridgeError::new(
                                "invalid_parameters",
                                format!("Positional parameter ${pos_1} is out of range."),
                                400,
                            ));
                        };
                        bind_named(
                            &order[order_idx],
                            tok.span.start,
                            tok.span.end,
                            &mut replacements,
                            &mut bound_values,
                        )?;
                    } else if let Some(&first) = rest.as_bytes().first()
                        && is_ident_start(first)
                        && rest.as_bytes()[1..].iter().copied().all(is_ident_continue)
                    {
                        bind_named(
                            rest,
                            tok.span.start,
                            tok.span.end,
                            &mut replacements,
                            &mut bound_values,
                        )?;
                    } else {
                        return Err(BridgeError::new(
                            "invalid_parameters",
                            format!("Unsupported SQL placeholder {raw_ph:?}."),
                            400,
                        ));
                    }
                } else {
                    return Err(BridgeError::new(
                        "invalid_parameters",
                        format!("Unsupported SQL placeholder {raw_ph:?}."),
                        400,
                    ));
                }
                idx += 1;
            }
            Token::Colon => {
                if let Some(next_tok) = tokens.get(idx + 1)
                    && tok.span.end == next_tok.span.start
                    && let Token::Word(word) = &next_tok.token
                    && word.quote_style.is_none()
                    && let Some(&first) = word.value.as_bytes().first()
                    && is_ident_start(first)
                    && word.value.as_bytes()[1..]
                        .iter()
                        .copied()
                        .all(is_ident_continue)
                {
                    bind_named(
                        &word.value,
                        tok.span.start,
                        next_tok.span.end,
                        &mut replacements,
                        &mut bound_values,
                    )?;
                    idx += 2;
                    continue;
                }
                idx += 1;
            }
            _ => {
                idx += 1;
            }
        }
    }

    if let Some(order) = parameter_order
        && !order.is_empty()
        && qmark_index != 0
        && qmark_index != order.len()
    {
        return Err(BridgeError::new(
            "invalid_parameters",
            "Positional SQL placeholder count disagrees with parameterOrder.",
            400,
        ));
    }

    if replacements.is_empty() {
        return Ok((sql_text.to_owned(), bound_values));
    }

    let mut cursor = LocationCursor::new(sql_text);
    let mut out = String::with_capacity(sql_text.len() + bound_values.len() * 8);
    let mut prev_byte = 0usize;
    for (start_loc, end_loc, replacement) in replacements {
        let start_byte = cursor.advance_to(start_loc)?;
        let end_byte = cursor.advance_to(end_loc)?;
        out.push_str(&sql_text[prev_byte..start_byte]);
        out.push_str(&replacement);
        prev_byte = end_byte;
    }
    out.push_str(&sql_text[prev_byte..]);

    Ok((out, bound_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn binds_parameters_safely_across_nested_comments_escaped_strings_and_exact_decimals() {
        let schema = json!({
            "type": "object",
            "required": ["region", "min_id", "min_total", "active"],
            "additionalProperties": false,
            "properties": {
                "region": { "type": "string" },
                "min_id": { "type": "integer" },
                "min_total": { "type": "number" },
                "active": { "type": "boolean" }
            }
        });
        let raw_p = json!({
            "region": "*/ UNION ALL SELECT secret FROM other --",
            "min_id": { "$number": "9007199254740993" },
            "min_total": { "$number": "0.1" },
            "active": true
        });
        let (params, lexemes) =
            normalize_incoming_parameters(raw_p.as_object().unwrap(), &Map::new()).unwrap();
        validate_parameters_schema(&params, &lexemes, Some(&schema)).unwrap();

        let order = vec![
            "region".to_owned(),
            "min_id".to_owned(),
            "min_total".to_owned(),
            "active".to_owned(),
        ];

        // Mixed `?` (count=1) with `parameterOrder` len 4 is rejected
        let mixed_sql = "SELECT col FROM t WHERE r = ? AND id >= $min_id;";
        assert!(
            prepare_sql_parameters(mixed_sql, &params, &lexemes, Some(&schema), Some(&order))
                .is_err()
        );

        // Adversarial SQL with E'a\'b', backticks, $$a'b$$, nested /* /* */ :region */, \r\n inside literal, and trailing comment after `;`
        let adversarial_sql = "SELECT E'a\\'b', 'line1\r\n:min_id', `owner's :region`, $$a'b :min_total$$, col::TEXT \
                               /* outer /* inner */ :region */ \
                               FROM t WHERE r = $1 AND id >= $min_id AND total >= :min_total AND ok = $4;\n-- trailing comment\n";
        let (rewritten, bound) = prepare_sql_parameters(
            adversarial_sql,
            &params,
            &lexemes,
            Some(&schema),
            Some(&order),
        )
        .unwrap();

        // \r\n inside literal and nested comment content remain untouched; trailing `;` is stripped
        assert!(rewritten.contains("'line1\r\n:min_id'"));
        assert!(rewritten.contains("/* outer /* inner */ :region */"));
        assert!(!rewritten.contains(';'));
        assert!(!rewritten.contains("UNION ALL"));
        assert_eq!(bound.len(), 4);
        assert_eq!(
            bound[0].1,
            ScalarValue::Utf8(Some("*/ UNION ALL SELECT secret FROM other --".to_owned()))
        );
        assert_eq!(bound[1].1, ScalarValue::Int64(Some(9_007_199_254_740_993)));
        // <= 15-digit decimal `0.1` stays exact Decimal128(1, 1), NOT Float64!
        assert_eq!(bound[2].1, ScalarValue::Decimal128(Some(1), 1, 1));
        assert_eq!(bound[3].1, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn enforces_raw_numeric_consistency_union_types_and_i64_bounds() {
        // Mismatched rawNumericParameters vs null/string/wrong number is rejected
        assert!(
            normalize_incoming_parameters(
                json!({"x": null}).as_object().unwrap(),
                json!({"x": "42"}).as_object().unwrap()
            )
            .is_err()
        );
        assert!(
            normalize_incoming_parameters(
                json!({"x": "not-a-number"}).as_object().unwrap(),
                json!({"x": "42"}).as_object().unwrap()
            )
            .is_err()
        );
        assert!(
            normalize_incoming_parameters(
                json!({"x": 99}).as_object().unwrap(),
                json!({"x": "42"}).as_object().unwrap()
            )
            .is_err()
        );

        // Union type ["string", "integer"] dispatches both string and integer values cleanly
        let union_schema = json!({
            "type": "object",
            "properties": {
                "code": { "type": ["string", "integer"] },
                "enum_int": { "enum": [1, 2, 3] },
                "tiny_dec": { "type": "number" }
            }
        });
        let tiny_38 = format!("0.{}1", "0".repeat(37));
        let (p_int, l_int) = normalize_incoming_parameters(
            json!({"code": 42, "enum_int": 2, "tiny_dec": {"$number": tiny_38}})
                .as_object()
                .unwrap(),
            &Map::new(),
        )
        .unwrap();
        validate_parameters_schema(&p_int, &l_int, Some(&union_schema)).unwrap();
        let (_, bound_int) = prepare_sql_parameters(
            "SELECT :code, :enum_int, :tiny_dec",
            &p_int,
            &l_int,
            Some(&union_schema),
            None,
        )
        .unwrap();
        assert_eq!(bound_int[0].1, ScalarValue::Int64(Some(42)));
        assert_eq!(bound_int[1].1, ScalarValue::Int64(Some(2)));
        assert_eq!(bound_int[2].1, ScalarValue::Decimal128(Some(1), 38, 38));

        // Out-of-i64 integer is refused cleanly
        let int_schema = json!({"type": "object", "properties": {"big": {"type": "integer"}}});
        let (p_big, l_big) = normalize_incoming_parameters(
            json!({"big": {"$number": "99999999999999999999"}})
                .as_object()
                .unwrap(),
            &Map::new(),
        )
        .unwrap();
        assert!(
            prepare_sql_parameters("SELECT :big", &p_big, &l_big, Some(&int_schema), None).is_err()
        );
    }
}
