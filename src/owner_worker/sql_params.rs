// Rust guideline compliant 2026-02-21
//! Recipe parameter JSON Schema validation and safe SQL literal substitution.

use super::exact::{BridgeError, valid_integer_lexeme, valid_json_number};
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
pub type NormalizedParameters = (BTreeMap<String, Value>, BTreeMap<String, String>);

/// Normalizes incoming `parameters` and `rawNumericParameters`, extracting any
/// `{ "$number": "<lex>" }` wrappers so both `docspec-owner.ts` and direct callers work.
pub fn normalize_incoming_parameters(
    raw_params: &Map<String, Value>,
    raw_numeric: &Map<String, Value>,
) -> Result<NormalizedParameters, BridgeError> {
    let mut params = BTreeMap::new();
    let mut lexemes = BTreeMap::new();
    for (k, v) in raw_numeric {
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
        "null" => val.is_null(),
        "boolean" => val.is_boolean(),
        "string" => val.is_string(),
        "integer" => {
            if let Some(lex) = raw_lex {
                valid_integer_lexeme(lex)
            } else if let Some(n) = val.as_i64() {
                let _ = n;
                true
            } else if let Some(n) = val.as_u64() {
                let _ = n;
                true
            } else if let Some(f) = val.as_f64() {
                f.is_finite() && f.fract() == 0.0
            } else {
                false
            }
        }
        "number" => value_numeric_lexeme(val, raw_lex).is_some(),
        "array" => val.is_array(),
        "object" => val.is_object(),
        _ => false,
    }
}

/// Validates recipe `parameters` against the recipe's JSON Schema object.
pub fn validate_parameters_schema(
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

fn sql_literal(
    value: &Value,
    prop_schema: Option<&Map<String, Value>>,
    raw_lexeme: Option<&str>,
) -> Result<String, BridgeError> {
    let declared_type = prop_schema
        .and_then(|p| p.get("type"))
        .and_then(|t| match t {
            Value::String(s) => Some(s.as_str()),
            Value::Array(arr) => arr.iter().filter_map(Value::as_str).find(|&s| s != "null"),
            _ => None,
        });

    if value.is_null() {
        return Ok("NULL".to_owned());
    }

    if declared_type == Some("boolean") || (declared_type.is_none() && value.is_boolean()) {
        let Some(b) = value.as_bool() else {
            return Err(BridgeError::new(
                "invalid_parameters",
                "Expected boolean recipe parameter.",
                400,
            ));
        };
        return Ok(if b {
            "TRUE".to_owned()
        } else {
            "FALSE".to_owned()
        });
    }

    if declared_type == Some("integer") {
        let lex_owned;
        let lex = if let Some(raw) = raw_lexeme {
            raw
        } else if let Some(i) = value.as_i64() {
            lex_owned = i.to_string();
            &lex_owned
        } else if let Some(u) = value.as_u64() {
            lex_owned = u.to_string();
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
        if let Ok(val_i64) = lex.parse::<i64>() {
            if val_i64 == i64::MIN {
                return Ok(format!("CAST('{val_i64}' AS BIGINT)"));
            }
            return Ok(format!("CAST({val_i64} AS BIGINT)"));
        }
        return Ok(lex.to_owned());
    }

    if declared_type == Some("number") || (declared_type.is_none() && value.is_number()) {
        let Some(lex) = value_numeric_lexeme(value, raw_lexeme) else {
            return Err(BridgeError::new(
                "invalid_parameters",
                "Expected exact numeric recipe parameter.",
                400,
            ));
        };
        if !lex.contains(['e', 'E']) {
            let unsigned = lex.strip_prefix('-').unwrap_or(lex.as_str());
            let (int_part, dot, frac_part) = match unsigned.split_once('.') {
                Some((i, f)) => (i, true, f),
                None => (unsigned, false, ""),
            };
            let combined = format!("{int_part}{frac_part}");
            let trimmed_sig = combined.trim_start_matches('0');
            let sig_len = if trimmed_sig.is_empty() {
                1
            } else {
                trimmed_sig.len()
            };
            let scale = if dot { frac_part.len() } else { 0 };
            let int_sig_len = int_part.trim_start_matches('0').len();
            let precision = (int_sig_len + scale).max(scale + 1).max(1);
            if sig_len > 15 && precision <= 38 {
                return Ok(format!("CAST('{lex}' AS DECIMAL({precision}, {scale}))"));
            }
        }
        return Ok(format!("CAST({lex} AS DOUBLE)"));
    }

    if declared_type == Some("string") || (declared_type.is_none() && value.is_string()) {
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
        let escaped = text.replace('\'', "''");
        return Ok(format!("'{escaped}'"));
    }

    Err(BridgeError::new(
        "invalid_parameters",
        format!("Unsupported recipe parameter type: {declared_type:?}"),
        400,
    ))
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Substitutes `?`, `$1..$N`, `:name`, and `$name` outside SQL strings, identifiers,
/// comments, and `::` cast operators.
pub fn bind_sql_parameters(
    sql_text: &str,
    parameters: &BTreeMap<String, Value>,
    raw_lexemes: &BTreeMap<String, String>,
    param_schema: Option<&Value>,
    parameter_order: Option<&[String]>,
) -> Result<String, BridgeError> {
    let props = param_schema
        .and_then(Value::as_object)
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object);
    let bytes = sql_text.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(n + 64);
    let mut i = 0usize;
    let mut qmark_index = 0usize;

    while i < n {
        let b = bytes[i];
        // Single-quoted string literal '...'
        if b == b'\'' {
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < n && bytes[i] == b'\'' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql_text[start..i]);
            continue;
        }
        // Double-quoted identifier "..."
        if b == b'"' {
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'"' {
                    i += 1;
                    if i < n && bytes[i] == b'"' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql_text[start..i]);
            continue;
        }
        // Line comment --
        if b == b'-' && i + 1 < n && bytes[i + 1] == b'-' {
            let start = i;
            i += 2;
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql_text[start..i]);
            continue;
        }
        // Block comment /* ... */
        if b == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = n.min(i + 2);
            out.push_str(&sql_text[start..i]);
            continue;
        }
        // Positional ? placeholder
        if b == b'?' {
            let Some(order) = parameter_order.filter(|o| qmark_index < o.len()) else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "Positional SQL placeholder has no matching parameterOrder entry.",
                    400,
                ));
            };
            let name = &order[qmark_index];
            qmark_index += 1;
            let Some(val) = parameters.get(name) else {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    format!("Missing value for parameter {name:?}."),
                    400,
                ));
            };
            let prop_map = props.and_then(|p| p.get(name)).and_then(Value::as_object);
            out.push_str(&sql_literal(
                val,
                prop_map,
                raw_lexemes.get(name).map(String::as_str),
            )?);
            i += 1;
            continue;
        }
        // PostgreSQL / DataFusion :: cast operator
        if b == b':' && i + 1 < n && bytes[i + 1] == b':' {
            out.push_str("::");
            i += 2;
            continue;
        }
        // $1..$N or $name or :name
        if (b == b':' || b == b'$') && i + 1 < n {
            if b == b'$' && matches!(bytes[i + 1], b'1'..=b'9') {
                let mut end = i + 2;
                while end < n && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end == n || !is_ident_continue(bytes[end]) {
                    let pos_1 = sql_text[i + 1..end].parse::<usize>().map_err(|_| {
                        BridgeError::new(
                            "invalid_parameters",
                            "Positional parameter index overflow.",
                            400,
                        )
                    })?;
                    let idx = pos_1 - 1;
                    let Some(order) = parameter_order.filter(|o| idx < o.len()) else {
                        return Err(BridgeError::new(
                            "invalid_parameters",
                            format!("Positional parameter ${pos_1} is out of range."),
                            400,
                        ));
                    };
                    let name = &order[idx];
                    let Some(val) = parameters.get(name) else {
                        return Err(BridgeError::new(
                            "invalid_parameters",
                            format!("Missing value for parameter {name:?}."),
                            400,
                        ));
                    };
                    let prop_map = props.and_then(|p| p.get(name)).and_then(Value::as_object);
                    out.push_str(&sql_literal(
                        val,
                        prop_map,
                        raw_lexemes.get(name).map(String::as_str),
                    )?);
                    i = end;
                    continue;
                }
            }
            if is_ident_start(bytes[i + 1]) {
                let mut end = i + 2;
                while end < n && is_ident_continue(bytes[end]) {
                    end += 1;
                }
                let name = &sql_text[i + 1..end];
                let Some(val) = parameters.get(name) else {
                    return Err(BridgeError::new(
                        "invalid_parameters",
                        format!("Missing value for parameter {name:?}."),
                        400,
                    ));
                };
                let prop_map = props.and_then(|p| p.get(name)).and_then(Value::as_object);
                out.push_str(&sql_literal(
                    val,
                    prop_map,
                    raw_lexemes.get(name).map(String::as_str),
                )?);
                i = end;
                continue;
            }
        }
        // Copy one UTF-8 character safely
        let ch = sql_text[i..].chars().next().expect("i < n");
        out.push(ch);
        i += ch.len_utf8();
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

    Ok(out)
}

pub fn strip_trailing_semicolon(sql_text: &str) -> String {
    let mut lines: Vec<&str> = sql_text.lines().collect();
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
    let joined = lines.join("\n");
    let trimmed = joined.trim_end();
    if let Some(without_semi) = trimmed.strip_suffix(';') {
        without_semi.trim_end().to_owned()
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn binds_positional_and_named_parameters_skipping_strings_comments_and_casts() {
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
            "region": "us'east",
            "min_id": { "$number": "9007199254740993" },
            "min_total": { "$number": "1234567890123456.7891" },
            "active": true
        });
        let (params, lexemes) =
            normalize_incoming_parameters(raw_p.as_object().unwrap(), &Map::new()).unwrap();
        validate_parameters_schema(&params, &lexemes, Some(&schema)).unwrap();

        let sql = "SELECT '?' AS lit, col::TEXT, \"$region\" -- ? $min_id\nFROM t /* :min_id */ WHERE r = ? AND id >= $min_id AND total >= :min_total AND ok = $4;";
        let order = vec![
            "region".to_owned(),
            "min_id".to_owned(),
            "min_total".to_owned(),
            "active".to_owned(),
        ];
        // Note: qmark_index == 1 != 4 fails positional count check when mixed with named;
        // pure named/dollar-positional or full ? list succeeds:
        assert!(bind_sql_parameters(sql, &params, &lexemes, Some(&schema), Some(&order)).is_err());

        let sql_named = "SELECT '?' AS lit, col::TEXT, \"$region\" -- $min_id\nFROM t /* :min_id */ WHERE r = $1 AND id >= $min_id AND total >= :min_total AND ok = $4;";
        let bound =
            bind_sql_parameters(sql_named, &params, &lexemes, Some(&schema), Some(&order)).unwrap();
        assert!(bound.contains("'us''east'"));
        assert!(bound.contains("CAST(9007199254740993 AS BIGINT)"));
        assert!(bound.contains("CAST('1234567890123456.7891' AS DECIMAL(20, 4))"));
        assert!(bound.contains("ok = TRUE"));
        assert!(!strip_trailing_semicolon(&bound).ends_with(';'));
    }
}
