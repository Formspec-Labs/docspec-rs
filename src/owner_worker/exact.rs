// Rust guideline compliant 2026-02-21
//! Lossless `ddk-exact-json-v1` Arrow-to-JSON encoding and temporal/numeric lexemes.

use arrow::{
    array::{
        Array, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray,
        Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray, ListArray,
        ListViewArray, MapArray, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use base64::Engine as _;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

pub const MAX_EXACT_NODES: usize = 300_000;
pub const MAX_EXACT_DEPTH: usize = 48;
pub const MAX_EXACT_STRING_BYTES: usize = 1_000_000;
pub const MAX_EXACT_NUMBER_LEN: usize = 1024;

/// Structured protocol error mapped to `{ code, message, status }` on stdout.
#[derive(Debug, Clone)]
pub struct BridgeError {
    pub code: &'static str,
    pub message: String,
    pub status: u16,
}

impl BridgeError {
    pub fn new(code: &'static str, message: impl Into<String>, status: u16) -> Self {
        Self {
            code,
            message: message.into(),
            status,
        }
    }
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BridgeError {}

/// Mutable node counter enforcing structure and depth budgets across a response page.
#[derive(Debug, Default)]
pub struct ExactBudget {
    pub nodes: usize,
}

impl ExactBudget {
    pub fn step(&mut self, depth: usize) -> Result<(), BridgeError> {
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > MAX_EXACT_NODES || depth > MAX_EXACT_DEPTH {
            return Err(BridgeError::new(
                "adapter_budget",
                "The owner page exceeds the supported structure budget.",
                413,
            ));
        }
        Ok(())
    }
}

/// Validates a JSON number lexeme against `/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/`
/// and the `ddk-exact-json-v1` length and exponent bounds.
pub fn valid_json_number(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_EXACT_NUMBER_LEN {
        return false;
    }
    let bytes = value.as_bytes();
    let mut i = 0usize;
    if bytes[i] == b'-' {
        i += 1;
        if i == bytes.len() {
            return false;
        }
    }
    if bytes[i] == b'0' {
        i += 1;
        if i < bytes.len() && bytes[i].is_ascii_digit() {
            return false;
        }
    } else if matches!(bytes[i], b'1'..=b'9') {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return false;
    }

    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        if i == bytes.len() || !bytes[i].is_ascii_digit() {
            return false;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }

    if i < bytes.len() && matches!(bytes[i], b'e' | b'E') {
        i += 1;
        let exp_sign_start = i;
        if i < bytes.len() && matches!(bytes[i], b'+' | b'-') {
            i += 1;
        }
        if i == bytes.len() || !bytes[i].is_ascii_digit() {
            return false;
        }
        let digits_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let exp_slice = &value[exp_sign_start..i];
        if exp_slice.len() >= 6 {
            return false;
        }
        let Ok(exp_mag) = value[digits_start..i].parse::<u32>() else {
            return false;
        };
        if exp_mag > 1000 {
            return false;
        }
    }

    i == bytes.len()
}

pub fn valid_integer_lexeme(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_EXACT_NUMBER_LEN {
        return false;
    }
    let bytes = value.as_bytes();
    let start = if bytes[0] == b'-' { 1 } else { 0 };
    if start >= bytes.len() {
        return false;
    }
    if bytes[start] == b'0' {
        return start + 1 == bytes.len();
    }
    matches!(bytes[start], b'1'..=b'9') && bytes[start + 1..].iter().all(u8::is_ascii_digit)
}

fn exact_number(lexeme: String) -> Result<Value, BridgeError> {
    if !valid_json_number(&lexeme) {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Numeric value exceeds exact JSON number bounds.",
            422,
        ));
    }
    Ok(json!({ "$number": lexeme }))
}

/// Formats an unscaled integer string and Arrow Decimal `scale` into the exact
/// fixed-point or scientific lexeme produced by `docspec-owner-bridge.py:_decimal_lexeme`.
pub fn decimal_lexeme_from_raw(raw_unscaled: &str, scale: i8) -> Result<String, BridgeError> {
    let (neg, raw_digits) = match raw_unscaled.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw_unscaled),
    };
    let stripped = raw_digits.trim_start_matches('0');
    let digits = if stripped.is_empty() { "0" } else { stripped };
    // Non-zero check: Arrow integer representations of 0 are never signed (-0),
    // so `neg` is only true when `digits != "0"`.
    let sign = if neg && digits != "0" { "-" } else { "" };
    let exp = -i32::from(scale);

    let text = if (-30..=0).contains(&exp) {
        let s = (-exp) as usize;
        if s == 0 {
            format!("{sign}{digits}")
        } else if digits == "0" {
            format!("0.{}", "0".repeat(s))
        } else if digits.len() <= s {
            let zeros = "0".repeat(s - digits.len());
            format!("{sign}0.{zeros}{digits}")
        } else {
            let split = digits.len() - s;
            format!("{sign}{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        // IEEE 754 / Python `str(Decimal)` formatting outside -30 <= exp <= 0
        let adj = (digits.len() as i32) + exp - 1;
        if exp <= 0 && adj >= -6 {
            let s = (-exp) as usize;
            if digits.len() <= s {
                let zeros = "0".repeat(s - digits.len());
                format!("{sign}0.{zeros}{digits}")
            } else {
                let split = digits.len() - s;
                format!("{sign}{}.{}", &digits[..split], &digits[split..])
            }
        } else {
            let first = &digits[..1];
            let rest = &digits[1..];
            let coeff = if rest.is_empty() {
                first.to_owned()
            } else {
                format!("{first}.{rest}")
            };
            format!("{sign}{coeff}E{adj:+}")
        }
    };

    if !valid_json_number(&text) {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Numeric value exceeds exact JSON number bounds.",
            422,
        ));
    }
    Ok(text)
}

fn float_lexeme_f32(value: f32) -> Result<String, BridgeError> {
    if !value.is_finite() {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Non-finite floating-point value cannot be serialized.",
            422,
        ));
    }
    if value == 0.0 && value.is_sign_negative() {
        return Ok("-0".to_owned());
    }
    let mut buf = ryu::Buffer::new();
    let text = buf.format_finite(value).to_owned();
    if !valid_json_number(&text) {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Floating-point value exceeds exact JSON bounds.",
            422,
        ));
    }
    Ok(text)
}

fn float_lexeme_f64(value: f64) -> Result<String, BridgeError> {
    if !value.is_finite() {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Non-finite floating-point value cannot be serialized.",
            422,
        ));
    }
    if value == 0.0 && value.is_sign_negative() {
        return Ok("-0".to_owned());
    }
    let mut buf = ryu::Buffer::new();
    let text = buf.format_finite(value).to_owned();
    if !valid_json_number(&text) {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Floating-point value exceeds exact JSON bounds.",
            422,
        ));
    }
    Ok(text)
}

/// Howard Hinnant's civil-from-days proleptic Gregorian calendar conversion.
/// Input `days` is days since `1970-01-01`.
fn civil_from_days(days: i64) -> Result<(i32, u32, u32), BridgeError> {
    let z = days.checked_add(719_468).ok_or_else(|| {
        BridgeError::new(
            "owner_unsupported_type",
            "Date value overflows calendar range.",
            422,
        )
    })?;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = if m <= 2 { y + 1 } else { y };
    if !(1..=9999).contains(&year) {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            format!("Calendar year {year} is outside supported ISO range 0001..9999."),
            422,
        ));
    }
    Ok((year as i32, m, d))
}

fn format_date_days(days: i64) -> Result<Value, BridgeError> {
    let (year, month, day) = civil_from_days(days)?;
    Ok(Value::String(format!("{year:04}-{month:02}-{day:02}")))
}

fn unit_scale_and_digits(unit: TimeUnit) -> (i64, usize) {
    match unit {
        TimeUnit::Second => (1, 0),
        TimeUnit::Millisecond => (1_000, 3),
        TimeUnit::Microsecond => (1_000_000, 6),
        TimeUnit::Nanosecond => (1_000_000_000, 9),
    }
}

fn format_fraction(rem: i64, digits: usize) -> String {
    if digits == 0 || rem == 0 {
        return String::new();
    }
    let formatted = format!(".{rem:0width$}", width = digits);
    let trimmed = formatted.trim_end_matches('0');
    if trimmed == "." {
        String::new()
    } else {
        trimmed.to_owned()
    }
}

pub fn format_epoch_timestamp(
    raw: i64,
    unit: TimeUnit,
    tz: Option<&str>,
) -> Result<String, BridgeError> {
    let (scale, digits) = unit_scale_and_digits(unit);
    let seconds = raw.div_euclid(scale);
    let rem = raw.rem_euclid(scale);
    let days = seconds.div_euclid(86_400);
    let sec_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days)?;
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;
    let frac = format_fraction(rem, digits);
    let suffix = match tz {
        Some(z) if !z.is_empty() => {
            if z.eq_ignore_ascii_case("UTC")
                || z == "+00:00"
                || z.eq_ignore_ascii_case("Z")
                || z.eq_ignore_ascii_case("Etc/UTC")
            {
                "Z".to_owned()
            } else {
                format!(" [{z}]")
            }
        }
        _ => String::new(),
    };
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{frac}{suffix}"
    ))
}

pub fn format_time_of_day(raw: i64, unit: TimeUnit) -> Result<String, BridgeError> {
    let (scale, digits) = unit_scale_and_digits(unit);
    if raw < 0 {
        return Err(BridgeError::new(
            "owner_unsupported_type",
            "Negative time-of-day value cannot be serialized.",
            422,
        ));
    }
    let total_seconds = raw.div_euclid(scale);
    let rem = raw.rem_euclid(scale);
    let hours = total_seconds / 3600;
    let rem_s = total_seconds % 3600;
    let minutes = rem_s / 60;
    let seconds = rem_s % 60;
    let frac = format_fraction(rem, digits);
    Ok(format!("{hours:02}:{minutes:02}:{seconds:02}{frac}"))
}

pub fn wrap_exact_object(entries: Vec<(String, Value)>) -> Result<Value, BridgeError> {
    let mut seen = BTreeSet::new();
    let mut needs_escape = false;
    for (key, _) in &entries {
        if !seen.insert(key.as_str()) {
            return Err(BridgeError::new(
                "adapter_transport",
                "Duplicate field names in struct cannot be serialized.",
                409,
            ));
        }
        if key == "$number" || key == "$object" {
            needs_escape = true;
        }
    }
    if needs_escape {
        let pairs: Vec<Value> = entries
            .into_iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k), v]))
            .collect();
        return Ok(json!({ "$object": pairs }));
    }
    let mut map = Map::with_capacity(entries.len());
    for (k, v) in entries {
        map.insert(k, v);
    }
    Ok(Value::Object(map))
}

fn encode_binary(bytes: &[u8]) -> Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    json!({
        "type": "binary",
        "encoding": "base64",
        "value": b64,
    })
}

fn encode_string(text: &str) -> Result<Value, BridgeError> {
    if text.len() > MAX_EXACT_STRING_BYTES {
        return Err(BridgeError::new(
            "adapter_budget",
            "An owner string exceeds the supported value budget.",
            413,
        ));
    }
    Ok(Value::String(text.to_owned()))
}

fn downcast_err(data_type: &DataType) -> BridgeError {
    BridgeError::new(
        "owner_unsupported_type",
        format!("Arrow array physical layout disagreed with declared type {data_type}"),
        422,
    )
}

/// Encodes one cell of an Arrow [`Array`] into lossless `ddk-exact-json-v1`.
pub fn encode_array_value(
    array: &dyn Array,
    index: usize,
    depth: usize,
    budget: &mut ExactBudget,
) -> Result<Value, BridgeError> {
    budget.step(depth)?;
    let dtype = array.data_type();
    if dtype == &DataType::Null || array.is_null(index) {
        return Ok(Value::Null);
    }
    match dtype {
        DataType::Null => Ok(Value::Null),
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            Ok(Value::Bool(a.value(index)))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::UInt8 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::UInt16 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            exact_number(a.value(index).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            let lex = decimal_lexeme_from_raw(&a.value(index).to_string(), *scale)?;
            exact_number(lex)
        }
        DataType::Decimal256(_, scale) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            let lex = decimal_lexeme_from_raw(&a.value(index).to_string(), *scale)?;
            exact_number(lex)
        }
        DataType::Float16 => {
            let a = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            let lex = float_lexeme_f32(a.value(index).to_f32())?;
            exact_number(lex)
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            let lex = float_lexeme_f32(a.value(index))?;
            exact_number(lex)
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            let lex = float_lexeme_f64(a.value(index))?;
            exact_number(lex)
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            encode_string(a.value(index))
        }
        DataType::LargeUtf8 => {
            let a = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            encode_string(a.value(index))
        }
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            encode_string(a.value(index))
        }
        DataType::Binary => {
            let a = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            Ok(encode_binary(a.value(index)))
        }
        DataType::LargeBinary => {
            let a = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            Ok(encode_binary(a.value(index)))
        }
        DataType::FixedSizeBinary(_) => {
            let a = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            Ok(encode_binary(a.value(index)))
        }
        DataType::BinaryView => {
            let a = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            Ok(encode_binary(a.value(index)))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            format_date_days(i64::from(a.value(index)))
        }
        DataType::Date64 => {
            let a = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| downcast_err(dtype))?;
            format_date_days(a.value(index).div_euclid(86_400_000))
        }
        DataType::Timestamp(unit, tz) => {
            let raw = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
            };
            Ok(Value::String(format_epoch_timestamp(
                raw,
                *unit,
                tz.as_deref(),
            )?))
        }
        DataType::Time32(unit) => {
            let raw = match unit {
                TimeUnit::Second => i64::from(
                    array
                        .as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .ok_or_else(|| downcast_err(dtype))?
                        .value(index),
                ),
                TimeUnit::Millisecond => i64::from(
                    array
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .ok_or_else(|| downcast_err(dtype))?
                        .value(index),
                ),
                _ => return Err(downcast_err(dtype)),
            };
            Ok(Value::String(format_time_of_day(raw, *unit)?))
        }
        DataType::Time64(unit) => {
            let raw = match unit {
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                _ => return Err(downcast_err(dtype)),
            };
            Ok(Value::String(format_time_of_day(raw, *unit)?))
        }
        DataType::Duration(unit) => {
            let raw = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<DurationSecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .ok_or_else(|| downcast_err(dtype))?
                    .value(index),
            };
            exact_number(raw.to_string())
        }
        DataType::List(_) => {
            let a = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let offsets = a.value_offsets();
            let start = offsets[index] as usize;
            let end = offsets[index + 1] as usize;
            encode_list_range(a.values().as_ref(), start, end, depth + 1, budget)
        }
        DataType::LargeList(_) => {
            let a = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let offsets = a.value_offsets();
            let start = offsets[index] as usize;
            let end = offsets[index + 1] as usize;
            encode_list_range(a.values().as_ref(), start, end, depth + 1, budget)
        }
        DataType::FixedSizeList(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let start = a.value_offset(index) as usize;
            let len = a.value_length() as usize;
            encode_list_range(a.values().as_ref(), start, start + len, depth + 1, budget)
        }
        DataType::ListView(_) => {
            let a = array
                .as_any()
                .downcast_ref::<ListViewArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let start = a.value_offset(index) as usize;
            let len = a.value_size(index) as usize;
            encode_list_range(a.values().as_ref(), start, start + len, depth + 1, budget)
        }
        DataType::LargeListView(_) => {
            let a = array
                .as_any()
                .downcast_ref::<LargeListViewArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let start = a.value_offset(index) as usize;
            let len = a.value_size(index) as usize;
            encode_list_range(a.values().as_ref(), start, start + len, depth + 1, budget)
        }
        DataType::Map(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let offsets = a.value_offsets();
            let start = offsets[index] as usize;
            let end = offsets[index + 1] as usize;
            let keys = a.keys().as_ref();
            let vals = a.values().as_ref();
            let mut pairs = Vec::with_capacity(end.saturating_sub(start));
            for elem_idx in start..end {
                let k = encode_array_value(keys, elem_idx, depth + 1, budget)?;
                let v = encode_array_value(vals, elem_idx, depth + 1, budget)?;
                pairs.push(Value::Array(vec![k, v]));
            }
            Ok(Value::Array(pairs))
        }
        DataType::Struct(_) => {
            let a = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err(dtype))?;
            let mut entries = Vec::with_capacity(a.num_columns());
            for (field, col) in a.fields().iter().zip(a.columns()) {
                let val = encode_array_value(col.as_ref(), index, depth + 1, budget)?;
                entries.push((field.name().clone(), val));
            }
            wrap_exact_object(entries)
        }
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any_dictionary_opt()
                .ok_or_else(|| downcast_err(dtype))?;
            let val_idx = dict_key_index(dict.keys(), index)
                .filter(|&k| k < dict.values().len())
                .ok_or_else(|| downcast_err(dtype))?;
            encode_array_value(dict.values().as_ref(), val_idx, depth, budget)
        }
        other => Err(BridgeError::new(
            "owner_unsupported_type",
            format!("Unsupported Arrow value type: {other}"),
            422,
        )),
    }
}

fn dict_key_index(keys: &dyn Array, index: usize) -> Option<usize> {
    let any = keys.as_any();
    if let Some(a) = any.downcast_ref::<Int8Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int16Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int32Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<UInt8Array>() {
        Some(usize::from(a.value(index)))
    } else if let Some(a) = any.downcast_ref::<UInt16Array>() {
        Some(usize::from(a.value(index)))
    } else if let Some(a) = any.downcast_ref::<UInt32Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<UInt64Array>() {
        usize::try_from(a.value(index)).ok()
    } else {
        None
    }
}

fn encode_list_range(
    values: &dyn Array,
    start: usize,
    end: usize,
    depth: usize,
    budget: &mut ExactBudget,
) -> Result<Value, BridgeError> {
    let mut out = Vec::with_capacity(end.saturating_sub(start));
    for idx in start..end {
        out.push(encode_array_value(values, idx, depth, budget)?);
    }
    Ok(Value::Array(out))
}

/// Encodes one row (`row_idx`) of a [`RecordBatch`] into an exact `ddk-exact-json-v1` object.
pub fn encode_batch_row(
    batch: &RecordBatch,
    row_idx: usize,
    budget: &mut ExactBudget,
) -> Result<Value, BridgeError> {
    let schema = batch.schema();
    let fields = schema.fields();
    let mut entries = Vec::with_capacity(batch.num_columns());
    for (col_idx, field) in fields.iter().enumerate() {
        let val = encode_array_value(batch.column(col_idx).as_ref(), row_idx, 1, budget)?;
        entries.push((field.name().clone(), val));
    }
    wrap_exact_object(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_lexemes_preserve_trailing_zeros_and_signs() {
        assert_eq!(
            decimal_lexeme_from_raw("123456700", 4).unwrap(),
            "12345.6700"
        );
        assert_eq!(decimal_lexeme_from_raw("-50", 4).unwrap(), "-0.0050");
        assert_eq!(decimal_lexeme_from_raw("0", 4).unwrap(), "0.0000");
        assert_eq!(
            decimal_lexeme_from_raw("12345678901234567891", 4).unwrap(),
            "1234567890123456.7891"
        );
        assert_eq!(decimal_lexeme_from_raw("-42", 0).unwrap(), "-42");
        assert_eq!(decimal_lexeme_from_raw("123", -2).unwrap(), "1.23E+4");
    }

    #[test]
    fn timestamps_and_times_match_python_bridge_formatting() {
        assert_eq!(
            format_epoch_timestamp(1_794_406_516_123_456_789, TimeUnit::Nanosecond, Some("UTC"))
                .unwrap(),
            "2026-11-11T14:15:16.123456789Z"
        );
        assert_eq!(
            format_epoch_timestamp(0, TimeUnit::Second, None).unwrap(),
            "1970-01-01T00:00:00"
        );
        assert_eq!(
            format_epoch_timestamp(-1_500, TimeUnit::Millisecond, Some("America/New_York"))
                .unwrap(),
            "1969-12-31T23:59:58.5 [America/New_York]"
        );
        assert_eq!(
            format_time_of_day(3_723_050_000, TimeUnit::Microsecond).unwrap(),
            "01:02:03.05"
        );
    }
}
