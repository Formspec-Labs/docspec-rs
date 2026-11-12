//! Generic, bounded scalar primitives; callers retain application policy.
// Rust guideline compliant 2026-02-21
use crate::budget::Budget;
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, LargeListArray, LargeStringArray,
        ListArray, StringArray, StringBuilder, StringViewArray, StructArray,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, TimeUnit},
    json::writer::{EncoderOptions, make_encoder},
};
use datafusion::{
    common::{DataFusionError, Result},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
    prelude::SessionContext,
};
use percent_encoding::percent_decode_str;
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Resolve,
    Normalize,
    Decode,
    UrlFilename,
    DispositionFilename,
    JsonArray,
    JsonCanonical,
    HttpDate,
    IriEncode,
    NquadEscape,
}

pub(super) fn byte_limit(function: &ScalarUDF) -> Option<usize> {
    function
        .inner()
        .as_any()
        .downcast_ref::<GenericFunction>()
        .map(|function| function.budget.limits.max_batch_bytes)
        .or_else(|| super::plan::constants::byte_limit(function))
        .or_else(|| super::json_many::byte_limit(function))
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Resolve => "url_resolve",
            Self::Normalize => "uri_normalize_encoding",
            Self::Decode => "percent_decode_utf8",
            Self::UrlFilename => "url_filename",
            Self::DispositionFilename => "disposition_filename",
            Self::JsonArray => "json_array",
            Self::JsonCanonical => "json_canonical",
            Self::HttpDate => "http_date",
            Self::IriEncode => "iri_encode",
            Self::NquadEscape => "nquad_escape",
        }
    }
}

struct GenericFunction {
    kind: Kind,
    signature: Signature,
    budget: Budget,
}
impl fmt::Debug for GenericFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericFunction")
            .field("kind", &self.kind)
            .field("max_bytes", &self.budget.limits.max_batch_bytes)
            .finish()
    }
}
impl PartialEq for GenericFunction {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.budget.limits.max_batch_bytes == other.budget.limits.max_batch_bytes
    }
}
impl Eq for GenericFunction {}
impl Hash for GenericFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.budget.limits.max_batch_bytes.hash(state);
    }
}

pub(super) fn register(context: &mut SessionContext, budget: &Budget) -> crate::Result<()> {
    datafusion_functions_json::register_all(context)?;
    super::json_many::register(context, budget);
    for kind in [
        Kind::Resolve,
        Kind::Normalize,
        Kind::Decode,
        Kind::UrlFilename,
        Kind::DispositionFilename,
        Kind::JsonArray,
        Kind::JsonCanonical,
        Kind::HttpDate,
        Kind::IriEncode,
        Kind::NquadEscape,
    ] {
        let signature = if kind == Kind::JsonArray {
            Signature::one_of(
                vec![TypeSignature::Exact(vec![]), TypeSignature::VariadicAny],
                Volatility::Immutable,
            )
        } else {
            string_signature(if kind == Kind::Resolve { 2 } else { 1 })
        };
        context.register_udf(ScalarUDF::from(GenericFunction {
            kind,
            signature,
            budget: budget.clone(),
        }));
    }
    Ok(())
}

pub(super) fn string_signature(arity: usize) -> Signature {
    // Uniform signatures force mixed string representations to one type. That
    // can expand repeated views past i32 offsets before our admission runs.
    // Include untyped nulls so they cannot select a coercing fallback signature
    // for the other argument. text() already handles every null representation.
    let strings = [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Null,
    ];
    let variants = if arity == 1 {
        strings
            .iter()
            .map(|t| TypeSignature::Exact(vec![t.clone()]))
            .collect()
    } else {
        assert_eq!(arity, 2, "string primitives accept one or two inputs");
        strings
            .iter()
            .flat_map(|left| {
                strings
                    .iter()
                    .map(move |right| TypeSignature::Exact(vec![left.clone(), right.clone()]))
            })
            .collect()
    };
    Signature::one_of(variants, Volatility::Immutable)
}

pub(super) fn capacity() -> DataFusionError {
    DataFusionError::ResourcesExhausted("function output exceeds its byte allowance".into())
}
fn admit(budget: &Budget, bytes: usize) -> Result<tokio::sync::OwnedSemaphorePermit> {
    budget.reserve(bytes).map_err(|_| capacity())
}
fn json_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            json_type(f.data_type())
        }
        DataType::Struct(fields) => fields.iter().all(|f| json_type(f.data_type())),
        _ => false,
    }
}
pub(super) fn text(array: &dyn Array, index: usize) -> Result<Option<&str>> {
    if array.data_type() == &DataType::Null || array.is_null(index) {
        return Ok(None);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(a.value(index)));
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(a.value(index)));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(a.value(index)));
    }
    Err(DataFusionError::Execution(
        "string function received a non-string array".into(),
    ))
}
// Count standard JSON escaping before invoking Arrow's actual serializer. This
// is admission accounting, not a second serializer or identity implementation.
fn quoted_bytes(value: &str) -> usize {
    value.bytes().fold(2usize, |n, b| {
        n.saturating_add(match b {
            b'"' | b'\\' | 8 | 9 | 10 | 12 | 13 => 2,
            0..=31 => 6,
            _ => 1,
        })
    })
}
fn json_bytes(array: &dyn Array, index: usize) -> Result<usize> {
    if array.is_null(index) {
        return Ok(4);
    }
    match array.data_type() {
        DataType::Null => Ok(4),
        DataType::Boolean => Ok(
            if array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(index)
            {
                4
            } else {
                5
            },
        ),
        // Every supported signed/unsigned integer fits 21 JSON bytes, including a sign.
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(21),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(quoted_bytes(text(array, index)?.unwrap()))
        }
        DataType::Struct(_) => {
            let a = array.as_any().downcast_ref::<StructArray>().unwrap();
            a.columns()
                .iter()
                .zip(a.fields())
                .try_fold(2usize, |bytes, (column, field)| {
                    Ok(bytes
                        .saturating_add(quoted_bytes(field.name()))
                        .saturating_add(2)
                        .saturating_add(json_bytes(column.as_ref(), index)?))
                })
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = list.value_offsets();
            json_list_bytes(
                list.values().as_ref(),
                offsets[index] as usize,
                offsets[index + 1] as usize,
            )
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let offsets = list.value_offsets();
            json_list_bytes(
                list.values().as_ref(),
                offsets[index] as usize,
                offsets[index + 1] as usize,
            )
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let start = list.value_offset(index) as usize;
            json_list_bytes(
                list.values().as_ref(),
                start,
                start + list.value_length() as usize,
            )
        }
        _ => Err(DataFusionError::Plan(
            "unsupported JSON argument type".into(),
        )),
    }
}
fn json_list_bytes(array: &dyn Array, start: usize, end: usize) -> Result<usize> {
    (start..end).try_fold(2usize, |n, index| {
        Ok(n.saturating_add(1)
            .saturating_add(json_bytes(array, index)?))
    })
}

impl ScalarUDFImpl for GenericFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.kind.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if self.kind == Kind::JsonArray && !types.iter().all(json_type) {
            return Err(DataFusionError::Plan(
                "unsupported JSON argument type".into(),
            ));
        }
        Ok(if self.kind == Kind::HttpDate {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_bytes = args.args.iter().fold(0usize, |n, arg| {
            n.saturating_add(match arg {
                ColumnarValue::Scalar(v) => v.size().saturating_mul(2),
                ColumnarValue::Array(_) => 0,
            })
        });
        let _scalar_admission = admit(
            &self.budget,
            scalar_bytes.saturating_add(args.args.len().saturating_mul(512)),
        )?;
        // Scalars remain one element. Broadcasting them before admission could
        // allocate number_rows copies of a large value outside the byte bound.
        let inputs = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(a) => Ok((a.clone(), false)),
                ColumnarValue::Scalar(v) => Ok((v.to_array_of_size(1)?, true)),
            })
            .collect::<Result<Vec<_>>>()?;
        if self.kind == Kind::JsonArray {
            self.json(&inputs, args.number_rows)
        } else if self.kind == Kind::JsonCanonical {
            self.canonical(&inputs, args.number_rows)
        } else if self.kind == Kind::HttpDate {
            self.http_date(&inputs, args.number_rows)
        } else {
            self.strings(&inputs, args.number_rows)
        }
    }
}
impl GenericFunction {
    fn http_date(&self, inputs: &[(ArrayRef, bool)], rows: usize) -> Result<ColumnarValue> {
        if rows.saturating_mul(9) > self.budget.limits.max_batch_bytes {
            if rows > 1 {
                return Err(super::fitting::smaller(
                    self.budget.limits.max_batch_bytes / 9,
                ));
            }
            return Err(capacity());
        }
        let admission = admit(&self.budget, rows.saturating_mul(16).saturating_add(512))?;
        let mut builder = TimestampMicrosecondBuilder::with_capacity(rows);
        for index in 0..rows {
            let value = text(inputs[0].0.as_ref(), if inputs[0].1 { 0 } else { index })?
                .and_then(|value| httpdate::parse_http_date(value).ok())
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|duration| i64::try_from(duration.as_micros()).ok());
            builder.append_option(value);
        }
        admitted_output(Arc::new(builder.finish().with_timezone("UTC")), admission)
    }

    fn canonical(&self, inputs: &[(ArrayRef, bool)], rows: usize) -> Result<ColumnarValue> {
        let get = |index| text(inputs[0].0.as_ref(), if inputs[0].1 { 0 } else { index });
        let mut largest = 0usize;
        let mut output = 0usize;
        for index in 0..rows {
            if let Some(value) = get(index)? {
                largest = largest.max(value.len());
                output = output.saturating_add(value.len().saturating_mul(8).saturating_add(32));
            }
            output = output.saturating_add(5);
            if rows > 1 && output > self.budget.limits.max_batch_bytes {
                return Err(super::fitting::smaller(index));
            }
        }
        // One parsed tree at a time, including Vec growth and sorted object
        // references. Retained output capacities coexist with the Arrow builder.
        let admission = admit(
            &self.budget,
            largest
                .saturating_mul(128)
                .saturating_add(output.saturating_mul(2))
                .saturating_add(rows.saturating_mul(64))
                .saturating_add(512),
        )?;
        let mut values = Vec::with_capacity(rows);
        let mut total = 0usize;
        for index in 0..rows {
            let encoded = get(index)?
                .map(|value| {
                    super::canonical::encode(
                        value,
                        value
                            .len()
                            .saturating_mul(8)
                            .saturating_add(32)
                            .min(self.budget.limits.max_batch_bytes.saturating_sub(total)),
                    )
                })
                .transpose()
                .map_err(|error| match error {
                    DataFusionError::ResourcesExhausted(_) if rows > 1 => {
                        super::fitting::smaller(index)
                    }
                    error => error,
                })?;
            total = total.saturating_add(encoded.as_ref().map_or(0, String::len));
            if total > self.budget.limits.max_batch_bytes {
                if rows > 1 {
                    return Err(super::fitting::smaller(index));
                }
                return Err(capacity());
            }
            values.push(encoded);
        }
        let mut builder = StringBuilder::with_capacity(rows, total);
        for value in values {
            builder.append_option(value);
        }
        admitted_output(Arc::new(builder.finish()), admission)
    }
    fn json(&self, inputs: &[(ArrayRef, bool)], rows: usize) -> Result<ColumnarValue> {
        let limit = self.budget.limits.max_batch_bytes;
        let mut total = 0usize;
        let mut largest = 0usize;
        for index in 0..rows {
            let size =
                inputs.iter().try_fold(2usize, |n, (array, scalar)| {
                    Ok::<_, DataFusionError>(n.saturating_add(1).saturating_add(json_bytes(
                        array.as_ref(),
                        if *scalar { 0 } else { index },
                    )?))
                })?;
            total = total.checked_add(size).ok_or_else(capacity)?;
            if total
                .saturating_add((index + 1).saturating_mul(5))
                .saturating_add(128)
                > limit
                && rows > 1
            {
                return Err(super::fitting::smaller(index));
            }
            if total > limit {
                return Err(capacity());
            }
            largest = largest.max(size);
        }
        let fields = inputs
            .iter()
            .map(|(a, _)| Arc::new(Field::new("argument", a.data_type().clone(), true)))
            .collect::<Vec<_>>();
        let metadata = fields
            .iter()
            .fold(0usize, |n, f| n.saturating_add(f.size().saturating_mul(4)));
        let admission = admit(
            &self.budget,
            total
                .saturating_add(largest)
                .saturating_add((rows + 1).saturating_mul(4))
                .saturating_add(metadata)
                .saturating_add(512),
        )?;
        let options = EncoderOptions::default().with_explicit_nulls(true);
        let mut encoders = inputs
            .iter()
            .zip(&fields)
            .map(|((array, _), field)| make_encoder(field, array.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut builder = StringBuilder::with_capacity(rows, total);
        let mut encoded = Vec::with_capacity(largest);
        for index in 0..rows {
            encoded.clear();
            encoded.push(b'[');
            for (ordinal, ((array, scalar), encoder)) in
                inputs.iter().zip(&mut encoders).enumerate()
            {
                if ordinal > 0 {
                    encoded.push(b',');
                }
                let row = if *scalar { 0 } else { index };
                if array.is_null(row) {
                    encoded.extend_from_slice(b"null");
                } else {
                    encoder.encode(row, &mut encoded);
                }
            }
            encoded.push(b']');
            builder.append_value(std::str::from_utf8(&encoded).map_err(|_| {
                DataFusionError::Execution("JSON encoder returned invalid UTF-8".into())
            })?);
        }
        admitted_output(Arc::new(builder.finish()), admission)
    }
    fn strings(&self, inputs: &[(ArrayRef, bool)], rows: usize) -> Result<ColumnarValue> {
        let get = |arg: usize, index: usize| {
            text(
                inputs[arg].0.as_ref(),
                if inputs[arg].1 { 0 } else { index },
            )
        };
        // Reserve retained capacities as well as encoded lengths. Resolution
        // can hold an expanded base and a geometrically grown relative result
        // together. The final Arrow builder is present alongside those values.
        let mut allowance = 0usize;
        for index in 0..rows {
            let input = get(0, index)?.map_or(0, str::len);
            let base = if self.kind == Kind::Resolve {
                get(1, index)?.map_or(0, str::len)
            } else {
                0
            };
            let factor = match self.kind {
                Kind::Resolve => 8,
                Kind::NquadEscape => 6,
                Kind::IriEncode => 3,
                _ => 4,
            };
            allowance = allowance.saturating_add(
                input
                    .saturating_add(base)
                    .saturating_mul(factor)
                    .saturating_add(64),
            );
            if rows > 1 && allowance > self.budget.limits.max_batch_bytes {
                return Err(super::fitting::smaller(index));
            }
        }
        let admission = admit(
            &self.budget,
            allowance
                .saturating_mul(2)
                .saturating_add(rows.saturating_mul(40))
                .saturating_add(512),
        )?;
        let mut values = Vec::with_capacity(rows);
        let mut bytes = 0usize;
        for index in 0..rows {
            let value = match get(0, index)? {
                None => None,
                Some(value) => match self.kind {
                    Kind::Resolve => resolve(value, get(1, index)?),
                    Kind::Normalize => Some(normalize(value)),
                    Kind::Decode => Some(decode(value)),
                    Kind::IriEncode => Some(iri_encode(value)),
                    Kind::NquadEscape => Some(nquad_escape(value)),
                    Kind::UrlFilename => url::Url::parse(value).ok().and_then(|u| {
                        u.path_segments()
                            .and_then(|mut segments| segments.next_back())
                            .filter(|s| !s.is_empty())
                            .map(decode)
                    }),
                    Kind::DispositionFilename => disposition(value),
                    Kind::JsonArray | Kind::JsonCanonical | Kind::HttpDate => unreachable!(),
                },
            };
            bytes = bytes
                .checked_add(value.as_ref().map_or(0, String::len))
                .ok_or_else(capacity)?;
            if bytes > self.budget.limits.max_batch_bytes {
                if rows > 1 {
                    return Err(super::fitting::smaller(index));
                }
                return Err(capacity());
            }
            values.push(value);
        }
        let mut builder = StringBuilder::with_capacity(rows, bytes);
        for value in values {
            builder.append_option(value);
        }
        admitted_output(Arc::new(builder.finish()), admission)
    }
}
fn admitted_output(
    array: ArrayRef,
    memory: tokio::sync::OwnedSemaphorePermit,
) -> Result<ColumnarValue> {
    crate::budget::admit_array(array, memory)
        .map(ColumnarValue::Array)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
fn resolve(reference: &str, base: Option<&str>) -> Option<String> {
    let base = match base {
        Some(value) => Some(url::Url::parse(value).ok()?),
        None => None,
    };
    url::Url::options()
        .base_url(base.as_ref())
        .parse(reference)
        .ok()
        .map(String::from)
}
fn decode(value: &str) -> String {
    percent_decode_str(value)
        .decode_utf8()
        .map_or_else(|_| value.to_owned(), |v| v.into_owned())
}
fn unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"-._~".contains(&byte)
}
fn iri_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let bytes = value.as_bytes();
    let out_len = bytes.iter().fold(0usize, |acc, &byte| {
        acc.saturating_add(if unreserved(byte) { 1 } else { 3 })
    });
    let mut out = String::with_capacity(out_len);
    for &byte in bytes {
        if unreserved(byte) {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0F) as usize] as char);
        }
    }
    out
}
fn nquad_escape(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let out_len = value.bytes().fold(0usize, |acc, byte| {
        acc.saturating_add(match byte {
            b'\\' | b'"' | b'\n' | b'\r' | b'\t' | 0x08 | 0x0C => 2,
            0x00..=0x1F | 0x7F => 6,
            _ => 1,
        })
    });
    let mut out = String::with_capacity(out_len);
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0C' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7F => {
                let byte = c as u8;
                out.push_str("\\u00");
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out
}
fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
fn normalize(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(bytes.len().saturating_mul(3));
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%'
            && index + 2 < bytes.len()
            && let Some(decoded) = hex(bytes[index + 1])
                .zip(hex(bytes[index + 2]))
                .map(|(a, b)| a * 16 + b)
        {
            if unreserved(decoded) {
                out.push(decoded as char);
            } else {
                out.push('%');
                out.push(HEX[(decoded >> 4) as usize] as char);
                out.push(HEX[(decoded & 15) as usize] as char);
            }
            index += 3;
            continue;
        }
        if unreserved(byte) || b":/?#[]@!$&'()*+,;=".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 15) as usize] as char);
        }
        index += 1;
    }
    out
}
fn extended(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next()?;
    parts.next()?;
    let encoded = parts.next()?;
    let bytes = encoded.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() || hex(bytes[i + 1]).is_none() || hex(bytes[i + 2]).is_none() {
                return None;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    let decoded = percent_decode_str(encoded).collect::<Vec<_>>();
    if charset.eq_ignore_ascii_case("utf-8") {
        String::from_utf8(decoded).ok()
    } else if charset.eq_ignore_ascii_case("iso-8859-1") {
        Some(decoded.into_iter().map(char::from).collect())
    } else {
        None
    }
}
fn disposition(value: &str) -> Option<String> {
    let mut i = value.find(';')? + 1;
    let bytes = value.as_bytes();
    let mut plain = None;
    let mut encoded = None;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b';') {
            i += 1;
        }
        let start = i;
        while i < bytes.len() && bytes[i] != b'=' && bytes[i] != b';' {
            i += 1;
        }
        if i == bytes.len() || bytes[i] != b'=' {
            return None;
        }
        let name = value[start..i].trim();
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let mut raw = Vec::new();
        if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                let byte = bytes[i];
                i += 1;
                if byte == b'"' {
                    closed = true;
                    break;
                }
                if byte == b'\\' {
                    if i == bytes.len() {
                        return None;
                    }
                    raw.push(bytes[i]);
                    i += 1;
                } else {
                    raw.push(byte);
                }
            }
            if !closed {
                return None;
            }
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] != b';' {
                return None;
            }
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b';' {
                i += 1;
            }
            raw.extend_from_slice(value[start..i].trim().as_bytes());
        }
        let parsed = String::from_utf8(raw).ok()?;
        if name.eq_ignore_ascii_case("filename") {
            if plain.is_some() {
                return None;
            }
            plain = Some(parsed);
        } else if name.eq_ignore_ascii_case("filename*") {
            if encoded.is_some() {
                return None;
            }
            encoded = Some(parsed);
        }
    }
    encoded
        .as_deref()
        .and_then(extended)
        .or(plain)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    #[test]
    fn output_buffers_keep_admission_after_array_and_function_drop() {
        let budget = Budget::new(crate::budget::Limits {
            memory_bytes: 1 << 20,
            max_batch_bytes: 64 << 10,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        for kind in [
            Kind::Normalize,
            Kind::IriEncode,
            Kind::NquadEscape,
            Kind::JsonCanonical,
            Kind::JsonArray,
            Kind::HttpDate,
        ] {
            let function = GenericFunction {
                kind,
                signature: Signature::variadic_any(Volatility::Immutable),
                budget: budget.clone(),
            };
            let value = match kind {
                Kind::JsonCanonical => r#"{"key":"value"}"#,
                Kind::HttpDate => "Sun, 06 Nov 1994 08:49:37 GMT",
                _ => "value with spaces",
            };
            let inputs = [(Arc::new(StringArray::from(vec![value])) as ArrayRef, false)];
            let output = match kind {
                Kind::JsonCanonical => function.canonical(&inputs, 1),
                Kind::JsonArray => function.json(&inputs, 1),
                Kind::HttpDate => function.http_date(&inputs, 1),
                _ => function.strings(&inputs, 1),
            }
            .unwrap();
            let ColumnarValue::Array(array) = output else {
                panic!("array output expected")
            };
            let buffer = array.slice(0, 1).to_data().buffers()[0].clone();
            drop(array);
            drop(function);
            assert!(
                budget.reserve(budget.limits.memory_bytes / 2).is_err(),
                "{kind:?}"
            );
            drop(buffer);
            drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
        }
    }

    #[test]
    fn iri_encode_and_nquad_escape_match_rfc3986_and_w3c_nquads() {
        assert_eq!(
            iri_encode("AZaz09-._~ /?%#é🚀"),
            "AZaz09-._~%20%2F%3F%25%23%C3%A9%F0%9F%9A%80"
        );
        assert_eq!(iri_encode(""), "");
        assert_eq!(
            nquad_escape(
                "quote \" slash \\ nl \n cr \r tab \t bs \x08 ff \x0C nul \x00 us \x1F del \x7F utf8 é🚀"
            ),
            r#"quote \" slash \\ nl \n cr \r tab \t bs \b ff \f nul \u0000 us \u001F del \u007F utf8 é🚀"#
        );
        assert_eq!(nquad_escape(""), "");
    }
}
