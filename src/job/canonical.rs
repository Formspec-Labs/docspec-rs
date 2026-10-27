//! Canonical JSON uses maintained parsing and serialization, with Python float spelling.
// Rust guideline compliant 2026-02-21
use jiter::JsonValue;
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeMap, SerializeSeq},
};
use std::{
    collections::BTreeMap,
    io::{self, Write},
};

struct Ordered<'a, 'b>(&'a JsonValue<'b>);
impl Serialize for Ordered<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            JsonValue::Null => serializer.serialize_none(),
            JsonValue::Bool(value) => serializer.serialize_bool(*value),
            JsonValue::Int(value) => serializer.serialize_i64(*value),
            JsonValue::BigInt(value) => {
                // RawValue is a maintained numeric pass-through, not a Value
                // map: ordinary object keys cannot become private number tags.
                let raw = serde_json::value::RawValue::from_string(value.to_string())
                    .map_err(S::Error::custom)?;
                raw.serialize(serializer)
            }
            JsonValue::Float(value) if value.is_finite() => serializer.serialize_f64(*value),
            JsonValue::Float(_) => Err(S::Error::custom("nonfinite canonical JSON number")),
            JsonValue::Str(value) => serializer.serialize_str(value),
            JsonValue::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values.iter() {
                    sequence.serialize_element(&Ordered(value))?;
                }
                sequence.end()
            }
            JsonValue::Object(values) => {
                // Rust str order is UTF-8 byte order. Later duplicate keys win,
                // matching the normative Python json.loads interpretation.
                let ordered = values
                    .iter()
                    .map(|(key, value)| (key.as_ref(), value))
                    .collect::<BTreeMap<_, _>>();
                let mut map = serializer.serialize_map(Some(ordered.len()))?;
                for (key, value) in ordered {
                    map.serialize_entry(key, &Ordered(value))?;
                }
                map.end()
            }
        }
    }
}
struct PythonFloat;
impl serde_json::ser::Formatter for PythonFloat {
    fn write_f64<W: ?Sized + Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        if !value.is_finite() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "nonfinite canonical JSON number",
            ));
        }
        let mut buffer = ryu::Buffer::new();
        let shortest = buffer.format_finite(value);
        let (sign, positive) = shortest
            .strip_prefix('-')
            .map_or(("", shortest), |s| ("-", s));
        let (mantissa, power) = positive.split_once('e').map_or((positive, 0), |(m, e)| {
            (m, e.parse::<i32>().expect("ryu exponent"))
        });
        let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
        let mut digits = mantissa
            .bytes()
            .filter(|byte| *byte != b'.')
            .map(char::from)
            .collect::<String>();
        let leading = digits.bytes().take_while(|byte| *byte == b'0').count();
        let exponent = point + power - leading as i32 - 1;
        digits.drain(..leading);
        while digits.ends_with('0') {
            digits.pop();
        }
        writer.write_all(sign.as_bytes())?;
        if digits.is_empty() {
            return writer.write_all(b"0.0");
        }
        if !(-4..16).contains(&exponent) {
            writer.write_all(&digits.as_bytes()[..1])?;
            if digits.len() > 1 {
                writer.write_all(b".")?;
                writer.write_all(&digits.as_bytes()[1..])?;
            }
            write!(writer, "e{exponent:+03}")
        } else {
            let point = exponent + 1;
            if point <= 0 {
                writer.write_all(b"0.")?;
                for _ in point..0 {
                    writer.write_all(b"0")?;
                }
                writer.write_all(digits.as_bytes())
            } else if point as usize >= digits.len() {
                writer.write_all(digits.as_bytes())?;
                for _ in digits.len()..point as usize {
                    writer.write_all(b"0")?;
                }
                writer.write_all(b".0")
            } else {
                let point = point as usize;
                writer.write_all(&digits.as_bytes()[..point])?;
                writer.write_all(b".")?;
                writer.write_all(&digits.as_bytes()[point..])
            }
        }
    }
}
struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "canonical JSON output exceeds byte allowance",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn encode(value: &str, allowance: usize) -> datafusion::common::Result<String> {
    let invalid = |_| {
        datafusion::common::DataFusionError::Execution(
            "invalid canonical JSON input or output".into(),
        )
    };
    let parsed = JsonValue::parse(value.as_bytes(), false).map_err(invalid)?;
    let mut output = Bounded {
        bytes: Vec::with_capacity(allowance),
        limit: allowance,
    };
    Ordered(&parsed)
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut output,
            PythonFloat,
        ))
        .map_err(|error| {
            if error.io_error_kind() == Some(io::ErrorKind::OutOfMemory) {
                datafusion::common::DataFusionError::ResourcesExhausted(
                    "function output exceeds its byte allowance".into(),
                )
            } else {
                datafusion::common::DataFusionError::Execution(
                    "invalid canonical JSON input or output".into(),
                )
            }
        })?;
    String::from_utf8(output.bytes).map_err(|_| {
        datafusion::common::DataFusionError::Execution("invalid canonical JSON UTF-8".into())
    })
}
