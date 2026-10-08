//! Native default and supplied-state rules for nullable scalar additions.

use crate::{Error, Result, budget::Limits, model::ColumnType};
use datafusion::common::ScalarValue;
use iceberg::spec::{Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Type};

/// Reserved native columns retain supplied state through physical rewrites.
pub const SUPPLIED_PREFIX: &str = "__docspec_supplied_";

pub fn supplied_id(name: &str) -> Option<i32> {
    name.strip_prefix(SUPPLIED_PREFIX)?.parse().ok()
}

pub fn supplied_name(id: i32) -> String {
    format!("{SUPPLIED_PREFIX}{id}")
}

pub fn native_type(data_type: ColumnType) -> Type {
    Type::Primitive(match data_type {
        ColumnType::Boolean => PrimitiveType::Boolean,
        ColumnType::Long => PrimitiveType::Long,
        ColumnType::Double => PrimitiveType::Double,
        ColumnType::String => PrimitiveType::String,
    })
}

pub fn qualified_type(data_type: &Type) -> bool {
    matches!(
        data_type,
        Type::Primitive(
            PrimitiveType::Boolean
                | PrimitiveType::Long
                | PrimitiveType::Double
                | PrimitiveType::String
        )
    )
}

pub fn literal(data_type: &Type, value: serde_json::Value) -> Result<Option<Literal>> {
    if !qualified_type(data_type) {
        return Err(invalid("default type is not qualified"));
    }
    // Native NestedField deserialization silently drops invalid literals. Parse
    // explicitly so a misspelled or out-of-range default cannot become null.
    Literal::try_from_json(value, data_type)
        .map_err(|_| invalid("default does not match the field type"))
}

pub fn scalar(field: &NestedField) -> Result<ScalarValue> {
    match (field.field_type.as_ref(), field.write_default.as_ref()) {
        (Type::Primitive(PrimitiveType::Boolean), None) => Ok(ScalarValue::Boolean(None)),
        (Type::Primitive(PrimitiveType::Long), None) => Ok(ScalarValue::Int64(None)),
        (Type::Primitive(PrimitiveType::Double), None) => Ok(ScalarValue::Float64(None)),
        (Type::Primitive(PrimitiveType::String), None) => Ok(ScalarValue::Utf8(None)),
        (_, Some(Literal::Primitive(PrimitiveLiteral::Boolean(v)))) => {
            Ok(ScalarValue::Boolean(Some(*v)))
        }
        (_, Some(Literal::Primitive(PrimitiveLiteral::Long(v)))) => {
            Ok(ScalarValue::Int64(Some(*v)))
        }
        (_, Some(Literal::Primitive(PrimitiveLiteral::Double(v)))) if v.0.is_finite() => {
            Ok(ScalarValue::Float64(Some(v.0)))
        }
        (_, Some(Literal::Primitive(PrimitiveLiteral::String(v)))) => {
            Ok(ScalarValue::Utf8(Some(v.clone())))
        }
        _ => Err(invalid("default is not a qualified scalar")),
    }
}

pub fn has_supplied(schema: &Schema) -> bool {
    schema
        .as_struct()
        .fields()
        .iter()
        .any(|f| supplied_id(&f.name).is_some())
}

pub fn evolved(metadata: &iceberg::spec::TableMetadata) -> bool {
    metadata.schemas_iter().any(|schema| has_supplied(schema))
}

pub fn validate(schema: &Schema) -> Result<()> {
    for marker in schema.as_struct().fields() {
        if !marker.name.starts_with(SUPPLIED_PREFIX) {
            continue;
        }
        let id = supplied_id(&marker.name).ok_or_else(|| invalid("invalid supplied field name"))?;
        let field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.id == id)
            .ok_or_else(|| invalid("supplied field has no source field"))?;
        if marker.name != supplied_name(id)
            || marker.id == id
            || field.required
            || !qualified_type(&field.field_type)
            || !marker.required
            || *marker.field_type != Type::Primitive(PrimitiveType::Boolean)
            || marker.initial_default != Some(Literal::bool(false))
            || marker.write_default != Some(Literal::bool(false))
        {
            return Err(invalid(
                "supplied field differs from its native source mapping",
            ));
        }
    }
    Ok(())
}

/// Refuse predictable default expansion before native or query decoding allocates it.
pub fn check_expansion(schema: &Schema, limits: &Limits, writing: bool) -> Result<()> {
    let mut per_row = 0usize;
    for field in schema.as_struct().fields() {
        let mut bytes = 0;
        if let Some(value) = if writing {
            &field.write_default
        } else {
            &field.initial_default
        } {
            bytes = bytes.max(match value {
                Literal::Primitive(PrimitiveLiteral::String(value)) => {
                    value.len().saturating_add(32)
                }
                Literal::Primitive(_) => 16,
                _ => return Err(invalid("nested defaults are not qualified")),
            });
        }
        per_row = per_row.saturating_add(bytes);
    }
    // The native string-default constructor retains an intermediate cloned value
    // vector while building the Arrow array. Reserve both predictable copies.
    if per_row.saturating_mul(limits.batch_rows).saturating_mul(2) > limits.max_batch_bytes {
        return Err(Error::refusal(
            "default_capacity",
            "expanded defaults exceed the batch allowance; reduce batch rows or default size",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::refusal("invalid_default", message)
}
