//! Stable source occurrence identities retained through physical rewrites.

use crate::{Error, Result};
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{Schema as ArrowSchema, SchemaRef},
    record_batch::RecordBatch,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadata, Type};
use std::sync::Arc;

pub(crate) const ORIGIN_FILE: &str = "__docspec_origin_file";
pub(crate) const ORIGIN_ROW: &str = "__docspec_origin_row";

pub(crate) fn hidden(name: &str) -> bool {
    matches!(name, ORIGIN_FILE | ORIGIN_ROW)
}

pub(crate) fn public_schema(table: &iceberg::table::Table) -> Result<SchemaRef> {
    let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        schema
            .fields()
            .iter()
            .filter(|field| !hidden(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )))
}

pub(crate) fn public_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !hidden(field.name()))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    Ok(batch.project(&indices)?)
}

pub(crate) fn has_origins(schema: &Schema) -> Result<bool> {
    match (
        schema.field_by_name(ORIGIN_FILE),
        schema.field_by_name(ORIGIN_ROW),
    ) {
        (None, None) => Ok(false),
        (Some(file), Some(row))
            if !file.required
                && !row.required
                && *file.field_type == Type::Primitive(PrimitiveType::String)
                && *row.field_type == Type::Primitive(PrimitiveType::Long)
                && file.initial_default.is_none()
                && row.initial_default.is_none()
                && file.write_default.is_none()
                && row.write_default.is_none() =>
        {
            Ok(true)
        }
        _ => Err(Error::refusal(
            "invalid_occurrence_identity",
            "stored occurrence fields have an invalid native schema",
        )),
    }
}

/// Allows adding storage identities without changing any logical field or ID.
pub(crate) fn preserves_schema(before: &Schema, after: &Schema) -> Result<bool> {
    let before_origins = has_origins(before)?;
    let after_origins = has_origins(after)?;
    if before_origins
        && (!after_origins
            || before.field_by_name(ORIGIN_FILE) != after.field_by_name(ORIGIN_FILE)
            || before.field_by_name(ORIGIN_ROW) != after.field_by_name(ORIGIN_ROW))
    {
        return Ok(false);
    }
    Ok(before
        .as_struct()
        .fields()
        .iter()
        .filter(|field| !hidden(&field.name))
        .eq(after
            .as_struct()
            .fields()
            .iter()
            .filter(|field| !hidden(&field.name)))
        && before
            .identifier_field_ids()
            .collect::<std::collections::BTreeSet<_>>()
            == after
                .identifier_field_ids()
                .collect::<std::collections::BTreeSet<_>>())
}

pub(crate) fn with_origins(metadata: &TableMetadata) -> Result<Schema> {
    let schema = metadata.current_schema();
    if has_origins(schema)? {
        return Ok(schema.as_ref().clone());
    }
    let file_id = metadata
        .last_column_id()
        .checked_add(1)
        .ok_or_else(invalid)?;
    let row_id = file_id
        .checked_add(1)
        .filter(|id| !iceberg::metadata_columns::is_metadata_field(*id))
        .ok_or_else(invalid)?;
    let mut fields = schema.as_struct().fields().to_vec();
    fields.push(Arc::new(NestedField::optional(
        file_id,
        ORIGIN_FILE,
        Type::Primitive(PrimitiveType::String),
    )));
    fields.push(Arc::new(NestedField::optional(
        row_id,
        ORIGIN_ROW,
        Type::Primitive(PrimitiveType::Long),
    )));
    Ok(Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(schema.identifier_field_ids())
        .build()?)
}

/// Materializes fallback origins while preserving every already stored identity.
pub(crate) fn materialize(
    batch: RecordBatch,
    origin: &str,
    start: u64,
    limit: usize,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let Ok(file_index) = schema.index_of(ORIGIN_FILE) else {
        return Ok(batch);
    };
    let row_index = schema.index_of(ORIGIN_ROW).map_err(|_| invalid())?;
    let files = batch
        .column(file_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(invalid)?;
    let rows = batch
        .column(row_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(invalid)?;
    if files.null_count() != rows.null_count() {
        return Err(invalid());
    }
    for index in 0..batch.num_rows() {
        if files.is_null(index) != rows.is_null(index)
            || (!rows.is_null(index) && (rows.value(index) < 0 || files.value(index).is_empty()))
        {
            return Err(invalid());
        }
    }
    if files.null_count() == 0 {
        return Ok(batch);
    }
    let extra = origin
        .len()
        .saturating_add(12)
        .saturating_mul(files.null_count());
    if super::effective_batch_bytes(&batch).saturating_add(extra) > limit {
        return Err(Error::refusal(
            "oversized_batch",
            "stored occurrence identity exceeds the batch allowance",
        ));
    }
    let end = start
        .checked_add(batch.num_rows() as u64)
        .filter(|end| *end <= i64::MAX as u64)
        .ok_or_else(invalid)?;
    let new_files = StringArray::from_iter_values((0..batch.num_rows()).map(|index| {
        if files.is_null(index) {
            origin
        } else {
            files.value(index)
        }
    }));
    let new_rows =
        Int64Array::from_iter_values((start..end).enumerate().map(|(index, fallback)| {
            if rows.is_null(index) {
                fallback as i64
            } else {
                rows.value(index)
            }
        }));
    let mut columns = batch.columns().to_vec();
    columns[file_index] = Arc::new(new_files);
    columns[row_index] = Arc::new(new_rows);
    let result = RecordBatch::try_new(schema, columns)?;
    if super::effective_batch_bytes(&result) > limit {
        return Err(Error::refusal(
            "oversized_batch",
            "stored occurrence identity exceeds the batch allowance",
        ));
    }
    Ok(result)
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_occurrence_identity",
        "stored occurrence identities must contain a nonempty origin and nonnegative row",
    )
}
