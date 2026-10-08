//! Exact pinned-schema interpretation through the maintained native reader.

use crate::{Error, Result};
use arrow::{
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::{
    metadata_columns::{RESERVED_FIELD_ID_FILE, is_metadata_field},
    scan::FileScanTaskStream,
    table::Table,
};
use std::sync::Arc;

/// Qualifies top-level renames, reorderings, removals and nullable additions.
pub(super) fn read_schema(table: &Table) -> Result<SchemaRef> {
    let current = table.metadata().current_schema();
    super::defaults::validate(current)?;
    // This virtual ID cannot also identify a nested physical field.
    if current.field_by_id(RESERVED_FIELD_ID_FILE).is_some() {
        return Err(Error::refusal(
            "reserved_field",
            "source schema uses the reserved Iceberg file field ID",
        ));
    }
    for field in current.as_struct().fields() {
        if is_metadata_field(field.id) {
            return Err(Error::refusal(
                "reserved_field",
                "source schema uses a reserved Iceberg metadata field ID",
            ));
        }
    }
    for old in table.metadata().schemas_iter() {
        if Arc::ptr_eq(old, current) {
            continue;
        }
        let roots = old
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect::<std::collections::BTreeSet<_>>();
        for field in current.as_struct().fields() {
            match old.field_by_id(field.id) {
                Some(previous)
                    if previous.field_type != field.field_type
                        || previous.required != field.required
                        || previous.initial_default != field.initial_default
                        || !roots.contains(&field.id) =>
                {
                    return Err(unsupported());
                }
                None if !field.field_type.is_primitive()
                    || ((field.required || field.initial_default.is_some())
                        && (table.metadata().format_version()
                            != iceberg::spec::FormatVersion::V3
                            || !super::defaults::qualified_type(&field.field_type)
                            || (field.required && field.initial_default.is_none()))) =>
                {
                    return Err(unsupported());
                }
                _ => {}
            }
        }
    }
    Ok(Arc::new(iceberg::arrow::schema_to_arrow_schema(current)?))
}

fn unsupported() -> Error {
    Error::refusal(
        "unsupported_schema_evolution",
        "native read interpretation for type, requiredness, nested moves or default changes is unqualified",
    )
}

pub(super) async fn plan_files(table: &Table) -> Result<FileScanTaskStream> {
    read_schema(table)?;
    let schema = table.metadata().current_schema().clone();
    let mut fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    // Iceberg 0.10.1's name-only transformer shortcut ignores field IDs. A
    // virtual run-end-encoded file column forces its full ID-aware transform,
    // including when all requested fields are absent and it reads all columns.
    // This column is transient; it is removed before application processing.
    fields.push(RESERVED_FIELD_ID_FILE);
    Ok(table
        .scan()
        .with_concurrency_limit(1)
        .build()?
        .plan_files()
        .await?
        .map_ok(move |mut task| {
            task.schema = schema.clone();
            task.project_field_ids = fields.clone();
            task
        })
        .boxed())
}

pub(super) fn strip_file_column(batch: RecordBatch, limit: usize) -> Result<RecordBatch> {
    // Include the transient native column in the admission check.
    if batch.get_array_memory_size() > limit {
        return Err(Error::refusal(
            "oversized_batch",
            "native reader exceeded its byte allowance",
        ));
    }
    let columns = batch.num_columns().checked_sub(1).ok_or_else(|| {
        Error::refusal("native_schema", "native reader omitted its metadata column")
    })?;
    if batch
        .schema()
        .field(columns)
        .metadata()
        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
        .and_then(|id| id.parse::<i32>().ok())
        != Some(RESERVED_FIELD_ID_FILE)
        || !matches!(batch.schema().field(columns).data_type(),
            DataType::RunEndEncoded(ends, values)
                if ends.data_type() == &DataType::Int32 && values.data_type() == &DataType::Utf8)
    {
        return Err(Error::refusal(
            "native_schema",
            "native reader returned an unexpected metadata column",
        ));
    }
    Ok(batch.project(&(0..columns).collect::<Vec<_>>())?)
}
