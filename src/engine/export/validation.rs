//! Export request, pin, schema, and Parquet output validation.

use super::{ExportMaterialized, ExportOperation};
use crate::{
    Error, Result,
    budget::Budget,
    export::{ExportField, ExportPin, ExportPolicy, ExportSpec, ExportValidation},
    model::{Id, ObjectRef},
    storage,
};
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use parquet::{
    arrow::{PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::metadata::FooterTail,
};
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

pub(super) fn validate_request(spec: &ExportSpec) -> Result<()> {
    let policy = &spec.policy;
    let columns = &policy.columns;
    let groups = policy.effective_groups();
    if spec.source.dataset != spec.dataset
        || groups.is_empty()
        || groups.iter().any(|g| g.is_empty())
        || groups.iter().collect::<BTreeSet<_>>().len() != groups.len()
        || columns.is_empty()
        || columns.iter().any(|name| name.is_empty())
        || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
        || policy.computed_columns.keys().any(|name| name.is_empty())
        || policy.order_by.iter().any(|sc| sc.field.is_empty())
        || policy.page_index_columns.iter().any(|c| c.is_empty())
        || policy.browse_range_columns.iter().any(|c| c.is_empty())
        || policy
            .facet_sets
            .iter()
            .any(|set| set.is_empty() || set.iter().any(|c| c.is_empty()))
        || policy.row_group_rows == Some(0)
    {
        return Err(Error::refusal(
            "invalid_export",
            "export requires one exact source and distinct explicit columns",
        ));
    }
    if let Some(fold) = &policy.fold_readings
        && (fold.target_column.is_empty() || fold.columns.iter().any(|c| c.is_empty()))
    {
        return Err(Error::refusal(
            "invalid_export",
            "export fold_readings requires a nonempty target_column",
        ));
    }
    for (child_name, child_spec) in &policy.child_tables {
        if Id::new(child_name).is_err()
            || child_name == "export"
            || child_spec.list_column.is_empty()
            || child_spec.parent_key_columns.is_empty()
            || child_spec.parent_key_columns.iter().any(|c| c.is_empty())
            || (!child_spec.child_key_columns.is_empty()
                && child_spec.child_key_columns.len() != child_spec.parent_key_columns.len())
            || child_spec.ordinal_column.is_empty()
            || child_spec.row_group_rows == 0
        {
            return Err(Error::refusal(
                "invalid_export",
                "invalid child_tables specification in export policy",
            ));
        }
    }
    Ok(())
}

pub(super) fn check_operation(
    operation: &ExportOperation,
    spec: &ExportSpec,
    digest: &str,
) -> Result<()> {
    if operation.request != *spec || operation.request_digest != digest {
        return Err(Error::refusal(
            "operation_conflict",
            "export identity belongs to a different request",
        ));
    }
    if operation.source.version_ref() != spec.source
        || operation.source_revision <= 0
        || !operation.source.verified
        || operation.source.id != operation.source.dataset.version
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(in crate::engine) fn validate_operation_record(operation: &ExportOperation) -> Result<()> {
    validate_request(&operation.request)?;
    let digest = storage::json_digest(&operation.request)?
        .to_hex()
        .to_string();
    check_operation(operation, &operation.request, &digest)
}

pub(in crate::engine) fn check_materialized(
    value: &ExportMaterialized,
    operation: &ExportOperation,
) -> Result<()> {
    let policy = &operation.request.policy;
    let expected_cols = policy.expected_output_columns();
    let groups = policy.effective_groups();
    let anchor_group = groups
        .iter()
        .find(|g| *g != "evidence" && *g != "receipts")
        .map_or_else(|| policy.primary_group(), String::as_str);
    let expected_rows = if anchor_group == "evidence" || anchor_group == "receipts" {
        operation
            .source
            .dataset
            .evidence
            .as_ref()
            .map(|ev| ev.table.rows)
    } else {
        operation
            .source
            .dataset
            .groups
            .get(anchor_group)
            .map(|g| g.rows)
    };
    let rows_ok = match expected_rows {
        Some(group_rows) if policy.filter.is_some() => value.rows <= group_rows,
        Some(group_rows) => value.rows == group_rows,
        None => false,
    };
    if value.request_digest != operation.request_digest
        || value.object.owner != operation.request.dataset
        || value
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(expected_cols.iter())
        || !rows_ok
        || value.child_tables.keys().ne(policy.child_tables.keys())
        || value.child_tables.values().any(|child| {
            child.object.owner != operation.request.dataset
                || child.validation.object != child.object
                || child.validation.rows != child.rows
                || child.fields.is_empty()
        })
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(in crate::engine) fn check_pin(pin: &ExportPin) -> Result<()> {
    let expected_cols = pin.policy.expected_output_columns();
    if pin.format != 1
        || pin.dataset != pin.source.dataset
        || pin.dataset != pin.object.owner
        || pin.validation.object != pin.object
        || pin.validation.rows != pin.rows
        || pin
            .fields
            .iter()
            .map(|field| &field.name)
            .ne(expected_cols.iter())
        || pin.source_revision <= 0
        || pin.fields.is_empty()
        || pin.child_tables.keys().ne(pin.policy.child_tables.keys())
        || pin.child_tables.values().any(|child| {
            child.object.owner != pin.dataset
                || child.validation.object != child.object
                || child.validation.rows != child.rows
                || child.fields.is_empty()
        })
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(in crate::engine) fn check_result(pin: &ExportPin, operation: &ExportOperation) -> Result<()> {
    check_pin(pin)?;
    let spec = &operation.request;
    if pin.id != spec.operation
        || pin.dataset != spec.dataset
        || pin.source != spec.source
        || pin.policy != spec.policy
        || pin.source_revision != operation.source_revision
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(super) fn invalid_export() -> Error {
    Error::refusal(
        "invalid_export",
        "retained export metadata does not identify one authorized immutable result",
    )
}

pub(super) fn authorized_schema(
    source: &SchemaRef,
    policy: &ExportPolicy,
) -> Result<(Vec<usize>, SchemaRef, Vec<ExportField>)> {
    let mut indices = Vec::with_capacity(policy.columns.len());
    let mut fields = Vec::with_capacity(policy.columns.len());
    let mut seen_ids = BTreeSet::new();
    for name in &policy.columns {
        if name.to_ascii_lowercase().starts_with("__docspec_") {
            return Err(Error::refusal(
                "export_disclosure",
                "internal occurrence and evidence fields cannot be exported",
            ));
        }
        let index = source.index_of(name).map_err(|_| {
            Error::refusal(
                "export_disclosure",
                "authorized column is absent from the exact source schema",
            )
        })?;
        let field = source.field(index);
        fields.push(sanitize_export_field(name, field, &mut seen_ids)?);
        indices.push(index);
    }
    let schema = Arc::new(Schema::new(fields));
    let fields = describe_schema(&schema)?;
    Ok((indices, schema, fields))
}

pub(super) fn sanitize_export_field(
    root_name: &str,
    field: &Field,
    seen_ids: &mut BTreeSet<i32>,
) -> Result<Field> {
    if field.name().to_ascii_lowercase().starts_with("__docspec_") {
        return Err(Error::refusal(
            "export_disclosure",
            "internal occurrence and evidence fields cannot be exported",
        ));
    }
    let data_type = match field.data_type() {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Decimal128(_, _) => field.data_type().clone(),
        DataType::List(item) => {
            DataType::List(Arc::new(sanitize_export_field(root_name, item, seen_ids)?))
        }
        DataType::Struct(fields) if !fields.is_empty() => {
            let mut sanitized = Vec::with_capacity(fields.len());
            for child in fields {
                sanitized.push(Arc::new(sanitize_export_field(root_name, child, seen_ids)?));
            }
            DataType::Struct(sanitized.into())
        }
        unsupported => {
            return Err(Error::refusal(
                "unsupported_export_type",
                format!("local flat export column {root_name} has unsupported type {unsupported}"),
            ));
        }
    };
    let id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|id| *id > 0)
        .ok_or_else(invalid_export)?;
    if !seen_ids.insert(id) {
        return Err(invalid_export());
    }
    // Rebuild every root and descendant field: source documentation/defaults
    // and arbitrary metadata never reach the public Parquet footer or its
    // encoded Arrow schema.
    Ok(
        Field::new(field.name(), data_type, field.is_nullable()).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            id.to_string(),
        )])),
    )
}

pub(super) fn count_primitive_leaves(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| data_type_primitive_leaves(field.data_type()))
        .fold(0_usize, usize::saturating_add)
}

fn data_type_primitive_leaves(data_type: &DataType) -> usize {
    match data_type {
        DataType::List(item) => data_type_primitive_leaves(item.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| data_type_primitive_leaves(field.data_type()))
            .fold(0_usize, usize::saturating_add),
        _ => 1,
    }
}

pub(super) fn sanitize_export_array(
    array: &arrow::array::ArrayRef,
    data_type: &DataType,
) -> Result<arrow::array::ArrayRef> {
    if array.data_type() == data_type {
        return Ok(array.clone());
    }
    match data_type {
        DataType::List(item) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(invalid_export)?;
            let values = sanitize_export_array(list.values(), item.data_type())?;
            Ok(Arc::new(arrow::array::ListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        DataType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(invalid_export)?;
            if structure.columns().len() != fields.len() {
                return Err(invalid_export());
            }
            let columns = structure
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(child, field)| sanitize_export_array(child, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(arrow::array::StructArray::try_new(
                fields.clone(),
                columns,
                structure.nulls().cloned(),
            )?))
        }
        _ => Err(invalid_export()),
    }
}

pub(super) fn describe_schema(schema: &Schema) -> Result<Vec<ExportField>> {
    if !schema.metadata().is_empty() {
        return Err(invalid_export());
    }
    let mut seen_ids = BTreeSet::new();
    schema
        .fields()
        .iter()
        .map(|field| {
            let sanitized = sanitize_export_field(field.name(), field, &mut seen_ids)
                .map_err(|_| invalid_export())?;
            if &sanitized != field.as_ref() {
                return Err(invalid_export());
            }
            let id = field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|id| *id > 0)
                .ok_or_else(invalid_export)?;
            Ok(ExportField {
                id,
                name: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
            })
        })
        .collect()
}

pub(super) async fn validate_output(
    materialized: &ExportMaterialized,
    budget: &Budget,
) -> Result<ExportValidation> {
    for child in materialized.child_tables.values() {
        let child_val =
            validate_output_object(&child.object, child.rows, &child.fields, budget).await?;
        if child_val.object != child.object || child_val.rows != child.rows {
            return Err(invalid_export());
        }
    }
    validate_output_object(
        &materialized.object,
        materialized.rows,
        &materialized.fields,
        budget,
    )
    .await
}

pub(super) async fn validate_output_object(
    object: &ObjectRef,
    rows: u64,
    fields: &[ExportField],
    budget: &Budget,
) -> Result<ExportValidation> {
    let object = object.clone();
    let fields = fields.to_vec();
    let limits = budget.limits.clone();
    let memory = budget.reserve(
        limits
            .max_metadata_bytes
            .saturating_mul(4)
            .saturating_add(limits.max_batch_bytes.saturating_mul(2)),
    )?;
    budget
        .blocking(move || {
            let _memory = memory;
            let mut file = File::open(&object.path)?;
            if file.metadata()?.len() != object.bytes {
                return Err(invalid_export());
            }
            file.seek(SeekFrom::End(-8))?;
            let mut tail = [0; 8];
            file.read_exact(&mut tail)?;
            if FooterTail::try_from(tail)?.metadata_length() > limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "export footer exceeds metadata allowance",
                ));
            }
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            if describe_schema(builder.schema())? != fields
                || u64::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(rows)
            {
                return Err(invalid_export());
            }
            // One small shape probe. The reader may decode compressed pages beyond
            // the returned rows; this is not a compressed-I/O or total-memory claim.
            let mut reader = builder.with_batch_size(8.min(limits.batch_rows)).build()?;
            let (sampled_rows, sampled_decoded_bytes) = match reader.next().transpose()? {
                None => (0, 0),
                Some(batch) => {
                    if storage::effective_batch_bytes(&batch) > limits.max_batch_bytes {
                        return Err(Error::refusal(
                            "oversized_batch",
                            "export sample exceeds the byte allowance",
                        ));
                    }
                    if batch
                        .columns()
                        .iter()
                        .zip(batch.schema().fields())
                        .any(|(array, field)| !field.is_nullable() && array.null_count() != 0)
                    {
                        return Err(invalid_export());
                    }
                    (batch.num_rows(), batch.get_array_memory_size())
                }
            };
            if (sampled_rows == 0) != (rows == 0) {
                return Err(invalid_export());
            }
            Ok(ExportValidation {
                object,
                rows,
                sampled_rows,
                sampled_decoded_bytes,
            })
        })
        .await
}
