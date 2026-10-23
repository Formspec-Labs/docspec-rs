// Rust guideline compliant 2026-02-21

use super::*;

pub(super) fn validate_pipeline_columns(
    joined_schema: &Schema,
    policy: &ExportPolicy,
    computed_names: &BTreeSet<String>,
) -> Result<()> {
    for name in &policy.columns {
        if name.to_ascii_lowercase().starts_with("__docspec_") {
            return Err(Error::refusal(
                "export_disclosure",
                "internal occurrence and evidence fields cannot be exported",
            ));
        }
        let is_fold_target = policy
            .fold_readings
            .as_ref()
            .is_some_and(|f| &f.target_column == name);
        let is_child_count = policy
            .child_tables
            .values()
            .any(|c| c.replace_with_count_column.as_ref() == Some(name));
        if !is_fold_target
            && !is_child_count
            && !computed_names.contains(name)
            && joined_schema.index_of(name).is_err()
        {
            return Err(Error::refusal(
                "export_disclosure",
                "authorized column is absent from the exact source schema",
            ));
        }
    }
    if let Some(fold) = &policy.fold_readings {
        if fold
            .target_column
            .to_ascii_lowercase()
            .starts_with("__docspec_")
        {
            return Err(Error::refusal(
                "export_disclosure",
                "internal occurrence and evidence fields cannot be exported",
            ));
        }
        for c in &fold.columns {
            if c.to_ascii_lowercase().starts_with("__docspec_")
                || joined_schema.index_of(c).is_err()
            {
                return Err(Error::refusal(
                    "export_disclosure",
                    "folded column is absent or unauthorized",
                ));
            }
        }
    }
    for child in policy.child_tables.values() {
        if joined_schema.index_of(&child.list_column).is_err() {
            return Err(Error::refusal(
                "export_disclosure",
                "child table list_column is absent from the joined schema",
            ));
        }
        for pk in &child.parent_key_columns {
            if joined_schema.index_of(pk).is_err() {
                return Err(Error::refusal(
                    "export_disclosure",
                    "child table parent_key_column is absent from the joined schema",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn next_available_id(seen_ids: &mut BTreeSet<i32>) -> i32 {
    let mut candidate = seen_ids.iter().next_back().copied().unwrap_or(0) + 1;
    while !seen_ids.insert(candidate) {
        candidate += 1;
    }
    candidate
}

pub(super) fn sanitize_or_assign_field(
    root_name: &str,
    field: &Field,
    seen_ids: &mut BTreeSet<i32>,
    allow_assign_id: bool,
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
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(item) => DataType::List(Arc::new(sanitize_or_assign_field(
            root_name,
            item,
            seen_ids,
            allow_assign_id,
        )?)),
        DataType::Struct(fields) if !fields.is_empty() => {
            let mut sanitized = Vec::with_capacity(fields.len());
            for child in fields {
                sanitized.push(Arc::new(sanitize_or_assign_field(
                    root_name,
                    child,
                    seen_ids,
                    allow_assign_id,
                )?));
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
    let parsed_id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|id| *id > 0);
    let id = match parsed_id {
        Some(existing) if seen_ids.insert(existing) => existing,
        Some(_) | None if allow_assign_id => next_available_id(seen_ids),
        _ => return Err(invalid_export()),
    };
    Ok(
        Field::new(field.name(), data_type, field.is_nullable()).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            id.to_string(),
        )])),
    )
}

fn build_fold_target_field(target_name: &str, seen_ids: &mut BTreeSet<i32>) -> Field {
    let outer_id = next_available_id(seen_ids);
    let struct_id = next_available_id(seen_ids);
    let field_label_id = next_available_id(seen_ids);
    let values_list_id = next_available_id(seen_ids);
    let values_item_id = next_available_id(seen_ids);

    let values_item = Field::new("item", DataType::Utf8, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.into(),
        values_item_id.to_string(),
    )]));
    let values_field =
        Field::new("values", DataType::List(Arc::new(values_item)), false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), values_list_id.to_string())]),
        );
    let label_field = Field::new("field", DataType::Utf8, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.into(),
        field_label_id.to_string(),
    )]));
    let struct_item = Field::new(
        "item",
        DataType::Struct(Fields::from(vec![label_field, values_field])),
        false,
    )
    .with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.into(),
        struct_id.to_string(),
    )]));
    Field::new(target_name, DataType::List(Arc::new(struct_item)), false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), outer_id.to_string())]),
    )
}

pub(super) fn build_parent_output_schema(
    joined_schema: &Schema,
    policy: &ExportPolicy,
    source_field_map: &HashMap<String, Field>,
    seen_ids: &mut BTreeSet<i32>,
    allow_assign_id: bool,
) -> Result<SchemaRef> {
    let expected_cols = policy.expected_output_columns();
    let mut out_fields = Vec::with_capacity(expected_cols.len());

    for col_name in &expected_cols {
        if let Some(fold) = &policy.fold_readings
            && col_name == &fold.target_column
        {
            out_fields.push(build_fold_target_field(col_name, seen_ids));
            continue;
        }
        if policy
            .child_tables
            .values()
            .any(|c| c.replace_with_count_column.as_ref() == Some(col_name))
        {
            let id = next_available_id(seen_ids);
            out_fields.push(Field::new(col_name, DataType::Int64, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), id.to_string())]),
            ));
            continue;
        }
        let joined_field = joined_schema.field_with_name(col_name).map_err(|_| {
            Error::refusal(
                "export_disclosure",
                "authorized column is absent from the exact source schema",
            )
        })?;
        let template_field = if policy.computed_columns.contains_key(col_name) {
            if let Some(orig) = source_field_map.get(col_name) {
                Field::new(col_name, joined_field.data_type().clone(), true)
                    .with_metadata(orig.metadata().clone())
            } else {
                Field::new(col_name, joined_field.data_type().clone(), true)
            }
        } else if let Some(orig) = source_field_map.get(col_name) {
            orig.clone()
                .with_nullable(orig.is_nullable() || joined_field.is_nullable())
        } else {
            joined_field.clone()
        };
        out_fields.push(sanitize_or_assign_field(
            col_name,
            &template_field,
            seen_ids,
            allow_assign_id,
        )?);
    }
    Ok(Arc::new(Schema::new(out_fields)))
}

pub(super) fn transform_to_parent_batch(
    batch: &RecordBatch,
    parent_schema: &SchemaRef,
    policy: &ExportPolicy,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(parent_schema.fields().len());
    for field in parent_schema.fields() {
        let col_name = field.name();
        if let Some(fold) = &policy.fold_readings
            && col_name == &fold.target_column
        {
            let folded = fold_readings_array(batch, policy, fold, field)?;
            columns.push(folded);
            continue;
        }
        if let Some(child_spec) = policy
            .child_tables
            .values()
            .find(|c| c.replace_with_count_column.as_ref() == Some(col_name))
        {
            let list_arr = batch
                .column_by_name(&child_spec.list_column)
                .ok_or_else(invalid_export)?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(invalid_export)?;
            let mut counts = Int64Builder::with_capacity(batch.num_rows());
            let offsets = list_arr.value_offsets();
            for row in 0..batch.num_rows() {
                if list_arr.is_null(row) {
                    counts.append_value(0);
                } else {
                    counts.append_value(i64::from(offsets[row + 1] - offsets[row]));
                }
            }
            columns.push(Arc::new(counts.finish()));
            continue;
        }
        let raw_col = batch.column_by_name(col_name).ok_or_else(invalid_export)?;
        let coerced = if raw_col.data_type() != field.data_type() {
            match sanitize_export_array(raw_col, field.data_type()) {
                Ok(arr) => arr,
                Err(_) => arrow::compute::cast(raw_col, field.data_type())?,
            }
        } else {
            raw_col.clone()
        };
        columns.push(coerced);
    }
    Ok(RecordBatch::try_new(parent_schema.clone(), columns)?)
}

fn fold_readings_array(
    batch: &RecordBatch,
    policy: &ExportPolicy,
    fold: &ReadingsFoldSpec,
    target_field: &Field,
) -> Result<ArrayRef> {
    let source_col_names: Vec<String> = if !fold.columns.is_empty() {
        fold.columns.clone()
    } else {
        let from_policy: Vec<String> = policy
            .columns
            .iter()
            .filter(|c| fold.matches_source_column(c))
            .cloned()
            .collect();
        if !from_policy.is_empty() {
            from_policy
        } else {
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|c| fold.matches_source_column(c))
                .collect()
        }
    };

    let DataType::List(struct_item_field) = target_field.data_type() else {
        return Err(invalid_export());
    };
    let DataType::Struct(struct_fields) = struct_item_field.data_type() else {
        return Err(invalid_export());
    };
    let DataType::List(values_item_field) = struct_fields[1].data_type() else {
        return Err(invalid_export());
    };

    let field_builder = StringBuilder::new();
    let values_builder =
        ListBuilder::new(StringBuilder::new()).with_field(values_item_field.clone());
    let struct_builder = StructBuilder::new(
        struct_fields.clone(),
        vec![Box::new(field_builder), Box::new(values_builder)],
    );
    let mut outer_builder = ListBuilder::new(struct_builder).with_field(struct_item_field.clone());

    let source_cols: Vec<(&str, &str, &ArrayRef)> = source_col_names
        .iter()
        .map(|name| {
            let arr = batch.column_by_name(name).ok_or_else(invalid_export)?;
            Ok((name.as_str(), fold.field_label(name), arr))
        })
        .collect::<Result<Vec<_>>>()?;

    for row in 0..batch.num_rows() {
        for &(_col_name, label, arr) in &source_cols {
            let values = extract_non_empty_strings(arr, row);
            if !values.is_empty() {
                let sb = outer_builder.values();
                sb.field_builder::<StringBuilder>(0)
                    .ok_or_else(invalid_export)?
                    .append_value(label);
                let vb = sb
                    .field_builder::<ListBuilder<StringBuilder>>(1)
                    .ok_or_else(invalid_export)?;
                for val in values {
                    vb.values().append_value(&val);
                }
                vb.append(true);
                sb.append(true);
            }
        }
        outer_builder.append(true);
    }

    Ok(Arc::new(outer_builder.finish()))
}

fn extract_non_empty_strings(array: &ArrayRef, row: usize) -> Vec<String> {
    if array.is_null(row) {
        return Vec::new();
    }
    if let Some(s) = array.as_any().downcast_ref::<StringArray>() {
        let v = s.value(row);
        return if v.is_empty() {
            Vec::new()
        } else {
            vec![v.to_owned()]
        };
    }
    if let Some(s) = array.as_any().downcast_ref::<LargeStringArray>() {
        let v = s.value(row);
        return if v.is_empty() {
            Vec::new()
        } else {
            vec![v.to_owned()]
        };
    }
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let slice = list.value(row);
        let mut out = Vec::new();
        for i in 0..slice.len() {
            if !slice.is_null(i)
                && let Some(v) = scalar_value_at(&slice, i)
                && !v.is_empty()
            {
                out.push(v);
            }
        }
        return out;
    }
    scalar_value_at(array, row)
        .filter(|v| !v.is_empty())
        .into_iter()
        .collect()
}

pub(super) fn scalar_value_at(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).to_owned());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(row).to_owned());
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<Date32Array>() {
        return Some(a.value(row).to_string());
    }
    ScalarValue::try_from_array(array, row)
        .ok()
        .map(|sv| sv.to_string())
}

pub(super) fn is_scalar_data_type(dt: &DataType) -> bool {
    !matches!(dt, DataType::List(_) | DataType::Struct(_))
}

pub(super) fn resolve_scalar_columns(
    explicit: &[String],
    order_by: &[SortColumn],
    out_schema: &Schema,
    flag_name: &str,
) -> Result<Vec<String>> {
    if !explicit.is_empty() {
        let mut out = Vec::with_capacity(explicit.len());
        for name in explicit {
            let field = out_schema.field_with_name(name).map_err(|_| {
                Error::refusal(
                    "invalid_export",
                    format!("{flag_name} {name} is absent from the exported schema"),
                )
            })?;
            if !is_scalar_data_type(field.data_type()) {
                return Err(Error::refusal(
                    "invalid_export",
                    format!("{flag_name} {name} must be a scalar column"),
                ));
            }
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
        return Ok(out);
    }
    let mut out = Vec::new();
    for sc in order_by {
        if let Ok(field) = out_schema.field_with_name(&sc.field)
            && is_scalar_data_type(field.data_type())
            && !out.contains(&sc.field)
        {
            out.push(sc.field.clone());
        }
    }
    Ok(out)
}
