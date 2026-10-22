//! Declarative column-level merge semantics for keyed `UpdateSpec` operations.

use super::*;
use crate::model::{UpdateMergeSpec, UpdateMergeStrategy};
use arrow::{
    array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, StructArray, new_empty_array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use datafusion::common::ScalarValue;
use iceberg::spec::{Schema, Type};
use std::collections::{BTreeMap, BTreeSet};

/// Validates a declarative merge specification against the current native schema and key columns.
pub(in crate::engine) fn validate(
    merge: &UpdateMergeSpec,
    native_schema: &Schema,
    keys: &[String],
) -> Result<()> {
    let key_set: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
    let public_fields: BTreeMap<&str, &iceberg::spec::NestedFieldRef> = native_schema
        .as_struct()
        .fields()
        .iter()
        .filter(|field| {
            !field
                .name
                .to_ascii_lowercase()
                .starts_with(storage::defaults::SUPPLIED_PREFIX)
                && !storage::identity::hidden(&field.name.to_ascii_lowercase())
        })
        .map(|field| (field.name.as_str(), field))
        .collect();

    if let Some(order_by) = &merge.order_by {
        let Some(field) = public_fields.get(order_by.as_str()) else {
            return Err(Error::refusal(
                "invalid_merge",
                format!("merge order_by column '{order_by}' is absent from the base schema"),
            ));
        };
        if !matches!(field.field_type.as_ref(), Type::Primitive(_)) {
            return Err(Error::refusal(
                "invalid_merge",
                format!("merge order_by column '{order_by}' must have an orderable scalar type"),
            ));
        }
    }

    for (column, strategy) in &merge.columns {
        let Some(field) = public_fields.get(column.as_str()) else {
            return Err(Error::refusal(
                "invalid_merge",
                format!("merge column '{column}' is absent from the base schema"),
            ));
        };
        if key_set.contains(column.as_str()) {
            return Err(Error::refusal(
                "invalid_merge",
                format!("update key column '{column}' cannot specify a merge strategy"),
            ));
        }
        match strategy {
            UpdateMergeStrategy::Replace | UpdateMergeStrategy::LatestOrBackfill => {}
            UpdateMergeStrategy::Min | UpdateMergeStrategy::Max => {
                if !matches!(field.field_type.as_ref(), Type::Primitive(_)) {
                    return Err(Error::refusal(
                        "invalid_merge",
                        format!(
                            "merge column '{column}' with min/max strategy requires a scalar type"
                        ),
                    ));
                }
            }
            UpdateMergeStrategy::SetUnion => {
                if !matches!(field.field_type.as_ref(), Type::List(_)) {
                    return Err(Error::refusal(
                        "invalid_merge",
                        format!("merge column '{column}' with set_union strategy requires a list type"),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Applies column-level merge rules between `prior` rows from `before` and `incoming`
/// replacement rows, folding any intra-batch duplicate keys in `incoming` first.
pub(in crate::engine) async fn apply_merge(
    context: &SessionContext,
    prior: DataFrame,
    incoming: DataFrame,
    native_schema: &Schema,
    keys: &[String],
    merge: &UpdateMergeSpec,
    budget: &Budget,
) -> Result<DataFrame> {
    validate(merge, native_schema, keys)?;
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(native_schema)?);

    let (prior_batches, _prior_permits) = collect_bounded(prior, budget).await?;
    let (incoming_batches, _incoming_permits) = collect_bounded(incoming, budget).await?;

    let key_indices: Vec<usize> = keys
        .iter()
        .map(|k| {
            arrow_schema
                .index_of(k)
                .map_err(|_| Error::refusal("unknown_key", "update key is absent from schema"))
        })
        .collect::<Result<Vec<_>>>()?;

    let order_by_index: Option<usize> = match &merge.order_by {
        Some(col_name) => Some(arrow_schema.index_of(col_name).map_err(|_| {
            Error::refusal(
                "invalid_merge",
                format!("merge order_by column '{col_name}' is absent from schema"),
            )
        })?),
        None => None,
    };

    // Map each public field index to its optional `__docspec_supplied_<id>` column index.
    let mut supplied_flag_for_col: BTreeMap<usize, usize> = BTreeMap::new();
    let mut is_supplied_flag_col: BTreeSet<usize> = BTreeSet::new();
    for (idx, field) in arrow_schema.fields().iter().enumerate() {
        if storage::defaults::supplied_id(field.name()).is_some() {
            is_supplied_flag_col.insert(idx);
        } else if let Some(ice_field) = native_schema.field_by_name(field.name()) {
            let flag_name = storage::defaults::supplied_name(ice_field.id);
            if let Ok(flag_idx) = arrow_schema.index_of(&flag_name) {
                supplied_flag_for_col.insert(idx, flag_idx);
            }
        }
    }

    let num_cols = arrow_schema.fields().len();

    // Extract prior rows keyed by their key tuple.
    let mut prior_by_key: BTreeMap<Vec<OrdScalar>, Vec<ScalarValue>> = BTreeMap::new();
    for batch in &prior_batches {
        let projected = align_batch(batch, &arrow_schema)?;
        for row_idx in 0..projected.num_rows() {
            let row_vals = extract_row(&projected, row_idx)?;
            let key_tuple = key_indices
                .iter()
                .map(|&k_idx| OrdScalar(row_vals[k_idx].clone()))
                .collect::<Vec<_>>();
            if let Some(existing) = prior_by_key.get_mut(&key_tuple) {
                *existing = merge_row_pair(
                    existing,
                    &row_vals,
                    &arrow_schema,
                    &key_indices,
                    order_by_index,
                    &merge.columns,
                    &supplied_flag_for_col,
                    &is_supplied_flag_col,
                )?;
            } else {
                prior_by_key.insert(key_tuple, row_vals);
            }
        }
    }

    // Group incoming rows by key, preserving first-seen key order and input ordinal.
    let mut key_order: Vec<Vec<OrdScalar>> = Vec::new();
    let mut incoming_by_key: BTreeMap<Vec<OrdScalar>, Vec<(usize, Vec<ScalarValue>)>> =
        BTreeMap::new();
    let mut ordinal = 0usize;
    for batch in &incoming_batches {
        let projected = align_batch(batch, &arrow_schema)?;
        for row_idx in 0..projected.num_rows() {
            let row_vals = extract_row(&projected, row_idx)?;
            let key_tuple = key_indices
                .iter()
                .map(|&k_idx| OrdScalar(row_vals[k_idx].clone()))
                .collect::<Vec<_>>();
            let entry = incoming_by_key.entry(key_tuple.clone()).or_insert_with(|| {
                key_order.push(key_tuple);
                Vec::new()
            });
            entry.push((ordinal, row_vals));
            ordinal += 1;
        }
    }

    let mut merged_rows: Vec<Vec<ScalarValue>> = Vec::with_capacity(key_order.len());
    for key_tuple in key_order {
        let mut group = incoming_by_key.remove(&key_tuple).unwrap_or_default();
        if group.len() > 1
            && let Some(ob_idx) = order_by_index
        {
            // Fold intra-batch duplicates ordered by (order_by ASC NULLS FIRST, input_ordinal ASC).
            group.sort_by(|(ord_a, row_a), (ord_b, row_b)| {
                compare_nulls_first(&row_a[ob_idx], &row_b[ob_idx]).then_with(|| ord_a.cmp(ord_b))
            });
        }
        let mut iter = group.into_iter().map(|(_, row)| row);
        let Some(first_incoming) = iter.next() else {
            continue;
        };
        let mut folded_incoming = normalize_single_row(
            first_incoming,
            &arrow_schema,
            &merge.columns,
        )?;
        for next_incoming in iter {
            folded_incoming = merge_row_pair(
                &folded_incoming,
                &next_incoming,
                &arrow_schema,
                &key_indices,
                order_by_index,
                &merge.columns,
                &supplied_flag_for_col,
                &is_supplied_flag_col,
            )?;
        }

        let final_row = if let Some(prior_row) = prior_by_key.get(&key_tuple) {
            merge_row_pair(
                prior_row,
                &folded_incoming,
                &arrow_schema,
                &key_indices,
                order_by_index,
                &merge.columns,
                &supplied_flag_for_col,
                &is_supplied_flag_col,
            )?
        } else {
            clear_origins(folded_incoming, &arrow_schema)
        };
        merged_rows.push(final_row);
    }

    if merged_rows.is_empty() {
        let provider = datafusion::datasource::empty::EmptyTable::new(arrow_schema);
        return Ok(context.read_table(Arc::new(provider))?);
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
    for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
        let scalars: Vec<ScalarValue> = merged_rows
            .iter()
            .map(|row| row[col_idx].clone())
            .collect();
        let array = scalars_to_array(scalars, field)?;
        columns.push(array);
    }
    let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;
    let _output_permit = budget.reserve(batch.get_array_memory_size())?;
    let mem_table = datafusion::datasource::MemTable::try_new(arrow_schema, vec![vec![batch]])?;
    Ok(context.read_table(Arc::new(mem_table))?)
}

async fn collect_bounded(
    frame: DataFrame,
    budget: &Budget,
) -> Result<(Vec<RecordBatch>, Vec<tokio::sync::OwnedSemaphorePermit>)> {
    let mut stream = frame.execute_stream().await?;
    let mut batches = Vec::new();
    let mut permits = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let bytes = batch.get_array_memory_size();
        if bytes > budget.limits.max_batch_bytes {
            return Err(Error::refusal(
                "oversized_batch",
                format!(
                    "merge batch uses {bytes} bytes, exceeding the {} byte allowance",
                    budget.limits.max_batch_bytes
                ),
            ));
        }
        permits.push(budget.reserve(bytes)?);
        batches.push(batch);
    }
    Ok((batches, permits))
}

fn align_batch(batch: &RecordBatch, target_schema: &ArrowSchema) -> Result<RecordBatch> {
    if batch.schema().fields().len() == target_schema.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(target_schema.fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
    {
        return Ok(batch.clone());
    }
    let mut cols = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        if let Ok(idx) = batch.schema().index_of(field.name()) {
            cols.push(coerce_array_to_field(batch.column(idx), field)?);
        } else if storage::identity::hidden(&field.name().to_ascii_lowercase()) {
            let null_scalar = ScalarValue::try_from(field.data_type())?;
            cols.push(null_scalar.to_array_of_size(batch.num_rows())?);
        } else {
            return Err(Error::refusal(
                "schema_mismatch",
                format!("missing field '{}' during update merge", field.name()),
            ));
        }
    }
    Ok(RecordBatch::try_new(Arc::new(target_schema.clone()), cols)?)
}

fn extract_row(batch: &RecordBatch, row_idx: usize) -> Result<Vec<ScalarValue>> {
    let mut vals = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        vals.push(ScalarValue::try_from_array(
            batch.column(col_idx).as_ref(),
            row_idx,
        )?);
    }
    Ok(vals)
}

fn normalize_single_row(
    mut row: Vec<ScalarValue>,
    schema: &ArrowSchema,
    strategies: &BTreeMap<String, UpdateMergeStrategy>,
) -> Result<Vec<ScalarValue>> {
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if matches!(
            strategies.get(field.name()),
            Some(UpdateMergeStrategy::SetUnion)
        ) {
            row[col_idx] = set_union_scalars(
                &ScalarValue::try_from(field.data_type())?,
                &row[col_idx],
                field,
            )?;
        }
    }
    Ok(clear_origins(row, schema))
}

fn clear_origins(mut row: Vec<ScalarValue>, schema: &ArrowSchema) -> Vec<ScalarValue> {
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let lower = field.name().to_ascii_lowercase();
        if lower == storage::identity::ORIGIN_FILE {
            row[col_idx] = ScalarValue::Utf8(None);
        } else if lower == storage::identity::ORIGIN_ROW {
            row[col_idx] = ScalarValue::Int64(None);
        }
    }
    row
}

#[allow(clippy::too_many_arguments)]
fn merge_row_pair(
    prior: &[ScalarValue],
    incoming: &[ScalarValue],
    schema: &ArrowSchema,
    key_indices: &[usize],
    order_by_index: Option<usize>,
    strategies: &BTreeMap<String, UpdateMergeStrategy>,
    supplied_flag_for_col: &BTreeMap<usize, usize>,
    is_supplied_flag_col: &BTreeSet<usize>,
) -> Result<Vec<ScalarValue>> {
    let incoming_wins = match order_by_index {
        None => true,
        Some(ob_idx) => {
            let p_ord = &prior[ob_idx];
            let i_ord = &incoming[ob_idx];
            p_ord.is_null() || (!i_ord.is_null() && compare_non_null(i_ord, p_ord).is_ge())
        }
    };

    let mut out = prior.to_vec();
    for &k_idx in key_indices {
        out[k_idx] = incoming[k_idx].clone();
    }

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if key_indices.contains(&col_idx) || is_supplied_flag_col.contains(&col_idx) {
            continue;
        }
        let lower = field.name().to_ascii_lowercase();
        if lower == storage::identity::ORIGIN_FILE {
            out[col_idx] = ScalarValue::Utf8(None);
            continue;
        }
        if lower == storage::identity::ORIGIN_ROW {
            out[col_idx] = ScalarValue::Int64(None);
            continue;
        }

        let strategy = strategies
            .get(field.name())
            .copied()
            .unwrap_or(UpdateMergeStrategy::Replace);
        let p_val = &prior[col_idx];
        let i_val = &incoming[col_idx];

        let (merged_val, picked_incoming) = match strategy {
            UpdateMergeStrategy::Replace => {
                if incoming_wins {
                    (i_val.clone(), true)
                } else {
                    (p_val.clone(), false)
                }
            }
            UpdateMergeStrategy::LatestOrBackfill => {
                if incoming_wins {
                    if !i_val.is_null() {
                        (i_val.clone(), true)
                    } else {
                        (p_val.clone(), false)
                    }
                } else if !p_val.is_null() {
                    (p_val.clone(), false)
                } else {
                    (i_val.clone(), true)
                }
            }
            UpdateMergeStrategy::Min => match (p_val.is_null(), i_val.is_null()) {
                (true, _) => (i_val.clone(), true),
                (false, true) => (p_val.clone(), false),
                (false, false) => {
                    if compare_non_null(i_val, p_val).is_lt() {
                        (i_val.clone(), true)
                    } else {
                        (p_val.clone(), false)
                    }
                }
            },
            UpdateMergeStrategy::Max => match (p_val.is_null(), i_val.is_null()) {
                (true, _) => (i_val.clone(), true),
                (false, true) => (p_val.clone(), false),
                (false, false) => {
                    if compare_non_null(i_val, p_val).is_gt() {
                        (i_val.clone(), true)
                    } else {
                        (p_val.clone(), false)
                    }
                }
            },
            UpdateMergeStrategy::SetUnion => {
                let unioned = set_union_scalars(p_val, i_val, field)?;
                (unioned, incoming_wins)
            }
        };

        out[col_idx] = merged_val;
        if let Some(&flag_idx) = supplied_flag_for_col.get(&col_idx) {
            let p_flag = matches!(prior[flag_idx], ScalarValue::Boolean(Some(true)));
            let i_flag = matches!(incoming[flag_idx], ScalarValue::Boolean(Some(true)));
            let merged_flag = match strategy {
                UpdateMergeStrategy::Replace => {
                    if picked_incoming {
                        i_flag
                    } else {
                        p_flag
                    }
                }
                UpdateMergeStrategy::LatestOrBackfill
                | UpdateMergeStrategy::Min
                | UpdateMergeStrategy::Max
                | UpdateMergeStrategy::SetUnion => p_flag || i_flag,
            };
            out[flag_idx] = ScalarValue::Boolean(Some(merged_flag));
        }
    }

    Ok(out)
}

fn set_union_scalars(
    prior: &ScalarValue,
    incoming: &ScalarValue,
    field: &FieldRef,
) -> Result<ScalarValue> {
    match field.data_type() {
        DataType::List(item_field) => {
            let (p_null, p_items) = extract_list_elements(prior)?;
            let (i_null, i_items) = extract_list_elements(incoming)?;
            if p_null && i_null {
                return Ok(ScalarValue::List(Arc::new(
                    GenericListArray::<i32>::new_null(item_field.clone(), 1),
                )));
            }
            let deduped = dedup_elements(p_items, i_items);
            let values_arr = scalars_to_array(deduped, item_field)?;
            let offsets = OffsetBuffer::from_lengths([values_arr.len()]);
            let list_arr =
                GenericListArray::<i32>::try_new(item_field.clone(), offsets, values_arr, None)?;
            Ok(ScalarValue::List(Arc::new(list_arr)))
        }
        DataType::LargeList(item_field) => {
            let (p_null, p_items) = extract_list_elements(prior)?;
            let (i_null, i_items) = extract_list_elements(incoming)?;
            if p_null && i_null {
                return Ok(ScalarValue::LargeList(Arc::new(
                    GenericListArray::<i64>::new_null(item_field.clone(), 1),
                )));
            }
            let deduped = dedup_elements(p_items, i_items);
            let values_arr = scalars_to_array(deduped, item_field)?;
            let offsets = OffsetBuffer::from_lengths([values_arr.len()]);
            let list_arr =
                GenericListArray::<i64>::try_new(item_field.clone(), offsets, values_arr, None)?;
            Ok(ScalarValue::LargeList(Arc::new(list_arr)))
        }
        other => Err(Error::refusal(
            "invalid_merge",
            format!("set_union requires a List or LargeList column, found {other:?}"),
        )),
    }
}

fn extract_list_elements(val: &ScalarValue) -> Result<(bool, Vec<ScalarValue>)> {
    match val {
        ScalarValue::List(arr) => extract_generic_list_elements(arr.as_ref()),
        ScalarValue::LargeList(arr) => extract_generic_list_elements(arr.as_ref()),
        ScalarValue::Null => Ok((true, Vec::new())),
        other => Err(Error::refusal(
            "invalid_merge",
            format!("expected list scalar value in set_union, found {other:?}"),
        )),
    }
}

fn extract_generic_list_elements<O: OffsetSizeTrait>(
    arr: &GenericListArray<O>,
) -> Result<(bool, Vec<ScalarValue>)> {
    if arr.len() == 0 || arr.is_null(0) {
        return Ok((true, Vec::new()));
    }
    let values = arr.value(0);
    let mut items = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        items.push(ScalarValue::try_from_array(values.as_ref(), i)?);
    }
    Ok((false, items))
}

fn dedup_elements(prior: Vec<ScalarValue>, incoming: Vec<ScalarValue>) -> Vec<ScalarValue> {
    let mut seen: BTreeSet<OrdScalar> = BTreeSet::new();
    for item in prior.into_iter().chain(incoming) {
        seen.insert(OrdScalar(item));
    }
    seen.into_iter().map(|ord| ord.0).collect()
}

pub(in crate::engine) fn scalars_to_array(
    scalars: Vec<ScalarValue>,
    target_field: &FieldRef,
) -> Result<ArrayRef> {
    let raw = if scalars.is_empty() {
        new_empty_array(target_field.data_type())
    } else {
        ScalarValue::iter_to_array(scalars)?
    };
    coerce_array_to_field(&raw, target_field)
}

pub(in crate::engine) fn coerce_array_to_field(
    array: &ArrayRef,
    target_field: &Field,
) -> Result<ArrayRef> {
    let target_type = target_field.data_type();
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    match (array.data_type(), target_type) {
        (DataType::List(_), DataType::List(dst_item)) => {
            let list = array
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .ok_or_else(|| Error::refusal("invalid_merge", "failed to downcast ListArray"))?;
            let values = coerce_array_to_field(list.values(), dst_item)?;
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                dst_item.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(dst_item)) => {
            let list = array
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .ok_or_else(|| {
                    Error::refusal("invalid_merge", "failed to downcast LargeListArray")
                })?;
            let values = coerce_array_to_field(list.values(), dst_item)?;
            Ok(Arc::new(GenericListArray::<i64>::try_new(
                dst_item.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Struct(_), DataType::Struct(dst_fields)) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::refusal("invalid_merge", "failed to downcast StructArray"))?;
            let mut cols = Vec::with_capacity(dst_fields.len());
            for (idx, dst_f) in dst_fields.iter().enumerate() {
                let src_col = struct_arr
                    .column_by_name(dst_f.name())
                    .or_else(|| (idx < struct_arr.num_columns()).then(|| struct_arr.column(idx)))
                    .ok_or_else(|| {
                        Error::refusal(
                            "invalid_merge",
                            format!("missing struct field '{}'", dst_f.name()),
                        )
                    })?;
                cols.push(coerce_array_to_field(src_col, dst_f)?);
            }
            Ok(Arc::new(StructArray::try_new(
                dst_fields.clone(),
                cols,
                struct_arr.nulls().cloned(),
            )?))
        }
        _ => Ok(arrow::compute::cast(array, target_type)?),
    }
}

/// Total ordering wrapper around `ScalarValue` used for deterministic key maps,
/// `order_by` comparisons, and `SetUnion` element deduplication.
#[derive(Clone, Debug)]
pub(in crate::engine) struct OrdScalar(pub ScalarValue);

impl PartialEq for OrdScalar {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for OrdScalar {}

impl PartialOrd for OrdScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdScalar {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_nulls_first(&self.0, &other.0)
    }
}

pub(in crate::engine) fn compare_nulls_first(
    a: &ScalarValue,
    b: &ScalarValue,
) -> std::cmp::Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => compare_non_null(a, b),
    }
}

pub(in crate::engine) fn compare_non_null(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if let Some(ord) = a.partial_cmp(b) {
        return ord;
    }
    match (a, b) {
        (ScalarValue::Struct(sa), ScalarValue::Struct(sb)) => {
            for (ca, cb) in sa.columns().iter().zip(sb.columns().iter()) {
                let va = ScalarValue::try_from_array(ca.as_ref(), 0).unwrap_or(ScalarValue::Null);
                let vb = ScalarValue::try_from_array(cb.as_ref(), 0).unwrap_or(ScalarValue::Null);
                let ord = compare_nulls_first(&va, &vb);
                if !ord.is_eq() {
                    return ord;
                }
            }
            sa.num_columns().cmp(&sb.num_columns())
        }
        (ScalarValue::List(la), ScalarValue::List(lb)) => {
            let (_, ea) = extract_generic_list_elements(la.as_ref()).unwrap_or((true, Vec::new()));
            let (_, eb) = extract_generic_list_elements(lb.as_ref()).unwrap_or((true, Vec::new()));
            for (va, vb) in ea.iter().zip(eb.iter()) {
                let ord = compare_nulls_first(va, vb);
                if !ord.is_eq() {
                    return ord;
                }
            }
            ea.len().cmp(&eb.len())
        }
        (ScalarValue::LargeList(la), ScalarValue::LargeList(lb)) => {
            let (_, ea) = extract_generic_list_elements(la.as_ref()).unwrap_or((true, Vec::new()));
            let (_, eb) = extract_generic_list_elements(lb.as_ref()).unwrap_or((true, Vec::new()));
            for (va, vb) in ea.iter().zip(eb.iter()) {
                let ord = compare_nulls_first(va, vb);
                if !ord.is_eq() {
                    return ord;
                }
            }
            ea.len().cmp(&eb.len())
        }
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}
