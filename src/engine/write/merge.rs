//! Declarative column-level merge semantics for keyed `UpdateSpec` operations.
//!
//! Supported strategies:
//! - `Replace`: replace with incoming value when incoming wins, else keep prior value.
//! - `LatestOrBackfill`: take non-null incoming value when incoming wins (falling back to prior),
//!   or keep non-null prior value when prior wins (backfilling from incoming).
//! - `Min`: keep the minimum non-null value across prior and incoming.
//! - `Max`: keep the maximum non-null value across prior and incoming.
//! - `SetUnion`: Deduplicated list union in canonical ascending element order.

use super::*;
use crate::model::{UpdateMergeSpec, UpdateMergeStrategy};
use arrow::{
    array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, StructArray, new_empty_array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::streaming::StreamingTable,
    common::ScalarValue,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use iceberg::spec::{Schema, Type};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
};
use tokio::sync::OwnedSemaphorePermit;

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
                        format!(
                            "merge column '{column}' with set_union strategy requires a list type"
                        ),
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

    // Map each `__docspec_supplied_<id>` column to its data column index via field ID lookup.
    let mut supplied_flag_for_col: BTreeMap<usize, usize> = BTreeMap::new();
    let mut is_supplied_flag_col: BTreeSet<usize> = BTreeSet::new();
    for (flag_idx, field) in arrow_schema.fields().iter().enumerate() {
        if let Some(source_id) = storage::defaults::supplied_id(field.name()) {
            is_supplied_flag_col.insert(flag_idx);
            if let Some(source_field) = native_schema.field_by_id(source_id)
                && let Ok(col_idx) = arrow_schema.index_of(&source_field.name)
            {
                supplied_flag_for_col.insert(col_idx, flag_idx);
            }
        }
    }

    let mut arena = MergeArena::new(budget);

    // Stream prior rows batch-by-batch, reserving incremental scalar sizes and dropping
    // each input RecordBatch and its batch permit immediately after folding its rows.
    let mut prior_by_key: BTreeMap<Vec<OrdScalar>, Vec<ScalarValue>> = BTreeMap::new();
    {
        let mut prior_stream = prior.execute_stream().await?;
        while let Some(batch) = prior_stream.try_next().await? {
            let batch_bytes = batch.get_array_memory_size();
            if batch_bytes > budget.limits.max_batch_bytes {
                return Err(Error::refusal(
                    "oversized_batch",
                    format!(
                        "merge batch uses {batch_bytes} bytes, exceeding the {} byte allowance",
                        budget.limits.max_batch_bytes
                    ),
                ));
            }
            let _batch_permit = budget.reserve(batch_bytes)?;
            let projected = align_batch(&batch, &arrow_schema)?;
            drop(batch);
            for row_idx in 0..projected.num_rows() {
                let row_vals = extract_row(&projected, row_idx)?;
                let key_tuple = key_indices
                    .iter()
                    .map(|&k_idx| OrdScalar(row_vals[k_idx].clone()))
                    .collect::<Vec<_>>();
                if let Some(existing) = prior_by_key.get_mut(&key_tuple) {
                    let before_bytes = row_scalar_bytes(existing);
                    let merged = merge_row_pair(
                        existing,
                        &row_vals,
                        &arrow_schema,
                        &key_indices,
                        order_by_index,
                        &merge.columns,
                        &supplied_flag_for_col,
                        &is_supplied_flag_col,
                    )?;
                    let after_bytes = row_scalar_bytes(&merged);
                    if after_bytes > before_bytes {
                        arena.reserve(after_bytes - before_bytes)?;
                    }
                    *existing = merged;
                } else {
                    arena.reserve(
                        key_scalar_bytes(&key_tuple).saturating_add(row_scalar_bytes(&row_vals)),
                    )?;
                    prior_by_key.insert(key_tuple, row_vals);
                }
            }
        }
    }

    // Stream incoming rows batch-by-batch, preserving first-seen key order and input ordinal.
    let mut key_order: Vec<Vec<OrdScalar>> = Vec::new();
    let mut incoming_by_key: BTreeMap<Vec<OrdScalar>, Vec<(usize, Vec<ScalarValue>)>> =
        BTreeMap::new();
    let mut ordinal = 0usize;
    {
        let mut incoming_stream = incoming.execute_stream().await?;
        while let Some(batch) = incoming_stream.try_next().await? {
            let batch_bytes = batch.get_array_memory_size();
            if batch_bytes > budget.limits.max_batch_bytes {
                return Err(Error::refusal(
                    "oversized_batch",
                    format!(
                        "merge batch uses {batch_bytes} bytes, exceeding the {} byte allowance",
                        budget.limits.max_batch_bytes
                    ),
                ));
            }
            let _batch_permit = budget.reserve(batch_bytes)?;
            let projected = align_batch(&batch, &arrow_schema)?;
            drop(batch);
            for row_idx in 0..projected.num_rows() {
                let row_vals = extract_row(&projected, row_idx)?;
                let key_tuple = key_indices
                    .iter()
                    .map(|&k_idx| OrdScalar(row_vals[k_idx].clone()))
                    .collect::<Vec<_>>();
                let row_bytes = row_scalar_bytes(&row_vals);
                if let Some(group) = incoming_by_key.get_mut(&key_tuple) {
                    arena.reserve(row_bytes.saturating_add(std::mem::size_of::<usize>()))?;
                    group.push((ordinal, row_vals));
                } else {
                    let key_bytes = key_scalar_bytes(&key_tuple);
                    arena.reserve(
                        key_bytes
                            .saturating_mul(2)
                            .saturating_add(row_bytes)
                            .saturating_add(std::mem::size_of::<usize>()),
                    )?;
                    key_order.push(key_tuple.clone());
                    incoming_by_key.insert(key_tuple, vec![(ordinal, row_vals)]);
                }
                ordinal += 1;
            }
        }
    }

    let mut merged_rows: Vec<Vec<ScalarValue>> = Vec::with_capacity(key_order.len());
    for key_tuple in key_order {
        let mut group = incoming_by_key.remove(&key_tuple).unwrap_or_default();
        let group_bytes: usize = group.iter().map(|(_, r)| row_scalar_bytes(r)).sum();
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
        let mut folded_incoming =
            normalize_single_row(first_incoming, &arrow_schema, &merge.columns)?;
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

        let prior_row = prior_by_key.remove(&key_tuple);
        let prior_bytes = prior_row.as_ref().map_or(0, |r| row_scalar_bytes(r));
        let final_row = if let Some(prior_row) = &prior_row {
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
        let final_bytes = row_scalar_bytes(&final_row);
        let consumed_bytes = group_bytes.saturating_add(prior_bytes);
        if final_bytes > consumed_bytes {
            arena.reserve(final_bytes - consumed_bytes)?;
        }
        merged_rows.push(final_row);
    }
    drop(prior_by_key);
    drop(incoming_by_key);

    if merged_rows.is_empty() {
        drop(arena);
        let provider = datafusion::datasource::empty::EmptyTable::new(arrow_schema);
        return Ok(context.read_table(Arc::new(provider))?);
    }

    drop(arena);
    let chunk_rows = budget.limits.batch_rows.max(1);
    let mut batches: VecDeque<(RecordBatch, OwnedSemaphorePermit)> = VecDeque::new();
    for chunk in merged_rows.chunks(chunk_rows) {
        append_bounded_batches(chunk, &arrow_schema, budget, &mut batches)?;
    }
    drop(merged_rows);

    let partition = Arc::new(BudgetedBatchPartition {
        schema: arrow_schema.clone(),
        batches: Arc::new(Mutex::new(batches)),
    });
    let streaming_table = StreamingTable::try_new(arrow_schema, vec![partition])?;
    Ok(context.read_table(Arc::new(streaming_table))?)
}

struct MergeArena<'a> {
    budget: &'a Budget,
    permit: Option<OwnedSemaphorePermit>,
}

impl<'a> MergeArena<'a> {
    fn new(budget: &'a Budget) -> Self {
        Self {
            budget,
            permit: None,
        }
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let next = self.budget.reserve(bytes)?;
        match &mut self.permit {
            Some(existing) => existing.merge(next),
            None => self.permit = Some(next),
        }
        Ok(())
    }
}

fn row_scalar_bytes(row: &[ScalarValue]) -> usize {
    row.iter().map(ScalarValue::size).sum()
}

fn key_scalar_bytes(key: &[OrdScalar]) -> usize {
    key.iter().map(|k| k.0.size()).sum()
}

fn rows_to_batch(rows: &[Vec<ScalarValue>], schema: &Arc<ArrowSchema>) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let scalars: Vec<ScalarValue> = rows.iter().map(|row| row[col_idx].clone()).collect();
        let array = scalars_to_array(scalars, field)?;
        columns.push(array);
    }
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn append_bounded_batches(
    slice: &[Vec<ScalarValue>],
    schema: &Arc<ArrowSchema>,
    budget: &Budget,
    out: &mut VecDeque<(RecordBatch, OwnedSemaphorePermit)>,
) -> Result<()> {
    if slice.is_empty() {
        return Ok(());
    }
    let batch = rows_to_batch(slice, schema)?;
    let bytes = batch.get_array_memory_size();
    if bytes > budget.limits.max_batch_bytes {
        if slice.len() <= 1 {
            return Err(Error::refusal(
                "oversized_batch",
                format!(
                    "merged row uses {bytes} bytes, exceeding the {} byte allowance",
                    budget.limits.max_batch_bytes
                ),
            ));
        }
        drop(batch);
        let mid = slice.len() / 2;
        append_bounded_batches(&slice[..mid], schema, budget, out)?;
        append_bounded_batches(&slice[mid..], schema, budget, out)?;
        return Ok(());
    }
    let permit = budget.reserve(bytes)?;
    out.push_back((batch, permit));
    Ok(())
}

#[derive(Debug)]
struct BudgetedBatchPartition {
    schema: SchemaRef,
    batches: Arc<Mutex<VecDeque<(RecordBatch, OwnedSemaphorePermit)>>>,
}

impl PartitionStream for BudgetedBatchPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let queue = self.batches.clone();
        let stream = futures::stream::unfold(
            (queue, None::<OwnedSemaphorePermit>),
            |(queue, _prev_permit)| async move {
                let next = queue.lock().ok()?.pop_front();
                next.map(|(batch, permit)| (Ok(batch), (queue, Some(permit))))
            },
        );
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
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
        let flag_idx = supplied_flag_for_col.get(&col_idx).copied();
        let p_supplied =
            flag_idx.is_none_or(|f_idx| matches!(prior[f_idx], ScalarValue::Boolean(Some(true))));
        let i_supplied = flag_idx
            .is_none_or(|f_idx| matches!(incoming[f_idx], ScalarValue::Boolean(Some(true))));
        let p_present = !p_val.is_null() && p_supplied;
        let i_present = !i_val.is_null() && i_supplied;

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
                    if i_present {
                        (i_val.clone(), true)
                    } else if p_present || (p_supplied && !i_supplied) {
                        (p_val.clone(), false)
                    } else {
                        (i_val.clone(), true)
                    }
                } else if p_present {
                    (p_val.clone(), false)
                } else if i_present || (i_supplied && !p_supplied) {
                    (i_val.clone(), true)
                } else {
                    (p_val.clone(), false)
                }
            }
            UpdateMergeStrategy::Min => match (p_present, i_present) {
                (false, true) => (i_val.clone(), true),
                (true, false) => (p_val.clone(), false),
                (false, false) => {
                    if p_supplied && !i_supplied {
                        (p_val.clone(), false)
                    } else {
                        (i_val.clone(), true)
                    }
                }
                (true, true) => {
                    if compare_non_null(i_val, p_val).is_lt() {
                        (i_val.clone(), true)
                    } else {
                        (p_val.clone(), false)
                    }
                }
            },
            UpdateMergeStrategy::Max => match (p_present, i_present) {
                (false, true) => (i_val.clone(), true),
                (true, false) => (p_val.clone(), false),
                (false, false) => {
                    if p_supplied && !i_supplied {
                        (p_val.clone(), false)
                    } else {
                        (i_val.clone(), true)
                    }
                }
                (true, true) => {
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

        if let Some(f_idx) = flag_idx {
            let p_flag = matches!(prior[f_idx], ScalarValue::Boolean(Some(true)));
            let i_flag = matches!(incoming[f_idx], ScalarValue::Boolean(Some(true)));
            let merged_flag = match strategy {
                UpdateMergeStrategy::SetUnion => !merged_val.is_null() && (p_flag || i_flag),
                UpdateMergeStrategy::Replace
                | UpdateMergeStrategy::LatestOrBackfill
                | UpdateMergeStrategy::Min
                | UpdateMergeStrategy::Max => {
                    if picked_incoming {
                        i_flag
                    } else {
                        p_flag
                    }
                }
            };
            out[f_idx] = ScalarValue::Boolean(Some(merged_flag));
        }
        out[col_idx] = merged_val;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Limits;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::MemTable;
    use futures::StreamExt;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

    #[tokio::test]
    async fn apply_merge_bisects_chunks_exceeding_max_batch_bytes_and_holds_stream_permits() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let native_schema = IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "updated_at",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "payload",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        // Build 4 individual 1-row batches (~450 bytes each) so each input batch fits under
        // `max_batch_bytes = 900`, while a 4-row `batch_rows = 4` merged chunk (~1500 bytes)
        // exceeds `max_batch_bytes = 900` and must be bisected by `append_bounded_batches`.
        let make_single_row = |id: i64, ts: i64, text: &str| -> RecordBatch {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![id])),
                    Arc::new(Int64Array::from(vec![Some(ts)])),
                    Arc::new(StringArray::from(vec![Some(text)])),
                ],
            )
            .unwrap()
        };
        let payload_str = "x".repeat(2048);
        // Each 1-row input batch is ~2.3 KiB and a 2-row batch is ~4.3 KiB, while a 4-row
        // chunk (`batch_rows = 4`) is ~8.4 KiB > `max_batch_bytes = 5000` and must be bisected.
        let max_batch_bytes = 5000;

        let budget = Budget::new(Limits {
            compute_threads: 2,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 4,
            max_batch_bytes,
            max_metadata_bytes: 4 << 20,
        })
        .unwrap();

        let ctx = budget.context();
        let prior_batches = vec![
            make_single_row(1, 10, &payload_str),
            make_single_row(2, 10, &payload_str),
            make_single_row(3, 10, &payload_str),
            make_single_row(4, 10, &payload_str),
        ];
        let incoming_batches = vec![
            make_single_row(1, 20, &payload_str),
            make_single_row(2, 20, &payload_str),
            make_single_row(3, 20, &payload_str),
            make_single_row(4, 20, &payload_str),
        ];
        let prior_df = ctx
            .read_table(Arc::new(
                MemTable::try_new(arrow_schema.clone(), vec![prior_batches]).unwrap(),
            ))
            .unwrap();
        let incoming_df = ctx
            .read_table(Arc::new(
                MemTable::try_new(arrow_schema.clone(), vec![incoming_batches]).unwrap(),
            ))
            .unwrap();

        let spec = UpdateMergeSpec {
            order_by: Some("updated_at".into()),
            columns: BTreeMap::from([
                ("updated_at".into(), UpdateMergeStrategy::Max),
                ("payload".into(), UpdateMergeStrategy::LatestOrBackfill),
            ]),
        };

        let merged_df = apply_merge(
            &ctx,
            prior_df,
            incoming_df,
            &native_schema,
            &["id".to_owned()],
            &spec,
            &budget,
        )
        .await
        .unwrap();

        let mut stream = merged_df.execute_stream().await.unwrap();
        let mut batch_count = 0usize;
        let mut total_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            assert!(
                batch.get_array_memory_size() <= max_batch_bytes,
                "batch bytes {} exceeded max_batch_bytes {}",
                batch.get_array_memory_size(),
                max_batch_bytes
            );
            batch_count += 1;
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 4);
        assert!(
            batch_count >= 2,
            "expected 4-row chunk to be bisected into >= 2 batches, got {batch_count}"
        );
    }
}
