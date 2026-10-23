//! Declarative column-level merge semantics for keyed `UpdateSpec` operations.
//!
//! Supported strategies:
//! - `Replace`: replace with incoming value when incoming wins, else keep prior value.
//! - `LatestOrBackfill`: take non-null incoming value when incoming wins (falling back to prior),
//!   or keep non-null prior value when prior wins (backfilling from incoming).
//! - `Min`: keep the minimum non-null value across prior and incoming.
//! - `Max`: keep the maximum non-null value across prior and incoming.
//! - `SetUnion`: Deduplicated list union in canonical ascending element order.
//!
//! ### Three-Valued Presence & Supplied-Bit Precedence
//! When a table schema has evolved or declares write defaults, each defaultable source
//! column `c` (with Iceberg field ID `id`) is paired with a hidden boolean tracking
//! column `__docspec_supplied_<id>`. During merge, each cell is classified into one of
//! three states:
//! 1. **Present** (`!val.is_null() && supplied`): an explicitly supplied non-null value.
//! 2. **Explicitly supplied NULL** (`val.is_null() && supplied`): the caller explicitly
//!    wrote `NULL` rather than omitting the column.
//! 3. **Omitted / defaulted NULL** (`val.is_null() && !supplied`): the column was omitted
//!    by the writer and filled with `NULL`.
//!
//! Precedence rules across strategies:
//! - `Replace`: always selects the winning row's value and its `supplied` bit (`incoming`
//!   when `incoming_wins`, else `prior`).
//! - `LatestOrBackfill`: prefers a **Present** value from the winning side, then a
//!   **Present** value from the losing side (backfill), then an **Explicitly supplied NULL**
//!   over an **Omitted NULL**, and finally falls back to the winning side. The output
//!   `supplied` bit is taken from whichever side contributed the chosen cell.
//! - `Min` / `Max`: compares values only when both sides are **Present**; if only one
//!   side is **Present**, that side wins; if neither is **Present**, an **Explicitly
//!   supplied NULL** beats an **Omitted NULL**. The output `supplied` bit is taken from
//!   the side that contributed the chosen cell.
//! - `SetUnion`: unions non-null list elements from both sides in ascending order; if
//!   both lists are `NULL`, the result is `NULL`. Its `supplied` bit is
//!   `!merged_val.is_null() && (prior_supplied || incoming_supplied)`.

use super::*;
use crate::model::{UpdateMergeSpec, UpdateMergeStrategy};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, StructArray, UInt32Array,
        new_empty_array, new_null_array,
    },
    buffer::{NullBuffer, OffsetBuffer},
    compute::{concat, interleave_record_batch, kernels::zip::zip, take},
    datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
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

/// Schema-resolved column indices and merge rules for one `apply_merge` invocation.
struct MergeSchemaContext {
    arrow_schema: Arc<ArrowSchema>,
    key_indices: Vec<usize>,
    key_set: BTreeSet<usize>,
    order_by_index: Option<usize>,
    strategies: BTreeMap<String, UpdateMergeStrategy>,
    supplied_flag_for_col: BTreeMap<usize, usize>,
    is_supplied_flag_col: BTreeSet<usize>,
}

impl MergeSchemaContext {
    fn resolve(native_schema: &Schema, keys: &[String], merge: &UpdateMergeSpec) -> Result<Self> {
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(native_schema)?);
        let key_indices: Vec<usize> = keys
            .iter()
            .map(|k| {
                arrow_schema
                    .index_of(k)
                    .map_err(|_| Error::refusal("unknown_key", "update key is absent from schema"))
            })
            .collect::<Result<Vec<_>>>()?;
        let key_set: BTreeSet<usize> = key_indices.iter().copied().collect();
        let order_by_index: Option<usize> = match &merge.order_by {
            Some(col_name) => Some(arrow_schema.index_of(col_name).map_err(|_| {
                Error::refusal(
                    "invalid_merge",
                    format!("merge order_by column '{col_name}' is absent from schema"),
                )
            })?),
            None => None,
        };
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
        Ok(Self {
            arrow_schema,
            key_indices,
            key_set,
            order_by_index,
            strategies: merge.columns.clone(),
            supplied_flag_for_col,
            is_supplied_flag_col,
        })
    }
}

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
/// replacement rows using vectorized Arrow `RowConverter`, `interleave_record_batch`,
/// and `zip` kernels, folding any intra-batch duplicate keys in `incoming` first.
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
    let ctx = MergeSchemaContext::resolve(native_schema, keys, merge)?;
    let key_sort_fields: Vec<SortField> = ctx
        .key_indices
        .iter()
        .map(|&idx| SortField::new(ctx.arrow_schema.field(idx).data_type().clone()))
        .collect();
    let key_converter = RowConverter::new(key_sort_fields)?;

    let mut arena = MergeArena::new(budget);

    // Collect aligned prior batches and index row locations by encoded key bytes.
    let mut prior_batches: Vec<RecordBatch> = Vec::new();
    let mut prior_by_key: BTreeMap<Box<[u8]>, Vec<(usize, usize)>> = BTreeMap::new();
    {
        let mut prior_stream = prior.execute_stream().await?;
        while let Some(batch) = prior_stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
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
            let projected = align_batch(&batch, &ctx.arrow_schema)?;
            drop(batch);
            arena.reserve(projected.get_array_memory_size())?;
            let key_cols: Vec<ArrayRef> = ctx
                .key_indices
                .iter()
                .map(|&idx| projected.column(idx).clone())
                .collect();
            let rows = key_converter.convert_columns(&key_cols)?;
            let batch_idx = prior_batches.len();
            for row_idx in 0..projected.num_rows() {
                let key_bytes: Box<[u8]> = rows.row(row_idx).as_ref().into();
                if let Some(locs) = prior_by_key.get_mut(&key_bytes) {
                    arena.reserve(std::mem::size_of::<(usize, usize)>())?;
                    locs.push((batch_idx, row_idx));
                } else {
                    arena.reserve(
                        key_bytes
                            .len()
                            .saturating_add(std::mem::size_of::<(usize, usize)>()),
                    )?;
                    prior_by_key.insert(key_bytes, vec![(batch_idx, row_idx)]);
                }
            }
            prior_batches.push(projected);
        }
    }

    // Collect aligned incoming batches and index row locations by encoded key bytes.
    type IndexedKeyLocations = BTreeMap<Box<[u8]>, Vec<(usize, (usize, usize))>>;
    let mut incoming_batches: Vec<RecordBatch> = Vec::new();
    let mut key_order: Vec<Box<[u8]>> = Vec::new();
    let mut incoming_by_key: IndexedKeyLocations = BTreeMap::new();
    let mut ordinal = 0usize;
    {
        let mut incoming_stream = incoming.execute_stream().await?;
        while let Some(batch) = incoming_stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
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
            let projected = align_batch(&batch, &ctx.arrow_schema)?;
            drop(batch);
            arena.reserve(projected.get_array_memory_size())?;
            let key_cols: Vec<ArrayRef> = ctx
                .key_indices
                .iter()
                .map(|&idx| projected.column(idx).clone())
                .collect();
            let rows = key_converter.convert_columns(&key_cols)?;
            let batch_idx = incoming_batches.len();
            for row_idx in 0..projected.num_rows() {
                let key_bytes: Box<[u8]> = rows.row(row_idx).as_ref().into();
                if let Some(group) = incoming_by_key.get_mut(&key_bytes) {
                    arena.reserve(std::mem::size_of::<(usize, (usize, usize))>())?;
                    group.push((ordinal, (batch_idx, row_idx)));
                } else {
                    arena.reserve(
                        key_bytes
                            .len()
                            .saturating_mul(2)
                            .saturating_add(std::mem::size_of::<(usize, (usize, usize))>()),
                    )?;
                    key_order.push(key_bytes.clone());
                    incoming_by_key.insert(key_bytes, vec![(ordinal, (batch_idx, row_idx))]);
                }
                ordinal += 1;
            }
            incoming_batches.push(projected);
        }
    }

    if key_order.is_empty() {
        drop(arena);
        let provider = datafusion::datasource::empty::EmptyTable::new(ctx.arrow_schema);
        return Ok(context.read_table(Arc::new(provider))?);
    }

    // Pre-encode `order_by` values across incoming batches if any key has duplicates.
    let has_incoming_dups = incoming_by_key.values().any(|g| g.len() > 1);
    let incoming_ob_rows = match (ctx.order_by_index, has_incoming_dups) {
        (Some(ob_idx), true) => {
            let ob_converter = RowConverter::new(vec![SortField::new(
                ctx.arrow_schema.field(ob_idx).data_type().clone(),
            )])?;
            Some(
                incoming_batches
                    .iter()
                    .map(|b| ob_converter.convert_columns(&[b.column(ob_idx).clone()]))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )
        }
        _ => None,
    };

    let mut ordered_incoming_groups: Vec<Vec<(usize, usize)>> = Vec::with_capacity(key_order.len());
    let mut prior_groups_for_keys: Vec<Option<Vec<(usize, usize)>>> =
        Vec::with_capacity(key_order.len());
    for key_bytes in &key_order {
        let mut group = incoming_by_key.remove(key_bytes).unwrap_or_default();
        if group.len() > 1
            && let Some(ob_rows) = &incoming_ob_rows
        {
            // Fold intra-batch duplicates ordered by (order_by ASC NULLS FIRST, input_ordinal ASC).
            group.sort_by(|(ord_a, (ba, ra)), (ord_b, (bb, rb))| {
                ob_rows[*ba]
                    .row(*ra)
                    .cmp(&ob_rows[*bb].row(*rb))
                    .then_with(|| ord_a.cmp(ord_b))
            });
        }
        ordered_incoming_groups.push(group.into_iter().map(|(_, loc)| loc).collect());
        prior_groups_for_keys.push(prior_by_key.remove(key_bytes));
    }
    drop(key_order);
    drop(incoming_by_key);
    drop(prior_by_key);
    drop(incoming_ob_rows);

    let folded_incoming = fold_grouped_rows(
        &incoming_batches,
        &ordered_incoming_groups,
        &ctx,
        true,
        &mut arena,
    )?;
    drop(incoming_batches);
    drop(ordered_incoming_groups);

    let merged_batch = if prior_groups_for_keys.iter().any(Option::is_some) {
        let mut matched_key_positions = Vec::new();
        let mut matched_prior_groups = Vec::new();
        for (pos, group) in prior_groups_for_keys.into_iter().enumerate() {
            if let Some(locs) = group
                && !locs.is_empty()
            {
                matched_key_positions.push(pos);
                matched_prior_groups.push(locs);
            }
        }
        let folded_prior = fold_grouped_rows(
            &prior_batches,
            &matched_prior_groups,
            &ctx,
            false,
            &mut arena,
        )?;
        drop(prior_batches);
        let incoming_subset = take_rows(&folded_incoming, &matched_key_positions)?;
        let merged_subset = merge_batches_vectorized(&folded_prior, &incoming_subset, &ctx)?;
        splice_merged_subset(
            &folded_incoming,
            &merged_subset,
            &matched_key_positions,
            &ctx.arrow_schema,
        )?
    } else {
        drop(prior_batches);
        folded_incoming
    };

    drop(arena);
    let chunk_rows = budget.limits.batch_rows.max(1);
    let mut batches: VecDeque<(RecordBatch, OwnedSemaphorePermit)> = VecDeque::new();
    let total_rows = merged_batch.num_rows();
    for start in (0..total_rows).step_by(chunk_rows) {
        let len = (total_rows - start).min(chunk_rows);
        append_bounded_batch_slice(&merged_batch, start, len, budget, &mut batches)?;
    }
    drop(merged_batch);

    let partition = Arc::new(BudgetedBatchPartition {
        schema: ctx.arrow_schema.clone(),
        batches: Arc::new(Mutex::new(batches)),
    });
    let streaming_table = StreamingTable::try_new(ctx.arrow_schema, vec![partition])?;
    Ok(context.read_table(Arc::new(streaming_table))?)
}

/// Folds a list of per-key row location groups into a single `RecordBatch` of length
/// `groups.len()` using vectorized `interleave_record_batch` and `merge_batches_vectorized`.
fn fold_grouped_rows(
    source_batches: &[RecordBatch],
    groups: &[Vec<(usize, usize)>],
    ctx: &MergeSchemaContext,
    normalize_initial: bool,
    arena: &mut MergeArena<'_>,
) -> Result<RecordBatch> {
    let batch_refs: Vec<&RecordBatch> = source_batches.iter().collect();
    let first_indices: Vec<(usize, usize)> = groups.iter().map(|g| g[0]).collect();
    let mut acc = interleave_record_batch(&batch_refs, &first_indices)?;
    if normalize_initial {
        acc = normalize_batch_columns(&acc, ctx)?;
    }
    arena.reserve(acc.get_array_memory_size())?;

    let max_len = groups.iter().map(Vec::len).max().unwrap_or(0);
    for step in 1..max_len {
        let mut active_positions = Vec::new();
        let mut step_indices = Vec::new();
        for (pos, group) in groups.iter().enumerate() {
            if let Some(&loc) = group.get(step) {
                active_positions.push(pos);
                step_indices.push(loc);
            }
        }
        let acc_subset = take_rows(&acc, &active_positions)?;
        let next_subset = interleave_record_batch(&batch_refs, &step_indices)?;
        let merged_subset = merge_batches_vectorized(&acc_subset, &next_subset, ctx)?;
        let before_bytes = acc.get_array_memory_size();
        acc = splice_merged_subset(&acc, &merged_subset, &active_positions, &ctx.arrow_schema)?;
        let after_bytes = acc.get_array_memory_size();
        if after_bytes > before_bytes {
            arena.reserve(after_bytes - before_bytes)?;
        }
    }
    Ok(acc)
}

fn take_rows(batch: &RecordBatch, positions: &[usize]) -> Result<RecordBatch> {
    let indices = UInt32Array::from_iter_values(positions.iter().map(|&p| p as u32));
    let cols = batch
        .columns()
        .iter()
        .map(|col| Ok(take(col.as_ref(), &indices, None)?))
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), cols)?)
}

fn splice_merged_subset(
    base: &RecordBatch,
    replacement_subset: &RecordBatch,
    subset_positions: &[usize],
    schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    if subset_positions.len() == base.num_rows() {
        return Ok(replacement_subset.clone());
    }
    let mut mapping = vec![(0usize, 0usize); base.num_rows()];
    for (idx, item) in mapping.iter_mut().enumerate() {
        *item = (0, idx);
    }
    for (sub_idx, &pos) in subset_positions.iter().enumerate() {
        mapping[pos] = (1, sub_idx);
    }
    let _ = schema;
    Ok(interleave_record_batch(
        &[base, replacement_subset],
        &mapping,
    )?)
}

/// Normalizes a newly gathered batch (deduplicating `SetUnion` list columns and clearing
/// origin columns) without per-cell `ScalarValue` unpacking.
fn normalize_batch_columns(batch: &RecordBatch, ctx: &MergeSchemaContext) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut cols = Vec::with_capacity(batch.num_columns());
    for (col_idx, field) in ctx.arrow_schema.fields().iter().enumerate() {
        let lower = field.name().to_ascii_lowercase();
        if lower == storage::identity::ORIGIN_FILE || lower == storage::identity::ORIGIN_ROW {
            cols.push(new_null_array(field.data_type(), num_rows));
        } else if matches!(
            ctx.strategies.get(field.name()),
            Some(UpdateMergeStrategy::SetUnion)
        ) {
            let empty_null = new_null_array(field.data_type(), num_rows);
            cols.push(set_union_arrays(&empty_null, batch.column(col_idx), field)?);
        } else {
            cols.push(batch.column(col_idx).clone());
        }
    }
    Ok(RecordBatch::try_new(ctx.arrow_schema.clone(), cols)?)
}

/// Vectorized column-by-column merge of two 1-to-1 key-aligned `RecordBatch`es.
fn merge_batches_vectorized(
    prior: &RecordBatch,
    incoming: &RecordBatch,
    ctx: &MergeSchemaContext,
) -> Result<RecordBatch> {
    let num_rows = prior.num_rows();
    let incoming_wins = compute_incoming_wins(prior, incoming, ctx.order_by_index)?;
    let mut out_cols: Vec<Option<ArrayRef>> = vec![None; ctx.arrow_schema.fields().len()];

    for &k_idx in &ctx.key_indices {
        out_cols[k_idx] = Some(incoming.column(k_idx).clone());
    }

    for (col_idx, field) in ctx.arrow_schema.fields().iter().enumerate() {
        if ctx.key_set.contains(&col_idx) || ctx.is_supplied_flag_col.contains(&col_idx) {
            continue;
        }
        let lower = field.name().to_ascii_lowercase();
        if lower == storage::identity::ORIGIN_FILE || lower == storage::identity::ORIGIN_ROW {
            out_cols[col_idx] = Some(new_null_array(field.data_type(), num_rows));
            continue;
        }

        let strategy = ctx
            .strategies
            .get(field.name())
            .copied()
            .unwrap_or(UpdateMergeStrategy::Replace);
        let p_col = prior.column(col_idx);
        let i_col = incoming.column(col_idx);
        let flag_idx = ctx.supplied_flag_for_col.get(&col_idx).copied();
        let p_flag_arr =
            flag_idx.and_then(|idx| prior.column(idx).as_any().downcast_ref::<BooleanArray>());
        let i_flag_arr =
            flag_idx.and_then(|idx| incoming.column(idx).as_any().downcast_ref::<BooleanArray>());

        let (merged_col, picked_incoming) = match strategy {
            UpdateMergeStrategy::SetUnion => {
                let unioned = set_union_arrays(p_col, i_col, field)?;
                (unioned, incoming_wins.clone())
            }
            UpdateMergeStrategy::Replace => {
                let merged = zip(&incoming_wins, &i_col, &p_col)?;
                (merged, incoming_wins.clone())
            }
            UpdateMergeStrategy::LatestOrBackfill
            | UpdateMergeStrategy::Min
            | UpdateMergeStrategy::Max => {
                let pick_inc = select_scalar_winner(
                    strategy,
                    p_col,
                    i_col,
                    p_flag_arr,
                    i_flag_arr,
                    &incoming_wins,
                )?;
                let merged = zip(&pick_inc, &i_col, &p_col)?;
                (merged, pick_inc)
            }
        };

        if let Some(f_idx) = flag_idx {
            let mut flags = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let p_flag = p_flag_arr.is_some_and(|a| !a.is_null(row) && a.value(row));
                let i_flag = i_flag_arr.is_some_and(|a| !a.is_null(row) && a.value(row));
                let merged_flag = match strategy {
                    UpdateMergeStrategy::SetUnion => !merged_col.is_null(row) && (p_flag || i_flag),
                    UpdateMergeStrategy::Replace
                    | UpdateMergeStrategy::LatestOrBackfill
                    | UpdateMergeStrategy::Min
                    | UpdateMergeStrategy::Max => {
                        if picked_incoming.value(row) {
                            i_flag
                        } else {
                            p_flag
                        }
                    }
                };
                flags.push(Some(merged_flag));
            }
            out_cols[f_idx] = Some(Arc::new(BooleanArray::from(flags)));
        }
        out_cols[col_idx] = Some(merged_col);
    }

    let final_cols = out_cols
        .into_iter()
        .enumerate()
        .map(|(idx, col)| col.unwrap_or_else(|| incoming.column(idx).clone()))
        .collect();
    Ok(RecordBatch::try_new(ctx.arrow_schema.clone(), final_cols)?)
}

fn compute_incoming_wins(
    prior: &RecordBatch,
    incoming: &RecordBatch,
    order_by_index: Option<usize>,
) -> Result<BooleanArray> {
    let num_rows = prior.num_rows();
    let Some(ob_idx) = order_by_index else {
        return Ok(BooleanArray::from(vec![true; num_rows]));
    };
    let p_ord = prior.column(ob_idx);
    let i_ord = incoming.column(ob_idx);
    let converter = RowConverter::new(vec![SortField::new(p_ord.data_type().clone())])?;
    let p_rows = converter.convert_columns(std::slice::from_ref(p_ord))?;
    let i_rows = converter.convert_columns(std::slice::from_ref(i_ord))?;
    let mut wins = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let win = p_ord.is_null(row) || (!i_ord.is_null(row) && i_rows.row(row) >= p_rows.row(row));
        wins.push(Some(win));
    }
    Ok(BooleanArray::from(wins))
}

fn select_scalar_winner(
    strategy: UpdateMergeStrategy,
    p_col: &ArrayRef,
    i_col: &ArrayRef,
    p_flag_arr: Option<&BooleanArray>,
    i_flag_arr: Option<&BooleanArray>,
    incoming_wins: &BooleanArray,
) -> Result<BooleanArray> {
    let num_rows = p_col.len();
    let row_cmp = if matches!(
        strategy,
        UpdateMergeStrategy::Min | UpdateMergeStrategy::Max
    ) {
        let converter = RowConverter::new(vec![SortField::new(p_col.data_type().clone())])?;
        Some((
            converter.convert_columns(std::slice::from_ref(p_col))?,
            converter.convert_columns(std::slice::from_ref(i_col))?,
        ))
    } else {
        None
    };

    let mut pick_incoming = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let p_supplied = p_flag_arr.is_none_or(|a| !a.is_null(row) && a.value(row));
        let i_supplied = i_flag_arr.is_none_or(|a| !a.is_null(row) && a.value(row));
        let p_present = !p_col.is_null(row) && p_supplied;
        let i_present = !i_col.is_null(row) && i_supplied;

        let take_inc = match strategy {
            UpdateMergeStrategy::LatestOrBackfill => {
                if incoming_wins.value(row) {
                    if i_present {
                        true
                    } else {
                        !(p_present || (p_supplied && !i_supplied))
                    }
                } else if p_present {
                    false
                } else {
                    i_present || (i_supplied && !p_supplied)
                }
            }
            UpdateMergeStrategy::Min => match (p_present, i_present, row_cmp.as_ref()) {
                (false, true, _) => true,
                (true, false, _) => false,
                (false, false, _) => !p_supplied || i_supplied,
                (true, true, Some((p_rows, i_rows))) => i_rows.row(row) < p_rows.row(row),
                (true, true, None) => incoming_wins.value(row),
            },
            UpdateMergeStrategy::Max => match (p_present, i_present, row_cmp.as_ref()) {
                (false, true, _) => true,
                (true, false, _) => false,
                (false, false, _) => !p_supplied || i_supplied,
                (true, true, Some((p_rows, i_rows))) => i_rows.row(row) > p_rows.row(row),
                (true, true, None) => incoming_wins.value(row),
            },
            UpdateMergeStrategy::Replace | UpdateMergeStrategy::SetUnion => {
                incoming_wins.value(row)
            }
        };
        pick_incoming.push(Some(take_inc));
    }
    Ok(BooleanArray::from(pick_incoming))
}

fn set_union_arrays(prior: &ArrayRef, incoming: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::List(item_field) => set_union_generic_list::<i32>(prior, incoming, item_field),
        DataType::LargeList(item_field) => {
            set_union_generic_list::<i64>(prior, incoming, item_field)
        }
        other => Err(Error::refusal(
            "invalid_merge",
            format!("set_union requires a List or LargeList column, found {other:?}"),
        )),
    }
}

fn set_union_generic_list<O: OffsetSizeTrait>(
    prior: &ArrayRef,
    incoming: &ArrayRef,
    item_field: &FieldRef,
) -> Result<ArrayRef> {
    let p_list = prior
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| Error::refusal("invalid_merge", "expected list array in set_union"))?;
    let i_list = incoming
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| Error::refusal("invalid_merge", "expected list array in set_union"))?;

    let num_rows = p_list.len();
    let p_values = coerce_array_to_field(p_list.values(), item_field)?;
    let i_values = coerce_array_to_field(i_list.values(), item_field)?;
    let combined_values = concat(&[p_values.as_ref(), i_values.as_ref()])?;
    let p_values_len = p_values.len();

    let converter = RowConverter::new(vec![SortField::new(item_field.data_type().clone())])?;
    let encoded_items = converter.convert_columns(std::slice::from_ref(&combined_values))?;

    let p_offsets = p_list.offsets();
    let i_offsets = i_list.offsets();
    let mut take_indices: Vec<u32> = Vec::new();
    let mut lengths: Vec<usize> = Vec::with_capacity(num_rows);
    let mut validity: Vec<bool> = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let p_null = p_list.is_null(row);
        let i_null = i_list.is_null(row);
        if p_null && i_null {
            lengths.push(0);
            validity.push(false);
            continue;
        }
        validity.push(true);
        let mut row_items: Vec<(arrow::row::Row<'_>, u32)> = Vec::new();
        if !p_null {
            let start = p_offsets[row].as_usize();
            let end = p_offsets[row + 1].as_usize();
            for idx in start..end {
                row_items.push((encoded_items.row(idx), idx as u32));
            }
        }
        if !i_null {
            let start = i_offsets[row].as_usize();
            let end = i_offsets[row + 1].as_usize();
            for idx in start..end {
                let combined_idx = p_values_len + idx;
                row_items.push((encoded_items.row(combined_idx), combined_idx as u32));
            }
        }
        row_items.sort_by(|(ra, _), (rb, _)| ra.cmp(rb));
        row_items.dedup_by(|(ra, _), (rb, _)| ra == rb);
        lengths.push(row_items.len());
        take_indices.extend(row_items.into_iter().map(|(_, idx)| idx));
    }

    let indices_arr = UInt32Array::from(take_indices);
    let deduped_values = if indices_arr.is_empty() {
        new_empty_array(item_field.data_type())
    } else {
        take(combined_values.as_ref(), &indices_arr, None)?
    };
    let deduped_values = coerce_array_to_field(&deduped_values, item_field)?;
    let offsets = OffsetBuffer::<O>::from_lengths(lengths);
    let nulls = NullBuffer::from(validity);
    Ok(Arc::new(GenericListArray::<O>::try_new(
        item_field.clone(),
        offsets,
        deduped_values,
        Some(nulls),
    )?))
}

fn append_bounded_batch_slice(
    merged_batch: &RecordBatch,
    offset: usize,
    len: usize,
    budget: &Budget,
    out: &mut VecDeque<(RecordBatch, OwnedSemaphorePermit)>,
) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    let batch = storage::compact_batch_slice(merged_batch, offset, len)?;
    let bytes = batch.get_array_memory_size();
    if bytes > budget.limits.max_batch_bytes {
        if len <= 1 {
            return Err(Error::refusal(
                "oversized_batch",
                format!(
                    "merged row uses {bytes} bytes, exceeding the {} byte allowance",
                    budget.limits.max_batch_bytes
                ),
            ));
        }
        drop(batch);
        let mid = len / 2;
        append_bounded_batch_slice(merged_batch, offset, mid, budget, out)?;
        append_bounded_batch_slice(merged_batch, offset + mid, len - mid, budget, out)?;
        return Ok(());
    }
    let permit = budget.reserve(bytes)?;
    out.push_back((batch, permit));
    Ok(())
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
                let next = match queue.lock() {
                    Ok(mut guard) => Ok(guard.pop_front()),
                    Err(_) => Err(datafusion::error::DataFusionError::Execution(
                        "merge batch partition mutex poisoned".into(),
                    )),
                };
                match next {
                    Ok(Some((batch, permit))) => Some((Ok(batch), (queue, Some(permit)))),
                    Ok(None) => None,
                    Err(err) => Some((Err(err), (queue, None))),
                }
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
            cols.push(new_null_array(field.data_type(), batch.num_rows()));
        } else {
            return Err(Error::refusal(
                "schema_mismatch",
                format!("missing field '{}' during update merge", field.name()),
            ));
        }
    }
    Ok(RecordBatch::try_new(Arc::new(target_schema.clone()), cols)?)
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

/// Total ordering wrapper around `ScalarValue` used by custom UDAF accumulators.
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
    if a.data_type() == b.data_type()
        && let Ok(converter) = RowConverter::new(vec![SortField::new(a.data_type())])
        && let (Ok(arr_a), Ok(arr_b)) = (a.to_array(), b.to_array())
        && let (Ok(rows_a), Ok(rows_b)) = (
            converter.convert_columns(&[arr_a]),
            converter.convert_columns(&[arr_b]),
        )
    {
        return rows_a.row(0).cmp(&rows_b.row(0));
    }
    a.data_type().to_string().cmp(&b.data_type().to_string())
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
