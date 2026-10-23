//! Bounded child Parquet slice reading with row-group browse range and scalar `PageIndex` pruning.

use super::{invalid_export, scalar_value_at};
use crate::{
    Error, Result,
    engine::Engine,
    export::{ExportKeyRange, ExportPin},
};
use arrow::{
    array::{Array, Int32Array, Int64Array, StringArray, UInt32Builder},
    compute::take,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::{
            ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
        },
        async_reader::AsyncFileReader,
    },
    file::{metadata::PageIndexPolicy, page_index::column_index::ColumnIndexMetaData},
};
use std::{fs::File, path::PathBuf};

/// Bounded query parameters for pruning and reading a slice of a child Parquet table.
#[derive(Clone, Debug)]
pub struct ChildSliceQuery<'a> {
    /// Row-group browse ranges recorded on the child table export pin.
    pub browse_ranges: &'a [ExportKeyRange],
    /// Name of the child parent-key column.
    pub key_col: &'a str,
    /// Name of the child ordinal column.
    pub ord_col: &'a str,
    /// Target parent key value as a string.
    pub key_value: &'a str,
    /// Parsed integer parent key value when the key column is `Int32` or `Int64`.
    pub key_i64: Option<i64>,
    /// Inclusive lower bound on ordinal.
    pub start_ordinal: i64,
    /// Exclusive upper bound on ordinal (`start_ordinal + limit`).
    pub end_ordinal: i64,
    /// Maximum number of matching child rows to return.
    pub limit: usize,
}

impl Engine {
    /// Reads a bounded slice `[start_ordinal .. start_ordinal + limit)` of child rows for a
    /// single parent key value using row-group browse ranges and Parquet scalar `PageIndex`
    /// pruning.
    ///
    /// # Errors
    /// Refuses unknown child tables, inconsistent pins, or unreadable child Parquet files.
    pub async fn read_export_child_slice(
        &self,
        pin: &ExportPin,
        child_name: &str,
        key_value: &str,
        start_ordinal: i64,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let memory = self.request_memory(&(pin, child_name, key_value, start_ordinal, limit))?;
        let child = pin
            .child_tables
            .get(child_name)
            .ok_or_else(|| {
                Error::refusal(
                    "missing_child_export",
                    "requested child table is not in export pin",
                )
            })?
            .clone();
        let child_spec = pin
            .policy
            .child_tables
            .get(child_name)
            .ok_or_else(invalid_export)?
            .clone();
        if child.object.owner != pin.dataset {
            return Err(invalid_export());
        }
        if limit == 0 || child.rows == 0 {
            return Ok(Vec::new());
        }
        let key_col = child_spec
            .effective_child_key_columns()
            .first()
            .cloned()
            .ok_or_else(invalid_export)?;
        let ord_col = child_spec.ordinal_column.clone();
        let end_ordinal = start_ordinal.saturating_add(i64::try_from(limit).unwrap_or(i64::MAX));
        let browse_ranges = child.browse_ranges.clone();
        let key_value = key_value.to_owned();
        let child_object = child.object.clone();
        self.admit_work(memory, move |engine| async move {
            let budget = &engine.inner.lifecycle.budget;
            let mem = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
            if !engine.inner.retained.is_local() {
                let _mem = mem;
                let reader = engine
                    .inner
                    .objects
                    .open_flat_parquet_reader(&child_object)
                    .await?;
                let query = ChildSliceQuery {
                    browse_ranges: &browse_ranges,
                    key_col: &key_col,
                    ord_col: &ord_col,
                    key_value: &key_value,
                    key_i64: None,
                    start_ordinal,
                    end_ordinal,
                    limit,
                };
                slice_child_parquet_async(reader, &query).await
            } else {
                let path = PathBuf::from(&child_object.path);
                let expected_bytes = child_object.bytes;
                budget
                    .blocking(move || {
                        let _mem = mem;
                        let meta = std::fs::metadata(&path).map_err(|_| invalid_export())?;
                        if meta.len() != expected_bytes {
                            return Err(invalid_export());
                        }
                        let query = ChildSliceQuery {
                            browse_ranges: &browse_ranges,
                            key_col: &key_col,
                            ord_col: &ord_col,
                            key_value: &key_value,
                            key_i64: None,
                            start_ordinal,
                            end_ordinal,
                            limit,
                        };
                        slice_child_parquet(&path, &query)
                    })
                    .await
            }
        })
        .await
    }
}

fn prepare_child_slice_plan<'a>(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &Schema,
    base: &ChildSliceQuery<'a>,
) -> Option<(ChildSliceQuery<'a>, Vec<usize>, Option<RowSelection>, usize)> {
    let num_rgs = metadata.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let key_is_int64 = schema
        .field_with_name(base.key_col)
        .is_ok_and(|f| matches!(f.data_type(), DataType::Int64 | DataType::Int32));
    let key_i64 = base.key_i64.or_else(|| {
        if key_is_int64 {
            base.key_value.parse::<i64>().ok()
        } else {
            None
        }
    });
    let query = ChildSliceQuery {
        key_i64,
        ..base.clone()
    };
    let matching_rgs = prune_child_row_groups(num_rgs, &query);
    if matching_rgs.is_empty() {
        return None;
    }
    let selection = build_page_index_selection_for_query(metadata, schema, &matching_rgs, &query);
    let batch_size = query.limit.clamp(64, 2048);
    Some((query, matching_rgs, selection, batch_size))
}

fn prune_child_row_groups(num_rgs: usize, query: &ChildSliceQuery<'_>) -> Vec<usize> {
    let mut matching_rgs = Vec::new();
    for rg_idx in 0..num_rgs {
        if let Some(kr) = query.browse_ranges.iter().find(|r| r.row_group == rg_idx) {
            let (Some(min_s), Some(max_s)) = (
                kr.min_values.get(query.key_col),
                kr.max_values.get(query.key_col),
            ) else {
                continue;
            };
            let in_key_bounds = if let Some(target_i) = query.key_i64 {
                let min_i = min_s.parse::<i64>().unwrap_or(i64::MIN);
                let max_i = max_s.parse::<i64>().unwrap_or(i64::MAX);
                target_i >= min_i && target_i <= max_i
            } else {
                query.key_value >= min_s.as_str() && query.key_value <= max_s.as_str()
            };
            if !in_key_bounds {
                continue;
            }
            if min_s == query.key_value
                && max_s == query.key_value
                && let (Some(min_o_s), Some(max_o_s)) = (
                    kr.min_values.get(query.ord_col),
                    kr.max_values.get(query.ord_col),
                )
                && let (Ok(min_o), Ok(max_o)) = (min_o_s.parse::<i64>(), max_o_s.parse::<i64>())
                && (max_o < query.start_ordinal || min_o >= query.end_ordinal)
            {
                continue;
            }
        }
        matching_rgs.push(rg_idx);
    }
    matching_rgs
}

fn filter_child_slice_batch(
    batch: &RecordBatch,
    query: &ChildSliceQuery<'_>,
    collected: &mut usize,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    let key_arr = batch
        .column_by_name(query.key_col)
        .ok_or_else(invalid_export)?;
    let ord_arr = batch
        .column_by_name(query.ord_col)
        .ok_or_else(invalid_export)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(invalid_export)?;

    let mut keep_indices = UInt32Builder::new();
    let mut keep_count = 0usize;
    for row in 0..batch.num_rows() {
        if ord_arr.is_null(row) {
            continue;
        }
        let ord = ord_arr.value(row);
        if ord < query.start_ordinal || ord >= query.end_ordinal {
            continue;
        }
        let matches_key = if let Some(target_i) = query.key_i64 {
            if let Some(a) = key_arr.as_any().downcast_ref::<Int64Array>() {
                !a.is_null(row) && a.value(row) == target_i
            } else if let Some(a) = key_arr.as_any().downcast_ref::<Int32Array>() {
                !a.is_null(row) && i64::from(a.value(row)) == target_i
            } else {
                false
            }
        } else if let Some(a) = key_arr.as_any().downcast_ref::<StringArray>() {
            !a.is_null(row) && a.value(row) == query.key_value
        } else {
            scalar_value_at(key_arr, row).as_deref() == Some(query.key_value)
        };
        if matches_key {
            keep_indices.append_value(row as u32);
            keep_count += 1;
            if *collected + keep_count >= query.limit {
                break;
            }
        }
    }
    if keep_count > 0 {
        let indices = keep_indices.finish();
        let cols = batch
            .columns()
            .iter()
            .map(|c| Ok(take(c.as_ref(), &indices, None)?))
            .collect::<Result<Vec<_>>>()?;
        out.push(RecordBatch::try_new(batch.schema(), cols)?);
        *collected += keep_count;
    }
    Ok(())
}

fn slice_child_parquet(
    path: &std::path::Path,
    base_query: &ChildSliceQuery<'_>,
) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let Some((query, matching_rgs, selection, batch_size)) =
        prepare_child_slice_plan(builder.metadata(), builder.schema(), base_query)
    else {
        return Ok(Vec::new());
    };

    let mut builder = builder
        .with_row_groups(matching_rgs)
        .with_batch_size(batch_size);
    if let Some(sel) = selection {
        builder = builder.with_row_selection(sel);
    }
    let reader = builder.build()?;

    let mut out = Vec::new();
    let mut collected = 0usize;
    for batch_res in reader {
        let batch = batch_res?;
        filter_child_slice_batch(&batch, &query, &mut collected, &mut out)?;
        if collected >= query.limit {
            break;
        }
    }
    Ok(out)
}

async fn slice_child_parquet_async(
    reader: Box<dyn AsyncFileReader>,
    base_query: &ChildSliceQuery<'_>,
) -> Result<Vec<RecordBatch>> {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
    let Some((query, matching_rgs, selection, batch_size)) =
        prepare_child_slice_plan(builder.metadata(), builder.schema(), base_query)
    else {
        return Ok(Vec::new());
    };

    let mut builder = builder
        .with_row_groups(matching_rgs)
        .with_batch_size(batch_size);
    if let Some(sel) = selection {
        builder = builder.with_row_selection(sel);
    }
    let mut stream = builder.build()?;

    let mut out = Vec::new();
    let mut collected = 0usize;
    while let Some(batch) = stream.try_next().await? {
        filter_child_slice_batch(&batch, &query, &mut collected, &mut out)?;
        if collected >= query.limit {
            break;
        }
    }
    Ok(out)
}

fn build_page_index_selection_for_query(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &Schema,
    matching_rgs: &[usize],
    query: &ChildSliceQuery<'_>,
) -> Option<RowSelection> {
    let col_idx = schema.index_of(query.key_col).ok()?;
    let column_index = metadata.column_index()?;
    let offset_index = metadata.offset_index()?;

    let mut selectors = Vec::new();
    for &rg_idx in matching_rgs {
        let rg_meta = metadata.row_group(rg_idx);
        let rg_rows = usize::try_from(rg_meta.num_rows()).ok()?;
        let rg_col_idx = column_index.get(rg_idx)?.get(col_idx)?;
        let rg_off_idx = offset_index.get(rg_idx)?.get(col_idx)?;
        let page_locations = rg_off_idx.page_locations();
        if page_locations.is_empty() {
            selectors.push(RowSelector::select(rg_rows));
            continue;
        }
        for (page_i, loc) in page_locations.iter().enumerate() {
            let start_row = usize::try_from(loc.first_row_index).ok()?;
            let next_row = page_locations
                .get(page_i + 1)
                .and_then(|next| usize::try_from(next.first_row_index).ok())
                .unwrap_or(rg_rows);
            let page_rows = next_row.saturating_sub(start_row);
            if page_rows == 0 {
                continue;
            }
            let page_matches = match rg_col_idx {
                ColumnIndexMetaData::BYTE_ARRAY(native_idx) => {
                    let min_ok = native_idx
                        .min_value(page_i)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .is_none_or(|min_s| query.key_value >= min_s);
                    let max_ok = native_idx
                        .max_value(page_i)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .is_none_or(|max_s| query.key_value <= max_s);
                    min_ok && max_ok
                }
                ColumnIndexMetaData::INT64(native_idx) => {
                    if let Some(target_i) = query.key_i64 {
                        let min_ok = native_idx.min_value(page_i).is_none_or(|&m| target_i >= m);
                        let max_ok = native_idx.max_value(page_i).is_none_or(|&m| target_i <= m);
                        min_ok && max_ok
                    } else {
                        true
                    }
                }
                _ => true,
            };
            if page_matches {
                selectors.push(RowSelector::select(page_rows));
            } else {
                selectors.push(RowSelector::skip(page_rows));
            }
        }
    }
    let key_selection = RowSelection::from(selectors);

    let Ok(ord_idx) = schema.index_of(query.ord_col) else {
        return Some(key_selection);
    };
    let mut ord_selectors = Vec::new();
    for &rg_idx in matching_rgs {
        let rg_meta = metadata.row_group(rg_idx);
        let rg_rows = usize::try_from(rg_meta.num_rows()).ok()?;
        let rg_col_idx = column_index.get(rg_idx)?.get(ord_idx)?;
        let rg_off_idx = offset_index.get(rg_idx)?.get(ord_idx)?;
        let page_locations = rg_off_idx.page_locations();
        if page_locations.is_empty() {
            ord_selectors.push(RowSelector::select(rg_rows));
            continue;
        }
        for (page_i, loc) in page_locations.iter().enumerate() {
            let start_row = usize::try_from(loc.first_row_index).ok()?;
            let next_row = page_locations
                .get(page_i + 1)
                .and_then(|next| usize::try_from(next.first_row_index).ok())
                .unwrap_or(rg_rows);
            let page_rows = next_row.saturating_sub(start_row);
            if page_rows == 0 {
                continue;
            }
            let page_matches = match rg_col_idx {
                ColumnIndexMetaData::INT64(native_idx) => {
                    let min_ok = native_idx
                        .min_value(page_i)
                        .is_none_or(|&m| m < query.end_ordinal);
                    let max_ok = native_idx
                        .max_value(page_i)
                        .is_none_or(|&m| m >= query.start_ordinal);
                    min_ok && max_ok
                }
                _ => true,
            };
            if page_matches {
                ord_selectors.push(RowSelector::select(page_rows));
            } else {
                ord_selectors.push(RowSelector::skip(page_rows));
            }
        }
    }
    Some(key_selection.intersection(&RowSelection::from(ord_selectors)))
}

/// Builds a Parquet `RowSelection` using the scalar `PageIndex` (`ColumnIndex` + `OffsetIndex`)
/// for a child table slice query.
#[allow(clippy::too_many_arguments)]
pub fn build_page_index_selection(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &Schema,
    matching_rgs: &[usize],
    key_col: &str,
    ord_col: &str,
    key_value: &str,
    key_i64: Option<i64>,
    start_ordinal: i64,
    end_ordinal: i64,
) -> Option<RowSelection> {
    let query = ChildSliceQuery {
        browse_ranges: &[],
        key_col,
        ord_col,
        key_value,
        key_i64,
        start_ordinal,
        end_ordinal,
        limit: 0,
    };
    build_page_index_selection_for_query(metadata, schema, matching_rgs, &query)
}
