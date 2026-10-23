//! Row-group browse key range tracking and in-memory facet count accumulation.

use super::{invalid_export, scalar_value_at};
use crate::{
    Error, Result,
    export::{ExportKeyRange, FacetCountEntry},
};
use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use std::collections::BTreeMap;

pub(super) struct BrowseRangeTracker {
    browse_cols: Vec<String>,
    current_start_row: u64,
    current_rows: u64,
    current_min: BTreeMap<String, String>,
    current_max: BTreeMap<String, String>,
    ranges: Vec<ExportKeyRange>,
}

impl BrowseRangeTracker {
    pub(super) fn new(browse_cols: &[String]) -> Self {
        Self {
            browse_cols: browse_cols.to_vec(),
            current_start_row: 0,
            current_rows: 0,
            current_min: BTreeMap::new(),
            current_max: BTreeMap::new(),
            ranges: Vec::new(),
        }
    }

    pub(super) fn observe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.current_rows = self
            .current_rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(invalid_export)?;
        if self.browse_cols.is_empty() {
            return Ok(());
        }
        for col_name in &self.browse_cols {
            let col_arr = batch.column_by_name(col_name).ok_or_else(invalid_export)?;
            let (min_val, max_val) = array_min_max_strings(col_arr);
            if let Some(min_s) = min_val {
                match self.current_min.get_mut(col_name) {
                    Some(cur) => {
                        if compare_browse_val(col_arr.data_type(), &min_s, cur).is_lt() {
                            *cur = min_s;
                        }
                    }
                    None => {
                        self.current_min.insert(col_name.clone(), min_s);
                    }
                }
            }
            if let Some(max_s) = max_val {
                match self.current_max.get_mut(col_name) {
                    Some(cur) => {
                        if compare_browse_val(col_arr.data_type(), &max_s, cur).is_gt() {
                            *cur = max_s;
                        }
                    }
                    None => {
                        self.current_max.insert(col_name.clone(), max_s);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish_row_group(&mut self) {
        if self.current_rows == 0 {
            return;
        }
        let rg_idx = self.ranges.len();
        let min_values = std::mem::take(&mut self.current_min);
        let max_values = std::mem::take(&mut self.current_max);
        let num_rows = self.current_rows;
        let start_row = self.current_start_row;
        self.current_start_row = self.current_start_row.saturating_add(num_rows);
        self.current_rows = 0;
        if !self.browse_cols.is_empty() {
            self.ranges.push(ExportKeyRange {
                row_group: rg_idx,
                start_row,
                num_rows,
                min_values,
                max_values,
            });
        }
    }

    pub(super) fn into_ranges(mut self) -> Vec<ExportKeyRange> {
        if self.current_rows > 0 {
            self.finish_row_group();
        }
        self.ranges
    }
}

pub(super) fn compare_browse_val(dt: &DataType, a: &str, b: &str) -> std::cmp::Ordering {
    match dt {
        DataType::Int64 | DataType::Int32 | DataType::Date32 => {
            if let (Ok(ia), Ok(ib)) = (a.parse::<i64>(), b.parse::<i64>()) {
                return ia.cmp(&ib);
            }
        }
        DataType::Float64 | DataType::Float32 => {
            if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
                return fa.total_cmp(&fb);
            }
        }
        _ => {}
    }
    a.cmp(b)
}

pub(super) fn array_min_max_strings(array: &ArrayRef) -> (Option<String>, Option<String>) {
    if array.len() == array.null_count() {
        return (None, None);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        let min = arrow::compute::min(a).map(|v| v.to_string());
        let max = arrow::compute::max(a).map(|v| v.to_string());
        return (min, max);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        let min = arrow::compute::min(a).map(|v| v.to_string());
        let max = arrow::compute::max(a).map(|v| v.to_string());
        return (min, max);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        let min = arrow::compute::min_string(a).map(str::to_owned);
        let max = arrow::compute::max_string(a).map(str::to_owned);
        return (min, max);
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        let min = arrow::compute::min_string(a).map(str::to_owned);
        let max = arrow::compute::max_string(a).map(str::to_owned);
        return (min, max);
    }
    let dt = array.data_type();
    let mut min_val: Option<String> = None;
    let mut max_val: Option<String> = None;
    for i in 0..array.len() {
        if let Some(val) = scalar_value_at(array, i) {
            if min_val
                .as_ref()
                .is_none_or(|m| compare_browse_val(dt, &val, m).is_lt())
            {
                min_val = Some(val.clone());
            }
            if max_val
                .as_ref()
                .is_none_or(|m| compare_browse_val(dt, &val, m).is_gt())
            {
                max_val = Some(val);
            }
        }
    }
    (min_val, max_val)
}

pub(super) struct FacetAccumulator {
    sets: Vec<Vec<String>>,
    counts: Vec<BTreeMap<BTreeMap<String, Option<String>>, u64>>,
    estimated_bytes: usize,
    max_metadata_bytes: usize,
}

impl FacetAccumulator {
    pub(super) fn new(sets: &[Vec<String>], max_metadata_bytes: usize) -> Self {
        Self {
            sets: sets.to_vec(),
            counts: vec![BTreeMap::new(); sets.len()],
            estimated_bytes: 0,
            max_metadata_bytes,
        }
    }

    pub(super) fn observe(
        &mut self,
        joined_batch: &RecordBatch,
        parent_batch: &RecordBatch,
    ) -> Result<()> {
        if self.sets.is_empty() {
            return Ok(());
        }
        let num_rows = parent_batch.num_rows();
        for (set_idx, grouping_set) in self.sets.iter().enumerate() {
            let mut cols = Vec::with_capacity(grouping_set.len());
            for col_name in grouping_set {
                let arr = parent_batch
                    .column_by_name(col_name)
                    .or_else(|| joined_batch.column_by_name(col_name))
                    .ok_or_else(|| {
                        Error::refusal(
                            "invalid_export",
                            format!("facet column {col_name} is absent from the exported schema"),
                        )
                    })?;
                cols.push((col_name.as_str(), arr));
            }
            for row in 0..num_rows {
                let mut key = BTreeMap::new();
                for &(name, arr) in &cols {
                    key.insert(name.to_owned(), scalar_value_at(arr, row));
                }
                if let Some(count) = self.counts[set_idx].get_mut(&key) {
                    *count += 1;
                } else {
                    let entry_bytes = key.iter().fold(64_usize, |acc, (k, v)| {
                        acc.saturating_add(k.len())
                            .saturating_add(v.as_ref().map_or(0, String::len))
                            .saturating_add(32)
                    });
                    self.estimated_bytes = self.estimated_bytes.saturating_add(entry_bytes);
                    if self.estimated_bytes > self.max_metadata_bytes {
                        return Err(Error::refusal(
                            "metadata_capacity",
                            "faceted export cardinality exceeds the declared metadata allowance",
                        ));
                    }
                    self.counts[set_idx].insert(key, 1);
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Vec<FacetCountEntry> {
        let mut out = Vec::new();
        for (grouping, map) in self.sets.into_iter().zip(self.counts) {
            for (values, count) in map {
                out.push(FacetCountEntry {
                    grouping: grouping.clone(),
                    values,
                    count,
                });
            }
        }
        out
    }
}
