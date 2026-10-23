//! Component representative sorting, aggregate evaluation, and change-signature hashing.

use super::{argument, invalid, lineage::build_chunk_bounded, topology::GraphTopology};
use crate::{
    budget::Budget,
    error::Result,
    model::{AggregateFunction, ConnectedComponentsTransform, SortDirection},
    storage,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Builder, Decimal128Builder,
        Int32Array, Int32Builder, Int64Array, Int64Builder, LargeListArray, LargeStringArray,
        ListArray, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Returns whether `dt` is a scalar data type supported by connected-components ordering and aggregation.
pub(super) fn is_supported_scalar(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

/// Typed scalar value extracted from an Arrow column for ordering, deduplication, and hashing.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ScalarVal {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Date32(i32),
    Timestamp(i64),
    Utf8(String),
}

impl ScalarVal {
    /// Feeds a type-tagged binary encoding of this scalar value into `hasher` without heap formatting.
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Bool(v) => {
                hasher.update(&[1, u8::from(*v)]);
            }
            Self::Int32(v) => {
                hasher.update(&[2]);
                hasher.update(&v.to_le_bytes());
            }
            Self::Int64(v) => {
                hasher.update(&[3]);
                hasher.update(&v.to_le_bytes());
            }
            Self::Date32(v) => {
                hasher.update(&[4]);
                hasher.update(&v.to_le_bytes());
            }
            Self::Timestamp(v) => {
                hasher.update(&[5]);
                hasher.update(&v.to_le_bytes());
            }
            Self::Utf8(s) => {
                hasher.update(&[6]);
                hasher.update(&(s.len() as u64).to_le_bytes());
                hasher.update(s.as_bytes());
            }
        }
    }
}

fn hash_optional_scalar(hasher: &mut blake3::Hasher, val: Option<&ScalarVal>) {
    match val {
        Some(scalar) => scalar.update_hash(hasher),
        None => {
            hasher.update(&[0]);
        }
    }
}

/// Extracts a supported scalar value at `row` from `col`, returning `None` if null or unsupported.
pub(super) fn extract_scalar(col: &ArrayRef, row: usize) -> Option<ScalarVal> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| ScalarVal::Bool(a.value(row))),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| ScalarVal::Int32(a.value(row))),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| ScalarVal::Int64(a.value(row))),
        DataType::Date32 => col
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .map(|a| ScalarVal::Date32(a.value(row))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| ScalarVal::Timestamp(a.value(row))),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| ScalarVal::Utf8(a.value(row).to_owned())),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| ScalarVal::Utf8(a.value(row).to_owned())),
        _ => None,
    }
}

/// Extracts non-empty strings from a `Utf8`, `LargeUtf8`, `List<Utf8>`, or `LargeList<Utf8>` column cell.
pub(crate) fn extract_strings(col: &ArrayRef, row: usize) -> Vec<String> {
    if col.is_null(row) {
        return Vec::new();
    }
    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| {
                let s = a.value(row);
                if s.is_empty() {
                    Vec::new()
                } else {
                    vec![s.to_owned()]
                }
            })
            .unwrap_or_default(),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| {
                let s = a.value(row);
                if s.is_empty() {
                    Vec::new()
                } else {
                    vec![s.to_owned()]
                }
            })
            .unwrap_or_default(),
        DataType::List(_) => col
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|list| extract_list_strings(&list.value(row)))
            .unwrap_or_default(),
        DataType::LargeList(_) => col
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|list| extract_list_strings(&list.value(row)))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn extract_list_strings(values: &ArrayRef) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(sa) = values.as_any().downcast_ref::<StringArray>() {
        for i in 0..sa.len() {
            if !sa.is_null(i) && !sa.value(i).is_empty() {
                out.push(sa.value(i).to_owned());
            }
        }
    } else if let Some(sa) = values.as_any().downcast_ref::<LargeStringArray>() {
        for i in 0..sa.len() {
            if !sa.is_null(i) && !sa.value(i).is_empty() {
                out.push(sa.value(i).to_owned());
            }
        }
    }
    out
}

#[derive(Clone, Copy, Debug)]
struct RowPos {
    batch: usize,
    row: usize,
}

struct EvaluatedComponent {
    document_id: String,
    rows: Vec<RowPos>,
}

/// Retained component rows sorted by representative priority (`order_by`, `row_key`, and physical occurrence).
pub(super) struct EvaluatedGraph {
    in_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    occ_files: Vec<Vec<String>>,
    occ_rows: Vec<Vec<i64>>,
    components: BTreeMap<String, EvaluatedComponent>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl EvaluatedGraph {
    /// Streams the columns needed for ordering and aggregation from `input`, retaining only rows
    /// belonging to `selected_doc_ids` (or all components when `None`) and sorting each component's
    /// member rows in representative priority order.
    pub(super) async fn from_topology(
        topo: GraphTopology,
        selected_doc_ids: Option<&BTreeSet<String>>,
        context: &SessionContext,
        spec: &ConnectedComponentsTransform,
        input: &str,
        budget: &Budget,
    ) -> Result<Self> {
        let df = context.table(input).await?;
        let in_schema = Arc::new(df.schema().as_arrow().clone());
        if selected_doc_ids.is_some_and(BTreeSet::is_empty) || topo.components.is_empty() {
            return Ok(Self {
                in_schema,
                batches: Vec::new(),
                occ_files: Vec::new(),
                occ_rows: Vec::new(),
                components: BTreeMap::new(),
                _permit: None,
            });
        }

        let resolve = |target: &str| -> Result<String> {
            Ok(in_schema.field_with_name(target)?.name().clone())
        };
        let mut proj_owned = vec![
            resolve(&spec.row_key)?,
            storage::OCCURRENCE_FILE.to_owned(),
            storage::OCCURRENCE_ROW.to_owned(),
        ];
        for sort in &spec.order_by {
            let name = resolve(&sort.field)?;
            if !proj_owned.iter().any(|c| c == &name) {
                proj_owned.push(name);
            }
        }
        for agg in &spec.aggregates {
            if let Some(field) = argument(&agg.function) {
                let name = resolve(field)?;
                if !proj_owned.iter().any(|c| c == &name) {
                    proj_owned.push(name);
                }
            }
        }
        let proj_refs: Vec<&str> = proj_owned.iter().map(String::as_str).collect();
        let mut stream = df.select_columns(&proj_refs)?.execute_stream().await?;

        let mut components: BTreeMap<String, EvaluatedComponent> = BTreeMap::new();
        for doc_id in topo.components.keys() {
            if selected_doc_ids.is_none_or(|set| set.contains(doc_id)) {
                components.insert(
                    doc_id.clone(),
                    EvaluatedComponent {
                        document_id: doc_id.clone(),
                        rows: Vec::new(),
                    },
                );
            }
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut occ_files: Vec<Vec<String>> = Vec::new();
        let mut occ_rows: Vec<Vec<i64>> = Vec::new();
        let mut retained_bytes = 0usize;
        let mut reserved_bytes = 0usize;
        let mut permit: Option<tokio::sync::OwnedSemaphorePermit> = topo._permit;

        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = if let Some(doc_set) = selected_doc_ids {
                let occ_file_arr = batch
                    .column_by_name(storage::OCCURRENCE_FILE)
                    .ok_or_else(|| {
                        invalid("connected_components batch is missing occurrence file column")
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        invalid("connected_components occurrence file column must be Utf8")
                    })?;
                let occ_row_arr = batch
                    .column_by_name(storage::OCCURRENCE_ROW)
                    .ok_or_else(|| {
                        invalid("connected_components batch is missing occurrence row column")
                    })?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        invalid("connected_components occurrence row column must be Int64")
                    })?;
                let mut any_match = false;
                let mut all_match = true;
                let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
                for i in 0..batch.num_rows() {
                    let key = (occ_file_arr.value(i).to_owned(), occ_row_arr.value(i));
                    let matched = topo
                        .occurrence_to_doc
                        .get(&key)
                        .is_some_and(|doc_id| doc_set.contains(doc_id));
                    any_match |= matched;
                    all_match &= matched;
                    mask_builder.append_value(matched);
                }
                if !any_match {
                    continue;
                }
                if all_match {
                    batch
                } else {
                    arrow::compute::filter_record_batch(&batch, &mask_builder.finish())?
                }
            } else {
                batch
            };

            let batch_idx = batches.len();
            let occ_file_arr = batch
                .column_by_name(storage::OCCURRENCE_FILE)
                .ok_or_else(|| {
                    invalid("connected_components batch is missing occurrence file column")
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    invalid("connected_components occurrence file column must be Utf8")
                })?;
            let occ_row_arr = batch
                .column_by_name(storage::OCCURRENCE_ROW)
                .ok_or_else(|| {
                    invalid("connected_components batch is missing occurrence row column")
                })?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    invalid("connected_components occurrence row column must be Int64")
                })?;

            let mut b_files = Vec::with_capacity(batch.num_rows());
            let mut b_rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let file = occ_file_arr.value(row).to_owned();
                let row_num = occ_row_arr.value(row);
                if let Some(doc_id) = topo.occurrence_to_doc.get(&(file.clone(), row_num))
                    && let Some(comp) = components.get_mut(doc_id)
                {
                    comp.rows.push(RowPos {
                        batch: batch_idx,
                        row,
                    });
                }
                b_files.push(file);
                b_rows.push(row_num);
            }

            retained_bytes = retained_bytes.saturating_add(batch.get_array_memory_size());
            let delta = retained_bytes.saturating_sub(reserved_bytes);
            if delta > 0 {
                let extra = budget.reserve(delta)?;
                reserved_bytes = reserved_bytes.saturating_add(delta);
                match &mut permit {
                    Some(existing) => existing.merge(extra),
                    None => permit = Some(extra),
                }
            }
            occ_files.push(b_files);
            occ_rows.push(b_rows);
            batches.push(batch);
        }

        if let Some(first_batch) = batches.first() {
            let b_schema = first_batch.schema();
            let rk_idx = b_schema.index_of(&resolve(&spec.row_key)?)?;
            let mut order_indices = Vec::with_capacity(spec.order_by.len());
            for sort in &spec.order_by {
                order_indices.push((b_schema.index_of(&resolve(&sort.field)?)?, sort.direction));
            }
            for comp in components.values_mut() {
                comp.rows.sort_by(|&a, &b| {
                    for &(col_idx, dir) in &order_indices {
                        let va = extract_scalar(batches[a.batch].column(col_idx), a.row);
                        let vb = extract_scalar(batches[b.batch].column(col_idx), b.row);
                        let ord = match (va, vb) {
                            (None, None) => Ordering::Equal,
                            // NULLS LAST in representative selection (`(col IS NULL) ASC`).
                            (Some(_), None) => Ordering::Less,
                            (None, Some(_)) => Ordering::Greater,
                            (Some(xa), Some(xb)) => match dir {
                                SortDirection::Asc => xa.cmp(&xb),
                                SortDirection::Desc => xb.cmp(&xa),
                            },
                        };
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    let rka = extract_strings(batches[a.batch].column(rk_idx), a.row)
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    let rkb = extract_strings(batches[b.batch].column(rk_idx), b.row)
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    rka.cmp(&rkb)
                        .then_with(|| occ_files[a.batch][a.row].cmp(&occ_files[b.batch][b.row]))
                        .then_with(|| occ_rows[a.batch][a.row].cmp(&occ_rows[b.batch][b.row]))
                });
            }
        }

        Ok(Self {
            in_schema,
            batches,
            occ_files,
            occ_rows,
            components,
            _permit: permit,
        })
    }

    fn col(&self, pos: RowPos, name: &str) -> Result<&ArrayRef> {
        self.batches[pos.batch].column_by_name(name).ok_or_else(|| {
            invalid("connected_components aggregate operand column is missing from batch")
        })
    }

    /// Evaluates configured component aggregates for `selected_doc_ids` (or all components when `None`)
    /// and returns bounded Arrow record batches respecting `budget`.
    pub(super) fn materialize_components_batches(
        &self,
        spec: &ConnectedComponentsTransform,
        selected_doc_ids: Option<&BTreeSet<String>>,
        budget: &Budget,
    ) -> Result<Vec<RecordBatch>> {
        let out_schema = output_schema(&self.in_schema, spec)?;
        let selected: Vec<&EvaluatedComponent> = self
            .components
            .values()
            .filter(|c| selected_doc_ids.is_none_or(|set| set.contains(&c.document_id)))
            .collect();
        if selected.is_empty() {
            return Ok(vec![RecordBatch::new_empty(out_schema)]);
        }
        let mut out = Vec::new();
        let max_rows = budget.limits.batch_rows.max(1);
        let max_bytes = budget.limits.max_batch_bytes;
        let build_batch = |chunk: &[&EvaluatedComponent]| -> Result<RecordBatch> {
            self.build_component_batch(spec, &out_schema, chunk)
        };
        for chunk in selected.chunks(max_rows) {
            build_chunk_bounded(chunk, max_bytes, &mut out, &build_batch)?;
        }
        Ok(out)
    }

    fn build_component_batch(
        &self,
        spec: &ConnectedComponentsTransform,
        out_schema: &SchemaRef,
        selected: &[&EvaluatedComponent],
    ) -> Result<RecordBatch> {
        let num_rows = selected.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + spec.aggregates.len());
        let mut doc_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        for comp in selected {
            doc_id_builder.append_value(&comp.document_id);
        }
        columns.push(Arc::new(doc_id_builder.finish()));

        for agg in &spec.aggregates {
            let col: ArrayRef = match &agg.function {
                AggregateFunction::CountRows => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        b.append_value(comp.rows.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Count { field } => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        let mut count = 0i64;
                        for &pos in &comp.rows {
                            if !self.col(pos, field)?.is_null(pos.row) {
                                count += 1;
                            }
                        }
                        b.append_value(count);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::CountDistinct { field } => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        let mut set: BTreeSet<ScalarVal> = BTreeSet::new();
                        for &pos in &comp.rows {
                            if let Some(val) = extract_scalar(self.col(pos, field)?, pos.row) {
                                set.insert(val);
                            }
                        }
                        b.append_value(set.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Sum { field } => {
                    let mut b = Decimal128Builder::with_capacity(num_rows)
                        .with_data_type(DataType::Decimal128(38, 0));
                    for comp in selected {
                        let mut sum = 0i128;
                        let mut any = false;
                        for &pos in &comp.rows {
                            match extract_scalar(self.col(pos, field)?, pos.row) {
                                Some(ScalarVal::Int32(v)) => {
                                    sum += i128::from(v);
                                    any = true;
                                }
                                Some(ScalarVal::Int64(v)) => {
                                    sum += i128::from(v);
                                    any = true;
                                }
                                _ => {}
                            }
                        }
                        if any {
                            b.append_value(sum);
                        } else {
                            b.append_null();
                        }
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Min { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let mut min_val: Option<ScalarVal> = None;
                        for &pos in &comp.rows {
                            if let Some(v) = extract_scalar(self.col(pos, field)?, pos.row) {
                                min_val = Some(match min_val {
                                    Some(cur) if cur <= v => cur,
                                    _ => v,
                                });
                            }
                        }
                        vals.push(min_val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::Max { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let mut max_val: Option<ScalarVal> = None;
                        for &pos in &comp.rows {
                            if let Some(v) = extract_scalar(self.col(pos, field)?, pos.row) {
                                max_val = Some(match max_val {
                                    Some(cur) if cur >= v => cur,
                                    _ => v,
                                });
                            }
                        }
                        vals.push(max_val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::FirstValue { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let val = if let Some(&pos) = comp.rows.first() {
                            extract_scalar(self.col(pos, field)?, pos.row)
                        } else {
                            None
                        };
                        vals.push(val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::CollectSet { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    self.build_collect_set_array(selected, field, src_dt)?
                }
                AggregateFunction::FirstValueBy { .. }
                | AggregateFunction::CollectDistinctWithProvenance { .. } => {
                    return Err(invalid(
                        "FirstValueBy and CollectDistinctWithProvenance are not supported in connected_components",
                    ));
                }
            };
            columns.push(col);
        }

        Ok(RecordBatch::try_new(out_schema.clone(), columns)?)
    }

    fn build_collect_set_array(
        &self,
        selected: &[&EvaluatedComponent],
        field: &str,
        src_dt: &DataType,
    ) -> Result<ArrayRef> {
        let n = selected.len();
        match src_dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::List(_) | DataType::LargeList(_) => {
                let mut builder = ListBuilder::with_capacity(StringBuilder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        for s in extract_strings(self.col(pos, field)?, pos.row) {
                            set.insert(s);
                        }
                    }
                    for item in set {
                        builder.values().append_value(&item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = ListBuilder::with_capacity(BooleanBuilder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Bool(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int32 => {
                let mut builder = ListBuilder::with_capacity(Int32Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Int32(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = ListBuilder::with_capacity(Int64Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Int64(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Date32 => {
                let mut builder = ListBuilder::with_capacity(Date32Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Date32(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let val_builder = if let Some(zone) = tz {
                    TimestampMicrosecondBuilder::new().with_timezone(zone.clone())
                } else {
                    TimestampMicrosecondBuilder::new()
                };
                let list_field = Arc::new(Field::new_list_field(src_dt.clone(), true));
                let mut builder = ListBuilder::with_capacity(val_builder, n).with_field(list_field);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Timestamp(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(invalid(
                "connected_components CollectSet operand has an unsupported data type",
            )),
        }
    }

    /// Computes a 32-byte BLAKE3 digest summarizing the evaluated outputs and contributor
    /// occurrences of `doc_id` to detect whether an affected component actually changed.
    pub(super) fn component_signature(
        &self,
        spec: &ConnectedComponentsTransform,
        doc_id: &str,
    ) -> Option<[u8; 32]> {
        let comp = self.components.get(doc_id)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(doc_id.as_bytes());
        hasher.update(&(comp.rows.len() as u64).to_le_bytes());
        if let Some(&rep) = comp.rows.first() {
            for agg in &spec.aggregates {
                if let Some(field) = argument(&agg.function) {
                    match &agg.function {
                        AggregateFunction::FirstValue { .. } => {
                            if let Ok(src) = self.col(rep, field) {
                                let val = extract_scalar(src, rep.row);
                                hash_optional_scalar(&mut hasher, val.as_ref());
                            }
                        }
                        AggregateFunction::CollectSet { .. } => {
                            for &pos in &comp.rows {
                                if let Ok(src) = self.col(pos, field) {
                                    match src.data_type() {
                                        DataType::Utf8
                                        | DataType::LargeUtf8
                                        | DataType::List(_)
                                        | DataType::LargeList(_) => {
                                            for s in extract_strings(src, pos.row) {
                                                hasher.update(&[0x10]);
                                                hasher.update(&(s.len() as u64).to_le_bytes());
                                                hasher.update(s.as_bytes());
                                            }
                                        }
                                        _ => {
                                            let val = extract_scalar(src, pos.row);
                                            hash_optional_scalar(&mut hasher, val.as_ref());
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            for &pos in &comp.rows {
                                if let Ok(src) = self.col(pos, field) {
                                    let val = extract_scalar(src, pos.row);
                                    hash_optional_scalar(&mut hasher, val.as_ref());
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut contributors: Vec<(&str, i64)> = comp
            .rows
            .iter()
            .map(|&pos| {
                (
                    self.occ_files[pos.batch][pos.row].as_str(),
                    self.occ_rows[pos.batch][pos.row],
                )
            })
            .collect();
        contributors.sort_unstable();
        for (file, row) in contributors {
            hasher.update(&(file.len() as u64).to_le_bytes());
            hasher.update(file.as_bytes());
            hasher.update(&row.to_le_bytes());
        }
        Some(*hasher.finalize().as_bytes())
    }
}

/// Constructs the output Arrow schema for a connected-components transform given `in_schema`.
pub(super) fn output_schema(
    in_schema: &SchemaRef,
    spec: &ConnectedComponentsTransform,
) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(1 + spec.aggregates.len());
    fields.push(Field::new(&spec.document_id_column, DataType::Utf8, true));
    for agg in &spec.aggregates {
        let field = match &agg.function {
            AggregateFunction::CountRows
            | AggregateFunction::Count { .. }
            | AggregateFunction::CountDistinct { .. } => {
                Field::new(&agg.name, DataType::Int64, false)
            }
            AggregateFunction::Sum { .. } => {
                Field::new(&agg.name, DataType::Decimal128(38, 0), true)
            }
            AggregateFunction::Min { field }
            | AggregateFunction::Max { field }
            | AggregateFunction::FirstValue { field } => {
                let src_dt = normalize_output_type(in_schema.field_with_name(field)?.data_type());
                Field::new(&agg.name, src_dt, true)
            }
            AggregateFunction::CollectSet { field } => {
                let src_dt = in_schema.field_with_name(field)?.data_type();
                let item_dt = match src_dt {
                    DataType::List(inner) | DataType::LargeList(inner) => {
                        normalize_output_type(inner.data_type())
                    }
                    other => normalize_output_type(other),
                };
                Field::new(
                    &agg.name,
                    DataType::List(Arc::new(Field::new_list_field(item_dt, true))),
                    false,
                )
            }
            AggregateFunction::FirstValueBy { .. }
            | AggregateFunction::CollectDistinctWithProvenance { .. } => {
                return Err(invalid(
                    "FirstValueBy and CollectDistinctWithProvenance are not supported in connected_components",
                ));
            }
        };
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn normalize_output_type(dt: &DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 => DataType::Utf8,
        other => other.clone(),
    }
}

fn build_scalar_array(vals: &[Option<ScalarVal>], data_type: &DataType) -> Result<ArrayRef> {
    let n = vals.len();
    match data_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Bool(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Int32(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Int64(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Date32(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Timestamp(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            let arr = if let Some(zone) = tz {
                b.finish().with_timezone(zone.clone())
            } else {
                b.finish()
            };
            Ok(Arc::new(arr))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for v in vals {
                match v {
                    Some(ScalarVal::Utf8(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => Err(invalid(
            "connected_components scalar output column has an unsupported data type",
        )),
    }
}
