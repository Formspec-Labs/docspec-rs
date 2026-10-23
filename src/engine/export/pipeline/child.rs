//! Child table extraction, sorting, and Parquet materialization during export.

use super::{
    HashingFile, build_writer_properties, count_primitive_leaves, describe_schema,
    facets::BrowseRangeTracker, invalid_export, next_available_id, sanitize_export_array,
    sanitize_or_assign_field, validate_output_object,
};
use crate::{
    Error, Result,
    budget::Budget,
    engine::write::identifier as quote,
    export::{ChildTableExport, ChildTableExportSpec, ExportField, ExportKeyRange},
    model::Id,
    storage,
};
use arrow::{
    array::{Array, ArrayRef, Int64Builder, ListArray, StructArray, UInt32Builder},
    compute::take,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{datasource::MemTable, logical_expr::col};
use futures::TryStreamExt;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

pub(super) struct ChildAccumulator {
    name: String,
    spec: ChildTableExportSpec,
    child_schema: SchemaRef,
    child_fields: Vec<ExportField>,
    struct_indices: Vec<usize>,
    batches: Vec<RecordBatch>,
    reservations: Vec<tokio::sync::OwnedSemaphorePermit>,
}

impl ChildAccumulator {
    pub(super) fn new(
        name: String,
        spec: ChildTableExportSpec,
        joined_schema: &Schema,
        source_field_map: &HashMap<String, Field>,
    ) -> Result<Self> {
        let list_field = joined_schema
            .field_with_name(&spec.list_column)
            .map_err(|_| invalid_export())?;
        let DataType::List(item_field) = list_field.data_type() else {
            return Err(Error::refusal(
                "invalid_export",
                format!("child table column {} is not a List", spec.list_column),
            ));
        };
        let DataType::Struct(struct_fields) = item_field.data_type() else {
            return Err(Error::refusal(
                "invalid_export",
                format!(
                    "child table column {} is not a List<Struct>",
                    spec.list_column
                ),
            ));
        };

        let mut seen_ids = BTreeSet::new();
        let mut out_fields = Vec::new();
        let child_key_names = spec.effective_child_key_columns();
        for (pk_name, ck_name) in spec.parent_key_columns.iter().zip(child_key_names.iter()) {
            let pk_field = source_field_map
                .get(pk_name)
                .cloned()
                .or_else(|| joined_schema.field_with_name(pk_name).ok().cloned())
                .ok_or_else(invalid_export)?;
            let renamed = pk_field.with_name(ck_name);
            out_fields.push(sanitize_or_assign_field(
                ck_name,
                &renamed,
                &mut seen_ids,
                true,
            )?);
        }

        let ord_id = next_available_id(&mut seen_ids);
        out_fields.push(
            Field::new(&spec.ordinal_column, DataType::Int64, false).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.into(), ord_id.to_string())],
            )),
        );

        let selected_subfields: Vec<String> = match &spec.struct_columns {
            Some(cols) => cols.clone(),
            None => struct_fields.iter().map(|f| f.name().clone()).collect(),
        };
        let mut struct_indices = Vec::with_capacity(selected_subfields.len());
        for sub_name in &selected_subfields {
            let (idx, sub_field) = struct_fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == sub_name)
                .ok_or_else(|| {
                    Error::refusal(
                        "invalid_export",
                        format!("child struct subfield {sub_name} is absent"),
                    )
                })?;
            struct_indices.push(idx);
            out_fields.push(sanitize_or_assign_field(
                sub_name,
                sub_field.as_ref(),
                &mut seen_ids,
                true,
            )?);
        }

        let child_schema = Arc::new(Schema::new(out_fields));
        let child_fields = describe_schema(&child_schema)?;
        Ok(Self {
            name,
            spec,
            child_schema,
            child_fields,
            struct_indices,
            batches: Vec::new(),
            reservations: Vec::new(),
        })
    }

    pub(super) fn extract_batch(&mut self, batch: &RecordBatch, budget: &Budget) -> Result<()> {
        let list_arr = batch
            .column_by_name(&self.spec.list_column)
            .ok_or_else(invalid_export)?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(invalid_export)?;
        let struct_arr = list_arr
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(invalid_export)?;
        let parent_keys = self
            .spec
            .parent_key_columns
            .iter()
            .map(|pk| batch.column_by_name(pk).cloned().ok_or_else(invalid_export))
            .collect::<Result<Vec<_>>>()?;

        let chunk_cap = self.spec.row_group_rows.max(1);
        let mut parent_idx_builder = UInt32Builder::with_capacity(chunk_cap);
        let mut ordinal_builder = Int64Builder::with_capacity(chunk_cap);
        let mut value_idx_builder = UInt32Builder::with_capacity(chunk_cap);
        let mut count_in_chunk = 0usize;

        let offsets = list_arr.value_offsets();
        for row in 0..batch.num_rows() {
            if list_arr.is_null(row) {
                continue;
            }
            let start = usize::try_from(offsets[row]).unwrap_or(0);
            let end = usize::try_from(offsets[row + 1]).unwrap_or(0);
            let row_u32 = u32::try_from(row).map_err(|_| invalid_export())?;
            for (ordinal, val_idx) in (start..end).enumerate() {
                if struct_arr.is_null(val_idx) {
                    continue;
                }
                let val_u32 = u32::try_from(val_idx).map_err(|_| invalid_export())?;
                parent_idx_builder.append_value(row_u32);
                ordinal_builder.append_value(ordinal as i64);
                value_idx_builder.append_value(val_u32);
                count_in_chunk += 1;
                if count_in_chunk >= chunk_cap {
                    self.flush_chunk(
                        &parent_keys,
                        struct_arr,
                        &mut parent_idx_builder,
                        &mut ordinal_builder,
                        &mut value_idx_builder,
                        budget,
                    )?;
                    count_in_chunk = 0;
                }
            }
        }
        if count_in_chunk > 0 {
            self.flush_chunk(
                &parent_keys,
                struct_arr,
                &mut parent_idx_builder,
                &mut ordinal_builder,
                &mut value_idx_builder,
                budget,
            )?;
        }
        Ok(())
    }

    fn flush_chunk(
        &mut self,
        parent_keys: &[ArrayRef],
        struct_arr: &StructArray,
        parent_idx_builder: &mut UInt32Builder,
        ordinal_builder: &mut Int64Builder,
        value_idx_builder: &mut UInt32Builder,
        budget: &Budget,
    ) -> Result<()> {
        let parent_indices = parent_idx_builder.finish();
        let ordinals = ordinal_builder.finish();
        let value_indices = value_idx_builder.finish();

        let mut cols = Vec::with_capacity(self.child_schema.fields().len());
        for pk_arr in parent_keys {
            cols.push(take(pk_arr.as_ref(), &parent_indices, None)?);
        }
        cols.push(Arc::new(ordinals));
        for &s_idx in &self.struct_indices {
            let sub_arr = struct_arr.column(s_idx);
            let taken = take(sub_arr.as_ref(), &value_indices, None)?;
            let target_field = self.child_schema.field(cols.len());
            let sanitized = sanitize_export_array(&taken, target_field.data_type())?;
            cols.push(sanitized);
        }

        let child_batch = RecordBatch::try_new(self.child_schema.clone(), cols)?;
        for slice in storage::split_oversized_batch(child_batch, budget.limits.max_batch_bytes)? {
            let mem = budget.reserve(storage::effective_batch_bytes(&slice))?;
            self.reservations.push(mem);
            self.batches.push(slice);
        }
        Ok(())
    }

    pub(super) async fn finish_and_write(
        mut self,
        owner: &Id,
        directory: &std::path::Path,
        is_staged: bool,
        budget: &Budget,
        workspace_root: &std::path::Path,
    ) -> Result<ChildTableExport> {
        let mut sort_exprs = Vec::new();
        let mut page_index_cols: Vec<String> = Vec::new();
        for ck in self.spec.effective_child_key_columns() {
            sort_exprs.push(col(quote(ck)).sort(true, false));
            page_index_cols.push(ck.clone());
        }
        sort_exprs.push(col(quote(&self.spec.ordinal_column)).sort(true, false));
        page_index_cols.push(self.spec.ordinal_column.clone());

        let file_name = if is_staged {
            format!("stage-child-{}-{}.parquet", self.name, uuid::Uuid::new_v4())
        } else {
            format!("{}.parquet", self.name)
        };
        let path = directory.join(file_name);
        let rg_rows = self.spec.row_group_rows.max(1);
        let max_row_group_bytes = budget.limits.max_batch_bytes.max(1);
        let schema = self.child_schema.clone();
        let fields = self.child_fields.clone();
        let owner_clone = owner.clone();
        let dir_buf = directory.to_path_buf();
        let limits = budget.limits.clone();
        let page_index_cols_clone = page_index_cols.clone();
        let path_for_create = path.clone();

        let primitive_leaves = count_primitive_leaves(&schema);
        let footer_group_bytes = fields.iter().fold(
            1024_usize.saturating_add(primitive_leaves.saturating_mul(1024)),
            |size, field| size.saturating_add(field.name.len() + field.data_type.len()),
        );

        let stream_opt = if self.batches.is_empty() {
            self.reservations.clear();
            None
        } else {
            let context = budget.context();
            let raw_batches = std::mem::take(&mut self.batches);
            let mem_table = MemTable::try_new(schema.clone(), vec![raw_batches])?;
            context.register_table("child_raw", Arc::new(mem_table))?;
            let df = context.table("child_raw").await?.sort(sort_exprs)?;
            self.reservations.clear();
            Some(crate::query::bounded(df.execute_stream().await?, budget)?)
        };

        let memory = budget.reserve(
            limits
                .max_batch_bytes
                .saturating_mul(2)
                .saturating_add(limits.max_metadata_bytes),
        )?;

        let mut writer = budget
            .blocking(move || {
                std::fs::create_dir_all(&dir_buf)?;
                let output = HashingFile::create(&path_for_create)?;
                let properties = build_writer_properties(
                    Some(rg_rows),
                    max_row_group_bytes,
                    &page_index_cols_clone,
                )?;
                Ok((
                    ArrowWriter::try_new(output, schema, Some(properties))?,
                    memory,
                ))
            })
            .await?;

        let mut tracker = BrowseRangeTracker::new(&page_index_cols);
        let mut total_rows = 0_u64;

        if let Some(stream) = stream_opt {
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }
                for slice in storage::split_oversized_batch(batch, budget.limits.max_batch_bytes)? {
                    total_rows = total_rows
                        .checked_add(slice.num_rows() as u64)
                        .ok_or_else(invalid_export)?;
                    let limits = budget.limits.clone();
                    let root_buf = workspace_root.to_path_buf();
                    let moved_tracker =
                        std::mem::replace(&mut tracker, BrowseRangeTracker::new(&[]));
                    let (next_writer, next_tracker) = budget
                        .blocking(move || {
                            let mut tracker = moved_tracker;
                            let reserve = limits
                                .scratch_bytes
                                .saturating_add((limits.max_batch_bytes as u64).saturating_mul(4));
                            if fs2::available_space(&root_buf)? < reserve {
                                return Err(Error::refusal(
                                    "disk_capacity",
                                    "child export output and declared scratch no longer fit available disk",
                                ));
                            }
                            let mut offset = 0usize;
                            while offset < slice.num_rows() {
                                let rem = slice.num_rows() - offset;
                                let in_prog = writer.0.in_progress_rows();
                                let take_len = rem.min(rg_rows.saturating_sub(in_prog).max(1));
                                let sub_batch = slice.slice(offset, take_len);
                                offset += take_len;

                                let prev_flushed = writer.0.flushed_row_groups().len();
                                writer.0.write(&sub_batch)?;
                                tracker.observe_batch(&sub_batch)?;

                                let flushed_now = writer
                                    .0
                                    .flushed_row_groups()
                                    .len()
                                    .saturating_sub(prev_flushed);
                                for _ in 0..flushed_now {
                                    tracker.finish_row_group();
                                }

                                if flushed_now == 0
                                    && (writer.0.in_progress_rows() >= rg_rows
                                        || writer.0.memory_size() >= limits.max_batch_bytes
                                        || writer.0.in_progress_size() >= max_row_group_bytes)
                                    && writer.0.in_progress_rows() > 0
                                {
                                    writer.0.flush()?;
                                    tracker.finish_row_group();
                                }
                                if writer
                                    .0
                                    .flushed_row_groups()
                                    .len()
                                    .saturating_mul(footer_group_bytes)
                                    > limits.max_metadata_bytes
                                {
                                    return Err(Error::refusal(
                                        "metadata_capacity",
                                        "estimated child export footer exceeds the declared metadata allowance",
                                    ));
                                }
                            }
                            Ok((writer, tracker))
                        })
                        .await?;
                    writer = next_writer;
                    tracker = next_tracker;
                }
            }
        }

        let max_metadata_bytes = budget.limits.max_metadata_bytes;
        let path_for_finish = path.clone();
        let (object, browse_ranges): (crate::model::ObjectRef, Vec<ExportKeyRange>) = budget
            .blocking(
                move || -> Result<(crate::model::ObjectRef, Vec<ExportKeyRange>)> {
                    let (mut output, _memory) = writer;
                    let groups = output
                        .flushed_row_groups()
                        .len()
                        .saturating_add(usize::from(output.in_progress_rows() != 0));
                    if groups.saturating_mul(footer_group_bytes) > max_metadata_bytes {
                        return Err(Error::refusal(
                            "metadata_capacity",
                            "estimated child export footer exceeds the declared metadata allowance",
                        ));
                    }
                    if output.in_progress_rows() > 0 {
                        tracker.finish_row_group();
                    }
                    let _metadata = output.finish()?;
                    let object = output.inner().finish(&owner_clone, &path_for_finish)?;
                    Ok((object, tracker.into_ranges()))
                },
            )
            .await?;

        let validation = validate_output_object(&object, total_rows, &fields, budget).await?;

        Ok(ChildTableExport {
            object,
            rows: total_rows,
            fields,
            validation,
            browse_ranges,
        })
    }
}
