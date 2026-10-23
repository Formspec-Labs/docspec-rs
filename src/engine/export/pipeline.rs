//! Extended multi-group, filtered, computed, folded, sorted, faceted, and child-table export pipeline.

mod child;
mod facets;
mod parent;
mod slice;

pub use slice::{ChildSliceQuery, build_page_index_selection};

use self::{
    child::ChildAccumulator,
    facets::{BrowseRangeTracker, FacetAccumulator},
    parent::*,
};
use super::{
    ExportMaterialized, ExportOperation, HashingFile, count_primitive_leaves, describe_schema,
    invalid_export, sanitize_export_array, validate_output_object,
};
use crate::{
    Error, Result,
    engine::{
        Engine, Protection,
        expr::{compile_df_predicate, compile_df_scalar_expr_with_hint},
        record, stop_after,
        write::identifier as quote,
    },
    evidence::{EVIDENCE, SOURCE_FILE, SOURCE_ROW},
    export::{
        ExportField, ExportKeyRange, ExportPolicy, ReadingsFoldSpec, ScalarExpr, SortColumn,
        SortDirection,
    },
    model::{Id, NativeSnapshotRef},
    storage,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        Int64Array, Int64Builder, LargeStringArray, ListArray, ListBuilder, StringArray,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{common::ScalarValue, logical_expr::col, prelude::SessionContext};
use futures::TryStreamExt;
use parquet::{
    arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY},
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    schema::types::ColumnPath,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    path::PathBuf,
    sync::Arc,
};

impl Engine {
    pub(super) async fn materialize_extended_export(
        &self,
        operation: &ExportOperation,
        staged_directory: Option<PathBuf>,
        stop: Option<&str>,
    ) -> Result<ExportMaterialized> {
        let spec = &operation.request;
        let policy = &spec.policy;
        let budget = &self.inner.lifecycle.budget;

        let is_staged = staged_directory.is_some();
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = staged_directory.unwrap_or_else(|| {
            self.inner
                .root
                .join("objects")
                .join(spec.dataset.as_str())
                .join(id.as_str())
        });
        if !is_staged {
            let lock = self.lock(&spec.dataset).await;
            let _guard = lock.lock().await;
            let view = self.inner.lifecycle.open(&spec.dataset).await?;
            self.check_export_authority(&view, operation).await?;
            self.inner
                .lifecycle
                .commit(
                    &view,
                    vec![record(
                        format!("protection/{id}"),
                        Protection {
                            id,
                            consumer: spec.dataset.clone(),
                            purpose: format!("{}/export", spec.operation),
                            paths: vec![format!("{}/", directory.display())],
                            retired: false,
                        },
                    )?],
                )
                .await?;
        }

        // 1. Register and join all requested groups in DataFusion.
        let (context, df, source_field_map, expected_unfiltered_rows) =
            self.build_joined_export_dataframe(operation).await?;

        // 2. Apply optional row filter (`policy.filter`).
        let df = if let Some(predicate) = &policy.filter {
            let expr = compile_df_predicate(&context, df.schema(), predicate)?;
            df.filter(expr)?
        } else {
            df
        };

        // 3. Apply optional `computed_columns`.
        let (df, computed_field_names) = if !policy.computed_columns.is_empty() {
            apply_computed_columns(&context, df, &policy.computed_columns)?
        } else {
            (df, BTreeSet::new())
        };

        // 4. Verify all required input columns exist and do not disclose `__docspec_*`.
        let joined_arrow_schema = Arc::new(df.schema().as_arrow().clone());
        validate_pipeline_columns(&joined_arrow_schema, policy, &computed_field_names)?;

        let mut seen_ids = BTreeSet::new();
        let multi_or_synthesized = policy.effective_groups().len() > 1
            || !policy.computed_columns.is_empty()
            || policy.fold_readings.is_some()
            || !policy.child_tables.is_empty();

        let parent_schema = build_parent_output_schema(
            &joined_arrow_schema,
            policy,
            &source_field_map,
            &mut seen_ids,
            multi_or_synthesized,
        )?;
        let parent_fields = describe_schema(&parent_schema)?;
        let page_index_cols = resolve_scalar_columns(
            &policy.page_index_columns,
            &policy.order_by,
            &parent_schema,
            "page_index_column",
        )?;
        let browse_cols = resolve_scalar_columns(
            &policy.browse_range_columns,
            &policy.order_by,
            &parent_schema,
            "browse_range_column",
        )?;

        // 5. Push `policy.order_by` into DataFusion's spill-enabled `SortExec`.
        let df = if !policy.order_by.is_empty() {
            let mut sort_exprs = Vec::with_capacity(policy.order_by.len());
            for sc in &policy.order_by {
                let field = parent_schema.field_with_name(&sc.field).map_err(|_| {
                    Error::refusal(
                        "invalid_export",
                        format!(
                            "order_by column {} is absent from the exported schema",
                            sc.field
                        ),
                    )
                })?;
                if !is_scalar_data_type(field.data_type()) {
                    return Err(Error::refusal(
                        "invalid_export",
                        format!("order_by column {} must be a scalar column", sc.field),
                    ));
                }
                let asc = sc.direction == SortDirection::Asc;
                let nulls_first = sc.nulls_first();
                if let Some(child_spec) = policy
                    .child_tables
                    .values()
                    .find(|c| c.replace_with_count_column.as_ref() == Some(&sc.field))
                {
                    let count_expr = context.parse_sql_expr(
                        &format!(
                            "COALESCE(CAST(ARRAY_LENGTH({}) AS BIGINT), 0)",
                            quote(&child_spec.list_column)
                        ),
                        df.schema(),
                    )?;
                    sort_exprs.push(count_expr.sort(asc, nulls_first));
                } else {
                    sort_exprs.push(col(quote(&sc.field)).sort(asc, nulls_first));
                }
            }
            df.sort(sort_exprs)?
        } else {
            df
        };

        // 6. Initialize child accumulators, facet accumulator, and streaming parent writer.
        let mut child_accumulators = BTreeMap::new();
        for (child_name, child_spec) in &policy.child_tables {
            child_accumulators.insert(
                child_name.clone(),
                ChildAccumulator::new(
                    child_name.clone(),
                    child_spec.clone(),
                    &joined_arrow_schema,
                    &source_field_map,
                )?,
            );
        }
        let mut facet_accumulator =
            FacetAccumulator::new(&policy.facet_sets, budget.limits.max_metadata_bytes);

        let (object, rows, browse_ranges, custody) = self
            .stream_parent_and_extract_children(
                operation,
                &directory,
                is_staged,
                stop,
                df,
                parent_schema,
                &parent_fields,
                policy.row_group_rows,
                &page_index_cols,
                &browse_cols,
                &mut child_accumulators,
                &mut facet_accumulator,
            )
            .await?;

        if policy.filter.is_none()
            && let Some(expected_rows) = expected_unfiltered_rows
            && rows != expected_rows
        {
            return Err(Error::refusal(
                "row_count_mismatch",
                "export rows differ from the retained native group",
            ));
        }

        // 7. Sort, stream, and validate child Parquet files (`{child_name}.parquet`).
        let mut child_exports = BTreeMap::new();
        for (child_name, acc) in child_accumulators {
            let child_export = acc
                .finish_and_write(
                    &spec.dataset,
                    &directory,
                    is_staged,
                    budget,
                    &self.inner.root,
                )
                .await?;
            child_exports.insert(child_name, child_export);
        }

        let facets = facet_accumulator.finish();
        let materialized = ExportMaterialized {
            request_digest: operation.request_digest.clone(),
            object,
            rows,
            fields: parent_fields,
            facets,
            browse_ranges,
            child_tables: child_exports,
        };
        if let Some(custody) = custody {
            self.retain_export_record(
                operation,
                &format!(
                    "export-host-closed/{}/{}/{}",
                    custody.operation, custody.operation_revision, custody.id
                ),
                &materialized,
            )
            .await?;
        }
        Ok(materialized)
    }

    async fn build_joined_export_dataframe(
        &self,
        operation: &ExportOperation,
    ) -> Result<(
        SessionContext,
        datafusion::prelude::DataFrame,
        HashMap<String, Field>,
        Option<u64>,
    )> {
        let policy = &operation.request.policy;
        let groups = policy.effective_groups();
        let dataset = &operation.source.dataset;
        let budget = &self.inner.lifecycle.budget;
        let context = budget.context();
        self.inner
            .objects
            .register_query_store(&context.runtime_env())?;

        let mut source_field_map: HashMap<String, Field> = HashMap::new();
        let mut select_exprs: Vec<String> = Vec::new();
        let mut seen_public_names: BTreeSet<String> = BTreeSet::new();

        let anchor_group_idx = groups
            .iter()
            .position(|g| g != "evidence" && g != "receipts");
        let anchor_idx = anchor_group_idx.unwrap_or(0);
        let anchor_alias = format!("g{anchor_idx}");
        let mut expected_rows = None;
        let need_occurrences = groups.len() > 1;

        for (idx, group_name) in groups.iter().enumerate() {
            let alias = format!("g{idx}");
            if group_name == "evidence" || group_name == "receipts" {
                let evidence = dataset.evidence.as_ref().ok_or_else(|| {
                    Error::refusal(
                        "missing_group",
                        "export policy names an absent receipt table",
                    )
                })?;
                if let Some(source_pin) = dataset.groups.get("source")
                    && evidence.source != NativeSnapshotRef::from(source_pin)
                {
                    return Err(invalid_export());
                }
                let (native, _) = self
                    .inner
                    .objects
                    .open_snapshot(&evidence.table, budget)
                    .await?;
                let arrow_schema = storage::schema::read_schema(&native)?;
                if let Ok(ev_field) = arrow_schema.field_with_name(EVIDENCE) {
                    if !seen_public_names.insert(EVIDENCE.to_owned()) {
                        return Err(Error::refusal(
                            "invalid_export",
                            "joined export groups contain duplicate column names",
                        ));
                    }
                    source_field_map.insert(EVIDENCE.to_owned(), ev_field.clone());
                }
                let native = self.inner.objects.bind_table(&native)?.into_native();
                context.register_table(
                    &alias,
                    crate::query::table(native, budget.clone(), Some(evidence.table.rows))?,
                )?;
                if idx == anchor_idx {
                    expected_rows = Some(evidence.table.rows);
                }
                select_exprs.push(format!("{alias}.{}", quote(EVIDENCE)));
            } else if group_name == "source" {
                let source_pin = dataset.groups.get("source").ok_or_else(|| {
                    Error::refusal("missing_group", "export policy names an absent field group")
                })?;
                let (native, _) = self.inner.objects.open_snapshot(source_pin, budget).await?;
                let arrow_schema = storage::schema::read_schema(&native)?;
                for field in arrow_schema.fields() {
                    let name = field.name();
                    if storage::defaults::supplied_id(name).is_none()
                        && !storage::identity::hidden(name)
                        && !name.to_ascii_lowercase().starts_with("__docspec_")
                        && (dataset.related.is_none()
                            || !crate::engine::refresh::hidden_field(name))
                    {
                        if !seen_public_names.insert(name.clone()) {
                            return Err(Error::refusal(
                                "invalid_export",
                                "joined export groups contain duplicate column names",
                            ));
                        }
                        source_field_map.insert(name.clone(), field.as_ref().clone());
                        select_exprs.push(format!("{alias}.{}", quote(name)));
                    }
                }
                let native = self.inner.objects.bind_table(&native)?.into_native();
                let provider = if need_occurrences {
                    crate::query::occurrences(native, budget.clone(), Some(source_pin.rows))?
                } else {
                    crate::query::table(native, budget.clone(), Some(source_pin.rows))?
                };
                context.register_table(&alias, provider)?;
                if idx == anchor_idx {
                    expected_rows = Some(source_pin.rows);
                }
            } else {
                let group_pin = dataset.groups.get(group_name).ok_or_else(|| {
                    Error::refusal("missing_group", "export policy names an absent field group")
                })?;
                let (native, _) = self.inner.objects.open_snapshot(group_pin, budget).await?;
                let arrow_schema = storage::schema::read_schema(&native)?;
                for field in arrow_schema.fields() {
                    let name = field.name();
                    if !storage::identity::hidden(name)
                        && !name.to_ascii_lowercase().starts_with("__docspec_")
                    {
                        if !seen_public_names.insert(name.clone()) {
                            return Err(Error::refusal(
                                "invalid_export",
                                "joined export groups contain duplicate column names",
                            ));
                        }
                        source_field_map.insert(name.clone(), field.as_ref().clone());
                        select_exprs.push(format!("{alias}.{}", quote(name)));
                    }
                }
                let native = self.inner.objects.bind_table(&native)?.into_native();
                context.register_table(
                    &alias,
                    crate::query::table(native, budget.clone(), Some(group_pin.rows))?,
                )?;
                if idx == anchor_idx {
                    expected_rows = Some(group_pin.rows);
                }
            }
        }

        if select_exprs.is_empty() {
            return Err(invalid_export());
        }

        let mut from_sql = anchor_alias.clone();
        for (idx, group_name) in groups.iter().enumerate() {
            if idx == anchor_idx {
                continue;
            }
            let alias = format!("g{idx}");
            if group_name == "evidence" || group_name == "receipts" {
                from_sql.push_str(&format!(
                    " LEFT JOIN {alias} ON {anchor_alias}.{} = {alias}.{} AND {anchor_alias}.{} = {alias}.{}",
                    quote(storage::OCCURRENCE_FILE),
                    quote(SOURCE_FILE),
                    quote(storage::OCCURRENCE_ROW),
                    quote(SOURCE_ROW),
                ));
            } else {
                from_sql.push_str(&format!(
                    " INNER JOIN {alias} ON {anchor_alias}.{} = {alias}.{} AND {anchor_alias}.{} = {alias}.{}",
                    quote(storage::OCCURRENCE_FILE),
                    quote(storage::OCCURRENCE_FILE),
                    quote(storage::OCCURRENCE_ROW),
                    quote(storage::OCCURRENCE_ROW),
                ));
            }
        }

        let sql = format!("SELECT {} FROM {from_sql}", select_exprs.join(", "));
        let df = context.sql(&sql).await?;
        Ok((context, df, source_field_map, expected_rows))
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_parent_and_extract_children(
        &self,
        operation: &ExportOperation,
        directory: &std::path::Path,
        is_staged: bool,
        stop: Option<&str>,
        df: datafusion::prelude::DataFrame,
        schema: SchemaRef,
        fields: &[ExportField],
        row_group_rows: Option<usize>,
        page_index_cols: &[String],
        browse_cols: &[String],
        child_accumulators: &mut BTreeMap<String, ChildAccumulator>,
        facet_accumulator: &mut FacetAccumulator,
    ) -> Result<(
        crate::model::ObjectRef,
        u64,
        Vec<ExportKeyRange>,
        Option<super::host::HostFile>,
    )> {
        let budget = &self.inner.lifecycle.budget;
        let spec = &operation.request;
        let policy = &spec.policy;
        let memory = budget.reserve(
            budget
                .limits
                .max_batch_bytes
                .saturating_mul(4)
                .saturating_add(budget.limits.max_metadata_bytes),
        )?;
        let path = directory.join(if is_staged {
            format!("stage-{}.parquet", uuid::Uuid::new_v4())
        } else {
            "export.parquet".into()
        });
        let initial_path = path.clone();
        let output_schema = schema.clone();
        let max_row_group_bytes = budget.limits.max_batch_bytes.max(1);
        let dir_buf = directory.to_path_buf();
        let page_index_cols_owned = page_index_cols.to_vec();
        let (output, properties, memory, native) = budget
            .blocking(move || {
                let _memory = memory;
                std::fs::create_dir_all(&dir_buf)?;
                let output = HashingFile::create(&initial_path)?;
                let properties = build_writer_properties(
                    row_group_rows,
                    max_row_group_bytes,
                    &page_index_cols_owned,
                )?;
                let native = if cfg!(unix) {
                    output.file.sync_all()?;
                    File::open(initial_path.parent().ok_or_else(invalid_export)?)?.sync_all()?;
                    Some(super::host::native_identity(&output.file)?)
                } else {
                    None
                };
                Ok((output, properties, _memory, native))
            })
            .await?;
        let custody = if let Some((device, inode)) = native {
            Some(
                self.retain_host_file(operation, &path, device, inode)
                    .await?,
            )
        } else {
            None
        };
        stop_after(stop, "host-created")?;
        let mut writer = budget
            .blocking(move || {
                Ok((
                    ArrowWriter::try_new(output, output_schema, Some(properties))?,
                    memory,
                ))
            })
            .await?;
        #[cfg(feature = "test-util")]
        crate::engine::work_started("export-materialization");

        let primitive_leaves = count_primitive_leaves(&schema);
        let footer_group_bytes = fields.iter().fold(
            1024_usize.saturating_add(primitive_leaves.saturating_mul(1024)),
            |size, field| size.saturating_add(field.name.len() + field.data_type.len()),
        );

        let stream = crate::query::bounded(df.execute_stream().await?, budget)?;
        futures::pin_mut!(stream);

        let mut tracker = BrowseRangeTracker::new(browse_cols);
        let mut rows = 0_u64;
        let max_rg_rows = row_group_rows
            .unwrap_or(if !page_index_cols.is_empty() {
                32768
            } else {
                1024 * 1024
            })
            .max(1);

        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            for acc in child_accumulators.values_mut() {
                acc.extract_batch(&batch, budget)?;
            }
            let parent_batch = transform_to_parent_batch(&batch, &schema, policy)?;
            facet_accumulator.observe(&batch, &parent_batch)?;

            for slice in
                storage::split_oversized_batch(parent_batch, budget.limits.max_batch_bytes)?
            {
                let columns = slice
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(column, field)| sanitize_export_array(column, field.data_type()))
                    .collect::<Result<Vec<_>>>()?;
                let sanitized_batch = RecordBatch::try_new(schema.clone(), columns)?;
                rows = rows
                    .checked_add(sanitized_batch.num_rows() as u64)
                    .ok_or_else(invalid_export)?;
                let limits = budget.limits.clone();
                let root = self.inner.root.clone();
                #[cfg(feature = "test-util")]
                let stop_payload = stop == Some("host-payload");
                let moved_tracker = std::mem::replace(&mut tracker, BrowseRangeTracker::new(&[]));
                let (next_writer, next_tracker) = budget
                    .blocking(move || {
                        let mut tracker = moved_tracker;
                        let reserve = limits
                            .scratch_bytes
                            .saturating_add((limits.max_batch_bytes as u64).saturating_mul(4));
                        if fs2::available_space(&root)? < reserve {
                            return Err(Error::refusal(
                                "disk_capacity",
                                "export output and declared scratch no longer fit available disk",
                            ));
                        }
                        let mut offset = 0usize;
                        while offset < sanitized_batch.num_rows() {
                            let rem = sanitized_batch.num_rows() - offset;
                            let in_prog = writer.0.in_progress_rows();
                            let take_len = rem.min(max_rg_rows.saturating_sub(in_prog).max(1));
                            let sub_batch = sanitized_batch.slice(offset, take_len);
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

                            let should_flush = {
                                #[cfg(feature = "test-util")]
                                let force = stop_payload;
                                #[cfg(not(feature = "test-util"))]
                                let force = false;
                                force
                                    || writer.0.in_progress_rows() >= max_rg_rows
                                    || writer.0.memory_size() >= limits.max_batch_bytes
                                    || writer.0.in_progress_size() >= max_row_group_bytes
                            };
                            if flushed_now == 0 && should_flush && writer.0.in_progress_rows() > 0 {
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
                                    "estimated flat export footer exceeds the declared metadata allowance",
                                ));
                            }
                        }
                        Ok((writer, tracker))
                    })
                    .await?;
                writer = next_writer;
                tracker = next_tracker;
                stop_after(stop, "host-payload")?;
            }
        }

        let owner = spec.dataset.clone();
        let max_metadata_bytes = budget.limits.max_metadata_bytes;
        let path_clone = path.clone();
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
                            "estimated flat export footer exceeds the declared metadata allowance",
                        ));
                    }
                    if output.in_progress_rows() > 0 {
                        tracker.finish_row_group();
                    }
                    let _metadata = output.finish()?;
                    let obj = output.inner().finish(&owner, &path_clone)?;
                    Ok((obj, tracker.into_ranges()))
                },
            )
            .await?;

        Ok((object, rows, browse_ranges, custody))
    }
}

pub(super) fn build_writer_properties(
    row_group_rows: Option<usize>,
    max_row_group_bytes: usize,
    page_index_cols: &[String],
) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(row_group_rows)
        .set_max_row_group_bytes(Some(max_row_group_bytes))
        .set_data_page_size_limit(1024 * 1024)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES))
        .set_column_index_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES));
    if page_index_cols.is_empty() {
        builder = builder.set_offset_index_disabled(true);
    } else {
        builder = builder.set_write_batch_size(256);
    }
    for col_name in page_index_cols {
        let col_path = ColumnPath::from(col_name.as_str());
        builder = builder
            .set_column_statistics_enabled(col_path.clone(), EnabledStatistics::Page)
            .set_column_data_page_size_limit(col_path, 16 * 1024);
    }
    Ok(builder.build())
}

fn apply_computed_columns(
    context: &SessionContext,
    df: datafusion::prelude::DataFrame,
    computed: &BTreeMap<String, ScalarExpr>,
) -> Result<(datafusion::prelude::DataFrame, BTreeSet<String>)> {
    let df_schema = df.schema().clone();
    let existing_names = df_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let mut exprs = Vec::new();
    let mut computed_names = BTreeSet::new();
    let mut replaced = BTreeSet::new();

    for col_name in &existing_names {
        if let Some(scalar_expr) = computed.get(col_name) {
            let compiled =
                compile_df_scalar_expr_with_hint(context, &df_schema, scalar_expr, Some(col_name))?;
            exprs.push(compiled.alias(col_name));
            computed_names.insert(col_name.clone());
            replaced.insert(col_name.clone());
        } else {
            exprs.push(col(quote(col_name)));
        }
    }
    for (col_name, scalar_expr) in computed {
        if !replaced.contains(col_name) {
            let compiled =
                compile_df_scalar_expr_with_hint(context, &df_schema, scalar_expr, Some(col_name))?;
            exprs.push(compiled.alias(col_name));
            computed_names.insert(col_name.clone());
        }
    }
    Ok((df.select(exprs)?, computed_names))
}
