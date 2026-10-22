//! Extended multi-group, filtered, computed, folded, sorted, faceted, and child-table export pipeline.

use super::{
    ExportMaterialized, ExportOperation, HashingFile, count_primitive_leaves, describe_schema,
    invalid_export, sanitize_export_array, validate_output_object,
};
use crate::{
    Error, Result,
    budget::Budget,
    engine::{Engine, Protection, record, stop_after, write::identifier as quote},
    evidence::{EVIDENCE, SOURCE_FILE, SOURCE_ROW},
    export::{
        ChildTableExport, ChildTableExportSpec, ExportField, ExportKeyRange, ExportPin,
        ExportPolicy, FacetCountEntry, Predicate, ReadingsFoldSpec, ScalarExpr, SortDirection,
        TaggedPredicate, TaggedScalarExpr,
    },
    model::{Id, NativeSnapshotRef},
    storage,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        Int64Array, Int64Builder, LargeStringArray, ListArray, ListBuilder, StringArray,
        StringBuilder, StructArray, StructBuilder, UInt32Builder,
    },
    compute::take,
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{
        DFSchema, ScalarValue,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::MemTable,
    logical_expr::{BinaryExpr, Case, Expr, Operator, Volatility, col, lit},
    physical_plan::SendableRecordBatchStream,
    prelude::SessionContext,
};
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder,
        arrow_reader::{
            ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
        },
        async_reader::AsyncFileReader,
    },
    basic::{Compression, ZstdLevel},
    file::{
        metadata::PageIndexPolicy,
        page_index::column_index::ColumnIndexMetaData,
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
    },
    schema::types::ColumnPath,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    path::PathBuf,
    sync::Arc,
};

impl Engine {
    /// Streams an unnested child Parquet table produced alongside an [`ExportPin`].
    ///
    /// # Errors
    /// Refuses unknown child tables, inconsistent pins, or unavailable objects.
    pub async fn read_export_child(
        &self,
        pin: &ExportPin,
        child_name: &str,
    ) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(&(pin, child_name))?;
        let pin = pin.clone();
        let child_name = child_name.to_owned();
        self.admit_stream(memory, move |engine| async move {
            let retained = engine
                .load_export_pin(&pin.dataset, &format!("export-result/{}", pin.id))
                .await?;
            if retained != pin {
                return Err(invalid_export());
            }
            let child = retained.child_tables.get(&child_name).ok_or_else(|| {
                Error::refusal("missing_child_export", "requested child table is not in export pin")
            })?;
            let stream = engine
                .inner
                .objects
                .read_flat_parquet(&child.object, &engine.inner.lifecycle.budget)
                .await?;
            if describe_schema(&stream.schema())? != child.fields {
                return Err(invalid_export());
            }
            Ok(stream)
        })
        .await
    }

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
                Error::refusal("missing_child_export", "requested child table is not in export pin")
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
        let end_ordinal =
            start_ordinal.saturating_add(i64::try_from(limit).unwrap_or(i64::MAX));
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
                slice_child_parquet_async(
                    reader,
                    &browse_ranges,
                    &key_col,
                    &ord_col,
                    &key_value,
                    start_ordinal,
                    end_ordinal,
                    limit,
                )
                .await
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
                        slice_child_parquet(
                            &path,
                            &browse_ranges,
                            &key_col,
                            &ord_col,
                            &key_value,
                            start_ordinal,
                            end_ordinal,
                            limit,
                        )
                    })
                    .await
            }
        })
        .await
    }

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
            let expr = compile_predicate(&context, df.schema(), predicate)?;
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
        let page_index_cols = resolve_page_index_columns(&parent_schema, policy)?;
        let browse_cols = resolve_browse_range_columns(&parent_schema, policy)?;

        // 5. Push `policy.order_by` into DataFusion's spill-enabled `SortExec`.
        let df = if !policy.order_by.is_empty() {
            let mut sort_exprs = Vec::with_capacity(policy.order_by.len());
            for sc in &policy.order_by {
                let field = parent_schema.field_with_name(&sc.column).map_err(|_| {
                    Error::refusal(
                        "invalid_export",
                        format!("order_by column {} is absent from the exported schema", sc.column),
                    )
                })?;
                if !is_scalar_data_type(field.data_type()) {
                    return Err(Error::refusal(
                        "invalid_export",
                        format!("order_by column {} must be a scalar column", sc.column),
                    ));
                }
                let asc = sc.direction == SortDirection::Asc;
                if let Some(child_spec) = policy
                    .child_tables
                    .values()
                    .find(|c| c.replace_with_count_column.as_ref() == Some(&sc.column))
                {
                    let count_expr = context.parse_sql_expr(
                        &format!(
                            "COALESCE(CAST(ARRAY_LENGTH({}) AS BIGINT), 0)",
                            quote(&child_spec.list_column)
                        ),
                        df.schema(),
                    )?;
                    sort_exprs.push(count_expr.sort(asc, sc.nulls_first));
                } else {
                    sort_exprs.push(col(quote(&sc.column)).sort(asc, sc.nulls_first));
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
                    Error::refusal("missing_group", "export policy names an absent receipt table")
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
                let (native, _) = self
                    .inner
                    .objects
                    .open_snapshot(source_pin, budget)
                    .await?;
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
                let (native, _) = self
                    .inner
                    .objects
                    .open_snapshot(group_pin, budget)
                    .await?;
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
        let max_rg_rows = row_group_rows.unwrap_or(1024 * 1024).max(1);

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

                            writer.0.write(&sub_batch)?;
                            tracker.observe_batch(&sub_batch)?;

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
                            if should_flush && writer.0.in_progress_rows() > 0 {
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
        drop(stream);

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

fn build_writer_properties(
    _row_group_rows: Option<usize>,
    _max_row_group_bytes: usize,
    page_index_cols: &[String],
) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(None)
        .set_max_row_group_bytes(None)
        .set_data_page_size_limit(1024 * 1024)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES))
        .set_column_index_truncate_length(Some(storage::STATISTICS_TRUNCATE_BYTES))
        .set_offset_index_disabled(true);
    if !page_index_cols.is_empty() {
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

fn compile_predicate(
    context: &SessionContext,
    df_schema: &DFSchema,
    predicate: &Predicate,
) -> Result<Expr> {
    let expr = match predicate {
        Predicate::SqlString(sql) => context.parse_sql_expr(sql, df_schema)?,
        Predicate::Ast(ast) => compile_tagged_predicate(context, df_schema, ast)?,
    };
    validate_export_expr(&expr)?;
    Ok(expr)
}

fn scalar_column_hint(s: &ScalarExpr) -> Option<&str> {
    if let ScalarExpr::Ast(TaggedScalarExpr::Column { name }) = s {
        Some(name.as_str())
    } else {
        None
    }
}

fn compile_tagged_predicate(
    context: &SessionContext,
    df_schema: &DFSchema,
    ast: &TaggedPredicate,
) -> Result<Expr> {
    let compile_bin = |left: &ScalarExpr, right: &ScalarExpr, op: Operator| -> Result<Expr> {
        let l_hint = scalar_column_hint(left);
        let r_hint = scalar_column_hint(right);
        let l_expr = compile_scalar_expr_with_hint(context, df_schema, left, r_hint)?;
        let r_expr = compile_scalar_expr_with_hint(context, df_schema, right, l_hint)?;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(l_expr),
            op,
            Box::new(r_expr),
        )))
    };

    match ast {
        TaggedPredicate::Sql { expression } => Ok(context.parse_sql_expr(expression, df_schema)?),
        TaggedPredicate::Eq { left, right } => compile_bin(left, right, Operator::Eq),
        TaggedPredicate::NotEq { left, right } => compile_bin(left, right, Operator::NotEq),
        TaggedPredicate::Lt { left, right } => compile_bin(left, right, Operator::Lt),
        TaggedPredicate::LtEq { left, right } => compile_bin(left, right, Operator::LtEq),
        TaggedPredicate::Gt { left, right } => compile_bin(left, right, Operator::Gt),
        TaggedPredicate::GtEq { left, right } => compile_bin(left, right, Operator::GtEq),
        TaggedPredicate::IsNull { expr } => {
            Ok(compile_scalar_expr(context, df_schema, expr)?.is_null())
        }
        TaggedPredicate::IsNotNull { expr } => {
            Ok(compile_scalar_expr(context, df_schema, expr)?.is_not_null())
        }
        TaggedPredicate::InList {
            expr,
            list,
            negated,
        } => {
            let col_hint = scalar_column_hint(expr);
            let target = compile_scalar_expr(context, df_schema, expr)?;
            let items = list
                .iter()
                .map(|v| compile_scalar_expr_with_hint(context, df_schema, v, col_hint))
                .collect::<Result<Vec<_>>>()?;
            Ok(target.in_list(items, *negated))
        }
        TaggedPredicate::And { predicates } => {
            let mut iter = predicates.iter();
            let Some(first) = iter.next() else {
                return Err(Error::refusal(
                    "invalid_export",
                    "AND predicate requires at least one branch",
                ));
            };
            let mut acc = compile_predicate(context, df_schema, first)?;
            for next in iter {
                acc = acc.and(compile_predicate(context, df_schema, next)?);
            }
            Ok(acc)
        }
        TaggedPredicate::Or { predicates } => {
            let mut iter = predicates.iter();
            let Some(first) = iter.next() else {
                return Err(Error::refusal(
                    "invalid_export",
                    "OR predicate requires at least one branch",
                ));
            };
            let mut acc = compile_predicate(context, df_schema, first)?;
            for next in iter {
                acc = acc.or(compile_predicate(context, df_schema, next)?);
            }
            Ok(acc)
        }
        TaggedPredicate::Not { predicate } => {
            let inner = compile_predicate(context, df_schema, predicate)?;
            Ok(Expr::Not(Box::new(inner)))
        }
    }
}

fn compile_scalar_expr(
    context: &SessionContext,
    df_schema: &DFSchema,
    scalar: &ScalarExpr,
) -> Result<Expr> {
    compile_scalar_expr_with_hint(context, df_schema, scalar, None)
}

fn compile_scalar_expr_with_hint(
    context: &SessionContext,
    df_schema: &DFSchema,
    scalar: &ScalarExpr,
    column_hint: Option<&str>,
) -> Result<Expr> {
    let expr = match scalar {
        ScalarExpr::SqlString(sql) => context.parse_sql_expr(sql, df_schema)?,
        ScalarExpr::Ast(ast) => compile_tagged_scalar(context, df_schema, ast, column_hint)?,
    };
    validate_export_expr(&expr)?;
    Ok(expr)
}

fn compile_tagged_scalar(
    context: &SessionContext,
    df_schema: &DFSchema,
    ast: &TaggedScalarExpr,
    column_hint: Option<&str>,
) -> Result<Expr> {
    match ast {
        TaggedScalarExpr::Sql { expression } => Ok(context.parse_sql_expr(expression, df_schema)?),
        TaggedScalarExpr::Column { name } => Ok(col(quote(name))),
        TaggedScalarExpr::Literal { value } => {
            json_to_literal_expr(context, df_schema, column_hint, value)
        }
        TaggedScalarExpr::CaseWhen {
            branches,
            else_expr,
        } => {
            if branches.is_empty() {
                return Err(Error::refusal(
                    "invalid_export",
                    "CASE expression requires at least one WHEN branch",
                ));
            }
            let mut when_then_expr = Vec::with_capacity(branches.len());
            for branch in branches {
                let w = compile_predicate(context, df_schema, &branch.when)?;
                let t = compile_scalar_expr_with_hint(context, df_schema, &branch.then, column_hint)?;
                when_then_expr.push((Box::new(w), Box::new(t)));
            }
            let else_box = match else_expr {
                Some(e) => Some(Box::new(compile_scalar_expr_with_hint(
                    context,
                    df_schema,
                    e,
                    column_hint,
                )?)),
                None => Some(Box::new(json_to_literal_expr(
                    context,
                    df_schema,
                    column_hint,
                    &serde_json::Value::Null,
                )?)),
            };
            Ok(Expr::Case(Case::new(None, when_then_expr, else_box)))
        }
        TaggedScalarExpr::Coalesce { exprs } => {
            let args = exprs
                .iter()
                .map(|e| compile_scalar_expr_with_hint(context, df_schema, e, column_hint))
                .collect::<Result<Vec<_>>>()?;
            let list_sql = args
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Ok(context.parse_sql_expr(&format!("COALESCE({list_sql})"), df_schema)?)
        }
        TaggedScalarExpr::Concat { exprs } => {
            let args = exprs
                .iter()
                .map(|e| compile_scalar_expr_with_hint(context, df_schema, e, column_hint))
                .collect::<Result<Vec<_>>>()?;
            let list_sql = args
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Ok(context.parse_sql_expr(&format!("CONCAT({list_sql})"), df_schema)?)
        }
        TaggedScalarExpr::Lower { expr } => {
            let inner = compile_scalar_expr_with_hint(context, df_schema, expr, column_hint)?;
            Ok(context.parse_sql_expr(&format!("LOWER({inner})"), df_schema)?)
        }
        TaggedScalarExpr::Upper { expr } => {
            let inner = compile_scalar_expr_with_hint(context, df_schema, expr, column_hint)?;
            Ok(context.parse_sql_expr(&format!("UPPER({inner})"), df_schema)?)
        }
        TaggedScalarExpr::Trim { expr } => {
            let inner = compile_scalar_expr_with_hint(context, df_schema, expr, column_hint)?;
            Ok(context.parse_sql_expr(&format!("TRIM({inner})"), df_schema)?)
        }
    }
}

fn json_to_literal_expr(
    _context: &SessionContext,
    df_schema: &DFSchema,
    column_hint: Option<&str>,
    value: &serde_json::Value,
) -> Result<Expr> {
    match value {
        serde_json::Value::Null => {
            if let Some(col_name) = column_hint
                && let Ok(field) = df_schema.field_with_unqualified_name(col_name)
                && let Ok(sv) = ScalarValue::try_from(field.data_type())
            {
                return Ok(lit(sv));
            }
            Ok(lit(ScalarValue::Utf8(None)))
        }
        serde_json::Value::Bool(b) => Ok(lit(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if let Some(col_name) = column_hint
                    && let Ok(field) = df_schema.field_with_unqualified_name(col_name)
                    && matches!(field.data_type(), DataType::Int32)
                    && let Ok(i32_val) = i32::try_from(i)
                {
                    return Ok(lit(i32_val));
                }
                Ok(lit(i))
            } else if let Some(f) = n.as_f64() {
                Ok(lit(f))
            } else {
                Err(Error::refusal("invalid_export", "unsupported numeric literal"))
            }
        }
        serde_json::Value::String(s) => Ok(lit(s.clone())),
        _ => Err(Error::refusal(
            "invalid_export",
            "unsupported JSON literal in export expression",
        )),
    }
}

fn validate_export_expr(expr: &Expr) -> Result<()> {
    let mut bad_disclosure = false;
    let mut bad_nonlocal = false;
    let _ = expr.apply(|part| {
        if let Expr::Column(column) = part
            && (storage::identity::hidden(&column.name)
                || column.name.to_ascii_lowercase().starts_with("__docspec_"))
        {
            bad_disclosure = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        let allowed = match part {
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::Negative(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::InList(_) => true,
            Expr::ScalarFunction(func) => {
                func.func.signature().volatility == Volatility::Immutable
                    && func.func.name() != "arrow_metadata"
            }
            _ => false,
        };
        if !allowed {
            bad_nonlocal = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if bad_disclosure {
        return Err(Error::refusal(
            "export_disclosure",
            "internal occurrence and evidence fields cannot be referenced in export expressions",
        ));
    }
    if bad_nonlocal {
        return Err(Error::refusal(
            "invalid_export",
            "export expressions must be row-local and immutable",
        ));
    }
    Ok(())
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
                compile_scalar_expr_with_hint(context, &df_schema, scalar_expr, Some(col_name))?;
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
                compile_scalar_expr_with_hint(context, &df_schema, scalar_expr, Some(col_name))?;
            exprs.push(compiled.alias(col_name));
            computed_names.insert(col_name.clone());
        }
    }
    Ok((df.select(exprs)?, computed_names))
}

fn validate_pipeline_columns(
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
        if fold.target_column.to_ascii_lowercase().starts_with("__docspec_") {
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

fn next_available_id(seen_ids: &mut BTreeSet<i32>) -> i32 {
    let mut candidate = seen_ids.iter().next_back().copied().unwrap_or(0) + 1;
    while !seen_ids.insert(candidate) {
        candidate += 1;
    }
    candidate
}

fn sanitize_or_assign_field(
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
    let values_field = Field::new("values", DataType::List(Arc::new(values_item)), false)
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            values_list_id.to_string(),
        )]));
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

fn build_parent_output_schema(
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
            out_fields.push(
                Field::new(col_name, DataType::Int64, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.into(),
                    id.to_string(),
                )])),
            );
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

fn transform_to_parent_batch(
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
    let mut outer_builder =
        ListBuilder::new(struct_builder).with_field(struct_item_field.clone());

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

fn scalar_value_at(array: &ArrayRef, row: usize) -> Option<String> {
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

fn is_scalar_data_type(dt: &DataType) -> bool {
    !matches!(dt, DataType::List(_) | DataType::Struct(_))
}

fn resolve_page_index_columns(schema: &Schema, policy: &ExportPolicy) -> Result<Vec<String>> {
    if !policy.page_index_columns.is_empty() {
        let mut out = Vec::with_capacity(policy.page_index_columns.len());
        for name in &policy.page_index_columns {
            let field = schema.field_with_name(name).map_err(|_| {
                Error::refusal(
                    "invalid_export",
                    format!("page_index_column {name} is absent from the exported schema"),
                )
            })?;
            if !is_scalar_data_type(field.data_type()) {
                return Err(Error::refusal(
                    "invalid_export",
                    format!("page_index_column {name} must be a scalar column"),
                ));
            }
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
        return Ok(out);
    }
    let mut out = Vec::new();
    for sc in &policy.order_by {
        if let Ok(field) = schema.field_with_name(&sc.column)
            && is_scalar_data_type(field.data_type())
            && !out.contains(&sc.column)
        {
            out.push(sc.column.clone());
        }
    }
    Ok(out)
}

fn resolve_browse_range_columns(schema: &Schema, policy: &ExportPolicy) -> Result<Vec<String>> {
    if !policy.browse_range_columns.is_empty() {
        let mut out = Vec::with_capacity(policy.browse_range_columns.len());
        for name in &policy.browse_range_columns {
            let field = schema.field_with_name(name).map_err(|_| {
                Error::refusal(
                    "invalid_export",
                    format!("browse_range_column {name} is absent from the exported schema"),
                )
            })?;
            if !is_scalar_data_type(field.data_type()) {
                return Err(Error::refusal(
                    "invalid_export",
                    format!("browse_range_column {name} must be a scalar column"),
                ));
            }
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
        return Ok(out);
    }
    let mut out = Vec::new();
    for sc in &policy.order_by {
        if let Ok(field) = schema.field_with_name(&sc.column)
            && is_scalar_data_type(field.data_type())
            && !out.contains(&sc.column)
        {
            out.push(sc.column.clone());
        }
    }
    Ok(out)
}

struct BrowseRangeTracker {
    browse_cols: Vec<String>,
    current_start_row: u64,
    current_rows: u64,
    current_min: BTreeMap<String, String>,
    current_max: BTreeMap<String, String>,
    ranges: Vec<ExportKeyRange>,
}

impl BrowseRangeTracker {
    fn new(browse_cols: &[String]) -> Self {
        Self {
            browse_cols: browse_cols.to_vec(),
            current_start_row: 0,
            current_rows: 0,
            current_min: BTreeMap::new(),
            current_max: BTreeMap::new(),
            ranges: Vec::new(),
        }
    }

    fn observe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
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

    fn finish_row_group(&mut self) {
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

    fn into_ranges(mut self) -> Vec<ExportKeyRange> {
        if self.current_rows > 0 {
            self.finish_row_group();
        }
        self.ranges
    }
}

fn compare_browse_val(dt: &DataType, a: &str, b: &str) -> std::cmp::Ordering {
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

fn array_min_max_strings(array: &ArrayRef) -> (Option<String>, Option<String>) {
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

struct FacetAccumulator {
    sets: Vec<Vec<String>>,
    counts: Vec<BTreeMap<BTreeMap<String, Option<String>>, u64>>,
    estimated_bytes: usize,
    max_metadata_bytes: usize,
}

impl FacetAccumulator {
    fn new(sets: &[Vec<String>], max_metadata_bytes: usize) -> Self {
        Self {
            sets: sets.to_vec(),
            counts: vec![BTreeMap::new(); sets.len()],
            estimated_bytes: 0,
            max_metadata_bytes,
        }
    }

    fn observe(&mut self, joined_batch: &RecordBatch, parent_batch: &RecordBatch) -> Result<()> {
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

    fn finish(self) -> Vec<FacetCountEntry> {
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

struct ChildAccumulator {
    name: String,
    spec: ChildTableExportSpec,
    child_schema: SchemaRef,
    child_fields: Vec<ExportField>,
    struct_indices: Vec<usize>,
    batches: Vec<RecordBatch>,
    reservations: Vec<tokio::sync::OwnedSemaphorePermit>,
}

impl ChildAccumulator {
    fn new(
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
                format!("child table column {} is not a List<Struct>", spec.list_column),
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

    fn extract_batch(&mut self, batch: &RecordBatch, budget: &Budget) -> Result<()> {
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

    async fn finish_and_write(
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
            sort_exprs.push(col(quote(&ck)).sort(true, false));
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

                                writer.0.write(&sub_batch)?;
                                tracker.observe_batch(&sub_batch)?;

                                if (writer.0.in_progress_rows() >= rg_rows
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

fn prune_child_row_groups(
    num_rgs: usize,
    browse_ranges: &[ExportKeyRange],
    key_col: &str,
    ord_col: &str,
    key_value: &str,
    key_i64: Option<i64>,
    start_ordinal: i64,
    end_ordinal: i64,
) -> Vec<usize> {
    let mut matching_rgs = Vec::new();
    for rg_idx in 0..num_rgs {
        if let Some(kr) = browse_ranges.iter().find(|r| r.row_group == rg_idx) {
            let (Some(min_s), Some(max_s)) =
                (kr.min_values.get(key_col), kr.max_values.get(key_col))
            else {
                continue;
            };
            let in_key_bounds = if let Some(target_i) = key_i64 {
                let min_i = min_s.parse::<i64>().unwrap_or(i64::MIN);
                let max_i = max_s.parse::<i64>().unwrap_or(i64::MAX);
                target_i >= min_i && target_i <= max_i
            } else {
                key_value >= min_s.as_str() && key_value <= max_s.as_str()
            };
            if !in_key_bounds {
                continue;
            }
            if min_s == key_value
                && max_s == key_value
                && let (Some(min_o_s), Some(max_o_s)) =
                    (kr.min_values.get(ord_col), kr.max_values.get(ord_col))
                && let (Ok(min_o), Ok(max_o)) = (min_o_s.parse::<i64>(), max_o_s.parse::<i64>())
                && (max_o < start_ordinal || min_o >= end_ordinal)
            {
                continue;
            }
        }
        matching_rgs.push(rg_idx);
    }
    matching_rgs
}

#[allow(clippy::too_many_arguments)]
fn filter_child_slice_batch(
    batch: &RecordBatch,
    key_col: &str,
    ord_col: &str,
    key_value: &str,
    key_i64: Option<i64>,
    start_ordinal: i64,
    end_ordinal: i64,
    limit: usize,
    collected: &mut usize,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    let key_arr = batch.column_by_name(key_col).ok_or_else(invalid_export)?;
    let ord_arr = batch
        .column_by_name(ord_col)
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
        if ord < start_ordinal || ord >= end_ordinal {
            continue;
        }
        let matches_key = if let Some(target_i) = key_i64 {
            if let Some(a) = key_arr.as_any().downcast_ref::<Int64Array>() {
                !a.is_null(row) && a.value(row) == target_i
            } else if let Some(a) = key_arr.as_any().downcast_ref::<Int32Array>() {
                !a.is_null(row) && i64::from(a.value(row)) == target_i
            } else {
                false
            }
        } else if let Some(a) = key_arr.as_any().downcast_ref::<StringArray>() {
            !a.is_null(row) && a.value(row) == key_value
        } else {
            scalar_value_at(key_arr, row).as_deref() == Some(key_value)
        };
        if matches_key {
            keep_indices.append_value(row as u32);
            keep_count += 1;
            if *collected + keep_count >= limit {
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

#[allow(clippy::too_many_arguments)]
fn slice_child_parquet(
    path: &std::path::Path,
    browse_ranges: &[ExportKeyRange],
    key_col: &str,
    ord_col: &str,
    key_value: &str,
    start_ordinal: i64,
    end_ordinal: i64,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let num_rgs = builder.metadata().num_row_groups();
    if num_rgs == 0 {
        return Ok(Vec::new());
    }

    let key_is_int64 = builder
        .schema()
        .field_with_name(key_col)
        .is_ok_and(|f| matches!(f.data_type(), DataType::Int64 | DataType::Int32));
    let key_i64 = if key_is_int64 {
        key_value.parse::<i64>().ok()
    } else {
        None
    };

    let matching_rgs = prune_child_row_groups(
        num_rgs,
        browse_ranges,
        key_col,
        ord_col,
        key_value,
        key_i64,
        start_ordinal,
        end_ordinal,
    );
    if matching_rgs.is_empty() {
        return Ok(Vec::new());
    }

    let selection = build_page_index_selection(
        builder.metadata(),
        builder.schema(),
        &matching_rgs,
        key_col,
        ord_col,
        key_value,
        key_i64,
        start_ordinal,
        end_ordinal,
    );

    let mut builder = builder
        .with_row_groups(matching_rgs)
        .with_batch_size(limit.clamp(64, 2048));
    if let Some(sel) = selection {
        builder = builder.with_row_selection(sel);
    }
    let reader = builder.build()?;

    let mut out = Vec::new();
    let mut collected = 0usize;
    for batch_res in reader {
        let batch = batch_res?;
        filter_child_slice_batch(
            &batch,
            key_col,
            ord_col,
            key_value,
            key_i64,
            start_ordinal,
            end_ordinal,
            limit,
            &mut collected,
            &mut out,
        )?;
        if collected >= limit {
            break;
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn slice_child_parquet_async(
    reader: Box<dyn AsyncFileReader>,
    browse_ranges: &[ExportKeyRange],
    key_col: &str,
    ord_col: &str,
    key_value: &str,
    start_ordinal: i64,
    end_ordinal: i64,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
    let num_rgs = builder.metadata().num_row_groups();
    if num_rgs == 0 {
        return Ok(Vec::new());
    }

    let key_is_int64 = builder
        .schema()
        .field_with_name(key_col)
        .is_ok_and(|f| matches!(f.data_type(), DataType::Int64 | DataType::Int32));
    let key_i64 = if key_is_int64 {
        key_value.parse::<i64>().ok()
    } else {
        None
    };

    let matching_rgs = prune_child_row_groups(
        num_rgs,
        browse_ranges,
        key_col,
        ord_col,
        key_value,
        key_i64,
        start_ordinal,
        end_ordinal,
    );
    if matching_rgs.is_empty() {
        return Ok(Vec::new());
    }

    let selection = build_page_index_selection(
        builder.metadata(),
        builder.schema(),
        &matching_rgs,
        key_col,
        ord_col,
        key_value,
        key_i64,
        start_ordinal,
        end_ordinal,
    );

    let mut builder = builder
        .with_row_groups(matching_rgs)
        .with_batch_size(limit.clamp(64, 2048));
    if let Some(sel) = selection {
        builder = builder.with_row_selection(sel);
    }
    let mut stream = builder.build()?;

    let mut out = Vec::new();
    let mut collected = 0usize;
    while let Some(batch) = stream.try_next().await? {
        filter_child_slice_batch(
            &batch,
            key_col,
            ord_col,
            key_value,
            key_i64,
            start_ordinal,
            end_ordinal,
            limit,
            &mut collected,
            &mut out,
        )?;
        if collected >= limit {
            break;
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn build_page_index_selection(
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
    let col_idx = schema.index_of(key_col).ok()?;
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
                        .is_none_or(|min_s| key_value >= min_s);
                    let max_ok = native_idx
                        .max_value(page_i)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .is_none_or(|max_s| key_value <= max_s);
                    min_ok && max_ok
                }
                ColumnIndexMetaData::INT64(native_idx) => {
                    if let Some(target_i) = key_i64 {
                        let min_ok = native_idx
                            .min_value(page_i)
                            .is_none_or(|&m| target_i >= m);
                        let max_ok = native_idx
                            .max_value(page_i)
                            .is_none_or(|&m| target_i <= m);
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

    let Ok(ord_idx) = schema.index_of(ord_col) else {
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
                        .is_none_or(|&m| m < end_ordinal);
                    let max_ok = native_idx
                        .max_value(page_i)
                        .is_none_or(|&m| m >= start_ordinal);
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
