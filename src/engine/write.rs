//! Native candidate writes using maintained relational and columnar operators.

use super::*;
use arrow::{
    array::BooleanArray,
    compute::{filter_record_batch, not},
    record_batch::RecordBatch,
};
use iceberg::{
    spec::{DataFile, Schema},
    table::Table,
};

pub(super) mod build;
mod compaction;
pub(super) mod merge;
mod verification;
pub(super) use super::sql_validation::{
    check_build_arrow_schema, check_build_fields, is_identity_source_projection, plan_build_input,
};
pub(super) use verification::verify_update;

pub(super) struct Prepared {
    pub table: TablePin,
    pub changes: Option<ChangePin>,
    pub metrics: WriteMetrics,
    pub schema_input: Option<ObjectRef>,
}

/// An explicit replacement scope, separate from potentially expensive new rows.
pub(super) struct Replacement<'a> {
    pub dataset: &'a Id,
    pub directory: locations::Location,
    pub before: &'a TablePin,
    pub keys: &'a [String],
    pub scope: DataFrame,
    pub replacement: DataFrame,
    pub replacement_bytes: Option<u64>,
}

pub(super) struct Applied {
    pub table: TablePin,
    pub removed: TablePin,
    pub added: TablePin,
    pub metrics: WriteMetrics,
}

/// Opens an admitted physical source without changing its canonical logical meaning.
///
/// # Errors
/// Refuses changed logical identity, row count, or public schema, and unreadable metadata.
pub(super) async fn open_captured_source(
    canonical: &TablePin,
    source: &TablePin,
    budget: &Budget,
) -> Result<Table> {
    let objects = storage::Objects::for_path(&source.metadata.path);
    open_captured_source_with_objects(&objects, canonical, source, budget).await
}

async fn open_captured_source_with_objects(
    objects: &storage::Objects,
    canonical: &TablePin,
    source: &TablePin,
    budget: &Budget,
) -> Result<Table> {
    if source.logical_metadata() != canonical.logical_metadata() || source.rows != canonical.rows {
        return Err(Error::refusal(
            "invalid_admission_dependencies",
            "captured source differs from its logical base",
        ));
    }
    let native = objects.open_pin(source, budget).await?;
    if source.metadata != canonical.metadata {
        let canonical = objects.open_pin(canonical, budget).await?;
        if !storage::identity::preserves_schema(
            canonical.metadata().current_schema(),
            native.metadata().current_schema(),
        )? {
            return Err(Error::refusal(
                "invalid_admission_dependencies",
                "captured source changes the logical schema",
            ));
        }
    }
    Ok(native)
}

pub(super) async fn normalize_replacement_input(
    context: &SessionContext,
    native: &Table,
    keys: &[String],
    budget: &Budget,
) -> Result<DataFrame> {
    let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
    let mut replacement =
        coerce_dataframe_for_schema(context.table("replacements").await?, &schema)?;
    if storage::defaults::has_supplied(native.metadata().current_schema())
        || storage::identity::has_origins(native.metadata().current_schema())?
    {
        replacement = schema::normalize(
            replacement,
            native.metadata().current_schema(),
            keys,
            budget,
        )
        .await?;
    }
    if !same_fields(&schema, replacement.schema().as_arrow())? {
        return Err(Error::refusal(
            "schema_mismatch",
            "replacement rows must match the base field names, types and nullability",
        ));
    }
    Ok(replacement)
}

pub(super) async fn build_update_scope(
    context: &SessionContext,
    replacement: &DataFrame,
    spec: &UpdateSpec,
    schema: &arrow::datatypes::Schema,
) -> Result<DataFrame> {
    let key_columns = || {
        spec.keys
            .iter()
            .map(|key| col(datafusion::common::Column::from_name(key)))
            .collect::<Vec<_>>()
    };
    let mut scope = replacement.clone().select(key_columns())?.distinct()?;
    if spec.deletions.is_some() {
        let deletions = coerce_dataframe_for_schema(context.table("deletions").await?, schema)?;
        context.deregister_table("deletions")?;
        context.register_table("deletions", deletions.clone().into_view())?;
        let expected_keys = spec
            .keys
            .iter()
            .map(|key| {
                schema
                    .field_with_name(key)
                    .map(|field| field.clone().with_nullable(true))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let supplied_keys = spec
            .keys
            .iter()
            .map(|key| {
                deletions
                    .schema()
                    .as_arrow()
                    .field_with_name(key)
                    .ok()
                    .map(|field| field.clone().with_nullable(true))
            })
            .collect::<Option<Vec<_>>>();
        if deletions.schema().fields().len() != spec.keys.len()
            || match supplied_keys {
                Some(fields) => !same_fields(
                    &arrow::datatypes::Schema::new(expected_keys),
                    &arrow::datatypes::Schema::new(fields),
                )?,
                None => true,
            }
        {
            return Err(Error::refusal(
                "schema_mismatch",
                "deletion rows must contain exactly the typed key fields",
            ));
        }
        if any_rows(key_join(
            replacement.clone(),
            deletions.clone(),
            &spec.keys,
            datafusion::logical_expr::JoinType::Inner,
        )?)
        .await?
        {
            return Err(Error::refusal(
                "conflicting_update",
                "replacement and deletion keys overlap",
            ));
        }
        scope = scope.union(deletions.select(key_columns())?)?.distinct()?;
    }
    Ok(scope)
}

impl Engine {
    pub(super) async fn pin_output(
        &self,
        dataset: &Id,
        table: &Table,
        rows: u64,
        shared: &[ObjectRef],
    ) -> Result<TablePin> {
        let table = self.inner.objects.bind_table(table)?;
        storage::pin_bound(dataset, &table, rows, &self.inner.lifecycle.budget, shared).await
    }

    pub(super) async fn pin_output_with_files(
        &self,
        dataset: &Id,
        table: &Table,
        rows: u64,
        shared: &[ObjectRef],
        known_files: Option<&[DataFile]>,
    ) -> Result<TablePin> {
        let table = self.inner.objects.bind_table(table)?;
        storage::pin_bound_with_files(
            dataset,
            &table,
            rows,
            &self.inner.lifecycle.budget,
            shared,
            known_files,
        )
        .await
    }

    pub(super) async fn create_output_at(
        &self,
        dataset: &Id,
        directory: &locations::Location,
        schema: Schema,
    ) -> Result<Table> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(dataset).await?;
        let directory = directory.clone();
        self.register_output(view.identifier().namespace(), move || match directory {
            locations::Location::Local(path) => storage::fresh_metadata(schema, &path),
            #[cfg(feature = "remote")]
            locations::Location::Remote(uri) => storage::fresh_metadata_at(schema, &uri),
        })
        .await
    }

    pub(super) async fn create_partitioned_output(
        &self,
        dataset: &Id,
        directory: &Path,
        schema: Schema,
        partition_by: &[PartitionFieldSpec],
    ) -> Result<Table> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(dataset).await?;
        let directory = directory.to_owned();
        let partition_by = partition_by.to_vec();
        self.register_output(view.identifier().namespace(), move || {
            let unbound = storage::build_unbound_partition_spec(&schema, &partition_by)?;
            storage::fresh_partitioned_metadata(&schema, &directory, unbound)
        })
        .await
    }

    pub(super) async fn replacement_output_at(
        &self,
        dataset: &Id,
        directory: &locations::Location,
        source: &Table,
    ) -> Result<Table> {
        let view = self.inner.lifecycle.open(dataset).await?;
        let source = source.clone();
        let location = directory.as_string()?;
        self.register_output(view.identifier().namespace(), move || {
            storage::replacement_metadata_at(source.metadata(), &location)
        })
        .await
    }

    pub(super) async fn register_output(
        &self,
        namespace: &iceberg::NamespaceIdent,
        prepare: impl FnOnce() -> Result<iceberg::spec::TableMetadata> + Send + 'static,
    ) -> Result<Table> {
        let lifecycle = &self.inner.lifecycle;
        let identifier = iceberg::TableIdent::new(
            namespace.clone(),
            format!("data_{}", uuid::Uuid::new_v4().simple()),
        );
        storage::register_candidate(
            lifecycle.catalog.as_ref(),
            &identifier,
            storage::CandidateWriter::configured(&self.inner.objects),
            prepare,
            &lifecycle.budget,
        )
        .await
    }

    pub(super) fn process_build<'a>(
        &'a self,
        spec: &'a RunSpec,
        directory: &'a Path,
        admission: &'a PhaseResult,
    ) -> futures::future::BoxFuture<'a, Result<Prepared>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            let input = admission.outputs.first().ok_or_else(|| {
                Error::refusal("corrupt_phase", "admission has no retained input")
            })?;
            let context = self.context();
            let dataframe = plan_build_input(&context, &input.path, &spec.sql).await?;
            let local_location = locations::Location::Local(directory.to_owned());
            if let Some(base) = &spec.base {
                let release = self.release_admitted(&spec.dataset, base).await?;
                let native = storage::open_pin(
                    &release.dataset.groups["source"],
                    &self.inner.lifecycle.budget,
                )
                .await?;
                if !spec.partition_by.is_empty() {
                    storage::check_partition_by_matches(
                        native.metadata().current_schema(),
                        native.metadata().default_partition_spec(),
                        &spec.partition_by,
                    )?;
                }
                // An explicit base fixes field meaning, order and allocation history.
                // SchemaSpec changes that interpretation; a rebuild supplies fresh values.
                let expected =
                    iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
                let dataframe = coerce_dataframe_for_schema(dataframe, &expected)?;
                let dataframe = schema::normalize(
                    dataframe,
                    native.metadata().current_schema(),
                    &[],
                    &self.inner.lifecycle.budget,
                )
                .await?;
                let table = self
                    .replacement_output_at(&spec.dataset, &local_location, &native)
                    .await?;
                let mut prepared = self.write_output(&spec.dataset, table, dataframe).await?;
                prepared.schema_input = Some(release.dataset.groups["source"].metadata.clone());
                return Ok(prepared);
            }
            check_build_fields(&dataframe)?;
            if !spec.partition_by.is_empty() {
                let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
                    dataframe.schema().as_arrow(),
                )?;
                let table = self
                    .create_partitioned_output(&spec.dataset, directory, schema, &spec.partition_by)
                    .await?;
                return self.write_output(&spec.dataset, table, dataframe).await;
            }
            self.write_frame_at(&spec.dataset, &local_location, dataframe)
                .await
        })
    }

    pub(super) async fn write_frame_at(
        &self,
        dataset: &Id,
        directory: &locations::Location,
        dataframe: DataFrame,
    ) -> Result<Prepared> {
        let schema =
            storage::arrow_schema_to_schema_auto_assign_ids(dataframe.schema().as_arrow())?;
        let table = self.create_output_at(dataset, directory, schema).await?;
        self.write_output(dataset, table, dataframe).await
    }

    async fn write_output(
        &self,
        dataset: &Id,
        table: Table,
        dataframe: DataFrame,
    ) -> Result<Prepared> {
        self.write_batch_stream(
            dataset,
            table,
            dataframe
                .execute_stream()
                .await?
                .map(|batch| batch.map_err(Error::from)),
        )
        .await
    }

    pub(super) async fn write_batch_stream<S>(
        &self,
        dataset: &Id,
        table: Table,
        batches: S,
    ) -> Result<Prepared>
    where
        S: futures::Stream<Item = Result<RecordBatch>> + Send,
    {
        let budget = &self.inner.lifecycle.budget;
        let files = storage::write_batches(&table, batches, budget).await?;
        let rows = files.iter().map(|file| file.record_count()).sum();
        let metrics = WriteMetrics {
            data_files_written: files.len() as u64,
            bytes_written: files.iter().map(|file| file.file_size_in_bytes()).sum(),
            rows_added: rows,
            ..WriteMetrics::default()
        };
        let table =
            storage::append(&table, self.inner.lifecycle.catalog.as_ref(), files.clone()).await?;
        let table = self
            .pin_output_with_files(dataset, &table, rows, &[], Some(&files))
            .await?;
        Ok(Prepared {
            table,
            changes: None,
            metrics,
            schema_input: None,
        })
    }

    pub(super) fn process_update<'a>(
        &'a self,
        spec: &'a UpdateSpec,
        directory: &'a locations::Location,
        admission: &'a PhaseResult,
        captured_source: Option<&'a TablePin>,
    ) -> futures::future::BoxFuture<'a, Result<Prepared>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            let base = self.release_admitted(&spec.dataset, &spec.base).await?;
            let canonical_before = base
                .dataset
                .groups
                .get("source")
                .ok_or_else(|| Error::refusal("invalid_base", "base has no source table"))?;
            // Older admissions consume their canonical source; retries never resolve
            // a newer physical representation after the request has been retained.
            let captured_before = captured_source.unwrap_or(canonical_before);
            let captured_native = open_captured_source_with_objects(
                &self.inner.objects,
                canonical_before,
                captured_before,
                &self.inner.lifecycle.budget,
            )
            .await?;
            let before_with_origins;
            let (before, native) =
                if !storage::identity::has_origins(captured_native.metadata().current_schema())? {
                    // Source occurrences survive the first value rewrite even without
                    // receipts. Add optional origin fields through native metadata;
                    // the existing reader fills original file/row before filtering.
                    before_with_origins = self
                        .source_with_origins_at(
                            &spec.dataset,
                            captured_before,
                            &directory.child("source-origins")?,
                        )
                        .await?;
                    let native = self
                        .inner
                        .objects
                        .open_pin(&before_with_origins, &self.inner.lifecycle.budget)
                        .await?;
                    (&before_with_origins, native)
                } else {
                    (captured_before, captured_native)
                };
            let context = self.context();
            let request = OperationRequest::Update(spec.clone());
            for ((name, source_spec), object) in request.inputs().iter().zip(&admission.outputs) {
                #[cfg(all(not(test), not(feature = "test-util")))]
                let local_path = if self.inner.objects.is_remote() {
                    let candidate = ObjectRef {
                        owner: object.owner.clone(),
                        path: source_spec.path.clone(),
                        digest: object.digest.clone(),
                        bytes: object.bytes,
                    };
                    storage::verify_object(&candidate)
                        .ok()
                        .map(|()| candidate.path)
                } else {
                    None
                };
                #[cfg(any(test, feature = "test-util"))]
                let local_path: Option<String> = {
                    let _ = source_spec;
                    None
                };
                let parquet_path = local_path.as_deref().unwrap_or(&object.path);
                context
                    .register_parquet(name.as_str(), parquet_path, ParquetReadOptions::default())
                    .await?;
            }
            let schema =
                iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
            let replacement = normalize_replacement_input(
                &context,
                &native,
                &spec.keys,
                &self.inner.lifecycle.budget,
            )
            .await?;
            context.deregister_table("replacements")?;
            context.register_table("replacements", replacement.clone().into_view())?;
            let mut field_ids = Vec::new();
            for key in &spec.keys {
                let field = native
                    .metadata()
                    .current_schema()
                    .field_by_name(key)
                    .ok_or_else(|| {
                        Error::refusal("unknown_key", "update key is absent from the base schema")
                    })?;
                field_ids.push(field.id);
            }
            if let Some(merge_spec) = &spec.merge {
                merge::validate(merge_spec, native.metadata().current_schema(), &spec.keys)?;
            }
            let scope = build_update_scope(&context, &replacement, spec, &schema).await?;
            let applied = self
                .replace_keys_with_merge(
                    Replacement {
                        dataset: &spec.dataset,
                        directory: directory.clone(),
                        before,
                        keys: &spec.keys,
                        scope,
                        replacement,
                        replacement_bytes: Some(
                            admission
                                .outputs
                                .iter()
                                .fold(0_u64, |sum, object| sum.saturating_add(object.bytes)),
                        ),
                    },
                    spec.merge.as_ref(),
                )
                .await?;
            let Applied {
                table,
                removed,
                added,
                metrics,
            } = self
                .finish_source_replacement_at(&spec.dataset, directory, captured_before, applied)
                .await?;
            let changes = ChangePin {
                rules: super::changes::WRITE_RULES.into(),
                base_version: spec.base.clone(),
                base_manifest: base.manifest,
                before_metadata: canonical_before.metadata.clone(),
                after_metadata: table.metadata.clone(),
                key_field_ids: field_ids,
                removed,
                added,
            };
            Ok(Prepared {
                table,
                changes: Some(changes),
                metrics,
                schema_input: None,
            })
        })
    }

    /// Retains the captured source and its change schema when no rows were applied.
    pub(super) async fn finish_source_replacement_at(
        &self,
        dataset: &Id,
        directory: &locations::Location,
        captured: &TablePin,
        mut applied: Applied,
    ) -> Result<Applied> {
        if applied.metrics.rows_removed == 0
            && applied.metrics.rows_added == 0
            && applied.table.metadata != captured.metadata
        {
            // Optional origins must not fork an unchanged captured source. The
            // empty change relations use its schema too, as endpoint verification
            // requires the added and removed schemas to match the result.
            applied.table = captured.clone();
            let native = self
                .inner
                .objects
                .open_pin(captured, &self.inner.lifecycle.budget)
                .await?;
            let empty = self
                .replacement_output_at(dataset, &directory.child("unchanged-changes")?, &native)
                .await?;
            applied.removed = self.pin_output(dataset, &empty, 0, &[]).await?;
            applied.added = applied.removed.clone();
        }
        Ok(applied)
    }

    /// Replaces complete key groups and retains actual applied rows with shared files.
    // Keep shared replacement state out of nested application poll frames.
    pub(super) fn replace_keys<'a>(
        &'a self,
        request: Replacement<'a>,
    ) -> futures::future::BoxFuture<'a, Result<Applied>> {
        self.replace_keys_with_merge(request, None)
    }

    pub(super) fn replace_keys_with_merge<'a>(
        &'a self,
        request: Replacement<'a>,
        merge: Option<&'a UpdateMergeSpec>,
    ) -> futures::future::BoxFuture<'a, Result<Applied>> {
        // Heap-box state machine to bound poll-frame stack size and worker admission
        Box::pin(async move {
            let Replacement {
                dataset,
                directory,
                before,
                keys,
                scope,
                replacement,
                replacement_bytes,
            } = request;
            let budget = &self.inner.lifecycle.budget;
            let (native, inventory) = self.inner.objects.open_snapshot(before, budget).await?;
            let schema =
                iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
            if !same_fields(&schema, replacement.schema().as_arrow())? {
                return Err(Error::refusal(
                    "schema_mismatch",
                    "replacement rows must preserve the complete native schema",
                ));
            }
            let context = self.context();
            let marker = format!("__docspec_match_{}", uuid::Uuid::new_v4().simple());
            let scope = scope.with_column(&marker, datafusion::prelude::lit(true))?;
            // Prepare distinct keys once. A logical view would rescan all input rows
            // and repeat DISTINCT for every source-file probe and rewrite.
            // Only the prepared Parquet files are consumed below; no key snapshot
            // is selected or pinned. Keep the configured writer and protected
            // attempt path without persisting/registering unused table metadata.
            let scope_table = {
                let _metadata =
                    budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
                let metadata = storage::fresh_metadata_at(
                    storage::arrow_schema_to_schema_auto_assign_ids(scope.schema().as_arrow())?,
                    &directory.child("keys")?.as_string()?,
                )?;
                drop(storage::bounded_json(
                    &metadata,
                    budget.limits.max_metadata_bytes,
                )?);
                Table::builder()
                    .metadata(metadata)
                    .identifier(native.identifier().clone())
                    .file_io(native.file_io().clone())
                    .runtime(iceberg::Runtime::try_current()?)
                    .readonly(true)
                    .build()?
            };
            let key_files =
                storage::write_batches(&scope_table, execute(scope).await?, budget).await?;
            let key_files_written = key_files.len() as u64;
            let key_bytes_written = key_files
                .iter()
                .map(|file| file.file_size_in_bytes())
                .sum::<u64>();
            let scope_schema =
                iceberg::arrow::schema_to_arrow_schema(scope_table.metadata().current_schema())?;
            if key_files.is_empty() {
                context.register_table(
                    "affected_keys",
                    Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
                        scope_schema,
                    ))),
                )?;
            } else {
                let paths = key_files
                    .iter()
                    .map(|file| file.file_path().to_owned())
                    .collect::<Vec<_>>();
                let prepared_keys = context
                    .read_parquet(paths, ParquetReadOptions::default().schema(&scope_schema))
                    .await?;
                context.register_table("affected_keys", prepared_keys.into_view())?;
            }
            let scan = Arc::new(storage::OccurrenceScan::values(&native, budget, inventory).await?);
            let compaction::ProbedFiles {
                mut shared,
                changed,
                rewrite_bytes,
                files_probed,
                files_skipped_by_pruning,
            } = compaction::probe_affected_files(
                &context,
                &native,
                &scope_table,
                keys,
                &key_files,
                &scan,
                &schema,
                &marker,
                budget,
            )
            .await?;
            // Remote output consumes retained object storage; host preflight
            // covers scratch and bounded writer buffers, not those remote bytes.
            let required = if directory.is_local() {
                rewrite_bytes
                    .saturating_mul(2)
                    .saturating_add(replacement_bytes.unwrap_or(0).saturating_mul(2))
            } else {
                (budget.limits.max_batch_bytes as u64).saturating_mul(4)
            }
            .saturating_add(budget.limits.scratch_bytes);
            if fs2::available_space(&self.inner.root)? < required {
                return Err(Error::refusal(
                    "disk_capacity",
                    "affected-file rewrites, change rows and declared scratch do not fit available disk",
                ));
            }
            let candidate = self
                .replacement_output_at(dataset, &directory.child("data")?, &native)
                .await?;
            let removed = self
                .replacement_output_at(dataset, &directory.child("removed")?, &native)
                .await?;
            let added = self
                .replacement_output_at(dataset, &directory.child("added")?, &native)
                .await?;
            let mut metrics = WriteMetrics {
                // Counts total native file decode passes: one key-only probe pass per
                // unpruned candidate file plus one full-row rewrite pass per affected file.
                files_scanned: files_probed.saturating_add(changed.len() as u64),
                files_reused: shared.len() as u64,
                bytes_reused: shared.iter().map(|file| file.file_size_in_bytes()).sum(),
                candidate_files_probed: files_probed,
                files_skipped_by_pruning,
                ..WriteMetrics::default()
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            let max_batch_bytes = budget.limits.max_batch_bytes;
            let source = futures::stream::iter(changed)
                .then(execute)
                .try_flatten()
                .boxed();
            let kept = source.and_then(move |batch| {
                let sender = sender.clone();
                async move {
                    let bytes = storage::effective_batch_bytes(&batch);
                    if !storage::has_list_of_struct(batch.schema().as_ref())
                        && bytes > max_batch_bytes
                    {
                        return Err(Error::refusal(
                            "oversized_batch",
                            format!("joined update batch uses {bytes} bytes, exceeding the {max_batch_bytes} byte allowance"),
                        ));
                    }
                    let (mut keep, mut remove) = split_rows(batch)?;
                    if storage::has_list_of_struct(keep.schema().as_ref()) {
                        if keep.num_rows() != 0 {
                            keep = storage::compact_batch_slice(&keep, 0, keep.num_rows())?;
                        }
                        if remove.num_rows() != 0 {
                            remove = storage::compact_batch_slice(&remove, 0, remove.num_rows())?;
                        }
                    }
                    if remove.num_rows() != 0 {
                        sender.send(Ok(remove)).await.map_err(|_| {
                            Error::refusal(
                                "writer_stopped",
                                "removed-row writer stopped before completion",
                            )
                        })?;
                    }
                    Ok(keep)
                }
            });
            let removed_rows = futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|batch| (batch, receiver))
            });
            let (kept_files, removed_files) = tokio::try_join!(
                storage::write_batches(&candidate, kept, budget),
                storage::write_batches(&removed, removed_rows, budget)
            )?;
            let replacement = if let Some(merge_spec) = merge {
                let prior_df = if removed_files.is_empty() {
                    let empty =
                        datafusion::datasource::empty::EmptyTable::new(Arc::new(schema.clone()));
                    context.read_table(Arc::new(empty))?
                } else {
                    let paths = removed_files
                        .iter()
                        .map(|file| file.file_path().to_owned())
                        .collect::<Vec<_>>();
                    context
                        .read_parquet(paths, ParquetReadOptions::default().schema(&schema))
                        .await?
                };
                merge::apply_merge(
                    &context,
                    prior_df,
                    replacement,
                    native.metadata().current_schema(),
                    keys,
                    merge_spec,
                    budget,
                )
                .await?
            } else {
                replacement
            };
            let added_files = storage::write_batches(
                &candidate,
                replacement.execute_stream().await?.map(|batch| {
                    let batch = batch.map_err(Error::from)?;
                    // Join fanout has no honest input-byte upper bound. Keep
                    // scratch and bounded writer buffers available as rows arrive.
                    let reserve = budget
                        .limits
                        .scratch_bytes
                        .saturating_add((budget.limits.max_batch_bytes as u64).saturating_mul(4));
                    if fs2::available_space(&self.inner.root)? < reserve {
                        return Err(Error::refusal(
                            "disk_capacity",
                            "replacement writer and declared scratch no longer fit available disk",
                        ));
                    }
                    Ok(batch)
                }),
                budget,
            )
            .await?;
            let _ = context.deregister_table("affected_keys");
            if directory.is_local()
                && let Ok(path) = directory.local_path()
            {
                let _ = std::fs::remove_dir_all(path.join("keys"));
            }
            metrics.rows_removed = removed_files.iter().map(|file| file.record_count()).sum();
            metrics.rows_added = added_files.iter().map(|file| file.record_count()).sum();
            let coalesced_files = compaction::coalesce_small_partition_files(
                &self.inner.root,
                &directory,
                &native,
                &candidate,
                &mut shared,
                &scan,
                metrics.rows_removed != 0 || metrics.rows_added != 0,
                budget,
            )
            .await?;
            metrics.data_files_written = key_files_written
                + (kept_files.len()
                    + removed_files.len()
                    + added_files.len()
                    + coalesced_files.len()) as u64;
            metrics.bytes_written = kept_files
                .iter()
                .chain(&removed_files)
                .chain(&added_files)
                .chain(&coalesced_files)
                .map(|file| file.file_size_in_bytes())
                .sum::<u64>()
                + key_bytes_written;
            let rows = before
                .rows
                .checked_sub(metrics.rows_removed)
                .and_then(|rows| rows.checked_add(metrics.rows_added))
                .ok_or_else(|| {
                    Error::refusal(
                        "invalid_changes",
                        "applied change counts do not fit the captured base",
                    )
                })?;
            shared.extend(coalesced_files);
            shared.extend(kept_files);
            shared.extend(added_files.clone());
            let catalog = self.inner.lifecycle.catalog.as_ref();
            // No applied rows means membership, values and schema are unchanged. Keep
            // the exact source pin so completed groups also reuse their prepared tables.
            let table = if metrics.rows_removed == 0 && metrics.rows_added == 0 {
                before.clone()
            } else {
                let candidate = storage::append(&candidate, catalog, shared).await?;
                self.pin_output(dataset, &candidate, rows, &before.objects)
                    .await?
            };
            let removed = storage::append(&removed, catalog, removed_files).await?;
            let added = storage::append(&added, catalog, added_files).await?;
            let removed = self
                .pin_output(dataset, &removed, metrics.rows_removed, &[])
                .await?;
            let added = self
                .pin_output(dataset, &added, metrics.rows_added, &table.objects)
                .await?;
            Ok(Applied {
                table,
                removed,
                added,
                metrics,
            })
        })
    }
}

pub(super) fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
pub(super) fn qualified(relation: &str, field: &str) -> Expr {
    Expr::Column(datafusion::common::Column::new(Some(relation), field))
}
fn key_join(
    left: DataFrame,
    right: DataFrame,
    keys: &[String],
    kind: datafusion::logical_expr::JoinType,
) -> Result<DataFrame> {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    // Typed expressions avoid recursive SQL parsing on deeply nested refresh
    // calls and preserve literal field names, including dots and quotes.
    let conditions = keys.iter().map(|key| {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(qualified("l", key)),
            Operator::IsNotDistinctFrom,
            Box::new(qualified("r", key)),
        ))
    });
    Ok(left
        .alias("l")?
        .join_on(right.alias("r")?, kind, conditions)?)
}
pub(super) fn coerce_dataframe_for_schema(
    frame: DataFrame,
    expected: &arrow::datatypes::Schema,
) -> Result<DataFrame> {
    let actual = frame.schema().as_arrow().clone();
    let mut needs_projection = false;
    let mut exprs = Vec::with_capacity(actual.fields().len());
    for field in actual.fields() {
        let col_expr =
            datafusion::prelude::col(datafusion::common::Column::from_name(field.name()));
        let target_type = if let Ok(expected_field) = expected.field_with_name(field.name()) {
            let coerced_actual = storage::coerce_data_type(field.data_type());
            if &coerced_actual == expected_field.data_type()
                || matches!(
                    (&coerced_actual, expected_field.data_type()),
                    (
                        arrow::datatypes::DataType::Binary,
                        arrow::datatypes::DataType::LargeBinary,
                    ) | (
                        arrow::datatypes::DataType::LargeBinary,
                        arrow::datatypes::DataType::Binary,
                    ) | (
                        arrow::datatypes::DataType::Utf8,
                        arrow::datatypes::DataType::LargeUtf8,
                    ) | (
                        arrow::datatypes::DataType::LargeUtf8,
                        arrow::datatypes::DataType::Utf8,
                    )
                )
            {
                expected_field.data_type().clone()
            } else {
                coerced_actual
            }
        } else {
            storage::coerce_data_type(field.data_type())
        };
        if field.data_type() != &target_type {
            needs_projection = true;
            exprs.push(
                datafusion::logical_expr::Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(col_expr),
                    target_type,
                ))
                .alias(field.name()),
            );
        } else {
            exprs.push(col_expr);
        }
    }
    if needs_projection {
        Ok(frame.select(exprs)?)
    } else {
        Ok(frame)
    }
}

pub(super) fn same_fields(
    left: &arrow::datatypes::Schema,
    right: &arrow::datatypes::Schema,
) -> Result<bool> {
    // The maintained converter assigns comparison-only IDs consistently and
    // preserves nested names, types and nullability. Caller files need no IDs.
    let fields = right
        .fields()
        .iter()
        .zip(left.fields())
        .map(|(actual, expected)| {
            actual
                .as_ref()
                .clone()
                .with_nullable(actual.is_nullable() || expected.is_nullable())
        })
        .collect::<Vec<_>>();
    if fields.len() != right.fields().len() {
        return Ok(false);
    }
    let right = arrow::datatypes::Schema::new(fields);
    Ok(storage::arrow_schema_to_schema_auto_assign_ids(left)?
        == storage::arrow_schema_to_schema_auto_assign_ids(&right)?)
}
pub(super) async fn any_rows(frame: DataFrame) -> Result<bool> {
    let batches = frame
        .select(vec![lit(1)])?
        .limit(0, Some(1))?
        .collect()
        .await?;
    Ok(batches.iter().any(|batch| batch.num_rows() != 0))
}
fn split_rows(batch: RecordBatch) -> Result<(RecordBatch, RecordBatch)> {
    let index = batch.num_columns() - 1;
    let selected = batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::refusal("invalid_changes", "native match marker is not boolean"))?;
    let inverse = not(selected)?;
    let rows = batch.project(&(0..index).collect::<Vec<_>>())?;
    Ok((
        filter_record_batch(&rows, &inverse)?,
        filter_record_batch(&rows, selected)?,
    ))
}

async fn execute(
    dataframe: DataFrame,
) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
    Ok(dataframe
        .execute_stream()
        .await?
        .map(|batch| batch.map_err(Error::from))
        .boxed())
}

#[cfg(test)]
mod tests;
