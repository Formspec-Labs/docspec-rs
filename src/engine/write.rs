//! Native candidate writes using maintained relational and columnar operators.

use super::*;
use arrow::{
    array::BooleanArray,
    compute::{filter_record_batch, not},
    record_batch::RecordBatch,
};
use iceberg::{spec::Schema, table::Table};

pub(super) mod build;
mod verification;
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
    open_captured_source_with_objects(&storage::Objects::Local, canonical, source, budget).await
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

fn check_build_fields(frame: &DataFrame) -> Result<()> {
    if frame.schema().fields().iter().any(|f| {
        f.name()
            .to_ascii_lowercase()
            .starts_with(storage::defaults::SUPPLIED_PREFIX)
            || storage::identity::hidden(&f.name().to_ascii_lowercase())
    }) {
        return Err(Error::refusal(
            "reserved_field",
            "supplied-state columns require an evolved source schema",
        ));
    }
    Ok(())
}

pub(super) async fn build_query(context: &SessionContext, sql: &str) -> Result<DataFrame> {
    let options = datafusion::execution::context::SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let dataframe = context.sql_with_options(sql, options).await?;
    validate_plan(dataframe.logical_plan())?;
    Ok(dataframe)
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

    pub(super) async fn create_output(
        &self,
        dataset: &Id,
        directory: &Path,
        schema: Schema,
    ) -> Result<Table> {
        self.create_output_at(
            dataset,
            &locations::Location::Local(directory.to_owned()),
            schema,
        )
        .await
    }

    async fn create_output_at(
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

    pub(super) async fn replacement_output(
        &self,
        dataset: &Id,
        directory: &Path,
        source: &Table,
    ) -> Result<Table> {
        self.replacement_output_at(
            dataset,
            &locations::Location::Local(directory.to_owned()),
            source,
        )
        .await
    }

    async fn replacement_output_at(
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

    pub(super) async fn process_build(
        &self,
        spec: &RunSpec,
        directory: &Path,
        admission: &PhaseResult,
    ) -> Result<Prepared> {
        let context = self.context();
        let input = admission
            .outputs
            .first()
            .ok_or_else(|| Error::refusal("corrupt_phase", "admission has no retained input"))?;
        context
            .register_parquet("input", &input.path, ParquetReadOptions::default())
            .await?;
        let dataframe = build_query(&context, &spec.sql).await?;
        if let Some(base) = &spec.base {
            let release = self.release_admitted(&spec.dataset, base).await?;
            let native = storage::open_pin(
                &release.dataset.groups["source"],
                &self.inner.lifecycle.budget,
            )
            .await?;
            // An explicit base fixes field meaning, order and allocation history.
            // SchemaSpec changes that interpretation; a rebuild supplies fresh values.
            let dataframe = schema::normalize(
                dataframe,
                native.metadata().current_schema(),
                &[],
                &self.inner.lifecycle.budget,
            )
            .await?;
            let table = self
                .replacement_output(&spec.dataset, directory, &native)
                .await?;
            let mut prepared = self.write_output(&spec.dataset, table, dataframe).await?;
            prepared.schema_input = Some(release.dataset.groups["source"].metadata.clone());
            return Ok(prepared);
        }
        check_build_fields(&dataframe)?;
        self.write_frame(&spec.dataset, directory, dataframe).await
    }

    pub(super) async fn write_frame(
        &self,
        dataset: &Id,
        directory: &Path,
        dataframe: DataFrame,
    ) -> Result<Prepared> {
        let schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(dataframe.schema().as_arrow())?;
        let table = self.create_output(dataset, directory, schema).await?;
        self.write_output(dataset, table, dataframe).await
    }

    async fn write_output(
        &self,
        dataset: &Id,
        table: Table,
        dataframe: DataFrame,
    ) -> Result<Prepared> {
        let budget = &self.inner.lifecycle.budget;
        let files = storage::write_batches(
            &table,
            dataframe
                .execute_stream()
                .await?
                .map(|batch| batch.map_err(Error::from)),
            budget,
        )
        .await?;
        let rows = files.iter().map(|file| file.record_count()).sum();
        let metrics = WriteMetrics {
            data_files_written: files.len() as u64,
            bytes_written: files.iter().map(|file| file.file_size_in_bytes()).sum(),
            rows_added: rows,
            ..WriteMetrics::default()
        };
        let table = storage::append(&table, self.inner.lifecycle.catalog.as_ref(), files).await?;
        let table = self.pin_output(dataset, &table, rows, &[]).await?;
        Ok(Prepared {
            table,
            changes: None,
            metrics,
            schema_input: None,
        })
    }

    pub(super) async fn process_update(
        &self,
        spec: &UpdateSpec,
        directory: &locations::Location,
        admission: &PhaseResult,
        captured_source: Option<&TablePin>,
    ) -> Result<Prepared> {
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
        for ((name, _), object) in request.inputs().iter().zip(&admission.outputs) {
            context
                .register_parquet(name.as_str(), &object.path, ParquetReadOptions::default())
                .await?;
        }
        let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
        let mut replacement = context.table("replacements").await?;
        if storage::defaults::has_supplied(native.metadata().current_schema())
            || storage::identity::has_origins(native.metadata().current_schema())?
        {
            replacement = schema::normalize(
                replacement,
                native.metadata().current_schema(),
                &spec.keys,
                &self.inner.lifecycle.budget,
            )
            .await?;
            context.deregister_table("replacements")?;
            context.register_table("replacements", replacement.clone().into_view())?;
        }
        if !same_fields(&schema, replacement.schema().as_arrow())? {
            return Err(Error::refusal(
                "schema_mismatch",
                "replacement rows must match the base field names, types and nullability",
            ));
        }
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
        let keys = spec
            .keys
            .iter()
            .map(|key| identifier(key))
            .collect::<Vec<_>>()
            .join(", ");
        if spec.deletions.is_some() {
            let deletions = context.table("deletions").await?;
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
                context.table("replacements").await?,
                context.table("deletions").await?,
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
        }
        let scope_sql = if spec.deletions.is_some() {
            format!(
                "SELECT DISTINCT {keys} FROM replacements UNION SELECT DISTINCT {keys} FROM deletions"
            )
        } else {
            format!("SELECT DISTINCT {keys} FROM replacements")
        };
        let scope = context.sql(&scope_sql).await?;
        let applied = self
            .replace_keys(Replacement {
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
            })
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
    }

    /// Retains the captured source and its change schema when no rows were applied.
    pub(super) async fn finish_source_replacement(
        &self,
        dataset: &Id,
        directory: &Path,
        captured: &TablePin,
        applied: Applied,
    ) -> Result<Applied> {
        self.finish_source_replacement_at(
            dataset,
            &locations::Location::Local(directory.to_owned()),
            captured,
            applied,
        )
        .await
    }

    async fn finish_source_replacement_at(
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
                    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
                        scope.schema().as_arrow(),
                    )?,
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
            let mut shared = Vec::new();
            let mut changed = Vec::new();
            let mut rewrite_bytes = 0_u64;
            let mut files_probed = 0_u64;
            if key_files.is_empty() {
                shared.extend(scan.files().cloned());
            } else {
                let native_schema = native.metadata().current_schema();
                let key_table_schema = scope_table.metadata().current_schema();
                // Exact affected points when they fit one batch allowance; otherwise
                // only the coarser affected-interval test below applies.
                let points = key_points::KeyPoints::collect(&context, keys, budget).await?;
                for (index, file) in scan.files().enumerate() {
                    if !file_may_overlap_key_files(
                        native_schema,
                        key_table_schema,
                        keys,
                        &key_files,
                        file,
                    ) || points
                        .as_ref()
                        .is_some_and(|points| !points.may_contain(native_schema, keys, file))
                    {
                        shared.push(file.clone());
                        continue;
                    }
                    files_probed = files_probed.saturating_add(1);
                    let name = format!("chunk_{index}");
                    context.register_table(
                        name.as_str(),
                        crate::query::file(
                            scan.clone(),
                            file.file_path().to_owned(),
                            Some(keys),
                            budget.clone(),
                        )?,
                    )?;
                    if any_rows(key_join(
                        context.table(&name).await?,
                        context.table("affected_keys").await?,
                        keys,
                        datafusion::logical_expr::JoinType::Inner,
                    )?)
                    .await?
                    {
                        rewrite_bytes = rewrite_bytes.saturating_add(file.file_size_in_bytes());
                        // Replace the key-only probe with the complete selected native schema.
                        context.deregister_table(&name)?;
                        context.register_table(
                            name.as_str(),
                            crate::query::file(
                                scan.clone(),
                                file.file_path().to_owned(),
                                None,
                                budget.clone(),
                            )?,
                        )?;
                        // One joined decode supplies both kept rows and actual removed rows.
                        let mut columns = schema
                            .fields()
                            .iter()
                            .map(|field| qualified("l", field.name()))
                            .collect::<Vec<_>>();
                        columns.push(qualified("r", &marker).is_not_null().alias(&marker));
                        changed.push(
                            key_join(
                                context.table(&name).await?,
                                context.table("affected_keys").await?,
                                keys,
                                datafusion::logical_expr::JoinType::Left,
                            )?
                            .select(columns)?,
                        );
                    } else {
                        shared.push(file.clone());
                    }
                    // Retained changed-file plans own their provider; idle catalog entries
                    // need not accumulate across the file population.
                    context.deregister_table(&name)?;
                }
            }
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
                files_scanned: files_probed.saturating_add(changed.len() as u64),
                files_reused: shared.len() as u64,
                bytes_reused: shared.iter().map(|file| file.file_size_in_bytes()).sum(),
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
                let bytes = batch.get_array_memory_size();
                if bytes > max_batch_bytes {
                    return Err(Error::refusal(
                        "oversized_batch",
                        format!("joined update batch uses {bytes} bytes, exceeding the {max_batch_bytes} byte allowance"),
                    ));
                }
                let (keep, remove) = split_rows(batch)?;
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
            metrics.rows_removed = removed_files.iter().map(|file| file.record_count()).sum();
            metrics.rows_added = added_files.iter().map(|file| file.record_count()).sum();
            metrics.data_files_written = key_files_written
                + (kept_files.len() + removed_files.len() + added_files.len()) as u64;
            metrics.bytes_written = kept_files
                .iter()
                .chain(&removed_files)
                .chain(&added_files)
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
    Ok(
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(left)?
            == iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&right)?,
    )
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

pub(super) fn untruncated_bound(
    datum: &iceberg::spec::Datum,
) -> Option<iceberg::spec::PrimitiveLiteral> {
    use iceberg::spec::PrimitiveLiteral;
    let lit = PrimitiveLiteral::from(datum.clone());
    match &lit {
        // Chunk statistics truncate variable-width bounds at `STATISTICS_TRUNCATE_BYTES`.
        // Only treat bounds strictly shorter than that limit as exact interval endpoints.
        PrimitiveLiteral::String(s) if s.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        PrimitiveLiteral::Binary(b) if b.len() >= crate::storage::STATISTICS_TRUNCATE_BYTES => None,
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => None,
        _ if lit.is_nan() => None,
        _ => Some(lit),
    }
}

fn file_may_overlap_key_files(
    native_schema: &iceberg::spec::Schema,
    scope_schema: &iceberg::spec::Schema,
    keys: &[String],
    key_files: &[iceberg::spec::DataFile],
    file: &iceberg::spec::DataFile,
) -> bool {
    if key_files.is_empty() {
        return false;
    }
    for key in keys {
        let (Some(native_field), Some(scope_field)) = (
            native_schema.field_by_name(key),
            scope_schema.field_by_name(key),
        ) else {
            continue;
        };
        let (native_id, scope_id) = (native_field.id, scope_field.id);
        let file_may_have_null = file
            .null_value_counts()
            .get(&native_id)
            .copied()
            .unwrap_or(1)
            > 0;
        let keys_may_have_null = key_files
            .iter()
            .any(|kf| kf.null_value_counts().get(&scope_id).copied().unwrap_or(1) > 0);
        // `key_join` uses `IsNotDistinctFrom`, so a possible NULL on both sides can match.
        if file_may_have_null && keys_may_have_null {
            continue;
        }
        let (Some(file_low), Some(file_high)) = (
            file.lower_bounds()
                .get(&native_id)
                .and_then(untruncated_bound),
            file.upper_bounds()
                .get(&native_id)
                .and_then(untruncated_bound),
        ) else {
            continue;
        };
        if file_low > file_high {
            continue;
        }
        let all_disjoint = key_files.iter().all(|kf| {
            let (Some(kf_low), Some(kf_high)) = (
                kf.lower_bounds().get(&scope_id).and_then(untruncated_bound),
                kf.upper_bounds().get(&scope_id).and_then(untruncated_bound),
            ) else {
                return false;
            };
            kf_low <= kf_high && (kf_high < file_low || kf_low > file_high)
        });
        if all_disjoint {
            return false;
        }
    }
    true
}

mod key_points;

#[cfg(test)]
mod tests;
