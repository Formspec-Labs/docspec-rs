//! Native candidate writes using maintained relational and columnar operators.

use super::*;
use arrow::{
    array::BooleanArray,
    compute::{filter_record_batch, not},
    record_batch::RecordBatch,
};
use iceberg::{spec::Schema, table::Table};

pub(super) struct Prepared {
    pub table: TablePin,
    pub changes: Option<ChangePin>,
    pub metrics: WriteMetrics,
}

/// An explicit replacement scope, separate from potentially expensive new rows.
pub(super) struct Replacement<'a> {
    pub dataset: &'a Id,
    pub directory: &'a Path,
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

impl Engine {
    pub(super) async fn create_output(
        &self,
        dataset: &Id,
        directory: &Path,
        schema: Schema,
    ) -> Result<Table> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(dataset).await?;
        let directory = directory.to_owned();
        self.register_output(view.table.identifier().namespace(), move || {
            storage::fresh_metadata(schema, &directory)
        })
        .await
    }

    async fn replacement_output(
        &self,
        dataset: &Id,
        directory: &Path,
        source: &Table,
    ) -> Result<Table> {
        let view = self.inner.lifecycle.open(dataset).await?;
        let source = source.clone();
        let directory = directory.to_owned();
        self.register_output(view.table.identifier().namespace(), move || {
            storage::replacement_metadata(source.metadata(), &directory)
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
        let dataframe = context.sql(&spec.sql).await?;
        validate_plan(dataframe.logical_plan())?;
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
        let table = storage::pin(dataset, &table, rows, budget, &[]).await?;
        Ok(Prepared {
            table,
            changes: None,
            metrics,
        })
    }

    pub(super) async fn process_update(
        &self,
        spec: &UpdateSpec,
        directory: &Path,
        admission: &PhaseResult,
    ) -> Result<Prepared> {
        let base = self.release(&spec.dataset, &spec.base).await?;
        let before = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| Error::refusal("invalid_base", "base has no source table"))?;
        let native = storage::open_pin(before).await?;
        let context = self.context();
        let request = OperationRequest::Update(spec.clone());
        for ((name, _), object) in request.inputs().iter().zip(&admission.outputs) {
            context
                .register_parquet(name.as_str(), &object.path, ParquetReadOptions::default())
                .await?;
        }
        let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
        let replacement = context.table("replacements").await?;
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
                directory,
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
        } = applied;
        let changes = ChangePin {
            base_version: spec.base.clone(),
            base_manifest: base.manifest,
            before_metadata: before.metadata.clone(),
            after_metadata: table.metadata.clone(),
            key_field_ids: field_ids,
            removed,
            added,
        };
        Ok(Prepared {
            table,
            changes: Some(changes),
            metrics,
        })
    }
    /// Replaces complete key groups and retains actual applied rows with shared files.
    pub(super) async fn replace_keys(&self, request: Replacement<'_>) -> Result<Applied> {
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
        let native = storage::open_pin(before).await?;
        let inventory = storage::snapshot_files(&native, budget).await?;
        storage::check_inventory_files(before, &inventory)?;
        let schema = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
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
        let scope_table = self
            .create_output(
                dataset,
                &directory.join("keys"),
                iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(scope.schema().as_arrow())?,
            )
            .await?;
        let key_files = storage::write_batches(&scope_table, execute(scope).await?, budget).await?;
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
        for (index, file) in scan.files().enumerate() {
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
        let required = rewrite_bytes
            .saturating_mul(2)
            .saturating_add(replacement_bytes.unwrap_or(0).saturating_mul(2))
            .saturating_add(budget.limits.scratch_bytes);
        if fs2::available_space(&self.inner.root)? < required {
            return Err(Error::refusal(
                "disk_capacity",
                "affected-file rewrites, change rows and declared scratch do not fit available disk",
            ));
        }
        let candidate = self
            .replacement_output(dataset, &directory.join("data"), &native)
            .await?;
        let removed = self
            .replacement_output(dataset, &directory.join("removed"), &native)
            .await?;
        let added = self
            .replacement_output(dataset, &directory.join("added"), &native)
            .await?;
        let mut metrics = WriteMetrics {
            files_scanned: (scan.files().count() + changed.len()) as u64,
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
        metrics.data_files_written =
            key_files_written + (kept_files.len() + removed_files.len() + added_files.len()) as u64;
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
            storage::pin(dataset, &candidate, rows, budget, &before.objects).await?
        };
        let removed = storage::append(&removed, catalog, removed_files).await?;
        let added = storage::append(&added, catalog, added_files).await?;
        let removed = storage::pin(dataset, &removed, metrics.rows_removed, budget, &[]).await?;
        let added =
            storage::pin(dataset, &added, metrics.rows_added, budget, &table.objects).await?;
        Ok(Applied {
            table,
            removed,
            added,
            metrics,
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
fn same_fields(left: &arrow::datatypes::Schema, right: &arrow::datatypes::Schema) -> Result<bool> {
    // The maintained converter assigns comparison-only IDs consistently and
    // preserves nested names, types and nullability. Caller files need no IDs.
    Ok(
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(left)?
            == iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(right)?,
    )
}
async fn any_rows(frame: DataFrame) -> Result<bool> {
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
