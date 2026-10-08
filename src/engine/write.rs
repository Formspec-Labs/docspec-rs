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

impl Engine {
    pub(super) async fn create_output(
        &self,
        dataset: &Id,
        directory: &Path,
        schema: Schema,
    ) -> Result<Table> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(dataset).await?;
        Ok(lifecycle
            .catalog
            .create_table(
                view.table.identifier().namespace(),
                TableCreation::builder()
                    .name(format!("data_{}", uuid::Uuid::new_v4().simple()))
                    .location(directory.to_string_lossy().into_owned())
                    .schema(schema)
                    .properties(storage::table_properties())
                    .build(),
            )
            .await?)
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
        let schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(dataframe.schema().as_arrow())?;
        let table = self.create_output(&spec.dataset, directory, schema).await?;
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
        let table = storage::pin(&spec.dataset, &table, rows, budget, &[]).await?;
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
        if base.dataset.groups.len() != 1 {
            return Err(Error::refusal(
                "dependent_groups",
                "source updates require dependent field-group refresh before selection",
            ));
        }
        let before = base
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| Error::refusal("invalid_base", "base has no source table"))?;
        let budget = &self.inner.lifecycle.budget;
        // Rewritten files will leave the next inventory. Check their old byte
        // identities before reading so corruption cannot become a new valid result.
        storage::verify_objects(before.objects.clone(), budget).await?;
        let native = storage::open_pin(before).await?;
        let inventory = storage::snapshot_files(&native, budget).await?;
        storage::check_inventory_files(before, &inventory)?;
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
            if any_rows(
                &context,
                &format!(
                    "SELECT 1 FROM replacements l INNER JOIN deletions r ON {} LIMIT 1",
                    key_match(&spec.keys, "l", "r")
                ),
            )
            .await?
            {
                return Err(Error::refusal(
                    "conflicting_update",
                    "replacement and deletion keys overlap",
                ));
            }
        }
        let marker = format!("__docspec_match_{}", uuid::Uuid::new_v4().simple());
        let scope_sql = if spec.deletions.is_some() {
            format!(
                "SELECT DISTINCT {keys}, TRUE AS {marker} FROM replacements UNION SELECT DISTINCT {keys}, TRUE AS {marker} FROM deletions"
            )
        } else {
            format!("SELECT DISTINCT {keys}, TRUE AS {marker} FROM replacements")
        };
        let scope = context.sql(&scope_sql).await?;
        // Prepare distinct keys once. A logical view would rescan all input rows
        // and repeat DISTINCT for every source-file probe and rewrite.
        let scope_table = self
            .create_output(
                &spec.dataset,
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
        let mut shared = Vec::new();
        let mut changed = Vec::new();
        let mut rewrite_bytes = 0_u64;
        for (index, file) in inventory.data.iter().enumerate() {
            let name = format!("chunk_{index}");
            context
                .register_parquet(
                    name.as_str(),
                    file.file_path(),
                    ParquetReadOptions::default(),
                )
                .await?;
            if any_rows(
                &context,
                &format!(
                    "SELECT 1 FROM {name} l INNER JOIN affected_keys r ON {} LIMIT 1",
                    key_match(&spec.keys, "l", "r")
                ),
            )
            .await?
            {
                rewrite_bytes = rewrite_bytes.saturating_add(file.file_size_in_bytes());
                // One joined decode supplies both kept rows and actual removed rows.
                changed.push(context.sql(&format!("SELECT l.*, r.{marker} IS NOT NULL AS {marker} FROM {name} l LEFT JOIN affected_keys r ON {}", key_match(&spec.keys, "l", "r"))).await?);
            } else {
                shared.push(file.clone());
            }
        }
        let required = rewrite_bytes
            .saturating_mul(2)
            .saturating_add(
                admission
                    .outputs
                    .iter()
                    .fold(0_u64, |sum, object| sum.saturating_add(object.bytes))
                    .saturating_mul(2),
            )
            .saturating_add(budget.limits.scratch_bytes);
        if fs2::available_space(&self.inner.root)? < required {
            return Err(Error::refusal(
                "disk_capacity",
                "affected-file rewrites, change rows and declared scratch do not fit available disk",
            ));
        }
        let candidate = self
            .create_output(
                &spec.dataset,
                &directory.join("data"),
                native.metadata().current_schema().as_ref().clone(),
            )
            .await?;
        let removed = self
            .create_output(
                &spec.dataset,
                &directory.join("removed"),
                native.metadata().current_schema().as_ref().clone(),
            )
            .await?;
        let added = self
            .create_output(
                &spec.dataset,
                &directory.join("added"),
                native.metadata().current_schema().as_ref().clone(),
            )
            .await?;
        let mut metrics = WriteMetrics {
            files_scanned: (inventory.data.len() + changed.len()) as u64,
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
            replacement
                .execute_stream()
                .await?
                .map(|batch| batch.map_err(Error::from)),
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
        let candidate = storage::append(&candidate, catalog, shared).await?;
        let removed = storage::append(&removed, catalog, removed_files).await?;
        let added = storage::append(&added, catalog, added_files).await?;
        let table = storage::pin(&spec.dataset, &candidate, rows, budget, &before.objects).await?;
        let removed =
            storage::pin(&spec.dataset, &removed, metrics.rows_removed, budget, &[]).await?;
        let added = storage::pin(
            &spec.dataset,
            &added,
            metrics.rows_added,
            budget,
            &table.objects,
        )
        .await?;
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
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn key_match(keys: &[String], left: &str, right: &str) -> String {
    keys.iter()
        .map(|key| {
            let key = identifier(key);
            format!("({left}.{key} IS NOT DISTINCT FROM {right}.{key})")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}
fn same_fields(left: &arrow::datatypes::Schema, right: &arrow::datatypes::Schema) -> Result<bool> {
    // The maintained converter assigns comparison-only IDs consistently and
    // preserves nested names, types and nullability. Caller files need no IDs.
    Ok(
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(left)?
            == iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(right)?,
    )
}
async fn any_rows(context: &SessionContext, sql: &str) -> Result<bool> {
    let batches = context.sql(sql).await?.collect().await?;
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
