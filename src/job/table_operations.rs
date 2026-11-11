//! Explicit native catalog reads and writes share one job context.
// Rust guideline compliant 2026-02-21
use super::{
    Job, query_result,
    tables::{AddedColumn, AppendWork, OptionalColumn, append_with_retry},
};
use crate::{
    Error, Result,
    storage::{self, WriteLayout},
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{FormatVersion, Schema, SortOrder, UnboundPartitionSpec},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

fn default_format_version() -> FormatVersion {
    FormatVersion::V2
}

/// One explicit catalog operation; schemas, work and physical layout are caller policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableOperation {
    /// Resolve one current catalog head without data reads or catalog writes.
    /// Multiple operations batch independent heads in one job/process.
    Resolve { name: String, table: TableIdent },
    CreateNamespace {
        name: String,
        namespace: NamespaceIdent,
        #[serde(default)]
        properties: HashMap<String, String>,
    },
    Create {
        name: String,
        table: TableIdent,
        location: String,
        schema: Box<Schema>,
        #[serde(default = "default_format_version")]
        format_version: FormatVersion,
        #[serde(default)]
        properties: HashMap<String, String>,
        #[serde(default)]
        partition_spec: Option<UnboundPartitionSpec>,
        #[serde(default)]
        sort_order: Option<SortOrder>,
    },
    Append {
        name: String,
        table: TableIdent,
        current_table: String,
        work_table: String,
        sql: String,
        identity_sql: String,
        layout: WriteLayout,
    },
    Purge {
        name: String,
        table: TableIdent,
        table_uuid: uuid::Uuid,
        metadata_location: String,
    },
    /// Adds optional top-level columns while the schema is `expected_schema_id`;
    /// a request whose columns all exist is a no-op.
    AddColumns {
        name: String,
        table: TableIdent,
        expected_schema_id: i32,
        columns: Vec<OptionalColumn>,
    },
}
/// Completed catalog authority, retained if any later operation fails.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableOperationResult {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<TableIdent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// `add_columns` only: the schema before the operation; equal to `schema_id` on a no-op.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_schema_id: Option<i32>,
    /// `add_columns` only: columns this operation committed; empty on a no-op.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_columns: Option<Vec<AddedColumn>>,
    pub retries: usize,
    pub files: Vec<WrittenFile>,
}
/// Identity metadata from the native writer without reopening its data.
#[derive(Debug, Serialize, Deserialize)]
pub struct WrittenFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
}
fn invalid(message: &str) -> Error {
    Error::refusal("invalid_spec", message)
}
impl TableOperation {
    pub(super) fn is_read_only(&self) -> bool {
        matches!(self, Self::Resolve { .. })
    }
    pub(super) fn validate_all(operations: &[Self], mut bindings: BTreeSet<String>) -> Result<()> {
        let mut names = BTreeSet::new();
        for operation in operations {
            let name = operation.name();
            if name.is_empty() || !names.insert(name.to_lowercase()) {
                return Err(invalid("duplicate or empty table operation name"));
            }
            if let Self::Create {
                schema,
                format_version,
                properties,
                ..
            } = operation
            {
                let mut ids = schema.identifier_field_ids().peekable();
                if *format_version != FormatVersion::V2
                    || ids.peek().is_none()
                    || ids.any(|id| schema.field_by_id(id).is_none_or(|field| !field.required))
                    || crate::storage::table_properties()
                        .iter()
                        .any(|(k, v)| properties.get(k).is_some_and(|actual| actual != v))
                {
                    return Err(invalid(
                        "table creation requires format_version 2, required identifier fields, and gc.enabled=true",
                    ));
                }
            }
            if let Self::Append {
                current_table,
                work_table,
                sql,
                identity_sql,
                ..
            } = operation
            {
                for name in [current_table, work_table] {
                    if name.is_empty() || !bindings.insert(name.to_lowercase()) {
                        return Err(invalid(
                            "append bindings must be distinct from inputs and steps",
                        ));
                    }
                }
                if sql.trim().is_empty() || identity_sql.trim().is_empty() {
                    return Err(invalid("append SQL and identity SQL are required"));
                }
            }
            if let Self::AddColumns { columns, .. } = operation {
                // A dotted name would address a nested field; only top-level additions exist.
                let mut seen = BTreeSet::new();
                if columns.is_empty()
                    || columns.iter().any(|column| {
                        column.name.is_empty()
                            || column.name.contains('.')
                            || !seen.insert(column.name.as_str())
                    })
                {
                    return Err(invalid(
                        "add_columns needs distinct, nonempty, top-level column names",
                    ));
                }
            }
        }
        Ok(())
    }
    fn name(&self) -> &str {
        match self {
            Self::Resolve { name, .. }
            | Self::CreateNamespace { name, .. }
            | Self::Create { name, .. }
            | Self::Append { name, .. }
            | Self::Purge { name, .. }
            | Self::AddColumns { name, .. } => name,
        }
    }
    pub(super) async fn execute(
        &self,
        job: &Job,
        catalog: &dyn Catalog,
    ) -> Result<TableOperationResult> {
        // Retain metadata admission through the catalog load and extraction.
        let _resolution_memory = if self.is_read_only() {
            Some(job.budget.reserve(job.budget.limits.max_metadata_bytes)?)
        } else {
            None
        };
        let mut result = TableOperationResult {
            name: self.name().to_owned(),
            kind: String::new(),
            exists: None,
            table: None,
            table_uuid: None,
            metadata_location: None,
            snapshot: None,
            schema_id: None,
            previous_schema_id: None,
            added_columns: None,
            retries: 0,
            files: vec![],
        };
        let completed = match self {
            Self::Resolve { table, .. } => {
                result.kind = "resolve".into();
                match catalog.load_table(table).await {
                    Ok(loaded) => {
                        loaded.metadata_location_result()?;
                        result.exists = Some(true);
                        loaded
                    }
                    Err(error) if error.kind() == iceberg::ErrorKind::TableNotFound => {
                        result.exists = Some(false);
                        result.table = Some(table.clone());
                        return Ok(result);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Self::CreateNamespace {
                namespace,
                properties,
                ..
            } => {
                catalog
                    .create_namespace(namespace, properties.clone())
                    .await?;
                result.kind = "create_namespace".into();
                return Ok(result);
            }
            Self::Create {
                table,
                location,
                schema,
                format_version,
                properties,
                partition_spec,
                sort_order,
                ..
            } => {
                result.kind = "create".into();
                let mut effective = storage::table_properties();
                effective.extend(properties.iter().map(|(k, v)| (k.clone(), v.clone())));
                catalog
                    .create_table(
                        table.namespace(),
                        TableCreation::builder()
                            .name(table.name().to_owned())
                            .location(location.clone())
                            .schema((**schema).clone())
                            .format_version(*format_version)
                            .properties(effective)
                            .partition_spec_opt(partition_spec.clone())
                            .sort_order_opt(sort_order.clone())
                            .build(),
                    )
                    .await?
            }
            Self::Append {
                table,
                current_table,
                work_table,
                sql,
                identity_sql,
                layout,
                ..
            } => {
                result.kind = "append".into();
                let outcome = append_with_retry(
                    catalog,
                    table,
                    |table| async move {
                        for name in [work_table, current_table] {
                            if job.context.table_exist(name)? {
                                job.release(name)?;
                            }
                        }
                        job.register_native_table(current_table, table).await?;
                        job.materialize(work_table, sql).await?;
                        let provider = job.context.table_provider(work_table).await?;
                        let rows = match provider.statistics().map(|s| s.num_rows) {
                            Some(datafusion::common::stats::Precision::Exact(rows)) => rows,
                            _ => {
                                return Err(invalid(
                                    "materialized append work has no exact row count",
                                ));
                            }
                        };
                        if rows == 0 {
                            return Ok(AppendWork::Empty);
                        }
                        let frame = job.context.table(work_table).await?;
                        let identity =
                            query_result::query(job, "append_identity", identity_sql).await?;
                        let rows = identity.data.as_array().ok_or_else(|| {
                            invalid("append identity must be one nonempty string cell")
                        })?;
                        let value = rows
                            .first()
                            .and_then(serde_json::Value::as_object)
                            .filter(|row| row.len() == 1)
                            .and_then(|row| row.values().next())
                            .and_then(serde_json::Value::as_str)
                            .filter(|value| !value.is_empty());
                        if rows.len() != 1 || value.is_none() {
                            return Err(invalid(
                                "append identity must be one nonempty string cell",
                            ));
                        }
                        Ok(AppendWork::Rows {
                            identity: value.expect("validated identity").to_owned(),
                            rows: frame,
                        })
                    },
                    |table, frame| async move {
                        let stream = super::plan::execute_stream(frame)
                            .await?
                            .map_err(Error::from);
                        storage::write_batches_with_layout(&table, stream, &job.budget, *layout)
                            .await
                    },
                )
                .await;
                // Release both bindings on success or refusal; in-flight frames
                // retain their providers until the writer and its admission end.
                for name in [work_table, current_table] {
                    if job.context.table_exist(name)? {
                        job.release(name)?;
                    }
                }
                let outcome = outcome?;
                result.retries = outcome.retries;
                result.files = outcome
                    .files
                    .iter()
                    .map(|file| WrittenFile {
                        path: file.file_path().to_owned(),
                        rows: file.record_count(),
                        bytes: file.file_size_in_bytes(),
                    })
                    .collect();
                outcome.table
            }
            Self::Purge {
                table,
                table_uuid,
                metadata_location,
                ..
            } => {
                super::tables::purge_selected(catalog, table, *table_uuid, metadata_location)
                    .await?;
                result.kind = "purge".into();
                result.table = Some(table.clone());
                result.table_uuid = Some(*table_uuid);
                result.metadata_location = Some(metadata_location.clone());
                return Ok(result);
            }
            Self::AddColumns {
                table,
                expected_schema_id,
                columns,
                ..
            } => {
                result.kind = "add_columns".into();
                let outcome = super::tables::add_optional_columns(
                    catalog,
                    table,
                    *expected_schema_id,
                    columns,
                )
                .await?;
                result.previous_schema_id = Some(outcome.previous_schema_id);
                result.added_columns = Some(outcome.added);
                outcome.table
            }
        };
        result.table = Some(if let Self::Resolve { table, .. } = self {
            // A generation-scoped catalog may return its physical identifier.
            // Report the logical name requested by the caller, without remapping.
            table.clone()
        } else {
            completed.identifier().clone()
        });
        result.table_uuid = Some(completed.metadata().uuid());
        result.metadata_location = completed.metadata_location().map(str::to_owned);
        result.snapshot = completed.metadata().current_snapshot_id();
        result.schema_id = Some(completed.metadata().current_schema_id());
        Ok(result)
    }
}
