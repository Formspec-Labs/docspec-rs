//! Native table transactions; callers supply work and writer policy.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, storage};
use iceberg::{
    Catalog, TableIdent,
    spec::{DataFile, PrimitiveType, Type},
    table::Table,
    transaction::{AddColumn, ApplyTransactionAction, Transaction},
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    future::Future,
    time::{Duration, Instant},
};

/// A freshly evaluated work list, with an identity covering its selected rows
/// and writer definition. Empty work performs neither a write nor a commit.
#[derive(Debug)]
pub enum AppendWork<T> {
    Empty,
    Rows { identity: String, rows: T },
}

/// Native authority after append, including already written file identities.
#[derive(Debug)]
pub struct AppendOutcome {
    pub table: Table,
    pub files: Vec<DataFile>,
    pub retries: usize,
    pub commit_duration: Duration,
}

fn conflict(error: &Error) -> bool {
    let mut cause: &dyn StdError = error;
    loop {
        if let Some(native) = cause.downcast_ref::<iceberg::Error>() {
            return native.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                || (native.kind() == iceberg::ErrorKind::DataInvalid
                    && native.message().starts_with("stale_authority:"));
        }
        match cause.source() {
            Some(source) => cause = source,
            None => return false,
        }
    }
}

/// Re-evaluates work against current authority after at most three conflicts.
///
/// `prepare` must identify the complete selected work and writer definition.
/// `write` uses that work with the supplied table schema and partition spec.
/// Completed files are reused only for the same work, UUID, schema and spec.
///
/// # Errors
/// Refuses empty work identities, nonempty work producing no files, exhausted
/// conflicts, catalog failures, and caller preparation/writer failures.
pub async fn append_with_retry<T, P, PF, W, WF>(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    mut prepare: P,
    mut write: W,
) -> Result<AppendOutcome>
where
    P: FnMut(Table) -> PF,
    PF: Future<Output = Result<AppendWork<T>>>,
    W: FnMut(Table, T) -> WF,
    WF: Future<Output = Result<Vec<DataFile>>>,
{
    let mut completed: Option<(Table, String, Vec<DataFile>)> = None;
    for retries in 0..=3 {
        let table = catalog.load_table(ident).await?;
        let AppendWork::Rows { identity, rows } = prepare(table.clone()).await? else {
            return Ok(AppendOutcome {
                table,
                files: vec![],
                retries,
                commit_duration: Duration::ZERO,
            });
        };
        if identity.is_empty() {
            return Err(Error::refusal(
                "invalid_spec",
                "append work identity is empty",
            ));
        }
        let reusable = completed.as_ref().is_some_and(|(base, work, _)| {
            work == &identity
                && base.metadata().uuid() == table.metadata().uuid()
                && base.metadata().current_schema_id() == table.metadata().current_schema_id()
                && base.metadata().default_partition_spec_id()
                    == table.metadata().default_partition_spec_id()
        });
        if !reusable {
            let files = write(table.clone(), rows).await?;
            if files.is_empty() {
                return Err(Error::refusal(
                    "invalid_spec",
                    "nonempty append work produced no files",
                ));
            }
            completed = Some((table.clone(), identity, files));
        }
        let files = &completed
            .as_ref()
            .expect("nonempty work owns completed files")
            .2;
        let started = Instant::now();
        match storage::append(&table, catalog, files.clone()).await {
            Ok(table) => {
                return Ok(AppendOutcome {
                    table,
                    files: completed.take().expect("committed files exist").2,
                    retries,
                    commit_duration: started.elapsed(),
                });
            }
            Err(error) if conflict(&error) && retries < 3 => continue,
            Err(error) if conflict(&error) => {
                return Err(Error::refusal(
                    "catalog_conflict",
                    "append did not settle within three conflict retries",
                ));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded append attempts return or refuse")
}

/// One optional top-level column to add; the table assigns its field id.
///
/// The shape cannot express a required column, a caller field id or a nested type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: PrimitiveType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// A column committed by [`add_optional_columns`], with its fresh native field id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddedColumn {
    pub name: String,
    pub field_id: i32,
}

/// Native authority after [`add_optional_columns`]; no added columns means a no-op.
#[derive(Debug)]
pub(super) struct ColumnsOutcome {
    pub table: Table,
    pub previous_schema_id: i32,
    pub added: Vec<AddedColumn>,
}

/// Adds missing optional top-level columns in one guarded schema commit.
///
/// The job admitted `columns` as a nonempty list of distinct, undotted names.
/// Columns already present as optional with the same type are skipped. When
/// every column is present nothing is committed, whatever `expected_schema_id`
/// says, so repeating a completed request (a retry after a lost response) is a
/// no-op. Otherwise the current schema must be `expected_schema_id`, and the
/// head must not move before the commit. New columns follow the existing ones in
/// request order, with fresh field ids after the table's last column id.
///
/// # Errors
/// Refuses a name present with another type or as required (`schema_mismatch`),
/// a schema other than `expected_schema_id` or a head that moves before the
/// commit (`stale_authority`), and catalog failures, without retry.
pub(super) async fn add_optional_columns(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    expected_schema_id: i32,
    columns: &[OptionalColumn],
) -> Result<ColumnsOutcome> {
    let table = catalog.load_table(ident).await?;
    let schema = table.metadata().current_schema();
    let previous_schema_id = schema.schema_id();
    let mut missing = Vec::new();
    for column in columns {
        match schema.as_struct().field_by_name(&column.name) {
            None => missing.push(column),
            Some(field)
                if !field.required
                    && *field.field_type == Type::Primitive(column.field_type.clone()) => {}
            Some(_) => {
                return Err(Error::refusal(
                    "schema_mismatch",
                    format!(
                        "column {} exists with another type or as required",
                        column.name
                    ),
                ));
            }
        }
    }
    if missing.is_empty() {
        return Ok(ColumnsOutcome {
            table,
            previous_schema_id,
            added: vec![],
        });
    }
    if previous_schema_id != expected_schema_id {
        return Err(Error::refusal(
            "stale_authority",
            format!("table schema {previous_schema_id} is not the expected {expected_schema_id}"),
        ));
    }
    let tx = Transaction::new(&table);
    let action = missing.iter().fold(tx.update_schema(), |action, column| {
        let field_type = Type::Primitive(column.field_type.clone());
        action.add_column(match &column.doc {
            Some(doc) => AddColumn::builder()
                .name(column.name.clone())
                .field_type(field_type)
                .doc(doc.clone())
                .build(),
            None => AddColumn::optional(&column.name, field_type),
        })
    });
    // The guard refuses any head but the one checked above, so native refresh
    // never re-applies these additions to a schema the caller did not expect.
    let guard = crate::catalog::GuardedCatalog::new(catalog, &table);
    let committed = match action.apply(tx)?.commit(&guard).await.map_err(Error::from) {
        Ok(committed) => committed,
        Err(error) if conflict(&error) => {
            return Err(Error::refusal(
                "stale_authority",
                "table changed before its columns were added",
            ));
        }
        Err(error) => return Err(error),
    };
    let schema = committed.metadata().current_schema();
    let added = missing
        .iter()
        .map(|column| AddedColumn {
            name: column.name.clone(),
            field_id: schema
                .as_struct()
                .field_by_name(&column.name)
                .expect("the committed schema holds every added column")
                .id,
        })
        .collect();
    Ok(ColumnsOutcome {
        table: committed,
        previous_schema_id,
        added,
    })
}

/// Purges only the exact table selected by the caller, in one native attempt.
///
/// # Errors
/// Refuses an absent metadata location or changed UUID/metadata. Catalog purge
/// errors propagate without blind retry; the caller must reload and recheck.
pub async fn purge_if_current(catalog: &dyn Catalog, selected: &Table) -> Result<()> {
    let metadata = selected
        .metadata_location()
        .ok_or_else(|| Error::refusal("invalid_spec", "purge requires committed table metadata"))?;
    purge_selected(
        catalog,
        selected.identifier(),
        selected.metadata().uuid(),
        metadata,
    )
    .await
}

/// Purges one exact native UUID and metadata reference after reloading authority.
///
/// # Errors
/// Refuses changed authority or any native catalog failure, without blind retry.
pub async fn purge_selected(
    catalog: &dyn Catalog,
    table: &TableIdent,
    table_uuid: uuid::Uuid,
    metadata_location: &str,
) -> Result<()> {
    let current = catalog.load_table(table).await?;
    if current.metadata().uuid() != table_uuid
        || current.metadata_location() != Some(metadata_location)
    {
        return Err(Error::refusal(
            "stale_authority",
            "table changed before purge",
        ));
    }
    catalog.purge_table(table).await?;
    Ok(())
}
