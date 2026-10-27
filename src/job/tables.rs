//! Native table transactions; callers supply work and writer policy.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, storage};
use iceberg::{Catalog, TableIdent, spec::DataFile, table::Table};
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

/// Purges only the exact table selected by the caller, in one native attempt.
///
/// # Errors
/// Refuses an absent metadata location or changed UUID/metadata. Catalog purge
/// errors propagate without blind retry; the caller must reload and recheck.
pub async fn purge_if_current(catalog: &dyn Catalog, selected: &Table) -> Result<()> {
    if selected.metadata_location().is_none() {
        return Err(Error::refusal(
            "invalid_spec",
            "purge requires committed table metadata",
        ));
    }
    let current = catalog.load_table(selected.identifier()).await?;
    if current.metadata().uuid() != selected.metadata().uuid()
        || current.metadata_location() != selected.metadata_location()
    {
        return Err(Error::refusal(
            "stale_authority",
            "table changed before purge",
        ));
    }
    catalog.purge_table(selected.identifier()).await?;
    Ok(())
}
