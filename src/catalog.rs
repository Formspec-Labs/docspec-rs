//! Native Iceberg lifecycle rows with stale-decision refusal at the catalog boundary.
//!
//! The initial layout appends bounded key/value changes. It is deliberately small;
//! metadata-growth qualification must precede adopting it for large histories.

use crate::{Error, Result, budget::Budget, model::Id, storage};
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, table::Table,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// Lifecycle authority injected with a maintained catalog and shared budget.
#[derive(Clone, Debug)]
pub struct Lifecycle {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) budget: Budget,
    cache: Arc<Mutex<Option<Arc<SnapshotRows>>>>,
}

#[derive(Debug)]
pub(crate) struct SnapshotRows {
    metadata: String,
    values: BTreeMap<String, (i64, String)>,
    history_bytes: usize,
    _reservation: tokio::sync::OwnedSemaphorePermit,
}

impl SnapshotRows {
    /// Reads a value with its native commit revision and serialized byte charge.
    pub(crate) fn get_record<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<(i64, usize, T)>> {
        self.values
            .get(key)
            .map(|(revision, json)| Ok((*revision, json.len(), serde_json::from_str(json)?)))
            .transpose()
    }
}

/// One immutable read boundary for an application decision.
#[derive(Debug, Clone)]
pub struct View {
    pub(crate) table: Table,
    pub revision: i64,
}

impl Lifecycle {
    /// Connects lifecycle operations to a maintained catalog.
    pub fn new(catalog: Arc<dyn Catalog>, budget: Budget) -> Self {
        Self {
            catalog,
            budget,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens or creates a dataset's lifecycle authority.
    ///
    /// # Errors
    /// Returns catalog or schema errors; callers serialize first-time creation.
    pub async fn open(&self, dataset: &Id) -> Result<View> {
        let namespace = NamespaceIdent::new(format!("d_{}", dataset.as_str().replace('-', "_")));
        // Preserve distinct names such as a-b and a_b by including a digest.
        let namespace = NamespaceIdent::new(format!(
            "{}_{}",
            namespace,
            &blake3::hash(dataset.as_str().as_bytes()).to_hex()[..12]
        ));
        if !self.catalog.namespace_exists(&namespace).await? {
            self.catalog
                .create_namespace(&namespace, HashMap::new())
                .await?;
        }
        let ident = TableIdent::new(namespace.clone(), "lifecycle".into());
        let table = if self.catalog.table_exists(&ident).await? {
            self.catalog.load_table(&ident).await?
        } else {
            let schema = Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("revision", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ]);
            self.catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("lifecycle".into())
                        .schema(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
                            &schema,
                        )?)
                        .properties(storage::table_properties())
                        .build(),
                )
                .await?
        };
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
        })
    }

    /// Reads the latest value at an exact authority boundary.
    ///
    /// # Errors
    /// Refuses malformed rows, oversized metadata, or invalid serialized values.
    pub async fn get<T: DeserializeOwned>(&self, view: &View, key: &str) -> Result<Option<T>> {
        Ok(self
            .rows(view)
            .await?
            .get_record(key)?
            .map(|(_, _, value)| value))
    }

    /// Lists current records at one authority boundary within the metadata budget.
    ///
    /// # Errors
    /// Refuses corrupt or oversized lifecycle history.
    pub async fn records(&self, view: &View) -> Result<BTreeMap<String, serde_json::Value>> {
        self.rows(view)
            .await?
            .values
            .iter()
            .map(|(key, (_, value))| Ok((key.clone(), serde_json::from_str(value)?)))
            .collect()
    }

    /// Retains one immutable state and its byte reservation across cache replacement.
    pub(crate) async fn rows(&self, view: &View) -> Result<Arc<SnapshotRows>> {
        let metadata = view.table.metadata_location_result()?.to_owned();
        if let Some(hit) = self
            .cache
            .lock()
            .map_err(cache_poisoned)?
            .as_ref()
            .filter(|entry| entry.metadata == metadata)
            .cloned()
        {
            return Ok(hit);
        }
        let reservation = self.budget.reserve(self.budget.limits.max_metadata_bytes)?;
        let mut current = BTreeMap::<String, (i64, String)>::new();
        if view.table.metadata().current_snapshot().is_none() {
            let entry = Arc::new(SnapshotRows {
                metadata,
                values: current,
                history_bytes: 0,
                _reservation: reservation,
            });
            *self.cache.lock().map_err(cache_poisoned)? = Some(entry.clone());
            return Ok(entry);
        }
        let batches = storage::read_batches(&view.table, &self.budget).await?;
        futures::pin_mut!(batches);
        let mut bytes = 0_usize;
        while let Some(batch) = batches.try_next().await? {
            let keys = strings(&batch, 0)?;
            let values = strings(&batch, 2)?;
            let revisions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::refusal("corrupt_metadata", "invalid revision column"))?;
            for i in 0..batch.num_rows() {
                bytes = bytes.saturating_add(history_row_bytes(keys.value(i), values.value(i)));
                check_history_size(bytes, self.budget.limits.max_metadata_bytes)?;
                let revision = revisions.value(i);
                if current
                    .get(keys.value(i))
                    .is_none_or(|(old, _)| *old < revision)
                {
                    current.insert(
                        keys.value(i).to_owned(),
                        (revision, values.value(i).to_owned()),
                    );
                }
            }
        }
        check_cache_size(&current, self.budget.limits.max_metadata_bytes)?;
        let entry = Arc::new(SnapshotRows {
            metadata,
            values: current,
            history_bytes: bytes,
            _reservation: reservation,
        });
        *self.cache.lock().map_err(cache_poisoned)? = Some(entry.clone());
        Ok(entry)
    }

    /// Atomically appends changes only while the captured authority remains current.
    ///
    /// # Errors
    /// Refuses stale views, oversized metadata, or storage failures. No blind retry occurs.
    pub async fn commit(
        &self,
        view: &View,
        changes: Vec<(String, serde_json::Value)>,
    ) -> Result<View> {
        if changes.is_empty() {
            return Err(Error::refusal(
                "empty_commit",
                "a lifecycle commit must change state",
            ));
        }
        let unique: std::collections::BTreeSet<_> = changes.iter().map(|(key, _)| key).collect();
        if unique.len() != changes.len() {
            return Err(Error::refusal(
                "duplicate_key",
                "each lifecycle key may change only once per commit",
            ));
        }
        let revision = view
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::refusal("revision_exhausted", "revision overflow"))?;
        let values = changes
            .iter()
            .map(|(_, v)| serde_json::to_string(v))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let bytes: usize = values.iter().map(String::len).sum::<usize>()
            + changes.iter().map(|(k, _)| k.len()).sum::<usize>();
        if bytes > self.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "lifecycle commit exceeds its byte budget",
            ));
        }
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            view.table.metadata().current_schema(),
        )?);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    changes.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![revision; changes.len()])),
                Arc::new(StringArray::from(
                    values.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
            ],
        )?;
        let guard = GuardedCatalog {
            inner: self.catalog.clone(),
            expected: view.table.clone(),
        };
        guard.load_table(view.table.identifier()).await?;
        // Prepare a bounded cache of the next immutable state. It becomes visible
        // only after the guarded native commit succeeds; failed commits cannot leak it.
        let reservation = self.budget.reserve(self.budget.limits.max_metadata_bytes)?;
        let previous = self.rows(view).await?;
        let history_bytes = changes
            .iter()
            .zip(&values)
            .fold(previous.history_bytes, |bytes, ((key, _), value)| {
                bytes.saturating_add(history_row_bytes(key, value))
            });
        // Warm and cold paths apply the same bound before acknowledging a commit.
        // Obsolete appended versions still count until compaction removes them.
        check_history_size(history_bytes, self.budget.limits.max_metadata_bytes)?;
        let mut next_values = previous.values.clone();
        for ((key, _), value) in changes.iter().zip(values) {
            next_values.insert(key.clone(), (revision, value));
        }
        check_cache_size(&next_values, self.budget.limits.max_metadata_bytes)?;
        let files = storage::write_batches(
            &view.table,
            futures::stream::iter(vec![Ok(batch)]),
            &self.budget,
        )
        .await?;
        let table = storage::append(&view.table, &guard, files).await?;
        let entry = Arc::new(SnapshotRows {
            metadata: table.metadata_location_result()?.to_owned(),
            values: next_values,
            history_bytes,
            _reservation: reservation,
        });
        *self.cache.lock().map_err(cache_poisoned)? = Some(entry);
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
        })
    }
}

fn history_row_bytes(key: &str, value: &str) -> usize {
    // Conservative per-record overhead for map nodes, revisions and owned strings.
    key.len().saturating_add(value.len()).saturating_add(128)
}

fn check_history_size(bytes: usize, limit: usize) -> Result<()> {
    if bytes > limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "lifecycle history exceeds its replay budget",
        ));
    }
    Ok(())
}

fn cache_poisoned<T>(_: std::sync::PoisonError<T>) -> Error {
    Error::refusal(
        "cache_unavailable",
        "lifecycle cache lock poisoned; reopen the workspace",
    )
}

fn check_cache_size(rows: &BTreeMap<String, (i64, String)>, limit: usize) -> Result<()> {
    let bytes = rows.iter().fold(0usize, |size, (key, (_, value))| {
        size.saturating_add(key.capacity())
            .saturating_add(value.capacity())
            .saturating_add(128)
    });
    if bytes > limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "current lifecycle records exceed the snapshot cache budget",
        ));
    }
    Ok(())
}

/// Encodes a bounded lifecycle record for an atomic commit.
///
/// # Errors
/// Returns serialization errors without changing catalog state.
pub fn record(
    key: impl Into<String>,
    value: impl Serialize,
) -> Result<(String, serde_json::Value)> {
    Ok((key.into(), serde_json::to_value(value)?))
}

fn strings(batch: &RecordBatch, column: usize) -> Result<&StringArray> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::refusal("corrupt_metadata", "invalid lifecycle string column"))
}

/// Stops native transaction refresh/retry from adopting another application's state.
#[derive(Debug)]
struct GuardedCatalog {
    inner: Arc<dyn Catalog>,
    expected: Table,
}

#[async_trait]
impl Catalog for GuardedCatalog {
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let current = self.inner.load_table(ident).await?;
        if ident != self.expected.identifier()
            || current.metadata_location() != self.expected.metadata_location()
        {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "stale_authority: reload and re-evaluate application guards",
            ));
        }
        Ok(current)
    }
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.load_table(commit.identifier()).await?;
        // FastAppend supplies the expected branch snapshot requirement. The maintained
        // SQL catalog also compare-and-swaps metadata_location after applying it.
        self.inner
            .update_table(commit)
            .await
            .map_err(|e| e.with_retryable(false))
    }
    async fn list_namespaces(
        &self,
        p: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(n).await
    }
    async fn update_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> iceberg::Result<Table> {
        self.inner.create_table(n, c).await
    }
    async fn drop_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> iceberg::Result<Table> {
        self.inner.register_table(t, m).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Limits;

    #[tokio::test]
    async fn retained_snapshot_survives_cache_replacement_and_holds_its_reservation() {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 32,
            max_batch_bytes: 4096,
            max_metadata_bytes: 1 << 20,
        })
        .unwrap();
        let lifecycle = Lifecycle::new(
            storage::local_catalog(root.path()).await.unwrap(),
            budget.clone(),
        );
        let a = lifecycle.open(&Id::new("a").unwrap()).await.unwrap();
        let a = lifecycle
            .commit(&a, vec![record("key", "first").unwrap()])
            .await
            .unwrap();
        let retained = lifecycle.rows(&a).await.unwrap();
        let weak = Arc::downgrade(&retained);
        let b = lifecycle.open(&Id::new("b").unwrap()).await.unwrap();
        let b = lifecycle
            .commit(&b, vec![record("key", "other dataset").unwrap()])
            .await
            .unwrap();
        assert_ne!(
            retained.metadata,
            lifecycle.rows(&b).await.unwrap().metadata
        );
        // The old native files are now inaccessible. Record reads must still use
        // the retained state; a cache lookup and replay would fail here.
        std::fs::rename(
            a.table.metadata().location(),
            root.path().join("unavailable-a"),
        )
        .unwrap();
        for _ in 0..3 {
            assert_eq!(
                retained.get_record::<String>("key").unwrap(),
                Some((a.revision, 7, "first".into()))
            );
        }
        assert_eq!(Arc::strong_count(&retained), 1);
        let remaining = budget.limits.memory_bytes / 2 - 2 * budget.limits.max_metadata_bytes;
        let available = budget.reserve(remaining).unwrap();
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop(retained);
        assert!(weak.upgrade().is_none());
        let reclaimed = budget.reserve(budget.limits.max_metadata_bytes).unwrap();
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop((available, reclaimed));
    }
}
