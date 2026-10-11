//! Native Iceberg lifecycle rows with stale-decision refusal at the catalog boundary.
//!
//! The initial layout appends bounded key/value changes. It is deliberately small;
//! metadata-growth qualification must precede adopting it for large histories.

use crate::{
    Error, Result,
    budget::Budget,
    model::{Id, RemovalIntent, RemovalProgress},
    storage,
};
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
    #[cfg(test)]
    pub(crate) lose_commit_response: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug)]
pub(crate) struct SnapshotRows {
    metadata: String,
    values: BTreeMap<String, (i64, String)>,
    history_bytes: usize,
    reserved_future_bytes: usize,
    _reservation: tokio::sync::OwnedSemaphorePermit,
}

impl SnapshotRows {
    #[cfg(test)]
    pub(crate) fn history_bytes(&self) -> usize {
        self.history_bytes
    }

    pub(crate) fn revision(&self, key: &str) -> Option<i64> {
        self.values.get(key).map(|(revision, _)| *revision)
    }

    /// Reports retained JSON size before a bounded consumer decodes it.
    pub(crate) fn record_bytes(&self, key: &str) -> usize {
        self.values.get(key).map_or(0, |(_, json)| json.len())
    }

    /// Iterates matching current records without allocating another record map.
    pub(crate) fn prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, i64, &'a str)> + 'a {
        self.values
            .range(prefix.to_owned()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, (revision, value))| (key.as_str(), *revision, value.as_str()))
    }

    /// Checks a retained key without decoding a potentially large result or reason.
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Charges decoded copies of cached records before allocating them.
    pub(crate) fn reserve_decoded(
        &self,
        budget: &Budget,
        keys: &[&str],
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let mut bytes = 4096_usize;
        for key in keys {
            if let Some((_, json)) = self.values.get(*key) {
                if json.len() > budget.limits.max_metadata_bytes {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "retained record exceeds metadata allowance",
                    ));
                }
                // Allow decoded strings, collection nodes, and temporary copies.
                // This is admission accounting, not a measured heap size.
                bytes = bytes.saturating_add(json.len().saturating_mul(8));
            }
        }
        budget.reserve(bytes)
    }

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

pub(crate) fn lifecycle_ident(dataset: &Id) -> TableIdent {
    let namespace = NamespaceIdent::new(format!("d_{}", dataset.as_str().replace('-', "_")));
    // Preserve distinct names such as a-b and a_b by including a digest.
    let namespace = NamespaceIdent::new(format!(
        "{}_{}",
        namespace,
        &blake3::hash(dataset.as_str().as_bytes()).to_hex()[..12]
    ));
    TableIdent::new(namespace, "lifecycle".into())
}

impl Lifecycle {
    /// Connects lifecycle operations to a maintained catalog.
    pub fn new(catalog: Arc<dyn Catalog>, budget: Budget) -> Self {
        Self {
            catalog,
            budget,
            cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            lose_commit_response: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Opens or creates a dataset's lifecycle authority.
    ///
    /// # Errors
    /// Returns catalog or schema errors; callers serialize first-time creation.
    pub async fn open(&self, dataset: &Id) -> Result<View> {
        let ident = lifecycle_ident(dataset);
        let namespace = ident.namespace().clone();
        if !self.catalog.namespace_exists(&namespace).await? {
            self.catalog
                .create_namespace(&namespace, HashMap::new())
                .await?;
        }
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
                reserved_future_bytes: 0,
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
        for (key, _) in current
            .range("removal-intent/".to_owned()..)
            .take_while(|(key, _)| key.starts_with("removal-intent/"))
        {
            check_removal_state_exists(&current, key)?;
        }
        let reserved_future_bytes = current
            .range("removal-state/".to_owned()..)
            .take_while(|(key, _)| key.starts_with("removal-state/"))
            .try_fold(0_usize, |total, (key, (revision, value))| {
                let reserved = removal_reservation(&current, key, *revision, value)?;
                total.checked_add(reserved).ok_or_else(reservation_overflow)
            })?;
        check_history_reservation(
            bytes,
            reserved_future_bytes,
            self.budget.limits.max_metadata_bytes,
        )?;
        check_cache_size(&current, self.budget.limits.max_metadata_bytes)?;
        let entry = Arc::new(SnapshotRows {
            metadata,
            values: current,
            history_bytes: bytes,
            reserved_future_bytes,
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
        let guard = GuardedCatalog::new(self.catalog.as_ref(), &view.table);
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
        let mut next_values = previous.values.clone();
        for ((key, _), value) in changes.iter().zip(values) {
            if let Some(id) = key.strip_prefix("removal-intent/") {
                validate_removal_id(id)?;
                if previous.contains_key(key) {
                    return Err(Error::refusal(
                        "corrupt_metadata",
                        "removal intents are immutable",
                    ));
                }
            }
            next_values.insert(key.clone(), (revision, value));
        }
        let mut reserved_future_bytes = previous.reserved_future_bytes;
        for (key, _) in &changes {
            if key.starts_with("removal-intent/") {
                check_removal_state_exists(&next_values, key)?;
            }
            if !key.starts_with("removal-state/") {
                continue;
            }
            if let Some((old_revision, old_value)) = previous.values.get(key) {
                reserved_future_bytes = reserved_future_bytes
                    .checked_sub(removal_reservation(
                        &previous.values,
                        key,
                        *old_revision,
                        old_value,
                    )?)
                    .ok_or_else(reservation_overflow)?;
            }
            let (_, value) = &next_values[key];
            reserved_future_bytes = reserved_future_bytes
                .checked_add(removal_reservation(&next_values, key, revision, value)?)
                .ok_or_else(reservation_overflow)?;
        }
        // Outcomes consume reserved room in the same commit that retains their result.
        check_history_reservation(
            history_bytes,
            reserved_future_bytes,
            self.budget.limits.max_metadata_bytes,
        )?;
        check_cache_size(&next_values, self.budget.limits.max_metadata_bytes)?;
        let files = storage::write_catalog_batches(
            &view.table,
            futures::stream::iter(vec![Ok(batch)]),
            &self.budget,
        )
        .await?;
        let table = storage::append(&view.table, self.catalog.as_ref(), files).await?;
        #[cfg(test)]
        if self
            .lose_commit_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::refusal(
                "lost_commit_response",
                "test interruption after native commit and before cache publication",
            ));
        }
        let entry = Arc::new(SnapshotRows {
            metadata: table.metadata_location_result()?.to_owned(),
            values: next_values,
            history_bytes,
            reserved_future_bytes,
            _reservation: reservation,
        });
        *self.cache.lock().map_err(cache_poisoned)? = Some(entry);
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
        })
    }
}

/// Returns the lifecycle history charge for one serialized row.
pub(crate) fn history_row_bytes(key: &str, value: &str) -> usize {
    // Conservative per-record overhead for map nodes, revisions and owned strings.
    key.len().saturating_add(value.len()).saturating_add(128)
}

fn check_removal_state_exists(rows: &BTreeMap<String, (i64, String)>, key: &str) -> Result<()> {
    let id = key
        .strip_prefix("removal-intent/")
        .ok_or_else(|| Error::refusal("corrupt_metadata", "invalid removal intent key"))?;
    validate_removal_id(id)?;
    if !rows.contains_key(&format!("removal-state/{id}")) {
        return Err(Error::refusal(
            "corrupt_metadata",
            "removal intent has no reserved progress state",
        ));
    }
    Ok(())
}

fn validate_removal_id(id: &str) -> Result<()> {
    Id::new(id)
        .map(|_| ())
        .map_err(|_| Error::refusal("corrupt_metadata", "invalid removal record identifier"))
}

fn removal_reservation(
    rows: &BTreeMap<String, (i64, String)>,
    key: &str,
    revision: i64,
    json: &str,
) -> Result<usize> {
    let id = key
        .strip_prefix("removal-state/")
        .ok_or_else(|| Error::refusal("corrupt_metadata", "invalid removal state key"))?;
    validate_removal_id(id)?;
    let state: RemovalProgress = serde_json::from_str(json)
        .map_err(|_| Error::refusal("corrupt_metadata", "invalid removal history reservation"))?;
    let (intent_revision, intent_json) =
        rows.get(&format!("removal-intent/{id}")).ok_or_else(|| {
            Error::refusal("corrupt_metadata", "removal state has no original intent")
        })?;
    let intent: RemovalIntent = serde_json::from_str(intent_json)
        .map_err(|_| Error::refusal("corrupt_metadata", "invalid removal intent"))?;
    if state.intent_revision <= 0
        || state.intent_revision > revision
        || *intent_revision != state.intent_revision
        || intent.revision != *intent_revision
        || intent.plan.removal.as_str() != id
    {
        return Err(Error::refusal(
            "corrupt_metadata",
            "removal state does not reference its exact original intent",
        ));
    }
    crate::engine::retention::removal::validate_progress(&intent, &state).map_err(|_| {
        Error::refusal(
            "corrupt_metadata",
            "invalid removal outcomes or reserved completion capacity",
        )
    })?;
    usize::try_from(state.reserved_future_bytes).map_err(|_| reservation_overflow())
}

fn reservation_overflow() -> Error {
    Error::refusal("metadata_capacity", "removal history reservation overflow")
}

fn check_history_reservation(history: usize, reserved: usize, limit: usize) -> Result<()> {
    let total = history
        .checked_add(reserved)
        .ok_or_else(reservation_overflow)?;
    check_history_size(total, limit)
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
pub(crate) struct GuardedCatalog<'a> {
    inner: &'a dyn Catalog,
    expected: Table,
}

impl<'a> GuardedCatalog<'a> {
    pub(crate) fn new(inner: &'a dyn Catalog, expected: &Table) -> Self {
        Self {
            inner,
            expected: expected.clone(),
        }
    }
}

#[async_trait]
impl Catalog for GuardedCatalog<'_> {
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let current = self.inner.load_table(ident).await?;
        if ident != self.expected.identifier()
            || current.metadata_location() != self.expected.metadata_location()
            || current.metadata() != self.expected.metadata()
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
    use crate::{
        budget::Limits,
        model::{ObjectProtection, ObjectRef, RemovalOutcome, RemovalPlan, RetentionInspection},
    };

    fn test_budget(max_metadata_bytes: usize) -> Budget {
        Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 32,
            max_batch_bytes: 4096,
            max_metadata_bytes,
        })
        .unwrap()
    }

    fn removal_intent(id: &str) -> RemovalIntent {
        RemovalIntent {
            plan: RemovalPlan {
                scope: crate::model::RemovalScope::Objects,
                dataset: Id::new("owner").unwrap(),
                removal: Id::new(id).unwrap(),
                reason: "catalog reservation test".into(),
                table_uuid: "fixture".into(),
                inspection: RetentionInspection {
                    boundaries: vec![],
                    objects: vec![ObjectProtection {
                        object: ObjectRef {
                            owner: Id::new("owner").unwrap(),
                            path: format!("/fixture/{id}"),
                            digest: "0".repeat(64),
                            bytes: 1,
                        },
                        blocker: None,
                    }],
                    protected_bytes: 0,
                    unmatched_bytes: 1,
                    metadata_objects_read: 0,
                },
                registrations: vec![],
            },
            revision: 1,
            boundaries: vec![],
            blocked: vec![],
            objects: vec![0],
            registrations: vec![],
        }
    }

    fn removal_state(intent_revision: i64, reserved: u64) -> RemovalProgress {
        RemovalProgress {
            intent_revision,
            reserved_future_bytes: reserved,
            registrations: vec![],
            objects: vec![if reserved == 0 {
                RemovalOutcome::Removed
            } else {
                RemovalOutcome::Unresolved
            }],
        }
    }

    #[tokio::test]
    async fn cold_and_warm_reservations_count_only_current_states() {
        let root = tempfile::tempdir().unwrap();
        let catalog = storage::local_catalog(root.path()).await.unwrap();
        let budget = test_budget(1 << 20);
        let lifecycle = Lifecycle::new(catalog.clone(), budget.clone());
        let view = lifecycle.open(&Id::new("owner").unwrap()).await.unwrap();
        let view = lifecycle
            .commit(
                &view,
                vec![
                    record("removal-intent/a", removal_intent("a")).unwrap(),
                    record("removal-state/a", removal_state(1, 1000)).unwrap(),
                    record("removal-intent/b", removal_intent("b")).unwrap(),
                    record("removal-state/b", removal_state(1, 2000)).unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            lifecycle.rows(&view).await.unwrap().reserved_future_bytes,
            3000
        );
        let view = lifecycle
            .commit(
                &view,
                vec![record("removal-state/a", removal_state(1, 0)).unwrap()],
            )
            .await
            .unwrap();
        let warm = lifecycle.rows(&view).await.unwrap();
        let reopened = Lifecycle::new(catalog, budget);
        let cold = reopened.rows(&view).await.unwrap();
        assert_eq!(warm.reserved_future_bytes, 2000);
        assert_eq!(cold.reserved_future_bytes, warm.reserved_future_bytes);
        assert_eq!(cold.history_bytes, warm.history_bytes);
        let view = reopened
            .commit(&view, vec![record("ordinary", true).unwrap()])
            .await
            .unwrap();
        assert_eq!(
            reopened.rows(&view).await.unwrap().reserved_future_bytes,
            2000
        );
    }

    #[tokio::test]
    async fn ordinary_commit_preserves_exact_terminal_headroom() {
        let root = tempfile::tempdir().unwrap();
        let catalog = storage::local_catalog(root.path()).await.unwrap();
        // Historical generations fill replay capacity while the current cache
        // stays small and the bounded native writer keeps its working room.
        let earlier_commits = 8;
        let mut intent = removal_intent("a");
        intent.revision = earlier_commits + 1;
        let state_key = "removal-state/a";
        let terminal = removal_state(intent.revision, 0);
        let terminal_charge =
            history_row_bytes(state_key, &serde_json::to_string(&terminal).unwrap());
        let floor = crate::engine::retention::removal::validate_progress(
            &intent,
            &removal_state(intent.revision, u64::MAX),
        )
        .unwrap();
        let pending = removal_state(intent.revision, floor as u64);
        let previous = "x".repeat(2048);
        let history = earlier_commits as usize
            * history_row_bytes("prior-status", &serde_json::to_string(&previous).unwrap())
            + history_row_bytes("removal-intent/a", &serde_json::to_string(&intent).unwrap())
            + history_row_bytes(state_key, &serde_json::to_string(&pending).unwrap());
        let lifecycle = Lifecycle::new(catalog.clone(), test_budget(history + floor));
        let mut view = lifecycle.open(&Id::new("owner").unwrap()).await.unwrap();
        for _ in 0..earlier_commits {
            view = lifecycle
                .commit(&view, vec![record("prior-status", &previous).unwrap()])
                .await
                .unwrap();
        }
        let view = lifecycle
            .commit(
                &view,
                vec![
                    record("removal-intent/a", intent).unwrap(),
                    record(state_key, pending).unwrap(),
                ],
            )
            .await
            .unwrap();
        // Reopen to prove the guarantee survives loss of the warm cache.
        let lifecycle = Lifecycle::new(catalog, test_budget(history + floor));
        let refused = lifecycle
            .commit(&view, vec![record("ordinary", true).unwrap()])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), "metadata_capacity");
        let view = lifecycle
            .commit(&view, vec![record(state_key, terminal).unwrap()])
            .await
            .unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        assert_eq!(rows.history_bytes, history + terminal_charge);
        assert_eq!(rows.reserved_future_bytes, 0);
    }

    #[tokio::test]
    async fn invalid_removal_references_refuse_before_native_commit() {
        let root = tempfile::tempdir().unwrap();
        let catalog = storage::local_catalog(root.path()).await.unwrap();
        let lifecycle = Lifecycle::new(catalog.clone(), test_budget(1 << 20));
        let owner = Id::new("owner").unwrap();
        let view = lifecycle.open(&owner).await.unwrap();
        let view = lifecycle
            .commit(
                &view,
                vec![
                    record("removal-intent/a", removal_intent("a")).unwrap(),
                    record("removal-state/a", removal_state(1, 1000)).unwrap(),
                ],
            )
            .await
            .unwrap();
        let invalid = [
            ("removal-state/a", serde_json::json!({"intent_revision": 1})),
            (
                "removal-state/a",
                serde_json::json!({"reserved_future_bytes": 1}),
            ),
            (
                "removal-state/a",
                serde_json::json!({"intent_revision": 1, "reserved_future_bytes": -1}),
            ),
            (
                "removal-state/a",
                serde_json::to_value(removal_state(0, 1)).unwrap(),
            ),
            (
                "removal-state/a",
                serde_json::to_value(removal_state(2, 1)).unwrap(),
            ),
            (
                "removal-state/missing",
                serde_json::to_value(removal_state(1, 1)).unwrap(),
            ),
            (
                "removal-state/bad/id",
                serde_json::to_value(removal_state(1, 1)).unwrap(),
            ),
            ("removal-intent/a", serde_json::json!("changed")),
            (
                "removal-intent/missing",
                serde_json::to_value(removal_intent("missing")).unwrap(),
            ),
            (
                "removal-state/a",
                serde_json::to_value(RemovalProgress {
                    intent_revision: 1,
                    reserved_future_bytes: 0,
                    registrations: vec![],
                    objects: vec![RemovalOutcome::Unresolved],
                })
                .unwrap(),
            ),
            (
                "removal-state/a",
                serde_json::to_value(RemovalProgress {
                    intent_revision: 1,
                    reserved_future_bytes: 1000,
                    registrations: vec![],
                    objects: vec![],
                })
                .unwrap(),
            ),
            (
                "removal-state/a",
                serde_json::to_value(RemovalProgress {
                    intent_revision: 1,
                    reserved_future_bytes: 1000,
                    registrations: vec![],
                    objects: vec![RemovalOutcome::Failed {
                        code: "invalid-code".into(),
                    }],
                })
                .unwrap(),
            ),
            ("removal-intent/bad/id", serde_json::json!("bad")),
        ];
        for (key, value) in invalid {
            let error = lifecycle
                .commit(&view, vec![(key.into(), value)])
                .await
                .unwrap_err();
            assert_eq!(error.code(), "corrupt_metadata", "{key}");
            assert_eq!(
                lifecycle.open(&owner).await.unwrap().revision,
                view.revision
            );
        }
        let error = lifecycle
            .commit(
                &view,
                vec![record("removal-state/a", removal_state(1, u64::MAX)).unwrap()],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "metadata_capacity");
        assert_eq!(
            lifecycle.open(&owner).await.unwrap().revision,
            view.revision
        );
    }

    #[test]
    fn cold_reservation_validation_rejects_malformed_and_dangling_states() {
        let key = "removal-state/a";
        let mut rows = BTreeMap::new();
        rows.insert(
            "removal-intent/a".into(),
            (1, serde_json::to_string(&removal_intent("a")).unwrap()),
        );
        let valid = serde_json::to_string(&removal_state(1, 1000)).unwrap();
        assert_eq!(removal_reservation(&rows, key, 2, &valid).unwrap(), 1000);
        for json in [
            "{}",
            "null",
            "{",
            "{\"intent_revision\":1,\"reserved_future_bytes\":\"42\"}",
        ] {
            assert_eq!(
                removal_reservation(&rows, key, 2, json).unwrap_err().code(),
                "corrupt_metadata"
            );
        }
        assert!(removal_reservation(&rows, key, 0, &valid).is_err());
        assert!(removal_reservation(&rows, "removal-state/../a", 2, &valid).is_err());
        rows.insert("removal-intent/a".into(), (2, "{}".into()));
        assert!(removal_reservation(&rows, key, 2, &valid).is_err());
        rows.clear();
        rows.insert(
            "removal-intent/a".into(),
            (1, serde_json::to_string(&removal_intent("a")).unwrap()),
        );
        assert!(check_removal_state_exists(&rows, "removal-intent/a").is_err());
        rows.clear();
        assert!(removal_reservation(&rows, key, 2, &valid).is_err());
        assert_eq!(
            check_history_reservation(1, usize::MAX, usize::MAX)
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
    }

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
