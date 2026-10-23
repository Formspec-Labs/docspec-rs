//! Native Iceberg lifecycle rows with stale-decision refusal at the catalog boundary.
//!
//! The initial layout appends bounded key/value changes. It is deliberately small;
//! metadata-growth qualification must precede adopting it for large histories.
// Rust guideline compliant 2026-02-21

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
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    spec::{DataFile, MAIN_BRANCH},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Summary of a durable lifecycle baseline compaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleCompactionResult {
    /// Active native lifecycle snapshots before compaction.
    pub revisions_before: usize,
    /// Active native lifecycle snapshots after compaction.
    pub revisions_after: usize,
    /// Physical lifecycle rows across active manifests before compaction.
    pub rows_before: usize,
    /// Deduplicated lifecycle rows retained in the compacted baseline.
    pub rows_after: usize,
    /// Replayed lifecycle history byte charge before compaction.
    pub metadata_bytes_before: usize,
    /// Replayed lifecycle history byte charge after compaction.
    pub metadata_bytes_after: usize,
    /// Active lifecycle Parquet data files before compaction.
    pub files_before: usize,
    /// Active lifecycle Parquet data files after compaction.
    pub files_after: usize,
    /// Native catalog sequence number of the compacted lifecycle view.
    pub current_revision: i64,
}

/// Lifecycle authority injected with a maintained catalog and shared budget.
#[derive(Clone, Debug)]
pub struct Lifecycle {
    retained_prefix: Option<String>,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) budget: Budget,
    cache: Arc<Mutex<LifecycleCache>>,
    #[cfg(all(not(test), not(feature = "test-util")))]
    view_cache: Arc<Mutex<HashMap<TableIdent, View>>>,
    #[cfg(all(not(test), not(feature = "test-util")))]
    deferred_commit: Arc<Mutex<HashMap<TableIdent, DeferredCommit>>>,
    known_tables: Arc<Mutex<HashSet<TableIdent>>>,
    #[cfg(test)]
    pub(crate) lose_commit_response: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Default)]
struct LifecycleCache {
    entries: BTreeMap<String, Arc<SnapshotRows>>,
    permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

#[cfg(all(not(test), not(feature = "test-util")))]
#[derive(Debug)]
struct DeferredCommit {
    base_view: View,
    base_rows: Arc<SnapshotRows>,
    changes: BTreeMap<String, serde_json::Value>,
    rows: Arc<SnapshotRows>,
}

#[derive(Debug)]
pub(crate) struct SnapshotRows {
    pub(crate) metadata: String,
    pub values: BTreeMap<String, (i64, Arc<str>)>,
    cache_bytes: usize,
    history_rows: usize,
    history_bytes: usize,
    reserved_future_bytes: usize,
    _reservation: Mutex<Arc<tokio::sync::OwnedSemaphorePermit>>,
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
            .map(|(key, (revision, value))| (key.as_str(), *revision, value.as_ref()))
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
    pub(crate) logical_identifier: TableIdent,
    pub revision: i64,
}

impl View {
    /// Returns the stable DocSpec identity, independent of physical catalog routing.
    pub(crate) fn identifier(&self) -> &TableIdent {
        &self.logical_identifier
    }
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
            retained_prefix: None,
            catalog,
            budget,
            cache: Arc::new(Mutex::new(LifecycleCache::default())),
            #[cfg(all(not(test), not(feature = "test-util")))]
            view_cache: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(all(not(test), not(feature = "test-util")))]
            deferred_commit: Arc::new(Mutex::new(HashMap::new())),
            known_tables: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(test)]
            lose_commit_response: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn with_retained_prefix(mut self, prefix: Option<String>) -> Self {
        self.retained_prefix = prefix;
        self
    }

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(crate) fn cached_view(&self, dataset: &Id) -> Option<View> {
        let ident = lifecycle_ident(dataset);
        if let Some(view) = self
            .deferred_commit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ident)
            .map(|d| d.base_view.clone())
        {
            return Some(view);
        }
        self.view_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ident)
            .cloned()
    }

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(crate) fn set_cached_view(&self, view: View) {
        let mut guard = self.view_cache.lock().unwrap_or_else(|e| e.into_inner());
        if guard.len() >= 256 {
            guard.clear();
        }
        guard.insert(view.logical_identifier.clone(), view);
    }

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(crate) async fn begin_deferred(&self, dataset: &Id) -> Result<()> {
        let view = self.open(dataset).await?;
        let rows = self.rows(&view).await?;
        self.deferred_commit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                view.logical_identifier.clone(),
                DeferredCommit {
                    base_view: view,
                    base_rows: rows.clone(),
                    changes: BTreeMap::new(),
                    rows,
                },
            );
        Ok(())
    }

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(crate) async fn commit_flushing_deferred(
        &self,
        view: &View,
        final_changes: Vec<(String, serde_json::Value)>,
    ) -> Result<View> {
        let deferred = self
            .deferred_commit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&view.logical_identifier);
        if let Some(mut deferred) = deferred {
            if final_changes.is_empty() {
                return Err(Error::refusal(
                    "empty_commit",
                    "a lifecycle commit must change state",
                ));
            }
            let unique: BTreeSet<_> = final_changes.iter().map(|(k, _)| k).collect();
            if unique.len() != final_changes.len() {
                return Err(Error::refusal(
                    "duplicate_key",
                    "each lifecycle key may change only once per commit",
                ));
            }
            for (k, v) in final_changes {
                deferred.changes.insert(k, v);
            }
            {
                self.cache.lock().map_err(cache_poisoned)?.entries.insert(
                    deferred.base_view.logical_identifier.to_string(),
                    deferred.base_rows,
                );
            }
            let merged: Vec<_> = deferred.changes.into_iter().collect();
            return self.commit(&deferred.base_view, merged).await;
        }
        self.commit(view, final_changes).await
    }

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(crate) async fn flush_deferred(&self, dataset: &Id) -> Result<()> {
        let ident = lifecycle_ident(dataset);
        let deferred = self
            .deferred_commit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&ident);
        if let Some(deferred) = deferred {
            {
                self.cache.lock().map_err(cache_poisoned)?.entries.insert(
                    deferred.base_view.logical_identifier.to_string(),
                    deferred.base_rows,
                );
            }
            if !deferred.changes.is_empty() {
                let merged: Vec<_> = deferred.changes.into_iter().collect();
                self.commit(&deferred.base_view, merged).await?;
            }
        }
        Ok(())
    }

    /// Opens or creates a dataset's lifecycle authority.
    ///
    /// # Errors
    /// Returns catalog or schema errors; callers serialize first-time creation.
    pub async fn open(&self, dataset: &Id) -> Result<View> {
        let ident = lifecycle_ident(dataset);
        #[cfg(all(not(test), not(feature = "test-util")))]
        if let Some(view) = self
            .deferred_commit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ident)
            .map(|d| d.base_view.clone())
        {
            return Ok(view);
        }
        #[cfg(all(not(test), not(feature = "test-util")))]
        if let Some(cached) = self
            .view_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ident)
            .cloned()
        {
            return Ok(cached);
        }
        let is_known = self
            .known_tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&ident);
        if is_known && let Ok(table) = self.catalog.load_table(&ident).await {
            let view = View {
                revision: table.metadata().last_sequence_number(),
                table,
                logical_identifier: ident,
            };
            #[cfg(all(not(test), not(feature = "test-util")))]
            {
                self.set_cached_view(view.clone());
            }
            return Ok(view);
        }
        let namespace = ident.namespace().clone();
        let namespace_existed = self.catalog.namespace_exists(&namespace).await?;
        if !namespace_existed {
            self.catalog
                .create_namespace(&namespace, HashMap::new())
                .await?;
        }
        let table = if namespace_existed && self.catalog.table_exists(&ident).await? {
            self.catalog.load_table(&ident).await?
        } else {
            let schema = Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("revision", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ]);
            let mut attempt = 0u32;
            loop {
                match self
                    .catalog
                    .create_table(
                        &namespace,
                        TableCreation::builder()
                            .name("lifecycle".into())
                            .location_opt(self.retained_prefix.as_ref().map(|prefix| {
                                format!(
                                    "{prefix}/{dataset}/{}/lifecycle",
                                    uuid::Uuid::new_v4().simple()
                                )
                            }))
                            .schema(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
                                &schema,
                            )?)
                            .properties(storage::table_properties())
                            .build(),
                    )
                    .await
                {
                    Ok(table) => break table,
                    Err(err) if err.kind() == iceberg::ErrorKind::TableAlreadyExists => {
                        break self.catalog.load_table(&ident).await?;
                    }
                    Err(err)
                        if attempt < 8
                            && ({
                                let msg = err.to_string().to_ascii_lowercase();
                                msg.contains("429")
                                    || msg.contains("toomanyrequestsexception")
                                    || msg.contains("rate limit")
                                    || msg.contains("502")
                                    || msg.contains("503")
                                    || msg.contains("504")
                            }) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_secs(
                            10 * u64::from(attempt + 1),
                        ))
                        .await;
                        attempt += 1;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        };
        self.known_tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(ident.clone());
        let view = View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: ident,
        };
        #[cfg(all(not(test), not(feature = "test-util")))]
        {
            self.set_cached_view(view.clone());
        }
        Ok(view)
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
    pub(crate) fn rows<'a>(
        &'a self,
        view: &'a View,
    ) -> futures::future::BoxFuture<'a, Result<Arc<SnapshotRows>>> {
        self.rows_inner(view, true)
    }

    /// Loads current records for baseline compaction, bounding memory by deduplicated live state.
    pub(crate) fn rows_for_compaction<'a>(
        &'a self,
        view: &'a View,
    ) -> futures::future::BoxFuture<'a, Result<Arc<SnapshotRows>>> {
        self.rows_inner(view, false)
    }

    fn prepare_cache_reservation(
        &self,
        cache_key: &str,
        extra_holds: usize,
    ) -> Result<(Arc<tokio::sync::OwnedSemaphorePermit>, bool)> {
        let existing_permit = {
            let guard = self.cache.lock().map_err(cache_poisoned)?;
            let old_retained = guard
                .entries
                .get(cache_key)
                .is_some_and(|old| Arc::strong_count(old) > 1 + extra_holds);
            if old_retained {
                None
            } else {
                guard.permit.clone()
            }
        };
        if let Some(permit) = existing_permit {
            return Ok((permit, false));
        }
        let permit = Arc::new(self.budget.reserve(self.budget.limits.max_metadata_bytes)?);
        Ok((permit, true))
    }

    fn publish_cache_entry(
        &self,
        cache_key: String,
        entry: Arc<SnapshotRows>,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        rotated: bool,
    ) -> Result<()> {
        let mut guard = self.cache.lock().map_err(cache_poisoned)?;
        if rotated || guard.permit.is_none() {
            for (k, existing) in &guard.entries {
                if k != &cache_key {
                    *existing
                        ._reservation
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = permit.clone();
                }
            }
            guard.permit = Some(permit);
        }
        guard.entries.insert(cache_key, entry);
        Ok(())
    }

    fn rows_inner<'a>(
        &'a self,
        view: &'a View,
        enforce_history_budget: bool,
    ) -> futures::future::BoxFuture<'a, Result<Arc<SnapshotRows>>> {
        Box::pin(async move {
            #[cfg(all(not(test), not(feature = "test-util")))]
            if let Some(rows) = self
                .deferred_commit
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&view.logical_identifier)
                .map(|d| d.rows.clone())
            {
                return Ok(rows);
            }
            let cache_key = view.logical_identifier.to_string();
            let metadata = view.table.metadata_location_result()?.to_owned();
            if let Some(hit) = self
                .cache
                .lock()
                .map_err(cache_poisoned)?
                .entries
                .get(&cache_key)
                .filter(|hit| hit.metadata == metadata)
                .cloned()
            {
                return Ok(hit);
            }
            let (reservation, rotated) = self.prepare_cache_reservation(&cache_key, 0)?;
            let mut current = BTreeMap::<String, (i64, Arc<str>)>::new();
            let mut cache_bytes = 0_usize;
            if view.table.metadata().current_snapshot().is_none() {
                let entry = Arc::new(SnapshotRows {
                    metadata,
                    values: current,
                    cache_bytes,
                    history_rows: 0,
                    history_bytes: 0,
                    reserved_future_bytes: 0,
                    _reservation: Mutex::new(reservation.clone()),
                });
                self.publish_cache_entry(cache_key, entry.clone(), reservation, rotated)?;
                return Ok(entry);
            }
            let batches = storage::read_batches(&view.table, &self.budget).await?;
            futures::pin_mut!(batches);
            let mut bytes = 0_usize;
            let mut history_rows = 0_usize;
            while let Some(batch) = batches.try_next().await? {
                let keys = strings(&batch, 0)?;
                let values = strings(&batch, 2)?;
                let revisions = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| Error::refusal("corrupt_metadata", "invalid revision column"))?;
                for i in 0..batch.num_rows() {
                    let key = keys.value(i);
                    let value = values.value(i);
                    let row_bytes = history_row_bytes(key, value);
                    history_rows = history_rows.saturating_add(1);
                    bytes = bytes.saturating_add(row_bytes);
                    if enforce_history_budget {
                        check_history_size(bytes, self.budget.limits.max_metadata_bytes)?;
                    }
                    let revision = revisions.value(i);
                    if current.get(key).is_none_or(|(old, _)| *old < revision) {
                        if let Some((_, old_value)) =
                            current.insert(key.to_owned(), (revision, Arc::from(value)))
                        {
                            cache_bytes =
                                cache_bytes.saturating_sub(history_row_bytes(key, &old_value));
                        }
                        cache_bytes = cache_bytes.saturating_add(row_bytes);
                        check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
                    }
                }
            }
            let reserved_future_bytes = validate_and_sum_removal_reservations(&current)?;
            let within_history_budget = check_history_reservation(
                bytes,
                reserved_future_bytes,
                self.budget.limits.max_metadata_bytes,
            )
            .is_ok();
            if enforce_history_budget && !within_history_budget {
                check_history_reservation(
                    bytes,
                    reserved_future_bytes,
                    self.budget.limits.max_metadata_bytes,
                )?;
            }
            check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
            let entry = Arc::new(SnapshotRows {
                metadata,
                values: current,
                cache_bytes,
                history_rows,
                history_bytes: bytes,
                reserved_future_bytes,
                _reservation: Mutex::new(reservation.clone()),
            });
            if within_history_budget {
                self.publish_cache_entry(cache_key, entry.clone(), reservation, rotated)?;
            }
            Ok(entry)
        })
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
        let unique: BTreeSet<_> = changes.iter().map(|(key, _)| key).collect();
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
        #[cfg(any(test, feature = "test-util"))]
        {
            let guard = GuardedCatalog::new(self.catalog.as_ref(), &view.table);
            guard.load_table(view.table.identifier()).await?;
        }
        // Prepare a bounded cache of the next immutable state. It becomes visible
        // only after the guarded native commit succeeds; failed commits cannot leak it.
        let cache_key = view.logical_identifier.to_string();
        let previous = self.rows(view).await?;
        let (reservation, rotated) = self.prepare_cache_reservation(&cache_key, 1)?;
        let history_rows = previous.history_rows.saturating_add(changes.len());
        let history_bytes = changes
            .iter()
            .zip(&values)
            .fold(previous.history_bytes, |bytes, ((key, _), value)| {
                bytes.saturating_add(history_row_bytes(key, value))
            });
        // Warm and cold paths apply the same bound before acknowledging a commit.
        // Obsolete appended versions still count until compaction removes them.
        let mut next_values = previous.values.clone();
        let mut cache_bytes = previous.cache_bytes;
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
            let row_bytes = history_row_bytes(key, &value);
            if let Some((_, old_value)) =
                next_values.insert(key.clone(), (revision, Arc::from(value)))
            {
                cache_bytes = cache_bytes.saturating_sub(history_row_bytes(key, &old_value));
            }
            cache_bytes = cache_bytes.saturating_add(row_bytes);
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
        check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
        #[cfg(all(not(test), not(feature = "test-util")))]
        {
            let mut guard = self
                .deferred_commit
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(deferred) = guard.get_mut(&view.logical_identifier) {
                for (k, v) in changes {
                    deferred.changes.insert(k, v);
                }
                let meta = deferred
                    .base_view
                    .table
                    .metadata_location_result()?
                    .to_owned();
                let entry = Arc::new(SnapshotRows {
                    metadata: meta,
                    values: next_values,
                    cache_bytes,
                    history_rows,
                    history_bytes,
                    reserved_future_bytes,
                    _reservation: Mutex::new(reservation.clone()),
                });
                deferred.rows = entry.clone();
                self.publish_cache_entry(cache_key, entry, reservation, rotated)?;
                return Ok(deferred.base_view.clone());
            }
        }
        let files = storage::write_catalog_batches(
            &view.table,
            futures::stream::iter(vec![Ok(batch)]),
            &self.budget,
        )
        .await?;
        let table = match storage::append(&view.table, self.catalog.as_ref(), files).await {
            Ok(table) => table,
            Err(error) => {
                #[cfg(all(not(test), not(feature = "test-util")))]
                {
                    self.view_cache
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&view.logical_identifier);
                }
                return Err(error);
            }
        };
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
        let meta = table.metadata_location_result()?.to_owned();
        let entry = Arc::new(SnapshotRows {
            metadata: meta,
            values: next_values,
            cache_bytes,
            history_rows,
            history_bytes,
            reserved_future_bytes,
            _reservation: Mutex::new(reservation.clone()),
        });
        self.publish_cache_entry(cache_key, entry, reservation, rotated)?;
        let next_view = View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: view.logical_identifier.clone(),
        };
        #[cfg(all(not(test), not(feature = "test-util")))]
        {
            self.set_cached_view(next_view.clone());
        }
        Ok(next_view)
    }

    /// Rewrites the lifecycle table to one baseline snapshot of live, unpruned records.
    ///
    /// # Errors
    /// Refuses stale views, empty compacted baselines, invalid removal reservations,
    /// oversized live state, or catalog commit failures.
    #[cfg(test)]
    pub(crate) async fn compact_baseline(
        &self,
        view: &View,
        remove_keys: &BTreeSet<String>,
    ) -> Result<(View, LifecycleCompactionResult)> {
        self.compact_baseline_with_records(view, remove_keys, &[])
            .await
    }

    pub(crate) async fn compact_baseline_with_records(
        &self,
        view: &View,
        remove_keys: &BTreeSet<String>,
        upsert_records: &[(String, serde_json::Value)],
    ) -> Result<(View, LifecycleCompactionResult)> {
        #[cfg(any(test, feature = "test-util"))]
        {
            let guard = GuardedCatalog::new(self.catalog.as_ref(), &view.table);
            guard.load_table(view.table.identifier()).await?;
        }
        if view.table.metadata().current_snapshot().is_none() {
            return Ok((
                view.clone(),
                LifecycleCompactionResult {
                    revisions_before: 0,
                    revisions_after: 0,
                    rows_before: 0,
                    rows_after: 0,
                    metadata_bytes_before: 0,
                    metadata_bytes_after: 0,
                    files_before: 0,
                    files_after: 0,
                    current_revision: view.revision,
                },
            ));
        }
        let next_revision = view
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::refusal("revision_exhausted", "revision overflow"))?;
        let revisions_before = view.table.metadata().snapshots().count();
        let files_before = snapshot_data_file_count(&view.table);
        let previous = self.rows_for_compaction(view).await?;
        let rows_before = previous.history_rows;
        let metadata_bytes_before = previous.history_bytes;
        let mut next_values = BTreeMap::<String, (i64, Arc<str>)>::new();
        let mut cache_bytes = 0_usize;
        for (key, (rev, value)) in &previous.values {
            if remove_keys.contains(key) {
                continue;
            }
            cache_bytes = cache_bytes.saturating_add(history_row_bytes(key, value));
            next_values.insert(key.clone(), (*rev, value.clone()));
        }
        drop(previous);
        for (key, value) in upsert_records {
            let encoded = serde_json::to_string(value)?;
            let row_bytes = history_row_bytes(key, &encoded);
            if let Some((_, old_value)) =
                next_values.insert(key.clone(), (next_revision, Arc::from(encoded)))
            {
                cache_bytes = cache_bytes.saturating_sub(history_row_bytes(key, &old_value));
            }
            cache_bytes = cache_bytes.saturating_add(row_bytes);
        }
        if next_values.is_empty() {
            return Err(Error::refusal(
                "empty_baseline",
                "lifecycle baseline compaction must retain at least one record",
            ));
        }
        let reserved_future_bytes = validate_and_sum_removal_reservations(&next_values)?;
        let history_rows = next_values.len();
        let history_bytes = cache_bytes;
        check_history_reservation(
            history_bytes,
            reserved_future_bytes,
            self.budget.limits.max_metadata_bytes,
        )?;
        check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
        let cache_key = view.logical_identifier.to_string();
        let (reservation, rotated) = self.prepare_cache_reservation(&cache_key, 0)?;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            view.table.metadata().current_schema(),
        )?);
        let batches = build_baseline_batches(
            &schema,
            &next_values,
            self.budget.limits.batch_rows,
            self.budget.limits.max_batch_bytes,
        )?;
        let files = storage::write_catalog_batches(
            &view.table,
            futures::stream::iter(batches),
            &self.budget,
        )
        .await?;
        let files_after = files.len();
        let table = match replace_baseline_snapshot(&view.table, self.catalog.as_ref(), files).await
        {
            Ok(table) => table,
            Err(error) => {
                #[cfg(all(not(test), not(feature = "test-util")))]
                {
                    self.view_cache
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&view.logical_identifier);
                }
                return Err(error);
            }
        };
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
        let revisions_after = table.metadata().snapshots().count();
        let current_revision = table.metadata().last_sequence_number();
        let meta = table.metadata_location_result()?.to_owned();
        let entry = Arc::new(SnapshotRows {
            metadata: meta,
            values: next_values,
            cache_bytes,
            history_rows,
            history_bytes,
            reserved_future_bytes,
            _reservation: Mutex::new(reservation.clone()),
        });
        self.publish_cache_entry(cache_key, entry, reservation, rotated)?;
        self.reclaim_expired_baseline_files(&table).await?;
        let next_view = View {
            revision: current_revision,
            table,
            logical_identifier: view.logical_identifier.clone(),
        };
        #[cfg(all(not(test), not(feature = "test-util")))]
        {
            self.set_cached_view(next_view.clone());
        }
        Ok((
            next_view,
            LifecycleCompactionResult {
                revisions_before,
                revisions_after,
                rows_before,
                rows_after: history_rows,
                metadata_bytes_before,
                metadata_bytes_after: history_bytes,
                files_before,
                files_after,
                current_revision,
            },
        ))
    }

    async fn reclaim_expired_baseline_files(&self, table: &Table) -> Result<()> {
        if self.retained_prefix.is_some() {
            return Ok(());
        }
        let live = storage::current_snapshot_files(table, &self.budget)
            .await?
            .paths;
        let location = PathBuf::from(table.metadata().location());
        self.budget
            .blocking(move || {
                for subdir in ["data", "metadata"] {
                    let dir = location.join(subdir);
                    let entries = match std::fs::read_dir(&dir) {
                        Ok(entries) => entries,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(err) => return Err(err.into()),
                    };
                    for entry in entries {
                        let entry = entry?;
                        let path = entry.path();
                        let meta = match std::fs::symlink_metadata(&path) {
                            Ok(meta) => meta,
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                            Err(err) => return Err(err.into()),
                        };
                        if !meta.file_type().is_file() {
                            continue;
                        }
                        if let Some(path_str) = path.to_str()
                            && !live.contains(path_str)
                        {
                            match std::fs::remove_file(&path) {
                                Ok(()) => {}
                                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                                Err(err) => return Err(err.into()),
                            }
                        }
                    }
                }
                Ok(())
            })
            .await
    }
}

fn snapshot_data_file_count(table: &Table) -> usize {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return 0;
    };
    snapshot
        .summary()
        .additional_properties
        .get("total-data-files")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| table.metadata().snapshots().count())
}

fn build_baseline_batches(
    schema: &Arc<Schema>,
    values: &BTreeMap<String, (i64, Arc<str>)>,
    batch_rows: usize,
    max_batch_bytes: usize,
) -> Result<Vec<Result<RecordBatch>>> {
    let max_rows = batch_rows.max(1);
    let mut batches = Vec::new();
    let mut keys = Vec::new();
    let mut revs = Vec::new();
    let mut vals = Vec::new();
    let mut current_bytes = 256_usize;
    for (key, (rev, value)) in values {
        let row_bytes = key.len().saturating_add(value.len()).saturating_add(64);
        if !keys.is_empty()
            && (keys.len() >= max_rows || current_bytes.saturating_add(row_bytes) > max_batch_bytes)
        {
            batches.push(Ok(RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(std::mem::take(&mut keys))),
                    Arc::new(Int64Array::from(std::mem::take(&mut revs))),
                    Arc::new(StringArray::from(std::mem::take(&mut vals))),
                ],
            )?));
            current_bytes = 256;
        }
        keys.push(key.as_str());
        revs.push(*rev);
        vals.push(value.as_ref());
        current_bytes = current_bytes.saturating_add(row_bytes);
    }
    if !keys.is_empty() {
        batches.push(Ok(RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(revs)),
                Arc::new(StringArray::from(vals)),
            ],
        )?));
    }
    Ok(batches)
}

async fn replace_baseline_snapshot(
    table: &Table,
    catalog: &dyn Catalog,
    files: Vec<DataFile>,
) -> Result<Table> {
    let old_snapshot_ids: Vec<i64> = table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    // Detach `MAIN_BRANCH` on the in-memory transaction base so `FastAppend` writes a standalone
    // baseline manifest list rather than inheriting prior delta manifests, and temporarily enable
    // `gc.enabled` so `expire_snapshots` can prune superseded snapshot entries before resetting
    // `gc.enabled` back to `"false"` within the same atomic catalog commit.
    let detached_metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_owned))
        .remove_ref(MAIN_BRANCH)
        .set_properties(HashMap::from([("gc.enabled".into(), "true".into())]))?
        .build()
        .map_err(|err| Error::refusal("corrupt_metadata", err.to_string()))?
        .metadata;
    let mut builder = Table::builder()
        .file_io(table.file_io().clone())
        .metadata(detached_metadata)
        .identifier(table.identifier().clone())
        .runtime(iceberg::Runtime::try_current()?);
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location);
    }
    let detached = builder.build()?;
    let guard = GuardedCatalog::with_detached(catalog, table, detached.clone());
    let tx = Transaction::new(&detached);
    let tx = tx
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(files)
        .apply(tx)?;
    let tx = if old_snapshot_ids.is_empty() {
        tx
    } else {
        tx.expire_snapshots()
            .expire_snapshot_ids(old_snapshot_ids)
            .apply(tx)?
    };
    let tx = tx
        .update_table_properties()
        .set("gc.enabled".into(), "false".into())
        .apply(tx)?;
    Ok(tx.commit(&guard).await?)
}

fn validate_and_sum_removal_reservations(
    rows: &BTreeMap<String, (i64, Arc<str>)>,
) -> Result<usize> {
    for (key, _) in rows
        .range("removal-intent/".to_owned()..)
        .take_while(|(key, _)| key.starts_with("removal-intent/"))
    {
        check_removal_state_exists(rows, key)?;
    }
    rows.range("removal-state/".to_owned()..)
        .take_while(|(key, _)| key.starts_with("removal-state/"))
        .try_fold(0_usize, |total, (key, (revision, value))| {
            let reserved = removal_reservation(rows, key, *revision, value)?;
            total.checked_add(reserved).ok_or_else(reservation_overflow)
        })
}

/// Returns the lifecycle history charge for one serialized row.
pub(crate) fn history_row_bytes(key: &str, value: &str) -> usize {
    // Conservative per-record overhead for map nodes, revisions and owned strings.
    key.len().saturating_add(value.len()).saturating_add(128)
}

fn check_removal_state_exists(rows: &BTreeMap<String, (i64, Arc<str>)>, key: &str) -> Result<()> {
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
    rows: &BTreeMap<String, (i64, Arc<str>)>,
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

fn check_cache_bytes(bytes: usize, limit: usize) -> Result<()> {
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
    detached: Option<Table>,
}

impl<'a> GuardedCatalog<'a> {
    pub(crate) fn new(inner: &'a dyn Catalog, expected: &Table) -> Self {
        Self {
            inner,
            expected: expected.clone(),
            detached: None,
        }
    }

    pub(crate) fn with_detached(inner: &'a dyn Catalog, expected: &Table, detached: Table) -> Self {
        Self {
            inner,
            expected: expected.clone(),
            detached: Some(detached),
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
        Ok(self.detached.clone().unwrap_or(current))
    }
    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        if self.detached.is_some() {
            let _ = self.load_table(commit.identifier()).await?;
            // The detached transaction base generates a `RefSnapshotIdMatch { snapshot_id: None }`
            // requirement so `FastAppend` writes a standalone manifest list without inheriting prior
            // manifests. `load_table` above verified the exact expected metadata location and
            // `SqlCatalog::update_table` performs an atomic compare-and-swap on `metadata_location`.
            let _ = commit.take_requirements();
        } else {
            #[cfg(any(test, feature = "test-util"))]
            self.load_table(commit.identifier()).await?;
        }
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
mod tests;
