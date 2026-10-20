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
    collections::{BTreeMap, BTreeSet, HashMap},
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
    cache: Arc<Mutex<Option<Arc<SnapshotRows>>>>,
    #[cfg(test)]
    pub(crate) lose_commit_response: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug)]
pub(crate) struct SnapshotRows {
    metadata: String,
    values: BTreeMap<String, (i64, Arc<str>)>,
    cache_bytes: usize,
    history_rows: usize,
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
            cache: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            lose_commit_response: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn with_retained_prefix(mut self, prefix: Option<String>) -> Self {
        self.retained_prefix = prefix;
        self
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
                .await?
        };
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: ident,
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
        self.rows_inner(view, true).await
    }

    /// Loads current records for baseline compaction, bounding memory by deduplicated live state.
    pub(crate) async fn rows_for_compaction(&self, view: &View) -> Result<Arc<SnapshotRows>> {
        self.rows_inner(view, false).await
    }

    async fn rows_inner(
        &self,
        view: &View,
        enforce_history_budget: bool,
    ) -> Result<Arc<SnapshotRows>> {
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
                _reservation: reservation,
            });
            *self.cache.lock().map_err(cache_poisoned)? = Some(entry.clone());
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
                if current
                    .get(key)
                    .is_none_or(|(old, _)| *old < revision)
                {
                    if let Some((_, old_value)) =
                        current.insert(key.to_owned(), (revision, Arc::from(value)))
                    {
                        cache_bytes = cache_bytes.saturating_sub(history_row_bytes(key, &old_value));
                    }
                    cache_bytes = cache_bytes.saturating_add(row_bytes);
                    check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
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
            _reservation: reservation,
        });
        if within_history_budget {
            *self.cache.lock().map_err(cache_poisoned)? = Some(entry.clone());
        }
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
        let guard = GuardedCatalog::new(self.catalog.as_ref(), &view.table);
        guard.load_table(view.table.identifier()).await?;
        // Prepare a bounded cache of the next immutable state. It becomes visible
        // only after the guarded native commit succeeds; failed commits cannot leak it.
        let reservation = self.budget.reserve(self.budget.limits.max_metadata_bytes)?;
        let previous = self.rows(view).await?;
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
            cache_bytes,
            history_rows,
            history_bytes,
            reserved_future_bytes,
            _reservation: reservation,
        });
        *self.cache.lock().map_err(cache_poisoned)? = Some(entry);
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
            logical_identifier: view.logical_identifier.clone(),
        })
    }

    /// Rewrites the lifecycle table to one baseline snapshot of live, unpruned records.
    ///
    /// # Errors
    /// Refuses stale views, empty compacted baselines, invalid removal reservations,
    /// oversized live state, or catalog commit failures.
    pub(crate) async fn compact_baseline(
        &self,
        view: &View,
        remove_keys: &BTreeSet<String>,
    ) -> Result<(View, LifecycleCompactionResult)> {
        let guard = GuardedCatalog::new(self.catalog.as_ref(), &view.table);
        guard.load_table(view.table.identifier()).await?;
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
        let _ = view
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
        if next_values.is_empty() {
            return Err(Error::refusal(
                "empty_baseline",
                "lifecycle baseline compaction must retain at least one record",
            ));
        }
        for (key, _) in next_values
            .range("removal-intent/".to_owned()..)
            .take_while(|(key, _)| key.starts_with("removal-intent/"))
        {
            check_removal_state_exists(&next_values, key)?;
        }
        let reserved_future_bytes = next_values
            .range("removal-state/".to_owned()..)
            .take_while(|(key, _)| key.starts_with("removal-state/"))
            .try_fold(0_usize, |total, (key, (revision, value))| {
                let reserved = removal_reservation(&next_values, key, *revision, value)?;
                total.checked_add(reserved).ok_or_else(reservation_overflow)
            })?;
        let history_rows = next_values.len();
        let history_bytes = cache_bytes;
        check_history_reservation(
            history_bytes,
            reserved_future_bytes,
            self.budget.limits.max_metadata_bytes,
        )?;
        check_cache_bytes(cache_bytes, self.budget.limits.max_metadata_bytes)?;
        let reservation = self.budget.reserve(self.budget.limits.max_metadata_bytes)?;
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
        let table = replace_baseline_snapshot(&view.table, self.catalog.as_ref(), files).await?;
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
        let entry = Arc::new(SnapshotRows {
            metadata: table.metadata_location_result()?.to_owned(),
            values: next_values,
            cache_bytes,
            history_rows,
            history_bytes,
            reserved_future_bytes,
            _reservation: reservation,
        });
        *self.cache.lock().map_err(cache_poisoned)? = Some(entry);
        Ok((
            View {
                revision: current_revision,
                table,
                logical_identifier: view.logical_identifier.clone(),
            },
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
            && (keys.len() >= max_rows
                || current_bytes.saturating_add(row_bytes) > max_batch_bytes)
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
    let guard = CompactingCatalog {
        inner: catalog,
        expected: table.clone(),
        detached: detached.clone(),
    };
    let tx = Transaction::new(&detached);
    let tx = tx.fast_append().add_data_files(files).apply(tx)?;
    let tx = if old_snapshot_ids.is_empty() {
        tx
    } else {
        tx.expire_snapshots()
            .expire_snapshot_ids(old_snapshot_ids)
            .apply(tx)?
    };
    Ok(tx.commit(&guard).await?)
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

/// Commits a detached single-manifest baseline while enforcing exact authority CAS.
#[derive(Debug)]
struct CompactingCatalog<'a> {
    inner: &'a dyn Catalog,
    expected: Table,
    detached: Table,
}

#[async_trait]
impl Catalog for CompactingCatalog<'_> {
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
        Ok(self.detached.clone())
    }
    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let _ = self.load_table(commit.identifier()).await?;
        // The detached transaction base generates a `RefSnapshotIdMatch { snapshot_id: None }`
        // requirement so `FastAppend` writes a standalone manifest list without inheriting prior
        // manifests. `load_table` above verified the exact expected metadata location and
        // `SqlCatalog::update_table` performs an atomic compare-and-swap on `metadata_location`.
        let _ = commit.take_requirements();
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
            (
                1,
                Arc::<str>::from(serde_json::to_string(&removal_intent("a")).unwrap()),
            ),
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
        rows.insert("removal-intent/a".into(), (2, Arc::from("{}")));
        assert!(removal_reservation(&rows, key, 2, &valid).is_err());
        rows.clear();
        rows.insert(
            "removal-intent/a".into(),
            (
                1,
                Arc::<str>::from(serde_json::to_string(&removal_intent("a")).unwrap()),
            ),
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
    async fn warm_commit_shares_unchanged_values_and_tracks_cache_bytes() {
        let root = tempfile::tempdir().unwrap();
        let catalog = storage::local_catalog(root.path()).await.unwrap();
        let lifecycle = Lifecycle::new(catalog.clone(), test_budget(1 << 20));
        let owner = Id::new("owner").unwrap();
        let view = lifecycle.open(&owner).await.unwrap();
        let view1 = lifecycle
            .commit(
                &view,
                vec![
                    record("untouched", "a".repeat(512)).unwrap(),
                    record("mutable", "v1").unwrap(),
                ],
            )
            .await
            .unwrap();
        let rows1 = lifecycle.rows(&view1).await.unwrap();
        let view2 = lifecycle
            .commit(&view1, vec![record("mutable", "v2-longer").unwrap()])
            .await
            .unwrap();
        let rows2 = lifecycle.rows(&view2).await.unwrap();
        // Unchanged values share the same Arc<str> allocation across commits.
        assert!(Arc::ptr_eq(
            &rows1.values["untouched"].1,
            &rows2.values["untouched"].1
        ));
        let reopened = Lifecycle::new(catalog, test_budget(1 << 20));
        let cold = reopened.rows(&view2).await.unwrap();
        assert_eq!(rows2.cache_bytes, cold.cache_bytes);
        assert_eq!(rows2.history_rows, cold.history_rows);
        assert_eq!(rows2.history_bytes, cold.history_bytes);
    }

    #[tokio::test]
    async fn compact_baseline_coalesces_superseded_revisions_and_reclaims_replay_budget() {
        let root = tempfile::tempdir().unwrap();
        let catalog = storage::local_catalog(root.path()).await.unwrap();
        let payload = "x".repeat(2048);
        let single_row_charge =
            history_row_bytes("authority", &serde_json::to_string(&payload).unwrap());
        let obsolete_charge =
            history_row_bytes("obsolete/1", &serde_json::to_string(&payload).unwrap());
        // Budget allows 8 historical commits, after which a 9th ordinary commit is refused.
        let budget = test_budget(7 * single_row_charge + obsolete_charge + 64);
        let lifecycle = Lifecycle::new(catalog.clone(), budget.clone());
        let owner = Id::new("owner").unwrap();
        let mut view = lifecycle.open(&owner).await.unwrap();
        view = lifecycle
            .commit(&view, vec![record("obsolete/1", &payload).unwrap()])
            .await
            .unwrap();
        for _ in 0..7 {
            view = lifecycle
                .commit(&view, vec![record("authority", &payload).unwrap()])
                .await
                .unwrap();
        }
        let authority_rev = view.revision;
        let refused = lifecycle
            .commit(&view, vec![record("authority", &payload).unwrap()])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), "metadata_capacity");

        let remove = BTreeSet::from(["obsolete/1".to_owned()]);
        let (compacted_view, stats) = lifecycle.compact_baseline(&view, &remove).await.unwrap();
        assert_eq!(stats.revisions_before, 8);
        assert_eq!(stats.revisions_after, 1);
        assert_eq!(stats.rows_before, 8);
        assert_eq!(stats.rows_after, 1);
        assert_eq!(stats.files_before, 8);
        assert_eq!(stats.files_after, 1);
        assert_eq!(
            stats.metadata_bytes_before,
            7 * single_row_charge + obsolete_charge
        );
        assert_eq!(stats.metadata_bytes_after, single_row_charge);
        assert_eq!(stats.current_revision, authority_rev + 1);

        // Cold reopen sees 1 snapshot, 1 file, 1 row, and preserves the exact key revision.
        let reopened = Lifecycle::new(catalog, budget);
        let cold_view = reopened.open(&owner).await.unwrap();
        assert_eq!(cold_view.revision, compacted_view.revision);
        assert_eq!(cold_view.table.metadata().snapshots().count(), 1);
        let cold_rows = reopened.rows(&cold_view).await.unwrap();
        assert_eq!(cold_rows.history_bytes, single_row_charge);
        assert_eq!(cold_rows.history_rows, 1);
        assert_eq!(cold_rows.revision("authority"), Some(authority_rev));
        assert!(!cold_rows.contains_key("obsolete/1"));

        // Subsequent commits succeed again within the reclaimed budget.
        let next = reopened
            .commit(&cold_view, vec![record("authority", &payload).unwrap()])
            .await
            .unwrap();
        assert_eq!(next.revision, compacted_view.revision + 1);
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
