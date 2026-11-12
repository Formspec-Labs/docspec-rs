//! Charge maintained cache-entry residency; native reader admission is separate.
// Rust guideline compliant 2026-02-21
use datafusion::execution::cache::{
    CacheAccessor, DefaultFilesMetadataCache,
    cache_manager::{
        CachedFileMetadataEntry, FileMetadata, FileMetadataCache, FileMetadataCacheEntry,
    },
};
use datafusion::object_store::path::Path;
use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct AdmittedMetadataCache {
    inner: DefaultFilesMetadataCache,
    bytes: Arc<Semaphore>,
    shared_available_min: Arc<AtomicUsize>,
}
impl AdmittedMetadataCache {
    pub(super) fn new(
        limit: usize,
        bytes: Arc<Semaphore>,
        shared_available_min: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner: DefaultFilesMetadataCache::new(limit),
            bytes,
            shared_available_min,
        }
    }
}

struct HeldMetadata {
    inner: Arc<dyn FileMetadata>,
    _memory: OwnedSemaphorePermit,
}
impl FileMetadata for HeldMetadata {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }
    fn memory_size(&self) -> usize {
        self.inner.memory_size().saturating_add(128)
    }
    fn extra_info(&self) -> HashMap<String, String> {
        self.inner.extra_info()
    }
}
impl CacheAccessor<Path, CachedFileMetadataEntry> for AdmittedMetadataCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.get(key)
    }
    fn put(&self, key: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        let previous = self.inner.remove(key);
        let Some(size) = value
            .file_metadata
            .memory_size()
            .checked_add(128)
            .and_then(|size| u32::try_from(size).ok())
        else {
            return previous;
        };
        let Ok(memory) = self.bytes.clone().try_acquire_many_owned(size) else {
            return previous;
        };
        self.shared_available_min
            .fetch_min(self.bytes.available_permits(), Ordering::Relaxed);
        self.inner.put(
            key,
            CachedFileMetadataEntry::new(
                value.meta,
                Arc::new(HeldMetadata {
                    inner: value.file_metadata,
                    _memory: memory,
                }),
            ),
        );
        previous
    }
    fn remove(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.remove(key)
    }
    fn contains_key(&self, key: &Path) -> bool {
        self.inner.contains_key(key)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn clear(&self) {
        self.inner.clear();
    }
    fn name(&self) -> String {
        "AdmittedFileMetadataCache".into()
    }
}
impl FileMetadataCache for AdmittedMetadataCache {
    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }
    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit);
    }
    fn list_entries(&self) -> HashMap<Path, FileMetadataCacheEntry> {
        self.inner.list_entries()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Metadata;
    impl FileMetadata for Metadata {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn memory_size(&self) -> usize {
            768
        }
        fn extra_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }
    fn entry(key: &Path) -> CachedFileMetadataEntry {
        CachedFileMetadataEntry::new(
            datafusion::object_store::ObjectMeta {
                location: key.clone(),
                last_modified: std::time::UNIX_EPOCH.into(),
                size: 1024,
                e_tag: None,
                version: None,
            },
            Arc::new(Metadata),
        )
    }
    #[test]
    fn cache_entries_hold_shared_admission_until_the_last_entry_owner_drops() {
        let bytes = Arc::new(Semaphore::new(1280));
        let floor = Arc::new(AtomicUsize::new(1280));
        let cache = AdmittedMetadataCache::new(4096, bytes.clone(), floor.clone());
        let key = Path::from("first.parquet");
        cache.put(&key, entry(&key));
        assert_eq!(bytes.available_permits(), 384);
        assert_eq!(floor.load(Ordering::Relaxed), 384);
        let reader = cache.get(&key).unwrap();
        assert!(reader.file_metadata.as_any().is::<Metadata>());
        let second = Path::from("second.parquet");
        cache.put(&second, entry(&second));
        assert!(
            !cache.contains_key(&second),
            "optional caching cannot exceed shared admission"
        );
        cache.clear();
        assert_eq!(
            bytes.available_permits(),
            384,
            "a live entry clone still owns its cache reservation"
        );
        drop(reader);
        assert_eq!(bytes.available_permits(), 1280);
    }

    #[tokio::test]
    async fn indexed_native_reader_keeps_admission_after_cache_eviction_and_provider_release() {
        use crate::{
            budget::{Budget, Limits},
            job::Job,
        };
        use arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::{
            datasource::physical_plan::parquet::metadata::CachedParquetMetaData,
            physical_plan::execute_stream,
        };
        use futures::TryStreamExt;
        use parquet::{
            arrow::ArrowWriter,
            file::properties::{EnabledStatistics, WriterProperties},
        };
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("indexed.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let properties = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(properties),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![Arc::new(Int64Array::from_iter_values(0..4096))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let size = file.metadata().await.unwrap().len();
        use parquet::arrow::async_reader::AsyncFileReader;
        let tail = file.get_bytes(size - 8..size).await.unwrap();
        let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
        assert!(footer_len < 4096);
        let limits = Limits {
            memory_bytes: 128 << 20,
            scratch_bytes: 8 << 20,
            max_metadata_bytes: 4096,
            batch_rows: 32,
            max_batch_bytes: 128 << 10,
            compute_threads: 1,
        };
        let small_budget =
            Budget::in_workspace(limits.clone(), &root.path().join("small-scratch")).unwrap();
        let small = Job::from_budget(small_budget.clone()).unwrap();
        assert_eq!(
            small
                .register_parquet("indexed", &path)
                .await
                .unwrap_err()
                .code(),
            "metadata_capacity",
            "the small footer cannot hide the larger page indexes"
        );
        assert_eq!(
            small_budget
                .runtime_env()
                .cache_manager
                .get_file_metadata_cache()
                .len(),
            0,
            "refuse before native cache/index decoding"
        );
        let mut limits = limits;
        limits.max_metadata_bytes = 1 << 20;
        let budget = Budget::in_workspace(limits, &root.path().join("native-scratch")).unwrap();
        let before = budget.bytes.available_permits();
        let job = Job::from_budget(budget.clone()).unwrap();
        job.register_parquet("indexed", &path).await.unwrap();
        let cache = budget.runtime_env().cache_manager.get_file_metadata_cache();
        let key = cache
            .list_entries()
            .into_keys()
            .next()
            .expect("native inference caches metadata");
        let entry = cache.get(&key).unwrap();
        let bare = entry
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap()
            .parquet_metadata()
            .clone();
        assert!(
            bare.column_index().is_some(),
            "the actual maintained reader loaded native page indexes"
        );
        drop(entry);
        cache.clear();
        assert!(
            budget.bytes.available_permits() < before,
            "cache eviction does not release the exact provider's reader reservation"
        );
        let plan = job
            .sql("SELECT SUM(id) AS total FROM indexed")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let context = budget.context().task_ctx();
        job.release("indexed").unwrap();
        drop(bare);
        drop(job);
        let mut stream = execute_stream(plan.clone(), context).unwrap();
        drop(plan);
        assert!(
            budget.bytes.available_permits() < before,
            "physical stream retains the same reader reservation"
        );
        let batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            8_386_560
        );
        drop(batch);
        drop(stream);
        cache.clear();
        assert_eq!(
            budget.bytes.available_permits(),
            before,
            "the final native reader and cache owners release admission"
        );
        eprintln!(
            "native_indexed_metadata_probe {}",
            serde_json::json!({"file_bytes":size,"footer_bytes":footer_len,"small_metadata_limit":4096,"rows":4096,"sum":8386560,"native_indexes_loaded":true})
        );
    }
}
