//! Reopens exact native URI identities exclusively from protected retained copies.
// Rust guideline compliant 2026-02-21

use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};

/// Creates read-only native I/O without rewriting paths inside Iceberg objects.
///
/// Unknown URIs and all mutations refuse; no ambient source fallback exists.
pub(crate) fn retained_io(
    paths: BTreeMap<String, String>,
    budget: &crate::budget::Budget,
    owner: Option<Arc<dyn Debug + Send + Sync>>,
) -> crate::Result<FileIO> {
    let size = crate::budget::metadata_size(&paths, budget.limits.max_metadata_bytes)?;
    let memory = Arc::new(MapMemory {
        _permit: budget.reserve(size.saturating_mul(8))?,
        _owner: owner,
    });
    Ok(FileIOBuilder::new(Arc::new(RetainedUris {
        paths: Arc::new(paths),
        memory: Some(memory),
    }))
    .build())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetainedUris {
    paths: Arc<BTreeMap<String, String>>,
    #[serde(skip)]
    memory: Option<Arc<MapMemory>>,
}

#[derive(Debug)]
struct MapMemory {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _owner: Option<Arc<dyn Debug + Send + Sync>>,
}

fn unavailable() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "native snapshot URI is outside its admitted retained read closure",
    )
}

impl RetainedUris {
    fn input(&self, path: &str) -> iceberg::Result<InputFile> {
        let retained = self.paths.get(path).ok_or_else(unavailable)?;
        FileIO::new_with_fs().new_input(retained)
    }
}

#[typetag::serde]
impl StorageFactory for RetainedUris {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        if self.memory.is_none() {
            return Err(unavailable());
        }
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for RetainedUris {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.input(path)?.exists().await
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.input(path)?.metadata().await
    }
    async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
        self.input(path)?.read().await
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(HeldRead {
            inner: self.input(path)?.reader().await?,
            _memory: self.memory.clone().ok_or_else(unavailable)?,
        }))
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        self.input(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
    async fn write(&self, _: &str, _: bytes::Bytes) -> iceberg::Result<()> {
        Err(unavailable())
    }
    async fn writer(&self, _: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Err(unavailable())
    }
    async fn delete(&self, _: &str) -> iceberg::Result<()> {
        Err(unavailable())
    }
    async fn delete_prefix(&self, _: &str) -> iceberg::Result<()> {
        Err(unavailable())
    }
    async fn delete_stream(
        &self,
        _: futures::stream::BoxStream<'static, String>,
    ) -> iceberg::Result<()> {
        Err(unavailable())
    }
    fn new_output(&self, _: &str) -> iceberg::Result<OutputFile> {
        Err(unavailable())
    }
}

struct HeldRead {
    inner: Box<dyn FileRead>,
    _memory: Arc<MapMemory>,
}

#[async_trait::async_trait]
impl FileRead for HeldRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<bytes::Bytes> {
        self.inner.read(range).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_uri_reader_shares_map_and_retains_admission_after_io_drop() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("original");
        std::fs::write(&path, b"original-native-bytes").unwrap();
        let limits = crate::budget::Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 1 << 20,
            max_batch_bytes: 65536,
            max_metadata_bytes: 65536,
            batch_rows: 16,
        };
        let budget = crate::budget::Budget::new(limits).unwrap();
        let owner = Arc::new(budget.reserve(8192).unwrap());
        let weak = Arc::downgrade(&owner);
        let io = retained_io(
            BTreeMap::from([(
                "s3://original/raw/member".into(),
                path.to_string_lossy().into_owned(),
            )]),
            &budget,
            Some(owner.clone()),
        )
        .unwrap();
        let input = io.new_input("s3://original/raw/member").unwrap();
        assert_eq!(input.location(), "s3://original/raw/member");
        assert!(io.new_input("s3://original/raw/missing").is_err());
        assert!(io.new_output("s3://original/raw/member").is_err());
        let reader = input.reader().await.unwrap();
        drop(owner);
        drop(input);
        drop(io);
        assert!(
            weak.upgrade().is_some(),
            "native readers retain admission after their FileIO is dropped"
        );
        assert_eq!(reader.read(0..8).await.unwrap().as_ref(), b"original");
        drop(reader);
        assert!(weak.upgrade().is_none());
        let maps = RetainedUris {
            paths: Arc::new(BTreeMap::new()),
            memory: None,
        };
        assert!(
            Arc::ptr_eq(&maps.paths, &maps.clone().paths),
            "native opens share the closure map"
        );
    }
}
