//! Send every Iceberg FileIO multipart write to S3 in parts of one fixed size.
//!
//! Cloudflare R2 refuses a multipart upload whose non-trailing parts differ
//! in length ("InvalidPart: All non-trailing parts must have the same
//! length"). OpenDAL 0.57's S3 writer turns the caller's write sizes into
//! part sizes unless the writer is opened with `.chunk(..)`, and
//! iceberg-storage-opendal 0.10.1 opens it without one, so Parquet data and
//! manifest writes produced unequal parts. This factory wraps that storage
//! and re-chunks each writer into [`PART_BYTES`] writes, the same part size
//! `RemoteObjects::upload_file` passes to OpenDAL.
// Rust guideline compliant 2026-02-21

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use iceberg::{
    Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
        StorageFactory,
    },
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Part size for every S3 multipart write: FileIO writers and `upload_file`.
///
/// S3 allows at most 10,000 parts, so this also bounds one object's size.
pub(crate) const PART_BYTES: usize = 8 << 20;

/// Builds OpenDAL S3 storage whose writers emit only fixed-size parts.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct FixedPartS3;

#[typetag::serde(name = "DocspecFixedPartS3")]
impl StorageFactory for FixedPartS3 {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let inner = OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }
        .build(config)?;
        Ok(Arc::new(FixedPartStorage { inner }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FixedPartStorage {
    inner: Arc<dyn Storage>,
}

#[async_trait]
#[typetag::serde(name = "DocspecFixedPartStorage")]
impl Storage for FixedPartStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        // One buffer is one OpenDAL write: a single PUT, never multipart.
        self.inner.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(FixedPartWriter {
            inner: self.inner.writer(path).await?,
            pending: BytesMut::new(),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.inner.delete_stream(paths).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        // Keep InputFile/OutputFile bound to this storage, not the inner one.
        Ok(InputFile::new(
            Arc::new(self.clone_handle()),
            path.to_owned(),
        ))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(
            Arc::new(self.clone_handle()),
            path.to_owned(),
        ))
    }
}

impl FixedPartStorage {
    fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Forwards only `PART_BYTES` writes, then the remainder at close.
struct FixedPartWriter {
    inner: Box<dyn FileWrite>,
    pending: BytesMut,
}

#[async_trait]
impl FileWrite for FixedPartWriter {
    async fn write(&mut self, mut bs: Bytes) -> Result<()> {
        if !self.pending.is_empty() {
            let take = (PART_BYTES - self.pending.len()).min(bs.len());
            self.pending.extend_from_slice(&bs.split_to(take));
            if self.pending.len() < PART_BYTES {
                return Ok(());
            }
            self.inner.write(self.pending.split().freeze()).await?;
        }
        // Whole parts pass through without copying.
        while bs.len() >= PART_BYTES {
            self.inner.write(bs.split_to(PART_BYTES)).await?;
        }
        if !bs.is_empty() {
            self.pending.reserve(PART_BYTES);
            self.pending.extend_from_slice(&bs);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<iceberg::io::FileMetadata> {
        if !self.pending.is_empty() {
            self.inner.write(self.pending.split().freeze()).await?;
        }
        self.inner.close().await
    }
}
