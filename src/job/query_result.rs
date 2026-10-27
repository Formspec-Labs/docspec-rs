//! Bounded SQL results for manifests, serialized by the maintained Arrow writer.
// Rust guideline compliant 2026-02-21
use super::Job;
use crate::{Error, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    sync::Arc,
};

/// A bounded named result returned beside exported file identities.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub name: String,
    pub data: serde_json::Value,
    #[serde(skip)]
    _memory: Option<Arc<MemoryReservation>>,
}

struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
    memory: MemoryReservation,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("SQL result exceeds metadata allowance"))?;
        // Keep encoded bytes and native JSON-tree admission until result drop.
        let allowance = size
            .checked_mul(32)
            .ok_or_else(|| io::Error::other("SQL result allocation overflow"))?;
        self.memory
            .try_resize(allowance)
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) async fn query(job: &Job, name: &str, sql: &str) -> Result<QueryResult> {
    let mut stream = job.sql(sql).await?.execute_stream().await?;
    let buffer = Buffer {
        bytes: Vec::new(),
        limit: job.budget.limits.max_metadata_bytes,
        memory: MemoryConsumer::new("SQL metadata result")
            .register(&job.budget.runtime_env().memory_pool),
    };
    let mut writer = arrow::json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow::json::writer::JsonArray>(buffer);
    while let Some(batch) = stream.try_next().await? {
        let size = batch.get_array_memory_size();
        if size > job.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "SQL result batch exceeds metadata allowance",
            ));
        }
        let _encoding = job.budget.reserve(size.checked_mul(8).ok_or_else(|| {
            Error::refusal("capacity", "SQL result encoding allowance overflow")
        })?)?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    let buffer = writer.into_inner();
    let data = serde_json::from_slice(&buffer.bytes)?;
    Ok(QueryResult {
        name: name.into(),
        data,
        _memory: Some(Arc::new(buffer.memory)),
    })
}
