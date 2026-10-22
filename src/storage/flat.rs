//! Configured immutable primitive Parquet streams with one bounded footer decode.

use super::Objects;
use crate::{Error, Result, budget::Budget, model::ObjectRef};
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderMetadata,
        async_reader::AsyncFileReader,
    },
    file::metadata::{FooterTail, ParquetMetaDataReader},
};
use std::sync::Arc;

fn invalid() -> Error {
    // Do not propagate configured transport errors containing request details.
    Error::refusal("export_read", "immutable Parquet object cannot be read")
}
fn capacity() -> Error {
    Error::refusal(
        "capacity",
        "immutable Parquet reader exceeds shared allowance",
    )
}

pub(super) async fn open_reader(
    objects: &Objects,
    object: &ObjectRef,
) -> Result<Box<dyn AsyncFileReader>> {
    objects.validate_path(&object.path)?;
    if object.bytes < 12 {
        return Err(invalid());
    }
    match objects {
        Objects::Local => {
            let file = tokio::fs::File::open(&object.path).await?;
            if file.metadata().await?.len() != object.bytes {
                return Err(invalid());
            }
            Ok(Box::new(file))
        }
        #[cfg(feature = "remote")]
        Objects::Remote(remote) => remote.flat_reader(object),
    }
}

pub(super) async fn read(
    objects: &Objects,
    object: &ObjectRef,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    // Match the existing remote streaming allowance; transport buffers remain
    // maintained-client allocations, not a measured whole-process memory bound.
    let transport = budget.reserve(2 << 20)?;
    let mut reader = open_reader(objects, object).await?;
    // Bound the footer before allocating or decoding it. Supply the decoded
    // interpretation to the maintained stream builder rather than rereading it.
    let tail = reader
        .get_bytes(object.bytes - 8..object.bytes)
        .await
        .map_err(|_| invalid())?;
    let tail: [u8; 8] = tail.as_ref().try_into().map_err(|_| invalid())?;
    let footer_len = FooterTail::try_from(tail)
        .map_err(|_| invalid())?
        .metadata_length();
    if footer_len > budget.limits.max_metadata_bytes {
        return Err(Error::refusal(
            "metadata_capacity",
            "export footer exceeds metadata allowance",
        ));
    }
    let footer_start = object
        .bytes
        .checked_sub(8 + footer_len as u64)
        .filter(|start| *start >= 4)
        .ok_or_else(invalid)?;
    // Serialized metadata plus retained native and Arrow interpretations.
    let metadata_memory = budget.reserve(footer_len.checked_mul(4).ok_or_else(capacity)?)?;
    let encoded = reader
        .get_bytes(footer_start..object.bytes - 8)
        .await
        .map_err(|_| invalid())?;
    if encoded.len() != footer_len {
        return Err(invalid());
    }
    let metadata = ParquetMetaDataReader::decode_metadata(&encoded).map_err(|_| invalid())?;
    drop(encoded);
    // The async reader buffers one selected row group. Admit its largest exact
    // compressed and declared decoded size before payload ranges are requested.
    let mut largest_group = 0usize;
    for group in metadata.row_groups() {
        let mut bytes = 0usize;
        for column in group.columns() {
            let (start, length) = column.byte_range();
            if start < 4
                || start
                    .checked_add(length)
                    .is_none_or(|end| end > footer_start)
            {
                return Err(invalid());
            }
            for size in [column.compressed_size(), column.uncompressed_size()] {
                let size = usize::try_from(size).map_err(|_| invalid())?;
                bytes = bytes.checked_add(size).ok_or_else(capacity)?;
            }
        }
        largest_group = largest_group.max(bytes);
    }
    let payload_memory = budget.reserve(
        largest_group
            .checked_add(
                budget
                    .limits
                    .max_batch_bytes
                    .checked_mul(2)
                    .ok_or_else(capacity)?,
            )
            .ok_or_else(capacity)?,
    )?;
    let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), Default::default())
        .map_err(|_| invalid())?;
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
        .with_batch_size(budget.limits.batch_rows);
    let schema = builder.schema().clone();
    let stream = builder.build().map_err(|_| invalid())?;
    let limit = budget.limits.max_batch_bytes;
    let stream = futures::stream::try_unfold(
        (
            stream,
            std::collections::VecDeque::<arrow::record_batch::RecordBatch>::new(),
            transport,
            metadata_memory,
            payload_memory,
        ),
        move |(mut stream, mut pending, transport, metadata_memory, payload_memory)| async move {
            loop {
                if let Some(batch) = pending.pop_front() {
                    return Ok(Some((
                        batch,
                        (stream, pending, transport, metadata_memory, payload_memory),
                    )));
                }
                let Some(batch) = stream.try_next().await.map_err(|_| invalid())? else {
                    return Ok(None);
                };
                for slice in super::split_oversized_batch(batch, limit).map_err(|_| {
                    Error::refusal("oversized_batch", "export batch exceeds the byte allowance")
                })? {
                    if super::effective_batch_bytes(&slice) > limit {
                        return Err(Error::refusal(
                            "oversized_batch",
                            "export batch exceeds the byte allowance",
                        ));
                    }
                    pending.push_back(slice);
                }
            }
        },
    )
    .map_err(|error: Error| datafusion::error::DataFusionError::External(Box::new(error)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

// Rust guideline compliant 2026-02-21

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{budget::Limits, model::Id};
    use arrow::{
        array::{Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;

    #[tokio::test]
    async fn local_flat_reader_holds_shared_capacity_until_stream_drop() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("export.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(7), None]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let (digest, bytes) = super::super::digest(&path).unwrap();
        let object = ObjectRef {
            owner: Id::new("owner").unwrap(),
            path: path.to_string_lossy().into_owned(),
            digest,
            bytes,
        };
        let budget = Budget::new(Limits {
            memory_bytes: 16 << 20,
            max_batch_bytes: 1 << 20,
            batch_rows: 1,
            ..Limits::default()
        })
        .unwrap();
        let mut stream = Objects::Local
            .read_flat_parquet(&object, &budget)
            .await
            .unwrap();
        assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
        let first = stream.try_next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);
        let id = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(!id.is_null(0));
        assert_eq!(id.value(0), 7);
        drop(first);
        drop(stream);
        let _all = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
    }
}
