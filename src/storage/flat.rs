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
    let (footer_len, footer_start) = footer_bounds(reader.as_mut(), object.bytes, budget).await?;
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
    read_prepared(
        budget,
        reader,
        metadata,
        metadata_memory,
        transport,
        footer_start,
    )
    .await
}

/// Admits one file's footer and declared index ranges before native decoding.
pub(crate) async fn admit_parquet_footer(
    path: &std::path::Path,
    budget: &Budget,
) -> Result<tokio::sync::OwnedSemaphorePermit> {
    let mut reader = tokio::fs::File::open(path).await?;
    let bytes = reader.metadata().await?.len();
    let (footer_len, footer_start) = footer_bounds(&mut reader, bytes, budget).await?;
    // Admit the bounded footer before decoding its maintained native index
    // descriptors. DataFusion eagerly decodes page indexes while inferring a
    // schema, even before cache insertion or physical page-pruning decisions.
    // Local metadata ranges cannot exceed this exact immutable file. Keep the
    // existing transport ceiling without reserving it for a tiny file; decoded
    // footer and validated enclosing-index allowances remain separate.
    let transport = usize::try_from(bytes.min(2 << 20)).map_err(|_| capacity())?;
    let mut memory = budget.reserve(
        footer_len
            .checked_mul(16)
            .and_then(|size| size.checked_add(transport))
            .ok_or_else(capacity)?,
    )?;
    let encoded = reader
        .get_bytes(footer_start..bytes - 8)
        .await
        .map_err(|_| invalid())?;
    if encoded.len() != footer_len {
        return Err(invalid());
    }
    let metadata = ParquetMetaDataReader::decode_metadata(&encoded).map_err(|_| invalid())?;
    let mut indexes = IndexRanges::default();
    for column in metadata
        .row_groups()
        .iter()
        .flat_map(|group| group.columns())
    {
        for (offset, length) in [
            (column.column_index_offset(), column.column_index_length()),
            (column.offset_index_offset(), column.offset_index_length()),
        ] {
            indexes.add(offset, length, footer_start)?;
        }
    }
    let index_charge = indexes.charge(footer_len, budget.limits.max_metadata_bytes)?;
    // Provider/plan ownership retains this allowance through native readers,
    // including bare metadata Arcs that may outlive a cache entry. Sixteen is
    // the same conservative native/Arrow decode estimate as footer admission;
    // it is not a whole-process RSS claim. Cache residency is charged separately.
    memory.merge(budget.reserve(index_charge)?);
    Ok(memory)
}

#[derive(Default)]
struct IndexRanges {
    decoded_bytes: usize,
    first: Option<u64>,
    end: u64,
}
impl IndexRanges {
    fn add(&mut self, offset: Option<i64>, length: Option<i32>, footer_start: u64) -> Result<()> {
        let bytes = index_range_bytes(offset, length, footer_start)?;
        self.decoded_bytes = self.decoded_bytes.checked_add(bytes).ok_or_else(capacity)?;
        if let Some(offset) = offset {
            let offset = u64::try_from(offset).map_err(|_| invalid())?;
            self.first = Some(self.first.map_or(offset, |first| first.min(offset)));
            self.end = self
                .end
                .max(offset.checked_add(bytes as u64).ok_or_else(invalid)?);
        }
        Ok(())
    }
    fn charge(&self, footer_bytes: usize, limit: usize) -> Result<usize> {
        // Parquet 58.4 fetches one enclosing page-index range, including gaps.
        let span = usize::try_from(self.end.saturating_sub(self.first.unwrap_or(self.end)))
            .map_err(|_| capacity())?;
        if footer_bytes
            .checked_add(span.max(self.decoded_bytes))
            .is_none_or(|bytes| bytes > limit)
        {
            return Err(Error::refusal(
                "metadata_capacity",
                "Parquet footer and enclosing page-index range exceed metadata allowance",
            ));
        }
        self.decoded_bytes
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(span.checked_mul(2)?))
            .ok_or_else(capacity)
    }
}

fn index_range_bytes(offset: Option<i64>, length: Option<i32>, footer_start: u64) -> Result<usize> {
    match (offset, length) {
        (None, None) => Ok(0),
        (Some(offset), Some(length)) => {
            let offset = u64::try_from(offset).map_err(|_| invalid())?;
            let length = usize::try_from(length).map_err(|_| invalid())?;
            if offset < 4
                || length == 0
                || offset
                    .checked_add(length as u64)
                    .is_none_or(|end| end > footer_start)
            {
                return Err(invalid());
            }
            Ok(length)
        }
        _ => Err(invalid()),
    }
}

async fn footer_bounds(
    reader: &mut dyn AsyncFileReader,
    bytes: u64,
    budget: &Budget,
) -> Result<(usize, u64)> {
    if bytes < 12 {
        return Err(invalid());
    }
    // Bound the footer before allocating or decoding it. Supply the decoded
    // interpretation to the maintained stream builder rather than rereading it.
    let tail = reader
        .get_bytes(bytes - 8..bytes)
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
    let footer_start = bytes
        .checked_sub(8 + footer_len as u64)
        .filter(|start| *start >= 4)
        .ok_or_else(invalid)?;
    Ok((footer_len, footer_start))
}

async fn read_prepared(
    budget: &Budget,
    reader: Box<dyn AsyncFileReader>,
    metadata: parquet::file::metadata::ParquetMetaData,
    metadata_memory: tokio::sync::OwnedSemaphorePermit,
    transport: tokio::sync::OwnedSemaphorePermit,
    footer_start: u64,
) -> Result<SendableRecordBatchStream> {
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
            budget.clone(),
        ),
        move |(mut stream, mut pending, transport, metadata_memory, payload_memory, budget)| async move {
            loop {
                if let Some(batch) = pending.pop_front() {
                    return Ok(Some((
                        batch,
                        (stream, pending, transport, metadata_memory, payload_memory, budget),
                    )));
                }
                let Some(batch) = stream.try_next().await.map_err(|_| invalid())? else {
                    return Ok(None);
                };
                for slice in super::split_oversized_batch(batch, limit, &budget).map_err(|_| {
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

    #[test]
    fn metadata_index_ranges_refuse_incomplete_negative_overflow_and_out_of_file_descriptors() {
        assert_eq!(index_range_bytes(Some(4), Some(12), 16).unwrap(), 12);
        assert_eq!(index_range_bytes(None, None, 16).unwrap(), 0);
        for (offset, length) in [
            (Some(-1), Some(1)),
            (Some(4), Some(-1)),
            (Some(4), None),
            (None, Some(4)),
            (Some(4), Some(13)),
            (Some(i64::MAX), Some(i32::MAX)),
        ] {
            assert!(index_range_bytes(offset, length, 16).is_err());
        }
    }

    #[test]
    fn metadata_index_ranges_charge_the_actual_transport_gap() {
        let mut indexes = IndexRanges::default();
        indexes.add(Some(4), Some(8), 20000).unwrap();
        indexes.add(Some(19990), Some(8), 20000).unwrap();
        assert_eq!(indexes.decoded_bytes, 16);
        assert_eq!(
            indexes.charge(512, 4096).unwrap_err().code(),
            "metadata_capacity"
        );
        assert_eq!(
            indexes.charge(512, 32768).unwrap(),
            16 * 16 + (19998 - 4) * 2
        );
    }

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
