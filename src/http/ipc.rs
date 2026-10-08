//! Demand-driven Arrow IPC encoding with admission owned by outgoing chunks.

use crate::{Error, Result, budget::Budget};
use arrow::ipc::writer::StreamWriter;
use axum::body::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, TryStreamExt};
use std::io::Write;
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn encode(
    batches: SendableRecordBatchStream,
    budget: Budget,
) -> Result<impl Stream<Item = Result<Bytes>> + Send + use<>> {
    if batches.schema().flattened_fields().iter().any(|field| {
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::Dictionary(..)
        )
    }) {
        return Err(Error::refusal(
            "unsupported_ipc_schema",
            "dictionary state retention is not qualified for HTTP streaming",
        ));
    }
    // Arrow temporarily encodes batch buffers before writing them. The
    // sink and each transferred chunk have separate reservations below.
    let retained = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    let limit = budget
        .limits
        .max_batch_bytes
        .saturating_mul(2)
        .max(budget.limits.max_metadata_bytes);
    let first = budget.reserve(limit)?;
    let writer = StreamWriter::try_new(
        Sink {
            bytes: Vec::new(),
            limit,
        },
        &batches.schema(),
    )?;
    Ok(futures::stream::try_unfold(
        (batches, writer, Some(first), budget, retained, false),
        |(mut batches, mut writer, first, budget, retained, finished)| async move {
            if finished {
                return Ok(None);
            }
            let mut finished = false;
            let reservation = if let Some(first) = first {
                first
            } else {
                let reservation = budget.reserve(writer.get_ref().limit)?;
                if let Some(batch) = batches.try_next().await? {
                    if batch.schema() != batches.schema() {
                        return Err(Error::refusal(
                            "invalid_schema",
                            "IPC batch differs from the declared stream schema",
                        ));
                    }
                    if batch.get_array_memory_size() > budget.limits.max_batch_bytes {
                        return Err(Error::refusal(
                            "oversized_batch",
                            "IPC input exceeds the batch allowance",
                        ));
                    }
                    writer.write(&batch)?;
                } else {
                    writer.finish()?;
                    finished = true;
                }
                reservation
            };
            let bytes = Bytes::from_owner(Chunk {
                bytes: std::mem::take(&mut writer.get_mut().bytes),
                _reservation: reservation,
            });
            // No producer task or queue: the next batch is requested only when
            // the HTTP body asks for it. A failed batch never emits a success end.
            tokio::task::consume_budget().await;
            Ok(Some((
                bytes,
                (batches, writer, None, budget, retained, finished),
            )))
        },
    ))
}

struct Chunk {
    bytes: Vec<u8>,
    _reservation: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Chunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct Sink {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("IPC chunk length overflow"))?;
        if length > self.limit {
            return Err(std::io::Error::other(
                "encoded IPC chunk exceeds its byte allowance",
            ));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn budget() -> Budget {
        Budget::new(crate::budget::Limits {
            memory_bytes: 64 << 10,
            max_batch_bytes: 4096,
            max_metadata_bytes: 4096,
            ..crate::budget::Limits::default()
        })
        .unwrap()
    }
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(7), None, Some(7)]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn demand_controls_batch_reads_and_chunks_keep_admission_until_last_clone_drops() {
        let budget = budget();
        let batch = batch();
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]).inspect(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        ));
        let mut stream = encode(batches, budget.clone()).unwrap().boxed();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let schema = stream.try_next().await.unwrap().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(schema);
        let first = stream.try_next().await.unwrap().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let clone = first.clone();
        drop(stream);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let remainder = budget
            .reserve(budget.limits.memory_bytes / 2 - 8192)
            .unwrap();
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop(first);
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop(clone);
        let recovered = budget.reserve(8192).unwrap();
        drop((remainder, recovered));
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }

    #[tokio::test]
    async fn encoding_preserves_strings_nulls_duplicates_and_complete_empty_streams() {
        for empty in [false, true] {
            let first = StringArray::from(vec![Some("a"), None, Some("a")]);
            let second = StringArray::from(vec![Some("changed"), Some(""), None]);
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
            let expected = if empty {
                vec![]
            } else {
                vec![
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(first)]).unwrap(),
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(second)]).unwrap(),
                ]
            };
            let batches = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(expected.clone().into_iter().map(Ok)),
            ));
            let mut stream = encode(batches, budget()).unwrap().boxed();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.unwrap() {
                bytes.extend_from_slice(&chunk);
            }
            assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
            let reader =
                arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap();
            assert_eq!(reader.schema(), schema);
            let actual = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn dictionaries_and_changing_batch_schemas_refuse() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for data_type in [
            dictionary.clone(),
            DataType::List(Arc::new(Field::new("item", dictionary, true))),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
            let batches = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            ));
            assert_eq!(
                encode(batches, budget()).err().unwrap().code(),
                "unsupported_ipc_schema"
            );
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            true,
        )]));
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch())]),
        ));
        let mut stream = encode(batches, budget()).unwrap().boxed();
        drop(stream.try_next().await.unwrap());
        assert_eq!(
            stream.try_next().await.unwrap_err().code(),
            "invalid_schema"
        );
    }

    #[tokio::test]
    async fn failures_abort_the_body_without_emitting_a_successful_end() {
        let budget = budget();
        let batch = batch();
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![
                Ok(batch),
                Err(datafusion::error::DataFusionError::External(Box::new(
                    Error::refusal("read_failed", "injected source failure"),
                ))),
            ]),
        ));
        let body = axum::body::Body::from_stream(encode(batches, budget.clone()).unwrap());
        assert!(axum::body::to_bytes(body, 1 << 20).await.is_err());
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());

        let large = Arc::new(StringArray::from(vec!["x".repeat(8192)]));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(RecordBatch::try_new(schema, vec![large]).unwrap())]),
        ));
        let mut stream = encode(batches, budget.clone()).unwrap().boxed();
        drop(stream.try_next().await.unwrap());
        assert_eq!(
            stream.try_next().await.unwrap_err().code(),
            "oversized_batch"
        );
        drop(stream);
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }

    #[test]
    fn encoded_chunks_refuse_before_growing_beyond_their_admission() {
        let mut sink = Sink {
            bytes: Vec::new(),
            limit: 7,
        };
        sink.write_all(b"1234").unwrap();
        assert!(sink.write_all(b"5678").is_err());
        assert_eq!(sink.bytes, b"1234");
        sink.write_all(b"567").unwrap();
        assert_eq!(sink.bytes.len(), 7);
    }
}
