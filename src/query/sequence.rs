//! Streams shaped DataFrames sequentially through one maintained table partition.

// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    dataframe::DataFrame,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use futures::{StreamExt, TryStreamExt, stream};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

/// Streams equally shaped frames, opening each child only after its predecessor ends.
///
/// Callers bound frame metadata and select the required columns before composition.
/// The provider and each execution retain the frames and their memory admission.
/// Child planning and reader errors propagate through the returned stream.
///
/// # Errors
/// Refuses empty input or unequal Arrow schemas, or an invalid streaming table.
pub(crate) fn sequence(
    frames: Vec<DataFrame>,
    memory: Vec<Arc<OwnedSemaphorePermit>>,
) -> Result<Arc<dyn TableProvider>> {
    let first = frames
        .first()
        .ok_or_else(|| Error::refusal("sequence_schema", "sequential frames require a schema"))?;
    let schema = Arc::new(first.schema().as_arrow().clone());
    if frames
        .iter()
        .any(|frame| frame.schema().as_arrow() != schema.as_ref())
    {
        return Err(Error::refusal(
            "sequence_schema",
            "sequential frames must have identical schemas",
        ));
    }
    let partition = Arc::new(SequencePartition {
        schema: schema.clone(),
        input: Arc::new(SequenceInput {
            frames,
            _memory: memory,
        }),
    });
    Ok(Arc::new(StreamingTable::try_new(schema, vec![partition])?))
}

#[derive(Debug)]
struct SequenceInput {
    // Drop frame metadata before releasing its admission.
    frames: Vec<DataFrame>,
    _memory: Vec<Arc<OwnedSemaphorePermit>>,
}

#[derive(Debug)]
struct SequencePartition {
    schema: SchemaRef,
    input: Arc<SequenceInput>,
}

impl PartitionStream for SequencePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let input = self.input.clone();
        let retained = input.clone();
        // `then` has one pending child; `try_flatten` drains it before asking
        // for the next index. Each frame keeps its own native column selection.
        let batches = stream::iter(0..input.frames.len())
            .then(move |index| {
                let input = input.clone();
                async move { input.frames[index].clone().execute_stream().await }
            })
            .try_flatten()
            .map(move |batch| {
                // Keep all retained frame metadata admitted even after EOF,
                // until this stream and the provider's other owners drop.
                let _keep_alive = &retained;
                batch
            });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{common::DataFusionError, prelude::SessionContext};
    use futures::FutureExt;
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
    };
    use tokio::sync::Semaphore;

    #[derive(Debug, Default)]
    struct Probe {
        active: AtomicUsize,
        peak: AtomicUsize,
        opened: AtomicUsize,
    }

    struct Active(Arc<Probe>);

    impl Drop for Active {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct FixturePartition {
        schema: SchemaRef,
        value: i64,
        probe: Arc<Probe>,
        eof: Arc<AtomicBool>,
        fail: bool,
    }

    impl PartitionStream for FixturePartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            self.probe.opened.fetch_add(1, Ordering::SeqCst);
            let active = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.probe.peak.fetch_max(active, Ordering::SeqCst);
            let guard = Active(self.probe.clone());
            let eof = self.eof.clone();
            let mut first = Some(if self.fail {
                Err(DataFusionError::Execution("later child failed".into()))
            } else {
                Ok(RecordBatch::try_new(
                    self.schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![self.value]))],
                )
                .unwrap())
            });
            let batches = stream::poll_fn(move |_context| {
                let _keep_alive = &guard;
                if let Some(batch) = first.take() {
                    Poll::Ready(Some(batch))
                } else if eof.load(Ordering::SeqCst) {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            });
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
        }
    }

    fn frame(value: i64, probe: &Arc<Probe>, eof: &Arc<AtomicBool>, fail: bool) -> DataFrame {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "signed",
            DataType::Int64,
            false,
        )]));
        let partition = Arc::new(FixturePartition {
            schema: schema.clone(),
            value,
            probe: probe.clone(),
            eof: eof.clone(),
            fail,
        });
        let provider = Arc::new(StreamingTable::try_new(schema, vec![partition]).unwrap());
        SessionContext::new().read_table(provider).unwrap()
    }

    async fn open(provider: &Arc<dyn TableProvider>) -> SendableRecordBatchStream {
        SessionContext::new()
            .read_table(provider.clone())
            .unwrap()
            .execute_stream()
            .await
            .unwrap()
    }

    fn value(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn next_child_waits_for_eof_and_only_one_child_is_active() {
        let probe = Arc::new(Probe::default());
        let first_eof = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(true));
        let provider = sequence(
            vec![
                frame(1, &probe, &first_eof, false),
                frame(2, &probe, &ready, false),
            ],
            vec![],
        )
        .unwrap();
        let mut stream = open(&provider).await;
        assert_eq!(probe.opened.load(Ordering::SeqCst), 0);
        assert_eq!(value(&stream.try_next().await.unwrap().unwrap()), 1);
        assert!(stream.try_next().now_or_never().is_none());
        assert_eq!(probe.opened.load(Ordering::SeqCst), 1);
        first_eof.store(true, Ordering::SeqCst);
        assert_eq!(value(&stream.try_next().await.unwrap().unwrap()), 2);
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(probe.opened.load(Ordering::SeqCst), 2);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn later_child_error_propagates() {
        let probe = Arc::new(Probe::default());
        let ready = Arc::new(AtomicBool::new(true));
        let provider = sequence(
            vec![
                frame(1, &probe, &ready, false),
                frame(2, &probe, &ready, true),
            ],
            vec![],
        )
        .unwrap();
        let mut stream = open(&provider).await;
        assert_eq!(value(&stream.try_next().await.unwrap().unwrap()), 1);
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("later child failed")
        );
        drop(stream);
        assert_eq!(probe.opened.load(Ordering::SeqCst), 2);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn early_drop_does_not_open_later_child_and_releases_stream_admission() {
        let probe = Arc::new(Probe::default());
        let ready = Arc::new(AtomicBool::new(true));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::new(semaphore.clone().try_acquire_owned().unwrap());
        let provider = sequence(
            vec![
                frame(1, &probe, &ready, false),
                frame(2, &probe, &ready, false),
            ],
            vec![permit],
        )
        .unwrap();
        let mut stream = open(&provider).await;
        assert_eq!(value(&stream.try_next().await.unwrap().unwrap()), 1);
        drop(provider);
        assert_eq!(semaphore.available_permits(), 0);
        drop(stream);
        assert_eq!(probe.opened.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn admission_survives_eof_until_provider_and_stream_owners_drop() {
        let probe = Arc::new(Probe::default());
        let ready = Arc::new(AtomicBool::new(true));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::new(semaphore.clone().try_acquire_owned().unwrap());
        let provider = sequence(vec![frame(1, &probe, &ready, false)], vec![permit]).unwrap();
        let mut stream = open(&provider).await;
        assert!(stream.try_next().await.unwrap().is_some());
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(semaphore.available_permits(), 0);
        drop(stream);
        assert_eq!(semaphore.available_permits(), 0);
        let mut surviving = open(&provider).await;
        drop(provider);
        assert_eq!(semaphore.available_permits(), 0);
        assert!(surviving.try_next().await.unwrap().is_some());
        assert!(surviving.try_next().await.unwrap().is_none());
        assert_eq!(semaphore.available_permits(), 0);
        drop(surviving);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
