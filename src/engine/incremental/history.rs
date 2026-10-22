//! Uses qualified changes as transient input to ordinary recoverable processing.

use super::*;
use arrow::datatypes::SchemaRef;
use std::sync::atomic::{AtomicU64, Ordering};

impl Engine {
    #[expect(
        clippy::too_many_arguments,
        reason = "History discovery binds canonical endpoints separately from physical scans"
    )]
    pub(super) fn history_keys<'a>(
        &'a self,
        dataset: &'a Id,
        output: &'a iceberg::table::Table,
        versions: (&'a ReleasePin, &'a ReleasePin),
        canonical: (&'a TablePin, &'a TablePin),
        old_native: &'a iceberg::table::Table,
        keys: &'a [String],
        key_schema: SchemaRef,
    ) -> futures::future::BoxFuture<'a, Result<Option<(TablePin, u64)>>> {
        Box::pin(async move {
        let budget = &self.inner.lifecycle.budget;
        let view = self
            .inner
            .lifecycle
            .open(&versions.1.dataset.dataset)
            .await?;
        let chain = match self
            .change_chain(&view, versions, canonical, old_native, changes::MAX_LINKS)
            .await?
        {
            changes::Forward::Eligible(chain) => chain,
            changes::Forward::Fallback(_) => return Ok(None),
        };
        // The complete refresh already owns maintenance admission. No chain
        // reference escapes into required recovery state: completed processing
        // retains its own output, input coverage and ordinary protections.
        let _memory = chain.memory;
        let scanned = Arc::new(AtomicU64::new(0));
        let rows = if keys.is_empty() {
            // A global group needs only existence of applied rows, not their values.
            let values = if chain.tables.is_empty() {
                vec![]
            } else {
                vec![0_i64]
            };
            drop(chain.tables);
            let batch = RecordBatch::try_new(
                key_schema,
                vec![Arc::new(arrow::array::Int64Array::from(values))],
            )?;
            futures::stream::once(async { Ok(batch) }).boxed()
        } else {
            let budget = budget.clone();
            let context = self.context();
            let scanned = scanned.clone();
            let keys = Arc::new(keys.to_vec());
            // Each delta inventory is captured once, used by its column-selected
            // reader, then dropped before the next delta opens. Project raw old
            // AND new keys: cancelling equal keys would hide same-key value edits.
            futures::stream::iter(chain.tables)
                .then(move |delta| {
                    let budget = budget.clone();
                    let context = context.clone();
                    let scanned = scanned.clone();
                    let keys = keys.clone();
                    let schema = key_schema.clone();
                    async move {
                        let scan =
                            Arc::new(storage::OccurrenceScan::open(&delta.table, &budget).await?);
                        scanned.fetch_add(scan.files().count() as u64, Ordering::Relaxed);
                        let provider = crate::query::files(scan, Some(&keys), budget)?;
                        let stream = context.read_table(provider)?.execute_stream().await?;
                        Ok::<_, Error>(stream.map(move |batch| {
                            let batch = batch?;
                            Ok(RecordBatch::try_new(
                                schema.clone(),
                                batch.columns().to_vec(),
                            )?)
                        }))
                    }
                })
                .try_flatten()
                .boxed()
        };
        let files = storage::write_batches(output, rows, budget).await?;
        let rows = files.iter().map(|file| file.record_count()).sum();
        let output = storage::append(output, self.inner.lifecycle.catalog.as_ref(), files).await?;
        Ok(Some((
            self.pin_output(dataset, &output, rows, &[]).await?,
            scanned.load(Ordering::Relaxed),
        )))
        })
    }
}
