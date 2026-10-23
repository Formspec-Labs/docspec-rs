//! Bounded batch materialization and full-refresh DataFrame builders for connected components.

use super::{eval::EvaluatedGraph, topology::GraphTopology};
use crate::{budget::Budget, error::Result, model::ConnectedComponentsTransform};
use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::{DataFrame, SessionContext};
use std::{collections::BTreeSet, sync::Arc};

/// Recursively builds Arrow [`RecordBatch`]es from `chunk` using `build_batch`, halving any chunk
/// whose materialized array memory exceeds `max_bytes` (down to single-row batches).
pub(super) fn build_chunk_bounded<T, F>(
    chunk: &[T],
    max_bytes: usize,
    out: &mut Vec<RecordBatch>,
    build_batch: &F,
) -> Result<()>
where
    F: Fn(&[T]) -> Result<RecordBatch>,
{
    let batch = build_batch(chunk)?;
    if batch.get_array_memory_size() > max_bytes && chunk.len() > 1 {
        let mid = chunk.len() / 2;
        build_chunk_bounded(&chunk[..mid], max_bytes, out, build_batch)?;
        build_chunk_bounded(&chunk[mid..], max_bytes, out, build_batch)?;
    } else {
        out.push(batch);
    }
    Ok(())
}

/// Builds bounded single-column scope batches containing `changed_scope_docs` for key replacement.
pub(super) fn build_scope_batches(
    doc_id_col: &str,
    changed_scope_docs: &BTreeSet<String>,
    budget: &Budget,
) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        doc_id_col,
        DataType::Utf8,
        true,
    )]));
    let docs: Vec<&str> = changed_scope_docs.iter().map(String::as_str).collect();
    if docs.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }
    let mut out = Vec::new();
    let max_rows = budget.limits.batch_rows.max(1);
    let max_bytes = budget.limits.max_batch_bytes;
    let build_batch = |chunk: &[&str]| -> Result<RecordBatch> {
        let n = chunk.len();
        let mut b = StringBuilder::with_capacity(n, n * 64);
        for &doc_id in chunk {
            b.append_value(doc_id);
        }
        Ok(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(b.finish())],
        )?)
    };
    for chunk in docs.chunks(max_rows) {
        build_chunk_bounded(chunk, max_bytes, &mut out, &build_batch)?;
    }
    Ok(out)
}

/// Evaluates the full connected-components source output [`DataFrame`] from `input`.
pub(in crate::engine) async fn source_frame(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    let topo = GraphTopology::from_table(context, spec, input, budget).await?;
    let graph = EvaluatedGraph::from_topology(topo, None, context, spec, input, budget).await?;
    let batches = graph.materialize_components_batches(spec, None, budget)?;
    Ok(context.read_batches(batches)?)
}

/// Evaluates the full connected-components contributor-evidence [`DataFrame`] from `input`.
pub(in crate::engine) async fn evidence_frame(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    let topo = GraphTopology::from_table(context, spec, input, budget).await?;
    let batches = topo.materialize_evidence_batches(spec, None, budget)?;
    Ok(context.read_batches(batches)?)
}
