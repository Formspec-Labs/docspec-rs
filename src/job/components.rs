//! Stream endpoint pairs through shared union-find; retain only unique nodes.
// Rust guideline compliant 2026-02-21
use super::{Job, StepStorage, materialized};
use crate::{Error, Result, budget::Budget, disjoint_set::DisjointSet};
use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::{TryStreamExt, stream};
use hashbrown::HashTable;
use serde::{Deserialize, Serialize};
use std::{collections::hash_map::RandomState, hash::BuildHasher, mem::size_of, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

/// SQL and column names for a generic undirected endpoint relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentsSettings {
    pub sql: String,
    pub left: String,
    pub right: String,
    pub node_limit: usize,
}
fn capacity() -> Error {
    Error::refusal(
        "capacity",
        "connected components exceeds its node or byte allowance",
    )
}
fn invalid() -> Error {
    Error::refusal(
        "invalid_spec",
        "components requires two non-null string endpoints and a positive node limit",
    )
}

struct Memory {
    budget: Budget,
    permit: OwnedSemaphorePermit,
}
impl Memory {
    fn new(budget: &Budget) -> Result<Self> {
        Ok(Self {
            budget: budget.clone(),
            permit: budget.reserve(0)?,
        })
    }
    fn resize(&mut self, bytes: usize) -> Result<()> {
        let current = self.permit.num_permits();
        if bytes > current {
            self.permit.merge(self.budget.reserve(bytes - current)?);
        } else if bytes < current {
            drop(self.permit.split(current - bytes));
        }
        Ok(())
    }
}
struct Graph {
    // The index owns only integer references: each UTF-8 key is stored once.
    nodes: Vec<String>,
    hashes: Vec<u64>,
    minimum: Vec<usize>,
    index: HashTable<usize>,
    hasher: RandomState,
    sets: DisjointSet,
    memory: Memory,
    key_bytes: usize,
    capacity: usize,
    node_limit: usize,
}
impl Graph {
    fn new(budget: &Budget, node_limit: usize) -> Result<Self> {
        Ok(Self {
            nodes: vec![],
            hashes: vec![],
            minimum: vec![],
            index: HashTable::new(),
            hasher: RandomState::new(),
            sets: DisjointSet::new(0),
            memory: Memory::new(budget)?,
            key_bytes: 0,
            capacity: 0,
            node_limit,
        })
    }
    fn storage_bytes(&self) -> usize {
        self.nodes.capacity() * size_of::<String>()
            + self.hashes.capacity() * size_of::<u64>()
            + self.minimum.capacity() * size_of::<usize>()
            + self.sets.storage_bytes()
            + self.index.allocation_size()
            + self.key_bytes
    }
    fn grow(&mut self) -> Result<()> {
        let target = self.capacity.saturating_mul(2).max(8).min(self.node_limit);
        let additional = target - self.nodes.len();
        // Reserve the old containers plus all new allocations before any
        // reallocation. Hashbrown's power-of-two buckets at <=7/8 load fit
        // this conservative four-buckets-per-entry bound, including controls.
        let vectors = target
            .saturating_mul(size_of::<String>() + size_of::<u64>() + 2 * size_of::<usize>() + 1);
        let table = target
            .saturating_mul(4)
            .saturating_mul(size_of::<usize>() + 1)
            .saturating_add(128);
        let transient = self
            .storage_bytes()
            .saturating_add(vectors)
            .saturating_add(table);
        self.memory.resize(transient)?;
        self.nodes
            .try_reserve_exact(additional)
            .map_err(|_| capacity())?;
        self.hashes
            .try_reserve_exact(additional)
            .map_err(|_| capacity())?;
        self.minimum
            .try_reserve_exact(additional)
            .map_err(|_| capacity())?;
        self.sets
            .try_reserve_nodes(target)
            .map_err(|_| capacity())?;
        let hashes = &self.hashes;
        self.index
            .try_reserve(additional, |id| hashes[*id])
            .map_err(|_| capacity())?;
        self.capacity = target;
        self.memory.resize(self.storage_bytes())
    }
    fn node(&mut self, value: &str) -> Result<usize> {
        let hash = self.hasher.hash_one(value);
        if let Some(id) = self.index.find(hash, |id| self.nodes[*id] == value) {
            return Ok(*id);
        }
        if self.nodes.len() == self.node_limit {
            return Err(capacity());
        }
        if self.nodes.len() == self.capacity {
            self.grow()?;
        }
        self.memory.resize(
            self.storage_bytes()
                .checked_add(value.len())
                .ok_or_else(capacity)?,
        )?;
        let owned = value.to_owned();
        self.key_bytes += owned.capacity();
        let id = self.nodes.len();
        self.nodes.push(owned);
        self.hashes.push(hash);
        self.minimum.push(id);
        assert_eq!(self.sets.push(), id);
        let hashes = &self.hashes;
        self.index.insert_unique(hash, id, |id| hashes[*id]);
        self.memory.resize(self.storage_bytes())?;
        Ok(id)
    }
    fn edge(&mut self, left: &str, right: &str) -> Result<()> {
        let left = self.node(left)?;
        let right = self.node(right)?;
        let a = self.sets.find(left);
        let b = self.sets.find(right);
        let minimum =
            if self.nodes[self.minimum[a]].as_bytes() <= self.nodes[self.minimum[b]].as_bytes() {
                self.minimum[a]
            } else {
                self.minimum[b]
            };
        let root = self.sets.union(a, b);
        self.minimum[root] = minimum;
        Ok(())
    }
    fn output(self) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("node", DataType::Utf8, false),
            Field::new("component", DataType::Utf8, false),
        ]));
        let output_schema = schema.clone();
        let batches = stream::try_unfold(
            (self, 0, None::<OwnedSemaphorePermit>),
            move |(mut graph, start, _previous)| {
                let schema = output_schema.clone();
                async move {
                    if start == graph.nodes.len() {
                        return Ok(None);
                    }
                    let mut end = start;
                    let mut node_bytes = 0usize;
                    let mut label_bytes = 0usize;
                    while end < graph.nodes.len()
                        && end - start < graph.memory.budget.limits.batch_rows
                    {
                        let root = graph.sets.find(end);
                        let label = graph.minimum[root];
                        let nodes = node_bytes.saturating_add(graph.nodes[end].len());
                        let labels = label_bytes.saturating_add(graph.nodes[label].len());
                        let bytes = nodes
                            .saturating_add(labels)
                            .saturating_add((end - start + 2).saturating_mul(8))
                            .saturating_add(512);
                        if bytes > graph.memory.budget.limits.max_batch_bytes {
                            break;
                        }
                        node_bytes = nodes;
                        label_bytes = labels;
                        end += 1;
                    }
                    if end == start {
                        return Err(datafusion::common::DataFusionError::External(Box::new(
                            capacity(),
                        )));
                    }
                    let permit = graph
                        .memory
                        .budget
                        .reserve(
                            node_bytes
                                .saturating_add(label_bytes)
                                .saturating_add((end - start + 1) * 8)
                                .saturating_add(512),
                        )
                        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                    let mut nodes = StringBuilder::with_capacity(end - start, node_bytes);
                    let mut labels = StringBuilder::with_capacity(end - start, label_bytes);
                    for id in start..end {
                        let root = graph.sets.find(id);
                        nodes.append_value(&graph.nodes[id]);
                        labels.append_value(&graph.nodes[graph.minimum[root]]);
                    }
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(nodes.finish()), Arc::new(labels.finish())],
                    )?;
                    Ok(Some((batch, (graph, end, Some(permit)))))
                }
            },
        );
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

pub(super) async fn compute(
    job: &Job,
    name: &str,
    settings: &ComponentsSettings,
) -> Result<(StepStorage, usize, usize)> {
    if settings.node_limit == 0
        || settings.left.is_empty()
        || settings.right.is_empty()
        || job.context.table_exist(name)?
    {
        return Err(invalid());
    }
    let frame = job.sql(&settings.sql).await?;
    let frame = frame
        .select_columns(&[&settings.left, &settings.right])
        .map_err(|_| invalid())?;
    if !frame.schema().fields().iter().all(|f| {
        matches!(
            f.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    }) {
        return Err(invalid());
    }
    let mut stream = frame.execute_stream().await?;
    let mut graph = Graph::new(&job.budget, settings.node_limit)?;
    while let Some(batch) = stream.try_next().await? {
        for row in 0..batch.num_rows() {
            let left =
                super::functions::text(batch.column(0).as_ref(), row)?.ok_or_else(invalid)?;
            let right =
                super::functions::text(batch.column(1).as_ref(), row)?.ok_or_else(invalid)?;
            graph.edge(left, right)?;
        }
    }
    drop(stream);
    let stream = graph.output();
    let schema = stream.schema();
    materialized::retain(&job.context, &job.budget, name, schema, stream).await
}
