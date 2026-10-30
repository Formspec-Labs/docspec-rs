//! Stream endpoint pairs through shared union-find; retain only unique nodes.
// Rust guideline compliant 2026-02-21
use super::{Job, StepStorage, materialized};
use crate::{Error, Result, budget::Budget, disjoint_set::DisjointSet};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, BinaryViewArray, FixedSizeBinaryArray,
        FixedSizeBinaryBuilder, LargeBinaryArray, LargeStringArray, StringArray, StringBuilder,
        StringViewArray,
    },
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
        "components requires two non-null string or binary endpoints and a positive node limit",
    )
}

#[derive(Clone, Copy)]
enum KeyKind {
    Utf8,
    Binary,
    FixedBinary(usize),
}
impl KeyKind {
    fn from_fields(left: &DataType, right: &DataType) -> Result<Self> {
        let is_utf8 = |dt: &DataType| {
            matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        };
        let is_binary = |dt: &DataType| match dt {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
            DataType::FixedSizeBinary(w) => *w > 0,
            _ => false,
        };
        if is_utf8(left) && is_utf8(right) {
            Ok(Self::Utf8)
        } else if let (DataType::FixedSizeBinary(lw), DataType::FixedSizeBinary(rw)) = (left, right)
        {
            if lw != rw || *lw <= 0 {
                return Err(invalid());
            }
            Ok(Self::FixedBinary(*lw as usize))
        } else if is_binary(left) && is_binary(right) {
            Ok(Self::Binary)
        } else {
            Err(invalid())
        }
    }
    fn output_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::FixedBinary(width) => DataType::FixedSizeBinary(width as i32),
        }
    }
}

fn endpoint_bytes(array: &dyn Array, row: usize) -> Result<&[u8]> {
    if array.is_null(row) {
        return Err(invalid());
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?
            .value(row)
            .as_bytes()),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(invalid)?
            .value(row)
            .as_bytes()),
        DataType::Utf8View => Ok(array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(invalid)?
            .value(row)
            .as_bytes()),
        DataType::Binary => Ok(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(invalid)?
            .value(row)),
        DataType::LargeBinary => Ok(array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(invalid)?
            .value(row)),
        DataType::BinaryView => Ok(array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .ok_or_else(invalid)?
            .value(row)),
        DataType::FixedSizeBinary(_) => Ok(array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(invalid)?
            .value(row)),
        _ => Err(invalid()),
    }
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
    // Keys are interned once into a contiguous arena and referenced by dense u32 IDs.
    kind: KeyKind,
    keys: Vec<u8>,
    offsets: Vec<u32>,
    minimum: Vec<u32>,
    index: HashTable<u32>,
    hasher: RandomState,
    sets: DisjointSet,
    memory: Memory,
    capacity: usize,
    node_limit: usize,
}
impl Graph {
    fn new(budget: &Budget, kind: KeyKind, node_limit: usize) -> Result<Self> {
        Ok(Self {
            kind,
            keys: vec![],
            offsets: vec![],
            minimum: vec![],
            index: HashTable::new(),
            hasher: RandomState::new(),
            sets: DisjointSet::new(0),
            memory: Memory::new(budget)?,
            capacity: 0,
            node_limit,
        })
    }
    fn len(&self) -> usize {
        self.minimum.len()
    }
    fn key_at<'a>(kind: KeyKind, keys: &'a [u8], offsets: &[u32], id: u32) -> &'a [u8] {
        let i = id as usize;
        match kind {
            KeyKind::FixedBinary(width) => &keys[i * width..(i + 1) * width],
            KeyKind::Utf8 | KeyKind::Binary => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                &keys[start..end]
            }
        }
    }
    fn key(&self, id: u32) -> &[u8] {
        Self::key_at(self.kind, &self.keys, &self.offsets, id)
    }
    fn storage_bytes(&self) -> usize {
        self.keys.capacity()
            + self.offsets.capacity() * size_of::<u32>()
            + self.minimum.capacity() * size_of::<u32>()
            + self.sets.storage_bytes()
            + self.index.allocation_size()
    }
    fn table_bytes(target: usize) -> usize {
        if target == 0 {
            return 0;
        }
        let min_buckets = target.saturating_mul(8).div_ceil(7).max(4);
        let buckets = min_buckets
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX);
        buckets
            .saturating_mul(size_of::<u32>() + 1)
            .saturating_add(64)
    }
    fn grow(&mut self) -> Result<()> {
        let target = self.capacity.saturating_mul(2).max(8).min(self.node_limit);
        if target > u32::MAX as usize {
            return Err(capacity());
        }
        let additional = target - self.len();
        let offset_target = match self.kind {
            KeyKind::FixedBinary(_) => 0,
            KeyKind::Utf8 | KeyKind::Binary => target.saturating_add(1),
        };
        let new_vectors = offset_target
            .saturating_mul(size_of::<u32>())
            .saturating_add(target.saturating_mul(2 * size_of::<u32>() + 1));
        let table = Self::table_bytes(target);
        let transient = self
            .keys
            .capacity()
            .saturating_add(self.index.allocation_size())
            .saturating_add(new_vectors)
            .saturating_add(table);
        self.memory.resize(transient)?;
        if offset_target > 0 {
            if self.offsets.is_empty() {
                self.offsets
                    .try_reserve_exact(offset_target)
                    .map_err(|_| capacity())?;
                self.offsets.push(0);
            } else {
                self.offsets
                    .try_reserve_exact(offset_target - self.offsets.len())
                    .map_err(|_| capacity())?;
            }
        }
        self.minimum
            .try_reserve_exact(additional)
            .map_err(|_| capacity())?;
        self.sets
            .try_reserve_nodes(target)
            .map_err(|_| capacity())?;
        let kind = self.kind;
        let keys = &self.keys;
        let offsets = &self.offsets;
        let hasher = &self.hasher;
        self.index
            .try_reserve(additional, |id| {
                hasher.hash_one(Self::key_at(kind, keys, offsets, *id))
            })
            .map_err(|_| capacity())?;
        self.capacity = target;
        self.memory.resize(self.storage_bytes())
    }
    fn node(&mut self, value: &[u8]) -> Result<u32> {
        let hash = self.hasher.hash_one(value);
        let kind = self.kind;
        let keys = &self.keys;
        let offsets = &self.offsets;
        if let Some(id) = self
            .index
            .find(hash, |id| Self::key_at(kind, keys, offsets, *id) == value)
        {
            return Ok(*id);
        }
        if self.len() == self.node_limit {
            return Err(capacity());
        }
        if self.len() == self.capacity {
            self.grow()?;
        }
        let needed = self
            .keys
            .len()
            .checked_add(value.len())
            .ok_or_else(capacity)?;
        if !matches!(self.kind, KeyKind::FixedBinary(_)) && needed > u32::MAX as usize {
            return Err(capacity());
        }
        if needed > self.keys.capacity() {
            let base = self.storage_bytes() - self.keys.capacity();
            let doubled = self.keys.capacity().saturating_mul(2).max(needed).max(64);
            let target_key_cap = if self
                .memory
                .resize(base.checked_add(doubled).ok_or_else(capacity)?)
                .is_ok()
            {
                doubled
            } else {
                self.memory
                    .resize(base.checked_add(needed).ok_or_else(capacity)?)?;
                needed
            };
            self.keys
                .try_reserve_exact(target_key_cap - self.keys.len())
                .map_err(|_| capacity())?;
        }
        self.keys.extend_from_slice(value);
        if !matches!(self.kind, KeyKind::FixedBinary(_)) {
            self.offsets.push(needed as u32);
        }
        let id = u32::try_from(self.len()).map_err(|_| capacity())?;
        self.minimum.push(id);
        assert_eq!(self.sets.push(), id as usize);
        let kind = self.kind;
        let keys = &self.keys;
        let offsets = &self.offsets;
        let hasher = &self.hasher;
        self.index.insert_unique(hash, id, |id| {
            hasher.hash_one(Self::key_at(kind, keys, offsets, *id))
        });
        self.memory.resize(self.storage_bytes())?;
        Ok(id)
    }
    fn edge(&mut self, left: &[u8], right: &[u8]) -> Result<()> {
        let left = self.node(left)?;
        let right = self.node(right)?;
        let a = self.sets.find(left as usize);
        let b = self.sets.find(right as usize);
        let min_a = self.minimum[a];
        let min_b = self.minimum[b];
        let minimum = if self.key(min_a) <= self.key(min_b) {
            min_a
        } else {
            min_b
        };
        let root = self.sets.union(a, b);
        self.minimum[root] = minimum;
        Ok(())
    }
    fn output(mut self) -> SendableRecordBatchStream {
        for id in 0..self.len() {
            let root = self.sets.find(id);
            self.minimum[id] = self.minimum[root];
        }
        self.index = HashTable::new();
        self.sets = DisjointSet::new(0);
        self.keys.shrink_to_fit();
        self.offsets.shrink_to_fit();
        self.minimum.shrink_to_fit();
        let _ = self.memory.resize(self.storage_bytes());

        let out_type = self.kind.output_type();
        let schema = Arc::new(Schema::new(vec![
            Field::new("node", out_type.clone(), false),
            Field::new("component", out_type, false),
        ]));
        let output_schema = schema.clone();
        let batches = stream::try_unfold(
            (self, 0usize, None::<OwnedSemaphorePermit>),
            move |(graph, start, _previous)| {
                let schema = output_schema.clone();
                async move {
                    if start == graph.len() {
                        return Ok(None);
                    }
                    let mut end = start;
                    let mut node_bytes = 0usize;
                    let mut label_bytes = 0usize;
                    while end < graph.len() && end - start < graph.memory.budget.limits.batch_rows {
                        let id = end as u32;
                        let label = graph.minimum[end];
                        let nodes = node_bytes.saturating_add(graph.key(id).len());
                        let labels = label_bytes.saturating_add(graph.key(label).len());
                        let offsets_overhead = match graph.kind {
                            KeyKind::FixedBinary(_) => 0,
                            KeyKind::Utf8 | KeyKind::Binary => (end - start + 2).saturating_mul(8),
                        };
                        let bytes = nodes
                            .saturating_add(labels)
                            .saturating_add(offsets_overhead)
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
                    let offsets_overhead = match graph.kind {
                        KeyKind::FixedBinary(_) => 0,
                        KeyKind::Utf8 | KeyKind::Binary => (end - start + 1) * 8,
                    };
                    let permit = graph
                        .memory
                        .budget
                        .reserve(
                            node_bytes
                                .saturating_add(label_bytes)
                                .saturating_add(offsets_overhead)
                                .saturating_add(512),
                        )
                        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                    let columns: Vec<ArrayRef> = match graph.kind {
                        KeyKind::Utf8 => {
                            let mut nodes = StringBuilder::with_capacity(end - start, node_bytes);
                            let mut labels = StringBuilder::with_capacity(end - start, label_bytes);
                            for id in start..end {
                                let node_str = std::str::from_utf8(graph.key(id as u32))
                                    .expect("verified UTF-8 key");
                                let label_str = std::str::from_utf8(graph.key(graph.minimum[id]))
                                    .expect("verified UTF-8 key");
                                nodes.append_value(node_str);
                                labels.append_value(label_str);
                            }
                            vec![Arc::new(nodes.finish()), Arc::new(labels.finish())]
                        }
                        KeyKind::Binary => {
                            let mut nodes = BinaryBuilder::with_capacity(end - start, node_bytes);
                            let mut labels = BinaryBuilder::with_capacity(end - start, label_bytes);
                            for id in start..end {
                                nodes.append_value(graph.key(id as u32));
                                labels.append_value(graph.key(graph.minimum[id]));
                            }
                            vec![Arc::new(nodes.finish()), Arc::new(labels.finish())]
                        }
                        KeyKind::FixedBinary(width) => {
                            let mut nodes =
                                FixedSizeBinaryBuilder::with_capacity(end - start, width as i32);
                            let mut labels =
                                FixedSizeBinaryBuilder::with_capacity(end - start, width as i32);
                            for id in start..end {
                                nodes
                                    .append_value(graph.key(id as u32))
                                    .expect("fixed width matches");
                                labels
                                    .append_value(graph.key(graph.minimum[id]))
                                    .expect("fixed width matches");
                            }
                            vec![Arc::new(nodes.finish()), Arc::new(labels.finish())]
                        }
                    };
                    let batch = RecordBatch::try_new(schema, columns)?;
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
    let kind = KeyKind::from_fields(
        frame.schema().field(0).data_type(),
        frame.schema().field(1).data_type(),
    )?;
    let mut stream = frame.execute_stream().await?;
    let mut graph = Graph::new(&job.budget, kind, settings.node_limit)?;
    while let Some(batch) = stream.try_next().await? {
        let left_col = batch.column(0).as_ref();
        let right_col = batch.column(1).as_ref();
        for row in 0..batch.num_rows() {
            let left = endpoint_bytes(left_col, row)?;
            let right = endpoint_bytes(right_col, row)?;
            graph.edge(left, right)?;
        }
    }
    drop(stream);
    let stream = graph.output();
    let schema = stream.schema();
    materialized::retain(&job.context, &job.budget, name, schema, stream).await
}
