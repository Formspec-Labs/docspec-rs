//! Disjoint-set union-find, bipartite graph topology construction, and affected-component closure.

use super::{
    CONTRIBUTOR, compute_document_id, eval::extract_strings, invalid, lineage::build_chunk_bounded,
    qualify,
};
use crate::{
    budget::Budget, error::Result, model::ConnectedComponentsTransform, model::ConnectedEdgeSpec,
    storage,
};
use arrow::{
    array::{Int64Array, Int64Builder, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

/// Returns whether an edge specification contributes body tokens for `document_id` derivation.
pub(super) fn is_body_edge(edge: &ConnectedEdgeSpec) -> bool {
    edge.namespace.eq_ignore_ascii_case("body")
        || edge.namespace.eq_ignore_ascii_case("bodies")
        || edge.column.eq_ignore_ascii_case("body_key")
}

/// Union-find (disjoint-set) forest with path halving and union by rank.
struct DisjointSet {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl DisjointSet {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn push(&mut self) -> usize {
        let idx = self.parent.len();
        self.parent.push(idx);
        self.rank.push(0);
        idx
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            let p = self.parent[x];
            self.parent[x] = self.parent[p];
            x = p;
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra] < self.rank[rb] {
            self.parent[ra] = rb;
        } else if self.rank[ra] > self.rank[rb] {
            self.parent[rb] = ra;
        } else {
            self.parent[rb] = ra;
            self.rank[ra] = self.rank[ra].saturating_add(1);
        }
    }
}

/// Lightweight streaming topology over `(row_key, edges..., __docspec_file, __docspec_row)`.
///
/// Groups input occurrences into connected components via shared non-empty `row_key` values and
/// namespaced edge tokens, then assigns each component its deterministic `document_id`.
pub(super) struct GraphTopology {
    pub(super) row_keys: Vec<String>,
    pub(super) occ_files: Vec<String>,
    pub(super) occ_rows: Vec<i64>,
    pub(super) row_tokens: Vec<Vec<(String, String)>>,
    pub(super) components: BTreeMap<String, Vec<usize>>,
    pub(super) occurrence_to_doc: HashMap<(String, i64), String>,
    pub(super) _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GraphTopology {
    /// Scans the projected edge and key columns from `input`, unifies connected occurrences,
    /// and computes each component's `document_id` under the memory `budget`.
    pub(super) async fn from_table(
        context: &SessionContext,
        spec: &ConnectedComponentsTransform,
        input: &str,
        budget: &Budget,
    ) -> Result<Self> {
        qualify(context, spec, input).await?;
        let df = context.table(input).await?;
        let in_schema = df.schema().as_arrow();
        let resolve = |target: &str| -> Result<String> {
            Ok(in_schema.field_with_name(target)?.name().clone())
        };
        let mut proj_owned = vec![
            resolve(&spec.row_key)?,
            storage::OCCURRENCE_FILE.to_owned(),
            storage::OCCURRENCE_ROW.to_owned(),
        ];
        for edge in &spec.edges {
            let name = resolve(&edge.column)?;
            if !proj_owned.iter().any(|c| c == &name) {
                proj_owned.push(name);
            }
        }
        let proj_refs: Vec<&str> = proj_owned.iter().map(String::as_str).collect();
        let mut stream = df.select_columns(&proj_refs)?.execute_stream().await?;

        let mut row_keys: Vec<String> = Vec::new();
        let mut occ_files: Vec<String> = Vec::new();
        let mut occ_rows: Vec<i64> = Vec::new();
        let mut row_tokens: Vec<Vec<(String, String)>> = Vec::new();
        let mut row_body_tokens: Vec<Vec<String>> = Vec::new();
        let mut dsu = DisjointSet::new(0);
        let mut key_owner: HashMap<String, usize> = HashMap::new();
        let mut token_owner: HashMap<(String, String), usize> = HashMap::new();
        let mut retained_bytes = 0usize;
        let mut reserved_bytes = 0usize;
        let mut permit: Option<tokio::sync::OwnedSemaphorePermit> = None;

        while let Some(batch) = stream.try_next().await? {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let row_key_col = batch
                .column_by_name(&spec.row_key)
                .ok_or_else(|| {
                    invalid("connected_components input batch is missing row_key column")
                })?
                .clone();
            let occ_file_col = batch
                .column_by_name(storage::OCCURRENCE_FILE)
                .ok_or_else(|| {
                    invalid("connected_components input batch is missing occurrence file column")
                })?
                .clone();
            let occ_row_col = batch
                .column_by_name(storage::OCCURRENCE_ROW)
                .ok_or_else(|| {
                    invalid("connected_components input batch is missing occurrence row column")
                })?
                .clone();
            let occ_file_arr = occ_file_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    invalid("connected_components occurrence file column must be Utf8")
                })?;
            let occ_row_arr = occ_row_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    invalid("connected_components occurrence row column must be Int64")
                })?;

            let mut edge_cols = Vec::with_capacity(spec.edges.len());
            for edge in &spec.edges {
                let col = batch
                    .column_by_name(&edge.column)
                    .ok_or_else(|| {
                        invalid("connected_components input batch is missing edge column")
                    })?
                    .clone();
                edge_cols.push((edge.namespace.to_ascii_lowercase(), is_body_edge(edge), col));
            }

            for i in 0..n {
                let global_idx = dsu.push();
                let rk = extract_strings(&row_key_col, i)
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                let file = occ_file_arr.value(i).to_owned();
                let row_num = occ_row_arr.value(i);
                retained_bytes = retained_bytes
                    .saturating_add(rk.len())
                    .saturating_add(file.len())
                    .saturating_add(32);

                if !rk.is_empty() {
                    let prev = *key_owner.entry(rk.clone()).or_insert(global_idx);
                    dsu.union(global_idx, prev);
                }

                let mut toks = Vec::new();
                let mut bodies = Vec::new();
                for (ns_lower, is_body, col) in &edge_cols {
                    for val in extract_strings(col, i) {
                        if val.is_empty() {
                            continue;
                        }
                        retained_bytes = retained_bytes
                            .saturating_add(ns_lower.len())
                            .saturating_add(val.len())
                            .saturating_add(16);
                        if *is_body {
                            bodies.push(val.clone());
                        }
                        let tok = (ns_lower.clone(), val);
                        let prev = *token_owner.entry(tok.clone()).or_insert(global_idx);
                        dsu.union(global_idx, prev);
                        toks.push(tok);
                    }
                }

                row_keys.push(rk);
                occ_files.push(file);
                occ_rows.push(row_num);
                row_tokens.push(toks);
                row_body_tokens.push(bodies);
            }

            let delta = retained_bytes.saturating_sub(reserved_bytes);
            if delta > 0 {
                let extra = budget.reserve(delta)?;
                reserved_bytes = reserved_bytes.saturating_add(delta);
                match &mut permit {
                    Some(existing) => existing.merge(extra),
                    None => permit = Some(extra),
                }
            }
        }

        let total_rows = row_keys.len();
        let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for i in 0..total_rows {
            by_root.entry(dsu.find(i)).or_default().push(i);
        }

        let mut components: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (_, rows) in by_root {
            let mut bodies = BTreeSet::new();
            let mut sources = BTreeSet::new();
            for &r in &rows {
                for b in &row_body_tokens[r] {
                    bodies.insert(b.clone());
                }
                if !row_keys[r].is_empty() {
                    sources.insert(row_keys[r].clone());
                }
            }
            if bodies.is_empty() && sources.is_empty() {
                for &r in &rows {
                    sources.insert(format!("{}:{}", occ_files[r], occ_rows[r]));
                }
            }
            let doc_id = compute_document_id(&bodies, &sources);
            components.entry(doc_id).or_default().extend(rows);
        }

        let mut occurrence_to_doc = HashMap::with_capacity(total_rows);
        for (doc_id, rows) in &components {
            for &r in rows {
                occurrence_to_doc.insert((occ_files[r].clone(), occ_rows[r]), doc_id.clone());
            }
        }

        Ok(Self {
            row_keys,
            occ_files,
            occ_rows,
            row_tokens,
            components,
            occurrence_to_doc,
            _permit: permit,
        })
    }

    /// Materializes contributor-evidence batches `(document_id, __docspec_source_file, __docspec_source_row)`
    /// for all components or the subset in `selected_doc_ids`, splitting oversized batches to respect `budget`.
    pub(super) fn materialize_evidence_batches(
        &self,
        spec: &ConnectedComponentsTransform,
        selected_doc_ids: Option<&BTreeSet<String>>,
        budget: &Budget,
    ) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(&spec.document_id_column, DataType::Utf8, true),
            Field::new(CONTRIBUTOR[0], DataType::Utf8, false),
            Field::new(CONTRIBUTOR[1], DataType::Int64, false),
        ]));
        let mut entries: Vec<(&str, &str, i64)> = Vec::new();
        for (doc_id, rows) in &self.components {
            if selected_doc_ids.is_some_and(|set| !set.contains(doc_id)) {
                continue;
            }
            for &r in rows {
                entries.push((
                    doc_id.as_str(),
                    self.occ_files[r].as_str(),
                    self.occ_rows[r],
                ));
            }
        }
        if entries.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }
        let mut out = Vec::new();
        let max_rows = budget.limits.batch_rows.max(1);
        let max_bytes = budget.limits.max_batch_bytes;
        let build_batch = |chunk: &[(&str, &str, i64)]| -> Result<RecordBatch> {
            let n = chunk.len();
            let mut doc_ids = StringBuilder::with_capacity(n, n * 64);
            let mut files = StringBuilder::with_capacity(n, n * 32);
            let mut rows = Int64Builder::with_capacity(n);
            for &(doc_id, file, row) in chunk {
                doc_ids.append_value(doc_id);
                files.append_value(file);
                rows.append_value(row);
            }
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(doc_ids.finish()),
                    Arc::new(files.finish()),
                    Arc::new(rows.finish()),
                ],
            )?)
        };
        for chunk in entries.chunks(max_rows) {
            build_chunk_bounded(chunk, max_bytes, &mut out, &build_batch)?;
        }
        Ok(out)
    }
}

/// Inverted index over a [`GraphTopology`] mapping `row_key`s and edge tokens to `document_id`s
/// and vice versa for bipartite breadth-first closure expansion.
struct GraphIndex {
    by_rk: HashMap<String, Vec<String>>,
    by_tok: HashMap<(String, String), Vec<String>>,
    comp_rks: HashMap<String, Vec<String>>,
    comp_toks: HashMap<String, Vec<(String, String)>>,
    empty_rk_docs: Vec<String>,
}

impl GraphIndex {
    fn from_topology(topo: &GraphTopology) -> Self {
        let mut by_rk: HashMap<String, Vec<String>> = HashMap::new();
        let mut by_tok: HashMap<(String, String), Vec<String>> = HashMap::new();
        let mut comp_rks: HashMap<String, Vec<String>> = HashMap::new();
        let mut comp_toks: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut empty_rk_docs: Vec<String> = Vec::new();

        for (doc_id, rows) in &topo.components {
            for &r in rows {
                let rk = &topo.row_keys[r];
                if !rk.is_empty() {
                    by_rk.entry(rk.clone()).or_default().push(doc_id.clone());
                    comp_rks.entry(doc_id.clone()).or_default().push(rk.clone());
                } else {
                    empty_rk_docs.push(doc_id.clone());
                }
                for tok in &topo.row_tokens[r] {
                    by_tok.entry(tok.clone()).or_default().push(doc_id.clone());
                    comp_toks
                        .entry(doc_id.clone())
                        .or_default()
                        .push(tok.clone());
                }
            }
        }
        Self {
            by_rk,
            by_tok,
            comp_rks,
            comp_toks,
            empty_rk_docs,
        }
    }

    /// Marks `docs` as visited in `visited_docs` and enqueues any newly discovered `row_key`s
    /// and edge tokens belonging to those components into `rk_queue` and `tok_queue`.
    fn expand_docs(
        &self,
        docs: &[String],
        visited_docs: &mut BTreeSet<String>,
        visited_rks: &mut HashSet<String>,
        visited_toks: &mut HashSet<(String, String)>,
        rk_queue: &mut VecDeque<String>,
        tok_queue: &mut VecDeque<(String, String)>,
    ) {
        for doc in docs {
            if visited_docs.insert(doc.clone()) {
                if let Some(rks) = self.comp_rks.get(doc) {
                    for nrk in rks {
                        if visited_rks.insert(nrk.clone()) {
                            rk_queue.push_back(nrk.clone());
                        }
                    }
                }
                if let Some(toks) = self.comp_toks.get(doc) {
                    for ntok in toks {
                        if visited_toks.insert(ntok.clone()) {
                            tok_queue.push_back(ntok.clone());
                        }
                    }
                }
            }
        }
    }
}

/// Computes the transitive closure of affected `document_id`s in `old_topo` and `new_topo`
/// starting from `seed_row_keys`.
///
/// Because a component merge or split can propagate across both the old and new topologies via
/// shared `row_key`s or shared edge tokens, BFS alternates between component sets in both graphs
/// until no new keys, tokens, or `document_id`s are reachable.
pub(super) fn affected_component_closure(
    old_topo: &GraphTopology,
    new_topo: &GraphTopology,
    seed_row_keys: &HashSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let old_index = GraphIndex::from_topology(old_topo);
    let new_index = GraphIndex::from_topology(new_topo);

    let mut visited_rks: HashSet<String> = HashSet::new();
    let mut visited_toks: HashSet<(String, String)> = HashSet::new();
    let mut old_docs: BTreeSet<String> = BTreeSet::new();
    let mut new_docs: BTreeSet<String> = BTreeSet::new();

    let mut rk_queue: VecDeque<String> = VecDeque::new();
    let mut tok_queue: VecDeque<(String, String)> = VecDeque::new();

    for rk in seed_row_keys {
        if rk.is_empty() {
            old_index.expand_docs(
                &old_index.empty_rk_docs,
                &mut old_docs,
                &mut visited_rks,
                &mut visited_toks,
                &mut rk_queue,
                &mut tok_queue,
            );
            new_index.expand_docs(
                &new_index.empty_rk_docs,
                &mut new_docs,
                &mut visited_rks,
                &mut visited_toks,
                &mut rk_queue,
                &mut tok_queue,
            );
        } else if visited_rks.insert(rk.clone()) {
            rk_queue.push_back(rk.clone());
        }
    }

    while !rk_queue.is_empty() || !tok_queue.is_empty() {
        while let Some(rk) = rk_queue.pop_front() {
            if let Some(docs) = old_index.by_rk.get(&rk) {
                old_index.expand_docs(
                    docs,
                    &mut old_docs,
                    &mut visited_rks,
                    &mut visited_toks,
                    &mut rk_queue,
                    &mut tok_queue,
                );
            }
            if let Some(docs) = new_index.by_rk.get(&rk) {
                new_index.expand_docs(
                    docs,
                    &mut new_docs,
                    &mut visited_rks,
                    &mut visited_toks,
                    &mut rk_queue,
                    &mut tok_queue,
                );
            }
        }
        while let Some(tok) = tok_queue.pop_front() {
            if let Some(docs) = old_index.by_tok.get(&tok) {
                old_index.expand_docs(
                    docs,
                    &mut old_docs,
                    &mut visited_rks,
                    &mut visited_toks,
                    &mut rk_queue,
                    &mut tok_queue,
                );
            }
            if let Some(docs) = new_index.by_tok.get(&tok) {
                new_index.expand_docs(
                    docs,
                    &mut new_docs,
                    &mut visited_rks,
                    &mut visited_toks,
                    &mut rk_queue,
                    &mut tok_queue,
                );
            }
        }
    }

    (old_docs, new_docs)
}
