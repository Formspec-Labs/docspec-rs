//! Bipartite connected-components clustering and representative aggregation.

use super::{
    aggregation::{CONTRIBUTOR, argument},
    incremental::source,
    refresh::LoadedInputs,
    write::Replacement,
    *,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Builder, Decimal128Builder,
        Int32Array, Int32Builder, Int64Array, Int64Builder, LargeListArray, LargeStringArray,
        ListArray, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
};
use write::identifier as quote;

/// Pure-Rust FIPS 180-4 SHA-256 lowercase hex digest.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((data.len() + 9).div_ceil(64) * 64);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = String::with_capacity(64);
    for word in h {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{word:08x}");
    }
    out
}

/// Computes the deterministic 64-char SHA-256 hex `document_id` for a component.
pub(crate) fn compute_document_id(bodies: &BTreeSet<String>, sources: &BTreeSet<String>) -> String {
    let json = if !bodies.is_empty() {
        let list: Vec<&str> = bodies.iter().map(String::as_str).collect();
        serde_json::to_vec(&("bodies", list)).expect("tuple serializes")
    } else {
        let list: Vec<&str> = sources.iter().map(String::as_str).collect();
        serde_json::to_vec(&("sources", list)).expect("tuple serializes")
    };
    sha256_hex(&json)
}

pub(super) fn validate(spec: &ConnectedComponentsTransform) -> Result<()> {
    let valid = |name: &str| {
        !name.trim().is_empty() && !name.to_ascii_lowercase().starts_with("__docspec_")
    };
    if !valid(&spec.row_key)
        || !valid(&spec.document_id_column)
        || spec.edges.is_empty()
        || spec.aggregates.is_empty()
    {
        return Err(Error::refusal(
            "invalid_refresh",
            "connected_components requires row_key, document_id_column, edges, and aggregates",
        ));
    }
    let mut edge_pairs = BTreeSet::new();
    for edge in &spec.edges {
        if edge.namespace.trim().is_empty()
            || !valid(&edge.column)
            || !edge_pairs.insert((
                edge.namespace.to_ascii_lowercase(),
                edge.column.to_ascii_lowercase(),
            ))
        {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components edges require distinct non-empty namespaces and columns",
            ));
        }
    }
    let mut order_cols = BTreeSet::new();
    for sort in &spec.order_by {
        if !valid(&sort.field) || !order_cols.insert(sort.field.to_ascii_lowercase()) {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components order_by columns must be distinct and non-empty",
            ));
        }
    }
    let mut output_names = BTreeSet::new();
    output_names.insert(spec.document_id_column.to_ascii_lowercase());
    for agg in &spec.aggregates {
        if !valid(&agg.name)
            || !output_names.insert(agg.name.to_ascii_lowercase())
            || argument(&agg.function).is_some_and(|arg| !valid(arg))
        {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components output fields and aggregate operands must be valid and distinct",
            ));
        }
    }
    Ok(())
}

pub(super) async fn register_input(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    releases: &BTreeMap<Id, &ReleasePin>,
    budget: &Budget,
) -> Result<()> {
    let release = releases.get(&spec.input.dataset).ok_or_else(invalid)?;
    if release.version_ref() != spec.input {
        return Err(invalid());
    }
    let src = source(release)?;
    if src.rows > i64::MAX as u64 {
        return Err(Error::refusal(
            "aggregate_range",
            "connected_components requires at most i64::MAX verified input rows",
        ));
    }
    let _ = context.deregister_table("cc_input");
    context.register_table(
        "cc_input",
        crate::query::occurrences(
            storage::open_pin(src, budget).await?,
            budget.clone(),
            Some(src.rows),
        )?,
    )?;
    qualify(context, spec, "cc_input").await
}

async fn qualify(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
) -> Result<()> {
    validate(spec)?;
    let frame = context.table(input).await?;
    let schema = frame.schema().as_arrow();
    let row_key_dt = schema.field_with_name(&spec.row_key)?.data_type();
    if !matches!(row_key_dt, DataType::Utf8 | DataType::LargeUtf8) {
        return Err(Error::refusal(
            "invalid_refresh",
            "connected_components row_key column must be Utf8",
        ));
    }
    for edge in &spec.edges {
        let dt = schema.field_with_name(&edge.column)?.data_type();
        let valid_edge = matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
            || matches!(
                dt,
                DataType::List(f) | DataType::LargeList(f)
                    if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8)
            );
        if !valid_edge {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components edge columns must be Utf8 or List<Utf8>",
            ));
        }
    }
    for sort in &spec.order_by {
        let dt = schema.field_with_name(&sort.field)?.data_type();
        if !is_supported_scalar(dt) {
            return Err(Error::refusal(
                "invalid_refresh",
                "connected_components order_by column must be a supported scalar type",
            ));
        }
    }
    for agg in &spec.aggregates {
        if let Some(field) = argument(&agg.function) {
            let dt = schema.field_with_name(field)?.data_type();
            let valid = match &agg.function {
                AggregateFunction::CountRows => true,
                AggregateFunction::Count { .. } | AggregateFunction::CountDistinct { .. } => {
                    is_supported_scalar(dt)
                }
                AggregateFunction::FirstValue { .. } => is_supported_scalar(dt),
                AggregateFunction::CollectSet { .. } => {
                    is_supported_scalar(dt)
                        || matches!(
                            dt,
                            DataType::List(f) | DataType::LargeList(f)
                                if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                        )
                }
                AggregateFunction::Sum { .. } => matches!(dt, DataType::Int32 | DataType::Int64),
                AggregateFunction::Min { .. } | AggregateFunction::Max { .. } => {
                    is_supported_scalar(dt)
                }
                AggregateFunction::FirstValueBy { .. }
                | AggregateFunction::CollectDistinctWithProvenance { .. } => false,
            };
            if !valid {
                return Err(Error::refusal(
                    "aggregate_value_type",
                    "connected_components aggregate operand has an unsupported data type",
                ));
            }
        }
    }
    Ok(())
}

fn is_supported_scalar(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ScalarVal {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Date32(i32),
    Timestamp(i64),
    Utf8(String),
}

fn extract_scalar(col: &ArrayRef, row: usize) -> Option<ScalarVal> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| ScalarVal::Bool(a.value(row))),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| ScalarVal::Int32(a.value(row))),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| ScalarVal::Int64(a.value(row))),
        DataType::Date32 => col
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .map(|a| ScalarVal::Date32(a.value(row))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| ScalarVal::Timestamp(a.value(row))),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| ScalarVal::Utf8(a.value(row).to_owned())),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| ScalarVal::Utf8(a.value(row).to_owned())),
        _ => None,
    }
}

fn extract_strings(col: &ArrayRef, row: usize) -> Vec<String> {
    if col.is_null(row) {
        return Vec::new();
    }
    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| {
                let s = a.value(row);
                if s.is_empty() {
                    Vec::new()
                } else {
                    vec![s.to_owned()]
                }
            })
            .unwrap_or_default(),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| {
                let s = a.value(row);
                if s.is_empty() {
                    Vec::new()
                } else {
                    vec![s.to_owned()]
                }
            })
            .unwrap_or_default(),
        DataType::List(_) => col
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|list| {
                let values = list.value(row);
                let mut out = Vec::new();
                if let Some(sa) = values.as_any().downcast_ref::<StringArray>() {
                    for i in 0..sa.len() {
                        if !sa.is_null(i) && !sa.value(i).is_empty() {
                            out.push(sa.value(i).to_owned());
                        }
                    }
                } else if let Some(sa) = values.as_any().downcast_ref::<LargeStringArray>() {
                    for i in 0..sa.len() {
                        if !sa.is_null(i) && !sa.value(i).is_empty() {
                            out.push(sa.value(i).to_owned());
                        }
                    }
                }
                out
            })
            .unwrap_or_default(),
        DataType::LargeList(_) => col
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|list| {
                let values = list.value(row);
                let mut out = Vec::new();
                if let Some(sa) = values.as_any().downcast_ref::<StringArray>() {
                    for i in 0..sa.len() {
                        if !sa.is_null(i) && !sa.value(i).is_empty() {
                            out.push(sa.value(i).to_owned());
                        }
                    }
                } else if let Some(sa) = values.as_any().downcast_ref::<LargeStringArray>() {
                    for i in 0..sa.len() {
                        if !sa.is_null(i) && !sa.value(i).is_empty() {
                            out.push(sa.value(i).to_owned());
                        }
                    }
                }
                out
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn is_body_edge(edge: &ConnectedEdgeSpec) -> bool {
    edge.namespace.eq_ignore_ascii_case("body")
        || edge.namespace.eq_ignore_ascii_case("bodies")
        || edge.column.eq_ignore_ascii_case("body_key")
}

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
struct GraphTopology {
    row_keys: Vec<String>,
    occ_files: Vec<String>,
    occ_rows: Vec<i64>,
    row_tokens: Vec<Vec<(String, String)>>,
    components: BTreeMap<String, Vec<usize>>,
    occurrence_to_doc: HashMap<(String, i64), String>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GraphTopology {
    async fn from_table(
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
                .ok_or_else(invalid)?
                .clone();
            let occ_file_col = batch
                .column_by_name(storage::OCCURRENCE_FILE)
                .ok_or_else(invalid)?
                .clone();
            let occ_row_col = batch
                .column_by_name(storage::OCCURRENCE_ROW)
                .ok_or_else(invalid)?
                .clone();
            let occ_file_arr = occ_file_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid)?;
            let occ_row_arr = occ_row_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid)?;

            let mut edge_cols = Vec::with_capacity(spec.edges.len());
            for edge in &spec.edges {
                let col = batch
                    .column_by_name(&edge.column)
                    .ok_or_else(invalid)?
                    .clone();
                edge_cols.push((
                    edge.namespace.to_ascii_lowercase(),
                    is_body_edge(edge),
                    col,
                ));
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

    fn materialize_evidence_batches(
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
                entries.push((doc_id.as_str(), self.occ_files[r].as_str(), self.occ_rows[r]));
            }
        }
        if entries.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }
        let mut out = Vec::new();
        let max_rows = budget.limits.batch_rows.max(1);
        let max_bytes = budget.limits.max_batch_bytes;
        for chunk in entries.chunks(max_rows) {
            build_evidence_chunk_bounded(&schema, chunk, max_bytes, &mut out)?;
        }
        Ok(out)
    }
}

fn build_evidence_chunk_bounded(
    schema: &SchemaRef,
    chunk: &[(&str, &str, i64)],
    max_bytes: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    let n = chunk.len();
    let mut doc_ids = StringBuilder::with_capacity(n, n * 64);
    let mut files = StringBuilder::with_capacity(n, n * 32);
    let mut rows = Int64Builder::with_capacity(n);
    for &(doc_id, file, row) in chunk {
        doc_ids.append_value(doc_id);
        files.append_value(file);
        rows.append_value(row);
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(doc_ids.finish()),
            Arc::new(files.finish()),
            Arc::new(rows.finish()),
        ],
    )?;
    if batch.get_array_memory_size() > max_bytes && n > 1 {
        let mid = n / 2;
        build_evidence_chunk_bounded(schema, &chunk[..mid], max_bytes, out)?;
        build_evidence_chunk_bounded(schema, &chunk[mid..], max_bytes, out)?;
    } else {
        out.push(batch);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct RowPos {
    batch: usize,
    row: usize,
}

struct EvaluatedComponent {
    document_id: String,
    rows: Vec<RowPos>,
}

struct EvaluatedGraph {
    in_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    occ_files: Vec<Vec<String>>,
    occ_rows: Vec<Vec<i64>>,
    components: BTreeMap<String, EvaluatedComponent>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl EvaluatedGraph {
    async fn from_topology(
        topo: GraphTopology,
        selected_doc_ids: Option<&BTreeSet<String>>,
        context: &SessionContext,
        spec: &ConnectedComponentsTransform,
        input: &str,
        budget: &Budget,
    ) -> Result<Self> {
        let df = context.table(input).await?;
        let in_schema = Arc::new(df.schema().as_arrow().clone());
        if selected_doc_ids.is_some_and(BTreeSet::is_empty) || topo.components.is_empty() {
            return Ok(Self {
                in_schema,
                batches: Vec::new(),
                occ_files: Vec::new(),
                occ_rows: Vec::new(),
                components: BTreeMap::new(),
                _permit: None,
            });
        }

        let resolve = |target: &str| -> Result<String> {
            Ok(in_schema.field_with_name(target)?.name().clone())
        };
        let mut proj_owned = vec![
            resolve(&spec.row_key)?,
            storage::OCCURRENCE_FILE.to_owned(),
            storage::OCCURRENCE_ROW.to_owned(),
        ];
        for sort in &spec.order_by {
            let name = resolve(&sort.field)?;
            if !proj_owned.iter().any(|c| c == &name) {
                proj_owned.push(name);
            }
        }
        for agg in &spec.aggregates {
            if let Some(field) = argument(&agg.function) {
                let name = resolve(field)?;
                if !proj_owned.iter().any(|c| c == &name) {
                    proj_owned.push(name);
                }
            }
        }
        let proj_refs: Vec<&str> = proj_owned.iter().map(String::as_str).collect();
        let mut stream = df.select_columns(&proj_refs)?.execute_stream().await?;

        let mut components: BTreeMap<String, EvaluatedComponent> = BTreeMap::new();
        for doc_id in topo.components.keys() {
            if selected_doc_ids.is_none_or(|set| set.contains(doc_id)) {
                components.insert(
                    doc_id.clone(),
                    EvaluatedComponent {
                        document_id: doc_id.clone(),
                        rows: Vec::new(),
                    },
                );
            }
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut occ_files: Vec<Vec<String>> = Vec::new();
        let mut occ_rows: Vec<Vec<i64>> = Vec::new();
        let mut retained_bytes = 0usize;
        let mut reserved_bytes = 0usize;
        let mut permit: Option<tokio::sync::OwnedSemaphorePermit> = topo._permit;

        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = if let Some(doc_set) = selected_doc_ids {
                let occ_file_arr = batch
                    .column_by_name(storage::OCCURRENCE_FILE)
                    .ok_or_else(invalid)?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(invalid)?;
                let occ_row_arr = batch
                    .column_by_name(storage::OCCURRENCE_ROW)
                    .ok_or_else(invalid)?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(invalid)?;
                let mut any_match = false;
                let mut all_match = true;
                let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
                for i in 0..batch.num_rows() {
                    let key = (occ_file_arr.value(i).to_owned(), occ_row_arr.value(i));
                    let matched = topo
                        .occurrence_to_doc
                        .get(&key)
                        .is_some_and(|doc_id| doc_set.contains(doc_id));
                    any_match |= matched;
                    all_match &= matched;
                    mask_builder.append_value(matched);
                }
                if !any_match {
                    continue;
                }
                if all_match {
                    batch
                } else {
                    arrow::compute::filter_record_batch(&batch, &mask_builder.finish())?
                }
            } else {
                batch
            };

            let batch_idx = batches.len();
            let occ_file_arr = batch
                .column_by_name(storage::OCCURRENCE_FILE)
                .ok_or_else(invalid)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid)?;
            let occ_row_arr = batch
                .column_by_name(storage::OCCURRENCE_ROW)
                .ok_or_else(invalid)?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid)?;

            let mut b_files = Vec::with_capacity(batch.num_rows());
            let mut b_rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let file = occ_file_arr.value(row).to_owned();
                let row_num = occ_row_arr.value(row);
                if let Some(doc_id) = topo.occurrence_to_doc.get(&(file.clone(), row_num))
                    && let Some(comp) = components.get_mut(doc_id)
                {
                    comp.rows.push(RowPos {
                        batch: batch_idx,
                        row,
                    });
                }
                b_files.push(file);
                b_rows.push(row_num);
            }

            retained_bytes = retained_bytes.saturating_add(batch.get_array_memory_size());
            let delta = retained_bytes.saturating_sub(reserved_bytes);
            if delta > 0 {
                let extra = budget.reserve(delta)?;
                reserved_bytes = reserved_bytes.saturating_add(delta);
                match &mut permit {
                    Some(existing) => existing.merge(extra),
                    None => permit = Some(extra),
                }
            }
            occ_files.push(b_files);
            occ_rows.push(b_rows);
            batches.push(batch);
        }

        if let Some(first_batch) = batches.first() {
            let b_schema = first_batch.schema();
            let rk_idx = b_schema.index_of(&resolve(&spec.row_key)?)?;
            let mut order_indices = Vec::with_capacity(spec.order_by.len());
            for sort in &spec.order_by {
                order_indices.push((b_schema.index_of(&resolve(&sort.field)?)?, sort.direction));
            }
            for comp in components.values_mut() {
                comp.rows.sort_by(|&a, &b| {
                    for &(col_idx, dir) in &order_indices {
                        let va = extract_scalar(batches[a.batch].column(col_idx), a.row);
                        let vb = extract_scalar(batches[b.batch].column(col_idx), b.row);
                        let ord = match (va, vb) {
                            (None, None) => Ordering::Equal,
                            // NULLS LAST in representative selection (`(col IS NULL) ASC`).
                            (Some(_), None) => Ordering::Less,
                            (None, Some(_)) => Ordering::Greater,
                            (Some(xa), Some(xb)) => match dir {
                                SortDirection::Asc => xa.cmp(&xb),
                                SortDirection::Desc => xb.cmp(&xa),
                            },
                        };
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    let rka = extract_strings(batches[a.batch].column(rk_idx), a.row)
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    let rkb = extract_strings(batches[b.batch].column(rk_idx), b.row)
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    rka.cmp(&rkb)
                        .then_with(|| occ_files[a.batch][a.row].cmp(&occ_files[b.batch][b.row]))
                        .then_with(|| occ_rows[a.batch][a.row].cmp(&occ_rows[b.batch][b.row]))
                });
            }
        }

        Ok(Self {
            in_schema,
            batches,
            occ_files,
            occ_rows,
            components,
            _permit: permit,
        })
    }

    fn col(&self, pos: RowPos, name: &str) -> Result<&ArrayRef> {
        self.batches[pos.batch]
            .column_by_name(name)
            .ok_or_else(invalid)
    }

    fn materialize_components_batches(
        &self,
        spec: &ConnectedComponentsTransform,
        selected_doc_ids: Option<&BTreeSet<String>>,
        budget: &Budget,
    ) -> Result<Vec<RecordBatch>> {
        let out_schema = output_schema(&self.in_schema, spec)?;
        let selected: Vec<&EvaluatedComponent> = self
            .components
            .values()
            .filter(|c| selected_doc_ids.is_none_or(|set| set.contains(&c.document_id)))
            .collect();
        if selected.is_empty() {
            return Ok(vec![RecordBatch::new_empty(out_schema)]);
        }
        let mut out = Vec::new();
        let max_rows = budget.limits.batch_rows.max(1);
        let max_bytes = budget.limits.max_batch_bytes;
        for chunk in selected.chunks(max_rows) {
            self.build_component_chunk_bounded(spec, &out_schema, chunk, max_bytes, &mut out)?;
        }
        Ok(out)
    }

    fn build_component_chunk_bounded(
        &self,
        spec: &ConnectedComponentsTransform,
        out_schema: &SchemaRef,
        selected: &[&EvaluatedComponent],
        max_bytes: usize,
        out: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        let batch = self.build_component_batch(spec, out_schema, selected)?;
        if batch.get_array_memory_size() > max_bytes && selected.len() > 1 {
            let mid = selected.len() / 2;
            self.build_component_chunk_bounded(
                spec,
                out_schema,
                &selected[..mid],
                max_bytes,
                out,
            )?;
            self.build_component_chunk_bounded(
                spec,
                out_schema,
                &selected[mid..],
                max_bytes,
                out,
            )?;
        } else {
            out.push(batch);
        }
        Ok(())
    }

    fn build_component_batch(
        &self,
        spec: &ConnectedComponentsTransform,
        out_schema: &SchemaRef,
        selected: &[&EvaluatedComponent],
    ) -> Result<RecordBatch> {
        let num_rows = selected.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + spec.aggregates.len());
        let mut doc_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        for comp in selected {
            doc_id_builder.append_value(&comp.document_id);
        }
        columns.push(Arc::new(doc_id_builder.finish()));

        for agg in &spec.aggregates {
            let col: ArrayRef = match &agg.function {
                AggregateFunction::CountRows => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        b.append_value(comp.rows.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Count { field } => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        let mut count = 0i64;
                        for &pos in &comp.rows {
                            if !self.col(pos, field)?.is_null(pos.row) {
                                count += 1;
                            }
                        }
                        b.append_value(count);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::CountDistinct { field } => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in selected {
                        let mut set: BTreeSet<ScalarVal> = BTreeSet::new();
                        for &pos in &comp.rows {
                            if let Some(val) = extract_scalar(self.col(pos, field)?, pos.row) {
                                set.insert(val);
                            }
                        }
                        b.append_value(set.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Sum { field } => {
                    let mut b = Decimal128Builder::with_capacity(num_rows)
                        .with_data_type(DataType::Decimal128(38, 0));
                    for comp in selected {
                        let mut sum = 0i128;
                        let mut any = false;
                        for &pos in &comp.rows {
                            match extract_scalar(self.col(pos, field)?, pos.row) {
                                Some(ScalarVal::Int32(v)) => {
                                    sum += i128::from(v);
                                    any = true;
                                }
                                Some(ScalarVal::Int64(v)) => {
                                    sum += i128::from(v);
                                    any = true;
                                }
                                _ => {}
                            }
                        }
                        if any {
                            b.append_value(sum);
                        } else {
                            b.append_null();
                        }
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Min { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let mut min_val: Option<ScalarVal> = None;
                        for &pos in &comp.rows {
                            if let Some(v) = extract_scalar(self.col(pos, field)?, pos.row) {
                                min_val = Some(match min_val {
                                    Some(cur) if cur <= v => cur,
                                    _ => v,
                                });
                            }
                        }
                        vals.push(min_val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::Max { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let mut max_val: Option<ScalarVal> = None;
                        for &pos in &comp.rows {
                            if let Some(v) = extract_scalar(self.col(pos, field)?, pos.row) {
                                max_val = Some(match max_val {
                                    Some(cur) if cur >= v => cur,
                                    _ => v,
                                });
                            }
                        }
                        vals.push(max_val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::FirstValue { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    let mut vals = Vec::with_capacity(num_rows);
                    for comp in selected {
                        let val = if let Some(&pos) = comp.rows.first() {
                            extract_scalar(self.col(pos, field)?, pos.row)
                        } else {
                            None
                        };
                        vals.push(val);
                    }
                    build_scalar_array(&vals, src_dt)?
                }
                AggregateFunction::CollectSet { field } => {
                    let src_dt = self.in_schema.field_with_name(field)?.data_type();
                    self.build_collect_set_array(selected, field, src_dt)?
                }
                AggregateFunction::FirstValueBy { .. }
                | AggregateFunction::CollectDistinctWithProvenance { .. } => {
                    return Err(invalid());
                }
            };
            columns.push(col);
        }

        Ok(RecordBatch::try_new(out_schema.clone(), columns)?)
    }

    fn build_collect_set_array(
        &self,
        selected: &[&EvaluatedComponent],
        field: &str,
        src_dt: &DataType,
    ) -> Result<ArrayRef> {
        let n = selected.len();
        match src_dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::List(_) | DataType::LargeList(_) => {
                let mut builder = ListBuilder::with_capacity(StringBuilder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        for s in extract_strings(self.col(pos, field)?, pos.row) {
                            set.insert(s);
                        }
                    }
                    for item in set {
                        builder.values().append_value(&item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = ListBuilder::with_capacity(BooleanBuilder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Bool(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int32 => {
                let mut builder = ListBuilder::with_capacity(Int32Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Int32(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = ListBuilder::with_capacity(Int64Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Int64(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Date32 => {
                let mut builder = ListBuilder::with_capacity(Date32Builder::new(), n);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Date32(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let val_builder = if let Some(zone) = tz {
                    TimestampMicrosecondBuilder::new().with_timezone(zone.clone())
                } else {
                    TimestampMicrosecondBuilder::new()
                };
                let list_field = Arc::new(Field::new_list_field(src_dt.clone(), true));
                let mut builder = ListBuilder::with_capacity(val_builder, n).with_field(list_field);
                for comp in selected {
                    let mut set = BTreeSet::new();
                    for &pos in &comp.rows {
                        if let Some(ScalarVal::Timestamp(v)) =
                            extract_scalar(self.col(pos, field)?, pos.row)
                        {
                            set.insert(v);
                        }
                    }
                    for item in set {
                        builder.values().append_value(item);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(invalid()),
        }
    }

    fn component_signature(
        &self,
        spec: &ConnectedComponentsTransform,
        doc_id: &str,
    ) -> Option<[u8; 32]> {
        let comp = self.components.get(doc_id)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(doc_id.as_bytes());
        hasher.update(&(comp.rows.len() as u64).to_le_bytes());
        if let Some(&rep) = comp.rows.first() {
            for agg in &spec.aggregates {
                if let Some(field) = argument(&agg.function) {
                    match &agg.function {
                        AggregateFunction::FirstValue { .. } => {
                            if let Ok(src) = self.col(rep, field) {
                                let val = extract_scalar(src, rep.row);
                                hasher.update(format!("{val:?}").as_bytes());
                            }
                        }
                        AggregateFunction::CollectSet { .. } => {
                            for &pos in &comp.rows {
                                if let Ok(src) = self.col(pos, field) {
                                    for s in extract_strings(src, pos.row) {
                                        hasher.update(&(s.len() as u64).to_le_bytes());
                                        hasher.update(s.as_bytes());
                                    }
                                    if let Some(val) = extract_scalar(src, pos.row) {
                                        hasher.update(format!("{val:?}").as_bytes());
                                    }
                                }
                            }
                        }
                        _ => {
                            for &pos in &comp.rows {
                                if let Ok(src) = self.col(pos, field) {
                                    let val = extract_scalar(src, pos.row);
                                    hasher.update(format!("{val:?}").as_bytes());
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut contributors: Vec<(&str, i64)> = comp
            .rows
            .iter()
            .map(|&pos| {
                (
                    self.occ_files[pos.batch][pos.row].as_str(),
                    self.occ_rows[pos.batch][pos.row],
                )
            })
            .collect();
        contributors.sort_unstable();
        for (file, row) in contributors {
            hasher.update(&(file.len() as u64).to_le_bytes());
            hasher.update(file.as_bytes());
            hasher.update(&row.to_le_bytes());
        }
        Some(*hasher.finalize().as_bytes())
    }
}

fn output_schema(
    in_schema: &SchemaRef,
    spec: &ConnectedComponentsTransform,
) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(1 + spec.aggregates.len());
    fields.push(Field::new(&spec.document_id_column, DataType::Utf8, true));
    for agg in &spec.aggregates {
        let field = match &agg.function {
            AggregateFunction::CountRows
            | AggregateFunction::Count { .. }
            | AggregateFunction::CountDistinct { .. } => {
                Field::new(&agg.name, DataType::Int64, false)
            }
            AggregateFunction::Sum { .. } => {
                Field::new(&agg.name, DataType::Decimal128(38, 0), true)
            }
            AggregateFunction::Min { field }
            | AggregateFunction::Max { field }
            | AggregateFunction::FirstValue { field } => {
                let src_dt = normalize_output_type(in_schema.field_with_name(field)?.data_type());
                Field::new(&agg.name, src_dt, true)
            }
            AggregateFunction::CollectSet { field } => {
                let src_dt = in_schema.field_with_name(field)?.data_type();
                let item_dt = match src_dt {
                    DataType::List(inner) | DataType::LargeList(inner) => {
                        normalize_output_type(inner.data_type())
                    }
                    other => normalize_output_type(other),
                };
                Field::new(
                    &agg.name,
                    DataType::List(Arc::new(Field::new_list_field(item_dt, true))),
                    false,
                )
            }
            AggregateFunction::FirstValueBy { .. }
            | AggregateFunction::CollectDistinctWithProvenance { .. } => {
                return Err(invalid());
            }
        };
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn normalize_output_type(dt: &DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 => DataType::Utf8,
        other => other.clone(),
    }
}

fn build_scalar_array(vals: &[Option<ScalarVal>], data_type: &DataType) -> Result<ArrayRef> {
    let n = vals.len();
    match data_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Bool(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Int32(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Int64(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Date32(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for v in vals {
                match v {
                    Some(ScalarVal::Timestamp(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            let arr = if let Some(zone) = tz {
                b.finish().with_timezone(zone.clone())
            } else {
                b.finish()
            };
            Ok(Arc::new(arr))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for v in vals {
                match v {
                    Some(ScalarVal::Utf8(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => Err(invalid()),
    }
}

pub(super) async fn source_frame(
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

pub(super) async fn evidence_frame(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    let topo = GraphTopology::from_table(context, spec, input, budget).await?;
    let batches = topo.materialize_evidence_batches(spec, None, budget)?;
    Ok(context.read_batches(batches)?)
}

/// Computes the transitive closure of affected `document_id`s in `old_topo` and `new_topo`
/// starting from `seed_row_keys`.
fn affected_component_closure(
    old_topo: &GraphTopology,
    new_topo: &GraphTopology,
    seed_row_keys: &HashSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    struct GraphIndex {
        by_rk: HashMap<String, Vec<String>>,
        by_tok: HashMap<(String, String), Vec<String>>,
        comp_rks: HashMap<String, Vec<String>>,
        comp_toks: HashMap<String, Vec<(String, String)>>,
        empty_rk_docs: Vec<String>,
    }

    fn index_topo(topo: &GraphTopology) -> GraphIndex {
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
        GraphIndex {
            by_rk,
            by_tok,
            comp_rks,
            comp_toks,
            empty_rk_docs,
        }
    }

    let GraphIndex {
        by_rk: old_by_rk,
        by_tok: old_by_tok,
        comp_rks: old_comp_rks,
        comp_toks: old_comp_toks,
        empty_rk_docs: old_empty_rk_docs,
    } = index_topo(old_topo);
    let GraphIndex {
        by_rk: new_by_rk,
        by_tok: new_by_tok,
        comp_rks: new_comp_rks,
        comp_toks: new_comp_toks,
        empty_rk_docs: new_empty_rk_docs,
    } = index_topo(new_topo);

    let mut visited_rks: HashSet<String> = HashSet::new();
    let mut visited_toks: HashSet<(String, String)> = HashSet::new();
    let mut old_docs: BTreeSet<String> = BTreeSet::new();
    let mut new_docs: BTreeSet<String> = BTreeSet::new();

    let mut rk_queue: VecDeque<String> = VecDeque::new();
    let mut tok_queue: VecDeque<(String, String)> = VecDeque::new();

    for rk in seed_row_keys {
        if rk.is_empty() {
            for doc in &old_empty_rk_docs {
                if old_docs.insert(doc.clone()) {
                    if let Some(rks) = old_comp_rks.get(doc) {
                        for nrk in rks {
                            if visited_rks.insert(nrk.clone()) {
                                rk_queue.push_back(nrk.clone());
                            }
                        }
                    }
                    if let Some(toks) = old_comp_toks.get(doc) {
                        for ntok in toks {
                            if visited_toks.insert(ntok.clone()) {
                                tok_queue.push_back(ntok.clone());
                            }
                        }
                    }
                }
            }
            for doc in &new_empty_rk_docs {
                if new_docs.insert(doc.clone()) {
                    if let Some(rks) = new_comp_rks.get(doc) {
                        for nrk in rks {
                            if visited_rks.insert(nrk.clone()) {
                                rk_queue.push_back(nrk.clone());
                            }
                        }
                    }
                    if let Some(toks) = new_comp_toks.get(doc) {
                        for ntok in toks {
                            if visited_toks.insert(ntok.clone()) {
                                tok_queue.push_back(ntok.clone());
                            }
                        }
                    }
                }
            }
        } else if visited_rks.insert(rk.clone()) {
            rk_queue.push_back(rk.clone());
        }
    }

    while !rk_queue.is_empty() || !tok_queue.is_empty() {
        while let Some(rk) = rk_queue.pop_front() {
            if let Some(docs) = old_by_rk.get(&rk) {
                for doc in docs {
                    if old_docs.insert(doc.clone()) {
                        if let Some(rks) = old_comp_rks.get(doc) {
                            for nrk in rks {
                                if visited_rks.insert(nrk.clone()) {
                                    rk_queue.push_back(nrk.clone());
                                }
                            }
                        }
                        if let Some(toks) = old_comp_toks.get(doc) {
                            for ntok in toks {
                                if visited_toks.insert(ntok.clone()) {
                                    tok_queue.push_back(ntok.clone());
                                }
                            }
                        }
                    }
                }
            }
            if let Some(docs) = new_by_rk.get(&rk) {
                for doc in docs {
                    if new_docs.insert(doc.clone()) {
                        if let Some(rks) = new_comp_rks.get(doc) {
                            for nrk in rks {
                                if visited_rks.insert(nrk.clone()) {
                                    rk_queue.push_back(nrk.clone());
                                }
                            }
                        }
                        if let Some(toks) = new_comp_toks.get(doc) {
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
        while let Some(tok) = tok_queue.pop_front() {
            if let Some(docs) = old_by_tok.get(&tok) {
                for doc in docs {
                    if old_docs.insert(doc.clone()) {
                        if let Some(rks) = old_comp_rks.get(doc) {
                            for nrk in rks {
                                if visited_rks.insert(nrk.clone()) {
                                    rk_queue.push_back(nrk.clone());
                                }
                            }
                        }
                        if let Some(toks) = old_comp_toks.get(doc) {
                            for ntok in toks {
                                if visited_toks.insert(ntok.clone()) {
                                    tok_queue.push_back(ntok.clone());
                                }
                            }
                        }
                    }
                }
            }
            if let Some(docs) = new_by_tok.get(&tok) {
                for doc in docs {
                    if new_docs.insert(doc.clone()) {
                        if let Some(rks) = new_comp_rks.get(doc) {
                            for nrk in rks {
                                if visited_rks.insert(nrk.clone()) {
                                    rk_queue.push_back(nrk.clone());
                                }
                            }
                        }
                        if let Some(toks) = new_comp_toks.get(doc) {
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
    }

    (old_docs, new_docs)
}

fn build_scope_batches(
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
    for chunk in docs.chunks(max_rows) {
        build_scope_chunk_bounded(&schema, chunk, max_bytes, &mut out)?;
    }
    Ok(out)
}

fn build_scope_chunk_bounded(
    schema: &SchemaRef,
    chunk: &[&str],
    max_bytes: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    let n = chunk.len();
    let mut b = StringBuilder::with_capacity(n, n * 64);
    for &doc_id in chunk {
        b.append_value(doc_id);
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(b.finish())])?;
    if batch.get_array_memory_size() > max_bytes && n > 1 {
        let mid = n / 2;
        build_scope_chunk_bounded(schema, &chunk[..mid], max_bytes, out)?;
        build_scope_chunk_bounded(schema, &chunk[mid..], max_bytes, out)?;
    } else {
        out.push(batch);
    }
    Ok(())
}

impl Engine {
    pub(super) async fn process_connected_components(
        &self,
        spec: &RefreshSpec,
        directory: &locations::Location,
        base: &ReleasePin,
        previous: &LoadedInputs,
        current: &LoadedInputs,
        captured: (&TablePin, &iceberg::table::Table),
    ) -> Result<write::Prepared> {
        let (captured_source, captured_native) = captured;
        let RefreshTransform::ConnectedComponents(cc) = &spec.transform else {
            return Err(invalid());
        };
        let budget = &self.inner.lifecycle.budget;
        let before = source(base)?;
        let context = self.context();
        register_input(&context, cc, &current.borrowed(), budget).await?;

        let (affected, scanned) = self
            .changed_keys(
                &spec.dataset,
                &directory.child("affected")?,
                (
                    &previous.releases[&cc.input.dataset],
                    &current.releases[&cc.input.dataset],
                ),
                (
                    previous.canonical_source(&cc.input.dataset)?,
                    current.canonical_source(&cc.input.dataset)?,
                ),
                std::slice::from_ref(&cc.row_key),
            )
            .await?;
        let native_affected = storage::open_pin(&affected, budget).await?;
        let (discovery_files, discovery_bytes) = {
            let inventory = storage::snapshot_files(&native_affected, budget).await?;
            (
                inventory.data.len() as u64,
                inventory
                    .data
                    .iter()
                    .map(|file| file.file_size_in_bytes())
                    .sum::<u64>(),
            )
        };

        let key_names = vec![cc.document_id_column.clone()];
        let (scope_batches, replacement_batches) = if affected.rows == 0 {
            let empty_scope = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                &cc.document_id_column,
                DataType::Utf8,
                true,
            )])));
            let in_schema = Arc::new(
                context
                    .table("cc_input")
                    .await?
                    .schema()
                    .as_arrow()
                    .clone(),
            );
            let empty_rep = RecordBatch::new_empty(output_schema(&in_schema, cc)?);
            (vec![empty_scope], vec![empty_rep])
        } else {
            context.register_table(
                "affected_rks",
                crate::query::table(native_affected, budget.clone(), Some(affected.rows))?,
            )?;
            let mut rk_stream = context
                .sql("SELECT DISTINCT \"k0\" FROM affected_rks")
                .await?
                .execute_stream()
                .await?;
            let mut seed_rks = HashSet::new();
            while let Some(batch) = rk_stream.try_next().await? {
                let col = batch.column(0).clone();
                for i in 0..batch.num_rows() {
                    let vals = extract_strings(&col, i);
                    if vals.is_empty() {
                        seed_rks.insert(String::new());
                    } else {
                        for s in vals {
                            seed_rks.insert(s);
                        }
                    }
                }
            }

            let old_release = previous
                .releases
                .get(&cc.input.dataset)
                .ok_or_else(invalid)?;
            let old_src = source(old_release)?;
            context.register_table(
                "cc_old_input",
                crate::query::occurrences(
                    storage::open_pin(old_src, budget).await?,
                    budget.clone(),
                    Some(old_src.rows),
                )?,
            )?;

            let old_topo =
                GraphTopology::from_table(&context, cc, "cc_old_input", budget).await?;
            let new_topo =
                GraphTopology::from_table(&context, cc, "cc_input", budget).await?;

            let (old_affected_docs, new_affected_docs) =
                affected_component_closure(&old_topo, &new_topo, &seed_rks);

            let old_graph = EvaluatedGraph::from_topology(
                old_topo,
                Some(&old_affected_docs),
                &context,
                cc,
                "cc_old_input",
                budget,
            )
            .await?;
            let mut old_sigs: HashMap<String, [u8; 32]> =
                HashMap::with_capacity(old_affected_docs.len());
            for doc_id in &old_affected_docs {
                if let Some(sig) = old_graph.component_signature(cc, doc_id) {
                    old_sigs.insert(doc_id.clone(), sig);
                }
            }
            drop(old_graph);

            let new_graph = EvaluatedGraph::from_topology(
                new_topo,
                Some(&new_affected_docs),
                &context,
                cc,
                "cc_input",
                budget,
            )
            .await?;

            let mut changed_scope_docs = BTreeSet::new();
            let mut changed_new_docs = BTreeSet::new();

            for doc_id in old_affected_docs.union(&new_affected_docs) {
                let old_sig = old_sigs.get(doc_id).copied();
                let new_sig = new_graph.component_signature(cc, doc_id);
                if old_sig != new_sig {
                    changed_scope_docs.insert(doc_id.clone());
                    if new_sig.is_some() {
                        changed_new_docs.insert(doc_id.clone());
                    }
                }
            }

            let scope_batches =
                build_scope_batches(&cc.document_id_column, &changed_scope_docs, budget)?;
            let replacement_batches =
                new_graph.materialize_components_batches(cc, Some(&changed_new_docs), budget)?;
            (scope_batches, replacement_batches)
        };

        let scope = context.read_batches(scope_batches)?;
        let replacement = context.read_batches(replacement_batches)?;
        let key_field_ids = key_names
            .iter()
            .map(|key| {
                captured_native
                    .metadata()
                    .current_schema()
                    .field_by_name(key)
                    .map(|field| field.id)
                    .ok_or_else(invalid)
            })
            .collect::<Result<Vec<_>>>()?;
        let (write_before, replacement) = self
            .source_replacement(&spec.dataset, captured, directory, replacement, &key_names)
            .await?;
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: directory.child("replacement")?,
                before: &write_before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        let mut applied = self
            .finish_source_replacement_at(&spec.dataset, directory, captured_source, applied)
            .await?;
        applied.metrics.files_scanned += scanned;
        applied.metrics.data_files_written += discovery_files;
        applied.metrics.bytes_written += discovery_bytes;
        Ok(write::Prepared {
            schema_input: None,
            changes: Some(ChangePin {
                rules: super::changes::WRITE_RULES.into(),
                base_version: base.id.clone(),
                base_manifest: base.manifest.clone(),
                before_metadata: before.metadata.clone(),
                after_metadata: applied.table.metadata.clone(),
                key_field_ids,
                removed: applied.removed,
                added: applied.added,
            }),
            table: applied.table,
            metrics: applied.metrics,
        })
    }

    pub(super) async fn connected_components_evidence(
        &self,
        spec: &RefreshSpec,
        directory: &locations::Location,
        processing: &PhaseResult,
        before: &TablePin,
        context: &SessionContext,
    ) -> Result<write::Prepared> {
        let RefreshTransform::ConnectedComponents(cc) = &spec.transform else {
            return Err(invalid());
        };
        let budget = &self.inner.lifecycle.budget;
        let key_names = vec![cc.document_id_column.clone()];
        let selected = quote(&cc.document_id_column);
        let scope = context
            .sql(&format!(
                "SELECT {selected} FROM removed UNION SELECT {selected} FROM added"
            ))
            .await?;
        let mut affected_doc_ids = BTreeSet::new();
        let mut scope_stream = scope.clone().execute_stream().await?;
        while let Some(batch) = scope_stream.try_next().await? {
            let col = batch.column(0).clone();
            for i in 0..batch.num_rows() {
                for s in extract_strings(&col, i) {
                    affected_doc_ids.insert(s);
                }
            }
        }
        let replacement_batches = if affected_doc_ids.is_empty() {
            let schema = Arc::new(Schema::new(vec![
                Field::new(&cc.document_id_column, DataType::Utf8, true),
                Field::new(CONTRIBUTOR[0], DataType::Utf8, false),
                Field::new(CONTRIBUTOR[1], DataType::Int64, false),
            ]));
            vec![RecordBatch::new_empty(schema)]
        } else {
            let loaded = refresh::load_inputs(
                processing.coverage.as_ref().ok_or_else(invalid)?,
                processing
                    .dependencies
                    .as_ref()
                    .map(|dependencies| &dependencies.input_sources)
                    .unwrap_or(&BTreeMap::new()),
                budget,
            )
            .await?;
            register_input(context, cc, &loaded.borrowed(), budget).await?;
            let topo = GraphTopology::from_table(context, cc, "cc_input", budget).await?;
            topo.materialize_evidence_batches(cc, Some(&affected_doc_ids), budget)?
        };
        let replacement = context.read_batches(replacement_batches)?;
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: directory.clone(),
                before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        Ok(write::Prepared {
            table: applied.table,
            changes: None,
            metrics: applied.metrics,
            schema_input: None,
        })
    }
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_related_result",
        "connected_components inputs, specification or contributor evidence disagree",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_matches_fips_180_4_test_vectors_and_document_id_rules() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let bodies = BTreeSet::from(["body:b".to_owned(), "body:a".to_owned()]);
        let sources = BTreeSet::from(["src:1".to_owned()]);
        assert_eq!(
            compute_document_id(&bodies, &sources),
            sha256_hex(b"[\"bodies\",[\"body:a\",\"body:b\"]]")
        );
        let empty_bodies = BTreeSet::new();
        assert_eq!(
            compute_document_id(&empty_bodies, &sources),
            sha256_hex(b"[\"sources\",[\"src:1\"]]")
        );
    }
}
