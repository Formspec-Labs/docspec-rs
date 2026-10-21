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
        if !valid(&sort.column) || !order_cols.insert(sort.column.to_ascii_lowercase()) {
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
        let dt = schema.field_with_name(&sort.column)?.data_type();
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

struct EvaluatedComponent {
    document_id: String,
    rows: Vec<usize>,
}

struct EvaluatedGraph {
    batch: RecordBatch,
    row_keys: Vec<String>,
    occ_files: Vec<String>,
    occ_rows: Vec<i64>,
    components: BTreeMap<String, EvaluatedComponent>,
}

impl EvaluatedGraph {
    async fn from_table(
        context: &SessionContext,
        spec: &ConnectedComponentsTransform,
        input: &str,
        budget: &Budget,
    ) -> Result<Self> {
        qualify(context, spec, input).await?;
        let df = context.table(input).await?;
        let schema = df.schema().as_arrow().clone();
        let mut stream = df.execute_stream().await?;
        let mut batches = Vec::new();
        let mut total_bytes = 0usize;
        while let Some(batch) = stream.try_next().await? {
            total_bytes = total_bytes.saturating_add(batch.get_array_memory_size());
            batches.push(batch);
        }
        let _permit = budget.reserve(total_bytes.min(budget.limits.max_batch_bytes))?;
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(Arc::new(schema))
        } else {
            arrow::compute::concat_batches(&Arc::new(schema), &batches)?
        };
        Self::from_batch(batch, spec)
    }

    fn from_batch(batch: RecordBatch, spec: &ConnectedComponentsTransform) -> Result<Self> {
        let n = batch.num_rows();
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

        let mut row_keys = Vec::with_capacity(n);
        let mut occ_files = Vec::with_capacity(n);
        let mut occ_rows = Vec::with_capacity(n);
        let occ_file_arr = occ_file_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        let occ_row_arr = occ_row_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(invalid)?;

        for i in 0..n {
            let rk = extract_strings(&row_key_col, i)
                .into_iter()
                .next()
                .unwrap_or_default();
            row_keys.push(rk);
            occ_files.push(occ_file_arr.value(i).to_owned());
            occ_rows.push(occ_row_arr.value(i));
        }

        let mut edge_cols = Vec::with_capacity(spec.edges.len());
        let mut namespaces = Vec::new();
        for edge in &spec.edges {
            let col = batch
                .column_by_name(&edge.column)
                .ok_or_else(invalid)?
                .clone();
            let ns_lower = edge.namespace.to_ascii_lowercase();
            let ns_idx = if let Some(idx) = namespaces.iter().position(|ns| *ns == ns_lower) {
                idx
            } else {
                let idx = namespaces.len();
                namespaces.push(ns_lower);
                idx
            };
            edge_cols.push((ns_idx, is_body_edge(edge), col));
        }

        let mut dsu = DisjointSet::new(n);
        let mut key_owner: HashMap<String, usize> = HashMap::with_capacity(n);
        let mut token_owner: HashMap<(usize, String), usize> = HashMap::with_capacity(n * 2);
        let mut row_body_tokens: Vec<Vec<String>> = vec![Vec::new(); n];

        for i in 0..n {
            if !row_keys[i].is_empty() {
                let prev = *key_owner.entry(row_keys[i].clone()).or_insert(i);
                dsu.union(i, prev);
            }
            for (ns_idx, is_body, col) in &edge_cols {
                for val in extract_strings(col, i) {
                    if val.is_empty() {
                        continue;
                    }
                    if *is_body {
                        row_body_tokens[i].push(val.clone());
                    }
                    let prev = *token_owner.entry((*ns_idx, val)).or_insert(i);
                    dsu.union(i, prev);
                }
            }
        }

        let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for i in 0..n {
            by_root.entry(dsu.find(i)).or_default().push(i);
        }

        let mut order_cols = Vec::with_capacity(spec.order_by.len());
        for sort in &spec.order_by {
            let col = batch
                .column_by_name(&sort.column)
                .ok_or_else(invalid)?
                .clone();
            order_cols.push((col, sort.direction));
        }

        let mut components: BTreeMap<String, EvaluatedComponent> = BTreeMap::new();
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
            let doc_id = compute_document_id(&bodies, &sources);
            components
                .entry(doc_id.clone())
                .or_insert_with(|| EvaluatedComponent {
                    document_id: doc_id,
                    rows: Vec::new(),
                })
                .rows
                .extend(rows);
        }

        for comp in components.values_mut() {
            comp.rows.sort_by(|&a, &b| {
                for (col, dir) in &order_cols {
                    let va = extract_scalar(col, a);
                    let vb = extract_scalar(col, b);
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
                row_keys[a]
                    .cmp(&row_keys[b])
                    .then_with(|| occ_files[a].cmp(&occ_files[b]))
                    .then_with(|| occ_rows[a].cmp(&occ_rows[b]))
            });
        }

        Ok(Self {
            batch,
            row_keys,
            occ_files,
            occ_rows,
            components,
        })
    }

    fn output_schema(&self, spec: &ConnectedComponentsTransform) -> Result<SchemaRef> {
        let in_schema = self.batch.schema();
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
            };
            fields.push(field);
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn materialize_components(
        &self,
        spec: &ConnectedComponentsTransform,
        selected_doc_ids: Option<&BTreeSet<String>>,
    ) -> Result<RecordBatch> {
        let out_schema = self.output_schema(spec)?;
        let selected: Vec<&EvaluatedComponent> = self
            .components
            .values()
            .filter(|c| selected_doc_ids.is_none_or(|set| set.contains(&c.document_id)))
            .collect();
        let num_rows = selected.len();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + spec.aggregates.len());
        let mut doc_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        for comp in &selected {
            doc_id_builder.append_value(&comp.document_id);
        }
        columns.push(Arc::new(doc_id_builder.finish()));

        for agg in &spec.aggregates {
            let col: ArrayRef = match &agg.function {
                AggregateFunction::CountRows => {
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in &selected {
                        b.append_value(comp.rows.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Count { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in &selected {
                        let count = comp.rows.iter().filter(|&&r| !src.is_null(r)).count();
                        b.append_value(count as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::CountDistinct { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let mut b = Int64Builder::with_capacity(num_rows);
                    for comp in &selected {
                        let set: BTreeSet<ScalarVal> = comp
                            .rows
                            .iter()
                            .filter_map(|&r| extract_scalar(src, r))
                            .collect();
                        b.append_value(set.len() as i64);
                    }
                    Arc::new(b.finish())
                }
                AggregateFunction::Sum { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let mut b = Decimal128Builder::with_capacity(num_rows)
                        .with_data_type(DataType::Decimal128(38, 0));
                    for comp in &selected {
                        let mut sum = 0i128;
                        let mut any = false;
                        for &r in &comp.rows {
                            match extract_scalar(src, r) {
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
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let vals: Vec<Option<ScalarVal>> = selected
                        .iter()
                        .map(|comp| {
                            comp.rows
                                .iter()
                                .filter_map(|&r| extract_scalar(src, r))
                                .min()
                        })
                        .collect();
                    build_scalar_array(&vals, src.data_type())?
                }
                AggregateFunction::Max { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let vals: Vec<Option<ScalarVal>> = selected
                        .iter()
                        .map(|comp| {
                            comp.rows
                                .iter()
                                .filter_map(|&r| extract_scalar(src, r))
                                .max()
                        })
                        .collect();
                    build_scalar_array(&vals, src.data_type())?
                }
                AggregateFunction::FirstValue { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    let vals: Vec<Option<ScalarVal>> = selected
                        .iter()
                        .map(|comp| comp.rows.first().and_then(|&r| extract_scalar(src, r)))
                        .collect();
                    build_scalar_array(&vals, src.data_type())?
                }
                AggregateFunction::CollectSet { field } => {
                    let src = self.batch.column_by_name(field).ok_or_else(invalid)?;
                    build_collect_set_array(&selected, src)?
                }
            };
            columns.push(col);
        }

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }

    fn materialize_evidence(
        &self,
        spec: &ConnectedComponentsTransform,
        selected_doc_ids: Option<&BTreeSet<String>>,
    ) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(&spec.document_id_column, DataType::Utf8, true),
            Field::new(CONTRIBUTOR[0], DataType::Utf8, false),
            Field::new(CONTRIBUTOR[1], DataType::Int64, false),
        ]));
        let mut doc_ids = StringBuilder::new();
        let mut files = StringBuilder::new();
        let mut rows = Int64Builder::new();
        for comp in self.components.values() {
            if selected_doc_ids.is_some_and(|set| !set.contains(&comp.document_id)) {
                continue;
            }
            for &r in &comp.rows {
                doc_ids.append_value(&comp.document_id);
                files.append_value(&self.occ_files[r]);
                rows.append_value(self.occ_rows[r]);
            }
        }
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(doc_ids.finish()),
                Arc::new(files.finish()),
                Arc::new(rows.finish()),
            ],
        )?)
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
                if let Some(field) = argument(&agg.function)
                    && let Some(src) = self.batch.column_by_name(field)
                {
                    match &agg.function {
                        AggregateFunction::FirstValue { .. } => {
                            let val = extract_scalar(src, rep);
                            hasher.update(format!("{val:?}").as_bytes());
                        }
                        AggregateFunction::CollectSet { .. } => {
                            for &r in &comp.rows {
                                for s in extract_strings(src, r) {
                                    hasher.update(&(s.len() as u64).to_le_bytes());
                                    hasher.update(s.as_bytes());
                                }
                                if let Some(val) = extract_scalar(src, r) {
                                    hasher.update(format!("{val:?}").as_bytes());
                                }
                            }
                        }
                        _ => {
                            for &r in &comp.rows {
                                let val = extract_scalar(src, r);
                                hasher.update(format!("{val:?}").as_bytes());
                            }
                        }
                    }
                }
            }
        }
        let mut contributors: Vec<(&str, i64)> = comp
            .rows
            .iter()
            .map(|&r| (self.occ_files[r].as_str(), self.occ_rows[r]))
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

fn build_collect_set_array(
    selected: &[&EvaluatedComponent],
    src: &ArrayRef,
) -> Result<ArrayRef> {
    let n = selected.len();
    match src.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::List(_) | DataType::LargeList(_) => {
            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), n);
            for comp in selected {
                let mut set = BTreeSet::new();
                for &r in &comp.rows {
                    for s in extract_strings(src, r) {
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
                for &r in &comp.rows {
                    if let Some(ScalarVal::Bool(v)) = extract_scalar(src, r) {
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
                for &r in &comp.rows {
                    if let Some(ScalarVal::Int32(v)) = extract_scalar(src, r) {
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
                for &r in &comp.rows {
                    if let Some(ScalarVal::Int64(v)) = extract_scalar(src, r) {
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
                for &r in &comp.rows {
                    if let Some(ScalarVal::Date32(v)) = extract_scalar(src, r) {
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
            let field = Arc::new(Field::new_list_field(src.data_type().clone(), true));
            let mut builder = ListBuilder::with_capacity(val_builder, n).with_field(field);
            for comp in selected {
                let mut set = BTreeSet::new();
                for &r in &comp.rows {
                    if let Some(ScalarVal::Timestamp(v)) = extract_scalar(src, r) {
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

pub(super) async fn source_frame(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    let graph = EvaluatedGraph::from_table(context, spec, input, budget).await?;
    let batch = graph.materialize_components(spec, None)?;
    Ok(context.read_batch(batch)?)
}

pub(super) async fn evidence_frame(
    context: &SessionContext,
    spec: &ConnectedComponentsTransform,
    input: &str,
    budget: &Budget,
) -> Result<DataFrame> {
    let graph = EvaluatedGraph::from_table(context, spec, input, budget).await?;
    let batch = graph.materialize_evidence(spec, None)?;
    Ok(context.read_batch(batch)?)
}

/// Computes the transitive closure of affected `document_id`s in `old_graph` and `new_graph`
/// starting from `seed_row_keys`.
fn affected_component_closure(
    spec: &ConnectedComponentsTransform,
    old_graph: &EvaluatedGraph,
    new_graph: &EvaluatedGraph,
    seed_row_keys: &HashSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    struct GraphIndex {
        by_rk: HashMap<String, Vec<String>>,
        by_tok: HashMap<(String, String), Vec<String>>,
        comp_rks: HashMap<String, Vec<String>>,
        comp_toks: HashMap<String, Vec<(String, String)>>,
    }

    fn index_graph(spec: &ConnectedComponentsTransform, graph: &EvaluatedGraph) -> GraphIndex {
        let mut by_rk: HashMap<String, Vec<String>> = HashMap::new();
        let mut by_tok: HashMap<(String, String), Vec<String>> = HashMap::new();
        let mut comp_rks: HashMap<String, Vec<String>> = HashMap::new();
        let mut comp_toks: HashMap<String, Vec<(String, String)>> = HashMap::new();

        for (doc_id, comp) in &graph.components {
            for &r in &comp.rows {
                let rk = &graph.row_keys[r];
                if !rk.is_empty() {
                    by_rk.entry(rk.clone()).or_default().push(doc_id.clone());
                    comp_rks.entry(doc_id.clone()).or_default().push(rk.clone());
                }
                for edge in &spec.edges {
                    if let Some(col) = graph.batch.column_by_name(&edge.column) {
                        let ns = edge.namespace.to_ascii_lowercase();
                        for val in extract_strings(col, r) {
                            if !val.is_empty() {
                                let tok = (ns.clone(), val);
                                by_tok.entry(tok.clone()).or_default().push(doc_id.clone());
                                comp_toks.entry(doc_id.clone()).or_default().push(tok);
                            }
                        }
                    }
                }
            }
        }
        GraphIndex {
            by_rk,
            by_tok,
            comp_rks,
            comp_toks,
        }
    }

    let GraphIndex {
        by_rk: old_by_rk,
        by_tok: old_by_tok,
        comp_rks: old_comp_rks,
        comp_toks: old_comp_toks,
    } = index_graph(spec, old_graph);
    let GraphIndex {
        by_rk: new_by_rk,
        by_tok: new_by_tok,
        comp_rks: new_comp_rks,
        comp_toks: new_comp_toks,
    } = index_graph(spec, new_graph);

    let mut visited_rks: HashSet<String> = HashSet::new();
    let mut visited_toks: HashSet<(String, String)> = HashSet::new();
    let mut old_docs: BTreeSet<String> = BTreeSet::new();
    let mut new_docs: BTreeSet<String> = BTreeSet::new();

    let mut rk_queue: VecDeque<String> = VecDeque::new();
    let mut tok_queue: VecDeque<(String, String)> = VecDeque::new();

    for rk in seed_row_keys {
        if !rk.is_empty() && visited_rks.insert(rk.clone()) {
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

impl Engine {
    pub(super) async fn process_connected_components(
        &self,
        spec: &RefreshSpec,
        directory: &Path,
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
                &directory.join("affected"),
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
        let (scope_batch, replacement_batch) = if affected.rows == 0 {
            let empty_scope = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                &cc.document_id_column,
                DataType::Utf8,
                true,
            )])));
            let empty_graph = EvaluatedGraph::from_batch(
                RecordBatch::new_empty(
                    context
                        .table("cc_input")
                        .await?
                        .schema()
                        .as_arrow()
                        .clone()
                        .into(),
                ),
                cc,
            )?;
            let empty_rep = empty_graph.materialize_components(cc, None)?;
            (empty_scope, empty_rep)
        } else {
            context.register_table(
                "affected_rks",
                crate::query::table(native_affected, budget.clone(), Some(affected.rows))?,
            )?;
            let mut rk_stream = context
                .sql("SELECT DISTINCT \"k0\" FROM affected_rks WHERE \"k0\" IS NOT NULL")
                .await?
                .execute_stream()
                .await?;
            let mut seed_rks = HashSet::new();
            while let Some(batch) = rk_stream.try_next().await? {
                let col = batch.column(0).clone();
                for i in 0..batch.num_rows() {
                    for s in extract_strings(&col, i) {
                        seed_rks.insert(s);
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

            let old_graph =
                EvaluatedGraph::from_table(&context, cc, "cc_old_input", budget).await?;
            let new_graph = EvaluatedGraph::from_table(&context, cc, "cc_input", budget).await?;

            let (old_affected_docs, new_affected_docs) =
                affected_component_closure(cc, &old_graph, &new_graph, &seed_rks);

            let mut changed_scope_docs = BTreeSet::new();
            let mut changed_new_docs = BTreeSet::new();

            for doc_id in old_affected_docs.union(&new_affected_docs) {
                let old_sig = old_graph.component_signature(cc, doc_id);
                let new_sig = new_graph.component_signature(cc, doc_id);
                if old_sig != new_sig {
                    changed_scope_docs.insert(doc_id.clone());
                    if new_sig.is_some() {
                        changed_new_docs.insert(doc_id.clone());
                    }
                }
            }

            let mut scope_builder =
                StringBuilder::with_capacity(changed_scope_docs.len(), changed_scope_docs.len() * 64);
            for doc_id in &changed_scope_docs {
                scope_builder.append_value(doc_id);
            }
            let scope_batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    &cc.document_id_column,
                    DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(scope_builder.finish())],
            )?;
            let replacement_batch =
                new_graph.materialize_components(cc, Some(&changed_new_docs))?;
            (scope_batch, replacement_batch)
        };

        let scope = context.read_batch(scope_batch)?;
        let replacement = context.read_batch(replacement_batch)?;
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
                directory: locations::Location::Local(directory.join("replacement")),
                before: &write_before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        let mut applied = self
            .finish_source_replacement(&spec.dataset, directory, captured_source, applied)
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
        directory: &Path,
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
        context.register_table("affected_groups", scope.clone().into_view())?;
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
        let evidence = evidence_frame(context, cc, "cc_input", budget).await?;
        context.register_table("contributors", evidence.into_view())?;
        let replacement = context
            .sql(&format!(
                "SELECT s.* FROM contributors s LEFT SEMI JOIN affected_groups k ON (s.{selected} IS NOT DISTINCT FROM k.{selected})"
            ))
            .await?;
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: locations::Location::Local(directory.to_owned()),
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
