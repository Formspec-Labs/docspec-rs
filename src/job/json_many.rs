//! Project disjoint raw JSON paths with one maintained parser traversal.
// Rust guideline compliant 2026-02-21
use super::functions::{capacity, text};
use crate::budget::Budget;
use arrow::{
    array::{ListBuilder, StringBuilder},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    logical_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        expr::ScalarFunction,
        simplify::{ExprSimplifyResult, SimplifyContext},
    },
    prelude::SessionContext,
};
use jiter::{Jiter, Peek};
use std::{
    any::Any,
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use tokio::sync::OwnedSemaphorePermit;

fn invalid() -> DataFusionError {
    DataFusionError::Plan("JSON paths must be literal arrays of string/integer paths, with prefix-disjoint selections".into())
}
fn item() -> Arc<Field> {
    Arc::new(
        Field::new("item", DataType::Utf8, true)
            .with_metadata(datafusion_functions_json::json_field_metadata()),
    )
}
fn output_type() -> DataType {
    DataType::List(item())
}
fn string(value: &ScalarValue) -> Result<&str> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(s),
        _ => Err(invalid()),
    }
}
fn literal(expr: &Expr) -> Result<&str> {
    match expr {
        Expr::Literal(value, _) => string(value),
        Expr::Cast(c)
            if matches!(
                c.data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            literal(&c.expr)
        }
        _ => Err(invalid()),
    }
}
#[derive(Default)]
struct Node {
    keys: HashMap<String, usize>,
    indices: HashMap<usize, usize>,
    slots: Vec<usize>,
}
struct Paths {
    literal: String,
    nodes: Vec<Node>,
    slots: usize,
    _memory: OwnedSemaphorePermit,
}
impl Paths {
    fn compile(value: &str, budget: &Budget) -> Result<Arc<Self>> {
        // Literal definitions alone use a DOM; the potentially huge input never does.
        // This allowance covers trie/hash-table growth and decoded definition keys.
        let size = value
            .len()
            .checked_mul(256)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(capacity)?;
        if size > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let memory = budget.reserve(size).map_err(|_| capacity())?;
        let definitions: serde_json::Value = serde_json::from_str(value).map_err(|_| invalid())?;
        let definitions = definitions.as_array().ok_or_else(invalid)?;
        let mut result = Self {
            literal: value.into(),
            nodes: vec![Node::default()],
            slots: definitions.len(),
            _memory: memory,
        };
        for (slot, path) in definitions.iter().enumerate() {
            let path = path.as_array().ok_or_else(invalid)?;
            let mut never = false;
            for part in path {
                match part {
                    serde_json::Value::String(_) => {}
                    serde_json::Value::Null => never = true,
                    serde_json::Value::Number(n) if n.as_i64().is_some_and(|n| n < 0) => {
                        never = true
                    }
                    serde_json::Value::Number(n) if n.as_u64().is_some() => {}
                    _ => return Err(invalid()),
                }
            }
            if never {
                continue;
            }
            let mut node = 0;
            for part in path {
                if !result.nodes[node].slots.is_empty() {
                    return Err(invalid());
                }
                let found = if let Some(key) = part.as_str() {
                    result.nodes[node].keys.get(key).copied()
                } else {
                    result.nodes[node]
                        .indices
                        .get(&usize::try_from(part.as_u64().unwrap()).map_err(|_| invalid())?)
                        .copied()
                };
                node = if let Some(found) = found {
                    found
                } else {
                    let next = result.nodes.len();
                    result.nodes.push(Node::default());
                    if let Some(key) = part.as_str() {
                        result.nodes[node].keys.insert(key.into(), next);
                    } else {
                        result.nodes[node].indices.insert(
                            usize::try_from(part.as_u64().unwrap()).map_err(|_| invalid())?,
                            next,
                        );
                    }
                    next
                };
            }
            if !result.nodes[node].keys.is_empty() || !result.nodes[node].indices.is_empty() {
                return Err(invalid());
            }
            result.nodes[node].slots.push(slot);
        }
        Ok(Arc::new(result))
    }
}
struct Projection {
    signature: Signature,
    budget: Budget,
    paths: Option<Arc<Paths>>,
}
impl fmt::Debug for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonMany")
            .field("compiled", &self.paths.is_some())
            .finish()
    }
}
impl PartialEq for Projection {
    fn eq(&self, other: &Self) -> bool {
        self.paths.as_ref().map(|x| &x.literal) == other.paths.as_ref().map(|x| &x.literal)
            && self.budget.limits.max_batch_bytes == other.budget.limits.max_batch_bytes
            && self.budget.limits.max_metadata_bytes == other.budget.limits.max_metadata_bytes
    }
}
impl Eq for Projection {}
impl Hash for Projection {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.paths.as_ref().map(|x| &x.literal).hash(h);
        self.budget.limits.max_batch_bytes.hash(h);
        self.budget.limits.max_metadata_bytes.hash(h);
    }
}
impl Projection {
    fn new(budget: Budget, paths: Option<Arc<Paths>>) -> Self {
        Self {
            signature: Signature::uniform(
                if paths.is_some() { 1 } else { 2 },
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            budget,
            paths,
        }
    }
}
pub(super) fn register(context: &mut SessionContext, budget: &Budget) {
    context.register_udf(ScalarUDF::from(Projection::new(budget.clone(), None)));
}
impl ScalarUDFImpl for Projection {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_get_many"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(output_type())
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if self.paths.is_some() {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let [raw, definition] = args.as_slice() else {
            return Err(invalid());
        };
        let paths = Paths::compile(literal(definition)?, &self.budget)?;
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction {
                func: Arc::new(ScalarUDF::from(Self::new(self.budget.clone(), Some(paths)))),
                args: vec![raw.clone()],
            },
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let owned;
        let paths = if let Some(paths) = &self.paths {
            paths
        } else {
            // Constant folding invokes an all-scalar function once. Ordinary
            // column execution must use the literal-specialized plan above.
            if args.args.len() != 2
                || args
                    .args
                    .iter()
                    .any(|v| matches!(v, ColumnarValue::Array(_)))
            {
                return Err(invalid());
            }
            let ColumnarValue::Scalar(definition) = &args.args[1] else {
                return Err(invalid());
            };
            owned = Paths::compile(string(definition)?, &self.budget)?;
            &owned
        };
        project(&args.args[0], paths, &self.budget)
    }
}
#[derive(Clone, Copy)]
enum Frame {
    Object(usize),
    Array(usize, usize),
}
fn take_child(child: Option<usize>, visited: &mut [bool]) -> Option<usize> {
    child.filter(|&id| {
        if visited[id] {
            false
        } else {
            visited[id] = true;
            true
        }
    })
}
fn spans(
    raw: &str,
    paths: &Paths,
    visited: &mut [bool],
    out: &mut [Option<(usize, usize)>],
    stack: &mut Vec<Frame>,
) {
    if paths.nodes[0].slots.is_empty()
        && paths.nodes[0].keys.is_empty()
        && paths.nodes[0].indices.is_empty()
    {
        return;
    }
    let mut parser = Jiter::new(raw.as_bytes());
    // A failed later sibling leaves already completed selections intact, just
    // like the maintained independent getter. No whole-input finish is added.
    let _parsed = (|| -> jiter::JiterResult<()> {
        let mut pending = Some((Some(0), parser.peek()?));
        loop {
            if let Some((node, peek)) = pending.take() {
                if let Some(node) = node {
                    let selected = &paths.nodes[node];
                    if !selected.slots.is_empty() {
                        let start = parser.current_index();
                        parser.known_skip(peek)?;
                        for &slot in &selected.slots {
                            out[slot] = Some((start, parser.current_index()));
                        }
                    } else if peek == Peek::Object {
                        if let Some(key) = parser.known_object()? {
                            let child = take_child(selected.keys.get(key).copied(), visited);
                            stack.push(Frame::Object(node));
                            pending = Some((child, parser.peek()?));
                            continue;
                        }
                    } else if peek == Peek::Array {
                        if let Some(peek) = parser.known_array()? {
                            let child = take_child(selected.indices.get(&0).copied(), visited);
                            stack.push(Frame::Array(node, 0));
                            pending = Some((child, peek));
                            continue;
                        }
                    } else {
                        parser.known_skip(peek)?;
                    }
                } else {
                    parser.known_skip(peek)?;
                }
            }
            loop {
                match stack.last_mut() {
                    Some(Frame::Object(node)) => {
                        if let Some(key) = parser.next_key()? {
                            let child =
                                take_child(paths.nodes[*node].keys.get(key).copied(), visited);
                            pending = Some((child, parser.peek()?));
                            break;
                        }
                        stack.pop();
                    }
                    Some(Frame::Array(node, index)) => {
                        if let Some(peek) = parser.array_step()? {
                            *index += 1;
                            let child =
                                take_child(paths.nodes[*node].indices.get(index).copied(), visited);
                            pending = Some((child, peek));
                            break;
                        }
                        stack.pop();
                    }
                    None => return Ok(()),
                }
            }
        }
    })();
}
fn project(input: &ColumnarValue, paths: &Paths, budget: &Budget) -> Result<ColumnarValue> {
    let scalar = matches!(input, ColumnarValue::Scalar(_));
    let rows = match input {
        ColumnarValue::Scalar(_) => 1,
        ColumnarValue::Array(array) => array.len(),
    };
    let get = |row| match input {
        ColumnarValue::Array(array) => text(array.as_ref(), row),
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                Ok(v.as_deref())
            }
            ScalarValue::Null => Ok(None),
            _ => Err(invalid()),
        },
    };
    let items = rows.checked_mul(paths.slots).ok_or_else(capacity)?;
    let mut bytes = items
        .checked_mul(5)
        .and_then(|n| n.checked_add(rows.saturating_mul(5)))
        .and_then(|n| n.checked_add(8))
        .ok_or_else(capacity)?;
    if bytes > budget.limits.max_batch_bytes || items > i32::MAX as usize {
        return Err(capacity());
    }
    let mut memory = budget
        .reserve(bytes.saturating_mul(2).saturating_add(1024))
        .map_err(|_| capacity())?;
    let largest = (0..rows).try_fold(0usize, |size, row| {
        Ok::<_, DataFusionError>(size.max(get(row)?.map_or(0, str::len)))
    })?;
    let state = paths
        .nodes
        .len()
        .checked_mul(64)
        .and_then(|n| n.checked_add(paths.slots.saturating_mul(32)))
        .and_then(|n| n.checked_add(largest.saturating_mul(2)))
        .ok_or_else(capacity)?;
    let _workspace = budget.reserve(state).map_err(|_| capacity())?;
    let mut visited = vec![false; paths.nodes.len()];
    let mut output = vec![None; paths.slots];
    let mut stack = Vec::with_capacity(paths.nodes.len());
    let values = StringBuilder::with_capacity(items, 0);
    let mut builder = ListBuilder::with_capacity(values, rows).with_field(item());
    for row in 0..rows {
        visited.fill(false);
        output.fill(None);
        stack.clear();
        let raw = get(row)?;
        if let Some(raw) = raw {
            spans(raw, paths, &mut visited, &mut output, &mut stack);
        }
        let added = output
            .iter()
            .flatten()
            .try_fold(0usize, |n, (start, end)| {
                n.checked_add(end - start).ok_or_else(capacity)
            })?;
        bytes = bytes.checked_add(added).ok_or_else(capacity)?;
        if bytes > budget.limits.max_batch_bytes || bytes > i32::MAX as usize {
            return Err(capacity());
        }
        memory.merge(
            budget
                .reserve(added.saturating_mul(2))
                .map_err(|_| capacity())?,
        );
        for span in &output {
            match (raw, span) {
                (Some(raw), Some((start, end))) => {
                    builder.values().append_value(&raw[*start..*end])
                }
                _ => builder.values().append_null(),
            }
        }
        builder.append(true);
    }
    let result = Arc::new(builder.finish());
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::List(result)))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}
