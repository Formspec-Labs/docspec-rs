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
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
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
    DataFusionError::Plan(
        "JSON paths must be arrays of string/integer paths, with prefix-disjoint selections".into(),
    )
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
fn literal(expr: &Expr) -> Result<Option<&str>> {
    match expr {
        Expr::Literal(value, _) => string(value).map(Some),
        Expr::Cast(c)
            if matches!(
                c.data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            literal(&c.expr)
        }
        _ => Ok(None),
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
    copies: usize,
    _memory: OwnedSemaphorePermit,
}
impl Paths {
    fn compile(value: &str, budget: &Budget) -> Result<Arc<Self>> {
        // Only path definitions use a DOM; the potentially huge input never does.
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
            copies: 0,
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
            result.copies = result.copies.max(result.nodes[node].slots.len());
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
            signature: super::functions::string_signature(if paths.is_some() { 1 } else { 2 }),
            budget,
            paths,
        }
    }
}
pub(super) fn register(context: &mut SessionContext, budget: &Budget) {
    context.register_udf(ScalarUDF::from(Projection::new(budget.clone(), None)));
}
pub(super) fn byte_limit(function: &ScalarUDF) -> Option<usize> {
    function
        .inner()
        .as_any()
        .downcast_ref::<Projection>()
        .map(|function| function.budget.limits.max_batch_bytes)
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
        let Some(definition) = literal(definition)? else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let paths = Paths::compile(definition, &self.budget)?;
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction {
                func: Arc::new(ScalarUDF::from(Self::new(self.budget.clone(), Some(paths)))),
                args: vec![raw.clone()],
            },
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match (&self.paths, args.args.as_slice()) {
            (Some(paths), [input]) => project(input, Definition::Fixed(paths), &self.budget),
            (None, [input, ColumnarValue::Scalar(value)]) => {
                let paths = Paths::compile(string(value)?, &self.budget)?;
                project(input, Definition::Fixed(&paths), &self.budget)
            }
            (None, [input, definition]) => {
                project(input, Definition::Rows(definition), &self.budget)
            }
            _ => Err(invalid()),
        }
    }
}
// Locked Jiter0.13 known_skip permits200 nonempty container levels. These
// relative heights preserve each independent getter's preceding-subtree skips;
// boundary differential fixtures must move with a future parser upgrade.
const SKIP_DEPTH: usize = 200;
#[derive(Clone, Copy)]
enum Container {
    Object,
    Array(usize),
}
struct Frame {
    node: Option<usize>,
    start: usize,
    kind: Container,
    height: usize,
    blocked: bool,
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
fn record(
    node: Option<usize>,
    start: usize,
    end: usize,
    height: usize,
    paths: &Paths,
    out: &mut [Option<(usize, usize)>],
) {
    if height <= SKIP_DEPTH
        && let Some(node) = node
    {
        for &slot in &paths.nodes[node].slots {
            out[slot] = Some((start, end));
        }
    }
}
fn push(
    stack: &mut Vec<Frame>,
    frame: Frame,
    budget: &Budget,
    memory: &mut OwnedSemaphorePermit,
) -> Result<()> {
    if stack.len() == stack.capacity() {
        let capacity = stack
            .capacity()
            .max(1)
            .checked_mul(2)
            .ok_or_else(capacity)?;
        let extra = capacity
            .checked_sub(stack.capacity())
            .and_then(|n| n.checked_mul(64))
            .ok_or_else(super::functions::capacity)?;
        memory.merge(
            budget
                .reserve(extra)
                .map_err(|_| super::functions::capacity())?,
        );
        stack
            .try_reserve_exact(capacity - stack.len())
            .map_err(|_| super::functions::capacity())?;
    }
    stack.push(frame);
    Ok(())
}
fn spans(
    raw: &str,
    paths: &Paths,
    visited: &mut [bool],
    out: &mut [Option<(usize, usize)>],
    stack: &mut Vec<Frame>,
    budget: &Budget,
    memory: &mut OwnedSemaphorePermit,
) -> Result<()> {
    if paths.nodes[0].slots.is_empty()
        && paths.nodes[0].keys.is_empty()
        && paths.nodes[0].indices.is_empty()
    {
        return Ok(());
    }
    let mut parser = Jiter::new(raw.as_bytes());
    // Parse failures preserve completed selections. Capacity errors propagate.
    macro_rules! parsed {
        ($value:expr) => {
            match $value {
                Ok(value) => value,
                Err(_) => return Ok(()),
            }
        };
    }
    let mut pending = Some((Some(0), parsed!(parser.peek())));
    let mut completed = None;
    loop {
        if let Some((node, peek)) = pending.take() {
            let start = parser.current_index();
            if peek == Peek::Object {
                if let Some(key) = parsed!(parser.known_object()) {
                    let child = take_child(
                        node.and_then(|n| paths.nodes[n].keys.get(key).copied()),
                        visited,
                    );
                    push(
                        stack,
                        Frame {
                            node,
                            start,
                            kind: Container::Object,
                            height: 1,
                            blocked: false,
                        },
                        budget,
                        memory,
                    )?;
                    pending = Some((child, parsed!(parser.peek())));
                    continue;
                }
            } else if peek == Peek::Array {
                if let Some(peek) = parsed!(parser.known_array()) {
                    let child = take_child(
                        node.and_then(|n| paths.nodes[n].indices.get(&0).copied()),
                        visited,
                    );
                    push(
                        stack,
                        Frame {
                            node,
                            start,
                            kind: Container::Array(0),
                            height: 1,
                            blocked: false,
                        },
                        budget,
                        memory,
                    )?;
                    pending = Some((child, peek));
                    continue;
                }
            } else {
                parsed!(parser.known_skip(peek));
            }
            record(node, start, parser.current_index(), 0, paths, out);
            completed = Some(0);
        }
        if let Some(height) = completed.take()
            && let Some(parent) = stack.last_mut()
        {
            parent.height = parent.height.max((height + 1).min(SKIP_DEPTH + 1));
            parent.blocked |= height > SKIP_DEPTH;
        }
        let Some(frame) = stack.last_mut() else {
            return Ok(());
        };
        let node = if frame.blocked { None } else { frame.node };
        match &mut frame.kind {
            Container::Object => {
                if let Some(key) = parsed!(parser.next_key()) {
                    let child = take_child(
                        node.and_then(|n| paths.nodes[n].keys.get(key).copied()),
                        visited,
                    );
                    pending = Some((child, parsed!(parser.peek())));
                    continue;
                }
            }
            Container::Array(index) => {
                if let Some(peek) = parsed!(parser.array_step()) {
                    *index += 1;
                    let child = take_child(
                        node.and_then(|n| paths.nodes[n].indices.get(index).copied()),
                        visited,
                    );
                    pending = Some((child, peek));
                    continue;
                }
            }
        }
        let frame = stack.pop().expect("inspected nonempty stack");
        record(
            frame.node,
            frame.start,
            parser.current_index(),
            frame.height,
            paths,
            out,
        );
        completed = Some(frame.height);
    }
}

enum Definition<'a> {
    Fixed(&'a Arc<Paths>),
    Rows(&'a ColumnarValue),
}

fn row_text(input: &ColumnarValue, row: usize) -> Result<Option<&str>> {
    match input {
        ColumnarValue::Array(array) => text(array.as_ref(), row),
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                Ok(v.as_deref())
            }
            ScalarValue::Null => Ok(None),
            _ => Err(invalid()),
        },
    }
}

fn output_capacity(rows: usize, preceding: usize) -> DataFusionError {
    if rows > 1 {
        super::fitting::smaller(preceding)
    } else {
        capacity()
    }
}

fn project(
    input: &ColumnarValue,
    definition: Definition<'_>,
    budget: &Budget,
) -> Result<ColumnarValue> {
    let input_rows = match input {
        ColumnarValue::Array(array) => Some(array.len()),
        ColumnarValue::Scalar(_) => None,
    };
    let definition_rows = match &definition {
        Definition::Rows(ColumnarValue::Array(array)) => Some(array.len()),
        _ => None,
    };
    if let (Some(a), Some(b)) = (input_rows, definition_rows)
        && a != b
    {
        return Err(invalid());
    }
    let scalar = input_rows.is_none() && definition_rows.is_none();
    let rows = input_rows.or(definition_rows).unwrap_or(1);
    if let Definition::Fixed(paths) = &definition {
        super::fitting::preflight(
            rows,
            budget.limits.max_batch_bytes,
            (0..rows).map(|row| {
                Ok(row_text(input, row)?
                    .map_or(0, str::len)
                    .saturating_mul(paths.copies)
                    .saturating_add(paths.slots.saturating_add(1).saturating_mul(5))
                    .saturating_add(256))
            }),
        )?;
    }
    // Fixed definitions keep their once-per-expression specialization. Dynamic
    // definitions compile once per row and drop after copying that row's spans.
    let fixed_slots = match &definition {
        Definition::Fixed(paths) => Some(paths.slots),
        Definition::Rows(_) => None,
    };
    let initial_items = rows
        .checked_mul(fixed_slots.unwrap_or(0))
        .ok_or_else(capacity)?;
    let mut items = 0usize;
    let mut bytes = initial_items
        .checked_mul(5)
        .and_then(|n| n.checked_add(rows.saturating_mul(5)))
        .and_then(|n| n.checked_add(8))
        .ok_or_else(capacity)?;
    if bytes > budget.limits.max_batch_bytes || initial_items > i32::MAX as usize {
        return Err(output_capacity(
            rows,
            budget.limits.max_batch_bytes.saturating_sub(8)
                / fixed_slots.unwrap_or(0).saturating_add(1).saturating_mul(5),
        ));
    }
    let mut memory = budget
        .reserve(bytes.saturating_mul(2).saturating_add(1024))
        .map_err(|_| capacity())?;
    let largest = (0..rows).try_fold(0usize, |size, row| {
        Ok::<_, DataFusionError>(size.max(row_text(input, row)?.map_or(0, str::len)))
    })?;
    let mut workspace = budget.reserve(0).map_err(|_| capacity())?;
    let mut visited: Vec<bool> = Vec::new();
    let mut output: Vec<Option<(usize, usize)>> = Vec::new();
    let mut stack: Vec<Frame> = Vec::new();
    let values = StringBuilder::with_capacity(initial_items, 0);
    let mut builder = ListBuilder::with_capacity(values, rows).with_field(item());
    for row in 0..rows {
        let paths = match &definition {
            Definition::Fixed(paths) => Arc::clone(paths),
            Definition::Rows(values) => {
                Paths::compile(row_text(values, row)?.ok_or_else(invalid)?, budget)?
            }
        };
        items = items.checked_add(paths.slots).ok_or_else(capacity)?;
        if fixed_slots.is_none() {
            let added = paths.slots.checked_mul(5).ok_or_else(capacity)?;
            bytes = bytes.checked_add(added).ok_or_else(capacity)?;
            if bytes > budget.limits.max_batch_bytes || items > i32::MAX as usize {
                return Err(output_capacity(rows, row));
            }
            memory.merge(
                budget
                    .reserve(added.saturating_mul(2))
                    .map_err(|_| capacity())?,
            );
        }
        let nodes = paths
            .nodes
            .len()
            .max(stack.capacity())
            .max(visited.capacity());
        let slots = paths.slots.max(output.capacity());
        let state = nodes
            .checked_mul(64)
            .and_then(|n| n.checked_add(slots.checked_mul(32)?))
            .and_then(|n| n.checked_add(largest.checked_mul(2)?))
            .ok_or_else(capacity)?;
        if state > workspace.num_permits() {
            workspace.merge(
                budget
                    .reserve(state - workspace.num_permits())
                    .map_err(|_| capacity())?,
            );
        }
        if visited.capacity() < paths.nodes.len() {
            visited
                .try_reserve_exact(paths.nodes.len() - visited.len())
                .map_err(|_| capacity())?;
        }
        if output.capacity() < paths.slots {
            output
                .try_reserve_exact(paths.slots - output.len())
                .map_err(|_| capacity())?;
        }
        stack.clear();
        if stack.capacity() < paths.nodes.len() {
            stack
                .try_reserve_exact(paths.nodes.len())
                .map_err(|_| capacity())?;
        }
        visited.resize(paths.nodes.len(), false);
        output.resize(paths.slots, None);
        visited.fill(false);
        output.fill(None);
        let raw = row_text(input, row)?;
        if let Some(raw) = raw {
            spans(
                raw,
                &paths,
                &mut visited,
                &mut output,
                &mut stack,
                budget,
                &mut workspace,
            )?;
        }
        let added = output
            .iter()
            .flatten()
            .try_fold(0usize, |n, (start, end)| {
                n.checked_add(end - start).ok_or_else(capacity)
            })?;
        bytes = bytes.checked_add(added).ok_or_else(capacity)?;
        if bytes > budget.limits.max_batch_bytes || bytes > i32::MAX as usize {
            return Err(output_capacity(rows, row));
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
    let result = super::admitted::array(Arc::new(builder.finish()), memory)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if scalar {
        let list = result
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("list output")
            .clone();
        Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list))))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projected_children_keep_output_admission_after_parent_drop() {
        let budget = Budget::new(crate::budget::Limits {
            memory_bytes: 128 << 10,
            max_batch_bytes: 4 << 10,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        let paths = Paths::compile(r#"[["key"]]"#, &budget).unwrap();
        let available = budget.limits.memory_bytes / 2 - paths._memory.num_permits();
        for input in [
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"key":"value"}"#.into()))),
            ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(vec![
                r#"{"key":"value"}"#,
            ]))),
        ] {
            let output = project(&input, Definition::Fixed(&paths), &budget).unwrap();
            let array = match output {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(ScalarValue::List(array)) => array,
                _ => panic!("list expected"),
            };
            let child = array.to_data().child_data()[0].clone();
            drop(array);
            assert!(budget.reserve(available).is_err());
            drop(child);
            drop(budget.reserve(available).unwrap());
        }
    }
    #[test]
    fn deep_unselected_frames_refuse_capacity_and_release_it_for_reuse() {
        let budget = Budget::new(crate::budget::Limits {
            memory_bytes: 128 << 10,
            max_metadata_bytes: 32 << 10,
            max_batch_bytes: 4 << 10,
            compute_threads: 1,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        let paths = Paths::compile(r#"[["later"]]"#, &budget).unwrap();
        let nested = r#"{"a":"#.repeat(1000) + "1" + &"}".repeat(1000);
        let raw = format!(r#"{{"nested":{nested},"later":7}}"#);
        let error = project(
            &ColumnarValue::Scalar(ScalarValue::Utf8(Some(raw))),
            Definition::Fixed(&paths),
            &budget,
        )
        .unwrap_err();
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        let result = project(
            &ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"later":7}"#.into()))),
            Definition::Fixed(&paths),
            &budget,
        )
        .unwrap();
        let ColumnarValue::Scalar(ScalarValue::List(values)) = result else {
            panic!("expected scalar list");
        };
        assert_eq!(text(values.values().as_ref(), 0).unwrap(), Some("7"));
    }
}
