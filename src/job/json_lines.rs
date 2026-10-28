//! Plain or gzip JSON-lines input; source keys and schemas belong to the caller.
// Rust guideline compliant 2026-02-21
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    json::ReaderBuilder,
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    common::{DataFusionError, Result},
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use flate2::read::MultiGzDecoder;
use futures::{StreamExt, TryStreamExt, stream};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek},
    path::PathBuf,
    sync::Arc,
};

/// One source file and its stable, non-secret evidence key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonLinesFile {
    pub key: String,
    pub path: PathBuf,
}

fn nullable(field: &Field) -> bool {
    field.is_nullable()
        && match field.data_type() {
            DataType::Struct(fields) => fields.iter().all(|f| nullable(f)),
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => nullable(child),
            _ => true,
        }
}

fn field_allowance(field: &Field) -> Option<usize> {
    // Sparse records still allocate typed arrays, validity maps and offsets.
    let children = match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .try_fold(0usize, |n, f| n.checked_add(field_allowance(f)?))?,
        DataType::FixedSizeList(child, n) => {
            field_allowance(child)?.checked_mul(usize::try_from(*n).ok()?)?
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::Map(child, _) => field_allowance(child)?,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok()?,
        _ => 0,
    };
    children.checked_add(128)
}

// Variable containers can produce many sparse typed values from short JSON.
// Charge their largest declared element shape per possible source value;
// fixed-size descendants are already multiplied by field_allowance.
fn item_allowance(field: &Field) -> Option<usize> {
    match field.data_type() {
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::Map(child, _) => Some(field_allowance(child)?.max(item_allowance(child)?)),
        DataType::Struct(fields) => fields
            .iter()
            .try_fold(0, |n, f| Some(n.max(item_allowance(f)?))),
        DataType::FixedSizeList(child, _) => item_allowance(child),
        _ => Some(0),
    }
}

/// Allocation preflight for the locked Arrow58 JSON decoder, not a JSON parser.
/// Strings pay for raw/decoded byte buffers; structural units pay for tape,
/// offsets, stack and schema-dependent sparse list elements. Native Arrow
/// still validates and decodes every record exactly once.
#[derive(Default)]
struct DecodeAllocation {
    bytes: usize,
    units: usize,
    quoted: bool,
    escaped: bool,
}
impl DecodeAllocation {
    fn grow(&mut self, input: &[u8], fixed: usize, item: usize) -> Result<usize> {
        for byte in input {
            if self.quoted {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.quoted = false;
                }
            } else if !byte.is_ascii_whitespace() {
                self.units = self.units.checked_add(1).ok_or_else(allocation_overflow)?;
                self.quoted = *byte == b'"';
            }
        }
        self.bytes = self
            .bytes
            .checked_add(input.len())
            .ok_or_else(allocation_overflow)?;
        // Eight byte copies cover raw Vec reallocations (old+new), native tape
        // strings, typed values and original-line evidence. A 128-byte unit
        // covers geometrically grown 8-byte tape/offset/stack entries, decoder
        // position vectors and validation fallback nodes. Twice each variable
        // element's fixed shape covers sparse typed arrays and their workspace.
        self.bytes
            .checked_mul(8)
            .and_then(|bytes| {
                item.checked_mul(2)
                    .and_then(|item| item.checked_add(128))
                    .and_then(|unit| self.units.checked_mul(unit))
                    .and_then(|units| bytes.checked_add(units))
            })
            .and_then(|bytes| bytes.checked_add(fixed))
            .ok_or_else(allocation_overflow)
    }
}
fn allocation_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("JSON decode allocation overflow".into())
}

pub(super) fn provider(
    files: Vec<JsonLinesFile>,
    input: SchemaRef,
    limits: &crate::budget::Limits,
) -> Result<Arc<dyn TableProvider>> {
    if input
        .fields()
        .iter()
        .any(|f| !nullable(f) || f.name().starts_with("__"))
    {
        return Err(DataFusionError::Plan(
            "JSON fields must be nullable and cannot use reserved __ names".into(),
        ));
    }
    let mut fields = input
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("__source_key", DataType::Utf8, false),
        Field::new("__source_line", DataType::Int64, false),
        Field::new("__raw_line", DataType::Utf8, false),
    ]);
    let schema = Arc::new(Schema::new(fields));
    let fixed_per_row = input
        .fields()
        .iter()
        .try_fold(32usize, |n, f| n.checked_add(field_allowance(f)?))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("JSON schema allowance overflow".into())
        })?;
    let item_per_value = input
        .fields()
        .iter()
        .try_fold(0usize, |n, f| Some(n.max(item_allowance(f)?)))
        .ok_or_else(allocation_overflow)?;
    // One sequential group per compute slot bounds open files and queued batches.
    let mut groups = vec![Vec::new(); limits.compute_threads];
    for (i, file) in files.into_iter().enumerate() {
        groups[i % limits.compute_threads].push(file);
    }
    let partitions = groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .map(|files| {
            Arc::new(JsonPartition {
                files,
                input: input.clone(),
                schema: schema.clone(),
                // One batch must hold a complete source line. Keep the native
                // signed-32-bit string offset bound as an independent ceiling.
                max_line: limits.max_batch_bytes.min(i32::MAX as usize),
                batch_rows: limits.batch_rows.min(64),
                fixed_per_row,
                item_per_value,
                max_batch_bytes: limits.max_batch_bytes,
            }) as Arc<dyn PartitionStream>
        })
        .collect();
    Ok(Arc::new(StreamingTable::try_new(schema, partitions)?))
}

#[derive(Debug)]
struct JsonPartition {
    files: Vec<JsonLinesFile>,
    input: SchemaRef,
    schema: SchemaRef,
    max_line: usize,
    batch_rows: usize,
    fixed_per_row: usize,
    item_per_value: usize,
    max_batch_bytes: usize,
}
impl PartitionStream for JsonPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let input = self.input.clone();
        let schema = self.schema.clone();
        let max_line = self.max_line;
        let batch_rows = self.batch_rows;
        let fixed_per_row = self.fixed_per_row;
        let item_per_value = self.item_per_value;
        let max_batch_bytes = self.max_batch_bytes;
        let files = stream::iter(self.files.clone())
            .map(move |file| {
                let state = Reader {
                    file,
                    input: input.clone(),
                    schema: schema.clone(),
                    reader: None,
                    line: 0,
                    max_line,
                    batch_rows,
                    fixed_per_row,
                    item_per_value,
                    max_batch_bytes,
                    reservation: MemoryConsumer::new("JSON-lines decoder")
                        .register(context.memory_pool()),
                };
                Ok::<_, DataFusionError>(stream::try_unfold(state, |mut state| async move {
                    tokio::task::spawn_blocking(move || {
                        state.next().map(|batch| batch.map(|batch| (batch, state)))
                    })
                    .await
                    .map_err(|_| DataFusionError::Execution("JSON-lines worker failed".into()))?
                }))
            })
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), files))
    }
}

struct Reader {
    file: JsonLinesFile,
    input: SchemaRef,
    schema: SchemaRef,
    reader: Option<BufReader<Box<dyn Read + Send>>>,
    line: i64,
    max_line: usize,
    batch_rows: usize,
    fixed_per_row: usize,
    item_per_value: usize,
    max_batch_bytes: usize,
    reservation: MemoryReservation,
}
impl Reader {
    fn error(&self, line: i64, message: &str) -> DataFusionError {
        DataFusionError::Execution(format!("{} line {line}: {message}", self.file.key))
    }
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let fixed = self
            .fixed_per_row
            .checked_add(self.file.key.len())
            .and_then(|n| n.checked_mul(self.batch_rows))
            // Cover the8KiB BufReader and the locked miniz inflater's32KiB
            // history, Huffman tables and stream state before opening gzip.
            .and_then(|n| n.checked_add(256 << 10))
            .ok_or_else(|| self.error(self.line + 1, "schema and evidence allowance overflow"))?;
        self.reservation.try_resize(fixed)?;
        if self.reader.is_none() {
            let mut file =
                File::open(&self.file.path).map_err(|_| self.error(1, "source open failed"))?;
            let mut magic = [0; 2];
            match file.read_exact(&mut magic) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
                Err(_) => return Err(self.error(1, "source read failed")),
            }
            file.rewind()
                .map_err(|_| self.error(1, "source rewind failed"))?;
            let source: Box<dyn Read + Send> = if magic == [0x1f, 0x8b] {
                Box::new(MultiGzDecoder::new(file))
            } else {
                Box::new(file)
            };
            self.reader = Some(BufReader::new(source));
        }
        let first = self.line + 1;
        let mut raw = Vec::new();
        let mut allocation = DecodeAllocation::default();
        let mut offsets = vec![0usize];
        while offsets.len() <= self.batch_rows && raw.len() < (8 << 20) {
            let start = raw.len();
            loop {
                let buffer = self
                    .reader
                    .as_mut()
                    .expect("opened reader")
                    .fill_buf()
                    .map_err(|_| {
                        DataFusionError::Execution(format!(
                            "{} line {}: source read failed",
                            self.file.key,
                            first + offsets.len() as i64 - 1
                        ))
                    })?;
                if buffer.is_empty() {
                    break;
                }
                let n = buffer
                    .iter()
                    .position(|b| *b == b'\n')
                    .map_or(buffer.len(), |n| n + 1);
                if raw.len() - start + n > self.max_line {
                    return Err(self.error(
                        first + offsets.len() as i64 - 1,
                        "line exceeds configured allowance",
                    ));
                }
                // Count without allocating, then admit raw growth and the
                // native decoder's byte/tape/schema expansion before either.
                let allowance = allocation.grow(&buffer[..n], fixed, self.item_per_value)?;
                self.reservation.try_resize(allowance)?;
                let newline = buffer[n - 1] == b'\n';
                raw.extend_from_slice(&buffer[..n]);
                self.reader.as_mut().expect("opened reader").consume(n);
                if newline {
                    break;
                }
            }
            if raw.len() == start {
                break;
            }
            offsets.push(raw.len());
        }
        if offsets.len() == 1 {
            self.reservation.free();
            return Ok(None);
        }
        let mut decoder = ReaderBuilder::new(self.input.clone())
            .with_batch_size(self.batch_rows)
            .with_ignore_type_conflicts(true)
            .build(raw.as_slice())
            .map_err(|_| self.error(first, "decoder setup failed"))?;
        let batch = match decoder.next().transpose() {
            Ok(Some(batch)) if batch.num_rows() == offsets.len() - 1 => batch,
            _ => {
                for (index, pair) in offsets.windows(2).enumerate() {
                    let parsed =
                        serde_json::from_slice::<serde_json::Value>(&raw[pair[0]..pair[1]]);
                    if !matches!(parsed, Ok(serde_json::Value::Object(_))) {
                        return Err(self.error(first + index as i64, "JSON decode failed"));
                    }
                }
                return Err(self.error(first, "typed JSON decode failed"));
            }
        };
        drop(decoder);
        let originals = offsets
            .windows(2)
            .enumerate()
            .map(|(i, p)| {
                std::str::from_utf8(&raw[p[0]..p[1]])
                    .map_err(|_| self.error(first + i as i64, "invalid UTF-8"))
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = batch.num_rows();
        let mut columns = batch.columns().to_vec();
        columns.extend([
            Arc::new(StringArray::from(vec![self.file.key.as_str(); rows])) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(first..first + rows as i64)),
            Arc::new(StringArray::from(originals)),
        ]);
        self.line += rows as i64;
        let result = RecordBatch::try_new(self.schema.clone(), columns)?;
        if result.get_array_memory_size() > self.max_batch_bytes {
            return Err(self.error(first, "decoded batch exceeds configured byte allowance"));
        }
        // Native consumers reserve the returned batch before asking for the
        // next one. Release completed decoder/raw workspace now, rather than
        // retaining the preflight upper bound between reader polls.
        drop(raw);
        let bytes = result.get_array_memory_size();
        if bytes > self.reservation.size() {
            return Err(DataFusionError::Internal(
                "JSON decoded output exceeded its preflight bound".into(),
            ));
        }
        self.reservation.shrink(self.reservation.size() - bytes);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn escaped_strings_have_the_same_bound_across_every_chunk_boundary() {
        let raw = br#"{"a":"quoted \\\" text","b":[null,{"c":12}]}"#;
        let expected = DecodeAllocation::default().grow(raw, 4096, 256).unwrap();
        for boundary in 0..=raw.len() {
            let mut allocation = DecodeAllocation::default();
            allocation.grow(&raw[..boundary], 4096, 256).unwrap();
            assert_eq!(
                allocation.grow(&raw[boundary..], 4096, 256).unwrap(),
                expected
            );
        }
        let sparse = format!("{{\"a\":\"{}\"}}", "x".repeat(32 << 10));
        assert!(
            DecodeAllocation::default()
                .grow(sparse.as_bytes(), 0, 0)
                .unwrap()
                < sparse.len() * 12
        );
        let dense = format!("[{}]", vec!["0"; 16 << 10].join(","));
        assert!(
            DecodeAllocation::default()
                .grow(dense.as_bytes(), 0, 0)
                .unwrap()
                > dense.len() * 32
        );
    }
    #[test]
    fn sparse_variable_elements_include_fixed_width_and_nested_schema_cost() {
        let child = Arc::new(Field::new("item", DataType::FixedSizeBinary(4096), true));
        let field = Field::new("items", DataType::List(child), true);
        assert!(item_allowance(&field).unwrap() > 4096);
        assert!(
            DecodeAllocation::default()
                .grow(
                    b"{\"items\":[null,null]}",
                    0,
                    item_allowance(&field).unwrap()
                )
                .unwrap()
                > 8192
        );
    }
}

#[cfg(test)]
mod reader_lifetime_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn gzip_reader_keeps_workspace_admitted_between_yields_and_closes_at_eof() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("one.gz");
        let mut writer = flate2::write::GzEncoder::new(
            File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        writer.write_all(b"{\"id\":1}\n").unwrap();
        writer.finish().unwrap();
        let limits = crate::budget::Limits {
            memory_bytes: 64 << 20,
            ..crate::budget::Limits::default()
        };
        let budget = crate::budget::Budget::new(limits.clone()).unwrap();
        let input = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let files = vec![JsonLinesFile {
            key: "one".into(),
            path,
        }];
        let schema = provider(files.clone(), input.clone(), &limits)
            .unwrap()
            .schema();
        let pool = &budget.runtime_env().memory_pool;
        let before = pool.reserved();
        let make = || Reader {
            file: files[0].clone(),
            input: input.clone(),
            schema: schema.clone(),
            reader: None,
            line: 0,
            max_line: 1 << 20,
            batch_rows: 64,
            fixed_per_row: 160,
            item_per_value: 0,
            max_batch_bytes: 1 << 20,
            reservation: MemoryConsumer::new("JSON-lines lifetime fixture").register(pool),
        };
        let mut reader = make();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.reader.is_some());
        assert!(
            reader.reservation.size() >= batch.get_array_memory_size() + (40 << 10),
            "gzip history and buffered reader must retain separate admission"
        );
        drop(batch);
        assert!(reader.next().unwrap().is_none());
        assert!(
            reader.reader.is_none(),
            "close gzip buffers before releasing EOF admission"
        );
        assert_eq!(pool.reserved(), before);
        drop(reader);
        let mut reader = make();
        drop(reader.next().unwrap().unwrap());
        assert!(pool.reserved() > before);
        drop(reader);
        assert_eq!(pool.reserved(), before);
    }
}
