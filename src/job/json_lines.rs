//! Generic gzip JSON-lines input; source keys and schemas belong to the caller.
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
    io::{BufRead, BufReader},
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
        DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
            field_allowance(child)?
        }
        _ => 0,
    };
    children.checked_add(128)
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
                // Half the configured memory belongs to the DataFusion pool.
                max_line: (limits.memory_bytes / 64).min(512 << 20),
                batch_rows: limits.batch_rows.min(64),
                fixed_per_row,
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
    reader: Option<BufReader<MultiGzDecoder<File>>>,
    line: i64,
    max_line: usize,
    batch_rows: usize,
    fixed_per_row: usize,
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
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| self.error(self.line + 1, "schema and evidence allowance overflow"))?;
        self.reservation.try_resize(fixed)?;
        if self.reader.is_none() {
            self.reader = Some(BufReader::new(MultiGzDecoder::new(
                File::open(&self.file.path).map_err(|_| self.error(1, "source open failed"))?,
            )));
        }
        let first = self.line + 1;
        let mut raw = Vec::new();
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
                            "{} line {}: gzip read failed",
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
                // Admit raw storage and decoder expansion before extending the buffer.
                // Decoder tape, variable data, raw evidence and Vec capacity.
                // Schema and repeated source-key storage are admitted separately.
                let allowance = (raw.len() + n)
                    .checked_mul(32)
                    .and_then(|n| n.checked_add(fixed))
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted("JSON line allocation overflow".into())
                    })?;
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
        Ok(Some(result))
    }
}
