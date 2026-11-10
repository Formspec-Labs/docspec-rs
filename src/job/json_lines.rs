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

// Keep reader buffers and gzip history admitted until the reader closes.
const READER_BYTES: usize = 256 << 10;

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

fn allocation_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("JSON allocation size overflow".into())
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

/// Names the file and the operating-system error behind a failed `action`.
///
/// A bare "open failed" cannot tell a missing file from an exhausted
/// descriptor limit; the error kind and OS code can.
fn source_error(
    file: &JsonLinesFile,
    line: i64,
    action: &str,
    error: &std::io::Error,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "{} line {line}: source {action} failed: {}: {error} ({:?})",
        file.key,
        file.path.display(),
        error.kind()
    ))
}

struct Reader {
    file: JsonLinesFile,
    input: SchemaRef,
    schema: SchemaRef,
    reader: Option<BufReader<Box<dyn Read + Send>>>,
    line: i64,
    max_line: usize,
    batch_rows: usize,
    max_batch_bytes: usize,
    reservation: MemoryReservation,
}
impl Reader {
    fn admit(
        reservation: &mut MemoryReservation,
        key: &str,
        line: i64,
        bytes: usize,
    ) -> Result<()> {
        reservation
            .try_resize(bytes)
            .map_err(|error| error.context(format!("{key} line {line}")))
    }

    fn error(&self, line: i64, message: &str) -> DataFusionError {
        DataFusionError::Execution(format!("{} line {line}: {message}", self.file.key))
    }
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        Self::admit(
            &mut self.reservation,
            &self.file.key,
            self.line + 1,
            READER_BYTES,
        )?;
        if self.reader.is_none() {
            let mut file = File::open(&self.file.path)
                .map_err(|error| source_error(&self.file, 1, "open", &error))?;
            let mut magic = [0; 2];
            match file.read_exact(&mut magic) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
                Err(error) => return Err(source_error(&self.file, 1, "read", &error)),
            }
            file.rewind()
                .map_err(|error| source_error(&self.file, 1, "rewind", &error))?;
            let source: Box<dyn Read + Send> = if magic == [0x1f, 0x8b] {
                Box::new(MultiGzDecoder::new(file))
            } else {
                Box::new(file)
            };
            self.reader = Some(BufReader::new(source));
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
                    .map_err(|error| {
                        let line = first + offsets.len() as i64 - 1;
                        source_error(&self.file, line, "read", &error)
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
                let line = first + offsets.len() as i64 - 1;
                let required = raw.len().checked_add(n).ok_or_else(allocation_overflow)?;
                if required > raw.capacity() {
                    let capacity = raw
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(allocation_overflow)?
                        .max(required);
                    // During reallocation both old and new allocations can be
                    // live. Charge their actual requested sizes, then retain
                    // only the resulting raw buffer's capacity.
                    let growing = capacity
                        .checked_add(raw.capacity())
                        .and_then(|bytes| bytes.checked_add(READER_BYTES))
                        .ok_or_else(allocation_overflow)?;
                    Self::admit(&mut self.reservation, &self.file.key, line, growing)?;
                    raw.try_reserve_exact(capacity - raw.len())
                        .map_err(|error| {
                            DataFusionError::ResourcesExhausted(format!(
                                "{} line {line}: raw buffer allocation failed: {error}",
                                self.file.key
                            ))
                        })?;
                    Self::admit(
                        &mut self.reservation,
                        &self.file.key,
                        line,
                        raw.capacity()
                            .checked_add(READER_BYTES)
                            .ok_or_else(allocation_overflow)?,
                    )?;
                }
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
            self.reader = None;
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
        // Native Arrow owns decoding and its temporary workspace. Account the
        // resulting arrays by their observed allocation, while retaining one
        // charge for the held raw buffer. No schema or structural multiplier.
        let typed_bytes = batch.get_array_memory_size();
        let held = typed_bytes
            .checked_add(raw.capacity())
            .and_then(|bytes| bytes.checked_add(READER_BYTES))
            .ok_or_else(allocation_overflow)?;
        Self::admit(&mut self.reservation, &self.file.key, first, held)?;
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
        let decoded = result.get_array_memory_size();
        let held = decoded
            .checked_add(raw.capacity())
            .and_then(|bytes| bytes.checked_add(READER_BYTES))
            .ok_or_else(allocation_overflow)?;
        Self::admit(&mut self.reservation, &self.file.key, first, held)?;
        tracing::debug!(
            raw_buffer_bytes = raw.capacity(),
            typed_array_bytes = typed_bytes,
            decoded_array_bytes = decoded,
            source_line = first,
            rows,
            "JSON-lines decoded allocation"
        );
        if decoded > self.max_batch_bytes {
            return Err(self.error(first, "decoded batch exceeds configured byte allowance"));
        }
        drop(raw);
        Self::admit(
            &mut self.reservation,
            &self.file.key,
            first,
            decoded
                .checked_add(READER_BYTES)
                .ok_or_else(allocation_overflow)?,
        )?;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod reader_lifetime_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_source_reports_its_path_and_the_os_error() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("absent.jsonl");
        let limits = crate::budget::Limits::default();
        let budget = crate::budget::Budget::new(limits.clone()).unwrap();
        let input = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let file = JsonLinesFile {
            key: "accepted_versions".into(),
            path: path.clone(),
        };
        let schema = provider(vec![file.clone()], input.clone(), &limits)
            .unwrap()
            .schema();
        let mut reader = Reader {
            file,
            input,
            schema,
            reader: None,
            line: 0,
            max_line: 1 << 20,
            batch_rows: 64,
            max_batch_bytes: 1 << 20,
            reservation: MemoryConsumer::new("JSON-lines missing source")
                .register(&budget.runtime_env().memory_pool),
        };
        let message = reader.next().unwrap_err().to_string();
        // ENOENT is 2 on every Unix the engine builds for.
        assert_eq!(
            message,
            format!(
                "Execution error: accepted_versions line 1: source open failed: {}: \
                 No such file or directory (os error 2) (NotFound)",
                path.display()
            )
        );
    }

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
