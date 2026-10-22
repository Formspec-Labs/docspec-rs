//! Demand-driven Arrow IPC encoding with admission owned by outgoing chunks.

use crate::{
    Engine, Error, Result,
    budget::Budget,
    model::{Id, InputSpec, OperationId, RunSpec, SourceRelationship, UpdateSpec},
};
use arrow::ipc::{
    reader::{StreamDecoder, StreamReader},
    writer::StreamWriter,
};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, header},
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn encode(
    batches: SendableRecordBatchStream,
    budget: Budget,
) -> Result<impl Stream<Item = Result<Bytes>> + Send + use<>> {
    if batches.schema().flattened_fields().iter().any(|field| {
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::Dictionary(..)
        )
    }) {
        return Err(Error::refusal(
            "unsupported_ipc_schema",
            "dictionary state retention is not qualified for HTTP streaming",
        ));
    }
    // Arrow temporarily encodes batch buffers before writing them. The
    // sink and each transferred chunk have separate reservations below.
    let retained = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    let limit = budget
        .limits
        .max_batch_bytes
        .saturating_mul(2)
        .max(budget.limits.max_metadata_bytes);
    let first = budget.reserve(limit)?;
    let writer = StreamWriter::try_new(
        Sink {
            bytes: Vec::new(),
            limit,
        },
        &batches.schema(),
    )?;
    Ok(futures::stream::try_unfold(
        (batches, writer, Some(first), budget, retained, false),
        |(mut batches, mut writer, first, budget, retained, finished)| async move {
            if finished {
                return Ok(None);
            }
            let mut finished = false;
            let reservation = if let Some(first) = first {
                first
            } else {
                let reservation = budget.reserve(writer.get_ref().limit)?;
                if let Some(batch) = batches.try_next().await? {
                    if batch.schema() != batches.schema() {
                        return Err(Error::refusal(
                            "invalid_schema",
                            "IPC batch differs from the declared stream schema",
                        ));
                    }
                    if batch.get_array_memory_size() > budget.limits.max_batch_bytes {
                        return Err(Error::refusal(
                            "oversized_batch",
                            "IPC input exceeds the batch allowance",
                        ));
                    }
                    writer.write(&batch)?;
                } else {
                    writer.finish()?;
                    finished = true;
                }
                reservation
            };
            let bytes = Bytes::from_owner(Chunk {
                bytes: std::mem::take(&mut writer.get_mut().bytes),
                _reservation: reservation,
            });
            // No producer task or queue: the next batch is requested only when
            // the HTTP body asks for it. A failed batch never emits a success end.
            tokio::task::consume_budget().await;
            Ok(Some((
                bytes,
                (batches, writer, None, budget, retained, finished),
            )))
        },
    ))
}

struct Chunk {
    bytes: Vec<u8>,
    _reservation: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Chunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct Sink {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("IPC chunk length overflow"))?;
        if length > self.limit {
            return Err(std::io::Error::other(
                "encoded IPC chunk exceeds its byte allowance",
            ));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) struct StagedIpc<T> {
    pub spec: T,
    pub staged_path: PathBuf,
}

impl<T> StagedIpc<T> {
    pub(super) fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.staged_path);
    }
}

pub(super) fn is_arrow_stream(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/vnd.apache.arrow.stream"))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcRunSpec {
    dataset: Id,
    operation: OperationId,
    #[serde(default)]
    base: Option<Id>,
    #[serde(default)]
    relationship: Option<SourceRelationship>,
    #[serde(default)]
    #[allow(dead_code)]
    input: Option<String>,
    #[serde(default)]
    expected_digest: Option<String>,
    #[serde(default = "default_ipc_sql")]
    sql: String,
    definition: String,
}

fn default_ipc_sql() -> String {
    "SELECT * FROM input".to_owned()
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcInputSpec {
    #[serde(default)]
    #[allow(dead_code)]
    path: Option<String>,
    #[serde(default)]
    digest: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcUpdateSpec {
    dataset: Id,
    operation: OperationId,
    definition: String,
    base: Id,
    keys: Vec<String>,
    #[serde(default)]
    replacements: IpcInputSpec,
    #[serde(default)]
    relationship: Option<SourceRelationship>,
    #[serde(default)]
    deletions: Option<InputSpec>,
}

pub(super) async fn stage_run(
    engine: &Engine,
    headers: &HeaderMap,
    body: Body,
) -> Result<StagedIpc<RunSpec>> {
    let spec_bytes = decode_spec_header(headers)?;
    let ipc_spec: IpcRunSpec = serde_json::from_slice(&spec_bytes).map_err(|e| {
        Error::refusal(
            "invalid_spec",
            format!("invalid RunSpec in X-Docspec-Spec header: {e}"),
        )
    })?;
    let expected_digest = resolve_expected_digest(headers, ipc_spec.expected_digest.as_deref())?;
    let (staged_path, parquet_digest) = stage_ipc_stream_to_parquet(
        &engine.scratch_dir(),
        &ipc_spec.dataset,
        &ipc_spec.operation,
        expected_digest.as_deref(),
        body,
        &engine.read_budget(),
    )
    .await?;
    let spec = RunSpec {
        dataset: ipc_spec.dataset,
        operation: ipc_spec.operation,
        base: ipc_spec.base,
        relationship: ipc_spec.relationship,
        input: staged_path.to_string_lossy().into_owned(),
        expected_digest: parquet_digest,
        sql: if ipc_spec.sql.trim().is_empty() {
            default_ipc_sql()
        } else {
            ipc_spec.sql
        },
        definition: ipc_spec.definition,
    };
    Ok(StagedIpc { spec, staged_path })
}

pub(super) async fn stage_update(
    engine: &Engine,
    headers: &HeaderMap,
    body: Body,
) -> Result<StagedIpc<UpdateSpec>> {
    let spec_bytes = decode_spec_header(headers)?;
    let ipc_spec: IpcUpdateSpec = serde_json::from_slice(&spec_bytes).map_err(|e| {
        Error::refusal(
            "invalid_spec",
            format!("invalid UpdateSpec in X-Docspec-Spec header: {e}"),
        )
    })?;
    let expected_digest =
        resolve_expected_digest(headers, ipc_spec.replacements.digest.as_deref())?;
    let (staged_path, parquet_digest) = stage_ipc_stream_to_parquet(
        &engine.scratch_dir(),
        &ipc_spec.dataset,
        &ipc_spec.operation,
        expected_digest.as_deref(),
        body,
        &engine.read_budget(),
    )
    .await?;
    let spec = UpdateSpec {
        dataset: ipc_spec.dataset,
        operation: ipc_spec.operation,
        definition: ipc_spec.definition,
        base: ipc_spec.base,
        keys: ipc_spec.keys,
        replacements: InputSpec {
            path: staged_path.to_string_lossy().into_owned(),
            digest: parquet_digest,
        },
        relationship: ipc_spec.relationship,
        deletions: ipc_spec.deletions,
    };
    Ok(StagedIpc { spec, staged_path })
}

fn decode_spec_header(headers: &HeaderMap) -> Result<Vec<u8>> {
    if let Some(value) = headers.get("x-docspec-spec") {
        let raw = value.to_str().map_err(|_| {
            Error::refusal("invalid_spec", "X-Docspec-Spec header must be valid ASCII")
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(Error::refusal(
                "invalid_spec",
                "X-Docspec-Spec header cannot be empty",
            ));
        }
        if trimmed.starts_with('{') {
            return Ok(trimmed.as_bytes().to_vec());
        }
        return decode_base64(trimmed);
    }
    if let Some(value) = headers.get("x-docspec-spec-base64") {
        let raw = value.to_str().map_err(|_| {
            Error::refusal(
                "invalid_spec",
                "X-Docspec-Spec-Base64 header must be valid ASCII",
            )
        })?;
        return decode_base64(raw.trim());
    }
    Err(Error::refusal(
        "invalid_spec",
        "Arrow IPC write stream requires an X-Docspec-Spec header",
    ))
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    fn val(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }
    let clean: Vec<u8> = input
        .bytes()
        .filter(|b| !b.is_ascii_whitespace() && *b != b'=')
        .collect();
    if clean.is_empty() || clean.len() % 4 == 1 {
        return Err(Error::refusal(
            "invalid_spec",
            "invalid base64 in X-Docspec-Spec header",
        ));
    }
    let mut out = Vec::with_capacity((clean.len() * 3) / 4);
    for chunk in clean.chunks(4) {
        let b0 = val(chunk[0]).ok_or_else(|| {
            Error::refusal("invalid_spec", "invalid base64 in X-Docspec-Spec header")
        })?;
        let b1 = val(chunk[1]).ok_or_else(|| {
            Error::refusal("invalid_spec", "invalid base64 in X-Docspec-Spec header")
        })?;
        out.push((b0 << 2) | (b1 >> 4));
        if chunk.len() > 2 {
            let b2 = val(chunk[2]).ok_or_else(|| {
                Error::refusal("invalid_spec", "invalid base64 in X-Docspec-Spec header")
            })?;
            out.push((b1 << 4) | (b2 >> 2));
            if chunk.len() > 3 {
                let b3 = val(chunk[3]).ok_or_else(|| {
                    Error::refusal("invalid_spec", "invalid base64 in X-Docspec-Spec header")
                })?;
                out.push((b2 << 6) | b3);
            }
        }
    }
    Ok(out)
}

fn resolve_expected_digest(
    headers: &HeaderMap,
    spec_digest: Option<&str>,
) -> Result<Option<String>> {
    let header_digest = if let Some(value) = headers.get("x-docspec-input-blake3") {
        let raw = value
            .to_str()
            .map_err(|_| {
                Error::refusal(
                    "input_digest_mismatch",
                    "X-Docspec-Input-Blake3 header must be valid ASCII",
                )
            })?
            .trim();
        if raw.is_empty() {
            return Err(Error::refusal(
                "input_digest_mismatch",
                "X-Docspec-Input-Blake3 header cannot be empty",
            ));
        }
        Some(raw.to_owned())
    } else {
        None
    };
    let spec_digest = spec_digest.map(str::trim).filter(|s| !s.is_empty());
    if let (Some(h), Some(s)) = (header_digest.as_deref(), spec_digest)
        && h != s
    {
        return Err(Error::refusal(
            "input_digest_mismatch",
            "X-Docspec-Input-Blake3 header and spec expected digest disagree",
        ));
    }
    Ok(header_digest.or_else(|| spec_digest.map(str::to_owned)))
}

struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct HashingFileWriter {
    file: std::fs::File,
    hasher: blake3::Hasher,
    written_bytes: u64,
    max_bytes: u64,
}

impl Write for HashingFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let next = self
            .written_bytes
            .checked_add(buf.len() as u64)
            .ok_or_else(|| std::io::Error::other("staged Parquet file size overflow"))?;
        if next > self.max_bytes {
            return Err(std::io::Error::other(
                "staged Parquet file exceeds scratch storage allowance",
            ));
        }
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.written_bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn validate_ipc_schema(schema: &arrow::datatypes::Schema) -> Result<()> {
    if schema.flattened_fields().iter().any(|field| {
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::Dictionary(..)
        )
    }) {
        return Err(Error::refusal(
            "unsupported_ipc_schema",
            "dictionary state retention is not qualified for HTTP streaming",
        ));
    }
    Ok(())
}

fn open_parquet_writer(
    path: &Path,
    schema: arrow::datatypes::SchemaRef,
    max_bytes: u64,
) -> Result<ArrowWriter<HashingFileWriter>> {
    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid zstd level"),
        ))
        .set_column_index_truncate_length(Some(crate::storage::STATISTICS_TRUNCATE_BYTES))
        .set_statistics_truncate_length(Some(crate::storage::STATISTICS_TRUNCATE_BYTES))
        .set_created_by("docspec-ipc".to_owned())
        .build();
    let writer = HashingFileWriter {
        file,
        hasher: blake3::Hasher::new(),
        written_bytes: 0,
        max_bytes,
    };
    ArrowWriter::try_new(writer, schema, Some(props))
        .map_err(|e| Error::refusal("parquet_write", format!("failed opening staged Parquet: {e}")))
}

fn map_parquet_write_err(err: parquet::errors::ParquetError) -> Error {
    let msg = err.to_string();
    if msg.contains("exceeds scratch storage allowance") || msg.contains("size overflow") {
        Error::refusal("disk_capacity", msg)
    } else {
        Error::refusal("parquet_write", format!("failed writing staged Parquet: {msg}"))
    }
}

async fn stage_ipc_stream_to_parquet(
    scratch_dir: &Path,
    dataset: &Id,
    operation: &Id,
    expected_digest: Option<&str>,
    body: Body,
    budget: &Budget,
) -> Result<(PathBuf, String)> {
    std::fs::create_dir_all(scratch_dir)?;
    let mut temp_guard = TempFileGuard::new(
        scratch_dir.join(format!("ipc-stage-{}.parquet", uuid::Uuid::new_v4())),
    );
    let mut decoder = StreamDecoder::new();
    let mut ipc_hasher = blake3::Hasher::new();
    let mut total_ipc_bytes = 0u64;
    let mut pre_batch_bytes: Option<Vec<u8>> = Some(Vec::new());
    let mut writer: Option<ArrowWriter<HashingFileWriter>> = None;
    let mut stream_schema: Option<arrow::datatypes::SchemaRef> = None;
    let max_chunk_bytes = budget
        .limits
        .max_batch_bytes
        .saturating_mul(2)
        .max(budget.limits.max_metadata_bytes);

    let mut data_stream = body.into_data_stream();
    while let Some(chunk) = data_stream.next().await {
        let chunk = chunk.map_err(|e| {
            Error::refusal(
                "invalid_ipc_stream",
                format!("failed reading HTTP Arrow IPC stream: {e}"),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        if chunk.len() > max_chunk_bytes {
            return Err(Error::refusal(
                "oversized_batch",
                "IPC input chunk exceeds the batch allowance",
            ));
        }
        total_ipc_bytes = total_ipc_bytes
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| {
                Error::refusal("disk_capacity", "IPC input stream byte count overflowed")
            })?;
        if total_ipc_bytes > budget.limits.scratch_bytes {
            return Err(Error::refusal(
                "disk_capacity",
                "IPC input stream exceeds scratch storage allowance",
            ));
        }
        let _chunk_permit = budget.reserve(chunk.len())?;
        ipc_hasher.update(&chunk);
        if let Some(buf) = pre_batch_bytes.as_mut() {
            if buf.len().saturating_add(chunk.len()) <= budget.limits.max_metadata_bytes {
                buf.extend_from_slice(&chunk);
            } else {
                pre_batch_bytes = None;
            }
        }
        let mut buffer = arrow::buffer::Buffer::from_slice_ref(&chunk);
        while let Some(batch) = decoder.decode(&mut buffer).map_err(|e| {
            Error::refusal("invalid_ipc_stream", format!("invalid Arrow IPC stream: {e}"))
        })? {
            pre_batch_bytes = None;
            let batch_bytes = batch.get_array_memory_size();
            if batch_bytes > budget.limits.max_batch_bytes {
                return Err(Error::refusal(
                    "oversized_batch",
                    "IPC input exceeds the batch allowance",
                ));
            }
            let _batch_permit = budget.reserve(batch_bytes.max(1))?;
            if let Some(expected_schema) = &stream_schema {
                if &batch.schema() != expected_schema {
                    return Err(Error::refusal(
                        "invalid_schema",
                        "IPC batch differs from the declared stream schema",
                    ));
                }
            } else {
                validate_ipc_schema(&batch.schema())?;
                stream_schema = Some(batch.schema());
                writer = Some(open_parquet_writer(
                    &temp_guard.path,
                    batch.schema(),
                    budget.limits.scratch_bytes,
                )?);
            }
            writer
                .as_mut()
                .expect("writer initialized")
                .write(&batch)
                .map_err(map_parquet_write_err)?;
        }
    }
    decoder.finish().map_err(|e| {
        Error::refusal(
            "invalid_ipc_stream",
            format!("truncated or incomplete Arrow IPC stream: {e}"),
        )
    })?;

    if writer.is_none() {
        let raw = pre_batch_bytes.ok_or_else(|| {
            Error::refusal(
                "invalid_ipc_stream",
                "Arrow IPC stream contained no schema or batches",
            )
        })?;
        if raw.is_empty() {
            return Err(Error::refusal(
                "invalid_ipc_stream",
                "Arrow IPC stream is empty",
            ));
        }
        let mut reader = StreamReader::try_new(std::io::Cursor::new(&raw), None).map_err(|e| {
            Error::refusal("invalid_ipc_stream", format!("invalid Arrow IPC stream: {e}"))
        })?;
        let schema = reader.schema();
        validate_ipc_schema(&schema)?;
        let mut out_writer =
            open_parquet_writer(&temp_guard.path, schema, budget.limits.scratch_bytes)?;
        for batch in &mut reader {
            let batch = batch.map_err(|e| {
                Error::refusal("invalid_ipc_stream", format!("invalid Arrow IPC stream: {e}"))
            })?;
            let batch_bytes = batch.get_array_memory_size();
            if batch_bytes > budget.limits.max_batch_bytes {
                return Err(Error::refusal(
                    "oversized_batch",
                    "IPC input exceeds the batch allowance",
                ));
            }
            let _batch_permit = budget.reserve(batch_bytes.max(1))?;
            out_writer.write(&batch).map_err(map_parquet_write_err)?;
        }
        writer = Some(out_writer);
    }

    let mut inner = writer
        .expect("writer initialized")
        .into_inner()
        .map_err(map_parquet_write_err)?;
    inner.file.flush()?;
    inner.file.sync_all()?;
    let parquet_digest = inner.hasher.finalize().to_hex().to_string();
    let ipc_digest = ipc_hasher.finalize().to_hex().to_string();

    if let Some(expected) = expected_digest
        && expected != ipc_digest
        && expected != parquet_digest
    {
        return Err(Error::refusal(
            "input_digest_mismatch",
            "supplied Arrow IPC stream does not match the expected BLAKE3 digest",
        ));
    }

    let final_path = scratch_dir.join(format!("ipc-{dataset}-{operation}-{ipc_digest}.parquet"));
    if !final_path.exists() {
        std::fs::rename(&temp_guard.path, &final_path)?;
        temp_guard.disarm();
    }
    Ok((final_path, parquet_digest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn budget() -> Budget {
        Budget::new(crate::budget::Limits {
            memory_bytes: 64 << 10,
            max_batch_bytes: 4096,
            max_metadata_bytes: 4096,
            ..crate::budget::Limits::default()
        })
        .unwrap()
    }
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(7), None, Some(7)]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn demand_controls_batch_reads_and_chunks_keep_admission_until_last_clone_drops() {
        let budget = budget();
        let batch = batch();
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]).inspect(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        ));
        let mut stream = encode(batches, budget.clone()).unwrap().boxed();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let schema = stream.try_next().await.unwrap().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(schema);
        let first = stream.try_next().await.unwrap().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let clone = first.clone();
        drop(stream);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let remainder = budget
            .reserve(budget.limits.memory_bytes / 2 - 8192)
            .unwrap();
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop(first);
        assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
        drop(clone);
        let recovered = budget.reserve(8192).unwrap();
        drop((remainder, recovered));
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }

    #[tokio::test]
    async fn encoding_preserves_strings_nulls_duplicates_and_complete_empty_streams() {
        for empty in [false, true] {
            let first = StringArray::from(vec![Some("a"), None, Some("a")]);
            let second = StringArray::from(vec![Some("changed"), Some(""), None]);
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
            let expected = if empty {
                vec![]
            } else {
                vec![
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(first)]).unwrap(),
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(second)]).unwrap(),
                ]
            };
            let batches = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(expected.clone().into_iter().map(Ok)),
            ));
            let mut stream = encode(batches, budget()).unwrap().boxed();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.unwrap() {
                bytes.extend_from_slice(&chunk);
            }
            assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
            let reader =
                arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap();
            assert_eq!(reader.schema(), schema);
            let actual = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn dictionaries_and_changing_batch_schemas_refuse() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for data_type in [
            dictionary.clone(),
            DataType::List(Arc::new(Field::new("item", dictionary, true))),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
            let batches = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            ));
            assert_eq!(
                encode(batches, budget()).err().unwrap().code(),
                "unsupported_ipc_schema"
            );
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            true,
        )]));
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch())]),
        ));
        let mut stream = encode(batches, budget()).unwrap().boxed();
        drop(stream.try_next().await.unwrap());
        assert_eq!(
            stream.try_next().await.unwrap_err().code(),
            "invalid_schema"
        );
    }

    #[tokio::test]
    async fn failures_abort_the_body_without_emitting_a_successful_end() {
        let budget = budget();
        let batch = batch();
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![
                Ok(batch),
                Err(datafusion::error::DataFusionError::External(Box::new(
                    Error::refusal("read_failed", "injected source failure"),
                ))),
            ]),
        ));
        let body = axum::body::Body::from_stream(encode(batches, budget.clone()).unwrap());
        assert!(axum::body::to_bytes(body, 1 << 20).await.is_err());
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());

        let large = Arc::new(StringArray::from(vec!["x".repeat(8192)]));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let batches = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(RecordBatch::try_new(schema, vec![large]).unwrap())]),
        ));
        let mut stream = encode(batches, budget.clone()).unwrap().boxed();
        drop(stream.try_next().await.unwrap());
        assert_eq!(
            stream.try_next().await.unwrap_err().code(),
            "oversized_batch"
        );
        drop(stream);
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }

    #[test]
    fn encoded_chunks_refuse_before_growing_beyond_their_admission() {
        let mut sink = Sink {
            bytes: Vec::new(),
            limit: 7,
        };
        sink.write_all(b"1234").unwrap();
        assert!(sink.write_all(b"5678").is_err());
        assert_eq!(sink.bytes, b"1234");
        sink.write_all(b"567").unwrap();
        assert_eq!(sink.bytes.len(), 7);
    }
}
