// Rust guideline compliant 2026-02-21
//! Streaming Parquet-to-N-Quads export with exact input and output SHA-256 receipts.

use crate::{Error, Result, budget::Limits};
use arrow::{
    array::{
        Array, AsArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
        StringArray, StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

const IO_BUF_BYTES: usize = 64 * 1024;

/// Digest identity of the source Parquet file consumed by [`export_nquads`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportNquadsInput {
    pub path: PathBuf,
    pub sha256: String,
}

/// Digest and quad-count summary of the written `.nq` output file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportNquadsOutput {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    pub quads: u64,
}

/// Machine-readable receipt returned by `docspec export-nquads`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportNquadsResult {
    pub status: String,
    pub input: ExportNquadsInput,
    pub output: ExportNquadsOutput,
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

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; IO_BUF_BYTES];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn resolve_output_path(output: &Path) -> Result<(PathBuf, PathBuf, String)> {
    let file_name = output
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| !n.is_empty())
        .ok_or_else(|| {
            Error::refusal(
                "invalid_export",
                format!("invalid N-Quads output path: {}", output.display()),
            )
        })?
        .to_owned();
    let raw_parent = match output.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(raw_parent)?;
    let parent_abs = std::fs::canonicalize(raw_parent)?;
    let output_abs = parent_abs.join(&file_name);
    Ok((parent_abs, output_abs, file_name))
}

fn dict_key_index(keys: &dyn Array, index: usize) -> Option<usize> {
    let any = keys.as_any();
    if let Some(a) = any.downcast_ref::<Int8Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int16Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int32Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<UInt8Array>() {
        Some(usize::from(a.value(index)))
    } else if let Some(a) = any.downcast_ref::<UInt16Array>() {
        Some(usize::from(a.value(index)))
    } else if let Some(a) = any.downcast_ref::<UInt32Array>() {
        usize::try_from(a.value(index)).ok()
    } else if let Some(a) = any.downcast_ref::<UInt64Array>() {
        usize::try_from(a.value(index)).ok()
    } else {
        None
    }
}

fn string_at<'a>(array: &'a dyn Array, index: usize, column: &str) -> Result<Option<&'a str>> {
    if array.data_type() == &DataType::Null || array.is_null(index) {
        return Ok(None);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(a.value(index)));
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(a.value(index)));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(a.value(index)));
    }
    if let Some(dict) = array.as_any_dictionary_opt() {
        let val_idx = dict_key_index(dict.keys(), index)
            .filter(|&k| k < dict.values().len())
            .ok_or_else(|| {
                Error::refusal(
                    "invalid_export",
                    format!("Dictionary key index out of bounds in column {column:?}"),
                )
            })?;
        return string_at(dict.values().as_ref(), val_idx, column);
    }
    Err(Error::refusal(
        "invalid_export",
        format!(
            "export-nquads column {column:?} must be Utf8, LargeUtf8 or Utf8View (found {})",
            array.data_type()
        ),
    ))
}

/// Streams a projected string column from a Parquet file to an atomic `.nq` file.
///
/// # Errors
/// Refuses missing/unreadable input files, missing or non-string columns, and I/O errors.
pub fn export_nquads(
    input: &Path,
    output: &Path,
    column: &str,
    limits: &Limits,
) -> Result<ExportNquadsResult> {
    if column.trim().is_empty() {
        return Err(Error::refusal(
            "invalid_export",
            "export-nquads --column must not be empty",
        ));
    }
    let input_abs = std::fs::canonicalize(input).map_err(|err| {
        Error::refusal(
            "missing_input",
            format!("Parquet input {} is unavailable: {err}", input.display()),
        )
    })?;
    if !std::fs::metadata(&input_abs)?.is_file() {
        return Err(Error::refusal(
            "invalid_export",
            format!(
                "Parquet input {} is not a regular file",
                input_abs.display()
            ),
        ));
    }
    let (parent_abs, output_abs, file_name) = resolve_output_path(output)?;
    let input_sha256 = sha256_file(&input_abs)?;

    let file = File::open(&input_abs)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let root_fields = builder.parquet_schema().root_schema().get_fields();
    let root_col_idx = root_fields
        .iter()
        .position(|f| f.name() == column)
        .or_else(|| builder.schema().index_of(column).ok())
        .ok_or_else(|| {
            Error::refusal(
                "invalid_export",
                format!(
                    "Parquet input {} does not contain column {column:?}",
                    input_abs.display()
                ),
            )
        })?;
    let arrow_field = builder.schema().field_with_name(column).map_err(|_| {
        Error::refusal(
            "invalid_export",
            format!(
                "Parquet Arrow schema for {} does not contain column {column:?}",
                input_abs.display()
            ),
        )
    })?;
    match arrow_field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            ) => {}
        other => {
            return Err(Error::refusal(
                "invalid_export",
                format!(
                    "export-nquads column {column:?} must be Utf8, LargeUtf8 or Utf8View (found {other})"
                ),
            ));
        }
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), [root_col_idx]);
    let batch_rows = limits.batch_rows.max(1);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(batch_rows)
        .build()?;

    let temp_path = parent_abs.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let temp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    let mut guard = TempFileGuard::new(temp_path.clone());
    let mut writer = BufWriter::with_capacity(IO_BUF_BYTES, temp_file);
    let mut out_hasher = Sha256::new();
    let mut out_bytes = 0u64;
    let mut quads = 0u64;

    for batch_res in reader {
        let batch = batch_res?;
        let col = batch.column(0).as_ref();
        for row_idx in 0..batch.num_rows() {
            let Some(line) = string_at(col, row_idx, column)? else {
                continue;
            };
            let raw = line.as_bytes();
            writer.write_all(raw)?;
            out_hasher.update(raw);
            out_bytes = out_bytes.saturating_add(raw.len() as u64);
            if !raw.ends_with(b"\n") {
                writer.write_all(b"\n")?;
                out_hasher.update(b"\n");
                out_bytes = out_bytes.saturating_add(1);
            }
            quads = quads.saturating_add(1);
        }
    }

    writer.flush()?;
    let inner_file = writer
        .into_inner()
        .map_err(|err| Error::from(err.into_error()))?;
    inner_file.sync_all()?;
    drop(inner_file);

    std::fs::rename(&temp_path, &output_abs)?;
    guard.disarm();
    let final_output = std::fs::canonicalize(&output_abs).unwrap_or(output_abs);

    Ok(ExportNquadsResult {
        status: "succeeded".to_owned(),
        input: ExportNquadsInput {
            path: input_abs,
            sha256: input_sha256,
        },
        output: ExportNquadsOutput {
            path: final_output,
            sha256: format!("{:x}", out_hasher.finalize()),
            bytes: out_bytes,
            quads,
        },
    })
}
