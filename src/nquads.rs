// Rust guideline compliant 2026-02-21
//! Streaming Parquet-to-N-Quads export with exact input and output SHA-256 receipts.

use crate::{
    Error, Result,
    budget::{Budget, Limits},
};
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
    io::{BufWriter, Read, Seek, Write},
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

fn hash_and_check_tail(
    file: &mut File,
    expected_len: u64,
    max_metadata_bytes: usize,
    display_path: &Path,
) -> Result<String> {
    if expected_len < 12 {
        return Err(Error::refusal(
            "invalid_export",
            format!(
                "Parquet input {} is smaller than a valid Parquet footer",
                display_path.display()
            ),
        ));
    }
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; IO_BUF_BYTES];
    let mut total_read = 0u64;
    let mut tail = [0u8; 8];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        total_read = total_read.saturating_add(read as u64);
        if read >= 8 {
            tail.copy_from_slice(&buf[read - 8..read]);
        } else {
            let keep = 8 - read;
            tail.copy_within(read..8, 0);
            tail[keep..8].copy_from_slice(&buf[..read]);
        }
    }
    if total_read != expected_len {
        return Err(Error::refusal(
            "invalid_export",
            format!(
                "Parquet input {} changed size while hashing",
                display_path.display()
            ),
        ));
    }
    if &tail[4..8] != b"PAR1" {
        return Err(Error::refusal(
            "invalid_export",
            format!(
                "Parquet input {} has an invalid Parquet footer magic",
                display_path.display()
            ),
        ));
    }
    let footer_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as usize;
    if footer_len > max_metadata_bytes {
        return Err(Error::refusal(
            "metadata_capacity",
            format!(
                "Parquet input {} footer ({footer_len} bytes) exceeds max_metadata_bytes ({max_metadata_bytes} bytes)",
                display_path.display()
            ),
        ));
    }
    file.rewind()?;
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

/// Streams a projected string column from a Parquet file to a create-only durable `.nq` file.
///
/// # Errors
/// Refuses invalid limits, missing/oversized inputs, existing output destinations,
/// non-string columns, empty or multi-line quad cells, and I/O errors.
pub async fn export_nquads(
    input: &Path,
    output: &Path,
    column: &str,
    limits: &Limits,
) -> Result<ExportNquadsResult> {
    let budget = Budget::new(limits.clone())?;
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
    let (parent_abs, output_abs, file_name) = resolve_output_path(output)?;
    if std::fs::symlink_metadata(&output_abs).is_ok() {
        return Err(Error::refusal(
            "existing_output",
            format!("N-Quads output already exists at {}", output_abs.display()),
        ));
    }

    let mut file = File::open(&input_abs).map_err(|err| {
        Error::refusal(
            "missing_input",
            format!(
                "Parquet input {} is unavailable: {err}",
                input_abs.display()
            ),
        )
    })?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(Error::refusal(
            "invalid_export",
            format!(
                "Parquet input {} is not a regular file",
                input_abs.display()
            ),
        ));
    }

    let _footer_permit = crate::storage::admit_parquet_footer(&input_abs, &budget).await?;
    let input_sha256 = hash_and_check_tail(
        &mut file,
        meta.len(),
        budget.limits.max_metadata_bytes,
        &input_abs,
    )?;

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
    let mut max_est_row_bytes = 0usize;
    let num_leaves = builder.parquet_schema().num_columns();
    for rg in builder.metadata().row_groups() {
        let rows = usize::try_from(rg.num_rows().max(0)).unwrap_or(0);
        if rows == 0 {
            continue;
        }
        let mut rg_bytes = 0usize;
        for leaf_idx in 0..num_leaves {
            if mask.leaf_included(leaf_idx) {
                let col_bytes =
                    usize::try_from(rg.column(leaf_idx).uncompressed_size().max(0)).unwrap_or(0);
                rg_bytes = rg_bytes.saturating_add(col_bytes);
            }
        }
        max_est_row_bytes = max_est_row_bytes.max(rg_bytes.div_ceil(rows));
    }
    let batch_rows = if max_est_row_bytes > 0 {
        (budget.limits.max_batch_bytes / max_est_row_bytes).clamp(1, budget.limits.batch_rows)
    } else {
        budget.limits.batch_rows
    };
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
        let batch_bytes = batch.get_array_memory_size();
        if batch_bytes > budget.limits.max_batch_bytes {
            return Err(Error::refusal(
                "capacity",
                format!(
                    "export-nquads decoded batch ({batch_bytes} bytes) exceeds max_batch_bytes ({} bytes)",
                    budget.limits.max_batch_bytes
                ),
            ));
        }
        let _batch_permit = budget.reserve(batch_bytes)?;
        let col = batch.column(0).as_ref();
        for row_idx in 0..batch.num_rows() {
            let Some(line) = string_at(col, row_idx, column)? else {
                continue;
            };
            let trimmed = line.strip_suffix('\n').unwrap_or(line);
            if trimmed.trim().is_empty() {
                return Err(Error::refusal(
                    "invalid_export",
                    format!(
                        "export-nquads column {column:?} contains an empty or whitespace-only quad"
                    ),
                ));
            }
            if trimmed.contains(['\n', '\r']) {
                return Err(Error::refusal(
                    "invalid_export",
                    format!("export-nquads column {column:?} contains embedded newline characters"),
                ));
            }
            let raw = trimmed.as_bytes();
            writer.write_all(raw)?;
            writer.write_all(b"\n")?;
            out_hasher.update(raw);
            out_hasher.update(b"\n");
            out_bytes = out_bytes.saturating_add((raw.len() as u64).saturating_add(1));
            quads = quads.saturating_add(1);
        }
    }

    writer.flush()?;
    let inner_file = writer
        .into_inner()
        .map_err(|err| Error::from(err.into_error()))?;
    inner_file.sync_all()?;
    drop(inner_file);

    std::fs::hard_link(&temp_path, &output_abs).map_err(|err| {
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            Error::refusal(
                "existing_output",
                format!("N-Quads output already exists at {}", output_abs.display()),
            )
        } else {
            Error::from(err)
        }
    })?;
    std::fs::remove_file(&temp_path)?;
    guard.disarm();
    File::open(&parent_abs)?.sync_all()?;
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
