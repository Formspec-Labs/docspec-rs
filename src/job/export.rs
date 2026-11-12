//! Ordered Parquet exports with explicit layout and hashes computed during writing.
// Rust guideline compliant 2026-02-21
use crate::{
    Error, Result,
    budget::Budget,
    model::{NullOrdering, SortColumn, SortDirection},
};
use arrow::row::{RowConverter, SortField};
use datafusion::{dataframe::DataFrame, prelude::col};
use futures::TryStreamExt;
use md5::Md5;
use parquet::{
    arrow::{ArrowSchemaConverter, ArrowWriter},
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{self, Write},
    path::Path,
};

/// Access pattern determining the indexes and page layout of an exported file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportProfile {
    Whole,
    Ranged,
    Lookup,
}

/// Complete, reproducible settings for one ordered output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportSettings {
    pub profile: ExportProfile,
    pub row_group_rows: usize,
    pub order_by: Vec<SortColumn>,
    /// Output fields; empty retains all fields, including ordering fields.
    #[serde(default)]
    pub columns: Vec<String>,
    pub dictionary_columns: BTreeSet<String>,
    pub delta_columns: BTreeSet<String>,
    pub key_column: Option<String>,
    pub created_by: String,
    pub footer_limit: usize,
}

/// Content identity returned without opening the completed file again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportedFile {
    pub sha256: String,
    pub md5: String,
    pub size: u64,
    pub rows: u64,
    pub footer_bytes: usize,
}

struct HashedWriter<W> {
    inner: W,
    sha256: Sha256,
    md5: Md5,
    blake3: blake3::Hasher,
    size: u64,
    tail: [u8; 8],
}
impl<W: Write> Write for HashedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(bytes)?;
        let bytes = &bytes[..n];
        self.sha256.update(bytes);
        self.md5.update(bytes);
        self.blake3.update(bytes);
        self.size += n as u64;
        if n >= 8 {
            self.tail.copy_from_slice(&bytes[n - 8..]);
        } else if n > 0 {
            self.tail.copy_within(n.., 0);
            self.tail[8 - n..].copy_from_slice(bytes);
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(super) struct PreparedExport {
    pub frame: DataFrame,
    pub input_schema: arrow::datatypes::SchemaRef,
    pub schema: arrow::datatypes::SchemaRef,
    pub output_indices: Vec<usize>,
    pub key_indices: Vec<usize>,
    pub converter: RowConverter,
    pub properties: WriterProperties,
    pub metadata_unit: usize,
}

// Share all schema-dependent export checks without opening a writer or scanning rows.
pub(super) fn prepare(
    frame: DataFrame,
    settings: &ExportSettings,
    budget: &Budget,
) -> Result<PreparedExport> {
    if settings.row_group_rows == 0
        || settings.order_by.is_empty()
        || settings.created_by.is_empty()
        || settings.footer_limit == 0
    {
        return Err(Error::refusal(
            "invalid_export",
            "export requires a positive row count, explicit order, writer identity and footer bound",
        ));
    }
    let order = settings
        .order_by
        .iter()
        .map(|sort| {
            col(&sort.field).sort(
                sort.direction == SortDirection::Asc,
                sort.nulls == NullOrdering::NullsFirst,
            )
        })
        .collect::<Vec<_>>();
    let frame = frame.sort(order)?;
    let input_schema = super::plan::declared_schema(&frame, budget)?;
    let output_indices = if settings.columns.is_empty() {
        (0..input_schema.fields().len()).collect::<Vec<_>>()
    } else {
        if settings.columns.iter().collect::<BTreeSet<_>>().len() != settings.columns.len() {
            return Err(Error::refusal(
                "invalid_export",
                "export columns must be distinct",
            ));
        }
        settings
            .columns
            .iter()
            .map(|name| input_schema.index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let schema = std::sync::Arc::new(input_schema.project(&output_indices)?);
    let descriptor = ArrowSchemaConverter::new().convert(&schema)?;
    let roots = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<BTreeSet<_>>();
    let leaves = descriptor
        .columns()
        .iter()
        .map(|leaf| leaf.path().parts().join("."))
        .collect::<BTreeSet<_>>();
    if settings
        .dictionary_columns
        .iter()
        .chain(&settings.delta_columns)
        .any(|name| !leaves.contains(name))
    {
        return Err(Error::refusal(
            "invalid_export",
            "writer column is absent from output",
        ));
    }
    if settings
        .dictionary_columns
        .intersection(&settings.delta_columns)
        .next()
        .is_some()
    {
        return Err(Error::refusal(
            "invalid_export",
            "delta and dictionary encoding cannot select the same column",
        ));
    }
    let mut properties = WriterProperties::builder()
        .set_write_batch_size(settings.row_group_rows.min(1024))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_created_by(settings.created_by.clone())
        .set_dictionary_enabled(false)
        .set_max_row_group_bytes(None)
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(if settings.profile == ExportProfile::Lookup {
            usize::MAX
        } else {
            settings.row_group_rows
        }))
        .set_offset_index_disabled(settings.profile == ExportProfile::Whole);
    match settings.profile {
        ExportProfile::Whole => {}
        ExportProfile::Ranged => {
            properties = properties
                .set_data_page_size_limit(64 << 10)
                .set_data_page_row_count_limit(1024)
                .set_write_batch_size(settings.row_group_rows.min(1024));
        }
        ExportProfile::Lookup => {
            let key = settings.key_column.as_ref().ok_or_else(|| {
                Error::refusal("invalid_export", "lookup export requires a key column")
            })?;
            if !roots.contains(key.as_str()) {
                return Err(Error::refusal(
                    "invalid_export",
                    "lookup key is absent from output",
                ));
            }
            properties = properties
                .set_data_page_size_limit(16 << 10)
                .set_write_batch_size(settings.row_group_rows.min(1024));
        }
    }
    for leaf in descriptor.columns() {
        let root = leaf.path().parts().join(".");
        let path = leaf.path().clone();
        if settings.dictionary_columns.contains(&root) {
            properties = properties.set_column_dictionary_enabled(path.clone(), true);
        }
        if settings.delta_columns.contains(&root) {
            properties = properties.set_column_encoding(path.clone(), Encoding::DELTA_BYTE_ARRAY);
        }
        if settings.profile == ExportProfile::Lookup && settings.key_column.as_ref() == Some(&root)
        {
            properties = properties.set_column_statistics_enabled(path, EnabledStatistics::Page);
        }
    }
    let key_indices = settings
        .order_by
        .iter()
        .map(|sort| input_schema.index_of(&sort.field))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let converter = RowConverter::new(
        key_indices
            .iter()
            .map(|i| SortField::new(input_schema.field(*i).data_type().clone()))
            .collect(),
    )?;
    let metadata_unit = crate::storage::writer_metadata_unit(leaves.iter().map(String::as_str));
    Ok(PreparedExport {
        frame,
        input_schema,
        schema,
        output_indices,
        key_indices,
        converter,
        properties: properties.build(),
        metadata_unit,
    })
}

pub(super) async fn export(
    frame: DataFrame,
    path: &Path,
    settings: &ExportSettings,
    budget: &Budget,
) -> Result<ExportedFile> {
    Ok(export_retained(frame, path, settings, budget).await?.0)
}

pub(super) async fn export_retained(
    frame: DataFrame,
    path: &Path,
    settings: &ExportSettings,
    budget: &Budget,
) -> Result<(ExportedFile, String)> {
    if path.exists() {
        return Err(Error::refusal(
            "invalid_export",
            "export output already exists",
        ));
    }
    let PreparedExport {
        frame,
        input_schema,
        schema,
        output_indices,
        key_indices,
        converter,
        properties,
        metadata_unit,
    } = prepare(frame, settings, budget)?;
    let temporary = budget
        .runtime_env()
        .disk_manager
        .create_tmp_file("SQL export")?;
    // Reserve before opening the encoder. The second batch allowance covers
    // the incoming batch while the previous row group remains in the writer.
    let allowance = budget
        .limits
        .max_batch_bytes
        .checked_mul(3)
        .ok_or_else(|| Error::refusal("capacity", "writer allowance overflow"))?;
    let mut writer_memory = budget.reserve(allowance)?;
    let metadata_limit = settings
        .footer_limit
        .saturating_mul(8)
        .min(budget.limits.max_metadata_bytes);
    if metadata_unit > metadata_limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "export schema exceeds metadata allowance",
        ));
    }
    // Native exports disable byte-triggered row groups and explicit flushes.
    // Admit the existing conservative file/schema unit now, then actual row-group
    // units before each write rather than reserving the whole allowed maximum.
    let mut metadata_memory = budget.reserve(metadata_unit)?;
    let previous_key: Option<Vec<u8>> = None;
    let hashed = HashedWriter {
        inner: budget.scratch_writer(&temporary)?,
        sha256: Sha256::new(),
        md5: Md5::new(),
        blake3: blake3::Hasher::new(),
        size: 0,
        tail: [0; 8],
    };
    let projected_schema = schema.clone();
    let mut writer = ArrowWriter::try_new(hashed, schema, Some(properties))?;
    let stream = crate::export::portable_stream(
        super::plan::execute_stream(frame).await?,
        input_schema,
        budget,
    );
    let projected_stream = futures::stream::try_unfold(
        (stream, converter, previous_key, key_indices, output_indices),
        |(mut stream, converter, mut previous_key, key_indices, output_indices)| async move {
            let Some(batch) = stream.try_next().await.map_err(Error::from)? else {
                return Ok::<_, Error>(None);
            };
            let keys = converter.convert_columns(
                &key_indices
                    .iter()
                    .map(|i| batch.column(*i).clone())
                    .collect::<Vec<_>>(),
            )?;
            for index in 0..keys.num_rows() {
                if (index == 0 && previous_key.as_deref() == Some(keys.row(index).as_ref()))
                    || (index > 0 && keys.row(index - 1) == keys.row(index))
                {
                    return Err(Error::refusal(
                        "invalid_export",
                        "export ordering keys are not unique",
                    ));
                }
            }
            if keys.num_rows() > 0 {
                previous_key = Some(keys.row(keys.num_rows() - 1).as_ref().to_vec());
            }
            let projected = batch.project(&output_indices)?;
            Ok(Some((
                projected,
                (stream, converter, previous_key, key_indices, output_indices),
            )))
        },
    )
    .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)));
    let projected_stream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            projected_schema,
            projected_stream,
        ),
    );
    let mut stream = crate::export::coalesce_portable_stream(
        projected_stream,
        settings.row_group_rows.min(1024),
        budget,
    );
    let mut rows = 0u64;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows() as u64;
        let groups = if settings.profile == ExportProfile::Lookup {
            usize::from(rows > 0)
        } else {
            (rows as usize).div_ceil(settings.row_group_rows)
        };
        let required_metadata = groups
            .checked_add(1)
            .and_then(|groups| groups.checked_mul(metadata_unit))
            .ok_or_else(|| Error::refusal("metadata_capacity", "export metadata overflow"))?;
        if required_metadata > metadata_limit {
            return Err(Error::refusal(
                "metadata_capacity",
                "export row groups exceed metadata allowance",
            ));
        }
        metadata_memory.merge(
            budget.reserve(required_metadata.saturating_sub(metadata_memory.num_permits()))?,
        );
        super::encoding::write(
            &mut writer,
            &batch,
            budget,
            &mut writer_memory,
            allowance,
            true,
            true,
            false,
        )?;
    }
    writer.finish()?;
    let hashed = writer.inner();
    let footer_bytes =
        u32::from_le_bytes(hashed.tail[..4].try_into().expect("fixed trailer")) as usize;
    if &hashed.tail[4..] != b"PAR1" || footer_bytes >= settings.footer_limit {
        return Err(Error::refusal(
            "invalid_export",
            "Parquet footer exceeds configured bound",
        ));
    }
    let result = ExportedFile {
        sha256: format!("{:x}", hashed.sha256.clone().finalize()),
        md5: format!("{:x}", hashed.md5.clone().finalize()),
        size: hashed.size,
        rows,
        footer_bytes,
    };
    let digest = hashed.blake3.finalize().to_hex().to_string();
    drop(writer);
    temporary.inner().as_file().sync_all()?;
    std::fs::hard_link(temporary.path(), path)?;
    budget.retain_temporary(temporary)?;
    sync_parent(path)?;
    Ok((result, digest))
}

pub(super) fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::refusal("invalid_export", "output has no parent"))?;
    std::fs::File::open(if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    })?
    .sync_all()?;
    Ok(())
}
