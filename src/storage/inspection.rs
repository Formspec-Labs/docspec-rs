//! Footer checks and bounded native samples, without payload-wide hashing.

use super::*;
use futures::StreamExt;
use iceberg::io::{FileRead, InputFile};
use parquet::{
    DecodeResult,
    file::metadata::{FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder},
};

/// Reads bounded footer ranges through the table's maintained native I/O.
///
/// The caller owns memory admission and runs this on an admitted blocking worker.
/// Serialized bytes are bounded; native decoder allocations are not an RSS bound.
async fn read_footer(input: &InputFile, file_size: u64, limit: usize) -> Result<ParquetMetaData> {
    if input.metadata().await?.size != file_size {
        return Err(Error::refusal(
            "file_size_mismatch",
            "file size differs from native metadata",
        ));
    }
    let reader = input.reader().await?;
    let mut decoder = footer_decoder(reader.as_ref(), file_size, limit).await?;
    if input.metadata().await?.size != file_size {
        return Err(Error::refusal(
            "file_size_mismatch",
            "file size differs from native metadata",
        ));
    }
    match decoder.try_decode()? {
        DecodeResult::Data(metadata) => Ok(metadata),
        _ => Err(parquet::errors::ParquetError::General(
            "Parquet footer could not be decoded from its complete ranges".into(),
        )
        .into()),
    }
}

// Keep native I/O injected so exact ranges and short reads can be checked.
async fn footer_decoder(
    reader: &dyn FileRead,
    file_size: u64,
    limit: usize,
) -> Result<ParquetMetaDataPushDecoder> {
    // Parquet's final eight bytes hold the serialized metadata length and magic.
    let tail_start = file_size.checked_sub(8).ok_or_else(|| {
        parquet::errors::ParquetError::General(
            "Parquet file is shorter than its footer tail".into(),
        )
    })?;
    let tail = reader.read(tail_start..file_size).await?;
    if tail.len() != 8 {
        return Err(parquet::errors::ParquetError::General(
            "short Parquet footer tail read".into(),
        )
        .into());
    }
    let size = FooterTail::try_from(tail.as_ref())?.metadata_length();
    if size > limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "Parquet footer exceeds metadata allowance",
        ));
    }
    let start = tail_start
        .checked_sub(u64::try_from(size).map_err(|_| {
            Error::refusal(
                "metadata_capacity",
                "Parquet footer length exceeds addressable bytes",
            )
        })?)
        .ok_or_else(|| {
            parquet::errors::ParquetError::General("Parquet footer length exceeds file size".into())
        })?;
    // Read only the body: the retained tail is fed to the same native decoder.
    let body = reader.read(start..tail_start).await?;
    if body.len() != size {
        return Err(parquet::errors::ParquetError::General(
            "short Parquet footer body read".into(),
        )
        .into());
    }
    let mut decoder = ParquetMetaDataPushDecoder::try_new(file_size)?
        .with_page_index_policy(PageIndexPolicy::Skip);
    decoder.push_range(tail_start..file_size, tail)?;
    decoder.push_range(start..tail_start, body)?;
    Ok(decoder)
}

/// Known bytes and possible physical schemas, shared within one operation only.
pub(crate) struct CheckedFile {
    pub object: ObjectRef,
    pub schemas: Arc<BTreeSet<String>>,
    pub rows: u64,
}

impl CheckedFile {
    pub fn estimated_bytes(&self) -> usize {
        self.object
            .path
            .len()
            .saturating_mul(2)
            .saturating_add(self.object.digest.len())
            .saturating_add(self.schemas.len().saturating_mul(96))
            .saturating_add(256)
    }
}

pub(crate) fn schema_identity(schema: &iceberg::spec::StructType) -> Result<String> {
    // Defaults and documentation are native read rules, absent from Parquet's
    // physical type tree. Compare only identities, names, types and nullability.
    fn physical(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(fields) => {
                for key in ["doc", "initial-default", "write-default"] {
                    fields.remove(key);
                }
                for value in fields.values_mut() {
                    physical(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    physical(value);
                }
            }
            _ => {}
        }
    }
    let mut schema = serde_json::to_value(schema)?;
    physical(&mut schema);
    Ok(json_digest(&schema)?.to_hex().to_string())
}

#[derive(Default)]
pub(crate) struct Inspection {
    pub files: usize,
    pub sampled_files: usize,
    pub sampled_rows: usize,
    pub sampled_bytes: usize,
}

pub(crate) async fn inspect_table(
    object_io: &Objects,
    pin: &TablePin,
    budget: &Budget,
    sample_files: usize,
    sample_bytes: usize,
    known: BTreeMap<String, CheckedFile>,
    admission: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<(Inspection, BTreeMap<String, CheckedFile>)> {
    #[cfg(feature = "test-util")]
    eprintln!("DOCSPEC_TEST_INSPECT_TABLE");
    object_io
        .check_available(pin.objects.clone(), budget)
        .await?;
    let table = object_io.open_pin(pin, budget).await?;
    super::defaults::check_expansion(table.metadata().current_schema(), &budget.limits, false)?;
    let expected = schema::read_schema(&table)?;
    // New material uses the same configured native inventory walk as pinning.
    // Scope checks run before each manifest read, without another identity pass.
    let inventory_memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    let inherited: BTreeMap<_, _> = pin
        .objects
        .iter()
        .map(|object| (object.path.clone(), object.clone()))
        .collect();
    let remote_root = if object_io.is_remote() {
        Some(object_io.pin_root(table.metadata().location())?)
    } else {
        None
    };
    let guard = remote_root.as_ref().map(|root| super::object_io::PinPaths {
        objects: object_io,
        root,
        inherited: &inherited,
    });
    let inventory =
        snapshot_files_guarded(&table, budget, guard.as_ref(), inventory_memory).await?;
    check_inventory_files(pin, &inventory)?;
    let schemas: BTreeSet<_> = table
        .metadata()
        .schemas_iter()
        .map(|schema| schema_identity(schema.as_struct()))
        .collect::<Result<_>>()?;
    // The guard walk has finished; move its retained identity map into the
    // footer worker instead of cloning the same inventory a second time.
    let objects = inherited;
    let rows = pin.rows;
    let metadata_limit = budget.limits.max_metadata_bytes;
    let footer_memory = budget.reserve(metadata_limit)?;
    let file_io = table.file_io().clone();
    let handle = tokio::runtime::Handle::current();
    let (mut result, selected, known) = budget.blocking(move || {
        let _memory = (footer_memory, admission);
        let mut known = known;
        let mut known_bytes = known.values().fold(0usize, |sum, file| sum.saturating_add(file.estimated_bytes()));
        let mut total = 0u64;
        let mut checked = 0usize;
        let mut selected = Vec::new();
        for native in &inventory.data {
            if native.file_format() != DataFileFormat::Parquet {
                return Err(Error::refusal("unsupported_format", "routine inspection requires Parquet"));
            }
            total = total.checked_add(native.record_count()).ok_or_else(||
                Error::refusal("row_count_overflow", "native record counts overflow"))?;
            let object = objects.get(native.file_path()).ok_or_else(|| Error::refusal("incomplete_inventory", "data file has no retained identity"))?;
            if object.bytes != native.file_size_in_bytes() {
                return Err(Error::refusal("file_size_mismatch", "native data file size differs from retained identity"));
            }
            if let Some(prior) = known.get(native.file_path()) {
                if prior.object != *object {
                    return Err(Error::refusal("conflicting_object", "shared path has conflicting retained identities"));
                }
                if prior.rows != native.record_count() {
                    return Err(Error::refusal("row_count_mismatch", "shared file has conflicting native row counts"));
                }
                // Prior qualification establishes the physical bytes. Reuse it only
                // when every possible original schema belongs to this table's history.
                if prior.schemas.is_subset(&schemas) { continue; }
            }
            #[cfg(feature = "test-util")]
            eprintln!("DOCSPEC_TEST_INSPECT_FILE {}", native.file_path());
            let input = file_io.new_input(native.file_path())?;
            // Filesystem FileIO may block internally; remote I/O uses the existing
            // runtime while the admitted worker retains cancellation ownership.
            let footer = handle.block_on(read_footer(
                &input, native.file_size_in_bytes(), metadata_limit,
            ))?;
            let physical = parquet::arrow::parquet_to_arrow_schema(
                footer.file_metadata().schema_descr(), None,
            )?;
            let physical = iceberg::arrow::arrow_schema_to_schema(&physical)?;
            let physical = schema_identity(physical.as_struct())?;
            if !schemas.contains(&physical) {
                return Err(Error::refusal("file_schema_mismatch", "physical field IDs, types or nullability differ from retained schema history"));
            }
            if u64::try_from(footer.file_metadata().num_rows()).ok() != Some(native.record_count()) {
                return Err(Error::refusal("row_count_mismatch", "Parquet footer differs from native record count"));
            }
            checked += 1;
            let checked_file = CheckedFile { object: object.clone(), schemas: Arc::new(BTreeSet::from([physical])), rows: native.record_count() };
            known_bytes = known_bytes.saturating_add(checked_file.estimated_bytes());
            if let Some(prior) = known.insert(object.path.clone(), checked_file) {
                known_bytes = known_bytes.saturating_sub(prior.estimated_bytes());
            }
            if known_bytes > metadata_limit { return Err(Error::refusal("metadata_capacity", "checked-file inventory exceeds metadata allowance")); }
            if sample_files > selected.len() {
                selected.push(native.clone());
            }
        }
        if total != rows {
            return Err(Error::refusal("row_count_mismatch", "native file counts differ from table pin"));
        }
        Ok((Inspection { files: checked, ..Inspection::default() }, selected, known))
    }).await?;
    if selected.is_empty() || sample_bytes == 0 {
        return Ok((result, known));
    }
    let _memory = budget.reserve(budget.limits.max_batch_bytes.saturating_mul(2))?;
    // The guarded inventory already established current membership and refused
    // native deletes. Build full-file native sample tasks from those descriptors
    // instead of loading the same manifest list and manifests a second time.
    let current = table.metadata().current_schema().clone();
    let mut fields = current
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    fields.push(iceberg::metadata_columns::RESERVED_FIELD_ID_FILE);
    let name_mapping = table
        .metadata()
        .properties()
        .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|raw| serde_json::from_str::<iceberg::spec::NameMapping>(raw).map(Arc::new))
        .transpose()?;
    let selected_count = selected.len();
    for file in selected {
        let task = iceberg::scan::FileScanTask::builder()
            .with_file_size_in_bytes(file.file_size_in_bytes())
            .with_start(0)
            .with_length(file.file_size_in_bytes())
            .with_record_count(Some(file.record_count()))
            .with_data_file_path(file.file_path().to_owned())
            .with_data_file_format(file.file_format())
            .with_schema(current.clone())
            .with_project_field_ids(fields.clone())
            .with_name_mapping(name_mapping.clone())
            .with_case_sensitive(true)
            .build();
        let mut stream = table
            .reader_builder()
            .with_data_file_concurrency_limit(1)
            // A small shape probe, not a statistically representative sample.
            .with_batch_size(8.min(budget.limits.batch_rows))
            .build()
            .read(futures::stream::once(async move { Ok(task) }).boxed())?
            .stream();
        result.sampled_files += 1;
        if let Some(batch) = stream.try_next().await? {
            let batch = schema::strip_file_column(batch, budget.limits.max_batch_bytes)?;
            if batch.schema().fields() != expected.fields()
                || batch
                    .columns()
                    .iter()
                    .zip(expected.fields())
                    .any(|(column, field)| !field.is_nullable() && column.null_count() != 0)
            {
                return Err(Error::refusal(
                    "sample_schema_mismatch",
                    "sample types or required values differ from pinned schema",
                ));
            }
            result.sampled_rows += batch.num_rows();
            result.sampled_bytes += batch.get_array_memory_size();
        }
        // Dropping this file's reader prevents a sample from draining its payload.
        if result.sampled_files == selected_count || result.sampled_bytes == sample_bytes {
            break;
        }
    }
    Ok((result, known))
}

#[cfg(test)]
mod tests;
