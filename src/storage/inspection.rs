//! Footer checks and bounded native samples, without payload-wide hashing.

use super::*;
use futures::StreamExt;
use parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use std::io::{Seek, SeekFrom};

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
    pin: &TablePin,
    budget: &Budget,
    sample_files: usize,
    sample_bytes: usize,
    known: BTreeMap<String, CheckedFile>,
    admission: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<(Inspection, BTreeMap<String, CheckedFile>)> {
    #[cfg(feature = "test-util")]
    eprintln!("DOCSPEC_TEST_INSPECT_TABLE");
    check_available(pin.objects.clone(), budget).await?;
    let table = open_pin(pin).await?;
    super::defaults::check_expansion(table.metadata().current_schema(), &budget.limits, false)?;
    let expected = schema::read_schema(&table)?;
    let inventory = snapshot_files(&table, budget).await?;
    check_inventory_files(pin, &inventory)?;
    let schemas: BTreeSet<_> = table
        .metadata()
        .schemas_iter()
        .map(|schema| schema_identity(schema.as_struct()))
        .collect::<Result<_>>()?;
    let objects: BTreeMap<_, _> = pin
        .objects
        .iter()
        .map(|object| (object.path.clone(), object.clone()))
        .collect();
    let rows = pin.rows;
    let metadata_limit = budget.limits.max_metadata_bytes;
    let footer_memory = budget.reserve(metadata_limit)?;
    let (mut result, selected, known) = budget.blocking(move || {
        let _memory = (footer_memory, admission);
        let mut known = known;
        let mut known_bytes = known.values().fold(0usize, |sum, file| sum.saturating_add(file.estimated_bytes()));
        let mut total = 0u64;
        let mut checked = 0usize;
        let mut selected = BTreeSet::new();
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
            let mut file = File::open(native.file_path())?;
            if file.metadata()?.len() != native.file_size_in_bytes() {
                return Err(Error::refusal("file_size_mismatch", "file size differs from native metadata"));
            }
            // Parse with the maintained reader, after bounding serialized footer bytes.
            file.seek(SeekFrom::End(-8))?;
            let mut tail = [0; 8];
            file.read_exact(&mut tail)?;
            let size = FooterTail::try_from(tail)?.metadata_length();
            if size > metadata_limit {
                return Err(Error::refusal("metadata_capacity", "Parquet footer exceeds metadata allowance"));
            }
            let footer = ParquetMetaDataReader::new().parse_and_finish(&file)?;
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
                selected.insert(native.file_path().to_owned());
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
    let mut tasks = schema::plan_files(&table).await?;
    while let Some(task) = tasks.try_next().await? {
        if !selected.contains(&task.data_file_path) {
            continue;
        }
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
        if result.sampled_files == selected.len() || result.sampled_bytes == sample_bytes {
            break;
        }
    }
    Ok((result, known))
}
