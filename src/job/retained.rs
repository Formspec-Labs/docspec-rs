//! Reopenable native phase artifacts for managed jobs, using the ordinary SQL service.
// Rust guideline compliant 2026-02-21
use super::{ComponentsSettings, ExportSettings, ExportedFile, Job, QueryResult};
use crate::{Error, Result, budget::Budget};
use arrow::{
    datatypes::{Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Cursor, Write},
    path::Path,
};

/// Identity computed while a native retained phase writes its immutable file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClosedFile {
    pub digest: String,
    pub sha256: String,
    pub size: u64,
    pub rows: u64,
    pub schema: Schema,
}

#[derive(Debug)]
pub(crate) struct MetadataBytes {
    pub bytes: Vec<u8>,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) fn encode_schema(schema: &Schema, budget: &Budget) -> Result<MetadataBytes> {
    let mut buffer = MetadataBuffer::new(budget);
    drop(StreamWriter::try_new(&mut buffer, schema)?);
    Ok(MetadataBytes {
        bytes: buffer.bytes,
        _memory: buffer.memory,
    })
}

pub(crate) fn retain_schema_into(
    budget: &Budget,
    schemas: &mut std::collections::BTreeMap<String, String>,
    memory: &mut Vec<tokio::sync::OwnedSemaphorePermit>,
    bytes: &[u8],
) -> Result<String> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let digest = format!("{:x}", Sha256::digest(bytes));
    if !schemas.contains_key(&digest) {
        let size = schemas
            .values()
            .map(String::len)
            .sum::<usize>()
            .checked_add(bytes.len().div_ceil(3).saturating_mul(4))
            .filter(|size| *size <= budget.limits.max_metadata_bytes)
            .ok_or_else(|| {
                Error::refusal(
                    "metadata_capacity",
                    "schema dictionary exceeds metadata allowance",
                )
            })?;
        let _ = size;
        memory.push(budget.reserve(bytes.len().saturating_mul(32))?);
        schemas.insert(digest.clone(), STANDARD.encode(bytes));
    }
    Ok(digest)
}

struct HashWriter<W> {
    inner: W,
    digest: blake3::Hasher,
    sha256: Sha256,
    size: u64,
}
impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.digest.update(&bytes[..count]);
        self.sha256.update(&bytes[..count]);
        self.size += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Job {
    pub(super) fn owned_relation_frame(
        &self,
        name: &str,
        output: bool,
        frame: datafusion::dataframe::DataFrame,
    ) -> Result<datafusion::dataframe::DataFrame> {
        let alias = format!("__docspec_relation_{}", uuid::Uuid::new_v4().simple());
        let frame = frame.alias(alias.as_str())?;
        let key = format!(
            "{}.{}",
            if output { "outputs" } else { "public" },
            serde_json::to_string(name)?
        );
        self.relation_aliases
            .lock()
            .map_err(|_| Error::refusal("worker", "native relation map is poisoned"))?
            .insert(alias, key);
        Ok(frame)
    }
    /// Registers managed step `name` from its sealed file at `path`, column metadata kept.
    ///
    /// A plain job keeps a step as planned, so its later steps and exports see
    /// each column's metadata, a lake column's `PARQUET:field_id` among it.
    /// DataFusion's Parquet reader skips top-level column metadata by default
    /// (`skip_metadata`), which gave a managed job's exports the plain job's rows
    /// without their field ids: different footers, so different files. Kept, the
    /// step's schema is the one its writer declared, and later steps plan as in
    /// the plain job.
    pub(crate) async fn reopen_retained(&self, name: &str, path: &Path) -> Result<()> {
        let frame = super::materialized::read_retained(&self.budget, path, |options| {
            options.execution.parquet.skip_metadata = false;
        })
        .await?;
        self.register_frame(name, self.owned_relation_frame(name, false, frame)?)
    }
    pub(crate) async fn retained_view(&self, step: &super::JobStep) -> Result<Schema> {
        let frame = match step {
            super::JobStep::Sql(step) => self.sql_bound(&step.sql, &step.parameters).await?,
            #[cfg(feature = "substrait")]
            super::JobStep::Substrait(_) => {
                return Err(Error::refusal(
                    "unsupported_managed_job",
                    "retained Substrait requires native artifact admission",
                ));
            }
            super::JobStep::Components(_) => {
                return Err(Error::refusal(
                    "invalid_spec",
                    "components require durable materialization",
                ));
            }
        };
        let schema = super::plan::inferred_schema(&frame, &self.budget)?;
        self.register_frame(
            step.name(),
            self.owned_relation_frame(step.name(), false, frame)?,
        )?;
        Ok(schema)
    }

    pub(crate) async fn input_schema(&self, name: &str) -> Result<Schema> {
        Ok(self
            .context
            .table_provider(name)
            .await?
            .schema()
            .as_ref()
            .clone())
    }

    pub(crate) async fn retained_dependencies(
        &self,
        sql: &str,
        parameters: &std::collections::BTreeMap<String, serde_json::Value>,
        known: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<String>> {
        use datafusion::{common::tree_node::TreeNodeRecursion, logical_expr::LogicalPlan};
        let frame = self.sql_bound(sql, parameters).await?;
        let owned = self
            .relation_aliases
            .lock()
            .map_err(|_| Error::refusal("worker", "native relation map is poisoned"))?;
        let mut names = std::collections::BTreeSet::new();
        frame.logical_plan().apply_with_subqueries(|plan| {
            if let LogicalPlan::SubqueryAlias(alias) = plan {
                if let Some(key) = owned.get(alias.alias.table()) {
                    if !known.contains_key(key) {
                        return Err(datafusion::common::DataFusionError::Plan(
                            "native relation has no admitted dependency identity".into(),
                        ));
                    }
                    names.insert(key.clone());
                    return Ok(TreeNodeRecursion::Jump);
                }
                if alias
                    .alias
                    .table()
                    .to_ascii_lowercase()
                    .starts_with("__docspec_relation_")
                {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "caller SQL cannot name a reserved native relation token".into(),
                    ));
                }
            }
            if let LogicalPlan::TableScan(scan) = plan {
                let reference = &scan.table_name;
                let output = reference.schema() == Some("outputs");
                let name = serde_json::to_string(reference.table()).map_err(|error| {
                    datafusion::common::DataFusionError::External(Box::new(error))
                })?;
                let key = format!("{}.{name}", if output { "outputs" } else { "public" });
                names.insert(key);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(names.into_iter().collect())
    }
    /// Writes SQL step `sql` to the immutable file at `path`, declared as a plain job declares it.
    pub(crate) async fn retained_sql(&self, sql: &str, path: &Path) -> Result<ClosedFile> {
        let frame = self.sql(sql).await?;
        let schema = super::plan::declared_schema(&frame, &self.budget)?;
        let stream = super::plan::execute_stream(frame).await?;
        self.retain_stream(stream, schema, path).await
    }

    pub(crate) async fn retained_components(
        &self,
        name: &str,
        settings: &ComponentsSettings,
        path: &Path,
    ) -> Result<ClosedFile> {
        let stream = super::components::compute_stream(self, name, settings).await?;
        // A components step declares its own fixed output, plain or managed.
        let schema = crate::export::portable_schema(&stream.schema());
        self.retain_stream(stream, schema, path).await
    }

    /// Writes `stream` to the immutable file at `path`, declared as `schema`.
    async fn retain_stream(
        &self,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
        path: &Path,
    ) -> Result<ClosedFile> {
        let mut stream = crate::export::portable_stream(stream, schema.clone(), &self.budget);
        let file = self
            .budget
            .runtime_env()
            .disk_manager
            .create_tmp_file("retained job phase")?;
        let allowance = self.budget.limits.max_batch_bytes.saturating_mul(3);
        let mut memory = self.budget.reserve(allowance)?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(self.budget.limits.batch_rows))
            .set_max_row_group_bytes(None)
            .build();
        let descriptor = parquet::arrow::ArrowSchemaConverter::new().convert(&schema)?;
        let leaves = descriptor
            .columns()
            .iter()
            .map(|leaf| leaf.path().parts().join("."))
            .collect::<Vec<_>>();
        let metadata_unit = crate::storage::writer_metadata_unit(leaves.iter().map(String::as_str));
        let metadata_limit = self.budget.limits.max_metadata_bytes;
        let _metadata = self.budget.reserve(metadata_limit)?;
        if metadata_unit > metadata_limit {
            return Err(Error::refusal(
                "metadata_capacity",
                "materialized schema exceeds footer allowance",
            ));
        }
        let output = HashWriter {
            inner: self.budget.scratch_writer(&file)?,
            digest: blake3::Hasher::new(),
            sha256: Sha256::new(),
            size: 0,
        };
        let mut writer = ArrowWriter::try_new(output, schema.clone(), Some(properties))?;
        let mut rows = 0u64;
        while let Some(batch) = stream.try_next().await? {
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| Error::refusal("capacity", "phase row count overflow"))?;
            super::encoding::write_bounded(
                &mut writer,
                &batch,
                &self.budget,
                &mut memory,
                allowance,
                true,
                true,
                true,
                Some(super::encoding::MetadataBound::Ceiling {
                    unit: metadata_unit,
                    limit: metadata_limit,
                    row_group_rows: self.budget.limits.batch_rows,
                }),
            )?;
        }
        writer.finish()?;
        let output = writer.inner();
        let closed = ClosedFile {
            digest: output.digest.finalize().to_hex().to_string(),
            sha256: format!("{:x}", output.sha256.clone().finalize()),
            size: output.size,
            rows,
            schema: schema.as_ref().clone(),
        };
        drop(writer);
        file.inner().as_file().sync_all()?;
        std::fs::hard_link(file.path(), path)?;
        self.budget.retain_temporary(file)?;
        sync_parent(path)?;
        Ok(closed)
    }

    pub(crate) async fn retained_export(
        &self,
        sql: &str,
        path: &Path,
        settings: &ExportSettings,
    ) -> Result<(ExportedFile, String, Schema)> {
        let frame = self.sql(sql).await?;
        let schema = super::export::prepare(frame.clone(), settings, &self.budget)?
            .schema
            .as_ref()
            .clone();
        let (file, digest) =
            super::export::export_retained(frame, path, settings, &self.budget).await?;
        Ok((file, digest, schema))
    }

    pub(crate) async fn report_ipc(&self, name: &str, sql: &str) -> Result<MetadataBytes> {
        let (frame, _) = super::query_result::plan_query_frame(self, name, sql).await?;
        let mut stream = super::plan::execute_stream(frame).await?;
        let schema = stream.schema();
        super::query_result::ensure_distinct_columns(name, &schema)?;
        let buffer = MetadataBuffer::new(&self.budget);
        let mut writer = StreamWriter::try_new(buffer, &schema)?;
        while let Some(batch) = stream.try_next().await? {
            if batch.get_array_memory_size() > self.budget.limits.max_metadata_bytes {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "report batch exceeds metadata allowance",
                ));
            }
            let _memory = self
                .budget
                .reserve(batch.get_array_memory_size().saturating_mul(8))?;
            writer.write(&batch)?;
        }
        writer.finish()?;
        let buffer = writer.into_inner()?;
        Ok(MetadataBytes {
            bytes: buffer.bytes,
            _memory: buffer.memory,
        })
    }

    pub(crate) fn report_result(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<(QueryResult, Schema, u64)> {
        let _memory = self.budget.reserve(bytes.len().saturating_mul(32))?;
        let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
        let schema = reader.schema();
        let columns = super::query_result::ensure_distinct_columns(name, &schema)?;
        let mut writer =
            arrow::json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow::json::writer::JsonArray>(MetadataBuffer::new(&self.budget));
        let mut rows = 0u64;
        for batch in reader {
            let batch = batch?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| Error::refusal("capacity", "report row count overflow"))?;
            writer.write(&batch)?;
        }
        writer.finish()?;
        let buffer = writer.into_inner();
        Ok((
            QueryResult::retained(
                name,
                columns,
                serde_json::from_slice(&buffer.bytes)?,
                buffer.memory,
            ),
            schema.as_ref().clone(),
            rows,
        ))
    }
}

struct MetadataBuffer {
    bytes: Vec<u8>,
    budget: Budget,
    memory: tokio::sync::OwnedSemaphorePermit,
}
impl MetadataBuffer {
    fn new(budget: &Budget) -> Self {
        Self {
            bytes: Vec::new(),
            budget: budget.clone(),
            memory: budget.reserve(0).expect("zero byte reservation"),
        }
    }
}
impl Write for MetadataBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= self.budget.limits.max_metadata_bytes)
            .ok_or_else(|| io::Error::other("report exceeds metadata allowance"))?;
        let charged = size
            .checked_mul(32)
            .ok_or_else(|| io::Error::other("report allocation overflow"))?;
        let additional = charged.saturating_sub(self.memory.num_permits());
        if additional > 0 {
            self.memory
                .merge(self.budget.reserve(additional).map_err(io::Error::other)?);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    super::export::sync_parent(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Limits;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field},
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};

    fn spilled(plan: &dyn ExecutionPlan) -> usize {
        let own = plan
            .metrics()
            .map_or(0, |metrics| metrics.spill_count().unwrap_or(0));
        own + plan
            .children()
            .iter()
            .map(|child| spilled(child.as_ref()))
            .sum::<usize>()
    }

    #[tokio::test]
    async fn finished_computation_releases_readers_while_report_and_schema_keep_admission() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("values.parquet");
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None)
                .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![std::sync::Arc::new(Int64Array::from(vec![1, 2]))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let budget = Budget::in_workspace(
            Limits {
                memory_bytes: 16 << 20,
                compute_threads: 1,
                scratch_bytes: 4 << 20,
                max_metadata_bytes: 1 << 20,
                max_batch_bytes: 64 << 10,
                batch_rows: 16,
            },
            &root.path().join("scratch"),
        )
        .unwrap();
        let available = || budget.reserve(0).unwrap().semaphore().available_permits();
        let job = Job::from_budget(budget.clone()).unwrap();
        job.register_parquet("events", &path).await.unwrap();
        let report = job
            .report_ipc("observed", "SELECT COUNT(*) AS observed_rows FROM events")
            .await
            .unwrap();
        let reader = StreamReader::try_new(Cursor::new(&report.bytes), None).unwrap();
        let schema = encode_schema(&reader.schema(), &budget).unwrap();
        let metadata_charge = report._memory.num_permits() + schema._memory.num_permits();
        assert!(metadata_charge > 0);
        let before = available();
        drop(job);
        let after = available();
        assert!(
            after >= before + std::fs::metadata(&path).unwrap().len() as usize,
            "the exact-file reader allowance must end after its last SQL consumer"
        );
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        drop(batches);
        drop(report);
        drop(schema);
        assert_eq!(available(), after + metadata_charge);
        eprintln!(
            "completed_owner_admission {}",
            serde_json::json!({"before_job_drop":before,"after_job_drop":after,"independent_metadata_charge":metadata_charge,"after_metadata_drop":available()})
        );
    }

    async fn spill_arm(root: &Path, scratch_bytes: u64) -> (bool, usize, u64, u64, String) {
        std::fs::create_dir_all(root).unwrap();
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        let retained = root.join("already-retained.bin");
        std::fs::write(&retained, vec![0x5a; 1 << 20]).unwrap();
        let initial = std::fs::metadata(&retained).unwrap().len();
        let limits = Limits {
            compute_threads: 1,
            memory_bytes: 8 << 20,
            scratch_bytes,
            batch_rows: 32,
            max_batch_bytes: 512 << 10,
            max_metadata_bytes: 256 << 10,
        };
        let host = Budget::in_workspace(limits, &root.join("scratch")).unwrap();
        let budget = host.with_retained_disk(initial).unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &host.runtime_env().memory_pool,
            &budget.runtime_env().memory_pool
        ));
        let file = budget
            .runtime_env()
            .disk_manager
            .create_tmp_file("new retained bytes")
            .unwrap();
        let mut writer = budget.scratch_writer(&file).unwrap();
        writer.write_all(&[0x22; 1024]).unwrap();
        drop(writer);
        std::fs::hard_link(file.path(), root.join("new-retained.bin")).unwrap();
        budget.retain_temporary(file).unwrap();
        assert_eq!(budget.retained_operation_disk_bytes(), initial + 1024);
        let job = Job::from_budget(budget.clone()).unwrap();
        let frame = job.sql("SELECT value, repeat(md5(CAST(value AS VARCHAR)), 256) AS payload FROM generate_series(0, 4095) ORDER BY payload").await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        };
        fn physical_bytes(root: &Path) -> u64 {
            std::fs::read_dir(root)
                .map(|entries| {
                    entries
                        .filter_map(std::result::Result::ok)
                        .map(|entry| {
                            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                                physical_bytes(&entry.path())
                            } else {
                                entry.metadata().map_or(0, |metadata| metadata.len())
                            }
                        })
                        .sum()
                })
                .unwrap_or(0)
        }
        let running = Arc::new(AtomicBool::new(true));
        let physical_peak = Arc::new(AtomicU64::new(physical_bytes(root)));
        let observer = {
            let running = running.clone();
            let peak = physical_peak.clone();
            let root = root.to_owned();
            std::thread::spawn(move || {
                while running.load(Ordering::Relaxed) {
                    peak.fetch_max(physical_bytes(&root), Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            })
        };
        let mut stream = execute_stream(plan.clone(), job.context.task_ctx()).unwrap();
        let mut rows = 0;
        let mut peak = budget.retained_operation_disk_bytes();
        let mut error = String::new();
        loop {
            match stream.try_next().await {
                Ok(Some(batch)) => rows += batch.num_rows(),
                Ok(None) => break,
                Err(failure) => {
                    error = failure.to_string();
                    break;
                }
            }
            peak = peak.max(budget.retained_operation_disk_bytes());
        }
        peak = peak.max(budget.retained_operation_disk_bytes());
        let spill_count = spilled(plan.as_ref());
        running.store(false, Ordering::Relaxed);
        observer.join().unwrap();
        eprintln!(
            "native_retained_spill_arm {}",
            serde_json::json!({"allowance":scratch_bytes,"initial_retained":initial,"new_retained":1024,"observed_native_combined_bytes":peak,"sampled_physical_combined_bytes":physical_peak.load(Ordering::Relaxed),"physical_sampling_ms":5,"spill_count":spill_count,"rows":rows,"error":error})
        );
        (
            rows == 4096 && error.is_empty(),
            spill_count,
            peak,
            initial,
            error,
        )
    }

    #[tokio::test]
    async fn retained_files_and_actual_native_spill_share_one_operation_allowance() {
        let root = tempfile::tempdir().unwrap();
        let enough = spill_arm(&root.path().join("enough"), 64 << 20).await;
        assert!(
            enough.0,
            "generous arm must finish through actual out-of-core execution: {}",
            enough.4
        );
        assert!(enough.1 > 0, "the passing arm must actually spill");
        let constrained_limit = (1 << 20) + (8 << 10);
        let constrained = spill_arm(&root.path().join("constrained"), constrained_limit).await;
        assert!(!constrained.0);
        assert!(
            constrained
                .4
                .contains("used disk space during the spilling process"),
            "native spill must enforce the reduced combined allowance before an export: {}",
            constrained.4
        );
        assert!(
            constrained.2 > constrained_limit,
            "the native post-write accounting must include cold retained bytes, new retained bytes and the refused spill chunk"
        );
        assert!(
            constrained.2 < constrained_limit + (2 << 20),
            "report the bounded native chunk overshoot rather than a strict pre-write/RSS guarantee"
        );
    }

    #[tokio::test]
    async fn exact_parquet_registration_refuses_a_footer_before_its_unbounded_decode() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("large-footer.parquet");
        let schema = std::sync::Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int64, false)],
            std::collections::HashMap::from([("large".into(), "x".repeat(64 << 10))]),
        ));
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None)
                .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![std::sync::Arc::new(Int64Array::from(Vec::<i64>::new()))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let limits = Limits {
            memory_bytes: 128 << 20,
            compute_threads: 1,
            scratch_bytes: 4 << 20,
            max_metadata_bytes: 4096,
            max_batch_bytes: 64 << 10,
            batch_rows: 16,
        };
        let job = Job::new(limits, root.path().join("scratch")).unwrap();
        assert_eq!(
            job.register_parquet("events", &path)
                .await
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
        assert!(!job.context.table_exist("events").unwrap());
    }
}
