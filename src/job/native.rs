//! Keep admitted native metadata alive through provider, plan and stream ownership.
// Rust guideline compliant 2026-02-21
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Result, Statistics},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

use crate::{budget::Budget, storage::NativeTableSnapshot};

/// Available summary facts from one input's exact selected native metadata.
///
/// These facts require no additional catalog, manifest or data reads. They
/// describe the selected metadata, rather than independently verifying files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeInputObservation {
    /// Registered input name, identifying its immutable selection in the job.
    pub name: String,
    /// Native total-data-files summary, or zero for an exactly empty selection.
    /// Missing or invalid optional summary counts remain unknown.
    pub data_file_count: Option<u64>,
    /// Retained snapshots present in this selected metadata file.
    pub snapshot_count: usize,
    /// Unavailable until the native planner exposes a maintained observation hook.
    pub manifest_count: Option<u64>,
    /// Unavailable; neither first-batch latency nor reader backpressure supplies this.
    pub planning_seconds: Option<f64>,
}

impl NativeInputObservation {
    pub(super) fn from_table(name: &str, table: &iceberg::table::Table) -> Self {
        let data_file_count = match table.metadata().current_snapshot() {
            None => Some(0),
            Some(snapshot) => snapshot
                .summary()
                .additional_properties
                .get("total-data-files")
                .and_then(|value| value.parse().ok()),
        };
        Self {
            name: name.to_owned(),
            data_file_count,
            snapshot_count: table.metadata().snapshots().count(),
            manifest_count: None,
            planning_seconds: None,
        }
    }
}

/// Exact native version and interpreted schema admitted by a bounded job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeVersionObservation {
    pub name: String,
    pub version: crate::model::VersionRef,
    pub schema_id: i32,
    pub schema: arrow::datatypes::Schema,
    /// Exact physical metadata opened for this invocation, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_snapshot: Option<NativeVersionSnapshotObservation>,
}

/// Physical snapshot and stored metadata bytes observed during a protected open.
///
/// This observation can identify input for later admission. The source's
/// existing availability and later admission govern object retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeVersionSnapshotObservation {
    /// Selector taken from the exact metadata loaded for the protected read.
    pub selector: NativeTableSnapshot,
    /// SHA-256 of the stored metadata, including compression when present.
    pub metadata_sha256: String,
    /// Verified byte length of the stored metadata.
    pub metadata_size: u64,
    #[serde(skip)]
    _memory: Option<Arc<OwnedSemaphorePermit>>,
}

impl NativeVersionSnapshotObservation {
    pub(super) fn from_table(
        table: &iceberg::table::Table,
        metadata_sha256: sha2::digest::Output<sha2::Sha256>,
        metadata_size: u64,
        budget: &Budget,
    ) -> crate::Result<Self> {
        let metadata_location = table.metadata_location().ok_or_else(|| {
            crate::Error::refusal("pin_mismatch", "opened metadata has no exact location")
        })?;
        // The returned descriptor owns the location and hex digest separately
        // from the table, so the result retains its own cumulative charge.
        let bytes = size_of::<Self>()
            .saturating_add(metadata_location.len())
            .saturating_add(metadata_sha256.len() * 2);
        let memory = budget.reserve(bytes)?;
        let metadata = table.metadata();
        Ok(Self {
            selector: NativeTableSnapshot {
                metadata_location: metadata_location.to_owned(),
                table_uuid: metadata.uuid(),
                snapshot: metadata.current_snapshot_id(),
                schema_id: metadata.current_schema_id(),
            },
            metadata_sha256: format!("{metadata_sha256:x}"),
            metadata_size,
            _memory: Some(Arc::new(memory)),
        })
    }
}

#[derive(Debug)]
pub(super) struct AdmittedTable {
    pub provider: Arc<dyn TableProvider>,
    pub memory: Arc<OwnedSemaphorePermit>,
    pub observation: NativeInputObservation,
}
#[async_trait::async_trait]
impl TableProvider for AdmittedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.provider.schema()
    }
    fn table_type(&self) -> TableType {
        self.provider.table_type()
    }
    fn statistics(&self) -> Option<Statistics> {
        self.provider.statistics()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let mut support = super::fitting::filter_support(self.provider.as_ref(), filters)?;
        let schema = self.provider.schema();
        for (support, filter) in support.iter_mut().zip(filters) {
            // `iceberg` 0.10 builds `StructAccessor` entries only for primitive
            // leaves, so binding a predicate over any top-level nested column
            // (`List`, `Struct`, `Map`) fails with `Accessor for Field … not found`.
            // Keep such predicates in DataFusion's `FilterExec` above the scan.
            if filter.column_refs().iter().any(|column| {
                let target = column.name.to_lowercase();
                schema.fields().iter().any(|field| {
                    field.data_type().is_nested() && field.name().to_lowercase() == target
                })
            }) {
                *support = TableProviderFilterPushDown::Unsupported;
            }
        }
        Ok(support)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AdmittedPlan {
            input: self
                .provider
                .scan(state, projection, filters, limit)
                .await?,
            memory: self.memory.clone(),
            // The pin wrapped lake inputs in this node with DataFusion's
            // default, so lake plans keep their round-robin repartition.
            parallel_input: true,
        }))
    }
}
/// Holds a metadata admission for as long as its plan runs.
///
/// The wrapper only keeps the permit alive, so plans keep the pin's (b0342a7)
/// shape. It keeps its input as a child, so the planner still splits a large
/// file scan beneath it and moves sorts through it (it keeps input order). What
/// it asks of input partitioning depends on what the pin had:
///
/// - Over a lake input ([`AdmittedTable`]) it asks for parallel input,
///   DataFusion's default, as the pin's wrapper there did: at two or more
///   threads the planner puts a round-robin repartition beneath it.
/// - Elsewhere ([`admit_metadata_plan`]: registered Parquet inputs, spilled
///   steps' replays, a step's whole plan), where the pin had no wrapper, it
///   declines, as DataFusion's pass-through `CoalesceBatchesExec` does. Asking
///   put a round-robin repartition beneath every such scan at two threads,
///   adding spill-capable consumers that shrank every spiller's share of the
///   pool (O10). A parent that wants parallel input still gets its
///   repartition, above the wrapper, as above the bare scan.
///
/// Whether limit, projection and filter pushdown stop at the wrapper, against
/// a plan without it, is a measured follow-up.
#[derive(Debug)]
struct AdmittedPlan {
    input: Arc<dyn ExecutionPlan>,
    memory: Arc<dyn fmt::Debug + Send + Sync>,
    /// Whether to ask for parallel input: only over lake inputs.
    parallel_input: bool,
}
/// Admits a plan the pin read unwrapped; it declines input partitioning.
pub(super) fn admit_metadata_plan(
    input: Arc<dyn ExecutionPlan>,
    memory: Arc<dyn fmt::Debug + Send + Sync>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(AdmittedPlan {
        input,
        memory,
        parallel_input: false,
    })
}
impl DisplayAs for AdmittedPlan {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NativeMetadataAdmission")
    }
}
impl ExecutionPlan for AdmittedPlan {
    fn name(&self) -> &str {
        "NativeMetadataAdmission"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![self.parallel_input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(datafusion::common::DataFusionError::Plan(
                "native metadata admission needs one child".into(),
            ));
        };
        Ok(Arc::new(Self {
            input: input.clone(),
            memory: self.memory.clone(),
            parallel_input: self.parallel_input,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let memory = self.memory.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream.map(move |batch| {
                let _retain = &memory;
                batch
            }),
        )))
    }
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }
}

/// `plan`'s operator names in pre-order, leaving out metadata admission.
///
/// The names are the plan as the planner shapes it, which admission must not
/// change: what remains can be compared with a plan made without the wrapper.
#[cfg(test)]
pub(super) fn operators_beside_admission(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut names = Vec::new();
    let mut pending = vec![plan.clone()];
    while let Some(plan) = pending.pop() {
        if !plan.as_any().is::<AdmittedPlan>() {
            names.push(plan.name().to_owned());
        }
        pending.extend(plan.children().into_iter().rev().cloned());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::job::Job;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::displayable;

    /// The pipeline's bounded-memory shape: two threads and 32-row batches,
    /// where the planner repartitions beneath any operator that asks for it.
    fn bounded_job(root: &tempfile::TempDir) -> Job {
        let limits = crate::budget::Limits {
            compute_threads: 2,
            batch_rows: 32,
            ..crate::budget::Limits::default()
        };
        Job::new(limits, root.path().join("scratch")).unwrap()
    }

    /// 1,024 rows of a key `id` and a short `text`.
    fn input_batch() -> RecordBatch {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        let rows = 1024;
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("text-{row:04}")),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn write_input(path: &std::path::Path) {
        let batch = input_batch();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn plan_of(job: &Job, sql: &str) -> Arc<dyn ExecutionPlan> {
        let frame = job.sql(sql).await.unwrap();
        frame.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn a_sort_over_a_parquet_input_is_planned_as_if_unadmitted() {
        let root = tempfile::tempdir().unwrap();
        let job = bounded_job(&root);
        let path = root.path().join("input.parquet");
        write_input(&path);
        job.register_parquet("input", &path).await.unwrap();
        let plan = plan_of(&job, "SELECT id, text FROM input ORDER BY text").await;
        // The pin (b0342a7) had no admission and planned this sort over one
        // scan: EXPLAIN ANALYZE of the same sort over a Parquet input on the
        // pinned binary (one-line scratch, cmp/probe-pin-r1-old_input-t2-m192-s1).
        // Admitted, the scan was split by a round-robin repartition beneath the
        // wrapper, doubling the sort and adding spill-capable consumers that
        // shrink every spiller's share of the pool.
        assert_eq!(
            operators_beside_admission(&plan),
            ["SortExec", "CompactViews", "DataSourceExec"],
            "{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn parents_plan_alike_over_an_admitted_and_a_bare_parquet_input() {
        // Declining partitioning must not take it from a parent that wants it:
        // a filter, an aggregate and a join get their repartition above the
        // wrapper, as over the bare scan. A file is admitted when registered;
        // a directory is not (`Job::register_parquet`), so the same rows give
        // both forms.
        let root = tempfile::tempdir().unwrap();
        let job = bounded_job(&root);
        let folder = root.path().join("folder");
        std::fs::create_dir(&folder).unwrap();
        let path = folder.join("part.parquet");
        write_input(&path);
        job.register_parquet("admitted", &path).await.unwrap();
        job.register_parquet("bare", &folder).await.unwrap();
        for statement in [
            "SELECT id FROM {t} WHERE id % 3 = 0",
            "SELECT text, count(*) AS n FROM {t} GROUP BY text",
            "SELECT a.id FROM {t} a JOIN {t} b ON a.id = b.id",
            "SELECT id, text FROM {t} ORDER BY text",
        ] {
            let admitted = plan_of(&job, &statement.replace("{t}", "admitted")).await;
            let bare = plan_of(&job, &statement.replace("{t}", "bare")).await;
            assert_eq!(
                operators_beside_admission(&admitted),
                operators_beside_admission(&bare),
                "{statement}\nadmitted:\n{}\nbare:\n{}",
                displayable(admitted.as_ref()).indent(true),
                displayable(bare.as_ref()).indent(true)
            );
        }
    }

    #[tokio::test]
    async fn a_lake_input_keeps_the_pins_round_robin_at_two_threads() {
        use datafusion::physical_plan::Partitioning;
        // The pin (b0342a7, src/job.rs:314 and native.rs:91-123) wrapped every
        // lake input in this node, asking for parallel input: a lake scan has
        // one partition and no row count, so at two threads the planner put a
        // round-robin repartition beneath the wrapper. Lake plans stay so.
        let root = tempfile::tempdir().unwrap();
        let job = bounded_job(&root);
        let lake = root.path().join("lake");
        let catalog = crate::storage::local_catalog(&lake).await.unwrap();
        let namespace = iceberg::NamespaceIdent::new("lake".into());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();
        let batch = input_batch();
        let schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(batch.schema().as_ref())
                .unwrap();
        let creation = iceberg::TableCreation::builder()
            .name("source".into())
            .location(lake.join("source").to_string_lossy().into_owned())
            .schema(schema)
            .properties(crate::storage::table_properties())
            .build();
        let table = catalog.create_table(&namespace, creation).await.unwrap();
        let rows = futures::stream::iter([Ok(batch)]);
        let files = crate::storage::write_batches(&table, rows, &job.budget)
            .await
            .unwrap();
        let table = crate::storage::append(&table, catalog.as_ref(), files)
            .await
            .unwrap();
        job.register_native_table("lake", table).await.unwrap();
        let plan = plan_of(&job, "SELECT id, text FROM lake").await;
        let mut pending = vec![plan.clone()];
        let admission = std::iter::from_fn(|| {
            let next = pending.pop()?;
            pending.extend(next.children().into_iter().cloned());
            Some(next)
        })
        .find(|node| node.as_any().is::<AdmittedPlan>())
        .expect("a lake input is admitted");
        let beneath = admission.children()[0];
        assert!(
            matches!(
                beneath.properties().output_partitioning(),
                Partitioning::RoundRobinBatch(2)
            ),
            "{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn protected_snapshot_result_admission_survives_partial_failure() {
        use crate::{
            budget::Limits,
            job::{JobFailure, JobResult},
            model::VersionRef,
        };
        use iceberg::{
            Runtime, TableIdent,
            io::FileIO,
            spec::{NestedField, PrimitiveType, Schema, Type},
            table::Table,
        };
        use serde_json::json;
        use sha2::{Digest, Sha256};
        use std::path::Path;

        let limits = Limits {
            memory_bytes: 128 << 20,
            ..Limits::default()
        };
        let budget = Budget::new(limits).unwrap();
        let available = || budget.reserve(0).unwrap().semaphore().available_permits();
        let initial = available();
        let metadata = crate::storage::fresh_metadata(
            Schema::builder()
                .with_fields([
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
            Path::new("memory:///table"),
        )
        .unwrap();
        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("memory:///table/metadata.json")
            .identifier(TableIdent::from_strs(["fixture", "source"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::try_current().unwrap())
            .readonly(true)
            .build()
            .unwrap();
        let version: VersionRef = serde_json::from_value(json!({
            "dataset": "source", "version": "initial", "manifest": {
                "owner": "source", "path": "memory:///manifest.json", "digest": "0".repeat(64), "bytes": 1,
            },
        })).unwrap();
        let mut completed: JobResult =
            serde_json::from_value(json!({"steps": [], "outputs": []})).unwrap();
        let observe = || {
            NativeVersionSnapshotObservation::from_table(
                &table,
                Sha256::digest(b"stored bytes"),
                12,
                &budget,
            )
        };
        let mut charges = Vec::new();
        for name in ["first", "second"] {
            let before = available();
            let physical_snapshot = observe().unwrap();
            let charge = before - available();
            assert!(charge > 0);
            charges.push(charge);
            assert_eq!(physical_snapshot.selector.snapshot, None);
            completed.native_versions.push(NativeVersionObservation {
                name: name.into(),
                version: version.clone(),
                schema_id: 0,
                schema: arrow::datatypes::Schema::empty(),
                physical_snapshot: Some(physical_snapshot),
            });
        }
        assert_eq!(charges[0], charges[1]);
        let exhausted = budget.reserve(available()).unwrap();
        assert_eq!(observe().unwrap_err().code(), "capacity");
        drop(exhausted);
        drop(table);
        let mut failure = JobFailure {
            completed,
            error: crate::Error::refusal("fixture", "late job failure"),
        };
        let report = failure.report();
        assert!(
            report["completed"]["native_versions"][0]["physical_snapshot"]["selector"]["snapshot"]
                .is_null()
        );
        let mut legacy = report["completed"]["native_versions"][0].clone();
        drop(report);
        legacy.as_object_mut().unwrap().remove("physical_snapshot");
        let legacy: NativeVersionObservation = serde_json::from_value(legacy).unwrap();
        assert!(legacy.physical_snapshot.is_none());
        assert_eq!(available(), initial - charges[0] - charges[1]);
        let remaining = failure.completed.native_versions.pop().unwrap();
        drop(failure);
        assert_eq!(available(), initial - charges[1]);
        drop(remaining);
        assert_eq!(available(), initial);
    }

    #[tokio::test]
    async fn metadata_admission_survives_deregister_frame_plan_and_stream() {
        use arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let provider = Arc::new(AdmittedTable {
            provider: Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            memory: Arc::new(sem.clone().try_acquire_owned().unwrap()),
            observation: NativeInputObservation {
                name: "source".into(),
                data_file_count: None,
                snapshot_count: 0,
                manifest_count: None,
                planning_seconds: None,
            },
        });
        let context = SessionContext::new();
        context.register_table("source", provider).unwrap();
        let frame = context.sql("SELECT id FROM source").await.unwrap();
        context.deregister_table("source").unwrap();
        drop(context);
        assert_eq!(sem.available_permits(), 0);
        let mut stream = frame.execute_stream().await.unwrap();
        assert_eq!(sem.available_permits(), 0);
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(stream.next().await.is_none());
        assert_eq!(sem.available_permits(), 0);
        drop(stream);
        assert_eq!(sem.available_permits(), 1);
    }
}
