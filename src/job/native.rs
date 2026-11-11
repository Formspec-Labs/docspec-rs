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
        super::fitting::filter_support(self.provider.as_ref(), filters)
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
        }))
    }
}
#[derive(Debug)]
struct AdmittedPlan {
    input: Arc<dyn ExecutionPlan>,
    memory: Arc<dyn fmt::Debug + Send + Sync>,
}
pub(super) fn admit_metadata_plan(
    input: Arc<dyn ExecutionPlan>,
    memory: Arc<dyn fmt::Debug + Send + Sync>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(AdmittedPlan { input, memory })
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

    #[tokio::test]
    async fn a_sort_over_a_parquet_input_is_planned_as_if_unadmitted() {
        use crate::{budget::Limits, job::Job};
        use arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            record_batch::RecordBatch,
        };
        use datafusion::physical_plan::displayable;
        use parquet::arrow::ArrowWriter;

        // The pipeline's bounded-memory shape: two threads and 32-row batches,
        // where the planner repartitions beneath any operator that asks for it.
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            compute_threads: 2,
            batch_rows: 32,
            ..Limits::default()
        };
        let job = Job::new(limits, root.path().join("scratch")).unwrap();
        let path = root.path().join("input.parquet");
        let rows = 1024;
        let batch = RecordBatch::try_from_iter([
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
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        job.register_parquet("input", &path).await.unwrap();
        let plan = job
            .sql("SELECT id, text FROM input ORDER BY text")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
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
