//! Keep admitted native metadata alive through provider, plan and stream ownership.
// Rust guideline compliant 2026-02-21
use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Result, Statistics, tree_node::TreeNodeRecursion},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, StatisticsArgs, stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

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

#[derive(Debug)]
pub(super) struct AdmittedTable {
    pub provider: Arc<dyn TableProvider>,
    pub memory: Arc<OwnedSemaphorePermit>,
    pub observation: NativeInputObservation,
}
#[async_trait::async_trait]
impl TableProvider for AdmittedTable {
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
    memory: Arc<OwnedSemaphorePermit>,
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
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(sem.available_permits(), 0);
        // DataFusion 55's stream adapter drops a finished inner stream, so the
        // last owner releases the admission at end of stream, not at drop.
        assert!(stream.next().await.is_none());
        assert_eq!(sem.available_permits(), 1);
        drop(stream);
        assert_eq!(sem.available_permits(), 1);
    }
}
