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
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub(super) struct AdmittedTable {
    pub provider: Arc<dyn TableProvider>,
    pub memory: Arc<OwnedSemaphorePermit>,
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
