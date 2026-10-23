//! Unit tests for join cardinality probe execution and lazy partner scan skipping.

use super::*;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::streaming::StreamingTable,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountedPartition {
    batch: RecordBatch,
    executions: Arc<AtomicUsize>,
}

impl PartitionStream for CountedPartition {
    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(RecordBatchStreamAdapter::new(
            self.batch.schema(),
            futures::stream::iter([Ok(self.batch.clone())]),
        ))
    }
}

fn register(context: &SessionContext, name: &str, values: Vec<i64>) -> Arc<AtomicUsize> {
    let executions = Arc::new(AtomicUsize::new(0));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new(storage::OCCURRENCE_FILE, DataType::Utf8, false),
        Field::new(storage::OCCURRENCE_ROW, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(values.clone())),
            Arc::new(StringArray::from(vec![name; values.len()])),
            Arc::new(Int64Array::from_iter_values(0..values.len() as i64)),
        ],
    )
    .unwrap();
    let partition = Arc::new(CountedPartition {
        batch,
        executions: executions.clone(),
    });
    context
        .register_table(
            name,
            Arc::new(StreamingTable::try_new(schema, vec![partition]).unwrap()),
        )
        .unwrap();
    executions
}

fn spec(cardinality: JoinCardinality) -> JoinTransform {
    let dataset = Id::new("fixture").unwrap();
    let version = VersionRef {
        dataset: dataset.clone(),
        version: Id::new("initial").unwrap(),
        manifest: ObjectRef {
            owner: dataset,
            path: String::new(),
            digest: String::new(),
            bytes: 0,
        },
    };
    JoinTransform {
        left: version.clone(),
        right: version,
        join: JoinDefinition {
            keys: vec![JoinKey {
                left: "key".into(),
                right: "key".into(),
            }],
            kind: JoinKind::Inner,
            nulls: NullMatching::Equal,
            cardinality,
            fields: vec![],
            filter: None,
            projections: BTreeMap::new(),
        },
        filter: None,
        projections: BTreeMap::new(),
    }
}

#[tokio::test]
async fn unique_keys_skip_partner_execution_and_duplicates_run_matched_proof() {
    for (cardinality, right_values, expected_left, expected_right, refuses) in [
        (JoinCardinality::ManyToMany, vec![1, 2], 0, 0, false),
        (JoinCardinality::ManyToOne, vec![1, 2], 0, 1, false),
        (JoinCardinality::OneToOne, vec![1, 2], 1, 1, false),
        (JoinCardinality::ManyToOne, vec![8, 8], 1, 2, false),
        (JoinCardinality::ManyToOne, vec![1, 1], 1, 2, true),
    ] {
        let budget = Budget::new(Limits {
            compute_threads: 2,
            memory_bytes: 512 << 20,
            scratch_bytes: 16 << 20,
            ..Limits::default()
        })
        .unwrap();
        let context = budget.context();
        let left = register(&context, "join_left", vec![1, 2]);
        let right = register(&context, "join_right", right_values);
        let result = join_plan(&context, &spec(cardinality), true).await;
        if refuses {
            assert_eq!(result.unwrap_err().code(), "join_cardinality");
        } else {
            assert!(result.is_ok());
        }
        // The returned main frame remains lazy: these counts cover proof execution only.
        assert_eq!(
            left.load(Ordering::SeqCst),
            expected_left,
            "{cardinality:?}"
        );
        assert_eq!(
            right.load(Ordering::SeqCst),
            expected_right,
            "{cardinality:?}"
        );
    }
}
