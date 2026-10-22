//! Availability retirement ends independent lookup without erasing retained dependent work.
#![cfg(feature = "test-util")]
// Rust guideline compliant 2026-02-21

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{
        AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, BackfillSpec,
        ComputedField, Id, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform, ReleasePin,
        RetirementScope, RunSpec,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

async fn open(root: &Path) -> Engine {
    Engine::open(root.join("workspace"), limits())
        .await
        .unwrap()
}

fn request(root: &Path, dataset: &str, operation: &str, values: Vec<i64>) -> RunSpec {
    let path = root.join(format!("{dataset}-{operation}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: id(dataset),
        operation: id(operation),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    }
}

fn backfill(base: &ReleasePin, name: &str, field_id: i32) -> BackfillSpec {
    BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(name),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id(name),
        group_definition: "scalar-v1".into(),
        fields: vec![ComputedField {
            id: field_id,
            name: name.into(),
            expression: format!("id + {field_id}"),
        }],
    }
}

fn summary(input: &ReleasePin, dataset: &str) -> RefreshSpec {
    RefreshSpec {
        dataset: id(dataset),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["id".into()],
                aggregates: vec![AggregateField {
                    name: "rows".into(),
                    function: AggregateFunction::CountRows,
                }],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    }
}

// Visit only this test's private immutable-object directory. Catalog retirement
// records may grow; existing input/output objects must neither disappear nor change.
fn objects(root: &Path) -> BTreeMap<PathBuf, (u64, SystemTime)> {
    let mut pending = vec![root.join("workspace/objects")];
    let mut files = BTreeMap::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files.insert(entry.path(), (metadata.len(), metadata.modified().unwrap()));
            }
        }
    }
    files
}

fn saved_availability(records: &BTreeMap<String, Value>, operation: &Id) -> BTreeSet<String> {
    records
        .keys()
        .filter(|key| {
            **key == format!("result/{operation}")
                || **key == format!("readback/{operation}")
                || key.starts_with(&format!("backfill-readback/{operation}/"))
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn retired_historical_build_refuses_lookup_and_own_retries_but_keeps_history_and_files() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let first = request(root.path(), "comments", "initial", vec![1, 1, 2]);
    let old = engine.run(&first).await.unwrap();
    assert_eq!(
        engine
            .plan_availability_retirement(
                &first.dataset,
                &id("current"),
                &first.operation,
                "end old availability"
            )
            .await
            .unwrap_err()
            .code(),
        "selected_result"
    );
    let replacement = request(root.path(), "comments", "replacement", vec![9]);
    let current = engine.run(&replacement).await.unwrap();
    let before = engine.inspect(&first.dataset).await.unwrap();
    let expected = saved_availability(&before, &first.operation);
    assert_eq!(
        expected,
        BTreeSet::from(["result/initial".into(), "readback/initial".into()])
    );
    let plan = engine
        .plan_availability_retirement(
            &first.dataset,
            &id("retire-old"),
            &first.operation,
            "end old independent availability",
        )
        .await
        .unwrap();
    assert_eq!(
        plan.scope,
        RetirementScope::ResultAvailability {
            operation: first.operation.clone()
        }
    );
    assert_eq!(
        plan.records
            .iter()
            .map(|record| record.key.clone())
            .collect::<BTreeSet<_>>(),
        expected
    );
    let mut incomplete = plan.clone();
    incomplete.retirement = id("incomplete");
    incomplete
        .records
        .retain(|record| record.key.starts_with("result/"));
    assert_eq!(
        engine
            .retire_availability(&incomplete)
            .await
            .unwrap_err()
            .code(),
        "stale_retirement"
    );
    let another = engine
        .plan_availability_retirement(
            &first.dataset,
            &id("another"),
            &first.operation,
            "a separate decision",
        )
        .await
        .unwrap();
    let retained_objects = objects(root.path());
    let decision = engine.retire_availability(&plan).await.unwrap();
    assert_eq!(objects(root.path()), retained_objects);
    std::fs::remove_file(&first.input).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(engine.retire_availability(&plan).await.unwrap(), decision);
    assert_eq!(
        engine
            .retire_availability(&another)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert_eq!(
        engine
            .release(&first.dataset, &first.operation)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert_eq!(
        engine.run(&first).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(
        engine.plan(&first).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(engine.latest(&first.dataset).await.unwrap(), current);
    let proof = engine
        .prove_succession(&old.version_ref(), &current.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.prior, old.version_ref());
    assert_eq!(proof.target, current.version_ref());
    assert_eq!(proof.selections_advanced, 1);
    let after = engine.inspect(&first.dataset).await.unwrap();
    for key in expected {
        assert_eq!(
            after[&key], before[&key],
            "retirement retains its immutable receipts"
        );
    }
    assert_eq!(
        objects(root.path()),
        retained_objects,
        "lookup and retries must not redo processing"
    );
}

#[tokio::test]
async fn abandoned_backfill_retires_every_saved_composition_without_recomputing_on_retry() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let source = request(root.path(), "comments", "initial", vec![1, 1, 2]);
    let base = engine.run(&source).await.unwrap();
    let pending = backfill(&base, "pending", 10);
    assert_eq!(
        engine
            .backfill_until(&pending, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .plan_availability_retirement(
                &pending.dataset,
                &id("active"),
                &pending.operation,
                "end pending availability"
            )
            .await
            .unwrap_err()
            .code(),
        "operation_active"
    );
    let sibling = engine
        .backfill(&backfill(&base, "sibling", 11))
        .await
        .unwrap();
    assert_eq!(
        engine
            .backfill_until(&pending, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&pending.dataset).await.unwrap();
    let expected = saved_availability(&before, &pending.operation);
    assert_eq!(
        expected.len(),
        2,
        "both independently retained compositions must be covered"
    );
    assert!(
        expected
            .iter()
            .all(|key| key.starts_with("backfill-readback/pending/"))
    );
    engine
        .abandon(
            &pending.dataset,
            &pending.operation,
            "stop this unselected backfill",
        )
        .await
        .unwrap();
    assert!(!before.contains_key("result/pending"));
    let retained = expected
        .iter()
        .map(|key| serde_json::from_value::<ReleasePin>(before[key].clone()).unwrap())
        .collect::<Vec<_>>();
    for pin in &retained {
        let combined = engine.read_combined(pin).await.unwrap();
        assert!(combined.schema().field_with_name("pending").is_ok());
        for mut stream in [combined, engine.read_group(pin, "source").await.unwrap()] {
            let mut actual = Vec::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                if let Some(values) = batch.column_by_name("pending") {
                    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert!(
                        ids.iter()
                            .zip(values.iter())
                            .all(|(id, value)| { value == id.map(|id| id + 10) })
                    );
                }
                actual.extend(ids.iter());
            }
            actual.sort();
            assert_eq!(actual, vec![Some(1), Some(1), Some(2)]);
        }
    }
    let plan = engine
        .plan_availability_retirement(
            &pending.dataset,
            &id("retire-pending"),
            &pending.operation,
            "discard saved unselected compositions",
        )
        .await
        .unwrap();
    assert_eq!(
        plan.records
            .iter()
            .map(|record| record.key.clone())
            .collect::<BTreeSet<_>>(),
        expected
    );
    let retained_objects = objects(root.path());
    let decision = engine.retire_availability(&plan).await.unwrap();
    std::fs::remove_file(&source.input).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(engine.retire_availability(&plan).await.unwrap(), decision);
    for pin in &retained {
        for error in [
            engine.read_group(pin, "source").await.err().unwrap(),
            engine.read_evidence(pin).await.err().unwrap(),
            engine.read_combined(pin).await.err().unwrap(),
            engine.compare(pin, &sibling, "source").await.unwrap_err(),
        ] {
            assert_eq!(error.code(), "availability_retired");
        }
    }
    assert_eq!(
        engine.backfill(&pending).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(
        engine.plan_backfill(&pending).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(engine.latest(&pending.dataset).await.unwrap(), sibling);
    let after = engine.inspect(&pending.dataset).await.unwrap();
    for (key, value) in &before {
        assert_eq!(
            after[key], *value,
            "retirement must preserve completed phases and readbacks"
        );
    }
    assert_eq!(objects(root.path()), retained_objects);
}

#[tokio::test]
async fn retained_dependent_refresh_resumes_after_source_retirement_while_fresh_capture_refuses() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let source = request(root.path(), "comments", "initial", vec![1, 1, 2]);
    let original = engine.run(&source).await.unwrap();
    let dependent = summary(&original, "retained-summary");
    assert_eq!(
        engine
            .refresh_until(&dependent, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained = engine.inspect(&dependent.dataset).await.unwrap();
    let replacement = request(root.path(), "comments", "replacement", vec![9]);
    engine.run(&replacement).await.unwrap();
    let plan = engine
        .plan_availability_retirement(
            &source.dataset,
            &id("retire-input"),
            &source.operation,
            "stop independent input lookup",
        )
        .await
        .unwrap();
    engine.retire_availability(&plan).await.unwrap();
    std::fs::remove_file(&source.input).unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    let inputs = BTreeMap::from([(source.dataset.clone(), original.version_ref())]);
    assert_eq!(
        engine.capture_inputs(&inputs).await.unwrap_err().code(),
        "availability_retired"
    );
    let fresh = summary(&original, "fresh-summary");
    assert_eq!(
        engine.refresh(&fresh).await.unwrap_err().code(),
        "availability_retired"
    );
    let resumed_plan = engine.plan_refresh(&dependent).await.unwrap();
    assert!(
        !resumed_plan
            .required_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert!(
        resumed_plan
            .reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let result = engine.refresh(&dependent).await.unwrap();
    assert_eq!(
        result.dataset.related.as_ref().unwrap().coverage.inputs,
        inputs
    );
    let after = engine.inspect(&dependent.dataset).await.unwrap();
    for (key, value) in retained.iter().filter(|(key, _)| key.starts_with("phase/")) {
        assert_eq!(
            after[key], *value,
            "already completed input processing must be reused"
        );
    }
    let mut stream = engine.read_group(&result, "source").await.unwrap();
    let mut values = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column_by_name("rows")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(values.insert(ids.value(row), counts.value(row)).is_none());
        }
    }
    assert_eq!(
        values,
        BTreeMap::from([(1, 2), (2, 1)]),
        "resume uses the retained source rather than the replacement head"
    );
}
