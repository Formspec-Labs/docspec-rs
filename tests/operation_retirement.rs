//! Terminal recovery retirement releases unused roots and preserves reusable phases.
#![cfg(feature = "test-util")]

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{Id, PhaseResult, ReleasePin, RunSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn open(root: &Path) -> Engine {
    Engine::open(
        root.join("workspace"),
        Limits {
            compute_threads: 2,
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 4 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap()
}

fn build(root: &Path, operation: &str, values: Vec<i64>) -> RunSpec {
    let path = root.join(format!("{operation}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: id("comments"),
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

fn phase_keys(records: &BTreeMap<String, Value>, operation: &Id) -> Vec<String> {
    records
        .iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.operation == *operation).then(|| key.clone())
        })
        .collect()
}

async fn values(engine: &Engine, release: &ReleasePin) -> Vec<i64> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut result = vec![];
    while let Some(batch) = stream.try_next().await.unwrap() {
        result.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn all_explicit_a_promises_retire_while_independent_selected_b_stays_protected() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let a = build(root.path(), "a", vec![1, 1, 2]);
    let original = engine.run(&a).await.unwrap();
    let original_records = engine.inspect(&a.dataset).await.unwrap();
    let phases = phase_keys(&original_records, &a.operation);
    assert!(!phases.is_empty());
    let b = build(root.path(), "b", vec![8, 9]);
    let selected = engine.run(&b).await.unwrap();
    let old_object = original.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    let new_object = selected.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    assert_ne!(old_object, new_object);
    let availability = engine
        .plan_availability_retirement(
            &a.dataset,
            &id("a-availability"),
            &a.operation,
            "end A lookup",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let phase_plan = engine
        .plan_phase_retirement(&a.dataset, &id("a-phases"), &phases, "end A phase recovery")
        .await
        .unwrap();
    engine.retire_phases(&phase_plan).await.unwrap();
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&old_object))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let recovery = engine
        .plan_operation_retirement(
            &a.dataset,
            &id("a-recovery"),
            &a.operation,
            "end A recovery",
        )
        .await
        .unwrap();
    assert!(
        recovery
            .records
            .iter()
            .any(|record| record.key.starts_with("protection/"))
    );
    let retired = engine.retire_operation(&recovery).await.unwrap();
    assert_eq!(engine.retire_operation(&recovery).await.unwrap(), retired);
    drop(engine);

    let engine = open(root.path()).await;
    let inspection = engine
        .inspect_retention(&[old_object.clone(), new_object.clone()])
        .await
        .unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .find(|item| item.object == old_object)
            .unwrap()
            .blocker
            .is_none()
    );
    assert!(
        inspection
            .objects
            .iter()
            .find(|item| item.object == new_object)
            .unwrap()
            .blocker
            .is_some()
    );
    assert_eq!(engine.latest(&b.dataset).await.unwrap(), selected);
    assert_eq!(values(&engine, &selected).await, vec![8, 9]);
    assert_eq!(
        storage::digest(Path::new(&old_object.path)).unwrap().0,
        old_object.digest
    );
    let after = engine.inspect(&a.dataset).await.unwrap();
    for (key, value) in original_records.iter().filter(|(key, _)| {
        key.starts_with("operation/")
            || key.starts_with("request/")
            || key.starts_with("request-dependencies/")
            || key.starts_with("protection/")
            || key.starts_with("result/")
            || key.starts_with("succession/")
            || key.starts_with("phase/")
            || key.starts_with("readback/")
    }) {
        assert_eq!(
            &after[key], value,
            "retirement preserves original receipt {key}"
        );
    }
}

#[tokio::test]
async fn completed_recovery_retirement_keeps_phases_reusable_and_creator_evidence_readable() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let a = build(root.path(), "a", vec![2, 2, 7]);
    let original = engine.run(&a).await.unwrap();
    let before = engine.inspect(&a.dataset).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &a.dataset,
            &id("a-recovery"),
            &a.operation,
            "end parent recovery only",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    drop(engine);

    let saved_input = root.path().join("a-unavailable.parquet");
    std::fs::rename(&a.input, &saved_input).unwrap();
    let engine = open(root.path()).await;
    assert_eq!(engine.run(&a).await.unwrap(), original);
    assert!(engine.plan(&a).await.unwrap().completed);
    std::fs::rename(saved_input, &a.input).unwrap();
    let mut b = a.clone();
    b.operation = id("b");
    assert!(
        engine
            .plan(&b)
            .await
            .unwrap()
            .reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let selected = engine.run(&b).await.unwrap();
    assert_eq!(
        selected.dataset.groups["source"],
        original.dataset.groups["source"]
    );
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(values(&engine, &selected).await, vec![2, 2, 7]);
    let after = engine.inspect(&a.dataset).await.unwrap();
    for generation in &recovery.records {
        assert_eq!(after[&generation.key], before[&generation.key]);
    }
    for key in phase_keys(&before, &a.operation) {
        assert_eq!(after[&key], before[&key]);
    }
    let objects = &selected.dataset.groups["source"].objects;
    assert!(
        engine
            .inspect_retention(objects)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
}

#[tokio::test]
async fn active_operation_must_be_abandoned_and_its_phases_remain_independent() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let a = build(root.path(), "a", vec![3, 3, 4]);
    assert_eq!(
        engine.run_until(&a, "processing").await.unwrap_err().code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .plan_operation_retirement(&a.dataset, &id("too-early"), &a.operation, "not terminal")
            .await
            .unwrap_err()
            .code(),
        "operation_active"
    );
    let before = engine.inspect(&a.dataset).await.unwrap();
    let phases = phase_keys(&before, &a.operation);
    assert!(!phases.is_empty());
    let abandoned = engine
        .abandon(&a.dataset, &a.operation, "withdraw A selection")
        .await
        .unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &a.dataset,
            &id("a-recovery"),
            &a.operation,
            "end abandoned parent recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    drop(engine);

    let engine = open(root.path()).await;
    assert_eq!(
        engine.run(&a).await.unwrap_err().code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.plan(&a).await.unwrap_err().code(),
        "operation_abandoned"
    );
    let mut b = a.clone();
    b.operation = id("b");
    assert!(
        engine
            .plan(&b)
            .await
            .unwrap()
            .reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let selected = engine.run(&b).await.unwrap();
    assert_eq!(values(&engine, &selected).await, vec![3, 3, 4]);
    let after = engine.inspect(&a.dataset).await.unwrap();
    assert_eq!(
        after["abandoned/a"],
        serde_json::to_value(abandoned).unwrap()
    );
    for key in phases {
        assert_eq!(after[&key], before[&key]);
    }
    for generation in &recovery.records {
        assert_eq!(after[&generation.key], before[&generation.key]);
    }
}
