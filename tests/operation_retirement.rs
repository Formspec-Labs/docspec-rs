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

#[tokio::test]
async fn compact_lifecycle_reclaims_retired_incomplete_operation_and_protections() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path()).await;
    let a = build(root.path(), "a", vec![1, 2, 3]);
    let initial_release = engine.run(&a).await.unwrap();

    // 1. Run incomplete operation b up to injected failure
    let b = build(root.path(), "b", vec![4, 5, 6]);
    assert_eq!(
        engine.run_until(&b, "processing").await.unwrap_err().code(),
        "injected_failure"
    );
    let before_abandon = engine.inspect(&b.dataset).await.unwrap();
    let b_phases = phase_keys(&before_abandon, &b.operation);
    assert!(
        !b_phases.is_empty(),
        "incomplete operation should have written phase records"
    );
    let b_candidate_attempts: Vec<String> = before_abandon
        .keys()
        .filter(|k| k.starts_with(&format!("candidate-attempt/{}/", b.operation)))
        .cloned()
        .collect();
    assert!(
        !b_candidate_attempts.is_empty(),
        "incomplete operation b should have registered candidate-attempt records"
    );
    let b_dep_key = format!("request-dependencies/{}", b.operation);
    assert!(
        before_abandon.contains_key(&b_dep_key),
        "incomplete operation b should have registered request-dependencies"
    );
    let b_protections: Vec<String> = before_abandon
        .iter()
        .filter_map(|(k, v)| {
            if !k.starts_with("protection/") {
                return None;
            }
            let purpose = v.get("purpose")?.as_str()?;
            if purpose.starts_with(&format!("{}/", b.operation)) {
                Some(k.clone())
            } else {
                None
            }
        })
        .collect();
    assert!(
        !b_protections.is_empty(),
        "incomplete operation b should have registered protection records with purpose matching operation b"
    );
    assert!(before_abandon.contains_key(&format!("operation/{}", b.operation)));
    assert!(before_abandon.contains_key(&format!("request/{}", b.operation)));

    // 2. Abandon operation b and retire its recovery
    let _abandoned = engine
        .abandon(&b.dataset, &b.operation, "abandon failed operation b")
        .await
        .unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &b.dataset,
            &id("b-recovery"),
            &b.operation,
            "end abandoned b recovery",
        )
        .await
        .unwrap();
    for attempt in &b_candidate_attempts {
        assert!(
            recovery.records.iter().any(|r| r.key == *attempt),
            "recovery plan must include candidate-attempt {attempt}"
        );
    }
    assert!(
        recovery.records.iter().any(|r| r.key == b_dep_key),
        "recovery plan must include request-dependencies record"
    );
    for prot in &b_protections {
        assert!(
            recovery.records.iter().any(|r| r.key == *prot),
            "recovery plan must include protection record {prot}"
        );
    }
    engine.retire_operation(&recovery).await.unwrap();

    // 3. Compact lifecycle
    let stats = engine.compact_lifecycle(&b.dataset).await.unwrap();
    assert_eq!(stats.revisions_after, 1);

    // 4. Assert obsolete attempts, dependencies, protections, and phases are reclaimed
    // while core recovery anchors and receipt records remain intact
    let after_compaction = engine.inspect(&b.dataset).await.unwrap();

    // 4a. Core recovery anchors and receipts must remain preserved
    assert!(
        after_compaction.contains_key(&format!("operation/{}", b.operation)),
        "operation/b must remain preserved in snapshot for recovery receipt validation"
    );
    assert!(
        after_compaction.contains_key(&format!("request/{}", b.operation)),
        "request/b must remain preserved in snapshot for recovery receipt validation"
    );
    assert!(
        after_compaction.contains_key("retirement/b-recovery"),
        "retirement receipt must remain in lifecycle snapshot"
    );
    let retired_markers: Vec<_> = after_compaction
        .keys()
        .filter(|k| k.starts_with("retired/"))
        .collect();
    assert!(
        !retired_markers.is_empty(),
        "retired record markers (retired/...) must remain preserved in lifecycle snapshot after compaction"
    );
    assert_eq!(
        retired_markers.len(),
        recovery.records.len(),
        "every retired record generation should retain its tombstone marker in the compacted lifecycle snapshot"
    );

    // 4b. Obsolete attempts must be reclaimed
    for prefix in [
        format!("candidate-attempt/{}/", b.operation),
        format!("source-attempt/{}/", b.operation),
        format!("release-attempt/{}/", b.operation),
    ] {
        let remaining_attempts: Vec<_> = after_compaction
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .collect();
        assert!(
            remaining_attempts.is_empty(),
            "obsolete attempt records for prefix '{prefix}' must be reclaimed by compact_lifecycle, but found: {remaining_attempts:?}"
        );
    }

    // 4c. Obsolete dependency records must be reclaimed
    assert!(
        !after_compaction.contains_key(&b_dep_key),
        "obsolete {b_dep_key} must be reclaimed by compact_lifecycle"
    );

    // 4d. Obsolete protections associated with the retired operation must be reclaimed
    for prot_key in &b_protections {
        assert!(
            !after_compaction.contains_key(prot_key),
            "obsolete protection record {prot_key} must be reclaimed by compact_lifecycle"
        );
    }
    let lingering_b_protections: Vec<_> = after_compaction
        .iter()
        .filter_map(|(k, v)| {
            if !k.starts_with("protection/") {
                return None;
            }
            let purpose = v.get("purpose")?.as_str()?;
            if purpose.starts_with(&format!("{}/", b.operation)) {
                Some((k.clone(), purpose.to_string()))
            } else {
                None
            }
        })
        .collect();
    assert!(
        lingering_b_protections.is_empty(),
        "no protections associated with operation b should remain after compaction, but found: {lingering_b_protections:?}"
    );

    // 4e. Obsolete phase records must be reclaimed
    for key in &b_phases {
        assert!(
            !after_compaction.contains_key(key),
            "obsolete phase record {key} must be reclaimed by compact_lifecycle"
        );
    }

    // 5. Verify the active release remains verified and retention inspection is clean
    drop(engine);
    let reopened = open(root.path()).await;
    assert_eq!(reopened.latest(&a.dataset).await.unwrap(), initial_release);
    assert_eq!(values(&reopened, &initial_release).await, vec![1, 2, 3]);
    assert_eq!(
        reopened.run(&b).await.unwrap_err().code(),
        "operation_abandoned"
    );
    assert_eq!(
        reopened.plan(&b).await.unwrap_err().code(),
        "operation_abandoned"
    );
    let reopened_inspect = reopened.inspect(&b.dataset).await.unwrap();
    assert!(reopened_inspect.contains_key(&format!("operation/{}", b.operation)));
    assert!(reopened_inspect.contains_key(&format!("request/{}", b.operation)));
    assert!(reopened_inspect.contains_key("retirement/b-recovery"));
    assert!(!reopened_inspect.contains_key(&b_dep_key));
    assert!(
        !reopened_inspect
            .keys()
            .any(|k| k.starts_with(&format!("candidate-attempt/{}/", b.operation)))
    );
    for prot_key in &b_protections {
        assert!(!reopened_inspect.contains_key(prot_key));
    }
}
