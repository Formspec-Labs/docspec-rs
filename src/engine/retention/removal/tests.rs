//! Real retired outputs exercise exact effects, durable retries, and fresh protection.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;

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
        max_metadata_bytes: 4 << 20,
    }
}

fn request(root: &Path, operation: &str, values: Vec<i64>) -> RunSpec {
    let input = root.join(format!("{operation}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: id("comments"),
        operation: id(operation),
        base: None,
        relationship: None,
        input: input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&input).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    }
}

async fn retire_old_promises(engine: &Engine, dataset: &Id) {
    let operation = id("old");
    let rows = engine.inspect(dataset).await.unwrap();
    let phases = rows
        .iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.operation == operation).then(|| key.clone())
        })
        .collect::<Vec<_>>();
    assert!(!phases.is_empty());
    let availability = engine
        .plan_availability_retirement(
            dataset,
            &id("old-availability"),
            &operation,
            "end old result availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let phases = engine
        .plan_phase_retirement(
            dataset,
            &id("old-phases"),
            &phases,
            "end old phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            dataset,
            &id("old-recovery"),
            &operation,
            "end old terminal recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
}

pub(super) async fn reclaimable() -> (tempfile::TempDir, Engine, ReleasePin, ReleasePin, ObjectRef)
{
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let old = engine
        .run(&request(root.path(), "old", vec![1, 1, 2]))
        .await
        .unwrap();
    let object = old.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    // A distinct source and no base ensure replacement does not depend on old bytes.
    let replacement = engine
        .run(&request(root.path(), "replacement", vec![8, 9]))
        .await
        .unwrap();
    retire_old_promises(&engine, &object.owner).await;
    let protection = engine
        .inspect_retention(std::slice::from_ref(&object))
        .await
        .unwrap();
    assert!(protection.objects[0].blocker.is_none());
    assert!(Path::new(&object.path).exists());
    (root, engine, old, replacement, object)
}

async fn plan(engine: &Engine, object: &ObjectRef, name: &str) -> RemovalPlan {
    engine
        .plan_removal(
            &object.owner,
            &id(name),
            std::slice::from_ref(object),
            "reclaim explicitly retired old output",
        )
        .await
        .unwrap()
}

async fn assert_replacement(engine: &Engine, replacement: &ReleasePin) {
    assert_eq!(
        engine.latest(&replacement.dataset.dataset).await.unwrap(),
        *replacement
    );
    let mut stream = engine.read_group(replacement, "source").await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        actual.extend(
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
    actual.sort_unstable();
    assert_eq!(actual, vec![8, 9]);
}

#[tokio::test]
async fn exact_deletion_preserves_selected_values_and_completed_cold_retries_skip_effects() {
    let (root, engine, _old, replacement, object) = reclaimable().await;
    let plan = plan(&engine, &object, "remove-old").await;
    assert!(plan.inspection.objects[0].blocker.is_none());
    assert!(!plan.registrations.is_empty());
    let removed = engine.remove_objects(&plan).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.intent.objects, vec![0]);
    assert_eq!(removed.progress.objects, vec![RemovalOutcome::Removed]);
    assert!(
        removed
            .progress
            .registrations
            .iter()
            .all(RemovalOutcome::finished)
    );
    assert_eq!(removed.progress.reserved_future_bytes, 0);
    assert_eq!(removed.confirmed_unlinked_bytes, object.bytes);
    assert_eq!(removed.reconciled_absent_bytes, 0);
    assert!(!Path::new(&object.path).exists());
    assert_replacement(&engine, &replacement).await;
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_replacement(&engine, &replacement).await;
    assert_eq!(
        engine
            .inspect_removal(&object.owner, &plan.removal)
            .await
            .unwrap(),
        removed
    );
    // A replacement at the completed path would fail a new hash, or be damaged by
    // a second unlink. The durable terminal result must bypass both effects.
    std::fs::write(&object.path, b"later unrelated bytes").unwrap();
    assert_eq!(engine.remove_objects(&plan).await.unwrap(), removed);
    assert_eq!(
        engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap(),
        removed
    );
    assert_eq!(
        std::fs::read(&object.path).unwrap(),
        b"later unrelated bytes"
    );
    assert_replacement(&engine, &replacement).await;
}

#[tokio::test]
async fn fresh_owner_protection_shrinks_a_stale_preview_without_deletion() {
    let (_root, engine, _old, replacement, object) = reclaimable().await;
    let plan = plan(&engine, &object, "stale-preview").await;
    assert!(plan.inspection.objects[0].blocker.is_none());
    let protection = engine
        .protect_objects(&id("new-consumer"), std::slice::from_ref(&object))
        .await
        .unwrap();
    let fresh = engine
        .inspect_retention(std::slice::from_ref(&object))
        .await
        .unwrap();
    assert!(fresh.objects[0].blocker.is_some());
    let result = engine.remove_objects(&plan).await.unwrap();
    assert!(result.complete);
    assert!(result.intent.objects.is_empty());
    assert!(result.intent.registrations.is_empty());
    assert_eq!(result.intent.blocked.len(), 1);
    assert_eq!(result.intent.blocked[0].object, object);
    assert!(result.intent.blocked[0].blocker.is_some());
    assert_eq!(result.confirmed_unlinked_bytes, 0);
    assert!(Path::new(&object.path).exists());
    assert_replacement(&engine, &replacement).await;
    drop(protection);
}

#[tokio::test]
async fn same_size_corruption_retains_failure_and_unavailability_until_exact_resume() {
    let (root, engine, _old, replacement, object) = reclaimable().await;
    let plan = plan(&engine, &object, "changed-target").await;
    let original = std::fs::read(&object.path).unwrap();
    let mut changed = original.clone();
    changed[0] ^= 1;
    std::fs::write(&object.path, &changed).unwrap();
    let failed = engine.remove_objects(&plan).await.unwrap();
    assert!(!failed.complete);
    assert_eq!(
        failed.progress.objects,
        vec![RemovalOutcome::Failed {
            code: "removal_changed".into()
        }]
    );
    assert!(failed.progress.reserved_future_bytes > 0);
    assert_eq!(failed.confirmed_unlinked_bytes, 0);
    assert_eq!(std::fs::read(&object.path).unwrap(), changed);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .inspect_removal(&object.owner, &plan.removal)
            .await
            .unwrap(),
        failed
    );
    let refused = engine
        .protect_objects(&id("late-consumer"), std::slice::from_ref(&object))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), "object_unavailable");
    assert_replacement(&engine, &replacement).await;
    std::fs::write(&object.path, original).unwrap();
    let resumed = engine
        .resume_removal(&object.owner, &plan.removal)
        .await
        .unwrap();
    assert!(resumed.complete);
    assert_eq!(resumed.intent, failed.intent);
    assert_eq!(resumed.progress.objects, vec![RemovalOutcome::Removed]);
    assert_eq!(resumed.progress.reserved_future_bytes, 0);
    assert_eq!(resumed.confirmed_unlinked_bytes, object.bytes);
    assert!(!Path::new(&object.path).exists());
    assert_replacement(&engine, &replacement).await;
}

#[tokio::test]
async fn already_absent_target_reports_absence_without_newly_unlinked_bytes() {
    let (root, engine, _old, replacement, object) = reclaimable().await;
    let plan = plan(&engine, &object, "missing-target").await;
    std::fs::remove_file(&object.path).unwrap();
    let absent = engine.remove_objects(&plan).await.unwrap();
    assert!(absent.complete);
    assert_eq!(absent.progress.objects, vec![RemovalOutcome::Absent]);
    assert_eq!(absent.confirmed_unlinked_bytes, 0);
    assert_eq!(absent.reconciled_absent_bytes, object.bytes);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap(),
        absent
    );
    assert_replacement(&engine, &replacement).await;
}

#[tokio::test]
async fn exact_batch_removes_retired_data_and_native_metadata_without_requiring_them_on_cold_retry()
{
    let (_root, engine, old, replacement, _) = reclaimable().await;
    let mut objects = old.dataset.groups["source"].objects.clone();
    objects.push(old.manifest.clone());
    assert!(objects.iter().any(|entry| entry.path.ends_with(".parquet")));
    assert!(objects.iter().any(|entry| entry.path.ends_with(".json")));
    let plan = engine
        .plan_removal(
            &old.dataset.dataset,
            &id("remove-old-batch"),
            &objects,
            "retired complete output closure",
        )
        .await
        .unwrap();
    assert!(
        plan.inspection
            .objects
            .iter()
            .all(|entry| entry.blocker.is_none())
    );
    let removed = engine.remove_objects(&plan).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.intent.objects.len(), plan.inspection.objects.len());
    for entry in &plan.inspection.objects {
        assert!(!Path::new(&entry.object.path).exists());
    }
    let expected_bytes: u64 = plan
        .inspection
        .objects
        .iter()
        .map(|entry| entry.object.bytes)
        .sum();
    assert_eq!(removed.confirmed_unlinked_bytes, expected_bytes);
    let workspace = engine.inner.root.clone();
    let limits = engine.inner.lifecycle.budget.limits.clone();
    drop(engine);
    let engine = Engine::open(workspace, limits).await.unwrap();
    assert_eq!(engine.remove_objects(&plan).await.unwrap(), removed);
    assert_replacement(&engine, &replacement).await;
    let inspection = engine.inspect_retention(&objects).await.unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .all(|entry| entry.blocker.is_none())
    );
}
