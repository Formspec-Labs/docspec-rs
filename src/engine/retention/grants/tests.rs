//! Exact consumer claims govern owner-grant retirement and reacquisition.
// Rust guideline compliant 2026-02-21

use super::super::owners::{OwnerProtection, ReferenceBinding, binding_key};
use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
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
        max_metadata_bytes: 16 << 20,
    }
}

async fn fixture() -> (tempfile::TempDir, Engine, ReleasePin) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("owner"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    (root, engine, release)
}

fn payload(release: &ReleasePin) -> ObjectRef {
    release.dataset.inputs["input"].clone()
}

async fn protect(engine: &Engine, consumer: &Id, object: &ObjectRef) -> ReferenceBinding {
    let protected = engine
        .protect_objects(consumer, std::slice::from_ref(object))
        .await
        .unwrap();
    let view = engine.inner.lifecycle.open(consumer).await.unwrap();
    let (_, value) = protected
        .binding(
            consumer,
            &view,
            "phase/probe/processing",
            &serde_json::Value::Null,
        )
        .unwrap();
    serde_json::from_value(value).unwrap()
}

async fn claim_phase(engine: &Engine, consumer: &Id, object: &ObjectRef, key: &str) {
    let protected = engine
        .protect_objects(consumer, std::slice::from_ref(object))
        .await
        .unwrap();
    let phase = PhaseResult {
        operation: id("fixture"),
        phase: "processing".into(),
        request_digest: "0".repeat(64),
        eligibility: 0,
        outputs: vec![object.clone()],
        table: None,
        rows: 0,
        changes: None,
        coverage: None,
        metrics: WriteMetrics::default(),
        dependencies: Some(PhaseDependencies::default()),
    };
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(consumer).await.unwrap();
    let claim = record(key, &phase).unwrap();
    let binding = protected.binding(consumer, &view, key, &claim.1).unwrap();
    lifecycle.commit(&view, vec![claim, binding]).await.unwrap();
}

async fn retire_phase(engine: &Engine, consumer: &Id, key: &str, retirement: &str) {
    let plan = engine
        .plan_phase_retirement(consumer, &id(retirement), &[key.into()], "end recovery")
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
}

#[tokio::test]
async fn a_new_claim_blocks_stale_execution_until_every_exact_claim_retires() {
    let (_root, engine, release) = fixture().await;
    let object = payload(&release);
    let consumer = id("consumer");
    let binding = protect(&engine, &consumer, &object).await;
    let reference = &binding.protections[0];
    let keys = vec![reference.token.clone()];
    let stale = engine
        .plan_grant_retirement(&object.owner, &id("stale"), &keys, "failed handoff")
        .await
        .unwrap();
    let first = "phase/first/processing";
    let second = "phase/second/processing";
    claim_phase(&engine, &consumer, &object, first).await;
    claim_phase(&engine, &consumer, &object, second).await;
    assert!(engine.retire_grants(&stale).await.is_err());
    assert!(
        engine
            .plan_grant_retirement(&object.owner, &id("blocked"), &keys, "end grant")
            .await
            .is_err()
    );
    retire_phase(&engine, &consumer, first, "first").await;
    assert!(
        engine
            .plan_grant_retirement(&object.owner, &id("still-blocked"), &keys, "end grant")
            .await
            .is_err()
    );
    retire_phase(&engine, &consumer, second, "second").await;
    let plan = engine
        .plan_grant_retirement(&object.owner, &id("released"), &keys, "last claim ended")
        .await
        .unwrap();
    assert_eq!(plan.records[0].revision, reference.revision);
    engine.retire_grants(&plan).await.unwrap();
    assert_eq!(std::fs::metadata(&object.path).unwrap().len(), object.bytes);
}

#[tokio::test]
async fn lost_reply_and_concurrent_retry_preserve_reacquired_and_unrelated_grants() {
    let (root, engine, release) = fixture().await;
    let object = payload(&release);
    let consumer = id("consumer");
    let original_binding = protect(&engine, &consumer, &object).await;
    let original = original_binding.protections[0].clone();
    engine
        .check_protection_references(&original_binding, std::slice::from_ref(&object))
        .await
        .unwrap();
    let other = protect(&engine, &id("other-consumer"), &object)
        .await
        .protections
        .remove(0);
    let distinct = protect(&engine, &consumer, &release.manifest)
        .await
        .protections
        .remove(0);
    let plan = engine
        .plan_grant_retirement(
            &object.owner,
            &id("retire"),
            std::slice::from_ref(&original.token),
            "unacknowledged handoff",
        )
        .await
        .unwrap();
    let before = engine.inner.lifecycle.open(&object.owner).await.unwrap();
    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (first, second) = tokio::join!(engine.retire_grants(&plan), engine.retire_grants(&plan));
    let receipt = first.unwrap();
    assert_eq!(second.unwrap(), receipt);
    assert_eq!(receipt.revision, before.revision + 1);
    assert_eq!(
        engine
            .check_protection_references(&original_binding, std::slice::from_ref(&object))
            .await
            .unwrap_err()
            .code(),
        "grant_retired"
    );
    let replacement = protect(&engine, &consumer, &object)
        .await
        .protections
        .remove(0);
    assert_eq!(replacement.token, original.token);
    assert!(replacement.revision > receipt.revision);
    assert_eq!(
        engine
            .check_protection_references(&original_binding, std::slice::from_ref(&object))
            .await
            .unwrap_err()
            .code(),
        "invalid_owner_protection"
    );
    let view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
    assert_eq!(engine.retire_grants(&plan).await.unwrap(), receipt);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&object.owner)
            .await
            .unwrap()
            .revision,
        view.revision
    );
    for reference in [&replacement, &other, &distinct] {
        let grant: OwnerProtection = engine
            .inner
            .lifecycle
            .get(&view, &reference.token)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(grant.revision, reference.revision);
    }
    claim_phase(&engine, &consumer, &object, "phase/new/processing").await;
    assert!(
        engine
            .plan_grant_retirement(
                &object.owner,
                &id("new-blocked"),
                &[replacement.token],
                "new claim is active"
            )
            .await
            .is_err()
    );
    drop(engine);
    let reopened = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(reopened.retire_grants(&plan).await.unwrap(), receipt);
}

#[tokio::test]
async fn orphan_grant_planning_does_not_create_a_consumer_lifecycle() {
    let (_root, engine, release) = fixture().await;
    let consumer = id("handoff-never-started");
    let protected = engine
        .protect_releases(&consumer, std::slice::from_ref(&release))
        .await
        .unwrap();
    // Extract the acquired token without opening the consumer's lifecycle.
    let owner = &release.dataset.dataset;
    let view = engine.inner.lifecycle.open(owner).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (key, _, _) = rows.prefix("owner-protection/").next().unwrap();
    let key = key.to_owned();
    let ident = crate::catalog::lifecycle_ident(&consumer);
    assert!(
        !engine
            .inner
            .lifecycle
            .catalog
            .table_exists(&ident)
            .await
            .unwrap()
    );
    let plan = engine
        .plan_grant_retirement(owner, &id("orphan"), &[key], "handoff failed")
        .await
        .unwrap();
    assert!(
        !engine
            .inner
            .lifecycle
            .catalog
            .table_exists(&ident)
            .await
            .unwrap()
    );
    engine.retire_grants(&plan).await.unwrap();
    assert!(
        !engine
            .inner
            .lifecycle
            .catalog
            .table_exists(&ident)
            .await
            .unwrap()
    );
    drop(protected);
}

#[tokio::test]
async fn corrupt_binding_revision_cannot_authorize_retirement() {
    let (_root, engine, release) = fixture().await;
    let object = payload(&release);
    let consumer = id("consumer");
    let key = "phase/first/processing";
    claim_phase(&engine, &consumer, &object, key).await;
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&consumer).await.unwrap();
    let mut binding: ReferenceBinding = lifecycle
        .get(&view, &binding_key(key))
        .await
        .unwrap()
        .unwrap();
    let reference = binding.protections[0].clone();
    binding.protections[0].revision += 1;
    binding.revision = view.revision + 1;
    let phase: PhaseResult = lifecycle.get(&view, key).await.unwrap().unwrap();
    lifecycle
        .commit(
            &view,
            vec![
                record(key, phase).unwrap(),
                record(binding_key(key), binding).unwrap(),
            ],
        )
        .await
        .unwrap();
    let before = lifecycle.open(&object.owner).await.unwrap();
    assert!(
        engine
            .plan_grant_retirement(
                &object.owner,
                &id("corrupt"),
                &[reference.token],
                "inconsistent reference"
            )
            .await
            .is_err()
    );
    assert_eq!(
        lifecycle.open(&object.owner).await.unwrap().revision,
        before.revision
    );
}

#[tokio::test]
async fn sealed_generation_binding_blocks_retirement_of_its_exact_owner_grants() {
    let (root, engine, _original) = fixture().await;
    let input = root.path().join("input.parquet");
    let family = id("family");
    let generation = id("generation");
    let mut members = BTreeMap::new();
    for name in ["comments", "dockets"] {
        let release = engine
            .run(&RunSpec {
                dataset: id(name),
                operation: id("initial"),
                base: None,
                relationship: Some(SourceRelationship::Coupled {
                    family: family.clone(),
                    generation: generation.clone(),
                }),
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
            })
            .await
            .unwrap();
        members.insert(release.dataset.dataset.clone(), release.version_ref());
    }
    let seal = GenerationSeal {
        family,
        generation,
        members,
    };
    engine.seal_generation(&seal).await.unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&seal.family).await.unwrap();
    let key = format!("generation/{}", seal.generation);
    let binding: ReferenceBinding = lifecycle
        .get(&view, &binding_key(&key))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.protections.len(), seal.members.len());
    engine.check_reference_binding(&view, &key).await.unwrap();
    for reference in &binding.protections {
        let before = lifecycle.open(&reference.owner).await.unwrap();
        let error = engine
            .plan_grant_retirement(
                &reference.owner,
                &id("sealed"),
                std::slice::from_ref(&reference.token),
                "family still retains its seal",
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "grant_in_use");
        assert_eq!(
            lifecycle.open(&reference.owner).await.unwrap().revision,
            before.revision
        );
    }
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
}

#[tokio::test]
async fn oversized_grant_plan_refuses_before_metadata_mutation() {
    let (_root, engine, release) = fixture().await;
    let before = engine
        .inner
        .lifecycle
        .open(&release.dataset.dataset)
        .await
        .unwrap();
    let reason = "x".repeat(limits().max_metadata_bytes + 1);
    let error = engine
        .plan_grant_retirement(
            &release.dataset.dataset,
            &id("oversized"),
            &[format!("owner-protection/{}", "0".repeat(64))],
            &reason,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "metadata_capacity");
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&release.dataset.dataset)
            .await
            .unwrap()
            .revision,
        before.revision
    );
}

async fn retire_operation_promises(engine: &Engine, dataset: &Id, operation: &Id, prefix: &str) {
    let records = engine.inspect(dataset).await.unwrap();
    let phases = records
        .iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.operation == *operation).then(|| key.clone())
        })
        .collect::<Vec<_>>();
    let plan = engine
        .plan_availability_retirement(
            dataset,
            &id(&format!("{prefix}-availability")),
            operation,
            "end old result availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&plan).await.unwrap();
    let plan = engine
        .plan_phase_retirement(
            dataset,
            &id(&format!("{prefix}-phases")),
            &phases,
            "end old phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let plan = engine
        .plan_operation_retirement(
            dataset,
            &id(&format!("{prefix}-operation")),
            operation,
            "end old operation recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&plan).await.unwrap();
}

#[tokio::test]
async fn retiring_last_consumer_and_owner_promises_releases_the_foreign_object() {
    let (root, engine, original) = fixture().await;
    let object = payload(&original);
    let consumer = id("consumer");
    let consumed = engine
        .run(&RunSpec {
            dataset: consumer.clone(),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: object.path.clone(),
            expected_digest: object.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let owner_view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
    let owner_rows = engine.inner.lifecycle.rows(&owner_view).await.unwrap();
    let grants = owner_rows
        .prefix("owner-protection/")
        .filter_map(|(key, _, json)| {
            let grant: OwnerProtection = serde_json::from_str(json).unwrap();
            (grant.consumer == consumer).then(|| key.to_owned())
        })
        .collect::<Vec<_>>();
    assert!(!grants.is_empty());
    assert!(
        engine
            .plan_grant_retirement(
                &object.owner,
                &id("active-consumer"),
                &grants,
                "consumer still retains result and recovery"
            )
            .await
            .is_err()
    );
    // Independent replacements make both original results eligible without
    // introducing a new dependency on the original managed input.
    let input = root.path().join("replacement.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![8, 9]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for dataset in [&object.owner, &consumer] {
        engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: id("replacement"),
                base: None,
                relationship: None,
                input: input.to_string_lossy().into_owned(),
                expected_digest: storage::digest(&input).unwrap().0,
                sql: "SELECT id + 10 AS id FROM input".into(),
                definition: "v1".into(),
            })
            .await
            .unwrap();
    }
    retire_operation_promises(&engine, &object.owner, &id("initial"), "owner").await;
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&object))
        .await
        .unwrap();
    assert!(inspection.objects[0].blocker.is_some());
    retire_operation_promises(&engine, &consumer, &id("initial"), "consumer").await;
    let plan = engine
        .plan_grant_retirement(
            &object.owner,
            &id("last-grants"),
            &grants,
            "all exact consumer claims ended",
        )
        .await
        .unwrap();
    engine.retire_grants(&plan).await.unwrap();
    drop(owner_rows);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.retire_grants(&plan).await.unwrap().plan, plan);
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&object))
        .await
        .unwrap();
    assert!(inspection.objects[0].blocker.is_none());
    assert_eq!(std::fs::metadata(&object.path).unwrap().len(), object.bytes);
    // Independently selected replacements still expose the exact expected values.
    use futures::TryStreamExt;
    for dataset in [&object.owner, &consumer] {
        let selected = engine.latest(dataset).await.unwrap();
        let mut stream = engine.read_group(&selected, "source").await.unwrap();
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
        assert_eq!(actual, vec![18, 19]);
    }
    assert_eq!(consumed.dataset.dataset, consumer);
}
