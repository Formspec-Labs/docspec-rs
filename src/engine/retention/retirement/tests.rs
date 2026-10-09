//! Corrupt native records cannot silently retire unrelated recovery promises.

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
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        ..Limits::default()
    }
}

#[tokio::test]
async fn lost_native_reply_and_concurrent_retry_record_one_retirement() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let dataset = id("comments");
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&dataset).await.unwrap();
    let view = lifecycle
        .commit(&view, vec![record("phase/first/processing", ()).unwrap()])
        .await
        .unwrap();
    let plan = engine
        .plan_phase_retirement(
            &dataset,
            &id("retire"),
            &["phase/first/processing".into()],
            "retire first",
        )
        .await
        .unwrap();
    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (first, second) = tokio::join!(engine.retire_phases(&plan), engine.retire_phases(&plan));
    let first = first.unwrap();
    assert_eq!(second.unwrap(), first);
    assert_eq!(first.revision, view.revision + 1);
    let after = lifecycle.open(&dataset).await.unwrap();
    assert_eq!(
        after.revision, first.revision,
        "reconciliation does not repeat the commit"
    );
    assert!(
        engine
            .active_completion::<serde_json::Value>(&after, "phase/first/processing")
            .await
            .unwrap()
            .is_none()
    );
    drop(engine);
    let reopened = Engine::open(root.path(), limits()).await.unwrap();
    assert_eq!(reopened.retire_phases(&plan).await.unwrap(), first);
}

#[tokio::test]
async fn receipt_membership_and_marker_presence_are_required() {
    for extra_marker in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let dataset = id("comments");
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&dataset).await.unwrap();
        let first = "phase/first/processing".to_owned();
        let second = "phase/second/processing".to_owned();
        let view = lifecycle
            .commit(
                &view,
                vec![record(&first, ()).unwrap(), record(&second, ()).unwrap()],
            )
            .await
            .unwrap();
        let plan = engine
            .plan_phase_retirement(
                &dataset,
                &id("retire"),
                std::slice::from_ref(&first),
                "retire only first",
            )
            .await
            .unwrap();
        let receipt = Retirement {
            plan: plan.clone(),
            revision: view.revision + 1,
        };
        let mut records = vec![record("retirement/retire", &receipt).unwrap()];
        if extra_marker {
            // The native transaction is atomic, but its second marker was never
            // authorized by the retained retirement decision.
            for key in [first, second.clone()] {
                let phase = RetainedGeneration {
                    key,
                    revision: view.revision,
                };
                records.push(
                    record(
                        marker_key(&phase),
                        RetiredRecord {
                            dataset: dataset.clone(),
                            table_uuid: plan.table_uuid.clone(),
                            record: phase,
                            retirement: plan.retirement.clone(),
                        },
                    )
                    .unwrap(),
                );
            }
        }
        let view = lifecycle.commit(&view, records).await.unwrap();
        if extra_marker {
            assert_eq!(
                engine
                    .active_completion::<serde_json::Value>(&view, &second)
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_retirement"
            );
        } else {
            assert_eq!(
                engine.retire_phases(&plan).await.unwrap_err().code(),
                "invalid_retirement"
            );
        }
        assert_eq!(
            engine.inspect_retention(&[]).await.unwrap_err().code(),
            "invalid_retirement"
        );
    }
}

#[tokio::test]
async fn legacy_processing_cannot_adopt_a_replacement_admission() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let spec = RunSpec {
        dataset: id("comments"),
        operation: id("original"),
        base: None,
        relationship: None,
        input: input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&input).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let records = lifecycle.records(&view).await.unwrap();
    let (processing_key, value) = records
        .iter()
        .find(|(key, _)| key.ends_with("/processing"))
        .unwrap();
    let mut legacy: PhaseResult = serde_json::from_value(value.clone()).unwrap();
    legacy.dependencies = None;
    // Install a genuinely unknown legacy generation. Retiring the bound
    // predecessor prevents its protection from qualifying this replacement.
    let processing_plan = engine
        .plan_phase_retirement(
            &spec.dataset,
            &id("prior-processing-format"),
            std::slice::from_ref(processing_key),
            "replace the fixture with legacy dependency metadata",
        )
        .await
        .unwrap();
    engine.retire_phases(&processing_plan).await.unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(&view, vec![record(processing_key, &legacy).unwrap()])
        .await
        .unwrap();
    let admission_key = records
        .keys()
        .find(|key| key.ends_with("/admission"))
        .unwrap();
    let plan = engine
        .plan_phase_retirement(
            &spec.dataset,
            &id("old-admission"),
            std::slice::from_ref(admission_key),
            "retire admission",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let replacement = RunSpec {
        operation: id("replacement"),
        sql: "SELECT id + 1 AS id FROM input".into(),
        ..spec.clone()
    };
    assert_eq!(
        engine
            .run_until(&replacement, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "phase_dependencies_unknown"
    );
    assert_eq!(
        engine.inspect(&spec.dataset).await.unwrap()[processing_key],
        serde_json::to_value(legacy).unwrap()
    );
}
