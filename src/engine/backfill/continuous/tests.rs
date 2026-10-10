//! Native fixtures for parent state checked at the exact fence acquisition boundary.
use super::*;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn fixture(root: &Path) -> (Engine, ContinuousBackfillSpec) {
    let path = root.join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("é🎉"), None])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.join("workspace"),
        Limits {
            compute_threads: 2,
            memory_bytes: 256 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 4 << 20,
        },
    )
    .await
    .unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let spec = ContinuousBackfillSpec {
        backfill: BackfillSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("parent"),
            base: base.id,
            definition: "v1".into(),
            group: id("length"),
            group_definition: "length-v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "text_length".into(),
                expression: "length(text)".into(),
            }],
        },
        optimistic_rounds: 1,
        // The test interleaves two workers directly without relying on short leases.
        fence_millis: 30_000,
    };
    (engine, spec)
}

async fn intent(engine: &Engine, spec: &ContinuousBackfillSpec) -> Intent {
    engine
        .continuous_intent(
            spec,
            storage::json_digest(spec).unwrap().to_hex().to_string(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn optimistic_same_parent_worker_adopts_live_fence_and_closes_it_with_selection() {
    let root = tempfile::tempdir().unwrap();
    let (engine, spec) = fixture(root.path()).await;
    assert_eq!(
        engine
            .continuous_backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let intent = intent(&engine, &spec).await;
    // Worker A fences the admitted parent; worker B resumes through the public API.
    let fence = engine.acquire_continuous_fence(&intent).await.unwrap();
    let release = engine.continuous_backfill(&spec).await.unwrap();
    let view = engine
        .inner
        .lifecycle
        .open(&spec.backfill.dataset)
        .await
        .unwrap();
    let completed: Completed = engine
        .inner
        .lifecycle
        .get(&view, &completed_key(&spec.backfill.operation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.fence.as_deref(), Some(fence.token.as_str()));
    let closed: SourceFence = engine
        .inner
        .lifecycle
        .get(&view, "source-fence")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed.token, fence.token);
    assert!(closed.closed);
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let revision = rows.revision(&format!("result/{}", release.id)).unwrap();
    assert_eq!(rows.revision("authority"), Some(revision));
    assert_eq!(
        rows.revision(&completed_key(&spec.backfill.operation)),
        Some(revision)
    );
    assert_eq!(rows.revision("source-fence"), Some(revision));
    assert_eq!(engine.continuous_backfill(&spec).await.unwrap(), release);
}

#[tokio::test]
async fn parent_abandoned_after_intent_capture_cannot_acquire_a_fence() {
    let root = tempfile::tempdir().unwrap();
    let (engine, spec) = fixture(root.path()).await;
    assert_eq!(
        engine
            .continuous_backfill_until(&spec, "continuous-admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let intent = intent(&engine, &spec).await;
    engine
        .abandon(
            &spec.backfill.dataset,
            &spec.backfill.operation,
            "withdraw parent before fence",
        )
        .await
        .unwrap();
    let before = engine.inspect(&spec.backfill.dataset).await.unwrap();
    assert!(!before.contains_key("source-fence"));
    assert_eq!(
        engine
            .acquire_continuous_fence(&intent)
            .await
            .unwrap_err()
            .code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.inspect(&spec.backfill.dataset).await.unwrap(),
        before
    );
}

#[tokio::test]
async fn completed_parent_cannot_reacquire_a_fence_from_its_captured_intent() {
    let root = tempfile::tempdir().unwrap();
    let (engine, spec) = fixture(root.path()).await;
    assert_eq!(
        engine
            .continuous_backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let intent = intent(&engine, &spec).await;
    let release = engine.continuous_backfill(&spec).await.unwrap();
    let before = engine.inspect(&spec.backfill.dataset).await.unwrap();
    assert!(!before.contains_key("source-fence"));
    assert_eq!(
        engine
            .acquire_continuous_fence(&intent)
            .await
            .unwrap_err()
            .code(),
        "continuous_changed"
    );
    assert_eq!(
        engine.inspect(&spec.backfill.dataset).await.unwrap(),
        before
    );
    assert_eq!(
        engine.latest(&spec.backfill.dataset).await.unwrap(),
        release
    );
}

#[tokio::test]
async fn corrupted_child_request_association_refuses_before_and_after_selection() {
    for phase in ["readback", "selection"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, spec) = fixture(root.path()).await;
        assert_eq!(
            engine
                .continuous_backfill_until(&spec, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let original = engine.latest(&spec.backfill.dataset).await.unwrap();
        let view = engine
            .inner
            .lifecycle
            .open(&spec.backfill.dataset)
            .await
            .unwrap();
        let child: Child = engine
            .inner
            .lifecycle
            .get(&view, &state_key(&spec.backfill.operation))
            .await
            .unwrap()
            .unwrap();
        let key = format!("request/{}", child.child);
        let OperationRequest::Backfill(mut altered) = engine
            .inner
            .lifecycle
            .get(&view, &key)
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("controlled child must retain its Backfill request");
        };
        // Model catalog corruption: retained ownership and admission still identify
        // the original request, while its recipe now claims different computation.
        altered.fields[0].expression = "length(text) + 1".into();
        engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![record(&key, OperationRequest::Backfill(altered)).unwrap()],
            )
            .await
            .unwrap();
        let before = engine.inspect(&spec.backfill.dataset).await.unwrap();
        assert_eq!(
            engine
                .plan_continuous_backfill(&spec)
                .await
                .unwrap_err()
                .code(),
            "invalid_continuous_backfill"
        );
        assert_eq!(
            engine.continuous_backfill(&spec).await.unwrap_err().code(),
            "invalid_continuous_backfill"
        );
        assert_eq!(
            engine.latest(&spec.backfill.dataset).await.unwrap(),
            original
        );
        assert_eq!(
            engine.inspect(&spec.backfill.dataset).await.unwrap(),
            before
        );
        assert_eq!(
            before.contains_key(&completed_key(&spec.backfill.operation)),
            phase == "selection"
        );
    }
}

#[tokio::test]
async fn first_capture_race_retains_protected_admission_without_computing_obsolete_work() {
    for selected in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, spec) = fixture(root.path()).await;
        let intent = intent(&engine, &spec).await;
        let (request, attempt) = engine.continuous_attempt(&intent).await.unwrap();
        let source = root.path().join("input.parquet");
        let advance = RunSpec {
            dataset: spec.backfill.dataset.clone(),
            operation: id("advance"),
            base: Some(spec.backfill.base.clone()),
            relationship: None,
            input: source.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&source).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        };
        if selected {
            engine.run(&advance).await.unwrap();
        } else {
            assert_eq!(
                engine
                    .run_until(&advance, "admission")
                    .await
                    .unwrap_err()
                    .code(),
                "injected_failure"
            );
        }
        // Force source movement between capture and the first protected admission.
        // The admission-only case changes the boundary without changing the head.
        assert_eq!(
            engine
                .prepare_backfill(&request, None, Some(&attempt))
                .await
                .unwrap_err()
                .code(),
            "stale_source_boundary"
        );
        let view = engine
            .inner
            .lifecycle
            .open(&spec.backfill.dataset)
            .await
            .unwrap();
        let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
        let revision = rows
            .revision(&intent_key(&spec.backfill.operation))
            .unwrap();
        for key in [
            child_key(&request.operation),
            state_key(&spec.backfill.operation),
            format!("operation/{}", request.operation),
            format!("request/{}", request.operation),
            format!("request-dependencies/{}", request.operation),
            format!("backfill/{}", request.operation),
        ] {
            assert_eq!(rows.revision(&key), Some(revision), "{key}");
        }
        let dependencies = engine
            .admitted_dependencies(&view, &request.operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dependencies.base, Some(attempt.child.capture.clone()));
        let before = engine.inspect(&spec.backfill.dataset).await.unwrap();
        assert!(!before.keys().any(|key| key.starts_with("phase/backfill/")
            || key.starts_with("backfill-prepared/")
            || key.starts_with("backfill-validation/")
            || key.starts_with("backfill-readback/")));
        assert!(!before.contains_key(&format!("result/{}", request.operation)));
        let fence = engine.acquire_continuous_fence(&intent).await.unwrap();
        let advanced = engine.run(&advance).await.unwrap();
        let release = engine.continuous_backfill(&spec).await.unwrap();
        assert_ne!(release.id, request.operation);
        assert_eq!(
            release.dataset.groups["source"],
            advanced.dataset.groups["source"]
        );
        let after = engine.inspect(&spec.backfill.dataset).await.unwrap();
        assert!(after.contains_key(&format!("abandoned/{}", request.operation)));
        assert!(!after.contains_key(&format!("result/{}", request.operation)));
        assert_eq!(after["source-fence"]["token"], fence.token);
        assert_eq!(after["source-fence"]["closed"], true);
        assert_eq!(
            after[&completed_key(&spec.backfill.operation)]["selected"]["version"],
            release.id.as_str()
        );
        let mut stream = engine.read_group(&release, "length").await.unwrap();
        let mut values = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let array = arrow::compute::cast(batch.column(2), &DataType::Int64).unwrap();
            values.extend(array.as_any().downcast_ref::<Int64Array>().unwrap().iter());
        }
        values.sort();
        assert_eq!(values, vec![None, Some(2)]);
    }
}
