//! Actual native exports preserve creation custody through failure and losing work.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::export::ExportRows;

fn id(name: &str) -> Id {
    Id::new(name).unwrap()
}
async fn fixture() -> (tempfile::TempDir, Engine, ExportSpec) {
    let root = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let input = root.path().join("input.parquet");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![
            Some(1),
            None,
            Some(1),
        ]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("host"),
        Limits {
            compute_threads: 1,
            memory_bytes: 256 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 1 << 20,
            max_metadata_bytes: 1 << 20,
        },
    )
    .await
    .unwrap();
    let source = engine
        .run(&RunSpec {
            dataset: id("data"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let spec = ExportSpec {
        dataset: id("data"),
        operation: id("export"),
        source: source.version_ref(),
        channel: id("download"),
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["value".into()],
        },
    };
    (root, engine, spec)
}
async fn files(engine: &Engine, spec: &ExportSpec) -> Vec<HostFile> {
    engine
        .inspect(&spec.dataset)
        .await
        .unwrap()
        .into_iter()
        .filter(|(key, _)| key.starts_with(&format!("export-host-file/{}/", spec.operation)))
        .map(|(_, value)| serde_json::from_value(value).unwrap())
        .collect()
}
async fn retire(engine: &Engine, spec: &ExportSpec) -> Retirement {
    let plan = engine
        .plan_export_retirement(
            &spec.dataset,
            &id("end-export"),
            &spec.operation,
            "discard unfinished export recovery",
        )
        .await
        .unwrap();
    engine.retire_export(&plan).await.unwrap()
}

#[tokio::test]
async fn partial_host_bytes_reclaim_after_exact_terminal_retirement_and_intent_retry() {
    let (root, engine, spec) = fixture().await;
    assert_eq!(
        engine
            .export_until(&spec, "host-payload")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let created = files(&engine, &spec).await;
    assert_eq!(created.len(), 1);
    let file = &created[0];
    let bytes = std::fs::metadata(&file.path).unwrap().len();
    assert!(bytes > 4);
    let before = engine.inspect(&spec.dataset).await.unwrap();
    assert!(!before.contains_key(&format!(
        "export-host-closed/{}/{}/{}",
        file.operation, file.operation_revision, file.id
    )));
    assert_eq!(
        engine
            .plan_export_file_removal(
                &spec.dataset,
                &id("remove"),
                &spec.operation,
                &file.id,
                "partial cleanup"
            )
            .await
            .unwrap_err()
            .code(),
        "export_stage_pending"
    );
    let retired = retire(&engine, &spec).await;
    assert!(
        retired
            .plan
            .records
            .iter()
            .any(|record| record.key == key(file))
    );
    assert_eq!(
        engine.export(&spec).await.unwrap_err().code(),
        "export_retired"
    );
    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    let plan = engine
        .plan_export_file_removal(
            &spec.dataset,
            &id("remove"),
            &spec.operation,
            &file.id,
            "partial cleanup",
        )
        .await
        .unwrap();
    assert_eq!(plan.inspection.unmatched_bytes, bytes);
    assert_eq!(
        engine
            .removal_until(&plan, "removal-intent")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let removed = engine.remove_objects(&plan).await.unwrap();
    assert_eq!(removed.confirmed_unlinked_bytes, bytes);
    assert!(!Path::new(&file.path).exists());
    assert_eq!(engine.remove_objects(&plan).await.unwrap(), removed);
    let after = engine.inspect(&spec.dataset).await.unwrap();
    for (key, value) in before {
        assert_eq!(after[&key], value);
    }
    let workspace = root.path().join("host");
    drop(engine);
    let cold = Engine::open(
        workspace,
        Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        cold.inspect_removal(&spec.dataset, &id("remove"))
            .await
            .unwrap(),
        removed
    );
}

#[tokio::test]
async fn closed_losing_file_reclaims_without_ending_immutable_winning_pin() {
    let (_root, engine, spec) = fixture().await;
    assert_eq!(
        engine
            .export_until(&spec, "materialization")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let operation: ExportOperation = engine
        .inner
        .lifecycle
        .get(&view, &format!("export-operation/{}", spec.operation))
        .await
        .unwrap()
        .unwrap();
    // The actual common native writer produces a second exact same-operation
    // completion, as a concurrent loser can do before first completion wins.
    let loser = engine
        .materialize_export(&operation, None, None)
        .await
        .unwrap();
    let pin = engine.export(&spec).await.unwrap();
    assert_ne!(loser.object, pin.object);
    assert_eq!(
        engine
            .plan_export_retirement(
                &spec.dataset,
                &id("early"),
                &spec.operation,
                "still selected"
            )
            .await
            .unwrap_err()
            .code(),
        "export_selected"
    );
    use tower::ServiceExt;
    let response = crate::http::router(engine.clone()).oneshot(axum::http::Request::builder()
        .method("POST").uri("/retention/exports/plan").header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::json!({"dataset": spec.dataset, "retirement": "http-end", "operation": spec.operation, "reason": "still selected"}).to_string())).unwrap()).await.unwrap();
    assert!(!response.status().is_success());
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
        "export_selected"
    );
    let successor = ExportSpec {
        operation: id("successor"),
        expected: Some(pin.id.clone()),
        ..spec.clone()
    };
    let latest = engine.export(&successor).await.unwrap();
    retire(&engine, &spec).await;
    let created = files(&engine, &spec).await;
    let losing = created
        .iter()
        .find(|file| file.path == loser.object.path)
        .unwrap();
    let winning = created
        .iter()
        .find(|file| file.path == pin.object.path)
        .unwrap();
    assert_eq!(
        engine
            .plan_export_file_removal(
                &spec.dataset,
                &id("wrong"),
                &spec.operation,
                &winning.id,
                "public bytes"
            )
            .await
            .unwrap_err()
            .code(),
        "export_stage_protected"
    );
    let plan = engine
        .plan_export_file_removal(
            &spec.dataset,
            &id("loser"),
            &spec.operation,
            &losing.id,
            "losing output",
        )
        .await
        .unwrap();
    assert_eq!(plan.inspection.objects[0].object, loser.object);
    engine.remove_objects(&plan).await.unwrap();
    assert!(!Path::new(&loser.object.path).exists());
    assert_eq!(engine.export(&spec).await.unwrap(), pin);
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &spec.channel)
            .await
            .unwrap(),
        latest
    );
    let mut batches = engine.read_export(&pin).await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    values.sort();
    assert_eq!(values, vec![None, Some(1), Some(1)]);
}

#[tokio::test]
async fn partial_same_bytes_replacement_cannot_acquire_original_creation_identity() {
    let (root, engine, spec) = fixture().await;
    assert_eq!(
        engine
            .export_until(&spec, "host-payload")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let created = files(&engine, &spec).await;
    let file = &created[0];
    retire(&engine, &spec).await;
    let plan = engine
        .plan_export_file_removal(
            &spec.dataset,
            &id("remove"),
            &spec.operation,
            &file.id,
            "partial cleanup",
        )
        .await
        .unwrap();
    let original = root.path().join("original-held");
    std::fs::rename(&file.path, &original).unwrap();
    std::fs::copy(&original, &file.path).unwrap();
    let outcome = engine.remove_objects(&plan).await.unwrap();
    assert_eq!(outcome.confirmed_unlinked_bytes, 0);
    assert!(Path::new(&file.path).exists());
    assert!(outcome.progress.objects.iter().any(
        |outcome| matches!(outcome, RemovalOutcome::Failed { code } if code == "removal_changed")
    ));
}
