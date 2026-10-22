//! Completed Backfill retries use immutable result metadata after recovery inputs disappear.

use super::scalar;

use docspec::{Engine, model::RunSpec};
use scalar::{fixture, id, input, limits, spec};

#[tokio::test]
async fn completed_retries_skip_unavailable_recovery_inputs_and_preserve_identity() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let request = spec(&base, "derived", "length", "text_length", "length(text)");
    let pending = engine.plan_backfill(&request).await.unwrap();
    assert!(!pending.completed);
    assert_eq!(pending.files_completed, Some(0));
    assert!(pending.files_pending.unwrap() > 0);
    let completed = engine.backfill(&request).await.unwrap();
    assert_eq!(completed.dataset.groups["source"].rows, rows.len() as u64);
    let before = engine.inspect(&request.dataset).await.unwrap();
    drop(engine);

    // Fault injection in this private fixture is not retention/deletion authority.
    // Keep every byte so the same source values and evidence can be restored.
    let source = &base.dataset.groups["source"];
    let mut paths = vec![
        root.path().join("input.parquet"),
        std::path::PathBuf::from(&base.manifest.path),
        std::path::PathBuf::from(&source.metadata.path),
    ];
    paths.extend(
        source
            .objects
            .iter()
            .filter(|object| object.path.ends_with(".parquet"))
            .map(|object| std::path::PathBuf::from(&object.path)),
    );
    paths.sort();
    paths.dedup();
    let quarantine = root.path().join("unavailable-inputs");
    std::fs::create_dir(&quarantine).unwrap();
    let retained = paths
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let saved = quarantine.join(index.to_string());
            std::fs::rename(path, &saved).unwrap();
            (path.clone(), saved)
        })
        .collect::<Vec<_>>();

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert!(plan.completed);
    assert_eq!(plan.rows, completed.dataset.groups["source"].rows);
    assert_eq!(plan.files_completed, None);
    assert_eq!(plan.files_pending, None);
    assert!(plan.failures.is_empty());
    assert_eq!(plan.promotion_refusal, None);
    assert_eq!(engine.backfill(&request).await.unwrap(), completed);

    let mut conflict = request.clone();
    // A nonexistent field would fail compilation if identity were checked late.
    conflict.fields[0].expression = "unknown_field + 1".into();
    assert_eq!(
        engine.plan_backfill(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
    assert_eq!(
        engine.backfill(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
    drop(engine);

    for (path, saved) in retained {
        std::fs::rename(saved, path).unwrap();
    }
    // Restored objects retain the original immutable identities and bytes.
    for object in std::iter::once(&base.manifest)
        .chain(std::iter::once(&source.metadata))
        .chain(source.objects.iter())
    {
        assert_eq!(
            docspec::storage::digest(std::path::Path::new(&object.path))
                .unwrap()
                .0,
            object.digest
        );
    }
}

#[tokio::test]
async fn completed_plan_honors_own_availability_independently_of_base_availability() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "derived", "length", "text_length", "length(text)");
    let completed = engine.backfill(&request).await.unwrap();
    let replacement = input(&root.path().join("replacement.parquet"), &[(Some(9), None)]);
    engine
        .run(&RunSpec {
            dataset: request.dataset.clone(),
            operation: id("replacement"),
            base: Some(completed.id.clone()),
            relationship: None,
            input: replacement.path,
            expected_digest: replacement.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let retirement = engine
        .plan_availability_retirement(
            &request.dataset,
            &id("retire-base"),
            &base.id,
            "end independent base availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&retirement).await.unwrap();
    assert!(engine.plan_backfill(&request).await.unwrap().completed);
    assert_eq!(engine.backfill(&request).await.unwrap(), completed);

    let retirement = engine
        .plan_availability_retirement(
            &request.dataset,
            &id("retire-derived"),
            &request.operation,
            "end completed Backfill availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&retirement).await.unwrap();
    let before = engine.inspect(&request.dataset).await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine.plan_backfill(&request).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "availability_retired"
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
}
