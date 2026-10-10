//! Continuous scalar cutover checked against source edits and retained recovery work.
#![cfg(feature = "test-util")]

use super::scalar;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    model::{
        BackfillSpec, ContinuousBackfillSpec, OperationRequest, ReleasePin, RunSpec, UpdateSpec,
    },
};
use futures::TryStreamExt;
use scalar::{fixture, id, input, limits, spec};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc};

pub(super) fn continuous(base: &ReleasePin) -> ContinuousBackfillSpec {
    ContinuousBackfillSpec {
        backfill: spec(base, "continuous", "length", "text_length", "length(text)"),
        optimistic_rounds: 2,
        // Small fixtures need a bounded intake window, not a timing assertion.
        fence_millis: 30_000,
    }
}

fn build(root: &Path, operation: &str, base: &ReleasePin, rows: &[scalar::Row]) -> RunSpec {
    let source = input(&root.join(format!("{operation}.parquet")), rows);
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: Some(base.id.clone()),
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

fn deletion_keys(path: &Path, ids: Vec<Option<i64>>) -> docspec::model::InputSpec {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    docspec::model::InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: docspec::storage::digest(path).unwrap().0,
    }
}

pub(super) fn completed_work(records: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    records
        .iter()
        .filter(|(key, _)| {
            key.starts_with("phase/backfill/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("backfill-readback/")
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub(super) fn assert_retained(before: &BTreeMap<String, Value>, after: &BTreeMap<String, Value>) {
    for (key, value) in before {
        assert_eq!(&after[key], value, "completed work changed: {key}");
    }
}

fn child_request(records: &BTreeMap<String, Value>, child: &docspec::model::Id) -> BackfillSpec {
    match serde_json::from_value::<OperationRequest>(records[&format!("request/{child}")].clone())
        .unwrap()
    {
        OperationRequest::Backfill(request) => request,
        other => panic!("controlled child must retain a Backfill request: {other:?}"),
    }
}

pub(super) async fn assert_values(engine: &Engine, release: &ReleasePin, rows: Vec<scalar::Row>) {
    let mut stream = engine.read_combined(release).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lengths = arrow::compute::cast(batch.column(2), &DataType::Int64).unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        actual.extend(
            ids.iter()
                .zip(text.iter())
                .zip(lengths.iter())
                .map(|((id, text), length)| ((id, text.map(str::to_owned)), length)),
        );
    }
    let mut expected = rows
        .into_iter()
        .map(|row| {
            let length = row.1.as_ref().map(|text| text.chars().count() as i64);
            (row, length)
        })
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cold_retry_recovers_every_durable_handoff_with_exact_parent_result() {
    for phase in [
        "continuous-admission",
        "files",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = continuous(&base);
        assert_eq!(
            engine
                .continuous_backfill_until(&request, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure",
            "{phase}"
        );
        let plan = engine.plan_continuous_backfill(&request).await.unwrap();
        if phase == "selection" {
            assert!(plan.completed.is_some());
        } else {
            assert!(plan.completed.is_none());
            assert!(plan.child.is_some());
            assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
        }
        let before = completed_work(&engine.inspect(&base.dataset.dataset).await.unwrap());
        if phase != "continuous-admission" {
            assert!(!before.is_empty());
        }
        drop(engine);
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let release = engine.continuous_backfill(&request).await.unwrap();
        assert_ne!(release.id, request.backfill.operation);
        assert_eq!(
            release.dataset.groups["source"],
            base.dataset.groups["source"]
        );
        assert_values(&engine, &release, rows).await;
        assert_retained(
            &before,
            &engine.inspect(&base.dataset.dataset).await.unwrap(),
        );
        assert_eq!(engine.continuous_backfill(&request).await.unwrap(), release);
        let plan = engine.plan_continuous_backfill(&request).await.unwrap();
        assert_eq!(plan.completed, Some(release));
        assert!(!plan.fenced);
    }
}

#[tokio::test]
async fn interrupted_parent_catches_up_replacements_deletions_and_insertions() {
    for phase in ["files", "readback"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = continuous(&base);
        assert_eq!(
            engine
                .continuous_backfill_until(&request, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let original_child = engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap()
            .child
            .unwrap();
        let before = completed_work(&engine.inspect(&base.dataset.dataset).await.unwrap());
        let original_files = before
            .iter()
            .filter(|(key, _)| key.starts_with("phase/backfill/"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        assert!(!original_files.is_empty());
        let replacements = vec![
            (Some(0), Some("changed é🎉".into())),
            (Some(9000), Some(String::new())),
        ];
        let latest = engine
            .update(&UpdateSpec {
                dataset: base.dataset.dataset.clone(),
                operation: id("source-edit"),
                definition: "v1".into(),
                base: base.id.clone(),
                keys: vec!["id".into()],
                replacements: input(&root.path().join("replacement.parquet"), &replacements),
                deletions: Some(deletion_keys(
                    &root.path().join("deletions.parquet"),
                    vec![Some(1)],
                )),
            })
            .await
            .unwrap();
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let release = engine.continuous_backfill(&request).await.unwrap();
        assert_ne!(release.id, original_child);
        assert_eq!(
            release.dataset.groups["source"],
            latest.dataset.groups["source"]
        );
        assert_eq!(release.dataset.boundary, latest.dataset.boundary);
        let mut expected = rows
            .into_iter()
            .filter(|row| row.0 != Some(0) && row.0 != Some(1))
            .collect::<Vec<_>>();
        expected.extend(replacements);
        assert_values(&engine, &release, expected).await;
        let after = engine.inspect(&base.dataset.dataset).await.unwrap();
        assert_retained(&before, &after);
        let origins = release.dataset.computed["length"].origins.as_ref().unwrap();
        let origins: Value =
            serde_json::from_slice(&std::fs::read(&origins.path).unwrap()).unwrap();
        let mut reused = 0;
        let mut computed = 0;
        for origin in origins["files"].as_array().unwrap() {
            let key = origin["generation"]["key"].as_str().unwrap();
            if let Some(original) = original_files.get(key) {
                assert_eq!(&origin["completion"], original);
                assert_eq!(original["execution"]["operation"], original_child.as_str());
                reused += 1;
            } else {
                assert_eq!(
                    origin["completion"]["execution"]["operation"],
                    release.id.as_str()
                );
                computed += 1;
            }
        }
        assert!(
            reused > 0 && computed > 0,
            "catch-up must reuse unchanged file work"
        );
        assert!(
            !engine
                .plan_continuous_backfill(&request)
                .await
                .unwrap()
                .fenced
        );
        assert_eq!(engine.continuous_backfill(&request).await.unwrap(), release);
    }
}

#[tokio::test]
async fn parent_identity_conflicts_and_public_child_entry_points_refuse() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = continuous(&base);
    assert_eq!(
        engine
            .continuous_backfill_until(&request, "continuous-admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let child = engine
        .plan_continuous_backfill(&request)
        .await
        .unwrap()
        .child
        .unwrap();
    let before = engine.inspect(&base.dataset.dataset).await.unwrap();
    let child = child_request(&before, &child);
    assert_eq!(
        engine.backfill(&child).await.unwrap_err().code(),
        "continuous_child_owned"
    );
    assert_eq!(
        engine.plan_backfill(&child).await.unwrap_err().code(),
        "continuous_child_owned"
    );
    let mut conflict = request.clone();
    conflict.backfill.fields[0].expression = "unknown_field + 1".into();
    assert_eq!(
        engine
            .continuous_backfill(&conflict)
            .await
            .unwrap_err()
            .code(),
        "operation_conflict"
    );
    assert_eq!(
        engine
            .plan_continuous_backfill(&conflict)
            .await
            .unwrap_err()
            .code(),
        "operation_conflict"
    );
    assert_eq!(engine.inspect(&base.dataset.dataset).await.unwrap(), before);
    let release = engine.continuous_backfill(&request).await.unwrap();
    let selected_child = match &release.dataset.request {
        OperationRequest::Backfill(child) => child,
        _ => panic!("winning release must retain its child request"),
    };
    assert_eq!(
        engine.backfill(selected_child).await.unwrap_err().code(),
        "continuous_child_owned"
    );
    assert_eq!(
        engine
            .plan_backfill(selected_child)
            .await
            .unwrap_err()
            .code(),
        "continuous_child_owned"
    );
    assert_eq!(engine.continuous_backfill(&request).await.unwrap(), release);
}

#[tokio::test]
async fn parent_abandonment_survives_reopen_and_keeps_completed_files() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = continuous(&base);
    assert_eq!(
        engine
            .continuous_backfill_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let plan = engine.plan_continuous_backfill(&request).await.unwrap();
    let before = engine.inspect(&base.dataset.dataset).await.unwrap();
    let child = child_request(&before, plan.child.as_ref().unwrap());
    let completed = completed_work(&before);
    assert!(!completed.is_empty());
    engine
        .abandon(
            &base.dataset.dataset,
            &request.backfill.operation,
            "withdraw enrichment",
        )
        .await
        .unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .continuous_backfill(&request)
            .await
            .unwrap_err()
            .code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap_err()
            .code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.backfill(&child).await.unwrap_err().code(),
        "continuous_child_owned"
    );
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
    assert_retained(
        &completed,
        &engine.inspect(&base.dataset.dataset).await.unwrap(),
    );
}

#[tokio::test]
async fn selected_parent_retries_exact_child_after_successor_and_honors_availability() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = continuous(&base);
    let completed = engine.continuous_backfill(&request).await.unwrap();
    let successor = engine
        .run(&build(
            root.path(),
            "successor",
            &completed,
            &[(Some(9), None)],
        ))
        .await
        .unwrap();
    let retirement = engine
        .plan_availability_retirement(
            &base.dataset.dataset,
            &id("retire-base"),
            &base.id,
            "end independent base availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&retirement).await.unwrap();
    drop(engine);
    // Withhold original admission files in this isolated fixture. Retry must use
    // the selected child's metadata without reopening the retired base payload.
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
    let quarantine = root.path().join("retained-base-inputs");
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
    assert_eq!(
        engine.continuous_backfill(&request).await.unwrap(),
        completed
    );
    assert_eq!(
        engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap()
            .completed,
        Some(completed.clone())
    );
    assert_eq!(
        engine.latest(&base.dataset.dataset).await.unwrap(),
        successor
    );
    for (path, saved) in retained {
        std::fs::rename(saved, path).unwrap();
    }
    let retirement = engine
        .plan_availability_retirement(
            &base.dataset.dataset,
            &id("retire-child"),
            &completed.id,
            "end selected child availability",
        )
        .await
        .unwrap();
    engine.retire_availability(&retirement).await.unwrap();
    let before = engine.inspect(&base.dataset.dataset).await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .continuous_backfill(&request)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert_eq!(
        engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert_eq!(engine.inspect(&base.dataset.dataset).await.unwrap(), before);
}

async fn lifecycle_revisions(root: &Path) -> BTreeMap<String, i64> {
    let catalog = docspec::storage::local_catalog(&root.join("workspace"))
        .await
        .unwrap();
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let mut revisions = BTreeMap::new();
    for namespace in catalog.list_namespaces(None).await.unwrap() {
        for ident in catalog.list_tables(&namespace).await.unwrap() {
            if ident.name() != "lifecycle" {
                continue;
            }
            let native = catalog.load_table(&ident).await.unwrap();
            let stream = docspec::storage::read_batches(&native, &budget)
                .await
                .unwrap();
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await.unwrap() {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let revision = revisions.entry(keys.value(row).to_owned()).or_insert(0);
                    *revision = (*revision).max(values.value(row));
                }
            }
        }
    }
    revisions
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_fence_refuses_new_intake_but_drains_already_accepted_source() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let rows = vec![
        (Some(1), Some("é🎉".into())),
        (None, None),
        (Some(1), Some(String::new())),
    ];
    let accepted = build(root.path(), "accepted-source", &base, &rows);
    assert_eq!(
        engine
            .run_until(&accepted, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let mut request = continuous(&base);
    request.optimistic_rounds = 1;
    let worker_engine = engine.clone();
    let worker_request = request.clone();
    let mut worker =
        tokio::spawn(async move { worker_engine.continuous_backfill(&worker_request).await });
    // The deadline bounds a hung correctness fixture; it is not a performance gate.
    let fenced = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let plan = engine.plan_continuous_backfill(&request).await.unwrap();
            if plan.fenced {
                return plan;
            }
            assert!(
                !worker.is_finished(),
                "controller finished before establishing the fence"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("controller must establish a bounded intake fence");
    assert!(fenced.pending_source.contains(&accepted.operation));
    let first_child = fenced.child.unwrap();
    let blocked = build(root.path(), "blocked-source", &base, &[(Some(9), None)]);
    assert_eq!(
        engine.run(&blocked).await.unwrap_err().code(),
        "source_fenced"
    );
    assert!(
        !engine
            .inspect(&base.dataset.dataset)
            .await
            .unwrap()
            .contains_key("operation/blocked-source")
    );
    let source = engine.run(&accepted).await.unwrap();
    let release = tokio::time::timeout(std::time::Duration::from_secs(30), &mut worker)
        .await
        .expect("accepted source drain and cutover must finish")
        .unwrap()
        .unwrap();
    assert_ne!(release.id, first_child);
    assert_eq!(
        release.dataset.groups["source"],
        source.dataset.groups["source"]
    );
    assert_eq!(release.dataset.boundary, source.dataset.boundary);
    assert_values(&engine, &release, rows).await;
    let plan = engine.plan_continuous_backfill(&request).await.unwrap();
    assert_eq!(plan.completed, Some(release.clone()));
    assert!(plan.pending_source.is_empty());
    assert!(!plan.fenced);
    let records = engine.inspect(&base.dataset.dataset).await.unwrap();
    assert_eq!(records["source-fence"]["closed"], true);
    let revisions = lifecycle_revisions(root.path()).await;
    let selected = revisions[&format!("result/{}", release.id)];
    assert_eq!(revisions["authority"], selected);
    assert_eq!(revisions["continuous-result/continuous"], selected);
    assert_eq!(revisions["source-fence"], selected);
    // A new source request can enter immediately after the atomic close.
    let intake = build(root.path(), "after-cutover", &release, &[(Some(11), None)]);
    assert_eq!(
        engine
            .run_until(&intake, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
}

#[tokio::test]
async fn expired_drain_returns_pending_ids_reopens_intake_and_preserves_work() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let pending = build(root.path(), "unfinished-source", &base, &[(Some(1), None)]);
    assert_eq!(
        engine
            .run_until(&pending, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let mut request = continuous(&base);
    request.optimistic_rounds = 1;
    // A short lease exercises expiry while an accepted request remains unfinished.
    request.fence_millis = 100;
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            engine.continuous_backfill(&request)
        )
        .await
        .expect("drain must honor its bounded lease")
        .unwrap_err()
        .code(),
        "source_fence_lost"
    );
    let plan = engine.plan_continuous_backfill(&request).await.unwrap();
    assert!(plan.completed.is_none());
    assert!(plan.pending_source.contains(&pending.operation));
    assert!(!plan.fenced);
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
    let before = completed_work(&engine.inspect(&base.dataset.dataset).await.unwrap());
    assert!(!before.is_empty());
    let intake = build(root.path(), "after-expiry", &base, &[(Some(2), None)]);
    assert_eq!(
        engine
            .run_until(&intake, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine
        .abandon(
            &base.dataset.dataset,
            &intake.operation,
            "deadline fixture intake withdrawn",
        )
        .await
        .unwrap();
    engine
        .abandon(
            &base.dataset.dataset,
            &pending.operation,
            "unfinished source withdrawn",
        )
        .await
        .unwrap();
    let release = engine.continuous_backfill(&request).await.unwrap();
    assert_values(&engine, &release, rows).await;
    assert_retained(
        &before,
        &engine.inspect(&base.dataset.dataset).await.unwrap(),
    );
    assert!(
        !engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap()
            .fenced
    );
}

#[cfg(unix)]
#[tokio::test]
async fn restore_point_recovers_unfinished_parent_and_protects_active_and_selected_outputs() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let request = continuous(&base);
    assert_eq!(
        engine
            .continuous_backfill_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let pending = engine.plan_continuous_backfill(&request).await.unwrap();
    let records = engine.inspect(&base.dataset.dataset).await.unwrap();
    let completed = completed_work(&records);
    let prepared: docspec::model::TablePin = serde_json::from_value(
        records
            .iter()
            .find(|(key, _)| key.starts_with("backfill-prepared/"))
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    let active_objects = prepared.objects.clone();
    assert!(!active_objects.is_empty());
    let inspection = engine.inspect_retention(&active_objects).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let point = engine
        .create_restore_point(&base.dataset.dataset, &id("unfinished-parent"))
        .await
        .unwrap();
    let workspace = std::fs::canonicalize(root.path().join("workspace")).unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    // Restore points retain absolute workspace identities. Reconstruct a fresh
    // catalog at that original location after isolated fixture catalog loss.
    std::fs::rename(
        workspace.join("catalog.sqlite"),
        workspace.join("catalog-lost.sqlite"),
    )
    .unwrap();
    Engine::restore_local(&workspace, &point, limits(), true)
        .await
        .unwrap();
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let restored = engine.plan_continuous_backfill(&request).await.unwrap();
    assert_eq!(restored.child, pending.child);
    assert_eq!(restored.round, pending.round);
    assert!(restored.completed.is_none());
    assert_retained(
        &completed,
        &engine.inspect(&base.dataset.dataset).await.unwrap(),
    );
    let release = engine.continuous_backfill(&request).await.unwrap();
    assert_values(&engine, &release, rows).await;
    assert_retained(
        &completed,
        &engine.inspect(&base.dataset.dataset).await.unwrap(),
    );
    let selected_objects = release.dataset.groups["length"].objects.clone();
    let inspection = engine.inspect_retention(&selected_objects).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    assert_eq!(engine.continuous_backfill(&request).await.unwrap(), release);
}

#[tokio::test]
async fn absent_target_refuses_same_type_source_field_id_remapping() {
    for phase in ["files", "readback"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, initial, _) = fixture(root.path(), false).await;
        let OperationRequest::Build(mut source) = initial.dataset.request.clone() else {
            panic!("scalar fixture must retain its original Build");
        };
        source.operation = id("authored-source");
        source.base = Some(initial.id.clone());
        source.sql = "SELECT text, CAST(id AS VARCHAR) AS alternate FROM input".into();
        let authored = engine.run(&source).await.unwrap();
        assert!(!authored.dataset.groups.contains_key("length"));
        let request = continuous(&authored);
        assert_eq!(
            engine
                .continuous_backfill_until(&request, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let before = completed_work(&engine.inspect(&authored.dataset.dataset).await.unwrap());
        source.operation = id("reordered-source");
        source.base = Some(authored.id.clone());
        source.sql = "SELECT CAST(id AS VARCHAR) AS alternate, text FROM input".into();
        let reordered = engine.run(&source).await.unwrap();
        let original_table = docspec::storage::open_pin(&authored.dataset.groups["source"])
            .await
            .unwrap();
        let new_table = docspec::storage::open_pin(&reordered.dataset.groups["source"])
            .await
            .unwrap();
        let original_text = original_table
            .metadata()
            .current_schema()
            .field_by_name("text")
            .unwrap();
        let reused_id = new_table
            .metadata()
            .current_schema()
            .field_by_name("alternate")
            .unwrap();
        assert_eq!(
            original_text.id, reused_id.id,
            "native auto IDs reused across reordered columns"
        );
        assert_eq!(
            original_text.field_type, reused_id.field_type,
            "the remap cannot be caught by type checking"
        );
        assert_eq!(
            engine
                .continuous_backfill(&request)
                .await
                .unwrap_err()
                .code(),
            "backfill_source_identity_changed"
        );
        assert_eq!(
            engine.latest(&authored.dataset.dataset).await.unwrap(),
            reordered
        );
        assert!(
            !engine
                .latest(&authored.dataset.dataset)
                .await
                .unwrap()
                .dataset
                .groups
                .contains_key("length")
        );
        assert_retained(
            &before,
            &engine.inspect(&authored.dataset.dataset).await.unwrap(),
        );
    }
}
