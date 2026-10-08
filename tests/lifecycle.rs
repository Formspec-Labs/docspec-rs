//! Native integration tests against the maintained SQLite-backed Iceberg catalog.

#[cfg(feature = "test-util")]
mod support;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    catalog::{Lifecycle, record},
    engine::verify_release,
    model::{Id, RunSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{fs::File, sync::Arc};
use tempfile::TempDir;

fn limits() -> Limits {
    Limits {
        scratch_bytes: 16 << 20,
        compute_threads: 2,
        batch_rows: 2,
        ..Limits::default()
    }
}

fn fixture(root: &std::path::Path) -> RunSpec {
    let path = root.join("comments.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("docket", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("D1"),
                Some("D1"),
                None,
                Some("D2"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("<p>same</p>"),
                Some("<p>same</p>"),
                None,
                Some(""),
            ])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: Id::new("comments").unwrap(),
        operation: Id::new("initial").unwrap(),
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "comments-v1".into(),
    }
}

#[tokio::test]
async fn exact_readback_idempotency_and_independent_catalog_free_read() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let root = directory.path().join("workspace");
    let engine = Engine::open(&root, limits()).await.unwrap();
    let release = engine.run(&spec).await.unwrap();
    assert_eq!(release.dataset.groups["source"].rows, 4);
    let before = engine.inspect(&spec.dataset).await.unwrap();
    assert_eq!(engine.run(&spec).await.unwrap(), release);
    assert_eq!(engine.inspect(&spec.dataset).await.unwrap(), before);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), release);
    let plan = engine.plan(&spec).await.unwrap();
    assert!(plan.completed);
    assert!(plan.required_phases.is_empty());
    drop(engine);
    // Hide the catalog, then decode values and multiplicity using only release pins.
    std::fs::rename(
        root.join("catalog.sqlite"),
        root.join("catalog.unavailable"),
    )
    .unwrap();
    verify_release(&release, limits()).await.unwrap();
    let table = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let batches: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> = batches
        .iter()
        .flat_map(|b| {
            let mut writer = arrow::json::ArrayWriter::new(Vec::new());
            writer.write_batches(&[b]).unwrap();
            writer.finish().unwrap();
            serde_json::from_slice::<Vec<serde_json::Value>>(&writer.into_inner()).unwrap()
        })
        .collect();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter()
            .filter(|r| r["id"] == 1 && r["comment"] == "<p>same</p>")
            .count(),
        2
    );
    assert!(rows.iter().any(|r| r["id"] == 2 && r["comment"].is_null()));
    assert!(rows.iter().any(|r| r["id"] == 3 && r["comment"] == ""));
}

#[tokio::test]
async fn native_snapshot_guard_rejects_stale_application_state() {
    let directory = TempDir::new().unwrap();
    let catalog = storage::local_catalog(directory.path()).await.unwrap();
    let lifecycle = Lifecycle::new(catalog, Budget::new(limits()).unwrap());
    let dataset = Id::new("guards").unwrap();
    let first = lifecycle.open(&dataset).await.unwrap();
    let second = lifecycle.open(&dataset).await.unwrap();
    lifecycle
        .commit(
            &first,
            vec![record("authority", "corrected dependencies").unwrap()],
        )
        .await
        .unwrap();
    let stale = lifecycle
        .commit(
            &second,
            vec![record("authority", "stale selection").unwrap()],
        )
        .await;
    assert!(stale.unwrap_err().to_string().contains("stale_authority"));
    let current = lifecycle.open(&dataset).await.unwrap();
    assert_eq!(
        lifecycle
            .get::<String>(&current, "authority")
            .await
            .unwrap()
            .as_deref(),
        Some("corrected dependencies")
    );
    assert_eq!(
        lifecycle.get::<String>(&first, "authority").await.unwrap(),
        None
    );
    assert_eq!(
        lifecycle
            .get::<String>(&current, "authority")
            .await
            .unwrap()
            .as_deref(),
        Some("corrected dependencies")
    );
    let duplicates = vec![
        record("authority", "one").unwrap(),
        record("authority", "two").unwrap(),
    ];
    assert_eq!(
        lifecycle
            .commit(&current, duplicates)
            .await
            .unwrap_err()
            .code(),
        "duplicate_key"
    );
}

#[tokio::test]
async fn every_acknowledged_metadata_update_survives_cache_loss() {
    let directory = TempDir::new().unwrap();
    let catalog = storage::local_catalog(directory.path()).await.unwrap();
    let profile = Limits {
        max_metadata_bytes: 4096,
        ..limits()
    };
    let lifecycle = Lifecycle::new(catalog.clone(), Budget::new(profile.clone()).unwrap());
    let dataset = Id::new("bounded-history").unwrap();
    let mut view = lifecycle.open(&dataset).await.unwrap();
    let mut acknowledged = None;
    for revision in 0..100 {
        let value = format!("{revision}:{}", "x".repeat(256));
        match lifecycle
            .commit(&view, vec![record("authority", &value).unwrap()])
            .await
        {
            Ok(next) => {
                view = next;
                acknowledged = Some(value);
                let cold = Lifecycle::new(catalog.clone(), Budget::new(profile.clone()).unwrap());
                let current = cold.open(&dataset).await.unwrap();
                assert_eq!(
                    cold.get::<String>(&current, "authority").await.unwrap(),
                    acknowledged
                );
            }
            Err(error) => {
                assert_eq!(error.code(), "metadata_capacity");
                assert!(revision > 1);
                let cold = Lifecycle::new(catalog, Budget::new(profile).unwrap());
                let current = cold.open(&dataset).await.unwrap();
                assert_eq!(
                    cold.get::<String>(&current, "authority").await.unwrap(),
                    acknowledged
                );
                return;
            }
        }
    }
    panic!("expected admission to refuse before exceeding the replay budget");
}

#[tokio::test]
async fn native_readers_hold_shared_admission_until_stream_drop() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.run(&spec).await.unwrap();
    let table = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let budget = Budget::new(Limits {
        memory_bytes: 8 << 20,
        max_batch_bytes: 2 << 20,
        compute_threads: 1,
        ..limits()
    })
    .unwrap();
    let first = storage::read_batches(&table, &budget).await.unwrap();
    assert_eq!(
        storage::read_batches(&table, &budget)
            .await
            .err()
            .unwrap()
            .code(),
        "capacity"
    );
    drop(first);
    let stream = storage::read_batches(&table, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 4);
}

#[tokio::test]
async fn plan_and_run_apply_the_same_definition_fence() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let original = engine.run(&spec).await.unwrap();
    let newer = RunSpec {
        operation: Id::new("new-definition").unwrap(),
        definition: "comments-v2".into(),
        ..spec.clone()
    };
    assert_eq!(
        engine.plan(&newer).await.unwrap_err().code(),
        "stale_definition"
    );
    assert_eq!(
        engine.run(&newer).await.unwrap_err().code(),
        "stale_definition"
    );
    engine
        .invalidate(
            &spec.dataset,
            "activate new definition",
            Some(newer.definition),
        )
        .await
        .unwrap();
    assert!(engine.plan(&spec).await.unwrap().completed);
    assert_eq!(engine.run(&spec).await.unwrap(), original);
}

#[tokio::test]
async fn live_workspace_owner_and_conflicting_operation_refuse() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let root = directory.path().join("workspace");
    let engine = Engine::open(&root, limits()).await.unwrap();
    assert_eq!(
        Engine::open(&root, limits()).await.unwrap_err().code(),
        "workspace_busy"
    );
    engine.run(&spec).await.unwrap();
    spec.sql = "SELECT id FROM input".into();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "operation_conflict"
    );
}

#[tokio::test]
async fn bad_input_and_non_query_sql_never_select_a_release() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    spec.sql = "DROP TABLE input".into();
    assert_eq!(engine.run(&spec).await.unwrap_err().code(), "query_only");
    spec.sql = "SELECT * FROM input".into();
    spec.expected_digest = "0".repeat(64);
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "input_digest_mismatch"
    );
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn every_durable_phase_survives_reopen_and_source_loss() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let directory = TempDir::new().unwrap();
        let spec = fixture(directory.path());
        let root = directory.path().join("workspace");
        let engine = Engine::open(&root, limits()).await.unwrap();
        assert_eq!(
            engine.run_until(&spec, phase).await.unwrap_err().code(),
            "injected_failure"
        );
        let retained = engine.inspect(&spec.dataset).await.unwrap();
        drop(engine);
        std::fs::remove_file(&spec.input).unwrap();
        let engine = Engine::open(&root, limits()).await.unwrap();
        let release = engine.run(&spec).await.unwrap();
        verify_release(&release, limits()).await.unwrap();
        let resumed = engine.inspect(&spec.dataset).await.unwrap();
        for (key, value) in retained {
            if key.starts_with("phase/") {
                assert_eq!(resumed[&key], value, "repeated qualified phase at {phase}");
            }
        }
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn dependency_correction_fences_adoption_without_output_change() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    assert!(engine.run_until(&spec, "processing").await.is_err());
    engine
        .invalidate(&spec.dataset, "language is material", None)
        .await
        .unwrap();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
    assert!(
        engine
            .inspect(&spec.dataset)
            .await
            .unwrap()
            .keys()
            .any(|k| k.ends_with("/processing"))
    );
}

#[tokio::test]
async fn new_operation_reuses_qualified_values_and_original_generation() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let old = engine.run(&spec).await.unwrap();
    spec.operation = Id::new("retry-new-operation").unwrap();
    std::fs::remove_file(&spec.input).unwrap();
    let new = engine.run(&spec).await.unwrap();
    assert_eq!(old.dataset.groups, new.dataset.groups);
    assert!(new.dataset.boundary > old.dataset.boundary);
    assert_eq!(new.dataset.inputs, old.dataset.inputs);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn older_preparation_cannot_replace_newer_coverage() {
    let directory = TempDir::new().unwrap();
    let old = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&old, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let new = RunSpec {
        operation: Id::new("newer").unwrap(),
        sql: "SELECT * FROM input WHERE id <> 2".into(),
        ..old.clone()
    };
    let selected = engine.run(&new).await.unwrap();
    assert_eq!(
        engine.run(&old).await.unwrap_err().code(),
        "coverage_regression"
    );
    assert_eq!(engine.latest(&old.dataset).await.unwrap(), selected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_retries_reconcile_to_one_immutable_release() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let (first, second) = tokio::join!(engine.run(&spec), engine.run(&spec));
    assert_eq!(first.unwrap(), second.unwrap());
    verify_release(&engine.latest(&spec.dataset).await.unwrap(), limits())
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_output_keeps_schema_and_readable_history() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let old = engine.run(&spec).await.unwrap();
    spec.operation = Id::new("empty").unwrap();
    spec.sql = "SELECT * FROM input WHERE FALSE".into();
    let empty = engine.run(&spec).await.unwrap();
    assert_eq!(empty.dataset.groups["source"].rows, 0);
    verify_release(&empty, limits()).await.unwrap();
    verify_release(&old, limits()).await.unwrap();
    let table = storage::open_pin(&empty.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(
        table.metadata().current_schema().as_struct().fields().len(),
        3
    );
}

#[tokio::test]
async fn independent_reader_rejects_same_size_corruption() {
    use std::io::{Read, Seek, Write};
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.run(&spec).await.unwrap();
    let object = release.dataset.groups["source"]
        .objects
        .iter()
        .find(|o| o.path.ends_with(".parquet"))
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&object.path)
        .unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    file.rewind().unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    assert_eq!(
        verify_release(&release, limits()).await.unwrap_err().code(),
        "corrupt_object"
    );
}

#[tokio::test]
async fn unstable_functions_do_not_become_reusable_results() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    for (id, sql) in [
        ("random", "SELECT random() AS value FROM input"),
        ("clock", "SELECT now() AS value FROM input"),
        ("array", "SELECT array_agg(id) AS ids FROM input"),
        ("first", "SELECT first_value(id) AS id FROM input"),
        (
            "window",
            "SELECT id, CAST(row_number() OVER () AS BIGINT) AS occurrence FROM input",
        ),
        ("limit", "SELECT * FROM input LIMIT 1"),
        (
            "nested-limit",
            "SELECT * FROM (SELECT * FROM input LIMIT 1)",
        ),
        ("offset", "SELECT * FROM input OFFSET 1"),
        ("fetch", "SELECT * FROM input FETCH FIRST 1 ROW ONLY"),
    ] {
        spec.operation = Id::new(id).unwrap();
        spec.sql = sql.into();
        assert_eq!(
            engine.run(&spec).await.unwrap_err().code(),
            "unrepeatable_query"
        );
    }
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
}

#[tokio::test]
async fn local_http_returns_the_engine_selection_and_structured_conflicts() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let router = docspec::http::router(engine);
    let request = |spec: &RunSpec| {
        Request::builder()
            .method("POST")
            .uri("/run")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(spec).unwrap()))
            .unwrap()
    };
    let first = router.clone().oneshot(request(&spec)).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let body = axum::body::to_bytes(first.into_body(), 1 << 20)
        .await
        .unwrap();
    let latest = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/datasets/comments/latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(latest.status(), StatusCode::OK);
    assert_eq!(
        body,
        axum::body::to_bytes(latest.into_body(), 1 << 20)
            .await
            .unwrap()
    );
    spec.sql = "SELECT id FROM input".into();
    assert_eq!(
        router.oneshot(request(&spec)).await.unwrap().status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn comparison_preserves_duplicates_and_historical_release() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let first = engine.run(&spec).await.unwrap();
    std::fs::remove_file(&spec.input).unwrap();
    spec.operation = Id::new("deduplicated").unwrap();
    spec.sql = "SELECT DISTINCT * FROM input".into();
    let second = engine.run(&spec).await.unwrap();
    assert_eq!(
        engine.release(&spec.dataset, &first.id).await.unwrap(),
        first
    );
    let difference = engine.compare(&first, &second, "source").await.unwrap();
    assert!(difference.same_schema);
    assert_eq!(difference.added_rows, Some(0));
    assert_eq!(difference.removed_rows, Some(1));
    assert!(difference.same_inputs);
    spec.operation = Id::new("unsupported-multiset").unwrap();
    spec.sql = "SELECT * FROM input EXCEPT ALL SELECT DISTINCT * FROM input".into();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "unsupported_sql"
    );
}

#[tokio::test]
async fn oversized_output_refuses_then_resumes_from_admitted_input() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    spec.sql = "SELECT id, repeat('x', 65536) AS comment FROM input".into();
    let root = directory.path().join("workspace");
    let small = Limits {
        max_batch_bytes: 16 << 10,
        ..limits()
    };
    let engine = Engine::open(&root, small).await.unwrap();
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "oversized_batch"
    );
    let records = engine.inspect(&spec.dataset).await.unwrap();
    assert!(records.keys().any(|key| key.ends_with("/admission")));
    assert!(!records.keys().any(|key| key.ends_with("/processing")));
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
    drop(engine);
    std::fs::remove_file(&spec.input).unwrap();
    let engine = Engine::open(&root, limits()).await.unwrap();
    let release = engine.run(&spec).await.unwrap();
    verify_release(&release, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[test]
fn killed_cli_worker_resumes_every_completed_handoff() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let directory = TempDir::new().unwrap();
        let spec = fixture(directory.path());
        support::kill_after(directory.path(), &spec, "build", phase);
        std::fs::remove_file(&spec.input).unwrap();
        let (release, work) = support::resume(directory.path(), "resume");
        let expected = match phase {
            "admission" => vec!["processing", "validation", "readback"],
            "processing" => vec!["validation", "readback"],
            "validation" => vec!["readback"],
            "readback" | "selection" => vec![],
            _ => unreachable!(),
        };
        assert_eq!(work, expected, "{phase}: only missing phases may execute");
        assert_eq!(release.dataset.groups["source"].rows, 4);
    }
}
