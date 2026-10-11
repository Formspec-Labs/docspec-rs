//! Native integration tests against the maintained SQLite-backed Iceberg catalog.

#[cfg(feature = "test-util")]
use super::support;

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
        base: None,
        relationship: None,
        dataset: Id::new("comments").unwrap(),
        operation: Id::new("initial").unwrap(),
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "comments-v1".into(),
    }
}

#[tokio::test]
async fn routine_checks_report_scope_and_deep_checks_find_same_size_input_damage() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};
    let root = TempDir::new().unwrap();
    let spec = fixture(root.path());
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.run(&spec).await.unwrap();
    let report = verify_release_at(&release, limits(), VerificationLevel::Routine)
        .await
        .unwrap();
    assert_eq!(report.level, VerificationLevel::Routine);
    assert_eq!(report.tables_checked, 1);
    assert_eq!(report.rows_from_metadata, 4);
    assert!(report.sampled_files > 0 && report.sampled_files <= 3);
    assert!(report.sampled_rows > 0 && report.sampled_rows <= 24);
    let input = &release.dataset.inputs["input"];
    let mut bytes = std::fs::read(&input.path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&input.path, bytes).unwrap();
    // Routine checks deliberately trust retained identities for unsampled payloads.
    verify_release_at(&release, limits(), VerificationLevel::Routine)
        .await
        .unwrap();
    assert_eq!(
        verify_release(&release, limits()).await.unwrap_err().code(),
        "corrupt_object"
    );
    // Serving uses only the selected result; producer inputs need not be re-opened.
    std::fs::remove_file(&input.path).unwrap();
    let mut stream = engine.read_group(&release, "source").await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 4);
    assert_eq!(
        verify_release_at(&release, limits(), VerificationLevel::Routine)
            .await
            .unwrap_err()
            .code(),
        "unavailable_phase"
    );
}

#[tokio::test]
async fn routine_checks_reject_damaged_footer_and_declared_counts() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};
    let root = TempDir::new().unwrap();
    let spec = fixture(root.path());
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.run(&spec).await.unwrap();
    let mut forged = release.clone();
    forged.dataset.groups.get_mut("source").unwrap().rows += 1;
    let bytes = serde_json::to_vec(&forged.dataset).unwrap();
    let path = root.path().join("forged.json");
    std::fs::write(&path, &bytes).unwrap();
    forged.manifest.path = path.to_string_lossy().into_owned();
    forged.manifest.bytes = bytes.len() as u64;
    forged.manifest.digest = blake3::hash(&bytes).to_hex().to_string();
    assert_eq!(
        verify_release_at(&forged, limits(), VerificationLevel::Routine)
            .await
            .unwrap_err()
            .code(),
        "row_count_mismatch"
    );
    let data = release.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let mut bytes = std::fs::read(&data.path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&data.path, bytes).unwrap();
    assert_eq!(
        verify_release_at(&release, limits(), VerificationLevel::Routine)
            .await
            .unwrap_err()
            .code(),
        "parquet"
    );
}

#[tokio::test]
async fn routine_samples_allow_several_individually_bounded_wide_batches() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};
    let root = TempDir::new().unwrap();
    let mut spec = fixture(root.path());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "comment",
        DataType::Utf8,
        false,
    )]));
    let values: Vec<String> = (0..24)
        .map(|row| {
            (0..470)
                .map(|part| {
                    blake3::hash(format!("{row}:{part}").as_bytes())
                        .to_hex()
                        .to_string()
                })
                .collect()
        })
        .collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&spec.input).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    spec.expected_digest = storage::digest(std::path::Path::new(&spec.input))
        .unwrap()
        .0;
    let profile = Limits {
        batch_rows: 1,
        max_batch_bytes: 64 << 10,
        ..limits()
    };
    let engine = Engine::open(root.path().join("workspace"), profile.clone())
        .await
        .unwrap();
    let release = engine.run(&spec).await.unwrap();
    let report = verify_release_at(&release, profile.clone(), VerificationLevel::Routine)
        .await
        .unwrap();
    assert_eq!(report.sampled_files, 3);
    assert!(report.sampled_decoded_bytes > profile.max_batch_bytes);
    assert!(report.sampled_decoded_bytes <= 3 * profile.max_batch_bytes);
}

#[test]
#[cfg(feature = "test-util")]
fn cli_validates_once_then_reads_and_resumes_without_inspection() {
    let root = TempDir::new().unwrap();
    let spec = fixture(root.path());
    let request = root.path().join("request.json");
    let profile = root.path().join("limits.json");
    let release_path = root.path().join("release.json");
    std::fs::write(&request, serde_json::to_vec(&spec).unwrap()).unwrap();
    std::fs::write(&profile, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let run = |args: &[&std::ffi::OsStr]| {
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
            .env("RUST_LOG", "docspec=info")
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg("--limits")
            .arg(&profile)
            .args(args)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        result
    };
    let created = run(&["run".as_ref(), request.as_os_str()]);
    std::fs::write(&release_path, &created.stdout).unwrap();
    let stderr = String::from_utf8(created.stderr).unwrap();
    assert_eq!(stderr.matches("DOCSPEC_TEST_INSPECT_TABLE").count(), 1);
    assert!(!stderr.contains("DOCSPEC_TEST_DEEP_TABLE"));
    assert!(stderr.contains("native_writer_progress"));
    assert!(stderr.contains("phase_work_finished"));
    for args in [
        vec!["resume".as_ref(), request.as_os_str()],
        vec![
            "read".as_ref(),
            release_path.as_os_str(),
            root.path().join("read.arrow").as_os_str(),
        ],
    ] {
        let result = run(&args);
        let stderr = String::from_utf8(result.stderr).unwrap();
        assert!(!stderr.contains("DOCSPEC_TEST_INSPECT_TABLE"));
        assert!(!stderr.contains("DOCSPEC_TEST_DEEP_TABLE"));
        assert!(!stderr.contains("native_writer_"));
        assert!(!stderr.contains("phase_work_started"));
    }
    let routine = run(&["verify".as_ref(), release_path.as_os_str()]);
    let report: serde_json::Value = serde_json::from_slice(&routine.stdout).unwrap();
    assert_eq!(report["checks"]["level"], "routine");
    assert_eq!(
        String::from_utf8(routine.stderr)
            .unwrap()
            .matches("DOCSPEC_TEST_INSPECT_TABLE")
            .count(),
        1
    );
    let deep = run(&[
        "verify".as_ref(),
        release_path.as_os_str(),
        "--deep".as_ref(),
    ]);
    let report: serde_json::Value = serde_json::from_slice(&deep.stdout).unwrap();
    assert_eq!(report["checks"]["level"], "deep");
    assert!(
        String::from_utf8(deep.stderr)
            .unwrap()
            .contains("DOCSPEC_TEST_DEEP_TABLE")
    );
}

#[tokio::test]
async fn exact_readback_idempotency_and_independent_catalog_free_read() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
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
    let table = storage::open_pin(&release.dataset.groups["source"], &pin_budget)
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
        // Admit the native writer's fixed metadata before exhausting retained
        // history. Refusal must preserve every previously acknowledged update.
        max_metadata_bytes: 64 << 10,
        ..limits()
    };
    let lifecycle = Lifecycle::new(catalog.clone(), Budget::new(profile.clone()).unwrap());
    let dataset = Id::new("bounded-history").unwrap();
    let mut view = lifecycle.open(&dataset).await.unwrap();
    let mut acknowledged = None;
    for revision in 0..100 {
        let value = format!("{revision}:{}", "x".repeat(4096));
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
    let budget = Budget::new(Limits {
        memory_bytes: 8 << 20,
        max_batch_bytes: 2 << 20,
        compute_threads: 1,
        ..limits()
    })
    .unwrap();
    let table = storage::open_pin(&release.dataset.groups["source"], &budget)
        .await
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
            .any(|k| k.starts_with("phase/") && k.ends_with("/processing"))
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
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
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
    let table = storage::open_pin(&empty.dataset.groups["source"], &pin_budget)
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
    assert!(
        records
            .keys()
            .any(|key| key.starts_with("phase/") && key.ends_with("/admission"))
    );
    assert!(
        !records
            .keys()
            .any(|key| key.starts_with("phase/") && key.ends_with("/processing"))
    );
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

#[cfg(feature = "test-util")]
#[tokio::test]
async fn relocated_build_retry_keeps_the_original_readback_request() {
    let directory = TempDir::new().unwrap();
    let mut spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original = engine.inspect(&spec.dataset).await.unwrap()
        [&format!("readback/{}", spec.operation)]
        .clone();
    std::fs::remove_file(&spec.input).unwrap();
    spec.input = directory
        .path()
        .join("moved.parquet")
        .to_string_lossy()
        .into_owned();
    let resumed = engine.run(&spec).await.unwrap();
    assert_eq!(serde_json::to_value(resumed).unwrap(), original);
}

#[tokio::test]
async fn initial_plan_names_every_missing_handoff_including_selection_without_input_reads() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    std::fs::remove_file(&spec.input).unwrap();
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan(&spec).await.unwrap();
    assert!(!plan.completed);
    assert!(plan.reusable_phases.is_empty());
    assert!(plan.reusable_phase_origins.is_empty());
    assert_eq!(
        plan.required_phases,
        vec![
            "admission",
            "processing",
            "validation",
            "readback",
            "selection"
        ]
    );
    assert_eq!(plan.required_phase_reasons.len(), 4);
    assert!(
        plan.required_phase_reasons
            .values()
            .all(|reason| *reason == docspec::engine::PlanPhaseReason::NoRetainedCompletion)
    );
    assert!(!plan.required_phase_reasons.contains_key("selection"));
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
}

#[cfg(feature = "test-util")]
#[test]
fn killed_readback_plan_names_pending_selection_and_resume_executes_no_payload_work() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    support::kill_after(directory.path(), &spec, "build", "readback");
    std::fs::remove_file(&spec.input).unwrap();
    let (before, work): (serde_json::Value, _) =
        support::run_cli(directory.path(), &["inspect", spec.dataset.as_str()]);
    assert!(work.is_empty());
    let request = directory.path().join("request.json");
    let (plan, work): (serde_json::Value, _) =
        support::run_cli(directory.path(), &["plan", request.to_str().unwrap()]);
    assert!(
        work.is_empty(),
        "planning must not execute retained payload work"
    );
    assert_eq!(plan["completed"], false);
    assert_eq!(plan["reusable_phases"], serde_json::json!(["readback"]));
    assert_eq!(plan["reusable_phase_origins"].as_object().unwrap().len(), 1);
    assert_eq!(
        plan["reusable_phase_origins"]["readback"]["key"],
        format!("readback/{}", spec.operation)
    );
    assert!(
        plan["reusable_phase_origins"]["readback"]["revision"]
            .as_i64()
            .unwrap()
            > 0
    );
    assert_eq!(plan["required_phases"], serde_json::json!(["selection"]));
    let (after_plan, work): (serde_json::Value, _) =
        support::run_cli(directory.path(), &["inspect", spec.dataset.as_str()]);
    assert!(work.is_empty());
    assert_eq!(
        after_plan, before,
        "planning must preserve exact durable records"
    );
    let (release, work) = support::resume(directory.path(), "resume");
    assert!(
        work.is_empty(),
        "pending selection must reuse readback without admission, SQL, validation or serialization work"
    );
    assert_eq!(
        serde_json::to_value(&release).unwrap(),
        before[format!("readback/{}", spec.operation)]
    );
    let (after, work): (serde_json::Value, _) =
        support::run_cli(directory.path(), &["inspect", spec.dataset.as_str()]);
    assert!(work.is_empty());
    for (key, value) in before.as_object().unwrap() {
        if key.starts_with("phase/") || key.starts_with("readback/") {
            assert_eq!(&after[key], value, "selection must preserve {key}");
        }
    }
    let (completed, work): (serde_json::Value, _) =
        support::run_cli(directory.path(), &["plan", request.to_str().unwrap()]);
    assert!(work.is_empty());
    assert_eq!(completed["completed"], true);
    assert_eq!(completed["required_phases"], serde_json::json!([]));
    assert_eq!(
        completed["reusable_phase_origins"],
        plan["reusable_phase_origins"]
    );
}

#[tokio::test]
async fn completed_plan_remains_complete_after_later_selection_without_reselecting_old_result() {
    let directory = TempDir::new().unwrap();
    let spec = fixture(directory.path());
    let engine = Engine::open(directory.path().join("workspace"), limits())
        .await
        .unwrap();
    let original = engine.run(&spec).await.unwrap();
    let next = RunSpec {
        operation: Id::new("later").unwrap(),
        ..spec.clone()
    };
    let later = engine.run(&next).await.unwrap();
    std::fs::remove_file(&spec.input).unwrap();
    let before = engine.inspect(&spec.dataset).await.unwrap();
    let plan = engine.plan(&spec).await.unwrap();
    assert!(plan.completed);
    assert!(plan.required_phases.is_empty());
    assert_eq!(plan.reusable_phase_origins.len(), 1);
    assert_eq!(
        plan.reusable_phase_origins["readback"].key,
        format!("readback/{}", spec.operation)
    );
    assert_eq!(engine.run(&spec).await.unwrap(), original);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), later);
    assert_eq!(engine.inspect(&spec.dataset).await.unwrap(), before);
}
