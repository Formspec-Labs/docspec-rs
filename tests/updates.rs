//! Applied-write changes are checked against an independent in-memory reconstruction.

#[cfg(feature = "test-util")]
use super::support;

#[cfg(feature = "test-util")]
#[path = "updates/legacy_changes.rs"]
mod legacy_changes;

#[cfg(all(feature = "test-util", unix))]
#[path = "updates/optional_history.rs"]
mod optional_history;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::verify_release,
    model::{Id, InputSpec, PhaseResult, RunSpec, TablePin, UpdateSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

type Row = (Option<i64>, Option<String>);
fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 16 << 20,
        ..Limits::default()
    }
}
fn input(path: &Path, rows: &[Row], keys_only: bool) -> InputSpec {
    named_input(path, rows, keys_only, ["id", "text"])
}
fn named_input(path: &Path, rows: &[Row], keys_only: bool, names: [&str; 2]) -> InputSpec {
    let mut fields = vec![Field::new(names[0], DataType::Int64, true)];
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    ))];
    if !keys_only {
        fields.push(Field::new(names[1], DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>(),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    batch_input(path, &batch)
}
fn batch_input(path: &Path, batch: &RecordBatch) -> InputSpec {
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}
fn original_rows() -> Vec<Row> {
    (0_i64..512)
        .map(|value| {
            (
                Some(value),
                Some(blake3::hash(&value.to_le_bytes()).to_hex().to_string()),
            )
        })
        .chain([(None, None), (Some(1), Some("duplicate key".into()))])
        .collect()
}
async fn fixture(root: &Path) -> (Engine, RunSpec, docspec::model::ReleasePin, Vec<Row>) {
    let rows = original_rows();
    let source = input(&root.join("input.parquet"), &rows, false);
    let spec = RunSpec {
        base: None,
        relationship: None,
        dataset: id("comments"),
        operation: id("initial"),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    // Force small initial native files while allowing realistic decoder buffers
    // during later joins. This makes physical sharing observable in a small test.
    let engine = Engine::open(
        root.join("workspace"),
        Limits {
            max_batch_bytes: 16 << 10,
            ..limits()
        },
    )
    .await
    .unwrap();
    let release = engine.run(&spec).await.unwrap();
    drop(engine);
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    (engine, spec, release, rows)
}
fn update(
    root: &Path,
    base: &str,
    operation: &str,
    replacements: &[Row],
    deletions: &[Row],
) -> UpdateSpec {
    UpdateSpec {
        dataset: id("comments"),
        operation: id(operation),
        definition: "v1".into(),
        base: id(base),
        keys: vec!["id".into()],
        replacements: input(
            &root.join(format!("{operation}-rows.parquet")),
            replacements,
            false,
        ),
        deletions: Some(input(
            &root.join(format!("{operation}-keys.parquet")),
            deletions,
            true,
        )),
    }
}
async fn read(pin: &TablePin) -> Vec<Row> {
    let native = storage::open_pin(pin).await.unwrap();
    if pin.snapshot.is_none() {
        return vec![];
    }
    let budget = Budget::new(limits()).unwrap();
    let stream = storage::read_batches(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut values = Vec::new();
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
        values.extend(
            ids.iter()
                .zip(text.iter())
                .map(|(id, text)| (id, text.map(str::to_owned))),
        );
    }
    values.sort();
    values
}
fn reconstruction(before: &[Row], replacements: &[Row], deletions: &[Row]) -> Vec<Row> {
    let mut expected: Vec<_> = before
        .iter()
        .filter(|row| {
            !replacements
                .iter()
                .chain(deletions)
                .any(|change| row.0 == change.0)
        })
        .cloned()
        .chain(replacements.iter().cloned())
        .collect();
    expected.sort();
    expected
}

#[tokio::test]
async fn applied_updates_preserve_duplicates_null_keys_history_and_unchanged_files() {
    let root = TempDir::new().unwrap();
    let (engine, spec, before, rows) = fixture(root.path()).await;
    let replacements = vec![
        (Some(1), Some("replacement".into())),
        (Some(1), Some("replacement".into())),
        (None, Some("null key replaced".into())),
        (Some(1000), None),
    ];
    let deletions = vec![(Some(2), None), (Some(999), None)];
    let mutation = update(root.path(), "initial", "changed", &replacements, &deletions);
    let release = engine.update(&mutation).await.unwrap();
    assert_eq!(
        read(&release.dataset.groups["source"]).await,
        reconstruction(&rows, &replacements, &deletions)
    );
    let certified = engine
        .change_certificate(&release.version_ref())
        .await
        .unwrap()
        .unwrap();
    let changes = certified.changes.source.as_ref().unwrap();
    assert_eq!(
        changes.before_metadata,
        before.dataset.groups["source"].metadata
    );
    assert_eq!(
        changes.after_metadata,
        release.dataset.groups["source"].metadata
    );
    let mut removed: Vec<_> = rows
        .iter()
        .filter(|row| [None, Some(1), Some(2)].contains(&row.0))
        .cloned()
        .collect();
    removed.sort();
    assert_eq!(read(&changes.removed).await, removed);
    let mut inserted = replacements.clone();
    inserted.sort();
    assert_eq!(read(&changes.added).await, inserted);
    let records = engine.inspect(&spec.dataset).await.unwrap();
    let phase = records
        .iter()
        .filter(|(key, _)| key.ends_with("/processing"))
        .map(|(_, value)| serde_json::from_value::<PhaseResult>(value.clone()).unwrap())
        .find(|phase| phase.operation == mutation.operation)
        .unwrap();
    assert!(
        phase.metrics.files_reused > 0,
        "fixture must span unaffected files"
    );
    assert_eq!(phase.metrics.rows_removed, removed.len() as u64);
    assert_eq!(phase.metrics.rows_added, replacements.len() as u64);
    let previous: Vec<_> = before.dataset.groups["source"]
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .collect();
    assert!(
        release.dataset.groups["source"]
            .objects
            .iter()
            .any(|object| previous.contains(&object))
    );
    assert_eq!(engine.update(&mutation).await.unwrap(), release);
    assert!(engine.plan_update(&mutation).await.unwrap().completed);
    let mut old = rows.clone();
    old.sort();
    assert_eq!(read(&before.dataset.groups["source"]).await, old);
    drop(engine);
    std::fs::rename(
        root.path().join("workspace/catalog.sqlite"),
        root.path().join("workspace/catalog.unavailable"),
    )
    .unwrap();
    verify_release(&release, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn update_handoffs_reuse_completed_work_after_input_loss() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let root = TempDir::new().unwrap();
        let (engine, _, _, rows) = fixture(root.path()).await;
        let replacements = vec![(Some(1), Some("shorter replacement".into()))];
        let mutation = update(root.path(), "initial", "changed", &replacements, &[]);
        assert_eq!(
            engine
                .update_until(&mutation, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let retained = engine.inspect(&mutation.dataset).await.unwrap();
        drop(engine);
        std::fs::remove_file(&mutation.replacements.path).unwrap();
        std::fs::remove_file(&mutation.deletions.as_ref().unwrap().path).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let result = engine.update(&mutation).await.unwrap();
        assert_eq!(
            read(&result.dataset.groups["source"]).await,
            reconstruction(&rows, &replacements, &[])
        );
        let resumed = engine.inspect(&mutation.dataset).await.unwrap();
        for (key, value) in retained.iter().filter(|(key, _)| key.starts_with("phase/")) {
            assert_eq!(resumed[key], *value);
        }
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn stale_updates_cannot_overwrite_a_selected_result() {
    let root = TempDir::new().unwrap();
    let (engine, _, _, _) = fixture(root.path()).await;
    let first = update(
        root.path(),
        "initial",
        "first",
        &[(Some(1), Some("first".into()))],
        &[],
    );
    let second = update(
        root.path(),
        "initial",
        "second",
        &[(Some(2), Some("second".into()))],
        &[],
    );
    assert_eq!(
        engine
            .update_until(&first, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let selected = engine.update(&second).await.unwrap();
    assert_eq!(
        engine.update(&first).await.unwrap_err().code(),
        "stale_base"
    );
    assert_eq!(
        engine.plan_update(&first).await.unwrap_err().code(),
        "stale_base"
    );
    assert_eq!(engine.latest(&first.dataset).await.unwrap(), selected);
    let mut corrected = first.clone();
    corrected.base = second.operation;
    assert_eq!(
        engine.update(&corrected).await.unwrap_err().code(),
        "operation_conflict"
    );
}

#[tokio::test]
async fn overlap_refuses_and_empty_replacement_can_delete_every_row() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, rows) = fixture(root.path()).await;
    let overlap = update(
        root.path(),
        "initial",
        "overlap",
        &[(None, Some("ambiguous".into()))],
        &[(None, None)],
    );
    assert_eq!(
        engine.update(&overlap).await.unwrap_err().code(),
        "conflicting_update"
    );
    assert_eq!(engine.latest(&overlap.dataset).await.unwrap(), before);
    let deletions: Vec<_> = rows.iter().map(|row| (row.0, None)).collect();
    let remove = update(root.path(), "initial", "remove", &[], &deletions);
    let empty = engine.update(&remove).await.unwrap();
    assert_eq!(empty.dataset.groups["source"].rows, 0);
    assert!(read(&empty.dataset.groups["source"]).await.is_empty());
    assert_eq!(
        engine
            .change_certificate(&empty.version_ref())
            .await
            .unwrap()
            .unwrap()
            .changes
            .source
            .unwrap()
            .removed
            .rows,
        rows.len() as u64
    );
    verify_release(&empty, limits()).await.unwrap();
    let append = update(
        root.path(),
        "remove",
        "append",
        &[(Some(101), Some("new".into()))],
        &[],
    );
    let result = engine.update(&append).await.unwrap();
    assert_eq!(
        read(&result.dataset.groups["source"]).await,
        vec![(Some(101), Some("new".into()))]
    );
}

#[tokio::test]
async fn identical_competing_updates_reconcile_and_noop_preserves_files() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, rows) = fixture(root.path()).await;
    let mutation = update(root.path(), "initial", "changed", &[], &[]);
    let (one, two) = tokio::join!(engine.update(&mutation), engine.update(&mutation));
    let result = one.unwrap();
    assert_eq!(two.unwrap(), result);
    assert_eq!(
        read(&result.dataset.groups["source"]).await,
        reconstruction(&rows, &[], &[])
    );
    let certified = engine
        .change_certificate(&result.version_ref())
        .await
        .unwrap()
        .unwrap();
    let changes = certified.changes.source.as_ref().unwrap();
    assert_eq!((changes.removed.rows, changes.added.rows), (0, 0));
    let parquet = |pin: &TablePin| {
        pin.objects
            .iter()
            .filter(|object| object.path.ends_with(".parquet"))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        parquet(&before.dataset.groups["source"]),
        parquet(&result.dataset.groups["source"])
    );
}

#[tokio::test]
async fn affected_base_with_changed_file_size_refuses_before_rewrite() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, _) = fixture(root.path()).await;
    let pin = &before.dataset.groups["source"];
    let file = pin
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    // Replacing native output changes its recorded size. Preparing retained
    // origins refuses the shared file without a preliminary payload hash.
    input(
        Path::new(&file.path),
        &[(Some(1), Some("corrupt".into()))],
        false,
    );
    assert_ne!(std::fs::metadata(&file.path).unwrap().len(), file.bytes);
    let mutation = update(root.path(), "initial", "changed", &[(Some(1), None)], &[]);
    assert_eq!(
        engine.update(&mutation).await.unwrap_err().code(),
        "unavailable_phase"
    );
    assert_eq!(engine.latest(&mutation.dataset).await.unwrap(), before);
}

#[tokio::test]
async fn routine_verification_inspects_required_state_files_once() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};
    let root = TempDir::new().unwrap();
    let (engine, _, _, _) = fixture(root.path()).await;
    let mutation = update(
        root.path(),
        "initial",
        "changed",
        &[(Some(1), Some("replacement".into()))],
        &[],
    );
    let release = engine.update(&mutation).await.unwrap();
    let paths: std::collections::BTreeSet<_> = release.dataset.groups["source"]
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .map(|object| object.path.as_str())
        .collect();
    let report = verify_release_at(&release, limits(), VerificationLevel::Routine)
        .await
        .unwrap();
    assert_eq!(report.data_files_checked, Some(paths.len()));
    assert!(report.sampled_files <= 3);
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn update_creation_inspects_only_new_files_without_duplicate_change_checks() {
    let root = TempDir::new().unwrap();
    let (engine, _, base, _) = fixture(root.path()).await;
    let mutation = update(
        root.path(),
        "initial",
        "changed",
        &[(Some(1), Some("new value".into()))],
        &[],
    );
    drop(engine);
    let request = root.path().join("request.json");
    let profile = root.path().join("limits.json");
    std::fs::write(&request, serde_json::to_vec(&mutation).unwrap()).unwrap();
    std::fs::write(&profile, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .arg("--limits")
        .arg(profile)
        .arg("update")
        .arg(request)
        .output()
        .unwrap();
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert!(result.status.success(), "{stderr}");
    let inspected: Vec<_> = stderr
        .lines()
        .filter_map(|line| line.strip_prefix("DOCSPEC_TEST_INSPECT_FILE "))
        .collect();
    assert!(!inspected.is_empty());
    assert_eq!(
        inspected.len(),
        inspected
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
    assert!(inspected.iter().all(|path| {
        !base.dataset.groups["source"]
            .objects
            .iter()
            .any(|object| object.path == *path)
    }));
    assert!(!stderr.contains("DOCSPEC_TEST_DEEP_TABLE"));
}

#[tokio::test]
async fn paired_writer_refusal_retains_admission_and_can_resume() {
    let root = TempDir::new().unwrap();
    // A value larger than the update allowance deterministically refuses in the
    // full affected-file reader. Key-only discovery still fits that allowance.
    let rows = vec![
        (Some(1), Some("a".repeat(64 << 10))),
        (Some(1), Some("duplicate".into())),
        (None, None),
    ];
    let source = input(&root.path().join("input.parquet"), &rows, false);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let before = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    drop(engine);
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            max_batch_bytes: 16 << 10,
            ..limits()
        },
    )
    .await
    .unwrap();
    let replacements = vec![];
    let deletions: Vec<_> = rows.iter().map(|row| (row.0, None)).collect();
    let mutation = update(root.path(), "initial", "changed", &replacements, &deletions);
    let failure =
        tokio::time::timeout(std::time::Duration::from_secs(10), engine.update(&mutation))
            .await
            .unwrap()
            .unwrap_err();
    assert_eq!(failure.code(), "oversized_batch");
    // Key-only discovery fits; the full old-file read now enforces admission
    // inside the paired writers before the joined-batch check can run.
    assert!(
        failure.to_string().contains("native reader exceeded"),
        "{failure}"
    );
    assert_eq!(engine.latest(&mutation.dataset).await.unwrap(), before);
    let plan = engine.plan_update(&mutation).await.unwrap();
    assert!(plan.reusable_phases.contains(&"admission".into()));
    assert!(plan.required_phases.contains(&"processing".into()));
    drop(engine);
    std::fs::remove_file(&mutation.replacements.path).unwrap();
    std::fs::remove_file(&mutation.deletions.as_ref().unwrap().path).unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let result = engine.update(&mutation).await.unwrap();
    assert_eq!(
        read(&result.dataset.groups["source"]).await,
        reconstruction(&rows, &replacements, &deletions)
    );
}

#[tokio::test]
async fn composite_nullable_quoted_keys_match_as_tuples() {
    let root = TempDir::new().unwrap();
    let rows = vec![
        (Some(1), Some("a".into())),
        (Some(1), Some("a".into())),
        (Some(1), None),
        (None, Some("a".into())),
        (None, None),
        (Some(2), Some("b".into())),
    ];
    let names = ["Ca.Se", "quo\"ted"];
    let original = named_input(&root.path().join("source.parquet"), &rows, false, names);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let initial = RunSpec {
        base: None,
        relationship: None,
        dataset: id("comments"),
        operation: id("initial"),
        input: original.path,
        expected_digest: original.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    engine.run(&initial).await.unwrap();
    let replacements = vec![
        (Some(1), Some("a".into())),
        (None, Some("a".into())),
        (None, Some("a".into())),
        (Some(3), None),
    ];
    let deletes = vec![(Some(1), None), (None, None)];
    let spec = UpdateSpec {
        dataset: initial.dataset,
        operation: id("changed"),
        definition: "v1".into(),
        base: initial.operation,
        keys: names.iter().map(|name| (*name).to_owned()).collect(),
        replacements: named_input(
            &root.path().join("rows.parquet"),
            &replacements,
            false,
            names,
        ),
        deletions: Some(named_input(
            &root.path().join("keys.parquet"),
            &deletes,
            false,
            names,
        )),
    };
    let result = engine.update(&spec).await.unwrap();
    let mut expected: Vec<_> = rows
        .iter()
        .filter(|row| !replacements.contains(row) && !deletes.contains(row))
        .cloned()
        .chain(replacements.iter().cloned())
        .collect();
    expected.sort();
    assert_eq!(read(&result.dataset.groups["source"]).await, expected);
    let mut removed: Vec<_> = rows
        .iter()
        .filter(|row| replacements.contains(row) || deletes.contains(row))
        .cloned()
        .collect();
    removed.sort();
    assert_eq!(
        read(
            &engine
                .change_certificate(&result.version_ref())
                .await
                .unwrap()
                .unwrap()
                .changes
                .source
                .unwrap()
                .removed
        )
        .await,
        removed
    );
}

#[tokio::test]
async fn independent_verifier_requires_changes_and_exact_endpoints() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, _) = fixture(root.path()).await;
    let spec = update(root.path(), "initial", "changed", &[(Some(1), None)], &[]);
    let result = engine.update(&spec).await.unwrap();
    let certified = engine
        .change_certificate(&result.version_ref())
        .await
        .unwrap()
        .unwrap();
    docspec::engine::verify_applied_changes(&before, &result, &certified.changes, limits())
        .await
        .unwrap();
    let mut rebound = result.clone();
    rebound
        .dataset
        .groups
        .insert("source".into(), before.dataset.groups["source"].clone());
    assert_eq!(
        docspec::engine::verify_applied_changes(&before, &rebound, &certified.changes, limits())
            .await
            .unwrap_err()
            .code(),
        "release_mismatch"
    );
    for change in 0..4 {
        let mut bad = certified.changes.clone();
        match change {
            0 => bad.source = None,
            1 => bad.source.as_mut().unwrap().key_field_ids = vec![999],
            2 => bad.source.as_mut().unwrap().base_manifest = result.manifest.clone(),
            _ => bad.source.as_mut().unwrap().removed.rows += 1,
        }
        assert_eq!(
            docspec::engine::verify_applied_changes(&before, &result, &bad, limits())
                .await
                .unwrap_err()
                .code(),
            "invalid_changes"
        );
    }
}

#[tokio::test]
async fn nested_replacements_preserve_values_and_native_field_ids() {
    use arrow::array::{Array, ListArray, StructArray};
    use arrow::datatypes::Int64Type;
    type NestedRow<'a> = (i64, Option<&'a str>, Option<Vec<Option<i64>>>);
    fn nested(path: &Path, child_name: &str, rows: &[NestedRow<'_>]) -> InputSpec {
        let tags =
            ListArray::from_iter_primitive::<Int64Type, _, _>(rows.iter().map(|row| row.2.clone()));
        let city = StringArray::from(rows.iter().map(|row| row.1).collect::<Vec<_>>());
        let fields = vec![
            Arc::new(Field::new(child_name, DataType::Utf8, true)),
            Arc::new(Field::new("tags", tags.data_type().clone(), true)),
        ];
        let details = StructArray::new(fields.into(), vec![Arc::new(city), Arc::new(tags)], None);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("details", details.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| Some(row.0)).collect::<Vec<_>>(),
                )),
                Arc::new(details),
            ],
        )
        .unwrap();
        batch_input(path, &batch)
    }
    let root = TempDir::new().unwrap();
    let original = nested(
        &root.path().join("source.parquet"),
        "city",
        &[
            (1, Some("old"), Some(vec![Some(1), None])),
            (2, None, None),
            (3, Some(""), Some(vec![])),
        ],
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let initial = RunSpec {
        base: None,
        relationship: None,
        dataset: id("comments"),
        operation: id("initial"),
        input: original.path,
        expected_digest: original.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    let before = engine.run(&initial).await.unwrap();
    let replacement = nested(
        &root.path().join("rows.parquet"),
        "city",
        &[(1, None, Some(vec![None, Some(9)])), (4, Some("new"), None)],
    );
    let spec = UpdateSpec {
        dataset: initial.dataset,
        operation: id("changed"),
        definition: "v1".into(),
        base: initial.operation,
        keys: vec!["id".into()],
        replacements: replacement,
        deletions: None,
    };
    let result = engine.update(&spec).await.unwrap();
    let old_table = storage::open_pin(&before.dataset.groups["source"])
        .await
        .unwrap();
    let native = storage::open_pin(&result.dataset.groups["source"])
        .await
        .unwrap();
    let original_fields = old_table.metadata().current_schema().as_struct().fields();
    let current_fields = native.metadata().current_schema().as_struct().fields();
    assert_eq!(&current_fields[..original_fields.len()], original_fields);
    // Origins follow every nested field ID; existing typed fields remain exact.
    assert_eq!(
        current_fields[original_fields.len()..]
            .iter()
            .map(|field| (field.name.as_str(), field.id))
            .collect::<Vec<_>>(),
        vec![("__docspec_origin_file", 6), ("__docspec_origin_row", 7)]
    );
    let budget = Budget::new(limits()).unwrap();
    let stream = storage::read_batches(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let details = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let city = details
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tags = details
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            let city = (!city.is_null(index)).then(|| city.value(index).to_owned());
            let tags = (!tags.is_null(index)).then(|| {
                tags.value(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            });
            values.push((ids.value(index), city, tags));
        }
    }
    values.sort();
    assert_eq!(
        values,
        vec![
            (1, None, Some(vec![None, Some(9)])),
            (2, None, None),
            (3, Some("".into()), Some(vec![])),
            (4, Some("new".into()), None)
        ]
    );
    let mut bad = spec.clone();
    bad.operation = id("bad-schema");
    bad.base = spec.operation;
    bad.replacements = nested(
        &root.path().join("bad.parquet"),
        "renamed",
        &[(1, Some("x"), None)],
    );
    assert_eq!(
        engine.update(&bad).await.unwrap_err().code(),
        "schema_mismatch"
    );
    assert_eq!(engine.latest(&bad.dataset).await.unwrap(), result);
}

#[tokio::test]
async fn http_update_routes_share_recovery_and_stale_base_refusals() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let (engine, _, _, _) = fixture(root.path()).await;
    let app = docspec::http::router(engine);
    let mut spec = update(root.path(), "initial", "changed", &[(Some(1), None)], &[]);
    for path in ["/updates/plan", "/updates/run", "/updates/run"] {
        let response = app
            .clone()
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&spec).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    spec.operation = id("stale");
    let response = app
        .oneshot(
            Request::post("/updates/run")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body["code"], "stale_base");
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_update_workers_resume_retained_handoffs_and_exact_changes() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let root = TempDir::new().unwrap();
        let (engine, source, base, rows) = fixture(root.path()).await;
        let replacements = [(Some(1), Some("replacement".into())), (None, None)];
        let deletions = [(Some(2), None)];
        let request = update(root.path(), "initial", "changed", &replacements, &deletions);
        drop(engine);
        support::kill_after(root.path(), &request, "update", phase);
        std::fs::remove_file(source.input).unwrap();
        std::fs::remove_file(&request.replacements.path).unwrap();
        std::fs::remove_file(&request.deletions.as_ref().unwrap().path).unwrap();

        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let before = engine.inspect(&request.dataset).await.unwrap();
        let completed: Vec<_> = before
            .iter()
            .filter(|(key, value)| {
                key.starts_with("phase/") && value["operation"] == request.operation.as_str()
            })
            .collect();
        assert!(!completed.is_empty());
        if phase == "selection" {
            assert_eq!(
                engine.latest(&request.dataset).await.unwrap().id,
                request.operation
            );
        } else {
            assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        }
        drop(engine);
        let (release, work) = support::resume(root.path(), "update");
        let expected = match phase {
            "admission" => vec!["processing", "validation", "readback"],
            "processing" => vec!["validation", "readback"],
            "validation" => vec!["readback"],
            "readback" | "selection" => vec![],
            _ => unreachable!(),
        };
        assert_eq!(work, expected, "{phase}: only missing phases may execute");
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_eq!(
            read(&release.dataset.groups["source"]).await,
            reconstruction(&rows, &replacements, &deletions)
        );
        let certified = engine
            .change_certificate(&release.version_ref())
            .await
            .unwrap()
            .unwrap();
        let changes = certified.changes.source.as_ref().unwrap();
        let mut removed: Vec<_> = rows
            .iter()
            .filter(|row| {
                replacements
                    .iter()
                    .chain(&deletions)
                    .any(|change| row.0 == change.0)
            })
            .cloned()
            .collect();
        removed.sort();
        assert_eq!(read(&changes.removed).await, removed);
        let mut added = replacements.to_vec();
        added.sort();
        assert_eq!(read(&changes.added).await, added);
        verify_release(&release, limits()).await.unwrap();
        verify_release(&base, limits()).await.unwrap();
        let after = engine.inspect(&request.dataset).await.unwrap();
        for (key, value) in completed {
            assert_eq!(&after[key], value, "{phase}: retained phase must be reused");
        }
        if let Some(readback) = before.get("readback/changed") {
            assert_eq!(after["readback/changed"], *readback);
            assert_eq!(readback, &serde_json::to_value(&release).unwrap());
        }
        assert_eq!(engine.update(&request).await.unwrap(), release);
    }
}
