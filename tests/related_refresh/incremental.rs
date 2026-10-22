//! Incremental joins checked against independent complete reconstruction.

use super::*;
use docspec::model::{InputSpec, TablePin, UpdateSpec};
use std::collections::BTreeSet;

fn write_rows(path: &Path, rows: &[Row], keys_only: bool) -> InputSpec {
    let mut fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ];
    let mut columns: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
    ];
    if !keys_only {
        fields.push(Field::new("text", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.2.as_deref()),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, columns).unwrap())
        .unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}
pub(super) async fn update(
    engine: &Engine,
    root: &Path,
    base: &ReleasePin,
    operation: &str,
    replacements: &[Row],
    deletions: &[Row],
) -> ReleasePin {
    let mut expected: Vec<Row> = occurrences(base)
        .await
        .into_iter()
        .map(|(_, row)| row)
        .filter(|row| {
            !replacements
                .iter()
                .chain(deletions)
                .any(|key| key.0 == row.0 && key.1 == row.1)
        })
        .collect();
    expected.extend_from_slice(replacements);
    expected.sort();
    let release = engine
        .update(&UpdateSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id(operation),
            base: base.id.clone(),
            definition: "v1".into(),
            keys: vec!["a".into(), "b".into()],
            replacements: write_rows(
                &root.join(format!(
                    "{}-{operation}-replace.parquet",
                    base.dataset.dataset
                )),
                replacements,
                false,
            ),
            deletions: Some(write_rows(
                &root.join(format!(
                    "{}-{operation}-delete.parquet",
                    base.dataset.dataset
                )),
                deletions,
                true,
            )),
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    let mut actual: Vec<_> = occurrences(&release)
        .await
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    actual.sort();
    assert_eq!(
        actual, expected,
        "source update {} {operation}",
        base.dataset.dataset
    );
    release
}
fn next(base: &ReleasePin, left: &ReleasePin, right: &ReleasePin, operation: &str) -> RefreshSpec {
    let mut spec = RefreshSpec {
        operation: id(operation),
        base: Some(base.id.clone()),
        mode: RefreshMode::Incremental,
        ..base.dataset.related.as_ref().unwrap().request.clone()
    };
    join_mut(&mut spec).left = left.version_ref();
    join_mut(&mut spec).right = right.version_ref();
    spec
}
fn row(a: Option<i64>, b: Option<i64>, value: &str) -> Row {
    (a, b, Some(value.into()))
}
pub(super) async fn files(pin: &TablePin) -> BTreeSet<String> {
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(pin, &budget).await.unwrap();
    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut paths = BTreeSet::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for index in 0..batch.num_rows() {
            paths.insert(text(&batch, storage::OCCURRENCE_FILE, index).unwrap());
        }
    }
    paths
}

#[tokio::test]
async fn incremental_matches_oracle_across_key_moves_nulls_duplicates_and_both_input_orders() {
    for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Full] {
        for nulls in [NullMatching::Never, NullMatching::Equal] {
            let root = TempDir::new().unwrap();
            let (engine, mut left, mut right) = fixture(root.path()).await;
            let mut spec = recipe(&left, &right);
            join_mut(&mut spec).join.kind = kind;
            join_mut(&mut spec).join.nulls = nulls;
            let mut joined = engine
                .refresh(&spec)
                .await
                .unwrap_or_else(|error| panic!("{kind:?} {nulls:?} {}: {error}", spec.operation));
            // New first partner for an old unmatched comment; remove the last
            // ordinary-key partner; shrink duplicate fanout and change null groups.
            right = update(
                &engine,
                root.path(),
                &right,
                "right-1",
                &[
                    row(Some(9), Some(9), "new-first"),
                    row(None, Some(2), "changed-null"),
                ],
                &[row(Some(1), Some(2), "")],
            )
            .await;
            let spec = next(&joined, &left, &right, "incremental-1");
            joined = engine
                .refresh(&spec)
                .await
                .unwrap_or_else(|error| panic!("{kind:?} {nulls:?} {}: {error}", spec.operation));
            check(
                &engine,
                &joined,
                &oracle(&left, &right, &join(&spec).join).await,
            )
            .await;
            // Move a key through deletion+insertion, add duplicate partners on
            // both sides, retain partial-null unmatched tuples under SQL equality.
            left = update(
                &engine,
                root.path(),
                &left,
                "left-2",
                &[
                    row(Some(5), Some(5), "moved"),
                    row(Some(5), Some(5), "moved"),
                    row(Some(1), None, "partial"),
                ],
                &[row(Some(9), Some(9), "")],
            )
            .await;
            right = update(
                &engine,
                root.path(),
                &right,
                "right-2",
                &[
                    row(Some(5), Some(5), "new-1"),
                    row(Some(5), Some(5), "new-2"),
                ],
                &[row(None, Some(2), "")],
            )
            .await;
            let spec = next(&joined, &left, &right, "incremental-2");
            joined = engine
                .refresh(&spec)
                .await
                .unwrap_or_else(|error| panic!("{kind:?} {nulls:?} {}: {error}", spec.operation));
            check(
                &engine,
                &joined,
                &oracle(&left, &right, &join(&spec).join).await,
            )
            .await;
            // The opposite producer admission order must yield the same complete
            // captured semantics, with an empty right side and retained left rows.
            right = update(
                &engine,
                root.path(),
                &right,
                "right-3",
                &[],
                &[
                    row(Some(5), Some(5), ""),
                    row(Some(9), Some(9), ""),
                    row(None, None, ""),
                    row(Some(8), Some(8), ""),
                    row(Some(1), None, ""),
                ],
            )
            .await;
            left = update(
                &engine,
                root.path(),
                &left,
                "left-3",
                &[row(Some(99), Some(99), "new-unmatched")],
                &[],
            )
            .await;
            let spec = next(&joined, &left, &right, "incremental-3");
            joined = engine
                .refresh(&spec)
                .await
                .unwrap_or_else(|error| panic!("{kind:?} {nulls:?} {}: {error}", spec.operation));
            check(
                &engine,
                &joined,
                &oracle(&left, &right, &join(&spec).join).await,
            )
            .await;
            verify_release(&joined, limits()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn incremental_shares_unaffected_output_and_evidence_and_adopts_exact_noop_pins() {
    let root = TempDir::new().unwrap();
    let (engine, mut left, mut right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let first = engine
        .backfill(&BackfillSpec {
            dataset: first.dataset.dataset.clone(),
            operation: id("with-lengths"),
            base: first.id.clone(),
            definition: "v1".into(),
            group: id("lengths"),
            group_definition: "length-v1".into(),
            fields: vec![ComputedField {
                id: 20,
                name: "length".into(),
                expression: "CAST(length(left_text) AS BIGINT)".into(),
            }],
        })
        .await
        .unwrap();
    let original_group = first.dataset.groups["lengths"].clone();
    let original_files = files(&first.dataset.groups["source"]).await;
    let original_evidence = files(&first.dataset.related.as_ref().unwrap().evidence).await;
    let objects = original_files
        .iter()
        .chain(&original_evidence)
        .map(|path| {
            (
                path,
                (
                    storage::digest(Path::new(path)).unwrap(),
                    std::fs::metadata(path).unwrap().modified().unwrap(),
                ),
            )
        })
        .collect::<Vec<_>>();
    left = update(
        &engine,
        root.path(),
        &left,
        "append",
        &[row(Some(50), Some(50), "fifty")],
        &[],
    )
    .await;
    right = update(
        &engine,
        root.path(),
        &right,
        "append",
        &[row(Some(50), Some(50), "partner")],
        &[],
    )
    .await;
    let joined = engine
        .refresh(&next(&first, &left, &right, "append"))
        .await
        .unwrap();
    assert!(original_files.is_subset(&files(&joined.dataset.groups["source"]).await));
    assert!(
        original_evidence
            .is_subset(&files(&joined.dataset.related.as_ref().unwrap().evidence).await)
    );
    left = update(
        &engine,
        root.path(),
        &left,
        "change",
        &[row(Some(50), Some(50), "changed")],
        &[],
    )
    .await;
    let joined = engine
        .refresh(&next(&joined, &left, &right, "change"))
        .await
        .unwrap();
    assert!(original_files.is_subset(&files(&joined.dataset.groups["source"]).await));
    assert!(
        original_evidence
            .is_subset(&files(&joined.dataset.related.as_ref().unwrap().evidence).await)
    );
    for object in original_group
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
    {
        assert!(joined.dataset.groups["lengths"].objects.contains(object));
    }
    for (path, (digest, modified)) in objects {
        assert_eq!(storage::digest(Path::new(path)).unwrap(), digest);
        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            modified
        );
    }
    check(
        &engine,
        &joined,
        &oracle(
            &left,
            &right,
            &join(&joined.dataset.related.as_ref().unwrap().request).join,
        )
        .await,
    )
    .await;
    left = update(&engine, root.path(), &left, "noop", &[], &[]).await;
    let noop = engine
        .refresh(&next(&joined, &left, &right, "noop"))
        .await
        .unwrap();
    assert_eq!(
        noop.dataset.groups["lengths"],
        joined.dataset.groups["lengths"]
    );
    assert_eq!(
        noop.dataset.groups["source"],
        joined.dataset.groups["source"]
    );
    assert_eq!(
        noop.dataset.related.as_ref().unwrap().evidence,
        joined.dataset.related.as_ref().unwrap().evidence
    );
    let certified = engine
        .change_certificate(&noop.version_ref())
        .await
        .unwrap()
        .unwrap();
    let changes = certified.changes.source.as_ref().unwrap();
    assert_eq!(changes.removed.rows, 0);
    assert_eq!(changes.added.rows, 0);
    assert_eq!(
        noop.dataset.related.as_ref().unwrap().coverage.inputs[&left.dataset.dataset],
        left.version_ref()
    );
}

#[tokio::test]
async fn incremental_refuses_unqualified_recipes_before_admission_and_binds_mode() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let before = engine.inspect(&id("joined")).await.unwrap();
    let mut spec = next(&first, &left, &right, "changed-recipe");
    join_mut(&mut spec).join.nulls = NullMatching::Never;
    assert_eq!(
        engine.refresh(&spec).await.unwrap_err().code(),
        "full_refresh_required"
    );
    assert_eq!(
        serde_json::to_value(engine.inspect(&id("joined")).await.unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    let mut missing = next(&first, &left, &right, "missing-base");
    missing.base = None;
    assert!(engine.plan_refresh(&missing).await.is_err());
    let mut completed = recipe(&left, &right);
    completed.mode = RefreshMode::Incremental;
    assert_eq!(
        engine.refresh(&completed).await.unwrap_err().code(),
        "operation_conflict"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn incremental_killed_workers_reuse_processing_evidence_and_readback() {
    for phase in ["processing", "evidence", "readback"] {
        let root = TempDir::new().unwrap();
        let (engine, left, right) = fixture(root.path()).await;
        let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
        let right = update(
            &engine,
            root.path(),
            &right,
            "changed",
            &[row(Some(9), Some(9), "first-partner")],
            &[],
        )
        .await;
        let spec = next(&first, &left, &right, "incremental");
        drop(engine);
        support::kill_after(root.path(), &spec, "refresh", phase);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let before = engine.inspect(&spec.dataset).await.unwrap();
        drop(engine);
        let (release, work) = support::resume(root.path(), "refresh");
        let expected = match phase {
            "processing" => vec!["evidence", "validation", "readback"],
            "evidence" => vec!["validation", "readback"],
            _ => vec![],
        };
        assert_eq!(work, expected);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let after = engine.inspect(&spec.dataset).await.unwrap();
        for (key, value) in before.iter().filter(|(key, _)| key.starts_with("phase/")) {
            assert_eq!(after.get(key), Some(value));
        }
        check(
            &engine,
            &release,
            &oracle(&left, &right, &join(&spec).join).await,
        )
        .await;
        assert_eq!(engine.refresh(&spec).await.unwrap(), release);
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn incremental_phase_reuse_fences_rebased_outputs_and_exact_manifests() {
    use docspec::{
        catalog::{Lifecycle, record},
        model::Authority,
    };
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let right = update(
        &engine,
        root.path(),
        &right,
        "changed",
        &[row(Some(9), Some(9), "first-partner")],
        &[],
    )
    .await;
    let spec = next(&first, &left, &right, "pending");
    assert_eq!(
        engine
            .refresh_until(&spec, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let mut different_mode = spec.clone();
    different_mode.mode = RefreshMode::Full;
    assert_eq!(
        engine.refresh(&different_mode).await.unwrap_err().code(),
        "operation_conflict"
    );
    let mut full = spec.clone();
    full.operation = id("full");
    full.mode = RefreshMode::Full;
    let current = engine.refresh(&full).await.unwrap();
    let rebased = next(&current, &left, &right, "rebased");
    let plan = engine.plan_refresh(&rebased).await.unwrap();
    assert!(
        plan.required_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert_eq!(
        engine
            .refresh_until(&rebased, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&spec.dataset).await.unwrap();
    let processing: Vec<docspec::model::PhaseResult> = records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
        .map(|(_, value)| serde_json::from_value(value.clone()).unwrap())
        .filter(|phase: &docspec::model::PhaseResult| phase.changes.is_some())
        .collect();
    assert!(
        processing
            .iter()
            .any(
                |phase| phase.changes.as_ref().unwrap().base_manifest == first.manifest
                    && phase.changes.as_ref().unwrap().removed.rows > 0
            )
    );
    assert!(
        processing
            .iter()
            .any(
                |phase| phase.changes.as_ref().unwrap().base_manifest == current.manifest
                    && phase.changes.as_ref().unwrap().removed.rows == 0
            )
    );
    // A same-ID manifest rebinding must not satisfy an operation captured earlier.
    let mut authority: Authority = serde_json::from_value(records["authority"].clone()).unwrap();
    let rebound = forged(
        root.path(),
        "same-bytes-different-manifest",
        current.clone(),
    );
    authority.release = Some(rebound.clone());
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.path().join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(
            &view,
            vec![
                record("authority", authority).unwrap(),
                record(format!("result/{}", current.id), rebound).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&rebased).await.unwrap_err().code(),
        "stale_base"
    );
    assert_eq!(
        engine.plan_refresh(&rebased).await.unwrap_err().code(),
        "stale_base"
    );
}

#[tokio::test]
async fn incremental_verifier_refuses_balanced_foreign_removals_and_unsupported_provenance() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let noop = engine
        .refresh(&next(&first, &left, &right, "noop"))
        .await
        .unwrap();
    let newer = update(
        &engine,
        root.path(),
        &left,
        "changed",
        &[
            row(Some(1), Some(2), "changed"),
            row(Some(1), Some(2), "changed"),
        ],
        &[],
    )
    .await;
    let mut other = recipe(&newer, &right);
    other.dataset = id("foreign");
    let other = engine.refresh(&other).await.unwrap();
    assert_eq!(
        first.dataset.groups["source"].rows,
        other.dataset.groups["source"].rows
    );
    // Balanced foreign removals/additions cancel algebraically but were never in
    // the base. Membership, not just count or the reconstruction equation, must refuse.
    let certified = engine
        .change_certificate(&noop.version_ref())
        .await
        .unwrap()
        .unwrap();
    let mut bad = certified.changes.clone();
    let changes = bad.source.as_mut().unwrap();
    changes.removed = other.dataset.groups["source"].clone();
    changes.added = other.dataset.groups["source"].clone();
    assert_eq!(
        docspec::engine::verify_applied_changes(&first, &noop, &bad, limits())
            .await
            .unwrap_err()
            .code(),
        "invalid_changes"
    );
    // Full refresh can legitimately change the join. Relabeling an otherwise
    // valid identity delta cannot qualify it as incremental execution.
    let mut full = next(&noop, &left, &right, "changed-recipe");
    full.mode = RefreshMode::Full;
    join_mut(&mut full).join.nulls = NullMatching::Never;
    let changed = engine.refresh(&full).await.unwrap();
    let mut bad = changed.clone();
    let mut applied = certified.changes.clone();
    applied.before = noop.version_ref();
    applied.operation = changed.id.clone();
    let changes = applied.source.as_mut().unwrap();
    changes.base_version = noop.id.clone();
    changes.base_manifest = noop.manifest.clone();
    changes.before_metadata = noop.dataset.groups["source"].metadata.clone();
    changes.after_metadata = bad.dataset.groups["source"].metadata.clone();
    changes.removed = noop.dataset.groups["source"].clone();
    changes.added = changed.dataset.groups["source"].clone();
    bad.dataset.related.as_mut().unwrap().request.mode = RefreshMode::Incremental;
    bad.dataset.request = docspec::model::OperationRequest::Refresh(Box::new(
        bad.dataset.related.as_ref().unwrap().request.clone(),
    ));
    let bad = forged(root.path(), "unsupported-mode", bad);
    assert_eq!(
        docspec::engine::verify_applied_changes(&noop, &bad, &applied, limits())
            .await
            .unwrap_err()
            .code(),
        "full_refresh_required"
    );
    let mut bad = certified.changes;
    bad.source.as_mut().unwrap().base_manifest =
        forged(root.path(), "base-alias", first.clone()).manifest;
    assert_eq!(
        docspec::engine::verify_applied_changes(&first, &noop, &bad, limits())
            .await
            .unwrap_err()
            .code(),
        "invalid_changes"
    );
}

#[tokio::test]
async fn incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let unchanged = next(&first, &left, &right, "noop");
    let right = update(
        &engine,
        root.path(),
        &right,
        "changed-right",
        &[row(Some(9), Some(9), "new matching value")],
        &[],
    )
    .await;
    drop(engine);
    let mut too_large = limits();
    // Other tests can free space between this setup and the actual preflight.
    // Exceed total capacity so that unrelated cleanup cannot satisfy the request.
    too_large.scratch_bytes = fs2::total_space(root.path())
        .unwrap()
        .checked_add(1)
        .unwrap();
    let engine = Engine::open(root.path().join("workspace"), too_large)
        .await
        .unwrap();
    // Exact value adoption needs no payload spill allowance, even while a newer
    // producer selection exists under the declared captured-input policy.
    let adopted = engine.refresh(&unchanged).await.unwrap();
    assert_eq!(
        adopted.dataset.groups["source"],
        first.dataset.groups["source"]
    );
    let spec = next(&adopted, &left, &right, "changed");
    assert_eq!(
        engine.refresh(&spec).await.unwrap_err().code(),
        "disk_capacity"
    );
    assert!(
        engine
            .plan_refresh(&spec)
            .await
            .unwrap()
            .reusable_phases
            .iter()
            .any(|phase| phase == "admission")
    );
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), adopted);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.refresh(&spec).await.unwrap();
    assert_ne!(
        release.dataset.groups["source"],
        first.dataset.groups["source"]
    );
    check(
        &engine,
        &release,
        &oracle(&left, &right, &join(&spec).join).await,
    )
    .await;
    verify_release(&release, limits()).await.unwrap();
}
