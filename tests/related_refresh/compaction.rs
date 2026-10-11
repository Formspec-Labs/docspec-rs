//! Incremental related results retain captured compacted output representations.
// Rust guideline compliant 2026-02-21

use super::*;
use docspec::model::{CompactionResult, CompactionSpec, TablePin};
#[cfg(feature = "test-util")]
use docspec::model::{NativeSnapshotRef, PhaseResult};
use std::collections::BTreeSet;

async fn unaffected_occurrences(
    pin: &TablePin,
    summary: bool,
    affected_label: &str,
) -> Vec<Location> {
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(pin, &budget).await.unwrap();

    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut retained = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for index in 0..batch.num_rows() {
            let affected = if summary {
                number(&batch, "a", index) == Some(10)
            } else {
                text(&batch, "left_text", index).as_deref() == Some(affected_label)
            };
            if !affected {
                retained.push((
                    text(&batch, storage::OCCURRENCE_FILE, index).unwrap(),
                    number(&batch, storage::OCCURRENCE_ROW, index).unwrap(),
                ));
            }
        }
    }
    retained.sort();
    assert!(!retained.is_empty());
    retained
}

#[tokio::test]
async fn edits_inside_compacted_related_files_preserve_untouched_occurrences_and_public_changes() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    use docspec::{engine::verify_release_at, model::VerificationLevel};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    for summary in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, left, right, base, compacted) = prepared(root.path(), summary).await;
        assert_eq!(compacted.output_files.len(), 1);
        // Read the actual rewritten file to establish that affected key 10 and
        // untouched key 20 share its physical payload before the edit.
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(&compacted.output_files[0]).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let mut affected = false;
        let mut untouched = false;
        for batch in reader {
            let batch = batch.unwrap();
            for index in 0..batch.num_rows() {
                if summary {
                    affected |= number(&batch, "a", index) == Some(10);
                    untouched |= number(&batch, "a", index) == Some(20);
                } else {
                    affected |= text(&batch, "left_text", index).as_deref() == Some("stable-ten");
                    untouched |=
                        text(&batch, "left_text", index).as_deref() == Some("stable-twenty");
                }
            }
        }
        assert!(
            affected && untouched,
            "the edited and retained scopes must share the compacted file"
        );
        let retained = unaffected_occurrences(&compacted.replacement, summary, "stable-ten").await;
        let replacement = (Some(10), Some(10), Some("changed-ten".into()));
        let left = incremental::update(
            &engine,
            root.path(),
            &left,
            "inside-edit",
            &[replacement.clone(), replacement],
            &[],
        )
        .await;
        let refreshed = engine
            .refresh(&next(&base, &left, &right, "inside-edit"))
            .await
            .unwrap();
        let retained_after =
            unaffected_occurrences(&refreshed.dataset.groups["source"], summary, "changed-ten")
                .await;
        assert_eq!(retained_after, retained);
        assert!(
            !physical_files(&refreshed.dataset.groups["source"], &pin_budget)
                .await
                .contains(&compacted.output_files[0])
        );
        check_related(&engine, &refreshed, &left, &right).await;
        let report = verify_release_at(&refreshed, limits(), VerificationLevel::Deep)
            .await
            .unwrap();
        assert_eq!(report.level, VerificationLevel::Deep);
        let names: &[&str] = if summary {
            &["a", "rows", "count", "sum", "min", "max"]
        } else {
            &["left_text", "right_text"]
        };
        forward_changes::check_forward(&engine, &base, &refreshed, names).await;
    }
}

fn row(value: &str) -> Row {
    (Some(50), Some(50), Some(value.into()))
}

async fn physical_files(pin: &TablePin, pin_budget: &docspec::budget::Budget) -> BTreeSet<String> {
    let native = storage::open_pin(pin, pin_budget).await.unwrap();
    native
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .map_ok(|task| task.data_file_path.clone())
        .try_collect()
        .await
        .unwrap()
}

fn next(base: &ReleasePin, left: &ReleasePin, right: &ReleasePin, operation: &str) -> RefreshSpec {
    if matches!(
        base.dataset.related.as_ref().unwrap().request.transform,
        RefreshTransform::Aggregate(_)
    ) {
        return aggregation::next(base, left, operation);
    }
    let mut spec = base.dataset.related.as_ref().unwrap().request.clone();
    spec.base = Some(base.id.clone());
    spec.operation = id(operation);
    spec.mode = RefreshMode::Incremental;
    join_mut(&mut spec).left = left.version_ref();
    join_mut(&mut spec).right = right.version_ref();
    spec
}

async fn check_related(
    engine: &Engine,
    release: &ReleasePin,
    left: &ReleasePin,
    right: &ReleasePin,
) {
    match &release.dataset.related.as_ref().unwrap().request.transform {
        RefreshTransform::Join(join) => {
            check(engine, release, &oracle(left, right, &join.join).await).await;
        }
        RefreshTransform::Aggregate(_) => {
            aggregation::check_summary(engine, release, left, false).await;
        }
    }
}

async fn compact(
    engine: &Engine,
    base: &ReleasePin,
    operation: &str,
    files: Vec<String>,
) -> CompactionResult {
    engine
        .compact(&CompactionSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id(operation),
            version: base.id.clone(),
            group: "source".into(),
            files,
            target_file_bytes: 16 << 20,
        })
        .await
        .unwrap()
}

async fn prepared(
    root: &Path,
    summary: bool,
) -> (Engine, ReleasePin, ReleasePin, ReleasePin, CompactionResult) {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    let (engine, mut left, mut right) = fixture(root).await;
    let spec = if summary {
        aggregation::summary(&left, false)
    } else {
        recipe(&left, &right)
    };
    let mut initial = engine.refresh(&spec).await.unwrap();
    // Separate incremental appends create real files without relying on batch
    // size or writer rollover to split a tiny fixture.
    for (key, operation) in [(10, "stable-ten"), (20, "stable-twenty")] {
        let stable = (Some(key), Some(key), Some(operation.into()));
        left = incremental::update(
            &engine,
            root,
            &left,
            operation,
            std::slice::from_ref(&stable),
            &[],
        )
        .await;
        right = incremental::update(
            &engine,
            root,
            &right,
            operation,
            std::slice::from_ref(&stable),
            &[],
        )
        .await;
        initial = engine
            .refresh(&next(&initial, &left, &right, operation))
            .await
            .unwrap();
    }
    let original = physical_files(&initial.dataset.groups["source"], &pin_budget).await;
    assert!(
        original.len() > 1,
        "partial compaction needs multiple original files"
    );
    let left = incremental::update(&engine, root, &left, "append", &[row("tail")], &[]).await;
    let right = incremental::update(&engine, root, &right, "append", &[row("partner")], &[]).await;
    let base = engine
        .refresh(&next(&initial, &left, &right, "append"))
        .await
        .unwrap();
    let all = physical_files(&base.dataset.groups["source"], &pin_budget).await;
    assert!(original.is_subset(&all));
    assert!(
        all.len() > original.len(),
        "the changed key needs a separate output file"
    );
    let compacted = compact(
        &engine,
        &base,
        "partial-layout",
        original.into_iter().collect(),
    )
    .await;
    assert!(compacted.verified);
    assert!(compacted.input_files.len() > compacted.output_files.len());
    assert_eq!(
        compacted.replacement.logical_metadata(),
        base.dataset.groups["source"].logical_metadata()
    );
    check_related(&engine, &base, &left, &right).await;
    (engine, left, right, base, compacted)
}

#[tokio::test]
async fn partial_compaction_is_shared_by_disjoint_refresh_and_noop_advances_coverage() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    for summary in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, mut left, mut right, mut base, compacted) =
            prepared(root.path(), summary).await;
        let retained: Vec<_> = compacted
            .output_files
            .iter()
            .map(|path| {
                (
                    path.clone(),
                    storage::digest(Path::new(path)).unwrap(),
                    std::fs::metadata(path).unwrap().modified().unwrap(),
                )
            })
            .collect();
        // Original null and duplicate groups remain unchanged. The separate tail
        // loses its last partner, then gains duplicate partners on the next step.
        for (operation, partners) in [
            ("remove-partner", vec![]),
            ("new-partners", vec![row("partner-one"), row("partner-two")]),
        ] {
            left = incremental::update(
                &engine,
                root.path(),
                &left,
                operation,
                &[row(operation)],
                &[],
            )
            .await;
            let deleted = if partners.is_empty() {
                vec![row("deleted")]
            } else {
                vec![]
            };
            right =
                incremental::update(&engine, root.path(), &right, operation, &partners, &deleted)
                    .await;
            base = engine
                .refresh(&next(&base, &left, &right, operation))
                .await
                .unwrap();
            check_related(&engine, &base, &left, &right).await;
            let files = physical_files(&base.dataset.groups["source"], &pin_budget).await;
            for (path, digest, modified) in &retained {
                assert!(files.contains(path), "reuse the unaffected compacted file");
                assert_eq!(storage::digest(Path::new(path)).unwrap(), *digest);
                assert_eq!(
                    std::fs::metadata(path).unwrap().modified().unwrap(),
                    *modified
                );
            }
        }
        let current = compact(&engine, &base, "noop-layout", vec![]).await;
        left = incremental::update(&engine, root.path(), &left, "noop", &[], &[]).await;
        let noop = engine
            .refresh(&next(&base, &left, &right, "noop"))
            .await
            .unwrap();
        assert_eq!(noop.dataset.groups["source"], current.replacement);
        assert_eq!(
            noop.dataset.related.as_ref().unwrap().coverage.inputs[&left.dataset.dataset],
            left.version_ref()
        );
        let certificate = engine
            .change_certificate(&noop.version_ref())
            .await
            .unwrap()
            .unwrap();
        let changes = certificate.changes.source.as_ref().unwrap();
        assert_eq!((changes.removed.rows, changes.added.rows), (0, 0));
        check_related(&engine, &noop, &left, &right).await;
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn admitted_related_refresh_resumes_exact_output_after_later_compaction_and_reopen() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    for summary in [false, true] {
        for stop in ["admission", "processing"] {
            let root = TempDir::new().unwrap();
            let (engine, left, right, base, captured) = prepared(root.path(), summary).await;
            let left =
                incremental::update(&engine, root.path(), &left, "edit", &[row("changed")], &[])
                    .await;
            let spec = next(&base, &left, &right, "interrupted");
            assert_eq!(
                engine.refresh_until(&spec, stop).await.unwrap_err().code(),
                "injected_failure"
            );
            let retained = engine.inspect(&spec.dataset).await.unwrap();
            let admission = retained["request-dependencies/interrupted"].clone();
            assert_eq!(
                serde_json::from_value::<TablePin>(admission["update_source"].clone()).unwrap(),
                captured.replacement
            );
            if stop == "admission" {
                // This separate catalog has only the admission record. Neither
                // selection nor a compaction completion can protect these objects.
                let inspection_root = root.path().join("inspection");
                let inspector = Engine::open(&inspection_root, limits()).await.unwrap();
                let lifecycle = docspec::catalog::Lifecycle::new(
                    storage::local_catalog(&inspection_root).await.unwrap(),
                    Budget::new(limits()).unwrap(),
                );
                let view = lifecycle.open(&id("isolated")).await.unwrap();
                lifecycle
                    .commit(
                        &view,
                        vec![
                            docspec::catalog::record(
                                "request-dependencies/interrupted",
                                &admission,
                            )
                            .unwrap(),
                        ],
                    )
                    .await
                    .unwrap();
                let protection = inspector
                    .inspect_retention(&captured.replacement.objects)
                    .await
                    .unwrap();
                assert!(protection.objects.iter().all(|object| {
                    object
                        .blocker
                        .as_ref()
                        .is_some_and(|blocker| blocker.key == "request-dependencies/interrupted")
                }));
            }
            let later = compact(&engine, &base, "later-layout", vec![]).await;
            assert_ne!(later.replacement.metadata, captured.replacement.metadata);
            drop(engine);
            let engine = Engine::open(root.path().join("workspace"), limits())
                .await
                .unwrap();
            let refreshed = engine.refresh(&spec).await.unwrap();
            let resumed = engine.inspect(&spec.dataset).await.unwrap();
            assert_eq!(resumed["request-dependencies/interrupted"], admission);
            for (key, value) in retained.iter().filter(|(key, _)| key.starts_with("phase/")) {
                assert_eq!(resumed[key], *value, "preserve completed phase {key}");
            }
            let processing = resumed
                .iter()
                .filter(|(key, _)| key.starts_with("phase/refresh/"))
                .filter_map(|(_, value)| serde_json::from_value::<PhaseResult>(value.clone()).ok())
                .find(|phase| phase.operation == spec.operation && phase.phase == "processing")
                .unwrap();
            assert!(
                processing
                    .dependencies
                    .unwrap()
                    .tables
                    .contains(&NativeSnapshotRef::from(&captured.replacement))
            );
            let files = physical_files(&refreshed.dataset.groups["source"], &pin_budget).await;
            assert!(
                captured
                    .output_files
                    .iter()
                    .all(|file| files.contains(file))
            );
            check_related(&engine, &refreshed, &left, &right).await;
        }
    }
}

#[tokio::test]
async fn global_summary_compaction_preserves_empty_nonempty_and_noop_transitions() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut input = source(&engine, root.path(), "input", "initial", None, &[]).await;
    let mut result = engine
        .refresh(&aggregation::summary(&input, true))
        .await
        .unwrap();
    let duplicate = (Some(1), Some(8), None);
    let null = (None, None, Some("null".into()));
    for (operation, replacements, deletions) in [
        ("empty-noop", vec![], vec![]),
        (
            "append",
            vec![duplicate.clone(), duplicate.clone(), null.clone()],
            vec![],
        ),
        ("nonempty-noop", vec![], vec![]),
        ("remove-all", vec![], vec![duplicate, null]),
        ("empty-noop-again", vec![], vec![]),
    ] {
        aggregation::check_summary(&engine, &result, &input, true).await;
        let layout = compact(&engine, &result, &format!("layout-{operation}"), vec![]).await;
        assert!(layout.verified);
        input = incremental::update(
            &engine,
            root.path(),
            &input,
            operation,
            &replacements,
            &deletions,
        )
        .await;
        let refreshed = engine
            .refresh(&aggregation::next(&result, &input, operation))
            .await
            .unwrap();
        aggregation::check_summary(&engine, &refreshed, &input, true).await;
        assert_eq!(
            refreshed.dataset.related.as_ref().unwrap().coverage.inputs[&input.dataset.dataset],
            input.version_ref()
        );
        if operation.contains("noop") {
            assert_eq!(refreshed.dataset.groups["source"], layout.replacement);
            let changes = engine
                .change_certificate(&refreshed.version_ref())
                .await
                .unwrap()
                .unwrap()
                .changes
                .source
                .unwrap();
            assert_eq!((changes.removed.rows, changes.added.rows), (0, 0));
        }
        result = refreshed;
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn equivalent_new_admission_reuses_processing_from_the_prior_output_layout() {
    for summary in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, left, right, base, first) = prepared(root.path(), summary).await;
        let left =
            incremental::update(&engine, root.path(), &left, "edit", &[row("changed")], &[]).await;
        let original = next(&base, &left, &right, "original");
        assert_eq!(
            engine
                .refresh_until(&original, "processing")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let retained = engine.inspect(&original.dataset).await.unwrap();
        let (key, record) = retained
            .iter()
            .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
            .find(|(_, value)| {
                let phase: PhaseResult = serde_json::from_value((*value).clone()).unwrap();
                phase.operation == original.operation
            })
            .unwrap();
        let processing: PhaseResult = serde_json::from_value(record.clone()).unwrap();
        assert!(
            processing
                .dependencies
                .as_ref()
                .unwrap()
                .tables
                .contains(&NativeSnapshotRef::from(&first.replacement))
        );
        engine
            .abandon(
                &original.dataset,
                &original.operation,
                "replace the operation after its processing completes",
            )
            .await
            .unwrap();
        let second = compact(&engine, &base, "equivalent-layout", vec![]).await;
        assert_ne!(first.replacement.metadata, second.replacement.metadata);
        let mut replacement = original.clone();
        replacement.operation = id("replacement");
        assert_eq!(
            engine
                .refresh_until(&replacement, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let admitted = engine.inspect(&replacement.dataset).await.unwrap();
        assert_eq!(
            serde_json::from_value::<TablePin>(
                admitted["request-dependencies/replacement"]["update_source"].clone()
            )
            .unwrap(),
            second.replacement
        );
        let plan = engine.plan_refresh(&replacement).await.unwrap();
        assert!(
            plan.reusable_phases
                .iter()
                .any(|phase| phase == "processing")
        );
        assert!(
            !plan
                .required_phases
                .iter()
                .any(|phase| phase == "processing")
        );
        let result = engine.refresh(&replacement).await.unwrap();
        let resumed = engine.inspect(&replacement.dataset).await.unwrap();
        assert_eq!(
            resumed.get(key),
            Some(record),
            "reuse the exact prior completion and its physical dependencies"
        );
        assert_eq!(result.dataset.groups["source"], processing.table.unwrap());
        check_related(&engine, &result, &left, &right).await;
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn new_refresh_reads_selected_producer_layout_and_preserves_exact_contributors() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};

    for summary in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, left, right, base, _) = prepared(root.path(), summary).await;
        let selected = compact(&engine, &left, "producer-layout", vec![]).await;
        let right_selected = if summary {
            None
        } else {
            Some(compact(&engine, &right, "right-producer-layout", vec![]).await)
        };
        let mut spec = base.dataset.related.as_ref().unwrap().request.clone();
        spec.dataset = id("captured-producers");
        spec.operation = id("captured-inputs");
        spec.base = None;
        spec.mode = RefreshMode::Full;
        assert_eq!(
            engine
                .refresh_until(&spec, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let admitted = engine.inspect(&spec.dataset).await.unwrap();
        let captures = &admitted["request-dependencies/captured-inputs"]["input_sources"];
        assert_eq!(
            serde_json::from_value::<TablePin>(captures[left.dataset.dataset.as_str()].clone())
                .unwrap(),
            selected.replacement
        );
        let budget = Budget::new(limits()).unwrap();
        let canonical_files = physical_files(&left.dataset.groups["source"], &budget).await;
        let selected_files = physical_files(&selected.replacement, &budget).await;
        let original_file = canonical_files.difference(&selected_files).next().unwrap();
        // Preserve size/availability while making the original payload unreadable.
        // Processing must consume the captured replacement, not the old file.
        let original_bytes = std::fs::read(original_file).unwrap();
        std::fs::write(original_file, vec![0; original_bytes.len()]).unwrap();
        let processing = engine.refresh_until(&spec, "processing").await;
        std::fs::write(original_file, original_bytes).unwrap();
        assert_eq!(processing.unwrap_err().code(), "injected_failure");
        let release = engine.refresh(&spec).await.unwrap();
        let related = release.dataset.related.as_ref().unwrap();
        assert_eq!(related.coverage.inputs, spec.inputs());
        assert_eq!(
            related.input_sources[&left.dataset.dataset],
            selected.replacement
        );
        if let Some(right_selected) = right_selected {
            assert_eq!(
                related.input_sources[&right.dataset.dataset],
                right_selected.replacement
            );
        }
        assert!(
            selected
                .output_files
                .iter()
                .all(|file| selected_files.contains(file))
        );
        check_related(&engine, &release, &left, &right).await;
        verify_release_at(&release, limits(), VerificationLevel::Deep)
            .await
            .unwrap();
        // A rehashed result cannot certify changed producer values by assigning
        // them the old logical metadata identity and preserving the row count.
        let changed = incremental::update(
            &engine,
            root.path(),
            &left,
            "false-equivalence",
            &[(Some(10), Some(10), Some("false-equivalence".into()))],
            &[],
        )
        .await;
        let mut false_source = changed.dataset.groups["source"].clone();
        assert_eq!(false_source.rows, selected.replacement.rows);
        false_source.logical_metadata =
            Some(left.dataset.groups["source"].logical_metadata().clone());
        let mut altered = release.clone();
        altered
            .dataset
            .related
            .as_mut()
            .unwrap()
            .input_sources
            .insert(left.dataset.dataset.clone(), false_source);
        let altered = forged(root.path(), "false-input-layout", altered);
        assert_eq!(
            verify_release_at(&altered, limits(), VerificationLevel::Deep)
                .await
                .unwrap_err()
                .code(),
            "invalid_related_result"
        );
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn completed_refresh_keeps_its_producer_layout_after_later_compaction_and_new_admission() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};

    for summary in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, left, right, base, _) = prepared(root.path(), summary).await;
        let left = incremental::update(
            &engine,
            root.path(),
            &left,
            "producer-edit",
            &[row("producer-changed")],
            &[],
        )
        .await;
        let first = compact(&engine, &left, "first-producer-layout", vec![]).await;
        let original = next(&base, &left, &right, "original-input-capture");
        assert_eq!(
            engine
                .refresh_until(&original, "processing")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let retained = engine.inspect(&original.dataset).await.unwrap();
        let (key, record) = retained
            .iter()
            .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
            .find(|(_, value)| {
                serde_json::from_value::<PhaseResult>((*value).clone())
                    .unwrap()
                    .operation
                    == original.operation
            })
            .unwrap();
        let processing: PhaseResult = serde_json::from_value(record.clone()).unwrap();
        assert_eq!(
            processing.dependencies.as_ref().unwrap().input_sources[&left.dataset.dataset],
            first.replacement
        );
        // A separate catalog retains only the completed phase. It must protect
        // the consumed producer representation without its owner's selection.
        let inspection_root = root.path().join("input-inspection");
        let inspector = Engine::open(&inspection_root, limits()).await.unwrap();
        let lifecycle = docspec::catalog::Lifecycle::new(
            storage::local_catalog(&inspection_root).await.unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&id("isolated-inputs")).await.unwrap();
        lifecycle
            .commit(
                &view,
                vec![docspec::catalog::record(key, &processing).unwrap()],
            )
            .await
            .unwrap();
        let protected = inspector
            .inspect_retention(&first.replacement.objects)
            .await
            .unwrap();
        assert!(protected.objects.iter().all(|object| {
            object
                .blocker
                .as_ref()
                .is_some_and(|blocker| blocker.key == *key)
        }));
        engine
            .abandon(
                &original.dataset,
                &original.operation,
                "reuse completed processing with a new operation",
            )
            .await
            .unwrap();
        let second = compact(&engine, &left, "later-producer-layout", vec![]).await;
        assert_ne!(first.replacement.metadata, second.replacement.metadata);
        let mut replacement = original.clone();
        replacement.operation = id("replacement-input-capture");
        assert_eq!(
            engine
                .refresh_until(&replacement, "admission")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let admitted = engine.inspect(&replacement.dataset).await.unwrap();
        assert_eq!(
            serde_json::from_value::<TablePin>(
                admitted["request-dependencies/replacement-input-capture"]["input_sources"]
                    [left.dataset.dataset.as_str()]
                .clone()
            )
            .unwrap(),
            second.replacement
        );
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let result = engine.refresh(&replacement).await.unwrap();
        let resumed = engine.inspect(&replacement.dataset).await.unwrap();
        assert_eq!(resumed.get(key), Some(record));
        assert_eq!(result.dataset.groups["source"], processing.table.unwrap());
        assert_eq!(
            result.dataset.related.as_ref().unwrap().input_sources[&left.dataset.dataset],
            first.replacement
        );
        check_related(&engine, &result, &left, &right).await;
        verify_release_at(&result, limits(), VerificationLevel::Deep)
            .await
            .unwrap();
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn legacy_summary_processing_resumes_canonical_inputs_without_recomputation() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};

    for incremental in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, mut left, _) = fixture(root.path()).await;
        let spec = if incremental {
            let base = engine
                .refresh(&aggregation::summary(&left, false))
                .await
                .unwrap();
            left = incremental::update(
                &engine,
                root.path(),
                &left,
                "legacy-producer-edit",
                &[row("legacy-change"), row("legacy-change")],
                &[],
            )
            .await;
            aggregation::next(&base, &left, "legacy-summary")
        } else {
            let mut spec = aggregation::summary(&left, false);
            spec.operation = id("legacy-summary");
            spec
        };
        assert_eq!(
            engine
                .refresh_until(&spec, "processing")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let retained = engine.inspect(&spec.dataset).await.unwrap();
        let (key, value) = retained
            .iter()
            .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
            .find(|(_, value)| {
                serde_json::from_value::<PhaseResult>((*value).clone())
                    .unwrap()
                    .operation
                    == spec.operation
            })
            .unwrap();
        let mut processing: PhaseResult = serde_json::from_value(value.clone()).unwrap();
        assert!(
            processing
                .dependencies
                .as_ref()
                .unwrap()
                .input_sources
                .is_empty()
        );
        let exact_table = processing.table.clone().unwrap();
        processing.dependencies = None;
        // Retire the protected generation before installing the old record shape,
        // keeping its original admission older than this processing completion.
        let retirement = engine
            .plan_phase_retirement(
                &spec.dataset,
                &id("legacy-processing-shape"),
                std::slice::from_ref(key),
                "qualify older aggregate recovery records",
            )
            .await
            .unwrap();
        engine.retire_phases(&retirement).await.unwrap();
        let lifecycle = docspec::catalog::Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        lifecycle
            .commit(
                &view,
                vec![docspec::catalog::record(key, &processing).unwrap()],
            )
            .await
            .unwrap();
        let later = compact(&engine, &left, "later-legacy-producer-layout", vec![]).await;
        let plan = engine.plan_refresh(&spec).await.unwrap();
        assert!(
            plan.reusable_phases
                .iter()
                .any(|phase| phase == "processing")
        );
        let original_files = later
            .output_files
            .iter()
            .map(|path| {
                let bytes = std::fs::read(path).unwrap();
                std::fs::write(path, vec![0; bytes.len()]).unwrap();
                (path, bytes)
            })
            .collect::<Vec<_>>();
        // Its original canonical input still reads normally; the newer selected
        // layout cannot be used for evidence or resumed processing in this case.
        let resumed = engine.refresh(&spec).await;
        for (path, bytes) in original_files {
            std::fs::write(path, bytes).unwrap();
        }
        let result = resumed.unwrap();
        assert_eq!(result.dataset.groups["source"], exact_table);
        assert!(
            result
                .dataset
                .related
                .as_ref()
                .unwrap()
                .input_sources
                .is_empty()
        );
        let after = engine.inspect(&spec.dataset).await.unwrap();
        assert_eq!(after[key], serde_json::to_value(&processing).unwrap());
        aggregation::check_summary(&engine, &result, &left, false).await;
        verify_release_at(&result, limits(), VerificationLevel::Deep)
            .await
            .unwrap();
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn subsequent_incremental_refresh_uses_old_and_new_captured_producer_layouts() {
    use docspec::{engine::verify_release_at, model::VerificationLevel};

    for summary in [false, true] {
        for retired_history in [false, true] {
            let root = TempDir::new().unwrap();
            let (engine, left, right) = fixture(root.path()).await;
            let old_layout = compact(&engine, &left, "old-input-layout", vec![]).await;
            let initial = if summary {
                aggregation::summary(&left, false)
            } else {
                recipe(&left, &right)
            };
            let base = engine.refresh(&initial).await.unwrap();
            assert_eq!(
                base.dataset.related.as_ref().unwrap().input_sources[&left.dataset.dataset],
                old_layout.replacement
            );
            // Moving a duplicate key group also changes a null-key group. The
            // independent oracle checks departing, new and untouched contributors.
            let moved = (Some(30), Some(30), Some("moved-duplicate".into()));
            let null_neighbor = (None, Some(2), Some("changed-null".into()));
            let updated = incremental::update(
                &engine,
                root.path(),
                &left,
                "move-input-key",
                &[moved.clone(), moved, null_neighbor.clone(), null_neighbor],
                &[(Some(1), Some(2), None)],
            )
            .await;
            let new_layout = compact(&engine, &updated, "new-input-layout", vec![]).await;
            if retired_history {
                let retirement = engine
                    .plan_change_retirement(
                        &updated.dataset.dataset,
                        &id("retire-input-delta"),
                        std::slice::from_ref(&updated.id),
                        "qualify captured-layout discovery without optional deltas",
                    )
                    .await
                    .unwrap();
                engine.retire_changes(&retirement).await.unwrap();
            }
            assert_eq!(
                engine
                    .change_certificate(&updated.version_ref())
                    .await
                    .unwrap()
                    .is_none(),
                retired_history
            );
            let spec = next(&base, &updated, &right, "captured-input-successor");
            let successor = engine.refresh(&spec).await.unwrap();
            assert_eq!(
                successor.dataset.related.as_ref().unwrap().input_sources[&updated.dataset.dataset],
                new_layout.replacement
            );
            assert_eq!(
                successor.dataset.related.as_ref().unwrap().coverage.inputs,
                spec.inputs()
            );
            check_related(&engine, &successor, &updated, &right).await;
            let names: &[&str] = if summary {
                &["a", "rows", "count", "sum", "min", "max"]
            } else {
                &["left_text", "right_text"]
            };
            forward_changes::check_forward(&engine, &base, &successor, names).await;
            verify_release_at(&successor, limits(), VerificationLevel::Deep)
                .await
                .unwrap();
        }
    }
}
