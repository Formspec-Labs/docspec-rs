//! Refresh scope follows applied rows across shared files and retained history.
// Rust guideline compliant 2026-02-21

use super::*;
use docspec::model::TablePin;

fn row(a: Option<i64>, b: Option<i64>, value: &str) -> Row {
    (a, b, Some(value.into()))
}

async fn engine(root: &Path) -> Engine {
    let mut profile = limits();
    // Keep the small multi-key fixture in one physical input/output file.
    profile.batch_rows = 64;
    Engine::open(root.join("workspace"), profile).await.unwrap()
}

fn assert_one_file(pin: &TablePin) {
    assert_eq!(
        pin.objects
            .iter()
            .filter(|object| object.path.ends_with(".parquet"))
            .count(),
        1,
        "the initial unrelated keys must share a physical file"
    );
}

async fn origins(pin: &TablePin, field: &str, value: Option<i64>) -> Vec<Location> {
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(pin, &budget).await.unwrap();

    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for index in 0..batch.num_rows() {
            if number(&batch, field, index) == value {
                result.push((
                    text(&batch, storage::OCCURRENCE_FILE, index).unwrap(),
                    number(&batch, storage::OCCURRENCE_ROW, index).unwrap(),
                ));
            }
        }
    }
    result.sort();
    assert!(!result.is_empty(), "the unchanged scope must have outputs");
    result
}

async fn joined_origins(release: &ReleasePin, label: &str) -> Vec<Location> {
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(&release.dataset.groups["source"], &budget)
        .await
        .unwrap();

    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for index in 0..batch.num_rows() {
            if text(&batch, "left_text", index).as_deref() == Some(label) {
                result.push((
                    text(&batch, storage::OCCURRENCE_FILE, index).unwrap(),
                    number(&batch, storage::OCCURRENCE_ROW, index).unwrap(),
                ));
            }
        }
    }
    result.sort();
    assert!(!result.is_empty(), "the unchanged scope must have outputs");
    result
}

fn next_join(
    base: &ReleasePin,
    left: &ReleasePin,
    right: &ReleasePin,
    operation: &str,
) -> RefreshSpec {
    let mut spec = base.dataset.related.as_ref().unwrap().request.clone();
    spec.base = Some(base.id.clone());
    spec.operation = id(operation);
    spec.mode = RefreshMode::Incremental;
    join_mut(&mut spec).left = left.version_ref();
    join_mut(&mut spec).right = right.version_ref();
    spec
}

#[tokio::test]
async fn join_history_narrows_rewritten_files_without_cancelling_same_key_edits() {
    let root = TempDir::new().unwrap();
    let engine = engine(root.path()).await;
    let left = source(
        &engine,
        root.path(),
        "left",
        "initial",
        None,
        &[
            row(Some(1), Some(1), "old"),
            row(Some(1), Some(1), "old"),
            row(Some(2), Some(2), "stable"),
            row(Some(3), Some(3), "move"),
            row(Some(4), Some(4), "waiting"),
            row(None, Some(2), "stable-null"),
        ],
    )
    .await;
    let right = source(
        &engine,
        root.path(),
        "right",
        "initial",
        None,
        &[
            row(Some(1), Some(1), "old-partner"),
            row(Some(2), Some(2), "stable-partner"),
            row(Some(3), Some(3), "old-move-partner"),
            row(None, Some(2), "null-partner"),
        ],
    )
    .await;
    assert_one_file(&left.dataset.groups["source"]);
    assert_one_file(&right.dataset.groups["source"]);
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let stable = joined_origins(&first, "stable").await;
    let stable_null = joined_origins(&first, "stable-null").await;
    let left = incremental::update(
        &engine,
        root.path(),
        &left,
        "value-edit",
        &[
            row(Some(1), Some(1), "edited"),
            row(Some(1), Some(1), "edited"),
        ],
        &[],
    )
    .await;
    let left = incremental::update(
        &engine,
        root.path(),
        &left,
        "move",
        &[row(Some(7), Some(7), "moved")],
        &[row(Some(3), Some(3), "")],
    )
    .await;
    let right = incremental::update(
        &engine,
        root.path(),
        &right,
        "value-edit",
        &[row(Some(1), Some(1), "edited-partner")],
        &[],
    )
    .await;
    let right = incremental::update(
        &engine,
        root.path(),
        &right,
        "new-matches",
        &[
            row(Some(4), Some(4), "new-partner"),
            row(Some(4), Some(4), "duplicate-partner"),
            row(Some(7), Some(7), "moved-partner"),
        ],
        &[],
    )
    .await;
    let spec = next_join(&first, &left, &right, "multi-hop");
    let refreshed = engine.refresh(&spec).await.unwrap();
    let expected = oracle(&left, &right, &join(&spec).join).await;
    assert_eq!(
        expected
            .iter()
            .filter(|r| r.2.as_deref() == Some("edited"))
            .count(),
        2
    );
    assert_eq!(
        expected
            .iter()
            .filter(|r| r.2.as_deref() == Some("waiting") && r.1.is_some())
            .count(),
        2
    );
    assert!(!expected.iter().any(|r| r.2.as_deref() == Some("move")));
    assert!(
        expected
            .iter()
            .any(|r| r.2.as_deref() == Some("moved") && r.3.as_deref() == Some("moved-partner"))
    );
    check(&engine, &refreshed, &expected).await;
    assert_eq!(joined_origins(&refreshed, "stable").await, stable);
    assert_eq!(joined_origins(&refreshed, "stable-null").await, stable_null);
    let mut full = recipe(&left, &right);
    full.dataset = id("full-join");
    let rebuilt = engine.refresh(&full).await.unwrap();
    check(&engine, &rebuilt, &expected).await;
}

#[tokio::test]
async fn summary_history_preserves_untouched_groups_and_recomputes_same_group_measures() {
    let root = TempDir::new().unwrap();
    let engine = engine(root.path()).await;
    let input = source(
        &engine,
        root.path(),
        "numbers",
        "initial",
        None,
        &[
            row(Some(1), Some(10), "old"),
            row(Some(1), Some(10), "old"),
            row(Some(2), Some(20), "stable"),
            row(Some(3), Some(30), "move"),
            row(None, None, "stable-null"),
        ],
    )
    .await;
    assert_one_file(&input.dataset.groups["source"]);
    let first = engine
        .refresh(&aggregation::summary(&input, false))
        .await
        .unwrap();
    let stable = origins(&first.dataset.groups["source"], "a", Some(2)).await;
    let stable_null = origins(&first.dataset.groups["source"], "a", None).await;
    // Changing b preserves the grouping key a but must change its measures.
    let input = incremental::update(
        &engine,
        root.path(),
        &input,
        "measure-edit",
        &[
            row(Some(1), Some(12), "edited"),
            row(Some(1), Some(12), "edited"),
        ],
        &[row(Some(1), Some(10), "")],
    )
    .await;
    // A second hop replaces those occurrences at the same complete key and moves a group.
    let input = incremental::update(
        &engine,
        root.path(),
        &input,
        "same-key-and-move",
        &[
            row(Some(1), Some(12), "edited-again"),
            row(Some(1), Some(12), "edited-again"),
            row(Some(7), Some(30), "moved"),
        ],
        &[row(Some(3), Some(30), "")],
    )
    .await;
    let spec = aggregation::next(&first, &input, "multi-hop");
    let refreshed = engine.refresh(&spec).await.unwrap();
    aggregation::check_summary(&engine, &refreshed, &input, false).await;
    let values = aggregation::values(&engine, &refreshed, false).await;
    assert_eq!(values[&Some(1)], (2, 2, Some(24), Some(12), Some(12)));
    assert!(!values.contains_key(&Some(3)));
    assert_eq!(values[&Some(7)], (1, 1, Some(30), Some(30), Some(30)));
    assert_eq!(
        origins(&refreshed.dataset.groups["source"], "a", Some(2)).await,
        stable
    );
    assert_eq!(
        origins(&refreshed.dataset.groups["source"], "a", None).await,
        stable_null
    );
    let mut full = aggregation::summary(&input, false);
    full.dataset = id("full-summary");
    let rebuilt = engine.refresh(&full).await.unwrap();
    aggregation::check_summary(&engine, &rebuilt, &input, false).await;
    assert_eq!(aggregation::values(&engine, &rebuilt, false).await, values);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn completed_processing_resumes_after_optional_input_history_retirement() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let right = incremental::update(
        &engine,
        root.path(),
        &right,
        "new-match",
        &[row(Some(9), Some(9), "new-partner")],
        &[],
    )
    .await;
    let certified = engine
        .change_certificate(&right.version_ref())
        .await
        .unwrap()
        .unwrap();
    let spec = next_join(&first, &left, &right, "interrupted");
    assert_eq!(
        engine
            .refresh_until(&spec, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let completed = engine.inspect(&spec.dataset).await.unwrap();
    let processing = completed
        .iter()
        .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
        .filter(|(_, value)| {
            let phase: docspec::model::PhaseResult =
                serde_json::from_value((*value).clone()).unwrap();
            phase.operation == spec.operation && phase.phase == "processing"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        processing.len(),
        1,
        "retain the interrupted operation's exact processing record"
    );
    let retirement = engine
        .plan_change_retirement(
            &right.dataset.dataset,
            &id("retire-optional"),
            std::slice::from_ref(&right.id),
            "end optional history after consumer processing completes",
        )
        .await
        .unwrap();
    engine.retire_changes(&retirement).await.unwrap();
    assert!(
        engine
            .change_certificate(&right.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    // Only the retired optional descriptor is damaged; captured endpoints remain intact.
    std::fs::write(
        &certified.certificate.descriptor.path,
        b"retired optional history",
    )
    .unwrap();
    let plan = engine.plan_refresh(&spec).await.unwrap();
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
    let refreshed = engine.refresh(&spec).await.unwrap();
    let resumed = engine.inspect(&spec.dataset).await.unwrap();
    for (key, value) in processing {
        assert_eq!(resumed.get(key), Some(value));
    }
    check(
        &engine,
        &refreshed,
        &oracle(&left, &right, &join(&spec).join).await,
    )
    .await;
    assert_eq!(engine.refresh(&spec).await.unwrap(), refreshed);
}

#[tokio::test]
async fn invalid_active_history_refuses_but_retired_history_falls_back_on_same_operation() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let right = incremental::update(
        &engine,
        root.path(),
        &right,
        "new-match",
        &[row(Some(9), Some(9), "new-partner")],
        &[],
    )
    .await;
    let certified = engine
        .change_certificate(&right.version_ref())
        .await
        .unwrap()
        .unwrap();
    let descriptor = &certified.certificate.descriptor.path;
    let retained = std::fs::read(descriptor).unwrap();
    let spec = next_join(&first, &left, &right, "captured-fallback");
    std::fs::write(descriptor, b"damaged active history").unwrap();
    assert_eq!(
        engine.refresh(&spec).await.unwrap_err().code(),
        "corrupt_object"
    );
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), first);
    let refused = engine.inspect(&spec.dataset).await.unwrap();
    assert!(
        !refused
            .iter()
            .filter(|(key, _)| key.starts_with("phase/refresh/") && key.ends_with("/processing"))
            .any(|(_, value)| {
                let phase: docspec::model::PhaseResult =
                    serde_json::from_value(value.clone()).unwrap();
                phase.operation == spec.operation
            }),
        "invalid history must not create a reusable processing completion"
    );
    // Restore the test-owned object before ending its optional history lifetime.
    std::fs::write(descriptor, &retained).unwrap();
    let retirement = engine
        .plan_change_retirement(
            &right.dataset.dataset,
            &id("retire-before-processing"),
            std::slice::from_ref(&right.id),
            "end optional history before retrying captured processing",
        )
        .await
        .unwrap();
    engine.retire_changes(&retirement).await.unwrap();
    assert!(
        engine
            .change_certificate(&right.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    let plan = engine.plan_refresh(&spec).await.unwrap();
    assert!(
        plan.required_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert!(
        !plan
            .reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let refreshed = engine.refresh(&spec).await.unwrap();
    check(
        &engine,
        &refreshed,
        &oracle(&left, &right, &join(&spec).join).await,
    )
    .await;
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), refreshed);
    assert_eq!(engine.refresh(&spec).await.unwrap(), refreshed);
}
