//! Native joins checked against direct Rust matching and exact input occurrence pairs.

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::verify_release,
    model::{
        BackfillSpec, ComputedField, Id, JoinCardinality, JoinDefinition, JoinField, JoinKey,
        JoinKind, JoinSide, JoinTransform, NullMatching, ObjectRef, RefreshMode, RefreshPolicy,
        RefreshSpec, RefreshTransform, ReleasePin, RunSpec, SuccessionProof,
    },
    storage,
};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

#[cfg(feature = "test-util")]
use super::support;

type Row = (Option<i64>, Option<i64>, Option<String>);
type Location = (String, i64);
type Matched = (
    Option<Location>,
    Option<Location>,
    Option<String>,
    Option<String>,
);

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}
fn rows() -> (Vec<Row>, Vec<Row>) {
    (
        vec![
            (Some(1), Some(2), Some("duplicate".into())),
            (Some(1), Some(2), Some("duplicate".into())),
            (None, Some(2), Some("left-null".into())),
            (Some(1), None, None),
            (Some(9), Some(9), Some(String::new())),
        ],
        vec![
            (Some(1), Some(2), Some("right-one".into())),
            (Some(1), Some(2), Some("right-two".into())),
            (None, Some(2), Some("right-null".into())),
            (Some(1), None, Some("partial-null".into())),
            (None, None, Some("all-null".into())),
            (Some(8), Some(8), None),
        ],
    )
}
async fn source(
    engine: &Engine,
    root: &Path,
    dataset: &str,
    operation: &str,
    base: Option<Id>,
    rows: &[Row],
) -> ReleasePin {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.2.as_deref()),
            )),
        ],
    )
    .unwrap();
    let path = root.join(format!("{dataset}-{operation}.parquet"));
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    engine
        .run(&RunSpec {
            dataset: id(dataset),
            operation: id(operation),
            base,
            relationship: None,
            input: path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap()
}
async fn fixture(root: &Path) -> (Engine, ReleasePin, ReleasePin) {
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    let (left, right) = rows();
    let left = source(&engine, root, "comments", "initial", None, &left).await;
    let right = source(&engine, root, "dockets", "initial", None, &right).await;
    (engine, left, right)
}
fn recipe(left: &ReleasePin, right: &ReleasePin) -> RefreshSpec {
    RefreshSpec {
        dataset: id("joined"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
            join: JoinDefinition {
                keys: ["a", "b"]
                    .into_iter()
                    .map(|name| JoinKey {
                        left: name.into(),
                        right: name.into(),
                    })
                    .collect(),
                kind: JoinKind::Left,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
                fields: vec![
                    JoinField {
                        side: JoinSide::Left,
                        field: "text".into(),
                        name: "left_text".into(),
                    },
                    JoinField {
                        side: JoinSide::Right,
                        field: "text".into(),
                        name: "right_text".into(),
                    },
                ],
            },
        }),
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn abandoned_refresh_cannot_select_but_new_operation_reuses_its_result() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let mut request = recipe(&left, &right);
    assert_eq!(
        engine
            .refresh_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    let retained: ReleasePin =
        serde_json::from_value(before[&format!("readback/{}", request.operation)].clone()).unwrap();
    let phases: Vec<docspec::model::PhaseResult> = before
        .values()
        .filter_map(|value| serde_json::from_value(value.clone()).ok())
        .collect();
    let evidence = phases
        .iter()
        .find(|phase| phase.phase == "evidence")
        .unwrap();
    assert_eq!(
        evidence.dependencies.as_ref().unwrap().tables,
        vec![(&retained.dataset.groups["source"]).into()]
    );
    let validation = phases
        .iter()
        .find(|phase| phase.phase == "validation")
        .unwrap();
    assert_eq!(
        validation.dependencies.as_ref().unwrap().tables,
        vec![(&retained.dataset.related.as_ref().unwrap().evidence).into()]
    );
    engine
        .abandon(&request.dataset, &request.operation, "withdrawn")
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&request).await.unwrap_err().code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.plan_refresh(&request).await.unwrap_err().code(),
        "operation_abandoned"
    );
    assert_eq!(
        engine.latest(&request.dataset).await.unwrap_err().code(),
        "no_release"
    );
    request.operation = id("replacement");
    let release = engine.refresh(&request).await.unwrap();
    assert_eq!(release.dataset.groups, retained.dataset.groups);
    let related = release.dataset.related.as_ref().unwrap();
    let retained_related = retained.dataset.related.as_ref().unwrap();
    assert_eq!(related.evidence, retained_related.evidence);
    assert_eq!(related.coverage, retained_related.coverage);
    assert_eq!(related.request.operation, request.operation);
    assert_eq!(engine.latest(&left.dataset.dataset).await.unwrap(), left);
    assert_eq!(engine.latest(&right.dataset.dataset).await.unwrap(), right);
}
fn join(spec: &RefreshSpec) -> &JoinTransform {
    let RefreshTransform::Join(join) = &spec.transform else {
        panic!("expected join");
    };
    join
}
fn join_mut(spec: &mut RefreshSpec) -> &mut JoinTransform {
    let RefreshTransform::Join(join) = &mut spec.transform else {
        panic!("expected join");
    };
    join
}
fn text(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let array = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}
fn number(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let array = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (!array.is_null(row)).then(|| array.value(row))
}
async fn occurrences(release: &ReleasePin) -> Vec<(Location, Row)> {
    let table = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let budget = Budget::new(limits()).unwrap();
    let mut stream = storage::read_occurrences(&table, &budget)
        .await
        .unwrap()
        .boxed();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            rows.push((
                (
                    text(&batch, storage::OCCURRENCE_FILE, row).unwrap(),
                    number(&batch, storage::OCCURRENCE_ROW, row).unwrap(),
                ),
                (
                    number(&batch, "a", row),
                    number(&batch, "b", row),
                    text(&batch, "text", row),
                ),
            ));
        }
    }
    rows
}
async fn oracle(left: &ReleasePin, right: &ReleasePin, join: &JoinDefinition) -> Vec<Matched> {
    let left = occurrences(left).await;
    let right = occurrences(right).await;
    let mut matched_right = vec![false; right.len()];
    let mut expected = Vec::new();
    for (left_at, left) in &left {
        let mut matched = false;
        for (index, (right_at, right)) in right.iter().enumerate() {
            let matches = left.0 == right.0
                && left.1 == right.1
                && (join.nulls == NullMatching::Equal || (left.0.is_some() && left.1.is_some()));
            if matches {
                matched = true;
                matched_right[index] = true;
                expected.push((
                    Some(left_at.clone()),
                    Some(right_at.clone()),
                    left.2.clone(),
                    right.2.clone(),
                ));
            }
        }
        if !matched && join.kind != JoinKind::Inner {
            expected.push((Some(left_at.clone()), None, left.2.clone(), None));
        }
    }
    if join.kind == JoinKind::Full {
        for ((at, row), matched) in right.into_iter().zip(matched_right) {
            if !matched {
                expected.push((None, Some(at), None, row.2));
            }
        }
    }
    expected.sort();
    expected
}
async fn check(engine: &Engine, release: &ReleasePin, expected: &[Matched]) {
    let mut actual = Vec::new();
    let mut stream = engine.read_group(release, "source").await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            let location = |side: &str| {
                let file = text(&batch, &format!("__docspec_{side}_file"), row);
                let position = number(&batch, &format!("__docspec_{side}_row"), row);
                assert_eq!(file.is_some(), position.is_some());
                file.zip(position)
            };
            actual.push((
                location("left"),
                location("right"),
                text(&batch, "left_text", row),
                text(&batch, "right_text", row),
            ));
        }
    }
    actual.sort();
    assert_eq!(actual, expected);
    let mut evidence = engine.read_evidence(release).await.unwrap();
    let mut pairs = Vec::new();
    while let Some(batch) = evidence.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            pairs.push((
                text(&batch, "__docspec_left_file", row).zip(number(
                    &batch,
                    "__docspec_left_row",
                    row,
                )),
                text(&batch, "__docspec_right_file", row).zip(number(
                    &batch,
                    "__docspec_right_row",
                    row,
                )),
            ));
        }
    }
    pairs.sort();
    assert_eq!(
        pairs,
        expected
            .iter()
            .map(|row| (row.0.clone(), row.1.clone()))
            .collect::<Vec<_>>()
    );
    let mut combined = engine.read_combined(release).await.unwrap();
    assert!(
        combined
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__docspec_"))
    );
    let mut count = 0;
    while let Some(batch) = combined.try_next().await.unwrap() {
        count += batch.num_rows();
    }
    assert_eq!(count, expected.len());
}

#[tokio::test]
async fn all_join_kinds_preserve_duplicate_and_partial_null_occurrences() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Full] {
        for nulls in [NullMatching::Never, NullMatching::Equal] {
            let mut spec = recipe(&left, &right);
            spec.dataset = id(&format!("{kind:?}-{nulls:?}").to_lowercase());
            join_mut(&mut spec).join.kind = kind;
            join_mut(&mut spec).join.nulls = nulls;
            let expected = oracle(&left, &right, &join(&spec).join).await;
            let release = engine.refresh(&spec).await.unwrap();
            check(&engine, &release, &expected).await;
        }
    }
    let empty = source(&engine, root.path(), "empty", "initial", None, &[]).await;
    let mut spec = recipe(&empty, &right);
    join_mut(&mut spec).join.kind = JoinKind::Full;
    let release = engine.refresh(&spec).await.unwrap();
    check(
        &engine,
        &release,
        &oracle(&empty, &right, &join(&spec).join).await,
    )
    .await;
    spec.dataset = id("both-empty");
    join_mut(&mut spec).right = empty.version_ref();
    let release = engine.refresh(&spec).await.unwrap();
    check(&engine, &release, &[]).await;
}

#[tokio::test]
async fn cardinality_guards_reject_fanout_and_self_joins_keep_exact_roles() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    for cardinality in [JoinCardinality::ManyToOne, JoinCardinality::OneToOne] {
        let mut spec = recipe(&left, &right);
        spec.dataset = id(&format!("{cardinality:?}").to_lowercase());
        join_mut(&mut spec).join.cardinality = cardinality;
        assert_eq!(
            engine.refresh(&spec).await.unwrap_err().code(),
            "join_cardinality"
        );
        assert_eq!(
            engine.latest(&spec.dataset).await.unwrap_err().code(),
            "no_release"
        );
        let inspection = engine.inspect(&spec.dataset).await.unwrap();
        let prepared: Vec<docspec::model::Protection> = inspection
            .iter()
            .filter(|(key, _)| key.starts_with("protection/"))
            .map(|(_, value)| serde_json::from_value(value.clone()).unwrap())
            .filter(|protection: &docspec::model::Protection| {
                protection.purpose.ends_with("/processing")
            })
            .collect();
        assert_eq!(prepared.len(), 1);
        for path in &prepared[0].paths {
            assert_eq!(
                std::fs::read_dir(path).unwrap().count(),
                0,
                "cardinality must refuse before any native output is written"
            );
        }
    }
    let mut spec = recipe(&left, &left);
    let release = engine.refresh(&spec).await.unwrap();
    check(
        &engine,
        &release,
        &oracle(&left, &left, &join(&spec).join).await,
    )
    .await;
    spec.dataset = left.dataset.dataset.clone();
    assert_eq!(
        engine.plan_refresh(&spec).await.unwrap_err().code(),
        "invalid_refresh"
    );
}

fn forged(root: &Path, name: &str, mut release: ReleasePin) -> ReleasePin {
    let path = root.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec(&release.dataset).unwrap()).unwrap();
    let (digest, bytes) = storage::digest(&path).unwrap();
    release.manifest = ObjectRef {
        owner: release.dataset.dataset.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    };
    release
}

#[tokio::test]
async fn offline_verifier_reconstructs_values_contributors_and_exact_input_graph() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let spec = recipe(&left, &right);
    let release = engine.refresh(&spec).await.unwrap();
    // A different recipe produces valid retained tables but cannot substitute
    // for this recipe's values or contributors, even with a fresh manifest hash.
    let mut other = spec.clone();
    other.dataset = id("other");
    join_mut(&mut other).join.kind = JoinKind::Inner;
    let different = engine.refresh(&other).await.unwrap();
    for (name, mutate) in ["source", "evidence", "roles", "values"]
        .into_iter()
        .enumerate()
    {
        let mut bad = release.clone();
        match mutate {
            "source" => {
                bad.dataset
                    .groups
                    .insert("source".into(), different.dataset.groups["source"].clone());
            }
            "evidence" => {
                bad.dataset.related.as_mut().unwrap().evidence =
                    different.dataset.related.as_ref().unwrap().evidence.clone();
            }
            "roles" | "values" => {
                let related = bad.dataset.related.as_mut().unwrap();
                if mutate == "roles" {
                    let join = join_mut(&mut related.request);
                    std::mem::swap(&mut join.left, &mut join.right);
                } else {
                    join_mut(&mut related.request).join.fields[0].side = JoinSide::Right;
                }
                bad.dataset.request =
                    docspec::model::OperationRequest::Refresh(Box::new(related.request.clone()));
            }
            _ => unreachable!(),
        }
        assert!(
            verify_release(&forged(root.path(), &name.to_string(), bad), limits())
                .await
                .is_err(),
            "{mutate}"
        );
    }
    let mut tight = limits();
    tight.max_metadata_bytes = release.manifest.bytes as usize + left.manifest.bytes as usize;
    assert_eq!(
        verify_release(&release, tight).await.unwrap_err().code(),
        "metadata_capacity"
    );
    drop(engine);
    std::fs::rename(
        root.path().join("workspace/catalog.sqlite"),
        root.path().join("hidden.sqlite"),
    )
    .unwrap();
    verify_release(&release, limits()).await.unwrap();
    assert!(!root.path().join("workspace/catalog.sqlite").exists());
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn captured_selection_records_lag_latest_refuses_and_coverage_cannot_regress() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let mut captured = recipe(&left, &right);
    let mut latest = captured.clone();
    latest.dataset = id("latest");
    latest.policy = RefreshPolicy::Latest;
    for spec in [&captured, &latest] {
        assert_eq!(
            engine
                .refresh_until(spec, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
    }
    let newer = source(
        &engine,
        root.path(),
        "comments",
        "newer",
        Some(left.id.clone()),
        &rows().0,
    )
    .await;
    // Observing a newer head reports lag; it does not consume that head's
    // content. A cold selection must work with only its catalog receipt.
    let hidden = root.path().join("unavailable-observed-head.json");
    std::fs::rename(&newer.manifest.path, &hidden).unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let result = engine.refresh(&captured).await.unwrap();
    let rows = engine.inspect(&captured.dataset).await.unwrap();
    let progress: BTreeMap<Id, SuccessionProof> =
        serde_json::from_value(rows["input-progress/initial"].clone()).unwrap();
    assert_eq!(progress[&id("comments")].selections_behind, 1);
    assert_eq!(progress[&id("comments")].observed_head, newer.version_ref());
    assert_eq!(
        engine.refresh(&latest).await.unwrap_err().code(),
        "stale_input"
    );
    assert_eq!(engine.refresh(&captured).await.unwrap(), result);
    std::fs::rename(hidden, &newer.manifest.path).unwrap();
    captured.operation = id("advance");
    captured.base = Some(result.id.clone());
    join_mut(&mut captured).left = newer.version_ref();
    let advanced = engine.refresh(&captured).await.unwrap();
    let comparison = engine.compare(&result, &advanced, "source").await.unwrap();
    assert_eq!(comparison.added_rows, Some(0));
    assert_eq!(comparison.removed_rows, Some(0));
    assert!(!comparison.same_inputs);
    captured.operation = id("regress");
    captured.base = Some(advanced.id.clone());
    join_mut(&mut captured).left = left.version_ref();
    assert_eq!(
        engine.refresh(&captured).await.unwrap_err().code(),
        "coverage_regression"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn completed_phases_resume_and_original_selection_reconciles_after_invalidation() {
    for phase in [
        "admission",
        "processing",
        "evidence",
        "validation",
        "readback",
        "selection",
    ] {
        let root = TempDir::new().unwrap();
        let (engine, left, right) = fixture(root.path()).await;
        let spec = recipe(&left, &right);
        drop(engine);
        support::kill_after(root.path(), &spec, "refresh", phase);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let before = engine.inspect(&spec.dataset).await.unwrap();
        drop(engine);
        let (release, work) = support::resume(root.path(), "refresh");
        let phases = [
            "admission",
            "processing",
            "evidence",
            "validation",
            "readback",
            "selection",
        ];
        let at = phases.iter().position(|value| *value == phase).unwrap();
        let expected: Vec<_> = phases[at + 1..]
            .iter()
            .filter(|value| **value != "selection")
            .map(|value| (*value).to_owned())
            .collect();
        assert_eq!(
            work, expected,
            "resume must execute only missing work after {phase}"
        );
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_eq!(engine.refresh(&spec).await.unwrap(), release);
        let after = engine.inspect(&spec.dataset).await.unwrap();
        for (key, value) in before.iter().filter(|(key, _)| key.starts_with("phase/")) {
            assert_eq!(after.get(key), Some(value), "{phase}: {key}");
        }
        check(
            &engine,
            &release,
            &oracle(&left, &right, &join(&spec).join).await,
        )
        .await;
        engine
            .invalidate(&left.dataset.dataset, "evidence correction", None)
            .await
            .unwrap();
        assert_eq!(engine.refresh(&spec).await.unwrap(), release);
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn output_base_and_producer_eligibility_fence_selection_without_losing_work() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = recipe(&left, &right);
    let second = RefreshSpec {
        operation: id("second"),
        ..first.clone()
    };
    engine
        .refresh_until(&first, "processing")
        .await
        .unwrap_err();
    let winner = engine.refresh(&second).await.unwrap();
    assert_eq!(
        engine.refresh(&first).await.unwrap_err().code(),
        "stale_base"
    );
    let pending = RefreshSpec {
        operation: id("pending"),
        base: Some(winner.id.clone()),
        ..first.clone()
    };
    engine
        .refresh_until(&pending, "readback")
        .await
        .unwrap_err();
    engine
        .invalidate(&left.dataset.dataset, "evidence correction", None)
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&pending).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(engine.latest(&first.dataset).await.unwrap(), winner);
    assert!(
        engine
            .inspect(&first.dataset)
            .await
            .unwrap()
            .keys()
            .any(|key| key.ends_with("/evidence"))
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn backfill_preserves_related_binding_and_noop_refresh_fences_old_cutover() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let spec = recipe(&left, &right);
    let base = engine.refresh(&spec).await.unwrap();
    let group = BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("length"),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id("lengths"),
        group_definition: "length-v1".into(),
        fields: vec![ComputedField {
            id: 20,
            name: "length".into(),
            expression: "CAST(length(left_text) AS BIGINT)".into(),
        }],
    };
    let enriched = engine.backfill(&group).await.unwrap();
    assert_eq!(enriched.dataset.related, base.dataset.related);
    let pending = BackfillSpec {
        operation: id("pending"),
        base: enriched.id.clone(),
        group: id("other"),
        fields: vec![ComputedField {
            id: 21,
            name: "other_length".into(),
            expression: "CAST(length(right_text) AS BIGINT)".into(),
        }],
        ..group
    };
    assert_eq!(
        engine
            .backfill_until(&pending, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let refreshed = engine
        .refresh(&RefreshSpec {
            operation: id("noop"),
            base: Some(enriched.id.clone()),
            ..spec
        })
        .await
        .unwrap();
    assert_eq!(
        refreshed.dataset.groups["source"],
        base.dataset.groups["source"]
    );
    assert!(refreshed.dataset.boundary > enriched.dataset.boundary);
    assert!(refreshed.dataset.computed.contains_key("lengths"));
    assert!(matches!(
        engine.backfill(&pending).await.unwrap_err().code(),
        "stale_source_boundary" | "stale_base"
    ));
    verify_release(&refreshed, limits()).await.unwrap();
}

#[tokio::test]
async fn quoted_helper_names_and_unmatched_duplicate_keys_are_supported() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let mut spec = recipe(&left, &right);
    join_mut(&mut spec).join.fields[0].name = "__weight".into();
    join_mut(&mut spec).join.fields[1].name = "__total".into();
    join_mut(&mut spec).join.fields.push(JoinField {
        side: JoinSide::Left,
        field: "a".into(),
        name: "quote\" and space".into(),
    });
    let release = engine.refresh(&spec).await.unwrap();
    verify_release(&release, limits()).await.unwrap();
    let empty = source(&engine, root.path(), "empty", "initial", None, &[]).await;
    spec.dataset = id("unmatched-duplicates");
    join_mut(&mut spec).left = empty.version_ref();
    join_mut(&mut spec).join.kind = JoinKind::Full;
    join_mut(&mut spec).join.cardinality = JoinCardinality::OneToOne;
    // Duplicate right keys have no matching left partner, so no occurrence fans out.
    let release = engine.refresh(&spec).await.unwrap();
    assert_eq!(
        release.dataset.groups["source"].rows,
        right.dataset.groups["source"].rows
    );
}

#[tokio::test]
async fn offline_parent_rules_reject_related_conversion_and_role_rebinding() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let raw = RunSpec {
        dataset: first.dataset.dataset.clone(),
        operation: id("raw"),
        base: Some(first.id.clone()),
        relationship: None,
        input: left.dataset.inputs["input"].path.clone(),
        expected_digest: left.dataset.inputs["input"].digest.clone(),
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    assert_eq!(engine.run(&raw).await.unwrap_err().code(), "related_source");
    let updated = docspec::model::UpdateSpec {
        dataset: first.dataset.dataset.clone(),
        operation: id("raw-update"),
        base: first.id.clone(),
        definition: "v1".into(),
        keys: vec!["left_text".into()],
        replacements: docspec::model::InputSpec {
            path: raw.input.clone(),
            digest: raw.expected_digest.clone(),
        },
        deletions: None,
    };
    assert_eq!(
        engine.update(&updated).await.unwrap_err().code(),
        "related_source"
    );
    let mut forged_build = first.clone();
    forged_build.id = raw.operation.clone();
    forged_build.dataset.version = raw.operation.clone();
    forged_build.dataset.request = docspec::model::OperationRequest::Build(raw);
    forged_build.dataset.related = None;
    forged_build.dataset.inputs = left.dataset.inputs.clone();
    forged_build.dataset.parent_manifest = Some(first.manifest.clone());
    forged_build.dataset.boundary += 1;
    assert_eq!(
        verify_release(&forged(root.path(), "conversion", forged_build), limits())
            .await
            .unwrap_err()
            .code(),
        "invalid_group"
    );
    // These tables actually implement the swapped-role recipe. Only the based
    // output's structural role-continuity rule should reject the forged claim.
    let mut swapped = recipe(&right, &left);
    swapped.dataset = id("swapped");
    let mut swapped = engine.refresh(&swapped).await.unwrap();
    swapped.id = id("rebound");
    swapped.dataset.dataset = first.dataset.dataset.clone();
    swapped.dataset.version = swapped.id.clone();
    swapped.dataset.boundary = first.dataset.boundary + 1;
    swapped.dataset.parent_manifest = Some(first.manifest.clone());
    let related = swapped.dataset.related.as_mut().unwrap();
    related.request.dataset = first.dataset.dataset.clone();
    related.request.operation = swapped.id.clone();
    related.request.base = Some(first.id.clone());
    swapped.dataset.request =
        docspec::model::OperationRequest::Refresh(Box::new(related.request.clone()));
    assert_eq!(
        verify_release(&forged(root.path(), "roles", swapped), limits())
            .await
            .unwrap_err()
            .code(),
        "invalid_group"
    );
}

fn nested_recipe(joined: &ReleasePin, right: &ReleasePin) -> RefreshSpec {
    let mut spec = recipe(joined, right);
    spec.dataset = id("nested");
    join_mut(&mut spec).join.keys = vec![JoinKey {
        left: "right_text".into(),
        right: "text".into(),
    }];
    join_mut(&mut spec).join.fields[0].field = "left_text".into();
    spec
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn cached_readback_checks_missing_transitive_objects_and_live_ancestor_eligibility() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let joined = engine.refresh(&recipe(&left, &right)).await.unwrap();
    let spec = nested_recipe(&joined, &right);
    assert_eq!(
        engine
            .refresh_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    // comments is reachable through joined, not an immediate input of nested.
    let path = &left.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .path;
    let hidden = format!("{path}.hidden");
    std::fs::rename(path, &hidden).unwrap();
    assert!(engine.refresh(&spec).await.is_err());
    assert_eq!(
        engine.latest(&spec.dataset).await.unwrap_err().code(),
        "no_release"
    );
    std::fs::rename(&hidden, path).unwrap();
    engine
        .invalidate(&left.dataset.dataset, "ancestor correction", None)
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&spec).await.unwrap_err().code(),
        "stale_eligibility"
    );
    // Historical readback still checks immutable retained semantics, not present eligibility.
    verify_release(&joined, limits()).await.unwrap();
}

#[tokio::test]
async fn http_refresh_and_catalog_free_cli_verification_use_shared_services() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let spec = recipe(&left, &right);
    let app = docspec::http::router(engine.clone());
    let post = |path: &str| {
        Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap()
    };
    let response = app.clone().oneshot(post("/refreshes/plan")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let plan: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert!(
        plan["required_phases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|phase| phase == "evidence")
    );
    let response = app.clone().oneshot(post("/refreshes/run")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let release: ReleasePin =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    check(
        &engine,
        &release,
        &oracle(&left, &right, &join(&spec).join).await,
    )
    .await;
    let response = app
        .oneshot(
            Request::get("/datasets/joined/versions/initial/evidence")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    assert_eq!(reader.schema().fields().len(), 4);
    let count: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
    assert_eq!(
        count as u64,
        release.dataset.related.as_ref().unwrap().evidence.rows
    );
    drop(engine);
    let release_path = root.path().join("release.json");
    let limits_path = root.path().join("limits.json");
    std::fs::write(&release_path, serde_json::to_vec(&release).unwrap()).unwrap();
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    std::fs::rename(
        root.path().join("workspace/catalog.sqlite"),
        root.path().join("hidden.sqlite"),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .arg("--limits")
        .arg(&limits_path)
        .arg("verify")
        .arg(&release_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let arrow_path = root.path().join("evidence.arrow");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .arg("--limits")
        .arg(limits_path)
        .arg("read-evidence")
        .arg(release_path)
        .arg(&arrow_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reader =
        arrow::ipc::reader::StreamReader::try_new(File::open(arrow_path).unwrap(), None).unwrap();
    assert_eq!(
        reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
        count
    );
    assert!(!root.path().join("workspace/catalog.sqlite").exists());
}

#[tokio::test]
async fn coupled_refresh_refuses_mixed_generations_then_advances_exact_coverage() {
    use docspec::model::{GenerationSeal, OperationRequest, SourceRelationship};
    async fn couple(engine: &Engine, base: &ReleasePin, generation: &str) -> ReleasePin {
        let OperationRequest::Build(mut request) = base.dataset.request.clone() else {
            unreachable!()
        };
        request.operation = id(generation);
        request.base = Some(base.id.clone());
        request.relationship = Some(SourceRelationship::Coupled {
            family: id("family"),
            generation: id(generation),
        });
        engine.run(&request).await.unwrap()
    }
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let first_left = couple(&engine, &left, "g1").await;
    let first_right = couple(&engine, &right, "g1").await;
    let seal = |generation: &str, left: &ReleasePin, right: &ReleasePin| GenerationSeal {
        family: id("family"),
        generation: id(generation),
        members: BTreeMap::from([
            (left.dataset.dataset.clone(), left.version_ref()),
            (right.dataset.dataset.clone(), right.version_ref()),
        ]),
    };
    engine
        .seal_generation(&seal("g1", &first_left, &first_right))
        .await
        .unwrap();
    let first = engine
        .refresh(&recipe(&first_left, &first_right))
        .await
        .unwrap();
    let second_left = couple(&engine, &first_left, "g2").await;
    let second_right = couple(&engine, &first_right, "g2").await;
    engine
        .seal_generation(&seal("g2", &second_left, &second_right))
        .await
        .unwrap();
    let mut next = recipe(&second_left, &first_right);
    next.operation = id("mixed");
    next.base = Some(first.id.clone());
    assert_eq!(
        engine.refresh(&next).await.unwrap_err().code(),
        "invalid_relationship"
    );
    assert_eq!(engine.latest(&next.dataset).await.unwrap(), first);
    next.operation = id("complete");
    join_mut(&mut next).right = second_right.version_ref();
    let result = engine.refresh(&next).await.unwrap();
    assert_eq!(
        result
            .dataset
            .related
            .as_ref()
            .unwrap()
            .coverage
            .generations[&id("family")]
            .generation,
        id("g2")
    );
    check(
        &engine,
        &result,
        &oracle(&second_left, &second_right, &join(&next).join).await,
    )
    .await;
}

#[path = "related_refresh/incremental.rs"]
mod incremental;

#[path = "related_refresh/aggregation.rs"]
mod aggregation;

#[path = "related_refresh/aggregation_types.rs"]
mod aggregation_types;

#[path = "related_refresh/selected_keys.rs"]
mod selected_keys;

#[path = "related_refresh/forward_changes.rs"]
mod forward_changes;

#[path = "related_refresh/history_keys.rs"]
mod history_keys;
