//! Native joins checked against direct Rust matching and exact input occurrence pairs.

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
#[cfg(feature = "test-util")]
use docspec::model::SuccessionProof;
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::verify_release,
    model::{
        BackfillSpec, ComputedField, Id, JoinCardinality, JoinDefinition, JoinField, JoinKey,
        JoinKind, JoinSide, JoinTransform, NullMatching, ObjectRef, RefreshMode, RefreshPolicy,
        RefreshSpec, RefreshTransform, ReleasePin, RunSpec,
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
            partition_by: Vec::new(),
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
    let budget = Budget::new(limits()).unwrap();
    let table = storage::open_pin(&release.dataset.groups["source"], &budget)
        .await
        .unwrap();

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
async fn policy_change_reuses_same_consumer_processing_but_rechecks_latest_inputs() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let captured = recipe(&left, &right);
    let expected = oracle(&left, &right, &join(&captured).join).await;
    let frozen_inputs = BTreeMap::from([
        (left.dataset.dataset.clone(), left.version_ref()),
        (right.dataset.dataset.clone(), right.version_ref()),
    ]);
    // External producers are no longer needed; authoritative native inputs stay.
    std::fs::remove_file(root.path().join("comments-initial.parquet")).unwrap();
    std::fs::remove_file(root.path().join("dockets-initial.parquet")).unwrap();
    drop(engine);
    assert_eq!(
        support::kill_after_with_work(root.path(), &captured, "refresh", "processing"),
        ["admission", "processing"]
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let before = engine.inspect(&captured.dataset).await.unwrap();
    let (processing_key, processing) = before
        .iter()
        .find_map(|(key, value)| {
            let phase =
                serde_json::from_value::<docspec::model::PhaseResult>(value.clone()).ok()?;
            (phase.phase == "processing").then_some((key.clone(), phase))
        })
        .unwrap();
    let protection_binding = format!(
        "reference-binding/{}",
        blake3::hash(processing_key.as_bytes()).to_hex(),
    );
    assert!(before.contains_key(&protection_binding));
    assert_eq!(processing.operation, captured.operation);
    assert_eq!(processing.coverage.as_ref().unwrap().inputs, frozen_inputs);
    let generation = engine
        .plan_phase_retirement(
            &captured.dataset,
            &id("observe-original-processing"),
            std::slice::from_ref(&processing_key),
            "observe the retained processing generation",
        )
        .await
        .unwrap()
        .records[0]
        .clone();
    let mut latest = captured.clone();
    latest.operation = id("latest-policy");
    latest.policy = RefreshPolicy::Latest;
    let plan = engine.plan_refresh(&latest).await.unwrap();
    assert_eq!(plan.reusable_phase_origins["processing"], generation);
    assert_eq!(engine.inspect(&captured.dataset).await.unwrap(), before);
    drop(engine);
    // Same dataset/material slot: policy affects selection, not joined values.
    assert_eq!(
        support::kill_after_with_work(root.path(), &latest, "refresh", "readback"),
        ["evidence", "validation", "readback"]
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let after = engine.inspect(&captured.dataset).await.unwrap();
    assert_eq!(after[&processing_key], before[&processing_key]);
    assert_eq!(after[&protection_binding], before[&protection_binding]);
    assert_eq!(
        engine
            .plan_phase_retirement(
                &captured.dataset,
                &id("observe-policy-processing"),
                std::slice::from_ref(&processing_key),
                "observe unchanged native generation",
            )
            .await
            .unwrap()
            .records[0],
        generation
    );
    for operation in [&captured.operation, &latest.operation] {
        assert_eq!(
            after[&format!("request-dependencies/{operation}")]["coverage"]["inputs"],
            serde_json::to_value(&frozen_inputs).unwrap()
        );
    }
    let retained: ReleasePin =
        serde_json::from_value(after[&format!("readback/{}", latest.operation)].clone()).unwrap();
    assert_eq!(
        retained.dataset.groups["source"],
        *processing.table.as_ref().unwrap()
    );
    assert_eq!(
        retained.dataset.related.as_ref().unwrap().coverage.inputs,
        frozen_inputs
    );
    let newer = source(
        &engine,
        root.path(),
        "comments",
        "advanced",
        Some(left.id.clone()),
        &[(Some(1), Some(2), Some("corrected-new-head".into()))],
    )
    .await;
    assert_ne!(newer.version_ref(), left.version_ref());
    assert_eq!(
        engine
            .release(&left.dataset.dataset, &left.id)
            .await
            .unwrap(),
        left
    );
    assert_eq!(
        engine
            .release(&right.dataset.dataset, &right.id)
            .await
            .unwrap(),
        right
    );
    let before_refusal = engine.inspect(&captured.dataset).await.unwrap();
    assert_eq!(
        engine.refresh(&latest).await.unwrap_err().code(),
        "stale_input"
    );
    assert_eq!(
        engine.inspect(&captured.dataset).await.unwrap(),
        before_refusal
    );
    assert_eq!(
        engine.latest(&captured.dataset).await.unwrap_err().code(),
        "no_release"
    );
    drop(engine);
    let captured_path = root.path().join("captured-request.json");
    std::fs::write(&captured_path, serde_json::to_vec(&captured).unwrap()).unwrap();
    let (selected, work): (ReleasePin, _) =
        support::run_cli(root.path(), &["refresh", captured_path.to_str().unwrap()]);
    assert_eq!(work, ["readback"]);
    assert_eq!(
        selected.dataset.groups["source"],
        retained.dataset.groups["source"]
    );
    let related = selected.dataset.related.as_ref().unwrap();
    assert_eq!(related.request.policy, RefreshPolicy::Captured);
    assert_eq!(related.coverage.inputs, frozen_inputs);
    assert_eq!(
        related.evidence,
        retained.dataset.related.as_ref().unwrap().evidence
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    check(&engine, &selected, &expected).await;
    verify_release(&selected, limits()).await.unwrap();
    let records = engine.inspect(&captured.dataset).await.unwrap();
    let progress: BTreeMap<Id, SuccessionProof> =
        serde_json::from_value(records[&format!("input-progress/{}", captured.operation)].clone())
            .unwrap();
    assert_eq!(
        progress[&left.dataset.dataset].observed_head,
        newer.version_ref()
    );
    assert_eq!(progress[&left.dataset.dataset].selections_behind, 1);
    assert_eq!(records[&processing_key], before[&processing_key]);
    assert_eq!(records[&protection_binding], before[&protection_binding]);
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
        partition_by: Vec::new(),
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
        relationship: None,
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

#[cfg(feature = "test-util")]
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

#[path = "related_refresh/compaction.rs"]
mod compaction;

#[path = "related_refresh/cardinality.rs"]
mod cardinality;

#[cfg(feature = "test-util")]
#[tokio::test]
async fn material_null_matching_change_reuses_admission_but_recomputes_values_and_evidence() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let original = recipe(&left, &right);
    let old_expected = oracle(&left, &right, &join(&original).join).await;
    let mut changed = original.clone();
    changed.operation = id("never-match-nulls");
    join_mut(&mut changed).join.nulls = NullMatching::Never;
    let expected = oracle(&left, &right, &join(&changed).join).await;
    assert_ne!(expected, old_expected);
    assert_eq!(expected.len(), 7);
    assert!(
        old_expected
            .iter()
            .any(|row| row.3.as_deref() == Some("right-null"))
    );
    assert!(
        old_expected
            .iter()
            .any(|row| row.3.as_deref() == Some("partial-null"))
    );
    assert!(
        !expected
            .iter()
            .any(|row| matches!(row.3.as_deref(), Some("right-null" | "partial-null")))
    );
    // Only the matching configuration changes. These frozen producer captures
    // include duplicate and partial-null keys with exact native occurrences.
    assert_eq!(changed.inputs(), original.inputs());
    assert_eq!(changed.definition, original.definition);
    assert_eq!(changed.policy, original.policy);
    std::fs::remove_file(root.path().join("comments-initial.parquet")).unwrap();
    std::fs::remove_file(root.path().join("dockets-initial.parquet")).unwrap();
    drop(engine);
    assert_eq!(
        support::kill_after_with_work(root.path(), &original, "refresh", "processing"),
        ["admission", "processing"]
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let before = engine.inspect(&original.dataset).await.unwrap();
    let (old_key, old_processing) = before
        .iter()
        .find_map(|(key, value)| {
            let phase =
                serde_json::from_value::<docspec::model::PhaseResult>(value.clone()).ok()?;
            (phase.phase == "processing").then_some((key.clone(), phase))
        })
        .unwrap();
    let original_plan = engine.plan_refresh(&original).await.unwrap();
    let old_generation = original_plan.reusable_phase_origins["processing"].clone();
    let plan = engine.plan_refresh(&changed).await.unwrap();
    assert_eq!(
        plan.reusable_phase_origins["admission"],
        original_plan.reusable_phase_origins["admission"]
    );
    assert!(!plan.reusable_phase_origins.contains_key("processing"));
    assert!(
        plan.required_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert_eq!(engine.inspect(&original.dataset).await.unwrap(), before);
    drop(engine);
    assert_eq!(
        support::kill_after_with_work(root.path(), &changed, "refresh", "readback"),
        ["processing", "evidence", "validation", "readback"]
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let after = engine.inspect(&original.dataset).await.unwrap();
    let old_binding = format!(
        "reference-binding/{}",
        blake3::hash(old_key.as_bytes()).to_hex()
    );
    assert_eq!(after[&old_key], before[&old_key]);
    assert_eq!(after[&old_binding], before[&old_binding]);
    assert_eq!(
        engine
            .plan_refresh(&original)
            .await
            .unwrap()
            .reusable_phase_origins["processing"],
        old_generation
    );
    let retained: ReleasePin =
        serde_json::from_value(after[&format!("readback/{}", changed.operation)].clone()).unwrap();
    assert_ne!(
        retained.dataset.groups["source"],
        *old_processing.table.as_ref().unwrap()
    );
    assert_eq!(
        retained.dataset.related.as_ref().unwrap().coverage.inputs,
        changed.inputs()
    );
    assert_eq!(
        engine.latest(&changed.dataset).await.unwrap_err().code(),
        "no_release"
    );
    let selected = engine.refresh(&changed).await.unwrap();
    assert_eq!(selected, retained);
    check(&engine, &selected, &expected).await;
    verify_release(&selected, limits()).await.unwrap();
    assert_eq!(
        engine.inspect(&original.dataset).await.unwrap()[&old_key],
        before[&old_key]
    );
}

#[tokio::test]
async fn collect_set_grouped_and_global_refresh_full_and_incrementally_with_refusals() {
    use arrow::array::ListArray;
    use docspec::{
        engine::verify_applied_changes,
        model::{
            AggregateField, AggregateFunction as F, AggregateTransform, AggregationDefinition,
        },
    };
    use std::collections::BTreeSet;

    fn collect_set_spec(input: &ReleasePin, global: bool) -> RefreshSpec {
        RefreshSpec {
            dataset: id(if global {
                "collect-global"
            } else {
                "collect-grouped"
            }),
            operation: id("initial"),
            base: None,
            definition: "collect-v1".into(),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: input.version_ref(),
                grouping: AggregationDefinition {
                    keys: if global { vec![] } else { vec!["a".into()] },
                    aggregates: vec![
                        AggregateField {
                            name: "rows".into(),
                            function: F::CountRows,
                        },
                        AggregateField {
                            name: "b_set".into(),
                            function: F::CollectSet { field: "b".into() },
                        },
                        AggregateField {
                            name: "text_set".into(),
                            function: F::CollectSet {
                                field: "text".into(),
                            },
                        },
                    ],
                },
            }),
        }
    }

    async fn check_collect_set(
        engine: &Engine,
        release: &ReleasePin,
        input: &ReleasePin,
        global: bool,
    ) {
        let mut expected: BTreeMap<Option<i64>, (i64, BTreeSet<i64>, BTreeSet<String>)> =
            BTreeMap::new();
        if global {
            expected.insert(Some(0), (0, BTreeSet::new(), BTreeSet::new()));
        }
        for (_, row) in occurrences(input).await {
            let key = if global { Some(0) } else { row.0 };
            let entry = expected
                .entry(key)
                .or_insert_with(|| (0, BTreeSet::new(), BTreeSet::new()));
            entry.0 += 1;
            if let Some(b) = row.1 {
                entry.1.insert(b);
            }
            if let Some(text) = row.2 {
                entry.2.insert(text);
            }
        }
        let expected_vec: BTreeMap<Option<i64>, (i64, Vec<i64>, Vec<String>)> = expected
            .into_iter()
            .map(|(k, (rows, b_set, text_set))| {
                (
                    k,
                    (
                        rows,
                        b_set.into_iter().collect(),
                        text_set.into_iter().collect(),
                    ),
                )
            })
            .collect();

        let mut actual = BTreeMap::new();
        let mut stream = engine.read_group(release, "source").await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let b_list = batch
                .column_by_name("b_set")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let text_list = batch
                .column_by_name("text_set")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!b_list.is_null(row));
                assert!(!text_list.is_null(row));
                let b_values = b_list.value(row);
                let b_ints = b_values.as_any().downcast_ref::<Int64Array>().unwrap();
                let b_vec: Vec<i64> = (0..b_ints.len())
                    .map(|idx| {
                        assert!(!b_ints.is_null(idx));
                        b_ints.value(idx)
                    })
                    .collect();
                let text_values = text_list.value(row);
                let text_strs = text_values.as_any().downcast_ref::<StringArray>().unwrap();
                let text_vec: Vec<String> = (0..text_strs.len())
                    .map(|idx| {
                        assert!(!text_strs.is_null(idx));
                        text_strs.value(idx).to_owned()
                    })
                    .collect();
                let key = number(&batch, if global { "__docspec_global" } else { "a" }, row);
                assert!(
                    actual
                        .insert(key, (number(&batch, "rows", row).unwrap(), b_vec, text_vec))
                        .is_none()
                );
            }
        }
        assert_eq!(actual, expected_vec);
    }

    let root = TempDir::new().unwrap();
    let (engine, mut input, _) = fixture(root.path()).await;
    let empty = source(&engine, root.path(), "empty-src", "initial", None, &[]).await;

    for global in [false, true] {
        let mut empty_spec = collect_set_spec(&empty, global);
        empty_spec.dataset = id(if global {
            "empty-collect-global"
        } else {
            "empty-collect-grouped"
        });
        let empty_release = engine.refresh(&empty_spec).await.unwrap();
        verify_release(&empty_release, limits()).await.unwrap();
        check_collect_set(&engine, &empty_release, &empty, global).await;

        let spec = collect_set_spec(&input, global);
        let wire = serde_json::to_vec(&spec).unwrap();
        assert_eq!(serde_json::from_slice::<RefreshSpec>(&wire).unwrap(), spec);
        let first = engine.refresh(&spec).await.unwrap();
        verify_release(&first, limits()).await.unwrap();
        check_collect_set(&engine, &first, &input, global).await;
    }

    let first_grouped = engine.latest(&id("collect-grouped")).await.unwrap();
    let first_global = engine.latest(&id("collect-global")).await.unwrap();

    input = incremental::update(
        &engine,
        root.path(),
        &input,
        "updated-1",
        &[
            (Some(1), Some(2), Some("alpha".into())),
            (Some(1), Some(7), Some("zebra".into())),
            (Some(4), None, None),
        ],
        &[(Some(9), Some(9), None)],
    )
    .await;

    for (base, global, names) in [
        (
            &first_grouped,
            false,
            vec!["a", "rows", "b_set", "text_set"],
        ),
        (&first_global, true, vec!["rows", "b_set", "text_set"]),
    ] {
        let next_spec = aggregation::next(base, &input, "incremental-1");
        let second = engine.refresh(&next_spec).await.unwrap();
        verify_release(&second, limits()).await.unwrap();
        let cert = engine
            .change_certificate(&second.version_ref())
            .await
            .unwrap()
            .unwrap();
        verify_applied_changes(base, &second, &cert.changes, limits())
            .await
            .unwrap();
        forward_changes::check_forward(&engine, base, &second, &names).await;
        check_collect_set(&engine, &second, &input, global).await;
    }

    // Refusals: reserved names, key collisions, and nested/unqualified operand types.
    for (agg_name, field_name) in [
        ("__docspec_bad", "text"),
        ("text_set", "__docspec_file"),
        ("a", "text"),
        ("text_set", "a"),
        ("text_set", ""),
    ] {
        let mut bad = collect_set_spec(&input, false);
        bad.dataset = id("bad-collect");
        let RefreshTransform::Aggregate(agg) = &mut bad.transform else {
            unreachable!()
        };
        agg.grouping.aggregates = vec![AggregateField {
            name: agg_name.into(),
            function: F::CollectSet {
                field: field_name.into(),
            },
        }];
        assert_eq!(
            engine.refresh(&bad).await.unwrap_err().code(),
            "invalid_refresh",
            "expected invalid_refresh for ({agg_name}, {field_name})"
        );
    }

    // Qualified scalar types (Boolean, Int32, Date32, Timestamp(Microsecond, UTC)) and Float64 refusal.
    use arrow::{
        array::{BooleanArray, Date32Array, Float64Array, Int32Array, TimestampMicrosecondArray},
        datatypes::TimeUnit,
    };
    let typed_path = root.path().join("typed-collect.parquet");
    let typed_schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("num32", DataType::Int32, true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("flt", DataType::Float64, true),
    ]));
    let typed_batch = RecordBatch::try_new(
        typed_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int32Array::from(vec![Some(30), Some(10), None])),
            Arc::new(Date32Array::from(vec![Some(19_500), Some(19_000), None])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(2_000_000), Some(1_000_000), None])
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&typed_path).unwrap(), typed_schema, None).unwrap();
    writer.write(&typed_batch).unwrap();
    writer.close().unwrap();
    let typed_src = engine
        .run(&RunSpec {
            dataset: id("typed-src"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: typed_path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&typed_path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let typed_spec = RefreshSpec {
        dataset: id("typed-collect"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: typed_src.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["grp".into()],
                aggregates: vec![
                    AggregateField {
                        name: "flags".into(),
                        function: F::CollectSet {
                            field: "flag".into(),
                        },
                    },
                    AggregateField {
                        name: "nums".into(),
                        function: F::CollectSet {
                            field: "num32".into(),
                        },
                    },
                    AggregateField {
                        name: "days".into(),
                        function: F::CollectSet {
                            field: "day".into(),
                        },
                    },
                    AggregateField {
                        name: "timestamps".into(),
                        function: F::CollectSet { field: "ts".into() },
                    },
                ],
            },
        }),
    };
    let typed_release = engine.refresh(&typed_spec).await.unwrap();
    verify_release(&typed_release, limits()).await.unwrap();
    let typed_batches: Vec<RecordBatch> = engine
        .read_group(&typed_release, "source")
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut actual_typed = BTreeMap::new();
    for batch in &typed_batches {
        let grps = batch
            .column_by_name("grp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let flags = batch
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let nums = batch
            .column_by_name("nums")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let days = batch
            .column_by_name("days")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let timestamps = batch
            .column_by_name("timestamps")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let grp = grps.value(row);
            let flag_vals: Vec<bool> = flags
                .value(row)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect();
            let num_vals: Vec<i32> = nums
                .value(row)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            let day_vals: Vec<i32> = days
                .value(row)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .values()
                .to_vec();
            let ts_vals: Vec<i64> = timestamps
                .value(row)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .values()
                .to_vec();
            actual_typed.insert(grp, (flag_vals, num_vals, day_vals, ts_vals));
        }
    }
    assert_eq!(
        actual_typed,
        BTreeMap::from([
            (
                1,
                (
                    vec![false, true],
                    vec![10, 30],
                    vec![19_000, 19_500],
                    vec![1_000_000, 2_000_000],
                ),
            ),
            (2, (vec![], vec![], vec![], vec![])),
        ])
    );

    let mut bad_flt = typed_spec.clone();
    bad_flt.dataset = id("bad-flt-collect");
    let RefreshTransform::Aggregate(agg) = &mut bad_flt.transform else {
        unreachable!()
    };
    agg.grouping.aggregates = vec![AggregateField {
        name: "flts".into(),
        function: F::CollectSet {
            field: "flt".into(),
        },
    }];
    assert_eq!(
        engine.refresh(&bad_flt).await.unwrap_err().code(),
        "aggregate_value_type"
    );
}

#[tokio::test]
async fn nested_struct_and_list_struct_join_fields_refresh_full_and_incrementally() {
    use arrow::{
        array::{Int32Array, ListArray, StructArray},
        buffer::{NullBuffer, OffsetBuffer},
    };
    use docspec::{
        engine::verify_applied_changes,
        model::{
            AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition,
            InputSpec, UpdateSpec,
        },
    };

    type Meta = Option<(Option<String>, i32)>;
    type OccurrenceSpan = (Option<String>, i32, i32);
    type Occurrences = Option<Vec<OccurrenceSpan>>;
    type NestedRow = (Option<i64>, Option<i64>, Meta, Occurrences);

    fn meta_fields() -> arrow::datatypes::Fields {
        vec![
            Arc::new(Field::new("source", DataType::Utf8, true)),
            Arc::new(Field::new("priority", DataType::Int32, false)),
        ]
        .into()
    }

    fn span_fields() -> arrow::datatypes::Fields {
        vec![
            Arc::new(Field::new("field", DataType::Utf8, true)),
            Arc::new(Field::new("start", DataType::Int32, false)),
            Arc::new(Field::new("end", DataType::Int32, false)),
        ]
        .into()
    }

    fn write_nested_parquet(path: &Path, rows: &[NestedRow], keys_only: bool) -> InputSpec {
        let mut fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ];
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
        ];
        if !keys_only {
            let m_fields = meta_fields();
            let s_fields = span_fields();
            let item_field = Arc::new(Field::new(
                "item",
                DataType::Struct(s_fields.clone()),
                false,
            ));
            fields.push(Field::new("meta", DataType::Struct(m_fields.clone()), true));
            fields.push(Field::new(
                "occurrences",
                DataType::List(item_field.clone()),
                true,
            ));

            let meta_nulls =
                NullBuffer::from(rows.iter().map(|r| r.2.is_some()).collect::<Vec<_>>());
            let meta_sources = StringArray::from_iter(
                rows.iter()
                    .map(|r| r.2.as_ref().and_then(|m| m.0.as_deref())),
            );
            let meta_priorities =
                Int32Array::from_iter_values(rows.iter().map(|r| r.2.as_ref().map_or(0, |m| m.1)));
            let meta_array = StructArray::new(
                m_fields,
                vec![Arc::new(meta_sources), Arc::new(meta_priorities)],
                Some(meta_nulls),
            );

            let mut offsets = Vec::with_capacity(rows.len() + 1);
            offsets.push(0i32);
            let mut list_valid = Vec::with_capacity(rows.len());
            let mut span_field_vals: Vec<Option<&str>> = Vec::new();
            let mut span_starts: Vec<i32> = Vec::new();
            let mut span_ends: Vec<i32> = Vec::new();
            for row in rows {
                match &row.3 {
                    Some(spans) => {
                        list_valid.push(true);
                        for (f, s, e) in spans {
                            span_field_vals.push(f.as_deref());
                            span_starts.push(*s);
                            span_ends.push(*e);
                        }
                        offsets.push(span_starts.len() as i32);
                    }
                    None => {
                        list_valid.push(false);
                        offsets.push(span_starts.len() as i32);
                    }
                }
            }
            let span_struct = StructArray::new(
                s_fields,
                vec![
                    Arc::new(StringArray::from(span_field_vals)),
                    Arc::new(Int32Array::from(span_starts)),
                    Arc::new(Int32Array::from(span_ends)),
                ],
                None,
            );
            let list_array = ListArray::new(
                item_field,
                OffsetBuffer::new(offsets.into()),
                Arc::new(span_struct),
                Some(NullBuffer::from(list_valid)),
            );

            columns.push(Arc::new(meta_array));
            columns.push(Arc::new(list_array));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(path).unwrap().0,
        }
    }

    fn read_meta(batch: &RecordBatch, name: &str, row: usize) -> Meta {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        if array.is_null(row) {
            return None;
        }
        let sources = array
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let priorities = array
            .column_by_name("priority")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        Some((
            (!sources.is_null(row)).then(|| sources.value(row).to_owned()),
            priorities.value(row),
        ))
    }

    fn read_occurrences_col(batch: &RecordBatch, name: &str, row: usize) -> Occurrences {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        if array.is_null(row) {
            return None;
        }
        let values = array.value(row);
        let structs = values.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = structs
            .column_by_name("field")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let starts = structs
            .column_by_name("start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ends = structs
            .column_by_name("end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        Some(
            (0..structs.len())
                .map(|idx| {
                    assert!(!structs.is_null(idx));
                    (
                        (!fields.is_null(idx)).then(|| fields.value(idx).to_owned()),
                        starts.value(idx),
                        ends.value(idx),
                    )
                })
                .collect(),
        )
    }

    async fn nested_source_occurrences(release: &ReleasePin) -> Vec<(Location, NestedRow)> {
        let budget = Budget::new(limits()).unwrap();
        let table = storage::open_pin(&release.dataset.groups["source"], &budget)
            .await
            .unwrap();
        let mut stream = storage::read_occurrences(&table, &budget)
            .await
            .unwrap()
            .boxed();
        let mut out = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            for row in 0..batch.num_rows() {
                out.push((
                    (
                        text(&batch, storage::OCCURRENCE_FILE, row).unwrap(),
                        number(&batch, storage::OCCURRENCE_ROW, row).unwrap(),
                    ),
                    (
                        number(&batch, "a", row),
                        number(&batch, "b", row),
                        read_meta(&batch, "meta", row),
                        read_occurrences_col(&batch, "occurrences", row),
                    ),
                ));
            }
        }
        out
    }

    type NestedMatched = (
        Option<Location>,
        Option<Location>,
        Meta,
        Occurrences,
        Meta,
        Occurrences,
    );

    async fn nested_oracle(
        left: &ReleasePin,
        right: &ReleasePin,
        join: &JoinDefinition,
    ) -> Vec<NestedMatched> {
        let left_rows = nested_source_occurrences(left).await;
        let right_rows = nested_source_occurrences(right).await;
        let mut matched_right = vec![false; right_rows.len()];
        let mut expected = Vec::new();
        for (left_at, l) in &left_rows {
            let mut matched = false;
            for (idx, (right_at, r)) in right_rows.iter().enumerate() {
                let matches = l.0 == r.0
                    && l.1 == r.1
                    && (join.nulls == NullMatching::Equal || (l.0.is_some() && l.1.is_some()));
                if matches {
                    matched = true;
                    matched_right[idx] = true;
                    expected.push((
                        Some(left_at.clone()),
                        Some(right_at.clone()),
                        l.2.clone(),
                        l.3.clone(),
                        r.2.clone(),
                        r.3.clone(),
                    ));
                }
            }
            if !matched && join.kind != JoinKind::Inner {
                expected.push((
                    Some(left_at.clone()),
                    None,
                    l.2.clone(),
                    l.3.clone(),
                    None,
                    None,
                ));
            }
        }
        if join.kind == JoinKind::Full {
            for ((right_at, r), matched) in right_rows.into_iter().zip(matched_right) {
                if !matched {
                    expected.push((None, Some(right_at), None, None, r.2, r.3));
                }
            }
        }
        expected.sort();
        expected
    }

    async fn check_nested_join(engine: &Engine, release: &ReleasePin, expected: &[NestedMatched]) {
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
                    read_meta(&batch, "left_meta", row),
                    read_occurrences_col(&batch, "left_occurrences", row),
                    read_meta(&batch, "right_meta", row),
                    read_occurrences_col(&batch, "right_occurrences", row),
                ));
            }
        }
        actual.sort();
        assert_eq!(actual, expected);

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

    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let left_initial_rows: Vec<NestedRow> = vec![
        (
            Some(1),
            Some(2),
            Some((Some("reg-a".into()), 10)),
            Some(vec![(Some("title".into()), 0, 5), (None, 10, 20)]),
        ),
        (Some(1), Some(2), Some((None, 20)), Some(vec![])),
        (
            None,
            Some(2),
            None,
            Some(vec![(Some("summary".into()), 1, 9)]),
        ),
        (
            Some(9),
            Some(9),
            Some((Some("unmatched-left".into()), 99)),
            None,
        ),
    ];
    let right_initial_rows: Vec<NestedRow> = vec![
        (
            Some(1),
            Some(2),
            Some((Some("docket-1".into()), 1)),
            Some(vec![(Some("docket_id".into()), 0, 12)]),
        ),
        (None, Some(2), Some((Some("docket-null".into()), 2)), None),
        (
            Some(8),
            Some(8),
            None,
            Some(vec![
                (Some("comment".into()), 3, 7),
                (Some("body".into()), 8, 15),
            ]),
        ),
    ];

    let left_input = write_nested_parquet(
        &root.path().join("left-initial.parquet"),
        &left_initial_rows,
        false,
    );
    let right_input = write_nested_parquet(
        &root.path().join("right-initial.parquet"),
        &right_initial_rows,
        false,
    );
    let mut left = engine
        .run(&RunSpec {
            dataset: id("nested-left"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: left_input.path,
            expected_digest: left_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let mut right = engine
        .run(&RunSpec {
            dataset: id("nested-right"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: right_input.path,
            expected_digest: right_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // Nested columns must still be refused as join keys or CollectSet operands.
    for bad_key in ["meta", "occurrences"] {
        let bad_join = RefreshSpec {
            dataset: id("bad-join-key"),
            operation: id(&format!("bad-join-{bad_key}")),
            base: None,
            definition: "v1".into(),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
            transform: RefreshTransform::Join(JoinTransform {
                left: left.version_ref(),
                right: right.version_ref(),
                join: JoinDefinition {
                    keys: vec![JoinKey {
                        left: bad_key.into(),
                        right: bad_key.into(),
                    }],
                    kind: JoinKind::Inner,
                    nulls: NullMatching::Equal,
                    cardinality: JoinCardinality::ManyToMany,
                    fields: vec![JoinField {
                        side: JoinSide::Left,
                        field: "a".into(),
                        name: "left_a".into(),
                    }],
                },
            }),
        };
        assert_eq!(
            engine.refresh(&bad_join).await.unwrap_err().code(),
            "join_key_type"
        );

        let bad_agg = RefreshSpec {
            dataset: id("bad-collect-type"),
            operation: id(&format!("bad-agg-{bad_key}")),
            base: None,
            definition: "v1".into(),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: left.version_ref(),
                grouping: AggregationDefinition {
                    keys: vec!["a".into()],
                    aggregates: vec![AggregateField {
                        name: "collected".into(),
                        function: AggregateFunction::CollectSet {
                            field: bad_key.into(),
                        },
                    }],
                },
            }),
        };
        assert_eq!(
            engine.refresh(&bad_agg).await.unwrap_err().code(),
            "aggregate_value_type"
        );
    }

    let make_join_spec =
        |dataset: &str, left: &ReleasePin, right: &ReleasePin, kind: JoinKind| RefreshSpec {
            dataset: id(dataset),
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
                    kind,
                    nulls: NullMatching::Equal,
                    cardinality: if kind == JoinKind::Left {
                        JoinCardinality::ManyToOne
                    } else {
                        JoinCardinality::ManyToMany
                    },
                    fields: vec![
                        JoinField {
                            side: JoinSide::Left,
                            field: "meta".into(),
                            name: "left_meta".into(),
                        },
                        JoinField {
                            side: JoinSide::Left,
                            field: "occurrences".into(),
                            name: "left_occurrences".into(),
                        },
                        JoinField {
                            side: JoinSide::Right,
                            field: "meta".into(),
                            name: "right_meta".into(),
                        },
                        JoinField {
                            side: JoinSide::Right,
                            field: "occurrences".into(),
                            name: "right_occurrences".into(),
                        },
                    ],
                },
            }),
        };

    for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Full] {
        let spec = make_join_spec(
            &format!("nested-join-{kind:?}").to_lowercase(),
            &left,
            &right,
            kind,
        );
        let release = engine.refresh(&spec).await.unwrap();
        verify_release(&release, limits()).await.unwrap();
        let expected = nested_oracle(&left, &right, &join(&spec).join).await;
        check_nested_join(&engine, &release, &expected).await;
    }

    let base_full = engine.latest(&id("nested-join-full")).await.unwrap();

    // Update both sides and run incremental refresh on the Full join with nested projections.
    left = engine
        .update(&UpdateSpec {
            dataset: left.dataset.dataset.clone(),
            operation: id("left-update-1"),
            base: left.id.clone(),
            definition: "v1".into(),
            keys: vec!["a".into(), "b".into()],
            replacements: write_nested_parquet(
                &root.path().join("left-replace-1.parquet"),
                &[
                    (
                        Some(1),
                        Some(2),
                        Some((Some("reg-a-updated".into()), 11)),
                        Some(vec![(Some("heading".into()), 2, 14)]),
                    ),
                    (
                        Some(5),
                        Some(5),
                        Some((Some("new-left".into()), 50)),
                        Some(vec![
                            (Some("clause".into()), 0, 4),
                            (Some("body".into()), 5, 19),
                        ]),
                    ),
                ],
                false,
            ),
            deletions: Some(write_nested_parquet(
                &root.path().join("left-delete-1.parquet"),
                &[(Some(9), Some(9), None, None)],
                true,
            )),
            relationship: None,
        })
        .await
        .unwrap();

    right = engine
        .update(&UpdateSpec {
            dataset: right.dataset.dataset.clone(),
            operation: id("right-update-1"),
            base: right.id.clone(),
            definition: "v1".into(),
            keys: vec!["a".into(), "b".into()],
            replacements: write_nested_parquet(
                &root.path().join("right-replace-1.parquet"),
                &[(
                    Some(5),
                    Some(5),
                    Some((Some("docket-5".into()), 5)),
                    Some(vec![(Some("docket_ref".into()), 1, 6)]),
                )],
                false,
            ),
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();

    let mut inc_spec = base_full.dataset.related.as_ref().unwrap().request.clone();
    inc_spec.operation = id("incremental-1");
    inc_spec.base = Some(base_full.id.clone());
    inc_spec.mode = RefreshMode::Incremental;
    join_mut(&mut inc_spec).left = left.version_ref();
    join_mut(&mut inc_spec).right = right.version_ref();

    let updated_full = engine.refresh(&inc_spec).await.unwrap();
    verify_release(&updated_full, limits()).await.unwrap();
    let cert = engine
        .change_certificate(&updated_full.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&base_full, &updated_full, &cert.changes, limits())
        .await
        .unwrap();
    forward_changes::check_forward(
        &engine,
        &base_full,
        &updated_full,
        &[
            "left_meta",
            "left_occurrences",
            "right_meta",
            "right_occurrences",
        ],
    )
    .await;
    let expected = nested_oracle(&left, &right, &join(&inc_spec).join).await;
    check_nested_join(&engine, &updated_full, &expected).await;
}
