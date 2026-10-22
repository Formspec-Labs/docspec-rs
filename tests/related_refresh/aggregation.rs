//! Summary values and exact contributor membership checked by a direct Rust oracle.
use super::*;
use arrow::array::Decimal128Array;
use docspec::model::{
    AggregateField, AggregateFunction as F, AggregateTransform, AggregationDefinition,
    OperationRequest,
};

pub(super) fn summary(input: &ReleasePin, global: bool) -> RefreshSpec {
    RefreshSpec {
        dataset: id(if global { "global" } else { "summary" }),
        operation: id("initial"),
        base: None,
        definition: "summary-v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: if global { vec![] } else { vec!["a".into()] },
                aggregates: vec![
                    ("rows", F::CountRows),
                    ("count", F::Count { field: "b".into() }),
                    ("sum", F::Sum { field: "b".into() }),
                    ("min", F::Min { field: "b".into() }),
                    ("max", F::Max { field: "b".into() }),
                ]
                .into_iter()
                .map(|(name, function)| AggregateField {
                    name: name.into(),
                    function,
                })
                .collect(),
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    }
}
fn aggregate_mut(spec: &mut RefreshSpec) -> &mut AggregateTransform {
    let RefreshTransform::Aggregate(aggregate) = &mut spec.transform else {
        panic!("expected summary")
    };
    aggregate
}
pub(super) fn next(base: &ReleasePin, input: &ReleasePin, operation: &str) -> RefreshSpec {
    let mut spec = base.dataset.related.as_ref().unwrap().request.clone();
    spec.base = Some(base.id.clone());
    spec.operation = id(operation);
    spec.mode = RefreshMode::Incremental;
    aggregate_mut(&mut spec).input = input.version_ref();
    spec
}
type Value = (i64, i64, Option<i128>, Option<i64>, Option<i64>);
pub(super) async fn values(
    engine: &Engine,
    release: &ReleasePin,
    global: bool,
) -> BTreeMap<Option<i64>, Value> {
    let mut result = BTreeMap::new();
    let mut stream = engine.read_group(release, "source").await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let sum = batch
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(sum.data_type(), &DataType::Decimal128(38, 0));
        for row in 0..batch.num_rows() {
            let key = number(&batch, if global { "__docspec_global" } else { "a" }, row);
            assert!(
                result
                    .insert(
                        key,
                        (
                            number(&batch, "rows", row).unwrap(),
                            number(&batch, "count", row).unwrap(),
                            (!sum.is_null(row)).then(|| sum.value(row)),
                            number(&batch, "min", row),
                            number(&batch, "max", row)
                        )
                    )
                    .is_none()
            );
        }
    }
    result
}
pub(super) async fn check_summary(
    engine: &Engine,
    release: &ReleasePin,
    input: &ReleasePin,
    global: bool,
) {
    let mut expected: BTreeMap<Option<i64>, Value> = BTreeMap::new();
    let mut contributors = Vec::new();
    if global {
        expected.insert(Some(0), (0, 0, None, None, None));
    }
    for (location, row) in occurrences(input).await {
        let key = if global { Some(0) } else { row.0 };
        let value = expected.entry(key).or_insert((0, 0, None, None, None));
        value.0 += 1;
        if let Some(number) = row.1 {
            value.1 += 1;
            value.2 = Some(value.2.unwrap_or(0) + i128::from(number));
            value.3 = Some(value.3.map_or(number, |v| v.min(number)));
            value.4 = Some(value.4.map_or(number, |v| v.max(number)));
        }
        contributors.push((key, location));
    }
    assert_eq!(values(engine, release, global).await, expected);
    let mut actual = Vec::new();
    let mut stream = engine.read_evidence(release).await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            actual.push((
                number(&batch, if global { "__docspec_global" } else { "a" }, row),
                (
                    text(&batch, "__docspec_input_file", row).unwrap(),
                    number(&batch, "__docspec_input_row", row).unwrap(),
                ),
            ));
        }
    }
    actual.sort();
    contributors.sort();
    assert_eq!(actual, contributors);
    let mut combined = engine.read_combined(release).await.unwrap();
    assert!(
        combined
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__docspec_"))
    );
    while combined.try_next().await.unwrap().is_some() {}
}

#[tokio::test]
async fn summaries_match_integer_oracle_with_duplicates_nulls_and_extreme_sums() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let input = source(
        &engine,
        root.path(),
        "numbers",
        "initial",
        None,
        &[
            (Some(1), Some(i64::MAX), None),
            (Some(1), Some(i64::MAX), None),
            (Some(2), Some(i64::MIN), None),
            (Some(2), Some(i64::MIN), None),
            (None, Some(3), None),
            (None, Some(3), None),
            (Some(4), None, None),
        ],
    )
    .await;
    for global in [false, true] {
        let spec = summary(&input, global);
        let wire = serde_json::to_vec(&spec).unwrap();
        assert_eq!(serde_json::from_slice::<RefreshSpec>(&wire).unwrap(), spec);
        let release = engine.refresh(&spec).await.unwrap();
        check_summary(&engine, &release, &input, global).await;
        assert_eq!(engine.refresh(&spec).await.unwrap(), release);
    }
}

#[tokio::test]
async fn incremental_groups_move_delete_and_replace_equal_values_with_new_contributors() {
    let root = TempDir::new().unwrap();
    let (engine, mut input, _) = fixture(root.path()).await;
    let mut result = engine.refresh(&summary(&input, false)).await.unwrap();
    check_summary(&engine, &result, &input, false).await;
    // These changes replace every a=1 contributor with identical measures and new text.
    let old_values = values(&engine, &result, false).await;
    let old_evidence = result.dataset.related.as_ref().unwrap().evidence.clone();
    input = incremental::update(
        &engine,
        root.path(),
        &input,
        "equal-values",
        &[
            (Some(1), Some(2), Some("new-one".into())),
            (Some(1), Some(2), Some("new-two".into())),
            (Some(1), None, Some("new-null".into())),
        ],
        &[],
    )
    .await;
    result = engine
        .refresh(&next(&result, &input, "equal-values"))
        .await
        .unwrap();
    assert_eq!(values(&engine, &result, false).await, old_values);
    assert_ne!(
        result.dataset.related.as_ref().unwrap().evidence,
        old_evidence
    );
    let changes = engine
        .change_certificate(&result.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    assert!(changes.removed.rows > 0);
    check_summary(&engine, &result, &input, false).await;
    // Move the null group to a new docket, then delete the last member of another group.
    for (operation, replacements, deletions) in [
        (
            "move",
            vec![(Some(7), Some(2), None)],
            vec![(None, Some(2), None)],
        ),
        ("delete", vec![], vec![(Some(9), Some(9), None)]),
    ] {
        input = incremental::update(
            &engine,
            root.path(),
            &input,
            operation,
            &replacements,
            &deletions,
        )
        .await;
        result = engine
            .refresh(&next(&result, &input, operation))
            .await
            .unwrap();
        check_summary(&engine, &result, &input, false).await;
        let mut full = summary(&input, false);
        full.dataset = id(&format!("full-{operation}"));
        let rebuilt = engine.refresh(&full).await.unwrap();
        assert_eq!(
            values(&engine, &result, false).await,
            values(&engine, &rebuilt, false).await
        );
    }
}

#[tokio::test]
async fn grouped_and_global_empty_transitions_and_noop_pins_remain_exact() {
    for global in [false, true] {
        let root = TempDir::new().unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let mut input = source(&engine, root.path(), "input", "initial", None, &[]).await;
        let mut result = engine.refresh(&summary(&input, global)).await.unwrap();
        check_summary(&engine, &result, &input, global).await;
        for (operation, replacement, deletion) in [
            ("empty-noop", vec![], vec![]),
            (
                "append",
                vec![(Some(1), Some(8), None), (Some(2), None, None)],
                vec![],
            ),
            (
                "remove-all",
                vec![],
                vec![(Some(1), Some(8), None), (Some(2), None, None)],
            ),
            ("empty-noop-again", vec![], vec![]),
        ] {
            input = incremental::update(
                &engine,
                root.path(),
                &input,
                operation,
                &replacement,
                &deletion,
            )
            .await;
            let next = engine
                .refresh(&next(&result, &input, operation))
                .await
                .unwrap();
            check_summary(&engine, &next, &input, global).await;
            if operation.contains("noop") {
                assert_eq!(
                    next.dataset.groups["source"],
                    result.dataset.groups["source"]
                );
                assert_eq!(
                    next.dataset.related.as_ref().unwrap().evidence,
                    result.dataset.related.as_ref().unwrap().evidence
                );
                let changes = engine
                    .change_certificate(&next.version_ref())
                    .await
                    .unwrap()
                    .unwrap()
                    .changes
                    .source
                    .unwrap();
                assert_eq!(changes.removed.rows, 0);
                assert_eq!(changes.added.rows, 0);
            }
            result = next;
        }
    }
}

#[tokio::test]
async fn summaries_share_unaffected_files_and_selected_computed_groups() {
    let root = TempDir::new().unwrap();
    let (engine, input, _) = fixture(root.path()).await;
    let first = engine.refresh(&summary(&input, false)).await.unwrap();
    let first = engine
        .backfill(&BackfillSpec {
            dataset: first.dataset.dataset.clone(),
            operation: id("computed"),
            base: first.id.clone(),
            definition: "summary-v1".into(),
            group: id("derived"),
            group_definition: "double-v1".into(),
            fields: vec![ComputedField {
                id: 20,
                name: "double_count".into(),
                expression: "rows * 2".into(),
            }],
        })
        .await
        .unwrap();
    let source_files = incremental::files(&first.dataset.groups["source"]).await;
    let evidence_files =
        incremental::files(&first.dataset.related.as_ref().unwrap().evidence).await;
    let original = source_files
        .iter()
        .chain(evidence_files.iter())
        .map(|path| {
            (
                path,
                storage::digest(Path::new(path)).unwrap(),
                std::fs::metadata(path).unwrap().modified().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let input = incremental::update(
        &engine,
        root.path(),
        &input,
        "append",
        &[(Some(50), Some(100), None)],
        &[],
    )
    .await;
    let changed = engine
        .refresh(&next(&first, &input, "append"))
        .await
        .unwrap();
    assert!(source_files.is_subset(&incremental::files(&changed.dataset.groups["source"]).await));
    assert!(
        evidence_files.is_subset(
            &incremental::files(&changed.dataset.related.as_ref().unwrap().evidence).await
        )
    );
    for object in first.dataset.groups["derived"]
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
    {
        assert!(changed.dataset.groups["derived"].objects.contains(object));
    }
    for (path, digest, modified) in original {
        assert_eq!(storage::digest(Path::new(path)).unwrap(), digest);
        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            modified
        );
    }
    check_summary(&engine, &changed, &input, false).await;
    let input = incremental::update(&engine, root.path(), &input, "noop", &[], &[]).await;
    let noop = engine
        .refresh(&next(&changed, &input, "noop"))
        .await
        .unwrap();
    assert_eq!(noop.dataset.groups, changed.dataset.groups);
    assert_eq!(
        noop.dataset.related.as_ref().unwrap().evidence,
        changed.dataset.related.as_ref().unwrap().evidence
    );
    check_summary(&engine, &noop, &input, false).await;
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn summary_workers_resume_only_remaining_phases_after_sigkill() {
    for phase in ["processing", "evidence", "readback"] {
        let root = TempDir::new().unwrap();
        let (engine, input, _) = fixture(root.path()).await;
        let first = engine.refresh(&summary(&input, false)).await.unwrap();
        let input = incremental::update(
            &engine,
            root.path(),
            &input,
            "append",
            &[(Some(20), Some(10), None)],
            &[],
        )
        .await;
        let spec = next(&first, &input, "append");
        drop(engine);
        support::kill_after(root.path(), &spec, "refresh", phase);
        let (release, work) = support::resume(root.path(), "refresh");
        assert_eq!(
            work,
            match phase {
                "processing" => vec!["evidence", "validation", "readback"],
                "evidence" => vec!["validation", "readback"],
                _ => vec![],
            }
        );
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        check_summary(&engine, &release, &input, false).await;
    }
}

#[tokio::test]
async fn summary_refuses_changed_recipes_and_wrong_native_contributors() {
    let root = TempDir::new().unwrap();
    let (engine, input, _) = fixture(root.path()).await;
    let first = engine.refresh(&summary(&input, false)).await.unwrap();
    let mut changed = next(&first, &input, "bad");
    aggregate_mut(&mut changed).grouping.aggregates[0].name = "changed".into();
    assert_eq!(
        engine.plan_refresh(&changed).await.unwrap_err().code(),
        "full_refresh_required"
    );
    let mut invalid = summary(&input, false);
    invalid.dataset = id("invalid");
    aggregate_mut(&mut invalid).grouping.aggregates[0].name = "a".into();
    assert_eq!(
        engine.plan_refresh(&invalid).await.unwrap_err().code(),
        "invalid_refresh"
    );
    let mut invalid = summary(&input, false);
    invalid.dataset = id("invalid");
    aggregate_mut(&mut invalid).grouping.aggregates[2].function = F::Sum {
        field: "text".into(),
    };
    assert_eq!(
        engine.refresh(&invalid).await.unwrap_err().code(),
        "aggregate_value_type"
    );
    assert!(
        engine
            .plan_refresh(&invalid)
            .await
            .unwrap()
            .reusable_phases
            .is_empty()
    );
    let different = source(
        &engine,
        root.path(),
        "different",
        "initial",
        None,
        &rows().0,
    )
    .await;
    let mut other = summary(&different, false);
    other.dataset = id("other");
    let other = engine.refresh(&other).await.unwrap();
    assert_eq!(
        values(&engine, &first, false).await,
        values(&engine, &other, false).await
    );
    let mut forged_release = first.clone();
    forged_release.dataset.related.as_mut().unwrap().evidence =
        other.dataset.related.as_ref().unwrap().evidence.clone();
    assert_eq!(
        verify_release(
            &forged(root.path(), "wrong-contributors", forged_release),
            limits()
        )
        .await
        .unwrap_err()
        .code(),
        "invalid_related_result"
    );
    // Changing transform roles is refused even when the input dataset set could match.
    let mut forged_release = first.clone();
    let related = forged_release.dataset.related.as_mut().unwrap();
    related.request.transform = recipe(&input, &input).transform;
    forged_release.dataset.request = OperationRequest::Refresh(Box::new(related.request.clone()));
    assert!(
        verify_release(
            &forged(root.path(), "wrong-transform", forged_release),
            limits()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn joined_summary_retains_catalog_free_transitive_occurrence_evidence() {
    let root = TempDir::new().unwrap();
    let (engine, left, right) = fixture(root.path()).await;
    let mut spec = recipe(&left, &right);
    join_mut(&mut spec).join.kind = JoinKind::Full;
    for name in ["a", "b", "text"] {
        join_mut(&mut spec).join.fields.push(JoinField {
            side: JoinSide::Left,
            field: name.into(),
            name: name.into(),
        });
    }
    let joined = engine.refresh(&spec).await.unwrap();
    let expected = oracle(&left, &right, &join(&spec).join).await;
    check(&engine, &joined, &expected).await;
    let summary = engine.refresh(&summary(&joined, false)).await.unwrap();
    check_summary(&engine, &summary, &joined, false).await;
    assert!(joined.dataset.groups["source"].rows > left.dataset.groups["source"].rows);
    drop(engine);
    std::fs::rename(
        root.path().join("workspace/catalog.sqlite"),
        root.path().join("hidden.sqlite"),
    )
    .unwrap();
    let budget = Budget::new(limits()).unwrap();
    // Resolve summary -> exact joined file/row -> exact original source tuple.
    let native = storage::open_pin(&joined.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut joined_rows = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            let location = (
                text(&batch, storage::OCCURRENCE_FILE, row).unwrap(),
                number(&batch, storage::OCCURRENCE_ROW, row).unwrap(),
            );
            let side = |file, index| {
                text(&batch, file, row).map(|file| (file, number(&batch, index, row).unwrap()))
            };
            joined_rows.insert(
                location,
                (
                    side("__docspec_left_file", "__docspec_left_row"),
                    side("__docspec_right_file", "__docspec_right_row"),
                    text(&batch, "left_text", row),
                    text(&batch, "right_text", row),
                ),
            );
        }
    }
    drop(stream);
    let mut stream = docspec::engine::read_evidence(&summary, &budget)
        .await
        .unwrap();
    let mut recovered = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            let location = (
                text(&batch, "__docspec_input_file", row).unwrap(),
                number(&batch, "__docspec_input_row", row).unwrap(),
            );
            recovered.push(joined_rows[&location].clone());
        }
    }
    recovered.sort();
    assert_eq!(recovered, expected);
    assert!(!root.path().join("workspace/catalog.sqlite").exists());
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn summary_coverage_guards_and_rebase_preserve_completed_work() {
    let root = TempDir::new().unwrap();
    let (engine, input, _) = fixture(root.path()).await;
    let first = engine.refresh(&summary(&input, false)).await.unwrap();
    let newer = incremental::update(
        &engine,
        root.path(),
        &input,
        "append",
        &[(Some(10), Some(3), None)],
        &[],
    )
    .await;
    let pending = next(&first, &newer, "pending");
    assert_eq!(
        engine
            .refresh_until(&pending, "evidence")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let winner = engine
        .refresh(&next(&first, &input, "winner"))
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&pending).await.unwrap_err().code(),
        "stale_base"
    );
    let rebased = next(&winner, &newer, "rebased");
    assert!(
        engine
            .plan_refresh(&rebased)
            .await
            .unwrap()
            .reusable_phases
            .is_empty()
    );
    let latest = RefreshSpec {
        policy: RefreshPolicy::Latest,
        operation: id("latest"),
        ..rebased.clone()
    };
    engine.refresh_until(&latest, "readback").await.unwrap_err();
    let newest = incremental::update(
        &engine,
        root.path(),
        &newer,
        "append-again",
        &[(Some(11), Some(4), None)],
        &[],
    )
    .await;
    assert_eq!(
        engine.refresh(&latest).await.unwrap_err().code(),
        "stale_input"
    );
    let selected = engine.refresh(&rebased).await.unwrap();
    assert_eq!(
        selected.dataset.related.as_ref().unwrap().coverage.inputs[&newer.dataset.dataset],
        newer.version_ref()
    );
    assert_eq!(
        engine
            .refresh(&next(&selected, &input, "backward"))
            .await
            .unwrap_err()
            .code(),
        "coverage_regression"
    );
    let pending = next(&selected, &newest, "stale");
    engine
        .refresh_until(&pending, "readback")
        .await
        .unwrap_err();
    engine
        .invalidate(&newest.dataset.dataset, "corrected evidence", None)
        .await
        .unwrap();
    assert_eq!(
        engine.refresh(&pending).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(
        engine.latest(&selected.dataset.dataset).await.unwrap(),
        selected
    );
}
