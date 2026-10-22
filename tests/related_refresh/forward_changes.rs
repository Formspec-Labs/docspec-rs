//! Related producers retain exact accelerated changes across incremental releases.

use super::*;
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use docspec::model::{
    AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, ChangeDescriptor,
    ComparisonIdentity, ComparisonOptions, ComparisonPath,
};

async fn change_rows(
    engine: &Engine,
    before: &ReleasePin,
    after: &ReleasePin,
    options: &ComparisonOptions,
) -> (ChangeDescriptor, RecordBatch) {
    let stream = engine
        .changes(before, after, "source", options)
        .await
        .unwrap();
    let schema = stream.schema();
    let descriptor = serde_json::from_str(&schema.metadata()["docspec.change"]).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batch = concat_batches(&schema, &batches).unwrap();
    let order = lexsort_to_indices(
        &(0..3)
            .map(|column| SortColumn {
                values: batch.column(column).clone(),
                options: None,
            })
            .collect::<Vec<_>>(),
        None,
    )
    .unwrap();
    // Only the stream descriptor varies. Preserve field metadata and typed values.
    let schema = Arc::new(Schema::new(schema.fields().clone()));
    let batch = RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &order, None).unwrap())
            .collect(),
    )
    .unwrap();
    (descriptor, batch)
}

pub(super) async fn check_forward(
    engine: &Engine,
    before: &ReleasePin,
    after: &ReleasePin,
    names: &[&str],
) {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    let native = storage::open_pin(&after.dataset.groups["source"], &pin_budget)
        .await
        .unwrap();
    let native_schema = native.metadata().current_schema();
    let field_ids: Vec<_> = names
        .iter()
        .map(|name| native_schema.field_by_name(name).unwrap().id)
        .collect();
    for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
        let options = ComparisonOptions {
            identity,
            force_full: false,
        };
        let (mut accelerated, actual) = change_rows(engine, before, after, &options).await;
        let (full, expected) = change_rows(
            engine,
            before,
            after,
            &ComparisonOptions {
                force_full: true,
                ..options
            },
        )
        .await;
        assert_eq!(accelerated.path, ComparisonPath::ForwardChanges);
        assert_eq!(accelerated.fallback, None);
        assert_eq!(full.path, ComparisonPath::Full);
        assert_eq!(accelerated.field_ids, field_ids);
        assert!(accelerated.effects.coverage_changed);
        let DataType::Struct(fields) = actual.schema().field(3).data_type().clone() else {
            panic!("changes must retain typed values")
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            names
        );
        assert!(
            fields
                .iter()
                .all(|field| !field.name().starts_with("__docspec_"))
        );
        accelerated.path = full.path;
        accelerated.fallback = full.fallback;
        assert_eq!(accelerated, full);
        assert!(
            actual.num_rows() > 0,
            "the fixture must exercise changed members"
        );
        assert_eq!(
            actual, expected,
            "exact values and endpoint addresses: {identity:?}"
        );
    }
}

async fn check_counts(engine: &Engine, summary: &ReleasePin, input: &ReleasePin) {
    let mut expected = BTreeMap::new();
    for (_, row) in occurrences(input).await {
        *expected.entry(row.0).or_insert(0i64) += 1;
    }
    let mut actual = BTreeMap::new();
    let mut stream = engine.read_group(summary, "source").await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            assert!(
                actual
                    .insert(number(&batch, "a", row), number(&batch, "rows", row))
                    .is_none()
            );
        }
    }
    assert_eq!(
        actual,
        expected
            .into_iter()
            .map(|(key, count)| (key, Some(count)))
            .collect::<BTreeMap<_, _>>()
    );
}

#[tokio::test]
async fn join_and_summary_forward_chains_match_full_typed_changes_and_addresses() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut input = source(
        &engine,
        root.path(),
        "comments",
        "initial",
        None,
        &[
            (Some(1), Some(2), Some("duplicate".into())),
            (Some(1), Some(2), Some("duplicate".into())),
            (None, Some(2), None),
        ],
    )
    .await;
    let right = source(
        &engine,
        root.path(),
        "dockets",
        "initial",
        None,
        &[
            (Some(1), Some(2), Some("partner".into())),
            (None, Some(2), None),
        ],
    )
    .await;
    let mut join_spec = recipe(&input, &right);
    let mut joined = engine.refresh(&join_spec).await.unwrap();
    let mut summary_spec = RefreshSpec {
        dataset: id("summary"),
        operation: id("initial"),
        base: None,
        definition: "summary-v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["a".into()],
                aggregates: vec![AggregateField {
                    name: "rows".into(),
                    function: AggregateFunction::CountRows,
                }],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };
    let mut summary = engine.refresh(&summary_spec).await.unwrap();
    let first_joined = joined.clone();
    let first_summary = summary.clone();
    // The join physically retains contributor columns that comparison must omit.
    let native = storage::open_pin(&joined.dataset.groups["source"], &pin_budget)
        .await
        .unwrap();
    assert!(
        native
            .metadata()
            .current_schema()
            .field_by_name("__docspec_left_file")
            .is_some()
    );
    for (operation, replacements, deletions) in [
        (
            "one",
            vec![(Some(1), Some(2), Some("replacement".into()))],
            vec![],
        ),
        (
            "two",
            vec![(Some(7), None, None)],
            vec![(None, Some(2), None)],
        ),
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
        join_spec.operation = id(operation);
        join_spec.base = Some(joined.id.clone());
        join_spec.mode = RefreshMode::Incremental;
        join_mut(&mut join_spec).left = input.version_ref();
        joined = engine.refresh(&join_spec).await.unwrap();
        check(
            &engine,
            &joined,
            &oracle(&input, &right, &join(&join_spec).join).await,
        )
        .await;
        summary_spec.operation = id(operation);
        summary_spec.base = Some(summary.id.clone());
        summary_spec.mode = RefreshMode::Incremental;
        let RefreshTransform::Aggregate(aggregate) = &mut summary_spec.transform else {
            unreachable!()
        };
        aggregate.input = input.version_ref();
        summary = engine.refresh(&summary_spec).await.unwrap();
        check_counts(&engine, &summary, &input).await;
        check_forward(
            &engine,
            &first_joined,
            &joined,
            &["left_text", "right_text"],
        )
        .await;
        check_forward(&engine, &first_summary, &summary, &["a", "rows"]).await;
    }
}
