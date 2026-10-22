//! Non-leading, reordered keys preserve complete joins and contributor summaries.

use super::*;
use docspec::model::{
    AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, InputSpec,
    UpdateSpec,
};

fn reordered_file(path: &Path, rows: &[Row]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.2.as_deref()))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

async fn reordered_source(engine: &Engine, root: &Path, dataset: &str, rows: &[Row]) -> ReleasePin {
    let input = reordered_file(&root.join(format!("{dataset}.parquet")), rows);
    engine
        .run(&RunSpec {
            dataset: id(dataset),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap()
}

async fn check_summary(engine: &Engine, summary: &ReleasePin, input: &ReleasePin) {
    let mut expected = BTreeMap::new();
    let mut expected_evidence = Vec::new();
    for (location, row) in occurrences(input).await {
        *expected.entry((row.1, row.0)).or_insert(0i64) += 1;
        expected_evidence.push(((row.1, row.0), location));
    }
    let mut actual = BTreeMap::new();
    let mut stream = engine.read_group(summary, "source").await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            assert!(
                actual
                    .insert(
                        (number(&batch, "b", row), number(&batch, "a", row)),
                        number(&batch, "rows", row).unwrap(),
                    )
                    .is_none()
            );
        }
    }
    assert_eq!(actual, expected);
    let mut actual_evidence = Vec::new();
    let mut stream = engine.read_evidence(summary).await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            actual_evidence.push((
                (number(&batch, "b", row), number(&batch, "a", row)),
                (
                    text(&batch, "__docspec_input_file", row).unwrap(),
                    number(&batch, "__docspec_input_row", row).unwrap(),
                ),
            ));
        }
    }
    actual_evidence.sort();
    expected_evidence.sort();
    assert_eq!(actual_evidence, expected_evidence);
}

#[tokio::test]
async fn reversed_nonleading_keys_refresh_joins_and_summaries_with_exact_evidence() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let (left_rows, right_rows) = rows();
    let left = reordered_source(&engine, root.path(), "comments", &left_rows).await;
    // The other side keeps a different physical key order.
    let right = source(
        &engine,
        root.path(),
        "dockets",
        "initial",
        None,
        &right_rows,
    )
    .await;
    let mut join_spec = recipe(&left, &right);
    join_mut(&mut join_spec).join.keys.reverse();
    join_mut(&mut join_spec).join.kind = JoinKind::Full;
    let joined = engine.refresh(&join_spec).await.unwrap();
    check(
        &engine,
        &joined,
        &oracle(&left, &right, &join(&join_spec).join).await,
    )
    .await;
    let mut summary_spec = RefreshSpec {
        dataset: id("summary"),
        operation: id("initial"),
        base: None,
        definition: "summary-v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: left.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["b".into(), "a".into()],
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
    let summary = engine.refresh(&summary_spec).await.unwrap();
    check_summary(&engine, &summary, &left).await;
    let left = engine
        .update(&UpdateSpec {
            dataset: left.dataset.dataset.clone(),
            operation: id("changed"),
            base: left.id.clone(),
            definition: "v1".into(),
            keys: vec!["a".into(), "b".into()],
            replacements: reordered_file(
                &root.path().join("replacement.parquet"),
                &[
                    (Some(1), Some(2), Some("replacement".into())),
                    (None, Some(2), Some("changed-null".into())),
                    (Some(7), None, Some("new-partial-null".into())),
                ],
            ),
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    join_spec.operation = id("changed");
    join_spec.base = Some(joined.id.clone());
    join_spec.mode = RefreshMode::Incremental;
    join_mut(&mut join_spec).left = left.version_ref();
    let joined = engine.refresh(&join_spec).await.unwrap();
    check(
        &engine,
        &joined,
        &oracle(&left, &right, &join(&join_spec).join).await,
    )
    .await;
    summary_spec.operation = id("changed");
    summary_spec.base = Some(summary.id.clone());
    summary_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(aggregate) = &mut summary_spec.transform else {
        unreachable!()
    };
    aggregate.input = left.version_ref();
    let summary = engine.refresh(&summary_spec).await.unwrap();
    check_summary(&engine, &summary, &left).await;
}
