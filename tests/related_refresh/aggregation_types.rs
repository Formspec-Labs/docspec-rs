//! Composite typed groups, quoted identifiers and native schema qualification.
use super::*;
use arrow::array::{BooleanArray, Decimal128Array, Float64Array, Int32Array};
use docspec::model::{
    AggregateField, AggregateFunction as F, AggregateTransform, AggregationDefinition, InputSpec,
    UpdateSpec,
};
type TypedRow = (i64, Option<&'static str>, Option<bool>, Option<i32>);
type Key = (Option<String>, Option<bool>);
const DOCKET: &str = "docket\"key";
fn boolean(batch: &RecordBatch, name: &str, row: usize) -> Option<bool> {
    let values = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    (!values.is_null(row)).then(|| values.value(row))
}
fn typed_file(path: &Path, rows: &[TypedRow]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(DOCKET, DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("amount", DataType::Int32, true),
        Field::new("floating", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.2))),
            Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.3))),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.0 as f64),
            )),
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
async fn check_typed(engine: &Engine, release: &ReleasePin, input: &ReleasePin, rows: &[TypedRow]) {
    let mut expected: BTreeMap<Key, (i64, i64, Option<i128>, Option<i32>)> = BTreeMap::new();
    for row in rows {
        let value = expected
            .entry((row.1.map(str::to_owned), row.2))
            .or_insert((0, 0, None, None));
        value.0 += 1;
        if let Some(amount) = row.3 {
            value.1 += 1;
            value.2 = Some(value.2.unwrap_or(0) + i128::from(amount));
            value.3 = Some(value.3.map_or(amount, |old| old.min(amount)));
        }
    }
    let mut actual = BTreeMap::new();
    let mut stream = engine.read_group(release, "source").await.unwrap();
    assert_eq!(
        stream
            .schema()
            .field_with_name("minimum")
            .unwrap()
            .data_type(),
        &DataType::Int32
    );
    while let Some(batch) = stream.try_next().await.unwrap() {
        let sum = batch
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let min = batch
            .column_by_name("minimum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                actual
                    .insert(
                        (text(&batch, DOCKET, row), boolean(&batch, "flag", row)),
                        (
                            number(&batch, "rows", row).unwrap(),
                            number(&batch, "count", row).unwrap(),
                            (!sum.is_null(row)).then(|| sum.value(row)),
                            (!min.is_null(row)).then(|| min.value(row))
                        )
                    )
                    .is_none()
            );
        }
    }
    assert_eq!(actual, expected);
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(&input.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut expected_evidence = Vec::new();
    let mut input_ids = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            let row_id = number(&batch, "id", row).unwrap();
            let intended = rows.iter().find(|r| r.0 == row_id).unwrap();
            assert_eq!(text(&batch, DOCKET, row).as_deref(), intended.1);
            assert_eq!(boolean(&batch, "flag", row), intended.2);
            input_ids.push(row_id);
            expected_evidence.push((
                intended.1.map(str::to_owned),
                intended.2,
                text(&batch, storage::OCCURRENCE_FILE, row).unwrap(),
                number(&batch, storage::OCCURRENCE_ROW, row).unwrap(),
            ));
        }
    }
    input_ids.sort();
    let mut intended_ids = rows.iter().map(|r| r.0).collect::<Vec<_>>();
    intended_ids.sort();
    assert_eq!(input_ids, intended_ids);
    let mut actual = Vec::new();
    let mut stream = engine.read_evidence(release).await.unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        for row in 0..batch.num_rows() {
            actual.push((
                text(&batch, DOCKET, row),
                boolean(&batch, "flag", row),
                text(&batch, "__docspec_input_file", row).unwrap(),
                number(&batch, "__docspec_input_row", row).unwrap(),
            ));
        }
    }
    actual.sort();
    expected_evidence.sort();
    assert_eq!(actual, expected_evidence);
}
#[tokio::test]
async fn quoted_utf8_boolean_composite_groups_and_int32_measures_refresh_exactly() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut rows = vec![
        (1, Some("A'\""), Some(true), Some(i32::MAX)),
        (2, Some("A'\""), Some(true), Some(i32::MAX)),
        (3, Some(""), None, None),
        (4, None, Some(false), Some(0)),
        (5, None, None, None),
    ];
    let input = typed_file(&root.path().join("input.parquet"), &rows);
    let mut input = engine
        .run(&RunSpec {
            dataset: id("typed"),
            operation: id("initial"),
            base: None,
            definition: "v1".into(),
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            relationship: None,
        })
        .await
        .unwrap();
    let mut spec = RefreshSpec {
        dataset: id("summary"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: vec![DOCKET.into(), "flag".into()],
                aggregates: vec![
                    ("rows", F::CountRows),
                    (
                        "count",
                        F::Count {
                            field: "amount".into(),
                        },
                    ),
                    (
                        "sum",
                        F::Sum {
                            field: "amount".into(),
                        },
                    ),
                    (
                        "minimum",
                        F::Min {
                            field: "amount".into(),
                        },
                    ),
                ]
                .into_iter()
                .map(|(name, function)| AggregateField {
                    name: name.into(),
                    function,
                })
                .collect(),
            },
        }),
    };
    let first = engine.refresh(&spec).await.unwrap();
    check_typed(&engine, &first, &input, &rows).await;
    let changed = [
        (1, None, Some(false), Some(-1)),
        (2, Some(""), None, Some(0)),
    ];
    input = engine
        .update(&UpdateSpec {
            dataset: input.dataset.dataset.clone(),
            operation: id("moved"),
            base: input.id.clone(),
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: typed_file(&root.path().join("moved.parquet"), &changed),
            deletions: None,
        })
        .await
        .unwrap();
    rows[..2].copy_from_slice(&changed);
    spec.operation = id("moved");
    spec.base = Some(first.id.clone());
    spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(aggregate) = &mut spec.transform else {
        unreachable!()
    };
    aggregate.input = input.version_ref();
    let result = engine.refresh(&spec).await.unwrap();
    check_typed(&engine, &result, &input, &rows).await;
    for (key, code) in [
        ("floating", "aggregate_key_type"),
        ("__docspec_private", "invalid_refresh"),
    ] {
        let mut refused = spec.clone();
        refused.dataset = id("refused");
        refused.operation = id("initial");
        refused.mode = RefreshMode::Full;
        refused.base = None;
        let RefreshTransform::Aggregate(aggregate) = &mut refused.transform else {
            unreachable!()
        };
        aggregate.grouping.keys = vec![key.into()];
        assert_eq!(engine.refresh(&refused).await.unwrap_err().code(), code);
    }
}
