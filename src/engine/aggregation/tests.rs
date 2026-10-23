use super::*;
use arrow::{
    array::{Array, Date32Array, Int64Array, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::fs::File;

#[test]
fn qualified_row_limit_bounds_every_signed_partial_sum() {
    let rows = i64::MAX as u64;
    check_row_count(rows).unwrap();
    assert_eq!(
        check_row_count(rows + 1).unwrap_err().code(),
        "aggregate_range"
    );
    // Worst possible signed operand magnitude, even for an all-negative partition.
    let worst = i128::from(i64::MIN)
        .abs()
        .checked_mul(i128::from(rows))
        .unwrap();
    assert!(worst < 10_i128.pow(38));
}

#[tokio::test]
async fn date32_keys_and_count_distinct_refresh_full_and_incrementally() {
    let root = tempfile::tempdir().unwrap();
    let write_input = |name: &str, rows: &[(i64, Option<i32>, Option<&str>)]| {
        let path = root.path().join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("day", DataType::Date32, true),
            Field::new("author", DataType::Utf8, true),
            Field::new("resolution", DataType::Utf8, true),
            Field::new("headline", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Date32Array::from_iter(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|_| Some("exact_match")),
                )),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|_| Some("unneeded payload body")),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(&path).unwrap().0,
        }
    };
    let id = |name| Id::new(name).unwrap();
    let limits = Limits {
        scratch_bytes: 16 << 20,
        ..Limits::default()
    };
    let engine = Engine::open(root.path().join("workspace"), limits.clone())
        .await
        .unwrap();
    let initial_input = write_input(
        "initial.parquet",
        &[
            (1, Some(19_000), Some("alice")),
            (2, Some(19_000), Some("alice")),
            (3, Some(19_000), Some("bob")),
            (4, Some(19_001), None),
            (5, None, Some("carol")),
        ],
    );
    let source = engine
        .run(&RunSpec {
            dataset: id("events"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: initial_input.path,
            expected_digest: initial_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let mut spec = RefreshSpec {
        dataset: id("daily-authors"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: source.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["day".into()],
                aggregates: vec![
                    AggregateField {
                        name: "rows".into(),
                        function: AggregateFunction::CountRows,
                    },
                    AggregateField {
                        name: "distinct_authors".into(),
                        function: AggregateFunction::CountDistinct {
                            field: "author".into(),
                        },
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };
    let first = engine.refresh(&spec).await.unwrap();
    verify_release(&first, limits.clone()).await.unwrap();

    let RefreshTransform::Aggregate(aggregate_spec) = &spec.transform else {
        unreachable!()
    };
    let budget = &engine.inner.lifecycle.budget;
    let context = engine.context();
    let releases = BTreeMap::from([(id("events"), &source)]);
    register_input(&context, aggregate_spec, &releases, budget)
        .await
        .unwrap();
    let full_frame = aggregate_frame_with_transform(&context, aggregate_spec, "aggregate_input")
        .await
        .unwrap();
    let full_plan = full_frame.create_physical_plan().await.unwrap();
    let full_rendered = datafusion::physical_plan::displayable(full_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        full_rendered.contains("AggregateExec: mode=Partial")
            && full_rendered.contains("AggregateExec: mode=Final"),
        "expected hash AggregateExec in plan:\n{full_rendered}"
    );
    assert!(
        !full_rendered.contains("SortExec"),
        "unexpected SortExec in hash aggregate plan:\n{full_rendered}"
    );
    assert!(
        full_rendered.contains("NativeScan: rows=Some(5), projection=[day, author]"),
        "expected projected NativeScan of only [day, author] in plan:\n{full_rendered}"
    );
    for omitted in [
        "id",
        "resolution",
        "headline",
        storage::OCCURRENCE_FILE,
        storage::OCCURRENCE_ROW,
    ] {
        assert!(
            !full_rendered.contains(omitted),
            "unneeded column {omitted} leaked into aggregate physical plan:\n{full_rendered}"
        );
    }

    let updated = engine
        .update(&UpdateSpec {
            dataset: id("events"),
            operation: id("updated"),
            base: source.id.clone(),
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: write_input(
                "replacements.parquet",
                &[
                    (2, Some(19_000), Some("dave")),
                    (4, Some(19_001), Some("erin")),
                ],
            ),
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    spec.operation = id("incremental");
    spec.base = Some(first.id.clone());
    spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(aggregate) = &mut spec.transform else {
        unreachable!()
    };
    aggregate.input = updated.version_ref();
    let second = engine.refresh(&spec).await.unwrap();
    verify_release(&second, limits).await.unwrap();

    let stream = engine.read_group(&second, "source").await.unwrap();
    let batches = datafusion::physical_plan::common::collect(stream)
        .await
        .unwrap();
    let mut actual = BTreeMap::new();
    for batch in batches {
        let days = batch
            .column_by_name("day")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let rows = batch
            .column_by_name("rows")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let authors = batch
            .column_by_name("distinct_authors")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let day = (!days.is_null(row)).then(|| days.value(row));
            actual.insert(day, (rows.value(row), authors.value(row)));
        }
    }
    assert_eq!(
        actual,
        BTreeMap::from([
            (None, (1, 1)),
            (Some(19_000), (3, 3)),
            (Some(19_001), (1, 1))
        ])
    );
}

#[tokio::test]
async fn hot_group_distinct_and_collect_set_respect_memory_and_incremental_refresh() {
    use crate::{
        Engine,
        budget::Limits,
        engine::{verify_applied_changes, verify_release},
        model::{
            AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, Id,
            InputSpec, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform, RunSpec,
            UpdateSpec,
        },
        storage,
    };
    use arrow::{
        array::{Array, Date32Array, Int64Array, ListArray, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use std::{collections::BTreeMap, fs::File, sync::Arc};

    let root = tempfile::TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Date32, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let write_input = |name: &str, rows: &[(i64, Option<i32>, Option<String>)]| {
        let path = root.path().join(name);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Date32Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| format!("payload-{}", r.0)),
                )),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(&path).unwrap().0,
        }
    };
    let id = |name| Id::new(name).unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 64,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    };
    let engine = Engine::open(root.path().join("workspace"), limits.clone())
        .await
        .unwrap();

    let mut initial_rows = Vec::new();
    for index in 0..128i64 {
        let author = format!("author-{index:03}");
        initial_rows.push((index * 2 + 1, Some(19_000), Some(author.clone())));
        initial_rows.push((index * 2 + 2, Some(19_000), Some(author)));
    }
    initial_rows.push((300, Some(19_000), None));
    initial_rows.push((301, Some(19_001), None));
    initial_rows.push((302, None, Some("zoe".into())));
    initial_rows.push((303, None, Some("amy".into())));
    initial_rows.push((304, None, Some("zoe".into())));

    let initial_input = write_input("initial.parquet", &initial_rows);
    let source = engine
        .run(&RunSpec {
            dataset: id("events"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: initial_input.path,
            expected_digest: initial_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    let mut spec = RefreshSpec {
        dataset: id("daily-authors"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: source.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["day".into()],
                aggregates: vec![
                    AggregateField {
                        name: "rows".into(),
                        function: AggregateFunction::CountRows,
                    },
                    AggregateField {
                        name: "distinct_authors".into(),
                        function: AggregateFunction::CountDistinct {
                            field: "author".into(),
                        },
                    },
                    AggregateField {
                        name: "author_set".into(),
                        function: AggregateFunction::CollectSet {
                            field: "author".into(),
                        },
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };

    let RefreshTransform::Aggregate(aggregate_spec) = &spec.transform else {
        unreachable!()
    };
    let budget = &engine.inner.lifecycle.budget;
    let context = engine.context();
    let releases = BTreeMap::from([(id("events"), &source)]);
    register_input(&context, aggregate_spec, &releases, budget)
        .await
        .unwrap();
    let full_frame = aggregate_frame_with_transform(&context, aggregate_spec, "aggregate_input")
        .await
        .unwrap();
    let full_plan = full_frame.create_physical_plan().await.unwrap();
    let full_rendered = datafusion::physical_plan::displayable(full_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        full_rendered.contains("AggregateExec: mode=Partial")
            && full_rendered.contains("AggregateExec: mode=Final"),
        "expected hash AggregateExec in plan:\n{full_rendered}"
    );
    assert!(
        !full_rendered.contains("SortExec"),
        "unexpected SortExec in CollectSet + CountDistinct plan:\n{full_rendered}"
    );
    assert!(
        full_rendered.contains("projection=[day, author]"),
        "expected projected NativeScan of only [day, author] in plan:\n{full_rendered}"
    );
    assert!(
        !full_rendered.contains("payload"),
        "unneeded payload column leaked into aggregate physical plan:\n{full_rendered}"
    );
    drop((full_plan, context));

    let first = engine.refresh(&spec).await.unwrap();
    verify_release(&first, limits.clone()).await.unwrap();

    let read_summary = |engine: &Engine, release: &crate::model::ReleasePin| {
        let engine = engine.clone();
        let release = release.clone();
        async move {
            let stream = engine.read_group(&release, "source").await.unwrap();
            let batches = datafusion::physical_plan::common::collect(stream)
                .await
                .unwrap();
            let mut actual = BTreeMap::new();
            for batch in batches {
                let days = batch
                    .column_by_name("day")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap();
                let rows = batch
                    .column_by_name("rows")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let distinct = batch
                    .column_by_name("distinct_authors")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let author_set = batch
                    .column_by_name("author_set")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert!(!author_set.is_null(row));
                    let values = author_set.value(row);
                    let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
                    let list: Vec<String> = (0..strings.len())
                        .map(|idx| {
                            assert!(!strings.is_null(idx));
                            strings.value(idx).to_owned()
                        })
                        .collect();
                    let day = (!days.is_null(row)).then(|| days.value(row));
                    actual.insert(day, (rows.value(row), distinct.value(row), list));
                }
            }
            actual
        }
    };

    let expected_hot: Vec<String> = (0..128).map(|idx| format!("author-{idx:03}")).collect();
    assert_eq!(
        read_summary(&engine, &first).await,
        BTreeMap::from([
            (None, (3, 2, vec!["amy".to_owned(), "zoe".to_owned()])),
            (Some(19_000), (257, 128, expected_hot.clone())),
            (Some(19_001), (1, 0, Vec::new())),
        ])
    );

    let updated = engine
        .update(&UpdateSpec {
            dataset: id("events"),
            operation: id("updated"),
            base: source.id.clone(),
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: write_input(
                "replacements.parquet",
                &[
                    (1, Some(19_000), Some("author-999".into())),
                    (2, Some(19_000), None),
                    (301, Some(19_001), Some("bea".into())),
                ],
            ),
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    spec.operation = id("incremental");
    spec.base = Some(first.id.clone());
    spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(aggregate) = &mut spec.transform else {
        unreachable!()
    };
    aggregate.input = updated.version_ref();
    let second = engine.refresh(&spec).await.unwrap();
    verify_release(&second, limits.clone()).await.unwrap();
    let cert = engine
        .change_certificate(&second.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&first, &second, &cert.changes, limits.clone())
        .await
        .unwrap();

    let mut expected_hot_after: Vec<String> =
        (1..128).map(|idx| format!("author-{idx:03}")).collect();
    expected_hot_after.push("author-999".into());
    assert_eq!(
        read_summary(&engine, &second).await,
        BTreeMap::from([
            (None, (3, 2, vec!["amy".to_owned(), "zoe".to_owned()])),
            (Some(19_000), (257, 128, expected_hot_after)),
            (Some(19_001), (1, 1, vec!["bea".to_owned()])),
        ])
    );

    // Exhausting either the byte admission semaphore or the shared DataFusion spill
    // pool during hot-group distinct accumulation refuses without advancing selection.
    let mut held = Vec::new();
    while let Ok(permit) = engine.inner.lifecycle.budget.reserve(16 << 20) {
        held.push(permit);
    }
    let mut refused = spec.clone();
    refused.operation = id("tight-memory-refusal");
    refused.base = Some(second.id.clone());
    refused.mode = RefreshMode::Full;
    assert_eq!(
        engine.refresh(&refused).await.unwrap_err().code(),
        "capacity"
    );
    drop(held);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), second);

    let pool_reservation =
        datafusion::execution::memory_pool::MemoryConsumer::new("hot-group-ceiling")
            .register(&engine.inner.lifecycle.budget.runtime_env().memory_pool);
    pool_reservation
        .try_grow((limits.memory_bytes / 2) - 256)
        .unwrap();
    let mut refused_pool = spec.clone();
    refused_pool.operation = id("tight-pool-refusal");
    refused_pool.base = Some(second.id.clone());
    refused_pool.mode = RefreshMode::Full;
    assert_eq!(
        engine.refresh(&refused_pool).await.unwrap_err().code(),
        "query"
    );
    drop(pool_reservation);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), second);
}
