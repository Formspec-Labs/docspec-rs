//! Manual workspace profiling for lake maintenance planning; excluded from CI.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::{Date32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

/// Emulates hourly news polling, then prints a JSON maintenance profile.
///
/// Run with:
/// `cargo test --locked --all-features --lib engine::profile_tests -- --ignored --nocapture`
///
/// The cadence mirrors news_lake I5: one initial backfill load, then hourly
/// appends. File counts, byte distributions, native snapshot growth, and the
/// retention inspection feed `iceberg_optimization_report.md`.
#[tokio::test]
#[ignore = "manual profiling harness; run explicitly with --ignored"]
async fn profile_hourly_polling_workspace() {
    let root = tempfile::tempdir().unwrap();
    let id = |name: &str| Id::new(name).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("observed_at", DataType::Date32, true),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("provider", DataType::Utf8, true),
        Field::new("headline", DataType::Utf8, true),
    ]));
    let write_input = |name: &str, rows: Vec<(i64, i32, &str, &str, String)>| {
        let path = root.path().join(name);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.4.clone()),
                )),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(&path).unwrap().0,
        }
    };
    // Initial load plus 24 hourly polls of 60 new records each.
    let initial_rows = 4096;
    let poll_rows = 60;
    let polls = 24u64;
    let headline = " senator announces committee vote on appropriations ".repeat(3);
    let row = |n: i64| {
        (
            n,
            19_000 + (n % 30) as i32,
            "exact_match",
            "wire",
            headline.clone(),
        )
    };
    let initial_input = write_input("initial.parquet", (0..initial_rows).map(row).collect());
    let engine = Engine::open(root.path().join("workspace"), Limits::default())
        .await
        .unwrap();
    let mut base = engine
        .run(&RunSpec {
            dataset: id("news"),
            operation: id("backfill"),
            base: None,
            relationship: None,
            input: initial_input.path,
            expected_digest: initial_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    for poll in 0..polls {
        let first = initial_rows + (poll * poll_rows) as i64;
        let update_input = write_input(
            &format!("poll-{poll}.parquet"),
            (first..first + poll_rows as i64).map(row).collect(),
        );
        base = engine
            .update(&UpdateSpec {
                dataset: id("news"),
                operation: id(&format!("poll-{poll}")),
                base: base.id.clone(),
                definition: "v1".into(),
                keys: vec!["id".into()],
                replacements: update_input,
                deletions: None,
            })
            .await
            .unwrap();
    }
    let budget = &engine.inner.lifecycle.budget;
    let native = storage::open_pin(crate::engine::incremental::source(&base).unwrap(), budget)
        .await
        .unwrap();
    let inventory = storage::snapshot_files(&native, budget).await.unwrap();
    let mut sizes = inventory
        .data
        .iter()
        .map(|file| file.file_size_in_bytes())
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    let total_bytes = sizes.iter().copied().sum::<u64>();
    let snapshots = native.metadata().snapshots().count();
    let objects = crate::engine::incremental::source(&base)
        .unwrap()
        .objects
        .clone();
    let inspection = engine.inspect_retention(&objects).await.unwrap();
    let compacted = engine
        .compact(&crate::model::CompactionSpec {
            dataset: id("news"),
            operation: id("profile-compact"),
            version: base.id.clone(),
            group: "source".into(),
            files: vec![],
            target_file_bytes: 64 << 20,
        })
        .await
        .unwrap();
    println!(
        "PROFILE_JSON {}",
        serde_json::json!({
            "polls": polls,
            "rows": inventory.data.iter().map(|f| f.record_count()).sum::<u64>(),
            "data_files": sizes.len(),
            "data_bytes": total_bytes,
            "min_file_bytes": sizes.first().copied(),
            "median_file_bytes": sizes.get(sizes.len() / 2).copied(),
            "max_file_bytes": sizes.last().copied(),
            "native_snapshots": snapshots,
            "release_objects": objects.len(),
            "metadata_objects_read": inspection.metadata_objects_read,
            "protected_bytes": inspection.protected_bytes,
            "unmatched_bytes": inspection.unmatched_bytes,
            "compaction": {
                "input_files": compacted.input_files.len(),
                "output_files": compacted.output_files.len(),
                "input_bytes": compacted.input_bytes,
                "output_bytes": compacted.output_bytes,
                "rows": compacted.rows,
            },
        })
    );
}

/// Captures the physical plan of the Date32 + COUNT DISTINCT aggregate.
///
/// Emulates the news_lake `coverage_daily` refresh over a projected native
/// source and prints the rendered DataFusion physical plan for the
/// optimization report. Predicate behavior at the native scan is recorded:
/// projection composes into the scan, while filters remain a DataFusion
/// `FilterExec` above `NativeScan` until row-group predicate pushdown lands.
#[tokio::test]
#[ignore = "manual plan capture; run explicitly with --ignored"]
async fn capture_coverage_daily_aggregate_plan() {
    let root = tempfile::tempdir().unwrap();
    let id = |name: &str| Id::new(name).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Date32, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("headline", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..5)),
            Arc::new(Date32Array::from_iter((0..5).map(|n| Some(19_000 + n)))),
            Arc::new(arrow::array::StringArray::from_iter_values(
                (0..5).map(|n| format!("author-{}", n % 3)),
            )),
            Arc::new(arrow::array::StringArray::from_iter_values(
                (0..5).map(|n| format!("headline-{n}")),
            )),
        ],
    )
    .unwrap();
    let input = root.path().join("input.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.path().join("workspace"), Limits::default())
        .await
        .unwrap();
    let source = engine
        .run(&RunSpec {
            dataset: id("events"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let transform = AggregateTransform {
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
        },
    };
    let budget = &engine.inner.lifecycle.budget;
    let context = engine.context();
    let releases = BTreeMap::from([(id("events"), &source)]);
    aggregation::register_input(&context, &transform, &releases, budget)
        .await
        .unwrap();
    // A day-range predicate records how filters interact with the native scan.
    let frame = context
        .sql(
            "SELECT day, COUNT(*) AS rows, COUNT(DISTINCT author) AS distinct_authors \
             FROM aggregate_input WHERE day >= DATE '2022-01-01' GROUP BY day",
        )
        .await
        .unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    println!(
        "PHYSICAL_PLAN\n{}",
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
    );
}
