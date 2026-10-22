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
            partition_by: Vec::new(),
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
                merge: None,
                relationship: None,
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
            partition_by: Vec::new(),
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
            filter: None,
            projections: BTreeMap::new(),
        },
        filter: None,
        projections: BTreeMap::new(),
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
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
}

/// Profiles lifecycle replay bytes, snapshot/file counts, cold-open latency, and warm-commit
/// scaling across 0, 8, 32, and 250 operations before and after `compact_lifecycle`.
///
/// Run with:
/// `cargo test --locked --all-features --lib engine::profile_tests::profile_lifecycle_compaction_and_warm_commit_scaling -- --ignored --nocapture`
#[tokio::test]
#[ignore = "manual profiling harness; run explicitly with --ignored"]
async fn profile_lifecycle_compaction_and_warm_commit_scaling() {
    let root = tempfile::tempdir().unwrap();
    let id = |name: &str| Id::new(name).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let path = root.path().join("init.parquet");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..16)),
            Arc::new(Int64Array::from_iter_values(0..16)),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let input_path = path.to_string_lossy().into_owned();
    let input_digest = storage::digest(&path).unwrap().0;

    let engine = Engine::open(root.path().join("workspace"), Limits::default())
        .await
        .unwrap();

    let checkpoints = [0_usize, 8, 32, 250];
    let mut samples = Vec::new();

    for &target in &checkpoints {
        let dataset = id(&format!("events-{target}"));
        let first = engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: id("op-0"),
                base: None,
                relationship: None,
                input: input_path.clone(),
                expected_digest: input_digest.clone(),
                sql: "SELECT id, val FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let mut latest = first.clone();
        let mut view = engine.inner.lifecycle.open(&dataset).await.unwrap();

        for step in 1..=target {
            let name = format!("op-{step}");
            let req = RunSpec {
                dataset: dataset.clone(),
                operation: id(&name),
                base: Some(latest.id.clone()),
                relationship: None,
                input: input_path.clone(),
                expected_digest: input_digest.clone(),
                sql: format!("SELECT id, val FROM input WHERE id >= -{step}"),
                definition: "v1".into(),
                partition_by: Vec::new(),
            };
            let request_digest = fingerprint(&OperationRequest::Build(req.clone())).unwrap();
            let op = Operation {
                request_digest: request_digest.clone(),
                eligibility: 0,
                epoch: 0,
                boundary: (step + 1) as u64,
                admission_fence: None,
                result_format: FORMAT,
                definition: "v1".into(),
                base: Some(latest.id.clone()),
                base_manifest: Some(latest.manifest.clone()),
                schema_key: None,
                require_current_boundary: false,
            };
            let phase = PhaseResult {
                operation: req.operation.clone(),
                phase: "processing".into(),
                request_digest: request_digest.clone(),
                eligibility: 0,
                outputs: latest.dataset.groups["source"].objects.clone(),
                table: Some(latest.dataset.groups["source"].clone()),
                rows: latest.dataset.groups["source"].rows,
                changes: None,
                coverage: None,
                metrics: WriteMetrics::default(),
                dependencies: Some(PhaseDependencies::default()),
            };
            let mut validation = phase.clone();
            validation.phase = "validation".into();
            let mut next_pin = latest.clone();
            next_pin.id = req.operation.clone();
            next_pin.dataset.version = req.operation.clone();
            next_pin.dataset.boundary = op.boundary;
            next_pin.dataset.request = OperationRequest::Build(req.clone());
            let link = crate::model::Succession {
                selected: next_pin.version_ref(),
                predecessor: Some(latest.version_ref()),
                boundary: op.boundary,
                eligibility: op.eligibility,
            };
            let authority = Authority {
                definition: "v1".into(),
                eligibility: 0,
                epoch: 0,
                boundary: op.boundary,
                selected: Some(next_pin.dataset.clone()),
                release: Some(next_pin.clone()),
                selection_revision: Some(view.revision + 2),
            };
            view = engine
                .inner
                .lifecycle
                .commit(
                    &view,
                    vec![
                        crate::catalog::record(format!("operation/{name}"), &op).unwrap(),
                        crate::catalog::record(
                            format!("request/{name}"),
                            OperationRequest::Build(req.clone()),
                        )
                        .unwrap(),
                        crate::catalog::record(
                            format!("phase/{request_digest}/0/processing"),
                            &phase,
                        )
                        .unwrap(),
                        crate::catalog::record(
                            format!("phase/checked/{request_digest}/validation"),
                            &validation,
                        )
                        .unwrap(),
                        crate::catalog::record(
                            format!("readback/{name}"),
                            ValidatedReadback::stamp(&next_pin),
                        )
                        .unwrap(),
                    ],
                )
                .await
                .unwrap();
            view = engine
                .inner
                .lifecycle
                .commit(
                    &view,
                    vec![
                        crate::catalog::record(format!("result/{name}"), &next_pin).unwrap(),
                        crate::catalog::record(format!("succession/{name}"), &link).unwrap(),
                        crate::catalog::record("authority", &authority).unwrap(),
                    ],
                )
                .await
                .unwrap();
            latest = next_pin;
        }

        // Measure warm-commit latency at this operation count.
        let t_warm = std::time::Instant::now();
        let warm_view = engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![crate::catalog::record("invalidation/0", format!("probe-{target}")).unwrap()],
            )
            .await
            .unwrap();
        let warm_commit_us = t_warm.elapsed().as_micros() as u64;

        // Measure cold-open replay latency before compaction.
        let cold_before = crate::catalog::Lifecycle::new(
            engine.inner.lifecycle.catalog.clone(),
            engine.inner.lifecycle.budget.clone(),
        );
        let t0 = std::time::Instant::now();
        let rows_before = cold_before.rows_for_compaction(&warm_view).await.unwrap();
        let cold_open_before_us = t0.elapsed().as_micros() as u64;
        let authority_bytes = rows_before.record_bytes("authority");
        let history_bytes_before = rows_before.history_bytes();
        drop(rows_before);

        // Compact lifecycle baseline.
        let t_compact = std::time::Instant::now();
        let stats = engine.compact_lifecycle(&dataset).await.unwrap();
        let compact_us = t_compact.elapsed().as_micros() as u64;

        let view_after = engine.inner.lifecycle.open(&dataset).await.unwrap();
        let cold_after = crate::catalog::Lifecycle::new(
            engine.inner.lifecycle.catalog.clone(),
            engine.inner.lifecycle.budget.clone(),
        );
        let t1 = std::time::Instant::now();
        let rows_after = cold_after.rows(&view_after).await.unwrap();
        let cold_open_after_us = t1.elapsed().as_micros() as u64;

        let proof = engine
            .prove_succession(&first.version_ref(), &latest.version_ref())
            .await
            .unwrap();
        assert_eq!(proof.selections_advanced, target);

        samples.push(serde_json::json!({
            "operations": target,
            "revisions_before": stats.revisions_before,
            "revisions_after": stats.revisions_after,
            "files_before": stats.files_before,
            "files_after": stats.files_after,
            "rows_before": stats.rows_before,
            "rows_after": stats.rows_after,
            "metadata_bytes_before": stats.metadata_bytes_before,
            "metadata_bytes_after": stats.metadata_bytes_after,
            "authority_json_bytes": authority_bytes,
            "uncompacted_probe_history_bytes": history_bytes_before,
            "cold_open_before_us": cold_open_before_us,
            "cold_open_after_us": cold_open_after_us,
            "warm_commit_us": warm_commit_us,
            "compact_lifecycle_us": compact_us,
            "current_revision": stats.current_revision,
            "post_compact_history_bytes": rows_after.history_bytes(),
        }));
    }

    println!(
        "LIFECYCLE_COMPACTION_PROFILE_JSON {}",
        serde_json::to_string_pretty(&samples).unwrap()
    );
}
