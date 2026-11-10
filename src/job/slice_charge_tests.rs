//! Operators fed by an outer join are charged for the rows they hold, not per slice.
//!
//! A join that emits its unmatched build rows at the end hands them on as
//! zero-copy `batch_rows` slices of one batch, and every operator that
//! charges a batch by its buffers then counts that whole batch per slice.
//! Tests that compare against plans without the compaction rule plan them
//! with `budget::without_retained_compaction` over the same catalog.
// Rust guideline compliant 2026-02-21
use super::*;
use arrow::datatypes::DataType;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, collect, displayable};
use std::sync::Arc;

const ROWS: usize = 200_000;
const MATCHED: usize = 2000;

/// The outer join whose unmatched `big` rows leave as slices of one batch.
const LEFT_JOIN: &str = "big LEFT JOIN mid ON big.k = mid.k";

/// A job retaining `big` (key, 200-byte body, payload), `mid` and `tiny`.
///
/// Retained steps carry no statistics, so every join builds on its left
/// input. Returns the bytes `big` retains.
async fn retained_job(payload: &str) -> (tempfile::TempDir, Job, usize) {
    let scratch = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            compute_threads: 2,
            // A 4 GiB pool admits an inflated charge so the test reports it.
            memory_bytes: 8 << 30,
            ..Limits::default()
        },
        scratch.path().join("scratch"),
    )
    .unwrap();
    let (storage, rows, retained) = job
        .materialize_detailed(
            "big",
            &format!(
                "SELECT value AS k, lpad(cast(value AS varchar), 200, 'b') AS body, \
                 {payload} AS payload FROM generate_series(1, {ROWS}) AS t(value)"
            ),
        )
        .await
        .unwrap();
    assert_eq!((storage, rows), (StepStorage::Memory, ROWS));
    for (name, keys) in [("mid", MATCHED), ("tiny", 1)] {
        let sql = format!("SELECT value AS k FROM generate_series(1, {keys}) AS t(value)");
        job.materialize(name, &sql).await.unwrap();
    }
    (scratch, job, retained)
}

/// Every operator called `name` in `plan`, outermost first.
fn operators(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    if plan.name() == name {
        found.push(plan.clone());
    }
    for child in plan.children() {
        found.extend(operators(child, name));
    }
    found
}

/// The sum of an executed operator's metric `name` over its partitions.
fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |value| value.as_usize())
}

/// Bytes an executed join charged the pool for its build side.
fn build_charge(join: &Arc<dyn ExecutionPlan>) -> usize {
    metric(join, "build_mem_used")
}

/// The compaction wraps placed directly over an operator called `producer`.
fn wraps_over(plan: &Arc<dyn ExecutionPlan>, producer: &str) -> Vec<Arc<dyn ExecutionPlan>> {
    operators(plan, "CompactViews")
        .into_iter()
        .filter(|wrap| wrap.children()[0].name() == producer)
        .collect()
}

/// Runs `sql` in `context` and returns the executed plan and its rows, sorted.
///
/// Sorted row encodings compare two plans' results as multisets, nested
/// values and nulls included, whatever order the plans emit them in.
async fn execute(
    context: &SessionContext,
    sql: &str,
) -> datafusion::common::Result<(Arc<dyn ExecutionPlan>, Vec<arrow::row::OwnedRow>)> {
    let frame = context.sql(sql).await?;
    let task = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let batches = collect(plan.clone(), task).await?;
    let converter = arrow::row::RowConverter::new(
        plan.schema()
            .fields()
            .iter()
            .map(|field| arrow::row::SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let mut rows = Vec::new();
    for batch in &batches {
        let converted = converter.convert_columns(batch.columns())?;
        rows.extend(converted.iter().map(|row| row.owned()));
    }
    rows.sort_unstable();
    Ok((plan, rows))
}

/// Holds all of the budget's shared admission but `spare` bytes, as retained
/// spills and window reservations can.
fn hold_admission(budget: &Budget, spare: usize) -> Vec<tokio::sync::OwnedSemaphorePermit> {
    let room = budget.reserve(spare).unwrap();
    let mut held = Vec::new();
    for chunk in [1 << 30, 1 << 20, 1 << 10, 1] {
        while let Ok(permit) = budget.reserve(chunk) {
            held.push(permit);
        }
    }
    drop(room);
    held
}

/// `big` with string-view text, as raw Parquet inputs are read; it needs the
/// string payload, since retained steps hold text.
const BIG_VIEWS: &str = "(SELECT k, arrow_cast(body, 'Utf8View') AS body, \
                         arrow_cast(payload, 'Utf8View') AS payload FROM big) AS big";

#[tokio::test]
async fn refused_compaction_keeps_the_batch_and_the_rows() {
    let (_scratch, job, _) = retained_job(payloads()[1].1).await;
    let native = crate::budget::without_retained_compaction(&job.context);
    let cases = [
        // New with the outer-join wrap: its output shares the build's views.
        (
            "outer join output",
            format!(
                "SELECT big.k, big.body FROM {BIG_VIEWS} LEFT JOIN mid ON big.k = mid.k \
                 CROSS JOIN tiny"
            ),
            "HashJoinExec",
        ),
        // Wrapped before the outer-join wrap: slices of the grouped views.
        (
            "aggregate output",
            format!("SELECT body, count(*) AS n FROM {BIG_VIEWS} GROUP BY body"),
            "AggregateExec",
        ),
    ];
    // Leave less than one 8,192-row view copy (about 2.7 MB) admissible.
    let _held = hold_admission(&job.budget, 1 << 20);
    let mut failures = Vec::new();
    for (case, sql, producer) in &cases {
        let (_, expected) = execute(&native, sql)
            .await
            .unwrap_or_else(|error| panic!("{case} without the rule: {error}"));
        let (plan, rows) = match execute(&job.context, sql).await {
            Ok(result) => result,
            Err(error) => {
                failures.push(format!("{case}: {error}"));
                continue;
            }
        };
        assert_eq!(rows.len(), expected.len(), "{case}");
        assert!(rows == expected, "{case}: rows differ from the native plan");
        // The fallback ran where the copy was refused, not merely nowhere.
        let refused = wraps_over(&plan, producer)
            .iter()
            .map(|wrap| metric(wrap, "refused_copies"))
            .sum::<usize>();
        if refused == 0 {
            failures.push(format!(
                "{case}: no refused copy over {producer}:\n{}",
                displayable(plan.as_ref()).indent(true)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Runs `sql`, checks it returns every `big` row with the payload intact in
/// type, and returns the executed plan with the pool's peak above its start.
async fn run(
    job: &Job,
    sql: &str,
    payload: fn(&DataType) -> bool,
) -> (Arc<dyn ExecutionPlan>, usize) {
    let frame = job.sql(sql).await.unwrap();
    let context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await.unwrap();
    let before = job.budget.runtime_env().memory_pool.reserved();
    job.budget.restart_pool_peak();
    let batches = collect(plan.clone(), context).await.unwrap();
    let peak = job.budget.pool_peak() - before;
    let column = batches[0].schema().index_of("payload").unwrap();
    assert!(payload(batches[0].column(column).data_type()), "{sql}");
    let rows = batches
        .iter()
        .map(|batch| batch.num_rows() - batch.column(column).null_count())
        .sum::<usize>();
    assert_eq!(rows, ROWS, "{sql}");
    (plan, peak)
}

/// A payload's name, its SQL expression and the check of its retained type.
type Payload = (&'static str, &'static str, fn(&DataType) -> bool);

/// The list payload and its control: the same bytes in a plain string column,
/// which the guard measured correctly before nested columns were.
fn payloads() -> [Payload; 2] {
    [
        (
            "list",
            "make_array(lpad(cast(value AS varchar), 150, 'x'), \
             lpad(cast(value AS varchar), 150, 'y'))",
            |data_type| matches!(data_type, DataType::List(item) if item.data_type() == &DataType::Utf8),
        ),
        (
            "string",
            "lpad(cast(value AS varchar), 300, 'x')",
            |data_type| data_type == &DataType::Utf8,
        ),
    ]
}

#[tokio::test]
async fn join_builds_charge_sliced_join_output_once() {
    let columns = "big.k, big.body, big.payload";
    // (case, query, operator whose build is read, operator emitting the slices)
    let cases = [
        (
            "hash join build",
            format!("SELECT {columns} FROM {LEFT_JOIN} LEFT JOIN tiny ON big.k = tiny.k"),
            "HashJoinExec",
            "HashJoinExec",
        ),
        (
            "nested loop join build",
            format!("SELECT {columns} FROM {LEFT_JOIN} LEFT JOIN tiny ON big.k + tiny.k = 2"),
            "NestedLoopJoinExec",
            "HashJoinExec",
        ),
        (
            "cross join build",
            format!("SELECT {columns} FROM {LEFT_JOIN} CROSS JOIN tiny"),
            "CrossJoinExec",
            "HashJoinExec",
        ),
        (
            // No row matches, so the nested loop join emits every `big` row
            // as an unfiltered range of its build batch.
            "cross join build after a nested loop join",
            format!(
                "SELECT {columns} FROM big LEFT JOIN tiny AS absent ON big.k + absent.k = 0 \
                 CROSS JOIN tiny"
            ),
            "CrossJoinExec",
            "NestedLoopJoinExec",
        ),
    ];
    let mut charges = Vec::new();
    for (name, payload, expected) in payloads() {
        let (_scratch, job, retained) = retained_job(payload).await;
        for (case, sql, consumer, producer) in &cases {
            let (plan, _) = run(&job, sql, expected).await;
            let consumer = operators(&plan, consumer)
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("{case}:\n{}", displayable(plan.as_ref()).indent(true)));
            // The reproduced shape: the build input is the outer join's output.
            assert!(
                !operators(consumer.children()[0], producer).is_empty(),
                "{case}:\n{}",
                displayable(plan.as_ref()).indent(true)
            );
            // One guard between the two: the producer's wrap is not wrapped again.
            assert!(
                operators(&plan, "CompactViews")
                    .iter()
                    .all(|guard| guard.children()[0].name() != "CompactViews"),
                "{case}:\n{}",
                displayable(plan.as_ref()).indent(true)
            );
            charges.push((*case, name, build_charge(&consumer), retained));
        }
    }
    eprintln!("build charged bytes against retained step bytes: {charges:#?}");
    // The whole left relation plus the join's own state. Unguarded, every
    // batch_rows slice of the unmatched rows is charged for all of them.
    let inflated = charges
        .iter()
        .filter(|(_, _, charged, retained)| *charged >= 2 * *retained)
        .collect::<Vec<_>>();
    assert!(
        inflated.is_empty(),
        "builds charged at twice the retained step bytes or more: {inflated:#?}"
    );
}

#[tokio::test]
async fn round_robin_repartition_charges_sliced_join_output_once() {
    let mut charges = Vec::new();
    for (name, payload, expected) in payloads() {
        let (_scratch, job, retained) = retained_job(payload).await;
        // The limit leaves one partition; the filter above it is spread
        // round-robin, so whole join output batches wait in its channels.
        let sql = format!(
            "SELECT k, body, payload FROM (SELECT big.k, big.body, big.payload, mid.k AS mid_k \
             FROM {LEFT_JOIN} LIMIT {ROWS}) AS joined WHERE mid_k IS NULL OR mid_k > 0"
        );
        let (plan, peak) = run(&job, &sql, expected).await;
        let text = displayable(plan.as_ref()).indent(true).to_string();
        let repartition = operators(&plan, "RepartitionExec")
            .into_iter()
            .find(|repartition| {
                matches!(
                    repartition.properties().partitioning,
                    Partitioning::RoundRobinBatch(_)
                )
            })
            .unwrap_or_else(|| panic!("no round-robin repartition:\n{text}"));
        assert!(
            !operators(&repartition, "HashJoinExec").is_empty(),
            "{text}"
        );
        let build = operators(&plan, "HashJoinExec")
            .iter()
            .map(build_charge)
            .sum::<usize>();
        // What the pool held beyond the join's own build while it ran.
        charges.push((name, peak.saturating_sub(build), retained));
    }
    eprintln!("pool peak above the join build against retained step bytes: {charges:?}");
    assert!(
        charges
            .iter()
            .all(|(_, waiting, retained)| *waiting < *retained / 4),
        "pool peak above the join build against retained step bytes: {charges:?}"
    );
}
