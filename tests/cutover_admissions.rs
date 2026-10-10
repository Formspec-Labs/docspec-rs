//! Cutover accounts for unresolved source requests below the selected boundary.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::DataType,
};
use docspec::{
    Engine,
    model::{PhaseResult, ReleasePin, RunSpec, SchemaField, SchemaSpec},
};
use futures::TryStreamExt;
use scalar::{fixture, id, input, limits, spec};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, time::SystemTime};

fn build(root: &Path, operation: &str, rows: &[scalar::Row]) -> RunSpec {
    let source = input(&root.join(format!("{operation}.parquet")), rows);
    RunSpec {
        dataset: id("comments"),
        operation: id(operation),
        base: None,
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

fn schema(base: &ReleasePin) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("schema-cutover"),
        base: base.id.clone(),
        definition: "v1".into(),
        next_definition: "v2".into(),
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".into(),
            },
            SchemaField {
                id: 2,
                name: "body".into(),
            },
        ],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    }
}

async fn hidden_admission(
    root: &Path,
    engine: &Engine,
    stop: &str,
) -> (RunSpec, ReleasePin, Vec<scalar::Row>) {
    let earlier = build(root, "earlier", &[(Some(8), Some("unselected".into()))]);
    assert_eq!(
        engine.run_until(&earlier, stop).await.unwrap_err().code(),
        "injected_failure"
    );
    let rows = vec![
        (Some(2), Some("é🎉".into())),
        (Some(2), Some(String::new())),
        (None, None),
    ];
    let later = engine.run(&build(root, "later", &rows)).await.unwrap();
    let records = engine.inspect(&earlier.dataset).await.unwrap();
    assert!(records["operation/earlier"]["boundary"].as_u64().unwrap() < later.dataset.boundary);
    assert!(!records.contains_key("result/earlier"));
    if stop == "readback" {
        assert!(records.contains_key("readback/earlier"));
    }
    assert_eq!(engine.latest(&earlier.dataset).await.unwrap(), later);
    (earlier, later, rows)
}

fn completed_work(records: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    records
        .iter()
        .filter(|(key, _)| {
            key.starts_with("phase/")
                || key.starts_with("readback/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("backfill-readback/")
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn file_times(release: &ReleasePin) -> BTreeMap<String, SystemTime> {
    std::iter::once(&release.manifest)
        .chain(
            release
                .dataset
                .groups
                .values()
                .flat_map(|group| std::iter::once(&group.metadata).chain(group.objects.iter())),
        )
        .map(|object| {
            (
                object.path.clone(),
                std::fs::metadata(&object.path).unwrap().modified().unwrap(),
            )
        })
        .collect()
}

fn assert_work_retained(before: &BTreeMap<String, Value>, after: &BTreeMap<String, Value>) {
    for (key, value) in before {
        assert_eq!(after[key], *value, "completed work changed: {key}");
    }
}

async fn assert_values(
    engine: &Engine,
    release: &ReleasePin,
    rows: Vec<scalar::Row>,
    computed: bool,
) {
    let mut stream = engine.read_combined(release).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lengths =
            computed.then(|| arrow::compute::cast(batch.column(2), &DataType::Int64).unwrap());
        let lengths = lengths
            .as_ref()
            .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap());
        actual.extend(
            ids.iter()
                .zip(texts.iter())
                .enumerate()
                .map(|(index, (id, text))| {
                    (
                        (id, text.map(str::to_owned)),
                        lengths.and_then(|values| {
                            (!values.is_null(index)).then(|| values.value(index))
                        }),
                    )
                }),
        );
    }
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let length = if computed {
                row.1.as_ref().map(|text| text.chars().count() as i64)
            } else {
                None
            };
            (row, length)
        })
        .collect();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn backfill_refuses_hidden_admission_and_reuses_completed_work_after_abandonment() {
    for stop in ["admission", "processing", "readback"] {
        backfill_hidden_admission_case(stop, false).await;
    }
}

#[tokio::test]
async fn format_10_backfill_refuses_hidden_admission_and_reuses_exact_completion() {
    backfill_hidden_admission_case("readback", true).await;
}

async fn backfill_hidden_admission_case(stop: &str, previous_format: bool) {
    let root = tempfile::tempdir().unwrap();
    let (engine, _, _) = fixture(root.path(), false).await;
    let (earlier, latest, rows) = hidden_admission(root.path(), &engine, stop).await;
    let request = spec(&latest, "cutover", "length", "text_length", "length(text)");
    if previous_format {
        assert_eq!(
            engine
                .backfill_until(&request, "files")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        super::admission_formats::original_admission(root.path(), "cutover", false).await;
    }
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.promotion_refusal.as_deref(), Some("pending_source"));
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "pending_source"
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), latest);
    let records = engine.inspect(&request.dataset).await.unwrap();
    assert!(!records.contains_key("result/cutover"));
    let work = completed_work(&records);
    assert!(work.keys().any(|key| key.starts_with("phase/backfill/")));
    assert!(
        work.keys()
            .any(|key| key.starts_with("backfill-validation/"))
    );
    let prepared: ReleasePin = serde_json::from_value(
        work.iter()
            .find(|(key, _)| key.starts_with("backfill-readback/cutover/"))
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    assert_eq!(
        prepared.dataset.format,
        if previous_format { 10 } else { 11 }
    );
    let times = file_times(&prepared);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "pending_source"
    );
    assert_work_retained(&work, &engine.inspect(&request.dataset).await.unwrap());
    engine
        .abandon(
            &request.dataset,
            &earlier.operation,
            "withdraw overtaken input",
        )
        .await
        .unwrap();
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert_eq!(plan.promotion_refusal, None);
    assert_eq!(plan.files_pending, Some(0));
    let selected = engine.backfill(&request).await.unwrap();
    assert_eq!(selected, prepared);
    assert_eq!(file_times(&selected), times);
    assert_work_retained(&work, &engine.inspect(&request.dataset).await.unwrap());
    assert_values(&engine, &selected, rows, true).await;
}

#[tokio::test]
async fn schema_refuses_hidden_admission_and_keeps_old_writer_until_exact_retry() {
    for stop in ["admission", "processing", "readback"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, _, _) = fixture(root.path(), false).await;
        let (earlier, latest, rows) = hidden_admission(root.path(), &engine, stop).await;
        let request = schema(&latest);
        assert_eq!(
            engine.evolve_schema(&request).await.unwrap_err().code(),
            "pending_source"
        );
        assert_eq!(engine.latest(&request.dataset).await.unwrap(), latest);
        let records = engine.inspect(&request.dataset).await.unwrap();
        assert_eq!(records["authority"]["definition"], "v1");
        assert!(!records.contains_key("result/schema-cutover"));
        let work = completed_work(&records);
        for phase in ["admission", "processing", "validation"] {
            assert!(work.values().any(|value| {
                serde_json::from_value::<PhaseResult>(value.clone()).is_ok_and(|result| {
                    result.operation == request.operation && result.phase == phase
                })
            }));
        }
        let prepared: ReleasePin =
            serde_json::from_value(records["readback/schema-cutover"].clone()).unwrap();
        let times = file_times(&prepared);
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_eq!(
            engine.evolve_schema(&request).await.unwrap_err().code(),
            "pending_source"
        );
        assert_work_retained(&work, &engine.inspect(&request.dataset).await.unwrap());
        engine
            .abandon(
                &request.dataset,
                &earlier.operation,
                "withdraw overtaken input",
            )
            .await
            .unwrap();
        let selected = engine.evolve_schema(&request).await.unwrap();
        assert_eq!(selected, prepared);
        assert_eq!(selected.dataset.definition, "v2");
        assert_eq!(
            engine.inspect(&request.dataset).await.unwrap()["authority"]["definition"],
            "v2"
        );
        assert_eq!(file_times(&selected), times);
        assert_work_retained(&work, &engine.inspect(&request.dataset).await.unwrap());
        assert_values(&engine, &selected, rows, false).await;
    }
}

#[tokio::test]
async fn retired_selected_history_does_not_become_a_source_backlog() {
    let root = tempfile::tempdir().unwrap();
    let (engine, prior, _) = fixture(root.path(), false).await;
    let rows = vec![(Some(4), Some("current".into()))];
    let latest = engine
        .run(&build(root.path(), "later", &rows))
        .await
        .unwrap();
    let availability = engine
        .plan_availability_retirement(
            &latest.dataset.dataset,
            &id("retire-prior-availability"),
            &prior.id,
            "end historical independent read promise",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &latest.dataset.dataset,
            &id("retire-prior-recovery"),
            &prior.id,
            "end historical retry promise",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let request = spec(&latest, "cutover", "length", "text_length", "length(text)");
    assert_eq!(
        engine
            .plan_backfill(&request)
            .await
            .unwrap()
            .promotion_refusal,
        None
    );
    let enriched = engine.backfill(&request).await.unwrap();
    assert_values(&engine, &enriched, rows.clone(), true).await;
    let selected = engine.evolve_schema(&schema(&enriched)).await.unwrap();
    assert_eq!(selected.dataset.definition, "v2");
    assert_values(&engine, &selected, rows, true).await;
}
