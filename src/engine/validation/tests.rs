//! Current built-in checks preserve exact native values and historical acknowledgments.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use serde_json::Value;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 8 << 20,
    }
}
async fn fixture() -> (tempfile::TempDir, Engine, RunSpec) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(1)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let request = RunSpec {
        dataset: id("data"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    (root, engine, request)
}
async fn material(engine: &Engine, dataset: &Id) -> BTreeMap<String, (i64, Value)> {
    let view = engine.inner.lifecycle.open(dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    rows.prefix("phase/")
        .filter(|(key, _, _)| {
            key.ends_with("/processing")
                || key.ends_with("/admission")
                || key.ends_with("/evidence")
        })
        .map(|(key, revision, json)| (key.into(), (revision, serde_json::from_str(json).unwrap())))
        .collect()
}
async fn readback(engine: &Engine, request: &RunSpec) -> (i64, ValidatedReadback) {
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (revision, _, completed) = rows
        .get_record(&format!("readback/{}", request.operation))
        .unwrap()
        .unwrap();
    (revision, completed)
}
async fn prior_readback(
    engine: &Engine,
    request: &RunSpec,
    version: Option<&str>,
) -> (i64, ValidatedReadback) {
    let (_, mut completed) = readback(engine, request).await;
    completed.validator = version.map(str::to_owned);
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let key = format!("readback/{}", request.operation);
    let protections = engine.retained_protections(&view, &key).await.unwrap();
    // A compatibility fixture changes only the typed check identity. The exact
    // native bytes, public pin, and real protection binding remain unchanged.
    let records = completion_records(
        &request.dataset,
        &view,
        &key,
        &completed,
        protections.as_ref(),
    )
    .unwrap();
    engine.inner.lifecycle.commit(&view, records).await.unwrap();
    readback(engine, request).await
}
async fn exact_values(engine: &Engine, release: &ReleasePin) {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    values.sort();
    assert_eq!(values, [None, Some(1), Some(1)]);
}

#[tokio::test]
async fn new_routine_validator_reuses_exact_processing_and_retires_only_old_checks() {
    let (_root, engine, request) = fixture().await;
    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = material(&engine, &request.dataset).await;
    let spec = OperationRequest::Build(request.clone());
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, "operation/initial")
        .await
        .unwrap()
        .unwrap();
    let processing_key = super::super::phase_key(
        &spec,
        &operation.request_digest,
        operation.eligibility,
        "processing",
        None,
        operation.schema_key.as_ref(),
    )
    .unwrap();
    let mut old: PhaseResult = engine
        .inner
        .lifecycle
        .get(&view, &processing_key)
        .await
        .unwrap()
        .unwrap();
    old.phase = "validation".into();
    let old_key = material_phase_key(
        &spec,
        &operation.request_digest,
        operation.eligibility,
        "validation",
        None,
        operation.schema_key.as_ref(),
    )
    .unwrap();
    let protections = engine
        .retained_protections(&view, &processing_key)
        .await
        .unwrap();
    let records = completion_records(
        &request.dataset,
        &view,
        &old_key,
        &old,
        protections.as_ref(),
    )
    .unwrap();
    engine.inner.lifecycle.commit(&view, records).await.unwrap();
    let plan = engine.plan(&request).await.unwrap();
    assert!(
        plan.reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert!(
        plan.required_phases
            .iter()
            .any(|phase| phase == "validation")
    );
    std::fs::remove_file(&request.input).unwrap();
    assert_eq!(
        engine
            .run_until(&request, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(material(&engine, &request.dataset).await, before);
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let current_key = super::super::phase_key(
        &spec,
        &operation.request_digest,
        operation.eligibility,
        "validation",
        None,
        operation.schema_key.as_ref(),
    )
    .unwrap();
    assert_ne!(current_key, old_key);
    let current = rows
        .get_record::<PhaseResult>(&current_key)
        .unwrap()
        .unwrap();
    assert_eq!(current.2, old);
    assert_eq!(
        engine
            .inner
            .routine_checks
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(current.0 > rows.get_record::<PhaseResult>(&old_key).unwrap().unwrap().0);
    assert_eq!(
        engine
            .run_until(&request, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .inner
            .routine_checks
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let fresh = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    assert_eq!(
        engine
            .inner
            .lifecycle
            .rows(&fresh)
            .await
            .unwrap()
            .get_record::<PhaseResult>(&current_key)
            .unwrap()
            .unwrap(),
        current
    );
    let plan = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("old-check"),
            &[old_key],
            "end superseded routine check",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let release = engine.run(&request).await.unwrap();
    assert_eq!(material(&engine, &request.dataset).await, before);
    exact_values(&engine, &release).await;
}

#[tokio::test]
async fn older_readback_rechecks_once_after_phase_retirement_and_completed_result_stays_exact() {
    for version in [None, Some("docspec.dataset-validation.v0")] {
        let (root, engine, request) = fixture().await;
        assert_eq!(
            engine
                .run_until(&request, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let before = material(&engine, &request.dataset).await;
        let (old_revision, old) = prior_readback(&engine, &request, version).await;
        let keys = engine
            .inspect(&request.dataset)
            .await
            .unwrap()
            .keys()
            .filter(|key| key.starts_with("phase/"))
            .cloned()
            .collect::<Vec<_>>();
        let plan = engine
            .plan_phase_retirement(
                &request.dataset,
                &id("old-phases"),
                &keys,
                "readback owns qualified result",
            )
            .await
            .unwrap();
        engine.retire_phases(&plan).await.unwrap();
        std::fs::remove_file(&request.input).unwrap();
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let plan = engine.plan(&request).await.unwrap();
        assert_eq!(
            plan.required_phases,
            ["validation", "readback", "selection"]
        );
        assert_eq!(
            plan.required_phase_reasons["validation"],
            PlanPhaseReason::ValidatorChanged
        );
        assert_eq!(
            engine
                .run_until(&request, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let (revision, current) = readback(&engine, &request).await;
        assert_eq!(
            engine
                .inner
                .routine_checks
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(revision > old_revision);
        assert!(current.current());
        assert_eq!(current.release, old.release);
        assert_eq!(material(&engine, &request.dataset).await, before);
        assert_eq!(
            engine
                .inner
                .initial_build_queries
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            engine
                .run_until(&request, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        assert_eq!(readback(&engine, &request).await.0, revision);
        assert_eq!(
            engine
                .inner
                .routine_checks
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let selected = engine.run(&request).await.unwrap();
        assert_eq!(selected, old.release);
        exact_values(&engine, &selected).await;
        // Strict inventory accepts the private stamp and all native integer IDs.
        engine
            .inspect_retention(&selected.dataset.groups["source"].objects)
            .await
            .unwrap();
        let (legacy_revision, _) = prior_readback(&engine, &request, version).await;
        assert_eq!(engine.run(&request).await.unwrap(), selected);
        assert!(engine.plan(&request).await.unwrap().completed);
        assert_eq!(readback(&engine, &request).await.0, legacy_revision);
        assert_eq!(material(&engine, &request.dataset).await, before);
    }
}

#[tokio::test]
async fn prior_readback_cannot_hide_missing_required_input_from_the_current_checker() {
    let (_root, engine, request) = fixture().await;
    assert_eq!(
        engine
            .run_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (revision, old) = prior_readback(&engine, &request, None).await;
    let before = material(&engine, &request.dataset).await;
    let input = old.release.dataset.inputs.values().next().unwrap();
    std::fs::remove_file(&input.path).unwrap();
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "unavailable_phase"
    );
    assert_eq!(readback(&engine, &request).await.0, revision);
    assert_eq!(material(&engine, &request.dataset).await, before);
    assert_eq!(
        engine.latest(&request.dataset).await.unwrap_err().code(),
        "no_release"
    );
}

#[tokio::test]
async fn related_readback_requalifies_current_check_without_rewriting_values_or_contributors() {
    let (_root, engine, request) = fixture().await;
    let input = engine.run(&request).await.unwrap();
    let refresh = RefreshSpec {
        dataset: id("summary"),
        operation: id("summary"),
        base: None,
        definition: "v1".into(),
        mode: RefreshMode::Full,
        policy: RefreshPolicy::Captured,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: vec![],
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
    assert_eq!(
        engine
            .refresh_until(&refresh, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let view = engine.inner.lifecycle.open(&refresh.dataset).await.unwrap();
    let key = "readback/summary";
    let old: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, key)
        .await
        .unwrap()
        .unwrap();
    let protections = engine.retained_protections(&view, key).await.unwrap();
    let records =
        completion_records(&refresh.dataset, &view, key, &old, protections.as_ref()).unwrap();
    engine.inner.lifecycle.commit(&view, records).await.unwrap();
    let before = material(&engine, &refresh.dataset).await;
    let evidence = engine
        .inspect(&refresh.dataset)
        .await
        .unwrap()
        .into_iter()
        .filter(|(key, _)| key.starts_with("phase/") && key.ends_with("/evidence"))
        .collect::<BTreeMap<_, _>>();
    let checks = engine
        .inner
        .routine_checks
        .load(std::sync::atomic::Ordering::SeqCst);
    let selected = engine.refresh(&refresh).await.unwrap();
    assert_eq!(selected, old);
    assert_eq!(
        engine
            .inner
            .routine_checks
            .load(std::sync::atomic::Ordering::SeqCst),
        checks + 1
    );
    assert_eq!(material(&engine, &refresh.dataset).await, before);
    let after = engine.inspect(&refresh.dataset).await.unwrap();
    for (key, value) in evidence {
        assert_eq!(after[&key], value);
    }
    let mut stream = engine.read_group(&selected, "source").await.unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("rows")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert!(stream.try_next().await.unwrap().is_none());
    assert_eq!(engine.refresh(&refresh).await.unwrap(), selected);
    assert_eq!(
        engine
            .inner
            .routine_checks
            .load(std::sync::atomic::Ordering::SeqCst),
        checks + 1
    );
}

#[tokio::test]
async fn inherited_files_skip_inspection_only_with_the_exact_current_base_acknowledgment() {
    let (_root, engine, request) = fixture().await;
    let release = engine.run(&request).await.unwrap();
    let budget = &engine.inner.lifecycle.budget;
    let source = &release.dataset.groups["source"];
    let mut current = verification::Checks::routine(engine.inner.objects.clone());
    engine
        .trust_current_readback(&release, &mut current)
        .await
        .unwrap();
    current.table(source, budget).await.unwrap();
    assert_eq!(current.report.data_files_checked, Some(0));
    drop(current);
    prior_readback(&engine, &request, None).await;
    let mut changed = verification::Checks::routine(engine.inner.objects.clone());
    engine
        .trust_current_readback(&release, &mut changed)
        .await
        .unwrap();
    changed.table(source, budget).await.unwrap();
    assert_eq!(changed.report.data_files_checked, Some(1));
}

#[tokio::test]
async fn current_readback_survives_upstream_retirement_without_revalidation_or_revival() {
    let (root, engine, request) = fixture().await;
    assert_eq!(
        engine
            .run_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (revision, current) = readback(&engine, &request).await;
    assert!(current.current());
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let phases = rows
        .prefix("phase/")
        .map(|(key, revision, json)| (key.to_owned(), (revision, json.to_owned())))
        .collect::<BTreeMap<_, _>>();
    assert!(!phases.is_empty());
    let keys = phases.keys().cloned().collect::<Vec<_>>();
    drop(rows);
    drop(view);
    let retirement = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("upstream"),
            &keys,
            "current readback retains the complete checked result",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    std::fs::remove_file(&request.input).unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine.plan(&request).await.unwrap().required_phases,
        ["selection"]
    );
    assert_eq!(
        engine
            .run_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(readback(&engine, &request).await.0, revision);
    assert_eq!(engine.run(&request).await.unwrap(), current.release);
    assert_eq!(readback(&engine, &request).await.0, revision);
    assert_eq!(
        engine
            .inner
            .routine_checks
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        engine
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let retained = rows
        .prefix("phase/")
        .map(|(key, revision, json)| (key.to_owned(), (revision, json.to_owned())))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(retained, phases);
}
