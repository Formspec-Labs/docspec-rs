//! Built-in validator changes qualify retained scalar work without recomputation.
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
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}
async fn fixture() -> (tempfile::TempDir, Engine, ReleasePin, BackfillSpec) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: id("data"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&path).unwrap().0,
            sql: "SELECT id FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let spec = BackfillSpec {
        dataset: id("data"),
        operation: id("scalar"),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id("next"),
        group_definition: "next-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "next_id".into(),
            expression: "id + 1".into(),
        }],
    };
    (root, engine, base, spec)
}

async fn material(engine: &Engine, dataset: &Id) -> BTreeMap<String, (i64, Value)> {
    let view = engine.inner.lifecycle.open(dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let mut found = BTreeMap::new();
    for (key, revision, json) in rows.prefix("") {
        if key.starts_with("phase/backfill/") || key.starts_with("backfill-prepared/") {
            found.insert(key.into(), (revision, serde_json::from_str(json).unwrap()));
        }
    }
    let keys = found.keys().cloned().collect::<BTreeSet<_>>();
    for (key, revision, json) in rows.prefix("reference-binding/") {
        let value: Value = serde_json::from_str(json).unwrap();
        if value["key"].as_str().is_some_and(|key| keys.contains(key)) {
            found.insert(key.into(), (revision, value));
        }
    }
    found
}

async fn legacy_ack(
    engine: &Engine,
    base: &ReleasePin,
    spec: &BackfillSpec,
    prepared: &PreparedCompletion,
    version: Option<&str>,
) -> (String, i64, Value) {
    let source = &base.dataset.groups["source"];
    let native = storage::open_pin(source, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    let definition = spec.group_definition();
    let expressions = Expressions::compile(
        &engine.context(),
        native.metadata().current_schema(),
        &definition,
        None,
    )
    .unwrap();
    // Retain actual native target/interpretation with the historical validation
    // identity. This compatibility fixture does not fake computed payloads.
    let digest = storage::json_digest(&(
        PROCESSOR,
        source.logical_metadata(),
        &definition,
        &expressions.input_fields,
    ))
    .unwrap();
    let digest = if let Some(version) = version {
        storage::json_digest(&(
            version,
            PROCESSOR,
            source.logical_metadata(),
            &definition,
            &expressions.input_fields,
        ))
        .unwrap()
    } else {
        digest
    };
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, &format!("operation/{}", spec.operation))
        .await
        .unwrap()
        .unwrap();
    let key = format!(
        "backfill-validation/{}/{}",
        operation.eligibility,
        digest.to_hex()
    );
    let completion = ValidationCompletion {
        validator: version.map(str::to_owned),
        origins: prepared.origins.clone(),
        output: prepared.table.metadata.clone(),
        dependencies: prepared.dependencies.clone(),
    };
    let request = OperationRequest::Backfill(spec.clone());
    let protections = engine
        .request_protections(&view, &spec.operation)
        .await
        .unwrap();
    let records = completion_records(
        request.dataset(),
        &view,
        &key,
        &completion,
        protections.as_ref(),
    )
    .unwrap();
    let view = engine.inner.lifecycle.commit(&view, records).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (revision, _, value) = rows.get_record::<Value>(&key).unwrap().unwrap();
    (key, revision, value)
}
// Encode a real native target with the exact missing-version shape produced by
// older engines. Only retained proof metadata changes in this compatibility
// fixture; source, target, manifest, key and ownership bindings stay exact.
async fn legacy_readback(engine: &Engine, spec: &BackfillSpec) {
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let key = rows
        .prefix(&format!("backfill-readback/{}/", spec.operation))
        .next()
        .unwrap()
        .0
        .to_owned();
    let release: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, &key)
        .await
        .unwrap()
        .unwrap();
    let protections = engine.retained_protections(&view, &key).await.unwrap();
    let records =
        completion_records(&spec.dataset, &view, &key, &release, protections.as_ref()).unwrap();
    engine.inner.lifecycle.commit(&view, records).await.unwrap();
    let records = engine.inspect(&spec.dataset).await.unwrap();
    assert!(records[&key].get("scalar_validator").is_none());
}

async fn prepared(engine: &Engine, spec: &BackfillSpec) -> PreparedCompletion {
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    serde_json::from_str(rows.prefix("backfill-prepared/").next().unwrap().2).unwrap()
}
async fn retire(engine: &Engine, spec: &BackfillSpec, keys: &[String]) {
    let plan = engine
        .plan_phase_retirement(
            &spec.dataset,
            &id("prior-validator"),
            keys,
            "end prior validation phase promise",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
}
async fn check_values(engine: &Engine, release: &ReleasePin) {
    let mut stream = engine.read_group(release, "next").await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let index = batch.schema().index_of("next_id").unwrap();
        values.extend(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    values.sort();
    assert_eq!(values, [Some(2), Some(2), Some(3)]);
}

#[tokio::test]
async fn current_scalar_validator_rechecks_legacy_and_new_operation_prepared_reuse() {
    for (version, new_operation) in [
        (None, false),
        (Some("docspec.scalar-validation.previous"), true),
    ] {
        let (root, engine, base, mut spec) = fixture().await;
        assert_eq!(
            engine
                .backfill_until(&spec, "processing")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let prepared = prepared(&engine, &spec).await;
        let original = material(&engine, &spec.dataset).await;
        let legacy = legacy_ack(&engine, &base, &spec, &prepared, version).await;
        if new_operation {
            spec.operation = id("new-validator-operation");
        }
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        assert_eq!(
            engine
                .backfill_until(&spec, "validation")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        assert_eq!(material(&engine, &spec.dataset).await, original);
        let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
        let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
        assert_eq!(
            rows.get_record::<Value>(&legacy.0).unwrap().unwrap().0,
            legacy.1
        );
        assert_eq!(
            rows.get_record::<Value>(&legacy.0).unwrap().unwrap().2,
            legacy.2
        );
        let (key, revision, json) = rows
            .prefix("backfill-validation/")
            .find(|(key, _, _)| *key != legacy.0)
            .unwrap();
        let current: ValidationCompletion = serde_json::from_str(json).unwrap();
        assert_eq!(current.validator.as_deref(), Some(VALIDATOR));
        assert_eq!(current.output, prepared.table.metadata);
        let current_generation = (key.to_owned(), revision);
        drop(rows);
        let result = engine.backfill(&spec).await.unwrap();
        assert_eq!(result.dataset.groups[spec.group.as_str()], prepared.table);
        check_values(&engine, &result).await;
        let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
        assert_eq!(
            engine
                .inner
                .lifecycle
                .rows(&view)
                .await
                .unwrap()
                .revision(&current_generation.0),
            Some(current_generation.1)
        );
    }
}

#[tokio::test]
async fn unselected_scalar_readback_requires_current_ack_but_completed_result_stays_exact() {
    let (root, engine, base, spec) = fixture().await;
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    legacy_readback(&engine, &spec).await;
    let prepared = prepared(&engine, &spec).await;
    let before = engine.inspect(&spec.dataset).await.unwrap();
    let readback_key = before
        .keys()
        .find(|key| key.starts_with("backfill-readback/"))
        .unwrap()
        .clone();
    let retained: ReleasePin = serde_json::from_value(before[&readback_key].clone()).unwrap();
    let keys = before
        .keys()
        .filter(|key| {
            key.starts_with("phase/backfill/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
        })
        .cloned()
        .collect::<Vec<_>>();
    let original = material(&engine, &spec.dataset).await;
    retire(&engine, &spec, &keys).await;
    let legacy = legacy_ack(&engine, &base, &spec, &prepared, None).await;
    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    assert_eq!(
        engine
            .backfill_until(&spec, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        material(&engine, &spec.dataset).await,
        original,
        "retired material phases must not revive"
    );
    let after = engine.inspect(&spec.dataset).await.unwrap();
    assert_eq!(after[&readback_key], before[&readback_key]);
    assert_eq!(after[&legacy.0], legacy.2);
    let completed = engine.backfill(&spec).await.unwrap();
    assert_eq!(
        completed.dataset.groups[spec.group.as_str()],
        retained.dataset.groups[spec.group.as_str()]
    );
    check_values(&engine, &completed).await;
    // Original completed acknowledgment remains available even if a later
    // validator cannot reopen historical check inputs; this is not a read lease.
    let source_metadata = &base.dataset.groups["source"].metadata.path;
    let bytes = std::fs::read(source_metadata).unwrap();
    std::fs::remove_file(source_metadata).unwrap();
    assert_eq!(engine.backfill(&spec).await.unwrap(), completed);
    assert!(engine.plan_backfill(&spec).await.unwrap().completed);
    std::fs::write(source_metadata, bytes).unwrap();
}

#[tokio::test]
async fn prior_scalar_ack_cannot_hide_missing_current_check_inputs() {
    let (_root, engine, base, spec) = fixture().await;
    assert_eq!(
        engine
            .backfill_until(&spec, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let prepared = prepared(&engine, &spec).await;
    let legacy = legacy_ack(&engine, &base, &spec, &prepared, None).await;
    let original = material(&engine, &spec.dataset).await;
    let path = &base.dataset.groups["source"].metadata.path;
    let bytes = std::fs::read(path).unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(engine.backfill(&spec).await.is_err());
    let after = engine.inspect(&spec.dataset).await.unwrap();
    assert_eq!(after[&legacy.0], legacy.2);
    assert!(!after.contains_key(&format!("result/{}", spec.operation)));
    assert_eq!(
        after
            .keys()
            .filter(|key| key.starts_with("backfill-validation/"))
            .count(),
        1
    );
    assert_eq!(material(&engine, &spec.dataset).await, original);
    std::fs::write(path, bytes).unwrap();
}

#[tokio::test]
async fn scalar_readback_validator_inspects_once_before_current_ack_reuse() {
    let (_root, engine, base, spec) = fixture().await;
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&spec.dataset).await.unwrap();
    let key = before
        .keys()
        .find(|key| key.starts_with("backfill-validation/"))
        .unwrap()
        .clone();
    retire(&engine, &spec, &[key]).await;
    let readback_key = before
        .keys()
        .find(|key| key.starts_with("backfill-readback/"))
        .unwrap();
    let readback: ReleasePin = serde_json::from_value(before[readback_key].clone()).unwrap();
    let source = &base.dataset.groups["source"];
    let output = &readback.dataset.groups[spec.group.as_str()];
    let binding = &readback.dataset.computed[spec.group.as_str()];
    let request = OperationRequest::Backfill(spec.clone());
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, &format!("operation/{}", spec.operation))
        .await
        .unwrap()
        .unwrap();
    let protections = engine
        .request_protections(&view, &spec.operation)
        .await
        .unwrap();
    let readback_protections = engine
        .retained_protections(&view, readback_key)
        .await
        .unwrap();
    let mut checks = super::super::verification::Checks::composition();
    for expected in [1, 1] {
        engine
            .validate_scalar_group(
                &request,
                &operation,
                source,
                protections.as_ref(),
                &binding.definition,
                &binding.input_fields,
                &binding.native_field_ids,
                output,
                binding.origins.as_ref(),
                readback_protections.as_ref(),
                &mut checks,
            )
            .await
            .unwrap();
        assert_eq!(checks.report.tables_checked, expected);
        assert_eq!(checks.report.data_files_checked, Some(expected));
        assert_eq!(checks.report.sampled_files, expected);
        checks.compose();
    }
}

#[tokio::test]
async fn current_scalar_readback_retains_validation_after_phase_retirement() {
    let (root, engine, base, spec) = fixture().await;
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&spec.dataset).await.unwrap();
    let readback_key = before
        .keys()
        .find(|key| key.starts_with("backfill-readback/"))
        .unwrap()
        .clone();
    assert_eq!(before[&readback_key]["scalar_validator"], VALIDATOR);
    let retained: ReleasePin = serde_json::from_value(before[&readback_key].clone()).unwrap();
    let keys = before
        .keys()
        .filter(|key| {
            key.starts_with("phase/backfill/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
        })
        .cloned()
        .collect::<Vec<_>>();
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let generations = keys
        .iter()
        .map(|key| (key.clone(), rows.revision(key).unwrap()))
        .collect::<BTreeMap<_, _>>();
    drop(rows);
    retire(&engine, &spec, &keys).await;
    let mut sibling = spec.clone();
    sibling.operation = id("sibling");
    sibling.group = id("double");
    sibling.group_definition = "double-v1".into();
    sibling.fields = vec![ComputedField {
        id: 11,
        name: "double_id".into(),
        expression: "id * 2".into(),
    }];
    let sibling_release = engine.backfill(&sibling).await.unwrap();
    std::fs::remove_file(root.path().join("input.parquet")).unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let completed = engine.backfill(&spec).await.unwrap();
    assert_eq!(
        completed.dataset.groups[spec.group.as_str()],
        retained.dataset.groups[spec.group.as_str()]
    );
    assert_eq!(
        completed.dataset.groups[sibling.group.as_str()],
        sibling_release.dataset.groups[sibling.group.as_str()]
    );
    check_values(&engine, &completed).await;
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    for (key, revision) in generations {
        assert_eq!(
            rows.revision(&key),
            Some(revision),
            "retired phase generation must not resurrect: {key}"
        );
    }
    let after = engine.inspect(&spec.dataset).await.unwrap();
    assert_eq!(after[&readback_key], before[&readback_key]);
    assert!(
        after
            .iter()
            .filter(|(key, _)| key.starts_with(&format!("backfill-readback/{}/", spec.operation)))
            .all(|(_, value)| value["scalar_validator"] == VALIDATOR)
    );
    // A reassembled readback still carries the qualification independently.
    assert_eq!(
        completed.dataset.parent_manifest,
        Some(sibling_release.manifest)
    );
    assert_eq!(
        base.dataset.groups["source"],
        completed.dataset.groups["source"]
    );
}
