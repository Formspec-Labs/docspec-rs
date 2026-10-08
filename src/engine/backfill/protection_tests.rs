//! Scalar recovery preserves the owner protections of the work it actually reused.
#![cfg(feature = "test-util")]

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

fn input(root: &Path) -> RunSpec {
    let path = root.join("input.parquet");
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
    RunSpec {
        dataset: id("producer"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
    }
}

fn binding(rows: &crate::catalog::SnapshotRows, claim: &str) -> Option<(String, i64, Value)> {
    rows.prefix("reference-binding/")
        .find_map(|(key, revision, json)| {
            let value: Value = serde_json::from_str(json).unwrap();
            (value["key"].as_str() == Some(claim)).then(|| (key.to_owned(), revision, value))
        })
}

async fn scalar_records(
    engine: &Engine,
    dataset: &Id,
    original_tokens: &Value,
) -> BTreeMap<String, (i64, Value)> {
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let mut records = BTreeMap::new();
    for (key, revision, json) in rows.prefix("").filter(|(key, _, _)| {
        key.starts_with("phase/backfill/")
            || key.starts_with("backfill-prepared/")
            || key.starts_with("backfill-validation/")
    }) {
        let (binding_key, binding_revision, binding) = binding(&rows, key).unwrap();
        assert_eq!(
            binding_revision, revision,
            "completion and binding commit together"
        );
        assert_eq!(binding["revision"], revision);
        assert_eq!(binding["consumer"], dataset.as_str());
        assert_eq!(binding["protections"], *original_tokens);
        engine.check_reference_binding(&view, key).await.unwrap();
        records.insert(
            key.to_owned(),
            (revision, serde_json::from_str(json).unwrap()),
        );
        records.insert(binding_key, (binding_revision, binding));
    }
    records
}

// Only this tiny fixture's owned data directory is visited. Lifecycle commits
// may add metadata, but recovery must not add or rewrite scalar/source payloads.
fn payloads(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut pending = vec![root.to_owned()];
    let mut result = BTreeMap::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                result.insert(path, (metadata.len(), metadata.modified().unwrap()));
            }
        }
    }
    result
}

#[tokio::test]
async fn scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let producer_request = input(root.path());
    let producer = engine.run(&producer_request).await.unwrap();
    let raw_input = producer.dataset.inputs.values().next().unwrap().clone();
    assert_eq!(raw_input.owner, producer_request.dataset);
    std::fs::remove_file(&producer_request.input).unwrap();
    let consumer = RunSpec {
        dataset: id("consumer"),
        input: raw_input.path.clone(),
        expected_digest: raw_input.digest.clone(),
        ..producer_request
    };
    let base = engine.run(&consumer).await.unwrap();
    assert!(
        base.dataset
            .inputs
            .values()
            .any(|input| input == &raw_input)
    );
    let view = engine
        .inner
        .lifecycle
        .open(&consumer.dataset)
        .await
        .unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let original_tokens =
        binding(&rows, "request-dependencies/initial").unwrap().2["protections"].clone();
    assert_eq!(original_tokens.as_array().unwrap().len(), 1);
    assert_eq!(original_tokens[0]["owner"], raw_input.owner.as_str());
    // Selection binding belongs to the later integration. Admission alone must
    // not qualify a recomposed readback's independently retained current result.
    assert!(binding(&rows, "result/initial").is_none());
    drop(rows);
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&raw_input.owner)
        .await
        .unwrap()
        .revision;
    let spec = BackfillSpec {
        dataset: consumer.dataset.clone(),
        operation: id("derived"),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id("calculated"),
        group_definition: "scalar-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "next_id".into(),
            expression: "id + 1".into(),
        }],
    };
    let data_directory = workspace.join("objects").join(consumer.dataset.as_str());
    let mut previous = BTreeMap::new();
    let mut retained_payloads = None;
    for (stop, family) in [
        ("files", "phase/backfill/"),
        ("processing", "backfill-prepared/"),
        ("validation", "backfill-validation/"),
    ] {
        for attempt in 0..2 {
            assert_eq!(
                engine.backfill_until(&spec, stop).await.unwrap_err().code(),
                "injected_failure"
            );
            let records = scalar_records(&engine, &spec.dataset, &original_tokens).await;
            assert!(records.keys().any(|key| key.starts_with(family)));
            for (key, value) in &previous {
                assert_eq!(
                    records.get(key),
                    Some(value),
                    "first completed generation remains intact"
                );
            }
            if attempt == 1 {
                assert_eq!(
                    records, previous,
                    "retry must retain the exact claim and binding"
                );
            }
            previous = records;
            let current_payloads = payloads(&data_directory);
            assert!(!current_payloads.is_empty());
            if let Some(expected) = &retained_payloads {
                assert_eq!(
                    &current_payloads, expected,
                    "resume must not create or rewrite payloads"
                );
            } else {
                retained_payloads = Some(current_payloads);
            }
            assert_eq!(
                engine
                    .inner
                    .lifecycle
                    .open(&raw_input.owner)
                    .await
                    .unwrap()
                    .revision,
                owner_revision,
                "propagation and retry must not create owner grants",
            );
        }
    }
    drop(engine);

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(
        engine
            .backfill_until(&spec, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        scalar_records(&engine, &spec.dataset, &original_tokens).await,
        previous
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (key, _, _) = rows.prefix("backfill-readback/").next().unwrap();
    assert!(
        binding(&rows, key).is_none(),
        "an unbound selected result leaves readback provenance unknown"
    );
    assert!(
        engine
            .retained_protections(&view, key)
            .await
            .unwrap()
            .is_none()
    );
    drop(rows);
    let release = engine.backfill(&spec).await.unwrap();
    assert_eq!(engine.backfill(&spec).await.unwrap(), release);
    assert_eq!(
        scalar_records(&engine, &spec.dataset, &original_tokens).await,
        previous
    );
    assert_eq!(payloads(&data_directory), retained_payloads.unwrap());
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&raw_input.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
}
