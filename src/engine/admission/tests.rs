//! Native request acknowledgment follows durable protection of its exact dependencies.
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

async fn fixture() -> (tempfile::TempDir, Engine) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    (root, engine)
}

fn request(root: &Path, dataset: &str) -> RunSpec {
    let path = root.join(format!("{dataset}.parquet"));
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
        dataset: id(dataset),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
    }
}

fn payload(release: &ReleasePin) -> ObjectRef {
    release.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone()
}

fn consumer(source: &ObjectRef, dataset: &str) -> RunSpec {
    RunSpec {
        dataset: id(dataset),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: source.path.clone(),
        expected_digest: source.digest.clone(),
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
    }
}

async fn capture(
    engine: &Engine,
    dataset: &Id,
    operation: &Id,
) -> (i64, AdmissionDependencies, Vec<Value>) {
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let keys = [
        format!("operation/{operation}"),
        format!("request/{operation}"),
        dependencies_key(operation),
    ];
    let (revision, _, dependencies) = rows
        .get_record::<AdmissionDependencies>(&keys[2])
        .unwrap()
        .unwrap();
    let mut protections = None;
    for key in &keys {
        assert_eq!(
            rows.revision(key),
            Some(revision),
            "request and dependencies must become durable together"
        );
        let (_, binding_revision, json) = rows
            .prefix("reference-binding/")
            .find(|(_, _, json)| {
                serde_json::from_str::<Value>(json).unwrap()["key"].as_str() == Some(key.as_str())
            })
            .unwrap();
        let binding: Value = serde_json::from_str(json).unwrap();
        assert_eq!(binding_revision, revision);
        assert_eq!(binding["revision"], revision);
        assert_eq!(binding["consumer"], dataset.as_str());
        let references = binding["protections"].as_array().unwrap().clone();
        if let Some(previous) = &protections {
            assert_eq!(previous, &references);
        } else {
            protections = Some(references);
        }
        engine.check_reference_binding(&view, key).await.unwrap();
    }
    (revision, dependencies, protections.unwrap())
}

#[tokio::test]
async fn managed_input_admission_reuses_its_recorded_owner_without_copying_or_hashing() {
    let (root, engine) = fixture().await;
    let producer_request = request(root.path(), "producer");
    let producer = engine.run(&producer_request).await.unwrap();
    let source = payload(&producer);
    std::fs::remove_file(&producer_request.input).unwrap();
    let mut request = consumer(&source, "consumer");
    let path = Path::new(&source.path);
    request.input = path
        .parent()
        .unwrap()
        .join(".")
        .join(path.file_name().unwrap())
        .to_string_lossy()
        .into_owned();
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&source.owner)
        .await
        .unwrap()
        .revision;
    // Fault only this private fixture's bytes while keeping size and identity
    // metadata intact. Admission must trust qualified producer creation, not hash again.
    let original = std::fs::read(&source.path).unwrap();
    let mut damaged = original.clone();
    damaged[0] ^= 1;
    std::fs::write(&source.path, damaged).unwrap();
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    std::fs::write(&source.path, original).unwrap();
    let first = capture(&engine, &request.dataset, &request.operation).await;
    assert_eq!(
        first.1.inputs,
        BTreeMap::from([("input".into(), source.clone())])
    );
    assert!(first.1.base.is_none());
    assert!(first.1.coverage.is_none());
    assert_eq!(first.2.len(), 1);
    assert_eq!(first.2[0]["owner"], source.owner.as_str());
    let records = engine.inspect(&request.dataset).await.unwrap();
    let admission: PhaseResult = records
        .iter()
        .find(|(key, _)| key.starts_with("phase/") && key.ends_with("/admission"))
        .map(|(_, value)| serde_json::from_value(value.clone()).unwrap())
        .unwrap();
    assert_eq!(admission.outputs, vec![source.clone()]);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision + 1
    );
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        capture(&engine, &request.dataset, &request.operation).await,
        first
    );
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        capture(&engine, &request.dataset, &request.operation).await,
        first
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision + 1
    );
    let release = engine.run(&request).await.unwrap();
    assert_eq!(release.dataset.inputs["input"], source);
    assert_eq!(
        release.dataset.groups["source"].rows,
        producer.dataset.groups["source"].rows
    );
}

#[tokio::test]
async fn provisional_managed_file_refuses_before_request_acknowledgment() {
    let (root, engine) = fixture().await;
    let producer = engine.run(&request(root.path(), "producer")).await.unwrap();
    let source = payload(&producer);
    let path = Path::new(&source.path)
        .parent()
        .unwrap()
        .join("provisional.parquet");
    std::fs::copy(&source.path, &path).unwrap();
    let provisional = ObjectRef {
        path: path.to_string_lossy().into_owned(),
        ..source.clone()
    };
    let request = consumer(&provisional, "consumer");
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&source.owner)
        .await
        .unwrap()
        .revision;
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "unqualified_owner_reference"
    );
    let records = engine.inspect(&request.dataset).await.unwrap();
    assert!(!records.contains_key("operation/initial"));
    assert!(!records.contains_key("request/initial"));
    assert!(!records.contains_key("request-dependencies/initial"));
    assert!(
        !records
            .keys()
            .any(|key| key.starts_with("reference-binding/"))
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
}

#[tokio::test]
async fn refresh_request_captures_and_protects_unused_members_of_its_producer_seal() {
    let (root, engine) = fixture().await;
    let family = id("family");
    let generation = id("generation");
    let mut releases = BTreeMap::new();
    for dataset in ["comments", "dockets", "unused-sibling"] {
        let mut request = request(root.path(), dataset);
        request.relationship = Some(SourceRelationship::Coupled {
            family: family.clone(),
            generation: generation.clone(),
        });
        let release = engine.run(&request).await.unwrap();
        releases.insert(request.dataset, release);
    }
    let seal = GenerationSeal {
        family: family.clone(),
        generation: generation.clone(),
        members: releases
            .iter()
            .map(|(dataset, release)| (dataset.clone(), release.version_ref()))
            .collect(),
    };
    engine.seal_generation(&seal).await.unwrap();
    let request = RefreshSpec {
        dataset: id("joined"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: releases[&id("comments")].version_ref(),
            right: releases[&id("dockets")].version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
                fields: vec![JoinField {
                    side: JoinSide::Left,
                    field: "id".into(),
                    name: "id".into(),
                }],
            },
        }),
    };
    assert_eq!(
        engine
            .refresh_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (_, dependencies, references) =
        capture(&engine, &request.dataset, &request.operation).await;
    let coverage = dependencies.coverage.unwrap();
    assert_eq!(
        coverage.inputs.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([id("comments"), id("dockets")])
    );
    assert_eq!(coverage.generations[&family], seal);
    assert_eq!(
        coverage.families[&family],
        seal.members.keys().cloned().collect::<BTreeSet<_>>()
    );
    assert!(dependencies.inputs.is_empty());
    assert_eq!(
        references
            .iter()
            .map(|reference| reference["owner"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "comments".to_owned(),
            "dockets".to_owned(),
            "unused-sibling".to_owned()
        ])
    );
    for reference in references {
        let owner = id(reference["owner"].as_str().unwrap());
        let view = engine.inner.lifecycle.open(&owner).await.unwrap();
        let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
        let (revision, _, grant) = rows
            .get_record::<Value>(reference["token"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(reference["revision"], revision);
        assert_eq!(grant["consumer"], request.dataset.as_str());
        assert!(
            grant["objects"]
                .as_array()
                .unwrap()
                .contains(&serde_json::to_value(&releases[&owner].manifest).unwrap())
        );
    }
}

#[tokio::test]
async fn missing_external_locator_reuses_material_work_and_captures_selected_history() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            memory_bytes: 256 << 20,
            ..limits()
        },
    )
    .await
    .unwrap();
    let mut request = request(root.path(), "comments");
    let original = engine.run(&request).await.unwrap();
    std::fs::remove_file(&request.input).unwrap();
    request.operation = id("reused");
    request.base = Some(original.id.clone());
    let release = engine.run(&request).await.unwrap();
    assert_eq!(
        release.dataset.groups["source"],
        original.dataset.groups["source"]
    );
    assert_eq!(release.dataset.inputs, original.dataset.inputs);
    assert_eq!(
        release.dataset.parent_manifest,
        Some(original.manifest.clone())
    );
    let (_, dependencies, references) =
        capture(&engine, &request.dataset, &request.operation).await;
    assert_eq!(dependencies.base, Some(original.version_ref()));
    assert!(dependencies.inputs.is_empty());
    assert!(
        references.is_empty(),
        "own history needs no foreign owner grant"
    );
}

#[tokio::test]
async fn lost_initial_request_reply_retains_exact_dependencies_without_reacquiring_owners() {
    let (root, engine) = fixture().await;
    let producer = engine.run(&request(root.path(), "producer")).await.unwrap();
    let source = payload(&producer);
    let request = consumer(&source, "consumer");
    drop(
        engine
            .protect_objects(&request.dataset, std::slice::from_ref(&source))
            .await
            .unwrap(),
    );
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&source.owner)
        .await
        .unwrap()
        .revision;
    let before = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "lost_commit_response"
    );
    assert!(
        !engine
            .inner
            .lifecycle
            .lose_commit_response
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    let retained = capture(&engine, &request.dataset, &request.operation).await;
    assert_eq!(retained.0, before.revision + 1);
    assert_eq!(retained.1.inputs["input"], source);
    let records = engine.inspect(&request.dataset).await.unwrap();
    assert!(!records.keys().any(|key| key.starts_with("phase/")));
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&request, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        capture(&engine, &request.dataset, &request.operation).await,
        retained
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
}

#[tokio::test]
async fn backfill_admission_binds_its_capture_and_foreign_base_in_one_native_commit() {
    let (root, engine) = fixture().await;
    let producer = engine.run(&request(root.path(), "producer")).await.unwrap();
    let source = payload(&producer);
    let base = engine.run(&consumer(&source, "consumer")).await.unwrap();
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&source.owner)
        .await
        .unwrap()
        .revision;
    let backfill = BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("derived"),
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
    assert_eq!(
        engine
            .backfill_until(&backfill, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained = capture(&engine, &backfill.dataset, &backfill.operation).await;
    assert_eq!(retained.1.base, Some(base.version_ref()));
    assert_eq!(retained.2.len(), 1);
    assert_eq!(retained.2[0]["owner"], source.owner.as_str());
    let view = engine
        .inner
        .lifecycle
        .open(&backfill.dataset)
        .await
        .unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let key = format!("backfill/{}", backfill.operation);
    let (revision, _, captured) = rows.get_record::<Value>(&key).unwrap().unwrap();
    assert_eq!(revision, retained.0);
    assert_eq!(
        captured["base_manifest"],
        serde_json::to_value(&base.manifest).unwrap()
    );
    let (_, binding_revision, json) = rows
        .prefix("reference-binding/")
        .find(|(_, _, json)| serde_json::from_str::<Value>(json).unwrap()["key"] == key)
        .unwrap();
    let binding: Value = serde_json::from_str(json).unwrap();
    assert_eq!(binding_revision, retained.0);
    assert_eq!(binding["revision"], retained.0);
    assert_eq!(
        binding["protections"],
        serde_json::to_value(&retained.2).unwrap()
    );
    engine.check_reference_binding(&view, &key).await.unwrap();
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
    drop(rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .backfill_until(&backfill, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        capture(&engine, &backfill.dataset, &backfill.operation).await,
        retained
    );
    let view = engine
        .inner
        .lifecycle
        .open(&backfill.dataset)
        .await
        .unwrap();
    engine.check_reference_binding(&view, &key).await.unwrap();
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
    let release = engine.backfill(&backfill).await.unwrap();
    assert_eq!(release.dataset.inputs["input"], source);
    assert_eq!(
        release.dataset.groups["next"].rows,
        base.dataset.groups["source"].rows
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&source.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
}

#[tokio::test]
async fn based_managed_rebuild_keeps_distinct_scopes_from_the_same_owner() {
    let (root, engine) = fixture().await;
    let mut producer_request = request(root.path(), "producer");
    let producer = engine.run(&producer_request).await.unwrap();
    let first_input = payload(&producer);
    let base = engine
        .run(&consumer(&first_input, "consumer"))
        .await
        .unwrap();
    producer_request.operation = id("changed");
    producer_request.base = Some(producer.id.clone());
    producer_request.sql = "SELECT id + 100 AS id FROM input".into();
    let changed = engine.run(&producer_request).await.unwrap();
    let second_input = payload(&changed);
    assert_ne!(first_input, second_input);
    assert_eq!(first_input.owner, second_input.owner);
    let mut rebuild = consumer(&second_input, "consumer");
    rebuild.operation = id("rebuild");
    rebuild.base = Some(base.id.clone());
    assert_eq!(
        engine
            .run_until(&rebuild, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained = capture(&engine, &rebuild.dataset, &rebuild.operation).await;
    assert_eq!(retained.1.base, Some(base.version_ref()));
    assert_eq!(retained.1.inputs["input"], second_input);
    assert_eq!(retained.2.len(), 2);
    assert!(
        retained
            .2
            .iter()
            .all(|reference| reference["owner"] == first_input.owner.as_str())
    );
    assert_ne!(retained.2[0]["token"], retained.2[1]["token"]);
    let owner_view = engine
        .inner
        .lifecycle
        .open(&first_input.owner)
        .await
        .unwrap();
    let owner_revision = owner_view.revision;
    let owner_rows = engine.inner.lifecycle.rows(&owner_view).await.unwrap();
    let mut tokens = BTreeMap::new();
    for reference in &retained.2 {
        let token = reference["token"].as_str().unwrap();
        let (_, _, grant) = owner_rows.get_record::<Value>(token).unwrap().unwrap();
        for object in [&first_input, &second_input] {
            if grant["objects"]
                .as_array()
                .unwrap()
                .contains(&serde_json::to_value(object).unwrap())
            {
                tokens.insert(object.path.clone(), token.to_owned());
            }
        }
    }
    assert_ne!(tokens[&first_input.path], tokens[&second_input.path]);
    drop(owner_rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .run_until(&rebuild, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        capture(&engine, &rebuild.dataset, &rebuild.operation).await,
        retained
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&first_input.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
    let inspected = engine
        .inspect_retention(&[first_input.clone(), second_input.clone()])
        .await
        .unwrap();
    assert_eq!(inspected.unmatched_bytes, 0);
    assert!(
        inspected
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    // Each corrupted claim still retains another valid token from the same
    // owner. Owner presence alone cannot substitute for the missing exact scope.
    for (claim, missing) in [
        (format!("operation/{}", rebuild.operation), &first_input),
        (dependencies_key(&rebuild.operation), &second_input),
    ] {
        let view = engine.inner.lifecycle.open(&rebuild.dataset).await.unwrap();
        let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
        let (_, _, value) = rows.get_record::<Value>(&claim).unwrap().unwrap();
        let (binding_key, _, json) = rows
            .prefix("reference-binding/")
            .find(|(_, _, json)| serde_json::from_str::<Value>(json).unwrap()["key"] == claim)
            .unwrap();
        let binding_key = binding_key.to_owned();
        let mut binding: Value = serde_json::from_str(json).unwrap();
        binding["revision"] = serde_json::json!(view.revision + 1);
        binding["protections"]
            .as_array_mut()
            .unwrap()
            .retain(|reference| {
                reference["token"].as_str() != Some(tokens[&missing.path].as_str())
            });
        assert_eq!(binding["protections"].as_array().unwrap().len(), 1);
        let corrupted = engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(&claim, value).unwrap(),
                    record(binding_key, binding).unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            engine
                .check_reference_binding(&corrupted, &claim)
                .await
                .unwrap_err()
                .code(),
            "invalid_owner_protection"
        );
    }
}
