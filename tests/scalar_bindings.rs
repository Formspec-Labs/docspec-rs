//! Reconstructed values and bound-input history checked against adversarial releases.

use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::verify_release,
    model::{BackfillSpec, ComputedField, Id, ObjectRef, ReleasePin, RunSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{path::Path, sync::Arc};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 2,
        scratch_bytes: 16 << 20,
        ..Limits::default()
    }
}
fn write(path: &Path, batch: &RecordBatch) {
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}
fn request(
    root: &Path,
    dataset: &str,
    operation: &str,
    base: Option<&ReleasePin>,
    batch: &RecordBatch,
) -> RunSpec {
    let path = root.join(format!("{operation}.parquet"));
    write(&path, batch);
    RunSpec {
        dataset: id(dataset),
        operation: id(operation),
        base: base.map(|base| base.id.clone()),
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    }
}
fn scalar_spec(base: &ReleasePin, fields: &[(&str, &str)]) -> BackfillSpec {
    BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("computed"),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id("derived"),
        group_definition: "scalar-v1".into(),
        fields: fields
            .iter()
            .enumerate()
            .map(|(i, (name, sql))| ComputedField {
                id: i as i32 + 1,
                name: (*name).into(),
                expression: (*sql).into(),
            })
            .collect(),
    }
}
fn rehash(root: &Path, name: &str, mut release: ReleasePin) -> ReleasePin {
    let path = root.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec(&release.dataset).unwrap()).unwrap();
    let (digest, bytes) = storage::digest(&path).unwrap();
    release.manifest = ObjectRef {
        owner: release.dataset.dataset.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    };
    release
}
async fn forge_values(
    engine: &Engine,
    root: &Path,
    release: &ReleasePin,
    values: Vec<ArrayRef>,
) -> ReleasePin {
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(&release.dataset.groups["derived"], &budget)
        .await
        .unwrap();
    let batches = storage::read_batches(&native, &budget)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let schema = batches[0].schema();
    let batch = arrow::compute::concat_batches(&schema, &batches).unwrap();
    let mut columns = batch.columns()[..2].to_vec();
    columns.extend(values);
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let alternate = engine
        .run(&request(root, "forged", "wrong_values", None, &batch))
        .await
        .unwrap();
    let mut forged = release.clone();
    forged
        .dataset
        .groups
        .insert("derived".into(), alternate.dataset.groups["source"].clone());
    // Exercise deep value comparison for a legacy binding. Modern origins
    // reject this substituted output earlier, through its metadata association.
    forged.dataset.computed.get_mut("derived").unwrap().origins = None;
    rehash(root, "wrong_values", forged)
}

#[tokio::test]
async fn opposite_boolean_outputs_cannot_hide_each_other_in_value_comparison() {
    let root = tempfile::TempDir::new().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
        ],
    )
    .unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let source = engine
        .run(&request(root.path(), "flags", "initial", None, &batch))
        .await
        .unwrap();
    let good = engine
        .backfill(&scalar_spec(&source, &[("first", "a"), ("second", "b")]))
        .await
        .unwrap();
    let bad = forge_values(
        &engine,
        root.path(),
        &good,
        vec![
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
        ],
    )
    .await;
    assert_eq!(
        verify_release(&bad, limits()).await.unwrap_err().code(),
        "invalid_group_values"
    );
    verify_release(&good, limits()).await.unwrap();
}

fn numbers(a: i64, b: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![a])),
            Arc::new(Int64Array::from(vec![b])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn redirected_rehashed_ancestor_does_not_change_an_authored_expression() {
    for change_request_kind in [false, true] {
        let root = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let source = engine
            .run(&request(
                root.path(),
                "numbers",
                "initial",
                None,
                &numbers(1, 100),
            ))
            .await
            .unwrap();
        let authored = engine
            .backfill(&scalar_spec(&source, &[("answer", "a + 1")]))
            .await
            .unwrap();
        let mut first_request = request(
            root.path(),
            "numbers",
            "a",
            Some(&authored),
            &numbers(2, 101),
        );
        if change_request_kind {
            first_request.sql = "SELECT b, a FROM input".into();
        }
        let a = engine.run(&first_request).await.unwrap();
        let b = engine
            .run(&request(
                root.path(),
                "numbers",
                "b",
                Some(&a),
                &numbers(3, 102),
            ))
            .await
            .unwrap();
        let mut wrong_parent = a;
        wrong_parent
            .dataset
            .computed
            .get_mut("derived")
            .unwrap()
            .input_fields
            .insert("a".into(), if change_request_kind { 1 } else { 2 });
        if change_request_kind {
            wrong_parent
                .dataset
                .computed
                .get_mut("derived")
                .unwrap()
                .request = docspec::model::OperationRequest::Update(docspec::model::UpdateSpec {
                dataset: first_request.dataset.clone(),
                operation: first_request.operation.clone(),
                base: authored.id.clone(),
                definition: "v1".into(),
                keys: vec!["a".into()],
                replacements: docspec::model::InputSpec {
                    path: first_request.input.clone(),
                    digest: first_request.expected_digest.clone(),
                },
                deletions: None,
                merge: None,
                relationship: None,
            });
        }
        let wrong_parent = rehash(root.path(), "wrong_parent", wrong_parent);
        let mut bad = forge_values(
            &engine,
            root.path(),
            &b,
            vec![Arc::new(Int64Array::from(vec![103]))],
        )
        .await;
        bad.dataset.parent_manifest = Some(wrong_parent.manifest.clone());
        let binding = bad.dataset.computed.get_mut("derived").unwrap();
        binding.base_manifest = wrong_parent.manifest;
        binding.input_fields.insert("a".into(), 2);
        let bad = rehash(root.path(), "wrong_history", bad);
        assert_eq!(
            verify_release(&bad, limits()).await.unwrap_err().code(),
            "invalid_group"
        );
        verify_release(&b, limits()).await.unwrap();
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn cached_readback_requires_older_binding_manifests_and_origin_schemas() {
    for schema in [false, true] {
        let root = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let source = engine
            .run(&request(
                root.path(),
                "numbers",
                "initial",
                None,
                &numbers(1, 100),
            ))
            .await
            .unwrap();
        let authored = engine
            .backfill(&scalar_spec(&source, &[("answer", "a + 1")]))
            .await
            .unwrap();
        let a = engine
            .run(&request(
                root.path(),
                "numbers",
                "a",
                Some(&authored),
                &numbers(2, 101),
            ))
            .await
            .unwrap();
        let request = request(root.path(), "numbers", "b", Some(&a), &numbers(3, 102));
        assert_eq!(
            engine
                .run_until(&request, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let path = if schema {
            &source.dataset.groups["source"].metadata.path
        } else {
            &authored.manifest.path
        };
        let retained = std::fs::read(path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(engine.run(&request).await.is_err());
        assert_eq!(engine.latest(&request.dataset).await.unwrap(), a);
        std::fs::write(path, retained).unwrap();
        let result = engine.run(&request).await.unwrap();
        verify_release(&result, limits()).await.unwrap();
    }
}
