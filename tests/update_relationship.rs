//! An exact-base Update can advance a dataset's coupled producer generation.
//!
//! `UpdateSpec::relationship` is explicit. Omitting it retains the base
//! relationship; supplying it replaces the producer declaration so a later
//! generation can be sealed and consumed. Previously the field did not exist
//! and callers that sent one had it silently dropped.

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{GenerationSeal, Id, InputSpec, RunSpec, SourceRelationship, UpdateSpec, VersionRef},
    storage,
};
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

type Row = (Option<i64>, Option<String>);

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 16 << 20,
        ..Limits::default()
    }
}

fn input(path: &Path, rows: &[Row], keys_only: bool) -> InputSpec {
    let mut fields = vec![Field::new("id", DataType::Int64, true)];
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    ))];
    if !keys_only {
        fields.push(Field::new("text", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

fn coupled(generation: &str) -> SourceRelationship {
    SourceRelationship::Coupled {
        family: id("news"),
        generation: id(generation),
    }
}

fn version_ref(pin: &docspec::model::ReleasePin) -> VersionRef {
    VersionRef {
        dataset: pin.dataset.dataset.clone(),
        version: pin.id.clone(),
        manifest: pin.manifest.clone(),
    }
}

async fn seal(engine: &Engine, generation: &str, pin: &docspec::model::ReleasePin) {
    let mut members = BTreeMap::new();
    members.insert(pin.dataset.dataset.clone(), version_ref(pin));
    engine
        .seal_generation(&GenerationSeal {
            family: id("news"),
            generation: id(generation),
            members,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn update_can_advance_a_coupled_generation() {
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();

    let initial_rows: Vec<Row> = (0_i64..8)
        .map(|value| (Some(value), Some(format!("row {value}"))))
        .collect();
    let source = input(&root.path().join("initial.parquet"), &initial_rows, false);
    let build = RunSpec {
        dataset: id("news"),
        operation: id("initial"),
        base: None,
        relationship: Some(coupled("g1")),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "news-v1".into(),
    };
    let first = engine.run(&build).await.unwrap();
    assert_eq!(first.dataset.relationship, coupled("g1"));
    seal(&engine, "g1", &first).await;

    let replacement = input(
        &root.path().join("replacement.parquet"),
        &[(Some(0), Some("row 0 corrected".into()))],
        false,
    );
    let update = UpdateSpec {
        dataset: id("news"),
        operation: id("poll-2"),
        definition: "news-v1".into(),
        base: first.id.clone(),
        keys: vec!["id".into()],
        replacements: replacement,
        deletions: None,
        merge: None,
        relationship: Some(coupled("g2")),
    };
    let second = engine.update(&update).await.unwrap();
    assert_eq!(second.dataset.relationship, coupled("g2"));

    // The first generation remains sealed and readable.
    assert_eq!(engine.latest(&id("news")).await.unwrap().id, second.id);

    seal(&engine, "g2", &second).await;

    // Consuming the new generation proves the full member set still matches
    // and that the update advanced every member's declaration.
    let mut inputs = BTreeMap::new();
    inputs.insert(id("news"), version_ref(&second));
    let coverage = engine.capture_inputs(&inputs).await.unwrap();
    assert_eq!(
        coverage.generations.get(&id("news")).unwrap().generation,
        id("g2")
    );

    // A restarted host resolves the retained relationship, not an in-memory value.
    drop(engine);
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(
        engine
            .latest(&id("news"))
            .await
            .unwrap()
            .dataset
            .relationship,
        coupled("g2")
    );
}

#[tokio::test]
async fn omitting_update_relationship_retains_the_base_declaration() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let initial_rows: Vec<Row> = (0_i64..4)
        .map(|value| (Some(value), Some(format!("row {value}"))))
        .collect();
    let source = input(&root.path().join("initial.parquet"), &initial_rows, false);
    let build = RunSpec {
        dataset: id("news"),
        operation: id("initial"),
        base: None,
        relationship: Some(coupled("g1")),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "news-v1".into(),
    };
    let first = engine.run(&build).await.unwrap();

    let update = UpdateSpec {
        dataset: id("news"),
        operation: id("poll-2"),
        definition: "news-v1".into(),
        base: first.id.clone(),
        keys: vec!["id".into()],
        replacements: input(
            &root.path().join("replacement.parquet"),
            &[(Some(1), Some("row 1 corrected".into()))],
            false,
        ),
        deletions: None,
        merge: None,
        relationship: None,
    };
    let second = engine.update(&update).await.unwrap();
    assert_eq!(second.dataset.relationship, coupled("g1"));
}

#[tokio::test]
async fn read_keys_and_update_prune_non_overlapping_files_and_project_columns() {
    use futures::TryStreamExt;
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();

    let batch1: Vec<Row> = (0_i64..4)
        .map(|v| (Some(v), Some(format!("payload-{v}"))))
        .collect();
    let source1 = input(&root.path().join("b1.parquet"), &batch1, false);
    let r1 = engine
        .run(&RunSpec {
            dataset: id("news"),
            operation: id("b1"),
            base: None,
            relationship: None,
            input: source1.path,
            expected_digest: source1.digest,
            sql: "SELECT * FROM input".into(),
            definition: "news-v1".into(),
        })
        .await
        .unwrap();

    let batch2: Vec<Row> = (100_i64..104)
        .map(|v| (Some(v), Some(format!("payload-{v}"))))
        .collect();
    let source2 = input(&root.path().join("b2.parquet"), &batch2, false);
    let r2 = engine
        .update(&UpdateSpec {
            dataset: id("news"),
            operation: id("b2"),
            definition: "news-v1".into(),
            base: r1.id.clone(),
            keys: vec!["id".into()],
            replacements: source2,
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    // Lookup only id=102 with projected columns=["id"]:
    let stream = engine
        .read_keys(
            &r2,
            &docspec::model::KeyReadSpec {
                key: "id".into(),
                values: vec!["102".into()],
                columns: Some(vec!["id".into()]),
            },
        )
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);
    assert_eq!(batches[0].num_columns(), 1);
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 102);
}
