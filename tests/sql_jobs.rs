//! Multi-input jobs preserve source evidence and own materialized memory.
// Rust guideline compliant 2026-02-21
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use docspec::{
    budget::Limits,
    job::{Job, JsonLinesFile, StepStorage},
};
use flate2::{Compression, write::GzEncoder};
use std::{io::Write, sync::Arc};

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 32 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 32 << 20,
        ..Limits::default()
    }
}

#[tokio::test]
async fn joins_files_and_nullable_json_without_losing_raw_lines() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input.parquet");
    super::scalar::input(
        &input,
        &[
            (Some(1), Some("first".into())),
            (Some(2), Some("second".into())),
        ],
    );
    let path = root.path().join("lines.gz");
    let raw = "{\"id\":1,\"value\":7}\n{\"id\":2,\"value\":\"older shape\"}\n";
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(raw.as_bytes()).unwrap();
    gzip.finish().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    job.register_parquet("items", &input).await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    job.register_json_lines(
        "events",
        vec![JsonLinesFile {
            key: "history/one".into(),
            path,
        }],
        schema,
    )
    .unwrap();
    let storage = job.materialize("joined", "SELECT i.id,i.text,e.value,e.__source_key,e.__source_line,e.__raw_line FROM items i JOIN events e ON i.id=e.id").await.unwrap();
    assert_eq!(storage, StepStorage::Memory);
    let batches = job
        .sql("SELECT * FROM joined ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 7);
    assert!(values.is_null(1));
    let lines = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(lines.values().as_ref(), &[1, 2]);
    let originals = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        originals.value(1),
        raw.split_inclusive('\n').nth(1).unwrap()
    );
    // Repeat reads reuse the materialized result after its source disappears.
    std::fs::remove_file(input).unwrap();
    assert_eq!(
        job.sql("SELECT * FROM joined")
            .await
            .unwrap()
            .count()
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn named_step_spills_and_keeps_exact_rows() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    let storage = job
        .materialize(
            "large",
            "SELECT value, repeat('x',1024) AS text FROM generate_series(1,24000)",
        )
        .await
        .unwrap();
    assert_eq!(storage, StepStorage::Scratch);
    assert_eq!(
        job.sql("SELECT * FROM large")
            .await
            .unwrap()
            .count()
            .await
            .unwrap(),
        24000
    );
}

#[tokio::test]
async fn refuses_required_json_fields_and_reports_exact_bad_line_safely() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("broken.gz");
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(b"{\"id\":1}\n{SECRET\n").unwrap();
    gzip.finish().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    let required = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let files = vec![JsonLinesFile {
        key: "history/bad".into(),
        path,
    }];
    assert!(
        job.register_json_lines("invalid", files.clone(), required)
            .is_err()
    );
    job.register_json_lines(
        "bad",
        files,
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
    )
    .unwrap();
    let error = job
        .sql("SELECT * FROM bad")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("history/bad line 2"), "{error}");
    assert!(!error.contains("SECRET"));
}

#[tokio::test]
async fn sparse_schema_is_admitted_before_decoding() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("sparse.gz");
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(b"{}\n").unwrap();
    gzip.finish().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    let fields = (0..20000)
        .map(|n| Field::new(format!("field{n}"), DataType::Int64, true))
        .collect::<Vec<_>>();
    job.register_json_lines(
        "sparse",
        vec![JsonLinesFile {
            key: "sparse".into(),
            path,
        }],
        Arc::new(Schema::new(fields)),
    )
    .unwrap();
    let error = job
        .sql("SELECT * FROM sparse")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
}

fn scratch_files(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(scratch_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

#[tokio::test]
async fn scratch_quota_is_shared_and_files_live_as_long_as_frames() {
    let root = tempfile::tempdir().unwrap();
    let scratch = root.path().join("scratch");
    let sql = "SELECT value,repeat('x',1024) AS text FROM generate_series(1,24000)";
    let job = Job::new(limits(), scratch.clone()).unwrap();
    job.materialize("first", sql).await.unwrap();
    let size = scratch_files(&scratch)
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum::<u64>();
    let frame = job.sql("SELECT * FROM first").await.unwrap();
    drop(job);
    assert!(!scratch_files(&scratch).is_empty());
    assert_eq!(frame.count().await.unwrap(), 24000);
    assert!(scratch_files(&scratch).is_empty());
    let mut bounded = limits();
    bounded.scratch_bytes = size * 2 - 1;
    let job = Job::new(bounded, scratch.clone()).unwrap();
    job.materialize("first", sql).await.unwrap();
    assert!(job.materialize("second", sql).await.is_err());
    assert_eq!(
        scratch_files(&scratch).len(),
        1,
        "failed spill must release its file"
    );
    assert_eq!(
        job.sql("SELECT * FROM first")
            .await
            .unwrap()
            .count()
            .await
            .unwrap(),
        24000
    );
    drop(job);
    assert!(scratch_files(&scratch).is_empty());
}

#[tokio::test]
async fn joins_two_pinned_tables_with_file_and_receipt() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, rows) = super::scalar::fixture(root.path(), false).await;
    let source = &release.dataset.groups["source"];
    let input = &release.dataset.inputs["input"];
    let partitioned = engine
        .run(&docspec::model::RunSpec {
            dataset: super::scalar::id("partitioned"),
            operation: super::scalar::id("partitioned-initial"),
            base: None,
            relationship: None,
            input: input.path.clone(),
            expected_digest: input.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: vec![docspec::model::PartitionFieldSpec {
                column: "id".into(),
                name: None,
                transform: docspec::model::PartitionTransformSpec::Identity,
            }],
        })
        .await
        .unwrap();
    let path = root.path().join("event.gz");
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(b"{\"id\":1}\n").unwrap();
    gzip.finish().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    job.register_iceberg("a", source).await.unwrap();
    job.register_iceberg("b", &partitioned.dataset.groups["source"])
        .await
        .unwrap();
    let plan = job
        .sql("SELECT id FROM b WHERE id=1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let description = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        description.contains("IcebergTableScan projection:[id]"),
        "{description}"
    );
    assert!(!description.contains("predicate:[]"), "{description}");
    job.register_parquet("f", &root.path().join("input.parquet"))
        .await
        .unwrap();
    job.register_json_lines(
        "r",
        vec![JsonLinesFile {
            key: "event".into(),
            path,
        }],
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
    )
    .unwrap();
    let result=job.sql("SELECT a.id FROM a JOIN b ON a.id=b.id JOIN f ON a.id=f.id JOIN r ON a.id=r.id WHERE a.id=1").await.unwrap().count().await.unwrap();
    let n = rows.iter().filter(|row| row.0 == Some(1)).count();
    assert_eq!(result, n * n * n);
}

#[tokio::test]
async fn released_names_keep_existing_frames_and_free_scratch_at_last_use() {
    let root = tempfile::tempdir().unwrap();
    let scratch = root.path().join("scratch");
    let job = Job::new(limits(), scratch.clone()).unwrap();
    assert_eq!(
        job.materialize(
            "held",
            "SELECT value,repeat('x',1024) AS text FROM generate_series(1,24000)"
        )
        .await
        .unwrap(),
        StepStorage::Scratch
    );
    let planned = job.sql("SELECT * FROM held").await.unwrap();
    job.release("held").unwrap();
    assert!(job.sql("SELECT * FROM held").await.is_err());
    assert!(!scratch_files(&scratch).is_empty());
    assert_eq!(planned.count().await.unwrap(), 24000);
    assert!(scratch_files(&scratch).is_empty());
    assert!(job.release("held").is_err());
}
