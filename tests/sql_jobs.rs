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

// Raw's comparator failed with these row widths, row counts and limits after
// several retained tables consumed almost all of the native 96 MiB pool.
async fn retained_wide_steps_leave_execution_memory(rows: usize) {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            compute_threads: 2,
            memory_bytes: 192 << 20,
            batch_rows: 32,
            max_batch_bytes: 1 << 20,
            scratch_bytes: 256 << 20,
            ..Limits::default()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    job.register_view("source", &format!("SELECT value AS id, CASE WHEN value%17=0 THEN NULL ELSE concat(cast(value AS VARCHAR),repeat('x',8192)) END AS payload FROM generate_series(1,{rows})"))
        .await.unwrap();
    let old = job
        .materialize("old", "SELECT * FROM source")
        .await
        .unwrap();
    let copy = job
        .materialize("old_copy", "SELECT * FROM old")
        .await
        .unwrap();
    assert_eq!(
        copy,
        StepStorage::Scratch,
        "retention must not consume execution headroom"
    );
    assert_eq!(
        old,
        if rows == 2048 {
            StepStorage::Memory
        } else {
            StepStorage::Scratch
        }
    );
    job.materialize("new", "SELECT * FROM source")
        .await
        .unwrap();
    job.materialize("paired", "SELECT id,max(payload) AS payload,count(*) AS n FROM (SELECT id,payload,row_number() OVER (ORDER BY id,payload) AS occurrence FROM old_copy UNION ALL SELECT id,payload,row_number() OVER (ORDER BY id,payload) AS occurrence FROM new) tagged GROUP BY id,occurrence")
        .await.unwrap();
    let batches = job
        .sql("SELECT count(*),sum(n),count(payload) FROM paired")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for (column, expected) in [rows, rows * 2, rows - rows / 17].into_iter().enumerate() {
        assert_eq!(
            batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected as i64
        );
    }
    // Releasing the retained tables must make the same allowance reusable.
    for name in ["old", "old_copy", "new", "paired"] {
        job.release(name).unwrap();
    }
    assert_eq!(
        job.materialize("again", "SELECT * FROM source LIMIT 2048")
            .await
            .unwrap(),
        StepStorage::Memory
    );
}

#[tokio::test]
async fn retained_wide_steps_fit_192_mib() {
    retained_wide_steps_leave_execution_memory(2048).await;
}

#[tokio::test]
async fn retained_wide_steps_fit_192_mib_at_four_times_rows() {
    retained_wide_steps_leave_execution_memory(8192).await;
}

#[tokio::test]
async fn retained_allowance_lives_until_the_last_frame_owner() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            compute_threads: 2,
            memory_bytes: 192 << 20,
            batch_rows: 32,
            max_batch_bytes: 1 << 20,
            scratch_bytes: 256 << 20,
            ..Limits::default()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    job.register_view(
        "source",
        "SELECT value AS id,repeat('x',8192) AS payload FROM generate_series(1,2048)",
    )
    .await
    .unwrap();
    assert_eq!(
        job.materialize("held", "SELECT * FROM source")
            .await
            .unwrap(),
        StepStorage::Memory
    );
    let frame = job.sql("SELECT * FROM held").await.unwrap();
    job.release("held").unwrap();
    assert_eq!(
        job.materialize("before_drop", "SELECT * FROM source")
            .await
            .unwrap(),
        StepStorage::Scratch
    );
    assert_eq!(frame.count().await.unwrap(), 2048);
    assert_eq!(
        job.materialize("after_drop", "SELECT * FROM source")
            .await
            .unwrap(),
        StepStorage::Memory
    );
}

#[tokio::test]
async fn distinct_preserves_real_seed_join_keys() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    for (name, file) in [
        ("old_ids", "distinct_using_old.parquet"),
        ("new_ids", "distinct_using_new.parquet"),
    ] {
        job.register_parquet(
            name,
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(file),
        )
        .await
        .unwrap();
    }
    let plain = "SELECT o.document_id FROM old_ids o JOIN new_ids n USING(legacy_document_id,document_id) WHERE o.source_ids=n.source_ids";
    let expected = job.sql(plain).await.unwrap().collect().await.unwrap();
    assert_eq!(expected.iter().map(|b| b.num_rows()).sum::<usize>(), 48);
    let actual = job
        .sql(&plain.replacen("SELECT", "SELECT DISTINCT", 1))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = |batches: &[arrow::record_batch::RecordBatch]| {
        let mut rows = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|row| {
                    arrow::util::display::array_value_to_string(b.column(0), row).unwrap()
                })
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = values(&expected);
    assert!(expected.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(values(&actual), expected);
}

#[tokio::test]
async fn byte_fitted_spill_and_row_groups_do_not_accumulate_past_the_allowance() {
    let root = tempfile::tempdir().unwrap();
    let mut ceiling = limits();
    ceiling.memory_bytes = 4 << 20;
    ceiling.max_metadata_bytes = 64 << 10;
    ceiling.batch_rows = 8192;
    ceiling.max_batch_bytes = 64 << 10;
    let job = Job::new(ceiling, root.path().join("scratch")).unwrap();
    job.register_view(
        "source",
        "SELECT unnest(range(0,8192)) AS id,repeat('x',512) AS value",
    )
    .await
    .unwrap();
    assert_eq!(
        job.materialize("spilled", "SELECT * FROM source")
            .await
            .unwrap(),
        StepStorage::Scratch
    );
    let batches = job
        .sql("SELECT count(*) AS n,sum(length(value)) AS bytes FROM spilled")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8192
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8192 * 512
    );
}

#[tokio::test]
async fn unnest_fanout_fits_materialization_without_application_chunking() {
    let root = tempfile::tempdir().unwrap();
    let mut ceiling = limits();
    ceiling.batch_rows = 32;
    ceiling.max_batch_bytes = 65536;
    let job = Job::new(ceiling.clone(), root.path().join("scratch")).unwrap();
    job.materialize(
        "fanout",
        "SELECT unnest(range(0,256)) AS id,repeat('x',1024) AS value",
    )
    .await
    .unwrap();
    let batches = job
        .sql("SELECT * FROM fanout")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches.len() > 1);
    assert!(
        batches
            .iter()
            .all(|batch| batch.get_array_memory_size() <= ceiling.max_batch_bytes)
    );
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &(0..256).collect::<Vec<i64>>()
    );
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        values
            .iter()
            .all(|value| value.is_some_and(|value| value == "x".repeat(1024)))
    );
}

#[tokio::test]
async fn wide_view_batches_fit_before_materialization_and_export() {
    use arrow::{
        array::{ArrayRef, StringViewArray},
        record_batch::RecordBatch,
    };
    use docspec::{
        job::{ExportProfile, ExportSettings},
        model::SortColumn,
    };
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("wide.parquet");
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut columns = vec![Arc::new(Int64Array::from_iter_values(0..32)) as ArrayRef];
    for column in 0..22 {
        fields.push(Field::new(format!("v{column}"), DataType::Utf8View, true));
        columns.push(Arc::new(StringViewArray::from_iter((0..32).map(|row| {
            (row % 5 != 0).then(|| format!("{column}:{row}:{}", "x".repeat(512)))
        }))));
    }
    let source = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&input).unwrap(),
        source.schema(),
        None,
    )
    .unwrap();
    writer.write(&source).unwrap();
    writer.close().unwrap();
    let mut ceiling = limits();
    ceiling.batch_rows = 32;
    ceiling.max_batch_bytes = 128 << 10;
    let job = Job::new(ceiling.clone(), root.path().join("scratch")).unwrap();
    job.register_parquet("wide", &input).await.unwrap();
    job.materialize("fitted", "SELECT * FROM wide")
        .await
        .unwrap();
    let batches = job
        .sql("SELECT * FROM fitted")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches.len() > 1);
    assert!(
        batches
            .iter()
            .all(|b| b.get_array_memory_size() <= ceiling.max_batch_bytes)
    );
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let expected = RecordBatch::try_new(
        actual.schema(),
        source
            .columns()
            .iter()
            .zip(actual.schema().fields())
            .map(|(column, field)| arrow::compute::cast(column, field.data_type()).unwrap())
            .collect(),
    )
    .unwrap();
    assert_eq!(actual, expected);
    let settings = ExportSettings {
        profile: ExportProfile::Whole,
        row_group_rows: 32,
        order_by: vec![SortColumn::asc("id")],
        columns: vec![],
        dictionary_columns: Default::default(),
        delta_columns: Default::default(),
        key_column: None,
        created_by: "byte fitting fixture".into(),
        footer_limit: 128 << 10,
    };
    let output = root.path().join("wide-output.parquet");
    assert_eq!(
        job.export("SELECT * FROM wide", &output, &settings)
            .await
            .unwrap()
            .rows,
        32
    );
    let read = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(output).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        arrow::compute::concat_batches(&read[0].schema(), &read).unwrap(),
        expected
    );
    let mut fragmented_limits = ceiling.clone();
    fragmented_limits.batch_rows = 3;
    let fragmented = Job::new(fragmented_limits, root.path().join("fragmented-scratch")).unwrap();
    fragmented.register_parquet("wide", &input).await.unwrap();
    let fragmented_output = root.path().join("fragmented-output.parquet");
    fragmented
        .export("SELECT * FROM wide", &fragmented_output, &settings)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(root.path().join("wide-output.parquet")).unwrap(),
        std::fs::read(fragmented_output).unwrap()
    );

    let too_large = root.path().join("large.parquet");
    super::scalar::input(&too_large, &[(Some(1), Some("x".repeat(256 << 10)))]);
    job.register_parquet("large", &too_large).await.unwrap();
    assert_eq!(
        job.materialize("refused", "SELECT * FROM large")
            .await
            .unwrap_err()
            .code(),
        "capacity"
    );
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
async fn sparse_schema_uses_actual_decoded_allocation() {
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
    let batches = job
        .sql("SELECT * FROM sparse")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    for batch in batches {
        assert_eq!(batch.num_columns(), 20003);
        assert!(
            batch.columns()[..20000]
                .iter()
                .all(|column| column.is_null(0))
        );
        assert!(batch.get_array_memory_size() <= limits().max_batch_bytes);
    }
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

#[tokio::test]
async fn plain_and_gzip_json_lines_share_exact_evidence_and_conflict_behavior() {
    use docspec::job::JobSpec;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    let raw = b"{\"id\":1,\"value\":\"first\"}\n{\"id\":\"conflict\",\"value\":\"second\"}";
    let plain = root.path().join("plain.gz");
    std::fs::write(&plain, raw).unwrap();
    let gzip = root.path().join("gzip.jsonl");
    let mut writer = GzEncoder::new(std::fs::File::create(&gzip).unwrap(), Compression::fast());
    writer.write_all(raw).unwrap();
    writer.finish().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
    ]);
    let mut results = Vec::new();
    for path in [plain, gzip] {
        let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("scratch"),"inputs":[{"kind":"json_lines","name":"input","files":[{"key":"same/evidence","path":path}],"schema":schema}],"steps":[],"outputs":[],"queries":[{"name":"rows","sql":"SELECT * FROM input ORDER BY __source_line"}]})).unwrap();
        results.push(spec.run(limits()).await.unwrap().queries.remove(0).data);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0][1]["id"], serde_json::Value::Null);
    assert_eq!(
        results[0][0]["__raw_line"],
        std::str::from_utf8(raw)
            .unwrap()
            .split_inclusive('\n')
            .next()
            .unwrap()
    );
    assert_eq!(results[0][1]["__source_line"], 2);
    assert_eq!(results[0][1]["__source_key"], "same/evidence");
    let broken = root.path().join("broken.jsonl");
    std::fs::write(&broken, [0x1f, 0x8b, 0x08]).unwrap();
    let job = Job::new(limits(), root.path().join("broken-scratch")).unwrap();
    job.register_json_lines(
        "broken",
        vec![JsonLinesFile {
            key: "broken/evidence".into(),
            path: broken,
        }],
        Arc::new(schema),
    )
    .unwrap();
    assert!(
        job.sql("SELECT * FROM broken")
            .await
            .unwrap()
            .collect()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn two_hundred_small_json_files_reuse_bounded_reader_slots() {
    let root = tempfile::tempdir().unwrap();
    let mut files = Vec::new();
    for index in 0..200 {
        let path = root.path().join(format!("{index}.jsonl"));
        std::fs::write(&path, "{\"id\":7}\n").unwrap();
        files.push(JsonLinesFile {
            key: format!("file-{index}"),
            path,
        });
    }
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    job.register_json_lines(
        "input",
        files,
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
    )
    .unwrap();
    let rows=job.sql("SELECT count(DISTINCT __source_key),sum(id),min(__source_line),max(__source_line) FROM input").await.unwrap().collect().await.unwrap();
    for (column, expected) in [200, 1400, 1, 1].into_iter().enumerate() {
        assert_eq!(
            rows[0]
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
}

#[tokio::test]
async fn sparse_json_fixed_width_output_is_charged_before_yield() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("sparse.jsonl");
    std::fs::write(
        &path,
        format!("{{\"items\":[{}]}}\n", vec!["null"; 4096].join(",")),
    )
    .unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    job.register_json_lines(
        "input",
        vec![JsonLinesFile {
            key: "sparse".into(),
            path,
        }],
        Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeBinary(4096),
                true,
            ))),
            true,
        )])),
    )
    .unwrap();
    let error = job
        .sql("SELECT * FROM input")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    assert!(error.to_string().contains("JSON-lines decoder"), "{error}");
}

#[tokio::test]
async fn run_end_encoded_fixed_width_output_is_charged_before_yield() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wrapped.jsonl");
    std::fs::write(&path, "{\"wrapped\":null}\n").unwrap();
    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    let encoded = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new(
            "values",
            DataType::FixedSizeBinary(32 << 20),
            true,
        )),
    );
    job.register_json_lines(
        "input",
        vec![JsonLinesFile {
            key: "wrapped".into(),
            path,
        }],
        Arc::new(Schema::new(vec![Field::new("wrapped", encoded, true)])),
    )
    .unwrap();
    let error = job
        .sql("SELECT * FROM input")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    assert!(error.to_string().contains("JSON-lines decoder"), "{error}");
}

#[tokio::test]
async fn json_view_schemas_admit_native_descriptor_capacity_and_keep_values() {
    use arrow::array::{BinaryViewArray, StringViewArray};
    for data_type in [DataType::Utf8View, DataType::BinaryView] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("view.jsonl");
        let text = "c".repeat(1 << 20);
        std::fs::write(&path, format!("{{\"value\":\"{text}\"}}\n")).unwrap();
        let ceiling = Limits {
            memory_bytes: 64 << 20,
            max_batch_bytes: 32 << 20,
            ..limits()
        };
        let job = Job::new(ceiling, root.path().join("scratch")).unwrap();
        job.register_json_lines(
            "input",
            vec![JsonLinesFile {
                key: "view".into(),
                path,
            }],
            Arc::new(Schema::new(vec![Field::new(
                "value",
                data_type.clone(),
                true,
            )])),
        )
        .unwrap();
        let batches = job
            .sql("SELECT * FROM input")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let value = batches[0].column(0);
        assert_eq!(value.data_type(), &data_type);
        if data_type == DataType::Utf8View {
            assert_eq!(
                value
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .value(0),
                text
            );
        } else {
            assert_eq!(
                value
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .unwrap()
                    .value(0),
                vec![0xcc; 1 << 19].as_slice()
            );
        }
    }
}

#[tokio::test]
async fn json_reservation_errors_keep_file_key_line_and_resource_cause() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("two.jsonl");
    std::fs::write(
        &path,
        format!(
            "{{\"id\":1}}\n{{\"ignored\":\"{}\"}}\n",
            "x".repeat(700 << 10)
        ),
    )
    .unwrap();
    let job = Job::new(
        Limits {
            memory_bytes: 2 << 20,
            batch_rows: 1,
            compute_threads: 1,
            ..limits()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    job.register_json_lines(
        "input",
        vec![JsonLinesFile {
            key: "context-fixture".into(),
            path,
        }],
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
    )
    .unwrap();
    let error = job
        .sql("SELECT id FROM input")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("context-fixture line 2"),
        "{error}"
    );
}
