//! Native output recovery across the gap before a lifecycle phase completes.
use super::*;
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

#[tokio::test]
async fn initial_candidate_recovers_committed_snapshot_without_producer_or_second_append() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("producer.parquet");
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            Some(1_i64),
            None,
            Some(1),
            Some(2),
        ]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&source).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("host"),
        Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 256 << 10,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let spec = RunSpec {
        dataset: Id::new("dataset").unwrap(),
        operation: Id::new("initial").unwrap(),
        base: None,
        relationship: None,
        input: source.to_str().unwrap().into(),
        expected_digest: storage::digest(&source).unwrap().0,
        sql: "SELECT value FROM input WHERE value IS NULL OR value < 2".into(),
        definition: "v1".into(),
    };
    assert!(engine.run_until(&spec, "admission").await.is_err());
    let request = OperationRequest::Build(spec.clone());
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (_, _, operation) = rows
        .get_record::<Operation>(&format!("operation/{}", spec.operation))
        .unwrap()
        .unwrap();
    let admission = engine
        .phase(&request, &operation, "admission")
        .await
        .unwrap()
        .unwrap();
    // Stop after the actual durable intent reservation, before metadata create
    // or registration. The subsequent call must resume this same candidate.
    let input = &admission.outputs[0];
    let frame = engine.initial_source(&spec, input).await.unwrap();
    let schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(frame.arrow_schema()).unwrap();
    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::Release);
    let saved = engine
        .reserve_candidate(&spec, &operation, input, schema)
        .await
        .unwrap();
    let pending = &saved.pending;
    assert!(
        saved.resuming,
        "lost intent commit response reconciles the exact durable candidate"
    );
    assert!(!Path::new(&pending.metadata.path).exists());
    assert!(
        !engine
            .inner
            .lifecycle
            .catalog
            .table_exists(&pending.identifier)
            .await
            .unwrap()
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    validate(
        &key(
            &spec.operation,
            pending.operation_revision,
            pending.processing_revision,
            &pending.input,
        )
        .unwrap(),
        pending,
        &rows,
    )
    .unwrap();
    drop(frame);
    // Model a stopped local writer before final publication. Its partial
    // temporary stays unreferenced; resumption publishes the exact final bytes.
    let parent = Path::new(&pending.metadata.path).parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();
    let partial = parent.join(".interrupted.candidate");
    std::fs::write(&partial, &pending.metadata_json.as_bytes()[..1]).unwrap();
    // Deliberately leave processing uncompleted after actual native append.
    let first = engine
        .process_initial_build(&spec, &operation, &admission)
        .await
        .unwrap();
    assert_eq!(first.table.table, pending.identifier.name());
    assert!(
        engine
            .phase(&request, &operation, "processing")
            .await
            .unwrap()
            .is_none()
    );
    std::fs::remove_file(&source).unwrap();
    let query_count = engine
        .inner
        .initial_build_queries
        .load(std::sync::atomic::Ordering::Relaxed);
    let recovered = engine
        .process_initial_build(&spec, &operation, &admission)
        .await
        .unwrap();
    assert_eq!(recovered.table, first.table);
    assert_eq!(recovered.metrics, first.metrics);
    assert_eq!(recovered.table.rows, 3);
    assert_eq!(
        std::fs::read(&pending.metadata.path).unwrap(),
        pending.metadata_json.as_bytes()
    );
    assert_eq!(std::fs::read(&partial).unwrap().len(), 1);
    // Exercise the owned Engine path, including admission availability and
    // normal processing completion, after the original producer disappears.
    let error = engine.run_until(&spec, "processing").await.unwrap_err();
    assert_eq!(error.code(), "injected_failure");
    let phase = engine
        .phase(&request, &operation, "processing")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(phase.table.as_ref(), Some(&first.table));
    assert_eq!(phase.metrics, first.metrics);
    assert_eq!(
        engine
            .inner
            .initial_build_queries
            .load(std::sync::atomic::Ordering::Relaxed),
        query_count,
        "recovery never opens Parquet or plans/executes SQL"
    );
    let table = storage::open_pin(&recovered.table, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    assert_eq!(table.metadata().snapshots().len(), 1);
    let mut stream = Box::pin(
        storage::read_batches(&table, &engine.inner.lifecycle.budget)
            .await
            .unwrap(),
    );
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual
            .extend((0..values.len()).map(|row| (!values.is_null(row)).then(|| values.value(row))));
    }
    actual.sort();
    assert_eq!(actual, vec![None, Some(1), Some(1)]);
    drop(stream);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_candidate_append_refuses_before_native_manifest_effects() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path().join("host"),
        Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 256 << 10,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let owner = Id::new("owner").unwrap();
    let directory = root.path().join("candidate");
    let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow).unwrap();
    let initial = engine
        .create_output(&owner, &directory, schema)
        .await
        .unwrap();
    let batch =
        RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![Some(1_i64)]))]).unwrap();
    let files = storage::write_batches(
        &initial,
        futures::stream::iter([Ok(batch)]),
        &engine.inner.lifecycle.budget,
    )
    .await
    .unwrap();
    storage::append(
        &initial,
        engine.inner.lifecycle.catalog.as_ref(),
        files.clone(),
    )
    .await
    .unwrap();
    let paths = || {
        let mut names = std::fs::read_dir(directory.join("metadata"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    let before = paths();
    let error = storage::append(&initial, engine.inner.lifecycle.catalog.as_ref(), files)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("stale_authority"));
    assert_eq!(
        paths(),
        before,
        "stale refresh writes no manifest or metadata"
    );
    let current = engine
        .inner
        .lifecycle
        .catalog
        .load_table(initial.identifier())
        .await
        .unwrap();
    assert_eq!(current.metadata().snapshots().len(), 1);
    engine.shutdown().await.unwrap();
}
