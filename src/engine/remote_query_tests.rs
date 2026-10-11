//! Maintained S3 Parquet reads through the privately composed Engine runtime.

use super::*;
use crate::storage::remote::objects::tests::ReadFixture;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;

#[tokio::test]
async fn composed_remote_context_queries_projected_filtered_parquet() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("counter", DataType::Int64, false),
        Field::new("unused", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("same"),
                None,
                Some("same"),
                Some("excluded"),
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 1, 3])),
            Arc::new(StringArray::from(vec!["not selected"; 4])),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let path = "s3://bucket/owned/input.parquet";
    let fixture = ReadFixture::inventory(HashMap::from([(path.into(), bytes)]));
    let remote = fixture.objects();
    assert!(!format!("{remote:?}").contains("dummy-secret"));
    let objects = storage::Objects::Remote(remote);
    // Query clients address a bucket. Existing admission remains the authority
    // for exact retained input URI scope; reject an escaped input before I/O.
    assert_eq!(
        objects
            .validate_retained_path("s3://bucket/outside/input.parquet")
            .unwrap_err()
            .code(),
        "remote_scope"
    );
    objects.validate_retained_path(path).unwrap();
    let host = tempfile::tempdir().unwrap();
    let ownership = worker::Host::acquire(host.path()).unwrap();
    let catalog = storage::local_catalog(host.path()).await.unwrap();
    let engine = Engine::compose(
        host.path().to_owned(),
        catalog,
        objects,
        Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 1 << 20,
            max_metadata_bytes: 1 << 20,
        },
        ownership,
        true,
    )
    .unwrap();
    let context = engine.context();
    context
        .register_parquet("input", path, ParquetReadOptions::default())
        .await
        .unwrap();
    let before = fixture.observed_paths();
    for sql in [
        "COPY input TO 's3://bucket/outside/copied.parquet' STORED AS PARQUET",
        "CREATE EXTERNAL TABLE outside_input STORED AS PARQUET LOCATION 's3://bucket/outside/input.parquet'",
        "SET datafusion.execution.target_partitions = 2",
    ] {
        assert!(write::build_query(&context, sql).await.is_err());
    }
    assert_eq!(
        fixture.observed_paths(),
        before,
        "forbidden SQL performs no transport effects"
    );
    let dataframe = write::build_query(&context, "SELECT text FROM input WHERE counter < 3")
        .await
        .unwrap();
    assert_eq!(dataframe.schema().fields().len(), 1);
    let mut stream = dataframe.execute_stream().await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        // DataFusion can expose its maintained Utf8View representation.
        let strings = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        let text = strings.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            values.push((!text.is_null(row)).then(|| text.value(row).to_owned()));
        }
    }
    values.sort();
    assert_eq!(values, vec![None, Some("same".into()), Some("same".into())]);
    engine.shutdown().await.unwrap();
    assert!(fixture.observed_paths().iter().all(|(_, uri)| uri == path));
    let requests = fixture.finish();
    assert!(requests.iter().any(|(method, _)| method == "GET"));
    assert!(
        requests
            .iter()
            .all(|(method, _)| matches!(method.as_str(), "HEAD" | "GET"))
    );
}
