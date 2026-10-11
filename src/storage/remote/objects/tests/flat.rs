//! The configured flat reader preserves values and uses only bounded ranges.

use super::*;
use crate::storage::Objects;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

fn payload() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)])),
            Arc::new(StringArray::from(vec![
                Some("same"),
                Some("same"),
                None,
                Some(""),
            ])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap()
}

#[tokio::test]
async fn flat_reader_reuses_one_footer_and_preserves_duplicates_null_and_empty() {
    let bytes = payload();
    let expected = object(&bytes);
    let footer_len = parquet::file::metadata::FooterTail::try_from(
        <[u8; 8]>::try_from(&bytes[bytes.len() - 8..]).unwrap(),
    )
    .unwrap()
    .metadata_length() as u64;
    let fixture = ReadFixture::inventory(HashMap::from([(expected.path.clone(), bytes)]));
    let objects = Objects::Remote(fixture.objects());
    let budget = Budget::new(Limits {
        memory_bytes: 32 << 20,
        max_batch_bytes: 4 << 20,
        batch_rows: 2,
        ..Limits::default()
    })
    .unwrap();
    let mut outside = expected.clone();
    outside.path = "s3://bucket/elsewhere/file.bin".into();
    let error = match objects.read_flat_parquet(&outside, &budget).await {
        Ok(_) => panic!("out-of-scope reader succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "remote_scope");
    assert!(fixture.observed_paths().is_empty());
    let mut stream = objects.read_flat_parquet(&expected, &budget).await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert!(batch.num_rows() <= 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let texts = texts.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            values.push((
                (!ids.is_null(row)).then(|| ids.value(row)),
                (!texts.is_null(row)).then(|| texts.value(row).to_owned()),
            ));
        }
    }
    assert_eq!(
        values,
        vec![
            (Some(1), Some("same".into())),
            (Some(1), Some("same".into())),
            (None, None),
            (Some(2), Some(String::new()))
        ]
    );
    let requests = fixture.finish();
    assert!(
        requests
            .iter()
            .all(|(method, range)| method == "GET" && range.is_some())
    );
    let tail = format!("bytes={}-{}", expected.bytes - 8, expected.bytes - 1);
    assert_eq!(
        requests
            .iter()
            .filter(|(_, range)| range.as_ref() == Some(&tail))
            .count(),
        1
    );
    let footer = format!(
        "bytes={}-{}",
        expected.bytes - 8 - footer_len,
        expected.bytes - 9
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(_, range)| range.as_ref() == Some(&footer))
            .count(),
        1
    );
    assert!(requests.len() >= 3);
}

#[tokio::test]
async fn flat_reader_bounds_footer_before_decode_and_releases_failed_open_capacity() {
    let bytes = payload();
    let expected = object(&bytes);
    let fixture = ReadFixture::inventory(HashMap::from([(expected.path.clone(), bytes)]));
    let objects = Objects::Remote(fixture.objects());
    let budget = Budget::new(Limits {
        memory_bytes: 8 << 20,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 1,
        ..Limits::default()
    })
    .unwrap();
    let error = match objects.read_flat_parquet(&expected, &budget).await {
        Ok(_) => panic!("oversized footer succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "metadata_capacity");
    assert_eq!(fixture.finish().len(), 1);
    // The failed opener owns no lingering transport reservation.
    let _all = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
}
