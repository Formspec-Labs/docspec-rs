//! Native footer checks over maintained filesystem and memory storage.

use super::read_footer;
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use iceberg::io::FileIO;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

fn parquet_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

async fn store(io: &FileIO, path: &str, bytes: Vec<u8>) {
    io.new_output(path)
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
}

#[tokio::test]
async fn local_and_memory_use_the_same_bounded_footer_decoder() {
    let bytes = parquet_bytes();
    let tail: [u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    let footer_size = super::FooterTail::try_from(tail).unwrap().metadata_length();
    let temporary = tempfile::tempdir().unwrap();
    let local_path = temporary.path().join("fixture.parquet");
    for (io, path) in [
        (
            FileIO::new_with_fs(),
            local_path.to_str().unwrap().to_owned(),
        ),
        (
            FileIO::new_with_memory(),
            "memory:///fixture.parquet".into(),
        ),
    ] {
        store(&io, &path, bytes.clone()).await;
        let input = io.new_input(&path).unwrap();
        let handle = tokio::runtime::Handle::current();
        let size = bytes.len() as u64;
        let footer = tokio::task::spawn_blocking(move || {
            handle.block_on(read_footer(&input, size, footer_size))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(footer.file_metadata().num_rows(), 3);
        assert_eq!(footer.file_metadata().schema_descr().num_columns(), 1);
        assert_eq!(footer.file_metadata().schema_descr().column(0).name(), "id");
        assert_eq!(
            read_footer(&io.new_input(&path).unwrap(), size, footer_size - 1)
                .await
                .unwrap_err()
                .code(),
            "metadata_capacity"
        );
        assert_eq!(
            read_footer(&io.new_input(&path).unwrap(), size + 1, footer_size)
                .await
                .unwrap_err()
                .code(),
            "file_size_mismatch"
        );
    }
}

#[tokio::test]
async fn malformed_tail_and_declared_lengths_refuse_before_body_decode() {
    let io = FileIO::new_with_memory();
    // Empty/tiny objects, bad magic, a truncated body, and an oversized declaration.
    for (bytes, limit, expected) in [
        (vec![], 64, "parquet"),
        (vec![0; 7], 64, "parquet"),
        (vec![0; 8], 64, "parquet"),
        (
            [64u32.to_le_bytes().as_slice(), b"PAR1"].concat(),
            64,
            "parquet",
        ),
        (
            [65u32.to_le_bytes().as_slice(), b"PAR1"].concat(),
            64,
            "metadata_capacity",
        ),
        (
            [&[0xff][..], 1u32.to_le_bytes().as_slice(), b"PAR1"].concat(),
            64,
            "parquet",
        ),
        (
            [0u32.to_le_bytes().as_slice(), b"PAR1"].concat(),
            64,
            "parquet",
        ),
        (
            [0u32.to_le_bytes().as_slice(), b"PARE"].concat(),
            64,
            "parquet",
        ),
    ] {
        let size = bytes.len() as u64;
        let path = "memory:///malformed.parquet";
        store(&io, path, bytes).await;
        assert_eq!(
            read_footer(&io.new_input(path).unwrap(), size, limit)
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
}

struct CountingReader {
    inner: Box<dyn iceberg::io::FileRead>,
    ranges: std::sync::Mutex<Vec<std::ops::Range<u64>>>,
    short_read: Option<usize>,
}

#[async_trait::async_trait]
impl iceberg::io::FileRead for CountingReader {
    async fn read(&self, range: std::ops::Range<u64>) -> iceberg::Result<bytes::Bytes> {
        let call = {
            let mut ranges = self.ranges.lock().unwrap();
            ranges.push(range.clone());
            ranges.len()
        };
        let bytes = self.inner.read(range).await?;
        if self.short_read == Some(call) {
            Ok(bytes.slice(..bytes.len() - 1))
        } else {
            Ok(bytes)
        }
    }
}

#[tokio::test]
async fn ranges_are_disjoint_and_short_reads_refuse_without_retry() {
    let bytes = parquet_bytes();
    let size = bytes.len() as u64;
    let tail: [u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    let footer_size = super::FooterTail::try_from(tail).unwrap().metadata_length();
    let io = FileIO::new_with_memory();
    let path = "memory:///counted.parquet";
    store(&io, path, bytes).await;
    let input = io.new_input(path).unwrap();
    let expected = vec![size - 8..size, size - 8 - footer_size as u64..size - 8];
    for short_read in [None, Some(1), Some(2)] {
        let reader = CountingReader {
            inner: input.reader().await.unwrap(),
            ranges: std::sync::Mutex::new(Vec::new()),
            short_read,
        };
        let decoded = super::footer_decoder(&reader, size, footer_size).await;
        if let Some(call) = short_read {
            assert_eq!(decoded.unwrap_err().code(), "parquet");
            assert_eq!(*reader.ranges.lock().unwrap(), expected[..call]);
        } else {
            let mut decoder = decoded.unwrap();
            let parquet::DecodeResult::Data(footer) = decoder.try_decode().unwrap() else {
                panic!("complete footer ranges did not decode");
            };
            assert_eq!(footer.file_metadata().num_rows(), 3);
            assert_eq!(*reader.ranges.lock().unwrap(), expected);
        }
    }
    let reader = CountingReader {
        inner: input.reader().await.unwrap(),
        ranges: std::sync::Mutex::new(Vec::new()),
        short_read: None,
    };
    assert_eq!(
        super::footer_decoder(&reader, size, footer_size - 1)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    assert_eq!(*reader.ranges.lock().unwrap(), expected[..1]);
}

#[tokio::test]
async fn impossible_footer_lengths_do_not_request_a_body() {
    let io = FileIO::new_with_memory();
    let path = "memory:///truncated-counted.parquet";
    store(
        &io,
        path,
        [64u32.to_le_bytes().as_slice(), b"PAR1"].concat(),
    )
    .await;
    let reader = CountingReader {
        inner: io.new_input(path).unwrap().reader().await.unwrap(),
        ranges: std::sync::Mutex::new(Vec::new()),
        short_read: None,
    };
    assert_eq!(
        super::footer_decoder(&reader, 8, 64)
            .await
            .unwrap_err()
            .code(),
        "parquet"
    );
    assert_eq!(*reader.ranges.lock().unwrap(), vec![0..8]);
    reader.ranges.lock().unwrap().clear();
    assert_eq!(
        super::footer_decoder(&reader, 7, 64)
            .await
            .unwrap_err()
            .code(),
        "parquet"
    );
    assert!(reader.ranges.lock().unwrap().is_empty());
}
