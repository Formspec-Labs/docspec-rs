//! Export settings and byte identity are independent of source ordering.
// Rust guideline compliant 2026-02-21
use docspec::{
    budget::Limits,
    job::{ExportProfile, ExportSettings, Job},
    model::SortColumn,
};
use parquet::{
    basic::{Compression, Encoding},
    file::reader::{FileReader, SerializedFileReader},
};
use std::collections::BTreeSet;

fn settings(profile: ExportProfile) -> ExportSettings {
    ExportSettings {
        profile,
        row_group_rows: 2,
        order_by: vec![SortColumn::asc("id")],
        columns: vec![],
        dictionary_columns: BTreeSet::from(["kind".into()]),
        delta_columns: BTreeSet::from(["text".into()]),
        key_column: Some("id".into()),
        created_by: "docspec export fixture v1".into(),
        footer_limit: 512 << 10,
    }
}

#[tokio::test]
async fn smallest_byte_allowances_refuse_before_writing_without_panicking() {
    let root = tempfile::tempdir().unwrap();
    for bytes in 1..=3 {
        let job = Job::new(
            Limits {
                memory_bytes: 32 << 20,
                max_batch_bytes: bytes,
                ..Limits::default()
            },
            root.path().join(format!("scratch-{bytes}")),
        )
        .unwrap();
        let mut settings = settings(ExportProfile::Whole);
        settings.dictionary_columns.clear();
        settings.delta_columns.clear();
        let path = root.path().join(format!("output-{bytes}.parquet"));
        let error = job
            .export("SELECT 1 AS id", &path, &settings)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "capacity");
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn export_is_byte_equal_after_input_permutation_and_hashes_written_bytes() {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 32 << 20,
        max_batch_bytes: 4 << 20,
        ..Limits::default()
    };
    let job = Job::new(limits, root.path().join("scratch")).unwrap();
    let a = root.path().join("a.parquet");
    let b = root.path().join("b.parquet");
    let first = job
        .export(
            "SELECT * FROM (VALUES (3,'cc','a'),(1,'aa','a'),(2,'bb','b')) t(id,text,kind)",
            &a,
            &settings(ExportProfile::Ranged),
        )
        .await
        .unwrap();
    let second = job
        .export(
            "SELECT * FROM (VALUES (2,'bb','b'),(3,'cc','a'),(1,'aa','a')) t(id,text,kind)",
            &b,
            &settings(ExportProfile::Ranged),
        )
        .await
        .unwrap();
    let bytes = std::fs::read(&a).unwrap();
    assert_eq!(bytes, std::fs::read(&b).unwrap());
    assert_eq!(first, second);
    assert_eq!(first.rows, 3);
    assert_eq!(first.size, bytes.len() as u64);
    assert_eq!(first.sha256, format!("{:x}", Sha256::digest(&bytes)));
    assert_eq!(first.md5, format!("{:x}", md5::Md5::digest(&bytes)));
    assert!(
        job.export(
            "SELECT 1 AS id,'x' AS text,'a' AS kind",
            &a,
            &settings(ExportProfile::Ranged)
        )
        .await
        .is_err()
    );
    assert_eq!(
        bytes,
        std::fs::read(&a).unwrap(),
        "existing output must survive refusal"
    );
    let reader = SerializedFileReader::new(std::fs::File::open(&a).unwrap()).unwrap();
    let meta = reader.metadata();
    assert_eq!(meta.num_row_groups(), 2);
    assert_eq!(meta.row_group(0).num_rows(), 2);
    assert_eq!(
        meta.file_metadata().created_by(),
        Some("docspec export fixture v1")
    );
    for group in meta.row_groups() {
        for column in group.columns() {
            assert!(matches!(column.compression(), Compression::ZSTD(_)));
            assert!(column.offset_index_offset().is_some());
        }
        assert!(
            group
                .column(1)
                .encodings()
                .any(|encoding| encoding == Encoding::DELTA_BYTE_ARRAY)
        );
        assert!(
            !group
                .column(1)
                .encodings()
                .any(|encoding| encoding == Encoding::RLE_DICTIONARY)
        );
        assert!(
            group
                .column(2)
                .encodings()
                .any(|encoding| encoding == Encoding::RLE_DICTIONARY)
        );
    }
}

#[tokio::test]
async fn whole_has_no_indexes_and_lookup_has_one_group_with_key_index() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            memory_bytes: 32 << 20,
            max_batch_bytes: 4 << 20,
            ..Limits::default()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    for profile in [ExportProfile::Whole, ExportProfile::Lookup] {
        let path = root.path().join(format!("{profile:?}.parquet"));
        job.export(
            "SELECT * FROM (VALUES (3,'cc','a'),(1,'aa','a'),(2,'bb','b')) t(id,text,kind)",
            &path,
            &settings(profile),
        )
        .await
        .unwrap();
        let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
        let meta = reader.metadata();
        if profile == ExportProfile::Whole {
            assert_eq!(meta.num_row_groups(), 2);
            for group in meta.row_groups() {
                for column in group.columns() {
                    assert!(column.offset_index_offset().is_none());
                    assert!(column.column_index_offset().is_none());
                }
            }
        } else {
            assert_eq!(meta.num_row_groups(), 1);
            assert!(meta.row_group(0).column(0).column_index_offset().is_some());
            assert!(meta.row_group(0).column(1).column_index_offset().is_none());
        }
    }
}

#[tokio::test]
async fn small_footer_limit_refuses_without_publishing_a_partial_file() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
    let path = root.path().join("too-large.parquet");
    let mut configuration = settings(ExportProfile::Whole);
    configuration.footer_limit = 1;
    assert!(
        job.export(
            "SELECT 1 AS id,'x' AS text,'a' AS kind",
            &path,
            &configuration
        )
        .await
        .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn refuses_ties_across_fixed_batches_and_volatile_queries() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
    let mut configuration = settings(ExportProfile::Whole);
    configuration.row_group_rows = 65536;
    let path = root.path().join("ties.parquet");
    let error=job.export("SELECT value AS id,'x' AS text,'a' AS kind FROM generate_series(1,1025) UNION ALL SELECT 1024,'x','a'",&path,&configuration).await.unwrap_err();
    assert!(error.to_string().contains("not unique"), "{error}");
    assert!(!path.exists());
    let error = job
        .export(
            "SELECT 1 AS id,cast(random() AS VARCHAR) AS text,'a' AS kind",
            &path,
            &configuration,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("immutable"), "{error}");
    assert!(!path.exists());
}

#[tokio::test]
async fn fixed_encoder_batches_and_hidden_order_fields_preserve_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut results = Vec::new();
    for batch_rows in [64, 2048] {
        let job = Job::new(
            Limits {
                batch_rows,
                ..Limits::default()
            },
            root.path().join(format!("scratch{batch_rows}")),
        )
        .unwrap();
        let mut configuration = settings(ExportProfile::Ranged);
        configuration.columns = vec!["text".into(), "kind".into()];
        configuration.row_group_rows = 1024;
        let path = root.path().join(format!("rows{batch_rows}.parquet"));
        job.export(
            "SELECT value AS id,'same' AS text,'a' AS kind FROM generate_series(1,3073)",
            &path,
            &configuration,
        )
        .await
        .unwrap();
        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3073);
        assert_eq!(reader.metadata().num_row_groups(), 4);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .num_columns(),
            2
        );
        results.push(std::fs::read(path).unwrap());
    }
    assert_eq!(results[0], results[1]);
}

#[tokio::test]
async fn nested_leaves_have_independent_encodings() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
    let sql = "SELECT 1 AS id,make_array(named_struct('kind','a','text','plain')) AS nested";
    let frame = job.sql(sql).await.unwrap();
    let descriptor = parquet::arrow::ArrowSchemaConverter::new()
        .convert(frame.schema().as_arrow())
        .unwrap();
    let kind = descriptor
        .columns()
        .iter()
        .find(|c| c.path().parts().last().unwrap() == "kind")
        .unwrap()
        .path()
        .parts()
        .join(".");
    let text = descriptor
        .columns()
        .iter()
        .find(|c| c.path().parts().last().unwrap() == "text")
        .unwrap()
        .path()
        .parts()
        .join(".");
    let mut configuration = settings(ExportProfile::Ranged);
    configuration.dictionary_columns = BTreeSet::from([kind]);
    configuration.delta_columns = BTreeSet::from([text]);
    let path = root.path().join("nested.parquet");
    job.export(sql, &path, &configuration).await.unwrap();
    let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
    let group = reader.metadata().row_group(0);
    assert!(
        group
            .column(1)
            .encodings()
            .any(|e| e == Encoding::RLE_DICTIONARY)
    );
    assert!(
        !group
            .column(2)
            .encodings()
            .any(|e| e == Encoding::RLE_DICTIONARY)
    );
    assert!(
        group
            .column(2)
            .encodings()
            .any(|e| e == Encoding::DELTA_BYTE_ARRAY)
    );
}

#[tokio::test]
async fn output_temporary_file_obeys_shared_disk_quota() {
    let root = tempfile::tempdir().unwrap();
    let job = Job::new(
        Limits {
            scratch_bytes: 64,
            ..Limits::default()
        },
        root.path().join("scratch"),
    )
    .unwrap();
    let path = root.path().join("quota.parquet");
    assert!(
        job.export(
            "SELECT 1 AS id,'x' AS text,'a' AS kind",
            &path,
            &settings(ExportProfile::Whole)
        )
        .await
        .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn parquet_and_materialized_strings_use_the_executed_schema() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("source.parquet");
    super::scalar::input(
        &input,
        &[
            (Some(2), Some("second".into())),
            (Some(1), Some("first".into())),
        ],
    );
    let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
    job.register_parquet("items", &input).await.unwrap();
    job.materialize("cached", "SELECT id,text,'a' AS kind FROM items")
        .await
        .unwrap();
    for (name, sql) in [
        ("direct", "SELECT id,text,'a' AS kind FROM items"),
        ("cached", "SELECT * FROM cached"),
    ] {
        let path = root.path().join(format!("{name}.parquet"));
        let result = job
            .export(sql, &path, &settings(ExportProfile::Ranged))
            .await
            .unwrap();
        assert_eq!(result.rows, 2);
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader.schema().field_with_name("text").unwrap().data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        let batches = reader
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let mut json = arrow::json::ArrayWriter::new(Vec::new());
        for batch in batches {
            json.write(&batch).unwrap();
        }
        json.finish().unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&json.into_inner()).unwrap();
        assert_eq!(
            rows,
            serde_json::json!([{"id":1,"text":"first","kind":"a"},{"id":2,"text":"second","kind":"a"}])
        );
    }
}
