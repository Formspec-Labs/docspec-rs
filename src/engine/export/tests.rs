//! Unit and integration tests for export schema authorization, row-group bounds, and pin identity.

use super::*;
use crate::export::ExportRows;
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::{
    ArrowWriter, PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder,
};
use std::{collections::HashMap, fs::File, sync::Arc};

#[test]
fn schema_authorization_removes_metadata_allows_nested_and_refuses_internal_or_unsupported_columns()
{
    let field = |name, datatype, id: &str| {
        Field::new(name, datatype, true).with_metadata(HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.into(), id.into()),
            ("doc".into(), "private-child-doc".into()),
            (
                "source_documentation".into(),
                "undisclosed-metadata-canary".into(),
            ),
        ]))
    };
    let map_entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int32, true),
            ]
            .into(),
        ),
        false,
    );
    let source = Arc::new(Schema::new_with_metadata(
        vec![
            field("public", DataType::Utf8, "7"),
            field("secret", DataType::Utf8, "9"),
            field("__docspec_left_file", DataType::Utf8, "11"),
            field(
                "nested_list",
                DataType::List(Arc::new(field("item", DataType::Utf8, "14"))),
                "13",
            ),
            field(
                "nested_struct",
                DataType::Struct(
                    vec![
                        field("start", DataType::Int32, "16"),
                        field("end", DataType::Int64, "18"),
                    ]
                    .into(),
                ),
                "15",
            ),
            field(
                "unsupported",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "17",
            ),
            field(
                "nested_internal",
                DataType::Struct(vec![field("__docspec_row", DataType::Int64, "20")].into()),
                "19",
            ),
            field(
                "nested_unsupported_struct",
                DataType::Struct(
                    vec![field(
                        "bad_dict",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        "22",
                    )]
                    .into(),
                ),
                "21",
            ),
            field(
                "nested_unsupported_list",
                DataType::List(Arc::new(field(
                    "item",
                    DataType::Map(Arc::new(map_entries), false),
                    "24",
                ))),
                "23",
            ),
            field(
                "nested_empty_struct",
                DataType::Struct(Vec::<Field>::new().into()),
                "25",
            ),
            field(
                "nested_missing_id",
                DataType::Struct(vec![Field::new("untagged", DataType::Int32, true)].into()),
                "27",
            ),
            field(
                "nested_duplicate_id",
                DataType::Struct(vec![field("dup", DataType::Int32, "29")].into()),
                "29",
            ),
        ],
        HashMap::from([("source_path".into(), "private-evidence-path".into())]),
    ));
    let mut policy = ExportPolicy {
        group: "source".into(),
        rows: ExportRows::All,
        columns: vec![
            "public".into(),
            "nested_list".into(),
            "nested_struct".into(),
        ],
        ..ExportPolicy::default()
    };
    let (indices, selected, fields) = authorized_schema(&source, &policy).unwrap();
    assert_eq!(indices, [0, 3, 4]);
    assert_eq!(count_primitive_leaves(&selected), 4);
    assert!(selected.metadata().is_empty());
    assert_eq!(
        selected.field(0).metadata(),
        &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "7".into())])
    );
    let DataType::List(list_item) = selected.field(1).data_type() else {
        panic!("expected list data type");
    };
    assert_eq!(
        list_item.metadata(),
        &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "14".into())])
    );
    let DataType::Struct(struct_fields) = selected.field(2).data_type() else {
        panic!("expected struct data type");
    };
    assert_eq!(
        struct_fields[0].metadata(),
        &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "16".into())])
    );
    assert_eq!(
        struct_fields[1].metadata(),
        &HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "18".into())])
    );
    for exported in &fields {
        assert!(!exported.data_type.contains("undisclosed-metadata-canary"));
        assert!(!exported.data_type.contains("private-child-doc"));
    }
    assert_eq!(fields[0].id, 7);
    assert_eq!(fields[1].id, 13);
    assert_eq!(fields[2].id, 15);
    for internal in ["__docspec_left_file", "nested_internal"] {
        policy.columns = vec![internal.into()];
        assert_eq!(
            authorized_schema(&source, &policy).unwrap_err().code(),
            "export_disclosure"
        );
    }
    for bad_type in [
        "unsupported",
        "nested_unsupported_struct",
        "nested_unsupported_list",
        "nested_empty_struct",
    ] {
        policy.columns = vec![bad_type.into()];
        assert_eq!(
            authorized_schema(&source, &policy).unwrap_err().code(),
            "unsupported_export_type"
        );
    }
    for bad_id in ["nested_missing_id", "nested_duplicate_id"] {
        policy.columns = vec![bad_id.into()];
        assert_eq!(
            authorized_schema(&source, &policy).unwrap_err().code(),
            "invalid_export"
        );
    }
}

#[tokio::test]
async fn nested_list_and_struct_columns_export_and_read_back() {
    use arrow::{
        array::{Int32Array, Int32Builder, ListBuilder, StringBuilder, StructArray, StructBuilder},
        buffer::NullBuffer,
    };
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("nested.parquet");
    let child_meta = |doc: &str| {
        HashMap::from([
            ("doc".into(), doc.into()),
            (
                "custom_secret".into(),
                "private-descendant-custom-canary".into(),
            ),
        ])
    };
    let span_fields: arrow::datatypes::Fields = vec![
        Field::new("start", DataType::Int32, false)
            .with_metadata(child_meta("private-descendant-doc-canary-start")),
        Field::new("end", DataType::Int32, true)
            .with_metadata(child_meta("private-descendant-doc-canary-end")),
    ]
    .into();
    let mut aliases_builder = ListBuilder::new(StringBuilder::new()).with_field(
        Field::new("item", DataType::Utf8, true).with_metadata(HashMap::from([(
            "custom_secret".into(),
            "private-descendant-custom-canary".into(),
        )])),
    );
    aliases_builder.values().append_value("https://a.example");
    aliases_builder.values().append_null();
    aliases_builder.values().append_value("https://b.example");
    aliases_builder.append(true);
    aliases_builder.append(false);
    aliases_builder.append(true);
    let aliases = Arc::new(aliases_builder.finish());

    let mut spans_builder = ListBuilder::new(StructBuilder::new(
        span_fields.clone(),
        vec![Box::new(Int32Builder::new()), Box::new(Int32Builder::new())],
    ))
    .with_field(
        Field::new("item", DataType::Struct(span_fields.clone()), true).with_metadata(
            HashMap::from([(
                "custom_secret".into(),
                "private-descendant-custom-canary".into(),
            )]),
        ),
    );
    spans_builder
        .values()
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(1);
    spans_builder
        .values()
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(4);
    spans_builder.values().append(true);
    spans_builder
        .values()
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(5);
    spans_builder
        .values()
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_null();
    spans_builder.values().append(true);
    spans_builder.append(true);
    spans_builder.append(true);
    spans_builder.append(false);
    let spans = Arc::new(spans_builder.finish());

    let details = Arc::new(StructArray::new(
        span_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 10, 20])),
            Arc::new(Int32Array::from(vec![Some(5), None, Some(25)])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("url_aliases", aliases.data_type().clone(), true),
        Field::new("spans", spans.data_type().clone(), true),
        Field::new("span", DataType::Struct(span_fields), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
            aliases.clone(),
            spans.clone(),
            details.clone(),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let id = |name| Id::new(name).unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let source = engine
        .run(&RunSpec {
            dataset: id("docs"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let spec = ExportSpec {
        dataset: id("docs"),
        operation: id("nested-export"),
        source: source.version_ref(),
        channel: id("download"),
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec![
                "id".into(),
                "url_aliases".into(),
                "spans".into(),
                "span".into(),
            ],
            ..ExportPolicy::default()
        },
    };
    let pin = engine.export(&spec).await.unwrap();
    assert_eq!(pin.rows, 3);
    assert_eq!(pin.fields.len(), 4);
    for field in &pin.fields {
        assert!(!field.data_type.contains("private-descendant-doc-canary"));
        assert!(!field.data_type.contains("private-descendant-custom-canary"));
    }
    let raw_bytes = std::fs::read(&pin.object.path).unwrap();
    assert!(
        !raw_bytes
            .windows(b"private-descendant".len())
            .any(|window| window == b"private-descendant")
    );
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap()).unwrap();
    assert!(builder.schema().metadata().is_empty());
    let read = engine.read_export(&pin).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(read)
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let exported = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let ids = exported
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for row in 0..exported.num_rows() {
        let source_row = usize::try_from(ids.value(row) - 1).unwrap();
        for col in 0..4 {
            let expected =
                sanitize_export_array(batch.column(col), exported.schema().field(col).data_type())
                    .unwrap();
            assert_eq!(
                exported.column(col).slice(row, 1).to_data(),
                expected.slice(source_row, 1).to_data()
            );
        }
    }
}

#[tokio::test]
async fn wide_nested_struct_exceeding_metadata_budget_refuses_export() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("wide-nested.parquet");
    let leaf_fields: arrow::datatypes::Fields = (0..40)
        .map(|index| Field::new(format!("f{index}"), DataType::Int32, false))
        .collect::<Vec<_>>()
        .into();
    let leaf_arrays = (0..40)
        .map(|index| {
            Arc::new(arrow::array::Int32Array::from(vec![index])) as arrow::array::ArrayRef
        })
        .collect::<Vec<_>>();
    let wide_struct = Arc::new(arrow::array::StructArray::new(
        leaf_fields.clone(),
        leaf_arrays,
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("wide", DataType::Struct(leaf_fields), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1_i64])),
            wide_struct,
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let id = |name| Id::new(name).unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("wide"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    drop(engine);

    // 41 primitive leaves (id + 40 struct children) require >42 KiB of
    // per-row-group footer admission, whereas root-column counting would
    // estimate <6 KiB and mistakenly admit under a 32 KiB metadata cap.
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            max_metadata_bytes: 32 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let spec = ExportSpec {
        dataset: id("wide"),
        operation: id("wide-export"),
        source: release.version_ref(),
        channel: id("download"),
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["id".into(), "wide".into()],
            ..ExportPolicy::default()
        },
    };
    let err = engine.export(&spec).await.unwrap_err();
    assert_eq!(err.code(), "metadata_capacity");
}

#[tokio::test]
async fn caller_limits_bound_row_groups_and_preserve_global_dictionary_encoding() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("limits-input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("custom_category", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let row_count = 160_i64;
    let ids = (0..row_count).collect::<Vec<_>>();
    let categories = (0..row_count)
        .map(|index| if index % 2 == 0 { "alpha" } else { "beta" })
        .collect::<Vec<_>>();
    let payloads = (0..row_count)
        .map(|index| format!("payload-{index:04}-{}", "x".repeat(64)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(ids)),
            Arc::new(arrow::array::StringArray::from(categories)),
            Arc::new(arrow::array::StringArray::from(payloads)),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let id = |name| Id::new(name).unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            batch_rows: 64,
            max_batch_bytes: 64 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("bounded"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let native_parquet = release.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let native_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&native_parquet.path).unwrap())
            .unwrap();
    assert!(native_reader.metadata().num_row_groups() >= 3);
    for group in native_reader.metadata().row_groups() {
        assert!(group.num_rows() <= 64);
        let category_col = group
            .columns()
            .iter()
            .find(|col| col.column_path().string() == "custom_category")
            .unwrap();
        assert!(category_col.dictionary_page_offset().is_some());
    }
    drop(engine);

    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            batch_rows: 8,
            max_batch_bytes: 16 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();

    let pin = engine
        .export(&ExportSpec {
            dataset: id("bounded"),
            operation: id("export-bounded"),
            source: release.version_ref(),
            channel: id("download"),
            expected: None,
            policy: ExportPolicy {
                group: "source".into(),
                rows: ExportRows::All,
                columns: vec!["id".into(), "custom_category".into(), "payload".into()],
                ..ExportPolicy::default()
            },
        })
        .await
        .unwrap();
    let export_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&pin.object.path).unwrap()).unwrap();
    assert!(export_reader.metadata().num_row_groups() >= 2);
    for group in export_reader.metadata().row_groups() {
        let category_col = group
            .columns()
            .iter()
            .find(|col| col.column_path().string() == "custom_category")
            .unwrap();
        assert!(category_col.dictionary_page_offset().is_some());
    }
}

#[tokio::test]
async fn retained_lookup_binds_requested_identity_and_output_is_protected() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![
            Some(1),
            None,
        ]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let id = |name| Id::new(name).unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            // This two-row identity fixture does not need the production scratch budget.
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let source = engine
        .run(&RunSpec {
            dataset: id("data"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let spec = ExportSpec {
        dataset: id("data"),
        operation: id("public"),
        source: source.version_ref(),
        channel: id("download"),
        expected: None,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["value".into()],
            ..ExportPolicy::default()
        },
    };
    let pin = engine.export(&spec).await.unwrap();
    // Both transport entrypoints resolve the same immutable retained export.
    let read = engine
        .read_export_id(&spec.dataset, &spec.operation)
        .await
        .unwrap();
    let batches = datafusion::physical_plan::common::collect(read)
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(1), None]);
    let mut wrong_pin = pin.clone();
    wrong_pin.object.digest = "0".repeat(64);
    assert_eq!(
        engine.read_export(&wrong_pin).await.err().unwrap().code(),
        "invalid_export"
    );
    let read = engine.read_export(&pin).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(read)
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let inspection = engine
        .inspect_retention(std::slice::from_ref(&pin.object))
        .await
        .unwrap();
    assert!(inspection.objects[0].blocker.is_some());
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record("export-result/wrong", &pin).unwrap(),
                record("export-channel/wrong", &pin).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .export_pin(&spec.dataset, &id("wrong"))
            .await
            .unwrap_err()
            .code(),
        "invalid_export"
    );
    assert_eq!(
        engine
            .latest_export(&spec.dataset, &id("wrong"))
            .await
            .unwrap_err()
            .code(),
        "invalid_export"
    );
    assert_eq!(
        engine.export_pin(&spec.dataset, &pin.id).await.unwrap(),
        pin
    );
    #[cfg(feature = "remote")]
    remote::test_retained_phase_revision_bindings(&engine, &spec, &pin).await;
}
