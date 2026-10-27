//! Native schema metadata must align without coercing incompatible values.
// Rust guideline compliant 2026-02-21
use arrow::{
    datatypes::{DataType, Field, Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use docspec::{
    budget::{Budget, Limits},
    job::{Job, JsonLinesFile},
};
use flate2::{Compression, write::GzEncoder};
use iceberg::{
    NamespaceIdent, TableCreation,
    spec::{ListType, NestedField, PrimitiveType, Schema, StructType, Type},
    table::Table,
};
use std::{io::Write, sync::Arc};
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        max_batch_bytes: 2 << 20,
        max_metadata_bytes: 2 << 20,
        batch_rows: 8,
        scratch_bytes: 8 << 20,
    }
}
async fn fixture(
    root: &std::path::Path,
    text: &str,
    renamed: bool,
) -> (Table, Vec<RecordBatch>, Budget) {
    let catalog = docspec::storage::local_catalog(&root.join("catalog"))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("nested".into());
    catalog
        .create_namespace(&namespace, Default::default())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "kinds",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    3,
                    "element",
                    Type::Primitive(PrimitiveType::String),
                )))),
            )),
            Arc::new(NestedField::optional(
                4,
                "fields",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    5,
                    "element",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            6,
                            "field",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            7,
                            "value",
                            Type::Primitive(PrimitiveType::String),
                        )),
                    ])),
                )))),
            )),
        ])
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("items".into())
                .schema(schema)
                .location(root.join("items").to_string_lossy().into_owned())
                .properties([("gc.enabled".into(), "true".into())])
                .build(),
        )
        .await
        .unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "kinds",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "fields",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new(
                        if renamed { "other" } else { "field" },
                        DataType::Utf8,
                        true,
                    ),
                    Field::new("value", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
    ]));
    let path = root.join("input.gz");
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(text.as_bytes()).unwrap();
    gzip.finish().unwrap();
    let job = Job::new(limits(), root.join("scratch")).unwrap();
    job.register_json_lines(
        "input",
        vec![JsonLinesFile {
            key: "fixture".into(),
            path,
        }],
        schema,
    )
    .unwrap();
    let batches = job
        .sql("SELECT id,kinds,fields FROM input")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    (table, batches, Budget::new(limits()).unwrap())
}
async fn write(
    table: &Table,
    batches: Vec<RecordBatch>,
    budget: &Budget,
) -> docspec::Result<Vec<iceberg::spec::DataFile>> {
    docspec::storage::write_batches_with_layout(
        table,
        futures::stream::iter(batches.into_iter().map(Ok)),
        budget,
        docspec::storage::WriteLayout {
            file_bytes: 1 << 20,
            row_group_rows: 8,
            row_group_bytes: 1 << 20,
        },
    )
    .await
}
#[tokio::test]
async fn nullable_list_and_struct_readings_align_to_required_native_children() {
    let root = tempfile::tempdir().unwrap();
    let (table, batches, budget) = fixture(
        root.path(),
        "{\"id\":1,\"kinds\":[\"bill\"],\"fields\":[{\"field\":\"title\",\"value\":\"A\"}]}\n",
        false,
    )
    .await;
    let files = write(&table, batches, &budget).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].record_count(), 1);
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(files[0].file_path()).unwrap(),
    )
    .unwrap();
    let expected =
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap();
    for name in ["kinds", "fields"] {
        assert_eq!(
            reader.schema().field_with_name(name).unwrap(),
            expected.field_with_name(name).unwrap()
        );
    }
}
#[tokio::test]
async fn actual_required_child_nulls_and_renamed_struct_fields_refuse() {
    for (text, renamed) in [
        ("{\"id\":1,\"kinds\":[null],\"fields\":[]}\n", false),
        (
            "{\"id\":1,\"kinds\":[],\"fields\":[{\"field\":null,\"value\":\"A\"}]}\n",
            false,
        ),
        (
            "{\"id\":1,\"kinds\":[],\"fields\":[{\"other\":\"title\",\"value\":\"A\"}]}\n",
            true,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (table, batches, budget) = fixture(root.path(), text, renamed).await;
        assert!(write(&table, batches, &budget).await.is_err(), "{text}");
    }
}
#[tokio::test]
async fn sliced_unused_null_children_do_not_refuse_valid_rows() {
    let root = tempfile::tempdir().unwrap();
    let(table,batches,budget)=fixture(root.path(),"{\"id\":1,\"kinds\":[null],\"fields\":[{\"field\":null}]}\n{\"id\":2,\"kinds\":[\"bill\"],\"fields\":[{\"field\":\"title\"}]}\n",false).await;
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let sliced = batch.slice(1, 1);
    let files = write(&table, vec![sliced], &budget).await.unwrap();
    assert_eq!(files.iter().map(|file| file.record_count()).sum::<u64>(), 1);
}
