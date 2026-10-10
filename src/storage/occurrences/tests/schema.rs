//! Maintained field-ID read and metadata-fork qualification before public evolution.

use super::*;
use iceberg::spec::{NestedField, PrimitiveType, Type};

async fn current_schema_batches(table: &Table, budget: &Budget) -> Vec<RecordBatch> {
    let full: Vec<RecordBatch> = storage::read_batches(table, budget)
        .await
        .unwrap()
        .boxed()
        .try_collect()
        .await
        .unwrap();
    let width = table.metadata().current_schema().as_struct().fields().len();
    let mut projections = vec![vec![], (0..width).rev().chain(0..width.min(1)).collect()];
    projections.extend((0..width).map(|index| vec![index]));
    for indices in projections {
        let selected = storage::read_projected_batches(table, budget, Some(&indices))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        // Existing assertions independently check the full values. Differential
        // reads also exercise sparse IDs, defaults and user _file.
        let expected = full
            .iter()
            .map(|batch| batch.project(&indices).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(selected, expected, "native columns {indices:?}");
    }
    full
}

#[tokio::test]
async fn native_current_schema_projection_preserves_field_ids_nullable_additions_and_historical_files()
 {
    let (root, catalog, original, _) = fixture().await;
    let budget = budget(2);
    let original_schema = original.metadata().current_schema();
    let mut renamed = original_schema.as_struct().fields()[0].as_ref().clone();
    renamed.name = "renamed".into();
    let schema = iceberg::spec::Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                3,
                "added",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(renamed),
            Arc::new(NestedField::optional(
                2,
                "optional",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();
    let location = root.path().join("evolved");
    let metadata = original
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(location.to_string_lossy().into_owned())
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let path = location.join(format!(
        "metadata/00000-{}.metadata.json",
        uuid::Uuid::new_v4()
    ));
    storage::write_new(&path, &serde_json::to_vec(&metadata).unwrap()).unwrap();
    let evolved = catalog
        .register_table(
            &iceberg::TableIdent::new(original.identifier().namespace().clone(), "evolved".into()),
            path.to_string_lossy().into_owned(),
        )
        .await
        .unwrap();
    assert_eq!(
        evolved
            .metadata()
            .current_schema()
            .field_by_name("renamed")
            .unwrap()
            .id,
        original_schema.field_by_name("id").unwrap().id
    );
    assert_ne!(evolved.metadata().uuid(), original.metadata().uuid());
    assert_eq!(
        evolved.metadata().current_snapshot_id(),
        original.metadata().current_snapshot_id()
    );
    assert_eq!(evolved.metadata().last_column_id(), 3);
    let inventory = storage::snapshot_files(&original, &budget).await.unwrap();
    let objects = inventory
        .data
        .iter()
        .map(|file| {
            (
                file.file_path().to_owned(),
                storage::digest(std::path::Path::new(file.file_path())).unwrap(),
                std::fs::metadata(file.file_path())
                    .unwrap()
                    .modified()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    drop(inventory);
    let batches = current_schema_batches(&evolved, &budget).await;
    let mut observed = Vec::new();
    for batch in &batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["added", "renamed", "optional"]
        );
        let defaults = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch.column(2).null_count(), batch.num_rows());
        for row in 0..batch.num_rows() {
            assert!(defaults.is_null(row));
            observed.push((!values.is_null(row)).then(|| values.value(row)));
        }
    }
    observed.sort();
    let mut expected =
        vec![vec![Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)]; 2].concat();
    expected.sort();
    assert_eq!(observed, expected);
    // Newly written explicit null, zero and empty values remain distinct.
    let arrow_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(evolved.metadata().current_schema()).unwrap(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![None, Some(0)])),
            Arc::new(Int64Array::from(vec![Some(100), Some(101)])),
            Arc::new(StringArray::from(vec![None, Some("")])),
        ],
    )
    .unwrap();
    let files = storage::write_batches(&evolved, futures::stream::iter([Ok(batch)]), &budget)
        .await
        .unwrap();
    let newer = storage::append(&evolved, catalog.as_ref(), files)
        .await
        .unwrap();
    let batches = current_schema_batches(&newer, &budget).await;
    let mut new_rows = batches.iter().flat_map(mixed_values).collect::<Vec<_>>();
    new_rows.sort();
    let mut expected = expected
        .into_iter()
        .map(|value| (value, None, None))
        .collect::<Vec<_>>();
    expected.extend([
        (Some(100), None, None),
        (Some(101), Some(0), Some(String::new())),
    ]);
    expected.sort();
    assert_eq!(new_rows, expected);
    let mut located = storage::read_occurrences(&newer, &budget)
        .await
        .unwrap()
        .boxed();
    let mut located_values = Vec::new();
    let mut locations = std::collections::BTreeSet::new();
    while let Some(batch) = located.try_next().await.unwrap() {
        located_values.extend(mixed_values(&batch));
        let files = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            assert!(locations.insert((files.value(index).to_owned(), rows.value(index))));
        }
    }
    located_values.sort();
    assert_eq!(located_values, expected);
    assert_eq!(locations.len(), expected.len());
    for (path, digest, modified) in objects {
        assert_eq!(
            storage::digest(std::path::Path::new(&path)).unwrap(),
            digest
        );
        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            modified
        );
    }
    let mut original_rows = 0;
    let mut stream = storage::read_batches(&original, &budget)
        .await
        .unwrap()
        .boxed();
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert_eq!(batch.schema().field(0).name(), "id");
        original_rows += batch.num_rows();
    }
    assert_eq!(original_rows, 14);
}

#[tokio::test]
async fn native_current_schema_projection_reorders_equal_types_by_id() {
    let root = TempDir::new().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let namespace = NamespaceIdent::new("schema_order".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("source".into())
                .schema(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&schema).unwrap())
                .location(root.path().join("source").to_string_lossy().into_owned())
                .properties(storage::table_properties())
                .build(),
        )
        .await
        .unwrap();
    let budget = budget(2);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(11), None])),
            Arc::new(Int64Array::from(vec![Some(22), Some(33)])),
        ],
    )
    .unwrap();
    let files = storage::write_batches(&table, futures::stream::iter([Ok(batch)]), &budget)
        .await
        .unwrap();
    let table = storage::append(&table, catalog.as_ref(), files)
        .await
        .unwrap();
    let fields = table.metadata().current_schema().as_struct().fields();
    let schema = iceberg::spec::Schema::builder()
        .with_fields([fields[1].clone(), fields[0].clone()])
        .build()
        .unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let evolved = Table::builder()
        .identifier(table.identifier().clone())
        .metadata(metadata)
        .file_io(table.file_io().clone())
        .runtime(iceberg::Runtime::try_current().unwrap())
        .build()
        .unwrap();
    let context = budget.context();
    context
        .register_table(
            "evolved",
            crate::query::table(evolved.clone(), budget.clone(), None).unwrap(),
        )
        .unwrap();
    let query = context
        .sql("SELECT b, a FROM evolved")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = current_schema_batches(&evolved, &budget).await;
    assert_eq!(query, batches);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "b");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![Some(22), Some(33)])
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![Some(11), None])
    );
    // With two physical columns and one new value plus the virtual column,
    // field counts coincide. The virtual type must still force ID mapping.
    let absent = iceberg::spec::Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            9,
            "a",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(absent)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let absent = iceberg::table::StaticTable::from_metadata(
        metadata,
        table.identifier().clone(),
        table.file_io().clone(),
    )
    .await
    .unwrap()
    .into_table();
    let batches = current_schema_batches(&absent, &budget).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for batch in batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).null_count(), batch.num_rows());
    }
}

#[tokio::test]
async fn new_id_with_reused_name_never_reads_old_values() {
    let (root, _catalog, original, _) = fixture().await;
    let schema = iceberg::spec::Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            9,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let metadata = original
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let metadata_path = root.path().join("reused-name.metadata.json");
    storage::write_new(&metadata_path, &serde_json::to_vec(&metadata).unwrap()).unwrap();
    let evolved = iceberg::table::StaticTable::from_metadata_file(
        metadata_path.to_str().unwrap(),
        original.identifier().clone(),
        original.file_io().clone(),
    )
    .await
    .unwrap()
    .into_table();
    let budget = budget(3);
    for batch in current_schema_batches(&evolved, &budget).await {
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(
            batch.schema().field(0).metadata()[parquet::arrow::PARQUET_FIELD_ID_META_KEY],
            "9"
        );
        assert_eq!(batch.column(0).null_count(), batch.num_rows());
    }
    let expected: Rows = {
        let mut rows = Rows::new();
        let mut stream = storage::read_occurrences(&original, &budget)
            .await
            .unwrap()
            .boxed();
        while let Some(batch) = stream.try_next().await.unwrap() {
            insert(&batch, &mut rows);
        }
        rows.into_keys().map(|key| (key, None)).collect()
    };
    let mut rows = Rows::new();
    let mut stream = storage::read_occurrences(&evolved, &budget)
        .await
        .unwrap()
        .boxed();
    while let Some(batch) = stream.try_next().await.unwrap() {
        insert(&batch, &mut rows);
    }
    assert_eq!(rows, expected);
    let scan = storage::OccurrenceScan::open(&evolved, &budget)
        .await
        .unwrap();
    let mut rows = Rows::new();
    for file in scan.files() {
        let mut stream = scan.read_file(file.file_path(), &budget).unwrap().boxed();
        while let Some(batch) = stream.try_next().await.unwrap() {
            insert(&batch, &mut rows);
        }
    }
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn unqualified_schema_interpretations_refuse_before_reading_payload() {
    let (_root, _catalog, original, _) = fixture().await;
    for field in [
        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::String)),
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(2, "added", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(iceberg::spec::Literal::long(7)),
        NestedField::optional(
            iceberg::metadata_columns::RESERVED_FIELD_ID_FILE,
            "collision",
            Type::Primitive(PrimitiveType::String),
        ),
    ] {
        let reserved = field.id == iceberg::metadata_columns::RESERVED_FIELD_ID_FILE;
        let schema = iceberg::spec::Schema::builder()
            .with_fields([Arc::new(field)])
            .build()
            .unwrap();
        let metadata = original
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = iceberg::table::StaticTable::from_metadata(
            metadata,
            original.identifier().clone(),
            original.file_io().clone(),
        )
        .await
        .unwrap()
        .into_table();
        let expected = if reserved {
            "reserved_field"
        } else {
            "unsupported_schema_evolution"
        };
        assert_eq!(
            storage::read_batches(&table, &budget(2))
                .await
                .err()
                .unwrap()
                .code(),
            expected
        );
        assert_eq!(
            storage::occurrence_schema(&table).unwrap_err().code(),
            expected
        );
    }
}

#[tokio::test]
async fn transient_native_column_is_validated_and_charged_before_removal() {
    let (_root, _catalog, original, _) = fixture().await;
    let tasks = storage::schema::plan_files(&original).await.unwrap();
    let mut batches = original
        .reader_builder()
        .with_batch_size(2)
        .with_data_file_concurrency_limit(1)
        .build()
        .read(tasks)
        .unwrap()
        .stream();
    let augmented = batches.try_next().await.unwrap().unwrap();
    let bytes = augmented.get_array_memory_size();
    let stripped = storage::schema::strip_file_column(augmented.clone(), bytes).unwrap();
    assert!(stripped.get_array_memory_size() < bytes);
    assert_eq!(
        storage::schema::strip_file_column(augmented, stripped.get_array_memory_size())
            .unwrap_err()
            .code(),
        "oversized_batch"
    );
    assert_eq!(
        storage::schema::strip_file_column(stripped.clone(), bytes)
            .unwrap_err()
            .code(),
        "native_schema"
    );
    let forged = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("forged", DataType::Int64, true).with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                iceberg::metadata_columns::RESERVED_FIELD_ID_FILE.to_string(),
            )])),
        ])),
        stripped.columns().to_vec(),
    )
    .unwrap();
    assert_eq!(
        storage::schema::strip_file_column(forged, bytes)
            .unwrap_err()
            .code(),
        "native_schema"
    );
}

fn mixed_values(batch: &RecordBatch) -> Vec<(Option<i64>, Option<i64>, Option<String>)> {
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let added = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let optional = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                (!values.is_null(row)).then(|| values.value(row)),
                (!added.is_null(row)).then(|| added.value(row)),
                (!optional.is_null(row)).then(|| optional.value(row).to_owned()),
            )
        })
        .collect()
}

#[tokio::test]
async fn all_fields_hidden_and_a_user_file_name_preserve_values_and_occurrences() {
    let (root, _catalog, original, _) = fixture().await;
    for hidden in [false, true] {
        let fields = if hidden {
            vec![]
        } else {
            vec![Arc::new(NestedField::optional(
                1,
                "_file",
                Type::Primitive(PrimitiveType::Long),
            ))]
        };
        let schema = iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .unwrap();
        let metadata = original
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let path = root.path().join(format!("hidden-{hidden}.metadata.json"));
        storage::write_new(&path, &serde_json::to_vec(&metadata).unwrap()).unwrap();
        let table = iceberg::table::StaticTable::from_metadata_file(
            path.to_str().unwrap(),
            original.identifier().clone(),
            original.file_io().clone(),
        )
        .await
        .unwrap()
        .into_table();
        let budget = budget(2);
        let batches = current_schema_batches(&table, &budget).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 14);
        let mut values = Vec::new();
        for batch in batches {
            assert_eq!(batch.num_columns(), usize::from(!hidden));
            if !hidden {
                assert_eq!(batch.schema().field(0).name(), "_file");
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter(),
                );
            }
        }
        if !hidden {
            values.sort();
            let mut expected =
                vec![vec![Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)]; 2].concat();
            expected.sort();
            assert_eq!(values, expected);
        }
        let mut stream = storage::read_occurrences(&table, &budget)
            .await
            .unwrap()
            .boxed();
        let mut locations = std::collections::BTreeSet::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let offset = usize::from(!hidden);
            let files = batch
                .column(offset)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let rows = batch
                .column(offset + 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(locations.insert((files.value(row).to_owned(), rows.value(row))));
            }
        }
        assert_eq!(locations.len(), 14);
    }
}
