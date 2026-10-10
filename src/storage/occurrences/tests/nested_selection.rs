//! Selected nested roots retain child identity, values, and parent nulls.

use super::*;
use arrow::{array::StructArray, buffer::NullBuffer};
use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};

#[tokio::test]
async fn selected_nested_roots_preserve_children_repeats_and_occurrence_rows() {
    let (root, catalog, _, base) = fixture().await;
    let budget = Budget::new(Limits {
        // Keep the correspondence fixture's process ceiling. The smaller
        // inventory allowance leaves space for selected schemas and mappings.
        max_metadata_bytes: 512 << 10,
        ..budget(2).limits
    })
    .unwrap();
    let nested = Type::Struct(StructType::new(vec![
        Arc::new(
            NestedField::optional(71, "label", Type::Primitive(PrimitiveType::String))
                .with_doc("Nullable nested label"),
        ),
        Arc::new(
            NestedField::required(903, "sequence", Type::Primitive(PrimitiveType::Long))
                .with_doc("Required nested sequence"),
        ),
    ]));
    let schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(401, "details", nested)),
            Arc::new(NestedField::required(
                19,
                "scalar",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    // New-table creation assigns consecutive IDs. Register fresh, empty metadata
    // with sparse IDs. No historical files or schema-evolution claim is involved.
    assert!(base.metadata().current_snapshot().is_none());
    let location = root.path().join("nested-selection");
    let metadata = base
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(uuid::Uuid::new_v4())
        .set_location(location.to_string_lossy().into_owned())
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .remove_schemas(&[base.metadata().current_schema_id()])
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let path = location.join(format!(
        "metadata/00000-{}.metadata.json",
        uuid::Uuid::new_v4()
    ));
    storage::write_new(&path, &serde_json::to_vec(&metadata).unwrap()).unwrap();
    let empty = catalog
        .register_table(
            &iceberg::TableIdent::new(
                base.identifier().namespace().clone(),
                "nested-selection".into(),
            ),
            path.to_string_lossy().into_owned(),
        )
        .await
        .unwrap();

    // Specify Arrow fields independently of the native schema converter and
    // selected reader. Stable IDs and documentation are expected child metadata.
    let children = vec![
        Arc::new(
            Field::new("label", DataType::Utf8, true).with_metadata(HashMap::from([
                (
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                    "71".to_owned(),
                ),
                ("doc".to_owned(), "Nullable nested label".to_owned()),
            ])),
        ),
        Arc::new(
            Field::new("sequence", DataType::Int64, false).with_metadata(HashMap::from([
                (
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                    "903".to_owned(),
                ),
                ("doc".to_owned(), "Required nested sequence".to_owned()),
            ])),
        ),
    ];
    let details_field = Arc::new(
        Field::new("details", DataType::Struct(children.clone().into()), true).with_metadata(
            HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                "401".to_owned(),
            )]),
        ),
    );
    let scalar_field = Arc::new(Field::new("scalar", DataType::Int64, false).with_metadata(
        HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
            "19".to_owned(),
        )]),
    ));
    let details = Arc::new(StructArray::new(
        children.into(),
        vec![
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some(""), None])),
            Arc::new(Int64Array::from(vec![11, 22, 33, 44])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    let scalar = Arc::new(Int64Array::from(vec![100, 200, 300, 400]));
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            details_field.clone(),
            scalar_field.clone(),
        ])),
        vec![details.clone(), scalar.clone()],
    )
    .unwrap();
    let files = storage::write_batches(&empty, futures::stream::iter([Ok(input)]), &budget)
        .await
        .unwrap();
    let table = storage::append(&empty, catalog.as_ref(), files)
        .await
        .unwrap();

    for (indices, fields, struct_columns, scalar_column) in [
        (vec![0], vec![details_field.clone()], vec![0], None),
        (
            vec![1, 0, 0],
            vec![scalar_field, details_field.clone(), details_field.clone()],
            vec![1, 2],
            Some(0),
        ),
    ] {
        let expected_schema = Schema::new(fields);
        let mut stream = storage::read_projected_batches(&table, &budget, Some(&indices))
            .await
            .unwrap()
            .boxed();
        let mut offset = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
            assert!(batch.num_rows() <= 2);
            for column in &struct_columns {
                let actual = batch
                    .column(*column)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let labels = actual
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let sequences = actual
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let source_row = offset + row;
                    assert_eq!(actual.is_null(row), source_row == 1);
                    if source_row != 1 {
                        assert_eq!(
                            labels.iter().nth(row).unwrap(),
                            [Some("alpha"), None, Some(""), None][source_row]
                        );
                        assert_eq!(sequences.value(row), [11, 22, 33, 44][source_row]);
                    }
                }
            }
            if let Some(column) = scalar_column {
                let actual = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(
                    actual.values(),
                    &scalar.values()[offset..offset + batch.num_rows()]
                );
            }
            offset += batch.num_rows();
        }
        assert_eq!(offset, 4);
    }

    for indices in [vec![0], vec![]] {
        let mut stream = storage::read_projected_batches(&empty, &budget, Some(&indices))
            .await
            .unwrap()
            .boxed();
        assert!(stream.try_next().await.unwrap().is_none());
    }

    let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
    let expected_schema = Schema::new(vec![
        details_field,
        Arc::new(Field::new(OCCURRENCE_ROW, DataType::Int64, false)),
    ]);
    let mut observed_files = 0;
    for file in scan.files() {
        observed_files += 1;
        let mut stream = scan
            .read_projected_file(file.file_path(), &budget, Some(&[0, 3]))
            .unwrap()
            .boxed();
        let mut offset = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
            let actual = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let source_row = offset + row;
                assert_eq!(actual.is_null(row), source_row == 1);
                if source_row != 1 {
                    assert_eq!(
                        actual.slice(row, 1).to_data(),
                        details.slice(source_row, 1).to_data()
                    );
                }
            }
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(
                positions.values(),
                &[0, 1, 2, 3][offset..offset + batch.num_rows()]
            );
            offset += batch.num_rows();
        }
        assert_eq!(offset, 4);
    }
    assert_eq!(observed_files, 1);
}
