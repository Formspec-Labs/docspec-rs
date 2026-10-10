//! Selected native values preserve file correspondence and skip unused decoding.

use super::*;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use std::io::{Seek, SeekFrom, Write};

fn selected_budget() -> Budget {
    // The same process ceiling as the correspondence fixture, with a smaller
    // inventory allowance leaving explicit space for retained selection maps.
    Budget::new(Limits {
        max_metadata_bytes: 512 << 10,
        ..budget(2).limits
    })
    .unwrap()
}

#[tokio::test]
async fn selected_locations_keep_original_offsets_across_files_and_empty_columns() {
    let (_root, _catalog, table, _) = fixture().await;
    let budget = selected_budget();
    let scan = OccurrenceScan::open(&table, &budget).await.unwrap();
    for file in scan.files() {
        for indices in [vec![], vec![2], vec![1], vec![2, 0, 1, 2]] {
            let mut stream = scan
                .read_projected_file(file.file_path(), &budget, Some(&indices))
                .unwrap()
                .boxed();
            let mut position = 0;
            while let Some(batch) = stream.try_next().await.unwrap() {
                assert_eq!(batch.num_columns(), indices.len());
                for (column, index) in indices.iter().enumerate() {
                    match index {
                        0 => {
                            let values = batch
                                .column(column)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let expected =
                                [Some(1), Some(1), None, Some(7), Some(8), Some(9), Some(10)];
                            assert_eq!(
                                values.iter().collect::<Vec<_>>(),
                                expected[position..position + batch.num_rows()]
                            );
                        }
                        1 => {
                            let values = batch
                                .column(column)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            assert!(values.iter().all(|value| value == Some(file.file_path())));
                        }
                        2 => {
                            let values = batch
                                .column(column)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            assert_eq!(
                                values.iter().collect::<Vec<_>>(),
                                (position..position + batch.num_rows())
                                    .map(|row| Some(row as i64))
                                    .collect::<Vec<_>>()
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                position += batch.num_rows();
            }
            assert_eq!(position, 7);
        }
    }
    drop(scan);
    let context = budget.context();
    context
        .register_table(
            "source",
            crate::query::occurrences(table.clone(), budget.clone(), Some(14)).unwrap(),
        )
        .unwrap();
    let result = context
        .sql("SELECT __docspec_row FROM source WHERE id >= 8")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = result
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, [4, 4, 5, 5, 6, 6]);
    // A zero-column scan still decodes rows and enforces complete-file counts.
    let inventory = storage::snapshot_files(&table, &budget).await.unwrap();
    let mut state = Occurrences::capture_projected(&table, &budget, inventory, true, Some(&[]))
        .await
        .unwrap();
    state.next().await.unwrap().unwrap();
    state.current.as_mut().unwrap().rows += 1;
    loop {
        match state.next().await {
            Err(error) => {
                assert_eq!(error.code(), "occurrence_count");
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("zero-column read accepted an incorrect native count"),
        }
    }
}

async fn create_table(
    catalog: &dyn Catalog,
    base: &Table,
    root: &std::path::Path,
    name: &str,
    fields: Vec<Arc<NestedField>>,
) -> Table {
    catalog
        .create_table(
            base.identifier().namespace(),
            TableCreation::builder()
                .name(name.into())
                .schema(
                    iceberg::spec::Schema::builder()
                        .with_fields(fields)
                        .build()
                        .unwrap(),
                )
                .location(root.join(name).to_string_lossy().into_owned())
                .properties(storage::table_properties())
                .build(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn selected_origins_keep_pairs_fallback_and_validation() {
    let (root, catalog, base, _) = fixture().await;
    let budget = selected_budget();
    for malformed in [false, true] {
        let table = create_table(
            catalog.as_ref(),
            &base,
            root.path(),
            if malformed { "bad-origins" } else { "origins" },
            vec![
                Arc::new(NestedField::optional(
                    10,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    70,
                    storage::identity::ORIGIN_FILE,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    90,
                    storage::identity::ORIGIN_ROW,
                    Type::Primitive(PrimitiveType::Long),
                )),
            ],
        )
        .await;
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("birth-file"), None])),
                Arc::new(Int64Array::from(vec![Some(42), malformed.then_some(43)])),
            ],
        )
        .unwrap();
        // Write a physical fixture directly: the DocSpec writer correctly fills
        // absent origins before storage, which would hide the reader fallback.
        let path = root.path().join(format!("raw-origins-{malformed}.parquet"));
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = iceberg::spec::DataFileBuilder::default()
            .content(iceberg::spec::DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path(path.to_string_lossy().into_owned())
            .file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .record_count(2)
            .build()
            .unwrap();
        let files = vec![file];
        let path = path.to_string_lossy().into_owned();
        let table = storage::append(&table, catalog.as_ref(), files)
            .await
            .unwrap();
        let inventory = storage::snapshot_files(&table, &budget).await.unwrap();
        let values = OccurrenceScan::values(&table, &budget, inventory)
            .await
            .unwrap();
        let mut selected = values
            .read_projected_file(&path, &budget, Some(&[2, 2]))
            .unwrap()
            .boxed();
        if malformed {
            assert_eq!(
                selected.try_next().await.unwrap_err().code(),
                "invalid_occurrence_identity"
            );
        } else {
            let batch = selected.try_next().await.unwrap().unwrap();
            for column in batch.columns() {
                assert_eq!(
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    &[42, 1]
                );
            }
            assert!(selected.try_next().await.unwrap().is_none());
        }
        drop(selected);
        drop(values);
        let mut selected = read_projected_occurrences(&table, &budget, Some(&[4, 3, 4]))
            .await
            .unwrap()
            .boxed();
        if malformed {
            assert_eq!(
                selected.try_next().await.unwrap_err().code(),
                "invalid_occurrence_identity"
            );
        } else {
            let batch = selected.try_next().await.unwrap().unwrap();
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[42, 1]
            );
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                [Some("birth-file"), Some(path.as_str())]
            );
        }
    }
}

#[tokio::test]
async fn sql_native_and_captured_reads_skip_a_damaged_unselected_page() {
    let (root, catalog, base, _) = fixture().await;
    let budget = selected_budget();
    let table = create_table(
        catalog.as_ref(),
        &base,
        root.path(),
        "wide",
        vec![
            Arc::new(NestedField::optional(
                10,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                90,
                "payload",
                Type::Primitive(PrimitiveType::String),
            )),
        ],
    )
    .await;
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![
                "x".repeat(8_000),
                "y".repeat(8_000),
            ])),
        ],
    )
    .unwrap();
    let files = storage::write_batches(&table, futures::stream::iter([Ok(batch)]), &budget)
        .await
        .unwrap();
    let path = files[0].file_path().to_owned();
    let table = storage::append(&table, catalog.as_ref(), files)
        .await
        .unwrap();
    let parquet = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let offset = parquet.metadata().row_group(0).column(1).data_page_offset();
    drop(parquet);
    let mut corrupt = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    corrupt.seek(SeekFrom::Start(offset as u64)).unwrap();
    corrupt.write_all(&[255; 8]).unwrap();
    corrupt.sync_all().unwrap();
    drop(corrupt);
    for located in [false, true] {
        for captured in [false, true] {
            let provider = if captured {
                let scan = if located {
                    OccurrenceScan::open(&table, &budget).await.unwrap()
                } else {
                    let inventory = storage::snapshot_files(&table, &budget).await.unwrap();
                    OccurrenceScan::values(&table, &budget, inventory)
                        .await
                        .unwrap()
                };
                crate::query::file(Arc::new(scan), path.clone(), None, budget.clone()).unwrap()
            } else if located {
                crate::query::occurrences(table.clone(), budget.clone(), Some(2)).unwrap()
            } else {
                crate::query::table(table.clone(), budget.clone(), Some(2)).unwrap()
            };
            let context = budget.context();
            context.register_table("source", provider).unwrap();
            let result = context
                .sql("SELECT id FROM source")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let values = result
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, [1, 2], "located={located} captured={captured}");
            assert!(
                context
                    .sql("SELECT payload FROM source")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .is_err()
            );
        }
    }
}
