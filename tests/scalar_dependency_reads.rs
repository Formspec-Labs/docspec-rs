//! Scalar dependencies preserve values and occurrence identities under narrow reads.

use super::{compaction, scalar};
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Budget,
    engine::verify_release,
    model::{
        ColumnAddition, ColumnType, ComputedField, ReleasePin, RunSpec, SchemaField, SchemaSpec,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeMap, fs::File, sync::Arc};

async fn values(release: &ReleasePin, group: &str) -> BTreeMap<(String, i64), Vec<Option<i64>>> {
    let budget = Budget::new(scalar::limits()).unwrap();
    let native = storage::open_pin(&release.dataset.groups[group], &budget)
        .await
        .unwrap();

    let stream = storage::read_batches(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut result = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = batch.columns()[2..]
                .iter()
                .map(|column| {
                    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!column.is_null(row)).then(|| column.value(row))
                })
                .collect();
            assert!(
                result
                    .insert((files.value(row).to_owned(), rows.value(row)), values)
                    .is_none()
            );
        }
    }
    result
}

async fn wide_fixture(root: &std::path::Path) -> (Engine, ReleasePin) {
    let path = root.join("wide.parquet");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("unused", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
            Arc::new(StringArray::from(vec!["x".repeat(128 << 10); 4])),
            Arc::new(Int64Array::from(vec![Some(9), None, Some(9), Some(-4)])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.join("workspace"), scalar::limits())
        .await
        .unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: scalar::id("wide"),
            operation: scalar::id("initial"),
            base: None,
            relationship: None,
            input: path.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&path).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    (engine, base)
}

#[tokio::test]
async fn wide_unused_values_do_not_consume_the_scalar_batch_allowance() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base) = wide_fixture(root.path()).await;
    let narrow = docspec::budget::Limits {
        batch_rows: 2,
        max_batch_bytes: 64 << 10,
        ..scalar::limits()
    };
    // Establish that the full native read cannot fit this decoded-batch bound.
    let full_budget = Budget::new(scalar::limits()).unwrap();
    let native = storage::open_pin(&base.dataset.groups["source"], &full_budget)
        .await
        .unwrap();

    let full = storage::read_occurrences(&native, &full_budget)
        .await
        .unwrap();
    futures::pin_mut!(full);
    let mut expected = BTreeMap::new();
    while let Some(batch) = full.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let input = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let files = batch
            .column_by_name(storage::OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let answer = (!input.is_null(row)).then(|| input.value(row) + ids.value(row));
            let repeated = (!input.is_null(row)).then(|| input.value(row) * 2);
            assert!(
                expected
                    .insert(
                        (files.value(row).to_owned(), rows.value(row)),
                        vec![answer, repeated]
                    )
                    .is_none()
            );
        }
    }
    let budget = Budget::new(narrow.clone()).unwrap();
    let stream = storage::read_occurrences(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    assert_eq!(
        stream.try_next().await.unwrap_err().code(),
        "oversized_batch"
    );
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), narrow)
        .await
        .unwrap();
    let mut request = scalar::spec(&base, "derive", "derived", "answer", "value + id");
    request.fields.push(ComputedField {
        id: 11,
        name: "again".into(),
        expression: "value + value".into(),
    });
    let result = engine.backfill(&request).await.unwrap();
    assert_eq!(values(&result, "derived").await, expected);
    verify_release(&result, scalar::limits()).await.unwrap();
}

#[tokio::test]
async fn uncompacted_wide_literal_preserves_full_batch_admission() {
    fallback_admission(false).await;
}

#[tokio::test]
async fn all_absent_optional_and_default_dependencies_preserve_full_batch_admission() {
    fallback_admission(true).await;
}

async fn fallback_admission(add_dependencies: bool) {
    let pin_budget = docspec::budget::Budget::new(scalar::limits()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let (engine, mut base) = wide_fixture(root.path()).await;
    if add_dependencies {
        base = engine
            .evolve_schema(&SchemaSpec {
                dataset: base.dataset.dataset.clone(),
                operation: scalar::id("add-fields"),
                base: base.id.clone(),
                definition: base.dataset.definition.clone(),
                next_definition: "v2".into(),
                fields: vec![
                    SchemaField {
                        id: 1,
                        name: "id".into(),
                    },
                    SchemaField {
                        id: 2,
                        name: "unused".into(),
                    },
                    SchemaField {
                        id: 3,
                        name: "value".into(),
                    },
                ],
                additions: vec![
                    ColumnAddition {
                        name: "optional".into(),
                        data_type: ColumnType::Long,
                        default: serde_json::Value::Null,
                    },
                    ColumnAddition {
                        name: "defaulted".into(),
                        data_type: ColumnType::Long,
                        default: 7.into(),
                    },
                ],
                write_defaults: BTreeMap::new(),
            })
            .await
            .unwrap();
    }
    let native = storage::open_pin(&base.dataset.groups["source"], &pin_budget)
        .await
        .unwrap();
    let snapshot = native.metadata().current_snapshot().unwrap();
    let manifests = native.manifest_list_reader(snapshot).load().await.unwrap();
    let mut files = Vec::new();
    for entry in manifests.entries() {
        let manifest = entry.load_manifest(native.file_io()).await.unwrap();
        files.extend(
            manifest
                .entries()
                .iter()
                .filter(|item| item.is_alive())
                .map(|item| item.data_file().clone()),
        );
    }
    let expected_values = if add_dependencies {
        vec![None, Some(7)]
    } else {
        vec![Some(17)]
    };
    // These files retain original physical addresses: no compaction or rewrite.
    let expected: BTreeMap<_, _> = files
        .iter()
        .flat_map(|file| {
            let expected_values = expected_values.clone();
            (0..file.record_count()).map(move |row| {
                (
                    (file.file_path().to_owned(), i64::try_from(row).unwrap()),
                    expected_values.clone(),
                )
            })
        })
        .collect();
    assert_eq!(expected.len(), 4);
    if add_dependencies {
        let added_ids: Vec<_> = native
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.name == "optional" || field.name == "defaulted")
            .map(|field| field.id)
            .collect();
        assert_eq!(added_ids.len(), 2);
        assert!(files.iter().all(|file| {
            added_ids
                .iter()
                .all(|id| !file.column_sizes().contains_key(id))
        }));
    }
    let expression = if add_dependencies {
        "optional"
    } else {
        "CAST(17 AS BIGINT)"
    };
    let mut request = scalar::spec(&base, "fallback", "derived", "answer", expression);
    request.definition = base.dataset.definition.clone();
    if add_dependencies {
        request.fields.push(ComputedField {
            id: 11,
            name: "default_answer".into(),
            expression: "defaulted".into(),
        });
    }
    drop(engine);
    let engine = Engine::open(
        root.path().join("workspace"),
        docspec::budget::Limits {
            batch_rows: 2,
            max_batch_bytes: 64 << 10,
            ..scalar::limits()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "oversized_batch"
    );
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), scalar::limits())
        .await
        .unwrap();
    // Use a fresh operation after refusal; output and occurrence identities must
    // match under an allowance large enough for the conservative full read.
    request.operation = scalar::id("adequate");
    let result = engine.backfill(&request).await.unwrap();
    assert_eq!(values(&result, "derived").await, expected);
    verify_release(&result, scalar::limits()).await.unwrap();
}

#[tokio::test]
async fn literal_only_reads_preserve_stored_origins_after_compaction() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = scalar::fixture(root.path(), false).await;
    let expected = compaction::occurrences(&base.dataset.groups["source"]).await;
    let compacted = engine
        .compact(&compaction::request(&base, "compact"))
        .await
        .unwrap();
    let adopted = engine
        .update(&compaction::update_request(
            root.path(),
            &base,
            "adopt",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(adopted.dataset.groups["source"], compacted.replacement);
    let request = scalar::spec(
        &adopted,
        "constant",
        "constant",
        "answer",
        "CAST(17 AS BIGINT)",
    );
    let result = engine.backfill(&request).await.unwrap();
    let expected: BTreeMap<_, _> = expected
        .into_iter()
        .map(|(file, row, _, _)| ((file, row), vec![Some(17)]))
        .collect();
    assert_eq!(values(&result, "constant").await, expected);
    assert!(result.dataset.computed["constant"].input_fields.is_empty());
    verify_release(&result, scalar::limits()).await.unwrap();
}
