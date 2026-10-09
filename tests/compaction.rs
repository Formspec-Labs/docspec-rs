//! Equivalent native representations preserve values, occurrences and saved recovery work.

#[path = "support/scalar.rs"]
mod scalar;

use arrow::array::{Int64Array, StringArray};
use docspec::{
    Engine,
    budget::Budget,
    model::{CompactionSpec, ReleasePin, TablePin},
    storage,
};
use futures::{StreamExt, TryStreamExt};
use scalar::{fixture, id, limits};

fn request(release: &ReleasePin, operation: &str) -> CompactionSpec {
    CompactionSpec {
        dataset: release.dataset.dataset.clone(),
        operation: id(operation),
        version: release.id.clone(),
        group: "source".into(),
        files: vec![],
        target_file_bytes: 16 << 20,
    }
}

async fn occurrences(pin: &TablePin) -> Vec<(String, i64, Option<i64>, Option<String>)> {
    let native = storage::open_pin(pin).await.unwrap();
    let budget = Budget::new(limits()).unwrap();
    let mut stream = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let origins = batch
            .column_by_name(storage::OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values.extend(
            origins
                .iter()
                .zip(positions.iter())
                .zip(ids.iter().zip(text.iter()))
                .map(|((origin, row), (id, text))| {
                    (
                        origin.unwrap().to_owned(),
                        row.unwrap(),
                        id,
                        text.map(str::to_owned),
                    )
                }),
        );
    }
    values.sort();
    values
}

async fn combined(
    engine: &Engine,
    release: &ReleasePin,
) -> Vec<(Option<i64>, Option<String>, Option<i64>)> {
    let mut stream = engine.read_combined(release).await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__docspec_origin_"))
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let length =
            arrow::compute::cast(batch.column(2), &arrow::datatypes::DataType::Int64).unwrap();
        let length = length.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend(
            ids.iter()
                .zip(text.iter())
                .zip(length.iter())
                .map(|((id, text), length)| (id, text.map(str::to_owned), length)),
        );
    }
    values.sort();
    values
}

#[tokio::test]
async fn source_compaction_preserves_duplicates_backfill_identity_and_logical_selection() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let backfill = scalar::spec(&base, "lengths", "length", "text_length", "length(text)");
    let release = engine.backfill(&backfill).await.unwrap();
    let expected = occurrences(&release.dataset.groups["source"]).await;
    let combined_before = combined(&engine, &release).await;
    let mut old_reader = engine.read_combined(&release).await.unwrap();
    let result = engine.compact(&request(&release, "compact")).await.unwrap();
    assert!(result.verified);
    assert!(result.input_files.len() > result.output_files.len());
    assert_eq!(
        result.replacement.logical_metadata(),
        &release.dataset.groups["source"].metadata
    );
    assert_eq!(occurrences(&result.replacement).await, expected);
    assert_eq!(combined(&engine, &release).await, combined_before);
    assert_eq!(
        engine.latest(&release.dataset.dataset).await.unwrap(),
        release
    );
    assert_eq!(engine.backfill(&backfill).await.unwrap(), release);
    let mut old_rows = 0;
    while let Some(batch) = old_reader.try_next().await.unwrap() {
        old_rows += batch.num_rows();
    }
    assert_eq!(old_rows, expected.len());
    let second = engine
        .compact(&request(&release, "compact-again"))
        .await
        .unwrap();
    assert_eq!(second.source, result.replacement);
    assert_eq!(occurrences(&second.replacement).await, expected);
    assert_eq!(
        second.replacement.logical_metadata(),
        result.replacement.logical_metadata()
    );
    let mut group = request(&release, "compact-length");
    group.group = "length".into();
    engine.compact(&group).await.unwrap();
    assert_eq!(combined(&engine, &release).await, combined_before);
}

#[tokio::test]
async fn empty_source_and_partial_file_groups_preserve_exact_membership() {
    for empty in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (engine, release, _) = fixture(root.path(), empty).await;
        let expected = occurrences(&release.dataset.groups["source"]).await;
        let mut spec = request(&release, "partial");
        if !empty {
            spec.files = release.dataset.groups["source"]
                .objects
                .iter()
                .filter(|object| object.path.ends_with(".parquet"))
                .take(2)
                .map(|object| object.path.clone())
                .collect();
        }
        let result = engine.compact(&spec).await.unwrap();
        assert_eq!(occurrences(&result.replacement).await, expected);
        for object in &result.source.objects {
            if object.path.ends_with(".parquet") && !result.input_files.contains(&object.path) {
                assert!(result.replacement.objects.contains(object));
            }
        }
        assert_eq!(engine.compact(&spec).await.unwrap(), result);
        let mut conflict = spec;
        conflict.target_file_bytes += 1;
        assert_eq!(
            engine.compact(&conflict).await.unwrap_err().code(),
            "invalid_compaction"
        );
    }
}

#[tokio::test]
async fn native_nested_decimal_time_and_null_values_survive_typed_rewrite() {
    use arrow::{
        array::{Array, Decimal128Array, ListArray, TimestampMicrosecondArray},
        datatypes::{Field, Int64Type, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("typed.parquet");
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None, Some(1)]),
        None,
        Some(vec![]),
    ]);
    let decimal = Decimal128Array::from(vec![Some(1234500), None, Some(-1)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let time = TimestampMicrosecondArray::from(vec![Some(1234567890123), None, Some(-1)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("nested", list.data_type().clone(), true),
        Field::new("decimal", decimal.data_type().clone(), true),
        Field::new("time", time.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(list), Arc::new(decimal), Arc::new(time)],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&docspec::model::RunSpec {
            dataset: id("typed"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    // Compare the two native interpretations. Iceberg assigns nested field IDs
    // and names at input admission, independently of this physical rewrite.
    let mut original = engine.read_group(&release, "source").await.unwrap();
    let expected = original.try_next().await.unwrap().unwrap();
    assert!(original.try_next().await.unwrap().is_none());
    let result = engine
        .compact(&request(&release, "typed-compact"))
        .await
        .unwrap();
    let budget = Budget::new(limits()).unwrap();
    let native = storage::open_pin(&result.replacement).await.unwrap();
    let mut stream = storage::read_batches(&native, &budget)
        .await
        .unwrap()
        .boxed();
    let actual = stream.try_next().await.unwrap().unwrap();
    for (index, expected) in expected.columns().iter().enumerate() {
        assert_eq!(actual.column(index).to_data(), expected.to_data());
    }
    assert!(stream.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn schema_defaults_and_supplied_state_remain_native_and_exact() {
    use docspec::model::{ColumnAddition, ColumnType, SchemaField, SchemaSpec};
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let release = engine
        .evolve_schema(&SchemaSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("defaulted"),
            base: base.id.clone(),
            definition: "v1".into(),
            next_definition: "v2".into(),
            fields: vec![
                SchemaField {
                    id: 2,
                    name: "text".into(),
                },
                SchemaField {
                    id: 1,
                    name: "id".into(),
                },
            ],
            additions: vec![ColumnAddition {
                name: "category".into(),
                data_type: ColumnType::String,
                default: serde_json::json!("retained"),
            }],
            write_defaults: std::collections::BTreeMap::new(),
        })
        .await
        .unwrap();
    let result = engine
        .compact(&request(&release, "default-compact"))
        .await
        .unwrap();
    assert_eq!(
        occurrences(&result.source).await,
        occurrences(&result.replacement).await
    );
    let before = storage::open_pin(&result.source).await.unwrap();
    let after = storage::open_pin(&result.replacement).await.unwrap();
    for field in before.metadata().current_schema().as_struct().fields() {
        assert_eq!(
            after.metadata().current_schema().field_by_id(field.id),
            Some(field)
        );
    }
    let budget = Budget::new(limits()).unwrap();
    let mut stream = storage::read_batches(&after, &budget)
        .await
        .unwrap()
        .boxed();
    let mut count = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        let category = batch
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(category.iter().all(|value| value == Some("retained")));
        count += batch.num_rows();
    }
    assert_eq!(count as u64, release.dataset.groups["source"].rows);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn saved_rewrites_and_verification_resume_after_reopen_without_replacement() {
    for phase in ["rewrite", "verification", "selection"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, release, _) = fixture(root.path(), false).await;
        let spec = request(&release, "resume");
        assert_eq!(
            engine.compact_until(&spec, phase).await.unwrap_err().code(),
            "injected_failure"
        );
        let before = engine.inspect(&spec.dataset).await.unwrap();
        let prepared = before["compaction-prepared/resume"].clone();
        drop(engine);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let result = engine.compact(&spec).await.unwrap();
        let after = engine.inspect(&spec.dataset).await.unwrap();
        assert_eq!(after["compaction-prepared/resume"], prepared);
        if phase != "rewrite" {
            assert_eq!(
                after["compaction-verified/resume"],
                before["compaction-verified/resume"]
            );
        }
        assert_eq!(
            occurrences(&result.replacement).await,
            occurrences(&result.source).await
        );
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn competing_representation_keeps_stale_completed_work_without_reselection() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path(), false).await;
    let first = request(&release, "first");
    assert_eq!(
        engine
            .compact_until(&first, "rewrite")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let second = engine.compact(&request(&release, "second")).await.unwrap();
    assert_eq!(
        engine.compact(&first).await.unwrap_err().code(),
        "stale_representation"
    );
    let rows = engine.inspect(&first.dataset).await.unwrap();
    assert!(rows.contains_key("compaction-verified/first"));
    let selected: docspec::model::CompactionResult = serde_json::from_value(
        rows[&format!(
            "representation/{}/source",
            release.dataset.groups["source"].logical_metadata().digest
        )]
            .clone(),
    )
    .unwrap();
    assert_eq!(selected, second);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn sibling_backfill_keeps_the_shared_equivalent_source_representation() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let spec = request(&base, "in-flight");
    assert_eq!(
        engine
            .compact_until(&spec, "rewrite")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let sibling = engine
        .backfill(&scalar::spec(
            &base,
            "sibling",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let expected = combined(&engine, &sibling).await;
    let selected = engine.compact(&spec).await.unwrap();
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), sibling);
    assert_eq!(combined(&engine, &sibling).await, expected);
    let next = engine
        .compact(&request(&sibling, "from-sibling"))
        .await
        .unwrap();
    assert_eq!(next.source, selected.replacement);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn admitted_compaction_survives_retired_historical_result_availability() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let spec = request(&base, "retained-rewrite");
    assert_eq!(
        engine
            .compact_until(&spec, "rewrite")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let prepared =
        engine.inspect(&spec.dataset).await.unwrap()["compaction-prepared/retained-rewrite"]
            .clone();
    let sibling = engine
        .backfill(&scalar::spec(
            &base,
            "current-sibling",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let expected = combined(&engine, &sibling).await;
    let retirement = engine
        .plan_availability_retirement(
            &spec.dataset,
            &id("retire-compaction-base"),
            &base.id,
            "end standalone historical access while retaining admitted compaction",
        )
        .await
        .unwrap();
    engine.retire_availability(&retirement).await.unwrap();
    assert_eq!(
        engine
            .release(&spec.dataset, &base.id)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );

    let completed = engine.compact(&spec).await.unwrap();
    assert_eq!(engine.compact(&spec).await.unwrap(), completed);
    assert_eq!(
        engine.inspect(&spec.dataset).await.unwrap()["compaction-prepared/retained-rewrite"],
        prepared
    );
    assert_eq!(
        occurrences(&completed.replacement).await,
        occurrences(&completed.source).await
    );
    assert_eq!(combined(&engine, &sibling).await, expected);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), sibling);
    assert_eq!(
        engine
            .compact(&request(&base, "fresh-after-retirement"))
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert!(
        !engine
            .inspect(&spec.dataset)
            .await
            .unwrap()
            .contains_key("compaction-request/fresh-after-retirement")
    );
}

#[tokio::test]
async fn tiny_targets_refuse_bounded_descriptor_inventory_without_selecting_partial_files() {
    fn parquet_files(directory: &std::path::Path) -> usize {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    parquet_files(&entry.path())
                } else {
                    usize::from(
                        entry
                            .path()
                            .extension()
                            .is_some_and(|extension| extension == "parquet"),
                    )
                }
            })
            .sum()
    }

    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    drop(engine);
    let engine = Engine::open(
        root.path().join("workspace"),
        docspec::budget::Limits {
            max_metadata_bytes: 256 << 10,
            batch_rows: 1,
            ..limits()
        },
    )
    .await
    .unwrap();
    let mut spec = request(&base, "tiny-target");
    spec.target_file_bytes = 1;
    let error = engine.compact(&spec).await.unwrap_err();
    assert_eq!(error.code(), "metadata_capacity");
    assert!(error.to_string().contains("writer"), "{error}");
    let retained = engine.inspect(&spec.dataset).await.unwrap();
    assert!(retained.contains_key("compaction-request/tiny-target"));
    for prefix in [
        "compaction-prepared",
        "compaction-verified",
        "compaction-result",
    ] {
        assert!(!retained.contains_key(&format!("{prefix}/tiny-target")));
    }
    assert!(
        !retained
            .keys()
            .any(|key| key.starts_with("representation/"))
    );
    let attempt = retained
        .iter()
        .find(|(key, value)| {
            key.starts_with("protection/")
                && value["purpose"].as_str() == Some("tiny-target/compaction")
        })
        .map(|(_, value)| value["paths"][0].as_str().unwrap())
        .unwrap();
    // One-row batches and a one-byte target stress completed-file descriptors.
    // The writer must stop before producing the full per-row file population.
    let partial_files = parquet_files(std::path::Path::new(attempt));
    assert!(partial_files > 0);
    assert!((partial_files as u64) < base.dataset.groups["source"].rows);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), base);

    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let completed = engine.compact(&spec).await.unwrap();
    assert_eq!(
        occurrences(&completed.replacement).await,
        occurrences(&completed.source).await
    );
}

#[tokio::test]
async fn huge_target_rotates_bounded_footer_windows_without_limiting_total_rows() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let expected = occurrences(&base.dataset.groups["source"]).await;
    drop(engine);
    let engine = Engine::open(
        root.path().join("workspace"),
        docspec::budget::Limits {
            max_metadata_bytes: 1 << 20,
            batch_rows: 1,
            ..limits()
        },
    )
    .await
    .unwrap();
    let mut spec = request(&base, "huge-target");
    spec.target_file_bytes = usize::MAX;
    let completed = engine.compact(&spec).await.unwrap();
    assert!(
        completed.output_files.len() > 1,
        "writer must rotate before reaching the unreachable file target"
    );
    assert_eq!(completed.rows, base.dataset.groups["source"].rows);
    assert_eq!(occurrences(&completed.replacement).await, expected);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), base);
    assert_eq!(engine.compact(&spec).await.unwrap(), completed);
}
