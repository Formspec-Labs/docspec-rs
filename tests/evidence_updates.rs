//! Receipt mutations preserve values, occurrence identity, recovery, and native custody.

use arrow::{
    array::{Array, ArrayRef, Int64Array, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    engine::verify_release,
    evidence::{EVIDENCE, EvidenceSpec, SOURCE_FILE, SOURCE_ROW},
    model::{
        AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, BackfillSpec,
        ComputedField, Id, InputSpec, PhaseResult, RefreshMode, RefreshPolicy, RefreshSpec,
        RefreshTransform, ReleasePin, RunSpec, UpdateSpec,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

#[cfg(feature = "test-util")]
use super::support;

type Location = (String, i64);
type Receipt = (Location, Option<String>);

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}
fn parquet(path: &Path, batch: RecordBatch) -> InputSpec {
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}
fn values(path: &Path, rows: &[(i64, Option<&str>)]) -> InputSpec {
    parquet(path, value_batch(rows))
}
fn value_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
async fn fixture(root: &Path) -> (Engine, ReleasePin) {
    let input = values(
        &root.join("source.parquet"),
        &[
            (1, Some("duplicate")),
            (1, Some("duplicate")),
            (2, None),
            (3, Some("")),
        ],
    );
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    let release = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            definition: "v1".into(),
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            relationship: None,
        })
        .await
        .unwrap();
    (engine, release)
}
async fn locations(engine: &Engine, release: &ReleasePin) -> Vec<(i64, Location)> {
    let mut stream = engine.read_source_occurrences(release).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
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
        let positions = batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                ids.value(row),
                (files.value(row).to_owned(), positions.value(row)),
            )
        }));
    }
    rows
}
fn receipt_input(path: &Path, rows: &[Receipt], deletions: bool) -> InputSpec {
    let mut fields = vec![
        Field::new(SOURCE_FILE, DataType::Utf8, false),
        Field::new(SOURCE_ROW, DataType::Int64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.0.0.as_str()),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0.1))),
    ];
    if !deletions {
        fields.push(Field::new(EVIDENCE, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>(),
        )));
    }
    parquet(
        path,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    )
}
fn request(
    root: &Path,
    base: &ReleasePin,
    operation: &str,
    replacements: &[Receipt],
    deletions: &[Receipt],
) -> EvidenceSpec {
    EvidenceSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        replacements: receipt_input(
            &root.join(format!("{operation}.parquet")),
            replacements,
            false,
        ),
        deletions: (!deletions.is_empty()).then(|| {
            receipt_input(
                &root.join(format!("{operation}-deleted.parquet")),
                deletions,
                true,
            )
        }),
    }
}
async fn receipts(engine: &Engine, release: &ReleasePin) -> Vec<Receipt> {
    let mut stream = engine.read_receipts(release).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                (files.value(row).to_owned(), positions.value(row)),
                (!values.is_null(row)).then(|| values.value(row).to_owned()),
            )
        }));
    }
    rows.sort();
    rows
}
async fn computed(engine: &Engine, release: &ReleasePin, name: &str) -> ReleasePin {
    engine
        .backfill(&BackfillSpec {
            dataset: release.dataset.dataset.clone(),
            operation: id(name),
            base: release.id.clone(),
            definition: release.dataset.definition.clone(),
            group: id(name),
            group_definition: "double-v1".into(),
            fields: vec![ComputedField {
                id: 1,
                name: format!("{name}_value"),
                expression: "id * 2".into(),
            }],
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn receipt_replacement_preserves_duplicates_nulls_source_and_computed_pins() {
    let root = TempDir::new().unwrap();
    let (engine, initial) = fixture(root.path()).await;
    let base = computed(&engine, &initial, "double").await;
    let positions = locations(&engine, &base).await;
    assert_ne!(
        positions[0].1, positions[1].1,
        "equal values are distinct occurrences"
    );
    let mut expected = vec![
        (positions[0].1.clone(), None),
        (positions[0].1.clone(), Some("".into())),
        (positions[0].1.clone(), Some("same".into())),
        (positions[0].1.clone(), Some("same".into())),
        (positions[1].1.clone(), Some("other occurrence".into())),
        (positions[2].1.clone(), Some("keep".into())),
    ];
    let spec = request(root.path(), &base, "receipts1", &expected, &[]);
    let attached = engine.update_evidence(&spec).await.unwrap();
    assert_eq!(attached.dataset.groups, base.dataset.groups);
    assert_eq!(attached.dataset.computed, base.dataset.computed);
    assert_eq!(attached.dataset.boundary, base.dataset.boundary);
    assert_eq!(attached.dataset.related, base.dataset.related);
    expected.sort();
    assert_eq!(receipts(&engine, &attached).await, expected);
    let changes = engine
        .change_certificate(&attached.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .receipts
        .unwrap();
    assert_eq!(changes.removed.rows, 0);
    verify_release(&attached, limits()).await.unwrap();
    let replace = vec![(positions[0].1.clone(), Some("corrected".into()))];
    let deleted = vec![(positions[1].1.clone(), None)];
    let changed = engine
        .update_evidence(&request(
            root.path(),
            &attached,
            "receipts2",
            &replace,
            &deleted,
        ))
        .await
        .unwrap();
    assert_eq!(changed.dataset.groups, base.dataset.groups);
    let delta = engine
        .change_certificate(&changed.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .receipts
        .unwrap();
    assert_eq!((delta.removed.rows, delta.added.rows), (5, 1));
    let mut remaining = vec![
        (positions[0].1.clone(), Some("corrected".into())),
        (positions[2].1.clone(), Some("keep".into())),
    ];
    remaining.sort();
    assert_eq!(receipts(&engine, &changed).await, remaining);
    verify_release(&changed, limits()).await.unwrap();
    assert_eq!(engine.update_evidence(&spec).await.unwrap(), attached);
    let extended = computed(&engine, &changed, "second").await;
    assert_eq!(
        extended.dataset.evidence.as_ref().unwrap().table,
        changed.dataset.evidence.as_ref().unwrap().table
    );
    verify_release(&extended, limits()).await.unwrap();
}

#[tokio::test]
async fn first_value_update_preserves_receipts_for_unrelated_rows_in_the_same_file() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let positions = locations(&engine, &base).await;
    assert_eq!(
        positions
            .iter()
            .map(|row| &row.1.0)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1
    );
    let rows = positions
        .iter()
        .map(|(value, location)| (location.clone(), Some(format!("receipt {value}"))))
        .collect::<Vec<_>>();
    let attached = engine
        .update_evidence(&request(root.path(), &base, "receipts", &rows, &[]))
        .await
        .unwrap();
    let update = UpdateSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("correction"),
        base: attached.id.clone(),
        definition: "v1".into(),
        keys: vec!["id".into()],
        replacements: values(&root.path().join("correction.parquet"), &[(1, Some("new"))]),
        deletions: None,
    };
    let changed = engine.update(&update).await.unwrap();
    let after = locations(&engine, &changed).await;
    for retained in [2, 3] {
        assert_eq!(
            after.iter().find(|row| row.0 == retained),
            positions.iter().find(|row| row.0 == retained)
        );
    }
    let mut expected = rows
        .into_iter()
        .filter(|(location, _)| {
            positions
                .iter()
                .any(|(value, original)| *value >= 2 && original == location)
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(receipts(&engine, &changed).await, expected);
    let receipt_changes = engine
        .change_certificate(&changed.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .receipts
        .unwrap();
    assert_eq!(
        (receipt_changes.removed.rows, receipt_changes.added.rows),
        (2, 0)
    );
    verify_release(&changed, limits()).await.unwrap();
    let retained = engine
        .inspect_retention(&changed.dataset.evidence.as_ref().unwrap().table.objects)
        .await
        .unwrap();
    assert!(
        retained
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let point = engine
        .create_restore_point(&id("comments"), &id("with-receipts"))
        .await
        .unwrap();
    assert!(!point.manifest.digest.is_empty());
    let receipt_key = engine
        .inspect(&id("comments"))
        .await
        .unwrap()
        .into_iter()
        .find_map(|(key, value)| {
            serde_json::from_value::<PhaseResult>(value)
                .ok()
                .filter(|phase| phase.operation == update.operation && phase.phase == "receipts")
                .map(|_| key)
        })
        .unwrap();
    let retirement = engine
        .plan_phase_retirement(
            &id("comments"),
            &id("receipt-recovery"),
            &[receipt_key],
            "completed selection retains the receipt result",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    assert_eq!(engine.update(&update).await.unwrap(), changed);
    assert_eq!(receipts(&engine, &changed).await, expected);
}

#[tokio::test]
async fn receipt_associations_refuse_unknown_rows_overlap_and_conflicting_retries() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let positions = locations(&engine, &base).await;
    let unknown = vec![((positions[0].1.0.clone(), 999), Some("wrong row".into()))];
    let bad = request(root.path(), &base, "unknown", &unknown, &[]);
    assert_eq!(
        engine.update_evidence(&bad).await.unwrap_err().code(),
        "invalid_evidence"
    );
    let same = vec![(positions[0].1.clone(), Some("receipt".into()))];
    let overlap = request(root.path(), &base, "overlap", &same, &same);
    assert_eq!(
        engine.update_evidence(&overlap).await.unwrap_err().code(),
        "invalid_evidence"
    );
    let spec = request(root.path(), &base, "accepted", &same, &[]);
    engine.update_evidence(&spec).await.unwrap();
    let mut conflict = spec.clone();
    conflict.replacements.digest = "f".repeat(64);
    assert_eq!(
        engine.update_evidence(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
    let stale = request(root.path(), &base, "stale", &same, &[]);
    assert_eq!(
        engine.update_evidence(&stale).await.unwrap_err().code(),
        "stale_base"
    );
}

#[tokio::test]
async fn nested_receipt_lists_preserve_order_empty_and_null_distinctions() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let locations = locations(&engine, &base).await;
    let mut nested = ListBuilder::new(StringBuilder::new());
    nested.values().append_value("second");
    nested.values().append_value("first");
    nested.values().append_null();
    nested.append(true);
    nested.append(true);
    nested.append(false);
    let nested = nested.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new(SOURCE_FILE, DataType::Utf8, false),
        Field::new(SOURCE_ROW, DataType::Int64, false),
        Field::new(EVIDENCE, nested.data_type().clone(), true),
    ]));
    let input = parquet(
        &root.path().join("nested.parquet"),
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    locations[..3].iter().map(|row| row.1.0.as_str()),
                )),
                Arc::new(Int64Array::from_iter_values(
                    locations[..3].iter().map(|row| row.1.1),
                )),
                Arc::new(nested),
            ],
        )
        .unwrap(),
    );
    let release = engine
        .update_evidence(&EvidenceSpec {
            dataset: id("comments"),
            operation: id("nested"),
            definition: "v1".into(),
            base: base.id,
            replacements: input,
            deletions: None,
        })
        .await
        .unwrap();
    let mut stream = engine.read_receipts(&release).await.unwrap();
    let mut actual = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let receipts = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let value = (!receipts.is_null(row)).then(|| {
                let child = receipts.value(row);
                child
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect::<Vec<_>>()
            });
            actual.insert(keys.value(row), value);
        }
    }
    assert_eq!(
        actual[&locations[0].1.1],
        Some(vec![Some("second".into()), Some("first".into()), None])
    );
    assert_eq!(actual[&locations[1].1.1], Some(vec![]));
    assert_eq!(actual[&locations[2].1.1], None);
    verify_release(&release, limits()).await.unwrap();
}

fn summary(input: &ReleasePin, base: Option<Id>, operation: &str) -> RefreshSpec {
    RefreshSpec {
        dataset: id("summary"),
        operation: id(operation),
        base: base.clone(),
        definition: "summary-v1".into(),
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: input.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["id".into()],
                aggregates: vec![AggregateField {
                    name: "count".into(),
                    function: AggregateFunction::CountRows,
                }],
            },
        }),
        policy: RefreshPolicy::Captured,
        mode: if base.is_some() {
            RefreshMode::Incremental
        } else {
            RefreshMode::Full
        },
        relationship: None,
    }
}

#[tokio::test]
async fn evidence_only_refresh_reuses_values_and_contributors_with_new_coverage() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let initial = engine
        .refresh(&summary(&base, None, "initial_summary"))
        .await
        .unwrap();
    let positions = locations(&engine, &base).await;
    let attached = engine
        .update_evidence(&request(
            root.path(),
            &base,
            "receipts",
            &[(positions[0].1.clone(), Some("new evidence".into()))],
            &[],
        ))
        .await
        .unwrap();
    let refreshed = engine
        .refresh(&summary(
            &attached,
            Some(initial.id.clone()),
            "refreshed_summary",
        ))
        .await
        .unwrap();
    assert_eq!(refreshed.dataset.groups, initial.dataset.groups);
    assert_eq!(
        refreshed.dataset.related.as_ref().unwrap().evidence,
        initial.dataset.related.as_ref().unwrap().evidence
    );
    assert_eq!(
        refreshed.dataset.related.as_ref().unwrap().coverage.inputs[&id("comments")],
        attached.version_ref()
    );
    let rows = engine.inspect(&id("summary")).await.unwrap();
    let processing = rows
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|phase| phase.operation == refreshed.id && phase.phase == "processing")
        .unwrap();
    assert_eq!(processing.metrics.data_files_written, 0);
    assert_eq!(processing.metrics.files_scanned, 0);
    verify_release(&refreshed, limits()).await.unwrap();
    let summary_locations = locations(&engine, &refreshed).await;
    let with_receipt = engine
        .update_evidence(&request(
            root.path(),
            &refreshed,
            "summary_receipt",
            &[(summary_locations[0].1.clone(), Some("summary note".into()))],
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(with_receipt.dataset.related, refreshed.dataset.related);
    assert_eq!(with_receipt.dataset.groups, refreshed.dataset.groups);
    verify_release(&with_receipt, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn active_backfill_adopts_new_receipts_without_recomputing_values() {
    for stop in ["processing", "readback"] {
        let root = TempDir::new().unwrap();
        let (engine, base) = fixture(root.path()).await;
        let spec = BackfillSpec {
            dataset: id("comments"),
            operation: id("enrichment"),
            base: base.id.clone(),
            definition: "v1".into(),
            group: id("enrichment"),
            group_definition: "v1".into(),
            fields: vec![ComputedField {
                id: 1,
                name: "length".into(),
                expression: "character_length(text)".into(),
            }],
        };
        assert_eq!(
            engine.backfill_until(&spec, stop).await.unwrap_err().code(),
            "injected_failure"
        );
        let completed = engine
            .inspect(&id("comments"))
            .await
            .unwrap()
            .into_iter()
            .filter(|(key, _)| key.starts_with("backfill-prepared/"))
            .collect::<BTreeMap<_, _>>();
        assert!(!completed.is_empty());
        let positions = locations(&engine, &base).await;
        let expected = vec![(
            positions[0].1.clone(),
            Some("arrived during backfill".into()),
        )];
        let attached = engine
            .update_evidence(&request(root.path(), &base, "receipts", &expected, &[]))
            .await
            .unwrap();
        let result = engine.backfill(&spec).await.unwrap();
        assert_eq!(
            result.dataset.groups["source"],
            base.dataset.groups["source"]
        );
        assert_eq!(
            result.dataset.evidence.as_ref().unwrap().table,
            attached.dataset.evidence.as_ref().unwrap().table
        );
        assert_eq!(receipts(&engine, &result).await, expected);
        let after = engine
            .inspect(&id("comments"))
            .await
            .unwrap()
            .into_iter()
            .filter(|(key, _)| key.starts_with("backfill-prepared/"))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            after, completed,
            "{stop}: retained scalar work was recomputed"
        );
        verify_release(&result, limits()).await.unwrap();
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn receipt_processing_survives_process_loss_without_original_input() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let positions = locations(&engine, &base).await;
    let rows = vec![(positions[0].1.clone(), Some("durable".into()))];
    let spec = request(root.path(), &base, "recover", &rows, &[]);
    engine.shutdown().await.unwrap();
    drop(engine);
    support::kill_after(root.path(), &spec, "evidence", "processing");
    std::fs::remove_file(&spec.replacements.path).unwrap();
    let (release, work) = support::resume(root.path(), "evidence");
    assert!(
        !work
            .iter()
            .any(|phase| phase == "admission" || phase == "processing")
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(receipts(&engine, &release).await, rows);
    assert_eq!(engine.update_evidence(&spec).await.unwrap(), release);
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn one_receipt_on_many_origin_files_uses_a_fixed_reader_budget() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    let root = TempDir::new().unwrap();
    // Small source files exercise reader fanout. Deterministic varying text keeps
    // compression from collapsing the fixture into one tiny native file.
    let mut state = 1_u64;
    let text = (0..512)
        .map(|_| {
            (0..4096)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    char::from(b'a' + (state % 26) as u8)
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>();
    let rows = text
        .iter()
        .enumerate()
        .map(|(index, value)| (index as i64, Some(value.as_str())))
        .collect::<Vec<_>>();
    let input_path = root.path().join("many-files.parquet");
    let batch = value_batch(&rows);
    // Decoder admission covers a complete source row group before batch slicing.
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&input_path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let input = InputSpec {
        path: input_path.to_string_lossy().into_owned(),
        digest: storage::digest(&input_path).unwrap().0,
    };
    let fixed = Limits {
        batch_rows: 1,
        ..limits()
    };
    let engine = Engine::open(root.path().join("workspace"), fixed.clone())
        .await
        .unwrap();
    let base = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            definition: "v1".into(),
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            relationship: None,
        })
        .await
        .unwrap();
    let compacted = engine
        .compact(&docspec::model::CompactionSpec {
            dataset: id("comments"),
            operation: id("many-files"),
            version: base.id.clone(),
            group: "source".into(),
            files: vec![],
            target_file_bytes: 8 << 10,
        })
        .await
        .unwrap();
    let positions = locations(&engine, &base).await;
    let file_count = compacted.output_files.len();
    assert!(
        file_count * 2 * fixed.max_batch_bytes > fixed.memory_bytes,
        "fixture must exceed the budget if every file opens a reader: {file_count}"
    );
    let initial = vec![(positions[0].1.clone(), Some("before".into()))];
    let attached = engine
        .update_evidence(&request(root.path(), &base, "first_receipt", &initial, &[]))
        .await
        .unwrap();
    // Append consumes the selected compacted representation, preserving its
    // files and origin fields. Subsequent receipt membership must read origins.
    let appended = engine
        .update(&UpdateSpec {
            dataset: id("comments"),
            operation: id("append"),
            base: attached.id,
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: values(&root.path().join("append.parquet"), &[(999, Some("new"))]),
            deletions: None,
        })
        .await
        .unwrap();
    let appended_objects = &appended.dataset.groups["source"].objects;
    assert!(
        compacted
            .output_files
            .iter()
            .all(|path| appended_objects.iter().any(|object| &object.path == path)),
        "append must share the captured compacted files"
    );
    let native = storage::open_pin(&appended.dataset.groups["source"], &pin_budget)
        .await
        .unwrap();
    assert!(
        native
            .metadata()
            .current_schema()
            .field_by_name("__docspec_origin_file")
            .is_some()
    );
    let corrected = vec![(positions[0].1.clone(), Some("after".into()))];
    let changed = engine
        .update_evidence(&request(
            root.path(),
            &appended,
            "one_receipt",
            &corrected,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(changed.dataset.groups, appended.dataset.groups);
    assert_eq!(receipts(&engine, &changed).await, corrected);
    let absent = request(
        root.path(),
        &changed,
        "absent_receipt",
        &[((positions[0].1.0.clone(), i64::MAX), Some("absent".into()))],
        &[],
    );
    assert_eq!(
        engine.update_evidence(&absent).await.unwrap_err().code(),
        "invalid_evidence"
    );
    verify_release(&changed, fixed).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn adopted_receipts_do_not_appear_as_pending_after_cold_validation_recovery() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let positions = locations(&engine, &base).await;
    let expected = vec![(positions[0].1.clone(), Some("retained".into()))];
    let attached = engine
        .update_evidence(&request(root.path(), &base, "receipts", &expected, &[]))
        .await
        .unwrap();
    let spec = UpdateSpec {
        dataset: id("comments"),
        operation: id("append"),
        base: attached.id,
        definition: "v1".into(),
        keys: vec!["id".into()],
        replacements: values(&root.path().join("append.parquet"), &[(4, Some("new"))]),
        deletions: None,
    };
    engine.shutdown().await.unwrap();
    drop(engine);
    support::kill_after(root.path(), &spec, "update", "validation");
    std::fs::remove_file(&spec.replacements.path).unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan_update(&spec).await.unwrap();
    assert!(
        plan.reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert!(
        plan.reusable_phases
            .iter()
            .any(|phase| phase == "validation")
    );
    assert!(
        !plan
            .required_phases
            .iter()
            .chain(&plan.reusable_phases)
            .any(|phase| phase == "receipts")
    );
    engine.shutdown().await.unwrap();
    drop(engine);
    let (release, work) = support::resume(root.path(), "update");
    assert!(!work.iter().any(|phase| {
        ["admission", "processing", "receipts", "validation"].contains(&phase.as_str())
    }));
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(receipts(&engine, &release).await, expected);
    verify_release(&release, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn inherited_receipt_deletions_survive_process_loss_without_repeating_work() {
    let root = TempDir::new().unwrap();
    let (engine, base) = fixture(root.path()).await;
    let positions = locations(&engine, &base).await;
    let original = vec![
        (positions[0].1.clone(), Some("replaced".into())),
        (positions[2].1.clone(), Some("survives".into())),
    ];
    let attached = engine
        .update_evidence(&request(root.path(), &base, "receipts", &original, &[]))
        .await
        .unwrap();
    let spec = UpdateSpec {
        dataset: id("comments"),
        operation: id("replace"),
        base: attached.id,
        definition: "v1".into(),
        keys: vec!["id".into()],
        replacements: values(
            &root.path().join("replacement.parquet"),
            &[(1, Some("corrected"))],
        ),
        deletions: None,
    };
    engine.shutdown().await.unwrap();
    drop(engine);
    support::kill_after(root.path(), &spec, "update", "receipts");
    std::fs::remove_file(&spec.replacements.path).unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let retained = engine
        .inspect(&id("comments"))
        .await
        .unwrap()
        .into_values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value).ok())
        .find(|phase| phase.operation == spec.operation && phase.phase == "receipts")
        .unwrap();
    let plan = engine.plan_update(&spec).await.unwrap();
    assert!(plan.reusable_phases.iter().any(|phase| phase == "receipts"));
    engine.shutdown().await.unwrap();
    drop(engine);
    let (release, work) = support::resume(root.path(), "update");
    assert!(
        !work
            .iter()
            .any(|phase| ["admission", "processing", "receipts"].contains(&phase.as_str()))
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(receipts(&engine, &release).await, vec![original[1].clone()]);
    let resumed = engine
        .inspect(&id("comments"))
        .await
        .unwrap()
        .into_values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value).ok())
        .find(|phase| phase.operation == spec.operation && phase.phase == "receipts")
        .unwrap();
    assert_eq!(
        resumed, retained,
        "resume must retain the complete original receipt completion"
    );
    assert_eq!(
        release.dataset.evidence.as_ref().unwrap().table,
        retained.table.unwrap()
    );
    let delta = engine
        .change_certificate(&release.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .receipts
        .unwrap();
    assert_eq!((delta.removed.rows, delta.added.rows), (1, 0));
    verify_release(&release, limits()).await.unwrap();
}
#[cfg(all(feature = "test-util", unix))]
#[path = "evidence_updates/optional_history.rs"]
mod optional_history;
