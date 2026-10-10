//! Exact comparison expectations use Rust bags and endpoint occurrence addresses.

#[path = "support/scalar.rs"]
mod scalar;

use arrow::{
    array::{Int64Array, StringArray, StructArray},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    model::{
        ChangeDescriptor, ComparisonFallback, ComparisonIdentity, ComparisonOptions,
        ComparisonPath, ReleasePin, RunSpec, UpdateSpec,
    },
    storage,
};
use futures::TryStreamExt;
use std::collections::BTreeMap;

type Row = (Option<i64>, Option<String>);
type Member = (String, i64, Row);
type Change = (String, Member);

fn members(batch: &RecordBatch, nested: bool) -> Vec<Member> {
    let (files, rows, ids, texts) = if nested {
        let values = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        (
            batch.column_by_name("file").unwrap(),
            batch.column_by_name("row").unwrap(),
            values.column_by_name("id").unwrap(),
            values.column_by_name("text").unwrap(),
        )
    } else {
        (
            batch.column_by_name(storage::OCCURRENCE_FILE).unwrap(),
            batch.column_by_name(storage::OCCURRENCE_ROW).unwrap(),
            batch.column_by_name("id").unwrap(),
            batch.column_by_name("text").unwrap(),
        )
    };
    files
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .zip(rows.as_any().downcast_ref::<Int64Array>().unwrap().iter())
        .zip(
            ids.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .zip(texts.as_any().downcast_ref::<StringArray>().unwrap().iter()),
        )
        .map(|((file, row), (id, text))| {
            (
                file.unwrap().to_owned(),
                row.unwrap(),
                (id, text.map(str::to_owned)),
            )
        })
        .collect()
}

async fn endpoint(engine: &Engine, release: &ReleasePin) -> Vec<Member> {
    let batches: Vec<RecordBatch> = engine
        .read_source_occurrences(release)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| members(batch, false))
        .collect()
}

async fn changes(
    engine: &Engine,
    before: &ReleasePin,
    after: &ReleasePin,
    options: &ComparisonOptions,
) -> (ChangeDescriptor, Vec<Change>) {
    let mut stream = engine
        .changes(before, after, "source", options)
        .await
        .unwrap();
    let descriptor = serde_json::from_str(&stream.schema().metadata()["docspec.change"]).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let sides = batch
            .column_by_name("side")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        rows.extend(
            sides
                .iter()
                .zip(members(&batch, true))
                .map(|(side, member)| (side.unwrap().to_owned(), member)),
        );
    }
    rows.sort();
    (descriptor, rows)
}

fn difference<T: Ord + Clone>(before: &[T], after: &[T]) -> BTreeMap<T, i64> {
    let mut counts = BTreeMap::new();
    for (values, weight) in [(before, -1), (after, 1)] {
        for row in values {
            *counts.entry(row.clone()).or_default() += weight;
        }
    }
    counts.retain(|_, count| *count != 0);
    counts
}

#[tokio::test]
async fn streaming_values_and_occurrences_match_independent_bags_in_both_directions() {
    let root = tempfile::tempdir().unwrap();
    let (engine, before, original) = scalar::fixture(root.path(), false).await;
    let old = endpoint(&engine, &before).await;
    // Identical duplicates, null and empty values, shrinkage, insertion, and a
    // same-value replacement whose birth identity still changes.
    let replacements = vec![
        (Some(1), Some("duplicate".into())),
        (Some(1), Some("duplicate".into())),
        (None, None),
        (Some(4096), Some(String::new())),
    ];
    let after = engine
        .update(&UpdateSpec {
            dataset: before.dataset.dataset.clone(),
            operation: scalar::id("changes"),
            definition: "v1".into(),
            base: before.id.clone(),
            keys: vec!["id".into()],
            replacements: scalar::input(&root.path().join("replacements.parquet"), &replacements),
            deletions: None,
        })
        .await
        .unwrap();
    let mut expected_values = original
        .iter()
        .filter(|row| row.0.is_some() && row.0 != Some(1))
        .cloned()
        .collect::<Vec<_>>();
    expected_values.extend(replacements);
    let new = endpoint(&engine, &after).await;
    assert!(
        difference(
            &expected_values,
            &new.iter().map(|row| row.2.clone()).collect::<Vec<_>>()
        )
        .is_empty()
    );

    for (first, second, first_members, second_members, first_values, second_values) in [
        (&before, &after, &old, &new, &original, &expected_values),
        (&after, &before, &new, &old, &expected_values, &original),
    ] {
        for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
            let options = ComparisonOptions {
                identity,
                force_full: false,
            };
            let (description, rows) = changes(&engine, first, second, &options).await;
            assert_eq!(description.before, first.version_ref());
            assert_eq!(description.after, second.version_ref());
            assert_eq!(description.identity, identity);
            assert_eq!(description.path, ComparisonPath::Full);
            assert_eq!(
                description.fallback,
                Some(ComparisonFallback::HistoryNotQualified)
            );
            assert_eq!(description.field_ids, vec![1, 2]);
            for (side, member) in &rows {
                assert!(
                    if side == "before" {
                        first_members
                    } else {
                        second_members
                    }
                    .contains(member)
                );
            }
            let actual_before = rows
                .iter()
                .filter(|row| row.0 == "before")
                .map(|row| row.1.clone())
                .collect::<Vec<_>>();
            let actual_after = rows
                .iter()
                .filter(|row| row.0 == "after")
                .map(|row| row.1.clone())
                .collect::<Vec<_>>();
            if identity == ComparisonIdentity::Values {
                let a = actual_before
                    .iter()
                    .map(|row| row.2.clone())
                    .collect::<Vec<_>>();
                let b = actual_after
                    .iter()
                    .map(|row| row.2.clone())
                    .collect::<Vec<_>>();
                assert_eq!(difference(&a, &b), difference(first_values, second_values));
                assert!(
                    a.iter().all(|row| !b.contains(row)),
                    "equal values must cancel"
                );
            } else {
                assert_eq!(
                    difference(&actual_before, &actual_after),
                    difference(first_members, second_members)
                );
                assert!(actual_before.iter().all(|row| !actual_after.contains(row)));
            }
            let counts = engine
                .compare_with_options(first, second, "source", &options)
                .await
                .unwrap();
            assert_eq!(counts.descriptor, description);
            assert_eq!(counts.added_rows, Some(actual_after.len() as u64));
            assert_eq!(counts.removed_rows, Some(actual_before.len() as u64));
        }
    }
}

#[tokio::test]
async fn unchanged_pins_skip_payloads_and_force_full_preserves_read_failures() {
    let root = tempfile::tempdir().unwrap();
    let (engine, before, _) = scalar::fixture(root.path(), false).await;
    let payload = before.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    std::fs::write(&payload.path, b"broken").unwrap();
    let (description, rows) =
        changes(&engine, &before, &before, &ComparisonOptions::default()).await;
    assert_eq!(description.path, ComparisonPath::Unchanged);
    assert!(description.fallback.is_none());
    assert!(rows.is_empty());
    let forced = engine
        .changes(
            &before,
            &before,
            "source",
            &ComparisonOptions {
                force_full: true,
                ..Default::default()
            },
        )
        .await;
    match forced {
        Err(_) => {}
        Ok(stream) => assert!(stream.try_collect::<Vec<_>>().await.is_err()),
    }
}

#[tokio::test]
async fn schema_changes_are_explicit_in_counts_and_refused_for_streams() {
    let root = tempfile::tempdir().unwrap();
    let (engine, before, rows) = scalar::fixture(root.path(), false).await;
    let input = scalar::input(&root.path().join("typed.parquet"), &rows);
    let after = engine
        .run(&RunSpec {
            dataset: scalar::id("different-types"),
            operation: scalar::id("typed"),
            base: None,
            relationship: None,
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT CAST(id AS VARCHAR) AS id, text FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let counts = engine.compare(&before, &after, "source").await.unwrap();
    assert!(!counts.same_schema);
    assert_eq!((counts.added_rows, counts.removed_rows), (None, None));
    assert_eq!(
        counts.descriptor.fallback,
        Some(ComparisonFallback::SchemaChanged)
    );
    let error = match engine
        .changes(&before, &after, "source", &ComparisonOptions::default())
        .await
    {
        Ok(_) => panic!("incompatible types must refuse"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "comparison_schema_changed");
}

#[tokio::test]
async fn computed_groups_use_retained_addresses_and_report_added_definitions() {
    let root = tempfile::tempdir().unwrap();
    let (engine, source, original) = scalar::fixture(root.path(), false).await;
    let before = engine
        .backfill(&scalar::spec(
            &source,
            "derive",
            "lengths",
            "length",
            "length(text)",
        ))
        .await
        .unwrap();
    let source_change = engine.compare(&source, &before, "source").await.unwrap();
    assert_eq!(source_change.descriptor.path, ComparisonPath::Unchanged);
    assert!(source_change.descriptor.effects.evidence_refs_changed);
    assert_eq!(
        source_change.descriptor.effects.changed_group_definitions,
        vec!["lengths"]
    );
    assert_eq!(
        (source_change.added_rows, source_change.removed_rows),
        (Some(0), Some(0))
    );
    let replacement = original
        .iter()
        .find(|row| row.0 == Some(0))
        .unwrap()
        .clone();
    let after = engine
        .update(&UpdateSpec {
            dataset: before.dataset.dataset.clone(),
            operation: scalar::id("equal-values"),
            definition: "v1".into(),
            base: before.id.clone(),
            keys: vec!["id".into()],
            replacements: scalar::input(&root.path().join("same.parquet"), &[replacement]),
            deletions: None,
        })
        .await
        .unwrap();
    for (identity, expected) in [
        (ComparisonIdentity::Values, 0),
        (ComparisonIdentity::Occurrences, 2),
    ] {
        let options = ComparisonOptions {
            identity,
            force_full: true,
        };
        let stream = engine
            .changes(&before, &after, "lengths", &options)
            .await
            .unwrap();
        let description: ChangeDescriptor =
            serde_json::from_str(&stream.schema().metadata()["docspec.change"]).unwrap();
        assert_eq!(description.fallback, Some(ComparisonFallback::Forced));
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected
        );
        for batch in batches {
            let values = batch
                .column_by_name("values")
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(
                values
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["length"]
            );
        }
        let counts = engine
            .compare_with_options(&before, &after, "lengths", &options)
            .await
            .unwrap();
        assert_eq!(
            (counts.added_rows, counts.removed_rows),
            (Some(expected as u64 / 2), Some(expected as u64 / 2))
        );
    }
}

#[tokio::test]
async fn default_only_changes_report_schema_rules_without_inventing_value_changes() {
    use docspec::model::{ColumnAddition, ColumnType, SchemaField, SchemaSpec};
    let root = tempfile::tempdir().unwrap();
    let (engine, original, _) = scalar::fixture(root.path(), false).await;
    let mut edit = SchemaSpec {
        dataset: original.dataset.dataset.clone(),
        operation: scalar::id("add-default"),
        base: original.id.clone(),
        definition: "v1".into(),
        next_definition: "v2".into(),
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".into(),
            },
            SchemaField {
                id: 2,
                name: "text".into(),
            },
        ],
        additions: vec![ColumnAddition {
            name: "score".into(),
            data_type: ColumnType::Long,
            default: 7.into(),
        }],
        write_defaults: BTreeMap::new(),
    };
    let before = engine.evolve_schema(&edit).await.unwrap();
    edit.operation = scalar::id("change-default");
    edit.base = before.id.clone();
    edit.definition = "v2".into();
    edit.next_definition = "v3".into();
    edit.additions.clear();
    edit.fields.push(SchemaField {
        id: 3,
        name: "score".into(),
    });
    edit.write_defaults.insert(3, 9.into());
    let after = engine.evolve_schema(&edit).await.unwrap();
    let result = engine.compare(&before, &after, "source").await.unwrap();
    assert!(!result.same_schema);
    assert!(result.descriptor.effects.schema_changed);
    assert!(result.descriptor.comparable_values);
    assert_eq!((result.added_rows, result.removed_rows), (Some(0), Some(0)));
    let stream = engine
        .changes(&before, &after, "source", &ComparisonOptions::default())
        .await
        .unwrap();
    assert!(
        stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
}

#[tokio::test]
async fn returned_change_stream_holds_maintenance_admission_until_drop() {
    let root = tempfile::tempdir().unwrap();
    let (engine, before, _) = scalar::fixture(root.path(), false).await;
    let records = engine.inspect(&before.dataset.dataset).await.unwrap();
    let key = records
        .keys()
        .find(|key| key.starts_with("phase/"))
        .unwrap()
        .clone();
    let plan = engine
        .plan_phase_retirement(
            &before.dataset.dataset,
            &scalar::id("retire-phase"),
            &[key],
            "finish recovery promise",
        )
        .await
        .unwrap();
    let stream = engine
        .changes(&before, &before, "source", &ComparisonOptions::default())
        .await
        .unwrap();
    let retirement = engine.retire_phases(&plan);
    tokio::pin!(retirement);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut retirement)
            .await
            .is_err()
    );
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(10), retirement)
        .await
        .unwrap()
        .unwrap();
}
