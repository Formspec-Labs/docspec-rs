//! Forward history agrees with independent endpoint bags and forced full streams.
// Rust guideline compliant 2026-02-21

use super::*;
use docspec::model::CompactionSpec;
use std::path::Path;

async fn update(
    engine: &Engine,
    root: &Path,
    base: &ReleasePin,
    name: &str,
    rows: &[Row],
) -> ReleasePin {
    engine
        .update(&UpdateSpec {
            dataset: base.dataset.dataset.clone(),
            operation: scalar::id(name),
            definition: base.dataset.definition.clone(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: scalar::input(&root.join(format!("{name}.parquet")), rows),
            deletions: None,
        })
        .await
        .unwrap()
}

async fn check(
    engine: &Engine,
    before: &ReleasePin,
    after: &ReleasePin,
    expected_values: &[Row],
    path: ComparisonPath,
    fallback: Option<ComparisonFallback>,
) {
    let old = endpoint(engine, before).await;
    let new = endpoint(engine, after).await;
    assert!(
        difference(
            expected_values,
            &new.iter().map(|row| row.2.clone()).collect::<Vec<_>>()
        )
        .is_empty()
    );
    for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
        let options = ComparisonOptions {
            identity,
            force_full: false,
        };
        let (descriptor, rows) = changes(engine, before, after, &options).await;
        let forced = ComparisonOptions {
            identity,
            force_full: true,
        };
        let (full_descriptor, full_rows) = changes(engine, before, after, &forced).await;
        assert_eq!(descriptor.path, path);
        assert_eq!(descriptor.fallback, fallback);
        assert_eq!(descriptor.before, before.version_ref());
        assert_eq!(descriptor.after, after.version_ref());
        assert_eq!(descriptor.identity, identity);
        assert_eq!(descriptor.field_ids, vec![1, 2]);
        assert_eq!(full_descriptor.path, ComparisonPath::Full);
        assert_eq!(full_descriptor.fallback, Some(ComparisonFallback::Forced));
        assert_eq!(descriptor.effects, full_descriptor.effects);
        assert_eq!(
            rows, full_rows,
            "full and accelerated streams must select identical endpoint addresses"
        );
        for (side, member) in &rows {
            assert!(if side == "before" { &old } else { &new }.contains(member));
        }
        let removed = rows
            .iter()
            .filter(|row| row.0 == "before")
            .map(|row| row.1.clone())
            .collect::<Vec<_>>();
        let added = rows
            .iter()
            .filter(|row| row.0 == "after")
            .map(|row| row.1.clone())
            .collect::<Vec<_>>();
        if identity == ComparisonIdentity::Values {
            let a = removed.iter().map(|row| row.2.clone()).collect::<Vec<_>>();
            let b = added.iter().map(|row| row.2.clone()).collect::<Vec<_>>();
            assert_eq!(
                difference(&a, &b),
                difference(
                    &old.iter().map(|row| row.2.clone()).collect::<Vec<_>>(),
                    expected_values
                )
            );
            assert!(
                a.iter().all(|row| !b.contains(row)),
                "equal values must cancel"
            );
        } else {
            assert_eq!(difference(&removed, &added), difference(&old, &new));
            assert!(removed.iter().all(|row| !added.contains(row)));
        }
        for options in [&options, &forced] {
            let counts = engine
                .compare_with_options(before, after, "source", options)
                .await
                .unwrap();
            assert_eq!(counts.added_rows, Some(added.len() as u64));
            assert_eq!(counts.removed_rows, Some(removed.len() as u64));
            assert_eq!(
                counts.descriptor,
                if options.force_full {
                    full_descriptor.clone()
                } else {
                    descriptor.clone()
                }
            );
        }
    }
}

#[tokio::test]
async fn reversions_duplicates_nulls_and_compacted_updates_match_exact_full_endpoints() {
    let root = tempfile::tempdir().unwrap();
    let (engine, before, original) = scalar::fixture(root.path(), false).await;
    let changed = update(
        &engine,
        root.path(),
        &before,
        "first",
        &[
            (Some(0), Some("temporary".into())),
            (Some(1), Some("duplicate".into())),
            (Some(1), Some("duplicate".into())),
            (None, Some("temporary null".into())),
            (Some(4096), Some(String::new())),
        ],
    )
    .await;
    engine
        .compact(&CompactionSpec {
            dataset: changed.dataset.dataset.clone(),
            operation: scalar::id("compact-middle"),
            version: changed.id.clone(),
            group: "source".into(),
            files: vec![],
            target_file_bytes: 16 << 20,
        })
        .await
        .unwrap();
    let mut final_rows = original
        .iter()
        .filter(|row| row.0 == Some(0) || row.0 == Some(9))
        .cloned()
        .collect::<Vec<_>>();
    final_rows.extend([
        (Some(1), Some("duplicate".into())),
        (None, None),
        (Some(4096), Some(String::new())),
        (Some(4097), None),
    ]);
    let after = update(&engine, root.path(), &changed, "final", &final_rows).await;
    let mut expected = original
        .iter()
        .filter(|row| row.0.is_some() && ![Some(0), Some(1), Some(9)].contains(&row.0))
        .cloned()
        .collect::<Vec<_>>();
    expected.extend(final_rows);
    check(
        &engine,
        &before,
        &after,
        &expected,
        ComparisonPath::ForwardChanges,
        None,
    )
    .await;

    // Optional history uses retained local result identity even after its own
    // complete-result availability ends.
    let availability = engine
        .plan_availability_retirement(
            &before.dataset.dataset,
            &scalar::id("retire-middle-availability"),
            &changed.id,
            "end independent middle-result lookup",
        )
        .await
        .unwrap();
    engine.retire_availability(&availability).await.unwrap();
    check(
        &engine,
        &before,
        &after,
        &expected,
        ComparisonPath::ForwardChanges,
        None,
    )
    .await;
    let history = engine
        .plan_change_retirement(
            &before.dataset.dataset,
            &scalar::id("retire-middle-history"),
            std::slice::from_ref(&changed.id),
            "end optional middle-hop acceleration",
        )
        .await
        .unwrap();
    engine.retire_changes(&history).await.unwrap();
    check(
        &engine,
        &before,
        &after,
        &expected,
        ComparisonPath::Full,
        Some(ComparisonFallback::HistoryNotQualified),
    )
    .await;
}

#[tokio::test]
async fn receipt_only_middle_hop_preserves_evidence_effects_and_corrupt_history_refuses() {
    use arrow::datatypes::{DataType, Field, Schema};
    use docspec::{
        evidence::{EVIDENCE, EvidenceSpec, SOURCE_FILE, SOURCE_ROW},
        model::InputSpec,
    };
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let (engine, before, original) = scalar::fixture(root.path(), false).await;
    let first = update(
        &engine,
        root.path(),
        &before,
        "first",
        &[(Some(0), Some("first".into()))],
    )
    .await;
    let member = endpoint(&engine, &first)
        .await
        .into_iter()
        .find(|row| row.2.0 == Some(2))
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(SOURCE_FILE, DataType::Utf8, false),
        Field::new(SOURCE_ROW, DataType::Int64, false),
        Field::new(EVIDENCE, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![member.0.as_str()])),
            Arc::new(Int64Array::from(vec![member.1])),
            Arc::new(StringArray::from(vec![Some("middle-hop evidence")])),
        ],
    )
    .unwrap();
    let path = root.path().join("receipt.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let middle = engine
        .update_evidence(&EvidenceSpec {
            dataset: first.dataset.dataset.clone(),
            operation: scalar::id("receipt-only"),
            definition: "v1".into(),
            base: first.id.clone(),
            replacements: InputSpec {
                path: path.to_string_lossy().into_owned(),
                digest: storage::digest(&path).unwrap().0,
            },
            deletions: None,
        })
        .await
        .unwrap();
    assert_eq!(middle.dataset.groups, first.dataset.groups);
    let bridge = engine
        .change_certificate(&middle.version_ref())
        .await
        .unwrap()
        .unwrap();
    assert!(bridge.changes.source.is_none());
    assert!(bridge.changes.receipts.is_some());
    let evidence_only = engine.compare(&first, &middle, "source").await.unwrap();
    assert_eq!(evidence_only.descriptor.path, ComparisonPath::Unchanged);
    assert!(evidence_only.descriptor.effects.evidence_refs_changed);
    assert_eq!(
        (evidence_only.added_rows, evidence_only.removed_rows),
        (Some(0), Some(0))
    );
    let after = update(
        &engine,
        root.path(),
        &middle,
        "last",
        &[(Some(0), Some("last".into()))],
    )
    .await;
    let expected = original
        .iter()
        .map(|row| {
            if row.0 == Some(0) {
                (Some(0), Some("last".into()))
            } else {
                row.clone()
            }
        })
        .collect::<Vec<_>>();
    check(
        &engine,
        &before,
        &after,
        &expected,
        ComparisonPath::ForwardChanges,
        None,
    )
    .await;
    let counts = engine.compare(&before, &after, "source").await.unwrap();
    assert!(counts.descriptor.effects.evidence_refs_changed);

    // Damage an endpoint-only data file: qualified discovery must read the
    // retained deltas, while forced full comparison must still open endpoints.
    let mut delta_paths = std::collections::BTreeSet::new();
    for release in [&first, &middle, &after] {
        let certified = engine
            .change_certificate(&release.version_ref())
            .await
            .unwrap()
            .unwrap();
        for pin in certified
            .changes
            .source
            .iter()
            .chain(&certified.changes.receipts)
        {
            for object in pin.removed.objects.iter().chain(&pin.added.objects) {
                delta_paths.insert(object.path.clone());
            }
        }
    }
    let payload = before.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet") && !delta_paths.contains(&object.path))
        .expect("fixture needs an endpoint payload outside every retained delta");
    let mut healthy_changes = Vec::new();
    for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
        let options = ComparisonOptions {
            identity,
            force_full: false,
        };
        healthy_changes.push((identity, changes(&engine, &before, &after, &options).await));
    }
    let healthy_payload = std::fs::read(&payload.path).unwrap();
    std::fs::write(&payload.path, b"broken endpoint payload").unwrap();
    for (identity, healthy) in healthy_changes {
        let (descriptor, rows) = changes(
            &engine,
            &before,
            &after,
            &ComparisonOptions {
                identity,
                force_full: false,
            },
        )
        .await;
        assert_eq!(descriptor.path, ComparisonPath::ForwardChanges);
        assert_eq!((descriptor, rows), healthy);
        let forced = engine
            .changes(
                &before,
                &after,
                "source",
                &ComparisonOptions {
                    identity,
                    force_full: true,
                },
            )
            .await;
        match forced {
            Err(_) => {}
            Ok(stream) => assert!(stream.try_collect::<Vec<_>>().await.is_err()),
        }
    }
    std::fs::write(&payload.path, healthy_payload).unwrap();

    let mut full_results = Vec::new();
    for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
        let options = ComparisonOptions {
            identity,
            force_full: true,
        };
        full_results.push((identity, changes(&engine, &before, &after, &options).await));
    }
    // A present descriptor with changed bytes is corruption, not absent history.
    std::fs::write(&bridge.certificate.descriptor.path, b"broken descriptor").unwrap();
    for (identity, expected_full) in full_results {
        let error = match engine
            .changes(
                &before,
                &after,
                "source",
                &ComparisonOptions {
                    identity,
                    force_full: false,
                },
            )
            .await
        {
            Ok(_) => panic!("damaged present history must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "corrupt_object");
        let options = ComparisonOptions {
            identity,
            force_full: true,
        };
        let (descriptor, rows) = changes(&engine, &before, &after, &options).await;
        assert_eq!((&descriptor, &rows), (&expected_full.0, &expected_full.1));
        assert_eq!(descriptor.path, ComparisonPath::Full);
        assert_eq!(descriptor.fallback, Some(ComparisonFallback::Forced));
        let old = endpoint(&engine, &before).await;
        let new = endpoint(&engine, &after).await;
        assert!(
            rows.iter()
                .all(|(side, member)| if side == "before" { &old } else { &new }.contains(member))
        );
        let counts = engine
            .compare_with_options(&before, &after, "source", &options)
            .await
            .unwrap();
        assert_eq!(
            counts.added_rows,
            Some(rows.iter().filter(|row| row.0 == "after").count() as u64)
        );
        assert_eq!(
            counts.removed_rows,
            Some(rows.iter().filter(|row| row.0 == "before").count() as u64)
        );
    }
}
