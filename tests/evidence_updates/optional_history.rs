//! Receipt state survives removal of optional deltas and their descriptor.

#![cfg(unix)]

use super::*;
use docspec::model::RemovalOutcome;

async fn source_values(engine: &Engine, release: &ReleasePin) -> Vec<(i64, Option<String>)> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
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
        rows.extend(
            ids.iter()
                .zip(text.iter())
                .map(|(value, text)| (value.unwrap(), text.map(str::to_owned))),
        );
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn terminal_recovery_retirement_releases_receipt_history_without_losing_receipts() {
    let root = TempDir::new().unwrap();
    let (engine, initial) = fixture(root.path()).await;
    let positions = locations(&engine, &initial).await;
    let expected_source = source_values(&engine, &initial).await;
    let original_receipts = vec![
        (positions[0].1.clone(), Some("old".into())),
        (positions[0].1.clone(), Some("duplicate".into())),
        (positions[0].1.clone(), Some("duplicate".into())),
        (positions[1].1.clone(), None),
        (positions[2].1.clone(), Some("keep".into())),
    ];
    let before = engine
        .update_evidence(&request(
            root.path(),
            &initial,
            "attached",
            &original_receipts,
            &[],
        ))
        .await
        .unwrap();
    let replacements = vec![
        (positions[0].1.clone(), None),
        (positions[0].1.clone(), Some("".into())),
        (positions[0].1.clone(), Some("new".into())),
        (positions[0].1.clone(), Some("new".into())),
    ];
    let spec = request(
        root.path(),
        &before,
        "receipt-update",
        &replacements,
        &[(positions[1].1.clone(), None)],
    );
    let after = engine.update_evidence(&spec).await.unwrap();
    let mut expected_receipts = replacements;
    expected_receipts.push((positions[2].1.clone(), Some("keep".into())));
    expected_receipts.sort();
    assert_eq!(after.dataset.groups, before.dataset.groups);
    assert_eq!(receipts(&engine, &after).await, expected_receipts);
    let certified = engine
        .change_certificate(&after.version_ref())
        .await
        .unwrap()
        .unwrap();
    assert!(certified.changes.source.is_none());
    let changes = certified.changes.receipts.as_ref().unwrap();
    assert_eq!((changes.removed.rows, changes.added.rows), (4, 4));
    let endpoint_paths = [&before, &after]
        .into_iter()
        .flat_map(|release| {
            release
                .dataset
                .groups
                .values()
                .chain(std::iter::once(
                    &release.dataset.evidence.as_ref().unwrap().table,
                ))
                .flat_map(|table| table.objects.iter())
                .map(|object| object.path.as_str())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut candidates = changes
        .removed
        .objects
        .iter()
        .chain(&changes.added.objects)
        .filter(|object| {
            object.path.ends_with(".parquet") && !endpoint_paths.contains(object.path.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!candidates.is_empty());
    assert!(!endpoint_paths.contains(certified.certificate.descriptor.path.as_str()));
    candidates.push(certified.certificate.descriptor.clone());
    candidates.sort_by(|left, right| left.path.cmp(&right.path));
    candidates.dedup();
    assert!(
        candidates
            .iter()
            .all(|object| Path::new(&object.path).exists())
    );

    let optional = engine
        .plan_change_retirement(
            &spec.dataset,
            &id("receipt-history"),
            std::slice::from_ref(&after.id),
            "end optional receipt acceleration",
        )
        .await
        .unwrap();
    engine.retire_changes(&optional).await.unwrap();
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .inspect_retention(&candidates)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );

    let phase_keys = engine
        .inspect(&spec.dataset)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value).unwrap();
            (phase.operation == spec.operation).then_some(key)
        })
        .collect::<Vec<_>>();
    assert!(!phase_keys.is_empty());
    let phases = engine
        .plan_phase_retirement(
            &spec.dataset,
            &id("receipt-phases"),
            &phase_keys,
            "end receipt phase recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &spec.dataset,
            &id("receipt-operation"),
            &spec.operation,
            "end terminal receipt recovery including optional preparation",
        )
        .await
        .unwrap();
    assert!(
        recovery
            .records
            .iter()
            .any(|record| { record.key == format!("prepared-changes/{}", spec.operation) })
    );
    assert!(
        recovery
            .records
            .iter()
            .any(|record| record.key.starts_with("protection/"))
    );
    engine.retire_operation(&recovery).await.unwrap();
    let removal = engine
        .plan_removal(
            &spec.dataset,
            &id("receipt-delta-files"),
            &candidates,
            "remove independently retired receipt delta files and descriptor",
        )
        .await
        .unwrap();
    assert!(
        removal
            .inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_none())
    );
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.intent.objects.len(), candidates.len());
    assert!(
        removed
            .progress
            .objects
            .iter()
            .all(|outcome| *outcome == RemovalOutcome::Removed)
    );
    assert!(removed.confirmed_unlinked_bytes > 0);
    assert!(
        candidates
            .iter()
            .all(|object| !Path::new(&object.path).exists())
    );
    assert!(endpoint_paths.iter().all(|path| Path::new(path).exists()));

    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), after);
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(locations(&engine, &after).await, positions);
    assert_eq!(source_values(&engine, &after).await, expected_source);
    assert_eq!(receipts(&engine, &before).await, {
        let mut expected = original_receipts;
        expected.sort();
        expected
    });
    assert_eq!(receipts(&engine, &after).await, expected_receipts);
    verify_release(&before, limits()).await.unwrap();
    verify_release(&after, limits()).await.unwrap();
    assert_eq!(engine.update_evidence(&spec).await.unwrap(), after);
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn equal_null_receipts_cannot_hide_a_changed_native_payload_type() {
    let root = TempDir::new().unwrap();
    let (engine, initial) = fixture(root.path()).await;
    let positions = locations(&engine, &initial).await;
    let expected = vec![(positions[0].1.clone(), None)];
    let attachment = request(root.path(), &initial, "attached-null", &expected, &[]);
    let attached = engine.update_evidence(&attachment).await.unwrap();
    let replacement = request(root.path(), &attached, "replaced-null", &expected, &[]);
    let replaced = engine.update_evidence(&replacement).await.unwrap();
    assert_eq!(receipts(&engine, &attached).await, expected);
    assert_eq!(receipts(&engine, &replaced).await, expected);

    // Use the normal native writer to produce a valid table with identical keys
    // and null values. A union could coerce these null payloads without exposing
    // that the table changed evidence from string to long.
    let typed = engine
        .run(&RunSpec {
            dataset: id("typed-receipts"),
            operation: id("long-payload"),
            base: None,
            relationship: None,
            input: attachment.replacements.path.clone(),
            expected_digest: attachment.replacements.digest.clone(),
            sql: format!(
                "SELECT {SOURCE_FILE}, {SOURCE_ROW}, CAST({EVIDENCE} AS BIGINT) AS {EVIDENCE} FROM input"
            ),
            definition: "typed-receipts-v1".into(),
        })
        .await
        .unwrap();
    verify_release(&typed, limits()).await.unwrap();
    let table = &typed.dataset.groups["source"];
    let native = storage::open_pin(table).await.unwrap();
    let schema =
        iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema()).unwrap();
    assert_eq!(
        schema.field_with_name(EVIDENCE).unwrap().data_type(),
        &DataType::Int64
    );
    let batches = engine
        .read_group(&typed, "source")
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column_by_name(SOURCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        positions[0].1.0
    );
    assert_eq!(
        batches[0]
            .column_by_name(SOURCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        positions[0].1.1
    );
    assert!(batches[0].column_by_name(EVIDENCE).unwrap().is_null(0));

    for (name, original, reason) in [
        (
            "initial-attachment",
            &attached,
            "receipt values changed their complete field types or nullability",
        ),
        (
            "replacement",
            &replaced,
            "receipt schema differs from its retained predecessor",
        ),
    ] {
        let mut forged = original.clone();
        forged.dataset.evidence.as_mut().unwrap().table = table.clone();
        let path = root.path().join(format!("{name}.json"));
        std::fs::write(&path, serde_json::to_vec(&forged.dataset).unwrap()).unwrap();
        let (digest, bytes) = storage::digest(&path).unwrap();
        forged.manifest = docspec::model::ObjectRef {
            owner: forged.dataset.dataset.clone(),
            path: path.to_string_lossy().into_owned(),
            digest,
            bytes,
        };
        let error = verify_release(&forged, limits()).await.unwrap_err();
        assert_eq!(error.code(), "invalid_evidence", "{name}: {error}");
        assert!(error.to_string().contains(reason), "{name}: {error}");
        verify_release(original, limits()).await.unwrap();
    }
}
