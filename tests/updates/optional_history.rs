//! Complete states survive removal of independently retired applied-write history.

#![cfg(unix)]

use super::*;
use docspec::model::{
    AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition,
    ComparisonIdentity, ComparisonOptions, ComparisonPath, RefreshMode, RefreshPolicy, RefreshSpec,
    RefreshTransform, RemovalOutcome,
};

#[tokio::test]
async fn removed_optional_deltas_preserve_complete_state_and_independent_consumers() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, rows) = fixture(root.path()).await;
    let replacements = vec![
        (Some(1), Some("replacement".into())),
        (Some(1000), Some("new".into())),
    ];
    let deletions = vec![(Some(2), None)];
    let spec = update(root.path(), "initial", "changed", &replacements, &deletions);
    let after = engine.update(&spec).await.unwrap();
    let expected = reconstruction(&rows, &replacements, &deletions);
    let certified = engine
        .change_certificate(&after.version_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certified.certificate.before, before.version_ref());
    assert_eq!(certified.certificate.after, after.version_ref());
    assert!(certified.revision > 0);
    let changes = certified.changes.source.as_ref().unwrap();
    let endpoint_paths = before.dataset.groups["source"]
        .objects
        .iter()
        .chain(&after.dataset.groups["source"].objects)
        .map(|object| object.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut delta_files = changes
        .removed
        .objects
        .iter()
        .chain(&changes.added.objects)
        .filter(|object| {
            object.path.ends_with(".parquet") && !endpoint_paths.contains(object.path.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    delta_files.sort_by(|left, right| left.path.cmp(&right.path));
    delta_files.dedup();
    assert!(!delta_files.is_empty());
    // The bounded descriptor has its own creation evidence and cleanup lifetime.
    delta_files.push(certified.certificate.descriptor.clone());
    assert!(
        delta_files
            .iter()
            .all(|object| Path::new(&object.path).exists())
    );

    let optional = engine
        .plan_change_retirement(
            &spec.dataset,
            &id("optional-history"),
            std::slice::from_ref(&after.id),
            "end optional applied-write comparison acceleration",
        )
        .await
        .unwrap();
    let retired = engine.retire_changes(&optional).await.unwrap();
    assert_eq!(engine.retire_changes(&optional).await.unwrap(), retired);
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    let blocked = engine
        .plan_removal(
            &spec.dataset,
            &id("still-required"),
            &delta_files,
            "check independent recovery protection",
        )
        .await
        .unwrap();
    assert!(
        blocked
            .inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let blocked = engine.remove_objects(&blocked).await.unwrap();
    assert!(blocked.intent.objects.is_empty());
    assert_eq!(blocked.confirmed_unlinked_bytes, 0);
    assert!(
        delta_files
            .iter()
            .all(|object| Path::new(&object.path).exists())
    );

    // Ending optional preparation preserves the current processing and validation
    // promises. Their exact changes are equal; inventory reports the first root.
    let prepared = engine
        .plan_phase_retirement(
            &spec.dataset,
            &id("prepared-history"),
            &[format!("prepared-changes/{}", spec.operation)],
            "end optional descriptor preparation recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&prepared).await.unwrap();
    let protected = engine.inspect_retention(&delta_files).await.unwrap();
    assert!(
        protected
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let retained = engine.inspect(&spec.dataset).await.unwrap();
    assert!(protected.objects.iter().any(|object| {
        object.blocker.as_ref().is_some_and(|blocker| {
            blocker.key.starts_with("phase/")
                && retained.get(&blocker.key).is_some_and(|value| {
                    let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
                    phase.operation == spec.operation
                        && matches!(phase.phase.as_str(), "processing" | "validation")
                        && phase.changes.as_ref() == Some(changes)
                })
        })
    }));
    let phase_keys = retained
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
            &id("update-phases"),
            &phase_keys,
            "end update phase recovery while retaining the complete result",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &spec.dataset,
            &id("update-recovery"),
            &spec.operation,
            "end terminal update attempt reservations",
        )
        .await
        .unwrap();
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
            &id("unused-deltas"),
            &delta_files,
            "remove only retired applied-row files",
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
    assert_eq!(removed.intent.objects.len(), delta_files.len());
    assert!(
        removed
            .progress
            .objects
            .iter()
            .all(|outcome| *outcome == RemovalOutcome::Removed)
    );
    assert!(removed.confirmed_unlinked_bytes > 0);
    assert!(
        delta_files
            .iter()
            .all(|object| !Path::new(&object.path).exists())
    );

    // Reopen so survival cannot come from the producing process's cached state.
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), after);
    assert_eq!(read(&after.dataset.groups["source"]).await, expected);
    let batches = engine
        .read_group(&after, "source")
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected.len()
    );
    verify_release(&after, limits()).await.unwrap();
    assert_eq!(engine.update(&spec).await.unwrap(), after);
    // Valid Parquet and an internally consistent manifest cannot substitute the
    // wrong complete values for the retained update request.
    let wrong = replaced_source(
        root.path(),
        "wrong-values",
        &after,
        &before.dataset.groups["source"],
    );
    assert_eq!(
        verify_release(&wrong, limits()).await.unwrap_err().code(),
        "invalid_changes"
    );
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    engine
        .prove_succession(&before.version_ref(), &after.version_ref())
        .await
        .unwrap();
    let comparison = engine
        .compare_with_options(
            &before,
            &after,
            "source",
            &ComparisonOptions {
                identity: ComparisonIdentity::Values,
                force_full: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(comparison.descriptor.path, ComparisonPath::Full);
    assert_eq!(
        (comparison.removed_rows, comparison.added_rows),
        (Some(3), Some(2))
    );

    let summary = engine
        .refresh(&RefreshSpec {
            dataset: id("summary"),
            operation: id("from-retired-history"),
            base: None,
            definition: "count-v1".into(),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: after.version_ref(),
                grouping: AggregationDefinition {
                    keys: vec![],
                    aggregates: vec![AggregateField {
                        name: "rows".into(),
                        function: AggregateFunction::CountRows,
                    }],
                    filter: None,
                    projections: std::collections::BTreeMap::new(),
                },
                filter: None,
                projections: std::collections::BTreeMap::new(),
            }),
        })
        .await
        .unwrap();
    let summary_batches = engine
        .read_group(&summary, "source")
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        summary_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        summary_batches[0]
            .column_by_name("rows")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected.len() as i64
    );
    verify_release(&summary, limits()).await.unwrap();

    let wrong = replaced_source(
        root.path(),
        "wrong-schema",
        &after,
        &summary.dataset.groups["source"],
    );
    assert_eq!(
        docspec::engine::verify_release_at(
            &wrong,
            limits(),
            docspec::model::VerificationLevel::Routine
        )
        .await
        .unwrap_err()
        .code(),
        "invalid_changes"
    );

    let point = engine
        .create_restore_point(&spec.dataset, &id("without-history"))
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    let workspace = root.path().join("workspace");
    Engine::restore_local(&workspace, &point, limits(), false)
        .await
        .unwrap();
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), after);
    assert!(
        engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    verify_release(&after, limits()).await.unwrap();

    // Equal values with a rewritten untouched birth must still refuse.
    let untouched_key = expected
        .iter()
        .find(|row| !matches!(row.0, Some(1 | 2 | 1000)))
        .unwrap()
        .0;
    let same_rows = expected
        .iter()
        .filter(|row| row.0 == untouched_key)
        .cloned()
        .collect::<Vec<_>>();
    let rewritten = engine
        .update(&update(
            root.path(),
            after.id.as_str(),
            "rewritten-birth",
            &same_rows,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(read(&rewritten.dataset.groups["source"]).await, expected);
    let wrong = replaced_source(
        root.path(),
        "wrong-birth",
        &after,
        &rewritten.dataset.groups["source"],
    );
    assert_eq!(
        verify_release(&wrong, limits()).await.unwrap_err().code(),
        "invalid_changes"
    );

    // Required current-state bytes still receive independent integrity checks.
    let current_file = after.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let mut bytes = std::fs::read(&current_file.path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&current_file.path, bytes).unwrap();
    assert!(verify_release(&after, limits()).await.is_err());
    assert!(verify_release(&summary, limits()).await.is_err());
}

#[tokio::test]
async fn local_certificates_require_the_exact_result_revision_and_well_formed_endpoints() {
    use docspec::catalog::{Lifecycle, record};

    for fault in ["revision", "malformed", "endpoint"] {
        let root = TempDir::new().unwrap();
        let (engine, _, before, _) = fixture(root.path()).await;
        let spec = update(root.path(), "initial", "changed", &[(Some(1), None)], &[]);
        let after = engine.update(&spec).await.unwrap();
        let certified = engine
            .change_certificate(&after.version_ref())
            .await
            .unwrap()
            .unwrap();
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        assert_eq!(certified.revision, view.revision);
        assert_eq!(certified.certificate.after, after.version_ref());
        let key = format!("change-certificate/{}", after.id);
        let mut certificate = certified.certificate;
        let records = match fault {
            // A plausible certificate committed later than the exact result is
            // not qualified by that earlier result's successful selection.
            "revision" => vec![record(&key, certificate).unwrap()],
            "malformed" => vec![
                record(&key, serde_json::json!({"unexpected": true})).unwrap(),
                record(format!("result/{}", after.id), &after).unwrap(),
            ],
            _ => {
                certificate.after.manifest = before.manifest.clone();
                vec![
                    record(&key, certificate).unwrap(),
                    record(format!("result/{}", after.id), &after).unwrap(),
                ]
            }
        };
        lifecycle.commit(&view, records).await.unwrap();
        assert!(
            engine
                .change_certificate(&after.version_ref())
                .await
                .is_err()
        );
        // Corrupt optional metadata must not redefine the complete result.
        verify_release(&after, limits()).await.unwrap();
        assert_eq!(engine.update(&spec).await.unwrap(), after);
    }
}

fn replaced_source(
    root: &Path,
    name: &str,
    release: &docspec::model::ReleasePin,
    source: &TablePin,
) -> docspec::model::ReleasePin {
    let mut result = release.clone();
    result
        .dataset
        .groups
        .insert("source".into(), source.clone());
    let path = root.join(format!("{name}.json"));
    let bytes = serde_json::to_vec(&result.dataset).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    result.manifest = docspec::model::ObjectRef {
        owner: result.dataset.dataset.clone(),
        path: path.to_string_lossy().into_owned(),
        digest: blake3::hash(&bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    };
    result
}
