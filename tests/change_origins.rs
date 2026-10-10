//! Source changes retain exact occurrence addresses without receipt associations.

#[path = "support/scalar.rs"]
#[expect(
    dead_code,
    reason = "shared fixture includes scalar backfill scenarios"
)]
mod scalar;

use arrow::array::{Int64Array, StringArray};
use docspec::{
    budget::Budget,
    engine::verify_release,
    model::{
        CompactionSpec, JoinCardinality, JoinDefinition, JoinField, JoinKey, JoinKind, JoinSide,
        JoinTransform, NullMatching, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform,
        ReleasePin, RunSpec, TablePin, UpdateSpec,
    },
    storage,
};
use futures::{StreamExt, TryStreamExt};
use scalar::{fixture, id, limits};

type Occurrence = (String, i64, Option<i64>, Option<String>);

fn update(
    root: &std::path::Path,
    base: &ReleasePin,
    operation: &str,
    rows: &[scalar::Row],
) -> UpdateSpec {
    UpdateSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        definition: base.dataset.definition.clone(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: scalar::input(&root.join(format!("{operation}.parquet")), rows),
        deletions: None,
    }
}

fn join(
    left: &ReleasePin,
    right: &ReleasePin,
    base: Option<&ReleasePin>,
    operation: &str,
) -> RefreshSpec {
    RefreshSpec {
        dataset: id("joined"),
        operation: id(operation),
        base: base.map(|release| release.id.clone()),
        definition: "join-v1".into(),
        policy: RefreshPolicy::Captured,
        mode: if base.is_some() {
            RefreshMode::Incremental
        } else {
            RefreshMode::Full
        },
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Never,
                cardinality: JoinCardinality::ManyToMany,
                fields: ["id", "text"]
                    .into_iter()
                    .map(|field| JoinField {
                        side: JoinSide::Left,
                        field: field.into(),
                        name: field.into(),
                    })
                    .collect(),
            },
        }),
    }
}

#[tokio::test]
async fn receipt_free_related_refresh_preserves_partial_file_survivors_and_noop() {
    let root = tempfile::tempdir().unwrap();
    let (engine, left, rows) = fixture(root.path(), false).await;
    let input = scalar::input(&root.path().join("partners.parquet"), &rows);
    let right = engine
        .run(&RunSpec {
            dataset: id("partners"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let base = engine
        .refresh(&join(&left, &right, None, "initial"))
        .await
        .unwrap();
    assert!(base.dataset.evidence.is_none());
    let source = &base.dataset.groups["source"];
    let original = occurrences(source).await;
    let affected = original.iter().find(|row| row.2 == Some(2)).unwrap();
    assert!(
        original
            .iter()
            .any(|row| row.0 == affected.0 && row.2 != Some(2)),
        "fixture must rewrite a file containing surviving joined rows"
    );
    let native = storage::open_pin(source).await.unwrap();
    assert!(
        native
            .metadata()
            .current_schema()
            .field_by_name("__docspec_origin_file")
            .is_none()
    );

    // A new unmatched input changes coverage and enters the replacement path,
    // but applies no joined rows. Preserve the exact source and change schema.
    let unmatched = engine
        .update(&update(
            root.path(),
            &left,
            "unmatched",
            &[(Some(9000), None)],
        ))
        .await
        .unwrap();
    let noop = engine
        .refresh(&join(&unmatched, &right, Some(&base), "unmatched"))
        .await
        .unwrap();
    assert_eq!(&noop.dataset.groups["source"], source);
    let changes = engine
        .change_certificate(&noop.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    assert_eq!((changes.removed.rows, changes.added.rows), (0, 0));
    let empty = storage::open_pin(&changes.removed).await.unwrap();
    assert_eq!(
        empty.metadata().current_schema(),
        native.metadata().current_schema()
    );
    verify_release(&noop, limits()).await.unwrap();

    let left = engine
        .update(&update(
            root.path(),
            &unmatched,
            "change-member",
            &[(Some(2), Some("changed".into()))],
        ))
        .await
        .unwrap();
    let changed = engine
        .refresh(&join(&left, &right, Some(&noop), "change-member"))
        .await
        .unwrap();
    assert!(changed.dataset.evidence.is_none());
    let current = occurrences(&changed.dataset.groups["source"]).await;
    // Discovery conservatively recomputes all keys from rewritten producer
    // files, including equal-valued contributor groups. Those applied groups
    // receive new births; rows kept outside that scope retain their identities.
    let changes = engine
        .change_certificate(&changed.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    let removed = occurrences(&changes.removed).await;
    assert!(!removed.is_empty());
    assert!(removed.iter().all(|row| original.contains(row)));
    assert!(removed.contains(affected));
    let removed_addresses: std::collections::BTreeSet<_> =
        removed.iter().map(|row| (row.0.clone(), row.1)).collect();
    assert_eq!(removed_addresses.len(), removed.len());
    let original_addresses: std::collections::BTreeSet<_> =
        original.iter().map(|row| (row.0.clone(), row.1)).collect();
    let kept: Vec<_> = original
        .iter()
        .filter(|row| !removed_addresses.contains(&(row.0.clone(), row.1)))
        .cloned()
        .collect();
    assert!(
        kept.iter().any(|row| row.0 == affected.0),
        "applied and kept members must share a physical base file"
    );
    assert_eq!(
        current
            .iter()
            .filter(|row| original_addresses.contains(&(row.0.clone(), row.1)))
            .cloned()
            .collect::<Vec<_>>(),
        kept
    );
    let added = occurrences(&changes.added).await;
    assert_eq!(
        added,
        current
            .iter()
            .filter(|row| !original_addresses.contains(&(row.0.clone(), row.1)))
            .cloned()
            .collect::<Vec<_>>()
    );
    assert_eq!(added.len(), removed.len());
    assert!(added.iter().all(|row| row.0.starts_with("docspec:")));
    let mut expected_values: Vec<_> = original
        .iter()
        .map(|row| {
            (
                row.2,
                if row.2 == Some(2) {
                    Some("changed".to_owned())
                } else {
                    row.3.clone()
                },
            )
        })
        .collect();
    let mut current_values: Vec<_> = current.iter().map(|row| (row.2, row.3.clone())).collect();
    expected_values.sort();
    current_values.sort();
    assert_eq!(current_values, expected_values);
    let replaced: Vec<_> = added.iter().filter(|row| row.2 == Some(2)).collect();
    assert_eq!(replaced.len(), 1);
    assert_eq!(
        (replaced[0].2, replaced[0].3.as_deref()),
        (Some(2), Some("changed"))
    );
    assert_ne!((&replaced[0].0, replaced[0].1), (&affected.0, affected.1));
    verify_release(&changed, limits()).await.unwrap();
}

async fn occurrences(pin: &TablePin) -> Vec<Occurrence> {
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

#[tokio::test]
async fn receipt_free_updates_preserve_original_addresses_and_new_births() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    assert!(base.dataset.evidence.is_none());
    let source = &base.dataset.groups["source"];
    let original = occurrences(source).await;

    // Preparing optional origin metadata must not change an exact no-op pin.
    let noop = engine
        .update(&update(root.path(), &base, "noop", &[]))
        .await
        .unwrap();
    assert_eq!(&noop.dataset.groups["source"], source);
    let changes = engine
        .change_certificate(&noop.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    assert_eq!((changes.removed.rows, changes.added.rows), (0, 0));
    assert_eq!(changes.before_metadata, source.metadata);
    assert_eq!(changes.after_metadata, source.metadata);
    verify_release(&noop, limits()).await.unwrap();

    // Equal displayed values still represent newly accepted source occurrences.
    // Duplicate and null-key groups exercise complete group replacement.
    let mut replacements: Vec<_> = rows
        .iter()
        .filter(|row| row.0 == Some(1) || row.0.is_none())
        .cloned()
        .collect();
    replacements.push((Some(1000), Some("new member".into())));
    let mutation = update(root.path(), &noop, "replace", &replacements);
    let changed = engine.update(&mutation).await.unwrap();
    assert!(changed.dataset.evidence.is_none());
    let changes = engine
        .change_certificate(&changed.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    let removed: Vec<_> = original
        .iter()
        .filter(|row| row.2 == Some(1) || row.2.is_none())
        .cloned()
        .collect();
    assert_eq!(occurrences(&changes.removed).await, removed);
    let current = occurrences(&changed.dataset.groups["source"]).await;
    assert_eq!(
        current
            .iter()
            .filter(|row| row.2 != Some(1) && row.2 != Some(1000) && row.2.is_some())
            .cloned()
            .collect::<Vec<_>>(),
        original
            .iter()
            .filter(|row| row.2 != Some(1) && row.2.is_some())
            .cloned()
            .collect::<Vec<_>>()
    );
    let added = occurrences(&changes.added).await;
    assert_eq!(
        added,
        current
            .iter()
            .filter(|row| row.2 == Some(1) || row.2 == Some(1000) || row.2.is_none())
            .cloned()
            .collect::<Vec<_>>()
    );
    assert!(added.iter().all(|row| {
        row.0.starts_with("docspec:")
            && !original
                .iter()
                .any(|old| (old.0.as_str(), old.1) == (row.0.as_str(), row.1))
    }));
    let mut added_values: Vec<_> = added.iter().map(|row| (row.2, row.3.clone())).collect();
    added_values.sort();
    replacements.sort();
    assert_eq!(added_values, replacements);
    verify_release(&changed, limits()).await.unwrap();
    assert_eq!(engine.update(&mutation).await.unwrap(), changed);

    let compacted = engine
        .compact(&CompactionSpec {
            dataset: changed.dataset.dataset.clone(),
            operation: id("compact"),
            version: changed.id.clone(),
            group: "source".into(),
            files: vec![],
            target_file_bytes: 16 << 20,
        })
        .await
        .unwrap();
    assert_eq!(occurrences(&compacted.replacement).await, current);

    // Later rewrites read stored origins from the compacted physical source.
    let second = engine
        .update(&update(
            root.path(),
            &changed,
            "replace-again",
            &[(Some(1), None)],
        ))
        .await
        .unwrap();
    let changes = engine
        .change_certificate(&second.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    assert_eq!(
        occurrences(&changes.removed).await,
        current
            .iter()
            .filter(|row| row.2 == Some(1))
            .cloned()
            .collect::<Vec<_>>()
    );
    verify_release(&second, limits()).await.unwrap();
}
