use super::*;
use crate::{
    budget::Limits,
    model::{ObjectProtection, ObjectRef, RemovalOutcome, RemovalPlan, RetentionInspection},
};
use std::path::Path;

fn test_budget(max_metadata_bytes: usize) -> Budget {
    Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 32,
        max_batch_bytes: 4096,
        max_metadata_bytes,
    })
    .unwrap()
}

fn removal_intent(id: &str) -> RemovalIntent {
    RemovalIntent {
        plan: RemovalPlan {
            scope: crate::model::RemovalScope::Objects,
            dataset: Id::new("owner").unwrap(),
            removal: Id::new(id).unwrap(),
            reason: "catalog reservation test".into(),
            table_uuid: "fixture".into(),
            inspection: RetentionInspection {
                boundaries: vec![],
                objects: vec![ObjectProtection {
                    object: ObjectRef {
                        owner: Id::new("owner").unwrap(),
                        path: format!("/fixture/{id}"),
                        digest: "0".repeat(64),
                        bytes: 1,
                    },
                    blocker: None,
                }],
                protected_bytes: 0,
                unmatched_bytes: 1,
                metadata_objects_read: 0,
            },
            registrations: vec![],
        },
        revision: 1,
        boundaries: vec![],
        blocked: vec![],
        objects: vec![0],
        registrations: vec![],
    }
}

fn removal_state(intent_revision: i64, reserved: u64) -> RemovalProgress {
    RemovalProgress {
        intent_revision,
        reserved_future_bytes: reserved,
        registrations: vec![],
        objects: vec![if reserved == 0 {
            RemovalOutcome::Removed
        } else {
            RemovalOutcome::Unresolved
        }],
    }
}

#[tokio::test]
async fn cold_and_warm_reservations_count_only_current_states() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let budget = test_budget(1 << 20);
    let lifecycle = Lifecycle::new(catalog.clone(), budget.clone());
    let view = lifecycle.open(&Id::new("owner").unwrap()).await.unwrap();
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("removal-intent/a", removal_intent("a")).unwrap(),
                record("removal-state/a", removal_state(1, 1000)).unwrap(),
                record("removal-intent/b", removal_intent("b")).unwrap(),
                record("removal-state/b", removal_state(1, 2000)).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        lifecycle.rows(&view).await.unwrap().reserved_future_bytes,
        3000
    );
    let view = lifecycle
        .commit(
            &view,
            vec![record("removal-state/a", removal_state(1, 0)).unwrap()],
        )
        .await
        .unwrap();
    let warm = lifecycle.rows(&view).await.unwrap();
    let reopened = Lifecycle::new(catalog, budget);
    let cold = reopened.rows(&view).await.unwrap();
    assert_eq!(warm.reserved_future_bytes, 2000);
    assert_eq!(cold.reserved_future_bytes, warm.reserved_future_bytes);
    assert_eq!(cold.history_bytes, warm.history_bytes);
    let view = reopened
        .commit(&view, vec![record("ordinary", true).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        reopened.rows(&view).await.unwrap().reserved_future_bytes,
        2000
    );
}

#[tokio::test]
async fn ordinary_commit_preserves_exact_terminal_headroom() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    // Historical generations fill replay capacity while the current cache
    // stays small and the bounded native writer keeps its working room.
    let earlier_commits = 8;
    let mut intent = removal_intent("a");
    intent.revision = earlier_commits + 1;
    let state_key = "removal-state/a";
    let terminal = removal_state(intent.revision, 0);
    let terminal_charge = history_row_bytes(state_key, &serde_json::to_string(&terminal).unwrap());
    let floor = crate::engine::retention::removal::validate_progress(
        &intent,
        &removal_state(intent.revision, u64::MAX),
    )
    .unwrap();
    let pending = removal_state(intent.revision, floor as u64);
    let previous = "x".repeat(2048);
    let history = earlier_commits as usize
        * history_row_bytes("prior-status", &serde_json::to_string(&previous).unwrap())
        + history_row_bytes("removal-intent/a", &serde_json::to_string(&intent).unwrap())
        + history_row_bytes(state_key, &serde_json::to_string(&pending).unwrap());
    let lifecycle = Lifecycle::new(catalog.clone(), test_budget(history + floor));
    let mut view = lifecycle.open(&Id::new("owner").unwrap()).await.unwrap();
    for _ in 0..earlier_commits {
        view = lifecycle
            .commit(&view, vec![record("prior-status", &previous).unwrap()])
            .await
            .unwrap();
    }
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("removal-intent/a", intent).unwrap(),
                record(state_key, pending).unwrap(),
            ],
        )
        .await
        .unwrap();
    // Reopen to prove the guarantee survives loss of the warm cache.
    let lifecycle = Lifecycle::new(catalog, test_budget(history + floor));
    let refused = lifecycle
        .commit(&view, vec![record("ordinary", true).unwrap()])
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "metadata_capacity");
    let view = lifecycle
        .commit(&view, vec![record(state_key, terminal).unwrap()])
        .await
        .unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    assert_eq!(rows.history_bytes, history + terminal_charge);
    assert_eq!(rows.reserved_future_bytes, 0);
}

#[tokio::test]
async fn invalid_removal_references_refuse_before_native_commit() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let lifecycle = Lifecycle::new(catalog.clone(), test_budget(1 << 20));
    let owner = Id::new("owner").unwrap();
    let view = lifecycle.open(&owner).await.unwrap();
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("removal-intent/a", removal_intent("a")).unwrap(),
                record("removal-state/a", removal_state(1, 1000)).unwrap(),
            ],
        )
        .await
        .unwrap();
    let invalid = [
        ("removal-state/a", serde_json::json!({"intent_revision": 1})),
        (
            "removal-state/a",
            serde_json::json!({"reserved_future_bytes": 1}),
        ),
        (
            "removal-state/a",
            serde_json::json!({"intent_revision": 1, "reserved_future_bytes": -1}),
        ),
        (
            "removal-state/a",
            serde_json::to_value(removal_state(0, 1)).unwrap(),
        ),
        (
            "removal-state/a",
            serde_json::to_value(removal_state(2, 1)).unwrap(),
        ),
        (
            "removal-state/missing",
            serde_json::to_value(removal_state(1, 1)).unwrap(),
        ),
        (
            "removal-state/bad/id",
            serde_json::to_value(removal_state(1, 1)).unwrap(),
        ),
        ("removal-intent/a", serde_json::json!("changed")),
        (
            "removal-intent/missing",
            serde_json::to_value(removal_intent("missing")).unwrap(),
        ),
        (
            "removal-state/a",
            serde_json::to_value(RemovalProgress {
                intent_revision: 1,
                reserved_future_bytes: 0,
                registrations: vec![],
                objects: vec![RemovalOutcome::Unresolved],
            })
            .unwrap(),
        ),
        (
            "removal-state/a",
            serde_json::to_value(RemovalProgress {
                intent_revision: 1,
                reserved_future_bytes: 1000,
                registrations: vec![],
                objects: vec![],
            })
            .unwrap(),
        ),
        (
            "removal-state/a",
            serde_json::to_value(RemovalProgress {
                intent_revision: 1,
                reserved_future_bytes: 1000,
                registrations: vec![],
                objects: vec![RemovalOutcome::Failed {
                    code: "invalid-code".into(),
                }],
            })
            .unwrap(),
        ),
        ("removal-intent/bad/id", serde_json::json!("bad")),
    ];
    for (key, value) in invalid {
        let error = lifecycle
            .commit(&view, vec![(key.into(), value)])
            .await
            .unwrap_err();
        assert_eq!(error.code(), "corrupt_metadata", "{key}");
        assert_eq!(
            lifecycle.open(&owner).await.unwrap().revision,
            view.revision
        );
    }
    let error = lifecycle
        .commit(
            &view,
            vec![record("removal-state/a", removal_state(1, u64::MAX)).unwrap()],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "metadata_capacity");
    assert_eq!(
        lifecycle.open(&owner).await.unwrap().revision,
        view.revision
    );
}

#[test]
fn cold_reservation_validation_rejects_malformed_and_dangling_states() {
    let key = "removal-state/a";
    let mut rows = BTreeMap::new();
    rows.insert(
        "removal-intent/a".into(),
        (
            1,
            Arc::<str>::from(serde_json::to_string(&removal_intent("a")).unwrap()),
        ),
    );
    let valid = serde_json::to_string(&removal_state(1, 1000)).unwrap();
    assert_eq!(removal_reservation(&rows, key, 2, &valid).unwrap(), 1000);
    for json in [
        "{}",
        "null",
        "{",
        "{\"intent_revision\":1,\"reserved_future_bytes\":\"42\"}",
    ] {
        assert_eq!(
            removal_reservation(&rows, key, 2, json).unwrap_err().code(),
            "corrupt_metadata"
        );
    }
    assert!(removal_reservation(&rows, key, 0, &valid).is_err());
    assert!(removal_reservation(&rows, "removal-state/../a", 2, &valid).is_err());
    rows.insert("removal-intent/a".into(), (2, Arc::from("{}")));
    assert!(removal_reservation(&rows, key, 2, &valid).is_err());
    rows.clear();
    rows.insert(
        "removal-intent/a".into(),
        (
            1,
            Arc::<str>::from(serde_json::to_string(&removal_intent("a")).unwrap()),
        ),
    );
    assert!(check_removal_state_exists(&rows, "removal-intent/a").is_err());
    rows.clear();
    assert!(removal_reservation(&rows, key, 2, &valid).is_err());
    assert_eq!(
        check_history_reservation(1, usize::MAX, usize::MAX)
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}

#[tokio::test]
async fn warm_commit_shares_unchanged_values_and_tracks_cache_bytes() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let lifecycle = Lifecycle::new(catalog.clone(), test_budget(1 << 20));
    let owner = Id::new("owner").unwrap();
    let view = lifecycle.open(&owner).await.unwrap();
    let view1 = lifecycle
        .commit(
            &view,
            vec![
                record("untouched", "a".repeat(512)).unwrap(),
                record("mutable", "v1").unwrap(),
            ],
        )
        .await
        .unwrap();
    let rows1 = lifecycle.rows(&view1).await.unwrap();
    let view2 = lifecycle
        .commit(&view1, vec![record("mutable", "v2-longer").unwrap()])
        .await
        .unwrap();
    let rows2 = lifecycle.rows(&view2).await.unwrap();
    // Unchanged values share the same Arc<str> allocation across commits.
    assert!(Arc::ptr_eq(
        &rows1.values["untouched"].1,
        &rows2.values["untouched"].1
    ));
    let reopened = Lifecycle::new(catalog, test_budget(1 << 20));
    let cold = reopened.rows(&view2).await.unwrap();
    assert_eq!(rows2.cache_bytes, cold.cache_bytes);
    assert_eq!(rows2.history_rows, cold.history_rows);
    assert_eq!(rows2.history_bytes, cold.history_bytes);
}

#[tokio::test]
async fn compact_baseline_coalesces_superseded_revisions_and_reclaims_replay_budget() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let payload = "x".repeat(2048);
    let single_row_charge =
        history_row_bytes("authority", &serde_json::to_string(&payload).unwrap());
    let obsolete_charge =
        history_row_bytes("obsolete/1", &serde_json::to_string(&payload).unwrap());
    // Budget allows 8 historical commits, after which a 9th ordinary commit is refused.
    let budget = test_budget(7 * single_row_charge + obsolete_charge + 64);
    let lifecycle = Lifecycle::new(catalog.clone(), budget.clone());
    let owner = Id::new("owner").unwrap();
    let mut view = lifecycle.open(&owner).await.unwrap();
    view = lifecycle
        .commit(&view, vec![record("obsolete/1", &payload).unwrap()])
        .await
        .unwrap();
    for _ in 0..7 {
        view = lifecycle
            .commit(&view, vec![record("authority", &payload).unwrap()])
            .await
            .unwrap();
    }
    let authority_rev = view.revision;
    let refused = lifecycle
        .commit(&view, vec![record("authority", &payload).unwrap()])
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "metadata_capacity");

    let remove = BTreeSet::from(["obsolete/1".to_owned()]);
    let (compacted_view, stats) = lifecycle.compact_baseline(&view, &remove).await.unwrap();
    assert_eq!(stats.revisions_before, 8);
    assert_eq!(stats.revisions_after, 1);
    assert_eq!(stats.rows_before, 8);
    assert_eq!(stats.rows_after, 1);
    assert_eq!(stats.files_before, 8);
    assert_eq!(stats.files_after, 1);
    assert_eq!(
        stats.metadata_bytes_before,
        7 * single_row_charge + obsolete_charge
    );
    assert_eq!(stats.metadata_bytes_after, single_row_charge);
    assert_eq!(stats.current_revision, authority_rev + 1);

    // Cold reopen sees 1 snapshot, 1 file, 1 row, and preserves the exact key revision.
    let reopened = Lifecycle::new(catalog, budget);
    let cold_view = reopened.open(&owner).await.unwrap();
    assert_eq!(cold_view.revision, compacted_view.revision);
    assert_eq!(cold_view.table.metadata().snapshots().count(), 1);
    assert_eq!(
        cold_view
            .table
            .metadata()
            .properties()
            .get("gc.enabled")
            .map(String::as_str),
        Some("false")
    );
    let live_files = storage::current_snapshot_files(&cold_view.table, &reopened.budget)
        .await
        .unwrap()
        .paths;
    assert_eq!(
        live_files.len(),
        4,
        "expected 1 metadata json, 1 manifest list, 1 manifest, and 1 parquet data file"
    );
    let location = PathBuf::from(cold_view.table.metadata().location());
    let mut disk_files = BTreeSet::new();
    for subdir in ["data", "metadata"] {
        for entry in std::fs::read_dir(location.join(subdir)).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                disk_files.insert(path.to_string_lossy().into_owned());
            }
        }
    }
    assert_eq!(disk_files, live_files);
    let cold_rows = reopened.rows(&cold_view).await.unwrap();
    assert_eq!(cold_rows.history_bytes, single_row_charge);
    assert_eq!(cold_rows.history_rows, 1);
    assert_eq!(cold_rows.revision("authority"), Some(authority_rev));
    assert!(!cold_rows.contains_key("obsolete/1"));

    // Subsequent commits succeed again within the reclaimed budget.
    let next = reopened
        .commit(&cold_view, vec![record("authority", &payload).unwrap()])
        .await
        .unwrap();
    assert_eq!(next.revision, compacted_view.revision + 1);
}

#[tokio::test]
async fn compact_baseline_survives_lost_commit_response_and_reclaims_files_on_retry() {
    let root = tempfile::tempdir().unwrap();
    let catalog = storage::local_catalog(root.path()).await.unwrap();
    let budget = test_budget(256 << 10);
    let lifecycle = Lifecycle::new(catalog.clone(), budget.clone());
    let owner = Id::new("lost_compact").unwrap();
    let mut view = lifecycle.open(&owner).await.unwrap();
    view = lifecycle
        .commit(&view, vec![record("obsolete/1", "drop-me").unwrap()])
        .await
        .unwrap();
    for step in 0..4 {
        view = lifecycle
            .commit(
                &view,
                vec![record("authority", format!("v-{step}")).unwrap()],
            )
            .await
            .unwrap();
    }
    let authority_rev = view.revision;
    let remove = BTreeSet::from(["obsolete/1".to_owned()]);

    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = lifecycle
        .compact_baseline(&view, &remove)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "lost_commit_response");

    // Reopen reads the committed single-snapshot baseline and all referenced files exist.
    let reopened_view = lifecycle.open(&owner).await.unwrap();
    assert_eq!(reopened_view.revision, authority_rev + 1);
    assert_eq!(reopened_view.table.metadata().snapshots().count(), 1);
    assert_eq!(
        reopened_view
            .table
            .metadata()
            .properties()
            .get("gc.enabled")
            .map(String::as_str),
        Some("false")
    );
    let live_after_lost = storage::current_snapshot_files(&reopened_view.table, &lifecycle.budget)
        .await
        .unwrap()
        .paths;
    for path in &live_after_lost {
        assert!(
            Path::new(path).is_file(),
            "missing live baseline file {path}"
        );
    }
    let rows = lifecycle.rows(&reopened_view).await.unwrap();
    assert_eq!(rows.history_rows, 1);
    assert_eq!(rows.revision("authority"), Some(authority_rev));
    assert!(!rows.contains_key("obsolete/1"));

    // Retrying compaction on the reopened view succeeds and reclaims all expired files.
    let (retried_view, stats) = lifecycle
        .compact_baseline(&reopened_view, &remove)
        .await
        .unwrap();
    assert_eq!(stats.revisions_after, 1);
    assert_eq!(stats.files_after, 1);
    assert_eq!(stats.rows_after, 1);
    let live_after_retry = storage::current_snapshot_files(&retried_view.table, &lifecycle.budget)
        .await
        .unwrap()
        .paths;
    let location = PathBuf::from(retried_view.table.metadata().location());
    let mut disk_files = BTreeSet::new();
    for subdir in ["data", "metadata"] {
        for entry in std::fs::read_dir(location.join(subdir)).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                disk_files.insert(path.to_string_lossy().into_owned());
            }
        }
    }
    assert_eq!(disk_files, live_after_retry);
}

#[tokio::test]
async fn retained_snapshot_survives_cache_replacement_and_holds_its_reservation() {
    let root = tempfile::tempdir().unwrap();
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 32,
        max_batch_bytes: 4096,
        max_metadata_bytes: 1 << 20,
    })
    .unwrap();
    let lifecycle = Lifecycle::new(
        storage::local_catalog(root.path()).await.unwrap(),
        budget.clone(),
    );
    let a = lifecycle.open(&Id::new("a").unwrap()).await.unwrap();
    let a = lifecycle
        .commit(&a, vec![record("key", "first").unwrap()])
        .await
        .unwrap();
    let retained = lifecycle.rows(&a).await.unwrap();
    let weak = Arc::downgrade(&retained);
    let b = lifecycle.open(&Id::new("b").unwrap()).await.unwrap();
    let b = lifecycle
        .commit(&b, vec![record("key", "other dataset").unwrap()])
        .await
        .unwrap();
    assert_ne!(a.table.metadata().location(), b.table.metadata().location());
    // Committing another dataset keeps `a` in the multi-dataset cache without thrashing.
    assert_eq!(Arc::strong_count(&retained), 2);
    // Committing a new revision to `a` replaces `a`'s cache entry while `retained` survives.
    let _a_next = lifecycle
        .commit(&a, vec![record("key", "second").unwrap()])
        .await
        .unwrap();
    // The old native files are now inaccessible. Record reads must still use
    // the retained state; a cache lookup and replay would fail here.
    std::fs::rename(
        a.table.metadata().location(),
        root.path().join("unavailable-a"),
    )
    .unwrap();
    for _ in 0..3 {
        assert_eq!(
            retained.get_record::<String>("key").unwrap(),
            Some((a.revision, 7, "first".into()))
        );
    }
    assert_eq!(Arc::strong_count(&retained), 1);
    let remaining = budget.limits.memory_bytes / 2 - 2 * budget.limits.max_metadata_bytes;
    let available = budget.reserve(remaining).unwrap();
    assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
    drop(retained);
    assert!(weak.upgrade().is_none());
    let reclaimed = budget.reserve(budget.limits.max_metadata_bytes).unwrap();
    assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
    drop((available, reclaimed));
}
