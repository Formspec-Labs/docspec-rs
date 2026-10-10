//! Ending compaction recovery releases obsolete layouts while preserving independent promises.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::{compaction, scalar};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::SchemaRef,
};
use docspec::{
    Engine,
    model::{ObjectRef, PhaseResult, ReleasePin, RemovalOutcome, RetentionPlan, Retirement},
    storage,
};
use futures::TryStreamExt;
use iceberg::{NamespaceIdent, TableIdent};
use scalar::{fixture, id, limits};
use std::{collections::BTreeSet, path::Path};

async fn selected_values(engine: &Engine, release: &ReleasePin) -> (SchemaRef, Vec<scalar::Row>) {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let schema = stream.schema();
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
                .map(|(id, text)| (id, text.map(str::to_owned))),
        );
    }
    rows.sort();
    (schema, rows)
}

async fn retire(engine: &Engine, operation: &str) -> Retirement {
    let plan = engine
        .plan_compaction_retirement(
            &id("comments"),
            &id(&format!("{operation}-recovery")),
            &id(operation),
            "end recovery of this exact compaction",
        )
        .await
        .unwrap();
    engine.retire_compaction(&plan).await.unwrap()
}

async fn blocker(engine: &Engine, object: &ObjectRef) -> Option<String> {
    engine
        .inspect_retention(std::slice::from_ref(object))
        .await
        .unwrap()
        .objects[0]
        .blocker
        .as_ref()
        .map(|blocker| blocker.key.clone())
}

fn data_file(result: &docspec::model::CompactionResult) -> ObjectRef {
    result
        .replacement
        .objects
        .iter()
        .find(|object| result.output_files.contains(&object.path))
        .unwrap()
        .clone()
}

#[tokio::test]
async fn retired_chain_reclaims_intermediate_data_and_metadata_without_changing_selected_layout() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, selected, _) = fixture(root.path(), false).await;
    let canonical = &selected.dataset.groups["source"];
    let original_occurrences = compaction::occurrences(canonical).await;
    let expected = selected_values(&engine, &selected).await;
    let a = engine
        .compact(&compaction::request(&selected, "a"))
        .await
        .unwrap();
    let b = engine
        .compact(&compaction::request(&selected, "b"))
        .await
        .unwrap();
    assert_eq!(b.source, a.replacement);
    assert_ne!(a.replacement.metadata, b.replacement.metadata);
    let a_file = data_file(&a);
    let b_file = data_file(&b);
    assert_ne!(a_file, b_file);
    assert!(!b.replacement.objects.contains(&a_file));
    let selection_key = format!("representation/{}/source", canonical.metadata.digest);
    let selected_record = engine.inspect(&id("comments")).await.unwrap()[&selection_key].clone();

    retire(&engine, "a").await;
    let witness = blocker(&engine, &a_file).await.unwrap();
    assert!(
        witness.ends_with("/b"),
        "B recovery still protects A: {witness}"
    );
    assert!(Path::new(&a_file.path).exists());
    let b_retirement = retire(&engine, "b").await;
    assert!(blocker(&engine, &a_file).await.is_none());
    assert!(blocker(&engine, &a.replacement.metadata).await.is_none());
    assert!(blocker(&engine, &b_file).await.is_some());
    assert!(blocker(&engine, &canonical.metadata).await.is_some());
    let original_file = canonical
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    assert!(blocker(&engine, original_file).await.is_some());
    // Retirement itself keeps exact bytes; physical removal is separate authority.
    for object in [&a_file, &a.replacement.metadata] {
        assert_eq!(
            storage::digest(Path::new(&object.path)).unwrap().0,
            object.digest
        );
    }
    let candidates = [a_file.clone(), a.replacement.metadata.clone()];
    let plan = engine
        .plan_removal(
            &id("comments"),
            &id("remove-a"),
            &candidates,
            "remove unneeded intermediate layout",
        )
        .await
        .unwrap();
    assert!(
        plan.inspection
            .objects
            .iter()
            .all(|item| item.blocker.is_none())
    );
    assert!(!plan.registrations.is_empty());
    let catalog = storage::local_catalog(&workspace).await.unwrap();
    for registration in &plan.registrations {
        let ident = TableIdent::new(
            NamespaceIdent::new(registration.namespace.clone()),
            registration.name.clone(),
        );
        assert!(catalog.table_exists(&ident).await.unwrap());
    }
    let removed = engine.remove_objects(&plan).await.unwrap();
    assert!(removed.complete);
    assert_eq!(
        removed.progress.objects,
        vec![RemovalOutcome::Removed; candidates.len()]
    );
    assert_eq!(
        removed.progress.registrations,
        vec![RemovalOutcome::Removed; plan.registrations.len()]
    );
    assert_eq!(
        removed.confirmed_unlinked_bytes,
        candidates.iter().map(|object| object.bytes).sum::<u64>()
    );
    for object in &candidates {
        assert!(!Path::new(&object.path).exists());
    }
    for registration in &plan.registrations {
        let ident = TableIdent::new(
            NamespaceIdent::new(registration.namespace.clone()),
            registration.name.clone(),
        );
        assert!(!catalog.table_exists(&ident).await.unwrap());
    }
    drop(catalog);
    drop(engine);

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(
        engine.retire_compaction(&b_retirement.plan).await.unwrap(),
        b_retirement
    );
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), selected);
    assert_eq!(
        engine.inspect(&id("comments")).await.unwrap()[&selection_key],
        selected_record
    );
    assert_eq!(selected_values(&engine, &selected).await, expected);
    assert_eq!(
        compaction::occurrences(canonical).await,
        original_occurrences
    );
    assert_eq!(
        compaction::occurrences(&b.replacement).await,
        original_occurrences
    );
    assert!(blocker(&engine, &b_file).await.is_some());
}

#[tokio::test]
async fn captured_update_keeps_intermediate_layout_until_both_operation_and_phase_recovery_end() {
    let root = tempfile::tempdir().unwrap();
    let (engine, selected, _) = fixture(root.path(), false).await;
    let expected = selected_values(&engine, &selected).await;
    let a = engine
        .compact(&compaction::request(&selected, "a"))
        .await
        .unwrap();
    let update = compaction::update_request(root.path(), &selected, "captured", &[]);
    assert_eq!(
        engine
            .update_until(&update, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&id("comments")).await.unwrap();
    assert_eq!(
        records["request-dependencies/captured"]["update_source"],
        serde_json::to_value(&a.replacement).unwrap()
    );
    let phases = records
        .iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("phase/") {
                return None;
            }
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.operation == update.operation).then(|| key.clone())
        })
        .collect::<Vec<_>>();
    assert!(!phases.is_empty());
    let b = engine
        .compact(&compaction::request(&selected, "b"))
        .await
        .unwrap();
    assert_eq!(b.source, a.replacement);
    retire(&engine, "a").await;
    retire(&engine, "b").await;
    let a_file = data_file(&a);
    assert!(blocker(&engine, &a_file).await.is_some());
    engine
        .abandon(&update.dataset, &update.operation, "stop captured update")
        .await
        .unwrap();
    let recovery = engine
        .plan_operation_retirement(
            &update.dataset,
            &id("captured-recovery"),
            &update.operation,
            "end captured update recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&recovery).await.unwrap();
    let witness = blocker(&engine, &a_file).await.unwrap();
    assert!(
        witness.starts_with("phase/"),
        "completed processing still protects A: {witness}"
    );
    let phase_plan = engine
        .plan_phase_retirement(
            &update.dataset,
            &id("captured-phases"),
            &phases,
            "end independently reusable captured phases",
        )
        .await
        .unwrap();
    engine.retire_phases(&phase_plan).await.unwrap();
    assert!(blocker(&engine, &a_file).await.is_none());
    assert!(Path::new(&a_file.path).exists());
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), selected);
    assert_eq!(selected_values(&engine, &selected).await, expected);
}

#[tokio::test]
async fn interrupted_bundles_retire_exactly_after_reopen_and_later_completion_invalidates_preview()
{
    for stop in ["rewrite", "verification"] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let (engine, selected, _) = fixture(root.path(), false).await;
        let expected = selected_values(&engine, &selected).await;
        let spec = compaction::request(&selected, "interrupted");
        assert_eq!(
            engine.compact_until(&spec, stop).await.unwrap_err().code(),
            "injected_failure"
        );
        let records = engine.inspect(&spec.dataset).await.unwrap();
        let prepared: docspec::model::CompactionResult =
            serde_json::from_value(records["compaction-prepared/interrupted"].clone()).unwrap();
        let plan = engine
            .plan_compaction_retirement(
                &spec.dataset,
                &id("interrupted-recovery"),
                &spec.operation,
                "explicitly stop interrupted compaction",
            )
            .await
            .unwrap();
        assert!(
            plan.records
                .iter()
                .any(|record| record.key.starts_with("protection/"))
        );
        assert_eq!(
            plan.records
                .iter()
                .any(|record| record.key == "compaction-verified/interrupted"),
            stop == "verification"
        );
        assert!(
            !plan
                .records
                .iter()
                .any(|record| record.key == "compaction-result/interrupted")
        );
        let receipt = engine.retire_compaction(&plan).await.unwrap();
        assert!(Path::new(&data_file(&prepared).path).exists());
        drop(engine);
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        assert_eq!(engine.retire_compaction(&plan).await.unwrap(), receipt);
        assert_eq!(
            engine.compact(&spec).await.unwrap_err().code(),
            "compaction_retired"
        );
        let after = engine.inspect(&spec.dataset).await.unwrap();
        for generation in &plan.records {
            assert_eq!(after[&generation.key], records[&generation.key]);
        }
        assert!(blocker(&engine, &data_file(&prepared)).await.is_none());
        assert_eq!(engine.latest(&spec.dataset).await.unwrap(), selected);
        assert_eq!(selected_values(&engine, &selected).await, expected);
    }

    let root = tempfile::tempdir().unwrap();
    let (engine, selected, _) = fixture(root.path(), false).await;
    let spec = compaction::request(&selected, "finishes-later");
    assert_eq!(
        engine
            .compact_until(&spec, "rewrite")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let preview = engine
        .plan_compaction_retirement(
            &spec.dataset,
            &id("old-preview"),
            &spec.operation,
            "capture unfinished recovery",
        )
        .await
        .unwrap();
    let completed = engine.compact(&spec).await.unwrap();
    assert_eq!(
        engine.retire_compaction(&preview).await.unwrap_err().code(),
        "stale_retirement"
    );
    assert_eq!(engine.compact(&spec).await.unwrap(), completed);
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), selected);
    retire(&engine, "finishes-later").await;
    assert_eq!(
        engine.compact(&spec).await.unwrap_err().code(),
        "compaction_retired"
    );
}

fn attempt_keys(plan: &RetentionPlan) -> BTreeSet<String> {
    plan.records
        .iter()
        .filter(|record| record.key.starts_with("protection/"))
        .map(|record| record.key.clone())
        .collect()
}

#[tokio::test]
async fn identical_operation_ids_keep_normal_and_compaction_attempt_retirement_separate() {
    for compact_first in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let (engine, selected, _) = fixture(root.path(), false).await;
        let spec = compaction::request(&selected, "initial");
        let compacted = engine.compact(&spec).await.unwrap();
        let compaction_plan = engine
            .plan_compaction_retirement(
                &spec.dataset,
                &id("compact-recovery"),
                &spec.operation,
                "end compaction recovery",
            )
            .await
            .unwrap();
        let operation_plan = engine
            .plan_operation_retirement(
                &spec.dataset,
                &id("operation-recovery"),
                &spec.operation,
                "end normal operation recovery",
            )
            .await
            .unwrap();
        let compaction_attempts = attempt_keys(&compaction_plan);
        let operation_attempts = attempt_keys(&operation_plan);
        assert!(!compaction_attempts.is_empty());
        assert!(!operation_attempts.is_empty());
        assert!(compaction_attempts.is_disjoint(&operation_attempts));
        let before = engine.inspect(&spec.dataset).await.unwrap();
        for key in &compaction_attempts {
            assert_eq!(before[key]["purpose"], "initial/compaction");
        }
        for key in &operation_attempts {
            assert_ne!(before[key]["purpose"], "initial/compaction");
            assert!(
                before[key]["purpose"]
                    .as_str()
                    .unwrap()
                    .starts_with("initial/")
            );
        }
        let untouched = if compact_first {
            engine.retire_compaction(&compaction_plan).await.unwrap();
            &operation_attempts
        } else {
            engine.retire_operation(&operation_plan).await.unwrap();
            assert_eq!(engine.compact(&spec).await.unwrap(), compacted);
            &compaction_attempts
        };
        let after = engine.inspect(&spec.dataset).await.unwrap();
        let retired = after
            .iter()
            .filter(|(key, _)| key.starts_with("retired/"))
            .map(|(_, value)| value["record"]["key"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>();
        assert!(untouched.is_disjoint(&retired));
        for key in untouched {
            assert_eq!(after[key], before[key]);
        }
        if compact_first {
            engine.retire_operation(&operation_plan).await.unwrap();
        } else {
            engine.retire_compaction(&compaction_plan).await.unwrap();
        }
        assert_eq!(engine.latest(&spec.dataset).await.unwrap(), selected);
    }
}

#[tokio::test]
async fn concurrent_identical_compactions_leave_one_exact_retirable_recovery_bundle() {
    let root = tempfile::tempdir().unwrap();
    let (engine, selected, _) = fixture(root.path(), false).await;
    let spec = compaction::request(&selected, "concurrent");
    let (left, right) = tokio::join!(engine.compact(&spec), engine.compact(&spec));
    let left = left.unwrap();
    assert_eq!(right.unwrap(), left);
    let receipt = retire(&engine, "concurrent").await;
    let captured = receipt
        .plan
        .records
        .iter()
        .filter(|record| record.key.starts_with("compaction-"))
        .map(|record| record.key.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        captured,
        BTreeSet::from([
            "compaction-request/concurrent",
            "compaction-prepared/concurrent",
            "compaction-verified/concurrent",
            "compaction-result/concurrent",
        ])
    );
    // Exact captured content and generations qualify this bundle; competing
    // acknowledgments need not arrive in prepared/verified/result revision order.
    assert_eq!(
        engine.retire_compaction(&receipt.plan).await.unwrap(),
        receipt
    );
    assert_eq!(engine.latest(&spec.dataset).await.unwrap(), selected);
    assert_eq!(
        compaction::occurrences(&left.replacement).await,
        compaction::occurrences(&left.source).await
    );
}
