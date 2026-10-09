//! Native registrations, active readers and corrupted intent boundaries.

use super::tests::reclaimable;
use super::*;
use std::time::Duration;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn preview(engine: &Engine, object: &ObjectRef) -> RemovalPlan {
    engine
        .plan_removal(
            &object.owner,
            &id("native-removal"),
            std::slice::from_ref(object),
            "retired fixture output",
        )
        .await
        .unwrap()
}

async fn alias(engine: &Engine, plan: &RemovalPlan) -> TableIdent {
    let original = &plan.registrations[0];
    let ident = TableIdent::new(
        NamespaceIdent::new(original.namespace.clone()),
        format!("data_{}", uuid::Uuid::new_v4().simple()),
    );
    engine
        .inner
        .lifecycle
        .catalog
        .register_table(&ident, original.metadata.clone())
        .await
        .unwrap();
    ident
}

#[tokio::test]
async fn new_native_heads_block_preview_execution_and_pending_resume_without_expanding_effects() {
    let (_root, engine, _old, _replacement, object) = reclaimable().await;
    let plan = preview(&engine, &object).await;
    let added = alias(&engine, &plan).await;
    assert_eq!(
        engine.remove_objects(&plan).await.unwrap_err().code(),
        "stale_removal"
    );
    assert!(Path::new(&object.path).exists());
    assert_eq!(
        engine
            .inspect_removal(&object.owner, &plan.removal)
            .await
            .unwrap_err()
            .code(),
        "no_removal"
    );
    engine
        .inner
        .lifecycle
        .catalog
        .drop_table(&added)
        .await
        .unwrap();
    let memory = engine.request_memory(&plan).unwrap();
    let request = plan.clone();
    assert_eq!(
        engine
            .maintain_work(memory, move |worker| async move {
                worker
                    .remove_admitted(&request, Some("removal-intent"))
                    .await
            })
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let added = alias(&engine, &plan).await;
    assert_eq!(
        engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap_err()
            .code(),
        "stale_removal"
    );
    assert!(Path::new(&object.path).exists());
    engine
        .inner
        .lifecycle
        .catalog
        .drop_table(&added)
        .await
        .unwrap();
    assert!(
        engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap()
            .complete
    );
    assert!(!Path::new(&object.path).exists());
}

#[tokio::test]
async fn provisional_native_head_detaches_while_selected_shared_data_remains_readable() {
    let (_root, engine, old, replacement, object) = reclaimable().await;
    let budget = &engine.inner.lifecycle.budget;
    let old_table = storage::open_pin(&old.dataset.groups["source"])
        .await
        .unwrap();
    let new_table = storage::open_pin(&replacement.dataset.groups["source"])
        .await
        .unwrap();
    let mut files = storage::snapshot_files(&old_table, budget)
        .await
        .unwrap()
        .data;
    files.extend(
        storage::snapshot_files(&new_table, budget)
            .await
            .unwrap()
            .data,
    );
    // This simulates an abandoned preparation with native data but no completed
    // TablePin. Its old attempt ownership remains recorded after retirement.
    let directory = Path::new(old_table.metadata().location()).join("provisional");
    let schema = old_table.metadata().current_schema().as_ref().clone();
    let namespace = crate::catalog::lifecycle_ident(&object.owner)
        .namespace()
        .clone();
    let provisional = engine
        .register_output(&namespace, move || {
            storage::fresh_metadata(schema, &directory)
        })
        .await
        .unwrap();
    let provisional = storage::append(&provisional, engine.inner.lifecycle.catalog.as_ref(), files)
        .await
        .unwrap();
    let plan = preview(&engine, &object).await;
    assert!(
        plan.registrations
            .iter()
            .any(|entry| entry.name == provisional.identifier().name())
    );
    let status = engine.remove_objects(&plan).await.unwrap();
    assert!(status.complete);
    assert!(
        !engine
            .inner
            .lifecycle
            .catalog
            .table_exists(provisional.identifier())
            .await
            .unwrap()
    );
    assert!(!Path::new(&object.path).exists());
    let mut stream = engine.read_group(&replacement, "source").await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    for file in &replacement.dataset.groups["source"].objects {
        assert!(Path::new(&file.path).exists());
    }
}

#[tokio::test]
async fn active_engine_reader_delays_physical_removal_until_eof() {
    let (_root, engine, _old, replacement, object) = reclaimable().await;
    let plan = preview(&engine, &object).await;
    let mut reader = engine.read_group(&replacement, "source").await.unwrap();
    let worker = engine.clone();
    let task = tokio::spawn(async move { worker.remove_objects(&plan).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!task.is_finished());
    assert!(Path::new(&object.path).exists());
    // A delivered batch does not close a stream that may have more data.
    assert!(reader.try_next().await.unwrap().is_some());
    assert!(!task.is_finished());
    while reader.try_next().await.unwrap().is_some() {}
    let status = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(status.complete);
    assert!(!Path::new(&object.path).exists());
}

#[tokio::test]
async fn malformed_reserved_marker_refuses_inventory_and_resume_before_effects() {
    let (_root, engine, _old, _replacement, object) = reclaimable().await;
    let plan = preview(&engine, &object).await;
    let memory = engine.request_memory(&plan).unwrap();
    let request = plan.clone();
    assert!(
        engine
            .maintain_work(memory, move |worker| async move {
                worker
                    .remove_admitted(&request, Some("removal-intent"))
                    .await
            })
            .await
            .is_err()
    );
    let view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
    let key = super::super::owners::unavailable_key(&object.path);
    let mut marker: super::super::owners::UnavailableObject = engine
        .inner
        .lifecycle
        .get(&view, &key)
        .await
        .unwrap()
        .unwrap();
    marker.object.digest = "f".repeat(64);
    engine
        .inner
        .lifecycle
        .commit(&view, vec![record(key, marker).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        engine
            .inspect_retention(std::slice::from_ref(&object))
            .await
            .unwrap_err()
            .code(),
        "invalid_removal"
    );
    assert_eq!(
        engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap_err()
            .code(),
        "invalid_removal"
    );
    assert!(Path::new(&object.path).exists());
}

#[tokio::test]
async fn forged_saved_registration_cannot_drop_lifecycle_or_unrelated_selected_native_table() {
    for lifecycle_target in [true, false] {
        let (_root, engine, _old, replacement, object) = reclaimable().await;
        let mut plan = preview(&engine, &object).await;
        let view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
        let protected_ident = if lifecycle_target {
            view.table.identifier().clone()
        } else {
            let ident = TableIdent::new(
                view.table.identifier().namespace().clone(),
                format!("data_{}", uuid::Uuid::new_v4().simple()),
            );
            engine
                .inner
                .lifecycle
                .catalog
                .register_table(
                    &ident,
                    replacement.dataset.groups["source"].metadata.path.clone(),
                )
                .await
                .unwrap();
            ident
        };
        let protected = engine
            .inner
            .lifecycle
            .catalog
            .load_table(&protected_ident)
            .await
            .unwrap();
        plan.registrations.push(RemovalRegistration {
            namespace: protected_ident.namespace().to_string(),
            name: protected_ident.name().into(),
            table_uuid: protected.metadata().uuid().to_string(),
            metadata: protected.metadata_location_result().unwrap().into(),
        });
        plan.registrations.sort();
        let intent = RemovalIntent {
            revision: view.revision + 1,
            registrations: (0..plan.registrations.len()).collect(),
            boundaries: plan.inspection.boundaries.clone(),
            blocked: vec![],
            objects: vec![0],
            plan,
        };
        let progress = RemovalProgress {
            intent_revision: intent.revision,
            reserved_future_bytes: (outcome_charge(&intent).unwrap() * 2) as u64,
            registrations: vec![RemovalOutcome::Unresolved; intent.registrations.len()],
            objects: vec![RemovalOutcome::Unresolved],
        };
        // Bypass the public service to represent malformed persisted application
        // metadata. Resume must validate the effect authority before using it.
        engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(intent_key(&intent.plan.removal), &intent).unwrap(),
                    record(state_key(&intent.plan.removal), &progress).unwrap(),
                    record(
                        super::super::owners::unavailable_key(&object.path),
                        super::super::owners::UnavailableObject {
                            object: object.clone(),
                            intent: intent.plan.removal.clone(),
                            revision: intent.revision,
                        },
                    )
                    .unwrap(),
                ],
            )
            .await
            .unwrap();
        let error = engine
            .resume_removal(&object.owner, &intent.plan.removal)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            if lifecycle_target {
                "invalid_removal"
            } else {
                "stale_removal"
            }
        );
        assert!(
            engine
                .inner
                .lifecycle
                .catalog
                .table_exists(&protected_ident)
                .await
                .unwrap()
        );
        assert!(Path::new(&object.path).exists());
    }
}
