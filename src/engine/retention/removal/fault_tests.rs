//! Cold recovery across intent, native-registration and file effects.

use super::tests::reclaimable;
use super::*;
use std::sync::atomic::Ordering;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn preview(engine: &Engine, object: &ObjectRef, name: &str) -> RemovalPlan {
    engine
        .plan_removal(
            &object.owner,
            &id(name),
            std::slice::from_ref(object),
            "retired fixture output",
        )
        .await
        .unwrap()
}

async fn interrupt(
    engine: &Engine,
    plan: &RemovalPlan,
    stop: &'static str,
) -> Result<RemovalStatus> {
    let memory = engine.request_memory(plan)?;
    let plan = plan.clone();
    engine
        .maintain_work(memory, move |worker| async move {
            worker.remove_admitted(&plan, Some(stop)).await
        })
        .await
}

#[tokio::test]
async fn cold_resume_reconciles_each_durable_effect_boundary_without_inventing_unlinks() {
    for stop in ["removal-intent", "removal-registrations", "removal-unlink"] {
        let (_root, engine, _old, replacement, object) = reclaimable().await;
        let plan = preview(&engine, &object, "stopped-removal").await;
        let workspace = engine.inner.root.clone();
        let limits = engine.inner.lifecycle.budget.limits.clone();
        assert_eq!(
            interrupt(&engine, &plan, stop).await.unwrap_err().code(),
            "injected_failure"
        );
        let saved = engine
            .inspect_removal(&object.owner, &plan.removal)
            .await
            .unwrap();
        assert!(!saved.complete);
        assert_eq!(Path::new(&object.path).exists(), stop != "removal-unlink");
        drop(engine);
        let engine = Engine::open(workspace, limits).await.unwrap();
        let resumed = engine
            .resume_removal(&object.owner, &plan.removal)
            .await
            .unwrap();
        assert!(resumed.complete);
        assert_eq!(resumed.intent, saved.intent);
        assert!(!Path::new(&object.path).exists());
        if stop == "removal-unlink" {
            assert_eq!(resumed.confirmed_unlinked_bytes, 0);
            assert_eq!(resumed.reconciled_absent_bytes, object.bytes);
        } else {
            assert_eq!(resumed.confirmed_unlinked_bytes, object.bytes);
            assert_eq!(resumed.reconciled_absent_bytes, 0);
        }
        assert_eq!(engine.remove_objects(&plan).await.unwrap(), resumed);
        let mut stream = engine.read_group(&replacement, "source").await.unwrap();
        assert!(stream.try_next().await.unwrap().is_some());
    }
}

#[tokio::test]
async fn lost_intent_and_outcome_replies_reconcile_the_same_saved_operation() {
    for outcome in [false, true] {
        let (_root, engine, _old, _replacement, object) = reclaimable().await;
        let plan = preview(&engine, &object, "lost-reply").await;
        let removed = if outcome {
            interrupt(&engine, &plan, "lost-removal-outcome")
                .await
                .unwrap()
        } else {
            engine
                .inner
                .lifecycle
                .lose_commit_response
                .store(true, Ordering::SeqCst);
            engine.remove_objects(&plan).await.unwrap()
        };
        assert!(removed.complete);
        assert!(!Path::new(&object.path).exists());
        let view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
        let before = view.revision;
        assert_eq!(engine.remove_objects(&plan).await.unwrap(), removed);
        assert_eq!(
            engine
                .inner
                .lifecycle
                .open(&object.owner)
                .await
                .unwrap()
                .revision,
            before
        );
        let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
        assert_eq!(rows.prefix("removal-intent/").count(), 1);
    }
}

#[tokio::test]
async fn terminal_absence_reconciliation_uses_reserved_floor_when_no_topup_can_fit() {
    let (_root, engine, _old, _replacement, object) = reclaimable().await;
    let plan = preview(&engine, &object, "terminal-floor").await;
    assert_eq!(
        interrupt(&engine, &plan, "removal-unlink")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert!(!Path::new(&object.path).exists());
    let saved = engine
        .inspect_removal(&object.owner, &plan.removal)
        .await
        .unwrap();
    let mut state = saved.progress;
    let floor = outcome_charge(&saved.intent).unwrap();
    state.registrations.fill(RemovalOutcome::Absent);
    state.objects.fill(RemovalOutcome::Failed {
        code: "removal_sync".into(),
    });
    state.reserved_future_bytes = floor as u64;
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&object.owner).await.unwrap();
    let view = lifecycle
        .commit(
            &view,
            vec![record(state_key(&plan.removal), &state).unwrap()],
        )
        .await
        .unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let mut limits = lifecycle.budget.limits.clone();
    limits.max_metadata_bytes = rows.history_bytes() + floor;
    let workspace = engine.inner.root.clone();
    drop(rows);
    drop(engine);
    let engine = Engine::open(workspace, limits).await.unwrap();
    let view = engine.inner.lifecycle.open(&object.owner).await.unwrap();
    assert_eq!(
        engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![record("unreserved", "cannot spend outcome space").unwrap()]
            )
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    let resumed = engine
        .resume_removal(&object.owner, &plan.removal)
        .await
        .unwrap();
    assert!(resumed.complete);
    assert_eq!(resumed.progress.reserved_future_bytes, 0);
    assert_eq!(resumed.confirmed_unlinked_bytes, 0);
    assert_eq!(resumed.reconciled_absent_bytes, object.bytes);
}
