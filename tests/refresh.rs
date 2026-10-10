//! Coordinated scalar refresh compared with complete independent Rust reconstruction.

use super::enriched;
use super::scalar;
#[cfg(feature = "test-util")]
use super::support;
use enriched::{check_values, enriched, forged};
use scalar::{Row, fixture, id, input, limits, spec};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    engine::verify_release,
    model::{InputSpec, OperationRequest, PhaseResult, ReleasePin, TablePin, UpdateSpec},
    storage,
};
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeSet, fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

fn keys(path: &Path, values: impl Iterator<Item = Option<i64>>) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter(values))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

fn update(
    root: &Path,
    base: &ReleasePin,
    operation: &str,
    rows: &[Row],
    deleted: &[Option<i64>],
) -> UpdateSpec {
    UpdateSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        definition: "v1".into(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: input(&root.join(format!("{operation}.parquet")), rows),
        deletions: Some(keys(
            &root.join(format!("{operation}-keys.parquet")),
            deleted.iter().copied(),
        )),
    }
}

fn data_paths(pin: &TablePin) -> BTreeSet<String> {
    pin.objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .map(|object| object.path.clone())
        .collect()
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_reuses_unchanged_file_computation_and_selects_all_groups_together() {
    let root = TempDir::new().unwrap();
    let (engine, base, original) = enriched(root.path()).await;
    assert!(
        data_paths(&base.dataset.groups["source"]).len() >= 3,
        "fixture needs a file outside the first/tail-key replacement scope"
    );
    let additions = [
        (Some(1), Some("one".into())),
        (Some(1), None),
        (None, Some("🎉".into())),
        (Some(900), Some(String::new())),
    ];
    let request = update(root.path(), &base, "changed", &additions, &[Some(2)]);
    assert!(
        engine
            .plan_update(&request)
            .await
            .unwrap()
            .required_phases
            .iter()
            .any(|p| p == "groups")
    );
    let before = engine.inspect(&request.dataset).await.unwrap();
    drop(engine);
    support::kill_after(root.path(), &request, "update", "processing");
    std::fs::remove_file(&request.replacements.path).unwrap();
    std::fs::remove_file(&request.deletions.as_ref().unwrap().path).unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    let prepared: PhaseResult = engine
        .inspect(&request.dataset)
        .await
        .unwrap()
        .iter()
        .filter(|(k, _)| k.starts_with("phase/"))
        .find_map(|(_, value)| {
            serde_json::from_value::<PhaseResult>(value.clone())
                .ok()
                .filter(|phase| phase.operation == request.operation && phase.phase == "processing")
        })
        .unwrap();
    let candidate_source = prepared.table.unwrap();
    let old_paths = data_paths(&base.dataset.groups["source"]);
    let new_paths = data_paths(&candidate_source);
    assert!(old_paths.intersection(&new_paths).next().is_some());
    let missing_files = new_paths.difference(&old_paths).count();
    assert!(missing_files > 0);
    drop(engine);
    let (release, work) = support::resume(root.path(), "update");
    assert_eq!(
        work.iter()
            .filter(|phase| phase.as_str() == "backfill-file")
            .count(),
        missing_files * base.dataset.computed.len()
    );
    assert!(
        !work
            .iter()
            .any(|phase| phase == "processing" || phase == "admission")
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let expected: Vec<_> = original
        .iter()
        .filter(|row| ![Some(1), None, Some(2)].contains(&row.0))
        .cloned()
        .chain(additions)
        .collect();
    check_values(&engine, &release, &expected).await;
    assert_eq!(
        release.dataset.parent_manifest.as_ref(),
        Some(&base.manifest)
    );
    assert_eq!(release.dataset.groups["source"], candidate_source);
    for (name, binding) in &release.dataset.computed {
        assert_eq!(binding.request, OperationRequest::Update(request.clone()));
        assert_eq!(binding.definition, base.dataset.computed[name].definition);
        assert_eq!(binding.source_metadata, candidate_source.metadata);
    }
    let after = engine.inspect(&request.dataset).await.unwrap();
    let selected_paths: BTreeSet<_> = release
        .dataset
        .groups
        .iter()
        .filter(|(name, _)| name.as_str() != "source")
        .flat_map(|(_, pin)| data_paths(pin))
        .collect();
    for (key, completion) in &before {
        if !key.starts_with("phase/backfill/") {
            continue;
        }
        assert_eq!(&after[key], completion);
        let source_path = completion["source"]["path"].as_str().unwrap();
        for output in completion["outputs"].as_array().unwrap() {
            assert_eq!(
                selected_paths.contains(output["path"].as_str().unwrap()),
                new_paths.contains(source_path)
            );
        }
    }
    verify_release(&base, limits()).await.unwrap();
    assert_eq!(engine.update(&request).await.unwrap(), release);
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_and_delete_all_keep_complete_group_semantics_without_recomputing_unchanged_files() {
    let root = TempDir::new().unwrap();
    let (engine, base, rows) = enriched(root.path()).await;
    let request = update(root.path(), &base, "noop", &[], &[]);
    drop(engine);
    support::kill_after(root.path(), &request, "update", "processing");
    let (noop, work) = support::resume(root.path(), "update");
    assert!(!work.iter().any(|phase| phase == "backfill-file"));
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(noop.dataset.groups, base.dataset.groups);
    check_values(&engine, &noop, &rows).await;
    let removed = update(
        root.path(),
        &noop,
        "empty",
        &[],
        &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    );
    let empty = engine.update(&removed).await.unwrap();
    assert!(empty.dataset.groups.values().all(|pin| pin.rows == 0));
    assert_eq!(empty.dataset.computed.len(), base.dataset.computed.len());
    check_values(&engine, &empty, &[]).await;
    verify_release(&noop, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_resumes_after_partial_groups_readback_and_lost_selection_response() {
    for phase in ["group/a", "groups", "readback", "selection"] {
        let root = TempDir::new().unwrap();
        let (engine, base, original) = enriched(root.path()).await;
        let rows = [(Some(8), Some("changed".into()))];
        let request = update(root.path(), &base, "changed", &rows, &[]);
        drop(engine);
        support::kill_after(root.path(), &request, "update", phase);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let before = engine.inspect(&request.dataset).await.unwrap();
        if phase != "selection" {
            assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        }
        drop(engine);
        let (release, work) = support::resume(root.path(), "update");
        let expected_assemblies = usize::from(phase == "group/a");
        assert_eq!(
            work.iter()
                .filter(|phase| phase.as_str() == "backfill-assembly")
                .count(),
            expected_assemblies
        );
        assert!(
            !work
                .iter()
                .any(|phase| ["admission", "processing", "validation"].contains(&phase.as_str()))
        );
        if ["readback", "selection"].contains(&phase) {
            assert!(work.is_empty());
        }
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let expected: Vec<_> = original
            .into_iter()
            .filter(|row| row.0 != Some(8))
            .chain(rows)
            .collect();
        check_values(&engine, &release, &expected).await;
        let after = engine.inspect(&request.dataset).await.unwrap();
        for (key, value) in before
            .iter()
            .filter(|(key, _)| key.starts_with("phase/") || key.starts_with("backfill-"))
        {
            assert_eq!(&after[key], value);
        }
    }
}

#[tokio::test]
async fn independent_refresh_verifier_rejects_missing_groups_inputs_and_false_source_interpretations()
 {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let request = update(
        root.path(),
        &base,
        "updated",
        &[(Some(9), Some("new".into()))],
        &[],
    );
    let release = engine.update(&request).await.unwrap();
    for kind in [
        "missing-group",
        "recipe",
        "request",
        "input-omission",
        "input-rebind",
        "parent",
        "boundary",
        "definition",
    ] {
        let mut changed = release.clone();
        match kind {
            "missing-group" => {
                changed.dataset.groups.remove("b");
                changed.dataset.computed.remove("b");
            }
            "recipe" => {
                changed
                    .dataset
                    .computed
                    .get_mut("b")
                    .unwrap()
                    .definition
                    .fields[0]
                    .expression = "id + 9".into()
            }
            "request" => {
                changed.dataset.computed.get_mut("b").unwrap().request =
                    base.dataset.request.clone()
            }
            "input-omission" => {
                changed.dataset.inputs.remove("input");
            }
            "input-rebind" => {
                let new_input = changed.dataset.inputs["updated/replacements"].clone();
                changed.dataset.inputs.insert("input".into(), new_input);
            }
            "parent" => changed.dataset.parent_manifest = None,
            "boundary" => changed.dataset.boundary = base.dataset.boundary,
            "definition" => {
                changed.dataset.definition = "different".into();
                if let OperationRequest::Update(spec) = &mut changed.dataset.request {
                    spec.definition = "different".into();
                }
                for binding in changed.dataset.computed.values_mut() {
                    binding.request = changed.dataset.request.clone();
                }
            }
            _ => unreachable!(),
        }
        let changed = forged(root.path(), kind, changed);
        assert_eq!(
            verify_release(&changed, limits()).await.unwrap_err().code(),
            "invalid_group",
            "{kind}"
        );
    }
}

#[tokio::test]
async fn invalidated_source_refuses_incremental_admission_without_changing_authority() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    engine
        .invalidate(&base.dataset.dataset, "source dependency correction", None)
        .await
        .unwrap();
    let request = update(root.path(), &base, "invalid", &[], &[]);
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(
        engine.plan_update(&request).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(
        engine.update(&request).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn relocated_retry_preserves_original_request_and_readback() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let mut request = update(root.path(), &base, "relocated", &[(Some(1), None)], &[]);
    assert_eq!(
        engine
            .update_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original = engine.inspect(&request.dataset).await.unwrap()["readback/relocated"].clone();
    std::fs::remove_file(&request.replacements.path).unwrap();
    std::fs::remove_file(&request.deletions.as_ref().unwrap().path).unwrap();
    request.replacements.path = root
        .path()
        .join("different-location.parquet")
        .to_string_lossy()
        .into_owned();
    request.deletions.as_mut().unwrap().path = root
        .path()
        .join("different-keys.parquet")
        .to_string_lossy()
        .into_owned();
    let resumed = engine.update(&request).await.unwrap();
    assert_eq!(serde_json::to_value(resumed).unwrap(), original);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn noop_target_reselection_is_a_stale_group_conflict_and_inherited_siblings_are_exact() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let pending = spec(&base, "pending", "a", "text_length", "length(text) + 1");
    assert_eq!(
        engine
            .backfill_until(&pending, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let selected = engine
        .backfill(&spec(&base, "same", "a", "text_length", "length(text)"))
        .await
        .unwrap();
    assert_eq!(selected.dataset.groups, base.dataset.groups);
    assert_ne!(selected.dataset.computed["a"], base.dataset.computed["a"]);
    assert_eq!(
        engine
            .plan_backfill(&pending)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("stale_group")
    );
    assert_eq!(
        engine.backfill(&pending).await.unwrap_err().code(),
        "stale_group"
    );

    let changed = engine
        .update(&update(
            root.path(),
            &selected,
            "changed",
            &[(Some(4), None)],
            &[],
        ))
        .await
        .unwrap();
    let extended = engine
        .backfill(&spec(&changed, "extra", "c", "third", "id + 3"))
        .await
        .unwrap();
    assert_eq!(
        extended.dataset.parent_manifest.as_ref(),
        Some(&changed.manifest)
    );
    assert_eq!(
        extended.dataset.computed["a"],
        changed.dataset.computed["a"]
    );
    verify_release(&extended, limits()).await.unwrap();
    for kind in ["omitted", "binding", "table", "parent"] {
        let mut tampered = extended.clone();
        match kind {
            "omitted" => {
                tampered.dataset.groups.remove("b");
                tampered.dataset.computed.remove("b");
            }
            "binding" => {
                tampered.dataset.computed.get_mut("b").unwrap().request =
                    selected.dataset.request.clone()
            }
            "table" => {
                tampered
                    .dataset
                    .groups
                    .insert("b".into(), selected.dataset.groups["b"].clone());
            }
            "parent" => tampered.dataset.parent_manifest = Some(selected.manifest.clone()),
            _ => unreachable!(),
        }
        assert_eq!(
            verify_release(&forged(root.path(), kind, tampered), limits())
                .await
                .unwrap_err()
                .code(),
            "invalid_group"
        );
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn refresh_and_backfill_races_preserve_captured_source_and_selected_groups() {
    let root = TempDir::new().unwrap();
    let (engine, base, original) = enriched(root.path()).await;
    let pending = spec(&base, "pending", "c", "third", "id + 3");
    assert_eq!(
        engine
            .backfill_until(&pending, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let changes = [(Some(0), Some("updated".into()))];
    let first = update(root.path(), &base, "first", &changes, &[]);
    assert_eq!(
        engine
            .update_until(&first, "groups")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine.backfill(&pending).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    assert_eq!(engine.latest(&first.dataset).await.unwrap(), base);
    // A newer admission does not veto a complete routine result against the still-selected base.
    let later = update(root.path(), &base, "later", &[(Some(2), None)], &[]);
    assert_eq!(
        engine
            .update_until(&later, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let selected = engine.update(&first).await.unwrap();
    let authority = engine.inspect(&first.dataset).await.unwrap()["authority"].clone();
    assert!(authority["boundary"].as_u64().unwrap() > selected.dataset.boundary);
    assert_eq!(
        engine.update(&later).await.unwrap_err().code(),
        "stale_base"
    );
    let expected: Vec<_> = original
        .into_iter()
        .filter(|row| row.0 != Some(0))
        .chain(changes)
        .collect();
    check_values(&engine, &selected, &expected).await;
    let catchup = update(root.path(), &selected, "catchup", &[(Some(2), None)], &[]);
    let caught_up = engine.update(&catchup).await.unwrap();
    let new_group = spec(&caught_up, "new-scope", "c", "third", "id + 3");
    let initial_plan = engine.plan_backfill(&new_group).await.unwrap();
    assert_eq!(
        initial_plan.promotion_refusal.as_deref(),
        Some("pending_source")
    );
    assert!(initial_plan.files_completed.is_some_and(|files| files > 0));
    assert_eq!(
        engine.backfill(&new_group).await.unwrap_err().code(),
        "pending_source"
    );
    assert_eq!(engine.latest(&later.dataset).await.unwrap(), caught_up);
    engine
        .abandon(
            &later.dataset,
            &later.operation,
            "later input was replaced by catchup against the selected source",
        )
        .await
        .unwrap();
    let complete = engine.backfill(&new_group).await.unwrap();
    assert_eq!(complete.dataset.groups.len(), 4);
    verify_release(&complete, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn failed_group_keeps_prepared_source_and_other_groups_without_partial_selection() {
    let root = TempDir::new().unwrap();
    let (engine, source, _) = fixture(root.path(), false).await;
    let first = engine
        .backfill(&spec(
            &source,
            "lengths",
            "a",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let base = engine
        .backfill(&spec(
            &first,
            "guarded",
            "b",
            "checked",
            "CASE WHEN text = 'bad' THEN CAST(text AS BIGINT) ELSE id END",
        ))
        .await
        .unwrap();
    let request = update(
        root.path(),
        &base,
        "failed",
        &[(Some(3), Some("bad".into()))],
        &[],
    );
    assert!(engine.update(&request).await.is_err());
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert!(before.keys().any(|key| key.starts_with("failure/failed/")));
    assert!(
        engine
            .plan_update(&request)
            .await
            .unwrap()
            .required_phases
            .iter()
            .any(|phase| phase == "groups")
    );
    assert_eq!(
        engine
            .update_until(&request, "group/a")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let after = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(
        before, after,
        "successful source and first-group work must need no additional acknowledgment"
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
}
