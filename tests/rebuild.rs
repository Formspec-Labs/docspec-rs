//! Complete source rebuilds preserve scalar definitions and qualify a new selection.

#[path = "support/enriched.rs"]
mod enriched;
#[path = "support/scalar.rs"]
mod scalar;
#[cfg(feature = "test-util")]
mod support;

use docspec::{
    Engine,
    engine::verify_release,
    model::{OperationRequest, PhaseResult, ReleasePin, RunSpec, TablePin, UpdateSpec},
};
use enriched::{check_values, enriched, forged};
use scalar::{Row, id, input, limits, spec};
use std::path::Path;
use tempfile::TempDir;

fn rebuild(root: &Path, base: &ReleasePin, operation: &str, rows: &[Row]) -> RunSpec {
    let source = input(&root.join(format!("{operation}.parquet")), rows);
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: Some(base.id.clone()),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
    }
}

fn rows() -> Vec<Row> {
    vec![
        (Some(1), Some("replaced".into())),
        (Some(1), Some("replaced".into())),
        (None, None),
        (None, Some(String::new())),
        (Some(90), Some("é🎉".into())),
    ]
}

#[tokio::test]
async fn full_rebuild_requalifies_source_and_every_group_without_changing_history() {
    let root = TempDir::new().unwrap();
    let (engine, base, original) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "rebuild", &rows());
    engine
        .invalidate(
            &request.dataset,
            "correct source shaping",
            Some("v2".into()),
        )
        .await
        .unwrap();
    request.definition = "v2".into();
    request.sql = "SELECT id * 2 AS id, upper(text) AS text FROM input".into();
    assert!(
        engine
            .plan(&request)
            .await
            .unwrap()
            .required_phases
            .contains(&"groups".into())
    );
    let result = engine.run(&request).await.unwrap();
    let expected: Vec<_> = rows()
        .into_iter()
        .map(|(id, text)| (id.map(|v| v * 2), text.map(|v| v.to_uppercase())))
        .collect();
    check_values(&engine, &result, &expected).await;
    assert_eq!(result.dataset.inputs.len(), 1);
    assert_eq!(
        result.dataset.inputs["input"].digest,
        request.expected_digest
    );
    assert_eq!(result.dataset.parent_manifest, Some(base.manifest.clone()));
    assert!(result.dataset.eligibility > base.dataset.eligibility);
    assert!(result.dataset.boundary > base.dataset.boundary);
    assert!(result.dataset.changes.is_none());
    for (name, binding) in &result.dataset.computed {
        assert_eq!(binding.definition, base.dataset.computed[name].definition);
        assert_eq!(binding.request, OperationRequest::Build(request.clone()));
        assert_eq!(
            binding.source_metadata,
            result.dataset.groups["source"].metadata
        );
        assert_eq!(binding.base_manifest, base.manifest);
    }
    verify_release(&base, limits()).await.unwrap();
    check_values(&engine, &base, &original).await;
    assert_eq!(
        engine.release(&request.dataset, &base.id).await.unwrap(),
        base
    );
    let mut addition = spec(
        &result,
        "later-field",
        "c",
        "double_length",
        "length(text) * 2",
    );
    addition.definition = "v2".into();
    let extended = engine.backfill(&addition).await.unwrap();
    verify_release(&extended, limits()).await.unwrap();
    // A later source update can inherit Build-backed groups without losing their input.
    let update = UpdateSpec {
        dataset: request.dataset.clone(),
        operation: id("later-update"),
        definition: "v2".into(),
        base: extended.id.clone(),
        keys: vec!["id".into()],
        replacements: input(&root.path().join("update.parquet"), &[]),
        deletions: None,
    };
    let refreshed = engine.update(&update).await.unwrap();
    assert_eq!(refreshed.dataset.groups, extended.dataset.groups);
    verify_release(&refreshed, limits()).await.unwrap();
    assert_eq!(engine.run(&request).await.unwrap(), result);
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_resumes_source_partial_groups_readback_and_lost_selection_response() {
    for phase in ["processing", "group/a", "groups", "readback", "selection"] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = enriched(root.path()).await;
        let request = rebuild(root.path(), &base, "rebuild", &rows());
        drop(engine);
        support::kill_after(root.path(), &request, "build", phase);
        std::fs::remove_file(&request.input).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let retained = engine.inspect(&request.dataset).await.unwrap();
        if phase != "selection" {
            assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        }
        drop(engine);
        // Retry may move locators; the original admitted request still binds readback.
        let mut relocated = request.clone();
        relocated.input = root
            .path()
            .join("unavailable-retry.parquet")
            .to_string_lossy()
            .into_owned();
        std::fs::write(
            root.path().join("request.json"),
            serde_json::to_vec(&relocated).unwrap(),
        )
        .unwrap();
        let (result, work) = support::resume(root.path(), "run");
        let assemblies = work
            .iter()
            .filter(|stage| stage.as_str() == "backfill-assembly")
            .count();
        assert_eq!(
            assemblies,
            match phase {
                "processing" => 2,
                "group/a" => 1,
                _ => 0,
            }
        );
        assert!(
            !work
                .iter()
                .any(|stage| stage == "admission" || stage == "processing")
        );
        if phase == "readback" || phase == "selection" {
            assert!(work.is_empty(), "{phase}: {work:?}");
        }
        assert_eq!(
            result.dataset.request,
            OperationRequest::Build(request.clone())
        );
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        check_values(&engine, &result, &rows()).await;
        let after = engine.inspect(&request.dataset).await.unwrap();
        for (key, value) in retained {
            if key.starts_with("phase/")
                || key.starts_with("backfill-prepared/")
                || key.starts_with("backfill-validation/")
                || key.starts_with("readback/")
            {
                assert_eq!(after[&key], value, "repeated completion: {phase}/{key}");
            }
        }
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn later_admission_fences_cutover_and_rebased_rebuild_reuses_source_work() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "stale", &rows());
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(
        engine
            .run_until(&request, "group/a")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained = engine.inspect(&request.dataset).await.unwrap();
    let prepared_group: TablePin = retained
        .iter()
        .find_map(|(key, value)| {
            (key.starts_with("backfill-prepared/") && !before.contains_key(key))
                .then(|| serde_json::from_value(value.clone()).unwrap())
        })
        .unwrap();
    let source: PhaseResult = retained
        .values()
        .find_map(|value| {
            serde_json::from_value::<PhaseResult>(value.clone())
                .ok()
                .filter(|phase| phase.operation == request.operation && phase.phase == "processing")
        })
        .unwrap();
    let update = UpdateSpec {
        dataset: request.dataset.clone(),
        operation: id("newer"),
        definition: "v1".into(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: input(
            &root.path().join("update.parquet"),
            &[(Some(2000), Some("newly accepted".into()))],
        ),
        deletions: None,
    };
    assert_eq!(
        engine
            .update_until(&update, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine.plan(&request).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    let newer = engine.update(&update).await.unwrap();
    request.operation = id("rebased");
    request.base = Some(newer.id.clone());
    std::fs::remove_file(&request.input).unwrap();
    let plan = engine.plan(&request).await.unwrap();
    assert!(plan.reusable_phases.contains(&"processing".into()));
    assert!(plan.reusable_phases.contains(&"validation".into()));
    let result = engine.run(&request).await.unwrap();
    assert_eq!(result.dataset.groups["source"], source.table.unwrap());
    assert_eq!(result.dataset.groups["a"], prepared_group);
    assert_eq!(result.dataset.parent_manifest, Some(newer.manifest));
    check_values(&engine, &result, &rows()).await;
    let after = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in retained {
        if key.starts_with("phase/") || key.starts_with("backfill-prepared/") {
            assert_eq!(after[&key], value);
        }
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn failed_or_invalidated_rebuild_keeps_previous_complete_selection() {
    let root = TempDir::new().unwrap();
    let (engine, base, original) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "missing-field", &rows());
    request.sql = "SELECT id FROM input".into();
    assert!(engine.run(&request).await.is_err());
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    let plan = engine.plan(&request).await.unwrap();
    assert!(plan.reusable_phases.contains(&"processing".into()));
    assert!(plan.required_phases.contains(&"groups".into()));
    request.operation = id("pending");
    request.sql = "SELECT * FROM input".into();
    assert_eq!(
        engine
            .run_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    engine
        .invalidate(&request.dataset, "dependency correction", None)
        .await
        .unwrap();
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    check_values(&engine, &base, &original).await;
    request.operation = id("requalified");
    let result = engine.run(&request).await.unwrap();
    assert!(result.dataset.eligibility > base.dataset.eligibility);
    check_values(&engine, &result, &rows()).await;
}

#[tokio::test]
async fn independent_rebuild_verifier_rejects_false_composition_and_input_claims() {
    let root = TempDir::new().unwrap();
    let (engine, old, _) = enriched(root.path()).await;
    engine
        .invalidate(&old.dataset.dataset, "requalify", None)
        .await
        .unwrap();
    let base = engine
        .run(&rebuild(root.path(), &old, "qualified", &rows()))
        .await
        .unwrap();
    assert!(base.dataset.eligibility > 0);
    let mut request = rebuild(root.path(), &base, "rebuild", &rows());
    request.sql = "SELECT id + 1 AS id, text FROM input".into();
    let release = engine.run(&request).await.unwrap();
    for kind in [
        "missing-group",
        "recipe",
        "request",
        "parent",
        "source",
        "input",
        "boundary",
        "eligibility",
        "changed-definition",
        "definition",
    ] {
        let mut changed = release.clone();
        match kind {
            "missing-group" => {
                changed.dataset.computed.remove("a");
                changed.dataset.groups.remove("a");
            }
            "recipe" => {
                changed
                    .dataset
                    .computed
                    .get_mut("a")
                    .unwrap()
                    .definition
                    .fields[0]
                    .expression = "id".into()
            }
            "request" => {
                changed.dataset.computed.get_mut("a").unwrap().request =
                    base.dataset.request.clone()
            }
            "parent" => changed.dataset.parent_manifest = None,
            "source" => {
                changed
                    .dataset
                    .computed
                    .get_mut("a")
                    .unwrap()
                    .source_metadata = base.dataset.groups["source"].metadata.clone()
            }
            "input" => {
                changed
                    .dataset
                    .inputs
                    .insert("old-evidence".into(), base.dataset.inputs["input"].clone());
            }
            "boundary" => changed.dataset.boundary = base.dataset.boundary,
            "eligibility" => {
                changed.dataset.eligibility = 0;
            }
            "changed-definition" => {
                changed.dataset.definition = "changed-without-invalidation".into();
                if let OperationRequest::Build(spec) = &mut changed.dataset.request {
                    spec.definition = changed.dataset.definition.clone();
                }
                for binding in changed.dataset.computed.values_mut() {
                    binding.request = changed.dataset.request.clone();
                }
            }
            "definition" => {
                if let OperationRequest::Build(spec) = &mut changed.dataset.request {
                    spec.base = None;
                }
            }
            _ => unreachable!(),
        }
        let error = verify_release(&forged(root.path(), kind, changed), limits())
            .await
            .unwrap_err();
        assert_eq!(error.code(), "invalid_group", "{kind}: {error}");
    }
}

#[tokio::test]
async fn rebuild_retry_identity_includes_base_and_self_base_refuses_before_admission() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "rebuild", &rows());
    request.base = Some(request.operation.clone());
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert_eq!(
        engine.plan(&request).await.unwrap_err().code(),
        "invalid_request"
    );
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "invalid_request"
    );
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
    request.base = Some(base.id);
    let result = engine.run(&request).await.unwrap();
    request.base = None;
    assert_eq!(
        engine.plan(&request).await.unwrap_err().code(),
        "operation_conflict"
    );
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "operation_conflict"
    );
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), result);
}
