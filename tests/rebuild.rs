//! Complete source rebuilds preserve scalar definitions and qualify a new selection.

use super::enriched;
use super::scalar;
#[cfg(feature = "test-util")]
use super::support;

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
        relationship: None,
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
async fn reordered_full_rebuild_preserves_source_and_computed_field_meaning() {
    use arrow::array::{Int64Array, StringArray};
    use futures::TryStreamExt;
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "reordered", &rows());
    request.sql = "SELECT text, id FROM input".into();
    let result = engine.run(&request).await.unwrap();
    assert_eq!(base.dataset.computed["a"].input_fields["text"], 2);
    assert_eq!(result.dataset.computed["a"].input_fields["text"], 2);
    assert_eq!(base.dataset.computed["b"].input_fields["id"], 1);
    assert_eq!(result.dataset.computed["b"].input_fields["id"], 1);
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let before = docspec::storage::open_pin(&base.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let after = docspec::storage::open_pin(&result.dataset.groups["source"], &budget)
        .await
        .unwrap();
    assert_eq!(
        after.metadata().current_schema(),
        before.metadata().current_schema()
    );
    assert_eq!(
        after.metadata().last_column_id(),
        before.metadata().last_column_id()
    );
    let mut stream = engine.read_combined(&result).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let text = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lengths = arrow::compute::cast(
            batch.column_by_name("text_length").unwrap(),
            &arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        let next = batch
            .column_by_name("next_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(
            ids.iter()
                .zip(text.iter())
                .zip(lengths.iter())
                .zip(next.iter())
                .map(|(((id, text), length), next)| ((id, text.map(str::to_owned)), length, next)),
        );
    }
    actual.sort();
    let mut expected = rows()
        .into_iter()
        .map(|row| {
            (
                row.clone(),
                row.1.as_ref().map(|s| s.chars().count() as i64),
                row.0.map(|v| v + 1),
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
    // Rehashing cannot redirect an inherited expression to another retained field.
    let mut wrong = result.clone();
    *wrong
        .dataset
        .computed
        .get_mut("a")
        .unwrap()
        .input_fields
        .get_mut("text")
        .unwrap() = 1;
    assert_eq!(
        verify_release(&forged(root.path(), "old-binding", wrong), limits())
            .await
            .unwrap_err()
            .code(),
        "invalid_group"
    );
    verify_release(&result, limits()).await.unwrap();
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
    assert!(
        engine
            .change_certificate(&result.version_ref())
            .await
            .unwrap()
            .is_none()
    );
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
    let (engine, initial, _) = enriched(root.path()).await;
    // Establish origin fields before capturing reusable work. Later value-only
    // source changes retain that exact interpretation and allocation history.
    let base = engine
        .update(&UpdateSpec {
            dataset: initial.dataset.dataset.clone(),
            operation: id("with-origins"),
            definition: initial.dataset.definition.clone(),
            base: initial.id.clone(),
            keys: vec!["id".into()],
            replacements: input(
                &root.path().join("with-origins.parquet"),
                &[(Some(1999), Some("existing origin".into()))],
            ),
            deletions: None,
        })
        .await
        .unwrap();
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
    assert!(plan.reusable_phases.contains(&"admission".into()));
    assert!(!plan.reusable_phases.contains(&"processing".into()));
    assert!(plan.required_phases.contains(&"processing".into()));
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

#[tokio::test]
async fn based_rebuild_preserves_renamed_hidden_fields_and_new_nullable_defaults() {
    use docspec::model::{ColumnAddition, ColumnType, SchemaField, SchemaSpec};
    use futures::TryStreamExt;
    use std::collections::BTreeMap;
    let root = TempDir::new().unwrap();
    let source = input(&root.path().join("input.parquet"), &rows());
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let initial_spec = RunSpec {
        dataset: id("schema-rebuild"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT id, text, id AS discarded FROM input".into(),
        definition: "v1".into(),
    };
    let initial = engine.run(&initial_spec).await.unwrap();
    let edit = SchemaSpec {
        dataset: initial.dataset.dataset.clone(),
        operation: id("renamed-hidden"),
        base: initial.id.clone(),
        definition: "v1".into(),
        next_definition: "v2".into(),
        fields: vec![
            SchemaField {
                id: 2,
                name: "body".into(),
            },
            SchemaField {
                id: 1,
                name: "comment_id".into(),
            },
        ],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    };
    let edited = engine.evolve_schema(&edit).await.unwrap();
    let mut build = RunSpec {
        operation: id("rebuilt"),
        base: Some(edited.id.clone()),
        definition: "v2".into(),
        // Query order differs from retained public order. Names declare correspondence.
        sql: "SELECT id AS comment_id, text AS body FROM input".into(),
        ..initial_spec.clone()
    };
    let rebuilt = engine.run(&build).await.unwrap();
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let before = docspec::storage::open_pin(&edited.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let after = docspec::storage::open_pin(&rebuilt.dataset.groups["source"], &budget)
        .await
        .unwrap();
    assert_eq!(
        after.metadata().current_schema(),
        before.metadata().current_schema()
    );
    assert_eq!(after.metadata().last_column_id(), 3);
    assert_eq!(
        after
            .metadata()
            .schemas_iter()
            .map(|s| (s.schema_id(), s))
            .collect::<BTreeMap<_, _>>(),
        before
            .metadata()
            .schemas_iter()
            .map(|s| (s.schema_id(), s))
            .collect::<BTreeMap<_, _>>()
    );
    assert!(
        after
            .metadata()
            .current_schema()
            .field_by_name("discarded")
            .is_none()
    );
    assert_eq!(
        after
            .metadata()
            .current_schema()
            .field_by_name("body")
            .unwrap()
            .id,
        2
    );
    assert_eq!(
        after
            .metadata()
            .current_schema()
            .field_by_name("comment_id")
            .unwrap()
            .id,
        1
    );
    verify_release(&initial, limits()).await.unwrap();
    verify_release(&edited, limits()).await.unwrap();
    verify_release(&rebuilt, limits()).await.unwrap();
    // Allocate beyond a hidden historical field, then preserve omission/default meaning.
    let addition = SchemaSpec {
        operation: id("added"),
        base: rebuilt.id.clone(),
        definition: "v2".into(),
        next_definition: "v3".into(),
        additions: vec![ColumnAddition {
            name: "score".into(),
            data_type: ColumnType::Long,
            default: 7.into(),
        }],
        ..edit
    };
    let added = engine.evolve_schema(&addition).await.unwrap();
    build.operation = id("rebuilt-added");
    build.base = Some(added.id.clone());
    build.definition = "v3".into();
    let added_rebuild = engine.run(&build).await.unwrap();
    let added_native = docspec::storage::open_pin(&added.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let rebuilt_native =
        docspec::storage::open_pin(&added_rebuild.dataset.groups["source"], &budget)
            .await
            .unwrap();
    assert_eq!(
        rebuilt_native.metadata().current_schema(),
        added_native.metadata().current_schema()
    );
    assert_eq!(rebuilt_native.metadata().last_column_id(), 5);
    assert_eq!(
        rebuilt_native
            .metadata()
            .current_schema()
            .field_by_name("score")
            .unwrap()
            .id,
        4
    );
    let mut stream = engine.read_group(&added_rebuild, "source").await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("comment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let text = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let score = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        actual.extend(
            ids.iter()
                .zip(text.iter())
                .zip(score.iter())
                .map(|((id, text), score)| ((id, text.map(str::to_owned)), score)),
        );
    }
    actual.sort();
    let mut expected = rows()
        .into_iter()
        .map(|row| (row, Some(7)))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
    verify_release(&added_rebuild, limits()).await.unwrap();
    for (operation, sql) in [
        ("wrong-name", "SELECT id, text AS body FROM input"),
        ("missing-field", "SELECT text AS body FROM input"),
        (
            "wrong-type",
            "SELECT CAST(id AS VARCHAR) AS comment_id, text AS body FROM input",
        ),
        (
            "extra-field",
            "SELECT id AS comment_id, text AS body, id AS extra FROM input",
        ),
    ] {
        let invalid = RunSpec {
            operation: id(operation),
            base: Some(added_rebuild.id.clone()),
            sql: sql.into(),
            ..build.clone()
        };
        assert_eq!(
            engine.run(&invalid).await.unwrap_err().code(),
            "schema_mismatch"
        );
        assert_eq!(
            engine.latest(&initial.dataset.dataset).await.unwrap(),
            added_rebuild
        );
    }
}

#[tokio::test]
async fn retained_legacy_rebuild_interpretation_stays_readable_while_new_pins_require_stable_ids() {
    let root = TempDir::new().unwrap();
    let (engine, initial, _) = scalar::fixture(root.path(), false).await;
    let OperationRequest::Build(mut request) = initial.dataset.request.clone() else {
        panic!("Build fixture");
    };
    request.operation = id("legacy-layout");
    request.sql = "SELECT text, id FROM input".into();
    let layout = engine.run(&request).await.unwrap();
    let current_format = layout.dataset.format;
    let mut legacy = layout.clone();
    legacy.dataset.format = 11;
    legacy.dataset.parent_manifest = Some(initial.manifest.clone());
    let OperationRequest::Build(spec) = &mut legacy.dataset.request else {
        panic!("Build fixture");
    };
    spec.base = Some(initial.id.clone());
    // This represents the older explicitly supported name-corresponded layout.
    let legacy = forged(root.path(), "legacy-based", legacy);
    verify_release(&legacy, limits()).await.unwrap();
    let mut current = legacy;
    current.dataset.format = current_format;
    assert_eq!(
        verify_release(
            &forged(root.path(), "new-format-renumbering", current),
            limits()
        )
        .await
        .unwrap_err()
        .code(),
        "invalid_schema"
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn based_rebuild_recomputes_only_when_retained_schema_history_changes() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = enriched(root.path()).await;
    let mut request = rebuild(root.path(), &base, "before-origins", &rows());
    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&request.dataset).await.unwrap();
    let first: PhaseResult = records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/"))
        .find_map(|(_, value)| {
            serde_json::from_value::<PhaseResult>(value.clone())
                .ok()
                .filter(|phase| phase.operation == request.operation && phase.phase == "processing")
        })
        .unwrap();
    let updated = engine
        .update(&UpdateSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("add-origins"),
            definition: base.dataset.definition.clone(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: input(
                &root.path().join("add-origins.parquet"),
                &[(Some(2000), Some("newly observed".into()))],
            ),
            deletions: None,
        })
        .await
        .unwrap();
    request.operation = id("after-origins");
    request.base = Some(updated.id.clone());
    let plan = engine.plan(&request).await.unwrap();
    assert!(plan.reusable_phases.contains(&"admission".into()));
    assert!(plan.required_phases.contains(&"processing".into()));
    let rebuilt = engine.run(&request).await.unwrap();
    assert_ne!(rebuilt.dataset.groups["source"], first.table.unwrap());
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let before = docspec::storage::open_pin(&updated.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let after = docspec::storage::open_pin(&rebuilt.dataset.groups["source"], &budget)
        .await
        .unwrap();
    assert_eq!(
        after.metadata().current_schema(),
        before.metadata().current_schema()
    );
    assert_eq!(
        after.metadata().last_column_id(),
        before.metadata().last_column_id()
    );
    check_values(&engine, &rebuilt, &rows()).await;
    verify_release(&rebuilt, limits()).await.unwrap();
}
