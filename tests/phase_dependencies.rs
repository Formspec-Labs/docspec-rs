//! Recovery dependencies retain exact consumed copies across reusable-key changes.
#![cfg(feature = "test-util")]

use super::scalar;

use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    engine::verify_release,
    model::{
        ColumnAddition, ColumnType, PhaseResult, RunSpec, SchemaField, SchemaSpec, UpdateSpec,
    },
    storage,
};
use scalar::{fixture, id, input, limits};
use std::collections::BTreeMap;

#[tokio::test]
async fn processing_keeps_its_consumed_input_when_an_admission_key_is_rebound() {
    for (update, legacy) in [(false, false), (true, false), (false, true)] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let incoming = input(
            &root.path().join("replacement.parquet"),
            &[(Some(77), None)],
        );
        let build = RunSpec {
            dataset: id("comments"),
            operation: id("pending"),
            base: None,
            relationship: None,
            input: incoming.path.clone(),
            expected_digest: incoming.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        };
        let change = UpdateSpec {
            dataset: id("comments"),
            operation: id("pending"),
            base: base.id.clone(),
            definition: "v1".into(),
            keys: vec!["id".into()],
            replacements: incoming,
            deletions: None,
            relationship: None,
        };
        let stopped = if update {
            engine.update_until(&change, "processing").await
        } else {
            engine.run_until(&build, "processing").await
        };
        assert_eq!(stopped.unwrap_err().code(), "injected_failure");
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&build.dataset).await.unwrap();
        let rows = lifecycle.records(&view).await.unwrap();
        let phases: Vec<_> = rows
            .iter()
            .filter_map(|(key, value)| {
                serde_json::from_value::<PhaseResult>(value.clone())
                    .ok()
                    .filter(|phase| phase.operation == build.operation)
                    .map(|phase| (key, phase))
            })
            .collect();
        let (admission_key, admission) =
            phases.iter().find(|(_, p)| p.phase == "admission").unwrap();
        let (processing_key, processing) = phases
            .iter()
            .find(|(_, p)| p.phase == "processing")
            .unwrap();
        let input_name = if update { "replacements" } else { "input" };
        let exact = &processing.dependencies.as_ref().unwrap().admitted_inputs[input_name];
        assert_eq!(exact, &admission.outputs[0]);
        let change_record = if legacy {
            let mut value = serde_json::to_value(processing).unwrap();
            value.as_object_mut().unwrap().remove("dependencies");
            record(*processing_key, value).unwrap()
        } else {
            // Simulate a future retired admission being recomputed under its
            // material key. Processing must still name the original copy.
            let replacement = root.path().join("same-bytes.parquet");
            std::fs::copy(&exact.path, &replacement).unwrap();
            let mut rebound = admission.clone();
            rebound.outputs[0].path = replacement.to_string_lossy().into_owned();
            record(*admission_key, &rebound).unwrap()
        };
        // The replaced generation must really be retired. Its previous binding
        // cannot remain authoritative after either legacy or material rebinding.
        let retirement = engine
            .plan_phase_retirement(
                &build.dataset,
                &id("old-material-generation"),
                std::slice::from_ref(&change_record.0),
                "replace the dependency fixture generation",
            )
            .await
            .unwrap();
        engine.retire_phases(&retirement).await.unwrap();
        let view = lifecycle.open(&build.dataset).await.unwrap();
        lifecycle.commit(&view, vec![change_record]).await.unwrap();
        std::fs::remove_file(&build.input).unwrap();
        if !legacy {
            let expected = engine
                .plan_phase_retirement(
                    &build.dataset,
                    &id("observe-processing"),
                    &[(*processing_key).clone()],
                    "observe the original completion generation",
                )
                .await
                .unwrap();
            let before_plan = engine.inspect(&build.dataset).await.unwrap();
            let planned = if update {
                engine.plan_update(&change).await.unwrap()
            } else {
                // A different operation can reuse the exact original work even
                // when the request locator no longer exists and admission moved.
                engine
                    .plan(&RunSpec {
                        operation: id("reuse-from-another-operation"),
                        input: root
                            .path()
                            .join("unavailable.parquet")
                            .to_string_lossy()
                            .into_owned(),
                        ..build.clone()
                    })
                    .await
                    .unwrap()
            };
            let origin = &planned.reusable_phase_origins["processing"];
            assert_eq!(origin, &expected.records[0]);
            let observed: PhaseResult =
                serde_json::from_value(before_plan[&origin.key].clone()).unwrap();
            assert_eq!(observed.operation, build.operation);
            assert_eq!(
                observed.dependencies.as_ref().unwrap().admitted_inputs[input_name],
                *exact
            );
            if !update {
                assert_ne!(observed.operation, planned.operation);
            }
            assert_eq!(engine.inspect(&build.dataset).await.unwrap(), before_plan);
        }
        let release = if update {
            engine.update(&change).await.unwrap()
        } else {
            engine.run(&build).await.unwrap()
        };
        let release_key = if update {
            "pending/replacements"
        } else {
            "input"
        };
        assert_eq!(&release.dataset.inputs[release_key], exact);
        assert_eq!(
            release.dataset.groups["source"],
            *processing.table.as_ref().unwrap()
        );
        let after = engine.inspect(&build.dataset).await.unwrap();
        let validation = after
            .values()
            .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
            .find(|p| p.operation == build.operation && p.phase == "validation")
            .unwrap();
        if legacy {
            assert!(validation.dependencies.is_none());
        } else {
            assert_eq!(validation.dependencies, processing.dependencies);
        }
        verify_release(&release, limits()).await.unwrap();
    }
}

#[tokio::test]
async fn evolved_build_keeps_exact_consumed_schema_metadata() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let evolved = engine
        .evolve_schema(&SchemaSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("evolved"),
            base: base.id,
            definition: "v1".into(),
            next_definition: "v2".into(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "id".into(),
                },
                SchemaField {
                    id: 2,
                    name: "text".into(),
                },
            ],
            additions: vec![ColumnAddition {
                name: "flag".into(),
                data_type: ColumnType::Boolean,
                default: serde_json::json!(true),
            }],
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap();
    let mut group = scalar::spec(
        &evolved,
        "old-group",
        "derived",
        "observed",
        "CASE WHEN flag THEN id ELSE -1 END",
    );
    group.definition = "v2".into();
    assert_eq!(
        engine
            .backfill_until(&group, "files")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&group.dataset).await.unwrap();
    let files: Vec<_> = before
        .iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
        .collect();
    assert!(!files.is_empty());
    for (_, file) in &files {
        assert_eq!(
            file["source_metadata"],
            serde_json::to_value(&evolved.dataset.groups["source"].metadata).unwrap()
        );
    }
    let reordered = engine
        .evolve_schema(&SchemaSpec {
            dataset: evolved.dataset.dataset.clone(),
            operation: id("reordered"),
            base: evolved.id,
            definition: "v2".into(),
            next_definition: "v3".into(),
            fields: vec![
                SchemaField {
                    id: 2,
                    name: "text".into(),
                },
                SchemaField {
                    id: 1,
                    name: "id".into(),
                },
                SchemaField {
                    id: 3,
                    name: "flag".into(),
                },
            ],
            additions: vec![],
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap();
    group.operation = id("new-group");
    group.base = reordered.id;
    group.definition = "v3".into();
    assert_eq!(
        engine.plan_backfill(&group).await.unwrap().files_pending,
        Some(0)
    );
    let parent = engine.backfill(&group).await.unwrap();
    let after = engine.inspect(&group.dataset).await.unwrap();
    for (key, file) in files {
        assert_eq!(&after[key], file);
    }
    let incoming = input(&root.path().join("new.parquet"), &[(Some(77), None)]);
    let request = RunSpec {
        dataset: parent.dataset.dataset.clone(),
        operation: id("rebuilt"),
        base: Some(parent.id.clone()),
        relationship: None,
        input: incoming.path,
        expected_digest: incoming.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v3".into(),
        partition_by: Vec::new(),
    };
    let result = engine.run(&request).await.unwrap();
    let state = engine.inspect(&request.dataset).await.unwrap();
    let phase = state
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|p| p.operation == request.operation && p.phase == "processing")
        .unwrap();
    assert_eq!(
        phase.dependencies.unwrap().schema_metadata,
        vec![parent.dataset.groups["source"].metadata.clone()]
    );
    verify_release(&result, limits()).await.unwrap();
    drop(engine);
    let reopened = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(reopened.run(&request).await.unwrap(), result);
}
