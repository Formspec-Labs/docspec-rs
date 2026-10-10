//! Actual worker death and fresh CLI recovery of continuous scalar work.
// Rust guideline compliant 2026-02-21
#![cfg(all(feature = "test-util", unix))]

use super::{
    continuous_backfill::{
        assert_retained as retained, assert_values, completed_work as completions,
        continuous as request,
    },
    scalar, support,
};
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::DataType,
};
use docspec::{
    Engine,
    model::{OperationRequest, ReleasePin, UpdateSpec},
    storage,
};
use futures::TryStreamExt;
use scalar::{fixture, id, input, limits};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

fn assert_child(records: &BTreeMap<String, Value>, child: &docspec::model::Id, base: &ReleasePin) {
    let OperationRequest::Backfill(child_request) =
        serde_json::from_value(records[&format!("request/{child}")].clone()).unwrap()
    else {
        panic!("continuous child must retain its exact Backfill request");
    };
    assert_eq!(child_request.operation, *child);
    assert_eq!(child_request.base, base.id);
    assert_eq!(
        records[&format!("continuous-child/{child}")]["parent"],
        "continuous"
    );
    assert_eq!(
        records[&format!("continuous-child/{child}")]["child"],
        child.as_str()
    );
}

/// Checks every value and its exact occurrence, including duplicate and null keys.
async fn assert_output(engine: &Engine, release: &ReleasePin, rows: Vec<scalar::Row>) {
    assert_values(engine, release, rows).await;

    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let source = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let source_stream = storage::read_occurrences(&source, &budget).await.unwrap();
    futures::pin_mut!(source_stream);
    let mut occurrences = BTreeMap::new();
    while let Some(batch) = source_stream.try_next().await.unwrap() {
        let files = batch
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
        let texts = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                occurrences
                    .insert(
                        (files.value(row).to_owned(), positions.value(row)),
                        (!texts.is_null(row)).then(|| texts.value(row).chars().count() as i64),
                    )
                    .is_none(),
                "source occurrence must be unique"
            );
        }
    }
    let mut computed = engine.read_group(release, "length").await.unwrap();
    let mut coverage = BTreeMap::new();
    while let Some(batch) = computed.try_next().await.unwrap() {
        let files = batch
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
        let lengths = arrow::compute::cast(
            batch.column_by_name("text_length").unwrap(),
            &DataType::Int64,
        )
        .unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                coverage
                    .insert(
                        (files.value(row).to_owned(), positions.value(row)),
                        (!lengths.is_null(row)).then(|| lengths.value(row)),
                    )
                    .is_none(),
                "computed occurrence must be covered exactly once"
            );
        }
    }
    assert_eq!(coverage, occurrences);
    let binding = &release.dataset.computed["length"];
    assert_eq!(
        binding.source_metadata,
        release.dataset.groups["source"].metadata
    );
    let origins: Value =
        serde_json::from_slice(&std::fs::read(&binding.origins.as_ref().unwrap().path).unwrap())
            .unwrap();
    let records = engine.inspect(&release.dataset.dataset).await.unwrap();
    let mut generations = BTreeSet::new();
    let mut evidence_rows = 0;
    for origin in origins["files"].as_array().unwrap() {
        let key = origin["generation"]["key"].as_str().unwrap();
        assert!(generations.insert(key));
        assert_eq!(origin["completion"], records[key]);
        evidence_rows += origin["completion"]["rows"].as_u64().unwrap();
    }
    assert_eq!(evidence_rows, occurrences.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_parent_resumes_only_missing_work_and_selects_exact_child() {
    for phase in [
        "continuous-admission",
        "files",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = request(&base);
        drop(engine);
        support::kill_after(root.path(), &request, "continuous-backfill", phase);
        // The original ingestion file is no longer required after source admission.
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let plan = engine.plan_continuous_backfill(&request).await.unwrap();
        let child = plan.child.clone().unwrap();
        let before = engine.inspect(&base.dataset.dataset).await.unwrap();
        assert_child(&before, &child, &base);
        let completed = completions(&before);
        if phase == "continuous-admission" {
            assert!(completed.is_empty());
        } else {
            assert!(
                completed
                    .keys()
                    .any(|key| key.starts_with("phase/backfill/"))
            );
        }
        for (prefix, phases) in [
            (
                "backfill-prepared/",
                &["processing", "validation", "readback", "selection"][..],
            ),
            (
                "backfill-validation/",
                &["validation", "readback", "selection"][..],
            ),
            ("backfill-readback/", &["readback", "selection"][..]),
        ] {
            assert_eq!(
                completed.keys().any(|key| key.starts_with(prefix)),
                phases.contains(&phase),
                "{phase}/{prefix}"
            );
        }
        if phase == "selection" {
            assert_eq!(plan.completed.as_ref().unwrap().id, child);
        } else {
            assert!(plan.completed.is_none());
            assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
        }
        drop(engine);
        let (release, work) = support::resume(root.path(), "continuous-backfill");
        let expected = match phase {
            "continuous-admission" => {
                let file_count = base.dataset.groups["source"]
                    .objects
                    .iter()
                    .filter(|object| object.path.ends_with(".parquet"))
                    .count();
                let mut work = vec!["backfill-assembly"];
                work.extend(std::iter::repeat_n("backfill-file", file_count));
                work.extend(["backfill-validation", "backfill-readback"]);
                work
            }
            "files" => vec![
                "backfill-assembly",
                "backfill-validation",
                "backfill-readback",
            ],
            "processing" => vec!["backfill-validation", "backfill-readback"],
            "validation" => vec!["backfill-readback"],
            "readback" | "selection" => vec![],
            _ => unreachable!(),
        };
        assert_eq!(
            work, expected,
            "{phase}: resume may execute only missing phases"
        );
        assert_eq!(release.id, child);
        assert_ne!(release.id, request.backfill.operation);
        assert_eq!(
            release.dataset.groups["source"],
            base.dataset.groups["source"]
        );
        assert_eq!(release.dataset.boundary, base.dataset.boundary);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_output(&engine, &release, rows).await;
        let after = engine.inspect(&base.dataset.dataset).await.unwrap();
        retained(&completed, &after);
        assert_eq!(
            after["continuous-result/continuous"]["selected"]["version"],
            child.as_str()
        );
        assert_eq!(
            engine
                .plan_continuous_backfill(&request)
                .await
                .unwrap()
                .completed,
            Some(release.clone())
        );
        assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), release);
        drop(engine);
        let (retried, work) = support::resume(root.path(), "continuous-backfill");
        assert_eq!(retried, release);
        assert!(work.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_parent_catches_up_source_edits_and_keeps_original_file_evidence() {
    for phase in ["files", "readback"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = request(&base);
        drop(engine);
        support::kill_after(root.path(), &request, "continuous-backfill", phase);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let original_child = engine
            .plan_continuous_backfill(&request)
            .await
            .unwrap()
            .child
            .unwrap();
        let before = completions(&engine.inspect(&base.dataset.dataset).await.unwrap());
        let replacements = vec![
            (Some(0), Some("changed é🎉".into())),
            (Some(9000), Some(String::new())),
        ];
        let successor = engine
            .update(&UpdateSpec {
                dataset: base.dataset.dataset.clone(),
                operation: id("source-edit"),
                definition: "v1".into(),
                base: base.id.clone(),
                keys: vec!["id".into()],
                replacements: input(&root.path().join("replacement.parquet"), &replacements),
                deletions: None,
            })
            .await
            .unwrap();
        drop(engine);
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        std::fs::remove_file(root.path().join("replacement.parquet")).unwrap();
        let (release, work) = support::resume(root.path(), "continuous-backfill");
        assert_ne!(release.id, original_child);
        assert!(work.iter().any(|phase| phase == "backfill-file"));
        assert_eq!(
            release.dataset.groups["source"],
            successor.dataset.groups["source"]
        );
        assert_eq!(release.dataset.boundary, successor.dataset.boundary);
        let mut expected = rows
            .into_iter()
            .filter(|row| row.0 != Some(0))
            .collect::<Vec<_>>();
        expected.extend(replacements);
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        assert_output(&engine, &release, expected).await;
        let after = engine.inspect(&base.dataset.dataset).await.unwrap();
        retained(&before, &after);
        assert_child(&after, &release.id, &successor);
        let origins: Value = serde_json::from_slice(
            &std::fs::read(
                &release.dataset.computed["length"]
                    .origins
                    .as_ref()
                    .unwrap()
                    .path,
            )
            .unwrap(),
        )
        .unwrap();
        let mut reused = 0;
        let mut computed = 0;
        for origin in origins["files"].as_array().unwrap() {
            let key = origin["generation"]["key"].as_str().unwrap();
            if let Some(prior) = before.get(key) {
                assert_eq!(&origin["completion"], prior);
                assert_eq!(prior["execution"]["operation"], original_child.as_str());
                reused += 1;
            } else {
                assert_eq!(
                    origin["completion"]["execution"]["operation"],
                    release.id.as_str()
                );
                computed += 1;
            }
        }
        assert!(reused > 0 && computed > 0);
        let mut expected_work = vec!["backfill-assembly"];
        expected_work.extend(std::iter::repeat_n("backfill-file", computed));
        expected_work.extend(["backfill-validation", "backfill-readback"]);
        assert_eq!(
            work, expected_work,
            "catch-up must compute only new file generations"
        );
        assert_eq!(
            after["continuous-result/continuous"]["selected"]["version"],
            release.id.as_str()
        );
        assert_eq!(
            engine
                .plan_continuous_backfill(&request)
                .await
                .unwrap()
                .completed,
            Some(release.clone())
        );
        assert_eq!(
            after[&format!("continuous-child/{}", release.id)]["previous"],
            original_child.as_str()
        );
        drop(engine);
        let (retried, work) = support::resume(root.path(), "continuous-backfill");
        assert_eq!(retried, release);
        assert!(work.is_empty());
    }
}
