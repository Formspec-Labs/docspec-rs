//! Field groups checked against direct Rust reconstruction and retained recovery state.

#[cfg(feature = "test-util")]
use super::support;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    engine::verify_release,
    model::{ReleasePin, RunSpec, UpdateSpec},
    storage,
};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use std::{fs::File, sync::Arc};
use tempfile::TempDir;

use super::scalar;
use scalar::{fixture, id, input, limits, spec};

#[cfg(feature = "test-util")]
async fn canonical_file_records(
    root: &std::path::Path,
) -> std::collections::BTreeMap<String, (i64, serde_json::Value)> {
    let catalog = storage::local_catalog(&root.join("workspace"))
        .await
        .unwrap();
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let mut records = std::collections::BTreeMap::<String, (i64, serde_json::Value)>::new();
    for namespace in catalog.list_namespaces(None).await.unwrap() {
        for ident in catalog.list_tables(&namespace).await.unwrap() {
            if ident.name() != "lifecycle" {
                continue;
            }
            let native = catalog.load_table(&ident).await.unwrap();
            let stream = storage::read_batches(&native, &budget).await.unwrap();
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await.unwrap() {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let revisions = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let key = keys.value(row);
                    let revision = revisions.value(row);
                    if key.starts_with("phase/backfill/")
                        && records.get(key).is_none_or(|(old, _)| *old < revision)
                    {
                        records.insert(
                            key.to_owned(),
                            (revision, serde_json::from_str(values.value(row)).unwrap()),
                        );
                    }
                }
            }
        }
    }
    records
}

async fn combined(engine: &Engine, release: &ReleasePin) -> Vec<(scalar::Row, Option<i64>)> {
    let mut stream = engine.read_combined(release).await.unwrap();
    let mut result = vec![];
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lengths = arrow::compute::cast(batch.column(2), &DataType::Int64).unwrap();
        let length = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        result.extend(
            ids.iter()
                .zip(text.iter())
                .zip(length.iter())
                .map(|((id, text), length)| ((id, text.map(str::to_owned)), length)),
        );
    }
    result.sort();
    result
}

#[tokio::test]
async fn combined_values_preserve_duplicate_null_empty_and_unicode_occurrences() {
    let pin_budget = docspec::budget::Budget::new(limits()).unwrap();
    let root = TempDir::new().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let request = spec(&base, "lengths", "length", "text_length", "length(text)");
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert!(plan.files_pending.is_some_and(|files| files > 1));
    assert_eq!(plan.files_completed, Some(0));
    let release = engine.backfill(&request).await.unwrap();
    assert_eq!(
        base.dataset.groups["source"],
        release.dataset.groups["source"]
    );
    assert_eq!(base.dataset.boundary, release.dataset.boundary);
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let length = row.1.as_ref().map(|text| text.chars().count() as i64);
            (row, length)
        })
        .collect();
    expected.sort();
    assert_eq!(combined(&engine, &release).await, expected);
    verify_release(&release, limits()).await.unwrap();
    assert_eq!(engine.backfill(&request).await.unwrap(), release);
    let done = engine.plan_backfill(&request).await.unwrap();
    assert!(done.completed);
    assert_eq!(done.files_pending, None);
    assert_eq!(done.files_completed, None);
    let native = storage::open_pin(&release.dataset.groups["length"], &pin_budget)
        .await
        .unwrap();
    assert_eq!(
        release.dataset.computed["length"].native_field_ids[&10],
        native
            .metadata()
            .current_schema()
            .field_by_name("text_length")
            .unwrap()
            .id
    );
    let mut conflicting = request;
    conflicting.fields[0].expression = "id + 1".into();
    assert_eq!(
        engine.backfill(&conflicting).await.unwrap_err().code(),
        "operation_conflict"
    );
}

#[tokio::test]
async fn empty_source_has_typed_complete_coverage_without_fake_rows() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), true).await;
    let request = spec(
        &base,
        "empty-group",
        "length",
        "text_length",
        "length(text)",
    );
    let release = engine.backfill(&request).await.unwrap();
    assert_eq!(release.dataset.groups["length"].rows, 0);
    assert!(combined(&engine, &release).await.is_empty());
    assert!(engine.plan_backfill(&request).await.unwrap().completed);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn retained_file_handoffs_resume_after_reopen_and_external_input_loss() {
    for phase in ["files", "processing", "validation", "readback", "selection"] {
        let root = TempDir::new().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = spec(&base, "resume", "length", "text_length", "length(text)");
        assert_eq!(
            engine
                .backfill_until(&request, phase)
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let before = engine.inspect(&request.dataset).await.unwrap();
        let completed: Vec<_> = before
            .iter()
            .filter(|(key, _)| {
                key.starts_with("phase/backfill/")
                    || key.starts_with("backfill-validation/")
                    || key.starts_with("backfill-readback/")
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert!(!completed.is_empty());
        if ["validation", "readback", "selection"].contains(&phase) {
            assert!(
                completed
                    .iter()
                    .any(|(key, _)| key.starts_with("backfill-validation/"))
            );
        }
        if ["readback", "selection"].contains(&phase) {
            assert!(
                completed
                    .iter()
                    .any(|(key, _)| key.starts_with("backfill-readback/"))
            );
        }
        drop(engine);
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let release = engine.backfill(&request).await.unwrap();
        assert_eq!(combined(&engine, &release).await.len(), rows.len());
        let after = engine.inspect(&request.dataset).await.unwrap();
        for (key, value) in completed {
            assert_eq!(after[&key], value);
        }
    }
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_backfill_workers_resume_retained_files_validation_and_readback() {
    for phase in ["files", "processing", "validation", "readback", "selection"] {
        let root = TempDir::new().unwrap();
        let (engine, base, rows) = fixture(root.path(), false).await;
        let request = spec(&base, "recovered", "length", "text_length", "length(text)");
        drop(engine);
        support::kill_after(root.path(), &request, "backfill", phase);
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let before = engine.inspect(&request.dataset).await.unwrap();
        assert_eq!(
            engine.plan_backfill(&request).await.unwrap().files_pending,
            if phase == "selection" { None } else { Some(0) }
        );
        let completed: Vec<_> = before
            .iter()
            .filter(|(key, _)| {
                key.starts_with("phase/backfill/")
                    || key.starts_with("backfill-prepared/")
                    || key.starts_with("backfill-validation/")
                    || key.starts_with("backfill-readback/")
            })
            .collect();
        assert!(!completed.is_empty());
        if ["processing", "validation", "readback", "selection"].contains(&phase) {
            assert!(
                completed
                    .iter()
                    .any(|(key, _)| key.starts_with("backfill-prepared/"))
            );
        }
        if ["validation", "readback", "selection"].contains(&phase) {
            assert!(
                completed
                    .iter()
                    .any(|(key, _)| key.starts_with("backfill-validation/"))
            );
        }
        if ["readback", "selection"].contains(&phase) {
            assert!(
                completed
                    .iter()
                    .any(|(key, _)| key.starts_with("backfill-readback/"))
            );
        }
        if phase == "selection" {
            assert_eq!(
                engine.latest(&request.dataset).await.unwrap().id,
                request.operation
            );
        } else {
            assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        }
        drop(engine);
        let (release, work) = support::resume(root.path(), "backfill");
        let expected = match phase {
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
        assert_eq!(work, expected, "{phase}: only missing phases may execute");
        let engine = Engine::open(root.path().join("workspace"), limits())
            .await
            .unwrap();
        let mut expected: Vec<_> = rows
            .into_iter()
            .map(|row| {
                let length = row.1.as_ref().map(|text| text.chars().count() as i64);
                (row, length)
            })
            .collect();
        expected.sort();
        assert_eq!(combined(&engine, &release).await, expected);
        assert_eq!(
            release.dataset.groups["source"],
            base.dataset.groups["source"]
        );
        let after = engine.inspect(&request.dataset).await.unwrap();
        for (key, value) in completed {
            assert_eq!(
                &after[key], value,
                "{phase}: retained completion must be reused"
            );
            if key.starts_with("backfill-readback/") {
                let public: ReleasePin = serde_json::from_value(value.clone()).unwrap();
                assert_eq!(public, release);
                assert_eq!(value["scalar_validator"], "docspec.scalar-validation.v1");
            }
        }
        assert_eq!(engine.backfill(&request).await.unwrap(), release);
        verify_release(&base, limits()).await.unwrap();
    }
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn stale_source_retains_files_and_catchup_reuses_unchanged_objects() {
    let root = TempDir::new().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let request = spec(&base, "old-source", "length", "text_length", "length(text)");
    assert_eq!(
        engine
            .backfill_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original_files = canonical_file_records(root.path()).await;
    assert!(!original_files.is_empty());
    let replacement = vec![(Some(0), Some("changed".into()))];
    let update = UpdateSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("update"),
        definition: "v1".into(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: input(&root.path().join("replacement.parquet"), &replacement),
        deletions: None,
    };
    let latest = engine.update(&update).await.unwrap();
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    assert_eq!(
        engine
            .plan_backfill(&request)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("stale_source_boundary")
    );
    let catchup = spec(&latest, "catchup", "length", "text_length", "length(text)");
    let plan = engine.plan_backfill(&catchup).await.unwrap();
    assert!(plan.files_completed.is_some_and(|files| files > 0));
    assert!(plan.files_pending.is_some_and(|files| files > 0));
    assert_eq!(
        engine
            .backfill_until(&catchup, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let prepared_records = engine.inspect(&catchup.dataset).await.unwrap();
    let prepared = prepared_records
        .iter()
        .find(|(key, value)| {
            key.starts_with("backfill-prepared/")
                && value.get("dependencies").is_some_and(|dependencies| {
                    dependencies["source"]["metadata"]
                        == serde_json::to_value(&latest.dataset.groups["source"].metadata).unwrap()
                })
                && value
                    .get("origins")
                    .is_some_and(|origins| !origins.is_null())
        })
        .unwrap()
        .1;
    let origins_ref: docspec::model::ObjectRef =
        serde_json::from_value(prepared["origins"].clone()).unwrap();
    let origins_bytes = std::fs::read(&origins_ref.path).unwrap();
    assert_eq!(
        origins_ref.digest,
        blake3::hash(&origins_bytes).to_hex().to_string()
    );
    let origins: serde_json::Value = serde_json::from_slice(&origins_bytes).unwrap();
    let canonical = canonical_file_records(root.path()).await;
    let mut reused = 0;
    let mut computed = 0;
    for origin in origins["files"].as_array().unwrap() {
        let key = origin["generation"]["key"].as_str().unwrap();
        let (revision, completion) = &canonical[key];
        assert_eq!(origin["generation"]["revision"], *revision);
        assert_eq!(&origin["completion"], completion);
        let execution = &completion["execution"];
        if let Some(original) = original_files.get(key) {
            assert_eq!(&canonical[key], original);
            assert_eq!(execution["operation"], request.operation.as_str());
            assert_eq!(
                completion["source_metadata"],
                serde_json::to_value(&base.dataset.groups["source"].metadata).unwrap()
            );
            reused += 1;
        } else {
            assert_eq!(execution["operation"], catchup.operation.as_str());
            assert_eq!(
                completion["source_metadata"],
                serde_json::to_value(&latest.dataset.groups["source"].metadata).unwrap()
            );
            computed += 1;
        }
        let declaration_ref: docspec::model::ObjectRef =
            serde_json::from_value(execution["declaration"].clone()).unwrap();
        let declaration_bytes = std::fs::read(&declaration_ref.path).unwrap();
        assert_eq!(
            declaration_ref.digest,
            blake3::hash(&declaration_bytes).to_hex().to_string()
        );
        let declaration: serde_json::Value = serde_json::from_slice(&declaration_bytes).unwrap();
        assert_eq!(declaration["processor"], origins["processor"]);
        assert_eq!(
            declaration["definition"],
            serde_json::to_value(catchup.group_definition()).unwrap()
        );
        assert_eq!(declaration["input_fields"], origins["input_fields"]);
    }
    assert!(reused > 0 && computed > 0);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = engine.backfill(&catchup).await.unwrap();
    assert_eq!(
        release.dataset.computed["length"].origins.as_ref(),
        Some(&origins_ref)
    );
    assert_eq!(std::fs::read(&origins_ref.path).unwrap(), origins_bytes);
    let after = engine.inspect(&catchup.dataset).await.unwrap();
    let validation = after
        .iter()
        .find(|(key, value)| {
            key.starts_with("backfill-validation/") && value["origins"] == prepared["origins"]
        })
        .unwrap()
        .1;
    assert_eq!(validation["origins"], prepared["origins"]);
    assert_eq!(engine.backfill(&catchup).await.unwrap(), release);
    assert_eq!(canonical_file_records(root.path()).await, canonical);
    let mut expected: Vec<_> = rows
        .into_iter()
        .filter(|row| row.0 != Some(0))
        .chain(replacement)
        .map(|row| {
            let length = row.1.as_ref().map(|text| text.chars().count() as i64);
            (row, length)
        })
        .collect();
    expected.sort();
    assert_eq!(combined(&engine, &release).await, expected);
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn concurrent_groups_merge_fresh_selection_and_same_group_writers_conflict() {
    let root = TempDir::new().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let first = spec(&base, "first", "a", "text_length", "length(text)");
    let second = spec(&base, "second", "b", "next_id", "id + 1");
    // Retain a whole release readback before another independent group wins.
    assert_eq!(
        engine
            .backfill_until(&first, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let prior = engine.inspect(&first.dataset).await.unwrap();
    let first_readback = prior
        .iter()
        .find(|(key, _)| key.starts_with("backfill-readback/first/"))
        .unwrap()
        .1
        .clone();
    let second_release = engine.backfill(&second).await.unwrap();
    let first_release = engine.backfill(&first).await.unwrap();
    assert_eq!(
        first_release.dataset.groups["b"],
        second_release.dataset.groups["b"]
    );
    assert_eq!(first_release.dataset.groups.len(), 3);
    assert_ne!(
        serde_json::to_value(&first_release).unwrap(),
        first_readback
    );
    assert_eq!(combined(&engine, &first_release).await.len(), rows.len());
    assert_eq!(engine.backfill(&second).await.unwrap(), second_release);
    assert_eq!(
        engine.latest(&base.dataset.dataset).await.unwrap(),
        first_release
    );
    // Two prepared replacements capture the same target; only one may replace it.
    let mut replacement = spec(
        &first_release,
        "replace",
        "a",
        "text_length",
        "CAST(1 AS INT)",
    );
    let mut loser = replacement.clone();
    loser.operation = id("loser");
    loser.fields[0].expression = "CAST(2 AS INT)".into();
    assert_eq!(
        engine
            .backfill_until(&replacement, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine
            .backfill_until(&loser, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let winner = engine.backfill(&replacement).await.unwrap();
    assert_eq!(
        engine.backfill(&loser).await.unwrap_err().code(),
        "stale_group"
    );
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), winner);
    replacement.operation = id("same-request-new-id");
    assert_eq!(
        engine
            .plan_backfill(&replacement)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("stale_group")
    );
}

#[tokio::test]
async fn refused_full_build_leaves_groups_usable_and_invalidation_refuses_stale_reuse() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let first = engine
        .backfill(&spec(&base, "first", "a", "text_length", "length(text)"))
        .await
        .unwrap();
    let before = engine.inspect(&base.dataset.dataset).await.unwrap()["authority"].clone();
    let replacement = input(
        &root.path().join("replacement.parquet"),
        &[(Some(0), Some("new".into()))],
    );
    let build = RunSpec {
        base: None,
        relationship: None,
        dataset: base.dataset.dataset.clone(),
        operation: id("unsupported-build"),
        input: replacement.path,
        expected_digest: replacement.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    };
    assert_eq!(
        engine.run(&build).await.unwrap_err().code(),
        "dependent_groups"
    );
    assert_eq!(
        engine.inspect(&base.dataset.dataset).await.unwrap()["authority"],
        before
    );
    let second = engine
        .backfill(&spec(&first, "second", "b", "next_id", "id + 1"))
        .await
        .unwrap();
    engine
        .invalidate(&base.dataset.dataset, "dependency omission", None)
        .await
        .unwrap();
    let third = spec(&second, "third", "c", "another_id", "id + 2");
    assert_eq!(
        engine.backfill(&third).await.unwrap_err().code(),
        "stale_eligibility"
    );
    assert_eq!(
        engine
            .plan_backfill(&third)
            .await
            .unwrap()
            .promotion_refusal
            .as_deref(),
        Some("stale_eligibility")
    );
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), second);
    assert!(
        engine
            .release(&base.dataset.dataset, &third.operation)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn competing_independent_backfills_both_survive_selection() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let first = spec(&base, "first", "a", "text_length", "length(text)");
    let second = spec(&base, "second", "b", "next_id", "id + 1");
    let (left, right) = tokio::join!(engine.backfill(&first), engine.backfill(&second));
    left.unwrap();
    right.unwrap();
    let latest = engine.latest(&base.dataset.dataset).await.unwrap();
    assert_eq!(latest.dataset.computed.len(), 2);
    verify_release(&latest, limits()).await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn missing_cached_objects_cannot_be_selected_after_readback() {
    for remove_input in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        let request = spec(&base, "cached", "length", "text_length", "length(text)");
        assert_eq!(
            engine
                .backfill_until(&request, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
        let records = engine.inspect(&request.dataset).await.unwrap();
        let candidate: ReleasePin = serde_json::from_value(
            records
                .iter()
                .find(|(key, _)| key.starts_with("backfill-readback/cached/"))
                .unwrap()
                .1
                .clone(),
        )
        .unwrap();
        let path = if remove_input {
            &candidate.dataset.inputs.values().next().unwrap().path
        } else {
            &candidate.dataset.groups["length"]
                .objects
                .iter()
                .find(|object| object.path.ends_with(".parquet"))
                .unwrap()
                .path
        };
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            engine.backfill(&request).await.unwrap_err().code(),
            "unavailable_phase"
        );
        assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        assert!(
            engine
                .release(&request.dataset, &request.operation)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn joined_output_respects_a_limit_that_each_individual_input_fits() {
    let root = TempDir::new().unwrap();
    let input = input(
        &root.path().join("input.parquet"),
        &[(Some(1), Some("x".repeat(400)))],
    );
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let base = engine
        .run(&RunSpec {
            base: None,
            relationship: None,
            dataset: id("wide"),
            operation: id("initial"),
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let a = engine
        .backfill(&spec(&base, "a", "a", "first_text", "repeat(text, 2)"))
        .await
        .unwrap();
    let b = engine
        .backfill(&spec(&a, "b", "b", "second_text", "repeat(text, 2)"))
        .await
        .unwrap();
    let mut limit = 0;
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    for pin in b.dataset.groups.values() {
        let native = storage::open_pin(pin, &budget).await.unwrap();
        // Measure the maintained reader's transient file column as well as values.
        let names = native
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .chain(std::iter::once("_file".to_owned()))
            .collect::<Vec<_>>();
        let stream = native
            .scan()
            .select(names)
            .with_concurrency_limit(1)
            .with_batch_size(Some(budget.limits.batch_rows))
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        futures::pin_mut!(stream);
        while let Some(batch) = stream.try_next().await.unwrap() {
            limit = limit.max(batch.get_array_memory_size());
        }
    }
    // Include the source locator allowance too, so only the final joined batch fails.
    let native = storage::open_pin(&b.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let mut source = storage::read_occurrences(&native, &budget)
        .await
        .unwrap()
        .boxed();
    while let Some(batch) = source.try_next().await.unwrap() {
        limit = limit.max(batch.get_array_memory_size());
    }
    drop(engine);
    // Exercise the shared payload reader independently of lifecycle metadata
    // batches, which need not fit this deliberately tiny joined-output limit.
    let budget = docspec::budget::Budget::new(Limits {
        max_batch_bytes: limit,
        ..limits()
    })
    .unwrap();
    let mut combined = docspec::engine::read_combined(&b, &budget).await.unwrap();
    let error = docspec::Error::from(combined.try_next().await.unwrap_err());
    assert_eq!(error.code(), "oversized_batch");
    assert!(error.to_string().contains("combined result"));
}

#[tokio::test]
async fn native_malformed_group_coverage_and_field_mapping_refuse() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let valid = engine
        .backfill(&spec(
            &base,
            "valid",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let native = storage::open_pin(&valid.dataset.groups["length"], &budget)
        .await
        .unwrap();

    let stream = storage::read_batches(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        batches.push(batch);
    }
    let schema = batches[0].schema();
    let batch = arrow::compute::concat_batches(&schema, &batches).unwrap();
    let paths = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let positions = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for kind in [
        "duplicate",
        "unknown",
        "range",
        "null",
        "mapping",
        "values",
        "binding",
        "origins",
    ] {
        let mut forged = valid.clone();
        if kind != "origins" {
            // Keep the existing deep reconstruction cases reachable for older
            // releases. The separate origins case checks the new association.
            forged.dataset.computed.get_mut("length").unwrap().origins = None;
        }
        if kind == "binding" {
            forged
                .dataset
                .computed
                .get_mut("length")
                .unwrap()
                .input_fields
                .insert("text".into(), 1);
        } else if kind == "mapping" {
            forged
                .dataset
                .computed
                .get_mut("length")
                .unwrap()
                .native_field_ids
                .insert(10, 99);
        } else {
            let mut paths: Vec<_> = paths.iter().map(|value| value.map(str::to_owned)).collect();
            let mut positions: Vec<_> = positions.iter().collect();
            match kind {
                "duplicate" => {
                    paths[0] = paths[1].clone();
                    positions[0] = positions[1];
                }
                "unknown" => paths[0] = Some("unknown.parquet".into()),
                "range" => positions[0] = Some(i64::MAX),
                "null" => positions[0] = None,
                "values" | "origins" => {}
                _ => unreachable!(),
            }
            let mut fields = schema.fields().to_vec();
            if kind == "null" {
                fields[1] = Arc::new(fields[1].as_ref().clone().with_nullable(true));
            }
            let malformed_schema = Arc::new(Schema::new(fields));
            let malformed = RecordBatch::try_new(
                malformed_schema.clone(),
                vec![
                    Arc::new(StringArray::from(paths)),
                    Arc::new(Int64Array::from(positions)),
                    if kind == "values" {
                        arrow::compute::cast(
                            &Int64Array::from(vec![Some(999); batch.num_rows()]),
                            batch.column(2).data_type(),
                        )
                        .unwrap()
                    } else {
                        batch.column(2).clone()
                    },
                ],
            )
            .unwrap();
            let path = root.path().join(format!("{kind}.parquet"));
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), malformed_schema, None).unwrap();
            writer.write(&malformed).unwrap();
            writer.close().unwrap();
            let alternate = engine
                .run(&RunSpec {
                    base: None,
                    relationship: None,
                    dataset: id(&format!("malformed_{kind}")),
                    operation: id("build"),
                    input: path.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&path).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                })
                .await
                .unwrap();
            if kind != "null" {
                let alternate_native =
                    storage::open_pin(&alternate.dataset.groups["source"], &budget)
                        .await
                        .unwrap();
                assert_eq!(
                    alternate_native.metadata().current_schema(),
                    native.metadata().current_schema(),
                    "{kind} must reach occurrence checks with the same schema"
                );
            }
            forged
                .dataset
                .groups
                .insert("length".into(), alternate.dataset.groups["source"].clone());
        }
        let path = root.path().join(format!("forged-{kind}.json"));
        std::fs::write(&path, serde_json::to_vec_pretty(&forged.dataset).unwrap()).unwrap();
        let (digest, bytes) = storage::digest(&path).unwrap();
        forged.manifest = docspec::model::ObjectRef {
            owner: forged.dataset.dataset.clone(),
            path: path.to_string_lossy().into_owned(),
            digest,
            bytes,
        };
        assert_eq!(
            verify_release(&forged, limits()).await.unwrap_err().code(),
            if kind == "values" {
                "invalid_group_values"
            } else {
                "invalid_group"
            },
            "{kind}"
        );
    }
}

#[tokio::test]
async fn identical_concurrent_attempts_reconcile_one_recorded_result() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "same", "length", "text_length", "length(text)");
    let (first, second) = tokio::join!(engine.backfill(&request), engine.backfill(&request));
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(
        engine.latest(&request.dataset).await.unwrap().id,
        request.operation
    );
}

#[tokio::test]
async fn failed_files_remain_distinct_from_successful_nulls_and_completed_files_survive() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let mut paths: Vec<_> = base.dataset.groups["source"]
        .objects
        .iter()
        .filter(|object| object.path.ends_with(".parquet"))
        .map(|object| &object.path)
        .collect();
    paths.sort();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(paths.last().unwrap()).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let target = reader
        .flat_map(|batch| {
            batch
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .find(|id| *id > 10)
        .unwrap();
    let expression =
        format!("CASE WHEN id = {target} THEN CAST(text AS BIGINT) ELSE CAST(NULL AS BIGINT) END");
    let request = spec(&base, "failure", "parse", "parsed", &expression);
    assert!(engine.backfill(&request).await.is_err());
    let plan = engine.plan_backfill(&request).await.unwrap();
    assert!(plan.files_completed.is_some_and(|files| files > 0));
    assert!(plan.files_pending.is_some_and(|files| files > 0));
    assert_eq!(plan.failures.len(), 1);
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    let correction = input(
        &root.path().join("correction.parquet"),
        &[(Some(target), Some("123".into()))],
    );
    let corrected = engine
        .update(&UpdateSpec {
            dataset: request.dataset.clone(),
            operation: id("corrected"),
            definition: "v1".into(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: correction,
            deletions: None,
        })
        .await
        .unwrap();
    let retry = spec(&corrected, "recovered", "parse", "parsed", &expression);
    let result = engine.backfill(&retry).await.unwrap();
    let values = combined(&engine, &result).await;
    assert_eq!(
        values
            .iter()
            .filter(|(_, value)| *value == Some(123))
            .count(),
        1
    );
    assert_eq!(
        values.iter().filter(|(_, value)| value.is_none()).count(),
        values.len() - 1
    );
}

#[tokio::test]
async fn http_backfill_routes_use_the_same_progress_and_conflict_rules() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let mut request = spec(&base, "http", "length", "text_length", "length(text)");
    let router = docspec::http::router(engine.clone());
    for path in ["/backfills/plan", "/backfills/run"] {
        let response = router
            .clone()
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        if path.ends_with("plan") {
            assert_eq!(value["promotion_refusal"], serde_json::Value::Null);
        } else {
            assert_eq!(value["id"], "http");
        }
    }
    request.operation = id("stale-http");
    let response = router
        .oneshot(
            Request::post("/backfills/run")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(value["code"], "stale_group");
}

#[tokio::test]
async fn nonlocal_and_volatile_expressions_refuse_before_selection() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    for (index, expression) in [
        "sum(id)",
        "random()",
        "row_number() OVER ()",
        "(SELECT max(id) FROM input)",
    ]
    .iter()
    .enumerate()
    {
        let request = spec(
            &base,
            &format!("bad{index}"),
            "invalid",
            "result",
            expression,
        );
        assert!(engine.plan_backfill(&request).await.is_err());
        assert!(engine.backfill(&request).await.is_err());
        assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
    }
}
