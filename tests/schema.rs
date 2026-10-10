//! Public metadata edits preserve values and fence writer-definition cutover.

use super::enriched;
use super::scalar;

#[cfg(feature = "test-util")]
use super::support;
use arrow::array::{Int64Array, StringArray};
use docspec::{
    engine::verify_release,
    model::{OperationRequest, PhaseResult, ReleasePin, RunSpec, SchemaField, SchemaSpec},
    storage,
};
use futures::{StreamExt, TryStreamExt};
use scalar::{fixture, id, limits};
use std::{collections::BTreeMap, path::Path};

fn request(base: &ReleasePin, operation: &str) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        next_definition: format!("{}-next", base.dataset.definition),
        fields: vec![
            SchemaField {
                id: 2,
                name: "body \"text\"".into(),
            },
            SchemaField {
                id: 1,
                name: "comment_id".into(),
            },
        ],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    }
}
fn payloads(release: &ReleasePin) -> BTreeMap<String, Vec<docspec::model::ObjectRef>> {
    release
        .dataset
        .groups
        .iter()
        .map(|(name, table)| {
            (
                name.clone(),
                table
                    .objects
                    .iter()
                    .filter(|o| o.path.ends_with(".parquet"))
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}
async fn source_rows(release: &ReleasePin) -> Vec<scalar::Row> {
    let budget = docspec::budget::Budget::new(limits()).unwrap();
    let table = storage::open_pin(&release.dataset.groups["source"])
        .await
        .unwrap();
    let mut stream = storage::read_batches(&table, &budget)
        .await
        .unwrap()
        .boxed();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("comment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column_by_name("body \"text\"")
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
    rows
}

#[tokio::test]
async fn rename_reorder_keeps_source_and_computed_files_and_old_reads() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, mut rows) = enriched::enriched(root.path()).await;
    let objects = payloads(&base);
    let times: Vec<_> = objects
        .values()
        .flatten()
        .map(|o| {
            (
                o.path.clone(),
                std::fs::metadata(&o.path).unwrap().modified().unwrap(),
            )
        })
        .collect();
    let request = request(&base, "renamed");
    let plan = engine.plan_schema(&request).await.unwrap();
    assert!(!plan.completed);
    let release = engine.evolve_schema(&request).await.unwrap();
    assert_eq!(payloads(&release), objects);
    for (path, time) in times {
        assert_eq!(std::fs::metadata(path).unwrap().modified().unwrap(), time);
    }
    assert_eq!(release.dataset.definition, request.next_definition);
    assert_eq!(release.dataset.inputs, base.dataset.inputs);
    for (name, group) in &base.dataset.computed {
        let new = &release.dataset.computed[name];
        assert_eq!(new.input_fields, group.input_fields);
        assert_eq!(new.definition, group.definition);
    }
    rows.sort();
    assert_eq!(source_rows(&release).await, rows);
    let mut combined = engine.read_combined(&release).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = combined.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("comment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column_by_name("body \"text\"")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
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
    let expected = rows
        .iter()
        .map(|row| {
            (
                row.clone(),
                row.1.as_ref().map(|s| s.chars().count() as i64),
                row.0.map(|v| v + 1),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    enriched::check_values(&engine, &base, &rows).await;
    verify_release(&release, limits()).await.unwrap();
    assert_eq!(engine.evolve_schema(&request).await.unwrap(), release);
    assert!(engine.plan_schema(&request).await.unwrap().completed);
    let mut conflict = request.clone();
    conflict.fields.reverse();
    assert_eq!(
        engine.evolve_schema(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
    let proof = engine
        .prove_succession(&base.version_ref(), &release.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 1);
    // A rehashed request cannot claim that the same values came from another ID.
    let mut forged = release.clone();
    if let OperationRequest::Schema(spec) = &mut forged.dataset.request {
        spec.fields.swap(0, 1);
    }
    assert_eq!(
        verify_release(
            &enriched::forged(root.path(), "wrong_schema", forged),
            limits()
        )
        .await
        .unwrap_err()
        .code(),
        "invalid_schema"
    );
}

#[tokio::test]
async fn invalid_edits_refuse_before_admission_and_empty_sources_evolve() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = enriched::enriched(root.path()).await;
    let original = engine.inspect(&base.dataset.dataset).await.unwrap();
    for case in [
        "consumed",
        "collision",
        "reserved",
        "duplicate",
        "unknown",
        "definition",
    ] {
        let mut edit = request(&base, case);
        match case {
            "consumed" => {
                edit.fields.remove(0);
            }
            "collision" => edit.fields[0].name = "TEXT_LENGTH".into(),
            "reserved" => edit.fields[0].name = storage::OCCURRENCE_ROW.to_uppercase(),
            "duplicate" => edit.fields[1].id = edit.fields[0].id,
            "unknown" => edit.fields[0].id = 90,
            "definition" => edit.next_definition = edit.definition.clone(),
            _ => unreachable!(),
        }
        assert!(engine.plan_schema(&edit).await.is_err(), "{case}");
        assert!(engine.evolve_schema(&edit).await.is_err(), "{case}");
        assert_eq!(
            engine.inspect(&base.dataset.dataset).await.unwrap(),
            original,
            "{case}"
        );
    }
    let empty = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(empty.path(), true).await;
    let release = engine
        .evolve_schema(&request(&base, "empty"))
        .await
        .unwrap();
    assert!(source_rows(&release).await.is_empty());
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn hidden_fields_stay_historical_and_new_writes_use_the_selected_schema() {
    use arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use docspec::model::{InputSpec, UpdateSpec};
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let (engine, base, rows) = fixture(root.path(), false).await;
    let mut edit = request(&base, "hidden");
    edit.fields.remove(0);
    let hidden = engine.evolve_schema(&edit).await.unwrap();
    assert_eq!(payloads(&hidden), payloads(&base));
    let native = storage::open_pin(&hidden.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(native.metadata().last_column_id(), 2);
    assert_eq!(
        native
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .len(),
        1
    );
    assert_eq!(
        native
            .metadata()
            .current_schema()
            .field_by_name("comment_id")
            .unwrap()
            .id,
        1
    );
    verify_release(&base, limits()).await.unwrap();
    let mut fresh = scalar::spec(&hidden, "fresh", "fresh", "triple", "comment_id * 3");
    fresh.definition = hidden.dataset.definition.clone();
    let enriched = engine.backfill(&fresh).await.unwrap();
    assert_eq!(
        enriched.dataset.computed["fresh"].input_fields,
        BTreeMap::from([("comment_id".into(), 1)])
    );
    let path = root.path().join("replacement.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "comment_id",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(1), None])) as ArrayRef],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let update = UpdateSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("updated"),
        base: enriched.id.clone(),
        definition: enriched.dataset.definition.clone(),
        keys: vec!["comment_id".into()],
        replacements: InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(&path).unwrap().0,
        },
        deletions: None,
    };
    let updated = engine.update(&update).await.unwrap();
    let mut stream = engine.read_combined(&updated).await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("comment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let triples = batch
            .column_by_name("triple")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(ids.iter().zip(triples.iter()));
    }
    let mut expected = rows
        .iter()
        .filter(|row| row.0 != Some(1) && row.0.is_some())
        .map(|row| row.0)
        .chain([Some(1), Some(1), None])
        .map(|id| (id, id.map(|n| n * 3)))
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    let native = storage::open_pin(&updated.dataset.groups["source"])
        .await
        .unwrap();
    // Origin fields extend the high-water mark without reusing hidden field 2.
    assert_eq!(native.metadata().last_column_id(), 4);
    assert_eq!(
        native
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.as_str(), field.id))
            .collect::<Vec<_>>(),
        vec![
            ("comment_id", 1),
            ("__docspec_origin_file", 3),
            ("__docspec_origin_row", 4),
        ]
    );
    verify_release(&updated, limits()).await.unwrap();
}

#[tokio::test]
async fn rewritten_native_state_cannot_masquerade_as_a_schema_edit() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let release = engine
        .evolve_schema(&request(&base, "changed"))
        .await
        .unwrap();
    for case in ["properties", "high_water", "references", "boundary"] {
        let mut forged = release.clone();
        if case == "boundary" {
            forged.dataset.boundary += 1;
            assert_eq!(
                verify_release(&enriched::forged(root.path(), case, forged), limits())
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_schema"
            );
            continue;
        }
        let pin = forged.dataset.groups.get_mut("source").unwrap();
        let old = pin.metadata.clone();
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&old.path).unwrap()).unwrap();
        match case {
            "properties" => metadata["properties"]["unapproved"] = "changed".into(),
            "high_water" => metadata["last-column-id"] = 99.into(),
            "references" => {
                metadata["refs"]["unapproved"] =
                    serde_json::json!({"type":"tag", "snapshot-id":pin.snapshot.unwrap()})
            }
            _ => unreachable!(),
        }
        let path = root.path().join(format!("{case}.metadata.json"));
        std::fs::write(&path, serde_json::to_vec(&metadata).unwrap()).unwrap();
        let (digest, bytes) = storage::digest(&path).unwrap();
        pin.metadata = docspec::model::ObjectRef {
            owner: old.owner.clone(),
            path: path.to_string_lossy().into_owned(),
            digest,
            bytes,
        };
        *pin.objects
            .iter_mut()
            .find(|object| **object == old)
            .unwrap() = pin.metadata.clone();
        assert_eq!(
            verify_release(&enriched::forged(root.path(), case, forged), limits())
                .await
                .unwrap_err()
                .code(),
            "invalid_schema",
            "{case}"
        );
    }
    verify_release(&release, limits()).await.unwrap();
}

#[tokio::test]
async fn http_and_cli_schema_routes_use_the_same_retained_request() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let app = docspec::http::router(engine.clone());
    let edit = request(&base, "through_http");
    let mut selected = None;
    for path in ["/schemas/plan", "/schemas/run", "/schemas/run"] {
        let response = app
            .clone()
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&edit).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        if path.ends_with("run") {
            let release: ReleasePin =
                serde_json::from_slice(&to_bytes(response.into_body(), 8 << 20).await.unwrap())
                    .unwrap();
            if let Some(previous) = &selected {
                assert_eq!(&release, previous);
            }
            selected = Some(release);
        }
    }
    let mut stale = edit.clone();
    stale.operation = id("stale");
    let response = app
        .clone()
        .oneshot(
            Request::post("/schemas/run")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&stale).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    drop((app, engine));
    let spec = root.path().join("schema.json");
    std::fs::write(&spec, serde_json::to_vec(&edit).unwrap()).unwrap();
    for command in ["plan-schema", "schema"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg(command)
            .arg(&spec)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if command == "plan-schema" {
            assert_eq!(value["completed"], true);
        } else {
            assert_eq!(
                serde_json::from_value::<ReleasePin>(value).unwrap(),
                selected.clone().unwrap()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "test-util")]
async fn killed_schema_workers_resume_only_missing_phases() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "readback",
        "selection",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, mut rows) = fixture(root.path(), false).await;
        let edit = request(&base, "killed");
        drop(engine);
        support::kill_after(root.path(), &edit, "schema", phase);
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        let (release, work) = support::resume(root.path(), "schema");
        let expected = match phase {
            "admission" => vec!["processing", "validation", "readback"],
            "processing" => vec!["validation", "readback"],
            "validation" => vec!["readback"],
            "readback" | "selection" => vec![],
            _ => unreachable!(),
        };
        assert_eq!(work, expected, "{phase}");
        rows.sort();
        assert_eq!(source_rows(&release).await, rows);
        verify_release(&release, limits()).await.unwrap();
    }
}

#[cfg(feature = "test-util")]
fn build(root: &Path, base: &ReleasePin, operation: &str, with_base: bool) -> RunSpec {
    let source = scalar::input(
        &root.join(format!("{operation}.parquet")),
        &[(Some(77), Some("accepted".into()))],
    );
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: with_base.then(|| base.id.clone()),
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
    }
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn cached_related_result_checks_schema_input_history() {
    use docspec::model::{
        AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, RefreshSpec,
        RefreshTransform,
    };
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let renamed = engine
        .evolve_schema(&request(&base, "renamed"))
        .await
        .unwrap();
    let refresh = RefreshSpec {
        dataset: id("summary"),
        operation: id("summarize"),
        base: None,
        definition: "summary-v1".into(),
        policy: Default::default(),
        mode: Default::default(),
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: renamed.version_ref(),
            grouping: AggregationDefinition {
                keys: vec![],
                aggregates: vec![AggregateField {
                    name: "rows".into(),
                    function: AggregateFunction::CountRows,
                }],
            },
        }),
    };
    assert_eq!(
        engine
            .refresh_until(&refresh, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let path = &base.dataset.groups["source"].metadata.path;
    let bytes = std::fs::read(path).unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(engine.refresh(&refresh).await.is_err());
    assert_eq!(
        engine.latest(&refresh.dataset).await.unwrap_err().code(),
        "no_release"
    );
    std::fs::write(path, bytes).unwrap();
    let result = engine.refresh(&refresh).await.unwrap();
    verify_release(&result, limits()).await.unwrap();
    // Related sources need explicit contributor/schema mapping before evolving.
    let mut unsupported = request(&result, "related_schema");
    unsupported.fields = vec![SchemaField {
        id: 1,
        name: "renamed_count".into(),
    }];
    assert_eq!(
        engine.plan_schema(&unsupported).await.unwrap_err().code(),
        "related_source"
    );
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn schema_cutover_fences_old_workers_and_later_admissions() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let old = build(root.path(), &base, "old_worker", false);
    assert_eq!(
        engine
            .run_until(&old, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let pending = engine.inspect(&base.dataset.dataset).await.unwrap();
    let edit = request(&base, "too_early");
    assert_eq!(
        engine.plan_schema(&edit).await.unwrap_err().code(),
        "pending_source"
    );
    assert_eq!(
        engine.evolve_schema(&edit).await.unwrap_err().code(),
        "pending_source"
    );
    assert_eq!(
        engine.inspect(&base.dataset.dataset).await.unwrap(),
        pending
    );
    let complete = engine.run(&old).await.unwrap();
    let pending_group = scalar::spec(
        &complete,
        "old_backfill",
        "pending",
        "length",
        "length(text)",
    );
    assert_eq!(
        engine
            .backfill_until(&pending_group, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let release = engine
        .evolve_schema(&request(&complete, "cutover"))
        .await
        .unwrap();
    assert_eq!(
        engine.backfill(&pending_group).await.unwrap_err().code(),
        "stale_definition"
    );
    assert_eq!(engine.run(&old).await.unwrap(), complete);
    assert_eq!(
        source_rows(&release).await,
        vec![(Some(77), Some("accepted".into()))]
    );
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), release);
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let edit = request(&base, "pending_schema");
    assert_eq!(
        engine
            .schema_until(&edit, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let phases = engine.inspect(&base.dataset.dataset).await.unwrap();
    let later = build(root.path(), &base, "later_input", true);
    assert_eq!(
        engine
            .run_until(&later, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        engine.evolve_schema(&edit).await.unwrap_err().code(),
        "stale_source_boundary"
    );
    let retained = engine.inspect(&base.dataset.dataset).await.unwrap();
    for (key, value) in &phases {
        if key.starts_with("phase/") {
            assert_eq!(retained[key], *value);
        }
    }
    assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
    // The accepted later input remains resumable after the stale schema refusal.
    let later = engine.run(&later).await.unwrap();
    assert_eq!(later.dataset.groups["source"].rows, 1);
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn schema_phases_resume_and_cached_readback_requires_base_schema() {
    for phase in [
        "admission",
        "processing",
        "validation",
        "groups",
        "readback",
        "selection",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (engine, base, mut rows) = fixture(root.path(), false).await;
        let edit = request(&base, "resumable");
        assert_eq!(
            engine.schema_until(&edit, phase).await.unwrap_err().code(),
            "injected_failure"
        );
        let retained = engine.inspect(&base.dataset.dataset).await.unwrap();
        let completed: BTreeMap<_, PhaseResult> = retained
            .iter()
            .filter(|(key, _)| key.starts_with("phase/"))
            .map(|(key, value)| (key.clone(), serde_json::from_value(value.clone()).unwrap()))
            .collect();
        std::fs::remove_file(root.path().join("input.parquet")).unwrap();
        if phase == "readback" {
            let path = &base.dataset.groups["source"].metadata.path;
            let bytes = std::fs::read(path).unwrap();
            std::fs::remove_file(path).unwrap();
            assert!(engine.evolve_schema(&edit).await.is_err());
            assert_eq!(engine.latest(&base.dataset.dataset).await.unwrap(), base);
            std::fs::write(path, bytes).unwrap();
        }
        let release = engine.evolve_schema(&edit).await.unwrap();
        let after = engine.inspect(&base.dataset.dataset).await.unwrap();
        for (key, value) in completed {
            assert_eq!(
                serde_json::from_value::<PhaseResult>(after[&key].clone()).unwrap(),
                value
            );
        }
        rows.sort();
        assert_eq!(source_rows(&release).await, rows);
        verify_release(&release, limits()).await.unwrap();
    }
}
