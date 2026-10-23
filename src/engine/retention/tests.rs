// Rust guideline compliant 2026-02-21

use super::*;

async fn workspace() -> (tempfile::TempDir, Engine) {
    let root = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        compute_threads: 1,
        memory_bytes: 32 << 20,
        scratch_bytes: 1 << 20,
        batch_rows: 32,
        max_batch_bytes: 64 << 10,
        max_metadata_bytes: 256 << 10,
    };
    let engine = Engine::open(root.path(), limits).await.unwrap();
    (root, engine)
}

fn admission(engine: &Engine, dataset: &Id, name: &str, boundary: u64) -> (RunSpec, Operation) {
    let request = RunSpec {
        dataset: dataset.clone(),
        operation: Id::new(name).unwrap(),
        base: None,
        relationship: None,
        input: engine
            .inner
            .root
            .join("missing-input.parquet")
            .display()
            .to_string(),
        expected_digest: "0".repeat(64),
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    assert!(!Path::new(&request.input).exists());
    let operation = Operation {
        request_digest: fingerprint(&OperationRequest::Build(request.clone())).unwrap(),
        eligibility: 0,
        epoch: 0,
        boundary,
        admission_fence: None,
        result_format: FORMAT,
        definition: request.definition.clone(),
        base: None,
        base_manifest: None,
        schema_key: None,
        require_current_boundary: false,
    };
    (request, operation)
}

fn abandonment(request: &RunSpec, operation: &Operation, revision: i64) -> Abandonment {
    Abandonment {
        dataset: request.dataset.clone(),
        operation: request.operation.clone(),
        request_digest: operation.request_digest.clone(),
        operation_revision: revision,
        reason: "superseded input".into(),
    }
}

fn admission_records(request: &RunSpec, operation: &Operation) -> Vec<(String, serde_json::Value)> {
    vec![
        record(format!("operation/{}", request.operation), operation).unwrap(),
        record(
            format!("request/{}", request.operation),
            OperationRequest::Build(request.clone()),
        )
        .unwrap(),
    ]
}

#[tokio::test]
async fn abandoned_gap_accepts_complete_interval_without_opening_inputs() {
    let (_root, engine) = workspace().await;
    let dataset = Id::new("complete").unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let mut view = lifecycle.open(&dataset).await.unwrap();
    for (name, boundary) in [("first", 2), ("second", 3)] {
        let (request, operation) = admission(&engine, &dataset, name, boundary);
        view = lifecycle
            .commit(&view, admission_records(&request, &operation))
            .await
            .unwrap();
        let abandoned = abandonment(&request, &operation, view.revision);
        view = lifecycle
            .commit(
                &view,
                vec![record(format!("abandoned/{name}"), abandoned).unwrap()],
            )
            .await
            .unwrap();
    }
    engine
        .check_abandoned_gap(&view, &dataset, 1, 3)
        .await
        .unwrap();
    engine
        .check_abandoned_gap(&view, &dataset, 3, 3)
        .await
        .unwrap();
}

#[tokio::test]
async fn abandoned_gap_counts_legacy_schema_but_not_composition_operations() {
    let (_root, engine) = workspace().await;
    let dataset = Id::new("mixed").unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let mut view = lifecycle.open(&dataset).await.unwrap();
    let mut legacy_abandoned = None;
    for (name, format, is_schema) in [
        ("modern_schema", FORMAT, true),
        ("retained_schema", 11, true),
        ("old_backfill", PREVIOUS_FORMAT, false),
        ("new_backfill", FORMAT, false),
        ("legacy_schema", PREVIOUS_FORMAT, true),
    ] {
        let (build, mut operation) = admission(&engine, &dataset, name, 1);
        let request = if is_schema {
            OperationRequest::Schema(SchemaSpec {
                dataset: dataset.clone(),
                operation: build.operation.clone(),
                base: Id::new("base").unwrap(),
                definition: "v1".into(),
                next_definition: "v2".into(),
                fields: vec![SchemaField {
                    id: 1,
                    name: "renamed".into(),
                }],
                additions: vec![],
                write_defaults: BTreeMap::new(),
            })
        } else {
            OperationRequest::Backfill(BackfillSpec {
                dataset: dataset.clone(),
                operation: build.operation.clone(),
                base: Id::new("base").unwrap(),
                definition: "v1".into(),
                group: Id::new("derived").unwrap(),
                group_definition: "g1".into(),
                fields: vec![ComputedField {
                    id: 1,
                    name: "value".into(),
                    expression: "1".into(),
                }],
            })
        };
        operation.result_format = format;
        operation.request_digest = fingerprint(&request).unwrap();
        view = lifecycle
            .commit(
                &view,
                vec![
                    record(format!("operation/{name}"), &operation).unwrap(),
                    record(format!("request/{name}"), request).unwrap(),
                ],
            )
            .await
            .unwrap();
        if name == "legacy_schema" {
            legacy_abandoned = Some(abandonment(&build, &operation, view.revision));
        }
    }
    let error = engine
        .check_abandoned_gap(&view, &dataset, 0, 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "pending_source");
    view = lifecycle
        .commit(
            &view,
            vec![record("abandoned/legacy_schema", legacy_abandoned.unwrap()).unwrap()],
        )
        .await
        .unwrap();
    // The other operations share this boundary but do not resolve or duplicate it.
    engine
        .check_abandoned_gap(&view, &dataset, 0, 1)
        .await
        .unwrap();
}

#[tokio::test]
async fn abandoned_gap_refuses_incomplete_or_conflicting_identities() {
    let (_root, engine) = workspace().await;
    let lifecycle = &engine.inner.lifecycle;
    for case in [
        "missing_boundary",
        "duplicate_boundary",
        "missing_request",
        "missing_abandonment",
        "request_dataset",
        "request_operation",
        "request_digest",
        "unknown_format",
        "abandonment_dataset",
        "abandonment_operation",
        "abandonment_digest",
        "admission_revision",
        "empty_reason",
        "selected_result",
        "future_boundary",
    ] {
        let dataset = Id::new(case).unwrap();
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let (mut request, mut operation) = admission(&engine, &dataset, "first", 1);
        match case {
            "request_dataset" => request.dataset = Id::new("other").unwrap(),
            "request_operation" => request.operation = Id::new("other").unwrap(),
            "unknown_format" => operation.result_format = FORMAT + 1,
            "future_boundary" => operation.boundary = 2,
            _ => {}
        }
        operation.request_digest = fingerprint(&OperationRequest::Build(request.clone())).unwrap();
        if case == "request_digest" {
            operation.request_digest = "wrong-digest".into();
        }
        // Keep lifecycle keys fixed even when the embedded request identity is forged.
        let mut admitted = vec![record("operation/first", &operation).unwrap()];
        if case != "missing_request" {
            admitted
                .push(record("request/first", OperationRequest::Build(request.clone())).unwrap());
        }
        let duplicate = admission(&engine, &dataset, "second", 1);
        if case == "duplicate_boundary" {
            admitted.extend(admission_records(&duplicate.0, &duplicate.1));
        }
        view = lifecycle.commit(&view, admitted).await.unwrap();
        let mut abandoned = abandonment(&request, &operation, view.revision);
        // The tombstone must identify the canonical operation, not the forged request.
        abandoned.dataset = dataset.clone();
        abandoned.operation = Id::new("first").unwrap();
        match case {
            "abandonment_dataset" => abandoned.dataset = Id::new("other").unwrap(),
            "abandonment_operation" => abandoned.operation = Id::new("other").unwrap(),
            "abandonment_digest" => abandoned.request_digest = "wrong-digest".into(),
            "admission_revision" => abandoned.operation_revision += 1,
            "empty_reason" => abandoned.reason = " \t".into(),
            _ => {}
        }
        if case != "missing_abandonment" {
            let mut retired = vec![record("abandoned/first", abandoned).unwrap()];
            if case == "duplicate_boundary" {
                retired.push(
                    record(
                        "abandoned/second",
                        abandonment(&duplicate.0, &duplicate.1, view.revision),
                    )
                    .unwrap(),
                );
            }
            if case == "selected_result" {
                retired.push(record("result/first", serde_json::json!({})).unwrap());
            }
            view = lifecycle.commit(&view, retired).await.unwrap();
        }
        let fence = if case == "missing_boundary" { 2 } else { 1 };
        let error = engine
            .check_abandoned_gap(&view, &dataset, 0, fence)
            .await
            .unwrap_err();
        let expected = if case == "missing_abandonment" {
            "pending_source"
        } else {
            "invalid_admission_history"
        };
        assert_eq!(error.code(), expected, "{case}: {error}");
    }
}

#[tokio::test]
async fn abandoned_gap_requires_original_admission_and_later_visible_abandonment() {
    let (_root, engine) = workspace().await;
    let lifecycle = &engine.inner.lifecycle;
    for case in [
        "same_commit",
        "before_admission",
        "rewritten_request",
        "rewritten_admission",
        "operation_after_view",
        "abandonment_after_view",
    ] {
        let dataset = Id::new(case).unwrap();
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let (request, operation) = admission(&engine, &dataset, "first", 1);
        let mut admitted = admission_records(&request, &operation);
        match case {
            "same_commit" => {
                admitted
                    .push(record("abandoned/first", abandonment(&request, &operation, 1)).unwrap());
                view = lifecycle.commit(&view, admitted).await.unwrap();
            }
            "before_admission" => {
                view = lifecycle
                    .commit(
                        &view,
                        vec![
                            record("abandoned/first", abandonment(&request, &operation, 2))
                                .unwrap(),
                        ],
                    )
                    .await
                    .unwrap();
                view = lifecycle.commit(&view, admitted).await.unwrap();
            }
            _ => {
                view = lifecycle.commit(&view, admitted).await.unwrap();
                let abandoned = abandonment(&request, &operation, view.revision);
                view = lifecycle
                    .commit(&view, vec![record("abandoned/first", abandoned).unwrap()])
                    .await
                    .unwrap();
                match case {
                    "rewritten_request" => {
                        view = lifecycle
                            .commit(
                                &view,
                                vec![
                                    record(
                                        "request/first",
                                        OperationRequest::Build(request.clone()),
                                    )
                                    .unwrap(),
                                ],
                            )
                            .await
                            .unwrap();
                    }
                    "rewritten_admission" => {
                        view = lifecycle
                            .commit(&view, admission_records(&request, &operation))
                            .await
                            .unwrap();
                    }
                    "operation_after_view" => view.revision = 0,
                    "abandonment_after_view" => view.revision = 1,
                    _ => unreachable!(),
                }
            }
        }
        let error = engine
            .check_abandoned_gap(&view, &dataset, 0, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "invalid_admission_history", "{case}: {error}");
    }
}

#[tokio::test]
async fn abandoned_gap_refuses_huge_or_reversed_intervals_without_expanding_them() {
    let (_root, engine) = workspace().await;
    let dataset = Id::new("bounded").unwrap();
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    let finite_overflow = engine.inner.lifecycle.budget.limits.max_metadata_bytes as u64 / 64 + 1;
    for fence in [finite_overflow, u64::MAX] {
        let error = engine
            .check_abandoned_gap(&view, &dataset, 0, fence)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "metadata_capacity", "fence {fence}: {error}");
    }
    let error = engine
        .check_abandoned_gap(&view, &dataset, 2, 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "invalid_admission_history");
    engine
        .check_abandoned_gap(&view, &dataset, u64::MAX, u64::MAX)
        .await
        .unwrap();
}

fn write_parquet_input(path: &Path, ids: &[i64]) -> InputSpec {
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.iter().copied())),
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|id| format!("row-{id}")),
            )),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

async fn run_build_step(engine: &Engine, dataset: &Id, op: &str, input: &InputSpec) -> ReleasePin {
    engine
        .run(&RunSpec {
            dataset: dataset.clone(),
            operation: Id::new(op).unwrap(),
            base: None,
            relationship: None,
            input: input.path.clone(),
            expected_digest: input.digest.clone(),
            sql: "SELECT id, val FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap()
}

async fn run_update_steps(
    engine: &Engine,
    root: &Path,
    dataset: &Id,
    mut latest: ReleasePin,
    steps: std::ops::RangeInclusive<i64>,
) -> ReleasePin {
    for step in steps {
        let upd = write_parquet_input(&root.join(format!("upd-{step}.parquet")), &[step * 10]);
        latest = engine
            .update(&UpdateSpec {
                dataset: dataset.clone(),
                operation: Id::new(format!("op-{step}")).unwrap(),
                base: latest.id.clone(),
                definition: "v1".into(),
                keys: vec!["id".into()],
                replacements: upd,
                deletions: None,
                merge: None,
                relationship: None,
            })
            .await
            .unwrap();
    }
    latest
}

#[tokio::test]
async fn compact_lifecycle_preserves_releases_exports_grants_incomplete_work_and_verification() {
    let root = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 64 << 20,
        batch_rows: 128,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
    };
    let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
    let dataset = Id::new("events").unwrap();
    let init_input = write_parquet_input(&root.path().join("init.parquet"), &[1, 2, 3]);
    let r0 = run_build_step(&engine, &dataset, "op-0", &init_input).await;
    let latest = run_update_steps(&engine, root.path(), &dataset, r0.clone(), 1..=6).await;

    // Materialize an ExportPin against the dataset.
    let export_pin = engine
        .export(&crate::export::ExportSpec {
            dataset: dataset.clone(),
            operation: Id::new("exp-1").unwrap(),
            source: latest.version_ref(),
            channel: Id::new("feed").unwrap(),
            expected: None,
            policy: crate::export::ExportPolicy {
                group: "source".into(),
                rows: crate::export::ExportRows::All,
                columns: vec!["id".into(), "val".into()],
                ..crate::export::ExportPolicy::default()
            },
        })
        .await
        .unwrap();

    // Create a downstream dataset that consumes `events`, recording an `owner-protection/*` grant.
    let consumer = Id::new("downstream").unwrap();
    let _consumer_release = engine
        .refresh(&RefreshSpec {
            dataset: consumer.clone(),
            operation: Id::new("sub-0").unwrap(),
            base: None,
            definition: "v1".into(),
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: latest.version_ref(),
                grouping: AggregationDefinition {
                    keys: vec!["id".into()],
                    aggregates: vec![AggregateField {
                        name: "rows".into(),
                        function: AggregateFunction::CountRows,
                    }],
                    filter: None,
                    projections: BTreeMap::new(),
                },
                filter: None,
                projections: BTreeMap::new(),
            }),
            policy: RefreshPolicy::default(),
            mode: RefreshMode::default(),
            relationship: None,
        })
        .await
        .unwrap();

    // Admit an unfinished operation on `events` (interrupted before completion).
    let pending_view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    let (pending_req, pending_op) = admission(&engine, &dataset, "op-pending", 8);
    let _ = engine
        .inner
        .lifecycle
        .commit(&pending_view, admission_records(&pending_req, &pending_op))
        .await
        .unwrap();

    assert!(
        Path::new(&r0.manifest.path).is_file(),
        "initial release manifest should exist before compaction"
    );
    let stats = engine.compact_lifecycle(&dataset).await.unwrap();
    assert!(stats.revisions_before > 10);
    assert_eq!(stats.revisions_after, 1);
    assert!(stats.files_before > 10);
    assert_eq!(stats.files_after, 1);
    assert!(stats.rows_after < stats.rows_before);
    assert!(stats.metadata_bytes_after < stats.metadata_bytes_before);

    drop(engine);
    let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
    let cold_view = reopened.inner.lifecycle.open(&dataset).await.unwrap();
    assert_eq!(cold_view.revision, stats.current_revision);
    assert_eq!(cold_view.table.metadata().snapshots().count(), 1);
    assert_eq!(
        cold_view
            .table
            .metadata()
            .properties()
            .get("gc.enabled")
            .map(String::as_str),
        Some("false")
    );
    let lifecycle_loc = std::path::PathBuf::from(cold_view.table.metadata().location());
    let data_files = std::fs::read_dir(lifecycle_loc.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(
        data_files, 1,
        "expired lifecycle data files must be unlinked"
    );
    let metadata_json_files = std::fs::read_dir(lifecycle_loc.join("metadata"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".metadata.json"))
        })
        .count();
    assert_eq!(
        metadata_json_files, 1,
        "expired lifecycle metadata.json files must be unlinked"
    );
    let cold_rows = reopened.inner.lifecycle.rows(&cold_view).await.unwrap();
    assert_eq!(cold_rows.history_bytes(), stats.metadata_bytes_after);
    assert!(cold_rows.contains_key("operation/op-pending"));
    assert!(cold_rows.contains_key("export-result/exp-1"));
    assert!(
        cold_rows.prefix("owner-protection/").next().is_some(),
        "active reader grant must remain preserved"
    );

    // Verify release, succession proof, export pin, and retention inspection after cold reopen.
    verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
        .await
        .unwrap();
    let proof = reopened
        .prove_succession(&r0.version_ref(), &latest.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 6);
    let objects = latest.dataset.groups["source"].objects.clone();
    let inspection = reopened.inspect_retention(&objects).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    assert!(inspection.protected_bytes > 0);
    assert_eq!(export_pin.id, Id::new("exp-1").unwrap());

    // Abandon the unfinished operation so subsequent cutovers and object reclamation can proceed cleanly.
    reopened
        .abandon(&dataset, &Id::new("op-pending").unwrap(), "test cleanup")
        .await
        .unwrap();
    reopened.compact_lifecycle(&dataset).await.unwrap();
    assert!(
        !Path::new(&r0.manifest.path).exists(),
        "checkpointed release manifest should be unlinked from disk once incomplete work is resolved"
    );
    assert!(
        Path::new(&latest.manifest.path).is_file(),
        "active release manifest must remain on disk"
    );
    assert!(
        Path::new(&export_pin.object.path).is_file(),
        "exported parquet file must remain on disk"
    );
    for object in &latest.dataset.groups["source"].objects {
        assert!(
            Path::new(&object.path).is_file(),
            "active source table object {} must remain on disk",
            object.path
        );
    }
    verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
        .await
        .unwrap();
    let inspection = reopened.inspect_retention(&objects).await.unwrap();
    assert_eq!(inspection.unmatched_bytes, 0);
    reopened
        .create_restore_point(&dataset, &Id::new("rp-after-compact").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn compact_lifecycle_survives_lost_commit_response_with_succession_checkpoint() {
    let root = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 64 << 20,
        batch_rows: 128,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
    };
    let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
    let dataset = Id::new("checkpoint_lost_reply").unwrap();
    let init_input = write_parquet_input(&root.path().join("init.parquet"), &[1, 2]);
    let r0 = run_build_step(&engine, &dataset, "op-0", &init_input).await;
    let latest = run_update_steps(&engine, root.path(), &dataset, r0.clone(), 1..=3).await;

    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = engine.compact_lifecycle(&dataset).await.unwrap_err();
    assert_eq!(err.code(), "lost_commit_response");

    drop(engine);
    let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
    let proof = reopened
        .prove_succession(&r0.version_ref(), &latest.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 3);
    verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
        .await
        .unwrap();

    let retry_stats = reopened.compact_lifecycle(&dataset).await.unwrap();
    assert_eq!(retry_stats.revisions_after, 1);
    assert_eq!(retry_stats.files_after, 1);
    assert!(
        !Path::new(&r0.manifest.path).exists(),
        "checkpointed release manifest should be reclaimed on retry after lost commit response"
    );
    let proof_after = reopened
        .prove_succession(&r0.version_ref(), &latest.version_ref())
        .await
        .unwrap();
    assert_eq!(proof_after.selections_advanced, 3);
    verify_release_at(&latest, limits, VerificationLevel::Deep)
        .await
        .unwrap();
}

#[tokio::test]
async fn compact_lifecycle_keeps_260_operations_within_metadata_budget() {
    let root = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        max_metadata_bytes: 3 << 20,
        ..Limits::default()
    };
    let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
    let dataset = Id::new("hourly").unwrap();
    let init_input = write_parquet_input(&root.path().join("init.parquet"), &[0, 1]);
    let first = run_build_step(&engine, &dataset, "op-0", &init_input).await;
    let mut latest = first.clone();
    let mut view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    for step in 1..260_u64 {
        let name = format!("op-{step}");
        let (mut req, mut op) = admission(&engine, &dataset, &name, step + 1);
        req.input = init_input.path.clone();
        req.expected_digest = init_input.digest.clone();
        req.sql = format!("SELECT id, val FROM input WHERE id >= -{step}");
        op.request_digest = fingerprint(&OperationRequest::Build(req.clone())).unwrap();
        let phase = PhaseResult {
            operation: req.operation.clone(),
            phase: "processing".into(),
            request_digest: op.request_digest.clone(),
            eligibility: 0,
            outputs: latest.dataset.groups["source"].objects.clone(),
            table: Some(latest.dataset.groups["source"].clone()),
            rows: latest.dataset.groups["source"].rows,
            changes: None,
            coverage: None,
            metrics: WriteMetrics::default(),
            dependencies: Some(PhaseDependencies::default()),
        };
        let mut validation = phase.clone();
        validation.phase = "validation".into();
        let mut next_pin = latest.clone();
        next_pin.id = req.operation.clone();
        next_pin.dataset.version = req.operation.clone();
        next_pin.dataset.boundary = op.boundary;
        next_pin.dataset.request = OperationRequest::Build(req.clone());
        let link = crate::model::Succession {
            selected: next_pin.version_ref(),
            predecessor: Some(latest.version_ref()),
            boundary: op.boundary,
            eligibility: op.eligibility,
        };
        let authority = Authority {
            definition: "v1".into(),
            eligibility: 0,
            epoch: 0,
            boundary: op.boundary,
            selected: Some(next_pin.dataset.clone()),
            release: Some(next_pin.clone()),
            selection_revision: Some(view.revision + 2),
        };
        view = engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(format!("operation/{name}"), &op).unwrap(),
                    record(
                        format!("request/{name}"),
                        OperationRequest::Build(req.clone()),
                    )
                    .unwrap(),
                    record(format!("phase/{}/0/processing", op.request_digest), &phase).unwrap(),
                    record(
                        format!("phase/checked/{}/validation", op.request_digest),
                        &validation,
                    )
                    .unwrap(),
                    record(
                        format!("readback/{name}"),
                        ValidatedReadback::stamp(&next_pin),
                    )
                    .unwrap(),
                ],
            )
            .await
            .unwrap();
        view = engine
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(format!("result/{name}"), &next_pin).unwrap(),
                    record(format!("succession/{name}"), &link).unwrap(),
                    record("authority", &authority).unwrap(),
                ],
            )
            .await
            .unwrap();
        latest = next_pin;
        if step % 26 == 0 {
            let stats = engine.compact_lifecycle(&dataset).await.unwrap();
            assert_eq!(stats.revisions_after, 1);
            assert_eq!(stats.files_after, 1);
            assert!(
                stats.rows_after <= 20,
                "expected O(1) rows_after, got {}",
                stats.rows_after
            );
            assert!(
                stats.metadata_bytes_after < 32 << 10,
                "expected O(1) metadata_bytes_after < 32 KiB, got {}",
                stats.metadata_bytes_after
            );
            view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        }
    }
    // Complete operation 260 via a real DataFusion build over the compacted lifecycle.
    let upd_input = write_parquet_input(&root.path().join("upd-260.parquet"), &[1, 2]);
    latest = run_build_step(&engine, &dataset, "op-260", &upd_input).await;
    let final_stats = engine.compact_lifecycle(&dataset).await.unwrap();
    assert_eq!(final_stats.revisions_after, 1);
    assert_eq!(final_stats.files_after, 1);
    assert!(
        final_stats.rows_after <= 32,
        "rows_after={} must remain O(1) bounded",
        final_stats.rows_after
    );
    assert!(
        final_stats.metadata_bytes_after < 32 << 10,
        "metadata_bytes_after={} must remain O(1) bounded (< 32 KiB)",
        final_stats.metadata_bytes_after
    );

    drop(engine);
    let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
    let cold_view = reopened.inner.lifecycle.open(&dataset).await.unwrap();
    let cold_rows = reopened.inner.lifecycle.rows(&cold_view).await.unwrap();
    assert_eq!(cold_rows.history_bytes(), final_stats.metadata_bytes_after);
    assert!(cold_rows.contains_key("succession-checkpoint"));
    assert!(!cold_rows.contains_key("result/op-0"));
    assert!(!cold_rows.contains_key("result/op-259"));
    assert!(cold_rows.contains_key("result/op-260"));
    verify_release_at(&latest, limits, VerificationLevel::Deep)
        .await
        .unwrap();
    let proof = reopened
        .prove_succession(&first.version_ref(), &latest.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 260);
}
