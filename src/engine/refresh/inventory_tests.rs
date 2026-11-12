//! Caller-scoped join inventory replay and admission lifetime checks.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

async fn fixture() -> (TempDir, Engine, ReleasePin, JoinTransform) {
    let root = TempDir::new().unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            compute_threads: 2,
            memory_bytes: 512 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let release = engine
        .run(&RunSpec {
            base: None,
            relationship: None,
            dataset: Id::new("source").unwrap(),
            operation: Id::new("initial").unwrap(),
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let join = JoinTransform {
        left: release.version_ref(),
        right: release.version_ref(),
        join: JoinDefinition {
            keys: vec![JoinKey {
                left: "id".into(),
                right: "id".into(),
            }],
            kind: JoinKind::Inner,
            nulls: NullMatching::Equal,
            cardinality: JoinCardinality::OneToOne,
            fields: vec![JoinField {
                side: JoinSide::Left,
                field: "id".into(),
                name: "id".into(),
            }],
            filter: None,
            projections: BTreeMap::new(),
        },
        filter: None,
        projections: BTreeMap::new(),
    };
    (root, engine, release, join)
}

fn budget() -> Budget {
    Budget::new(Limits {
        compute_threads: 2,
        memory_bytes: 16 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 64 << 10,
        batch_rows: 2,
        ..Limits::default()
    })
    .unwrap()
}

#[tokio::test]
async fn cardinality_and_main_join_replay_without_manifest_and_share_self_join_admission() {
    let (_root, _engine, release, join) = fixture().await;
    let budget = budget();
    let context = budget.context();
    let releases = BTreeMap::from([(release.dataset.dataset.clone(), &release)]);
    register_join_inputs(&context, &join, &releases, &budget, true)
        .await
        .unwrap();
    // Exactly one captured inventory owns the existing double-metadata allowance.
    let remaining = budget.limits.memory_bytes / 2 - budget.limits.max_metadata_bytes * 2;
    drop(budget.reserve(remaining).unwrap());
    assert!(budget.reserve(remaining + 1).is_err());

    let native = storage::open_pin(&release.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let manifest = native
        .metadata()
        .current_snapshot()
        .unwrap()
        .manifest_list();
    std::fs::remove_file(manifest).unwrap();
    assert!(storage::snapshot_files(&native, &budget).await.is_err());
    // Each iteration executes both cardinality probes and the main join.
    for _ in 0..2 {
        let batches = join_plan(&context, &join, true)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
            })
            .collect();
        values.sort();
        assert_eq!(values, [None, Some(1), Some(2)]);
        for batch in &batches {
            for name in TUPLE {
                assert_eq!(batch.column_by_name(name).unwrap().null_count(), 0);
            }
        }
    }
    context.deregister_table("join_left").unwrap();
    assert!(budget.reserve(remaining + 1).is_err());
    context.deregister_table("join_right").unwrap();
    drop(context);
    drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
}

#[tokio::test]
async fn one_shot_join_inputs_keep_inventory_lazy() {
    let (_root, _engine, release, mut join) = fixture().await;
    let releases = BTreeMap::from([(release.dataset.dataset.clone(), &release)]);
    for (cardinality, enforce) in [
        (JoinCardinality::OneToOne, false),
        (JoinCardinality::ManyToMany, true),
    ] {
        join.join.cardinality = cardinality;
        let budget = budget();
        let context = budget.context();
        register_join_inputs(&context, &join, &releases, &budget, enforce)
            .await
            .unwrap();
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }
}

#[tokio::test]
async fn select_prepared_with_policy_enforces_requested_policy_across_refresh_and_build() {
    let (root, engine, _source, join) = fixture().await;
    let policy_id = Id::new("policy-a").unwrap();
    let other_policy_id = Id::new("policy-b").unwrap();

    // 1. Unqualified Build must refuse `Some(policy_id)` before `result/{op}` exists.
    let build_input = root.path().join("input.parquet");
    let build_spec = RunSpec {
        base: None,
        relationship: None,
        dataset: Id::new("unqualified-build").unwrap(),
        operation: Id::new("build-1").unwrap(),
        input: build_input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&build_input).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    };
    let build_req = OperationRequest::Build(build_spec.clone());
    assert_eq!(
        engine
            .run_admitted(&build_req, Some("readback"))
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let build_view = engine
        .inner
        .lifecycle
        .open(&build_spec.dataset)
        .await
        .unwrap();
    let build_op: Operation = engine
        .inner
        .lifecycle
        .get(&build_view, "operation/build-1")
        .await
        .unwrap()
        .unwrap();
    let build_rb: ValidatedReadback = engine
        .inner
        .lifecycle
        .get(&build_view, "readback/build-1")
        .await
        .unwrap()
        .unwrap();
    let build_protections = engine
        .retained_protections(&build_view, "readback/build-1")
        .await
        .unwrap();
    assert_eq!(
        engine
            .select_prepared_with_policy(
                &build_req,
                &build_op,
                Retained {
                    value: build_rb.release.clone(),
                    protections: build_protections,
                },
                None,
                Some(&policy_id),
            )
            .await
            .unwrap_err()
            .code(),
        "unknown_operation"
    );
    let build_view = engine
        .inner
        .lifecycle
        .open(&build_spec.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<ReleasePin>(&build_view, "result/build-1")
            .await
            .unwrap()
            .is_none()
    );

    // 2. Unqualified Refresh must also refuse `Some(policy_id)` before `result/{op}` exists.
    let refresh_spec = RefreshSpec {
        dataset: Id::new("related").unwrap(),
        operation: Id::new("refresh-1").unwrap(),
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        partition_by: Vec::new(),
        transform: RefreshTransform::Join(join.clone()),
    };
    let refresh_req = OperationRequest::Refresh(Box::new(refresh_spec.clone()));
    assert_eq!(
        engine
            .run_admitted(&refresh_req, Some("readback"))
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let view = engine
        .inner
        .lifecycle
        .open(&refresh_spec.dataset)
        .await
        .unwrap();
    let refresh_op: Operation = engine
        .inner
        .lifecycle
        .get(&view, "operation/refresh-1")
        .await
        .unwrap()
        .unwrap();
    let refresh_rb: ValidatedReadback = engine
        .inner
        .lifecycle
        .get(&view, "readback/refresh-1")
        .await
        .unwrap()
        .unwrap();
    let refresh_protections = engine
        .retained_protections(&view, "readback/refresh-1")
        .await
        .unwrap();
    assert_eq!(
        engine
            .select_prepared_with_policy(
                &refresh_req,
                &refresh_op,
                Retained {
                    value: refresh_rb.release.clone(),
                    protections: refresh_protections.clone(),
                },
                None,
                Some(&policy_id),
            )
            .await
            .unwrap_err()
            .code(),
        "unknown_operation"
    );
    let view = engine
        .inner
        .lifecycle
        .open(&refresh_spec.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<ReleasePin>(&view, "result/refresh-1")
            .await
            .unwrap()
            .is_none()
    );

    // 3. Policy-governed Refresh must refuse unmanaged selection, wrong policy, and unaccepted policy.
    let policy: managed_build::QualificationPolicy = serde_json::from_value(serde_json::json!({
        "id": "policy-a",
        "rules_sha256": "a".repeat(64),
        "evaluator": {
            "id": "urn:ddk:quality:datacontract-cli",
            "version": "1.2.9",
            "distribution_sha256": "b".repeat(64),
        },
        "semantics": {
            "id": "urn:ddk:quality:duckdb-direct-view",
            "revision": "1",
        },
        "required_checks": ["check-1"],
        "expected_epoch": 0,
        "expected_selection": null,
    }))
    .unwrap();
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record(
                    managed_build::initial_policy_key(&refresh_spec.operation),
                    &policy,
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .select_prepared(
                &refresh_req,
                &refresh_op,
                Retained {
                    value: refresh_rb.release.clone(),
                    protections: refresh_protections.clone(),
                },
                None,
            )
            .await
            .unwrap_err()
            .code(),
        "stale_qualification_policy"
    );
    assert_eq!(
        engine
            .select_prepared_with_policy(
                &refresh_req,
                &refresh_op,
                Retained {
                    value: refresh_rb.release.clone(),
                    protections: refresh_protections.clone(),
                },
                None,
                Some(&other_policy_id),
            )
            .await
            .unwrap_err()
            .code(),
        "stale_qualification_policy"
    );
    assert_eq!(
        engine
            .select_prepared_with_policy(
                &refresh_req,
                &refresh_op,
                Retained {
                    value: refresh_rb.release.clone(),
                    protections: refresh_protections.clone(),
                },
                None,
                Some(&policy_id),
            )
            .await
            .unwrap_err()
            .code(),
        "qualification_required"
    );
    let view = engine
        .inner
        .lifecycle
        .open(&refresh_spec.dataset)
        .await
        .unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<ReleasePin>(&view, "result/refresh-1")
            .await
            .unwrap()
            .is_none()
    );

    // 4. Completed unqualified Refresh must still check `guard_completed_policy` when `Some(policy_id)` is requested.
    let refresh_done_spec = RefreshSpec {
        dataset: Id::new("related-done").unwrap(),
        operation: Id::new("refresh-done").unwrap(),
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        partition_by: Vec::new(),
        transform: RefreshTransform::Join(join),
    };
    let refresh_done_req = OperationRequest::Refresh(Box::new(refresh_done_spec.clone()));
    let completed = engine.refresh(&refresh_done_spec).await.unwrap();
    let done_view = engine
        .inner
        .lifecycle
        .open(&refresh_done_spec.dataset)
        .await
        .unwrap();
    let done_op: Operation = engine
        .inner
        .lifecycle
        .get(&done_view, "operation/refresh-done")
        .await
        .unwrap()
        .unwrap();
    let done_protections = engine
        .retained_protections(&done_view, "readback/refresh-done")
        .await
        .unwrap();
    assert_eq!(
        engine
            .select_prepared_with_policy(
                &refresh_done_req,
                &done_op,
                Retained {
                    value: completed.clone(),
                    protections: done_protections.clone(),
                },
                None,
                Some(&policy_id),
            )
            .await
            .unwrap_err()
            .code(),
        "unknown_operation"
    );
    assert_eq!(
        engine
            .select_prepared(
                &refresh_done_req,
                &done_op,
                Retained {
                    value: completed.clone(),
                    protections: done_protections,
                },
                None,
            )
            .await
            .unwrap(),
        completed
    );
}
