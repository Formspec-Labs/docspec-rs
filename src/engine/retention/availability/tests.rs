//! Retirement must preserve current authority and reconcile one exact decision.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

async fn fixture(root: &Path) -> (Engine, RunSpec, ReleasePin) {
    let input = root.join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let engine = Engine::open(
        root.join("workspace"),
        Limits {
            compute_threads: 2,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 4 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let first = RunSpec {
        dataset: id("comments"),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: input.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&input).unwrap().0,
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
    };
    let old = engine.run(&first).await.unwrap();
    engine
        .run(&RunSpec {
            operation: id("current"),
            ..first.clone()
        })
        .await
        .unwrap();
    (engine, first, old)
}

#[tokio::test]
async fn lost_reply_and_concurrent_retry_preserve_one_availability_decision() {
    let root = tempfile::tempdir().unwrap();
    let (engine, first, old) = fixture(root.path()).await;
    let plan = engine
        .plan_availability_retirement(
            &first.dataset,
            &id("retire"),
            &first.operation,
            "end standalone access",
        )
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (left, right) = tokio::join!(
        engine.retire_availability(&plan),
        engine.retire_availability(&plan)
    );
    let decision = left.unwrap();
    assert_eq!(decision, right.unwrap());
    assert_eq!(
        lifecycle.open(&first.dataset).await.unwrap().revision,
        decision.revision
    );
    assert_eq!(
        engine.run(&first).await.unwrap_err().code(),
        "availability_retired"
    );
    let inventory = engine
        .inspect_retention(std::slice::from_ref(&old.manifest))
        .await
        .unwrap();
    assert!(
        inventory.objects[0].blocker.is_some(),
        "existing grants and independent retained work still protect bytes"
    );
    assert!(Path::new(&old.manifest.path).exists());
}

#[tokio::test]
async fn inconsistent_current_authority_cannot_authorize_retirement() {
    let root = tempfile::tempdir().unwrap();
    let (engine, first, old) = fixture(root.path()).await;
    let plan = engine
        .plan_availability_retirement(
            &first.dataset,
            &id("retire"),
            &first.operation,
            "end standalone access",
        )
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&first.dataset).await.unwrap();
    let (_, _, authority): (_, _, Authority) = lifecycle
        .rows(&view)
        .await
        .unwrap()
        .get_record("authority")
        .unwrap()
        .unwrap();
    for change in ["operation", "owner", "revision", "receipt"] {
        let mut bad = authority.clone();
        match change {
            "operation" => bad.release.as_mut().unwrap().id = id("different"),
            "owner" => bad.release.as_mut().unwrap().manifest.owner = id("different"),
            "revision" => bad.selection_revision = Some(1),
            "receipt" => {
                bad.release.as_mut().unwrap().manifest.digest = "0".repeat(64);
            }
            _ => unreachable!(),
        }
        let view = lifecycle.open(&first.dataset).await.unwrap();
        lifecycle
            .commit(&view, vec![record("authority", &bad).unwrap()])
            .await
            .unwrap();
        for operation in [&first.operation, &id("current")] {
            assert_eq!(
                engine
                    .plan_availability_retirement(
                        &first.dataset,
                        &id("other"),
                        operation,
                        "end standalone access"
                    )
                    .await
                    .unwrap_err()
                    .code(),
                "retention_inventory",
                "{change}"
            );
        }
        assert_eq!(
            engine.retire_availability(&plan).await.unwrap_err().code(),
            "retention_inventory",
            "{change}"
        );
        assert_eq!(
            engine.inspect_retention(&[]).await.unwrap_err().code(),
            "retention_inventory",
            "{change}"
        );
        let view = lifecycle.open(&first.dataset).await.unwrap();
        lifecycle
            .commit(&view, vec![record("authority", &authority).unwrap()])
            .await
            .unwrap();
    }
    engine.retire_availability(&plan).await.unwrap();
    assert_eq!(
        engine.latest(&first.dataset).await.unwrap().id,
        id("current")
    );
    // A fully consistent old alias still cannot become a current authority
    // after the referenced availability was retired.
    let view = lifecycle.open(&first.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let mut bad = authority;
    bad.selection_revision = rows.revision("result/initial");
    bad.selected = Some(old.dataset.clone());
    bad.release = Some(old);
    lifecycle
        .commit(&view, vec![record("authority", &bad).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        engine.inspect_retention(&[]).await.unwrap_err().code(),
        "retention_inventory"
    );
}

#[tokio::test]
async fn pointer_receipt_and_marker_must_form_one_native_decision() {
    for missing in ["pointer", "receipt", "marker", "pointer_identity"] {
        let root = tempfile::tempdir().unwrap();
        let (engine, first, _) = fixture(root.path()).await;
        let plan = engine
            .plan_availability_retirement(
                &first.dataset,
                &id("retire"),
                &first.operation,
                "end standalone access",
            )
            .await
            .unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&first.dataset).await.unwrap();
        let receipt = Retirement {
            plan: plan.clone(),
            revision: view.revision + 1,
        };
        let mut records = Vec::new();
        if missing != "receipt" {
            records.push(record("retirement/retire", &receipt).unwrap());
        }
        if missing != "pointer" {
            let mut pointer = pointer_record(&plan, &first.operation).unwrap();
            if missing == "pointer_identity" {
                pointer.1["operation"] = serde_json::json!("different");
            }
            records.push(pointer);
        }
        if missing != "marker" {
            for generation in &plan.records {
                records.push(
                    record(
                        retirement::marker_key(generation),
                        retirement::RetiredRecord {
                            dataset: plan.dataset.clone(),
                            table_uuid: plan.table_uuid.clone(),
                            record: generation.clone(),
                            retirement: plan.retirement.clone(),
                        },
                    )
                    .unwrap(),
                );
            }
        }
        lifecycle.commit(&view, records).await.unwrap();
        assert_eq!(
            engine.retire_availability(&plan).await.unwrap_err().code(),
            "invalid_retirement",
            "{missing}"
        );
        assert_eq!(
            engine.inspect_retention(&[]).await.unwrap_err().code(),
            "invalid_retirement",
            "{missing}"
        );
        if missing != "pointer" {
            assert_eq!(
                engine.run(&first).await.unwrap_err().code(),
                "invalid_retirement",
                "{missing}"
            );
        }
    }
}
