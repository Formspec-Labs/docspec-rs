//! Closed real Parquet stages exercise scoped cleanup with maintained S3 clients.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::export::ExportRows;
use crate::storage::remote::objects::tests::{
    ReadFixture,
    writes::{Behavior, WriteFixture},
};
use arrow::array::Int64Array;
use std::collections::HashMap;

fn limits() -> Limits {
    Limits {
        compute_threads: 1,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 1 << 20,
    }
}
fn id(name: &str) -> Id {
    Id::new(name).unwrap()
}

struct Fixture {
    root: tempfile::TempDir,
    engine: Engine,
    spec: ExportSpec,
    stage: ExportMaterialized,
    writer: WriteFixture,
    expected: Vec<u8>,
    preserved: BTreeMap<String, serde_json::Value>,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let host = root.path().join("host");
    let local = Engine::open(&host, limits()).await.unwrap();
    let input = root.path().join("input.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
    )
    .unwrap();
    let mut parquet = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    parquet.write(&batch).unwrap();
    parquet.close().unwrap();
    let source = local
        .run(&RunSpec {
            dataset: id("data"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&input).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let policy = ExportPolicy {
        group: "source".into(),
        rows: ExportRows::All,
        columns: vec!["value".into()],
    };
    // Actual maintained writer/validation produce the staged bytes. This fixture
    // qualifies transfer/cleanup, not remote native source materialization.
    let original = local
        .export(&ExportSpec {
            dataset: id("data"),
            operation: id("writer"),
            source: source.version_ref(),
            channel: id("local-download"),
            expected: None,
            policy: policy.clone(),
        })
        .await
        .unwrap();
    let expected = std::fs::read(&original.object.path).unwrap();
    let spec = ExportSpec {
        dataset: id("data"),
        operation: id("remote-export"),
        source: source.version_ref(),
        channel: id("download"),
        expected: None,
        policy,
    };
    let operation = local
        .admit_export(
            &spec,
            storage::json_digest(&spec).unwrap().to_hex().as_ref(),
        )
        .await
        .unwrap();
    let view = local.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = local.inner.lifecycle.rows(&view).await.unwrap();
    let revision = rows.revision("export-operation/remote-export").unwrap();
    let attempt_id = id("closed-stage");
    let host_prefix = format!(
        "{}/objects/{}/{attempt_id}/",
        local.inner.root.display(),
        spec.dataset
    );
    let destination = format!(
        "s3://bucket/owned/{}/{attempt_id}/export.parquet",
        spec.dataset
    );
    let writer = WriteFixture::for_path(
        Behavior::LosePutResponse,
        format!("/bucket/owned/{}/{attempt_id}/export.parquet", spec.dataset),
    );
    let engine = Engine::compose(
        local.inner.root.clone(),
        local.inner.lifecycle.catalog.clone(),
        storage::Objects::Remote(writer.objects()),
        limits(),
        local.inner.host.clone(),
        false,
    )
    .unwrap();
    drop(local);
    let attempt = ExportAttempt {
        id: attempt_id.clone(),
        operation: spec.operation.clone(),
        operation_revision: revision,
        request_digest: operation.request_digest.clone(),
        eligibility: operation.eligibility,
        epoch: operation.epoch,
        host_prefix: host_prefix.clone(),
        retained_prefix: format!("s3://bucket/owned/{}/{attempt_id}/", spec.dataset),
        destination,
    };
    let protection = Protection {
        id: attempt_id.clone(),
        consumer: spec.dataset.clone(),
        purpose: format!("{}/export", spec.operation),
        paths: vec![attempt.host_prefix.clone(), attempt.retained_prefix.clone()],
        retired: false,
    };
    let suffix = format!("{}/{revision}", spec.operation);
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record(format!("export-attempt/{suffix}"), &attempt).unwrap(),
                record(format!("protection/{attempt_id}"), &protection).unwrap(),
            ],
        )
        .await
        .unwrap();
    std::fs::create_dir_all(host_prefix.trim_end_matches('/')).unwrap();
    let path = format!("{host_prefix}stage-{}.parquet", uuid::Uuid::new_v4());
    std::fs::copy(&original.object.path, &path).unwrap();
    let stage = ExportMaterialized {
        request_digest: operation.request_digest.clone(),
        object: ObjectRef {
            path,
            ..original.object.clone()
        },
        rows: original.rows,
        fields: original.fields.clone(),
    };
    let validation = ExportValidation {
        object: stage.object.clone(),
        ..original.validation.clone()
    };
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![record(format!("export-staged/{suffix}"), &stage).unwrap()],
        )
        .await
        .unwrap();
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![record(format!("export-stage-validation/{suffix}"), &validation).unwrap()],
        )
        .await
        .unwrap();
    let preserved = engine
        .inspect(&spec.dataset)
        .await
        .unwrap()
        .into_iter()
        .filter(|(key, _)| {
            key.starts_with("export-attempt/")
                || key.starts_with("export-staged/")
                || key.starts_with("export-stage-validation/")
                || key == "protection/closed-stage"
        })
        .collect();
    Fixture {
        root,
        engine,
        spec,
        stage,
        writer,
        expected,
        preserved,
    }
}

async fn remote_completion(fixture: &Fixture) {
    assert_eq!(
        fixture
            .engine
            .plan_export_stage_removal(
                &fixture.spec.dataset,
                &id("stage-removal"),
                &fixture.spec.operation,
                "closed stage cleanup"
            )
            .await
            .unwrap_err()
            .code(),
        "export_stage_pending"
    );
    assert_eq!(
        fixture
            .engine
            .export_until(&fixture.spec, "transfer-uncommitted")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    // A published object alone cannot end host recovery availability.
    assert_eq!(
        fixture
            .engine
            .plan_export_stage_removal(
                &fixture.spec.dataset,
                &id("stage-removal"),
                &fixture.spec.operation,
                "closed stage cleanup"
            )
            .await
            .unwrap_err()
            .code(),
        "export_stage_pending"
    );
    assert_eq!(
        fixture
            .engine
            .export_until(&fixture.spec, "transfer")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let view = fixture
        .engine
        .inner
        .lifecycle
        .open(&fixture.spec.dataset)
        .await
        .unwrap();
    let rows = fixture.engine.inner.lifecycle.rows(&view).await.unwrap();
    let completed = completed_stage(
        &rows,
        &fixture.spec.operation,
        Some(&fixture.engine.inner.lifecycle.budget),
    )
    .unwrap()
    .unwrap();
    assert_eq!(completed.object, fixture.stage.object);
    let protection: Protection = rows
        .get_record("protection/closed-stage")
        .unwrap()
        .unwrap()
        .2;
    assert_eq!(
        completed_host_prefix(&protection, &rows).unwrap(),
        Some(completed.host_prefix)
    );
    for (key, json) in &fixture.preserved {
        if key.starts_with("export-staged/") || key.starts_with("export-stage-validation/") {
            assert!(
                validate_record(key, &json.to_string(), &rows, None)
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test]
async fn completed_remote_stage_cleanup_reconciles_intent_and_unlink_gaps() {
    for phase in ["removal-intent", "removal-unlink", "lost-removal-outcome"] {
        let fixture = fixture().await;
        remote_completion(&fixture).await;
        let Fixture {
            root,
            engine,
            spec,
            stage,
            writer,
            expected,
            preserved,
        } = fixture;
        let report = writer.finish();
        assert_eq!(report.published.as_ref(), Some(&expected));
        assert_eq!(report.methods("PUT"), 1);
        // The mock transport is closed now. Cleanup and completed export retry
        // must succeed solely from durable metadata, without remote requests.
        let plan = engine
            .plan_export_stage_removal(
                &spec.dataset,
                &id("stage-removal"),
                &spec.operation,
                "closed stage cleanup",
            )
            .await
            .unwrap();
        assert!(plan.registrations.is_empty());
        if phase == "lost-removal-outcome" {
            let status = engine.removal_until(&plan, phase).await.unwrap();
            assert!(status.complete);
        } else {
            assert_eq!(
                engine.removal_until(&plan, phase).await.unwrap_err().code(),
                "injected_failure"
            );
            assert_eq!(
                Path::new(&stage.object.path).exists(),
                phase == "removal-intent"
            );
        }
        let status = engine
            .resume_removal(&spec.dataset, &plan.removal)
            .await
            .unwrap();
        assert!(status.complete);
        assert!(!Path::new(&stage.object.path).exists());
        assert_eq!(
            status.confirmed_unlinked_bytes + status.reconciled_absent_bytes,
            stage.object.bytes
        );
        assert_eq!(
            status.reconciled_absent_bytes,
            if phase == "removal-unlink" {
                stage.object.bytes
            } else {
                0
            }
        );
        assert_eq!(engine.remove_objects(&plan).await.unwrap(), status);
        let pin = engine.export(&spec).await.unwrap();
        assert_eq!(pin.object.digest, stage.object.digest);
        assert!(pin.object.path.starts_with("s3://"));
        let records = engine.inspect(&spec.dataset).await.unwrap();
        for (key, value) in &preserved {
            assert_eq!(&records[key], value);
        }
        let catalog = engine.inner.lifecycle.catalog.clone();
        drop(engine);
        let read = ReadFixture::inventory(HashMap::from([(pin.object.path.clone(), expected)]));
        let cold_host = root.path().join("cold");
        let ownership = worker::Host::acquire(&cold_host).unwrap();
        let cold = Engine::compose(
            std::fs::canonicalize(&cold_host).unwrap(),
            catalog,
            storage::Objects::Remote(read.objects()),
            limits(),
            ownership,
            false,
        )
        .unwrap();
        assert_eq!(
            cold.remove_objects(&plan).await.unwrap_err().code(),
            "export_host_unavailable"
        );
        let mut batches = cold.read_export(&pin).await.unwrap();
        let mut values = Vec::new();
        while let Some(batch) = batches.try_next().await.unwrap() {
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter(),
            );
        }
        assert_eq!(values, vec![Some(1), None]);
        cold.shutdown().await.unwrap();
        assert!(
            read.finish()
                .iter()
                .all(|(method, _)| matches!(method.as_str(), "HEAD" | "GET"))
        );
    }
}

#[tokio::test]
async fn stage_cleanup_refuses_forged_child_and_changed_bytes() {
    let fixture = fixture().await;
    remote_completion(&fixture).await;
    let Fixture {
        root: _root,
        engine,
        spec,
        stage,
        writer,
        ..
    } = fixture;
    writer.finish();
    let plan = engine
        .plan_export_stage_removal(
            &spec.dataset,
            &id("stage-removal"),
            &spec.operation,
            "closed stage cleanup",
        )
        .await
        .unwrap();
    for change_digest in [true, false] {
        let mut wrong = plan.clone();
        if change_digest {
            wrong.inspection.objects[0].object.digest = "0".repeat(64);
        } else {
            wrong.inspection.objects[0].object.path = Path::new(&stage.object.path)
                .parent()
                .unwrap()
                .join("unqualified.parquet")
                .to_string_lossy()
                .into_owned();
        }
        assert_eq!(
            engine.remove_objects(&wrong).await.unwrap_err().code(),
            "invalid_removal"
        );
        assert!(Path::new(&stage.object.path).exists());
    }
    let mut bytes = std::fs::read(&stage.object.path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&stage.object.path, bytes).unwrap();
    let status = engine.remove_objects(&plan).await.unwrap();
    assert!(!status.complete);
    assert!(matches!(
        &status.progress.objects[0],
        RemovalOutcome::Failed { .. }
    ));
    assert!(Path::new(&stage.object.path).exists());
}

#[tokio::test]
async fn stage_cleanup_refuses_symlink_without_removing_target() {
    let fixture = fixture().await;
    remote_completion(&fixture).await;
    let Fixture {
        root,
        engine,
        spec,
        stage,
        writer,
        expected,
        ..
    } = fixture;
    writer.finish();
    let plan = engine
        .plan_export_stage_removal(
            &spec.dataset,
            &id("symlink-removal"),
            &spec.operation,
            "closed stage cleanup",
        )
        .await
        .unwrap();
    let target = root.path().join("external.parquet");
    std::fs::write(&target, &expected).unwrap();
    std::fs::remove_file(&stage.object.path).unwrap();
    std::os::unix::fs::symlink(&target, &stage.object.path).unwrap();
    let status = engine.remove_objects(&plan).await.unwrap();
    assert!(!status.complete);
    assert!(matches!(
        &status.progress.objects[0],
        RemovalOutcome::Failed { .. }
    ));
    assert_eq!(std::fs::read(&target).unwrap(), expected);
    assert!(
        std::fs::symlink_metadata(&stage.object.path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[tokio::test]
async fn stage_cleanup_honors_exact_retained_grant_until_retirement() {
    let fixture = fixture().await;
    remote_completion(&fixture).await;
    let Fixture {
        root: _root,
        engine,
        spec,
        stage,
        writer,
        ..
    } = fixture;
    writer.finish();
    engine
        .test_stage_grant_lifetime(&stage.object, &spec.operation)
        .await;
}

#[tokio::test]
async fn stage_cleanup_waits_for_configured_reader_and_its_admitted_stream() {
    let fixture = fixture().await;
    remote_completion(&fixture).await;
    let Fixture {
        root: _root,
        engine,
        spec,
        stage,
        writer,
        expected,
        ..
    } = fixture;
    let pin = engine.export(&spec).await.unwrap();
    let plan = engine
        .plan_export_stage_removal(
            &spec.dataset,
            &id("reader-stage-removal"),
            &spec.operation,
            "closed stage cleanup",
        )
        .await
        .unwrap();
    writer.finish();
    let (read, reached, release) =
        ReadFixture::held_inventory(HashMap::from([(pin.object.path.clone(), expected)]));
    let configured = Engine::compose(
        engine.inner.root.clone(),
        engine.inner.lifecycle.catalog.clone(),
        storage::Objects::Remote(read.objects()),
        limits(),
        engine.inner.host.clone(),
        false,
    )
    .unwrap();
    drop(engine);
    let reader = configured.clone();
    let opening = tokio::spawn(async move { reader.read_export(&pin).await.unwrap() });
    tokio::time::timeout(std::time::Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    let remover = configured.clone();
    let mut removing = tokio::spawn(async move { remover.remove_objects(&plan).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut removing)
            .await
            .is_err()
    );
    assert!(Path::new(&stage.object.path).exists());
    release.send(()).unwrap();
    let stream = tokio::time::timeout(std::time::Duration::from_secs(5), opening)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut removing)
            .await
            .is_err()
    );
    drop(stream);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), removing)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(!Path::new(&stage.object.path).exists());
    configured.shutdown().await.unwrap();
    read.finish();
}

#[tokio::test]
async fn remote_export_owner_proof_accepts_exact_dual_path_completion_after_retirement() {
    let fixture = fixture().await;
    remote_completion(&fixture).await;
    let Fixture {
        root: _root,
        engine,
        spec,
        writer,
        ..
    } = fixture;
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let completed = completed_stage(&rows, &spec.operation, Some(&engine.inner.lifecycle.budget))
        .unwrap()
        .unwrap();
    let object = completed.remote;
    let report = writer.finish();
    assert_eq!(report.methods("PUT"), 1);
    // Creation and ownership resolve entirely from retained native metadata.
    // The closed transport makes an accidental payload recheck fail this test.
    assert_eq!(
        engine
            .qualified_object(&spec.dataset, &object.path, &object.digest)
            .await
            .unwrap(),
        object
    );
    let retirement = engine
        .plan_export_retirement(
            &spec.dataset,
            &id("end-export"),
            &spec.operation,
            "end unused recovery",
        )
        .await
        .unwrap();
    engine.retire_export(&retirement).await.unwrap();
    assert_eq!(
        engine
            .qualified_object(&spec.dataset, &object.path, &object.digest)
            .await
            .unwrap(),
        object
    );
    let unknown = object.path.replace("export.parquet", "uncreated.parquet");
    assert_eq!(
        engine
            .qualified_object(&spec.dataset, &unknown, &object.digest)
            .await
            .unwrap_err()
            .code(),
        "unqualified_owner_reference"
    );
    let view = engine.inner.lifecycle.open(&spec.dataset).await.unwrap();
    let mut protection: Protection = engine
        .inner
        .lifecycle
        .get(&view, "protection/closed-stage")
        .await
        .unwrap()
        .unwrap();
    protection.paths.push("s3://bucket/owned/another/".into());
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![record("protection/closed-stage", &protection).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .qualified_object(&spec.dataset, &object.path, &object.digest)
            .await
            .unwrap_err()
            .code(),
        "invalid_export"
    );
    engine.shutdown().await.unwrap();
}
