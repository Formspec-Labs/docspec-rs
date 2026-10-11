//! Native catalog intent recovery; provider and full Engine qualification are separate.
// Rust guideline compliant 2026-02-21
use super::*;

#[tokio::test]
async fn saved_target_intent_survives_restart_and_refuses_retargeting_before_registration() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let source = storage::local_catalog(source_root.path()).await.unwrap();
    let target = storage::local_catalog(target_root.path()).await.unwrap();
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 128 << 20,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 1 << 20,
        ..Limits::default()
    })
    .unwrap();
    let dataset = Id::new("restore-intent").unwrap();
    let point_id = Id::new("catalog-loss").unwrap();
    let reference = RestorePointRef {
        dataset: dataset.clone(),
        id: point_id.clone(),
        manifest: ObjectRef {
            owner: dataset.clone(),
            path: format!(
                "s3://bucket/owned/{dataset}/restore_00000000000040008000000000000001/point.json"
            ),
            bytes: 19,
            digest: "a".repeat(64),
        },
    };
    let lifecycle = Lifecycle::new(source.clone(), budget.clone());
    let view = lifecycle.open(&dataset).await.unwrap();
    let view = lifecycle
        .commit(
            &view,
            vec![
                ("value".into(), serde_json::json!(17)),
                record(
                    "protection/restore_00000000000040008000000000000001",
                    restore_reservation(&reference).unwrap(),
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    let ident = crate::catalog::lifecycle_ident(&dataset);
    let metadata = storage::object(
        &dataset,
        Path::new(view.table.metadata_location_result().unwrap()),
    )
    .unwrap();
    let image_generation = uuid::Uuid::new_v4();
    // The point is still usable from a later serving generation. Its original
    // image generation remains unchanged; the new intent binds the current one.
    let current_generation = uuid::Uuid::new_v4();
    let target_generation = uuid::Uuid::new_v4();
    let image = CatalogImage {
        format: remote::FORMAT,
        generation: image_generation,
        namespaces: BTreeMap::from([(
            ident.namespace().to_string(),
            source
                .get_namespace(ident.namespace())
                .await
                .unwrap()
                .properties()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )]),
        tables: vec![Registration {
            table: ident.to_string(),
            table_uuid: view.table.metadata().uuid().to_string(),
            metadata: metadata.clone(),
            revision: view.revision,
            authority: Some(capture_authority(&lifecycle, &view).await.unwrap()),
        }],
    };
    assert!(
        !restore_intent(
            &target,
            &image,
            &reference,
            current_generation,
            target_generation,
            false,
            false
        )
        .await
        .unwrap()
    );
    assert!(!target.namespace_exists(ident.namespace()).await.unwrap());
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            current_generation,
            target_generation,
            false,
            true
        )
        .await
        .unwrap()
    );
    assert!(
        target
            .list_tables(ident.namespace())
            .await
            .unwrap()
            .is_empty()
    );
    drop(target);
    let target = storage::local_catalog(target_root.path()).await.unwrap();
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            current_generation,
            target_generation,
            false,
            false
        )
        .await
        .unwrap()
    );
    let mut other = reference.clone();
    other.id = Id::new("other-point").unwrap();
    assert!(
        restore_intent(
            &target,
            &image,
            &other,
            current_generation,
            target_generation,
            false,
            true
        )
        .await
        .is_err()
    );
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            image_generation,
            target_generation,
            false,
            true
        )
        .await
        .is_err()
    );
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            current_generation,
            target_generation,
            true,
            true
        )
        .await
        .is_err()
    );
    assert!(
        target
            .list_tables(ident.namespace())
            .await
            .unwrap()
            .is_empty()
    );
    source.drop_table(&ident).await.unwrap();
    target
        .register_table(&ident, metadata.path.clone())
        .await
        .unwrap();
    check_target(
        &Lifecycle::new(target.clone(), budget.clone()),
        &image,
        &reference,
    )
    .await
    .unwrap();
    let point = RemoteRestorePoint {
        format: remote::FORMAT,
        id: point_id,
        dataset: dataset.clone(),
        generation: image_generation,
        retained_prefix: "s3://bucket/owned".into(),
        catalog: ObjectRef {
            owner: dataset.clone(),
            path: reference
                .manifest
                .path
                .replace("point.json", "catalog.json"),
            bytes: 23,
            digest: "b".repeat(64),
        },
        objects: vec![metadata],
    };
    remote::validate(&reference, &point).unwrap();
    check_image(&image, &point).unwrap();
    let mut missing_lifecycle = image.clone();
    missing_lifecycle.tables[0].table = format!("{}.data", ident.namespace());
    assert!(check_image(&missing_lifecycle, &point).is_err());
    let mut extra_namespace = image.clone();
    extra_namespace
        .namespaces
        .insert("unrestorable".into(), BTreeMap::new());
    assert!(check_image(&extra_namespace, &point).is_err());
    let mut wrong_owner = point.clone();
    wrong_owner.dataset = Id::new("missing-owner").unwrap();
    assert!(check_image(&image, &wrong_owner).is_err());

    let restored = Lifecycle::new(target, budget);
    let fenced = fence_registration(&restored, &image.tables[0])
        .await
        .unwrap();
    assert_eq!(fenced.revision, image.tables[0].revision + 1);
    assert_eq!(
        restored
            .get::<Authority>(&fenced, "authority")
            .await
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    let reopened = Lifecycle::new(restored.catalog.clone(), restored.budget.clone());
    let restored_view = fence_registration(&reopened, &image.tables[0])
        .await
        .unwrap();
    assert_eq!(
        restored_view.table.metadata_location_result().unwrap(),
        fenced.table.metadata_location_result().unwrap()
    );
    check_target(&reopened, &image, &reference).await.unwrap();
    check_restore_reservation(&restored, &restored_view, &reference, &point)
        .await
        .unwrap();
    let mut mismatched_point = point.clone();
    let mut mismatched_reference = reference.clone();
    mismatched_point.id = Id::new("other-promise").unwrap();
    mismatched_reference.id = mismatched_point.id.clone();
    assert!(
        check_restore_reservation(
            &restored,
            &restored_view,
            &mismatched_reference,
            &mismatched_point
        )
        .await
        .is_err()
    );
    let mut malformed = reference.clone();
    malformed.manifest.path = malformed.manifest.path.replace(
        "restore_00000000000040008000000000000001",
        "nested/restore_00000000000040008000000000000001",
    );
    assert!(remote::validate(&malformed, &point).is_err());
    let done = restored
        .commit(
            &restored_view,
            remote_completion(&reference, &point).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        restore_reference(&restored, &done, &point_key(&reference.id))
            .await
            .unwrap(),
        Some(reference)
    );
    assert_eq!(restored.get::<i64>(&done, "value").await.unwrap(), Some(17));
    let protection = restored
        .get::<Protection>(&done, "protection/restore_00000000000040008000000000000001")
        .await
        .unwrap()
        .unwrap();
    assert!(protection.retired);
    assert!(source.load_table(&ident).await.is_err());
}

// The native catalog is local and the immutable object backend is the existing
// localhost S3 fixture. This proves the actual Engine pre-effect refusal, not
// hosted catalog detachment or provider deletion authority.
#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_pending_source_cannot_recreate_a_later_deleted_destination() {
    use crate::engine::locations::remote_tests::{fixture, saved};
    use crate::storage::remote::objects::tests::writes::{Behavior, WriteFixture};
    use std::sync::atomic::Ordering;

    let mut server = WriteFixture::for_path(Behavior::HoldPutThenDelete, "/bucket/owned/".into());
    server.release.take().unwrap().send(()).unwrap();
    let objects = server.objects();
    let (_root, engine, spec, _) = fixture(objects.clone()).await;
    assert_eq!(
        engine
            .run_until(&spec, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (attempt, phase) = saved(&engine, &spec).await;
    assert!(phase.is_none());
    let lifecycle = &engine.inner.lifecycle;
    let captured = lifecycle.open(&spec.dataset).await.unwrap();
    let registration = Registration {
        table: captured.identifier().to_string(),
        table_uuid: captured.table.metadata().uuid().to_string(),
        metadata: storage::object(
            &spec.dataset,
            Path::new(captured.table.metadata_location_result().unwrap()),
        )
        .unwrap(),
        revision: captured.revision,
        authority: Some(capture_authority(lifecycle, &captured).await.unwrap()),
    };
    // Advance beyond the point, then remove exactly the synthetic uploaded key.
    assert_eq!(
        engine
            .run_until(&spec, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let destination = attempt.inputs.get("input").unwrap();
    objects.file_io().delete(&destination.path).await.unwrap();
    lifecycle
        .catalog
        .drop_table(captured.identifier())
        .await
        .unwrap();
    lifecycle
        .catalog
        .register_table(captured.identifier(), registration.metadata.path.clone())
        .await
        .unwrap();

    // A lost fence response reconciles one exact authority-only native commit.
    lifecycle.lose_commit_response.store(true, Ordering::SeqCst);
    let fenced = fence_registration(lifecycle, &registration).await.unwrap();
    assert_eq!(fenced.revision, captured.revision + 1);
    let cold = Lifecycle::new(lifecycle.catalog.clone(), lifecycle.budget.clone());
    let replay = fence_registration(&cold, &registration).await.unwrap();
    assert_eq!(
        replay.table.metadata_location_result().unwrap(),
        fenced.table.metadata_location_result().unwrap()
    );
    assert_eq!(saved(&engine, &spec).await.0.inputs, attempt.inputs);
    assert!(
        std::path::Path::new(&spec.input).is_file(),
        "source stays available to expose any accidental recreation"
    );
    assert_eq!(
        engine.run(&spec).await.unwrap_err().code(),
        "stale_eligibility"
    );
    // A new operation is admitted at the new epoch with a distinct destination.
    let mut new = spec.clone();
    new.operation = Id::new("after-restore").unwrap();
    assert_eq!(
        engine
            .run_until(&new, "source-reservation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let (new_attempt, _) = saved(&engine, &new).await;
    assert_ne!(
        new_attempt.inputs.get("input").unwrap().path,
        destination.path
    );
    engine.shutdown().await.unwrap();
    let report = server.finish();
    assert_eq!(report.methods("PUT"), 1);
    assert_eq!(report.methods("GET"), 1);
    assert_eq!(report.methods("DELETE"), 1);
    assert_eq!(
        report.requests.last().unwrap().0,
        "DELETE",
        "old retry performs no object I/O after the observed deletion"
    );
    assert!(report.published.is_none());
}

#[tokio::test]
async fn portable_namespace_properties_preserve_location_custom_values_and_intent() {
    // Maintained catalog namespace behavior only; this is not hosted restore.
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let source = storage::local_catalog(source_root.path()).await.unwrap();
    let target = storage::local_catalog(target_root.path()).await.unwrap();
    let dataset = Id::new("portable-namespace").unwrap();
    let namespace = crate::catalog::lifecycle_ident(&dataset)
        .namespace()
        .clone();
    source
        .create_namespace(
            &namespace,
            std::collections::HashMap::from([
                ("namespace_id".into(), "source-registration".into()),
                ("location".into(), "s3://bucket/namespace-directory".into()),
                ("retained-custom".into(), "exact-value".into()),
            ]),
        )
        .await
        .unwrap();
    let captured = source.get_namespace(&namespace).await.unwrap();
    let image = CatalogImage {
        format: remote::FORMAT,
        generation: uuid::Uuid::new_v4(),
        namespaces: BTreeMap::from([(
            namespace.to_string(),
            captured
                .properties()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )]),
        tables: Vec::new(),
    };
    let reference = RestorePointRef {
        dataset: dataset.clone(),
        id: Id::new("portable").unwrap(),
        manifest: ObjectRef {
            owner: dataset.clone(),
            path: format!(
                "s3://bucket/owned/{dataset}/restore_00000000000040008000000000000001/point.json"
            ),
            bytes: 1,
            digest: "a".repeat(64),
        },
    };
    let generation = uuid::Uuid::new_v4();
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            image.generation,
            generation,
            false,
            true
        )
        .await
        .unwrap()
    );
    let created = target.get_namespace(&namespace).await.unwrap();
    assert!(!created.properties().contains_key("namespace_id"));
    let exact_intent = created.properties()["docspec.restore.intent"].clone();
    target
        .update_namespace(
            &namespace,
            std::collections::HashMap::from([
                ("namespace_id".into(), "target-registration".into()),
                ("location".into(), "s3://bucket/namespace-directory/".into()),
            ]),
        )
        .await
        .unwrap();
    let lifecycle = Lifecycle::new(target.clone(), Budget::new(Limits::default()).unwrap());
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            image.generation,
            generation,
            false,
            false
        )
        .await
        .unwrap()
    );
    check_target(&lifecycle, &image, &reference).await.unwrap();
    for (key, changed, original) in [
        (
            "location",
            "s3://bucket/another-directory/",
            "s3://bucket/namespace-directory/",
        ),
        ("retained-custom", "different-value", "exact-value"),
        (
            "docspec.restore.intent",
            "different-intent",
            exact_intent.as_str(),
        ),
    ] {
        target
            .update_namespace(
                &namespace,
                std::collections::HashMap::from([(key.into(), changed.into())]),
            )
            .await
            .unwrap();
        assert_eq!(
            restore_intent(
                &target,
                &image,
                &reference,
                image.generation,
                generation,
                false,
                false
            )
            .await
            .unwrap_err()
            .code(),
            "invalid_restore_point"
        );
        if key != "docspec.restore.intent" {
            assert_eq!(
                check_target(&lifecycle, &image, &reference)
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_restore_point"
            );
        }
        target
            .update_namespace(
                &namespace,
                std::collections::HashMap::from([(key.into(), original.into())]),
            )
            .await
            .unwrap();
    }
    assert!(
        restore_intent(
            &target,
            &image,
            &reference,
            image.generation,
            generation,
            false,
            false
        )
        .await
        .unwrap()
    );
    check_target(&lifecycle, &image, &reference).await.unwrap();
    assert!(target.list_tables(&namespace).await.unwrap().is_empty());
    assert_eq!(
        source.get_namespace(&namespace).await.unwrap().properties(),
        captured.properties()
    );
}
