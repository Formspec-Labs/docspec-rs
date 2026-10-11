//! Coupled full reconstruction keeps generation interpretation without old payloads.
// Rust guideline compliant 2026-02-21

use super::*;

pub(super) async fn end_current_recovery(engine: &Engine, release: &ReleasePin, name: &str) {
    let dataset = &release.dataset.dataset;
    let records = engine.inspect(dataset).await.unwrap();
    let keys = records
        .iter()
        .filter(|(key, value)| {
            key.starts_with("phase/") && value["operation"] == release.id.as_str()
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(!keys.is_empty());
    let phases = engine
        .plan_phase_retirement(
            dataset,
            &id(&format!("{name}-phases")),
            &keys,
            "end full rebuild recovery while preserving selected availability",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let operation = engine
        .plan_operation_retirement(
            dataset,
            &id(&format!("{name}-operation")),
            &release.id,
            "end terminal rebuild recovery while preserving selected availability",
        )
        .await
        .unwrap();
    engine.retire_operation(&operation).await.unwrap();
}

#[tokio::test]
async fn coupled_full_build_reclaims_old_payload_after_exact_promises_end() {
    let (root, engine, _) = fixture().await;
    let workspace = root.path().join("workspace");
    let family = id("family");
    let input = root.path().join("input.parquet");
    let mut old = Vec::new();
    for dataset in ["comments", "dockets"] {
        old.push(
            engine
                .run(&RunSpec {
                    dataset: id(dataset),
                    operation: id("g1-source"),
                    base: None,
                    relationship: Some(SourceRelationship::Coupled {
                        family: family.clone(),
                        generation: id("g1"),
                    }),
                    input: input.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&input).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                })
                .await
                .unwrap(),
        );
    }
    let old_seal = GenerationSeal {
        family: family.clone(),
        generation: id("g1"),
        members: old
            .iter()
            .map(|release| (release.dataset.dataset.clone(), release.version_ref()))
            .collect(),
    };
    engine.seal_generation(&old_seal).await.unwrap();
    let candidate = old[0].dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    let replacement = root.path().join("replacement.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&replacement).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut current = Vec::new();
    for release in &old {
        current.push(
            engine
                .run(&RunSpec {
                    dataset: release.dataset.dataset.clone(),
                    operation: id("g2-source"),
                    base: Some(release.id.clone()),
                    relationship: Some(SourceRelationship::Coupled {
                        family: family.clone(),
                        generation: id("g2"),
                    }),
                    input: replacement.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&replacement).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                })
                .await
                .unwrap(),
        );
    }
    for (before, after) in old.iter().zip(&current) {
        assert_eq!(
            after.dataset.parent_manifest.as_ref(),
            Some(&before.manifest)
        );
        assert_eq!(
            after.dataset.relationship,
            SourceRelationship::Coupled {
                family: family.clone(),
                generation: id("g2")
            }
        );
    }
    let current_seal = GenerationSeal {
        family: family.clone(),
        generation: id("g2"),
        members: current
            .iter()
            .map(|release| (release.dataset.dataset.clone(), release.version_ref()))
            .collect(),
    };
    engine.seal_generation(&current_seal).await.unwrap();
    let records = engine.inspect(&family).await.unwrap();
    let binding: ReferenceBinding =
        serde_json::from_value(records[&binding_key("generation/g2")].clone()).unwrap();
    let owner_records = engine.inspect(&candidate.owner).await.unwrap();
    let current_grants = binding
        .protections
        .iter()
        .filter(|reference| reference.owner == candidate.owner)
        .map(|reference| {
            serde_json::from_value::<OwnerProtection>(owner_records[&reference.token].clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(!current_grants.is_empty());
    assert!(
        current_grants
            .iter()
            .all(|grant| !grant.objects.contains(&candidate)),
        "g2 owns history metadata, not the obsolete g1 payload"
    );
    assert!(
        current_grants
            .iter()
            .any(|grant| grant.objects.contains(&old[0].manifest))
    );
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let seal_plan = engine
        .plan_generation_retirement(
            &family,
            &id("end-g1"),
            &id("g1"),
            "end standalone old generation availability",
        )
        .await
        .unwrap();
    let seal_retirement = engine.retire_generation(&seal_plan).await.unwrap();
    assert_eq!(
        engine
            .capture_inputs(&old_seal.members)
            .await
            .unwrap_err()
            .code(),
        "generation_retired"
    );
    for (index, release) in old.iter().enumerate() {
        end_member_promises(&engine, release, &format!("old-{index}")).await;
        end_current_recovery(&engine, &current[index], &format!("current-{index}")).await;
    }
    // Exact old foreign grants remain a separate promise after the seal ends.
    let records = engine.inspect(&candidate.owner).await.unwrap();
    let grants = records
        .iter()
        .filter(|(key, value)| {
            key.starts_with("owner-protection/")
                && value["consumer"] == family.as_str()
                && value["objects"].as_array().is_some_and(|objects| {
                    objects
                        .iter()
                        .any(|object| object["path"] == candidate.path)
                })
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(!grants.is_empty());
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let grants_plan = engine.plan_grant_retirement(&candidate.owner, &id("end-g1-grants"), &grants,
        "old seal is retired; current seal protects only exact current values and history metadata").await.unwrap();
    engine.retire_grants(&grants_plan).await.unwrap();
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_none()
    );
    let removal = engine
        .plan_removal(
            &candidate.owner,
            &id("obsolete-member"),
            std::slice::from_ref(&candidate),
            "reclaim one exact obsolete member payload after all its promises end",
        )
        .await
        .unwrap();
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.confirmed_unlinked_bytes, candidate.bytes);
    assert!(!std::path::Path::new(&candidate.path).exists());
    for release in &old {
        for metadata in [
            &release.manifest,
            &release.dataset.groups["source"].metadata,
        ] {
            assert!(std::path::Path::new(&metadata.path).exists());
            assert!(
                engine
                    .inspect_retention(std::slice::from_ref(metadata))
                    .await
                    .unwrap()
                    .objects[0]
                    .blocker
                    .is_some()
            );
        }
    }
    std::fs::remove_file(&replacement).unwrap();
    drop(engine);
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(
        engine.retire_generation(&seal_plan).await.unwrap(),
        seal_retirement
    );
    assert_eq!(engine.remove_objects(&removal).await.unwrap(), removed);
    assert_eq!(
        engine.seal_generation(&current_seal).await.unwrap(),
        current_seal
    );
    let coverage = engine.capture_inputs(&current_seal.members).await.unwrap();
    assert_eq!(coverage.generations[&family], current_seal);
    crate::engine::inputs::verify_input_coverage(&coverage, limits())
        .await
        .unwrap();
    assert_eq!(
        engine
            .capture_inputs(&old_seal.members)
            .await
            .unwrap_err()
            .code(),
        "availability_retired"
    );
    assert_eq!(
        engine.inspect(&family).await.unwrap()["generation/g1"],
        serde_json::to_value(&old_seal).unwrap()
    );
    for release in &current {
        assert_selected_source_values(&engine, release, &[3, 4]).await;
        crate::engine::verify_release(release, limits())
            .await
            .unwrap();
        let OperationRequest::Build(request) = &release.dataset.request else {
            panic!("full Build fixture");
        };
        assert_eq!(engine.run(request).await.unwrap(), *release);
    }
}
