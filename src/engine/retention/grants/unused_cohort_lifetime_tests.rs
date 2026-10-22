//! A captured cohort's unused member retains interpretation, not unused row values.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::array::{Array, StringArray};
use futures::TryStreamExt;

#[tokio::test]
async fn subset_coverage_reclaims_unused_member_after_standalone_promises_end() {
    let (root, engine, _) = fixture().await;
    let workspace = root.path().join("workspace");
    let input = root.path().join("cohort.parquet");
    let write_input = |path: &std::path::Path, values: Vec<i64>| {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    };
    write_input(&input, vec![1, 1, 2]);
    let family = id("family");
    let mut members = Vec::new();
    for dataset in ["comments", "dockets", "unused"] {
        members.push(
            engine
                .run(&RunSpec {
                    dataset: id(dataset),
                    operation: id("initial"),
                    base: None,
                    relationship: Some(SourceRelationship::Coupled {
                        family: family.clone(),
                        generation: id("g1"),
                    }),
                    input: input.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&input).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                    partition_by: Vec::new(),
                })
                .await
                .unwrap(),
        );
    }
    let seal = GenerationSeal {
        family: family.clone(),
        generation: id("g1"),
        members: members
            .iter()
            .map(|release| (release.dataset.dataset.clone(), release.version_ref()))
            .collect(),
    };
    engine.seal_generation(&seal).await.unwrap();
    let request = RefreshSpec {
        dataset: id("consumer"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: members[0].version_ref(),
            right: members[1].version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
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
        }),
    };
    let derived = engine.refresh(&request).await.unwrap();
    let coverage = &derived.dataset.related.as_ref().unwrap().coverage;
    assert_eq!(coverage.inputs.len(), 2);
    assert_eq!(coverage.generations[&family], seal);
    let payload = |release: &ReleasePin| {
        release.dataset.groups["source"]
            .objects
            .iter()
            .find(|object| object.path.ends_with(".parquet"))
            .unwrap()
            .clone()
    };
    let candidate = payload(&members[2]);
    let consumed = payload(&members[0]);
    let replacement = root.path().join("replacement.parquet");
    write_input(&replacement, vec![8]);
    let current_unused = engine
        .run(&RunSpec {
            dataset: members[2].dataset.dataset.clone(),
            operation: id("replacement"),
            base: Some(members[2].id.clone()),
            relationship: Some(SourceRelationship::Coupled {
                family: family.clone(),
                generation: id("g2"),
            }),
            input: replacement.to_string_lossy().into_owned(),
            expected_digest: storage::digest(&replacement).unwrap().0,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let seal_plan = engine
        .plan_generation_retirement(
            &family,
            &id("end-g1"),
            &id("g1"),
            "end standalone full cohort availability; admitted subset remains retained",
        )
        .await
        .unwrap();
    engine.retire_generation(&seal_plan).await.unwrap();
    end_member_promises(&engine, &members[2], "old-unused").await;
    // End only the newer unused member's recovery; keep its selected values.
    let records = engine
        .inspect(&current_unused.dataset.dataset)
        .await
        .unwrap();
    let keys = records
        .iter()
        .filter(|(key, value)| {
            key.starts_with("phase/") && value["operation"] == current_unused.id.as_str()
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(!keys.is_empty());
    let phases = engine
        .plan_phase_retirement(
            &current_unused.dataset.dataset,
            &id("new-unused-phases"),
            &keys,
            "end unused member replacement recovery",
        )
        .await
        .unwrap();
    engine.retire_phases(&phases).await.unwrap();
    let operation = engine
        .plan_operation_retirement(
            &current_unused.dataset.dataset,
            &id("new-unused-operation"),
            &current_unused.id,
            "end unused member replacement recovery",
        )
        .await
        .unwrap();
    engine.retire_operation(&operation).await.unwrap();
    // Admission deliberately captured the complete seal for operation recovery.
    // End that separate promise; selected result availability keeps exact inputs.
    super::coupled_build_lifetime_tests::end_current_recovery(
        &engine,
        &derived,
        "consumer-recovery",
    )
    .await;
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let records = engine.inspect(&candidate.owner).await.unwrap();
    let grants = records
        .iter()
        .filter(|(key, value)| {
            key.starts_with("owner-protection/")
                && value["objects"].as_array().is_some_and(|objects| {
                    objects
                        .iter()
                        .any(|object| object["path"] == candidate.path)
                })
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(!grants.is_empty());
    let grants_plan = engine
        .plan_grant_retirement(
            &candidate.owner,
            &id("end-standalone-grants"),
            &grants,
            "retired standalone seal owned unused member payload; subset only needs its metadata",
        )
        .await
        .unwrap();
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
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&consumed))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let removal = engine
        .plan_removal(
            &candidate.owner,
            &id("unused-payload"),
            std::slice::from_ref(&candidate),
            "reclaim one unused old cohort member payload",
        )
        .await
        .unwrap();
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.confirmed_unlinked_bytes, candidate.bytes);
    assert!(!std::path::Path::new(&candidate.path).exists());
    for metadata in [
        &members[2].manifest,
        &members[2].dataset.groups["source"].metadata,
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
    std::fs::remove_file(&input).unwrap();
    std::fs::remove_file(&replacement).unwrap();
    drop(engine);
    let cold = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(cold.refresh(&request).await.unwrap(), derived);
    assert_selected_source_values(&cold, &derived, &[1, 1, 1, 1, 2]).await;
    crate::engine::inputs::verify_input_coverage(coverage, limits())
        .await
        .unwrap();
    crate::engine::verify_release(&derived, limits())
        .await
        .unwrap();
    assert!(
        cold.inspect_retention(std::slice::from_ref(&consumed))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let files = [payload(&members[0]).path, payload(&members[1]).path];
    let mut evidence = cold.read_evidence(&derived).await.unwrap();
    let mut pairs = Vec::new();
    while let Some(batch) = evidence.try_next().await.unwrap() {
        let text = |name: &str| {
            arrow::compute::cast(batch.column_by_name(name).unwrap(), &DataType::Utf8).unwrap()
        };
        let left = text("__docspec_left_file");
        let right = text("__docspec_right_file");
        let left = left.as_any().downcast_ref::<StringArray>().unwrap();
        let right = right.as_any().downcast_ref::<StringArray>().unwrap();
        let rows = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        };
        let left_rows = rows("__docspec_left_row");
        let right_rows = rows("__docspec_right_row");
        for row in 0..batch.num_rows() {
            assert!(
                !left.is_null(row)
                    && !right.is_null(row)
                    && !left_rows.is_null(row)
                    && !right_rows.is_null(row)
            );
            pairs.push((
                left.value(row).to_owned(),
                left_rows.value(row),
                right.value(row).to_owned(),
                right_rows.value(row),
            ));
        }
    }
    pairs.sort();
    let expected = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 2)]
        .into_iter()
        .map(|(left, right)| (files[0].clone(), left, files[1].clone(), right))
        .collect::<Vec<_>>();
    assert_eq!(pairs, expected);
    assert_eq!(cold.remove_objects(&removal).await.unwrap(), removed);
}
