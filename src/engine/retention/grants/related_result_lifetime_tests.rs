//! Full Refresh result availability owns current contributors, not old execution grants.
// Rust guideline compliant 2026-02-21

use super::coupled_build_lifetime_tests::end_current_recovery;
use super::*;
use arrow::array::{Array, StringArray};
use futures::TryStreamExt;

fn refresh(
    left: &ReleasePin,
    right: &ReleasePin,
    base: Option<Id>,
    operation: &str,
) -> RefreshSpec {
    RefreshSpec {
        dataset: id("consumer"),
        operation: id(operation),
        base,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
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
            },
        }),
    }
}

async fn contributor_pairs(
    engine: &Engine,
    release: &ReleasePin,
) -> Vec<(String, i64, String, i64)> {
    let mut stream = engine.read_evidence(release).await.unwrap();
    let mut pairs = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let strings = |name: &str| {
            arrow::compute::cast(batch.column_by_name(name).unwrap(), &DataType::Utf8).unwrap()
        };
        let left = strings("__docspec_left_file");
        let right = strings("__docspec_right_file");
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
            assert!(!left.is_null(row) && !right.is_null(row));
            assert!(!left_rows.is_null(row) && !right_rows.is_null(row));
            pairs.push((
                left.value(row).into(),
                left_rows.value(row),
                right.value(row).into(),
                right_rows.value(row),
            ));
        }
    }
    pairs.sort();
    pairs
}

#[tokio::test]
async fn full_refresh_reclaims_old_exclusive_input_after_execution_promises_end() {
    let (root, engine, _) = fixture().await;
    let workspace = root.path().join("workspace");
    let input = root.path().join("input.parquet");
    let mut old = Vec::new();
    for dataset in ["comments", "dockets"] {
        old.push(
            engine
                .run(&RunSpec {
                    dataset: id(dataset),
                    operation: id("initial"),
                    base: None,
                    relationship: None,
                    input: input.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&input).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                })
                .await
                .unwrap(),
        );
    }
    let old_consumer = engine
        .refresh(&refresh(&old[0], &old[1], None, "initial"))
        .await
        .unwrap();
    let candidate = old[0].dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone();
    let replacement = root.path().join("replacement.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![3, 3, 4]))],
    )
    .unwrap();
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
                    operation: id("replacement"),
                    base: Some(release.id.clone()),
                    relationship: None,
                    input: replacement.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&replacement).unwrap().0,
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                })
                .await
                .unwrap(),
        );
    }
    let request = refresh(
        &current[0],
        &current[1],
        Some(old_consumer.id.clone()),
        "replacement",
    );
    let derived = engine.refresh(&request).await.unwrap();
    assert_eq!(
        derived.dataset.parent_manifest.as_ref(),
        Some(&old_consumer.manifest)
    );
    assert_selected_source_values(&engine, &derived, &[3, 3, 3, 3, 4]).await;
    let files = current
        .iter()
        .map(|release| {
            let files = release.dataset.groups["source"]
                .objects
                .iter()
                .filter(|object| object.path.ends_with(".parquet"))
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 1);
            files[0].path.clone()
        })
        .collect::<Vec<_>>();
    let expected_pairs = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 2)]
        .into_iter()
        .map(|(left, right)| (files[0].clone(), left, files[1].clone(), right))
        .collect::<Vec<_>>();
    assert_eq!(contributor_pairs(&engine, &derived).await, expected_pairs);
    let consumer_records = engine.inspect(&derived.dataset.dataset).await.unwrap();
    let binding: ReferenceBinding =
        serde_json::from_value(consumer_records[&binding_key("readback/replacement")].clone())
            .unwrap();
    let records = engine.inspect(&candidate.owner).await.unwrap();
    let current_grants = binding
        .protections
        .iter()
        .filter(|reference| reference.owner == candidate.owner)
        .map(|reference| {
            serde_json::from_value::<OwnerProtection>(records[&reference.token].clone()).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(!current_grants.is_empty());
    assert!(
        current_grants
            .iter()
            .all(|grant| !grant.objects.contains(&candidate))
    );
    assert!(
        current_grants
            .iter()
            .any(|grant| grant.objects.contains(&old[0].manifest))
    );
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
    assert_eq!(
        engine
            .plan_grant_retirement(
                &candidate.owner,
                &id("too-early"),
                &grants,
                "execution still promises old captured inputs"
            )
            .await
            .unwrap_err()
            .code(),
        "grant_in_use"
    );
    for (index, release) in old.iter().enumerate() {
        end_member_promises(&engine, release, &format!("old-{index}")).await;
        end_current_recovery(&engine, &current[index], &format!("new-{index}")).await;
    }
    end_member_promises(&engine, &old_consumer, "old-consumer").await;
    end_current_recovery(&engine, &derived, "new-consumer").await;
    assert!(
        engine
            .inspect_retention(std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .objects[0]
            .blocker
            .is_some()
    );
    let grant_plan = engine.plan_grant_retirement(&candidate.owner, &id("end-old-grants"), &grants,
        "all old execution and availability promises ended; current result owns exact current inputs").await.unwrap();
    engine.retire_grants(&grant_plan).await.unwrap();
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
            &id("old-payload"),
            std::slice::from_ref(&candidate),
            "reclaim one exclusive old producer payload",
        )
        .await
        .unwrap();
    let removed = engine.remove_objects(&removal).await.unwrap();
    assert!(removed.complete);
    assert_eq!(removed.confirmed_unlinked_bytes, candidate.bytes);
    assert!(!std::path::Path::new(&candidate.path).exists());
    for metadata in [
        &old[0].manifest,
        &old[0].dataset.groups["source"].metadata,
        &old_consumer.manifest,
        &old_consumer
            .dataset
            .related
            .as_ref()
            .unwrap()
            .evidence
            .metadata,
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
    std::fs::remove_file(&replacement).unwrap();
    drop(engine);
    let cold = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(cold.refresh(&request).await.unwrap(), derived);
    assert_selected_source_values(&cold, &derived, &[3, 3, 3, 3, 4]).await;
    assert_eq!(contributor_pairs(&cold, &derived).await, expected_pairs);
    crate::engine::verify_release(&derived, limits())
        .await
        .unwrap();
    assert_eq!(cold.remove_objects(&removal).await.unwrap(), removed);
}
