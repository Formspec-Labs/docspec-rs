//! Owner-side native decisions protect real outputs before foreign references exist.

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

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

async fn fixture() -> (tempfile::TempDir, Engine) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    (root, engine)
}

fn request(root: &Path, dataset: &str) -> RunSpec {
    let path = root.join(format!("{dataset}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    RunSpec {
        dataset: id(dataset),
        operation: id("initial"),
        base: None,
        relationship: None,
        input: path.to_string_lossy().into_owned(),
        expected_digest: storage::digest(&path).unwrap().0,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
    }
}

fn payload(release: &ReleasePin) -> ObjectRef {
    release.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap()
        .clone()
}

#[tokio::test]
async fn owner_grant_retry_and_reopen_reuse_one_native_commit() {
    let (root, engine) = fixture().await;
    let release = engine.run(&request(root.path(), "comments")).await.unwrap();
    let owner = &release.dataset.dataset;
    let consumer = id("consumer");
    let before = engine.inner.lifecycle.open(owner).await.unwrap();
    let grant = engine
        .protect_releases(&consumer, std::slice::from_ref(&release))
        .await
        .unwrap();
    assert_eq!(grant.references.len(), 1);
    let reference = grant.references[0].clone();
    assert_eq!(reference.owner, *owner);
    assert_eq!(reference.revision, before.revision + 1);
    let retry = engine
        .protect_releases(&consumer, &[release.clone(), release.clone()])
        .await
        .unwrap();
    assert_eq!(retry.references, grant.references);
    assert_eq!(
        engine.inner.lifecycle.open(owner).await.unwrap().revision,
        reference.revision
    );
    let view = engine.inner.lifecycle.open(owner).await.unwrap();
    let retained: OwnerProtection = engine
        .inner
        .lifecycle
        .get(&view, &reference.token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.consumer, consumer);
    assert!(retained.objects.contains(&release.manifest));
    assert!(retained.objects.contains(&payload(&release)));
    drop(retry);
    drop(grant);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let retry = engine
        .protect_releases(&consumer, std::slice::from_ref(&release))
        .await
        .unwrap();
    assert_eq!(retry.references, vec![reference.clone()]);
    assert_eq!(
        engine.inner.lifecycle.open(owner).await.unwrap().revision,
        reference.revision
    );
}

#[tokio::test]
async fn lost_native_owner_reply_reconciles_without_another_commit() {
    let (root, engine) = fixture().await;
    let release = engine.run(&request(root.path(), "comments")).await.unwrap();
    let owner = &release.dataset.dataset;
    let before = engine.inner.lifecycle.open(owner).await.unwrap();
    engine
        .inner
        .lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let protected = engine
        .protect_releases(&id("consumer"), std::slice::from_ref(&release))
        .await
        .unwrap();
    assert!(
        !engine
            .inner
            .lifecycle
            .lose_commit_response
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(protected.references[0].revision, before.revision + 1);
    let after = engine.inner.lifecycle.open(owner).await.unwrap();
    assert_eq!(after.revision, protected.references[0].revision);
    let records = engine.inner.lifecycle.records(&after).await.unwrap();
    assert_eq!(
        records
            .keys()
            .filter(|key| key.starts_with("owner-protection/"))
            .count(),
        1
    );
    let retry = engine
        .protect_releases(&id("consumer"), std::slice::from_ref(&release))
        .await
        .unwrap();
    assert_eq!(retry.references, protected.references);
    assert_eq!(
        engine.inner.lifecycle.open(owner).await.unwrap().revision,
        after.revision
    );
}

#[tokio::test]
async fn nested_release_grants_follow_each_actual_owner_without_rehashing_payloads() {
    let (root, engine) = fixture().await;
    let left = engine.run(&request(root.path(), "comments")).await.unwrap();
    let right = engine.run(&request(root.path(), "dockets")).await.unwrap();
    let joined = engine
        .refresh(&RefreshSpec {
            dataset: id("joined"),
            operation: id("initial"),
            base: None,
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
        })
        .await
        .unwrap();
    let expected = engine
        .reference_objects(std::slice::from_ref(&joined))
        .await
        .unwrap();
    assert_eq!(
        expected
            .objects
            .iter()
            .map(|object| object.owner.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([id("comments"), id("dockets"), id("joined")])
    );
    // Creation already established the immutable identities. Damage confined to
    // this fixture proves owner acquisition does not silently add payload hashing.
    let damaged = payload(&left);
    let mut bytes = std::fs::read(&damaged.path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&damaged.path, bytes).unwrap();
    let consumer = id("search");
    let protected = engine.protect_releases(&consumer, &[joined]).await.unwrap();
    assert_eq!(
        protected
            .references
            .iter()
            .map(|reference| reference.owner.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([id("comments"), id("dockets"), id("joined")])
    );
    let mut actual = BTreeMap::new();
    for reference in &protected.references {
        let view = engine.inner.lifecycle.open(&reference.owner).await.unwrap();
        let grant: OwnerProtection = engine
            .inner
            .lifecycle
            .get(&view, &reference.token)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(grant.owner, reference.owner);
        assert_eq!(grant.consumer, consumer);
        for object in grant.objects {
            assert_eq!(object.owner, reference.owner);
            assert!(actual.insert(object.path.clone(), object).is_none());
        }
    }
    assert_eq!(
        actual,
        expected
            .objects
            .iter()
            .map(|object| (object.path.clone(), object.clone()))
            .collect()
    );
    assert_eq!(actual[&damaged.path], damaged);
}

#[tokio::test]
async fn a_reserved_path_without_completed_identity_cannot_grant_a_reference() {
    let (root, engine) = fixture().await;
    let release = engine.run(&request(root.path(), "comments")).await.unwrap();
    let completed = payload(&release);
    let path = Path::new(&completed.path)
        .parent()
        .unwrap()
        .join("provisional.parquet");
    let bytes = b"an interrupted writer has not acknowledged this file";
    std::fs::write(&path, bytes).unwrap();
    let provisional = ObjectRef {
        owner: completed.owner.clone(),
        path: path.to_string_lossy().into_owned(),
        digest: blake3::hash(bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    };
    let before = engine.inner.lifecycle.open(&completed.owner).await.unwrap();
    assert_eq!(
        engine
            .acquire_owner(&completed.owner, &id("consumer"), vec![provisional])
            .await
            .unwrap_err()
            .code(),
        "unqualified_owner_reference"
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&completed.owner)
            .await
            .unwrap()
            .revision,
        before.revision
    );
    engine
        .acquire_owner(&completed.owner, &id("consumer"), vec![completed.clone()])
        .await
        .unwrap();
}

#[tokio::test]
async fn forged_object_identities_cannot_acquire_owner_protection() {
    let (root, engine) = fixture().await;
    let release = engine.run(&request(root.path(), "comments")).await.unwrap();
    let original = payload(&release);
    let consumer = id("consumer");
    let before = engine.inner.lifecycle.open(&original.owner).await.unwrap();
    for change in ["digest", "size", "owner"] {
        let mut forged = original.clone();
        match change {
            "digest" => forged.digest = "0".repeat(64),
            "size" => forged.bytes += 1,
            "owner" => forged.owner = id("different"),
            _ => unreachable!(),
        }
        assert_eq!(
            engine
                .acquire_owner(&original.owner, &consumer, vec![forged])
                .await
                .unwrap_err()
                .code(),
            "invalid_owner_protection"
        );
    }
    let mut conflicting = release.clone();
    conflicting
        .dataset
        .groups
        .get_mut("source")
        .unwrap()
        .objects
        .iter_mut()
        .find(|object| object.path == original.path)
        .unwrap()
        .digest = "0".repeat(64);
    let error = engine
        .protect_releases(&consumer, &[release, conflicting])
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), "retention_inventory");
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&original.owner)
            .await
            .unwrap()
            .revision,
        before.revision
    );
}

#[tokio::test]
async fn owner_availability_and_grants_conflict_at_the_native_boundary() {
    let (root, engine) = fixture().await;
    let release = engine.run(&request(root.path(), "comments")).await.unwrap();
    let object = payload(&release);
    let owner = &object.owner;
    let consumer = id("consumer");
    let lifecycle = &engine.inner.lifecycle;
    let stale = lifecycle.open(owner).await.unwrap();
    let reference = engine
        .acquire_owner(owner, &consumer, vec![object.clone()])
        .await
        .unwrap();
    // These direct native records exercise the future removal reservation's
    // authority race. No removal service or physical deletion is implied.
    let unavailable = UnavailableObject {
        object: object.clone(),
        intent: id("retire-object"),
        revision: stale.revision + 1,
    };
    let refused = lifecycle
        .commit(
            &stale,
            vec![record(unavailable_key(&object.path), unavailable).unwrap()],
        )
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("stale_authority"));
    assert_eq!(
        lifecycle.open(owner).await.unwrap().revision,
        reference.revision
    );

    let other = release.manifest.clone();
    let stale = lifecycle.open(owner).await.unwrap();
    let rows = lifecycle.rows(&stale).await.unwrap();
    available(&rows, &stale, &other).unwrap();
    let grant = OwnerProtection {
        owner: owner.clone(),
        consumer: consumer.clone(),
        table_uuid: stale.table.metadata().uuid().to_string(),
        revision: stale.revision + 1,
        objects: vec![other.clone()],
    };
    let key = protection_key(&grant).unwrap();
    let unavailable = UnavailableObject {
        object: other.clone(),
        intent: id("retire-manifest"),
        revision: stale.revision + 1,
    };
    let reserved = lifecycle
        .commit(
            &stale,
            vec![record(unavailable_key(&other.path), unavailable).unwrap()],
        )
        .await
        .unwrap();
    let refused = lifecycle
        .commit(&stale, vec![record(&key, grant).unwrap()])
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("stale_authority"));
    assert_eq!(
        engine
            .acquire_owner(owner, &consumer, vec![other])
            .await
            .unwrap_err()
            .code(),
        "object_unavailable"
    );
    let after = lifecycle.open(owner).await.unwrap();
    assert_eq!(after.revision, reserved.revision);
    assert!(
        lifecycle
            .get::<OwnerProtection>(&after, &key)
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn shared_owner_grant_survives_retirement_of_its_creation_phase() {
    let (root, engine) = fixture().await;
    let request = request(root.path(), "comments");
    assert_eq!(
        engine
            .run_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&request.dataset).await.unwrap();
    let (key, value) = records
        .iter()
        .find(|(key, _)| key.starts_with("phase/") && key.ends_with("/processing"))
        .unwrap();
    let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
    let candidate = phase
        .table
        .unwrap()
        .objects
        .into_iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let consumer = id("consumer");
    let reference = engine
        .acquire_owner(&request.dataset, &consumer, vec![candidate.clone()])
        .await
        .unwrap();
    let plan = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("retire-processing"),
            std::slice::from_ref(key),
            "owner grant keeps the shared object",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let inspected = engine
        .inspect_retention(std::slice::from_ref(&candidate))
        .await
        .unwrap();
    assert_eq!(
        inspected.objects[0].blocker.as_ref().unwrap().key,
        reference.token
    );
    let revision = engine
        .inner
        .lifecycle
        .open(&request.dataset)
        .await
        .unwrap()
        .revision;
    let retry = engine
        .acquire_owner(&request.dataset, &consumer, vec![candidate.clone()])
        .await
        .unwrap();
    assert_eq!(retry, reference);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&request.dataset)
            .await
            .unwrap()
            .revision,
        revision
    );
    assert_eq!(
        std::fs::metadata(&candidate.path).unwrap().len(),
        candidate.bytes
    );
}

async fn coupled_sources(engine: &Engine, root: &Path) -> (GenerationSeal, Vec<ReleasePin>) {
    let family = id("family");
    let generation = id("generation");
    let mut releases = Vec::new();
    for dataset in ["comments", "dockets"] {
        let mut request = request(root, dataset);
        request.relationship = Some(SourceRelationship::Coupled {
            family: family.clone(),
            generation: generation.clone(),
        });
        releases.push(engine.run(&request).await.unwrap());
    }
    let seal = GenerationSeal {
        family,
        generation,
        members: releases
            .iter()
            .map(|release| (release.dataset.dataset.clone(), release.version_ref()))
            .collect(),
    };
    (seal, releases)
}

#[tokio::test]
async fn sealing_binds_exact_owner_grants_in_the_same_native_consumer_commit() {
    let (root, engine) = fixture().await;
    let (seal, releases) = coupled_sources(&engine, root.path()).await;
    let mut owner_revisions = BTreeMap::new();
    for owner in seal.members.keys() {
        owner_revisions.insert(
            owner.clone(),
            engine.inner.lifecycle.open(owner).await.unwrap().revision,
        );
    }
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let key = format!("generation/{}", seal.generation);
    let view = engine.inner.lifecycle.open(&seal.family).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (claim_revision, _, recorded) = rows.get_record::<GenerationSeal>(&key).unwrap().unwrap();
    let (binding_revision, _, binding) = rows
        .get_record::<ReferenceBinding>(&binding_key(&key))
        .unwrap()
        .unwrap();
    assert_eq!(recorded, seal);
    assert_eq!(claim_revision, binding_revision);
    assert_eq!(binding.revision, claim_revision);
    assert_eq!(rows.revision("family-members"), Some(claim_revision));
    assert_eq!(binding.consumer, seal.family);
    assert_eq!(binding.key, key);
    assert_eq!(binding.protections.len(), releases.len());
    let candidates = releases
        .iter()
        .map(|release| release.manifest.clone())
        .collect::<Vec<_>>();
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert!(
        inspection
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    let references = engine.reference_objects(&releases).await.unwrap();
    for reference in &binding.protections {
        let owner_view = engine.inner.lifecycle.open(&reference.owner).await.unwrap();
        let owner_rows = engine.inner.lifecycle.rows(&owner_view).await.unwrap();
        let (native_revision, _, grant) = owner_rows
            .get_record::<OwnerProtection>(&reference.token)
            .unwrap()
            .unwrap();
        assert_eq!(native_revision, reference.revision);
        assert_eq!(native_revision, owner_revisions[&reference.owner] + 1);
        assert_eq!(grant.consumer, seal.family);
        assert_eq!(grant.table_uuid, reference.table_uuid);
        let expected = references
            .objects
            .iter()
            .filter(|object| object.owner == reference.owner)
            .map(|object| (object.path.clone(), object.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            grant
                .objects
                .iter()
                .map(|object| (object.path.clone(), object.clone()))
                .collect::<BTreeMap<_, _>>(),
            expected
        );
        owner_revisions.insert(reference.owner.clone(), native_revision);
    }
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&seal.family)
            .await
            .unwrap()
            .revision,
        claim_revision
    );
    for (owner, revision) in &owner_revisions {
        assert_eq!(
            engine.inner.lifecycle.open(owner).await.unwrap().revision,
            *revision
        );
    }
    drop(references);
    drop(rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let view = engine.inner.lifecycle.open(&seal.family).await.unwrap();
    assert_eq!(view.revision, claim_revision);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .get::<ReferenceBinding>(&view, &binding_key(&key))
            .await
            .unwrap()
            .unwrap(),
        binding
    );
    for (owner, revision) in owner_revisions {
        assert_eq!(
            engine.inner.lifecycle.open(&owner).await.unwrap().revision,
            revision
        );
    }
}

#[tokio::test]
async fn retained_seal_refuses_forged_binding_or_missing_owner_token() {
    let (root, engine) = fixture().await;
    let (seal, releases) = coupled_sources(&engine, root.path()).await;
    engine.seal_generation(&seal).await.unwrap();
    let key = format!("generation/{}", seal.generation);
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&seal.family).await.unwrap();
    let good: ReferenceBinding = lifecycle
        .get(&view, &binding_key(&key))
        .await
        .unwrap()
        .unwrap();
    for field in ["digest", "owner-token", "no-protections", "member-manifest"] {
        let view = lifecycle.open(&seal.family).await.unwrap();
        let mut forged = good.clone();
        forged.revision = view.revision + 1;
        match field {
            "digest" => forged.digest = "0".repeat(64),
            "owner-token" => {
                forged.protections[0].token = format!("owner-protection/{}", "0".repeat(64));
            }
            "no-protections" => forged.protections.clear(),
            "member-manifest" => {
                // This is a real, valid grant for a smaller object set. It must
                // not stand in for protection of the sealed member's manifest.
                forged.protections[0] = engine
                    .acquire_owner(
                        &releases[0].dataset.dataset,
                        &seal.family,
                        vec![payload(&releases[0])],
                    )
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        // Rewrite claim and binding together so a stale revision cannot mask
        // failures to check the claim digest or referenced owner grant.
        let committed = lifecycle
            .commit(
                &view,
                vec![
                    record(&key, &seal).unwrap(),
                    record(binding_key(&key), forged).unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            engine.seal_generation(&seal).await.unwrap_err().code(),
            "invalid_owner_protection"
        );
        let error = engine.inspect_retention(&[]).await.unwrap_err();
        assert_eq!(
            error.code(),
            if field == "owner-token" {
                "retention_inventory"
            } else {
                "invalid_owner_protection"
            }
        );
        assert_eq!(
            lifecycle.open(&seal.family).await.unwrap().revision,
            committed.revision
        );
    }
}

#[tokio::test]
async fn partial_owner_handoff_refuses_sealing_and_reuses_prior_protection_on_retry() {
    let (root, engine) = fixture().await;
    let (seal, releases) = coupled_sources(&engine, root.path()).await;
    let first = &releases[0].dataset.dataset;
    let unavailable = &releases[1].manifest;
    let lifecycle = &engine.inner.lifecycle;
    let first_revision = lifecycle.open(first).await.unwrap().revision;
    let last_view = lifecycle.open(&unavailable.owner).await.unwrap();
    let last_view = lifecycle
        .commit(
            &last_view,
            vec![
                record(
                    unavailable_key(&unavailable.path),
                    UnavailableObject {
                        object: unavailable.clone(),
                        intent: id("future-removal-reservation"),
                        revision: last_view.revision + 1,
                    },
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    // This fixture exercises owner authority only; it performs no deletion.
    for _ in 0..2 {
        assert_eq!(
            engine.seal_generation(&seal).await.unwrap_err().code(),
            "object_unavailable"
        );
        let first_view = lifecycle.open(first).await.unwrap();
        assert_eq!(first_view.revision, first_revision + 1);
        let first_rows = lifecycle.rows(&first_view).await.unwrap();
        let mut grants = first_rows.prefix("owner-protection/");
        let (_, revision, json) = grants.next().unwrap();
        assert_eq!(revision, first_revision + 1);
        let grant: OwnerProtection = serde_json::from_str(json).unwrap();
        assert_eq!(grant.consumer, seal.family);
        assert!(grants.next().is_none());
        assert_eq!(
            lifecycle.open(&unavailable.owner).await.unwrap().revision,
            last_view.revision
        );
        let family_view = lifecycle.open(&seal.family).await.unwrap();
        let family_rows = lifecycle.rows(&family_view).await.unwrap();
        assert!(!family_rows.contains_key(&format!("generation/{}", seal.generation)));
        assert!(
            !family_rows.contains_key(&binding_key(&format!("generation/{}", seal.generation)))
        );
        assert!(!family_rows.contains_key("family-members"));
    }
}

#[tokio::test]
async fn failed_consumer_handoff_keeps_grants_for_a_successful_retry() {
    let (root, engine) = fixture().await;
    let (seal, releases) = coupled_sources(&engine, root.path()).await;
    let unavailable_manifest = &releases[1].manifest;
    let temporary = root.path().join("temporarily-unavailable-manifest.json");
    std::fs::rename(&unavailable_manifest.path, &temporary).unwrap();
    assert_eq!(
        engine.seal_generation(&seal).await.unwrap_err().code(),
        "io"
    );
    let lifecycle = &engine.inner.lifecycle;
    let key = format!("generation/{}", seal.generation);
    let family = lifecycle.open(&seal.family).await.unwrap();
    assert!(
        lifecycle
            .get::<GenerationSeal>(&family, &key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        lifecycle
            .get::<ReferenceBinding>(&family, &binding_key(&key))
            .await
            .unwrap()
            .is_none()
    );
    let mut retained = BTreeMap::new();
    for owner in seal.members.keys() {
        let view = lifecycle.open(owner).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        let mut grants = rows.prefix("owner-protection/");
        let (key, revision, json) = grants.next().unwrap();
        assert!(grants.next().is_none());
        let grant: OwnerProtection = serde_json::from_str(json).unwrap();
        assert_eq!(grant.consumer, seal.family);
        retained.insert(
            owner.clone(),
            (view.revision, key.to_owned(), revision, grant),
        );
    }
    std::fs::rename(temporary, &unavailable_manifest.path).unwrap();
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let family = lifecycle.open(&seal.family).await.unwrap();
    let rows = lifecycle.rows(&family).await.unwrap();
    let (claim_revision, _, _) = rows.get_record::<GenerationSeal>(&key).unwrap().unwrap();
    let (binding_revision, _, binding) = rows
        .get_record::<ReferenceBinding>(&binding_key(&key))
        .unwrap()
        .unwrap();
    assert_eq!(claim_revision, binding_revision);
    for reference in binding.protections {
        let (owner_revision, token, revision, grant) = &retained[&reference.owner];
        assert_eq!(&reference.token, token);
        assert_eq!(&reference.revision, revision);
        let view = lifecycle.open(&reference.owner).await.unwrap();
        assert_eq!(view.revision, *owner_revision);
        assert_eq!(
            lifecycle
                .get::<OwnerProtection>(&view, token)
                .await
                .unwrap()
                .unwrap(),
            *grant
        );
    }
}

#[tokio::test]
async fn lost_consumer_seal_reply_reuses_the_committed_binding_and_owner_grants() {
    let (root, engine) = fixture().await;
    let (seal, releases) = coupled_sources(&engine, root.path()).await;
    let protected = engine
        .protect_releases(&seal.family, &releases)
        .await
        .unwrap();
    let owner_references = protected.references.clone();
    drop(protected);
    let lifecycle = &engine.inner.lifecycle;
    let before = lifecycle.open(&seal.family).await.unwrap();
    lifecycle
        .lose_commit_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        engine.seal_generation(&seal).await.unwrap_err().code(),
        "lost_commit_response"
    );
    assert!(
        !lifecycle
            .lose_commit_response
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    let key = format!("generation/{}", seal.generation);
    let committed = lifecycle.open(&seal.family).await.unwrap();
    assert_eq!(committed.revision, before.revision + 1);
    let rows = lifecycle.rows(&committed).await.unwrap();
    let (claim_revision, _, claim) = rows.get_record::<GenerationSeal>(&key).unwrap().unwrap();
    let (binding_revision, _, binding) = rows
        .get_record::<ReferenceBinding>(&binding_key(&key))
        .unwrap()
        .unwrap();
    assert_eq!(claim, seal);
    assert_eq!(claim_revision, committed.revision);
    assert_eq!(claim_revision, binding_revision);
    assert_eq!(binding.protections, owner_references);
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    assert_eq!(
        lifecycle.open(&seal.family).await.unwrap().revision,
        committed.revision
    );
    for reference in &owner_references {
        assert_eq!(
            lifecycle.open(&reference.owner).await.unwrap().revision,
            reference.revision
        );
    }
    drop(rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&seal.family)
            .await
            .unwrap()
            .revision,
        committed.revision
    );
    for reference in &owner_references {
        assert_eq!(
            engine
                .inner
                .lifecycle
                .open(&reference.owner)
                .await
                .unwrap()
                .revision,
            reference.revision
        );
    }
}

#[tokio::test]
async fn a_family_that_owns_a_member_needs_no_foreign_grant_for_its_own_objects() {
    let (root, engine) = fixture().await;
    let family = id("comments");
    let generation = id("generation");
    let mut members = BTreeMap::new();
    for dataset in ["comments", "dockets"] {
        let mut request = request(root.path(), dataset);
        request.relationship = Some(SourceRelationship::Coupled {
            family: family.clone(),
            generation: generation.clone(),
        });
        let release = engine.run(&request).await.unwrap();
        members.insert(release.dataset.dataset.clone(), release.version_ref());
    }
    let seal = GenerationSeal {
        family,
        generation,
        members,
    };
    let lifecycle = &engine.inner.lifecycle;
    let before = lifecycle.open(&seal.family).await.unwrap().revision;
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let key = format!("generation/{}", seal.generation);
    let view = lifecycle.open(&seal.family).await.unwrap();
    assert_eq!(view.revision, before + 1);
    let rows = lifecycle.rows(&view).await.unwrap();
    let (_, _, binding) = rows
        .get_record::<ReferenceBinding>(&binding_key(&key))
        .unwrap()
        .unwrap();
    assert_eq!(binding.protections.len(), 1);
    assert_eq!(binding.protections[0].owner, id("dockets"));
    assert!(rows.prefix("owner-protection/").next().is_none());
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    assert_eq!(
        lifecycle.open(&seal.family).await.unwrap().revision,
        view.revision
    );
    let foreign = &binding.protections[0];
    assert_eq!(
        lifecycle.open(&foreign.owner).await.unwrap().revision,
        foreign.revision
    );
    drop(rows);
    drop(engine);

    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let reopened = engine.inner.lifecycle.open(&seal.family).await.unwrap();
    assert_eq!(reopened.revision, view.revision);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .get::<ReferenceBinding>(&reopened, &binding_key(&key))
            .await
            .unwrap()
            .unwrap(),
        binding
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&foreign.owner)
            .await
            .unwrap()
            .revision,
        foreign.revision
    );
}

#[tokio::test]
async fn validation_only_origins_qualify_original_creations_after_cache_replacement() {
    let (root, engine) = fixture().await;
    let base = engine.run(&request(root.path(), "comments")).await.unwrap();
    let original = BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("original-scalar"),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id("next"),
        group_definition: "next-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "next_id".into(),
            expression: "id + 1".into(),
        }],
    };
    assert_eq!(
        engine
            .backfill_until(&original, "validation")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before = engine.inspect(&original.dataset).await.unwrap();
    let (validation_key, validation_json) = before
        .iter()
        .find(|(key, _)| key.starts_with("backfill-validation/"))
        .unwrap();
    let validation: ValidationCompletion = serde_json::from_value(validation_json.clone()).unwrap();
    let origins_ref = validation.origins.as_ref().unwrap();
    let originals: ScalarOrigins =
        serde_json::from_slice(&std::fs::read(&origins_ref.path).unwrap()).unwrap();
    let file = &originals.files[0].completion;
    let mut creations = vec![
        file.execution.as_ref().unwrap().declaration.clone(),
        file.descriptor.clone(),
        file.outputs[0].clone(),
    ];
    creations.sort_by(|left, right| left.path.cmp(&right.path));
    let replaced_keys = before
        .keys()
        .filter(|key| key.starts_with("phase/backfill/") || key.starts_with("backfill-prepared/"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        replaced_keys
            .iter()
            .any(|key| key.starts_with("phase/backfill/"))
    );
    assert!(
        replaced_keys
            .iter()
            .any(|key| key.starts_with("backfill-prepared/"))
    );
    let retirement = engine
        .plan_phase_retirement(
            &original.dataset,
            &id("retire-original-cache"),
            &replaced_keys,
            "validation independently retains the original executions",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    let mut replacement = original.clone();
    replacement.operation = id("replacement-scalar");
    // Stop before the old validation refuses this newly prepared association.
    assert_eq!(
        engine
            .backfill_until(&replacement, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let after = engine.inspect(&original.dataset).await.unwrap();
    assert_eq!(&after[validation_key], validation_json);
    for key in &replaced_keys {
        assert_ne!(after[key], before[key]);
    }
    assert!(
        !after
            .keys()
            .any(|key| key.starts_with("backfill-readback/"))
    );
    for (key, json) in &after {
        if key.starts_with("result/") || key.starts_with("readback/") {
            let release: ReleasePin = serde_json::from_value(json.clone()).unwrap();
            assert!(
                release.dataset.computed.is_empty(),
                "a scalar release would mask the validation-only anchor"
            );
        }
    }
    let view = engine
        .inner
        .lifecycle
        .open(&original.dataset)
        .await
        .unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let mut directly_created = BTreeSet::new();
    visit_creation(
        &rows,
        &view,
        engine.inner.lifecycle.budget.limits.max_metadata_bytes,
        |object| {
            directly_created.insert(object.path.clone());
            Ok(())
        },
    )
    .await
    .unwrap();
    for object in &creations {
        assert!(
            !directly_created.contains(&object.path),
            "replacement phases must not mask original creation lookup"
        );
        assert_eq!(
            engine
                .qualified_object(&original.dataset, &object.path, &object.digest)
                .await
                .unwrap(),
            *object
        );
    }
    let grant = engine
        .acquire_owner(&original.dataset, &id("fresh-consumer"), creations.clone())
        .await
        .unwrap();
    let view = engine
        .inner
        .lifecycle
        .open(&original.dataset)
        .await
        .unwrap();
    let retained: OwnerProtection = engine
        .inner
        .lifecycle
        .get(&view, &grant.token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.objects, creations);
}
