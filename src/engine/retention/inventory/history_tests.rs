//! Ordering receipts retain catalog identities without promising endpoint payload availability.
// Rust guideline compliant 2026-02-21

use super::super::owners::{
    OwnerProtection, OwnerProtectionRef, ReferenceBinding, binding_key, protection_key,
};
use super::*;
use crate::catalog::SnapshotRows;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        ..Limits::default()
    }
}

fn object(root: &Path, owner: &str, name: &str) -> ObjectRef {
    ObjectRef {
        owner: id(owner),
        path: root
            .join(format!("{owner}-{name}"))
            .to_string_lossy()
            .into_owned(),
        digest: "0".repeat(64),
        bytes: 17,
    }
}

fn release(root: &Path, owner: &str, version: &str) -> ReleasePin {
    let metadata = object(root, owner, &format!("{version}.metadata.json"));
    ReleasePin {
        id: id(version),
        manifest: object(root, owner, &format!("{version}.release.json")),
        verified: true,
        dataset: DatasetPin {
            format: FORMAT,
            dataset: id(owner),
            version: id(version),
            groups: BTreeMap::from([(
                "source".into(),
                TablePin {
                    logical_metadata: None,
                    table: version.into(),
                    metadata: metadata.clone(),
                    snapshot: Some(1),
                    schema_id: 0,
                    objects: vec![metadata, object(root, owner, &format!("{version}.parquet"))],
                    rows: 1,
                },
            )]),
            computed: BTreeMap::new(),
            inputs: BTreeMap::new(),
            relationship: SourceRelationship::Independent,
            related: None,
            evidence: None,
            definition: "v1".into(),
            eligibility: 1,
            boundary: 1,
            request: OperationRequest::Build(RunSpec {
                dataset: id(owner),
                operation: id(version),
                base: None,
                relationship: None,
                input: root.join("unused-input").to_string_lossy().into_owned(),
                expected_digest: "0".repeat(64),
                sql: "SELECT id FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            }),
            parent_manifest: None,
            processor: "fixture".into(),
            validation: VerificationLevel::Routine,
            partition_by: Vec::new(),
        },
    }
}

fn related(result: &mut ReleasePin, target: &ReleasePin) {
    let request = RefreshSpec {
        dataset: result.dataset.dataset.clone(),
        operation: result.id.clone(),
        base: None,
        definition: "v1".into(),
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: target.version_ref(),
            grouping: AggregationDefinition {
                keys: Vec::new(),
                aggregates: Vec::new(),
            },
        }),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
    };
    result.dataset.request = OperationRequest::Refresh(Box::new(request.clone()));
    result.dataset.related = Some(Box::new(RelatedResult {
        input_sources: BTreeMap::new(),
        request,
        coverage: InputCoverage {
            inputs: BTreeMap::from([(target.dataset.dataset.clone(), target.version_ref())]),
            families: BTreeMap::new(),
            generations: BTreeMap::new(),
        },
        evidence: result.dataset.groups["source"].clone(),
    }));
}

fn link(result: &ReleasePin, previous: &ReleasePin) -> Succession {
    Succession {
        selected: result.version_ref(),
        predecessor: Some(previous.version_ref()),
        boundary: result.dataset.boundary,
        eligibility: result.dataset.eligibility,
    }
}

fn progress(root: &Path, target: &ReleasePin) -> BTreeMap<Id, SuccessionProof> {
    let owner = target.dataset.dataset.as_str();
    BTreeMap::from([(
        target.dataset.dataset.clone(),
        SuccessionProof {
            prior: release(root, owner, "prior").version_ref(),
            target: target.version_ref(),
            observed_head: release(root, owner, "observed").version_ref(),
            // Producer and consumer revisions belong to independent lifecycle tables.
            observed_revision: 1_000_000,
            selections_advanced: 1,
            selections_behind: 1,
        },
    )])
}

fn roots(rows: &SnapshotRows, dataset: &Id, key: &str) -> Result<Vec<ObjectRef>> {
    let (_, revision, json) = rows
        .prefix(key)
        .find(|(candidate, _, _)| *candidate == key)
        .unwrap();
    reference_roots(
        rows,
        key,
        json,
        &RetentionBlocker {
            table: crate::catalog::lifecycle_ident(dataset).to_string(),
            key: key.into(),
            revision,
        },
        limits().max_metadata_bytes,
    )
}

#[tokio::test]
async fn history_receipts_do_not_expand_unavailable_endpoints_but_results_keep_captured_inputs() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let mut target = release(root.path(), "producer", "target");
    let manifest = serde_json::to_vec(&target.dataset).unwrap();
    std::fs::write(&target.manifest.path, &manifest).unwrap();
    target.manifest.digest = blake3::hash(&manifest).to_hex().to_string();
    target.manifest.bytes = manifest.len() as u64;
    let mut result = release(root.path(), "consumer", "selected");
    related(&mut result, &target);
    let previous = release(root.path(), "consumer", "previous");
    let order = link(&result, &previous);
    let progress = progress(root.path(), &target);
    let observed = &progress[&target.dataset.dataset].observed_head.manifest;
    assert!(!Path::new(&previous.manifest.path).exists());
    assert!(!Path::new(&observed.path).exists());
    let view = lifecycle.open(&result.dataset.dataset).await.unwrap();
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("result/selected", &result).unwrap(),
                record("succession/selected", &order).unwrap(),
                record("input-progress/selected", &progress).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert!(progress[&target.dataset.dataset].observed_revision > view.revision);
    let rows = lifecycle.rows(&view).await.unwrap();
    for key in ["succession/selected", "input-progress/selected"] {
        assert!(
            roots(&rows, &result.dataset.dataset, key)
                .unwrap()
                .is_empty()
        );
    }
    let captured = roots(&rows, &result.dataset.dataset, "result/selected").unwrap();
    assert!(captured.contains(&target.manifest));
    assert!(captured.contains(&result.dataset.groups["source"].objects[1]));
    let candidates = vec![
        previous.manifest,
        observed.clone(),
        target.manifest.clone(),
        target.dataset.groups["source"].objects[1].clone(),
    ];
    drop(rows);
    let inspection = engine.inspect_retention(&candidates).await.unwrap();
    assert_eq!(
        inspection.metadata_objects_read, 1,
        "only the actual captured producer manifest is opened"
    );
    for candidate in inspection.objects {
        assert_eq!(
            candidate.blocker.is_some(),
            candidates[2..].contains(&candidate.object)
        );
    }
}

#[tokio::test]
async fn succession_roots_refuse_unanchored_or_malformed_ordering_receipts() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    for case in [
        "missing-result",
        "different-commit",
        "wrong-selected",
        "wrong-boundary",
        "wrong-eligibility",
        "foreign-predecessor",
        "self-predecessor",
        "wrong-key",
        "wrong-table",
        "unverified",
        "invalid-inline-path",
    ] {
        let dataset = id(case);
        let mut result = release(root.path(), case, "selected");
        let previous = release(root.path(), case, "previous");
        let mut order = link(&result, &previous);
        let mut key = "succession/selected";
        match case {
            "wrong-selected" => order.selected.manifest.digest = "1".repeat(64),
            "wrong-boundary" => order.boundary += 1,
            "wrong-eligibility" => order.eligibility += 1,
            "foreign-predecessor" => {
                order.predecessor = Some(release(root.path(), "other", "previous").version_ref())
            }
            "self-predecessor" => order.predecessor = Some(order.selected.clone()),
            "wrong-key" => key = "succession/other",
            "unverified" => result.verified = false,
            "invalid-inline-path" => {
                order.predecessor.as_mut().unwrap().manifest.path = "relative-path".into()
            }
            _ => {}
        }
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let mut records = vec![record(key, &order).unwrap()];
        if case == "different-commit" {
            view = lifecycle
                .commit(&view, vec![record("result/selected", &result).unwrap()])
                .await
                .unwrap();
        } else if case != "missing-result" {
            records.push(record("result/selected", &result).unwrap());
        }
        let view = lifecycle.commit(&view, records).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        let table = if case == "wrong-table" {
            id("other")
        } else {
            dataset
        };
        let error = roots(&rows, &table, key).unwrap_err();
        assert!(
            matches!(
                error.code(),
                "retention_inventory" | "invalid_succession" | "unavailable_succession"
            ),
            "{case}: {error}"
        );
    }
}

#[tokio::test]
async fn progress_roots_refuse_wrong_receipts_endpoints_and_coverage() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    for case in [
        "missing-result",
        "different-commit",
        "unrelated-result",
        "wrong-result-version",
        "unverified",
        "wrong-table",
        "wrong-coverage-key",
        "wrong-target",
        "zero-observed-revision",
        "negative-observed-revision",
        "prior-dataset",
        "observed-owner",
        "invalid-inline-path",
        "advanced-zero-different",
        "advanced-nonzero-equal",
        "behind-zero-different",
        "behind-nonzero-equal",
        "repeated-prefix",
        "same-version-different-manifest",
    ] {
        let dataset = id(case);
        let target = release(root.path(), "producer", "target");
        let mut result = release(root.path(), case, "selected");
        related(&mut result, &target);
        let mut progress = progress(root.path(), &target);
        let proof = progress.get_mut(&target.dataset.dataset).unwrap();
        match case {
            "unrelated-result" => result.dataset.related = None,
            "wrong-result-version" => result.dataset.version = id("different"),
            "unverified" => result.verified = false,
            "wrong-target" => {
                proof.target = release(root.path(), "producer", "other").version_ref()
            }
            "zero-observed-revision" => proof.observed_revision = 0,
            "negative-observed-revision" => proof.observed_revision = -1,
            "prior-dataset" => proof.prior.dataset = id("other"),
            "observed-owner" => proof.observed_head.manifest.owner = id("other"),
            "invalid-inline-path" => proof.observed_head.manifest.path = "relative-path".into(),
            "advanced-zero-different" => proof.selections_advanced = 0,
            "advanced-nonzero-equal" => proof.prior = proof.target.clone(),
            "behind-zero-different" => proof.selections_behind = 0,
            "behind-nonzero-equal" => proof.observed_head = proof.target.clone(),
            "same-version-different-manifest" => proof.prior.version = proof.target.version.clone(),
            _ => {}
        }
        if case == "wrong-coverage-key" {
            let proof = progress.remove(&target.dataset.dataset).unwrap();
            progress.insert(id("other"), proof);
        }
        let key = if case == "repeated-prefix" {
            "input-progress/input-progress/selected"
        } else {
            "input-progress/selected"
        };
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let mut records = vec![record(key, &progress).unwrap()];
        if case == "different-commit" {
            view = lifecycle
                .commit(&view, vec![record("result/selected", &result).unwrap()])
                .await
                .unwrap();
        } else if case != "missing-result" {
            records.push(record("result/selected", &result).unwrap());
        }
        let view = lifecycle.commit(&view, records).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        let table = if case == "wrong-table" {
            id("other")
        } else {
            dataset
        };
        assert_eq!(
            roots(&rows, &table, key).unwrap_err().code(),
            "retention_inventory",
            "{case}"
        );
    }
}

#[tokio::test]
async fn existing_succession_bindings_and_owner_grants_remain_enforced() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let owner = id("producer");
    let consumer = id("consumer");
    let foreign = object(root.path(), owner.as_str(), "old-payload.parquet");
    let view = lifecycle.open(&owner).await.unwrap();
    // A retained historical grant is already durable authority. This fixture
    // creates only its exact native receipt; no payload processing is needed.
    let grant = OwnerProtection {
        owner: owner.clone(),
        consumer: consumer.clone(),
        table_uuid: view.table.metadata().uuid().to_string(),
        revision: view.revision + 1,
        objects: vec![foreign.clone()],
    };
    let token = protection_key(&grant).unwrap();
    lifecycle
        .commit(&view, vec![record(&token, &grant).unwrap()])
        .await
        .unwrap();
    let result = release(root.path(), consumer.as_str(), "selected");
    let previous = release(root.path(), consumer.as_str(), "previous");
    let order = link(&result, &previous);
    let view = lifecycle.open(&consumer).await.unwrap();
    let order_value = serde_json::to_value(&order).unwrap();
    let mut binding = ReferenceBinding {
        consumer: consumer.clone(),
        key: "succession/selected".into(),
        revision: view.revision + 1,
        digest: blake3::hash(&serde_json::to_vec(&order_value).unwrap())
            .to_hex()
            .to_string(),
        protections: vec![OwnerProtectionRef {
            owner,
            token,
            table_uuid: grant.table_uuid,
            revision: grant.revision,
        }],
    };
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("result/selected", &result).unwrap(),
                record("succession/selected", &order).unwrap(),
                record(binding_key(&binding.key), &binding).unwrap(),
            ],
        )
        .await
        .unwrap();
    engine
        .check_reference_binding(&view, &binding.key)
        .await
        .unwrap();
    let inspected = engine
        .inspect_retention(std::slice::from_ref(&foreign))
        .await
        .unwrap();
    assert_eq!(inspected.metadata_objects_read, 0);
    assert!(
        inspected.objects[0]
            .blocker
            .as_ref()
            .unwrap()
            .key
            .starts_with("owner-protection/")
    );
    let reference = &binding.protections[0];
    assert_eq!(
        engine
            .plan_grant_retirement(
                &reference.owner,
                &id("legacy-observation"),
                std::slice::from_ref(&reference.token),
                "legacy observation still promises protection",
            )
            .await
            .unwrap_err()
            .code(),
        "grant_in_use"
    );
    binding.revision = view.revision + 1;
    binding.protections[0].token = "owner-protection/missing".into();
    let view = lifecycle
        .commit(
            &view,
            vec![
                record("result/selected", &result).unwrap(),
                record("succession/selected", &order).unwrap(),
                record(binding_key(&binding.key), &binding).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .check_reference_binding(&view, &binding.key)
            .await
            .unwrap_err()
            .code(),
        "invalid_owner_protection"
    );
    assert_eq!(
        engine
            .inspect_retention(&[foreign])
            .await
            .unwrap_err()
            .code(),
        "retention_inventory"
    );
}
