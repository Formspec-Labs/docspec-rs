//! Explicit roots retain exact identities and admission without payload reads.

use super::*;

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
        path: root.join(name).to_string_lossy().into_owned(),
        digest: "0".repeat(64),
        bytes: 17,
    }
}

fn table(root: &Path, owner: &str, name: &str) -> TablePin {
    let metadata = object(root, owner, &format!("{name}.metadata.json"));
    TablePin {
        logical_metadata: None,
        table: name.into(),
        metadata: metadata.clone(),
        snapshot: Some(1),
        schema_id: 0,
        objects: vec![metadata, object(root, owner, &format!("{name}.parquet"))],
        rows: 1,
    }
}

fn release(root: &Path, owner: &str, version: &str) -> ReleasePin {
    ReleasePin {
        id: id(version),
        dataset: DatasetPin {
            format: FORMAT,
            dataset: id(owner),
            version: id(version),
            groups: BTreeMap::from([("source".into(), table(root, owner, version))]),
            computed: BTreeMap::new(),
            inputs: BTreeMap::from([(
                "source".into(),
                object(root, owner, &format!("{version}.input")),
            )]),
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
                input: root
                    .join("unused-external-input")
                    .to_string_lossy()
                    .into_owned(),
                expected_digest: "0".repeat(64),
                sql: "SELECT * FROM source".into(),
                definition: "v1".into(),
            }),
            parent_manifest: None,
            processor: "fixture".into(),
            validation: VerificationLevel::Routine,
        },
        manifest: object(root, owner, &format!("{version}.release.json")),
        verified: true,
    }
}

fn persist_manifest(release: &mut ReleasePin) {
    let bytes = serde_json::to_vec(&release.dataset).unwrap();
    std::fs::write(&release.manifest.path, &bytes).unwrap();
    release.manifest.digest = blake3::hash(&bytes).to_hex().to_string();
    release.manifest.bytes = bytes.len() as u64;
}

fn direct_objects(release: &ReleasePin) -> Vec<ObjectRef> {
    std::iter::once(release.manifest.clone())
        .chain(release.dataset.inputs.values().cloned())
        .chain(
            release
                .dataset
                .groups
                .values()
                .flat_map(|table| table.objects.clone()),
        )
        .collect()
}

fn selected_authority(release: &ReleasePin, revision: i64) -> Authority {
    Authority {
        definition: release.dataset.definition.clone(),
        eligibility: release.dataset.eligibility,
        boundary: release.dataset.boundary,
        selected: Some(release.dataset.clone()),
        release: Some(release.clone()),
        selection_revision: Some(revision),
        ..Authority::default()
    }
}

#[tokio::test]
async fn authority_reuses_its_result_identity_without_lending_protection_to_unknown_results() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let dataset = id("consumer");
    let mut view = lifecycle.open(&dataset).await.unwrap();
    for (version, known) in [("known", true), ("unknown", false)] {
        let release = release(root.path(), dataset.as_str(), version);
        let key = format!("result/{version}");
        let authority = selected_authority(&release, view.revision + 1);
        let scope =
            known.then(|| ReferenceProtections::empty(&dataset, &lifecycle.budget).unwrap());
        let mut records =
            completion_records(&dataset, &view, &key, &release, scope.as_ref()).unwrap();
        records.push(record("authority", authority).unwrap());
        view = lifecycle.commit(&view, records).await.unwrap();

        let rows = lifecycle.rows(&view).await.unwrap();
        assert!(!rows.contains_key(&super::super::owners::binding_key("authority")));
        assert_eq!(
            engine
                .retained_protections(&view, &key)
                .await
                .unwrap()
                .is_some(),
            known,
        );
        let objects = direct_objects(&release);
        assert!(
            objects
                .iter()
                .all(|object| !Path::new(&object.path).exists())
        );
        let inspected = engine.inspect_retention(&objects).await.unwrap();
        assert_eq!(inspected.unmatched_bytes, 0);
        assert_eq!(inspected.metadata_objects_read, 0);
        assert!(
            inspected
                .objects
                .iter()
                .all(|object| object.blocker.is_some())
        );
    }
}

#[tokio::test]
async fn authority_alias_refuses_missing_mismatched_and_incomplete_selection_metadata() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    for case in [
        "missing-result",
        "different-result",
        "result-revision",
        "future-selection",
        "zero-selection",
        "wrong-dataset",
        "wrong-version",
        "wrong-owner",
        "unverified",
        "empty-with-revision",
        "selected-only",
        "release-only",
    ] {
        let dataset = id(case);
        let view = lifecycle.open(&dataset).await.unwrap();
        let mut release = release(root.path(), case, "selected");
        match case {
            "wrong-dataset" => release.dataset.dataset = id("other"),
            "wrong-version" => release.dataset.version = id("other"),
            "wrong-owner" => release.manifest.owner = id("other"),
            "unverified" => release.verified = false,
            _ => {}
        }
        let mut authority = selected_authority(&release, view.revision + 1);
        match case {
            "future-selection" => authority.selection_revision = Some(view.revision + 2),
            "zero-selection" => authority.selection_revision = Some(0),
            "empty-with-revision" => {
                authority.selected = None;
                authority.release = None;
            }
            "selected-only" => authority.release = None,
            "release-only" => authority.selected = None,
            _ => {}
        }
        let mut result = release;
        if case == "different-result" {
            result.dataset.definition = "different".into();
        }
        let mut records = vec![record("authority", &authority).unwrap()];
        if case != "missing-result" {
            records.push(record("result/selected", &result).unwrap());
        }
        let mut view = lifecycle.commit(&view, records).await.unwrap();
        if case == "result-revision" {
            view = lifecycle
                .commit(&view, vec![record("result/selected", &result).unwrap()])
                .await
                .unwrap();
        }
        let rows = lifecycle.rows(&view).await.unwrap();
        let (_, revision, json) = rows
            .prefix("authority")
            .find(|(key, _, _)| *key == "authority")
            .unwrap();
        let origin = RetentionBlocker {
            table: view.table.identifier().to_string(),
            key: "authority".into(),
            revision,
        };
        assert_eq!(
            reference_roots(
                &rows,
                "authority",
                json,
                &origin,
                limits().max_metadata_bytes
            )
            .unwrap_err()
            .code(),
            "retention_inventory",
            "{case}",
        );
    }
}

#[tokio::test]
async fn empty_authority_has_no_selected_result_or_protection_claim() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&id("consumer")).await.unwrap();
    lifecycle
        .commit(
            &view,
            vec![record("authority", Authority::default()).unwrap()],
        )
        .await
        .unwrap();
    let inspected = engine.inspect_retention(&[]).await.unwrap();
    assert!(inspected.objects.is_empty());
    assert_eq!(inspected.metadata_objects_read, 0);
}

#[tokio::test]
async fn closure_collects_history_computation_coverage_and_seals_without_payload_reads() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut base = release(root.path(), "consumer", "base");
    let mut producer = release(root.path(), "producer", "covered");
    let mut sibling = release(root.path(), "sibling", "sealed");
    for release in [&mut base, &mut producer, &mut sibling] {
        persist_manifest(release);
    }
    let mut selected = release(root.path(), "consumer", "selected");
    selected.dataset.parent_manifest = Some(base.manifest.clone());
    let schema_only = object(root.path(), "schema_owner", "schema-only.metadata.json");
    selected.dataset.computed.insert(
        "computed".into(),
        ComputedGroup {
            qualification: None,
            origins: None,
            definition: GroupDefinition {
                group: id("computed"),
                definition: "v1".into(),
                fields: Vec::new(),
            },
            input_fields: BTreeMap::new(),
            request: selected.dataset.request.clone(),
            native_field_ids: BTreeMap::new(),
            source_metadata: schema_only.clone(),
            base_manifest: base.manifest.clone(),
        },
    );
    let evidence = table(root.path(), "evidence_owner", "evidence");
    selected.dataset.related = Some(Box::new(RelatedResult {
        request: RefreshSpec {
            dataset: id("consumer"),
            operation: id("selected"),
            base: None,
            definition: "v1".into(),
            transform: RefreshTransform::Aggregate(AggregateTransform {
                input: producer.version_ref(),
                grouping: AggregationDefinition {
                    keys: Vec::new(),
                    aggregates: Vec::new(),
                },
            }),
            policy: RefreshPolicy::Captured,
            mode: RefreshMode::Full,
            relationship: None,
        },
        coverage: InputCoverage {
            inputs: BTreeMap::from([(id("producer"), producer.version_ref())]),
            families: BTreeMap::from([(
                id("family"),
                BTreeSet::from([id("producer"), id("sibling")]),
            )]),
            generations: BTreeMap::from([(
                id("family"),
                GenerationSeal {
                    family: id("family"),
                    generation: id("generation"),
                    members: BTreeMap::from([
                        (id("producer"), producer.version_ref()),
                        (id("sibling"), sibling.version_ref()),
                    ]),
                },
            )]),
        },
        evidence: evidence.clone(),
    }));
    let expected = [&base, &producer, &sibling, &selected]
        .into_iter()
        .flat_map(direct_objects)
        .chain([schema_only])
        .chain(evidence.objects)
        .map(|object| (object.path.clone(), object))
        .collect::<BTreeMap<_, _>>();
    // Only release manifests exist. The supplied root and every native/payload
    // path are absent: collection must trust the anchored pins' inventories.
    let references = engine
        .reference_objects(&[selected.clone(), selected])
        .await
        .unwrap();
    assert_eq!(
        references.objects,
        expected.into_values().collect::<Vec<_>>()
    );
    let budget = &engine.inner.lifecycle.budget;
    let all = limits().memory_bytes / 2;
    assert!(
        budget.reserve(all).is_err(),
        "returned objects retain admission"
    );
    drop(references);
    drop(budget.reserve(all).unwrap());
}

fn origin() -> RetentionBlocker {
    RetentionBlocker {
        table: "fixture".into(),
        key: "explicit-release/root".into(),
        revision: 0,
    }
}

#[test]
fn native_paths_require_exact_identities_discovered_anywhere_in_the_closure() {
    let object = object(Path::new("/retained"), "actual_owner", "payload.parquet");
    let mut unknown = Inventory::collecting(4096).unwrap();
    unknown.path(&object.path, &origin()).unwrap();
    assert_eq!(
        unknown.into_references().unwrap_err().code(),
        "retention_unknown_dependencies"
    );
    let mut known = Inventory::collecting(4096).unwrap();
    known.path(&object.path, &origin()).unwrap();
    known.object(&object, &origin()).unwrap();
    known.path(&object.path, &origin()).unwrap();
    assert_eq!(known.into_references().unwrap(), vec![object]);
}

#[tokio::test]
async fn native_expansion_never_assigns_the_metadata_owner_to_an_unknown_sidecar() {
    let root = tempfile::tempdir().unwrap();
    let budget = Budget::new(limits()).unwrap();
    let _memory = budget.reserve(limits().max_metadata_bytes).unwrap();
    let sidecar = object(root.path(), "sidecar_owner", "statistics.puffin");
    let schema = iceberg::spec::Schema::builder().build().unwrap();
    let metadata = storage::fresh_metadata(schema, root.path())
        .unwrap()
        .into_builder(None)
        .set_statistics(iceberg::spec::StatisticsFile {
            snapshot_id: 1,
            statistics_path: sidecar.path.clone(),
            file_size_in_bytes: sidecar.bytes as i64,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: Vec::new(),
        })
        .build()
        .unwrap()
        .metadata;
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let mut native = object(root.path(), "metadata_owner", "native.metadata.json");
    std::fs::write(&native.path, &bytes).unwrap();
    native.bytes = bytes.len() as u64;
    native.digest = blake3::hash(&bytes).to_hex().to_string();
    let mut inventory = Inventory::collecting(limits().max_metadata_bytes).unwrap();
    inventory.native(&native, &origin()).unwrap();
    inventory.expand(&budget, root.path()).await.unwrap();
    assert_eq!(inventory.metadata_objects_read, 1);
    assert!(
        inventory
            .required_paths
            .as_ref()
            .unwrap()
            .contains(&sidecar.path)
    );
    assert!(!Path::new(&sidecar.path).exists());
    assert_eq!(
        inventory.into_references().unwrap_err().code(),
        "retention_unknown_dependencies"
    );
    let mut inventory = Inventory::collecting(limits().max_metadata_bytes).unwrap();
    inventory.native(&native, &origin()).unwrap();
    inventory.expand(&budget, root.path()).await.unwrap();
    inventory.object(&sidecar, &origin()).unwrap();
    let references = inventory.into_references().unwrap();
    assert!(references.contains(&native));
    assert!(
        references.contains(&sidecar),
        "retain the actual foreign owner"
    );
}

#[test]
fn conflicts_refuse_and_duplicate_references_do_not_consume_more_capacity() {
    let original = object(Path::new("/retained"), "owner", "payload.parquet");
    for field in ["owner", "digest", "bytes"] {
        let mut conflicting = original.clone();
        match field {
            "owner" => conflicting.owner = id("other"),
            "digest" => conflicting.digest = "1".repeat(64),
            _ => conflicting.bytes += 1,
        }
        let mut inventory = Inventory::collecting(4096).unwrap();
        inventory.object(&original, &origin()).unwrap();
        assert_eq!(
            inventory
                .object(&conflicting, &origin())
                .unwrap_err()
                .code(),
            "retention_inventory"
        );
    }
    let limit = reference_bytes(&original) + original.path.len();
    let mut inventory = Inventory::collecting(limit).unwrap();
    for _ in 0..100 {
        inventory.object(&original, &origin()).unwrap();
    }
    assert_eq!(inventory.bytes, limit);
    assert_eq!(
        inventory.path("/another", &origin()).unwrap_err().code(),
        "metadata_capacity"
    );
    let mut inspection = Inventory::new(&[], 4096).unwrap();
    inspection.path("/unknown-native-file", &origin()).unwrap();
    inspection.object(&original, &origin()).unwrap();
    assert!(
        inspection.candidates.is_empty(),
        "inspection remains candidate-first"
    );
}

#[tokio::test]
async fn oversized_roots_refuse_before_following_missing_metadata() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut selected = release(root.path(), "owner", "selected");
    selected.dataset.parent_manifest = Some(object(root.path(), "owner", "missing.release.json"));
    selected.dataset.definition = "x".repeat(limits().max_metadata_bytes);
    assert_eq!(
        engine
            .reference_objects(&[selected])
            .await
            .err()
            .unwrap()
            .code(),
        "metadata_capacity"
    );
}

#[tokio::test]
async fn repeated_bindings_share_claim_memory_and_owner_changes_refuse_inspection() {
    use super::super::owners::{OwnerProtectionRef, ReferenceBinding};
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let lifecycle = &engine.inner.lifecycle;
    let owner = id("owner");
    let view = lifecycle.open(&owner).await.unwrap();
    let boundary = RetentionBoundary {
        table: view.table.identifier().to_string(),
        metadata: view.table.metadata_location_result().unwrap().to_owned(),
        revision: view.revision,
    };
    let binding = ReferenceBinding {
        consumer: id("consumer"),
        key: "generation/fixture".into(),
        revision: 1,
        digest: "0".repeat(64),
        protections: vec![OwnerProtectionRef {
            owner,
            token: "owner-protection/fixture".into(),
            table_uuid: view.table.metadata().uuid().to_string(),
            revision: 1,
        }],
    };
    let required = vec![object(root.path(), "owner", "manifest.json")];
    let mut inventory = Inventory::new(&[], limits().max_metadata_bytes).unwrap();
    inventory
        .binding(binding.clone(), required.clone())
        .unwrap();
    let bytes = inventory.bytes;
    // Many retained claims can share one immutable token. Their inspection must
    // not retain repeated grant obligations or consume another owner allowance.
    for _ in 0..100 {
        inventory
            .binding(binding.clone(), required.clone())
            .unwrap();
    }
    assert_eq!(inventory.bytes, bytes);
    assert_eq!(inventory.claims.len(), 1);
    assert_eq!(inventory.claims.values().next().unwrap().len(), 1);
    lifecycle
        .commit(&view, vec![record("fixture-change", true).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        inventory
            .check_bindings(lifecycle, &[boundary])
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
}
