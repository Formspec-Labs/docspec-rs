//! Accepted metadata fixtures have no source, output, or manifest payloads.
// Rust guideline compliant 2026-02-21

use super::*;
use serde_json::json;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn object(dataset: &Id, name: &str) -> ObjectRef {
    ObjectRef {
        owner: dataset.clone(),
        path: format!("/absent/{name}"),
        digest: blake3::hash(name.as_bytes()).to_hex().to_string(),
        bytes: 1,
    }
}

fn table(dataset: &Id, name: &str) -> TablePin {
    TablePin {
        table: name.into(),
        metadata: object(dataset, name),
        snapshot: Some(1),
        schema_id: 0,
        objects: vec![],
        rows: 1,
        logical_metadata: None,
    }
}

fn build(dataset: &Id, name: &str, base: Option<&ReleasePin>) -> OperationRequest {
    OperationRequest::Build(RunSpec {
        dataset: dataset.clone(),
        operation: id(name),
        base: base.map(|base| base.id.clone()),
        relationship: None,
        input: "/absent/input.parquet".into(),
        expected_digest: "0".repeat(64),
        sql: "SELECT * FROM input".into(),
        definition: base
            .map_or("v1", |base| base.dataset.definition.as_str())
            .into(),
            partition_by: Vec::new(),
    })
}

fn backfill(base: &ReleasePin, name: &str, group: &str, definition: &str) -> OperationRequest {
    OperationRequest::Backfill(BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(name),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        group: id(group),
        group_definition: definition.into(),
        fields: vec![ComputedField {
            id: 10,
            name: format!("{group}_length"),
            expression: "length(text)".into(),
        }],
    })
}

fn candidate(request: &OperationRequest, base: Option<&ReleasePin>) -> ReleasePin {
    let dataset = request.dataset();
    let name = request.operation().as_str();
    let mut value = ReleasePin {
        id: request.operation().clone(),
        manifest: object(dataset, &format!("manifest-{name}")),
        verified: true,
        dataset: DatasetPin {
            format: FORMAT,
            dataset: dataset.clone(),
            version: request.operation().clone(),
            groups: base.map_or_else(
                || BTreeMap::from([("source".into(), table(dataset, "source"))]),
                |base| base.dataset.groups.clone(),
            ),
            computed: base.map_or_else(BTreeMap::new, |base| base.dataset.computed.clone()),
            inputs: BTreeMap::new(),
            relationship: SourceRelationship::Independent,
            related: None,
            evidence: None,
            definition: request.result_definition().into(),
            boundary: base.map_or(1, |base| base.dataset.boundary),
            eligibility: 0,
            request: request.clone(),
            parent_manifest: base.map(|base| base.manifest.clone()),
            processor: PROCESSOR.into(),
            validation: VerificationLevel::Routine,
            partition_by: Vec::new(),
        },
    };
    match request {
        OperationRequest::Backfill(spec) => {
            value
                .dataset
                .groups
                .insert(spec.group.to_string(), table(dataset, name));
            value.dataset.computed.insert(
                spec.group.to_string(),
                ComputedGroup {
                    origins: Some(object(dataset, &format!("origins-{name}"))),
                    qualification: None,
                    definition: spec.group_definition(),
                    input_fields: BTreeMap::from([("text".into(), 1)]),
                    request: request.clone(),
                    native_field_ids: BTreeMap::from([(10, 2)]),
                    source_metadata: value.dataset.groups["source"].metadata.clone(),
                    base_manifest: base.unwrap().manifest.clone(),
                },
            );
        }
        OperationRequest::Evidence(_) => {}
        _ => {
            if let Some(base) = base {
                value
                    .dataset
                    .groups
                    .insert("source".into(), table(dataset, &format!("source-{name}")));
                if !matches!(request, OperationRequest::Schema(_)) {
                    value.dataset.boundary += 1;
                }
                for (group, binding) in &mut value.dataset.computed {
                    binding.request = request.clone();
                    binding.base_manifest = base.manifest.clone();
                    binding.source_metadata = value.dataset.groups["source"].metadata.clone();
                    binding.origins = Some(object(dataset, &format!("origins-{name}-{group}")));
                    value
                        .dataset
                        .groups
                        .insert(group.clone(), table(dataset, &format!("{name}-{group}")));
                }
            }
        }
    }
    value
}

async fn workspace() -> (tempfile::TempDir, Engine, View, ReleasePin) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path(),
        Limits {
            compute_threads: 1,
            memory_bytes: 256 << 20,
            scratch_bytes: 1 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 512 << 10,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let dataset = id("source");
    let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
    let source = candidate(&build(&dataset, "source-first", None), None);
    let view = select(&engine, &view, &source, None).await;
    (root, engine, view, source)
}

async fn select(
    engine: &Engine,
    view: &View,
    value: &ReleasePin,
    previous: Option<&ReleasePin>,
) -> View {
    let request = &value.dataset.request;
    let operation = Operation {
        request_digest: fingerprint(request).unwrap(),
        boundary: value.dataset.boundary,
        eligibility: value.dataset.eligibility,
        epoch: 0,
        admission_fence: None,
        result_format: value.dataset.format,
        definition: request.definition().into(),
        base: request.base().cloned(),
        base_manifest: previous
            .filter(|_| request.base().is_some())
            .map(|previous| previous.manifest.clone()),
        schema_key: None,
        require_current_boundary: false,
    };
    let view = engine
        .inner
        .lifecycle
        .commit(
            view,
            vec![
                record(format!("operation/{}", value.id), operation).unwrap(),
                record(format!("request/{}", value.id), request).unwrap(),
            ],
        )
        .await
        .unwrap();
    let link = Succession {
        selected: value.version_ref(),
        predecessor: previous.map(ReleasePin::version_ref),
        boundary: value.dataset.boundary,
        eligibility: value.dataset.eligibility,
    };
    engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record(format!("result/{}", value.id), value).unwrap(),
                record(format!("succession/{}", value.id), link).unwrap(),
                record(
                    "authority",
                    json!({"selected": value.dataset, "release": value,
            "selection_revision": view.revision + 1}),
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap()
}

async fn established() -> (tempfile::TempDir, Engine, View, ReleasePin) {
    let (root, engine, view, source) = workspace().await;
    let original = candidate(
        &backfill(&source, "target-first", "target", "A"),
        Some(&source),
    );
    let view = select(&engine, &view, &original, Some(&source)).await;
    (root, engine, view, original)
}

#[tokio::test]
async fn allows_coordinated_updates_siblings_schema_and_evidence_after_retirement() {
    let (_root, engine, mut view, original) = established().await;
    let dataset = original.dataset.dataset.clone();
    let update = OperationRequest::Update(UpdateSpec {
        dataset: dataset.clone(),
        operation: id("update"),
        definition: "v1".into(),
        base: original.id.clone(),
        keys: vec!["text".into()],
        replacements: InputSpec {
            path: "/absent/update".into(),
            digest: "0".repeat(64),
        },
        deletions: None,
        merge: None,
        relationship: None,
    });
    let mut current = candidate(&update, Some(&original));
    view = select(&engine, &view, &current, Some(&original)).await;
    let next = candidate(
        &backfill(&current, "sibling", "sibling", "S"),
        Some(&current),
    );
    view = select(&engine, &view, &next, Some(&current)).await;
    current = next;
    let next = candidate(
        &OperationRequest::Schema(SchemaSpec {
            dataset: dataset.clone(),
            operation: id("schema"),
            definition: "v1".into(),
            next_definition: "v2".into(),
            base: current.id.clone(),
            fields: vec![SchemaField {
                id: 1,
                name: "renamed_text".into(),
            }],
            additions: vec![],
            write_defaults: BTreeMap::new(),
        }),
        Some(&current),
    );
    view = select(&engine, &view, &next, Some(&current)).await;
    current = next;
    let next = candidate(
        &OperationRequest::Evidence(crate::evidence::EvidenceSpec {
            dataset: dataset.clone(),
            operation: id("evidence"),
            definition: "v2".into(),
            base: current.id.clone(),
            replacements: InputSpec {
                path: "/absent/receipts".into(),
                digest: "0".repeat(64),
            },
            deletions: None,
        }),
        Some(&current),
    );
    view = select(&engine, &view, &next, Some(&current)).await;
    current = next;
    let next = candidate(
        &build(&dataset, "full-rebuild", Some(&current)),
        Some(&current),
    );
    view = select(&engine, &view, &next, Some(&current)).await;
    view = engine
        .inner
        .lifecycle
        .commit(
            &view,
            vec![
                record("availability/target-first", json!({"retired": true})).unwrap(),
                record("recovery-retired/target-first", json!({"retired": true})).unwrap(),
            ],
        )
        .await
        .unwrap();
    engine
        .check_backfill_target_lineage(&view, &original, &next, &id("target"))
        .await
        .unwrap();
    // A captured current endpoint may be behind the observed accepted head.
    engine
        .check_backfill_target_lineage(&view, &original, &current, &id("target"))
        .await
        .unwrap();
}

#[tokio::test]
async fn competing_same_definition_and_aba_replacements_conflict() {
    for aba in [false, true] {
        let (_root, engine, view, original) = established().await;
        let replacement = candidate(
            &backfill(
                &original,
                "competitor",
                "target",
                if aba { "B" } else { "A" },
            ),
            Some(&original),
        );
        let view = select(&engine, &view, &replacement, Some(&original)).await;
        let (view, current) = if aba {
            let reverted = candidate(
                &backfill(&replacement, "reverted", "target", "A"),
                Some(&replacement),
            );
            (
                select(&engine, &view, &reverted, Some(&replacement)).await,
                reverted,
            )
        } else {
            (view, replacement)
        };
        assert_eq!(
            engine
                .check_backfill_target_lineage(&view, &original, &current, &id("target"))
                .await
                .unwrap_err()
                .code(),
            "backfill_target_conflict"
        );
    }
}

#[tokio::test]
async fn absent_target_cannot_appear_and_disappear_in_intervening_history() {
    let (_root, engine, view, original) = workspace().await;
    let present = candidate(
        &backfill(&original, "competitor", "target", "A"),
        Some(&original),
    );
    let view = select(&engine, &view, &present, Some(&original)).await;
    // A later full replacement can omit a previously selected group. It cannot
    // erase the competing selection observed since the original absent capture.
    let absent = candidate(&build(&original.dataset.dataset, "replacement", None), None);
    let mut absent = absent;
    absent.dataset.boundary = 2;
    let view = select(&engine, &view, &absent, Some(&present)).await;
    assert_eq!(
        engine
            .check_backfill_target_lineage(&view, &original, &absent, &id("target"))
            .await
            .unwrap_err()
            .code(),
        "backfill_target_conflict"
    );
}

#[tokio::test]
async fn absent_target_and_equal_endpoints_remain_valid() {
    let (_root, engine, view, original) = workspace().await;
    engine
        .check_backfill_target_lineage(&view, &original, &original, &id("target"))
        .await
        .unwrap();
    let next = candidate(
        &build(&original.dataset.dataset, "update", Some(&original)),
        Some(&original),
    );
    let view = select(&engine, &view, &next, Some(&original)).await;
    engine
        .check_backfill_target_lineage(&view, &original, &next, &id("target"))
        .await
        .unwrap();
    let mut unbased = candidate(&build(&original.dataset.dataset, "unbased", None), None);
    unbased.dataset.boundary = next.dataset.boundary + 1;
    let view = select(&engine, &view, &unbased, Some(&next)).await;
    engine
        .check_backfill_target_lineage(&view, &original, &unbased, &id("target"))
        .await
        .unwrap();
}

#[tokio::test]
async fn refuses_remapped_authored_ids_and_uncoordinated_binding_changes() {
    for fault in ["ids", "binding", "source", "parent"] {
        let (_root, engine, view, original) = established().await;
        let mut next = candidate(
            &build(&original.dataset.dataset, "next", Some(&original)),
            Some(&original),
        );
        match fault {
            "ids" => {
                next.dataset
                    .computed
                    .get_mut("target")
                    .unwrap()
                    .input_fields
                    .insert("text".into(), 7);
            }
            "binding" => {
                next.dataset.computed.get_mut("target").unwrap().request =
                    original.dataset.request.clone()
            }
            "source" => {
                next.dataset
                    .computed
                    .get_mut("target")
                    .unwrap()
                    .source_metadata = object(&original.dataset.dataset, "unrelated")
            }
            "parent" => {
                next.dataset.parent_manifest = Some(object(&original.dataset.dataset, "unrelated"))
            }
            _ => unreachable!(),
        }
        let view = select(&engine, &view, &next, Some(&original)).await;
        assert!(
            engine
                .check_backfill_target_lineage(&view, &original, &next, &id("target"))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn rejects_forged_endpoints_and_unrelated_or_backward_history() {
    let (_root, engine, view, original) = established().await;
    let next = candidate(
        &build(&original.dataset.dataset, "next", Some(&original)),
        Some(&original),
    );
    let view = select(&engine, &view, &next, Some(&original)).await;
    for fault in ["manifest", "coverage", "binding", "unrelated"] {
        let mut endpoint = next.clone();
        match fault {
            "manifest" => endpoint.manifest.digest = "f".repeat(64),
            "coverage" => endpoint.dataset.boundary += 1,
            "binding" => endpoint.dataset.computed.clear(),
            "unrelated" => {
                endpoint.id = id("isolated");
                endpoint.dataset.version = endpoint.id.clone();
            }
            _ => unreachable!(),
        }
        assert!(
            engine
                .check_backfill_target_lineage(&view, &original, &endpoint, &id("target"))
                .await
                .is_err()
        );
    }
    assert!(
        engine
            .check_backfill_target_lineage(&view, &next, &original, &id("target"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rejects_request_receipt_and_chain_inconsistency() {
    for fault in ["request", "operation", "link", "result"] {
        let (_root, engine, view, original) = established().await;
        let next = candidate(
            &build(&original.dataset.dataset, "next", Some(&original)),
            Some(&original),
        );
        let view = select(&engine, &view, &next, Some(&original)).await;
        let (key, value) = match fault {
            "request" => (
                "request/next",
                json!(build(&original.dataset.dataset, "wrong", Some(&original))),
            ),
            "operation" => ("operation/next", json!({"request_digest": "wrong"})),
            "link" => (
                "succession/next",
                json!({"selected": next.version_ref(), "predecessor": next.version_ref(), "boundary": 2, "eligibility": 0}),
            ),
            "result" => {
                let mut forged = next.clone();
                forged.verified = false;
                ("result/next", json!(forged))
            }
            _ => unreachable!(),
        };
        let view = engine
            .inner
            .lifecycle
            .commit(&view, vec![record(key, value).unwrap()])
            .await
            .unwrap();
        assert!(
            engine
                .check_backfill_target_lineage(&view, &original, &next, &id("target"))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn refuses_bounded_metadata_traversal_before_history_grows_unbounded() {
    let (_root, engine, mut view, original) = established().await;
    let mut current = original.clone();
    // Retained lifecycle data fits the allowance, but decoding the whole proof
    // with its conservative temporary-allocation charge must refuse.
    for index in 0..32 {
        let next = candidate(
            &build(
                &original.dataset.dataset,
                &format!("step-{index}"),
                Some(&current),
            ),
            Some(&current),
        );
        view = select(&engine, &view, &next, Some(&current)).await;
        current = next;
    }
    assert_eq!(
        engine
            .check_backfill_target_lineage(&view, &original, &current, &id("target"))
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}
