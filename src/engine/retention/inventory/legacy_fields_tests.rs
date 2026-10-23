//! Strict unknown-field rejection and required phase change decoding tests.

use super::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn object(name: &str) -> ObjectRef {
    ObjectRef {
        owner: id("comments"),
        path: format!("/retained/{name}"),
        digest: "0".repeat(64),
        bytes: 17,
    }
}

fn table(name: &str) -> TablePin {
    let metadata = object(&format!("{name}.metadata.json"));
    TablePin {
        table: name.into(),
        logical_metadata: None,
        metadata: metadata.clone(),
        snapshot: Some(1),
        schema_id: 0,
        objects: vec![metadata],
        rows: 0,
    }
}

fn state(format: u32) -> DatasetPin {
    let source = table("source");
    DatasetPin {
        format,
        dataset: id("comments"),
        version: id("after"),
        groups: BTreeMap::from([("source".into(), source.clone())]),
        computed: BTreeMap::new(),
        inputs: BTreeMap::new(),
        relationship: SourceRelationship::Independent,
        related: None,
        evidence: Some(Box::new(crate::evidence::EvidenceResult {
            source: (&source).into(),
            table: table("receipts"),
        })),
        definition: "v1".into(),
        eligibility: 1,
        boundary: 1,
        request: OperationRequest::Build(RunSpec {
            dataset: id("comments"),
            operation: id("after"),
            base: None,
            relationship: None,
            input: "/retained/input.parquet".into(),
            expected_digest: "0".repeat(64),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        }),
        parent_manifest: None,
        processor: "fixture".into(),
        validation: VerificationLevel::Routine,
        partition_by: Vec::new(),
    }
}

fn changes() -> ChangePin {
    ChangePin {
        rules: String::new(),
        base_version: id("before"),
        base_manifest: object("before.release.json"),
        before_metadata: object("before.metadata.json"),
        after_metadata: object("source.metadata.json"),
        key_field_ids: vec![1],
        removed: table("removed"),
        added: table("added"),
    }
}

fn legacy_changes() -> serde_json::Value {
    let mut value = serde_json::to_value(changes()).unwrap();
    value.as_object_mut().unwrap().remove("rules");
    value
}

#[test]
fn unrelated_and_malformed_dependency_fields_still_refuse() {
    for path in [
        "/unknown_dependency",
        "/evidence/unknown_dependency",
        "/groups/source/changes",
        "/changes",
        "/evidence/changes",
    ] {
        let mut encoded = serde_json::to_value(state(11)).unwrap();
        let (parent, field) = path.rsplit_once('/').unwrap();
        encoded
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                field.into(),
                serde_json::to_value(object("unknown")).unwrap(),
            );
        assert!(
            serde_json::from_value::<Strict<DatasetPin>>(encoded).is_err(),
            "{path}"
        );
    }
}

#[test]
fn release_and_authority_do_not_ignore_changes_outside_the_complete_state() {
    let dataset = serde_json::to_value(state(11)).unwrap();
    let mut release = serde_json::json!({
        "id": "after", "dataset": dataset,
        "manifest": object("after.release.json"), "verified": true,
    });
    release["changes"] = legacy_changes();
    assert!(serde_json::from_value::<Strict<ReleasePin>>(release.clone()).is_err());
    release.as_object_mut().unwrap().remove("changes");
    let mut authority = serde_json::json!({
        "definition": "v1", "eligibility": 1, "epoch": 0, "boundary": 1,
        "selected": dataset, "release": release, "selection_revision": 1,
    });
    authority["changes"] = legacy_changes();
    assert!(serde_json::from_value::<Strict<Authority>>(authority).is_err());
}

#[test]
fn required_phase_changes_remain_part_of_the_decoded_recovery_claim() {
    let expected = PhaseResult {
        operation: id("after"),
        phase: "processing".into(),
        request_digest: "0".repeat(64),
        eligibility: 1,
        outputs: Vec::new(),
        table: Some(table("source")),
        rows: 0,
        changes: Some(changes()),
        coverage: None,
        metrics: WriteMetrics::default(),
        dependencies: Some(PhaseDependencies::default()),
    };
    let mut encoded = serde_json::to_value(&expected).unwrap();
    encoded["changes"] = legacy_changes();
    let actual: Strict<PhaseResult> = serde_json::from_value(encoded).unwrap();
    assert_eq!(actual.0, expected);
    assert!(actual.0.changes.is_some());
}
