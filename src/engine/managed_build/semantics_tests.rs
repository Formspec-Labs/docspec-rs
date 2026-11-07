//! Evaluator mapping revisions preserve all raw DCC check coverage.
// Rust guideline compliant 2026-02-21
use super::*;
use serde_json::json;

#[test]
fn revision_two_requires_execution_fact_and_rejects_crossed_rules() {
    let version:VersionRef=serde_json::from_value(json!({"dataset":"tiny","version":"candidate","manifest":{"owner":"tiny","path":"objects/m.json","digest":"a".repeat(64),"bytes":1}})).unwrap();
    let read = NativeReadReceipt {
        read_id: uuid::Uuid::nil(),
        version: version.clone(),
        schema_id: 0,
        schema: Schema::empty(),
        rows: 0,
        ipc_sha256: "b".repeat(64),
    };
    let evidence = |revision: &str, fact: bool, status: &str| -> EvaluatedQuality {
        let mut checks =
            vec![json!({"id":"amounts.nonnegative","status":"passed","nativeCheckIds":["raw-1"]})];
        let mut required = vec!["amounts.nonnegative"];
        if fact {
            checks
                .push(json!({"id":QUALITY_EXECUTION_COMPLETE,"status":status,"nativeCheckIds":[]}));
            required.push(QUALITY_EXECUTION_COMPLETE);
        }
        serde_json::from_value(json!({"status":"passed","target":{"kind":"native","nativeVersion":version},"evaluator":{"id":"urn:ddk:quality:datacontract-cli","version":"1.2.9","distributionSha256":"c".repeat(64)},"rules":{"path":"rules.json","sha256":"d".repeat(64)},"mappingSemantics":{"id":"urn:ddk:quality:duckdb-direct-view","revision":revision},"requiredCheckIds":required,"checks":checks,"nativeReport":{"path":"raw.json","sha256":"e".repeat(64)},"nativeRead":{"path":"read.json","sha256":"f".repeat(64)}})).unwrap()
    };
    let rules = |evaluated: &EvaluatedQuality| -> ResolvedRules {
        serde_json::from_value(json!({"document":{"id":"tiny","version":"1","schema":[{"properties":[]}]},"requiredCheckIds":evaluated.required_check_ids,"mappingSemantics":{"id":evaluated.mapping_semantics.id,"revision":evaluated.mapping_semantics.revision}})).unwrap()
    };
    let raw = || -> DccReport {
        serde_json::from_value(json!({"dataContractId":"tiny","dataContractVersion":"1","datacontractCliVersion":"1.2.9","dryRun":false,"result":"passed","checks":[{"id":"raw-1","key":"amounts.nonnegative","result":"passed"}]})).unwrap()
    };
    let two = evidence("2", true, "passed");
    validate_dcc(&two, &rules(&two), raw(), &read).unwrap();
    let one = evidence("1", false, "passed");
    validate_dcc(&one, &rules(&one), raw(), &read).unwrap();
    for invalid in [
        evidence("2", false, "passed"),
        evidence("2", true, "failed"),
        evidence("2", true, "unknown"),
        evidence("2", true, "not_run"),
        evidence("1", true, "passed"),
    ] {
        assert!(validate_dcc(&invalid, &rules(&invalid), raw(), &read).is_err());
    }
    assert!(validate_dcc(&two, &rules(&one), raw(), &read).is_err());
    assert!(validate_dcc(&one, &rules(&two), raw(), &read).is_err());
    let mut missing = raw();
    missing.checks.clear();
    assert!(validate_dcc(&two, &rules(&two), missing, &read).is_err());
}
