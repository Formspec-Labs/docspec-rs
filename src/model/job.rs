//! Whole-job candidates remain distinct from single-dataset release versions.
// Rust guideline compliant 2026-02-21
use super::{Id, ObjectRef};
use serde::{Deserialize, Serialize};

/// Exact immutable completion of one native multi-input, multi-output calculation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobCandidateRef {
    pub scheme: String,
    pub scheme_version: String,
    pub dataset: Id,
    pub operation: Id,
    pub manifest: ObjectRef,
}

/// Exact lifecycle row generation in one native table incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobInputRecordRef {
    pub table: String,
    pub table_uuid: String,
    pub key: String,
    pub revision: i64,
}

/// Exact guarded adoption of one producer definition by the workspace owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProducerAdoptionRef {
    pub scheme: String,
    pub scheme_version: String,
    pub dataset: Id,
    pub adoption: Id,
    pub request_digest: String,
    pub native_record: JobInputRecordRef,
    pub definition_sha256: String,
}
