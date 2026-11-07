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
