//! Explicit disclosure policies and immutable local Parquet distributions.
// Rust guideline compliant 2026-02-21

use crate::model::{Id, ObjectRef, OperationId, VersionRef};
use serde::{Deserialize, Serialize};

/// Explicitly authorizes the active row population in the selected native snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportRows {
    /// Include every active row; row restrictions must be prepared upstream.
    All,
}

/// Authorizes named columns from every active row of one retained field group.
///
/// The first local format supports flat primitive columns. It does not copy
/// source metadata, evidence files, hidden occurrence columns, or deleted files.
/// Row filtering and combined field groups require a separately qualified policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPolicy {
    pub group: String,
    /// Caller-supplied row authorization; the engine never infers restrictions.
    pub rows: ExportRows,
    /// Exact names in the requested output order; an empty list is refused.
    pub columns: Vec<String>,
}

/// Creates and selects one authorized export using a stable retry identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub source: VersionRef,
    pub channel: Id,
    /// Exact export currently selected by the channel, or an unoccupied channel.
    pub expected: Option<Id>,
    pub policy: ExportPolicy,
}

/// Records the retained native identity and physical interpretation of one column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportField {
    pub id: i32,
    pub name: String,
    /// Arrow's precise primitive type description, including units and precision.
    pub data_type: String,
    pub nullable: bool,
}

/// Retains creation checks so retries need not reopen the output payload.
///
/// The writer hashes bytes as it writes. Validation independently checks the
/// file size, readable footer, field identities/types, row count, and a bounded
/// decoded sample. These checks do not establish full independent reconstruction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportValidation {
    pub object: ObjectRef,
    pub rows: u64,
    pub sampled_rows: usize,
    pub sampled_decoded_bytes: usize,
}

/// Identifies an independently readable file selected through the dataset catalog.
///
/// Resolve a channel once and retain this pin while reading its immutable file.
/// This local format does not establish remote publication or anonymous access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPin {
    pub format: u32,
    pub id: Id,
    pub dataset: Id,
    pub source: VersionRef,
    /// Native lifecycle revision accepting the exact source release.
    pub source_revision: i64,
    pub policy: ExportPolicy,
    pub object: ObjectRef,
    pub rows: u64,
    pub fields: Vec<ExportField>,
    pub validation: ExportValidation,
}
