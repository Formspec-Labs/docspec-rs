//! Receipt values associated with exact, stable source occurrences.

use crate::model::{ChangePin, Id, InputSpec, NativeSnapshotRef, OperationId, TablePin};
use serde::{Deserialize, Serialize};

/// Native field containing the source occurrence's stable file identity.
pub const SOURCE_FILE: &str = "source_file";
/// Native field containing the nonnegative row within that stable identity.
pub const SOURCE_ROW: &str = "source_row";
/// Native field containing an opaque receipt value, including nested values.
pub const EVIDENCE: &str = "evidence";

/// Replaces complete receipt groups for exact source occurrences.
///
/// Replacement Parquet contains exactly `source_file: string`,
/// `source_row: long`, and `evidence` in that order. The evidence field may use
/// any supported native type; subsequent changes preserve its complete schema.
/// Every key must identify an occurrence in the captured source. Multiple,
/// duplicate, and null receipt values are preserved. Optional deletion Parquet
/// contains only the two key fields. Deletion and replacement keys cannot overlap.
///
/// The original Parquet bytes are retained. A URI in an opaque receipt does not
/// promise that the referenced attachment or external resource was retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub definition: String,
    pub base: Id,
    pub replacements: InputSpec,
    pub deletions: Option<InputSpec>,
}

/// A receipt table bound to the selected source and its applied changes.
///
/// This is separate from a join or aggregate's contributor evidence. A source
/// update removes receipts for replaced or deleted occurrences and preserves
/// receipts for every surviving occurrence. An unchanged receipt table retains
/// its exact pin and has no receipt change record for that selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceResult {
    pub source: NativeSnapshotRef,
    pub table: TablePin,
    pub changes: Option<ChangePin>,
}
