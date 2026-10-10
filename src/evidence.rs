//! Receipt values associated with exact, stable source occurrences.

use crate::model::{Id, InputSpec, NativeSnapshotRef, OperationId, TablePin};
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

/// A complete receipt table bound to the selected source.
///
/// This is separate from a join or aggregate's contributor evidence. A source
/// update removes receipts for replaced or deleted occurrences and preserves
/// receipts for every surviving occurrence. An unchanged receipt table retains
/// its exact pin. Optional applied-change history has a separate lifetime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "EvidenceResultFields")]
pub struct EvidenceResult {
    pub source: NativeSnapshotRef,
    pub table: TablePin,
}

// Older state formats embedded optional changes. Skip that member without
// allocating its tables or allowing it to become a qualified change claim.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceResultFields {
    source: NativeSnapshotRef,
    table: TablePin,
    #[serde(default, rename = "changes")]
    _changes: Option<serde::de::IgnoredAny>,
}

impl From<EvidenceResultFields> for EvidenceResult {
    fn from(fields: EvidenceResultFields) -> Self {
        Self {
            source: fields.source,
            table: fields.table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ObjectRef;

    fn result() -> EvidenceResult {
        let table = TablePin {
            table: "receipts".into(),
            metadata: ObjectRef {
                owner: Id::new("comments").unwrap(),
                path: "receipts.metadata.json".into(),
                digest: "0".repeat(64),
                bytes: 100,
            },
            snapshot: Some(1),
            schema_id: 0,
            objects: Vec::new(),
            rows: 0,
            logical_metadata: None,
        };
        EvidenceResult {
            source: (&table).into(),
            table,
        }
    }

    #[test]
    fn current_receipt_state_roundtrips_without_change_history() {
        let expected = result();
        let encoded = serde_json::to_value(&expected).unwrap();
        assert!(encoded.get("changes").is_none());
        assert_eq!(
            serde_json::from_value::<EvidenceResult>(encoded).unwrap(),
            expected
        );
    }

    #[test]
    fn legacy_change_history_is_ignored_without_becoming_state() {
        let expected = result();
        let history = serde_json::json!({
            "base_version": "previous",
            "base_manifest": &expected.table.metadata,
            "before_metadata": &expected.table.metadata,
            "after_metadata": &expected.table.metadata,
            "key_field_ids": [1, 2],
            "removed": &expected.table,
            "added": &expected.table,
        });
        for changes in [serde_json::Value::Null, history] {
            let mut legacy = serde_json::to_value(&expected).unwrap();
            legacy["changes"] = changes;
            let decoded: EvidenceResult = serde_json::from_value(legacy).unwrap();
            assert_eq!(decoded, expected);
            assert!(
                serde_json::to_value(decoded)
                    .unwrap()
                    .get("changes")
                    .is_none()
            );
        }
    }

    #[test]
    fn unrelated_unknown_receipt_fields_are_refused() {
        let mut encoded = serde_json::to_value(result()).unwrap();
        encoded["changes"] = serde_json::Value::Null;
        encoded["unknown_claim"] = serde_json::Value::Bool(true);
        let error = serde_json::from_value::<EvidenceResult>(encoded).unwrap_err();
        assert!(error.to_string().contains("unknown field `unknown_claim`"));
    }
}
