//! Versioned identities, dataset/release pins, and lifecycle state records.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{
    ChangePin, ComputedGroup, OperationRequest, PartitionFieldSpec, RelatedResult, WriteMetrics,
};

/// A validated identifier, safe in catalog names and workspace paths.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Id(String);

impl Id {
    /// Validates a bounded ASCII identifier.
    ///
    /// # Errors
    /// Refuses empty, long, traversal, or non-identifier values.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Error::refusal(
                "invalid_id",
                "expected 1–128 ASCII letters, digits, underscores or hyphens",
            ));
        }
        Ok(Self(value))
    }
    /// Returns the exact identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Stable retry identity; conflicting requests cannot reuse an operation.
pub type OperationId = Id;

/// An explicit decision to stop an operation while retaining its completed work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Abandonment {
    pub dataset: Id,
    pub operation: OperationId,
    pub request_digest: String,
    /// Revision of the original admission, not the abandonment commit.
    pub operation_revision: i64,
    pub reason: String,
}

/// An immutable object bound to its protection owner and exact bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub owner: Id,
    pub path: String,
    pub digest: String,
    pub bytes: u64,
}

/// An independently readable, exact native snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePin {
    pub table: String,
    pub metadata: ObjectRef,
    pub snapshot: Option<i64>,
    pub schema_id: i32,
    pub objects: Vec<ObjectRef>,
    pub rows: u64,
    /// Original logical state when this pin is a verified equivalent representation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_metadata: Option<ObjectRef>,
}

impl TablePin {
    /// Returns the original metadata identity of this unchanged logical state.
    pub fn logical_metadata(&self) -> &ObjectRef {
        self.logical_metadata.as_ref().unwrap_or(&self.metadata)
    }
}

/// Rewrites selected live files while preserving their logical group state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionSpec {
    pub dataset: Id,
    pub operation: Id,
    pub version: Id,
    pub group: String,
    /// Empty selects every live data file; supplied paths must be distinct and current.
    #[serde(default)]
    pub files: Vec<String>,
    pub target_file_bytes: usize,
}

/// Exact equivalent physical representation and measured rewrite sizes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionResult {
    pub spec: CompactionSpec,
    pub source: TablePin,
    pub replacement: TablePin,
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub rows: u64,
    pub verified: bool,
}

/// Exact native interpretation without copying the table's physical object inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSnapshotRef {
    pub metadata: ObjectRef,
    pub snapshot: Option<i64>,
    pub schema_id: i32,
}

impl From<&TablePin> for NativeSnapshotRef {
    fn from(table: &TablePin) -> Self {
        Self {
            metadata: table.metadata.clone(),
            snapshot: table.snapshot,
            schema_id: table.schema_id,
        }
    }
}

/// A complete data, schema, evidence, and input-coverage selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPin {
    pub format: u32,
    pub dataset: Id,
    pub version: Id,
    pub groups: BTreeMap<String, TablePin>,
    pub computed: BTreeMap<String, ComputedGroup>,
    pub inputs: BTreeMap<String, ObjectRef>,
    /// Producer-declared relationship retained with this exact version.
    pub relationship: SourceRelationship,
    /// Exact related inputs and contributor relation establishing this source table.
    pub related: Option<Box<RelatedResult>>,
    /// Opaque receipt values associated with exact surviving source occurrences.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<Box<crate::evidence::EvidenceResult>>,
    pub definition: String,
    pub eligibility: u64,
    pub boundary: u64,
    pub request: OperationRequest,
    /// Exact selected combination replaced or extended, including concurrent sibling groups.
    pub parent_manifest: Option<ObjectRef>,
    pub processor: String,
    /// Validation scope completed before selection. Older releases meet at least routine scope.
    #[serde(default)]
    pub validation: VerificationLevel,
    /// Active partition layout for the source group, empty when unpartitioned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<PartitionFieldSpec>,
}

/// An immutable consumer release selected through the lifecycle table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePin {
    pub id: Id,
    pub dataset: DatasetPin,
    pub manifest: ObjectRef,
    /// Routine validation/readback completed; this does not certify every payload value.
    pub verified: bool,
}

impl ReleasePin {
    /// Returns the exact identity used for accepted-history checks.
    pub fn version_ref(&self) -> VersionRef {
        VersionRef {
            dataset: self.dataset.dataset.clone(),
            version: self.id.clone(),
            manifest: self.manifest.clone(),
        }
    }
}

/// The amount of payload work requested by an independent verifier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationLevel {
    /// Check references, schemas, sizes, footer counts and a bounded row sample.
    #[default]
    Routine,
    /// Also hash payloads and reconstruct complete derived values and evidence.
    Deep,
}

/// Reports the scope of completed checks; routine counts come from metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub level: VerificationLevel,
    pub tables_checked: usize,
    /// Unique files whose footers were inspected; not collected by the deep verifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_files_checked: Option<usize>,
    pub rows_from_metadata: u64,
    pub sampled_files: usize,
    pub sampled_rows: usize,
    pub sampled_decoded_bytes: usize,
}

/// Exact identity of one accepted release, without embedding its payload inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRef {
    pub dataset: Id,
    pub version: Id,
    pub manifest: ObjectRef,
}

/// Producer-declared admissibility of a version alongside related datasets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceRelationship {
    /// This version imposes no cross-dataset generation requirement.
    #[default]
    Independent,
    /// Consumed members of this family must match one sealed generation.
    Coupled { family: Id, generation: Id },
}

/// Create-once producer selection of exact members belonging to one generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationSeal {
    pub family: Id,
    pub generation: Id,
    pub members: BTreeMap<Id, VersionRef>,
}

/// Exact consumed versions and the producer seals required to combine them.
///
/// Retaining this metadata permits independent relationship checks. It does not
/// promise payload availability or certify current eligibility after capture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputCoverage {
    pub inputs: BTreeMap<Id, VersionRef>,
    /// Immutable member set established by each family's first seal.
    pub families: BTreeMap<Id, std::collections::BTreeSet<Id>>,
    pub generations: BTreeMap<Id, GenerationSeal>,
}

/// Accepted predecessor, committed atomically with the selected result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Succession {
    pub selected: VersionRef,
    pub predecessor: Option<VersionRef>,
    /// Descriptive source admission boundary; ordering comes from predecessor links.
    pub boundary: u64,
    pub eligibility: u64,
}

/// Evidence that target coverage follows prior coverage on the observed selected chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessionProof {
    pub prior: VersionRef,
    pub target: VersionRef,
    pub observed_head: VersionRef,
    pub observed_revision: i64,
    /// Accepted selections between prior and target, including composition-only changes.
    pub selections_advanced: usize,
    /// Accepted selections after target at this observation, not elapsed time or acquisition lag.
    pub selections_behind: usize,
}

/// An expensive completed phase retained independently of its parent operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub operation: OperationId,
    pub phase: String,
    pub request_digest: String,
    pub eligibility: u64,
    pub outputs: Vec<ObjectRef>,
    pub table: Option<TablePin>,
    pub rows: u64,
    pub changes: Option<ChangePin>,
    pub coverage: Option<InputCoverage>,
    pub metrics: WriteMetrics,
    /// Exact retained dependencies used by this completion. Absence marks an older
    /// record whose recovery dependencies cannot authorize cleanup by inference.
    #[serde(default)]
    pub dependencies: Option<PhaseDependencies>,
}

/// Small immutable references needed to recover or reuse a completed phase.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseDependencies {
    /// Equivalent related-input sources actually consumed by this completion.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_sources: BTreeMap<Id, TablePin>,
    /// Input names before the operation-specific release prefix is applied.
    pub admitted_inputs: BTreeMap<String, ObjectRef>,
    /// Selected schema/history consumed while preparing this phase.
    pub base_manifest: Option<ObjectRef>,
    /// Metadata consumed for schema/default interpretation, without its old data rows.
    pub schema_metadata: Vec<ObjectRef>,
    /// Native tables consumed by later processing or validation.
    pub tables: Vec<NativeSnapshotRef>,
}

/// Mutable eligibility and coverage facts checked by the same lifecycle commit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub definition: String,
    pub eligibility: u64,
    pub epoch: u64,
    pub boundary: u64,
    pub selected: Option<DatasetPin>,
    pub release: Option<ReleasePin>,
    /// Native revision that selected this release, preserved across later admissions.
    pub selection_revision: Option<i64>,
}

/// Objects protected by an owner before a consumer can promise their availability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Protection {
    pub id: Id,
    pub consumer: Id,
    pub purpose: String,
    pub paths: Vec<String>,
    pub retired: bool,
}
