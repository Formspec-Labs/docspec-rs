//! Retention plans, retirement receipts, physical removal intents, and comparison/change models.

use serde::{Deserialize, Serialize};

use super::{Id, ObjectRef, OperationId, TablePin, VersionRef};

/// Exact lifecycle snapshot observed during a read-only protection inspection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionBoundary {
    pub table: String,
    pub metadata: String,
    pub revision: i64,
}

/// One exact native generation of a retained lifecycle record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedGeneration {
    pub key: String,
    pub revision: i64,
}

/// The independent availability promise ended by a retirement decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetirementScope {
    PhaseRecovery,
    OptionalChanges,
    ResultAvailability {
        operation: OperationId,
    },
    OperationRecovery {
        operation: OperationId,
    },
    CompactionRecovery {
        operation: OperationId,
    },
    /// Recovery of one export operation, separate from immutable pin availability.
    ExportRecovery {
        operation: OperationId,
    },
    /// Standalone availability of one immutable producer generation seal.
    GenerationSeal {
        generation: Id,
    },
    OwnerGrants,
    RestorePoints,
}

/// Explicit retirement of retained promises, without deleting physical objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPlan {
    pub dataset: Id,
    pub retirement: Id,
    pub scope: RetirementScope,
    pub reason: String,
    pub table_uuid: String,
    pub metadata: String,
    pub snapshot_id: i64,
    pub revision: i64,
    pub records: Vec<RetainedGeneration>,
}

/// A durable retirement decision and its native lifecycle revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retirement {
    pub plan: RetentionPlan,
    pub revision: i64,
}

/// One retained record sufficient to protect a candidate object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionBlocker {
    pub table: String,
    pub key: String,
    pub revision: i64,
}

/// A candidate's first protection witness, not an exhaustive list of owners.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectProtection {
    pub object: ObjectRef,
    pub blocker: Option<RetentionBlocker>,
}

/// Read-only protection evidence; unmatched objects are not authorized for removal.
///
/// Each table has its own observed boundary. Concurrent work can add references
/// during or after inspection. Removal requires fresh eligibility, worker
/// exclusion and a durable exact intent, none of which this report supplies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInspection {
    pub boundaries: Vec<RetentionBoundary>,
    pub objects: Vec<ObjectProtection>,
    pub protected_bytes: u64,
    pub unmatched_bytes: u64,
    /// Unique release or native metadata files decoded, excluding lifecycle rows.
    pub metadata_objects_read: usize,
}

/// One exact owned native registration, detached without purging its files.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalRegistration {
    pub namespace: String,
    pub name: String,
    pub table_uuid: String,
    pub metadata: String,
}

/// Exact supported authority for a removal plan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemovalScope {
    /// Owned embedded-catalog inventory and native registrations.
    #[default]
    Objects,
    /// One original host stage whose verified remote completion is durable.
    ExportHostStage {
        operation: Id,
        operation_revision: i64,
        stage_revision: i64,
        completion_revision: i64,
        host_root: String,
    },
    /// One exactly created host child after terminal export recovery retirement.
    ExportHostFile {
        operation: Id,
        operation_revision: i64,
        file: Id,
        creation_revision: i64,
        host_root: String,
    },
}

impl RemovalScope {
    fn is_objects(&self) -> bool {
        matches!(self, Self::Objects)
    }
}

/// Bounded explicit removal candidates and their observed protection boundaries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalPlan {
    #[serde(default, skip_serializing_if = "RemovalScope::is_objects")]
    pub scope: RemovalScope,
    pub dataset: Id,
    pub removal: Id,
    pub reason: String,
    pub table_uuid: String,
    pub inspection: RetentionInspection,
    pub registrations: Vec<RemovalRegistration>,
}

/// Immutable removal scope chosen from a preview by fresh eligibility checks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalIntent {
    pub plan: RemovalPlan,
    pub revision: i64,
    /// Fresh inventory boundaries captured before reserving effects.
    pub boundaries: Vec<RetentionBoundary>,
    /// Originally eligible objects excluded by new protections.
    pub blocked: Vec<ObjectProtection>,
    /// Indices into the preview's candidates; no later attempt may expand these.
    pub objects: Vec<usize>,
    /// Indices into the preview's exact native registrations.
    pub registrations: Vec<usize>,
}

/// A durable effect acknowledgment; unresolved or failed effects may already have occurred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemovalOutcome {
    Unresolved,
    Removed,
    Absent,
    Failed { code: String },
}

impl RemovalOutcome {
    pub(crate) fn finished(&self) -> bool {
        matches!(self, Self::Removed | Self::Absent)
    }
}

/// Current acknowledgments and reserved capacity for an immutable removal intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalProgress {
    pub intent_revision: i64,
    /// Lifecycle replay-accounting bytes, not filesystem storage or memory usage.
    pub reserved_future_bytes: u64,
    /// Positions correspond to the intent's registration indices.
    pub registrations: Vec<RemovalOutcome>,
    /// Positions correspond to the intent's object indices.
    pub objects: Vec<RemovalOutcome>,
}

/// Exact saved removal decisions, acknowledgments and cumulative logical byte outcomes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovalStatus {
    pub intent: RemovalIntent,
    pub progress: RemovalProgress,
    pub complete: bool,
    /// Logical file sizes with acknowledged unlink, not reclaimed allocated blocks.
    pub confirmed_unlinked_bytes: u64,
    /// Logical file sizes reconciled absent; no new unlink is claimed for them.
    pub reconciled_absent_bytes: u64,
}

/// Equality used to cancel matching rows in a comparison.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonIdentity {
    /// Compare typed visible values and their multiplicities.
    Values,
    /// Also compare each row's stable occurrence address.
    #[default]
    Occurrences,
}

/// Exact-release values-only lookup for a bounded set of scalar keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyReadSpec {
    pub key: String,
    pub values: Vec<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

/// Selects comparison identity and an explicit full reconstruction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComparisonOptions {
    pub identity: ComparisonIdentity,
    pub force_full: bool,
}

/// The work actually selected for a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonPath {
    /// The exact retained field-group pin is shared by both endpoints.
    Unchanged,
    /// Compose qualified local applied writes from the exact before version.
    ForwardChanges,
    /// Reconstruct differences from both complete endpoint relations.
    Full,
}

/// Why the comparison selected full reconstruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonFallback {
    Forced,
    /// Optional local change evidence is absent or retired.
    HistoryNotQualified,
    /// Endpoints do not form a supported forward relationship.
    NotForward,
    /// This field group has no qualified applied-change reader.
    UnsupportedGroup,
    /// Optional history exceeds the declared link or cumulative metadata bound.
    HistoryLimit,
    /// Differing schemas cannot be compared through an implicit cast.
    SchemaChanged,
}

/// Exact metadata differences, independent of the requested row identity.
///
/// Changed references are conservative signals, not proof of unequal payload values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonEffects {
    pub schema_changed: bool,
    pub input_refs_changed: bool,
    pub coverage_changed: bool,
    pub definition_changed: bool,
    /// Source receipts, contributor references, or computed execution evidence/bindings changed.
    pub evidence_refs_changed: bool,
    pub relationship_changed: bool,
    /// Computed groups whose declared definitions were added, removed or changed.
    pub changed_group_definitions: Vec<String>,
}

/// Describes a streamed difference between two exact retained endpoints.
///
/// Arrow schema metadata `docspec.change` contains this JSON. Each output row has
/// `side` (before/after), `file`, `row`, and a typed `values` struct. The address is
/// an occurrence in the corresponding endpoint, not necessarily a current file
/// location after compaction. Value mode chooses deterministic representatives
/// of surplus equal values; it does not assert which occurrence disappeared.
/// Completeness requires successful end of stream. This is not a portable
/// applied-write certificate or a retained availability promise.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeDescriptor {
    pub format: u32,
    pub before: VersionRef,
    pub after: VersionRef,
    pub group: String,
    pub identity: ComparisonIdentity,
    pub path: ComparisonPath,
    pub fallback: Option<ComparisonFallback>,
    /// Native identifiers of the visible value fields in output order.
    pub field_ids: Vec<i32>,
    /// Whether visible field identities and types permit a comparison without casts.
    pub comparable_values: bool,
    /// Native declared identifier sets, sorted; empty establishes no key here.
    pub before_key_field_ids: Vec<i32>,
    pub after_key_field_ids: Vec<i32>,
    pub effects: ComparisonEffects,
}

/// Multiset comparison counts and the exact comparison semantics.
#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub descriptor: ChangeDescriptor,
    pub same_schema: bool,
    pub same_inputs: bool,
    pub added_rows: Option<u64>,
    pub removed_rows: Option<u64>,
}

/// Typed applied rows retained by a recovery phase or optional change descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePin {
    /// Qualified local production semantics; absent older claims are not certified.
    #[serde(default)]
    pub rules: String,
    pub base_version: Id,
    pub base_manifest: ObjectRef,
    pub before_metadata: ObjectRef,
    pub after_metadata: ObjectRef,
    pub key_field_ids: Vec<i32>,
    pub removed: TablePin,
    pub added: TablePin,
}

/// Applied changes stored independently of the complete result's required evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedChangeSet {
    pub format: u32,
    pub rules: String,
    pub operation: OperationId,
    pub before: VersionRef,
    pub source: Option<ChangePin>,
    pub receipts: Option<ChangePin>,
}

/// Local change qualification selected atomically with its exact result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeCertificate {
    pub format: u32,
    pub rules: String,
    pub before: VersionRef,
    pub after: VersionRef,
    pub descriptor: ObjectRef,
}

/// A captured optional certificate and its bounded typed description.
///
/// This metadata identifies retained evidence but creates no lasting protection.
/// Readers and consumers must acquire their own admission or retention promise.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertifiedChanges {
    pub revision: i64,
    pub certificate: ChangeCertificate,
    pub changes: AppliedChangeSet,
}

/// Counts observed while discovering and writing a candidate; no reporting scan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteMetrics {
    /// Native file decode passes executed (`files_probed + files_rewritten` for keyed
    /// replacements, where each affected file is decoded once for key probing and once
    /// for full-row rewriting).
    pub files_scanned: u64,
    pub files_reused: u64,
    pub data_files_written: u64,
    pub bytes_reused: u64,
    pub bytes_written: u64,
    pub rows_removed: u64,
    pub rows_added: u64,
    /// Candidate files whose key columns were decoded after metadata/zone-map pruning.
    #[serde(default)]
    pub candidate_files_probed: u64,
    /// Files skipped before key decoding via Iceberg column bounds or Parquet row-group statistics.
    #[serde(default)]
    pub files_skipped_by_pruning: u64,
}
