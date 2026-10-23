//! Operation specifications, schema edits, backfills, and `OperationRequest`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{
    Id, NativeSnapshotRef, ObjectRef, OperationId, RefreshSpec, ReleasePin, SourceRelationship,
};

fn replacement_inputs(
    replacements: &InputSpec,
    deletions: &Option<InputSpec>,
) -> BTreeMap<String, InputSpec> {
    let mut inputs = BTreeMap::from([("replacements".into(), replacements.clone())]);
    if let Some(input) = deletions {
        inputs.insert("deletions".into(), input.clone());
    }
    inputs
}

/// Partition transform applied to a source column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionTransformSpec {
    Identity,
    Bucket { num_buckets: u32 },
    Year,
    Month,
    Day,
    Truncate { width: u32 },
}

/// One declared partition field on a dataset's source table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionFieldSpec {
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub transform: PartitionTransformSpec,
}

/// A reproducible local run, with trusted expected digests for supplied inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    pub dataset: Id,
    pub operation: OperationId,
    /// Exact selected version to rebuild, preserving its computed group definitions.
    #[serde(default)]
    pub base: Option<Id>,
    /// Omission inherits the exact base, or declares a new source independent.
    /// Changing an existing relationship requires its selected base.
    #[serde(default)]
    pub relationship: Option<SourceRelationship>,
    pub input: String,
    pub expected_digest: String,
    pub sql: String,
    pub definition: String,
    /// Optional Iceberg partition layout for the source table.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<PartitionFieldSpec>,
}

/// A selected local input with an expected immutable byte identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub path: String,
    pub digest: String,
}

/// Column-level merge strategy when updating existing keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateMergeStrategy {
    /// Replace with incoming value when incoming wins, else keep prior value.
    #[default]
    Replace,
    /// Take non-null incoming value when incoming wins (falling back to prior),
    /// or keep non-null prior value when prior wins (backfilling from incoming).
    LatestOrBackfill,
    /// Keep the minimum non-null value across prior and incoming.
    Min,
    /// Keep the maximum non-null value across prior and incoming.
    Max,
    /// Deduplicated list union in canonical ascending element order.
    SetUnion,
}

impl UpdateMergeStrategy {
    fn from_kind_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "replace" => Ok(Self::Replace),
            "latest_or_backfill" => Ok(Self::LatestOrBackfill),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "set_union" => Ok(Self::SetUnion),
            other => Err(format!("unknown update merge strategy '{other}'")),
        }
    }
}

impl<'de> Deserialize<'de> for UpdateMergeStrategy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TaggedStrategy {
            #[serde(alias = "strategy")]
            kind: String,
        }

        let raw = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(s) = raw {
            return Self::from_kind_str(&s).map_err(serde::de::Error::custom);
        }
        let tagged: TaggedStrategy =
            serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        Self::from_kind_str(&tagged.kind).map_err(serde::de::Error::custom)
    }
}

/// Declarative column-level merge rules for keyed updates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateMergeSpec {
    /// Optional ordering column determining whether incoming or prior row is newer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<String>,
    /// Per-column merge strategies; omitted non-key columns default to `Replace`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, UpdateMergeStrategy>,
}

/// Complete replacement rows and deletion keys against an exact selected version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub definition: String,
    pub base: Id,
    pub keys: Vec<String>,
    pub replacements: InputSpec,
    /// Omission retains the base relationship; a supplied value replaces it.
    #[serde(default)]
    pub relationship: Option<SourceRelationship>,
    pub deletions: Option<InputSpec>,
    /// Optional column-level merge rules when replacement keys match existing rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge: Option<UpdateMergeSpec>,
}

/// Selects an existing root field's identity and current display name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
}

/// Scalar types qualified for metadata-only source additions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Boolean,
    Long,
    Double,
    String,
}

/// Adds a nullable field; its literal applies to old absence and future omission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnAddition {
    pub name: String,
    pub data_type: ColumnType,
    /// Null when omitted. Explicitly supplied nulls never take this default.
    #[serde(default)]
    pub default: serde_json::Value,
}

/// Edits source fields and defaults without rewriting existing values.
///
/// `fields` lists the complete new root order; omitted fields remain in retained
/// old versions. Additions follow this order and receive fresh native IDs. Type
/// changes and nested edits require other operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub base: Id,
    /// Expected active writer definition while preparing this change.
    pub definition: String,
    /// Writer definition selected atomically with the new schema.
    pub next_definition: String,
    pub fields: Vec<SchemaField>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additions: Vec<ColumnAddition>,
    /// Changes future omission rules by field ID, without changing old values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub write_defaults: BTreeMap<i32, serde_json::Value>,
}

/// A stable field identity and a row-local SQL expression over source values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedField {
    /// Stable logical field ID within its group, independent of native table numbering.
    pub id: i32,
    pub name: String,
    pub expression: String,
}

/// Independently computes a field group against a retained source version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub base: Id,
    /// Required active source definition.
    pub definition: String,
    pub group: Id,
    pub group_definition: String,
    pub fields: Vec<ComputedField>,
}

/// One recoverable scalar-field change while source versions continue advancing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousBackfillSpec {
    /// The operation identifies the parent intent; its base fixes original meaning.
    pub backfill: BackfillSpec,
    /// Attempts before temporarily fencing new source admissions, from 1 through 16.
    pub optimistic_rounds: u32,
    /// Maximum local fence lifetime, from 1 through 300000 milliseconds.
    pub fence_millis: u64,
}

/// Bounded control progress; child phases retain the actual processing evidence.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuousBackfillPlan {
    pub completed: Option<ReleasePin>,
    pub child: Option<Id>,
    pub round: u32,
    pub pending_source: Vec<Id>,
    pub fenced: bool,
}

/// Material scalar computation, independent of its adoption operation and captured source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupDefinition {
    pub group: Id,
    pub definition: String,
    pub fields: Vec<ComputedField>,
}

/// Records omitted source-field dependencies for one exact scalar definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyCorrection {
    pub dataset: Id,
    /// Stable retry identity; changed content requires a new identifier.
    pub id: Id,
    pub definition: GroupDefinition,
    /// Positive stable source-field IDs, separate from the original SQL bindings.
    pub fields: std::collections::BTreeSet<i32>,
    pub reason: String,
}

/// Immutable acknowledgment of a correction and its actual native commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectionDecision {
    pub request: DependencyCorrection,
    /// Exact scalar implementation whose material dependency scope was corrected.
    pub processor: String,
    /// Native lifecycle revision that atomically retained this decision and requirements.
    pub revision: i64,
}

impl BackfillSpec {
    /// Returns the material definition reused across source refreshes.
    pub fn group_definition(&self) -> GroupDefinition {
        GroupDefinition {
            group: self.group.clone(),
            definition: self.group_definition.clone(),
            fields: self.fields.clone(),
        }
    }
}

/// Exact source and expression binding for a completely covered field group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputedGroup {
    /// Immutable original file executions; absence means unknown historical origins.
    #[serde(default)]
    pub origins: Option<ObjectRef>,
    /// Immutable supplemental correspondence; original SQL bindings stay unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualification: Option<Box<DependencyQualification>>,
    pub definition: GroupDefinition,
    /// SQL input names bound to root field IDs in this exact source interpretation.
    pub input_fields: BTreeMap<String, i32>,
    /// Operation establishing complete coverage; reused files retain their original identities.
    pub request: OperationRequest,
    pub native_field_ids: BTreeMap<i32, i32>,
    pub source_metadata: ObjectRef,
    pub base_manifest: ObjectRef,
}

/// Retained correspondence between original execution context and selected source fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyQualification {
    pub checker: String,
    /// Digest-addressed scope of the exact scalar processor and group definition.
    pub scope: String,
    pub fields: std::collections::BTreeSet<i32>,
    /// Original file acknowledgments must precede this dataset lifecycle revision.
    pub first_revision: i64,
    pub origins: ObjectRef,
    pub target: NativeSnapshotRef,
}

/// A typed request sharing the same recovery and guarded selection path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "spec", rename_all = "snake_case")]
pub enum OperationRequest {
    Build(RunSpec),
    Update(UpdateSpec),
    Backfill(BackfillSpec),
    Refresh(Box<RefreshSpec>),
    Schema(SchemaSpec),
    Evidence(crate::evidence::EvidenceSpec),
}

impl OperationRequest {
    /// Returns the dataset whose authority selects this operation.
    pub fn dataset(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.dataset,
            Self::Update(spec) => &spec.dataset,
            Self::Backfill(spec) => &spec.dataset,
            Self::Refresh(spec) => &spec.dataset,
            Self::Schema(spec) => &spec.dataset,
            Self::Evidence(spec) => &spec.dataset,
        }
    }
    /// Returns the stable retry identity.
    pub fn operation(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.operation,
            Self::Update(spec) => &spec.operation,
            Self::Backfill(spec) => &spec.operation,
            Self::Refresh(spec) => &spec.operation,
            Self::Schema(spec) => &spec.operation,
            Self::Evidence(spec) => &spec.operation,
        }
    }
    /// Returns the required active definition.
    pub fn definition(&self) -> &str {
        match self {
            Self::Build(spec) => &spec.definition,
            Self::Update(spec) => &spec.definition,
            Self::Backfill(spec) => &spec.definition,
            Self::Refresh(spec) => &spec.definition,
            Self::Schema(spec) => &spec.definition,
            Self::Evidence(spec) => &spec.definition,
        }
    }
    /// Returns the writer definition selected with this operation's result.
    pub fn result_definition(&self) -> &str {
        match self {
            Self::Schema(spec) => &spec.next_definition,
            _ => self.definition(),
        }
    }
    /// Returns the selected version whose composition this operation captures.
    pub fn base(&self) -> Option<&Id> {
        match self {
            Self::Build(spec) => spec.base.as_ref(),
            Self::Update(spec) => Some(&spec.base),
            Self::Backfill(spec) => Some(&spec.base),
            Self::Refresh(spec) => spec.base.as_ref(),
            Self::Schema(spec) => Some(&spec.base),
            Self::Evidence(spec) => Some(&spec.base),
        }
    }
    /// Lists bounded input declarations, excluding their payloads.
    pub fn inputs(&self) -> BTreeMap<String, InputSpec> {
        match self {
            Self::Build(spec) => BTreeMap::from([(
                "input".into(),
                InputSpec {
                    path: spec.input.clone(),
                    digest: spec.expected_digest.clone(),
                },
            )]),
            Self::Update(spec) => replacement_inputs(&spec.replacements, &spec.deletions),
            Self::Backfill(_) | Self::Refresh(_) | Self::Schema(_) => BTreeMap::new(),
            Self::Evidence(spec) => replacement_inputs(&spec.replacements, &spec.deletions),
        }
    }

    /// Returns the retained input name for this operation's provenance.
    pub(crate) fn input_key(&self, name: &str) -> String {
        match self {
            Self::Build(_) => name.to_owned(),
            Self::Update(_)
            | Self::Backfill(_)
            | Self::Refresh(_)
            | Self::Schema(_)
            | Self::Evidence(_) => {
                format!("{}/{name}", self.operation())
            }
        }
    }
}

#[cfg(test)]
mod unknown_field_tests {
    use super::*;

    fn update() -> serde_json::Value {
        serde_json::json!({
            "dataset": "news_captures",
            "operation": "append-1",
            "definition": "news-v1",
            "base": "admit-1",
            "keys": ["id"],
            "replacements": {"path": "replacements.parquet", "digest": "a".repeat(64)},
        })
    }

    #[test]
    fn update_rejects_unknown_fields_instead_of_dropping_them() {
        let mut value = update();
        value["unacknowledged"] = serde_json::json!("must refuse");
        let error = serde_json::from_value::<UpdateSpec>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    #[test]
    fn update_relationship_is_an_explicit_optional_field() {
        let spec: UpdateSpec = serde_json::from_value(update()).unwrap();
        assert!(spec.relationship.is_none());

        let mut value = update();
        value["relationship"] = serde_json::json!({
            "kind": "coupled", "family": "news", "generation": "g2"
        });
        let spec: UpdateSpec = serde_json::from_value(value).unwrap();
        assert_eq!(
            spec.relationship,
            Some(SourceRelationship::Coupled {
                family: Id::new("news").unwrap(),
                generation: Id::new("g2").unwrap(),
            })
        );
    }

    #[test]
    fn run_spec_rejects_unknown_fields() {
        let value = serde_json::json!({
            "dataset": "news_captures",
            "operation": "admit-1",
            "input": "source.parquet",
            "expected_digest": "a".repeat(64),
            "sql": "SELECT * FROM input",
            "definition": "news-v1",
            "unexpected": true,
        });
        assert!(serde_json::from_value::<RunSpec>(value).is_err());
    }
}
