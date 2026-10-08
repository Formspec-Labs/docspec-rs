//! Versioned identities and lifecycle records; no storage or execution dependencies.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
}

/// A complete data, schema, evidence, and input-coverage selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPin {
    pub format: u32,
    pub dataset: Id,
    pub version: Id,
    pub groups: BTreeMap<String, TablePin>,
    pub inputs: BTreeMap<String, ObjectRef>,
    pub definition: String,
    pub eligibility: u64,
    pub boundary: u64,
    pub request: OperationRequest,
    pub changes: Option<ChangePin>,
    pub processor: String,
}

/// An immutable consumer release selected through the lifecycle table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePin {
    pub id: Id,
    pub dataset: DatasetPin,
    pub manifest: ObjectRef,
    pub verified: bool,
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
    pub metrics: WriteMetrics,
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

/// Full multiset comparison of one field group; duplicate observations count separately.
#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub same_schema: bool,
    pub same_inputs: bool,
    pub added_rows: Option<u64>,
    pub removed_rows: Option<u64>,
}

/// A reproducible local run, with trusted expected digests for supplied inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub input: String,
    pub expected_digest: String,
    pub sql: String,
    pub definition: String,
}

/// A selected local input with an expected immutable byte identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSpec {
    pub path: String,
    pub digest: String,
}

/// Complete replacement rows and deletion keys against an exact selected version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSpec {
    pub dataset: Id,
    pub operation: OperationId,
    pub definition: String,
    pub base: Id,
    pub keys: Vec<String>,
    pub replacements: InputSpec,
    pub deletions: Option<InputSpec>,
}

/// A typed request sharing the same recovery and guarded selection path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "spec", rename_all = "snake_case")]
pub enum OperationRequest {
    Build(RunSpec),
    Update(UpdateSpec),
}

impl OperationRequest {
    /// Returns the dataset whose authority selects this operation.
    pub fn dataset(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.dataset,
            Self::Update(spec) => &spec.dataset,
        }
    }
    /// Returns the stable retry identity.
    pub fn operation(&self) -> &Id {
        match self {
            Self::Build(spec) => &spec.operation,
            Self::Update(spec) => &spec.operation,
        }
    }
    /// Returns the required active definition.
    pub fn definition(&self) -> &str {
        match self {
            Self::Build(spec) => &spec.definition,
            Self::Update(spec) => &spec.definition,
        }
    }
    /// Returns the version that an update is allowed to replace.
    pub fn base(&self) -> Option<&Id> {
        match self {
            Self::Build(_) => None,
            Self::Update(spec) => Some(&spec.base),
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
            Self::Update(spec) => {
                let mut inputs =
                    BTreeMap::from([("replacements".into(), spec.replacements.clone())]);
                if let Some(input) = &spec.deletions {
                    inputs.insert("deletions".into(), input.clone());
                }
                inputs
            }
        }
    }
}

/// Typed before/after rows from applied writes, selected with their exact result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePin {
    pub base_version: Id,
    pub base_manifest: ObjectRef,
    pub before_metadata: ObjectRef,
    pub after_metadata: ObjectRef,
    pub key_field_ids: Vec<i32>,
    pub removed: TablePin,
    pub added: TablePin,
}

/// Counts observed while discovering and writing a candidate; no reporting scan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteMetrics {
    pub files_scanned: u64,
    pub files_reused: u64,
    pub data_files_written: u64,
    pub bytes_reused: u64,
    pub bytes_written: u64,
    pub rows_removed: u64,
    pub rows_added: u64,
}
