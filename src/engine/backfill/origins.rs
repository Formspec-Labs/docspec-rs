//! Immutable scalar declarations and canonical original file executions.

use super::*;

mod checks;
pub(in crate::engine) use checks::ScalarOriginChecks;

/// Borrowed source, output group, and compiled field-binding context for scalar verification.
#[derive(Clone, Copy)]
pub(in crate::engine) struct ScalarOriginBinding<'a> {
    pub(in crate::engine) source: &'a TablePin,
    pub(in crate::engine) group: &'a TablePin,
    pub(in crate::engine) definition: &'a GroupDefinition,
    pub(in crate::engine) input_fields: &'a BTreeMap<String, i32>,
    pub(in crate::engine) native_field_ids: &'a BTreeMap<i32, i32>,
}

/// The exact declaration compiled for newly computed files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct OriginalDeclaration {
    pub(in crate::engine) processor: String,
    pub(in crate::engine) definition: GroupDefinition,
    pub(in crate::engine) input_fields: BTreeMap<String, i32>,
}

/// The original operation and its shared immutable declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct OriginalExecution {
    pub(in crate::engine) operation: Id,
    pub(in crate::engine) declaration: ObjectRef,
}

/// A canonical completion with its actual retained native generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct FileOrigin {
    pub(in crate::engine) generation: RetainedGeneration,
    pub(in crate::engine) completion: FileCompletion,
}

/// Independently readable originals bound to the assembled source and output.
///
/// An empty file list is known empty; an absent descriptor remains unknown.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct ScalarOrigins {
    pub(in crate::engine) processor: String,
    pub(in crate::engine) definition: GroupDefinition,
    pub(in crate::engine) input_fields: BTreeMap<String, i32>,
    pub(in crate::engine) dependencies: GroupDependencies,
    pub(in crate::engine) files: Vec<FileOrigin>,
}

/// Writes bounded immutable metadata and hashes its already serialized bytes.
///
/// # Errors
/// Returns metadata capacity, serialization or storage errors.
#[cfg(test)]
pub(in crate::engine) fn write(
    owner: &Id,
    path: &Path,
    value: &impl Serialize,
    budget: &Budget,
) -> Result<ObjectRef> {
    let size = inputs::metadata_size(value, budget.limits.max_metadata_bytes)?;
    let _buffer = budget.reserve(size)?;
    let bytes = serde_json::to_vec(value)?;
    storage::write_new(path, &bytes)?;
    Ok(ObjectRef {
        owner: owner.clone(),
        path: path.to_string_lossy().into_owned(),
        digest: blake3::hash(&bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    })
}

pub(in crate::engine) async fn write_at(
    objects: &storage::Objects,
    owner: &Id,
    path: &locations::Location,
    value: &impl Serialize,
    budget: &Budget,
) -> Result<ObjectRef> {
    let size = inputs::metadata_size(value, budget.limits.max_metadata_bytes)?;
    let _buffer = budget.reserve(size)?;
    let bytes = serde_json::to_vec(value)?;
    let object = ObjectRef {
        owner: owner.clone(),
        path: path.as_string()?,
        digest: blake3::hash(&bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    };
    objects
        .create_candidate_metadata(&object, bytes, budget, false)
        .await?;
    Ok(object)
}

/// Reads bounded hash-bound metadata through the maintained manifest reader.
///
/// # Errors
/// Returns metadata capacity, object identity, decoding or storage errors.
pub(in crate::engine) async fn read<T: serde::de::DeserializeOwned + Send + 'static>(
    object: &ObjectRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<T> {
    let objects = storage::Objects::for_path(&object.path);
    inputs::read_manifest(&objects, object, budget, bytes, memory).await
}
