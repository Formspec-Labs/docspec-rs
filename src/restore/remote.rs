//! Portable catalog-loss points retaining an exact immutable object closure.
// Rust guideline compliant 2026-02-21

use super::*;

/// A catalog-loss restore point. Objects remain at their protected immutable URIs.
/// This is not a backup against object loss or an online catalog handoff.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteRestorePoint {
    pub format: u32,
    pub id: Id,
    pub dataset: Id,
    pub generation: uuid::Uuid,
    pub retained_prefix: String,
    pub catalog: ObjectRef,
    pub objects: Vec<ObjectRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogImage {
    pub format: u32,
    pub generation: uuid::Uuid,
    pub namespaces: BTreeMap<String, BTreeMap<String, String>>,
    pub tables: Vec<Registration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Registration {
    pub table: String,
    pub table_uuid: String,
    pub metadata: ObjectRef,
    pub revision: i64,
    /// Exact captured authority; only lifecycle tables carry this fence input.
    pub authority: Option<RestoreAuthority>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestoreAuthority {
    pub epoch: u64,
    pub digest: String,
}

/// A fresh configured generation whose native registration image was verified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredRemoteCatalog {
    pub point: Id,
    pub generation: uuid::Uuid,
    pub tables: usize,
    pub retained_bytes: u64,
}

pub(crate) const FORMAT: u32 = 2;

pub(crate) fn validate(reference: &RestorePointRef, point: &RemoteRestorePoint) -> Result<()> {
    let prefix = format!("{}/{}/", point.retained_prefix, point.dataset);
    let attempt = reference
        .manifest
        .path
        .strip_prefix(&prefix)
        .and_then(|path| path.strip_suffix("/point.json"))
        .and_then(|name| name.strip_prefix("restore_"))
        .ok_or_else(|| invalid("portable restore attempt differs"))?;
    if uuid::Uuid::parse_str(attempt)
        .ok()
        .map(|id| id.simple().to_string())
        .as_deref()
        != Some(attempt)
    {
        return Err(invalid("portable restore attempt is not canonical"));
    }
    if point.format != FORMAT
        || reference.dataset != point.dataset
        || reference.id != point.id
        || reference.manifest.owner != point.dataset
        || point.catalog.owner != point.dataset
        || !reference.manifest.path.starts_with(&prefix)
        || reference.manifest.path.strip_suffix("/point.json")
            != point.catalog.path.strip_suffix("/catalog.json")
        || !reference.manifest.path.ends_with("/point.json")
        || !point.catalog.path.ends_with("/catalog.json")
        || !point
            .objects
            .windows(2)
            .all(|pair| pair[0].path < pair[1].path)
    {
        return Err(invalid("portable restore identity differs"));
    }
    Ok(())
}
