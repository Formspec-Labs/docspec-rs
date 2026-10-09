//! Explicit local restore promises and durable native catalog selection.
//!
//! Restore points retain exact immutable files in the original workspace. They
//! are not portable backups. Iceberg lifecycle rows advertise and retire points;
//! immutable backup artifacts carry the bytes needed to restore an older native
//! catalog without relying on files that later cleanup may have unlinked.
// Rust guideline compliant 2026-02-21

use crate::{
    Error, Result,
    model::{Id, ObjectRef, RetentionBoundary},
    storage,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};

/// A completed native catalog image and its exact retained object dependencies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestorePoint {
    pub format: u32,
    pub id: Id,
    pub dataset: Id,
    pub workspace: String,
    pub catalog: ObjectRef,
    pub boundaries: Vec<RetentionBoundary>,
    pub objects: Vec<RestoreObject>,
    pub linked_bytes: u64,
    pub copied_bytes: u64,
    /// Logical mapping bytes supplied by an already retained backup identity.
    #[serde(default)]
    pub reused_bytes: u64,
}

/// An original immutable identity and its independently retained backup path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreObject {
    pub original: ObjectRef,
    pub backup: ObjectRef,
}

/// The exact immutable restore artifact advertised by one lifecycle record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestorePointRef {
    pub dataset: Id,
    pub id: Id,
    pub manifest: ObjectRef,
}

/// Typed creation evidence retained after a restore promise ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreMaterialized {
    pub reference: RestorePointRef,
    pub catalog: ObjectRef,
    pub backups: Vec<ObjectRef>,
}

/// Exact shared backup dependencies protected while one restore capture is unfinished.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreReuse {
    pub dataset: Id,
    pub id: Id,
    pub objects: Vec<ObjectRef>,
}

/// The fresh native catalog selected by a completed local restore.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredCatalog {
    pub point: Id,
    pub generation: Id,
    pub path: String,
    pub restored_bytes: u64,
    pub reused_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogSelection {
    format: u32,
    restored: RestoredCatalog,
}

// This bounds the small startup pointer before any engine budget exists.
const SELECTOR_BYTES: usize = 4096;
pub(crate) const FORMAT: u32 = 1;

pub(crate) fn invalid(message: &'static str) -> Error {
    Error::refusal("invalid_restore_point", message)
}

/// Resolves the selected native SQLite file without creating or mutating it.
///
/// # Errors
/// Refuses malformed selectors, escaped paths, symlinks, or missing selected files.
pub(crate) fn catalog_path(root: &Path) -> Result<PathBuf> {
    let selector = root.join("catalog-current.json");
    if !selector.try_exists()? {
        return Ok(root.join("catalog.sqlite"));
    }
    let selection: CatalogSelection = read_json(&selector, SELECTOR_BYTES)?;
    let expected = root
        .join("catalogs")
        .join(format!("{}.sqlite", selection.restored.generation));
    if selection.format != FORMAT || Path::new(&selection.restored.path) != expected {
        return Err(invalid("native catalog selector identity differs"));
    }
    regular_owned_file(root, &expected)?;
    Ok(expected)
}

pub(crate) fn select_catalog(root: &Path, restored: &RestoredCatalog) -> Result<()> {
    let expected = root
        .join("catalogs")
        .join(format!("{}.sqlite", restored.generation));
    if Path::new(&restored.path) != expected {
        return Err(invalid("restored catalog path differs from its generation"));
    }
    regular_owned_file(root, &expected)?;
    let selection = CatalogSelection {
        format: FORMAT,
        restored: restored.clone(),
    };
    atomic_json(
        &root.join("catalog-current.json"),
        &selection,
        SELECTOR_BYTES,
        true,
    )
}

pub(crate) fn read_point(
    root: &Path,
    reference: &RestorePointRef,
    limit: usize,
) -> Result<RestorePoint> {
    if reference.manifest.bytes > limit as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "restore manifest exceeds its metadata allowance",
        ));
    }
    let path = Path::new(&reference.manifest.path);
    regular_owned_file(root, path)?;
    storage::verify_object(&reference.manifest)?;
    let point: RestorePoint = read_json(path, limit)?;
    validate_point(root, reference, &point)?;
    Ok(point)
}

pub(crate) fn validate_point(
    root: &Path,
    reference: &RestorePointRef,
    point: &RestorePoint,
) -> Result<()> {
    if point.format != FORMAT
        || point.id != reference.id
        || point.dataset != reference.dataset
        || reference.manifest.owner != point.dataset
        || Path::new(&point.workspace) != root
    {
        return Err(invalid(
            "restore point belongs to another identity, format, or workspace",
        ));
    }
    let catalog = Path::new(&point.catalog.path);
    let manifest = Path::new(&reference.manifest.path);
    let directory = manifest
        .parent()
        .ok_or_else(|| invalid("restore manifest has no parent"))?;
    let owned = root.join("objects").join(point.dataset.as_str());
    let relative = directory
        .strip_prefix(&owned)
        .map_err(|_| invalid("restore artifact escapes its owner"))?;
    let parts = relative.components().collect::<Vec<_>>();
    if catalog.parent() != Some(directory)
        || catalog.file_name().and_then(|value| value.to_str()) != Some("catalog.sqlite")
        || manifest.file_name().and_then(|value| value.to_str()) != Some("point.json")
        || parts.len() != 2
        || !matches!(parts[0], Component::Normal(_))
        || parts[1] != Component::Normal("restore".as_ref())
        || point.catalog.owner != point.dataset
    {
        return Err(invalid("restore catalog image identity differs"));
    }
    let mut objects = BTreeMap::new();
    for object in std::iter::once(&point.catalog)
        .chain(std::iter::once(&reference.manifest))
        .chain(
            point
                .objects
                .iter()
                .flat_map(|object| [&object.original, &object.backup]),
        )
    {
        if object.digest.len() != 64
            || !object.digest.bytes().all(|value| value.is_ascii_hexdigit())
        {
            return Err(invalid("restore object identities are invalid or repeated"));
        }
        if let Some(previous) = objects.insert(&object.path, object)
            && previous != object
        {
            return Err(invalid(
                "one restore path has conflicting immutable identities",
            ));
        }
        lexical_owned_path(root, Path::new(&object.path))?;
    }
    let mut retained_bytes = 0u64;
    let mut originals = std::collections::BTreeSet::new();
    for object in &point.objects {
        backup_path(root, &object.backup)?;
        if !originals.insert(&object.original.path)
            || object.backup.digest != object.original.digest
            || object.backup.bytes != object.original.bytes
            || !(Path::new(&object.original.path).starts_with(root.join("objects"))
                || Path::new(&object.original.path).starts_with(root.join("warehouse")))
        {
            return Err(invalid(
                "restore backup differs from its original immutable identity",
            ));
        }
        retained_bytes = retained_bytes
            .checked_add(object.backup.bytes)
            .ok_or_else(|| invalid("restore object sizes overflow"))?;
    }
    if point
        .linked_bytes
        .checked_add(point.copied_bytes)
        .and_then(|bytes| bytes.checked_add(point.reused_bytes))
        != Some(retained_bytes)
    {
        return Err(invalid("restore retained byte accounting differs"));
    }
    if !point
        .boundaries
        .windows(2)
        .all(|pair| pair[0].table < pair[1].table)
    {
        return Err(invalid(
            "restore catalog boundaries are not uniquely ordered",
        ));
    }
    for boundary in &point.boundaries {
        if !objects.contains_key(&boundary.metadata) || boundary.revision < 0 {
            return Err(invalid(
                "restore boundary lacks its retained native metadata",
            ));
        }
    }
    Ok(())
}

/// Checks the narrow immutable artifact namespace used by retained restore points.
pub(crate) fn backup_path(root: &Path, object: &ObjectRef) -> Result<()> {
    let path = Path::new(&object.path);
    let owned = root.join("objects").join(object.owner.as_str());
    let relative = path
        .strip_prefix(&owned)
        .map_err(|_| invalid("restore backup escapes its original owner"))?;
    let parts = relative.components().collect::<Vec<_>>();
    if parts.len() != 3
        || !matches!(parts[0], Component::Normal(_))
        || parts[1] != Component::Normal("restore".as_ref())
    {
        return Err(invalid(
            "restore backup is outside its immutable artifact namespace",
        ));
    }
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| invalid("restore backup filename is invalid"))?;
    if name != "catalog.sqlite"
        && name != "point.json"
        && !name.strip_prefix("object-").is_some_and(|index| {
            !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(invalid("restore backup filename is unsupported"));
    }
    lexical_owned_path(root, path)?;
    Ok(())
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid("restore metadata must be a regular file"));
    }
    if metadata.len() > limit as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "restore metadata exceeds its allowance",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(Error::refusal(
            "metadata_capacity",
            "restore metadata exceeds its allowance",
        ));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) fn atomic_json(
    path: &Path,
    value: &impl Serialize,
    limit: usize,
    replace: bool,
) -> Result<()> {
    let bytes = storage::bounded_json(value, limit)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("restore metadata has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{}.pending", uuid::Uuid::new_v4().simple()));
    storage::write_new(&temporary, &bytes)?;
    if replace {
        fs::rename(&temporary, path)?;
    } else {
        // Hard-link publication refuses an existing identity and exposes only
        // fully written bytes. A crash can leave an unreferenced temporary link.
        fs::hard_link(&temporary, path)?;
        fs::remove_file(&temporary)?;
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(crate) fn regular_owned_file(root: &Path, path: &Path) -> Result<()> {
    lexical_owned_path(root, path)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("restore object has no parent"))?;
    plain_directory(root, parent)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid("restore object is not a regular owned file"));
    }
    Ok(())
}

pub(crate) fn plain_directory(root: &Path, path: &Path) -> Result<()> {
    let relative = lexical_owned_path(root, path)?;
    let mut current = root.to_owned();
    for part in relative.components() {
        current.push(part);
        let metadata = fs::symlink_metadata(&current)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid("restore path traverses a symlink or non-directory"));
        }
    }
    Ok(())
}

fn lexical_owned_path<'a>(root: &Path, path: &'a Path) -> Result<&'a Path> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| invalid("restore object escapes its workspace"))?;
    if !root.is_absolute()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        || path
            .to_string_lossy()
            .split('/')
            .any(|part| part == "." || part == "..")
    {
        return Err(invalid("restore object path is not canonical"));
    }
    Ok(relative)
}
