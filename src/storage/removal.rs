//! Exact local removal after the caller proves creation, ownership, and durable intent.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result, budget::Budget, model::ObjectRef};
use std::{
    fs::{File, Metadata},
    io::{ErrorKind, Read},
    path::{Component, Path, PathBuf},
};

/// The observed effect on one exact file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalRemovalOutcome {
    Deleted,
    Absent,
}

/// Removes one checked regular file and durably synchronizes its parent.
///
/// The caller must first prove typed creation and record exact removal intent.
/// Arbitrary simultaneous external filesystem mutation is unsupported: the final
/// identity checks do not provide an atomic conditional unlink. Completed retries
/// belong to the caller and must not invoke this adapter again.
///
/// # Errors
/// Refuses escaped paths, symlinks, nonregular targets, changed bytes or identity,
/// exhausted capacity, unsupported platforms, and filesystem failures. Filesystem
/// refusals use fixed codes and messages suitable for bounded retained outcomes.
pub(crate) async fn remove_owned_file(
    root: &Path,
    object: &ObjectRef,
    budget: &Budget,
) -> Result<LocalRemovalOutcome> {
    owned_worker(root, object, budget, remove_file).await
}

/// Reconciles durable absence without hashing or unlinking a present target.
///
/// # Errors
/// Refuses unsafe paths, symlinks, nonregular targets, exhausted capacity, or
/// filesystem failures. True means absence and parent synchronization succeeded.
pub(crate) async fn reconcile_absent_file(
    root: &Path,
    object: &ObjectRef,
    budget: &Budget,
) -> Result<bool> {
    owned_worker(root, object, budget, reconcile_absence).await
}

async fn owned_worker<T: Send + 'static>(
    root: &Path,
    object: &ObjectRef,
    budget: &Budget,
    effect: fn(&Path, &ObjectRef) -> Result<T>,
) -> Result<T> {
    let bytes = std::mem::size_of::<ObjectRef>()
        .saturating_add(std::mem::size_of::<PathBuf>())
        .saturating_add(root.as_os_str().len())
        .saturating_add(object.path.len())
        .saturating_add(object.digest.len())
        .saturating_add(object.owner.as_str().len());
    if bytes > budget.limits.max_metadata_bytes {
        return Err(Error::refusal(
            "metadata_capacity",
            "removal identity exceeds metadata allowance",
        ));
    }
    let inventory = budget.reserve(bytes)?;
    let root = root.to_owned();
    let object = object.clone();
    budget
        .blocking(move || {
            let _inventory = inventory;
            effect(&root, &object)
        })
        .await
}

fn checked_path<'a>(root: &Path, object: &'a ObjectRef) -> Result<&'a Path> {
    let path = Path::new(&object.path);
    let relative = path.strip_prefix(root).map_err(|_| invalid_path())?;
    let mut parts = relative.components();
    let objects = parts.next();
    let owner = parts.next();
    let attempt = parts.next();
    let file = parts.next();
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
        || object
            .path
            .split(std::path::MAIN_SEPARATOR)
            .any(|part| part == "." || part == "..")
        || objects != Some(Component::Normal("objects".as_ref()))
        || owner != Some(Component::Normal(object.owner.as_str().as_ref()))
        || !matches!(attempt, Some(Component::Normal(_)))
        || !matches!(file, Some(Component::Normal(_)))
        || parts.any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(invalid_path());
    }
    let parent = path.parent().ok_or_else(invalid_path)?;
    check_ancestors(parent)?;
    Ok(path)
}

fn reconcile_absence(root: &Path, object: &ObjectRef) -> Result<bool> {
    let path = checked_path(root, object)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            check_regular(&metadata)?;
            Ok(false)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            sync_parent(path.parent().ok_or_else(invalid_path)?)?;
            Ok(true)
        }
        Err(_) => Err(io_refusal()),
    }
}

fn remove_file(root: &Path, object: &ObjectRef) -> Result<LocalRemovalOutcome> {
    remove_file_with(root, object, |path| std::fs::remove_file(path), sync_parent)
}

// Keep the irreversible system calls injectable without a separate filesystem
// implementation. A failed synchronization can follow a successful unlink.
fn remove_file_with(
    root: &Path,
    object: &ObjectRef,
    unlink: impl FnOnce(&Path) -> std::io::Result<()>,
    synchronize: impl FnOnce(&Path) -> Result<()>,
) -> Result<LocalRemovalOutcome> {
    let path = checked_path(root, object)?;
    let parent = path.parent().ok_or_else(invalid_path)?;
    let before = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            synchronize(parent)?;
            return Ok(LocalRemovalOutcome::Absent);
        }
        Err(_) => return Err(io_refusal()),
    };
    check_regular(&before)?;
    if before.len() != object.bytes {
        return Err(changed());
    }
    let mut input = File::open(path).map_err(|_| io_refusal())?;
    let opened = input.metadata().map_err(|_| io_refusal())?;
    check_identity(&before, &opened)?;
    let mut hasher = blake3::Hasher::new();
    // Match the one MiB reservation owned by Budget::blocking.
    let mut buffer = vec![0; 1 << 20];
    let mut bytes = 0_u64;
    loop {
        let count = input.read(&mut buffer).map_err(|_| io_refusal())?;
        if count == 0 {
            break;
        }
        bytes = bytes.checked_add(count as u64).ok_or_else(changed)?;
        if bytes > object.bytes {
            return Err(changed());
        }
        hasher.update(&buffer[..count]);
    }
    if bytes != object.bytes || hasher.finalize().to_hex().as_str() != object.digest {
        return Err(changed());
    }
    check_identity(&opened, &input.metadata().map_err(|_| io_refusal())?)?;
    check_ancestors(parent)?;
    check_identity(
        &opened,
        &std::fs::symlink_metadata(path).map_err(|_| changed())?,
    )?;
    unlink(path).map_err(|_| io_refusal())?;
    synchronize(parent)?;
    Ok(LocalRemovalOutcome::Deleted)
}

fn check_ancestors(parent: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in parent.components() {
        current.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&current).map_err(|_| io_refusal())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(invalid_path());
        }
    }
    Ok(())
}

fn check_regular(metadata: &Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::refusal(
            "removal_file_type",
            "removal target must be a regular file",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn check_identity(before: &Metadata, after: &Metadata) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    check_regular(after)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Err(changed());
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_identity(_before: &Metadata, _after: &Metadata) -> Result<()> {
    Err(Error::refusal(
        "removal_platform",
        "local removal identity checks require Unix",
    ))
}

fn sync_parent(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| Error::refusal("removal_sync", "removal parent synchronization failed"))
}

fn invalid_path() -> Error {
    Error::refusal(
        "removal_path",
        "removal path is outside the exact owned regular-file namespace",
    )
}

fn changed() -> Error {
    Error::refusal(
        "removal_changed",
        "removal target identity or bytes changed",
    )
}

fn io_refusal() -> Error {
    Error::refusal("removal_io", "local removal filesystem operation failed")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{budget::Limits, model::Id};

    fn fixture() -> (tempfile::TempDir, PathBuf, ObjectRef, Budget) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("objects/owner/attempt/data");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"original").unwrap();
        let object = super::super::object(&Id::new("owner").unwrap(), &path).unwrap();
        let budget = Budget::new(Limits::default()).unwrap();
        (temporary, root, object, budget)
    }

    #[tokio::test]
    async fn absence_reconciliation_skips_present_bytes_and_rejects_unsafe_types() {
        let (_temporary, root, object, budget) = fixture();
        std::fs::write(&object.path, b"different bytes and size").unwrap();
        assert!(
            !reconcile_absent_file(&root, &object, &budget)
                .await
                .unwrap()
        );
        assert_eq!(
            std::fs::read(&object.path).unwrap(),
            b"different bytes and size"
        );
        std::fs::remove_file(&object.path).unwrap();
        assert!(
            reconcile_absent_file(&root, &object, &budget)
                .await
                .unwrap()
        );
        std::fs::create_dir(&object.path).unwrap();
        assert_eq!(
            reconcile_absent_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_file_type"
        );
        std::fs::remove_dir(&object.path).unwrap();
        std::os::unix::fs::symlink(root.join("missing"), &object.path).unwrap();
        assert_eq!(
            reconcile_absent_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_file_type"
        );
    }

    #[tokio::test]
    async fn deletes_exact_file_and_reconciles_absence() {
        let (_temporary, root, object, budget) = fixture();
        let neighbor = Path::new(&object.path).with_file_name("neighbor");
        std::fs::write(&neighbor, b"keep").unwrap();
        assert_eq!(
            remove_owned_file(&root, &object, &budget).await.unwrap(),
            LocalRemovalOutcome::Deleted
        );
        assert!(!Path::new(&object.path).exists());
        assert_eq!(std::fs::read(neighbor).unwrap(), b"keep");
        assert_eq!(
            remove_owned_file(&root, &object, &budget).await.unwrap(),
            LocalRemovalOutcome::Absent
        );
    }

    #[test]
    fn failed_unlink_preserves_bytes_and_does_not_synchronize() {
        let (_temporary, root, object, _budget) = fixture();
        let error = remove_file_with(
            &root,
            &object,
            |_| Err(std::io::Error::from(ErrorKind::PermissionDenied)),
            |_| panic!("failed unlink must not reach directory synchronization"),
        )
        .unwrap_err();
        assert_eq!(error.code(), "removal_io");
        assert_eq!(std::fs::read(&object.path).unwrap(), b"original");
        assert_eq!(
            remove_file(&root, &object).unwrap(),
            LocalRemovalOutcome::Deleted
        );
    }

    #[test]
    fn failed_directory_sync_reports_uncertainty_then_reconciles_absence() {
        let (_temporary, root, object, _budget) = fixture();
        let error = remove_file_with(
            &root,
            &object,
            |path| std::fs::remove_file(path),
            |_| {
                Err(Error::refusal(
                    "removal_sync",
                    "injected synchronization failure",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), "removal_sync");
        assert!(!Path::new(&object.path).exists());

        // An absent file is still unresolved until its parent sync succeeds.
        let error = remove_file_with(
            &root,
            &object,
            |_| panic!("absence reconciliation must not unlink again"),
            |_| {
                Err(Error::refusal(
                    "removal_sync",
                    "injected synchronization failure",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), "removal_sync");
        assert_eq!(
            remove_file(&root, &object).unwrap(),
            LocalRemovalOutcome::Absent
        );
    }

    #[tokio::test]
    async fn refuses_same_size_changed_bytes() {
        let (_temporary, root, object, budget) = fixture();
        std::fs::write(&object.path, b"replaced").unwrap();
        assert_eq!(
            remove_owned_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_changed"
        );
        assert_eq!(std::fs::read(&object.path).unwrap(), b"replaced");
    }

    #[tokio::test]
    async fn refuses_escaped_paths_and_wrong_owner() {
        let (_temporary, root, object, budget) = fixture();
        for path in [
            root.join("outside"),
            root.join("objects/owner/attempt/../data"),
            root.join("objects/owner/attempt/./data"),
            root.join("objects/other/attempt/data"),
        ] {
            let mut escaped = object.clone();
            escaped.path = path.to_string_lossy().into_owned();
            assert_eq!(
                remove_owned_file(&root, &escaped, &budget)
                    .await
                    .unwrap_err()
                    .code(),
                "removal_path"
            );
        }
        assert!(Path::new(&object.path).exists());
    }

    #[tokio::test]
    async fn refuses_directory_and_leaf_symlink() {
        let (_temporary, root, object, budget) = fixture();
        std::fs::remove_file(&object.path).unwrap();
        std::fs::create_dir(&object.path).unwrap();
        assert_eq!(
            remove_owned_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_file_type"
        );
        std::fs::remove_dir(&object.path).unwrap();
        let target = root.join("external");
        std::fs::write(&target, b"original").unwrap();
        std::os::unix::fs::symlink(&target, &object.path).unwrap();
        assert_eq!(
            remove_owned_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_file_type"
        );
        assert_eq!(std::fs::read(target).unwrap(), b"original");
    }

    #[tokio::test]
    async fn refuses_ancestor_symlink() {
        let (_temporary, root, object, budget) = fixture();
        let attempt = Path::new(&object.path).parent().unwrap();
        let moved = root.join("external");
        std::fs::rename(attempt, &moved).unwrap();
        std::os::unix::fs::symlink(&moved, attempt).unwrap();
        assert_eq!(
            remove_owned_file(&root, &object, &budget)
                .await
                .unwrap_err()
                .code(),
            "removal_path"
        );
        assert_eq!(std::fs::read(moved.join("data")).unwrap(), b"original");
    }
}
