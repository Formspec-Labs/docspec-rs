//! Restore-point capture and fresh native catalog recovery under local ownership.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::restore::{
    self, RestoreMaterialized, RestoreObject, RestorePoint, RestorePointRef, RestoreReuse,
    RestoredCatalog,
};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::fs::{self, File};

fn point_key(id: &Id) -> String {
    format!("restore-point/{id}")
}

impl Engine {
    /// Captures a native catalog and independently retained immutable dependencies.
    ///
    /// The point is advertised only after its catalog image and all dependencies
    /// are durable. Same-filesystem immutable objects use hard links; other files
    /// use bounded copies. Explicit retirement ends the recovery promise.
    ///
    /// # Errors
    /// Refuses injected catalogs, unsupported platforms, incomplete removals,
    /// unknown dependencies, conflicting identities, corruption, or exhausted capacity.
    pub async fn create_restore_point(&self, dataset: &Id, point: &Id) -> Result<RestorePointRef> {
        self.create_restore_request(dataset, point, None).await
    }

    /// Injects a deterministic capture failure at a retained recovery boundary.
    ///
    /// # Errors
    /// Returns the ordinary capture errors or the requested injected failure.
    #[cfg(feature = "test-util")]
    pub async fn create_restore_point_until(
        &self,
        dataset: &Id,
        point: &Id,
        stop: &str,
    ) -> Result<RestorePointRef> {
        self.create_restore_request(dataset, point, Some(stop.to_owned()))
            .await
    }

    async fn create_restore_request(
        &self,
        dataset: &Id,
        point: &Id,
        stop: Option<String>,
    ) -> Result<RestorePointRef> {
        let memory = self.request_memory(&(dataset, point, &stop))?;
        let dataset = dataset.clone();
        let point = point.clone();
        self.maintain_work(memory, move |engine| async move {
            engine
                .create_restore_point_admitted(dataset, point, stop.as_deref())
                .await
        })
        .await
    }

    async fn create_restore_point_admitted(
        &self,
        dataset: Id,
        id: Id,
        stop: Option<&str>,
    ) -> Result<RestorePointRef> {
        self.restore_catalog()?;
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        let root = &self.inner.root;
        let view = lifecycle.open(&dataset).await?;
        let key = point_key(&id);
        if let Some(reference) = restore_reference(lifecycle, &view, &key).await? {
            let worker_root = root.clone();
            let worker_reference = reference.clone();
            budget
                .blocking(move || {
                    restore::read_point(&worker_root, &worker_reference, limit)?;
                    Ok(())
                })
                .await?;
            return Ok(reference);
        }
        // One deterministic identity survives every failed or canceled retry.
        // The retained protection proves this namespace belongs to this point.
        let identity = format!("{}\0{}", dataset, id);
        let attempt = Id::new(format!(
            "restore_{}",
            blake3::hash(identity.as_bytes()).to_hex()
        ))?;
        let attempt_directory = root
            .join("objects")
            .join(dataset.as_str())
            .join(attempt.as_str());
        let reservation = Protection {
            id: attempt.clone(),
            consumer: dataset.clone(),
            purpose: format!("{id}/restore"),
            paths: vec![format!("{}/", attempt_directory.display())],
            retired: false,
        };
        let reservation_key = format!("protection/{attempt}");
        let mut view = match lifecycle.get::<Protection>(&view, &reservation_key).await? {
            Some(existing) if existing == reservation => view,
            Some(_) => {
                return Err(restore::invalid(
                    "restore attempt reservation conflicts with its identity",
                ));
            }
            None => {
                lifecycle
                    .commit(&view, vec![record(&reservation_key, &reservation)?])
                    .await?
            }
        };
        stop_after(stop, "restore-reservation")?;
        let saved_reuse = restore_reuse(lifecycle, &view, &dataset, &id).await?;
        let memory = Arc::new(budget.reserve(limit.saturating_mul(4))?);
        let directory = attempt_directory.join("restore");
        create_owned_directories(root, &directory)?;
        let manifest_path = directory.join("point.json");
        // Manifest publication is the immutable completion barrier. Never rebuild
        // or remove a completed artifact just because its final commit response was lost.
        if fs::symlink_metadata(&manifest_path).is_ok() {
            let worker_root = root.clone();
            let worker_dataset = dataset.clone();
            let worker_id = id.clone();
            let worker_memory = memory.clone();
            let (reference, point) = budget
                .blocking(move || {
                    let _memory = worker_memory;
                    restore::regular_owned_file(&worker_root, &manifest_path)?;
                    let reference = RestorePointRef {
                        dataset: worker_dataset.clone(),
                        id: worker_id,
                        manifest: storage::object(&worker_dataset, &manifest_path)?,
                    };
                    let point = restore::read_point(&worker_root, &reference, limit)?;
                    let required = point
                        .objects
                        .iter()
                        .filter(|object| {
                            Path::new(&object.backup.path).parent()
                                != Path::new(&reference.manifest.path).parent()
                        })
                        .map(|object| &object.backup);
                    let retained = saved_reuse
                        .as_ref()
                        .map(|saved| {
                            saved
                                .objects
                                .iter()
                                .map(|object| (object.path.as_str(), object))
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default();
                    for object in required {
                        if retained.get(object.path.as_str()).copied() != Some(object) {
                            return Err(restore::invalid(
                                "completed capture lacks its shared dependency protection",
                            ));
                        }
                    }
                    verify_point_files(&worker_root, &point, false)?;
                    Ok((reference, point))
                })
                .await?;
            publish_point(lifecycle, &view, &reference, &point, reservation).await?;
            stop_after(stop, "restore-committed")?;
            return Ok(reference);
        }
        let worker_root = root.clone();
        let worker_directory = directory.clone();
        budget
            .blocking(move || clear_incomplete_attempt(&worker_root, &worker_directory, limit))
            .await?;
        // This shared traversal refuses pending removals. Repeated refusal writes
        // no new reservation or catalog history after the first stable attempt.
        let (mut boundaries, mut objects) = self.restore_dependencies(&dataset).await?;
        let reusable = objects
            .iter()
            .filter(|object| restore::backup_path(root, object).is_ok())
            .map(|object| (object.path.clone(), object.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        let reuse = RestoreReuse {
            dataset: dataset.clone(),
            id: id.clone(),
            objects: reusable,
        };
        if saved_reuse.as_ref() != Some(&reuse) {
            view = lifecycle
                .commit(&view, vec![record(format!("restore-reuse/{id}"), &reuse)?])
                .await?;
            refresh_capture_owner(root, &dataset, &view, &mut boundaries, &mut objects, budget)
                .await?;
        }
        stop_after(stop, "restore-reuse")?;
        let catalog_path = directory.join("catalog.sqlite");
        vacuum_catalog(&restore::catalog_path(root)?, &catalog_path).await?;
        stop_after(stop, "restore-catalog")?;
        let worker_root = root.clone();
        let owner = dataset.clone();
        let worker_directory = directory.clone();
        let worker_memory = memory.clone();
        let interrupt_objects = stop == Some("restore-objects");
        let (catalog, objects, linked_bytes, copied_bytes, reused_bytes) = budget
            .blocking(move || {
                let _memory = worker_memory;
                let catalog = storage::object(&owner, &catalog_path)?;
                // Existing point files are already exact roots from the common
                // inventory. Reuse their original ownership and digest identities.
                let mut reusable = BTreeMap::<(String, u64), ObjectRef>::new();
                for object in &objects {
                    if restore::backup_path(&worker_root, object).is_ok() {
                        reusable
                            .entry((object.digest.clone(), object.bytes))
                            .or_insert_with(|| object.clone());
                    }
                }
                let mut retained = Vec::with_capacity(objects.len());
                let mut linked_bytes = 0u64;
                let mut copied_bytes = 0u64;
                let mut reused_bytes = 0u64;
                for (index, original) in objects.into_iter().enumerate() {
                    restore::regular_owned_file(&worker_root, Path::new(&original.path))?;
                    let identity = (original.digest.clone(), original.bytes);
                    let (backup, bytes) = if let Some(backup) = reusable.get(&identity) {
                        (backup.clone(), &mut reused_bytes)
                    } else {
                        let path = worker_directory.join(format!("object-{index}"));
                        let linked = retain_file(&original, &path)?;
                        let backup = ObjectRef {
                            owner: owner.clone(),
                            path: path.to_string_lossy().into_owned(),
                            digest: original.digest.clone(),
                            bytes: original.bytes,
                        };
                        reusable.insert(identity, backup.clone());
                        (
                            backup,
                            if linked {
                                &mut linked_bytes
                            } else {
                                &mut copied_bytes
                            },
                        )
                    };
                    *bytes = bytes
                        .checked_add(original.bytes)
                        .ok_or_else(|| restore::invalid("restore byte accounting overflow"))?;
                    retained.push(RestoreObject { original, backup });
                    if interrupt_objects {
                        return Err(Error::refusal(
                            "injected_failure",
                            "interrupted after partial restore objects",
                        ));
                    }
                }
                File::open(&worker_directory)?.sync_all()?;
                Ok((catalog, retained, linked_bytes, copied_bytes, reused_bytes))
            })
            .await?;
        let point = RestorePoint {
            format: restore::FORMAT,
            id: id.clone(),
            dataset: dataset.clone(),
            workspace: root.to_string_lossy().into_owned(),
            catalog,
            boundaries,
            objects,
            linked_bytes,
            copied_bytes,
            reused_bytes,
        };
        let worker_point = point.clone();
        let worker_dataset = dataset.clone();
        let manifest = budget
            .blocking(move || {
                restore::atomic_json(&manifest_path, &worker_point, limit, false)?;
                storage::object(&worker_dataset, &manifest_path)
            })
            .await?;
        let reference = RestorePointRef {
            dataset,
            id,
            manifest,
        };
        restore::validate_point(root, &reference, &point)?;
        stop_after(stop, "restore-manifest")?;
        publish_point(lifecycle, &view, &reference, &point, reservation).await?;
        stop_after(stop, "restore-committed")?;
        Ok(reference)
    }

    /// Restores an exact point after excluding every prior local workspace actor.
    ///
    /// Run this after closing the old engine and all read streams. Recovery checks
    /// every retained byte identity, restores missing original immutable paths,
    /// and selects a fresh SQLite file. It retains previous catalog files.
    /// `allow_unregistered_import` explicitly permits a completed backup artifact
    /// when current catalog authority is unavailable or lacks the point. A known
    /// retired or conflicting point always refuses, including explicit imports.
    ///
    /// # Errors
    /// Refuses a live host, retired/conflicting points, unsupported platforms,
    /// missing or changed backup bytes, conflicting original bytes, or failed I/O.
    pub async fn restore_local(
        root: impl AsRef<Path>,
        point: &RestorePointRef,
        limits: Limits,
        allow_unregistered_import: bool,
    ) -> Result<RestoredCatalog> {
        if !cfg!(unix) {
            return Err(Error::refusal(
                "restore_platform",
                "local restore requires Unix workspace fencing",
            ));
        }
        let runtime = worker::runtime()?;
        let host = worker::Host::acquire(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        let point = point.clone();
        let custody = worker::Custody::new(host.clone());
        runtime
            .spawn(async move {
                let result =
                    Self::restore_local_owned(root, point, limits, allow_unregistered_import, host)
                        .await;
                custody.complete();
                result
            })
            .await?
    }

    async fn restore_local_owned(
        root: PathBuf,
        reference: RestorePointRef,
        limits: Limits,
        allow_unregistered_import: bool,
        _host: Arc<worker::Host>,
    ) -> Result<RestoredCatalog> {
        let scratch = root.join("scratch");
        fs::create_dir_all(&scratch)?;
        let budget = Budget::in_workspace(limits.clone(), &scratch)?;
        let limit = budget.limits.max_metadata_bytes;
        let _memory = budget.reserve(limit.saturating_mul(4))?;
        let worker_root = root.clone();
        let worker_reference = reference.clone();
        let point = budget
            .blocking(move || restore::read_point(&worker_root, &worker_reference, limit))
            .await?;
        // Inspect current authority before creating the fresh candidate. Import
        // is an explicit catalog-loss procedure, never a fallback from retirement.
        check_current_authority(&root, &reference, &budget, allow_unregistered_import).await?;
        let worker_root = root.clone();
        let worker_point = point.clone();
        let (restored_bytes, reused_bytes) = budget
            .blocking(move || {
                // Hash each Unix inode once even when backup and original names
                // refer to the same immutable bytes. Conflicting identities refuse.
                verify_point_files(&worker_root, &worker_point, true)?;
                let mut restored_bytes = 0u64;
                let mut reused_bytes = 0u64;
                for object in &worker_point.objects {
                    let target = Path::new(&object.original.path);
                    let total = if target.try_exists()? {
                        &mut reused_bytes
                    } else {
                        let parent = target
                            .parent()
                            .ok_or_else(|| restore::invalid("restore target has no parent"))?;
                        create_owned_directories(&worker_root, parent)?;
                        retain_file(&object.backup, target)?;
                        &mut restored_bytes
                    };
                    *total = total
                        .checked_add(object.original.bytes)
                        .ok_or_else(|| restore::invalid("restore byte accounting overflow"))?;
                }
                Ok((restored_bytes, reused_bytes))
            })
            .await?;
        let generation = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let catalogs = root.join("catalogs");
        create_owned_directories(&root, &catalogs)?;
        let fresh_path = catalogs.join(format!("{generation}.sqlite"));
        vacuum_catalog(Path::new(&point.catalog.path), &fresh_path).await?;
        let catalog = storage::local_catalog_at(&root, &fresh_path).await?;
        let fresh = Lifecycle::new(catalog, budget.clone());
        // Opening exact boundaries proves the maintained reader sees the saved
        // native metadata. The point's all-root closure was already verified.
        for boundary in &point.boundaries {
            let ident = iceberg::TableIdent::from_strs(boundary.table.split('.'))?;
            let table = fresh.catalog.load_table(&ident).await?;
            if table.metadata_location_result()? != boundary.metadata
                || table.metadata().last_sequence_number() != boundary.revision
            {
                return Err(restore::invalid(
                    "fresh catalog differs from its captured native boundary",
                ));
            }
        }
        let view = fresh.open(&reference.dataset).await?;
        let key = point_key(&reference.id);
        if let Some(existing) = restore_reference(&fresh, &view, &key).await? {
            if existing != reference {
                return Err(restore::invalid(
                    "restored point identity conflicts with captured authority",
                ));
            }
        } else {
            let attempt_directory = Path::new(&reference.manifest.path)
                .parent()
                .and_then(Path::parent)
                .ok_or_else(|| restore::invalid("restore artifact lacks its reserved attempt"))?;
            let attempt = Id::new(
                attempt_directory
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| restore::invalid("restore attempt identity is invalid"))?,
            )?;
            fresh
                .commit(
                    &view,
                    vec![
                        record(key, &reference)?,
                        record(
                            format!("restore-materialized/{}", reference.id),
                            materialized(&reference, &point),
                        )?,
                        record(
                            format!("restore-reuse/{}", reference.id),
                            RestoreReuse {
                                dataset: reference.dataset.clone(),
                                id: reference.id.clone(),
                                objects: Vec::new(),
                            },
                        )?,
                        record(
                            format!("protection/{attempt}"),
                            Protection {
                                id: attempt,
                                consumer: reference.dataset.clone(),
                                purpose: format!("{}/restore", reference.id),
                                paths: vec![format!("{}/", attempt_directory.display())],
                                retired: true,
                            },
                        )?,
                    ],
                )
                .await?;
        }
        let restored = RestoredCatalog {
            point: reference.id,
            generation,
            path: fresh_path.to_string_lossy().into_owned(),
            restored_bytes,
            reused_bytes,
        };
        // SQLite commits above use the maintained native catalog. Its durable
        // database plus any journal remain beside this generation's unique file.
        File::open(&fresh_path)?.sync_all()?;
        File::open(&catalogs)?.sync_all()?;
        restore::select_catalog(&root, &restored)?;
        Ok(restored)
    }

    fn restore_catalog(&self) -> Result<()> {
        if !self.inner.owned_catalog {
            return Err(Error::refusal(
                "restore_catalog_unqualified",
                "restore points require the owned embedded catalog",
            ));
        }
        if !cfg!(unix) {
            return Err(Error::refusal(
                "restore_platform",
                "local restore requires Unix workspace fencing",
            ));
        }
        Ok(())
    }
}

async fn restore_reference(
    lifecycle: &Lifecycle,
    view: &View,
    key: &str,
) -> Result<Option<RestorePointRef>> {
    let rows = lifecycle.rows(view).await?;
    let Some(revision) = rows.revision(key) else {
        return Ok(None);
    };
    if retention::retirement::record_retired(&rows, view, key, revision, &lifecycle.budget)? {
        return Err(Error::refusal(
            "restore_point_retired",
            "the point's recovery promise was explicitly retired",
        ));
    }
    let _memory = rows.reserve_decoded(&lifecycle.budget, &[key])?;
    let reference = rows
        .get_record::<RestorePointRef>(key)?
        .ok_or_else(|| restore::invalid("restore point record disappeared"))?
        .2;
    if point_key(&reference.id) != key
        || crate::catalog::lifecycle_ident(&reference.dataset) != *view.table.identifier()
        || reference.manifest.owner != reference.dataset
    {
        return Err(restore::invalid(
            "restore reference differs from its lifecycle owner",
        ));
    }
    Ok(Some(reference))
}

fn materialized(reference: &RestorePointRef, point: &RestorePoint) -> RestoreMaterialized {
    let directory = Path::new(&reference.manifest.path).parent();
    let backups = point
        .objects
        .iter()
        .filter(|object| Path::new(&object.backup.path).parent() == directory)
        .map(|object| (object.backup.path.clone(), object.backup.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    RestoreMaterialized {
        reference: reference.clone(),
        catalog: point.catalog.clone(),
        backups,
    }
}

async fn restore_reuse(
    lifecycle: &Lifecycle,
    view: &View,
    dataset: &Id,
    id: &Id,
) -> Result<Option<RestoreReuse>> {
    let key = format!("restore-reuse/{id}");
    let rows = lifecycle.rows(view).await?;
    let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
    let Some((revision, _, reuse)) = rows.get_record::<RestoreReuse>(&key)? else {
        return Ok(None);
    };
    if reuse.dataset != *dataset || reuse.id != *id {
        return Err(restore::invalid(
            "restore reuse record differs from its capture identity",
        ));
    }
    if !reuse.objects.is_empty()
        && retention::retirement::record_retired(&rows, view, &key, revision, &lifecycle.budget)?
    {
        return Err(Error::refusal(
            "restore_attempt_retired",
            "shared restore preparation dependencies were explicitly retired; use a new point ID",
        ));
    }
    Ok(Some(reuse))
}

async fn refresh_capture_owner(
    root: &Path,
    owner: &Id,
    view: &View,
    boundaries: &mut [RetentionBoundary],
    objects: &mut Vec<ObjectRef>,
    budget: &Budget,
) -> Result<()> {
    let name = view.table.identifier().to_string();
    let boundary = boundaries
        .iter_mut()
        .find(|boundary| boundary.table == name)
        .ok_or_else(|| restore::invalid("capture omitted its owning lifecycle table"))?;
    boundary.metadata = view.table.metadata_location_result()?.to_owned();
    boundary.revision = view.revision;
    let paths = storage::snapshot_files(&view.table, budget).await?;
    let known = objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    for path in paths.paths.iter().filter(|path| !known.contains(*path)) {
        let worker_root = root.to_owned();
        let worker_owner = owner.clone();
        let path = path.clone();
        let object = budget
            .blocking(move || {
                restore::regular_owned_file(&worker_root, Path::new(&path))?;
                storage::object(&worker_owner, Path::new(&path))
            })
            .await?;
        objects.push(object);
        inputs::metadata_size(&objects, budget.limits.max_metadata_bytes)?;
    }
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(())
}

async fn publish_point(
    lifecycle: &Lifecycle,
    view: &View,
    reference: &RestorePointRef,
    point: &RestorePoint,
    mut reservation: Protection,
) -> Result<()> {
    reservation.retired = true;
    lifecycle
        .commit(
            view,
            vec![
                record(point_key(&reference.id), reference)?,
                record(
                    format!("restore-materialized/{}", reference.id),
                    materialized(reference, point),
                )?,
                record(
                    format!("restore-reuse/{}", reference.id),
                    RestoreReuse {
                        dataset: reference.dataset.clone(),
                        id: reference.id.clone(),
                        objects: Vec::new(),
                    },
                )?,
                record(format!("protection/{}", reservation.id), reservation)?,
            ],
        )
        .await?;
    Ok(())
}

fn verify_point_files(root: &Path, point: &RestorePoint, originals: bool) -> Result<usize> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    let mut verified = BTreeMap::new();
    let mut count = 0usize;
    let mut verify = |object: &ObjectRef| -> Result<()> {
        restore::regular_owned_file(root, Path::new(&object.path))?;
        let metadata = fs::metadata(&object.path)?;
        if metadata.len() != object.bytes {
            return Err(restore::invalid(
                "restore object size differs from retained identity",
            ));
        }
        #[cfg(unix)]
        let physical = (metadata.dev(), metadata.ino());
        #[cfg(not(unix))]
        let physical = object.path.clone();
        let identity = (&object.digest, object.bytes);
        if let Some(previous) = verified.get(&physical) {
            if previous != &(identity.0.clone(), identity.1) {
                return Err(restore::invalid(
                    "one restore inode has conflicting immutable identities",
                ));
            }
            return Ok(());
        }
        storage::verify_object(object)?;
        verified.insert(physical, (identity.0.clone(), identity.1));
        count += 1;
        Ok(())
    };
    verify(&point.catalog)?;
    for object in &point.objects {
        verify(&object.backup)?;
        if originals && Path::new(&object.original.path).try_exists()? {
            verify(&object.original)?;
        }
    }
    Ok(count)
}

fn clear_incomplete_attempt(root: &Path, directory: &Path, limit: usize) -> Result<()> {
    restore::plain_directory(root, directory)?;
    match fs::symlink_metadata(directory.join("point.json")) {
        Ok(_) => {
            return Err(restore::invalid(
                "completed restore artifact cannot be rebuilt",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut paths = Vec::new();
    let mut bytes = 0usize;
    // Validate the complete flat attempt before the first unlink. Unknown files,
    // directories and symlinks refuse; this is not an orphan/directory sweep.
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        restore::regular_owned_file(root, &path)?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| restore::invalid("restore attempt filename is invalid"))?;
        let supported = matches!(
            name,
            "catalog.sqlite"
                | "catalog.sqlite-journal"
                | "catalog.sqlite-wal"
                | "catalog.sqlite-shm"
        ) || name.strip_prefix("object-").is_some_and(|index| {
            !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit())
        }) || name.strip_prefix('.').is_some_and(|tail| {
            tail.strip_suffix(".pending")
                .or_else(|| tail.strip_suffix(".restore"))
                .is_some_and(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        });
        if !supported {
            return Err(restore::invalid(
                "restore attempt contains an unrecognized file",
            ));
        }
        bytes = bytes
            .saturating_add(path.as_os_str().len())
            .saturating_add(128);
        if bytes > limit {
            return Err(Error::refusal(
                "metadata_capacity",
                "restore attempt cleanup exceeds its allowance",
            ));
        }
        paths.push(path);
    }
    for path in paths {
        fs::remove_file(path)?;
    }
    File::open(directory)?.sync_all()?;
    Ok(())
}

async fn check_current_authority(
    root: &Path,
    reference: &RestorePointRef,
    budget: &Budget,
    allow_unregistered_import: bool,
) -> Result<()> {
    let current = async {
        let path = restore::catalog_path(root)?;
        if !path.try_exists()? {
            return Ok(None);
        }
        let catalog = storage::local_catalog_at(root, &path).await?;
        let current = Lifecycle::new(catalog, budget.clone());
        let ident = crate::catalog::lifecycle_ident(&reference.dataset);
        if !current.catalog.table_exists(&ident).await? {
            return Ok(None);
        }
        let table = current.catalog.load_table(&ident).await?;
        let view = View {
            revision: table.metadata().last_sequence_number(),
            table,
        };
        restore_reference(&current, &view, &point_key(&reference.id)).await
    }
    .await;
    match current {
        Ok(Some(registered)) if registered == *reference => Ok(()),
        Ok(Some(_)) => Err(Error::refusal(
            "restore_point_conflict",
            "restore reference differs from current authority",
        )),
        Ok(None) if allow_unregistered_import => Ok(()),
        Ok(None) => Err(Error::refusal(
            "restore_point_unregistered",
            "point is not currently registered; explicit artifact import is required",
        )),
        Err(error) if allow_unregistered_import && matches!(error.code(), "iceberg" | "io") => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn vacuum_catalog(source: &Path, target: &Path) -> Result<()> {
    if target.try_exists()? {
        return Err(restore::invalid("fresh catalog target already exists"));
    }
    let options = SqliteConnectOptions::new().filename(source).read_only(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .map_err(catalog_error)?;
    let result = async {
        let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&mut connection)
            .await
            .map_err(catalog_error)?;
        let size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&mut connection)
            .await
            .map_err(catalog_error)?;
        let required = pages
            .checked_mul(size)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| restore::invalid("native catalog size is invalid"))?;
        let parent = target
            .parent()
            .ok_or_else(|| restore::invalid("native catalog has no parent"))?;
        if fs2::available_space(parent)? < required {
            return Err(Error::refusal(
                "restore_space",
                "insufficient space for a fresh native catalog",
            ));
        }
        sqlx::query("VACUUM INTO ?")
            .bind(target.to_string_lossy().as_ref())
            .execute(&mut connection)
            .await
            .map_err(catalog_error)?;
        Ok(())
    }
    .await;
    let closed = connection.close().await.map_err(catalog_error);
    result?;
    closed?;
    File::open(target)?.sync_all()?;
    File::open(
        target
            .parent()
            .ok_or_else(|| restore::invalid("native catalog has no parent"))?,
    )?
    .sync_all()?;
    let mut check = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(target).read_only(true),
    )
    .await
    .map_err(catalog_error)?;
    let result = sqlx::query_scalar::<_, String>("PRAGMA integrity_check(1)")
        .fetch_one(&mut check)
        .await
        .map_err(catalog_error);
    let closed = check.close().await.map_err(catalog_error);
    if result? != "ok" {
        return Err(restore::invalid("native catalog integrity check failed"));
    }
    closed
}

fn catalog_error(error: sqlx::Error) -> Error {
    Error::refusal("restore_catalog", error.to_string())
}

fn create_owned_directories(root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| restore::invalid("restore directory escapes its workspace"))?;
    let mut current = root.to_owned();
    for part in relative.components() {
        if !matches!(part, std::path::Component::Normal(_)) {
            return Err(restore::invalid("restore directory is not canonical"));
        }
        current.push(part);
        match fs::create_dir(&current) {
            Ok(()) => {
                File::open(
                    current
                        .parent()
                        .ok_or_else(|| restore::invalid("restore directory has no parent"))?,
                )?
                .sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        restore::plain_directory(root, &current)?;
    }
    Ok(())
}

fn retain_file(source: &ObjectRef, target: &Path) -> Result<bool> {
    let metadata = fs::metadata(&source.path)?;
    if !metadata.is_file() || metadata.len() != source.bytes {
        return Err(restore::invalid("immutable restore source size differs"));
    }
    match fs::hard_link(&source.path, target) {
        Ok(()) => {
            File::open(target)?.sync_all()?;
            File::open(
                target
                    .parent()
                    .ok_or_else(|| restore::invalid("restore file has no parent"))?,
            )?
            .sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Err(error.into()),
        Err(_) => {
            let parent = target
                .parent()
                .ok_or_else(|| restore::invalid("restore file has no parent"))?;
            if fs2::available_space(parent)? < source.bytes {
                return Err(Error::refusal(
                    "restore_space",
                    "insufficient space for immutable restore copy",
                ));
            }
            let temporary = parent.join(format!(".{}.restore", uuid::Uuid::new_v4().simple()));
            storage::copy_verified(
                &source.owner,
                Path::new(&source.path),
                &temporary,
                &source.digest,
            )?;
            // A lost worker can leave only an unreferenced partial temporary.
            // Never expose a partial file at a native metadata-selected path.
            fs::hard_link(&temporary, target)?;
            fs::remove_file(&temporary)?;
            File::open(parent)?.sync_all()?;
            Ok(false)
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn verification_hashes_shared_inode_once_and_refuses_conflicting_claims() {
        let root = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(root.path()).unwrap();
        let owner = Id::new("owner").unwrap();
        let source = root.join("original");
        let backup = root.join("backup");
        let catalog = root.join("catalog");
        fs::write(&source, b"retained immutable bytes").unwrap();
        fs::hard_link(&source, &backup).unwrap();
        fs::write(&catalog, b"catalog identity").unwrap();
        let original = storage::object(&owner, &source).unwrap();
        let saved = storage::object(&owner, &backup).unwrap();
        let mut point = RestorePoint {
            format: restore::FORMAT,
            id: Id::new("point").unwrap(),
            dataset: owner.clone(),
            workspace: root.to_string_lossy().into_owned(),
            catalog: storage::object(&owner, &catalog).unwrap(),
            boundaries: Vec::new(),
            linked_bytes: original.bytes,
            copied_bytes: 0,
            reused_bytes: 0,
            objects: vec![RestoreObject {
                original,
                backup: saved,
            }],
        };
        assert_eq!(verify_point_files(&root, &point, true).unwrap(), 2);
        point.objects[0].original.digest = "0".repeat(64);
        assert_eq!(
            verify_point_files(&root, &point, true).unwrap_err().code(),
            "invalid_restore_point"
        );
    }

    #[test]
    fn incomplete_cleanup_refuses_symlinks_and_completed_artifacts_without_deleting() {
        let root = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(root.path()).unwrap();
        let directory = root.join("attempt");
        fs::create_dir(&directory).unwrap();
        let source = root.join("unique");
        fs::write(&source, b"must remain").unwrap();
        let regular = directory.join("object-0");
        fs::write(&regular, b"partial").unwrap();
        let link = directory.join("object-1");
        std::os::unix::fs::symlink(&source, &link).unwrap();
        assert!(clear_incomplete_attempt(&root, &directory, 4096).is_err());
        assert!(regular.exists());
        assert_eq!(fs::read(&source).unwrap(), b"must remain");
        fs::remove_file(link).unwrap();
        fs::write(directory.join("point.json"), b"completed marker").unwrap();
        assert!(clear_incomplete_attempt(&root, &directory, 4096).is_err());
        assert!(regular.exists());
    }
}
