//! Explicit quiescent catalog-loss restore through maintained native registrations.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::restore::remote::{
    self, CatalogImage, Registration, RemoteRestorePoint, RestoreAuthority, RestoredRemoteCatalog,
};
use iceberg::{NamespaceIdent, TableIdent};

impl Engine {
    pub(super) async fn create_remote_point(
        &self,
        dataset: Id,
        id: Id,
        stop: Option<&str>,
    ) -> Result<RestorePointRef> {
        let generation = self.remote_maintenance()?;
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(8))?);
        let mut view = self.existing_lifecycle_view(&dataset).await?;
        if let Some(reference) = restore_reference(lifecycle, &view, &point_key(&id)).await? {
            self.remote_point(&reference, &memory).await?;
            return Ok(reference);
        }
        // Only durably completed capture is reused. An unacknowledged capture
        // restarts in a fresh protected attempt; no old destination is overwritten.
        let attempt = Id::new(format!("restore_{}", uuid::Uuid::new_v4().simple()))?;
        let directory = self
            .inner
            .retained
            .child(dataset.as_str())?
            .child(attempt.as_str())?;
        let protection = Protection {
            id: attempt.clone(),
            consumer: dataset.clone(),
            purpose: format!("{id}/restore"),
            paths: vec![directory.protection_prefix()?],
            retired: false,
        };
        view = lifecycle
            .commit(
                &view,
                vec![record(format!("protection/{attempt}"), &protection)?],
            )
            .await?;
        stop_after(stop, "restore-reservation")?;
        let (boundaries, objects) = self.restore_dependencies(&dataset).await?;
        for object in &objects {
            self.inner.objects.validate_retained_path(&object.path)?;
        }
        let mut image = CatalogImage {
            format: remote::FORMAT,
            generation,
            namespaces: BTreeMap::new(),
            tables: Vec::new(),
        };
        for boundary in &boundaries {
            let ident = TableIdent::from_strs(boundary.table.split('.'))?;
            let table = lifecycle.catalog.load_table(&ident).await?;
            if table.metadata_location_result()? != boundary.metadata
                || table.metadata().last_sequence_number() != boundary.revision
            {
                return Err(restore::invalid("catalog changed during quiescent capture"));
            }
            let authority = if ident.name() == "lifecycle" {
                let view = View {
                    revision: table.metadata().last_sequence_number(),
                    table: table.clone(),
                    logical_identifier: ident.clone(),
                };
                Some(capture_authority(lifecycle, &view).await?)
            } else {
                None
            };
            let owner = self.retained_owner(table.metadata().location())?;
            if crate::catalog::lifecycle_ident(&owner).namespace() != ident.namespace() {
                return Err(restore::invalid(
                    "registration is outside its exact owner namespace",
                ));
            }
            let name = ident.namespace().to_string();
            if let std::collections::btree_map::Entry::Vacant(entry) = image.namespaces.entry(name)
            {
                let namespace = lifecycle.catalog.get_namespace(ident.namespace()).await?;
                entry.insert(
                    namespace
                        .properties()
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                );
            }
            let metadata = objects
                .binary_search_by(|object| object.path.cmp(&boundary.metadata))
                .ok()
                .map(|index| objects[index].clone())
                .ok_or_else(|| restore::invalid("captured native metadata has no identity"))?;
            image.tables.push(Registration {
                table: boundary.table.clone(),
                table_uuid: table.metadata().uuid().to_string(),
                metadata,
                revision: boundary.revision,
                authority,
            });
        }
        image.tables.sort_by(|a, b| a.table.cmp(&b.table));
        // Freeze exact dependencies before emitting either artifact. The captured
        // image intentionally precedes this bookkeeping-only owner commit.
        view = lifecycle
            .commit(
                &view,
                vec![record(
                    format!("restore-reuse/{id}"),
                    RestoreReuse {
                        dataset: dataset.clone(),
                        id: id.clone(),
                        objects: objects.clone(),
                    },
                )?],
            )
            .await?;
        stop_after(stop, "restore-reuse")?;
        self.inner
            .objects
            .check_available(objects.clone(), budget)
            .await?;
        let catalog = self
            .write_remote_restore_json(
                &dataset,
                &directory.child("catalog.json")?.as_string()?,
                &image,
                &memory,
            )
            .await?;
        stop_after(stop, "restore-catalog")?;
        let point = RemoteRestorePoint {
            format: remote::FORMAT,
            id: id.clone(),
            dataset: dataset.clone(),
            generation,
            retained_prefix: self
                .inner
                .objects
                .retained_prefix()
                .ok_or_else(locations::unsupported)?,
            catalog,
            objects,
        };
        let manifest = self
            .write_remote_restore_json(
                &dataset,
                &directory.child("point.json")?.as_string()?,
                &point,
                &memory,
            )
            .await?;
        let reference = RestorePointRef {
            dataset: dataset.clone(),
            id,
            manifest,
        };
        remote::validate(&reference, &point)?;
        stop_after(stop, "restore-manifest")?;
        for boundary in &boundaries {
            let ident = TableIdent::from_strs(boundary.table.split('.'))?;
            let expected = if &ident == view.identifier() {
                view.table.metadata_location_result()?
            } else {
                &boundary.metadata
            };
            if lifecycle
                .catalog
                .load_table(&ident)
                .await?
                .metadata_location_result()?
                != expected
            {
                return Err(restore::invalid(
                    "catalog changed before restore point publication",
                ));
            }
        }
        check_restore_reservation(lifecycle, &view, &reference, &point).await?;
        let records = remote_completion(&reference, &point)?;
        if let Err(error) = lifecycle.commit(&view, records).await {
            let fresh = self.existing_lifecycle_view(&dataset).await?;
            if restore_reference(lifecycle, &fresh, &point_key(&reference.id))
                .await?
                .as_ref()
                != Some(&reference)
            {
                return Err(error);
            }
        }
        stop_after(stop, "restore-committed")?;
        Ok(reference)
    }

    async fn write_remote_restore_json<
        T: Serialize + serde::de::DeserializeOwned + Send + 'static,
    >(
        &self,
        owner: &Id,
        path: &str,
        value: &T,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ObjectRef> {
        let budget = &self.inner.lifecycle.budget;
        inputs::metadata_size(value, budget.limits.max_metadata_bytes)?;
        let bytes = serde_json::to_vec(value)?;
        let expected = ObjectRef {
            owner: owner.clone(),
            path: path.into(),
            bytes: bytes.len() as u64,
            digest: blake3::hash(&bytes).to_hex().to_string(),
        };
        let objects = self.inner.objects.clone();
        let object = expected.clone();
        let admission = memory.clone();
        let handle = tokio::runtime::Handle::current();
        // Keep the exact serialized buffer and worker admission through creation.
        let creation = budget
            .blocking_admitted(move || {
                let _admission = admission;
                objects.create_release_metadata_admitted(&object, bytes, &handle)
            })
            .await;
        let mut read = 0;
        let verified: Result<T> =
            inputs::read_manifest(&self.inner.objects, &expected, budget, &mut read, memory).await;
        match (creation, verified) {
            (_, Ok(_)) => Ok(expected),
            (Err(error), Err(_)) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn remote_point(
        &self,
        reference: &RestorePointRef,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<RemoteRestorePoint> {
        let mut bytes = 0;
        let point: RemoteRestorePoint = inputs::read_manifest(
            &self.inner.objects,
            &reference.manifest,
            &self.inner.lifecycle.budget,
            &mut bytes,
            memory,
        )
        .await?;
        remote::validate(reference, &point)?;
        if Some(point.retained_prefix.clone()) != self.inner.objects.retained_prefix() {
            return Err(restore::invalid("restore object scope differs"));
        }
        self.inner
            .objects
            .validate_retained_path(&point.catalog.path)?;
        for object in &point.objects {
            self.inner.objects.validate_retained_path(&object.path)?;
        }
        Ok(point)
    }

    /// Restores a completed catalog-loss point into a fresh configured generation.
    ///
    /// Both connections must explicitly declare quiescent maintenance. Stop all
    /// old actors and drain/revoke destructive credentials before calling; no
    /// distributed lease is inferred. Original immutable URIs are never reused
    /// for new bytes. The old generation is detached without purge. Restart normal
    /// clients only with the returned target generation after success.
    ///
    /// # Errors
    /// Refuses unqualified scope, retired/conflicting points, reserved deletion
    /// targets, missing objects, foreign target registrations or unknown outcomes.
    pub async fn restore_remote(
        &self,
        reference: &RestorePointRef,
        target: storage::remote::RemoteStorage,
        allow_unregistered_import: bool,
    ) -> Result<RestoredRemoteCatalog> {
        self.restore_remote_request(reference, target, allow_unregistered_import, None)
            .await
    }

    /// Stops quiescent restoration after a durable catalog recovery boundary.
    ///
    /// # Errors
    /// Returns ordinary restore refusals or the requested injected interruption.
    #[cfg(feature = "test-util")]
    pub async fn restore_remote_until(
        &self,
        reference: &RestorePointRef,
        target: storage::remote::RemoteStorage,
        allow_unregistered_import: bool,
        stop: &str,
    ) -> Result<RestoredRemoteCatalog> {
        self.restore_remote_request(
            reference,
            target,
            allow_unregistered_import,
            Some(stop.to_owned()),
        )
        .await
    }

    async fn restore_remote_request(
        &self,
        reference: &RestorePointRef,
        target: storage::remote::RemoteStorage,
        allow_unregistered_import: bool,
        stop: Option<String>,
    ) -> Result<RestoredRemoteCatalog> {
        self.remote_maintenance()?;
        let generation = target
            .maintenance_generation
            .ok_or_else(|| restore::invalid("target must declare quiescent maintenance"))?;
        let storage::Objects::Remote(source_objects) = &self.inner.objects else {
            return Err(locations::unsupported());
        };
        if generation == self.remote_maintenance()?
            || !source_objects.same_placement(&target.objects)
        {
            return Err(restore::invalid(
                "restore requires a fresh generation with the exact object scope",
            ));
        }
        let memory = self.request_memory(reference)?;
        let reference = reference.clone();
        self.maintain_work(memory, move |engine| async move {
            engine
                .restore_remote_admitted(
                    reference,
                    target,
                    generation,
                    allow_unregistered_import,
                    stop.as_deref(),
                )
                .await
        })
        .await
    }

    async fn restore_remote_admitted(
        &self,
        reference: RestorePointRef,
        target: storage::remote::RemoteStorage,
        generation: uuid::Uuid,
        allow_unregistered_import: bool,
        stop: Option<&str>,
    ) -> Result<RestoredRemoteCatalog> {
        let budget = &self.inner.lifecycle.budget;
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(10))?);
        let point = self.remote_point(&reference, &memory).await?;
        let source_generation = self.remote_maintenance()?;
        let mut bytes = 0;
        let image: CatalogImage = inputs::read_manifest(
            &self.inner.objects,
            &point.catalog,
            budget,
            &mut bytes,
            &memory,
        )
        .await?;
        check_image(&image, &point)?;
        let restored = RestoredRemoteCatalog {
            point: reference.id.clone(),
            generation,
            tables: image.tables.len(),
            retained_bytes: point.objects.iter().try_fold(0u64, |sum, object| {
                sum.checked_add(object.bytes)
                    .ok_or_else(|| restore::invalid("restore byte count overflow"))
            })?,
        };
        let fresh = Lifecycle::new(target.catalog.clone(), budget.clone());
        let owner_ident = crate::catalog::lifecycle_ident(&reference.dataset);
        let target_started = restore_intent(
            &target.catalog,
            &image,
            &reference,
            source_generation,
            generation,
            allow_unregistered_import,
            false,
        )
        .await?;
        if target.catalog.table_exists(&owner_ident).await? {
            let view = fresh.open(&reference.dataset).await?;
            if let Some(done) = restore_reference(&fresh, &view, &point_key(&reference.id)).await? {
                if done == reference && target_started {
                    return Ok(restored);
                }
                return Err(restore::invalid(
                    "target contains another completed restore",
                ));
            }
        }
        // A current explicit retirement always wins over a saved point. Import
        // applies only when current authority is absent, never to a known refusal.
        let mut current_authority = false;
        if self
            .inner
            .lifecycle
            .catalog
            .table_exists(&owner_ident)
            .await?
        {
            let view = self.existing_lifecycle_view(&reference.dataset).await?;
            match restore_reference(&self.inner.lifecycle, &view, &point_key(&reference.id)).await?
            {
                Some(saved) if saved == reference => {
                    current_authority = true;
                }
                None if allow_unregistered_import => {}
                _ => return Err(restore::invalid("current restore authority differs")),
            }
        } else if !allow_unregistered_import && !target_started {
            return Err(restore::invalid("catalog-loss import must be explicit"));
        }
        // Reject reservations from every surviving current lifecycle before any
        // catalog effect; a late exact deleter can only affect unavailable keys.
        let current = self.remote_registration_inventory(&point.objects).await?;
        check_target(&fresh, &image, &reference).await?;
        restore_intent(
            &target.catalog,
            &image,
            &reference,
            source_generation,
            generation,
            allow_unregistered_import,
            true,
        )
        .await?;
        stop_after(stop, "restore-remote-intent")?;
        let storage::Objects::Remote(objects) = &self.inner.objects else {
            return Err(locations::unsupported());
        };
        if current_authority || (target_started && !allow_unregistered_import) {
            // Catalog loss moves no immutable bytes. A live point or saved
            // non-import intent preserves their qualified identity; check presence.
            self.inner
                .objects
                .check_available(point.objects.clone(), budget)
                .await?;
        } else {
            // Explicit import has no surviving live promise to trust. Verify
            // the saved identities once for this unacknowledged restore attempt.
            for object in &point.objects {
                objects.verify(object, budget).await?;
            }
        }
        for old in &current {
            let ident = TableIdent::from_strs(old.table.split('.'))?;
            let native = self.inner.lifecycle.catalog.load_table(&ident).await?;
            if native.metadata_location_result()? != old.metadata.path
                || native.metadata().uuid().to_string() != old.table_uuid
            {
                return Err(restore::invalid(
                    "old registration changed during quiescent restore",
                ));
            }
            if let Err(error) = self.inner.lifecycle.catalog.drop_table(&ident).await
                && self.inner.lifecycle.catalog.table_exists(&ident).await?
            {
                return Err(error.into());
            }
        }
        stop_after(stop, "restore-remote-detached")?;
        for (name, properties) in &image.namespaces {
            let namespace = NamespaceIdent::new(name.clone());
            if !target.catalog.namespace_exists(&namespace).await? {
                target
                    .catalog
                    .create_namespace(&namespace, properties.clone().into_iter().collect())
                    .await?;
            }
        }
        for saved in &image.tables {
            let ident = TableIdent::from_strs(saved.table.split('.'))?;
            let table = if target.catalog.table_exists(&ident).await? {
                target.catalog.load_table(&ident).await?
            } else {
                match target
                    .catalog
                    .register_table(&ident, saved.metadata.path.clone())
                    .await
                {
                    Ok(table) => table,
                    Err(error) => {
                        if !target.catalog.table_exists(&ident).await? {
                            return Err(error.into());
                        }
                        target.catalog.load_table(&ident).await?
                    }
                }
            };
            check_restored_registration(&fresh, saved, table).await?;
        }
        stop_after(stop, "restore-remote-registered")?;
        // Captured unfinished operations may name keys deleted after capture.
        // Fence every dataset before admitting any client to this generation.
        // Completed acknowledgments remain exact; new operations can reuse phases.
        for saved in image
            .tables
            .iter()
            .filter(|entry| entry.authority.is_some())
        {
            fence_registration(&fresh, saved).await?;
        }
        stop_after(stop, "restore-remote-fenced")?;
        let view = fresh.open(&reference.dataset).await?;
        check_restore_reservation(&fresh, &view, &reference, &point).await?;
        if let Err(error) = fresh
            .commit(&view, remote_completion(&reference, &point)?)
            .await
        {
            let view = fresh.open(&reference.dataset).await?;
            if restore_reference(&fresh, &view, &point_key(&reference.id))
                .await?
                .as_ref()
                != Some(&reference)
            {
                return Err(error);
            }
        }
        Ok(restored)
    }

    async fn remote_registration_inventory(
        &self,
        closure: &[ObjectRef],
    ) -> Result<Vec<Registration>> {
        let lifecycle = &self.inner.lifecycle;
        let mut registrations = Vec::new();
        let mut charged = 0usize;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        for namespace in lifecycle.catalog.list_namespaces(None).await? {
            let ident = TableIdent::new(namespace.clone(), "lifecycle".into());
            let tables = lifecycle.catalog.list_tables(&namespace).await?;
            if tables.iter().any(|table| table.name() != "lifecycle") && !tables.contains(&ident) {
                return Err(restore::invalid(
                    "partial source detach needs explicit reconciliation",
                ));
            }
            let mut lifecycle_table = None;
            if tables.contains(&ident) {
                let table = lifecycle.catalog.load_table(&ident).await?;
                let view = View {
                    revision: table.metadata().last_sequence_number(),
                    table,
                    logical_identifier: ident.clone(),
                };
                let rows = lifecycle.rows(&view).await?;
                self.validate_removal_records(&rows, &view)?;
                check_removals_finished(&rows, &lifecycle.budget)?;
                source_authority(&rows)?;
                for object in closure {
                    // Marker keys use exact URI, independent of object owner.
                    if rows.contains_key(&retention::owners::unavailable_key(&object.path)) {
                        return Err(restore::invalid(
                            "restore references a reserved deletion target",
                        ));
                    }
                }
                lifecycle_table = Some(view.table);
            }
            for ident in tables {
                let table = if ident.name() == "lifecycle" {
                    lifecycle_table
                        .take()
                        .ok_or_else(|| restore::invalid("lifecycle registration disappeared"))?
                } else {
                    lifecycle.catalog.load_table(&ident).await?
                };
                self.inner
                    .objects
                    .validate_retained_path(table.metadata_location_result()?)?;
                let owner = self.retained_owner(table.metadata().location())?;
                if crate::catalog::lifecycle_ident(&owner).namespace() != ident.namespace() {
                    return Err(restore::invalid(
                        "registration is outside its exact owner namespace",
                    ));
                }
                let registration = Registration {
                    table: ident.to_string(),
                    table_uuid: table.metadata().uuid().to_string(),
                    metadata: ObjectRef {
                        owner,
                        path: table.metadata_location_result()?.into(),
                        bytes: 0,
                        digest: String::new(),
                    },
                    revision: table.metadata().last_sequence_number(),
                    authority: None,
                };
                charged = charged
                    .saturating_add(inputs::metadata_size(&registration, limit)?)
                    .saturating_add(128);
                if charged > limit {
                    return Err(Error::refusal(
                        "capacity",
                        "remote catalog inventory exceeds metadata capacity",
                    ));
                }
                registrations.push(registration);
            }
        }
        // Detach data first, lifecycle last, so an interrupted pass can still
        // reconcile current unavailable-key authority from remaining lifecycle.
        registrations
            .sort_by_key(|entry| (entry.table.ends_with(".lifecycle"), entry.table.clone()));
        Ok(registrations)
    }
}

fn check_image(image: &CatalogImage, point: &RemoteRestorePoint) -> Result<()> {
    if image.format != remote::FORMAT
        || image.generation != point.generation
        || !image
            .tables
            .windows(2)
            .all(|pair| pair[0].table < pair[1].table)
        || image.tables.is_empty()
    {
        return Err(restore::invalid("catalog image identity differs"));
    }
    let owner = crate::catalog::lifecycle_ident(&point.dataset);
    if !image
        .namespaces
        .contains_key(&owner.namespace().to_string())
        || image.namespaces.keys().any(|namespace| {
            let expected = format!("{namespace}.lifecycle");
            image
                .tables
                .binary_search_by(|table| table.table.cmp(&expected))
                .is_err()
        })
    {
        return Err(restore::invalid(
            "catalog image lacks required lifecycle authority",
        ));
    }
    for table in &image.tables {
        let ident = TableIdent::from_strs(table.table.split('.'))?;
        if ident.namespace().as_ref().len() != 1
            || crate::catalog::lifecycle_ident(&table.metadata.owner).namespace()
                != ident.namespace()
            || !image
                .namespaces
                .contains_key(&ident.namespace().to_string())
            || !point
                .objects
                .binary_search_by(|object| object.path.cmp(&table.metadata.path))
                .ok()
                .is_some_and(|index| point.objects[index] == table.metadata)
            || uuid::Uuid::parse_str(&table.table_uuid).is_err()
            || (ident.name() == "lifecycle") != table.authority.is_some()
            || table.authority.as_ref().is_some_and(|authority| {
                authority.epoch == u64::MAX
                    || authority.digest.len() != 64
                    || !authority
                        .digest
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(restore::invalid("catalog image registration differs"));
        }
    }
    Ok(())
}

async fn capture_authority(lifecycle: &Lifecycle, view: &View) -> Result<RestoreAuthority> {
    let rows = lifecycle.rows(view).await?;
    let _memory = rows.reserve_decoded(&lifecycle.budget, &["authority"])?;
    let authority = source_authority(&rows)?;
    if authority.epoch == u64::MAX {
        return Err(restore::invalid(
            "captured authority epoch cannot be advanced",
        ));
    }
    Ok(RestoreAuthority {
        epoch: authority.epoch,
        digest: storage::json_digest(&authority)?.to_hex().to_string(),
    })
}

// Accept exactly the captured registration or the single restore fence commit.
// An existing target intent is checked by the caller before either is adopted.
async fn check_restored_registration(
    lifecycle: &Lifecycle,
    saved: &Registration,
    table: iceberg::table::Table,
) -> Result<View> {
    let view = View {
        revision: table.metadata().last_sequence_number(),
        table,
        logical_identifier: TableIdent::from_strs(saved.table.split('.'))?,
    };
    if view.table.metadata().uuid().to_string() != saved.table_uuid {
        return Err(restore::invalid("restored native registration differs"));
    }
    if view.table.metadata_location_result()? == saved.metadata.path
        && view.revision == saved.revision
    {
        return Ok(view);
    }
    let expected = saved
        .authority
        .as_ref()
        .ok_or_else(|| restore::invalid("restored native registration differs"))?;
    if saved.revision.checked_add(1) != Some(view.revision) {
        return Err(restore::invalid(
            "restored authority has an unexpected commit",
        ));
    }
    let rows = lifecycle.rows(&view).await?;
    let _memory = rows.reserve_decoded(&lifecycle.budget, &["authority"])?;
    let Some((revision, _, mut authority)) = rows.get_record::<Authority>("authority")? else {
        return Err(restore::invalid("restored authority fence is missing"));
    };
    if revision != view.revision
        || expected.epoch.checked_add(1) != Some(authority.epoch)
        || rows
            .prefix("")
            .any(|(key, revision, _)| key != "authority" && revision > saved.revision)
    {
        return Err(restore::invalid("restored authority fence differs"));
    }
    authority.epoch = expected.epoch;
    if storage::json_digest(&authority)?.to_hex().as_str() != expected.digest {
        return Err(restore::invalid("restored authority content differs"));
    }
    Ok(view)
}

async fn fence_registration(lifecycle: &Lifecycle, saved: &Registration) -> Result<View> {
    let expected = saved
        .authority
        .as_ref()
        .ok_or_else(|| restore::invalid("lifecycle fence input is missing"))?;
    let ident = TableIdent::from_strs(saved.table.split('.'))?;
    let view = check_restored_registration(
        lifecycle,
        saved,
        lifecycle.catalog.load_table(&ident).await?,
    )
    .await?;
    if view.revision != saved.revision {
        return Ok(view);
    }
    let rows = lifecycle.rows(&view).await?;
    let _memory = rows.reserve_decoded(&lifecycle.budget, &["authority"])?;
    let mut authority: Authority = rows
        .get_record("authority")?
        .map(|(_, _, value)| value)
        .unwrap_or_default();
    if storage::json_digest(&authority)?.to_hex().as_str() != expected.digest
        || authority.epoch != expected.epoch
    {
        return Err(restore::invalid("captured lifecycle authority differs"));
    }
    authority.epoch = expected
        .epoch
        .checked_add(1)
        .ok_or_else(|| restore::invalid("restored authority epoch overflow"))?;
    match lifecycle
        .commit(&view, vec![record("authority", &authority)?])
        .await
    {
        Ok(fenced) => Ok(fenced),
        Err(error) => {
            let observed = check_restored_registration(
                lifecycle,
                saved,
                lifecycle.catalog.load_table(&ident).await?,
            )
            .await?;
            if observed.revision == saved.revision {
                Err(error)
            } else {
                Ok(observed)
            }
        }
    }
}

fn check_removals_finished(rows: &crate::catalog::SnapshotRows, budget: &Budget) -> Result<()> {
    for (key, _, _) in rows.prefix("removal-state/") {
        let _memory = rows.reserve_decoded(budget, &[key])?;
        let (_, _, progress): (_, _, RemovalProgress) = rows
            .get_record(key)?
            .ok_or_else(|| restore::invalid("removal progress is missing"))?;
        if progress
            .objects
            .iter()
            .chain(&progress.registrations)
            .any(|outcome| !outcome.finished())
        {
            return Err(Error::refusal(
                "restore_removal_pending",
                "finish saved removal decisions before restoring a catalog",
            ));
        }
    }
    Ok(())
}

async fn check_target(
    lifecycle: &Lifecycle,
    image: &CatalogImage,
    reference: &RestorePointRef,
) -> Result<()> {
    let catalog = &lifecycle.catalog;
    for namespace in catalog.list_namespaces(None).await? {
        let expected = image
            .namespaces
            .get(&namespace.to_string())
            .ok_or_else(|| restore::invalid("target generation is not fresh"))?;
        let mut actual = catalog
            .get_namespace(&namespace)
            .await?
            .properties()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut expected = expected.clone();
        if &namespace == crate::catalog::lifecycle_ident(&reference.dataset).namespace() {
            actual.remove("docspec.restore.intent");
            expected.remove("docspec.restore.intent");
        }
        if actual != expected {
            return Err(restore::invalid("target namespace properties differ"));
        }
        for ident in catalog.list_tables(&namespace).await? {
            let saved = image
                .tables
                .iter()
                .find(|entry| entry.table == ident.to_string())
                .ok_or_else(|| restore::invalid("target contains an unknown registration"))?;
            check_restored_registration(lifecycle, saved, catalog.load_table(&ident).await?)
                .await?;
        }
    }
    Ok(())
}

fn remote_completion(
    reference: &RestorePointRef,
    point: &RemoteRestorePoint,
) -> Result<Vec<(String, serde_json::Value)>> {
    let mut protection = restore_reservation(reference)?;
    protection.retired = true;
    Ok(vec![
        record(format!("protection/{}", protection.id), protection)?,
        record(point_key(&reference.id), reference)?,
        record(
            format!("restore-materialized/{}", reference.id),
            RestoreMaterialized {
                reference: reference.clone(),
                catalog: point.catalog.clone(),
                backups: Vec::new(),
            },
        )?,
        record(
            format!("restore-reuse/{}", reference.id),
            RestoreReuse {
                dataset: reference.dataset.clone(),
                id: reference.id.clone(),
                objects: Vec::new(),
            },
        )?,
    ])
}

fn restore_reservation(reference: &RestorePointRef) -> Result<Protection> {
    let directory = reference
        .manifest
        .path
        .strip_suffix("/point.json")
        .ok_or_else(|| restore::invalid("restore artifact directory differs"))?;
    let attempt = Id::new(
        directory
            .rsplit('/')
            .next()
            .ok_or_else(|| restore::invalid("restore attempt differs"))?,
    )?;
    Ok(Protection {
        id: attempt,
        consumer: reference.dataset.clone(),
        purpose: format!("{}/restore", reference.id),
        paths: vec![format!("{directory}/")],
        retired: false,
    })
}

async fn check_restore_reservation(
    lifecycle: &Lifecycle,
    view: &View,
    reference: &RestorePointRef,
    point: &RemoteRestorePoint,
) -> Result<()> {
    remote::validate(reference, point)?;
    let expected = restore_reservation(reference)?;
    let key = format!("protection/{}", expected.id);
    let rows = lifecycle.rows(view).await?;
    let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
    let Some((revision, _, saved)) = rows.get_record::<Protection>(&key)? else {
        return Err(restore::invalid("captured restore reservation is missing"));
    };
    if revision <= 0 || revision > view.revision || saved != expected {
        return Err(restore::invalid("captured restore reservation differs"));
    }
    Ok(())
}

// The fresh target records its exact intended point before old registrations
// disappear. Namespace creation is maintained and conditional; an existing
// different intent is never overwritten. This is not a mutable latest pointer.
async fn restore_intent(
    catalog: &Arc<dyn Catalog>,
    image: &CatalogImage,
    reference: &RestorePointRef,
    source_generation: uuid::Uuid,
    generation: uuid::Uuid,
    allow_unregistered_import: bool,
    create: bool,
) -> Result<bool> {
    const KEY: &str = "docspec.restore.intent";
    let namespace = crate::catalog::lifecycle_ident(&reference.dataset)
        .namespace()
        .clone();
    let mut expected = image
        .namespaces
        .get(&namespace.to_string())
        .ok_or_else(|| restore::invalid("restore owner namespace is absent"))?
        .clone();
    expected.insert(
        KEY.into(),
        serde_json::to_string(&(
            reference,
            image.generation,
            source_generation,
            generation,
            allow_unregistered_import,
        ))?,
    );
    if !catalog.namespace_exists(&namespace).await? {
        if !create {
            return Ok(false);
        }
        if let Err(error) = catalog
            .create_namespace(&namespace, expected.clone().into_iter().collect())
            .await
            && !catalog.namespace_exists(&namespace).await?
        {
            return Err(error.into());
        }
    }
    let actual = catalog.get_namespace(&namespace).await?;
    if actual.properties().iter().collect::<BTreeMap<_, _>>()
        != expected.iter().collect::<BTreeMap<_, _>>()
    {
        return Err(restore::invalid(
            "target namespace is bound to another restore",
        ));
    }
    Ok(true)
}

fn source_authority(rows: &crate::catalog::SnapshotRows) -> Result<Authority> {
    let authority: Authority = rows
        .get_record("authority")?
        .map(|(_, _, value)| value)
        .unwrap_or_default();
    if let Some(selected) = &authority.selected
        && (!selected.computed.is_empty()
            || selected.related.is_some()
            || selected.evidence.is_some()
            || selected.relationship != SourceRelationship::Independent)
    {
        return Err(restore::invalid(
            "remote maintenance currently requires an independent source-only selection",
        ));
    }
    Ok(authority)
}

#[cfg(test)]
mod tests;
