//! Explicit exact-object removal with durable intent and bounded reconciliation.

use super::*;
use crate::catalog::{SnapshotRows, history_row_bytes};
use iceberg::{NamespaceIdent, TableIdent};

#[cfg(test)]
mod fault_tests;
#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;

const FAILURE_CODE_BYTES: usize = 64;

pub(crate) fn intent_key(id: &Id) -> String {
    format!("removal-intent/{id}")
}

fn state_key(id: &Id) -> String {
    format!("removal-state/{id}")
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_removal",
        "removal identities, outcomes or reservations disagree",
    )
}

fn conflict() -> Error {
    Error::refusal(
        "removal_conflict",
        "removal identity already binds a different exact plan",
    )
}

/// Validates current outcomes and the capacity needed for their final acknowledgment.
/// This pure check is shared by cold replay, ordinary commits and removal execution.
pub(crate) fn validate_progress(
    intent: &RemovalIntent,
    progress: &RemovalProgress,
) -> Result<usize> {
    if progress.intent_revision != intent.revision
        || intent.revision <= 0
        || progress.objects.len() != intent.objects.len()
        || progress.registrations.len() != intent.registrations.len()
    {
        return Err(invalid());
    }
    for outcome in progress.objects.iter().chain(&progress.registrations) {
        if let RemovalOutcome::Failed { code } = outcome
            && (code.is_empty()
                || code.len() > FAILURE_CODE_BYTES
                || !code
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
        {
            return Err(invalid());
        }
    }
    if progress.objects.iter().any(RemovalOutcome::finished)
        && !progress.registrations.iter().all(RemovalOutcome::finished)
    {
        return Err(invalid());
    }
    let floor = outcome_charge(intent)?;
    let complete = finished(progress);
    if (complete && progress.reserved_future_bytes != 0)
        || (!complete && progress.reserved_future_bytes < floor as u64)
    {
        return Err(invalid());
    }
    Ok(floor)
}

// Maximum JSON widths for fixed fields and bounded ASCII error codes; arrays add
// one comma per entry (conservative for their final entry). No payload is copied.
fn outcome_charge(intent: &RemovalIntent) -> Result<usize> {
    let empty = RemovalProgress {
        intent_revision: i64::MAX,
        reserved_future_bytes: u64::MAX,
        registrations: Vec::new(),
        objects: Vec::new(),
    };
    let value = serde_json::to_string(&empty)?;
    let longest = serde_json::to_string(&RemovalOutcome::Failed {
        code: "x".repeat(FAILURE_CODE_BYTES),
    })?
    .len()
    .checked_add(1)
    .ok_or_else(invalid)?;
    intent
        .objects
        .len()
        .checked_add(intent.registrations.len())
        .and_then(|count| count.checked_mul(longest))
        .and_then(|bytes| {
            bytes.checked_add(history_row_bytes(&state_key(&intent.plan.removal), &value))
        })
        .ok_or_else(invalid)
}

fn finished(progress: &RemovalProgress) -> bool {
    progress
        .objects
        .iter()
        .chain(&progress.registrations)
        .all(RemovalOutcome::finished)
}

fn failure(error: Error) -> RemovalOutcome {
    let code = error.code();
    RemovalOutcome::Failed {
        code: if code.len() <= FAILURE_CODE_BYTES {
            code
        } else {
            "removal_effect"
        }
        .into(),
    }
}

impl Engine {
    fn removal_catalog(&self) -> Result<()> {
        self.retirement_catalog()?;
        if !cfg!(unix) {
            return Err(Error::refusal(
                "removal_platform",
                "local removal identity checks require Unix",
            ));
        }
        Ok(())
    }

    async fn existing_removal_view(&self, dataset: &Id) -> Result<View> {
        let table = self
            .inner
            .lifecycle
            .catalog
            .load_table(&crate::catalog::lifecycle_ident(dataset))
            .await
            .map_err(|error| {
                if error.kind() == iceberg::ErrorKind::TableNotFound {
                    Error::refusal("no_removal", "no removal intent has been committed")
                } else {
                    error.into()
                }
            })?;
        Ok(View {
            revision: table.metadata().last_sequence_number(),
            table,
        })
    }
    /// Previews explicit owned files without hashing or deleting payloads.
    ///
    /// # Errors
    /// Refuses unsupported catalogs, unqualified identities, conflicting paths,
    /// unknown retention roots or exhausted metadata capacity.
    pub async fn plan_removal(
        &self,
        dataset: &Id,
        removal: &Id,
        objects: &[ObjectRef],
        reason: &str,
    ) -> Result<RemovalPlan> {
        let memory = self.request_memory(&(dataset, removal, objects, reason))?;
        let dataset = dataset.clone();
        let removal = removal.clone();
        let objects = objects.to_vec();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine
                .removal_plan_admitted(dataset, removal, objects, reason)
                .await
        })
        .await
    }

    async fn removal_plan_admitted(
        &self,
        dataset: Id,
        removal: Id,
        mut objects: Vec<ObjectRef>,
        reason: String,
    ) -> Result<RemovalPlan> {
        self.removal_catalog()?;
        if reason.trim().is_empty() || objects.is_empty() {
            return Err(Error::refusal(
                "invalid_removal",
                "removal needs exact candidates and a reason",
            ));
        }
        objects.sort_by(|a, b| a.path.cmp(&b.path));
        if objects
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path && pair[0] != pair[1])
        {
            return Err(invalid());
        }
        objects.dedup();
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        {
            // Owner creation proofs decode historical rows independently of the
            // candidate count. Release their allowance before the inventory walk.
            let _proof_memory = lifecycle.budget.reserve(limit.saturating_mul(8))?;
            self.qualify_removal_objects(&view, &rows, &dataset, &objects)
                .await?;
        }
        drop(rows);
        // Keep returned inspection and native-registration collections charged
        // after their helpers release transient traversal/decoder allowances.
        let _memory = lifecycle.budget.reserve(limit.saturating_mul(4))?;
        let inspection = self.inspect_retention_admitted(&objects).await?;
        drop(objects);
        let registrations = self
            .removal_registrations(&inspection.objects, &inspection.boundaries)
            .await?;
        let plan = RemovalPlan {
            dataset,
            removal,
            reason,
            table_uuid: view.table.metadata().uuid().to_string(),
            inspection,
            registrations,
        };
        inputs::metadata_size(&plan, lifecycle.budget.limits.max_metadata_bytes)?;
        Ok(plan)
    }

    async fn qualify_removal_objects(
        &self,
        view: &View,
        rows: &SnapshotRows,
        owner: &Id,
        objects: &[ObjectRef],
    ) -> Result<()> {
        for object in objects {
            super::inventory::validate_object(object)?;
            self.check_object_owner(rows, owner, object)?;
            super::owners::available(rows, view, object)?;
        }
        super::owners::confirm_creation(
            rows,
            view,
            objects,
            self.inner.lifecycle.budget.limits.max_metadata_bytes,
        )
        .await
    }

    /// Executes only previewed files that remain eligible, or reconciles the saved intent.
    ///
    /// # Errors
    /// Refuses changed identities, new native references, unsafe local paths or
    /// insufficient durable outcome capacity. Effect failures remain inspectable.
    pub async fn remove_objects(&self, plan: &RemovalPlan) -> Result<RemovalStatus> {
        let memory = self.request_memory(plan)?;
        let plan = plan.clone();
        self.maintain_work(memory, move |engine| async move {
            engine.remove_admitted(&plan, None).await
        })
        .await
    }

    /// Interrupts removal at a durable effect boundary for process qualification.
    ///
    /// # Errors
    /// Returns removal failures or `injected_failure` at the requested boundary.
    #[cfg(feature = "test-util")]
    pub async fn removal_until(&self, plan: &RemovalPlan, phase: &str) -> Result<RemovalStatus> {
        let memory = self.request_memory(&(plan, phase))?;
        let plan = plan.clone();
        let phase = phase.to_owned();
        self.maintain_work(memory, move |engine| async move {
            engine.remove_admitted(&plan, Some(&phase)).await
        })
        .await
    }

    /// Reads saved removal outcomes without touching payloads.
    ///
    /// # Errors
    /// Refuses missing or malformed removal records and unsupported catalogs.
    pub async fn inspect_removal(&self, dataset: &Id, removal: &Id) -> Result<RemovalStatus> {
        let memory = self.request_memory(&(dataset, removal))?;
        let dataset = dataset.clone();
        let removal = removal.clone();
        self.admit_work(memory, move |engine| async move {
            engine.removal_catalog()?;
            let view = engine.existing_removal_view(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let _memory = rows.reserve_decoded(
                &engine.inner.lifecycle.budget,
                &[&intent_key(&removal), &state_key(&removal)],
            )?;
            let (intent, progress) = engine.saved_removal(&rows, &view, &removal)?;
            status(intent, progress)
        })
        .await
    }

    /// Resumes the exact saved removal without expanding its scope.
    ///
    /// # Errors
    /// Refuses missing or malformed intent, changed native heads, or exhausted capacity.
    pub async fn resume_removal(&self, dataset: &Id, removal: &Id) -> Result<RemovalStatus> {
        let memory = self.request_memory(&(dataset, removal))?;
        let dataset = dataset.clone();
        let removal = removal.clone();
        self.maintain_work(memory, move |engine| async move {
            engine.removal_catalog()?;
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            let view = engine.existing_removal_view(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let _memory = rows.reserve_decoded(
                &engine.inner.lifecycle.budget,
                &[&intent_key(&removal), &state_key(&removal)],
            )?;
            let (intent, progress) = engine.saved_removal(&rows, &view, &removal)?;
            engine
                .finish_removal(view, intent, progress, None, false)
                .await
        })
        .await
    }

    async fn remove_admitted(
        &self,
        plan: &RemovalPlan,
        stop: Option<&str>,
    ) -> Result<RemovalStatus> {
        self.removal_catalog()?;
        let lock = self.lock(&plan.dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&plan.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        if rows.contains_key(&intent_key(&plan.removal)) {
            let _memory = rows.reserve_decoded(
                &lifecycle.budget,
                &[&intent_key(&plan.removal), &state_key(&plan.removal)],
            )?;
            let (intent, progress) = self.saved_removal(&rows, &view, &plan.removal)?;
            if intent.plan != *plan {
                return Err(conflict());
            }
            return self
                .finish_removal(view, intent, progress, stop, false)
                .await;
        }
        self.validate_removal_plan(&view, plan)?;
        let objects = plan
            .inspection
            .objects
            .iter()
            .map(|entry| entry.object.clone())
            .collect::<Vec<_>>();
        {
            // Historical creation-proof decoding is bounded separately from the
            // request-owned candidate list and ends before protection traversal.
            let _proof_memory = lifecycle.budget.reserve(limit.saturating_mul(8))?;
            self.qualify_removal_objects(&view, &rows, &plan.dataset, &objects)
                .await?;
        }
        drop(rows);
        // Inventory charges bound the fresh inspection, selected subset, native
        // registrations and index lists. The request permit covers plan copies.
        let _memory = lifecycle.budget.reserve(limit.saturating_mul(4))?;
        let fresh = self.inspect_retention_admitted(&objects).await?;
        drop(objects);
        let chosen = plan
            .inspection
            .objects
            .iter()
            .zip(&fresh.objects)
            .enumerate()
            .filter_map(|(index, (old, new))| {
                (old.blocker.is_none() && new.blocker.is_none()).then_some(index)
            })
            .collect::<Vec<_>>();
        let selected = chosen
            .iter()
            .map(|index| fresh.objects[*index].clone())
            .collect::<Vec<_>>();
        let current_native = self
            .removal_registrations(&selected, &fresh.boundaries)
            .await?;
        drop(selected);
        let mut registrations = Vec::new();
        for registration in current_native {
            let index = plan
                .registrations
                .binary_search(&registration)
                .map_err(|_| {
                    Error::refusal(
                        "stale_removal",
                        "a new or changed native head references candidates; capture a fresh plan",
                    )
                })?;
            registrations.push(index);
        }
        // Restore the original encoding/commit allowance only after nested
        // native inventory and its file-reader reservations have been released.
        let _encoding_memory = lifecycle.budget.reserve(limit.saturating_mul(8))?;
        let intent = RemovalIntent {
            plan: plan.clone(),
            revision: view.revision.checked_add(1).ok_or_else(invalid)?,
            boundaries: fresh.boundaries,
            blocked: plan
                .inspection
                .objects
                .iter()
                .zip(fresh.objects)
                .filter_map(|(old, new)| {
                    (old.blocker.is_none() && new.blocker.is_some()).then_some(new)
                })
                .collect(),
            objects: chosen,
            registrations,
        };
        let mut progress = RemovalProgress {
            intent_revision: intent.revision,
            reserved_future_bytes: 0,
            registrations: vec![RemovalOutcome::Unresolved; intent.registrations.len()],
            objects: vec![RemovalOutcome::Unresolved; intent.objects.len()],
        };
        if !finished(&progress) {
            progress.reserved_future_bytes = (outcome_charge(&intent)? as u64)
                .checked_mul(2)
                .ok_or_else(invalid)?;
        }
        let mut changes = vec![
            record(intent_key(&plan.removal), &intent)?,
            record(state_key(&plan.removal), &progress)?,
        ];
        for index in &intent.objects {
            let object = &plan.inspection.objects[*index].object;
            changes.push(record(
                super::owners::unavailable_key(&object.path),
                super::owners::UnavailableObject {
                    object: object.clone(),
                    intent: plan.removal.clone(),
                    revision: intent.revision,
                },
            )?);
        }
        let after = match lifecycle.commit(&view, changes).await {
            Ok(after) => after,
            Err(error) => {
                let after = lifecycle.open(&plan.dataset).await?;
                let rows = lifecycle.rows(&after).await?;
                if !rows.contains_key(&intent_key(&plan.removal)) {
                    return Err(error);
                }
                let (saved, saved_progress) = self.saved_removal(&rows, &after, &plan.removal)?;
                if saved != intent || saved_progress != progress {
                    return Err(conflict());
                }
                after
            }
        };
        stop_after(stop, "removal-intent")?;
        self.finish_removal(after, intent, progress, stop, true)
            .await
    }

    fn validate_removal_plan(&self, view: &View, plan: &RemovalPlan) -> Result<()> {
        if plan.reason.trim().is_empty()
            || plan.table_uuid != view.table.metadata().uuid().to_string()
            || crate::catalog::lifecycle_ident(&plan.dataset) != *view.table.identifier()
            || plan.inspection.objects.is_empty()
            || !plan
                .inspection
                .objects
                .windows(2)
                .all(|p| p[0].object.path < p[1].object.path)
            || !plan.registrations.windows(2).all(|p| p[0] < p[1])
        {
            return Err(invalid());
        }
        let boundary = plan
            .inspection
            .boundaries
            .iter()
            .find(|b| b.table == view.table.identifier().to_string())
            .ok_or_else(invalid)?;
        if boundary.revision <= 0 || boundary.revision > view.revision {
            return Err(invalid());
        }
        inputs::metadata_size(plan, self.inner.lifecycle.budget.limits.max_metadata_bytes)?;
        Ok(())
    }

    fn saved_removal(
        &self,
        rows: &SnapshotRows,
        view: &View,
        id: &Id,
    ) -> Result<(RemovalIntent, RemovalProgress)> {
        let (revision, _, intent) = rows
            .get_record::<RemovalIntent>(&intent_key(id))?
            .ok_or_else(|| Error::refusal("no_removal", "no removal intent has been committed"))?;
        self.validate_removal_intent(rows, view, &intent_key(id), revision, &intent)?;
        let (_, _, progress) = rows
            .get_record::<RemovalProgress>(&state_key(id))?
            .ok_or_else(invalid)?;
        validate_progress(&intent, &progress)?;
        Ok((intent, progress))
    }

    pub(super) fn validate_removal_records(
        &self,
        rows: &SnapshotRows,
        view: &View,
    ) -> Result<BTreeSet<String>> {
        let mut markers = BTreeSet::new();
        let mut bytes = 0usize;
        for (key, revision, json) in rows.prefix("removal-intent/") {
            let intent: RemovalIntent = serde_json::from_str(json)?;
            self.validate_removal_intent(rows, view, key, revision, &intent)?;
            for index in &intent.objects {
                let marker = super::owners::unavailable_key(
                    &intent.plan.inspection.objects[*index].object.path,
                );
                bytes = bytes.saturating_add(marker.len()).saturating_add(128);
                if bytes > self.inner.lifecycle.budget.limits.max_metadata_bytes
                    || !markers.insert(marker)
                {
                    return Err(invalid());
                }
            }
        }
        // Catalog replay checks exact state-to-intent membership as well. Keep
        // inventory explicit rather than silently accepting a future key format.
        for (key, _, _) in rows.prefix("removal-state/") {
            let id = Id::new(key.strip_prefix("removal-state/").ok_or_else(invalid)?)?;
            if !rows.contains_key(&intent_key(&id)) {
                return Err(invalid());
            }
        }
        Ok(markers)
    }

    pub(super) fn validate_removal_intent(
        &self,
        rows: &SnapshotRows,
        view: &View,
        key: &str,
        revision: i64,
        intent: &RemovalIntent,
    ) -> Result<()> {
        self.validate_removal_plan(view, &intent.plan)?;
        if key != intent_key(&intent.plan.removal)
            || revision != intent.revision
            || revision <= 0
            || revision > view.revision
            || !indices_valid(&intent.objects, intent.plan.inspection.objects.len())
            || !indices_valid(&intent.registrations, intent.plan.registrations.len())
            || (intent.objects.is_empty() && !intent.registrations.is_empty())
        {
            return Err(invalid());
        }
        for index in &intent.registrations {
            self.removal_registration_ident(&intent.plan.registrations[*index])?;
        }
        for index in &intent.objects {
            let entry = &intent.plan.inspection.objects[*index];
            if entry.blocker.is_some() {
                return Err(invalid());
            }
            self.check_object_owner(rows, &intent.plan.dataset, &entry.object)?;
            let (marker_revision, _, marker) = rows
                .get_record::<super::owners::UnavailableObject>(&super::owners::unavailable_key(
                    &entry.object.path,
                ))?
                .ok_or_else(invalid)?;
            if marker_revision != revision
                || marker.revision != revision
                || marker.intent != intent.plan.removal
                || marker.object != entry.object
            {
                return Err(invalid());
            }
        }
        let (state_revision, _, state) = rows
            .get_record::<RemovalProgress>(&state_key(&intent.plan.removal))?
            .ok_or_else(invalid)?;
        if state_revision < revision || state_revision > view.revision {
            return Err(invalid());
        }
        validate_progress(intent, &state)?;
        Ok(())
    }

    async fn finish_removal(
        &self,
        mut view: View,
        intent: RemovalIntent,
        mut progress: RemovalProgress,
        stop: Option<&str>,
        eligibility_checked: bool,
    ) -> Result<RemovalStatus> {
        let lifecycle = &self.inner.lifecycle;
        let floor = validate_progress(&intent, &progress)? as u64;
        if finished(&progress) {
            return status(intent, progress);
        }
        // Completed retries return above. Unresolved retries check current roots
        // and native heads, but can only perform the original exact effects.
        if !eligibility_checked {
            // New namespaces can enlarge the fresh inspection independently of
            // the saved intent. Charge returned collections until checks finish.
            let _inspection_memory = lifecycle
                .budget
                .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(4))?;
            let objects = intent
                .objects
                .iter()
                .map(|index| intent.plan.inspection.objects[*index].object.clone())
                .collect::<Vec<_>>();
            let fresh = self.inspect_retention_admitted(&objects).await?;
            drop(objects);
            if fresh.objects.iter().any(|entry| entry.blocker.is_some()) {
                return Err(Error::refusal(
                    "stale_removal",
                    "a retained root now protects a reserved target",
                ));
            }
            let current = self
                .removal_registrations(&fresh.objects, &fresh.boundaries)
                .await?;
            for registration in &current {
                if !intent
                    .registrations
                    .iter()
                    .any(|index| &intent.plan.registrations[*index] == registration)
                {
                    return Err(Error::refusal(
                        "stale_removal",
                        "an unreserved native head now references removal targets",
                    ));
                }
            }
            for (position, index) in intent.registrations.iter().enumerate() {
                let registration = &intent.plan.registrations[*index];
                if !progress.registrations[position].finished()
                    && current.binary_search(registration).is_err()
                    && lifecycle
                        .catalog
                        .table_exists(&self.removal_registration_ident(registration)?)
                        .await?
                {
                    return Err(Error::refusal(
                        "stale_removal",
                        "saved native registration no longer belongs to the exact removal scope",
                    ));
                }
            }
        }
        let mut next = progress.clone();
        // Terminal reconciliation can consume the final reserved row without a
        // top-up: unknown effects may already have made every target absent.
        let required = floor.checked_mul(2).ok_or_else(invalid)?;
        if progress.reserved_future_bytes < required {
            for (position, index) in intent.registrations.iter().enumerate() {
                if next.registrations[position].finished() {
                    continue;
                }
                let registration = &intent.plan.registrations[*index];
                let ident = TableIdent::new(
                    NamespaceIdent::new(registration.namespace.clone()),
                    registration.name.clone(),
                );
                if !lifecycle.catalog.table_exists(&ident).await? {
                    next.registrations[position] = RemovalOutcome::Absent;
                }
            }
            if next.registrations.iter().all(RemovalOutcome::finished) {
                for (position, index) in intent.objects.iter().enumerate() {
                    if next.objects[position].finished() {
                        continue;
                    }
                    let object = &intent.plan.inspection.objects[*index].object;
                    match storage::removal::reconcile_absent_file(
                        &self.inner.root,
                        object,
                        &lifecycle.budget,
                    )
                    .await
                    {
                        Ok(true) => next.objects[position] = RemovalOutcome::Absent,
                        Ok(false) => {}
                        Err(error) => next.objects[position] = failure(error),
                    }
                }
            }
            if finished(&next) {
                next.reserved_future_bytes = 0;
                self.save_removal_progress(&view, &intent, &next).await?;
                return status(intent, next);
            }
        }
        // Reserve one bounded attempt report plus a final reconciliation report.
        // Later owner commits cannot spend either amount, including after restart.
        if progress.reserved_future_bytes < required {
            next.reserved_future_bytes = required;
            view = self.save_removal_progress(&view, &intent, &next).await?;
            progress = next.clone();
        }
        for (position, index) in intent.registrations.iter().enumerate() {
            if next.registrations[position].finished() {
                continue;
            }
            next.registrations[position] = match self
                .detach_registration(&intent.plan.registrations[*index])
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => failure(error),
            };
        }
        stop_after(stop, "removal-registrations")?;
        if next.registrations.iter().all(RemovalOutcome::finished) {
            for (position, index) in intent.objects.iter().enumerate() {
                if next.objects[position].finished() {
                    continue;
                }
                let object = &intent.plan.inspection.objects[*index].object;
                next.objects[position] = match storage::removal::remove_owned_file(
                    &self.inner.root,
                    object,
                    &lifecycle.budget,
                )
                .await
                {
                    Ok(storage::removal::LocalRemovalOutcome::Deleted) => RemovalOutcome::Removed,
                    Ok(storage::removal::LocalRemovalOutcome::Absent) => RemovalOutcome::Absent,
                    Err(error) => failure(error),
                };
                stop_after(stop, "removal-unlink")?;
            }
        }
        // An identical failed retry needs no new report. Keep its unused reserve.
        if next.objects == progress.objects && next.registrations == progress.registrations {
            return status(intent, progress);
        }
        next.reserved_future_bytes = if finished(&next) { 0 } else { floor };
        #[cfg(test)]
        if stop == Some("lost-removal-outcome") {
            lifecycle
                .lose_commit_response
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.save_removal_progress(&view, &intent, &next).await?;
        status(intent, next)
    }

    async fn save_removal_progress(
        &self,
        view: &View,
        intent: &RemovalIntent,
        progress: &RemovalProgress,
    ) -> Result<View> {
        validate_progress(intent, progress)?;
        let lifecycle = &self.inner.lifecycle;
        match lifecycle
            .commit(
                view,
                vec![record(state_key(&intent.plan.removal), progress)?],
            )
            .await
        {
            Ok(after) => Ok(after),
            Err(error) => {
                let after = lifecycle.open(&intent.plan.dataset).await?;
                let rows = lifecycle.rows(&after).await?;
                let (saved_intent, saved) =
                    self.saved_removal(&rows, &after, &intent.plan.removal)?;
                if saved_intent != *intent {
                    return Err(conflict());
                }
                if saved == *progress {
                    Ok(after)
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn detach_registration(
        &self,
        registration: &RemovalRegistration,
    ) -> Result<RemovalOutcome> {
        let ident = self.removal_registration_ident(registration)?;
        let catalog = &self.inner.lifecycle.catalog;
        if !catalog.table_exists(&ident).await? {
            return Ok(RemovalOutcome::Absent);
        }
        let table = catalog.load_table(&ident).await?;
        if table.metadata().uuid().to_string() != registration.table_uuid
            || table.metadata_location_result()? != registration.metadata
        {
            return Err(Error::refusal(
                "stale_removal",
                "reserved native registration changed; no file effects are permitted",
            ));
        }
        match catalog.drop_table(&ident).await {
            Ok(()) => Ok(RemovalOutcome::Removed),
            Err(error) => {
                if !catalog.table_exists(&ident).await? {
                    Ok(RemovalOutcome::Absent)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    fn removal_registration_ident(&self, registration: &RemovalRegistration) -> Result<TableIdent> {
        let name = registration
            .name
            .strip_prefix("data_")
            .ok_or_else(invalid)?;
        if name.len() != 32
            || uuid::Uuid::parse_str(name).is_err()
            || uuid::Uuid::parse_str(&registration.table_uuid).is_err()
        {
            return Err(invalid());
        }
        let path = Path::new(&registration.metadata);
        let relative = path
            .strip_prefix(self.inner.root.join("objects"))
            .map_err(|_| invalid())?;
        let components = relative.components().collect::<Vec<_>>();
        if components.len() < 4
            || components
                .iter()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            || path.components().collect::<PathBuf>().to_str()
                != Some(registration.metadata.as_str())
        {
            return Err(invalid());
        }
        let owner = Id::new(components[0].as_os_str().to_str().ok_or_else(invalid)?)?;
        Id::new(components[1].as_os_str().to_str().ok_or_else(invalid)?)?;
        if registration.namespace
            != crate::catalog::lifecycle_ident(&owner)
                .namespace()
                .to_string()
        {
            return Err(invalid());
        }
        Ok(TableIdent::new(
            NamespaceIdent::new(registration.namespace.clone()),
            registration.name.clone(),
        ))
    }

    async fn removal_registrations(
        &self,
        objects: &[ObjectProtection],
        boundaries: &[RetentionBoundary],
    ) -> Result<Vec<RemovalRegistration>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let _memory = lifecycle.budget.reserve(limit.saturating_mul(5))?;
        let paths = objects
            .iter()
            .map(|entry| entry.object.path.as_str())
            .collect::<BTreeSet<_>>();
        let mut result = Vec::new();
        let mut charged = 0usize;
        for boundary in boundaries {
            let namespace = boundary
                .table
                .strip_suffix(".lifecycle")
                .ok_or_else(invalid)?;
            let namespace = NamespaceIdent::new(namespace.into());
            let tables = lifecycle.catalog.list_tables(&namespace).await?;
            let mut owner_rows = None;
            for ident in tables {
                tokio::task::consume_budget().await;
                if ident.name() == "lifecycle" {
                    continue;
                }
                charged = charged
                    .saturating_add(ident.to_string().len())
                    .saturating_add(256);
                if charged > limit {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "native registration inventory exceeds allowance",
                    ));
                }
                let table = lifecycle.catalog.load_table(&ident).await?;
                let files = storage::snapshot_files(&table, &lifecycle.budget).await?;
                if !files.paths.iter().any(|path| paths.contains(path.as_str())) {
                    continue;
                }
                let suffix = ident.name().strip_prefix("data_").ok_or_else(invalid)?;
                if suffix.len() != 32 || uuid::Uuid::parse_str(suffix).is_err() {
                    return Err(invalid());
                }
                let location = Path::new(table.metadata().location());
                let relative = location
                    .strip_prefix(self.inner.root.join("objects"))
                    .map_err(|_| invalid())?;
                let owner = relative
                    .components()
                    .next()
                    .and_then(|part| part.as_os_str().to_str())
                    .ok_or_else(invalid)?;
                let owner = Id::new(owner)?;
                let owner_ident = crate::catalog::lifecycle_ident(&owner);
                if owner_ident.namespace() != ident.namespace()
                    || !Path::new(table.metadata_location_result()?)
                        .starts_with(location.join("metadata"))
                    || !table.metadata().default_partition_spec().is_unpartitioned()
                    || !table.metadata().default_sort_order().is_unsorted()
                    || table.metadata().encryption_keys_iter().next().is_some()
                {
                    return Err(invalid());
                }
                if owner_rows.is_none() {
                    let owner_view = lifecycle.open(&owner).await?;
                    if owner_view.table.metadata_location_result()? != boundary.metadata {
                        return Err(Error::refusal(
                            "stale_removal",
                            "owner changed during native inventory; capture a fresh plan",
                        ));
                    }
                    owner_rows = Some(lifecycle.rows(&owner_view).await?);
                }
                let rows = owner_rows.as_ref().ok_or_else(invalid)?;
                self.check_object_owner(
                    rows,
                    &owner,
                    &ObjectRef {
                        owner: owner.clone(),
                        path: table.metadata_location_result()?.into(),
                        digest: "0".repeat(64),
                        bytes: 0,
                    },
                )?;
                let registration = RemovalRegistration {
                    namespace: namespace.to_string(),
                    name: ident.name().into(),
                    table_uuid: table.metadata().uuid().to_string(),
                    metadata: table.metadata_location_result()?.into(),
                };
                charged = charged
                    .saturating_add(inputs::metadata_size(&registration, limit)?)
                    .saturating_add(128);
                if charged > limit {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "native registration inventory exceeds allowance",
                    ));
                }
                result.push(registration);
            }
        }
        result.sort();
        Ok(result)
    }
}

fn indices_valid(indices: &[usize], length: usize) -> bool {
    indices.iter().all(|index| *index < length) && indices.windows(2).all(|pair| pair[0] < pair[1])
}

fn status(intent: RemovalIntent, progress: RemovalProgress) -> Result<RemovalStatus> {
    validate_progress(&intent, &progress)?;
    let mut confirmed_unlinked_bytes = 0u64;
    let mut reconciled_absent_bytes = 0u64;
    for (position, index) in intent.objects.iter().enumerate() {
        let bytes = intent
            .plan
            .inspection
            .objects
            .get(*index)
            .ok_or_else(invalid)?
            .object
            .bytes;
        let target = match progress.objects[position] {
            RemovalOutcome::Removed => Some(&mut confirmed_unlinked_bytes),
            RemovalOutcome::Absent => Some(&mut reconciled_absent_bytes),
            _ => None,
        };
        if let Some(target) = target {
            *target = target.checked_add(bytes).ok_or_else(invalid)?;
        }
    }
    Ok(RemovalStatus {
        complete: finished(&progress),
        intent,
        progress,
        confirmed_unlinked_bytes,
        reconciled_absent_bytes,
    })
}
