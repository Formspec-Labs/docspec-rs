//! Explicit operation abandonment; completed phases and every object remain retained.
//!
//! This ends an operation's permission to acknowledge work or select a result.
//! Already-started I/O can finish under its existing protection. Abandonment is
//! neither worker quiescence nor permission to delete a phase or attempt.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::catalog::View;
use std::collections::BTreeSet;

mod availability;
pub(super) mod compaction_recovery;
mod export_recovery;
mod grants;
pub(in crate::engine) mod inventory;
pub(super) mod owners;
mod recovery;
pub(crate) mod removal;
pub(in crate::engine) mod retirement;
mod seals;

impl Engine {
    /// Requires a qualified outcome for every accepted source operation.
    ///
    /// A later selected boundary cannot stand in for an earlier request's
    /// selection or explicit abandonment. Call from the locked cutover view.
    /// `selecting` may name only the already checked schema operation selected
    /// by the same guarded commit: format-10 schemas also admitted source input.
    pub(super) async fn check_cutover_inputs(
        &self,
        view: &View,
        dataset: &Id,
        selecting: Option<&Id>,
    ) -> Result<()> {
        if self
            .pending_source_operations(view, dataset)
            .await?
            .iter()
            .any(|operation| Some(operation) != selecting)
        {
            return Err(pending_source());
        }
        Ok(())
    }

    /// Proves that every unselected admission in a captured interval was abandoned.
    /// Caller uses the same locked view for admission or final selection.
    pub(super) async fn check_abandoned_gap(
        &self,
        view: &View,
        dataset: &Id,
        selected: u64,
        fence: u64,
    ) -> Result<()> {
        let gap = fence.checked_sub(selected).ok_or_else(invalid_gap)?;
        if gap == 0 {
            return Ok(());
        }
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        // A conservative application charge for a BTreeSet<u64> entry, including
        // node overhead. Admission does not bound all native allocator behavior.
        let charge = gap
            .checked_mul(64)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(|| {
                Error::refusal(
                    "metadata_capacity",
                    "admission interval exceeds metadata allowance",
                )
            })?;
        let _interval = budget.reserve(charge)?;
        let _decoding = budget.reserve(limit.saturating_mul(4))?;
        let rows = self.inner.lifecycle.rows(view).await?;
        let mut boundaries = BTreeSet::new();
        for (key, revision, json) in rows.prefix("operation/") {
            tokio::task::consume_budget().await;
            let operation: Operation = serde_json::from_str(json)?;
            if operation.boundary > fence {
                return Err(invalid_gap());
            }
            if operation.boundary <= selected {
                continue;
            }
            let id = Id::new(key.strip_prefix("operation/").ok_or_else(invalid_gap)?)?;
            let (request_revision, _, request) = rows
                .get_record::<OperationRequest>(&format!("request/{id}"))?
                .ok_or_else(invalid_gap)?;
            if revision <= 0
                || revision > view.revision
                || request_revision != revision
                || request.dataset() != dataset
                || request.operation() != &id
                || fingerprint(&request)? != operation.request_digest
                || !supported_format(operation.result_format)
            {
                return Err(invalid_gap());
            }
            // Format 10 schema operations advanced the admission counter. New
            // schema and all backfill operations preserve their source coverage.
            if matches!(
                request,
                OperationRequest::Backfill(_) | OperationRequest::Evidence(_)
            ) || (matches!(request, OperationRequest::Schema(_))
                && operation.result_format != PREVIOUS_FORMAT)
            {
                continue;
            }
            let (abandoned_revision, _, abandoned) = rows
                .get_record::<Abandonment>(&format!("abandoned/{id}"))?
                .ok_or_else(pending_source)?;
            if abandoned_revision <= revision
                || abandoned_revision > view.revision
                || abandoned.operation_revision != revision
                || abandoned.operation != id
                || abandoned.dataset != *dataset
                || abandoned.request_digest != operation.request_digest
                || abandoned.reason.trim().is_empty()
                || rows.contains_key(&format!("result/{id}"))
                || !boundaries.insert(operation.boundary)
            {
                return Err(invalid_gap());
            }
        }
        if boundaries.len() as u64 != gap {
            return Err(invalid_gap());
        }
        Ok(())
    }

    /// Stops future completion and selection for one admitted, unfinished operation.
    ///
    /// Identical retries return the retained decision. Completed phases remain
    /// reusable by other operation IDs. No payloads are read or removed.
    ///
    /// # Errors
    /// Refuses missing operations, selected results, conflicting reasons, empty
    /// reasons, or a stale native commit. Running I/O is not forcibly canceled.
    pub async fn abandon(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        let memory = self.request_memory(&(dataset, operation, reason))?;
        let dataset = dataset.clone();
        let operation = operation.clone();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.abandon_admitted(&dataset, &operation, &reason).await
        })
        .await
    }

    async fn abandon_admitted(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        self.require_local()?;
        if reason.trim().is_empty() {
            return Err(Error::refusal(
                "missing_reason",
                "abandonment requires a reason",
            ));
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        if reason.len() > lifecycle.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "abandonment reason exceeds metadata allowance",
            ));
        }
        let view = lifecycle.open(dataset).await?;
        if let Some(abandoned) = self
            .abandon_continuous_in(&view, dataset, operation, reason)
            .await?
        {
            return Ok(abandoned);
        }
        let rows = lifecycle.rows(&view).await?;
        let (revision, _, admitted) = rows
            .get_record::<Operation>(&format!("operation/{operation}"))?
            .ok_or_else(|| Error::refusal("no_operation", "operation has not been admitted"))?;
        let abandoned = Abandonment {
            dataset: dataset.clone(),
            operation: operation.clone(),
            request_digest: admitted.request_digest,
            operation_revision: revision,
            reason: reason.to_owned(),
        };
        let key = format!("abandoned/{operation}");
        if let Some((_, _, existing)) = rows.get_record::<Abandonment>(&key)? {
            if existing != abandoned {
                return Err(Error::refusal(
                    "abandonment_conflict",
                    "operation already has a different abandonment decision",
                ));
            }
            return Ok(existing);
        }
        if rows.contains_key(&format!("result/{operation}")) {
            return Err(Error::refusal(
                "operation_completed",
                "selected results remain available; abandonment only stops unfinished work",
            ));
        }
        lifecycle
            .commit(&view, vec![record(key, &abandoned)?])
            .await?;
        Ok(abandoned)
    }

    pub(super) async fn check_not_abandoned(&self, view: &View, operation: &Id) -> Result<()> {
        self.check_continuous_capability(view, operation).await?;
        if self
            .inner
            .lifecycle
            .rows(view)
            .await?
            .contains_key(&format!("abandoned/{operation}"))
        {
            return Err(Error::refusal(
                "operation_abandoned",
                "operation was explicitly abandoned; completed work remains retained",
            ));
        }
        Ok(())
    }

    /// Compacts superseded lifecycle revisions and transient records behind the active release.
    ///
    /// Coalesces historical lifecycle appends into one native baseline snapshot while
    /// preserving selected releases, retained results, active reader grants, exports,
    /// restore points, retirement receipts, and unfinished operations.
    ///
    /// # Errors
    /// Refuses remote-only engines without local authority, corrupt metadata, stale
    /// catalog views, or exhausted metadata capacity.
    pub async fn compact_lifecycle(
        &self,
        dataset: &Id,
    ) -> Result<crate::catalog::LifecycleCompactionResult> {
        let memory = self.request_memory(dataset)?;
        let dataset = dataset.clone();
        self.admit_work(memory, move |engine| async move {
            engine.compact_lifecycle_admitted(&dataset).await
        })
        .await
    }

    async fn compact_lifecycle_admitted(
        &self,
        dataset: &Id,
    ) -> Result<crate::catalog::LifecycleCompactionResult> {
        self.require_local()?;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(dataset).await?;
        let rows = lifecycle.rows_for_compaction(&view).await?;
        let mut remove_keys = collect_obsolete_lifecycle_keys(&rows)?;
        let checkpoint_plan = self
            .plan_succession_checkpoint(dataset, &view, &rows, &mut remove_keys)
            .await?;
        let head_ref_paths: BTreeSet<String> = if let Some((_, _, authority)) =
            rows.get_record::<Authority>("authority")?
            && let Some(head_release) = authority.release
        {
            self.reference_objects(std::slice::from_ref(&head_release))
                .await
                .map(|refs| refs.objects.into_iter().map(|o| o.path).collect())
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
        prune_obsolete_protections(&rows, &head_ref_paths, &mut remove_keys)?;
        drop(rows);
        let upserts = checkpoint_plan
            .as_ref()
            .map_or(&[][..], |plan| plan.records.as_slice());
        let outcome = lifecycle
            .compact_baseline_with_records(&view, &remove_keys, upserts)
            .await;
        match outcome {
            Ok((_, result)) => {
                if let Some(plan) = checkpoint_plan
                    && let Some(old_path) = plan.old_archive_path
                {
                    let _ = std::fs::remove_file(&old_path);
                    if let Some(parent) = Path::new(&old_path).parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                }
                if self.inner.retained.is_local() {
                    self.reclaim_superseded_dataset_objects(dataset).await?;
                }
                Ok(result)
            }
            Err(err) => {
                if let Some(plan) = checkpoint_plan {
                    let uncommitted = lifecycle
                        .catalog
                        .load_table(view.table.identifier())
                        .await
                        .ok()
                        .and_then(|t| t.metadata_location().map(str::to_owned))
                        == view.table.metadata_location().map(str::to_owned);
                    if uncommitted {
                        let _ = std::fs::remove_file(&plan.new_archive_path);
                        if let Some(parent) = Path::new(&plan.new_archive_path).parent() {
                            let _ = std::fs::remove_dir(parent);
                        }
                    }
                }
                Err(err)
            }
        }
    }

    async fn reclaim_superseded_dataset_objects(&self, dataset: &Id) -> Result<()> {
        let (live_paths, live_native_metadata, protected_prefixes) =
            self.live_retained_closure().await?;
        let ident = crate::catalog::lifecycle_ident(dataset);
        let tables = self
            .inner
            .lifecycle
            .catalog
            .list_tables(ident.namespace())
            .await?;
        for native_ident in tables {
            if native_ident == ident || !native_ident.name().starts_with("data_") {
                continue;
            }
            let Ok(native) = self.inner.lifecycle.catalog.load_table(&native_ident).await else {
                continue;
            };
            let Ok(metadata_loc) = native.metadata_location_result() else {
                let _ = self.inner.lifecycle.catalog.drop_table(&native_ident).await;
                continue;
            };
            if !live_native_metadata.contains(metadata_loc)
                && !is_protected_path(metadata_loc, &protected_prefixes)
            {
                let _ = self.inner.lifecycle.catalog.drop_table(&native_ident).await;
            }
        }

        let dataset_objects_dir = self.inner.retained.local_path()?.join(dataset.as_str());
        self.inner
            .lifecycle
            .budget
            .blocking(move || {
                reclaim_unreferenced_tree(&dataset_objects_dir, &live_paths, &protected_prefixes)
            })
            .await
    }

    async fn plan_succession_checkpoint(
        &self,
        dataset: &Id,
        view: &View,
        rows: &crate::catalog::SnapshotRows,
        remove_keys: &mut BTreeSet<String>,
    ) -> Result<Option<CheckpointCompactionPlan>> {
        use crate::engine::history::{
            ArchivedSuccessionEntry, SuccessionArchive, SuccessionCheckpoint, read_link,
            validate_checkpoint,
        };

        let authority: Authority = rows
            .get_record("authority")?
            .map(|(_, _, value)| value)
            .unwrap_or_default();
        let Some(head_release) = &authority.release else {
            return Ok(None);
        };
        let mut protected_ops = BTreeSet::from([head_release.id.clone()]);
        let mut protected_keys = BTreeSet::new();
        for (_, _, json) in rows.prefix("retirement/") {
            let receipt: Retirement = serde_json::from_str(json)?;
            match &receipt.plan.scope {
                RetirementScope::ResultAvailability { operation }
                | RetirementScope::OperationRecovery { operation } => {
                    protected_ops.insert(operation.clone());
                }
                _ => {}
            }
            for generation in receipt.plan.records {
                if let Some(op_str) = generation
                    .key
                    .strip_prefix("result/")
                    .or_else(|| generation.key.strip_prefix("operation/"))
                    .or_else(|| generation.key.strip_prefix("change-certificate/"))
                    && let Ok(op) = Id::new(op_str)
                {
                    protected_ops.insert(op);
                }
                protected_keys.insert(generation.key);
            }
        }
        for prefix in ["availability-retired/", "operation-retired/"] {
            for (key, _, _) in rows.prefix(prefix) {
                if let Some(op_str) = key.strip_prefix(prefix)
                    && let Ok(op) = Id::new(op_str)
                {
                    protected_ops.insert(op);
                }
            }
        }
        for (key, _, json) in rows.prefix("operation/") {
            let Some(op_str) = key.strip_prefix("operation/") else {
                continue;
            };
            let Ok(op) = Id::new(op_str) else {
                continue;
            };
            if !rows.contains_key(&format!("result/{op}"))
                && !rows.contains_key(&format!("abandoned/{op}"))
            {
                protected_ops.insert(op);
                if let Ok(admitted) = serde_json::from_str::<Operation>(json)
                    && let Some(base) = admitted.base
                {
                    protected_ops.insert(base);
                }
            }
        }
        for (_, _, json) in rows.prefix("export-operation/") {
            if let Ok(op) = serde_json::from_str::<crate::engine::export::ExportOperation>(json) {
                protected_ops.insert(op.source.id);
            }
        }
        for prefix in ["export-result/", "export-channel/"] {
            for (_, _, json) in rows.prefix(prefix) {
                if let Ok(pin) = serde_json::from_str::<crate::export::ExportPin>(json) {
                    protected_ops.insert(pin.source.version);
                }
            }
        }
        for (key, _, json) in rows.prefix("compaction-request/") {
            let Some(compaction_op) = key.strip_prefix("compaction-request/") else {
                continue;
            };
            let is_incomplete = !rows.contains_key(&format!("compaction-result/{compaction_op}"));
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json)
                && let Some(ver) = val
                    .pointer("/spec/version")
                    .or_else(|| val.pointer("/spec/base"))
                    .and_then(|v| v.as_str())
                && let Ok(op) = Id::new(ver)
                && (is_incomplete || op == head_release.id)
            {
                protected_ops.insert(op);
            }
        }
        for (key, _, json) in rows.prefix("continuous") {
            let Ok(val) = serde_json::from_str::<serde_json::Value>(json) else {
                continue;
            };
            if key.starts_with("continuous/") {
                if let Some(ver) = val.pointer("/initial/version").and_then(|v| v.as_str())
                    && let Ok(op) = Id::new(ver)
                {
                    protected_ops.insert(op);
                }
            } else if key.starts_with("continuous-child/") {
                for ptr in ["/capture/version", "/child"] {
                    if let Some(ver) = val.pointer(ptr).and_then(|v| v.as_str())
                        && let Ok(op) = Id::new(ver)
                    {
                        protected_ops.insert(op);
                    }
                }
            }
        }

        let existing_checkpoint = if let Some((_, _, cp)) =
            rows.get_record::<SuccessionCheckpoint>("succession-checkpoint")?
        {
            if validate_checkpoint(&cp, rows, &view.identifier().to_string(), view.revision)
                .is_err()
            {
                return Ok(None);
            }
            Some(cp)
        } else {
            None
        };

        let mut live_chain = Vec::new();
        let mut current = head_release.version_ref();
        let mut previous_revision = None;
        let mut previous_boundary = authority.boundary;
        let mut previous_eligibility = authority.eligibility;
        loop {
            let link_key = format!("succession/{}", current.version);
            if let Some(cp) = &existing_checkpoint
                && current == cp.head
                && !rows.contains_key(&link_key)
            {
                if previous_revision.is_none_or(|child| cp.head_revision >= child)
                    || cp.head_boundary > previous_boundary
                    || cp.head_eligibility > previous_eligibility
                {
                    return Ok(None);
                }
                break;
            }
            let Ok((revision, _, link)) = read_link(rows, view.revision, &current) else {
                return Ok(None);
            };
            if previous_revision.is_some_and(|child| revision >= child)
                || link.boundary > previous_boundary
                || link.eligibility > previous_eligibility
            {
                return Ok(None);
            }
            previous_revision = Some(revision);
            previous_boundary = link.boundary;
            previous_eligibility = link.eligibility;
            let predecessor = link.predecessor.clone();
            live_chain.push((revision, link));
            let Some(predecessor) = predecessor else {
                if existing_checkpoint.is_some() {
                    return Ok(None);
                }
                break;
            };
            current = predecessor;
        }

        let last_protected_idx = live_chain
            .iter()
            .rposition(|(_, link)| protected_ops.contains(&link.selected.version))
            .unwrap_or(0);
        let to_archive = &live_chain[last_protected_idx + 1..];
        if to_archive.is_empty() {
            return Ok(None);
        }

        let mut new_source_boundaries = 0_u64;
        for (_, link) in to_archive {
            let op_id = &link.selected.version;
            let op_key = format!("operation/{op_id}");
            let req_key = format!("request/{op_id}");
            let (Some((op_rev, _, op)), Some((req_rev, _, req))) = (
                rows.get_record::<Operation>(&op_key)?,
                rows.get_record::<OperationRequest>(&req_key)?,
            ) else {
                return Ok(None);
            };
            if op_rev <= 0
                || op_rev != req_rev
                || req.operation() != op_id
                || req.dataset() != dataset
            {
                return Ok(None);
            }
            let is_source = matches!(
                req,
                OperationRequest::Build(_)
                    | OperationRequest::Update(_)
                    | OperationRequest::Refresh(_)
            ) || (matches!(req, OperationRequest::Schema(_))
                && op.result_format == PREVIOUS_FORMAT);
            if is_source {
                if op.boundary == 0 {
                    return Ok(None);
                }
                new_source_boundaries = new_source_boundaries.saturating_add(1);
            }
            for exact in [
                format!("result/{op_id}"),
                format!("succession/{op_id}"),
                op_key,
                req_key,
                format!("runtime/{op_id}"),
                format!("change-certificate/{op_id}"),
                format!("input-progress/{op_id}"),
            ] {
                if protected_keys.contains(&exact) {
                    return Ok(None);
                }
                let bkey = owners::binding_key(&exact);
                if protected_keys.contains(&bkey) {
                    return Ok(None);
                }
                if rows.contains_key(&bkey) {
                    remove_keys.insert(bkey);
                }
                if rows.contains_key(&exact) {
                    remove_keys.insert(exact);
                }
            }
        }

        let mut entries = to_archive
            .iter()
            .map(|(revision, link)| ArchivedSuccessionEntry {
                revision: *revision,
                link: link.clone(),
            })
            .collect::<Vec<_>>();
        let mut old_archive_path = None;
        let mut total_source_boundaries = new_source_boundaries;
        if let Some(cp) = &existing_checkpoint {
            let mut metadata_bytes = 0usize;
            let previous_archive = self
                .read_succession_archive(cp, &mut metadata_bytes)
                .await?;
            entries.extend(previous_archive.entries);
            total_source_boundaries = total_source_boundaries.saturating_add(cp.source_boundaries);
            remove_keys.insert(format!("protection/{}", cp.protection));
            old_archive_path = Some(cp.archive.path.clone());
        }

        let (head_revision, head_link) = &to_archive[0];
        let attempt = Id::new(format!("checkpoint-{}", uuid::Uuid::new_v4().simple()))?;
        let prefix = self
            .inner
            .retained
            .child(dataset.as_str())?
            .child(attempt.as_str())?;
        let protection_prefix = prefix.protection_prefix()?;
        let dir = self
            .inner
            .retained
            .local_path()?
            .join(dataset.as_str())
            .join(attempt.as_str());
        std::fs::create_dir_all(&dir)?;
        let archive_path = dir.join("succession-checkpoint.json");
        let new_archive_path = archive_path.to_string_lossy().into_owned();
        let archive = SuccessionArchive {
            dataset: dataset.clone(),
            head: head_link.selected.clone(),
            head_revision: *head_revision,
            source_boundaries: total_source_boundaries,
            entries,
        };
        let archive_bytes = serde_json::to_vec(&archive)?;
        if let Err(err) = std::fs::write(&archive_path, &archive_bytes) {
            let _ = std::fs::remove_dir(&dir);
            return Err(err.into());
        }
        let archive_obj = ObjectRef {
            owner: dataset.clone(),
            path: new_archive_path.clone(),
            digest: blake3::hash(&archive_bytes).to_hex().to_string(),
            bytes: archive_bytes.len() as u64,
        };
        let protection = Protection {
            id: attempt.clone(),
            consumer: dataset.clone(),
            purpose: "succession-checkpoint/archive".into(),
            paths: vec![protection_prefix],
            retired: false,
        };
        let checkpoint = SuccessionCheckpoint {
            dataset: dataset.clone(),
            head: head_link.selected.clone(),
            head_revision: *head_revision,
            head_boundary: head_link.boundary,
            head_eligibility: head_link.eligibility,
            source_boundaries: total_source_boundaries,
            count: archive.entries.len() as u64,
            archive: archive_obj,
            protection: attempt.clone(),
        };
        let records = vec![
            record(format!("protection/{attempt}"), &protection)?,
            record("succession-checkpoint", &checkpoint)?,
        ];
        Ok(Some(CheckpointCompactionPlan {
            records,
            new_archive_path,
            old_archive_path,
        }))
    }
}

struct CheckpointCompactionPlan {
    records: Vec<(String, serde_json::Value)>,
    new_archive_path: String,
    old_archive_path: Option<String>,
}

fn collect_obsolete_lifecycle_keys(
    rows: &crate::catalog::SnapshotRows,
) -> Result<BTreeSet<String>> {
    let authority: Authority = rows
        .get_record("authority")?
        .map(|(_, _, value)| value)
        .unwrap_or_default();
    let selected_id = authority.release.as_ref().map(|release| release.id.clone());

    let mut protected_keys = BTreeSet::new();
    for (_, _, json) in rows.prefix("retirement/") {
        let receipt: Retirement = serde_json::from_str(json)?;
        for generation in receipt.plan.records {
            protected_keys.insert(generation.key);
        }
    }

    let mut completed_non_selected = BTreeSet::new();
    let mut incomplete_ops = BTreeSet::new();
    let mut incomplete_digests = BTreeSet::new();
    for (key, _, json) in rows.prefix("operation/") {
        let Some(op_str) = key.strip_prefix("operation/") else {
            continue;
        };
        let Ok(op) = Id::new(op_str) else {
            continue;
        };
        if rows.contains_key(&format!("result/{op}")) {
            if Some(&op) != selected_id.as_ref()
                && !rows.contains_key(&format!("availability-retired/{op}"))
                && !rows.contains_key(&format!("operation-retired/{op}"))
            {
                completed_non_selected.insert(op);
            }
        } else if !rows.contains_key(&format!("abandoned/{op}")) {
            if let Ok(admitted) = serde_json::from_str::<Operation>(json) {
                incomplete_digests.insert(admitted.request_digest);
            }
            incomplete_ops.insert(op);
        }
    }

    let mut candidate_keys = BTreeSet::new();
    for op in &completed_non_selected {
        for exact in [
            format!("readback/{op}"),
            format!("request-dependencies/{op}"),
            format!("groups/{op}"),
            format!("prepared-changes/{op}"),
        ] {
            if rows.contains_key(&exact) {
                candidate_keys.insert(exact);
            }
        }
        for prefix in [
            format!("source-attempt/{op}/"),
            format!("candidate-attempt/{op}/"),
            format!("release-attempt/{op}/"),
        ] {
            for (key, _, _) in rows.prefix(&prefix) {
                candidate_keys.insert(key.to_owned());
            }
        }
    }

    let is_obsolete_op = |op: &Id| -> bool {
        if Some(op) == selected_id.as_ref()
            || incomplete_ops.contains(op)
            || rows.contains_key(&format!("availability-retired/{op}"))
            || rows.contains_key(&format!("operation-retired/{op}"))
        {
            return false;
        }
        completed_non_selected.contains(op) || !rows.contains_key(&format!("operation/{op}"))
    };

    for (key, _, json) in rows.prefix("phase/") {
        if key.starts_with("phase/backfill/") {
            continue;
        }
        if !incomplete_ops.is_empty()
            && (key.starts_with("phase/input/")
                || key.starts_with("phase/build/")
                || key.starts_with("phase/checked/"))
        {
            continue;
        }
        if incomplete_digests
            .iter()
            .any(|digest| key.starts_with(&format!("phase/{digest}/")))
        {
            continue;
        }
        if let Ok(phase) = serde_json::from_str::<PhaseResult>(json)
            && is_obsolete_op(&phase.operation)
        {
            candidate_keys.insert(key.to_owned());
        }
    }

    if incomplete_ops.is_empty()
        && let Some(head_release) = &authority.release
    {
        let mut active_logical_digests = BTreeSet::new();
        for table in head_release.dataset.groups.values() {
            active_logical_digests.insert(table.logical_metadata().digest.clone());
            active_logical_digests.insert(table.metadata.digest.clone());
        }
        if let Some(evidence) = &head_release.dataset.evidence {
            active_logical_digests.insert(evidence.table.logical_metadata().digest.clone());
            active_logical_digests.insert(evidence.table.metadata.digest.clone());
        }
        if let Some(related) = &head_release.dataset.related {
            active_logical_digests.insert(related.evidence.logical_metadata().digest.clone());
            active_logical_digests.insert(related.evidence.metadata.digest.clone());
            for source in related.input_sources.values() {
                active_logical_digests.insert(source.logical_metadata().digest.clone());
                active_logical_digests.insert(source.metadata.digest.clone());
            }
        }

        let mut kept_compaction_ops = BTreeSet::new();
        for (key, _, json) in rows.prefix("representation/") {
            if let Ok(result) = serde_json::from_str::<CompactionResult>(json) {
                if active_logical_digests.contains(&result.source.logical_metadata().digest)
                    || active_logical_digests
                        .contains(&result.replacement.logical_metadata().digest)
                    || protected_keys.contains(key)
                    || protected_keys.contains(&owners::binding_key(key))
                {
                    kept_compaction_ops.insert(result.spec.operation);
                } else {
                    candidate_keys.insert(key.to_owned());
                }
            }
        }

        for (key, _, json) in rows.prefix("compaction-request/") {
            let Some(compaction_op) = key.strip_prefix("compaction-request/") else {
                continue;
            };
            if !rows.contains_key(&format!("compaction-result/{compaction_op}")) {
                continue;
            }
            if let Ok(capture) = serde_json::from_str::<crate::engine::compaction::Capture>(json)
                && is_obsolete_op(&capture.spec.version)
                && !kept_compaction_ops.contains(&capture.spec.operation)
                && !active_logical_digests.contains(&capture.source.logical_metadata().digest)
                && !active_logical_digests.contains(&capture.source.metadata.digest)
            {
                let prefixes = [
                    "compaction-request",
                    "compaction-prepared",
                    "compaction-verified",
                    "compaction-result",
                ];
                let any_protected = prefixes.iter().any(|prefix| {
                    let k = format!("{prefix}/{compaction_op}");
                    protected_keys.contains(&k) || protected_keys.contains(&owners::binding_key(&k))
                });
                if !any_protected {
                    for prefix in prefixes {
                        let exact = format!("{prefix}/{compaction_op}");
                        if rows.contains_key(&exact) {
                            candidate_keys.insert(exact);
                        }
                    }
                }
            }
        }
    }

    let mut remove_keys = BTreeSet::new();
    for key in candidate_keys {
        if protected_keys.contains(&key) {
            continue;
        }
        let bkey = owners::binding_key(&key);
        if protected_keys.contains(&bkey) {
            continue;
        }
        if rows.contains_key(&bkey) {
            remove_keys.insert(bkey);
        }
        remove_keys.insert(key);
    }

    Ok(remove_keys)
}

fn prune_obsolete_protections(
    rows: &crate::catalog::SnapshotRows,
    head_ref_paths: &BTreeSet<String>,
    remove_keys: &mut BTreeSet<String>,
) -> Result<()> {
    let authority: Authority = rows
        .get_record("authority")?
        .map(|(_, _, value)| value)
        .unwrap_or_default();
    let selected_id = authority.release.as_ref().map(|release| release.id.clone());

    let mut protected_keys = BTreeSet::new();
    for (_, _, json) in rows.prefix("retirement/") {
        let receipt: Retirement = serde_json::from_str(json)?;
        for generation in receipt.plan.records {
            protected_keys.insert(generation.key);
        }
    }

    let mut completed_non_selected = BTreeSet::new();
    let mut incomplete_ops = BTreeSet::new();
    for (key, _, _) in rows.prefix("operation/") {
        let Some(op_str) = key.strip_prefix("operation/") else {
            continue;
        };
        let Ok(op) = Id::new(op_str) else {
            continue;
        };
        if rows.contains_key(&format!("result/{op}")) {
            if Some(&op) != selected_id.as_ref()
                && !rows.contains_key(&format!("availability-retired/{op}"))
                && !rows.contains_key(&format!("operation-retired/{op}"))
            {
                completed_non_selected.insert(op);
            }
        } else if !rows.contains_key(&format!("abandoned/{op}")) {
            incomplete_ops.insert(op);
        }
    }

    let is_obsolete_op = |op: &Id| -> bool {
        if Some(op) == selected_id.as_ref()
            || incomplete_ops.contains(op)
            || rows.contains_key(&format!("availability-retired/{op}"))
            || rows.contains_key(&format!("operation-retired/{op}"))
        {
            return false;
        }
        completed_non_selected.contains(op) || !rows.contains_key(&format!("operation/{op}"))
    };

    let kept_jsons: Vec<&str> = rows
        .prefix("")
        .filter(|(k, _, _)| {
            !remove_keys.contains(*k)
                && !k.starts_with("protection/")
                && !k.starts_with("reference-binding/")
        })
        .map(|(_, _, json)| json)
        .collect();

    for (key, _, json) in rows.prefix("protection/") {
        if protected_keys.contains(key) {
            continue;
        }
        let bkey = owners::binding_key(key);
        if protected_keys.contains(&bkey) {
            continue;
        }
        let Ok(protection) = serde_json::from_str::<Protection>(json) else {
            continue;
        };
        let needle = format!("/{}/", protection.id.as_str());
        if head_ref_paths.iter().any(|p| p.contains(&needle))
            || kept_jsons.iter().any(|j| j.contains(&needle))
        {
            continue;
        }
        let Some((op_str, kind)) = protection.purpose.split_once('/') else {
            continue;
        };
        if kind == "compaction" {
            let req_key = format!("compaction-request/{op_str}");
            if (!rows.contains_key(&req_key) || remove_keys.contains(&req_key))
                && (!rows.contains_key(&format!("compaction-result/{op_str}"))
                    || remove_keys.contains(&format!("compaction-result/{op_str}")))
            {
                if rows.contains_key(&bkey) {
                    remove_keys.insert(bkey);
                }
                remove_keys.insert(key.to_owned());
            }
            continue;
        }
        if !(kind == "admission"
            || kind == "processing"
            || kind == "changes"
            || kind == "release"
            || kind.starts_with("backfill/"))
        {
            continue;
        }
        let Ok(op) = Id::new(op_str) else {
            continue;
        };
        if !is_obsolete_op(&op) {
            continue;
        }
        let attempt = &protection.id;
        let has_live_attempt = [
            format!("source-attempt/{op}/{attempt}"),
            format!("candidate-attempt/{op}/{attempt}"),
            format!("release-attempt/{op}/{attempt}"),
        ]
        .into_iter()
        .any(|attempt_key| rows.contains_key(&attempt_key) && !remove_keys.contains(&attempt_key));
        if !has_live_attempt {
            if rows.contains_key(&bkey) {
                remove_keys.insert(bkey);
            }
            remove_keys.insert(key.to_owned());
        }
    }
    Ok(())
}

fn is_protected_path(path: &str, protected_prefixes: &BTreeSet<String>) -> bool {
    protected_prefixes
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

fn reclaim_unreferenced_tree(
    root_dir: &Path,
    live_paths: &BTreeSet<String>,
    protected_prefixes: &BTreeSet<String>,
) -> Result<()> {
    if !root_dir.exists() {
        return Ok(());
    }
    reclaim_unreferenced_subdir(root_dir, live_paths, protected_prefixes, true)
}

fn reclaim_unreferenced_subdir(
    dir: &Path,
    live_paths: &BTreeSet<String>,
    protected_prefixes: &BTreeSet<String>,
    is_root: bool,
) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            let dir_prefix = format!("{}/", path.to_string_lossy());
            if is_protected_path(&dir_prefix, protected_prefixes) {
                continue;
            }
            reclaim_unreferenced_subdir(&path, live_paths, protected_prefixes, false)?;
            let _ = std::fs::remove_dir(&path);
        } else if file_type.is_file() {
            let path_str = path.to_string_lossy();
            if !live_paths.contains(path_str.as_ref())
                && !is_protected_path(path_str.as_ref(), protected_prefixes)
            {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }
    if !is_root {
        let _ = std::fs::remove_dir(dir);
    }
    Ok(())
}

fn invalid_gap() -> Error {
    Error::refusal(
        "invalid_admission_history",
        "admission interval or abandonment identities are incomplete or inconsistent",
    )
}

fn pending_source() -> Error {
    Error::refusal(
        "pending_source",
        "source admission is unresolved; select or explicitly abandon it before cutover",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn workspace() -> (tempfile::TempDir, Engine) {
        let root = tempfile::TempDir::new().unwrap();
        let limits = Limits {
            compute_threads: 1,
            memory_bytes: 32 << 20,
            scratch_bytes: 1 << 20,
            batch_rows: 32,
            max_batch_bytes: 64 << 10,
            max_metadata_bytes: 256 << 10,
        };
        let engine = Engine::open(root.path(), limits).await.unwrap();
        (root, engine)
    }

    fn admission(engine: &Engine, dataset: &Id, name: &str, boundary: u64) -> (RunSpec, Operation) {
        let request = RunSpec {
            dataset: dataset.clone(),
            operation: Id::new(name).unwrap(),
            base: None,
            relationship: None,
            input: engine
                .inner
                .root
                .join("missing-input.parquet")
                .display()
                .to_string(),
            expected_digest: "0".repeat(64),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        };
        assert!(!Path::new(&request.input).exists());
        let operation = Operation {
            request_digest: fingerprint(&OperationRequest::Build(request.clone())).unwrap(),
            eligibility: 0,
            epoch: 0,
            boundary,
            admission_fence: None,
            result_format: FORMAT,
            definition: request.definition.clone(),
            base: None,
            base_manifest: None,
            schema_key: None,
            require_current_boundary: false,
        };
        (request, operation)
    }

    fn abandonment(request: &RunSpec, operation: &Operation, revision: i64) -> Abandonment {
        Abandonment {
            dataset: request.dataset.clone(),
            operation: request.operation.clone(),
            request_digest: operation.request_digest.clone(),
            operation_revision: revision,
            reason: "superseded input".into(),
        }
    }

    fn admission_records(
        request: &RunSpec,
        operation: &Operation,
    ) -> Vec<(String, serde_json::Value)> {
        vec![
            record(format!("operation/{}", request.operation), operation).unwrap(),
            record(
                format!("request/{}", request.operation),
                OperationRequest::Build(request.clone()),
            )
            .unwrap(),
        ]
    }

    #[tokio::test]
    async fn abandoned_gap_accepts_complete_interval_without_opening_inputs() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("complete").unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let mut view = lifecycle.open(&dataset).await.unwrap();
        for (name, boundary) in [("first", 2), ("second", 3)] {
            let (request, operation) = admission(&engine, &dataset, name, boundary);
            view = lifecycle
                .commit(&view, admission_records(&request, &operation))
                .await
                .unwrap();
            let abandoned = abandonment(&request, &operation, view.revision);
            view = lifecycle
                .commit(
                    &view,
                    vec![record(format!("abandoned/{name}"), abandoned).unwrap()],
                )
                .await
                .unwrap();
        }
        engine
            .check_abandoned_gap(&view, &dataset, 1, 3)
            .await
            .unwrap();
        engine
            .check_abandoned_gap(&view, &dataset, 3, 3)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_gap_counts_legacy_schema_but_not_composition_operations() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("mixed").unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let mut legacy_abandoned = None;
        for (name, format, is_schema) in [
            ("modern_schema", FORMAT, true),
            ("retained_schema", 11, true),
            ("old_backfill", PREVIOUS_FORMAT, false),
            ("new_backfill", FORMAT, false),
            ("legacy_schema", PREVIOUS_FORMAT, true),
        ] {
            let (build, mut operation) = admission(&engine, &dataset, name, 1);
            let request = if is_schema {
                OperationRequest::Schema(SchemaSpec {
                    dataset: dataset.clone(),
                    operation: build.operation.clone(),
                    base: Id::new("base").unwrap(),
                    definition: "v1".into(),
                    next_definition: "v2".into(),
                    fields: vec![SchemaField {
                        id: 1,
                        name: "renamed".into(),
                    }],
                    additions: vec![],
                    write_defaults: BTreeMap::new(),
                })
            } else {
                OperationRequest::Backfill(BackfillSpec {
                    dataset: dataset.clone(),
                    operation: build.operation.clone(),
                    base: Id::new("base").unwrap(),
                    definition: "v1".into(),
                    group: Id::new("derived").unwrap(),
                    group_definition: "g1".into(),
                    fields: vec![ComputedField {
                        id: 1,
                        name: "value".into(),
                        expression: "1".into(),
                    }],
                })
            };
            operation.result_format = format;
            operation.request_digest = fingerprint(&request).unwrap();
            view = lifecycle
                .commit(
                    &view,
                    vec![
                        record(format!("operation/{name}"), &operation).unwrap(),
                        record(format!("request/{name}"), request).unwrap(),
                    ],
                )
                .await
                .unwrap();
            if name == "legacy_schema" {
                legacy_abandoned = Some(abandonment(&build, &operation, view.revision));
            }
        }
        let error = engine
            .check_abandoned_gap(&view, &dataset, 0, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "pending_source");
        view = lifecycle
            .commit(
                &view,
                vec![record("abandoned/legacy_schema", legacy_abandoned.unwrap()).unwrap()],
            )
            .await
            .unwrap();
        // The other operations share this boundary but do not resolve or duplicate it.
        engine
            .check_abandoned_gap(&view, &dataset, 0, 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_gap_refuses_incomplete_or_conflicting_identities() {
        let (_root, engine) = workspace().await;
        let lifecycle = &engine.inner.lifecycle;
        for case in [
            "missing_boundary",
            "duplicate_boundary",
            "missing_request",
            "missing_abandonment",
            "request_dataset",
            "request_operation",
            "request_digest",
            "unknown_format",
            "abandonment_dataset",
            "abandonment_operation",
            "abandonment_digest",
            "admission_revision",
            "empty_reason",
            "selected_result",
            "future_boundary",
        ] {
            let dataset = Id::new(case).unwrap();
            let mut view = lifecycle.open(&dataset).await.unwrap();
            let (mut request, mut operation) = admission(&engine, &dataset, "first", 1);
            match case {
                "request_dataset" => request.dataset = Id::new("other").unwrap(),
                "request_operation" => request.operation = Id::new("other").unwrap(),
                "unknown_format" => operation.result_format = FORMAT + 1,
                "future_boundary" => operation.boundary = 2,
                _ => {}
            }
            operation.request_digest =
                fingerprint(&OperationRequest::Build(request.clone())).unwrap();
            if case == "request_digest" {
                operation.request_digest = "wrong-digest".into();
            }
            // Keep lifecycle keys fixed even when the embedded request identity is forged.
            let mut admitted = vec![record("operation/first", &operation).unwrap()];
            if case != "missing_request" {
                admitted.push(
                    record("request/first", OperationRequest::Build(request.clone())).unwrap(),
                );
            }
            let duplicate = admission(&engine, &dataset, "second", 1);
            if case == "duplicate_boundary" {
                admitted.extend(admission_records(&duplicate.0, &duplicate.1));
            }
            view = lifecycle.commit(&view, admitted).await.unwrap();
            let mut abandoned = abandonment(&request, &operation, view.revision);
            // The tombstone must identify the canonical operation, not the forged request.
            abandoned.dataset = dataset.clone();
            abandoned.operation = Id::new("first").unwrap();
            match case {
                "abandonment_dataset" => abandoned.dataset = Id::new("other").unwrap(),
                "abandonment_operation" => abandoned.operation = Id::new("other").unwrap(),
                "abandonment_digest" => abandoned.request_digest = "wrong-digest".into(),
                "admission_revision" => abandoned.operation_revision += 1,
                "empty_reason" => abandoned.reason = " \t".into(),
                _ => {}
            }
            if case != "missing_abandonment" {
                let mut retired = vec![record("abandoned/first", abandoned).unwrap()];
                if case == "duplicate_boundary" {
                    retired.push(
                        record(
                            "abandoned/second",
                            abandonment(&duplicate.0, &duplicate.1, view.revision),
                        )
                        .unwrap(),
                    );
                }
                if case == "selected_result" {
                    retired.push(record("result/first", serde_json::json!({})).unwrap());
                }
                view = lifecycle.commit(&view, retired).await.unwrap();
            }
            let fence = if case == "missing_boundary" { 2 } else { 1 };
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, fence)
                .await
                .unwrap_err();
            let expected = if case == "missing_abandonment" {
                "pending_source"
            } else {
                "invalid_admission_history"
            };
            assert_eq!(error.code(), expected, "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn abandoned_gap_requires_original_admission_and_later_visible_abandonment() {
        let (_root, engine) = workspace().await;
        let lifecycle = &engine.inner.lifecycle;
        for case in [
            "same_commit",
            "before_admission",
            "rewritten_request",
            "rewritten_admission",
            "operation_after_view",
            "abandonment_after_view",
        ] {
            let dataset = Id::new(case).unwrap();
            let mut view = lifecycle.open(&dataset).await.unwrap();
            let (request, operation) = admission(&engine, &dataset, "first", 1);
            let mut admitted = admission_records(&request, &operation);
            match case {
                "same_commit" => {
                    admitted.push(
                        record("abandoned/first", abandonment(&request, &operation, 1)).unwrap(),
                    );
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                }
                "before_admission" => {
                    view = lifecycle
                        .commit(
                            &view,
                            vec![
                                record("abandoned/first", abandonment(&request, &operation, 2))
                                    .unwrap(),
                            ],
                        )
                        .await
                        .unwrap();
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                }
                _ => {
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                    let abandoned = abandonment(&request, &operation, view.revision);
                    view = lifecycle
                        .commit(&view, vec![record("abandoned/first", abandoned).unwrap()])
                        .await
                        .unwrap();
                    match case {
                        "rewritten_request" => {
                            view = lifecycle
                                .commit(
                                    &view,
                                    vec![
                                        record(
                                            "request/first",
                                            OperationRequest::Build(request.clone()),
                                        )
                                        .unwrap(),
                                    ],
                                )
                                .await
                                .unwrap();
                        }
                        "rewritten_admission" => {
                            view = lifecycle
                                .commit(&view, admission_records(&request, &operation))
                                .await
                                .unwrap();
                        }
                        "operation_after_view" => view.revision = 0,
                        "abandonment_after_view" => view.revision = 1,
                        _ => unreachable!(),
                    }
                }
            }
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, 1)
                .await
                .unwrap_err();
            assert_eq!(error.code(), "invalid_admission_history", "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn abandoned_gap_refuses_huge_or_reversed_intervals_without_expanding_them() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("bounded").unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        let finite_overflow =
            engine.inner.lifecycle.budget.limits.max_metadata_bytes as u64 / 64 + 1;
        for fence in [finite_overflow, u64::MAX] {
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, fence)
                .await
                .unwrap_err();
            assert_eq!(error.code(), "metadata_capacity", "fence {fence}: {error}");
        }
        let error = engine
            .check_abandoned_gap(&view, &dataset, 2, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "invalid_admission_history");
        engine
            .check_abandoned_gap(&view, &dataset, u64::MAX, u64::MAX)
            .await
            .unwrap();
    }

    fn write_parquet_input(path: &Path, ids: &[i64]) -> InputSpec {
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(ids.iter().copied())),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("row-{id}")),
                )),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(path).unwrap().0,
        }
    }

    #[tokio::test]
    async fn compact_lifecycle_preserves_releases_exports_grants_incomplete_work_and_verification()
    {
        let root = tempfile::TempDir::new().unwrap();
        let limits = Limits {
            compute_threads: 2,
            memory_bytes: 256 << 20,
            scratch_bytes: 64 << 20,
            batch_rows: 128,
            max_batch_bytes: 1 << 20,
            max_metadata_bytes: 4 << 20,
        };
        let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
        let dataset = Id::new("events").unwrap();
        let init_input = write_parquet_input(&root.path().join("init.parquet"), &[1, 2, 3]);
        let r0 = engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: Id::new("op-0").unwrap(),
                base: None,
                relationship: None,
                input: init_input.path,
                expected_digest: init_input.digest,
                sql: "SELECT id, val FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let mut latest = r0.clone();
        for step in 1..=6 {
            let upd = write_parquet_input(
                &root.path().join(format!("upd-{step}.parquet")),
                &[step * 10],
            );
            latest = engine
                .update(&UpdateSpec {
                    dataset: dataset.clone(),
                    operation: Id::new(format!("op-{step}")).unwrap(),
                    base: latest.id.clone(),
                    definition: "v1".into(),
                    keys: vec!["id".into()],
                    replacements: upd,
                    deletions: None,
                    relationship: None,
                })
                .await
                .unwrap();
        }

        // Materialize an ExportPin against the dataset.
        let export_pin = engine
            .export(&crate::export::ExportSpec {
                dataset: dataset.clone(),
                operation: Id::new("exp-1").unwrap(),
                source: latest.version_ref(),
                channel: Id::new("feed").unwrap(),
                expected: None,
                policy: crate::export::ExportPolicy {
                    group: "source".into(),
                    rows: crate::export::ExportRows::All,
                    columns: vec!["id".into(), "val".into()],
                },
            })
            .await
            .unwrap();

        // Create a downstream dataset that consumes `events`, recording an `owner-protection/*` grant.
        let consumer = Id::new("downstream").unwrap();
        let _consumer_release = engine
            .refresh(&RefreshSpec {
                dataset: consumer.clone(),
                operation: Id::new("sub-0").unwrap(),
                base: None,
                definition: "v1".into(),
                transform: RefreshTransform::Aggregate(AggregateTransform {
                    input: latest.version_ref(),
                    grouping: AggregationDefinition {
                        keys: vec!["id".into()],
                        aggregates: vec![AggregateField {
                            name: "rows".into(),
                            function: AggregateFunction::CountRows,
                        }],
                    },
                }),
                policy: RefreshPolicy::default(),
                mode: RefreshMode::default(),
                relationship: None,
            })
            .await
            .unwrap();

        // Admit an unfinished operation on `events` (interrupted before completion).
        let pending_view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        let (pending_req, pending_op) = admission(&engine, &dataset, "op-pending", 8);
        let _ = engine
            .inner
            .lifecycle
            .commit(&pending_view, admission_records(&pending_req, &pending_op))
            .await
            .unwrap();

        assert!(
            Path::new(&r0.manifest.path).is_file(),
            "initial release manifest should exist before compaction"
        );
        let stats = engine.compact_lifecycle(&dataset).await.unwrap();
        assert!(stats.revisions_before > 10);
        assert_eq!(stats.revisions_after, 1);
        assert!(stats.files_before > 10);
        assert_eq!(stats.files_after, 1);
        assert!(stats.rows_after < stats.rows_before);
        assert!(stats.metadata_bytes_after < stats.metadata_bytes_before);

        drop(engine);
        let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
        let cold_view = reopened.inner.lifecycle.open(&dataset).await.unwrap();
        assert_eq!(cold_view.revision, stats.current_revision);
        assert_eq!(cold_view.table.metadata().snapshots().count(), 1);
        assert_eq!(
            cold_view
                .table
                .metadata()
                .properties()
                .get("gc.enabled")
                .map(String::as_str),
            Some("false")
        );
        let lifecycle_loc = std::path::PathBuf::from(cold_view.table.metadata().location());
        let data_files = std::fs::read_dir(lifecycle_loc.join("data"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            data_files, 1,
            "expired lifecycle data files must be unlinked"
        );
        let metadata_json_files = std::fs::read_dir(lifecycle_loc.join("metadata"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".metadata.json"))
            })
            .count();
        assert_eq!(
            metadata_json_files, 1,
            "expired lifecycle metadata.json files must be unlinked"
        );
        let cold_rows = reopened.inner.lifecycle.rows(&cold_view).await.unwrap();
        assert_eq!(cold_rows.history_bytes(), stats.metadata_bytes_after);
        assert!(cold_rows.contains_key("operation/op-pending"));
        assert!(cold_rows.contains_key("export-result/exp-1"));
        assert!(
            cold_rows.prefix("owner-protection/").next().is_some(),
            "active reader grant must remain preserved"
        );

        // Verify release, succession proof, export pin, and retention inspection after cold reopen.
        verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
            .await
            .unwrap();
        let proof = reopened
            .prove_succession(&r0.version_ref(), &latest.version_ref())
            .await
            .unwrap();
        assert_eq!(proof.selections_advanced, 6);
        let objects = latest.dataset.groups["source"].objects.clone();
        let inspection = reopened.inspect_retention(&objects).await.unwrap();
        assert_eq!(inspection.unmatched_bytes, 0);
        assert!(inspection.protected_bytes > 0);
        assert_eq!(export_pin.id, Id::new("exp-1").unwrap());

        // Abandon the unfinished operation so subsequent cutovers and object reclamation can proceed cleanly.
        reopened
            .abandon(&dataset, &Id::new("op-pending").unwrap(), "test cleanup")
            .await
            .unwrap();
        reopened.compact_lifecycle(&dataset).await.unwrap();
        assert!(
            !Path::new(&r0.manifest.path).exists(),
            "checkpointed release manifest should be unlinked from disk once incomplete work is resolved"
        );
        assert!(
            Path::new(&latest.manifest.path).is_file(),
            "active release manifest must remain on disk"
        );
        assert!(
            Path::new(&export_pin.object.path).is_file(),
            "exported parquet file must remain on disk"
        );
        for object in &latest.dataset.groups["source"].objects {
            assert!(
                Path::new(&object.path).is_file(),
                "active source table object {} must remain on disk",
                object.path
            );
        }
        verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
            .await
            .unwrap();
        let inspection = reopened.inspect_retention(&objects).await.unwrap();
        assert_eq!(inspection.unmatched_bytes, 0);
        reopened
            .create_restore_point(&dataset, &Id::new("rp-after-compact").unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn compact_lifecycle_survives_lost_commit_response_with_succession_checkpoint() {
        let root = tempfile::TempDir::new().unwrap();
        let limits = Limits {
            compute_threads: 2,
            memory_bytes: 256 << 20,
            scratch_bytes: 64 << 20,
            batch_rows: 128,
            max_batch_bytes: 1 << 20,
            max_metadata_bytes: 4 << 20,
        };
        let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
        let dataset = Id::new("checkpoint_lost_reply").unwrap();
        let init_input = write_parquet_input(&root.path().join("init.parquet"), &[1, 2]);
        let r0 = engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: Id::new("op-0").unwrap(),
                base: None,
                relationship: None,
                input: init_input.path,
                expected_digest: init_input.digest,
                sql: "SELECT id, val FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let mut latest = r0.clone();
        for step in 1..=3 {
            let upd = write_parquet_input(
                &root.path().join(format!("upd-{step}.parquet")),
                &[step * 10],
            );
            latest = engine
                .update(&UpdateSpec {
                    dataset: dataset.clone(),
                    operation: Id::new(format!("op-{step}")).unwrap(),
                    base: latest.id.clone(),
                    definition: "v1".into(),
                    keys: vec!["id".into()],
                    replacements: upd,
                    deletions: None,
                    relationship: None,
                })
                .await
                .unwrap();
        }

        engine
            .inner
            .lifecycle
            .lose_commit_response
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = engine.compact_lifecycle(&dataset).await.unwrap_err();
        assert_eq!(err.code(), "lost_commit_response");

        drop(engine);
        let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
        let proof = reopened
            .prove_succession(&r0.version_ref(), &latest.version_ref())
            .await
            .unwrap();
        assert_eq!(proof.selections_advanced, 3);
        verify_release_at(&latest, limits.clone(), VerificationLevel::Deep)
            .await
            .unwrap();

        let retry_stats = reopened.compact_lifecycle(&dataset).await.unwrap();
        assert_eq!(retry_stats.revisions_after, 1);
        assert_eq!(retry_stats.files_after, 1);
        assert!(
            !Path::new(&r0.manifest.path).exists(),
            "checkpointed release manifest should be reclaimed on retry after lost commit response"
        );
        let proof_after = reopened
            .prove_succession(&r0.version_ref(), &latest.version_ref())
            .await
            .unwrap();
        assert_eq!(proof_after.selections_advanced, 3);
        verify_release_at(&latest, limits, VerificationLevel::Deep)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn compact_lifecycle_keeps_260_operations_within_metadata_budget() {
        let root = tempfile::TempDir::new().unwrap();
        let limits = Limits {
            max_metadata_bytes: 3 << 20,
            ..Limits::default()
        };
        let engine = Engine::open(root.path(), limits.clone()).await.unwrap();
        let dataset = Id::new("hourly").unwrap();
        let init_input = write_parquet_input(&root.path().join("init.parquet"), &[0, 1]);
        let first = engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: Id::new("op-0").unwrap(),
                base: None,
                relationship: None,
                input: init_input.path.clone(),
                expected_digest: init_input.digest.clone(),
                sql: "SELECT id, val FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let mut latest = first.clone();
        let mut view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        for step in 1..260_u64 {
            let name = format!("op-{step}");
            let (mut req, mut op) = admission(&engine, &dataset, &name, step + 1);
            req.input = init_input.path.clone();
            req.expected_digest = init_input.digest.clone();
            req.sql = format!("SELECT id, val FROM input WHERE id >= -{step}");
            op.request_digest = fingerprint(&OperationRequest::Build(req.clone())).unwrap();
            let phase = PhaseResult {
                operation: req.operation.clone(),
                phase: "processing".into(),
                request_digest: op.request_digest.clone(),
                eligibility: 0,
                outputs: latest.dataset.groups["source"].objects.clone(),
                table: Some(latest.dataset.groups["source"].clone()),
                rows: latest.dataset.groups["source"].rows,
                changes: None,
                coverage: None,
                metrics: WriteMetrics::default(),
                dependencies: Some(PhaseDependencies::default()),
            };
            let mut validation = phase.clone();
            validation.phase = "validation".into();
            let mut next_pin = latest.clone();
            next_pin.id = req.operation.clone();
            next_pin.dataset.version = req.operation.clone();
            next_pin.dataset.boundary = op.boundary;
            next_pin.dataset.request = OperationRequest::Build(req.clone());
            let link = crate::model::Succession {
                selected: next_pin.version_ref(),
                predecessor: Some(latest.version_ref()),
                boundary: op.boundary,
                eligibility: op.eligibility,
            };
            let authority = Authority {
                definition: "v1".into(),
                eligibility: 0,
                epoch: 0,
                boundary: op.boundary,
                selected: Some(next_pin.dataset.clone()),
                release: Some(next_pin.clone()),
                selection_revision: Some(view.revision + 2),
            };
            view = engine
                .inner
                .lifecycle
                .commit(
                    &view,
                    vec![
                        record(format!("operation/{name}"), &op).unwrap(),
                        record(
                            format!("request/{name}"),
                            OperationRequest::Build(req.clone()),
                        )
                        .unwrap(),
                        record(format!("phase/{}/0/processing", op.request_digest), &phase)
                            .unwrap(),
                        record(
                            format!("phase/checked/{}/validation", op.request_digest),
                            &validation,
                        )
                        .unwrap(),
                        record(
                            format!("readback/{name}"),
                            ValidatedReadback::stamp(&next_pin),
                        )
                        .unwrap(),
                    ],
                )
                .await
                .unwrap();
            view = engine
                .inner
                .lifecycle
                .commit(
                    &view,
                    vec![
                        record(format!("result/{name}"), &next_pin).unwrap(),
                        record(format!("succession/{name}"), &link).unwrap(),
                        record("authority", &authority).unwrap(),
                    ],
                )
                .await
                .unwrap();
            latest = next_pin;
            if step % 26 == 0 {
                let stats = engine.compact_lifecycle(&dataset).await.unwrap();
                assert_eq!(stats.revisions_after, 1);
                assert_eq!(stats.files_after, 1);
                assert!(
                    stats.rows_after <= 20,
                    "expected O(1) rows_after, got {}",
                    stats.rows_after
                );
                assert!(
                    stats.metadata_bytes_after < 32 << 10,
                    "expected O(1) metadata_bytes_after < 32 KiB, got {}",
                    stats.metadata_bytes_after
                );
                view = engine.inner.lifecycle.open(&dataset).await.unwrap();
            }
        }
        // Complete operation 260 via a real DataFusion build over the compacted lifecycle.
        let upd_input = write_parquet_input(&root.path().join("upd-260.parquet"), &[1, 2]);
        latest = engine
            .run(&RunSpec {
                dataset: dataset.clone(),
                operation: Id::new("op-260").unwrap(),
                base: None,
                relationship: None,
                input: upd_input.path,
                expected_digest: upd_input.digest,
                sql: "SELECT id, val FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            })
            .await
            .unwrap();
        let final_stats = engine.compact_lifecycle(&dataset).await.unwrap();
        assert_eq!(final_stats.revisions_after, 1);
        assert_eq!(final_stats.files_after, 1);
        assert!(
            final_stats.rows_after <= 32,
            "rows_after={} must remain O(1) bounded",
            final_stats.rows_after
        );
        assert!(
            final_stats.metadata_bytes_after < 32 << 10,
            "metadata_bytes_after={} must remain O(1) bounded (< 32 KiB)",
            final_stats.metadata_bytes_after
        );

        drop(engine);
        let reopened = Engine::open(root.path(), limits.clone()).await.unwrap();
        let cold_view = reopened.inner.lifecycle.open(&dataset).await.unwrap();
        let cold_rows = reopened.inner.lifecycle.rows(&cold_view).await.unwrap();
        assert_eq!(cold_rows.history_bytes(), final_stats.metadata_bytes_after);
        assert!(cold_rows.contains_key("succession-checkpoint"));
        assert!(!cold_rows.contains_key("result/op-0"));
        assert!(!cold_rows.contains_key("result/op-259"));
        assert!(cold_rows.contains_key("result/op-260"));
        verify_release_at(&latest, limits, VerificationLevel::Deep)
            .await
            .unwrap();
        let proof = reopened
            .prove_succession(&first.version_ref(), &latest.version_ref())
            .await
            .unwrap();
        assert_eq!(proof.selections_advanced, 260);
    }
}
