// Rust guideline compliant 2026-02-21

use super::*;
use crate::catalog::View;
use serde::Deserialize;
use std::collections::BTreeSet;

pub(super) struct CheckpointCompactionPlan {
    pub(super) records: Vec<(String, serde_json::Value)>,
    pub(super) new_archive_path: String,
    pub(super) old_archive_path: Option<String>,
}

#[derive(Deserialize)]
struct CompactionRequestRef {
    spec: CompactionSpecVersionRef,
}

#[derive(Deserialize)]
struct CompactionSpecVersionRef {
    #[serde(alias = "base")]
    version: Id,
}

#[derive(Deserialize)]
struct ContinuousRecordRef {
    initial: VersionFieldRef,
}

#[derive(Deserialize)]
struct VersionFieldRef {
    version: Id,
}

#[derive(Deserialize)]
struct ContinuousChildRecordRef {
    capture: Option<VersionFieldRef>,
    child: Option<Id>,
}

struct ObsoleteLifecycleIndex {
    authority: Authority,
    selected_id: Option<Id>,
    protected_keys: BTreeSet<String>,
    retired_ops: BTreeSet<Id>,
    completed_non_selected: BTreeSet<Id>,
    incomplete_ops: BTreeSet<Id>,
    incomplete_digests: BTreeSet<String>,
}

impl ObsoleteLifecycleIndex {
    fn from_rows(rows: &crate::catalog::SnapshotRows) -> Result<Self> {
        let authority: Authority = rows
            .get_record("authority")?
            .map(|(_, _, value)| value)
            .unwrap_or_default();
        let selected_id = authority.release.as_ref().map(|release| release.id.clone());

        let mut protected_keys = BTreeSet::new();
        let mut retired_ops = BTreeSet::new();
        for (_, _, json) in rows.prefix("retirement/") {
            let receipt: Retirement = serde_json::from_str(json)?;
            if let RetirementScope::OperationRecovery { operation } = &receipt.plan.scope {
                retired_ops.insert(operation.clone());
                protected_keys.insert(format!("operation/{operation}"));
                protected_keys.insert(format!("request/{operation}"));
            } else {
                for generation in receipt.plan.records {
                    protected_keys.insert(generation.key);
                }
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
                    && !retired_ops.contains(&op)
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

        Ok(Self {
            authority,
            selected_id,
            protected_keys,
            retired_ops,
            completed_non_selected,
            incomplete_ops,
            incomplete_digests,
        })
    }

    fn is_obsolete_op(&self, rows: &crate::catalog::SnapshotRows, op: &Id) -> bool {
        if Some(op) == self.selected_id.as_ref()
            || self.incomplete_ops.contains(op)
            || (rows.contains_key(&format!("availability-retired/{op}"))
                && !self.retired_ops.contains(op))
        {
            return false;
        }
        self.retired_ops.contains(op)
            || self.completed_non_selected.contains(op)
            || !rows.contains_key(&format!("operation/{op}"))
    }
}

impl Engine {
    pub(super) async fn compact_lifecycle_admitted(
        &self,
        dataset: &Id,
    ) -> Result<crate::catalog::LifecycleCompactionResult> {
        self.require_data_work()?;
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
        for (key, _, _) in rows.prefix("availability-retired/") {
            if let Some(op_str) = key.strip_prefix("availability-retired/")
                && let Ok(op) = Id::new(op_str)
            {
                protected_ops.insert(op);
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
            if let Ok(req) = serde_json::from_str::<CompactionRequestRef>(json)
                && (is_incomplete || req.spec.version == head_release.id)
            {
                protected_ops.insert(req.spec.version);
            }
        }
        for (key, _, json) in rows.prefix("continuous") {
            if key.starts_with("continuous/") {
                if let Ok(rec) = serde_json::from_str::<ContinuousRecordRef>(json) {
                    protected_ops.insert(rec.initial.version);
                }
            } else if key.starts_with("continuous-child/")
                && let Ok(rec) = serde_json::from_str::<ContinuousChildRecordRef>(json)
            {
                if let Some(capture) = rec.capture {
                    protected_ops.insert(capture.version);
                }
                if let Some(child) = rec.child {
                    protected_ops.insert(child);
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
        let new_archive_path = prefix.child("succession-checkpoint.json")?.as_string()?;
        let archive = SuccessionArchive {
            dataset: dataset.clone(),
            head: head_link.selected.clone(),
            head_revision: *head_revision,
            source_boundaries: total_source_boundaries,
            entries,
        };
        let archive_bytes = serde_json::to_vec(&archive)?;
        let archive_obj = ObjectRef {
            owner: dataset.clone(),
            path: new_archive_path.clone(),
            digest: blake3::hash(&archive_bytes).to_hex().to_string(),
            bytes: archive_bytes.len() as u64,
        };
        self.inner
            .objects
            .create_candidate_metadata(
                &archive_obj,
                archive_bytes,
                &self.inner.lifecycle.budget,
                false,
            )
            .await?;
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

fn collect_obsolete_lifecycle_keys(
    rows: &crate::catalog::SnapshotRows,
) -> Result<BTreeSet<String>> {
    let index = ObsoleteLifecycleIndex::from_rows(rows)?;
    let mut candidate_keys = BTreeSet::new();
    for op in index
        .completed_non_selected
        .iter()
        .chain(index.retired_ops.iter())
    {
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

    for (key, _, json) in rows.prefix("phase/") {
        if key.starts_with("phase/backfill/") {
            continue;
        }
        if !index.incomplete_ops.is_empty()
            && (key.starts_with("phase/input/")
                || key.starts_with("phase/build/")
                || key.starts_with("phase/checked/"))
        {
            continue;
        }
        if index
            .incomplete_digests
            .iter()
            .any(|d| key.starts_with(&format!("phase/{d}/")))
        {
            continue;
        }
        if let Ok(phase) = serde_json::from_str::<PhaseResult>(json)
            && index.is_obsolete_op(rows, &phase.operation)
        {
            candidate_keys.insert(key.to_owned());
        }
    }

    if index.incomplete_ops.is_empty()
        && let Some(head_release) = &index.authority.release
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
                    || index.protected_keys.contains(key)
                    || index.protected_keys.contains(&owners::binding_key(key))
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
                && index.is_obsolete_op(rows, &capture.spec.version)
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
                    index.protected_keys.contains(&k)
                        || index.protected_keys.contains(&owners::binding_key(&k))
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
        let bkey = owners::binding_key(&key);
        if index.protected_keys.contains(&key) || index.protected_keys.contains(&bkey) {
            continue;
        }
        if rows.contains_key(&bkey) {
            remove_keys.insert(bkey);
        }
        remove_keys.insert(key);
    }

    Ok(remove_keys)
}

fn collect_record_paths(value: &serde_json::Value, paths: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(s) if s.contains('/') => {
            paths.insert(s.clone());
        }
        serde_json::Value::Array(items) => {
            items.iter().for_each(|i| collect_record_paths(i, paths))
        }
        serde_json::Value::Object(map) => map.values().for_each(|v| collect_record_paths(v, paths)),
        _ => {}
    }
}

fn prune_obsolete_protections(
    rows: &crate::catalog::SnapshotRows,
    head_ref_paths: &BTreeSet<String>,
    remove_keys: &mut BTreeSet<String>,
) -> Result<()> {
    let index = ObsoleteLifecycleIndex::from_rows(rows)?;
    let mut kept_record_paths = head_ref_paths.clone();
    for (k, _, json) in rows.prefix("") {
        if !remove_keys.contains(k)
            && !k.starts_with("protection/")
            && !k.starts_with("reference-binding/")
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(json)
        {
            collect_record_paths(&value, &mut kept_record_paths);
        }
    }

    for (key, _, json) in rows.prefix("protection/") {
        let bkey = owners::binding_key(key);
        if index.protected_keys.contains(key) || index.protected_keys.contains(&bkey) {
            continue;
        }
        let Ok(protection) = serde_json::from_str::<Protection>(json) else {
            continue;
        };
        let needle = format!("/{}/", protection.id.as_str());
        if protection
            .paths
            .iter()
            .any(|pfx| kept_record_paths.iter().any(|p| p.starts_with(pfx)))
            || kept_record_paths.iter().any(|p| p.contains(&needle))
        {
            continue;
        }
        let Some((op_str, kind)) = protection.purpose.split_once('/') else {
            continue;
        };
        if kind == "compaction" {
            let req_key = format!("compaction-request/{op_str}");
            let res_key = format!("compaction-result/{op_str}");
            if (!rows.contains_key(&req_key) || remove_keys.contains(&req_key))
                && (!rows.contains_key(&res_key) || remove_keys.contains(&res_key))
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
        if !index.is_obsolete_op(rows, &op) {
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
