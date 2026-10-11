//! Exact closed host stages share durable removal intent and outcomes.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::engine::export::remote::{CompletedStage, completed_stage};

pub(super) struct HostStageProof {
    pub object: ObjectRef,
    pub retained_path: String,
}

impl Engine {
    pub(super) fn check_removal_scope(&self, scope: &RemovalScope) -> Result<()> {
        match scope {
            RemovalScope::Objects => self.removal_catalog(),
            RemovalScope::ExportHostStage { host_root, .. }
            | RemovalScope::ExportHostFile { host_root, .. } => {
                if !cfg!(unix) {
                    return Err(Error::refusal(
                        "removal_platform",
                        "local removal identity checks require Unix",
                    ));
                }
                if host_root != self.inner.root.to_string_lossy().as_ref()
                    || (matches!(scope, RemovalScope::ExportHostStage { .. })
                        && matches!(&self.inner.objects, storage::Objects::Local))
                {
                    return Err(Error::refusal(
                        "export_host_unavailable",
                        "export file belongs to another configured exclusive host",
                    ));
                }
                Ok(())
            }
        }
    }

    fn export_stage(&self, rows: &SnapshotRows, operation: &Id) -> Result<CompletedStage> {
        let stage = completed_stage(rows, operation, Some(&self.inner.lifecycle.budget))?
            .ok_or_else(|| {
                Error::refusal(
                    "export_stage_pending",
                    "verified remote export completion must precede stage removal",
                )
            })?;
        let expected = self
            .inner
            .root
            .join("objects")
            .join(stage.object.owner.as_str())
            .join(stage.attempt.as_str());
        if stage.host_prefix != format!("{}/", expected.display()) {
            return Err(Error::refusal(
                "export_host_unavailable",
                "completed export stage belongs to another exclusive host",
            ));
        }
        self.inner
            .objects
            .validate_retained_path(&stage.remote.path)?;
        Ok(stage)
    }

    pub(super) fn validate_export_stage_plan(
        &self,
        rows: &SnapshotRows,
        view: &View,
        plan: &RemovalPlan,
    ) -> Result<HostStageProof> {
        if matches!(plan.scope, RemovalScope::ExportHostFile { .. }) {
            return self.validate_export_file_plan(rows, view, plan);
        }
        let RemovalScope::ExportHostStage {
            operation,
            operation_revision,
            stage_revision,
            completion_revision,
            host_root,
        } = &plan.scope
        else {
            return Err(invalid());
        };
        let stage = completed_stage(rows, operation, Some(&self.inner.lifecycle.budget))?
            .ok_or_else(invalid)?;
        let expected = Path::new(host_root)
            .join("objects")
            .join(stage.object.owner.as_str())
            .join(stage.attempt.as_str());
        if stage.host_prefix != format!("{}/", expected.display())
            || stage.object.owner != plan.dataset
            || stage.operation_revision != *operation_revision
            || stage.stage_revision != *stage_revision
            || stage.completion_revision != *completion_revision
            || plan.inspection.objects
                != [ObjectProtection {
                    object: stage.object.clone(),
                    blocker: None,
                }]
            || plan.inspection.boundaries.len() != 1
            || plan.inspection.boundaries[0].table != view.identifier().to_string()
            || !plan.registrations.is_empty()
        {
            return Err(invalid());
        }
        Ok(HostStageProof {
            object: stage.object,
            retained_path: stage.remote.path,
        })
    }

    pub(super) fn check_stage_grants(
        &self,
        rows: &SnapshotRows,
        view: &View,
        object: &ObjectRef,
    ) -> Result<()> {
        let budget = &self.inner.lifecycle.budget;
        for (key, revision, _) in rows.prefix("owner-protection/") {
            let _memory = rows.reserve_decoded(budget, &[key])?;
            let (_, _, grant): (_, _, super::super::owners::OwnerProtection) =
                rows.get_record(key)?.ok_or_else(invalid)?;
            super::super::owners::validate_grant(view, key, revision, &grant)?;
            for retained in grant
                .objects
                .iter()
                .filter(|retained| retained.path == object.path)
            {
                if retained != object {
                    return Err(invalid());
                }
                if !super::super::retirement::record_retired(rows, view, key, revision, budget)? {
                    return Err(Error::refusal(
                        "export_stage_protected",
                        "a retained owner grant still promises these exact host bytes",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Previews one exact original host stage after verified remote co-completion.
    ///
    /// This does not authorize remote object deletion or whole-catalog removal.
    /// Original attempt, protection and phase metadata remain immutable.
    ///
    /// # Errors
    /// Refuses incomplete remote transfer, foreign host custody, unsupported
    /// platforms, malformed retained proof, empty reasons or exhausted capacity.
    pub async fn plan_export_stage_removal(
        &self,
        dataset: &Id,
        removal: &Id,
        operation: &Id,
        reason: &str,
    ) -> Result<RemovalPlan> {
        let memory = self.request_memory(&(dataset, removal, operation, reason))?;
        let (dataset, removal, operation, reason) = (
            dataset.clone(),
            removal.clone(),
            operation.clone(),
            reason.to_owned(),
        );
        self.admit_work(memory, move |engine| async move {
            if reason.trim().is_empty() {
                return Err(invalid());
            }
            let view = engine.existing_removal_view(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let stage = engine.export_stage(&rows, &operation)?;
            let scope = RemovalScope::ExportHostStage {
                operation,
                operation_revision: stage.operation_revision,
                stage_revision: stage.stage_revision,
                completion_revision: stage.completion_revision,
                host_root: engine.inner.root.to_string_lossy().into_owned(),
            };
            engine.check_removal_scope(&scope)?;
            engine.check_stage_grants(&rows, &view, &stage.object)?;
            super::super::owners::available(&rows, &view, &stage.object)?;
            let bytes = stage.object.bytes;
            let plan = RemovalPlan {
                scope,
                dataset,
                removal,
                reason,
                table_uuid: view.table.metadata().uuid().to_string(),
                inspection: RetentionInspection {
                    boundaries: vec![RetentionBoundary {
                        table: view.identifier().to_string(),
                        metadata: view.table.metadata_location_result()?.into(),
                        revision: view.revision,
                    }],
                    objects: vec![ObjectProtection {
                        object: stage.object.clone(),
                        blocker: None,
                    }],
                    protected_bytes: 0,
                    unmatched_bytes: bytes,
                    metadata_objects_read: 0,
                },
                registrations: Vec::new(),
            };
            engine.validate_removal_plan(&view, &plan)?;
            engine.validate_export_stage_plan(&rows, &view, &plan)?;
            Ok(plan)
        })
        .await
    }

    pub(super) async fn remove_export_stage_admitted(
        &self,
        plan: &RemovalPlan,
        stop: Option<&str>,
    ) -> Result<RemovalStatus> {
        self.check_removal_scope(&plan.scope)?;
        let lock = self.lock(&plan.dataset).await;
        let _guard = lock.lock().await;
        let view = self.existing_removal_view(&plan.dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        if rows.contains_key(&intent_key(&plan.removal)) {
            let _memory = rows.reserve_decoded(
                &self.inner.lifecycle.budget,
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
        let stage = self.validate_export_stage_plan(&rows, &view, plan)?;
        self.inner
            .objects
            .validate_retained_path(&stage.retained_path)?;
        let object = &plan.inspection.objects[0].object;
        self.check_stage_grants(&rows, &view, object)?;
        super::super::owners::available(&rows, &view, object)?;
        let intent = RemovalIntent {
            plan: plan.clone(),
            revision: view.revision.checked_add(1).ok_or_else(invalid)?,
            boundaries: vec![RetentionBoundary {
                table: view.identifier().to_string(),
                metadata: view.table.metadata_location_result()?.into(),
                revision: view.revision,
            }],
            blocked: Vec::new(),
            objects: vec![0],
            registrations: Vec::new(),
        };
        drop(rows);
        let bytes = inputs::metadata_size(
            &intent,
            self.inner.lifecycle.budget.limits.max_metadata_bytes,
        )?;
        let _encoding = self
            .inner
            .lifecycle
            .budget
            .reserve(bytes.saturating_mul(8).saturating_add(4096))?;
        self.start_removal(view, intent, stop).await
    }
}

#[cfg(all(test, feature = "remote", feature = "test-util", unix))]
impl Engine {
    pub(in crate::engine) async fn test_stage_grant_lifetime(
        &self,
        object: &ObjectRef,
        operation: &Id,
    ) {
        use super::super::owners::{OwnerProtection, protection_key};
        use super::super::retirement::{RetiredRecord, marker_key};
        let removal = Id::new("grant-stage-removal").unwrap();
        let plan = self
            .plan_export_stage_removal(&object.owner, &removal, operation, "closed stage cleanup")
            .await
            .unwrap();
        let view = self.existing_removal_view(&object.owner).await.unwrap();
        // A retained valid grant is a stronger promise even though current
        // public admission cannot create host-stage grants. This metadata fixture
        // exercises conservative replay, not a newly supported public grant API.
        let grant = OwnerProtection {
            owner: object.owner.clone(),
            consumer: Id::new("old-consumer").unwrap(),
            table_uuid: view.table.metadata().uuid().to_string(),
            revision: view.revision + 1,
            objects: vec![object.clone()],
        };
        let key = protection_key(&grant).unwrap();
        self.inner
            .lifecycle
            .commit(&view, vec![record(&key, &grant).unwrap()])
            .await
            .unwrap();
        assert_eq!(
            self.remove_objects(&plan).await.unwrap_err().code(),
            "export_stage_protected"
        );
        assert_eq!(
            self.plan_export_stage_removal(
                &object.owner,
                &removal,
                operation,
                "closed stage cleanup"
            )
            .await
            .unwrap_err()
            .code(),
            "export_stage_protected"
        );
        assert!(Path::new(&object.path).exists());
        let view = self.existing_removal_view(&object.owner).await.unwrap();
        let generation = RetainedGeneration {
            key,
            revision: grant.revision,
        };
        let retirement = Id::new("old-stage-grant").unwrap();
        let receipt = Retirement {
            revision: view.revision + 1,
            plan: RetentionPlan {
                dataset: object.owner.clone(),
                retirement: retirement.clone(),
                scope: RetirementScope::OwnerGrants,
                reason: "end legacy host-stage grant".into(),
                table_uuid: view.table.metadata().uuid().to_string(),
                metadata: view.table.metadata_location_result().unwrap().into(),
                snapshot_id: view
                    .table
                    .metadata()
                    .current_snapshot()
                    .unwrap()
                    .snapshot_id(),
                revision: view.revision,
                records: vec![generation.clone()],
            },
        };
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(
                        marker_key(&generation),
                        RetiredRecord {
                            dataset: object.owner.clone(),
                            table_uuid: receipt.plan.table_uuid.clone(),
                            record: generation,
                            retirement: retirement.clone(),
                        },
                    )
                    .unwrap(),
                    record(format!("retirement/{retirement}"), receipt).unwrap(),
                ],
            )
            .await
            .unwrap();
        assert!(self.remove_objects(&plan).await.unwrap().complete);
        assert!(!Path::new(&object.path).exists());
    }
}
