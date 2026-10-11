//! Named original host children reuse exact removal intent and unlink outcomes.
// Rust guideline compliant 2026-02-21
use super::export_stage::HostStageProof;
use super::*;
use crate::engine::export::host::{self, HostFile};

impl Engine {
    pub(super) fn export_file_creation(
        &self,
        rows: &SnapshotRows,
        plan: &RemovalPlan,
    ) -> Result<HostFile> {
        let RemovalScope::ExportHostFile {
            operation,
            operation_revision,
            file,
            creation_revision,
            host_root,
        } = &plan.scope
        else {
            return Err(invalid());
        };
        let key = format!("export-host-file/{operation}/{operation_revision}/{file}");
        let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&key])?;
        let (revision, _, created): (_, _, HostFile) =
            rows.get_record(&key)?.ok_or_else(invalid)?;
        host::validate(
            &created,
            &key,
            revision,
            rows,
            Some(&self.inner.lifecycle.budget),
        )?;
        if revision != *creation_revision || &created.host_root != host_root {
            return Err(invalid());
        }
        Ok(created)
    }

    pub(super) fn validate_export_file_plan(
        &self,
        rows: &SnapshotRows,
        view: &View,
        plan: &RemovalPlan,
    ) -> Result<HostStageProof> {
        let created = self.export_file_creation(rows, plan)?;
        self.check_export_file_promises(rows, &created.path)?;
        let key = host::key(&created);
        if !super::super::retirement::record_retired(
            rows,
            view,
            &key,
            rows.revision(&key).ok_or_else(invalid)?,
            &self.inner.lifecycle.budget,
        )? {
            return Err(Error::refusal(
                "export_stage_pending",
                "end exact export recovery before removing its original host child",
            ));
        }
        let retained_path = host::validate(
            &created,
            &key,
            rows.revision(&key).ok_or_else(invalid)?,
            rows,
            Some(&self.inner.lifecycle.budget),
        )?;
        if plan.inspection.objects.len() != 1
            || plan.inspection.objects[0].blocker.is_some()
            || plan.inspection.objects[0].object.path != created.path
            || plan.inspection.objects[0].object.owner != plan.dataset
            || plan.inspection.boundaries.len() != 1
            || plan.inspection.boundaries[0].table != view.identifier().to_string()
            || !plan.registrations.is_empty()
        {
            return Err(invalid());
        }
        let object = plan.inspection.objects[0].object.clone();
        let closed_key = key.replacen("export-host-file/", "export-host-closed/", 1);
        let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&closed_key])?;
        if let Some((revision, _, closed)) =
            rows.get_record::<crate::engine::export::ExportMaterialized>(&closed_key)?
        {
            let operation_key = format!("export-operation/{}", created.operation);
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&operation_key])?;
            let (_, _, operation): (_, _, crate::engine::export::ExportOperation) =
                rows.get_record(&operation_key)?.ok_or_else(invalid)?;
            crate::engine::export::check_materialized(&closed, &operation)?;
            if closed.object != object || revision <= rows.revision(&key).ok_or_else(invalid)? {
                return Err(invalid());
            }
        }
        Ok(HostStageProof {
            object,
            retained_path,
        })
    }

    fn check_export_file_promises(&self, rows: &SnapshotRows, path: &str) -> Result<()> {
        for (key, _, json) in rows
            .prefix("export-result/")
            .chain(rows.prefix("export-channel/"))
        {
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[key])?;
            let pin: crate::export::ExportPin = serde_json::from_str(json)?;
            crate::engine::export::check_pin(&pin)?;
            if pin.object.path == path {
                return Err(Error::refusal(
                    "export_stage_protected",
                    "an immutable export pin still promises these exact payload bytes",
                ));
            }
        }
        Ok(())
    }

    /// Previews a named original export child after explicit recovery retirement.
    ///
    /// Closed files reuse their saved hash. Unclosed files need one bounded hash
    /// because their exact final bytes have never been recorded. No prefix is swept.
    ///
    /// # Errors
    /// Refuses active recovery, changed original identity, symlinks, unavailable
    /// host custody, active owner grants or reservations, and metadata capacity.
    pub async fn plan_export_file_removal(
        &self,
        dataset: &Id,
        removal: &Id,
        operation: &Id,
        file: &Id,
        reason: &str,
    ) -> Result<RemovalPlan> {
        let memory = self.request_memory(&(dataset, removal, operation, file, reason))?;
        let (dataset, removal, operation, file, reason) = (
            dataset.clone(),
            removal.clone(),
            operation.clone(),
            file.clone(),
            reason.to_owned(),
        );
        self.admit_work(memory, move |engine| async move {
            if reason.trim().is_empty() {
                return Err(invalid());
            }
            let view = engine.existing_removal_view(&dataset).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let operation_revision = rows
                .revision(&format!("export-operation/{operation}"))
                .ok_or_else(invalid)?;
            let key = format!("export-host-file/{operation}/{operation_revision}/{file}");
            let _memory = rows.reserve_decoded(&engine.inner.lifecycle.budget, &[&key])?;
            let (creation_revision, _, created): (_, _, HostFile) =
                rows.get_record(&key)?.ok_or_else(invalid)?;
            let retained_path = host::validate(
                &created,
                &key,
                creation_revision,
                &rows,
                Some(&engine.inner.lifecycle.budget),
            )?;
            let scope = RemovalScope::ExportHostFile {
                operation,
                operation_revision,
                file,
                creation_revision,
                host_root: created.host_root.clone(),
            };
            engine.check_removal_scope(&scope)?;
            engine
                .inner
                .objects
                .validate_retained_path(&retained_path)?;
            if !super::super::retirement::record_retired(
                &rows,
                &view,
                &key,
                creation_revision,
                &engine.inner.lifecycle.budget,
            )? {
                return Err(Error::refusal(
                    "export_stage_pending",
                    "end exact export recovery before removing its original host child",
                ));
            }
            let closed_key = key.replacen("export-host-file/", "export-host-closed/", 1);
            let _closed_memory =
                rows.reserve_decoded(&engine.inner.lifecycle.budget, &[&closed_key])?;
            let object = if let Some((_, _, closed)) =
                rows.get_record::<crate::engine::export::ExportMaterialized>(&closed_key)?
            {
                closed.object
            } else {
                storage::removal::inspect_created_file(
                    &engine.inner.root,
                    &ObjectRef {
                        owner: dataset.clone(),
                        path: created.path.clone(),
                        digest: "0".repeat(64),
                        bytes: 0,
                    },
                    created.device,
                    created.inode,
                    &engine.inner.lifecycle.budget,
                )
                .await?
            };
            engine.check_stage_grants(&rows, &view, &object)?;
            super::super::owners::available(&rows, &view, &object)?;
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
                    protected_bytes: 0,
                    unmatched_bytes: object.bytes,
                    metadata_objects_read: 0,
                    objects: vec![ObjectProtection {
                        object,
                        blocker: None,
                    }],
                },
                registrations: Vec::new(),
            };
            engine.validate_removal_plan(&view, &plan)?;
            engine.validate_export_file_plan(&rows, &view, &plan)?;
            Ok(plan)
        })
        .await
    }
}
