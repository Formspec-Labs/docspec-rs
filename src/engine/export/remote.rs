//! Protected remote destinations and separately retained host export stages.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::catalog::SnapshotRows;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct ExportAttempt {
    id: Id,
    operation: Id,
    operation_revision: i64,
    request_digest: String,
    eligibility: u64,
    epoch: u64,
    host_prefix: String,
    retained_prefix: String,
    destination: String,
}

/// Immutable creation evidence for one closed original host stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::engine) struct CompletedStage {
    pub(in crate::engine) object: ObjectRef,
    pub(in crate::engine) remote: ObjectRef,
    pub(in crate::engine) attempt: Id,
    pub(in crate::engine) operation_revision: i64,
    pub(in crate::engine) stage_revision: i64,
    pub(in crate::engine) completion_revision: i64,
    pub(in crate::engine) host_prefix: String,
}

impl Engine {
    pub(super) async fn remote_materialize_export(
        &self,
        operation: &ExportOperation,
        stop: Option<&str>,
    ) -> Result<ExportMaterialized> {
        let attempt = self.reserve_export_attempt(operation).await?;
        let suffix = format!("{}/{}", attempt.operation, attempt.operation_revision);
        let staged_key = format!("export-staged/{suffix}");
        let staged_validation_key = format!("export-stage-validation/{suffix}");
        let view = self
            .inner
            .lifecycle
            .open(&operation.request.dataset)
            .await?;
        let staged = match self
            .inner
            .lifecycle
            .get::<ExportMaterialized>(&view, &staged_key)
            .await?
        {
            Some(staged) => staged,
            None => {
                let staged = self
                    .materialize_export(operation, Some(PathBuf::from(&attempt.host_prefix)))
                    .await?;
                self.retain_export_record(operation, &staged_key, &staged)
                    .await?
            }
        };
        check_staged(&attempt, &staged, operation)?;
        stop_after(stop, "staging")?;
        let view = self
            .inner
            .lifecycle
            .open(&operation.request.dataset)
            .await?;
        let validation = match self
            .inner
            .lifecycle
            .get::<ExportValidation>(&view, &staged_validation_key)
            .await?
        {
            Some(validation) => validation,
            None => {
                let validation = validate_output(&staged, &self.inner.lifecycle.budget).await?;
                self.retain_export_record(operation, &staged_validation_key, &validation)
                    .await?
            }
        };
        check_staged_validation(&staged, &validation)?;
        stop_after(stop, "stage-validation")?;
        let expected = ObjectRef {
            path: attempt.destination.clone(),
            ..staged.object.clone()
        };
        self.inner.objects.validate_retained_path(&expected.path)?;
        let view = self
            .inner
            .lifecycle
            .open(&operation.request.dataset)
            .await?;
        self.check_export_authority(&view, operation).await?;
        self.check_export_channel(&view, &operation.request, operation.source_revision)
            .await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let attempt_key = format!("export-attempt/{suffix}");
        check_attempt(
            &attempt_key,
            &attempt,
            operation,
            &rows,
            Some(&self.inner.lifecycle.budget),
        )?;
        check_stage_order(&attempt_key, &staged_key, &rows)?;
        if rows
            .revision(&staged_validation_key)
            .ok_or_else(invalid_export)?
            <= rows.revision(&staged_key).ok_or_else(invalid_export)?
        {
            return Err(invalid_export());
        }
        // The saved destination and known staged identity precede PUT. Reconcile
        // that destination before opening host bytes, even if staging is missing.
        let transferred = self
            .inner
            .objects
            .transfer_immutable_file(
                &expected,
                PathBuf::from(&staged.object.path),
                &self.inner.lifecycle.budget,
                true,
            )
            .await?;
        if transferred != expected {
            return Err(invalid_export());
        }
        stop_after(stop, "transfer-uncommitted")?;
        let materialized = ExportMaterialized {
            object: transferred.clone(),
            ..staged
        };
        let validation = ExportValidation {
            object: transferred,
            ..validation
        };
        let dataset = &operation.request.dataset;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.check_export_authority(&view, operation).await?;
        self.check_export_channel(&view, &operation.request, operation.source_revision)
            .await?;
        let materialized_key = format!("export-materialized/{}", operation.request.operation);
        let validation_key = format!("export-validation/{}", operation.request.operation);
        if let Some(saved) = self
            .inner
            .lifecycle
            .get::<ExportMaterialized>(&view, &materialized_key)
            .await?
        {
            let saved_validation: ExportValidation = self
                .inner
                .lifecycle
                .get(&view, &validation_key)
                .await?
                .ok_or_else(invalid_export)?;
            if saved != materialized || saved_validation != validation {
                return Err(invalid_export());
            }
            return Ok(saved);
        }
        // These two completions use remote identity and become durable together.
        // The stage records describe host custody, not remote creation evidence.
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(&materialized_key, &materialized)?,
                    record(&validation_key, &validation)?,
                ],
            )
            .await?;
        stop_after(stop, "transfer")?;
        Ok(materialized)
    }

    async fn reserve_export_attempt(&self, operation: &ExportOperation) -> Result<ExportAttempt> {
        let dataset = &operation.request.dataset;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.check_export_authority(&view, operation).await?;
        self.check_export_channel(&view, &operation.request, operation.source_revision)
            .await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let revision = rows
            .revision(&format!("export-operation/{}", operation.request.operation))
            .ok_or_else(invalid_export)?;
        let key = format!("export-attempt/{}/{revision}", operation.request.operation);
        let _attempt_memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&key])?;
        if let Some((_, _, saved)) = rows.get_record::<ExportAttempt>(&key)? {
            check_attempt(
                &key,
                &saved,
                operation,
                &rows,
                Some(&self.inner.lifecycle.budget),
            )?;
            self.check_export_host(&saved, dataset)?;
            self.inner
                .objects
                .validate_retained_path(&saved.destination)?;
            return Ok(saved);
        }
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let retained = self
            .inner
            .retained
            .child(dataset.as_str())?
            .child(id.as_str())?;
        let host = self
            .inner
            .root
            .join("objects")
            .join(dataset.as_str())
            .join(id.as_str());
        let saved = ExportAttempt {
            id: id.clone(),
            operation: operation.request.operation.clone(),
            operation_revision: revision,
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            epoch: operation.epoch,
            host_prefix: format!("{}/", host.display()),
            retained_prefix: retained.protection_prefix()?,
            destination: retained.child("export.parquet")?.as_string()?,
        };
        self.inner
            .objects
            .validate_retained_path(&saved.destination)?;
        let protection = Protection {
            id: id.clone(),
            consumer: dataset.clone(),
            purpose: format!("{}/export", saved.operation),
            paths: vec![saved.host_prefix.clone(), saved.retained_prefix.clone()],
            retired: false,
        };
        if let Err(error) = self
            .inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record(format!("protection/{id}"), protection)?,
                    record(&key, &saved)?,
                ],
            )
            .await
        {
            // A lost catalog response can already have saved the exact intent.
            let after = self.inner.lifecycle.open(dataset).await?;
            self.check_export_authority(&after, operation).await?;
            self.check_export_channel(&after, &operation.request, operation.source_revision)
                .await?;
            let rows = self.inner.lifecycle.rows(&after).await?;
            let _after_memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&key])?;
            if let Some((_, _, recovered)) = rows.get_record::<ExportAttempt>(&key)? {
                check_attempt(
                    &key,
                    &recovered,
                    operation,
                    &rows,
                    Some(&self.inner.lifecycle.budget),
                )?;
                self.check_export_host(&recovered, dataset)?;
                self.inner
                    .objects
                    .validate_retained_path(&recovered.destination)?;
                return Ok(recovered);
            }
            return Err(error);
        }
        Ok(saved)
    }

    fn check_export_host(&self, attempt: &ExportAttempt, dataset: &Id) -> Result<()> {
        let expected = self
            .inner
            .root
            .join("objects")
            .join(dataset.as_str())
            .join(attempt.id.as_str());
        if attempt.host_prefix != format!("{}/", expected.display()) {
            return Err(Error::refusal(
                "export_host_unavailable",
                "unfinished export staging belongs to another exclusive host",
            ));
        }
        Ok(())
    }
}

fn check_attempt(
    key: &str,
    attempt: &ExportAttempt,
    operation: &ExportOperation,
    rows: &SnapshotRows,
    budget: Option<&Budget>,
) -> Result<()> {
    let protection_key = format!("protection/{}", attempt.id);
    let _memory = budget
        .map(|budget| rows.reserve_decoded(budget, &[&protection_key]))
        .transpose()?;
    let attempt_revision = rows.revision(key).ok_or_else(invalid_export)?;
    let protection: Protection = rows
        .get_record(&protection_key)?
        .ok_or_else(invalid_export)?
        .2;
    let host = attempt
        .host_prefix
        .strip_suffix('/')
        .ok_or_else(invalid_export)?;
    let retained = attempt
        .retained_prefix
        .strip_suffix('/')
        .ok_or_else(invalid_export)?;
    if key
        != format!(
            "export-attempt/{}/{}",
            attempt.operation, attempt.operation_revision
        )
        || attempt.operation_revision <= 0
        || attempt_revision <= attempt.operation_revision
        || rows.revision(&protection_key) != Some(attempt_revision)
        || rows.revision(&format!("export-operation/{}", attempt.operation))
            != Some(attempt.operation_revision)
        || attempt.operation != operation.request.operation
        || attempt.request_digest != operation.request_digest
        || attempt.eligibility != operation.eligibility
        || attempt.epoch != operation.epoch
        || !Path::new(host).is_absolute()
        || !storage::valid_object_locator(host)
        || !host.ends_with(&format!("/{}/{}", operation.request.dataset, attempt.id))
        || !retained.starts_with("s3://")
        || !storage::valid_object_locator(retained)
        || !retained.ends_with(&format!("/{}/{}", operation.request.dataset, attempt.id))
        || attempt.destination != format!("{}export.parquet", attempt.retained_prefix)
        || protection.id != attempt.id
        || protection.consumer != operation.request.dataset
        || protection.purpose != format!("{}/export", attempt.operation)
        || protection.retired
        || protection.paths != [attempt.host_prefix.clone(), attempt.retained_prefix.clone()]
    {
        return Err(invalid_export());
    }
    Ok(())
}

fn check_staged(
    attempt: &ExportAttempt,
    staged: &ExportMaterialized,
    operation: &ExportOperation,
) -> Result<()> {
    check_materialized(staged, operation)?;
    let name = staged
        .object
        .path
        .strip_prefix(&attempt.host_prefix)
        .ok_or_else(invalid_export)?;
    let token = name
        .strip_prefix("stage-")
        .and_then(|name| name.strip_suffix(".parquet"))
        .ok_or_else(invalid_export)?;
    if !storage::valid_object_locator(&staged.object.path) || uuid::Uuid::parse_str(token).is_err()
    {
        return Err(invalid_export());
    }
    Ok(())
}

fn check_staged_validation(
    staged: &ExportMaterialized,
    validation: &ExportValidation,
) -> Result<()> {
    if validation.object != staged.object
        || validation.rows != staged.rows
        || (validation.sampled_rows == 0) != (staged.rows == 0)
    {
        return Err(invalid_export());
    }
    Ok(())
}

pub(in crate::engine) fn validate_record(
    key: &str,
    json: &str,
    rows: &SnapshotRows,
    budget: Option<&Budget>,
) -> Result<Option<ObjectRef>> {
    let (family, suffix) = key.split_once('/').ok_or_else(invalid_export)?;
    let (id, revision) = suffix.split_once('/').ok_or_else(invalid_export)?;
    let operation_key = format!("export-operation/{id}");
    let attempt_key = format!("export-attempt/{suffix}");
    let staged_key = format!("export-staged/{suffix}");
    let _memory = budget
        .map(|budget| {
            rows.reserve_decoded(budget, &[key, &operation_key, &attempt_key, &staged_key])
        })
        .transpose()?;
    let operation: ExportOperation = rows
        .get_record(&operation_key)?
        .ok_or_else(invalid_export)?
        .2;
    validate_operation_record(&operation)?;
    let attempt: ExportAttempt = if family == "export-attempt" {
        serde_json::from_str(json)?
    } else {
        rows.get_record(&attempt_key)?.ok_or_else(invalid_export)?.2
    };
    if attempt.operation_revision.to_string() != revision {
        return Err(invalid_export());
    }
    check_attempt(&attempt_key, &attempt, &operation, rows, budget)?;
    if family == "export-attempt" {
        return Ok(None);
    }
    let staged: ExportMaterialized = if family == "export-staged" {
        serde_json::from_str(json)?
    } else {
        rows.get_record(&staged_key)?.ok_or_else(invalid_export)?.2
    };
    check_staged(&attempt, &staged, &operation)?;
    check_stage_order(&attempt_key, &staged_key, rows)?;
    if family == "export-stage-validation" {
        let validation: ExportValidation = serde_json::from_str(json)?;
        check_staged_validation(&staged, &validation)?;
        if rows.revision(key).ok_or_else(invalid_export)?
            <= rows.revision(&staged_key).ok_or_else(invalid_export)?
        {
            return Err(invalid_export());
        }
    } else if family != "export-staged" {
        return Err(invalid_export());
    }
    if let Some(completed) = completed_stage(rows, &operation.request.operation, budget)? {
        if completed.object != staged.object {
            return Err(invalid_export());
        }
        // The closed bytes are now durable remotely. Original stage metadata
        // remains creation proof; it no longer promises host payload availability.
        return Ok(None);
    }
    Ok(Some(staged.object))
}

// Remote creation proof derives only from the original protected destination and
// the exact closed, checked stage. Host files need not remain available to read it.
pub(in crate::engine) fn check_completed(
    operation: &ExportOperation,
    materialized: &ExportMaterialized,
    validation: &ExportValidation,
    rows: &SnapshotRows,
    budget: Option<&Budget>,
) -> Result<CompletedStage> {
    let revision = rows
        .revision(&format!("export-operation/{}", operation.request.operation))
        .ok_or_else(invalid_export)?;
    let suffix = format!("{}/{revision}", operation.request.operation);
    let attempt_key = format!("export-attempt/{suffix}");
    let staged_key = format!("export-staged/{suffix}");
    let staged_validation_key = format!("export-stage-validation/{suffix}");
    let _memory = budget
        .map(|budget| {
            rows.reserve_decoded(budget, &[&attempt_key, &staged_key, &staged_validation_key])
        })
        .transpose()?;
    let attempt: ExportAttempt = rows.get_record(&attempt_key)?.ok_or_else(invalid_export)?.2;
    check_attempt(&attempt_key, &attempt, operation, rows, budget)?;
    let staged: ExportMaterialized = rows
        .get_record(&format!("export-staged/{suffix}"))?
        .ok_or_else(invalid_export)?
        .2;
    let staged_validation: ExportValidation = rows
        .get_record(&format!("export-stage-validation/{suffix}"))?
        .ok_or_else(invalid_export)?
        .2;
    check_staged(&attempt, &staged, operation)?;
    check_staged_validation(&staged, &staged_validation)?;
    check_stage_order(&attempt_key, &staged_key, rows)?;
    let stage_revision = rows.revision(&staged_key).ok_or_else(invalid_export)?;
    let stage_validation_revision = rows
        .revision(&staged_validation_key)
        .ok_or_else(invalid_export)?;
    let materialized_revision = rows
        .revision(&format!(
            "export-materialized/{}",
            operation.request.operation
        ))
        .ok_or_else(invalid_export)?;
    if stage_validation_revision <= stage_revision
        || materialized_revision <= stage_validation_revision
        || rows.revision(&format!(
            "export-validation/{}",
            operation.request.operation
        )) != Some(materialized_revision)
    {
        return Err(invalid_export());
    }
    let completed = CompletedStage {
        object: staged.object.clone(),
        remote: materialized.object.clone(),
        attempt: attempt.id.clone(),
        operation_revision: revision,
        stage_revision,
        completion_revision: materialized_revision,
        host_prefix: attempt.host_prefix.clone(),
    };
    let expected = ObjectRef {
        path: attempt.destination,
        ..staged.object.clone()
    };
    if materialized
        != &(ExportMaterialized {
            object: expected.clone(),
            ..staged
        })
        || validation
            != &(ExportValidation {
                object: expected,
                ..staged_validation
            })
    {
        return Err(invalid_export());
    }
    Ok(completed)
}

pub(in crate::engine) fn completed_stage(
    rows: &SnapshotRows,
    operation: &Id,
    budget: Option<&Budget>,
) -> Result<Option<CompletedStage>> {
    let operation_key = format!("export-operation/{operation}");
    let materialized_key = format!("export-materialized/{operation}");
    let validation_key = format!("export-validation/{operation}");
    let _memory = budget
        .map(|budget| {
            rows.reserve_decoded(
                budget,
                &[&operation_key, &materialized_key, &validation_key],
            )
        })
        .transpose()?;
    let output = rows.get_record::<ExportMaterialized>(&materialized_key)?;
    if output
        .as_ref()
        .is_some_and(|(_, _, output)| !output.object.path.starts_with("s3://"))
    {
        return Ok(None);
    }
    let validation = rows.get_record::<ExportValidation>(&validation_key)?;
    let (Some((_, _, output)), Some((_, _, validation))) = (output, validation) else {
        if rows.contains_key(&materialized_key) || rows.contains_key(&validation_key) {
            return Err(invalid_export());
        }
        return Ok(None);
    };
    if !output.object.path.starts_with("s3://") {
        return Ok(None);
    }
    let (_, _, operation): (_, _, ExportOperation) = rows
        .get_record(&operation_key)?
        .ok_or_else(invalid_export)?;
    validate_operation_record(&operation)?;
    check_completed(&operation, &output, &validation, rows, budget).map(Some)
}

/// Ends only a completed export's host prefix, preserving remote protection.
pub(in crate::engine) fn completed_host_prefix(
    protection: &Protection,
    rows: &SnapshotRows,
) -> Result<Option<String>> {
    let Some(operation) = protection.purpose.strip_suffix("/export") else {
        return Ok(None);
    };
    let operation = Id::new(operation)?;
    let Some(stage) = completed_stage(rows, &operation, None)? else {
        return Ok(None);
    };
    if stage.attempt == protection.id {
        Ok(Some(stage.host_prefix))
    } else {
        Ok(None)
    }
}

pub(in crate::engine) fn check_pin_binding(
    pin: &ExportPin,
    operation: &ExportOperation,
    rows: &SnapshotRows,
    budget: Option<&Budget>,
) -> Result<()> {
    let materialized_key = format!("export-materialized/{}", pin.id);
    let validation_key = format!("export-validation/{}", pin.id);
    let _memory = budget
        .map(|budget| rows.reserve_decoded(budget, &[&materialized_key, &validation_key]))
        .transpose()?;
    let materialized: ExportMaterialized = rows
        .get_record(&format!("export-materialized/{}", pin.id))?
        .ok_or_else(invalid_export)?
        .2;
    let validation: ExportValidation = rows
        .get_record(&format!("export-validation/{}", pin.id))?
        .ok_or_else(invalid_export)?
        .2;
    check_completed(operation, &materialized, &validation, rows, budget)?;
    if pin.object != materialized.object
        || pin.rows != materialized.rows
        || pin.fields != materialized.fields
        || pin.validation != validation
    {
        return Err(invalid_export());
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn test_retained_phase_revision_bindings(
    engine: &Engine,
    spec: &ExportSpec,
    pin: &ExportPin,
) {
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    let (revision, _, operation) = rows
        .get_record::<ExportOperation>(&format!("export-operation/{}", spec.operation))
        .unwrap()
        .unwrap();
    let id = Id::new("binding-attempt").unwrap();
    let attempt = ExportAttempt {
        id: id.clone(),
        operation: spec.operation.clone(),
        operation_revision: revision,
        request_digest: operation.request_digest.clone(),
        eligibility: operation.eligibility,
        epoch: operation.epoch,
        host_prefix: format!(
            "{}/objects/{}/{id}/",
            engine.inner.root.display(),
            spec.dataset
        ),
        retained_prefix: format!("s3://bucket/owned/{}/{id}/", spec.dataset),
        destination: format!("s3://bucket/owned/{}/{id}/export.parquet", spec.dataset),
    };
    let protection = Protection {
        id: id.clone(),
        consumer: spec.dataset.clone(),
        purpose: format!("{}/export", spec.operation),
        paths: vec![attempt.host_prefix.clone(), attempt.retained_prefix.clone()],
        retired: false,
    };
    let suffix = format!("{}/{revision}", spec.operation);
    let attempt_key = format!("export-attempt/{suffix}");
    let staged_key = format!("export-staged/{suffix}");
    let stage_validation_key = format!("export-stage-validation/{suffix}");
    lifecycle
        .commit(
            &view,
            vec![
                record(&attempt_key, &attempt).unwrap(),
                record(format!("protection/{id}"), &protection).unwrap(),
            ],
        )
        .await
        .unwrap();
    let stage_object = ObjectRef {
        path: format!(
            "{}stage-{}.parquet",
            attempt.host_prefix,
            uuid::Uuid::new_v4()
        ),
        ..pin.object.clone()
    };
    let staged = ExportMaterialized {
        request_digest: operation.request_digest.clone(),
        object: stage_object.clone(),
        rows: pin.rows,
        fields: pin.fields.clone(),
    };
    let stage_validation = ExportValidation {
        object: stage_object,
        ..pin.validation.clone()
    };
    let remote_object = ObjectRef {
        path: attempt.destination.clone(),
        ..pin.object.clone()
    };
    let output = ExportMaterialized {
        object: remote_object.clone(),
        ..staged.clone()
    };
    let validation = ExportValidation {
        object: remote_object,
        ..stage_validation.clone()
    };
    let materialized_key = format!("export-materialized/{}", spec.operation);
    let validation_key = format!("export-validation/{}", spec.operation);
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(&view, vec![record(&staged_key, &staged).unwrap()])
        .await
        .unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(
            &view,
            vec![record(&stage_validation_key, &stage_validation).unwrap()],
        )
        .await
        .unwrap();
    // Matching values alone cannot establish an atomic transfer completion.
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(&view, vec![record(&materialized_key, &output).unwrap()])
        .await
        .unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    lifecycle
        .commit(&view, vec![record(&validation_key, &validation).unwrap()])
        .await
        .unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    assert_eq!(
        check_completed(
            &operation,
            &output,
            &validation,
            &rows,
            Some(&lifecycle.budget)
        )
        .unwrap_err()
        .code(),
        "invalid_export"
    );
    lifecycle
        .commit(
            &view,
            vec![
                record(&materialized_key, &output).unwrap(),
                record(&validation_key, &validation).unwrap(),
            ],
        )
        .await
        .unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    check_completed(
        &operation,
        &output,
        &validation,
        &rows,
        Some(&lifecycle.budget),
    )
    .unwrap();
    // A later identical protection record cannot replace original admission proof.
    lifecycle
        .commit(
            &view,
            vec![record(format!("protection/{id}"), &protection).unwrap()],
        )
        .await
        .unwrap();
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let rows = lifecycle.rows(&view).await.unwrap();
    assert_eq!(
        check_completed(
            &operation,
            &output,
            &validation,
            &rows,
            Some(&lifecycle.budget)
        )
        .unwrap_err()
        .code(),
        "invalid_export"
    );
}

fn check_stage_order(attempt_key: &str, staged_key: &str, rows: &SnapshotRows) -> Result<()> {
    if rows.revision(staged_key).ok_or_else(invalid_export)?
        <= rows.revision(attempt_key).ok_or_else(invalid_export)?
    {
        return Err(invalid_export());
    }
    Ok(())
}

#[cfg(all(test, feature = "remote", feature = "test-util", unix))]
mod stage_removal_tests;
