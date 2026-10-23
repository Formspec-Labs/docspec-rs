//! Same-destination release creation and bounded independent readback.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseAttempt {
    attempt: Id,
    operation: Id,
    operation_revision: i64,
    request_digest: String,
    eligibility: u64,
    prefix: String,
    manifest: ObjectRef,
    consumer: Id,
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_release_attempt",
        "release intent differs from its protected operation generation",
    )
}
fn key(operation: &Id, revision: i64, material: &str) -> String {
    format!("release-attempt/{operation}/{revision}/{material}/release")
}

pub(in crate::engine) fn validate_record(
    key: &str,
    json: &str,
    rows: &crate::catalog::SnapshotRows,
    budget: Option<&Budget>,
) -> Result<()> {
    let saved: ReleaseAttempt = serde_json::from_str(json)?;
    validate(key, &saved, rows, budget)
}
fn validate(
    key_value: &str,
    saved: &ReleaseAttempt,
    rows: &crate::catalog::SnapshotRows,
    budget: Option<&Budget>,
) -> Result<()> {
    // Inventory already admits its complete decoded snapshot; focused
    // recovery and creation callers admit these referenced records separately.
    let _memory = budget
        .map(|budget| {
            rows.reserve_decoded(
                budget,
                &[
                    &format!("operation/{}", saved.operation),
                    &format!("request/{}", saved.operation),
                    &format!("protection/{}", saved.attempt),
                ],
            )
        })
        .transpose()?;
    let (revision, _, operation) = rows
        .get_record::<Operation>(&format!("operation/{}", saved.operation))?
        .ok_or_else(invalid)?;
    let (request_revision, _, request) = rows
        .get_record::<OperationRequest>(&format!("request/{}", saved.operation))?
        .ok_or_else(invalid)?;
    let intent_revision = rows.revision(key_value).ok_or_else(invalid)?;
    let (protection_revision, _, protection) = rows
        .get_record::<Protection>(&format!("protection/{}", saved.attempt))?
        .ok_or_else(invalid)?;
    if key_value
        != key(
            &saved.operation,
            saved.operation_revision,
            &saved.manifest.digest,
        )
        || revision != saved.operation_revision
        || request_revision != revision
        || intent_revision <= revision
        || saved.operation != *request.operation()
        || saved.consumer != *request.dataset()
        || saved.manifest.owner != saved.consumer
        || saved.request_digest != operation.request_digest
        || saved.request_digest != fingerprint(&request)?
        || saved.eligibility != operation.eligibility
        || protection_revision != intent_revision
        || protection.id != saved.attempt
        || protection.consumer != saved.consumer
        || protection.paths != [saved.prefix.clone()]
        || protection.purpose != format!("{}/release", saved.operation)
        || protection.retired
        || saved.manifest.path != format!("{}release.json", saved.prefix)
        || saved.manifest.bytes == 0
        || saved.manifest.digest.len() != 64
        || !saved.manifest.digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid());
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine) async fn create_release(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        dataset: &DatasetPin,
        stop: Option<&str>,
    ) -> Result<ObjectRef> {
        if dataset.dataset != *spec.dataset() || dataset.version != *spec.operation() {
            return Err(invalid());
        }
        let budget = &self.inner.lifecycle.budget;
        let size = inputs::metadata_size(dataset, budget.limits.max_metadata_bytes)?;
        let memory = Arc::new(
            budget.reserve(
                size.saturating_mul(4)
                    .saturating_add(self.inner.objects.metadata_read_allowance()),
            )?,
        );
        let (saved, mut bytes) = self
            .reserve_release(spec, operation, dataset, &memory)
            .await?;
        // An incomplete intent resolves its exact object before serialization or
        // creation. Absence means only the first native open/HEAD was NotFound.
        if bytes.is_none() {
            if let Some(decoded) = self.read_release_manifest(&saved.manifest, &memory).await? {
                if decoded != *dataset {
                    return Err(Error::refusal(
                        "release_mismatch",
                        "retained release differs from prepared result",
                    ));
                }
                return Ok(saved.manifest);
            }
            let retained = dataset.clone();
            let expected = saved.manifest.clone();
            let worker_memory = memory.clone();
            let limit = budget.limits.max_metadata_bytes;
            bytes = Some(
                budget
                    .blocking(move || {
                        let _memory = worker_memory;
                        let bytes = storage::bounded_json(&retained, limit)?;
                        if bytes.len() as u64 != expected.bytes
                            || blake3::hash(&bytes).to_hex().as_str() != expected.digest
                        {
                            return Err(invalid());
                        }
                        Ok(bytes)
                    })
                    .await?,
            );
        }
        let objects = self.inner.objects.clone();
        let manifest = saved.manifest.clone();
        let worker_memory = memory.clone();
        let handle = tokio::runtime::Handle::current();
        let outcome = budget
            .blocking(move || {
                let _memory = worker_memory;
                objects.create_release_metadata_admitted(
                    &manifest,
                    bytes.ok_or_else(invalid)?,
                    &handle,
                )
            })
            .await;
        if let Err(error) = &outcome {
            if !matches!(
                error.code(),
                "remote_conflict" | "remote_unconfirmed" | "metadata_conflict" | "io"
            ) {
                return outcome.map(|_| saved.manifest);
            }
        } else {
            stop_after(stop, "release-create")?;
            #[cfg(all(not(test), not(feature = "test-util")))]
            return Ok(saved.manifest);
        }
        // One buffer handles ordinary independent readback and uncertain-write
        // reconciliation. Never verify with one GET and decode with another.
        let decoded = self.read_release_manifest(&saved.manifest, &memory).await?;
        let Some(decoded) = decoded else {
            return match outcome {
                Err(error) => Err(error),
                Ok(()) => Err(Error::refusal(
                    "unavailable_release",
                    "acknowledged release is absent",
                )),
            };
        };
        if decoded != *dataset {
            return Err(Error::refusal(
                "release_mismatch",
                "serialized release differs from prepared result",
            ));
        }
        Ok(saved.manifest)
    }

    async fn read_release_manifest(
        &self,
        manifest: &ObjectRef,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Option<DatasetPin>> {
        #[cfg(test)]
        self.inner
            .release_decodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        inputs::read_manifest_if_present(
            &self.inner.objects,
            manifest,
            &self.inner.lifecycle.budget,
            &mut 0,
            memory,
        )
        .await
    }

    async fn reserve_release(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        dataset: &DatasetPin,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(ReleaseAttempt, Option<Vec<u8>>)> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(spec.dataset()).await?;
        let rows = lifecycle.rows(&view).await?;
        let revision = rows
            .revision(&format!("operation/{}", spec.operation()))
            .ok_or_else(invalid)?;
        let material = storage::json_digest(dataset)?.to_hex().to_string();
        let key_value = key(spec.operation(), revision, &material);
        let _decoded = rows.reserve_decoded(&lifecycle.budget, &[&key_value])?;
        let check_saved =
            |saved: &ReleaseAttempt, rows: &crate::catalog::SnapshotRows| -> Result<()> {
                validate(&key_value, saved, rows, Some(&lifecycle.budget))?;
                let directory = self
                    .inner
                    .retained
                    .child(spec.dataset().as_str())?
                    .child(saved.attempt.as_str())?;
                if directory.protection_prefix()? != saved.prefix {
                    return Err(invalid());
                }
                self.inner
                    .objects
                    .validate_retained_path(&saved.manifest.path)
            };
        if let Some((_, _, saved)) = rows.get_record::<ReleaseAttempt>(&key_value)? {
            check_saved(&saved, &rows)?;
            self.check(&view, operation, spec.operation()).await?;
            return Ok((saved, None));
        }
        let attempt = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .retained
            .child(spec.dataset().as_str())?
            .child(attempt.as_str())?;
        let path = directory.child("release.json")?.as_string()?;
        self.inner.objects.validate_retained_path(&path)?;
        let retained = dataset.clone();
        let owner = spec.dataset().clone();
        let worker_memory = memory.clone();
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let (manifest, bytes) = lifecycle
            .budget
            .blocking(move || {
                let _memory = worker_memory;
                let bytes = storage::bounded_json(&retained, limit)?;
                let manifest = ObjectRef {
                    owner,
                    path,
                    digest: blake3::hash(&bytes).to_hex().to_string(),
                    bytes: bytes.len() as u64,
                };
                Ok((manifest, bytes))
            })
            .await?;
        if manifest.digest != material {
            return Err(invalid());
        }
        let pending = ReleaseAttempt {
            attempt: attempt.clone(),
            operation: spec.operation().clone(),
            operation_revision: revision,
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            prefix: directory.protection_prefix()?,
            manifest,
            consumer: spec.dataset().clone(),
        };
        let protection = Protection {
            id: attempt.clone(),
            consumer: spec.dataset().clone(),
            purpose: format!("{}/release", spec.operation()),
            paths: vec![pending.prefix.clone()],
            retired: false,
        };
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let fresh = lifecycle.open(spec.dataset()).await?;
        self.check(&fresh, operation, spec.operation()).await?;
        let rows = lifecycle.rows(&fresh).await?;
        let _fresh_decoded = rows.reserve_decoded(&lifecycle.budget, &[&key_value])?;
        if rows.revision(&format!("operation/{}", spec.operation())) != Some(revision) {
            return Err(invalid());
        }
        if let Some((_, _, saved)) = rows.get_record::<ReleaseAttempt>(&key_value)? {
            check_saved(&saved, &rows)?;
            return Ok((saved, None));
        }
        if let Err(error) = lifecycle
            .commit(
                &fresh,
                vec![
                    record(format!("protection/{attempt}"), protection)?,
                    record(&key_value, &pending)?,
                ],
            )
            .await
        {
            let after = lifecycle.open(spec.dataset()).await?;
            self.check(&after, operation, spec.operation()).await?;
            let rows = lifecycle.rows(&after).await?;
            let _after_decoded = rows.reserve_decoded(&lifecycle.budget, &[&key_value])?;
            if let Some((_, _, saved)) = rows.get_record::<ReleaseAttempt>(&key_value)? {
                check_saved(&saved, &rows)?;
                return Ok((saved, None));
            }
            return Err(error);
        }
        Ok((pending, Some(bytes)))
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests;
