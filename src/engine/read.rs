//! Verified native field-group reads through the execution host's shared budget.

use super::*;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};

impl Engine {
    /// Streams one retained field group from an exact release.
    ///
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses unknown groups, invalid retained releases, or exhausted reader capacity.
    pub async fn read_group(
        &self,
        release: &ReleasePin,
        group: &str,
    ) -> Result<SendableRecordBatchStream> {
        if group != "source" {
            self.require_local()?;
        }
        let memory = self.request_memory(&(release, group))?;
        let release = release.clone();
        let group = group.to_owned();
        self.admit_stream(memory, move |engine| async move {
            let view = engine.check_read_release(&release).await?;
            let canonical = release.dataset.groups.get(&group).ok_or_else(|| {
                Error::refusal("missing_group", "release lacks the requested field group")
            })?;
            let pin = engine
                .representation_pin(&view, &release.id, &group, canonical)
                .await?;
            read_pin_with_objects(&engine.inner.objects, &pin, &engine.inner.lifecycle.budget).await
        })
        .await
    }

    /// Streams the exact contributor occurrences retained by a related result.
    ///
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses absent evidence, invalid retained releases, or exhausted reader capacity.
    pub async fn read_evidence(&self, release: &ReleasePin) -> Result<SendableRecordBatchStream> {
        self.require_local()?;
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            engine.check_read_release(&release).await?;
            let related = release.dataset.related.as_ref().ok_or_else(|| {
                Error::refusal("missing_evidence", "release has no contributor relation")
            })?;
            read_pin(&related.evidence, &engine.inner.lifecycle.budget).await
        })
        .await
    }

    pub(super) async fn check_read_release(&self, release: &ReleasePin) -> Result<View> {
        if !release.verified || release.id != release.dataset.version {
            return Err(Error::refusal(
                "release_mismatch",
                "read pin is not a completed immutable selection",
            ));
        }
        let lock = self.lock(&release.dataset.dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = self
            .existing_lifecycle_view(&release.dataset.dataset)
            .await?;
        self.check_operation_available(&view, &release.id).await?;
        let rows = lifecycle.rows(&view).await?;
        // Completed readbacks are independently retained even if a later failure
        // or abandonment prevented selection. Backfill uses its manifest digest
        // in the readback key, so all lookups remain exact rather than history scans.
        let keys = [
            format!("result/{}", release.id),
            format!("readback/{}", release.id),
            format!(
                "backfill-readback/{}/{}",
                release.id, release.manifest.digest
            ),
        ];
        for key in keys {
            let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
            if let Some((revision, _, retained)) = rows.get_record::<ReleasePin>(&key)?
                && revision > 0
                && revision <= view.revision
                && retained == *release
            {
                return Ok(view);
            }
        }
        Err(Error::refusal(
            "release_mismatch",
            "read pin differs from this host's available result or completed readback",
        ))
    }

    pub(crate) fn read_budget(&self) -> Budget {
        self.inner.lifecycle.budget.clone()
    }
}

/// Streams a retained field group without requiring its producing catalog.
///
/// The injected budget shares admission with other readers and application work.
/// The caller must retain the files or rely on a published availability window.
/// A serialized pin identifies content; it creates no lease or host protection.
///
/// # Errors
/// Refuses unknown groups, invalid retained releases, or exhausted reader capacity.
pub async fn read_group(
    release: &ReleasePin,
    group: &str,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let pin = release.dataset.groups.get(group).ok_or_else(|| {
        Error::refusal("missing_group", "release lacks the requested field group")
    })?;
    resolve_release(release, budget).await?;
    read_pin(pin, budget).await
}

/// Streams retained contributor occurrences without the producing catalog.
///
/// The caller must retain the files or rely on a published availability window.
/// A serialized pin identifies content; it creates no lease or host protection.
///
/// # Errors
/// Refuses absent evidence, invalid retained releases, or exhausted reader capacity.
pub async fn read_evidence(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let related =
        release.dataset.related.as_ref().ok_or_else(|| {
            Error::refusal("missing_evidence", "release has no contributor relation")
        })?;
    resolve_release(release, budget).await?;
    read_pin(&related.evidence, budget).await
}

pub(super) async fn read_pin(pin: &TablePin, budget: &Budget) -> Result<SendableRecordBatchStream> {
    read_pin_with_objects(&storage::Objects::Local, pin, budget).await
}

pub(super) async fn read_pin_with_objects(
    objects: &storage::Objects,
    pin: &TablePin,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let table = objects.open_pin(pin, budget).await?;
    let schema = storage::identity::public_schema(&table)?;
    let stream = storage::read_batches(&table, budget)
        .await?
        .and_then(|batch| async move { storage::identity::public_batch(batch) })
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

/// Streams a verified physical representation without requiring its producer.
/// The caller retains its immutable files for the lifetime of the stream.
///
/// # Errors
/// Refuses unverified representations, unreadable native files, or capacity errors.
pub async fn read_compaction(
    result: &CompactionResult,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    if !result.verified {
        return Err(Error::refusal(
            "invalid_compaction",
            "representation is not verified",
        ));
    }
    read_pin(&result.replacement, budget).await
}
