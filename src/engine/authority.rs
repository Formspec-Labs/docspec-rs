//! Explicit fencing of previously admitted operations without changing their data.
// Rust guideline compliant 2026-02-21

use super::*;

impl Engine {
    /// Fences older operations while preserving the selected release.
    ///
    /// The expected epoch identifies one monotone transition. Retrying that
    /// transition returns its completed target without another commit. A later
    /// transition refuses the stale request. This fences publication; it does
    /// not revoke credentials, stop readers, or establish global quiescence.
    ///
    /// # Errors
    /// Refuses missing authority, a different epoch, overflow, or catalog errors.
    /// A native conflict or uncertain response can be reconciled with the same
    /// expected epoch; never silently replace it with the current epoch.
    pub async fn fence_operations(&self, dataset: &Id, expected_epoch: u64) -> Result<u64> {
        let next = expected_epoch
            .checked_add(1)
            .ok_or_else(|| Error::refusal("revision_exhausted", "authority epoch overflow"))?;
        let memory = self.request_memory(&(dataset, expected_epoch))?;
        let dataset = dataset.clone();
        self.admit_work(memory, move |engine| async move {
            let lock = engine.lock(&dataset).await;
            let _guard = lock.lock().await;
            // A fencing request cannot create a new dataset as a side effect.
            let view = engine.existing_lifecycle_view(&dataset).await?;
            let mut authority: Authority = engine
                .inner
                .lifecycle
                .get(&view, "authority")
                .await?
                .ok_or_else(|| Error::refusal("no_release", "dataset authority is absent"))?;
            if authority.epoch == next {
                return Ok(next);
            }
            if authority.epoch != expected_epoch {
                return Err(Error::refusal(
                    "stale_authority",
                    "authority epoch differs from the requested transition",
                ));
            }
            authority.epoch = next;
            engine
                .inner
                .lifecycle
                .commit(&view, vec![record("authority", &authority)?])
                .await?;
            Ok(next)
        })
        .await
    }
}
