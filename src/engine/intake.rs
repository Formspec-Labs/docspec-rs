//! Bounded local source admission fencing for continuous field-group cutover.

use super::*;

// Cutover may stop new admissions for at most five minutes per generation.
// Retries reuse that generation without extending its deadline.
const MAX_FENCE_MILLIS: u64 = 300_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct SourceFence {
    pub(in crate::engine) dataset: Id,
    pub(in crate::engine) owner: Id,
    pub(in crate::engine) token: String,
    pub(in crate::engine) host: String,
    pub(in crate::engine) expires_millis: u64,
    pub(in crate::engine) closed: bool,
}

pub(in crate::engine) fn validate_record(fence: &SourceFence) -> Result<()> {
    for identity in [&fence.token, &fence.host] {
        let uuid = uuid::Uuid::parse_str(identity)
            .map_err(|_| invalid_fence("source fence identity is not a UUID"))?;
        if uuid.is_nil() || uuid.to_string() != *identity {
            return Err(invalid_fence("source fence UUID is not canonical"));
        }
    }
    if fence.expires_millis == 0 {
        return Err(invalid_fence("source fence has no monotonic deadline"));
    }
    Ok(())
}

impl SourceFence {
    fn live(&self, host: &str, now: u64) -> Result<bool> {
        validate_record(self)?;
        if self.host != host || self.closed || now >= self.expires_millis {
            return Ok(false);
        }
        if self.expires_millis - now > MAX_FENCE_MILLIS {
            return Err(invalid_fence("source fence exceeds the bounded deadline"));
        }
        Ok(true)
    }

    fn exact_live(&self, expected: &Self, host: &str, now: u64) -> Result<()> {
        validate_record(expected)?;
        if self != expected || !self.live(host, now)? {
            return Err(Error::refusal(
                "source_fence_lost",
                "source fence changed or expired; retry continuous cutover",
            ));
        }
        Ok(())
    }
}

impl Engine {
    async fn source_fence(&self, view: &View) -> Result<Option<SourceFence>> {
        let fence: Option<SourceFence> = self.inner.lifecycle.get(view, "source-fence").await?;
        if let Some(fence) = &fence {
            validate_record(fence)?;
            if &crate::catalog::lifecycle_ident(&fence.dataset) != view.table.identifier() {
                return Err(invalid_fence("source fence dataset differs"));
            }
        }
        Ok(fence)
    }

    pub(in crate::engine) async fn check_source_intake(&self, view: &View) -> Result<()> {
        if let Some(fence) = self.source_fence(view).await?
            && fence.live(
                &self.inner.host.instance,
                self.inner.host.monotonic_millis()?,
            )?
        {
            return Err(Error::refusal(
                "source_fenced",
                "continuous cutover temporarily blocks new source admissions; retry after the fence closes or expires",
            ));
        }
        Ok(())
    }

    pub(in crate::engine) async fn acquire_continuous_fence(
        &self,
        intent: &crate::engine::backfill::continuous::Intent,
    ) -> Result<SourceFence> {
        self.acquire_source_fence_guarded(
            &intent.spec.backfill.dataset,
            &intent.spec.backfill.operation,
            intent.spec.fence_millis,
            Some(intent),
        )
        .await
    }

    #[cfg(test)]
    pub(in crate::engine) async fn acquire_source_fence(
        &self,
        dataset: &Id,
        owner: &Id,
        millis: u64,
    ) -> Result<SourceFence> {
        self.acquire_source_fence_guarded(dataset, owner, millis, None)
            .await
    }

    async fn acquire_source_fence_guarded(
        &self,
        dataset: &Id,
        owner: &Id,
        millis: u64,
        intent: Option<&crate::engine::backfill::continuous::Intent>,
    ) -> Result<SourceFence> {
        if !self.inner.owned_catalog {
            return Err(Error::refusal(
                "source_fence_catalog_unqualified",
                "source fencing requires the owned embedded catalog",
            ));
        }
        if millis == 0 || millis > MAX_FENCE_MILLIS {
            return Err(Error::refusal(
                "invalid_source_fence_budget",
                "source fence duration must be between 1 and 300000 milliseconds",
            ));
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        if let Some(intent) = intent {
            // Parent termination and fence acquisition share this lock and view.
            self.check_continuous_fence_owner(&view, intent).await?;
        }
        let now = self.inner.host.monotonic_millis()?;
        if let Some(fence) = self.source_fence(&view).await?
            && fence.live(&self.inner.host.instance, now)?
        {
            if &fence.owner == owner {
                return Ok(fence);
            }
            return Err(Error::refusal(
                "source_fenced",
                "another continuous cutover owns the source fence; retry after it closes or expires",
            ));
        }
        let fence = SourceFence {
            dataset: dataset.clone(),
            owner: owner.clone(),
            token: uuid::Uuid::new_v4().to_string(),
            host: self.inner.host.instance.clone(),
            expires_millis: now
                .checked_add(millis)
                .ok_or_else(|| invalid_fence("source fence monotonic deadline overflow"))?,
            closed: false,
        };
        self.inner
            .lifecycle
            .commit(&view, vec![record("source-fence", &fence)?])
            .await?;
        Ok(fence)
    }

    /// Checks the exact live fence against the caller's locked selecting view.
    pub(in crate::engine) async fn check_source_fence(
        &self,
        view: &View,
        fence: &SourceFence,
    ) -> Result<()> {
        if &crate::catalog::lifecycle_ident(&fence.dataset) != view.table.identifier() {
            return Err(invalid_fence("requested source fence dataset differs"));
        }
        let current = self
            .source_fence(view)
            .await?
            .ok_or_else(|| Error::refusal("source_fence_lost", "source fence is missing"))?;
        current.exact_live(
            fence,
            &self.inner.host.instance,
            self.inner.host.monotonic_millis()?,
        )
    }

    /// Returns the close record for the caller's atomic selection commit.
    pub(in crate::engine) async fn closed_source_fence_record(
        &self,
        view: &View,
        fence: &SourceFence,
    ) -> Result<(String, serde_json::Value)> {
        self.check_source_fence(view, fence).await?;
        let mut closed = fence.clone();
        closed.closed = true;
        record("source-fence", closed)
    }

    /// Closes a still-live owned generation, preserving any successor generation.
    pub(in crate::engine) async fn close_source_fence(&self, fence: &SourceFence) -> Result<()> {
        validate_record(fence)?;
        let lock = self.lock(&fence.dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(&fence.dataset).await?;
        let Some(current) = self.source_fence(&view).await? else {
            return Ok(());
        };
        if current != *fence
            || !current.live(
                &self.inner.host.instance,
                self.inner.host.monotonic_millis()?,
            )?
        {
            return Ok(());
        }
        let closed = self.closed_source_fence_record(&view, fence).await?;
        self.inner.lifecycle.commit(&view, vec![closed]).await?;
        Ok(())
    }
}

fn invalid_fence(message: impl Into<String>) -> Error {
    Error::refusal("invalid_source_fence", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> SourceFence {
        SourceFence {
            dataset: Id::new("dataset").unwrap(),
            owner: Id::new("parent").unwrap(),
            token: uuid::Uuid::new_v4().to_string(),
            host: uuid::Uuid::new_v4().to_string(),
            expires_millis: 100,
            closed: false,
        }
    }

    #[test]
    fn monotonic_deadline_and_host_bound_liveness() {
        let fence = fence();
        assert!(fence.live(&fence.host, 99).unwrap());
        assert!(!fence.live(&fence.host, 100).unwrap());
        assert!(!fence.live(&fence.host, 101).unwrap());
        assert!(!fence.live(&uuid::Uuid::new_v4().to_string(), 0).unwrap());
        let mut closed = fence.clone();
        closed.closed = true;
        assert!(!closed.live(&closed.host, 0).unwrap());
    }

    #[test]
    fn exact_live_requires_full_generation_equality() {
        let fence = fence();
        assert!(fence.exact_live(&fence, &fence.host, 99).is_ok());
        let mut successor = fence.clone();
        successor.token = uuid::Uuid::new_v4().to_string();
        assert!(successor.exact_live(&fence, &fence.host, 99).is_err());
        let mut altered = fence.clone();
        altered.expires_millis += 1;
        assert!(altered.exact_live(&fence, &fence.host, 99).is_err());
        assert!(fence.exact_live(&fence, &fence.host, 100).is_err());
    }

    #[test]
    fn malformed_records_fail_closed() {
        let mut fence = fence();
        let mut value = serde_json::to_value(&fence).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<SourceFence>(value).is_err());
        fence.token = "invalid".into();
        assert!(validate_record(&fence).is_err());
        fence.token = uuid::Uuid::new_v4().to_string();
        fence.expires_millis = 0;
        assert!(validate_record(&fence).is_err());
        fence.expires_millis = MAX_FENCE_MILLIS + 1;
        assert!(fence.live(&fence.host, 0).is_err());
    }

    fn limits() -> Limits {
        Limits {
            compute_threads: 2,
            memory_bytes: 256 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 16 << 20,
            ..Limits::default()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_reuses_deadline_and_stale_close_preserves_successor() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let dataset = Id::new("dataset").unwrap();
        let owner = Id::new("parent").unwrap();
        let first = engine
            .acquire_source_fence(&dataset, &owner, MAX_FENCE_MILLIS)
            .await
            .unwrap();
        let retry = engine
            .acquire_source_fence(&dataset, &owner, 1)
            .await
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(
            engine
                .acquire_source_fence(&dataset, &Id::new("other").unwrap(), 1)
                .await
                .unwrap_err()
                .code(),
            "source_fenced"
        );
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        assert_eq!(
            engine.check_source_intake(&view).await.unwrap_err().code(),
            "source_fenced"
        );
        let mut successor = first.clone();
        successor.token = uuid::Uuid::new_v4().to_string();
        engine
            .inner
            .lifecycle
            .commit(&view, vec![record("source-fence", &successor).unwrap()])
            .await
            .unwrap();
        engine.close_source_fence(&first).await.unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        assert_eq!(
            engine.source_fence(&view).await.unwrap(),
            Some(successor.clone())
        );
        engine.close_source_fence(&successor).await.unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        engine.check_source_intake(&view).await.unwrap();
        assert!(engine.source_fence(&view).await.unwrap().unwrap().closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reopened_host_expires_previous_host_generation() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let dataset = Id::new("dataset").unwrap();
        let owner = Id::new("parent").unwrap();
        let first = engine
            .acquire_source_fence(&dataset, &owner, MAX_FENCE_MILLIS)
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        engine.check_source_intake(&view).await.unwrap();
        assert!(engine.check_source_fence(&view, &first).await.is_err());
        let successor = engine
            .acquire_source_fence(&dataset, &owner, MAX_FENCE_MILLIS)
            .await
            .unwrap();
        assert_ne!(first.host, successor.host);
        assert_ne!(first.token, successor.token);
        engine.close_source_fence(&first).await.unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        assert_eq!(engine.source_fence(&view).await.unwrap(), Some(successor));
        engine.shutdown().await.unwrap();
    }
}
