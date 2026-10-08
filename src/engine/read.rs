//! Verified native field-group reads through the execution host's shared budget.

use super::*;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};

impl Engine {
    /// Streams one independently verified field group from an exact release.
    ///
    /// # Errors
    /// Refuses unknown groups, invalid retained releases, or exhausted reader capacity.
    pub async fn read_group(
        &self,
        release: &ReleasePin,
        group: &str,
    ) -> Result<SendableRecordBatchStream> {
        read_group(release, group, &self.inner.lifecycle.budget).await
    }

    /// Streams the exact contributor occurrences retained by a related result.
    ///
    /// # Errors
    /// Refuses absent evidence, invalid retained releases, or exhausted reader capacity.
    pub async fn read_evidence(&self, release: &ReleasePin) -> Result<SendableRecordBatchStream> {
        read_evidence(release, &self.inner.lifecycle.budget).await
    }

    pub(crate) fn read_budget(&self) -> Budget {
        self.inner.lifecycle.budget.clone()
    }
}

/// Streams an independently verified field group without requiring its producing catalog.
///
/// The injected budget shares admission with other readers and application work.
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
    verify_release_with_budget(release, budget).await?;
    read_pin(pin, budget).await
}

/// Streams independently verified contributor occurrences without the producing catalog.
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
    verify_release_with_budget(release, budget).await?;
    read_pin(&related.evidence, budget).await
}

async fn read_pin(pin: &TablePin, budget: &Budget) -> Result<SendableRecordBatchStream> {
    let table = storage::open_pin(pin).await?;
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let stream = storage::read_batches(&table, budget)
        .await?
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
