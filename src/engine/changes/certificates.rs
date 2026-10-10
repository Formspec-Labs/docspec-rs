//! Optional local applied-write evidence, separate from complete state and recovery.

use super::*;
use crate::catalog::SnapshotRows;
use crate::engine::retention::retirement::record_retired;

// Change qualification describes the producing algorithm, not a sampled readback.
pub(in crate::engine) const WRITE_RULES: &str = "docspec.local-applied-writes.v1";
const CHANGE_FORMAT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct PreparedChanges {
    pub format: u32,
    pub rules: String,
    pub operation: OperationId,
    pub before: VersionRef,
    pub descriptor: ObjectRef,
}

impl From<&ChangeCertificate> for PreparedChanges {
    fn from(value: &ChangeCertificate) -> Self {
        Self {
            format: value.format,
            rules: value.rules.clone(),
            operation: value.after.version.clone(),
            before: value.before.clone(),
            descriptor: value.descriptor.clone(),
        }
    }
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_changes",
        "optional changes differ from their qualified local selection",
    )
}

pub(in crate::engine) fn check_prepared(
    prepared: &PreparedChanges,
    key: &str,
    dataset: &Id,
) -> Result<()> {
    if prepared.format != CHANGE_FORMAT
        || prepared.rules != WRITE_RULES
        || key != format!("prepared-changes/{}", prepared.operation)
        || &prepared.before.dataset != dataset
        || &prepared.before.manifest.owner != dataset
        || &prepared.descriptor.owner != dataset
        || prepared.operation == prepared.before.version
    {
        return Err(invalid());
    }
    Ok(())
}

#[derive(Deserialize)]
struct Selected {
    id: Id,
    manifest: ObjectRef,
    verified: bool,
    dataset: SelectedDataset,
}

#[derive(Deserialize)]
struct SelectedDataset {
    dataset: Id,
    version: Id,
}

pub(in crate::engine) fn check_certificate(
    rows: &SnapshotRows,
    revision: i64,
    key: &str,
    certificate: &ChangeCertificate,
) -> Result<()> {
    let after = &certificate.after;
    check_prepared(
        &certificate.into(),
        &format!("prepared-changes/{}", after.version),
        &after.dataset,
    )?;
    let (selected_revision, _, selected): (_, _, Selected) = rows
        .get_record(&format!("result/{}", after.version))?
        .ok_or_else(invalid)?;
    if revision <= 0
        || revision != selected_revision
        || key != format!("change-certificate/{}", after.version)
        || after.manifest.owner != after.dataset
        || selected.id != after.version
        || selected.dataset.version != after.version
        || selected.dataset.dataset != after.dataset
        || selected.manifest != after.manifest
        || !selected.verified
    {
        return Err(invalid());
    }
    Ok(())
}

pub(in crate::engine) fn check_applied(
    changes: &AppliedChangeSet,
    prepared: &PreparedChanges,
) -> Result<()> {
    check_bindings(changes)?;
    if changes.format != prepared.format
        || changes.rules != prepared.rules
        || changes.operation != prepared.operation
        || changes.before != prepared.before
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn check_bindings(changes: &AppliedChangeSet) -> Result<()> {
    if changes.format != CHANGE_FORMAT
        || changes.rules != WRITE_RULES
        || changes.before.version == changes.operation
        || changes.before.manifest.owner != changes.before.dataset
        || (changes.source.is_none() && changes.receipts.is_none())
    {
        return Err(invalid());
    }
    for pin in changes.source.iter().chain(&changes.receipts) {
        if pin.rules != WRITE_RULES
            || pin.base_version != changes.before.version
            || pin.base_manifest != changes.before.manifest
            || pin.before_metadata.owner != changes.before.dataset
            || pin.after_metadata.owner != changes.before.dataset
            || pin.removed.metadata.owner != changes.before.dataset
            || pin.added.metadata.owner != changes.before.dataset
        {
            return Err(invalid());
        }
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine) async fn prepare_changes(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        before: Option<&ReleasePin>,
        source: Option<&ChangePin>,
        receipts: Option<&ChangePin>,
    ) -> Result<()> {
        if source.is_none() && receipts.is_none() {
            return Ok(());
        }
        // An old recovered phase remains useful, but its missing production
        // qualification cannot be supplied by today's sampled validation.
        if source
            .into_iter()
            .chain(receipts)
            .any(|pin| pin.rules != WRITE_RULES)
        {
            return Ok(());
        }
        let lifecycle = &self.inner.lifecycle;
        let _memory = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(4))?;
        let before = before.ok_or_else(invalid)?.version_ref();
        let applied = AppliedChangeSet {
            format: CHANGE_FORMAT,
            rules: WRITE_RULES.into(),
            operation: spec.operation().clone(),
            before: before.clone(),
            source: source.cloned(),
            receipts: receipts.cloned(),
        };
        let key = format!("prepared-changes/{}", spec.operation());
        let view = lifecycle.open(spec.dataset()).await?;
        if let Some(prepared) = self
            .active_completion::<PreparedChanges>(&view, &key)
            .await?
        {
            check_prepared(&prepared, &key, spec.dataset())?;
            check_applied(&applied, &prepared)?;
            // Reuse the prepared byte identity. Selection independently reads
            // and validates the descriptor once before certifying its result.
            if storage::json_digest(&applied)?.to_hex().as_str() != prepared.descriptor.digest {
                return Err(invalid());
            }
            return Ok(());
        }
        let directory = self.reserve_attempt(spec, operation, "changes").await?;
        let path = directory.join("changes.json");
        inputs::metadata_size(&applied, lifecycle.budget.limits.max_metadata_bytes)?;
        let bytes = serde_json::to_vec(&applied)?;
        storage::write_new(&path, &bytes)?;
        let prepared = PreparedChanges {
            format: CHANGE_FORMAT,
            rules: WRITE_RULES.into(),
            operation: spec.operation().clone(),
            before,
            descriptor: ObjectRef {
                owner: spec.dataset().clone(),
                path: path.to_string_lossy().into_owned(),
                digest: blake3::hash(&bytes).to_hex().to_string(),
                bytes: bytes.len() as u64,
            },
        };
        check_prepared(&prepared, &key, spec.dataset())?;
        check_applied(&applied, &prepared)?;
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(spec.dataset()).await?;
        self.check_completion(&view, operation, spec.operation())
            .await?;
        if let Some(existing) = self
            .active_completion::<PreparedChanges>(&view, &key)
            .await?
        {
            check_prepared(&existing, &key, spec.dataset())?;
            check_applied(&applied, &existing)?;
            if existing.descriptor.digest != prepared.descriptor.digest
                || existing.descriptor.bytes != prepared.descriptor.bytes
            {
                return Err(invalid());
            }
            return Ok(());
        }
        lifecycle
            .commit(&view, vec![record(&key, prepared)?])
            .await?;
        Ok(())
    }

    /// Resolves optional local change evidence without reading data payloads.
    ///
    /// Missing or retired history returns `None`. The returned metadata creates
    /// no lasting retention promise; actual consumers need their own admission.
    ///
    /// # Errors
    /// Refuses unavailable results, changed objects, malformed local certificates,
    /// mismatched selections, or exhausted metadata capacity.
    pub async fn change_certificate(
        &self,
        result: &VersionRef,
    ) -> Result<Option<CertifiedChanges>> {
        let memory = self.request_memory(result)?;
        let result = result.clone();
        self.admit_work(memory, move |engine| async move {
            let lock = engine.lock(&result.dataset).await;
            let _guard = lock.lock().await;
            let lifecycle = &engine.inner.lifecycle;
            let view = lifecycle.open(&result.dataset).await?;
            engine
                .check_operation_available(&view, &result.version)
                .await?;
            let rows = lifecycle.rows(&view).await?;
            let key = format!("change-certificate/{}", result.version);
            let result_key = format!("result/{}", result.version);
            let _decoded = rows.reserve_decoded(&lifecycle.budget, &[&key, &result_key])?;
            let (_, _, selected): (_, _, Selected) =
                rows.get_record(&result_key)?.ok_or_else(invalid)?;
            if selected.id != result.version
                || selected.dataset.version != result.version
                || selected.dataset.dataset != result.dataset
                || selected.manifest != result.manifest
                || !selected.verified
            {
                return Err(invalid());
            }
            let Some((revision, _, certificate)) = rows.get_record::<ChangeCertificate>(&key)?
            else {
                return Ok(None);
            };
            if record_retired(&rows, &view, &key, revision, &lifecycle.budget)? {
                return Ok(None);
            }
            check_certificate(&rows, revision, &key, &certificate)?;
            if certificate.after != result {
                return Err(invalid());
            }
            let memory = Arc::new(
                lifecycle
                    .budget
                    .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
            );
            let changes: AppliedChangeSet =
                inputs::read_manifest(&certificate.descriptor, &lifecycle.budget, &mut 0, &memory)
                    .await?;
            check_applied(&changes, &(&certificate).into())?;
            Ok(Some(CertifiedChanges {
                revision,
                certificate,
                changes,
            }))
        })
        .await
    }

    pub(in crate::engine) async fn changes_for_selection(
        &self,
        view: &View,
        rows: &SnapshotRows,
        release: &ReleasePin,
    ) -> Result<Option<ChangeCertificate>> {
        let key = format!("prepared-changes/{}", release.id);
        let budget = &self.inner.lifecycle.budget;
        let _decoded = rows.reserve_decoded(budget, &[&key])?;
        let Some((revision, _, prepared)) = rows.get_record::<PreparedChanges>(&key)? else {
            return Ok(None);
        };
        if record_retired(rows, view, &key, revision, budget)? {
            return Ok(None);
        }
        check_prepared(&prepared, &key, &release.dataset.dataset)?;
        if release.dataset.parent_manifest.as_ref() != Some(&prepared.before.manifest)
            || release.dataset.request.base() != Some(&prepared.before.version)
        {
            return Err(invalid());
        }
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
        let applied: AppliedChangeSet =
            inputs::read_manifest(&prepared.descriptor, budget, &mut 0, &memory).await?;
        check_applied(&applied, &prepared)?;
        if applied.source.as_ref().is_some_and(|pin| {
            release
                .dataset
                .groups
                .get("source")
                .is_none_or(|source| source.metadata != pin.after_metadata)
        }) || applied.receipts.as_ref().is_some_and(|pin| {
            release
                .dataset
                .evidence
                .as_ref()
                .is_none_or(|receipts| receipts.table.metadata != pin.after_metadata)
        }) {
            return Err(invalid());
        }
        Ok(Some(ChangeCertificate {
            format: prepared.format,
            rules: prepared.rules,
            before: prepared.before,
            after: release.version_ref(),
            descriptor: prepared.descriptor,
        }))
    }
}
