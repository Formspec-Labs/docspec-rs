//! Exact original host file custody precedes export payload writes.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::catalog::SnapshotRows;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct HostFile {
    pub operation: Id,
    pub operation_revision: i64,
    pub id: Id,
    pub attempt: Id,
    pub host_root: String,
    pub path: String,
    pub device: u64,
    pub inode: u64,
}

pub(in crate::engine) fn key(file: &HostFile) -> String {
    format!(
        "export-host-file/{}/{}/{}",
        file.operation, file.operation_revision, file.id
    )
}

pub(in crate::engine) fn validate(
    file: &HostFile,
    key: &str,
    revision: i64,
    rows: &SnapshotRows,
    budget: Option<&crate::budget::Budget>,
) -> Result<String> {
    let attempt_key = format!(
        "export-attempt/{}/{}",
        file.operation, file.operation_revision
    );
    let operation_key = format!("export-operation/{}", file.operation);
    let protection_key = format!("protection/{}", file.attempt);
    let _memory = budget
        .map(|budget| rows.reserve_decoded(budget, &[&operation_key, &protection_key]))
        .transpose()?;
    let (_, _, operation): (_, _, ExportOperation) = rows
        .get_record(&operation_key)?
        .ok_or_else(invalid_export)?;
    validate_operation_record(&operation)?;
    let (protection_revision, _, protection): (_, _, Protection) = rows
        .get_record(&protection_key)?
        .ok_or_else(invalid_export)?;
    let remote = remote::attempt_protection(rows, &attempt_key, budget)?;
    let directory = Path::new(&file.host_root)
        .join("objects")
        .join(operation.request.dataset.as_str())
        .join(file.attempt.as_str());
    let expected = directory.join(if remote.is_some() {
        format!("stage-{}.parquet", file.id)
    } else {
        "export.parquet".into()
    });
    let prefix = format!("{}/", directory.display());
    if key != self::key(file)
        || revision <= protection_revision
        || protection_revision <= file.operation_revision
        || rows.revision(&operation_key) != Some(file.operation_revision)
        || protection.id != file.attempt
        || protection.consumer != operation.request.dataset
        || protection.purpose != format!("{}/export", file.operation)
        || protection.retired
        || protection.paths.first() != Some(&prefix)
        || (remote.is_none() && protection.paths.as_slice() != std::slice::from_ref(&prefix))
        || file.inode == 0
        || file.path != expected.to_string_lossy()
        || !Path::new(&file.host_root).is_absolute()
        || !storage::valid_object_locator(&file.host_root)
    {
        return Err(invalid_export());
    }

    if let Some((saved, host_prefix, _)) = &remote
        && (saved != &protection || host_prefix != &prefix)
    {
        return Err(invalid_export());
    }
    Ok(if remote.is_some() {
        format!("{}export.parquet", protection.paths[1])
    } else {
        file.path.clone()
    })
}

impl Engine {
    pub(super) async fn retain_host_file(
        &self,
        operation: &ExportOperation,
        path: &Path,
        device: u64,
        inode: u64,
    ) -> Result<HostFile> {
        let lock = self.lock(&operation.request.dataset).await;
        let _guard = lock.lock().await;
        let view = self
            .inner
            .lifecycle
            .open(&operation.request.dataset)
            .await?;
        self.check_export_authority(&view, operation).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let revision = rows
            .revision(&format!("export-operation/{}", operation.request.operation))
            .ok_or_else(invalid_export)?;
        let attempt = path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            .ok_or_else(invalid_export)?;
        let id = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("stage-"))
            .and_then(|name| name.strip_suffix(".parquet"))
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        let file = HostFile {
            operation: operation.request.operation.clone(),
            operation_revision: revision,
            id: Id::new(id)?,
            attempt: Id::new(attempt)?,
            host_root: self.inner.root.to_string_lossy().into_owned(),
            path: path.to_string_lossy().into_owned(),
            device,
            inode,
        };
        let key = key(&file);
        validate(
            &file,
            &key,
            view.revision.checked_add(1).ok_or_else(invalid_export)?,
            &rows,
            Some(&self.inner.lifecycle.budget),
        )?;
        self.inner
            .lifecycle
            .commit(&view, vec![record(key, &file)?])
            .await?;
        Ok(file)
    }
}

pub(super) fn native_identity(file: &File) -> Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Err(Error::refusal(
            "removal_platform",
            "host stage custody requires Unix file identity",
        ))
    }
}

#[cfg(all(test, feature = "test-util", unix))]
mod tests;
