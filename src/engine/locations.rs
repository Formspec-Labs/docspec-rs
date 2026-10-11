//! Exact immutable destinations, separate from host custody and scratch paths.

use super::*;
pub(super) mod release;

#[derive(Clone, Debug)]
pub(crate) enum Location {
    Local(PathBuf),
    #[cfg(feature = "remote")]
    Remote(String),
}

impl Location {
    pub(super) fn root(objects: &storage::Objects, host: &Path) -> Self {
        let prefix = objects.retained_prefix();
        #[cfg(feature = "remote")]
        if let Some(prefix) = prefix {
            return Self::Remote(prefix);
        }
        #[cfg(not(feature = "remote"))]
        let _ = prefix;
        Self::Local(host.join("objects"))
    }

    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub(crate) fn child(&self, name: &str) -> Result<Self> {
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.chars().any(|c| {
                c.is_control() || c.is_whitespace() || matches!(c, '/' | '\\' | '%' | '?' | '#')
            })
        {
            return Err(invalid());
        }
        Ok(match self {
            Self::Local(path) => Self::Local(path.join(name)),
            #[cfg(feature = "remote")]
            Self::Remote(uri) => Self::Remote(format!("{uri}/{name}")),
        })
    }

    pub(crate) fn as_string(&self) -> Result<String> {
        match self {
            Self::Local(path) => path.to_str().map(str::to_owned).ok_or_else(invalid),
            #[cfg(feature = "remote")]
            Self::Remote(uri) => Ok(uri.clone()),
        }
    }

    pub(crate) fn protection_prefix(&self) -> Result<String> {
        Ok(format!("{}/", self.as_string()?))
    }

    pub(crate) fn local_path(&self) -> Result<&Path> {
        match self {
            Self::Local(path) => Ok(path),
            #[cfg(feature = "remote")]
            Self::Remote(_) => Err(unsupported()),
        }
    }

    pub(crate) fn into_local(self) -> Result<PathBuf> {
        match self {
            Self::Local(path) => Ok(path),
            #[cfg(feature = "remote")]
            Self::Remote(_) => Err(unsupported()),
        }
    }

    pub(crate) fn relative<'a>(&self, path: &'a str) -> Option<&'a str> {
        match self {
            Self::Local(root) => Path::new(path).strip_prefix(root).ok()?.to_str(),
            #[cfg(feature = "remote")]
            Self::Remote(root) => path
                .strip_prefix(root)?
                .strip_prefix('/')
                .filter(|tail| !tail.is_empty()),
        }
    }
}

// Pending identities are not admitted inputs or completed phase evidence. This
// record retains exact destinations after an unknown transfer outcome. Protection
// is committed in the same guarded transaction; recovery uses the same destination.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceAttempt {
    attempt: Id,
    consumer: Id,
    operation: Id,
    operation_revision: i64,
    request_digest: String,
    eligibility: u64,
    prefix: String,
    pub(super) inputs: BTreeMap<String, ObjectRef>,
}

impl Engine {
    pub(super) async fn reserve_source_attempt(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        view: &View,
        captured: &BTreeMap<String, ObjectRef>,
    ) -> Result<(SourceAttempt, bool)> {
        let rows = self.inner.lifecycle.rows(view).await?;
        let (operation_revision, _, _) = rows
            .get_record::<Operation>(&format!("operation/{}", spec.operation()))?
            .ok_or_else(invalid)?;
        let key = source_attempt_key(spec.operation(), operation_revision);
        if let Some((_, _, saved)) = rows.get_record::<SourceAttempt>(&key)? {
            self.validate_source_attempt(&key, &saved, &rows)?;
            self.check(view, operation, spec.operation()).await?;
            return Ok((saved, true));
        }
        // Source stat belongs only to a new intent. Recovery resolves the exact
        // saved destination first, even when the original source no longer exists.
        let attempt = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .retained
            .child(spec.dataset().as_str())?
            .child(attempt.as_str())?;
        let mut inputs = BTreeMap::new();
        for (name, input) in spec.inputs() {
            if captured.contains_key(&name) {
                continue;
            }
            let path = directory.child(&format!("{name}.parquet"))?.as_string()?;
            self.inner.objects.validate_retained_path(&path)?;
            inputs.insert(
                name,
                ObjectRef {
                    owner: spec.dataset().clone(),
                    path,
                    digest: input.digest,
                    bytes: std::fs::metadata(input.path)?.len(),
                },
            );
        }
        let pending = SourceAttempt {
            attempt: attempt.clone(),
            consumer: spec.dataset().clone(),
            operation: spec.operation().clone(),
            operation_revision,
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            prefix: directory.protection_prefix()?,
            inputs,
        };
        let protection = Protection {
            id: attempt.clone(),
            consumer: spec.dataset().clone(),
            purpose: format!("{}/admission", spec.operation()),
            paths: vec![pending.prefix.clone()],
            retired: false,
        };
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let fresh = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&fresh, operation, spec.operation()).await?;
        let rows = self.inner.lifecycle.rows(&fresh).await?;
        if rows.revision(&format!("operation/{}", spec.operation())) != Some(operation_revision) {
            return Err(invalid());
        }
        if let Some((_, _, saved)) = rows.get_record::<SourceAttempt>(&key)? {
            self.validate_source_attempt(&key, &saved, &rows)?;
            return Ok((saved, true));
        }
        let records = vec![
            record(format!("protection/{attempt}"), protection)?,
            record(&key, &pending)?,
        ];
        if let Err(error) = self.inner.lifecycle.commit(&fresh, records).await {
            // A lost response can follow a durable commit. Adopt only the exact
            // operation-generation intent from a fresh native lifecycle view.
            let after = self.inner.lifecycle.open(spec.dataset()).await?;
            self.check(&after, operation, spec.operation()).await?;
            let rows = self.inner.lifecycle.rows(&after).await?;
            if let Some((_, _, saved)) = rows.get_record::<SourceAttempt>(&key)? {
                self.validate_source_attempt(&key, &saved, &rows)?;
                return Ok((saved, true));
            }
            return Err(error);
        }
        Ok((pending, false))
    }

    fn validate_source_attempt(
        &self,
        key: &str,
        saved: &SourceAttempt,
        rows: &crate::catalog::SnapshotRows,
    ) -> Result<()> {
        validate_source_attempt(key, saved, rows)?;
        let directory = self
            .inner
            .retained
            .child(saved.consumer.as_str())?
            .child(saved.attempt.as_str())?;
        if saved.prefix != directory.protection_prefix()? {
            return Err(invalid());
        }
        for object in saved.inputs.values() {
            self.inner.objects.validate_retained_path(&object.path)?;
        }
        Ok(())
    }

    pub(super) async fn reserve_location(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        purpose: &str,
    ) -> Result<Location> {
        let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
        let directory = self
            .inner
            .retained
            .child(spec.dataset().as_str())?
            .child(id.as_str())?;
        let reservation = Protection {
            id: id.clone(),
            consumer: spec.dataset().clone(),
            purpose: format!("{}/{}", spec.operation(), purpose),
            paths: vec![directory.protection_prefix()?],
            retired: false,
        };
        let lock = self.lock(spec.dataset()).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(spec.dataset()).await?;
        self.check(&view, operation, spec.operation()).await?;
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![record(format!("protection/{id}"), reservation)?],
            )
            .await?;
        match &directory {
            Location::Local(path) => std::fs::create_dir_all(path)?,
            #[cfg(feature = "remote")]
            Location::Remote(_) => {}
        }
        Ok(directory)
    }
}

pub(super) fn validate_source_attempt_record(
    key: &str,
    json: &str,
    rows: &crate::catalog::SnapshotRows,
) -> Result<()> {
    let pending: SourceAttempt = serde_json::from_str(json)?;
    validate_source_attempt(key, &pending, rows)
}

fn validate_source_attempt(
    key: &str,
    pending: &SourceAttempt,
    rows: &crate::catalog::SnapshotRows,
) -> Result<()> {
    let (operation_revision, _, operation) = rows
        .get_record::<Operation>(&format!("operation/{}", pending.operation))?
        .ok_or_else(invalid)?;
    let (request_revision, _, request) = rows
        .get_record::<OperationRequest>(&format!("request/{}", pending.operation))?
        .ok_or_else(invalid)?;
    let intent_revision = rows.revision(key).ok_or_else(invalid)?;
    if key != source_attempt_key(&pending.operation, pending.operation_revision)
        || operation_revision != pending.operation_revision
        || request_revision != operation_revision
        || pending.consumer != *request.dataset()
        || pending.operation != *request.operation()
        || pending.request_digest != operation.request_digest
        || pending.request_digest != fingerprint(&request)?
        || pending.eligibility != operation.eligibility
        || pending.inputs.is_empty()
        || intent_revision <= operation_revision
    {
        return Err(invalid());
    }
    let (protection_revision, _, reservation) = rows
        .get_record::<Protection>(&format!("protection/{}", pending.attempt))?
        .ok_or_else(invalid)?;
    if protection_revision != intent_revision
        || reservation.id != pending.attempt
        || reservation.consumer != pending.consumer
        || reservation.paths != [pending.prefix.clone()]
        || reservation.purpose != format!("{}/admission", pending.operation)
        || reservation.retired
    {
        return Err(invalid());
    }
    let requested = request.inputs();
    for (name, object) in &pending.inputs {
        let input = requested.get(name).ok_or_else(invalid)?;
        if object.owner != pending.consumer
            || object.digest != input.digest
            || object.path != format!("{}{name}.parquet", pending.prefix)
            || !storage::valid_object_locator(&object.path)
            || object.digest.len() != 64
            || !object.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub(super) fn source_attempt_key(operation: &Id, revision: i64) -> String {
    format!("source-attempt/{operation}/{revision}/admission")
}

pub(super) fn invalid() -> Error {
    Error::refusal(
        "retained_location",
        "immutable destination differs from its protected location",
    )
}

pub(super) fn unsupported() -> Error {
    Error::refusal(
        "remote_build_unavailable",
        "this operation is not qualified for remote storage; use initial Build and selected source reads",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_local_attempt_location_preserves_owned_prefix_and_host_separation() {
        let host = PathBuf::from("/host/workspace");
        let root = Location::root(&storage::Objects::Local, &host);
        let attempt = root.child("dataset").unwrap().child("attempt").unwrap();
        assert_eq!(
            attempt.protection_prefix().unwrap(),
            "/host/workspace/objects/dataset/attempt/"
        );
        assert_eq!(
            attempt.local_path().unwrap(),
            Path::new("/host/workspace/objects/dataset/attempt")
        );
        assert_eq!(
            attempt.relative("/host/workspace/objects/dataset/attempt/input.parquet"),
            Some("input.parquet")
        );
        assert_eq!(
            attempt.relative("/host/workspace/objects/dataset/attempt-other/input.parquet"),
            None
        );
        for name in [
            "../other",
            "..",
            "input%2fother",
            "input/other",
            "input\\other",
        ] {
            assert!(attempt.child(name).is_err());
        }
    }

    #[cfg(feature = "remote")]
    #[test]
    fn remote_location_never_becomes_a_host_path_or_crosses_an_attempt_prefix() {
        let root = Location::Remote("s3://bucket/owned".into());
        let attempt = root.child("dataset").unwrap().child("attempt").unwrap();
        assert_eq!(
            attempt.protection_prefix().unwrap(),
            "s3://bucket/owned/dataset/attempt/"
        );
        assert_eq!(
            attempt.relative("s3://bucket/owned/dataset/attempt/input.parquet"),
            Some("input.parquet")
        );
        assert_eq!(
            attempt.relative("s3://bucket/owned/dataset/attempt-other/input.parquet"),
            None
        );
        assert!(attempt.local_path().is_err());
        assert!(attempt.into_local().is_err());
    }
}

#[cfg(all(test, feature = "remote", feature = "test-util"))]
mod remote_tests;
