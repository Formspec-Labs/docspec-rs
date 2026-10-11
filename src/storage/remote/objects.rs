//! Create-only S3 objects and independent streaming reads through maintained OpenDAL.
//!
//! There is no durable partial multipart resume. A failed write remains
//! unconfirmed until explicitly checked; this adapter never automatically
//! retransmits it. These primitives do not qualify the complete remote engine.

use crate::{Error, Result, budget::Budget, model::ObjectRef};
use futures::TryStreamExt;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use object_store::{ClientOptions, ObjectStore, RetryConfig, aws::AmazonS3Builder};
use opendal::{Operator, layers::TimeoutLayer, services::S3};
use std::{collections::HashMap, fmt, fs::File, io::Read, path::PathBuf, sync::Arc};

const CHUNK: usize = 8 << 20;
// Shared admission allowance for a streaming read. The HTTP transport manages
// response buffers; this is not a configured chunk limit or exact RSS bound.
pub(in crate::storage) const READ_ADMISSION: usize = 2 << 20;
// S3 supports at most 10,000 multipart parts. Fixed part size also bounds
// retained native part metadata; reject larger transfers before writing.
const MAX_UPLOAD_BYTES: u64 = CHUNK as u64 * 10_000;
// Admission covers source/buffered/in-flight chunks and bounded part metadata.
// This is a conservative payload allowance, not a measured whole-process RSS cap.
const UPLOAD_ADMISSION: usize = 4 * CHUNK + (4 << 20);

/// Conditional writer and independent reader for one owned S3 prefix.
#[derive(Clone)]
pub struct RemoteObjects {
    operator: Operator,
    file_io: FileIO,
    query_store: Arc<dyn ObjectStore>,
    scope: Scope,
}

impl fmt::Debug for RemoteObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteObjects").finish_non_exhaustive()
    }
}

impl RemoteObjects {
    pub(super) fn from_resolved(
        properties: &HashMap<String, String>,
        owned_prefix: &str,
    ) -> Result<Self> {
        let scope = Scope::new(owned_prefix)?;
        // Support the explicit credential subset used by this adapter. Refuse
        // unmapped S3 options rather than silently diverging from native FileIO.
        let supported = [
            "s3.endpoint",
            "s3.region",
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
            "s3.path-style-access",
            "s3.disable-ec2-metadata",
            "s3.disable-config-load",
        ];
        if properties
            .keys()
            .any(|key| key.starts_with("s3.") && !supported.contains(&key.as_str()))
        {
            return Err(config());
        }
        let required = |key: &str| {
            properties
                .get(key)
                .filter(|value| !value.is_empty())
                .ok_or_else(config)
        };
        let mut builder = S3::default()
            .bucket(&scope.bucket)
            .endpoint(required("s3.endpoint")?)
            .region(required("s3.region")?)
            .access_key_id(required("s3.access-key-id")?)
            .secret_access_key(required("s3.secret-access-key")?)
            .disable_ec2_metadata()
            .disable_config_load();
        if properties.contains_key("client.region") {
            // Native FileIO lets this override s3.region. Avoid two regions.
            return Err(config());
        }
        if let Some(token) = properties.get("s3.session-token") {
            builder = builder.session_token(token);
        }
        match properties.get("s3.path-style-access").map(String::as_str) {
            None | Some("false") => builder = builder.enable_virtual_host_style(),
            Some("true") => {}
            _ => return Err(config()),
        }
        // No RetryLayer: a timed-out close can already have committed remotely.
        let operator = Operator::new(builder)
            .map_err(|_| config())?
            .finish()
            .layer(
                TimeoutLayer::new()
                    .with_timeout(super::REQUEST_TIMEOUT)
                    .with_io_timeout(super::REQUEST_TIMEOUT),
            );
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .with_props(properties.iter().filter(|(key, _)| key.starts_with("s3.")))
        .build();
        // Build the maintained query client from the same explicit resolution.
        // Never use from_env: both required credential fields select its static
        // provider, preventing metadata/config discovery and ambient credentials.
        let endpoint = required("s3.endpoint")?;
        let virtual_hosted = properties
            .get("s3.path-style-access")
            .is_none_or(|value| value == "false");
        // OpenDAL accepts the service endpoint and adds the virtual bucket host.
        // AmazonS3Builder expects that host already present in virtual mode.
        // Preserve OpenDAL's scheme-less host and global AWS endpoint semantics.
        let query_endpoint = if endpoint.starts_with("http") {
            endpoint.to_owned()
        } else {
            format!("https://{endpoint}")
        };
        let query_endpoint = query_endpoint.replace(&format!("//{}.", scope.bucket), "//");
        // Match native canonicalization before selecting the regional endpoint.
        let query_endpoint = reqwest_rest::Url::parse(&query_endpoint)
            .map_err(|_| config())?
            .to_string();
        let query_endpoint = query_endpoint.trim_end_matches('/');
        let query_endpoint = if query_endpoint == "https://s3.amazonaws.com" {
            format!("https://s3.{}.amazonaws.com", required("s3.region")?)
        } else {
            query_endpoint.to_owned()
        };
        let query_endpoint = if virtual_hosted {
            query_endpoint.replacen("://", &format!("://{}.", scope.bucket), 1)
        } else {
            query_endpoint
        };
        let mut query = AmazonS3Builder::new()
            .with_bucket_name(&scope.bucket)
            .with_endpoint(query_endpoint)
            .with_region(required("s3.region")?)
            .with_access_key_id(required("s3.access-key-id")?)
            .with_secret_access_key(required("s3.secret-access-key")?)
            .with_virtual_hosted_style_request(virtual_hosted)
            // Explicit HTTP endpoints support existing localhost transport fixtures.
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(endpoint.starts_with("http://"))
                    .with_timeout(super::REQUEST_TIMEOUT)
                    .with_connect_timeout(super::CONNECT_TIMEOUT),
            )
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: super::REQUEST_TIMEOUT,
                ..RetryConfig::default()
            });
        if let Some(token) = properties.get("s3.session-token") {
            query = query.with_token(token);
        }
        let query_store = Arc::new(query.build().map_err(|_| config())?);
        Ok(Self {
            operator,
            file_io,
            query_store,
            scope,
        })
    }

    pub(in crate::storage) fn register_query_store(
        &self,
        runtime: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        let url = datafusion::execution::object_store::ObjectStoreUrl::parse(format!(
            "s3://{}/",
            self.scope.bucket
        ))?;
        runtime.register_object_store(url.as_ref(), self.query_store.clone());
        Ok(())
    }

    /// Returns native I/O for independently opening immutable table pins.
    ///
    /// This native handle is unrestricted and includes credential configuration;
    /// do not log its Debug output. Callers retain read and retention authority.
    pub fn file_io(&self) -> FileIO {
        self.file_io.clone()
    }

    /// Creates bounded metadata bytes without replacing an existing object.
    ///
    /// Admission charges the vector's capacity plus a payload-size allowance.
    /// The caller owns the supplied allocation before admission. The worker owns
    /// its bytes and reservation through actual completion, even on cancellation.
    /// Keep the caller's Tokio runtime alive and driving I/O until workers finish.
    /// Success acknowledges the write; [`Self::verify`] independently checks it.
    ///
    /// # Errors
    /// Refuses oversized metadata, wrong source identity, capacity exhaustion,
    /// conditional conflict, or an unconfirmed remote write.
    pub async fn create_bytes(
        &self,
        expected: &ObjectRef,
        bytes: Vec<u8>,
        budget: &Budget,
    ) -> Result<ObjectRef> {
        let key = self.scope.key(&expected.path)?;
        if bytes.len() > budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "capacity",
                "remote metadata exceeds configured limit",
            ));
        }
        let admission = bytes.capacity().checked_add(bytes.len()).ok_or_else(|| {
            Error::refusal(
                "capacity",
                "remote metadata allocation exceeds admission granularity",
            )
        })?;
        let memory = budget.reserve(admission)?;
        let objects = self.clone();
        let expected = expected.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                if bytes.len() as u64 != expected.bytes
                    || blake3::hash(&bytes).to_hex().as_str() != expected.digest
                {
                    return Err(source_changed());
                }
                handle.block_on(objects.create_bytes_by_key(&key, bytes))?;
                Ok(expected)
            })
            .await
    }

    // Caller has already admitted the owned vector and its worker lifetime.
    // Candidate serialization and external expected-identity checks stay at their
    // respective boundaries; this entry point scopes the candidate once.
    pub(in crate::storage) async fn create_bytes_admitted(
        &self,
        path: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let key = self.scope.key(path)?;
        self.create_bytes_by_key(&key, bytes).await
    }

    // Both callers validate their scope once before entering this shared core.
    async fn create_bytes_by_key(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.operator
            .write_with(key, bytes)
            .if_not_exists(true)
            .await
            .map_err(write_error)?;
        Ok(())
    }

    /// Streams a retained local file into a create-only remote object.
    ///
    /// Copies and hashes once with eight MiB chunks and one native part in flight.
    /// Checks retained length and digest before closing the writer. Acknowledged
    /// writes still require explicit independent readback for qualification.
    /// Admission and the file/writer live in the worker through actual completion,
    /// even if its caller is canceled. Keep the caller's runtime driving I/O.
    /// No durable partial multipart resume or automatic retransmission is provided.
    ///
    /// # Errors
    /// Refuses out-of-scope destinations, oversized transfers, changed source,
    /// exhausted capacity, conditional conflict, and unconfirmed write outcomes.
    pub async fn upload_file(
        &self,
        expected: &ObjectRef,
        source: PathBuf,
        budget: &Budget,
    ) -> Result<ObjectRef> {
        let key = self.scope.key(&expected.path)?;
        if expected.bytes > MAX_UPLOAD_BYTES {
            return Err(Error::refusal(
                "capacity",
                "remote transfer exceeds bounded part inventory",
            ));
        }
        let memory = budget.reserve(UPLOAD_ADMISSION)?;
        let operator = self.operator.clone();
        let expected = expected.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                let mut source = File::open(source)?;
                if source.metadata()?.len() != expected.bytes {
                    return Err(source_changed());
                }
                handle.block_on(async {
                    let mut writer = operator
                        .writer_with(&key)
                        .if_not_exists(true)
                        .chunk(CHUNK)
                        .concurrent(1)
                        .await
                        .map_err(write_error)?;
                    let copied = async {
                        let mut hasher = blake3::Hasher::new();
                        let mut total = 0_u64;
                        loop {
                            let mut bytes = vec![0; CHUNK];
                            let mut filled = 0;
                            while filled < CHUNK {
                                let read = source.read(&mut bytes[filled..])?;
                                if read == 0 {
                                    break;
                                }
                                filled += read;
                            }
                            if filled == 0 {
                                break;
                            }
                            total = total
                                .checked_add(filled as u64)
                                .ok_or_else(source_changed)?;
                            if total > expected.bytes {
                                return Err(source_changed());
                            }
                            bytes.truncate(filled);
                            hasher.update(&bytes);
                            writer.write(bytes).await.map_err(write_error)?;
                        }
                        if total != expected.bytes
                            || source.metadata()?.len() != expected.bytes
                            || hasher.finalize().to_hex().as_str() != expected.digest
                        {
                            return Err(source_changed());
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = copied {
                        // Abort only this unpublished multipart attempt; never delete
                        // the destination. Failure leaves provider parts unconfirmed.
                        let _ = writer.abort().await;
                        return Err(error);
                    }
                    writer.close().await.map_err(write_error)?;
                    Ok(expected)
                })
            })
            .await
    }

    /// Independently checks exact remote length and digest through one streamed range.
    ///
    /// Returns the existing reference for explicit verification or reconciliation.
    /// This full integrity pass is never implicit in ordinary table opens. Uses
    /// one range GET with no range chunking, prefetch concurrency, or collection.
    /// Hashes transport-managed Buffer slices before polling the next buffer.
    /// Admission provides a buffer allowance, not an exact transport or RSS bound.
    /// Short streams, excess bytes, and changed observed sizes refuse. Empty
    /// objects need only before/after stat and the retained empty digest.
    /// The caller keeps its runtime driving I/O through worker completion.
    ///
    /// # Errors
    /// Refuses invalid scope, capacity exhaustion, remote read failure, or mismatch.
    pub async fn verify(&self, expected: &ObjectRef, budget: &Budget) -> Result<ObjectRef> {
        let key = self.scope.key(&expected.path)?;
        let memory = budget.reserve(READ_ADMISSION)?;
        let objects = self.clone();
        let expected = expected.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                let (digest, _) =
                    handle.block_on(objects.identity_by_key(&key, Some(expected.bytes)))?;
                if digest != expected.digest {
                    return Err(mismatch());
                }
                Ok(expected)
            })
            .await
    }

    // Absence is authoritative only at the first native stat. Transport/auth
    // failures and disappearance during readback never permit another write.
    pub(in crate::storage) async fn verify_if_present(
        &self,
        expected: &ObjectRef,
        budget: &Budget,
    ) -> Result<Option<ObjectRef>> {
        let key = self.scope.key(&expected.path)?;
        let memory = budget.reserve(READ_ADMISSION)?;
        let objects = self.clone();
        let expected = expected.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                handle.block_on(async {
                    let metadata = match objects.operator.stat(&key).await {
                        Ok(metadata) => metadata,
                        Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                            return Ok(None);
                        }
                        Err(_) => return Err(read_error()),
                    };
                    if !metadata.is_file() {
                        return Err(mismatch());
                    }
                    let (digest, _) = objects
                        .identity_after_stat(&key, Some(expected.bytes), metadata.content_length())
                        .await?;
                    if digest != expected.digest {
                        return Err(mismatch());
                    }
                    Ok(Some(expected))
                })
            })
            .await
    }

    pub(in crate::storage) fn validate_uri(path: &str) -> Result<()> {
        split_s3(path).map(|_| ())
    }

    pub(in crate::storage) fn owned_location(&self) -> String {
        format!(
            "s3://{}/{}",
            self.scope.bucket,
            self.scope.prefix.trim_end_matches('/')
        )
    }

    pub(in crate::storage) fn validate_path(&self, path: &str) -> Result<()> {
        self.scope.key(path).map(|_| ())
    }

    pub(in crate::storage) fn validate_table_location(&self, location: &str) -> Result<()> {
        let (bucket, key) = split_s3(location)?;
        let root = self.scope.prefix.trim_end_matches('/');
        if bucket != self.scope.bucket || (key != root && !key.starts_with(&self.scope.prefix)) {
            return Err(scope_error());
        }
        Ok(())
    }

    // The caller owns one read allowance and worker for the complete pin inventory.
    // No nested admission, worker, second GET or implicit verification pass.
    pub(in crate::storage) async fn identity_admitted(
        &self,
        owner: &crate::model::Id,
        path: &str,
    ) -> Result<ObjectRef> {
        let key = self.scope.key(path)?;
        let (digest, bytes) = self.identity_by_key(&key, None).await?;
        Ok(ObjectRef {
            owner: owner.clone(),
            path: path.into(),
            digest,
            bytes,
        })
    }

    // Reuse an already captured identity without rehashing unchanged bytes.
    // Configured scope is read authority; it never changes the retained owner.
    pub(in crate::storage) async fn check_available_admitted(
        &self,
        expected: &ObjectRef,
    ) -> Result<()> {
        let key = self.scope.key(&expected.path)?;
        let unavailable = || {
            Error::refusal(
                "unavailable_phase",
                "retained completion references an unavailable object",
            )
        };
        let metadata = self.operator.stat(&key).await.map_err(|_| unavailable())?;
        if !metadata.is_file() || metadata.content_length() != expected.bytes {
            return Err(unavailable());
        }
        Ok(())
    }

    // Fill the caller's admitted metadata buffer once. Hashing and parsing use
    // that same buffer, so there is no second GET or unverified substitution.
    pub(in crate::storage) async fn read_metadata_admitted(
        &self,
        expected: &ObjectRef,
        bytes: &mut [u8],
    ) -> Result<()> {
        if !self
            .read_metadata_if_present_admitted(expected, bytes)
            .await?
        {
            return Err(read_error());
        }
        Ok(())
    }

    pub(in crate::storage) async fn read_metadata_if_present_admitted(
        &self,
        expected: &ObjectRef,
        bytes: &mut [u8],
    ) -> Result<bool> {
        let key = self.scope.key(&expected.path)?;
        let metadata = match self.operator.stat(&key).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => return Ok(false),
            Err(_) => return Err(read_error()),
        };
        if !metadata.is_file() || metadata.content_length() != expected.bytes {
            return Err(mismatch());
        }
        let mut total = 0usize;
        if !bytes.is_empty() {
            let reader = self.operator.reader(&key).await.map_err(|_| read_error())?;
            let mut stream = reader
                .into_stream(0..expected.bytes)
                .await
                .map_err(|_| read_error())?;
            while let Some(buffer) = stream.try_next().await.map_err(|_| read_error())? {
                for slice in buffer {
                    let end = total.checked_add(slice.len()).ok_or_else(mismatch)?;
                    let target = bytes.get_mut(total..end).ok_or_else(mismatch)?;
                    target.copy_from_slice(&slice);
                    total = end;
                }
            }
        }
        let metadata = self.operator.stat(&key).await.map_err(|_| read_error())?;
        if total != bytes.len()
            || !metadata.is_file()
            || metadata.content_length() != expected.bytes
        {
            return Err(mismatch());
        }
        Ok(true)
    }

    async fn identity_by_key(
        &self,
        key: &str,
        expected_bytes: Option<u64>,
    ) -> Result<(String, u64)> {
        let bytes = self
            .operator
            .stat(key)
            .await
            .map_err(|_| read_error())?
            .content_length();
        self.identity_after_stat(key, expected_bytes, bytes).await
    }

    async fn identity_after_stat(
        &self,
        key: &str,
        expected_bytes: Option<u64>,
        bytes: u64,
    ) -> Result<(String, u64)> {
        if expected_bytes.is_some_and(|expected| expected != bytes) {
            return Err(mismatch());
        }
        let mut hasher = blake3::Hasher::new();
        let mut total = 0_u64;
        if bytes != 0 {
            let reader = self.operator.reader(key).await.map_err(|_| read_error())?;
            let mut stream = reader
                .into_stream(0..bytes)
                .await
                .map_err(|_| read_error())?;
            while let Some(buffer) = stream.try_next().await.map_err(|_| read_error())? {
                total = total
                    .checked_add(buffer.len() as u64)
                    .ok_or_else(mismatch)?;
                if total > bytes {
                    return Err(mismatch());
                }
                for slice in buffer {
                    hasher.update(&slice);
                }
            }
        }
        if total != bytes
            || self
                .operator
                .stat(key)
                .await
                .map_err(|_| read_error())?
                .content_length()
                != bytes
        {
            return Err(mismatch());
        }
        Ok((hasher.finalize().to_hex().to_string(), total))
    }
}

#[derive(Clone)]
struct Scope {
    bucket: String,
    prefix: String,
}

impl Scope {
    fn new(uri: &str) -> Result<Self> {
        let (bucket, key) = split_s3(uri.strip_suffix('/').unwrap_or(uri))?;
        Ok(Self {
            bucket: bucket.into(),
            prefix: format!("{key}/"),
        })
    }

    fn key(&self, uri: &str) -> Result<String> {
        let (bucket, key) = split_s3(uri)?;
        if bucket != self.bucket || !key.starts_with(&self.prefix) || key == self.prefix {
            return Err(scope_error());
        }
        Ok(key.into())
    }
}

fn split_s3(uri: &str) -> Result<(&str, &str)> {
    let (bucket, key) = uri
        .strip_prefix("s3://")
        .and_then(|rest| rest.split_once('/'))
        .ok_or_else(scope_error)?;
    if bucket.len() < 3
        || bucket.len() > 63
        || !bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        || !bucket
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        || bucket.contains("..")
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || key
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || matches!(c, '%' | '?' | '#' | '\\'))
    {
        return Err(scope_error());
    }
    Ok((bucket, key))
}

fn config() -> Error {
    Error::refusal(
        "remote_config",
        "conditional object configuration is unsupported or incomplete",
    )
}
fn scope_error() -> Error {
    Error::refusal(
        "remote_scope",
        "destination must be a strict S3 URI within the owned prefix",
    )
}
fn source_changed() -> Error {
    Error::refusal(
        "object_changed",
        "source differs from retained object identity",
    )
}
fn mismatch() -> Error {
    Error::refusal(
        "object_changed",
        "remote object differs from retained object identity",
    )
}
fn read_error() -> Error {
    Error::refusal("remote_read", "independent remote object read failed")
}
fn write_error(error: opendal::Error) -> Error {
    match error.kind() {
        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch => {
            Error::refusal(
                "remote_conflict",
                "conditional creation refused an existing or competing object",
            )
        }
        _ => Error::refusal(
            "remote_unconfirmed",
            "remote write is unconfirmed; explicitly verify retained identity before deciding whether to retry",
        ),
    }
}

#[cfg(test)]
pub(crate) mod tests;

// Rust guideline compliant 2026-02-21
