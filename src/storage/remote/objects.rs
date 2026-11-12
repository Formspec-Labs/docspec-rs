//! Create-only S3 objects and independent streaming reads through maintained OpenDAL.
//!
//! There is no durable partial multipart resume. A failed write remains
//! unconfirmed until explicitly checked; this adapter never automatically
//! retransmits it. These primitives do not qualify the complete remote engine.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result, budget::Budget, model::ObjectRef};
use futures::TryStreamExt;
use iceberg::io::{FileIO, FileIOBuilder};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig,
    aws::AmazonS3Builder,
    client::{HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService},
};
use opendal::{
    Operator,
    layers::{HttpClientLayer, TimeoutLayer},
    raw::{HttpBody, HttpClient, HttpFetch},
    services::S3,
};
use std::ops::Range;
use std::{collections::HashMap, fmt, fs::File, io::Read, path::PathBuf, sync::Arc};

use super::PART_BYTES as CHUNK;
// Shared admission allowance for a streaming read. The HTTP transport manages
// response buffers; this is not a configured chunk limit or exact RSS bound.
pub(in crate::storage) const READ_ADMISSION: usize = 2 << 20;
// S3 supports at most 10,000 multipart parts. Fixed part size also bounds
// retained native part metadata; reject larger transfers before writing.
const MAX_UPLOAD_BYTES: u64 = CHUNK as u64 * 10_000;
// Admission covers source/buffered/in-flight chunks and bounded part metadata.
// This is a conservative payload allowance, not a measured whole-process RSS cap.
const UPLOAD_ADMISSION: usize = 4 * CHUNK + (4 << 20);

struct ConditionalPutClient(HttpClient);
impl HttpFetch for ConditionalPutClient {
    async fn fetch(
        &self,
        request: axum::http::Request<opendal::Buffer>,
    ) -> opendal::Result<axum::http::Response<HttpBody>> {
        let conditional_put = request.method() == axum::http::Method::PUT
            && request
                .headers()
                .contains_key(axum::http::header::IF_NONE_MATCH);
        let response = self.0.fetch(request).await?;
        // OpenDAL 0.57 maps both 304 and 412 to the same permanent error.
        // A 304 PUT response cannot prove that our create precondition failed.
        if conditional_put && response.status() == axum::http::StatusCode::NOT_MODIFIED {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "unexpected HTTP304 on conditional PUT",
            ));
        }
        Ok(response)
    }
}

#[derive(Debug, Clone)]
struct SnapshotHttp(object_store::client::HttpClient);

impl SnapshotHttp {
    fn new(allow_http: bool) -> Result<Self> {
        let client = reqwest_rest::Client::builder()
            .redirect(reqwest_rest::redirect::Policy::none())
            .no_proxy()
            .https_only(!allow_http)
            .connect_timeout(super::CONNECT_TIMEOUT)
            .timeout(super::REQUEST_TIMEOUT)
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .build()
            .map_err(|_| config())?;
        Ok(Self(object_store::client::HttpClient::new(client)))
    }
}

impl HttpConnector for SnapshotHttp {
    fn connect(
        &self,
        _options: &ClientOptions,
    ) -> object_store::Result<object_store::client::HttpClient> {
        Ok(object_store::client::HttpClient::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl HttpService for SnapshotHttp {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        use axum::http::Method;

        let get = request.method() == Method::GET;
        if !(get || request.method() == Method::HEAD) || request.body().content_length() != 0 {
            return Err(HttpError::new(
                HttpErrorKind::Request,
                std::io::Error::other("snapshot source supports only read requests"),
            ));
        }
        let response = self.0.execute(request).await?;
        if get && response.status().is_success() {
            return Ok(response);
        }
        // Keep status and headers for native checks; retry handling collects error bodies.
        Ok(response.map(|_| bytes::Bytes::new().into()))
    }
}

/// Conditional writer and independent reader for one owned S3 prefix.
#[derive(Clone)]
pub struct RemoteObjects {
    operator: Operator,
    single_put: Operator,
    file_io: FileIO,
    query_store: Arc<dyn ObjectStore>,
    scope: Scope,
    temporary: bool,
    placement: String,
}

#[derive(Clone)]
pub(crate) struct SnapshotSource {
    query_store: Arc<dyn ObjectStore>,
    scope: Scope,
    pub(crate) placement: String,
}

impl fmt::Debug for SnapshotSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotSource").finish_non_exhaustive()
    }
}

impl SnapshotSource {
    pub(super) fn from_resolved(
        properties: &HashMap<String, String>,
        prefix: &str,
    ) -> Result<Self> {
        let scope = Scope::new(prefix)?;
        let (query, placement) = query_builder(properties, &scope)?;
        let allow_http = properties
            .get("s3.endpoint")
            .is_some_and(|value| value.starts_with("http://"));
        let query_store = Arc::new(
            query
                .with_http_connector(SnapshotHttp::new(allow_http)?)
                .build()
                .map_err(|_| config())?,
        );
        Ok(Self {
            query_store,
            scope,
            placement,
        })
    }

    pub(crate) fn prefix(&self) -> String {
        self.scope.location()
    }

    pub(crate) fn check_scope(prefix: &str, uri: &str) -> Result<()> {
        Scope::new(prefix)?.key(uri).map(|_| ())
    }

    fn object_path(&self, uri: &str) -> Result<object_store::path::Path> {
        let key = self.scope.key(uri)?;
        object_store::path::Path::parse(key).map_err(|_| scope_error())
    }

    pub(crate) const TRANSPORT_ADMISSION: usize = READ_ADMISSION;

    pub(crate) async fn size(&self, uri: &str, budget: &Budget) -> Result<u64> {
        let object_path = self.object_path(uri)?;
        let transport = budget.reserve(READ_ADMISSION)?;
        self.size_admitted_path(&object_path, &transport).await
    }

    pub(crate) async fn size_admitted(
        &self,
        uri: &str,
        transport: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<u64> {
        let object_path = self.object_path(uri)?;
        self.size_admitted_path(&object_path, transport).await
    }

    async fn size_admitted_path(
        &self,
        object_path: &object_store::path::Path,
        _transport: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<u64> {
        Ok(self
            .query_store
            .head(object_path)
            .await
            .map_err(|_| read_error())?
            .size)
    }

    // The caller holds the exact destination allowance until it consumes the bytes.
    #[cfg(test)]
    pub(crate) async fn read_range(
        &self,
        uri: &str,
        size: u64,
        range: Range<u64>,
        destination: &mut [u8],
        budget: &Budget,
    ) -> Result<()> {
        let max_chunk = budget
            .limits
            .max_batch_bytes
            .min(crate::storage::SNAPSHOT_MEMBER_CHUNK_BYTES);
        if range.start >= range.end
            || range.end > size
            || range.end - range.start != destination.len() as u64
            || destination.len() > max_chunk
        {
            return Err(mismatch());
        }
        let object_path = self.object_path(uri)?;
        let transport = budget.reserve(READ_ADMISSION)?;
        self.read_range_admitted_path(&object_path, size, range, destination, &transport)
            .await
    }

    pub(crate) async fn read_range_admitted(
        &self,
        uri: &str,
        size: u64,
        range: Range<u64>,
        destination: &mut [u8],
        max_chunk: usize,
        transport: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        if range.start >= range.end
            || range.end > size
            || range.end - range.start != destination.len() as u64
            || destination.len() > max_chunk
        {
            return Err(mismatch());
        }
        let object_path = self.object_path(uri)?;
        self.read_range_admitted_path(&object_path, size, range, destination, transport)
            .await
    }

    async fn read_range_admitted_path(
        &self,
        object_path: &object_store::path::Path,
        size: u64,
        range: Range<u64>,
        destination: &mut [u8],
        _transport: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        let response = self
            .query_store
            .get_opts(
                object_path,
                object_store::GetOptions {
                    range: Some(object_store::GetRange::Bounded(range.clone())),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| read_error())?;
        // Refuse the provider's range and complete size before polling its body.
        if response.range != range || response.meta.size != size {
            return Err(mismatch());
        }
        RemoteObjects::fill_metadata(response, destination).await
    }
}

/// A local file and its known identities, ready for one create-only transfer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileUpload {
    pub source: PathBuf,
    pub destination: String,
    pub sha256: String,
    pub md5: String,
    pub size: u64,
    pub content_type: String,
    pub cache_control: String,
}

/// Transport outcome only; an existing destination is not a readback assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateFileResult {
    Created,
    AlreadyExists,
}

/// Exact-key temporary deletion authority for a quiescent namespace generation.
///
/// Construct this only from provider-enforced temporary credentials permitting
/// HeadObject and DeleteObject on exactly `objects`, with the supplied expiry.
/// The controller must stop all hosts and readers, fence old catalog registrations,
/// and permanently forbid key reuse before execution. Expiry prevents new requests;
/// it does not cancel an already accepted deletion. Normal storage credentials are
/// deliberately not accepted without an explicit session token. Supported DELETE
/// calls use this temporary operator; the same process can also hold normal read
/// and catalog credentials. This is not hostile-process credential isolation.
#[derive(Clone)]
pub struct RemoteRemoval {
    objects: RemoteObjects,
    generation: uuid::Uuid,
    exact: Arc<std::collections::BTreeMap<String, ObjectRef>>,
    expires: u64,
}

/// Nonsecret exact authority description and named temporary credential bindings.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteRemovalConfig {
    pub owned_prefix: String,
    pub generation: uuid::Uuid,
    pub objects: Vec<ObjectRef>,
    pub expires: u64,
    pub properties: HashMap<String, String>,
    pub secret_env: HashMap<String, String>,
}
impl RemoteRemovalConfig {
    /// Resolves named temporary credentials without connecting a catalog.
    ///
    /// # Errors
    /// Refuses literal credentials, catalog configuration, missing secrets or authority.
    pub fn resolve(self) -> Result<RemoteRemoval> {
        const SECRETS: [&str; 3] = [
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
        ];
        if self
            .properties
            .keys()
            .any(|key| !key.starts_with("s3.") || SECRETS.contains(&key.as_str()))
            || self
                .secret_env
                .keys()
                .any(|key| !SECRETS.contains(&key.as_str()))
            || SECRETS
                .iter()
                .any(|key| !self.secret_env.contains_key(*key))
        {
            return Err(removal_authority());
        }
        let objects = super::remote_objects(&self.owned_prefix, self.properties, self.secret_env)?;
        RemoteRemoval::new(objects, self.generation, self.objects, self.expires)
    }
}

impl fmt::Debug for RemoteRemoval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteRemoval")
            .field("generation", &self.generation)
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

impl RemoteRemoval {
    /// Binds explicit temporary credentials to an exact removal plan.
    ///
    /// This validates supplied scope; provider policy enforcement is an issuer
    /// responsibility, not inferred from the opaque session token. No requests run.
    ///
    /// # Errors
    /// Refuses absent temporary credentials, invalid identities, duplicate keys,
    /// invalid scope, or expiry outside the next fifteen minutes.
    pub fn new(
        objects: RemoteObjects,
        generation: uuid::Uuid,
        expected: Vec<ObjectRef>,
        expires: u64,
    ) -> Result<Self> {
        let now = removal_now()?;
        if !objects.temporary
            || generation.is_nil()
            || expected.is_empty()
            || expires <= now
            || expires > now.saturating_add(900)
        {
            return Err(removal_authority());
        }
        let mut exact = std::collections::BTreeMap::new();
        for object in expected {
            objects.scope.key(&object.path)?;
            if object.digest.len() != 64
                || !object
                    .digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                || exact.insert(object.path.clone(), object).is_some()
            {
                return Err(removal_authority());
            }
        }
        Ok(Self {
            objects,
            generation,
            exact: Arc::new(exact),
            expires,
        })
    }

    pub(crate) fn check(&self, generation: uuid::Uuid, expected: &[ObjectRef]) -> Result<()> {
        if generation != self.generation
            || removal_now()? >= self.expires
            || expected
                .iter()
                .any(|object| self.exact.get(&object.path) != Some(object))
        {
            return Err(removal_authority());
        }
        Ok(())
    }

    pub(crate) fn check_plan(&self, generation: uuid::Uuid, expected: &[ObjectRef]) -> Result<()> {
        if expected.len() != self.exact.len() {
            return Err(removal_authority());
        }
        self.check(generation, expected)
    }

    pub(crate) async fn remove(
        &self,
        reader: &RemoteObjects,
        expected: &ObjectRef,
        budget: &Budget,
    ) -> Result<crate::storage::removal::LocalRemovalOutcome> {
        self.check(self.generation, std::slice::from_ref(expected))?;
        let key = self.objects.scope.key(&expected.path)?;
        // The identity reader must use the same owned object scope. It keeps
        // ordinary credentials separate from the HEAD/DELETE-only actor.
        if self.objects.placement != reader.placement {
            return Err(removal_authority());
        }
        let reader_key = reader.scope.key(&expected.path)?;
        let memory = budget.reserve(READ_ADMISSION)?;
        let actor = self.clone();
        let reader = reader.clone();
        let expected = expected.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                handle.block_on(async {
                    actor.check(actor.generation, std::slice::from_ref(&expected))?;
                    let metadata = match actor.objects.operator.stat(&key).await {
                        Ok(metadata) => metadata,
                        Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                            return Ok(crate::storage::removal::LocalRemovalOutcome::Absent);
                        }
                        Err(_) => return Err(removal_effect()),
                    };
                    if !metadata.is_file() || metadata.content_length() != expected.bytes {
                        return Err(mismatch());
                    }
                    let (digest, _) = reader
                        .identity_by_key(&reader_key, Some(expected.bytes))
                        .await?;
                    if digest != expected.digest {
                        return Err(mismatch());
                    }
                    actor.check(actor.generation, std::slice::from_ref(&expected))?;
                    actor
                        .objects
                        .operator
                        .delete(&key)
                        .await
                        .map_err(|_| removal_effect())?;
                    Ok(crate::storage::removal::LocalRemovalOutcome::Deleted)
                })
            })
            .await
    }

    pub(crate) async fn absent(&self, expected: &ObjectRef, budget: &Budget) -> Result<bool> {
        self.check(self.generation, std::slice::from_ref(expected))?;
        let key = self.objects.scope.key(&expected.path)?;
        let actor = self.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                handle.block_on(async {
                    if removal_now()? >= actor.expires {
                        return Err(removal_authority());
                    }
                    match actor.objects.operator.stat(&key).await {
                        Ok(_) => Ok(false),
                        Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(true),
                        Err(_) => Err(removal_effect()),
                    }
                })
            })
            .await
    }
}

fn removal_now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| removal_authority())
}
fn removal_authority() -> Error {
    Error::refusal(
        "remote_removal_authority",
        "exact temporary deletion authority is unavailable",
    )
}
fn removal_effect() -> Error {
    Error::refusal(
        "remote_removal_effect",
        "exact remote deletion or absence is unconfirmed",
    )
}

impl fmt::Debug for RemoteObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteObjects").finish_non_exhaustive()
    }
}

static REGISTERED_PREFIXES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, RemoteObjects>>,
> = std::sync::OnceLock::new();

fn registered_prefixes()
-> &'static std::sync::Mutex<std::collections::BTreeMap<String, RemoteObjects>> {
    REGISTERED_PREFIXES.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

fn query_builder(
    properties: &HashMap<String, String>,
    scope: &Scope,
) -> Result<(AmazonS3Builder, String)> {
    let required = |key: &str| {
        properties
            .get(key)
            .filter(|value| !value.is_empty())
            .ok_or_else(config)
    };
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
    let placement = format!(
        "{}\n{}\n{}",
        query_endpoint,
        required("s3.region")?,
        virtual_hosted
    );
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
    Ok((query, placement))
}

impl RemoteObjects {
    pub(crate) fn for_uri(uri: &str) -> Option<Self> {
        if !uri.starts_with("s3://") {
            return None;
        }
        let guard = registered_prefixes().lock().ok()?;
        guard
            .iter()
            .filter(|(prefix, _)| {
                uri == prefix.as_str()
                    || uri
                        .strip_prefix(prefix.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, objects)| objects.clone())
    }

    pub(crate) fn same_placement(&self, other: &Self) -> bool {
        self.placement == other.placement && self.owned_location() == other.owned_location()
    }

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
        let make_builder = || -> Result<S3> {
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
            Ok(builder)
        };
        // Keep multipart users on their existing operator: native S3 refuses
        // MD5 multipart. Whole-buffer creates use a separate native checksum.
        let finish = |builder: S3| -> Result<Operator> {
            Ok(Operator::new(builder)
                .map_err(|_| config())?
                .finish()
                .layer(
                    TimeoutLayer::new()
                        .with_timeout(super::REQUEST_TIMEOUT)
                        .with_io_timeout(super::REQUEST_TIMEOUT),
                ))
        };
        let operator = finish(make_builder()?)?;
        let single_put =
            finish(make_builder()?.checksum_algorithm("md5"))?.layer(HttpClientLayer::new(
                HttpClient::with(ConditionalPutClient(HttpClient::default())),
            ));
        let file_io = FileIOBuilder::new(Arc::new(super::FixedPartS3))
            .with_props(properties.iter().filter(|(key, _)| key.starts_with("s3.")))
            .build();
        let (query, placement) = query_builder(properties, &scope)?;
        let query_store = Arc::new(query.build().map_err(|_| config())?);
        let temporary = properties
            .get("s3.session-token")
            .is_some_and(|token| !token.is_empty());
        let instance = Self {
            operator,
            single_put,
            file_io,
            query_store,
            scope,
            temporary,
            placement,
        };
        if !temporary && let Ok(mut guard) = registered_prefixes().lock() {
            guard.insert(instance.owned_location(), instance.clone());
        }
        Ok(instance)
    }

    pub(crate) fn register_query_store(
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

    pub(in crate::storage) fn flat_reader(
        &self,
        object: &ObjectRef,
    ) -> Result<Box<dyn parquet::arrow::async_reader::AsyncFileReader>> {
        // The complete URI is checked before constructing a native range reader.
        let key = self.scope.key(&object.path)?;
        let path = object_store::path::Path::parse(key).map_err(|_| scope_error())?;
        Ok(Box::new(
            parquet::arrow::async_reader::ParquetObjectReader::new(self.query_store.clone(), path)
                .with_file_size(object.bytes),
        ))
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
        self.single_put
            .write_with(key, bytes)
            .if_not_exists(true)
            .await
            .map_err(write_error)?;
        Ok(())
    }

    /// Creates one known file with native Content-MD5 and no readback.
    ///
    /// The entire file and transfer allowance are reserved before opening or
    /// allocating. The worker retains admission through completion after caller
    /// cancellation. File identities are checked during one local read; OpenDAL
    /// computes the transport checksum. No multipart or automatic retry occurs.
    /// A 412 reports `AlreadyExists`, without claiming the stored bytes match.
    /// Keep the runtime driving I/O until canceled workers finish.
    ///
    /// # Errors
    /// Refuses scope/capacity, changed local bytes and unconfirmed writes.
    pub async fn create_file(
        &self,
        expected: &FileUpload,
        budget: &Budget,
    ) -> Result<CreateFileResult> {
        use sha2::Digest;
        let key = self.scope.key(&expected.destination)?;
        let size = usize::try_from(expected.size)
            .map_err(|_| Error::refusal("capacity", "file transfer size is not addressable"))?;
        let allowance = size
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| Error::refusal("capacity", "file transfer allowance overflow"))?;
        let memory = budget.reserve(allowance)?;
        let expected = expected.clone();
        let objects = self.clone();
        let handle = tokio::runtime::Handle::current();
        budget
            .blocking_admitted(move || {
                let _memory = memory;
                let mut file = File::open(&expected.source)?;
                if file.metadata()?.len() != expected.size {
                    return Err(source_changed());
                }
                let mut bytes = vec![0; size];
                let mut total = 0usize;
                let mut sha = sha2::Sha256::new();
                let mut md5 = md5::Md5::new();
                while total < size {
                    let end = total.saturating_add(65536).min(size);
                    let count = file.read(&mut bytes[total..end])?;
                    if count == 0 {
                        return Err(source_changed());
                    }
                    sha.update(&bytes[total..total + count]);
                    md5.update(&bytes[total..total + count]);
                    total += count;
                }
                if file.read(&mut [0; 1])? != 0
                    || format!("{:x}", sha.finalize()) != expected.sha256
                    || format!("{:x}", md5.finalize()) != expected.md5
                {
                    return Err(source_changed());
                }
                let result = handle.block_on(async {
                    objects
                        .single_put
                        .write_with(&key, bytes)
                        .if_not_exists(true)
                        .content_type(&expected.content_type)
                        .cache_control(&expected.cache_control)
                        .await
                });
                match result {
                    Ok(_) => Ok(CreateFileResult::Created),
                    Err(error)
                        if !error.is_temporary()
                            && matches!(
                                error.kind(),
                                opendal::ErrorKind::AlreadyExists
                                    | opendal::ErrorKind::ConditionNotMatch
                            ) =>
                    {
                        Ok(CreateFileResult::AlreadyExists)
                    }
                    Err(error) => Err(write_error(error)),
                }
            })
            .await
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
        self.scope.location()
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

    #[cfg(all(not(test), not(feature = "test-util")))]
    pub(in crate::storage) async fn stat_identity_admitted(
        &self,
        owner: &crate::model::Id,
        path: &str,
    ) -> Result<ObjectRef> {
        let key = self.scope.key(path)?;
        let bytes = self
            .operator
            .stat(&key)
            .await
            .map_err(|_| read_error())?
            .content_length();
        Ok(ObjectRef {
            owner: owner.clone(),
            path: path.into(),
            digest: blake3::hash(format!("{path}:{bytes}").as_bytes())
                .to_hex()
                .to_string(),
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
        if bytes.len() as u64 != expected.bytes {
            return Err(mismatch());
        }
        let path = object_store::path::Path::parse(key).map_err(|_| scope_error())?;
        let options = object_store::GetOptions {
            range: (expected.bytes != 0)
                .then_some(object_store::GetRange::Bounded(0..expected.bytes)),
            ..Default::default()
        };
        let response = match self.query_store.get_opts(&path, options).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(_) => return Err(read_error()),
        };
        // Content-Range carries the entire object's size on this same GET.
        // Check it before consuming any body or allocating a replacement buffer.
        if response.meta.size != expected.bytes || response.range != (0..expected.bytes) {
            return Err(mismatch());
        }
        Self::fill_metadata(response, bytes).await?;
        Ok(true)
    }

    // One capped GET supplies both observed object size and bytes. The caller
    // holds transport admission; observed bytes are admitted before payload allocation.
    pub(in crate::storage) async fn read_metadata_capped_admitted(
        &self,
        path: &str,
        limit: usize,
        budget: &Budget,
        memory: &mut tokio::sync::OwnedSemaphorePermit,
    ) -> Result<bytes::Bytes> {
        let key = self.scope.key(path)?;
        let path = object_store::path::Path::parse(key).map_err(|_| scope_error())?;
        let end = u64::try_from(limit)
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or_else(mismatch)?;
        let response = self
            .query_store
            .get_opts(
                &path,
                object_store::GetOptions {
                    range: Some(object_store::GetRange::Bounded(0..end)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| read_error())?;
        if response.meta.size > limit as u64 {
            return Err(Error::refusal(
                "metadata_capacity",
                "native metadata exceeds the configured byte limit",
            ));
        }
        if response.range != (0..response.meta.size) {
            return Err(mismatch());
        }
        memory.merge(budget.reserve(usize::try_from(response.meta.size).map_err(|_| mismatch())?)?);
        let mut bytes = vec![0; usize::try_from(response.meta.size).map_err(|_| mismatch())?];
        Self::fill_metadata(response, &mut bytes).await?;
        Ok(bytes.into())
    }
    async fn fill_metadata(response: object_store::GetResult, bytes: &mut [u8]) -> Result<()> {
        let mut total = 0usize;
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(|_| read_error())? {
            let end = total.checked_add(chunk.len()).ok_or_else(mismatch)?;
            bytes
                .get_mut(total..end)
                .ok_or_else(mismatch)?
                .copy_from_slice(&chunk);
            total = end;
        }
        if total != bytes.len() {
            return Err(mismatch());
        }
        Ok(())
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
        #[cfg(any(test, feature = "test-util"))]
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
        #[cfg(all(not(test), not(feature = "test-util")))]
        if total != bytes {
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

    fn location(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix.trim_end_matches('/'))
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

#[cfg(test)]
mod removal_tests;
