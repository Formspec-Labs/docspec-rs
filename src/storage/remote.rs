//! Maintained REST catalog and S3 storage configured at application startup.
//!
//! This adapter connects native catalog reads and conditional object primitives.
//! Source admission, exports, restore, and removal still need remote qualification.
//! Secrets are resolved from named environment variables without logging values.

use crate::{Error, Result};
use async_trait::async_trait;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    table::Table,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

pub mod objects;
#[doc(inline)]
pub use objects::RemoteObjects;

/// Startup connections sharing one resolved configuration.
#[derive(Clone)]
pub struct RemoteStorage {
    pub catalog: Arc<dyn Catalog>,
    pub objects: RemoteObjects,
}

impl RemoteStorage {
    /// Creates immutable native metadata and registers its exact catalog reference.
    ///
    /// The caller retains the exact native `metadata_location` before effects for
    /// unknown-result reconciliation. Its canonical URI must be directly beneath
    /// the metadata's table location in this object's owned scope.
    /// The caller creates the namespace and protects the owned attempt location
    /// and metadata dependencies before calling. The shared admitted worker owns
    /// metadata through creation, including after caller cancellation. Keep the
    /// runtime driving I/O until any detached worker finishes. Retain the
    /// immutable object if catalog registration fails; this method neither retries
    /// nor cleans up. Cancellation during creation does not start registration,
    /// but cancellation or response loss during catalog registration may leave a
    /// committed registration. The caller reconciles the retained identifier and
    /// exact metadata URI after an unknown registration outcome.
    /// The returned owned table uses this connection's object configuration and
    /// the current native runtime. If its returned location fails validation,
    /// registration may already exist; reconcile the original identifier and URI.
    /// The caller retains the returned metadata and registration
    /// for its intended availability lifetime. No DocSpec lifecycle selection or
    /// remote Engine operation is performed.
    ///
    /// # Errors
    /// Refuses compressed metadata, mismatched native destinations, metadata
    /// capacity, destinations outside this object's owned scope,
    /// competing creation, unconfirmed remote writes and catalog registration failure.
    pub async fn register_candidate(
        &self,
        identifier: &TableIdent,
        metadata_location: iceberg::MetadataLocation,
        metadata: iceberg::spec::TableMetadata,
        budget: &crate::budget::Budget,
    ) -> Result<Table> {
        super::register_candidate(
            self.catalog.as_ref(),
            identifier,
            super::CandidateWriter::Remote(self.objects.clone(), Some(metadata_location)),
            move || Ok(metadata),
            budget,
        )
        .await
    }
}

impl fmt::Debug for RemoteStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteStorage").finish_non_exhaustive()
    }
}

/// Initializes catalog and conditional object I/O from one secret resolution.
///
/// `owned_prefix` is a strict `s3://bucket/nonempty-prefix/` URI. This connects
/// object primitives only; it does not qualify the complete remote engine.
///
/// # Errors
/// Refuses invalid configuration, unavailable secrets, or invalid object scope.
pub async fn remote_storage(
    owned_prefix: &str,
    properties: HashMap<String, String>,
    secret_env: HashMap<String, String>,
) -> Result<RemoteStorage> {
    let properties = resolve_properties(properties, secret_env)?;
    let objects = RemoteObjects::from_resolved(&properties, owned_prefix)?;
    let catalog = catalog_from_resolved(properties).await?;
    Ok(RemoteStorage { catalog, objects })
}

/// Initializes conditional object I/O without connecting a catalog.
///
/// Uses the same property and secret resolution as [`remote_storage`].
///
/// # Errors
/// Refuses invalid configuration, unavailable secrets, or invalid object scope.
pub fn remote_objects(
    owned_prefix: &str,
    properties: HashMap<String, String>,
    secret_env: HashMap<String, String>,
) -> Result<RemoteObjects> {
    RemoteObjects::from_resolved(&resolve_properties(properties, secret_env)?, owned_prefix)
}

// Bound metadata control requests while allowing remote connection setup.
// These limits cover REST requests, not whole operations or bulk S3 transfers.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Connects maintained REST and S3 adapters using explicit properties and secret references.
///
/// `properties` contains Iceberg REST/S3 properties such as `uri`, `warehouse`,
/// `s3.endpoint`, and `s3.region`. Each `secret_env` entry maps an Iceberg property
/// (for example `token`) to an environment-variable name, never its value.
/// Local properties override server-vended table configuration in the maintained
/// REST adapter. Keep headers redacted and avoid ambient credential discovery.
/// REST requests allow 15 seconds to connect and 60 seconds overall.
/// Catalog construction is lazy; the first catalog operation checks connectivity.
///
/// # Errors
/// Refuses missing required properties, unavailable secrets, and initialization failures.
pub async fn remote_catalog(
    properties: HashMap<String, String>,
    secret_env: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>> {
    catalog_from_resolved(resolve_properties(properties, secret_env)?).await
}

fn resolve_properties(
    mut properties: HashMap<String, String>,
    secret_env: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    for (property, variable) in secret_env {
        let value = std::env::var(variable)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::refusal("remote_secret", "a referenced secret is unavailable"))?;
        properties.insert(property, value);
    }
    properties.insert("disable-header-redaction".into(), "false".into());
    properties.insert("s3.disable-ec2-metadata".into(), "true".into());
    properties.insert("s3.disable-config-load".into(), "true".into());
    Ok(properties)
}

async fn catalog_from_resolved(properties: HashMap<String, String>) -> Result<Arc<dyn Catalog>> {
    for key in ["uri", "warehouse"] {
        if properties.get(key).is_none_or(|value| value.is_empty()) {
            return Err(Error::refusal(
                "remote_config",
                "catalog URI and warehouse are required",
            ));
        }
    }
    let client = reqwest_rest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| Error::refusal("remote_config", "native HTTP client initialization failed"))?;
    let inner = RestCatalogBuilder::default()
        .with_client(client)
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("docspec", properties)
        .await
        .map_err(|_| Error::refusal("remote_config", "native catalog initialization failed"))?;
    Ok(Arc::new(RemoteCatalog { inner }))
}

// Upstream RestCatalog derives Debug over configuration containing secrets.
// Preserve Catalog's required Debug API without exposing that configuration.
struct RemoteCatalog {
    inner: RestCatalog,
}

impl fmt::Debug for RemoteCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteCatalog").finish_non_exhaustive()
    }
}

#[async_trait]
impl Catalog for RemoteCatalog {
    async fn list_namespaces(
        &self,
        p: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(n).await
    }
    async fn update_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> iceberg::Result<Table> {
        self.inner.create_table(n, c).await
    }
    async fn load_table(&self, t: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(t).await
    }
    async fn drop_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> iceberg::Result<Table> {
        self.inner.register_table(t, m).await
    }
    async fn update_table(&self, c: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(c).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn debug_redacts_dummy_secrets_without_network_requests() {
        let secret = "dummy-secret-must-not-appear";
        let catalog = remote_catalog(
            HashMap::from([
                ("uri".into(), "https://catalog.invalid".into()),
                ("warehouse".into(), "test-warehouse".into()),
                ("token".into(), secret.into()),
                ("s3.secret-access-key".into(), secret.into()),
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();
        let debug = format!("{catalog:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("RemoteCatalog"));
    }

    #[tokio::test]
    async fn missing_required_configuration_is_refused_before_connecting() {
        for properties in [
            HashMap::new(),
            HashMap::from([("uri".into(), "https://catalog.invalid".into())]),
            HashMap::from([
                ("uri".into(), "https://catalog.invalid".into()),
                ("warehouse".into(), String::new()),
            ]),
        ] {
            let error = remote_catalog(properties, HashMap::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), "remote_config");
        }
    }
}

// Rust guideline compliant 2026-02-21
