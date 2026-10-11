//! Opt-in tiny synthetic qualification of the maintained R2 catalog seam.
//!
//! Retains its synthetic namespace, table, metadata, data, and rejected candidate
//! objects. It never retires objects or changes existing table configuration.

use async_trait::async_trait;
use docspec::{
    Error, Result,
    budget::{Budget, Limits},
    catalog::{Lifecycle, record},
    model::Id,
    storage::remote::remote_catalog,
};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, table::Table,
};
use serde_json::json;
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Upstream error bodies may include provider details; routine output is bounded.
        eprintln!("r2_catalog_probe failed: {}", error.code());
        std::process::exit(1);
    }
}

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| Error::refusal("remote_config", "required probe configuration is missing"))
}

async fn connect() -> Result<Arc<dyn Catalog>> {
    remote_catalog(
        HashMap::from([
            ("uri".into(), env("DOCSPEC_CATALOG_URI")?),
            ("warehouse".into(), env("DOCSPEC_CATALOG_WAREHOUSE")?),
            ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::from([
            ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_S3_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await
}

async fn run() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 2 || arguments[0] != "--allow-synthetic-writes" {
        return Err(Error::refusal(
            "probe_opt_in",
            "supply --allow-synthetic-writes and a fresh qualification UUID",
        ));
    }
    let suffix = arguments[1]
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("probe_dataset", "qualification dataset prefix required"))?;
    let uuid = uuid::Uuid::parse_str(suffix)
        .map_err(|_| Error::refusal("probe_dataset", "qualification UUID required"))?;
    if uuid.to_string() != suffix {
        return Err(Error::refusal(
            "probe_dataset",
            "canonical qualification UUID required",
        ));
    }
    let dataset = Id::new(arguments[1].clone())?;
    let namespace = NamespaceIdent::new(format!(
        "d_{}_{}",
        dataset.as_str().replace('-', "_"),
        &blake3::hash(dataset.as_str().as_bytes()).to_hex()[..12]
    ));
    let ident = TableIdent::new(namespace.clone(), "lifecycle".into());
    let catalog = connect().await?;
    if catalog.namespace_exists(&namespace).await? {
        return Err(Error::refusal(
            "probe_existing_namespace",
            "use a fresh qualification UUID",
        ));
    }
    // Emit the exact reference before writes so partial failures remain discoverable.
    println!(
        "{}",
        json!({"dataset": dataset, "table": ident.to_string(), "status": "starting"})
    );
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 16,
        max_batch_bytes: 4096,
        max_metadata_bytes: 1 << 20,
    })?;
    let faults = Arc::new(ProbeCatalog {
        inner: catalog.clone(),
        dataset: dataset.clone(),
        budget: budget.clone(),
        fault: AtomicU8::new(0),
        server_conflict: AtomicBool::new(false),
    });
    let lifecycle = Lifecycle::new(faults.clone(), budget.clone());
    let initial = lifecycle.open(&dataset).await?;
    let seeded = lifecycle
        .commit(&initial, vec![record("probe/seed", "seed")?])
        .await?;

    // Competing write runs after GuardedCatalog's final load, before REST update.
    faults.fault.store(1, Ordering::SeqCst);
    if lifecycle
        .commit(&seeded, vec![record("probe/rejected", "must-not-select")?])
        .await
        .is_ok()
        || !faults.server_conflict.load(Ordering::SeqCst)
    {
        return Err(Error::refusal(
            "probe_expected_snapshot",
            "server expected-snapshot conflict was not observed",
        ));
    }
    let winner = lifecycle.open(&dataset).await?;
    if winner.revision != seeded.revision + 1
        || lifecycle
            .get::<String>(&winner, "probe/winner")
            .await?
            .as_deref()
            != Some("winner")
        || lifecycle
            .get::<String>(&winner, "probe/rejected")
            .await?
            .is_some()
    {
        return Err(Error::refusal(
            "probe_conflict_readback",
            "conflicting commit changed selected rows",
        ));
    }

    faults.fault.store(2, Ordering::SeqCst);
    if lifecycle
        .commit(&winner, vec![record("probe/lost-response", suffix)?])
        .await
        .is_ok()
    {
        return Err(Error::refusal(
            "probe_response_loss",
            "response-loss injection did not occur",
        ));
    }
    // A fresh client and lifecycle cache must recover the exact committed identity.
    let reopened_catalog = connect().await?;
    let reopened = Lifecycle::new(reopened_catalog.clone(), budget.clone());
    let recovered = reopened.open(&dataset).await?;
    let rows = reopened.records(&recovered).await?;
    let expected = std::collections::BTreeMap::from([
        ("probe/seed".into(), json!("seed")),
        ("probe/winner".into(), json!("winner")),
        ("probe/lost-response".into(), json!(suffix)),
    ]);
    if rows != expected || recovered.revision != winner.revision + 1 {
        return Err(Error::refusal(
            "probe_reconcile",
            "fresh native read did not recover exact committed rows",
        ));
    }
    let table = reopened_catalog.load_table(&ident).await?;
    println!(
        "{}",
        json!({
            "dataset": dataset, "table": ident.to_string(),
            "metadata_location": table.metadata_location(),
            "snapshot_id": table.metadata().current_snapshot_id(),
            "revision": recovered.revision, "expected_rows": expected,
            "server_expected_snapshot_conflict": true,
            "injected_lost_response_reconciled": true,
            "fresh_native_reader_exact_rows": true, "status": "passed",
            "retained_synthetic_objects": true,
            "scope": "catalog and lifecycle seam; full remote engine remains unqualified"
        })
    );
    Ok(())
}

// Faults occur around actual maintained remote writes, never in a simulated store.
struct ProbeCatalog {
    inner: Arc<dyn Catalog>,
    dataset: Id,
    budget: Budget,
    // 0: ordinary; 1: compete immediately before commit; 2: discard successful reply.
    fault: AtomicU8,
    server_conflict: AtomicBool,
}
impl fmt::Debug for ProbeCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeCatalog").finish_non_exhaustive()
    }
}
#[async_trait]
impl Catalog for ProbeCatalog {
    async fn update_table(&self, c: TableCommit) -> iceberg::Result<Table> {
        let fault = self.fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            let winner = Lifecycle::new(self.inner.clone(), self.budget.clone());
            let view = winner.open(&self.dataset).await.map_err(|_| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "probe competing open failed",
                )
            })?;
            winner
                .commit(&view, vec![("probe/winner".into(), json!("winner"))])
                .await
                .map_err(|_| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        "probe competing commit failed",
                    )
                })?;
        }
        let result = self.inner.update_table(c).await;
        if fault == 1
            && result
                .as_ref()
                .err()
                .is_some_and(|error| error.kind() == iceberg::ErrorKind::CatalogCommitConflicts)
        {
            self.server_conflict.store(true, Ordering::SeqCst);
        }
        let table = result?;
        if fault == 2 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "probe discarded successful commit response",
            )
            .with_retryable(false));
        }
        Ok(table)
    }
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
}

// Rust guideline compliant 2026-02-21
