//! A committed source snapshot survives a newer admission before phase acknowledgment.
// Rust guideline compliant 2026-02-21
#![cfg(feature = "test-util")]

use super::scalar;

use async_trait::async_trait;
use docspec::{
    Engine,
    engine::verify_release,
    model::{Authority, PhaseResult, RunSpec, UpdateSpec},
    storage,
};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, table::Table,
};
use scalar::{fixture, id, input, limits, spec};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::oneshot;

// Small correctness fixtures must fail instead of hanging when a handoff is lost.
// This deadline is not a throughput acceptance threshold.
const HANDOFF_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Pause {
    data: bool,
    lost_response: bool,
    committed: oneshot::Sender<Table>,
    release: oneshot::Receiver<()>,
}

/// Delays one native data commit response while forwarding real catalog behavior.
#[derive(Debug)]
struct PausedCatalog {
    inner: Arc<dyn Catalog>,
    next: Mutex<Option<Pause>>,
}

impl PausedCatalog {
    fn arm(&self) -> (oneshot::Receiver<Table>, oneshot::Sender<()>) {
        self.arm_kind(true)
    }

    fn arm_kind(&self, data: bool) -> (oneshot::Receiver<Table>, oneshot::Sender<()>) {
        self.arm_response(data, false)
    }

    fn arm_response(
        &self,
        data: bool,
        lost_response: bool,
    ) -> (oneshot::Receiver<Table>, oneshot::Sender<()>) {
        let (committed, observed) = oneshot::channel();
        let (release, resumed) = oneshot::channel();
        let mut next = self.next.lock().unwrap();
        assert!(
            next.is_none(),
            "the previous native commit pause is still armed"
        );
        *next = Some(Pause {
            data,
            lost_response,
            committed,
            release: resumed,
        });
        (observed, release)
    }
}

#[async_trait]
impl Catalog for PausedCatalog {
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let is_data = commit.identifier().name() != "lifecycle";
        let committed = self.inner.update_table(commit).await?;
        let pause = {
            let mut next = self.next.lock().unwrap();
            if next.as_ref().is_some_and(|pause| pause.data == is_data) {
                next.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            // The real native commit has completed. The engine has not received
            // its table yet, so it cannot have acknowledged the source phase.
            pause.committed.send(committed.clone()).map_err(|_| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "test owner dropped the native commit receiver",
                )
                .with_retryable(false)
            })?;
            tokio::time::timeout(HANDOFF_DEADLINE, pause.release)
                .await
                .map_err(|_| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        "test owner did not release the committed source",
                    )
                    .with_retryable(false)
                })?
                .map_err(|_| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        "test owner dropped the native commit release",
                    )
                    .with_retryable(false)
                })?;
            if pause.lost_response {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "injected lost acknowledgment after the native commit",
                )
                .with_retryable(false));
            }
        }
        Ok(committed)
    }

    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(ident).await
    }
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn drop_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(ident).await
    }
    async fn purge_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(ident).await
    }
    async fn table_exists(&self, ident: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(ident).await
    }
    async fn rename_table(
        &self,
        source: &TableIdent,
        destination: &TableIdent,
    ) -> iceberg::Result<()> {
        self.inner.rename_table(source, destination).await
    }
    async fn register_table(&self, ident: &TableIdent, metadata: String) -> iceberg::Result<Table> {
        self.inner.register_table(ident, metadata).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_workers_cannot_acknowledge_late_native_source_or_group_commits() {
    for backfill in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        drop(engine);
        let workspace = root.path().join("workspace");
        let catalog = Arc::new(PausedCatalog {
            inner: storage::local_catalog(&workspace).await.unwrap(),
            next: Mutex::new(None),
        });
        let engine = Engine::with_catalog(&workspace, catalog.clone(), limits())
            .await
            .unwrap();
        let source = input(&root.path().join("rebuild.parquet"), &[(Some(9000), None)]);
        let request = RunSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("stopped"),
            base: Some(base.id.clone()),
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: base.dataset.definition.clone(),
        };
        let group = spec(&base, "stopped", "lengths", "text_length", "length(text)");
        let (committed, release) = catalog.arm();
        let worker = {
            let engine = engine.clone();
            let request = request.clone();
            tokio::spawn(async move {
                if backfill {
                    engine.backfill(&group).await
                } else {
                    engine.run(&request).await
                }
            })
        };
        let native = tokio::time::timeout(HANDOFF_DEADLINE, committed)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            HANDOFF_DEADLINE,
            engine.abandon(
                &request.dataset,
                &request.operation,
                "withdrawn during write",
            ),
        )
        .await
        .expect("abandonment must not wait for native I/O")
        .unwrap();
        let after_abandonment = engine.inspect(&request.dataset).await.unwrap();
        release.send(()).unwrap();
        let error = tokio::time::timeout(HANDOFF_DEADLINE, worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), "operation_abandoned");
        assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
        assert_eq!(
            engine.inspect(&request.dataset).await.unwrap(),
            after_abandonment,
            "late completion must not add lifecycle records"
        );
        // Abandonment is a logical fence, not permission to destroy files from
        // the write that had already started. Its reservation is still retained.
        assert!(Path::new(native.metadata_location_result().unwrap()).exists());
        assert!(after_abandonment.iter().any(|(key, value)| {
            key.starts_with("protection/")
                && value["purpose"]
                    .as_str()
                    .is_some_and(|purpose| purpose.starts_with("stopped/"))
                && value["retired"] == false
        }));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_abandonment_response_reconciles_without_another_native_commit() {
    use docspec::{budget::Budget, catalog::Lifecycle};
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "stopped", "lengths", "text_length", "length(text)");
    assert_eq!(
        engine
            .backfill_until(&request, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    drop(engine);
    let workspace = root.path().join("workspace");
    let catalog = Arc::new(PausedCatalog {
        inner: storage::local_catalog(&workspace).await.unwrap(),
        next: Mutex::new(None),
    });
    let engine = Engine::with_catalog(&workspace, catalog.clone(), limits())
        .await
        .unwrap();
    let (committed, release) = catalog.arm_kind(false);
    let worker = {
        let engine = engine.clone();
        let request = request.clone();
        tokio::spawn(async move {
            engine
                .abandon(&request.dataset, &request.operation, "withdrawn")
                .await
        })
    };
    let native = tokio::time::timeout(HANDOFF_DEADLINE, committed)
        .await
        .unwrap()
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(release);
    let decision = engine
        .abandon(&request.dataset, &request.operation, "withdrawn")
        .await
        .unwrap();
    assert_eq!(decision.operation, request.operation);
    let lifecycle = Lifecycle::new(catalog, Budget::new(limits()).unwrap());
    assert_eq!(
        lifecycle.open(&request.dataset).await.unwrap().revision,
        native.metadata().last_sequence_number()
    );
    assert_eq!(
        engine.backfill(&request).await.unwrap_err().code(),
        "operation_abandoned"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selection_wins_over_abandonment_even_when_its_response_is_lost() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let request = spec(&base, "finished", "lengths", "text_length", "length(text)");
    assert_eq!(
        engine
            .backfill_until(&request, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    drop(engine);
    let workspace = root.path().join("workspace");
    let catalog = Arc::new(PausedCatalog {
        inner: storage::local_catalog(&workspace).await.unwrap(),
        next: Mutex::new(None),
    });
    let engine = Engine::with_catalog(&workspace, catalog.clone(), limits())
        .await
        .unwrap();
    let (committed, release) = catalog.arm_kind(false);
    let worker = {
        let engine = engine.clone();
        let request = request.clone();
        tokio::spawn(async move { engine.backfill(&request).await })
    };
    tokio::time::timeout(HANDOFF_DEADLINE, committed)
        .await
        .unwrap()
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(release);
    assert_eq!(
        engine
            .abandon(&request.dataset, &request.operation, "too late")
            .await
            .unwrap_err()
            .code(),
        "operation_completed"
    );
    let selected = engine.latest(&request.dataset).await.unwrap();
    assert_eq!(selected.id, request.operation);
    assert_eq!(engine.backfill(&request).await.unwrap(), selected);
    assert!(
        !engine
            .inspect(&request.dataset)
            .await
            .unwrap()
            .contains_key("abandoned/finished")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_admission_preserves_finished_source_before_acknowledgment_and_rebase_reuses_it() {
    let root = TempDir::new().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let base = engine
        .backfill(&spec(
            &base,
            "enriched",
            "length",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    drop(engine);

    let workspace = root.path().join("workspace");
    let catalog = Arc::new(PausedCatalog {
        inner: storage::local_catalog(&workspace).await.unwrap(),
        next: Mutex::new(None),
    });
    let engine = Engine::with_catalog(&workspace, catalog.clone(), limits())
        .await
        .unwrap();
    let source = input(
        &root.path().join("rebuild.parquet"),
        &[
            (Some(1), Some("replacement".into())),
            (Some(1), Some("replacement".into())),
            (None, None),
        ],
    );
    let request = RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id("paused-rebuild"),
        base: Some(base.id.clone()),
        relationship: None,
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
    };
    let (committed, release) = catalog.arm();
    let worker = {
        let engine = engine.clone();
        let request = request.clone();
        tokio::spawn(async move { engine.run(&request).await })
    };
    let committed = tokio::time::timeout(HANDOFF_DEADLINE, committed)
        .await
        .expect("source commit must reach the deterministic pause")
        .expect("source worker must send its committed native table");
    let before = engine.inspect(&request.dataset).await.unwrap();
    assert!(!before.values().any(|value| {
        serde_json::from_value::<PhaseResult>(value.clone())
            .is_ok_and(|phase| phase.operation == request.operation && phase.phase == "processing")
    }));
    let original_authority: Authority =
        serde_json::from_value(before["authority"].clone()).unwrap();

    let newer = UpdateSpec {
        dataset: request.dataset.clone(),
        operation: id("newer-admission"),
        definition: request.definition.clone(),
        base: base.id.clone(),
        keys: vec!["id".into()],
        replacements: input(
            &root.path().join("newer.parquet"),
            &[(
                Some(9000),
                Some("accepted while source response is paused".into()),
            )],
        ),
        deletions: None,
    };
    let admission =
        tokio::time::timeout(HANDOFF_DEADLINE, engine.update_until(&newer, "admission"))
            .await
            .expect("newer admission must not wait for the paused source response");
    assert_eq!(admission.unwrap_err().code(), "injected_failure");
    let advanced: Authority = serde_json::from_value(
        engine.inspect(&request.dataset).await.unwrap()["authority"].clone(),
    )
    .unwrap();
    assert!(advanced.boundary > original_authority.boundary);
    assert_eq!(advanced.eligibility, original_authority.eligibility);
    release.send(()).expect("source worker must remain paused");
    let error = tokio::time::timeout(HANDOFF_DEADLINE, worker)
        .await
        .expect("released source worker must finish")
        .expect("source worker must not panic")
        .unwrap_err();
    assert_eq!(error.code(), "stale_source_boundary");
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);

    let retained = engine.inspect(&request.dataset).await.unwrap();
    let (phase_key, source) = retained
        .iter()
        .find_map(|(key, value)| {
            serde_json::from_value::<PhaseResult>(value.clone())
                .ok()
                .filter(|phase| phase.operation == request.operation && phase.phase == "processing")
                .map(|phase| (key.clone(), phase))
        })
        .expect("finished source work must be retained under its original qualification");
    assert_eq!(source.eligibility, original_authority.eligibility);
    assert_eq!(
        source.request_digest,
        before[&format!("operation/{}", request.operation)]["request_digest"]
            .as_str()
            .unwrap()
    );
    assert_eq!(source.rows, 3);
    let source_pin = source.table.unwrap();
    assert_eq!(
        source_pin.metadata.path,
        committed.metadata_location_result().unwrap()
    );
    assert_eq!(
        source_pin.snapshot,
        committed.metadata().current_snapshot_id()
    );
    assert!(!retained.contains_key(&format!("result/{}", request.operation)));

    let newer = engine.update(&newer).await.unwrap();
    let mut rebased = request;
    rebased.operation = id("rebased-rebuild");
    rebased.base = Some(newer.id.clone());
    std::fs::remove_file(&rebased.input).unwrap();
    let plan = engine.plan(&rebased).await.unwrap();
    assert!(plan.reusable_phases.contains(&"processing".into()));
    let result = engine.run(&rebased).await.unwrap();
    assert_eq!(result.dataset.groups["source"], source_pin);
    assert_eq!(result.dataset.parent_manifest, Some(newer.manifest));
    assert_eq!(engine.latest(&rebased.dataset).await.unwrap(), result);
    verify_release(&result, limits()).await.unwrap();
    let after = engine.inspect(&rebased.dataset).await.unwrap();
    assert_eq!(after[&phase_key], retained[&phase_key]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnected_run_keeps_native_commit_custody_and_finishes_beneath_shutdown() {
    for lost_response in [false, true] {
        let root = TempDir::new().unwrap();
        let (engine, base, _) = fixture(root.path(), false).await;
        drop(engine);
        let workspace = root.path().join("workspace");
        let catalog = Arc::new(PausedCatalog {
            inner: storage::local_catalog(&workspace).await.unwrap(),
            next: Mutex::new(None),
        });
        let engine = Engine::with_catalog(&workspace, catalog.clone(), limits())
            .await
            .unwrap();
        let source = input(
            &root.path().join("replacement.parquet"),
            &[(Some(4), Some("after disconnect".into()))],
        );
        let request = RunSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("disconnected"),
            base: Some(base.id.clone()),
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: base.dataset.definition.clone(),
        };
        // Success pauses the data commit before the nested historical release
        // lookup. Error pauses an actual lifecycle commit before its lost response.
        let (committed, release) = catalog.arm_response(!lost_response, lost_response);
        let caller = {
            let engine = engine.clone();
            let request = request.clone();
            tokio::spawn(async move { engine.run(&request).await })
        };
        tokio::time::timeout(HANDOFF_DEADLINE, committed)
            .await
            .unwrap()
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(
            Engine::open(&workspace, limits()).await.unwrap_err().code(),
            "workspace_busy"
        );
        let mut drain = Box::pin(engine.shutdown());
        assert!(futures::poll!(&mut drain).is_pending());
        assert_eq!(
            engine.inspect(&request.dataset).await.unwrap_err().code(),
            "host_closing"
        );
        release
            .send(())
            .expect("the native operation must survive caller cancellation");
        tokio::time::timeout(HANDOFF_DEADLINE, drain)
            .await
            .unwrap()
            .unwrap();
        drop(engine);
        let engine = Engine::open(&workspace, limits()).await.unwrap();
        let plan = engine.plan(&request).await.unwrap();
        assert_eq!(plan.completed, !lost_response);
        let before = engine.inspect(&request.dataset).await.unwrap();
        let operation = before[&format!("operation/{}", request.operation)].clone();
        let result = engine.run(&request).await.unwrap();
        assert_eq!(result.id, request.operation);
        assert_eq!(
            engine.inspect(&request.dataset).await.unwrap()
                [&format!("operation/{}", request.operation)],
            operation
        );
        verify_release(&result, limits()).await.unwrap();
    }
}
