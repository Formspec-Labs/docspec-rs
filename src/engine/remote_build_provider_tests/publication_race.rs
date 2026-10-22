//! Same-head publication through independent clients or isolated provider processes.
mod epoch_handoff;
mod processes;
use super::*;
use crate::engine::export::ExportMaterialized;
use crate::export::{
    ExportField, ExportPin, ExportPolicy, ExportRows, ExportSpec, ExportValidation,
};
use async_trait::async_trait;
use iceberg::{Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, table::Table};
use std::{
    fmt,
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

// Two prepared candidates only: no retries, polling or repeated races. A missing
// peer must stop the held commit instead of hanging the provider qualification.
const HOLD_DEADLINE: Duration = Duration::from_secs(60);

struct Rendezvous {
    barrier: tokio::sync::Barrier,
    metadata: StdMutex<Vec<String>>,
    files: Option<(std::path::PathBuf, String)>,
}

pub(super) struct RaceCatalog {
    inner: Arc<dyn Catalog>,
    lifecycle: TableIdent,
    armed: AtomicBool,
    loaded: StdMutex<Option<String>>,
    rendezvous: Arc<Rendezvous>,
}

impl fmt::Debug for RaceCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaceCatalog").finish_non_exhaustive()
    }
}

impl RaceCatalog {
    fn pair(first: Arc<dyn Catalog>, second: Arc<dyn Catalog>, dataset: &Id) -> [Arc<Self>; 2] {
        let rendezvous = Arc::new(Rendezvous {
            barrier: tokio::sync::Barrier::new(2),
            metadata: StdMutex::new(Vec::new()),
            files: None,
        });
        [first, second].map(|inner| {
            Arc::new(Self {
                inner,
                lifecycle: crate::catalog::lifecycle_ident(dataset),
                armed: AtomicBool::new(false),
                loaded: StdMutex::new(None),
                rendezvous: rendezvous.clone(),
            })
        })
    }

    pub(super) fn arm(&self) {
        *self.loaded.lock().unwrap() = None;
        assert!(!self.armed.swap(true, Ordering::SeqCst));
    }
}

#[cfg(feature = "test-util")]
pub(super) fn held_generation_catalog(
    inner: Arc<dyn Catalog>,
    lifecycle: TableIdent,
    directory: &Path,
) -> Arc<RaceCatalog> {
    Arc::new(RaceCatalog {
        inner,
        lifecycle,
        armed: AtomicBool::new(false),
        loaded: StdMutex::new(None),
        rendezvous: Arc::new(Rendezvous {
            barrier: tokio::sync::Barrier::new(1),
            metadata: StdMutex::new(Vec::new()),
            files: Some((directory.to_owned(), "maintenance-old".into())),
        }),
    })
}

#[async_trait]
impl Catalog for RaceCatalog {
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let table = self.inner.load_table(ident).await?;
        if table.identifier() == &self.lifecycle {
            *self.loaded.lock().unwrap() = Some(table.metadata_location_result()?.to_owned());
        }
        Ok(table)
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let held =
            commit.identifier() == &self.lifecycle && self.armed.swap(false, Ordering::SeqCst);
        if held {
            let metadata = self.loaded.lock().unwrap().clone().ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "held commit has no observed preflight metadata",
                )
                .with_retryable(false)
            })?;
            {
                let mut observed = self.rendezvous.metadata.lock().unwrap();
                if observed.len() >= 2 || observed.first().is_some_and(|prior| prior != &metadata) {
                    return Err(iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        "publication clients did not reach the same captured metadata",
                    )
                    .with_retryable(false));
                }
                observed.push(metadata.clone());
            }
            if let Some((directory, side)) = &self.rendezvous.files {
                processes::hold(directory, side, &metadata).await?;
            } else {
                tokio::time::timeout(HOLD_DEADLINE, self.rendezvous.barrier.wait())
                    .await
                    .map_err(|_| {
                        iceberg::Error::new(
                            iceberg::ErrorKind::Unexpected,
                            "publication peer did not reach its commit boundary",
                        )
                        .with_retryable(false)
                    })?;
            }
        }
        // Forward the original maintained commit, including all requirements.
        // No synthetic result, metadata rewrite, provider retry or fault response.
        let result = self.inner.update_table(commit).await;
        if held
            && let Some((directory, side)) = &self.rendezvous.files
            && side == "epoch-a"
        {
            epoch_handoff::native_outcome(directory, &result)?;
        }
        if held
            && let Some((directory, side)) = &self.rendezvous.files
            && side == "epoch-b"
            && let Ok(table) = &result
        {
            epoch_handoff::hold_selected(directory, table).await?;
        }
        #[cfg(feature = "test-util")]
        if held
            && let Some((directory, side)) = &self.rendezvous.files
            && side == "maintenance-old"
        {
            super::maintenance::native_outcome(directory, &result)?;
        }
        result
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

fn limits() -> Limits {
    Limits {
        compute_threads: 1,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
    }
}

fn input(path: &Path, keys: Vec<i64>, texts: Vec<Option<&str>>) -> Result<InputSpec> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("counter", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(texts)),
            Arc::new(Int64Array::from(keys)),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path)?.0,
    })
}

fn request(source: &ReleasePin, operation: &str, expected: Option<Id>) -> Result<ExportSpec> {
    Ok(ExportSpec {
        dataset: source.dataset.dataset.clone(),
        operation: Id::new(operation)?,
        source: source.version_ref(),
        channel: Id::new("public")?,
        expected,
        policy: ExportPolicy {
            group: "source".into(),
            rows: ExportRows::All,
            columns: vec!["counter".into(), "text".into()],
            ..ExportPolicy::default()
        },
    })
}

#[derive(Debug, PartialEq, Eq)]
struct Completion {
    materialized_revision: i64,
    materialized: ExportMaterialized,
    validation_revision: i64,
    validation: ExportValidation,
}

async fn completion(engine: &Engine, spec: &ExportSpec) -> Result<Completion> {
    let lifecycle = &engine.inner.lifecycle;
    let view = engine.existing_lifecycle_view(&spec.dataset).await?;
    let rows = lifecycle.rows(&view).await?;
    let materialized_key = format!("export-materialized/{}", spec.operation);
    let validation_key = format!("export-validation/{}", spec.operation);
    let _memory = rows.reserve_decoded(&lifecycle.budget, &[&materialized_key, &validation_key])?;
    let (materialized_revision, _, materialized) =
        rows.get_record(&materialized_key)?.ok_or_else(|| {
            Error::refusal(
                "provider_evidence",
                "materialized race candidate is missing",
            )
        })?;
    let (validation_revision, _, validation) =
        rows.get_record(&validation_key)?.ok_or_else(|| {
            Error::refusal("provider_evidence", "validated race candidate is missing")
        })?;
    Ok(Completion {
        materialized_revision,
        materialized,
        validation_revision,
        validation,
    })
}

struct Outcome {
    old: ExportPin,
    winner: ExportPin,
    current_source: ReleasePin,
    losing_object: ObjectRef,
}

async fn race(
    first: &Engine,
    second: &Engine,
    catalogs: &[Arc<RaceCatalog>; 2],
    dataset: &Id,
    producer: &Path,
    process_scope: Option<(&Path, &str)>,
) -> Result<Outcome> {
    let original = input(
        &producer.join("initial.parquet"),
        vec![1, 2, 3],
        vec![Some("old"), None, Some("")],
    )?;
    let initial = first
        .run(&RunSpec {
            dataset: dataset.clone(),
            operation: Id::new("initial")?,
            base: None,
            relationship: None,
            input: original.path.clone(),
            expected_digest: original.digest,
            sql: "SELECT * FROM input".into(),
            definition: "publication-race-v1".into(),
        })
        .await?;
    std::fs::remove_file(&original.path)?;
    let old = first.export(&request(&initial, "old", None)?).await?;
    let replacement = input(
        &producer.join("replacement.parquet"),
        vec![1, 1],
        vec![Some("new"), Some("new")],
    )?;
    let updated = first
        .update(&UpdateSpec {
            dataset: dataset.clone(),
            operation: Id::new("updated")?,
            definition: "publication-race-v1".into(),
            base: initial.id.clone(),
            keys: vec!["counter".into()],
            replacements: replacement.clone(),
            deletions: None,
            relationship: None,
        })
        .await?;
    std::fs::remove_file(&replacement.path)?;
    let left = request(&updated, "left", Some(old.id.clone()))?;
    let right = request(&updated, "right", Some(old.id.clone()))?;
    // Prepare sequentially; the sole injected overlap is final publication.
    for (engine, spec) in [(first, &left), (second, &right)] {
        let stopped = engine.export_until(spec, "validation").await.unwrap_err();
        if stopped.code() != "injected_failure" {
            return Err(stopped);
        }
    }
    let left_completed = completion(first, &left).await?;
    let right_completed = completion(second, &right).await?;
    assert_ne!(
        left_completed.materialized.object.path,
        right_completed.materialized.object.path
    );
    assert_eq!(first.latest_export(dataset, &left.channel).await?, old);
    let (left_result, right_result, captured) = if let Some((directory, prefix)) = process_scope {
        processes::run(directory, prefix, &left, &right).await?
    } else {
        catalogs[0].arm();
        catalogs[1].arm();
        let (left_result, right_result) = tokio::join!(first.export(&left), second.export(&right));
        (
            left_result.map_err(|e| e.code().to_owned()),
            right_result.map_err(|e| e.code().to_owned()),
            catalogs[0].rendezvous.metadata.lock().unwrap().clone(),
        )
    };
    let (winner, winner_engine, winner_spec, loser_engine, loser_spec, losing_code, losing_object) =
        match (left_result, right_result) {
            (Ok(winner), Err(error)) => (
                winner,
                first,
                &left,
                second,
                &right,
                error,
                right_completed.materialized.object.clone(),
            ),
            (Err(error), Ok(winner)) => (
                winner,
                second,
                &right,
                first,
                &left,
                error,
                left_completed.materialized.object.clone(),
            ),
            (left, right) => {
                eprintln!(
                    "{}",
                    json!({"phase": "publication_race_outcome_mismatch", "left_code": left.as_ref().err(), "right_code": right.as_ref().err(), "arrivals": catalogs[0].rendezvous.metadata.lock().unwrap().len()})
                );
                return Err(Error::refusal(
                    "provider_evidence",
                    "same-head publication did not produce exactly one successful selection",
                ));
            }
        };
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0], captured[1]);
    assert!(!catalogs[0].armed.load(Ordering::SeqCst));
    assert!(!catalogs[1].armed.load(Ordering::SeqCst));
    let winning_completion = if winner.id == left.operation {
        &left_completed
    } else {
        &right_completed
    };
    assert_eq!(winner.object, winning_completion.materialized.object);
    assert_eq!(winner.validation, winning_completion.validation);
    assert_eq!(winner.source, updated.version_ref());
    assert_eq!(winner.rows, 4);
    assert_eq!(old.rows, 3);
    assert_eq!(winner_engine.export(winner_spec).await?, winner);
    assert_eq!(
        loser_engine.export(loser_spec).await.unwrap_err().code(),
        "export_conflict"
    );
    assert_eq!(completion(first, &left).await?, left_completed);
    assert_eq!(completion(second, &right).await?, right_completed);
    assert_eq!(first.latest_export(dataset, &left.channel).await?, winner);
    assert_eq!(second.latest_export(dataset, &left.channel).await?, winner);
    let view = first.existing_lifecycle_view(dataset).await?;
    let rows = first.inner.lifecycle.rows(&view).await?;
    let selected_revision = rows
        .revision(&format!("export-result/{}", winner.id))
        .ok_or_else(|| {
            Error::refusal("provider_evidence", "winning immutable result is missing")
        })?;
    assert!(selected_revision > 0);
    assert_eq!(
        rows.revision("export-channel/public"),
        Some(selected_revision)
    );
    assert!(
        rows.revision(&format!("export-result/{}", loser_spec.operation))
            .is_none()
    );
    // Losing work remains exact and available, but a stale expected channel is
    // not silently changed. No second materialization or validation is requested.
    loser_engine
        .inner
        .objects
        .check_available(
            vec![losing_object.clone()],
            &loser_engine.inner.lifecycle.budget,
        )
        .await?;
    println!(
        "{}",
        json!({"phase": "publication_race_selected", "dataset": dataset, "captured_metadata": captured[0], "winner": winner.id, "loser": loser_spec.operation, "initial_loser_code": losing_code,
        "old_object": old.object, "winning_object": winner.object, "losing_object": losing_object, "retained_export_bytes": old.object.bytes + winner.object.bytes + losing_object.bytes,
        "catalog_and_native_attempt_bytes_excluded": true, "separate_os_processes": process_scope.is_some()})
    );
    Ok(Outcome {
        old,
        winner,
        current_source: updated,
        losing_object,
    })
}

async fn read_values(engine: &Engine, pin: &ExportPin, current: bool) -> Result<()> {
    assert_eq!(
        pin.fields,
        vec![
            ExportField {
                id: 2,
                name: "counter".into(),
                data_type: "Int64".into(),
                nullable: false
            },
            ExportField {
                id: 1,
                name: "text".into(),
                data_type: "Utf8".into(),
                nullable: true
            }
        ]
    );
    let mut stream = engine.read_export(pin).await?;
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if rows.len().saturating_add(batch.num_rows()) > 4 {
            return Err(Error::refusal(
                "provider_evidence",
                "publication read exceeded four synthetic rows",
            ));
        }
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(batch.column(1), &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(count.null_count(), 0);
        for row in 0..batch.num_rows() {
            rows.push((
                count.value(row),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
            ));
        }
    }
    rows.sort();
    let expected = if current {
        vec![
            (1, Some("new".into())),
            (1, Some("new".into())),
            (2, None),
            (3, Some("".into())),
        ]
    } else {
        vec![(1, Some("old".into())), (2, None), (3, Some("".into()))]
    };
    assert_eq!(rows, expected);
    Ok(())
}

async fn cold_read(engine: &Engine, outcome: &Outcome) -> Result<()> {
    let dataset = &outcome.winner.dataset;
    assert_eq!(
        engine.latest_export(dataset, &Id::new("public")?).await?,
        outcome.winner
    );
    assert_eq!(engine.latest(dataset).await?, outcome.current_source);
    read_values(engine, &outcome.old, false).await?;
    read_values(engine, &outcome.winner, true).await?;
    println!(
        "{}",
        json!({"phase": "publication_race_passed", "dataset": dataset, "winner": outcome.winner.object, "old": outcome.old.object, "losing_retained": outcome.losing_object, "remote_cleanup": false, "request_counts_measured": false})
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_clients_preserve_one_publication_winner_and_losing_completion() {
    let root = tempfile::tempdir().unwrap();
    let producer = tempfile::tempdir().unwrap();
    let dataset = Id::new("publication-race").unwrap();
    let catalog_root = root.path().join("catalog");
    let catalogs = RaceCatalog::pair(
        storage::local_catalog(&catalog_root).await.unwrap(),
        storage::local_catalog(&catalog_root).await.unwrap(),
        &dataset,
    );
    let first = Engine::with_catalog(root.path().join("first"), catalogs[0].clone(), limits())
        .await
        .unwrap();
    let second = Engine::with_catalog(root.path().join("second"), catalogs[1].clone(), limits())
        .await
        .unwrap();
    let outcome = race(&first, &second, &catalogs, &dataset, producer.path(), None)
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    drop(first);
    drop(second);
    drop(catalogs);
    drop(producer);
    let cold = Engine::with_catalog(
        root.path().join("cold"),
        storage::local_catalog(&catalog_root).await.unwrap(),
        limits(),
    )
    .await
    .unwrap();
    cold_read(&cold, &outcome).await.unwrap();
    cold.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh qualification UUID and isolated live R2/Basin configuration"]
async fn native_remote_clients_race_publication_and_read_pins_from_a_fresh_host() {
    if let Err(error) = qualify().await {
        eprintln!(
            "{}",
            json!({"phase": "publication_race_failed", "code": error.code()})
        );
        panic!("isolated publication race failed; retain all references");
    }
}

async fn qualify() -> Result<()> {
    let name = env("DOCSPEC_QUALIFICATION_ID")?;
    let suffix = name
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("provider_scope", "fresh qualification UUID required"))?;
    if uuid::Uuid::parse_str(suffix)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(suffix)
    {
        return Err(Error::refusal(
            "provider_scope",
            "canonical qualification UUID required",
        ));
    }
    let dataset = Id::new(&name)?;
    let prefix = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{name}/publication-race",
        env("DOCSPEC_S3_BUCKET")?
    );
    let mut remote_a = connect(&prefix).await?;
    let mut remote_b = connect(&prefix).await?;
    if remote_a
        .catalog
        .namespace_exists(crate::catalog::lifecycle_ident(&dataset).namespace())
        .await?
    {
        return Err(Error::refusal(
            "provider_scope",
            "qualification namespace already exists",
        ));
    }
    let catalogs = RaceCatalog::pair(remote_a.catalog, remote_b.catalog, &dataset);
    remote_a.catalog = catalogs[0].clone();
    remote_b.catalog = catalogs[1].clone();
    let first_host = tempfile::tempdir()?;
    let second_host = tempfile::tempdir()?;
    let producer = tempfile::tempdir()?;
    let first = Engine::with_remote(first_host.path(), remote_a, limits()).await?;
    let second = Engine::with_remote(second_host.path(), remote_b, limits()).await?;
    println!(
        "{}",
        json!({"phase": "publication_race_start", "dataset": dataset, "prefix": prefix, "clients": 2, "separate_os_processes": true, "hold_deadline_seconds": HOLD_DEADLINE.as_secs()})
    );
    // Retain the tiny control records/logs even when a child fails.
    let rendezvous = tempfile::tempdir()?.keep();
    println!(
        "{}",
        json!({"phase":"publication_process_control", "directory":rendezvous})
    );
    let outcome = race(
        &first,
        &second,
        &catalogs,
        &dataset,
        producer.path(),
        Some((&rendezvous, &prefix)),
    )
    .await?;
    first.shutdown().await?;
    second.shutdown().await?;
    drop(first);
    drop(second);
    drop(catalogs);
    drop(producer);
    let cold_host = tempfile::tempdir()?;
    let cold = Engine::with_remote(cold_host.path(), connect(&prefix).await?, limits()).await?;
    cold_read(&cold, &outcome).await?;
    cold.shutdown().await?;
    Ok(())
}
