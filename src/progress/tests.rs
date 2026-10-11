//! Held real native writers distinguish live observations from durable completion.

use super::*;
use crate::{
    Error,
    budget::{Budget, Limits},
    storage,
};
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{
    StreamExt,
    future::{Either, select},
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, oneshot};
use tracing::{
    Event, Subscriber,
    field::{Field as TraceField, Visit},
    instrument::WithSubscriber,
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

type Fields = BTreeMap<String, String>;

#[derive(Clone, Default)]
struct Capture {
    events: Arc<Mutex<Vec<Fields>>>,
    progress: Arc<Notify>,
}

struct Values(Fields);

impl Visit for Values {
    fn record_str(&mut self, field: &TraceField, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &TraceField, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut values = Values(BTreeMap::new());
        event.record(&mut values);
        let progress = values
            .0
            .get("event")
            .is_some_and(|name| name == "native_writer_progress");
        self.events.lock().unwrap().push(values.0);
        if progress {
            self.progress.notify_one();
        }
    }
}

impl Capture {
    fn names(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|fields| fields.get("event").cloned())
            .collect()
    }
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(1)]))],
    )
    .unwrap()
}

async fn table(root: &std::path::Path, batch: &RecordBatch) -> iceberg::table::Table {
    let catalog = storage::local_catalog(root).await.unwrap();
    let namespace = iceberg::NamespaceIdent::new("progress".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    catalog
        .create_table(
            &namespace,
            iceberg::TableCreation::builder()
                .name("source".into())
                .location(root.join("source").to_string_lossy().into_owned())
                .schema(
                    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(batch.schema().as_ref())
                        .unwrap(),
                )
                .properties(storage::table_properties())
                .build(),
        )
        .await
        .unwrap()
}

fn budget() -> Budget {
    Budget::new(Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
        batch_rows: 4,
    })
    .unwrap()
}

// tracing-core's single-dispatcher interest fast path consults the current
// thread's dispatcher. Parallel writers without our subscriber can initialize
// the same callsites as disabled. Exercise each real observer in its own process.
fn run_isolated(name: &str) -> bool {
    const CHILD: &str = "DOCSPEC_PROGRESS_OBSERVER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(name) {
        return false;
    }
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = ChildGuard(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD, name)
            .stdin(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "isolated progress observer {name} failed: {status}"
            );
            return true;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "isolated progress observer {name} timed out"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[tokio::test]
async fn held_writer_reports_accepted_work_before_failure_or_abandonment() {
    if run_isolated(
        "progress::tests::held_writer_reports_accepted_work_before_failure_or_abandonment",
    ) {
        return;
    }
    for fail in [true, false] {
        let root = tempfile::TempDir::new().unwrap();
        let input = batch();
        // Count exactly the same necessary materialized Arrow batch as the writer.
        let expected_bytes =
            storage::identity::materialize(input.clone(), "docspec:test", 0, 1 << 20)
                .unwrap()
                .get_array_memory_size();
        let table = table(root.path(), &input).await;
        let budget = budget();
        let dataset = Id::new("source").unwrap();
        let operation = Id::new("observed").unwrap();
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let (send, receive) = oneshot::channel::<Result<RecordBatch>>();
        let batches = futures::stream::iter([Ok(input)])
            .chain(futures::stream::once(async { receive.await.unwrap() }));
        let work = Box::pin(
            phase(
                &dataset,
                &operation,
                "processing",
                storage::write_batches(&table, batches, &budget),
            )
            .with_subscriber(subscriber),
        );
        let observed = Box::pin(capture.progress.notified());
        // This deadline bounds only a tiny correctness fixture, not performance.
        let remaining = match tokio::time::timeout(Duration::from_secs(10), select(work, observed))
            .await
            .unwrap()
        {
            Either::Right(((), remaining)) => remaining,
            Either::Left(_) => panic!("writer completed before held input was released"),
        };
        {
            let events = capture.events.lock().unwrap();
            let live = events
                .iter()
                .find(|fields| {
                    fields
                        .get("event")
                        .is_some_and(|name| name == "native_writer_progress")
                })
                .unwrap();
            assert_eq!(live["rows_accepted"], "3");
            assert_eq!(live["decoded_bytes_accepted"], expected_bytes.to_string());
            assert_eq!(live["total_rows"], "unknown");
            assert_eq!(live["total_decoded_bytes"], "unknown");
            assert!(!events.iter().any(|fields| {
                fields.get("event").is_some_and(|name| {
                    name == "native_writer_closed" || name == "phase_work_finished"
                })
            }));
        }
        if fail {
            send.send(Err(Error::refusal(
                "fixture_input",
                "sensitive source location is not logged",
            )))
            .unwrap();
            assert_eq!(remaining.await.unwrap_err().code(), "fixture_input");
            assert!(capture.names().contains(&"native_writer_failed".into()));
            assert!(capture.names().contains(&"phase_work_failed".into()));
            assert!(
                !capture
                    .names()
                    .iter()
                    .any(|name| name.ends_with("abandoned"))
            );
        } else {
            // WithSubscriber installs its dispatcher while polling, not while
            // dropping. Match the CLI's active subscriber during cancellation;
            // Instrumented then enters the retained phase span for inner Drop.
            let dispatcher = remaining.dispatcher().clone();
            tracing::dispatcher::with_default(&dispatcher, || drop(remaining));
            drop(send);
            assert!(capture.names().contains(&"native_writer_abandoned".into()));
            assert!(capture.names().contains(&"phase_work_abandoned".into()));
            assert!(!capture.names().iter().any(|name| name.ends_with("failed")));
        }
        assert!(!capture.names().contains(&"native_writer_closed".into()));
        assert!(!capture.names().contains(&"phase_work_finished".into()));
        let events = capture.events.lock().unwrap();
        assert!(!format!("{events:?}").contains("sensitive source location"));
        assert!(!format!("{events:?}").contains("phase durably complete"));
    }
}

#[tokio::test]
async fn writer_close_reports_totals_without_committing_and_catalog_writes_stay_quiet() {
    if run_isolated(
        "progress::tests::writer_close_reports_totals_without_committing_and_catalog_writes_stay_quiet",
    ) {
        return;
    }
    let root = tempfile::TempDir::new().unwrap();
    let input = batch();
    let table = table(root.path(), &input).await;
    let budget = budget();
    let capture = Capture::default();
    let files = storage::write_batches(&table, futures::stream::iter([Ok(input.clone())]), &budget)
        .with_subscriber(tracing_subscriber::registry().with(capture.clone()))
        .await
        .unwrap();
    assert_eq!(files.iter().map(|file| file.record_count()).sum::<u64>(), 3);
    assert!(table.metadata().current_snapshot().is_none());
    {
        let events = capture.events.lock().unwrap();
        let closed = events
            .iter()
            .find(|fields| {
                fields
                    .get("event")
                    .is_some_and(|name| name == "native_writer_closed")
            })
            .unwrap();
        assert_eq!(closed["rows_accepted"], "3");
        assert_eq!(closed["files"], files.len().to_string());
    }
    let quiet = Capture::default();
    let files = storage::write_catalog_batches(&table, futures::stream::iter([Ok(input)]), &budget)
        .with_subscriber(tracing_subscriber::registry().with(quiet.clone()))
        .await
        .unwrap();
    assert_eq!(files.iter().map(|file| file.record_count()).sum::<u64>(), 3);
    assert!(
        !quiet
            .names()
            .iter()
            .any(|name| name.starts_with("native_writer_"))
    );
}
