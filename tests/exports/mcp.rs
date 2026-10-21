//! Opt-in real SpicyRegs MCP reads of immutable DocSpec local distributions.
// Rust guideline compliant 2026-02-21

use super::*;
use arrow::array::Array;
use docspec::evidence::{EVIDENCE, EvidenceSpec, SOURCE_FILE, SOURCE_ROW};
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

struct Consumer {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Consumer {
    fn open(root: &Path, pin: &ExportPin, metadata: Option<&str>) -> Self {
        if metadata.is_none() {
            storage::verify_object(&pin.object).unwrap();
        }
        std::fs::create_dir(root).unwrap();
        // The fixture adds a table name, preserving the exact immutable bytes.
        std::fs::hard_link(&pin.object.path, root.join("comments.parquet")).unwrap();
        let python = std::env::var_os("DOCSPEC_SPICY_REGS_PYTHON")
            .expect("set DOCSPEC_SPICY_REGS_PYTHON to the existing sibling Python environment");
        let repository = std::env::var_os("DOCSPEC_SPICY_REGS_REPO")
            .expect("set DOCSPEC_SPICY_REGS_REPO to the existing sibling checkout");
        let binding = root.join("docspec-exports.json");
        if let Some(metadata) = metadata {
            std::fs::write(
                &binding,
                serde_json::to_vec(&json!({
                    "version": 1, "metadata_url": metadata, "binary": env!("CARGO_BIN_EXE_docspec"),
                    "tables": {"comments": {"file": "comments.parquet", "export": pin}}
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let mut command = Command::new(python);
        command.env_clear();
        if metadata.is_some() {
            command.env("SPICY_REGS_DOCSPEC_EXPORTS", &binding);
        }
        let mut child = command
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/qualification/spicy_regs_mcp.py"),
            )
            .arg(repository)
            .arg(root)
            .env("SPICY_REGS_DATA_DIR", root)
            .env("SPICY_REGS_HOME_DIR", root)
            .env(
                "SPICY_REGS_CONNECTION_TTL",
                if metadata.is_some() { "0" } else { "86400" },
            )
            .env("SPICY_REGS_MEMORY_LIMIT", "256MB")
            .env("PYTHONIOENCODING", "utf-8")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
        }
    }

    fn query(&mut self) -> Value {
        writeln!(self.input, "query").unwrap();
        self.input.flush().unwrap();
        let mut line = String::new();
        assert_ne!(
            self.output.read_line(&mut line).unwrap(),
            0,
            "MCP helper exited before reply"
        );
        serde_json::from_str(&line).unwrap()
    }

    fn finish(mut self) {
        writeln!(self.input, "close").unwrap();
        self.input.flush().unwrap();
        assert!(self.child.wait().unwrap().success());
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // A failed assertion must not leave an application reader running.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn null_occurrence(engine: &Engine, release: &ReleasePin) -> (String, i64) {
    let mut stream = engine.read_source_occurrences(release).await.unwrap();
    let mut found = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let files = batch
            .column_by_name(storage::OCCURRENCE_FILE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = batch
            .column_by_name(storage::OCCURRENCE_ROW)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                found.push((files.value(row).to_owned(), rows.value(row)));
            }
        }
    }
    assert_eq!(found.len(), 1);
    found.pop().unwrap()
}

async fn evidence(
    engine: &Engine,
    root: &Path,
    base: &ReleasePin,
    operation: &str,
    location: &(String, i64),
    value: &str,
) -> ReleasePin {
    let schema = Arc::new(Schema::new(vec![
        Field::new(SOURCE_FILE, DataType::Utf8, false),
        Field::new(SOURCE_ROW, DataType::Int64, false),
        Field::new(EVIDENCE, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![location.0.as_str()])),
            Arc::new(Int64Array::from(vec![location.1])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap();
    let path = root.join(format!("{operation}.parquet"));
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    engine
        .update_evidence(&EvidenceSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id(operation),
            definition: base.dataset.definition.clone(),
            base: base.id.clone(),
            replacements: InputSpec {
                path: path.to_string_lossy().into_owned(),
                digest: storage::digest(&path).unwrap().0,
            },
            deletions: None,
        })
        .await
        .unwrap()
}

async fn assert_receipt(
    engine: &Engine,
    release: &ReleasePin,
    location: &(String, i64),
    expected: &str,
) {
    let batches: Vec<_> = engine
        .read_receipts(release)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|batch| batch.num_rows() != 0).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        location.0
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        location.1
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        expected
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the explicitly configured existing SpicyRegs checkout and Python environment"]
async fn actual_spicy_regs_mcp_reads_immutable_exports_across_updates() {
    reads_across_updates(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the explicitly configured existing SpicyRegs checkout and Python environment"]
async fn actual_spicy_regs_mcp_admits_exact_exports_and_release_evidence() {
    reads_across_updates(true).await;
}

struct MetadataHost(tokio::task::JoinHandle<()>);
impl Drop for MetadataHost {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn reads_across_updates(trusted: bool) {
    let root = tempfile::tempdir().unwrap();
    let rows = vec![
        (Some(1), Some("duplicate".into()), "private-canary".into()),
        (Some(1), Some("duplicate".into()), "private-canary".into()),
        (Some(2), None, "private-canary".into()),
        (Some(3), Some("".into()), "private-canary".into()),
        (None, Some("雪 🌶".into()), "private-canary".into()),
    ];
    let input = write_input(&root.path().join("input.parquet"), &rows, false);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let (metadata, _host) = if trusted {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = docspec::http::router(engine.clone());
        (
            Some(url),
            Some(MetadataHost(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }))),
        )
    } else {
        (None, None)
    };
    let initial = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let location = null_occurrence(&engine, &initial).await;
    let initial = evidence(
        &engine,
        root.path(),
        &initial,
        "receipt-before",
        &location,
        "original evidence",
    )
    .await;
    let first = engine.export(&request(&initial, "first")).await.unwrap();
    let mut old_reader = Consumer::open(
        &root.path().join("reader-first"),
        &first,
        metadata.as_deref(),
    );
    let old_values = json!([
        [1, "duplicate"],
        [1, "duplicate"],
        [2, null],
        [3, ""],
        [null, "雪 🌶"]
    ]);
    let first_reply = old_reader.query();
    assert_eq!(first_reply["rows"], old_values);
    if trusted {
        assert_eq!(
            first_reply["publication"]["export"],
            serde_json::to_value(&first).unwrap()
        );
        assert_eq!(
            first_reply["publication"]["evidence"]["manifest_blake3"],
            first.source.manifest.digest
        );
    }

    let corrected = engine
        .update(&UpdateSpec {
            dataset: initial.dataset.dataset.clone(),
            operation: id("corrected"),
            definition: "v1".into(),
            base: initial.id.clone(),
            keys: vec!["id".into()],
            replacements: write_input(
                &root.path().join("replace.parquet"),
                &[(Some(1), Some("corrected".into()), "private-canary".into())],
                false,
            ),
            deletions: Some(write_input(
                &root.path().join("delete.parquet"),
                &[(Some(2), None, String::new())],
                true,
            )),
            relationship: None,
        })
        .await
        .unwrap();
    assert_eq!(null_occurrence(&engine, &corrected).await, location);
    let mut spec = request(&corrected, "second");
    spec.expected = Some(first.id.clone());
    let second = engine.export(&spec).await.unwrap();
    let mut new_reader = Consumer::open(
        &root.path().join("reader-second"),
        &second,
        metadata.as_deref(),
    );
    let new_values = json!([[1, "corrected"], [3, ""], [null, "雪 🌶"]]);
    assert_eq!(new_reader.query()["rows"], new_values);
    assert_eq!(old_reader.query()["rows"], old_values);

    let changed = evidence(
        &engine,
        root.path(),
        &corrected,
        "receipt-after",
        &location,
        "changed evidence",
    )
    .await;
    assert_ne!(changed.id, corrected.id);
    assert_eq!(changed.dataset.groups, corrected.dataset.groups);
    assert_receipt(&engine, &initial, &location, "original evidence").await;
    assert_receipt(&engine, &corrected, &location, "original evidence").await;
    assert_receipt(&engine, &changed, &location, "changed evidence").await;
    let mut spec = request(&changed, "third");
    spec.expected = Some(second.id.clone());
    let third = engine.export(&spec).await.unwrap();
    assert_eq!(third.source, changed.version_ref());
    assert_ne!(third.source, second.source);
    assert_eq!(independent_rows(&third), independent_rows(&second));
    let mut evidence_reader = Consumer::open(
        &root.path().join("reader-third"),
        &third,
        metadata.as_deref(),
    );
    let third_reply = evidence_reader.query();
    assert_eq!(third_reply["rows"], new_values);
    if trusted {
        assert_eq!(
            third_reply["publication"]["export"],
            serde_json::to_value(&third).unwrap()
        );
        assert_eq!(
            third_reply["publication"]["evidence"]["version"],
            changed.id.to_string()
        );
        assert_eq!(third_reply["publication"]["evidence"]["row_mapping"], false);
        let frozen = old_reader.query();
        assert_eq!(
            frozen["publication"]["export"],
            serde_json::to_value(&first).unwrap()
        );
        assert_eq!(frozen["admissions"], json!({"metadata": 2, "hash": 1}));
    }
    // Completed retries keep exact artifacts; evidence was checked by DocSpec, not MCP.
    assert_eq!(engine.export(&spec).await.unwrap(), third);
    assert_eq!(old_reader.query()["rows"], old_values);
    assert_eq!(new_reader.query()["rows"], new_values);
    old_reader.finish();
    new_reader.finish();
    evidence_reader.finish();
}
