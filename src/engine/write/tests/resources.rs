//! Shared admission while wide buffers, receipt work and slow output overlap.
// Rust guideline compliant 2026-02-21

use super::*;
use crate::{
    evidence::{EVIDENCE, EvidenceSpec, SOURCE_FILE, SOURCE_ROW},
    storage::remote::objects::tests::writes::{Behavior, WriteFixture},
};
use arrow::{
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use std::{
    fs::File,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const DEADLINE: Duration = Duration::from_secs(15);

fn available(budget: &Budget) -> usize {
    // SnapshotRows retains its own admission across commits. Observe free
    // reservation capacity without clearing cache or allocating payload buffers.
    let (mut low, mut high) = (0, budget.limits.memory_bytes / 2);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        match budget.reserve(middle) {
            Ok(permit) => {
                drop(permit);
                low = middle;
            }
            Err(error) => {
                assert_eq!(error.code(), "capacity");
                high = middle - 1;
            }
        }
    }
    low
}

fn input(path: &Path, batch: &RecordBatch) -> InputSpec {
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    let (digest, _) = storage::digest(path).unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest,
    }
}

fn receipt(path: &Path, file: &str, row: i64, value: &str) -> InputSpec {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new(SOURCE_FILE, DataType::Utf8, false),
            Field::new(SOURCE_ROW, DataType::Int64, false),
            Field::new(EVIDENCE, DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![file])),
            Arc::new(Int64Array::from(vec![row])),
            Arc::new(StringArray::from(vec![Some(value)])),
        ],
    )
    .unwrap();
    input(path, &batch)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_slice_receipts_and_held_output_share_admission_without_losing_selection() {
    let root = TempDir::new().unwrap();
    let limits = Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 0,
        batch_rows: 64,
        max_batch_bytes: 8 << 20,
        max_metadata_bytes: 2 << 20,
    };
    let engine = Engine::open(root.path().join("workspace"), limits)
        .await
        .unwrap();
    let budget = &engine.inner.lifecycle.budget;
    let pool = budget.limits.memory_bytes / 2;
    assert!(available(budget) <= pool);
    // A one-row slice retains the complete multi-megabyte backing value buffer.
    // These fixture allocations are outside reservation accounting; sample RSS.
    let values = (0..1024)
        .map(|row| {
            if row % 7 == 0 {
                None
            } else {
                Some(format!("row-{row}:{}", "文".repeat(1365)))
            }
        })
        .collect::<Vec<_>>();
    let keys = (0..1024)
        .map(|row| (row < 1022).then_some(row / 2))
        .collect::<Vec<_>>();
    let wide = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(StringArray::from(values.clone())),
        ],
    )
    .unwrap();
    let slice = wide.slice(1, 1);
    let retained = slice.clone();
    let backing_bytes = retained.column(1).get_array_memory_size();
    assert!(backing_bytes > 3 << 20);
    let sliced_values = retained
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let original_values = wide
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        sliced_values.value_data().as_ptr(),
        original_values.value_data().as_ptr()
    );
    assert_eq!(
        sliced_values.value_data().len(),
        original_values.value_data().len()
    );
    let source = input(&root.path().join("input.parquet"), &wide);
    let request = RunSpec {
        dataset: Id::new("wide").unwrap(),
        operation: Id::new("initial").unwrap(),
        input: source.path,
        expected_digest: source.digest,
        sql: "SELECT * FROM input".into(),
        definition: "v1".into(),
        base: None,
        relationship: None,
        partition_by: Vec::new(),
    };
    let original = engine.run(&request).await.unwrap();
    let original_records = engine.inspect(&request.dataset).await.unwrap();
    let operation: Operation =
        serde_json::from_value(original_records["operation/initial"].clone()).unwrap();
    let directory = engine
        .reserve_attempt(
            &OperationRequest::Build(request.clone()),
            &operation,
            "q1-buffered",
        )
        .await
        .unwrap();
    let native = storage::open_pin(&original.dataset.groups["source"], budget)
        .await
        .unwrap();
    let output = engine
        .create_output(
            &request.dataset,
            &directory.join("wide"),
            native.metadata().current_schema().as_ref().clone(),
        )
        .await
        .unwrap();
    let mut occurrences = engine.read_source_occurrences(&original).await.unwrap();
    let first = occurrences.try_next().await.unwrap().unwrap();
    let file = first
        .column_by_name(storage::OCCURRENCE_FILE)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(1)
        .to_owned();
    let row = first
        .column_by_name(storage::OCCURRENCE_ROW)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(1);
    drop(first);
    drop(occurrences);
    drop(wide);

    let (started, observed) = oneshot::channel();
    let (release, next) = oneshot::channel::<()>();
    let batches = futures::stream::iter([Ok(slice)]).chain(futures::stream::once(async move {
        // The maintained writer has encoded its first batch before asking for more.
        started.send(()).unwrap();
        next.await.unwrap();
        Ok(retained.clone())
    }));
    let writer_budget = budget.clone();
    let before_writer = available(budget);
    let writer =
        tokio::spawn(async move { storage::write_batches(&output, batches, &writer_budget).await });
    tokio::time::timeout(DEADLINE, observed)
        .await
        .unwrap()
        .unwrap();
    let writer_charge = budget.limits.max_batch_bytes * 4 + budget.limits.max_metadata_bytes;
    let with_writer = available(budget);
    assert_eq!(with_writer + writer_charge, before_writer);

    // Send an actual compressed native Parquet file through the configured
    // maintained conditional upload. The fixture holds the response after PUT.
    let source_file = original.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    let expected = ObjectRef {
        owner: request.dataset.clone(),
        path: "s3://bucket/owned/file.bin".into(),
        digest: source_file.digest.clone(),
        bytes: source_file.bytes,
    };
    let source_bytes = std::fs::read(&source_file.path).unwrap();
    let mut fixture =
        WriteFixture::for_path(Behavior::HoldPutResponse, "/bucket/owned/file.bin".into());
    let objects = fixture.objects();
    let upload_budget = budget.clone();
    let upload_expected = expected.clone();
    let upload_source = PathBuf::from(&source_file.path);
    let upload = tokio::spawn(async move {
        objects
            .upload_file(&upload_expected, upload_source, &upload_budget)
            .await
    });
    tokio::time::timeout(DEADLINE, fixture.started.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    let held_at = Instant::now();
    assert!(available(budget) < with_writer);

    let spec = EvidenceSpec {
        dataset: request.dataset.clone(),
        operation: Id::new("receipts").unwrap(),
        definition: "v1".into(),
        base: original.id.clone(),
        replacements: receipt(
            &root.path().join("receipts.parquet"),
            &file,
            row,
            "retained receipt",
        ),
        deletions: None,
    };
    let attached = tokio::time::timeout(DEADLINE, engine.update_evidence(&spec))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attached.dataset.groups, original.dataset.groups);
    let mut receipts = engine.read_receipts(&attached).await.unwrap();
    let actual = receipts.try_next().await.unwrap().unwrap();
    assert_eq!(actual.num_rows(), 1);
    assert_eq!(
        actual
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        file
    );
    assert_eq!(
        actual
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        row
    );
    assert_eq!(
        actual
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "retained receipt"
    );
    assert!(receipts.try_next().await.unwrap().is_none());
    drop(actual);
    drop(receipts);
    let saved = engine.inspect(&request.dataset).await.unwrap();
    let oversized = "x".repeat(budget.limits.max_batch_bytes + 1);
    let bad = EvidenceSpec {
        operation: Id::new("oversized-receipt").unwrap(),
        base: attached.id.clone(),
        replacements: receipt(
            &root.path().join("oversized.parquet"),
            &file,
            row,
            &oversized,
        ),
        ..spec
    };
    let error = tokio::time::timeout(DEADLINE, engine.update_evidence(&bad))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), "oversized_batch");
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), attached);
    let refused = engine.inspect(&request.dataset).await.unwrap();
    for (key, value) in &saved {
        if key.starts_with("phase/") {
            assert_eq!(refused.get(key), Some(value));
        }
    }
    drop(oversized);

    // Cancellation ends the caller, while the owned HTTP worker still holds
    // upload admission. The native writer/backing slice remain live as well.
    let held_upload = available(budget);
    upload.abort();
    assert!(upload.await.unwrap_err().is_cancelled());
    assert_eq!(available(budget), held_upload);
    // A declared slow-output occupancy window allows the existing1s RSS sampler
    // to observe held buffers; it is injected delay, not engine compute latency.
    if let Some(wait) = Duration::from_secs(2).checked_sub(held_at.elapsed()) {
        tokio::time::sleep(wait).await;
    }
    fixture.release.take().unwrap().send(()).unwrap();
    let recovered = tokio::time::timeout(DEADLINE, async {
        loop {
            if let Ok(permit) = budget.reserve(held_upload + 1) {
                break permit;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(recovered);
    let after_upload = available(budget);
    assert!(after_upload > held_upload);
    let report = fixture.finish();
    assert_eq!(report.requests, vec![("PUT".into(), None)]);
    assert_eq!(report.published.as_deref(), Some(source_bytes.as_slice()));
    // Native writer cancellation releases its own buffers and final sliced
    // backing reference. Partial local output stays under its exact protection.
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    drop(release);
    assert_eq!(available(budget), after_upload + writer_charge);
    let mut source = engine.read_group(&attached, "source").await.unwrap();
    let mut observed = Vec::new();
    while let Some(batch) = source.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        observed.extend((0..batch.num_rows()).map(|i| {
            (
                (!ids.is_null(i)).then(|| ids.value(i)),
                (!texts.is_null(i)).then(|| texts.value(i).to_owned()),
            )
        }));
    }
    let mut literal = keys.into_iter().zip(values).collect::<Vec<_>>();
    literal.sort();
    observed.sort();
    assert_eq!(observed, literal);
    eprintln!(
        "Q1_OBSERVATION memory_limit_bytes={} sliced_backing_bytes={} writer_admission_bytes={} put_bytes={} no_spill=true",
        budget.limits.memory_bytes, backing_bytes, writer_charge, expected.bytes
    );
}
