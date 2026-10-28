//! Check observed process memory for a wide dictionary decoded into string views.
// Rust guideline compliant 2026-02-21
#![cfg(any(target_os = "macos", target_os = "linux"))]

use arrow::{
    array::{Array, StringArray, StringViewArray, UInt32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{budget::Limits, job::Job};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{path::Path, process::Command, sync::Arc};

const MEMORY: usize = 128 << 20;
// Allow a fixed 64 MiB for executable/runtime/allocator overhead, separately
// from the configured 128 MiB shared execution and custom-admission allowance.
const OVERHEAD: usize = 64 << 20;
const ROWS: usize = 2048;
const INPUT: &str = "DOCSPEC_TEST_VIEW_MEMORY_INPUT";

// Match the ordinary CLI allocator for this isolated process qualification.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn run_projection(path: &Path) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let job = Job::new(
                Limits {
                    memory_bytes: MEMORY,
                    max_batch_bytes: 4 << 20,
                    batch_rows: ROWS,
                    compute_threads: 1,
                    ..Limits::default()
                },
                root.path().join("scratch"),
            )
            .unwrap();
            job.register_parquet("source", path).await.unwrap();
            let batches = job
                .sql("SELECT json_get_many(raw,paths) AS selected FROM source")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                ROWS
            );
            for batch in batches {
                let selected = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                let values = selected
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(values.len(), batch.num_rows() * 2);
                for row in 0..selected.len() {
                    let offset = selected.value_offsets()[row] as usize;
                    assert_eq!(values.value(offset), "1");
                    assert!(values.is_null(offset + 1));
                }
            }
        });
}

#[test]
fn repeated_dictionary_views_respect_process_memory() {
    if let Some(path) = std::env::var_os(INPUT) {
        run_projection(Path::new(&path));
        return;
    }
    // Prepare the compressed fixture outside the measured child. One shared
    // value represents >2 GiB of logical text, without allocating that copy.
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("repeated.parquet");
    let raw = format!(r#"{{"keep":1,"padding":"{}"}}"#, "x".repeat(1 << 20));
    let one = StringViewArray::from(vec![raw]);
    let values = arrow::compute::take(&one, &UInt32Array::from(vec![0; ROWS]), None).unwrap();
    assert!(values.get_array_memory_size() < 2 << 20);
    let paths = Arc::new(StringViewArray::from_iter_values(
        (0..ROWS).map(|row| format!(r#"[["keep"],["missing_{row}"]]"#)),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("raw", DataType::Utf8View, false),
        Field::new("paths", DataType::Utf8View, false),
    ]));
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(2 << 20)
        .set_max_row_group_row_count(Some(ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, vec![values, paths]).unwrap())
        .unwrap();
    writer.close().unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < 2 << 20);

    let mut command = Command::new("/usr/bin/time");
    #[cfg(target_os = "macos")]
    command.arg("-l");
    #[cfg(target_os = "linux")]
    command.args(["-f", "DOCSPEC_MAXRSS_KIB=%M"]);
    let output = command
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "repeated_dictionary_views_respect_process_memory",
            "--nocapture",
        ])
        .env(INPUT, path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    #[cfg(target_os = "macos")]
    let peak = stderr
        .lines()
        .find(|line| line.contains("maximum resident set size"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    #[cfg(target_os = "linux")]
    let peak = stderr
        .lines()
        .find_map(|line| line.strip_prefix("DOCSPEC_MAXRSS_KIB="))
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024;
    println!(
        "peak_process_memory_bytes={peak}, configured_memory_bytes={MEMORY}, fixed_overhead_bytes={OVERHEAD}"
    );
    assert!(peak <= MEMORY + OVERHEAD, "{stdout}\n{stderr}");
    #[cfg(target_os = "macos")]
    {
        let footprint = stderr
            .lines()
            .find(|line| line.contains("peak memory footprint"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        println!("peak_physical_footprint_bytes={footprint}");
        assert!(footprint <= MEMORY + OVERHEAD, "{stdout}\n{stderr}");
    }
}
