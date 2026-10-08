//! Diagnose native batch allocation without rewriting the retained Comments input.

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion::execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::TryStreamExt;
use serde_json::json;
use std::sync::Arc;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(worker_threads = 12)]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(args.len() == 3, "usage: comments_scan INPUT ROWS VIEWS");
    let batch_rows: usize = args[1].parse()?;
    let views: bool = args[2].parse()?;
    // Match the engine's reference execution pool, not a new production profile.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(12 << 30)))
        .with_max_temp_directory_size(0)
        .build()?;
    let config = SessionConfig::new()
        .with_batch_size(batch_rows)
        .with_target_partitions(12)
        .set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            views,
        )
        .set_bool(
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
            false,
        );
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    context
        .register_parquet("input", &args[0], ParquetReadOptions::default())
        .await?;
    let mut stream = context
        .sql("SELECT * FROM input")
        .await?
        .execute_stream()
        .await?;
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut oversized = 0u64;
    let mut maximum = 0usize;
    let mut largest_oversized_row = 0;
    let mut largest_oversized_values = 0;
    let mut maxima = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows() as u64;
        batches += 1;
        let bytes = batch.get_array_memory_size();
        if bytes > 64 << 20 {
            oversized += 1;
        }
        let prior_row_maximum = largest_oversized_row;
        let new_maximum = bytes > maximum;
        if new_maximum || bytes > 64 << 20 {
            let mut row_bytes = vec![0u64; batch.num_rows()];
            let mut columns = Vec::new();
            for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                let value_bytes = value_lengths(array.as_ref(), &mut row_bytes)?;
                if new_maximum {
                    columns.push(
                        json!({"name": field.name(), "type": format!("{:?}", array.data_type()),
                        "array_bytes": array.get_array_memory_size(), "value_bytes": value_bytes}),
                    );
                }
            }
            let value_bytes = row_bytes.iter().sum::<u64>();
            let max_row_bytes = row_bytes.iter().max().copied().unwrap_or(0);
            if bytes > 64 << 20 {
                largest_oversized_row = largest_oversized_row.max(max_row_bytes);
                largest_oversized_values = largest_oversized_values.max(value_bytes);
            }
            if new_maximum {
                maximum = bytes;
                let observation = json!({"batch_number": batches, "rows": batch.num_rows(),
                    "array_bytes": bytes, "string_value_bytes": value_bytes,
                    "max_row_string_bytes": max_row_bytes, "columns": columns});
                eprintln!("{observation}");
                maxima.push(observation);
            }
        }
        // Preserve useful diagnostics if a later batch or external stop fails.
        if largest_oversized_row > prior_row_maximum || batches.is_multiple_of(1024) {
            eprintln!(
                "{}",
                json!({"rows": rows, "batches": batches, "oversized_batches": oversized,
                "max_array_bytes": maximum, "largest_oversized_row_bytes": largest_oversized_row})
            );
        }
    }
    println!(
        "{}",
        json!({"batch_rows": batch_rows, "forced_views": views, "rows": rows,
        "batches": batches, "oversized_batches": oversized, "max_array_bytes": maximum,
        "max_row_value_bytes_in_oversized_batches": largest_oversized_row,
        "max_value_bytes_in_oversized_batches": largest_oversized_values,
        "allocation_maxima": maxima})
    );
    Ok(())
}

fn value_lengths(array: &dyn Array, row_bytes: &mut [u64]) -> anyhow::Result<u64> {
    let mut total = 0;
    macro_rules! count {
        ($array_type:ty) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("matching Arrow type");
            for (index, value) in values.iter().enumerate() {
                let len = value.map_or(0, str::len) as u64;
                row_bytes[index] += len;
                total += len;
            }
        }};
    }
    match array.data_type() {
        DataType::Utf8 => count!(StringArray),
        DataType::LargeUtf8 => count!(LargeStringArray),
        DataType::Utf8View => count!(StringViewArray),
        other => anyhow::bail!("Comments diagnostic expects strings, got {other:?}"),
    }
    Ok(total)
}
