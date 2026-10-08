//! Compare maintained Parquet codecs on retained output from the failed pilot.

use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use std::{fs::File, time::Instant};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(args.len() == 3, "usage: comments_codec INPUT OUTPUT CODEC");
    let compression = match args[2].as_str() {
        "none" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        _ => anyhow::bail!("codec must be none, snappy or zstd"),
    };
    let start = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&args[0])?)?
        .with_batch_size(4096)
        .build()?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4096))
        .set_compression(compression)
        .build();
    let mut file = File::create_new(&args[1])?;
    use arrow::record_batch::RecordBatchReader;
    let mut writer = ArrowWriter::try_new(&mut file, reader.schema(), Some(properties))?;
    let mut rows = 0;
    for batch in reader {
        let batch = batch?;
        rows += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;
    file.sync_all()?;
    println!(
        "{}",
        json!({"codec": format!("{compression:?}"), "rows": rows,
        "bytes": file.metadata()?.len(), "encode_seconds": start.elapsed().as_secs_f64()})
    );
    Ok(())
}
