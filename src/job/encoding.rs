//! Admit known Parquet encoding copies before native writes.
// Rust guideline compliant 2026-02-21
use crate::{Result, budget::Budget};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::io::Write;

/// Admit the known live input, encoded buffer growth and statistics copies.
/// Parquet58 byte_array.rs::compute_min_max copies both full values; the V2
/// delta fallback also retains last_value. Use the portable piece size as a
/// conservative bound for each copy, including non-byte fields. The encoded
/// buffer term includes Vec growth. Native compression/dictionary workspace
/// still has no public pre-allocation estimate; this is not a global RSS bound.
pub(super) fn write<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    batch: &RecordBatch,
    budget: &Budget,
    writer_memory: &mut tokio::sync::OwnedSemaphorePermit,
    base_reserved: usize,
    delta: bool,
    flush: bool,
) -> Result<()> {
    let input = batch.get_array_memory_size();
    let mut offset = 0;
    let mut target = batch.num_rows();
    while offset < batch.num_rows() {
        let take = target.min(batch.num_rows() - offset);
        let piece = batch.slice(offset, take);
        let logical = crate::export::portable_batch_bytes(&piece)?;
        let encoded_growth = logical.saturating_mul(2);
        let statistics_copies = logical.saturating_mul(2);
        let delta_last_value = if delta { logical } else { 0 };
        let required = input
            .saturating_add(writer.memory_size())
            .saturating_add(encoded_growth)
            .saturating_add(statistics_copies)
            .saturating_add(delta_last_value);
        let reserved = writer_memory.num_permits();
        let mut admission = match budget.reserve(required.saturating_sub(reserved)) {
            Ok(admission) => admission,
            Err(_) if flush && take > 1 => {
                target = take / 2;
                continue;
            }
            Err(_) if flush && writer.in_progress_rows() > 0 => {
                // The caller still owns the fixed input/retained-state
                // reservation. Release that group before refusing a row that
                // can fit by itself; no new input is written during retry.
                writer.flush()?;
                continue;
            }
            Err(error) => return Err(error),
        };
        writer.write(&piece)?;
        if flush && writer.memory_size() > budget.limits.max_batch_bytes {
            writer.flush()?;
        }
        // Retained encoder state stays covered by `writer_memory` before
        // releasing the transient copies. Native auto-flush is covered too.
        if !flush {
            let retained = writer.memory_size().max(base_reserved);
            if retained > reserved {
                let needed = retained - reserved;
                let from_admission = needed.min(admission.num_permits());
                if from_admission > 0
                    && let Some(transferred) = admission.split(from_admission)
                {
                    writer_memory.merge(transferred);
                }
                let current = writer_memory.num_permits();
                if retained > current {
                    writer_memory.merge(budget.reserve(retained - current)?);
                }
            } else if retained < reserved {
                drop(writer_memory.split(reserved - retained));
            }
        }
        drop(admission);
        offset += take;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::budget::{Budget, Limits};
    use arrow::{
        array::{Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        file::properties::{EnabledStatistics, WriterProperties},
    };
    use std::sync::Arc;

    fn budget(memory: usize) -> Budget {
        Budget::new(Limits {
            memory_bytes: memory,
            max_batch_bytes: 64 << 10,
            compute_threads: 1,
            ..Limits::default()
        })
        .unwrap()
    }
    fn batch(rows: usize, bytes: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n("x".repeat(bytes), rows),
            ))],
        )
        .unwrap()
    }
    fn writer(batch: &RecordBatch) -> ArrowWriter<Vec<u8>> {
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_bytes(Some(16 << 10))
            .build();
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap()
    }

    #[test]
    fn known_encoder_copies_fit_pieces_and_refuse_single_rows_before_write() {
        let budget = budget(512 << 10);
        let base = 2 * budget.limits.max_batch_bytes;
        let mut base_permit = budget.reserve(base).unwrap();
        let input = batch(40, 1500);
        let mut output = writer(&input);
        super::write(
            &mut output,
            &input,
            &budget,
            &mut base_permit,
            base,
            false,
            true,
        )
        .unwrap();
        let bytes = output.into_inner().unwrap();
        let read = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            arrow::compute::concat_batches(&input.schema(), &read).unwrap(),
            input
        );

        let input = batch(1, 60 << 10);
        let mut output = writer(&input);
        let before = output.bytes_written();
        let error = super::write(
            &mut output,
            &input,
            &budget,
            &mut base_permit,
            base,
            false,
            true,
        )
        .unwrap_err();
        assert_eq!(error.code(), "capacity");
        assert_eq!(output.bytes_written(), before);
        assert_eq!(output.in_progress_rows(), 0);

        let larger = self::budget(2 << 20);
        let mut base_permit = larger.reserve(base).unwrap();
        let mut output = writer(&input);
        super::write(
            &mut output,
            &input,
            &larger,
            &mut base_permit,
            base,
            false,
            true,
        )
        .unwrap();
        let read = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(
            output.into_inner().unwrap(),
        ))
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
        assert_eq!(
            read.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "x".repeat(60 << 10)
        );
    }

    #[test]
    fn retained_group_is_flushed_before_refusing_the_next_fitting_row() {
        let budget = budget(288 << 10);
        let base = 2 * budget.limits.max_batch_bytes;
        let mut base_permit = budget.reserve(base).unwrap();
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![
                "x".repeat(15 << 10),
                "y".repeat(20 << 10),
            ]))],
        )
        .unwrap();
        let mut output = writer(&input);
        super::write(
            &mut output,
            &input,
            &budget,
            &mut base_permit,
            base,
            false,
            true,
        )
        .unwrap();
        let read = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(
            output.into_inner().unwrap(),
        ))
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(
            arrow::compute::concat_batches(&input.schema(), &read).unwrap(),
            input
        );
    }
}
