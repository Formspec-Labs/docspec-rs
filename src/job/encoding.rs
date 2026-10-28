//! Admit known Parquet encoding copies before native writes.
// Rust guideline compliant 2026-02-21

#[cfg(test)]
mod tests {
    use crate::budget::{Budget, Limits};
    use arrow::{array::{Array, StringArray}, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch};
    use parquet::{arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder}, file::properties::{WriterProperties, EnabledStatistics}};
    use std::{io::Cursor, sync::Arc};

    fn budget(memory: usize) -> Budget {
        Budget::new(Limits { memory_bytes: memory, max_batch_bytes: 64 << 10, compute_threads: 1, ..Limits::default() }).unwrap()
    }
    fn batch(rows: usize, bytes: usize) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)])), vec![Arc::new(StringArray::from_iter_values(std::iter::repeat_n("x".repeat(bytes), rows)))]).unwrap()
    }
    fn writer(batch: &RecordBatch) -> ArrowWriter<Vec<u8>> {
        let properties = WriterProperties::builder().set_dictionary_enabled(false).set_statistics_enabled(EnabledStatistics::Chunk).set_max_row_group_bytes(Some(16 << 10)).build();
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap()
    }

    #[test]
    fn known_encoder_copies_fit_pieces_and_refuse_single_rows_before_write() {
        let budget = budget(512 << 10);
        let base = 2 * budget.limits.max_batch_bytes;
        let _base = budget.reserve(base).unwrap();
        let input = batch(40, 1500);
        let mut output = writer(&input);
        super::write(&mut output, &input, &budget, base, false, true).unwrap();
        let bytes = output.into_inner().unwrap();
        let read = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap().build().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(arrow::compute::concat_batches(&input.schema(), &read).unwrap(), input);

        let input = batch(1, 60 << 10);
        let mut output = writer(&input);
        let before = output.bytes_written();
        let error = super::write(&mut output, &input, &budget, base, false, true).unwrap_err();
        assert_eq!(error.code(), "capacity");
        assert_eq!(output.bytes_written(), before);
        assert_eq!(output.in_progress_rows(), 0);

        let larger = self::budget(2 << 20);
        let _base = larger.reserve(base).unwrap();
        let mut output = writer(&input);
        super::write(&mut output, &input, &larger, base, false, true).unwrap();
        let read = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(output.into_inner().unwrap())).unwrap().build().unwrap().next().unwrap().unwrap();
        assert_eq!(read.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "x".repeat(60 << 10));
    }
}
