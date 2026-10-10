//! Requested columns mapped to unique native dependencies and exact output order.

use crate::{Error, Result, budget::Budget};
use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use iceberg::{metadata_columns::RESERVED_FIELD_ID_FILE, spec::Schema};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
enum Column {
    Native(usize),
    File,
    Row,
}

#[derive(Debug)]
pub(super) struct Selection {
    pub fields: Vec<i32>,
    schema: SchemaRef,
    columns: Vec<Column>,
    file: bool,
    row: bool,
    _memory: OwnedSemaphorePermit,
}

impl Selection {
    pub fn new(
        native: &Schema,
        schema: &SchemaRef,
        indices: &[usize],
        located: bool,
        materialize_origins: bool,
        budget: &Budget,
    ) -> Result<Self> {
        let roots = native.as_struct().fields();
        if indices.iter().any(|index| *index >= schema.fields().len()) {
            return Err(Error::refusal(
                "invalid_projection",
                "field is outside the captured schema",
            ));
        }
        // Cover the owned mapping, projected schema handles, temporary index maps
        // and a task's field-ID copy before allocating them. Field values are shared.
        let mapping_bytes = indices
            .len()
            .saturating_mul(size_of::<Column>())
            .saturating_add(roots.len().saturating_mul(
                size_of::<bool>() + size_of::<Option<usize>>() + 2 * size_of::<i32>(),
            ))
            .saturating_add(size_of::<Self>() + size_of::<i32>());
        let memory = reserve_projection(budget, schema, indices.len(), mapping_bytes)?;
        let output = Arc::new(schema.project(indices)?);
        let file = located && indices.contains(&roots.len());
        let row = located && indices.contains(&(roots.len() + 1));
        let mut needed = vec![false; roots.len()];
        for &index in indices {
            if index < roots.len() {
                needed[index] = true;
            }
        }
        if materialize_origins {
            let origin_file = roots
                .iter()
                .position(|field| field.name == super::identity::ORIGIN_FILE);
            let origin_row = roots
                .iter()
                .position(|field| field.name == super::identity::ORIGIN_ROW);
            let origin_requested = origin_file.is_some_and(|index| needed[index])
                || origin_row.is_some_and(|index| needed[index]);
            if (file || row || origin_requested) && super::identity::has_origins(native)? {
                needed[origin_file.expect("validated origin file")] = true;
                needed[origin_row.expect("validated origin row")] = true;
            }
        }
        let mut positions = vec![None; roots.len()];
        let mut fields = Vec::with_capacity(needed.iter().filter(|needed| **needed).count() + 1);
        for (index, field) in roots.iter().enumerate() {
            if needed[index] {
                positions[index] = Some(fields.len());
                fields.push(field.id);
            }
        }
        // Force the maintained ID-aware transform, including all-absent selections.
        fields.push(RESERVED_FIELD_ID_FILE);
        let columns = indices
            .iter()
            .map(|&index| {
                if index < roots.len() {
                    Column::Native(positions[index].expect("requested native dependency"))
                } else if located && index == roots.len() {
                    Column::File
                } else {
                    Column::Row
                }
            })
            .collect();
        Ok(Self {
            fields,
            schema: output,
            columns,
            file,
            row,
            _memory: memory,
        })
    }

    pub fn apply(
        &self,
        batch: RecordBatch,
        location: Option<(&str, u64, u64)>,
        limit: usize,
    ) -> Result<RecordBatch> {
        let (file, row) = if self.file || self.row {
            let (path, start, end) = location.ok_or_else(|| {
                Error::refusal(
                    "invalid_projection",
                    "occurrence selection requires a complete-file reader",
                )
            })?;
            super::occurrences::location_columns(
                &batch, path, start, end, self.file, self.row, limit,
            )?
        } else {
            (None, None)
        };
        let columns = self
            .columns
            .iter()
            .map(|column| match column {
                Column::Native(index) => batch.column(*index).clone(),
                Column::File => file.as_ref().expect("requested file column").clone(),
                Column::Row => row.as_ref().expect("requested row column").clone(),
            })
            .collect();
        let result = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;
        if result.get_array_memory_size() > limit {
            return Err(Error::refusal(
                "oversized_batch",
                "selected output exceeds the reader byte allowance",
            ));
        }
        Ok(result)
    }
}

/// Admits the mapping and projected schema before allocating either.
pub(crate) fn reserve_projection(
    budget: &Budget,
    schema: &SchemaRef,
    columns: usize,
    mapping_bytes: usize,
) -> Result<OwnedSemaphorePermit> {
    // Arrow project shares Field objects, builds a field-handle vector, converts
    // it to an Arc slice, and clones schema metadata. Cover both handle arrays
    // while conversion is in progress. Metadata strings use owned copies.
    let metadata_bytes = schema
        .metadata()
        .iter()
        .fold(0usize, |bytes, (key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        });
    // Reserve spare map buckets and their control bytes conservatively; this
    // admission estimate is not an allocator-level measurement.
    let metadata_buckets = schema
        .metadata()
        .capacity()
        .saturating_mul(2)
        .saturating_add(1);
    let bytes = columns
        .checked_mul(2 * size_of::<Arc<arrow::datatypes::Field>>())
        .and_then(|bytes| {
            bytes.checked_add(mapping_bytes.saturating_add(size_of::<arrow::datatypes::Schema>()))
        })
        .and_then(|bytes| bytes.checked_add(metadata_bytes))
        .and_then(|bytes| {
            bytes.checked_add(metadata_buckets.saturating_mul(2 * size_of::<String>() + 1))
        })
        .ok_or_else(|| {
            crate::Error::refusal("metadata_capacity", "column selection is too large")
        })?;
    if bytes > budget.limits.max_metadata_bytes {
        return Err(crate::Error::refusal(
            "metadata_capacity",
            "column selection exceeds metadata allowance",
        ));
    }
    budget.reserve(bytes)
}
