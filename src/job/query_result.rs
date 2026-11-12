//! Bounded SQL results for manifests, serialized by the maintained Arrow writer.
// Rust guideline compliant 2026-02-21
use super::Job;
use crate::{Error, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    sync::Arc,
};

/// A bounded named result returned beside exported file identities.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    pub data: serde_json::Value,
    #[serde(skip)]
    _memory: Option<Arc<MemoryReservation>>,
    #[serde(skip)]
    _metadata: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl QueryResult {
    pub(crate) fn retained(
        name: &str,
        columns: Vec<String>,
        data: serde_json::Value,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            name: name.into(),
            columns,
            data,
            _memory: None,
            _metadata: Some(Arc::new(memory)),
        }
    }
}

pub(super) fn ensure_distinct_columns(
    name: &str,
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<String>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if !seen.insert(field.name().as_str()) {
            return Err(Error::refusal(
                "invalid_sql",
                format!(
                    "SQL query {name:?} repeats output column {:?}; give each column a distinct alias",
                    field.name()
                ),
            ));
        }
        columns.push(field.name().clone());
    }
    Ok(columns)
}

pub(super) async fn plan_query_frame(
    job: &Job,
    name: &str,
    sql: &str,
) -> Result<(datafusion::dataframe::DataFrame, Vec<String>)> {
    let frame = job.sql(sql).await.map_err(|error| {
        if error
            .message()
            .contains("Projections require unique expression names")
        {
            Error::refusal("invalid_sql", error.message())
        } else {
            error
        }
    })?;
    let columns = ensure_distinct_columns(name, frame.schema().as_arrow())?;
    Ok((frame, columns))
}

struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
    memory: MemoryReservation,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("SQL result exceeds metadata allowance"))?;
        // Keep encoded bytes and native JSON-tree admission until result drop.
        let allowance = size
            .checked_mul(32)
            .ok_or_else(|| io::Error::other("SQL result allocation overflow"))?;
        self.memory
            .try_resize(allowance)
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) async fn query(job: &Job, name: &str, sql: &str) -> Result<QueryResult> {
    Ok(query_with_joins(job, name, sql).await?.0)
}

pub(super) async fn query_with_joins(
    job: &Job,
    name: &str,
    sql: &str,
) -> Result<(QueryResult, Vec<super::metrics::JoinSummary>)> {
    let (frame, _) = plan_query_frame(job, name, sql).await?;
    let (plan, mut stream) = super::plan::execute_plan_stream(frame).await?;
    let columns = ensure_distinct_columns(name, &stream.schema())?;
    let buffer = Buffer {
        bytes: Vec::new(),
        limit: job.budget.limits.max_metadata_bytes,
        memory: MemoryConsumer::new("SQL metadata result")
            .register(&job.budget.runtime_env().memory_pool),
    };
    let mut writer = arrow::json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow::json::writer::JsonArray>(buffer);
    while let Some(batch) = stream.try_next().await? {
        let size = batch.get_array_memory_size();
        if size > job.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "SQL result batch exceeds metadata allowance",
            ));
        }
        let _encoding = job.budget.reserve(size.checked_mul(8).ok_or_else(|| {
            Error::refusal("capacity", "SQL result encoding allowance overflow")
        })?)?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    let buffer = writer.into_inner();
    let data = serde_json::from_slice(&buffer.bytes)?;
    Ok((
        QueryResult {
            name: name.into(),
            columns,
            data,
            _memory: Some(Arc::new(buffer.memory)),
            _metadata: None,
        },
        super::materialized::summarize_joins(&plan),
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        budget::Limits,
        job::{Job, JobSpec},
    };
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
        record_batch::RecordBatch,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn test_limits() -> Limits {
        Limits {
            compute_threads: 2,
            memory_bytes: 32 << 20,
            scratch_bytes: 16 << 20,
            max_metadata_bytes: 2 << 20,
            max_batch_bytes: 2 << 20,
            batch_rows: 16,
        }
    }

    #[tokio::test]
    async fn query_refuses_duplicate_output_column_names() {
        let root = tempfile::tempdir().unwrap();
        for (label, sql) in [
            ("explicit_alias", "SELECT 1 AS sha, 2 AS sha"),
            ("empty_batches", "SELECT 1 AS sha, 2 AS sha WHERE false"),
            (
                "join_qualified",
                "SELECT k.sha, c.sha FROM k LEFT JOIN c ON c.sha = k.sha",
            ),
            ("join_star", "SELECT * FROM k LEFT JOIN c ON c.sha = k.sha"),
        ] {
            let spec: JobSpec = serde_json::from_value(json!({
                "scratch": root.path().join(label),
                "inputs": [],
                "steps": [
                    {"name": "k", "sql": "SELECT 'a' AS sha UNION ALL SELECT 'b' AS sha"},
                    {"name": "c", "sql": "SELECT 'a' AS sha"}
                ],
                "outputs": [],
                "queries": [{"name": "dup_rows", "sql": sql}]
            }))
            .unwrap();
            let err = spec.run(test_limits()).await.expect_err(label).error;
            assert_eq!(err.code(), "invalid_sql", "{label}: {err}");
            assert!(
                err.to_string().contains("sha"),
                "{label}: expected column name in {err}"
            );
        }

        // Distinct aliases over the same join succeed and keep both values.
        let ok_spec: JobSpec = serde_json::from_value(json!({
            "scratch": root.path().join("distinct"),
            "inputs": [],
            "steps": [
                {"name": "k", "sql": "SELECT 'a' AS sha UNION ALL SELECT 'b' AS sha"},
                {"name": "c", "sql": "SELECT 'a' AS sha"}
            ],
            "outputs": [],
            "queries": [{
                "name": "distinct_rows",
                "sql": "SELECT k.sha AS k_sha, c.sha AS c_sha FROM k LEFT JOIN c ON c.sha = k.sha ORDER BY k.sha"
            }]
        }))
        .unwrap();
        let res = ok_spec.run(test_limits()).await.unwrap();
        assert_eq!(res.queries[0].columns, vec!["k_sha", "c_sha"]);
        assert_eq!(
            res.queries[0].data,
            json!([
                {"k_sha": "a", "c_sha": "a"},
                {"k_sha": "b", "c_sha": null}
            ])
        );

        // Managed-job `report_ipc` and `report_result` also refuse duplicate output columns.
        let job = Job::new(test_limits(), root.path().join("managed")).unwrap();
        for sql in [
            "SELECT 1 AS dup_col, 2 AS dup_col",
            "SELECT l.v, r.v FROM (SELECT 1 AS v) l LEFT JOIN (SELECT 1 AS v) r ON l.v = r.v",
        ] {
            let ipc_err = job.report_ipc("dup_ipc", sql).await.unwrap_err();
            assert_eq!(ipc_err.code(), "invalid_sql", "{ipc_err}");
        }

        let dup_schema = Arc::new(Schema::new(vec![
            Field::new("dup_col", DataType::Int64, false),
            Field::new("dup_col", DataType::Int64, false),
        ]));
        let dup_batch = RecordBatch::try_new(
            dup_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let mut ipc_bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_bytes, &dup_schema).unwrap();
            writer.write(&dup_batch).unwrap();
            writer.finish().unwrap();
        }
        let result_err = job.report_result("dup_res", &ipc_bytes).unwrap_err();
        assert_eq!(result_err.code(), "invalid_sql", "{result_err}");

        // Schema-only `prepare-job` also refuses duplicate query columns up front.
        for (idx, sql) in [
            "SELECT 1 AS same, 2 AS same",
            "SELECT l.v, r.v FROM (SELECT 1 AS v) l LEFT JOIN (SELECT 1 AS v) r ON l.v = r.v",
        ]
        .into_iter()
        .enumerate()
        {
            let prep_bytes = serde_json::to_vec(&json!({
                "scratch": root.path().join("must-not-exist"),
                "inputs": [],
                "steps": [],
                "outputs": [],
                "queries": [{"name": "dup_prep", "sql": sql}]
            }))
            .unwrap();
            let prep_path = root.path().join(format!("prep-{idx}.json"));
            std::fs::write(&prep_path, &prep_bytes).unwrap();
            let prep_sha = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&prep_bytes));
            let prep_req: crate::job::PrepareJobSpec = serde_json::from_value(json!({
                "job": {"path": prep_path, "sha256": prep_sha},
                "inputs": []
            }))
            .unwrap();
            let prep_err = prep_req.prepare(test_limits()).await.unwrap_err();
            assert_eq!(prep_err.code(), "invalid_sql", "{prep_err}");
        }
    }
}
