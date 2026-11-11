//! Prepare calculations in the guarded job context without opening input files.
// Rust guideline compliant 2026-02-21
use super::Job;
use crate::{Error, Result, budget::Limits};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, ScalarValue},
    dataframe::DataFrame,
    logical_expr::{Expr, TableType},
    physical_plan::ExecutionPlan,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
pub(super) mod constants;

#[derive(Debug)]
pub(super) struct SchemaOnlyTable(pub(super) SchemaRef);

pub(super) fn inferred_schema(
    frame: &DataFrame,
    _budget: &crate::budget::Budget,
) -> crate::Result<Schema> {
    // Scalar-subquery decorrelation can change Arrow nullability. Describe the
    // maintained optimized plan, preserving its SessionState, without creating
    // a physical plan or asking schema-only providers to scan.
    let (state, plan) = frame.clone().into_parts();
    let owner = constants::owner(&state)?;
    let planning = owner.enter();
    let optimized = state.optimize(&plan);
    planning.check()?;
    Ok(optimized?.schema().as_arrow().clone())
}

/// The schema a materialized SQL step or an export declares: its optimized plan's, portable.
///
/// The one owner of every SQL step's and export's declared schema, plain or
/// managed (a components step declares its own fixed output): a step kept in
/// memory, the same step written to a file and an export of either then agree
/// on types, nullability and each column's own metadata (a lake column's
/// `PARQUET:field_id` among it). The running plan's schema can differ from the
/// planned one (see [`inferred_schema`]), so nothing is declared from a
/// running stream.
///
/// # Errors
/// Returns the planner's refusal of the frame's plan.
pub(super) fn declared_schema(
    frame: &DataFrame,
    budget: &crate::budget::Budget,
) -> crate::Result<SchemaRef> {
    let schema = inferred_schema(frame, budget)?;
    Ok(crate::export::portable_schema(&schema))
}

pub(super) async fn execute_stream(
    frame: DataFrame,
) -> crate::Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let (state, _) = frame.clone().into_parts();
    let owner = constants::owner(&state)?;
    let planning = owner.enter();
    let context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await;
    planning.check()?;
    let plan = super::native::admit_metadata_plan(plan?, owner);
    drop(planning);
    Ok(datafusion::physical_plan::execute_stream(plan, context)?)
}

#[async_trait::async_trait]
impl TableProvider for SchemaOnlyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "a schema-only input cannot scan rows".into(),
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaInput {
    name: String,
    schema: Schema,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
enum Calculation {
    Sql {
        sql: String,
        #[serde(default)]
        parameters: BTreeMap<String, Value>,
        #[cfg(feature = "substrait")]
        save_substrait: Option<super::substrait::PlanDestinations>,
    },
    #[cfg(feature = "substrait")]
    Substrait { path: PathBuf, sha256: String },
}

/// A schema-only calculation request with no input path or native catalog access.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobPlanSpec {
    scratch: PathBuf,
    inputs: Vec<SchemaInput>,
    calculation: Calculation,
}

/// Inferred schema and separate optional observations about the saved format.
#[derive(Debug, Serialize)]
pub struct JobPlanResult {
    format: &'static str,
    schema: Schema,
    #[cfg(feature = "substrait")]
    #[serde(skip_serializing_if = "Option::is_none")]
    substrait: Option<super::substrait::SubstraitReceipt>,
}

impl JobPlanSpec {
    /// Infers the output using only explicitly provided input schemas.
    ///
    /// # Errors
    /// Refuses invalid bindings, unsupported calculations, and capacity or artifact failures.
    pub async fn prepare(self, limits: Limits) -> Result<JobPlanResult> {
        let job = Job::new(limits, self.scratch)?;
        let mut names = BTreeSet::new();
        for SchemaInput { name, schema } in self.inputs {
            if name.is_empty() || !names.insert(name.to_lowercase()) {
                return Err(Error::refusal(
                    "invalid_spec",
                    "duplicate or empty input name",
                ));
            }
            job.context
                .register_table(name, Arc::new(SchemaOnlyTable(Arc::new(schema))))?;
        }
        match self.calculation {
            Calculation::Sql {
                sql,
                parameters,
                #[cfg(feature = "substrait")]
                save_substrait,
            } => {
                let frame = job.sql_bound(&sql, &parameters).await?;
                let schema = inferred_schema(&frame, &job.budget)?;
                #[cfg(feature = "substrait")]
                let substrait = save_substrait
                    .map(|destinations| job.save_substrait(frame, &destinations))
                    .transpose()?;
                Ok(JobPlanResult {
                    format: "sql",
                    schema,
                    #[cfg(feature = "substrait")]
                    substrait,
                })
            }
            #[cfg(feature = "substrait")]
            Calculation::Substrait { path, sha256 } => {
                let (frame, receipt, _memory) = job
                    .read_substrait(&super::substrait::SavedPlan { path, sha256 })
                    .await?;
                Ok(JobPlanResult {
                    format: "substrait",
                    schema: inferred_schema(&frame, &job.budget)?,
                    substrait: Some(receipt),
                })
            }
        }
    }
}

impl Job {
    pub(super) async fn sql_bound(
        &self,
        sql: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<DataFrame> {
        let frame = self.sql_unbound(sql).await?;
        let names = frame.logical_plan().get_parameter_names()?;
        if names.len() != parameters.len()
            || names
                .iter()
                .any(|name| !parameters.contains_key(name.trim_start_matches('$')))
        {
            return Err(Error::refusal(
                "invalid_parameters",
                "SQL placeholder names must match the supplied parameters exactly",
            ));
        }
        if parameters.is_empty() {
            return Ok(frame);
        }
        let values = parameters
            .iter()
            .map(|(name, value)| {
                let scalar = match value {
                    Value::Null => ScalarValue::Null,
                    Value::Bool(value) => ScalarValue::Boolean(Some(*value)),
                    Value::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    Value::Number(value) if value.is_i64() => ScalarValue::Int64(value.as_i64()),
                    Value::Number(value) if value.is_f64() => ScalarValue::Float64(value.as_f64()),
                    _ => {
                        return Err(Error::refusal(
                            "unsupported_parameters",
                            "SQL parameters support null, boolean, string, signed 64-bit integer and double",
                        ));
                    }
                };
                Ok((name.clone(), scalar))
            })
            .collect::<Result<Vec<_>>>()?;
        let frame = frame.with_param_values(values)?;
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[tokio::test]
    async fn optimizer_literal_expansion_refuses_before_evaluation_including_subqueries_and_parameters()
     {
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            memory_bytes: 16 << 20,
            compute_threads: 1,
            scratch_bytes: 4 << 20,
            batch_rows: 16,
            max_batch_bytes: 512,
            max_metadata_bytes: 512,
        };
        let job = Job::new(limits, root.path().join("scratch")).unwrap();
        for sql in [
            "SELECT repeat('x',4096)",
            "SELECT length(repeat('x',4096))",
            "SELECT (SELECT length(repeat('x',4096))) AS n",
            "SELECT length(repeat(s,4096)) FROM (SELECT 'x' AS s)",
            "SELECT length(repeat('x',CASE WHEN TRUE THEN 4096 ELSE value END)) FROM generate_series(1,1)",
            "SELECT length(repeat(CASE WHEN TRUE THEN 'x' ELSE CAST(value AS VARCHAR) END,4096)) FROM generate_series(1,1)",
            "SELECT array_length(generate_series(1,4096))",
            "SELECT length(repeat(array_element(make_array('x'),1),4096))",
            "SELECT array_length(array_repeat(CAST(NULL AS BIGINT),4096))",
            "SELECT array_length(array_resize(make_array(1),4096,CAST(NULL AS BIGINT)))",
        ] {
            let frame = job.sql(sql).await.unwrap();
            assert_eq!(
                inferred_schema(&frame, &job.budget).unwrap_err().code(),
                "capacity",
                "{sql}"
            );
        }
        let frame = job
            .sql_bound(
                "SELECT length(repeat('x',$n))",
                &BTreeMap::from([("n".into(), serde_json::json!(4096))]),
            )
            .await
            .unwrap();
        assert_eq!(
            inferred_schema(&frame, &job.budget).unwrap_err().code(),
            "capacity"
        );
        let frame = job.sql("SELECT length(repeat('x',8)) AS n").await.unwrap();
        inferred_schema(&frame, &job.budget).unwrap();
        use futures::TryStreamExt;
        let batches = execute_stream(frame)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0)
                .unwrap()
                .to_string(),
            "8"
        );
        let frame = job.sql("SELECT arrow_cast(array_slice(make_array(CAST(NULL AS VARCHAR)),1,0),'List(Utf8)') AS empty_values, named_struct('id',CAST(NULL AS BIGINT)) AS item, arrow_cast(NULL,'FixedSizeBinary(32)') AS body").await.unwrap();
        inferred_schema(&frame, &job.budget).unwrap();
    }

    #[tokio::test]
    async fn native_array_resize_admits_the_explicit_fill_width() {
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            memory_bytes: 16 << 20,
            compute_threads: 1,
            scratch_bytes: 4 << 20,
            batch_rows: 16,
            max_batch_bytes: 4096,
            max_metadata_bytes: 4096,
        };
        let job = Job::new(limits, root.path().join("scratch")).unwrap();
        let sql = format!(
            "SELECT array_length(array_resize(make_array('a'),8,'{}')) AS n",
            "z".repeat(1024)
        );
        let frame = job.sql(&sql).await.unwrap();
        assert_eq!(
            inferred_schema(&frame, &job.budget).unwrap_err().code(),
            "capacity"
        );
    }

    #[tokio::test]
    async fn scalar_planning_admission_survives_late_execution_and_stream_ownership() {
        use futures::TryStreamExt;
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            memory_bytes: 16 << 20,
            compute_threads: 1,
            scratch_bytes: 4 << 20,
            batch_rows: 16,
            max_batch_bytes: 1024,
            max_metadata_bytes: 1024,
        };
        let budget =
            crate::budget::Budget::in_workspace(limits, &root.path().join("scratch")).unwrap();
        let job = Job::from_budget(budget.clone()).unwrap();
        let frame = job
            .sql("SELECT repeat(array_element(make_array('x'),1),8) AS value")
            .await
            .unwrap();
        let owner = constants::owner(&frame.clone().into_parts().0).unwrap();
        assert_eq!(owner.admitted_bytes(), 0);
        // SQL construction has not reserved an unused planning allowance.
        // The later maintained planning boundary must still admit its work.
        let remaining = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
        assert_eq!(
            execute_stream(frame).await.err().unwrap().code(),
            "capacity"
        );
        assert_eq!(owner.admitted_bytes(), 0);
        drop(remaining);
        let frame = job
            .sql("SELECT repeat(array_element(make_array('x'),1),8) AS value")
            .await
            .unwrap();
        let mut stream = execute_stream(frame).await.unwrap();
        let charge = owner.admitted_bytes();
        assert!(charge > 0);
        let remaining = budget
            .reserve(budget.limits.memory_bytes / 2 - charge)
            .unwrap();
        drop(job);
        drop(owner);
        assert!(
            budget.reserve(charge).is_err(),
            "planning memory remains owned while only the native stream is live"
        );
        let batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batch.column(0), 0)
                .unwrap()
                .to_string(),
            "xxxxxxxx"
        );
        drop(batch);
        drop(stream);
        let _released = budget.reserve(charge).unwrap();
        drop(remaining);
    }

    #[tokio::test]
    async fn schema_only_preparation_uses_a_provider_that_refuses_scans() {
        let root = tempfile::tempdir().unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]);
        let sql = "SELECT region, id FROM orders WHERE id > 2";
        let result = JobPlanSpec {
            scratch: root.path().join("prepare"),
            inputs: vec![SchemaInput {
                name: "orders".into(),
                schema: schema.clone(),
            }],
            calculation: Calculation::Sql {
                sql: sql.into(),
                parameters: BTreeMap::new(),
                #[cfg(feature = "substrait")]
                save_substrait: None,
            },
        }
        .prepare(Limits::default())
        .await
        .unwrap();
        assert_eq!(
            result.schema,
            Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
            ])
        );

        let job = Job::new(Limits::default(), root.path().join("canary")).unwrap();
        job.context
            .register_table("orders", Arc::new(SchemaOnlyTable(Arc::new(schema))))
            .unwrap();
        let error = job.sql(sql).await.unwrap().collect().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("a schema-only input cannot scan rows")
        );
    }
}
