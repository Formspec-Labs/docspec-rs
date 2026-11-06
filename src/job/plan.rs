//! Prepare calculations in the guarded job context without opening input files.
// Rust guideline compliant 2026-02-21
use super::Job;
use crate::{Error, Result, budget::Limits};
use arrow::datatypes::Schema;
use datafusion::{common::ScalarValue, dataframe::DataFrame, datasource::empty::EmptyTable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

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
                .register_table(name, Arc::new(EmptyTable::new(Arc::new(schema))))?;
        }
        match self.calculation {
            Calculation::Sql {
                sql,
                parameters,
                #[cfg(feature = "substrait")]
                save_substrait,
            } => {
                let frame = job.sql_bound(&sql, &parameters).await?;
                let schema = frame.schema().as_arrow().clone();
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
                    schema: frame.schema().as_arrow().clone(),
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
        let frame = self.sql(sql).await?;
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
        Ok(frame.with_param_values(values)?)
    }
}
