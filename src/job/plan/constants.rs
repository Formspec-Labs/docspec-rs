//! Admit native scalar expansion when the maintained kernel is actually invoked.
// Rust guideline compliant 2026-02-21
use crate::{Error, Result, budget::Budget};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::{
    common::{DataFusionError, ExprSchema, ScalarValue},
    execution::SessionState,
    logical_expr::{
        ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature,
    },
    prelude::SessionContext,
};
use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
pub(in crate::job) struct Scope {
    charges: Mutex<Charges>,
    budget: Budget,
    limit: usize,
    planning: AtomicUsize,
    failures: AtomicUsize,
    refusal: AtomicU8,
}
#[derive(Debug)]
struct Charges {
    used: usize,
    memory: tokio::sync::OwnedSemaphorePermit,
}
pub(in crate::job) struct Planning {
    scope: Arc<Scope>,
    before: usize,
}
impl Drop for Planning {
    fn drop(&mut self) {
        self.scope.planning.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Planning {
    pub(in crate::job) fn check(&self) -> Result<()> {
        if self.scope.failures.load(Ordering::Acquire) == self.before {
            Ok(())
        } else {
            Err(self.scope.error())
        }
    }
}
impl Scope {
    pub(in crate::job) fn enter(self: &Arc<Self>) -> Planning {
        self.planning.fetch_add(1, Ordering::AcqRel);
        Planning {
            scope: self.clone(),
            before: self.failures.load(Ordering::Acquire),
        }
    }
    fn error(&self) -> Error {
        if self.refusal.load(Ordering::Acquire) == 2 {
            Error::refusal(
                "unsupported_literal_expansion",
                "native scalar arguments are outside the bounded expansion profile",
            )
        } else {
            Error::refusal(
                "capacity",
                "native scalar expansion exceeds its admitted metadata or batch allowance",
            )
        }
    }
    fn refuse(&self, kind: u8) -> DataFusionError {
        self.refusal.store(kind, Ordering::Release);
        self.failures.fetch_add(1, Ordering::AcqRel);
        DataFusionError::ResourcesExhausted(self.error().message().into())
    }
    fn charge(&self, bytes: usize) -> datafusion::common::Result<()> {
        let mut charges = self.charges.lock().map_err(|_| self.refuse(1))?;
        let used = charges
            .used
            .checked_add(bytes)
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| self.refuse(1))?;
        // Admit actual kernel work, including conservative native optimizer
        // copies, before invoking it. Empty contexts consume no literal budget.
        let memory = self
            .budget
            .reserve(bytes.checked_mul(4).ok_or_else(|| self.refuse(1))?)
            .map_err(|_| self.refuse(1))?;
        charges.memory.merge(memory);
        charges.used = used;
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::job) fn admitted_bytes(&self) -> usize {
        self.charges.lock().unwrap().memory.num_permits()
    }
}

pub(in crate::job) fn register(context: &SessionContext, budget: &Budget) -> Result<()> {
    // Keep cumulative folded-value estimates within this Job's allowance.
    // Reserve four times each actual estimate lazily from the shared host pool,
    // rather than charging every empty context its entire possible allowance.
    let limit = budget
        .limits
        .max_metadata_bytes
        .min(budget.limits.max_batch_bytes)
        .min((budget.limits.memory_bytes / 32).max(1));
    let scope = Arc::new(Scope {
        charges: Mutex::new(Charges {
            used: 0,
            memory: budget.reserve(0)?,
        }),
        budget: budget.clone(),
        limit,
        planning: AtomicUsize::new(0),
        failures: AtomicUsize::new(0),
        refusal: AtomicU8::new(0),
    });
    let state = context.state();
    let mut functions = std::collections::BTreeMap::new();
    for function in state.scalar_functions().values() {
        if matches!(
            function.name(),
            "repeat"
                | "lpad"
                | "rpad"
                | "array_repeat"
                | "array_resize"
                | "array_range"
                | "range"
                | "generate_series"
                | "array_fill"
        ) {
            functions.insert(function.name().to_owned(), function.clone());
        }
    }
    for native in functions.into_values() {
        context.register_udf(ScalarUDF::from(Admitted {
            native,
            scope: scope.clone(),
        }));
    }
    Ok(())
}
pub(in crate::job) fn owner(state: &SessionState) -> Result<Arc<Scope>> {
    state
        .scalar_functions()
        .values()
        .find_map(|function| {
            function
                .inner()
                .as_any()
                .downcast_ref::<Admitted>()
                .map(|function| function.scope.clone())
        })
        .ok_or_else(|| {
            Error::refusal(
                "native_scalar_admission",
                "native Job has no scalar admission owner",
            )
        })
}
pub(in crate::job) fn byte_limit(function: &ScalarUDF) -> Option<usize> {
    function
        .inner()
        .as_any()
        .downcast_ref::<Admitted>()
        .map(|function| function.scope.budget.limits.max_batch_bytes)
}

#[derive(Debug)]
struct Admitted {
    native: Arc<ScalarUDF>,
    scope: Arc<Scope>,
}
impl PartialEq for Admitted {
    fn eq(&self, other: &Self) -> bool {
        self.native == other.native && Arc::ptr_eq(&self.scope, &other.scope)
    }
}
impl Eq for Admitted {}
impl Hash for Admitted {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.native.hash(state);
        Arc::as_ptr(&self.scope).hash(state);
    }
}
impl ScalarUDFImpl for Admitted {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.native.name()
    }
    fn aliases(&self) -> &[String] {
        self.native.aliases()
    }
    #[expect(
        deprecated,
        reason = "preserve the pinned native scalar display-name delegation"
    )]
    fn display_name(&self, args: &[Expr]) -> datafusion::common::Result<String> {
        self.native.display_name(args)
    }
    fn schema_name(&self, args: &[Expr]) -> datafusion::common::Result<String> {
        self.native.schema_name(args)
    }
    fn signature(&self) -> &Signature {
        self.native.signature()
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        self.native.return_type(args)
    }
    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        self.native.return_field_from_args(args)
    }
    #[expect(deprecated, reason = "preserve maintained scalar nullability behavior")]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.native.is_nullable(args, schema)
    }
    fn coerce_types(&self, args: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        self.native.coerce_types(args)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let planning = self.scope.planning.load(Ordering::Acquire) > 0;
        // Constant folding can pass one-row arrays produced by a constructor.
        // Check actual argument values, never their earlier SQL syntax or shape.
        let bytes = invocation_bytes(self.name(), &args.args, args.number_rows)
            .map_err(|_| self.scope.refuse(2))?;
        let _memory = if planning {
            self.scope
                .charge(bytes.checked_add(128).ok_or_else(|| self.scope.refuse(1))?)?;
            None
        } else {
            if bytes > self.scope.budget.limits.max_batch_bytes {
                if args.number_rows <= 1 {
                    return Err(self.scope.refuse(1));
                }
                return Err(super::super::fitting::smaller(
                    args.number_rows
                        .saturating_mul(self.scope.budget.limits.max_batch_bytes)
                        .checked_div(bytes)
                        .unwrap_or(0)
                        .max(1),
                ));
            }
            Some(
                self.scope
                    .budget
                    .reserve(bytes.saturating_mul(4))
                    .map_err(|_| {
                        if args.number_rows > 1 {
                            super::super::fitting::smaller(args.number_rows / 2)
                        } else {
                            self.scope.refuse(1)
                        }
                    })?,
            )
        };
        self.native.invoke_with_args(args)
    }
}

fn integer(value: &ScalarValue) -> Result<i128> {
    match value {
        ScalarValue::Int64(Some(value)) => Ok(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Ok(i128::from(*value)),
        _ => Err(Error::refusal(
            "unsupported_literal_expansion",
            "integer scalar argument required",
        )),
    }
}
fn invocation_bytes(name: &str, args: &[ColumnarValue], rows: usize) -> Result<usize> {
    let invalid = || {
        Error::refusal(
            "unsupported_literal_expansion",
            "native scalar sizing is unavailable",
        )
    };
    let mut total = 0usize;
    let rows = if args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        1
    } else {
        rows
    };
    for row in 0..rows {
        let values: Vec<std::borrow::Cow<'_, ScalarValue>> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Ok(std::borrow::Cow::Borrowed(value)),
                ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row)
                    .map(std::borrow::Cow::Owned)
                    .map_err(Error::from),
            })
            .collect::<Result<Vec<_>>>()?;
        let arg = |index: usize| {
            values
                .get(index)
                .map(|value| value.as_ref())
                .ok_or_else(invalid)
        };
        let bytes = match name {
            "repeat" => {
                if arg(0)?.is_null() || arg(1)?.is_null() {
                    continue;
                }
                let len = match arg(0)? {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => value.len(),
                    _ => return Err(invalid()),
                };
                (len as i128)
                    .checked_mul(integer(arg(1)?)?.max(0))
                    .ok_or_else(invalid)?
            }
            "lpad" | "rpad" => {
                if values.iter().any(|value| value.is_null()) {
                    continue;
                }
                integer(arg(1)?)?
                    .max(0)
                    .checked_mul(4)
                    .ok_or_else(invalid)?
            }
            "array_repeat" => {
                if arg(1)?.is_null() {
                    continue;
                }
                // Nested repetition can allocate take indices proportional to
                // child cardinality even when Null children own no buffers.
                // The initial bounded profile supports flat scalar elements.
                if arg(0)?.data_type().is_nested() {
                    return Err(invalid());
                }
                integer(arg(1)?)?
                    .max(0)
                    .checked_mul(arg(0)?.size().saturating_add(64) as i128)
                    .ok_or_else(invalid)?
            }
            "array_resize" => {
                // Parquet/DataFusion may retain a nonzero value underneath the
                // length's null bit; the pinned resize kernel reads that value.
                // Refuse this unqualified input rather than treating it as zero.
                if arg(1)?.is_null() {
                    return Err(invalid());
                }
                if arg(0)?.is_null() {
                    continue;
                }
                if let DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) = arg(0)?.data_type()
                    && field.data_type().is_nested()
                {
                    return Err(invalid());
                }
                if values
                    .get(2)
                    .is_some_and(|value| value.data_type().is_nested())
                {
                    return Err(invalid());
                }
                let fill = values.get(2).map_or(0, |value| value.size());
                integer(arg(1)?)?
                    .max(0)
                    .checked_mul(arg(0)?.size().max(fill).saturating_add(64) as i128)
                    .ok_or_else(invalid)?
            }
            "range" | "array_range" | "generate_series" => {
                if values.iter().any(|value| value.is_null()) {
                    continue;
                }
                let (start, end) = if values.len() == 1 {
                    (0, integer(arg(0)?)?)
                } else {
                    (integer(arg(0)?)?, integer(arg(1)?)?)
                };
                let step = values
                    .get(2)
                    .map(|value| integer(value.as_ref()))
                    .transpose()?
                    .unwrap_or(1);
                if step == 0 {
                    return Err(invalid());
                }
                let count = (end - start)
                    .unsigned_abs()
                    .div_ceil(step.unsigned_abs())
                    .saturating_add(1);
                i128::try_from(count)
                    .map_err(|_| invalid())?
                    .checked_mul(16)
                    .ok_or_else(invalid)?
            }
            _ => return Err(invalid()),
        };
        total = total
            .checked_add(usize::try_from(bytes).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{budget::Limits, job::Job};
    use arrow::{
        array::{Array, ArrayRef, Int64Array, ListArray, NullArray},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::Field,
    };

    fn job(root: &std::path::Path) -> Job {
        Job::new(
            Limits {
                memory_bytes: 32 << 20,
                compute_threads: 1,
                scratch_bytes: 4 << 20,
                batch_rows: 16,
                max_batch_bytes: 16 << 10,
                max_metadata_bytes: 16 << 10,
            },
            root.join("scratch"),
        )
        .unwrap()
    }
    fn arguments(job: &Job, function: &ScalarUDF, args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        let fields = args
            .iter()
            .enumerate()
            .map(|(index, arg)| Arc::new(Field::new(format!("arg{index}"), arg.data_type(), true)))
            .collect::<Vec<_>>();
        let scalar_arguments = vec![None; fields.len()];
        let return_field = function
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        ScalarFunctionArgs {
            args,
            arg_fields: fields,
            number_rows: 1,
            return_field,
            config_options: job.context.copied_config().options().clone(),
        }
    }
    #[tokio::test]
    async fn literal_admission_is_lazy_cumulative_and_shared_across_contexts() {
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            memory_bytes: 16 << 20,
            compute_threads: 1,
            scratch_bytes: 4 << 20,
            batch_rows: 16,
            max_batch_bytes: 1024,
            max_metadata_bytes: 1024,
        };
        let budget = Budget::in_workspace(limits, &root.path().join("scratch")).unwrap();
        let jobs = (0..32)
            .map(|_| Job::from_budget(budget.clone()).unwrap())
            .collect::<Vec<_>>();
        let all = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
        for job in &jobs {
            let frame = job.sql("SELECT 1 AS value").await.unwrap();
            super::super::inferred_schema(&frame, &budget).unwrap();
            assert_eq!(owner(&job.context.state()).unwrap().admitted_bytes(), 0);
        }
        drop(all);
        let job = &jobs[0];
        let scope = owner(&job.context.state()).unwrap();
        let planning = scope.enter();
        let function = job.context.state().scalar_functions()["repeat"].clone();
        let invoke = || {
            function.invoke_with_args(arguments(
                job,
                &function,
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into()))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(256))),
                ],
            ))
        };
        let ColumnarValue::Scalar(value) = invoke().unwrap() else {
            panic!("constant repeat must preserve its scalar result");
        };
        assert_eq!(value, ScalarValue::Utf8(Some("x".repeat(256))));
        let charge = scope.admitted_bytes();
        assert!(charge > 0 && charge < 4 * scope.limit);
        let remaining = budget
            .reserve(budget.limits.memory_bytes / 2 - charge)
            .unwrap();
        assert!(matches!(
            invoke(),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(scope.admitted_bytes(), charge);
        drop(remaining);
        invoke().unwrap();
        assert!(scope.admitted_bytes() > charge);
        // Two 256-byte results fit this allowance; a third cumulative call
        // exceeds it even though each individual result is small.
        assert!(matches!(
            invoke(),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        drop(planning);
        drop(scope);
        drop(function);
        drop(jobs);
        let _released = budget.reserve(budget.limits.memory_bytes / 2).unwrap();
    }
    #[tokio::test]
    async fn flat_null_elements_and_fill_preserve_native_values() {
        let root = tempfile::tempdir().unwrap();
        let job = job(root.path());
        let result=crate::job::query_result::query(&job,"positive","SELECT array_repeat(CAST(NULL AS BIGINT),2) AS repeated,array_resize(make_array(CAST(1 AS BIGINT)),3,CAST(NULL AS BIGINT)) AS resized").await.unwrap();
        assert_eq!(
            result.data,
            serde_json::json!([{"repeated":[null,null],"resized":[1,null,null]}])
        );
    }
    #[test]
    fn null_resize_length_with_nonzero_backing_value_refuses_before_native_kernel() {
        let root = tempfile::tempdir().unwrap();
        let job = job(root.path());
        let function = job
            .context
            .state()
            .scalar_functions()
            .get("array_resize")
            .unwrap()
            .clone();
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1))],
            &DataType::Int64,
            true,
        ))
        .to_array_of_size(1)
        .unwrap();
        let counts = Int64Array::new(vec![4096].into(), Some(NullBuffer::new_null(1)));
        assert!(counts.is_null(0));
        assert_eq!(counts.value(0), 4096);
        let args = arguments(
            &job,
            &function,
            vec![
                ColumnarValue::Array(list),
                ColumnarValue::Array(Arc::new(counts)),
            ],
        );
        assert!(
            function
                .invoke_with_args(args)
                .unwrap_err()
                .to_string()
                .contains("outside the bounded expansion profile")
        );
    }
    #[test]
    fn nested_null_children_refuse_despite_tiny_buffers() {
        let root = tempfile::tempdir().unwrap();
        let job = job(root.path());
        let function = job
            .context
            .state()
            .scalar_functions()
            .get("array_repeat")
            .unwrap()
            .clone();
        let nested = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Null, true)),
            OffsetBuffer::new(vec![0, 4096].into()),
            Arc::new(NullArray::new(4096)),
            None,
        )) as ArrayRef;
        assert!(
            nested.get_buffer_memory_size() <= 32,
            "the fixture has thousands of children without corresponding payload buffers"
        );
        let args = arguments(
            &job,
            &function,
            vec![
                ColumnarValue::Array(nested),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
        );
        assert!(
            function
                .invoke_with_args(args)
                .unwrap_err()
                .to_string()
                .contains("outside the bounded expansion profile")
        );
    }
}
