//! Maintained scalar expression compilation with explicit row-local reuse limits.

use super::*;
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{
        DFSchema,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    logical_expr::{Expr, Volatility},
    physical_expr::PhysicalExpr,
};

pub(super) struct Expressions {
    pub native: iceberg::spec::Schema,
    pub schema: SchemaRef,
    pub native_field_ids: BTreeMap<i32, i32>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    source_fields: usize,
}

impl Expressions {
    pub fn compile(
        context: &SessionContext,
        source: &iceberg::spec::Schema,
        spec: &BackfillSpec,
    ) -> Result<Self> {
        validate(spec)?;
        let source = iceberg::arrow::schema_to_arrow_schema(source)?;
        let df_schema = DFSchema::try_from(source.clone())?;
        let mut expressions = Vec::new();
        let mut fields = vec![
            Field::new(
                storage::OCCURRENCE_FILE,
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            Field::new(
                storage::OCCURRENCE_ROW,
                arrow::datatypes::DataType::Int64,
                false,
            ),
        ];
        for field in &spec.fields {
            if source
                .fields()
                .iter()
                .any(|existing| existing.name().eq_ignore_ascii_case(&field.name))
            {
                return Err(Error::refusal(
                    "field_conflict",
                    "computed field names must differ from source fields",
                ));
            }
            let expression = context.parse_sql_expr(&field.expression, &df_schema)?;
            expression.apply(|part| {
                let allowed = match part {
                    Expr::Alias(_) | Expr::Column(_) | Expr::Literal(_, _) | Expr::BinaryExpr(_) | Expr::Like(_) | Expr::SimilarTo(_) |
                    Expr::Not(_) | Expr::Negative(_) | Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::IsTrue(_) | Expr::IsFalse(_) |
                    Expr::IsUnknown(_) | Expr::IsNotTrue(_) | Expr::IsNotFalse(_) | Expr::IsNotUnknown(_) |
                    Expr::Between(_) | Expr::Case(_) | Expr::Cast(_) | Expr::TryCast(_) | Expr::InList(_) => true,
                    Expr::ScalarFunction(function) => function.func.signature().volatility == Volatility::Immutable,
                    _ => false,
                };
                if !allowed {
                    return Err(datafusion::common::DataFusionError::External(Box::new(Error::refusal("nonlocal_expression", "backfill accepts row-local immutable expressions without aggregates, windows or subqueries"))));
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let physical = context.create_physical_expr(expression, &df_schema)?;
            fields.push(Field::new(&field.name, physical.data_type(&source)?, true));
            expressions.push(physical);
        }
        let inferred = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
            &arrow::datatypes::Schema::new(fields),
        )?;
        if inferred
            .as_struct()
            .fields()
            .iter()
            .any(|field| !matches!(field.field_type.as_ref(), iceberg::spec::Type::Primitive(_)))
        {
            return Err(Error::refusal(
                "unsupported_computed_type",
                "nested computed results require separate field-identity qualification",
            ));
        }
        // New Iceberg tables assign their own sequential IDs. Keep logical field
        // identities explicit, mapped to the exact native schema in each result.
        let native_field_ids = spec
            .fields
            .iter()
            .map(|field| {
                Ok((
                    field.id,
                    inferred
                        .field_by_name(&field.name)
                        .ok_or_else(invalid_group)?
                        .id,
                ))
            })
            .collect::<Result<_>>()?;
        let native = inferred;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&native)?);
        Ok(Self {
            native,
            schema,
            native_field_ids,
            expressions,
            source_fields: source.fields().len(),
        })
    }

    pub fn evaluate(&self, batch: RecordBatch, limit: usize) -> Result<RecordBatch> {
        let mut columns = vec![
            batch.column(self.source_fields).clone(),
            batch.column(self.source_fields + 1).clone(),
        ];
        for (index, expression) in self.expressions.iter().enumerate() {
            let value = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
            let value = arrow::compute::cast(&value, self.schema.field(index + 2).data_type())?;
            columns.push(value);
            if columns
                .iter()
                .map(|array| array.get_array_memory_size())
                .sum::<usize>()
                > limit
            {
                return Err(Error::refusal(
                    "oversized_batch",
                    "computed values exceed batch allowance",
                ));
            }
        }
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
