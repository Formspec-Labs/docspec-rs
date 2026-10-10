//! Maintained scalar expression compilation with explicit row-local reuse limits.

use super::*;
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{
        DFSchema,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    logical_expr::{Expr, Volatility},
    physical_expr::PhysicalExpr,
};

pub(super) struct Expressions {
    pub native: iceberg::spec::Schema,
    pub schema: SchemaRef,
    pub native_field_ids: BTreeMap<i32, i32>,
    pub input_fields: BTreeMap<String, i32>,
    pub dependencies: String,
    pub logical: Vec<Expr>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    source_fields: usize,
}

impl Expressions {
    pub fn compile(
        context: &SessionContext,
        source: &iceberg::spec::Schema,
        spec: &GroupDefinition,
        binding: Option<&BTreeMap<String, i32>>,
    ) -> Result<Self> {
        validate_definition(spec)?;
        let native_source = source;
        let source = iceberg::arrow::schema_to_arrow_schema(native_source)?;
        let df_schema = DFSchema::try_from(source.clone())?;
        // Parse retained SQL against its original input names, then resolve those
        // names by native ID. A reused display name must not capture an old expression.
        let aliases = if let Some(binding) = binding {
            let fields = binding
                .iter()
                .map(|(name, id)| {
                    let index = root_index(native_source, *id)?;
                    Ok(source.field(index).clone().with_name(name))
                })
                .collect::<Result<Vec<_>>>()?;
            DFSchema::try_from(arrow::datatypes::Schema::new(fields))?
        } else {
            df_schema.clone()
        };
        let mut expressions = Vec::new();
        let mut logical = Vec::new();
        let mut input_fields = BTreeMap::new();
        let mut observes_metadata = false;
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
            let expression = context.parse_sql_expr(&field.expression, &aliases)?;
            expression.apply(|part| {
                if let Expr::ScalarFunction(function) = part {
                    // The pinned registry exposes schema metadata through this
                    // immutable function, so docs are material when it is used.
                    observes_metadata |= function.func.name() == "arrow_metadata";
                }
                if let Expr::Column(column) = part {
                    if storage::identity::hidden(&column.name) {
                        return Err(query_error(Error::refusal("invalid_expression", "computed expressions cannot consume internal occurrence storage fields")));
                    }
                    let id = match binding {
                        Some(binding) => *binding.get(&column.name).ok_or_else(invalid_binding).map_err(query_error)?,
                        None => native_source.as_struct().fields()[source.index_of(&column.name)?].id,
                    };
                    input_fields.insert(column.name.clone(), id);
                }
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
            let expression = expression
                .transform(|part| {
                    if let Expr::Column(mut column) = part {
                        let id = *input_fields
                            .get(&column.name)
                            .ok_or_else(invalid_binding)
                            .map_err(query_error)?;
                        column.name = source
                            .field(root_index(native_source, id).map_err(query_error)?)
                            .name()
                            .clone();
                        column.relation = None;
                        Ok(Transformed::yes(Expr::Column(column)))
                    } else {
                        Ok(Transformed::no(part))
                    }
                })?
                .data;
            let physical = context.create_physical_expr(expression.clone(), &df_schema)?;
            fields.push(Field::new(&field.name, physical.data_type(&source)?, true));
            logical.push(expression);
            expressions.push(physical);
        }
        if binding.is_some_and(|binding| binding != &input_fields) {
            return Err(invalid_binding());
        }
        let dependencies = input_fields
            .values()
            .map(|id| field_interpretation(native_source, *id, observes_metadata))
            .collect::<Result<Vec<_>>>()?;
        let dependencies = blake3::hash(&serde_json::to_vec(&(
            PROCESSOR,
            spec,
            &input_fields,
            dependencies,
        ))?)
        .to_hex()
        .to_string();
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
        // Inferred outputs use sequential native IDs. Keep logical field
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
            input_fields,
            dependencies,
            logical,
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

/// Normalizes material read interpretation while preserving stable field identity.
pub(in crate::engine) fn field_interpretation(
    schema: &iceberg::spec::Schema,
    id: i32,
    observes_metadata: bool,
) -> Result<iceberg::spec::NestedField> {
    let mut field = schema.as_struct().fields()[root_index(schema, id)?]
        .as_ref()
        .clone();
    field.name.clear();
    // Future omission policy cannot change this immutable file's values.
    field.write_default = None;
    if !observes_metadata {
        field.doc = None;
    }
    Ok(field)
}

fn root_index(schema: &iceberg::spec::Schema, id: i32) -> Result<usize> {
    schema
        .as_struct()
        .fields()
        .iter()
        .position(|field| field.id == id)
        .ok_or_else(invalid_binding)
}

pub(super) fn invalid_binding() -> Error {
    Error::refusal(
        "invalid_input_binding",
        "computed inputs must name exactly the consumed root field IDs",
    )
}

fn query_error(error: Error) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
