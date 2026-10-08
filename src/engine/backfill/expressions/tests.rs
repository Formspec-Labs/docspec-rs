//! Field-ID binding and material dependency falsification without catalog state.

use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

fn field(id: i32, name: &str) -> Arc<NestedField> {
    Arc::new(NestedField::optional(
        id,
        name,
        Type::Primitive(PrimitiveType::Long),
    ))
}
fn schema(fields: Vec<Arc<NestedField>>) -> Schema {
    Schema::builder().with_fields(fields).build().unwrap()
}
fn definition(expression: &str) -> GroupDefinition {
    GroupDefinition {
        group: Id::new("derived").unwrap(),
        definition: "v1".into(),
        fields: vec![ComputedField {
            id: 7,
            name: "answer".into(),
            expression: expression.into(),
        }],
    }
}
fn evaluate(expressions: &Expressions, schema: &Schema, values: Vec<ArrayRef>) -> RecordBatch {
    let mut fields = iceberg::arrow::schema_to_arrow_schema(schema)
        .unwrap()
        .fields()
        .to_vec();
    fields.extend(expressions.schema.fields()[..2].iter().cloned());
    let mut values = values;
    values.push(Arc::new(StringArray::from(vec!["source.parquet"])));
    values.push(Arc::new(Int64Array::from(vec![0])));
    expressions
        .evaluate(
            RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), values).unwrap(),
            1 << 20,
        )
        .unwrap()
}

#[tokio::test]
async fn rename_reorder_and_name_reuse_preserve_bound_values_and_material_identity() {
    let context = SessionContext::new();
    let original = schema(vec![field(10, "quo\"ted"), field(20, "unused")]);
    let spec = definition("\"quo\"\"ted\" + 1");
    let first = Expressions::compile(&context, &original, &spec, None).unwrap();
    assert_eq!(
        first.input_fields,
        BTreeMap::from([("quo\"ted".into(), 10)])
    );
    let evolved = schema(vec![
        field(30, "quo\"ted"),
        field(10, "new name"),
        field(90, "new unrelated"),
    ]);
    let bound = Expressions::compile(&context, &evolved, &spec, Some(&first.input_fields)).unwrap();
    let fresh = Expressions::compile(&context, &evolved, &spec, None).unwrap();
    assert_eq!(first.dependencies, bound.dependencies);
    assert_ne!(fresh.dependencies, bound.dependencies);
    let values = vec![
        Arc::new(Int64Array::from(vec![100])) as ArrayRef,
        Arc::new(Int64Array::from(vec![9])),
        Arc::new(Int64Array::from(vec![999])),
    ];
    let bound = evaluate(&bound, &evolved, values.clone());
    let fresh = evaluate(&fresh, &evolved, values);
    assert_eq!(
        bound
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        fresh
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        101
    );
    let missing = schema(vec![field(30, "quo\"ted")]);
    assert_eq!(
        Expressions::compile(&context, &missing, &spec, Some(&first.input_fields))
            .err()
            .unwrap()
            .code(),
        "invalid_input_binding"
    );
    let extra = BTreeMap::from([("quo\"ted".into(), 10), ("unused".into(), 20)]);
    assert_eq!(
        Expressions::compile(&context, &original, &spec, Some(&extra))
            .err()
            .unwrap()
            .code(),
        "invalid_input_binding"
    );
}

#[tokio::test]
async fn constants_ignore_schema_but_consumed_types_and_metadata_observers_invalidate() {
    let context = SessionContext::new();
    let mut original = field(10, "value").as_ref().clone();
    original.doc = Some("before".into());
    let before = schema(vec![Arc::new(original.clone())]);
    original.doc = Some("after".into());
    let after = schema(vec![Arc::new(original.clone()), field(20, "unused")]);
    for (expression, same) in [("value + 1", true), ("arrow_metadata(value, 'doc')", false)] {
        let spec = definition(expression);
        let first = Expressions::compile(&context, &before, &spec, None).unwrap();
        let second =
            Expressions::compile(&context, &after, &spec, Some(&first.input_fields)).unwrap();
        assert_eq!(
            first.dependencies == second.dependencies,
            same,
            "{expression}"
        );
        if !same {
            let first = evaluate(&first, &before, vec![Arc::new(Int64Array::from(vec![1]))]);
            let second = evaluate(
                &second,
                &after,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            );
            assert_eq!(
                first
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "before"
            );
            assert_eq!(
                second
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "after"
            );
        }
    }
    let spec = definition("value + 1");
    let first = Expressions::compile(&context, &before, &spec, None).unwrap();
    original.field_type = Box::new(Type::Primitive(PrimitiveType::Int));
    let changed_type = schema(vec![Arc::new(original)]);
    let second =
        Expressions::compile(&context, &changed_type, &spec, Some(&first.input_fields)).unwrap();
    assert_ne!(first.dependencies, second.dependencies);
    let literal = definition("CAST(NULL AS BIGINT)");
    let first = Expressions::compile(&context, &before, &literal, None).unwrap();
    let second = Expressions::compile(
        &context,
        &schema(vec![field(99, "other")]),
        &literal,
        Some(&first.input_fields),
    )
    .unwrap();
    assert!(first.input_fields.is_empty());
    assert_eq!(first.dependencies, second.dependencies);
}
