//! One admitted comparison context for counts and streamed endpoint members.

mod window;

use super::*;
use arrow::{
    array::{Array, ArrayRef, Int64Array, StructArray},
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::Column,
    functions_aggregate::{min_max::max, sum::sum},
    functions_window::row_number::row_number,
    logical_expr::{Expr, ExprFunctionExt, when},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use tokio::sync::OwnedSemaphorePermit;

struct Plan {
    descriptor: ChangeDescriptor,
    fields: Fields,
    relation: Option<DataFrame>,
    memory: Arc<OwnedSemaphorePermit>,
}

impl Engine {
    fn forward_relation(
        &self,
        chain: changes::Chain,
        fields: &Fields,
        located: bool,
    ) -> Result<Option<DataFrame>> {
        let context = self.context();
        let budget = &self.inner.lifecycle.budget;
        let mut frames = Vec::with_capacity(chain.tables.len());
        for delta in chain.tables {
            let provider = if located {
                crate::query::occurrences(delta.table, budget.clone(), Some(delta.rows))?
            } else {
                crate::query::table(delta.table, budget.clone(), Some(delta.rows))?
            };
            frames.push(signed_frame(
                context.read_table(provider)?,
                fields,
                located,
                delta.weight,
            )?);
        }
        if frames.is_empty() {
            Ok(None)
        } else {
            Ok(Some(context.read_table(crate::query::sequence(
                frames,
                chain.memory,
            )?)?))
        }
    }

    /// Compares visible values and duplicate counts between exact retained endpoints.
    ///
    /// # Errors
    /// Refuses unavailable endpoints, missing groups, unsupported values or exhausted capacity.
    pub async fn compare(
        &self,
        before: &ReleasePin,
        after: &ReleasePin,
        group: &str,
    ) -> Result<Comparison> {
        self.compare_with_options(
            before,
            after,
            group,
            &ComparisonOptions {
                identity: ComparisonIdentity::Values,
                force_full: false,
            },
        )
        .await
    }

    /// Counts differences under explicit value or occurrence semantics.
    ///
    /// Incompatible visible fields return unknown counts. Default-only rule
    /// changes report schema effects without inventing value differences. Counts
    /// and row streams share admission and signed multiset semantics.
    ///
    /// # Errors
    /// Refuses unavailable endpoints, missing groups, unsupported values or exhausted capacity.
    pub async fn compare_with_options(
        &self,
        before: &ReleasePin,
        after: &ReleasePin,
        group: &str,
        options: &ComparisonOptions,
    ) -> Result<Comparison> {
        let memory = self.request_memory(&(before, after, group, options))?;
        let (before, after, group, options) = (
            before.clone(),
            after.clone(),
            group.to_owned(),
            options.clone(),
        );
        self.admit_work(memory, move |engine| async move {
            let mut plan = engine
                .comparison_plan(&before, &after, &group, &options, false)
                .await?;
            let same_schema = !plan.descriptor.effects.schema_changed;
            let same_inputs = !plan.descriptor.effects.input_refs_changed
                && !plan.descriptor.effects.coverage_changed;
            let mut counts = None;
            if plan.descriptor.comparable_values {
                counts = Some(if let Some(relation) = plan.relation.take() {
                    // EXCEPT ALL in pinned DataFusion loses duplicate differences.
                    // Aggregate signed multiplicity without materializing row payloads.
                    let grouped = relation.aggregate(
                        identity_columns(&plan),
                        vec![sum(col("weight")).alias("net")],
                    )?;
                    let totals = grouped
                        .aggregate(
                            vec![],
                            vec![
                                sum(when(col("net").gt(lit(0_i64)), col("net"))
                                    .otherwise(lit(0_i64))?)
                                .alias("added"),
                                sum(when(col("net").lt(lit(0_i64)), -col("net"))
                                    .otherwise(lit(0_i64))?)
                                .alias("removed"),
                            ],
                        )?
                        .collect()
                        .await?;
                    let mut counts = [0; 2];
                    for (index, output) in counts.iter_mut().enumerate() {
                        let array = totals
                            .first()
                            .and_then(|batch| {
                                batch.column(index).as_any().downcast_ref::<Int64Array>()
                            })
                            .filter(|array| array.len() == 1)
                            .ok_or_else(|| {
                                Error::refusal(
                                    "invalid_comparison",
                                    "comparison returned no scalar count",
                                )
                            })?;
                        *output = if array.is_null(0) {
                            0
                        } else {
                            u64::try_from(array.value(0)).map_err(|_| {
                                Error::refusal("invalid_comparison", "negative difference count")
                            })?
                        };
                    }
                    counts
                } else {
                    [0, 0]
                });
            }
            Ok(Comparison {
                descriptor: plan.descriptor,
                same_schema,
                same_inputs,
                added_rows: counts.map(|counts| counts[0]),
                removed_rows: counts.map(|counts| counts[1]),
            })
        })
        .await
    }

    /// Streams changed endpoint members with typed values and exact occurrence addresses.
    ///
    /// Schema metadata `docspec.change` describes endpoints, identity and the chosen
    /// path. Read through successful EOF to establish complete coverage. Value mode
    /// returns deterministic representatives of surplus values; occurrence mode
    /// also distinguishes replacement by an equal value with a new birth identity.
    /// The stream protects its admitted inputs until EOF or drop.
    ///
    /// # Errors
    /// Refuses unavailable endpoints, incompatible schemas, unsupported values,
    /// exhausted capacity, or failed reads. A stream error never means empty changes.
    pub async fn changes(
        &self,
        before: &ReleasePin,
        after: &ReleasePin,
        group: &str,
        options: &ComparisonOptions,
    ) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(&(before, after, group, options))?;
        let (before, after, group, options) = (
            before.clone(),
            after.clone(),
            group.to_owned(),
            options.clone(),
        );
        self.admit_stream(memory, move |engine| async move {
            let plan = engine
                .comparison_plan(&before, &after, &group, &options, true)
                .await?;
            engine.comparison_stream(plan).await
        })
        .await
    }

    async fn comparison_plan(
        &self,
        before: &ReleasePin,
        after: &ReleasePin,
        group: &str,
        options: &ComparisonOptions,
        addresses: bool,
    ) -> Result<Plan> {
        let before_view = self.check_read_release(before).await?;
        let after_view = if before == after {
            before_view.clone()
        } else {
            self.check_read_release(after).await?
        };
        let budget = &self.inner.lifecycle.budget;
        resolve_release(before, budget).await?;
        if before != after {
            resolve_release(after, budget).await?;
        }
        let first = before.dataset.groups.get(group).ok_or_else(|| {
            Error::refusal(
                "missing_group",
                "before release lacks the requested field group",
            )
        })?;
        let second = after.dataset.groups.get(group).ok_or_else(|| {
            Error::refusal(
                "missing_group",
                "after release lacks the requested field group",
            )
        })?;
        // Keep metadata, descriptor and schemas admitted with the returned plan/stream.
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(3))?);
        let identical = first == second;
        let (canonical_first, canonical_second) = (first, second);
        let first = self
            .representation_pin(&before_view, &before.id, group, first)
            .await?;
        let second = if identical
            && before_view.revision == after_view.revision
            && before.dataset.dataset == after.dataset.dataset
        {
            first.clone()
        } else {
            self.representation_pin(&after_view, &after.id, group, second)
                .await?
        };
        for pin in [&first, &second] {
            if pin.metadata.bytes > budget.limits.max_metadata_bytes as u64 {
                return Err(Error::refusal(
                    "metadata_capacity",
                    "comparison metadata exceeds its allowance",
                ));
            }
        }
        let left = storage::open_pin(&first, &self.inner.lifecycle.budget).await?;
        let right = if first == second {
            left.clone()
        } else {
            storage::open_pin(&second, &self.inner.lifecycle.budget).await?
        };
        storage::identity::has_origins(left.metadata().current_schema())?;
        storage::identity::has_origins(right.metadata().current_schema())?;
        let left_schema = storage::identity::public_schema(&left)?;
        let right_schema = storage::identity::public_schema(&right)?;
        let computed = before.dataset.computed.contains_key(group);
        let same_kind = computed == after.dataset.computed.contains_key(group);
        if computed {
            check_group_addresses(&left_schema)?;
        }
        if after.dataset.computed.contains_key(group) {
            check_group_addresses(&right_schema)?;
        }
        // Native IDs are local to a table lineage; fresh builds reallocate them.
        // Relax display names/order only across an exact retained schema boundary.
        let retained_schema = if same_kind
            && !computed
            && group == "source"
            && left_schema != right_schema
            && before.dataset.dataset == after.dataset.dataset
        {
            let mut extra_metadata = 0usize;
            for (canonical, selected) in [(canonical_first, &first), (canonical_second, &second)] {
                if canonical != selected {
                    if canonical.metadata.bytes > budget.limits.max_metadata_bytes as u64 {
                        return Err(Error::refusal(
                            "metadata_capacity",
                            "canonical comparison metadata exceeds its allowance",
                        ));
                    }
                    extra_metadata += 1;
                }
            }
            // Canonical histories are temporary qualification inputs; reserve
            // their native metadata before opening and release after this check.
            let _canonical_memory = budget.reserve(
                budget
                    .limits
                    .max_metadata_bytes
                    .saturating_mul(extra_metadata),
            )?;
            let canonical_left = if canonical_first == &first {
                left.clone()
            } else {
                storage::open_pin(canonical_first, &self.inner.lifecycle.budget).await?
            };
            let canonical_right = if canonical_second == &second {
                right.clone()
            } else {
                storage::open_pin(canonical_second, &self.inner.lifecycle.budget).await?
            };
            retained_schema_boundary(canonical_left.metadata(), canonical_right.metadata())
        } else {
            false
        };
        // Computed groups have separate logical IDs; retain their strict rule.
        let aligned = if same_kind && left_schema == right_schema {
            Some(right_schema.fields().clone())
        } else if retained_schema {
            aligned_fields(
                left.metadata().current_schema(),
                right.metadata().current_schema(),
                &left_schema,
                &right_schema,
            )?
        } else {
            None
        };
        let comparable = aligned.is_some();
        let mut before_keys = left
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect::<Vec<_>>();
        let mut after_keys = right
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect::<Vec<_>>();
        before_keys.sort_unstable();
        after_keys.sort_unstable();
        let public_rules = |schema: &iceberg::spec::Schema| {
            schema
                .as_struct()
                .fields()
                .iter()
                .filter(|field| !storage::identity::hidden(&field.name))
                .cloned()
                .collect::<Vec<_>>()
        };
        let same_schema = same_kind
            && before_keys == after_keys
            && public_rules(left.metadata().current_schema())
                == public_rules(right.metadata().current_schema());
        let visible = |field: &Field| {
            let occurrence = computed
                && matches!(
                    field.name().as_str(),
                    storage::OCCURRENCE_FILE | storage::OCCURRENCE_ROW
                );
            let contributor = group == "source"
                && before.dataset.related.is_some()
                && after.dataset.related.is_some()
                && refresh::hidden_field(field.name());
            !(occurrence || contributor)
        };
        let fields: Fields = left_schema
            .fields()
            .iter()
            .filter(|field| visible(field))
            .cloned()
            .collect();
        let after_fields: Fields = aligned
            .as_ref()
            .map(|aligned| {
                left_schema
                    .fields()
                    .iter()
                    .zip(aligned)
                    .filter(|(field, _)| visible(field))
                    .map(|(_, field)| field.clone())
                    .collect()
            })
            .unwrap_or_default();
        let field_ids = fields
            .iter()
            .map(|field| {
                left.metadata()
                    .current_schema()
                    .field_by_name(field.name())
                    .map(|field| field.id)
                    .ok_or_else(|| {
                        Error::refusal(
                            "invalid_comparison",
                            "visible field lacks a native identity",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let unchanged = identical && same_schema && !options.force_full;
        let mut descriptor = ChangeDescriptor {
            format: 1,
            before: before.version_ref(),
            after: after.version_ref(),
            group: group.to_owned(),
            identity: options.identity,
            path: if unchanged {
                ComparisonPath::Unchanged
            } else {
                ComparisonPath::Full
            },
            fallback: if !comparable {
                Some(ComparisonFallback::SchemaChanged)
            } else if unchanged {
                None
            } else if options.force_full {
                Some(ComparisonFallback::Forced)
            } else {
                Some(ComparisonFallback::HistoryNotQualified)
            },
            field_ids,
            comparable_values: comparable,
            before_key_field_ids: before_keys,
            after_key_field_ids: after_keys,
            effects: effects(before, after, same_schema),
        };
        let located = addresses || options.identity == ComparisonIdentity::Occurrences;
        let mut forward = None;
        if comparable && !unchanged && !options.force_full {
            let outcome = if group != "source" || computed {
                changes::Forward::Fallback(ComparisonFallback::UnsupportedGroup)
            } else if !same_schema {
                changes::Forward::Fallback(ComparisonFallback::SchemaChanged)
            } else {
                self.change_chain(
                    &after_view,
                    (before, after),
                    (canonical_first, canonical_second),
                    &left,
                    changes::MAX_LINKS,
                )
                .await?
            };
            match outcome {
                changes::Forward::Eligible(chain) => {
                    descriptor.path = ComparisonPath::ForwardChanges;
                    descriptor.fallback = None;
                    forward = Some(self.forward_relation(chain, &fields, located)?);
                }
                changes::Forward::Fallback(reason) => descriptor.fallback = Some(reason),
            }
        }
        let relation = if !comparable || unchanged {
            None
        } else if let Some(relation) = forward {
            relation
        } else {
            let context = self.context();
            let mut frames = Vec::new();
            for (table, pin, selected, weight) in [
                (left, first, &fields, -1_i64),
                (right, second, &after_fields, 1_i64),
            ] {
                let provider = if located && !computed {
                    crate::query::occurrences(table, budget.clone(), Some(pin.rows))?
                } else {
                    crate::query::table(table, budget.clone(), Some(pin.rows))?
                };
                frames.push(signed_frame(
                    context.read_table(provider)?,
                    selected,
                    located,
                    weight,
                )?);
            }
            let after = frames.pop().expect("two endpoint frames");
            Some(frames.pop().expect("two endpoint frames").union(after)?)
        };
        Ok(Plan {
            descriptor,
            fields,
            relation,
            memory,
        })
    }

    async fn comparison_stream(&self, mut plan: Plan) -> Result<SendableRecordBatchStream> {
        if !plan.descriptor.comparable_values {
            return Err(Error::refusal(
                "comparison_schema_changed",
                "changed-row streams require identical visible field identities and types",
            ));
        }
        let stream: SendableRecordBatchStream = if let Some(relation) = plan.relation.take() {
            let frame = change_members(relation, plan.fields.len(), plan.descriptor.identity)?;
            let context = Arc::new(frame.task_ctx());
            let physical = window::admit(
                frame.create_physical_plan().await?,
                &self.inner.lifecycle.budget,
            )?;
            datafusion::physical_plan::execute_stream(physical, context)?
        } else {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                futures::stream::empty(),
            ))
        };
        let descriptor = storage::bounded_json(
            &plan.descriptor,
            self.inner.lifecycle.budget.limits.max_metadata_bytes,
        )?;
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("side", DataType::Utf8, false),
                Field::new("file", DataType::Utf8, false),
                Field::new("row", DataType::Int64, false),
                Field::new("values", DataType::Struct(plan.fields.clone()), false),
            ],
            [(
                "docspec.change".to_owned(),
                String::from_utf8(descriptor)
                    .map_err(|_| Error::refusal("invalid_comparison", "descriptor is not UTF-8"))?,
            )]
            .into(),
        ));
        let output_schema = schema.clone();
        let limit = self.inner.lifecycle.budget.limits.max_batch_bytes;
        let batches = stream.map(move |batch| {
            let _keep_metadata = &plan.memory;
            let batch = batch?;
            if batch.get_array_memory_size() > limit {
                return Err(datafusion::common::DataFusionError::External(Box::new(
                    Error::refusal(
                        "oversized_batch",
                        "comparison output exceeds its batch allowance",
                    ),
                )));
            }
            let values: ArrayRef = Arc::new(if plan.fields.is_empty() {
                StructArray::new_empty_fields(batch.num_rows(), None)
            } else {
                StructArray::try_new(plan.fields.clone(), batch.columns()[3..].to_vec(), None)?
            });
            Ok(RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    batch.column(0).clone(),
                    batch.column(1).clone(),
                    batch.column(2).clone(),
                    values,
                ],
            )?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

fn retained_schema_boundary(
    before: &iceberg::spec::TableMetadata,
    after: &iceberg::spec::TableMetadata,
) -> bool {
    // Compare the complete native schema, including names, nested IDs and rules.
    // A fresh auto-numbered build retains only its new schema; numeric ID reuse
    // alone cannot establish this boundary. Schema-preserving replacements keep it.
    after
        .schemas_iter()
        .any(|schema| schema == before.current_schema())
        || before
            .schemas_iter()
            .any(|schema| schema == after.current_schema())
}

fn aligned_fields(
    before: &iceberg::spec::Schema,
    after: &iceberg::spec::Schema,
    before_arrow: &Schema,
    after_arrow: &Schema,
) -> Result<Option<Fields>> {
    let after_fields = after
        .as_struct()
        .fields()
        .iter()
        .filter(|field| !storage::identity::hidden(&field.name))
        .map(|field| (field.id, field))
        .collect::<std::collections::BTreeMap<_, _>>();
    if before_arrow.fields().len() != after_fields.len() {
        return Ok(None);
    }
    let mut aligned = Vec::with_capacity(before_arrow.fields().len());
    for field in before_arrow.fields() {
        let first = before.field_by_name(field.name()).ok_or_else(|| {
            Error::refusal(
                "invalid_comparison",
                "visible field lacks a native identity",
            )
        })?;
        let Some(second) = after_fields.get(&first.id) else {
            return Ok(None);
        };
        let other = after_arrow.field_with_name(&second.name)?;
        // Native types retain nested identities and null rules. Root names and
        // order alone are presentation; defaults remain separately reported.
        if first.field_type != second.field_type
            || first.required != second.required
            || field.data_type() != other.data_type()
            || field.is_nullable() != other.is_nullable()
        {
            return Ok(None);
        }
        aligned.push(Arc::new(other.clone()));
    }
    Ok(Some(aligned.into()))
}

fn signed_frame(
    frame: DataFrame,
    fields: &Fields,
    located: bool,
    weight: i64,
) -> Result<DataFrame> {
    let mut columns = fields
        .iter()
        .enumerate()
        .map(|(i, field)| literal_column(field.name()).alias(format!("c{i}")))
        .collect::<Vec<_>>();
    if located {
        columns.extend([
            literal_column(storage::OCCURRENCE_FILE).alias("file"),
            literal_column(storage::OCCURRENCE_ROW).alias("row"),
        ]);
    }
    columns.push(lit(weight).alias("weight"));
    Ok(frame.select(columns)?)
}

fn change_members(
    relation: DataFrame,
    fields: usize,
    identity: ComparisonIdentity,
) -> Result<DataFrame> {
    let values = (0..fields)
        .map(|i| col(format!("c{i}")))
        .collect::<Vec<_>>();
    let mut occurrences = values.clone();
    occurrences.extend([col("file"), col("row")]);
    // Cancel identical occurrences before choosing value representatives. This
    // removes unchanged endpoint rows and prevents intermediate change-history
    // addresses from surviving value cancellation. Qualified occurrences are
    // unique within each endpoint; their remaining signed weights are +/-1.
    let net = relation
        .aggregate(occurrences.clone(), vec![sum(col("weight")).alias("net")])?
        .filter(col("net").not_eq(lit(0_i64)))?;
    let frame = if identity == ComparisonIdentity::Occurrences {
        // Exact occurrence differences need only the maintained grouped sum.
        // Ranking or sorting every occurrence would perform unnecessary work.
        net
    } else {
        occurrences.push(col("net").alias("weight"));
        value_members(net.select(occurrences)?, values)?
    };
    let mut columns = vec![
        when(col("net").lt(lit(0_i64)), lit("before"))
            .otherwise(lit("after"))?
            .alias("side"),
        col("file"),
        col("row"),
    ];
    columns.extend((0..fields).map(|i| col(format!("c{i}"))));
    Ok(frame.select(columns)?)
}

fn value_members(relation: DataFrame, mut values: Vec<Expr>) -> Result<DataFrame> {
    // Rank each side separately so matching ordinals cancel exactly one member.
    // Causal ROW_NUMBER uses DataFusion's bounded, sorted window; an unbounded
    // SUM window would buffer a whole duplicate group outside its memory pool.
    // Sorting and aggregation use the shared pool and maintained spill support.
    let mut ranked = values.clone();
    ranked.push(col("weight"));
    let rank = row_number()
        .partition_by(ranked)
        .order_by(vec![
            col("file").sort(true, true),
            col("row").sort(true, true),
        ])
        .build()?
        .alias("ordinal");
    values.push(col("ordinal"));
    // A surviving ordinal has exactly one endpoint member. Vectorized MAX keeps
    // that address; canceled groups never expose a combined representative.
    Ok(relation
        .window(vec![rank])?
        .aggregate(
            values,
            vec![
                sum(col("weight")).alias("net"),
                max(col("file")).alias("file"),
                max(col("row")).alias("row"),
            ],
        )?
        .filter(col("net").not_eq(lit(0_i64)))?)
}

fn literal_column(name: &str) -> Expr {
    Expr::Column(Column::new_unqualified(name))
}

fn identity_columns(plan: &Plan) -> Vec<Expr> {
    let mut columns = (0..plan.fields.len())
        .map(|i| col(format!("c{i}")))
        .collect::<Vec<_>>();
    if plan.descriptor.identity == ComparisonIdentity::Occurrences {
        columns.extend([col("file"), col("row")]);
    }
    columns
}

fn effects(before: &ReleasePin, after: &ReleasePin, same_schema: bool) -> ComparisonEffects {
    let before = &before.dataset;
    let after = &after.dataset;
    let computed_evidence_changed = before.computed.len() != after.computed.len()
        || before.computed.iter().zip(&after.computed).any(
            |((name, group), (other_name, other))| {
                name != other_name
                    || group.origins != other.origins
                    || group.qualification != other.qualification
                    || group.source_metadata != other.source_metadata
                    || group.base_manifest != other.base_manifest
                    || group.input_fields != other.input_fields
                    || group.native_field_ids != other.native_field_ids
            },
        );
    let changed_group_definitions = before
        .computed
        .keys()
        .chain(after.computed.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|name| {
            before.computed.get(*name).map(|group| &group.definition)
                != after.computed.get(*name).map(|group| &group.definition)
        })
        .cloned()
        .collect();
    ComparisonEffects {
        schema_changed: !same_schema,
        input_refs_changed: before.inputs != after.inputs,
        coverage_changed: before.related.as_ref().map(|related| &related.coverage)
            != after.related.as_ref().map(|related| &related.coverage),
        definition_changed: before.definition != after.definition
            || before.processor != after.processor,
        evidence_refs_changed: before.evidence != after.evidence
            || before.related.as_ref().map(|related| &related.evidence)
                != after.related.as_ref().map(|related| &related.evidence)
            || computed_evidence_changed,
        relationship_changed: before.relationship != after.relationship,
        changed_group_definitions,
    }
}

fn check_group_addresses(schema: &arrow::datatypes::Schema) -> Result<()> {
    for (name, kind) in [
        (storage::OCCURRENCE_FILE, DataType::Utf8),
        (storage::OCCURRENCE_ROW, DataType::Int64),
    ] {
        let field = schema.field_with_name(name)?;
        if field.data_type() != &kind || field.is_nullable() {
            return Err(Error::refusal(
                "invalid_comparison",
                "computed group has invalid retained occurrence fields",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::{Budget, Limits};
    use arrow::array::StringArray;
    use datafusion::{datasource::MemTable, physical_plan::displayable};

    #[test]
    fn field_alignment_refuses_changed_ids_types_nullability_and_membership() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let schema = |id, required, kind| {
            iceberg::spec::Schema::builder()
                .with_fields([Arc::new(NestedField::new(
                    id,
                    "value",
                    Type::Primitive(kind),
                    required,
                ))])
                .build()
                .unwrap()
        };
        let before = schema(1, false, PrimitiveType::Long);
        let before_arrow = iceberg::arrow::schema_to_arrow_schema(&before).unwrap();
        let empty = iceberg::spec::Schema::builder().build().unwrap();
        for after in [
            schema(2, false, PrimitiveType::Long),
            schema(1, true, PrimitiveType::Long),
            schema(1, false, PrimitiveType::String),
            empty,
        ] {
            let after_arrow = iceberg::arrow::schema_to_arrow_schema(&after).unwrap();
            assert!(
                aligned_fields(&before, &after, &before_arrow, &after_arrow)
                    .unwrap()
                    .is_none()
            );
            assert!(
                aligned_fields(&after, &before, &after_arrow, &before_arrow)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn addressed_differences_cancel_occurrences_before_choosing_value_members() {
        let budget = Budget::new(Limits::default()).unwrap();
        let frame = |rows: &[(&str, &str, i64)]| {
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "c0",
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|v| v.0))) as ArrayRef,
                ),
                (
                    "file",
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|v| v.1))) as ArrayRef,
                ),
                (
                    "row",
                    Arc::new(Int64Array::from(vec![0; rows.len()])) as ArrayRef,
                ),
                (
                    "weight",
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|v| v.2))) as ArrayRef,
                ),
            ])
            .unwrap();
            budget.context().read_batch(batch).unwrap()
        };
        let full = [
            ("old", "initial", -1),
            ("new", "z-kept", -1),
            ("new", "a-final", 1),
            ("new", "z-kept", 1),
        ];
        // Lexically later intermediate addresses would survive raw value ranking
        // even though they belong to neither endpoint. Cancel their exact births.
        let chain = [
            ("old", "initial", -1),
            ("new", "zz-intermediate", 1),
            ("new", "zz-intermediate", -1),
            ("new", "a-final", 1),
        ];
        let mut expected = vec![
            (
                "before".to_owned(),
                "initial".to_owned(),
                0,
                "old".to_owned(),
            ),
            (
                "after".to_owned(),
                "a-final".to_owned(),
                0,
                "new".to_owned(),
            ),
        ];
        expected.sort();
        for identity in [ComparisonIdentity::Values, ComparisonIdentity::Occurrences] {
            for input in [&full[..], &chain[..]] {
                let frame = change_members(frame(input), 1, identity).unwrap();
                let context = Arc::new(frame.task_ctx());
                let plan =
                    window::admit(frame.create_physical_plan().await.unwrap(), &budget).unwrap();
                let display = displayable(plan.as_ref()).indent(true).to_string();
                if identity == ComparisonIdentity::Occurrences {
                    assert!(display.contains("AggregateExec"));
                    assert!(!display.contains("WindowAggExec"));
                    assert!(!display.contains("SortExec:"));
                    assert!(!display.contains("ComparisonWindowInput:"));
                }
                let mut actual = Vec::new();
                for batch in datafusion::physical_plan::collect(plan, context)
                    .await
                    .unwrap()
                {
                    let strings = |index| {
                        batch
                            .column(index)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                    };
                    let rows = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for index in 0..batch.num_rows() {
                        actual.push((
                            strings(0).value(index).to_owned(),
                            strings(1).value(index).to_owned(),
                            rows.value(index),
                            strings(3).value(index).to_owned(),
                        ));
                    }
                }
                actual.sort();
                assert_eq!(actual, expected);
            }
        }
    }

    #[tokio::test]
    async fn duplicate_ranking_uses_causal_sorted_windows_and_shared_aggregates() {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::in_workspace(
            Limits {
                compute_threads: 2,
                memory_bytes: 64 << 20,
                scratch_bytes: 16 << 20,
                batch_rows: 32,
                max_batch_bytes: 64 << 10,
                max_metadata_bytes: 512 << 10,
            },
            root.path(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("file", DataType::Utf8, false),
            Field::new("row", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ]));
        let value = "x".repeat(512);
        let mut batches = Vec::new();
        // One duplicate group spans many input batches. The new side has one
        // extra member, whose deterministic address must be retained.
        for (weight, count) in [(-1_i64, 2048), (1_i64, 2049)] {
            for start in (0..count).step_by(32) {
                let end = (start + 32).min(count);
                let len = (end - start) as usize;
                batches.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(StringArray::from(vec![value.as_str(); len])),
                            Arc::new(StringArray::from(vec![
                                if weight < 0 {
                                    "before"
                                } else {
                                    "after"
                                };
                                len
                            ])),
                            Arc::new(Int64Array::from_iter_values(start..end)),
                            Arc::new(Int64Array::from(vec![weight; len])),
                        ],
                    )
                    .unwrap(),
                );
            }
        }
        let input = Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
        let frame = budget.context().read_table(input.clone()).unwrap();
        let frame = change_members(frame, 1, ComparisonIdentity::Values).unwrap();
        let context = frame.task_ctx();
        let physical = window::admit(frame.create_physical_plan().await.unwrap(), &budget).unwrap();
        let display = displayable(physical.as_ref()).indent(true).to_string();
        eprintln!("{display}");
        assert!(display.contains("BoundedWindowAggExec"));
        assert!(display.contains("mode=[Sorted]"));
        assert!(display.contains("ComparisonWindowInput:"));
        assert!(
            !display
                .lines()
                .any(|line| line.trim_start().starts_with("WindowAggExec:"))
        );
        assert!(display.contains("AggregateExec"));
        assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
        let output = datafusion::physical_plan::collect(physical, context.into())
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = output.iter().find(|batch| batch.num_rows() != 0).unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "after"
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2048
        );
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());

        // A deliberately insufficient operator pool must produce an error,
        // never an empty successful change relation. Reuse the same input arrays.
        let constrained = Budget::in_workspace(
            Limits {
                memory_bytes: 64 << 10,
                ..budget.limits.clone()
            },
            root.path(),
        )
        .unwrap();
        let frame = constrained.context().read_table(input).unwrap();
        let error = change_members(frame, 1, ComparisonIdentity::Values)
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ));
    }

    #[tokio::test]
    async fn oversized_sorted_batches_refuse_before_window_even_when_values_cancel() {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::in_workspace(
            Limits {
                compute_threads: 2,
                // Leave the participating sort/aggregate operators enough pool
                // space to reach the independently enforced batch-byte limit.
                memory_bytes: 256 << 20,
                scratch_bytes: 16 << 20,
                batch_rows: 32,
                max_batch_bytes: 64 << 10,
                max_metadata_bytes: 512 << 10,
            },
            root.path(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("file", DataType::Utf8, false),
            Field::new("row", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ]));
        let value = "x".repeat(32 << 10);
        let mut batches = Vec::new();
        for weight in [-1_i64, 1] {
            for row in 0_i64..64 {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![value.as_str()])),
                        Arc::new(StringArray::from(vec![if weight < 0 {
                            "before"
                        } else {
                            "after"
                        }])),
                        Arc::new(Int64Array::from(vec![row])),
                        Arc::new(Int64Array::from(vec![weight])),
                    ],
                )
                .unwrap();
                assert!(batch.get_array_memory_size() < budget.limits.max_batch_bytes);
                batches.push(batch);
            }
        }
        let frame = budget
            .context()
            .read_table(Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()))
            .unwrap();
        let frame = change_members(frame, 1, ComparisonIdentity::Values).unwrap();
        let context = Arc::new(frame.task_ctx());
        let plan = window::admit(frame.create_physical_plan().await.unwrap(), &budget).unwrap();
        let error = datafusion::physical_plan::collect(plan, context)
            .await
            .unwrap_err();
        let error = Error::from(error);
        assert_eq!(error.code(), "oversized_batch", "{error:#?}");
        drop(budget.reserve(budget.limits.memory_bytes / 2).unwrap());
    }
}
