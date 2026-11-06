//! Produce and consume the bounded logical Substrait profile in a DocSpec job.
// Rust guideline compliant 2026-02-21
use super::Job;
use crate::{Error, Result};
use datafusion::{
    catalog::TableProvider,
    common::{DataFusionError, TableReference, tree_node::TreeNodeRecursion},
    dataframe::DataFrame,
    execution::SessionState,
    logical_expr::LogicalPlan,
};
use datafusion_substrait::{
    extensions::Extensions,
    logical_plan::{
        consumer::{
            SubstraitConsumer, from_read_rel, from_substrait_plan_with_consumer, from_substrait_rel,
        },
        producer::to_substrait_plan,
    },
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use substrait::proto::{
    AggregateRel, AggregationPhase, Plan, ReadRel, Rel, RelCommon,
    aggregate_function::AggregationInvocation,
    extensions::simple_extension_declaration::MappingType, join_rel::JoinType, plan_rel, read_rel,
    rel,
};

/// An immutable protobuf artifact, verified before decoding.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SavedPlan {
    pub path: PathBuf,
    pub sha256: String,
}

/// Create-only destinations for the binary and upstream Protobuf JSON.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlanDestinations {
    pub path: PathBuf,
    pub readable_path: PathBuf,
}

/// Observed format and declaration identities for an emitted or loaded plan.
#[derive(Debug, Deserialize, Serialize)]
pub struct SubstraitReceipt {
    path: PathBuf,
    sha256: String,
    size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    readable_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readable_sha256: Option<String>,
    version: serde_json::Value,
    extension_declarations: serde_json::Value,
    extensions_sha256: String,
    datafusion: String,
    datafusion_substrait: String,
    substrait_rust: String,
    standard_validity: String,
}

impl Job {
    pub(super) fn save_substrait(
        &self,
        frame: DataFrame,
        destinations: &PlanDestinations,
    ) -> Result<SubstraitReceipt> {
        let plan = frame.into_optimized_plan()?;
        validate_plan(&plan)?;
        let mut plan = to_substrait_plan(&plan, &self.context.state())?;
        // The upstream registry uses hash maps, but the wire order of anchors has no semantic meaning.
        plan.extensions.sort_by_cached_key(Message::encode_to_vec);
        let bytes = plan.encode_to_vec();
        let readable = serde_json::to_vec_pretty(&plan)?;
        self.check_plan_size(bytes.len())?;
        self.check_plan_size(readable.len())?;
        write_new(&destinations.path, &bytes)?;
        write_new(&destinations.readable_path, &readable)?;
        let mut receipt = receipt(&plan, destinations.path.clone(), &bytes)?;
        receipt.readable_path = Some(destinations.readable_path.clone());
        receipt.readable_sha256 = Some(sha256(&readable));
        Ok(receipt)
    }

    pub(super) async fn read_substrait(
        &self,
        binding: &SavedPlan,
    ) -> Result<(
        DataFrame,
        SubstraitReceipt,
        tokio::sync::OwnedSemaphorePermit,
    )> {
        let file = File::open(&binding.path)?;
        let size = usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX);
        self.check_plan_size(size)?;
        // The bounded wire payload, Protobuf tree and normalized plan may coexist.
        const DECODE_ALLOWANCE: usize = 32;
        let memory = self.budget.reserve(size.saturating_mul(DECODE_ALLOWANCE))?;
        let mut bytes = Vec::with_capacity(size);
        file.take((size as u64).saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() != size || sha256(&bytes) != binding.sha256 {
            return Err(Error::refusal(
                "substrait_digest",
                "saved Substrait SHA-256 differs",
            ));
        }
        let plan = Plan::decode(bytes.as_slice())
            .map_err(|error| Error::refusal("substrait_decode", error.to_string()))?;
        if !matches!(
            plan.relations.as_slice(),
            [rel] if matches!(&rel.rel_type, Some(plan_rel::RelType::Root(root)) if root.input.is_some())
        ) || plan.version.as_ref().is_none_or(|version| {
            version.major_number != 0
                || version.minor_number != 77
                || version.patch_number != 0
                || version.producer != "datafusion"
        }) || plan.advanced_extensions.is_some()
            || !plan.parameter_bindings.is_empty()
            || !plan.type_aliases.is_empty()
            || !plan.expected_type_urls.is_empty()
        {
            return Err(Error::refusal(
                "substrait_profile",
                "expected one Substrait 0.77.0 root without dynamic parameters or advanced extensions",
            ));
        }
        let extensions = guarded_extensions(&plan)?;
        let state = self.context.state();
        let consumer = NamedInputs {
            state: &state,
            extensions: &extensions,
        };
        let logical = from_substrait_plan_with_consumer(&consumer, &plan).await?;
        validate_plan(&logical)?;
        let receipt = receipt(&plan, binding.path.clone(), &bytes)?;
        Ok((DataFrame::new(state, logical), receipt, memory))
    }

    fn check_plan_size(&self, size: usize) -> Result<()> {
        if size > self.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "Substrait plan exceeds metadata allowance",
            ));
        }
        Ok(())
    }
}

#[expect(
    deprecated,
    reason = "validate both original namespace representations"
)]
fn guarded_extensions(plan: &Plan) -> Result<Extensions> {
    let mut anchors = BTreeSet::new();
    let mut names = BTreeSet::new();
    if !plan.extension_uris.is_empty()
        || !plan.extension_urns.is_empty()
        || plan.extensions.iter().any(|declaration| {
            let Some(MappingType::ExtensionFunction(function)) = &declaration.mapping_type else {
                return true;
            };
            function.extension_uri_reference != u32::MAX
                || function.extension_urn_reference != u32::MAX
                || function.name.is_empty()
                || function.name.contains(':')
                || !anchors.insert(function.function_anchor)
                || !names.insert(&function.name)
        })
    {
        return Err(Error::refusal(
            "substrait_extensions",
            "the pinned DataFusion profile requires unique local function declarations",
        ));
    }
    Ok(Extensions::try_from(&plan.extensions)?)
}

fn has_enhancement(common: Option<&RelCommon>) -> bool {
    common.is_some_and(|common| common.advanced_extension.is_some())
}

#[expect(
    deprecated,
    reason = "refuse legacy arguments that the consumer can ignore"
)]
fn supported_aggregate(relation: &AggregateRel) -> bool {
    relation.input.is_some()
        && relation.measures.iter().all(|measure| {
            measure.measure.as_ref().is_some_and(|function| {
                function.phase == AggregationPhase::Unspecified as i32
                    && function.options.is_empty()
                    && function.output_type.is_none()
                    && function.args.is_empty()
                    && matches!(
                        AggregationInvocation::try_from(function.invocation),
                        Ok(AggregationInvocation::All | AggregationInvocation::Distinct)
                    )
            })
        })
}

struct NamedInputs<'a> {
    state: &'a SessionState,
    extensions: &'a Extensions,
}

#[async_trait::async_trait]
impl SubstraitConsumer for NamedInputs<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        if !matches!(table_ref, TableReference::Bare { .. }) {
            return Err(DataFusionError::Plan(
                "Substrait requires a bare admitted input name".into(),
            ));
        }
        self.state
            .schema_for_ref(table_ref.clone())?
            .table(table_ref.table())
            .await
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl datafusion::execution::FunctionRegistry {
        self.state
    }

    async fn consume_rel(&self, relation: &Rel) -> datafusion::common::Result<LogicalPlan> {
        let (common, advanced) = match &relation.rel_type {
            Some(rel::RelType::Read(read)) => (&read.common, &read.advanced_extension),
            Some(rel::RelType::Project(rel)) if rel.input.is_some() => {
                (&rel.common, &rel.advanced_extension)
            }
            Some(rel::RelType::Filter(rel)) if rel.input.is_some() && rel.condition.is_some() => {
                (&rel.common, &rel.advanced_extension)
            }
            Some(rel::RelType::Join(rel))
                if rel.left.is_some()
                    && rel.right.is_some()
                    && rel.expression.is_some()
                    && rel.r#type == JoinType::Inner as i32 =>
            {
                (&rel.common, &rel.advanced_extension)
            }
            Some(rel::RelType::Aggregate(rel)) if supported_aggregate(rel) => {
                (&rel.common, &rel.advanced_extension)
            }
            Some(rel::RelType::Fetch(rel)) if rel.input.is_some() => {
                (&rel.common, &rel.advanced_extension)
            }
            Some(rel::RelType::Sort(rel)) if rel.input.is_some() => {
                (&rel.common, &rel.advanced_extension)
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "unsupported Substrait relation".into(),
                ));
            }
        };
        if advanced.is_some() || has_enhancement(common.as_ref()) {
            return Err(DataFusionError::Plan(
                "Substrait relation extensions are unsupported".into(),
            ));
        }
        from_substrait_rel(self, relation).await
    }

    async fn consume_read(&self, read: &ReadRel) -> datafusion::common::Result<LogicalPlan> {
        // DataFusion only passes scan filters to the provider. A separate FilterRel
        // preserves required filtering even when the provider prunes approximately.
        if read.filter.is_some() || read.best_effort_filter.is_some() {
            return Err(DataFusionError::Plan(
                "Substrait scan filters require a separate FilterRel".into(),
            ));
        }
        match &read.read_type {
            Some(read_rel::ReadType::NamedTable(named))
                if named.names.len() == 1 && named.advanced_extension.is_none() =>
            {
                from_read_rel(self, read).await
            }
            Some(read_rel::ReadType::VirtualTable(_)) if read.projection.is_none() => {
                from_read_rel(self, read).await
            }
            _ => Err(DataFusionError::Plan(
                "Substrait reads require an admitted named input or literal rows".into(),
            )),
        }
    }
}

fn validate_plan(plan: &LogicalPlan) -> Result<()> {
    crate::query::validate_immutable_functions(plan)?;
    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_) => {}
            LogicalPlan::TableScan(scan)
                if matches!(scan.table_name, TableReference::Bare { .. }) => {}
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "relation outside the DocSpec Substrait profile: {}",
                    node.display()
                )));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[expect(deprecated, reason = "preserve the plan's original URI declarations")]
fn receipt(plan: &Plan, path: PathBuf, bytes: &[u8]) -> Result<SubstraitReceipt> {
    let extension_declarations = serde_json::json!({
        "uris": plan.extension_uris,
        "urns": plan.extension_urns,
        "declarations": plan.extensions,
    });
    let extension_bytes = serde_json::to_vec(&extension_declarations)?;
    Ok(SubstraitReceipt {
        path,
        sha256: sha256(bytes),
        size: bytes.len(),
        readable_path: None,
        readable_sha256: None,
        version: serde_json::to_value(&plan.version)?,
        extension_declarations,
        extensions_sha256: sha256(&extension_bytes),
        datafusion: "53.1.0".into(),
        datafusion_substrait: "53.1.0".into(),
        substrait_rust: "0.62.2".into(),
        standard_validity: "unknown".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Limits;
    use substrait::proto::{
        JoinRel, PlanRel, RelRoot, extensions::SimpleExtensionDeclaration,
        extensions::simple_extension_declaration::ExtensionFunction,
    };

    fn single_relation(relation: rel::RelType) -> Plan {
        Plan {
            version: Some(substrait::version::version_with_producer("datafusion")),
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(Rel {
                        rel_type: Some(relation),
                    }),
                    names: Vec::new(),
                })),
            }],
            ..Default::default()
        }
    }

    async fn decoded_error(plan: Plan) -> Error {
        let root = tempfile::tempdir().unwrap();
        let job = Job::new(Limits::default(), root.path().join("scratch")).unwrap();
        let path = root.path().join("saved-plan.substrait");
        let bytes = plan.encode_to_vec();
        std::fs::write(&path, &bytes).unwrap();
        job.read_substrait(&SavedPlan {
            path,
            sha256: sha256(&bytes),
        })
        .await
        .expect_err("the guarded consumer must refuse these saved bytes")
    }

    #[tokio::test]
    async fn saved_virtual_read_cannot_ignore_a_required_filter() {
        let plan = single_relation(rel::RelType::Read(Box::new(ReadRel {
            filter: Some(Box::default()),
            read_type: Some(read_rel::ReadType::VirtualTable(Default::default())),
            ..Default::default()
        })));
        let error = decoded_error(plan).await;
        assert!(error.message().contains("separate FilterRel"));
    }

    #[tokio::test]
    async fn malformed_saved_join_refuses_before_consumer_panics() {
        let plan = single_relation(rel::RelType::Join(Box::default()));
        assert!(
            decoded_error(plan)
                .await
                .message()
                .contains("unsupported Substrait relation")
        );
    }

    #[tokio::test]
    #[expect(
        deprecated,
        reason = "exercise the producer's original namespace sentinel"
    )]
    async fn duplicate_extension_anchors_cannot_overwrite_one_another() {
        let mut plan = single_relation(rel::RelType::Join(Box::<JoinRel>::default()));
        plan.extensions = ["sum", "count"]
            .map(|name| SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                    extension_uri_reference: u32::MAX,
                    extension_urn_reference: u32::MAX,
                    function_anchor: 1,
                    name: name.into(),
                })),
            })
            .into();
        assert_eq!(decoded_error(plan).await.code(), "substrait_extensions");
    }
}
