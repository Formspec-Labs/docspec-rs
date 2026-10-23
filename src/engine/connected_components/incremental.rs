//! Incremental connected-components source and contributor-evidence refresh execution.

use super::{
    CONTRIBUTOR,
    eval::{EvaluatedGraph, extract_strings, output_schema},
    invalid,
    lineage::build_scope_batches,
    quote, register_input,
    topology::{GraphTopology, affected_component_closure},
};
use crate::{
    engine::{
        Engine, PhaseResult,
        incremental::source,
        locations,
        refresh::{self, LoadedInputs},
        write::{self, Replacement},
    },
    error::Result,
    model::{RefreshSpec, RefreshTransform, ReleasePin, TablePin},
    storage,
};
use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

impl Engine {
    /// Incrementally updates a `connected_components` dataset by finding changed input `row_key`s,
    /// expanding the transitive closure of affected components across old and new topologies,
    /// comparing component signatures, and replacing only modified `document_id` rows.
    pub(in crate::engine) async fn process_connected_components(
        &self,
        spec: &RefreshSpec,
        directory: &locations::Location,
        base: &ReleasePin,
        previous: &LoadedInputs,
        current: &LoadedInputs,
        captured: (&TablePin, &iceberg::table::Table),
    ) -> Result<write::Prepared> {
        let RefreshTransform::ConnectedComponents(cc) = &spec.transform else {
            return Err(invalid(
                "process_connected_components requires a ConnectedComponents refresh transform",
            ));
        };
        let budget = &self.inner.lifecycle.budget;
        let before = source(base)?;
        let context = self.context();
        register_input(&context, cc, &current.borrowed(), budget).await?;

        let previous_input = previous.releases.get(&cc.input.dataset).ok_or_else(|| {
            invalid("previous release for connected_components input dataset is missing")
        })?;
        let current_input = current.releases.get(&cc.input.dataset).ok_or_else(|| {
            invalid("current release for connected_components input dataset is missing")
        })?;

        let (affected, scanned) = self
            .changed_keys(
                &spec.dataset,
                &directory.child("affected")?,
                (previous_input, current_input),
                (
                    previous.canonical_source(&cc.input.dataset)?,
                    current.canonical_source(&cc.input.dataset)?,
                ),
                std::slice::from_ref(&cc.row_key),
            )
            .await?;
        let native_affected = storage::open_pin(&affected, budget).await?;
        let (discovery_files, discovery_bytes) = {
            let inventory = storage::snapshot_files(&native_affected, budget).await?;
            (
                inventory.data.len() as u64,
                inventory
                    .data
                    .iter()
                    .map(|file| file.file_size_in_bytes())
                    .sum::<u64>(),
            )
        };

        let key_names = vec![cc.document_id_column.clone()];
        let (scope_batches, replacement_batches) = if affected.rows == 0 {
            let empty_scope = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                &cc.document_id_column,
                DataType::Utf8,
                true,
            )])));
            let in_schema = Arc::new(context.table("cc_input").await?.schema().as_arrow().clone());
            let empty_rep = RecordBatch::new_empty(output_schema(&in_schema, cc)?);
            (vec![empty_scope], vec![empty_rep])
        } else {
            context.register_table(
                "affected_rks",
                crate::query::table(native_affected, budget.clone(), Some(affected.rows))?,
            )?;
            let mut rk_stream = context
                .sql("SELECT DISTINCT \"k0\" FROM affected_rks")
                .await?
                .execute_stream()
                .await?;
            let mut seed_rks = HashSet::new();
            while let Some(batch) = rk_stream.try_next().await? {
                let col = batch.column(0).clone();
                for i in 0..batch.num_rows() {
                    let vals = extract_strings(&col, i);
                    if vals.is_empty() {
                        seed_rks.insert(String::new());
                    } else {
                        for s in vals {
                            seed_rks.insert(s);
                        }
                    }
                }
            }

            let old_src = source(previous_input)?;
            context.register_table(
                "cc_old_input",
                crate::query::occurrences(
                    storage::open_pin(old_src, budget).await?,
                    budget.clone(),
                    Some(old_src.rows),
                )?,
            )?;

            let old_topo = GraphTopology::from_table(&context, cc, "cc_old_input", budget).await?;
            let new_topo = GraphTopology::from_table(&context, cc, "cc_input", budget).await?;

            let (old_affected_docs, new_affected_docs) =
                affected_component_closure(&old_topo, &new_topo, &seed_rks);

            let old_graph = EvaluatedGraph::from_topology(
                old_topo,
                Some(&old_affected_docs),
                &context,
                cc,
                "cc_old_input",
                budget,
            )
            .await?;
            let mut old_sigs: HashMap<String, [u8; 32]> =
                HashMap::with_capacity(old_affected_docs.len());
            for doc_id in &old_affected_docs {
                if let Some(sig) = old_graph.component_signature(cc, doc_id) {
                    old_sigs.insert(doc_id.clone(), sig);
                }
            }
            drop(old_graph);

            let new_graph = EvaluatedGraph::from_topology(
                new_topo,
                Some(&new_affected_docs),
                &context,
                cc,
                "cc_input",
                budget,
            )
            .await?;

            let mut changed_scope_docs = BTreeSet::new();
            let mut changed_new_docs = BTreeSet::new();

            for doc_id in old_affected_docs.union(&new_affected_docs) {
                let old_sig = old_sigs.get(doc_id).copied();
                let new_sig = new_graph.component_signature(cc, doc_id);
                if old_sig != new_sig {
                    changed_scope_docs.insert(doc_id.clone());
                    if new_sig.is_some() {
                        changed_new_docs.insert(doc_id.clone());
                    }
                }
            }

            let scope_batches =
                build_scope_batches(&cc.document_id_column, &changed_scope_docs, budget)?;
            let replacement_batches =
                new_graph.materialize_components_batches(cc, Some(&changed_new_docs), budget)?;
            (scope_batches, replacement_batches)
        };

        let scope = context.read_batches(scope_batches)?;
        let replacement = context.read_batches(replacement_batches)?;
        self.apply_keyed_source_replacement(
            &spec.dataset,
            directory,
            base,
            before,
            captured,
            &key_names,
            scope,
            replacement,
            (scanned, discovery_files, discovery_bytes),
        )
        .await
    }

    /// Incrementally updates the contributor-evidence group for a `connected_components` refresh
    /// over the `document_id` scope modified during source processing.
    pub(in crate::engine) async fn connected_components_evidence(
        &self,
        spec: &RefreshSpec,
        directory: &locations::Location,
        processing: &PhaseResult,
        before: &TablePin,
        context: &SessionContext,
    ) -> Result<write::Prepared> {
        let RefreshTransform::ConnectedComponents(cc) = &spec.transform else {
            return Err(invalid(
                "connected_components_evidence requires a ConnectedComponents refresh transform",
            ));
        };
        let budget = &self.inner.lifecycle.budget;
        let key_names = vec![cc.document_id_column.clone()];
        let selected = quote(&cc.document_id_column);
        let scope = context
            .sql(&format!(
                "SELECT {selected} FROM removed UNION SELECT {selected} FROM added"
            ))
            .await?;
        let mut affected_doc_ids = BTreeSet::new();
        let mut scope_stream = scope.clone().execute_stream().await?;
        while let Some(batch) = scope_stream.try_next().await? {
            let col = batch.column(0).clone();
            for i in 0..batch.num_rows() {
                for s in extract_strings(&col, i) {
                    affected_doc_ids.insert(s);
                }
            }
        }
        let replacement_batches = if affected_doc_ids.is_empty() {
            let schema = Arc::new(Schema::new(vec![
                Field::new(&cc.document_id_column, DataType::Utf8, true),
                Field::new(CONTRIBUTOR[0], DataType::Utf8, false),
                Field::new(CONTRIBUTOR[1], DataType::Int64, false),
            ]));
            vec![RecordBatch::new_empty(schema)]
        } else {
            let loaded = refresh::load_inputs(
                processing.coverage.as_ref().ok_or_else(|| {
                    invalid("connected_components processing phase is missing input coverage")
                })?,
                processing
                    .dependencies
                    .as_ref()
                    .map(|dependencies| &dependencies.input_sources)
                    .unwrap_or(&BTreeMap::new()),
                budget,
            )
            .await?;
            register_input(context, cc, &loaded.borrowed(), budget).await?;
            let topo = GraphTopology::from_table(context, cc, "cc_input", budget).await?;
            topo.materialize_evidence_batches(cc, Some(&affected_doc_ids), budget)?
        };
        let replacement = context.read_batches(replacement_batches)?;
        let applied = self
            .replace_keys(Replacement {
                dataset: &spec.dataset,
                directory: directory.clone(),
                before,
                keys: &key_names,
                scope,
                replacement,
                replacement_bytes: None,
            })
            .await?;
        Ok(write::Prepared {
            table: applied.table,
            changes: None,
            metrics: applied.metrics,
            schema_input: None,
        })
    }
}
