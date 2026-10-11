//! End standalone seal availability while preserving exact consumer coverage.

use super::owners::{ReferenceBinding, binding_key};
use super::retirement::{invalid_retirement, record_retired};
use super::*;
use crate::catalog::SnapshotRows;

fn validate_record(
    rows: &SnapshotRows,
    view: &View,
    generation: &Id,
    budget: &crate::budget::Budget,
) -> Result<RetainedGeneration> {
    let key = format!("generation/{generation}");
    let binding_key = binding_key(&key);
    let _memory = rows.reserve_decoded(budget, &[&key, &binding_key, "family-members"])?;
    let (revision, _, seal): (_, _, GenerationSeal) = rows
        .get_record(&key)?
        .ok_or_else(|| Error::refusal("unsealed_generation", "generation has no retained seal"))?;
    let (_, _, members): (_, _, BTreeSet<Id>) = rows
        .get_record("family-members")?
        .ok_or_else(invalid_retirement)?;
    inputs::validate_seal(&seal)?;
    if seal.generation != *generation
        || &crate::catalog::lifecycle_ident(&seal.family) != view.identifier()
        || members != seal.members.keys().cloned().collect()
    {
        return Err(invalid_retirement());
    }
    let (binding_revision, _, binding): (_, _, ReferenceBinding) =
        rows.get_record(&binding_key)?
            .ok_or_else(invalid_retirement)?;
    if binding.key != key || binding_revision != revision {
        return Err(invalid_retirement());
    }
    super::owners::validate_binding(
        rows,
        view,
        &binding_key,
        binding_revision,
        &binding,
        budget.limits.max_metadata_bytes,
    )?;
    Ok(RetainedGeneration { key, revision })
}

pub(super) fn validate_plan(
    rows: &SnapshotRows,
    view: &View,
    plan: &RetentionPlan,
    budget: &crate::budget::Budget,
) -> Result<()> {
    if let RetirementScope::GenerationSeal { generation } = &plan.scope
        && plan.records != [validate_record(rows, view, generation, budget)?]
    {
        return Err(invalid_retirement());
    }
    Ok(())
}

fn check_available(
    rows: &SnapshotRows,
    view: &View,
    generation: &Id,
    budget: &crate::budget::Budget,
) -> Result<()> {
    let key = format!("generation/{generation}");
    if let Some(revision) = rows.revision(&key)
        && record_retired(rows, view, &key, revision, budget)?
    {
        return Err(Error::refusal(
            "generation_retired",
            "this generation seal is unavailable for new input capture or sealing",
        ));
    }
    Ok(())
}

impl Engine {
    pub(in crate::engine) async fn check_generation_available(
        &self,
        view: &View,
        generation: &Id,
    ) -> Result<()> {
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(view).await?;
        check_available(&rows, view, generation, &lifecycle.budget)
    }

    /// Captures one seal's exact generation for standalone availability retirement.
    ///
    /// Planning preserves the immutable seal, member identities and every object.
    /// Consumer coverage and independently retained results keep their own protection.
    ///
    /// # Errors
    /// Refuses unknown or retired seals, malformed bindings, empty reasons,
    /// unqualified catalogs, or exhausted metadata capacity.
    pub async fn plan_generation_retirement(
        &self,
        family: &Id,
        retirement: &Id,
        generation: &Id,
        reason: &str,
    ) -> Result<RetentionPlan> {
        let memory = self.request_memory(&(family, retirement, generation, reason))?;
        let (family, retirement, generation, reason) = (
            family.clone(),
            retirement.clone(),
            generation.clone(),
            reason.to_owned(),
        );
        self.admit_work(memory, move |engine| async move {
            engine.retirement_catalog()?;
            if reason.trim().is_empty() {
                return Err(invalid_retirement());
            }
            let lock = engine.lock(&family).await;
            let _guard = lock.lock().await;
            let view = engine.inner.lifecycle.open(&family).await?;
            let rows = engine.inner.lifecycle.rows(&view).await?;
            let record =
                validate_record(&rows, &view, &generation, &engine.inner.lifecycle.budget)?;
            check_available(&rows, &view, &generation, &engine.inner.lifecycle.budget)?;
            super::retirement::plan(
                &view,
                family,
                retirement,
                RetirementScope::GenerationSeal { generation },
                reason,
                vec![record],
            )
        })
        .await
    }

    /// Ends the standalone seal promise without deleting members or consumer evidence.
    ///
    /// The exact seal identity remains retained. New capture or resealing refuses;
    /// admitted consumer work and retained results preserve their independent promises.
    /// Owner grants require separate unused-grant retirement before removal.
    ///
    /// # Errors
    /// Refuses another scope, changed generations, malformed bindings, conflicting
    /// retries, unqualified catalogs, or exhausted metadata capacity.
    pub async fn retire_generation(&self, plan: &RetentionPlan) -> Result<Retirement> {
        if !matches!(plan.scope, RetirementScope::GenerationSeal { .. }) {
            return Err(invalid_retirement());
        }
        self.retire_retained(plan).await
    }
}
