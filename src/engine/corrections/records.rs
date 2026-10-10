//! Immutable scoped corrections and directly addressed cumulative requirements.

use crate::engine::{backfill, inputs::metadata_size, *};
use std::collections::BTreeSet;

// Rust guideline compliant 2026-02-21

/// Current material requirements for the exact scalar processor and definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct Requirements {
    pub(in crate::engine) processor: String,
    pub(in crate::engine) definition: GroupDefinition,
    pub(in crate::engine) fields: BTreeSet<i32>,
    pub(in crate::engine) latest_decision: Id,
    pub(in crate::engine) first_revision: i64,
    pub(in crate::engine) revision: i64,
}

/// Addresses requirements independently of source definitions, epochs and adoption IDs.
pub(in crate::engine) fn scope_key(definition: &GroupDefinition) -> Result<String> {
    let bytes = serde_json::to_vec(&(backfill::PROCESSOR, definition))?;
    Ok(format!(
        "correction-requirements/{}",
        blake3::hash(&bytes).to_hex()
    ))
}

fn decision_key(id: &Id) -> String {
    format!("dependency-correction/{id}")
}

impl Engine {
    /// Retains omitted material dependencies without changing selected output or global eligibility.
    ///
    /// Identical retries return the original decision. Recording a correction does
    /// not certify historical evidence or qualify any completed computation.
    ///
    /// # Errors
    /// Refuses invalid declarations, empty reasons or fields, conflicting retry
    /// content, corrupt metadata, exhausted capacity, or failed native commits.
    pub async fn correct_dependencies(
        &self,
        correction: &DependencyCorrection,
    ) -> Result<CorrectionDecision> {
        let memory = self.request_memory(correction)?;
        let correction = correction.clone();
        self.admit_work(memory, move |engine| async move {
            engine.correct_dependencies_admitted(&correction).await
        })
        .await
    }

    async fn correct_dependencies_admitted(
        &self,
        correction: &DependencyCorrection,
    ) -> Result<CorrectionDecision> {
        validate(correction)?;
        let lifecycle = &self.inner.lifecycle;
        let limit = lifecycle.budget.limits.max_metadata_bytes;
        let lock = self.lock(&correction.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&correction.dataset).await?;
        if let Some(previous) = self.correction_decision(&view, correction).await? {
            return Ok(previous);
        }
        let current = self.requirements(&view, &correction.definition).await?;
        // Admission covers the bounded cumulative set and serialized commit copies.
        let bytes = metadata_size(&(correction, &current), limit)?;
        let _memory = lifecycle.budget.reserve(bytes.saturating_mul(8))?;
        let revision = view
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::refusal("revision_exhausted", "correction revision overflow"))?;
        let first_revision = current
            .as_ref()
            .map_or(revision, |state| state.first_revision);
        let mut fields = current.map_or_else(BTreeSet::new, |state| state.fields);
        fields.extend(&correction.fields);
        let requirements = Requirements {
            processor: backfill::PROCESSOR.into(),
            definition: correction.definition.clone(),
            fields,
            latest_decision: correction.id.clone(),
            first_revision,
            revision,
        };
        let decision = CorrectionDecision {
            request: correction.clone(),
            processor: backfill::PROCESSOR.into(),
            revision,
        };
        metadata_size(&(&decision, &requirements), limit)?;
        let changes = vec![
            record(decision_key(&correction.id), &decision)?,
            record(scope_key(&correction.definition)?, &requirements)?,
        ];
        match lifecycle.commit(&view, changes).await {
            Ok(committed) if committed.revision == revision => Ok(decision),
            Ok(_) => Err(corrupt()),
            Err(error) => {
                // A catalog may commit successfully and lose its response. Resolve
                // the immutable ID from a fresh view; never retry a stale union.
                let fresh = lifecycle.open(&correction.dataset).await?;
                match self.correction_decision(&fresh, correction).await? {
                    Some(retained) => Ok(retained),
                    None => Err(error),
                }
            }
        }
    }

    /// Resolves the current requirements at the caller's exact decision boundary.
    pub(in crate::engine) async fn requirements(
        &self,
        view: &View,
        definition: &GroupDefinition,
    ) -> Result<Option<Requirements>> {
        let lifecycle = &self.inner.lifecycle;
        metadata_size(definition, lifecycle.budget.limits.max_metadata_bytes)?;
        let key = scope_key(definition)?;
        let rows = lifecycle.rows(view).await?;
        let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
        let Some((revision, _, state)) = rows.get_record::<Requirements>(&key)? else {
            return Ok(None);
        };
        if state.processor != backfill::PROCESSOR
            || state.definition != *definition
            || state.revision != revision
            || revision <= 0
            || state.first_revision <= 0
            || state.first_revision > revision
            || state.fields.is_empty()
            || state.fields.iter().any(|field| *field <= 0)
        {
            return Err(corrupt());
        }
        Ok(Some(state))
    }

    async fn correction_decision(
        &self,
        view: &View,
        correction: &DependencyCorrection,
    ) -> Result<Option<CorrectionDecision>> {
        let lifecycle = &self.inner.lifecycle;
        let key = decision_key(&correction.id);
        let rows = lifecycle.rows(view).await?;
        let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
        let Some((revision, _, decision)) = rows.get_record::<CorrectionDecision>(&key)? else {
            return Ok(None);
        };
        if decision.processor != backfill::PROCESSOR
            || decision.revision != revision
            || revision <= 0
        {
            return Err(corrupt());
        }
        if decision.request != *correction {
            return Err(Error::refusal(
                "correction_conflict",
                "the correction identifier already retains different request content",
            ));
        }
        Ok(Some(decision))
    }
}

pub(in crate::engine) fn validate(correction: &DependencyCorrection) -> Result<()> {
    backfill::validate_definition(&correction.definition)?;
    if correction.reason.trim().is_empty() {
        return Err(Error::refusal(
            "missing_reason",
            "dependency corrections require a retained reason",
        ));
    }
    if correction.fields.is_empty() || correction.fields.iter().any(|field| *field <= 0) {
        return Err(Error::refusal(
            "invalid_correction",
            "dependency corrections require positive source-field IDs",
        ));
    }
    Ok(())
}

fn corrupt() -> Error {
    Error::refusal(
        "corrupt_metadata",
        "correction records must match their scope and actual native revision",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn correction(id: &str, fields: &[i32]) -> DependencyCorrection {
        DependencyCorrection {
            dataset: Id::new("comments").unwrap(),
            id: Id::new(id).unwrap(),
            definition: GroupDefinition {
                group: Id::new("classification").unwrap(),
                definition: "text-classifier-v1".into(),
                fields: vec![ComputedField {
                    id: 1,
                    name: "label".into(),
                    expression: "text".into(),
                }],
            },
            fields: fields.iter().copied().collect(),
            reason: "language was material".into(),
        }
    }

    fn limits() -> Limits {
        Limits {
            compute_threads: 2,
            memory_bytes: 256 << 20,
            scratch_bytes: 16 << 20,
            max_batch_bytes: 4 << 20,
            max_metadata_bytes: 16 << 20,
            ..Limits::default()
        }
    }

    #[tokio::test]
    async fn retry_conflict_and_cumulative_corrections_preserve_authority() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let first = correction("omission-one", &[2]);
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&first.dataset).await.unwrap();
        let authority = Authority {
            definition: "source-v1".into(),
            ..Authority::default()
        };
        lifecycle
            .commit(
                &view,
                vec![
                    record("authority", &authority).unwrap(),
                    record("head", "retained-head").unwrap(),
                ],
            )
            .await
            .unwrap();
        let decision = engine.correct_dependencies(&first).await.unwrap();
        assert_eq!(engine.correct_dependencies(&first).await.unwrap(), decision);
        let stable = lifecycle.open(&first.dataset).await.unwrap().revision;
        let mut changed = first.clone();
        changed.fields.insert(3);
        assert_eq!(
            engine
                .correct_dependencies(&changed)
                .await
                .unwrap_err()
                .code(),
            "correction_conflict"
        );
        assert_eq!(
            lifecycle.open(&first.dataset).await.unwrap().revision,
            stable
        );
        let second = correction("omission-two", &[3]);
        let newer = engine.correct_dependencies(&second).await.unwrap();
        let current = lifecycle.open(&first.dataset).await.unwrap();
        let requirements = engine
            .requirements(&current, &first.definition)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(requirements.fields, BTreeSet::from([2, 3]));
        assert_eq!(requirements.latest_decision, second.id);
        assert_eq!(requirements.first_revision, decision.revision);
        assert_eq!(requirements.revision, newer.revision);
        assert_eq!(engine.correct_dependencies(&first).await.unwrap(), decision);
        assert_eq!(
            lifecycle
                .get::<Authority>(&current, "authority")
                .await
                .unwrap(),
            Some(authority)
        );
        assert_eq!(
            lifecycle.get::<String>(&current, "head").await.unwrap(),
            Some("retained-head".into())
        );
        let mut different = first.definition.clone();
        different.fields[0].expression = "upper(text)".into();
        assert!(
            engine
                .requirements(&current, &different)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_corrections_preserve_both_fields_and_retry_generations() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let first = correction("concurrent-one", &[2]);
        let second = correction("concurrent-two", &[3]);
        let (one, two) = tokio::join!(
            engine.correct_dependencies(&first),
            engine.correct_dependencies(&second),
        );
        let (one, two) = (one.unwrap(), two.unwrap());
        assert_ne!(one.revision, two.revision);
        let view = engine.inner.lifecycle.open(&first.dataset).await.unwrap();
        let current = engine
            .requirements(&view, &first.definition)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.fields, BTreeSet::from([2, 3]));
        assert_eq!(current.first_revision, one.revision.min(two.revision));
        assert_eq!(current.revision, one.revision.max(two.revision));
        assert_eq!(engine.correct_dependencies(&first).await.unwrap(), one);
        assert_eq!(engine.correct_dependencies(&second).await.unwrap(), two);
        assert_eq!(
            engine
                .inner
                .lifecycle
                .open(&first.dataset)
                .await
                .unwrap()
                .revision,
            view.revision
        );
    }

    #[tokio::test]
    async fn lost_commit_response_reconciles_actual_native_generation() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let request = correction("lost-response", &[2]);
        engine
            .inner
            .lifecycle
            .lose_commit_response
            .store(true, Ordering::SeqCst);
        let decision = engine.correct_dependencies(&request).await.unwrap();
        let view = engine.inner.lifecycle.open(&request.dataset).await.unwrap();
        assert_eq!(decision.revision, view.revision);
        assert_eq!(
            engine.correct_dependencies(&request).await.unwrap(),
            decision
        );
        assert_eq!(
            engine
                .requirements(&view, &request.definition)
                .await
                .unwrap()
                .unwrap()
                .revision,
            decision.revision
        );
    }

    #[tokio::test]
    async fn invalid_requests_leave_native_revision_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), limits()).await.unwrap();
        let valid = correction("invalid", &[2]);
        let before = engine
            .inner
            .lifecycle
            .open(&valid.dataset)
            .await
            .unwrap()
            .revision;
        let mut missing_reason = valid.clone();
        missing_reason.reason = " ".into();
        let mut missing_fields = valid.clone();
        missing_fields.fields.clear();
        let mut nonpositive = valid.clone();
        nonpositive.fields.insert(0);
        let mut invalid_definition = valid.clone();
        invalid_definition.definition.fields[0].expression.clear();
        for request in [
            missing_reason,
            missing_fields,
            nonpositive,
            invalid_definition,
        ] {
            assert!(engine.correct_dependencies(&request).await.is_err());
        }
        assert_eq!(
            engine
                .inner
                .lifecycle
                .open(&valid.dataset)
                .await
                .unwrap()
                .revision,
            before
        );
    }
}
