//! Explicit operation abandonment; completed phases and every object remain retained.
//!
//! This ends an operation's permission to acknowledge work or select a result.
//! Already-started I/O can finish under its existing protection. Abandonment is
//! neither worker quiescence nor permission to delete a phase or attempt.

use super::*;
use crate::catalog::View;
use std::collections::BTreeSet;

mod availability;
pub(super) mod compaction_recovery;
mod grants;
mod inventory;
pub(super) mod owners;
mod recovery;
pub(crate) mod removal;
pub(in crate::engine) mod retirement;
mod seals;

impl Engine {
    /// Requires a qualified outcome for every accepted source operation.
    ///
    /// A later selected boundary cannot stand in for an earlier request's
    /// selection or explicit abandonment. Call from the locked cutover view.
    /// `selecting` may name only the already checked schema operation selected
    /// by the same guarded commit: format-10 schemas also admitted source input.
    pub(super) async fn check_cutover_inputs(
        &self,
        view: &View,
        dataset: &Id,
        selecting: Option<&Id>,
    ) -> Result<()> {
        if self
            .pending_source_operations(view, dataset)
            .await?
            .iter()
            .any(|operation| Some(operation) != selecting)
        {
            return Err(pending_source());
        }
        Ok(())
    }

    /// Proves that every unselected admission in a captured interval was abandoned.
    /// Caller uses the same locked view for admission or final selection.
    pub(super) async fn check_abandoned_gap(
        &self,
        view: &View,
        dataset: &Id,
        selected: u64,
        fence: u64,
    ) -> Result<()> {
        let gap = fence.checked_sub(selected).ok_or_else(invalid_gap)?;
        if gap == 0 {
            return Ok(());
        }
        let budget = &self.inner.lifecycle.budget;
        let limit = budget.limits.max_metadata_bytes;
        // A conservative application charge for a BTreeSet<u64> entry, including
        // node overhead. Admission does not bound all native allocator behavior.
        let charge = gap
            .checked_mul(64)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(|| {
                Error::refusal(
                    "metadata_capacity",
                    "admission interval exceeds metadata allowance",
                )
            })?;
        let _interval = budget.reserve(charge)?;
        let _decoding = budget.reserve(limit.saturating_mul(4))?;
        let rows = self.inner.lifecycle.rows(view).await?;
        let mut boundaries = BTreeSet::new();
        for (key, revision, json) in rows.prefix("operation/") {
            tokio::task::consume_budget().await;
            let operation: Operation = serde_json::from_str(json)?;
            if operation.boundary > fence {
                return Err(invalid_gap());
            }
            if operation.boundary <= selected {
                continue;
            }
            let id = Id::new(key.strip_prefix("operation/").ok_or_else(invalid_gap)?)?;
            let (request_revision, _, request) = rows
                .get_record::<OperationRequest>(&format!("request/{id}"))?
                .ok_or_else(invalid_gap)?;
            if revision <= 0
                || revision > view.revision
                || request_revision != revision
                || request.dataset() != dataset
                || request.operation() != &id
                || fingerprint(&request)? != operation.request_digest
                || !supported_format(operation.result_format)
            {
                return Err(invalid_gap());
            }
            // Format 10 schema operations advanced the admission counter. New
            // schema and all backfill operations preserve their source coverage.
            if matches!(
                request,
                OperationRequest::Backfill(_) | OperationRequest::Evidence(_)
            ) || (matches!(request, OperationRequest::Schema(_))
                && operation.result_format == FORMAT)
            {
                continue;
            }
            let (abandoned_revision, _, abandoned) = rows
                .get_record::<Abandonment>(&format!("abandoned/{id}"))?
                .ok_or_else(pending_source)?;
            if abandoned_revision <= revision
                || abandoned_revision > view.revision
                || abandoned.operation_revision != revision
                || abandoned.operation != id
                || abandoned.dataset != *dataset
                || abandoned.request_digest != operation.request_digest
                || abandoned.reason.trim().is_empty()
                || rows.contains_key(&format!("result/{id}"))
                || !boundaries.insert(operation.boundary)
            {
                return Err(invalid_gap());
            }
        }
        if boundaries.len() as u64 != gap {
            return Err(invalid_gap());
        }
        Ok(())
    }

    /// Stops future completion and selection for one admitted, unfinished operation.
    ///
    /// Identical retries return the retained decision. Completed phases remain
    /// reusable by other operation IDs. No payloads are read or removed.
    ///
    /// # Errors
    /// Refuses missing operations, selected results, conflicting reasons, empty
    /// reasons, or a stale native commit. Running I/O is not forcibly canceled.
    pub async fn abandon(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        let memory = self.request_memory(&(dataset, operation, reason))?;
        let dataset = dataset.clone();
        let operation = operation.clone();
        let reason = reason.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.abandon_admitted(&dataset, &operation, &reason).await
        })
        .await
    }

    async fn abandon_admitted(
        &self,
        dataset: &Id,
        operation: &OperationId,
        reason: &str,
    ) -> Result<Abandonment> {
        self.require_local()?;
        if reason.trim().is_empty() {
            return Err(Error::refusal(
                "missing_reason",
                "abandonment requires a reason",
            ));
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        if reason.len() > lifecycle.budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "abandonment reason exceeds metadata allowance",
            ));
        }
        let view = lifecycle.open(dataset).await?;
        if let Some(abandoned) = self
            .abandon_continuous_in(&view, dataset, operation, reason)
            .await?
        {
            return Ok(abandoned);
        }
        let rows = lifecycle.rows(&view).await?;
        let (revision, _, admitted) = rows
            .get_record::<Operation>(&format!("operation/{operation}"))?
            .ok_or_else(|| Error::refusal("no_operation", "operation has not been admitted"))?;
        let abandoned = Abandonment {
            dataset: dataset.clone(),
            operation: operation.clone(),
            request_digest: admitted.request_digest,
            operation_revision: revision,
            reason: reason.to_owned(),
        };
        let key = format!("abandoned/{operation}");
        if let Some((_, _, existing)) = rows.get_record::<Abandonment>(&key)? {
            if existing != abandoned {
                return Err(Error::refusal(
                    "abandonment_conflict",
                    "operation already has a different abandonment decision",
                ));
            }
            return Ok(existing);
        }
        if rows.contains_key(&format!("result/{operation}")) {
            return Err(Error::refusal(
                "operation_completed",
                "selected results remain available; abandonment only stops unfinished work",
            ));
        }
        lifecycle
            .commit(&view, vec![record(key, &abandoned)?])
            .await?;
        Ok(abandoned)
    }

    pub(super) async fn check_not_abandoned(&self, view: &View, operation: &Id) -> Result<()> {
        self.check_continuous_capability(view, operation).await?;
        if self
            .inner
            .lifecycle
            .rows(view)
            .await?
            .contains_key(&format!("abandoned/{operation}"))
        {
            return Err(Error::refusal(
                "operation_abandoned",
                "operation was explicitly abandoned; completed work remains retained",
            ));
        }
        Ok(())
    }
}

fn invalid_gap() -> Error {
    Error::refusal(
        "invalid_admission_history",
        "admission interval or abandonment identities are incomplete or inconsistent",
    )
}

fn pending_source() -> Error {
    Error::refusal(
        "pending_source",
        "source admission is unresolved; select or explicitly abandon it before cutover",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn workspace() -> (tempfile::TempDir, Engine) {
        let root = tempfile::TempDir::new().unwrap();
        let limits = Limits {
            compute_threads: 1,
            memory_bytes: 32 << 20,
            scratch_bytes: 1 << 20,
            batch_rows: 32,
            max_batch_bytes: 64 << 10,
            max_metadata_bytes: 256 << 10,
        };
        let engine = Engine::open(root.path(), limits).await.unwrap();
        (root, engine)
    }

    fn admission(engine: &Engine, dataset: &Id, name: &str, boundary: u64) -> (RunSpec, Operation) {
        let request = RunSpec {
            dataset: dataset.clone(),
            operation: Id::new(name).unwrap(),
            base: None,
            relationship: None,
            input: engine
                .inner
                .root
                .join("missing-input.parquet")
                .display()
                .to_string(),
            expected_digest: "0".repeat(64),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        };
        assert!(!Path::new(&request.input).exists());
        let operation = Operation {
            request_digest: fingerprint(&OperationRequest::Build(request.clone())).unwrap(),
            eligibility: 0,
            epoch: 0,
            boundary,
            admission_fence: None,
            result_format: FORMAT,
            definition: request.definition.clone(),
            base: None,
            base_manifest: None,
            schema_key: None,
            require_current_boundary: false,
        };
        (request, operation)
    }

    fn abandonment(request: &RunSpec, operation: &Operation, revision: i64) -> Abandonment {
        Abandonment {
            dataset: request.dataset.clone(),
            operation: request.operation.clone(),
            request_digest: operation.request_digest.clone(),
            operation_revision: revision,
            reason: "superseded input".into(),
        }
    }

    fn admission_records(
        request: &RunSpec,
        operation: &Operation,
    ) -> Vec<(String, serde_json::Value)> {
        vec![
            record(format!("operation/{}", request.operation), operation).unwrap(),
            record(
                format!("request/{}", request.operation),
                OperationRequest::Build(request.clone()),
            )
            .unwrap(),
        ]
    }

    #[tokio::test]
    async fn abandoned_gap_accepts_complete_interval_without_opening_inputs() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("complete").unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let mut view = lifecycle.open(&dataset).await.unwrap();
        for (name, boundary) in [("first", 2), ("second", 3)] {
            let (request, operation) = admission(&engine, &dataset, name, boundary);
            view = lifecycle
                .commit(&view, admission_records(&request, &operation))
                .await
                .unwrap();
            let abandoned = abandonment(&request, &operation, view.revision);
            view = lifecycle
                .commit(
                    &view,
                    vec![record(format!("abandoned/{name}"), abandoned).unwrap()],
                )
                .await
                .unwrap();
        }
        engine
            .check_abandoned_gap(&view, &dataset, 1, 3)
            .await
            .unwrap();
        engine
            .check_abandoned_gap(&view, &dataset, 3, 3)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_gap_counts_legacy_schema_but_not_composition_operations() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("mixed").unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let mut view = lifecycle.open(&dataset).await.unwrap();
        let mut legacy_abandoned = None;
        for (name, format, is_schema) in [
            ("modern_schema", FORMAT, true),
            ("old_backfill", PREVIOUS_FORMAT, false),
            ("new_backfill", FORMAT, false),
            ("legacy_schema", PREVIOUS_FORMAT, true),
        ] {
            let (build, mut operation) = admission(&engine, &dataset, name, 1);
            let request = if is_schema {
                OperationRequest::Schema(SchemaSpec {
                    dataset: dataset.clone(),
                    operation: build.operation.clone(),
                    base: Id::new("base").unwrap(),
                    definition: "v1".into(),
                    next_definition: "v2".into(),
                    fields: vec![SchemaField {
                        id: 1,
                        name: "renamed".into(),
                    }],
                    additions: vec![],
                    write_defaults: BTreeMap::new(),
                })
            } else {
                OperationRequest::Backfill(BackfillSpec {
                    dataset: dataset.clone(),
                    operation: build.operation.clone(),
                    base: Id::new("base").unwrap(),
                    definition: "v1".into(),
                    group: Id::new("derived").unwrap(),
                    group_definition: "g1".into(),
                    fields: vec![ComputedField {
                        id: 1,
                        name: "value".into(),
                        expression: "1".into(),
                    }],
                })
            };
            operation.result_format = format;
            operation.request_digest = fingerprint(&request).unwrap();
            view = lifecycle
                .commit(
                    &view,
                    vec![
                        record(format!("operation/{name}"), &operation).unwrap(),
                        record(format!("request/{name}"), request).unwrap(),
                    ],
                )
                .await
                .unwrap();
            if name == "legacy_schema" {
                legacy_abandoned = Some(abandonment(&build, &operation, view.revision));
            }
        }
        let error = engine
            .check_abandoned_gap(&view, &dataset, 0, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "pending_source");
        view = lifecycle
            .commit(
                &view,
                vec![record("abandoned/legacy_schema", legacy_abandoned.unwrap()).unwrap()],
            )
            .await
            .unwrap();
        // The other operations share this boundary but do not resolve or duplicate it.
        engine
            .check_abandoned_gap(&view, &dataset, 0, 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_gap_refuses_incomplete_or_conflicting_identities() {
        let (_root, engine) = workspace().await;
        let lifecycle = &engine.inner.lifecycle;
        for case in [
            "missing_boundary",
            "duplicate_boundary",
            "missing_request",
            "missing_abandonment",
            "request_dataset",
            "request_operation",
            "request_digest",
            "unknown_format",
            "abandonment_dataset",
            "abandonment_operation",
            "abandonment_digest",
            "admission_revision",
            "empty_reason",
            "selected_result",
            "future_boundary",
        ] {
            let dataset = Id::new(case).unwrap();
            let mut view = lifecycle.open(&dataset).await.unwrap();
            let (mut request, mut operation) = admission(&engine, &dataset, "first", 1);
            match case {
                "request_dataset" => request.dataset = Id::new("other").unwrap(),
                "request_operation" => request.operation = Id::new("other").unwrap(),
                "unknown_format" => operation.result_format = FORMAT + 1,
                "future_boundary" => operation.boundary = 2,
                _ => {}
            }
            operation.request_digest =
                fingerprint(&OperationRequest::Build(request.clone())).unwrap();
            if case == "request_digest" {
                operation.request_digest = "wrong-digest".into();
            }
            // Keep lifecycle keys fixed even when the embedded request identity is forged.
            let mut admitted = vec![record("operation/first", &operation).unwrap()];
            if case != "missing_request" {
                admitted.push(
                    record("request/first", OperationRequest::Build(request.clone())).unwrap(),
                );
            }
            let duplicate = admission(&engine, &dataset, "second", 1);
            if case == "duplicate_boundary" {
                admitted.extend(admission_records(&duplicate.0, &duplicate.1));
            }
            view = lifecycle.commit(&view, admitted).await.unwrap();
            let mut abandoned = abandonment(&request, &operation, view.revision);
            // The tombstone must identify the canonical operation, not the forged request.
            abandoned.dataset = dataset.clone();
            abandoned.operation = Id::new("first").unwrap();
            match case {
                "abandonment_dataset" => abandoned.dataset = Id::new("other").unwrap(),
                "abandonment_operation" => abandoned.operation = Id::new("other").unwrap(),
                "abandonment_digest" => abandoned.request_digest = "wrong-digest".into(),
                "admission_revision" => abandoned.operation_revision += 1,
                "empty_reason" => abandoned.reason = " \t".into(),
                _ => {}
            }
            if case != "missing_abandonment" {
                let mut retired = vec![record("abandoned/first", abandoned).unwrap()];
                if case == "duplicate_boundary" {
                    retired.push(
                        record(
                            "abandoned/second",
                            abandonment(&duplicate.0, &duplicate.1, view.revision),
                        )
                        .unwrap(),
                    );
                }
                if case == "selected_result" {
                    retired.push(record("result/first", serde_json::json!({})).unwrap());
                }
                view = lifecycle.commit(&view, retired).await.unwrap();
            }
            let fence = if case == "missing_boundary" { 2 } else { 1 };
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, fence)
                .await
                .unwrap_err();
            let expected = if case == "missing_abandonment" {
                "pending_source"
            } else {
                "invalid_admission_history"
            };
            assert_eq!(error.code(), expected, "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn abandoned_gap_requires_original_admission_and_later_visible_abandonment() {
        let (_root, engine) = workspace().await;
        let lifecycle = &engine.inner.lifecycle;
        for case in [
            "same_commit",
            "before_admission",
            "rewritten_request",
            "rewritten_admission",
            "operation_after_view",
            "abandonment_after_view",
        ] {
            let dataset = Id::new(case).unwrap();
            let mut view = lifecycle.open(&dataset).await.unwrap();
            let (request, operation) = admission(&engine, &dataset, "first", 1);
            let mut admitted = admission_records(&request, &operation);
            match case {
                "same_commit" => {
                    admitted.push(
                        record("abandoned/first", abandonment(&request, &operation, 1)).unwrap(),
                    );
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                }
                "before_admission" => {
                    view = lifecycle
                        .commit(
                            &view,
                            vec![
                                record("abandoned/first", abandonment(&request, &operation, 2))
                                    .unwrap(),
                            ],
                        )
                        .await
                        .unwrap();
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                }
                _ => {
                    view = lifecycle.commit(&view, admitted).await.unwrap();
                    let abandoned = abandonment(&request, &operation, view.revision);
                    view = lifecycle
                        .commit(&view, vec![record("abandoned/first", abandoned).unwrap()])
                        .await
                        .unwrap();
                    match case {
                        "rewritten_request" => {
                            view = lifecycle
                                .commit(
                                    &view,
                                    vec![
                                        record(
                                            "request/first",
                                            OperationRequest::Build(request.clone()),
                                        )
                                        .unwrap(),
                                    ],
                                )
                                .await
                                .unwrap();
                        }
                        "rewritten_admission" => {
                            view = lifecycle
                                .commit(&view, admission_records(&request, &operation))
                                .await
                                .unwrap();
                        }
                        "operation_after_view" => view.revision = 0,
                        "abandonment_after_view" => view.revision = 1,
                        _ => unreachable!(),
                    }
                }
            }
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, 1)
                .await
                .unwrap_err();
            assert_eq!(error.code(), "invalid_admission_history", "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn abandoned_gap_refuses_huge_or_reversed_intervals_without_expanding_them() {
        let (_root, engine) = workspace().await;
        let dataset = Id::new("bounded").unwrap();
        let view = engine.inner.lifecycle.open(&dataset).await.unwrap();
        let finite_overflow =
            engine.inner.lifecycle.budget.limits.max_metadata_bytes as u64 / 64 + 1;
        for fence in [finite_overflow, u64::MAX] {
            let error = engine
                .check_abandoned_gap(&view, &dataset, 0, fence)
                .await
                .unwrap_err();
            assert_eq!(error.code(), "metadata_capacity", "fence {fence}: {error}");
        }
        let error = engine
            .check_abandoned_gap(&view, &dataset, 2, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "invalid_admission_history");
        engine
            .check_abandoned_gap(&view, &dataset, u64::MAX, u64::MAX)
            .await
            .unwrap();
    }
}
