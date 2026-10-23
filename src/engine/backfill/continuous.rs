//! One immutable scalar intent with exact recoverable source captures.
//!
//! Continuous backfill coordinates optimistic and fenced child attempts through
//! five catalog control records keyed by parent or child operation ID:
//! - `continuous/{parent}` ([`Intent`]): immutable parent intent, initial base
//!   pin, control epoch/eligibility, and authored source field bindings.
//! - `continuous-child/{child}` ([`Child`]): immutable ownership and capture
//!   record for a specific child backfill attempt (`round`, `previous`, and
//!   captured source `VersionRef`).
//! - `continuous-state/{parent}` ([`Child`]): mutable pointer to the latest
//!   admitted child attempt for `parent`.
//! - `continuous-result/{parent}` ([`Completed`]): terminal selection binding
//!   `parent` to the winning child `VersionRef` and optional fence token.
//! - `continuous-abandoned/{parent}` ([`Abandonment`]): terminal abandonment
//!   record preventing further child attempts while preserving completed work.

#[cfg(all(test, feature = "test-util"))]
mod tests;

use super::*;
use crate::catalog::SnapshotRows;
use crate::engine::intake::SourceFence;

type Record = (String, serde_json::Value);

/// Immutable parent intent record stored at `continuous/{parent}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct Intent {
    pub(in crate::engine) spec: ContinuousBackfillSpec,
    pub(in crate::engine) digest: String,
    pub(in crate::engine) initial: VersionRef,
    pub(in crate::engine) epoch: u64,
    pub(in crate::engine) eligibility: u64,
    pub(in crate::engine) input_fields: BTreeMap<String, i32>,
}

#[derive(Default, Deserialize)]
struct ControlAuthority {
    epoch: u64,
    eligibility: u64,
    definition: String,
}

#[derive(Default, Deserialize)]
struct CaptureAuthority {
    boundary: u64,
    release: Option<ReleasePin>,
}

/// Child capture and round ownership record stored at `continuous-child/{child}`
/// and referenced by `continuous-state/{parent}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct Child {
    dataset: Id,
    parent: Id,
    intent_digest: String,
    round: u32,
    child: Id,
    previous: Option<Id>,
    request_digest: String,
    capture: VersionRef,
    admission_boundary: u64,
}

/// Terminal parent completion record stored at `continuous-result/{parent}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::engine) struct Completed {
    dataset: Id,
    parent: Id,
    intent_digest: String,
    selected: VersionRef,
    fence: Option<String>,
}

/// Active child backfill attempt paired with its parent [`Intent`] and optional [`SourceFence`].
pub(super) struct Attempt {
    pub(super) intent: Intent,
    child: Child,
    fence: Option<SourceFence>,
}

fn validate_spec(spec: &ContinuousBackfillSpec) -> Result<()> {
    validate(&spec.backfill)?;
    if !(1..=16).contains(&spec.optimistic_rounds) || !(1..=300_000).contains(&spec.fence_millis) {
        return Err(Error::refusal(
            "invalid_continuous_budget",
            "continuous backfill requires 1–16 optimistic attempts and a 1–300000 millisecond fence",
        ));
    }
    Ok(())
}

fn intent_key(id: &Id) -> String {
    format!("continuous/{id}")
}
fn child_key(id: &Id) -> String {
    format!("continuous-child/{id}")
}
fn state_key(id: &Id) -> String {
    format!("continuous-state/{id}")
}
fn completed_key(id: &Id) -> String {
    format!("continuous-result/{id}")
}
fn abandoned_key(id: &Id) -> String {
    format!("continuous-abandoned/{id}")
}
fn invalid(reason: impl Into<String>) -> Error {
    Error::refusal("invalid_continuous_backfill", reason)
}
fn changed() -> Error {
    Error::refusal(
        "continuous_changed",
        "another worker advanced this backfill; resume its retained child",
    )
}

fn validate_intent(intent: &Intent) -> Result<()> {
    validate_spec(&intent.spec)?;
    if intent.digest != storage::json_digest(&intent.spec)?.to_hex().as_str()
        || intent.initial.dataset != intent.spec.backfill.dataset
        || intent.initial.version != intent.spec.backfill.base
        || intent.initial.manifest.owner != intent.initial.dataset
        || intent
            .input_fields
            .iter()
            .any(|(name, id)| name.is_empty() || *id <= 0)
        || intent.input_fields.values().collect::<BTreeSet<_>>().len() != intent.input_fields.len()
    {
        return Err(invalid(
            "parent intent identity or authored field binding differs",
        ));
    }
    Ok(())
}

fn validate_child(child: &Child, rows: &SnapshotRows, revision: i64) -> Result<()> {
    let (parent_revision, _, intent): (_, _, Intent) = rows
        .get_record(&intent_key(&child.parent))?
        .ok_or_else(|| invalid("child intent is missing"))?;
    validate_intent(&intent)?;
    let (owner_revision, _, owner): (_, _, Child) = rows
        .get_record(&child_key(&child.child))?
        .ok_or_else(|| invalid("child owner is missing"))?;
    let (operation_revision, _, operation): (_, _, Operation) = rows
        .get_record(&format!("operation/{}", child.child))?
        .ok_or_else(|| invalid("child admission is missing"))?;
    let (request_revision, _, request): (_, _, OperationRequest) = rows
        .get_record(&format!("request/{}", child.child))?
        .ok_or_else(|| invalid("child request is missing"))?;
    let mut expected = intent.spec.backfill.clone();
    expected.operation = child.child.clone();
    expected.base = child.capture.version.clone();
    let identity_matches = child.round > 0
        && child.child != child.parent
        && child.intent_digest == intent.digest
        && child.dataset == intent.spec.backfill.dataset
        && child.capture.dataset == child.dataset
        && child.capture.manifest.owner == child.dataset
        && owner == *child;
    let revisions_match = revision > 0
        && owner_revision == revision
        && operation_revision == revision
        && request_revision == revision
        && parent_revision <= revision;
    let request_matches = request == OperationRequest::Backfill(expected)
        && fingerprint(&request)? == child.request_digest
        && operation.request_digest == child.request_digest
        && operation.base.as_ref() == Some(&child.capture.version);
    if !(identity_matches && revisions_match && request_matches) {
        return Err(invalid(
            "child identity, material request or admission revision differs",
        ));
    }
    match &child.previous {
        None if child.round == 1 && parent_revision == revision => {}
        Some(previous) => {
            let (previous_revision, _, previous): (_, _, Child) = rows
                .get_record(&child_key(previous))?
                .ok_or_else(|| invalid("previous child is missing"))?;
            if previous.parent != child.parent
                || previous.intent_digest != child.intent_digest
                || previous.round.checked_add(1) != Some(child.round)
                || previous_revision >= revision
            {
                return Err(invalid(
                    "child round does not follow its parent's previous capture",
                ));
            }
        }
        _ => return Err(invalid("first child must be admitted with its parent")),
    }
    Ok(())
}

impl Engine {
    async fn continuous_child_in(&self, view: &View, key: &str) -> Result<Option<Child>> {
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(view).await?;
        let _memory = rows.reserve_decoded(&lifecycle.budget, &[key])?;
        let Some((revision, _, child)) = rows.get_record::<Child>(key)? else {
            return Ok(None);
        };
        let _dependencies = rows.reserve_decoded(
            &lifecycle.budget,
            &[
                &intent_key(&child.parent),
                &child_key(&child.child),
                &format!("request/{}", child.child),
                &format!("operation/{}", child.child),
            ],
        )?;
        let _previous = child
            .previous
            .as_ref()
            .map(|id| rows.reserve_decoded(&lifecycle.budget, &[&child_key(id)]))
            .transpose()?;
        validate_child(&child, &rows, revision)?;
        if crate::catalog::lifecycle_ident(&child.dataset) != *view.identifier() {
            return Err(invalid("child belongs to another dataset"));
        }
        Ok(Some(child))
    }
    /// Adds or replaces a scalar group while source intake continues.
    ///
    /// # Errors
    /// Refuses changed intent, competing target definitions, unresolved source
    /// admissions, expired cutover budgets and unavailable retained inputs.
    pub async fn continuous_backfill(&self, spec: &ContinuousBackfillSpec) -> Result<ReleasePin> {
        self.continuous_backfill_internal(spec, None).await
    }

    /// Interrupts a continuous operation after a retained recovery boundary.
    ///
    /// # Errors
    /// Returns `injected_failure` at the requested phase.
    #[cfg(feature = "test-util")]
    pub async fn continuous_backfill_until(
        &self,
        spec: &ContinuousBackfillSpec,
        phase: &str,
    ) -> Result<ReleasePin> {
        self.continuous_backfill_internal(spec, Some(phase)).await
    }

    async fn continuous_backfill_internal(
        &self,
        spec: &ContinuousBackfillSpec,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        self.require_local()?;
        validate_spec(spec)?;
        let memory = self.request_memory(&(spec, stop))?;
        let spec = spec.clone();
        let stop = stop.map(str::to_owned);
        self.admit_work(memory, move |engine| async move {
            engine
                .continuous_backfill_admitted(&spec, stop.as_deref())
                .await
        })
        .await
    }

    async fn continuous_backfill_admitted(
        &self,
        spec: &ContinuousBackfillSpec,
        stop: Option<&str>,
    ) -> Result<ReleasePin> {
        let dataset = &spec.backfill.dataset;
        let parent = &spec.backfill.operation;
        let digest = storage::json_digest(spec)?.to_hex().to_string();
        // A committed result takes precedence over source movement or old leases.
        if let Some(result) = self.continuous_completed(dataset, parent, &digest).await? {
            return Ok(result);
        }
        let intent = self.continuous_intent(spec, digest).await?;
        let mut fence = None;
        let outcome = async {
            for round in 0..=spec.optimistic_rounds {
                if let Some(result) = self
                    .continuous_completed(dataset, parent, &intent.digest)
                    .await?
                {
                    return Ok(result);
                }
                if round == spec.optimistic_rounds {
                    fence = Some(self.acquire_continuous_fence(&intent).await?);
                    #[cfg(feature = "test-util")]
                    work_started("continuous-fence");
                    self.drain_continuous_source(fence.as_ref().expect("acquired fence"))
                        .await?;
                }
                let (request, mut attempt) = self.continuous_attempt(&intent).await?;
                attempt.fence = fence.clone();
                let result = Box::pin(self.prepare_backfill(&request, stop, Some(&attempt))).await;
                match result {
                    Ok(value) => return Ok(value),
                    Err(error)
                        if round < spec.optimistic_rounds
                            && matches!(
                                error.code(),
                                "stale_source_boundary"
                                    | "pending_source"
                                    | "continuous_changed"
                                    | "source_fence_lost"
                            ) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(changed())
        }
        .await;
        if let Some(fence) = &fence {
            // Close only our exact token; a lost response or successor is harmless.
            self.close_source_fence(fence).await?;
        }
        if outcome
            .as_ref()
            .is_err_and(|error| error.code() != "injected_failure")
            && let Some(result) = self
                .continuous_completed(dataset, parent, &intent.digest)
                .await?
        {
            return Ok(result);
        }
        outcome
    }

    async fn continuous_completed(
        &self,
        dataset: &Id,
        parent: &Id,
        digest: &str,
    ) -> Result<Option<ReleasePin>> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        self.continuous_result_in(&view, parent, digest).await
    }

    async fn continuous_result_in(
        &self,
        view: &View,
        parent: &Id,
        digest: &str,
    ) -> Result<Option<ReleasePin>> {
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(view).await?;
        let _control_memory = rows.reserve_decoded(
            &lifecycle.budget,
            &[&intent_key(parent), &completed_key(parent)],
        )?;
        let Some(intent) = lifecycle.get::<Intent>(view, &intent_key(parent)).await? else {
            return Ok(None);
        };
        validate_intent(&intent)?;
        if intent.digest != digest {
            return Err(conflicting_operation());
        }
        let Some(completed) = lifecycle
            .get::<Completed>(view, &completed_key(parent))
            .await?
        else {
            return Ok(None);
        };
        self.continuous_child_in(view, &child_key(&completed.selected.version))
            .await?
            .ok_or_else(|| invalid("winning child is missing"))?;
        let _completion_dependencies = rows.reserve_decoded(
            &lifecycle.budget,
            &[
                &intent_key(parent),
                &completed_key(parent),
                &child_key(&completed.selected.version),
                &format!("succession/{}", completed.selected.version),
            ],
        )?;
        validate_control_record(
            &completed_key(parent),
            &serde_json::to_string(&completed)?,
            &rows,
            view.identifier().to_string().as_str(),
            rows.revision(&completed_key(parent))
                .ok_or_else(|| invalid("completion revision is missing"))?,
        )?;
        self.check_operation_available(view, &completed.selected.version)
            .await?;
        let _result_memory = rows.reserve_decoded(
            &lifecycle.budget,
            &[&format!("result/{}", completed.selected.version)],
        )?;
        let result: ReleasePin = lifecycle
            .get(view, &format!("result/{}", completed.selected.version))
            .await?
            .ok_or_else(|| invalid("winning child result is missing"))?;
        if result.version_ref() != completed.selected {
            return Err(invalid("winning child result differs"));
        }
        Ok(Some(result))
    }

    async fn continuous_intent(
        &self,
        spec: &ContinuousBackfillSpec,
        digest: String,
    ) -> Result<Intent> {
        let lifecycle = &self.inner.lifecycle;
        let request = &spec.backfill;
        let lock = self.lock(&request.dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(&request.dataset).await?;
        let rows = lifecycle.rows(&view).await?;
        let _intent_memory =
            rows.reserve_decoded(&lifecycle.budget, &[&intent_key(&request.operation)])?;
        if let Some(intent) = lifecycle
            .get::<Intent>(&view, &intent_key(&request.operation))
            .await?
        {
            if intent.digest != digest || intent.spec != *spec {
                return Err(conflicting_operation());
            }
            self.check_continuous_parent(&view, &intent).await?;
            return Ok(intent);
        }
        self.check_continuous_operation_id(&view, &request.operation)
            .await?;
        if lifecycle
            .rows(&view)
            .await?
            .contains_key(&format!("operation/{}", request.operation))
        {
            return Err(conflicting_operation());
        }
        self.check_operation_available(&view, &request.base).await?;
        let _source_memory = rows.reserve_decoded(
            &lifecycle.budget,
            &["authority", &format!("result/{}", request.base)],
        )?;
        let initial: ReleasePin = lifecycle
            .get(&view, &format!("result/{}", request.base))
            .await?
            .ok_or_else(|| {
                Error::refusal("no_release", "initial backfill source is unavailable")
            })?;
        let authority: ControlAuthority =
            lifecycle.get(&view, "authority").await?.unwrap_or_default();
        if authority.definition != request.definition {
            return Err(Error::refusal(
                "stale_definition",
                "active source definition differs",
            ));
        }
        if initial.dataset.eligibility != authority.eligibility {
            return Err(Error::refusal(
                "stale_eligibility",
                "initial source requires current qualification",
            ));
        }
        let source = initial
            .dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid_group("initial backfill release lacks a source group"))?;
        let table = storage::open_pin(source, &self.inner.lifecycle.budget).await?;
        let expressions = Expressions::compile(
            &self.context(),
            table.metadata().current_schema(),
            &request.group_definition(),
            None,
        )?;
        Ok(Intent {
            spec: spec.clone(),
            digest,
            initial: initial.version_ref(),
            epoch: authority.epoch,
            eligibility: authority.eligibility,
            input_fields: expressions.input_fields,
        })
    }

    async fn check_continuous_parent(&self, view: &View, intent: &Intent) -> Result<()> {
        validate_intent(intent)?;
        let lifecycle = &self.inner.lifecycle;
        let parent = &intent.spec.backfill.operation;
        if lifecycle
            .rows(view)
            .await?
            .contains_key(&abandoned_key(parent))
        {
            return Err(Error::refusal(
                "operation_abandoned",
                "continuous backfill was explicitly abandoned; completed work remains retained",
            ));
        }
        let _memory = lifecycle
            .rows(view)
            .await?
            .reserve_decoded(&lifecycle.budget, &["authority"])?;
        let authority: ControlAuthority =
            lifecycle.get(view, "authority").await?.unwrap_or_default();
        if authority.epoch != intent.epoch
            || authority.eligibility != intent.eligibility
            || authority.definition != intent.spec.backfill.definition
        {
            return Err(Error::refusal(
                "stale_continuous_definition",
                "backfill eligibility, epoch or active writer definition changed",
            ));
        }
        Ok(())
    }

    async fn continuous_attempt(&self, intent: &Intent) -> Result<(BackfillSpec, Attempt)> {
        let lifecycle = &self.inner.lifecycle;
        let parent = &intent.spec.backfill.operation;
        let dataset = &intent.spec.backfill.dataset;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = lifecycle.open(dataset).await?;
        let _memory = lifecycle
            .rows(&view)
            .await?
            .reserve_decoded(&lifecycle.budget, &["authority", &state_key(parent)])?;
        self.check_continuous_parent(&view, intent).await?;
        let authority: CaptureAuthority =
            lifecycle.get(&view, "authority").await?.unwrap_or_default();
        let current = authority.release.as_ref().ok_or_else(|| {
            invalid_group("continuous capture authority lacks a selected release")
        })?;
        self.continuous_target(&view, intent, current).await?;
        let previous = self.continuous_child_in(&view, &state_key(parent)).await?;
        if let Some(child) = &previous
            && child.capture == current.version_ref()
            && child.admission_boundary == authority.boundary
            && !lifecycle
                .rows(&view)
                .await?
                .contains_key(&format!("abandoned/{}", child.child))
        {
            let request: OperationRequest = lifecycle
                .get(&view, &format!("request/{}", child.child))
                .await?
                .ok_or_else(|| invalid_group("continuous child request record is missing"))?;
            let OperationRequest::Backfill(request) = request else {
                return Err(invalid_group(
                    "continuous child request record is not a backfill request",
                ));
            };
            return Ok((
                request,
                Attempt {
                    intent: intent.clone(),
                    child: child.clone(),
                    fence: None,
                },
            ));
        }
        let round = previous
            .as_ref()
            .map_or(Some(1), |child| child.round.checked_add(1))
            .ok_or_else(|| invalid("backfill round overflow"))?;
        let id = storage::json_digest(&(
            &intent.digest,
            round,
            current.version_ref(),
            authority.boundary,
        ))?
        .to_hex()
        .to_string();
        let mut request = intent.spec.backfill.clone();
        request.operation = Id::new(format!("continuous-{id}"))?;
        request.base = current.id.clone();
        let child = Child {
            dataset: dataset.clone(),
            parent: parent.clone(),
            intent_digest: intent.digest.clone(),
            round,
            child: request.operation.clone(),
            previous: previous.map(|child| child.child),
            request_digest: fingerprint(&OperationRequest::Backfill(request.clone()))?,
            capture: current.version_ref(),
            admission_boundary: authority.boundary,
        };
        Ok((
            request,
            Attempt {
                intent: intent.clone(),
                child,
                fence: None,
            },
        ))
    }

    async fn continuous_target(
        &self,
        view: &View,
        intent: &Intent,
        current: &ReleasePin,
    ) -> Result<()> {
        let _memory = self.inner.lifecycle.rows(view).await?.reserve_decoded(
            &self.inner.lifecycle.budget,
            &[&format!("result/{}", intent.initial.version)],
        )?;
        let original: ReleasePin = self
            .inner
            .lifecycle
            .get(view, &format!("result/{}", intent.initial.version))
            .await?
            .ok_or_else(|| invalid("initial target metadata is missing"))?;
        if original.version_ref() != intent.initial {
            return Err(invalid("initial target identity differs"));
        }
        self.check_backfill_target_lineage(view, &original, current, &intent.spec.backfill.group)
            .await
    }

    pub(in crate::engine) async fn check_continuous_operation_id(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<()> {
        let rows = self.inner.lifecycle.rows(view).await?;
        if rows.contains_key(&intent_key(operation)) {
            return Err(conflicting_operation());
        }
        if rows.contains_key(&child_key(operation)) {
            return Err(Error::refusal(
                "continuous_child_owned",
                "this child belongs to a continuous backfill; resume its parent",
            ));
        }
        Ok(())
    }

    pub(super) async fn check_continuous_child(
        &self,
        view: &View,
        spec: &BackfillSpec,
        attempt: Option<&Attempt>,
    ) -> Result<()> {
        let Some(attempt) = attempt else {
            return self
                .check_continuous_operation_id(view, &spec.operation)
                .await;
        };
        let lifecycle = &self.inner.lifecycle;
        let mut expected = attempt.intent.spec.backfill.clone();
        expected.operation = attempt.child.child.clone();
        expected.base = attempt.child.capture.version.clone();
        if attempt.child.child != spec.operation
            || attempt.child.dataset != spec.dataset
            || attempt.child.request_digest
                != fingerprint(&OperationRequest::Backfill(spec.clone()))?
            || attempt.child.parent != attempt.intent.spec.backfill.operation
            || attempt.child.intent_digest != attempt.intent.digest
            || *spec != expected
        {
            return Err(invalid("controlled child request differs"));
        }
        if let Some(owner) = self
            .continuous_child_in(view, &child_key(&spec.operation))
            .await?
        {
            if owner != attempt.child {
                return Err(conflicting_operation());
            }
            if let Some(result) = self
                .continuous_result_in(view, &owner.parent, &owner.intent_digest)
                .await?
            {
                return if result.id == spec.operation {
                    Ok(())
                } else {
                    Err(changed())
                };
            }
        } else if lifecycle
            .rows(view)
            .await?
            .contains_key(&format!("operation/{}", spec.operation))
        {
            return Err(conflicting_operation());
        }
        self.check_continuous_parent(view, &attempt.intent).await?;
        if let Some(recorded) = lifecycle
            .get::<Intent>(view, &intent_key(&attempt.child.parent))
            .await?
            && recorded != attempt.intent
        {
            return Err(conflicting_operation());
        }
        Ok(())
    }

    pub(super) async fn continuous_admission_records(
        &self,
        view: &View,
        spec: &BackfillSpec,
        attempt: Option<&Attempt>,
        records: &mut Vec<Record>,
    ) -> Result<bool> {
        let Some(attempt) = attempt else {
            return Ok(false);
        };
        self.check_continuous_child(view, spec, Some(attempt))
            .await?;
        let lifecycle = &self.inner.lifecycle;
        let child = &attempt.child;
        let _memory = lifecycle
            .rows(view)
            .await?
            .reserve_decoded(&lifecycle.budget, &["authority", &state_key(&child.parent)])?;
        let previous = self
            .continuous_child_in(view, &state_key(&child.parent))
            .await?;
        if previous.as_ref().map(|child| &child.child) != child.previous.as_ref() {
            return Err(changed());
        }
        let authority: CaptureAuthority =
            lifecycle.get(view, "authority").await?.unwrap_or_default();
        // A stale capture is still a valid exact, protected input. Retain its
        // child with the parent so bounded fallback can fence intake, then have
        // the caller recapture without computing this obsolete candidate.
        let stale_capture = authority
            .release
            .as_ref()
            .map(ReleasePin::version_ref)
            .as_ref()
            != Some(&child.capture)
            || authority.boundary != child.admission_boundary;
        self.continuous_target(
            view,
            &attempt.intent,
            authority.release.as_ref().ok_or_else(|| {
                invalid_group("continuous admission authority lacks a selected release")
            })?,
        )
        .await?;
        let rows = lifecycle.rows(view).await?;
        if !rows.contains_key(&intent_key(&child.parent)) {
            if rows.contains_key(&format!("operation/{}", child.parent))
                || rows.contains_key(&child_key(&child.parent))
            {
                return Err(conflicting_operation());
            }
            records.push(record(intent_key(&child.parent), &attempt.intent)?);
        }
        if let Some(previous) = previous {
            self.continuous_terminal_child(
                &rows,
                &previous,
                "superseded continuous capture",
                records,
            )?;
        }
        records.push(record(child_key(&child.child), child)?);
        records.push(record(state_key(&child.parent), child)?);
        Ok(stale_capture)
    }

    fn continuous_terminal_child(
        &self,
        rows: &SnapshotRows,
        child: &Child,
        reason: &str,
        records: &mut Vec<Record>,
    ) -> Result<()> {
        if rows.contains_key(&format!("result/{}", child.child))
            || rows.contains_key(&format!("abandoned/{}", child.child))
        {
            return Ok(());
        }
        let (revision, _, operation): (_, _, Operation) = rows
            .get_record(&format!("operation/{}", child.child))?
            .ok_or_else(|| invalid("owned child admission is missing"))?;
        if operation.request_digest != child.request_digest {
            return Err(invalid("owned child digest differs"));
        }
        records.push(record(
            format!("abandoned/{}", child.child),
            Abandonment {
                dataset: child.dataset.clone(),
                operation: child.child.clone(),
                request_digest: child.request_digest.clone(),
                operation_revision: revision,
                reason: reason.to_owned(),
            },
        )?);
        Ok(())
    }

    pub(super) async fn select_continuous_backfill(
        &self,
        view: &View,
        authority: Authority,
        release: &Retained<ReleasePin>,
        attempt: &Attempt,
    ) -> Result<()> {
        let OperationRequest::Backfill(spec) = &release.dataset.request else {
            return Err(invalid_group(
                "continuous selection candidate is not a backfill release",
            ));
        };
        self.check_continuous_child(view, spec, Some(attempt))
            .await?;
        let lifecycle = &self.inner.lifecycle;
        let state: Child = lifecycle
            .get(view, &state_key(&attempt.child.parent))
            .await?
            .ok_or_else(|| invalid("backfill state is missing"))?;
        if state != attempt.child {
            return Err(changed());
        }
        let current = authority.release.as_ref().ok_or_else(|| {
            invalid_group("continuous selection authority lacks a selected release")
        })?;
        self.continuous_target(view, &attempt.intent, current)
            .await?;
        let progress = if let Some(related) = &release.dataset.related {
            Some(Box::pin(self.guard_inputs(&related.coverage, None, RefreshPolicy::Latest)).await?)
        } else {
            None
        };
        let owned_fence: Option<SourceFence> = lifecycle.get(view, "source-fence").await?;
        let mut owned_fence =
            owned_fence.filter(|fence| fence.owner == attempt.child.parent && !fence.closed);
        if let Some(expected) = &attempt.fence {
            self.check_source_fence(view, expected).await?;
        } else if let Some(fence) = &owned_fence {
            match self.check_source_fence(view, fence).await {
                Ok(()) => {}
                Err(error) if error.code() == "source_fence_lost" => owned_fence = None,
                Err(error) => return Err(error),
            }
        }
        let completed = Completed {
            dataset: spec.dataset.clone(),
            parent: attempt.child.parent.clone(),
            intent_digest: attempt.intent.digest.clone(),
            selected: release.version_ref(),
            fence: owned_fence.as_ref().map(|fence| fence.token.clone()),
        };
        let records = vec![record(completed_key(&attempt.child.parent), completed)?];
        // Check the deadline after history and upstream-head proof, immediately
        // before the shared guarded commit. Native commit I/O itself is not timed out.
        self.select_with_records(
            view,
            authority,
            release,
            progress.as_ref(),
            records,
            owned_fence.as_ref(),
        )
        .await
    }

    async fn drain_continuous_source(&self, fence: &SourceFence) -> Result<()> {
        let mut observed = None;
        let mut delay = 20;
        loop {
            let lock = self.lock(&fence.dataset).await;
            let guard = lock.lock().await;
            let view = self.inner.lifecycle.open(&fence.dataset).await?;
            self.check_source_fence(&view, fence).await?;
            if observed != Some(view.revision) {
                if self
                    .pending_source_operations(&view, &fence.dataset)
                    .await?
                    .is_empty()
                {
                    return Ok(());
                }
                observed = Some(view.revision);
            }
            drop(guard);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            delay = (delay * 2).min(250);
        }
    }

    /// Inspects retained control progress without opening data files.
    ///
    /// # Errors
    /// Refuses changed intent, unavailable completed results and invalid metadata.
    pub async fn plan_continuous_backfill(
        &self,
        spec: &ContinuousBackfillSpec,
    ) -> Result<ContinuousBackfillPlan> {
        self.require_local()?;
        validate_spec(spec)?;
        let memory = self.request_memory(spec)?;
        let spec = spec.clone();
        self.admit_work(memory, move |engine| async move {
            let dataset = &spec.backfill.dataset;
            let parent = &spec.backfill.operation;
            let lock = engine.lock(dataset).await;
            let _guard = lock.lock().await;
            let lifecycle = &engine.inner.lifecycle;
            let view = lifecycle.open(dataset).await?;
            let _memory = lifecycle.rows(&view).await?.reserve_decoded(
                &lifecycle.budget,
                &[&intent_key(parent), &state_key(parent)],
            )?;
            let digest = storage::json_digest(&spec)?.to_hex().to_string();
            let child = engine
                .continuous_child_in(&view, &state_key(parent))
                .await?;
            let completed = engine.continuous_result_in(&view, parent, &digest).await?;
            if completed.is_some() {
                return Ok(ContinuousBackfillPlan {
                    completed,
                    round: child.as_ref().map_or(0, |child| child.round),
                    child: child.map(|child| child.child),
                    pending_source: Vec::new(),
                    fenced: false,
                });
            }
            if completed.is_none()
                && let Some(intent) = lifecycle.get::<Intent>(&view, &intent_key(parent)).await?
            {
                engine.check_continuous_parent(&view, &intent).await?;
            }
            Ok(ContinuousBackfillPlan {
                completed,
                round: child.as_ref().map_or(0, |child| child.round),
                child: child.map(|child| child.child),
                pending_source: engine.pending_source_operations(&view, dataset).await?,
                fenced: match engine.check_source_intake(&view).await {
                    Ok(()) => false,
                    Err(e) if e.code() == "source_fenced" => true,
                    Err(e) => return Err(e),
                },
            })
        })
        .await
    }

    pub(in crate::engine) async fn check_continuous_capability(
        &self,
        view: &View,
        operation: &Id,
    ) -> Result<()> {
        if let Some(child) = self
            .inner
            .lifecycle
            .get::<Child>(view, &child_key(operation))
            .await?
        {
            let intent: Intent = self
                .inner
                .lifecycle
                .get(view, &intent_key(&child.parent))
                .await?
                .ok_or_else(|| invalid("child has no parent intent"))?;
            if intent.digest != child.intent_digest {
                return Err(invalid("child parent digest differs"));
            }
            if self
                .inner
                .lifecycle
                .rows(view)
                .await?
                .contains_key(&abandoned_key(&child.parent))
            {
                return Err(Error::refusal(
                    "operation_abandoned",
                    "continuous parent was abandoned; completed work remains retained",
                ));
            }
        }
        Ok(())
    }

    pub(in crate::engine) async fn check_continuous_source_schema(
        &self,
        view: &View,
        operation: &Id,
        schema: &iceberg::spec::Schema,
    ) -> Result<()> {
        let Some(child) = self
            .inner
            .lifecycle
            .get::<Child>(view, &child_key(operation))
            .await?
        else {
            return Ok(());
        };
        let intent: Intent = self
            .inner
            .lifecycle
            .get(view, &intent_key(&child.parent))
            .await?
            .ok_or_else(|| invalid("child intent is missing"))?;
        validate_intent(&intent)?;
        // Schema operations already change the fenced writer definition. A full
        // source rebuild must additionally preserve authored field IDs/names;
        // native auto-numbering is not evidence that a reused ID has the same meaning.
        for (name, id) in &intent.input_fields {
            if schema
                .field_by_id(*id)
                .is_none_or(|field| field.name != *name)
            {
                return Err(Error::refusal(
                    "backfill_source_identity_changed",
                    "source rebuild changed authored field IDs; start a new intent against its schema",
                ));
            }
        }
        Ok(())
    }

    pub(in crate::engine) async fn check_continuous_fence_owner(
        &self,
        view: &View,
        intent: &Intent,
    ) -> Result<()> {
        let parent = &intent.spec.backfill.operation;
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(view).await?;
        let _memory = rows.reserve_decoded(&lifecycle.budget, &[&intent_key(parent)])?;
        let saved: Intent = lifecycle
            .get(view, &intent_key(parent))
            .await?
            .ok_or_else(|| invalid("fence requires an admitted parent intent"))?;
        if saved != *intent {
            return Err(conflicting_operation());
        }
        if rows.contains_key(&completed_key(parent)) {
            return Err(changed());
        }
        self.check_continuous_parent(view, intent).await
    }

    /// Called under the dataset lock by the existing abandonment service.
    pub(in crate::engine) async fn abandon_continuous_in(
        &self,
        view: &View,
        dataset: &Id,
        parent: &Id,
        reason: &str,
    ) -> Result<Option<Abandonment>> {
        let lifecycle = &self.inner.lifecycle;
        let rows = lifecycle.rows(view).await?;
        let Some((revision, _, intent)) = rows.get_record::<Intent>(&intent_key(parent))? else {
            return Ok(None);
        };
        let abandoned = Abandonment {
            dataset: dataset.clone(),
            operation: parent.clone(),
            request_digest: intent.digest,
            operation_revision: revision,
            reason: reason.to_owned(),
        };
        if let Some((_, _, old)) = rows.get_record::<Abandonment>(&abandoned_key(parent))? {
            if old != abandoned {
                return Err(Error::refusal(
                    "abandonment_conflict",
                    "parent already has a different abandonment decision",
                ));
            }
            return Ok(Some(old));
        }
        if rows.contains_key(&completed_key(parent)) {
            return Err(Error::refusal(
                "operation_completed",
                "continuous parent already selected a result",
            ));
        }
        let mut records = vec![record(abandoned_key(parent), &abandoned)?];
        if let Some((_, _, child)) = rows.get_record::<Child>(&state_key(parent))? {
            self.continuous_terminal_child(&rows, &child, reason, &mut records)?;
        }
        if let Some(mut fence) = lifecycle.get::<SourceFence>(view, "source-fence").await?
            && fence.owner == *parent
            && fence.dataset == *dataset
            && !fence.closed
        {
            crate::engine::intake::validate_record(&fence)?;
            fence.closed = true;
            records.push(record("source-fence", fence)?);
        }
        lifecycle.commit(view, records).await?;
        Ok(Some(abandoned))
    }
}

/// Control metadata carries no extra payload-availability promise.
pub(in crate::engine) fn validate_control_record(
    key: &str,
    json: &str,
    rows: &SnapshotRows,
    table: &str,
    revision: i64,
) -> Result<bool> {
    let dataset = if let Some(id) = key.strip_prefix("continuous/") {
        let intent: Intent = serde_json::from_str(json)?;
        validate_intent(&intent)?;
        if intent.spec.backfill.operation.as_str() != id
            || intent.digest != storage::json_digest(&intent.spec)?.to_hex().as_str()
            || intent.initial.dataset != intent.spec.backfill.dataset
            || intent.initial.version != intent.spec.backfill.base
            || intent.initial.manifest.owner != intent.initial.dataset
            || intent.input_fields.values().any(|id| *id <= 0)
        {
            return Err(invalid("parent intent identity differs"));
        }
        intent.spec.backfill.dataset
    } else if key.starts_with("continuous-child/") || key.starts_with("continuous-state/") {
        let child: Child = serde_json::from_str(json)?;
        validate_child(&child, rows, revision)?;
        if key
            != if key.starts_with("continuous-child/") {
                child_key(&child.child)
            } else {
                state_key(&child.parent)
            }
        {
            return Err(invalid("child control key differs"));
        }
        child.dataset
    } else if let Some(id) = key.strip_prefix("continuous-result/") {
        let completed: Completed = serde_json::from_str(json)?;
        let (_, _, intent): (_, _, Intent) = rows
            .get_record(&intent_key(&completed.parent))?
            .ok_or_else(|| invalid("completion intent is missing"))?;
        let (_, _, child): (_, _, Child) = rows
            .get_record(&child_key(&completed.selected.version))?
            .ok_or_else(|| invalid("winning child is missing"))?;
        let (selected_revision, _, succession): (_, _, Succession) = rows
            .get_record(&format!("succession/{}", completed.selected.version))?
            .ok_or_else(|| invalid("winning selection is missing"))?;
        if completed.parent.as_str() != id
            || completed.dataset != intent.spec.backfill.dataset
            || completed.intent_digest != intent.digest
            || child.parent != completed.parent
            || child.intent_digest != completed.intent_digest
            || completed.selected != succession.selected
            || completed.selected.dataset != completed.dataset
            || selected_revision != revision
            || rows.revision(&format!("result/{}", completed.selected.version)) != Some(revision)
            || rows.contains_key(&abandoned_key(&completed.parent))
        {
            return Err(invalid(
                "parent completion differs from atomic child selection",
            ));
        }
        completed.dataset
    } else if let Some(id) = key.strip_prefix("continuous-abandoned/") {
        let abandoned: Abandonment = serde_json::from_str(json)?;
        let (parent_revision, _, intent): (_, _, Intent) = rows
            .get_record(&intent_key(&abandoned.operation))?
            .ok_or_else(|| invalid("abandoned parent is missing"))?;
        if abandoned.operation.as_str() != id
            || abandoned.dataset != intent.spec.backfill.dataset
            || abandoned.request_digest != intent.digest
            || abandoned.operation_revision != parent_revision
            || revision <= parent_revision
            || abandoned.reason.trim().is_empty()
            || rows.contains_key(&completed_key(&abandoned.operation))
        {
            return Err(invalid("parent abandonment differs from exact intent"));
        }
        abandoned.dataset
    } else {
        return Ok(false);
    };
    if crate::catalog::lifecycle_ident(&dataset).to_string() != table || revision <= 0 {
        return Err(invalid(
            "continuous metadata belongs to another dataset or revision",
        ));
    }
    Ok(true)
}
