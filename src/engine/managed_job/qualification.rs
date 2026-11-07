//! Exact retained Arrow report checks, independent from external DCC qualification.
// Rust guideline compliant 2026-02-21
use super::*;
use arrow::{
    array::{
        Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    ipc::convert::try_schema_from_ipc_buffer,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Evaluator {
    id: String,
    version: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Semantics {
    id: String,
    revision: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewNames {
    inputs: Vec<String>,
    steps: Vec<String>,
    outputs: Vec<String>,
    reports: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequiredReport {
    name: String,
    sql_sha256: String,
    schema_ipc_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CounterPolicy {
    id: Id,
    profile: String,
    evaluator: Evaluator,
    semantics: Semantics,
    job_sha256: String,
    view_names: ViewNames,
    required_reports: Vec<RequiredReport>,
}
impl CounterPolicy {
    pub(super) fn validate(&self, id: &Id, job_sha256: &str, job: &JobSpec) -> Result<()> {
        if &self.id != id
            || self.profile != COUNTERS
            || self.job_sha256 != job_sha256
            || self.evaluator.id != "urn:docspec:evaluator:native-job-reports"
            || self.evaluator.version != "1"
            || self.semantics.id != "urn:docspec:semantics:complete-integer-zero-row"
            || self.semantics.revision != "1"
            || self.view_names.inputs
                != job
                    .inputs
                    .iter()
                    .map(|input| input.name().to_owned())
                    .collect::<Vec<_>>()
            || self.view_names.steps
                != job
                    .steps
                    .iter()
                    .map(|step| step.name().to_owned())
                    .collect::<Vec<_>>()
            || self.view_names.outputs
                != job
                    .outputs
                    .iter()
                    .map(|output| output.name.clone())
                    .collect::<Vec<_>>()
            || self.view_names.reports
                != job
                    .queries
                    .iter()
                    .map(|query| query.name.clone())
                    .collect::<Vec<_>>()
            || self.required_reports.is_empty()
        {
            return Err(mismatch());
        }
        let mut names = BTreeSet::new();
        for report in &self.required_reports {
            let original = job
                .queries
                .iter()
                .find(|query| query.name == report.name)
                .ok_or_else(mismatch)?;
            if !names.insert(&report.name) || sha256(original.sql.as_bytes()) != report.sql_sha256 {
                return Err(mismatch());
            }
            let bytes = STANDARD
                .decode(&report.schema_ipc_base64)
                .map_err(|_| mismatch())?;
            let schema = try_schema_from_ipc_buffer(&bytes)?;
            validate_counter_schema(&schema)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Check {
    column: String,
    status: String,
    value: u8,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportAcceptance {
    name: String,
    sql_sha256: String,
    completion: ObjectRef,
    schema_sha256: String,
    checks: Vec<Check>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    profile: String,
    dataset: Id,
    operation: Id,
    request_digest: String,
    candidate: JobCandidateRef,
    view_set_digest: String,
    policy_id: Id,
    policy_sha256: String,
    expected_epoch: u64,
    expected_selection: Option<JobCandidateRef>,
    evaluator: Evaluator,
    semantics: Semantics,
    reports: Vec<ReportAcceptance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecord {
    table: String,
    key: String,
    revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Accepted {
    #[serde(flatten)]
    body: Body,
    identity: String,
    body_sha256: String,
    body_bytes_base64: String,
    native_record: NativeRecord,
    policy: JobArtifactWitness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selected {
    request_digest: String,
    pub(super) candidate: JobCandidateRef,
    policy_id: Id,
    accepted_qualification: String,
}

impl Engine {
    async fn job_binding(
        &self,
        dataset: &Id,
        operation: &Id,
        digest: &str,
        candidate: &JobCandidateRef,
    ) -> Result<(Held<Admission>, Held<execution::Completion>)> {
        let admitted = self
            .job_record::<Admission>(dataset, &format!("job-operation/{operation}"))
            .await?
            .ok_or_else(mismatch)?;
        self.check_job_recovery(&admitted).await?;
        let completed = self
            .job_record::<execution::Completion>(dataset, &format!("job-result/{operation}"))
            .await?
            .ok_or_else(|| Error::refusal("job_pending", "whole-job execution is not complete"))?;
        if admitted.request_digest != digest || completed.candidate != *candidate {
            return Err(mismatch());
        }
        Ok((admitted, completed))
    }

    pub(super) async fn accept_job(
        &self,
        dataset: &Id,
        operation: &Id,
        digest: &str,
        candidate: &JobCandidateRef,
        policy_id: &Id,
    ) -> Result<()> {
        let (admitted, completed) = self
            .job_binding(dataset, operation, digest, candidate)
            .await?;
        let key = format!("job-accepted/{operation}/{policy_id}");
        if let Some(saved) = self.job_record::<Accepted>(dataset, &key).await? {
            validate_accepted(&saved, dataset, operation, digest, candidate, policy_id)?;
            return Ok(());
        }
        let active = self
            .job_record::<Id>(dataset, &format!("job-active/{operation}"))
            .await?
            .ok_or_else(mismatch)?;
        if active.value != *policy_id {
            return Err(mismatch());
        }
        let decision = self
            .job_record::<Decision>(dataset, &format!("job-policy/{operation}/{policy_id}"))
            .await?
            .ok_or_else(mismatch)?;
        let budget = &self.inner.lifecycle.budget;
        let policy_bytes = witness_bytes(&decision.policy, budget)?;
        let policy: CounterPolicy = serde_json::from_slice(&policy_bytes.value)?;
        let job_bytes = witness_bytes(&admitted.job, budget)?;
        let job: JobSpec = serde_json::from_slice(&job_bytes.value)?;
        policy.validate(policy_id, &admitted.request.job.sha256, &job)?;
        let manifest_bytes = witness_bytes(&completed.manifest, budget)?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes.value)?;
        if manifest["execution"] != "completed" || manifest["request_digest"] != digest {
            return Err(mismatch());
        }
        let view_set_digest = manifest["view_set_digest"]
            .as_str()
            .ok_or_else(mismatch)?
            .to_owned();
        let reports = manifest["reports"].as_array().ok_or_else(mismatch)?;
        if reports.len() != job.queries.len()
            || reports.iter().zip(&job.queries).any(|(report, query)| {
                report["name"] != query.name || report["sql_sha256"] != sha256(query.sql.as_bytes())
            })
        {
            return Err(mismatch());
        }
        let mut accepted_reports = Vec::new();
        for required in &policy.required_reports {
            let report = reports
                .iter()
                .find(|report| report["name"] == required.name)
                .ok_or_else(mismatch)?;
            let completion: ObjectRef = serde_json::from_value(report["completion"].clone())?;
            let bytes = execution::read_retained(&completion, budget)?;
            let reader = StreamReader::try_new(Cursor::new(&bytes.value), None)?;
            let expected = STANDARD
                .decode(&required.schema_ipc_base64)
                .map_err(|_| mismatch())?;
            let expected = try_schema_from_ipc_buffer(&expected)?;
            if reader.schema().as_ref() != &expected {
                return Err(Error::refusal(
                    "job_report_schema",
                    "native report schema differs from the original policy",
                ));
            }
            validate_counter_schema(&expected)?;
            let mut rows = 0usize;
            for batch in reader {
                let batch = batch?;
                rows = rows.checked_add(batch.num_rows()).ok_or_else(capacity)?;
                if rows > 1 {
                    return Err(counter_failure());
                }
                for column in batch.columns() {
                    if column.len() == 1 && (column.is_null(0) || !zero(column.as_ref())?) {
                        return Err(counter_failure());
                    }
                }
            }
            if rows != 1 || report["rows"] != 1 {
                return Err(counter_failure());
            }
            accepted_reports.push(ReportAcceptance {
                name: required.name.clone(),
                sql_sha256: required.sql_sha256.clone(),
                completion,
                schema_sha256: report["schema_sha256"]
                    .as_str()
                    .ok_or_else(mismatch)?
                    .into(),
                checks: expected
                    .fields()
                    .iter()
                    .map(|field| Check {
                        column: field.name().clone(),
                        status: "passed".into(),
                        value: 0,
                    })
                    .collect(),
            });
        }
        let body = Body {
            profile: COUNTERS.into(),
            dataset: dataset.clone(),
            operation: operation.clone(),
            request_digest: digest.into(),
            candidate: candidate.clone(),
            view_set_digest,
            policy_id: policy_id.clone(),
            policy_sha256: decision.policy.sha256.clone(),
            expected_epoch: decision.expected_epoch,
            expected_selection: decision.expected_selection.clone(),
            evaluator: policy.evaluator,
            semantics: policy.semantics,
            reports: accepted_reports,
        };
        let encoded = serde_json::to_vec(&body)?;
        if encoded.len() > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let active_key = format!("job-active/{operation}");
        let _memory = rows.reserve_decoded(budget, &["authority", &active_key, &key])?;
        if let Some((_, _, saved)) = rows.get_record::<Accepted>(&key)? {
            return validate_accepted(&saved, dataset, operation, digest, candidate, policy_id);
        }
        let authority: Authority = rows
            .get_record("authority")?
            .map_or_else(Authority::default, |(_, _, authority)| authority);
        producer::require_unbound(&authority)?;
        if rows
            .get_record::<Id>(&active_key)?
            .is_none_or(|(_, _, active)| active != *policy_id)
        {
            return Err(mismatch());
        }
        check_authority(
            &authority,
            &admitted.base_definition,
            decision.expected_epoch,
            &decision.expected_selection,
        )?;
        let accepted = Accepted {
            body,
            identity: blake3::hash(&encoded).to_hex().to_string(),
            body_sha256: sha256(&encoded),
            body_bytes_base64: STANDARD.encode(encoded),
            native_record: NativeRecord {
                table: view.identifier().to_string(),
                key: key.clone(),
                revision: view.revision + 1,
            },
            policy: decision.policy.clone(),
        };
        self.inner
            .lifecycle
            .commit(&view, vec![record(&key, &accepted)?])
            .await?;
        Ok(())
    }

    pub(super) async fn select_job(
        &self,
        dataset: &Id,
        operation: &Id,
        digest: &str,
        candidate: &JobCandidateRef,
        policy_id: &Id,
        identity: &str,
    ) -> Result<()> {
        let (admitted, _) = self
            .job_binding(dataset, operation, digest, candidate)
            .await?;
        let selected_key = format!("job-selected/{operation}");
        if let Some(saved) = self.job_record::<Selected>(dataset, &selected_key).await? {
            if saved.request_digest == digest
                && saved.candidate == *candidate
                && saved.policy_id == *policy_id
                && saved.accepted_qualification == identity
            {
                return Ok(());
            }
            return Err(mismatch());
        }
        let accepted_key = format!("job-accepted/{operation}/{policy_id}");
        let accepted = self
            .job_record::<Accepted>(dataset, &accepted_key)
            .await?
            .ok_or_else(|| {
                Error::refusal(
                    "quality_pending",
                    "exact native whole-job reports have not been accepted",
                )
            })?;
        validate_accepted(&accepted, dataset, operation, digest, candidate, policy_id)?;
        if accepted.identity != identity {
            return Err(mismatch());
        }
        let decision = self
            .job_record::<Decision>(dataset, &format!("job-policy/{operation}/{policy_id}"))
            .await?
            .ok_or_else(mismatch)?;
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let active_key = format!("job-active/{operation}");
        let _memory = rows.reserve_decoded(
            &self.inner.lifecycle.budget,
            &["authority", &active_key, &selected_key],
        )?;
        let mut authority: Authority = rows
            .get_record("authority")?
            .map_or_else(Authority::default, |(_, _, authority)| authority);
        producer::require_unbound(&authority)?;
        if rows
            .get_record::<Id>(&active_key)?
            .is_none_or(|(_, _, active)| active != *policy_id)
        {
            return Err(mismatch());
        }
        check_authority(
            &authority,
            &admitted.base_definition,
            decision.expected_epoch,
            &decision.expected_selection,
        )?;
        authority.job_selected = Some(candidate.clone());
        authority.definition = admitted.request.definition.clone();
        authority.selection_revision = Some(view.revision + 1);
        authority.boundary = authority.boundary.checked_add(1).ok_or_else(capacity)?;
        let selected = Selected {
            request_digest: digest.into(),
            candidate: candidate.clone(),
            policy_id: policy_id.clone(),
            accepted_qualification: identity.into(),
        };
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![
                    record("authority", authority)?,
                    record(selected_key, selected)?,
                ],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn requalify_job(
        &self,
        command: ManagedJobCommand,
    ) -> Result<ManagedJobReceipt> {
        let ManagedJobCommand::Requalify {
            ref dataset,
            ref operation,
            ref request_digest,
            ref candidate,
            ref previous_policy_id,
            ref policy_id,
            ref policy,
            expected_epoch,
            ref expected_selection,
        } = command
        else {
            return Err(mismatch());
        };
        let (admitted, _) = self
            .job_binding(dataset, operation, request_digest, candidate)
            .await?;
        if policy_id == previous_policy_id {
            return Err(mismatch());
        }
        let action_digest = json_identity(&command)?;
        let key = format!("job-policy/{operation}/{policy_id}");
        if let Some(saved) = self.job_record::<Decision>(dataset, &key).await? {
            if saved.action_digest != action_digest {
                return Err(mismatch());
            }
            return self.job_status(dataset, operation, Some(policy_id)).await;
        }
        if self
            .job_record::<Selected>(dataset, &format!("job-selected/{operation}"))
            .await?
            .is_some()
        {
            return Err(Error::refusal(
                "requalification_after_selection",
                "new requalification after selection is outside this native profile",
            ));
        }
        let bytes = read_artifact(policy, &self.inner.lifecycle.budget)?;
        let resolved: CounterPolicy = serde_json::from_slice(&bytes.value)?;
        let job_bytes = witness_bytes(&admitted.job, &self.inner.lifecycle.budget)?;
        let job: JobSpec = serde_json::from_slice(&job_bytes.value)?;
        resolved.validate(policy_id, &admitted.request.job.sha256, &job)?;
        let witness = planned_witness(
            dataset,
            &admitted.directory,
            &format!("policy-{policy_id}.json"),
            &bytes.value,
        );
        let decision = Decision {
            policy_id: policy_id.clone(),
            policy: witness,
            expected_epoch,
            expected_selection: expected_selection.clone(),
            action_digest,
        };
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.inner.lifecycle.open(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let active_key = format!("job-active/{operation}");
        let _memory = rows.reserve_decoded(
            &self.inner.lifecycle.budget,
            &["authority", &active_key, &key],
        )?;
        if rows
            .get_record::<Id>(&active_key)?
            .is_none_or(|(_, _, active)| active != *previous_policy_id)
            || rows.contains_key(&key)
        {
            return Err(mismatch());
        }
        let authority: Authority = rows
            .get_record("authority")?
            .map_or_else(Authority::default, |(_, _, authority)| authority);
        producer::require_unbound(&authority)?;
        check_authority(
            &authority,
            &admitted.base_definition,
            expected_epoch,
            expected_selection,
        )?;
        self.inner
            .lifecycle
            .commit(
                &view,
                vec![record(&key, decision)?, record(active_key, policy_id)?],
            )
            .await?;
        drop(_guard);
        self.job_status(dataset, operation, Some(policy_id)).await
    }
}

fn validate_accepted(
    accepted: &Accepted,
    dataset: &Id,
    operation: &Id,
    digest: &str,
    candidate: &JobCandidateRef,
    policy_id: &Id,
) -> Result<()> {
    let bytes = STANDARD
        .decode(&accepted.body_bytes_base64)
        .map_err(|_| mismatch())?;
    if accepted.body.dataset != *dataset
        || accepted.body.operation != *operation
        || accepted.body.request_digest != digest
        || accepted.body.candidate != *candidate
        || accepted.body.policy_id != *policy_id
        || blake3::hash(&bytes).to_hex().as_str() != accepted.identity
        || sha256(&bytes) != accepted.body_sha256
        || serde_json::to_vec(&accepted.body)? != bytes
        || accepted.policy.sha256 != accepted.body.policy_sha256
    {
        return Err(mismatch());
    }
    Ok(())
}

pub(super) fn validate_retained_acceptance(accepted: &Accepted) -> Result<()> {
    validate_accepted(
        accepted,
        &accepted.body.dataset,
        &accepted.body.operation,
        &accepted.body.request_digest,
        &accepted.body.candidate,
        &accepted.body.policy_id,
    )
}

fn validate_counter_schema(schema: &Schema) -> Result<()> {
    let mut names = BTreeSet::new();
    if schema.fields().is_empty()
        || schema.fields().iter().any(|field| {
            !names.insert(field.name())
                || !matches!(
                    field.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
        })
    {
        return Err(Error::refusal(
            "job_report_schema",
            "required report fields must be distinct native integer counters",
        ));
    }
    Ok(())
}

fn zero(column: &dyn Array) -> Result<bool> {
    macro_rules! check {
        ($kind:ident) => {
            column
                .as_any()
                .downcast_ref::<$kind>()
                .ok_or_else(mismatch)?
                .value(0)
                == 0
        };
    }
    Ok(match column.data_type() {
        DataType::Int8 => check!(Int8Array),
        DataType::Int16 => check!(Int16Array),
        DataType::Int32 => check!(Int32Array),
        DataType::Int64 => check!(Int64Array),
        DataType::UInt8 => check!(UInt8Array),
        DataType::UInt16 => check!(UInt16Array),
        DataType::UInt32 => check!(UInt32Array),
        DataType::UInt64 => check!(UInt64Array),
        _ => return Err(counter_failure()),
    })
}
fn counter_failure() -> Error {
    Error::refusal(
        "job_report_failed",
        "every declared counter requires one complete non-null integer zero row",
    )
}
