//! Whole-job records participate in the existing native retention scopes and inventory.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::catalog::SnapshotRows;

pub(in crate::engine) fn record_roots(
    key: &str,
    json: &str,
    table: &str,
    budget: &Budget,
) -> Result<Option<Vec<ObjectRef>>> {
    if !key.starts_with("job-") {
        return Ok(None);
    }
    let _memory = budget.reserve(json.len().saturating_mul(32).saturating_add(4096))?;
    let mut objects = Vec::new();
    if let Some(operation) = key.strip_prefix("job-operation/") {
        let admitted: Admission = serde_json::from_str(json)?;
        if admitted.request.operation.as_str() != operation
            || json_identity(&admitted.request)? != admitted.request_digest
            || crate::catalog::lifecycle_ident(&admitted.request.dataset).to_string() != table
        {
            return Err(mismatch());
        }
        for witness in [
            &admitted.admitted_request,
            &admitted.job,
            &admitted.initial_policy,
        ] {
            let _verified = witness_bytes(witness, budget)?;
            objects.push(witness.reference.clone());
        }
        let bytes = witness_bytes(&admitted.admitted_request, budget)?;
        let original: ManagedJobCommand = serde_json::from_slice(&bytes.value)?;
        let ManagedJobCommand::Prepare(original) = original else {
            return Err(mismatch());
        };
        if json_identity(&original)? != admitted.request_digest {
            return Err(mismatch());
        }
        for (name, input) in &admitted.input_grants {
            if &input.name != name
                || input.reuse_from.as_ref()
                    != admitted
                        .request
                        .snapshot_inputs
                        .iter()
                        .find(|binding| &binding.name == name)
                        .and_then(|binding| binding.reuse_from.as_ref())
            {
                return Err(mismatch());
            }
            objects.extend(input_custody::input_objects(input));
        }
    } else if key.starts_with("job-snapshot-member/") {
        let member: snapshots::SnapshotMember = serde_json::from_str(json)?;
        if key
            != format!(
                "job-snapshot-member/{}/{}",
                member.operation,
                sha256(member.object.uri.as_bytes())
            )
            || member.object.size != member.object.retained.bytes
            || !valid_sha256(&member.object.sha256)
        {
            return Err(mismatch());
        }
        objects.push(member.object.retained);
    } else if key.starts_with("job-snapshot-plan/") {
        let plan: snapshots::SnapshotPlan = serde_json::from_str(json)?;
        if key
            != format!(
                "job-snapshot-plan/{}/{}",
                plan.operation,
                sha256(plan.selector.name().as_bytes())
            )
        {
            return Err(mismatch());
        }
        objects.extend(plan.metadata.into_iter().map(|object| object.retained));
    } else if key.starts_with("job-member/") {
        let member: execution::MemberCompletion = serde_json::from_str(json)?;
        if !key.starts_with(&format!("job-member/{}/", member.operation))
            || member.member.size != member.member.retained.bytes
            || !valid_sha256(&member.member.sha256)
        {
            return Err(mismatch());
        }
        objects.push(member.member.retained);
    } else if key.starts_with("job-input/") || key.starts_with("job-input-material/") {
        let input: execution::InputAdmission = serde_json::from_str(json)?;
        if let Some(custody) = &input.snapshot_custody {
            custody.validate(&input.selector)?;
            if !input.files.is_empty() {
                return Err(mismatch());
            }
            objects.extend(custody.objects.iter().map(|object| object.retained.clone()));
        }
        for origin in &input.provenance {
            objects.extend(origin.objects().cloned());
        }
        for file in input.files {
            if file.size != file.retained.bytes || !valid_sha256(&file.sha256) {
                return Err(mismatch());
            }
            objects.push(file.retained);
        }
    } else if key.starts_with("job-phase/") || key.starts_with("job-material/") {
        let phase: Phase = serde_json::from_str(json)?;
        if key.starts_with("job-material/")
            && key != format!("job-material/{}", phase.material_identity)
        {
            return Err(mismatch());
        }
        if let Some(object) = phase.object {
            objects.push(object);
        }
    } else if let Some(operation) = key.strip_prefix("job-result/") {
        let completed: execution::Completion = serde_json::from_str(json)?;
        if completed.candidate.operation.as_str() != operation
            || completed.candidate.manifest != completed.manifest.reference
            || completed.candidate.scheme != "urn:docspec:whole-job-candidate"
            || completed.candidate.scheme_version != "1"
        {
            return Err(mismatch());
        }
        let bytes = witness_bytes(&completed.manifest, budget)?;
        let document: serde_json::Value = serde_json::from_slice(&bytes.value)?;
        fields(
            &document,
            &[
                "profile",
                "dataset",
                "operation",
                "request_digest",
                "job",
                "admitted_request",
                "inputs",
                "steps",
                "outputs",
                "reports",
                "view_set",
                "view_set_digest",
                "schemas",
                "evaluator",
                "runtime",
                "execution",
                "result",
            ],
        )?;
        if !document["profile"].as_str().is_some_and(known_job_profile)
            || document["execution"] != "completed"
            || document["operation"] != operation
            || document["dataset"] != completed.candidate.dataset.as_str()
        {
            return Err(mismatch());
        }
        objects.push(completed.manifest.reference);
        let view: JobArtifactWitness = serde_json::from_value(document["view_set"].clone())?;
        let _verified = witness_bytes(&view, budget)?;
        if document["view_set_digest"] != view.reference.digest {
            return Err(mismatch());
        }
        objects.push(view.reference);
        let original: JobArtifactWitness =
            serde_json::from_value(document["admitted_request"].clone())?;
        let request_bytes = witness_bytes(&original, budget)?;
        let ManagedJobCommand::Prepare(request) = serde_json::from_slice(&request_bytes.value)?
        else {
            return Err(mismatch());
        };
        if document["profile"] != completion_profile(&request) {
            return Err(mismatch());
        }
        objects.push(original.reference);
        let mut admitted_inputs_read = BTreeSet::new();
        for input in document["inputs"].as_array().ok_or_else(mismatch)? {
            let snapshot = input.get("snapshot_custody");
            let mut expected = vec![
                "name",
                "kind",
                "selector",
                "files",
                "schema_sha256",
                "admission",
            ];
            if snapshot.is_some() {
                expected.push("snapshot_custody");
            }
            if input.get("custody_reference").is_some() {
                expected.push("custody_reference");
            }
            if input.get("custody_grant").is_some() {
                expected.push("custody_grant");
            }
            fields(input, &expected)?;
            if let Some(snapshot) = snapshot {
                let custody: snapshots::SnapshotCustody = serde_json::from_value(snapshot.clone())?;
                let selector: JobInput = serde_json::from_value(input["selector"].clone())?;
                custody.validate(&selector)?;
                if request
                    .snapshot_inputs
                    .iter()
                    .find(|binding| binding.name == selector.name())
                    .is_none_or(|binding| {
                        binding.metadata_sha256 != custody.metadata_sha256
                            || binding.metadata_size != custody.metadata_size
                    })
                {
                    return Err(mismatch());
                }
                objects.extend(custody.objects.into_iter().map(|object| object.retained));
            } else if input["kind"] == "native_table" {
                return Err(mismatch());
            }
            let admission: ObjectRef = serde_json::from_value(input["admission"].clone())?;
            if input.get("custody_reference").is_some()
                && admitted_inputs_read.insert(admission.path.clone())
            {
                // The immutable admission document retains provenance after the
                // receiving operation and phase rows have independently retired.
                let retained = execution::read_retained(&admission, budget)?;
                let document: RetainedInputs = serde_json::from_slice(&retained.value)?;
                if document.profile != "urn:docspec:whole-job-input-admission:2"
                    || document.dataset != request.dataset
                    || document.operation != request.operation
                    || document.request_digest != json_identity(&request)?
                {
                    return Err(mismatch());
                }
                for saved in &document.inputs {
                    objects.extend(input_custody::input_objects(saved));
                }
            }
            if let Some(source) = request
                .snapshot_inputs
                .iter()
                .find(|binding| input["name"] == binding.name)
                .and_then(|binding| binding.reuse_from.as_ref())
            {
                if input["custody_grant"]["profile"]
                    != "urn:docspec:whole-job-input-custody-grant:1"
                    || input["custody_grant"]["source"] != serde_json::to_value(source)?
                    || input.get("custody_reference").is_none()
                {
                    return Err(mismatch());
                }
            } else if input.get("custody_grant").is_some() {
                return Err(mismatch());
            }
            objects.push(admission);
            for member in input["files"].as_array().ok_or_else(mismatch)? {
                objects.push(serde_json::from_value(member["retained"].clone())?);
            }
        }
        for family in ["steps", "outputs", "reports"] {
            for value in document[family].as_array().ok_or_else(mismatch)? {
                match family {
                    "steps" => fields(
                        value,
                        &[
                            "name",
                            "material_identity",
                            "schema_sha256",
                            "completion",
                            "dependencies",
                        ],
                    )?,
                    "outputs" => fields(
                        value,
                        &[
                            "name",
                            "path",
                            "sha256",
                            "md5",
                            "size",
                            "rows",
                            "footer_bytes",
                            "schema_sha256",
                            "settings",
                            "completion",
                            "dependencies",
                        ],
                    )?,
                    "reports" => fields(
                        value,
                        &[
                            "name",
                            "sql_sha256",
                            "schema_sha256",
                            "rows",
                            "data",
                            "completion",
                            "dependencies",
                        ],
                    )?,
                    _ => return Err(mismatch()),
                }
                if !value["completion"].is_null() {
                    objects.push(serde_json::from_value(value["completion"].clone())?);
                }
            }
        }
    } else if key.starts_with("job-policy/") {
        let decision: Decision = serde_json::from_str(json)?;
        let _verified = witness_bytes(&decision.policy, budget)?;
        // Accepted/decision audit bytes do not grant payload lifetime.
    } else if key.starts_with("job-active/") {
        let _: Id = serde_json::from_str(json)?;
    } else if key.starts_with("job-attempt/") {
        let attempt: Attempt = serde_json::from_str(json)?;
        if !matches!(attempt.status.as_str(), "started" | "failed") {
            return Err(mismatch());
        }
    } else if key.starts_with("job-accepted/") {
        let accepted: qualification::Accepted = serde_json::from_str(json)?;
        qualification::validate_retained_acceptance(&accepted)?;
    } else if key.starts_with("job-selected/") {
        let _: qualification::Selected = serde_json::from_str(json)?;
    } else if !producer::retention_record(key, json, table, budget)? {
        return Err(mismatch());
    }
    for object in &objects {
        super::super::retention::inventory::validate_object(object)?;
        if crate::catalog::lifecycle_ident(&object.owner).to_string() != table {
            return Err(mismatch());
        }
    }
    Ok(Some(objects))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedInputs {
    profile: String,
    dataset: Id,
    operation: Id,
    request_digest: String,
    inputs: Vec<execution::InputAdmission>,
}

fn fields(value: &serde_json::Value, expected: &[&str]) -> Result<()> {
    let object = value.as_object().ok_or_else(mismatch)?;
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(mismatch());
    }
    Ok(())
}

pub(in crate::engine) fn retirement_records(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    availability: bool,
    budget: &Budget,
) -> Result<Option<Vec<RetainedGeneration>>> {
    let operation_key = format!("job-operation/{operation}");
    if !rows.contains_key(&operation_key) {
        return Ok(None);
    }
    let result_key = format!("job-result/{operation}");
    let _memory = rows.reserve_decoded(budget, &[&operation_key, &result_key, "authority"])?;
    let (_, _, admitted): (_, _, Admission) =
        rows.get_record(&operation_key)?.ok_or_else(mismatch)?;
    if admitted.request.operation != *operation
        || &crate::catalog::lifecycle_ident(&admitted.request.dataset) != view.identifier()
    {
        return Err(mismatch());
    }
    if !rows.contains_key(&result_key) {
        let abandoned_key = format!("abandoned/{operation}");
        let _memory = rows.reserve_decoded(budget, &[&abandoned_key])?;
        let (_, _, abandoned): (_, _, Abandonment) =
            rows.get_record(&abandoned_key)?.ok_or_else(|| {
                Error::refusal(
                    "operation_active",
                    "complete or abandon the whole-job operation before retirement",
                )
            })?;
        if abandoned.operation != *operation
            || abandoned.dataset != admitted.request.dataset
            || abandoned.request_digest != admitted.request_digest
        {
            return Err(mismatch());
        }
    }
    let authority: Authority = rows
        .get_record("authority")?
        .map_or_else(Authority::default, |(_, _, authority)| authority);
    if availability
        && authority
            .job_selected
            .as_ref()
            .is_some_and(|selected| selected.operation == *operation)
    {
        return Err(Error::refusal(
            "selected_result",
            "current whole-job selection remains available",
        ));
    }
    let keys = if availability {
        vec![result_key]
    } else {
        vec![operation_key, format!("protection/{}", admitted.protection)]
    };
    let records = keys
        .into_iter()
        .map(|key| {
            let revision = rows
                .revision(&key)
                .filter(|revision| *revision > 0 && *revision <= view.revision)
                .ok_or_else(mismatch)?;
            Ok(RetainedGeneration { key, revision })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(records))
}

impl Engine {
    pub(super) async fn check_job_recovery(&self, admitted: &Admission) -> Result<()> {
        let view = self.read_view(&admitted.request.dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let key = format!("job-operation/{}", admitted.request.operation);
        let revision = rows.revision(&key).ok_or_else(mismatch)?;
        if rows.contains_key(&format!("abandoned/{}", admitted.request.operation)) {
            return Err(Error::refusal(
                "operation_abandoned",
                "whole-job operation was explicitly abandoned",
            ));
        }
        if super::super::retention::retirement::record_retired(
            &rows,
            &view,
            &key,
            revision,
            &self.inner.lifecycle.budget,
        )? {
            return Err(Error::refusal(
                "recovery_retired",
                "whole-job operation recovery was explicitly retired",
            ));
        }
        self.check_operation_available(&view, &admitted.request.operation)
            .await
    }
}

pub(in crate::engine) fn abandonment(
    rows: &SnapshotRows,
    view: &View,
    operation: &Id,
    reason: &str,
    budget: &Budget,
) -> Result<Option<Abandonment>> {
    let key = format!("job-operation/{operation}");
    if !rows.contains_key(&key) {
        return Ok(None);
    }
    let _memory = rows.reserve_decoded(budget, &[&key, "authority"])?;
    let (revision, _, admitted): (_, _, Admission) = rows.get_record(&key)?.ok_or_else(mismatch)?;
    if &crate::catalog::lifecycle_ident(&admitted.request.dataset) != view.identifier() {
        return Err(mismatch());
    }
    if rows.contains_key(&format!("job-selected/{operation}")) {
        return Err(Error::refusal(
            "selected_result",
            "a selected whole-job operation cannot be abandoned",
        ));
    }
    Ok(Some(Abandonment {
        dataset: admitted.request.dataset,
        operation: operation.clone(),
        request_digest: admitted.request_digest,
        operation_revision: revision,
        reason: reason.into(),
    }))
}

pub(in crate::engine) fn validate_selection(
    rows: &SnapshotRows,
    authority: &Authority,
    revision: i64,
    table: &str,
    budget: &Budget,
) -> Result<bool> {
    if authority.job_profile.is_none() && authority.job_selected.is_none() {
        return Ok(false);
    }
    if !authority
        .job_profile
        .as_deref()
        .is_some_and(known_job_profile)
        || authority.selected.is_some()
        || authority.release.is_some()
    {
        return Err(mismatch());
    }
    let Some(selected) = &authority.job_selected else {
        if authority.selection_revision.is_some() {
            return Err(mismatch());
        }
        return Ok(true);
    };
    if crate::catalog::lifecycle_ident(&selected.dataset).to_string() != table
        || authority
            .selection_revision
            .is_none_or(|selected_revision| selected_revision <= 0 || selected_revision > revision)
    {
        return Err(mismatch());
    }
    let key = format!("job-selected/{}", selected.operation);
    let result_key = format!("job-result/{}", selected.operation);
    let _memory = rows.reserve_decoded(budget, &[&key, &result_key])?;
    let (selected_revision, _, receipt): (_, _, qualification::Selected) =
        rows.get_record(&key)?.ok_or_else(mismatch)?;
    let (_, _, completed): (_, _, execution::Completion) =
        rows.get_record(&result_key)?.ok_or_else(mismatch)?;
    if Some(selected_revision) != authority.selection_revision
        || receipt.candidate != *selected
        || completed.candidate != *selected
        || rows.contains_key(&format!("availability-retired/{}", selected.operation))
    {
        return Err(mismatch());
    }
    Ok(true)
}
