//! Reconciles native completions before reopening inputs or executing graph rows.
// Rust guideline compliant 2026-02-21
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileMember {
    key: Option<String>,
    path: PathBuf,
    pub(super) sha256: String,
    pub(super) size: u64,
    pub(super) retained: ObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InputAdmission {
    pub(super) name: String,
    pub(super) selector: JobInput,
    pub(super) files: Vec<FileMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) snapshot_custody: Option<snapshots::SnapshotCustody>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) provenance: Vec<input_custody::InputOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) reuse_from: Option<JobInputCustodyRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MemberCompletion {
    pub(super) operation: Id,
    request_digest: String,
    input: String,
    member_index: usize,
    pub(super) member: FileMember,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    pub candidate: JobCandidateRef,
    pub manifest: JobArtifactWitness,
}

#[derive(Serialize)]
struct ViewDocument<'a> {
    profile: &'static str,
    dataset: &'a Id,
    operation: &'a Id,
    request_digest: &'a str,
    inputs: &'a [serde_json::Value],
    steps: &'a [serde_json::Value],
    outputs: &'a [serde_json::Value],
    reports: &'a [serde_json::Value],
    #[serde(skip_serializing_if = "Option::is_none")]
    producer_evidence: Option<&'a producer::ProducerEvidence>,
}
#[derive(Serialize)]
struct JobIdentity<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    path: &'a Path,
    sha256: &'a str,
    size: u64,
}
#[derive(Serialize)]
struct CompletionDocument<'a> {
    profile: &'static str,
    dataset: &'a Id,
    operation: &'a Id,
    request_digest: &'a str,
    job: JobIdentity<'a>,
    admitted_request: &'a JobArtifactWitness,
    inputs: &'a [serde_json::Value],
    steps: &'a [serde_json::Value],
    outputs: &'a [serde_json::Value],
    reports: &'a [serde_json::Value],
    view_set: &'a JobArtifactWitness,
    view_set_digest: &'a str,
    schemas: &'a BTreeMap<String, String>,
    evaluator: serde_json::Value,
    runtime: &'a PhaseRuntime,
    execution: &'static str,
    result: &'a JobResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    producer_evidence: Option<&'a producer::ProducerEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    producer_evidence_witness: Option<&'a JobArtifactWitness>,
}

impl Engine {
    pub(super) async fn resume_job(&self, admitted: &Admission) -> Result<()> {
        let request = &admitted.request;
        self.check_job_recovery(admitted).await?;
        let result_key = format!("job-result/{}", request.operation);
        if self
            .job_record::<Completion>(&request.dataset, &result_key)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let host_budget = &self.inner.lifecycle.budget;
        self.check_job_runtime(admitted).await?;
        let initial = owned_bytes(&admitted.directory, host_budget.limits.max_metadata_bytes)?
            .checked_add(input_custody::inherited_bytes(admitted)?)
            .ok_or_else(capacity)?;
        let budget = host_budget.with_retained_disk(initial)?;
        *self.inner.retained_disk.lock().await = Arc::downgrade(&budget.runtime_env().disk_manager);
        std::fs::create_dir_all(&admitted.directory)?;
        for (name, witness) in [
            ("request.json", &admitted.admitted_request),
            ("job.json", &admitted.job),
            ("policy-initial.json", &admitted.initial_policy),
        ] {
            let bytes = witness_bytes(witness, &budget)?;
            self.retain_job_bytes(
                &request.dataset,
                &admitted.directory,
                name,
                bytes.value.clone(),
                &budget,
            )
            .await?;
        }
        let job_bytes = witness_bytes(&admitted.job, &budget)?;
        let spec: JobSpec = serde_json::from_slice(&job_bytes.value)?;
        spec.validate()?;
        let mut inputs = Vec::new();
        let mut schemas = BTreeMap::new();
        let mut schema_memory = Vec::new();
        let mut identities = BTreeMap::new();
        let mut result = JobResult {
            job_id: spec.id.clone(),
            job_spec_sha256: admitted.job.sha256.clone(),
            native_inputs: Vec::new(),
            native_versions: Vec::new(),
            table_operations: Vec::new(),
            #[cfg(feature = "substrait")]
            saved_plans: Vec::new(),
            inputs: Vec::new(),
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
        };
        // Establish custody of every raw member before registering any executable relation.
        let mut admissions = Vec::new();
        let mut admission_memory = Vec::new();
        let mut input_view_memory = Vec::new();
        let reused_donor_bytes = self.plan_job_snapshots(admitted, &spec, &budget).await?;
        let budget = if reused_donor_bytes > 0 {
            let retained = owned_bytes(&admitted.directory, host_budget.limits.max_metadata_bytes)?
                .checked_add(input_custody::inherited_bytes(admitted)?)
                .and_then(|sum| sum.checked_add(reused_donor_bytes))
                .ok_or_else(capacity)?;
            let next = host_budget.with_retained_disk(retained).map_err(|error| {
                if error.code() == "disk_capacity" {
                    Error::refusal(
                        "disk_capacity",
                        "complete admitted native and file inputs exceed the operation allowance",
                    )
                } else {
                    error
                }
            })?;
            *self.inner.retained_disk.lock().await =
                Arc::downgrade(&next.runtime_env().disk_manager);
            next
        } else {
            budget
        };
        let producer_evidence = self
            .seal_job_producer_evidence(admitted, &spec, &budget)
            .await?;
        let job = Job::from_budget(budget.clone())?;
        for (index, input) in spec.inputs.iter().enumerate() {
            let key = format!("job-input/{}/{index}", request.operation);
            if let Some(saved) = self
                .job_record::<InputAdmission>(&request.dataset, &key)
                .await?
            {
                if serde_json::to_value(&saved.selector)? != serde_json::to_value(input)? {
                    return Err(mismatch());
                }
                retain_input_admission(
                    &budget,
                    &mut admissions,
                    &mut admission_memory,
                    saved.value,
                )?;
                continue;
            }
            if matches!(input, JobInput::NativeTable { .. }) {
                if let Some(inherited) = admitted.input_grants.get(input.name()) {
                    let memory = self.request_memory(inherited)?;
                    let inherited = inherited.clone();
                    self.commit_job_record(&request.dataset, &key, &inherited)
                        .await?;
                    self.job_phase_sealed(&key).await?;
                    retain_input_admission(
                        &budget,
                        &mut admissions,
                        &mut admission_memory,
                        inherited,
                    )?;
                    drop(memory);
                    continue;
                }
                let snapshot = self.admit_job_snapshot(admitted, input, &budget).await?;
                let donor = admitted.input_donors.get(input.name());
                let mut provenance = vec![input_custody::InputOrigin::from_admission(admitted)];
                if let Some(donor) = donor {
                    for origin in &donor.provenance {
                        if origin.operation != admitted.request.operation
                            && snapshot
                                .objects
                                .iter()
                                .any(|object| object.origin_operation == origin.operation)
                        {
                            provenance.push(origin.clone());
                        }
                    }
                }
                let saved = InputAdmission {
                    name: input.name().into(),
                    selector: input.clone(),
                    files: Vec::new(),
                    snapshot_custody: Some(snapshot.value),
                    provenance,
                    reuse_from: donor.and_then(|donor| donor.reuse_from.clone()),
                };
                self.commit_job_record(&request.dataset, &key, &saved)
                    .await?;
                self.job_phase_sealed(&key).await?;
                retain_input_admission(&budget, &mut admissions, &mut admission_memory, saved)?;
                continue;
            }
            let binding = request
                .file_inputs
                .iter()
                .find(|binding| binding.name == input.name())
                .ok_or_else(mismatch)?;
            let material_key = format!("job-input-material/{}", input_identity(input, binding)?);
            if let Some(saved) = self
                .reusable_job_record::<InputAdmission>(&request.dataset, &material_key)
                .await?
            {
                let mut saved = saved.value;
                saved.selector = input.clone();
                let paths = input_paths(input)?;
                for (file, (_, path)) in saved.files.iter_mut().zip(paths) {
                    file.path = path;
                }
                self.commit_job_record(&request.dataset, &key, &saved)
                    .await?;
                retain_input_admission(&budget, &mut admissions, &mut admission_memory, saved)?;
                continue;
            }
            let original = input_paths(input)?;
            let mut files = Vec::new();
            for (member_index, ((key, path), declared)) in
                original.into_iter().zip(&binding.members).enumerate()
            {
                let member_key = format!("job-member/{}/{index}/{member_index}", request.operation);
                if let Some(saved) = self
                    .job_record::<MemberCompletion>(&request.dataset, &member_key)
                    .await?
                {
                    if saved.operation != request.operation
                        || saved.request_digest != admitted.request_digest
                        || saved.input != input.name()
                        || saved.member_index != member_index
                        || saved.member.key != key
                        || saved.member.path != path
                        || saved.member.sha256 != declared.sha256
                        || saved.member.size != declared.size
                    {
                        return Err(mismatch());
                    }
                    files.push(saved.value.member);
                    continue;
                }
                let extension = if matches!(input, JobInput::Parquet { .. }) {
                    ".parquet"
                } else {
                    ""
                };
                let retained_path = admitted.directory.join(format!(
                    "input-{index}-{member_index}-{}{extension}",
                    declared.sha256
                ));
                let retained =
                    copy_input(&request.dataset, &path, &retained_path, declared, &budget).await?;
                let member = FileMember {
                    key,
                    path,
                    sha256: declared.sha256.clone(),
                    size: declared.size,
                    retained,
                };
                self.commit_job_record(
                    &request.dataset,
                    &member_key,
                    &MemberCompletion {
                        operation: request.operation.clone(),
                        request_digest: admitted.request_digest.clone(),
                        input: input.name().into(),
                        member_index,
                        member: member.clone(),
                    },
                )
                .await?;
                files.push(member);
            }
            let input = InputAdmission {
                name: input.name().into(),
                selector: input.clone(),
                files,
                snapshot_custody: None,
                provenance: Vec::new(),
                reuse_from: None,
            };
            self.commit_job_record(&request.dataset, &key, &input)
                .await?;
            self.commit_job_record(&request.dataset, &material_key, &input)
                .await?;
            retain_input_admission(&budget, &mut admissions, &mut admission_memory, input)?;
        }
        let admission_bytes = serde_json::to_vec(
            &serde_json::json!({"profile":if request.snapshot_inputs.is_empty() {"urn:docspec:whole-job-input-admission:1"} else {"urn:docspec:whole-job-input-admission:2"},"dataset":request.dataset,"operation":request.operation,"request_digest":admitted.request_digest,"inputs":admissions}),
        )?;
        let admission = self
            .retain_job_bytes(
                &request.dataset,
                &admitted.directory,
                "inputs.json",
                admission_bytes,
                &budget,
            )
            .await?;
        for (index, (input, saved)) in spec.inputs.iter().zip(&admissions).enumerate() {
            match input {
                JobInput::Parquet { name, .. } => {
                    job.register_parquet(name, Path::new(&saved.files[0].retained.path))
                        .await?
                }
                JobInput::JsonLines { name, schema, .. } => job.register_json_lines(
                    name,
                    saved
                        .files
                        .iter()
                        .map(|file| crate::job::JsonLinesFile {
                            key: file.key.clone().expect("validated JSON Lines key"),
                            path: file.retained.path.clone().into(),
                        })
                        .collect(),
                    Arc::new(schema.clone()),
                )?,
                JobInput::NativeTable { name, .. } => {
                    let custody = saved.snapshot_custody.as_ref().ok_or_else(mismatch)?;
                    custody.validate(input)?;
                    let mut memory = custody.memory(&budget)?;
                    for object in custody.objects.iter().filter(|object| {
                        matches!(
                            object.role.as_str(),
                            "data" | "position_deletes" | "equality_deletes"
                        )
                    }) {
                        memory.merge(
                            crate::storage::admit_parquet_footer(
                                Path::new(&object.retained.path),
                                &budget,
                            )
                            .await?,
                        );
                    }
                    job.register_retained_snapshot(
                        name,
                        &snapshots::selection(input)?,
                        custody.paths(),
                        memory,
                    )
                    .await?;
                    result
                        .native_inputs
                        .push(job.native_input_observation(name).await?);
                }
                _ => return Err(mismatch()),
            }
            result.inputs.push(observed_managed_input(input, saved)?);
            let schema = job.input_schema(input.name()).await?;
            let bytes = crate::job::retained::encode_schema(&schema, &budget)?;
            let schema_sha256 = crate::job::retained::retain_schema_into(
                &budget,
                &mut schemas,
                &mut schema_memory,
                &bytes.bytes,
            )?;
            producer::check_input_schema(admitted, input, &schema_sha256)?;
            let material = if let Some(custody) = &saved.snapshot_custody {
                input_custody::material_identity(input, custody)?
            } else {
                let binding = request
                    .file_inputs
                    .iter()
                    .find(|binding| binding.name == input.name())
                    .ok_or_else(mismatch)?;
                input_identity(input, binding)?
            };
            let identity = json_identity(&(material, &schema))?;
            identities.insert(relation_key(false, input.name())?, identity);
            let kind = match input {
                JobInput::Parquet { .. } => "parquet",
                JobInput::JsonLines { .. } => "json_lines",
                JobInput::NativeTable { .. } => "native_table",
                _ => return Err(mismatch()),
            };
            let mut view = serde_json::json!({"name":input.name(),"kind":kind,"selector":input,"files":saved.files,"schema_sha256":schema_sha256,"admission":admission.reference});
            if let Some(custody) = &saved.snapshot_custody {
                view["snapshot_custody"] = serde_json::to_value(custody)?;
                view["custody_reference"] = serde_json::to_value(
                    self.current_input_reference(admitted, index, saved).await?,
                )?;
                if let Some(source) = &saved.reuse_from {
                    view["custody_grant"] = serde_json::json!({"profile":"urn:docspec:whole-job-input-custody-grant:1","source":source});
                }
            }
            input_view_memory.push(self.request_memory(&view)?);
            inputs.push(view);
        }
        // Native providers/FileIO and the distinct input views now own their
        // own admission. The original transfer descriptions have no consumers.
        drop(admission);
        drop(admissions);
        drop(admission_memory);
        let mut steps = Vec::new();
        for (index, step) in spec.steps.iter().enumerate() {
            let key = format!("job-phase/{}/step/{index}", request.operation);
            let sql = match step {
                JobStep::Sql(step) => Some(step.sql.as_str()),
                JobStep::Components(step) => Some(step.components.sql.as_str()),
                #[cfg(feature = "substrait")]
                JobStep::Substrait(_) => None,
            };
            let empty = BTreeMap::new();
            let parameters = match step {
                JobStep::Sql(step) => &step.parameters,
                _ => &empty,
            };
            let dependencies = if let Some(sql) = sql {
                dependency_identities(&job, sql, parameters, &identities).await?
            } else {
                identities
                    .iter()
                    .map(|(name, value)| format!("{name}:{value}"))
                    .collect()
            };
            let material_definition = match step {
                JobStep::Sql(step) => {
                    serde_json::json!({"kind":"sql","sql":step.sql,"parameters":step.parameters,"materialize":step.materialize})
                }
                JobStep::Components(step) => {
                    serde_json::json!({"kind":"components","settings":step.components})
                }
                #[cfg(feature = "substrait")]
                JobStep::Substrait(step) => {
                    serde_json::json!({"kind":"substrait","plan":step.substrait})
                }
            };
            let identity = json_identity(&(
                step.name(),
                material_definition,
                &dependencies,
                &admitted.runtime,
            ))?;
            let material_key = format!("job-material/{identity}");
            let materialized = matches!(step, JobStep::Components(_))
                || matches!(step, JobStep::Sql(step) if step.materialize);
            let phase = if materialized {
                if let Some(saved) = self.job_record::<Phase>(&request.dataset, &key).await? {
                    validate_phase(&saved, admitted, step.name(), &identity)?;
                    saved.value
                } else if let Some(saved) = self
                    .reusable_job_record::<Phase>(&request.dataset, &material_key)
                    .await?
                {
                    validate_phase(&saved, admitted, step.name(), &identity)?;
                    self.commit_job_record(&request.dataset, &key, &saved.value)
                        .await?;
                    saved.value
                } else {
                    let path = admitted.directory.join(format!(
                        "step-{index}-{}.parquet",
                        uuid::Uuid::new_v4().simple()
                    ));
                    let file = match step {
                        JobStep::Sql(step) if step.parameters.is_empty() => {
                            job.retained_sql(&step.sql, &path).await?
                        }
                        JobStep::Components(step) => {
                            job.retained_components(&step.name, &step.components, &path)
                                .await?
                        }
                        _ => {
                            return Err(Error::refusal(
                                "unsupported_parameters",
                                "materialized SQL parameters are not supported",
                            ));
                        }
                    };
                    let schema = crate::job::retained::encode_schema(&file.schema, &budget)?;
                    let phase = Phase {
                        operation: request.operation.clone(),
                        request_digest: admitted.request_digest.clone(),
                        name: step.name().into(),
                        material_identity: identity.clone(),
                        schema_sha256: sha256(&schema.bytes),
                        schema_ipc_base64: STANDARD.encode(&schema.bytes),
                        object: Some(ObjectRef {
                            owner: request.dataset.clone(),
                            path: path.to_string_lossy().into_owned(),
                            digest: file.digest,
                            bytes: file.size,
                        }),
                        sha256: Some(file.sha256),
                        rows: file.rows,
                        file: None,
                        dependencies,
                        origin_runtime: admitted.runtime.clone(),
                    };
                    // Only this native catalog seal makes the expensive phase reusable.
                    self.commit_job_record(&request.dataset, &key, &phase)
                        .await?;
                    self.commit_job_record(&request.dataset, &material_key, &phase)
                        .await?;
                    self.job_phase_sealed(&key).await?;
                    phase
                }
            } else {
                let schema = job.retained_view(step).await?;
                let bytes = crate::job::retained::encode_schema(&schema, &budget)?;
                Phase {
                    operation: request.operation.clone(),
                    request_digest: admitted.request_digest.clone(),
                    name: step.name().into(),
                    material_identity: identity.clone(),
                    schema_sha256: sha256(&bytes.bytes),
                    schema_ipc_base64: STANDARD.encode(&bytes.bytes),
                    object: None,
                    sha256: None,
                    rows: 0,
                    file: None,
                    dependencies,
                    origin_runtime: admitted.runtime.clone(),
                }
            };
            if let Some(object) = &phase.object {
                job.reopen_retained(step.name(), Path::new(&object.path))
                    .await?;
            }
            retain_phase_schema(&phase, &budget, &mut schemas, &mut schema_memory)?;
            identities.insert(relation_key(false, step.name())?, identity);
            steps.push(serde_json::json!({"name":phase.name,"material_identity":phase.material_identity,"schema_sha256":phase.schema_sha256,"completion":phase.object,"dependencies":phase.dependencies}));
            result.steps.push(step.step_result(if materialized {
                StepStorage::Scratch
            } else {
                StepStorage::View
            })?);
            // The phase seal precedes all explicit release_after effects.
            for name in step.release_after() {
                job.release(name)?;
            }
        }
        let output_directory = admitted.directory.join("outputs");
        std::fs::create_dir_all(&output_directory)?;
        let mut outputs = Vec::new();
        for (index, output) in spec.outputs.iter().enumerate() {
            let key = format!("job-phase/{}/output/{index}", request.operation);
            let dependencies =
                dependency_identities(&job, &output.sql, &BTreeMap::new(), &identities).await?;
            let identity = json_identity(&(
                &output.name,
                &output.sql,
                &output.settings,
                &dependencies,
                &admitted.runtime,
            ))?;
            let material_key = format!("job-material/{identity}");
            let phase =
                if let Some(saved) = self.job_record::<Phase>(&request.dataset, &key).await? {
                    validate_phase(&saved, admitted, &output.name, &identity)?;
                    saved.value
                } else if let Some(saved) = self
                    .reusable_job_record::<Phase>(&request.dataset, &material_key)
                    .await?
                {
                    validate_phase(&saved, admitted, &output.name, &identity)?;
                    self.commit_job_record(&request.dataset, &key, &saved.value)
                        .await?;
                    saved.value
                } else {
                    let path = output_directory
                        .join(format!("{index}-{}.parquet", uuid::Uuid::new_v4().simple()));
                    let (file, digest, schema) = job
                        .retained_export(&output.sql, &path, &output.settings)
                        .await?;
                    let schema = crate::job::retained::encode_schema(&schema, &budget)?;
                    let phase = Phase {
                        operation: request.operation.clone(),
                        request_digest: admitted.request_digest.clone(),
                        name: output.name.clone(),
                        material_identity: identity,
                        schema_sha256: sha256(&schema.bytes),
                        schema_ipc_base64: STANDARD.encode(&schema.bytes),
                        object: Some(ObjectRef {
                            owner: request.dataset.clone(),
                            path: path.to_string_lossy().into_owned(),
                            digest,
                            bytes: file.size,
                        }),
                        sha256: Some(file.sha256.clone()),
                        rows: file.rows,
                        file: Some(file),
                        dependencies,
                        origin_runtime: admitted.runtime.clone(),
                    };
                    self.commit_job_record(&request.dataset, &key, &phase)
                        .await?;
                    self.commit_job_record(&request.dataset, &material_key, &phase)
                        .await?;
                    self.job_phase_sealed(&key).await?;
                    phase
                };
            retain_phase_schema(&phase, &budget, &mut schemas, &mut schema_memory)?;
            let file = phase.file.clone().ok_or_else(mismatch)?;
            let object = phase.object.as_ref().ok_or_else(mismatch)?;
            job.register_completed_output(&output.name, Path::new(&object.path))
                .await?;
            identities.insert(
                relation_key(true, &output.name)?,
                phase.material_identity.clone(),
            );
            result.outputs.push(OutputResult {
                name: output.name.clone(),
                file: file.clone(),
            });
            outputs.push(serde_json::json!({"name":output.name,"path":object.path,"sha256":file.sha256,"md5":file.md5,"size":file.size,"rows":file.rows,"footer_bytes":file.footer_bytes,"schema_sha256":phase.schema_sha256,"settings":output.settings,"completion":object,"dependencies":phase.dependencies}));
        }
        let mut reports = Vec::new();
        for (index, query) in spec.queries.iter().enumerate() {
            let key = format!("job-phase/{}/report/{index}", request.operation);
            let dependencies =
                dependency_identities(&job, &query.sql, &BTreeMap::new(), &identities).await?;
            let identity =
                json_identity(&(&query.name, &query.sql, &dependencies, &admitted.runtime))?;
            let material_key = format!("job-material/{identity}");
            let phase =
                if let Some(saved) = self.job_record::<Phase>(&request.dataset, &key).await? {
                    validate_phase(&saved, admitted, &query.name, &identity)?;
                    saved.value
                } else if let Some(saved) = self
                    .reusable_job_record::<Phase>(&request.dataset, &material_key)
                    .await?
                {
                    validate_phase(&saved, admitted, &query.name, &identity)?;
                    self.commit_job_record(&request.dataset, &key, &saved.value)
                        .await?;
                    saved.value
                } else {
                    let ipc = job.report_ipc(&query.name, &query.sql).await?;
                    let reader = StreamReader::try_new(Cursor::new(&ipc.bytes), None)?;
                    let schema = crate::job::retained::encode_schema(&reader.schema(), &budget)?;
                    let mut rows = 0u64;
                    for batch in reader {
                        rows = rows
                            .checked_add(batch?.num_rows() as u64)
                            .ok_or_else(capacity)?;
                    }
                    let witness = self
                        .retain_job_bytes(
                            &request.dataset,
                            &admitted.directory,
                            &format!("report-{index}-{}.arrow", uuid::Uuid::new_v4()),
                            ipc.bytes.clone(),
                            &budget,
                        )
                        .await?;
                    let phase = Phase {
                        operation: request.operation.clone(),
                        request_digest: admitted.request_digest.clone(),
                        name: query.name.clone(),
                        material_identity: identity,
                        schema_sha256: sha256(&schema.bytes),
                        schema_ipc_base64: STANDARD.encode(&schema.bytes),
                        object: Some(witness.reference),
                        sha256: Some(witness.sha256),
                        rows,
                        file: None,
                        dependencies,
                        origin_runtime: admitted.runtime.clone(),
                    };
                    #[cfg(test)]
                    self.job_phase_sealed(&format!("before:{key}")).await?;
                    self.commit_job_record(&request.dataset, &key, &phase)
                        .await?;
                    self.commit_job_record(&request.dataset, &material_key, &phase)
                        .await?;
                    self.job_phase_sealed(&key).await?;
                    phase
                };
            let object = phase.object.as_ref().ok_or_else(mismatch)?;
            let bytes = read_retained(object, &budget)?;
            let (rendered, _, rows) = job.report_result(&query.name, &bytes.value)?;
            if rows != phase.rows {
                return Err(mismatch());
            }
            retain_phase_schema(&phase, &budget, &mut schemas, &mut schema_memory)?;
            reports.push(serde_json::json!({"name":query.name,"sql_sha256":sha256(query.sql.as_bytes()),"schema_sha256":phase.schema_sha256,"rows":phase.rows,"data":rendered.data,"completion":object,"dependencies":phase.dependencies}));
            result.queries.push(rendered);
        }
        let _view_memory = self.request_memory(&reports)?;
        // All report and export consumers have finished. Their sealed bytes,
        // schema dictionary and report representations keep their own admission;
        // native scan providers and optimizer values no longer need to overlap
        // completion assembly and the lifecycle writer.
        drop(job);
        let view_reports = reports.iter().map(|report| serde_json::json!({"name":report["name"],"sql_sha256":report["sql_sha256"],"schema_sha256":report["schema_sha256"],"rows":report["rows"],"completion":report["completion"],"dependencies":report["dependencies"]})).collect::<Vec<_>>();
        let view_document = ViewDocument {
            profile: match completion_profile(request) {
                PRODUCER_PROFILE => "urn:docspec:whole-job-view-set:3",
                SNAPSHOT_PROFILE => "urn:docspec:whole-job-view-set:2",
                _ => "urn:docspec:whole-job-view-set:1",
            },
            dataset: &request.dataset,
            operation: &request.operation,
            request_digest: &admitted.request_digest,
            inputs: &inputs,
            steps: &steps,
            outputs: &outputs,
            reports: &view_reports,
            producer_evidence: producer_evidence.as_ref().map(|evidence| &evidence.0),
        };
        let view_bytes = encode_metadata(&view_document, &budget)?;
        let view_set = self
            .retain_job_bytes(
                &request.dataset,
                &admitted.directory,
                "view-set.json",
                view_bytes.value.clone(),
                &budget,
            )
            .await?;
        let manifest_document = CompletionDocument {
            profile: completion_profile(request),
            dataset: &request.dataset,
            operation: &request.operation,
            request_digest: &admitted.request_digest,
            job: JobIdentity {
                id: spec.id.as_deref(),
                path: &request.job.path,
                sha256: &request.job.sha256,
                size: admitted.job.reference.bytes,
            },
            admitted_request: &admitted.admitted_request,
            inputs: &inputs,
            steps: &steps,
            outputs: &outputs,
            reports: &reports,
            view_set: &view_set,
            view_set_digest: &view_set.reference.digest,
            schemas: &schemas,
            evaluator: serde_json::json!({"id":"urn:docspec:evaluator:native-job-reports","version":"1"}),
            runtime: &admitted.runtime,
            execution: "completed",
            result: &result,
            producer_evidence: producer_evidence.as_ref().map(|evidence| &evidence.0),
            producer_evidence_witness: producer_evidence.as_ref().map(|evidence| &evidence.1),
        };
        let manifest_bytes = encode_metadata(&manifest_document, &budget)?;
        let manifest = self
            .retain_job_bytes(
                &request.dataset,
                &admitted.directory,
                "completion.json",
                manifest_bytes.value.clone(),
                &budget,
            )
            .await?;
        let candidate = JobCandidateRef {
            scheme: "urn:docspec:whole-job-candidate".into(),
            scheme_version: "1".into(),
            dataset: request.dataset.clone(),
            operation: request.operation.clone(),
            manifest: manifest.reference.clone(),
        };
        self.commit_job_record(
            &request.dataset,
            &result_key,
            &Completion {
                candidate,
                manifest,
            },
        )
        .await
    }

    pub(super) async fn job_status(
        &self,
        dataset: &Id,
        operation: &Id,
        requested_policy: Option<&Id>,
    ) -> Result<ManagedJobReceipt> {
        let budget = &self.inner.lifecycle.budget;
        let view = self.read_view(dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        let keys = [
            "job-operation",
            "job-active",
            "job-result",
            "job-selected",
            "job-attempt",
        ]
        .map(|family| format!("{family}/{operation}"));
        let _state =
            rows.reserve_decoded(budget, &keys.iter().map(String::as_str).collect::<Vec<_>>())?;
        let (_, _, admitted): (_, _, Admission) = rows.get_record(&keys[0])?.ok_or_else(|| {
            Error::refusal("no_operation", "whole-job operation has not been admitted")
        })?;
        let (_, _, active): (_, _, Id) = rows.get_record(&keys[1])?.ok_or_else(mismatch)?;
        let policy_id = requested_policy.unwrap_or(&active);
        let policy_key = format!("job-policy/{operation}/{policy_id}");
        let accepted_key = format!("job-accepted/{operation}/{policy_id}");
        let _policy = rows.reserve_decoded(budget, &[&policy_key, &accepted_key])?;
        let (_, _, decision): (_, _, Decision) =
            rows.get_record(&policy_key)?.ok_or_else(mismatch)?;
        let completed: Option<Completion> = rows.get_record(&keys[2])?.map(|(_, _, value)| value);
        let selected: Option<qualification::Selected> =
            rows.get_record(&keys[3])?.map(|(_, _, value)| value);
        let attempt: Option<Attempt> = rows.get_record(&keys[4])?.map(|(_, _, value)| value);
        let accepted: Option<qualification::Accepted> =
            rows.get_record(&accepted_key)?.map(|(_, _, value)| value);
        let partial;
        let input_custody;
        let input_custody_memory;
        let (job_id, candidate, view_set_digest, outputs, reports, result, completion_manifest) =
            if let Some(completed) = completed {
                let bytes = witness_bytes(&completed.manifest, &self.inner.lifecycle.budget)?;
                let document: serde_json::Value = serde_json::from_slice(&bytes.value)?;
                input_custody = input_custody::completed_references(&document)?;
                input_custody_memory = self.request_memory(&input_custody)?;
                (
                    document["result"]["job_id"]
                        .as_str()
                        .or_else(|| document["job"]["id"].as_str())
                        .map(str::to_owned),
                    Some(completed.candidate.clone()),
                    document["view_set_digest"].as_str().map(str::to_owned),
                    document["outputs"]
                        .as_array()
                        .cloned()
                        .ok_or_else(mismatch)?,
                    document["reports"]
                        .as_array()
                        .cloned()
                        .ok_or_else(mismatch)?,
                    Some(document["result"].clone()),
                    Some(completed.manifest.clone()),
                )
            } else {
                let references = self.partial_custody_references(&view, &rows, &admitted)?;
                input_custody = references.value;
                input_custody_memory = references._memory;
                partial = self.partial_job_result(&admitted, &rows).await?;
                (
                    partial.value.result["job_id"].as_str().map(str::to_owned),
                    None,
                    None,
                    partial.value.outputs.clone(),
                    partial.value.reports.clone(),
                    Some(partial.value.result.clone()),
                    None,
                )
            };
        let mut receipt = ManagedJobReceipt {
            dataset: dataset.clone(),
            operation: operation.clone(),
            job_id,
            job_spec_sha256: admitted.job.sha256.clone(),
            request_digest: admitted.request_digest.clone(),
            status: if candidate.is_some() {
                "completed"
            } else if attempt
                .as_ref()
                .is_some_and(|attempt| attempt.status == "failed")
            {
                "failed"
            } else if attempt.is_some() {
                "unknown"
            } else {
                "pending"
            }
            .into(),
            candidate,
            view_set_digest,
            policy_id: policy_id.clone(),
            active_policy_id: active.clone(),
            accepted_qualification: accepted.map(serde_json::to_value).transpose()?,
            selection: if selected.is_some() {
                "selected"
            } else {
                "pending"
            }
            .into(),
            completed_outputs: outputs,
            completed_reports: reports,
            completion_manifest,
            admitted_request: admitted.admitted_request.clone(),
            policy: decision.policy.clone(),
            result,
            input_custody,
            _memory: None,
        };
        receipt._memory = Some(self.request_memory(&receipt)?);
        drop(input_custody_memory);
        Ok(receipt)
    }

    async fn partial_job_result(
        &self,
        admitted: &Admission,
        rows: &crate::catalog::SnapshotRows,
    ) -> Result<Held<PartialResult>> {
        let budget = &self.inner.lifecycle.budget;
        let bytes = witness_bytes(&admitted.job, budget)?;
        let spec: JobSpec = serde_json::from_slice(&bytes.value)?;
        let mut result = JobResult {
            job_id: spec.id.clone(),
            job_spec_sha256: admitted.job.sha256.clone(),
            native_inputs: Vec::new(),
            native_versions: Vec::new(),
            table_operations: Vec::new(),
            #[cfg(feature = "substrait")]
            saved_plans: Vec::new(),
            inputs: Vec::new(),
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
        };
        let mut outputs = Vec::new();
        let mut reports = Vec::new();
        let operation = &admitted.request.operation;
        for (index, input) in spec.inputs.iter().enumerate() {
            let key = format!("job-input/{operation}/{index}");
            let _memory = rows.reserve_decoded(budget, &[&key])?;
            if let Some((_, _, saved)) = rows.get_record::<InputAdmission>(&key)? {
                result.inputs.push(observed_managed_input(input, &saved)?);
            }
        }
        for (index, step) in spec.steps.iter().enumerate() {
            let key = format!("job-phase/{operation}/step/{index}");
            if rows.contains_key(&key) {
                result.steps.push(step.step_result(StepStorage::Scratch)?);
            }
        }
        for (index, output) in spec.outputs.iter().enumerate() {
            let key = format!("job-phase/{operation}/output/{index}");
            let _memory = rows.reserve_decoded(budget, &[&key])?;
            let Some((_, _, phase)): Option<(_, _, Phase)> = rows.get_record(&key)? else {
                continue;
            };
            let object = phase.object.as_ref().ok_or_else(mismatch)?;
            let file = phase.file.as_ref().ok_or_else(mismatch)?;
            if phase.name != output.name
                || file.rows != phase.rows
                || file.size != object.bytes
                || phase.sha256.as_deref() != Some(file.sha256.as_str())
            {
                return Err(mismatch());
            }
            result.outputs.push(OutputResult {
                name: output.name.clone(),
                file: file.clone(),
            });
            outputs.push(serde_json::json!({"name":output.name,"path":object.path,"sha256":file.sha256,"md5":file.md5,"size":file.size,"rows":file.rows,"footer_bytes":file.footer_bytes,"schema_sha256":phase.schema_sha256,"settings":output.settings,"completion":object,"dependencies":phase.dependencies}));
        }
        let renderer = Job::from_budget(budget.clone())?;
        for (index, query) in spec.queries.iter().enumerate() {
            let key = format!("job-phase/{operation}/report/{index}");
            let _memory = rows.reserve_decoded(budget, &[&key])?;
            let Some((_, _, phase)): Option<(_, _, Phase)> = rows.get_record(&key)? else {
                continue;
            };
            let object = phase.object.as_ref().ok_or_else(mismatch)?;
            let ipc = read_retained(object, budget)?;
            let (data, data_status) = match renderer.report_result(&query.name, &ipc.value) {
                Ok((query, _, actual_rows)) if actual_rows == phase.rows => {
                    let data = query.data.clone();
                    result.queries.push(query);
                    (data, "completed")
                }
                Ok(_) => return Err(mismatch()),
                Err(_) => (serde_json::Value::Null, "unknown"),
            };
            reports.push(serde_json::json!({"name":query.name,"sql_sha256":sha256(query.sql.as_bytes()),"schema_sha256":phase.schema_sha256,"rows":phase.rows,"data":data,"data_status":data_status,"completion":object,"dependencies":phase.dependencies}));
        }
        let _memory = self.request_memory(&(&outputs, &reports, &result))?;
        let value = PartialResult {
            outputs,
            reports,
            result: serde_json::to_value(result)?,
        };
        Ok(Held { value, _memory })
    }
}

struct PartialResult {
    outputs: Vec<serde_json::Value>,
    reports: Vec<serde_json::Value>,
    result: serde_json::Value,
}

fn validate_phase(phase: &Phase, admitted: &Admission, name: &str, identity: &str) -> Result<()> {
    if phase.origin_runtime != admitted.runtime {
        return Err(Error::refusal(
            "unqualified_job_runtime",
            "retained phase was produced by another native runtime",
        ));
    }
    if phase.name != name
        || phase.material_identity != identity
        || phase
            .object
            .as_ref()
            .is_some_and(|object| object.owner != admitted.request.dataset)
    {
        return Err(mismatch());
    }
    Ok(())
}

fn observed_managed_input(input: &JobInput, saved: &InputAdmission) -> Result<InputResult> {
    match input {
        JobInput::Parquet { name, path } => {
            let member = saved.files.first().ok_or_else(mismatch)?;
            Ok(InputResult {
                name: name.clone(),
                kind: "parquet".into(),
                path: Some(path.clone()),
                sha256: Some(member.sha256.clone()),
                bytes: Some(member.size),
                ..Default::default()
            })
        }
        JobInput::JsonLines { name, .. } => {
            let mut files = Vec::with_capacity(saved.files.len());
            let mut total_bytes = 0_u64;
            for member in &saved.files {
                total_bytes = total_bytes.checked_add(member.size).ok_or_else(capacity)?;
                files.push(InputFileResult {
                    key: member.key.clone(),
                    path: member.path.clone(),
                    sha256: Some(member.sha256.clone()),
                    bytes: member.size,
                });
            }
            let (path, digest) = if files.len() == 1 {
                (Some(files[0].path.clone()), files[0].sha256.clone())
            } else {
                let summary = files
                    .iter()
                    .map(|f| (&f.key, f.sha256.as_deref().unwrap_or_default(), f.bytes))
                    .collect::<Vec<_>>();
                (None, Some(sha256(&serde_json::to_vec(&summary)?)))
            };
            Ok(InputResult {
                name: name.clone(),
                kind: "json_lines".into(),
                path,
                sha256: digest,
                bytes: Some(total_bytes),
                files,
                ..Default::default()
            })
        }
        JobInput::NativeTable {
            name,
            metadata_location,
            table_uuid,
            snapshot,
            schema_id,
        } => {
            let custody = saved.snapshot_custody.as_ref().ok_or_else(mismatch)?;
            Ok(InputResult {
                name: name.clone(),
                kind: "native_table".into(),
                uri: Some(metadata_location.clone()),
                sha256: Some(custody.metadata_sha256.clone()),
                bytes: Some(custody.metadata_size),
                table_uuid: Some(*table_uuid),
                snapshot_id: *snapshot,
                schema_id: Some(*schema_id),
                ..Default::default()
            })
        }
        _ => Err(mismatch()),
    }
}

fn retain_input_admission(
    budget: &Budget,
    admissions: &mut Vec<InputAdmission>,
    memory: &mut Vec<tokio::sync::OwnedSemaphorePermit>,
    input: InputAdmission,
) -> Result<()> {
    let size = crate::budget::metadata_size(&input, budget.limits.max_metadata_bytes)?;
    memory.push(budget.reserve(size.saturating_mul(16))?);
    admissions.push(input);
    Ok(())
}

fn input_paths(input: &JobInput) -> Result<Vec<(Option<String>, PathBuf)>> {
    match input {
        JobInput::Parquet { path, .. } => Ok(vec![(None, path.clone())]),
        JobInput::JsonLines { files, .. } => Ok(files
            .iter()
            .map(|file| (Some(file.key.clone()), file.path.clone()))
            .collect()),
        _ => Err(Error::refusal(
            "unsupported_job_custody",
            "native owner protection is required for this input",
        )),
    }
}

fn input_identity(input: &JobInput, binding: &JobFileInput) -> Result<String> {
    let interpretation = match input {
        JobInput::Parquet { .. } => serde_json::json!({"kind":"parquet"}),
        JobInput::JsonLines { schema, .. } => {
            serde_json::json!({"kind":"json_lines","schema":schema})
        }
        _ => return Err(mismatch()),
    };
    json_identity(&(input.name(), interpretation, binding))
}

async fn dependency_identities(
    job: &Job,
    sql: &str,
    parameters: &BTreeMap<String, serde_json::Value>,
    identities: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    job.retained_dependencies(sql, parameters, identities)
        .await?
        .into_iter()
        .map(|name| {
            identities
                .get(&name)
                .map(|identity| format!("{name}:{identity}"))
                .ok_or_else(|| {
                    Error::refusal(
                        "managed_job_dependency",
                        format!("native planned relation {name} has no admitted identity"),
                    )
                })
        })
        .collect()
}

fn relation_key(output: bool, name: &str) -> Result<String> {
    Ok(format!(
        "{}.{}",
        if output { "outputs" } else { "public" },
        serde_json::to_string(name)?
    ))
}

fn retain_phase_schema(
    phase: &Phase,
    budget: &Budget,
    schemas: &mut BTreeMap<String, String>,
    memory: &mut Vec<tokio::sync::OwnedSemaphorePermit>,
) -> Result<()> {
    if phase.schema_ipc_base64.len() > budget.limits.max_metadata_bytes.saturating_mul(2) {
        return Err(capacity());
    }
    let _decode = budget.reserve(phase.schema_ipc_base64.len().saturating_mul(8))?;
    let bytes = STANDARD
        .decode(&phase.schema_ipc_base64)
        .map_err(|_| mismatch())?;
    if crate::job::retained::retain_schema_into(budget, schemas, memory, &bytes)?
        != phase.schema_sha256
    {
        return Err(mismatch());
    }
    Ok(())
}

pub(super) fn read_retained(reference: &ObjectRef, budget: &Budget) -> Result<Held<Vec<u8>>> {
    if reference.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(capacity());
    }
    let memory = budget.reserve((reference.bytes as usize).saturating_mul(32))?;
    let file = File::open(&reference.path)?;
    if file.metadata()?.len() != reference.bytes {
        return Err(mismatch());
    }
    let mut bytes = Vec::with_capacity(reference.bytes as usize);
    file.take(reference.bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != reference.bytes
        || blake3::hash(&bytes).to_hex().as_str() != reference.digest
    {
        return Err(mismatch());
    }
    Ok(Held {
        value: bytes,
        _memory: memory,
    })
}

async fn copy_input(
    owner: &Id,
    original: &Path,
    path: &Path,
    declared: &JobFileMember,
    budget: &Budget,
) -> Result<ObjectRef> {
    let (owner, original, path, declared, held_budget) = (
        owner.clone(),
        original.to_owned(),
        path.to_owned(),
        declared.clone(),
        budget.clone(),
    );
    budget
        .blocking_admitted(move || {
            copy_input_admitted(&owner, &original, &path, &declared, &held_budget)
        })
        .await
}

fn copy_input_admitted(
    owner: &Id,
    original: &Path,
    path: &Path,
    declared: &JobFileMember,
    budget: &Budget,
) -> Result<ObjectRef> {
    let present = path.try_exists()?;
    if present && std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(mismatch());
    }
    // Only an uncertain native copy rereads its protected exact destination.
    // A sealed member resolves above without reopening either source or target.
    let mut input = File::open(if present { path } else { original })?;
    if !input.metadata()?.is_file() || input.metadata()?.len() != declared.size {
        return Err(mismatch());
    }
    let temporary = if present {
        None
    } else {
        Some(
            budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("retained input member")?,
        )
    };
    let mut output = temporary
        .as_ref()
        .map(|file| budget.scratch_writer(file))
        .transpose()?;
    let mut sha = Sha256::new();
    let mut digest = blake3::Hasher::new();
    let mut size = 0u64;
    let chunk = budget.limits.max_batch_bytes.min(64 << 10);
    let _memory = budget.reserve(chunk)?;
    let mut bytes = vec![0u8; chunk];
    loop {
        let count = input.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count as u64)
            .filter(|size| *size <= declared.size)
            .ok_or_else(mismatch)?;
        if let Some(output) = &mut output {
            output.write_all(&bytes[..count])?;
        }
        sha.update(&bytes[..count]);
        digest.update(&bytes[..count]);
    }
    if size != declared.size || format!("{:x}", sha.finalize()) != declared.sha256 {
        return Err(mismatch());
    }
    drop(output);
    if let Some(temporary) = temporary {
        temporary.inner().as_file().sync_all()?;
        std::fs::hard_link(temporary.path(), path)?;
        budget.retain_temporary(temporary)?;
    }
    File::open(path.parent().ok_or_else(mismatch)?)?.sync_all()?;
    Ok(ObjectRef {
        owner: owner.clone(),
        path: path.to_string_lossy().into_owned(),
        digest: digest.finalize().to_hex().to_string(),
        bytes: size,
    })
}

fn owned_bytes(directory: &Path, limit: usize) -> Result<u64> {
    if !directory.exists() {
        return Ok(0);
    }
    let mut pending = vec![directory.to_owned()];
    let mut bytes = 0u64;
    let mut count = 0usize;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            count += 1;
            if count.saturating_mul(256) > limit {
                return Err(capacity());
            }
            let entry = entry?;
            let metadata = entry.metadata()?;
            if entry.file_type()?.is_symlink() {
                return Err(mismatch());
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                bytes = bytes.checked_add(metadata.len()).ok_or_else(capacity)?;
            }
        }
    }
    Ok(bytes)
}
