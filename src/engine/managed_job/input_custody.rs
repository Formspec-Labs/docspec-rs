//! Transfers exact same-owner input custody under the existing guarded operation root.
// Rust guideline compliant 2026-02-21
use super::*;
use crate::catalog::SnapshotRows;

const SCHEME: &str = "urn:docspec:whole-job-input-custody";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginArtifact {
    pub reference: ObjectRef,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InputOrigin {
    pub operation: Id,
    pub request_digest: String,
    pub definition: String,
    pub runtime: PhaseRuntime,
    pub job: OriginArtifact,
    pub admitted_request: OriginArtifact,
    pub policy: OriginArtifact,
}

impl InputOrigin {
    pub(super) fn from_admission(admitted: &Admission) -> Self {
        let artifact = |value: &JobArtifactWitness| OriginArtifact {
            reference: value.reference.clone(),
            sha256: value.sha256.clone(),
        };
        Self {
            operation: admitted.request.operation.clone(),
            request_digest: admitted.request_digest.clone(),
            definition: admitted.request.definition.clone(),
            runtime: admitted.runtime.clone(),
            job: artifact(&admitted.job),
            admitted_request: artifact(&admitted.admitted_request),
            policy: artifact(&admitted.initial_policy),
        }
    }

    pub(super) fn objects(&self) -> impl Iterator<Item = &ObjectRef> {
        [
            &self.job.reference,
            &self.admitted_request.reference,
            &self.policy.reference,
        ]
        .into_iter()
    }
}

pub(super) fn input_objects(input: &execution::InputAdmission) -> Vec<ObjectRef> {
    input
        .files
        .iter()
        .map(|file| file.retained.clone())
        .chain(
            input
                .snapshot_custody
                .iter()
                .flat_map(|custody| custody.objects.iter().map(|object| object.retained.clone())),
        )
        .chain(
            input
                .provenance
                .iter()
                .flat_map(|origin| origin.objects().cloned()),
        )
        .collect()
}

pub(super) fn material_identity(
    input: &JobInput,
    custody: &snapshots::SnapshotCustody,
) -> Result<String> {
    // Location of the native retained copy and donation history are provenance,
    // not influences on data values or the original native interpretation.
    let members = custody
        .objects
        .iter()
        .map(|object| (&object.uri, &object.role, &object.sha256, object.size))
        .collect::<Vec<_>>();
    json_identity(&(
        input,
        &custody.metadata_sha256,
        custody.metadata_size,
        members,
    ))
}

fn valid_reference(reference: &JobInputCustodyRef, view: &View, dataset: &Id) -> Result<()> {
    if reference.scheme != SCHEME
        || reference.scheme_version != "1"
        || &reference.dataset != dataset
        || reference.native_record.table != view.identifier().to_string()
        || reference.native_record.table_uuid != view.table.metadata().uuid().to_string()
        || reference.native_record.revision <= 0
        || reference.native_record.revision > view.revision
        || reference.admission_digest.algorithm != "blake3"
        || !valid_sha256(&reference.admission_digest.value)
        || !valid_sha256(&reference.request_digest)
    {
        return Err(mismatch());
    }
    Ok(())
}

impl Engine {
    pub(super) async fn prepare_input_grants(
        &self,
        request: &ManagedJobPrepare,
        spec: &JobSpec,
        view: &View,
        rows: &SnapshotRows,
    ) -> Result<Held<BTreeMap<String, execution::InputAdmission>>> {
        let budget = &self.inner.lifecycle.budget;
        let mut grants = BTreeMap::new();
        let mut grant_memory = budget.reserve(0)?;
        for binding in &request.snapshot_inputs {
            let Some(reference) = &binding.reuse_from else {
                continue;
            };
            valid_reference(reference, view, &request.dataset)?;
            if reference.operation == request.operation {
                return Err(mismatch());
            }
            let operation_key = format!("job-operation/{}", reference.operation);
            let abandoned_key = format!("abandoned/{}", reference.operation);
            let _source = rows.reserve_decoded(budget, &[&operation_key, &abandoned_key])?;
            let (operation_revision, _, original): (_, _, Admission) =
                rows.get_record(&operation_key)?.ok_or_else(mismatch)?;
            if original.request.dataset != request.dataset
                || original.request.operation != reference.operation
                || original.request_digest != reference.request_digest
                || json_identity(&original.request)? != original.request_digest
            {
                return Err(mismatch());
            }
            if rows.contains_key(&abandoned_key) {
                return Err(Error::refusal(
                    "operation_abandoned",
                    "input custody donor was explicitly abandoned",
                ));
            }
            if super::super::retention::retirement::record_retired(
                rows,
                view,
                &operation_key,
                operation_revision,
                budget,
            )? {
                return Err(Error::refusal(
                    "recovery_retired",
                    "input custody donor recovery was explicitly retired",
                ));
            }
            self.check_operation_available(view, &reference.operation)
                .await?;
            let job_bytes = witness_bytes(&original.job, budget)?;
            let original_spec: JobSpec = serde_json::from_slice(&job_bytes.value)?;
            let (ordinal, original_input) = original_spec
                .inputs
                .iter()
                .enumerate()
                .find(|(_, input)| input.name() == reference.input)
                .ok_or_else(mismatch)?;
            let expected_key = format!("job-input/{}/{ordinal}", reference.operation);
            if reference.native_record.key != expected_key {
                return Err(mismatch());
            }
            let _input = rows.reserve_decoded(budget, &[&expected_key])?;
            let (revision, _, mut input): (_, _, execution::InputAdmission) =
                rows.get_record(&expected_key)?.ok_or_else(mismatch)?;
            if revision != reference.native_record.revision
                || json_identity(&input)? != reference.admission_digest.value
                || input.name != reference.input
                || serde_json::to_value(&input.selector)? != serde_json::to_value(original_input)?
            {
                return Err(mismatch());
            }
            if super::super::retention::retirement::record_retired(
                rows,
                view,
                &expected_key,
                revision,
                budget,
            )? {
                return Err(Error::refusal(
                    "input_custody_retired",
                    "the requested input phase recovery was explicitly retired",
                ));
            }
            let target = spec
                .inputs
                .iter()
                .find(|input| input.name() == binding.name)
                .ok_or_else(mismatch)?;
            if serde_json::to_value(snapshots::selection(original_input)?)?
                != serde_json::to_value(snapshots::selection(target)?)?
            {
                return Err(mismatch());
            }
            let custody = input.snapshot_custody.as_ref().ok_or_else(mismatch)?;
            custody.validate(original_input)?;
            if custody.metadata_sha256 != binding.metadata_sha256
                || custody.metadata_size != binding.metadata_size
                || !input.files.is_empty()
                || custody.objects.iter().any(|object| {
                    matches!(
                        object.role.as_str(),
                        "position_deletes" | "equality_deletes"
                    )
                })
            {
                return Err(mismatch());
            }
            let original_binding = original
                .request
                .snapshot_inputs
                .iter()
                .find(|binding| binding.name == reference.input)
                .ok_or_else(mismatch)?;
            if original_binding.metadata_sha256 != binding.metadata_sha256
                || original_binding.metadata_size != binding.metadata_size
            {
                return Err(mismatch());
            }
            if input.provenance.is_empty() {
                if custody
                    .objects
                    .iter()
                    .any(|object| object.origin_operation != original.request.operation)
                {
                    return Err(mismatch());
                }
                input
                    .provenance
                    .push(InputOrigin::from_admission(&original));
            }
            for origin in &input.provenance {
                for object in origin.objects() {
                    self.check_custody_object(rows, view, object, &origin.operation)?;
                }
            }
            for object in &custody.objects {
                if !input
                    .provenance
                    .iter()
                    .any(|origin| origin.operation == object.origin_operation)
                {
                    return Err(mismatch());
                }
                self.check_custody_object(rows, view, &object.retained, &object.origin_operation)?;
            }
            input.name = binding.name.clone();
            input.selector = target.clone();
            input.reuse_from = Some(reference.clone());
            // Transfer the descriptor's admission before the source-row permits end.
            grant_memory.merge(self.request_memory(&input)?);
            grants.insert(binding.name.clone(), input);
        }
        Ok(Held {
            value: grants,
            _memory: grant_memory,
        })
    }

    pub(super) fn check_custody_object(
        &self,
        rows: &SnapshotRows,
        view: &View,
        object: &ObjectRef,
        origin: &Id,
    ) -> Result<()> {
        let dataset = &object.owner;
        let prefix = self
            .inner
            .root
            .join("objects")
            .join(dataset.as_str())
            .join(format!("job-{origin}"));
        if !Path::new(&object.path).starts_with(prefix) {
            return Err(mismatch());
        }
        let protection = format!("protection/job-{origin}");
        let unavailable = super::super::retention::owners::unavailable_key(&object.path);
        let _memory =
            rows.reserve_decoded(&self.inner.lifecycle.budget, &[&protection, &unavailable])?;
        self.check_object_owner(rows, dataset, object)?;
        super::super::retention::owners::available(rows, view, object)
    }

    pub(super) fn check_job_evidence_object(
        &self,
        rows: &SnapshotRows,
        view: &View,
        object: &ObjectRef,
    ) -> Result<()> {
        let owner = self.inner.root.join("objects").join(object.owner.as_str());
        let relative = Path::new(&object.path)
            .strip_prefix(owner)
            .map_err(|_| mismatch())?;
        let origin = relative
            .components()
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .and_then(|attempt| attempt.strip_prefix("job-"))
            .ok_or_else(mismatch)?;
        self.check_custody_object(rows, view, object, &Id::new(origin)?)
    }

    pub(super) fn custody_reference(
        &self,
        view: &View,
        rows: &SnapshotRows,
        admitted: &Admission,
        ordinal: usize,
        input: &execution::InputAdmission,
    ) -> Result<JobInputCustodyRef> {
        let key = format!("job-input/{}/{ordinal}", admitted.request.operation);
        let revision = rows.revision(&key).ok_or_else(mismatch)?;
        Ok(JobInputCustodyRef {
            scheme: SCHEME.into(),
            scheme_version: "1".into(),
            dataset: admitted.request.dataset.clone(),
            operation: admitted.request.operation.clone(),
            request_digest: admitted.request_digest.clone(),
            input: input.name.clone(),
            native_record: JobInputRecordRef {
                table: view.identifier().to_string(),
                table_uuid: view.table.metadata().uuid().to_string(),
                key,
                revision,
            },
            admission_digest: JobInputAdmissionDigest {
                algorithm: "blake3".into(),
                value: json_identity(input)?,
            },
        })
    }

    pub(super) async fn current_input_reference(
        &self,
        admitted: &Admission,
        ordinal: usize,
        input: &execution::InputAdmission,
    ) -> Result<JobInputCustodyRef> {
        let view = self.read_view(&admitted.request.dataset).await?;
        let rows = self.inner.lifecycle.rows(&view).await?;
        self.custody_reference(&view, &rows, admitted, ordinal, input)
    }

    pub(super) fn partial_custody_references(
        &self,
        view: &View,
        rows: &SnapshotRows,
        admitted: &Admission,
    ) -> Result<Held<Vec<JobInputCustodyRef>>> {
        let budget = &self.inner.lifecycle.budget;
        let bytes = witness_bytes(&admitted.job, budget)?;
        let spec: JobSpec = serde_json::from_slice(&bytes.value)?;
        let mut result = Vec::new();
        let mut memory = budget.reserve(0)?;
        for (index, input) in spec
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| matches!(input, JobInput::NativeTable { .. }))
        {
            let key = format!("job-input/{}/{index}", admitted.request.operation);
            let _memory = rows.reserve_decoded(budget, &[&key])?;
            if let Some((_, _, saved)) = rows.get_record::<execution::InputAdmission>(&key)? {
                if saved.name != input.name() {
                    return Err(mismatch());
                }
                let reference = self.custody_reference(view, rows, admitted, index, &saved)?;
                memory.merge(self.request_memory(&reference)?);
                result.push(reference);
            }
        }
        Ok(Held {
            value: result,
            _memory: memory,
        })
    }
}

pub(super) fn inherited_bytes(admitted: &Admission) -> Result<u64> {
    reference_bytes(
        &admitted.directory,
        admitted.input_grants.values(),
        producer::inherited_objects(admitted),
    )
}

fn reference_bytes<'a>(
    directory: &Path,
    inputs: impl Iterator<Item = &'a execution::InputAdmission>,
    evidence: &[ObjectRef],
) -> Result<u64> {
    let mut unique = BTreeMap::<String, ObjectRef>::new();
    for object in inputs
        .flat_map(input_objects)
        .chain(evidence.iter().cloned())
    {
        if let Some(old) = unique.insert(object.path.clone(), object.clone())
            && old != object
        {
            return Err(mismatch());
        }
    }
    unique
        .values()
        .filter(|object| !Path::new(&object.path).starts_with(directory))
        .try_fold(0u64, |sum, object| {
            sum.checked_add(object.bytes).ok_or_else(capacity)
        })
}

pub(super) fn completed_references(
    document: &serde_json::Value,
) -> Result<Vec<JobInputCustodyRef>> {
    let inputs = document["inputs"].as_array().ok_or_else(mismatch)?;
    let native = inputs
        .iter()
        .filter(|input| input["kind"] == "native_table")
        .collect::<Vec<_>>();
    // Historical completed candidates cannot acquire new unbound observations.
    if !native
        .iter()
        .any(|input| input.get("custody_reference").is_some())
    {
        return Ok(Vec::new());
    }
    native
        .iter()
        .map(|input| {
            serde_json::from_value(input.get("custody_reference").ok_or_else(mismatch)?.clone())
                .map_err(Into::into)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(path: &str, bytes: u64) -> execution::InputAdmission {
        execution::InputAdmission {
            name: "raw".into(),
            selector: JobInput::NativeTable {
                name: "raw".into(),
                metadata_location: "/original/metadata.json".into(),
                table_uuid: uuid::Uuid::nil(),
                snapshot: None,
                schema_id: 0,
            },
            files: Vec::new(),
            provenance: Vec::new(),
            reuse_from: None,
            snapshot_custody: Some(snapshots::SnapshotCustody {
                profile: "urn:docspec:whole-job-snapshot-custody:2".into(),
                scope: "selected_snapshot".into(),
                metadata_sha256: "0".repeat(64),
                metadata_size: bytes,
                objects: vec![snapshots::SnapshotObject {
                    origin_operation: Id::new("source").unwrap(),
                    uri: "/original/metadata.json".into(),
                    role: "metadata".into(),
                    sha256: "0".repeat(64),
                    size: bytes,
                    retained: ObjectRef {
                        owner: Id::new("native-job").unwrap(),
                        path: path.into(),
                        digest: "0".repeat(64),
                        bytes,
                    },
                }],
            }),
        }
    }

    #[test]
    fn inherited_disk_charge_uses_exact_unique_physical_references() {
        let directory = Path::new("/retained/current");
        let original = input("/retained/old/object", 12);
        let alias = original.clone();
        assert_eq!(
            reference_bytes(directory, [&original, &alias].into_iter(), &[]).unwrap(),
            12
        );
        let distinct = input("/retained/old/another", 12);
        assert_eq!(
            reference_bytes(directory, [&original, &distinct].into_iter(), &[]).unwrap(),
            24
        );
        let conflict = input("/retained/old/object", 13);
        assert_eq!(
            reference_bytes(directory, [&original, &conflict].into_iter(), &[])
                .unwrap_err()
                .code(),
            mismatch().code()
        );
        let huge = input("/retained/old/huge", u64::MAX);
        assert!(reference_bytes(directory, [&original, &huge].into_iter(), &[]).is_err());
        let own = input("/retained/current/member", 12);
        assert_eq!(
            reference_bytes(directory, [&original, &own].into_iter(), &[]).unwrap(),
            12,
            "owned directory census already charges current files"
        );
        assert_eq!(
            reference_bytes(
                directory,
                [&original].into_iter(),
                &input_objects(&original)
            )
            .unwrap(),
            12,
            "proof evidence and custody share the same physical metadata charge"
        );
    }
}
