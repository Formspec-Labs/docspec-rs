//! Retains exact selected native read closures through ordinary job member seals.
// Rust guideline compliant 2026-02-21
use super::*;
use iceberg::{
    io::{FileIO, FileRead, InputFile},
    spec::DataContentType,
};

const CUSTODY: &str = "urn:docspec:whole-job-snapshot-custody:2";
const SNAPSHOT_MEMBER_CONCURRENCY: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotSourcePlacement {
    prefix: String,
    connection: String,
}

#[derive(Clone, Copy)]
struct MemberCopy<'a> {
    uri: &'a str,
    role: &'a str,
    size: u64,
    sha: Option<&'a str>,
}

enum SnapshotReader {
    Local {
        input: InputFile,
        reader: Box<dyn FileRead>,
    },
    #[cfg(feature = "remote")]
    S3(Arc<crate::storage::remote::SnapshotSource>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotObject {
    pub origin_operation: Id,
    pub uri: String,
    pub role: String,
    pub sha256: String,
    pub size: u64,
    pub retained: ObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotCustody {
    pub profile: String,
    pub scope: String,
    pub metadata_sha256: String,
    pub metadata_size: u64,
    pub objects: Vec<SnapshotObject>,
}

impl SnapshotCustody {
    pub(super) fn validate(&self, input: &JobInput) -> Result<()> {
        let selected = selection(input)?;
        let mut names = BTreeSet::new();
        if self.profile != CUSTODY
            || self.scope != "selected_snapshot"
            || !valid_sha256(&self.metadata_sha256)
            || self
                .objects
                .windows(2)
                .any(|pair| pair[0].uri >= pair[1].uri)
            || self.objects.iter().any(|object| {
                !names.insert(&object.uri)
                    || !valid_sha256(&object.sha256)
                    || object.size != object.retained.bytes
                    || !matches!(
                        object.role.as_str(),
                        "metadata"
                            | "manifest_list"
                            | "manifest"
                            | "data"
                            | "position_deletes"
                            | "equality_deletes"
                    )
            })
        {
            return Err(mismatch());
        }
        let metadata = self
            .objects
            .iter()
            .filter(|object| object.role == "metadata")
            .collect::<Vec<_>>();
        if metadata.len() != 1
            || metadata[0].uri != selected.metadata_location
            || metadata[0].sha256 != self.metadata_sha256
            || metadata[0].size != self.metadata_size
        {
            return Err(mismatch());
        }
        let lists = self
            .objects
            .iter()
            .filter(|object| object.role == "manifest_list")
            .count();
        if selected.snapshot.is_none() && self.objects.len() != 1
            || selected.snapshot.is_some() && lists != 1
        {
            return Err(mismatch());
        }
        Ok(())
    }
    pub(super) fn paths(&self) -> BTreeMap<String, String> {
        self.objects
            .iter()
            .map(|object| (object.uri.clone(), object.retained.path.clone()))
            .collect()
    }

    pub(super) fn memory(&self, budget: &Budget) -> Result<tokio::sync::OwnedSemaphorePermit> {
        if self.profile != CUSTODY || self.scope != "selected_snapshot" {
            return Err(mismatch());
        }
        let size = super::super::inputs::metadata_size(self, budget.limits.max_metadata_bytes)?;
        let mut memory = budget.reserve(size.saturating_mul(32))?;
        // The permit covers this map and its clones held by maintained native I/O.
        // Actual native footer admission is added before relation registration.
        for object in &self.objects {
            if !valid_sha256(&object.sha256) || object.size != object.retained.bytes {
                return Err(mismatch());
            }
        }
        // Keep a nonzero owned permit even for the valid empty-snapshot closure.
        memory.merge(budget.reserve(1)?);
        Ok(memory)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotMember {
    pub operation: Id,
    pub request_digest: String,
    pub object: SnapshotObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotPlan {
    pub operation: Id,
    pub request_digest: String,
    pub selector: JobInput,
    pub metadata_sha256: String,
    pub metadata_size: u64,
    pub metadata: Vec<SnapshotObject>,
    pub payloads: Vec<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Payload {
    uri: String,
    role: String,
    size: u64,
}

pub(super) fn selection(input: &JobInput) -> Result<crate::storage::NativeTableSnapshot> {
    let JobInput::NativeTable {
        metadata_location,
        table_uuid,
        snapshot,
        schema_id,
        ..
    } = input
    else {
        return Err(mismatch());
    };
    Ok(crate::storage::NativeTableSnapshot {
        metadata_location: metadata_location.clone(),
        table_uuid: *table_uuid,
        snapshot: *snapshot,
        schema_id: *schema_id,
    })
}

impl Engine {
    pub(super) fn admit_snapshot_source(
        &self,
        spec: &JobSpec,
        grants: &BTreeMap<String, execution::InputAdmission>,
    ) -> Result<Option<SnapshotSourcePlacement>> {
        let mut original = spec.inputs.iter().filter_map(|input| match input {
            JobInput::NativeTable {
                name,
                metadata_location,
                ..
            } if !grants.contains_key(name) && storage::is_remote_uri(metadata_location) => {
                Some(metadata_location.as_str())
            }
            _ => None,
        });
        let Some(first) = original.next() else {
            return Ok(None);
        };
        #[cfg(feature = "remote")]
        {
            let source = self
                .snapshot_source
                .as_ref()
                .ok_or_else(source_unavailable)?;
            let placement = SnapshotSourcePlacement {
                prefix: source.prefix(),
                connection: source.placement.clone(),
            };
            for uri in std::iter::once(first).chain(original) {
                check_member_scope(Some(&placement), true, uri)?;
            }
            Ok(Some(placement))
        }
        #[cfg(not(feature = "remote"))]
        {
            let _ = (first, original);
            Err(source_unavailable())
        }
    }

    #[cfg(feature = "remote")]
    fn snapshot_source_for(
        &self,
        admitted: &Admission,
    ) -> Result<&Arc<crate::storage::remote::SnapshotSource>> {
        let placement = admitted
            .snapshot_source
            .as_ref()
            .ok_or_else(source_unavailable)?;
        let source = self
            .snapshot_source
            .as_ref()
            .ok_or_else(source_unavailable)?;
        if placement.prefix != source.prefix() || placement.connection != source.placement {
            return Err(Error::refusal(
                "snapshot_source_placement",
                "configured S3 snapshot source differs from its admitted placement",
            ));
        }
        Ok(source)
    }

    pub(super) async fn producer_snapshot_metadata(
        &self,
        admitted: &Admission,
        input: &JobInput,
        budget: &Budget,
    ) -> Result<Held<(SnapshotObject, iceberg::spec::TableMetadata)>> {
        let inherited = admitted.input_grants.get(input.name());
        let plan = if inherited.is_none() {
            Some(self.job_snapshot_plan(admitted, input, budget).await?)
        } else {
            None
        };
        let objects = if let Some(inherited) = inherited {
            let custody = inherited.snapshot_custody.as_ref().ok_or_else(mismatch)?;
            custody.validate(input)?;
            custody.objects.as_slice()
        } else {
            &plan.as_ref().ok_or_else(mismatch)?.metadata
        };
        let selected = selection(input)?;
        let object = objects
            .iter()
            .find(|object| object.role == "metadata" && object.uri == selected.metadata_location)
            .ok_or_else(mismatch)?;
        let mut memory = self.request_memory(object)?;
        let (metadata, decoded) =
            crate::storage::read_retained_snapshot_metadata(&object.retained, budget).await?;
        memory.merge(decoded);
        Ok(Held {
            value: (object.clone(), metadata),
            _memory: memory,
        })
    }

    pub(super) async fn plan_job_snapshots(
        &self,
        admitted: &Admission,
        spec: &JobSpec,
        budget: &Budget,
    ) -> Result<()> {
        let mut unique = BTreeMap::new();
        let mut memory = budget.reserve(0)?;
        // All native metadata is admitted before any input payload copy begins.
        for input in &spec.inputs {
            if !matches!(input, JobInput::NativeTable { .. }) {
                continue;
            }
            let inherited = admitted.input_grants.get(input.name());
            let plan = if inherited.is_none() {
                Some(self.job_snapshot_plan(admitted, input, budget).await?)
            } else {
                None
            };
            let metadata = if let Some(inherited) = inherited {
                let custody = inherited.snapshot_custody.as_ref().ok_or_else(mismatch)?;
                custody.validate(input)?;
                custody.objects.as_slice()
            } else {
                &plan.as_ref().ok_or_else(mismatch)?.metadata
            };
            for object in metadata {
                add_claim(
                    &mut unique,
                    &mut memory,
                    budget,
                    Claim {
                        uri: &object.uri,
                        role: &object.role,
                        sha: Some(&object.sha256),
                        size: object.size,
                        inherited: inherited.is_some()
                            || object.origin_operation != admitted.request.operation,
                    },
                )?;
            }
            if let Some(plan) = &plan {
                for object in &plan.payloads {
                    add_claim(
                        &mut unique,
                        &mut memory,
                        budget,
                        Claim {
                            uri: &object.uri,
                            role: &object.role,
                            sha: None,
                            size: object.size,
                            inherited: donor_has_uri(admitted, input.name(), &object.uri),
                        },
                    )?;
                }
            }
            super::super::inputs::metadata_size(&unique, budget.limits.max_metadata_bytes)?;
        }
        let total = unique
            .values()
            .map(|(size, _, _, _)| *size)
            .chain(
                admitted
                    .request
                    .file_inputs
                    .iter()
                    .flat_map(|input| input.members.iter().map(|member| member.size)),
            )
            .try_fold(0u64, |sum, size| sum.checked_add(size).ok_or_else(capacity))?;
        if total > budget.limits.scratch_bytes {
            return Err(Error::refusal(
                "disk_capacity",
                "complete admitted native and file inputs exceed the operation allowance",
            ));
        }
        Ok(())
    }

    async fn job_snapshot_plan(
        &self,
        admitted: &Admission,
        input: &JobInput,
        budget: &Budget,
    ) -> Result<Held<SnapshotPlan>> {
        let request = &admitted.request;
        let binding = request
            .snapshot_inputs
            .iter()
            .find(|binding| binding.name == input.name())
            .ok_or_else(mismatch)?;
        let key = format!(
            "job-snapshot-plan/{}/{}",
            request.operation,
            sha256(input.name().as_bytes())
        );
        if let Some(saved) = self
            .job_record::<SnapshotPlan>(&request.dataset, &key)
            .await?
        {
            if saved.operation != request.operation
                || saved.request_digest != admitted.request_digest
                || serde_json::to_value(&saved.selector)? != serde_json::to_value(input)?
                || saved.metadata_sha256 != binding.metadata_sha256
                || saved.metadata_size != binding.metadata_size
            {
                return Err(mismatch());
            }
            return Ok(saved);
        }
        let plan = self
            .plan_job_snapshot(admitted, input, binding, budget)
            .await?;
        self.commit_job_record(&request.dataset, &key, &plan.value)
            .await?;
        Ok(plan)
    }

    pub(super) async fn admit_job_snapshot(
        &self,
        admitted: &Admission,
        input: &JobInput,
        budget: &Budget,
    ) -> Result<Held<SnapshotCustody>> {
        let request = &admitted.request;
        let binding = request
            .snapshot_inputs
            .iter()
            .find(|binding| binding.name == input.name())
            .ok_or_else(mismatch)?;
        let held_plan = self.job_snapshot_plan(admitted, input, budget).await?;
        let plan = &held_plan.value;
        let total = plan
            .metadata
            .iter()
            .map(|object| object.size)
            .chain(plan.payloads.iter().map(|object| object.size))
            .try_fold(0u64, |sum, size| sum.checked_add(size).ok_or_else(capacity))?;
        // One operation budget already owns old/partial files and native spill.
        // Refuse the complete selected closure before any payload transfer.
        if total > budget.limits.scratch_bytes {
            return Err(Error::refusal(
                "disk_capacity",
                "selected native read closure exceeds the operation disk allowance",
            ));
        }
        let mut objects = plan.metadata.clone();
        let remote = storage::is_remote_uri(&selection(input)?.metadata_location);
        let mut unresolved = Vec::new();
        for payload in &plan.payloads {
            if let Some(reused) = donor_member(
                admitted,
                input.name(),
                &payload.uri,
                &payload.role,
                payload.size,
            )? {
                check_member_scope(admitted.snapshot_source.as_ref(), remote, &payload.uri)?;
                objects.push(reused);
            } else {
                let key = format!(
                    "job-snapshot-member/{}/{}",
                    request.operation,
                    sha256(payload.uri.as_bytes())
                );
                unresolved.push((payload, key));
            }
        }
        let mut to_copy = Vec::new();
        if !unresolved.is_empty() {
            let view = self.read_view(&request.dataset).await?;
            let rows = self.inner.lifecycle.rows(&view).await?;
            let existing_keys = unresolved
                .iter()
                .filter(|(_, key)| rows.contains_key(key))
                .map(|(_, key)| key.as_str())
                .collect::<Vec<_>>();
            let _decoded = rows.reserve_decoded(&self.inner.lifecycle.budget, &existing_keys)?;
            for (payload, key) in unresolved {
                if let Some((_, _, saved)) = rows.get_record::<SnapshotMember>(&key)? {
                    if saved.operation != request.operation
                        || saved.request_digest != admitted.request_digest
                        || saved.object.uri != payload.uri
                        || saved.object.role != payload.role
                        || saved.object.size != payload.size
                    {
                        return Err(mismatch());
                    }
                    objects.push(saved.object);
                } else {
                    to_copy.push((payload, key));
                }
            }
        }
        if !to_copy.is_empty() {
            let mut pending_iter = to_copy.into_iter().peekable();
            let mut active = futures::stream::FuturesUnordered::new();
            let mut completed_batch: Vec<(String, SnapshotMember)> = Vec::new();
            while pending_iter.peek().is_some() || !active.is_empty() {
                while active.len() < SNAPSHOT_MEMBER_CONCURRENCY {
                    let Some(&(payload, _)) = pending_iter.peek() else {
                        break;
                    };
                    let member_path = admitted
                        .directory
                        .join(format!("snapshot-{}", sha256(payload.uri.as_bytes())));
                    let prepared = prepare_member_copy(
                        admitted,
                        remote,
                        MemberCopy {
                            uri: &payload.uri,
                            role: &payload.role,
                            size: payload.size,
                            sha: None,
                        },
                        member_path,
                        budget,
                    )?;
                    match budget.reserve(prepared.reserve_bytes) {
                        Ok(permit) => {
                            let (_, key) = pending_iter.next().ok_or_else(mismatch)?;
                            active.push(async move {
                                let object = copy_member_admitted(
                                    self, admitted, remote, prepared, permit, false, budget,
                                )
                                .await?;
                                Ok::<(String, SnapshotMember), Error>((
                                    key,
                                    SnapshotMember {
                                        operation: request.operation.clone(),
                                        request_digest: admitted.request_digest.clone(),
                                        object,
                                    },
                                ))
                            });
                        }
                        Err(_) if !active.is_empty() => break,
                        Err(error) => {
                            if !completed_batch.is_empty() {
                                let _ = self
                                    .flush_snapshot_members(
                                        admitted,
                                        &mut completed_batch,
                                        &mut objects,
                                    )
                                    .await;
                            }
                            return Err(error);
                        }
                    }
                }
                let Some(first) = active.next().await else {
                    break;
                };
                match first {
                    Ok(item) => completed_batch.push(item),
                    Err(error) => {
                        if !completed_batch.is_empty() {
                            let _ = self
                                .flush_snapshot_members(
                                    admitted,
                                    &mut completed_batch,
                                    &mut objects,
                                )
                                .await;
                        }
                        return Err(error);
                    }
                }
                while let std::task::Poll::Ready(Some(next_done)) = futures::poll!(active.next()) {
                    match next_done {
                        Ok(item) => completed_batch.push(item),
                        Err(error) => {
                            if !completed_batch.is_empty() {
                                let _ = self
                                    .flush_snapshot_members(
                                        admitted,
                                        &mut completed_batch,
                                        &mut objects,
                                    )
                                    .await;
                            }
                            return Err(error);
                        }
                    }
                }
                if completed_batch.len() >= SNAPSHOT_MEMBER_CONCURRENCY
                    || (pending_iter.peek().is_none() && active.is_empty())
                {
                    if let Err(error) = self
                        .flush_snapshot_members(admitted, &mut completed_batch, &mut objects)
                        .await
                    {
                        if error.code() == "capacity" && !active.is_empty() {
                            while let Some(next_done) = active.next().await {
                                completed_batch.push(next_done?);
                            }
                            self.flush_snapshot_members(
                                admitted,
                                &mut completed_batch,
                                &mut objects,
                            )
                            .await?;
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
            if !completed_batch.is_empty() {
                self.flush_snapshot_members(admitted, &mut completed_batch, &mut objects)
                    .await?;
            }
        }
        objects.sort_by(|left, right| left.uri.cmp(&right.uri));
        let value = SnapshotCustody {
            profile: CUSTODY.into(),
            scope: "selected_snapshot".into(),
            metadata_sha256: binding.metadata_sha256.clone(),
            metadata_size: binding.metadata_size,
            objects,
        };
        let memory = self.request_memory(&value)?;
        Ok(Held {
            value,
            _memory: memory,
        })
    }

    async fn plan_job_snapshot(
        &self,
        admitted: &Admission,
        input: &JobInput,
        binding: &JobSnapshotInput,
        budget: &Budget,
    ) -> Result<Held<SnapshotPlan>> {
        // Bound cumulative native manifest decoding and the closure/path vectors.
        let memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(4))?;
        let selected = selection(input)?;
        let remote = storage::is_remote_uri(&selected.metadata_location);
        if binding.metadata_size > budget.limits.max_metadata_bytes as u64 {
            return Err(capacity());
        }
        let metadata = self
            .snapshot_member(
                admitted,
                remote,
                MemberCopy {
                    uri: &selected.metadata_location,
                    role: "metadata",
                    size: binding.metadata_size,
                    sha: Some(&binding.metadata_sha256),
                },
                budget,
            )
            .await?;
        let mut objects = vec![metadata];
        let io = crate::storage::custody::retained_io(paths(&objects)?, budget, None)?;
        let (table, _metadata_memory) =
            crate::storage::open_snapshot_with_io_admitted(&selected, io, budget).await?;
        let mut payloads = BTreeMap::<String, Payload>::new();
        let mut metadata_bytes = binding.metadata_size;
        if let Some(snapshot) = table.metadata().current_snapshot() {
            if snapshot.schema(table.metadata())?.schema_id() != selected.schema_id {
                return Err(Error::refusal(
                    "unsupported_snapshot_schema",
                    "retained snapshot profile requires snapshot and selected current schema identities to agree",
                ));
            }
            let list = self
                .snapshot_metadata(
                    admitted,
                    remote,
                    snapshot.manifest_list(),
                    "manifest_list",
                    &mut metadata_bytes,
                    budget,
                )
                .await?;
            objects.push(list);
            let list_io = crate::storage::custody::retained_io(paths(&objects)?, budget, None)?;
            let read_table = iceberg::table::Table::builder()
                .metadata(table.metadata_ref())
                .metadata_location(&selected.metadata_location)
                .identifier(iceberg::TableIdent::from_strs(["retained", "snapshot"])?)
                .runtime(iceberg::Runtime::try_current()?)
                .file_io(list_io)
                .readonly(true)
                .disable_cache()
                .build()?;
            let list = read_table.manifest_list_reader(snapshot).load().await?;
            let mut seen = BTreeSet::new();
            let mut descriptor_bytes = 0usize;
            for entry in list.entries() {
                if !seen.insert(entry.manifest_path.clone()) {
                    return Err(mismatch());
                }
                let expected_size = u64::try_from(entry.manifest_length).map_err(|_| mismatch())?;
                let manifest = if let Some(reused) = donor_member(
                    admitted,
                    input.name(),
                    &entry.manifest_path,
                    "manifest",
                    expected_size,
                )? {
                    check_member_scope(
                        admitted.snapshot_source.as_ref(),
                        remote,
                        &entry.manifest_path,
                    )?;
                    metadata_bytes = metadata_bytes
                        .checked_add(reused.size)
                        .filter(|total| *total <= budget.limits.max_metadata_bytes as u64)
                        .ok_or_else(capacity)?;
                    reused
                } else {
                    let copied = self
                        .snapshot_metadata(
                            admitted,
                            remote,
                            &entry.manifest_path,
                            "manifest",
                            &mut metadata_bytes,
                            budget,
                        )
                        .await?;
                    if copied.size != expected_size {
                        return Err(mismatch());
                    }
                    copied
                };
                objects.push(manifest);
                let io = crate::storage::custody::retained_io(paths(&objects)?, budget, None)?;
                let manifest = entry.load_manifest(&io).await?;
                for item in manifest.entries().iter().filter(|item| item.is_alive()) {
                    let file = item.data_file();
                    if file.file_format() != iceberg::spec::DataFileFormat::Parquet {
                        return Err(Error::refusal(
                            "unsupported_snapshot_file",
                            "retained native profile reads Parquet data and delete files only",
                        ));
                    }
                    let role = match file.content_type() {
                        DataContentType::Data => "data",
                        DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                            return Err(Error::refusal(
                                "unsupported_snapshot_deletes",
                                "retained delete-file consumption needs independent native reader qualification",
                            ));
                        }
                    };
                    if objects.iter().any(|object| object.uri == file.file_path()) {
                        return Err(mismatch());
                    }
                    check_member_scope(
                        admitted.snapshot_source.as_ref(),
                        remote,
                        file.file_path(),
                    )?;
                    let payload = Payload {
                        uri: file.file_path().into(),
                        role: role.into(),
                        size: file.file_size_in_bytes(),
                    };
                    if let Some(prior) = payloads.get(&payload.uri) {
                        if prior.size != payload.size || prior.role != payload.role {
                            return Err(mismatch());
                        }
                    } else {
                        descriptor_bytes = descriptor_bytes
                            .checked_add(super::super::inputs::metadata_size(
                                &payload,
                                budget.limits.max_metadata_bytes,
                            )?)
                            .ok_or_else(capacity)?;
                        payloads.insert(payload.uri.clone(), payload);
                    }
                    if descriptor_bytes.saturating_add(metadata_bytes as usize)
                        > budget.limits.max_metadata_bytes
                    {
                        return Err(capacity());
                    }
                }
            }
        }
        Ok(Held {
            _memory: memory,
            value: SnapshotPlan {
                operation: admitted.request.operation.clone(),
                request_digest: admitted.request_digest.clone(),
                selector: input.clone(),
                metadata_sha256: binding.metadata_sha256.clone(),
                metadata_size: binding.metadata_size,
                metadata: objects,
                payloads: payloads.into_values().collect(),
            },
        })
    }

    async fn snapshot_metadata(
        &self,
        admitted: &Admission,
        remote: bool,
        uri: &str,
        role: &str,
        total: &mut u64,
        budget: &Budget,
    ) -> Result<SnapshotObject> {
        let key = format!(
            "job-snapshot-member/{}/{}",
            admitted.request.operation,
            sha256(uri.as_bytes())
        );
        let size = if let Some(saved) = self
            .job_record::<SnapshotMember>(&admitted.request.dataset, &key)
            .await?
        {
            if saved.object.uri != uri || saved.object.role != role {
                return Err(mismatch());
            }
            saved.object.size
        } else {
            check_member_scope(admitted.snapshot_source.as_ref(), remote, uri)?;
            self.snapshot_source_size(admitted, uri, remote, budget)
                .await?
        };
        *total = total
            .checked_add(size)
            .filter(|total| *total <= budget.limits.max_metadata_bytes as u64)
            .ok_or_else(capacity)?;
        self.snapshot_member(
            admitted,
            remote,
            MemberCopy {
                uri,
                role,
                size,
                sha: None,
            },
            budget,
        )
        .await
    }

    async fn snapshot_member(
        &self,
        admitted: &Admission,
        remote: bool,
        member: MemberCopy<'_>,
        budget: &Budget,
    ) -> Result<SnapshotObject> {
        let MemberCopy {
            uri,
            role,
            size,
            sha: expected_sha,
        } = member;
        let request = &admitted.request;
        let key = format!(
            "job-snapshot-member/{}/{}",
            request.operation,
            sha256(uri.as_bytes())
        );
        if let Some(saved) = self
            .job_record::<SnapshotMember>(&request.dataset, &key)
            .await?
        {
            if saved.operation != request.operation
                || saved.request_digest != admitted.request_digest
                || saved.object.uri != uri
                || saved.object.role != role
                || saved.object.size != size
                || expected_sha.is_some_and(|expected| expected != saved.object.sha256)
            {
                return Err(mismatch());
            }
            return Ok(saved.value.object);
        }
        let member_path = admitted
            .directory
            .join(format!("snapshot-{}", sha256(uri.as_bytes())));
        let prepared = prepare_member_copy(admitted, remote, member, member_path, budget)?;
        let permit = budget.reserve(prepared.reserve_bytes)?;
        let object =
            copy_member_admitted(self, admitted, remote, prepared, permit, true, budget).await?;
        self.commit_job_record(
            &request.dataset,
            &key,
            &SnapshotMember {
                operation: request.operation.clone(),
                request_digest: admitted.request_digest.clone(),
                object: object.clone(),
            },
        )
        .await?;
        Ok(object)
    }

    async fn flush_snapshot_members(
        &self,
        admitted: &Admission,
        completed_batch: &mut Vec<(String, SnapshotMember)>,
        objects: &mut Vec<SnapshotObject>,
    ) -> Result<()> {
        if completed_batch.is_empty() {
            return Ok(());
        }
        File::open(&admitted.directory)?.sync_all()?;
        let entries = completed_batch
            .iter()
            .map(|(key, member)| (key.as_str(), member))
            .collect::<Vec<_>>();
        self.commit_job_records(&admitted.request.dataset, &entries)
            .await?;
        for (_, member) in completed_batch.drain(..) {
            objects.push(member.object);
        }
        Ok(())
    }
}

fn donor_has_uri(admitted: &Admission, input_name: &str, uri: &str) -> bool {
    admitted
        .input_donors
        .get(input_name)
        .and_then(|donor| donor.snapshot_custody.as_ref())
        .is_some_and(|custody| {
            custody
                .objects
                .binary_search_by(|object| object.uri.as_str().cmp(uri))
                .is_ok()
        })
}

fn donor_member(
    admitted: &Admission,
    input_name: &str,
    uri: &str,
    role: &str,
    expected_size: u64,
) -> Result<Option<SnapshotObject>> {
    let Some(donor) = admitted.input_donors.get(input_name) else {
        return Ok(None);
    };
    let custody = donor.snapshot_custody.as_ref().ok_or_else(mismatch)?;
    let Ok(index) = custody
        .objects
        .binary_search_by(|object| object.uri.as_str().cmp(uri))
    else {
        return Ok(None);
    };
    let object = &custody.objects[index];
    if object.role != role
        || object.size != expected_size
        || object.retained.bytes != expected_size
        || !valid_sha256(&object.sha256)
    {
        return Err(mismatch());
    }
    let meta = std::fs::symlink_metadata(&object.retained.path)?;
    if !meta.file_type().is_file() || meta.len() != expected_size {
        return Err(mismatch());
    }
    Ok(Some(object.clone()))
}

fn paths(objects: &[SnapshotObject]) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for object in objects {
        if result
            .insert(object.uri.clone(), object.retained.path.clone())
            .is_some()
        {
            return Err(mismatch());
        }
    }
    Ok(result)
}

fn source_unavailable() -> Error {
    Error::refusal(
        "snapshot_source_unavailable",
        "S3 snapshot input requires its admitted source configuration",
    )
}

fn check_member_scope(
    placement: Option<&SnapshotSourcePlacement>,
    remote: bool,
    uri: &str,
) -> Result<()> {
    if remote {
        let placement = placement.ok_or_else(source_unavailable)?;
        #[cfg(feature = "remote")]
        return storage::remote::SnapshotSource::check_scope(&placement.prefix, uri).map_err(
            |_| {
                Error::refusal(
                    "snapshot_source_scope",
                    "snapshot member is outside its admitted S3 source scope",
                )
            },
        );
        #[cfg(not(feature = "remote"))]
        {
            let _ = placement;
            return Err(source_unavailable());
        }
    }
    if Path::new(uri).is_absolute() || uri.starts_with("file://") {
        Ok(())
    } else {
        Err(Error::refusal(
            "snapshot_source_scope",
            "local snapshot member requires an exact local location",
        ))
    }
}

impl Engine {
    async fn snapshot_source_size(
        &self,
        admitted: &Admission,
        uri: &str,
        remote: bool,
        budget: &Budget,
    ) -> Result<u64> {
        #[cfg(not(feature = "remote"))]
        let _ = (admitted, budget);
        if remote {
            #[cfg(feature = "remote")]
            return self.snapshot_source_for(admitted)?.size(uri, budget).await;
            #[cfg(not(feature = "remote"))]
            return Err(source_unavailable());
        }
        Ok(FileIO::new_with_fs().new_input(uri)?.metadata().await?.size)
    }
}

struct PreparedMemberCopy<'a> {
    member: MemberCopy<'a>,
    path: PathBuf,
    present: bool,
    chunk_cap: usize,
    chunk: usize,
    reserve_bytes: usize,
}

fn prepare_member_copy<'a>(
    admitted: &Admission,
    remote: bool,
    member: MemberCopy<'a>,
    member_path: PathBuf,
    budget: &Budget,
) -> Result<PreparedMemberCopy<'a>> {
    check_member_scope(admitted.snapshot_source.as_ref(), remote, member.uri)?;
    let present = member_path.try_exists()?;
    if present
        && std::fs::symlink_metadata(&member_path)?
            .file_type()
            .is_symlink()
    {
        return Err(mismatch());
    }
    let chunk_cap = budget
        .limits
        .max_batch_bytes
        .min(storage::SNAPSHOT_MEMBER_CHUNK_BYTES);
    if chunk_cap == 0 && member.size > 0 {
        return Err(capacity());
    }
    let chunk = member.size.min(chunk_cap as u64) as usize;
    let transport = if !present && remote {
        #[cfg(feature = "remote")]
        {
            storage::remote::SnapshotSource::TRANSPORT_ADMISSION
        }
        #[cfg(not(feature = "remote"))]
        {
            return Err(source_unavailable());
        }
    } else {
        0
    };
    let reserve_bytes = chunk.checked_add(transport).ok_or_else(capacity)?;
    Ok(PreparedMemberCopy {
        member,
        path: member_path,
        present,
        chunk_cap,
        chunk,
        reserve_bytes,
    })
}

async fn copy_member_admitted(
    engine: &Engine,
    admitted: &Admission,
    remote: bool,
    prepared: PreparedMemberCopy<'_>,
    permit: tokio::sync::OwnedSemaphorePermit,
    sync_parent: bool,
    budget: &Budget,
) -> Result<SnapshotObject> {
    #[cfg(not(feature = "remote"))]
    let _ = engine;
    let MemberCopy {
        uri,
        role,
        size,
        sha: expected_sha,
    } = prepared.member;
    let present = prepared.present;
    let reader = if !present && remote {
        #[cfg(feature = "remote")]
        {
            let source = engine.snapshot_source_for(admitted)?;
            if size == 0 && source.size_admitted(uri, &permit).await? != 0 {
                return Err(mismatch());
            }
            SnapshotReader::S3(source.clone())
        }
        #[cfg(not(feature = "remote"))]
        return Err(source_unavailable());
    } else {
        let input = if present {
            FileIO::new_with_fs().new_input(prepared.path.to_string_lossy())?
        } else {
            FileIO::new_with_fs().new_input(uri)?
        };
        if input.metadata().await?.size != size {
            return Err(mismatch());
        }
        SnapshotReader::Local {
            reader: input.reader().await?,
            input,
        }
    };
    let temporary = if present {
        None
    } else {
        Some(
            budget
                .runtime_env()
                .disk_manager
                .create_tmp_file("retained native member")?,
        )
    };
    let mut output = temporary
        .as_ref()
        .map(|file| budget.scratch_writer(file))
        .transpose()?;
    #[cfg(feature = "remote")]
    let mut destination = match &reader {
        SnapshotReader::S3(_) => vec![0; prepared.chunk],
        SnapshotReader::Local { .. } => Vec::new(),
    };
    let chunk = prepared.chunk_cap as u64;
    let mut sha = Sha256::new();
    let mut digest = blake3::Hasher::new();
    let mut start = 0;
    while start < size {
        let end = start.saturating_add(chunk).min(size);
        let local;
        let bytes: &[u8] = match &reader {
            SnapshotReader::Local { reader, .. } => {
                local = reader.read(start..end).await?;
                &local
            }
            #[cfg(feature = "remote")]
            SnapshotReader::S3(source) => {
                let slice = &mut destination[..(end - start) as usize];
                source
                    .read_range_admitted(uri, size, start..end, slice, prepared.chunk_cap, &permit)
                    .await?;
                slice
            }
        };
        if bytes.len() as u64 != end - start {
            return Err(mismatch());
        }
        if let Some(output) = &mut output {
            output.write_all(bytes)?;
        }
        sha.update(bytes);
        digest.update(bytes);
        start = end;
    }
    #[cfg(feature = "remote")]
    drop(destination);
    drop(permit);
    let sha256 = format!("{:x}", sha.finalize());
    if expected_sha.is_some_and(|expected| expected != sha256) {
        return Err(mismatch());
    }
    if let SnapshotReader::Local { input, .. } = &reader
        && input.metadata().await?.size != size
    {
        return Err(mismatch());
    }
    drop(output);
    if let Some(temporary) = temporary {
        temporary.inner().as_file().sync_all()?;
        std::fs::hard_link(temporary.path(), &prepared.path)?;
        budget.retain_temporary(temporary)?;
    }
    if sync_parent {
        File::open(prepared.path.parent().ok_or_else(mismatch)?)?.sync_all()?;
    }
    Ok(SnapshotObject {
        origin_operation: admitted.request.operation.clone(),
        uri: uri.into(),
        role: role.into(),
        sha256,
        size,
        retained: ObjectRef {
            owner: admitted.request.dataset.clone(),
            path: prepared.path.to_string_lossy().into_owned(),
            digest: digest.finalize().to_hex().to_string(),
            bytes: size,
        },
    })
}

struct Claim<'a> {
    uri: &'a str,
    role: &'a str,
    sha: Option<&'a str>,
    size: u64,
    inherited: bool,
}

fn add_claim(
    unique: &mut BTreeMap<String, (u64, String, Option<String>, bool)>,
    memory: &mut tokio::sync::OwnedSemaphorePermit,
    budget: &Budget,
    claim: Claim<'_>,
) -> Result<()> {
    if let Some((size, role, sha, inherited)) = unique.get(claim.uri) {
        if *size != claim.size
            || role != claim.role
            || sha
                .as_deref()
                .zip(claim.sha)
                .is_some_and(|(old, new)| old != new)
        {
            return Err(mismatch());
        }
        if *inherited != claim.inherited {
            return Err(Error::refusal(
                "unsupported_mixed_custody",
                "shared native inputs must consistently request the same admitted custody",
            ));
        }
        return Ok(());
    }
    memory.merge(
        budget.reserve(
            claim
                .uri
                .len()
                .saturating_add(claim.role.len())
                .saturating_add(claim.sha.map_or(0, str::len))
                .saturating_add(size_of::<(String, u64)>())
                .saturating_mul(8),
        )?,
    );
    unique.insert(
        claim.uri.into(),
        (
            claim.size,
            claim.role.into(),
            claim.sha.map(str::to_owned),
            claim.inherited,
        ),
    );
    Ok(())
}
