//! Exact producer generations and metadata-only related-input qualification.

use super::*;
use std::collections::BTreeSet;
use tokio::sync::OwnedMutexGuard;

impl Engine {
    /// Seals exact accepted producer versions once, without reading their payloads.
    ///
    /// An identical retry reconciles the original seal, even after later source
    /// invalidation. New seals require currently eligible, accepted members whose
    /// declarations all name this family and generation. Cleanup remains disabled.
    ///
    /// # Errors
    /// Refuses conflicting reseals, missing history, stale eligibility, mismatched
    /// producer declarations, invalid identities, or exhausted metadata capacity.
    pub async fn seal_generation(&self, seal: &GenerationSeal) -> Result<GenerationSeal> {
        let memory = self.request_memory(seal)?;
        let seal = seal.clone();
        self.admit_work(memory, move |engine| async move {
            engine.seal_generation_admitted(&seal).await
        })
        .await
    }

    async fn seal_generation_admitted(&self, seal: &GenerationSeal) -> Result<GenerationSeal> {
        let lifecycle = &self.inner.lifecycle;
        let memory = Arc::new(
            lifecycle
                .budget
                .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
        );
        metadata_size(seal, lifecycle.budget.limits.max_metadata_bytes)?;
        validate_seal(seal)?;
        let key = format!("generation/{}", seal.generation);
        let members: BTreeSet<_> = seal.members.keys().cloned().collect();
        // Reconcile an existing seal before reopening its metadata closure or
        // acquiring new protection. Acquisition then runs without consumer locks.
        {
            let lock = self.lock(&seal.family).await;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&seal.family).await?;
            if let Some(previous) = self.retained_seal(&view, seal, &members).await? {
                return Ok(previous);
            }
            if lifecycle
                .get::<BTreeSet<Id>>(&view, "family-members")
                .await?
                .is_some_and(|family| family != members)
            {
                return Err(Error::refusal(
                    "family_membership",
                    "a family retains its original member datasets; use a new family for a different set",
                ));
            }
        }
        let mut release_memory = lifecycle
            .budget
            .reserve(lifecycle.budget.limits.max_metadata_bytes.saturating_mul(8))?;
        let mut releases = Vec::with_capacity(seal.members.len());
        let mut release_bytes = 0usize;
        let expected = SourceRelationship::Coupled {
            family: seal.family.clone(),
            generation: seal.generation.clone(),
        };
        for version in seal.members.values() {
            let release = self
                .available_release_admitted(&version.dataset, &version.version)
                .await?;
            if release.version_ref() != *version || release.dataset.relationship != expected {
                return Err(invalid_relationship());
            }
            release_bytes = release_bytes.saturating_add(metadata_size(
                &release,
                lifecycle.budget.limits.max_metadata_bytes,
            )?);
            check_size(release_bytes, lifecycle.budget.limits.max_metadata_bytes)?;
            releases.push(release);
        }
        // Decoding is finished. Retain a size-based allowance for these pins
        // while the collector separately admits its metadata traversal.
        let retained_memory = release_memory
            .split(release_bytes.saturating_mul(4))
            .ok_or_else(capacity)?;
        drop(release_memory);
        let protections = self.protect_releases(&seal.family, &releases).await?;
        drop(releases);
        drop(retained_memory);
        let mut datasets: BTreeSet<_> = seal.members.keys().cloned().collect();
        datasets.insert(seal.family.clone());
        let _guards = self.lock_inputs(datasets).await;
        let view = lifecycle.open(&seal.family).await?;
        let family: Option<BTreeSet<Id>> = lifecycle.get(&view, "family-members").await?;
        if let Some(previous) = self.retained_seal(&view, seal, &members).await? {
            return Ok(previous);
        }
        if family.as_ref().is_some_and(|family| family != &members) {
            return Err(Error::refusal(
                "family_membership",
                "a family retains its original member datasets; use a new family for a different set",
            ));
        }
        let mut bytes = metadata_size(seal, lifecycle.budget.limits.max_metadata_bytes)?;
        for version in seal.members.values() {
            let accepted = self.qualify_input(version).await?;
            let declared = read_relationship(
                &self.inner.objects,
                version,
                &lifecycle.budget,
                &mut bytes,
                &memory,
            )
            .await?;
            if accepted != expected || declared != expected {
                return Err(invalid_relationship());
            }
        }
        let claim = record(&key, seal)?;
        let binding = protections.binding(&seal.family, &view, &key, &claim.1)?;
        let mut records = vec![claim, binding];
        if family.is_none() {
            records.push(record("family-members", members)?);
        }
        lifecycle.commit(&view, records).await?;
        Ok(seal.clone())
    }

    async fn retained_seal(
        &self,
        view: &crate::catalog::View,
        seal: &GenerationSeal,
        members: &BTreeSet<Id>,
    ) -> Result<Option<GenerationSeal>> {
        let lifecycle = &self.inner.lifecycle;
        let key = format!("generation/{}", seal.generation);
        let Some(previous) = lifecycle.get::<GenerationSeal>(view, &key).await? else {
            return Ok(None);
        };
        let family: Option<BTreeSet<Id>> = lifecycle.get(view, "family-members").await?;
        if previous != *seal || family.as_ref() != Some(members) {
            return Err(Error::refusal(
                "generation_conflict",
                "a sealed generation cannot change its exact members",
            ));
        }
        self.check_reference_binding(view, &key).await?;
        Ok(Some(previous))
    }

    /// Captures admissible accepted inputs and their exact producer seals.
    ///
    /// Reads bounded manifests and accepted history, never data payloads. Inputs
    /// may be historical but must remain eligible. A captured result requires a
    /// fresh guard at selection; this operation alone does not advance output.
    ///
    /// # Errors
    /// Refuses unknown or invalidated versions, unsealed or mixed generations,
    /// rebound identities, missing manifests, and exhausted metadata capacity.
    pub async fn capture_inputs(&self, inputs: &BTreeMap<Id, VersionRef>) -> Result<InputCoverage> {
        let memory = self.request_memory(inputs)?;
        let inputs = inputs.clone();
        self.admit_work(memory, move |engine| async move {
            engine.capture_inputs_admitted(&inputs, true).await
        })
        .await
    }

    pub(super) async fn capture_inputs_admitted(
        &self,
        inputs: &BTreeMap<Id, VersionRef>,
        require_available: bool,
    ) -> Result<InputCoverage> {
        Ok(self
            .capture_inputs_with_memory(inputs, require_available)
            .await?
            .0)
    }

    pub(super) async fn capture_inputs_with_memory(
        &self,
        inputs: &BTreeMap<Id, VersionRef>,
        require_available: bool,
    ) -> Result<(InputCoverage, tokio::sync::OwnedSemaphorePermit)> {
        let lifecycle = &self.inner.lifecycle;
        let budget = &lifecycle.budget;
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
        let mut bytes = metadata_size(inputs, budget.limits.max_metadata_bytes)?;
        validate_members(inputs)?;
        let mut relationships = BTreeMap::new();
        let mut datasets: BTreeSet<_> = inputs.keys().cloned().collect();
        // Immutable manifests establish the complete lock set before acquisition.
        // Acquiring family locks after source locks would deadlock with sealing.
        for (dataset, version) in inputs {
            let relationship =
                read_relationship(&self.inner.objects, version, budget, &mut bytes, &memory)
                    .await?;
            if let SourceRelationship::Coupled { family, .. } = &relationship {
                datasets.insert(family.clone());
            }
            relationships.insert(dataset.clone(), relationship);
        }
        let _guards = self.lock_inputs(datasets).await;
        let mut generations = BTreeMap::new();
        let mut families = BTreeMap::new();
        for (dataset, version) in inputs {
            if require_available {
                let view = lifecycle.open(dataset).await?;
                self.check_operation_available(&view, &version.version)
                    .await?;
            }
            if self.qualify_input(version).await? != relationships[dataset] {
                return Err(invalid_relationship());
            }
            if let SourceRelationship::Coupled { family, generation } = &relationships[dataset]
                && !generations.contains_key(family)
            {
                let view = lifecycle.open(family).await?;
                let seal: GenerationSeal = lifecycle
                    .get(&view, &format!("generation/{generation}"))
                    .await?
                    .ok_or_else(|| {
                        Error::refusal(
                            "unsealed_generation",
                            "seal exact producer members before consuming this generation",
                        )
                    })?;
                let members: BTreeSet<Id> = lifecycle
                    .get(&view, "family-members")
                    .await?
                    .ok_or_else(invalid_relationship)?;
                bytes = bytes.saturating_add(metadata_size(
                    &(&seal, &members),
                    budget.limits.max_metadata_bytes,
                )?);
                check_size(bytes, budget.limits.max_metadata_bytes)?;
                families.insert(family.clone(), members);
                generations.insert(family.clone(), seal);
            }
        }
        let coverage = InputCoverage {
            inputs: inputs.clone(),
            families,
            generations,
        };
        check_relationships(&coverage, &relationships)?;
        let bytes = metadata_size(&coverage, budget.limits.max_metadata_bytes)?
            .saturating_mul(4)
            .saturating_add(4096);
        // Transfer the capture's existing allowance instead of making the
        // caller reserve another maximum while capture is still decoding.
        let mut memory = Arc::try_unwrap(memory).map_err(|_| capacity())?;
        if bytes > memory.num_permits() {
            memory.merge(budget.reserve(bytes - memory.num_permits())?);
        } else {
            drop(memory.split(memory.num_permits() - bytes));
        }
        Ok((coverage, memory))
    }

    pub(super) async fn lock_inputs(&self, datasets: BTreeSet<Id>) -> Vec<OwnedMutexGuard<()>> {
        let mut guards = Vec::with_capacity(datasets.len());
        for dataset in datasets {
            guards.push(self.lock(&dataset).await.lock_owned().await);
        }
        guards
    }

    // Caller holds this dataset's lock, together with any other input locks.
    pub(super) async fn qualify_input(&self, version: &VersionRef) -> Result<SourceRelationship> {
        let lifecycle = &self.inner.lifecycle;
        let view = lifecycle.open(&version.dataset).await?;
        self.follows(&view, version, version).await?;
        let release = self
            .resolve_release(&view, &version.dataset, &version.version)
            .await?;
        let authority: Authority = lifecycle.get(&view, "authority").await?.unwrap_or_default();
        if release.version_ref() != *version || !supported_format(release.dataset.format) {
            return Err(invalid_relationship());
        }
        if release.dataset.eligibility != authority.eligibility {
            return Err(Error::refusal(
                "stale_eligibility",
                "input requires requalification under its producer's current eligibility",
            ));
        }
        Ok(release.dataset.relationship)
    }
}

/// Checks retained input relationships without consulting a producer catalog.
///
/// Verifies manifest bytes, exact identities and the retained seals' relationship
/// rules. This metadata check does not verify data payloads, current eligibility,
/// acquisition completeness, or authenticate who issued a supplied coverage record.
///
/// # Errors
/// Refuses altered manifests, incomplete or conflicting seals, mixed producer
/// generations, and metadata exceeding the configured allowance.
pub async fn verify_input_coverage(coverage: &InputCoverage, limits: Limits) -> Result<()> {
    let budget = Budget::new(limits)?;
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut bytes = metadata_size(coverage, budget.limits.max_metadata_bytes)?;
    validate_members(&coverage.inputs)?;
    let mut relationships = BTreeMap::new();
    for (dataset, version) in &coverage.inputs {
        relationships.insert(
            dataset.clone(),
            read_relationship(
                &storage::Objects::Local,
                version,
                &budget,
                &mut bytes,
                &memory,
            )
            .await?,
        );
    }
    check_relationships(coverage, &relationships)
}

pub(super) fn check_relationships(
    coverage: &InputCoverage,
    relationships: &BTreeMap<Id, SourceRelationship>,
) -> Result<()> {
    let mut required = BTreeSet::new();
    for (dataset, relationship) in relationships {
        if let SourceRelationship::Coupled { family, generation } = relationship {
            required.insert(family);
            let seal = coverage
                .generations
                .get(family)
                .ok_or_else(invalid_relationship)?;
            if &seal.family != family
                || &seal.generation != generation
                || seal.members.get(dataset) != coverage.inputs.get(dataset)
            {
                return Err(invalid_relationship());
            }
        }
    }
    if required.len() != coverage.generations.len()
        || !coverage.families.keys().eq(coverage.generations.keys())
    {
        return Err(invalid_relationship());
    }
    for seal in coverage.generations.values() {
        validate_seal(seal)?;
        if !coverage
            .families
            .get(&seal.family)
            .is_some_and(|members| members.iter().eq(seal.members.keys()))
        {
            return Err(invalid_relationship());
        }
        // A current member's rule also constrains an older consumed member whose
        // own declaration was Independent or belonged to a different family.
        for (dataset, member) in &seal.members {
            if let Some(consumed) = coverage.inputs.get(dataset)
                && (consumed != member
                    || relationships.get(dataset)
                        != Some(&SourceRelationship::Coupled {
                            family: seal.family.clone(),
                            generation: seal.generation.clone(),
                        }))
            {
                return Err(invalid_relationship());
            }
        }
    }
    Ok(())
}

fn validate_seal(seal: &GenerationSeal) -> Result<()> {
    validate_members(&seal.members)
}

fn validate_members(members: &BTreeMap<Id, VersionRef>) -> Result<()> {
    if members.is_empty()
        || members.iter().any(|(dataset, version)| {
            dataset != &version.dataset || &version.manifest.owner != dataset
        })
    {
        return Err(invalid_relationship());
    }
    Ok(())
}

// Ignore payload inventories when reading relationship declarations. Hashing the
// whole bounded manifest still binds this header to its exact immutable bytes.
#[derive(Deserialize)]
struct InputHeader {
    format: u32,
    dataset: Id,
    version: Id,
    relationship: SourceRelationship,
}

async fn read_relationship(
    objects: &storage::Objects,
    version: &VersionRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<SourceRelationship> {
    let header: InputHeader =
        read_manifest(objects, &version.manifest, budget, bytes, memory).await?;
    if !supported_format(header.format)
        || header.dataset != version.dataset
        || header.version != version.version
    {
        return Err(invalid_relationship());
    }
    Ok(header.relationship)
}

pub(super) async fn read_manifest<T: serde::de::DeserializeOwned + Send + 'static>(
    objects: &storage::Objects,
    object: &ObjectRef,
    budget: &Budget,
    bytes: &mut usize,
    memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<T> {
    let size = usize::try_from(object.bytes).map_err(|_| capacity())?;
    *bytes = bytes.saturating_add(size);
    check_size(*bytes, budget.limits.max_metadata_bytes)?;
    let object = object.clone();
    let memory = memory.clone();
    let buffer = budget.reserve(size.saturating_add(objects.metadata_read_allowance()))?;
    let objects = objects.clone();
    let handle = tokio::runtime::Handle::current();
    budget
        .blocking(move || {
            // Parsing can outlive an async caller's cancellation. Keep both
            // metadata and buffer/transport admission with the actual worker.
            let _memory = memory;
            let _buffer = buffer;
            let mut bytes = vec![0; size];
            objects.read_metadata_admitted(&object, &mut bytes, &handle)?;
            Ok(serde_json::from_slice(&bytes)?)
        })
        .await
}

pub(super) fn relationship(
    request: &OperationRequest,
    base: Option<&DatasetPin>,
) -> SourceRelationship {
    let explicit = match request {
        OperationRequest::Build(spec) => spec.relationship.as_ref(),
        OperationRequest::Refresh(spec) => spec.relationship.as_ref(),
        _ => None,
    };
    if let Some(relationship) = explicit {
        return relationship.clone();
    }
    base.map(|base| base.relationship.clone())
        .unwrap_or_default()
}

pub(super) fn check_rebuild(authority: &Authority, request: &OperationRequest) -> Result<()> {
    if let OperationRequest::Build(spec) = request
        && spec.base.is_none()
        && let Some(selected) = &authority.selected
        && (selected.relationship != SourceRelationship::Independent
            || spec
                .relationship
                .as_ref()
                .is_some_and(|relationship| relationship != &selected.relationship))
    {
        return Err(Error::refusal(
            "relationship_base",
            "provide the selected base to rebuild a coupled source or change its relationship",
        ));
    }
    Ok(())
}

pub(super) fn invalid_relationship() -> Error {
    Error::refusal(
        "invalid_relationship",
        "input identities or producer generation declarations disagree",
    )
}

// A counting sink bounds serialized metadata without allocating a second copy.
pub(super) fn metadata_size(value: &impl Serialize, limit: usize) -> Result<usize> {
    struct Counter {
        remaining: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("metadata capacity"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { remaining: limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| capacity())?;
    Ok(limit - counter.remaining)
}
fn check_size(bytes: usize, limit: usize) -> Result<()> {
    if bytes > limit {
        Err(capacity())
    } else {
        Ok(())
    }
}
fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "related-input metadata exceeds its combined allowance",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canceled_manifest_read_keeps_admission_until_its_blocking_work_exits() {
        // Queue the actual reader behind one controlled blocking job. Cancellation
        // cannot abort spawn_blocking work; its captured permits must survive.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = tempfile::TempDir::new().unwrap();
            let path = root.path().join("manifest.json");
            let dataset = Id::new("comments").unwrap();
            std::fs::write(&path, serde_json::to_vec(&serde_json::json!({
                "format": FORMAT, "dataset": dataset, "version": "first", "relationship": {"kind": "independent"}
            })).unwrap()).unwrap();
            let version = VersionRef { dataset: dataset.clone(), version: Id::new("first").unwrap(), manifest: storage::object(&dataset, &path).unwrap() };
            let size = version.manifest.bytes as usize;
            let limits = Limits { compute_threads: 1, memory_bytes: 8 << 20, max_metadata_bytes: 1 << 20, ..Limits::default() };
            let budget = Budget::new(limits).unwrap();
            let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap());
            let weak = Arc::downgrade(&memory);
            let (started, ready) = tokio::sync::oneshot::channel();
            let (resume, paused) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = paused.recv();
            });
            ready.await.unwrap();
            let reader_budget = budget.clone();
            let reader = tokio::spawn(async move {
                read_relationship(&storage::Objects::Local, &version, &reader_budget, &mut 0, &memory).await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while weak.strong_count() < 2 { tokio::task::yield_now().await; }
            }).await.unwrap();
            reader.abort();
            assert!(reader.await.unwrap_err().is_cancelled());
            assert_eq!(weak.strong_count(), 1, "queued native work owns the metadata reservation");
            let remaining = budget.limits.memory_bytes / 2 - budget.limits.max_metadata_bytes - (1 << 20) - size;
            let spare = budget.reserve(remaining).unwrap();
            assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
            drop(spare);
            resume.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let Ok(all) = budget.reserve(budget.limits.memory_bytes / 2) {
                        drop(all);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            assert_eq!(weak.strong_count(), 0);
        });
    }

    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn canceled_remote_manifest_read_keeps_worker_and_admission_through_decode() {
        use crate::storage::remote::objects::tests::ReadFixture;
        use std::sync::atomic::{AtomicBool, Ordering};

        // Test-local observation of deserialization after its awaiting caller exits.
        // No production state or test-only hook is added to the reader.
        static DECODED: AtomicBool = AtomicBool::new(false);
        #[derive(Debug)]
        struct Decoded;
        impl<'de> Deserialize<'de> for Decoded {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                let value = serde_json::Value::deserialize(deserializer)?;
                assert_eq!(
                    value["value"], 42,
                    "decode the bytes served by the held GET"
                );
                DECODED.store(true, Ordering::Release);
                Ok(Self)
            }
        }

        let payload = br#"{"value":42}"#.to_vec();
        let expected = ObjectRef {
            owner: Id::new("manifest-owner").unwrap(),
            path: "s3://bucket/owned/manifest.json".into(),
            digest: blake3::hash(&payload).to_hex().to_string(),
            bytes: payload.len() as u64,
        };
        let (fixture, received, release) = ReadFixture::held(payload);
        let objects = storage::Objects::Remote(fixture.objects());
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 16 << 20,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        })
        .unwrap();
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes).unwrap());
        let weak = Arc::downgrade(&memory);
        let worker_budget = budget.clone();
        let reader = tokio::spawn(async move {
            read_manifest::<Decoded>(&objects, &expected, &worker_budget, &mut 0, &memory).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        assert_eq!(
            weak.strong_count(),
            1,
            "the active worker owns metadata admission"
        );
        assert_eq!(
            budget
                .reserve(budget.limits.memory_bytes / 2)
                .unwrap_err()
                .code(),
            "capacity"
        );
        assert_eq!(
            budget
                .blocking_admitted(|| Ok(()))
                .await
                .unwrap_err()
                .code(),
            "capacity"
        );
        assert!(!DECODED.load(Ordering::Acquire));
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if DECODED.load(Ordering::Acquire)
                    && weak.strong_count() == 0
                    && let Ok(all) = budget.reserve(budget.limits.memory_bytes / 2)
                {
                    drop(all);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Worker-slot release happens just after closure return; await it separately.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match budget.blocking_admitted(|| Ok(())).await {
                    Ok(()) => break,
                    Err(error) if error.code() == "capacity" => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected worker error: {error}"),
                }
            }
        })
        .await
        .unwrap();
        let requests = fixture.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "GET")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "HEAD")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn canceled_partial_input_lock_acquisition_releases_prior_locks() {
        let root = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(root.path(), Limits::default()).await.unwrap();
        let first = Id::new("a").unwrap();
        let second = Id::new("b").unwrap();
        let first_lock = engine.lock(&first).await;
        let second_lock = engine.lock(&second).await;
        let held = second_lock.lock().await;
        let task = tokio::spawn(async move { engine.lock_inputs([first, second].into()).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while first_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(first_lock.try_lock().unwrap());
        drop(held);
        drop(second_lock.try_lock().unwrap());
    }
}
