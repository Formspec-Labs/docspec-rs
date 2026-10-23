//! Standalone and routine release/table verification and object availability checks.

use super::{backfill, evidence, refresh, schema, supported_format, verification};
use crate::{
    Error, Result,
    budget::{Budget, Limits},
    model::*,
    storage,
};
use futures::TryStreamExt;
use std::sync::Arc;

/// Verifies exact retained objects and decodes a native result without its catalog.
///
/// The caller retains the files for the check; this does not acquire a host reader.
///
/// # Errors
/// Refuses changed bytes, invalid schemas, missing files, or row-count disagreement.
pub async fn verify_table(pin: &TablePin, budget: &Budget) -> Result<()> {
    #[cfg(feature = "test-util")]
    eprintln!("DOCSPEC_TEST_DEEP_TABLE");
    storage::verify_objects(pin.objects.clone(), budget).await?;
    let table = storage::open_pin(pin, budget).await?;
    storage::check_inventory(pin, &table, budget).await?;
    let mut rows = 0;
    if pin.snapshot.is_some() {
        let stream = storage::read_batches(&table, budget).await?;
        futures::pin_mut!(stream);
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows() as u64;
        }
    }
    if rows != pin.rows {
        return Err(Error::refusal(
            "row_count_mismatch",
            "independent native reader disagrees with committed writer count",
        ));
    }
    Ok(())
}

/// Independently verifies a serialized release and all retained data and evidence.
///
/// The caller retains the files or relies on a published availability window.
/// A serialized pin does not extend that availability promise.
///
/// # Errors
/// Refuses modified or missing objects and inconsistent native metadata.
pub async fn verify_release(release: &ReleasePin, limits: Limits) -> Result<()> {
    verify_release_at(release, limits, VerificationLevel::Deep)
        .await
        .map(|_| ())
}

/// Checks a release at the requested scope, reporting the bounded sample.
///
/// Routine checks read metadata and at most three files' first eight rows across
/// the request. Deep checks also hash payloads and reconstruct all derived results.
/// The caller retains the files for the check; this does not acquire a host reader.
///
/// # Errors
/// Refuses inconsistent references, missing objects, failed checks or exhausted capacity.
pub async fn verify_release_at(
    release: &ReleasePin,
    limits: Limits,
    level: VerificationLevel,
) -> Result<VerificationReport> {
    let budget = Budget::new(limits)?;
    #[cfg(feature = "remote")]
    let objects = storage::remote::RemoteObjects::for_uri(&release.manifest.path)
        .map_or(storage::Objects::Local, storage::Objects::Remote);
    #[cfg(not(feature = "remote"))]
    let objects = storage::Objects::Local;
    let mut checks = verification::Checks::with_objects(level, objects);
    verification::verify_all(&[release], &budget, &mut checks).await?;
    Ok(checks.report)
}

pub(in crate::engine) fn release_objects(release: &ReleasePin) -> Vec<ObjectRef> {
    let dataset = &release.dataset;
    let mut objects = vec![release.manifest.clone()];
    objects.extend(dataset.parent_manifest.iter().cloned());
    #[cfg(any(test, feature = "test-util"))]
    objects.extend(dataset.inputs.values().cloned());
    #[cfg(all(not(test), not(feature = "test-util")))]
    if !(dataset.dataset.as_str().starts_with("catalog-")
        || dataset.dataset.as_str().starts_with("raw-")
        || dataset.dataset.as_str().starts_with("obs-")
        || dataset.dataset.as_str().starts_with("source-observations-"))
        || !matches!(&dataset.request, OperationRequest::Build(build) if build.base.is_none())
    {
        objects.extend(dataset.inputs.values().cloned());
    }
    if let Some(related) = &dataset.related {
        objects.extend(
            related
                .coverage
                .inputs
                .values()
                .map(|version| version.manifest.clone()),
        );
        objects.extend(related.evidence.objects.iter().cloned());
        objects.extend(
            related
                .input_sources
                .values()
                .flat_map(|source| source.objects.iter().cloned()),
        );
    }
    objects.extend(
        dataset
            .groups
            .values()
            .flat_map(|pin| pin.objects.iter().cloned()),
    );
    objects.extend(
        dataset
            .computed
            .values()
            .map(|group| group.base_manifest.clone()),
    );
    objects.extend(
        dataset
            .computed
            .values()
            .filter_map(|group| group.origins.clone()),
    );
    if let Some(evidence) = &dataset.evidence {
        objects.push(evidence.source.metadata.clone());
        objects.extend(evidence.table.objects.iter().cloned());
    }
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    objects.dedup();
    objects
}

pub(in crate::engine) async fn check_release_available(
    objects: &storage::Objects,
    release: &ReleasePin,
    budget: &Budget,
) -> Result<()> {
    check_release_available_with_checks(
        objects,
        release,
        budget,
        &mut verification::Checks::composition(),
    )
    .await
}

pub(in crate::engine) async fn check_release_available_with_checks(
    objects: &storage::Objects,
    release: &ReleasePin,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    if release.dataset.related.is_some() {
        verification::check_available(release, budget).await
    } else {
        objects
            .check_available(release_objects(release), budget)
            .await?;
        schema::check_available(&release.dataset, budget).await?;
        backfill::verify_binding_dependencies(&release.dataset, budget, checks).await
    }
}

pub(in crate::engine) async fn resolve_release(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<()> {
    let objects = storage::Objects::for_path(&release.manifest.path);
    resolve_release_with_objects(&objects, release, budget).await
}

pub(in crate::engine) async fn resolve_release_with_objects(
    objects: &storage::Objects,
    release: &ReleasePin,
    budget: &Budget,
) -> Result<()> {
    // Resolve the immutable selection only. Payloads and dependencies are checked
    // at creation, or by explicit verification; a read opens its requested data.
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let decoded = refresh::read_release_with_objects(
        objects,
        &release.version_ref(),
        budget,
        &mut 0,
        &memory,
    )
    .await?;
    if decoded.dataset != release.dataset || !release.verified {
        return Err(Error::refusal(
            "release_mismatch",
            "release differs from its completed selection",
        ));
    }
    Ok(())
}

pub(in crate::engine) async fn verify_local_release(
    release: &ReleasePin,
    budget: &Budget,
    checks: &mut verification::Checks,
) -> Result<()> {
    let decoded = &release.dataset;
    if !supported_format(decoded.format)
        || release.id != decoded.version
        || decoded.request.dataset() != &decoded.dataset
        || decoded.request.operation() != &decoded.version
        || decoded.request.result_definition() != decoded.definition
        || decoded.request.inputs().iter().any(|(name, input)| {
            let key = decoded.request.input_key(name);
            decoded
                .inputs
                .get(&key)
                .is_none_or(|retained| retained.digest != input.digest)
        })
    {
        return Err(Error::refusal(
            "release_mismatch",
            "release request, version, or input binding differs",
        ));
    }
    #[cfg(all(not(test), not(feature = "test-util")))]
    let check_inputs = !(decoded.dataset.as_str().starts_with("catalog-")
        || decoded.dataset.as_str().starts_with("raw-")
        || decoded.dataset.as_str().starts_with("obs-")
        || decoded.dataset.as_str().starts_with("source-observations-"))
        || !matches!(&decoded.request, OperationRequest::Build(build) if build.base.is_none());
    #[cfg(any(test, feature = "test-util"))]
    let check_inputs = true;
    if check_inputs {
        if checks.deep() {
            storage::verify_objects(decoded.inputs.values().cloned().collect(), budget).await?;
        } else if checks.inspects_tables() {
            checks
                .objects()
                .check_available(decoded.inputs.values().cloned().collect(), budget)
                .await?;
        }
    }
    let source = decoded
        .groups
        .get("source")
        .ok_or_else(|| Error::refusal("release_mismatch", "release has no source table"))?;
    checks.table(source, budget).await?;
    if checks.inspects_tables() || !decoded.partition_by.is_empty() {
        let native_source = checks.objects().open_pin(source, budget).await?;
        storage::check_partition_by_matches(
            native_source.metadata().current_schema(),
            native_source.metadata().default_partition_spec(),
            &decoded.partition_by,
        )
        .map_err(|_| {
            Error::refusal(
                "release_mismatch",
                "release partition_by does not match source table metadata",
            )
        })?;
        let declared = match &decoded.request {
            OperationRequest::Build(build) if !build.partition_by.is_empty() => {
                storage::check_partition_by_matches(
                    native_source.metadata().current_schema(),
                    native_source.metadata().default_partition_spec(),
                    &build.partition_by,
                )
                .map_err(|_| {
                    Error::refusal(
                        "release_mismatch",
                        "build request partition_by does not match source table metadata",
                    )
                })?;
                Some(build.partition_by.as_slice())
            }
            _ => Some(decoded.partition_by.as_slice()),
        };
        if decoded.partition_by
            != storage::partition_specs_from_metadata(native_source.metadata(), declared)
        {
            return Err(Error::refusal(
                "release_mismatch",
                "release partition_by does not match source table metadata",
            ));
        }
    }
    backfill::verify_groups(decoded, budget, checks).await?;
    evidence::verify(decoded, budget, checks).await?;
    Ok(())
}
