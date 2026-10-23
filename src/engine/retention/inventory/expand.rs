//! Lifecycle record dispatch and external metadata expansion for retention inventory.

use super::{
    Expansion, Inventory, Pending, ProgressResult, Strict, capacity, decode, invalid_inventory,
    unknown_dependencies, validate_object, validate_selection,
};
use crate::{
    Result,
    budget::{Budget, Limits},
    engine::{
        CompletedGroups, Operation,
        backfill::{
            Capture, FileCompletion, PreparedCompletion, ReadbackCompletion, ValidationCompletion,
            origins::{OriginalDeclaration, ScalarOrigins},
        },
        changes::PreparedChanges,
        inputs, supported_format,
    },
    model::{
        Abandonment, AppliedChangeSet, Authority, ChangeCertificate, CompactionResult,
        CorrectionDecision, DatasetPin, DependencyQualification, GenerationSeal, Id,
        OperationRequest, PhaseResult, Protection, ReleasePin, RetentionBlocker, Succession,
        SuccessionProof,
    },
    storage,
};
use iceberg::{TableIdent, spec::TableMetadata, table::Table};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

#[derive(Serialize, Deserialize)]
struct Runtime {
    processor: String,
    #[serde(default)]
    package_version: Option<String>,
    dependency_lock_blake3: String,
    limits: Limits,
}

fn required_record<T: DeserializeOwned>(
    rows: &crate::catalog::SnapshotRows,
    key: &str,
) -> Result<T> {
    rows.get_record(key)?
        .map(|(_, _, value)| value)
        .ok_or_else(unknown_dependencies)
}

impl Inventory {
    pub(super) fn record(
        &mut self,
        key: &str,
        json: &str,
        rows: &crate::catalog::SnapshotRows,
        origin: &RetentionBlocker,
    ) -> Result<()> {
        // Most-specific families precede their common prefix.
        if key == "source-fence" {
            let fence: crate::engine::intake::SourceFence = decode(json)?;
            crate::engine::intake::validate_record(&fence)?;
            if crate::catalog::lifecycle_ident(&fence.dataset).to_string() != origin.table {
                return Err(invalid_inventory("source fence dataset differs"));
            }
        } else if key.starts_with("prepared-changes/") {
            let prepared: PreparedChanges = decode(json)?;
            crate::engine::changes::check_prepared(&prepared, key, &prepared.before.dataset)?;
            if crate::catalog::lifecycle_ident(&prepared.before.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("prepared change dataset differs"));
            }
            self.prepared_changes(prepared, origin)?;
        } else if key.starts_with("change-certificate/") {
            let certificate: ChangeCertificate = decode(json)?;
            crate::engine::changes::check_certificate(rows, origin.revision, key, &certificate)?;
            if crate::catalog::lifecycle_ident(&certificate.after.dataset).to_string()
                != origin.table
            {
                return Err(invalid_inventory("change certificate dataset differs"));
            }
            self.prepared_changes(PreparedChanges::from(&certificate), origin)?;
        } else if let Some(id) = key.strip_prefix("dependency-correction/") {
            let decision: CorrectionDecision = decode(json)?;
            crate::engine::corrections::records::validate(&decision.request)?;
            if decision.request.id.as_str() != id
                || decision.processor != crate::engine::backfill::PROCESSOR
                || decision.revision != origin.revision
                || decision.revision <= 0
                || crate::catalog::lifecycle_ident(&decision.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("dependency correction identity differs"));
            }
        } else if key.starts_with("correction-requirements/") {
            let requirements: crate::engine::corrections::records::Requirements = decode(json)?;
            let decision_key = format!("dependency-correction/{}", requirements.latest_decision);
            let (_, decision_revision, decision_json) = rows
                .prefix(&decision_key)
                .find(|(key, _, _)| *key == decision_key)
                .ok_or_else(unknown_dependencies)?;
            if decision_json.len() > self.limit {
                return Err(capacity());
            }
            let decision: CorrectionDecision = decode(decision_json)?;
            crate::engine::corrections::records::validate(&decision.request)?;
            if key != crate::engine::corrections::records::scope_key(&requirements.definition)?
                || requirements.processor != crate::engine::backfill::PROCESSOR
                || requirements.revision != origin.revision
                || decision_revision != requirements.revision
                || requirements.first_revision <= 0
                || requirements.first_revision > requirements.revision
                || requirements.fields.is_empty()
                || requirements.fields.iter().any(|field| *field <= 0)
                || decision.processor != requirements.processor
                || decision.request.definition != requirements.definition
                || decision.request.id != requirements.latest_decision
                || decision.revision != requirements.revision
                || !decision.request.fields.is_subset(&requirements.fields)
                || crate::catalog::lifecycle_ident(&decision.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory(
                    "dependency requirements identity differs",
                ));
            }
        } else if key.starts_with("dependency-qualification/") {
            let qualification: DependencyQualification = decode(json)?;
            if key != crate::engine::corrections::qualification_key(&qualification)?
                || crate::catalog::lifecycle_ident(&qualification.origins.owner).to_string()
                    != origin.table
                || qualification.first_revision > origin.revision
            {
                return Err(invalid_inventory(
                    "dependency qualification identity differs",
                ));
            }
            self.qualification(&qualification, origin)?;
        } else if let Some(id) = key.strip_prefix("restore-reuse/") {
            let reuse: crate::restore::RestoreReuse = decode(json)?;
            if reuse.id.as_str() != id
                || crate::catalog::lifecycle_ident(&reuse.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("restore preparation identity differs"));
            }
            for object in &reuse.objects {
                self.object(object, origin)?;
            }
        } else if let Some(id) = key.strip_prefix("restore-point/") {
            let reference: crate::restore::RestorePointRef = decode(json)?;
            if reference.id.as_str() != id
                || reference.manifest.owner != reference.dataset
                || crate::catalog::lifecycle_ident(&reference.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("restore point identity differs"));
            }
            self.enqueue(
                &reference.manifest,
                Expansion::Restore(reference.clone()),
                origin,
            )?;
        } else if let Some(id) = key.strip_prefix("restore-materialized/") {
            let created: crate::restore::RestoreMaterialized = decode(json)?;
            if created.reference.id.as_str() != id
                || created.reference.manifest.owner != created.reference.dataset
                || created.catalog.owner != created.reference.dataset
                || created
                    .backups
                    .iter()
                    .any(|object| object.owner != created.reference.dataset)
                || crate::catalog::lifecycle_ident(&created.reference.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory(
                    "restore creation evidence identity differs",
                ));
            }
            // Creation evidence is retained after explicit retirement. Only the
            // active point promises continued availability of its backup objects.
            for object in std::iter::once(&created.reference.manifest)
                .chain(std::iter::once(&created.catalog))
                .chain(&created.backups)
            {
                validate_object(object)?;
            }
        } else if let Some(id) = key.strip_prefix("compaction-request/") {
            let capture: crate::engine::compaction::Capture = decode(json)?;
            if capture.spec.operation.as_str() != id
                || capture.manifest.owner != capture.spec.dataset
                || crate::catalog::lifecycle_ident(&capture.spec.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("compaction request identity differs"));
            }
            self.release(&capture.manifest, origin)?;
            self.table(&capture.source, origin)?;
        } else if [
            "compaction-prepared/",
            "compaction-verified/",
            "compaction-result/",
            "representation/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
        {
            let result: CompactionResult = decode(json)?;
            let capture: crate::engine::compaction::Capture = required_record(
                rows,
                &format!("compaction-request/{}", result.spec.operation),
            )?;
            crate::engine::compaction::check_result(&capture, &result)?;
            let expected = if key.starts_with("representation/") {
                format!(
                    "representation/{}/{}",
                    result.source.logical_metadata().digest,
                    result.spec.group
                )
            } else {
                format!(
                    "{}/{}",
                    key.split('/').next().unwrap_or_default(),
                    result.spec.operation
                )
            };
            if key != expected
                || (!key.starts_with("compaction-prepared/") && !result.verified)
                || crate::catalog::lifecycle_ident(&result.spec.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("compaction completion identity differs"));
            }
            // Selection promises an equivalent current layout, not recovery of
            // the rewrite that produced it. Recovery records root both sides.
            // The replacement retains logical metadata without its old payload.
            if !key.starts_with("representation/") {
                self.table(&result.source, origin)?;
            }
            self.table(&result.replacement, origin)?;
        } else if key.starts_with("export-host-file/") || key.starts_with("export-host-closed/") {
            let created_key = key.replacen("export-host-closed/", "export-host-file/", 1);
            let (revision, _, file): (_, _, crate::engine::export::host::HostFile) = rows
                .get_record(&created_key)?
                .ok_or_else(|| invalid_inventory("host creation evidence missing"))?;
            crate::engine::export::host::validate(&file, &created_key, revision, rows, None)?;
            let completed =
                crate::engine::export::remote::completed_stage(rows, &file.operation, None)?;
            let ended = completed
                .as_ref()
                .is_some_and(|stage| stage.object.path == file.path);
            if key.starts_with("export-host-closed/") {
                let closed: crate::engine::export::ExportMaterialized = decode(json)?;
                let (_, _, operation): (_, _, crate::engine::export::ExportOperation) = rows
                    .get_record(&format!("export-operation/{}", file.operation))?
                    .ok_or_else(|| invalid_inventory("host export operation missing"))?;
                crate::engine::export::check_materialized(&closed, &operation)?;
                if closed.object.path != file.path
                    || rows.revision(key).is_none_or(|closed| closed <= revision)
                {
                    return Err(invalid_inventory("host closed file differs from creation"));
                }
                if !ended {
                    self.object(&closed.object, origin)?;
                    for child in closed.child_tables.values() {
                        self.object(&child.object, origin)?;
                    }
                }
            } else if !ended {
                self.path(&file.path, origin)?;
            }
        } else if [
            "export-attempt/",
            "export-staged/",
            "export-stage-validation/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
        {
            // Host staging is a retained recovery dependency, never remote creation evidence.
            if let Some(staged) =
                crate::engine::export::remote::validate_record(key, json, rows, None)?
            {
                self.object(&staged, origin)?;
                if key.starts_with("export-staged/") {
                    let record: crate::engine::export::ExportMaterialized = decode(json)?;
                    for child in record.child_tables.values() {
                        self.object(&child.object, origin)?;
                    }
                }
            }
        } else if let Some(id) = key.strip_prefix("export-operation/") {
            let operation: crate::engine::export::ExportOperation = decode(json)?;
            crate::engine::export::validate_operation_record(&operation)?;
            if operation.request.operation.as_str() != id
                || crate::catalog::lifecycle_ident(&operation.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("export operation identity differs"));
            }
            self.release_pin(&operation.source, origin)?;
        } else if let Some(id) = key.strip_prefix("export-materialized/") {
            let output: crate::engine::export::ExportMaterialized = decode(json)?;
            let operation: crate::engine::export::ExportOperation =
                required_record(rows, &format!("export-operation/{id}"))?;
            crate::engine::export::validate_operation_record(&operation)?;
            crate::engine::export::check_materialized(&output, &operation)?;
            if operation.request.operation.as_str() != id
                || crate::catalog::lifecycle_ident(&operation.request.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("export creation identity differs"));
            }
            if output.object.path.starts_with("s3://") {
                let validation: crate::export::ExportValidation =
                    required_record(rows, &format!("export-validation/{id}"))?;
                crate::engine::export::remote::check_completed(
                    &operation,
                    &output,
                    &validation,
                    rows,
                    None,
                )?;
            }
            self.object(&output.object, origin)?;
            for child in output.child_tables.values() {
                self.object(&child.object, origin)?;
            }
        } else if let Some(id) = key.strip_prefix("export-validation/") {
            let validation: crate::export::ExportValidation = decode(json)?;
            let output: crate::engine::export::ExportMaterialized =
                required_record(rows, &format!("export-materialized/{id}"))?;
            if validation.object != output.object || validation.rows != output.rows {
                return Err(invalid_inventory(
                    "export validation differs from its output",
                ));
            }
            self.object(&validation.object, origin)?;
        } else if key.starts_with("export-result/") || key.starts_with("export-channel/") {
            let pin: crate::export::ExportPin = decode(json)?;
            crate::engine::export::check_pin(&pin)?;
            let operation: crate::engine::export::ExportOperation =
                required_record(rows, &format!("export-operation/{}", pin.id))?;
            crate::engine::export::check_result(&pin, &operation)?;
            let expected = if key.starts_with("export-result/") {
                format!("export-result/{}", pin.id)
            } else {
                format!("export-channel/{}", operation.request.channel)
            };
            if key != expected
                || crate::catalog::lifecycle_ident(&pin.dataset).to_string() != origin.table
            {
                return Err(invalid_inventory("export selection identity differs"));
            }
            if pin.object.path.starts_with("s3://") {
                crate::engine::export::remote::check_pin_binding(&pin, &operation, rows, None)?;
            }
            self.object(&pin.object, origin)?;
            for child in pin.child_tables.values() {
                self.object(&child.object, origin)?;
            }
            self.release(&pin.source.manifest, origin)?;
        } else if key.starts_with("phase/backfill/") {
            let phase: FileCompletion = decode(json)?;
            self.file_completion(&phase, origin)?;
        } else if key.starts_with("phase/") {
            let phase: PhaseResult = decode(json)?;
            if !matches!(
                phase.phase.as_str(),
                "admission" | "processing" | "validation" | "evidence" | "receipts"
            ) {
                return Err(invalid_inventory("unsupported completed phase kind"));
            }
            for object in &phase.outputs {
                self.object(object, origin)?;
            }
            if let Some(table) = &phase.table {
                self.table(table, origin)?;
            }
            if let Some(changes) = &phase.changes {
                self.changes(changes, origin)?;
            }
            if let Some(coverage) = &phase.coverage {
                self.coverage(coverage, origin)?;
            }
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            for object in dependencies.admitted_inputs.values() {
                self.object(object, origin)?;
            }
            if let Some(base) = &dependencies.base_manifest {
                self.release(base, origin)?;
            }
            for schema in &dependencies.schema_metadata {
                self.object(schema, origin)?;
            }
            for source in dependencies.input_sources.values() {
                self.table(source, origin)?;
            }
            for table in &dependencies.tables {
                self.snapshot(table, origin)?;
            }
        } else if key.starts_with("backfill-prepared/") {
            let phase: PreparedCompletion = decode(json)?;
            self.table(&phase.table, origin)?;
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            self.group_dependencies(dependencies, origin)?;
            if let Some(origins) = &phase.origins {
                self.enqueue(origins, Expansion::Origins, origin)?;
            }
        } else if key.starts_with("backfill-validation/") {
            let phase: ValidationCompletion = decode(json)?;
            self.object(&phase.output, origin)?;
            let dependencies = phase
                .dependencies
                .as_ref()
                .ok_or_else(unknown_dependencies)?;
            self.group_dependencies(dependencies, origin)?;
            if let Some(origins) = &phase.origins {
                self.enqueue(origins, Expansion::Origins, origin)?;
            }
        } else if key.starts_with("backfill/") {
            let capture: Capture = decode(json)?;
            self.release(&capture.base_manifest, origin)?;
            if let Some(target) = &capture.target {
                self.native(target, origin)?;
            }
            if let Some(binding) = &capture.target_binding {
                self.computed(binding, origin, false)?;
            }
            if let Some(related) = &capture.related {
                self.related(related, origin)?;
            }
        } else if key.starts_with("backfill-readback/") {
            let completed: ReadbackCompletion = decode(json)?;
            self.release_pin(&completed.release, origin)?;
        } else if key.starts_with("readback/") {
            let completed: crate::engine::validation::ValidatedReadback = decode(json)?;
            self.release_pin(&completed.release, origin)?;
        } else if key.starts_with("result/") {
            self.release_pin(&decode(json)?, origin)?;
        } else if key == "authority" {
            let authority: Authority = decode(json)?;
            validate_selection(rows, &authority, origin.revision, &origin.table, self.limit)?;
            if let Some(release) = &authority.release {
                // Authority adds no dependency to its exact immutable result.
                // Its binding is inspected once through that result record.
                self.release_pin(release, origin)?;
            }
        } else if key.starts_with("operation/") {
            let operation: Operation = decode(json)?;
            if !supported_format(operation.result_format) {
                return Err(unknown_dependencies());
            }
            let base_checkpointed = operation.base.as_ref().is_some_and(|base| {
                !rows.contains_key(&format!("result/{base}"))
                    && rows
                        .get_record::<crate::engine::history::SuccessionCheckpoint>(
                            "succession-checkpoint",
                        )
                        .ok()
                        .flatten()
                        .is_some_and(|(_, _, cp)| &cp.head.version == base)
            });
            if let Some(base) = &operation.base_manifest {
                if base_checkpointed {
                    self.history(base, origin)?;
                } else {
                    self.release(base, origin)?;
                }
            }
            if let Some(base) = &operation.base
                && !base_checkpointed
            {
                let (_, _, release) = rows
                    .get_record::<ReleasePin>(&format!("result/{base}"))?
                    .ok_or_else(unknown_dependencies)?;
                self.release_pin(&release, origin)?;
            }
        } else if key.starts_with("request-dependencies/") {
            let dependencies: crate::engine::admission::AdmissionDependencies = decode(json)?;
            if let Some(base) = &dependencies.base {
                self.release(&base.manifest, origin)?;
            }
            if let Some(coverage) = &dependencies.coverage {
                self.coverage(coverage, origin)?;
            }
            for object in dependencies.inputs.values() {
                self.object(object, origin)?;
            }
            for source in dependencies.input_sources.values() {
                self.table(source, origin)?;
            }
            if let Some(source) = &dependencies.update_source {
                self.table(source, origin)?;
            }
        } else if key.starts_with("request/") {
            let request: OperationRequest = decode(json)?;
            if self.shallow
                || (!rows.contains_key(&format!("result/{}", request.operation()))
                    && !rows.contains_key(&format!("abandoned/{}", request.operation())))
            {
                for input in request.inputs().values() {
                    self.path(&input.path, origin)?;
                }
                if let OperationRequest::Refresh(refresh) = &request {
                    for input in refresh.inputs().values() {
                        self.release(&input.manifest, origin)?;
                    }
                }
            }
        } else if key.starts_with("groups/") {
            let groups: CompletedGroups = decode(json)?;
            let CompletedGroups::Captured { outputs, source } = groups else {
                return Err(unknown_dependencies());
            };
            self.snapshot(&source, origin)?;
            for object in outputs.values() {
                self.native(object, origin)?;
            }
        } else if key.starts_with("generation/") {
            let seal: GenerationSeal = decode(json)?;
            if self.shallow
                && (key != format!("generation/{}", seal.generation)
                    || origin.table != crate::catalog::lifecycle_ident(&seal.family).to_string()
                    || seal.members.is_empty()
                    || seal.members.iter().any(|(dataset, version)| {
                        dataset != &version.dataset || dataset != &version.manifest.owner
                    }))
            {
                return Err(invalid_inventory("generation seal identity differs"));
            }
            self.seal(&seal, origin)?;
        } else if key == "family-members" {
            let _: BTreeSet<Id> = decode(json)?;
        } else if key == "succession-checkpoint" {
            let checkpoint: crate::engine::history::SuccessionCheckpoint = decode(json)?;
            crate::engine::history::validate_checkpoint(
                &checkpoint,
                rows,
                &origin.table,
                origin.revision,
            )?;
            self.object(&checkpoint.archive, origin)?;
        } else if key.starts_with("succession/") {
            let succession: Succession = decode(json)?;
            if key != format!("succession/{}", succession.selected.version)
                || crate::catalog::lifecycle_ident(&succession.selected.dataset).to_string()
                    != origin.table
            {
                return Err(invalid_inventory("succession receipt identity differs"));
            }
            super::super::history::check_link_receipt(rows, origin.revision, &succession)?;
            validate_object(&succession.selected.manifest)?;
            if let Some(previous) = &succession.predecessor {
                validate_object(&previous.manifest)?;
            }
        } else if let Some(version) = key.strip_prefix("input-progress/") {
            let progress: BTreeMap<Id, SuccessionProof> = decode(json)?;
            let (revision, _, result): (_, _, ProgressResult) = rows
                .get_record(&format!("result/{version}"))?
                .ok_or_else(|| invalid_inventory("progress result is missing"))?;
            let related = result
                .dataset
                .related
                .as_ref()
                .ok_or_else(|| invalid_inventory("progress result has no input coverage"))?;
            if revision != origin.revision
                || result.id.as_str() != version
                || result.dataset.version != result.id
                || result.manifest.owner != result.dataset.dataset
                || !result.verified
                || crate::catalog::lifecycle_ident(&result.dataset.dataset).to_string()
                    != origin.table
                || !progress.keys().eq(related.coverage.inputs.keys())
            {
                return Err(invalid_inventory("progress receipt identity differs"));
            }
            for (dataset, proof) in &progress {
                if related.coverage.inputs.get(dataset) != Some(&proof.target)
                    || proof.observed_revision <= 0
                    || (proof.selections_advanced == 0) != (proof.prior == proof.target)
                    || (proof.selections_behind == 0) != (proof.target == proof.observed_head)
                {
                    return Err(invalid_inventory("progress receipt coverage differs"));
                }
                for version in [&proof.prior, &proof.target, &proof.observed_head] {
                    if &version.dataset != dataset || version.manifest.owner != *dataset {
                        return Err(invalid_inventory("progress endpoint identity differs"));
                    }
                    validate_object(&version.manifest)?;
                }
                for (left, right) in [
                    (&proof.prior, &proof.target),
                    (&proof.target, &proof.observed_head),
                    (&proof.prior, &proof.observed_head),
                ] {
                    if left.version == right.version && left != right {
                        return Err(invalid_inventory("progress version identity conflicts"));
                    }
                }
            }
            // Order and lag are catalog evidence. Availability promises come
            // from actual results/phases, not merely observing these identities.
            // Existing owner grants remain independent roots until retired.
        } else if key.starts_with("release-attempt/") {
            crate::engine::locations::release::validate_record(key, json, rows, None)?;
        } else if key.starts_with("candidate-attempt/") {
            crate::engine::write::build::validate_record(key, json, rows, None)?;
        } else if key.starts_with("source-attempt/") {
            // Pending transfer intent is metadata, never qualified payload evidence.
            crate::engine::locations::validate_source_attempt_record(key, json, rows)?;
        } else if key.starts_with("protection/") {
            let protection: Protection = decode(json)?;
            if !protection.retired {
                let completed_host =
                    crate::engine::export::remote::completed_host_prefix(&protection, rows)?;
                for prefix in &protection.paths {
                    if completed_host.as_ref() == Some(prefix) {
                        continue;
                    }
                    if !prefix.ends_with('/') {
                        return Err(invalid_inventory("unsupported protection path"));
                    }
                    if self.required_paths.is_some() && !self.protected_prefixes.contains(prefix) {
                        self.charge(prefix.len().saturating_add(128))?;
                        self.protected_prefixes.insert(prefix.clone());
                    }
                    let candidates = self
                        .candidates
                        .range(prefix.clone()..)
                        .take_while(|(path, _)| path.starts_with(prefix))
                        .filter(|(_, object)| object.blocker.is_none())
                        .map(|(path, _)| path.clone())
                        .collect::<Vec<_>>();
                    for path in candidates {
                        self.path(&path, origin)?;
                    }
                }
            }
        } else if crate::engine::backfill::continuous::validate_control_record(
            key,
            json,
            rows,
            &origin.table,
            origin.revision,
        )? {
            // Parent/control pointers do not add a second payload lifetime.
        } else if key.starts_with("abandoned/") {
            let _: Abandonment = decode(json)?;
        } else if key.starts_with("runtime/") {
            let _: Runtime = decode(json)?;
        } else if key.starts_with("failure/") || key.starts_with("invalidation/") {
            let _: String = decode(json)?;
        } else {
            return Err(invalid_inventory(format!(
                "unsupported lifecycle root {key}"
            )));
        }
        Ok(())
    }

    pub(super) async fn expand(
        &mut self,
        objects: &storage::Objects,
        budget: &Budget,
        root: &Path,
    ) -> Result<()> {
        while let Some(Pending {
            object,
            kind,
            origin,
        }) = self.pending.pop().or_else(|| self.pending_native.pop())
        {
            tokio::task::consume_budget().await;
            if self
                .visited
                .get(&object.path)
                .is_some_and(|entry| entry.complete)
            {
                continue;
            }
            // The immutable descriptor supplies its size before decoding. The
            // worker retains this allowance if its waiting caller is canceled.
            let bytes = usize::try_from(object.bytes).map_err(|_| capacity())?;
            if bytes > budget.limits.max_metadata_bytes {
                return Err(capacity());
            }
            let memory = Arc::new(budget.reserve(bytes.saturating_mul(8).saturating_add(4096))?);
            match kind {
                Expansion::Origins => {
                    let Strict(originals): Strict<ScalarOrigins> = inputs::read_manifest(
                        objects,
                        &object,
                        budget,
                        &mut self.metadata_bytes,
                        &memory,
                    )
                    .await?;
                    self.group_dependencies(&originals.dependencies, &origin)?;
                    for file in &originals.files {
                        self.file_completion(&file.completion, &origin)?;
                    }
                }
                Expansion::Declaration => {
                    // Declarations contain field identities and definitions,
                    // with no transitive object references.
                    let _: Strict<OriginalDeclaration> = inputs::read_manifest(
                        objects,
                        &object,
                        budget,
                        &mut self.metadata_bytes,
                        &memory,
                    )
                    .await?;
                }
                Expansion::Release { .. } => {
                    let Strict(dataset): Strict<DatasetPin> = inputs::read_manifest(
                        objects,
                        &object,
                        budget,
                        &mut self.metadata_bytes,
                        &memory,
                    )
                    .await?;
                    if dataset.dataset != object.owner {
                        return Err(invalid_inventory("release owner differs"));
                    }
                    if self.visited[&object.path].release_payloads {
                        self.dataset(&dataset, &origin)?;
                    } else {
                        self.interpretation(&dataset, &origin)?;
                    }
                }
                Expansion::Changes(prepared) => {
                    let Strict(applied): Strict<AppliedChangeSet> = inputs::read_manifest(
                        objects,
                        &object,
                        budget,
                        &mut self.metadata_bytes,
                        &memory,
                    )
                    .await?;
                    crate::engine::changes::check_applied(&applied, &prepared)?;
                    for changes in applied.source.iter().chain(applied.receipts.iter()) {
                        self.optional_changes(changes, &origin)?;
                    }
                }
                Expansion::Restore(reference) => {
                    #[cfg(feature = "remote")]
                    if matches!(objects, storage::Objects::Remote(_)) {
                        let point: crate::restore::remote::RemoteRestorePoint =
                            inputs::read_manifest(
                                objects,
                                &object,
                                budget,
                                &mut self.metadata_bytes,
                                &memory,
                            )
                            .await?;
                        crate::restore::remote::validate(&reference, &point)?;
                        if Some(point.retained_prefix) != objects.retained_prefix() {
                            return Err(invalid_inventory("restore scope differs"));
                        }
                        self.object(&point.catalog, &origin)?;
                        for retained in &point.objects {
                            objects.validate_retained_path(&retained.path)?;
                            self.object(retained, &origin)?;
                        }
                        if let Some(entry) = self.visited.get_mut(&object.path) {
                            entry.complete = true;
                        }
                        self.metadata_objects_read += 1;
                        continue;
                    }
                    let point: crate::restore::RestorePoint = inputs::read_manifest(
                        objects,
                        &object,
                        budget,
                        &mut self.metadata_bytes,
                        &memory,
                    )
                    .await?;
                    crate::restore::validate_point(root, &reference, &point)?;
                    self.object(&point.catalog, &origin)?;
                    for retained in &point.objects {
                        self.object(&retained.backup, &origin)?;
                    }
                }
                Expansion::Native(_) => {
                    let (Strict(metadata), _decoded): (Strict<TableMetadata>, _) =
                        inputs::read_native_manifest(
                            objects,
                            &object,
                            budget,
                            &mut self.metadata_bytes,
                            &memory,
                        )
                        .await?;
                    let actual = (metadata.current_snapshot_id(), metadata.current_schema_id());
                    let binding = self
                        .visited
                        .get(&object.path)
                        .and_then(|entry| entry.binding);
                    if binding.is_some_and(|expected| expected != actual) {
                        return Err(invalid_inventory("native snapshot or schema differs"));
                    }
                    if let Some(entry) = self.visited.get_mut(&object.path) {
                        entry.binding = Some(actual);
                    }
                    let table = Table::builder()
                        .identifier(TableIdent::from_strs(["retention", "snapshot"])?)
                        .metadata(metadata)
                        .metadata_location(&object.path)
                        .file_io(iceberg::io::FileIO::new_with_fs())
                        .runtime(iceberg::Runtime::try_current()?)
                        .readonly(true)
                        .build()?;
                    let files = objects.snapshot_files(&table, budget).await?;
                    for path in &files.paths {
                        self.path(path, &origin)?;
                    }
                }
            }
            if let Some(entry) = self.visited.get_mut(&object.path) {
                entry.complete = true;
            }
            self.metadata_objects_read += 1;
        }
        Ok(())
    }
}
