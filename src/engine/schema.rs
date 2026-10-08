//! Retained native schema edits and atomic writer-definition cutover.

use super::*;
use iceberg::spec::{Schema, TableMetadata};
use std::collections::BTreeSet;

impl Engine {
    /// Plans a source schema edit against an exact selected version.
    ///
    /// # Errors
    /// Refuses invalid fields, dependent field removal, stale bases or definitions.
    pub async fn plan_schema(&self, spec: &SchemaSpec) -> Result<RunPlan> {
        self.plan_request(&OperationRequest::Schema(spec.clone()))
            .await
    }

    /// Selects a schema and writer definition without rewriting source values.
    ///
    /// # Errors
    /// Refuses unsupported edits, stale cutover boundaries or unavailable retained data.
    pub async fn evolve_schema(&self, spec: &SchemaSpec) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Schema(spec.clone()), None)
            .await
    }

    /// Interrupts after a durable schema phase for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or ordinary schema-operation errors.
    #[cfg(feature = "test-util")]
    pub async fn schema_until(&self, spec: &SchemaSpec, phase: &str) -> Result<ReleasePin> {
        self.run_internal(&OperationRequest::Schema(spec.clone()), Some(phase))
            .await
    }

    pub(super) async fn check_schema(
        &self,
        authority: &Authority,
        request: &OperationRequest,
        new_admission: bool,
    ) -> Result<()> {
        let OperationRequest::Schema(spec) = request else {
            return Ok(());
        };
        let base = authority.selected.as_ref().ok_or_else(invalid)?;
        if new_admission && authority.boundary != base.boundary {
            return Err(Error::refusal(
                "pending_source",
                "resolve pending source admission before changing the selected schema",
            ));
        }
        let source = base.groups.get("source").ok_or_else(invalid)?;
        let native = storage::open_pin(source).await?;
        let proposed = fields(native.metadata().current_schema(), spec)?;
        for group in base.computed.values() {
            if group
                .input_fields
                .values()
                .any(|id| proposed.field_by_id(*id).is_none())
            {
                return Err(Error::refusal(
                    "dependent_fields",
                    "schema edit removes a field consumed by a selected computed group",
                ));
            }
            if group.definition.fields.iter().any(|field| {
                proposed
                    .as_struct()
                    .fields()
                    .iter()
                    .any(|source| source.name.eq_ignore_ascii_case(&field.name))
            }) {
                return Err(Error::refusal(
                    "field_collision",
                    "source and computed field names must remain distinct",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn admit_schema(
        &self,
        spec: &SchemaSpec,
        operation: &Operation,
    ) -> Result<PhaseResult> {
        #[cfg(feature = "test-util")]
        work_started("admission");
        let base = self.release(&spec.dataset, &spec.base).await?;
        if operation.base_manifest.as_ref() != Some(&base.manifest) {
            return Err(invalid());
        }
        let phase = PhaseResult {
            operation: spec.operation.clone(),
            phase: "admission".into(),
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            outputs: vec![base.manifest],
            table: None,
            rows: 0,
            changes: None,
            coverage: None,
            metrics: WriteMetrics::default(),
        };
        self.complete(&OperationRequest::Schema(spec.clone()), operation, &phase)
            .await
    }

    pub(super) async fn process_schema(
        &self,
        spec: &SchemaSpec,
        directory: &Path,
        operation: &Operation,
    ) -> Result<write::Prepared> {
        let base = self.release(&spec.dataset, &spec.base).await?;
        if operation.base_manifest.as_ref() != Some(&base.manifest) {
            return Err(invalid());
        }
        let source = base.dataset.groups.get("source").ok_or_else(invalid)?;
        let native = storage::open_pin(source).await?;
        let previous = native.metadata_ref();
        let spec_owned = spec.clone();
        let directory = directory.to_owned();
        let view = self.inner.lifecycle.open(&spec.dataset).await?;
        let output = self
            .register_output(view.table.identifier().namespace(), move || {
                evolve(
                    &previous,
                    &spec_owned,
                    &directory.to_string_lossy(),
                    uuid::Uuid::new_v4(),
                )
            })
            .await?;
        let table = storage::pin(
            &spec.dataset,
            &output,
            source.rows,
            &self.inner.lifecycle.budget,
            &source.objects,
        )
        .await?;
        Ok(write::Prepared {
            table,
            changes: None,
            metrics: WriteMetrics::default(),
        })
    }
}

pub(super) fn validate(spec: &SchemaSpec) -> Result<()> {
    let ids = spec
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<BTreeSet<_>>();
    let names = spec
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    if spec.definition.trim().is_empty()
        || spec.next_definition.trim().is_empty()
        || spec.definition == spec.next_definition
        || spec.base == spec.operation
        || spec.fields.is_empty()
        || ids.len() != spec.fields.len()
        || names.len() != spec.fields.len()
        || spec.fields.iter().any(|field| {
            field.id <= 0
                || field.name.trim().is_empty()
                || field.name.eq_ignore_ascii_case(storage::OCCURRENCE_FILE)
                || field.name.eq_ignore_ascii_case(storage::OCCURRENCE_ROW)
        })
    {
        return Err(Error::refusal(
            "invalid_schema",
            "schema edit needs distinct existing root fields, an exact base and a new writer definition",
        ));
    }
    Ok(())
}

fn fields(before: &Schema, spec: &SchemaSpec) -> Result<Schema> {
    validate(spec)?;
    let roots = before
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field))
        .collect::<BTreeMap<_, _>>();
    let fields = spec
        .fields
        .iter()
        .map(|selected| {
            let mut field = roots
                .get(&selected.id)
                .ok_or_else(invalid)?
                .as_ref()
                .clone();
            field.name = selected.name.clone();
            Ok(Arc::new(field))
        })
        .collect::<Result<Vec<_>>>()?;
    // Native validation prevents removing identifier fields or invalidating nested IDs.
    Ok(Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(before.identifier_field_ids())
        .build()?)
}

fn evolve(
    before: &TableMetadata,
    spec: &SchemaSpec,
    location: &str,
    uuid: uuid::Uuid,
) -> Result<TableMetadata> {
    Ok(before
        .clone()
        .into_builder(None)
        .assign_uuid(uuid)
        .set_location(location.into())
        .add_current_schema(fields(before.current_schema(), spec)?)?
        .build()?
        .metadata)
}

/// Checks all native state, allowing only the declared fields and generated metadata identity.
pub(super) async fn verify_transition(
    spec: &SchemaSpec,
    before: &DatasetPin,
    after: &DatasetPin,
    budget: &Budget,
) -> Result<()> {
    validate(spec)?;
    if before.format != FORMAT
        || after.format != FORMAT
        || before.dataset != spec.dataset
        || after.dataset != spec.dataset
        || after.version != spec.operation
        || before.version != spec.base
        || before.definition != spec.definition
        || after.definition != spec.next_definition
        || before.related.is_some()
        || after.related.is_some()
        || after.changes.is_some()
        || before.inputs != after.inputs
        || before.relationship != after.relationship
        || before.eligibility != after.eligibility
        || before.boundary.checked_add(1) != Some(after.boundary)
    {
        return Err(invalid());
    }
    let first = before.groups.get("source").ok_or_else(invalid)?;
    let second = after.groups.get("source").ok_or_else(invalid)?;
    let reservation = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(4))?;
    fn inherited(pin: &TablePin) -> BTreeMap<&str, &ObjectRef> {
        pin.objects
            .iter()
            .filter(|object| *object != &pin.metadata)
            .map(|object| (object.path.as_str(), object))
            .collect::<BTreeMap<_, _>>()
    }
    if first.rows != second.rows
        || first.snapshot != second.snapshot
        || inherited(first) != inherited(second)
    {
        return Err(invalid());
    }
    let first = storage::open_pin(first).await?.metadata_ref();
    let second = storage::open_pin(second).await?.metadata_ref();
    let limit = budget.limits.max_metadata_bytes;
    let spec = spec.clone();
    budget
        .blocking(move || {
            let _reservation = reservation;
            if first.uuid() == second.uuid() {
                return Err(invalid());
            }
            let expected = evolve(&first, &spec, second.location(), second.uuid())?;
            // The native builder has no timestamp setter. Round-trip only its bounded
            // expected metadata, normalize this generated clock, then use native Eq
            // so private references and unordered maps also participate in the proof.
            let mut normalized: serde_json::Value =
                serde_json::from_slice(&storage::bounded_json(&expected, limit)?)?;
            normalized["last-updated-ms"] = second.last_updated_ms().into();
            let expected: TableMetadata = serde_json::from_value(normalized)?;
            if expected != *second {
                return Err(invalid());
            }
            Ok(())
        })
        .await
}

pub(super) async fn check_available(dataset: &DatasetPin, budget: &Budget) -> Result<()> {
    let OperationRequest::Schema(spec) = &dataset.request else {
        return Ok(());
    };
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?);
    let mut bytes = 0;
    let before: DatasetPin = inputs::read_manifest(
        dataset.parent_manifest.as_ref().ok_or_else(invalid)?,
        budget,
        &mut bytes,
        &memory,
    )
    .await?;
    verify_transition(spec, &before, dataset, budget).await
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_schema",
        "schema selection differs from its exact base, field identities or retained native state",
    )
}
