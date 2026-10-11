//! Retained native schema edits and atomic writer-definition cutover.

use super::*;
use iceberg::spec::{
    FormatVersion, Literal, NestedField, PrimitiveType, Schema, TableMetadata, Type,
};
use std::collections::BTreeSet;

impl Engine {
    pub(super) async fn schema_key(
        &self,
        view: &View,
        authority: &Authority,
        request: &OperationRequest,
    ) -> Result<Option<String>> {
        if !matches!(request, OperationRequest::Build(build) if build.base.is_some()) {
            return Ok(None);
        }
        let source = authority
            .selected
            .as_ref()
            .and_then(|s| s.groups.get("source"))
            .ok_or_else(invalid)?;
        // An unchanged first based Build can retain the original material key.
        // Later reuse rechecks compatibility if that exact generation changes.
        let key = phase_key(
            request,
            &fingerprint(request)?,
            authority.eligibility,
            "processing",
            None,
            None,
        )?;
        let exact_source = {
            let rows = self.inner.lifecycle.rows(view).await?;
            let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&key])?;
            self.active_completion::<PhaseResult>(view, &key)
                .await?
                .is_some_and(|phase| phase.table.as_ref() == Some(source))
        };
        if exact_source {
            return Ok(None);
        }
        let native = storage::open_pin(source, &self.inner.lifecycle.budget).await?;
        Ok(Some(schema_identity(
            native.metadata(),
            &self.inner.lifecycle.budget,
        )?))
    }
    /// Qualifies a reused schema-less generation against the exact captured base.
    pub(super) async fn check_reused_build_schema(
        &self,
        view: &View,
        request: &OperationRequest,
        format: u32,
        schema_key: Option<&String>,
        phase: &PhaseResult,
    ) -> Result<()> {
        let OperationRequest::Build(build) = request else {
            return Ok(());
        };
        let Some(base) = &build.base else {
            return Ok(());
        };
        if format < 12 || schema_key.is_some() {
            return Ok(());
        }
        let key = format!("result/{base}");
        let rows = self.inner.lifecycle.rows(view).await?;
        let _memory = rows.reserve_decoded(&self.inner.lifecycle.budget, &[&key])?;
        let (_, _, captured) = rows.get_record::<ReleasePin>(&key)?.ok_or_else(invalid)?;
        let before = captured.dataset.groups.get("source").ok_or_else(invalid)?;
        let after = phase.table.as_ref().ok_or_else(invalid)?;
        if before == after {
            return Ok(());
        }
        verify_rebuild(before, after, format, &self.inner.lifecycle.budget).await
            .map_err(|error| {
                if error.code() == "invalid_schema" {
                    Error::refusal("phase_generation_conflict", "retained processing generation changes the captured schema; retire that processing generation before retrying")
                } else {
                    error
                }
            })
    }

    /// Plans a source schema edit against an exact selected version.
    ///
    /// # Errors
    /// Refuses invalid fields, dependent field removal, stale bases or definitions.
    pub async fn plan_schema(&self, spec: &SchemaSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Schema(spec.clone()), memory)
            .await
    }

    /// Selects a schema and writer definition without rewriting source values.
    ///
    /// # Errors
    /// Refuses unsupported edits, stale cutover boundaries or unavailable retained data.
    pub async fn evolve_schema(&self, spec: &SchemaSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Schema(spec.clone()), memory, None)
            .await
    }

    /// Interrupts after a durable schema phase for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or ordinary schema-operation errors.
    #[cfg(feature = "test-util")]
    pub async fn schema_until(&self, spec: &SchemaSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(OperationRequest::Schema(spec.clone()), memory, Some(phase))
            .await
    }

    pub(super) async fn check_schema(
        &self,
        view: &crate::catalog::View,
        authority: &Authority,
        request: &OperationRequest,
        new_admission: bool,
    ) -> Result<()> {
        let OperationRequest::Schema(spec) = request else {
            if matches!(request, OperationRequest::Build(build) if build.base.is_none())
                && let Some(source) = authority
                    .selected
                    .as_ref()
                    .and_then(|s| s.groups.get("source"))
                && storage::defaults::evolved(
                    storage::open_pin(source, &self.inner.lifecycle.budget)
                        .await?
                        .metadata(),
                )
            {
                return Err(Error::refusal(
                    "schema_base_required",
                    "rebuild an evolved source against its exact selected base",
                ));
            }
            return Ok(());
        };
        let base = authority.selected.as_ref().ok_or_else(invalid)?;
        if new_admission {
            self.check_abandoned_gap(view, &spec.dataset, base.boundary, authority.boundary)
                .await?;
        }
        let source = base.groups.get("source").ok_or_else(invalid)?;
        let native = storage::open_pin(source, &self.inner.lifecycle.budget).await?;
        let proposed = fields(native.metadata(), spec)?;
        storage::defaults::check_expansion(&proposed, &self.inner.lifecycle.budget.limits, false)?;
        storage::defaults::check_expansion(&proposed, &self.inner.lifecycle.budget.limits, true)?;
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
    ) -> Result<Retained<PhaseResult>> {
        #[cfg(feature = "test-util")]
        work_started("admission");
        let base = self.release_admitted(&spec.dataset, &spec.base).await?;
        if operation.base_manifest.as_ref() != Some(&base.manifest) {
            return Err(invalid());
        }
        let phase = PhaseResult {
            operation: spec.operation.clone(),
            phase: "admission".into(),
            request_digest: operation.request_digest.clone(),
            eligibility: operation.eligibility,
            outputs: vec![base.manifest.clone()],
            table: None,
            rows: 0,
            changes: None,
            coverage: None,
            metrics: WriteMetrics::default(),
            dependencies: Some(PhaseDependencies {
                base_manifest: Some(base.manifest),
                ..PhaseDependencies::default()
            }),
        };
        let view = self.inner.lifecycle.open(&spec.dataset).await?;
        let protections = self.request_protections(&view, &spec.operation).await?;
        self.complete(
            &OperationRequest::Schema(spec.clone()),
            operation,
            phase,
            protections,
        )
        .await
    }

    pub(super) async fn process_schema(
        &self,
        spec: &SchemaSpec,
        directory: &Path,
        operation: &Operation,
    ) -> Result<write::Prepared> {
        let base = self.release_admitted(&spec.dataset, &spec.base).await?;
        if operation.base_manifest.as_ref() != Some(&base.manifest) {
            return Err(invalid());
        }
        let source = base.dataset.groups.get("source").ok_or_else(invalid)?;
        let native = storage::open_pin(source, &self.inner.lifecycle.budget).await?;
        let previous = native.metadata_ref();
        let spec_owned = spec.clone();
        let directory = directory.to_owned();
        let view = self.inner.lifecycle.open(&spec.dataset).await?;
        let output = self
            .register_output(view.identifier().namespace(), move || {
                evolve(
                    &previous,
                    &spec_owned,
                    &directory.to_string_lossy(),
                    uuid::Uuid::new_v4(),
                )
            })
            .await?;
        let table = self
            .pin_output(&spec.dataset, &output, source.rows, &source.objects)
            .await?;
        Ok(write::Prepared {
            table,
            changes: None,
            metrics: WriteMetrics::default(),
            schema_input: Some(source.metadata.clone()),
        })
    }
}

fn schema_identity(metadata: &TableMetadata, budget: &Budget) -> Result<String> {
    let _memory = budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?;
    let schemas = metadata
        .schemas_iter()
        .map(|schema| (schema.schema_id(), schema))
        .collect::<BTreeMap<_, _>>();
    let mut material = serde_json::to_value((
        metadata.format_version(),
        metadata.current_schema_id(),
        metadata.last_column_id(),
        schemas,
    ))?;
    if let Some(schemas) = material[3].as_object_mut() {
        for schema in schemas.values_mut() {
            if let Some(ids) = schema["identifier-field-ids"].as_array_mut() {
                // Native identifiers are an unordered set; field order is material.
                ids.sort_by_key(serde_json::Value::as_i64);
            }
        }
    }
    material.sort_all_objects();
    Ok(blake3::hash(&storage::bounded_json(
        &material,
        budget.limits.max_metadata_bytes,
    )?)
    .to_hex()
    .to_string())
}

pub(super) async fn verify_rebuild(
    before: &TablePin,
    after: &TablePin,
    format: u32,
    budget: &Budget,
) -> Result<()> {
    let before = storage::open_pin(before, budget).await?;
    // Older results allowed name-corresponded rebuilds without stable native IDs.
    // Their retained interpretation stays readable; new based Builds are exact.
    if format < 12 && !storage::defaults::evolved(before.metadata()) {
        return Ok(());
    }
    let after = storage::open_pin(after, budget).await?;
    if schema_identity(before.metadata(), budget)? != schema_identity(after.metadata(), budget)? {
        return Err(invalid());
    }
    Ok(())
}

/// Normalize fresh observations once; internal rewrites carry stored values and flags.
pub(super) async fn normalize(
    frame: DataFrame,
    schema: &Schema,
    keys: &[String],
    budget: &Budget,
) -> Result<DataFrame> {
    use arrow::datatypes::DataType;
    use datafusion::prelude::{col, lit, when};
    storage::defaults::validate(schema)?;
    storage::defaults::check_expansion(schema, &budget.limits, true)?;
    let incoming = frame.schema().as_arrow();
    let native = iceberg::arrow::schema_to_arrow_schema(schema)?;
    for field in incoming.fields() {
        if native.field_with_name(field.name()).is_err() || storage::identity::hidden(field.name())
        {
            return Err(Error::refusal(
                "schema_mismatch",
                "input contains a field outside the selected source schema",
            ));
        }
    }
    for key in keys {
        if incoming.field_with_name(key).is_err()
            || storage::defaults::supplied_id(key).is_some()
            || storage::identity::hidden(key)
        {
            return Err(Error::refusal(
                "unknown_key",
                "update keys must be explicitly supplied source fields",
            ));
        }
    }
    let mut values = BTreeMap::new();
    let mut flags = BTreeMap::new();
    for field in schema.as_struct().fields() {
        if storage::identity::hidden(&field.name) {
            values.insert(field.id, lit(storage::defaults::scalar(field)?));
            continue;
        }
        if storage::defaults::supplied_id(&field.name).is_some() {
            continue;
        }
        let marker = storage::defaults::supplied_name(field.id);
        let tracked = schema.field_by_name(&marker).is_some();
        let supplied = incoming.field_with_name(&field.name).is_ok();
        if let Ok(actual) = incoming.field_with_name(&field.name)
            && !write::same_fields(
                &arrow::datatypes::Schema::new(vec![native.field_with_name(&field.name)?.clone()]),
                &arrow::datatypes::Schema::new(vec![actual.clone()]),
            )?
        {
            return Err(Error::refusal(
                "schema_mismatch",
                "supplied values must retain the selected field type and requiredness",
            ));
        }
        let explicit = incoming.field_with_name(&marker).ok();
        if !tracked {
            if !supplied {
                return Err(Error::refusal(
                    "schema_mismatch",
                    "input omits an existing source field",
                ));
            }
            values.insert(
                field.id,
                col(datafusion::common::Column::from_name(&field.name)),
            );
            continue;
        }
        let default = lit(storage::defaults::scalar(field)?);
        let flag = if let Some(flag) = explicit {
            if !supplied || flag.data_type() != &DataType::Boolean || flag.is_nullable() {
                return Err(Error::refusal(
                    "invalid_presence",
                    "supplied flags need a value column and a required Boolean type",
                ));
            }
            col(datafusion::common::Column::from_name(&marker))
        } else {
            lit(supplied)
        };
        let value = if supplied {
            // A physical column may contain explicit nulls. Only its supplied
            // flag, never value equality or nullness, selects the omission rule.
            when(
                flag.clone(),
                col(datafusion::common::Column::from_name(&field.name)),
            )
            .otherwise(default)?
        } else {
            default
        };
        values.insert(field.id, value);
        flags.insert(field.id, flag);
    }
    let key_absence = keys
        .iter()
        .filter_map(|key| {
            let field = schema.field_by_name(key)?;
            incoming
                .field_with_name(&storage::defaults::supplied_name(field.id))
                .ok()?;
            Some(
                col(datafusion::common::Column::from_name(
                    storage::defaults::supplied_name(field.id),
                ))
                .is_not_true(),
            )
        })
        .reduce(|left, right| left.or(right));
    // Explicit per-row key omission is rare. A bounded key-flag probe avoids a
    // custom execution operator while refusing before defaulted keys define scope.
    if let Some(missing) = key_absence
        && write::any_rows(frame.clone().filter(missing)?).await?
    {
        return Err(Error::refusal(
            "invalid_presence",
            "update key rows must be explicitly supplied",
        ));
    }
    let expressions = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            let expression = match storage::defaults::supplied_id(&field.name) {
                Some(id) => flags.remove(&id),
                None => values.remove(&field.id),
            }
            .ok_or_else(invalid)?;
            Ok(expression.alias(&field.name))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(frame.select(expressions)?)
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
        .map(|field| field.name.to_ascii_lowercase())
        .chain(
            spec.additions
                .iter()
                .map(|field| field.name.to_ascii_lowercase()),
        )
        .collect::<BTreeSet<_>>();
    if spec.definition.trim().is_empty()
        || spec.next_definition.trim().is_empty()
        || spec.definition == spec.next_definition
        || spec.base == spec.operation
        || (spec.fields.is_empty() && spec.additions.is_empty())
        || ids.len() != spec.fields.len()
        || names.len() != spec.fields.len() + spec.additions.len()
        || spec.fields.iter().any(|field| {
            field.id <= 0
                || field.name.trim().is_empty()
                || field.name.to_ascii_lowercase().starts_with("__docspec_")
        })
        || spec.additions.iter().any(|field| {
            field.name.trim().is_empty()
                || field.name.to_ascii_lowercase().starts_with("__docspec_")
        })
    {
        return Err(Error::refusal(
            "invalid_schema",
            "schema edit needs distinct root fields, an exact base and a new writer definition",
        ));
    }
    Ok(())
}

fn fields(metadata: &TableMetadata, spec: &SchemaSpec) -> Result<Schema> {
    validate(spec)?;
    let before = metadata.current_schema();
    let roots = before
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field))
        .collect::<BTreeMap<_, _>>();
    let mut fields = spec
        .fields
        .iter()
        .map(|selected| {
            let mut field = roots
                .get(&selected.id)
                .ok_or_else(invalid)?
                .as_ref()
                .clone();
            if field.name.starts_with(storage::defaults::SUPPLIED_PREFIX)
                || storage::identity::hidden(&field.name)
            {
                return Err(invalid());
            }
            field.name = selected.name.clone();
            if let Some(value) = spec.write_defaults.get(&field.id) {
                if before
                    .field_by_name(&storage::defaults::supplied_name(field.id))
                    .is_none()
                {
                    return Err(invalid());
                }
                field.write_default = storage::defaults::literal(&field.field_type, value.clone())?;
            }
            Ok(Arc::new(field))
        })
        .collect::<Result<Vec<_>>>()?;
    if spec
        .write_defaults
        .keys()
        .any(|id| !spec.fields.iter().any(|f| f.id == *id))
    {
        return Err(invalid());
    }
    // Keep presence paired with the logical field through rename/reorder/removal.
    let markers = fields
        .iter()
        .filter_map(|field| {
            before
                .field_by_name(&storage::defaults::supplied_name(field.id))
                .cloned()
        })
        .collect::<Vec<_>>();
    fields.extend(markers);
    let mut next = metadata.last_column_id();
    for addition in &spec.additions {
        let id = next.checked_add(1).ok_or_else(invalid)?;
        next = id
            .checked_add(1)
            .filter(|id| !iceberg::metadata_columns::is_metadata_field(*id))
            .ok_or_else(invalid)?;
        let data_type = storage::defaults::native_type(addition.data_type);
        let default = storage::defaults::literal(&data_type, addition.default.clone())?;
        let mut field = NestedField::optional(id, &addition.name, data_type);
        field.initial_default = default.clone();
        field.write_default = default;
        fields.push(Arc::new(field));
        fields.push(Arc::new(
            NestedField::required(
                next,
                storage::defaults::supplied_name(id),
                Type::Primitive(PrimitiveType::Boolean),
            )
            .with_initial_default(Literal::bool(false))
            .with_write_default(Literal::bool(false)),
        ));
    }
    // Physical maintenance fields are never selected or renamed by schema requests.
    fields.extend(
        before
            .as_struct()
            .fields()
            .iter()
            .filter(|field| storage::identity::hidden(&field.name))
            .cloned(),
    );
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
    let schema = fields(before, spec)?;
    let version = if storage::defaults::has_supplied(&schema) {
        FormatVersion::V3
    } else {
        before.format_version()
    };
    Ok(before
        .clone()
        .into_builder(None)
        .assign_uuid(uuid)
        .set_location(location.into())
        .upgrade_format_version(version)?
        .add_current_schema(schema)?
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
    if !supported_format(before.format)
        || !supported_format(after.format)
        || before.dataset != spec.dataset
        || after.dataset != spec.dataset
        || after.version != spec.operation
        || before.version != spec.base
        || before.definition != spec.definition
        || after.definition != spec.next_definition
        || before.related.is_some()
        || after.related.is_some()
        || before.inputs != after.inputs
        || before.relationship != after.relationship
        || before.eligibility != after.eligibility
        || if after.format == PREVIOUS_FORMAT {
            before.boundary.checked_add(1) != Some(after.boundary)
        } else {
            before.boundary != after.boundary
        }
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
    let first = storage::open_pin(first, budget).await?.metadata_ref();
    let second = storage::open_pin(second, budget).await?.metadata_ref();
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
        &storage::Objects::Local,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_schema_identity_ignores_set_order_but_keeps_allocation_state() {
        let root = tempfile::tempdir().unwrap();
        let schema = Schema::builder()
            .with_fields((1..=4).map(|id| {
                Arc::new(NestedField::required(
                    id,
                    format!("key_{id}"),
                    Type::Primitive(PrimitiveType::Long),
                ))
            }))
            .with_identifier_field_ids([1, 2, 3, 4])
            .build()
            .unwrap();
        let metadata = storage::fresh_metadata(schema, root.path()).unwrap();
        let budget = Budget::new(Limits::default()).unwrap();
        let expected = schema_identity(&metadata, &budget).unwrap();
        let encoded = serde_json::to_value(&metadata).unwrap();
        // Native deserialization rebuilds unordered identifier sets per open.
        for _ in 0..16 {
            let reopened: TableMetadata = serde_json::from_value(encoded.clone()).unwrap();
            assert_eq!(schema_identity(&reopened, &budget).unwrap(), expected);
        }
        let mut changed = encoded;
        changed["last-column-id"] = 99.into();
        let changed: TableMetadata = serde_json::from_value(changed).unwrap();
        assert_ne!(schema_identity(&changed, &budget).unwrap(), expected);
    }
}
