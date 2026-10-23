//! Applied receipt changes and source occurrence association without value recomputation.

use super::*;
use crate::evidence::{EVIDENCE, EvidenceResult, EvidenceSpec, SOURCE_FILE, SOURCE_ROW};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::{logical_expr::JoinType, physical_plan::SendableRecordBatchStream};
use write::{Applied, Prepared, Replacement, any_rows, identifier as quote, same_fields};

pub(super) struct PreparedReceipts {
    pub result: EvidenceResult,
    pub changes: Option<ChangePin>,
}

fn keys() -> Vec<String> {
    vec![SOURCE_FILE.into(), SOURCE_ROW.into()]
}

fn invalid(reason: &str) -> Error {
    Error::refusal("invalid_evidence", reason)
}

pub(super) fn validate(spec: &EvidenceSpec) -> Result<()> {
    if spec.base == spec.operation || spec.definition.is_empty() {
        return Err(invalid(
            "evidence needs a nonempty definition and a different base operation",
        ));
    }
    if std::iter::once(&spec.replacements)
        .chain(spec.deletions.iter())
        .any(|input| {
            input.digest.len() != 64 || !input.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(invalid("evidence inputs need BLAKE3 identities"));
    }
    Ok(())
}

fn check_schema(schema: &ArrowSchema, payload: bool) -> Result<()> {
    let expected = if payload { 3 } else { 2 };
    if schema.fields().len() != expected
        || schema.field(0).name() != SOURCE_FILE
        // The maintained Parquet reader may expose a string view; all three
        // representations map to Iceberg string without value coercion.
        || !matches!(schema.field(0).data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        || schema.field(1).name() != SOURCE_ROW
        || schema.field(1).data_type() != &DataType::Int64
        || (payload && schema.field(2).name() != EVIDENCE)
    {
        return Err(invalid(
            "receipt rows need source_file: string, source_row: long, and evidence; deletion rows contain only those keys",
        ));
    }
    Ok(())
}

fn key_columns() -> Vec<datafusion::logical_expr::Expr> {
    vec![col(SOURCE_FILE), col(SOURCE_ROW)]
}

async fn receipt_inputs(
    context: &SessionContext,
    has_deletions: bool,
) -> Result<(DataFrame, DataFrame)> {
    let replacement = context.table("replacements").await?;
    check_schema(replacement.schema().as_arrow(), true)?;
    let mut scope = replacement.clone().select(key_columns())?.distinct()?;
    if has_deletions {
        let deletions = context.table("deletions").await?;
        check_schema(deletions.schema().as_arrow(), false)?;
        if any_rows(scope.clone().join(
            deletions.clone(),
            JoinType::Inner,
            &[SOURCE_FILE, SOURCE_ROW],
            &[SOURCE_FILE, SOURCE_ROW],
            None,
        )?)
        .await?
        {
            return Err(invalid("receipt replacement and deletion keys overlap"));
        }
        scope = scope.union(deletions)?.distinct()?;
    }
    Ok((replacement, scope))
}

/// Enforce association while processing new receipt keys, not on ordinary reads.
async fn check_associations(scope: DataFrame, source: &TablePin, budget: &Budget) -> Result<()> {
    if any_rows(
        scope.clone().filter(
            col(SOURCE_FILE)
                .is_null()
                .or(col(SOURCE_FILE).eq(lit("")))
                .or(col(SOURCE_ROW).is_null())
                .or(col(SOURCE_ROW).lt(lit(0_i64))),
        )?,
    )
    .await?
    {
        return Err(invalid(
            "receipt keys must be nonempty stable files and nonnegative rows",
        ));
    }
    let (unmatched, _memory) = associate(scope, source, budget, JoinType::LeftAnti).await?;
    if any_rows(unmatched).await? {
        return Err(invalid(
            "receipt keys name occurrences outside the captured source",
        ));
    }
    Ok(())
}

/// A retained source result proves whether the previous receipt table can be adopted.
pub(super) fn adopts_receipts(
    request: &OperationRequest,
    processing: &PhaseResult,
    base: &DatasetPin,
) -> Result<bool> {
    let source = processing
        .table
        .as_ref()
        .ok_or_else(|| invalid("processing lacks source"))?;
    let before = base
        .groups
        .get("source")
        .ok_or_else(|| invalid("base lacks source"))?;
    Ok(source.logical_metadata() == before.logical_metadata()
        || matches!(request, OperationRequest::Schema(_))
        || processing
            .changes
            .as_ref()
            .is_some_and(|changes| changes.removed.rows == 0))
}

impl Engine {
    pub(super) fn source_replacement<'a>(
        &'a self,
        dataset: &'a Id,
        captured: (&'a TablePin, &'a iceberg::table::Table),
        directory: &'a locations::Location,
        replacement: DataFrame,
        keys: &'a [String],
    ) -> futures::future::BoxFuture<'a, Result<(TablePin, DataFrame)>> {
        Box::pin(async move {
            let (source, native) = captured;
            let mut native = native.clone();
            let before = if !storage::identity::has_origins(native.metadata().current_schema())? {
                let pin = self
                    .source_with_origins_at(dataset, source, &directory.child("source-origins")?)
                    .await?;
                native = self
                    .inner
                    .objects
                    .open_pin(&pin, &self.inner.lifecycle.budget)
                    .await?;
                pin
            } else {
                source.clone()
            };
            let replacement = schema::normalize(
                replacement,
                native.metadata().current_schema(),
                keys,
                &self.inner.lifecycle.budget,
            )
            .await?;
            Ok((before, replacement))
        })
    }

    #[cfg(test)]
    pub(super) fn source_with_origins<'a>(
        &'a self,
        dataset: &'a Id,
        source: &'a TablePin,
        directory: &'a Path,
    ) -> futures::future::BoxFuture<'a, Result<TablePin>> {
        Box::pin(async move {
            self.source_with_origins_at(
                dataset,
                source,
                &locations::Location::Local(directory.to_owned()),
            )
            .await
        })
    }

    pub(super) fn source_with_origins_at<'a>(
        &'a self,
        dataset: &'a Id,
        source: &'a TablePin,
        directory: &'a locations::Location,
    ) -> futures::future::BoxFuture<'a, Result<TablePin>> {
        Box::pin(async move {
            let native = self
                .inner
                .objects
                .open_pin(source, &self.inner.lifecycle.budget)
                .await?;
            let previous = native.metadata_ref();
            let location = directory.as_string()?;
            let view = self.inner.lifecycle.open(dataset).await?;
            let output = self
                .register_output(view.identifier().namespace(), move || {
                    let schema = storage::identity::with_origins(&previous)?;
                    Ok(previous
                        .as_ref()
                        .clone()
                        .into_builder(None)
                        .assign_uuid(uuid::Uuid::new_v4())
                        .set_location(location)
                        .add_current_schema(schema)?
                        .build()?
                        .metadata)
                })
                .await?;
            let mut pin = self
                .pin_output(dataset, &output, source.rows, &source.objects)
                .await?;
            pin.logical_metadata = Some(source.logical_metadata().clone());
            Ok(pin)
        })
    }
    /// Plans receipt replacement without reading its payloads.
    ///
    /// # Errors
    /// Refuses stale bases, conflicting identities, and invalid requests.
    pub async fn plan_evidence(&self, spec: &EvidenceSpec) -> Result<RunPlan> {
        let memory = self.request_memory(spec)?;
        self.plan_request(OperationRequest::Evidence(spec.clone()), memory)
            .await
    }

    /// Updates receipts while adopting the source and computed values unchanged.
    ///
    /// # Errors
    /// Refuses stale bases, unknown occurrences, overlapping keys, and schema mismatches.
    pub async fn update_evidence(&self, spec: &EvidenceSpec) -> Result<ReleasePin> {
        let memory = self.request_memory(spec)?;
        self.run_internal(OperationRequest::Evidence(spec.clone()), memory, None)
            .await
    }

    /// Interrupts receipt updates after a durable completion for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected failure or an ordinary evidence update error.
    #[cfg(feature = "test-util")]
    pub async fn evidence_until(&self, spec: &EvidenceSpec, phase: &str) -> Result<ReleasePin> {
        let memory = self.request_memory(&(spec, phase))?;
        self.run_internal(
            OperationRequest::Evidence(spec.clone()),
            memory,
            Some(phase),
        )
        .await
    }

    /// Streams receipt values associated with this exact retained result.
    ///
    /// # Errors
    /// Refuses missing receipts, unavailable results, and capacity exhaustion.
    pub async fn read_receipts(&self, release: &ReleasePin) -> Result<SendableRecordBatchStream> {
        self.require_data_work()?;
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            engine.check_read_release(&release).await?;
            let evidence = release
                .dataset
                .evidence
                .as_ref()
                .ok_or_else(|| invalid("result has no receipt table"))?;
            read::read_pin_with_objects(
                &engine.inner.objects,
                &evidence.table,
                &engine.inner.lifecycle.budget,
            )
            .await
        })
        .await
    }

    /// Streams source values with stable occurrence keys for receipt association.
    ///
    /// # Errors
    /// Refuses unavailable results, invalid source pins, and capacity exhaustion.
    pub async fn read_source_occurrences(
        &self,
        release: &ReleasePin,
    ) -> Result<SendableRecordBatchStream> {
        self.require_data_work()?;
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            let view = engine.check_read_release(&release).await?;
            let canonical = release
                .dataset
                .groups
                .get("source")
                .ok_or_else(|| invalid("result has no source table"))?;
            let pin = engine
                .resolve_representation_pin(&view, "source", canonical)
                .await?;
            let context = engine.context();
            context.register_table(
                "source",
                crate::query::occurrences(
                    engine
                        .inner
                        .objects
                        .open_pin(&pin, &engine.inner.lifecycle.budget)
                        .await?,
                    engine.inner.lifecycle.budget.clone(),
                    Some(pin.rows),
                )?,
            )?;
            Ok(context.table("source").await?.execute_stream().await?)
        })
        .await
    }

    // Construct phase state outside the enclosing source-processing poll frame.
    pub(super) fn process_receipts<'a>(
        &'a self,
        spec: &'a EvidenceSpec,
        directory: &'a locations::Location,
        admission: &'a PhaseResult,
    ) -> futures::future::BoxFuture<'a, Result<Prepared>> {
        Box::pin(async move {
            let base = self.release_admitted(&spec.dataset, &spec.base).await?;
            let source = base
                .dataset
                .groups
                .get("source")
                .ok_or_else(|| invalid("base has no source table"))?;
            let context = self.context();
            let request = OperationRequest::Evidence(spec.clone());
            for ((name, _), object) in request.inputs().iter().zip(&admission.outputs) {
                context
                    .register_parquet(name, &object.path, ParquetReadOptions::default())
                    .await?;
            }
            let (replacement, scope) = receipt_inputs(&context, spec.deletions.is_some()).await?;
            check_associations(scope.clone(), source, &self.inner.lifecycle.budget).await?;
            let before = if let Some(previous) = &base.dataset.evidence {
                previous.table.clone()
            } else {
                self.write_frame_at(
                    &spec.dataset,
                    &directory.child("empty-before")?,
                    replacement.clone().limit(0, Some(0))?,
                )
                .await?
                .table
            };
            let before_native = self
                .inner
                .objects
                .open_pin(&before, &self.inner.lifecycle.budget)
                .await?;
            let expected =
                iceberg::arrow::schema_to_arrow_schema(before_native.metadata().current_schema())?;
            if !same_fields(&expected, replacement.schema().as_arrow())? {
                return Err(invalid(
                    "receipt replacements must preserve the complete native schema",
                ));
            }
            let applied = self
                .replace_keys(Replacement {
                    dataset: &spec.dataset,
                    directory: directory.clone(),
                    before: &before,
                    keys: &keys(),
                    scope,
                    replacement,
                    replacement_bytes: Some(
                        admission
                            .outputs
                            .iter()
                            .fold(0_u64, |bytes, object| bytes.saturating_add(object.bytes)),
                    ),
                })
                .await?;
            let Applied {
                table,
                removed,
                added,
                metrics,
            } = applied;
            // First attachment has an explicit empty before state. The independently
            // written empty removed table is sufficient to identify it without
            // adding a permanent root for the transient empty replacement base.
            let before_metadata = base.dataset.evidence.as_ref().map_or_else(
                || removed.metadata.clone(),
                |previous| previous.table.metadata.clone(),
            );
            let changes = ChangePin {
                rules: changes::WRITE_RULES.into(),
                base_version: base.id,
                base_manifest: base.manifest,
                before_metadata,
                after_metadata: table.metadata.clone(),
                key_field_ids: key_field_ids(&before_native)?,
                removed,
                added,
            };
            Ok(Prepared {
                table,
                changes: Some(changes),
                metrics,
                schema_input: None,
            })
        })
    }

    /// Retains receipt deletion separately from value processing on source changes.
    pub(super) fn inherit_receipts<'a>(
        &'a self,
        request: &'a OperationRequest,
        operation: &'a Operation,
        processing: &'a Retained<PhaseResult>,
        base: &'a ReleasePin,
        stop: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<Option<Retained<PreparedReceipts>>>> {
        Box::pin(async move {
            let Some(previous) = &base.dataset.evidence else {
                return Ok(None);
            };
            let source = processing
                .table
                .as_ref()
                .ok_or_else(|| invalid("processing lacks source"))?;
            if adopts_receipts(request, processing, &base.dataset)? {
                return Ok(Some(Retained {
                    value: PreparedReceipts {
                        result: EvidenceResult {
                            source: source.into(),
                            table: previous.table.clone(),
                        },
                        changes: None,
                    },
                    protections: processing
                        .protections
                        .as_ref()
                        .map(ReferenceProtections::duplicate)
                        .transpose()?,
                }));
            }
            let dependencies = Some(PhaseDependencies {
                base_manifest: Some(base.manifest.clone()),
                tables: vec![source.into(), (&previous.table).into()],
                ..PhaseDependencies::default()
            });
            let retained =
                if let Some(completion) = self.phase(request, operation, "receipts").await? {
                    if completion.dependencies != dependencies {
                        return Err(self.phase_generation_conflict(request, operation, "receipts")?);
                    }
                    completion
                } else {
                    #[cfg(feature = "test-util")]
                    work_started("receipts");
                    let directory = self
                        .reserve_location(request, operation, "receipts")
                        .await?;
                    let context = self.context();
                    let native = self
                        .inner
                        .objects
                        .open_pin(&previous.table, &self.inner.lifecycle.budget)
                        .await?;
                    context.register_table(
                        "receipts",
                        crate::query::table(
                            native.clone(),
                            self.inner.lifecycle.budget.clone(),
                            Some(previous.table.rows),
                        )?,
                    )?;
                    let replacement = context.table("receipts").await?.limit(0, Some(0))?;
                    let scope = if let Some(changes) = &processing.changes {
                        context.register_table(
                            "removed",
                            crate::query::occurrences(
                                self.inner
                                    .objects
                                    .open_pin(&changes.removed, &self.inner.lifecycle.budget)
                                    .await?,
                                self.inner.lifecycle.budget.clone(),
                                Some(changes.removed.rows),
                            )?,
                        )?;
                        context
                            .table("removed")
                            .await?
                            .select(vec![
                                col(storage::OCCURRENCE_FILE).alias(SOURCE_FILE),
                                col(storage::OCCURRENCE_ROW).alias(SOURCE_ROW),
                            ])?
                            .distinct()?
                    } else {
                        // A complete rebuild creates fresh occurrence identities. Remove
                        // the old associations explicitly; historical releases keep them.
                        context
                            .table("receipts")
                            .await?
                            .select(key_columns())?
                            .distinct()?
                    };
                    let Applied {
                        table,
                        removed,
                        added,
                        metrics,
                    } = self
                        .replace_keys(Replacement {
                            dataset: request.dataset(),
                            directory,
                            before: &previous.table,
                            keys: &keys(),
                            scope,
                            replacement,
                            replacement_bytes: Some(0),
                        })
                        .await?;
                    let changes = ChangePin {
                        rules: changes::WRITE_RULES.into(),
                        base_version: base.id.clone(),
                        base_manifest: base.manifest.clone(),
                        before_metadata: previous.table.metadata.clone(),
                        after_metadata: table.metadata.clone(),
                        key_field_ids: key_field_ids(&native)?,
                        removed,
                        added,
                    };
                    let mut outputs = table.objects.clone();
                    outputs.extend(changes.removed.objects.iter().cloned());
                    outputs.extend(changes.added.objects.iter().cloned());
                    outputs.sort_by(|a, b| a.path.cmp(&b.path));
                    outputs.dedup();
                    let completion = PhaseResult {
                        phase: "receipts".into(),
                        rows: table.rows,
                        table: Some(table),
                        outputs,
                        changes: Some(changes),
                        dependencies,
                        metrics,
                        ..processing.value.clone()
                    };
                    let protections = processing
                        .protections
                        .as_ref()
                        .map(ReferenceProtections::duplicate)
                        .transpose()?;
                    self.complete(request, operation, completion, protections)
                        .await?
                };
            stop_after(stop, "receipts")?;
            Ok(Some(Retained {
                value: PreparedReceipts {
                    result: EvidenceResult {
                        source: source.into(),
                        table: retained
                            .table
                            .clone()
                            .ok_or_else(|| invalid("receipt completion lacks table"))?,
                    },
                    changes: retained.changes.clone(),
                },
                protections: retained.protections,
            }))
        })
    }
}

fn key_field_ids(table: &iceberg::table::Table) -> Result<Vec<i32>> {
    [SOURCE_FILE, SOURCE_ROW]
        .iter()
        .map(|name| {
            table
                .metadata()
                .current_schema()
                .field_by_name(name)
                .map(|field| field.id)
                .ok_or_else(|| invalid("receipt schema lacks occurrence keys"))
        })
        .collect()
}

/// Independently reads retained receipts without the producing catalog.
///
/// The caller must preserve files for the stream lifetime. The pin itself does
/// not grant storage protection or promise an external receipt URI remains live.
///
/// # Errors
/// Refuses missing receipts, inconsistent manifests, and unreadable native files.
pub async fn read_receipts(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    resolve_release(release, budget).await?;
    let evidence = release
        .dataset
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("result has no receipt table"))?;
    read::read_pin(&evidence.table, budget).await
}

/// Checks a receipt selection; full membership reconstruction is explicit deep work.
pub(super) fn verify<'a>(
    dataset: &'a DatasetPin,
    budget: &'a Budget,
    checks: &'a mut verification::Checks,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let Some(parent) = &dataset.parent_manifest else {
            return if dataset.evidence.is_some()
                || matches!(dataset.request, OperationRequest::Evidence(_))
            {
                Err(invalid("receipt result lacks a parent"))
            } else {
                Ok(())
            };
        };
        let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
        let base: DatasetPin =
            inputs::read_manifest(checks.objects(), parent, budget, &mut 0, &memory).await?;
        if base.dataset != dataset.dataset
            || (!matches!(dataset.request, OperationRequest::Backfill(_))
                && dataset.request.base() != Some(&base.version))
        {
            return Err(invalid("receipt result differs from its captured parent"));
        }
        let Some(evidence) = &dataset.evidence else {
            return if base.evidence.is_some()
                || matches!(dataset.request, OperationRequest::Evidence(_))
            {
                Err(invalid("result discarded captured receipts"))
            } else {
                Ok(())
            };
        };
        let source = dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid("result has no source"))?;
        if !matches!(dataset.request, OperationRequest::Evidence(_)) && base.evidence.is_none() {
            return Err(invalid("inherited receipt result has no prior table"));
        }
        if matches!(dataset.request, OperationRequest::Evidence(_))
            && base.groups.get("source") != Some(source)
        {
            return Err(invalid("receipt update changed its captured source"));
        }
        verify_result(source, evidence, budget, checks).await?;
        if let Some(previous) = &base.evidence
            && previous.table != evidence.table
        {
            let before = storage::open_pin(&previous.table, budget).await?;
            let after = storage::open_pin(&evidence.table, budget).await?;
            if !storage::identity::preserves_schema(
                before.metadata().current_schema(),
                after.metadata().current_schema(),
            )? {
                return Err(invalid(
                    "receipt schema differs from its retained predecessor",
                ));
            }
        }
        if checks.deep() {
            if let Some(previous) = &base.evidence
                && previous.table != evidence.table
            {
                checks.table(&previous.table, budget).await?;
            }
            if let OperationRequest::Evidence(spec) = &dataset.request {
                verify_replacement(dataset, &base, spec, budget).await?;
            } else {
                verify_inheritance(dataset, &base, budget).await?;
            }
        }
        Ok(())
    })
}

/// Checks applied receipts once while their recovery phase owns the change rows.
pub(super) fn verify_prepared<'a>(
    request: &'a OperationRequest,
    source: &'a TablePin,
    base: &'a ReleasePin,
    prepared: &'a PreparedReceipts,
    budget: &'a Budget,
    checks: &'a mut verification::Checks,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let evidence = &prepared.result;
        verify_result(source, evidence, budget, checks).await?;
        if let Some(changes) = &prepared.changes {
            let before = base
                .dataset
                .evidence
                .as_ref()
                .map(|previous| &previous.table)
                .unwrap_or(&changes.removed);
            if changes.base_manifest != base.manifest
                || changes.base_version != base.id
                || changes.before_metadata != before.metadata
                || changes.after_metadata != evidence.table.metadata
                || (base.dataset.evidence.is_none() && before.rows != 0)
            {
                return Err(invalid(
                    "receipt changes do not bind the captured before and after states",
                ));
            }
            changes::verify_native_changes(
                before,
                &evidence.table,
                &keys(),
                changes,
                budget,
                checks,
            )
            .await?;
        } else if base
            .dataset
            .evidence
            .as_ref()
            .map(|previous| &previous.table)
            != Some(&evidence.table)
        {
            return Err(invalid(
                "unchanged receipts do not preserve their original table",
            ));
        }
        if matches!(request, OperationRequest::Evidence(_)) && prepared.changes.is_none() {
            return Err(invalid("receipt update lacks applied change evidence"));
        }
        Ok(())
    })
}

/// Checks complete receipt state without requiring optional transition history.
pub(super) fn verify_result<'a>(
    source: &'a TablePin,
    evidence: &'a EvidenceResult,
    budget: &'a Budget,
    checks: &'a mut verification::Checks,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if evidence.source != NativeSnapshotRef::from(source) {
            return Err(invalid(
                "receipt association uses another source interpretation",
            ));
        }
        checks.table(&evidence.table, budget).await?;
        let native = storage::open_pin(&evidence.table, budget).await?;
        check_schema(
            &iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?,
            true,
        )?;
        if checks.deep() {
            let context = budget.context();
            context.register_table(
                "receipts",
                crate::query::table(native, budget.clone(), Some(evidence.table.rows))?,
            )?;
            check_associations(
                context
                    .table("receipts")
                    .await?
                    .select(key_columns())?
                    .distinct()?,
                source,
                budget,
            )
            .await?;
        }
        Ok(())
    })
}

// Reconstruct receipt replacement from retained original inputs, not optional deltas.
async fn verify_replacement(
    dataset: &DatasetPin,
    base: &DatasetPin,
    spec: &EvidenceSpec,
    budget: &Budget,
) -> Result<()> {
    let current = dataset
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("missing receipts"))?;
    let source = dataset
        .groups
        .get("source")
        .ok_or_else(|| invalid("missing source"))?;
    let context = budget.context();
    for (name, input) in dataset.request.inputs() {
        let object = dataset
            .inputs
            .get(&dataset.request.input_key(&name))
            .filter(|object| object.digest == input.digest)
            .ok_or_else(|| invalid("receipt input differs from its captured request"))?;
        context
            .register_parquet(&name, &object.path, ParquetReadOptions::default())
            .await?;
    }
    let (replacement, scope) = receipt_inputs(&context, spec.deletions.is_some()).await?;
    check_associations(scope.clone(), source, budget).await?;
    let expected = if let Some(previous) = &base.evidence {
        let native = storage::open_pin(&previous.table, budget).await?;
        let expected = iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?;
        if !same_fields(&expected, replacement.schema().as_arrow())? {
            return Err(invalid(
                "receipt replacements must preserve the complete native schema",
            ));
        }
        context.register_table(
            "previous",
            crate::query::table(native, budget.clone(), Some(previous.table.rows))?,
        )?;
        context
            .table("previous")
            .await?
            .join(
                scope,
                JoinType::LeftAnti,
                &[SOURCE_FILE, SOURCE_ROW],
                &[SOURCE_FILE, SOURCE_ROW],
                None,
            )?
            .union(replacement)?
    } else {
        replacement
    };
    verify_receipt_values(
        &context,
        expected,
        &current.table,
        budget,
        "receipt values differ from the retained replacement and deletion inputs",
    )
    .await
}

// Verify complete inheritance, including duplicate/null/nested receipt values.
// The difference is grouped once, so each relation is consumed once.
async fn verify_inheritance(
    dataset: &DatasetPin,
    base: &DatasetPin,
    budget: &Budget,
) -> Result<()> {
    let previous = base
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("inherited receipt result has no prior table"))?;
    let current = dataset
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("missing receipts"))?;
    let context = budget.context();
    context.register_table(
        "previous",
        crate::query::table(
            storage::open_pin(&previous.table, budget).await?,
            budget.clone(),
            Some(previous.table.rows),
        )?,
    )?;
    let (expected, _memory) = associate(
        context.table("previous").await?,
        dataset
            .groups
            .get("source")
            .ok_or_else(|| invalid("missing source"))?,
        budget,
        JoinType::LeftSemi,
    )
    .await?;
    verify_receipt_values(
        &context,
        expected,
        &current.table,
        budget,
        "inherited receipts differ from the surviving source occurrences",
    )
    .await
}

async fn verify_receipt_values(
    context: &SessionContext,
    expected: DataFrame,
    current: &TablePin,
    budget: &Budget,
    reason: &str,
) -> Result<()> {
    let native = storage::open_pin(current, budget).await?;
    if !same_fields(
        expected.schema().as_arrow(),
        &iceberg::arrow::schema_to_arrow_schema(native.metadata().current_schema())?,
    )? {
        return Err(invalid(
            "receipt values changed their complete field types or nullability",
        ));
    }
    context.register_table(
        "current",
        crate::query::table(native, budget.clone(), Some(current.rows))?,
    )?;
    context.register_table("expected", expected.into_view())?;
    let columns = [SOURCE_FILE, SOURCE_ROW, EVIDENCE]
        .iter()
        .map(|name| quote(name))
        .collect::<Vec<_>>()
        .join(", ");
    if changes::any_signed_multiset_mismatch(
        context,
        &columns,
        &[("expected", &[("delta", 1)]), ("current", &[("delta", -1)])],
        "SUM(delta) <> 0",
    )
    .await?
    {
        return Err(invalid(reason));
    }
    Ok(())
}

// Initial physical row ranges prove membership without opening source payloads.
// Once origins are stored, read only those two columns. The receipt population
// and native file inventory remain under the shared execution/metadata budgets.
async fn associate(
    rows: DataFrame,
    source: &TablePin,
    budget: &Budget,
    kind: JoinType,
) -> Result<(DataFrame, Option<tokio::sync::OwnedSemaphorePermit>)> {
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::Field,
        record_batch::RecordBatch,
    };
    let objects = storage::Objects::for_path(&source.metadata.path);
    let (native, inventory) = objects.open_snapshot(source, budget).await?;
    let context = budget.context();
    if !storage::identity::has_origins(native.metadata().current_schema())? {
        let memory = budget.reserve(budget.limits.max_metadata_bytes)?;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("__source_file", DataType::Utf8, false),
            Field::new("__source_rows", DataType::Int64, false),
        ]));
        let files =
            StringArray::from_iter_values(inventory.data.iter().map(|file| file.file_path()));
        let lengths = Int64Array::from_iter_values(
            inventory
                .data
                .iter()
                .map(|file| {
                    i64::try_from(file.record_count())
                        .map_err(|_| invalid("source row count exceeds occurrence range"))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let columns: Vec<ArrayRef> = vec![Arc::new(files), Arc::new(lengths)];
        let batch = RecordBatch::try_new(schema, columns)?;
        if batch.get_array_memory_size() > budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "source occurrence ranges exceed metadata allowance",
            ));
        }
        let ranges = context.read_batch(batch)?;
        return Ok((
            rows.join(
                ranges,
                kind,
                &[SOURCE_FILE],
                &["__source_file"],
                Some(
                    col(SOURCE_ROW)
                        .gt_eq(lit(0_i64))
                        .and(col(SOURCE_ROW).lt(col("__source_rows"))),
                ),
            )?,
            Some(memory),
        ));
    }
    let scan = Arc::new(storage::OccurrenceScan::values(&native, budget, inventory).await?);
    let columns = [
        storage::identity::ORIGIN_FILE.to_owned(),
        storage::identity::ORIGIN_ROW.to_owned(),
    ];
    context.register_table(
        "source_origins",
        crate::query::files(scan, Some(&columns), budget.clone())?,
    )?;
    let occurrences = context.table("source_origins").await?.select(vec![
        col(storage::identity::ORIGIN_FILE).alias(SOURCE_FILE),
        col(storage::identity::ORIGIN_ROW).alias(SOURCE_ROW),
    ])?;
    Ok((
        rows.join(
            occurrences,
            kind,
            &[SOURCE_FILE, SOURCE_ROW],
            &[SOURCE_FILE, SOURCE_ROW],
            None,
        )?,
        None,
    ))
}
