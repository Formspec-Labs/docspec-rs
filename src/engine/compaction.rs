//! Recoverable typed file rewrites selected independently of logical dataset versions.

use super::*;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::stream::BoxStream;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Capture {
    pub spec: CompactionSpec,
    pub source: TablePin,
    pub manifest: ObjectRef,
}

impl Engine {
    /// Compacts exact live files without changing the logical dataset version.
    ///
    /// Rewrites and verifies under ordinary work admission. Old pins and completed
    /// rewrites remain retained; compaction does not authorize physical removal.
    ///
    /// # Errors
    /// Refuses stale representations, unavailable inputs, unsupported native layouts,
    /// conflicting operation identities, insufficient capacity, or unequal output.
    pub async fn compact(&self, spec: &CompactionSpec) -> Result<CompactionResult> {
        let memory = self.request_memory(spec)?;
        let spec = spec.clone();
        self.admit_work(memory, move |engine| async move {
            engine.compact_admitted(&spec, None).await
        })
        .await
    }

    /// Interrupts after a durable compaction boundary for recovery qualification.
    ///
    /// # Errors
    /// Returns the injected interruption or ordinary compaction errors.
    #[cfg(feature = "test-util")]
    pub async fn compact_until(
        &self,
        spec: &CompactionSpec,
        phase: &str,
    ) -> Result<CompactionResult> {
        let memory = self.request_memory(&(spec, phase))?;
        let spec = spec.clone();
        let phase = phase.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.compact_admitted(&spec, Some(&phase)).await
        })
        .await
    }

    pub(super) async fn representation_pin(
        &self,
        view: &View,
        _version: &Id,
        group: &str,
        canonical: &TablePin,
    ) -> Result<TablePin> {
        let retained: Option<CompactionResult> = self
            .inner
            .lifecycle
            .get(
                view,
                &format!(
                    "representation/{}/{group}",
                    canonical.logical_metadata().digest
                ),
            )
            .await?;
        match retained {
            Some(result)
                if result.verified
                    && result.spec.group == group
                    && result.replacement.logical_metadata() == canonical.logical_metadata()
                    && result.source.logical_metadata() == canonical.logical_metadata()
                    && result.replacement.rows == canonical.rows =>
            {
                Ok(result.replacement)
            }
            Some(_) => Err(invalid(
                "selected representation differs from its logical group",
            )),
            None => Ok(canonical.clone()),
        }
    }

    async fn compact_admitted(
        &self,
        spec: &CompactionSpec,
        stop: Option<&str>,
    ) -> Result<CompactionResult> {
        Id::new(&spec.group)?;
        if spec.target_file_bytes == 0
            || spec.files.iter().collect::<BTreeSet<_>>().len() != spec.files.len()
        {
            return Err(invalid(
                "compaction needs a positive output size and distinct file paths",
            ));
        }
        let lifecycle = &self.inner.lifecycle;
        let lock = self.lock(&spec.dataset).await;
        let request_key = format!("compaction-request/{}", spec.operation);
        let result_key = format!("compaction-result/{}", spec.operation);
        let prepared_key = format!("compaction-prepared/{}", spec.operation);
        let verified_key = format!("compaction-verified/{}", spec.operation);
        let captured = {
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(done) = lifecycle
                .get::<CompactionResult>(&view, &result_key)
                .await?
            {
                if done.spec != *spec || !done.verified {
                    return Err(invalid(
                        "compaction identity is already bound to another request",
                    ));
                }
                return Ok(done);
            }
            if let Some(capture) = lifecycle.get::<Capture>(&view, &request_key).await? {
                if capture.spec != *spec {
                    return Err(invalid(
                        "compaction identity is already bound to another request",
                    ));
                }
                let protections = self
                    .retained_protections(&view, &request_key)
                    .await?
                    .ok_or_else(|| {
                        invalid("compaction admission lacks retained owner protection")
                    })?;
                Some((capture, protections))
            } else {
                None
            }
        };
        let (capture, protections) = if let Some(captured) = captured {
            captured
        } else {
            let capture = {
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                self.check_operation_available(&view, &spec.version).await?;
                let release = self
                    .resolve_release(&view, &spec.dataset, &spec.version)
                    .await?;
                let canonical = release
                    .dataset
                    .groups
                    .get(&spec.group)
                    .ok_or_else(|| invalid("release lacks the requested field group"))?;
                let source = self
                    .representation_pin(&view, &spec.version, &spec.group, canonical)
                    .await?;
                Capture {
                    spec: spec.clone(),
                    source,
                    manifest: release.manifest,
                }
            };
            // Owner grants are acquired outside consumer locking, before admission.
            let mut objects = capture.source.objects.clone();
            objects.push(capture.manifest.clone());
            let protections = self.protect_objects(&spec.dataset, &objects).await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(existing) = lifecycle.get::<Capture>(&view, &request_key).await? {
                if existing.spec != *spec {
                    return Err(invalid(
                        "compaction identity is already bound to another request",
                    ));
                }
                let retained = self
                    .retained_protections(&view, &request_key)
                    .await?
                    .ok_or_else(|| {
                        invalid("compaction admission lacks retained owner protection")
                    })?;
                (existing, retained)
            } else {
                let release = self
                    .resolve_release(&view, &spec.dataset, &spec.version)
                    .await?;
                let canonical = release
                    .dataset
                    .groups
                    .get(&spec.group)
                    .ok_or_else(|| invalid("logical group disappeared"))?;
                if release.manifest != capture.manifest
                    || self
                        .representation_pin(&view, &spec.version, &spec.group, canonical)
                        .await?
                        != capture.source
                {
                    return Err(Error::refusal(
                        "stale_representation",
                        "representation changed before compaction admission",
                    ));
                }
                lifecycle
                    .commit(
                        &view,
                        completion_records(
                            &spec.dataset,
                            &view,
                            &request_key,
                            &capture,
                            Some(&protections),
                        )?,
                    )
                    .await?;
                (capture, protections)
            }
        };
        let view = lifecycle.open(&spec.dataset).await?;
        let mut prepared = if let Some(completed) = lifecycle
            .get::<CompactionResult>(&view, &prepared_key)
            .await?
        {
            check_result(&capture, &completed)?;
            storage::check_available(completed.replacement.objects.clone(), &lifecycle.budget)
                .await?;
            completed
        } else {
            let id = Id::new(uuid::Uuid::new_v4().simple().to_string())?;
            let directory = self
                .inner
                .root
                .join("objects")
                .join(spec.dataset.as_str())
                .join(id.as_str());
            {
                let _guard = lock.lock().await;
                let view = lifecycle.open(&spec.dataset).await?;
                lifecycle
                    .commit(
                        &view,
                        vec![record(
                            format!("protection/{id}"),
                            Protection {
                                id,
                                consumer: spec.dataset.clone(),
                                purpose: format!("{}/compaction", spec.operation),
                                paths: vec![format!("{}/", directory.display())],
                                retired: false,
                            },
                        )?],
                    )
                    .await?;
            }
            std::fs::create_dir_all(&directory)?;
            #[cfg(feature = "test-util")]
            work_started("compaction-rewrite");
            let candidate = self.rewrite_compaction(&capture, &directory).await?;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(existing) = lifecycle
                .get::<CompactionResult>(&view, &prepared_key)
                .await?
            {
                check_result(&capture, &existing)?;
                existing
            } else {
                lifecycle
                    .commit(
                        &view,
                        completion_records(
                            &spec.dataset,
                            &view,
                            &prepared_key,
                            &candidate,
                            Some(&protections),
                        )?,
                    )
                    .await?;
                candidate
            }
        };
        stop_after(stop, "rewrite")?;
        let view = lifecycle.open(&spec.dataset).await?;
        if let Some(verified) = lifecycle
            .get::<CompactionResult>(&view, &verified_key)
            .await?
        {
            check_result(&capture, &verified)?;
            let mut expected = prepared.clone();
            expected.verified = true;
            if verified != expected {
                return Err(invalid("verification names another rewrite"));
            }
            prepared = verified;
        } else {
            #[cfg(feature = "test-util")]
            work_started("compaction-verification");
            verify_equivalence(&prepared, &lifecycle.budget).await?;
            prepared.verified = true;
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            lifecycle
                .commit(
                    &view,
                    completion_records(
                        &spec.dataset,
                        &view,
                        &verified_key,
                        &prepared,
                        Some(&protections),
                    )?,
                )
                .await?;
        }
        stop_after(stop, "verification")?;
        {
            let _guard = lock.lock().await;
            let view = lifecycle.open(&spec.dataset).await?;
            if let Some(existing) = lifecycle
                .get::<CompactionResult>(&view, &result_key)
                .await?
            {
                if existing != prepared {
                    return Err(invalid("completed representation differs from retry"));
                }
                return Ok(existing);
            }
            let release = self
                .resolve_release(&view, &spec.dataset, &spec.version)
                .await?;
            if release.manifest != capture.manifest {
                return Err(invalid("logical result identity changed during compaction"));
            }
            let canonical = release
                .dataset
                .groups
                .get(&spec.group)
                .ok_or_else(|| invalid("logical group disappeared"))?;
            let current = self
                .representation_pin(&view, &spec.version, &spec.group, canonical)
                .await?;
            if current != capture.source {
                return Err(Error::refusal(
                    "stale_representation",
                    "another equivalent representation was selected; the completed rewrite remains retained",
                ));
            }
            let representation_key = format!(
                "representation/{}/{}",
                capture.source.logical_metadata().digest,
                spec.group
            );
            let mut records = completion_records(
                &spec.dataset,
                &view,
                &representation_key,
                &prepared,
                Some(&protections),
            )?;
            records.extend(completion_records(
                &spec.dataset,
                &view,
                &result_key,
                &prepared,
                Some(&protections),
            )?);
            lifecycle.commit(&view, records).await?;
        }
        stop_after(stop, "selection")?;
        Ok(prepared)
    }

    async fn rewrite_compaction(
        &self,
        capture: &Capture,
        directory: &Path,
    ) -> Result<CompactionResult> {
        let spec = &capture.spec;
        let budget = &self.inner.lifecycle.budget;
        let native = storage::open_pin(&capture.source).await?;
        let scan = capture_scan(&native, budget, spec.group == "source").await?;
        let chosen = if spec.files.is_empty() {
            scan.files()
                .map(|file| file.file_path().to_owned())
                .collect::<Vec<_>>()
        } else {
            spec.files.clone()
        };
        let selected = chosen.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if selected.iter().any(|path| !scan.contains_file(path)) {
            return Err(invalid(
                "selected file is outside the captured live snapshot",
            ));
        }
        let input_bytes = scan
            .files()
            .filter(|file| selected.contains(file.file_path()))
            .map(|file| file.file_size_in_bytes())
            .sum::<u64>();
        if fs2::available_space(&self.inner.root)?
            < input_bytes
                .saturating_mul(2)
                .saturating_add(budget.limits.scratch_bytes)
        {
            return Err(Error::refusal(
                "disk_capacity",
                "compaction output and declared scratch do not fit available disk",
            ));
        }
        let source = native.metadata_ref();
        let output_directory = directory.to_owned();
        let source_group = spec.group == "source";
        let view = self.inner.lifecycle.open(&spec.dataset).await?;
        let output = self
            .register_output(view.table.identifier().namespace(), move || {
                let metadata = storage::replacement_metadata(&source, &output_directory)?;
                if source_group && !storage::identity::has_origins(metadata.current_schema())? {
                    let schema = storage::identity::with_origins(&metadata)?;
                    Ok(metadata
                        .into_builder(None)
                        .add_current_schema(schema)?
                        .build()?
                        .metadata)
                } else {
                    Ok(metadata)
                }
            })
            .await?;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            output.metadata().current_schema(),
        )?);
        let root = self.inner.root.clone();
        let remaining_space = budget
            .limits
            .scratch_bytes
            .saturating_add((budget.limits.max_batch_bytes as u64).saturating_mul(4));
        let rows = selected_stream(&scan, &chosen, budget)
            .map_ok(move |batch| {
                if fs2::available_space(&root)? < remaining_space {
                    return Err(Error::refusal(
                        "disk_capacity",
                        "compaction output and declared scratch no longer fit available disk",
                    ));
                }
                rewrite_batch(batch, &schema, source_group)
            })
            .and_then(futures::future::ready);
        let written =
            storage::write_batches_sized(&output, rows, budget, spec.target_file_bytes).await?;
        let output_files = written
            .iter()
            .map(|file| file.file_path().to_owned())
            .collect::<Vec<_>>();
        let output_bytes = written.iter().map(|file| file.file_size_in_bytes()).sum();
        let rows = written.iter().map(|file| file.record_count()).sum();
        let mut files = scan
            .files()
            .filter(|file| !selected.contains(file.file_path()))
            .cloned()
            .collect::<Vec<_>>();
        files.extend(written);
        let output = storage::append(&output, self.inner.lifecycle.catalog.as_ref(), files).await?;
        let mut replacement = storage::pin(
            &spec.dataset,
            &output,
            capture.source.rows,
            budget,
            &capture.source.objects,
        )
        .await?;
        replacement.logical_metadata = Some(capture.source.logical_metadata().clone());
        Ok(CompactionResult {
            spec: spec.clone(),
            source: capture.source.clone(),
            replacement,
            input_files: chosen,
            output_files,
            input_bytes,
            output_bytes,
            rows,
            verified: false,
        })
    }
}

async fn capture_scan(
    table: &iceberg::table::Table,
    budget: &Budget,
    source: bool,
) -> Result<storage::OccurrenceScan> {
    if source {
        storage::OccurrenceScan::open(table, budget).await
    } else {
        storage::OccurrenceScan::values(
            table,
            budget,
            storage::snapshot_files(table, budget).await?,
        )
        .await
    }
}

fn selected_stream<'a>(
    scan: &'a storage::OccurrenceScan,
    paths: &'a [String],
    budget: &'a Budget,
) -> BoxStream<'a, Result<RecordBatch>> {
    futures::stream::iter(paths)
        .map(|path| scan.read_file(path, budget))
        .try_flatten()
        .boxed()
}

fn rewrite_batch(batch: RecordBatch, schema: &SchemaRef, source: bool) -> Result<RecordBatch> {
    let input = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let name = match field.name().as_str() {
                storage::identity::ORIGIN_FILE if source => storage::OCCURRENCE_FILE,
                storage::identity::ORIGIN_ROW if source => storage::OCCURRENCE_ROW,
                name => name,
            };
            Ok(batch.column(input.index_of(name)?).clone())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn comparable(batch: RecordBatch) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !storage::identity::hidden(field.name()))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    Ok(batch.project(&fields)?)
}

async fn verify_equivalence(result: &CompactionResult, budget: &Budget) -> Result<()> {
    let before = storage::open_pin(&result.source).await?;
    let after = storage::open_pin(&result.replacement).await?;
    let source = result.spec.group == "source";
    let input = capture_scan(&before, budget, source).await?;
    let output = capture_scan(&after, budget, source).await?;
    let left = selected_stream(&input, &result.input_files, budget)
        .map_ok(comparable)
        .and_then(futures::future::ready)
        .boxed();
    let right = selected_stream(&output, &result.output_files, budget)
        .map_ok(comparable)
        .and_then(futures::future::ready)
        .boxed();
    compare_streams(left, right, result.rows).await
}

async fn compare_streams(
    mut left: BoxStream<'_, Result<RecordBatch>>,
    mut right: BoxStream<'_, Result<RecordBatch>>,
    expected_rows: u64,
) -> Result<()> {
    let mut a: Option<RecordBatch> = None;
    let mut b: Option<RecordBatch> = None;
    let mut ai = 0;
    let mut bi = 0;
    let mut compared = 0u64;
    loop {
        if a.as_ref().is_none_or(|batch| ai == batch.num_rows()) {
            a = left.try_next().await?;
            ai = 0;
            if a.as_ref().is_some_and(|batch| batch.num_rows() == 0) {
                continue;
            }
        }
        if b.as_ref().is_none_or(|batch| bi == batch.num_rows()) {
            b = right.try_next().await?;
            bi = 0;
            if b.as_ref().is_some_and(|batch| batch.num_rows() == 0) {
                continue;
            }
        }
        match (&a, &b) {
            (None, None) if compared == expected_rows => return Ok(()),
            (Some(a), Some(b)) => {
                let length = (a.num_rows() - ai).min(b.num_rows() - bi);
                if a.schema().fields() != b.schema().fields()
                    || a.columns().iter().zip(b.columns()).any(|(a, b)| {
                        a.slice(ai, length).to_data() != b.slice(bi, length).to_data()
                    })
                {
                    return Err(Error::refusal(
                        "compaction_inequivalent",
                        "rewritten values, types, order, multiplicity or occurrence identities differ",
                    ));
                }
                compared = compared
                    .checked_add(length as u64)
                    .ok_or_else(|| invalid("equivalence row count overflows"))?;
                ai += length;
                bi += length;
            }
            _ => {
                return Err(Error::refusal(
                    "compaction_inequivalent",
                    "rewritten stream has missing or additional rows",
                ));
            }
        }
    }
}

pub(super) fn check_result(capture: &Capture, result: &CompactionResult) -> Result<()> {
    if result.spec != capture.spec
        || result.source != capture.source
        || result.source.logical_metadata() != result.replacement.logical_metadata()
        || result.source.rows != result.replacement.rows
    {
        return Err(invalid(
            "retained compaction differs from its captured request",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::refusal("invalid_compaction", message)
}
