//! Complete affected-key recomputation over immutable native occurrence files.

mod history;

use super::{refresh, write::Replacement, *};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use refresh::{TUPLE, tuple_columns};
use write::identifier as quote;

fn full_refresh() -> Error {
    Error::refusal(
        "full_refresh_required",
        "incremental refresh requires a qualified related base with unchanged input roles, native schemas and transform definition",
    )
}

impl Engine {
    // Metadata-only qualification precedes the admission boundary. Captured pins
    // are immutable; final selection separately checks current producer authority.
    pub(super) async fn check_incremental(
        &self,
        authority: &Authority,
        request: &OperationRequest,
    ) -> Result<()> {
        let OperationRequest::Refresh(spec) = request else {
            return Ok(());
        };
        if spec.mode != RefreshMode::Incremental {
            return Ok(());
        }
        qualify(
            spec,
            authority.selected.as_ref().ok_or_else(full_refresh)?,
            authority.eligibility,
            &self.inner.lifecycle.budget,
        )
        .await
    }

    pub(super) fn process_incremental<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        admission: &'a PhaseResult,
        operation: &'a Operation,
        captured_source: Option<&'a TablePin>,
    ) -> futures::future::BoxFuture<'a, Result<write::Prepared>> {
        Box::pin(async move {
        let coverage = admission.coverage.as_ref().ok_or_else(full_refresh)?;
        if coverage.inputs != spec.inputs() {
            return Err(full_refresh());
        }
        let base = self
            .release_admitted(&spec.dataset, spec.base.as_ref().ok_or_else(full_refresh)?)
            .await?;
        check_manifest(&base, operation)?;
        let related = base.dataset.related.as_ref().ok_or_else(full_refresh)?;
        let budget = &self.inner.lifecycle.budget;
        // A base is read as an immutable result, never inferred from table heads.
        resolve_release(&base, budget).await?;
        let canonical = source(&base)?;
        // Resume keeps its admitted layout; older admissions use the canonical
        // source instead of resolving a newly selected representation.
        let captured = captured_source.unwrap_or(canonical);
        let native = write::open_captured_source(canonical, captured, budget).await?;
        let previous =
            refresh::load_inputs(&related.coverage, &related.input_sources, budget).await?;
        let current = refresh::load_inputs(
            coverage,
            admission
                .dependencies
                .as_ref()
                .map(|dependencies| &dependencies.input_sources)
                .unwrap_or(&BTreeMap::new()),
            budget,
        )
        .await?;
        if previous.releases.iter().all(|(dataset, old)| {
            current.releases.get(dataset).is_some_and(|new| {
                old.dataset
                    .groups
                    .get("source")
                    .zip(new.dataset.groups.get("source"))
                    .is_some_and(|(old, new)| old.logical_metadata() == new.logical_metadata())
            })
        }) {
            // Receipt-only input succession advances exact coverage while the
            // complete value result and contributor occurrences remain unchanged.
            // Preserve the current native schema ID as well as its fields;
            // fresh creation can renumber an evolved source's schema.
            let empty = self
                .replacement_output_at(&spec.dataset, &directory.child("empty-changes")?, &native)
                .await?;
            let empty = self.pin_output(&spec.dataset, &empty, 0, &[]).await?;
            let key_field_ids = refresh::output_keys(&spec.transform)
                .iter()
                .map(|name| {
                    native
                        .metadata()
                        .current_schema()
                        .field_by_name(name)
                        .map(|field| field.id)
                        .ok_or_else(full_refresh)
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(write::Prepared {
                table: captured.clone(),
                changes: Some(ChangePin {
                    rules: super::changes::WRITE_RULES.into(),
                    base_version: base.id.clone(),
                    base_manifest: base.manifest.clone(),
                    before_metadata: canonical.metadata.clone(),
                    after_metadata: captured.metadata.clone(),
                    key_field_ids,
                    removed: empty.clone(),
                    added: empty,
                }),
                metrics: WriteMetrics::default(),
                schema_input: None,
            });
        }
        match &spec.transform {
            RefreshTransform::Join(_) => {
                self.process_incremental_join(
                    spec,
                    directory,
                    &base,
                    &previous,
                    &current,
                    (captured, &native),
                )
                .await
            }
            RefreshTransform::Aggregate(_) => {
                self.process_aggregation(
                    spec,
                    directory,
                    &base,
                    &previous,
                    &current,
                    (captured, &native),
                )
                .await
            }
            RefreshTransform::ConnectedComponents(_) => {
                Box::pin(self.process_connected_components(
                    spec,
                    directory,
                    &base,
                    &previous,
                    &current,
                    (captured, &native),
                ))
                .await
            }
        }
        })
    }

    fn process_incremental_join<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        base: &'a ReleasePin,
        previous: &'a refresh::LoadedInputs,
        current: &'a refresh::LoadedInputs,
        captured: (&'a TablePin, &'a iceberg::table::Table),
    ) -> futures::future::BoxFuture<'a, Result<write::Prepared>> {
        Box::pin(async move {
        let (captured_source, native) = captured;
        let budget = &self.inner.lifecycle.budget;
        let related = base.dataset.related.as_ref().ok_or_else(full_refresh)?;
        let RefreshTransform::Join(join) = &spec.transform else {
            return Err(full_refresh());
        };
        let RefreshTransform::Join(previous_join) = &related.request.transform else {
            return Err(full_refresh());
        };
        let context = self.context();
        let mut discovery = WriteMetrics::default();
        for (index, old_version, new_version) in [
            (0, &previous_join.left, &join.left),
            (1, &previous_join.right, &join.right),
        ] {
            let old = source(&previous.releases[&old_version.dataset])?;
            let new = source(&current.releases[&new_version.dataset])?;
            let keys = join
                .join
                .keys
                .iter()
                .map(|key| {
                    if index == 0 {
                        key.left.clone()
                    } else {
                        key.right.clone()
                    }
                })
                .collect::<Vec<_>>();
            let discovery_dir = directory.child(&format!("discovery-{index}"))?;
            let (pin, scanned) = Box::pin(self.changed_keys(
                &spec.dataset,
                &discovery_dir,
                (
                    &previous.releases[&old_version.dataset],
                    &current.releases[&new_version.dataset],
                ),
                (
                    previous.canonical_source(&old_version.dataset)?,
                    current.canonical_source(&new_version.dataset)?,
                ),
                &keys,
            ))
            .await?;
            discovery.files_scanned += scanned;
            let native = storage::open_pin(&pin, &self.inner.lifecycle.budget).await?;
            let files = storage::snapshot_files(&native, budget).await?;
            discovery.data_files_written += files.data.len() as u64;
            discovery.bytes_written += files
                .data
                .iter()
                .map(|file| file.file_size_in_bytes())
                .sum::<u64>();
            context.register_table(
                format!("keys_{index}"),
                crate::query::table(native, budget.clone(), Some(pin.rows))?,
            )?;
            let _ = (old, new);
        }
        let keys = self
            .write_frame_at(
                &spec.dataset,
                &directory.child("affected-keys")?,
                context
                    .sql("SELECT * FROM keys_0 UNION SELECT * FROM keys_1")
                    .await?,
            )
            .await?;
        discovery.data_files_written += keys.metrics.data_files_written;
        discovery.bytes_written += keys.metrics.bytes_written;
        let affected_native = storage::open_pin(&keys.table, &self.inner.lifecycle.budget).await?;
        let affected_schema = affected_native.metadata().current_schema().clone();
        let affected_data = {
            let inv = storage::current_snapshot_files(&affected_native, budget).await?;
            inv.data
        };
        context.register_table(
            "affected",
            crate::query::table(
                affected_native,
                budget.clone(),
                Some(keys.table.rows),
            )?,
        )?;
        let k_cols: Vec<String> = (0..join.join.keys.len())
            .map(|i| format!("k{i}"))
            .collect();
        let affected_points = if keys.table.rows == 0 {
            None
        } else {
            write::key_points::KeyPoints::collect_table(&context, "affected", &k_cols, budget)
                .await?
        };
        for (index, old_version, new_version) in [
            (0, &previous_join.left, &join.left),
            (1, &previous_join.right, &join.right),
        ] {
            let old = source(&previous.releases[&old_version.dataset])?;
            let new = source(&current.releases[&new_version.dataset])?;
            let side_keys: Vec<String> = join
                .join
                .keys
                .iter()
                .map(|key| {
                    if index == 0 {
                        key.left.clone()
                    } else {
                        key.right.clone()
                    }
                })
                .collect();
            for (prefix, pin) in [("old", old), ("new", new)] {
                let side_native =
                    storage::open_pin(pin, &self.inner.lifecycle.budget).await?;
                if keys.table.rows == 0 {
                    let occ_schema = storage::occurrence_schema(&side_native)?;
                    context.register_table(
                        format!("{prefix}_{index}"),
                        Arc::new(datafusion::datasource::empty::EmptyTable::new(occ_schema)),
                    )?;
                } else {
                    let side_schema = side_native.metadata().current_schema().clone();
                    let side_partition_spec =
                        side_native.metadata().default_partition_spec().clone();
                    let (retained, retained_rows) = {
                        let inv = storage::current_snapshot_files(&side_native, budget).await?;
                        let mut set = std::collections::BTreeSet::new();
                        let mut rows = 0u64;
                        for file in &inv.data {
                            if write::file_may_overlap_mapped_key_files(
                                &side_schema,
                                &side_partition_spec,
                                &affected_schema,
                                &side_keys,
                                &k_cols,
                                &affected_data,
                                file,
                            ) && affected_points
                                .as_ref()
                                .is_none_or(|p| p.may_contain(&side_schema, &side_keys, file))
                            {
                                set.insert(file.file_path().to_owned());
                                rows = rows.saturating_add(file.record_count());
                            }
                        }
                        (set, rows)
                    };
                    context.register_table(
                        format!("{prefix}_{index}"),
                        crate::query::pruned_occurrences(
                            side_native,
                            budget.clone(),
                            Some(retained_rows),
                            Arc::new(retained),
                        )?,
                    )?;
                }
            }
        }
        drop(affected_data);
        drop(affected_points);
        // Null-safe membership also includes unmatched partial-null groups under
        // ordinary SQL equality. The actual join keeps its declared null policy.
        for side in 0..2 {
            let predicate = join
                .join
                .keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    format!(
                        "(s.{} IS NOT DISTINCT FROM k.\"k{index}\")",
                        quote(if side == 0 { &key.left } else { &key.right })
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            for prefix in ["old", "new"] {
                let frame = context
                    .sql(&format!(
                        "SELECT s.* FROM {prefix}_{side} s LEFT SEMI JOIN affected k ON {predicate}"
                    ))
                    .await?;
                context.register_table(format!("{prefix}_scope_{side}"), frame.into_view())?;
            }
        }
        context.register_table(
            "before",
            crate::query::table(
                native.clone(),
                budget.clone(),
                Some(captured_source.rows),
            )?,
        )?;
        let mut joins = Vec::new();
        for side in 0..2 {
            joins.push(format!(
                "LEFT JOIN old_scope_{side} s{side} ON b.{} = s{side}.{} AND b.{} = s{side}.{}",
                quote(TUPLE[side * 2]),
                quote(storage::OCCURRENCE_FILE),
                quote(TUPLE[side * 2 + 1]),
                quote(storage::OCCURRENCE_ROW)
            ));
        }
        let scope = context
            .sql(&format!(
                "SELECT DISTINCT {} FROM before b {} WHERE s0.{} IS NOT NULL OR s1.{} IS NOT NULL",
                tuple_columns(Some("b")),
                joins.join(" "),
                quote(storage::OCCURRENCE_FILE),
                quote(storage::OCCURRENCE_FILE)
            ))
            .await?;
        context.register_table("join_left", context.table("new_scope_0").await?.into_view())?;
        context.register_table(
            "join_right",
            context.table("new_scope_1").await?.into_view(),
        )?;
        let replacement = refresh::join_registered(&context, join).await?;
        let before = source(base)?;
        let key_field_ids = TUPLE
            .iter()
            .map(|key| {
                native
                    .metadata()
                    .current_schema()
                    .field_by_name(key)
                    .map(|field| field.id)
                    .ok_or_else(full_refresh)
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = TUPLE.map(str::to_owned);
        let (write_before, replacement) = self
            .source_replacement(&spec.dataset, captured, directory, replacement, &keys)
            .await?;
        let applied = Box::pin(self.replace_keys(Replacement {
            dataset: &spec.dataset,
            directory: directory.child("replacement")?,
            before: &write_before,
            keys: &keys,
            scope,
            replacement,
            replacement_bytes: None,
        }))
        .await?;
        let mut applied = self
            .finish_source_replacement_at(&spec.dataset, directory, captured_source, applied)
            .await?;
        applied.metrics.files_scanned += discovery.files_scanned;
        applied.metrics.data_files_written += discovery.data_files_written;
        applied.metrics.bytes_written += discovery.bytes_written;
        let changes = ChangePin {
            rules: super::changes::WRITE_RULES.into(),
            base_version: base.id.clone(),
            base_manifest: base.manifest.clone(),
            before_metadata: before.metadata.clone(),
            after_metadata: applied.table.metadata.clone(),
            key_field_ids,
            removed: applied.removed,
            added: applied.added,
        };
        Ok(write::Prepared {
            table: applied.table,
            changes: Some(changes),
            metrics: applied.metrics,
            schema_input: None,
        })
        })
    }

    // Capture each native inventory once, read only changed files, and stream
    // typed keys. Paths identify occurrences even when two files contain equal bytes.
    pub(super) fn changed_keys<'a>(
        &'a self,
        dataset: &'a Id,
        directory: &'a locations::Location,
        versions: (&'a ReleasePin, &'a ReleasePin),
        canonical: (&'a TablePin, &'a TablePin),
        keys: &'a [String],
    ) -> futures::future::BoxFuture<'a, Result<(TablePin, u64)>> {
        Box::pin(async move {
        let budget = &self.inner.lifecycle.budget;
        let old = source(versions.0)?;
        let new = source(versions.1)?;
        let old_native = storage::open_pin(old, &self.inner.lifecycle.budget).await?;
        let schema = storage::occurrence_schema(&old_native)?;
        let indices = keys
            .iter()
            .map(|key| schema.index_of(key))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let key_schema = Arc::new(Schema::new(if keys.is_empty() {
            vec![arrow::datatypes::Field::new(
                "k0",
                arrow::datatypes::DataType::Int64,
                true,
            )]
        } else {
            indices
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    schema
                        .field(*field)
                        .clone()
                        .with_name(format!("k{index}"))
                        .with_nullable(true)
                })
                .collect::<Vec<_>>()
        }));
        let table = self
            .create_output_at(
                dataset,
                directory,
                storage::arrow_schema_to_schema_auto_assign_ids(&key_schema)?,
            )
            .await?;
        // Equivalent representations carry their original logical identity. Resolve
        // this before file discovery so physical rewrites never generate work keys.
        if old.logical_metadata() == new.logical_metadata() {
            return Ok((self.pin_output(dataset, &table, 0, &[]).await?, 0));
        }
        if let Some(discovered) = Box::pin(self.history_keys(
            dataset,
            &table,
            versions,
            canonical,
            &old_native,
            keys,
            key_schema.clone(),
        ))
        .await?
        {
            return Ok(discovered);
        }
        let new_native = storage::open_pin(new, &self.inner.lifecycle.budget).await?;
        let old_scan = storage::OccurrenceScan::open(&old_native, budget).await?;
        let new_scan = storage::OccurrenceScan::open(&new_native, budget).await?;
        let new_indices = keys
            .iter()
            .map(|key| new_scan.schema().index_of(key))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let count = old_scan
            .files()
            .count()
            .saturating_add(new_scan.files().count());
        let map_bytes = count
            .saturating_add(old.objects.len())
            .saturating_add(new.objects.len())
            .saturating_mul(128);
        if map_bytes > budget.limits.max_metadata_bytes {
            return Err(Error::refusal(
                "metadata_capacity",
                "changed-file discovery exceeds metadata allowance",
            ));
        }
        let _maps = budget.reserve(map_bytes)?;
        let old_files = old_scan
            .files()
            .map(|file| (file.file_path(), file))
            .collect::<BTreeMap<_, _>>();
        let new_files = new_scan
            .files()
            .map(|file| (file.file_path(), file))
            .collect::<BTreeMap<_, _>>();
        // One immutable path cannot acquire a different digest or descriptor.
        let old_objects = old
            .objects
            .iter()
            .map(|object| (object.path.as_str(), object))
            .collect::<BTreeMap<_, _>>();
        let new_objects = new
            .objects
            .iter()
            .map(|object| (object.path.as_str(), object))
            .collect::<BTreeMap<_, _>>();
        for (path, file) in &old_files {
            if let Some(other) = new_files.get(path)
                && (file != other || old_objects.get(path) != new_objects.get(path))
            {
                return Err(Error::refusal(
                    "immutable_file_conflict",
                    "a retained occurrence file changed its descriptor or digest",
                ));
            }
        }
        let changed = old_files
            .keys()
            .filter(|path| !new_files.contains_key(*path))
            .map(|path| (&old_scan, *path, indices.as_slice()))
            .chain(
                new_files
                    .keys()
                    .filter(|path| !old_files.contains_key(*path))
                    .map(|path| (&new_scan, *path, new_indices.as_slice())),
            )
            .collect::<Vec<_>>();
        let scanned = changed.len() as u64;
        let rows = if keys.is_empty() {
            // Global scope depends only on immutable file membership. Do not decode payloads.
            let batch = RecordBatch::try_new(
                key_schema.clone(),
                vec![Arc::new(arrow::array::Int64Array::from(if scanned == 0 {
                    vec![]
                } else {
                    vec![0]
                }))],
            )?;
            futures::stream::once(async { Ok(batch) }).boxed()
        } else {
            futures::stream::iter(changed)
                .map(|(scan, path, indices)| scan.read_projected_file(path, budget, Some(indices)))
                .try_flatten()
                .map_ok(move |batch| {
                    RecordBatch::try_new(key_schema.clone(), batch.columns().to_vec())
                        .map_err(Error::from)
                })
                .and_then(futures::future::ready)
                .boxed()
        };
        let files = storage::write_batches(&table, rows, budget).await?;
        let rows = files.iter().map(|file| file.record_count()).sum();
        let table = storage::append(&table, self.inner.lifecycle.catalog.as_ref(), files).await?;
        Ok((
            self.pin_output(dataset, &table, rows, &[]).await?,
            if keys.is_empty() { 0 } else { scanned },
        ))
        })
    }

    pub(super) fn incremental_evidence<'a>(
        &'a self,
        spec: &'a RefreshSpec,
        directory: &'a locations::Location,
        processing: &'a PhaseResult,
        operation: &'a Operation,
    ) -> futures::future::BoxFuture<'a, Result<write::Prepared>> {
        Box::pin(async move {
        let base = self
            .release_admitted(&spec.dataset, spec.base.as_ref().ok_or_else(full_refresh)?)
            .await?;
        check_manifest(&base, operation)?;
        let before = &base
            .dataset
            .related
            .as_ref()
            .ok_or_else(full_refresh)?
            .evidence;
        let changes = processing.changes.as_ref().ok_or_else(full_refresh)?;
        if changes.base_manifest != base.manifest {
            return Err(full_refresh());
        }
        if changes.removed.rows == 0 && changes.added.rows == 0 {
            return Ok(write::Prepared {
                table: before.clone(),
                changes: None,
                metrics: WriteMetrics::default(),
                schema_input: None,
            });
        }
        let context = self.context();
        for (alias, pin) in [("removed", &changes.removed), ("added", &changes.added)] {
            context.register_table(
                alias,
                crate::query::table(
                    storage::open_pin(pin, &self.inner.lifecycle.budget).await?,
                    self.inner.lifecycle.budget.clone(),
                    Some(pin.rows),
                )?,
            )?;
        }
        if let RefreshTransform::Aggregate(_) = &spec.transform {
            return self
                .aggregate_evidence(spec, directory, processing, before, &context)
                .await;
        }
        if let RefreshTransform::ConnectedComponents(_) = &spec.transform {
            return Box::pin(self.connected_components_evidence(
                spec, directory, processing, before, &context,
            ))
            .await;
        }
        let columns = tuple_columns(None);
        let keys = TUPLE.map(str::to_owned);
        let applied = Box::pin(self.replace_keys(Replacement {
            dataset: &spec.dataset,
            directory: directory.clone(),
            before,
            keys: &keys,
            scope: context
                .sql(&format!("SELECT DISTINCT {columns} FROM removed"))
                .await?,
            replacement: context.sql(&format!("SELECT {columns} FROM added")).await?,
            replacement_bytes: Some(
                changes
                    .added
                    .objects
                    .iter()
                    .fold(0u64, |bytes, object| bytes.saturating_add(object.bytes)),
            ),
        }))
        .await?;
        Ok(write::Prepared {
            table: applied.table,
            changes: None,
            metrics: applied.metrics,
            schema_input: None,
        })
        })
    }
}
pub(super) fn source(release: &ReleasePin) -> Result<&TablePin> {
    release
        .dataset
        .groups
        .get("source")
        .ok_or_else(full_refresh)
}

fn check_manifest(base: &ReleasePin, operation: &Operation) -> Result<()> {
    if operation.base_manifest.as_ref() != Some(&base.manifest) {
        return Err(Error::refusal(
            "stale_base",
            "prepared work requires its exact captured base manifest",
        ));
    }
    Ok(())
}

/// Shares incremental provenance qualification between admission and offline reads.
pub(super) async fn qualify(
    spec: &RefreshSpec,
    base: &DatasetPin,
    eligibility: u64,
    budget: &Budget,
) -> Result<()> {
    let related = base.related.as_ref().ok_or_else(full_refresh)?;
    if spec.base.as_ref() != Some(&base.version)
        || base.eligibility != eligibility
        || spec.definition != base.definition
        || spec.definition != related.request.definition
        || !spec.transform.same_definition(&related.request.transform)
    {
        return Err(full_refresh());
    }
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut bytes = 0;
    for (old, new) in related
        .request
        .transform
        .versions()
        .into_iter()
        .zip(spec.transform.versions())
    {
        let old = refresh::read_release(old, budget, &mut bytes, &memory).await?;
        let new = refresh::read_release(new, budget, &mut bytes, &memory).await?;
        let old = storage::open_pin(source(&old)?, budget).await?;
        let new = storage::open_pin(source(&new)?, budget).await?;
        if storage::identity::public_schema(&old)? != storage::identity::public_schema(&new)? {
            return Err(full_refresh());
        }
    }
    Ok(())
}
