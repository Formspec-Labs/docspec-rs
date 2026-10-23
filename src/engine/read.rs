//! Verified native field-group reads through the execution host's shared budget.

use super::*;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use std::collections::BTreeSet;

impl Engine {
    /// Streams one retained field group from an exact release.
    ///
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses unknown groups, invalid retained releases, or exhausted reader capacity.
    pub async fn read_group(
        &self,
        release: &ReleasePin,
        group: &str,
    ) -> Result<SendableRecordBatchStream> {
        self.read_group_projected(release, group, None).await
    }

    /// Streams one retained field group from an exact release with optional column projection.
    ///
    /// # Errors
    /// Refuses unknown groups/columns, invalid retained releases, or exhausted reader capacity.
    pub async fn read_group_projected(
        &self,
        release: &ReleasePin,
        group: &str,
        columns: Option<Vec<String>>,
    ) -> Result<SendableRecordBatchStream> {
        self.require_data_work()?;
        let memory = self.request_memory(&(release, group, &columns))?;
        let release = release.clone();
        let group = group.to_owned();
        self.admit_stream(memory, move |engine| async move {
            let view = engine.check_read_release(&release).await?;
            let canonical = release.dataset.groups.get(&group).ok_or_else(|| {
                Error::refusal("missing_group", "release lacks the requested field group")
            })?;
            let pin = engine
                .representation_pin(&view, &release.id, &group, canonical)
                .await?;
            read_pin_projected_with_objects(
                &engine.inner.objects,
                &pin,
                columns.as_deref(),
                &engine.inner.lifecycle.budget,
            )
            .await
        })
        .await
    }

    /// Streams the exact contributor occurrences retained by a related result.
    ///
    /// Resolves this host's available result once and protects it until EOF or drop.
    ///
    /// # Errors
    /// Refuses absent evidence, invalid retained releases, or exhausted reader capacity.
    pub async fn read_evidence(&self, release: &ReleasePin) -> Result<SendableRecordBatchStream> {
        self.require_data_work()?;
        let memory = self.request_memory(release)?;
        let release = release.clone();
        self.admit_stream(memory, move |engine| async move {
            engine.check_read_release(&release).await?;
            let related = release.dataset.related.as_ref().ok_or_else(|| {
                Error::refusal("missing_evidence", "release has no contributor relation")
            })?;
            read_pin_with_objects(
                &engine.inner.objects,
                &related.evidence,
                &engine.inner.lifecycle.budget,
            )
            .await
        })
        .await
    }

    /// Streams source rows matching a bounded set of scalar keys from an exact release.
    ///
    /// Resolves this host's available result once, prunes non-overlapping files via
    /// Iceberg manifest bounds on values-only scans, and projects/filters matching rows.
    ///
    /// # Errors
    /// Refuses unknown keys/columns, invalid retained releases, or exhausted reader capacity.
    pub async fn read_keys(
        &self,
        release: &ReleasePin,
        spec: &crate::model::KeyReadSpec,
    ) -> Result<SendableRecordBatchStream> {
        self.require_group("source")?;
        let memory = self.request_memory(&(release, spec))?;
        let release = release.clone();
        let spec = spec.clone();
        self.admit_stream(memory, move |engine| async move {
            let view = engine.check_read_release(&release).await?;
            let canonical = release.dataset.groups.get("source").ok_or_else(|| {
                Error::refusal("missing_group", "release lacks the requested field group")
            })?;
            let pin = engine
                .representation_pin(&view, &release.id, "source", canonical)
                .await?;
            read_keys_with_objects(
                &engine.inner.objects,
                &pin,
                &spec,
                &engine.inner.lifecycle.budget,
            )
            .await
        })
        .await
    }

    pub(super) async fn check_read_release(&self, release: &ReleasePin) -> Result<View> {
        if !release.verified || release.id != release.dataset.version {
            return Err(Error::refusal(
                "release_mismatch",
                "read pin is not a completed immutable selection",
            ));
        }
        let lock = self.lock(&release.dataset.dataset).await;
        let _guard = lock.lock().await;
        let lifecycle = &self.inner.lifecycle;
        let view = self
            .existing_lifecycle_view(&release.dataset.dataset)
            .await?;
        self.check_operation_available(&view, &release.id).await?;
        let rows = lifecycle.rows(&view).await?;
        // Completed readbacks are independently retained even if a later failure
        // or abandonment prevented selection. Backfill uses its manifest digest
        // in the readback key, so all lookups remain exact rather than history scans.
        let keys = [
            format!("result/{}", release.id),
            format!("readback/{}", release.id),
            format!(
                "backfill-readback/{}/{}",
                release.id, release.manifest.digest
            ),
        ];
        for key in keys {
            let _memory = rows.reserve_decoded(&lifecycle.budget, &[&key])?;
            if let Some((revision, _, retained)) = rows.get_record::<ReleasePin>(&key)?
                && revision > 0
                && revision <= view.revision
                && retained == *release
            {
                return Ok(view);
            }
        }
        Err(Error::refusal(
            "release_mismatch",
            "read pin differs from this host's available result or completed readback",
        ))
    }

    pub(crate) fn read_budget(&self) -> Budget {
        self.inner.lifecycle.budget.clone()
    }

    pub(crate) fn scratch_dir(&self) -> PathBuf {
        self.inner.root.join("scratch")
    }
}

/// Streams a retained field group without requiring its producing catalog.
///
/// The injected budget shares admission with other readers and application work.
/// The caller must retain the files or rely on a published availability window.
/// A serialized pin identifies content; it creates no lease or host protection.
///
/// # Errors
/// Refuses unknown groups, invalid retained releases, or exhausted reader capacity.
pub async fn read_group(
    release: &ReleasePin,
    group: &str,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let pin = release.dataset.groups.get(group).ok_or_else(|| {
        Error::refusal("missing_group", "release lacks the requested field group")
    })?;
    resolve_release(release, budget).await?;
    read_pin(pin, budget).await
}

/// Streams retained contributor occurrences without the producing catalog.
///
/// The caller must retain the files or rely on a published availability window.
/// A serialized pin identifies content; it creates no lease or host protection.
///
/// # Errors
/// Refuses absent evidence, invalid retained releases, or exhausted reader capacity.
pub async fn read_evidence(
    release: &ReleasePin,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let related =
        release.dataset.related.as_ref().ok_or_else(|| {
            Error::refusal("missing_evidence", "release has no contributor relation")
        })?;
    resolve_release(release, budget).await?;
    read_pin(&related.evidence, budget).await
}

pub(super) async fn read_pin(pin: &TablePin, budget: &Budget) -> Result<SendableRecordBatchStream> {
    let objects = storage::Objects::for_path(&pin.metadata.path);
    read_pin_with_objects(&objects, pin, budget).await
}

pub(super) async fn read_pin_with_objects(
    objects: &storage::Objects,
    pin: &TablePin,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    read_pin_projected_with_objects(objects, pin, None, budget).await
}

pub(super) async fn read_pin_projected_with_objects(
    objects: &storage::Objects,
    pin: &TablePin,
    columns: Option<&[String]>,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let table = objects.open_pin(pin, budget).await?;
    let public_schema = storage::identity::public_schema(&table)?;
    let Some(cols) = columns else {
        let stream = storage::read_batches(&table, budget)
            .await?
            .and_then(|batch| async move { storage::identity::public_batch(batch) })
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            public_schema,
            stream,
        )));
    };
    if cols.is_empty()
        || cols
            .iter()
            .any(|c| c.is_empty() || c.to_ascii_lowercase().starts_with("__docspec_"))
        || cols.iter().collect::<BTreeSet<_>>().len() != cols.len()
    {
        return Err(Error::refusal(
            "invalid_projection",
            "requested columns must be distinct public columns",
        ));
    }
    let public_indices = cols
        .iter()
        .map(|name| {
            public_schema.index_of(name).map_err(|_| {
                Error::refusal(
                    "invalid_projection",
                    "requested column is absent from the group schema",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let out_schema = Arc::new(public_schema.project(&public_indices)?);
    let read_schema = storage::schema::read_schema(&table)?;
    let read_indices = cols
        .iter()
        .map(|name| {
            read_schema.index_of(name).map_err(|_| {
                Error::refusal(
                    "invalid_projection",
                    "requested column is absent from the physical read schema",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let stream = storage::read_projected_batches(&table, budget, Some(&read_indices))
        .await?
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
}

/// Streams a verified physical representation without requiring its producer.
/// The caller retains its immutable files for the lifetime of the stream.
///
/// # Errors
/// Refuses unverified representations, unreadable native files, or capacity errors.
pub async fn read_compaction(
    result: &CompactionResult,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    if !result.verified {
        return Err(Error::refusal(
            "invalid_compaction",
            "representation is not verified",
        ));
    }
    read_pin(&result.replacement, budget).await
}

/// Streams source rows matching `spec` without requiring the producing catalog.
///
/// # Errors
/// Refuses invalid releases, unknown keys/columns, or exhausted reader capacity.
pub async fn read_keys(
    release: &ReleasePin,
    spec: &crate::model::KeyReadSpec,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    let pin = release.dataset.groups.get("source").ok_or_else(|| {
        Error::refusal("missing_group", "release lacks the requested field group")
    })?;
    resolve_release(release, budget).await?;
    read_keys_with_objects(&storage::Objects::Local, pin, spec, budget).await
}

pub(super) async fn read_keys_with_objects(
    objects: &storage::Objects,
    pin: &TablePin,
    spec: &crate::model::KeyReadSpec,
    budget: &Budget,
) -> Result<SendableRecordBatchStream> {
    if spec.key.is_empty() || spec.key.to_ascii_lowercase().starts_with("__docspec_") {
        return Err(Error::refusal(
            "invalid_key_read",
            "key read requires a public source key column",
        ));
    }
    let table = objects.open_pin(pin, budget).await?;
    let public_schema = storage::identity::public_schema(&table)?;
    let key_public_idx = public_schema.index_of(&spec.key).map_err(|_| {
        Error::refusal(
            "invalid_key_read",
            "requested key column is absent from the source schema",
        )
    })?;
    if !matches!(
        public_schema.field(key_public_idx).data_type(),
        arrow::datatypes::DataType::Utf8
            | arrow::datatypes::DataType::LargeUtf8
            | arrow::datatypes::DataType::Utf8View
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::Decimal128(_, 0)
            | arrow::datatypes::DataType::Binary
            | arrow::datatypes::DataType::LargeBinary
            | arrow::datatypes::DataType::FixedSizeBinary(_)
            | arrow::datatypes::DataType::Date32
            | arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
    ) {
        return Err(Error::refusal(
            "invalid_key_read",
            "key read requires a scalar Utf8, Int32, Int64, UInt64/Decimal128, Binary, Date32, or TimestampMicrosecond key column",
        ));
    }
    let out_indices: Vec<usize> = match &spec.columns {
        Some(cols) => {
            if cols.is_empty()
                || cols
                    .iter()
                    .any(|c| c.is_empty() || c.to_ascii_lowercase().starts_with("__docspec_"))
                || cols.iter().collect::<BTreeSet<_>>().len() != cols.len()
            {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "requested columns must be distinct public source columns",
                ));
            }
            cols.iter()
                .map(|name| {
                    public_schema.index_of(name).map_err(|_| {
                        Error::refusal(
                            "invalid_key_read",
                            "requested column is absent from the source schema",
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
        None => (0..public_schema.fields().len()).collect(),
    };
    let out_schema = Arc::new(public_schema.project(&out_indices)?);
    if spec.values.is_empty() || pin.snapshot.is_none() || pin.rows == 0 {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema,
            futures::stream::empty(),
        )));
    }
    let inventory = storage::current_snapshot_files(&table, budget).await?;
    let scan = Arc::new(storage::OccurrenceScan::values(&table, budget, inventory).await?);
    let scan_schema = scan.schema();
    let mut scan_indices = Vec::with_capacity(out_indices.len() + 1);
    for &pub_idx in &out_indices {
        let name = public_schema.field(pub_idx).name();
        let idx = scan_schema.index_of(name)?;
        if !scan_indices.contains(&idx) {
            scan_indices.push(idx);
        }
    }
    let key_scan_idx = scan_schema.index_of(&spec.key)?;
    if !scan_indices.contains(&key_scan_idx) {
        scan_indices.push(key_scan_idx);
    }
    let projected_schema = Arc::new(scan_schema.project(&scan_indices)?);
    let key_in_projected = projected_schema.index_of(&spec.key)?;
    let out_in_projected: Vec<usize> = out_schema
        .fields()
        .iter()
        .map(|f| projected_schema.index_of(f.name()))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let key_field = table.metadata().current_schema().field_by_name(&spec.key);
    let key_field_id = key_field.map(|f| f.id);
    // Parse requested values once into the key column's own domain. Integer keys
    // compare as integers: no per-row string formatting, and values that cannot
    // occur in the column (non-numeric, out of range) are dropped up front.
    let target = Arc::new(KeyTargets::parse(
        public_schema.field(key_public_idx).data_type(),
        &spec.values,
    ));
    if target.is_empty() {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema,
            futures::stream::empty(),
        )));
    }
    let key_partitions = key_field
        .map(|field| {
            target.partition_filters(
                table.metadata().default_partition_spec(),
                field.id,
                &field.field_type,
            )
        })
        .unwrap_or_default();
    let candidate_paths: Vec<String> = scan
        .files()
        .filter(|file| target.file_may_contain(file, key_field_id, &key_partitions))
        .map(|file| file.file_path().to_owned())
        .collect();

    let budget_clone = budget.clone();
    let stream =
        futures::stream::iter(candidate_paths)
            .map(move |path| scan.read_projected_file(&path, &budget_clone, Some(&scan_indices)))
            .try_flatten()
            .and_then(move |batch| {
                let target = target.clone();
                let out_in_projected = out_in_projected.clone();
                async move {
                    filter_batch_by_keys(&batch, key_in_projected, &target, &out_in_projected)
                }
            })
            .try_filter(|batch| futures::future::ready(batch.num_rows() > 0))
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));

    Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
}

/// Requested key values in the key column's comparison domain.
#[derive(Debug)]
enum KeyTargets {
    Strings(BTreeSet<String>),
    Ints(BTreeSet<i64>),
    Int128s(BTreeSet<i128>),
    Binaries(BTreeSet<Vec<u8>>),
    Dates(BTreeSet<i32>),
    Timestamps(BTreeSet<i64>),
}

fn parse_binary_key_candidates(value: &str) -> Vec<Vec<u8>> {
    let mut out = vec![value.as_bytes().to_vec()];
    let hex_str = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .or_else(|| value.strip_prefix("hex:"))
        .or_else(|| value.strip_prefix("HEX:"))
        .unwrap_or(value);
    if !hex_str.is_empty()
        && hex_str.len().is_multiple_of(2)
        && let Some(decoded) = (0..hex_str.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).ok())
            .collect::<Option<Vec<u8>>>()
        && !out.contains(&decoded)
    {
        out.push(decoded);
    }
    out
}

impl KeyTargets {
    fn parse(data_type: &arrow::datatypes::DataType, values: &[String]) -> Self {
        use arrow::{
            array::Array,
            datatypes::{DataType, TimeUnit},
        };
        match data_type {
            DataType::Int64 => Self::Ints(values.iter().filter_map(|v| v.parse().ok()).collect()),
            DataType::Int32 => Self::Ints(
                values
                    .iter()
                    .filter_map(|v| v.parse::<i32>().ok())
                    .map(i64::from)
                    .collect(),
            ),
            DataType::UInt64 | DataType::Decimal128(_, 0) => {
                Self::Int128s(values.iter().filter_map(|v| v.parse().ok()).collect())
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Self::Binaries(
                    values
                        .iter()
                        .flat_map(|v| parse_binary_key_candidates(v))
                        .collect(),
                )
            }
            DataType::Date32 => Self::Dates(
                values
                    .iter()
                    .filter_map(|v| {
                        if let Ok(days) = v.parse::<i32>() {
                            return Some(days);
                        }
                        let src = arrow::array::StringArray::from(vec![v.as_str()]);
                        let casted = arrow::compute::cast(&src, &DataType::Date32).ok()?;
                        let arr = casted
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()?;
                        (!arr.is_null(0)).then(|| arr.value(0))
                    })
                    .collect(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::Timestamps(
                values
                    .iter()
                    .filter_map(|v| {
                        if let Ok(micros) = v.parse::<i64>() {
                            return Some(micros);
                        }
                        let src = arrow::array::StringArray::from(vec![v.as_str()]);
                        let casted = arrow::compute::cast(
                            &src,
                            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                        )
                        .or_else(|_| {
                            arrow::compute::cast(
                                &src,
                                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                            )
                        })
                        .ok()?;
                        let arr = casted
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?;
                        (!arr.is_null(0)).then(|| arr.value(0))
                    })
                    .collect(),
            ),
            _ => Self::Strings(values.iter().cloned().collect()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Strings(set) => set.is_empty(),
            Self::Ints(set) => set.is_empty(),
            Self::Int128s(set) => set.is_empty(),
            Self::Binaries(set) => set.is_empty(),
            Self::Dates(set) => set.is_empty(),
            Self::Timestamps(set) => set.is_empty(),
        }
    }

    fn partition_filters(
        &self,
        partition_spec: &iceberg::spec::PartitionSpec,
        field_id: i32,
        field_type: &iceberg::spec::Type,
    ) -> Vec<(
        usize,
        std::collections::HashSet<iceberg::spec::PrimitiveLiteral>,
    )> {
        use iceberg::spec::{PrimitiveLiteral, PrimitiveType, Type};
        let Type::Primitive(prim) = field_type else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (part_idx, pf) in partition_spec.fields().iter().enumerate() {
            if pf.source_id != field_id {
                continue;
            }
            let mut set = std::collections::HashSet::new();
            let mut complete = true;
            match (self, prim) {
                (Self::Strings(values), PrimitiveType::String) => {
                    for value in values {
                        let lit = PrimitiveLiteral::String(value.clone());
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Ints(values), PrimitiveType::Int) => {
                    for value in values {
                        let Ok(v) = i32::try_from(*value) else {
                            continue;
                        };
                        let lit = PrimitiveLiteral::Int(v);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Ints(values), PrimitiveType::Long) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Long(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (Self::Dates(values), PrimitiveType::Date) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Int(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                (
                    Self::Timestamps(values),
                    PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                ) => {
                    for value in values {
                        let lit = PrimitiveLiteral::Long(*value);
                        if let Some(part) =
                            storage::compute_partition_literal(pf.transform, field_type, &lit)
                        {
                            set.insert(part);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                }
                _ => {
                    complete = false;
                }
            }
            if complete {
                out.push((part_idx, set));
            }
        }
        out
    }

    /// Returns `false` only when partition metadata or exact (untruncated) manifest
    /// bounds prove that no requested value lies inside the file's key range.
    fn file_may_contain(
        &self,
        file: &iceberg::spec::DataFile,
        key_field_id: Option<i32>,
        key_partitions: &[(
            usize,
            std::collections::HashSet<iceberg::spec::PrimitiveLiteral>,
        )],
    ) -> bool {
        use iceberg::spec::{Literal, PrimitiveLiteral};
        for (part_idx, allowed) in key_partitions {
            let Some(slot) = file.partition().fields().get(*part_idx) else {
                continue;
            };
            match slot {
                None => return false,
                Some(Literal::Primitive(p)) => {
                    if !allowed.contains(p) {
                        return false;
                    }
                }
                _ => {}
            }
        }
        let Some(field_id) = key_field_id else {
            return true;
        };
        let (Some(low), Some(high)) = (
            file.lower_bounds()
                .get(&field_id)
                .and_then(super::write::untruncated_bound),
            file.upper_bounds()
                .get(&field_id)
                .and_then(super::write::untruncated_bound),
        ) else {
            return true;
        };
        match (self, low, high) {
            (Self::Strings(set), PrimitiveLiteral::String(low), PrimitiveLiteral::String(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (Self::Ints(set), low, high) => {
                let as_i64 = |lit| match lit {
                    PrimitiveLiteral::Int(v) => Some(i64::from(v)),
                    PrimitiveLiteral::Long(v) => Some(v),
                    _ => None,
                };
                match (as_i64(low), as_i64(high)) {
                    (Some(low), Some(high)) => low > high || set.range(low..=high).next().is_some(),
                    _ => true,
                }
            }
            (Self::Int128s(set), PrimitiveLiteral::Int128(low), PrimitiveLiteral::Int128(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (
                Self::Binaries(set),
                PrimitiveLiteral::Binary(low),
                PrimitiveLiteral::Binary(high),
            ) => low > high || set.range(low..=high).next().is_some(),
            (Self::Dates(set), PrimitiveLiteral::Int(low), PrimitiveLiteral::Int(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            (Self::Timestamps(set), PrimitiveLiteral::Long(low), PrimitiveLiteral::Long(high)) => {
                low > high || set.range(low..=high).next().is_some()
            }
            _ => true,
        }
    }
}

fn filter_batch_by_keys(
    batch: &RecordBatch,
    key_idx: usize,
    target: &KeyTargets,
    out_indices: &[usize],
) -> Result<RecordBatch> {
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
        TimestampMicrosecondArray, UInt64Array,
    };
    let col = batch.column(key_idx);
    let any = col.as_any();
    let mask: BooleanArray = match target {
        KeyTargets::Strings(set) => {
            if let Some(arr) = any.downcast_ref::<StringArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<StringViewArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
        KeyTargets::Ints(set) => {
            if let Some(arr) = any.downcast_ref::<Int64Array>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<Int32Array>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&i64::from(v)))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
        KeyTargets::Int128s(set) => {
            if let Some(arr) = any.downcast_ref::<Decimal128Array>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<UInt64Array>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&i128::from(v)))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
        KeyTargets::Binaries(set) => {
            if let Some(arr) = any.downcast_ref::<BinaryArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<LargeBinaryArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else if let Some(arr) = any.downcast_ref::<FixedSizeBinaryArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(v))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
        KeyTargets::Dates(set) => {
            if let Some(arr) = any.downcast_ref::<Date32Array>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&v))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
        KeyTargets::Timestamps(set) => {
            if let Some(arr) = any.downcast_ref::<TimestampMicrosecondArray>() {
                arr.iter()
                    .map(|v| Some(v.is_some_and(|v| set.contains(&v))))
                    .collect()
            } else {
                return Err(Error::refusal(
                    "invalid_key_read",
                    "unsupported physical array type for key column",
                ));
            }
        }
    };
    let projected = batch.project(out_indices)?;
    Ok(arrow::compute::filter_record_batch(&projected, &mask)?)
}
