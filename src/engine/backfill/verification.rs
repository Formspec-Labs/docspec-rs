//! Independent native reads establish coverage, values and input binding history.

pub(crate) use super::super::verification::verify_groups;

use super::{origins::ScalarOriginBinding, *};
use arrow::array::{Array, Int64Array, StringArray};

/// Rechecks metadata needed by cached scalar results, without decoding values.
pub(in crate::engine) async fn verify_binding_dependencies(
    dataset: &DatasetPin,
    budget: &Budget,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    if dataset.computed.is_empty() {
        return Ok(());
    }
    let mut history = History::new(budget, checks.objects().clone())?;
    let source = dataset
        .groups
        .get("source")
        .ok_or_else(|| invalid_group("dataset is missing source group"))?;
    for (name, binding) in &dataset.computed {
        verify_binding_history(dataset, name, &mut history, checks).await?;
        let group = dataset
            .groups
            .get(name)
            .ok_or_else(|| invalid_group("dataset is missing table pin for computed group"))?;
        checks
            .scalar_origins(
                binding.origins.as_ref(),
                ScalarOriginBinding {
                    source,
                    group,
                    definition: &binding.definition,
                    input_fields: &binding.input_fields,
                    native_field_ids: &binding.native_field_ids,
                },
                budget,
            )
            .await?;
    }
    Ok(())
}

/// Verifies every attached computed group's request provenance, binding history, scalar origins,
/// and physical table data against `source`.
pub(in crate::engine) async fn verify_computed_groups(
    dataset: &DatasetPin,
    source: &TablePin,
    history: &mut History<'_>,
    budget: &Budget,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    for (name, binding) in &dataset.computed {
        if name != binding.definition.group.as_str()
            || binding.request.dataset() != &dataset.dataset
            || binding.request.result_definition() != dataset.definition
            || binding.request.inputs().iter().any(|(name, input)| {
                dataset
                    .inputs
                    .get(&binding.request.input_key(name))
                    .is_none_or(|object| object.digest != input.digest)
            })
        {
            return Err(invalid_group(
                "computed group name, dataset, result definition, or input digest disagrees with release",
            ));
        }
        verify_binding_history(dataset, name, history, checks).await?;
        let group = dataset
            .groups
            .get(name)
            .ok_or_else(|| invalid_group("dataset is missing table pin for computed group"))?;
        let origin_binding = ScalarOriginBinding {
            source,
            group,
            definition: &binding.definition,
            input_fields: &binding.input_fields,
            native_field_ids: &binding.native_field_ids,
        };
        checks
            .scalar_origins(binding.origins.as_ref(), origin_binding, budget)
            .await?;
        verify_group_data(origin_binding, budget, checks).await?;
    }
    Ok(())
}

// Independent receipt and field-group selections can advance while scalar work
// runs. Establish exactly which input additions came from those selections;
// equal source rows alone never authorize an unrelated provenance change.
pub(in crate::engine) async fn compatible_input_history(
    base: &DatasetPin,
    parent: &DatasetPin,
    history: &mut History<'_>,
) -> Result<()> {
    if base.inputs == parent.inputs {
        return Ok(());
    }
    let mut retained = None;
    let mut visited = BTreeSet::new();
    loop {
        let current = retained.as_deref().unwrap_or(parent);
        if current.version == base.version {
            return if current == base {
                Ok(())
            } else {
                Err(invalid_group(
                    "ancestor version matching base version has different manifest contents",
                ))
            };
        }
        if !visited.insert(current.version.clone())
            || current.dataset != base.dataset
            || current.definition != base.definition
            || current.eligibility != base.eligibility
            || current.boundary != base.boundary
            || current.groups.get("source") != base.groups.get("source")
            || current.related != base.related
            || current.relationship != base.relationship
        {
            return Err(invalid_group(
                "intermediate release between base and parent modified source or dataset invariants",
            ));
        }
        let previous =
            history
                .read(current.parent_manifest.as_ref().ok_or_else(|| {
                    invalid_group("intermediate release is missing parent_manifest")
                })?)
                .await?;
        let mut expected = previous.inputs.clone();
        match &current.request {
            OperationRequest::Evidence(spec) => {
                if spec.base != previous.version
                    || current.groups != previous.groups
                    || !super::super::corrections::inherits_groups(
                        &current.computed,
                        &previous.computed,
                    )
                {
                    return Err(invalid_group(
                        "intermediate evidence release disagrees with its parent groups or version",
                    ));
                }
                for (name, input) in current.request.inputs() {
                    let key = current.request.input_key(&name);
                    let object = current.inputs.get(&key).ok_or_else(|| {
                        invalid_group("intermediate evidence release is missing declared input")
                    })?;
                    if object.digest != input.digest
                        || expected.insert(key, object.clone()).is_some()
                    {
                        return Err(invalid_group(
                            "intermediate evidence input digest disagrees or overwrites existing input",
                        ));
                    }
                }
            }
            OperationRequest::Backfill(_) => {}
            _ => {
                return Err(invalid_group(
                    "intermediate release between base and parent is neither Evidence nor Backfill",
                ));
            }
        }
        if expected != current.inputs {
            return Err(invalid_group(
                "intermediate release inputs do not match expected inherited inputs",
            ));
        }
        retained = Some(previous);
        tokio::task::consume_budget().await;
    }
}

pub(in crate::engine) fn check_refreshed_groups(
    dataset: &DatasetPin,
    base: &DatasetPin,
    parent: &ObjectRef,
) -> Result<()> {
    if base.computed.len() != dataset.computed.len() {
        return Err(invalid_group(
            "refreshed dataset computed group count differs from base",
        ));
    }
    for (name, previous) in &base.computed {
        let refreshed = dataset
            .computed
            .get(name)
            .ok_or_else(|| invalid_group("refreshed dataset is missing base computed group"))?;
        if refreshed.definition != previous.definition
            || refreshed.request != dataset.request
            || &refreshed.base_manifest != parent
        {
            return Err(invalid_group(
                "refreshed computed group definition, request, or base_manifest disagrees",
            ));
        }
    }
    Ok(())
}

/// Request-local bounded cache sharing decoded dataset manifests across scalar group checks.
pub(in crate::engine) struct History<'a> {
    entries: BTreeMap<String, (ObjectRef, Arc<DatasetPin>)>,
    bytes: usize,
    memory: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub(in crate::engine) budget: &'a Budget,
    objects: storage::Objects,
}
impl<'a> History<'a> {
    pub(in crate::engine) fn new(budget: &'a Budget, objects: storage::Objects) -> Result<Self> {
        Ok(Self {
            entries: BTreeMap::new(),
            bytes: 0,
            memory: Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?),
            budget,
            objects,
        })
    }
    pub(in crate::engine) async fn read(&mut self, object: &ObjectRef) -> Result<Arc<DatasetPin>> {
        if let Some((expected, dataset)) = self.entries.get(&object.path) {
            return if expected == object {
                Ok(dataset.clone())
            } else {
                Err(invalid_group(
                    "cached history manifest path resolves to a conflicting object reference",
                ))
            };
        }
        let dataset: DatasetPin = inputs::read_manifest(
            &self.objects,
            object,
            self.budget,
            &mut self.bytes,
            &self.memory,
        )
        .await?;
        if !supported_format(dataset.format) {
            return Err(invalid_group(
                "historical dataset manifest has unsupported format",
            ));
        }
        let dataset = Arc::new(dataset);
        self.entries
            .insert(object.path.clone(), (object.clone(), dataset.clone()));
        Ok(dataset)
    }
    pub(in crate::engine) async fn base(
        &mut self,
        binding: &ComputedGroup,
    ) -> Result<Arc<DatasetPin>> {
        let base = self.read(&binding.base_manifest).await?;
        if &base.dataset != binding.request.dataset()
            || Some(&base.version) != binding.request.base()
        {
            return Err(invalid_group(
                "computed group base manifest dataset or version disagrees with binding request",
            ));
        }
        Ok(base)
    }
}

// Walk actual owning selections, not a binding's self-declared operation kind.
// Each inherited step must reach the original authored backfill.
async fn verify_binding_history(
    dataset: &DatasetPin,
    name: &str,
    history: &mut History<'_>,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    let mut retained = None;
    let mut visited = BTreeSet::new();
    loop {
        let owner = retained.as_deref().unwrap_or(dataset);
        let binding = owner
            .computed
            .get(name)
            .ok_or_else(|| invalid_group("owning release is missing computed group binding"))?;
        let source = owner
            .groups
            .get("source")
            .ok_or_else(|| invalid_group("owning release is missing source group"))?;
        if !visited.insert((owner.dataset.clone(), owner.version.clone()))
            || owner.request.dataset() != &owner.dataset
            || owner.request.operation() != &owner.version
            || binding.definition.group.as_str() != name
            || binding.source_metadata != source.metadata
        {
            return Err(invalid_group(
                "owning release identity, group name, or source_metadata is invalid or cyclic",
            ));
        }
        // Validate each owning selection's immutable proof, including intermediate
        // inherited bindings. Historical verification never reads current corrections.
        checks
            .scalar_qualification(
                binding,
                source,
                owner
                    .groups
                    .get(name)
                    .ok_or_else(|| invalid_group("owning release is missing group table pin"))?,
                history.budget,
            )
            .await?;
        if matches!(&owner.request, OperationRequest::Evidence(_)) {
            let parent =
                history
                    .read(owner.parent_manifest.as_ref().ok_or_else(|| {
                        invalid_group("evidence owner is missing parent_manifest")
                    })?)
                    .await?;
            if parent.dataset != owner.dataset
                || !parent
                    .computed
                    .get(name)
                    .is_some_and(|previous| super::super::corrections::inherits(binding, previous))
                || parent.groups.get("source") != Some(source)
                || parent.groups.get(name) != owner.groups.get(name)
            {
                return Err(invalid_group(
                    "evidence owner does not validly inherit computed group from parent",
                ));
            }
            retained = Some(parent);
        } else if let OperationRequest::Backfill(spec) = &owner.request {
            if spec.group.as_str() == name {
                let base = history.base(binding).await?;
                if binding.request != owner.request
                    || binding.definition != spec.group_definition()
                    || base.groups.get("source") != Some(source)
                {
                    return Err(invalid_group(
                        "authored backfill binding request, definition, or base source disagrees",
                    ));
                }
                let native = storage::open_pin(source, history.budget).await?;
                let expected = Expressions::compile(
                    &history.budget.context(),
                    native.metadata().current_schema(),
                    &binding.definition,
                    None,
                )?;
                return if expected.input_fields == binding.input_fields {
                    Ok(())
                } else {
                    Err(invalid_group(
                        "authored backfill input_fields disagree with compiled expression inputs",
                    ))
                };
            }
            // A sibling addition inherits this binding and its exact value table.
            let parent = history
                .read(owner.parent_manifest.as_ref().ok_or_else(|| {
                    invalid_group("sibling backfill owner is missing parent_manifest")
                })?)
                .await?;
            if parent.dataset != owner.dataset
                || !parent
                    .computed
                    .get(name)
                    .is_some_and(|previous| super::super::corrections::inherits(binding, previous))
                || parent.groups.get("source") != Some(source)
                || parent.groups.get(name) != owner.groups.get(name)
            {
                return Err(invalid_group(
                    "sibling backfill owner does not validly inherit computed group from parent",
                ));
            }
            retained = Some(parent);
        } else {
            let parent_ref = owner
                .parent_manifest
                .as_ref()
                .ok_or_else(|| invalid_group("refreshing owner is missing parent_manifest"))?;
            if binding.request != owner.request || &binding.base_manifest != parent_ref {
                return Err(invalid_group(
                    "refreshed binding request or base_manifest disagrees with owning release",
                ));
            }
            let parent = history.base(binding).await?;
            let previous = parent.computed.get(name).ok_or_else(|| {
                invalid_group("parent release is missing inherited computed group")
            })?;
            let before = parent
                .groups
                .get("source")
                .ok_or_else(|| invalid_group("parent release is missing source group"))?;
            if previous.definition != binding.definition
                || inherited_inputs(&owner.request, previous, before, source, history.budget)
                    .await?
                    != binding.input_fields
            {
                return Err(invalid_group(
                    "refreshed binding definition or inherited input fields disagree with parent",
                ));
            }
            // Identity-preserving source operations cannot secretly renumber IDs.
            if matches!(&owner.request, OperationRequest::Update(_))
                || matches!(&owner.request, OperationRequest::Refresh(spec) if spec.mode == RefreshMode::Incremental)
            {
                let before = storage::open_pin(before, history.budget).await?;
                let after = storage::open_pin(source, history.budget).await?;
                if !storage::identity::preserves_schema(
                    before.metadata().current_schema(),
                    after.metadata().current_schema(),
                )? {
                    return Err(invalid_group(
                        "update or incremental refresh did not preserve source field IDs",
                    ));
                }
            }
            if let OperationRequest::Schema(spec) = &owner.request {
                super::super::schema::verify_transition(spec, &parent, owner, history.budget)
                    .await?;
            }
            retained = Some(parent);
        }
        tokio::task::consume_budget().await;
    }
}

pub(super) async fn verify_group_data(
    binding: ScalarOriginBinding<'_>,
    budget: &Budget,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    let ScalarOriginBinding {
        source,
        group,
        definition,
        input_fields,
        native_field_ids,
    } = binding;
    if group.rows != source.rows {
        return Err(invalid_group(
            "computed group row count does not match source row count",
        ));
    }
    checks.table(group, budget).await?;
    let source_native = storage::open_pin(source, budget).await?;
    let group_native = storage::open_pin(group, budget).await?;
    let expressions = Expressions::compile(
        &budget.context(),
        source_native.metadata().current_schema(),
        definition,
        Some(input_fields),
    )?;
    if expressions.native != **group_native.metadata().current_schema()
        || &expressions.native_field_ids != native_field_ids
    {
        return Err(Error::refusal(
            "invalid_group",
            "computed schema or logical-to-native field mapping differs",
        ));
    }
    if !checks.deep() {
        return Ok(());
    }
    if !storage::identity::has_origins(source_native.metadata().current_schema())? {
        let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
        let inventory = storage::snapshot_files(&source_native, budget).await?;
        let mut coverage: BTreeMap<String, FileCoverage> = inventory
            .data
            .iter()
            .map(|file| {
                (
                    file.file_path().to_owned(),
                    FileCoverage {
                        rows: file.record_count(),
                        ranges: BTreeMap::new(),
                    },
                )
            })
            .collect();
        let mut segments = 0usize;
        let stream = storage::read_batches(&group_native, budget).await?;
        futures::pin_mut!(stream);
        while let Some(batch) = stream.try_next().await? {
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid_group("group occurrence file column is not Utf8"))?;
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid_group("group occurrence row column is not Int64"))?;
            if paths.null_count() != 0 || positions.null_count() != 0 {
                return Err(invalid_group(
                    "group occurrence file or row column contains nulls",
                ));
            }
            let mut index = 0;
            while index < batch.num_rows() {
                let path = paths.value(index);
                let start = u64::try_from(positions.value(index))
                    .map_err(|_| invalid_group("group occurrence row position is negative"))?;
                let mut end = start
                    .checked_add(1)
                    .ok_or_else(|| invalid_group("group occurrence row position overflows"))?;
                index += 1;
                while index < batch.num_rows()
                    && paths.value(index) == path
                    && positions.value(index) >= 0
                    && positions.value(index) as u64 == end
                {
                    end = end
                        .checked_add(1)
                        .ok_or_else(|| invalid_group("group occurrence row range end overflows"))?;
                    index += 1;
                }
                let file = coverage.get_mut(path).ok_or_else(|| {
                    invalid_group("group occurrence references unknown source file path")
                })?;
                let before = file.ranges.len();
                file.insert(start, end)?;
                segments = segments - before + file.ranges.len();
                if segments.saturating_mul(64) > budget.limits.max_metadata_bytes {
                    return Err(Error::refusal(
                        "metadata_capacity",
                        "group coverage intervals exceed verification allowance",
                    ));
                }
            }
        }
        if coverage.values().any(|file| !file.complete()) {
            return Err(invalid_group(
                "computed group does not completely cover all source file row ranges",
            ));
        }
    }
    // A full join detects every absent or invented stable identity. Native row
    // counts are equal and source identities are unique, so complete coverage
    // also excludes repeated computed occurrences without another payload scan.
    let context = budget.context();
    context.register_table(
        "source",
        crate::query::occurrences(source_native, budget.clone(), Some(source.rows))?,
    )?;
    context.register_table(
        "actual",
        crate::query::table(group_native, budget.clone(), Some(group.rows))?,
    )?;
    let mut columns = vec![col(storage::OCCURRENCE_FILE), col(storage::OCCURRENCE_ROW)];
    columns.extend(
        expressions
            .logical
            .into_iter()
            .enumerate()
            .map(|(index, expression)| {
                Expr::Cast(datafusion::logical_expr::expr::Cast::new(
                    Box::new(expression),
                    expressions.schema.field(index + 2).data_type().clone(),
                ))
                .alias(&definition.fields[index].name)
            }),
    );
    context.register_table(
        "expected",
        context.table("source").await?.select(columns)?.into_view(),
    )?;
    use super::super::write::qualified;
    use datafusion::logical_expr::{BinaryExpr, JoinType, Operator};
    let different = definition
        .fields
        .iter()
        .map(|field| {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(qualified("e", &field.name)),
                Operator::IsDistinctFrom,
                Box::new(qualified("a", &field.name)),
            ))
        })
        .reduce(Expr::or)
        .ok_or_else(|| invalid_group("computed group definition has no fields"))?
        .or(qualified("e", storage::OCCURRENCE_FILE).is_null())
        .or(qualified("a", storage::OCCURRENCE_FILE).is_null());
    let locations = [storage::OCCURRENCE_FILE, storage::OCCURRENCE_ROW]
        .into_iter()
        .map(|field| qualified("e", field).eq(qualified("a", field)));
    let batches = context
        .table("expected")
        .await?
        .alias("e")?
        .join_on(
            context.table("actual").await?.alias("a")?,
            JoinType::Full,
            locations,
        )?
        .filter(different)?
        .select(vec![lit(1)])?
        .limit(0, Some(1))?
        .collect()
        .await?;
    if batches.iter().any(|batch| batch.num_rows() != 0) {
        return Err(Error::refusal(
            "invalid_group_values",
            "computed values differ from independent source reconstruction",
        ));
    }
    Ok(())
}

struct FileCoverage {
    rows: u64,
    ranges: BTreeMap<u64, u64>,
}

impl FileCoverage {
    fn insert(&mut self, mut start: u64, mut end: u64) -> Result<()> {
        if start >= end || end > self.rows {
            return Err(invalid_group(
                "file coverage interval is empty or exceeds source file row count",
            ));
        }
        if let Some((&previous, &stop)) = self.ranges.range(..=start).next_back() {
            if stop > start {
                return Err(invalid_group(
                    "file coverage interval overlaps with a preceding interval",
                ));
            }
            if stop == start {
                start = previous;
                self.ranges.remove(&previous);
            }
        }
        if let Some((&next, &stop)) = self.ranges.range(start..).next() {
            if next < end {
                return Err(invalid_group(
                    "file coverage interval overlaps with a succeeding interval",
                ));
            }
            if next == end {
                end = stop;
                self.ranges.remove(&next);
            }
        }
        self.ranges.insert(start, end);
        Ok(())
    }
    fn complete(&self) -> bool {
        if self.rows == 0 {
            self.ranges.is_empty()
        } else {
            self.ranges.len() == 1 && self.ranges.get(&0) == Some(&self.rows)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history_dataset(version: &str) -> DatasetPin {
        let request = RunSpec {
            dataset: Id::new("comments").unwrap(),
            operation: Id::new(version).unwrap(),
            base: None,
            relationship: None,
            input: "unused.parquet".into(),
            expected_digest: "unused".into(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        };
        DatasetPin {
            format: FORMAT,
            dataset: request.dataset.clone(),
            version: request.operation.clone(),
            groups: BTreeMap::new(),
            computed: BTreeMap::new(),
            inputs: BTreeMap::new(),
            relationship: SourceRelationship::Independent,
            related: None,
            evidence: None,
            definition: request.definition.clone(),
            eligibility: 0,
            boundary: 0,
            request: OperationRequest::Build(request),
            parent_manifest: None,
            processor: "test".into(),
            validation: VerificationLevel::Routine,
            partition_by: Vec::new(),
        }
    }

    #[tokio::test]
    async fn history_reuses_exact_manifests_and_bounds_combined_admission() {
        let root = tempfile::tempdir().unwrap();
        let mut objects = Vec::new();
        for version in ["first", "second"] {
            let dataset = history_dataset(version);
            let path = root.path().join(format!("{version}.json"));
            std::fs::write(&path, serde_json::to_vec(&dataset).unwrap()).unwrap();
            objects.push(storage::object(&dataset.version, &path).unwrap());
        }
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 16 << 20,
            scratch_bytes: 16 << 20,
            max_metadata_bytes: (objects[0].bytes + objects[1].bytes - 1) as usize,
            ..Limits::default()
        })
        .unwrap();
        let mut history = History::new(&budget, storage::Objects::Local).unwrap();
        let first = history.read(&objects[0]).await.unwrap();
        std::fs::remove_file(&objects[0].path).unwrap();
        let cached = history.read(&objects[0]).await.unwrap();
        assert!(Arc::ptr_eq(&first, &cached));
        assert_eq!(history.bytes as u64, objects[0].bytes);
        let mut conflicting = objects[0].clone();
        conflicting.digest = "different".into();
        assert_eq!(
            history.read(&conflicting).await.unwrap_err().code(),
            "invalid_group"
        );
        // The cumulative limit refuses before opening even a missing second file.
        std::fs::remove_file(&objects[1].path).unwrap();
        assert_eq!(
            history.read(&objects[1]).await.unwrap_err().code(),
            "metadata_capacity"
        );
        assert!(budget.reserve(budget.limits.memory_bytes / 2).is_err());
        drop((first, cached, history));
        assert!(budget.reserve(budget.limits.memory_bytes / 2).is_ok());
    }

    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn configured_history_reuses_exact_parent_and_preserves_read_scope() {
        use crate::storage::remote::objects::tests::ReadFixture;

        let dataset = history_dataset("parent");
        let payload = serde_json::to_vec(&dataset).unwrap();
        let object = ObjectRef {
            owner: dataset.dataset.clone(),
            path: "s3://bucket/owned/parent.json".into(),
            digest: blake3::hash(&payload).to_hex().to_string(),
            bytes: payload.len() as u64,
        };
        let fixture = ReadFixture::inventory(std::collections::HashMap::from([(
            object.path.clone(),
            payload,
        )]));
        let checks = super::super::super::verification::Checks::routine(storage::Objects::Remote(
            fixture.objects(),
        ));
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 16 << 20,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        })
        .unwrap();
        let mut history = History::new(&budget, checks.objects().clone()).unwrap();
        let first = history.read(&object).await.unwrap();
        let cached = history.read(&object).await.unwrap();
        assert_eq!(first.as_ref(), &dataset);
        assert!(Arc::ptr_eq(&first, &cached));
        assert_eq!(history.bytes as u64, object.bytes);
        let mut conflicting = object.clone();
        conflicting.digest = "different".into();
        assert_eq!(
            history.read(&conflicting).await.unwrap_err().code(),
            "invalid_group"
        );
        let mut outside = object.clone();
        outside.path = "s3://bucket/outside/parent.json".into();
        assert_eq!(
            history.read(&outside).await.unwrap_err().code(),
            "remote_scope"
        );
        assert_eq!(
            fixture.observed_paths(),
            ["HEAD", "GET", "HEAD"].map(|method| (method.into(), object.path.clone()))
        );

        // The changes verifier uses the same configured single-read path, with
        // its existing exact release identity checks rather than the cache.
        let release = refresh::read_release_with_objects(
            checks.objects(),
            &VersionRef {
                dataset: dataset.dataset.clone(),
                version: dataset.version.clone(),
                manifest: object.clone(),
            },
            &budget,
            &mut 0,
            &history.memory,
        )
        .await
        .unwrap();
        assert_eq!(release.dataset, dataset);
        assert_eq!(release.manifest, object);
        let requests = fixture.finish();
        assert_eq!(requests.len(), 6);
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "GET")
                .count(),
            2
        );
    }

    #[test]
    fn equal_counts_do_not_prove_coverage() {
        let mut file = FileCoverage {
            rows: 6,
            ranges: BTreeMap::new(),
        };
        file.insert(2, 4).unwrap();
        file.insert(4, 6).unwrap();
        assert!(!file.complete());
        assert!(file.insert(3, 5).is_err());
        file.insert(0, 2).unwrap();
        assert!(file.complete());
        assert!(file.insert(0, 1).is_err());
        assert!(file.insert(6, 7).is_err());
    }
}
