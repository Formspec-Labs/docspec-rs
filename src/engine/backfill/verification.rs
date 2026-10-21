//! Independent native reads establish coverage, values and input binding history.

use super::*;
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
    let source = dataset.groups.get("source").ok_or_else(invalid_group)?;
    for (name, binding) in &dataset.computed {
        verify_binding_history(dataset, name, &mut history, checks).await?;
        checks
            .scalar_origins(
                binding.origins.as_ref(),
                source,
                dataset.groups.get(name).ok_or_else(invalid_group)?,
                &binding.definition,
                &binding.input_fields,
                &binding.native_field_ids,
                budget,
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn verify_groups(
    dataset: &DatasetPin,
    budget: &Budget,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    if dataset.groups.len() != dataset.computed.len() + 1 {
        return Err(invalid_group());
    }
    check_names(dataset)?;
    let source = dataset.groups.get("source").ok_or_else(invalid_group)?;
    let mut history = History::new(budget, checks.objects().clone())?;
    match &dataset.request {
        OperationRequest::Build(spec) => {
            if dataset.related.is_some() || dataset.inputs.len() != dataset.request.inputs().len() {
                return Err(invalid_group());
            }
            if let Some(version) = &spec.base {
                let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
                let base = history.read(parent).await?;
                if base.dataset != dataset.dataset
                    || base.related.is_some()
                    || &base.version != version
                    || base.version == dataset.version
                    || dataset.boundary <= base.boundary
                    || dataset.eligibility < base.eligibility
                    || (dataset.definition != base.definition
                        && dataset.eligibility == base.eligibility)
                {
                    return Err(invalid_group());
                }
                check_refreshed_groups(dataset, &base, parent)?;
                super::super::schema::verify_rebuild(
                    base.groups.get("source").ok_or_else(invalid_group)?,
                    source,
                    dataset.format,
                    budget,
                )
                .await?;
                if dataset.relationship != inputs::relationship(&dataset.request, Some(&base)) {
                    return Err(inputs::invalid_relationship());
                }
            } else if !dataset.computed.is_empty() || dataset.parent_manifest.is_some() {
                return Err(invalid_group());
            } else if dataset.relationship != inputs::relationship(&dataset.request, None) {
                return Err(inputs::invalid_relationship());
            }
        }
        OperationRequest::Update(spec) => {
            if dataset.related.is_some() {
                return Err(invalid_group());
            }
            let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
            let base = history.read(parent).await?;
            if base.dataset != spec.dataset
                || base.dataset != dataset.dataset
                || base.version != spec.base
                || base.version == dataset.version
            {
                return Err(invalid_group());
            }
            let mut inherited_inputs = dataset.inputs.clone();
            for name in dataset.request.inputs().keys() {
                let key = dataset.request.input_key(name);
                if base.inputs.contains_key(&key) || inherited_inputs.remove(&key).is_none() {
                    return Err(invalid_group());
                }
            }
            if inherited_inputs != base.inputs
                || base.related.is_some()
                || dataset.relationship != inputs::relationship(&dataset.request, Some(&base))
                || base.eligibility != dataset.eligibility
                || dataset.boundary <= base.boundary
                || dataset.definition != base.definition
            {
                return Err(invalid_group());
            }
            super::super::write::verify_update(dataset, &base, spec, budget, checks).await?;
            check_refreshed_groups(dataset, &base, parent)?;
        }
        OperationRequest::Backfill(spec) => {
            let selected = dataset
                .computed
                .get(spec.group.as_str())
                .ok_or_else(invalid_group)?;
            if selected.request != dataset.request || selected.definition != spec.group_definition()
            {
                return Err(invalid_group());
            }
            let base = history.base(selected).await?;
            let parent = history
                .read(dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?)
                .await?;
            if parent.dataset != dataset.dataset
                || parent.version == dataset.version
                || parent.groups.get("source") != Some(source)
                || parent.inputs != dataset.inputs
                || parent.related != dataset.related
                || base.related != dataset.related
                || parent.relationship != dataset.relationship
                || base.relationship != dataset.relationship
                || parent.boundary != dataset.boundary
                || parent.definition != dataset.definition
                || parent.eligibility != dataset.eligibility
                || base.boundary != dataset.boundary
                || base.eligibility != dataset.eligibility
                || parent.groups.get(spec.group.as_str()) != base.groups.get(spec.group.as_str())
                || parent.computed.get(spec.group.as_str())
                    != base.computed.get(spec.group.as_str())
            {
                return Err(invalid_group());
            }
            compatible_input_history(&base, &parent, &mut history).await?;
            let mut inherited = dataset.computed.clone();
            inherited.remove(spec.group.as_str());
            let mut expected = parent.computed.clone();
            expected.remove(spec.group.as_str());
            if !super::super::corrections::inherits_groups(&inherited, &expected) {
                return Err(invalid_group());
            }
            for name in inherited.keys() {
                if dataset.groups.get(name) != parent.groups.get(name) {
                    return Err(invalid_group());
                }
            }
        }
        OperationRequest::Refresh(spec) => {
            let related = dataset.related.as_ref().ok_or_else(invalid_group)?;
            if &related.request != spec.as_ref()
                || related.coverage.inputs != spec.inputs()
                || !dataset.inputs.is_empty()
            {
                return Err(invalid_group());
            }
            if let Some(version) = &spec.base {
                let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
                let base = history.read(parent).await?;
                if base.dataset != dataset.dataset
                    || &base.version != version
                    || base.version == dataset.version
                    || dataset.boundary <= base.boundary
                    || dataset.eligibility < base.eligibility
                    || (dataset.definition != base.definition
                        && dataset.eligibility == base.eligibility)
                    || dataset.relationship != inputs::relationship(&dataset.request, Some(&base))
                    || base.related.as_ref().is_some_and(|previous| {
                        !previous.request.transform.same_roles(&spec.transform)
                    })
                {
                    return Err(invalid_group());
                }
                if spec.mode == RefreshMode::Incremental {
                    super::super::incremental::qualify(spec, &base, dataset.eligibility, budget)
                        .await?;
                }
                check_refreshed_groups(dataset, &base, parent)?;
            } else if spec.mode == RefreshMode::Incremental
                || !dataset.computed.is_empty()
                || dataset.parent_manifest.is_some()
                || dataset.relationship != inputs::relationship(&dataset.request, None)
            {
                return Err(invalid_group());
            }
        }
        OperationRequest::Schema(spec) => {
            let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
            let base = history.read(parent).await?;
            super::super::schema::verify_transition(spec, &base, dataset, budget).await?;
            check_refreshed_groups(dataset, &base, parent)?;
        }
        OperationRequest::Evidence(spec) => {
            let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
            let base = history.read(parent).await?;
            let mut inherited_inputs = dataset.inputs.clone();
            for name in dataset.request.inputs().keys() {
                let key = dataset.request.input_key(name);
                if base.inputs.contains_key(&key) || inherited_inputs.remove(&key).is_none() {
                    return Err(invalid_group());
                }
            }
            if base.dataset != dataset.dataset
                || base.version != spec.base
                || inherited_inputs != base.inputs
                || dataset.groups != base.groups
                || !super::super::corrections::inherits_groups(&dataset.computed, &base.computed)
                || dataset.related != base.related
                || dataset.relationship != base.relationship
                || dataset.boundary != base.boundary
                || dataset.eligibility != base.eligibility
                || dataset.definition != base.definition
            {
                return Err(invalid_group());
            }
        }
    }
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
            return Err(invalid_group());
        }
        verify_binding_history(dataset, name, &mut history, checks).await?;
        checks
            .scalar_origins(
                binding.origins.as_ref(),
                source,
                dataset.groups.get(name).ok_or_else(invalid_group)?,
                &binding.definition,
                &binding.input_fields,
                &binding.native_field_ids,
                budget,
            )
            .await?;
        verify_group_data(
            source,
            dataset.groups.get(name).ok_or_else(invalid_group)?,
            &binding.definition,
            &binding.input_fields,
            &binding.native_field_ids,
            budget,
            checks,
        )
        .await?;
    }
    Ok(())
}

// Independent receipt and field-group selections can advance while scalar work
// runs. Establish exactly which input additions came from those selections;
// equal source rows alone never authorize an unrelated provenance change.
async fn compatible_input_history(
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
                Err(invalid_group())
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
            return Err(invalid_group());
        }
        let previous = history
            .read(current.parent_manifest.as_ref().ok_or_else(invalid_group)?)
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
                    return Err(invalid_group());
                }
                for (name, input) in current.request.inputs() {
                    let key = current.request.input_key(&name);
                    let object = current.inputs.get(&key).ok_or_else(invalid_group)?;
                    if object.digest != input.digest
                        || expected.insert(key, object.clone()).is_some()
                    {
                        return Err(invalid_group());
                    }
                }
            }
            OperationRequest::Backfill(_) => {}
            _ => return Err(invalid_group()),
        }
        if expected != current.inputs {
            return Err(invalid_group());
        }
        retained = Some(previous);
        tokio::task::consume_budget().await;
    }
}

fn check_refreshed_groups(
    dataset: &DatasetPin,
    base: &DatasetPin,
    parent: &ObjectRef,
) -> Result<()> {
    if base.computed.len() != dataset.computed.len() {
        return Err(invalid_group());
    }
    for (name, previous) in &base.computed {
        let refreshed = dataset.computed.get(name).ok_or_else(invalid_group)?;
        if refreshed.definition != previous.definition
            || refreshed.request != dataset.request
            || &refreshed.base_manifest != parent
        {
            return Err(invalid_group());
        }
    }
    Ok(())
}

// One bounded, request-local cache shares retained manifests across scalar groups.
// Worker custody is delegated to the existing verified manifest reader.
struct History<'a> {
    entries: BTreeMap<String, (ObjectRef, Arc<DatasetPin>)>,
    bytes: usize,
    memory: Arc<tokio::sync::OwnedSemaphorePermit>,
    budget: &'a Budget,
    objects: storage::Objects,
}
impl<'a> History<'a> {
    fn new(budget: &'a Budget, objects: storage::Objects) -> Result<Self> {
        Ok(Self {
            entries: BTreeMap::new(),
            bytes: 0,
            memory: Arc::new(budget.reserve(budget.limits.max_metadata_bytes.saturating_mul(2))?),
            budget,
            objects,
        })
    }
    async fn read(&mut self, object: &ObjectRef) -> Result<Arc<DatasetPin>> {
        if let Some((expected, dataset)) = self.entries.get(&object.path) {
            return if expected == object {
                Ok(dataset.clone())
            } else {
                Err(invalid_group())
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
            return Err(invalid_group());
        }
        let dataset = Arc::new(dataset);
        self.entries
            .insert(object.path.clone(), (object.clone(), dataset.clone()));
        Ok(dataset)
    }
    async fn base(&mut self, binding: &ComputedGroup) -> Result<Arc<DatasetPin>> {
        let base = self.read(&binding.base_manifest).await?;
        if &base.dataset != binding.request.dataset()
            || Some(&base.version) != binding.request.base()
        {
            return Err(invalid_group());
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
        let binding = owner.computed.get(name).ok_or_else(invalid_group)?;
        let source = owner.groups.get("source").ok_or_else(invalid_group)?;
        if !visited.insert((owner.dataset.clone(), owner.version.clone()))
            || owner.request.dataset() != &owner.dataset
            || owner.request.operation() != &owner.version
            || binding.definition.group.as_str() != name
            || binding.source_metadata != source.metadata
        {
            return Err(invalid_group());
        }
        // Validate each owning selection's immutable proof, including intermediate
        // inherited bindings. Historical verification never reads current corrections.
        checks
            .scalar_qualification(
                binding,
                source,
                owner.groups.get(name).ok_or_else(invalid_group)?,
                history.budget,
            )
            .await?;
        if matches!(&owner.request, OperationRequest::Evidence(_)) {
            let parent = history
                .read(owner.parent_manifest.as_ref().ok_or_else(invalid_group)?)
                .await?;
            if parent.dataset != owner.dataset
                || !parent
                    .computed
                    .get(name)
                    .is_some_and(|previous| super::super::corrections::inherits(binding, previous))
                || parent.groups.get("source") != Some(source)
                || parent.groups.get(name) != owner.groups.get(name)
            {
                return Err(invalid_group());
            }
            retained = Some(parent);
        } else if let OperationRequest::Backfill(spec) = &owner.request {
            if spec.group.as_str() == name {
                let base = history.base(binding).await?;
                if binding.request != owner.request
                    || binding.definition != spec.group_definition()
                    || base.groups.get("source") != Some(source)
                {
                    return Err(invalid_group());
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
                    Err(invalid_group())
                };
            }
            // A sibling addition inherits this binding and its exact value table.
            let parent = history
                .read(owner.parent_manifest.as_ref().ok_or_else(invalid_group)?)
                .await?;
            if parent.dataset != owner.dataset
                || !parent
                    .computed
                    .get(name)
                    .is_some_and(|previous| super::super::corrections::inherits(binding, previous))
                || parent.groups.get("source") != Some(source)
                || parent.groups.get(name) != owner.groups.get(name)
            {
                return Err(invalid_group());
            }
            retained = Some(parent);
        } else {
            let parent_ref = owner.parent_manifest.as_ref().ok_or_else(invalid_group)?;
            if binding.request != owner.request || &binding.base_manifest != parent_ref {
                return Err(invalid_group());
            }
            let parent = history.base(binding).await?;
            let previous = parent.computed.get(name).ok_or_else(invalid_group)?;
            let before = parent.groups.get("source").ok_or_else(invalid_group)?;
            if previous.definition != binding.definition
                || inherited_inputs(&owner.request, previous, before, source, history.budget)
                    .await?
                    != binding.input_fields
            {
                return Err(invalid_group());
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
                    return Err(invalid_group());
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
    source: &TablePin,
    group: &TablePin,
    definition: &GroupDefinition,
    input_fields: &BTreeMap<String, i32>,
    native_field_ids: &BTreeMap<i32, i32>,
    budget: &Budget,
    checks: &mut super::super::verification::Checks,
) -> Result<()> {
    if group.rows != source.rows {
        return Err(invalid_group());
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
                .ok_or_else(invalid_group)?;
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid_group)?;
            if paths.null_count() != 0 || positions.null_count() != 0 {
                return Err(invalid_group());
            }
            let mut index = 0;
            while index < batch.num_rows() {
                let path = paths.value(index);
                let start = u64::try_from(positions.value(index)).map_err(|_| invalid_group())?;
                let mut end = start.checked_add(1).ok_or_else(invalid_group)?;
                index += 1;
                while index < batch.num_rows()
                    && paths.value(index) == path
                    && positions.value(index) >= 0
                    && positions.value(index) as u64 == end
                {
                    end = end.checked_add(1).ok_or_else(invalid_group)?;
                    index += 1;
                }
                let file = coverage.get_mut(path).ok_or_else(invalid_group)?;
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
            return Err(invalid_group());
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
        .ok_or_else(invalid_group)?
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
            return Err(invalid_group());
        }
        if let Some((&previous, &stop)) = self.ranges.range(..=start).next_back() {
            if stop > start {
                return Err(invalid_group());
            }
            if stop == start {
                start = previous;
                self.ranges.remove(&previous);
            }
        }
        if let Some((&next, &stop)) = self.ranges.range(start..).next() {
            if next < end {
                return Err(invalid_group());
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
