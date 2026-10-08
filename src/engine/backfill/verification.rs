//! Independent native reads establish exact one-to-one occurrence coverage.

use super::*;
use arrow::array::{Array, Int64Array, StringArray};

pub(crate) async fn verify_groups(dataset: &DatasetPin, budget: &Budget) -> Result<()> {
    if dataset.groups.len() != dataset.computed.len() + 1 {
        return Err(invalid_group());
    }
    check_names(dataset)?;
    let source = dataset.groups.get("source").ok_or_else(invalid_group)?;
    let mut checked_base: Option<(ObjectRef, DatasetPin)> = None;
    match &dataset.request {
        OperationRequest::Build(spec) => {
            if dataset.related.is_some() || dataset.inputs.len() != dataset.request.inputs().len() {
                return Err(invalid_group());
            }
            if let Some(version) = &spec.base {
                let parent = dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?;
                let base = read_manifest(parent, budget).await?;
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
                if dataset.relationship != inputs::relationship(&dataset.request, Some(&base)) {
                    return Err(inputs::invalid_relationship());
                }
                checked_base = Some((parent.clone(), base));
            } else if !dataset.computed.is_empty() || dataset.parent_manifest.is_some() {
                return Err(invalid_group());
            } else if dataset.relationship != inputs::relationship(&dataset.request, None) {
                return Err(inputs::invalid_relationship());
            }
        }
        OperationRequest::Update(_) => {
            if dataset.related.is_some() {
                return Err(invalid_group());
            }
            let changes = dataset.changes.as_ref().ok_or_else(invalid_group)?;
            if dataset.parent_manifest.as_ref() != Some(&changes.base_manifest) {
                return Err(invalid_group());
            }
            let base = read_manifest(&changes.base_manifest, budget).await?;
            let mut inherited_inputs = dataset.inputs.clone();
            for name in dataset.request.inputs().keys() {
                let key = dataset.request.input_key(name);
                if base.inputs.contains_key(&key) || inherited_inputs.remove(&key).is_none() {
                    return Err(invalid_group());
                }
            }
            if inherited_inputs != base.inputs
                || base.related.is_some()
                || dataset.relationship != base.relationship
                || base.eligibility != dataset.eligibility
                || dataset.boundary <= base.boundary
                || dataset.definition != base.definition
            {
                return Err(invalid_group());
            }
            check_refreshed_groups(dataset, &base, &changes.base_manifest)?;
            checked_base = Some((changes.base_manifest.clone(), base));
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
            let base = read_base(selected, budget).await?;
            let parent = read_manifest(
                dataset.parent_manifest.as_ref().ok_or_else(invalid_group)?,
                budget,
            )
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
                || base.inputs != dataset.inputs
                || base.boundary != dataset.boundary
                || base.eligibility != dataset.eligibility
                || parent.groups.get(spec.group.as_str()) != base.groups.get(spec.group.as_str())
                || parent.computed.get(spec.group.as_str())
                    != base.computed.get(spec.group.as_str())
            {
                return Err(invalid_group());
            }
            let mut inherited = dataset.computed.clone();
            inherited.remove(spec.group.as_str());
            let mut expected = parent.computed.clone();
            expected.remove(spec.group.as_str());
            if inherited != expected {
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
                let base = read_manifest(parent, budget).await?;
                if base.dataset != dataset.dataset
                    || &base.version != version
                    || base.version == dataset.version
                    || dataset.boundary <= base.boundary
                    || dataset.eligibility < base.eligibility
                    || (dataset.definition != base.definition
                        && dataset.eligibility == base.eligibility)
                    || dataset.relationship != inputs::relationship(&dataset.request, Some(&base))
                    || base.related.as_ref().is_some_and(|previous| {
                        previous.request.left.dataset != spec.left.dataset
                            || previous.request.right.dataset != spec.right.dataset
                    })
                {
                    return Err(invalid_group());
                }
                if spec.mode == RefreshMode::Incremental {
                    if dataset
                        .changes
                        .as_ref()
                        .map(|changes| &changes.base_manifest)
                        != Some(parent)
                    {
                        return Err(invalid_group());
                    }
                    super::super::incremental::qualify(spec, &base, dataset.eligibility, budget)
                        .await?;
                }
                check_refreshed_groups(dataset, &base, parent)?;
                checked_base = Some((parent.clone(), base));
            } else if spec.mode == RefreshMode::Incremental
                || !dataset.computed.is_empty()
                || dataset.parent_manifest.is_some()
                || dataset.relationship != inputs::relationship(&dataset.request, None)
            {
                return Err(invalid_group());
            }
        }
    }
    for (name, binding) in &dataset.computed {
        if name != binding.definition.group.as_str()
            || binding.request.dataset() != &dataset.dataset
            || binding.request.definition() != dataset.definition
            || binding.request.inputs().iter().any(|(name, input)| {
                dataset
                    .inputs
                    .get(&binding.request.input_key(name))
                    .is_none_or(|object| object.digest != input.digest)
            })
        {
            return Err(invalid_group());
        }
        if checked_base
            .as_ref()
            .is_none_or(|(object, _)| object != &binding.base_manifest)
        {
            drop(checked_base.take());
            checked_base = Some((
                binding.base_manifest.clone(),
                read_base(binding, budget).await?,
            ));
        }
        let (_, base) = checked_base.as_ref().ok_or_else(invalid_group)?;
        verify_group(
            source,
            dataset.groups.get(name).ok_or_else(invalid_group)?,
            binding,
            base,
            budget,
        )
        .await?;
    }
    Ok(())
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

async fn read_manifest(object: &ObjectRef, budget: &Budget) -> Result<DatasetPin> {
    if object.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "group source binding exceeds metadata allowance",
        ));
    }
    storage::verify_objects(vec![object.clone()], budget).await?;
    let base: DatasetPin = serde_json::from_reader(std::fs::File::open(&object.path)?)?;
    if base.format != FORMAT {
        return Err(invalid_group());
    }
    Ok(base)
}

async fn read_base(binding: &ComputedGroup, budget: &Budget) -> Result<DatasetPin> {
    let base = read_manifest(&binding.base_manifest, budget).await?;
    if &base.dataset != binding.request.dataset() || Some(&base.version) != binding.request.base() {
        return Err(invalid_group());
    }
    Ok(base)
}

async fn verify_group(
    source: &TablePin,
    group: &TablePin,
    binding: &ComputedGroup,
    base: &DatasetPin,
    budget: &Budget,
) -> Result<()> {
    if &base.dataset != binding.request.dataset() || Some(&base.version) != binding.request.base() {
        return Err(invalid_group());
    }
    if binding.source_metadata != source.metadata {
        return Err(invalid_group());
    }
    match &binding.request {
        OperationRequest::Backfill(spec) => {
            if binding.definition != spec.group_definition()
                || base.groups.get("source") != Some(source)
            {
                return Err(invalid_group());
            }
        }
        OperationRequest::Update(_) | OperationRequest::Build(_) | OperationRequest::Refresh(_) => {
            if base
                .computed
                .get(binding.definition.group.as_str())
                .map(|group| &group.definition)
                != Some(&binding.definition)
            {
                return Err(invalid_group());
            }
        }
    }
    verify_group_data(
        source,
        group,
        &binding.definition,
        &binding.native_field_ids,
        budget,
    )
    .await
}

pub(super) async fn verify_group_data(
    source: &TablePin,
    group: &TablePin,
    definition: &GroupDefinition,
    native_field_ids: &BTreeMap<i32, i32>,
    budget: &Budget,
) -> Result<()> {
    let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
    if group.rows != source.rows {
        return Err(invalid_group());
    }
    storage::verify_objects(group.objects.clone(), budget).await?;
    let source_native = storage::open_pin(source).await?;
    let group_native = storage::open_pin(group).await?;
    storage::check_inventory(group, &group_native, budget).await?;
    let expressions = Expressions::compile(
        &SessionContext::new(),
        source_native.metadata().current_schema(),
        definition,
    )?;
    if expressions.native != **group_native.metadata().current_schema()
        || &expressions.native_field_ids != native_field_ids
    {
        return Err(Error::refusal(
            "invalid_group",
            "computed schema or logical-to-native field mapping differs",
        ));
    }
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
