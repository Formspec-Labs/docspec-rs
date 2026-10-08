//! Independent native reads establish exact one-to-one occurrence coverage.

use super::*;
use arrow::array::{Array, Int64Array, StringArray};

pub(crate) async fn verify_groups(dataset: &DatasetPin, budget: &Budget) -> Result<()> {
    if dataset.groups.len() != dataset.computed.len() + 1 {
        return Err(invalid_group());
    }
    check_names(dataset)?;
    let source = dataset.groups.get("source").ok_or_else(invalid_group)?;
    if let OperationRequest::Backfill(spec) = &dataset.request {
        let selected = dataset
            .computed
            .get(spec.group.as_str())
            .ok_or_else(invalid_group)?;
        if &selected.request != spec {
            return Err(invalid_group());
        }
        let base = read_base(selected, budget).await?;
        if dataset.inputs != base.inputs || dataset.boundary != base.boundary {
            return Err(invalid_group());
        }
    } else if !dataset.computed.is_empty() {
        return Err(invalid_group());
    }
    for (name, binding) in &dataset.computed {
        if name != binding.request.group.as_str()
            || binding.request.dataset != dataset.dataset
            || binding.request.definition != dataset.definition
        {
            return Err(invalid_group());
        }
        verify_group(
            source,
            dataset.groups.get(name).ok_or_else(invalid_group)?,
            binding,
            budget,
        )
        .await?;
    }
    Ok(())
}

async fn read_base(binding: &ComputedGroup, budget: &Budget) -> Result<DatasetPin> {
    if binding.base_manifest.bytes > budget.limits.max_metadata_bytes as u64 {
        return Err(Error::refusal(
            "metadata_capacity",
            "group source binding exceeds metadata allowance",
        ));
    }
    storage::verify_objects(vec![binding.base_manifest.clone()], budget).await?;
    let base: DatasetPin =
        serde_json::from_reader(std::fs::File::open(&binding.base_manifest.path)?)?;
    if base.format != 3
        || base.dataset != binding.request.dataset
        || base.version != binding.request.base
    {
        return Err(invalid_group());
    }
    Ok(base)
}

pub(super) async fn verify_group(
    source: &TablePin,
    group: &TablePin,
    binding: &ComputedGroup,
    budget: &Budget,
) -> Result<()> {
    let _metadata = budget.reserve(budget.limits.max_metadata_bytes)?;
    let base = read_base(binding, budget).await?;
    if binding.source_metadata != source.metadata
        || base.groups.get("source") != Some(source)
        || group.rows != source.rows
    {
        return Err(invalid_group());
    }
    storage::verify_objects(group.objects.clone(), budget).await?;
    let source_native = storage::open_pin(source).await?;
    let group_native = storage::open_pin(group).await?;
    storage::check_inventory(group, &group_native, budget).await?;
    let expressions = Expressions::compile(
        &SessionContext::new(),
        source_native.metadata().current_schema(),
        &binding.request,
    )?;
    if expressions.native != **group_native.metadata().current_schema()
        || expressions.native_field_ids != binding.native_field_ids
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
