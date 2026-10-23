//! Candidate file probing and opportunistic small-file coalescing during key updates.

use super::{any_rows, key_join, qualified};
use crate::{Error, Result, budget::Budget, engine::key_pruning, engine::locations, storage};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{StreamExt, TryStreamExt};
use iceberg::{spec::DataFile, table::Table};
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::Arc,
};

/// Minimum small-file size threshold (64 KiB).
const MIN_SMALL_FILE_BYTES: u64 = 64 << 10;
/// Maximum small-file size threshold (512 KiB).
const MAX_SMALL_FILE_BYTES: u64 = 512 << 10;
/// Minimum number of small files in one partition (across >=2 attempt directories)
/// before opportunistic small-file compaction triggers.
const SMALL_FILE_COALESCE_TRIGGER: usize = 24;
/// Maximum number of small files coalesced in a single update pass.
const MAX_COALESCE_BATCH_FILES: usize = 32;
/// Minimum number of smallest files coalesced when a partition exceeds `PARTITION_FILE_CAP`.
const MIN_COALESCE_BATCH_FILES: usize = 16;
/// Partition file count threshold above which the smallest files are coalesced
/// back toward `MAX_COALESCE_BATCH_FILES`.
const PARTITION_FILE_CAP: usize = 48;

pub(super) struct ProbedFiles {
    pub shared: Vec<DataFile>,
    pub changed: Vec<DataFrame>,
    pub rewrite_bytes: u64,
    pub files_probed: u64,
    pub files_skipped_by_pruning: u64,
}

/// Prunes snapshot files using manifest/partition bounds and exact `KeyPoints`, then
/// probes the remaining candidate files up to 16-way concurrently for key matches.
#[allow(clippy::too_many_arguments)]
pub(super) async fn probe_affected_files(
    context: &SessionContext,
    native: &Table,
    scope_table: &Table,
    keys: &[String],
    key_files: &[DataFile],
    scan: &Arc<storage::OccurrenceScan>,
    schema: &arrow::datatypes::Schema,
    marker: &str,
    budget: &Budget,
) -> Result<ProbedFiles> {
    if key_files.is_empty() {
        return Ok(ProbedFiles {
            shared: scan.files().cloned().collect(),
            changed: Vec::new(),
            rewrite_bytes: 0,
            files_probed: 0,
            files_skipped_by_pruning: scan.files().count() as u64,
        });
    }
    let native_schema = native.metadata().current_schema();
    let partition_spec = native.metadata().default_partition_spec();
    let key_table_schema = scope_table.metadata().current_schema();
    // Exact affected points when they fit one batch allowance; otherwise
    // only the coarser affected-interval test below applies.
    let points =
        key_pruning::KeyPoints::collect(context, native_schema, partition_spec, keys, budget)
            .await?;
    let affected_keys_df = context.table("affected_keys").await?;
    let mut outcomes = Vec::new();
    let mut candidates = Vec::new();
    let mut files_skipped_by_pruning = 0_u64;
    for (index, file) in scan.files().enumerate() {
        if !key_pruning::file_matches_pruned_keys(
            native_schema,
            partition_spec,
            key_table_schema,
            keys,
            keys,
            key_files,
            points.as_ref(),
            file,
        ) {
            files_skipped_by_pruning = files_skipped_by_pruning.saturating_add(1);
            outcomes.push(Some(file.clone()));
        } else {
            outcomes.push(None);
            candidates.push((index, file.clone()));
        }
    }
    let files_probed = candidates.len() as u64;
    let probed = futures::stream::iter(candidates.into_iter().map(|(index, file)| {
        let affected_keys_df = &affected_keys_df;
        async move {
            let key_probe = context.read_table(crate::query::file(
                scan.clone(),
                file.file_path().to_owned(),
                Some(keys),
                budget.clone(),
            )?)?;
            if any_rows(key_join(
                key_probe,
                affected_keys_df.clone(),
                keys,
                datafusion::logical_expr::JoinType::Inner,
            )?)
            .await?
            {
                // Replace the key-only probe with the complete selected native schema.
                let full_file = context.read_table(crate::query::file(
                    scan.clone(),
                    file.file_path().to_owned(),
                    None,
                    budget.clone(),
                )?)?;
                // One joined decode supplies both kept rows and actual removed rows.
                let mut columns = schema
                    .fields()
                    .iter()
                    .map(|field| qualified("l", field.name()))
                    .collect::<Vec<_>>();
                columns.push(qualified("r", marker).is_not_null().alias(marker));
                let joined = key_join(
                    full_file,
                    affected_keys_df.clone(),
                    keys,
                    datafusion::logical_expr::JoinType::Left,
                )?
                .select(columns)?;
                Ok::<_, Error>((index, file, Some(joined)))
            } else {
                Ok((index, file, None))
            }
        }
    }))
    .buffered(16)
    .try_collect::<Vec<_>>()
    .await?;
    let mut changed = Vec::new();
    let mut rewrite_bytes = 0_u64;
    for (index, file, changed_df) in probed {
        if let Some(df) = changed_df {
            rewrite_bytes = rewrite_bytes.saturating_add(file.file_size_in_bytes());
            changed.push(df);
        } else {
            outcomes[index] = Some(file);
        }
    }
    Ok(ProbedFiles {
        shared: outcomes.into_iter().flatten().collect(),
        changed,
        rewrite_bytes,
        files_probed,
        files_skipped_by_pruning,
    })
}

/// Opportunistically coalesces fragmented shared files within a single partition
/// during an active key update when origin columns are present.
///
/// Compaction threshold invariants:
/// - `small_threshold` is `1/8` of `max_batch_bytes`, clamped to `[64 KiB, 512 KiB]`.
/// - **Small-file trigger** (`small_files >= 24 && small_dirs >= 2`): when at least
///   24 untouched files below `small_threshold` span at least 2 distinct attempt
///   directories in the same partition, up to 32 of those small files are rewritten
///   together.
/// - **Partition file-count trigger** (`part_files > 48 && part_dirs >= 2`): when a
///   partition accumulates more than 48 untouched files across at least 2 directories,
///   the smallest `(group.len() - 32).clamp(16, 32)` files are coalesced in original
///   file order to bound manifest growth while keeping per-update rewrite cost bounded.
#[expect(
    clippy::too_many_arguments,
    reason = "Coalesces small partition files across native/candidate tables with shared manifest state and budget"
)]
pub(super) async fn coalesce_small_partition_files(
    root: &Path,
    directory: &locations::Location,
    native: &Table,
    candidate: &Table,
    shared: &mut Vec<DataFile>,
    scan: &Arc<storage::OccurrenceScan>,
    has_applied_changes: bool,
    budget: &Budget,
) -> Result<Vec<DataFile>> {
    if !has_applied_changes || !storage::identity::has_origins(native.metadata().current_schema())?
    {
        return Ok(Vec::new());
    }
    let small_threshold = (budget.limits.max_batch_bytes as u64 / 8)
        .clamp(MIN_SMALL_FILE_BYTES, MAX_SMALL_FILE_BYTES);
    let mut partition_groups: Vec<Vec<usize>> = Vec::new();
    let mut partition_index: HashMap<iceberg::spec::Struct, usize> = HashMap::new();
    for (idx, file) in shared.iter().enumerate() {
        if let Some(&pos) = partition_index.get(file.partition()) {
            partition_groups[pos].push(idx);
        } else {
            let pos = partition_groups.len();
            partition_index.insert(file.partition().clone(), pos);
            partition_groups.push(vec![idx]);
        }
    }
    let mut indices = Vec::new();
    for group in partition_groups {
        let small_files = group
            .iter()
            .filter(|&&idx| shared[idx].file_size_in_bytes() < small_threshold)
            .count();
        let small_dirs = group
            .iter()
            .filter(|&&idx| shared[idx].file_size_in_bytes() < small_threshold)
            .filter_map(|&idx| Path::new(shared[idx].file_path()).parent())
            .collect::<BTreeSet<_>>()
            .len();
        let part_files = group.len();
        let part_dirs = group
            .iter()
            .filter_map(|&idx| Path::new(shared[idx].file_path()).parent())
            .collect::<BTreeSet<_>>()
            .len();
        if small_files >= SMALL_FILE_COALESCE_TRIGGER && small_dirs >= 2 {
            indices = group
                .into_iter()
                .filter(|&idx| shared[idx].file_size_in_bytes() < small_threshold)
                .take(MAX_COALESCE_BATCH_FILES)
                .collect();
            break;
        } else if part_files > PARTITION_FILE_CAP && part_dirs >= 2 {
            let mut by_size = group
                .iter()
                .map(|&idx| (shared[idx].file_size_in_bytes(), idx))
                .collect::<Vec<_>>();
            by_size.sort_unstable_by_key(|(bytes, _)| *bytes);
            let take = (group.len().saturating_sub(MAX_COALESCE_BATCH_FILES))
                .clamp(MIN_COALESCE_BATCH_FILES, MAX_COALESCE_BATCH_FILES);
            let mut selected = by_size
                .into_iter()
                .take(take)
                .map(|(_, idx)| idx)
                .collect::<Vec<_>>();
            selected.sort_unstable();
            indices = selected;
            break;
        }
    }
    let coalesce_bytes = indices
        .iter()
        .map(|&idx| shared[idx].file_size_in_bytes())
        .sum::<u64>();
    let reserve = coalesce_bytes
        .saturating_mul(2)
        .saturating_add(budget.limits.scratch_bytes);
    if directory.is_local() && fs2::available_space(root)? < reserve {
        indices.clear();
    }
    if indices.len() < 2 {
        return Ok(Vec::new());
    }
    let selected_set = indices.into_iter().collect::<BTreeSet<_>>();
    let mut remaining = Vec::with_capacity(shared.len() - selected_set.len());
    let mut paths = Vec::with_capacity(selected_set.len());
    for (idx, file) in std::mem::take(shared).into_iter().enumerate() {
        if selected_set.contains(&idx) {
            paths.push(file.file_path().to_owned());
        } else {
            remaining.push(file);
        }
    }
    *shared = remaining;
    let scan_for_coalesce = scan.clone();
    let coalesced_stream = futures::stream::iter(paths)
        .map(move |path| scan_for_coalesce.read_file(&path, budget))
        .try_flatten();
    let target_file_bytes = usize::try_from(coalesce_bytes)
        .unwrap_or(usize::MAX)
        .max(budget.limits.max_batch_bytes);
    storage::write_batches_sized(candidate, coalesced_stream, budget, target_file_bytes).await
}
