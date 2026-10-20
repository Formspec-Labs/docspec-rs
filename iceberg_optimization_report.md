# docspec-rs Iceberg Optimization Report — news_lake shape

Scope (Phase 0): the **docspec-rs-managed native lifecycle tables** that news_lake
datasets will use as out-of-core storage, profiled on the engine itself
(iceberg-rs 0.10.1, arrow/parquet 58.4.0, DataFusion 53.1.0). Access mode is
**Direct**: the engine *is* the writer and the maintenance gateway, so every
recommended action below is a docspec-rs operation (`compact`,
`plan-operation-retirement`, `seal-generation`), never Spark/Trino SQL.

**Safety:** `retire-operation` and retirement effects permanently drop retained
recovery records and authorize physical reclamation of unprotected objects,
and `compact` rewrites data files. Read-only inspection (`inspect-retention`,
refresh/compaction result metrics) is always safe; run the destructive steps
only per the schedule below and only after `plan-*` output has been reviewed.

## Summary: Why Each Action, In One Sentence

| Action | Why? (data-driven) |
|---|---|
| **Run writer fixes (Group 2) first — ZSTD(3), Parquet 2.0, 1 MiB data pages, dictionary encoding, chunk statistics, byte-bounded row groups** (landed in this change set) | The measured workspace shows every hourly poll appending one 3,241–15,649-byte file, so writer-side efficiency and bounded row groups must be correct before any rewrite, or compaction output inherits the same bad shape. |
| **Daily bin-pack compaction via `compact` with `target_file_bytes` 64 MiB** (Group 1) | Measured: 25 post-poll files (93,572 B, median 3,247 B, 100 % under 64 MiB) collapsed to **1 file / 36,707 B (25:1, ~2.5× byte reduction)** in the harness; at 26-year scale, uncompacted hourly appends project to ~228k files vs ~80 compacted. |
| **Skip sort/z-order compaction** (rejected) | The decision gate requires selective, frequent point/range lookups to amortize rewrites; news_lake access is month-range aggregate refreshes (`coverage_daily`) and docspec-rs `compact` preserves logical order with no sort-order spec — the 90 % partition-style prune already comes from release pinning + projected reads. |
| **Weekly logical retirement via `plan-operation-retirement` → `retire-operation`** (Group 3) | docspec-rs accumulates one retained release + native metadata per operation (measured: 28 release objects, `metadata_objects_read` 48 for one day of polling); 26 years of hourly polls need bounded reclamation, which is this engine's snapshot-expiry analog. |
| **Skip aggressive snapshot expiry** (rejected as unnecessary beyond weekly retirement) | `RetirementInspection` showed `protected_bytes` 118,852 / `unmatched_bytes` 0 — nothing is unowned today; aggressive expiry's only lever (dropping replay/recovery history) conflicts with news_lake's evidence-bound audit requirements. |
| **`seal-generation` per producer cycle under `require_data_work`** (Group 3) | Coupled producer families (news sources polled together) need one exact accepted-version boundary per generation so incremental aggregate refresh can qualify inputs metadata-only, without payload passes. |
| **Do NOT enable per-page (`EnabledStatistics::Page`) statistics** (rejected, deliberate deviation) | Offset indexes are disabled on both writers (bounded metadata admission), so page statistics are unreadable to every reader while multiplying footer size up to ~pages-per-row-group — the export footer admission (32 MiB cap, `validate_output`) could be breached on wide/long exports. |
| **Do NOT force per-column encodings for `resolution`/`aboutness`/`processor`/`provider` beyond dictionary enablement** (rejected) | `set_column_encoding` *forces* an encoding with no fallback (verified in parquet 58.4.0 source), which degrades high-cardinality columns; `set_dictionary_enabled(true)` (already default) gives dictionary encoding with transparent fallback, and the four news_lake columns are explicitly enabled for intent. |
| **Do NOT adopt MonthTransform partitioned native writes yet** (deferred; see I5 section) | docspec-rs writes via `UnpartitionedWriter`; partition-spec support is a real follow-up, and at 5 GB live scale file-level compaction plus projection pushdown already delivers the measured pruning. |

## Phase 1 — Measured profile

Harness: `cargo test --locked --all-features --lib engine::profile_tests -- --ignored --nocapture`
(emulates news_lake: 4,096-row backfill + 24 hourly polls of 60 records each,
news-shaped schema with low-cardinality `resolution`/`provider` and a wide
`headline` payload column).

| Metric | Measured |
|---|---|
| Rows | 5,536 |
| Live data files | **25** (one per poll, plus backfill) |
| File size min / median / max | 3,241 B / 3,247 B / 15,649 B |
| Files under 64 MiB | **100 %** |
| Live data bytes | 93,572 B |
| Native snapshots in the current pin | 1 (each release pins its own native snapshot; accumulation appears as retained release/native-metadata objects) |
| Release objects / metadata objects read | 28 / 48 |
| `inspect_retention` | protected 118,852 B, unmatched 0 B |
| Compaction (64 MiB target) | **25 files → 1 file; 93,572 B → 36,707 B; 5,536 rows preserved** |

**Small-file pressure:** severe and structural — every poll appends one file,
so without compaction a 26-year, hourly-polled dataset accumulates
~24 × 365 × 26 ≈ 228k files. The writer's rolling `target_file_bytes`
(`write_batches_sized`) bounds only individual write bursts; sustained
compaction is the correct countermeasure (measured 25:1 collapse).

## Phase 2 — Workload (derived)

- **Writer:** docspec-rs engine itself; ingestion cadence hourly appends
  (news polling), copy-on-write semantics (no delete files; corrections are
  keyed replacements producing new files).
- **Access:** `coverage_daily`-style incremental aggregate refresh
  (`Date32` keys + `COUNT`/`COUNT(DISTINCT)`) over month ranges; projected
  reads of validated snapshots; occasional full-group exports for consumers.
- **Pruning:** release pinning resolves an exact snapshot before any payload
  read; scans are projection-pushed (verified in the plan below). Assumed
  90 % month-range prune for extrapolation.
- **Intent assumptions** (no live interview; derived from the news_lake brief):
  freshness minutes-hours, 26-year retention with audit/evidence obligations,
  moderate query volume (~1k refreshes/reads per month per dataset).

## Phase 4 — Scenario simulation (directional)

`simulate.py --profile profile.json --workload workload.json --priority total`
on a news_lake-scale extrapolation (5 GB live, 100 % small files, 90 % prune):

```
scenario        scan GB/q   latency s  query $/mo  maint $/mo  storage $/mo  total $/mo
do_nothing      0.5         0.25       2.44        0.0         0.15          2.59
light           0.45        0.225      2.2         0.1         0.13          2.42
targeted_sort   0.125       0.062      0.61        0.1         0.13          0.83
aggressive      0.075       0.037      0.37        0.73        0.14          1.24
storage_min     0.475       0.237      2.32        0.02        0.11          2.46
```

Interpretation for docspec-rs: the simulator's `targeted_sort` win is an
artifact of its generic "sorted filter column" heuristic — docspec-rs has no
sort compaction and its scans are local, unpriced pin reads. The honest
ranking is **light ≈ daily bin-pack compaction**, which captures the file-count
collapse (the actual measured benefit) without a nonexistent sort operator or
aggressive retention loss. The simulator's own docstring marks these as
order-of-magnitude only.

## Commands (docspec-rs operations, in run order)

Group 2 (writer, landed in this change set — takes effect on the next write):

```sh
cargo build --locked --bin docspec   # writer properties land with the binary
```

Group 1 (layout):

```sh
# Daily bin-pack after the polling cycle. files: [] selects every live file.
docspec compact compaction.json
# compaction.json: {"dataset":"news","operation":"<id>","version":"<release-id>",
#                   "group":"source","files":[],"target_file_bytes":67108864}
```

Group 3 (maintenance):

```sh
docspec inspect-retention objects.json                 # read-only protection census
docspec plan-operation-retirement --dataset news --retirement <id> --operation <id> --reason "weekly bounded reclamation"
docspec retire-operation plan.json                     # only after reviewing the plan
docspec seal-generation generation.json                # once per producer cycle
```

## Schedule (matched to hourly news polling)

| When | Action |
|---|---|
| Per poll (hourly) | `update` append; writer properties (Group 2) apply automatically |
| Daily, ~04:00 local after the night's polls | `compact` with 64 MiB target (Group 1) |
| Per producer cycle | `seal-generation` for the coupled news-source family |
| Weekly, after compaction | `inspect-retention` → `plan-operation-retirement` → review → `retire-operation` (Group 3) |
| Monthly | Re-profile with the harness (`engine::profile_tests`) and refresh this report |

## Monitoring thresholds (trigger the next run)

| Signal (source) | Threshold | Action |
|---|---|---|
| Median live file bytes (compaction input inventory) | < 8 MiB for two consecutive days | Run `compact` |
| Data-file count since last compaction | > 48 (2 days of polls) | Run `compact` early |
| `metadata_objects_read` (inspect-retention) | > 512 per dataset | Weekly retirement; investigate per-operation metadata growth |
| `unmatched_bytes` (inspect-retention) | > 0 | Stop reclamation; audit ownership before any retirement |
| Compaction `output_bytes` ≥ `input_bytes` on a mostly-small input | ratio > 0.95 | Writer regression — check writer properties landed |

## news_lake I5 month-partitioned layout mapped onto docspec datasets

Iceberg-profile I5 specifies month-partitioned tables (`MonthTransform` on
`observed_at`/`recorded_at`) created on the Python side. Mapping onto
docspec-rs:

- **Prune equivalence:** a month-partitioned scan prunes to ~1/312 of history;
  docspec's release pin + `NativeScan` projection achieves the equivalent for
  aggregate refreshes because each refresh reads one pinned snapshot and only
  the referenced columns (plan below), and compaction bounds the file set it
  must open. Range filters on `observed_at` additionally benefit from chunk
  statistics once row-group predicate pushdown lands (see gaps).
- **Generation discipline ≈ partition evolution boundary:** seal one
  `seal-generation` per producer cycle and retire terminal operations weekly;
  docspec's immutable release pins give the same "old data stays readable"
  guarantee partition evolution provides, without a spec change.
- **Gap (follow-up):** docspec-rs native writes use
  `partitioning::unpartitioned_writer::UnpartitionedWriter` — there is no
  partition-spec support in the writer or in `compact` today, so native files
  carry no Iceberg partition transforms. If news_lake requires
  Iceberg-native `MonthTransform` layout (e.g. for foreign engines reading the
  objects directly with partition pruning), docspec-rs needs: (1) partitioned
  writer support in `storage.rs`, (2) partition-aware `plan_files`/selection,
  and (3) a partition-evolution story for existing tables. Until then, treat
  docspec datasets as unpartitioned Iceberg tables and rely on
  pin-projection-compaction pruning.

## Group 2 ingestion fixes applied (Task B) — verified against parquet 58.4.0

Ordering honored: writer fixes first, layout second, maintenance third.
Every setter below was verified in the vendored source
(`~/.cargo/registry/src/.../parquet-58.4.0/src/file/properties.rs`), not taken
from skill text:

| Change | File | Verified API |
|---|---|---|
| Explicit ZSTD level 3 (was crate default level 1) | `src/storage.rs`, `src/engine/export.rs` | `set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))`; `From<ParquetError>` exists so `?` composes |
| Parquet 2.0 writer version | both writers | `set_writer_version(WriterVersion::PARQUET_2_0)` (default was `PARQUET_1_0`) |
| 1 MiB data pages (explicit) | both writers | `set_data_page_size_limit(1024 * 1024)` (parquet default equals 1 MiB) |
| Dictionary encoding with fallback | both writers | `set_dictionary_enabled(true)` (+ explicit `set_column_dictionary_enabled(ColumnPath::from(col), true)` for `resolution`, `aboutness`, `processor`, `provider`) |
| Chunk statistics on exports (was `None`) | `src/engine/export.rs` | `set_statistics_enabled(EnabledStatistics::Chunk)` + `set_statistics_truncate_length(Some(64))` — foreign readers gain row-group pruning |
| Byte-bounded export row groups | `src/engine/export.rs` | `set_max_row_group_row_count(None)` + `set_max_row_group_bytes(Some(128 * 1024 * 1024))`; the writer's existing 64 MiB memory flush fires first, the byte cap only guards abnormal widths |
| Row-group sizing on native writes | `src/storage.rs` | kept `set_max_row_group_row_count(Some(budget.limits.batch_rows))` (now 8,192): one row group ≤ one admitted batch ≤ `max_batch_bytes` (64 MiB) decoded, i.e. under a ~128 MiB encoded target by admission; `set_max_row_group_bytes` deliberately unset because a byte cap larger than the rolling file target would skew output file sizing |
| Reader batch range 8,192–65,536 | `src/budget.rs` | `Limits::default().batch_rows` 4,096 → 8,192 (drives reader batches and writer row groups); wide-row datasets must scale `max_batch_bytes` accordingly (three test fixtures updated: `engine/write/build/tests.rs`, `engine/locations/release/tests.rs`) |
| Projection pushdown (already present, now plan-verified) | `src/query.rs` | `TableProvider::scan(projection)` → `NativePartition::project` → selected-column native reads; `NativeScan` display now includes `projection=[...]` |

## Skill corrections applied

- **parquet-optimization row-group bug:** the skill's
  `.set_max_row_group_size(100_000_000)` was **not** used anywhere. In parquet
  58.x that setter takes a **row count**, not bytes — 100,000,000 rows per row
  group would buffer unbounded memory. Verified in the vendored source:
  `set_max_row_group_size` is `#[deprecated(since = "58.0.0", note = "Use
  set_max_row_group_row_count instead")]`; the byte-based API
  `set_max_row_group_bytes(Option<usize>)` **does exist** ("Row groups are
  flushed when their estimated encoded size exceeds this threshold"; if both
  limits are set, the smaller wins) and is used on the export writer with
  `set_max_row_group_row_count(None)`. The native writer uses the documented
  row-count equivalent because its admission model and rolling file target are
  row-count-aligned.
- **Page statistics deviation:** `EnabledStatistics::Page` exists in 58.4.0,
  but is deliberately not enabled — offset indexes are disabled on both
  writers for bounded metadata admission, so page statistics would be
  unreadable to readers while inflating footers (the export path enforces a
  32 MiB footer allowance). Chunk-level truncated statistics (`Some(64)`)
  provide the row-group pruning foreign readers actually use.
- **Verified-not-copied generally:** `EnabledStatistics::{None,Chunk,Page}`,
  `set_column_encoding(ColumnPath, Encoding)` (forces an encoding; therefore
  not used generically), `set_offset_index_disabled(bool)`, and
  `set_column_dictionary_enabled(ColumnPath, bool)` were all confirmed against
  the installed source before use; skills were treated as hints, the compiler
  and vendored source as authority.

## Task C — DataFusion physical plan for `coverage_daily`

Captured via `engine::profile_tests::capture_coverage_daily_aggregate_plan`
(`DataFrame::create_physical_plan` + `ExecutionPlan::display_indent`), for the
`Date32`-keyed aggregate with `COUNT(DISTINCT ...)` plus a day-range predicate:

```
ProjectionExec: expr=[day@0 as day, count(Int64(1))@1 as rows, count(DISTINCT aggregate_input.author)@2 as distinct_authors]
  AggregateExec: mode=FinalPartitioned, gby=[day@0 as day], aggr=[count(Int64(1)), count(DISTINCT aggregate_input.author)]
    RepartitionExec: partitioning=Hash([day@0], 12), input_partitions=12
      AggregateExec: mode=Partial, gby=[day@0 as day], aggr=[count(Int64(1)), count(DISTINCT aggregate_input.author)]
        FilterExec: day@0 >= 2022-01-01
          RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
            DataSourceExec: NativeScan: rows=Some(5), projection=[day, author]
```

- **Hash aggregate, not sort aggregate:** `AggregateExec mode=Partial` +
  `mode=FinalPartitioned` with `gby=[...]` and no `orderby:`/`SortExec`
  (asserted in `engine::aggregation::tests::date32_keys_and_count_distinct_refresh_full_and_incrementally`).
- **Projection pushdown at the scan:** `NativeScan ... projection=[day, author]`
  — the `headline`/`id` payload columns are not scanned (asserted in the same
  test, which also asserts the absence of `id`, `resolution`, `headline`, and
  the hidden occurrence columns from the rendered plan).
- **Predicate behavior (recorded follow-up gap):** the `day >=` filter remains
  a `FilterExec` *above* `NativeScan`; the custom `TableProvider` currently
  ignores `scan(filters)`, so SQL predicates do not yet prune Parquet row
  groups via chunk statistics (file-level selection by pin/fields is what
  prunes today). Implementing predicate composition into the native reader is
  the highest-leverage scan follow-up for month-range refreshes.

## Reproducibility

- Profile: `cargo test --locked --all-features --lib engine::profile_tests::profile_hourly_polling_workspace -- --ignored --nocapture`
- Plan: `cargo test --locked --all-features --lib engine::profile_tests::capture_coverage_daily_aggregate_plan -- --ignored --nocapture`
- Simulator inputs preserved at
  `/private/var/folders/g8/10vdwk2n07z85vqn_vlv2lfc0000gn/T/opencode/docspec-profile/{profile,workload}.json`
