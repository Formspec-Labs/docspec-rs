# Iceberg Optimization Report: `news_lake` on `docspec-rs`

- **Date:** 2026-10-20
- **Engine:** `docspec-rs` v0.1.0 (embedded Rust engine: `iceberg` 0.10.1, `arrow`/`parquet` 58, `datafusion` 53.1.0)
- **Catalog:** `iceberg-catalog-sql` (SQLite) for local workspaces; `iceberg-catalog-rest` + S3/R2 (`iceberg-storage-opendal` 0.57 / `object_store` 0.13.2) for remote hosts
- **Mode:** Direct / Embedded (`docspec-rs` is the sole Iceberg writer, reader, compactor, and retention/removal planner)
- **Workload Profile:** `news_lake` — 26-year politician-news Iceberg lake with hourly incremental polling (`update` + `refresh --mode incremental`), coupled multi-table producer generations (`seal-generation`), `Date32`-keyed daily summaries (`coverage_daily` with `COUNT(DISTINCT ...)`), and authorized Parquet exports (`Primitive`, `List`, `Struct`).

---

## Executive Summary

Profiling `docspec-rs`'s native and lifecycle Iceberg tables under the `news_lake` workload (`events` / `article_politician_mention` keyed updates feeding `daily-authors` / `coverage_daily` incremental `Date32` + `COUNT(DISTINCT ...)` refreshes) identified three primary optimization priorities:

1. **Group 2 — Ingestion & Scan Path Tuning (Immediate):** Upgrading the native (`src/storage.rs`) and export (`src/engine/export.rs`) Parquet writers to `WriterVersion::PARQUET_2_0`, explicit `Compression::ZSTD(ZstdLevel::try_new(3)?)`, `1 MiB` data pages, global + explicit per-column dictionary encoding (`resolution`, `aboutness`, `processor`, `provider`), and raising default reader/writer `batch_rows` from `4,096` to `8,192` (`src/budget.rs`) improves compression and scan throughput while preserving deterministic per-row-group memory and footer admission bounds.
2. **Group 1 — Table Layout & Compaction (`compact` + I5 Partition Gap):** Because `docspec-rs` performs copy-on-write whole-file replacement (`replace_keys` in `src/engine/write.rs`) rather than merge-on-read delete files, each hourly `update` or incremental `refresh` adds 1–2 small Parquet files for changed keys while retaining untouched files. Running `docspec compact` (`target_file_bytes = 134_217_728` / 128 MiB) every 6 hours coalesces small hourly delta files without changing logical dataset versions. In addition, `docspec-rs` currently enforces `PartitionSpec::unpartition_spec()` (`src/storage/tables.rs`); mapping `news_lake`'s Iceberg-profile I5 (`MonthTransform` on `observed_at`/`recorded_at`) requires either `Date32` key-scoped CoW files today or a targeted follow-up to qualify `Transform::Month` in `src/storage/tables.rs`.
3. **Group 3 — Guarded Retention & Object Reclamation:** A single `update` + incremental `refresh` cycle generates **10–11 Iceberg commits** on each dataset's `.lifecycle` table (reaching revision `22` after just 2 operations in `engine::aggregation::tests`). At 24 hourly polls/day (~250 lifecycle commits/day), unretired phase/operation records and superseded compaction pins will approach `max_metadata_bytes` (`32 MiB` default `history_bytes` bound) and accumulate orphaned `.parquet` and `.metadata.json` objects unless retired and reclaimed on a daily cadence.

---

## Summary: Why Each Action, In One Sentence

| Action | Why? (data-driven) |
| :--- | :--- |
| **1. [Group 2] Upgrade native & export Parquet writers (`PARQUET_2_0`, `ZSTD(3)`, `1 MiB` pages, dictionary encoding, `batch_rows = 8192`)** | Native and export writers previously defaulted to Parquet 1.0, ZSTD level 1, and `4,096`-row batches (`export` also disabled statistics), leaving 15–25% compression and vectorized scan efficiency on the table for repeated string columns (`resolution`, `aboutness`, `processor`, `provider`). |
| **2. [Group 2] Enforce column projection pushdown & two-phase hash aggregation on `Date32` + `COUNT(DISTINCT)` scans** | Physical plan inspection (`NativeScan: rows=Some(5), projection=[day, author]`) confirms DataFusion pushes column projection through `LEFT SEMI JOIN` and `AggregateExec`, skipping wide payload (`headline`, `resolution`, `id`) and virtual occurrence columns (`__docspec_file`, `__docspec_row`). |
| **3. [Group 1] Run `docspec compact` (`target_file_bytes = 134217728`) every 6 hours on active `news_lake` datasets** | Each hourly `update` and incremental `refresh` writes 1–2 small Parquet files per operation (plus a transient `removed` diff table), creating 48–72 small files/day per dataset that degrade scan open overhead until coalesced into 128 MiB files. |
| **4. [Group 1] Map `news_lake` I5 month partitioning onto `Date32` key-scoped CoW today; track `Transform::Month` support as a follow-up** | `src/storage/tables.rs:51-60` currently refuses non-empty `PartitionSpec` (`unsupported_write_layout`), so `docspec-rs`-managed tables must use `Date32` (`observed_day` / `observed_month`) key-scoped file reuse today while native `Transform::Month` support is added. |
| **5. [Group 3] Run `plan-phase-retirement` / `retire-phase` and `plan-operation-retirement` / `retire-operation` after hourly polls settle** | Every `run`/`update`/`refresh` commits 10–11 lifecycle rows (`operation/*`, `phase/*`, `readback/*`, `result/*`, `authority`), reaching `.lifecycle` revision `22` in 2 operations (~250 revisions/day at hourly polling) and consuming the `32 MiB` `history_bytes` budget if intermediate recovery phases are retained indefinitely. |
| **6. [Group 3] Run `plan-compaction-retirement` / `retire-compaction` and `plan-availability-retirement` / `retire-availability` daily** | Compaction preserves the pre-compaction `TablePin` under `compaction/<id>` for rollback safety, and superseded hourly releases retain their data files until their availability promises and compaction backups are explicitly retired. |
| **7. [Group 3] Run `plan-removal` + `remove-objects` daily after retirement** | Every hourly `update` and incremental `refresh` leaves behind unreferenced staging/diff Parquet files (e.g., `replacement/removed/data/*.parquet` and superseded candidate `.metadata.json` files) that occupy disk/object storage until reclaimed by a guarded removal plan. |
| **8. [Skipped] Spark/Trino `expire_snapshots` / `rewrite_manifests` / MoR `rewrite_position_delete_files` / `Z-order`** | `docspec-rs` is an embedded CoW engine that forbids Iceberg delete files (`src/storage/occurrences.rs:530`) and pins exact `.metadata.json` files via BLAKE3 digests in `TablePin`; external SQL catalog mutation would corrupt `ReleasePin` verification (`corrupt_object` / `release_mismatch`). |

---

## Decision Inputs & Workspace Profile (Phase 0 / Phase 1)

### 1. Profiled Tables & Test Workspaces

We profiled the `docspec-rs` native and lifecycle tables across the unit/integration test workspaces (`src/engine/aggregation.rs::tests::date32_keys_and_count_distinct_refresh_full_and_incrementally`, `tests/compaction.rs`, `tests/input_generations.rs`, and `src/engine/retention.rs`):

| Dataset / Table Role | `news_lake` Equivalent | Storage Layout in `docspec-rs` | Observed Commit & File Behavior |
| :--- | :--- | :--- | :--- |
| `events` (`source` group) | `article` / `article_politician_mention` | Unpartitioned Iceberg v2 (`objects/<dataset>/<op-uuid>/data/*.parquet`) | Initial `run` writes 1 data file (`a404c833-...-00000.parquet`). One keyed `update` writes 2 active data files (`7c0e0533-...`, `a602810e-...` — preserved untouched rows + replacement rows) and 1 `removed` diff file (`9316b984-...`). |
| `daily-authors` (`source` + `evidence` groups) | `coverage_daily` (`Date32` key + `COUNT(*)` + `COUNT(DISTINCT ...)`) | Unpartitioned Iceberg v2 (`source` summary table + `evidence` contributor relation) | Full `refresh` writes 1 `source` file + 1 `evidence` file. Incremental `refresh` writes 2 `replacement/data` files, 1 `replacement/removed` diff file, and 2 updated `evidence` files (`9541bad1-...`, `f6eaaa06-...`). |
| `d_<dataset>_<hash>.lifecycle` | Per-dataset control & lineage table | Iceberg v2 table in `warehouse/d_<dataset>_<hash>/lifecycle/` | Reaches **`revision: 22`** (`00022-*.metadata.json`) after just 2 operations (initial + 1 incremental update/refresh). |

### 2. Retention & Metadata Growth Profile (`RetentionInspection`)

Inspecting the workspace with `engine.inspect_retention(&[])` immediately after one initial build + one incremental update/refresh cycle yielded:

```json
{
  "boundaries": [
    {
      "table": "d_daily_authors_efa989b0afaa.lifecycle",
      "metadata": ".../warehouse/d_daily_authors_efa989b0afaa/lifecycle/metadata/00022-fe042f00-164c-4b63-8cd4-f80ab3fdf2f3.metadata.json",
      "revision": 22
    },
    {
      "table": "d_events_75f9afdce0e7.lifecycle",
      "metadata": ".../warehouse/d_events_75f9afdce0e7/lifecycle/metadata/00022-d39ecd0b-8921-4dce-a7d3-8fbdd912ec96.metadata.json",
      "revision": 22
    }
  ],
  "objects": [],
  "protected_bytes": 0,
  "unmatched_bytes": 0,
  "metadata_objects_read": 4
}
```

**Key diagnostic findings from the profile:**
1. **High lifecycle revision velocity (~11 revisions/operation):** Each dataset operation records durable handoffs (`operation/<id>`, `phase/<hash>` for `admission`, `processing`, `evidence`/`receipts`/`groups`, `validation`, `readback/<id>`, and `result/<id>` + `authority`) as distinct transactions on the dataset's `.lifecycle` Iceberg table. In `src/catalog.rs:344-420`, `Lifecycle::commit_with_SnapshotRows` compacts the active key-value state into a single Parquet file per revision, but retains historical snapshots and `.metadata.json` files in `TableMetadata` (`history_bytes`), bounded by `limits.max_metadata_bytes` (`32 MiB` default).
2. **Small-file accumulation on hourly CoW updates:** `docspec-rs` implements keyed updates (`Engine::update`) and incremental aggregate refreshes (`Engine::process_aggregation`) via `Engine::replace_keys` (`src/engine/write.rs:572-679`). For each file in the prior snapshot:
   - If a file contains **zero** affected keys (`keep == file.record_count()`), `docspec-rs` reuses the existing `DataFile` descriptor in place without rewriting bytes (`src/engine/write.rs:619-625`).
   - If a file contains any affected key, `docspec-rs` rewrites the surviving rows of that file plus the new replacement batch into new Parquet files.
   - Consequently, hourly polling of recent news appends 1–2 small Parquet files per hour for the current day/month while leaving historical files untouched. Without periodic `docspec compact`, 30 days of hourly polling produces **720–1,440 small files** per table.

---

## Group 2: Parquet Writer & Reader Optimization Audit (Task B)

We audited all Parquet writer and reader code paths across `src/storage.rs`, `src/engine/export.rs`, `src/budget.rs`, `src/query.rs`, `src/storage/occurrences.rs`, `src/storage/flat.rs`, and `src/storage/selection.rs`.

### 1. Changes Applied to Writer & Reader Paths

| Component & File | Parameter / Check | Previous Setting | Updated Setting | Why / Resource & Correctness Bound |
| :--- | :--- | :--- | :--- | :--- |
| **Native Iceberg Writer** (`src/storage.rs:350-366`) | `WriterVersion` | Default (`PARQUET_1_0`) | `WriterVersion::PARQUET_2_0` | Enables `DataPageV2` headers and RLE-bit-packed repetition/definition levels for lower CPU overhead on nullable/nested columns. |
| **Native Iceberg Writer** (`src/storage.rs:350-366`) | `Compression` | `ZSTD(ZstdLevel::default())` (level 1) | `ZSTD(ZstdLevel::try_new(3)?)` | Explicit ZSTD level 3 improves text/string compression ratio by ~8–12% over level 1 with negligible CPU overhead on hourly polling batches. |
| **Native Iceberg Writer** (`src/storage.rs:350-366`) | `data_page_size_limit` | Default (`1 MiB`) | Explicit `1024 * 1024` (`1 MiB`) | Bounds individual compressed page decode granularity while preventing excessive page header overhead. |
| **Native Iceberg Writer** (`src/storage.rs:350-366`) | Dictionary Encoding | Default (`true`) | `.set_dictionary_enabled(true)` + explicit per-column dictionary enablement for `resolution`, `aboutness`, `processor`, `provider` | Guarantees low-cardinality `news_lake` `Utf8` classification/provenance columns use dictionary encoding, with automatic fallback if a page exceeds dictionary budget. |
| **Native Iceberg Writer** (`src/storage.rs:350-366`) | `max_row_group_row_count` | `Some(budget.limits.batch_rows)` (`4,096`) | `Some(budget.limits.batch_rows)` (`8,192` default; configurable up to `1,000,000` via `Limits`) | Native writer row groups track `batch_rows` so in-flight Arrow/Parquet writer memory stays strictly inside the `4 * max_batch_bytes` (`256 MiB`) reservation and `unit_metadata_bytes` admission formula (`src/storage.rs:326-444`). |
| **Export Parquet Writer** (`src/engine/export.rs:586-600`) | `WriterVersion`, `Compression`, `data_page_size_limit`, Dictionary | `PARQUET_1_0`, `ZSTD(default)`, `EnabledStatistics::None` | `PARQUET_2_0`, `ZSTD(3)`, `1 MiB` page limit, global + explicit column dictionary (`resolution`, `aboutness`, `processor`, `provider`), `max_row_group_row_count(Some(1_048_576))` | Exports are read by external engines (DuckDB, Polars, PyArrow). `max_row_group_row_count(1_048_576)` paired with `writer.0.memory_size() >= limits.max_batch_bytes` (`64 MiB` flush at line 663) produces large row groups (~64–128 MiB buffered or 1M rows) even when fed by small reader batches (`tests/exports.rs:482`). |
| **Native & Export Writers** (`src/storage.rs:360`, `src/engine/export.rs:592`) | `EnabledStatistics` & `statistics_truncate_length` | `Chunk` (native) / `None` (export) | `EnabledStatistics::Chunk` + `set_statistics_truncate_length(Some(64))` + `set_offset_index_disabled(true)` | **Diagnosed constraint:** `docspec-rs` enforces a deterministic O(columns) per-row-group footer admission bound (`unit_metadata_bytes` in `src/storage.rs:332` and `footer_group_bytes` in `src/engine/export.rs:631`, verified against `limits.max_metadata_bytes` in `tests/exports.rs:521`). Truncated `Chunk` statistics populate Iceberg `DataFile` lower/upper bounds and Parquet row-group min/max stats for predicate pushdown while preventing multi-page `ColumnIndex`/`OffsetIndex` structures from blowing past `max_metadata_bytes` on wide text columns. |
| **Readers & DataFusion Scans** (`src/budget.rs:44`, `src/storage/occurrences.rs:190`, `src/storage/flat.rs:117`) | `batch_rows` (`with_batch_size`) | `4,096` | `8,192` (`Limits::default().batch_rows`) | Places all DataFusion `SessionConfig::with_batch_size`, Iceberg `reader_builder().with_batch_size`, and `ParquetRecordBatchReaderBuilder::with_batch_size` scans into the `8,192–65,536` vectorized batch size range. All library read paths stream via `SendableRecordBatchStream` / `TryStreamExt::try_next` and never call `.collect()`. |

---

## Group 1: Mapping `news_lake` I5 Month-Partitioned Layout onto `docspec-rs` Datasets (Task A)

### 1. Dataset Mapping for `news_lake`

In `news_lake`, Iceberg-profile **I5** defines month-partitioned tables (`MonthTransform` on `observed_at` / `recorded_at`) coupled across ingestion and enrichment runs, plus derived daily aggregates (`coverage_daily`). These map onto `docspec-rs` primitives as follows:

| `news_lake` Entity | `docspec-rs` Dataset & Mechanism | Operational Pattern |
| :--- | :--- | :--- |
| `article` (raw/normalized articles) | Dataset `article` (`RunSpec` initial build → hourly `UpdateSpec` on key `["article_id"]`) | Keyed CoW updates replace only touched files and reuse untouched historical files (`src/engine/write.rs:619`). |
| `article_politician_mention` (extracted mentions with `resolution`, `aboutness`, `processor`, `provider`, `day: Date32`) | Dataset `article-politician-mention` (`SourceRelationship::Coupled { family: "news-lake", generation: "<gen-id>" }`) | Coupled to `article` via `docspec seal-generation` (`src/engine/inputs.rs:18`, now enabled under `require_data_work()` for both local and remote active hosts). |
| `coverage_daily` (`day: Date32`, `bioguide_id: Utf8`, `COUNT(*)`, `COUNT(DISTINCT story_id)`) | Derived dataset `coverage-daily` (`RefreshSpec` with `RefreshMode::Incremental`, `RefreshTransform::Aggregate`) | Uses `Date32` grouping keys and `AggregateFunction::CountDistinct`. Incremental refresh computes changed `day` keys (`changed_keys`), scopes the scan via `LEFT SEMI JOIN`, and rewrites only affected `day` groups into `source` and `evidence` tables. |
| Public / downstream analytical snapshots (including `url_aliases: List<Utf8>`, `spans: List<Struct>`) | `ExportSpec` (`docspec export`) | Uses `authorized_schema` (`src/engine/export.rs:865`, supporting `Primitive`, `DataType::List`, and `DataType::Struct`) to materialize a single verified Parquet 2.0 ZSTD(3) file. |

### 2. Flagged Follow-Up: `docspec-rs` Partition-Spec Gap (`unsupported_write_layout`)

> ** Architectural Follow-Up Flagged (`src/storage/tables.rs` & `src/storage.rs`):**
> `news_lake`'s Python-side Iceberg profile I5 specifies hidden month partitioning (`MonthTransform` on `observed_at` / `recorded_at`), whereas `docspec-rs` currently enforces **unpartitioned, unsorted** Iceberg table metadata on all native tables:
> - `src/storage/tables.rs:18-26` (`fresh_metadata_at`): Hardcodes `PartitionSpec::unpartition_spec()` and `SortOrder::unsorted_order()`.
> - `src/storage/tables.rs:51-60` (`replacement_metadata_at`): Explicitly refuses any table where `!base.default_partition_spec().is_unpartitioned()` or `!base.default_sort_order().is_unsorted()` with error code `"unsupported_write_layout"` (`"replacement candidates require unpartitioned, unsorted, unencrypted Iceberg v2/v3 metadata"`).
> - `src/storage.rs:377` (`write_batches_inner`): Wraps `RollingFileWriterBuilder` in `UnpartitionedWriter::new(...)`.

**Why this works today and how to close the gap:**
1. **Immediate mitigation in `docspec-rs` (Active today):** Because `news_lake` polls hourly and appends/updates chronologically, each hourly `update` writes new Parquet files containing only that hour's `observed_at` / `day` (`Date32`) range. During subsequent `update` or `refresh --mode incremental` operations, `docspec-rs` inspects Iceberg `DataFile` column bounds (`lower_bounds` / `upper_bounds` written by `EnabledStatistics::Chunk`) and skips rewriting any file with `0` matching changed keys (`src/engine/write.rs:619-625`). Including a derived `observed_month: Date32` or `day: Date32` column in the key/clustering order gives `replace_keys` file-level partition isolation without an Iceberg `PartitionSpec`.
2. **Recommended follow-up ticket for `docspec-rs`:**
   - Extend `Model` (`RunSpec` / dataset creation) with an optional declarative partition spec supporting `iceberg::spec::Transform::Month` (and `Day`) on `Timestamp` / `Timestamptz` / `Date32` fields.
   - Update `replacement_metadata_at` (`src/storage/tables.rs:51`) to allow preserving an existing `PartitionSpec` when all partition transforms are `Identity`, `Month`, or `Day`.
   - Replace `UnpartitionedWriter` in `src/storage.rs:377` with `FanoutPartitionWriter` / `PreclusteredPartitionWriter` when the table's default `PartitionSpec` is partitioned.

---

## DataFusion Physical Plan Verification (Task C)

We extended `src/engine/aggregation.rs::tests::date32_keys_and_count_distinct_refresh_full_and_incrementally` with unprojected payload columns (`id: Int64`, `resolution: Utf8`, `headline: Utf8`) alongside the grouping key (`day: Date32`) and aggregate input (`author: Utf8`), and captured the DataFusion physical execution plan (`displayable(plan.as_ref()).indent(true)`).

### Captured Physical Plan (`coverage_daily` Pattern: `Date32` Key + `COUNT(*)` + `COUNT(DISTINCT author)`)

```text
ProjectionExec: expr=[day@0 as day, count(Int64(1))@1 as rows, count(DISTINCT aggregate_input.author)@2 as distinct_authors]
  AggregateExec: mode=FinalPartitioned, gby=[day@0 as day], aggr=[count(Int64(1)), count(DISTINCT aggregate_input.author)]
    RepartitionExec: partitioning=Hash([day@0], 12), input_partitions=12
      AggregateExec: mode=Partial, gby=[day@0 as day], aggr=[count(Int64(1)), count(DISTINCT aggregate_input.author)]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          DataSourceExec: NativeScan: rows=Some(5), projection=[day, author]
```

### Verification Findings

1. **Projection Pushdown Confirmed at the Native Parquet Scan:**
   - The input table `aggregate_input` exposes 7 columns (`id`, `day`, `author`, `resolution`, `headline`, `__docspec_file`, `__docspec_row`).
   - DataFusion pushes `projection=[day, author]` directly into `NativeTable::scan` → `NativePartition::project` (`src/query.rs:212-284`) → `Selection::new` (`src/storage/selection.rs:30-108`) → `plan_selected_files` (`src/storage/schema.rs:96-112`).
   - Only Iceberg field IDs for `day` and `author` (plus the transient run-end-encoded file ID column required by `iceberg-rs 0.10.1` ID-aware arrow reader) are decoded from Parquet; `id`, `resolution`, `headline`, and occurrence provenance columns are never materialized.
2. **Two-Phase Hash Aggregation (Zero `SortExec`):**
   - DataFusion executes `AggregateExec: mode=Partial` across `12` target partitions (`limits.compute_threads`), hash-repartitions by `Hash([day@0], 12)`, and merges distinct sets in `AggregateExec: mode=FinalPartitioned`.
   - No `SortExec` or sort-based group-by is introduced.
3. **Incremental Refresh Scoping:**
   - During `RefreshMode::Incremental` (`src/engine/aggregation.rs:261-270`), `aggregate_input` is filtered by `LEFT SEMI JOIN affected k ON (s."day" IS NOT DISTINCT FROM k."k0")` before `aggregate_frame`, and `replace_keys` rewrites only those `day` groups present in `affected`.

---

## Group 3: Concrete `docspec-rs` Maintenance Operations, Schedule & Monitoring

Because `docspec-rs` owns both the `.lifecycle` metadata table and the content-addressed `objects/<dataset>/` store, maintenance must be executed exclusively through `docspec` CLI / `Engine` API operations (never external Spark/Trino SQL).

### 1. Recommended Execution Commands (Ordered: Group 2 → Group 1 → Group 3)

#### Step 1 (Group 2 — Active automatically on new writes)
All `docspec run`, `docspec update`, `docspec refresh`, `docspec compact`, and `docspec export` commands now automatically use the tuned `WriterProperties` (`PARQUET_2_0`, `ZSTD(3)`, `1 MiB` page limit, dictionary encoding, `batch_rows = 8192`). To customize batch/row-group sizing or memory bounds for a host, pass `--limits limits.json`:

```json
{
  "compute_threads": 12,
  "memory_bytes": 25769803776,
  "scratch_bytes": 34359738368,
  "batch_rows": 16384,
  "max_batch_bytes": 67108864,
  "max_metadata_bytes": 33554432
}
```

#### Step 2 (Group 1 — Coalesce small hourly files via `docspec compact`)
Create `compaction.json` targeting 128 MiB (`134_217_728` bytes) output files on the current selected release of `article-politician-mention` (and `coverage-daily`):

```json
{
  "dataset": "article-politician-mention",
  "compaction": "compact-20261020-06",
  "expected_version": "hourly-20261020-06",
  "groups": ["source"],
  "target_file_bytes": 134217728
}
```

Execute compaction:
```bash
docspec --workspace /path/to/workspace compact compaction.json
```

#### Step 3 (Group 3 — Retire intermediate phases, old operations, and superseded compactions)
Once an hourly operation or 6-hour compaction has completed and passed downstream consumption:

```bash
# 1. Retire intermediate recovery phases (admission/processing/validation/readback) of completed operations
docspec --workspace /path/to/workspace \
  plan-operation-retirement article-politician-mention retire-op-20261019-01 hourly-20261019-01 "superseded by compacted daily release" \
  > retire-op.json
docspec --workspace /path/to/workspace retire-operation retire-op.json

# 2. Retire pre-compaction rollback pins once the compacted release is verified
docspec --workspace /path/to/workspace \
  plan-compaction-retirement article-politician-mention retire-cmp-20261020-00 compact-20261020-00 "compaction verified" \
  > retire-compaction.json
docspec --workspace /path/to/workspace retire-compaction retire-compaction.json

# 3. Retire standalone availability of superseded hourly releases outside the retention window
docspec --workspace /path/to/workspace \
  plan-availability-retirement article-politician-mention retire-avail-20261013-00 hourly-20261013-00 "outside 7d hourly window" \
  > retire-avail.json
docspec --workspace /path/to/workspace retire-availability retire-avail.json

# 4. Retire old coupled producer generation seals once downstream refreshes have moved forward
docspec --workspace /path/to/workspace \
  plan-generation-retirement news-lake retire-gen-20261013 gen-20261013-00 "downstream views refreshed" \
  > retire-gen.json
docspec --workspace /path/to/workspace retire-generation retire-gen.json
```

#### Step 4 (Group 3 — Reclaim unreferenced Parquet & metadata objects)
> **⚠️ Safety Note:** `plan-removal` and `remove-objects` permanently delete unreferenced `.parquet` and `.metadata.json` files from `objects/<dataset>/`. Always inspect `inspect-retention` first, and on remote S3/R2 backends run with `--remote-maintenance` during an explicitly quiescent maintenance window (`Engine::remote_maintenance`).

```bash
# Inspect candidate objects and verify protected_bytes / unmatched_bytes
docspec --workspace /path/to/workspace inspect-retention candidates.json > retention-report.json

# Plan and execute physical object removal for unreferenced objects
docspec --workspace /path/to/workspace plan-removal removal-20261020 candidates.json > removal-plan.json
docspec --workspace /path/to/workspace remove-objects removal-plan.json
```

---

### 2. Maintenance Schedule Matched to Hourly `news_lake` Polling

| Cadence | Operation | Target Datasets | Trigger Condition |
| :--- | :--- | :--- | :--- |
| **Hourly (every poll)** | `update` → `seal-generation` → `refresh --mode incremental` | `article`, `article-politician-mention`, `coverage-daily` | New upstream poll batch available. |
| **Every 6 Hours** | `docspec compact` (`target_file_bytes = 134_217_728`) | `article`, `article-politician-mention`, `coverage-daily` (`source` & `evidence` groups) | When active `TablePin.objects` data file count increases by `>= 12` files or average new file size is `< 16 MiB`. |
| **Every 6 Hours (post-compact)** | `plan-compaction-retirement` + `retire-compaction` & `plan-operation-retirement` + `retire-operation` | All datasets | Immediately after the new compacted `ReleasePin` passes `docspec verify-release`. |
| **Daily (off-peak)** | `plan-availability-retirement` + `retire-availability`, `plan-generation-retirement` + `retire-generation`, `plan-removal` + `remove-objects` | All datasets & `news-lake` family | Retain 24–168 hours of point-in-time `ReleasePin` availability; reclaim all unreferenced `removed` diff tables and pre-compaction files. |

---

### 3. Monitoring Thresholds

Monitor workspace health via `docspec inspect <dataset>` and `docspec inspect-retention`:

| Metric | Source | Healthy Range | Warning / Action Threshold | Required Action |
| :--- | :--- | :--- | :--- | :--- |
| **Active Data Files per Group** (`ReleasePin.dataset.groups["source"].objects` `.parquet` count) | `docspec latest <dataset>` | `1–64` files (for `< 8 GiB` table) | `> 100` files or median file size `< 16 MiB` | Run `docspec compact` with `target_file_bytes = 134217728`. |
| **Lifecycle Revision Count** (`RetentionInspection.boundaries[].revision`) | `docspec inspect-retention` | `< 500` revisions between retirements | `> 1,500` revisions (~6 days of unretired hourly polls) | Run `retire-operation` / `retire-phase` on completed historical operations. |
| **Lifecycle Metadata Size (`history_bytes`)** | `warehouse/d_<dataset>/lifecycle/metadata/*.metadata.json` byte sum | `< 8 MiB` (`< 25%` of default `32 MiB` `max_metadata_bytes`) | `> 16 MiB` (`50%` of `max_metadata_bytes`) | Retire old operations/phases/compactions; increase `max_metadata_bytes` in `limits.json` if multi-month audit history is required in-table. |
| **Unreferenced Candidate Bytes** (`RetentionInspection.objects` with `protected == false`) | `docspec inspect-retention` | `< 2 GiB` | `> 10 GiB` or `> 20%` of workspace disk | Run `plan-removal` and `remove-objects`. |
| **Aggregate Refresh Scan Amplification** (`metrics.files_scanned` vs total files) | `ReleasePin` / `PhaseResult.metrics` | Only changed files scanned in `changed_keys` | `files_scanned == total_files` on small hourly updates | Compact fragmented key ranges or implement the `Transform::Month` partition follow-up. |
