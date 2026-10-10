# Native reader efficiency research

Researched 2026-10-10 against the local lockfile and cached published crate source.
This is a documentation and source investigation. It ran no builds, tests,
benchmarks, corpus reads, or payload hashes and changed no implementation.

## Decision

Keep Iceberg's maintained reader and writer. Improve the small DataFusion adapter
where it prevents column selection and retained statistics from reaching the
execution plan. The published `iceberg-datafusion` provider is compatible with
our versions, but it cannot directly replace the current adapter while preserving
DocSpec's selected schema, occurrence identities, and reader admission. Generic
`ParquetSource` is useful for flat files; substituting it for native Iceberg reads
would move substantial correctness work into DocSpec.

The reported Comments failure is a join-memory failure. These reader findings
identify avoidable work and constraints; they do not establish that changing the
reader alone will fix that failure or meet the complete-path acceptance target.
The accepted architecture remains Iceberg/Parquet storage, Arrow batches, and
DataFusion relational execution; see [PLAN.md](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/PLAN.md:9).

## Exact compatibility and present behavior

[Cargo.toml](/Users/mikewolfd/Work/spicy-stack/docspec-rs/Cargo.toml:13) pins
DataFusion 53.1.0 and Iceberg 0.10.1. The lockfile resolves Arrow and Parquet to
58.4.0. Apache's published `iceberg-datafusion` 0.10.1 requires DataFusion
`53.1.0`, Iceberg `0.10.0`, and Parquet `58`, with Rust 1.94. Those Cargo ranges
accept the existing resolved versions. The integration is currently absent from
DocSpec's dependency graph. This is source-level compatibility, not a new compiled
qualification. [Published dependency manifest](https://docs.rs/crate/iceberg-datafusion/0.10.1/source/Cargo.toml).

The locally retained integration package identifies upstream commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9` in `.cargo_vcs_info.json`.
The pinned upstream links below use that commit or the DataFusion/Arrow release
tags. Unversioned API pages can describe newer behavior and are not the authority
for this recommendation.

| Boundary | Current behavior | Consequence |
| --- | --- | --- |
| Native table registration | One `StreamingTable` partition calls `storage::read_batches` or `read_occurrences`. | SQL projection does not reach this native reader. |
| Captured file registration | `query::files` and `file` can supply an explicit root-column projection to `OccurrenceScan::read_projected_file`. | A qualified narrow-read mechanism already exists for value reads. |
| Ordinary native read | One in-flight data file, configured batch rows, default-expansion check, and stream-owned admission. | Resource policy stays independent of DataFusion's operator pool. |
| Occurrence read | Complete, unfiltered files; no native deletes; exact decoded row counts; retained origins or physical birth locations. | Filtering before assigning fallback positions would change evidence identity. |
| Writer | Maintained Iceberg rolling Parquet writer, Zstandard, truncated chunk statistics, disabled offset index, row-group rows tied to batch rows. | Existing safeguards address large text and metadata growth, with a possible small-row-group tradeoff. |

Local evidence: [query.rs](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/query.rs:26),
[projected reads](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/occurrences.rs:312),
[reader admission](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:86),
[occurrence checks](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/occurrences.rs:369),
[writer properties](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:202).

## What the maintained providers can replace

`IcebergStaticTableProvider` captures a `Table` without refreshing its catalog.
Its snapshot constructor uses the snapshot's schema; its ordinary constructor
advertises the metadata's current schema. Its `scan` accepts projection, filters,
and limit, and declares filter pushdown `Inexact`, leaving DataFusion responsible
for residual filtering. That is the appropriate upstream starting point for
immutable reads; the catalog-backed provider refreshes table metadata and would
not preserve a previously selected DocSpec version. [Provider implementation](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/integrations/datafusion/src/table/mod.rs#L244-L340).

However, its `IcebergTableScan` builds `table.scan().select(...).with_filter(...)`
and calls `to_arrow()`. It ignores `TaskContext`, does not pass DocSpec's batch or
concurrency settings, does not own DocSpec admission permits, and exposes one
DataFusion partition with no implementation of physical statistics. A wrapper
around its emitted batches could reject large results only after upstream
allocation. Its pushed limit filters later batches once the limit is reached;
the scan alone does not terminate the upstream stream at that point. Neither
provider adoption nor a limit therefore establishes a byte or I/O bound.
[Scan implementation](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/integrations/datafusion/src/physical_plan/scan.rs#L106-L237).

The current adapter was deliberately introduced for full comparisons, where all
fields and rows were required. Its original review explicitly accepted the lack
of scan pushdown for that caller. Reusing it more broadly changes that tradeoff.
[Original rationale](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/query-reader-consistency.md:42).

`ParquetSource` with `FileScanConfig` and `DataSourceExec` supplies maintained
projection, predicate pruning, late materialization, concurrent file/row-group
reads, and custom reader integration. Its default schema adaptation fills missing
columns with nulls. That is not an Iceberg scan: it does not by itself apply the
selected Iceberg snapshot, field-ID interpretation, defaults, partition constants,
or delete membership. A `ParquetAccessPlan` can select ranges, but somebody must
derive them correctly. Replacing Iceberg with this path would require more owned
logic than retaining the native reader. Flat admission already uses DataFusion's
Parquet registration in [write.rs](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/engine/write.rs:98).
[DataFusion 53.1.0 ParquetSource](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-parquet/src/source.rs#L91-L264).

## Guarantees a replacement must preserve

**Selected schema and field identity.** A DocSpec pin includes both snapshot and
schema IDs. Schema-only changes can leave an older data snapshot under a newer
selected schema. `schema::plan_files` assigns the selected current schema and
field IDs to each task. It adds transient `_file` to force Iceberg 0.10.1 through
its ID-aware transformation: the upstream shortcut compares position, datatype,
nullability, and name, but not field-ID metadata. The full path maps by ID and
supplies missing-field defaults. Direct provider adoption would bypass this
workaround and the explicit unsupported-evolution refusals.
[Local interpretation](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/schema.rs:17),
[task override](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/schema.rs:78),
[upstream transformer](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/iceberg/src/arrow/record_batch_transformer.rs#L438-L568).

**Defaults and supplied state.** Iceberg distinguishes defaults used to interpret
old files from defaults used for new writes. DocSpec also retains supplied-state
markers so omitted values stay distinguishable from explicitly supplied nulls.
Keep those columns and the pre-allocation default-expansion refusal through any
projection change. A provider that returns the expected visible values is not
enough if it discards the information needed for later updates.
[Default rules](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/defaults.rs:83),
[Iceberg column projection and defaults](https://iceberg.apache.org/spec/#column-projection).

**Occurrence identity and deletes.** `_file` identifies current physical storage;
DocSpec's stored origin fields survive compaction and replacements. `_pos` and
`_row_id` have reserved definitions in the pinned Iceberg package, but the
inspected reader pipeline materializes `_file`, not a qualified physical-row
position stream. A reserved-field declaration is not proof of usable occurrence
support. The native reader does implement equality and positional deletion
filtering; DocSpec's occurrence path deliberately refuses delete-bearing tasks
because counting surviving rows would not recover original physical positions.
Keep that refusal unless positions are preserved before filtering and the
evidence model is separately qualified.
[Origin implementation](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/identity.rs:128),
[reader pipeline](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/iceberg/src/arrow/reader/pipeline.rs#L214-L389),
[reserved metadata definitions](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/iceberg/src/metadata_columns.rs).

## Recommended order

1. **Expose existing narrow reads and trustworthy statistics.** Replace the
   generic registration boundary with a small `TableProvider::scan` implementation
   that sends requested root fields to the existing native task projection, while
   retaining required origin and supplied-state dependencies. Keep the `_file`
   workaround. Use the selected pin's already-retained row count for unfiltered,
   delete-free data and carry useful statistics into physical planning; do not
   rescan payloads to discover them. Preserve exact versus estimated statistics
   after filters. The intended improvement is less text decoded for key-only and
   narrow operations and better planning information, not a second reader.
   DataFusion documents `TableProvider` as this integration point, and its
   `StreamingTableExec` currently projects only after the partition produces a
   batch. [Provider extension point](https://datafusion.apache.org/library-user-guide/custom-table-providers.html),
   [post-read projection](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/streaming.rs#L262-L284).

2. **Keep filtering behind occurrence and schema correctness.** Column projection
   can preserve every row and its ordinal. Predicate, page, and row-group pruning
   cannot preserve fallback ordinals merely by counting emitted rows. First
   qualify narrow occurrence reads, including stored-origin columns; then consider
   filter pushdown only where the reader preserves identity and no full-coverage
   check depends on consuming every row. Reuse Iceberg predicates and retain a
   residual DataFusion filter for inexact support. Do not enable the currently
   disabled dynamic join filter while its null-safe comparison issue remains;
   see [budget.rs](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/budget.rs:98).

3. **Separate decode batch rows from Parquet row-group sizing when measurements
   justify it.** They presently share `batch_rows`. Parquet 58.4.0 can flush on
   both row count and estimated encoded bytes through
   `set_max_row_group_row_count` and `set_max_row_group_bytes`; the smaller bound
   wins. That permits small decoded batches with larger output row groups using
   existing writer properties. The byte threshold is an encoded-size estimate,
   checked around writes, not a strict allocation cap; an initial batch or one
   huge value can exceed it. Preserve stream/writer admission and metadata closure.
   Measure footer growth, retained size, write time, and read time together before
   choosing a larger group. [Pinned properties](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/file/properties.rs),
   [pinned writer behavior](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/arrow/arrow_writer/mod.rs).

4. **Treat string views as a representation choice, not a memory guarantee.**
   Arrow projection and batch slicing share buffers. `StringViewArray` can avoid
   copying long string contents, but a small slice can retain the complete backing
   buffers. Its `gc()` copies referenced data and should be considered only when
   a substantially smaller result remains live long enough to justify that copy.
   Keep view-compatible operators and writer paths where qualified; avoid casting
   every string to plain UTF-8. Do not introduce per-batch compaction merely to
   make memory accounting look smaller. The existing diagnostic already compares
   view and plain representations; consult its retained results before another
   scan. [Arrow 58.4.0 view behavior](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-array/src/array/byte_view_array.rs#L461-L515),
   [local diagnostic](/Users/mikewolfd/Work/spicy-stack/docspec-rs/examples/comments_scan.rs:23),
   [retained diagnostic interpretation](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/zstd-writer-review.md:25).

5. **Retain current compression and bounded metadata until a selective workload
   demonstrates a better tradeoff.** Zstandard already addresses the measured
   uncompressed-output problem. Chunk statistics with 64-byte truncation and
   disabled offset indexes limit metadata from large text. Page indexes and Bloom
   filters would consume additional storage and metadata; they cannot accelerate
   the full-population path merely by existing. Consider targeted statistics on
   useful key fields after native predicate pushdown works, while preserving
   truncated statistics for full text. The maintained writer already exposes the
   needed properties; a new codec or index framework is unnecessary.
   [Current rationale](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage.rs:202),
   [writer review](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/zstd-writer-review.md:11).

## Resource limits and qualification boundary

The current semaphore reservations and `FairSpillPool` divide an application
budget; they do not account for every allocator, decoder page, string buffer,
writer footer, or queued upstream task. `max_batch_bytes` is checked after native
decode. One very large retained text value can therefore dominate a batch even
with a small row setting. Retain the process-level monitor and distinguish
admission from observed peak memory.
[Budget split](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/budget.rs:83),
[post-decode check](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/storage/schema.rs:106).

Iceberg's maintained builder has separate data-file concurrency, batch rows,
range-fetch concurrency, range coalescing, and footer-prefetch settings. The
current one-file policy is deliberately conservative. Increasing it multiplies
simultaneously active decoder buffers and needs admission per active reader.
Remote-range tuning should wait for the actual object-store workload; it does not
resolve a local join build-side allocation failure.
[Pinned reader controls](https://raw.githubusercontent.com/apache/iceberg-rust/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/iceberg/src/arrow/reader/mod.rs).

Before replacing the adapter, the decisive comparisons are selected-schema
reorder/rename/default behavior; duplicate and null preservation; occurrence
identity before and after rewrites; early drop and failure releasing admission;
and a narrow query proving that unwanted large-text columns are never decoded.
Only then does a frozen retained-workload measurement establish the practical
benefit. No performance gain or replacement-provider qualification is claimed by
this research.
