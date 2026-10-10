# DataFusion and Arrow efficiency decisions

Research date: 2026-10-10. Initial implementation baseline: `ce3ad6b`;
follow-up inspected `9c818a0` and the current reader/writer source. The lockfile
resolves DataFusion 53.1.0, Arrow/Parquet 58.4.0, Iceberg 0.10.1 and Tokio 1.53.2.

**The documented changes now pass the full local performance gate:** native
planning statistics, larger flat-export row groups and four oracle threads
complete three fresh 10-million-Comments runs in 311.61–392.03 seconds, including
exact independent readback. See [the current result](STATUS.md#full-local-performance-acceptance).
Keep maintained execution, decoding and writing. The remaining reader opportunity
is to push column selection down so narrow queries avoid decoding unused values.

## Current decisions and next work

**Keep the current engine family. Connect its existing optimizations to our native
reader, then reduce repeated work and output overhead.** Official documentation
and independent source investigations support these changes; they do not provide
a measured speedup for DocSpec. This follow-up ran no new performance experiment
and read no corpus payloads.

The [accepted measurements](../experiments/comments-e2e/acceptance-oracle-threads4-20261010.json)
separate the costs. Engine command stages total 127.42–129.75 seconds; the exact
independent checker takes 180.92–261.26 seconds, or 58–67% of complete elapsed time.
The checker is qualification work. Keep ordinary creation and readback checks
bounded, retain their results, and reserve complete reconstruction for acceptance
or a specific investigation, as [AGENTS](../AGENTS.md) already requires.

| Order | Concrete change | User value | Acceptance evidence |
| --- | --- | --- | --- |
| First | Push SQL column selection through `NativeTable::scan` into Iceberg field-ID selection. Extend the existing captured-file reader to support requested occurrence columns. | Key-only joins, summaries and reads stop decoding unrelated long text and nested values. Rows may still be scanned; bytes decoded per row should fall. | Same values, defaults, types and physical identities; an instrumented narrow read proves unused value columns are not decoded. Include empty/default-only projections and reordered fields. |
| Next | Share one captured file-task inventory for the same immutable selection within an operation. | Repeated queries avoid repeated metadata traversal as retained history grows. Reuse the existing bounded `Arc<OccurrenceScan>`; keep values streamed. | Count inventory/planning work and actual metadata requests as history grows; confirm exact selection, admission and reader lifetime. |
| Next | Check constrained-side duplicate keys before consulting the other side of a join. Reuse the existing matched-key check when duplicates exist. | A unique Dockets key set can establish many-to-one cardinality without a second scan of all Comments. The main join still reads its required inputs. | Compare matched/unmatched duplicates, composite and null-safe keys; measure both unique and duplicate-present inputs. The latter may pay an extra small-side grouping pass. |
| Next | Separate native Parquet row-group limits from execution batch rows. Keep Iceberg's maintained rolling writer. | Reduce footer work and potentially compressed output size without enlarging processing batches. | Compare write plus subsequent read time, writer/process memory, serialized metadata, peak disk and retained bytes under the same limits. |
| As needed | Make CLI runtime configuration follow the selected resource profile; keep blocking work bounded. | Avoid misleading thread settings and preserve responsiveness under simultaneous operations. | Verify actual worker settings, cancellation/permit lifetime and mixed-operation memory. Do not describe partition count as a process-wide thread cap. |
| When measured | Reuse immutable metadata and enable safe selective pruning through the adapter that actually reads the files. | Reduce repeated footer/range requests for small or remote reads. | Prove cache hits/request reduction, byte bounds, exact snapshot/schema interpretation and unchanged occurrence identities. |

### Column selection is the smallest useful reader change

`NativeTable::scan` currently puts `ProjectionExec` above a full-width native
scan, and `try_swapping_with_projection` declines integration. Iceberg already
accepts selected field IDs. DataFusion's maintained data-source interfaces supply
the planner integration; the work belongs in our thin adapter. See
[query.rs](../src/query.rs) and the [official pruning explanation](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/).

This needs more than forwarding an index vector. Our current
`OccurrenceScan::read_projected_file` rejects projection for located reads.
Extend it to decode requested value fields plus any stored origin fields needed
to produce requested occurrence IDs. Keep the selected schema, transient native
file-column workaround and missing-field defaults. For files whose row identities
come from physical offsets, assign positions over the complete unfiltered stream
before applying any row filter. Output projection can then discard internal
columns. See [occurrences.rs](../src/storage/occurrences.rs).

Table-backed occurrence reads currently rebuild an inventory through
`snapshot_files`, which visits snapshots retained in the selected metadata file.
The join's cardinality probe and main query execute separately. Share captured
tasks for the same selection within that operation instead of repeating this
history traversal. Keep the complete retention inventory at the lifecycle
boundaries that require it. Iceberg already caches some table metadata, so fewer
planning passes do not automatically imply the same reduction in storage reads.
[Local inventory](../src/storage.rs), [join execution](../src/engine/refresh.rs),
[pinned Iceberg scan context](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/scan/context.rs).

Also qualify zero-column and defaults-only reads: the pinned Iceberg projection
builder can fall back to all columns when no selected physical field exists.
An empty logical projection therefore does not establish cheap `COUNT(*)`.
Keep the existing aggregate-statistics correctness guard until a maintained fix
passes the regression. Predicate pruning is a separate step: missing physical
columns can be treated as null before initial defaults are reconstructed, so a
residual filter cannot recover rows already discarded. See the pinned
[projection](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/projection.rs),
[predicate handling](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/predicate_visitor.rs)
and [reader pipeline](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/pipeline.rs).

Keep row filtering in DataFusion unless the native reader can avoid I/O or
decoding. Reimplementing the same filter after decoding adds owned code without
removing that work. The official [table-provider guide](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
recommends using the maintained vectorized filter when source pushdown offers no
such advantage. Use physical plans to establish where selection and filtering
actually happen; their presence in SQL does not establish reduced reads. This
guide describes current upstream interfaces; implementation must use our pinned
versions above.

### Finish the native writer improvement without inventing a writer

The flat export already lets ArrowWriter combine small batches and flushes on
estimated writer memory. The native writer still sets its row-group row limit to
`batch_rows`, normally 4,096. These are different paths; the export result does
not establish an improved native layout. See [storage.rs](../src/storage.rs) and
[export.rs](../src/engine/export.rs).

Parquet 58.4 supports separate row and encoded-byte group thresholds, which can
pass through Iceberg's existing `ParquetWriterBuilder`. Estimated encoded
row-group size and estimated writer memory remain different measures. Preserve
or replace the current conservative admission and footer accounting with equally
effective bounds before raising group limits. Group size, rolling file size and
execution batch size serve different purposes. Changing only the row cap would still leave native metadata
accounting charging groups per input batch and closing writers early. Account for
the new layout too. Larger files can also increase bytes rewritten for a small
update, so include a fixed-size update in the comparison.
[Writer properties](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/file/properties.rs),
[ArrowWriter memory behavior](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/arrow/arrow_writer/mod.rs),
[Iceberg writer](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/iceberg/src/writer/file_writer/parquet_writer.rs).

### Keep the efficient paths already present

The engine already streams batches, shares a `RuntimeEnv` across query contexts,
uses a shared operator-memory pool and scratch manager, admits native readers and
writers, compresses output with Zstandard, reuses completed phases and exposes
native row statistics for join planning. Preserve those paths. Arrow array-handle
clones are not payload copies; view arrays can share long strings but can also
keep large backing buffers alive. Copy only where measured lifetime memory
justifies it. [Pinned Arrow view implementation](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/arrow-array/src/array/byte_view_array.rs).

`Limits.compute_threads` sets DataFusion partitions and blocking-job admission;
the CLI independently starts 12 Tokio worker threads. Tokio's blocking pool is
separate, and running blocking jobs are not canceled by dropping their caller.
The current worker-owned permits are therefore useful. Align configuration before
adding concurrency, without building another scheduler. [Budget](../src/budget.rs),
[CLI](../src/main.rs), [Tokio blocking-task guidance](https://docs.rs/tokio/1.53.2/tokio/task/fn.spawn_blocking.html).

DataFusion's `FairSpillPool` can refuse memory growth earlier, prompting operators
that support spilling to spill. It is not an allocator-level RSS limit. Keep
observed process measurements.
DataFusion also supports spill compression, but it would affect DataFusion
scratch, not the separate DuckDB checker responsible for the observed large spill.
Change it only for a measured DataFusion spill workload.
[Pinned memory pools](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/execution/src/memory_pool/pool.rs),
[pinned spill settings](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/common/src/config.rs).

### Storage and upgrade decisions

The qualified flow retains about 10.6 GB from 2.74 GB of inputs: 3.86×, including
admitted originals, native results, evidence and flat releases. Sampled peak
additional logical space reaches 42.93 GB with scratch; sampling makes this a
lower bound. These are different retained products, not proof that a single
output inflated by that factor. Attribute improvements to
their actual copy or phase. Sharing unchanged files and avoiding redundant
materializations should precede another cache or codec. Keep required original
bytes, releases and recovery evidence; measure coexistence during updates and
compaction as well as final size.

DataFusion's metadata cache belongs to its own reader paths; configuring it does
not automatically cache the Iceberg decoder hidden behind our custom source.
Establish which maintained reader owns a repeated request before introducing any
cache. [Pinned runtime/cache interface](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/execution/src/runtime_env.rs).

Newer upstream releases offer useful scan improvements, but a version upgrade
must move the compatible dependency family together. The inspected DataFusion
55.2 tag uses Arrow/Parquet 59.2; our Iceberg integration uses 58. It is not a
one-line upgrade. Keep the regression guards for filtered counts and null-safe
dynamic joins until the replacement passes them. Custom joins, blanket indexes,
another SQL backend and distributed scheduling have no measured justification
for this workload. [DataFusion 55 improvements](https://datafusion.apache.org/blog/2026/08/25/datafusion-55.0.0/),
[55.2 dependency manifest](https://raw.githubusercontent.com/apache/datafusion/55.2.0/Cargo.toml).

Qualify one change at a time against retained inputs. First establish exact
behavior and eliminated work with bounded fixtures and physical plans; then use
the existing frozen performance workflow when a timing or storage claim needs
measurement. Preserve the accepted workload and resource ceilings.

The initial diagnosis and priorities below remain research history. The
implementation follow-up records what was subsequently changed and measured.

## What the failed run establishes

The [first full acceptance attempt](../experiments/comments-e2e/acceptance-20261010.json)
completed Comments admission/shaping in 30.50 seconds, then failed inside the
Comments-to-Dockets stage when a partitioned hash build could not grow its memory
reservation. The interrupted run lasted 48.76 seconds. It does not establish
which query within that stage failed: cardinality validation executes before the
main join, and the log does not distinguish them.

Sampled aggregate resident memory peaked at 9.54 GB. Retained logical storage at
failure was 5.48 GB against 2.74 GB of input, almost entirely original admitted
bytes plus native data. Sampled scratch was zero. These are measurements of the
failed attempt; the full flow and final storage amplification remain unqualified.

## Changes in priority order

| Priority | Change to qualify | User value and meaningful cost |
| --- | --- | --- |
| First | Supply native scan statistics to the physical planner. | Let DataFusion choose a smaller hash-build input, avoiding memory proportional to the wrong full-width table. |
| First | Push requested columns into Iceberg's reader. | Key-only checks, summaries and narrow reads avoid decoding unused long text and nested fields. |
| Next | Remove avoidable population scans from cardinality validation. | A small-side uniqueness proof can avoid scanning every Comment solely to establish many-to-one cardinality. Preserve matched-key-only semantics. |
| Then | Separate processing batch size from output row-group size. | Potentially reduce footer overhead and retained bytes while keeping small execution batches. Measure the memory tradeoff. |
| When demonstrated | Tune views, read concurrency, predicate pruning and remote ranges. | Address measured copying, I/O or request overhead without adding speculative machinery. |

### Native scan statistics and joins

Our generic `StreamingTable` does not expose native row counts. DataFusion
53.1.0's physical join optimizer consults child-plan statistics, using bytes when
available and otherwise rows. If both are unknown, it leaves the build side
unchanged. Supplying only `TableProvider::statistics` would miss this physical
planning path. [Pinned join selection](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-optimizer/src/join_selection.rs).

Use the maintained `DataSourceExec` with a thin native `DataSource` and
`TableProvider`, reusing our reader and admission rules. This interface already
supports physical statistics and column-selection integration. Use counts from
the exact selected pin or captured file subset; keep unknown decoded byte sizes
unknown. Compressed Parquet bytes are not hash-build memory. Mark statistics
appropriately after filters or limits, and do not scan payloads to obtain planning
estimates. [Pinned DataSource interface](https://docs.rs/datafusion-datasource/53.1.0/src/datafusion_datasource/source.rs.html).

Hash-join build memory includes retained decoded rows and join state. Its pinned
implementation fails on a refused reservation; configuring a spill directory
does not give this operator a spill algorithm. A supported sort-merge join is a
candidate for inputs that still cannot fit, with sorting and scratch costs to
measure. Do not select it globally without evidence. [Pinned hash build](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/joins/hash_join/exec.rs),
[pinned sort-merge implementation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs).

Before implementing the correction, capture physical plans for both the
cardinality probe and the main join using the existing providers, exact retained
pins and session settings. Plan creation need not execute the payload scan.
Check both asymmetric input directions; hard-coding Dockets as the build side
would solve only one arrangement.

### Read only needed columns

In [query.rs](../src/query.rs), SQL projection currently happens after
`NativePartition` has emitted a full-width batch. Explicit captured-file value
reads already have native projection support; use that same field-ID-aware path
for table and occurrence queries. The goal is work proportional to selected
columns rather than the complete row width. The full-value join still needs its
requested payload; key-only operations do not. DataFusion's Parquet guidance
explains why avoiding unused column reads is valuable before decoding begins.
[Official pruning explanation](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/).

Preserve selected schema IDs, missing-field defaults, required origin fields and
supplied-state markers. Column selection preserves row positions; filtering does
not. Do not enable row-group, page or predicate pruning for occurrence reads until
the reader preserves physical positions before filtering. Keep the existing
null-safe dynamic-filter workaround.

The compatible `iceberg-datafusion` 0.10.1 provider accepts projection, but its
physical scan also lacks statistics, omits our resource controls, and bypasses
our selected-schema workaround. Replacing it wholesale would lose required
behavior without fixing the join planner. Keep the maintained Iceberg decoder
behind our thin integration. [Published provider scan](https://github.com/apache/iceberg-rust/blob/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/integrations/datafusion/src/physical_plan/scan.rs).

### Cardinality work

The current many-to-one check semi-joins the right keys against the full left
input before checking duplicates. A candidate optimization is to check duplicate
right keys first. If none exist, no left scan is needed to prove cardinality. If
duplicates exist, keep the existing matching-key validation: duplicates without
matches must remain legal. This is an application-level inference from
[join_plan](../src/engine/refresh.rs), not an upstream performance claim. Qualify
null modes, composite keys, matched and unmatched duplicates; apply the same
reasoning separately to each constrained side of a one-to-one join.
The duplicate-present case adds a right-side grouping pass before the existing
check. Retain that tradeoff in the experiment; actual corpus uniqueness and a
speed improvement have not been measured.

### Output size and Arrow memory

The native writer currently ties row-group rows to the 4,096-row processing
batch. Parquet 58.4 supports separate row and estimated encoded-byte thresholds.
Larger groups may reduce metadata and improve compression, but writer memory and
encoded size are different measures. Preserve admission and footer bounds; a byte
threshold cannot replace them. Compare write time, subsequent read time, peak
memory and retained storage together. Zstandard already addresses the measured
uncompressed-output problem. [Pinned Arrow writer documentation](https://docs.rs/parquet/58.4.0/parquet/arrow/arrow_writer/struct.ArrowWriter.html),
[pinned writer properties](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/file/properties.rs).

Keep Arrow string views through compatible processing and writing so operations
can share long-text buffers. A small result can still retain a large backing
buffer; copying it is justified only when the reduced lifetime memory outweighs
that work. Do not add routine per-batch copying, string conversion or compaction.
[Arrow view behavior](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-array/src/array/byte_view_array.rs#L461-L515).

## Dependency upgrades and execution limits

DataFusion 55 adds useful Parquet physical-row/file columns and better nested-leaf
pruning. These may eventually remove adapter work, but native Iceberg schema and
occurrence guarantees still need qualification. The 55.2.0 dependency family uses
Arrow/Parquet 59.2, while our Iceberg family uses 58; changing one crate is not a
compatible drop-in upgrade. The newer hash build also lacks the assumed spill
cure. [55 release notes](https://datafusion.apache.org/blog/2026/08/25/datafusion-55.0.0/),
[55.2 dependencies](https://raw.githubusercontent.com/apache/datafusion/55.2.0/Cargo.toml),
[55.2 hash implementation](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/joins/hash_join/exec.rs).

Increasing partitions or reader concurrency is not automatically faster: more
simultaneous buffers compete for the same budget. Keep the current limits while
fixing wasted work. The operator pool and admission permits are not a complete
allocator or process-memory bound, so retain observed process memory and disk
measurements. Distributed scheduling, custom joins, a second SQL store, allocator
changes and blanket indexes have no demonstrated role in this failure.

## Evidence needed to accept an improvement

Reuse retained snapshots for planning and focused diagnostics. Validate projected
schema/default/origin behavior, nulls and duplicates, early-drop admission release,
and a narrow read that proves unused payload columns are not decoded. Confirm the
actual physical build input and statistics without adding a diagnostic to every
ordinary read.

Then use the unchanged full acceptance protocol with a newly captured executable.
Keep the failed baseline and report elapsed time, peak resident memory, scratch,
peak additional disk and retained storage. A resumed diagnostic cannot replace
the required fresh runs. The first decision is whether the correction completes
the flow within its limits; later tuning must demonstrate a complete-path benefit.

Supporting independent research: [join diagnosis](reviews/comments-join-memory-diagnosis.md),
[join alternatives](reviews/datafusion-join-research.md), and
[reader/writer investigation](reviews/native-reader-efficiency-research.md).
This research used official documentation, tagged source and retained evidence;
it did not rerun the corpus, change Rust code or claim a new performance result.

## Implementation follow-up

The initial research above is retained as written. Subsequent qualification is
tracked in [STATUS](STATUS.md#native-query-planning). Native physical statistics
plus a required DataFusion correctness guard now complete the previously failing
join; the next full attempt reached a flat-export footer-accounting refusal.
That observation promoted row-group layout ahead of further scan work. The
[frozen layout comparison](../experiments/comments-e2e/export-layout-plan.md)
keeps the same reader, workload and resource limits. The implemented flat writer
now finishes the 10-million-row export in 32.67 seconds, with 10 row groups and
a 30,192-byte footer. Processing, evidence, summaries and both exports finished
in about 135 seconds on the shared workstation. These are retained observations,
not an isolated speedup comparison.

The next failure moved to the independent exact checker: DuckDB exhausted its
32 GiB spill allowance while grouping complete joined values. The full attempt
still failed. [DuckDB/Arrow follow-up research](reviews/oracle-efficiency-research.md)
prioritizes fewer active threads, then exact dictionary encoding of repeated
contributor paths if needed. Both use maintained engine behavior; neither changes
the required equality checks. The [frozen diagnostic](../experiments/comments-e2e/oracle-efficiency-plan.md)
reuses completed artifacts instead of repeating the engine. Native projection
and cardinality scan elimination remain candidates, not implemented improvements.
No complete-path performance acceptance follows from these partial results.

The complete retained-output oracle subsequently passed at four threads in
164.19 seconds, with sampled RSS 21.74 GiB and scratch 30.44 GiB. Keep that smallest
setting change and defer dictionary/keyed alternatives. Exact checks are intact;
this does not qualify fresh-run performance. See the
[diagnostic receipt](../experiments/comments-e2e/oracle-threads4-20261010.json).

The subsequent [fresh full acceptance series](../experiments/comments-e2e/acceptance-oracle-threads4-20261010.json)
passed all three repetitions in **311.61, 346.31 and 392.03 seconds**, including
exact independent readback. Sampled tree RSS remained below 24 GiB and scratch
below 32 GiB. Retained data was about 10.6 GB per run, or 3.86× frozen inputs;
peak additional logical disk reached 42.93 GB. These observations qualify the
defined local full workload. They do not establish an isolated speedup, remote
performance or complete lifecycle coverage. Keep the maintained engine/writer
changes and smaller oracle concurrency; further tuning should target measured
I/O, duplicate storage and scratch costs.
