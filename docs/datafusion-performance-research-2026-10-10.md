# DataFusion and Arrow efficiency decisions

Research date: 2026-10-10. Implementation baseline: `ce3ad6b`; the lockfile
resolves DataFusion 53.1.0, Arrow/Parquet 58.4.0 and Iceberg 0.10.1.

**The documented changes now pass the full local performance gate:** native
planning statistics, larger flat-export row groups and four oracle threads
complete three fresh 10-million-Comments runs in 311.61–392.03 seconds, including
exact independent readback. See [the current result](STATUS.md#full-local-performance-acceptance).
Keep maintained execution, decoding and writing. The remaining reader opportunity
is to push column selection down so narrow queries avoid decoding unused values.

The initial diagnosis and priorities below are retained as research history.
The implementation follow-up records what was subsequently changed and measured.

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
