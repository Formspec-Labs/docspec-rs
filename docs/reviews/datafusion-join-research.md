# DataFusion join research for the failed full Comments collection

**Recommendation: retain the compatible engine version, connect trusted native
scan statistics to the maintained physical planner, and verify the resulting
build side. Keep sort-merge join as the maintained spilling alternative.**
Neither a dependency upgrade nor a larger scratch allowance by itself fixes the
observed hash-build failure. No available measurement yet establishes which
alternative is fastest for the complete frozen workload.

Official documentation and tagged Apache source reviewed on 2026-10-10.
Repository baseline: `ce3ad6b88ed748bdcc56cf33ce0da43a78ac2752`;
`Cargo.toml:14-25` pins DataFusion 53.1.0 and Iceberg 0.10.1 with Arrow/Parquet 58.
The companion `comments-join-memory-diagnosis.md` records the actual failure,
local call trace and unresolved plan identity. This research performed no
implementation, builds, tests, benchmarks or payload reads/hashes.

## What the compatible version actually does

DataFusion 53.1.0's physical `JoinSelection` reads
`ExecutionPlan::partition_statistics(None)` from both children. It compares byte
sizes first, then row counts. Without either comparison, it keeps the build side
and resolves Auto to partitioned hash join. DocSpec's `StreamingTable` has no
statistics input and its physical scan inherits unknown statistics. These are
verified version-specific behaviors, not assumptions from current documentation.
[Pinned join selection](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-optimizer/src/join_selection.rs),
[pinned streaming provider](https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog/src/streaming.rs).

The failed `HashJoinInput[6]` belongs to the partitioned left-build path. The
pinned implementation retains the entire build stream in memory and returns a
reservation error when growth fails. Its misleading nearby comment about deciding
whether to spill is not a spill implementation. A configured spill directory and
`FairSpillPool` do not change that code path.
[Pinned hash-join implementation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/joins/hash_join/exec.rs).

The resource requirement is the retained, decoded build data plus join state and
concurrent operators, not the source's compressed Parquet size. Native row counts
are already checked before pinning (`src/storage.rs:584-606`) and are the simplest
credible planning input. Row count is a useful fallback, not a general estimate
of variable-width or nested memory. Keep decoded byte sizes absent unless the
source can justify them; avoid attaching compressed sizes as exact memory sizes.

## Maintained options and tradeoffs

| Option | Verified support | Applicability and recommendation |
| --- | --- | --- |
| `DataSourceExec` with a native `DataSource` | The pinned `DataSource` interface includes `partition_statistics`, stream opening, column-selection and fetch hooks; `DataSourceExec` delegates statistics and handles physical-plan machinery. | Preferred integration boundary if DocSpec's occurrence reader must remain. Move the existing source adapter to this interface; reuse its reader/admission and supply exact snapshot/subset counts. This is a small source adapter, not a new execution engine. |
| `FileScanConfigBuilder::with_statistics` with a maintained file source | The pinned builder accepts statistics and projects them for output columns. Native Parquet support can avoid a generic streaming wrapper. | Prefer this if the reader work can preserve exact occurrence file/row identity, native field IDs/schema interpretation, file admission and batch bounds. It is not a blind replacement for the current occurrence scan. |
| `StreamingTable` plus a statistics wrapper | The pinned streaming provider and physical scan expose no statistics builder. An execution-plan wrapper could supply statistics. | A narrow fallback when it removes less code than changing the source adapter. It must preserve physical rewrite/projection/limit behavior; adding only `TableProvider::statistics` misses the actual optimizer call. Do not use upstream's synthetic test-only `StatisticsExec`. |
| `datafusion.optimizer.prefer_hash_join=false` | With multiple target partitions and join repartitioning enabled, pinned physical planning selects `SortMergeJoinExec` for this equality-join path. Its buffered equal-key groups can spill. | Smallest configuration-only alternative. Qualify under the existing scratch/time limits; sorting both inputs can add CPU, I/O and temporary bytes. It is a controlled comparison or fallback, not an evidence-free global default. |
| `iceberg-datafusion` 0.10.1 | Version-compatible with DataFusion 53.1.0/Arrow 58; its static provider can select an exact snapshot. | Maintained but not a direct cure here: its physical scan also supplies no statistics override, and it does not provide DocSpec's occurrence identity or shared reader admission. |

Sources: [pinned DataSource/DataSourceExec](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource/src/source.rs),
[pinned FileScanConfig](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource/src/file_scan_config.rs),
[pinned physical planner](https://github.com/apache/datafusion/blob/53.1.0/datafusion/core/src/physical_planner.rs),
[pinned sort-merge implementation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs),
[Iceberg dependency versions](https://github.com/apache/iceberg-rust/blob/v0.10.1/Cargo.toml),
[Iceberg static snapshot provider](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/integrations/datafusion/src/table/mod.rs),
[Iceberg physical scan](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/integrations/datafusion/src/physical_plan/scan.rs).

The sort-merge implementation supports the inner, left and full joins used by
DocSpec and the semi joins used by its cardinality probes. Its streaming side
does not itself spill; buffering, sorts, output expansion and reader allocations
still consume resources. Thus it avoids whole-build hash retention without
promising every join can complete in the budget. The existing null-safe key
semantics and dynamic-filter workaround remain mandatory.

These alternatives do not justify a custom spilling hash join, materializing a
`MemTable`, a manual hash map, a hand-written join swap optimizer, or a separate
query engine. A forced SQL reversal also duplicates maintained swap logic and
can harm workloads whose original left input is smaller. None is the first
correction for this missing-statistics boundary.

## Newer DataFusion is a separate compatibility decision

The current published API documentation identifies DataFusion 55.2.0. Its tagged
workspace uses Arrow/Parquet 59.2.0, while this repository and the compatible
Iceberg adapter use Arrow/Parquet 58. An upgrade crosses a shared Rust type and
dependency boundary; changing the DataFusion version alone is not a qualified
replacement.
[Current published DataFusion API](https://docs.rs/datafusion/latest/datafusion/),
[55.2.0 dependency declarations](https://github.com/apache/datafusion/blob/55.2.0/Cargo.toml).

Version 55.2.0 adds a `StatisticsRegistry` with pluggable statistics providers and
maintained propagation through common operators. Its join-selection code consults
that registry. This can reduce the need for a physical wrapper in a future
upgrade, but a native-source-specific provider still has to associate trusted
counts with the correct plan nodes. The streaming scan itself still has no
statistics hook. This is an integration option, not automatic discovery of the
DocSpec pin's row count.
[55.2.0 statistics registry](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/operator_statistics/mod.rs),
[55.2.0 join selection](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-optimizer/src/join_selection.rs),
[55.2.0 streaming scan](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/streaming.rs).

The 55.2.0 hash build still grows a reservation for each retained build batch and
returns failure when that reservation cannot grow. Upstream's hash-spilling epic
is an open implementation proposal, not a supported feature demonstrated in this
tag. Do not select an upgrade on an assumption that hash spilling is now present.
[55.2.0 hash build](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/joins/hash_join/exec.rs),
[upstream hash-spilling work](https://github.com/apache/datafusion/issues/24768).

Current configuration documentation describes join preferences, partition counts
and memory tuning, but the tagged source above determines applicability to this
binary. Ballista's tuning guide contains a generic spill statement alongside an
explicit no-spill hash strategy; the tagged implementation resolves that ambiguity
for this engine. Ballista documents adaptive, distributed planning; introducing Ballista
would broaden this local-engine problem and is unnecessary to obtain the
compatible sort-merge alternative.
[DataFusion configuration documentation](https://datafusion.apache.org/user-guide/configs.html),
[Ballista tuning documentation](https://datafusion.apache.org/ballista/user-guide/tuning-guide.html).

## Prioritized next work

1. **Capture the real plans before choosing the fix.** Distinguish the cardinality
   probe from the main join; record physical left/right children, column sets,
   statistics, partition mode and exact native input pins. The existing failure
   log identifies a partitioned build, not its named source. Use maintained
   physical-plan creation with execution disabled; another provider over plain
   Parquet would be a different experiment.
2. **Repair the existing native source boundary.** Choose the compatible
   `DataSourceExec`/file-source route that preserves the reader's required
   behavior with the least code. Make exact counts visible to physical planning
   for both full-table and captured-file paths. Keep the existing optimizer and
   allow it to map inner/left/full and semi join sides. Evaluate the resulting
   physical plan; do not equate the presence of a row count with a successful
   memory bound.
3. **Validate semantics and compare only justified alternatives.** Cover all
   declared join kinds, both null modes, composite keys, empty inputs, duplicate
   occurrences, unmatched duplicates and both asymmetric input directions.
   Preserve matched-key-only cardinality checks. Consider the maintained
   sort-merge setting if the corrected build still cannot fit or as an explicit
   one-factor comparison under the same limits.
4. **Collect acceptance evidence anew.** Retain the failed attempt; capture the
   exact new executable; run the unchanged full protocol and independent oracle.
   Report complete elapsed time, observed RSS, scratch, retained bytes and exact
   output equivalence. Diagnostic plans or smaller fixtures cannot establish the
   frozen full-workload acceptance result.

The likely benefit of statistics is a smaller retained build; the likely cost of
sort-merge is sorting and scratch I/O. Those are code-grounded hypotheses. The
fastest valid complete pipeline remains an empirical question. This research
does not change the workload, limits, repetition rule, or correctness criteria.

## Focused candidate: prove right-key uniqueness before the many-to-one probe

**Static verdict: semantics-preserving, with a conditional performance benefit.**
For `ManyToOne` with cardinality enforcement enabled, first ask the maintained
SQL engine for `SELECT 1 FROM join_right r GROUP BY <right keys> HAVING COUNT(*) > 1
LIMIT 1`. If no row is returned, skip only the existing right-side cardinality
probe. If a duplicate is found, execute that probe unchanged. Keep `OneToOne`
separate, and preserve the existing `ManyToMany` and enforcement-disabled paths.

The proof follows directly from `src/engine/refresh.rs:618-660`: the current left
semi join retains each matching right occurrence once. Its result is a subset
of the right input. Grouping this subset by the same complete key tuple cannot
produce a count greater than the corresponding group in the whole right input.
If every whole-input count is at most one, the current probe cannot reject.
Use `COUNT(*)`, the same key expressions and the same pinned provider; do not
deduplicate occurrences or treat a discovered duplicate as a rejection.

This proof covers both null modes and inner/left/full output joins. Under ordinary
equality, duplicate null-containing keys may cause an unnecessary fallback but
cannot cause a new semantic rejection. Under null-safe equality, the unchanged
fallback still decides whether those keys have matching partners. Unmatched
duplicates remain accepted. An empty right input safely takes the fast path.

When right keys are globally unique, cardinality validation needs only the right
scan/grouping and avoids scanning Comments for matching-key existence. In that
case its input work changes from both populations to the right population alone;
the final result join still reads both. Duplicate-present cases pay an extra
right grouping pass before the existing validation. That aggregation uses memory
and can encounter resource limits; `LIMIT 1` does not guarantee early completion.
Actual Dockets uniqueness, elapsed-time benefit and budget behavior remain
unmeasured. This is a small query-level candidate using maintained operators,
independent of the physical-statistics correction and not a substitute for it.

Focused validation should compare the existing and candidate cardinality decision
for unique keys, matched/unmatched duplicates, composite partial-null keys under
both modes, and empty inputs. Planning evidence should show no left scan on the
successful uniqueness branch. No corpus scans or implementation changes were
performed for this assessment.
