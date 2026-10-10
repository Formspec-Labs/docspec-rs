# Native query statistics: static design review

**Verdict: RECONSIDER the claim that `Inexact` alone preserves execution. Keep
the thin native provider and maintained join optimizer; qualify the limit/count
case below before enabling row hints in general query contexts.**

Reviewed 2026-10-10 against `775fd75`, with query implementation baseline
`ce3ad6b`. The parent is implementing a revised design concurrently; this report
reviews the design, not that uncommitted patch. No Cargo commands, tests,
benchmarks, or corpus reads were run. Dependency citations below name locally
available crates under the Cargo registry; their versions are pinned in
`Cargo.toml:19-24`.

## Finding: an inexact count can become an incorrect exact zero

Severity: **BLOCKER for the original safety argument**. Category: correctness.

DataFusion 53.1.0's `Statistics::with_fetch` treats both `Exact(n)` and
`Inexact(n)` as an upper bound and returns `Exact(0)` when `n <= skip`
(`datafusion-common-53.1.0/src/stats.rs:452-464`). `GlobalLimitExec` delegates to
that method (`datafusion-physical-plan-53.1.0/src/limit.rs:227-230`). The public
meaning of `Inexact` is an estimate, not an upper bound (`stats.rs:30-37`).

This is relevant even when the source supplies its correct row count:

1. `FilterExec` applies its default 20% selectivity to predicates unsupported by
   interval analysis (`datafusion-physical-plan-53.1.0/src/filter.rs:72,310-324`).
   Scalar function expressions are unsupported
   (`datafusion-physical-expr-53.1.0/src/intervals/utils.rs:38-61`).
2. `GlobalLimitExec` can turn the reduced estimate into exact zero.
3. `Count::value_from_stats` accepts that exact count for `COUNT(*)`
   (`datafusion-functions-aggregate-53.1.0/src/count.rs:367-390`).
4. `AggregateStatistics` replaces the aggregate and its input with a literal
   (`datafusion-physical-optimizer-53.1.0/src/aggregate_statistics.rs:55-87`).

The rule order does not prevent this path. The physical planner creates partial
and final aggregates (`datafusion-53.1.0/src/physical_planner.rs:1046-1084`) and a
global limit (`physical_planner.rs:1204-1232`). `AggregateStatistics` runs before
join selection, aggregation combination, and physical limit pushdown
(`datafusion-physical-optimizer-53.1.0/src/optimizer.rs:91-118,140-143`). Its
aggregate traversal selects the partial aggregate and then reads statistics from
that aggregate's input, including the limit
(`aggregate_statistics.rs:57,117-138`).

Proposed regression using the existing fixture at baseline `src/query.rs:237-282`
(`1, 1, NULL, 2, 3, 4, 5`) and source hint `Inexact(7)`:

```sql
SELECT COUNT(*)
FROM (
    SELECT id
    FROM retained
    WHERE length(CAST(id AS VARCHAR)) > 0
    LIMIT 10 OFFSET 2
)
```

The correct result is **4**. The source-derived failure prediction is
`Inexact(7) -> Filter Inexact(2) -> GlobalLimit Exact(0) -> literal COUNT 0`.
Expected failing optimized shape: a literal projection over a placeholder row,
with the native scan removed. This SQL prediction has **not** been executed.
The issue can also affect other statistics-bearing providers; adding native
statistics exposes a path the existing unknown statistics avoid.

**Recommendation: RESHAPE.** Run this precise regression before accepting the
change. If reproduced, remove only the `aggregate_statistics` physical rule from
the shared session's maintained default rule list using `SessionStateBuilder`;
retain join selection and the other maintained rules. The builder supports rule
replacement (`datafusion-53.1.0/src/execution/session_state.rs:1231-1236`). Do not
build a custom join optimizer or claim a runtime bug has been reproduced from
this static review alone. A qualified upstream fix could replace this guard.

The focused consumer trace supports that narrow guard. The other direct physical
row-statistics consumers are `JoinSelection`, which chooses legal join sides and
modes (`join_selection.rs:59-100`), and `EnforceDistribution`, which decides
optional round-robin work while preserving required hash alignment
(`enforce_distribution.rs:1091-1139`). `OutputRequirements` forwards statistics.
No statistics-based empty-result or join-elimination rule was found in the
pinned physical optimizer; the logical optimizer does not consume these physical
statistics. `GlobalLimitExec` executes the child through `LimitStream`, without
using estimated rows to truncate execution (`limit.rs:190-220`). This makes
removing `aggregate_statistics` sufficient for the traced false-zero execution
path, although inaccurate estimates can still select slower or larger plans.
This is a scoped source assessment, not a general correctness proof of DataFusion.

## Provenance and behavior assessment

| Question | Evidence and decision |
| --- | --- |
| Is selected snapshot metadata the right snapshot? | Yes. `open_pin` verifies the metadata object and exact snapshot/schema binding (`src/storage.rs:677-695`). It does not compare the optional summary count with live file counts. |
| Is any parseable `total-records` qualified as an upper bound? | No such check exists here. Native append computes summaries, but parsing proves only representation. Missing/malformed/overflowing values should be absent; a stale but parseable number remains unqualified. The revised explicit `Option<u64>` from verified `TablePin.rows` is preferable. |
| Where is the verified count established? | `pin` sums live native `record_count` with checked addition and compares it with prepared rows (`src/storage.rs:591-606`). Reuse that result without rescanning manifests. Preserve the selected table/count pairing; arbitrary or modified tables receive no hint. |
| Do partial deletes invalidate the count? | Current pinning refuses native delete files (`src/storage.rs:533-541`). Replacement metadata drops old snapshots, and output is appended as a new candidate (`src/storage/tables.rs:51-78`; `src/storage.rs:354-368`). Do not extend this count argument to future delete-file support without qualification. |
| Are captured-file counts usable? | Yes, for the selected subset. Captured tasks correspond to complete, unfiltered Parquet files and refuse deletes (`src/storage/occurrences.rs:369-383`). Sum only matching selected file records with checked conversion/addition. Unknown paths must retain their existing refusal, not become a zero-row success (`occurrences.rs:318-320`). |
| Does `Inexact` preserve a plain full `COUNT(*)` scan? | At the source, yes: `Count::value_from_stats` requires `Exact`. It is not an end-to-end guarantee after other operators. `LIMIT 0`, genuinely exhausted offsets, and unrelated logical simplification can legitimately remove execution. Deep verification needs an explicit scan requirement, not an estimate label. |
| What about projection, filters and limits? | Retain default unsupported filter pushdown. Use maintained projection and global limit initially; do not claim native column skipping until implemented. Keep one source partition and reject invalid partition indexes; return the same whole-source count for `None` and partition zero. Unsupported fetch/projection hooks must return no replacement. |
| What about bytes and malformed counts? | Keep source `total_byte_size` and column statistics absent. Compressed file bytes do not measure retained Arrow/hash state. Fallible conversion and checked sums should yield absent hints on overflow, never wrapping counts. |

## Lineage, relationships and value

The design follows `docs/PLAN.md:11-17,44-56`: use maintained Arrow/DataFusion
components and reuse completed validation. The retained full-run failure and
remaining performance qualification are documented in `docs/STATUS.md:78-98`.
This is execution infrastructure serving successful bounded join processing;
it does not change release selection or the full acceptance target
(`docs/PLAN.md:136-149`).

The adapter owns source statistics and preserves `NativePartition` execution
(`src/query.rs:125-186` at baseline). `DataSourceExec` delegates statistics and
opening to the source, adding maintained batch splitting
(`datafusion-datasource-53.1.0/src/source.rs:298-322`). `JoinSelection` consumes
physical statistics, compares byte sizes first and row counts second, and accepts
inexact values (`datafusion-physical-optimizer-53.1.0/src/join_selection.rs:59-100`).
An inaccurate hint can change build side or collect/partition choice; neither
choice alone changes relational meaning. A smaller row population does not prove
a smaller decoded-memory footprint or a successful full run.

The existing streaming provider has no statistics input
(`datafusion-catalog-53.1.0/src/streaming.rs:35-42,127-134`), so a thin source adapter
fills a real gap. Passing verified counts adds caller work but avoids unqualified
summary assumptions and manifest rereads. Removing the adapter leaves the
observed missing-statistics planning problem. A custom join optimizer would
duplicate maintained behavior. The design's value claim fails if real plans
still build the wrong input, or if the unchanged full acceptance flow remains
outside its resource/time limits; neither is resolved by this static review.

Existing query tests cover pins, duplicates/nulls, empty schemas, capacity and
batch bounds (`src/query.rs:295-452` at baseline). They do not cover the proposed
statistics, asymmetric join selection, or filter/offset/count composition.
Read those tests; none were run here. Preserve them and add the specific semantic
regression plus plan inspection in both asymmetric input directions.

**Revised recommendation:** retain `DataSourceExec`, unchanged readers, explicit
verified counts, inexact source precision, and absent byte estimates. Resolve
the filter/offset/count case before approval. Confidence is high in the static
code trace; runtime reproduction, patch conformance, memory improvement and full
acceptance remain unverified.

## Addendum: compatible upstream remedy, checked 2026-10-10

**No newer published 53.x fix was found.** Apache's release archive and the
published crate version list both stop at **53.1.0** for this major version.
The official 53.1.0 source still promotes an inexact count to exact zero when
the offset exhausts its estimate. Thus there is no published compatible patch
to substitute based on this check. This does not establish the status of
unreleased branches or later incompatible major versions.
[Apache release archive](https://archive.apache.org/dist/datafusion/),
[published crate versions](https://docs.rs/crate/datafusion/latest),
[53.1.0 limit statistics](https://github.com/apache/datafusion/blob/53.1.0/datafusion/common/src/stats.rs#L452-L464).

**No narrower general-purpose configuration guard was found in 53.1.0.**
`AggregateStatistics` reads no configuration condition; it only passes the
configuration onward during recursion. `collect_statistics=false` applies to
the default listing provider's table creation, not custom physical statistics.
`default_filter_selectivity=100` would avoid the default-estimate step in this
specific reproduction, but it does not constrain interval-derived or other
operator estimates to upper bounds; it is not a general correction. These are
source-based conclusions, not runtime comparisons.
[53.1.0 aggregate rule](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-optimizer/src/aggregate_statistics.rs#L46-L97),
[53.1.0 configuration](https://github.com/apache/datafusion/blob/53.1.0/datafusion/common/src/config.rs#L495-L500),
[53.1.0 filter estimation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/filter.rs#L310-L347).

The maintained `SessionStateBuilder::with_physical_optimizer_rules` remains the
supported way to omit that one rule while retaining the default join optimizer.
This is scoped to the sessions constructed with the modified rule list; it is
not a process-wide DataFusion switch. Retain the focused regression as the
condition for removing the guard after a qualified upstream correction. No Rust
changes, Cargo commands, or tests were performed for this addendum.
[53.1.0 session builder](https://github.com/apache/datafusion/blob/53.1.0/datafusion/core/src/execution/session_state.rs#L1231-L1237).

## Author execution addendum

The root agent reproduced the predicted zero-versus-four error in
`native-statistics-initial-check.log`; the initial suite failed. The narrow guard
then passed the regression, including the maintained flat-Parquet case, and the
full repository gate in `native-statistics-full-check.log`. Final library checks
also pass after the independent implementation review's lookup and diagnostic
corrections (`native-statistics-review-fixes-tests.log`). These are author-run
runtime observations, separate from the reviewer's static assessment. The final
plan capture in `native-join-plan-verified.log` chooses the smaller Dockets build.
No complete-path performance claim follows from these checks.
