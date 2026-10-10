# Comments join memory diagnosis

**Decision: the failed collection is a real acceptance failure. The leading
explanation is a statistics-free native scan feeding a non-spilling hash build.**
The missing statistics and hash-build behavior are verified in code. Which
Comments-stage query failed, and which named table occupied its physical build
side, remain hypotheses until its actual physical plan is captured.

Static review on 2026-10-10 of the relevant source at
`ce3ad6b88ed748bdcc56cf33ce0da43a78ac2752`, using the semi-formal code-review
workflow and Rust guidance. No implementation, tests, builds, benchmarks,
payload hashes, downloads, or budget changes were performed by this reviewer.
Official-source alternatives are evaluated in `datafusion-join-research.md`.

## Observed failure

Evidence directory:
`workspaces/qualification/comments-e2e-20261010/20261010T093708-acceptance-1aae15fa/run-01/`.

- `comments-dockets.stderr.log` records a failed additional 8.1 MB reservation
  for `HashJoinInput[6]`, already holding 923.9 MB, with 854.1 KB remaining in the
  shared pool. These are the error's formatted units.
- `result.json` records `passed: false`, 48.763463875 seconds for the interrupted
  run, and 16.870032707985956 seconds for the failed join stage. That stage exited
  1 with `stop_reason: null`.
- Sampled process-tree RSS was 9,540,452,352 bytes; the child peak reported by
  `/usr/bin/time` was 9,558,114,304 bytes. Sampled scratch was zero. These measures
  do not identify the live Arrow buffers or prove an unsampled peak.
- The join, summaries, exports and independent final oracle did not complete.
  The stopped run cannot support an elapsed-time or correctness acceptance pass.

`Budget::create` assigns half the declared memory to DataFusion's
`FairSpillPool` and half to DocSpec admission (`src/budget.rs:83-94`). For this
24 GiB limit, DataFusion's pool is 12 GiB. Pool reservations, Arrow buffer sizes
and process RSS are different measures. The error and null monitor stop reason
identify an internal allocation refusal, not a sampled RSS-limit termination.
The evidence does not establish an operating-system allocation failure.

## Function and data-flow trace

The upstream paths below are relative to the locally installed, pinned crate
source under
`/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.

| Boundary | Verified behavior and implication |
| --- | --- |
| `process_refresh` → `source_frame` → `join_frame` (`src/engine/refresh.rs:104-128,499-515`) | Opens the exact released native source pins, registers `query::occurrences` as `join_left` and `join_right`, then constructs the join with cardinality enforcement. |
| `query::provider` (`src/query.rs:17-45`) | Builds a single `NativePartition` inside DataFusion `StreamingTable`. No row or byte statistics are supplied. `projected_files` repeats this boundary for captured files (`67-98`). |
| `StreamingTable::scan` → `StreamingTableExec` | `datafusion-catalog-53.1.0/src/streaming.rs:37-80,96-134` exposes stream, schema, ordering and limit but no statistics field. The physical stream has no `partition_statistics` override (`datafusion-physical-plan-53.1.0/src/streaming.rs:236-285`); the default returns unknown statistics (`execution_plan.rs:476-491`). |
| Physical join selection | `datafusion-physical-optimizer-53.1.0/src/join_selection.rs:59-81,124-131,225-253,258-294` asks the physical children for statistics. It compares bytes, then row counts; absent both, it does not swap inputs. Auto mode falls back to partitioned hash join. |
| Hash build | `datafusion-physical-plan-53.1.0/src/joins/hash_join/exec.rs:1319-1338` names a partitioned left-build reservation `HashJoinInput[partition]`. `collect_left_input` consumes all build batches; each batch must grow the reservation before being retained (`1807-1813,1862-1885`). On refusal it returns the error; this path does not spill. |
| Reader and final consumer | `NativePartition::execute` uses the native occurrence reader (`src/query.rs:125-187`). Streaming-table column selection happens after the source emits its batch. `query::bounded` admits final result batches (`194-218`); it does not constrain internal hash-build state. |

**Finding F1 — P1: trusted native row counts are lost at the query-provider
boundary.** The engine already checks the native live-file record total against
the prepared row count before issuing a pin (`src/storage.rs:584-606`;
`TablePin.rows`, `src/model.rs:79`). Occurrence scans reject partial or filtered
tasks, deletes, and record-count/file-size mismatch
(`src/storage/occurrences.rs:369-383`). This supplies a credible metadata basis
for exact unfiltered row counts, yet the physical join optimizer cannot see it.
Adding only `TableProvider::statistics` does not repair this physical call path;
the pinned trait even documents that its optional method is not used in the main
planning path (`datafusion-catalog-53.1.0/src/table.rs:295-301`).

**Finding F2 — P1: scratch capacity cannot rescue this hash build.**
`FairSpillPool` enables cooperating operators to manage a finite allocation; it
does not implement a spill algorithm on behalf of every operator. The failed
consumer belongs to pinned `HashJoinExec`'s whole-build retention. Increasing
scratch, reducing emitted batch size, or enabling the already configured spill
directory does not remove that retained build. A smaller build or a maintained
spilling join algorithm is required; neither is an acceptance proof by itself.

## Hypotheses and remaining discriminator

**H1 — Likely: the final full-width join builds Comments.** The logical main
query writes Comments as `join_left` (`src/engine/refresh.rs:662-667`), and this
scan cannot tell the physical optimizer that 10,000,000 Comments exceed 279,719
Dockets (`experiments/comments-e2e/workload.py:16,19`). Building the large,
full-width side is consistent with the allocation error. However, logical SQL
order alone does not prove final physical order; optimizer transformations and
cardinality-probe execution must be accounted for. Confidence: strong mechanism,
unconfirmed exact failed plan.

**H2 — Unresolved: a cardinality probe failed first.** Many-to-one executes a
right-side duplicate probe before the main join. One-to-one also executes the
left-side probe (`src/engine/refresh.rs:618-660`). The error does not label the
probe versus the final query. `LIMIT 1` limits duplicate evidence returned; it
does not guarantee the underlying semi join or grouping avoids scanning or
building its full input. A small projected probe would have different memory
behavior from the final full-width output, but no retained physical plan proves
that distinction here.

Capture the optimized physical plans for the actual right probe, left probe when
applicable, and main join, using the same native providers, exact pins and session
configuration. Record input names/rows, schemas, join type, null-equality mode,
partition mode, physical left/right children, and their statistics. Pinned
`DataFrame::create_physical_plan` provides plan creation; `explain` with analysis
disabled is another maintained entry point
(`datafusion-53.1.0/src/dataframe/mod.rs:298-299,1736`). The existing CLI
`plan-refresh` describes lifecycle phases, not these physical plans. A query
against plain Parquet through another provider would not reproduce this boundary.

## Smallest justified correction and semantic obligations

First expose trusted native scan counts through a maintained physical source
boundary and let DataFusion choose and swap build inputs. Prefer the existing
`DataSourceExec` integration point over implementing join execution or a custom
join-reordering optimizer. Exact counts must correspond to the selected snapshot
and file subset, survive column selection, and respect any actually applied
limit/filter. Leave unknown decoded byte sizes and distinct counts unknown;
compressed file sizes are not exact hash-build memory. The related research
explains the compatible built-in scan and sort-merge alternatives.

This is a candidate correction, not a measured guarantee that the smaller side
fits or the full pipeline meets the deadline. Do not hard-code Dockets as the
build side, reverse every SQL join, assert key uniqueness, discard duplicate
rows, or change the declared workload. Maintained `HashJoinExec::swap_inputs`
already remaps join kinds, keys, filters and output ordering
(`hash_join/exec.rs:1047-1085`): inner/full remain their kind; left becomes right;
semi joins exchange sides. That is safer than maintaining parallel SQL rules.

Preserve the precise cardinality meaning: maximum matching partners per
occurrence; duplicate business keys remain distinct (`src/model.rs:269-275`).
The semi-join probes group only keys that match the opposite side. A global
uniqueness check would wrongly reject unmatched duplicates. Both null modes,
composite partial-null keys, empty inputs, unmatched outer rows, all output
occurrence fields, and the many-to-many/one-to-one/many-to-one variants must
retain their existing behavior. Keep the qualified dynamic-filter workaround in
`src/budget.rs:103-108`; a performance change must not silently re-enable it.

Before renewed collection, the necessary evidence is focused semantic validation
plus actual plan inspection for asymmetric inputs in both directions and both
probe roles. Then repeat the unchanged full protocol under its original limits
with a new exact-build capture. The original failed attempt remains retained.
No speedup, successful memory bound or acceptance pass is claimed by this review.
