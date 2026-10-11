# Duplicate-first cardinality: independent review

**Verdict: approve the implementation, subject to the root-owned checks.** No
actionable correctness or resource-ownership defect was found. The change skips
the partner scan when constrained keys are unique and retains the old matched-key
proof when duplicates exist. Representative elapsed-time, byte and resource
effects remain unmeasured by this review.

This blind static review covers the uncommitted `src/engine/refresh.rs`,
`tests/related_refresh.rs` registration and new
`tests/related_refresh/cardinality.rs` on 2026-10-10. I applied the repository,
Rust and semi-formal review guidance, traced production callers and fixture
assertions, and read relevant pinned DataFusion source. I did not read the
author's implementation note or the unrelated change-chain review, and ran no
builds, tests, benchmarks or provider calls.

## Findings

No changes requested. One nonblocking performance observation remains:

**O1 — The duplicate branch adds constrained-side work.** At
`src/engine/refresh.rs:578-579`, a positive duplicate probe runs the unchanged
matched-key probe afterward. This is the intended safe fallback, but its extra
scan/grouping is real. For N partner rows and M constrained rows, the unique
branch replaces an N+M input probe with an M input probe; the duplicate branch
adds an M scan and grouping before the original work. Actual grouping state
scales with distinct keys and can spill. The scan-count fixture explicitly
expects the extra constrained execution. Measure both branches before claiming
a complete-path speedup; this does not require another implementation framework
or block the semantics fix.

## Function trace

| Function / seam | Reference | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `join_frame` / `register_join_inputs` | `src/engine/refresh.rs:513-558` | Exact input pins + definition → registered providers | Reuses admitted immutable captures for cardinality probes/main query; exact self-join versions share the capture but retain separate aliases. |
| `join_registered` | `src/engine/refresh.rs:561-565` | Incremental scoped views → joined frame | Calls the same plan with cardinality enabled; caller registers the new affected scopes at `src/engine/incremental.rs:276-282`. |
| `join_plan` | `src/engine/refresh.rs:568-587` | Registered sources + spec + enforcement flag → lazy main frame / refusal | Executes the duplicate proof first, short-circuits the matched probe on false, preserves the `join_cardinality` refusal before returning the main frame. |
| `duplicate_keys_query` | `src/engine/refresh.rs:590-623` | Side-specific complete key tuple + null policy → grouping SQL | Quotes exact side names, groups all tuple fields together, excludes any-null tuples only for `Never`. |
| `join_queries` | `src/engine/refresh.rs:625-767` | Schemas/spec → constrained-side probes + main SQL | Existing identical scalar-type checks and join predicates remain unchanged. `ManyToMany` or disabled enforcement produces no probes; `ManyToOne` constrains right; `OneToOne` constrains right then left. |
| `has_rows` | `src/engine/refresh.rs:902-910` | Probe SQL → boolean | Uses maintained streamed execution, stops on the first nonempty result batch, and propagates query/read errors. No rows are collected into a project-owned key index. |
| `CountedPartition`, `register`, `spec` | `src/engine/refresh.rs:987-1062` | Fixture rows → maintained streaming provider + counters/spec | Increments on provider execution; batch handles are shared. The test intentionally leaves the returned main frame lazy. |
| Integration registration | `tests/related_refresh.rs:1119-1120`; `tests/integration.rs:49` | Child module → existing integration target | The new ordinary suite is registered through the repository's grouped target. |

## Semantic proof and data flow

The initial hypothesis was that grouping the constrained side could accidentally
reject legal unmatched duplicates or miss partially null composite matches.
The source refutes both cases:

1. `join_queries` validates key fields/types before any duplicate query runs
   (`refresh.rs:644-660`). Public request validation already requires nonempty
   keys (`refresh.rs:361-369`). The new helper consumes the existing definition;
   it does not create or persist a second key identity.
2. Under ordinary equality, any null key component prevents a match. Requiring
   every component `IS NOT NULL` before grouping therefore excludes exactly those
   tuples that cannot create matching fanout. Under null-safe equality, the
   filter is omitted; SQL grouping retains duplicate all-null and partial-null
   tuples (`refresh.rs:609-621`).
3. The fast branch admits only when the complete matchable key tuples have no
   duplicates. In that case no partner can match more than one constrained row.
   It does not matter whether the main join is inner, left or full.
4. When any duplicate exists, the old semi-join proof still determines whether
   duplicate constrained rows actually match the other side. Unmatched duplicate
   keys remain legal; the new preliminary result alone never refuses a request
   (`refresh.rs:578-584`).
5. Both proofs read the same immutable registered sources. In the incremental
   path these are the existing complete affected scopes, not mutable live heads
   (`incremental.rs:236-252,276-282`). No source pin, operation identity, output
   schema, contributor-address rule or selection boundary changes.

The new grouping uses the existing DataFusion context. It retains the shared
`FairSpillPool` and scratch bound (`src/budget.rs:85-88`) and existing optimizer
correctness guards (`src/budget.rs:101-127`). Pinned DataFusion 53.1.0's grouped
aggregation implements reservation/spill handling in
`datafusion-physical-plan/src/aggregates/row_hash.rs`; no custom in-memory set,
parallel reader fan-out or row-by-row execution was added. These facts do not
establish a whole-process memory ceiling or a representative spill cost.

## Test behavior and limits

- `unique_keys_skip_partner_execution_and_duplicates_run_matched_proof`
  (`src/engine/refresh.rs:1065`) asserts proof execution counts and refusal for
  many-to-many, unique many-to-one, unique one-to-one, unmatched duplicates and
  matched duplicates. On a passing run, unique many-to-one must execute the
  constrained provider once and partner provider zero times; duplicate cases
  must execute the constrained provider twice and partner once. These are
  physical provider executions, not network bytes or full-flow timings.
- `composite_duplicates_only_refuse_when_the_constrained_keys_match`
  (`tests/related_refresh/cardinality.rs:7`) exercises distinct unmatched duplicate
  keys on both sides, every null-placement pattern in a two-column key, both
  constrained roles, both null policies and self-joins. Successful full joins
  pass the existing direct Rust matching oracle; `check` compares values and
  exact left/right occurrence evidence (`tests/related_refresh.rs:280-368`).
- Existing all-kind/null/duplicate and empty-input coverage
  (`tests/related_refresh.rs:375`) remains relevant. Existing declared-cardinality
  tests assert refusal before native result files exist
  (`tests/related_refresh.rs:410-445`); unmatched duplicate acceptance is also
  retained (`tests/related_refresh.rs:771`). The new shortcut is independent of
  join kind because the main query and matched-key predicate are unchanged.

The fixtures are meaningful and cover the new branch behavior; this reviewer
has not executed them. They do not measure real native range requests,
large-distinct-key grouping memory, or duplicate-heavy runtime overhead. Those
are evidence limits, not identified semantic defects.

**Conclusion:** APPROVE. The patch uses maintained relational operators to avoid
provably unnecessary work while preserving the existing refusal semantics.
Changed-path coverage is adequate for the implementation; confidence is high
in the traced integer/composite/null cases. Runtime gate results and performance
claims remain the implementation owner's responsibility.
