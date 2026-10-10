# Join inventory reuse: independent static review

**Verdict: APPROVE.** No correctness blocker found. The retained-task capacity
tradeoff below must remain explicit; this review establishes no speed, process
memory, or full-workload improvement.

Scope: current `src/engine/refresh.rs` and its new `inventory_tests.rs` against
`f05dab6`, including the revised one-shot comment and fixture engine limits.
Reviewed under the Rust and semi-formal code-review skills. No builds, tests,
benchmarks, or corpus reads were run by this reviewer. Only this report was
written.

## Findings

**WARNING — retained task capacity changes the accepted workload envelope.**
`refresh.rs:538` captures all native tasks before cardinality probes. Capture
retains each task's full-root `project_field_ids` vector and bounds their
cumulative size (`storage/occurrences.rs:296–338`). The old lazy path plans only
the query's selected fields as execution starts
(`storage/occurrences.rs:127–150`). A wide, many-file input can therefore reach
`metadata_capacity` sooner under the same configured limit. The inventories
also remain admitted between probes and through the main query. This is an
explicit, bounded refusal before output, not silent data loss, and does not
justify raising limits. Document the tradeoff and qualify width/file-count
scaling before claiming equivalent capacity or complete-path improvement.
Confidence: high in the changed bound; no failing full-data workload was measured.

## Patch and function trace

The patch shares one captured immutable scan across a join's cardinality probes
and main execution. Exact same-version self-join roles share its ownership;
one-shot joins retain their existing lazy provider.

| Function | Evidence | Verified behavior |
| --- | --- | --- |
| `process_refresh` / `source_frame` | `refresh.rs:109–134,880–895` | Full processing owns a local context and enables cardinality; incremental processing keeps its existing separate path. |
| `join_frame` / `register_join_inputs` | `refresh.rs:505–551` | Checks each exact release version; captures only for enforced many-to-one/one-to-one; shares only equal `VersionRef`s. |
| `join_plan` / `join_queries` | `refresh.rs:560–714` | Existing type checks, null policy, left/right aliases, probes, and output occurrence tuple stay unchanged. |
| `OccurrenceScan::open` / `capture` | `storage/occurrences.rs:271–338` | Completes bounded inventory/task qualification without opening payloads; retains the existing metadata permit. |
| `query::files` / `NativePartition::project` | `query.rs:82–141,208–250` | Shares captured scan through projected providers; composes root-column indices before native reads and retains row-count statistics. |
| `read_projected_file` / execution | `storage/occurrences.rs:362–402`; `query.rs:321–404` | Each query owns fresh stream state; selected files open sequentially; reader/projection reservations follow streams. |

## Data flow and invariants

- `captured` is local to registration. Registered providers own `Arc` references;
  a self-join still has separate SQL aliases and reader state. Distinct versions
  never share through this branch (`refresh.rs:526–548`).
- Both old and new paths use `Occurrences`, `Selection`, the pinned native schema,
  and the maintained reader. Full-file task checks remain intact. Selection
  replaces captured task field IDs before decoding, retaining the virtual field
  that forces ID-aware interpretation (`occurrences.rs:383–398`;
  `selection.rs:38–107`; `schema.rs:78–110`). Defaults, field identity, stored
  origins, nulls, and original row offsets do not switch to name-only or
  post-decode interpretation.
- Captured-file row sums replace the selected pin's row statistic, retaining
  `Precision::Inexact` and unknown decoded size (`query.rs:96–103,284–297`).
  Pin creation already requires the same native row sum (`storage.rs:628–642`).
- Capture errors/cancellation drop temporary task state and permits. A registered
  first input remains owned by the local context if the second input fails;
  propagation out of `process_refresh` drops that context. Successful execution
  retains scans until its context/plan/stream owners drop. No detached work,
  cross-request cache, or durable lifecycle reference is introduced.
- Disabled enforcement and many-to-many use the old provider. Existing one-probe
  many-to-one and two-probe one-to-one logic remains unchanged
  (`refresh.rs:525,544–546,673–680`).

## Test behavior and limits

- `cardinality_and_main_join_replay_without_manifest_and_share_self_join_admission`
  (`inventory_tests.rs:86`) removes the test-owned manifest list after capture,
  confirms fresh inventory loading fails, then executes both probes and the main
  query twice. Exact nullable values and non-null role addresses are asserted.
  Reservation assertions distinguish one shared inventory from two and require
  ownership to survive removal of one alias, then release after the last owner.
  This is a meaningful regression test, not just a call-count assertion.
- `one_shot_join_inputs_keep_inventory_lazy` (`inventory_tests.rs:141`) covers
  disabled enforcement and many-to-many and reserves the entire application
  pool immediately after registration. It checks no retained inventory admission
  in those branches; it does not measure process memory or execute those queries.
- Existing cardinality rejection and exact self-role oracle checks remain
  relevant (`tests/related_refresh.rs:410–455`), as does successful unmatched
  duplicate handling (`:771–794`). Captured projection composition, empty
  selection, SQL nulls, and synthetic row positions are explicitly asserted in
  `src/query/selection_tests.rs:142–239`.
- Existing reader tests cover partial-read ownership, EOF/error release, and
  selected-file streams surviving their original scan owner
  (`storage/occurrences/tests.rs:157–184,219–237`). The new tests do not separately
  inject cancellation during capture or a second-input registration failure;
  those drop paths were assessed statically. No width/file-count crossover or
  retained-memory scaling result accompanies this review.

## Conclusion

The change achieves request-scoped replay using the existing native reader and
keeps query semantics and early column selection. Coverage is **adequate for
this narrow change**, subject to the parent's execution gate. Confidence is
**high for local correctness and ownership**, with resource scaling unqualified.

VERDICT: APPROVE
