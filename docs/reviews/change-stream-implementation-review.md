# Public change stream: independent implementation review

## Finding and verdict

**Static verdict: APPROVE the scoped full-comparison foundation.** No remaining
blocking defect was found in the frozen patch. The computed-group,
schema-default, source-origin, descriptor, and window-admission findings are
corrected, including the later no-op empty-change schema regression, whose
unchanged regression tests now pass. Repository gate coverage is complete across
the retained original and continuation runs described below. This is not a claim
that one uninterrupted gate passed, or that full R4 is complete.

Scope: working changes against `48bbf53d7af289ffe630b867f15be2daa9da1882`,
reviewed 2026-10-10 under the Rust and semi-formal code-review guidance. This is
the public full-comparison foundation: chain composition, portable certificates,
independent accelerator retirement, and complete R4 qualification remain outside
this checkpoint. This reviewer changed only this document and ran no build,
test, formatter, benchmark, corpus scan, or commit. No other review report or
change-history design was read. A window-resource concern was communicated by
another agent; the claims below were independently traced in pinned source.

The last material finding concerned sorted batches and window memory. Pinned
DataFusion 53.1 sorts by row count and
[releases its output reservation before returning each batch](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-53.1.0/src/stream.rs:741).
The window has no native pool admission for its input, copied keys, and retained
tail. Checking only final output bytes therefore missed oversized intermediate
batches, including fully canceled differences.

The correction inserts
[WindowInput](/Users/mikewolfd/Work/spicy-stack/docspec-rs/src/engine/comparison/window.rs:23)
after maintained physical optimization. It accepts only one exact `RowNumber`
function in `BoundedWindowAggExec` with `Sorted` input, refuses unbounded windows
and excess workers, preserves the child's exact plan properties and ordering,
and checks actual bytes and rows before yielding to the window
(`window.rs:133–175`). The plan and returned input stream retain per-worker
permits. This adds no sort implementation, row cache, or payload copy.

The final allowance admits twelve batch payloads per actual worker plus a
separate rows-by-fields estimate for scalar keys, maps, and rank state
(`window.rs:63–88`). The payload estimate explicitly includes owned key strings
and old/new concatenation overlap: a one-row tail can retain the backing bytes
of a larger sliced batch. These allocations follow the pinned
[partition buffer extension](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-expr-53.1.0/src/window_state.rs:281),
[key extraction](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-53.1.0/src/windows/bounded_window_agg_exec.rs:833),
[window-state key cloning](/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-expr-53.1.0/src/window/standard.rs:175),
and pruning paths (`bounded_window_agg_exec.rs:1100–1149`). This closes the
specific unsupported ownership assumptions found in review. It remains an
allocation estimate, not a measured or formally established process-memory cap;
the multiplier and bookkeeping estimate need requalification on operator changes.

## Function and data trace

The patch shares one admitted endpoint plan between counts and row streams,
uses maintained readers and DataFusion operators, and adds thin CLI/HTTP entry
points. It removes duplicated comparison setup without adding another change
journal.

| Path | Verified behavior |
| --- | --- |
| `compare` → `compare_with_options` (`comparison.rs:32,58`) | The legacy method selects values. Signed multiplicities are grouped by the selected identity, then positive and negative excess become counts. Incompatible visible schemas return unknown counts. |
| `changes` → `comparison_plan` (`comparison.rs:151,174`) | Retained available releases are checked, immutable selection is resolved, and verified physical representations are selected. Exact shared canonical pins can skip payload reads unless forced. |
| `comparison_stream` → `change_members` (`comparison.rs:369,441`) | Output contains side, address, and typed values; schema metadata binds endpoints, identity, selected path, fallback, field IDs, and independent effects. |
| Source replacement (`write.rs:182,384`; `evidence.rs:105`; `incremental.rs:279`; `aggregation.rs:266`) | Direct and related source rewrites add missing origin fields. A shared finalizer restores the exact captured source and matching empty change schemas when no rows were applied. |
| HTTP `/changes`, CLI `Changes` | Both call the engine and preserve Arrow descriptor metadata; successful completion requires the normal Arrow IPC end marker. |

Each endpoint contributes one signed row, `-1` before and `+1` after. Value
identity uses every selected typed field; occurrence identity also uses file and
row. `ROW_NUMBER` ranks each side within that identity by address. Grouping by
identity and ordinal cancels one matching member from each side. A surviving
ordinal has exactly one endpoint row, so the separate `MAX(file)` and `MAX(row)`
cannot combine two surviving addresses. Duplicate excess is retained. Value
mode chooses deterministic representatives and correctly does not claim which
equal-valued occurrence disappeared. Null grouping is delegated to maintained
operators and covered by the independent scalar oracle.

The pinned `ROW_NUMBER` evaluator is causal and supports bounded execution
(`datafusion-functions-window-53.1.0/src/row_number.rs:132–165`);
`StandardWindowExpr::uses_bounded_memory` delegates that capability
(`datafusion-physical-expr-53.1.0/src/window/standard.rs:275`). The reviewed
physical-plan log selects `BoundedWindowAggExec`, `mode=[Sorted]`, followed by
partial and final grouped aggregates. This resolves the previous full-partition
`SUM` window issue; the narrow input wrapper handles the separate application
admission boundary described above.

## Corrected findings and lifetime checks

- Computed groups use the ordinary native reader with their retained address
  columns, validate the address schema, and exclude those columns from public
  values (`comparison.rs:237–290,335–350`). This avoids adding a second address
  pair through `occurrence_schema`.
- Schema effects compare native visible field rules, including defaults, plus
  sorted endpoint identifier sets. Arrow compatibility is a separate decision,
  allowing default-only edits to report a schema change with zero value changes
  (`comparison.rs:247–279`).
- Source origins are preserved through all changed source replacement call
  paths. The existing storage writer keeps prior origin pairs and assigns new
  births to added rows (`storage.rs:278–286`). The shared no-op finalizer restores
  captured pins and both empty change schemas (`write.rs:384–410`).
- The common unchanged-input refresh path now uses `replacement_output` for its
  empty change table (`incremental.rs:70–80`). Previously, `create_output` called
  `fresh_metadata`; pinned Iceberg creation resets schema IDs and can reuse the
  fresh ID when fields match (`table_metadata_builder.rs:78–89,636–666`). An
  origin-evolved source could therefore retain its exact pin while empty
  changes had a different native schema ID, correctly failing
  `verify_native_changes` (`engine/changes.rs:160–163`). The reused replacement
  helper clones native metadata and clears snapshots without renumbering its
  schema (`storage/tables.rs:37–78`). The source remains `before.clone()`, and
  complete native schema validation remains unchanged. This is a production
  correction, not a fixture adjustment.
- Descriptor key IDs are sorted and reported separately for each endpoint.
  Evidence effects include computed execution origins, qualification, source
  metadata, base manifest, input fields, and native field bindings. Definition
  changes remain separate (`comparison.rs:499–540`). These compare retained
  references and declarations; they do not prove payload inequality or perform
  new execution qualification.
- The returned stream retains comparison metadata and window allowances.
  `admit_stream` retains the maintenance read guard and engine through EOF/drop
  (`engine/worker.rs:175–196`). HTTP retains the engine through response lifetime
  and reuses demand-driven admitted IPC encoding (`http/ipc.rs:28–69`). Errors
  propagate without a successful IPC end marker. Pinned Arrow's `finish` writes
  that marker and flushes (`arrow-ipc-58.4.0/src/writer.rs:1462–1474`). CLI uses the
  existing direct-writer pattern rather than the HTTP encoder's explicit
  temporary-buffer reservations; total CLI memory remains unqualified.

## Evidence and limits

Reviewed test assertions establish the following intended coverage; execution
status is tracked separately below.

- `tests/comparison.rs` uses independent Rust bags for endpoint membership,
  duplicate/null/empty values, both directions and identities, and count/stream
  agreement. It also covers forced corrupted reads versus the unchanged
  shortcut, incompatible types, default-only schema rules, and maintenance
  admission until returned-stream drop.
- Its computed-group fixture retains execution evidence from a `length(text)`
  backfill, checks an unchanged source's evidence/definition signals, and checks
  that an equal-valued replacement yields no value members but two occurrence
  members. This is a scalar retained computed-group case, not independent
  qualification of every computed execution/evidence shape.
- `tests/change_origins.rs` covers direct receipt-free updates, duplicate/null
  key groups, exact no-op pins, retries, compaction, later rewrites, and a related
  join update with survivors in the rewritten file. It checks related no-op
  source and empty-change schema identity. The same helper is used for aggregate
  refreshes, but that newly affected path lacks an equivalent dedicated fixture.
- `tests/change_transport.rs` checks HTTP/CLI agreement with engine batches,
  descriptor preservation, option defaults, success terminators, missing groups,
  and HTTP request limits. Transport agreement is not an independent semantic
  oracle; a failure after some public-stream rows is not directly covered here.
- `comparison.rs` includes a physical-plan/large-duplicate fixture, plan-permit
  retention and release, and a small operator-pool refusal. A separate fixture
  scatters wide rows across individually valid input batches and requires an
  `oversized_batch` error after sorting even though all values would cancel. It
  also checks admission release after error. These establish the checked
  handoff and refusal behavior, not measured resident memory.

Observed root-owned evidence:
[window-plan-5.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-window-plan-5.log)
records successful bounded/sorted-plan, grouped-output, lifetime, operator-pool,
and oversized-sorted-batch assertions with the new wrapper.
[targeted-6.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-targeted-6.log)
records successful origin, comparison, and transport suites, including related
survivors/no-op, retained computed groups, and default-only rules. These focused
runs preceded the final payload-allowance increase and equivalent Clippy
predicate cleanup. The root-owned
[full-check-3.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-full-check-3.log)
records successful library tests and integration suites through the suites
preceding `defaults`, including the origin, comparison, and transport suites on
the final production sources. The root reports that formatting and strict
Clippy also passed. The run then stopped at `tests/defaults.rs:497`; it is not a
successful complete repository gate.

The reviewer statically checked the subsequent test-only correction. The
rebuild explicitly uses `updated.id` as its base (`tests/defaults.rs:475`), so
comparing its complete native schema with `updated` rather than the pre-update
`changed` release is correct. The retained failure shows the added origin pair
and its resulting native schema ID as the difference; all public/default fields
are preserved. The assertion still compares the full native schemas, including
origins. The `unchanged_edit` helper now excludes all reserved `__docspec_`
fields (`tests/defaults.rs:85`), matching the production schema request validator
(`engine/schema.rs:412–416`); it does not strip them from the stored schema or
weaken the equality assertion. No new blocking finding resulted from this
test-only review.

[defaults-check.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-defaults-check.log)
records the corrected defaults suite passing. The root-owned
[gate-continuation.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-gate-continuation.log)
then records successful intervening integration suites but stops in
`related_refresh`. Aggregate and join no-op fixtures failed with
`invalid_changes` at `tests/related_refresh/aggregation.rs:270,362` and
`tests/related_refresh/incremental.rs:353`. These were real regressions caused by
the empty-change constructor described above. The reviewer independently
traced and statically approved its narrow production correction.

[gate-continuation-2.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-gate-continuation-2.log)
records the unchanged `related_refresh` suite passing, including the previously
failing aggregate and join no-ops, followed by successful restore, catalog
restore, retention-inspection, and scalar-binding suites. It then stopped at
`tests/schema.rs:350`: the fixture still expected the highest allocated field
ID to be 2 after an update now adds origin fields. The reviewed test-only
correction expects 4 and asserts the complete current name/ID list:
`comment_id=1`, `__docspec_origin_file=3`, `__docspec_origin_row=4`. This preserves
the earlier assertion that hiding field 2 leaves its historical allocation
intact, and explicitly prevents reuse of that ID. It matches
`storage/identity.rs:98–125`, which assigns origins above `last_column_id`.
Public values and release verification assertions remain unchanged.

[gate-continuation-3.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-gate-continuation-3.log)
records corrected schema and succession suites passing, then stops on obsolete
update-fixture expectations. The reviewer checked both test-only adjustments.
The nested-update fixture compares every original complete `NestedField` with
the current prefix and requires exactly origin fields 6 and 7 after nested
field ID 5 (`tests/updates.rs:764–774`). Recursive field IDs, types, defaults, the
typed value oracle, and the renamed-child refusal remain checked. This is nested
write coverage, not qualification of all nested public comparison outputs.

The overwritten-Parquet fixture now explicitly establishes a changed file size
and expects `unavailable_phase`, retaining its unchanged-latest assertion
(`tests/updates.rs:411–434`). The actual path is `process_update` →
`source_with_origins` → `storage::pin`, which checks inherited object sizes in
`check_object_available` (`storage.rs:657,150–161`) before native decoding.
Renaming the fixture to its actual earlier refusal is accurate; this fixture
no longer establishes incompatible physical-schema rejection. No production
native validity guard was changed.

[gate-continuation-4.log](/Users/mikewolfd/Work/spicy-stack/docspec-rs/docs/reviews/change-stream-gate-continuation-4.log)
records corrected updates and worker initialization passing, successful execution
of the doc-test target, a clean formatting check, and strict all-target,
all-feature Clippy passing. The root reports exit 0, and the finalized log
confirms those results. Production sources have not changed since the no-op
constructor fix.

Gate coverage is complete across `full-check-3`, the corrected defaults run,
and continuations 1–4. The unchanged complete `related_refresh` suite exercised
the production no-op correction. Earlier unaffected suites were deliberately
not repeated; this evidence is not one uninterrupted full-gate pass or a claim
that every suite ran after the final narrow correction. Failures and interrupted
gates remain retained. No execution was performed by this reviewer.

Nested values, broader computed shapes, direct aggregate-refresh origin
qualification, mid-stream public transport errors, high-cardinality memory
measurement, and the complete R4 qualification matrix remain gaps. The public
byte check can conservatively refuse a sorted batch even when the eventual
change relation would be small; that is the intended refusal behavior.

Confidence is high in the traced semantic corrections and handoff checks,
moderate in the unmeasured allocation estimate. No timing, scan-byte,
whole-process memory, or complete-R4 claim is established. Static approval and
completed gate coverage are limited to this foundation.
