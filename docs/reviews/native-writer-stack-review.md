# Native writer stack boundary review

The shared boxed writer boundary is a focused correction for the observed
default-stack failure. Static review finds no change to rows, file production,
budget lifetimes or error handling. Runtime closure still requires the original
failing test and repository gate at their unchanged stack settings.

This review used the Rust and semi-formal code-review skills. The reviewer did
not build, run tests, change Rust files or alter runtime limits. Caller line
references below identify frozen checkpoint A, tree
`712cb67ad36968ef67e6a0a25e705abc518af2c8`; writer references identify the focused
correction to that tree. Unrelated compacted-update edits were excluded.

## Findings and diagnosis

**F1 — BLOCKER, resource correctness, corrected in source; execution pending.**
The original `write_batches` and `write_batches_sized` returned concrete async
futures. Paired change writes held two such futures inside `try_join!`
(`src/engine/write.rs:508`), while other writer calls were also embedded in the
same method (`:369`, `:512`). The expanded writer therefore contributed its
construction and state sizes throughout deeply nested application polls.

The owner's `/tmp/docspec-local-stacktrace-deep.log:138`–`:149` traces one call
through `plan_files`, occurrence capture, `replace_keys`, aggregation,
incremental refresh, `prepare_source` and `prepare_and_select`. It reaches native
Avro manifest-list parsing. The trace contains no recursive DocSpec call cycle;
the writer is not being polled at the failing instruction.

The owner's `/tmp/docspec-stack-disasm.log` establishes these coexisting poll
frames from their AArch64 stack-allocation prologues:

| Poll frame | Bytes |
| --- | ---: |
| `replace_keys` | 211,568 |
| `process_aggregation` | 113,472 |
| `prepare_source` | 558,864 |
| `prepare_and_select` | 763,872 |

Those frames alone total 1,647,776 bytes before the remaining caller and native
parser frames. This supports reducing embedded future storage at the shared
writer boundary. It does not establish the corrected frame sizes or an overall
throughput improvement; those were not measured by this reviewer.

**F2 — OBSERVATION, qualification.** The correction has not yet been executed at
the time of this static review. Preserve the failed gate, backtrace and
disassembly alongside the eventual corrected test result. Do not claim the
default-stack regression resolved from source review alone.

## Patch and function trace

`write_batches` now returns the sized writer's `BoxFuture` directly.
`write_batches_sized` is a non-async constructor that returns
`Box::pin(async move { ... })`. Comparing against checkpoint A with whitespace
ignored shows the writer body is unchanged; the remaining edits are signatures,
the lifetime bound, the box and comments (`src/storage.rs:173`–`:351`).

| Function | Inputs and output | Verified behavior |
| --- | --- | --- |
| `write_batches`, `src/storage.rs:175` | Table, batch stream, budget → boxed result future | Uses the existing maximum batch size as target and delegates directly. It adds no second box. |
| `write_batches_sized`, `src/storage.rs:186` | Same inputs plus target bytes → boxed result future | Constructs and boxes the unchanged writer body outside the caller's poll. |
| `write_output`, `src/engine/write.rs:149` | DataFrame → prepared native pin | Executes one stream, awaits the shared writer, then appends and pins its files. |
| `replace_keys`, `src/engine/write.rs:336` | Captured base, key scope, replacement frame → table and actual changes | Shares unchanged files; writes keys, kept/removed pairs and additions through the shared boundary. |
| Compaction rewrite, `src/engine/compaction.rs:434` | Chosen native rows and target size → written files | Calls the same sized boundary, preserving its disk checks and exact equivalence check downstream. |
| Occurrence capture, `src/storage/occurrences.rs:93` | Native table and admitted inventory → bounded scan | Calls maintained manifest planning at `:112`; this is where the original nested application stack reaches the parser. |

## Data flow and invariants

- `table` and `budget` remain borrowed for the returned future's lifetime; `S`
  remains `Send` and is moved into the future. The explicit `'a` bound makes that
  relationship visible without cloning payloads (`src/storage.rs:186`–`:195`).
- Invalid-target rejection and both budget reservations remain inside the async
  body. Constructing an unpolled writer has no file or budget effect
  (`src/storage.rs:196`–`:205`).
- Reservations still cover stream consumption, metadata rotation, write and
  final close, and release with the same future on success or error
  (`src/storage.rs:204`, `:205`, `:347`–`:351`).
- The paired writers remain in the existing `try_join!`; channel capacity,
  backpressure and shared-files behavior are unchanged
  (`src/engine/write.rs:477`–`:511`). No worker or scheduler was added.
- This adds one fixed future allocation per writer invocation, not per row or
  batch. It leaves scans, writes, hashing and validation unchanged. Avoiding
  oversized nested construction storage is the concrete benefit; throughput and
  total memory improvements require measurement.

## Tests and edge cases

The original regression is
`quoted_utf8_boolean_composite_groups_and_int32_measures_refresh_exactly`
(`tests/related_refresh/aggregation_types.rs:148`). It creates typed groups,
moves records between nullable composite groups, performs incremental refresh
(`:242`), and independently compares exact summary values and contributor
occurrences (`:49`–`:145`). Its five-row starting input shows the observed stack
failure does not require large payloads. It must pass without increasing the
thread stack.

Existing compaction tests exercise empty input and partial-file sharing,
bounded descriptor refusal followed by a retry, and footer rotation under an
unreachable file-size target. In the live source these are
`empty_source_and_partial_file_groups_preserve_exact_membership`
(`tests/compaction.rs:499`),
`tiny_targets_refuse_bounded_descriptor_inventory_without_selecting_partial_files`
(`:950`) and
`huge_target_rotates_bounded_footer_windows_without_limiting_total_rows`
(`:1029`). Their assertions cover unchanged row identities, no selection after
partial failure, successful retry and continued bounded rotation. No new test
that merely mirrors the box implementation is warranted.

## Conclusion

VERDICT: APPROVE

Justification:

- The focused source change addresses the observed embedded-future pressure at
  the shared writer boundary and preserves its processing behavior.
- Coverage of changed behavior: ADEQUATE existing regression and writer cases;
  corrected execution remains the owner's required next check.
- Confidence: HIGH for semantic preservation; MEDIUM for complete runtime
  closure until the default-stack regression and repository gate pass.

## Author execution follow-up

The original default-stack regression passes in
[native-writer-stack-corrected.log](native-writer-stack-corrected.log). The
[complete repository gate](local-lifecycle-check.log) also passes at unchanged
stack and resource settings, including the full related-refresh matrix. These
are author-run outcomes; the original failure and frame evidence remain in
[native-writer-stack-backtrace.log](native-writer-stack-backtrace.log) and
[native-writer-stack-disassembly.log](native-writer-stack-disassembly.log). No
throughput or corrected frame-size claim is made.
