# Flat-export row-group implementation review

Static verdict: **APPROVE**, with a nonblocking test-coverage warning for refusal
caused solely by the final pending row group. No correctness, disclosure, retry,
or selection regression was found in the changed implementation.

Scope: the uncommitted changes to `src/engine/export.rs` and `tests/exports.rs`
against `fbdab8b2e97f31eeaed22e6bdc97b6882bf3292e`, reviewed 2026-10-10. This
review used the repository instructions, Rust guidance, and semi-formal review
workflow. The reviewer derived findings independently without reading the author
or design-review conclusions. Builds, tests, benchmarks, and corpus payload reads
were outside this review; the parent task owns runtime qualification.

The final reviewed two-file diff has SHA-256
`9893a42cdb8c3427b2511b2275bcf6894c39f363c78ecd860e75acc8a62eeee2`
(`git diff fbdab8b -- src/engine/export.rs tests/exports.rs`). The reviewed
dependency is pinned in `Cargo.lock:3278-3281`.

## Findings

### W1 — Isolate the pending-group-only refusal boundary

- Severity: **WARNING**, nonblocking.
- Category: test-coverage.
- Location: `src/engine/export.rs:512-520`; `tests/exports.rs:392` and
  `tests/exports.rs:468`.
- Description: the small-batch test proves successful finalization of a pending
  group, and the memory-pressure fixture targets accumulated-footer refusal.
  Neither test specifically arranges for already flushed groups to fit while the
  final pending group is the first group to exceed the allowance.
- Evidence chain: the loop checks only `flushed_row_groups()` at
  `src/engine/export.rs:482-493`. The new final check adds one when
  `in_progress_rows() != 0` before `finish()` at lines 512-522. The small-batch
  assertion expects one final group; the pressure fixture checks refusal and
  retained selection but does not identify which check refused. Deleting the new
  pending-group addition would therefore lack a dedicated regression oracle.
- Suggestion: when extending this qualification, add an integration case whose
  flushed groups fit and whose final pending group exceeds the estimate. Assert
  that the prior selection remains unchanged and no materialization/result is
  retained. The present implementation is correct by static tracing, so this is
  not a merge blocker.

## Patch summary

The patch decouples physical Parquet row groups from native reader batches.
`materialize_export` no longer sets the row-group row count to `batch_rows` and
no longer flushes after every batch. It flushes when the maintained writer's
memory estimate reaches `max_batch_bytes`, then counts all completed groups plus
any pending group before finalization (`src/engine/export.rs:435-439`,
`src/engine/export.rs:476-493`, `src/engine/export.rs:505-524`).

The direct caller remains `export_admitted`, which records materialization only
after successful return and independently validates it before guarded selection
(`src/engine/export.rs:158-188`, `src/engine/export.rs:203-225`). This preserves
the immutable-release, disclosure, and recovery requirements in
`docs/requirements/02-incremental-publication.md:9-29`.

The concrete structural improvement is fewer forced row groups for small read
batches, using the existing writer and admission budget. The patch adds no new
buffer, scheduler, persisted state, configuration setting, or dependency. The
size of any throughput or whole-process memory improvement remains unmeasured
by this review.

## Function trace

For maintained dependency references below, `parquet/` means the locally
inspected `parquet-58.4.0/src/` directory in the Cargo registry.

| Function / method | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `Engine::export` / `export_request` | `src/engine/export.rs:56`, `:69` | Explicit `ExportSpec` | Immutable `ExportPin` or refusal | Validates the request and admits owned work before export execution. |
| `Engine::export_admitted` | `src/engine/export.rs:141` | Admitted request and optional stop phase | Retained/reconciled result | Reuses completed results and phases; only selects after validation and fresh authority/channel checks. |
| `Engine::materialize_export` | `src/engine/export.rs:374` | Captured source release and authorized columns | `ExportMaterialized` | Reads exact selected files, writes projected batches, bounds estimated accumulated metadata, finalizes, hashes, and syncs output. |
| `OccurrenceScan::values` / `read_projected_file` | `src/storage/occurrences.rs:236`, `:317` | Exact native inventory and field indices | Bounded projected batch stream | Projects by field ID; retains metadata and batch reservations; refuses unsupported tasks and checks source row counts. |
| `authorized_schema` | `src/engine/export.rs:640` | Source schema and explicit policy | Indices, clean schema, field descriptions | Preserves order, field IDs, types, and nullability; excludes internal fields and arbitrary source metadata. |
| `Budget::reserve` / `blocking` | `src/budget.rs:158`, `:130` | Byte count / blocking closure | Owned capacity permit / closure result | Refuses exhausted capacity; permits remain with blocking work until completion. |
| `ArrowWriter::write` / `memory_size` | `parquet/arrow/arrow_writer/mod.rs:349`, `:301` | Typed batch / pending columns | Encoded pending group / estimate | Maintained writer accumulates batches, automatically enforces its row-count cap, and reports pending-column memory estimates. |
| `ArrowWriter::flush` / `finish` | `parquet/arrow/arrow_writer/mod.rs:439`, `:488` | Pending writer state | Completed group / file metadata | Flush takes the pending group; finish flushes it and writes file metadata. |
| `SerializedFileWriter::write_metadata` | `parquet/file/writer.rs:329` | Completed group metadata | Serialized footer | Takes ownership of accumulated row groups at line 344; inspecting them after finish would miss the records. |
| `HashingFile::write` / `finish` | `src/engine/export.rs:831`, `:816` | Accepted writer bytes / owner and path | Streaming digest and durable object reference | Hashes only accepted bytes and syncs file/directory after Parquet finalization. |
| `validate_output` | `src/engine/export.rs:729` | Materialized object | Retained `ExportValidation` | Checks actual serialized footer length before decoding, exact schema/counts, file size, and a bounded returned sample. |
| `small_read_batches_export_exact_data_with_bounded_footer` | `tests/exports.rs:392` | Native fixture reopened with one-row batches | Assertions | Requires one output group, exact values, repeat identity, and selected pin. |
| `memory_driven_footer_limit_preserves_prior_selection_and_can_resume` | `tests/exports.rs:468` | Distinct wide strings and a smaller writer allowance | Assertions | Requires export-footer refusal, preserved prior selection, absence of false completions, and exact resumed data. |

## Data flow and invariants

**Reader size remains a processing control.** `batch_rows` still enters the
native reader at `src/storage/occurrences.rs:116`. The projected stream reserves
two batch allowances at line 365; `strip_file_column` checks returned Arrow
memory before exposing batches (`src/storage/schema.rs:106-112`). Nothing in
this patch widens source admission or changes which rows/columns are read.

**The writer retains one owned reservation across batches and finish.** The
existing reservation is `4 * max_batch_bytes + max_metadata_bytes`
(`src/engine/export.rs:420-442`). The writer and permit move together through
each blocking closure and remain together at finalization (lines 465-495 and
507-523). The batch is consumed by each closure; the patch adds no collection of
Arrow batches. Returning an error drops writer state and the permit without
recording materialization.

**The flush threshold is an estimate, not a hard peak-memory ceiling.** Parquet
sums pending column estimates (`parquet/arrow/arrow_writer/mod.rs:297-305`),
which include encoded bytes and encoder estimates
(`parquet/column/writer/mod.rs:549-555`). The check occurs after a batch is
written, so a batch can cross the threshold before the explicit flush. It does
not measure compression scratch, allocator overhead, native reader buffers, or
whole-process resident memory. The reservation provides unchanged admission
headroom; runtime measurements are still required for a memory claim.

**The maintained row cap remains active.** Removing the explicit reader-sized
cap restores Parquet 58.4's default of 1,048,576 rows
(`parquet/file/properties.rs:47`, `:599-606`). `write` splits crossing batches and
flushes when this cap is reached (`parquet/arrow/arrow_writer/mod.rs:362-369`,
`:409-418`). Such groups also enter the existing post-write metadata check.

**Metadata is checked before the consuming boundary.** `footer_group_bytes`
remains the documented conservative admission estimate and is unchanged
(`src/engine/export.rs:449-454`). The loop checks flushed groups, and the final
check adds exactly one nonempty pending group using saturating arithmetic.
Parquet holds one pending group; `finish` flushes that group and then consumes
the accumulated records. Empty output adds none. Independently, the later
validation checks actual footer byte length (`src/engine/export.rs:748-755`).
Thus this estimate cannot authorize selection of an actual oversized footer.

**Disclosure and logical values are unchanged.** Explicit field order/type/ID
reconstruction remains in `authorized_schema`; batches reuse those projected
columns under the rebuilt schema (`src/engine/export.rs:390`, `:456-459`,
`:640-725`). Compression, disabled statistics, and disabled offset indexes remain
unchanged. Row groups alter physical grouping, not the selected rows or field
values. The complete decoded row total must equal the retained source count
before finalization (lines 499-504).

**Failure and retry remain on the existing lifecycle.** Protection is retained
before the writer opens its unique output (lines 391-418). A footer refusal
returns before materialization retention at lines 165-168. Completed phases
remain reusable at lines 158-188. Final selection still checks current authority
and expected channel under the dataset lock (lines 203-225); completed retries
return their original result at lines 148-153. The patch introduces no partial
file adoption or channel update on failure.

## Test behavior and edge cases

These are source-level coverage assessments, not test-run results.

| Test / scenario | Source evidence | Expected behavior and limit |
| --- | --- | --- |
| Small reader batches | `tests/exports.rs:392` | The output finishes as one group under the fixture allowance; independent decoding and digest verification compare all expected values. This directly rejects the old forced-per-batch layout. |
| Genuine writer/footer pressure | `tests/exports.rs:468` | Wide distinct strings are intended to create memory-driven groups; the error must mention the flat-export footer, so an earlier generic metadata refusal cannot silently satisfy the test. Source creation uses a separate 16 MiB fixture allowance and one-row batches at lines 486-489; the export-under-test still uses 32 KiB batches and 256 KiB metadata at lines 514-516. The parent task owns execution evidence for reaching the intended guard. |
| Primitive types, nulls, column order and IDs | `tests/exports.rs:159-388` | Expected unchanged: independently decoded typed arrays equal original values, with deliberate reorder and omitted private column. |
| Duplicate/null occurrences and completed retry | `tests/exports.rs:560` | Expected unchanged: exact decoded multiset, clean metadata, source identity, and unchanged file modification time on retry. |
| Replacement, deletion and historical download | `tests/exports.rs:612` | Expected unchanged: exact new population, old download intact, completed old retry cannot move the channel backward. |
| Empty source | `tests/exports.rs:673` | Expected unchanged: readable standalone schema, zero rows/sample, explicit disclosure policy. The new pending-group count is zero. |
| Completed-phase reuse and lost response | `tests/exports.rs:719` | Expected unchanged: withholding source metadata does not block retained validation/selection reuse; lost selection response reconciles. Requires `test-util`. |
| Stale authority or channel | `tests/exports.rs:773` | Expected unchanged: completed output is retained without promotion. Requires `test-util`. |
| Process-killed durable handoffs | `tests/exports.rs:833` | Expected unchanged: completed materialization/validation are not repeated. Requires `test-util`; this review ran no processes. |
| Final pending group alone exceeds estimate | `src/engine/export.rs:512-520` | Correct refusal by inspection; no dedicated fixture, as recorded in W1. |
| Maintained automatic row-cap flush | `parquet/arrow/arrow_writer/mod.rs:362-418` | Completed groups are included in the existing loop check. No new fixture specifically crosses the default cap. |

`independent_rows` verifies the object digest, clean field metadata, column order,
and all decoded values (`tests/exports.rs:113-155`). It uses the same maintained
Parquet implementation through an independent file reader; it is not
cross-library qualification or a resource measurement. The fixture sorts rows,
so it checks multiplicity and values without promising physical row order.

## Conclusion

VERDICT: **APPROVE**

- The patch achieves the intended structural change by letting the maintained
  writer combine read batches while preserving existing admission, disclosure,
  finalization, and selection checks.
- Coverage of changed paths: **ADEQUATE** for the scoped change, with W1 retained
  as a narrow, nonblocking missing boundary case.
- Confidence: **HIGH** in the static correctness and lifecycle trace. Runtime
  fixture success, full-flow timing, metadata size, scratch, and whole-process
  memory qualification belong to the parent task's retained evidence.
