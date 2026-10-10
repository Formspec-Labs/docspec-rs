# Flat export layout: design review

**Decision: RECONSIDER the current batch-to-row-group coupling.** Keep the maintained ArrowWriter, its default row limit, the existing memory reservation and both metadata checks. Remove the reader-batch row-group override and unconditional flush; flush when ArrowWriter's estimated buffered memory reaches `max_batch_bytes`. Include the final pending group in footer admission. This is the smallest defensible correction; an additional encoded-byte target is unnecessary for this change.

Static review on 2026-10-10 at `fbdab8b2e97f31eeaed22e6bdc97b6882bf3292e`. This reviewer changed only this document, ran no builds/tests/benchmarks, and read no corpus payloads. Runtime qualification belongs to the root agent.

## Findings

### F1 — Reader batching makes the required export exceed its own estimate

**BLOCKER; intent-versus-shape; RESHAPE.** The writer sets its row-group cap to `batch_rows`, then flushes after every input batch (`src/engine/export.rs:430-440`, `:478-490`). Small reader batches therefore create small output groups even when combining them would fit memory. Group metadata accumulates across the single output file.

The retained request exports 32 fields; its limits are 4,096 rows/batch and 32 MiB metadata (`run-01/comments-dockets-export.request.json:19-51`, `run-01/limits.json:5-7`, under the evidence directory below). The current formula charges at least `1024 × (1 + 32) = 33,792` bytes/group before names/type descriptions (`src/engine/export.rs:454-456`). Ten million rows need at least 2,442 groups at that cap: **82,520,064 bytes of estimated metadata**, exceeding the allowance irrespective of compression. File-ending partial batches can add groups.

The retained error is exactly the pre-finalization admission refusal (`run-01/comments-dockets-export.stderr.log:1`; branch at `src/engine/export.rs:480-490`). **It does not establish that serialized footer bytes exceeded 32 MiB.** The actual footer-length check runs later, after materialization (`src/engine/export.rs:165-182`, `:733-741`). The failed export stage lasted 18.28 seconds; these are failed-stage diagnostics, not complete acceptance (`run-01/comments-dockets-export.metrics.json:20-22`).

Evidence directory: `workspaces/qualification/comments-e2e-native-statistics-20261010/20261010T101127-acceptance-d075da9a/`.

### F2 — Larger groups need memory-based flushing and final-group admission

**CONCERN; resource accounting; RESHAPE.** Simply removing `flush()` leaves the batch-sized row cap in place; simply removing both controls without a memory check can buffer wide groups. Keep ArrowWriter's default row cap and flush after each write when `memory_size() >= max_batch_bytes`. Preserve the existing `4 × max_batch_bytes + max_metadata_bytes` writer reservation (`src/engine/export.rs:420-426`).

The maintained estimate sums active column writers; it excludes retained completed-group metadata (`parquet-58.4.0/src/arrow/arrow_writer/mod.rs:292-305`). It is neither serialized footer length nor whole-process resident memory. A threshold checked after a write can overshoot during that input batch; compression, decoding and allocator transients remain outside a mathematical RSS guarantee. The native reader separately reserves two batch allowances and admits decoded values against its byte limit (`src/storage/occurrences.rs:133-139`, `:357-366`). Keeping the writer's existing reservation provides admission headroom, not proof that every allocation fits it.

Keep the conservative accumulated-footer guard separate from the memory estimate. Count `flushed_row_groups().len()` plus one when `in_progress_rows() > 0`, or explicitly flush and check before finalization. **Do not check the flushed-group vector only after `finish()`**: the native file writer moves that vector into metadata serialization (`parquet-58.4.0/src/file/writer.rs:327-345`). Preserve `validate_output`'s actual tail-length check (`src/engine/export.rs:733-741`). Label the first refusal as estimated footer accounting so operators do not mistake it for a measured footer.

### F3 — The old refusal fixture encodes the behavior being removed

**CONCERN; qualification; RESHAPE.** `flat_export_footer_limit_preserves_prior_selection_and_can_resume` sets `batch_rows: 1` to force a group per row (`tests/exports.rs:429-445`). Under the proposed policy, those tiny inputs should combine, so keeping its expected failure would preserve the defect.

Use the small existing population to prove that tiny reader batches now succeed with exact independent values and fewer groups. Retain a separate public refusal/retry case whose bounded writer memory genuinely forces enough groups to exhaust metadata: create the source with the normal profile, then reopen with a small batch-byte allowance and distinct variable-width values that individually fit that allowance. Keep the specific export refusal, unchanged prior selection, absent materialization/result, and successful exact retry assertions (`tests/exports.rs:443-475`). Tune only fixture data/declared limits; add no test-only production setting. A small focused writer test should cover a memory-triggered flush and a final partial group that would cross the footer allowance. Execute and adjust the fixtures before accepting their claimed coverage.

## Artifact summary and lineage

This is a product distribution path with proof checks: an exact release and explicit column policy enter; the maintained native reader streams authorized values; ArrowWriter produces one immutable Parquet object; independent creation checks precede guarded catalog selection (`src/export.rs:15-39`, `:54-85`; `src/engine/export.rs:141-225`, `:374-390`). Physical row-group boundaries are not part of the exported value/schema promise.

| Prior artifact | Relationship and constraint |
| --- | --- |
| `docs/requirements/02-incremental-publication.md:13-23` | Native results stay canonical; flat distribution preserves exact population, types and authorized bytes. |
| `docs/PLAN.md:44-57`, `:69-75` | Reuse maintained work and completed phases; validate output once. |
| `docs/reviews/local-export-review.md:13-17` | Earlier review qualified the narrow refusal/retry shape; explicitly did not qualify full-scale memory or performance. |
| `Cargo.lock:204-205`, `:3278-3279` | Arrow and Parquet are pinned to 58.4.0; maintained APIs were checked at this version. |

| Component / named seam | Owner | Dependency and consumer |
| --- | --- | --- |
| `ExportPolicy` / `ExportPin` | Export service | Exact release and authorization → immutable consumer download (`src/export.rs:15-85`). |
| `OccurrenceScan::read_projected_file` | Shared native storage | Captured snapshot and field IDs → authorized Arrow batches (`src/storage/occurrences.rs:317-366`). |
| ArrowWriter / creation validation | Export service over Parquet | Batches → protected object, streaming digest and retained validation (`src/engine/export.rs:398-425`, `:504-515`, `:714-810`). |
| Catalog result/channel selection | Shared lifecycle | Completed checked object → guarded selection (`src/engine/export.rs:203-225`). |

## Invariants and commitments

| Commitment | Status under the proposed correction | Evidence and failure prevented |
| --- | --- | --- |
| Exact active rows, values, nulls, requested order and stable field IDs | PRESERVED; must requalify | Shared scan, rebuilt authorized schema and row-count check stay unchanged (`src/engine/export.rs:374-390`, `:461-464`, `:497-501`, `:625-710`). Existing independent type/value tests remain load-bearing (`tests/exports.rs:159-388`). |
| Only authorized bytes and metadata are disclosed | PRESERVED | Keep statistics/indexes disabled and stripped schema metadata (`src/engine/export.rs:438-440`, `:673-684`). |
| Declared resource allowances and controlled refusal | RELIED-UPON | Keep reservations, disk check and both metadata guards (`src/engine/export.rs:420-426`, `:469-490`, `:720-741`); larger groups change when memory is retained, so runtime evidence is required. |
| Completed output survives interruption; prior selection survives failure | PRESERVED | Keep phase records and guarded selection unchanged (`src/engine/export.rs:158-188`, `:203-225`). Unfinished materialization is not a reusable completed phase. |

## Maintained API choice and user value

ArrowWriter already combines batches, exposes `memory_size()` and retains a default row cap. Its encoded-size estimate, `in_progress_size()`, is documented as no larger than its column memory estimate. Testing the same encoded threshold after each batch adds no protection. Native `set_max_row_group_bytes` can split later batches using average encoded bytes/row, but skips that prediction for an empty group and cannot promise a resident-memory bound for skewed rows (`parquet-58.4.0/src/arrow/arrow_writer/mod.rs:349-418`, `:973-1000`; `src/file/properties.rs:46-47`, `:746-758`). [Maintained ArrowWriter documentation](https://docs.rs/parquet/58.4.0/parquet/arrow/arrow_writer/struct.ArrowWriter.html).

**Choose the memory-only flush policy now.** An additional encoded-byte target would need a concrete reader/layout objective or measured advantage. No new tuning knob, batching layer, metadata serializer or writer abstraction is justified. The concrete improvement is fewer forced groups and fewer repeated column metadata records. Throughput, output size and peak-memory improvements are hypotheses until measured.

## Counterfactuals and verification

- **Kill criterion:** unchanged limits still refuse the representative output, or the full path exceeds its memory/storage allowance. Report that result; do not raise budgets or weaken metadata checks.
- **Opposite decision:** raising metadata allowances keeps reader batching coupled to file layout and postpones the same scaling failure. Replacing the writer adds debt when the pinned component already supports the required behavior.
- **Removal probe:** deleting the footer guard allows accumulated metadata to grow unchecked; deleting flat export breaks the required consumer path. Neither serves the requirement.
- **Sibling subsumption:** the maintained writer fully supplies buffering/flushing; the existing lifecycle already supplies protection, identity and recovery. Keep those responsibilities there.
- **Root verification:** focused layout/refusal/final-group tests, existing primitive/disclosure/recovery integration, repository gate, then the unchanged complete Comments acceptance path and independent oracle. Retain observed row-group count, actual footer bytes, export time and process memory with the run evidence. Full acceptance remains unqualified until the required fresh runs complete (`docs/PLAN.md:143`; `docs/STATUS.md:53-65`).

**Verdict: RECONSIDER the existing layout; adopt the minimal correction above.** Intent and current shape diverge at batch-driven group creation. The user-value claim is supported by deterministic accounting and the retained refusal. The correction preserves the named guarantees and pays down accidental coupling; the maintained writer subsumes the buffering work. Confidence is **HIGH** in the failure diagnosis and **MEDIUM** in runtime sufficiency until the root agent executes qualification.
