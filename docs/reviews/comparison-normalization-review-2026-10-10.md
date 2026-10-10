# Comparison normalization review — 2026-10-10

No production correctness defect was found in the reviewed normalization. One test-coverage finding was corrected during review: the successful large-duplicate fixture now uses different before/after origins, so its duplicate group reaches the window instead of disappearing in the new first aggregation. The final bounded recheck also resolves the wide-batch fixture concern. The retained implementation-owner log shows the targeted comparison tests passing; this reviewer performed no execution.

## Scope and method

Reviewed `src/engine/comparison.rs` against `0011f7588ab58171c0f3d71e829b4902215ab70b`, including the subsequent fixture corrections. The initial reviewed working-file blob was `fe0b34c7686493b2f5fdfdb43dd31042bd46fe75`; the final rechecked blob is `3ebda979a251721f30dd4300bcdcc61c5fbee63b`. Other working-tree changes are outside this review except where their current implementations are relevant callers or callees.

Applied the repository instructions, the Rust universal/performance guidance, and the semi-formal code-review skill. This was static review: no builds, tests, formatting, benchmarks, or corpus reads. Other agents' reviews and the author's untracked change-chain review were not read. Sources include the comparison/model/query code, occurrence creation and preservation, stream admission, existing comparison tests, and the installed DataFusion 53.1.0 implementation.

The requirements assessed are typed endpoint changes, duplicate/null preservation, value versus occurrence equality, exact endpoint membership, bounded processing, and maintained operators from `docs/requirements/04-change-tracking.md:9-23`. Full R4 chain qualification is outside this patch: the public planner still selects only `Full` or `Unchanged` (`src/engine/comparison.rs:304-359`; `src/model.rs:708-726`).

## Findings

### F1 — Corrected: the large-duplicate test had stopped exercising a large window partition

- Severity: **WARNING / P2**, test coverage.
- Initial location: `src/engine/comparison.rs:711-723` in the initial reviewed diff.
- The original fixture used `file = "source"` and the same row positions on both sides. The new aggregation at `src/engine/comparison.rs:454-456` canceled every shared occurrence before `value_members`, leaving only the extra after row. Physical-plan assertions still observed a bounded window, but execution no longer exercised a duplicate partition spanning batches.
- Suggested correction: give the two sides distinct origins while keeping repeated values and the one-member surplus.
- Resolution verified in code: `src/engine/comparison.rs:720-727` now chooses `"before"` and `"after"`. The entire duplicate population therefore reaches ranking; its expected surplus remains after row 2048 (`:767-775`). The retained targeted log cited in the final recheck records this test passing.

### O1 — Correct normalization relies on unique admitted occurrence addresses

- Severity: **OBSERVATION**, correctness boundary.
- `change_members` emits one row per nonzero grouped weight; it does not expand a weight whose magnitude exceeds one (`src/engine/comparison.rs:454-473`). This is correct for the current endpoint producer, which creates one signed row per endpoint occurrence (`:336-359`).
- New stored origins use a writer UUID and monotonically increasing positions (`src/storage.rs:290-306`). Initial physical origins use each file once and its decoded row position (`src/storage/occurrences.rs:174-243`). Imported hidden origins are rejected (`src/engine/write.rs:123-132`; `src/engine/schema.rs:261-285`). Compaction copies occurrence addresses (`src/engine/compaction.rs:511-525`), and computed expressions copy them one-for-one (`src/engine/backfill/expressions.rs:186-207`).
- A future history producer must establish the same endpoint uniqueness and continuity before calling this helper. The synthetic chain input is an algebra check, not an admission check for arbitrary change files. No extra scan or infrastructure is recommended for the current trusted endpoint path.

### O2 — Operator work is reduced for occurrences; value-mode cost has a tradeoff

- Severity: **OBSERVATION**, performance and resource use.
- Occurrence mode now needs the grouped sum and filter, without rank, sort, or the window adapter (`src/engine/comparison.rs:454-460`). This is a concrete structural simplification.
- Value mode adds grouping by typed values and address before the existing ranking. It reduces later work when endpoints share many occurrences, but when identities are all different it adds a full aggregation without reducing the ranking input (`:448-463`, `:476-504`).
- Public comparison still scans both endpoints (`:336-359`), so this patch does not establish update-size-proportional I/O or latency. No throughput, peak-memory, spill-volume, or complete-lifecycle improvement was measured in this review.

## Patch summary and function trace

The patch separates exact occurrence cancellation from value representative selection. Addresses shared by both endpoints disappear first. Occurrence mode returns the remaining addressed rows directly; value mode ranks each remaining sign independently and cancels matching value ordinals.

| Function | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `changes` | `src/engine/comparison.rs:151` | Exact releases, group, options → admitted Arrow stream | Opens one comparison plan with addresses and passes it to the stream builder. |
| `comparison_plan` | `src/engine/comparison.rs:174` | Releases and address requirement → descriptor, typed fields, signed relation | Checks releases/schema interpretation; aliases visible fields; reads before with -1 and after with +1. Identical pins can skip payloads. |
| `compare_with_options` | `src/engine/comparison.rs:58` | Same endpoints/options → counts | Groups directly by requested identity and sums positive/negative multiplicity. It is an independent count path within the same plan construction. |
| `comparison_stream` | `src/engine/comparison.rs:369` | Plan → typed changed members | Calls normalization, admits the physical window when present, streams errors, enforces output batch size, and wraps values without changing their types. |
| `change_members` | `src/engine/comparison.rs:440` | Signed addressed relation and identity → side/address/value rows | Sums by values plus address, drops exact cancellation, and only invokes value ranking for value semantics. |
| `value_members` | `src/engine/comparison.rs:476` | Remaining unit signed rows → surplus representatives | Ranks by address within each value/sign; aggregates by value/ordinal; keeps only nonzero signed groups. |
| `window::admit` | `src/engine/comparison/window.rs:23` | Physical plan and shared budget → admitted plan | Refuses unbounded windows or unsupported window shapes. Reserves buffer estimates for fully sorted `ROW_NUMBER`; is a no-op when no window exists. |
| `WindowInput::execute` | `src/engine/comparison/window.rs:159` | Sorted input partition → checked batches | Rejects row/byte-limit excess before window execution. |
| `query::occurrences` / `NativePartition::execute` | `src/query.rs:32`, `:317` | Pinned native table → streamed addressed rows | Uses the shared occurrence reader and budget; errors remain stream errors. |
| `admit_stream` | `src/engine/worker.rs:159` | Stream-producing operation → retained stream | Keeps the engine and maintenance read guard through stream consumption/drop. |

Important external callers are HTTP `changes` (`src/http.rs:156-175`) and CLI `Command::Changes` (`src/main.rs:713-735`). They pass options into the same engine service; neither constructs a separate comparison implementation.

## Data flow and invariants

1. **Typed values and exact endpoint pins.** `comparison_plan` compares public Arrow schemas before building the relation and chooses explicit visible columns (`src/engine/comparison.rs:236-246`, `:272-303`, `:344-355`). The stream reconstructs a typed struct with those fields (`:394-434`). The normalization adds no casts, text serialization, or ad hoc value hashing.
2. **Unit residuals.** For one `(values, file, row)` key, endpoint uniqueness means at most one -1 and one +1. Grouping leaves -1, 0, or +1; zero is removed. Changed values at the same address remain separate typed keys. Equal values at new addresses remain separate occurrence changes.
3. **Safe representatives.** Value ranking partitions by all values and the residual sign (`:481-488`). Within each value, a matching ordinal has either a before/after pair, which cancels, or exactly one member. Therefore the independent `MAX(file)` and `MAX(row)` cannot combine different surviving addresses (`:494-504`). Removing shared occurrences first prevents an unchanged endpoint address or a canceled intermediate birth from becoming the surplus representative.
4. **Counts stay consistent.** Exact occurrence cancellation preserves the total signed multiplicity per value. Value ranking then emits exactly the absolute surplus for that value; occurrence mode emits the unit residuals. This agrees with the count path's grouped signed sums (`:84-99`).
5. **Errors and custody remain visible.** Batch mapping propagates upstream failures before building output (`:410-419`). Engine stream admission retains its guard (`src/engine/worker.rs:175-197`). Successful EOF, rather than stream creation or an empty partial prefix, establishes completeness (`src/model.rs:745-753`).

The first grouping is lazy DataFusion work, not a Rust collection of all rows. All relevant contexts share `FairSpillPool` and the configured scratch limit (`src/budget.rs:85-96`, `:110-126`). The pinned DataFusion grouped-aggregate implementation registers a memory consumer and uses partial emission, spilling, or refusal according to its mode (`datafusion-physical-plan-53.1.0/src/aggregates/row_hash.rs:569-606` in the Cargo registry). It accounts after processing an input batch (`:731-784`), so the existence of this pool is not a strict whole-process allocation or RSS proof. The patch adds no custom execution infrastructure; its window adapter is pre-existing.

## Test behavior and concrete edge cases

These are static expectations and coverage assessments. Execution evidence inspected during the final recheck is identified separately below.

| Test or case | Evidence | Assessment |
| --- | --- | --- |
| Shared endpoint address and canceled intermediate birth | `addressed_differences_cancel_occurrences_before_choosing_value_members`, `src/engine/comparison.rs:588-683` | Both inputs normalize to the specified old/final rows in both identities. Occurrence plan assertions cover removal of window/sort infrastructure. The full fixture distinguishes the new deterministic representative from the old raw-ranking result. |
| Large duplicate surplus | `duplicate_ranking_uses_causal_sorted_windows_and_shared_aggregates`, `src/engine/comparison.rs:687-797` | After F1's correction, distinct addresses survive the first grouping, and value ranking must retain exactly the extra after row. The small-pool branch requires resource failure rather than empty success. The final retained targeted log records a pass, including this branch. |
| Oversized sorted window input | `oversized_sorted_batches_refuse_before_window_even_when_values_cancel`, `src/engine/comparison.rs:801-857` | Distinct origins prevent early cancellation. The final fixture gives upstream operators enough memory to reach the independent 64 KiB batch guard; the exact `oversized_batch` assertion remains. The retained failed and successful runs resolve the earlier mismatch as described below. |
| Null, empty, duplicate, shrink, insertion, reverse direction, and same-value replacement | `tests/comparison.rs:126-234` | Reads each endpoint independently, checks every returned address belongs to the stated side, compares Rust multiset differences, and compares stream cardinality to counts. This remains substantive coverage of the changed path. |
| Same pin and failed forced read | `tests/comparison.rs:237-266` | Unchanged can be empty without payload access; force-full must surface corrupted-payload failure. The patch does not alter that selection. |
| Incompatible field types | `tests/comparison.rs:269-301` | Counts remain unknown and streaming refuses before normalization. |
| Computed values equal at new occurrences | `tests/comparison.rs:304-389` | Value mode remains empty; occurrence mode returns both sides using retained source addresses. |
| Default-only schema-rule change | `tests/comparison.rs:393-449` | Metadata effects remain visible while equal values/addresses cancel. |
| Stream dropped before consumption | `tests/comparison.rs:452-485` | The existing maintenance guard test uses the unchanged path. Guard implementation is shared with the changed path and untouched here. |

No new broad typed-value qualification is supplied by the new string fixture. The existing null/scalar integration oracle and the maintained typed grouping path support this narrow normalization change; they do not establish every native nested type, arbitrary malformed histories, or an accelerated-chain rollout.

## Exploration record

- **H1:** canceling exact addressed rows first prevents invalid intermediate representatives. **Confirmed** by the signed-relation producer, the grouping trace, and the two new fixture inputs (`src/engine/comparison.rs:336-359`, `:440-504`, `:613-642`).
- **H2:** removing rank in occurrence mode loses multiplicity. **Refined:** it would for arbitrary repeated addresses, but admitted current endpoint producers establish unique origins/positions. No such valid endpoint failure was found in the traced writer, reader, compaction, or computed-group paths cited above.
- **H3:** existing large-duplicate coverage remains unchanged. **Refuted** for the initial fixture, then corrected through distinct side origins (F1).
- **H4:** fewer operators proves a faster complete comparison. **Refuted:** occurrence work is structurally simpler; value mode adds preaggregation, and the planner still reads both endpoints. Measurement remains necessary for speed/resource claims.

## Final bounded recheck

The recheck read the updated comparison diff, the exact fixture, and the retained targeted logs. It performed no execution and changed only this report.

`docs/reviews/comparison-normalization-targeted-2-2026-10-10.log:14-28` records `ResourcesExhausted` while reserving memory for sort during aggregate spill in `GroupedHashAggregateStream[0] (net)`. That error occurred before the window guard, so the original fixture's `oversized_batch` assertion failed even though processing correctly refused. The cause remains retained rather than being treated as empty changes.

The final fixture raises only its local `memory_bytes` from 64 MiB to 256 MiB and documents why (`src/engine/comparison.rs:803-812`). It retains the 64 KiB batch-byte allowance and the exact `oversized_batch` assertion (`:856`). The separate 64 KiB insufficient-operator-pool check remains unchanged (`:778-797`). The production normalization and limits are unchanged from the initial reviewed blob.

`docs/reviews/comparison-normalization-targeted-3-2026-10-10.log:5-10` records passing endpoint/intermediate cancellation, oversized-window-input refusal, and successful duplicate-ranking tests. This is implementation-owner execution evidence inspected by the reviewer, not independent reviewer execution. The repository gate is outside this bounded recheck and was not claimed to have passed. No benchmark was run or speed improvement inferred.

## Conclusion

VERDICT: **APPROVE**

The production normalization is supported by the current endpoint-identity and multiset traces; no unresolved production defect was found. F1 is corrected, and the final bounded recheck resolves the outstanding resource-fixture concern without weakening the guard or changing production limits.

- Coverage of changed paths: **ADEQUATE** for this normalization after F1 and the resource-fixture correction.
- Confidence: **HIGH** for endpoint/multiset semantics and the targeted guard behavior; no whole-process resource qualification is claimed.
- This review does not approve full R4 completion, history acceleration, or any measured performance claim.

## Root execution addendum, 2026-10-10

The root-owned [second gate](dependency-normalization-full-check-2-2026-10-10.log)
passed formatting, strict Clippy and every executable suite. Its final Rustdoc
step failed because compiled dependency files were missing. The
[documentation-only continuation](dependency-normalization-doc-check-2026-10-10.log)
rebuilt those artifacts and passed. All gate components are therefore covered
across these runs; this is not one uninterrupted pass. No production code changed
after the reviewed normalization and hashing edits. The SQL fixture correction
compares paired rows without relying on unspecified order. This addendum records
root execution, not independent reviewer execution or performance measurement.
