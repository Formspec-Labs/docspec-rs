# Compacted related output: independent implementation review

**Static decision: APPROVE. No unresolved production finding.** The focused test addition resolves the review's initial coverage gap around editing inside a compacted output file and independently checking its change proof. Test execution and the repository gate remain implementation-owner responsibilities; this review does not claim their completion or measured performance.

## Scope and basis

Reviewed the working diff against `bd6eadecb9ac052fcd2b6090075501b0e2e08290` in `src/engine.rs`, `src/engine/{admission,refresh,incremental,aggregation,evidence,write}.rs`, `src/engine/write/tests.rs`, `tests/related_refresh.rs`, `tests/related_refresh/compaction.rs`, and the narrow helper visibility change in `tests/related_refresh/forward_changes.rs`. Read the parent/repository instructions and applicable Rust and semi-formal review guidance. Traced relevant storage, compaction, change verification, retention, and phase callers/callees. No builds, tests, benchmarks, corpus reads, cleanup, or production/test edits were performed by this reviewer. The unrelated change-chain review was excluded.

Requirements used: R4 exact endpoints and occurrence-aware change evidence (`docs/requirements/04-change-tracking.md:13`); R5 frozen prior output, complete affected groups, unchanged-value adoption, and retained phase recovery (`docs/requirements/05-dependent-refresh.md:13`, `:23`, `:33`, `:37`); R6 equivalent physical layout, stable occurrence identity, and protection of old/in-flight references (`docs/requirements/06-maintenance-retention.md:13`). Status claims were not used as correctness evidence.

## Patch summary and function trace

Incremental joins and summaries now capture the selected physical source representation of their prior **output** at request admission. Processing consumes that immutable capture; changes continue to name the canonical logical base. Source updates share the same metadata validation helper.

| Function / location | Input → output | Verified static behavior |
| --- | --- | --- |
| `captures_source`, `src/engine/admission.rs:22` | Operation request → capture decision | Includes source updates and incremental refresh; excludes full refresh. |
| `prepare_and_select`, `src/engine.rs:979` | Request and guarded authority → retained operation | Resolves the representation once for a new admission at `:1041`; an existing operation bypasses recapture at `:1032`; stale preparation retries against a fresh boundary at `:1087`. |
| `protect_admission` / `admitted_dependencies`, `src/engine/admission.rs:151`, `:94` | Captured base/layout → protected retained dependencies | Protects captured source objects at `:292`; verifies request kind, owner, revision, and retained binding before reuse at `:108-146`. |
| `prepare_source`, `src/engine.rs:752` | Operation/capture → processing phase | Resolves completed processing first at `:761`; otherwise passes the admitted source at `:885-919` and records it in native dependencies at `:956`. |
| `process_refresh`, `src/engine/refresh.rs:109` | Refresh/capture → prepared source | Forwards the capture only to the incremental path. Full reconstruction keeps its existing behavior. |
| `open_captured_source`, `src/engine/write.rs:43` | Canonical/captured pins → opened native table | Rejects different logical identity or row count, checks immutable metadata and snapshot/schema IDs through `storage::open_pin`, and permits only a preserved public schema. |
| `process_incremental`, `src/engine/incremental.rs:40` | Qualified base, coverage, capture → prepared source/change relation | Legacy absence falls back to the canonical pin at `:63`; unchanged input values adopt the exact captured output with empty changes at `:95-108`. |
| `process_incremental_join`, `src/engine/incremental.rs:137` | Changed input keys and captured prior output → complete replacement | Reads prior output from the capture at `:254`; retains existing old/new input discovery; replaces complete affected tuples and uses canonical before metadata at `:316-324`. |
| `process_aggregation`, `src/engine/aggregation.rs:184` | Affected groups and captured prior output → complete replacement | Uses captured field IDs and source at `:256-282`; its change relation keeps canonical before metadata at `:289-297`. |
| `source_replacement`, `src/engine/evidence.rs:136` | Capture/native table and fresh rows → normalized replacement | Reuses the opened native table; adds origin fields only when absent and normalizes fresh rows against that schema. |
| `process_update`, `src/engine/write.rs:212` | Update/capture → prepared source | Uses the shared capture check before origin augmentation; the former inline logical/schema checks are consolidated. |
| `replace_keys` / `finish_source_replacement`, `src/engine/write.rs:422`, `:394` | Explicit key scope → shared/replaced files and actual removed/added rows | Shares untouched files; preserves stored origins during rewrites; an empty applied change returns the captured pin and matching empty change schemas. |
| `phase` / `complete`, `src/engine.rs:1745`, `:1837` | Deterministic phase identity → retained winning completion | Earlier qualified processing keeps its original dependency and protection records. A later request's capture does not relabel consumed inputs. |
| `verify_changes` / `verify_native_changes`, `src/engine/changes.rs:88`, `:161` | Canonical base, actual result, applied rows → proof check | Requires exact before/after metadata at `:145-157`; deep checking reconstructs values and occurrence membership at `:217-281`. |

Public entry is `Engine::refresh` (`src/engine/refresh.rs:34`) through `run_internal` and `prepare_and_select` (`src/engine.rs:701`, `:979`). Evidence construction consumes the actual applied source changes (`src/engine/incremental.rs:490`), and promotion retains the existing input/authority guards (`src/engine/refresh.rs:236`).

## Data flow and invariants

- **Capture is immutable and protected.** The selected verified representation becomes `AdmissionDependencies.update_source`. Its objects are retained before the request is acknowledged. The same commit binds the dependency record (`src/engine/admission.rs:48-70`); reuse validates its digest and revision through `validate_binding` (`src/engine/retention/owners.rs:234-289`). Retention's existing typed walker includes the source table (`src/engine/retention/inventory.rs:1380-1393`). A changed capture cannot inherit the earlier binding merely by keeping its operation ID.
- **Physical and logical identities remain distinct.** Layout selection requires verified equivalent logical metadata and row count (`src/engine/compaction.rs:52-84`). The shared opener additionally checks native schema preservation. Actual edits produce a new candidate; unchanged values retain the captured equivalent pin. Change proof endpoints remain the canonical base and actual result, rather than the current mutable representation selection.
- **Occurrence identity survives rewriting.** Existing origin values pass through the common reader; fallback file/row origins are materialized before filtering (`src/storage/occurrences.rs:174-195`, `src/storage/identity.rs:128-202`). Fresh replacements receive fresh origins through schema normalization (`src/engine/schema.rs:249-286`). Compaction equivalence already checks values, multiplicity, order, and occurrence columns (`src/engine/compaction.rs:540-605`).
- **Recovery follows completed work.** A retry after admission consumes the retained capture. A retry or equivalent new operation with completed processing adopts that phase before performing more work. Evidence and validation bind to those consumed phase dependencies (`src/engine/refresh.rs:152-166`, `src/engine.rs:1282-1302`), including when another compaction is selected later.

## Tests and concrete edge cases

These are assertions read from test source, not reported execution results. Static traces support their expected behavior; execution remains the implementation owner's responsibility. The suite is registered through `tests/related_refresh.rs:1116` and `tests/integration.rs:43`.

| Test / location | Behavior checked |
| --- | --- |
| `captured_source_requires_logical_identity_count_and_public_schema`, `src/engine/write/tests.rs:37` | Accepts optional origins; refuses wrong logical identity, row count, and a falsely labelled equivalent public schema. |
| `partial_compaction_is_shared_by_disjoint_refresh_and_noop_advances_coverage`, `tests/related_refresh/compaction.rs:259` | Join and grouped-summary edits share unchanged compacted files by path/digest/mtime; duplicate/null data remains subject to value/contributor oracles; no-op coverage adopts the exact newer representation. |
| `edits_inside_compacted_related_files_preserve_untouched_occurrences_and_public_changes`, `tests/related_refresh/compaction.rs:43` | Establishes edited and surviving rows in one actual compacted file, replaces the affected group with duplicate input, verifies stable surviving output occurrences and changed physical file, checks complete values/contributors, and explicitly requests deep verification. Reuses `check_forward` (`tests/related_refresh/forward_changes.rs:48`) to compare exact public accelerated/full values and endpoint addresses for both value and occurrence identities. |
| `admitted_related_refresh_resumes_exact_output_after_later_compaction_and_reopen`, `tests/related_refresh/compaction.rs:337` | Interrupts at admission/processing; isolates admission as the sole retention root; selects another layout; reopens and verifies exact capture, completed records, processing dependencies, files, values, and contributors. |
| `global_summary_compaction_preserves_empty_nonempty_and_noop_transitions`, `tests/related_refresh/compaction.rs:428` | Exercises empty/nonempty global groups, duplicates/nulls, removal of all input, and exact no-op layout/coverage adoption. |
| `equivalent_new_admission_reuses_processing_from_the_prior_output_layout`, `tests/related_refresh/compaction.rs:490` | Abandons the original operation after processing, captures a later layout under a new operation, then retains the prior completed table and physical dependencies without relabeling the completion. |

## Resource use, reuse, and conclusion

The concrete simplification is one shared logical/count/schema capture validator and continued use of the existing admission, protection, replacement, and evidence machinery. Joins and summaries now pass their already opened captured native table into normalization instead of reopening the canonical output in that helper. This is a code-path observation, not a speed measurement.

The common replacement algorithm still probes source-file keys and rewrites files containing affected keys (`src/engine/write.rs:488-568`). Capturing a useful layout prevents falling back to its older fragmented files; it does not establish delta-proportional I/O or a throughput improvement. No new independent cache, lifecycle record family, payload proof scan, or cleanup behavior was introduced by this patch.

**VERDICT: APPROVE.** The reviewed implementation preserves the admitted physical output while retaining logical change endpoints, complete value/contributor handling, occurrence identity, and immutable recovery dependencies. Coverage of changed paths is **ADEQUATE** for this scoped implementation checkpoint after the added proof fixture. Confidence is **HIGH for static consistency**, with runtime qualification and performance measurements outside this review.
