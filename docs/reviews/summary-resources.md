# Summary refresh resource review

No actionable resource finding remains. The design uses the existing refresh lifecycle, replacement writer, admitted readers and shared DataFusion runtime. No new scheduler, cache, index or transport service is needed. The unnecessary metadata reservation and default-stack failure are corrected, and direct numeric-limit coverage is added. The complete gate and no-default-feature compilation pass. **VERDICT: APPROVE** for this summary checkpoint.

This is a static semi-formal review of the uncommitted summary extension against `67a2c07`. The reviewer applied the Rust performance, universal, library and resilience guidance, inspected changed code, relevant callers/callees, maintained dependency source and owner-produced logs, and did not execute tests or edit production/test code. Scope is grouped/global fixed-state summaries and their resource/recovery integration; complete-path performance and whole-process memory qualification remain outside this checkpoint.

## Findings

### SR-R1 — Discovery inventory admission outlived its useful work — CLOSED

- **Severity/category:** WARNING / resource efficiency.
- **Location:** `src/engine/aggregation.rs:206–217`.
- **Scenario:** The first implementation retained the affected-key native inventory throughout replacement writing solely to read its file count afterward. That retained descriptor memory and its `2 × max_metadata_bytes` reservation while the writer and native readers required their own reservations. A constrained operation could refuse despite the discovery inventory no longer being needed.
- **Resolution:** File count and byte totals are extracted in a short block; the inventory and reservation drop before provider registration and replacement writing. The source/evidence pins and their existing protection remain intact. This removes unnecessary live admission without adding another cache or metadata path.

### SR-R2 — Default-stack guard scenario aborted during incremental summary processing — CLOSED

- **Severity/category:** BLOCKER / resource correctness.
- **Location:** `src/engine/incremental.rs`, Aggregate branch of `process_incremental`; owner fixture `summary_coverage_guards_and_rebase_preserve_completed_work` in `tests/related_refresh/aggregation.rs`.
- **Evidence:** `summary-correctness-expanded.log` and `summary-stack-diagnostic.log` record a stack overflow on the normal test stack. Phase markers place it after incremental processing starts and before evidence. Boxing the aggregate-dispatch boundary alone has not closed it (`summary-stack-boundary-fix.log`). Basic summary fixtures pass, so those successes do not establish this guard path.
- **Resolution:** `process_incremental` now contains only common input loading/verification and boxed transform dispatch (`src/engine/incremental.rs:52–85`). Join-specific preparation moved to `process_incremental_join` (`:88`), so its unused locals/poll frame no longer lie on the summary path. The owner-produced `summary-stack-transform-split.log` passes the same guard fixture on the default stack, followed by the complete `summary-check.log` regression gate. The unsuccessful shared Build/Update boxing changes were reverted. No larger stack or test-body-only workaround is used.

### SR-R3 — Numeric admission refusal lacked direct branch coverage — CLOSED

- **Severity/category:** WARNING / test coverage.
- **Location:** `src/engine/aggregation.rs:355`, `370`.
- **Evidence:** The initial extreme-value fixture verified widened sums and exact Decimal128 output, but did not exercise `aggregate_range` when the declared verified source row count exceeds `i64::MAX`.
- **Resolution:** The shared `check_row_count` helper now has a direct test accepting `i64::MAX`, refusing the next count with `aggregate_range`, and using checked i128 arithmetic to prove the worst signed partial sum remains below `10^38`. `register_input` calls it before opening the source provider. The test passes in the unit section of `docs/reviews/summary-check.log`. No giant dataset or artificial runtime hook is needed.

### SR-R4 — Evidence scope is evaluated twice — ACCEPTED OBSERVATION

- **Severity/category:** OBSERVATION / performance.
- **Location:** `src/engine/aggregation.rs:299–336`.
- **Evidence:** The distinct union of removed/added summary keys supplies the writer's materialized removal scope and also remains a logical view inside the replacement contributor semi-join. It can therefore reread/deduplicate summary delta rows twice. It does not repeat the SUM computation or materialize unbounded contributor arrays.
- **Disposition:** Keep this narrow reuse of maintained relational operators for now. A general callback/cache abstraction or extra durable discovery phase is not justified by a timing measurement. Document the repeated delta-key work and distinguish one contributor output stream from one total read pass.

## Patch summary

`RefreshTransform` names either a Join or an Aggregate within the existing `RefreshSpec`. Aggregate recipes select one exact native input, direct grouping fields and named count/sum/min/max functions. Grouping keys initially accept Boolean, Int32, Int64 and Utf8; sum/min/max operands accept signed Int32/Int64. COUNT follows input occurrence/null semantics. A joined input's duplicate fanout therefore contributes multiple rows.

Full summary preparation streams a maintained DataFusion aggregate into the existing native writer. Its separately retained evidence contains each input file/row and its typed group key. Incremental preparation unions old/new keys from changed immutable files, recomputes complete affected new groups and invokes the shared replacement writer. Evidence replaces complete affected groups from the new captured input. Actual removed/added summary rows retain deliberately replaced groups even if aggregate values remain equal, so changed contributors are not lost.

Global aggregation uses an empty grouping list in SQL and adds a reserved constant output key. Empty input produces one summary row with zero counts and null sum/min/max, and no contributors. Incremental global discovery uses file membership to create at most one affected key without decoding changed-file keys. An unchanged file population produces an empty replacement stream and exact prior pins. This is a discovery optimization; prior hashing, actual global recomputation and final independent verification still read payloads when required.

## Function trace

| Function / method | File:line | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `RefreshTransform::versions`, `same_roles`, `same_definition`; `RefreshSpec::inputs` | `src/model.rs:280`, `286`, `294`, `324` | Typed transform → exact inputs / comparison | Centralizes one-input versus two-role dispatch; kind and role order matter even when dataset sets match. |
| `validate`, `admit_refresh`, `process_refresh` | `src/engine/refresh.rs:284`, `46`, `85` | Recipe and admitted coverage → refusal / native preparation | Keeps common input capture/verification and phase acknowledgment; branches only where a transform builds native rows. |
| `phase_key`, `fingerprint` | `src/engine/refresh.rs:371`; `src/engine.rs:1213` | Transform, base and mode → reuse/retry identity | Includes the complete transform in material processing identity; admission reuses verified exact input vectors. Incremental keys retain exact output-base manifests. |
| `aggregation::validate`, `qualify`, `check_row_count` | `src/engine/aggregation.rs:10`, `83`, `355` | Grouping definition/schema/count → success/refusal | Checks named distinct output fields, reserved names, the supported type domain and arithmetic range. |
| `register_input` | `src/engine/aggregation.rs:65` | Exact input pin → admitted native occurrence provider | Verifies input identity, caps row count, opens the source pin and applies type qualification. |
| `aggregate_frame`, `evidence_frame` | `src/engine/aggregation.rs:117`, `155` | Qualified input/definition → lazy frames | Casts signed operands before SUM; global SQL omits GROUP BY; evidence streams group keys plus exact file/row rather than arrays. |
| `source_frame`, shared `evidence_frame`, `output_keys`, `hidden_field` | `src/engine/refresh.rs:755`, `769`, `783`, `789` | Transform → source/evidence/key/visibility behavior | Small dispatch helpers preserve the shared lifecycle, verifier, delta reader and combined-reader path. |
| `process_incremental`, `process_incremental_join`, `changed_keys`, `incremental_evidence` | `src/engine/incremental.rs:52`, `88`, `267`, `404` | Exact base and input coverage → native changed keys/source/evidence | Reuses captured manifest checks and byte verification; separate boxed transform preparation avoids unnecessary join poll-stack costs on Aggregate. |
| `OccurrenceScan::open`, `read_file` | `src/storage/occurrences.rs:209`, `257` | Exact native files → bounded occurrence streams | Retains inventory and stream admissions, checks complete file tasks/counts, and reads requested file payloads serially. Global discovery never invokes `read_file`. |
| `process_aggregation` | `src/engine/aggregation.rs:180` | Old/new inputs and exact base → Prepared | Materializes affected keys, scopes new input, executes one aggregate replacement stream, and constructs exact ChangePin key IDs/endpoints. |
| `aggregate_evidence` | `src/engine/aggregation.rs:286` | Applied summary changes/new input → evidence table | Unions old/new group keys, streams new contributors and replaces complete groups with the existing writer. |
| `replace_keys`, `write_batches`, `pin` | `src/engine/write.rs:255`; `src/storage.rs:162`, `440` | Scope and replacement rows → shared native candidate/change files | Existing admitted paired writers, pre-split batch checks, disk checks, file ownership and no-op pin adoption remain shared. |
| `verify_related_with_inputs` | `src/engine/refresh.rs:621` | Candidate, evidence and exact retained inputs → semantic validity | Reconstructs complete aggregate values and contributor relation, compares schema and typed multiplicities, and retains join-only cardinality checks for Join. |
| `verify_changes`, `verify_groups` | `src/engine/changes.rs:6`; `src/engine/backfill/verification.rs:6` | Source/change/composition pins → validity | Uses transform-specific replacement keys, exact parent/base binding and same-role checks; previous signed multiset validation remains shared. |
| `select_refresh`, `guard_inputs`, dependency `walk` | `src/engine/refresh.rs:177`, `239`; `src/engine/verification.rs:79` | Retained candidate and current authorities → selection/refusal | Existing sorted dependency locks, accepted succession, captured/latest policy and transitive eligibility continue to apply. |
| `read_combined`, `compare`, `read_evidence` | `src/engine/backfill.rs:80`; `src/engine.rs:331`; `src/engine/read.rs:23`, `54` | Exact release → visible rows/comparison/evidence | Hides the global helper where existing related helpers are hidden; raw evidence remains independently readable through the same services. |

The engine processor/format identity advances to v8 (`src/engine.rs:81–82`). The JSON recipe shape intentionally changes to the typed transform; no compatibility/migration behavior is assumed. Existing join tests were adapted to construct the same recipes through the enum rather than replaced by summary-only coverage.

## Data flow and invariants

1. **Signed sum arithmetic has a proven finite range.** `register_input` requires `N ≤ i64::MAX`; `qualify` admits only Int32/Int64 for SUM. Casting every operand to Decimal128(38,0) before aggregation makes every possible partial sum's absolute value at most `(2^63 − 1) × 2^63`, strictly below `10^38`. This handles cancellation/order and negative `i64::MIN`, not just positive final values. Pinned DataFusion's `datafusion-functions-aggregate-53.1.0/src/sum.rs:296`, `377–381` uses wrapping arithmetic, so this input-domain guard is essential. Final verification using the same native SUM is not the sole numeric oracle.
2. **Group membership stays complete.** Old/new changed files contribute a conservative union of typed keys. New input is filtered by a null-safe semi-join before aggregation. A removed final member still creates an affected key from the old file, so the old output/evidence group is removed even when there is no replacement row. Changed values or occurrence identity force replacement of the entire group.
3. **Global empty and global no-op are different.** A true zero-key aggregate always returns the intended empty-input summary. Incremental no-change applies LIMIT 0 to the replacement frame instead. Changed files create the global key even when the last input row disappears. The shared writer then removes the prior global row and writes the zero-count row; contributor evidence becomes empty. An unchanged empty source instead shares exact pins.
4. **Evidence denotes input membership, not only winning extrema.** Every contributing input occurrence belongs to its group, including null measure values. Native source rows retain the original values used by COUNT/SUM/MIN/MAX. For a joined input, evidence identifies the joined physical row; its retained source tuple and existing dependency graph resolve onward to original inputs without flattening transitive evidence into arrays.
5. **Aggregate state remains maintained and bounded by execution admission.** There is no Rust map of all groups/contributors in production. DataFusion owns grouped execution and spill through the existing shared Budget runtime. Application readers/writers retain their separate byte admissions; wide keys or many groups can still require spill or cause a refusal. Fixed-state aggregate functions do not make total group-key storage constant, especially for Utf8 keys. No exact resident-memory ceiling is inferred.
6. **Preparation and evidence remain separate useful recovery boundaries.** Each phase owns its source references, streams and completion. The new code adds no spawned producer or detached queue. The existing replacement writer's paired streams retain their capacity-one channel and reservations through completion/cancellation. Input manifest/worker custody and dependency-graph bounds continue to use the previously qualified helpers.
7. **No-op/reuse decisions retain source meaning.** Exact input files may be adopted under a new accepted version while coverage advances. Aggregate recipes, transform kind, input roles, native schemas, output eligibility and exact base manifest remain material guards. Equal aggregate values with different input occurrences still update evidence because applied delta keys include both removed and added groups.
8. **The optimization claim is reduced rewrites, not delta-proportional latency.** Global key discovery avoids per-file row decoding, but other validation/hashing and global recomputation remain. Grouped scoped views and output-file probes still scan broadly; evidence scope has the documented repeated summary-delta query. Independent verification reconstructs complete summaries and required joined ancestors. Existing partial counters and compressed-byte disk estimates do not become complete I/O or hard resource guarantees.

## Test behavior and current evidence

Owner-produced `docs/reviews/summary-correctness.log` passes the basic summary fixtures, and `summary-join-regression.log` passes the adapted existing join/incremental suite. The reviewer inspected their assertions. `summary-expanded-final.log` passes the expanded joined-input, composite-type and guard fixtures on the default stack. The final `summary-check.log` passes formatting, Clippy with warnings denied, and all-feature unit/integration/doc tests, including the numeric-limit proof and existing shared writer/recovery suites. `summary-no-default.log` records successful all-target compilation without default features; it reports feature-disabled test import/dead-code warnings. All summary logs, including failed attempts and the debugger trace, are retained under `docs/reviews/`. No reviewer tests were run.

| Test / assertion location | Behavior actually checked | Assessment |
| --- | --- | --- |
| `summaries_match_integer_oracle_with_duplicates_nulls_and_extreme_sums`, `tests/related_refresh/aggregation.rs:136`; oracle `:89` | Direct Rust i128 accumulation computes counts, sums and extrema; exact Decimal128(38,0) output is asserted; duplicates, null keys, all-null groups and repeated signed extremes are included. | Independent arithmetic/membership oracle, not two copies of the SQL generator. |
| `incremental_groups_move_delete_and_replace_equal_values_with_new_contributors`, `:169` | Equal aggregate values still yield new evidence and applied removals; group moves/final-member deletion match Rust and a full reconstruction. | Exercises evidence changes separately from visible value changes. |
| `grouped_and_global_empty_transitions_and_noop_pins_remain_exact`, `:235` | Empty→unchanged→nonempty→empty→unchanged; exact source/evidence pins and empty change tables on no-op. | Covers the global special branch and ordinary empty grouping behavior. |
| `summaries_share_unaffected_files_and_selected_computed_groups`, `:290` | Checks unaffected source/evidence file paths, digests and modification times, inherited scalar-group objects and exact no-op group pins. | Proves actual file/completed-group reuse, not a timing gain. |
| `summary_workers_resume_only_remaining_phases_after_sigkill`, `:373` | Real CLI termination at processing/evidence/readback handoffs; resumed work markers contain only missing phases; final source/evidence match the oracle. | Meaningful recovery coverage through existing transports/services. |
| `summary_refuses_changed_recipes_and_wrong_native_contributors`, `:407` | Changed incremental definition, colliding output names and unsupported SUM type refuse; rehashed same-valued foreign contributor table and wrong transform refuse. | Verifies typed recipe and evidence identity beyond checksums/counts. |
| `joined_summary_retains_catalog_free_transitive_occurrence_evidence`, `:486` | Hides the catalog; resolves summary contributor file/row to joined source tuples and compares recovered original pairs with the Rust join oracle. | Strong transitive evidence assertion; passes in the expanded suite. |
| `summary_coverage_guards_and_rebase_preserve_completed_work`, `:561` | Exercises stale base, rebase, captured/latest input movement, regression and invalidation. | The formerly aborting path passes on the default stack in the targeted transform-split and expanded-suite logs. |
| `quoted_utf8_boolean_composite_groups_and_int32_measures_refresh_exactly`, `tests/related_refresh/aggregation_types.rs:148`; oracle `:49` | Groups quoted Utf8/nullable Boolean keys, preserves Int32 extrema, checks i128 sums, moves group membership, verifies exact occurrence evidence and refuses floating/reserved keys. | Meaningful second type-domain oracle; passes in the expanded suite. |
| `qualified_row_limit_bounds_every_signed_partial_sum`, `src/engine/aggregation.rs:370` | Accepts the exact row limit, refuses the next count, and proves the worst signed partial-sum bound with checked arithmetic. | Direct SR-R3 regression passes in the unit gate. |

Inspected README (`README.md:308–349`) and STATUS (`docs/STATUS.md:611–655`) state the accepted type/range domain, joined-row count semantics, global-empty behavior, repeated evidence delta query, broad scans and lack of complete-path performance qualification. Existing shared-runtime spill-quota limitations remain unchanged. The passing gate establishes this implementation checkpoint, not large-corpus resource or throughput qualification.

## Conclusion

**VERDICT: APPROVE.** The implementation achieves the scoped summary behavior through maintained shared services; the identified resource/capability issues are closed by inspected code and passing owner-produced evidence.

- Coverage of changed paths: **ADEQUATE**, supported by the inspected expanded assertions and final regression gate.
- Confidence: **HIGH** for this bounded implementation scope.
- No whole-process memory, hard scratch/disk limit, throughput or complete lifecycle acceptance is claimed.
