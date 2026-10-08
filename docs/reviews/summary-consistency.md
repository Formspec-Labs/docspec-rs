# Native summaries: consistency review

Reviewed 2026-10-08 against `67a2c07`, including the untracked aggregation implementation and fixtures. This review applies the semi-formal code-review and applicable Microsoft Rust guidance. The reviewer inspected the changed source, important callers and callees, maintained aggregate implementation, assertions and owner-produced logs. The reviewer did not execute tests or edit implementation.

## Findings and disposition

No remaining consistency blocker was found in this checkpoint. The observed default-stack failure and the key-type coverage gap are closed. The expanded summary suite, full repository gate and no-default-features build all pass; inspected execution evidence appears below.

### F1 — Expanded recovery fixture exceeds the default stack

**Severity:** BLOCKER, runtime correctness. **Disposition:** CLOSED in source and focused default-stack execution.

The initial summary fixtures passed, but `summary_coverage_guards_and_rebase_preserve_completed_work` aborted while preparing an incremental summary (`tests/related_refresh/aggregation.rs:561`). The inspected `summary-stack-trace.log` locates the failure in maintained SQL parsing called by `write::any_rows`, beneath `replace_keys`, `process_aggregation`, `process_incremental` and the common lifecycle. Boxing alone did not resolve it; those failed attempts are retained. The final change separates join-specific preparation from shared loading and transform dispatch (`src/engine/incremental.rs:52–105`), so the summary path no longer carries the join preparation body. `summary-stack-transform-split.log` records the complete guard fixture passing on the normal stack. No stack-size increase is used.

### F2 — Qualify the advertised non-Int64 and composite key paths

**Severity:** WARNING, test coverage. **Disposition:** CLOSED in assertions and expanded default-stack execution.

The implementation accepts Boolean, Int32, Int64 and Utf8 grouping keys, and signed Int32/Int64 aggregate operands (`src/engine/aggregation.rs:91–113`). The initial independent oracle exercises single Int64 keys and Int64 operands (`tests/related_refresh/aggregation.rs:9–133`). The added `quoted_utf8_boolean_composite_groups_and_int32_measures_refresh_exactly` fixture covers a quoted Utf8 plus nullable Boolean composite key, Int32 operands, all-null versus zero, group moves and float/reserved-key refusal (`tests/related_refresh/aggregation_types.rs:148–258`). Its direct Rust oracle checks typed output values and exact contributor multiplicity (`:49–145`). The inspected `docs/reviews/summary-expanded-final.log` records that fixture and the complete expanded summary set passing on the default stack.

### Observations

- Exact integer sums have a concrete bound, rather than relying on the dependency to detect overflow. DataFusion 53.1 uses wrapping addition for its primitive group accumulator and ordinary accumulator (`datafusion-functions-aggregate-53.1.0/src/sum.rs:288–300,377–381`, under the local Cargo registry). Casting each signed operand to Decimal128(38,0) before summation and limiting verified occurrences to `i64::MAX` bounds every partial sum's magnitude by `2^63 × (2^63−1) < 10^38` (`src/engine/aggregation.rs:75–81,102–114,131–141,355–364`). No custom accumulator is necessary for this supported domain.
- The new operation reuses the existing lifecycle, exact base, replacement writer, dependency graph and transports. It adds no second journal, scheduler or mutable coverage cursor. File reuse is demonstrated by fixtures; it does not establish delta-proportional latency or the full performance target.
- The older README scope statements were corrected to agree with the new summary section. Historical checkpoint sections in `docs/STATUS.md` remain historical; its current capability table and summary checkpoint describe the implemented scope.

## Patch summary

The patch changes `RefreshSpec` to carry an explicit `RefreshTransform`, with typed equality-join and summary variants (`src/model.rs:228–330`). Full summaries produce group keys and named count/sum/min/max values from one exact native source. Incremental summaries discover keys in changed files, recompute complete affected groups, and use the shared replacement writer. A separate native table retains each contributing input file/row occurrence and its group keys. Empty global input has one result and no contributors; empty grouped input has neither.

The manifest/processor version advances to 8 (`src/engine.rs:81–82`). The changed API is deliberately greenfield; no migration for earlier experimental manifests is claimed. Existing join fixtures are adapted to the typed variant and remain part of the required regression gate.

## Function trace

| Function or method | Source | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `RefreshTransform::{versions,same_roles,same_definition}` / `RefreshSpec::inputs` | `src/model.rs:280–330` | Typed recipe → ordered roles, comparisons and exact input map | Variant and ordered datasets remain distinct even when a self-join and a summary have the same deduplicated input map. |
| `fingerprint` / refresh `phase_key` | `src/engine.rs:1220–1228`; `src/engine/refresh.rs:371–394` | Request and eligibility → stable identities | Request includes the transform; incremental phases also bind mode and exact base manifest. Full admission reuse intentionally binds verified inputs, with transform qualification repeated before computation. |
| `validate` / aggregation `validate` | `src/engine/refresh.rs:284–342`; `src/engine/aggregation.rs:10–34` | Request → accepted shape or refusal | External inputs, distinct output identity, nonempty aggregate list, quoted names, unique output names and reserved-prefix refusal. |
| `register_input` / `qualify` / `check_row_count` | `src/engine/aggregation.rs:65–116,355–364` | Exact input release and definition → native occurrence relation | Exact reference, verified row bound, allowed key/value types and field existence. |
| `aggregate_frame` / `evidence_frame` | `src/engine/aggregation.rs:117–177` | Native occurrences → summary values / complete contributor rows | Widened sums, SQL null/count rules, hidden global constant, and one evidence row for every source occurrence including null operands. |
| `admit_refresh` / `process_refresh` | `src/engine/refresh.rs:46–109` | Captured inputs → durable admission / prepared source | Uses existing dependency verification and full/incremental dispatch. Aggregate qualification occurs before acknowledging admission completion. |
| `check_incremental` / `qualify` | `src/engine/incremental.rs:32–49,489–521` | Selected parent and target → qualification or `full_refresh_required` | Same exact parent version, output eligibility/definition, transform variant, ordered role datasets, grouping recipe and native input schema. |
| `process_incremental` / `process_incremental_join` | `src/engine/incremental.rs:52–263` | Captured coverage and operation → variant preparation | Shared exact base/load/hash checks precede boxed dispatch; the join algorithm is isolated from the summary future without changing its key/tuple processing. |
| `changed_keys` | `src/engine/incremental.rs:267–402` | Old/new native file inventories → typed affected keys | Rejects a reused path with different immutable identity. Reads keys from removed/added files; the global branch emits one constant only if file membership changes. |
| `process_aggregation` | `src/engine/aggregation.rs:180–285` | Exact base, old/new inputs → prepared summary and applied change | Null-safe complete new groups; global no-op suppression; native key IDs; exact before/after and base bindings. |
| `replace_keys` | `src/engine/write.rs:255–477` | Distinct scope and replacement rows → candidate plus actual removed/added tables | Keeps unrelated files; deliberately rewrites every affected group even when values match; retains exact prior pin only when both applied row counts are zero. |
| `prepare_evidence` / `aggregate_evidence` | `src/engine/refresh.rs:111–175`; `src/engine/aggregation.rs:286–346` | Full input or applied summary changes → durable evidence table | Full source occurrences or the union of removed/added group keys; independently removes and replaces contributors through the shared writer. |
| `select_refresh` / `guard_inputs` | `src/engine/refresh.rs:177–222,239–280` | Prepared release and current authority → atomic selected release | Shared sorted locks, exact base, transform role continuity, current eligibility, accepted forward coverage, generation seals and operation-scoped Latest policy. |
| `verify_groups` | `src/engine/backfill/verification.rs:119–164,203–220` | Immutable manifest and exact parent → composition validation | Related request/coverage agreement, role continuity, incremental provenance, exact change-parent binding and preserved scalar definitions. |
| `verify_related_with_inputs` | `src/engine/refresh.rs:621–686` | Independently verified input releases → complete reconstruction | Validates recipe, exact native source/evidence schemas and typed value/contributor multisets. Summary evidence is reconstructed from the input, not inferred from aggregate equality. |
| `verify_changes` | `src/engine/changes.rs:18–141` | Summary change, before and after → verified delta | Transform-specific native key IDs, exact endpoints, removed-row membership and `before − removed + added = after`. |
| `read_combined` / `compare` | `src/engine/backfill.rs:88–135`; `src/engine.rs:384–413` | Verified releases → public values / multiset difference | Shared hidden-field selection removes the synthetic global key while retaining user aggregate values. Native/evidence reads preserve physical matching fields. |

## Data flow and invariants

1. **Exact transform identity.** The typed transform is created in the public request and remains in the request fingerprint, prepared material identity and immutable `RelatedResult`. Deduplicated input coverage never substitutes for the ordered role check. The same incremental qualification is used by runtime admission and offline composition verification. A Full result cannot be relabeled Incremental merely by attaching a valid arithmetic delta.
2. **Complete affected groups.** Old/new immutable file membership determines a conservative set of changed keys. Current source occurrences are selected using null-safe key membership before complete aggregation (`src/engine/aggregation.rs:197–245`). This covers removed final members, moves and new groups. A wide affected group may require most or all of an input scan; no bounded latency claim follows.
3. **Evidence changes independently of values.** `replace_keys` emits removed/added summary rows for deliberately replaced groups even when their values are equal (`src/engine/write.rs:335–337,414–465`). Their key union controls contributor replacement (`src/engine/aggregation.rs:297–340`). Dropping equal-value rows from the applied change in a future optimization would break this dependency and is not currently done.
4. **Global empty and no-op are separate.** Initial empty global aggregation uses SQL aggregate semantics, producing a singleton count-zero/null-measure result. Incremental global preparation suppresses that synthetic result only when changed-file discovery is empty (`src/engine/aggregation.rs:229–236`). Removing the final input file therefore replaces the singleton and removes all contributors; a true no-op preserves exact source and evidence pins.
5. **Numeric exactness.** The allowed signed operands and verified occurrence bound apply before arithmetic. Complete-group recomputation avoids subtraction or arithmetic delta accumulation. Null operands remain contributors although they do not increment `Count` or alter Sum/Min/Max.
6. **Preparation survives without granting authority.** Existing durable phases and protections remain the recovery boundary (`src/engine.rs:649–652,1024–1043,1046–1073`). Completion and selection retain their separate guards. The summary extension does not introduce an independent mutable head or cursor.
7. **Transitive lineage stays source-pinned.** Evidence points to exact input file/row occurrences. For a summary of a join, those occurrences resolve to the join's source tuples. The bounded semantic dependency graph independently verifies children before parents (`src/engine/verification.rs:122–181`); it does not create a general public row-lineage traversal service.

## Test behavior and execution evidence

Assertions read in `tests/related_refresh/aggregation.rs`:

| Fixture | Source | What it establishes |
| --- | --- | --- |
| `summaries_match_integer_oracle_with_duplicates_nulls_and_extreme_sums` | `:136` | Rust i128 oracle, duplicate and null contributions, sums beyond i64, exact Decimal128 schema, grouped/global forms and completed retry identity. |
| `incremental_groups_move_delete_and_replace_equal_values_with_new_contributors` | `:169` | Complete key moves/deletion, equal summary values with different evidence, full rebuild equality and exact contributor multiset. |
| `grouped_and_global_empty_transitions_and_noop_pins_remain_exact` | `:235` | Empty grouped/global creation, add/remove-last transitions and exact no-op source/evidence pins. |
| `summaries_share_unaffected_files_and_selected_computed_groups` | `:290` | Unaffected source/evidence objects keep bytes and modification times; scalar files survive and a no-op retains all selected group pins. |
| `summary_workers_resume_only_remaining_phases_after_sigkill` | `:373` | Real child termination after processing/evidence/readback; expected missing-work markers and independent values/evidence after resume. |
| `summary_refuses_changed_recipes_and_wrong_native_contributors` | `:407` | Changed incremental recipe, invalid names/type, independently rehashed foreign evidence and wrong transform refusal. |
| `joined_summary_retains_catalog_free_transitive_occurrence_evidence` | `:486` | Join fanout and unmatched inputs; catalog-hidden summary → joined occurrence → original source tuple resolution against the Rust join oracle. |
| `summary_coverage_guards_and_rebase_preserve_completed_work` | `:561` | Competing selection, rebase phase isolation, Latest refusal, Captured forward progress, backward refusal and source invalidation. The focused default-stack rerun reaches every assertion. |
| `quoted_utf8_boolean_composite_groups_and_int32_measures_refresh_exactly` | `tests/related_refresh/aggregation_types.rs:148` | Independent typed values and occurrence evidence for quoted/composite/null keys, Int32 measures and moved groups; explicit unsupported-key refusal. |
| `qualified_row_limit_bounds_every_signed_partial_sum` | `src/engine/aggregation.rs:370` | Exact acceptance/refusal boundary and checked worst-case magnitude below decimal precision 38. |

The logs are retained under `docs/reviews/`. The inspected initial `summary-correctness.log` reports the initial summary fixtures passing. `summary-correctness-expanded.log`, `summary-stack-diagnostic.log`, `summary-stack-boundary-fix.log` and `summary-stack-trace.log` retain the expanded default-stack failure. `summary-stack-transform-split.log` records the focused guard fixture passing after the structural fix. `summary-expanded-final.log` records all nine summary/type fixtures passing, including catalog-hidden transitive evidence and the guard path.

The owner ran `./scripts/check` to successful completion, with retained evidence in `summary-check.log`. The script checks formatting, runs locked all-target/all-feature Clippy with warnings denied, and runs the locked all-feature test suite (`scripts/check:1–6`). The inspected log includes the arithmetic bound unit test, all summary/type fixtures, the existing join/incremental matrix, and the lifecycle, update, backfill, rebuild, transport, dependency and succession suites. All finish successfully. The later `cargo check --locked --all-targets --no-default-features` also completes successfully in `summary-no-default.log`; it reports unused test imports/helpers when `test-util` is disabled. That build is successful, not warning-free. The all-feature Clippy gate is clean. No reviewer execution is represented as owner execution or vice versa.

These small fixtures establish semantic and named-handoff behavior. They do not establish arbitrary-instruction process termination, sudden power-loss durability, resource bounds for large cardinality, remote publication, maintenance/restore or the 10M performance target. Full verification also uses the same maintained relational engine as construction; the Rust fixture oracles provide independent value/contributor calculations for the cases they exercise.

## Conclusion

**VERDICT: APPROVE** for the static consistency of this local grouped/global summary checkpoint.

The reviewed algorithm satisfies the stated summary design: one typed transform and existing lifecycle, complete-group replacement, exact bounded integer arithmetic, separate contributors and independent retained reconstruction. The observed runtime abort is closed in focused and full normal-stack execution. Coverage is **ADEQUATE** for the stated checkpoint, with the explicit limits above. Confidence is **HIGH** in the static reasoning and inspected targeted/full-gate evidence. This approval does not certify the complete R5 requirement, general aggregate SQL, maintenance/restore, remote publication or the 10M performance target.
