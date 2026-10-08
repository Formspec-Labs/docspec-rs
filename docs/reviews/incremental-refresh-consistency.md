# Incremental native refresh: consistency review

Reviewed 2026-10-08 against `af329e9`, including the untracked incremental implementation and fixtures. This review applies the semi-formal code-review and applicable Microsoft Rust guidance. The reviewer inspected source, callers, maintained dependency implementations, assertions and owner-produced logs. The reviewer did not execute tests or edit implementation.

## Findings and disposition

No remaining consistency blocker was found in this checkpoint. The findings below were raised during implementation and are closed in source. The inspected full repository gate and subsequent strengthened computed-group fixture both pass; execution details appear below.

### F1 — Exact-base phase identity also needs an exact-base guard

**Severity:** WARNING, correctness. **Disposition:** CLOSED.

The first implementation included the base manifest in an incremental phase key but checked only the selected version ID when reusing the phase. Processing and evidence also reloaded the base by ID. Rebinding that ID to another manifest could therefore pass the early reuse guard. The existing accepted-succession checks provided an additional final-selection defense; the finding did not establish that an isolated result-row corruption could successfully select a wrong result.

The operation now captures the manifest before preparation (`src/engine.rs:609–625`), and `check_authority` compares it with the selected release (`src/engine.rs:1131–1139`). Both incremental processing and evidence check the reloaded base against that capture (`src/engine/incremental.rs:63–66,364–367,425–433`). Evidence additionally requires the processing change record to name the same manifest (`src/engine/incremental.rs:374–377`). The focused test preserves a processing phase, rebinds the selected manifest under the same ID, and requires `stale_base` from both planning and execution (`tests/related_refresh/incremental.rs:430–537`).

### F2 — Offline verification must enforce incremental provenance

**Severity:** BLOCKER, correctness. **Disposition:** CLOSED.

Originally the change verifier checked its own before manifest, while the composition verifier checked a separate parent manifest using only full-refresh rules. A valid full reconstruction with a changed recipe could be relabeled as incremental and supplied a mathematically valid remove-all/add-all change. Neither row equality nor valid endpoints alone established that incremental reuse was permitted.

The shared `incremental::qualify` now checks the exact parent version, unchanged output eligibility and definition, related parent, exact join recipe and role datasets, and equal native input schemas (`src/engine/incremental.rs:436–467`). Admission calls it before advancing the boundary (`src/engine.rs:597–606`); offline verification calls the same helper and requires `changes.base_manifest == parent_manifest` (`src/engine/backfill/verification.rs:145–164`). The fixture builds a legitimate changed-recipe full result, attaches a valid complete replacement delta, rehashes the relabeled manifest and requires refusal. A separate same-content manifest alias tests the exact parent binding (`tests/related_refresh/incremental.rs:540–616`).

### F3 — Pinned dynamic join filtering can discard valid null matches

**Severity:** BLOCKER, correctness in a maintained dependency. **Disposition:** CLOSED by a narrow configuration mitigation.

The failing fixtures exposed intermittent retained source rows after a null-key deletion and stale right-only output tuples. A subsequent run could pass without an algorithm change. The failure artifacts are retained in `docs/reviews/incremental-refresh-source-oracle.log`, `docs/reviews/incremental-refresh-empty-side-diagnostic.log` and the other incremental attempt logs.

The dependency source explains the failure. Paths below are relative to `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`:

- `datafusion-physical-plan-53.1.0/src/joins/hash_join/exec.rs:842–865` permits dynamic filtering for inner joins without checking `NullEquality`.
- `datafusion-physical-plan-53.1.0/src/joins/hash_join/shared_bounds.rs:146–167` constructs ordinary minimum/maximum comparisons, then combines them with membership using `AND` at `:419–435` and `:524–536`. Null-safe matching semantics are not supplied to that filter.
- The single-column membership path also uses ordinary `IN` semantics (`shared_bounds.rs:98–128`; `datafusion-physical-expr-53.1.0/src/expressions/in_list.rs:126–128`).
- `datafusion-datasource-parquet-53.1.0/src/opener.rs:294–301,804–814` reevaluates dynamic predicates during reading and can discard a batch after a filter update. This accounts for timing-dependent omissions.

`Budget::context` disables only join dynamic-filter pushdown (`src/budget.rs:96–110`). Ordinary joins and other pruning remain configured as before. The source-update helper now derives expected replacement/deletion values directly in Rust before comparing the native result, so a wrong source update cannot silently redefine the join oracle (`tests/related_refresh/incremental.rs:34–92`). The complete join-kind/null-policy fixture and source-intent checks pass with this mitigation. Re-enabling the optimization requires qualification of an upstream correction, including both bounds and membership behavior. No cost estimate is inferred from this correctness fix.

### O1 — Coverage and performance boundaries

**Severity:** OBSERVATION, test coverage and performance.

The strengthened sharing fixture selects a `lengths` scalar group before incremental refresh, verifies that unchanged computed Parquet object references remain shared, and requires the exact prior computed-group pin after a no-op (`tests/related_refresh/incremental.rs:231–247,308–314,338–341`). Its separate execution passes. The changed native-schema refusal is statically traced through the shared qualification helper; the new focused refusal fixture changes the recipe rather than the input schema (`tests/related_refresh/incremental.rs:359–383`).

The fixtures prove file sharing and exact output/evidence reuse, not delta-proportional runtime or total I/O. Discovery expands affected keys through old and new source scans; validation and readback still reconstruct complete target joins (`src/engine/incremental.rs:118–193`; `src/engine/refresh.rs:597–659`). The documentation states the missing counters, broader scans, unmeasured optimization cost and remaining lifecycle work (`docs/STATUS.md:571–606`).

## Patch summary

The patch adds explicit `RefreshMode::Incremental`, retaining full reconstruction as the default (`src/model.rs:217–243`). It discovers changed immutable source files, collects their old/new keys, recomputes complete affected match groups, and reuses the shared replacement writer for source values and contributor evidence. Applied changes retain exact before/after pins and actual removed/added rows. The same lifecycle services retain recovery, guarded selection and independently readable history.

The format and processor advance to version 7 (`src/engine.rs:80–81`). This is a greenfield format change, not a migration implementation. Backfill operation constructors explicitly leave the new incremental-only base-manifest field absent (`src/engine/backfill.rs:187,349`). CLI and HTTP continue decoding the existing refresh request and invoking the same service; no second incremental transport implementation is introduced.

## Function trace

| Function or type | File:line | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| `RefreshMode`, `RefreshSpec` | `src/model.rs:217` | Serialized work choice → request | Defaults to full; incremental is explicit. |
| `plan_request` | `src/engine.rs:159` | Request and lifecycle view → required/reusable phases | Rejects conflicting operation identity; qualifies unfinished incremental work before mutation; uses captured exact base for phase lookup. |
| `prepare_and_select` | `src/engine.rs:568` | Request → selected release | Captures base manifest with operation; dispatches incremental processing/evidence; retains common validation, groups, readback and selection. |
| `manifest_ref`, `base_manifest` | `src/engine/incremental.rs:8,16` | Authority and request → optional manifest | Applies the additional capture only to incremental refresh. |
| `check_incremental`, `qualify` | `src/engine/incremental.rs:32,436` | Request, selected parent, eligibility, budget → qualification/refusal | Uses one implementation for live admission and offline provenance checks. |
| `process_refresh` | `src/engine/refresh.rs:81` | Request and admitted coverage → prepared source | Dispatches incremental mode; full mode retains the complete join path. |
| `process_incremental` | `src/engine/incremental.rs:52` | Exact base, coverage, operation → source, changes, metrics | Expands old/new key groups, derives old tuple scope, computes scoped new join, applies replacement. |
| `changed_keys` | `src/engine/incremental.rs:239` | Old/new native source pins → typed key table | Compares live immutable file descriptors and object references; reads only removed/added files for key extraction. |
| `OccurrenceScan::read_file`, `check_task` | `src/storage/occurrences.rs:257,281` | Captured whole-file task → located batches | Requires exact source membership and complete unfiltered Parquet tasks; rejects deletes, splits and predicates. |
| `join_frame`, `join_registered` | `src/engine/refresh.rs:429,448` | Full/scoped occurrence providers → joined rows | Shares field/type checks, declared null behavior, cardinality preflight and contributor tuple construction. |
| `process_update` | `src/engine/write.rs:112` | Raw replacement/deletion input → prepared source | Retains source-update overlap/schema rules, then invokes the same replacement primitive. |
| `replace_keys` | `src/engine/write.rs:255` | Before pin, explicit distinct key scope, new rows → applied pins | Verifies before bytes; shares unaffected files; splits rewritten batches into kept/removed rows; appends actual new rows. |
| `key_match`, `any_rows`, `split_rows` | `src/engine/write.rs:484,501,505` | Key columns/queries/batches → matches, kept and removed batches | Parenthesizes each null-safe component; existence probes use the corrected shared query configuration. |
| `prepare_evidence`, `incremental_evidence` | `src/engine/refresh.rs:107`; `src/engine/incremental.rs:357` | Processing delta and exact base → retained contributor table | Removes old affected tuples and adds actual new tuples through `replace_keys`; preserves exact evidence pin on no-op. |
| `phase_key`, `fingerprint` | `src/engine/refresh.rs:338`; `src/engine.rs:1212` | Request/base/eligibility → stable identities | Mode is part of request identity; incremental phase identity also binds exact base manifest, inputs, recipe and definition. |
| `phase`, `complete`, `check_authority` | `src/engine.rs:974,1045,1121` | Captured operation → reuse/completion/refusal | Reuse and promotion require current captured authority; acknowledgement retains existing epoch-only completion semantics. |
| `verify_changes` | `src/engine/changes.rs:6` | Request, after pin, delta → independent validation | Checks endpoints, native schema and key IDs, removed membership and duplicate-aware reconstruction in one aggregate. |
| `verify_local_release`, `verify_groups` | `src/engine.rs:1496`; `src/engine/backfill/verification.rs:6` | Decoded manifest → local consistency | Requires change presence by operation/mode; binds incremental delta to composition parent and shared qualification rules. |
| `verify_related_with_inputs`, `equal_multisets` | `src/engine/refresh.rs:597,682` | Exact input releases/source/evidence → full validation | Reconstructs complete typed matches and checks values, tuple multiplicities, evidence schema and cardinality. Errors identify the failed comparison. |
| `select_refresh`, `guard_inputs` | `src/engine/refresh.rs:156,219` | Candidate and current authority → accepted result/progress | Retains sorted dependency locks, current eligibility/seals, exact output base, accepted input succession, and captured/latest policy. |
| `Budget::context` | `src/budget.rs:96` | Shared runtime and limits → query session | Disables the qualified unsafe join dynamic-filter optimization for all callers. |

## Data flow and invariants

1. **An incremental phase belongs to one exact base.** The manifest is captured with the operation, enters incremental phase identities, and is rechecked before reuse and final selection. A differently based operation cannot adopt an old delta merely because target inputs and recipe match (`src/engine.rs:609–625,1131–1139`; `src/engine/refresh.rs:344–355`). A completed operation still reconciles its own historical result before stale-work guards, preserving existing retry semantics.

2. **Every changed occurrence contributes its old and new key.** `changed_keys` compares live native file paths and exact descriptors/object references; a path with changed immutable identity refuses. Different paths remain different physical occurrences even with equal contents. Keys are extracted before relational filtering through the existing source-pinned occurrence reader (`src/engine/incremental.rs:247–352`). Discovery tables stay inside the protected processing attempt, not a new authoritative index.

3. **Affected means the complete key group on both sides.** Distinct old/new keys from both producers form one union. Null-safe semi-joins expand that set through all old and new rows, including partial-null keys when the actual join uses ordinary equality. Old output tuples qualify if either present contributor is in an old affected group. New matches use the original join/null/cardinality rules (`src/engine/incremental.rs:125–193`). This covers an old unmatched row acquiring its first partner, a matched row losing its last partner, key moves and duplicate fanout. Null-safe discovery does not change the declared join's null behavior.

4. **Sharing preserves bytes, while replacement records actual writes.** The writer hashes the before inventory before decoding, prepares the distinct scope once, probes source files, and copies only affected-file kept rows into new files. The same decode produces actual removed rows. Added files feed both the new source and added-row pin with inherited ownership (`src/engine/write.rs:265–340,379–478`). Caller scopes are distinct in every current call site. No removed/added rows means the exact prior source pin is returned (`:459–465`).

5. **Evidence follows applied contributor tuples.** Incremental evidence uses distinct tuples from actual removed rows and tuples from actual added rows. It shares unaffected evidence files and adopts the exact old evidence pin when the applied change is empty (`src/engine/incremental.rs:357–415`). Full independent comparison still establishes evidence equality with the selected result (`src/engine/refresh.rs:643–659`).

6. **The change certificate proves membership and reconstruction.** For each complete typed row, the verifier computes `remaining = before - removed` and `delta = before - removed + added - after`. It refuses negative remaining multiplicity or nonzero delta. Positional aliases prevent arbitrary source field names from colliding with helper columns (`src/engine/changes.rs:88–141`). Before/source/change tables must have identical native schemas and the declared native key IDs. The old source is verified shallowly; this does not recursively replay its historical transformations.

7. **Prepared work does not itself select a result.** The existing lifecycle still separates acknowledged preparation from eligibility to continue or select. The final dependency lock set and accepted-succession checks remain in `select_refresh`; captures may lag newer admitted inputs under the declared captured policy, whereas latest requires exact observed input heads (`src/engine/refresh.rs:156–201,219–260`). No new independent table-head authority is added.

8. **Limits remain explicit.** Native whole-file occurrence support, equal input schemas and unchanged recipe are prerequisites, not silent fallback transformations. The metadata maps include retained object inventory lengths in their allowance (`src/engine/incremental.rs:252–266`). Unknown join fanout does not use source bytes as an output upper bound; the shared writer checks available disk while batches arrive (`src/engine/write.rs:414–434`). Hard process-memory, total-I/O and representative throughput claims remain outside this review.

## Test behavior and execution evidence

The assertions below were read directly. The owner-produced `docs/reviews/incremental-refresh-qualified-tests.log` records success for the focused incremental suite after the dynamic-filter mitigation. The final `./scripts/check` completed with exit 0, as reported by the owner and corroborated by the inspected `docs/reviews/incremental-refresh-check.log`: formatting, Clippy with warnings denied and all-feature tests passed, including the final one-aggregate change verifier and the existing update suites. A later test-only addition strengthened computed-group coverage; `docs/reviews/incremental-refresh-computed-groups.log` separately records that fixture passing against the final production implementation. The all-targets no-default-features check also completed with exit 0; `docs/reviews/incremental-refresh-no-default.log` retains unused test-import/helper warnings with `test-util` disabled. That build passed but was not warning-free; the all-feature Clippy gate was clean.

| Test | Location | What its assertions establish |
| --- | --- | --- |
| `incremental_matches_oracle_across_key_moves_nulls_duplicates_and_both_input_orders` | `tests/related_refresh/incremental.rs:124` | Every join kind and null policy follows direct Rust source-update and complete-match reconstruction through first/last partners, moved keys, duplicate fanout, partial nulls and an empty right side. |
| `incremental_shares_unaffected_output_and_evidence_and_adopts_exact_noop_pins` | `:227` | Original unaffected source/evidence files retain their digests and modification times; unchanged computed files remain shared. A new no-op producer version advances coverage with exact prior source/evidence/computed pins and empty changes. |
| `incremental_refuses_unqualified_recipes_before_admission_and_binds_mode` | `:359` | Changed recipe refuses without any lifecycle mutation; missing base refuses; changing a completed operation's mode conflicts. |
| `incremental_killed_workers_reuse_processing_evidence_and_readback` | `:387` | Real CLI workers are killed after named durable handoffs; resume reports exactly the expected remaining work, preserves phase records, matches the independent oracle and reconciles its completed result. |
| `incremental_phase_reuse_fences_rebased_outputs_and_exact_manifests` | `:430` | Mode changes conflict before completion; a new base requires new processing and retains distinct deltas; same-ID manifest rebinding refuses in both plan and run. |
| `incremental_verifier_refuses_balanced_foreign_removals_and_unsupported_provenance` | `:540` | Foreign removed/added rows that cancel algebraically still fail membership; rehashed false incremental provenance and a different parent manifest fail independently. |
| `incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile` | `:619` | Disk refusal preserves admission and the old selection; reopening with a smaller declared scratch allowance resumes and keeps the no-op source pin. |

The direct matcher and contributor checks are in `tests/related_refresh.rs:201–296`; they compare exact file/ordinal pairs, duplicate values, nulls and evidence using the public read paths. The new source helper at `tests/related_refresh/incremental.rs:34–92` separately checks request intent. Thus correct derived results are not inferred solely from two SQL implementations agreeing on a corrupted producer result.

The shared writer also remains covered by existing source-update assertions, including composite nullable quoted keys, nested values/native IDs, endpoint forgeries, competing operations and killed-worker recovery (`tests/updates.rs:184,476,550,580,763`). Those suites passed in the inspected full gate. Named post-completion kills do not establish arbitrary interruption during every native writer instruction or sudden power-loss durability.

## Conclusion

**VERDICT: APPROVE** for the static consistency of this local incremental equality-join checkpoint.

F1 and F2 are closed by exact guards and shared live/offline qualification. F3 is closed by disabling the specific unsafe dependency optimization, with retained failure evidence and passing source-intent/join oracle tests. The shared replacement primitive removes duplication between raw updates and derived refresh; unchanged-file fixtures demonstrate the intended reduction in rewriting. The one-aggregate verifier avoids a second before/removed scan by construction, without asserting a measured speedup.

Coverage is **ADEQUATE** for the stated checkpoint, with the explicit execution boundaries above. Confidence is **HIGH** in the static reasoning and inspected targeted/full-gate results. This approval does not certify the complete R5 requirement, general incremental SQL, maintenance/restore, remote publication or the 10M performance target.
