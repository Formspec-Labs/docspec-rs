# Coordinated scalar refresh consistency review

Scope: the uncommitted checkpoint after `9e10497`, reviewed statically on
2026-10-08. The reviewer used the semi-formal code-review and Microsoft Rust
guidance, read the changed implementations and their callers, and ran no tests.
Only this report was edited. This is a review of scalar groups refreshed by an
exact-base source update, not full R5, arbitrary joins or complete lineage.

## Findings

All findings below are closed in the reviewed working tree. No unresolved
consistency blocker remains for the scoped scalar refresh.

### F1 — An update must not adopt an invalidated source transformation

- **Severity:** BLOCKER; lifecycle correctness. **Status: CLOSED.**
- **Initial location:** `src/engine.rs:542-568`, `:720-745`;
  `src/engine/write.rs:91-103`, `:234-258`.
- **Evidence chain:** The new path permits updates after groups are selected and
  recomputes their scalar values, but source update reuses the already shaped base
  source. Initially, admission compared request definition and selected version
  without comparing the selected result's qualification eligibility.
- **Failure sequence:** Select source and groups at eligibility 0; invalidate a
  relevant dependency or source definition; start an update at eligibility 1.
  Untouched shaped source files are adopted while only groups are recomputed.
  That does not requalify the original source transformation.
- **Correction:** Require the exact selected base's eligibility to match authority
  before incremental admission and in planning. Preserve historical completed
  operation reconciliation. Explicit source requalification remains outside this
  slice.
- **Closure:** `check_update_eligibility` at `src/engine.rs:1046` is called in
  planning (`:185`) and before source admission (`:546`). The exact-base check
  runs first. Completed operation reconciliation still returns its historical
  result before the new guard. `tests/refresh.rs:410` checks both refusal paths
  and equality of the entire lifecycle record map before and after refusal.

### F2 — Shallow update verification must preserve parent evidence and interpretation

- **Severity:** BLOCKER; independent verification. **Status: CLOSED.**
- **Initial location:** `src/engine/backfill/verification.rs:18-38`;
  `src/engine.rs:1315-1345`.
- **Evidence chain:** Initial update verification checked parent identity and
  group definitions, but only the current update's declared input digests were
  required in the result. It did not require inherited inputs to match the parent.
  A new serialized manifest could omit old retained input evidence and still pass;
  deleting those omitted objects also escaped verification.
- **Correction:** Require exact parent inputs plus the current request's declared
  admitted inputs, without dropped, rebound or extra entries. Check equal parent
  eligibility and unchanged source definition, and require the update boundary to
  advance beyond its parent. A forged boundary regression or changed definition
  cannot describe an operation this implementation is allowed to perform. These
  checks need only the immediate parent, not recursive history traversal.
- **Closure:** `src/engine/backfill/verification.rs:24-51` removes exactly the
  current declared input entries and compares the remainder to the immediate
  parent, alongside eligibility, boundary, definition and group recipe checks.
  `tests/refresh.rs:338` writes new digest-valid manifests with omitted/rebound
  inputs, missing groups/parent, changed recipes/requests/definition and a stale
  boundary; each must fail independent verification.

### F3 — Target promotion and independent verification disagree after a no-op reselection

- **Severity:** WARNING; concurrency and refusal semantics. **Status: CLOSED.**
- **Initial location:** `src/engine/backfill.rs:538-546`;
  `src/engine/backfill/verification.rs:67-70`.
- **Evidence chain:** Whole-group reuse can preserve a group's native `TablePin`
  while a new backfill records a new adoption request. Promotion initially compared
  only native metadata against `Capture.target`. Independent verification also
  compared the complete target binding in the actual parent with the captured
  base's target binding.
- **Failure sequence:** A prepares a replacement against target T. B reselects T's
  unchanged recipe and reuses its physical pin. A's planning/promotion guard sees
  unchanged metadata, but composed readback fails `invalid_group` because B changed
  the adoption binding. The selection remains safe, but a normal stale-target
  conflict is misreported as corrupt group data after unnecessary work.
- **Correction:** Use the same exact target-selection predicate in plan,
  promotion and verification, or explicitly define consistent material-only
  no-op semantics. Add a no-op same-group reselection racing a pending replacement.
- **Closure:** `Capture.target_binding` at `src/engine/backfill.rs:22` records the
  full expected adoption binding. Both planning and execution use `promotion`,
  whose target comparison at `:541-546` includes that binding. The fixture at
  `tests/refresh.rs:463` proves identical physical pins but changed adoption
  records produce `stale_group` in both paths. Its subsequent update/backfill
  also checks exact inherited siblings and rejects forged sibling composition.

## Patch summary

An update now prepares a candidate source and refreshes every existing scalar
group before one selected release changes. `GroupDefinition` separates material
expressions and logical field IDs from the operation adopting their results. The
shared `prepare_group` serves both explicit backfill and coordinated update,
retaining file work, assembled groups and group validation. Format 4 adds an
exact immediate parent manifest so independent verification can check sibling-
inclusive composition without walking ancestor history.

## Function trace

Initial inspected locations are recorded below; closure references identify the
final working-tree locations when they change.

| Function or type | Location | Inputs and outputs | Inspected behavior |
| --- | --- | --- | --- |
| `DatasetPin`, `GroupDefinition`, `ComputedGroup` | `src/model.rs:73`, `:197`, `:216` | Serialized selection, material recipe and adoption binding | Parent points to exact prior composition; recipe remains independent of request identity. |
| `BackfillSpec::group_definition` | `src/model.rs:205` | Backfill request → material recipe | Preserves group, fields and processing definition; excludes source version and operation identity. |
| `plan_request` | `src/engine.rs:163` | Typed request → reusable/required phases | Adds group progress for enriched update bases while preserving stable completed-operation behavior. |
| `prepare_and_select` | `src/engine.rs:515` | Exact-base update → one verified combined release | Source phases complete first; prepares every inherited recipe, records group progress, constructs parent binding, verifies and selects under existing authority guards. |
| `check_authority`, `check_base`, definition/eligibility checks | `src/engine.rs:1015-1069` | Current authority and operation → refusal or permission | Exact selected version prevents old source updates from replacing sibling changes; F1 adds the source qualification fence. |
| `process_update` | `src/engine/write.rs:85` | Base source and admitted replacement/deletion rows → candidate and actual changes | Existing source update implementation remains; blanket group refusal is removed because the caller now refreshes all groups. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Operation, source pin, recipe → verified native group and field map | Material preparation key binds processor, source metadata, recipe and eligibility; file keys still bind exact objects and native schema. |
| `reserve_group`, `acknowledge_files`, `compute_file` | `src/engine/backfill/preparation.rs:180`, `:218`, `:247` | Captured work → protected attempts and retained file outputs | Same qualified reader/writer path as backfill; persistence checks execution epoch while final promotion checks mutable authority. |
| `prepare_backfill` and `promotion` | `src/engine/backfill.rs:288`, `:508` | Captured source/target → sibling-preserving release | Uses shared preparation, selects the actual current release as parent, verifies that combination and retries composition after compatible siblings. F3 concerns target predicate alignment. |
| `validate_definition`, `Expressions::compile` | `src/engine/backfill.rs:55`; `src/engine/backfill/expressions.rs:26` | Recipe and native source schema → row-local physical expressions | Retains the prior deterministic scalar/type/field-ID restrictions with the new material recipe type. |
| `read_combined`, `check_names` | `src/engine/backfill.rs:87`, `:635` | Verified groups → combined stream | Display columns now come from material definitions; locator joins and output admission are unchanged. |
| `release_objects` | `src/engine.rs:1295` | Release → exact retained references | Includes parent, inputs, tables, group bases and change dependencies; shared availability checking replaces duplicate release-reference collection. |
| `verify_release_with_budget`, `verify_changes` | `src/engine.rs:1325`, `:1376` | Serialized release → independent verification | Requires format 4 and current request identity, checks source/change endpoints and delegates exact group composition. |
| `verify_groups` | `src/engine/backfill/verification.rs:6` | Current dataset and immediate parent → composition check | Builds cannot silently contain groups; updates preserve group recipes/membership; backfills preserve exact siblings and expected target. F2 identifies missing parent invariants. |
| `read_manifest`, `read_base`, `verify_group`, `verify_group_data` | `src/engine/backfill/verification.rs:134`, `:149`, `:157`, `:200` | Exact retained metadata and group → bindings/schema/coverage | Reads immediate records only, verifies actual adoption kind and source metadata, then reuses independent native coverage checks. |

## Data flow and invariants

| Invariant | Creation, mutation and use | Assessment |
| --- | --- | --- |
| One coherent source/group selection | Candidate source and groups remain local until final release commit | Every inherited group is prepared before selection; native heads do not become dataset authority. |
| Exact-base source update | Registered operation captures the selected version; common phase and final checks re-evaluate it | A sibling or competing source selection cannot be overwritten by an older captured update. |
| File reuse preserves physical occurrence identity | File key binds exact `ObjectRef`, full native schema and recipe plus eligibility | Unchanged source objects can reuse group outputs; rewritten files compute fresh locators and values. |
| Whole-group reuse is operation-independent | Preparation digest uses exact source metadata and material recipe | A no-op source update can reuse complete group pins while recording a new adoption binding. F3 aligns target conflict semantics with this change. |
| Preparation survives failed promotion | Shared preparation records completion under captured epoch; selection still needs current source/base/definition | Partial completed groups remain useful after later group failures or stale promotion. |
| Original inputs remain evidence | Update clones parent inputs and adds current admission; readback checks exact map | F2 ensures the independent reader checks this generator invariant too. |
| Immediate parent captures actual composition | Update parent is its exact base; backfill parent is the actual sibling-inclusive current release | Shallow verification can compare complete sibling membership without an ancestor walk. |
| Historical completed operations remain stable | Result reconciliation precedes new admission and stale guards | Retry returns the previously acknowledged release without changing latest. |
| Global invalidation cannot silently requalify source values | Incremental updates inherit shaped source rather than rerunning its original transformation | F1 requires conservative refusal; broader requalification remains deferred. |

## Test behavior and edge cases

The reviewer read the assertions below and followed their implementation paths.
Execution results were supplied by the implementing agent; this reviewer ran no
commands that execute tests or benchmarks.

| Fixture | Location | Assertions and expected behavior |
| --- | --- | --- |
| `refresh_reuses_unchanged_file_computation_and_selects_all_groups_together` | `tests/refresh.rs:151` | Kills after acknowledged source processing, removes external inputs, and resumes. Evaluations equal new source-file count times selected group count; unchanged per-file records and output identities remain. The Rust oracle at `:91` checks both computed columns, duplicate/null keys, null/empty text and Unicode. Latest remains the old coherent result until selection. |
| `noop_and_delete_all_keep_complete_group_semantics_without_recomputing_unchanged_files` | `tests/refresh.rs:257` | A truly empty update preserves every native source/group pin and performs no file computation. Delete-all retains typed empty schemas and all group membership. The no-op branch in `src/engine/write.rs:370` requires actual removed and added counts both zero. |
| `refresh_resumes_after_partial_groups_readback_and_lost_selection_response` | `tests/refresh.rs:286` | Actual child-process termination occurs after one group, all groups, readback and selection. Resume prepares only the remaining group, does not repeat source work, and performs no marked expensive work after readback/selection. Retained phase/group records remain exact and every combined value is checked. |
| `independent_refresh_verifier_rejects_missing_groups_inputs_and_false_source_interpretations` | `tests/refresh.rs:338` | Rewrites and rehashes malformed release manifests so refusal tests semantic bindings rather than only manifest checksum mismatch. Covers F2, group membership and recipe/request binding. |
| `invalidated_source_refuses_incremental_admission_without_changing_authority` | `tests/refresh.rs:410` | Planning and execution both refuse stale source qualification; all lifecycle records remain unchanged. Covers F1 before admission mutation. |
| `relocated_retry_preserves_original_request_and_readback` | `tests/refresh.rs:432` | After retained readback, replacement/deletion paths are removed and changed. Material-identical retry returns the exact original release. `src/engine.rs:592-602` loads the original admitted request for provenance while caller paths remain available for copies not yet completed. |
| `noop_target_reselection_is_a_stale_group_conflict_and_inherited_siblings_are_exact` | `tests/refresh.rs:463` | Covers F3 and verifies a subsequent backfill over a refreshed result preserves exact sibling pins/bindings. Omitting/rebinding siblings or changing the immediate parent is refused. |
| `refresh_and_backfill_races_preserve_captured_source_and_selected_groups` | `tests/refresh.rs:550` | Admitting an update fences pending schema cutover. A complete routine update can select its captured boundary while newer admission remains pending; that newer operation then refuses its stale base. A fresh update catches up and a new backfill reuses retained unchanged-file work. |
| `failed_group_keeps_prepared_source_and_other_groups_without_partial_selection` | `tests/refresh.rs:618` | A deterministic expression fails on newly admitted data after an earlier group completes. Latest remains unchanged; failure is retained; retry through the successful earlier group creates no extra lifecycle acknowledgment. |
| `relocated_build_retry_keeps_the_original_readback_request` | `tests/lifecycle.rs:689` | Applies the same original-request/readback identity regression to Build, whose shared path changed. |

`tests/support/scalar.rs` extracts existing fixture construction for both backfill
and refresh tests. The unchanged backfill tests continue to check scalar
restrictions, native coverage and field mappings, failed-file retention,
sibling merges, stale authorities and process recovery. Removing their former
Update refusal assertion is intentional; full Build over selected groups still
refuses, and the new refresh fixtures qualify Update instead.

The first focused run exposed a no-op optimization gap: source native metadata
was rebuilt despite no applied row changes, so complete groups could not reuse
exact prepared pins. The implementing agent retained this failed attempt in
`docs/reviews/refresh-noop-failed.log`, added the count-based source-pin reuse
branch, and reran the focused fixture. This was a real failing assertion, not a
post-hoc performance claim.

Reviewed execution evidence at closure: the implementing agent ran
`./scripts/check` and `cargo check --locked --no-default-features`, both completing
with exit 0. The retained `docs/reviews/refresh-check.log` shows the unit, backfill,
lifecycle, coordinated-refresh and source-update suites passed, including the
relocated Build retry. The gate includes formatting and Clippy with warnings denied.
The reviewer read that log and `scripts/check`; no tests were executed by this
reviewer. Earlier focused refresh/backfill logs also passed. No throughput,
memory-ceiling or 10-million-row claim follows from these fixture runs.

## Conclusion

**VERDICT: APPROVE** for the scoped coordinated scalar refresh.

The patch prepares and verifies every selected scalar group against the candidate
source before one authoritative release selection. Exact parent, source
eligibility and target adoption checks close F1–F3. Shared preparation removes
separate backfill/update implementations while preserving retained file work and
validation. The count-based no-op path also avoids new source/group metadata
identities when no rows were applied; the fixtures establish reuse, not a speed
estimate.

**Coverage: ADEQUATE** for this slice, including independent multiset value
oracles, malformed composition, partial failure, history, actual process loss,
and deterministic concurrency interleavings. **Confidence: HIGH** for the static
consistency assessment. The final repository gate and no-default-features
check passed as recorded above.

`README.md:93-163` and `docs/STATUS.md:239` now describe coordinated scalar
updates and preserve the explicit full-rebuild, source-requalification, related-input
and performance limits. Those claims match the inspected implementation.

This approval does not qualify arbitrary incremental SQL/joins, original-source
requalification after global invalidation, permanent occurrence identity across
compaction, complete lineage, remote publication, destructive maintenance or
workload-scale performance. The verifier deliberately checks immediate retained
parents and current objects without recursively requalifying ancestor history.
