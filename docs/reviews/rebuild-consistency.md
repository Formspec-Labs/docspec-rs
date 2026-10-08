# Enriched full-rebuild consistency review

Scope: the uncommitted checkpoint after `656b60e`, reviewed statically on
2026-10-08 using the semi-formal code-review and Microsoft Rust guidance. The
reviewer read changed implementation, important callers/callees, fixtures and
owner-supplied logs. The reviewer ran no tests and edited only this report.
This review covers full replacement of a source while preserving its scalar
group definitions, including requalification after invalidation. It does not
qualify general schema evolution or continuous-ingestion catch-up.

## Findings

No unresolved production consistency defect was found in the inspected patch.

### F1 — Isolate the definition and eligibility verifier guards

- **Severity:** WARNING; test coverage. **Status: CLOSED.**
- **Initial location:** `tests/rebuild.rs:356-362` in the first reviewed fixture.
- **Evidence chain:** The initial changed-definition forgery changed the dataset
  and outer request but left group request bindings unchanged. Therefore the
  shared exact-request comparison could reject it even if the intended
  definition/eligibility guard were removed. Its parent also had eligibility zero,
  so it did not independently exercise eligibility regression.
- **Correction and closure:** The revised fixture at `tests/rebuild.rs:313`
  first creates a qualified parent with eligibility above zero. The regression
  case changes only eligibility; the changed-definition case changes the outer
  request and every group binding together (`:376-386`). These cases now isolate
  the respective comparisons in `src/engine/backfill/verification.rs:25-27`.
  The source SQL also differs from the parent, making the source-binding forgery
  a real mismatch rather than a reused identical source pin.

### O1 — Rebuild input replacement is explicit

- **Severity:** OBSERVATION; semantics.
- **Location:** `src/engine.rs:711-719`; `README.md:163`;
  `tests/rebuild.rs:196`.
- **Assessment:** A rebuild treats its declared input/query as the complete new
  source. Rebasing against a later selected version does not implicitly merge
  that version's rows into the replacement input. This is deliberate full-build
  behavior, documented as complete replacement; the exact parent preserves the
  historical relationship. Ordinary updates retain their captured-input behavior.

## Patch summary

`RunSpec.base` optionally names the exact selected version whose scalar definitions
must survive a complete source rebuild. Existing plan/run/resume and HTTP services
accept the request. Full source shaping remains in `process_build`; scalar work
remains in shared `prepare_group`. One final lifecycle selection publishes the
verified source and complete group set.

Operation identity includes the composition base, while source-phase reuse uses
input digest, SQL, source definition, processor and eligibility. This permits a
newly based operation to adopt exactly the previous source computation without
misrepresenting the original phase provenance. A persisted operation flag applies
the strict admission-boundary check only to explicit rebuilds. Completion
acknowledgment uses the existing execution-epoch rule independently of promotion.

## Function trace

| Function or type | Location | Inputs and outputs | Inspected behavior |
| --- | --- | --- | --- |
| `RunSpec`, `OperationRequest::base`, `input_key` | `src/model.rs:145`, `:263`, `:293` | Optional base and typed request → exact version and retained input name | Missing JSON base defaults to `None`; Build uses `input`, Update uses its operation-prefixed names. |
| `Operation` | `src/engine.rs:48` | Captured authority → persisted execution identity | Stores whether the operation requires the exact current admission boundary; ordinary Update and Backfill do not inherit this policy. |
| `Engine::plan_request` | `src/engine.rs:165` | Request → reusable and required stages | Applies definition/base/eligibility/group guards, then persisted operation guards; discovers group preparation for either explicit rebuild or Update. Completed historical operations retain their existing reconciliation path. |
| `Engine::run`, `run_internal` | `src/engine.rs:452`, `:486` | Build request → release or refusal | Stable request conflict detection and completed-result reconciliation remain around the shared execution path. |
| CLI run/resume; HTTP plan/run | `src/main.rs:195`, `:198`; `src/http.rs:59`, `:65` | JSON `RunSpec` → same engine services | The optional base flows through the existing interfaces without a second implementation. |
| `prepare_and_select` | `src/engine.rs:518` | Captured request and authority → combined release | Checks exact base before admission, captures strict cutover policy, shapes source, resets Build inputs, prepares all inherited recipes, independently verifies and selects atomically. |
| `process_build` | `src/engine/write.rs:40` | Retained input and deterministic SQL → native source pin | Reads the declared retained input, validates the maintained logical plan, writes native source output and returns no applied-write `ChangePin`. The base does not affect SQL inputs. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Candidate source, recipe and captured operation → group pin and field map | Unchanged shared implementation preserves per-file work, source/schema/recipe identity and epoch-checked completion. The source caller now checks current authority before each group. |
| `check`, `check_completion`, `phase`, `complete` | `src/engine.rs:879`, `:890`, `:906`, `:976` | Current authority and captured operation → permission or refusal | Start/reuse and selection require current eligibility/definition/base and, for rebuilds, boundary. Finished phases retain their original qualification under the epoch guard. |
| `phase_key`, `fingerprint` | `src/engine.rs:1006`, `:1123` | Material request and qualification → reuse key or operation digest | Build operation digest includes base; material source-phase key excludes base and operation ID but includes digest, SQL, definition, processor and eligibility. Inputs use their existing content-based admission key. |
| `check_authority`, `check_preparation`, `check_group_rebuild`, `validate_spec` | `src/engine.rs:1042`, `:1061`, `:1071`, `:1170` | Captured/current identity → structured refusal | Strict boundary applies only when recorded; epoch guards completion; unbased Build cannot discard groups; self-base refuses before admission. Backfill reuses the moved epoch guard. |
| `verify_release_with_budget`, `verify_changes` | `src/engine.rs:1379`, `:1425` | Serialized release → verified source, evidence and composition | Uses the shared retained-input name; Build still requires no applied-write change pin, and source is independently read. |
| `verify_groups`, `check_refreshed_groups`, `verify_group` | `src/engine/backfill/verification.rs:6`, `:141`, `:184` | Current dataset and exact immediate parent → composition verification | Rebuild requires exact parent dataset/version, advancing boundary, nonregressing qualification, increased qualification for definition change, complete recipes and fresh actual Build requests. Each group binds the new source metadata and native coverage. |
| `WorkspaceLock::drop` | `src/budget.rs:144` | End of local ownership → explicit OS unlock | Uses maintained `fs2::FileExt::unlock` so an inherited descriptor cannot prolong a dropped owner's advisory lock. The private lock is held by the engine's shared `Inner` at `src/engine.rs:44`; acquired ownership is released once on final drop. |
| Shared test helpers and existing request fixtures | `tests/support/enriched.rs:15`, `:35`, `:79`; `src/query.rs:161`; existing tests | Fixtures → deterministic inputs and independent expected values | Extracts existing multi-column oracle and digest-valid forgery helper; existing Build initializers explicitly use `base: None`. No query adapter behavior changes. |

## Data flow and invariants

| Invariant | Creation, mutation and use | Assessment |
| --- | --- | --- |
| Full replacement preserves every scalar recipe | The exact base supplies definitions; the source/group loop creates fresh adoption bindings | Shared verifier checks the complete recipe membership, not merely group count. Incompatible source expressions fail without changing latest. |
| Source requalification is real computation | Invalidation advances eligibility; source and group reuse keys include it | Prior qualifications cannot supply processing results under the new eligibility. Exact retained input bytes can still reuse admission safely. |
| Strict cutover cannot overwrite later admission | Registration captures the current boundary and explicit rebuild policy; phase start/reuse and final selection recheck it | A newer source admission refuses the old operation's cutover, even when the selected base has not yet changed. |
| Completed work survives lost promotion eligibility | Source/group/readback completion records keep their original qualification; acknowledgment checks epoch only | Later admission, selection or invalidation can block continuation/selection while retaining completed output for qualified future work. No new current-selection authority is granted by completion. |
| Rebase does not change material source identity | Build operation fingerprint includes base; material source-phase key excludes it | A new operation against a later composition may reuse the original source pin and phase provenance when all material dependencies and eligibility match. |
| New and historical input evidence are distinct | Build resets current inputs; Update inherits; `parent_manifest` retains exact history relation | New Build inputs must match the declared map exactly. Old retained releases remain separately readable; no destructive cleanup is introduced. |
| Build-backed groups remain usable after later work | Each binding carries the actual Build request; shared `input_key` resolves its input | Subsequent Backfill preserves that binding; subsequent Update adopts refreshed recipes and retains the Build input evidence. |
| Original retries preserve identity | Operation fingerprint includes optional base; captured original request binds output | Changing base with the same operation refuses; moving input locators with identical material request can reuse the original readback. Completed historical retries return the original release. |
| Independent verification remains bounded to immediate composition | Verifier reads exact immediate parent manifests and current objects, not all ancestors | This proves current composition and coverage within the existing immutable-evidence trust model, not complete historical lineage or active live authority from an offline file. |

## Test behavior and edge cases

| Fixture | Location | Assertion and trace |
| --- | --- | --- |
| `full_rebuild_requalifies_source_and_every_group_without_changing_history` | `tests/rebuild.rs:44` | Changes active definition and source SQL, checks all source and scalar values with an independent Rust multiset oracle, exact new input/parent/qualification, preserved recipes and actual Build bindings. Reads historical data, adds another group, performs a later no-op Update, and retries the original completed Build. |
| `rebuild_resumes_source_partial_groups_readback_and_lost_selection_response` | `tests/rebuild.rs:124` | Actually kills child workers after source processing, one group, all groups, readback and selection; removes and changes external input paths; checks remaining assembly work, absence of repeated source work, exact retained records and original request provenance. All columns are checked after resume. |
| `later_admission_fences_cutover_and_rebased_rebuild_reuses_source_work` | `tests/rebuild.rs:196` | Pauses completed work, admits a newer source operation, verifies stale boundary in plan/run and unchanged latest, selects the newer operation, then rebases. The source pin, already prepared group pin and retained phase records remain exact despite removed external input. |
| `failed_or_invalidated_rebuild_keeps_previous_complete_selection` | `tests/rebuild.rs:276` | A missing source field fails group compilation after retaining source work. A separate fully read-back candidate loses eligibility before selection and refuses. Old combined values remain readable; a fresh operation requalifies successfully. |
| `independent_rebuild_verifier_rejects_false_composition_and_input_claims` | `tests/rebuild.rs:313` | Newly serialized, rehashed manifests falsify group membership/recipe/request, parent, source, input map, boundary, eligibility or explicit-base declaration. Revised eligibility/definition cases isolate their intended guards (F1). |
| `rebuild_retry_identity_includes_base_and_self_base_refuses_before_admission` | `tests/rebuild.rs:403` | Self-base plan/run refuse without changing lifecycle state. After successful rebuild, removing the base under the same operation identity refuses in both paths. |
| `newer_admission_preserves_finished_source_before_acknowledgment_and_rebase_reuses_it` | `tests/rebuild_completion.rs:165` | A delegating catalog performs the real native source commit, pauses its response before source-phase acknowledgment, then permits another admission. After release, processing is retained under its original qualification but the operation refuses stale cutover. A new base reuses the exact committed metadata/snapshot and unchanged original phase record. |

The completion-race adapter at `tests/rebuild_completion.rs:63` forwards real
catalog operations and pauses only a native data commit response. It exposes a
positive notification that the real commit succeeded, asserts no processing
acknowledgment exists yet, and uses bounded handoff waits. This exercises the
otherwise missing interval between expensive completion and lifecycle acknowledgment;
an ordinary after-phase pause would not prove that property.

The final lock regression at `src/budget.rs:160` retains a duplicate descriptor,
confirms live ownership still excludes another owner, drops the original owner,
reacquires, then proves closing the old duplicate does not release the new owner's
lock. The reviewer inspected `fs2` 0.4.3's Unix implementation, which delegates
explicit release to `flock(LOCK_UN)`. This is local advisory-lock behavior, not
remote ownership or restore fencing.

The shared enriched oracle checks every source/computed column, duplicate counts,
nulls, empty strings and Unicode. Existing backfill, update and lifecycle suites
remain relevant because completion guards and Build reuse keys are shared. No
request for unsupported aggregate groups, arbitrary lineage or a benchmark is
implied by these fixtures.

Final execution evidence: the implementing agent ran `./scripts/check` and
`cargo check --locked --no-default-features`, both completing with exit 0. The
retained `docs/reviews/rebuild-check.log` records the strengthened malformed
eligibility/definition cases, exact rebased source/group-pin reuse, deterministic
native commit race, lock-release regression and existing suites passing. The gate
includes formatting and Clippy with warnings denied. The reviewer inspected the
log and assertions and did not execute tests or benchmarks.

## Conclusion

**VERDICT: APPROVE** for the inspected full source/scalar rebuild implementation.
The final repository gate and no-default-features check passed.

The implementation follows the small shared-path design, preserves exact base and
retry identity, and separates durable completion from mutable cutover authority.
It reuses shared scalar preparation and input naming instead of introducing a
second service. Rebased source reuse is demonstrated by identity assertions; no
end-to-end speedup or workload-scale memory claim is made.

**Coverage: ADEQUATE** for the scoped behavior after inspection of the new
deterministic completion-race fixture. **Confidence: HIGH** in the static
consistency assessment. README and STATUS describe complete replacement, strict cutover, preserved history and
the remaining broader schema/dependency/performance limits consistently with the
code. Greenfield experiment output compatibility remains outside this checkpoint.
