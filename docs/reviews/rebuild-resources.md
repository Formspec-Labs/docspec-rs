# Full rebuild: resource and recovery review

**No open implementation findings.** The review requested a deterministic test
for authority changing before acknowledgment and an exact prepared-group reuse
assertion. Both are present in the final patch. The unrelated workspace-release
failure has a separate [review](workspace-release.md) and deterministic regression.

Scope: rebuild changes relative to `656b60e`, including shared recovery behavior,
model bindings, independent verification, fixture extraction and documentation.
This is a static semi-formal review using the code-review skill and applicable
Microsoft Rust guidelines. The reviewer did not execute tests, edit production
code or create commits.

## Findings and closure

**RB1 — Test stale completion before acknowledgment.** Initial tests advanced
authority after a durable handoff, so they did not exercise the new permissive
completion guard. This test-coverage warning is closed by
`tests/rebuild_completion.rs:165`. Its injected catalog pauses after a real native
commit and before returning the table, admits newer source work, then releases
the old worker. The test requires durable processing with its original request
digest and eligibility, refuses stale selection, and subsequently reuses the exact
source pin under a new operation. No production pause hook was added.

**RB2 — Prove prepared-group reuse across base changes.** This nonblocking coverage
observation is closed in `tests/rebuild.rs:210`, `:263`: the test captures the
previously prepared group table and requires exact equality in the rebased result.
Unchanged journal records alone would not establish this identity. The test also
checks source-pin equality and complete independent values.

No further correctness, resource-lifetime or avoidable-complexity defect was found
in the scoped final diff.

## Patch summary

An optional exact `RunSpec.base` lets the normal build service replace a source
while retaining every selected scalar recipe. It prepares and verifies the source
and groups before selecting one complete result. Full rebuilds retain their complete
replacement input; incremental updates continue retaining prior input evidence.

The operation identity includes the composition base. Source processing reuse
depends on input digest, SQL, definition, processor and eligibility, so rebasing
does not force identical work to run again. A changed eligibility still creates
a different source-processing key.

Finished work is acknowledged under its original qualification even if selection
or eligibility changed while it ran. The execution epoch still fences recording;
strict authority checks govern beginning subsequent source phases, each source-run
group preparation and final selection. Explicit rebuilds also fence later source
admissions. This distinction retains useful work without allowing stale adoption.

## Function trace

| Function or type | Location | Inputs → outputs and verified behavior |
| --- | --- | --- |
| `RunSpec`, `OperationRequest::base`, `input_key` | `src/model.rs:145`, `:263`, `:293` | Optional exact base and bounded request → selected parent and consistent retained-input naming. The input-key helper removes duplicated build/update branching from writers and verifiers. |
| `Operation` | `src/engine.rs:48` | Captured request, eligibility, epoch, boundary and base → retained authority requirements; the new flag enables stricter boundary checking for explicitly based builds. |
| `plan_request` | `src/engine.rs:165` | Request and fresh lifecycle → reusable/required phases; checks unbased enriched-build refusal and includes group preparation for any explicit base containing groups. |
| `run_internal`, `prepare_and_select` | `src/engine.rs:486`, `:518` | Build request → source phases → serial shared group preparation → readback → guarded selection; preserves original captured request provenance and reconciles an identical operation's durable result. |
| `check_completion`, `complete` | `src/engine.rs:890`, `:976` | Finished phase plus fresh authority → epoch check and old-qualification record; existing deterministic winners remain unchanged. Neither method changes selection authority. |
| `phase`, `reserve_attempt` | `src/engine.rs:906`, `:941` | Attempted source work → strict authority guard before reuse/start or new protected directory. Completion uses a different guard intentionally. |
| `phase_key` | `src/engine.rs:1006` | Input declarations or source material dependencies → cache key. Build processing/validation omit composition base, operation and input location; admission remains digest-based. |
| `fingerprint` | `src/engine.rs:1123` | Material operation request, including build base → stable retry identity. The same operation cannot silently switch its parent. |
| `check_authority`, `check_preparation` | `src/engine.rs:1042`, `:1061` | Fresh authority → strict adoption/start eligibility or epoch-only completion eligibility. Exact-base rebuilds reject advanced admission boundaries. |
| `check_group_rebuild`, `validate_spec` | `src/engine.rs:1071`, `:1170` | Request and selected state → refusal of unbased enriched replacement or self-base before admission. Existing SQL qualification remains in force. |
| `process_build`, `validate_plan` | `src/engine/write.rs:40`; `src/engine.rs:1258` | Retained input and immutable qualified SQL → native source result. Only retained `input` scans are accepted; source computation does not read the composition base. This supports the new reuse-key separation. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Exact source pin, material recipe and eligibility → prepared native group and field mapping. Source-run callers freshly check authority at `src/engine.rs:725`; cached preparation resolves before payload computation. |
| `plan_backfill`, `prepare_backfill` | `src/engine/backfill.rs:138`, `:289` | Standalone backfill retains its existing source/target rules; its internal operation does not use the new full-rebuild boundary flag. The epoch helper is shared instead of copied. |
| `verify_release_with_budget` | `src/engine.rs:1379` | Serialized release and retained objects → exact request/input checks, native source checks and group verification; uses the same input-key function as construction. |
| `verify_groups`, `check_refreshed_groups` | `src/engine/backfill/verification.rs:6`, `:141` | Build or update composition → exact preserved recipe set and actual adoption request/base checks. Explicit builds require an advancing boundary, nonregressing eligibility and increased eligibility when changing definition. |
| `read_manifest`, `read_base`, `verify_group`, `verify_group_data` | `src/engine/backfill/verification.rs:161`, `:176`, `:184`, `:226` | Immediate checked parent and exact source/group pins → binding, schema and complete occurrence checks. Build-backed groups are accepted through the same maintained native reader path; no recursive history walk is added. |
| CLI / HTTP callers | `src/main.rs:195`, `:198`; `src/http.rs:59`, `:65` | Existing JSON plan/run/resume routes deserialize the optional base and delegate to the same services. No parallel rebuild interface or transport policy is introduced. |
| Shared fixture / oracle | `tests/support/enriched.rs:15`, `:34`, `:79` | Native fixture → direct Rust expected values and deliberately rehashed malformed manifests; moved from refresh tests without duplicating the oracle. |
| Paused native catalog | `tests/rebuild_completion.rs:45`, `:63` | One armed native commit → bounded one-shot test rendezvous. Lifecycle commits are forwarded without pausing; all other catalog methods delegate to the maintained adapter. |

The other touched fixture constructors and the query test only supply `base: None`.
Their source schemas and assertions retain their previous behavior.

## Data flow and invariants

| State | Creation, use and lifetime | Invariant and concrete boundary |
| --- | --- | --- |
| Source material key | Digest + SQL + source definition + processor + eligibility → processing/validation record | A different parent with identical source material can reuse the exact table. Different source qualification cannot collide with the old processing key. |
| Operation identity | Request including optional parent → retained operation/request | A base change requires a new operation even when source work is reusable. Request provenance remains the originally admitted request. |
| Retained completion | Work starts under strict authority → finishes → fresh epoch check → original key | Changes to selection or qualification cannot relabel the result as new work. The next strict check refuses an obsolete attempt. Cache keys, not the permissive completion check, determine later reuse. |
| Group composition | Explicit base's recipes → shared preparation against candidate source → complete dataset | Every group receives the new source binding and actual rebuild request. No source-only selection occurs after a group failure. |
| Reused group | Reused exact source metadata + same recipe + same eligibility → existing prepared key | Existing objects and owners remain unchanged. Tests require exact source and group table identities after rebase. |
| Input evidence | Full rebuild starts a new complete input map; update clones prior evidence before adding current inputs | The new full result does not claim old input bytes as its current complete source. The exact parent manifest retains history linkage, and old releases retain their own evidence. |
| Admission boundary | Explicit rebuild records current admitted boundary → subsequent source admission increments it | A later admission refuses the old rebuild's cutover even if selected base has not changed. Routine incremental refresh retains its existing, less strict captured-input rule. |
| Resource custody | Existing source writer, serial group loop, native admission, assembly permit and blocking-worker permits | No new execution pool, concurrent fan-out, payload collection or descriptor index is introduced. Existing group aggregate metadata admission still spans append/pin. |
| Metadata verification | One checked immediate base reused across groups | Bounded shallow verification remains; the patch does not recursively load every ancestor or hold a cache of every historical manifest. |

Hypotheses checked: excluding base from source reuse might omit a computation
dependency (refuted by `process_build` and retained-input-only plan validation);
allowing stale completion might permit stale publication (refuted by original keys
and strict final selection); source reuse might still recompute groups (refuted by
prepared-key lookup and exact-pin assertion); recovery tests might miss the changed
completion guard (initially confirmed, closed by the native catalog rendezvous).

## Tests and edge cases

Tests were read, not executed by this reviewer.

| Test | Location | Inspected assertion path and expected behavior |
| --- | --- | --- |
| `full_rebuild_requalifies_source_and_every_group_without_changing_history` | `tests/rebuild.rs:44` | Changes SQL and definition after invalidation, checks both computed fields using Rust reconstruction, exact new input provenance, preserved recipes, readable old values, and subsequent backfill/update. |
| `rebuild_resumes_source_partial_groups_readback_and_lost_selection_response` | `:124` | Real SIGKILL at named durable boundaries; deletes external input, changes retry locator and resumes in another process. Work markers permit only missing group preparation; readback/selection resume performs no instrumented expensive work. |
| `later_admission_fences_cutover_and_rebased_rebuild_reuses_source_work` | `:196` | Later admission refuses old plan/run; completed newer update becomes the new base. With original input absent, rebased result must use exact retained source and prepared-group pins and preserve completion records. |
| `failed_or_invalidated_rebuild_keeps_previous_complete_selection` | `:276` | Missing recipe input field leaves old complete result selected and source processing reusable. Invalidation after readback refuses old selection; a new qualified operation succeeds. |
| `independent_rebuild_verifier_rejects_false_composition_and_input_claims` | `:313` | Rehashes forged JSON to reach semantic checks: omitted group, altered recipe/request/source binding, absent parent, excess input, stale boundary, false definition/eligibility claims all refuse. |
| `rebuild_retry_identity_includes_base_and_self_base_refuses_before_admission` | `:403` | Self-base plan/run refusal leaves lifecycle unchanged; changing base under a completed operation produces `operation_conflict`. |
| `newer_admission_preserves_finished_source_before_acknowledgment_and_rebase_reuses_it` | `tests/rebuild_completion.rs:165` | Real native source commit is paused before acknowledgment; a newer admission advances authority. Releasing the worker records the original qualified source but returns `stale_source_boundary`. Rebase reuses exact native metadata/snapshot and unchanged completion record after deleting external input. |

The deterministic pause uses one-shot channels and a deadline rather than timing
assumptions. Its `Catalog` adapter changes only the response handoff and keeps
real native commit behavior. A mid-phase eligibility-change variant is not
separately exercised: eligibility invalidation after readback is tested, and the
epoch-only completion branch is statically traced. Arbitrary failure injection and
restore-epoch behavior remain outside this checkpoint's qualification.

The reviewer inspected the owner's completed `docs/reviews/rebuild-check.log`:
the formatting/lint gate, all test suites, the native completion race and final
additional check succeed. The owner reports the final additional check as
`cargo check --locked --no-default-features`. Earlier transient failures remain in
`docs/reviews/rebuild-regressions-failed.log`, and the deterministic workspace-lock
failure remains in `docs/reviews/workspace-release-failed.log`. This is inspected
execution evidence, not reviewer execution.

## Efficiency and scope

The concrete improvement is reuse of exact source and group tables after a
composition rebase, with no second preparation implementation or transport path.
Shared input naming and refreshed-group verification remove duplicated decisions;
the fixture extraction shares an independent value oracle. Exact-pin and work-entry
assertions substantiate avoided computation and retained outputs; no elapsed-time
speedup is claimed.

Full independent readback remains population-dependent. Native codec/scalar
allocation is not a whole-process resident-memory guarantee, lifecycle history
still has its existing finite serialized-size cap, and preflight disk estimates
remain estimates. Full schema evolution, continuous-ingestion catch-up, related-input
dependencies, destructive maintenance, remote providers and the Comments performance
target remain open, consistent with README and STATUS.

## Conclusion

**VERDICT: APPROVE.** The patch preserves resource ownership and exact composition,
retains finished work across stale authority, and reuses source/group work through
the existing services. RB1 and RB2 are closed. Changed-path coverage is **ADEQUATE**
for the scoped rebuild behavior; static confidence is **HIGH**. The owner gate
passes. This approval does not extend the documented performance or broader
lifecycle qualification boundaries.
