# Durable request admission and owner protection review

Current verdict: **APPROVE the corrected scoped durable operation/request-admission increment.** F1–F4 are closed by the corrections and evidence below. The implementation agent confirmed the final repository gate passed; see the validation closure at the end. The first full gate exposed F4, a compatible Backfill concurrency regression; its fresh-admission retry correction passed static re-review and the targeted Backfill suite. The initial findings and failed runs remain review evidence. Later phase, readback, final-selection, retirement, or deletion milestones remain outside this increment.

This is a blind semi-formal review of the uncommitted changes on `codex/greenfield-lifecycle`, starting from `src/engine/admission.rs`, its tests, the admission changes in `engine.rs`, `backfill.rs`, and `refresh.rs`, and retention owner/binding extensions. Parent/repository guidance and the Rust and semi-formal code-review skills were applied. The implementation agent owns formatting and executed validation. This reviewer ran no Cargo, formatting, benchmark, payload-processing, or deletion command and changed only this report.

## Findings

### F1 — Backfill's distinct admission transaction lacks protection assertions

- Severity: WARNING (P2)
- Category: test-coverage
- Location: `src/engine/backfill.rs:430-467`; `src/engine/admission/tests.rs:61-85`.
- Description: Backfill has a separate capture-and-commit branch, including an additional `backfill/<operation>` claim. The new admission fixture never invokes it, and its `capture` helper checks only operation, request, and request-dependencies claims. Existing backfill recovery tests exercise retained computation and readback, but do not inspect owner grants or the new atomic bindings.
- Evidence chain: `prepare_backfill` loads the exact historical base, releases its consumer lock for owner acquisition, rechecks the captured boundary, creates `Capture`, and calls `PreparedAdmission::records` before one lifecycle commit (`backfill.rs:408-467`). The generic Build/Refresh path uses a different loop and lock set (`engine.rs:934-1038`). The new tests exercise managed Build, provisional refusal, Refresh with a producer seal, based Build reuse, and a lost Build request reply (`admission/tests.rs:89-241`). None establishes that the backfill claim and the base's foreign dependencies survive the separate transaction and retry path.
- Suggestion: Add a native Backfill case whose base contains a foreign dependency. Assert the operation/request/backfill/request-dependencies claims and their bindings share the committed revision, the exact owner token covers that dependency, and reopen/retry preserves the binding without another owner commit. A lost initial request reply can be included at this same boundary.

### F2 — Same-owner grant unions lack an end-to-end acceptance/refusal case

- Severity: WARNING (P2)
- Category: test-coverage
- Location: `src/engine/admission.rs:171-176`; `src/engine/retention/owners.rs:68-94`, `:193-238`, `:256-275`; `src/engine/retention/inventory.rs:383-483`.
- Description: The patch changes the former one-grant-per-owner rule into a union of distinct grants. None of the inspected native tests drives that new shape through admission, binding validation, reopen, and inspection, or proves that omitting one required grant still refuses.
- Evidence chain: Admission first protects its release closure and then merges a separate managed-input grant (`admission.rs:171-176`). A based consumer can therefore acquire two different tokens from one owner. `merge` now sorts by `(owner, token)`; `validate_binding` permits repeated owners; `cover_required` accumulates exact coverage across grants; inspection associates obligations with the exact consumer/token set. Existing seal tests issue one grant for each owner (`owners/tests.rs:557-595`); the new managed-input test has one token (`admission/tests.rs:111`), while the Refresh test has distinct owners (`:187-188`). The repeated-binding collector test verifies shared accounting and a changed owner boundary, not a successful or incomplete grant union (`inventory/collector_tests.rs:347-401`).
- Suggestion: Create a consumer with an old foreign dependency and a different managed input from that same owner, then admit a based operation. Assert both tokens persist in sorted form, retry does not mint a replacement union grant, and inspection/reopen accepts. Use a retained claim requiring both scopes to show that removing one token refuses rather than borrowing coverage from another claim. Also verify that merging an identical token deduplicates it and a conflicting token identity refuses.

## Patch summary

The change records `AdmissionDependencies` with an exact base, captured producer coverage, and qualified managed inputs. Owner acquisition precedes the first operation/request commit. The consumer then rechecks its metadata boundary and commits the request, dependencies, and digest-bound protection references together. Managed inputs reuse their retained object identity instead of being copied and hashed again. Refresh reuses its admitted producer capture.

The owner layer accepts multiple immutable grant references for one owner, while inventory validates their combined exact scope. Inventory's in-progress adjustment deduplicates obligations only when consumer and full token set are identical; distinct sets remain separate. Later propagation into independent completions/readbacks and newly introduced final-selection dependencies remains outside this increment.

The governing requirements are stable operation recovery and retained phases (`docs/PLAN.md:67-75`; `docs/requirements/01-durable-storage.md:27-42`), owner protection before a durable foreign consumer (`docs/reviews/owner-protection-design.md`, acquisition sequence and next-integration sections), and shared exact retention roots (`docs/requirements/06-maintenance-retention.md:21-31`). The performance rule is to reuse qualified immutable inputs and avoid unnecessary reads/copies (`docs/PLAN.md:42-57`).

## Function trace

Paths and lines describe the reviewed working copy before the final formatter pass. Small related helpers are grouped only when they share one behavior.

| Function / method | File:line | Inputs → output | Verified static behavior |
| --- | --- | --- | --- |
| `run_internal` / `run_admitted` | `src/engine.rs:693`, `:706` | Request → retained release or error | Run through owned worker admission; reconcile an already selected exact operation after an error. |
| `prepare_and_select` | `src/engine.rs:924` | Request and optional stop → release | Resolve an existing operation first; acquire outside consumer locks; lock a sorted set; repeat eligibility/base/schema checks; commit initial claims together. |
| `prepare_backfill` | `src/engine/backfill.rs:378` | Backfill request → release | Uses its exact requested historical base and a separate initial capture transaction; owner acquisition occurs after dropping the consumer guard. |
| `PreparedAdmission::datasets` / `check_boundary` | `src/engine/admission.rs:27`, `:36` | Capture / view → lock IDs / refusal | Lock consumer, direct producers, and families; refuse any changed consumer metadata location. |
| `PreparedAdmission::records` / `dependencies_key` | `src/engine/admission.rs:46`, `:65` | Capture, view, request records → appended claims | Add typed dependencies and bind operation, request, dependencies, and Backfill claims to the same next revision. |
| `protect_admission` | `src/engine/admission.rs:110` | Request, base, boundary, permit → prepared capture | Capture Refresh seals, deduplicate exact versions, load producer releases, resolve managed locators, protect full release closure and managed objects, retain size-based permits. |
| `admitted_dependencies` | `src/engine/admission.rs:70` | View and operation → optional capture | Require co-revisioned request/operation, dataset/base/coverage/digest agreement, and a local binding for this new record family. Missing legacy captures retain the prior fallback. |
| `prepare_source` | `src/engine.rs:740` | Request/operation → reusable or new phase | Resolve completed processing/admission before payload work. For missing admission, use captured managed objects directly and copy/hash only uncaptured inputs. |
| `admit_refresh` | `src/engine/refresh.rs:54` | Refresh request → admission phase | Reuse retained coverage; only a legacy request recaptures it. |
| `capture_inputs_admitted` / `guard_inputs` | `src/engine/inputs.rs:162`; `src/engine/refresh.rs:272` | Exact versions / coverage → capture / proofs | Capture producer families under sorted locks and recheck direct input eligibility and immutable family membership at the consumer commit. |
| `release_admitted` / `resolve_release` | `src/engine.rs:426`, `:433` | Producer/version → retained pin | Lock producer; check retained dataset/version and verified status. |
| `ReferenceProtections::merge` / `retain_memory` | `src/engine/retention/owners.rs:68`, `:97` | Grants → combined retained references | Require same consumer, allowance, and budget; refuse conflicting exact token references; sort/deduplicate and release excess accounting. |
| `ReferenceProtections::binding` | `src/engine/retention/owners.rs:106` | Consumer/view/claim → binding record | Check consumer table; bind next native revision, claim digest, and copied immutable references. |
| `validate_binding` | `src/engine/retention/owners.rs:193` | Claim rows and binding → required roots | Check key, revision, consumer, digest, strict token order; derive roots from the typed claim, not the supplied grants. |
| `validate_reference` / `cover_required` / `check_covered` | `src/engine/retention/owners.rs:240`, `:256`, `:271` | Exact grant and obligations → coverage/refusal | Check owner/consumer/UUID/revision; compare exact object identity within each matching grant; require every obligation covered. |
| `retained_protections` / `local_binding` | `src/engine/retention/owners.rs:304`, `:322` | Local retained claim → references | Decode/check the local claim and retain charged token references. This path deliberately does not reopen owners. |
| `check_reference_binding` / `check_protection_references` | `src/engine/retention/owners.rs:345`, `:356` | Retained binding → authoritative check | Open each owner once, validate exact retained grants and availability, and check the union. |
| `protect_releases` / `protect_objects` | `src/engine/retention/owners.rs:388`, `:398` | Qualified roots/objects → acquired references | Reuse typed closure collection; refuse conflicting path identities; group exact objects by actual owner; hold one owner lock at a time. |
| `qualified_object` | `src/engine/retention/owners.rs:442` | Managed path/digest → qualified object | Resolve through retained creation evidence, check reservation ancestry and unavailable state; do not hash payloads. |
| `acquire_owner` / `confirm_creation` / `visit_creation` | `src/engine/retention/owners.rs:475`, `:582`, `:603` | Exact owner scope → immutable grant | Check availability and production ancestry, reconcile an existing exact token, validate typed creation evidence, commit through owner authority, reconcile lost acknowledgment. |
| `read_retained` | `src/engine/retention/owners.rs:658` | Exact key and limit → typed row | Bound serialized row size before decoding. |
| `reference_objects` | `src/engine/retention/inventory.rs:27` | Producer-anchored roots → charged exact closure | Walk only supplied roots; reuse complete inventories; refuse unknown native identities; return a size-charged result. |
| `reference_roots` / `Inventory::record` | `src/engine/retention/inventory.rs:262`, `:734` | Typed claim → direct references | Share inventory parsing; shallow mode skips external expansion and path-only inputs; recognize the new dependencies row. |
| `Inventory::binding` / `check_bindings` | `src/engine/retention/inventory.rs:383`, `:446` | Bindings and owner boundaries → checked coverage | Share obligations for identical consumer/token sets; decode each token once; refuse owner-boundary changes; preserve separate coverage for distinct token sets. |

## Data flow and invariants

- **A durable request follows durable owner acquisition.** `protect_admission` returns only after all acquisitions complete (`admission.rs:171-178`). The caller then checks the consumer boundary and commits the claims (`engine.rs:984-1037`; `backfill.rs:437-466`). A partial acquisition can retain surplus grants, but cannot commit a partial request.
- **Acquisition does not invert consumer locks.** Both callers drop their consumer guards first (`engine.rs:979`; `backfill.rs:432`). Owner locks are scoped to one iteration (`owners.rs:423-426`); the later consumer/input set is sorted by `lock_inputs` (`inputs.rs:225-230`).
- **Prepared inputs survive the first commit.** `AdmissionDependencies` is serialized with the request, then the temporary preparation state is dropped (`engine.rs:1036-1041`). Recovery resolves the retained capture; it does not depend on the original preparation allocation.
- **Managed data preserves the original object owner.** Resolution reads typed creation records, not a caller-supplied path/digest alone (`owners.rs:442-472`). Admission emits that `ObjectRef` unchanged (`engine.rs:823-827`), with no copy or digest pass in that branch. This is a code-path property, not measured throughput or physical I/O evidence.
- **Grant coverage is an exact union.** Roots are deduplicated by path with conflicting identity refusal. Each grant can mark only matching-owner exact identities; every obligation must be covered. Identical token sets may share obligations; another claim's different set cannot supply missing coverage.
- **Resource accounting stays bounded but remains conservative.** Requests, root pins, traversal state, lifecycle histories, and owner decoding use shared permits and metadata limits (`admission.rs:127-170`; `inventory.rs:39-69`; `owners.rs:398-434`, `:442-452`; `catalog.rs:181-226`). Decoding allowances can impose a configuration-dependent admission floor. No whole-process memory ceiling, latency improvement, or metadata-growth qualification was measured here.

## Test behavior and edge cases

These are assertions inspected statically, not tests executed by this reviewer.

| Test | Location | Assertion trace and expected behavior |
| --- | --- | --- |
| `managed_input_admission_reuses_its_recorded_owner_without_copying_or_hashing` | `src/engine/admission/tests.rs:89` | Same-size payload damage still reaches the admission stop; retained output is exactly the foreign object; retry/reopen preserve capture and owner revision; repaired payload completes. Expected to pass by the managed branch. |
| `provisional_managed_file_refuses_before_request_acknowledgment` | `src/engine/admission/tests.rs:133` | A copied file in an existing attempt has no typed creation record; qualification refuses before any operation/request/dependencies binding exists. |
| `refresh_request_captures_and_protects_unused_members_of_its_producer_seal` | `src/engine/admission/tests.rs:152` | Direct inputs exclude a sibling, but captured seal and grants retain that sibling's manifest too. The version-union loop includes it. |
| `missing_external_locator_reuses_material_work_and_captures_selected_history` | `src/engine/admission/tests.rs:201` | A missing original external file does not prevent reuse of retained processing; own predecessor is captured without a foreign grant. |
| `lost_initial_request_reply_retains_exact_dependencies_without_reacquiring_owners` | `src/engine/admission/tests.rs:219` | Pre-acquired exact grant directs the injected lost response to the consumer transaction; reopen reuses its capture and owner revision. |
| Existing seal binding/refusal cases | `src/engine/retention/owners/tests.rs:532`, `:643` | Assert same-revision seal/binding and exact owner identity; changed digest, missing token, empty grants, or insufficient manifest scope refuses. These cover the old one-token-per-owner shape. |
| `repeated_bindings_share_claim_memory_and_owner_changes_refuse_inspection` | `src/engine/retention/inventory/collector_tests.rs:348` | Repeated identical binding must leave charged state unchanged; a changed owner boundary refuses before token validation. The in-progress dedup adjustment directly supports this assertion. |

Additional concrete paths: a changed consumer boundary refuses before request commitment; partial owner acquisition leaves durable extra protection; an old request without `request-dependencies` retains the legacy path; conflicting same-path object identities refuse; oversized captures refuse before consumer acknowledgment. Static traces support these behaviors. F1/F2 identify the missing direct assertions for the new Backfill and grant-union paths.

## Assumptions and validation limits

- The owned local catalog and immutable managed-path model remain the trust boundary. Ordinary replay trusts the original atomic acquisition/claim commit; it does not authenticate every owner again through `retained_protections`.
- Owner grants remain active and immutable in this checkpoint. Retirement, deletion, restore fencing, remote providers, and later completion/selection integration are not qualified by this review.
- The shared owner acquisition/removal stale-view tests predate this admission change. Their existence supports the primitive's test design, not proof of all new integration races.
- The working tree was still being edited for identical-binding deduplication during review. The final report needs a short re-read of those lines and disposition evidence.
- The inspected `docs/reviews/admission-protection-compile.log` reports a successful compile with an unused `ReferenceProtections::empty` warning. This is implementation-agent evidence only; it is not a repository gate, test execution, or final post-format result.

## Initial conclusion

VERDICT: NEEDS DISCUSSION

Justification:
- The inspected code implements durable operation/request admission with owner-first acquisition and a guarded consumer handoff; no correctness failure was demonstrated in those static paths.
- Coverage of changed paths: INSUFFICIENT for the specific Backfill transaction and same-owner multi-token union identified in F1/F2.
- Confidence: MEDIUM until those branches have direct evidence and the implementation agent supplies final validation results.

## Disposition review: added tests and decoded-record accounting

The implementation agent added `backfill_admission_binds_its_capture_and_foreign_base_in_one_native_commit` (`src/engine/admission/tests.rs:494`) and `based_managed_rebuild_keeps_distinct_scopes_from_the_same_owner` (`:621`). Static review confirms that the first checks the Backfill claim/binding revision, exact base, reopen reuse, unchanged owner revision, and eventual result. The second checks distinct same-owner tokens, preserved capture after reopen, successful inspection, and separate refusals after removing each required grant scope from a corresponding claim. These address F1/F2's substantive coverage gaps; their execution evidence remains pending.

The re-review also includes the added `SnapshotRows::reserve_decoded` (`src/catalog.rs:63`), `admission_memory` (`src/engine/admission.rs:78`), actual record charges in the admission callers, and local claim/binding charges (`src/engine/retention/owners.rs:364`). The helper checks cached serialized lengths before decoding and returns a shared permit; the changed callers retain it during their decoded values' use. This removes the new blanket outer decoding reservation. It is conservative admission accounting, not an allocation measurement.

### F3 — Nonempty admission roots still hit the collector's fixed memory floor

- Severity: WARNING (P2)
- Category: correctness / resource use
- Location: `src/engine/admission.rs:280`; `src/engine/retention/inventory.rs:49`.
- Description: Record-sized outer charges do not yet let a tiny based operation use the existing 256 MiB memory / 16 MiB metadata profile. The integration now calls the full collector for every nonempty selected base, including an otherwise ordinary based Build or Update.
- Evidence chain: `protect_admission` unconditionally passes its nonempty `releases` vector to `protect_releases`; that calls `reference_objects`, whose nonempty branch reserves `8 * max_metadata_bytes`. For that profile the reservation is 128 MiB, exactly the entire application semaphore (`src/budget.rs:83-94`). At least request and cached lifecycle-row permits are already held (`src/engine/worker.rs:90-96`; `src/catalog.rs:204-227` after the new helper's insertion), so `Budget::reserve` refuses immediately (`src/budget.rs:142-153`). The collector existed before this patch, but invoking it for all based admission is new. This is a static lower bound, not an RSS observation.
- Suggestion: Extend the resource correction through explicit-root collection, preserving a bound on unknown external metadata without charging the maximum eightfold for a small known root. Verify a based Build/Update or Backfill under the unchanged 256 MiB / 16 MiB profile, in addition to the initial-build worker fixture. Do not raise fixture limits merely to conceal the new floor.

The implementation-agent log `admission-protection-focused.log` records passing admission tests, including both F1/F2 cases. Those findings are closed.

The next resource correction changes `reference_objects` to hold one graph-state allowance (`inventory.rs:49`), admits external decoders by their exact descriptor sizes (`:975-985`), and retains the returned vector's split permit (`:62-71`). It also drops creation decoding before owner commit (`owners.rs:574`) and charges an operation's implicit base-release row (`:364-373`). Static tracing accepts this collector correction, and the now-256 MiB based-reuse test passes in the broader unit log.

However, `admission-protection-unit-focused.log` retains a failed existing worker/Refresh case: `queued_writer_does_not_deadlock_public_build_or_refresh_internal_reads` returns `capacity` at `worker/tests.rs:338`. The remaining lower-bound trace is the new 64 MiB unknown-seal allowance (`admission.rs:156-157`), capture's 16 MiB allowance (`inputs.rs:168`), `follows`' 16 MiB allowance (`history.rs:55`), old/new 16 MiB lifecycle-cache permits during a producer cache miss, and an 8 MiB batch allowance (`storage.rs:87`). That totals 136 MiB against the profile's 128 MiB application pool before request charges. The collector portion of F3 is corrected; the same resource regression remains open for Refresh admission.

Current disposition: F1/F2 closed; F3 open for the failed existing-profile Refresh case. Final verdict remains pending resource disposition and implementation-agent validation.

## Final re-review and disposition

**F1 closed.** The Backfill test asserts its additional capture binding, common native revision, foreign owner reference, unchanged owner revision through reopen/retry, and eventual result. Its passing execution is retained in both `admission-protection-focused.log` and the final engine-focused log.

**F2 closed.** The same-owner test retains two distinct tokens, verifies reopen/retry and inspection, and rejects a claim missing either of the separately required scopes. Exact token-set deduplication in `Inventory::binding` shares only identical obligations; static tracing confirms that distinct sets remain isolated. The relevant native and collector tests pass in the final focused log. No union grant is minted solely to combine references.

**F3 closed.** The final correction extracts `capture_inputs_with_memory` from the existing input capture (`src/engine/inputs.rs:169`). It returns the captured coverage with its existing permit, resized to the retained coverage's bounded serialized charge (`:228-239`). `protect_admission` merges that permit (`src/engine/admission.rs:153-156`), removing the extra maximum allowance during producer qualification. The original capture behavior, source/family lock order, eligibility checks, and metadata traversal remain shared. Together with the collector and cached-row changes documented above, this removes the demonstrated fixed reservation overlaps without increasing fixture budgets.

Additional final traces:

| Function | File:line | Inputs → output | Verified static behavior |
| --- | --- | --- | --- |
| `SnapshotRows::reserve_decoded` | `src/catalog.rs:63` | Cached keys and budget → owned permit | Bound each cached JSON length, sum its conservative decoded charge before allocation, and use the shared semaphore. |
| `admission_memory` | `src/engine/admission.rs:78` | Consumer view/operation → permit | Charge the exact retained dependency and request rows before decoding them. |
| `capture_inputs_admitted` / `capture_inputs_with_memory` | `src/engine/inputs.rs:162`, `:169` | Exact input versions → coverage / coverage plus permit | Existing callers retain a value-returning wrapper; protected admission takes ownership of the same capture allowance. Blocking manifest work keeps its shared permit until completion. |
| `reference_objects` / `Inventory::expand` | `src/engine/retention/inventory.rs:27`, `:960` | Explicit releases → bounded exact objects | One graph allowance plus one descriptor-sized decoder at a time; complete inventories still avoid native rereads; returned references keep their split permit. |
| `local_binding` | `src/engine/retention/owners.rs:345` | Exact retained claim → binding/roots/permit | Charge both claim and binding, and charge an operation's indirect base-release row before the shallow walker decodes it. |
| `acquire_owner` | `src/engine/retention/owners.rs:529` | Qualified object scope → durable grant | Release the completed creation-decoder reservation before the guarded native commit; ownership, identity, and unavailable-state decisions remain unchanged. |

Executed evidence was produced by the implementation agent and inspected by this reviewer:

- `docs/reviews/admission-protection-unit-focused.log` retains the intermediate failure of the existing worker/Refresh test. It is not passing evidence.
- `docs/reviews/admission-protection-unit-final.log` records **56 passed, 0 failed**, including the existing 256 MiB queued-writer Build/Refresh case, the 256 MiB based-reuse case, the new Backfill and same-owner scope tests, owner stale-view/recovery tests, and collector deduplication/unknown-identity tests. The run finished in 19.09 seconds; this is test duration, not a workload performance result.
- The full `./scripts/check` is running into `docs/reviews/admission-protection-check.log`. Its exit status was not yet available at this final review. Root must record that outcome before claiming the repository gate passed.

The reviewer performed static analysis and inspected retained execution logs; the reviewer did not execute tests. No open actionable finding remains within the specified increment. The concrete improvement is direct reuse of qualified managed inputs, shared typed root traversal, and removal of overlapping maximum reservations. No throughput, whole-process memory, metadata-growth, remote-provider, deletion, or complete lifecycle claim follows from these checks.

VERDICT: APPROVE

Justification:
- The scoped request-admission paths acquire exact foreign-owner protection before committing their durable consumer claims, recheck the consumer boundary, and reuse retained capture on retry.
- Coverage of changed scoped paths: ADEQUATE after F1–F3 corrections and the final focused evidence; full repository gate pending.
- Confidence: HIGH for the traced local admission and recovery behavior; resource and lifecycle limits remain as stated above.

## Full-gate follow-up: compatible concurrent admission

### F4 — Unrelated consumer commits incorrectly abort compatible Backfills

- Severity: WARNING (P2)
- Category: correctness / concurrency
- Location: the former `PreparedAdmission::check_boundary` refusal at `src/engine/admission.rs:36`; Backfill admission handoff at `src/engine/backfill.rs:449`.
- Evidence: The original full repository gate in `docs/reviews/admission-protection-check.log` fails `tests/backfill.rs::competing_independent_backfills_both_survive_selection`. The test runs two independent computed groups against the same base using `tokio::join!`, requires both to finish, and independently verifies the combined release (`tests/backfill.rs:479-489`). One group's consumer-table commit changes the other group's captured metadata location; the initial implementation returned `stale_authority` instead of rebuilding its still-compatible admission decision. The gate's failed result supersedes the earlier statement that it was pending.
- Required correction: Reopen current consumer state and repeat the actual admission predicates when only the captured metadata boundary changed. Preserve semantic refusals and durable owner grants; do not replay the old decision.

The correction replaces the unconditional refusal with `PreparedAdmission::is_current` (`src/engine/admission.rs:36`). Both callers discard the temporary preparation and continue their outer admission loop when it returns false (`src/engine.rs:1005-1010`; `src/engine/backfill.rs:449-453`). Static re-review confirms:

1. `continue` ends the current loop scope and drops its input/consumer locks and decoding permits. The next iteration starts from a newly opened lifecycle view, and rechecks operation identity, completed result, and abandonment (`engine.rs:943-961`; `backfill.rs:390-404`).
2. Generic admission repeats definition, exact base, eligibility, group, incremental, and schema checks before retrying (`engine.rs:972-983`). An incompatible selected base or changed definition still refuses; the code cannot advance by merely accepting the new revision.
3. Backfill reloads its exact historical requested base and fresh authority, checks definition and eligibility, and uses the new authority boundary in its capture (`backfill.rs:413-425`, `:468-477`). Existing-operation reconciliation still uses its retained capture.
4. Reacquisition happens outside consumer locks. Content-addressed identical owner grants are reused; previously acquired grants remain durable. The loop contains metadata capture/qualification only, so it does not rerun data processing or discard an acknowledged phase.

F4 is closed. The implementation-agent log `docs/reviews/admission-protection-concurrency.log` records **15 passed, 0 failed**, including `competing_independent_backfills_both_survive_selection`, same-group conflict cases, invalidation refusal, and recovery cases. This reviewer inspected that completed result without executing Cargo. The rerun repository gate remains pending. No additional issue was found in this retry control.

Current scoped verdict remains APPROVE, with all recorded findings closed and the final repository-wide execution outcome still owned by the implementation agent.

## Final inspection accounting check

The last narrow correction changes explicit retention inspection from its old combined `8 * max_metadata_bytes` allowance to `5 * max_metadata_bytes`: one bounded graph allowance plus conservative decoded lifecycle-row copies (`src/engine/retention/inventory.rs:107-110`). External JSON decoding now independently reserves `8 * object.bytes + 4096` for one immutable descriptor at a time (`:976-986`), and native readers retain their existing separate admission. This removes the old external-decoder maximum from the inspection's standing reservation rather than charging it twice.

Static re-review checked the strict row decoder's temporary `Value`/typed copies (`inventory.rs:1040-1059`), claim/binding root validation (`owners.rs:205-243`), and the per-object permit lifetime through `expand`. The graph and metadata limits remain enforced, and no new scan or persistent state is introduced. The multipliers remain conservative accounting choices, not measured peak heap or whole-process guarantees. No new actionable finding resulted. `git diff --check` also completed without reported errors; no Cargo or formatting command was run by this reviewer.

The final full gate is running into `docs/reviews/admission-protection-check-final.log`; that log's result must replace the pending status only after the implementation agent confirms its exit.

## Implementation validation closure

The implementation agent confirmed `./scripts/check` exited successfully on the
final code. [The retained log](admission-protection-check-final.log) records passing
formatting, strict all-target/all-feature Clippy, all-feature unit/integration
tests and doc tests. This supersedes the earlier pending execution notes. The
independent reviewer supplied static review and inspected focused results; the
implementation agent executed the checks. No corpus performance run or full
lifecycle qualification is claimed by this result.
