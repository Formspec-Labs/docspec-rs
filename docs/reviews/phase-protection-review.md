# Saved-phase protection review

## Findings

No material implementation defect was established in the reviewed scope. The
[complete repository gate](phase-protection-check-complete.log) passed formatting,
all-target/all-feature Clippy, the library and integration suites, and the
documentation-test stage. Its full log was reviewed, and the author reported
exit status 0. F1 is resolved; F2 remains a nonblocking capacity observation.

This is a static review of the implementation against `b44189b` on
`codex/greenfield-lifecycle`, supplemented by the author's execution logs. The
reviewer did not run Cargo, formatting, tests or benchmarks.
The author's [focused test log](phase-protection-focused.log) was read and records
the three owner-scope/generation regression tests passing. The subsequent
[gate attempt](phase-protection-check-final.log) records the added scalar and
refresh tests passing and two library-fixture failures assessed below. The
[repaired gate log](phase-protection-check-repaired.log) records the full library
suite passing, including all owner/scalar/refresh/schema and corrected legacy
assertions. That gate subsequently stopped at an integration fixture using the
same invalid legacy-rewrite setup. The reviewed narrow fixture migrations passed
in [the targeted integration log](phase-protection-legacy-fixtures.log) and again
in the complete repository gate. These failed attempts remain retained. The
gate's type-complexity correction names the
returned admission/processing pair `PreparedSource`; both return branches and its
caller preserve the previously reviewed values and scopes.

### F1 — Distinct protection handoffs now have focused assertions

- **Severity:** WARNING; test coverage. **Disposition:** RESOLVED by the targeted
  assertions and passing complete repository gate.
- **Locations:** `src/engine/refresh.rs:148`, `src/engine/schema.rs:149`,
  `src/engine.rs:1355`, `src/engine/backfill/preparation.rs:78`,
  `src/engine/backfill.rs:580`.
- **Scenario:** A saved file, evidence phase or group comes from an earlier
  generation, while the current request has a different owner scope. The value can
  be correct even if its new binding accidentally uses the current request's scope.
  A values-only recovery test would miss that failure.
- **Evidence:** The Build tests establish material reuse across different owner
  scopes and unknown retired-generation fallback. The scalar fixture now checks
  exact binding/revision preservation over cached files, assembly, validation and
  restart. The new refresh fixture checks evidence against the exact processing
  snapshot and captured producer scope, then reuses evidence for validation and
  readback. The schema fixture checks the captured base, schema admission,
  coordinated source/group references and readback scope. Shared helpers check
  claim/binding revision equality and resolve exact owner grants. Each new fixture
  also checks that propagation leaves owner revisions unchanged.
- **Action:** No further change is required for this scoped checkpoint. A forced
  concurrent file winner remains a residual case covered by static tracing rather
  than a dedicated protection-specific race test.
- **Verification:** Passing output has been reviewed for all owner-scope, scalar,
  refresh, schema/group and corrected legacy assertions in
  `phase-protection-check-complete.log`. The combined schema fixture uses the
  justified scalar memory profile described below. No bad binding was observed.

### F2 — Binding growth remains a declared metadata-capacity limit

- **Severity:** OBSERVATION; resource use and recovery.
- **Locations:** `src/engine/backfill/preparation.rs:150`,
  `src/engine/backfill/preparation.rs:344`, `src/catalog.rs:315`.
- **Scenario:** Many retained owner references make the per-file binding large.
  The preparation loop chooses its flush point from completion bytes/count; the
  acknowledgment path subsequently includes each binding in the byte limit and
  can refuse the whole pending bundle.
- **Evidence:** The acknowledgment path acquires a duplicate scope's allowance
  before building its binding, charges the full completion/binding records and
  commits them together. The catalog also caps cumulative retained history, so
  splitting a bundle larger than that same limit cannot make the complete
  operation fit. It could preserve a partial prefix, but that is a separate
  recovery improvement.
- **Disposition:** No blocking defect or same-limit complete-operation retry loop
  was established. Keep the capacity refusal explicit. Do not add chunk machinery
  or claim a throughput improvement without a workload that fits the complete
  history and working-memory limits. A small boundary test can check that normal
  admissible bundles succeed and refusal retains previously acknowledged bundles.

## Patch summary

The patch associates saved values with their acquired foreign-owner references,
then propagates the references of the values actually consumed. A `None` scope
means unknown earlier protection; an empty known scope means the qualified value
has no foreign-owner grants. These states remain distinct.

`completion_records` writes a completion and its binding in one guarded catalog
commit. `Retained<T>` keeps a loaded value and its protection together. Scalar
file completions, prepared groups, validation, combined groups and readbacks use
the same helper. Superseded bindings may be ignored only after retirement of
their exact earlier native generation.

The governing requirements are `docs/PLAN.md:69` and the shared phase-completion
rules in `docs/requirements/01-durable-storage.md`, with independent phase roots
and exact retirement from `docs/requirements/06-maintenance-retention.md`.

Important callers are `run_admitted` / `prepare_and_select`
(`src/engine.rs:743`, `src/engine.rs:978`), `backfill_admitted`
(`src/engine/backfill.rs:348`) and the refresh/schema public services
(`src/engine/refresh.rs:54`, `src/engine/schema.rs:46`).

## Function trace

| Function or type | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `Retained<T>` | `src/engine.rs:72` | Value plus optional admitted protection | Keeps the returned generation and its scope together; dereferencing borrows the value. |
| `PreparedSource` | `src/engine.rs:77` | Optional retained admission and retained processing | Gives names to the pair; cached and newly prepared return branches and the caller retain the same values and protection scopes. |
| `completion_records` | `src/engine.rs:90` | Consumer, view, key, value, optional scope → record list | Builds one claim and, when known, its binding against the same next revision. |
| `prepare_source` | `src/engine.rs:777` | Request, captured operation, stop point → `PreparedSource` | Cached processing keeps its original binding. Newly processed output combines captured request and actual admission scopes. |
| `phase` | `src/engine.rs:1582` | Material phase key → optional retained phase | Uses one view for active completion and binding; retains existing output availability checks. |
| `complete` | `src/engine.rs:1674` | Candidate phase and its scope → retained winner | Under the dataset lock, checks eligibility and existing generation. A concurrent winner supplies both returned value and binding. New records commit atomically. |
| `admit_refresh` | `src/engine/refresh.rs:54` | Captured input coverage → retained admission | Uses protection from the same captured request dependencies. No new owner acquisition is introduced. |
| `prepare_evidence` | `src/engine/refresh.rs:131` | Actual processing generation → retained related result | Cached evidence keeps its scope; new evidence duplicates processing's scope. Generation compatibility checks remain. |
| `admit_schema` | `src/engine/schema.rs:122` | Exact base manifest → retained admission | Confirms the captured base and carries request protection into the phase commit. |
| `prepare_and_select` | `src/engine.rs:978` | Captured request and source phases → readback/selection | Validation inherits actual processing/evidence. Group preparation merges its own returned scope. New groups/readback bind the assembled scope; cached groups/readback retain their own bindings. |
| `prepare_group` | `src/engine/backfill/preparation.rs:10` | Source pin/scope, expressions → `PreparedGroup` | Reused file scopes enter assembly. New file work uses the source scope. Prepared/validation winners return their stored bindings. Unknown input scope is never upgraded. |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:316` | Pending files and source scope → durable bundle | Writes each new file claim/binding together with bounded admitted scope copies. A concurrent file winner is not substituted into the assembly that already consumed this attempt's output. |
| `backfill_admitted` | `src/engine/backfill.rs:348` | Backfill request/base → selected release | Reuses retained target readback before preparation. Each composition attempt merges prepared and current-result scopes. If current selection changes, it recomposes. |
| `retained_backfill_readback` | `src/engine/backfill.rs:753` | Request/capture/base → newest compatible retained target | Checks target identities across readbacks, retains the selected key, then loads its binding from the same view. |
| `duplicate` / `inherit` | `src/engine/retention/owners.rs:60`, `:80` | Known scopes → bounded copy/union or unknown | Reserves memory before copying. Unknown on either side remains unknown; known references deduplicate through existing `merge`. |
| `earlier_binding` | `src/engine/retention/owners.rs:294` | Current claim and older binding revisions → earlier-generation flag | Requires the binding's exact generation to have a retirement marker preceding replacement. Callers additionally validate the receipt. |
| `request_protections` / `retained_protections` | `src/engine/retention/owners.rs:389`, `:403` | Exact local claim → admitted references or unknown | Resolves immutable local bindings without re-opening owners; immutable owner grants are the prerequisite. |
| `local_binding` | `src/engine/retention/owners.rs:421` | One native view and claim → validated binding/roots/allowance | Checks same-revision digest binding. An earlier retired generation returns unknown instead of lending its scope to replacement work. |
| `inspect_retention_admitted` | `src/engine/retention/inventory.rs:94` | Exact candidates → protection inventory | Ignores superseded bindings only when the already validated retirement set contains their exact generation. |

## Data flow and invariants

1. **Admission establishes foreign protection before consumer acknowledgment.**
   `protect_admission` acquires owner grants (`src/engine/admission.rs:267`), then
   `PreparedAdmission::records` binds request roots in the consumer commit
   (`src/engine/admission.rs:40`). This patch propagates those existing references;
   it does not infer grants from paths.
2. **A claim and binding share native revision and serialized identity.**
   `completion_records` supplies both records to the same commit.
   `ReferenceProtections::binding` uses `view.revision + 1`
   (`src/engine/retention/owners.rs:146`); `validate_binding` verifies claim revision,
   consumer, ordering and digest (`src/engine/retention/owners.rs:233`).
3. **Reuse follows the consumed generation.** Saved phase reads load the value and
   scope from one view. A new request's scope can add protection but cannot replace
   the earlier value's scope. Concurrent completion winners retain their own pair
   (`src/engine.rs:1696`).
4. **Unknown is contagious.** `inherit` clears a known target when its input is
   unknown and never upgrades an unknown target (`owners.rs:80`). The behavior is
   conservative for earlier records that lack bindings.
5. **Explicit retirement affects one generation.** The earlier-binding path checks
   the old revision's marker and receipt, rather than accepting any retirement at
   the same logical key (`owners.rs:294`, `:456`; `retention/phases.rs:38`).
6. **Owner tokens remain immutable and non-retirable in this checkpoint.** That
   makes local propagation valid without repeating owner reads (`owners.rs:398`).
   Future token retirement must revise this assumption together with removal and
   reference-admission guards.
7. **A file race does not change the assembly's actual bytes.** The assembly may
   have consumed this worker's output before another worker wins the file key.
   Acknowledgment preserves the winner's record, while the assembled table keeps
   this worker's source scope and its own reserved output (`preparation.rs:365`).

Protection scopes escape preparation only inside `Retained<T>` or
`PreparedGroup`. Scope copies retain their permits. Bindings escape as durable
catalog records; no payload transfer, payload digest pass, or new owner commit is
introduced by propagation. Local binding validation does decode the claim again
and collect direct references (`owners.rs:249`, `:275`), so this is bounded metadata
work, not zero-cost reuse. No end-to-end speed or whole-process memory claim was
tested.

## Test behavior and edge cases

The following assertions were read. All listed library and integration tests have
passing author-produced output in `phase-protection-check-complete.log`. Their
static traces agree with the observed results. The reviewer did not execute tests.

| Test | Evidence and expected behavior |
| --- | --- |
| `material_reuse_carries_original_owner_scope_into_validation_and_readback` (`owners/phase_tests.rs:119`) | Same bytes from two owners reuse the original physical phase. It asserts same-revision bindings, original scope in validation/readback, original input identity and unchanged owner revisions. The reviewed propagation supports these assertions. |
| `retired_bound_generation_does_not_lend_scope_to_an_unknown_recomputation` (`owners/phase_tests.rs:226`) | Retires known admission/processing, writes an unbound replacement, and asserts downstream scope stays unknown while stale binding rows remain unchanged. `earlier_binding` plus `inherit` support the expected behavior. |
| `an_earlier_binding_requires_retirement_of_its_exact_native_generation` (`owners/phase_tests.rs:377`) | Rewrites the claim without its binding, then retires a different generation. Both checks must refuse with `invalid_owner_protection`; the old marker lookup is exact. |
| `scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen` (`backfill/protection_tests.rs:116`) | Checks file/prepared/validation claim-binding revisions and exact original owner references across repeated stop points, restart and selection. It asserts unchanged payload paths/sizes/modification times and owner revisions, and explicitly expects unknown backfill readback while selected-result protection is deferred. Passing output was reviewed. |
| `refresh_evidence_inherits_the_exact_captured_producer_scope` (`owners/phase_tests.rs:435`) | Asserts evidence depends on the exact processing snapshot and carries both captured producer scopes. Resuming to readback preserves the evidence generation and carries its exact scope to validation/readback without owner writes. The `binding` helper checks same-revision identity and exact grant coverage. Passing output was reviewed. |
| `schema_admission_and_coordinated_groups_preserve_foreign_base_scope` (`owners/phase_tests.rs:534`) | Asserts schema admission retains the enriched base manifest and original foreign scope; coordinated group source/output references equal the readback pins. Group/readback scopes preserve the original grant, input identity and next definition, without owner writes. Its first scalar operation exhausted the initial test profile; the corrected fixture uses the established scalar profile. Corrected execution passed. |
| `legacy_processing_cannot_adopt_a_replacement_admission` (`retention/phases/tests.rs:145`) | The corrected fixture explicitly retires the bound processing generation, then writes an unbound replacement with unknown dependencies. It subsequently retires/replaces admission and still expects `phase_dependencies_unknown` without mutating processing. This preserves the original generation-order question while keeping protection-format corruption out of its setup. Corrected execution passed. |
| `legacy_group_completions_resume_and_keep_unknown_dependencies` (`tests/backfill_dependencies.rs:156`) | Retires only the prepared/validation generations it will rewrite, then installs their older unbound shapes. Real worker recovery still asserts the exact remaining work, unchanged prepared output and preserved unknown dependencies; new validation may retain the exact snapshots it checked. Both dependency suites passed in the targeted and complete gate logs. |
| `processing_keeps_its_consumed_input_when_an_admission_key_is_rebound` (`tests/phase_dependencies.rs:21`) | Retires the exact key being replaced: processing for the legacy arm, admission for the material-rebinding arms. Processing still returns its original table/input; validation preserves the corresponding known or unknown dependency state. Build, update and legacy arms passed in the targeted integration log. |
| `processing_recovers_after_admission_retirement_and_restart` (`tests/phase_retirement.rs:76`) | Removes the original external locator after retiring admission. Processing and its recorded inputs remain the reused output, with retention inspection and independent verification. The patch preserves this route. |
| `validation_requires_explicit_retirement_after_processing_replacement` (`tests/phase_retirement.rs:273`) | A new physical processing generation cannot use old validation. Existing dependency/table comparisons still refuse before selection. |
| `retained_readback_selects_after_upstream_retirement_without_source` (`tests/phase_retirement.rs:308`) | Cached readback bypasses upstream work after restart; it is not itself phase-retirable. The patch adds local binding validation to that path without requiring upstream phases again. |
| `scalar_preparation_cannot_reuse_validation_from_a_retired_generation` (`tests/phase_retirement.rs:382`) | New assembled metadata conflicts with retained old validation, but file computations remain reusable. The patch preserves both the generation check and file reuse. |
| `evidence_requires_explicit_retirement_after_source_replacement` (`tests/phase_retirement.rs:447`) | Replaced processing cannot silently reuse old evidence. Existing dependency comparison remains before scope propagation. |
| `retained_scalar_readback_recomposes_with_a_new_sibling_after_retirement` (`tests/phase_retirement.rs:509`) | Retained target output survives upstream retirement and joins a later sibling selection. Its own readback binding now travels with the selected retained target. |

Remaining unqualified cases are a metadata-capacity boundary including nonempty
bindings and a forced concurrent file winner. No protection-specific
race test was read that forces a file-completion winner between lookup and
acknowledgment. Static review of that branch found no value/scope substitution.

## Gate-failure assessment

The schema fixture's original profile allocated 256 MiB total, of which the
application admission pool receives 128 MiB (`src/budget.rs:83`). Its first scalar
write has the following simultaneous reservations, all already present at the
review baseline:

| Reservation | MiB under that profile | Evidence |
| --- | ---: | --- |
| Captured native scan inventory | 32 | `src/storage.rs:370`, retained by `src/storage/occurrences.rs:291` |
| Scalar assembly | 32 | `src/engine/backfill/preparation.rs:72` |
| Verification context | 16 | `src/engine/verification.rs:108` |
| Current lifecycle cache | 16 | `src/catalog.rs:204`, `:247` |
| Scalar computation | 8 | `src/engine/backfill/preparation.rs:399` |
| Selected-file reader | 8 | `src/storage/occurrences.rs:355` |
| Native writer | 16 | `src/storage.rs:179` |

The sum is 128 MiB before the existing nonzero request admission
(`src/engine/worker.rs:90`) or the new small protection scopes. This is a static
reservation floor, not observed resident memory. Comparing the relevant baseline
source confirms that the new test profile could not accommodate this complete
scalar path before propagation either. The
[diagnostic log](phase-protection-capacity-diagnosis.log) records capacity failure
during initial backfill; its captured stack is the later attempt to record the
failure through a lifecycle write, so it does not by itself identify the first
reservation that refused.

Using 512 MiB only for this new combined schema/scalar fixture is appropriate.
It matches the scalar protection fixture, while other 256 MiB fixtures and
production/default limits remain unchanged (`owners/phase_tests.rs:40`, `:537`).
This change does not qualify scalar execution at 256 MiB or show a whole-process
memory improvement. No production admission redesign is warranted from this
fixture mismatch.

The legacy fixture originally changed a newly bound completion without changing
its binding. Refusal with `invalid_owner_protection` is correct for that setup.
Rebinding the same `dependencies: None` value would instead hit
`retention_unknown_dependencies` in the typed root walker
(`src/engine/retention/inventory.rs:801`). The final narrow correction retires the
original processing generation and writes a replacement with no binding
(`src/engine/retention/phases/tests.rs:185`). `earlier_binding` then returns unknown
after validating exact retirement, allowing the later admission-generation check
to run. Static trace supports the corrected expectation. Production protection
checks remain unchanged, and corrected execution passed in the repaired gate log.

The same setup correction is appropriate in the integration dependency fixtures.
`tests/backfill_dependencies.rs:74` retires exactly the prepared/validation keys
collected for replacement, before removing their dependency fields in a fresh
view. Resume still asserts no repeated completed work, the exact original
prepared table, preserved legacy shapes and independent release verification
(`tests/backfill_dependencies.rs:110`).

`tests/phase_dependencies.rs:95` derives the retirement key from the actual
replacement record. The legacy branch replaces processing; other branches replace
admission. After the fresh-view write, the unchanged assertions still require
processing's original consumed input and table, and its correct known/unknown
validation dependencies (`tests/phase_dependencies.rs:118`). Neither correction
weakens the behavior under test or changes production guards. Passing results for
both suites, including the unrelated schema-metadata dependency test, were read
in `phase-protection-legacy-fixtures.log`.

The malformed/isolated retention fixtures remain correctly unchanged:
`tests/retention_inspection.rs:130` explicitly expects unknown-dependency refusal,
and `tests/retention_inspection.rs:379` expects altered retained roots to refuse
inspection. Those are corruption/refusal tests, unlike the recovery fixtures.

## Scope limits and simplicity

Selected `result/` and authority binding propagation remains outside this patch:
`select_with_progress` currently writes selection records through the existing
path (`src/engine/history.rs:160`). Consequently a backfill composition using an
unbound current result becomes unknown (`src/engine/backfill.rs:580`), even when
its target preparation is known. This is conservative unfinished selection work,
not a defect requiring the current patch to invent protection.

Deletion, owner-grant retirement, automatic expiration and restore qualification
remain separate unfinished work. This review does not approve enabling them.
The existing typed root walker is reused for binding validation and inventory;
the patch adds no second journal, database, scheduler or payload verifier.
`Retained<T>` and the shared commit helper reduce the opportunity to accidentally
mix a returned value with a different attempt's protection.

## Exploration record

- **H1:** Cached or concurrent completions might receive the current request's
  scope. **Refuted by the reviewed trace:** loaded values and winners load their
  own scope from the same view; genuinely new output inherits consumed scopes.
- **H2:** Retiring and replacing a phase could leave its old binding falsely
  qualifying the replacement. **Refuted for the implemented paths:** exact old
  retirement is required and the replacement returns unknown.
- **H3:** Added binding bytes could create an unavoidable retry loop for an
  otherwise admissible complete operation. **Not established:** the catalog's
  existing cumulative history bound already excludes a larger total bundle.
  Partial-prefix acknowledgment remains a possible future recovery improvement.
- **H4:** Backfill readback cannot yet claim fully propagated selected-result
  protection. **Confirmed and scoped out:** selection propagation is explicitly
  later work; unknown fallback is conservative.

## Conclusion

VERDICT: APPROVE

Justification:

- The implementation achieves the reviewed propagation boundary without a
  demonstrated correctness or removal-safety regression.
- Coverage of changed paths: ADEQUATE for this scoped checkpoint. All targeted
  library assertions passed, including the corrected schema/legacy fixtures;
  the migrated integration dependency fixtures also passed. F1 is resolved.
  The complete repository gate passed on the final reviewed source.
- Confidence: HIGH within the reviewed propagation boundary. Static traces cover
  fresh, cached, concurrent-winner, legacy/unknown and retired-generation branches;
  the author's retained execution logs qualify the exercised paths. The residual
  race/capacity cases and deferred selection/removal work remain explicit above.
