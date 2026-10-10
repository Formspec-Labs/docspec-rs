# Scoped dependency correction: adoption review

Static review on 2026-10-10 of the working adoption changes over
`5508a1144d872e4f72ed636eab02dc0eedc0146c`. **APPROVE the scoped fixes after
re-review.** The retained-target race and repeated source inventory read below
are resolved in the current implementation. This review ran no builds, tests,
formatting, benchmarks or payload scans. The follow-up also added the requested
internal regression fixture in `src/engine/backfill/correction_tests.rs`; it has
not been executed by this reviewer. Broader production qualification remains
with the implementation owner.

## Findings

Original traces below retain the first reviewed line locations. The re-review
paragraphs describe the current fixes and supersede the original findings.

### F1 — Preserve the retained target across correction races — resolved

**Original finding: BLOCKER / correctness.** Original location: `src/engine/backfill.rs:742`, with the
readback acknowledgment at `src/engine/backfill.rs:813-830`.

An existing readback passes the current-requirement guard once at
`backfill.rs:623-625`. Its target binding is then copied at `627-640`. If a
correction commits after that guard but before `qualify_corrections` at `742`,
the qualification pass replaces the target's proof (`corrections.rs:132-144`).
For historical files with adequate same-field evidence, this succeeds. The
result has a different manifest digest, so `backfill.rs:752-755` names a new
readback key. The new-key branch retains it under the same operation ID without
checking the other readbacks' target bindings (`781-835`). The final selection
guard accepts its current proof.

This violates the documented rule that a retained target requiring a new proof
needs a new operation ID. It also creates state that the existing recovery
reader explicitly rejects: `retained_backfill_readback` compares complete target
bindings across readbacks and returns `phase_generation_conflict` when they
differ (`backfill.rs:945-952`). If selection is interrupted, a subsequent retry
cannot recover this operation. A same-turn readback followed by a compatible
sibling-selection retry can reach the same upgrade path; the initial guard is
not revisited by the composition loop.

Before acknowledging a new backfill readback, compare its target binding and
table with any retained readback for that operation using the fresh lifecycle
view under the dataset lock. A changed qualification must produce
`correction_required` and the new-operation instruction, preserving the old
readback. Retain a local frozen target after the first successful readback too,
so ordinary composition retries do not knowingly create incompatible metadata.
The locked check must also cover concurrent attempts that both initially saw
no readback. Add a same-field correction race after retained-readback lookup,
plus a concurrent first-readback case; assert one immutable target and a usable
new-ID recovery path.

**Re-review:** `check_retained_target` compares the complete target table and
binding, including supplemental proof (`src/engine/backfill.rs:1009-1015`).
Composition checks an already loaded readback before serialization (`755-758`).
The acknowledgment path also invokes `check_backfill_target` on the fresh view
while holding the dataset lock (`829-836`); that helper checks every retained
readback for the operation (`991-1005`). It therefore sees readbacks committed
by another worker or an earlier loop iteration even when the attempt's initial
lookup was empty. The guarded native commit uses the same view. A concurrent
cross-process change causes the existing commit guard to refuse rather than
blindly retry. These checks close the reported race while preserving sibling
recomposition with an unchanged target.

The new internal test,
`fresh_readback_view_fences_target_proof_changes_but_allows_siblings`
(`src/engine/backfill/correction_tests.rs:60`), creates and replays a real
production readback, selects a real sibling, records a real correction and
constructs a candidate differing only in target qualification. It verifies
that the supplemental proof itself and the current-requirement guard accept
the candidate, an old view sees no retained target, and the fresh locked view
refuses with `correction_required`. It also checks that sibling recomposition
passes the target guard and that the original readback record, revision,
manifest bytes and selected sibling remain unchanged. This is a deterministic
boundary regression; it does not simulate concurrent scheduling. Execution is
pending the owner's gate.

### F2 — Reuse the source inventory already checked by origins — resolved

**Original finding: WARNING / performance.** Original location:
`src/engine/backfill/origins/checks.rs:85-86`.

`qualification` first calls `check` (`origins/checks.rs:53-61`). On a cold
descriptor, `check` loads the captured source inventory at `441`. It then calls
`correspondence`, which loads the target inventory again at `85-86`. In the
supported unchanged-source case, these are the same exact native reference.
This is actual repeated metadata I/O: `storage::snapshot_files` reads manifest
list metadata and content and each retained manifest at `storage.rs:504-532`.
The origins checker caches schemas and descriptors, but does not retain the
checked source inventory (`origins/checks.rs:25-39`).

Reuse the bounded checked file facts when captured and target references match,
or share a request-local inventory keyed by the exact native reference. Preserve
the caller-specific `ObjectRef`, row-count and completeness checks. This removes
an immediate duplicate inventory read; no throughput or resource improvement
has been measured by this review. A focused counter or withheld-metadata fixture
should establish that a single proof check does not reload the same inventory.

**Re-review:** the originals checker now retains bounded source path/size/row
facts in `source_files` (`src/engine/backfill/origins/checks.rs:25-38`). Both the
originals check (`482`) and supplemental correspondence (`104`) call the same
helper. The helper resolves its key from already identity-checked table metadata,
returns the existing `Arc` on a hit, and charges newly retained facts to the
metadata allowance (`252-277`). This removes the immediate duplicate source
inventory load without retaining full native file descriptors. Before a warm
proof return, correspondence still checks the caller's source object identities
and duplicate paths (`89-101`). The existing cache test now asserts the same
`Arc` is returned with no second admission (`671-677`). This establishes the
intended source path by static inspection; no measured speedup is claimed.

## Patch summary and scope

The patch records requirements for an exact scalar definition, retains a small
inline supplemental proof, and applies current requirements when selecting new
computed results or affected exports. Original SQL declarations, file completion
generations and source interpretations remain separate. The reviewed scope is
`corrections.rs`, its records reader, scalar preparation/origins/verification,
generic and backfill composition, shared selection, exports, and the model.
Retention's separate inventory and owner changes were inspected only as called
interfaces; this report does not independently certify retention/restore closure.

The reference design is `docs/dependency-corrections.md` and the inline-proof
decision in `docs/reviews/dependency-correction-proof-design.md`. Repository,
semi-formal review and Rust guidance were read before source review.

## Function trace

| Function or boundary | File:line | Input and output | Verified behavior |
| --- | --- | --- | --- |
| `scope_key` | `src/engine/corrections/records.rs:21` | Definition → requirement key | Hashes the exact scalar processor and complete definition. |
| `correct_dependencies_admitted` | `src/engine/corrections/records.rs:54` | Correction → decision | Dataset lock; immutable ID reconciliation; cumulative union and stable first revision committed together. |
| `requirements` | `src/engine/corrections/records.rs:112` | Supplied view, definition → state | Reads the directly addressed state and validates definition, processor and native revision. |
| `validate_qualification`, `qualification_key` | `src/engine/corrections.rs:10`, `25` | Proof → validated identity | Requires known checker, scoped key, positive fields/revision and agreeing owners; hashes proof content. |
| `check_association` | `src/engine/corrections.rs:30` | Proof, binding, source → result | Requires exact origins, definition scope, target native reference and dataset owner. |
| `expected`, `candidate` | `src/engine/corrections.rs:46`, `54` | Current requirements and origins → proof | Carries cumulative fields and original first revision; missing origins remains unknown. |
| `inherits`, `inherits_groups` | `src/engine/corrections.rs:70`, `93` | Child and parent → boolean | Keeps every original binding field equal; proof may be added or strengthened, not removed. |
| `check_prepared_correction` | `src/engine/corrections.rs:102` | Canonical prepared origins → result | Evaluates current requirements before the validation-cache branch. |
| `qualify_corrections` | `src/engine/corrections.rs:117` | Candidate dataset → proofs/completions | Handles every included computed group, reuses exact retained proof, or checks and acknowledges a new proof. F1 concerns callers with an already retained target. |
| `guard_corrections` | `src/engine/corrections.rs:166` | Supplied view, dataset, optional group → result | Metadata-only equality against current requirements; optional group filtering precedes lookup. |
| `field_interpretation` | `src/engine/backfill/expressions.rs:217` | Schema, stable ID → normalized field | Shared with SQL compilation; clears name/write default and preserves initial read default/material metadata. |
| `prepare_group` | `src/engine/backfill/preparation.rs:67` | Request, source, definition → prepared group | Converges canonical file and prepared winners on original checks; corrected definitions refuse new missing-file computation (`192-195`); supplemental check runs before validation (`353-354`). |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:463` | Pending completions → retained winners | Existing winners retain their actual revisions; newly committed files use returned native revision (`553-558`). |
| `Checks::qualification`, `correspondence` | `src/engine/backfill/origins/checks.rs:44`, `65` | Proof and exact pins → result | Verifies ordinary origins association, complete immutable source-file correspondence, then required field interpretations. F2 is the repeated inventory. |
| `Checks::correction_file` | `src/engine/backfill/origins/checks.rs:114` | Original file, target schema, proof → result | Requires every positive original acknowledgment revision to precede the first correction; missing declaration/schema/field remains unknown; mismatched or nonprimitive fields refuse. |
| `scalar_qualification`, `scalar_correspondence` | `src/engine/verification.rs:45`, `60` | Binding/proof → result | Reuses the request's originals checker without current-correction lookup. |
| `verify_groups`, `verify_binding_dependencies` | `src/engine/backfill/verification.rs:34`, `7` | Immutable dataset → result | Checks bindings and histories; permits only the narrow supplemental inheritance change. |
| `verify_binding_history` | `src/engine/backfill/verification.rs:404` | Binding owner/history → result | Checks each owning manifest's proof before following parent history (`426`); never loads current requirements. |
| Generic composition/readback | `src/engine.rs:1600`, `1248` | Prepared values or retained readback → selection | New manifests qualify all groups at `1624`; retained readback goes directly to final guarded selection. |
| Backfill composition/readback | Original `src/engine/backfill.rs:620`, `690` | Prepared target/current sibling set → selection | Qualifies all groups before serialization; the new locked acknowledgment guard closes F1 as described above. |
| `select_with_progress` | `src/engine/history.rs:160` | Supplied view and retained release → commit | Checks all computed groups at `169` before writing authority/result/succession. |
| `admit_export`, `check_export_authority` | `src/engine/export.rs:237`, `306` | Export source/group → admission/guard | Checks affected consumed group at admission, unfinished resume, and final selection; completed replay precedes checks (`148-154`, `206-211`). |
| `resolve_release` | `src/engine.rs:2458` | Immutable release → resolution | Compares serialized selection without consulting current correction requirements. |

## Data flow and invariants

- Correction state enters through `DependencyCorrection` (`src/model.rs:820`)
  and produces immutable `CorrectionDecision` plus `Requirements`. The union
  grows while `first_revision` stays fixed (`records.rs:75-95`); it does not
  rewrite original execution metadata or broad eligibility.
- A proof is created from the exact current requirement state, original origins
  object and target native reference (`corrections.rs:54-66`). It enters the
  candidate `ComputedGroup.qualification` and escapes through a completion and
  immutable manifest (`corrections.rs:139-159`, `backfill.rs:745-830`). Original
  definition, SQL bindings and generations remain unchanged.
- Fresh deficient work cannot become an older baseline: every file is checked
  against the first correction revision (`origins/checks.rs:122-125`), including
  canonical retained winners. The pre-compute check is an early refusal; the
  retained revision check closes a correction arriving during computation.
- Final authority comes from the supplied selection view (`history.rs:169`),
  followed by the native guarded commit (`234`). `GuardedCatalog` refuses a
  changed metadata location and marks update errors nonretryable
  (`src/catalog.rs:564-583`). The proof pass may become stale, but stale proof
  cannot pass this final current-state equality check.
- Historical checks validate embedded immutable evidence. Source-only exports
  skip unconsumed computed groups before requirement lookup
  (`corrections.rs:173-178`). No mutable lookup was added to `resolve_release`
  or `verify_binding_history`.
- Readback immutability is an independent rule. A correct proof does not permit
  replacing an already retained target association; the initial F1 broke that
  rule without bypassing the current-requirement selection guard. The new
  acknowledgment check preserves the retained target independently.

## Tests and concrete edge cases

These are static assertion traces, not executed results.

| Test or scenario | Evidence and expected behavior |
| --- | --- |
| `retry_conflict_and_cumulative_corrections_preserve_authority` | `records.rs:226`: checks stable retry, conflicting ID refusal, cumulative fields and unchanged authority/head (`247-279`). Expected to exercise record semantics; it does not test adoption. |
| `lost_commit_response_reconciles_actual_native_generation` | `records.rs:283`: injects loss of the native commit response and checks returned/replayed revision (`287-298`). |
| `invalid_requests_leave_native_revision_unchanged` | `records.rs:303`: rejects empty reason/fields, nonpositive IDs and invalid definitions without a commit (`308-321`). |
| `http_and_cli_reconcile_the_same_decision_after_reopen` | `tests/dependency_correction_transport.rs:26`: checks HTTP decision/refusal shape and CLI replay across engine reopen (`54-123`). |
| Originals cache test | The original test checked changed initial default refusal and warm descriptor/schema reuse. Its current added source-inventory assertions (`origins/checks.rs:671-677`) require shared facts and no second admission. |
| Existing global invalidation test | `tests/lifecycle.rs:582`: calls global `invalidate`, expects `stale_eligibility`, and preserves processing. Its name is not evidence of scoped correction coverage. |
| Correction before reuse, adequate same-field originals | Early correspondence checks all original revisions/fields; expected to qualify without changing originals. Production fixture is being added by the owner. |
| Correction after retained-readback lookup | The fresh-view target guard now refuses changed proof before acknowledgment. `correction_tests.rs:60` covers stale-versus-fresh view behavior and valid same-field proof; actual concurrent scheduling remains outside that helper fixture. |
| Correction after export materialization/validation | Final `check_export_authority` at `export.rs:213` refuses stale consumed-group proof while retaining existing bytes. Completed export replay exits earlier. Runtime qualification remains with the owner. |
| Missing historical schema/field or fresh acknowledgment | `correction_file` refuses unknown/mismatch before allowing proof. No values are relabeled as having consumed the supplemental field. |

## Conclusion

**VERDICT: APPROVE the reviewed adoption implementation and focused fixes.**
F1 and F2 are resolved by the fresh-view immutable-target check and shared
source facts. No remaining actionable defect was found in the reviewed paths.
Coverage of the focused changed boundaries is supported by the new assertions;
the broader adoption test suite and Q4 qualification are **not yet assessed as
complete** because the owner is still implementing and executing them.
Confidence is **high** in the static traces. This review supplies no build,
execution or performance result.

## Final addendum: qualification future size and empty computed groups

Focused static re-review on 2026-10-10: **APPROVE; no concrete issue found.**
Both callers now immediately await `Box::pin(self.qualify_corrections(...))`
(`src/engine.rs:1679-1686`, `src/engine/backfill.rs:761-768`). Boxing changes
where the qualification future's state is stored. It preserves the existing
borrowed dataset, protections and check context, propagates the same errors,
and completes before group verification, serialization and readback. It creates
no detached task and changes no selection or acknowledgment ordering. The
tradeoff is one heap allocation per qualification call; this is not per row or
file and adds no payload read or clone.

The early return for an empty `dataset.computed` precedes the lifecycle open
(`src/engine/corrections.rs:145-150`). There is no scalar binding to qualify in
that case. Both callers still invoke `verify_groups`, which requires the source
group and consistent group membership (`src/engine/backfill/verification.rs:39-43`).
The final current-requirement guard remains in shared selection. The fast path
therefore avoids an unnecessary current-lifecycle lookup without admitting a
missing source or bypassing any affected computed binding.

The retained debugger trace places the failure in SQL parsing, beneath
`process_incremental_join` and `prepare_and_select`
(`dependency-correction-stack-backtrace-full.log:50-55,94,102`); it contains no
live qualification frame. The retained targeted rerun reports the exact
`incremental_phase_reuse_fences_rebased_outputs_and_exact_manifests` test passing
(`dependency-correction-stack-boxed.log:6-8`). Those are the implementation
owner's retained observations, not execution performed by this reviewer. At
this recheck, `dependency-correction-full-check-2.log` showed completed checking
and test compilation in progress; this addendum does not claim a completed full
gate or a measured future-size reduction. No code, tests or build settings were
changed during this final re-review.
