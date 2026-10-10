# Continuous scalar backfill: process recovery and retention review

Reviewed baseline: `d6f39b313b6a95e69bb4c0565b19bc87aac46d98`.
This is an independent static review of the local controller's R3/R6/R7 recovery
and removal boundaries, followed by a static recheck of the author's local repair
and new process/retention fixtures. No builds, tests, benchmarks or commits were
run by this reviewer. References in the original findings identify the baseline;
the follow-up section identifies the reviewed working-tree changes. Unrelated
concurrent changes are outside this review. Author review reports and the
unrelated change-chain review were not read.

## Current disposition after the repair

**Static approval for the bounded repair and qualification fixtures; execution
results remain pending this review.** F1 is repaired. The new fixtures address
the acknowledged-handoff portion of F2 and superseded-output portion of F3. No
additional correctness defect was found in those changes. Live-fence SIGKILL,
death during an in-flight native operation, and the broader race/scaling cases
remain outside the implemented fixtures.

The repair returns `stale_capture` from the already-locked admission view
(`src/engine/backfill/continuous.rs:672–733`). It keeps current parent/child and
target-lineage checks, commits the original exact admission and its protection
bindings, then returns `stale_source_boundary` before opening scalar input or
running preparation (`src/engine/backfill.rs:624–655`). It adds no metadata
reopen or payload pass. A public retry sees the retained child's old capture or
admission boundary and creates the successor through the existing path. If the
process dies after this admission commit but before the stale return, that same
public retry still recaptures; the volatile boolean is not recovery authority.

`first_capture_race_retains_protected_admission_without_computing_obsolete_work`
(`src/engine/backfill/continuous/tests.rs:289–385`) exercises selected-source and
admission-only movement. It checks co-committed parent/child/operation/request/
dependency/capture revisions, calls the production admission-dependency validator,
compares its base with the original exact capture, and asserts that scalar
completion records and the obsolete result do not exist. It then acquires the
fence, resolves the accepted source, and checks fresh selection, old-child
abandonment, owned-fence closure and direct scalar values. This would fail on the
baseline when looking for the absent retained parent.

`tests/continuous_backfill_process.rs:141–255` uses the existing real CLI kill
and resume harness at the advertised durable handoffs. It checks exact child
identity, selection timing, retained completions, omitted/repeated expensive
phases, and zero-work completed retries. Its independent row and occurrence
comparison (`:44–137`) checks duplicates, nulls, empty text, Unicode lengths,
unique source/computed occurrence coverage, and generation evidence.
`killed_parent_catches_up_source_edits_and_keeps_original_file_evidence`
(`:259–370`) adds replacement/insertion after worker death, checks original file
generations and exact new-file work, and resumes without external input files.
Deletion remains covered by the existing in-process catch-up fixture, not this
new killed-process case. `PausedOperation::ContinuousBackfill` uses the shared
CLI pause/resume service, with no separate execution implementation.

`superseded_child_promises_retire_independently_before_exact_removal`
(`tests/continuous_backfill_retention.rs:37–220`) selects exact shared and unique
old output objects from the winning origins. It refuses active-operation
retirement, supersedes the child, retires recovery and phases, and proves that
the old readback still blocks unique-output removal. After availability
retirement, it verifies opposite eligibility for the shared and unique objects,
removes only the latter through the real removal service, and checks exact
effects, retained control/completion records, cold retry, winning-parent replay,
exact combined values and the surviving shared file digest. This exercises the
new control records through actual inventory and removal; it does not claim
simultaneous new-protection or live-reader races.

The new suites are registered in `tests/integration.rs:24–25`. The assertions are
meaningful and proportional to these boundaries. Actual passes must come from
the author's retained test results; this static recheck does not supply them.

## Findings

### F1 — Initial admission races can prevent the bounded fence fallback

**WARNING / correctness / P2.**
Location: `src/engine/backfill/continuous.rs:269`, `:698`, `:994`.

If each optimistic capture becomes stale before its first protected child
admission, the controller exhausts those attempts without retaining its parent.
The next iteration then returns `invalid_continuous_backfill` because fence
acquisition requires that missing parent. This is the source-pressure case for
which the bounded fence is needed. The path refuses without losing accepted work,
but it does not deliver the promised fallback.

Evidence chain:

1. `continuous_intent` initially returns an in-memory `Intent`; it does not commit
   it (`continuous.rs:383–459`).
2. `continuous_attempt` captures the selected source and admission boundary before
   protected admission (`:494–568`).
3. Another source selection, or only a new source admission, can advance that
   state. `continuous_admission_records` returns `stale_source_boundary` before
   adding the first parent and child records (`:698–724`).
4. The outer loop consumes an optimistic attempt for that refusal (`:262–287`).
5. The final iteration acquires a fence before making another child attempt
   (`:269–274`). `check_continuous_fence_owner` refuses when the parent record is
   absent (`:994–997`).

No baseline test exercises this pre-admission interleaving. The existing
continuous recovery tests start after protected admission or later phases
(`tests/continuous_backfill.rs:123–176`).

**Minimum repair:** preserve the existing atomic parent/first-child protected
admission. Permit an exact captured child to be admitted after the live
source/boundary changes, while retaining the definition, eligibility, target
lineage, child-identity and prepared-admission checks. After that commit, report
the stale capture before expensive group work. The next attempt can supersede
the child through the existing abandonment path, and the fence can now resolve a
durable owner. There is no need for a separate parent protection graph.

This is compatible with the shared admission implementation:
`protect_admission` retains the exact base version and owner protections
(`src/engine/admission.rs:170–175`, `:285–302`); `PreparedAdmission::records`
binds them to operation, request, dependency and capture records in the same
commit (`:48–70`); `prepare_backfill` checks the current admission view before
that commit (`src/engine/backfill.rs:554–627`). Retain those checks.

Simply removing the stale-capture guard and waiting for promotion also preserves
source selection safety: promotion compares the exact source, selected boundary,
and captured admission fence (`backfill.rs:1055–1064`). It would perform avoidable
work on an already stale source. Also, admission-only movement followed by exact
abandonment can legitimately leave the same source eligible: the current
`Capture::admission_fence` records the admission view (`backfill.rs:604–605`),
while `Child::admission_boundary` records the earlier capture. An explicit
post-admission stale return makes the retry behavior clear without relabeling
either capture.

**Minimum regression:** use private helpers in
`src/engine/backfill/continuous/tests.rs` to capture an attempt, advance source
selection before `prepare_backfill`, and repeat for admission-only movement.
Assert parent/child/operation/request/protection records commit together, stale
work performs no scalar computation, the retained intent can acquire its fence,
and recapture selects the exact current source. Preserve the existing
abandoned/completed-parent fence refusals.

### F2 — Reopen tests do not qualify continuous worker death

**WARNING / test-coverage.**
Location: `tests/continuous_backfill.rs:123–176`; `src/main.rs:296–306`.

The baseline controller suite injects a returned error, drops the engine, and
reopens it. The real-process harness supports ordinary scalar backfill, but its
pause-operation enum does not include continuous backfill. The controller's
acknowledged parent/child admission and parent selection therefore lack actual
SIGKILL coverage. R3-T03 and R7-T04/T05 require recovery evidence beyond orderly
returns (`docs/requirements/03-schema-backfill.md:55`,
`docs/requirements/07-operations-qualification.md:48–49`).

Reuse `tests/support/mod.rs::kill_after`, `resume`, and `run_cli` (`:12–132`).
The harness verifies signal 9 and records which expensive phases execute on
resume. The closest complete example is
`tests/backfill.rs::killed_backfill_workers_resume_retained_files_validation_and_readback`
(`:226–323`). Add the continuous CLI kind to that maintained path.

A live-fence death test needs a different pause location from an ordinary
returned fault. `continuous_backfill_admitted` closes its fence before returning
any outcome (`continuous.rs:294–297`), while `pause-after` emits its marker only
after the service returns (`main.rs:430–433`). A normal `stop_after` immediately
after acquiring the fence would therefore acknowledge an already-closed fence.
Pause inside the live controller, or observe a live hosted controller before
killing its process. Do not call the ordinary returned-fault test a live-fence
death test.

### F3 — Superseded-child reclamation has no end-to-end protection proof

**WARNING / test-coverage.**
Location: `tests/continuous_backfill.rs:180–275`, `:347–401`, `:717–792`.

The suite proves unchanged-file reuse across catch-up, parent abandonment, and
protection while an unfinished or selected result is retained. It does not end
each superseded child's lifetime independently and then physically remove its
unique outputs. The existing generic removal tests do not exercise the new
continuous control records and shared scalar generations together. This leaves
the controller-specific R6-T04/T05 and Q3 acceptance case open
(`docs/requirements/06-maintenance-retention.md:46–47`,
`docs/requirements/07-operations-qualification.md:105–107`).

Three separate promises can protect a superseded child stopped after readback:

- Operation/capture recovery and owned attempt paths.
- Exact completed file, prepared-group and validation generations.
- Standalone `backfill-readback/<child>/...` availability.

`backfill-readback` belongs to result availability, not phase retirement
(`src/engine/retention/retirement.rs:19–64`). Availability retirement accepts
unselected readbacks only after exact abandonment
(`src/engine/retention/availability.rs:168–185`, `:228–235`). Superseding a child
already creates that abandonment in the replacement admission commit
(`continuous.rs:726–735`, `:739–766`). Do not weaken these independent scopes to
make the removal fixture pass.

No unsafe deletion path was established in this static review. The shared
inventory contains the necessary relationships: parent/control records validate
without adding payload lifetime (`continuous.rs:1060–1145`;
`retention/inventory.rs:1524–1533`), while selected scalar origins retain the exact
original source, schema metadata, descriptors, outputs and declaration
(`retention/inventory.rs:926–965`, `:1569–1576`). Runtime qualification must prove
that these relationships protect shared bytes after old promises end.

## Patch summary and function trace

The controller adds one immutable parent request and successive exact source
captures. Child operations reuse the existing scalar admission, processing,
validation, readback and guarded selection services. A host-bound source fence
temporarily blocks new admissions while accepted source work drains. Parent
completion and owned-fence closure join the existing child-selection commit.
The CLI and HTTP invoke the same engine method
(`src/main.rs:680–688`, `src/http.rs:703–708`).

| Function | Baseline location | Inputs → output | Verified role |
| --- | --- | --- | --- |
| `continuous_backfill_internal` | `continuous.rs:230` | Spec/stop → release | Validates request and enters owned worker/maintenance protection. |
| `continuous_backfill_admitted` | `continuous.rs:247` | Immutable spec → result/error | Recovers completion first, retries exact captures, fences/drains, then closes owned fence. |
| `continuous_intent` / `check_continuous_parent` | `continuous.rs:383`, `:462` | Spec/view → original meaning/eligibility | Freezes authored fields and refuses abandonment or writer/eligibility changes. |
| `continuous_attempt` / `continuous_admission_records` | `continuous.rs:494`, `:672` | Intent/live view → exact child/admission records | Reuses identical capture or admits a successor with exact old-child abandonment. |
| `prepare_backfill` / `prepare_group` | `backfill.rs:508`; `preparation.rs:67` | Exact child → retained scalar files/assembled group | Shared protected admission, computation, validation and readback; checks source field interpretation. |
| `promotion` | `backfill.rs:1032` | Authority/capture/source → eligibility | Refuses stale source, boundary, definition or target. |
| `select_continuous_backfill` / `select_with_records` | `continuous.rs:770`; `history.rs:174` | Qualified release/attempt → lifecycle commit | Rechecks child/lineage/fence, commits result, parent completion and fence closure together. |
| `continuous_result_in` | `continuous.rs:322` | Parent/digest/view → original release | Validates atomic child selection and honors winning-child availability retirement. |
| `acquire_source_fence_guarded` / `check_source_fence` | `intake.rs:109`, `:165` | Parent/view/deadline → exact local fence | Checks parent under dataset lock; checks host, token and monotonic expiry. |
| `drain_continuous_source` | `continuous.rs:831` | Live fence → resolved admissions/error | Scans only changed lifecycle revisions; retains pending source operations. |
| `close_source_fence` | `intake.rs:197` | Exact token → optional close commit | Old closes cannot close another generation. |
| `abandon_continuous_in` | `continuous.rs:1008` | Parent/reason → abandonment | Atomically stops current child and closes owned fence; keeps outputs. |
| `validate_control_record` / inventory `record` | `continuous.rs:1061`; `retention/inventory.rs:1054` | Control rows → validated metadata | Preserves control receipts without creating a duplicate payload promise. |
| Inventory `file_completion` / `computed` / `expand` | `retention/inventory.rs:947`, `:926`, `:1569` | Selected/recoverable origin → exact object closure | Retains reused file objects independently of the original phase's active status. |
| Operation/phase/availability retirement | `retention/recovery.rs:321`; `retirement.rs:352`; `availability.rs:263` | Exact logical promise → durable retirement | Leaves records available to explain history while ending the named protection. |
| `plan_removal` / `remove_objects` | `retention/removal.rs:160`, `:254` | Exact candidate list → planned/reserved effects | Reuses shared inventory; effects enter exclusive maintenance. |

## Data flow and invariants

- `Intent` is created from the original request, selected source and authored field
  IDs, then first retained with a protected child. Its digest owns every child;
  immutable request conflicts refuse (`continuous.rs:383–459`, `:624–668`). F1 is
  the gap between constructing this value and retaining it.
- `Child` fixes capture, round, predecessor and request digest. State points to
  the current child; prior owner records remain immutable. Co-committed admission
  revisions are checked on recovery (`continuous.rs:121–175`, `:734–735`).
- Completed scalar work can outlive the child that computed it. Catch-up copies
  original completion/generation information into selected origins rather than
  attributing reused computation to the new child
  (`src/engine/backfill/preparation.rs:24–56`).
- `Completed.selected` names the exact winning child. Its record must share the
  result/succession revision, and no parent abandonment may coexist
  (`continuous.rs:1094–1118`). A later selected source cannot change the parent
  retry result (`:255–257`, `:367–380`).
- Fence identity belongs to one local host instance and monotonic deadline.
  Reopening creates a new host UUID (`src/engine/worker.rs:20–27`), so the previous
  host's fence is inactive (`intake.rs:35–43`). Local exclusive workspace ownership
  is part of this argument; it does not qualify multi-host or remote leases.
- Ordinary work holds shared maintenance protection; retirement/removal effects
  require exclusive maintenance (`worker.rs:120–165`,
  `retention/retirement.rs:499–515`, `retention/removal.rs:254–260`). A queued
  cleanup cannot remove an active worker's input merely because an old child
  becomes abandoned.

## Minimum meaningful qualification

| Case and highest-risk ordering | Reuse | Required independent assertions |
| --- | --- | --- |
| Initial capture → source/admission advance → initial admission → fallback | Private continuous fixture/helpers at `continuous/tests.rs:14–87` | F1 regression above; exact protected admission, no obsolete computation, usable fence and current-source completion. |
| SIGKILL after parent/child admission, file completion, preparation, validation, readback and selection | `support::kill_after` / `resume`; `backfill.rs:226–323` test pattern | Old selection before cutover; exact parent result after cutover; retained records unchanged; only missing expensive phases execute; remove external fixture input before resume; direct Rust row/null/empty/Unicode/duplicate expectations. |
| SIGKILL after old-child readback → source replacements/deletions/inserts → cold parent resume | `continuous_backfill.rs:180–275`, scalar fixture `tests/support/scalar.rs:63–115` | New child, old child exactly abandoned, current source/boundary selected, original generation retained for unchanged files, independently rebuilt expected values. |
| Live fence → accepted source still pending → SIGKILL → new host | Existing fence tests `continuous_backfill.rs:551–638`, `intake.rs:339–363`, with a live pause as described in F2 | Old lease stops blocking new intake; pending source identity survives; no premature parent selection; resume/resolve that source, then parent reaches exact current source. |
| Old child readback → catch-up reuses some files → retire old promises one at a time → remove unique old objects | Existing retirement recipe `retention/removal/tests.rs:49–92`; effect/reopen assertions `:161–209` | Before final applicable retirement, unique old objects remain blocked; after it, only unique outputs become eligible. Reused scalar output plus descriptor/declaration remains blocked. Exact removal outcomes and file absence agree; selected combined values and origins remain exact before/after reopen; parent retry still returns its winning child. |
| Saved removal preview → current/new protection → removal execution | Shared removal service and its existing fresh-eligibility tests | Candidate scope can shrink, never expand; protected selected/shared objects survive. Limit controller-specific additions to protections introduced by its catch-up path. |

Use objects actually produced by the old child. Select one unique output by exact
object identity absent from the winning origins, and one reused output explicitly
present in them. A boolean blocker assertion over only shared files would not
establish successful reclamation; deleting only unrelated scratch would not
exercise scalar retention. A retirement after readback must include its
availability promise. Parent control records should remain readable after old
child payload removal.

Existing baseline test traces support these narrower conclusions:

- `cold_retry_recovers_every_durable_handoff_with_exact_parent_result`
  (`tests/continuous_backfill.rs:123`) checks returned faults and cold engine
  reopen; it does not execute signal termination.
- `interrupted_parent_catches_up_replacements_deletions_and_insertions` (`:180`)
  checks exact direct row expectations and mixed original/new file generations;
  it does not execute retirement/removal.
- `parent_abandonment_survives_reopen_and_keeps_completed_files` (`:347`) checks
  retained metadata, refusal and unchanged selection after parent abandonment.
- `selected_parent_retries_exact_child_after_successor_and_honors_availability`
  (`:404`) checks exact completed replay and subsequent `availability_retired`;
  temporary fixture quarantine is not a removal-service qualification.
- `active_fence_refuses_new_intake_but_drains_already_accepted_source` (`:551`)
  checks a live same-process fence and atomic result/parent/fence revisions.
- `expired_drain_returns_pending_ids_reopens_intake_and_preserves_work` (`:641`)
  checks bounded expiry and explicit resolution of pending operations.
- `restore_point_recovers_unfinished_parent_and_protects_active_and_selected_outputs`
  (`:717`) checks fresh-catalog restore and positive blockers, not removal after
  those promises end.

These are static interpretations of assertions, not newly observed passes.
Tests must remain in the existing registered integration suite unless they need
separate process isolation (`Cargo.toml:7–29`; `tests/integration.rs:23`). The
acknowledged-handoff matrix does not kill an in-flight native commit, hash or
unlink. Broader process windows, related-input races and scaling stay explicitly
unqualified after this bounded work.

## Exploration record and conclusion

H1: the parent can always enter its fence after exhausting optimistic attempts.
**Refuted** by the pre-admission race in F1.

H2: parent completion is recoverable independently of later source movement.
**Supported statically** by the atomic selection receipt and availability check;
actual continuous SIGKILL qualification remains F2.

H3: ending superseded-child recovery preserves selected reused generations and
can release unique objects. **Supported by the shared ownership paths**, with no
concrete unsafe deletion found; the missing lifecycle test remains F3.

**Baseline verdict: REQUEST CHANGES.** The baseline has F1 and lacks the
continuous process/removal assertions described above.

**Follow-up verdict: APPROVE the scoped implementation and fixture design.**
F1's repair preserves protected admission and avoids obsolete scalar work. The
new assertions provide **ADEQUATE** coverage design for acknowledged continuous
handoffs, catch-up after death, and exact superseded-output removal. Runtime
qualification remains pending the author's runs. Confidence is **HIGH** for the
repair's static trace and **MEDIUM** for composed process/retention behavior
before those results. Live-fence death and broader races remain explicit limits,
so this verdict does not establish full R3/R6/R7 completion.

## Root-run evidence update

The root agent's retained
[targeted-run log](continuous-lifecycle-scalar-targeted-2026-10-10.log)
now supersedes the pending targeted-execution status above. The root agent
reported strict Clippy success; the log records successful checking and passing
backfill library, continuous controller/process/retention, and initial scalar
projection cases. In particular, it records passes for the protected
first-capture race regression, both real-process recovery cases, and exact
superseded-child retirement/removal. No failures are recorded in these targeted
runs. This reviewer inspected the existing log and did not execute or rerun it.

The results support the bounded repair and tested handoff/removal behavior;
they do not qualify SIGKILL while a fence remains live, death during an in-flight
native operation, or broader concurrent reader/protection/removal races. The
complete repository gate remains pending the separate scalar-review correction.
Full R3/R6/R7 completion and wider performance qualification remain outside this
evidence.
