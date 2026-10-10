# Continuous scalar backfill: resource and recovery review

**Decision: approve the corrected local controller as a static implementation
review. No blocking resource finding remains.** The controller reuses existing
scalar processing, completion, selection, and retention paths. This review does
not establish measured convergence, whole-process memory, total retained-disk
bounds, or full R3 completion.

Scope: uncommitted changes over `c38cebdf8e9ab48a536d881f403f2159a267fd92`,
including final corrections inspected on 2026-10-10. The reviewer read repository
instructions, Rust guidance, semi-formal review guidance, `docs/PLAN.md`, and R3/R6
requirements. The reviewer did not read author reviews or
`change-chain-implementation-review.md`, and ran no builds, tests, benchmarks, or
destructive operations. The implementation owner owns execution evidence. The
reviewer wrote only this report.

## Findings and disposition

### F1 — Decoded controller metadata admission: corrected

**Original severity: BLOCKER. Category: resource correctness. Status: closed by
static recheck.**

The initial controller decoded full initial/current/winning releases and full
`Authority` records without reserving those additional copies. `Lifecycle::get`
only decodes cached JSON; the cache's reservation covers serialized rows
(`src/catalog.rs:39`, `src/catalog.rs:102`, `src/catalog.rs:186`). Request admission
covers the supplied specification (`src/engine/worker.rs:100`). The original
lineage reservation began after its caller had already allocated endpoint copies.

The corrected controller uses `SnapshotRows::reserve_decoded` before the affected
reads and retains those permits through their use:
`continuous_result_in` (`src/engine/backfill/continuous.rs:330`, `:369`),
`continuous_intent` (`:394`, `:416`), `check_continuous_parent` (`:476`),
`continuous_attempt` (`:501`), `continuous_target` (`:577`), and
`continuous_admission_records` (`:686`). `ControlAuthority` decodes only epoch,
eligibility, and definition; `CaptureAuthority` decodes one release inventory
(`src/engine/backfill/continuous.rs:23`). The final selection now moves `Authority`
through the shared selector instead of making an extra full clone (`:770`, `:820`).
This closes the introduced full-metadata accounting gap and removes an avoidable
copy. It is not a measured RSS improvement or a proof that all native allocations
are bounded.

The final shared-child-validator recheck preserves this disposition.
`continuous_child_in` reserves the previous child before validation follows that
reference (`src/engine/backfill/continuous.rs:197`). `continuous_result_in`
validates the winning child, then reserves the intent, completion, child, and
succession records before completion validation (`src/engine/backfill/continuous.rs:347`).
The separate full-result reservation remains in place (`:369`). No new blocking
resource finding arose from these final edits.

### F2 — Live fence and restore fixture coverage: assertions added; execution separate

**Severity: OBSERVATION. Category: qualification.**

The initial suite tested interruption followed by source edits between controller
calls, plus fence primitives. During review, the implementation owner added:

- `active_fence_refuses_new_intake_but_drains_already_accepted_source`
  (`tests/continuous_backfill.rs:551`): reaches the fence with an unresolved
  accepted request, refuses new intake without an admission record, resumes the
  accepted source, checks exact output values, and checks that selection, parent
  completion, and fence close share one native lifecycle revision.
- `expired_drain_returns_pending_ids_reopens_intake_and_preserves_work`
  (`tests/continuous_backfill.rs:641`): checks fence expiry, preserved pending IDs
  and completed work, reopened intake, explicit source abandonment, and retry.
- `restore_point_recovers_unfinished_parent_and_protects_active_and_selected_outputs`
  (`tests/continuous_backfill.rs:717`): inspects retained output protection,
  creates a restore point, replaces the isolated fixture catalog at its original
  workspace path, resumes the same pending child, and checks output values and
  retained completion records.

These assertions address the missing fixture paths. This reviewer did not execute
them or read a passing log. Their timeouts bound a hung correctness fixture; they
are not performance gates. Superseded-child retirement/removal races, process-kill
recovery, representative continuous-input performance, and peak/retained storage
qualification remain separate R3/R6 work (`docs/requirements/03-schema-backfill.md:55`,
`docs/requirements/06-maintenance-retention.md:46`).

### F3 — Completed plan's later-history scan: removed

**Original severity: OBSERVATION. Category: simplicity/performance. Status: closed.**

The original plan resolved a completed parent and then scanned all later source
history. The corrected `plan_continuous_backfill` returns that exact completed
plan immediately, with no pending work or active fence for the completed parent
(`src/engine/backfill/continuous.rs:881`). It no longer retains the decoded result
while proving unrelated later backlog. This removes unnecessary work and avoids a
later history-capacity refusal hiding the parent's completed plan. No speedup was
measured.

## Patch and decision summary

One immutable scalar-field request owns a parent identity and exact captured-source
child requests. The controller retries compatible source movement, then temporarily
refuses new source admissions while accepted work drains. The winning result,
parent completion, and live owned fence close join the existing guarded selection
commit (`src/engine/backfill/continuous.rs:247`, `:770`, `src/engine/history.rs:174`).
The deadline is checked at the final shared commit boundary
(`src/engine/history.rs:261`).

The useful product outcome is adding a scalar group while ingestion advances
without discarding completed file work. CLI and HTTP call the same engine methods
(`src/main.rs:680`, `src/http.rs:696`). The controller adds no independent scheduler,
bulk-value journal, storage backend, or scalar computation implementation.

| Requirement or sibling | Relationship and evidence |
| --- | --- |
| One selected dataset state | Extra parent/fence records join shared guarded selection (`docs/PLAN.md:23`, `src/engine/history.rs:260`). |
| Do useful work once | Retained readback, prepared groups, and canonical file completions resolve before recomputation (`docs/PLAN.md:44`, `src/engine/backfill.rs:640`, `src/engine/backfill/preparation.rs:103`, `:154`). |
| R3 strict cutover | Existing exact-source promotion and complete admission proof precede continuous selection (`docs/requirements/03-schema-backfill.md:45`, `src/engine/backfill.rs:893`). |
| R6 independent phase lifetimes | Child supersession records abandonment; completed phases remain independent roots (`docs/requirements/06-maintenance-retention.md:27`, `src/engine/backfill/continuous.rs:739`, `src/engine/retention/inventory.rs:1330`). |
| Restore | Uses common inventory and existing catalog/dependency capture (`docs/PLAN.md:104`, `src/engine/restore.rs:179`, `src/engine/retention/inventory.rs:140`). |

## Function and relationship trace

In this table, abbreviated implementation paths are beneath `src/engine/`.

| Function or method | Inputs → outputs; verified static behavior |
| --- | --- |
| `validate_spec`, `validate_intent`, public/internal run (`backfill/continuous.rs:66`, `:102`, `:213`, `:230`) | Specification → admitted worker/result. Checks intent and attempt/fence bounds. Existing worker owns lifetime after caller cancellation (`worker.rs:120`, `:226`). |
| `continuous_backfill_admitted` (`backfill/continuous.rs:247`) | Intent → selected child/refusal. Completion first, bounded per-call attempt loop, fence/drain, shared `prepare_backfill`, exact-token close, lost-response lookup. |
| `continuous_completed`, `continuous_result_in` (`backfill/continuous.rs:310`, `:322`) | Parent/digest → exact available child result. Locked metadata lookup validates atomic completion and availability without reopening historical payload. |
| `continuous_intent`, `check_continuous_parent` (`backfill/continuous.rs:383`, `:462`) | Original source/schema → immutable expression field IDs and authority. Compiles once for new intent; checks abandonment, epoch, eligibility, and writer definition. |
| `continuous_attempt`, `continuous_target` (`backfill/continuous.rs:494`, `:571`) | Intent/current selection → exact child. Same capture/boundary resumes the child; otherwise derives a stable child identity and checks target history. |
| ID/child/capability checks (`backfill/continuous.rs:594`, `:612`, `:911`) | Operation/view → permission/refusal. Refuses public child execution and conflicting parent IDs; parent abandonment reaches existing completion checks. Source admission calls the ID check (`src/engine.rs:998`). |
| `continuous_admission_records`, `continuous_terminal_child` (`backfill/continuous.rs:672`, `:739`) | Captured attempt → existing admission records. Rechecks capture after unlocked owner protection; commits parent/child/state together; supersedes old child without deleting phase output. |
| `prepare_backfill` (`backfill.rs:508`) | Child/attempt → retained or selected result. Reuses retained readback or shared group preparation, then checks source, admissions, ancestor eligibility, and continuous selection. |
| `prepare_group`, `compute_file`, `acknowledge_files` (`backfill/preparation.rs:67`, `:590`, `:497`) | Source files → scalar output and durable completion. Existing batch/writer path reuses canonical files, processes missing files, and acknowledges output plus coverage. |
| `check_continuous_source_schema` (`backfill/continuous.rs:947`) | Already-open native schema → original field-ID/name check. The caller reuses its existing native open (`backfill/preparation.rs:82`). |
| Target-lineage helper and decoders (`history/target.rs:51`, `:162`, `:248`, `:277`, `:314`) | Endpoints/retained selections → proof/refusal. Bounded metadata traversal checks target competition/reversions, IDs, request receipts, and composition lineage. |
| `select_continuous_backfill`, `select_with_records` (`backfill/continuous.rs:770`, `history.rs:174`) | Qualified child/locked authority → atomic selection. Checks parent/state/target and latest related inputs; shared selector appends completion and exact fence close. |
| `drain_continuous_source` (`backfill/continuous.rs:831`) | Fence → empty accepted backlog/refusal. Rechecks liveness, rescans backlog only on changed lifecycle revision, releases lock, sleeps with capped backoff. |
| Fence liveness/acquire/check/close (`intake.rs:35`, `:46`, `:85`, `:109`, `:165`, `:197`) | Host/token/deadline → admission eligibility. Owned local catalog only; live retries preserve deadline; stale close cannot affect successor. Parent termination and acquisition share a lock/view. |
| `plan_continuous_backfill`, `check_continuous_fence_owner`, `abandon_continuous_in` (`backfill/continuous.rs:859`, `:985`, `:1008`) | Parent → progress, fence eligibility, or abandonment. Completed plan is immediate; abandonment closes its fence and retains completed work. |
| Control validator/inventory dispatch (`backfill/continuous.rs:1061`, `retention/inventory.rs:1524`) | Control records → validity. Common retention/restore inventory recognizes the records without adding a second payload lifetime. |

## Data flow and invariants

**H1: catch-up reuses completed computation. Confirmed statically.** Child identity
changes with source capture/admission boundary; canonical file identity uses exact
source object and material expression dependencies, not child ID
(`src/engine/backfill.rs:1132`). The preparer looks up that identity before reading
and computing the file. It adopts existing output descriptors and original
execution records (`src/engine/backfill/preparation.rs:154`, `:25`). A changed
physical source file is the conservative recomputation unit. This does not prove
row-level minimal work or reuse across arbitrary physical rewrites.

**H2: new full metadata decodes participate in application admission. Confirmed
after F1 correction.** Caller reservations cover their decoded endpoints; the
history helper reserves and charges its own traversal before decoding, retains
current/child target descriptions, and yields (`src/engine/history/target.rs:25`,
`:67`, `:85`, `:157`). Native library memory and total process RSS still require
measurement.

**H3: parent/child termination preserves independent recovery work. Confirmed
statically.** Abandonment does not retire prepared, validated, or readback roots.
Common inventory follows them; exact retirement scopes separate phase recovery,
operation recovery, and result availability (`src/engine/retention/inventory.rs:1330`,
`:1364`, `src/engine/retention/retirement.rs:44`). Operation retirement keeps
admission identity readable (`src/engine/retention/recovery.rs:45`). Parent
completion honors winning-child availability retirement and adds no permanent
second lifetime. The restore fixture now tests the intended integration, subject
to its separate execution result.

The immutable intent persists original meaning. Each child persists exact source,
admission boundary, and predecessor identity. State selects the active child.
Completion exists only with winning selection. Fence token/host/deadline persist
local control authority. Their number grows with attempts, while payload ownership
remains in ordinary child admission, completion, readback, and result records.

| Resource dimension | Actual boundary and remaining cost |
| --- | --- |
| Bulk values | `compute_file` streams occurrence batches through expressions and the maintained writer (`backfill/preparation.rs:613`). No full-dataset value materialization is introduced by this controller. |
| Metadata/history | For H accepted selections, O admissions, and F source files, target proof traverses captured history; pending-source proof traverses history and operations; preparation visits F files even when outputs are reused (`history/target.rs:86`, `history/admissions.rs:76`, `:111`, `backfill/preparation.rs:154`). The metadata allowance can refuse growing history; time is not independent of H/O/F. |
| Repeated proof | Capture, admission after unlocked protection, and final selection cross distinct mutable-state boundaries (`backfill/continuous.rs:509`, `:711`, `:791`; `backfill.rs:570`). Fresh checks are justified. Each proof reparses the chain using the existing exact-snapshot row cache (`src/catalog.rs:208`). No general proof cache is recommended without measured need. |
| Time/fence | Per-call attempts are bounded; durable child sequence can advance across retries (`backfill/continuous.rs:262`, `:535`). Fence lifetime is capped at 300000 ms and tied to local host identity (`intake.rs:7`, `:35`). Drain yields. The fence does not impose a hard wall-time limit on all computation or native commit I/O (`backfill/continuous.rs:818`). |
| Peak disk | Existing processing checks current free space against a source-size estimate plus scratch before writing (`backfill/preparation.rs:602`). No second full-data spool is added. This estimate is not a hard bound on arbitrary output expansion, coexisting versions, or whole-filesystem use. |
| Retained disk | Reused outputs remain shared. A new capture can retain new native assembly, declaration/origins, validation, readback, failed-attempt material, and newly computed files (`backfill/preparation.rs:122`, `:324`, `backfill.rs:819`). Supersession does not reclaim these bytes; explicit retirement controls their lifetime. |
| Restore | Common inventory retains native registration closures and recognizes controls (`retention/inventory.rs:217`, `:1524`, `restore.rs:181`). Old-host fences expire on reopen (`intake.rs:37`). Fixture assertions are not broad destructive-maintenance/provider qualification. |

## Tests and edge cases inspected

No tests were executed by this reviewer. These are the read assertions and their
expected paths, not claims of passing runs.

| Test | Assertion scope |
| --- | --- |
| `cold_retry_recovers_every_durable_handoff_with_exact_parent_result` (`tests/continuous_backfill.rs:123`) | Reopen at retained admission/file/processing/validation/readback/selection boundaries; remove original input; expect exact result, unchanged completions, and independently computed scalar values. |
| `interrupted_parent_catches_up_replacements_deletions_and_insertions` (`tests/continuous_backfill.rs:180`) | Expect inherited original executions plus new executions, exact membership/values, and new child identity after between-call source edits. |
| Parent conflict/child entry-point refusal (`tests/continuous_backfill.rs:279`) | Changed parent meaning and public child execution/planning refuse without mutation. |
| Parent abandonment (`tests/continuous_backfill.rs:347`) | Run/plan refuse after reopen while completions remain identical. Current plan checks an incomplete parent's authority/abandonment after completion-first lookup (`backfill/continuous.rs:891`). |
| Completed retry/availability (`tests/continuous_backfill.rs:404`) | Withhold old-base payload after its availability retirement; exact child metadata still resolves; later child availability retirement refuses. This fixture does not physically remove files through the removal service. |
| Active fence, expiry, restore (`tests/continuous_backfill.rs:551`, `:641`, `:717`) | See F2. Tests now exercise actual controller drain/selection and original-path fresh-catalog recovery. |
| Target-lineage tests (`src/engine/history/target/tests.rs:231`, `:317`, `:354`, `:378`, `:403`, `:446`, `:481`, `:522`) | Accepted sibling/source steps, equal endpoints, competing/reverted target refusal, ID/binding/receipt mismatch, and capacity refusal. The historical-retirement fixture writes markers directly; it is not cleanup qualification. |
| Fence primitives (`src/engine/intake.rs:239`, `:251`, `:264`, `:290`, `:339`) | Deadline/host, exact token, malformed record, non-extending retry, stale close, and reopened-host behavior. |

## User value, counterfactuals, and qualification

Keep this controller and the shared preparer. Removing the controller would leave
callers managing captures and retries themselves and lose parent-level recovery
through source movement. Replacing child completions with another progress store
would duplicate existing identity, protection, and retention without demonstrated
benefit. The corrected code removes unnecessary inventory copies and completed-plan
history work without adding infrastructure.

The design fails its value test if a fixed small source change recomputes unchanged
scalar files, if retained storage exceeds the supported budget without an explicit
retirement path, or if accepted input cannot reach complete fenced selection under
the declared workload. Measure those cases while increasing population, files,
and history. Record bytes read/written, file reuse, catalog work, foreground
latency, peak memory, peak additional disk, and retained disk; count shared objects
once. Existing R3/R6 acceptance already calls for this evidence, so no second
diagnostic framework is needed (`docs/PLAN.md:152`,
`docs/requirements/03-schema-backfill.md:63`,
`docs/requirements/06-maintenance-retention.md:53`).

## Verdict

**VERDICT: APPROVE for the reviewed static local-controller implementation.**

- Intent versus shape: matches the focused continuous scalar workflow.
- Resource findings: F1 corrected; F3 unnecessary work removed.
- Simplicity/reuse: existing native batches, scalar completions, selection, and
  inventory remain authoritative.
- Test assertions: adequate for the stated bounded fixtures after F2 additions;
  execution evidence remains with the implementation owner.
- Qualification: representative continuous input, complete failure/retirement
  races, whole-process resources, peak/retained disk, and full R3 remain open.
- Confidence: high for the static traces; no measured performance conclusion.
