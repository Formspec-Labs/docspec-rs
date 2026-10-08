# Acknowledged-handoff process recovery review

Scope: the uncommitted qualification diff after `11dd142`, inspected statically
on 2026-10-08 using the semi-formal code-review and Microsoft Rust guidance. The
reviewer changed only this report and ran no tests. Initial line references below
identify the inspected pre-fix working tree; closure evidence supersedes them.

## Findings

F1 and F2 are closed by the inspected follow-up changes. O1 remains the explicit
qualification boundary. The initial evidence below is retained for reviewability;
the final disposition appears after closure evidence.

### F1 — Preserved completion records alone do not prove skipped computation

- **Severity:** WARNING; test coverage and qualification accuracy.
- **Location:** `tests/backfill.rs:353-363`, `tests/updates.rs:822-829`.
- **Evidence chain:** The assertions compare lifecycle records before and after
  resume and label that equality as reuse. However, `Engine::complete` returns the
  first existing completion even if a caller computed another candidate
  (`src/engine.rs:885-888`). Backfill acknowledgments also preserve existing
  completion records. A regression that computes and discards fresh work could
  leave these assertions green. Deleting the external original input does not
  prevent recomputation from already admitted retained inputs.
- **Required qualification adjustment:** Observe entry to the expensive work
  separately from its acknowledgment. Small `test-util` work-start markers or
  exact attempt/output observations are sufficient. Assert that phases completed
  before the kill do not execute again, while checking the existing retained
  identities and reconstructed values. Without that observation, describe the
  evidence as preserved completions and correct resume rather than measured
  absence of repeated work.

### F2 — The resume child can hang the gate indefinitely

- **Severity:** WARNING; test harness reliability.
- **Location:** `tests/support/mod.rs:68-76`.
- **Evidence chain:** The paused child has a marker deadline and is killed and
  reaped, but `resume` uses blocking `Command::output()` without a deadline. A
  deadlock during recovery prevents the test from reaching any assertion or
  cleanup. The marker deadline does not bound this second child.
- **Required correction:** Give resume a bounded wait and kill/reap it on timeout.
  Redirecting stdout/stderr to fixture-owned files avoids pipe-drain deadlock
  while retaining diagnostics. This is a correctness-fixture deadline, not a
  performance acceptance target.

### O1 — The exercised failure boundary is deliberately narrow

- **Severity:** OBSERVATION; qualification scope.
- **Location:** `src/main.rs:168-189`, `tests/support/mod.rs:46-63`.
- **Evidence chain:** The CLI emits its marker only after the engine returns
  `injected_failure` at a named completed phase, then waits forever while the
  engine/workspace remains alive. On Unix the harness asserts signal 9 after
  killing that process. This is real process loss at an acknowledged handoff.
  Processing-local futures have already returned at that point.
- **Scope:** This does not exercise death during a native commit, incomplete
  output writes, an unacknowledged file bundle, machine power loss, or host loss.
  A `files` fixture must state whether all files were acknowledged before the
  marker; it cannot silently imply arbitrary mid-file interruption. Keep these
  limits explicit in the status document.

## Patch summary

The hidden feature-gated CLI command gains build/update/backfill dispatch. Shared
test helpers terminate a live CLI process at a durable marker and resume through
a fresh process. The existing build recovery fixture reuses this helper; new
update and backfill fixtures check retained state, exact output reconstruction,
historical readability, and stable operation retry.

## Function trace

| Function or path | Location | Inputs and output | Inspected behavior |
| --- | --- | --- | --- |
| `PausedOperation`, `PauseAfter` dispatch | `src/main.rs:39-48`, `:104-109`, `:167-190` | Request kind/path and phase → durable marker or error | Parses the appropriate typed request; marker requires the engine's injected stop; successful completion without a stop is an error. Default remains build. |
| `support::kill_after` | `tests/support/mod.rs:12-66` | Fixture root, serialized request, kind, phase → killed/reaped child | Uses the built CLI, writes the exact request, waits for the exact marker, kills, waits and asserts Unix SIGKILL. Reader preserves recent diagnostics. |
| `support::resume` | `tests/support/mod.rs:69-83` | Same fixture/request and public command → parsed release | Starts a fresh CLI process and checks exit success; F2 identifies the missing timeout. |
| `run_until`, `update_until`, `backfill_until` | `src/engine.rs:438`, `:457`; `src/engine/backfill.rs:237` | Typed request plus phase → original run path with stop | Feature-gated wrappers call the actual engine paths, not fake recovery implementations. |
| `stop_after` and durable call sites | `src/engine.rs:616`, `:657`, `:671`, `:741`, `:784`, `:1121`; `src/engine/backfill.rs:462-512`, `:604`, `:630` | Requested/actual phase → injected failure | Stops follow acknowledgment/selection; error reconciliation preserves injected stops for the harness. |
| `phase`, `complete` | `src/engine.rs:799`, `:869` | Captured operation and phase → retained phase | Lookup validates authority and retained availability; first acknowledgment wins. That last property motivates F1's separate work-entry assertion. |
| `killed_cli_worker_resumes_every_completed_handoff` | `tests/lifecycle.rs:661` | Build fixture and named phases → assertions | Deletes external input and resumes the exact operation; retains existing source row-count assertion. |
| `killed_update_workers_resume_retained_handoffs_and_exact_changes` | `tests/updates.rs:757` | Source/replacement/deletion fixture → verified historical and new releases | Deletes all external files, inspects selected state before resume, checks full values plus actual added/removed rows and exact retry. |
| `killed_backfill_workers_resume_retained_files_validation_and_readback` | `tests/backfill.rs:285` | Native source/scalar group → verified combined result | Checks the expected retained phase records, pre-selection latest, duplicate/null/Unicode value reconstruction, unchanged source pin and stable retry. |

## Data flow and invariants

| Invariant | State creation, use and escape | Assessment |
| --- | --- | --- |
| Kill targets a live acknowledged worker | Parent writes one request, child returns the matching injected stop and emits marker, parent kills/reaps the child | Unix signal assertion prevents a normal exit from being presented as SIGKILL evidence. |
| Resume uses stable identity | `request.json` survives under the fixture root and is passed unchanged to the public command | No alternate operation ID or request material is substituted. |
| Workspace has one process owner | Fixture drops its engine before spawning each CLI; child is reaped before the next open | No live parent engine bypasses the workspace lock during kill/resume. |
| Input admission survives loss of external originals | Fixtures delete original files after the kill; resume reads retained inputs | Correctly qualifies retained input availability at completed admission or later boundaries. |
| Unselected candidates cannot become latest before promotion | Fixtures inspect the post-kill authority and compare with the old release except at selection | A completed intermediate phase is not confused with a selected dataset. |
| Result correctness is independently checked | Update reconstructs source and change rows; backfill derives scalar values in Rust and checks combined rows | Preserves duplicates and nulls, beyond row-count agreement. |
| Earlier completion identity remains stable | Fixtures compare exact retained records and completed readback pins | Establishes persistence; F1 requires an additional observation to prove skipped execution. |
| Every child is bounded and cleaned up | Pause child has deadline/kill/wait; resume initially has none | F2 addresses the incomplete harness lifetime rule. |

## Test behavior and edge cases

- The build fixture's helper extraction preserves its original kind, request,
  handoffs, external-input removal and resume command. The new helper strengthens
  evidence by requiring kill success and Unix SIGKILL rather than merely ignoring
  the kill result (`tests/lifecycle.rs:661-676`, `tests/support/mod.rs:54-63`).
- Update recovery checks exact complete-key replacement/deletion semantics after
  each named stop, including duplicate and null keys inherited from its fixture
  (`tests/updates.rs:71-109`, `:757-831`). Its independent reconstruction and
  historical verification should pass for the previously reviewed engine path.
- Backfill recovery checks retained file/prepared/validation/readback records,
  exact combined values and unchanged source pin (`tests/backfill.rs:285-368`).
  The stop at selection also verifies the already selected operation before
  retry. These assertions qualify acknowledged recovery but initially did not
  observe repeated expensive work.
- Missing marker, an invalid phase, an early child error, or normal child exit
  causes the kill helper to fail rather than report qualified recovery. A blocked
  resume is the concrete uncovered error path in F2.
- Non-Unix platforms check unsuccessful termination but cannot use the Unix
  signal assertion. No cross-platform execution claim follows from static review.

## Initial conclusion

**VERDICT: REQUEST CHANGES** to the qualification assertions and bounded resume
harness. No production lifecycle correctness defect was found in this narrow
diff. **Coverage: INSUFFICIENT** for a no-repeated-work claim until F1 is addressed;
**confidence: HIGH** for the concrete gaps and the acknowledged-handoff scope.
Execution remains the implementing agent's responsibility.

## Closure evidence

- **F1 closed.** `test-util` observations now mark actual entry to admission,
  source processing, validation and readback (`src/engine.rs:573`, `:624`, `:668`,
  `:717`). Backfill marks missing group assembly, group validation, composition
  readback and individual file computation (`src/engine/backfill.rs:377`, `:506`,
  `:585`, `:717`). The helper exists only under the feature gate
  (`src/engine.rs:1139-1144`), and every call is also gated. The markers add no
  durable state and do not change ordinary builds.
- Resume reads markers from the new child's stderr and returns them with its
  release (`tests/support/mod.rs:94-104`). Each fixture compares the entire
  observed sequence with the exact remaining work
  (`tests/lifecycle.rs:674-681`, `tests/updates.rs:797-804`,
  `tests/backfill.rs:342-353`). Nonempty expected sequences provide a positive
  control for the observation mechanism; completed readback/selection expects no
  work markers. Existing exact completion and output checks remain in place.
- **F2 closed.** Resume redirects stdout/stderr to fixture-owned files and uses
  `try_wait` with a bounded deadline; timeout kills and reaps the child
  (`tests/support/mod.rs:69-93`). This avoids an unread stdout pipe blocking a
  large serialized release. The timeout branch was inspected statically; the
  passing recovery fixtures exercise normal successful completion, not a forced
  timeout. No production timeout or scheduling framework was introduced.
- **O1 preserved.** Backfill explicitly checks zero pending files before resume
  (`tests/backfill.rs:297-300`). That fixture covers recovery after all file work
  is acknowledged and before group assembly, rather than claiming arbitrary
  partial-file recovery. `README.md:252-256` and `docs/STATUS.md:214-235` now name
  acknowledged worker-loss boundaries and exclude power loss, arbitrary writes
  and catalog-commit failures, remote recovery and broader qualification.

## Final conclusion

**VERDICT: APPROVE** for process termination and resume at the named acknowledged
handoffs.

- The harness verifies a live child's Unix SIGKILL, releases its workspace before
  reopening, resumes the exact operation in a new process, and checks remaining
  work independently from persisted completion identity. The added value and
  change-row assertions cover the new update/backfill dispatch paths.
- **Coverage of changed paths: ADEQUATE** for this narrow qualification. The
  implementing agent completed `./scripts/check` with exit 0; the inspected
  [gate output](process-recovery-check.log) records successful build, update and
  backfill process-kill fixtures with the exact remaining-work assertions, plus
  the existing suites and Clippy. The appended `cargo check --locked
  --no-default-features` also completed successfully, checking the build without
  fault-observation features. This reviewer inspected the saved evidence and ran
  no tests.
- **Confidence: HIGH** for the inspected local paths. This is evidence of recovery
  from acknowledged worker loss, not broader storage or disaster-recovery
  qualification. No remaining correctness or false-qualification finding was
  identified after the corrections.
