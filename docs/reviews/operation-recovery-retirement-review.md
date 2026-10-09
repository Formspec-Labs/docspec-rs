# Terminal-operation recovery retirement review

**Disposition: APPROVE the corrected implementation; focused checks and the
complete repository gate passed.** The terminal-state,
Backfill, and memory-accounting findings below are corrected in the inspected
source. This is a bounded review of logical recovery retirement against
`596be07`, not approval for physical removal or completion of R6.

The independent reviewer read the repository instructions, applicable Rust and
semi-formal review guidance, R6, the implementation plan, and
`retention-next-step-review.md`. The reviewer inspected production changes,
callers, tests, and author logs. No Rust edits, Cargo commands, formatting,
cleanup, or commits were performed by the reviewer.

## Findings and corrections

### F1 — corrected: terminal completion must match the admitted request

The first collector used the history receipt check to qualify completion. That
check deliberately reads ordering identity, so an otherwise consistent result
and succession pair with a different `dataset.request` could qualify the wrong
request as completed. Recovery retirement needs the additional request match.

`recovery::terminal` now reads the result's request through a narrow decoder and
compares it with the original admitted request before reusing
`history::check_link_receipt` (`src/engine/retention/recovery.rs:48`). It also
requires the original admission/request revision, fingerprint, dataset, supported
format, later completion revision, and absence of abandonment. The alternative
terminal path checks the abandonment's original revision, request fingerprint,
identity, nonempty reason, and native boundary. The adversarial request test
preserves the ordering identity and confirms why the new check is necessary
(`src/engine/retention/recovery/tests.rs:222`). Static finding closed; runtime
qualification passed in the corrected internal log.

### F2 — corrected: saved retirement validation must admit original record copies

Exact retirement retry and uncertain-response reconciliation initially reached
`validate_saved` with only the submitted plan's memory reservation. A small plan
can refer to large original requests and result records. Inventory's broad
allowance did not protect those separate retry paths.

`reserve_identity` now charges the original operation, request, result,
succession, and abandonment before decoding. `validate_saved` owns that permit
and charges each named attempt before reading it
(`src/engine/retention/recovery.rs:106`, `:221`). The test exhausts the shared
pool, expects refusal before decoding, releases the held permits, and then
validates the same receipt (`src/engine/retention/recovery/tests.rs:266`). Static
finding closed; this is admission accounting, not measured peak memory.

### F3 — corrected: early retirement membership checks must charge their receipt

The new planning path and same-generation claim check initially reached
`record_retired` without admitting the potentially larger retirement receipt.
That receipt can contain many attempt generations unrelated to the incoming
request or binding size.

The shared helper now accepts the budget, returns immediately if the marker is
absent, and charges both present marker and receipt before decoding
(`src/engine/retention/retirement.rs:72`). All callers use this correction,
including earlier phase/owner callers. The exhaustion test now also expects
retired membership lookup to refuse while a live record with no marker still
requires no decoder reservation (`src/engine/retention/recovery/tests.rs:288`).
Static finding closed. The complete repository gate includes this follow-up;
the earlier internal pass predates it.

### Backfill and documentation corrections

The first completed Backfill planning branch allowed a missing original
admission and indexed the source group directly. It now requires matching
admission fingerprint, exact stored request, dataset, operation/version,
verified status, and a source group; invalid receipts refuse instead of panicking
(`src/engine/backfill.rs:205`). A focused unit fixture covers absent/conflicting
admission and malformed result identity without any source files
(`src/engine/backfill.rs:1079`).

The availability test initially attempted an unbased Build after selecting a
computed group, which the existing `dependent_groups` guard correctly rejects.
It now uses a permitted based replacement
(`tests/completed_backfill_plan.rs:96`). README wording now distinguishes fresh
retirement planning (`recovery_retired`) from completed-result replay and
abandoned-operation refusal. These corrections are retained here rather than
presenting this as a no-finding review.

## Patch and function trace

| Path | Decision and evidence |
| --- | --- |
| Shared representation | `OperationRecovery { operation }` extends the closed `RetentionPlan` scope; phase and availability APIs remain thin (`src/model.rs:507`, `src/engine/retention/retirement.rs:30`). |
| Planning | `plan_operation_retirement` holds admitted worker ownership, locks the dataset before opening it, qualifies terminal identity, and captures exact original recovery rows and owned attempts (`recovery.rs:277`). |
| Attempt ownership | `check_attempt` checks exact key/id, consumer, original-operation prefix, a single valid purpose, canonical workspace directory, active reservation, and native revision bounds (`recovery.rs:140`). It never infers ownership from age. |
| Execution | `retire_retained` takes exclusive worker ownership before the dataset lock, recollects the full current set, requires equality with the plan, and atomically appends markers and one receipt. Exact retry and lost response reuse the durable decision (`retirement.rs:329`, `:386`, `:437`). |
| Saved receipt | `validate_markers` checks scope, native revisions, membership, and same-commit markers. `validate_saved` additionally qualifies original terminal identity and each named owned attempt (`retirement.rs:133`, `recovery.rs:221`). |
| Inventory | The existing validated retirement set filters the exact independent root and its binding. Adding operation/reservation families uses the same mechanism (`inventory.rs:147`, `:196`, `:248`). |
| Propagation | `local_binding` refuses a same-generation retired bound claim; older retired generations remain distinct from a newly completed claim. Missing legacy bindings remain unknown (`owners.rs:421`). |
| Creator evidence | Original reservation rows stay unchanged. `check_object_owner` can still validate files created in a closed attempt for a live phase/result (`owners.rs:679`). |
| Completed Backfill plan | Identity checks and result return precede base/native input reads. Completed plans expose retained row metadata and absent file diagnostics, rather than scanning recovery inputs (`backfill.rs:195`). |
| Transports | CLI and HTTP call the same plan/execute methods with the shared types; no independent retirement logic (`src/main.rs:84`, `src/http.rs:151`). |

## Invariants and architecture assessment

**Independent lifetimes are preserved.** Recovery retirement never cascades to
phase results, result/readback availability, owner grants, succession, or input
progress. Raw operation/request/reservation rows remain identity and creator
evidence. This implements the separation required by R6:9, :23, and :27.

**New protections cannot silently expand a saved decision.** Execution waits for
admitted workers, takes a fresh view, and compares the complete recollected
generation set with the plan. A new reservation or changed original generation
refuses. All markers append together with the decision; uncertain commit results
reconcile only an identical receipt.

**Normal replay needs no new recovery scan.** Only a completed or explicitly
abandoned operation can retire. Build/Update/Schema/Refresh still share
`prepare_and_select`, which checks request identity and returns the available
completed result before preparation (`src/engine.rs:993`). Backfill follows the
same ordering (`src/engine/backfill.rs:420`). Abandoned operations still refuse.
The existing availability guard remains before preparation and error
reconciliation. No recovery flag is added to every normal read or reconciliation
site. The previously removed unbased Build predecessor acquisition remains
absent: admission takes `authority.release` only for an explicitly based request
(`src/engine.rs:1052`). Ordering-only history does not regain a content grant.

**Lock order and cancellation remain consistent.** Plan uses admitted read
ownership; execution uses exclusive maintenance ownership. Both acquire the
dataset lock afterward. The owned worker retains this custody after caller
disconnect, and forced worker loss retains host quarantine
(`src/engine/worker.rs:110`, `:135`, `:160`). No foreign-owner acquisition occurs
under the new retirement dataset lock. Physical readers remain outside this
barrier, so these facts do not qualify destructive cleanup.

**Cost and reuse are bounded but unmeasured.** Planning/execution scan the current
dataset's attempt rows once, with a bounded sorted output. Saved receipt
validation visits only captured records, avoiding a complete attempt scan for
each historical retirement. Shared plan construction, commit, marker validation,
inventory, and reconciliation avoid a second retirement implementation. Narrow
terminal decoders avoid copying full file inventories. R6 metadata/history
scaling and peak memory remain unqualified. The corrected membership helper
avoids decoding or additional reservation for the ordinary live-record case.

## Tests and execution evidence

The reviewer inspected these assertions and the author-owned execution receipts:

- `tests/operation_retirement.rs:93` retires A's availability, phases, then
  recovery; the exact unused A object changes from blocked to unblocked after
  cold reopen, selected B stays protected/readable, and A's bytes and original
  lifecycle records remain unchanged.
- `tests/operation_retirement.rs:207` retires a completed parent, quarantines its
  external input, retries its result and plan, restores the input, then reuses its
  retained processing in another operation. Shared output and creator evidence
  remain readable after reopen.
- `tests/operation_retirement.rs:271` refuses active work, requires explicit
  abandonment, preserves phases, and permits another operation to reuse them.
- Internal recovery tests cover stale/incomplete sets, canonical attempt
  ownership, altered completed request, lost native reply, concurrent/cold retry,
  immutable source generations, and refusal to propagate a retired bound claim.
- `tests/completed_backfill_plan.rs` exercises completed retries with unavailable
  recovery inputs, conflicting requests, and independent own/base availability.
  Existing Backfill assertions now distinguish absent completed diagnostics from
  exact pending diagnostics.
- `tests/operation_retirement_transport.rs` compares CLI and HTTP plans and
  durable decisions, checks active/fresh-retirement refusals, and reads selected
  values after reopen. Existing worker tests cover canceled admitted and
  maintenance callers; no new operation-specific cancellation execution is
  claimed here.

The first author internal build log,
`docs/reviews/operation-recovery-retirement-internal.log`, stopped at E0277:
`unwrap_err` required `ReferenceProtections: Debug`. The author corrected the
assertion with `.err().unwrap()` without changing the production type. Preserve
that failed log as evidence.

`docs/reviews/operation-recovery-retirement-internal-corrected.log` records
passing recovery tests for request mismatch, saved-record budget refusal,
lost-response/concurrent/cold retry, and stale/canonical attempt checks.
`docs/reviews/operation-recovery-retirement-integration.log` records passing
completed Backfill, terminal operation, and transport tests. In particular, the
unused-A-object witness, selected-B readback, unavailable-input completed replay,
phase reuse, and cold CLI retry executed successfully. These are author runs
observed by the reviewer, not reviewer-executed tests.

The complete repository gate subsequently finished successfully. The reviewer
inspected `scripts/check` and the completed
`docs/reviews/operation-recovery-retirement-check.log`: formatting checks, strict
Clippy, and all library, integration, and documentation test stages passed. The
log includes the latest shared membership reservation and exact Backfill
stored-request checks. This remains author-executed verification inspected by
the reviewer; the reviewer performed no reexecution.

## Scope and remaining qualification

The positive witness concerns one unused local object after every named
independent promise retires. It is not evidence that ordinary update ancestry,
foreign grants, coupled generation seals, or legacy unknown dependencies can be
discarded. Grants remain independent roots; unknown live dependency scopes remain
conservative. No objects are deleted, no reclaimed-byte or performance result is
claimed, and restore, active-reader deletion safety, compaction, automatic expiry,
and the full lifecycle acceptance target remain outside this checkpoint.
