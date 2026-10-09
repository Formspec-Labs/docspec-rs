# Owner-grant retirement review

## Review status

Baseline: `2b92029b2d24087456b857493e2b29a4449899c7`, observed with a clean
worktree. This is an independent static architecture and code review. The
implementation owner runs all formatting, build and test commands. This reviewer
has not executed Cargo, changed Rust code, committed, or removed files.

**Final static verdict: APPROVE. No material findings remain open.** Grant
retirement should end an unused protection promise, preserve its creation
evidence, and permit a new exact generation without reviving the old one. It
must not imply that any physical object is reclaimable or deleted.

## Findings and required implementation checks

### GR1 — Preserve every live binding before ending a grant

**Severity: BLOCKER if omitted. Category: correctness.**

`retained_protections` deliberately propagates locally retained grant references
without reopening their owners (`src/engine/retention/owners.rs:398`). Admission
binds the operation, request, captured dependencies and Backfill capture in one
consumer commit (`src/engine/admission.rs:40`). Completed work carries its own
binding through `completion_records` (`src/engine.rs:90`), and selected results
carry the winning work's binding (`src/engine/history.rs:220`). Generation seals
have independent bindings (`src/engine/inputs.rs:113`).

Failure case: an operation ends but its processing phase remains independently
reusable. Retiring its grant from operation status alone permits that phase to
propagate a retired reference into later validation and results. The existing
phase test demonstrates that reused work keeps the original owner's scope
(`src/engine/retention/owners/phase_tests.rs:166`).

**Required shape:** scan all bindings in each grant's named consumer and ignore
only exact claim generations with validated retirement. Preserve live phases,
readbacks, results, admission dependencies and seals. Execute the proof and owner
decision under existing exclusive maintenance admission, which waits for admitted
workers (`src/engine/worker.rs:110`, `:135`). A plan captured before a new binding
appears must refuse at execution. Ordinary propagation needs no new owner scan
when this proof holds.

### GR2 — A retired generation cannot be returned by reacquisition

**Severity: BLOCKER if omitted. Category: correctness.**

The grant key hashes owner, consumer and exact objects, excluding revision
(`src/engine/retention/owners.rs:196`). `acquire_owner` currently returns an
existing grant at that key (`:640`). References include owner table UUID and
native revision (`:25`), and validation compares them (`:322`).

Failure case: retire generation N, reacquire the same object scope, and return N
again. A new consumer binding then names a retired promise. Replacing N without
enforcing revision identity has the opposite failure: an old reference silently
inherits the new promise.

**Required shape:** preserve exact retirement markers, create a new native
generation after retirement, and refuse stale references. Identical retries of
an active grant must retain their original generation. Retirement retries must
reconcile their old decision after reacquisition without retiring the new grant.
Keep original grant evidence usable for identity qualification: `visit_creation`
already treats grants as typed creation evidence (`:803`), separately from
whether the grant remains an active root.

### GR3 — State the treatment of historical observation bindings

**Severity: CONCERN. Category: intent-versus-shape.**

Current succession and input-progress writes do not create protection bindings
(`src/engine/history.rs:227`). Their direct metadata roots preserve ordering and
lag evidence, not endpoint payload availability
(`src/engine/retention/inventory.rs:1024`, `:1037`). The earlier design correction
explicitly supersedes permanent content retention through those observations
(`docs/reviews/history-metadata-design.md`, “Decision and correction”).

However, existing historical bindings are still validated and enforced
(`src/engine/retention/inventory.rs:196`;
`src/engine/retention/inventory/history_tests.rs:375`). They have no present
retirement scope. Treating every such record as an eternal content consumer
would leave historical observation grants unretirable even after all actual
content promises end.

**Recommendation: RESHAPE or explicitly DEFER this older-binding case.** Validate
the observational claim and make its lifetime rule explicit. If this increment
only retires completely unbound grants or grants whose bound claims have explicit
retirement, state that conservative limit rather than claiming all obsolete
grants can retire. Do not silently ignore a malformed or still-content-bearing
binding.

### GR4 — Keep retirement work local, admitted and reusable

**Severity: CONCERN. Category: performance and resource use.**

A grant names one consumer, and `validate_reference` rejects another consumer
(`src/engine/retention/owners.rs:322`). The existing broad retention inspection
enumerates the catalog and expands external metadata; it is an explicit
diagnostic, not needed for the narrow binding-lifetime proof
(`src/engine/retention/inventory.rs:75`). `SnapshotRows` already supplies bounded
current-record iteration and decoded-record reservations (`src/catalog.rs:41`).

**Recommendation: KEEP one consumer scan per retirement request**, grouping
grants by consumer if several are selected. Validate and reuse retirement
membership once rather than decoding the same receipt for each historical
binding. Keep scratch collections and returned state charged for their lifetime.
Do not move this work into ordinary reads or completed-phase reuse. Native
lifecycle history still has a configured byte ceiling; this is bounded admission,
not measured throughput or unbounded-history qualification.

An interrupted owner-first handoff may leave a grant whose consumer table does
not exist (`src/engine/retention/owners/tests.rs:767`). `Lifecycle::open` creates
missing tables (`src/catalog.rs:127`): read-only planning should check existence
and load an existing table, without creating the absent consumer.

## Artifact and lineage

The beneficiary is an operator controlling retained storage while preserving
promised results and recovery. This increment is operations infrastructure that
removes an obsolete independent root; physical cleanup remains a separate task.

| Prior artifact | Relationship | Evidence |
| --- | --- | --- |
| Shared plan | Requires one inventory, explicit retirement, fresh eligibility and no ordinary-read dependency walks | `docs/PLAN.md:44`, `:96` |
| R6 | Separates provenance, retained content and recovery; preserves independent completed phases | `docs/requirements/06-maintenance-retention.md:9`, `:23`, `:25`, `:27` |
| R6 planning/removal rule | New protections shrink old plans; expiration alone does not authorize deletion | `docs/requirements/06-maintenance-retention.md:29`, `:31` |
| Owner protection checkpoint | Owner-first acquisition survives failed handoffs; legacy unknowns do not establish removal eligibility | `docs/STATUS.md:260`, `:303` |
| History correction | Ordering observations do not promise endpoint payload availability | `docs/reviews/history-metadata-design.md`, “Which promise retains which objects” |
| Terminal operation retirement | Recovery retirement preserves independent phases/results and original native receipts | `src/engine/retention/recovery.rs:169`, `:220` |

| Component or seam | Owner | Inputs and outputs | Consumers |
| --- | --- | --- | --- |
| Owner acquisition | Engine retention | Exact object scope → owner grant and exact reference | Admission, seals |
| Consumer binding | Engine admission/completion | Grant references plus exact claim → same-commit binding | Reuse, selection, inventory |
| Retirement decision | Engine retention | Explicit plan plus fresh generations → durable receipt and markers | Reuse, inventory, retries |
| Worker exclusion | Engine worker | Admitted shared work / exclusive maintenance → stable mutation interval | All owned engine operations |
| Physical inventory | Engine retention | Independent live roots → exact object witnesses | Explicit inspection; future removal |

These seams are named in code and governed by R6. No new storage adapter or
separate index is required for this increment.

## Function and data-flow trace

| Function | Baseline location | Verified behavior |
| --- | --- | --- |
| `protect_objects` | `src/engine/retention/owners.rs:533` | Deduplicates exact objects, groups foreign owners, locks each owner and acquires before consumer acknowledgment. |
| `acquire_owner` | `src/engine/retention/owners.rs:618` | Checks availability and original production ownership, reuses identical grant or qualifies exact creation evidence before committing. |
| `PreparedAdmission::records` | `src/engine/admission.rs:40` | Stores captured dependencies and binds independent request roots in the consumer's guarded commit. |
| `completion_records` | `src/engine.rs:90` | Commits known protection beside the winning completion; unknown legacy input protection remains unknown. |
| `local_binding` | `src/engine/retention/owners.rs:421` | Checks exact claim digest/revision; permits a superseded binding only after its exact generation retired. |
| `retained_protections` | `src/engine/retention/owners.rs:403` | Returns charged local references without owner or historical graph reads. |
| `retire_retained_admitted` | `src/engine/retention/retirement.rs:338` | Reconciles identical decisions, checks captured native boundary and fresh exact records, appends receipt and markers in one commit. |
| `Inventory::check_bindings` | `src/engine/retention/inventory.rs:568` | Reads each owner at its recorded inspection boundary and validates exact references and union coverage. |
| `maintain_work` | `src/engine/worker.rs:135` | Owns exclusive admission until the actual worker finishes, including caller cancellation. |

The critical state is `(owner table UUID, token, native revision)`. It starts in
the owner commit, escapes into durable consumer bindings, and propagates into
completed work. Retirement may end that exact state only after all bindings that
can propagate it have ended. The underlying object identity and creation evidence
have a separate lifetime.

## Qualification and counterfactuals

Existing tests establish acquisition retry/reopen, lost native replies, phase
independence, immutable winning bindings, unavailable object refusal and legacy
unknown handling. They do not yet establish grant retirement. Required focused
checks for the new path are:

- An unused owner-first handoff grant retires; owner payload size and modification
  time remain unchanged; inventory loses only that independent witness.
- A live seal, operation dependency, phase, readback or result independently
  blocks retirement; retiring the parent alone is insufficient.
- A new binding after planning wins at execution, including canceled callers
  whose admitted worker still owns shared admission.
- Retire, reacquire identical scope, retry the original retirement, and reopen:
  the new grant remains active and old references fail exact-generation checks.
- Unknown native decisions reconcile; conflicting retirement identities and
  malformed markers/bindings refuse before a new retirement commit.
- Bounded metadata refusal occurs before committing; missing consumer tables and
  inaccessible payloads do not make metadata-only planning read or create them.
- Historical observational bindings receive the documented GR3 treatment.

The kill criterion is a surviving content consumer that can propagate a grant
after its retirement, or a new acquisition that reuses a retired generation.
The opposite design—permanent grants—preserves safety but defeats eventual
reclamation. Removing grants altogether breaks the owner-first durable handoff.
Existing retirement receipts and worker exclusion partially subsume this work;
reuse them rather than introducing another lifecycle framework.

User value remains unproven if all obsolete owner grants still block inventory
after their actual content promises have ended. No storage savings, physical
cleanup, provider qualification or full R6 completion can be inferred from this
metadata increment.

## Implementation review

The initial saved diff adds `OwnerGrants` to the shared plan/executor, a local
consumer check in `src/engine/retention/grants.rs`, retired-generation checks in
owner-reference validation, and CLI/HTTP calls. Static review confirms:

- **GR1 addressed:** `check_eligible` groups exact grants by their named consumer,
  validates each claim, ignores only validated retired generations, and blocks
  live token references (`grants.rs:46`). Shared execution invokes that check
  before appending any marker (`retirement.rs:456`), inside existing exclusive
  maintenance. The new path never invokes broad inventory or external metadata
  expansion.
- **GR2 addressed:** acquisition checks retirement before returning a previous
  grant; a retired scope receives the next owner revision. Existing exact scope
  skips another creation-history walk (`owners.rs:657`). `validate_saved` accepts
  a later generation only after the old retirement receipt (`grants.rs:20`), so
  cold old retries preserve the replacement. Reference validation still compares
  the exact generation and separately refuses retired current grants.
- **GR3 explicitly deferred:** this increment preserves old observational and
  legacy bindings as blockers. That is safe and conservative; their remaining
  lifecycle work must stay visible in status. New selection already creates no
  such observational bindings.
- **GR4 mostly addressed:** consumers are scanned once; the extracted
  `retired_records` helper validates receipt/marker membership once per consumer
  and keeps the inventory checks aligned. Missing consumer tables are checked
  with `table_exists` and are not created (`grants.rs:108`).

### GR5 — Admit the actual decoded consumer records

**Severity: WARNING. Category: resource accounting. Status: CLOSED.**

The initial consumer scanner reserves `5 * max_metadata_bytes` for its retirement
set and transient decoding (`src/engine/retention/grants.rs:118`), then decodes a
binding and invokes `validate_binding`. That helper holds a generic JSON claim
while the root walker also decodes its typed contents; an operation claim can
additionally decode its retained base result. The equivalent existing local
binding path admits the binding and claim with `SnapshotRows::reserve_decoded`
and explicitly adds `result/{base}` (`src/engine/retention/owners.rs:456`).

The fixed allowance neither follows the records actually decoded nor explains
why it covers the extra base. It also makes a tiny consumer reserve several
maximum-sized decoders. **Required correction:** use the existing per-record
reservation approach for binding, claim and any retained base, retaining it
through validation; reserve the bounded retirement set separately. This is an
admission-accounting finding, not a measured resident-memory claim.

**Closure:** `owners::reserve_claim` now shares exact claim/base admission with
the existing local-binding path (`src/engine/retention/owners.rs:323`). The grant
scanner admits each binding before decoding it and holds the claim reservation
through validation (`src/engine/retention/grants.rs:127`). Its retirement-set
state has a separate bounded permit (`:119`). This removes the redundant
maximum-sized transient allowance while preserving the established decoded-copy
accounting; no throughput or resident-memory reduction was measured.

### GR6 — Exercise active-reference refusals and independent binding kinds

**Severity: WARNING. Category: test coverage. Status: CLOSED.**

The initial focused tests cover competing phase claims, lost replies, concurrent
retirement retry, cold retry after reacquisition, missing consumer tables,
malformed reference revisions and oversized requests. Their assertions are
consistent with the traced behavior. The positive complete-lifetime fixture
retires real owner/consumer promises and requires the foreign object to lose its
inventory witness while its bytes remain.

The new `check_grant_active` refusal branches still need direct assertions:
an old reference must refuse immediately after retirement and again after a new
grant generation replaces it. A live generation seal and a historical
observational binding should independently block retirement. These cases protect
the precise differences from the already-covered phase path.

**Closure:** the grant retry test now checks active-reference success, exact
`grant_retired` refusal before reacquisition, and `invalid_owner_protection`
after the replacement (`src/engine/retention/grants/tests.rs:152`). A real sealed
family refuses grant retirement for every protected member (`:349`). The
historical observation test now directly requires `grant_in_use` without dropping
its prior protection assertions
(`src/engine/retention/inventory/history_tests.rs:446`). The positive lifetime
case reopens the workspace, retries the old retirement, confirms the unused
foreign object loses its witness, and reads exact replacement values (`:487`).

| Focused case | Assertions reviewed | Static assessment |
| --- | --- | --- |
| New phase claims after planning | Fresh execution refuses; each surviving independent phase continues blocking | Exercises fresh consumer proof rather than only owner-generation changes |
| Lost reply / concurrent / cold retry | One retirement decision, new grant revision, old reference refusal, unrelated grants unchanged | Exercises reconciliation and exact-generation identity before and after replacement |
| Missing consumer | Table absent before planning, after planning and after execution | Verifies the no-create behavior at the actual catalog boundary |
| Malformed binding / oversized request | Refusal and unchanged owner revision | Covers failed-proof and request-admission boundaries |
| Real generation seal / old observation binding | Exact `grant_in_use`; seal remains replayable | Covers independent and deliberately conservative claim lifetimes |
| Complete owner/consumer lifetime | Foreign object becomes unmatched; file remains; replacement values survive cold reopen | Establishes useful retirement without claiming physical reclamation |
| CLI / HTTP | Equal plans and decisions, structured conflicts, cold CLI replay, unchanged sizes/timestamps | Confirms both interfaces use the shared library service |

The reviewer read `src/engine/retention/grants/tests.rs`, the changed historical
binding test and `tests/grant_retirement_transport.rs`; no tests were executed by
the reviewer. The implementation owner's initial internal log records a
fixture-file failure and remains failure evidence. The owner's
`docs/reviews/grant-retirement-internal-corrected.log` now records a passing
complete retention-module run. The complete repository gate and transport run
remain the owner's execution responsibilities; static approval does not replace
those checks. The final test-only edits add an explained `dead_code` expectation
to the shared transport fixture and replace cloned single-element slices with
`std::slice::from_ref`; they do not change the reviewed behavior. The observed
`docs/reviews/grant-retirement-transport.log` records a passing transport run.
The initial strict-Clippy failure is retained in
`docs/reviews/grant-retirement-check-initial.log`. The next gate stopped at
`incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile`:
`unwrap_err()` received a successful `ReleasePin` instead of `disk_capacity`.
That failure is retained in
`docs/reviews/grant-retirement-check-disk-failure.log`.

**Disk-test correction: APPROVE.** The production guard is unchanged: it adds
declared scratch to rewrite/replacement requirements, then compares that total
against fresh available space (`src/engine/write.rs:449`). The old test captured
available space plus 1 MiB before awaiting engine opening and refresh; unrelated
cleanup could make that threshold satisfiable. The corrected test uses checked
total filesystem capacity plus one byte (`tests/related_refresh/incremental.rs:647`),
which file cleanup cannot satisfy. The exact `disk_capacity` assertion, reusable
admission check, unchanged selected result and successful smaller-profile retry
all remain (`:656`–`:678`). This removes a timing-dependent fixture condition
without weakening or changing the production guard.

The observed `docs/reviews/grant-retirement-disk-corrected.log` records a passing
targeted correction. The replacement full gate in
`docs/reviews/grant-retirement-check.log` now records the completed passing run,
including the corrected disk case, final worker tests and documentation tests.
The owner reports `./scripts/check` exited zero; the inspected script runs
formatting checks, strict Clippy and the complete test suite. The reviewer read
the completed evidence and ran no Cargo, formatting or test commands.

## Verdict

**VERDICT: APPROVE.** GR5 and GR6 are closed by the reviewed corrections.

- Intent and shape match this bounded R6 increment; live consumer promises remain
  authoritative and stale generations cannot inherit reacquired protection.
- Coverage of changed paths is adequate for static approval. The implementation
  owner still owns execution and the repository gate.
- Shared retirement membership and decoded-claim admission reduce duplicate
  implementation; reacquisition reuses qualified exact scope. No performance
  measurement or reclaimed-byte result is claimed.
- Ordinary data reads gain no consumer scan, inventory walk or owner-history
  validation. Planning and execution use only lifecycle metadata.
- Historical observation bindings, generation-seal lifetimes, descendant
  dependencies, physical removal and broader R6 qualification remain explicit
  limits. Unknown legacy protection is not removal authority.
- Confidence: HIGH for the statically reviewed local correctness and recovery
  paths; runtime and hosted-provider qualification remain separate.
