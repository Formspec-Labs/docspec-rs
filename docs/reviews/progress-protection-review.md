# Authority and input-progress protection review — superseded candidate

Status: **SUPERSEDED; do not use this review as approval to commit or enable the
candidate.** The independent architecture follow-up identified a material scope
problem: permanently acquiring complete producer closures solely to retain
observed input progress conflates ordering evidence with retained result content.
R6 explicitly gives those different lifetimes
(`docs/requirements/06-maintenance-retention.md:9`, `:21`), and the accepted-history
code promises ordering survives optional history/payload retirement
(`src/engine/history.rs:193`). The author is replacing that design. A fresh review
is required for the replacement.

The retained static analysis below found no additional blocking implementation
defect within the candidate's assumed permanent-grant boundary. That narrow
assumption did not adequately assess the model's effect on future retirement;
the earlier scoped approval is withdrawn. This review covers uncommitted changes
against `7e94506`, including the new
`src/engine/progress.rs` and its tests. It does not approve physical deletion,
grant retirement, automatic expiration, or completion of R6 or the full plan.

The review used static function and state tracing under the repository guidance,
the semi-formal code-review skill, and the Rust skill. The reviewer did not run
Cargo, format files, change Rust, or commit. Author-run evidence is identified
separately below. The workspace file map had no `docspec-rs` slice; repository
instructions and the current diff established the scope.

## 1. Patch summary

The inventory now checks that mutable `authority` names the exact retained
`result/<id>` and its selection revision before following its references
(`src/engine/retention/inventory.rs:852`). This avoids a second mutable protection
binding on authority. A later admission or invalidation can rewrite authority
without changing the original selection revision.

Selection now resolves every recorded progress endpoint—`prior`, `target`, and
`observed_head`—against an existing exact consumer scope or newly acquired owner
grants (`src/engine/progress.rs:34`). Missing scope causes the caller to release
its locks, acquire the exact producer version, and repeat mutable guards.
`input-progress` and its binding then commit with result, succession, and
authority (`src/engine/history.rs:261`). The actual result keeps its original
scope; observing another producer head does not add that head to the result's
material inputs.

The important callers are `select_prepared` (`src/engine.rs:1516`),
`select_refresh` (`src/engine/refresh.rs:222`), and the backfill composition loop
(`src/engine/backfill.rs:569`). The first delegates Refresh to the dedicated
related-input path. The backfill path keeps the immutable dependency graph and
completed readback outside the owner-acquisition retry loop.

## 2. Function trace

| Function or path | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `PreparedProgress::resolve` | `src/engine/progress.rs:34` | Progress proofs, candidate/prior scopes → ready scope or missing versions | Deduplicates manifest paths, refuses conflicting exact versions, reuses matching coverage, and merges each candidate/prior scope once. |
| `covers_input` | `src/engine/progress.rs:152` | Release and exact version → boolean | Requires equality with that release's retained coverage entry, including manifest identity. |
| `PreparedProgress::acquire` | `src/engine/progress.rs:96` | Missing versions → retained scopes | Bounds state, resolves each exact immutable result under its producer lock, releases that lock, then acquires owner grants. |
| `Engine::resolve_release` | `src/engine.rs:472` | Producer view and version → release | Checks dataset, result ID, dataset version, and verified status. Acquisition additionally compares the full `VersionRef`. |
| `protect_releases` / `protect_objects` | `src/engine/retention/owners.rs:513`, `:526` | Anchored releases → consumer grant references | Uses the shared exact-object collector, deduplicates references, and takes one owner lock at a time. |
| `acquire_owner` | `src/engine/retention/owners.rs:611` | Exact owner object set → immutable grant reference | Checks unavailability and ownership; reuses matching deterministic grants; otherwise checks creation evidence and commits before acknowledgment. Reconciles a lost response by the exact grant. |
| `select_prepared` | `src/engine.rs:1516` | Retained release → selected release | Rechecks existing result and operation authority; retries missing protection outside the consumer lock. |
| `select_refresh` | `src/engine/refresh.rs:222` | Prepared Refresh → selected release | Locks the dependency/family/output set, rechecks authority, ancestor eligibility, roles, coverage, and policy on every iteration. |
| Backfill selection loop | `src/engine/backfill.rs:725` | Current composition and retained readback → selected release | Rechecks promotion, abandoned-gap rules, ancestors, and actual current release. A sibling change exits the inner loop for recomposition; missing protection retains the same completed composition. |
| `lock_inputs` | `src/engine/inputs.rs:242` | Ordered dataset set → owned guards | Takes locks in `BTreeSet` order. No owner acquisition occurs while this guard vector is alive. |
| `guard_inputs` / `follows` | `src/engine/refresh.rs:295`, `src/engine/history.rs:44` | Captured/prior coverage and current producer state → exact succession proofs | Checks current eligibility and family seals; verifies endpoints on the accepted chain; records the freshly observed producer revision. Latest policy refuses a different head. |
| `select` / `select_with_progress` | `src/engine/history.rs:142`, `:161` | Guarded authority, release, progress → missing versions or committed selection | Validates the actual predecessor; keeps unknown succession provenance unknown; acquires no owners; atomically writes claims and their bindings after progress resolves. |
| `completion_records` / `ReferenceProtections::binding` | `src/engine.rs:92`, `src/engine/retention/owners.rs:146` | Claim and retained references → claim/binding records | Binds exact consumer, key, next native revision, and claim digest. |
| `retained_protections` / `ReferenceProtections::merge` | `src/engine/retention/owners.rs:403`, `:104` | Qualified local binding/scopes → retained references | Checks the local generation, preserves missing legacy bindings as unknown, deduplicates tokens, and retains shared-memory admission. |
| `Inventory::record` authority branch | `src/engine/retention/inventory.rs:852` | Authority and the same snapshot's rows → root references or refusal | Requires selected/release agreement, a positive nonfuture selection revision, correct dataset authority, verified release, exact result equality, and matching result revision. |
| `Lifecycle::commit` | `src/catalog.rs:257` | Captured view and complete record bundle → guarded native commit | Bounds the bundle and retained history, checks the expected table, and publishes all selection records together; it does not blindly retry stale metadata. |

## 3. Data flow and invariants

Initial hypotheses and outcomes:

- **H1: progress must protect all endpoints, including a head absent from the
  captured inputs. Confirmed.** `resolve` enumerates all three endpoint fields
  (`src/engine/progress.rs:44`) and requests missing versions. The binding is
  written only after every final endpoint resolves (`src/engine/history.rs:225`).
- **H2: acquiring owners while holding selection locks would invert lock order.
  Refuted for the patch.** Refresh exits its guarded block before acquisition
  (`src/engine/refresh.rs:271`); ordinary selection does likewise
  (`src/engine.rs:1558`); backfill explicitly drops guards and view
  (`src/engine/backfill.rs:756`). Acquisition's initial producer lock also ends
  before `protect_releases` (`src/engine/progress.rs:132`).
- **H3: owner commits could invalidate the first observed revision. Confirmed,
  and handled.** The next loop computes fresh proofs rather than comparing an
  old observation revision to the newer grant revision. Exact version identity
  controls reuse (`src/engine/progress.rs:67`); the fresh producer view controls
  `observed_revision` (`src/engine/history.rs:117`).
- **H4: an authority rewrite must not require reselection or manufacture a known
  result scope. Confirmed.** Selection revision may precede authority's current
  revision, but must equal the retained result's revision
  (`src/engine/retention/inventory.rs:862`). The inventory creates no authority
  binding. Existing binding validation still runs through the result record
  (`src/engine/retention/inventory.rs:179`).

`PreparedProgress.acquired` is request-local. Each exact missing version enters
only after owner acquisition completes; its memory permit and protection scope
live until the request finishes (`src/engine/progress.rs:138`). An interrupted
request drops that in-memory cache but leaves durable owner grants. Deterministic
grant identity lets a retry recover the grant without another owner commit
(`src/engine/retention/owners.rs:633`). No progress or result is acknowledged on
the missing-scope return (`src/engine/history.rs:233`).

An acquired head can become stale before selection. It stays protected by its
durable grant, but `resolve` includes only scopes selected by the final proofs
(`src/engine/progress.rs:58`). A changed consumer base or eligibility refuses on
the fresh guard; a compatible sibling backfill causes fresh composition. These
paths preserve the already completed phase/readback work. They satisfy the
checkpoint's R6-T04 protection purpose; they are not an implementation of the
fresh retirement/removal protocol in R6-T05
(`docs/requirements/06-maintenance-retention.md:46`).

Memory admission is present at the new ownership boundaries: proof/missing
storage (`src/engine/progress.rs:41`), cumulative acquired version identities and
their permits (`:106`), decoded producer result (`:121`), and retained scopes
(`src/engine/retention/owners.rs:52`). Scope merges and commit/history growth
have their own metadata bounds (`src/engine/retention/owners.rs:111`,
`src/catalog.rs:283`, `:315`). These are admission bounds, not measured peak-heap
or storage results. Capacity failure preserves completed work and grants; it
cannot authorize a partial selection.

The shared collector and owner-acquisition service avoid a second retention
implementation. Matching candidate/prior scopes avoid new owner commits. Keeping
the backfill dependency graph outside the inner retry avoids repeating its
immutable read/availability walk for an owner-only metadata change
(`src/engine/backfill.rs:716`). A newly encountered endpoint still requires a
metadata-closure walk and potentially an owner creation-evidence scan. This
review makes no throughput or resource-reduction claim.

## 4. Test behavior and edge cases

The following assertions were read and traced, separately from execution:

| Test | Location | Expected behavior and evidence |
| --- | --- | --- |
| `latest_selection_reuses_the_exact_target_scope_without_owner_commits` | `src/engine/progress/tests.rs:171` | Uses candidate coverage for all equal endpoints; selection bypasses the acquisition stop, leaves owner revision/grants unchanged, and commits matching result/progress scopes and fresh observed revision. |
| `captured_selection_protects_the_final_observed_head_and_reconciles_interrupted_grants` | `src/engine/progress/tests.rs:239` | A newer computed head forces acquisition; repeated interrupted calls reuse the grant without a result/progress row; another head change produces a final proof and grant for that final head while keeping the candidate result unchanged. |
| `related_backfill_reuses_its_readback_while_protecting_a_newly_observed_producer_head` | `src/engine/progress/tests.rs:393` | Related Backfill acquires a newer producer-head scope, resumes with identical retained readback and group-file metadata, commits exact progress with the result, and leaves owner grants unchanged on retry. |
| `latest_refresh_reuses_known_prior_and_target_scopes_when_coverage_advances` | `src/engine/progress/tests.rs:452` | Prior and target differ; progress includes both retained scopes, bypasses acquisition, records advancement to the current target, and preserves owner revision and grants. |
| `unknown_candidate_scope_can_qualify_progress_without_upgrading_the_result` | `src/engine/progress/tests.rs:493` | Replaces retired validation with an unbound generation; independently acquires exact progress scope while the readback/result remain unbound. |
| `authority_reuses_its_result_identity_without_lending_protection_to_unknown_results` | `src/engine/retention/inventory/collector_tests.rs:115` | Known and unbound results both resolve exact authority roots; no authority binding appears and no absent payload is read. The result's known/unknown binding state remains unchanged. |
| `authority_alias_refuses_missing_mismatched_and_incomplete_selection_metadata` | `src/engine/retention/inventory/collector_tests.rs:163` | Malformed authority/result identities, result revisions, missing results, and incomplete selections refuse with `retention_inventory`. |
| `empty_authority_has_no_selected_result_or_protection_claim` | `src/engine/retention/inventory/collector_tests.rs:247` | A consistent empty authority adds no object roots. |
| `completed_and_interrupted_roots_protect_without_opening_payloads` | `tests/retention_inspection.rs:37` | Admission and invalidation advance authority metadata while its selected result remains unchanged; inventory still finds exact blockers without reading removed payload files. |

Relevant existing regression paths also exercise captured lag, Latest refusal,
nonregressing coverage (`tests/related_refresh.rs:536`), stale output base and
producer eligibility (`:653`), and related Backfill cutover (`:700`). Their
assertions do not by themselves establish process-loss behavior at the newly
introduced owner-acquisition boundary.

Read author-run evidence: [compilation](progress-protection-compile.log),
[progress tests](progress-protection-focused.log),
[authority tests](authority-protection-focused.log), and
[retention inspection](authority-protection-inspection.log) report success.
Those logs cover the original targeted fixtures; the added related-Backfill and
advancing-coverage fixtures were inspected statically after they were saved. The
reviewer did not reproduce these executions. Full repository-gate status belongs
to the author's final verification record, not this static review.

## 5. Findings and qualification boundaries

**BLOCKER — retain ordering evidence without permanent full-payload grants.**
The architecture follow-up identified that `PreparedProgress::acquire` sends a
newly observed head through `protect_releases` (`src/engine/progress.rs:133`).
That acquires the complete historical object closure
(`src/engine/retention/owners.rs:521`), although the retained claim is a succession
proof used to report ordering/lag (`src/engine/history.rs:117`). Permanent grants
would keep those payloads after independent result-retention promises end. This
violates the distinct lifetimes in R6 (`docs/requirements/06-maintenance-retention.md:9`,
`:21`). Replace this candidate with an ordering-evidence representation that does
not silently extend content availability. This material architecture finding
supersedes the earlier narrow scoped approval; it was not found by the original
static pass.

No additional blocking implementation defect was established by the retained
static trace.

**OBSERVATION — qualification scope.** The new progress interruption fixture
stops and resumes within one engine process (`src/engine/progress/tests.rs:263`).
This tests a durable grant left before selection and a fresh subsequent decision;
it does not directly test killed-process/reopen at that exact boundary or inject
producer invalidation during an in-flight acquisition. The fresh-guard trace
supports the intended behavior. Retain those distinctions when reporting
qualification; broader crash/race qualification remains required by
`docs/requirements/07-operations-qualification.md:83`.

**OBSERVATION — metadata and retention cost.** A missing endpoint calls the
existing full release-closure collector (`src/engine/progress.rs:133`;
`src/engine/retention/inventory.rs:27`), and a new owner scope can scan retained
creation evidence (`src/engine/retention/owners.rs:649`). Superseded grants remain
durable even when omitted from the final progress binding. This is conservative
under the present permanent-grant, no-deletion boundary, but it has a real
history/storage cost. No new fixture measures that cost against head churn or
retained-history depth. Separate ordering-evidence lifetime from result payload
lifetime before enabling retirement, and qualify growth under Q7
(`docs/requirements/07-operations-qualification.md:133`). This checkpoint does
not establish that future ordering-only retention must keep every full payload.

## 6. Conclusion

VERDICT: REQUEST CHANGES. This candidate is superseded by the architecture
decision described at the top. The static trace remains evidence about the
reviewed candidate; it is not approval of the replacement.

Justification:

- The candidate checks authority against its exact immutable result and acquires
  missing progress scope before one guarded selection acknowledgment.
- Coverage of the reviewed paths: ADEQUATE for the candidate's scoped behavior,
  with the crash/race and scaling limits stated above.
- Confidence: HIGH for the static identity, locking, and atomic-record traces;
  MEDIUM for unexecuted concurrency and resource behavior.

No runtime-pass claim is made for the newly added related-Backfill and
advancing-coverage tests. They were inspected statically only.

This review does not establish physical-maintenance readiness, independent
retention windows, restoration support, hosted behavior, or the complete
R1–R7/performance acceptance plan (`docs/PLAN.md:96`, `:121`, `:134`).
