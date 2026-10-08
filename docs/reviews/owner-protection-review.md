# Owner protection and generation-seal review

Current verdict: APPROVE for the scoped acquisition/seal checkpoint after the
re-review below. The initial findings are retained as review evidence and are now
closed. Execution of the final repository gate remains the implementing agent's
responsibility.

Static review of the uncommitted owner-acquisition, exact-reference collector, and
generation-seal changes on 2026-10-08. No Cargo command, formatting command, or
test was run by this reviewer. The implementing agent owns execution evidence.
This checkpoint does not complete foreign-reference integration or enable
physical removal, snapshot expiration, grant retirement, or restore.

## Initial findings

### F1 — A tiny generation seal can exhaust admission before collecting references

- Severity: WARNING (P2)
- Category: correctness / resource use
- Location: `src/engine/inputs.rs:57`, `src/engine/retention/inventory.rs:49`
- Description: The new nested fixed reservations impose an avoidable memory
  floor independent of actual seal size. A workspace with `memory_bytes = 512
  << 20` and `max_metadata_bytes = 16 << 20` can produce the small releases used
  by the owner fixtures, but cannot seal even one such coupled release.
- Evidence chain: `seal_generation_admitted` retains one metadata allowance
  (`inputs.rs:28-31`) and eight more while gathering releases (`inputs.rs:57-59`).
  It calls `protect_releases` before dropping either (`inputs.rs:80-82`), which
  calls `reference_objects` (`owners.rs:237`). The collector reserves another
  eight allowances (`inventory.rs:49`). These alone require 272 MiB. The
  application pool for that configuration is 256 MiB (`budget.rs:83-94`), and
  `Budget::reserve` refuses immediately (`budget.rs:142-153`). Request, worker,
  and lifecycle-cache reservations add further demand. This is a static lower
  bound, not an RSS measurement. The owner tests use this exact configuration
  (`owners/tests.rs:15-23`), but do not call `seal_generation`. Existing generation
  tests inherit the 24 GiB default (`tests/support/scalar.rs:20-27`,
  `budget.rs:31-41`) and cannot expose it.
- Suggestion: Charge the retained release collection from bounded observed
  sizes, or pass one appropriately scoped admission across collection and
  acquisition. Preserve admission while owned values remain alive. Add a small
  end-to-end seal/retry test using the owner fixture's resource limits.

### F2 — Retention inspection repeatedly replays the same owner histories

- Severity: WARNING (P2)
- Category: performance / maintainability
- Location: `src/engine/retention/inventory.rs:171-175`,
  `src/engine/retention/owners.rs:209-225`
- Description: Every consumer binding opens and loads each referenced owner
  again. With multiple owners, the existing single-entry lifecycle cache causes
  repeated full owner-history scans within one inspection request.
- Evidence chain: For every `reference-binding/` row, inspection calls
  `check_protection_references` (`inventory.rs:171-175`). That opens an owner and
  calls `lifecycle.rows` for each token (`owners.rs:209-214`). `Lifecycle` caches
  one metadata location globally (`catalog.rs:28`, `catalog.rs:169-180`); a miss
  reads all lifecycle batches and replaces that entry (`catalog.rs:193-226`).
  Concrete case: a family has many retained generation bindings, each referring
  to producer A and producer B. Each binding loads A then B; the next binding
  loads A then B again. These are unchanged immutable boundaries during a quiet
  inspection. Work is proportional to bindings times referenced owner-history
  bytes, in addition to inspection's own catalog walk. If the owner history grows
  with the retained bindings, this introduces multiplicative growth. No elapsed
  time or throughput claim is made.
- Suggestion: Resolve binding references once against bounded, request-local
  owner observations, or reconcile them during the inventory's existing owner
  traversal. Preserve exact owner UUID, token, revision, consumer, and
  availability checks. Bound retained validation state and test repeated
  bindings to multiple owners with read-count evidence. Do not solve this with
  an unbounded cross-request cache.

### F3 — The consumer-binding recovery boundary lacks direct adversarial coverage

- Severity: WARNING
- Category: test-coverage
- Location: `src/engine/inputs.rs:106-112`,
  `src/engine/retention/owners.rs:133-228`
- Description: The new owner tests exercise grants and owner-side stale commits,
  but do not assert the final claim/binding pair or any binding-validation failure
  path. This leaves the new consumer-side protection promise less qualified than
  the owner-side primitive.
- Evidence chain: The new owner tests call `protect_releases` or `acquire_owner`
  (`owners/tests.rs:68-505`). Existing generation tests check exact reseals,
  membership conflicts, restart, and invalidation (`tests/input_generations.rs:79-169`,
  `tests/input_generations.rs:331-394`), but do not inspect the new binding or
  simulate a lost response specifically after the family commit. The injected
  owner-response test consumes its fault at the owner commit
  (`owners/tests.rs:121-161`), before a consumer binding exists.
- Suggestion: Test that the seal and binding have the same retained native
  revision and exact claim digest; inspect the expected owner grants; exercise
  retry after the consumer commit response is lost; and show refusal for a
  binding naming a missing/rebound owner token or altered claim. These are
  behavioral recovery assertions, not tests of formatting helpers.

## Patch summary

The collector reuses retention's typed dependency traversal to return an exact,
deduplicated `ObjectRef` closure. The owner service groups that closure by actual
object owner, verifies creation evidence, and commits a content-addressed durable
grant. Generation sealing acquires those grants before atomically committing its
seal and a digest-bound token list. Inspection recognizes the new owner grant,
consumer binding, and unavailability record families.

The accepted plan requires owner acquisition and removal reservation to conflict
through the same guarded owner state, with protection committed before another
dataset retains a reference. The implementation supplies the acquisition side
and tests native stale-view conflicts; it does not supply a removal service.
The repository's once-only work rule also applies to metadata reads
(`docs/PLAN.md:44-57`, `AGENTS.md:29-47`). Required lifecycle promises remain in
scope (`docs/PLAN.md:69-75`, `docs/PLAN.md:96-106`; the supplied accepted-plan
attachment's Object protection and Retention and deletion sections).

Important entry points are public `seal_generation` (`inputs.rs:17`), CLI sealing
(`main.rs:370`), HTTP sealing (`http.rs:148-152`), and public
`inspect_retention` (`inventory.rs:78`). The new protection primitive has one
production integration here: generation sealing (`inputs.rs:80`). Ordinary
data reads do not acquire these new grants.

## Function trace

Paths below are relative to the repository. Adjacent helper rows group functions
only when they share one small behavior.

| Function | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `seal_generation` | `src/engine/inputs.rs:17` | Exact family/member seal | Seal or refusal | Charges request and runs in owned admitted work. |
| `seal_generation_admitted` | `src/engine/inputs.rs:26` | Validated seal | Guarded consumer commit | Reconciles first; loads producer releases; acquires owners without consumer locks; reacquires member/family locks; checks current eligibility; commits claim and binding together. |
| `retained_seal` | `src/engine/inputs.rs:116` | Family view, expected seal and members | Existing seal or refusal | Exact historical comparison precedes binding validation; no payload reread. |
| `release_admitted` / `resolve_release` | `src/engine.rs:425`, `src/engine.rs:432` | Producer/version | Retained `ReleasePin` | Locks producer and checks selected result identity and verified status. |
| `lock_inputs` / `qualify_input` | `src/engine/inputs.rs:219`, `src/engine/inputs.rs:228` | Ordered dataset IDs / exact version | Guards / relationship | Locks in deterministic order; checks succession, retained identity, supported format and current eligibility. |
| `read_relationship` / `read_manifest` | `src/engine/inputs.rs:350`, `src/engine/inputs.rs:366` | Exact immutable object, byte allowance | Header / decoded metadata | Bounds read, verifies length and digest, decodes that buffer; keeps admission during blocking work. |
| `reference_objects` | `src/engine/retention/inventory.rs:27` | Producer-anchored release pins | Charged exact references | Bounds root shape, traverses explicit roots only, retains reservation in result. Caller is responsible for anchoring roots. |
| `Inventory::new` / `collecting` | `src/engine/retention/inventory.rs:289`, `src/engine/retention/inventory.rs:323` | Candidates / byte limit | Traversal state | Collection is explicit; ordinary inspection remains candidate-first. |
| `charge` | `src/engine/retention/inventory.rs:347` | Added metadata state size | Refusal at limit | Uses saturating accounting before retained insertions. |
| `path` / `object` | `src/engine/retention/inventory.rs:355`, `src/engine/retention/inventory.rs:380` | Path / exact object | Required path / deduplicated object | Collection records unknown native paths separately; rejects conflicting exact identities. |
| `enqueue` | `src/engine/retention/inventory.rs:402` | Object, interpretation, provenance | Charged pending entry | Deduplicates identity and interpretation; routes collection's native work after releases. |
| `release`, `snapshot`, `native` | `src/engine/retention/inventory.rs:456`, `src/engine/retention/inventory.rs:460`, `src/engine/retention/inventory.rs:468` | Exact root / native binding | Pending metadata | Distinguishes release and native interpretation; optional exact snapshot/schema binding. |
| `table` | `src/engine/retention/inventory.rs:472` | Retained complete table pin | Exact objects; completed native root | Requires metadata in inventory; uses creation-time inventories without native rereads. |
| `coverage`, `seal`, `computed`, `related`, `changes` | `src/engine/retention/inventory.rs:492-525` | Typed dependency records | Queued exact dependencies | Includes all sealed siblings, base releases, schema metadata, evidence, and before/after change dependencies. |
| `dataset` / `release_pin` | `src/engine/retention/inventory.rs:527`, `src/engine/retention/inventory.rs:552` | Dataset / anchored release | Dependency closure | Checks supported format and release identity; trusts supplied complete retained inventory. |
| `expand` / `into_references` | `src/engine/retention/inventory.rs:759`, `src/engine/retention/inventory.rs:329` | Charged pending roots | Exact closure or refusal | Reads each visited metadata identity at most once; native path-only dependencies need a matching exact identity before success. |
| `inspect_retention` / `inspect_retention_admitted` | `src/engine/retention/inventory.rs:78`, `src/engine/retention/inventory.rs:87` | Candidate objects | Boundaries and blockers | New grant records protect exact objects; bindings validate their token list; unavailability is recognized without deleting anything. |
| `protect_releases` | `src/engine/retention/owners.rs:232` | Consumer and anchored releases | Charged owner references | Collects closure, groups foreign owners, sorts objects, and serially locks/commits each owner. Self-owned references stay in the consumer authority. |
| `acquire_owner` | `src/engine/retention/owners.rs:263` | Owner, consumer, sorted objects | Durable token reference | Checks unavailability and producer reservation; reuses existing exact grant; confirms creation once for a new grant; reconciles a lost native response. |
| `check_object_owner` | `src/engine/retention/owners.rs:317` | Owner rows and exact object | Refusal or success | Requires owned attempt path, BLAKE3 shape, and retained attempt reservation belonging to the owner. |
| `confirm_creation` | `src/engine/retention/owners.rs:370` | Owner rows and expected objects | Creation qualification | Matches exact identity in typed phase/result/readback evidence or previous valid grants; reserved provisional paths alone fail. |
| `ReferenceProtections::binding` | `src/engine/retention/owners.rs:56` | Consumer view, claim key and value | Binding record | Carries claim digest, next native revision and previously acquired grant references. |
| `binding_key`, `unavailable_key`, `value_digest`, `protection_key` | `src/engine/retention/owners.rs:76-101` | Key/path/value/grant | Stable hash identity | Grant key includes owner, consumer and exact ordered objects; no mutable revision in retry key. |
| `validate_scope` / `validate_grant` | `src/engine/retention/owners.rs:122`, `src/engine/retention/owners.rs:103` | Owner grant and native view | Structural validation | Requires foreign consumer, nonempty sorted same-owner objects, actual table UUID and exact retained revision/key. |
| `available` | `src/engine/retention/owners.rs:164` | Owner rows, object path | Availability or refusal | An owner-side unavailability record refuses acquisition; no age-based inference. |
| `validate_binding` | `src/engine/retention/owners.rs:133` | Consumer rows, binding | Validation | Requires same-revision claim, exact digest, consumer authority and ordered foreign owner references. |
| `check_reference_binding` | `src/engine/retention/owners.rs:185` | Consumer view and claim key | Validation | Validates present bindings; absent bindings preserve legacy inventory promises rather than claiming an earlier owner grant. |
| `check_protection_references` | `src/engine/retention/owners.rs:201` | Binding token list | Validation | Checks each current owner's token, UUID, revision, consumer, and object availability; repeated owner reads cause F2. |
| `reference` / `invalid` | `src/engine/retention/owners.rs:359`, `src/engine/retention/owners.rs:434` | Grant / none | Token / structured refusal | Copies exact committed identifiers; emits `invalid_owner_protection`. |
| `Lifecycle::rows` / `commit` | `src/catalog.rs:169`, `src/catalog.rs:234` | Immutable view / changes | Bounded rows / new view | One-entry cache; native guarded commit, bounded retained history, lost-response fault after native commit. |
| `GuardedCatalog::load_table` / `update_table` | `src/catalog.rs:402`, `src/catalog.rs:414` | Expected native metadata / update | Native compare-and-swap | Stale application views refuse; adapter retries cannot refresh stale application decisions. |

## Data flow and invariants

- An `ObjectRef` binds owner, path, digest and length (`model.rs:62-69`). The
  collector creates one entry per path and refuses disagreements. Native metadata
  supplies paths only; those never acquire the metadata object's owner by
  inference (`inventory.rs:329-339`, `inventory.rs:355-364`).
- Complete release/table inventories originate in retained producer results;
  `seal_generation_admitted` resolves those results before calling the collector
  (`inputs.rs:66-80`, `engine.rs:432-455`). The collector is private and documents
  that precondition (`inventory.rs:16-22`). Its synthetic tests intentionally
  bypass producer anchoring to isolate traversal.
- The collector's owned references escape only with a held permit
  (`inventory.rs:10-13`, `inventory.rs:62-65`). Owner grouping makes copies under
  another reservation (`owners.rs:239-248`); returned token references keep their
  reservation until the seal finishes (`owners.rs:257-260`). F1 concerns excessive
  simultaneous reservation, not an uncovered allocation proved here.
- Every newly acquired foreign grant is committed under the same owner lifecycle
  view that was checked for unavailability (`owners.rs:271-302`). A stale native
  commit refuses. Acquisition never acknowledges an object based solely on its
  directory reservation (`owners.rs:295-301`, `owners.rs:425-429`).
- The stable token key excludes revision and table UUID; reuse checks both
  retained grant content and current owner UUID before returning
  (`owners.rs:96-119`, `owners.rs:285-294`). Lost-response reconciliation requires
  equality with the intended committed grant (`owners.rs:302-312`).
- Owner locks are acquired one at a time before the consumer/member lock set
  (`owners.rs:251-255`, `inputs.rs:80-85`). The consumer later checks mutable
  producer eligibility while its member locks are held (`inputs.rs:97-105`).
  This prevents local invalidation/cutover from slipping through the final seal.
- A failed handoff can leave a durable unused grant. That is conservative extra
  retention, permitted by the accepted plan; it is not evidence of completed
  consumer selection. No grant release exists in this increment.
- Seal and binding are one lifecycle change batch (`inputs.rs:106-112`). Existing
  binding validation requires their native revisions and digest to agree
  (`owners.rs:140-159`). Current construction creates the exact closure; the
  validator does not independently reconstruct that closure on retries.
- Legacy seals without bindings are accepted as legacy promises backed by the
  full inventory (`owners.rs:192-196`). This does not establish protection-token
  completeness for earlier records and must remain explicit before deletion.

## Test behavior and edge cases

All entries below are static trace assessments. No pass is claimed from this
review alone.

| Test | Location | Expected assertion path and limit |
| --- | --- | --- |
| `closure_collects_history_computation_coverage_seals_and_changes_without_payload_reads` | `inventory/collector_tests.rs:103` | Explicit roots plus persisted ancestor manifests produce exact expected objects; missing native/payload paths prove no hidden payload read. Returned references retain admission until dropped. |
| `native_paths_require_exact_identities_discovered_anywhere_in_the_closure` | `inventory/collector_tests.rs:219` | Missing identity refuses; later exact object satisfies the path without owner inference. |
| `native_expansion_never_assigns_the_metadata_owner_to_an_unknown_sidecar` | `inventory/collector_tests.rs:235` | Real native statistics sidecar remains unresolved until supplied with its actual owner; native expansion counter is asserted. |
| `conflicts_refuse_and_duplicate_references_do_not_consume_more_capacity` | `inventory/collector_tests.rs:289` | Owner/digest/length conflicts refuse, duplicates do not increase charged state, extra path exceeds bound; candidate inspection behavior stays separate. |
| `oversized_roots_refuse_before_following_missing_metadata` | `inventory/collector_tests.rs:328` | Oversized serialized root refuses before attempting missing ancestor I/O. |
| `owner_grant_retry_and_reopen_reuse_one_native_commit` | `owners/tests.rs:68` | Exact grant retry and reopened workspace preserve token and native revision; duplicate roots deduplicate. |
| `lost_native_owner_reply_reconciles_without_another_commit` | `owners/tests.rs:121` | Fault follows actual native commit; acquisition finds exact committed grant and does not append another. No consumer commit is involved. |
| `nested_release_grants_follow_each_actual_owner_without_rehashing_payloads` | `owners/tests.rs:165` | A joined release's collected objects partition among their actual owners; damaged payload bytes are not rehashed during acquisition. This proves acquisition does not verify payload integrity anew. |
| `a_reserved_path_without_completed_identity_cannot_grant_a_reference` | `owners/tests.rs:256` | A physical provisional file under a valid reservation lacks typed creation evidence and refuses without advancing revision. |
| `forged_object_identities_cannot_acquire_owner_protection` | `owners/tests.rs:298` | Forged digest, length or owner refuses; conflicting collector roots refuse before owner mutation. |
| `owner_availability_and_grants_conflict_at_the_native_boundary` | `owners/tests.rs:351` | Both orderings of grant versus directly inserted unavailability invalidate the stale native view; acquisition after unavailability refuses. This is a primitive race test, not a removal-service test. |
| `shared_owner_grant_survives_retirement_of_its_creation_phase` | `owners/tests.rs:433` | A grant remains the inspector's blocker after exact creation-phase retirement; retry reuses grant and file remains present. |
| `independent_and_coupled_inputs_follow_the_entire_consumed_member_map` | `tests/input_generations.rs:79` | New sealing runs through protection integration; mixed, unsealed and changed-member cases retain existing relationship refusals. No direct binding assertions. |
| `stable_family_membership_and_exact_retry_survive_restart_and_invalidation` | `tests/input_generations.rs:331` | Racing full/partial seal selects one; family/member lock overlap does not deadlock; exact retry after invalidation/reopen skips missing release manifests. No grant-failure assertion. |
| `invalid_seals_and_rebound_input_keys_refuse` | `tests/input_generations.rs:503` | Independent declaration and empty seal refuse before acquisition; rebound input identity refuses. |

Concrete uncovered cases are the small-budget seal in F1, repeated bindings to
multiple owners in F2, and consumer-binding failure/recovery branches in F3.
Partial multi-owner acquisition is conservatively safe by ordering: earlier
grants survive a later refusal and there is no consumer commit. Its precise retry
behavior deserves inclusion with the consumer-handoff test when adding fault
coverage. Physical removal and the broader foreign-reference integration are
unfinished by declared scope, not regressions introduced by this patch.

## Exploration record

- H1: Supplied release roots can reuse their complete inventories without new
  payload reads. Confirmed for the production seal caller by its producer-result
  lookup and for traversal by complete-pin marking (`inputs.rs:66-80`,
  `inventory.rs:472-489`, `inventory.rs:552-566`).
- H2: The native owner boundary excludes stale protection/removal decisions.
  Confirmed at the acquisition primitive and guarded adapter. Qualification of
  removal eligibility, effects and reconciliation remains unavailable because
  there is no removal service (`owners.rs:40-48`, `catalog.rs:402-421`).
- H3: Scoped reservations permit small metadata operations on small workspaces.
  Refuted for sealing by the simultaneous fixed-reservation lower bound in F1.
- H4: Inventory's once-per-request traversal prevents repeated owner work.
  Refined: external closure expansion deduplicates, but new binding validation
  bypasses that state and thrashes the single lifecycle cache (F2).
- H5: Added recovery tests qualify both sides of owner-to-consumer handoff.
  Refuted: owner-side recovery is covered; direct consumer-binding adversity is
  missing (F3).

## Initial conclusion

VERDICT: REQUEST CHANGES

Justification:

- The patch establishes an understandable owner-first acquisition path and a
  same-commit seal/binding boundary. Its collector reuses maintained native
  readers and existing typed inventory code, avoiding a second dependency walker
  or payload verification pass.
- Resolve F1 and F2 and qualify the consumer boundary in F3 before claiming this
  checkpoint complete. No discovered trace permits a dangling reference through
  the implemented local seal path while cleanup remains disabled.
- Coverage of changed paths: INSUFFICIENT for the new consumer-binding failure
  boundary and the identified resource cases.
- Confidence: HIGH for the static reservation bound, cache-replay trace, exact
  owner ordering and enumerated test gaps; execution and large-history resource
  measurements remain outside this review.

## Re-review and final disposition

The implementation was revised in response to this review. The initial line
references and traces above describe the first reviewed snapshot; the references
in this section describe the corrected source. This reviewer again used static
inspection only and ran no Cargo, formatting, or test command.

**F1 closed.** Sealing now splits its decoding reservation and retains only a
size-based charge for the releases before entering the collector
(`src/engine/inputs.rs:80-88`). The collector likewise splits its traversal
reservation to keep only the returned exact-object charge
(`src/engine/retention/inventory.rs:61-73`). Neither transition transiently
requests another permit for the same retained data. The original fixed 272 MiB
lower bound therefore no longer applies. The new end-to-end seal tests use the
existing 512 MiB / 16 MiB fixture, so they exercise the configuration that exposed
the issue (`src/engine/retention/owners/tests.rs:15-23`,
`src/engine/retention/owners/tests.rs:532`). This is a reservation-lifetime
improvement, not a measured process-memory or throughput result.

**F2 closed.** Inspection now stores bounded obligations grouped by owner and
token while it walks consumer bindings
(`src/engine/retention/inventory.rs:179-184`,
`src/engine/retention/inventory.rs:350-402`). Repeated references to one token
reuse the same obligation and merge required direct member manifests by exact
path. Conflicting token/consumer or object identities refuse. After the catalog
walk, `check_bindings` opens each referenced owner once, requires its native
metadata location to equal the boundary already reported, loads one
`SnapshotRows`, and validates the owner's requested tokens from it
(`src/engine/retention/inventory.rs:222-223`,
`src/engine/retention/inventory.rs:405-442`). Full grant payloads are not retained
globally. Grant availability was already checked during the first walk at that
same boundary (`src/engine/retention/inventory.rs:170-175`).

This still permits one additional lifecycle-history read per referenced owner
after the ordinary inventory visit. That bounded second pass is explicit and
avoids keeping every owner's full history in memory; its count no longer grows
with the number of bindings. The code's nested owner/token loops establish the
read-count bound structurally. No timed scaling claim or measured native-request
count is attached to it.

**F3 closed for this checkpoint.** The new tests directly cover atomic consumer
pairing, exact owner-object partitions, changed/missing bindings and tokens,
partial owner acquisition, failed consumer handoff, lost consumer response,
restart, and a family that also owns a member. The inspection-specific route now
has both success and refusal assertions. Required direct member roots come from
the committed typed seal rather than the supplied token list
(`src/engine/retention/owners.rs:161-187`); `validate_reference` checks those
manifests against the referenced owner's sorted exact objects
(`src/engine/retention/owners.rs:190-214`). Self-owned roots are correctly excluded
from foreign-token requirements (`src/engine/retention/owners.rs:177`).

| Added or extended test | Source | Verified static behavior |
| --- | --- | --- |
| `sealing_binds_exact_owner_grants_in_the_same_native_consumer_commit` | `src/engine/retention/owners/tests.rs:532` | Uses the small memory fixture; checks same native revision for claim, binding and membership; compares complete per-owner object partitions; retries/reopens without new commits; now runs `inspect_retention` after a real seal and requires all candidate manifests protected (`:558-568`). |
| `retained_seal_refuses_forged_binding_or_missing_owner_token` | `src/engine/retention/owners/tests.rs:643` | Rewrites claim/binding at one revision so stale revision is not the reason for refusal; changes digest, token, direct-owner coverage, and substitutes a real payload-only grant. Both seal retry and inspection refuse (`:691-703`). |
| `partial_owner_handoff_refuses_sealing_and_reuses_prior_protection_on_retry` | `src/engine/retention/owners/tests.rs:712` | Later owner's unavailability leaves the first grant retained and reused, with no family seal, binding, or membership commit. |
| `failed_consumer_handoff_keeps_grants_for_a_successful_retry` | `src/engine/retention/owners/tests.rs:767` | Missing member manifest causes failure after owner acquisition but before consumer selection; restoring the file lets retry use identical grants. |
| `lost_consumer_seal_reply_reuses_the_committed_binding_and_owner_grants` | `src/engine/retention/owners/tests.rs:836` | Preacquires owner grants so the injected lost reply occurs on the final family commit; checks retained same-revision claim/binding and retry/reopen without another owner or family commit. |
| `a_family_that_owns_a_member_needs_no_foreign_grant_for_its_own_objects` | `src/engine/retention/owners/tests.rs:915` | Family/member overlap needs only the other owner's grant and reconciles across reopen. |
| `repeated_bindings_share_claim_memory_and_owner_changes_refuse_inspection` | `src/engine/retention/inventory/collector_tests.rs:348` | Repeated bindings keep one owner/token obligation and unchanged byte charge; a native owner commit after the captured boundary causes `stale_authority` before token use. This measures admitted state through assertions, not process RSS. |

The previously produced `docs/reviews/owner-protection-focused-ready.log` was read
and records successful focused execution through the initial consumer tests. It
predates the final lost-consumer, self-owned-family, and inspection test additions;
it is not cited as execution evidence for those additions. At re-review time,
`docs/reviews/owner-protection-check.log` recorded successful strict compilation
checking and was still compiling/running the full repository gate. Final gate
completion must be reported from its completed log by the implementing agent.

The remaining scope limits are deliberate:

- The constructor collects and grants the complete transitive object closure.
  Retry validation checks immutable tokens and direct member-manifest coverage;
  it does not reconstruct the full closure each time. These grants are permanent
  in this increment. Future grant retirement must establish its own complete
  dependency and claim-lifetime rules before deleting anything.
- Legacy bindings may be absent, and other foreign-reference sites remain
  unfinished. Current complete inventory protections and disabled cleanup remain
  necessary; this review does not authorize deletion or claim full plan delivery.
- Full-history replay remains bounded by the current lifecycle metadata allowance.
  The improved inspection path has not qualified large-history latency, total
  process memory, or the complete Comments performance target.

VERDICT: APPROVE

Justification:

- The corrected increment provides durable owner-first acquisition and atomic
  seal/binding selection, with no remaining material correctness issue found in
  the reviewed local paths.
- F1's simultaneous maximum reservations and F2's per-binding history replay
  were removed while preserving exact identities, bounded state and conservative
  recovery. F3's missing consumer and inspection boundary tests were added.
- Coverage of changed paths: ADEQUATE for the scoped checkpoint by static test
  trace; final execution status belongs to the completed repository gate.
- Confidence: HIGH for the scoped static verdict. No physical-deletion,
  grant-retirement, hosted-operation, full-memory, or throughput qualification is
  implied.
