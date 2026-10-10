# R4: independent change-history lifetime

## Decision and findings

**Verdict: RECONSIDER the lifetime of `ChangePin` before enabling retained-chain
comparison.** Keep the applied-write producer, endpoint comparison, accepted
succession, and exact retirement machinery. Move optional change evidence out of
the complete state's required object closure and into separately retained catalog
records. A required consumer must name and protect the exact certificate it used.
This achieves R4 without introducing another event system.

This is a blind static architecture review of commit
`0011f7588ab58171c0f3d71e829b4902215ab70b`. No other review reports, corpus files,
builds, tests, formatting, benchmarks, or commits were used. The referenced test
source was inspected, not executed. Recommendations below are designs, not claims
of implemented or measured behavior.

### Finding 1 — BLOCKER: optional changes currently belong to the state closure

**Category:** intent-versus-shape; **recommendation:** RESHAPE.

R4 says a complete state remains readable when optional acceleration expires
(`docs/requirements/04-change-tracking.md:25`). Current results embed
`DatasetPin.changes`, and receipt selections embed `EvidenceResult.changes`
(`src/model.rs:139-166`, `src/evidence.rs:35-47`). `Inventory::dataset` follows
both, while `Inventory::changes` expands the base release, before/after native
metadata, and change tables (`src/engine/retention/inventory.rs:969-1005`). A
retained successor therefore protects historical acceleration even after the
historical result's own availability promise retires.

The problem is broader than the root walker. Cached-result availability includes
change objects in `release_objects`; explicit result verification opens the prior
source and applied changes. Update and incremental-refresh group verification
also require an embedded change record (`src/engine.rs:2298-2371,2422-2430`,
`src/engine/changes.rs:19-75,150-185`,
`src/engine/backfill/verification.rs:80-105,178-185`). Receipt verification requires
change evidence for a receipt update (`src/engine/evidence.rs:635-674`). Merely
ignoring certificates in the comparison planner would leave these obligations.

Ordinary managed field-group reads already resolve the retained selection and
open the requested table, without replaying its changes
(`src/engine/read.rs:13-30,53-90`). Preserve that behavior. Complete the separation
for admission, resume, explicit verification, and retention as well.

### Finding 2 — BLOCKER: a certificate root must not recursively retain endpoints

**Category:** named seam; **recommendation:** RESHAPE.

`parent_manifest` is another full-release edge in the root inventory
(`src/engine/retention/inventory.rs:1000-1002`). Required scalar, schema, and receipt
checks read parents for real interpretation evidence
(`src/engine/backfill/verification.rs:405-510`,
`src/engine/schema.rs:614-627`, `src/engine/evidence.rs:563-607`). Those are not
automatically disposable. But interpreting every historical reference as a full
state root also imports all of that parent's optional change tables and earlier
state dependencies.

Introduce a precise distinction between a retained state, required interpretation
evidence, and an optional comparison certificate. A certificate's endpoint
identities identify what was compared; they do not themselves promise continued
availability of either entire endpoint. The certificate must retain its own
removed/added relation and schema/identity interpretation. A consumer requiring
endpoint payloads must retain them explicitly.

Do not solve this by removing all parent protection. Some present consumers need
parent manifests, native schema metadata, or receipt/source relations. Preserve
those required objects until an equivalent, independently qualified proof replaces
their need. The first useful increment can retire optional change payloads while
conservatively retaining existing required provenance; completing broader R6
history retirement requires removing the remaining unnecessary full-state edges.

### Finding 3 — CONCERN: generic object protection needs an exact certificate claim

**Category:** ownership; **recommendation:** KEEP the mechanism, RESHAPE its input.

The existing owner-first mechanism already records exact objects before the
consumer's claim becomes durable. `ReferenceBinding` binds the consumer's record
key, revision, digest, and owner grants. Grant retirement refuses while an active
consumer names a grant (`src/engine/retention/owners.rs:16-43,546-604`,
`src/engine/retention/grants.rs:125-166`). This is enough infrastructure; another
lease registry or comparison database would duplicate it.

Extend typed consumer dependencies with an exact required-certificate reference.
Use the same record decoding and root collection to derive its object closure.
For a foreign owner, acquire existing owner grants before acknowledging the
consumer; for a local owner, the typed consumer record itself must be a root.
`protect_objects` deliberately filters out own objects, so owner grants alone
cannot provide the latter guarantee (`src/engine/retention/owners.rs:581-589`).

Ending optional discovery is distinct from ending an existing consumer's promise.
Retirement may hide a certificate from new optional comparisons while its bytes
remain protected by a required consumer, phase, reader, restore point, or active
attempt. Fresh required adoption must check the certificate's availability under
the owner authority, not just the object's `object-unavailable` marker.

### Finding 4 — CONCERN: append-only lifecycle history remains a separate bound

**Category:** scope and resource cost; **recommendation:** KEEP source succession;
RESHAPE later metadata retention without coupling it to certificate expiry.

Succession is already a separate co-committed relationship. Its root inspection
checks identities without expanding endpoint payloads
(`src/engine/history.rs:173-234`,
`src/engine/retention/inventory.rs:1388-1400,1446-1448`). Keep this separation.
However, `check_link_receipt` still proves each link against the corresponding
`result/<version>` row (`src/engine/history.rs:321-354`). Removing bulky retired
result rows would currently break ordering even if their tiny succession records
survived.

Cold lifecycle reads scan appended rows; commits count obsolete rows and copy the
current record map (`src/catalog.rs:228-253,342-365`). Retiring certificate payloads
alone therefore cannot establish history-independent metadata cost. Preserve the
minimal accepted-selection receipt needed by succession, and qualify lifecycle
compaction against restore/retry obligations before removing old record bodies.
Do not make optional certificates a new reason to retain every historical result
body indefinitely. This metadata work is separate from the smallest credible
certificate increment; it remains necessary for full R6/Q7 qualification.

## Artifact, lineage, and relationships

The artifact under review is the R4/R6 seam: local certification, forward
composition, optional acceleration retirement, required consumer evidence, and
accepted source succession. It is **proof infrastructure serving product and
operational outcomes**: small updates can identify exact affected members, while
expired acceleration only makes comparison slower.

| Prior artifact | Relationship | Evidence |
| --- | --- | --- |
| Accepted implementation plan | One lifecycle selection, creation-time validation, immutable reads, shared retention | `docs/PLAN.md:23-33,44-64,69-82,96-106` |
| R1 shared completion and admission | Qualified work survives; reuse binds exact scope and validator dependencies | `docs/requirements/01-durable-storage.md:29-40` |
| R4 | Applied writes, trusted links, full fallback, independent optional lifetime, separate order | `docs/requirements/04-change-tracking.md:13-29` |
| R5 | Required coverage cannot regress; absent order is not repaired by full value comparison | `docs/requirements/05-dependent-refresh.md:19-29,37` |
| R6 | State, recovery, optional acceleration, and provenance have distinct lifetimes | `docs/requirements/06-maintenance-retention.md:9,21-35` |
| R7 Q7/Q8 | Measure independent history axes and complete consumer cost | `docs/requirements/07-operations-qualification.md:133-147` |

| Component / seam | Owner | Inputs and consumers | Current status |
| --- | --- | --- | --- |
| Applied row production | Write/refresh/receipt processing | Removed/added native relations; validation and future certificates | Existing `ChangePin`; `src/engine/write.rs:368-379`, `src/engine/evidence.rs:328-365` |
| Creation proof | Engine changes and verification | Exact native endpoints, schema/identity rules, removed/added relation | Shared proof exists; exact reconstruction runs only in deep scope; `src/engine/changes.rs:78-201` |
| Trusted certificate admission | Dataset lifecycle selection | Prepared qualified evidence and exact selected result | New named seam needed at `select_with_progress`; `src/engine/history.rs:160-235` |
| Comparison | Engine comparison module | Selected endpoint context, optional certificates, endpoint oracle | One service already exposes identity/path/effects; `src/engine/comparison.rs:174-329` |
| Required consumer evidence | Existing typed completion plus owner protection | Exact certificate claim, protected objects | Extend existing `PhaseDependencies` and binding decoding; `src/model.rs:494-505`, `src/engine/retention/owners.rs:35-43` |
| Optional retirement and removal | Existing retention module | Exact catalog generations, fresh protection, durable effects | Add scope; reuse `src/engine/retention/retirement.rs:470-595` |
| Accepted source ordering | History module | Co-committed succession receipts, R5 coverage checks | Keep independent; `src/engine/history.rs:44-138` |

The implementation's greenfield rule permits a format change rather than a
compatibility adapter (`docs/requirements/04-change-tracking.md:3`). Design the new
format directly for independent lifetimes. Legacy embedded changes are unsupported
accelerators and use endpoint fallback where the endpoints remain supported; an
unsupported endpoint format may require rebuilding. Do not retain the old shape
in the new format merely to avoid that explicit boundary.

## Invariants and commitments

| Invariant | Status and required action | Failure if lost |
| --- | --- | --- |
| Complete states do not require optional acceleration | **Currently broken for retention/resume/verification.** Separate roots and checks as in findings 1–2. Source: R4 line 25. | Retired deltas either remain undeletable or break future use of the state. |
| Required evidence outlives its consumer promise | **Relied upon.** Bind exact certificate generation and objects to existing consumer roots. Source: R4 line 25; R6 lines 21–25. | A retry or retained result claims evidence that cleanup has removed. |
| Certificate trust comes from qualified creation and guarded selection | **New seam required.** A well-shaped file or `verified: true` alone is insufficient. Source: R4 line 15. | Forged or truncated changes omit affected work. |
| Bad present evidence refuses; absent/unsupported optional evidence falls back | **Required behavior for the new path.** Keep errors separate from non-applicability. Source: R4 line 23. | Corruption becomes an empty result or silently loses diagnosis. |
| Source order survives change expiry | **Preserved in current separation; relied upon by R5.** Keep succession and required selection receipts. Source: R4 line 27; R5 line 29. | Equal values or timestamps incorrectly authorize coverage regression. |
| New protection wins over an old removal plan | **Relied upon.** Use existing maintenance ownership and fresh inventory. Source: R6 lines 27–31. | Certificate removal races a consumer admission or live stream. |
| Compaction preserves logical occurrence and endpoint identity | **Relied upon.** Certificates bind logical endpoints; physical representations stay replaceable. Source: R6 lines 11–15. | A rewrite appears as a logical edit or old certificates point at invented occurrences. |

## Minimal credible design

### Target state, independent of migration

**Remove applied-change payloads from `DatasetPin` and `EvidenceResult`.** The
complete dataset retains current source/group/receipt/contributor selections,
their semantic input and interpretation dependencies, and the immutable completion
evidence that qualifies those selections. It does not retain removed/added tables
merely because an update once used them. A small immutable transition identity may
remain as provenance; it is a metadata fact, not a required payload reference.

`ChangePin` can remain the internal prepared applied-write relation while work is
in progress. Its long-lived external home is the separately selected certificate
descriptor. A completed result and its available optional certificates are
co-committed facts with separate retention promises. This is the end-state for
source changes and receipt changes, not a source-only exception.

| Boundary | Target verification and protection |
| --- | --- |
| Processing/evidence creation | The writer produces exact applied rows under attempt protection; its phase owns the native outputs needed to finish or recover. |
| Change certification | Establish the supported applied-write proof once and record exact endpoints, descriptor identity, material validator rules, and scope. A qualified construction or completed independent proof establishes trust; routine samples alone do not. |
| Result creation/readback | Verify assembled state and mandatory evidence, bind its durable completed checks, and select the optional certificate separately in the same guarded commit. |
| Routine state verification/admission/resume | Resolve the completed immutable state and its mandatory dependencies. Check retained completion identity and applicability; do not require optional transition payloads. |
| Deep state verification | Hash/read current retained objects and independently reconstruct semantic derived results from required inputs/evidence. Do not quietly make a claim about every expired historical transition. |
| Explicit transition audit | Recheck applied-write membership and the before-minus-removed-plus-added equation against the exact endpoints. It requires the selected transition evidence and reports unavailable proof when that evidence has legitimately expired. Present corrupt evidence refuses. |
| Recovery phase | Retain only the exact transition relation/certificate that its remaining work or declared reuse proof actually requires, until that phase promise is explicitly retired. |

Do not propagate a phase's entire applied-change dependency set into the final
result by default. A completed receipt phase may need removed source occurrences
to prove or repeat its own work; the final state's retained receipt relation and
qualified completion do not thereby need every earlier removal relation. If a
specific required consumer must retain that proof, name it explicitly. Conversely,
do not erase evidence from an active recovery phase to make a retry appear valid.
Its original material validation identity and exact proof scope remain load-bearing
under R1/R6 (`docs/requirements/01-durable-storage.md:31-40`,
`docs/requirements/06-maintenance-retention.md:27`).

This changes the meaning of later state verification deliberately: verifying a
complete retained state and auditing the historical procedure that produced it
are distinct operations. Explain the actual scope in the existing verification
result/status rather than adding a mandatory diagnostic workflow. Keep deep
transition audit available while its evidence is retained. Required semantic
evidence for the state never becomes optional merely because it was created in
the same operation as a change accelerator.

### Catalog record and descriptor

Create one small immutable `change-certificate/<result>/<group-or-scope>` record
for each supported relation, in the existing dataset lifecycle table. The spelling
is illustrative. It must contain:

- Certificate format and semantics/validator identity, with an explicit local
  trust scope and completed proof level. A processor package hash alone is not
  the qualification identity.
- Exact `before` and `after` `VersionRef`s, the selected operation, group/scope,
  and the co-committed result generation. The native lifecycle row supplies the
  actual commit revision; check equality with the result's selection revision.
- Comparison identity, schema/field-ID interpretation, key rules, and the exact
  completeness claim. Preserve multiplicity and occurrence addresses; a business
  key that groups several occurrences must not collapse them into one row.
- One immutable descriptor `ObjectRef` (owner, digest, byte size, path). That
  descriptor holds native removed/added pins, required interpretation metadata,
  and conservative field/evidence effects. Keep row keys and variable-size native
  inventories outside catalog rows.

Separate catalog selection from physical descriptor construction to avoid a
checksum cycle. Prepare a descriptor bound to the exact before state, intended
result version, native after table, and write completion. Serialize the result
manifest. Then atomically commit the certificate's exact after-manifest binding
with `result/`, authority, and succession. The descriptor need not contain its
own enclosing result's final manifest digest; the guarded certificate record
supplies that binding. A result need not contain the certificate's payload pins.

Do not embed a second copy of the complete `ReleasePin`, all ancestor manifests,
or full owner grants in every certificate. Do not use a certificate as another
source-order record. The producer owns it; catalog anchoring establishes local
trust. Imported files take the full path until external admission is separately
qualified (R4 line 48).

### Root meaning and availability

Use three concrete traversal purposes inside the existing inventory, rather than
a new general dependency framework:

| Root purpose | What it protects | What it does not imply |
| --- | --- | --- |
| State | Selected source/groups, receipts/contributors, exact semantic inputs, necessary interpretation/origin evidence | All prior optional changes |
| Required proof | Exact schema, origin, correspondence, or chosen change evidence necessary to interpret/recover the consumer | General availability of every historical state mentioned by identity |
| Optional certificate | Its descriptor, removed/added tables, and required schema/identity metadata | Entire endpoint states, earlier certificates, or a source succession promise |

The present inventory's typed methods and `Expansion` queue are a natural place
for this distinction (`src/engine/retention/inventory.rs:454-475,794-889`). Start
by excluding optional changes from every state/provenance traversal; merely
excluding them from top-level `result/` leaves them reachable through parents.
Keep actually required parent evidence. Replace unnecessary full-state parent
expansion only after each consuming check's precise dependency has been traced.

A new `RetirementScope::OptionalChanges` should name exact certificate record
generations using the existing `RetainedGeneration`, plan, retirement receipt,
and marker rules (`src/model.rs:538-570`). Logical retirement ends normal optional
discovery; it does not erase immutable certificate identity or delete bytes. An
already bound consumer retains the exact descriptor and data through its own
root, even if the optional root retires. Subsequent exact removal still intersects
the captured plan with fresh eligibility.

Retiring only the optional root may reclaim zero bytes while processing phases,
attempt reservations, owner grants, or restore points still protect them. That is
a truthful blocker, not a reason to release those roots automatically. Existing
phase and operation retirement can end the corresponding recovery promises.
Restore currently captures native registrations as well as logical roots, so
retained restore points can legitimately extend physical lifetime
(`src/engine/retention/inventory.rs:222-224`).

### Creation, use, and required consumer binding

The writer retains the expensive phase and its applied relations before selection.
The certificate creator qualifies exact endpoint membership, identity/multiplicity,
and completeness for the supported write path once, then records that qualification
with its precise dependencies. Existing `verify_native_changes` provides the
independent membership/reconstruction oracle; its routine branch stops before
the full equation (`src/engine/changes.rs:165-201`). Do not relabel routine output
inspection as a complete proof. A trusted applied-write construction can establish
completeness only after that construction and its recovery path are qualified
against the oracle. Use a deep qualification when admitting untrusted preexisting
change material.

Ordinary state availability must stop calling applied-change checks. Explicit
state verification checks the retained state and mandatory evidence; checking a
historical transition is a distinct scope requiring the relevant transition
evidence. Missing optional history must be reported as unavailable for that
historical audit, not as a successful full historical reconstruction. This does
not weaken the endpoint full-comparison oracle, which operates on retained
endpoint values independently.

A consumer that needs reproducible chosen evidence records the exact certificate
identity, generation, descriptor reference, and qualified interpretation in its
ordinary phase/result dependencies. Its `ReferenceBinding` continues to bind that
typed claim's digest. Protection acquisition checks availability before the
consumer commit; failures retain excess protection for recovery. Consumer reuse
requires its own retained claim, even after optional discovery ends. Never infer
permission to adopt a retired certificate from finding its bytes on disk.

For streamed comparison, existing `admit_stream` lifetime protects the captured
relations through EOF/drop (`src/engine/comparison.rs:140-171`). Use the same
captured endpoint views and request budget for certificate lookup and composition;
do not create a second read/admission subsystem.

### Forward composition and exact failure classification

Extend the existing comparison planner with a typed discovery outcome such as
`QualifiedChain`, `NotApplicable(reason)`, or an error. Keep these decisions in
the same comparison service and retain the force-full control. Do not catch a
generic error and turn it into fallback.

| Observation | Required outcome |
| --- | --- |
| No certificate record, or exact optional generation durably retired | Full endpoint comparison; explicit missing/retired-history reason |
| Recognized certificate format with unsupported transform/identity/schema semantics, reverse request, unrelated endpoints, or declared chain limit | Full endpoint comparison if its own semantics support the request; explicit reason |
| Present supported certificate with malformed fields, conflicting generation/endpoint, missing referenced object, digest mismatch, incomplete relation, or invalid continuity | Refusal; no empty result and no corruption-hiding fallback |
| Unknown certificate format | Treat as unsupported only through a validated outer version/trust header; arbitrary parse failure is malformed evidence |
| Required consumer binding loses its chosen evidence | Refusal; optional fallback must not silently change its retained proof |

Resolve chain metadata before reading large payloads. Every hop must connect exact
version and interpretation identities. Selection history may establish an accepted
forward route, but it cannot prove a change relation complete. Conversely, change
files cannot establish R5 ordering. Unsupported composition-only hops may initially
fall back; qualify explicit no-value-change hops before skipping them.

For supported hops, compose native typed relations by logical identity, preserving
all relevant occurrences within a key. Cancel intermediate or reverted changes
only under the requested values/occurrences equality. Union affected field groups
conservatively. Preserve the current independent metadata-effects reporting so an
empty row delta cannot hide receipt, schema, definition, or coverage changes
(`src/model.rs:729-770`). Use maintained DataFusion sort/aggregate/join execution
and bounded spill. Do not materialize an unbounded map of all changed rows in Rust.

## Smallest meaningful implementation sequence

These are reviewable increments toward the accepted R4/R6 requirements, not new
definitions of completion. Keep one design and the existing services throughout.

1. **Separate state and transition evidence before accelerating reads.** Introduce
   the new result/certificate format and exact typed root purposes. Produce the
   independently retained local certificate through the same processing and
   selection services. Move mandatory update/receipt creation checks out of later
   state availability; retain the needed creation/interpretation evidence.
   Implement explicit optional-certificate retirement through the existing exact
   retirement service. Decisive acceptance: a retained updated state still reads,
   admits as a new exact input, resumes qualified work, and full-compares after
   its optional changes retire and eligible change-only objects are removed.
   Source succession remains valid. R4-T01–T03 and the R4-T05 lifetime seam land
   together; a new unused certificate type alone is not this increment.

   The next code step is a coherent change across `model.rs`/`evidence.rs`, the
   producer's prepared completion, `history::select_with_progress`, the typed
   inventory, `release_objects`, and `verify_local_release`/receipt/group checks.
   Add the exact optional-retirement scope at the same checkpoint. Keep the
   existing full comparator as the sole public comparison path until the
   retirement/read/admission/resume fixture passes. That proves the target
   lifetime without first building chain composition or preserving old embedded
   changes as permanent required roots.

2. **Use a single certified hop in the existing comparison service.** Add local
   trusted discovery, exact failure classification, and explicit path/fallback
   reporting for the first supported source update. Qualify it against full
   comparison for duplicates, deletion, replacement, equal-value/new-occurrence
   replacement, no-op, receipt-only effects, malformed evidence, and interrupted
   creation/selection. Retain force-full as the independent comparator. Report
   any still unsupported scope; do not promote routine samples into certificates.

3. **Compose bounded forward chains and bind required consumers.** Extend the
   same relation planner to continuity, reversions, one-to-many shrinkage, hot
   keys, and bounded long chains. Add chosen-certificate dependencies to the
   ordinary consumer phase/result path and exercise owner-first protection,
   same-dataset roots, retirement/adoption races, shared bytes, and lost responses.
   R5 candidate discovery uses this service or full fallback; it keeps its separate
   succession check. An existing required binding survives optional expiry;
   missing required evidence refuses.

4. **Close remaining R4/R6 qualification, including storage and metadata cost.**
   Add supported schema/field/evidence transitions and compaction equivalence;
   preserve unsupported fallback until each case qualifies. Trace and reduce
   unnecessary full-state ancestry retention, preserving actual scalar/schema/
   receipt obligations. Compact bulky retired catalog history only with minimal
   selection/order receipts, retry identity, removal outcomes, and restore support
   proven intact. Measure the fixed-update growth axes and complete consumer flow
   required by R4-T06, R6-T07, and Q7/Q8. Cross-workspace trust and remote deletion
   remain separate qualifications, not automatic consequences of local results.

For the retirement increment, the most revealing fixture is a successor with both
source changes and receipt changes, another successor that preserves a computed
group, a retained consumer of one exact certificate, and an unbound certificate.
Retire optional history: state and order still work; the consumer's exact evidence
stays protected; only unbound eligible change objects can disappear; new optional
comparison falls back. Then retire the consumer's promise and verify the common
removal inventory can release its formerly protected objects. Corrupt a present
certificate independently and require refusal.

## Performance and complexity costs

Let `N` be endpoint rows, `D` the changed occurrences across the selected chain,
`H` selected hops, `L` retained lifecycle bytes, and `F` referenced native files.
These are analytical costs; no timings were collected.

| Path | Expected work and limiting cost |
| --- | --- |
| Write/certification | Reuse already emitted removed/added tables. Descriptor construction is proportional to its file/schema references; add a small co-committed catalog row. An initial independent proof may still scan endpoint populations. Do not claim update-proportional production until measured. |
| Warm chain discovery | Approximately `H` exact record lookups and small descriptor checks; no per-row catalog work. Bound retained descriptors and native relations. |
| Cold discovery | Present lifecycle cache loading is `O(L)` before hop lookup. A short chain does not currently make cold catalog access proportional to `H`. |
| Composition | Read `D` occurrence/value rows; maintained hash/sort operations may spill, with sorting up to `O(D log D)`. Hot keys and long chains can approach or exceed endpoint work; fall back before starting an uneconomic unsupported path where metadata establishes that choice. |
| Full fallback | Complete endpoint work, roughly proportional to endpoint population plus native metadata and the chosen multiset operation. Retirement changes cost, not correctness. |
| Retention preview | Current inventory visits catalog roots across datasets and deduplicates references; certificate roots add work proportional to their live descriptors/files. A required consumer can prevent reclamation after optional expiry. |
| Retained storage | Active certificates retain delta payloads and small interpretation metadata, not entire endpoint closures. Required consumer/phase/restore roots can extend this. Retired catalog receipts still accumulate until separately qualified lifecycle compaction. |

Concrete simplification: one applied-write producer, one comparison planner, one
typed root collector, and one retirement/removal mechanism. Remove change-payload
dependencies from complete-state admission and repeated verification. No global
event log, per-key catalog graph, new SQL store, or durable comparison cache is
needed. The cost is a small certificate type, an optional-retirement scope, and
explicit traversal/verification purposes. The larger remaining cost is correctly
separating real historical interpretation evidence from accidental full-state
retention; a boolean `optional` flag cannot perform that analysis.

Preregister cold/warm comparison, creation/proof overhead, resumed work, catalog
bytes/requests, object bytes, peak memory, spill, and actual retained/reclaimed
storage with fixed updates while `N`, `H`, `L`, and `F` grow independently. Include
required consumers and restore points. Fewer endpoint reads in the comparison
function alone do not establish complete-path benefit.

## User value and counterfactuals

**User value:** consumers receive exact change scope when it is available and can
keep using retained states after the optional speedup expires. Operators can name
the real blocker to reclamation. The added proof infrastructure is justified only
if creation, recovery, and retention overhead still produce a useful complete
consumer improvement (R4 lines 42–48; R7 Q7/Q8).

**Smaller alternatives:** keeping only full comparison has the least new machinery
and remains the reliable fallback, but does not satisfy the small-update outcome.
Keeping embedded change tables forever simplifies current verifier assumptions
but directly contradicts independent lifetimes. Adding a detached certificate
without changing state and parent closure traversal fails for the same reason.
The existing owner protections and retirement generations subsume the proposed
protection infrastructure; reuse them.

**Kill criteria:** stop and reshape this design if retiring an unbound certificate
still makes a retained endpoint unusable, if a required consumer loses its chosen
proof, if a malformed active certificate silently becomes fallback, or if an
accepted-order decision requires an expired change payload. A measured complete
path whose certificate maintenance exceeds its consumer savings invalidates the
performance claim, even if chain lookup itself is fast.

**Removal probe:** removing all optional certificates must leave state reads and
full comparison usable. Removing a required chosen certificate must make its
consumer fail explicitly. Removing required succession must make R5 promotion
refuse; full value reconstruction cannot manufacture order.

**Six-month critic:** the likely concern is that small certificate records were
added while copied lifecycle rows, generic parent expansion, or repeated deep
certification kept total cost tied to history/population. Record those dimensions
now and retain them in the completion criteria.

## Verdict record and reasoning trace

| Review hypothesis | Static observation | Conclusion |
| --- | --- | --- |
| Optional history already has an independent root lifetime | Result/receipt/phase walkers expand `ChangePin`; no optional-change retirement scope exists | Refuted; reshape root ownership |
| Source order must be carried by change certificates | Succession commits separately and inspection does not expand endpoint payloads | Refuted; keep the existing separation |
| Existing protections can retain chosen certificate proof | Exact owner grants and typed consumer bindings exist; local objects need direct consumer roots | Confirmed with the typed dependency extension |
| A missing optional chain can always be treated like malformed evidence | R4 explicitly differentiates fallback from invalid present evidence | Refuted; preserve typed classification |
| Separate certificates alone bound historical cost | Cold catalog loading counts appended history and parent proofs retain ancestry | Refuted; metadata/provenance qualification remains |

**Verdict:** RECONSIDER, with the concrete reshape described above.

- Intent versus shape: diverges at state/change retention and verification.
- User-value claim: supported, conditional on complete-path measurement.
- Commitment status: source-order separation is honored; optional lifetime is open.
- Conceptual debt: pays down implicit dependency coupling by reusing existing
  lifecycle, comparison, protection, and removal services.
- Sibling subsumption: existing retirement and owner-protection machinery covers
  the infrastructure; certificate semantics and root classification are missing.
- Confidence: high in the static coupling findings; medium in implementation cost
  until exact parent-proof dependencies and race fixtures qualify the new format.

All six architecture-review phases were covered. No runtime qualification was
attempted; this report does not certify full R4/R6 completion or performance.
