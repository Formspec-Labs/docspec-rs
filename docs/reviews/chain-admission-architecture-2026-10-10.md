# R4 chain admission architecture review

**Verdict: APPROVE the existing comparison service as the implementation seam.**
Add bounded local certificate discovery and supply its signed native relations to
the existing normalization. Do not add a second comparator, event log, cache or
reader lease system. A transient comparison already has the required maintenance
exclusion. Durable R5 use needs an explicit retained dependency only when its
request or phase promises to retain the selected certificates.

This is an independent static review of commit
`2a5ae513cb0aa7c40f771a534474956ca92364c8`, dated 2026-10-10. It reviews the next
R4 scope, not an implementation patch. No code, builds, tests, formatting,
benchmarks, cleanup or commits were performed. The untracked
`change-chain-implementation-review.md` and other agents' reports were not read.
The semi-formal architecture-review and Rust guidelines informed the review.

## Findings

### F1 — Share an internal lookup; do not call the public inspector recursively

**CONCERN · named-seam · RESHAPE**

`change_certificate` is an inspection operation. It acquires `admit_work`, opens a
fresh dataset view, and applies standalone result-availability checks
(`src/engine/changes/certificates.rs:241–295`). Calling it for every link from
`comparison_plan` would re-admit an already admitted operation and repeat view
selection. A queued maintenance writer can make nested read admission wait behind
the writer while the outer comparison retains its read guard: the existing stream
implementation explicitly avoids this pattern (`src/engine/worker.rs:175–178`).

Extract the existing certificate/result revision checks into an internal lookup
over the already captured `View` and `SnapshotRows`. Share it with inspection.
Read and digest each selected descriptor once through `inputs::read_manifest`
(`src/engine/inputs.rs:399–440`). Keep the public inspector's result-availability
check at its public boundary; do not require every intermediate standalone result
to remain available during chain traversal.

The latter distinction is intentional: optional history retains interpretation
metadata and applied tables without retaining complete intermediate states
(`src/engine/retention/inventory.rs:993–1008`;
`docs/optional-change-history.md:15–19`). The requested comparison endpoints still
must pass `check_read_release` and immutable manifest resolution, once each
(`src/engine/comparison.rs:182–192`).

### F2 — Stream protection exists; durable certificate adoption does not

**CONCERN · ownership · KEEP existing stream custody; RESHAPE retained adoption
before claiming it**

`compare_with_options` uses `admit_work`; `changes` uses `admit_stream`
(`src/engine/comparison.rs:72–75,165–170`). Those hold shared maintenance admission
through execution or stream EOF/drop (`src/engine/worker.rs:111–134,159–200`).
Certificate retirement and physical removal use exclusive maintenance admission
(`src/engine/retention/retirement.rs:501–509`;
`src/engine/retention/removal.rs:257–272`). Thus a chain discovered inside this
scope cannot lose its certificate promise or files before the stream finishes.
The existing runtime exclusion is sufficient for the local transient path.

The same is not a durable promise after the scope ends. `CertifiedChanges`
explicitly creates no lasting protection (`src/model.rs:1117–1126`). Current
`AdmissionDependencies` has no selected certificate dependency
(`src/engine/admission.rs:11–19`). `protect_releases` walks complete states, which
deliberately omit optional changes (`src/engine/retention/owners.rs:546–555`;
`src/engine/retention/inventory.rs:1011–1034`). `protect_objects` excludes
same-owner objects; their protection must come from a retained local record
(`src/engine/retention/owners.rs:568–603`).

When a frozen R5 request or recoverable phase names a chain as required evidence,
retain its exact certificate generations, descriptor references, endpoints and
rules in that ordinary request/phase dependency. Extend the existing typed root
walker to protect the descriptor, interpretation metadata and applied table
objects. Use existing owner-first `ReferenceProtections` for foreign objects and
commit the consumer binding atomically with its record
(`src/engine/admission.rs:42–65`;
`src/engine/retention/owners.rs:149–171,663–726`). Acquire without holding consumer
dataset locks and retain the existing boundary recheck.

Retirement may end the original optional promise while a previously admitted
consumer retains its own promise. Fresh consumers cannot acquire a retired
certificate merely because its bytes remain. Resume must use the retained exact
claim, rather than fresh discovery that can choose a different path. Existing
binding validation and propagation already provide the relevant generation and
retirement mechanics (`src/engine/retention/owners.rs:234–289,437–495`).

This is a blocker to claiming durable certificate adoption, not a reason to build
a new ledger for transient comparison. R5 can consume the relation inside its
existing admitted operation and retain an ordinary completed native discovery
phase. Its chosen material dependencies determine what must survive; it needs no
independent comparison cache (`docs/requirements/01-durable-storage.md:29–40`;
`docs/requirements/05-dependent-refresh.md:35–39`).

### F3 — Compose stable occurrences before selecting value representatives

**CONCERN · commitment-violation · KEEP the existing normalization**

The writer's removed relation preserves prior origins, and the added relation
shares the actual candidate files (`src/engine/write.rs:519–534,595–660`). Updates
bind `ChangePin.before_metadata` to the canonical source even when they process a
qualified compaction representation (`src/engine/write.rs:192–240,371–379`).
Continuity must therefore use exact version/manifests and canonical logical
source references. Comparing those links to the current physical representation
would falsely reject a valid transition after compaction.

For each qualified source transition, use removed rows with weight -1 and added
rows with weight +1, through the maintained occurrence reader. Feed their union
to `change_members`, which first cancels exact `(values, file, row)` identities
and only then ranks surplus equal values (`src/engine/comparison.rs:440–504`).
The existing test illustrates why ranking raw chain members first can expose an
intermediate address belonging to neither endpoint
(`src/engine/comparison.rs:588–646`).

Do not build a Rust map from business key to one before/after row. The writer
supports replacement of all rows for a key and preserves multiplicity. Counts
alone do not identify the surviving occurrence, and equal displayed values do
not establish occurrence equality (`docs/requirements/04-change-tracking.md:9,
21`; `src/model.rs:745–753`). Schema/identity checks and local production trust
establish eligibility; normal comparison must not repeat the independent full
transition audit.

### F4 — A missing accelerator is a fallback; a broken selected claim is an error

**CONCERN · trust-posture · RESHAPE path selection as an explicit decision**

Current local trust has a concrete authority: known producing rules and a
certificate co-committed with a verified result at the same native revision
(`src/engine/changes/certificates.rs:72–114`;
`src/engine/history.rs:220–244`). A producer marker is not portable admission,
and markerless recovered work does not become certified by later sampling
(`src/engine/changes/certificates.rs:153–160`).

Represent discovery as eligible chain, explicit fallback, or refusal. Do not
catch all discovery/read errors and silently execute a full comparison. Missing
or retired certificates, unsupported groups/rules of comparison, reverse or
unrelated endpoints, and a declared chain bound can select full reconstruction.
A present malformed certificate, wrong result revision, conflicting immutable
identity, descriptor digest mismatch, impossible continuity, or a read failure
after selecting the accelerator must refuse. A valid chain that simply does not
reach the requested before version is an unsupported relationship, not proof of
empty changes (`docs/requirements/04-change-tracking.md:15,23,48`).

Validate each selected link's exact after identity, exact predecessor identity,
local result receipt, producing rules, source before/after metadata and schema
compatibility. Check cycles/nondecreasing causal revisions so malformed history
does not become an arbitrary chain-limit fallback. Derive output effects from
the original requested endpoints, retaining the current evidence/coverage/
definition distinctions even when the net value relation is empty
(`src/engine/comparison.rs:521–561`).

### F5 — Source succession and refresh discovery remain separate decisions

**OBSERVATION · sibling-conflict · KEEP the existing ordering authority**

An applied certificate's predecessor identifies the exact base used for the
write. Accepted succession records the prior selected result and is co-committed
independently (`src/engine/history.rs:180–200,220–244`). They must not substitute
for each other. R5 promotion still requires its retained accepted-history proof;
a full rebuild does not repair missing ordering evidence
(`docs/requirements/05-dependent-refresh.md:27–35,45`).

Current R5 `changed_keys` discovers changes from physical file membership, with a
logical-metadata fast path (`src/engine/incremental.rs:316–455`). A qualified R4
relation can replace that candidate-discovery input. It cannot replace old/new
join-key expansion, previously contributing rows, complete affected-group
recomputation, evidence handling, or promotion checks
(`docs/requirements/05-dependent-refresh.md:19–25`). Occurrence mode is the safe
default for evidence-dependent refresh; value mode alone can erase an equal-valued
replacement whose origin changed.

## Artifact summary

The decision under review is how to implement local trusted forward composition
for R4-T03/T04 and preserve its R5/R6 boundaries. This is a product capability:
small updates should expose exact changed members without full endpoint scans.
The upstream plan requires one engine, maintained Arrow/DataFusion processing,
work reuse and one protection inventory (`docs/PLAN.md:11–40,44–57,96–102`).
`docs/STATUS.md:109–112,166–169` correctly leaves acceleration, R5 reuse and
resource qualification unfinished.

The certificate producer and the full comparator are implemented. The proposed
new work supplies a third plan path to that comparator; it does not redefine
complete-state readability or accepted source order. External certificate
admission and remote trust are outside the inspected local scope.

## Lineage and relationships

| Prior artifact | Relationship | Source |
| --- | --- | --- |
| Accepted implementation plan | One engine, maintained components, immutable selected versions, shared retention | `docs/PLAN.md:11–40,44–57,96–102` |
| Research requirements UR-02/UR-04/UR-06 | Select once, preserve evidence effects, measure the complete consumer path | `docs/requirements/RESEARCH.md:27–33` |
| R1 shared phase/read requirements | Own exact pins and qualified scope; preserve completed work | `docs/requirements/01-durable-storage.md:29–40` |
| R4 design | Local trust, exact composition, fallback/refusal, optional acceleration | `docs/requirements/04-change-tracking.md:13–27` |
| R5 design | Consume the same context, discover complete affected scope, preserve nonregression | `docs/requirements/05-dependent-refresh.md:13–39` |
| R6 retention | Different promises have independent lifetimes; new protections win | `docs/requirements/06-maintenance-retention.md:21–33` |
| Optional history implementation at reviewed commit | Separates complete state from certificate/phase roots | `docs/optional-change-history.md:7–19` |

| Component / seam | Owner | Existing responsibility | Required addition |
| --- | --- | --- | --- |
| Applied writes → certificate | R4 producer + guarded selection | Produce actual removed/added rows; bind local rules and same result revision | Reuse |
| Captured comparison context → relation | R1/R4 engine | Endpoint admission, schemas, physical representation, shared budget | Internal bounded chain discovery |
| Relation → count/stream | R4 + DataFusion | Signed multiset normalization; exact endpoint addresses | Feed qualified history through the same implementation |
| Stream → maintenance | R1/R6 worker | Shared admission to EOF/drop; exclusive retirement/removal | Reuse; no nested public admission |
| Retained consumer → owner objects | R1/R6 admission/inventory | Exact generations, owner grants, local roots, atomic bindings | A chosen-chain dependency only for durable adoption |
| R4 → R5 discovery | R5 | Old/new key expansion and complete affected groups | Consume internal relation without opening another context |
| Accepted succession → promotion | R5/history | Independent coverage order | Preserve unchanged |

These seams are named by the cited requirements; no new cross-layer framework is
needed. The missing durable dependency shape in F2 is an implementation gap,
not an undefined owner.

## Invariants and commitments

| Invariant | Status | Evidence and failure mode |
| --- | --- | --- |
| One committed catalog selection defines local trust | RELIED-UPON | Certificate/result revision equality in `certificates.rs:84–96`; trusting a supplied file bypasses local authority. |
| Each chain ends at the requested exact versions | RELIED-UPON | `ChangeDescriptor` endpoint refs and R4 continuity; matching values or version names alone can join unrelated states. |
| Occurrence and value identity are distinct | PRESERVED by shared normalizer | `comparison.rs:450–504`; early value cancellation can lose source replacement or expose intermediate addresses. |
| Optional history is independent of complete state | PRESERVED if intermediate availability is not reimposed | `inventory.rs:993–1034`; traversing every full release would recreate lifetime and I/O coupling. |
| Required consumer evidence survives retirement | RELIED-UPON; durable chain shape missing | R4:25 and R6:23–31; retaining metadata without a typed root leaves restart-dependent dangling inputs. |
| Reader protection lasts through consumption | PRESERVED by current host | `worker.rs:175–200`; releasing at plan return lets maintenance race lazy reads. |
| Source order is independent of accelerator retention | PRESERVED if history stays separate | R4:27 and `history.rs:192–200`; treating a change chain as the promotion order can admit regression or branches. |
| Shared budgets bound retained plans and decoded data | RELIED-UPON | `budget.rs:85–95`; an unbounded list of native providers can exhaust application memory before spill helps. |
| Qualification is not a speed claim | PRESERVED | `docs/STATUS.md:166–169`; static traces and algebra tests do not measure whole-process memory or latency. |

## Smallest implementation shape

1. Extend the current plan-path/fallback enums. Keep `force_full` independent of
   optional discovery; a forced full oracle should not open accelerator files.
2. Capture one local view for same-dataset traversal. Reuse endpoint checks and
   schemas; follow exact predecessor certificate links backward from `after`
   until `before` is reached. Do not enumerate all catalog history or traverse
   full historical payload inventories.
3. Admit a bounded collection of compact link metadata before building native
   plans. Bound cumulative decoded bytes and links, including empty transitions.
   Release abandoned discovery state before full fallback. A conservative named
   link bound prevents excessive DataFusion plan depth; document that its value
   is a safety limit, not a measured optimal threshold.
4. Validate local certificate authority and metadata continuity once. Open each
   needed applied native table once through the existing readers. For source-only
   comparison, a transition without source changes can be bridged only when the
   exact source identity proves no source effect; otherwise use explicit full
   fallback. Unsupported schema mappings or field groups also fall back.
5. Feed the signed native relation to the existing count or member path. Let
   maintained aggregation/sort spill within the existing pool and scratch limits
   (`src/budget.rs:85–95`). Keep compact admissions alive with the plan/stream.
   Do not collect decoded history into a Rust map or add a custom spill engine.
6. Keep R5 access internal to the admitted context. Count rows from a needed
   stream instead of executing both counts and changes. When a chosen certificate
   must survive restart, represent that fact through F2's existing request/phase
   protection mechanism before acknowledging the dependent work.

This reduces the new implementation to local authority selection and relation
assembly. The existing comparison, decoding, spill, output descriptor and lifetime
code do the rest. The expected I/O improvement is that supported comparisons read
the chosen delta tables instead of both endpoint populations. It remains an
unmeasured expectation; source-update key scans and physical rewrites retain
their existing costs (`docs/requirements/04-change-tracking.md:42–46`).

## User-value and counterfactual checks

The direct beneficiary is a consumer discovering which members changed. R5 gains
a narrower discovery relation while keeping its complete recomputation rules.
Conceptual debt stays small because local certificate production and a compatible
normalizer already exist. A separate chain service would duplicate trust,
schema interpretation, budgeting and lifetime decisions.

The value claim fails if fixed-size updates still decode complete endpoint
payloads merely to certify each link, or if history metadata/provider costs
dominate the full fallback under the declared bound. Measure cold and warm work,
unchanged results, long chains, high churn, memory/spill and interrupted recovery
as R4-T06 requires; no benchmark was run for this review.

The correctness kill criterion is any accelerated result that differs from a
forced full result in duplicate counts, typed values or side-specific endpoint
addresses. Other decisive failures are an empty value result hiding an evidence
effect, or required retained chain files becoming removable while a live consumer
still depends on them.

The counter-decision is to keep full comparison only. It simplifies discovery but
leaves the stated small-update outcome unfulfilled. Removing optional history
must preserve complete reads and enable the existing full path; removing required
succession must continue to refuse R5 promotion. The existing comparator fully
subsumes normalization and presentation, while the existing owner/inventory
mechanics partially subsume durable adoption. A future reviewer should challenge
repeated endpoint reads and duplicate dependency stores before proposing new
indexes or caches.

## Qualification needed for this implementation

Use focused fixtures with independent endpoint expectations and the existing
forced full oracle. Include duplicate/null keys, one-to-many shrinkage, insert
then remove, replacement then reversion, equal value with a new origin, and a
lexically late intermediate origin. Compare counts and stream addresses as well
as values. Exercise source updates after compaction and endpoint representation
changes without logical edits.

Exercise missing/retired history, a valid but unsupported/reverse relationship,
the declared chain limit, and incompatible schemas. Separate those from forged
revision/endpoints, cycles, descriptor mutation and selected-table read failures;
only the first class falls back. Verify evidence/definition/coverage effects on
empty value differences and unsupported computed-group paths.

For lifetime, pause a returned chain stream, request certificate retirement and
physical removal, and verify they wait until EOF/drop. Then show a new comparison
falls back after retirement. For durable adoption, separately establish an exact
consumer binding, retire the optional producer promise, restart, and verify its
retained dependency still reads while fresh adoption follows the retirement rule.
The existing endpoint test establishes the same stream seam but not those new
certificate-specific cases (`tests/comparison.rs:452–483`).

## Exploration trace and verdict basis

- **H1:** The current comparator already supplies composition algebra and bounded
  stream lifetime. **Confirmed:** signed occurrence cancellation and value
  ranking exist, and `admit_stream` owns maintenance custody to EOF/drop.
- **H2:** The public certificate inspector can be called unchanged by all
  consumers. **Refuted:** it re-enters host admission and checks standalone result
  availability, which differs from optional-history availability.
- **H3:** Existing release protection automatically retains optional history for
  future consumers. **Refuted:** complete-state inventory excludes optional
  changes. A required chain must be an explicit retained dependency.
- **H4:** The chain can replace R5 ordering and candidate logic. **Refuted:**
  accepted succession is independently retained, and R5 still needs complete
  old/new dependency expansion and recomputation.

**APPROVE** the shared-service direction and the smallest transient implementation
above. Intent and shape match R4; the user-value claim is supported by the code's
available seams, with performance unmeasured. Commitments are honored if the
listed admission, identity, fallback and lifetime rules hold. The design pays
down duplicated work and is partly subsumed by maintained/current facilities.
Confidence is high for the static local design and limited for runtime/resource
behavior until implementation qualification. Durable certificate adoption and
R5 integration remain explicit work, not implied by a successful chain stream.
