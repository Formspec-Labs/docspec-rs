# Continuous backfill: consistency and recovery review

Review date: 2026-10-10. Reviewed commit: `8566235f32abc90ea21cfbd4b75f610766cfdc2d`.

**Verdict: RECONSIDER the current fixed-capture service as the complete R3 solution. Keep its preparation, reuse, coverage, and guarded selection machinery. Add a durable transition that owns successive exact captures and a bounded admission fence, with complete accounting for previously accepted source requests.**

This is a static design review, not qualification. No source edits, builds, tests, benchmarks, or corpus reads were performed. Citations below refer to the reviewed commit. The separate change-chain implementation review was not read or changed. The Rust and semi-formal architecture-review skills informed this review. The research requirements were read locally; their linked practitioner corpus was deliberately not opened, so this review makes no new empirical claim from it.

## Findings

### F1 — BLOCKER: the current service cannot advance one backfill intent through moving inputs

Category: intent-vs-shape. Location: `src/engine/backfill.rs:510–626,1027–1072`.

An admitted backfill permanently captures its base manifest, source boundary, admission counter, and target binding. Promotion refuses any change in its source or source boundary and tells the caller to create another operation. This preserves safety and completed computation, but it does not implement the retained catch-up/cutover lifecycle required by R3. The top-level status correctly calls continuous-ingestion cutover unfinished (`docs/STATUS.md:5–12`; `docs/requirements/03-schema-backfill.md:39–47`).

**RESHAPE:** retain one immutable user intent and a small durable record of its exact preparation attempts. Each attempt binds a fresh complete source selection and uses an immutable child operation identity. The transition retries source movement within configured limits and closes through the common native selection. It must not mutate an existing `BackfillSpec.base`, `Operation.boundary`, readback, or request digest. Preparation attempts remain private until the winning cutover; replacing the selected dataset after every attempt is not a substitute for one complete activation.

### F2 — BLOCKER: a maximum admission number is not proof that all accepted work was resolved

Category: named-seam. Location: `src/engine.rs:1098–1150`; `src/engine/retention.rs:23–104`.

Source request admission increments `Authority.boundary` before external input copying and the durable admission phase (`src/engine.rs:801–875`). Two updates can be admitted against the same exact base. After one selects, the other's exact-base check refuses (`src/engine.rs:1945–1971,2026–2036`). Its earlier admission can now lie below the selected boundary. The existing abandoned-gap check skips every operation at or below that selected number, so it cannot establish that such an earlier request was selected or deliberately withdrawn. This is a limitation of reusing that helper for the new guarantee; the existing code does not claim it implements continuous cutover.

**RESHAPE:** before promotion, account for every source-admission identity through the fence, including unresolved identities below the selected boundary. A request is resolved only by a matching accepted selection on the current succession chain, or a matching explicit abandonment. Processing, readback, failure, staleness, a large counter, or a later full replacement are not implicit abandonment. Missing or inconsistent records refuse. Keep the check metadata-only and bounded. Do not copy request payloads into another job ledger.

The smallest complete fence policy may refuse new source submissions, drain already accepted work, and return resumable `pending_source` when a conflicting request needs explicit resolution. It must reopen ingestion at its declared deadline and retain candidate work. This satisfies a bounded fence; it does not promise automatic convergence for unresolved conflicting intent. Accepting new inputs during the fence, or automatically rebasing conflicting old requests, would require additional explicit intake/ordering semantics and is unnecessary for this policy.

### F3 — BLOCKER: target identity must survive legitimate refresh without accepting a competing replacement

Category: unstated-assumption. Location: `src/engine.rs:1440–1482`; `src/engine/backfill.rs:1061–1070`.

The current exact target binding check is correct within one capture. It is wrong as a literal invariant across an entire replacement backfill: normal source updates recompute existing groups and produce new bindings and tables while preserving their `GroupDefinition`. Conversely, checking only that definition would accept an intervening competing replacement, including A → B → A or a competing same-definition adoption.

**RESHAPE:** retain the initial expected target and prove its allowed lineage at each new capture. An addition requires the group to remain absent. A replacement allows the original target's qualified descendants through source refresh and compatible composition, while an explicit replacement of that target requires a new user intent. Each child then uses the exact target binding at its own capture. Reuse the accepted succession and existing binding-history semantics; do not introduce an independent mutable group head (`src/engine/history.rs:44–135`; `src/engine/backfill/verification.rs:448–516`).

Retain authored source-field meaning as well. Expressions already support stable-ID binding and reject unknown interpretation (`src/engine/backfill/expressions.rs:35–49,87–140`). A new capture must not reparse the same expression against a reused display name and silently change meaning. Use qualified field correspondence or refuse the incompatible source/schema transition.

### F4 — BLOCKER: fence ownership and all mutable eligibility must participate in the selecting commit

Category: named-seam. Location: `src/engine/history.rs:160–244`.

Existing selection already commits authority, release result, accepted succession, and optional applied-change evidence together. Corrections are checked from that same view. A controller that independently marks itself complete or drops its fence before this commit creates a lost-response or admission race. A timer alone does not fence a delayed worker (`docs/requirements/01-durable-storage.md:17–23`).

**RESHAPE:** extend the common selection assembly with narrowly typed transition records and guards. Check exact transition owner/generation, active fence, deadline policy, current source and boundary, expected target lineage and exact child target, active definition/eligibility/epoch, complete candidate coverage, corrections, and all live heads required by the cutover scope. Atomically select the result and active definitions, record the parent transition result, and close the fence. Native expected-state conflict must restart these decisions against a fresh view. It must not repeat qualified file computation.

For related inputs, `guard_ancestors` uses captured policy (`src/engine/refresh.rs:284–293`), whereas strict live-head comparison exists in `guard_inputs` under `RefreshPolicy::Latest` (`src/engine/refresh.rs:298–338`). Do not describe captured ancestor validation as R3's required live-head fence. Declare which input heads are load-bearing for the transition and check them under the corresponding dataset/family locks. R5's routine captured policy remains unchanged (`docs/requirements/05-dependent-refresh.md:27–39`).

### F5 — CONCERN: preserve the distinction between source and computed definitions

Category: debt-accretion. Location: `src/model.rs:865–900,1014–1030`.

`SchemaSpec` has expected and selected source writer definitions. `BackfillSpec` has a required source definition plus an independent `GroupDefinition`; current computed-only backfill selects that definition as part of the new dataset. Source updates inherit every selected computed definition and bind an exact base (`src/engine.rs:1440–1482,1945–1971`).

**KEEP** that distinction. A computed-only addition or replacement can atomically activate its new group definition without inventing an unrelated version of the source writer. Old source workers cannot select their earlier base; old competing backfills cannot select their earlier target. A transition that changes source schema or interpretation must explicitly select its new source definition with the result, as schema selection already does (`src/engine/history.rs:203–212`). Preserve original phase provenance when recording expensive old work; a retained phase completion is not permission for an obsolete worker to select a dataset (`src/engine.rs:1727–1742`).

## Artifact summary and decision frame

The problem is adding or improving fields while ingestion continues, reusing completed processing, and exposing only complete results with exact source correspondence. The beneficiary is the dataset operator and the reader who must distinguish successful null/empty output from unfinished or failed work. This is product behavior with necessary recovery support, not a new scheduling platform (`docs/requirements/03-schema-backfill.md:5–19,37–47`).

The current shape is an exact-base scalar backfill with material per-file reuse, independently retained preparation/validation/readback, and guarded composition. The accepted requirement adds catch-up, atomic definition activation, old-worker refusal, later-input preservation, and bounded cutover under sustained input. Existing status documents explicitly preserve that gap (`docs/STATUS.md:1748–1772,2092–2121,2167–2205,2712–2731`).

The review first tested the existing shape against its own requirements. No author proposal was supplied at assignment. During review, the implementation owner proposed a durable parent and bounded admission fence; the conclusions here include the failure cases that proposal must satisfy.

## Lineage and relationships

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| Accepted implementation plan | One dataset lifecycle authority; immutable phases; strict schema cutover preserves later input | `docs/PLAN.md:23–40,67–87,117–120` |
| R1 durable storage | Shared phase identities, protected outputs, stable retry, no second job ledger | `docs/requirements/01-durable-storage.md:17–42` |
| R3 schema/backfill | Normative catch-up and cutover behavior; task acceptance | `docs/requirements/03-schema-backfill.md:37–59` |
| R5 dependent refresh | Shares preparation and selection; captured promotion remains separate from strict cutover | `docs/requirements/05-dependent-refresh.md:27–39` |
| Research requirements | Qualified phase reuse and usable recovery are first-delivery requirements | `docs/requirements/RESEARCH.md:3–9,24–33` |
| Q2 and Q4 | Concurrent enrichments/source correction/compaction, and late dependency omission | `docs/requirements/07-operations-qualification.md:93–115` |
| Scoped corrections | Preserve original executions; same-file supplemental correspondence; unknown interpretation refuses | `docs/dependency-corrections.md:18–38,41–72` |
| Current implementation status | Implemented fixed-capture/recovery seams do not redefine completion | `docs/STATUS.md:5–20,1938–1944` |

| Component or seam | Owner | Existing dependency | Proposed use / missing edge |
| --- | --- | --- | --- |
| Request admission | R1 engine admission | Exact request, protected dependencies, lifecycle view | Add the new-source fence check at the guarded admission commit, not only at API entry; `src/engine.rs:990–1150` |
| Source processing | Existing build/update/refresh services | Retained inputs and exact base | Continue already admitted work while a fence blocks new submissions; `src/engine.rs:750–975` |
| Scalar preparation | R3 `prepare_group` | Exact source, field bindings, material definition, file completions | Reuse unchanged files across exact attempts; `src/engine/backfill/preparation.rs:67–113,147–208,280–449` |
| Native selection | R1 history/selection | Guarded lifecycle commit | Atomically close parent transition and fence with selected result; `src/engine/history.rs:160–244` |
| Accepted-source accounting | R1/R3 | Source admissions, explicit abandonment, accepted succession | New shared cutover check; existing gap helper alone is insufficient; `src/engine/retention.rs:23–104` |
| Target succession | R3/R4 | Accepted selections and computed binding lineage | Validate replacement intent over successive captures; `src/engine/history.rs:44–135`; `src/engine/backfill/verification.rs:448–516` |
| Protection and retirement | R6 shared owners | Atomic completion/protection bindings | Parent and child roots protect exactly referenced work, including retained readback; `src/engine/admission.rs:48–70`; `src/engine.rs:111–124` |
| Public plan/run/inspect/resume | R3 shared engine, thin CLI/HTTP | Existing services | Show parent intent, exact covered source, pending identities, fence status and reusable work; R3-T06 at `docs/requirements/03-schema-backfill.md:58` |

## Invariants and commitments

| Invariant | Status in current seams | Failure if weakened |
| --- | --- | --- |
| One native lifecycle selection defines the complete dataset | PRESERVED; `docs/PLAN.md:23–33`; `src/engine/history.rs:213–244` | Independent table heads expose mismatched source/group/schema states. |
| Operation identity binds immutable request and original result | PRESERVED; `src/engine/backfill.rs:525–568`; `src/engine.rs:1000–1007,1939–1943` | Reusing an ID for a later source makes retry response and provenance ambiguous. |
| Completed expensive work survives failed promotion | PRESERVED; material file keys and retained prepared/validation results at `src/engine/backfill.rs:1127–1131`; `src/engine/backfill/preparation.rs:89–111,334–449` | Every new source or lost response can restart the historical computation. |
| A finite cutover accounts for accepted input, not just selected maximum sequence | REQUIRED, not yet represented by the current gap helper; R3:45 and F2 | An earlier stale request disappears from the cutover's accounting. |
| Pending, failed, null, empty, and deleted remain distinct | RELIED-UPON; R3:9,39–45; current file failures and selected outputs | Coverage can be overstated or removed source rows can survive in enrichment. |
| Reuse preserves original execution and current material qualification | PRESERVED for the documented scalar case; `src/engine/backfill/preparation.rs:192–207,354–373`; `src/engine/corrections.rs:217–237` | A correction recorded after reuse lookup is ignored at selection. |
| Old actors cannot select after cutover | PRESERVED by exact base/definition/epoch/target guards; transition fence generation is NEWLY INTRODUCED | An expired fence owner or old writer can overwrite a newer selected definition. |
| Ordinary reads remain immutable-pin reads | PRESERVED; `docs/PLAN.md:44–65`; `src/engine/backfill.rs:153–174` | Introducing transition validation into every read adds changing authority and repeated history work to readers. |

Routine validation is not a deep independent coverage reconstruction. The current verifier checks metadata and schema, then returns when the requested scope is not deep (`src/engine/backfill/verification.rs:522–553`). Exact occurrence coverage and value reconstruction remain necessary qualification evidence; the transition must retain and reuse the actual declared check scope rather than relabel it.

## Smallest complete implementable shape

### 1. Retain the intent; capture immutable attempts

Store a typed transition request using the existing lifecycle table and stable operation ID. It binds dataset, initial selected version/manifest, expected source definition and eligibility/epoch, expected target meaning/lineage, requested group definition, field meaning, and explicit attempt/fence limits. A retry with changed intent conflicts. A same-intent retry returns the retained terminal result or resumes the durable state.

Keep one bounded current-progress record and immutable references to useful completed attempts. Do not append the entire attempt history to every progress update. Each child attempt binds an exact current base, exact target binding, source and related inputs, and immutable request. A new source or a changed correction qualification can require a new child ID without changing the material file reuse keys. Persist that child identity before its preparation so a crash does not invent a different retry identity.

Use `prepare_group` for complete target snapshots. It already resolves exact file completions before reading source payloads; a changed snapshot enumerates its present files, reuses matching files, computes changed/new files, and omits removed files (`src/engine/backfill/preparation.rs:147–208,280–296`). This is a complete comparison/reconstruction path allowed by R3:43. It is sufficient without a new R4 accelerator. Any later change-chain optimization must agree with this full target path. Do not union old outputs with only newly inserted inputs: replacements, removals, one-to-many shrink, and all-empty results must be represented.

### 2. Attempt ordinary strict cutover, then take a finite fence when needed

While ingestion proceeds, prepare against successive complete selections. A fresh source capture must preserve original target intent and authored field meaning. Compatible siblings can be composed into the latest parent without repeating target file work, following the current composition loop (`src/engine/backfill.rs:706–743,765–808`). A competing target replacement refuses the transition, rather than being recaptured as if the user requested overwriting it.

After the configured ordinary attempts or lag threshold, install a durable dataset admission fence in one guarded commit. Retain owner transition ID, unique generation, captured admission number, start/deadline policy, and state. Check that record in the same view that admits each new source request. A source request racing the fence either commits first and belongs to the drained set, or observes/conflicts with the fence and receives an explicit retryable refusal. An API-entry check alone is insufficient because dependency protection runs outside the dataset lock (`src/engine.rs:1036–1092`).

Already admitted operations and their missing input-transfer phases can resume under their original identities. The fence must not stop their computation/completion, and must not count an incomplete external-file reference as a retained accepted payload. Current `request/` admission and completed `admission` phase are separate facts (`src/engine.rs:801–875,1134–1150`). No new request rejected by the fence may increment the source counter or be reported accepted.

Do not hold a dataset mutex while waiting for workers or doing payload work. Waiting is bounded by the declared fence/attempt policy and returns durable progress. Resource refusal or unresolved input retains completed work and releases or expires the fence. A caller can resolve a stale conflicting request explicitly and resume; the transition must never silently abandon or rebase that caller's request.

### 3. Prove the drained set from exact identities

For each source-admission entry through the captured fence, validate dataset, operation ID, request digest, original admission revision, and its unique admission number. Include `Build`, `Update`, and `Refresh`; historical schema requests that consumed an admission number need their retained format semantics. Current schema/evidence/backfill composition requests do not consume new source numbers (`src/engine.rs:1098–1125`; `src/engine/retention.rs:75–84`). They still participate in definition, source, target, and correction guards.

A selected entry is accounted for only when its result identity and immutable `succession/<id>` belong to the currently accepted chain. Use retained selection metadata; historical payload retirement does not require reopening old values. Accepted succession is distinct from composition parentage, and the existing walker validates endpoints against the observed head (`src/engine/history.rs:44–135`). A matching explicit `Abandonment` must bind the exact admitted request/revision, have a later decision revision, and have no selected result (`src/engine/retention.rs:85–99,154–181`).

Check unresolved entries below the current selected boundary. `result/<id>` alone, an isolated branch, a processing completion, or readback is insufficient. A selected later full replacement may intentionally change old data, but its presence does not prove that a pending earlier request was accepted into history. Missing or duplicate sequence evidence is an error; do not infer completeness from equal counters or counts.

If an accepted request is failed, stale, or still running, report its identity and reason as pending. Reopen intake within the fence bound and leave the transition resumable. This is the honest terminal outcome of that attempt, not successful cutover. The successful case drains all accepted requests; consequently there is no accepted post-fence input requiring a new queue. Intake resumes under the selected definitions after success, or the old definitions after expiry/refusal.

### 4. Select exactly once; recover from the selection record

At final commit, use the fresh locked view to recheck the source selection, related required heads, target, complete candidate bindings, current dependency requirements, active definitions, eligibility/epoch, and drained admission set. Check fence owner and generation. A reopened or replaced fence invalidates a delayed worker even when the old worker's other pins still match. The deadline governs admission reopening; the native guarded state/generation check supplies exclusion. Document the local clock/restart policy rather than treating a timestamp by itself as a fencing token.

For the owned local host, the smaller clock policy is an engine-instance UUID plus a deadline measured from that instance's monotonic origin. Persist the owning instance and attempt generation with the deadline offset. A reopened host has a new instance UUID; an earlier instance's fence is expired and cannot authorize selection. Never interpret an old offset using a new monotonic origin. Clear or supersede the stale fence through the guarded lifecycle commit before admitting new work. This relies on the existing exclusive workspace ownership and worker-lifetime rules; it makes no remote lease claim. A monotonic deadline bounds the admission policy, not indefinitely stalled native I/O or a mutex held by a stalled commit, so qualification must record actual lock/commit latency and the declared failure limit.

Commit result/authority, accepted succession, the parent transition's exact winning result, and fence closure together using the existing lifecycle commit. The child result must be identifiable from the parent without relabeling the child's dataset or replacing its manifest. A lost reply reconciles this record before checking expired fences or starting another attempt. Selected definitions and completed parent identity cannot become separate authorities.

For computed-only transitions, the selected `ComputedGroup.definition` is the active computation definition and existing source definition stays unchanged. For source-schema transitions, extend the existing explicit expected/next writer-definition handling. Old source selection must fail through exact base/definition/epoch checks; old target selection must fail through exact target/transition checks. Retained old-phase acknowledgments may remain useful under their original provenance, subject to existing execution authority and abandonment guards.

### 5. Preserve corrections, protection, and observability

Every final selection calls `guard_corrections` on the same decision view. A correction after a child's immutable readback cannot mutate that readback's target: use a fresh qualified child association, reuse only eligible original file work, or refuse with the precise correction failure. The existing correction rules explicitly refuse unknown historical interpretation and rerunning a deficient definition after discovering its omission (`src/engine/backfill/preparation.rs:192–207`; `src/engine/corrections.rs:217–258`; `docs/dependency-corrections.md:25–38`).

Protect the parent, exact child inputs, reusable completions, and selected result through existing owner bindings. Fence expiry ends admission blocking, not retention protection or the identity of completed work. Plan/inspect/resume should report intent, covered exact source, selected versus prepared status, unresolved source identities, reuse/refusal reasons, and fence remaining/expired state. Avoid a second scheduler, a copied per-row progress ledger, or a new read-path dependency.

### Hard dependency order

1. Define and implement exact source-admission accounting, including holes below the selected boundary, matching abandonment, and accepted succession. Reuse this shared check in strict schema/backfill promotion. This is a safety prerequisite; completing it alone does not implement continuous ingestion.
2. Define immutable transition/child identities and allowed target lineage. Retain selected field meaning, correction requirements, exact child inputs, and recovery roots. These are prerequisites for safely advancing the capture.
3. Implement guarded new-admission fencing, bounded host-instance expiry/reopen, and already-admitted-worker continuation. Make parent completion and fence closure part of the same selecting commit before exposing the live service as complete.
4. Connect capture/prepare/catch-up/drain/select through public plan, run, inspect, and resume. Qualify the successful path and bounded completion-or-refusal behavior together, including lost replies and restart.

Independent fixture/oracle preparation and metadata-resource review can proceed alongside the first two items. The live service cannot safely bypass these dependencies by retrying the current fixed-capture API. This review specifies the complete required path; it does not claim that any of these additions is implemented.

## Recovery and qualification cases

The following are required tests and independent reconstruction cases, not results of this review.

| Ordered failure or race | Required observable outcome |
| --- | --- |
| Source admission pauses after protection; fence commits before request commit | The late new request refuses or conflicts without consuming an admission number. A request that committed first is included in the drain. |
| External request retained, input transfer incomplete, then worker loss | The request remains pending; promotion cannot count its locator as retained bytes or resolved work. Resume finishes the original transfer, or the caller explicitly resolves it. |
| Two updates admitted against one base; later admission selects first | Earlier unresolved request blocks cutover even below selected boundary. No automatic abandonment or hidden rebase. |
| Source insert/correction/reversion/removal/one-to-many shrink during successive captures | Final selected combined values, multiplicity, field interpretation, occurrence coverage, and evidence equal full processing of the exact winning inputs. |
| Two independent new groups backfill concurrently | Compatible sibling selection survives composition retry; expensive completed target work is reused. |
| Two replacements race, including same-definition replacement and A → B → A | One intent cannot recapture a competitor's target and overwrite it silently. |
| Source update refreshes the old target group during replacement backfill | Legitimate target lineage remains eligible; a literal initial binding comparison does not block ordinary catch-up. |
| Compaction changes physical files while logical source remains qualified | Preserve correspondence through supported logical identity; never infer it from equal rows or business keys. |
| Receipt-only selection changes during catch-up | Preserve the new evidence binding while reusing qualified values; exact latest composition appears in the selected result. |
| Source schema rename/name reuse or definition invalidation races a new capture | Preserve authored field meaning or refuse; never silently rebind expression text. |
| Scoped correction lands after reuse lookup or immutable readback | Same-view guard refuses stale adoption; a new child can supplement only adequate retained correspondence. Unknown facts remain unknown. |
| Fence timeout, cancellation, process kill, restart, delayed old controller | New intake reopens within the declared policy; old generation cannot promote; retained files and input records remain available. |
| Native selection succeeds, response/report fails | Same parent ID returns the exact selected result without new preparation, another cutover, or later source recapture. |
| Related required head or generation changes at commit | Strict cutover refuses/retries; ordinary captured R5 refresh semantics remain unchanged. |
| Capacity failure during drain, history check, or candidate assembly | Bounded refusal preserves work and ends admission blocking; no unbounded wait, fabricated coverage, or skipped input. |
| No source rows; every row removed; successful null/empty fields | Complete empty outputs remain valid; pending/failed outcomes never become successful empty results. |

Qualify pause and restart behavior with independent processes, using the actual local ownership and native commit implementation. The current workspace takes an exclusive host lock (`src/budget.rs:179–197`); this is not evidence of concurrent-host or remote catalog fencing. R3-T07/Q2/Q4 and complete-path backfill/update/resume measurements remain open until the new behavior is run and retained (`docs/requirements/03-schema-backfill.md:59–65`; `docs/requirements/07-operations-qualification.md:93–115`).

## User-value and counterfactual analysis

The proposed shape lets an operator request a field once, continue ingestion during historical work, reuse completed files as the source changes, and receive either one fully selected result or a precise resumable refusal. It pays down the current caller-managed sequence of new backfill IDs. Its necessary conceptual cost is one retained transition and a finite admission-fence state, both within the existing lifecycle authority. It adds no payload cache, generic workflow engine, or permanent intake queue.

Smaller alternatives do not deliver the same outcome. A client retry loop loses durable intent and target replacement protection. Holding the dataset mutex during preparation blocks accepted workers and lacks bounded restart behavior. Comparing only admission counters misses unresolved holes. Ignoring later admissions adopts R5 captured semantics and weakens R3. Bumping the source definition for a computed-only field introduces unrelated invalidation without additional protection. A durable intake queue is justified only if callers must receive acceptance during the fence or demand automatic conflict resolution; neither follows from the bounded-fence requirement.

The kill criterion is any selected transition whose source, group definition, coverage, evidence, accepted-input accounting, or fence generation differs from the atomic cutover decision. A second kill criterion is an intake pause that persists beyond the documented bound after timeout or recovery. A user-value failure is repeatedly recomputing qualified historical files after each source movement or requiring operators to invent new child identities after lost replies.

If the new transition were removed, current fixed-capture backfill would remain useful but the operator would again manage moving inputs and stale cutover manually. Existing scalar preparation, accepted succession, corrections, and selection partially subsume the work; they do not supply the finite transition or full admitted-request accounting. A future reviewer should reject a controller that accumulated its own queue, mutable latest pointer, payload copies, or independent cache instead of extending these existing seams.

## Final verdict

**RECONSIDER — implement the focused durable transition, exact target lineage, complete accepted-input accounting, and bounded guarded admission fence described above.**

- Intent versus current shape: **DIVERGES**. Fixed captures preserve safety but do not deliver continuous catch-up/cutover.
- User-value claim: **SUPPORTED** by R3 and retained research requirements; performance benefit remains unmeasured.
- Commitment status: existing immutable-phase and single-selection rules are **HONORED**; continuous cutover remains **UNIMPLEMENTED**.
- Conceptual debt delta: **PAYS DOWN** caller retry/identity work if confined to ordinary lifecycle records and shared helpers.
- Sibling subsumption: **PARTIAL**. Reuse preparation, correction checks, succession, protection, and selection.
- Confidence: **HIGH** in the static identity/race findings and the bounded-fence shape; **UNQUALIFIED** for implementation, timing bounds, process failure, scale, and remote execution.
