# R7 — Qualify and operate the complete greenfield lifecycle

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern scope. This plan supplies evidence and operating limits; it does not certify a runtime that has not been built.

## User outcome and requirements

“I can see which phase is progressing, what work a retry will reuse, what remains before publication, and whether the job fits its resource and freshness limits.”

Start the native Comments fixture and measurement design while architecture choices remain changeable. The existing implementation and earlier admission benchmarks may be comparison arms, but they do not constrain design or prove this lifecycle.

The [research acceptance map](RESEARCH.md) sets the first milestone: a recoverable fixed-schema Comments path from selected input through independent publication readback. Exercise phase reuse and late failures before broader incremental and schema features. These cases derive from historical practitioner incidents; do not label them current defects or claim the new architecture's performance from interview timings.

## Early decision and measurement work

Record actual consumers and required data/evidence formats before format freeze. Set workload distributions and product targets for acknowledgement delay, change-to-release freshness, backfill lag, recovery time and failure-domain loss tolerance, resource/cost limits, disclosure policy, and independent retention windows. Acknowledged work survives worker loss; broader disaster guarantees must be explicit.

Compare maintained catalog, table, query, and execution components against the required behavior before building equivalent infrastructure. Use bounded probes for durable commits/grouping, snapshot changes, stable-field evolution, enrichment composition, and disclosure layouts. Measure the first working source-update-to-durable-commit-to-independent-reader path as soon as available; extend it through the full lifecycle before production qualification. Label omitted stages unmeasured. A prototype or component win does not qualify the entire system.

Before selecting the engine or storage layout, run bounded probes of [process memory](#q1-process-memory), [concurrent field-group updates](#q2-concurrent-field-group-updates), and [native cleanup](#q3-native-cleanup-and-retained-data). Record the exact package/API versions, enabled features, configuration, and supported readers tested together. These probes inform selection; the owning tasks must still qualify their complete behavior before claiming support.

The fixture uses retained, digest-verified source assets or deterministic offline source objects. Keep source policy injectable: accepted revision ordering, duplicate/stale observations, correction precedence, explicit scoped deletion, missing source, withheld content, and evidence retention. Missing input does not imply deletion or prove source completeness.

Before an expensive phase, report known input sizes, estimated memory/scratch needs with uncertainty, configured engine limits, whole-process limits, and available shared execution capacity. Use maintained execution controls for admission/backpressure; do not add a second scheduler. Unknown capacity is not unlimited capacity. Waiting/refusal must preserve qualified phase results. A DuckDB or other engine limit is distinct from the process's total memory and from a measured allocation cause.

## Operations and recovery

One read-only status surface assembles catalog and release observations: accepted source versions, acquisition cutoff, selected schema/definition/output, exact processed input coverage, observed live input heads and lag, pending/failed backfill scope, committed operation and phase identities, acknowledgement state, pending groups if used, candidate/public release, and retention/recovery blockers. Report the actual source/subject, receipt/evidence, processor, schema, policy, decoder, validator, and runtime/package identities consumed; distinguish material reuse dependencies from full execution provenance.

Expose selection, restoration, processing, subject writing, receipt/evidence combination or transfer, validation, upload, public promotion, and readback as explicit phase boundaries where present. Emit start/end and sampled progress during necessary work: rows/bytes completed and known totals, elapsed time, I/O and request counters, whole-process resident memory, engine observations when available, and scratch/spill high-water use. Missing totals or metrics remain unknown. Progress instrumentation must not add a full-data scan or retain every row in telemetry. Distinguish measured duration and resource use from an unproven cause of delay or allocation failure.

For each phase, show pending/running/complete/failed/unknown status, durable completion evidence, planned reuse, the next required check/action, and why a retry would repeat work. Reconcile stale running records against durable results and available worker evidence; an old timestamp alone does not prove either failure or completion. Phase logging diagnoses repeated work, while R1's retained results and supported resume prevent it.

Distinguish a complete captured-version result with lag from incomplete processing. A missing watermark or denominator is unknown, not zero backlog. Timestamp external readback separately; no atomic status snapshot across unrelated providers is implied. Distinguish output/definition conflict, strict latest-input refusal, branch reconciliation, pending durability, lost response, exhausted budget, processing failure, corrupt evidence, unavailable retained bytes, and expired reader support.

Recovery commands resolve exact operation/phase identity and preconditions. Recover committed work without re-running it; resume matching verified continuations; retry uncertain uncommitted computation as a visible attempt; reconcile publication/removal intents; restore a qualified catalog/object point. One maintained command or documented sequence accepts retained phase results and explains the remaining work. Resume readback from a verified public write without republishing. Preserve failure and original-generation evidence. Never repair corruption by guessing absent metadata or silently reset resource budgets.

Production inspection, resume, and verification use the same maintained decoders and evidence-member resolution as ordinary readers. Keep structured validation outcomes and their durable completion separate from optional report presentation: a formatting exception preserves qualified data and can retry rendering alone. Independent tests use separate reference expectations so shared production helpers do not conceal a shared semantic error. R7-T02/T07 must provide coherent operating instructions; do not infer a missing backend feature merely because operators cannot discover an existing resume path.

A maintained executor owns scheduling/retries if selected; catalog commits determine dataset truth. Operators use the same semantics through APIs and CLI. No bespoke pass-status database, scheduler, or duplicate recovery journal is required.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R7-T01** | Phase vocabulary, progress and preflight; start with draft R1 interfaces | Define bounded live status, consumed identities, actual progress/resource counters, reuse/rerun reasons, next actions, and capacity preconditions. Distinguish engine and process limits, phase completion and job/release success, unknown metrics, stale running records, and protected history. No extra full scan or per-row telemetry. |
| **R7-T02** | Supported API/CLI resume and maintained decoding; T01 plus available feature slices | Expose inspect, recover, resume, reconcile, and restore through shared services and coherent instructions. Resume transfer→validation and publication→readback without repeating completed work. Mismatched dependencies refuse reuse; report-formatting failure preserves structured outcomes and bytes; uncertain attempts retain budgets/evidence. |
| **R7-T03** | Native Comments reference fixture; draft T01, parallel with design | Deliver a fixed-schema selected-input-to-readback fixture first, with duplicates/order, null/empty, receipts, refusals, exact identities, and late failure points. Reuse verified assets and independent expectations. Add joins, evolution, enrichments, branches, and policy changes as later slices. |
| **R7-T04** | Research-derived acceptance and lifecycle faults; T03, available R1/R2/minimum R6 first, then other features | Implement the lifecycle matrix and [adversarial qualification cases](#adversarial-qualification-cases) with standard test/reporting tools. Prove qualified unchanged and resumed phases do not repeat, invalidated checks do, and reporting failure preserves data. Retain logs, seeds, pins, counters, and failures; independent content/evidence results remain exact. |
| **R7-T05** | Deployment/provider recovery qualification; T04, R1/R2/R6 | Use independent processes/readers and selected real services to qualify concurrent writers, worker loss, definition fencing, public pointer recovery, deletion, and catalog/object restore. Include [Q6's delayed writers and unchanged pointer](#q6-delayed-authority-and-restore). Test doubles do not establish real provider guarantees. |
| **R7-T06** | Full-path cost and priority selection; starts with T03, then each delivered slice | Preregister targets/arms before results. Count selection/decoding/validation passes and measure cold admission/index construction, transfer, processing, recovery, retained phases, and independent reading. Compare unchanged and interrupted/resumed runs, not only warm uninterrupted work. Include [Q7's metadata growth](#q7-metadata-and-evidence-scaling) and [Q8's complete consumer path](#q8-complete-consumer-path). Prioritize confirmed duplicate work and report unresolved attribution. |
| **R7-T07** | Supported operations and evidence; draft resume instructions with T02, qualify claims after T04–T06 | Publish one coherent operating guide with actual consumed identities, supported resume inputs/commands, remaining work, capacity preconditions, and tested limits. Distinguish feature absence, discoverability gaps, fixed scope, and unverified recovery. Keep implementation/capacity/deployment/publication claims separate. |

## Required lifecycle matrix

| Scenario | Required result |
| --- | --- |
| Initial build, no-op, append, correction, removal, reversion | Exact values and membership; preserve meaningful types, null/missing/empty, multiplicity, ordering, source observations, and generation/reuse distinctions. |
| Qualified unchanged input repeated within/across operations | Shared exact selection, maintained decoding, and eligible same-scope verification avoid duplicate work; early completion lookup avoids full payload staging. Unknown identity or changed trust/validator scope still requires checks. Count actual passes and transfers. |
| Worker loss after completed restoration, matching, or receipt transfer | Independently retained phase/result and exact inputs survive; supported resume skips that completed phase and starts missing validation or later work. Partial files, a footer, or a stale running marker alone cannot authorize reuse. |
| Failure rendering a report after durable validation | Verified bytes and structured result remain protected; rendering can retry without download, processing, or validation repetition. Failure before validation completion cannot claim success. |
| Receipt-only, irrelevant implementation, or relevant policy/validator change | Evidence-only changes stay visible without unnecessary value recomputation. Reuse reflects adequate material dependencies; relevant changes rerun affected checks; broad package hashes and equal rows alone do not decide validity. |
| Maintained decoder and member-resolution edge cases | Production commands interpret valid evidence consistently, including nested diagnostics, missing fields, list/null distinctions, and explicit refusal. Independent reference expectations catch shared-helper mistakes. |
| Verified ordered inheritance versus changed population/order | Eligible ordered streaming preserves values, duplicates, positions and history without a mandatory full-payload index. Reordering, missing/added members or changed policy takes a qualified fallback or refuses; it never silently pairs unrelated receipts. |
| Public write complete, readback or its report interrupted | Reconcile exact public identity, preserve completion, and resume independent readback without another public write. Local validation never substitutes for the required public check. |
| Progress and resource pressure | Observe phase rows/bytes, timing, engine/process memory and scratch without another full scan; unknowns stay explicit. Capacity wait/refusal preserves completed work, and status explains reuse/remaining work rather than guessing from timestamps. |
| Schema rename/reorder/optional addition | Stable field meaning and historical reads; qualified metadata-only cases avoid unnecessary payload rewrites and never claim computed coverage. |
| Computed field or definition change | Reprocess affected dependencies, retain complete coverage and failures, and reuse unrelated values with evidence. |
| Independent enrichment and corrected input | Base data stays unchanged for enrichment-only edits; stale source bindings cannot match by business key alone; explicit correspondence can justify adoption. |
| Continuous ingestion and routine refresh | Complete captured results advance with honest lag and nonregressing accepted coverage; stable inputs eventually catch up. |
| Branch, backward vector, missing succession evidence | Automatic promotion refuses or performs an explicitly retained reconciliation; timestamp ordering is insufficient. |
| Strict schema cutover and late workers | Complete boundary plus atomic definition selection; no lost accepted updates; old-definition commits refuse. |
| Join moves, new matches, duplicate/null keys, hot groups | All affected old/new outputs and groups match independent full processing, with bounded contributor evidence. |
| Empty source/output and all-row removal | Complete empty outcomes remain meaningful and independently readable without placeholder rows. |
| Object write, commit, response, optional group-flush failures | Every acknowledged operation survives worker loss; durable unknown responses reconcile; uncovered operations remain pending. |
| Public pointer and deletion interruptions | Durable pre-effect intent protects objects and availability; exact readback/outcomes reconcile after restart. |
| Compaction, expired windows, and concurrent references | No logical change; new required roots defeat stale removal plans; nominal expiry cannot break retained dependencies. |
| Catalog restore or point-in-time recovery | Restore matches protected object versions and declared support; expired restore points cannot imply data availability. |
| Actual consumers and changed disclosure policy | Promised readers return exact pinned results; unauthorized historical/unselected bytes are never included in a new release. |
| Corruption or unavailable evidence | Refuse with diagnostics; only missing optional change accelerators permit a correct slower comparison. |

## Adversarial qualification cases

These cases refine the lifecycle matrix and belong to the existing tasks named below. R7-T04 coordinates their evidence; the owner tasks implement the behavior. Record the initial pins and protection, ordered actions and pause/crash points, expected outcomes, observed results, and applicable product targets. Preserve failed attempts and independent expectations. An implemented test or passing component probe alone does not establish complete lifecycle support. The [review record](https://github.com/Formspec-Labs/DocSpec/blob/main/docs/plans/native-dataset-lifecycle-2026-10-07/ADVERSARIAL-REVIEW.md) preserves their rationale.

### Q1: Process memory

**Owners:** R1-T01/T03 and R7-T01/T04/T06. Run a bounded probe before execution-component selection; qualify the complete processing path against declared limits.

Run a narrow branch that retains slices of wide text batches while receipt decoding, validation, compression, upload, and a deliberately slow writer overlap. Include an individual value or receipt larger than the normal batch allowance and an operation without a spill path. Record which allocation classes each admission limit controls, including retained backing buffers and allocations outside engine reservations.

**Pass:** Supported work completes within declared memory/scratch limits and progress targets. Excess work takes a supported wait, backpressure, or actionable refusal path that preserves completed phases. Distinguish logical queue bytes, shared backing allocations, engine reservations, and observed whole-process memory; a small reservation or batch estimate alone cannot establish the bound. Releasing the final references must permit the expected buffers to become reclaimable.

### Q2: Concurrent field-group updates

**Owners:** R1-T01, R3-T01/T07, R6-T02, and R7-T04/T06. Probe the pinned API and physical layout before storage/layout selection; qualify the full evolution behavior when delivered.

Ingest new comments while independent enrichments backfill different field groups, a source correction changes material inputs, and compaction changes physical row locations. Compare same-table column attachment, separate field groups, and equivalent wide-table processing. Retain conflict and retry traces along with the exact component combination.

**Pass:** Each selected result has exact values, source-occurrence correspondence, coverage, schema, and evidence. Conflict recovery reuses still-qualified expensive phases, explains invalidation, and meets foreground and backfill targets. Include composition, rewriting, validation, export, and maintenance costs in layout selection. A format's ability to store separate columns does not establish safe concurrent API behavior.

### Q3: Native cleanup and retained data

**Owners:** R1-T01/T05, R6-T01/T04/T05/T07, and R7-T04/T05. Probe retention integration before native storage selection; qualify every enabled deletion route before use.

Keep an old reader, a prepared but unselected snapshot, a completed phase whose parent failed, a public release, and a supported restore point active. Exercise supported native cleanup routes, including automatic cleanup when enabled. Remove protections separately, retain shared references, and race a new protection against removal preparation.

**Pass:** Every promised version and phase remains readable; native cleanup honors the same exact-object protection and removal decisions as DocSpec. Removing one owner preserves shared bytes. After the final applicable protection ends, fresh eligibility and removal intent permit exact reclamation with truthful availability. Disable any cleanup route that cannot honor these rules; reject a component combination whose unavoidable cleanup bypasses them.

### Q4: Discovered dependency omission

**Owners:** R3-T01/T07 and R7-T04. Include the semantic case in foundational dependency design, then test the actual reuse and commit path.

A classification declared text as its only material input. Pause a new adoption after reuse lookup, record that language was also material, then resume the pending adoption with a source whose language changed. Exercise adequate supplemental evidence, unknown historical input, and unaffected results separately.

**Pass:** Adoption cannot rely on the stale eligibility decision or ignore the recorded omission. Preserve the original execution, generation, and dependency declaration. Adequate retained evidence may qualify a new association with an honest supplemental declaration; unknown historical input remains unknown and requires conservative revalidation or recomputation. Unaffected qualified work remains reusable.

### Q5: Related input consistency

**Owners:** R2-T01, R5-T01/T05, and R7-T04. Establish the source relationship in the fixture before qualifying refresh and publication.

A producer declares Comments C2 and Dockets D2 as one related generation. Starting from C1/D1, attempt selection and publication of C2/D1, then C2/D2. Repeat with explicitly independent-source rules. Each source version individually follows accepted history, so nonregression alone cannot distinguish the cases.

**Pass:** Under the coupled-source rule, refuse C2/D1 and accept C2/D2 when all other checks pass. Under independent-source rules, allow an admissible mixed selection and report its exact pins. Apply the same declared relationship to output/coverage selection and release eligibility. Full reconstruction of an inadmissible combination cannot make it eligible.

### Q6: Delayed authority and restore

**Owners:** R1-T04/T06, R2-T04, R6-T05, and R7-T05. Qualify the selected provider and supported handoff/restore procedure with independent processes.

Pause publisher A after durable intent under authority epoch e1. Hand authority to B/e2 while leaving the public pointer unchanged, then resume A with its still-matching expected-pointer token. Let B publish, lose its response before reconciliation, and resume. Separately restore an advertised catalog point while an old publisher or deleter remains alive; deliver its delayed requests and responses after recovery.

**Pass:** Storage enforces current authority even when A's pointer token still matches. A cannot relabel its old attempt with e2. Reconcile B's exact effect and resume readback without repeating a confirmed write. Recovery fences old destructive actors before serving or resuming effects, reconciles pending intents, and preserves the declared pointer and object-availability guarantees. Restored metadata cannot promise deleted bytes. Catalog lease checks or test doubles alone do not qualify the external write boundary.

### Q7: Metadata and evidence scaling

**Owners:** R2-T06, R4-T06, R6-T07, and R7-T06. Include these axes in layout, lookup, and maintenance qualification.

Hold logical data and update size fixed while independently increasing fragment count, retained versions, retained phase results, and evidence-chain depth. Measure opening a pinned result, reuse lookup, commit, explanation, retention preview, and retirement. Record metadata bytes and requests as well as payload work, memory, temporary space, and retained storage.

**Pass:** Each operation meets its preregistered product target without losing exact content, evidence, or retention meaning. Explain protected bytes that cannot yet be reclaimed. Include index/compaction construction, verification, and ongoing maintenance in any claimed benefit. If a target fails, reshape the measured dominant stage before claiming support at that scale.

### Q8: Complete consumer path

**Owners:** R2-T01/T05/T06 and R7-T06. Define promised consumers before format freeze and qualify each supported distribution.

Keep a reader on an old release pin while publishing corrections, deletions, and evidence-only changes. Follow the new release through any required flat export or search distribution, transfer, and independent readback. Include cold admission and interrupted/resumed publication.

**Pass:** Each read returns one consistent data/schema/evidence selection through its promised availability window. New distributions respect disclosure policy and exact release membership. Measure the entire update-to-consumer path, including export, transfer, independent verification, and retries. Native update speed alone cannot pass a target that includes consumer delivery; any omitted consumer stage remains explicitly unqualified.

## Performance acceptance

Use equivalent native inputs and an independent full-reconstruction oracle. Hold changed rows and fields fixed while independently increasing population, row width, and update history. Include skew, hot groups, long change chains, wide text, nested attachments, heavy deletes, no-op, first build, cold/warm caches and indexes, receipt-only changes, unrelated/relevant dependency changes, and sustained ingestion. Compare uninterrupted, unchanged-repeat, and interrupted/resumed paths at each expensive phase boundary. Interleave repeated arms; retain environment deviations and all failures.

Measure change-to-independent-verified-release latency, durable acknowledgement, throughput/backlog, restore time, selection/decoding/validation pass counts, scans, restored/rewritten/transferred/uploaded bytes, requests, catalog/manifest growth, verification, whole-process and engine memory, scratch/spill, and retained/high-water storage. Separate grouping/capacity wait, restoration, compute, subject writing, receipt combination/transfer, validation, admission/commit, backup/recovery, disclosure materialization, and consumer read cost. Include index construction, cold admission/download, maintenance/invalidation, and retry costs. Attribute combined-stage costs only as far as measurements support; historical interview timings are case evidence, not a speedup baseline.

Emit counters during the necessary operation and collect bounded process observations; do not run another full scan solely to populate progress reports. Keep independent validation cost visible rather than deleting its checks as apparent duplication. A proposed optimization must preserve exact content and evidence, reduce the measured repeated work or total cost it targets, and disclose any shifted storage/recovery cost. Evaluate compaction and indexes over their expected useful interval.

Correctness is exact. Performance gates come from preregistered product targets, not inherited relative percentages. A delta-write or change-comparison improvement does not qualify the complete path. If fixed-size changes become unaffordable as data/history grows, reshape the measured dominant stage before claiming capacity.

The lifecycle comparison qualifies the assembled design. A claim about Rust-specific gains requires a separate comparison holding algorithms, layout, I/O, validation, resource limits, and work performed equivalent. Otherwise attribute observed gains to the tested combination and keep language-specific attribution unresolved.

## Remaining product decisions

Actual consumers, operating targets, failure domains, disclosure, and retention durations remain explicit inputs. Draft fixtures and interfaces may proceed without invented numbers. Consumer requirements precede format freeze; relevant targets and probe evidence precede component/mechanism selection; full qualification precedes production claims.

General incremental SQL, automatic failover, universal indexes, and orphan sweeping require a demonstrated need and explicit scope decision. Existing libraries or services are candidates, not mandatory dependencies.
