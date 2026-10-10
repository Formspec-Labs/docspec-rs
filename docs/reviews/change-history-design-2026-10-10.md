# Forward change-history design review

**Decision: RECONSIDER the count-only comparison surface; keep the retained-write and accepted-history foundations.** Complete R4 by composing a streamed change relation with exact endpoints, declared identity, retained addresses, conservative impact, and explicit path/fallback metadata. A shortcut that returns faster counts is useful but does not finish this scope.

Static review of `49f05943649c8b563bc451b65c8d45e5ecdf704e`, 2026-10-10. No Rust edits, builds, tests, formatting, benchmarks, or corpus payload reads were performed. No other review conclusions were read, including `change-chain-implementation-review.md`. Code citations describe the reviewed baseline, not concurrent implementation changes. Performance conclusions below concern work performed, not measured throughput.

Citation shorthand: R1, R3, R4, R6, and R7 refer to their numbered files in `docs/requirements/`, listed in the lineage table. `engine.rs`, `history.rs`, and `worker.rs` refer to `src/engine.rs`, `src/engine/history.rs`, and `src/engine/worker.rs`. Numbers after a colon are source lines.

## Findings

### F1 — Consumer-visible rows and their proof are missing

**BLOCKER · intent-vs-shape · RESHAPE.** `Engine::compare` reduces two complete value relations to added/removed counts. `Comparison` carries neither endpoints, addresses, path, fallback reason, identity semantics, nor rows (`src/engine.rs:584-727`; `src/model.rs:689-696`). The CLI prints that JSON and HTTP has no comparison or changes route (`src/main.rs:670-682`; `src/http.rs:31-118`). R4 explicitly requires the relation and public result, not only a count (`docs/requirements/04-change-tracking.md:9-23`).

Keep the full multiset algorithm as the value oracle. Make counts and streaming consumers use one comparison service. Its bounded descriptor must name the exact before/after `VersionRef`, requested group/change kind, schema and key field IDs, identity rule, completeness/trust scope, chosen path, and any fallback reason. Return a typed stream containing retained before/after membership addresses and logical keys, preserving duplicate members. A two-sided relation is sufficient; do not arbitrarily pair rows when a replacement key has several old or new members. Schema/definition/coverage/evidence changes must remain separately observable even when value counts are zero.

### F2 — Existing removed rows do not always contain original occurrence addresses

**BLOCKER · unstated-assumption · RESHAPE.** Ordinary updates add stored source origins only when receipts exist (`src/engine/write.rs:198-218`). The same condition applies to join/aggregate replacement preparation (`src/engine/evidence.rs:105-146`). `replace_keys` selects the native schema and writes removed rows to new files (`src/engine/write.rs:487-501,522-527,622-627`). Without stored origins, reading those files as occurrences identifies their new change-file positions, not their original positions in the before endpoint. The verifier explicitly permits a legacy value-only check (`src/engine/changes.rs:128-139`).

Declare the proof's identity mode. For an occurrence-complete accelerated path, retain origins during applied writes independently of receipt presence, reusing `source_with_origins`, native origin materialization, and existing readers (`src/engine/evidence.rs:150-184`; `src/storage/occurrences.rs:174-206`). Do not invent old occurrence identities from equal values. Existing value-only history may support a clearly labeled value-reference result, or require the full endpoint path for requested occurrence semantics. New reference schemas must distinguish a retained value-row address from a stable source occurrence address. A new descriptor alone cannot retroactively establish the latter.

### F3 — Routine completion is not a portable completeness certificate

**BLOCKER · named-seam · RESHAPE.** `ChangePin` binds the base version/manifest, native endpoint metadata, key IDs, and removed/added tables, but has no format, comparison identity, completeness rule, or validator identity (`src/model.rs:994-1004`). `ReleasePin.verified` explicitly means routine validation/readback, not every payload value (`src/model.rs:168-175`). Routine production reuses validation phases and does not execute the deep membership/reconstruction query (`src/engine.rs:1411-1453`; `src/engine/changes.rs:165-201`). Treating every serialized `verified` pin as a complete portable certificate would overstate that evidence.

Version the narrow applied-write descriptor and identify the qualified producer/validation rule and exact retained delta identities. Local acceleration may rely on a descriptor produced by the existing validated write path and co-committed selected result; it need not re-prove endpoint membership with full scans on every read. Admission of an external or legacy descriptor must establish that trust separately, or use full comparison. Invalid present metadata, conflicting immutable identities, truncated present rows, or discontinuity must refuse. A recognized absence or unsupported acceleration rule permits fallback. Never catch every verification error as “history unavailable.” These boundaries follow R4-T01/T03 and R1's same-scope reuse rules (`docs/requirements/04-change-tracking.md:13-17,33-35,48`; `docs/requirements/01-durable-storage.md:31-40`).

### F4 — Optional history cannot currently expire independently of retained state

**BLOCKER for R4-T05 completion · commitment-violation · RESHAPE.** The retention inventory expands every `ChangePin` into its base release, before/after native metadata, and both change tables. Every retained dataset with changes invokes that expansion; receipt changes do too (`src/engine/retention/inventory.rs:969-1005`). Available retirement scopes do not distinguish optional change acceleration (`src/model.rs:546-555`). Therefore simply removing those files, or weakening `changes()` globally, is not a valid optional-history retirement implementation.

Make optional acceleration availability explicit through the existing retention lifecycle, with exact immutable descriptor identity and fresh eligibility. Separate the optional accelerator root from independently required endpoint, phase, input, receipt, consumer, and restore roots; preserve legacy unknown protection conservatively. Required consumers retain their chosen descriptor and referenced rows. A retired/missing optional accelerator changes comparison to a reported full path while endpoint reads remain valid. Retain the small ordering receipts independently. This is required by R4-T05 and R6, not a reason to reduce R4 completion (`docs/requirements/04-change-tracking.md:25-27,37`; `docs/requirements/06-maintenance-retention.md:9,23-31`).

### F5 — Accepted order and a usable change edge are different facts

**CONCERN · named-seam · KEEP ordering; RESHAPE traversal reuse.** `follows` correctly uses a captured `SnapshotRows`, validates co-committed selection receipts, walks from the current accepted head, and checks decreasing revisions and exact manifests (`src/engine/history.rs:44-138,321-354`). It returns only ordering evidence. Selection deliberately permits composition-only advances, and its actual predecessor can differ from a previously captured operation base (`src/engine/history.rs:7-11,180-202`; `docs/STATUS.md:1810-1832`). A valid succession step is therefore not proof that `DatasetPin.changes` describes that exact predecessor-to-child transition.

Reuse one bounded accepted-history walker/context for both services; keep `prove_succession`'s strict R5 behavior. For comparison, classify reverse/unrelated/unavailable optional history as explicit full-path reasons without relaxing malformed-present-history refusal. On each forward step, validate the requested group's exact transition. Skip a step only when its logical table identity is unchanged, while retaining schema/definition/evidence/coverage effects. A nonempty descriptor must bind the step's actual relevant base/result identities, not merely share an operation ID. An unsupported composition or schema mapping takes a full path. Do not infer order from `parent_manifest`, admission numbers, wall time, or equal rows.

### F6 — Existing R5 discovery must eventually consume the shared result

**CONCERN · user-value · RESHAPE.** Incremental join refresh independently calls `changed_keys` for each input. That service compares native file inventories and reads keys from changed files rather than consuming retained R4 applied writes (`src/engine/incremental.rs:134-185,315-405`). The R4 requirement explicitly names shared comparison consumers and the R1 exact-input context (`docs/requirements/04-change-tracking.md:17,23`; `docs/requirements/01-durable-storage.md:38`). Improving only the CLI comparison cannot establish improved dependent refresh.

Expose affected keys from the same comparison relation so refresh can use a qualified chain and preserve its existing complete old/new-key affected-group logic. Retain the present file-discovery/full path as a reported unsupported-history fallback. This is an integration boundary, not permission to claim delta-proportional writes: current replacement still probes source files and rewrites affected files (`src/engine/write.rs:451-508`; `docs/requirements/04-change-tracking.md:42`). Measure discovery and writes separately.

## Artifact summary

The reviewed decision is R4-T04 forward composition, constrained by R4-T01/T03/T05 and R6 rather than a proposed replacement storage architecture. Its product outcome is to tell a consumer exactly which members, values, evidence, and material field groups changed between two immutable selections without repeatedly comparing entire endpoints (`docs/requirements/04-change-tracking.md:5-25,33-38`).

The current implementation supplies suitable foundations: applied writes already retain typed removed/added rows from the same operation; selection commits its result and succession link together; Arrow/DataFusion readers and bounded IPC streaming already exist (`src/engine/write.rs:345-379,535-589`; `src/engine/history.rs:220-234`; `src/engine/read.rs:13-30`; `src/http/ipc.rs:11-85`). The remaining work crosses the model, writer proof, comparison service, retention inventory, CLI/HTTP, and refresh-consumer seams. It is product functionality supported by proof infrastructure. It does not justify a new event store, durable index, backend, global cache, or scheduler (`docs/PLAN.md:11-17`; `docs/requirements/04-change-tracking.md:13`).

## Lineage and relationships

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| Accepted implementation plan | Iceberg-only engine, shared services, bounded metadata and bulk streams | `docs/PLAN.md:11-40` |
| R1 phase and selected-input rules | Reuse exact qualified work without repeating payload admission | `docs/requirements/01-durable-storage.md:27-40` |
| R3 identity rules | Separates keys, occurrences, values, schema, definition, and evidence | `docs/requirements/03-schema-backfill.md:9-35` |
| R4 design and task rows | Declares the exact change/comparison/retention seams | `docs/requirements/04-change-tracking.md:9-38` |
| R6 retention and maintenance | Different lifetimes for content, recovery, ordering, and acceleration | `docs/requirements/06-maintenance-retention.md:9-31` |
| Current status | Explicitly leaves composition, lineage certificates, and unavailable-history fallback unfinished | `docs/STATUS.md:1488` |
| R7 qualification | Exact identity and evidence; corruption refusal; growth axes and complete consumer path | `docs/requirements/07-operations-qualification.md:55-79,133-157` |

`git log` places the R4 requirement in the initial retained foundation (`3bd6445`); subsequent implementation history includes ordering/content lifetime separation (`4fd1ca6`) and receipt-only updates (`ce3ad6b`). These are lineage pointers, not substitutes for the normative requirements above. The practitioner research is used only through the repository's explicit requirements; incident payloads and linked prior review conclusions were outside this static review.

| Component/seam | Owner and dependencies | Consumer | Named? |
| --- | --- | --- | --- |
| Applied write → typed changes | Native replacement service; existing source/join/summary/receipt paths | Comparison proof | Yes: R4-T02; `src/engine/write.rs:383-634` |
| Selected result → accepted succession | Lifecycle selection; captured catalog rows | R5 ordering and R4 path discovery | Yes: `src/engine/history.rs:160-234` |
| Retained changes → public relation/certificate | R4 service over exact selected inputs | CLI, HTTP, refresh | Required by R4; missing public shape in `src/model.rs:689-696` |
| Descriptor → physical protection | R6 inventory and exact retirement/removal | Active streams and retained consumers | Existing inventory; optional distinction missing, F4 |
| Batches → consumer bytes | Engine owned stream and HTTP IPC | Native/HTTP readers | Yes: `src/engine/worker.rs:159-200`; `src/http.rs:629-663` |

## Invariants and commitments

| Invariant | Status in proposed bounded composition | Failure if broken |
| --- | --- | --- |
| Catalog selection establishes the exact result; table heads do not | RELIED-UPON: bind both endpoints and every selected edge; `docs/PLAN.md:24-40` | Compares an unselected or mixed result |
| Value equality does not establish occurrence/evidence equality | RELIED-UPON, currently absent from count-only surface; R3:15-19 and F1/F2 | Reversion or receipt change disappears |
| Source order is independent of optional changes | PRESERVE existing `follows` semantics; R4:27; `history.rs:192-195,228-234` | Removing an accelerator breaks nonregression checks |
| Invalid present evidence refuses | REQUIRED boundary, not a generic fallback; R4:23 | Corruption appears as a valid empty or full result |
| Routine reads reuse committed qualification | PRESERVE with versioned writer proof; R1:38-40; `engine.rs:2520-2530` | Every small comparison repeats endpoint scans |
| Optional acceleration expires without breaking readable states | NOT YET IMPLEMENTED independently, F4; R4:25 and R6:23-31 | Cleanup either cannot reclaim history or deletes required evidence |
| Stream admission lasts through EOF/drop | REUSE existing ownership; `worker.rs:175-200` | Deletion races with an in-flight comparison |
| Composition is bounded in metadata, payload memory, and scratch | REQUIRED for long chains; R4:21,46 | Long history creates an unbounded row map or plan |

## User-value analysis and safe implementation boundary

Keep a small service over existing native relations. The following is a proposed implementation shape, not a claim that it is already implemented:

1. **Resolve once.** Admit the request and both exact endpoints, capture one lifecycle snapshot, and keep its bounded row handle. Reuse the requested native schema and selected references across eligibility, composition, and output. Compare trusted logical table identities before opening endpoint values. A same-table fast result may still carry changed evidence, definition, schema, or coverage metadata (`src/engine/read.rs:53-104`; `src/engine/history.rs:53-60`; `src/model.rs:80-89,141-165`).
2. **Select one honest path.** Record `unchanged`, retained forward composition, or full comparison plus a specific reason. A full control bypasses acceleration. Missing optional history, unsupported descriptor/identity/schema handling, reverse direction, or unrelated states use full comparison; malformed available evidence refuses. Fail capacity explicitly rather than returning partial rows as complete. Schema differences need explicit typed before/after handling or a precise unsupported result, never a zero count (`R4:21-23,48`; existing schema mismatch currently returns unknown counts at `src/engine.rs:641-658`).
3. **Compose complete key groups.** Treat each write's removed and added sides as multisets. For each logical replacement key, preserve the earliest old members and latest new members. Verify that each intervening add side equals the next remove side under the declared identity and schema interpretation, including empty groups and multiplicity. Use maintained DataFusion relational operators and its shared memory/scratch budget; spill/partition long chains or take an explicit supported fallback before exceeding bounds. Do not collect all row payloads into a Rust map. Reuse the existing signed-count approach where multiset comparison is needed; the baseline already documents why its maintained `EXCEPT ALL` path was unsuitable (`src/engine.rs:690-700`).
4. **Preserve all meanings.** Report inserts, removals, replacements, and one-to-many shrinkage as exact endpoint memberships. A value reversion can have zero net value changes while old/new occurrences remain distinct. An insert followed by removal cancels only under the chosen identity rule. Union changed field groups conservatively; unknown precision broadens impact. Keep evidence-only changes from `EvidenceResult.changes`, contributor evidence, computed definitions, schema rules, and coverage as distinct channels/effects (`src/evidence.rs:13-46`; `src/model.rs:150-165,854-868`). A changed contributor/definition must not disappear because source values compare equal. Where no applied evidence delta exists, use an explicit full evidence comparison or conservative affected scope.
5. **Deliver the rows.** Return the descriptor with a protected typed stream. Reuse `admit_stream` lifetime ownership and IPC encoding; extend the helper minimally if the descriptor and stream need to be returned together. CLI output may be Arrow IPC with bounded JSON status; HTTP should use the same service and expose endpoint/path metadata without buffering bulk rows. Success/completeness is established only after successful EOF, not after response headers (`src/engine/worker.rs:159-200`; `src/http/ipc.rs:42-85`; `src/storage/occurrences.rs:49-53`). Do not return an unguarded stream from an `admit_work` scope whose maintenance guard has ended.
6. **Protect precisely.** Use existing phase/completion and exact root mechanisms if a composed native result is retained for recovery or a consumer certificate. A live stream needs lifetime protection; a durable consumer promise needs durable roots. Resolve optional history retirement through F4. Do not manufacture a new job ledger merely to serve transient comparisons (`R1:29,36`; R6:25-31).

This pays down duplicated comparison/discovery logic and avoids endpoint payload work for eligible small changes. It still reads bounded history metadata and the selected change rows; native write discovery can still scale with file population. A static review cannot claim a latency or memory improvement. The value claim fails if the real consumer still reconstructs keys from endpoint scans, if a relation hides receipt/occurrence changes, or if history growth exceeds declared budgets (`R4:42-48`; R7:133-157).

Required implementation evidence is exact agreement with independent full reconstruction for inserts, deletes, duplicate key groups, one-to-many shrinkage, no-op, multi-step reversion, changed occurrences with equal values, receipt-only changes, schema/backfill/definition/coverage-only changes, and compaction. Also qualify mismatched descriptors, altered/truncated rows, discontinuity, missing/retired optional history, reverse/unrelated versions, stream cancellation, retention during a stream, and a retained consumer blocking cleanup. Measure payload and metadata reads, decode/verification passes, write overhead, history growth, memory, spill, and retries. This preserves R4-T01–T06 and R7; it does not substitute a source-count fixture for full completion (`R4:33-48`; `R7:55-79,133-157`).

## Counterfactual analysis

- **Kill criterion:** a qualified chain disagrees with the full value/occurrence/evidence oracle, drops a member or effect, or requires whole-endpoint scans on every eligible small comparison. That invalidates the accelerated rule; retain the correct full service while repairing it (R4:21-23,48).
- **Counter-decision:** always compare complete endpoints. This preserves an independent oracle and simplifies acceleration trust, but fails the explicit small-update outcome and repeats population-scale work (`R4:7`; `src/engine.rs:660-703`).
- **Removal probe:** removing the proposed accelerator leaves full comparison usable; removing required endpoint/evidence roots breaks consumers. Removing succession breaks R5 ordering even when endpoint values remain equal (`history.rs:7-15`; R4:25-27). Keep those dependencies distinct.
- **Sibling subsumption:** existing `ChangePin`, native replacement, succession traversal, readers, worker admission, and IPC partially subsume the proposal. They should be extended, not copied. No existing public relation/certificate or independent optional-history policy supplies the complete requested behavior (F1–F4).
- **Future critic:** the likely defect is calling a count shortcut “R4 complete,” relying on `verified` without stated trust, or making optional acceleration permanent through recursive result roots. The explicit public result and qualification matrix make those errors observable.

## Verdict

**RECONSIDER: implement the bounded public relation and descriptor in the existing comparison service, preserve actual occurrence/value distinctions at write time, and give optional history an explicit retention/fallback boundary.**

- Intent versus current shape: **DIVERGES**; the count-only API cannot deliver exact changed members and references (F1).
- User-value claim: **SUPPORTED** by the retained applied-write and streaming primitives, conditional on the stated implementation and qualification (F1–F6).
- Commitment status: **DRIFTING if reduced to counts**; preserve the full R4 requirements and independent R6 lifetimes.
- Conceptual debt delta: **PAYS DOWN** when consumers share the existing comparison/read context; a parallel event store/index would add unsupported debt.
- Sibling subsumption: **PARTIAL**; reuse the concrete foundations above.
- Confidence: **HIGH** for the traced baseline and missing seams; runtime correctness, performance, and cross-workspace trust remain untested in this static review.
