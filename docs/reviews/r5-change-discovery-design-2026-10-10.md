# R5 shared change discovery: architecture review

**Verdict: APPROVE the bounded local design below.** Share R4's certificate
qualification with R5, then read old and new keys from the qualified applied
changes. Keep the existing complete affected-group recomputation. Do not add a
durable chain-adoption record for work consumed entirely within the current
maintenance admission.

This is a static design review against `e7078a7`, October 10, 2026. No builds,
tests, benchmarks, or source changes were performed. It does not qualify the
proposed implementation or remote execution. The reviewer read the requirements,
current consumers, retention and worker paths, and the pinned maintained APIs;
earlier R5 review conclusions were excluded. Concurrent unrelated working-tree
edits are outside this review.

## Findings and decisions

1. **CONCERN — reshape the shared boundary before normalization.** R4's public
   value comparison is not the correct input to affected-key discovery. An update
   from `(docket=A, text=old)` to `(docket=A, text=new)` must keep A affected even
   though the key values cancel. Equal-value replacement with a different source
   occurrence also changes join tuples and contributor evidence. Share certificate
   admission and raw applied sides, then project keys; do not compare a key-only
   value relation or drop net cancellations. R5 explicitly permits conservative
   supersets (`docs/requirements/05-dependent-refresh.md:19,23,47`). Current
   discovery preserves this by reading both removed and added files
   (`src/engine/incremental.rs:415–447`), and summary evidence deliberately keeps
   replaced groups even when aggregate values are equal
   (`src/engine/aggregation.rs:313–320`).

2. **OBSERVATION — keep chain retention transient for this local consumer.** The
   whole refresh is admitted once through `run_internal` and keeps its worker's
   maintenance read guard until completion (`src/engine.rs:701–725`,
   `src/engine/worker.rs:111–155`). Certificate retirement and physical removal
   require the corresponding exclusive admission
   (`src/engine/retention/retirement.rs:501–509`,
   `src/engine/retention/removal.rs:257,272,308`). Local process ownership is an
   OS-held exclusive lock (`src/budget.rs:173–197`), including quarantine after
   uncertain private-worker termination (`src/engine/worker.rs:56–72`). This
   proves a local lifetime boundary, not a distributed lease.

3. **CONCERN — keep invalid history distinct from unavailable acceleration.**
   Missing or explicitly retired certificates may use the existing conservative
   endpoint-file path. A present contradictory certificate, changed descriptor,
   inconsistent predecessor, or malformed delta must refuse. Reuse the existing
   checks; do not catch all discovery errors and silently fall back. Current local
   checks distinguish these cases (`src/engine/changes/certificates.rs:127–179`,
   `src/engine/comparison/chain.rs:72–147`). Failed reads must not become empty
   scope (`docs/requirements/04-change-tracking.md:23`,
   `docs/requirements/05-dependent-refresh.md:37`).

4. **OBSERVATION — retain completed processing, not its optional accelerator.**
   Retry already resolves completed processing before input loading or discovery
   (`src/engine.rs:761–799`). Its own output, applied-change tables, base manifest,
   coverage and owner protections are committed independently
   (`src/engine.rs:923–961`). Adding chain objects to that recovery dependency set
   would keep optional history alive after it stopped serving recovery, increasing
   retained disk without improving the result.

## Artifact, lineage, and relationships

The decision is a product-performance change: smaller source corrections should
read fewer key columns while producing identical joins, summaries and evidence.
It refines R5-T02/T03 using the local R4 capability; it does not replace R5 source
ordering, final selection, evidence preparation, or R6 protection.

| Source | Constraint inherited | Current seam |
| --- | --- | --- |
| `docs/PLAN.md:69–86` | Reuse completed work; old/new keys; exact captured coverage | `prepare_source`, `process_incremental`, `select_refresh` |
| `docs/requirements/04-change-tracking.md:15–27,35–37` | Local authority, continuity, optional history, separate succession | `local_certificate`, current `forward_changes` |
| `docs/requirements/05-dependent-refresh.md:13–39` | Complete conservative affected scope; evidence changes; recoverable processing | `changed_keys`, join/summary processors, ordinary phase completion |
| `docs/requirements/06-maintenance-retention.md:23–31` | One inventory and explicit recovery promises | Admission owner grants, phase roots, maintenance exclusion |

| Component and owner | Consumes | Produces or protects |
| --- | --- | --- |
| Engine change qualification | Captured owner catalog view, exact canonical endpoint releases | Bounded qualified source-change pins, explicit fallback or refusal |
| Engine incremental discovery | Qualified delta sides or conservative endpoint-file differences | Typed old/new join or grouping keys |
| DataFusion and native reader adapter | Narrow typed key relations and captured endpoint tables | Deduplicated affected scope, complete recomputed output |
| Engine processing completion | Prepared output and exact admitted dependencies | Existing `PhaseResult`, reusable across later failure |
| Engine retention | Result/phase roots and active work | Exact object protection through the actual lifetime |

These are existing seams. Moving the shared internal qualification helper into
`engine::changes` is a ownership clarification, not a new public subsystem.

## Reviewed implementation shape

1. Extract the admission portion of
   `src/engine/comparison/chain.rs:30–147` into a small internal result that owns
   the qualified source pins and their metadata permits. Keep one captured owner
   `View` and its existing `SnapshotRows` per source discovery. Retain the current
   link/byte bounds, causal revisions, co-commit checks, exact canonical endpoint
   metadata, receipt-only logical-identity bridges, and missing/invalid distinction.
   Public comparison keeps its identity normalization; R5 uses the same qualified
   pins without that normalization. Do not call public `changes()` or
   `change_certificate()` from an already admitted refresh: those methods acquire
   another maintenance guard and resolve endpoints again
   (`src/engine/comparison.rs:152–193`,
   `src/engine/changes/certificates.rs:321–350`).

2. Pass the already loaded old/new `ReleasePin` values to discovery, rather than
   losing their exact local selection identity at the current `TablePin`-only
   boundary (`src/engine/incremental.rs:135–160`,
   `src/engine/aggregation.rs:199–208`). Preserve canonical source pins for chain
   continuity; a later physical representation choice must not rewrite the
   certificate endpoints. Reuse parsed endpoint metadata when passing it to
   fallback or schema checks instead of reopening it in another layer.

3. For a qualified chain, read the declared key columns from every nonempty
   removed and added source table. Use one sequential native stream, with key
   projection applied inside each child plan, before sequence composition. Admit
   parsed metadata and keep its permits alive through stream completion. Preserve
   the current full schema/field-rule qualification rather than weakening it to
   matching column names. For global aggregation, a qualified nonempty source side
   establishes global scope from its retained row count; no key payload is needed.

4. Feed those raw keys into the existing deduplication and recomputation paths.
   Joins union both changed sides, use null-safe scope membership, remove old
   contributor tuples and compute newly possible matches with the declared join
   semantics (`src/engine/incremental.rs:186–263`). Summaries recompute entire
   affected groups (`src/engine/aggregation.rs:226–253`). Intermediate chain keys
   may overstate scope, including a move followed by a reversion. That is safe and
   avoids building another net-change algorithm. Do not materialize a separate
   change table for every edge or create an in-memory key set.

5. Keep logical-identity equality as the earliest zero-value-work path
   (`src/engine/incremental.rs:59–103,359–362`). Still advance exact captured
   coverage and run the existing evidence/qualification path; an empty key set
   is not an empty complete result or proof of unchanged evidence. A receipt-only
   hop inside a mixed chain contributes no source keys but must pass continuity.

6. Missing/retired history, unsupported local acceleration and explicit history
   bounds use the current changed-file discovery, which reads the key columns of
   the old/new immutable-file difference. It is a conservative discovery fallback,
   not an exact row-diff implementation. Full reconstruction remains the output
   oracle. Preserve the immutable-path descriptor/digest check
   (`src/engine/incremental.rs:364–447`). Do not report this path as delta-only or
   infer accepted source succession from successful chain discovery: final
   `guard_inputs` remains authoritative (`src/engine/refresh.rs:232–269`).

## Protection and recovery decision

| Object or fact | Needed while discovering | Needed by completed processing |
| --- | --- | --- |
| Captured local certificate/result catalog records | Yes, for authority and continuity | No new chain dependency |
| Applied descriptor objects and source delta metadata/manifests/data files | Yes, protected by active maintenance admission and existing producer roots | No, after output is committed |
| Intermediate endpoint schema metadata | Yes where needed to interpret qualified history | No new whole-endpoint payload promise |
| Old/new exact input releases and output base | Yes | Yes, already covered by admission/base/coverage roots |
| Completed output, its applied changes and contributor evidence | As produced | Yes, through ordinary phase/result roots |
| Accepted source succession | Separately required by coverage guards | Separately retained; never substituted by optional certificates |

The old source inputs are protected through the admitted prior related result;
targets are protected through captured input coverage. Admission gathers those
releases and acquires owner-side protection before acknowledgment
(`src/engine/admission.rs:165–225,280–290`,
`src/engine/retention/owners.rs:544–555`). Inventory follows related coverage into
exact input releases (`src/engine/retention/inventory.rs:909–916,980–982`). Optional
change roots separately protect their descriptors and delta interpretation/files
without promising whole intermediate endpoint availability
(`src/engine/retention/inventory.rs:993–1008,1062–1078`).

If an attempt dies before processing completion, its discovery was uncertain work.
Retry can reread still-retained history or take the qualified fallback after
history retirement. If processing completed, retry reuses it before touching
history. Neither outcome requires permanently adopting a chain. If a future
consumer returns a lazy stream beyond admission, schedules reads in another
process, or promises replay of that exact chain, this conclusion changes: its
actual descriptor/delta/schema objects and authority evidence need a durable
consumer protection binding before that promise. Current local exclusion must
not be treated as cross-host protection.

## Ordinary discovery phase comparison

**Defer a separate discovery completion until a measured or observed retry problem
justifies it.** The current required useful boundary is completed matching/
processing, which already exists. A written `affected-keys` table is not by itself
a committed phase (`src/engine/incremental.rs:186–204`,
`src/engine.rs:931–961`). The requirements permit rerunning uncertain uncommitted
work; they do not require a completion for every intermediate native relation
(`docs/requirements/05-dependent-refresh.md:37–39`).

If repeated expensive discovery becomes the concrete problem, retain the already
written final affected-key relation as an ordinary `PhaseResult`: bind exact
before/target inputs, base, definition, discovery rules and table; resolve it before
chain discovery; protect that table and inputs, not every historical accelerator.
This needs explicit integration into planning/inspection, phase retirement,
inventory and restore qualification. Merely adding a phase string is insufficient:
inventory currently enumerates allowed phases, and retirement enumerates their
suffixes (`src/engine.rs:389–392`,
`src/engine/retention/inventory.rs:1283–1318`,
`src/engine/retention/retirement.rs:19–36`). This alternative adds metadata commits
and retained key storage. Durable chain adoption adds owner grants and historical
storage but still recomputes keys, so it is the weaker choice for that user problem.

## Maintained execution and resource limits

The current native provider already pushes selected columns into its reader
(`src/query.rs:161–178,207–250`). `query::sequence` retains admitted metadata and
executes one child at a time (`src/query/sequence.rs:19–89`). In pinned DataFusion
53.1.0, `StreamingTable::scan` passes projection to `StreamingTableExec`, not into
`PartitionStream::execute`; retain the inner native projection. In pinned
`futures-util` 0.3.34, `TryFlatten::poll_next` drops a finished child before polling
the outer stream for another. Sources inspected: registry
`datafusion-catalog-53.1.0/src/streaming.rs:96–134` and
`futures-util-0.3.34/src/stream/try_stream/try_flatten.rs:56–71`, versions from
`Cargo.lock:1104–1105,2177–2178`.

Expected structural benefit is key reads proportional to retained applied rows
instead of every row in rewritten endpoint files. This is not a throughput claim:
catalog-history loading, endpoint manifest work, matching scans, fanout, output
rewrites, and retained history still cost time, memory and disk. The current chain
limits are safety bounds, not a measured crossover. Measure complete refresh and
retry paths before adding crossover heuristics or automatic retention.

## Invariants, falsification, and remaining gates

| Invariant | Status in the reviewed design | Failure test |
| --- | --- | --- |
| Affected scope has no omissions | Preserved by union of raw old/new source keys | Same-key non-key update, null transition, equal-value new occurrence |
| Public value comparison and R5 discovery share authority checks | Preserved by qualification extraction | Corrupt/co-commit-mismatched descriptor refuses both consumers |
| Optional history never determines complete-result readability | Preserved by endpoint protection and fallback | Retire history before retry; complete result still reconstructs |
| Completed expensive work survives parent failure | Relies on existing ordinary processing completion | Failure after processing, retire history, cold resume reuses processing |
| Coverage order and evidence are independent of value equality | Preserved by existing coverage/evidence selection | Receipt-only advance and missing succession evidence |
| Maintenance cannot remove bytes used by the current attempt | Relies on owned-host admission | Queue retirement/removal while discovery is live; release only after worker finishes |

Before enabling R5 acceleration, compare complete join and summary results and
contributor relations against independent full reconstruction for both changed
sides, moved/new/deleted/null keys, duplicate fanout, same-key field updates,
multi-hop reversions, equal-value new occurrences, global/empty summaries,
compaction and evidence-only transitions. Exercise missing/retired history and
invalid present history separately. Qualify failure before/after processing,
cancellation/worker loss and maintenance waiting. Check observable read behavior
with a bounded instrumentation fixture; correctness equality alone does not prove
narrow reads. Then measure fixed updates against population, file count and history
growth, including additional/retained disk and failed attempts.

Kill criteria are an omitted affected key, dependence on retired optional history
after processing completion, or higher complete-path cost without meaningful
benefit. Keeping only the existing conservative file path is the valid
counter-decision: it simplifies planning but preserves unnecessary key reads after
large rewrites. Removing the new accelerator must therefore make processing
slower, never change its result or recovery promise. The likely future review
concern is hidden broad endpoint work, not the absence of another retention ledger.

Intent and shape match; user value is supported structurally but unmeasured.
The design preserves the cited commitments and pays down duplicated authority
logic. Confidence is high for the local ownership/lifetime decision and medium
for complete-path resource benefit pending the stated qualification.
