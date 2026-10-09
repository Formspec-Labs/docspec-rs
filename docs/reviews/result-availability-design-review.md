# Result availability retirement design review

**Verdict: APPROVE the shared scoped design for implementation.** Reuse the
existing exact-generation retirement machinery. The operation-level availability
pointer proposed during review makes the access checks simpler, provided it is
validated against the same durable decision and checked inside admitted work.
This is a design verdict, not approval of an implementation or of physical
removal.

This independent static review uses `4fd1ca6`, the proposed design in the review
request, the repository instructions, the Rust guidance, and the semi-formal
architecture review method. No Rust was edited and no Cargo command, formatting,
benchmark, or commit was run by the reviewer.

## Artifact and lineage

The proposal retires the independent availability promise for one operation's
result and saved readbacks. It preserves their original catalog rows and the
accepted succession chain. A closed scope distinguishes `PhaseRecovery` from
`ResultAvailability { operation: Id }` in shared `RetentionPlan`, `Retirement`,
and `RetainedGeneration` types. Thin phase and availability APIs use common
validation, native commit, and uncertain-response reconciliation.

| Authority or neighboring component | Requirement or relationship |
| --- | --- |
| R6, `docs/requirements/06-maintenance-retention.md:9`, `:23` | Content availability, provenance, and recovery have distinct lifetimes. One scope must not silently retire another promise. |
| R6, `:25`, `:27`, `:29` | Retained results, phases, and unfinished operations protect dependencies independently; execution cannot expand a stale plan. |
| PLAN, `docs/PLAN.md:67`, `:89`, `:96` | Durable completion, lost-response reconciliation, and one protection inventory remain the governing behavior. |
| Accepted ordering, `src/engine/history.rs:191`, `:297`, `:321` | Ordering uses original co-committed result receipts. Preserve those values and native revisions after availability retirement. |
| Phase retirement, `src/engine/retention/phases.rs:264` | Existing exact-generation checks, atomic markers, and retry reconciliation supply the reusable mechanics. |
| Worker ownership, `src/engine/worker.rs:110`, `:135`, `:172` | Admitted workers hold maintenance read ownership through completion; retirement takes exclusive ownership. |
| Inventory, `src/engine/retention/inventory.rs:170`, `:919`, `:933` | Owner grants and admitted dependencies are separate roots, even when a referenced result's independent availability ends. |

## Findings and decisions

### KEEP — share the representation and commit path

The current phase implementation's reusable work is exact native generation and
catalog-boundary validation, one guarded append, receipt/marker agreement, and
uncertain-commit reconciliation (`src/engine/retention/phases.rs:264`, `:333`).
Duplicating that path for availability would create parallel implementations of
the same failure rules. A closed scope with a small scope-specific eligibility
check is simpler. No generic policy framework, dynamic callbacks, or second
retention inventory is needed.

Retain distinct lookup semantics: phase retirement means an old completion is
no longer reusable and a new generation may be produced; availability retirement
is irreversible for the named operation and must refuse replay under that ID.
`active_completion` currently returns `None` for retirement
(`src/engine/retention/phases.rs:149`). Reusing that result directly for saved
readbacks would allow processing to restart after explicit retirement.

### RESHAPE — validate one operation-level pointer inside admitted entry

The refinement adds `availability-retired/<operation>` pointing to the retirement
receipt. Commit it with the exact-generation markers. Check it once at the
run/backfill/plan admitted entry, before preparation and outside result-error
reconciliation. Also check public historical lookup and fresh dependency
admission. A validated pointer must agree with the dataset, operation, scope,
receipt revision, and complete marker set; pointer existence alone is not proof.
Inventory must detect missing or inconsistent pointers as well as bad markers.

This removes the need to repeat the same check at every raw result fast path.
Those paths include normal completion (`src/engine.rs:1003`), error
reconciliation (`src/engine.rs:766`), Refresh selection
(`src/engine/refresh.rs:235`), and Backfill retries
(`src/engine/backfill.rs:373`, `:403`, `:574`, `:727`). Retirement cannot commit
during the whole admitted worker because it holds the read guard until its
owned service future ends (`src/engine/worker.rs:129`, `:172`). Execution holds
the write guard before acquiring the dataset lock (`:152`,
`src/engine/retention/phases.rs:267`). Keep retirement limited to the owned local
catalog; the existing catalog refusal is at `:351`.

Checking before worker admission would leave a race while waiting for the read
guard. Checking inside preparation's error boundary would let the old raw result
reconciliation turn a refusal into success. The proposed placement avoids both.

### RESHAPE — capture and freshly compare the complete operation set

Availability planning should derive the exact `result/<operation>`,
`readback/<operation>`, and all `backfill-readback/<operation>/<digest>`
generations. Validate each key and embedded operation/dataset identity; never
accept arbitrary keys merely because the scope names an operation. A Backfill
operation can retain several composition readbacks
(`src/engine/backfill.rs:604`, `:631`, `:755`).

Re-enumerate under exclusive maintenance ownership at execution and require the
same exact set. The current phase loop checks only listed generations
(`src/engine/retention/phases.rs:301`); by itself it would miss a new readback
saved after planning. Refuse a changed set and require a fresh plan. Do not append
new targets to the old plan or retire only a convenient subset.

Refuse the currently selected operation using freshly checked authority. Permit
an operation with saved readbacks and no selected result only after exact durable
abandonment. Existing abandonment already refuses any operation with a result
and binds the admitted request/revision (`src/engine/retention.rs:155`). Receipt
retry must return the original completed decision before fresh eligibility is
considered, as phase retirement already does (`phases.rs:272`).

### RESHAPE — separate fresh availability from retained dependency resolution

The existing release helpers serve both public lookup and processing of already
admitted dependencies (`src/engine.rs:463`, `:1190`,
`src/engine/write.rs:103`, `:185`, `src/engine/schema.rs:129`, `:166`). Preserve
a raw receipt resolver for the latter; otherwise retiring an old producer's
independent availability would break a consumer whose dependency was retained
before retirement.

In particular, `qualify_input` is also called by `guard_inputs`
(`src/engine/refresh.rs:300`), including guards on retained ancestors (`:277`).
Adding an unconditional availability refusal there would prevent such consumers
from selecting. Fresh capture, fresh admission, and new generation seals must
check availability separately (`src/engine/inputs.rs:169`, `:66`). An existing
retained seal can reconcile its own independent promise (`:37`, `:122`). Fresh
Backfill admission also has a direct base lookup that needs the check
(`src/engine/backfill.rs:417`). Unknown legacy dependency scope must remain
unknown; the new checks must not manufacture acquisition evidence.

### KEEP — retire independent roots, preserve dependent traversal and grants

Filter a retired result/readback generation and its matching binding as
independent roots. Do not make raw receipt lookup globally return absent. The
operation branch still resolves its base receipt, request dependencies still
resolve captured manifests, and retained result/phase manifests still resolve
their own required content (`src/engine/retention/inventory.rs:919`, `:933`,
`:874`). Original ordering receipts likewise remain readable.

The existing phase inventory handles superseded bindings but does not filter a
same-generation binding merely because its completion is retired (`:179`). The
new shared exact-generation set should handle that deliberately. Do not infer
retirement from operation ID, prefix, a missing binding, or a later generation.
Every existing owner grant remains its own root (`:170`). This checkpoint can
end logical availability without making any foreign payload reclaimable.

### RESHAPE — budget discovered generations and preserve existing decisions

The availability request contains a small operation ID and reason, but planning
discovers a potentially much larger readback set. `request_memory` charges the
request before cloning (`src/engine/worker.rs:90`); it does not account for this
newly collected set. Charge discovered keys/generations and identity decoding
before growth. Use narrow identity reads where possible and avoid storing a
second full `ReleasePin` list. Preserve the inventory's decode-once retirement
set (`src/engine/retention/inventory.rs:147`) instead of decoding an entire batch
for every marked root.

Renaming Rust types is harmless to durable data. Changing field names or record
prefixes needs an explicit readable treatment of existing phase decisions;
otherwise a previously retired completion can become active again or an existing
marker can appear orphaned. The greenfield API policy removes a compatibility
promise, but does not make already recorded retirement facts safely ignorable.

Markers append metadata and do not shrink lifecycle history. The catalog bounds
both replayed history and its current cache (`src/catalog.rs:228`, `:242`) and
preflights commit bytes (`:283`). This is bounded explicit retirement, not a
solution to long-term metadata growth or Q7 scaling.

### REVERSE — remove unbased Build's ordering-only payload acquisition

`src/engine.rs:1036` explicitly acquires the prior release even for an unbased
Build because selection retains a history edge. That rationale is obsolete after
`4fd1ca6`: selection now proves the predecessor with catalog receipts
(`src/engine/history.rs:179`, `:193`).

No material processing dependency was found for an admitted unbased Build:

- `process_build` reads the previous source only when `spec.base` exists
  (`src/engine/write.rs:102`).
- `schema_key` returns no inherited schema key without that base; an evolved
  source refuses unbased rebuild (`src/engine/schema.rs:15`, `:71`).
- Computed groups, coupled relationships, and related sources already require
  the appropriate explicit operation/base (`src/engine.rs:1827`,
  `src/engine/inputs.rs:449`, `src/engine/refresh.rs:399`).
- `captured_manifest` does not capture an unbased Build; result composition only
  resolves an explicit base (`src/engine.rs:1729`, `:1189`).

Stop passing the prior release into protection acquisition for that case. Keep
the admission and final authority/schema/group checks. Preserve existing grants;
their retirement is a separate qualified change. This removes unnecessary new
closure traversal, dependency metadata, and possible foreign-owner acquisition.
No speed, memory, or retained-byte improvement was measured in this review.

## Invariants and qualification required

| Invariant | Design status | Decisive implementation evidence |
| --- | --- | --- |
| One scope cannot retire another promise | Supported by the closed enum | Reject mixed keys, foreign operation IDs, malformed pointers, and scope/receipt disagreement. |
| A stale plan cannot expand or partly retire an operation | Requires complete-set check | Add/change a Backfill readback after planning; execution refuses without markers. |
| Current selection remains available | Requires fresh authority check | Current result refuses; historical result can retire; selection racing planning remains protected. |
| Retirement survives lost replies and reopen | Existing shared mechanism applies | Lost native reply, identical concurrent retry, conflicting retirement ID, and cold reopen preserve one decision. |
| Retired operation IDs cannot replay or reselect | Supported by admitted-entry pointer check | Run and plan retries refuse, including all operation kinds and abandoned readbacks; ordinary phase retirement still allows new phase generations. |
| Independent consumers survive producer retirement | Requires split lookup use | Stop a protected Refresh/Backfill, retire its historical producer result, resume; fresh capture of that retired producer refuses. |
| History and original receipt revisions survive | Supported by append-only markers | Succession proofs still work after retirement; original result/succession values and revisions remain unchanged. |
| Shared bytes and unknown scope remain conservative | Supported by root-level filtering | Retained descendants/phases and legacy grants still block objects; an unbound claim is never treated as a known empty acquisition. |
| Work and metadata remain bounded | Existing guards plus explicit new accounting | Small-budget and growing-readback-set cases refuse before unbounded collection or partial commit. |

The user benefit is explicit control over historical result availability while
keeping previously promised recovery and dependency use intact. Duplicating the
whole phase service would not improve that benefit; it would add another place
for retry and generation rules to drift. Conversely, unifying availability
lookup with phase lookup would erase a required semantic distinction.

The verdict covers this representation and ownership model only. Physical
deletion, owner-grant retirement, reader/public availability policies, restore
fencing, automatic expiration, and the broader R6 qualification remain separate.
