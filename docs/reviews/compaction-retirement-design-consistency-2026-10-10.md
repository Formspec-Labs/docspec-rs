# Compaction retirement: consistency and recovery design review

**Verdict: APPROVE the bounded design below for implementation.** Reuse exact-generation retirement to explicitly end a named compaction's entire recovery promise, preserve its typed records as proof, keep the selected representation as an independent live root, and send exact newly unprotected objects through existing removal. Merely adding compaction key prefixes to phase retirement does not produce this behavior.

Static review of `a3d695f` on 2026-10-10. No build, test, benchmark, corpus operation or production mutation was performed. This is a design verdict, not implementation or runtime qualification. Line references below identify that baseline.

## Findings and required decisions

### F1 — Separate selected layout protection from recovery dependencies

**BLOCKER for a key-prefix-only implementation; intent versus shape.**

The inventory currently treats `compaction-prepared/`, `compaction-verified/`, `compaction-result/` and `representation/` alike: it validates the captured request, then protects both `result.source` and `result.replacement` (`src/engine/retention/inventory.rs:1193-1228`). Thus the latest representation B permanently protects its predecessor A even after A's own recovery records retire.

**RESHAPE:** Keep the same `CompactionResult` value and immutable request/result evidence. Validate a selected representation against its request exactly as today, but make its physical root only `table(result.replacement)`. That existing helper retains the replacement closure and `logical_metadata` as an exact metadata object; it does not expand the logical metadata's old payload (`src/engine/retention/inventory.rs:886-905`). Recovery records continue to protect both source and replacement until explicitly retired. Source/request identities retained in lifecycle rows are proof references after recovery ends, not an implicit promise to retain every predecessor payload.

The selected key remains `representation/<logical-metadata-digest>/<group>`. It must never be included in the compaction-recovery retirement scope. Both managed readers and future compaction resolve that key (`src/engine/compaction.rs:52-84,152-154`; `src/engine/read.rs:21-29`). Removing it would silently revert managed reads to the canonical layout, contrary to the requested separation of maintenance and retention.

### F2 — Retirement must fence the request and release its exact attempts

**BLOCKER; recovery and scope.**

Compaction returns a saved result before reading its request and otherwise resumes request/prepared/verified state without consulting retirement (`src/engine/compaction.rs:106-135,215-223,280-311`). Each rewrite also creates an independent `protection/<id>` prefix reservation whose purpose is `<operation>/compaction` (`src/engine/compaction.rs:232-247`). Retiring completions alone leaves this prefix protecting obsolete output (`src/engine/retention/inventory.rs:1494-1508`). Skipping retired completions without a request fence could recreate outputs under an operation whose recovery was explicitly ended.

**RESHAPE:** Add `RetirementScope::CompactionRecovery { operation }`, a planner, and a thin executor using `retire_retained`. Explicitly document this as ending all recovery for the named compaction, including a stalled or interrupted one. That decision is the explicit abandonment required by R6; no new scheduler state or separate abandonment record is necessary.

Collect the exact existing generations of:

- `compaction-request/<operation>` (required);
- `compaction-prepared/<operation>`, `compaction-verified/<operation>` and `compaction-result/<operation>` (when present);
- owned `protection/<id>` records whose purpose equals exactly `<operation>/compaction` and whose path equals the expected owned attempt directory.

The plan must name all such records at its boundary. Execute under existing exclusive `maintain_work`, then the dataset lock, and compare a fresh collection with the saved list. A worker that completes another phase or creates another attempt after planning makes the plan stale; execution must not silently enlarge it. Existing receipt, marker and lost-response handling can commit/reconcile the decision (`src/engine/retention/retirement.rs:501-633`).

At the first locked compaction entry, validate any existing request's spec identity, then check its exact retirement marker **before** returning `compaction-result` or resuming any phase. A retired request refuses with a specific recovery-retired result, including after reopening the Engine. Keep all original rows. Existing creation qualification deliberately reads `compaction-prepared` outputs and must continue to work after retirement (`src/engine/retention/owners.rs:956-1001`).

### F3 — Prevent same-ID cross-kind attempt retirement

**BLOCKER; ownership.**

Normal operation recovery collects every protection with a purpose starting `<operation>/`; its validator accepts any syntactically valid purpose (`src/engine/retention/recovery.rs:162-185,217-239`). A normal operation and compaction can therefore share an ID while having separate request families, and the normal operation's recovery plan can capture the compaction reservation.

**RESHAPE:** Compaction recovery must accept only purpose exactly `<operation>/compaction`. Normal operation recovery must exclude that reserved purpose in collection and reject it when validating a supplied/saved attempt. Apply the same distinction to every validation path, not just preview. Do not solve this by globally making unrelated operation namespaces mutually exclusive.

### F4 — Narrow selected representation owner grants

**CONCERN; cross-owner retention and accuracy of reclamation claims.**

Selection currently copies the original source protections into both the representation and result binding (`src/engine/compaction.rs:350-363`). Foreign owner grants retain their complete object scope as independent roots (`src/engine/retention/inventory.rs:289-301`). Grant retirement refuses every token still listed by an active binding, even if the newly narrowed typed root no longer needs those predecessor objects (`src/engine/retention/grants.rs:140-164`). Changing root traversal alone therefore does not end overbroad foreign-source protection.

**RESHAPE for new selections:** Acquire a separate exact protection set for replacement objects plus its logical metadata through `protect_objects`, before the final consumer dataset lock. Use it only for the `representation/` binding; preserve source protections on recovery records. The helper already deduplicates identities, acquires qualified per-owner scopes, and avoids grants for locally owned objects (`src/engine/retention/owners.rs:558-602`). Recheck the captured/current representation before selection as today. A partial rewrite must keep its shared foreign files and logical metadata protected.

Do not simply drop an owner token if some of its objects are obsolete: one token can cover both required and obsolete objects. Do not weaken `grant_in_use` while leaving a binding that later insists that token is active (`src/engine/retention/inventory.rs:695-707`). Old broader grants remain explicit independent promises until the existing owner-grant retirement service ends them.

Already-selected records with broad bindings require either a separately qualified same-layout rebind or a subsequent selection using the narrowed binding. Without that work, report their retained grants as blockers; do not claim that this increment reclaims every historical foreign source. A generic grant-pruning mechanism is unnecessary for the local intermediate-representation case.

## Artifact, lineage and relationships

The decision serves an operator who wants repeated compaction to stop accumulating obsolete intermediate files while retaining selected values and recoverable work. It is a maintenance product capability built on existing lifecycle evidence; it adds no second deletion authority.

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| Accepted implementation plan | One inventory, explicit retirement, durable removal and exact immutable pins constrain the design. | `docs/PLAN.md:35-40,67-75,96-106` |
| R6 requirements | Recovery, retained results and physical layout have distinct lifetimes; explicit abandonment permits retirement, fresh protections win, automatic sweeping is deferred. | `docs/requirements/06-maintenance-retention.md:9-37` |
| Current status | Existing compaction and writer adoption are implemented; obsolete recovery retirement is explicitly open because active representations root their predecessor. | `docs/STATUS.md:840-850,910-938` |
| Earlier compaction review | Admitted recovery must survive retirement of the historical base's standalone availability. | `docs/reviews/compaction-review.md`, F1 |
| Existing terminal-operation retirement | Supplies exact collection, attempt ownership checks, saved-receipt validation and independent retained roots. Its broader attempt predicate needs the F3 correction. | `src/engine/retention/recovery.rs:191-299` |

| Component or seam | Owner and dependence | Consequence |
| --- | --- | --- |
| Compaction capture, rewrite, verification, selection | `engine/compaction.rs`; lifecycle records and native immutable pins | Capture before output; reuse saved phases before payload work; guard representation freshness at selection. |
| Retirement plan and commit | `engine/retention/retirement.rs`; exact record generations and existing maintenance admission | Add one scope and its collection/validation, reuse the common commit and reconciliation. |
| Root inventory and grant bindings | `engine/retention/inventory.rs`, `owners.rs`, `grants.rs` | Selected layout protects its output; recovery protects exact input/output; foreign references require active exact grants. |
| Removal effects | `engine/retention/removal.rs`; fresh common inventory and typed creation evidence | Detach exact native heads, reserve unavailability and reconcile exact file outcomes; retirement itself unlinks nothing. |
| Readers and writers | `engine/read.rs`, `engine.rs`, `engine/worker.rs`; selected or admitted immutable pins | Drained readers cannot race retirement; captured update layouts remain independent roots after another compaction. |

The governing seams are named in the plan and R6 requirements. The proposed compaction-specific meaning of explicit retirement must be documented on the new API and transport request; it should not depend on knowing an internal key prefix.

## Lifetime and state shape

| State | Active physical dependencies | After explicit compaction-recovery retirement |
| --- | --- | --- |
| Captured request | Exact source layout and captured release dependencies; qualified owner grants | Typed request remains identity/proof; its marker permanently stops this operation from resuming or returning an availability-promising result. |
| Prepared rewrite | Exact captured source and completed replacement | Typed completion remains creation evidence; independent selected/admitted roots may still retain its files. |
| Verified rewrite | Same source/output plus retained successful verification | Verified fact remains in lifecycle metadata; no new payload scan is required merely to retire it. |
| Completed result | Same recovery/replay dependencies | Result metadata remains; `compact` retry refuses rather than advertising removed output. |
| Selected representation | Replacement objects, logical metadata, narrowly required owner grants | Remains selected and protected regardless of recovery retirement. |
| Attempt prefix | Files inside this operation's exact owned compaction attempt | Its protection ends only in the explicit exact-generation bundle; unknown incomplete files do not thereby become qualified removal candidates. |
| Separately admitted consumer | Its own exact captured layout and dependencies | Unchanged; may prevent reclamation after the compaction bundle ends. |

This bundle is a deliberate API choice: it ends the named compaction's entire recovery promise, not an accidental cascade from parent failure. R6 requires completed phases to survive cancellation, failure and reporting errors and permits their retirement through explicit abandonment (`06-maintenance-retention.md:27`). It does not require a separate public policy/control for every internal record family. Independent per-phase expiry can be added if a concrete reuse policy needs it; adding it now would require retirement-aware recovery precedence across prepared, verified and result state with no demonstrated user benefit.

Suggested API shape is `plan_compaction_retirement(dataset, retirement, operation, reason) -> RetentionPlan` and `retire_compaction(plan) -> Retirement`. Names may follow current transport conventions. Existing bounded `RetentionPlan` carries the scope, reason, lifecycle identity/boundary and exact generations (`src/model.rs:537-577`). Use the existing inspection and removal APIs for blocker and reclaimable-byte reporting; do not invent byte savings in the metadata-only retirement plan.

The collector should validate dataset/operation/group identity, captured request equality, stage revision order, and consistency between prepared, verified and result records. A verified record must equal its prepared record with verification set, and a result must match verified state. Validate exact attempt identity, path, consumer, purpose and revision. Saved-receipt validation must retain these checks after markers suppress physical roots, without expanding retired payloads or rescanning every attempt for every receipt. Reject orphan/malformed markers and wrong-scope records through the shared receipt machinery.

## Invariants and failure traces

| Invariant and source | Status in proposed design | Falsifying failure |
| --- | --- | --- |
| Compaction preserves logical selection and occurrence meaning (`06-maintenance-retention.md:15-17`). | PRESERVED: retirement neither overwrites the canonical result nor removes the selected representation key. | A managed read falls back to the old layout after recovery retirement. |
| Completed work survives parent failure (`docs/PLAN.md:69-75`; R6:27). | PRESERVED: no automatic retirement; only the explicit named bundle ends its recovery. | A failed/stale compaction loses prepared files without an explicit retirement receipt. |
| New protections win; plans cannot expand (`R6:29-31`). | RELIES ON existing maintenance, fresh exact collection and removal intersection. | A phase/attempt added after preview is silently included in execution. |
| Old admitted readers/writers retain bytes (`R6:17,25`). | PRESERVED: `maintain_work` excludes admitted work; update captures remain roots. | Retirement/removal runs while a held Engine stream is still using the old layout. |
| Logical interpretation metadata differs from an available native snapshot (`TablePin::logical_metadata`, `src/model.rs:80-88`). | PRESERVED: `table(replacement)` retains the metadata object without expanding old payload. | Removing predecessor payload deletes metadata needed to interpret a surviving group. |
| Creation proof survives retirement (`owners.rs:997-1001`). | PRESERVED: original rows remain readable; new unavailability markers prevent reacquisition after reservation. | Removal cannot qualify the old replacement after its recovery promise ends. |
| One removal authority (`docs/PLAN.md:96-106`; R6:31). | PRESERVED: exact candidates still use current removal. | Retirement directly unlinks a directory or treats an age/listing as orphan proof. |

The concurrency order is concrete. `compact` holds shared admitted-work ownership for its full owned service task (`compaction.rs:24-30`; `worker.rs:111-134`). Retirement takes exclusive ownership and then the dataset lock (`retirement.rs:501-519`); streams retain their shared ownership until EOF/drop (`worker.rs:159-200`). Therefore execution waits for a currently running compaction or held Engine stream. If a planned compaction advances before exclusivity is acquired, fresh collection detects it and refuses the stale plan. Once retirement commits, the request fence rejects later retries. Removal separately holds the same exclusion and reserves unavailability before effects (`removal.rs:326-450`). Catalog-free exported read helpers explicitly require the caller to retain bytes; they are not implicit host leases (`read.rs:112-116,161-176`).

An update admitted against A remains protected after B becomes selected because `request-dependencies.update_source` roots its exact pin (`inventory.rs:1380-1392`). Do not rewrite that capture to B during retirement. Parent manifests, scalar dependencies, retained results, restore points and owner grants also remain real blockers; suppress only the named compaction roots.

## Actual removal and its limits

The useful acceptance case is canonical S, compacted A, then compacted B. Retire recovery for A and B, which includes their attempt reservations. B remains selected and protects B plus logical interpretation metadata. A-only objects become candidates if no independent writer, result, phase, grant or restore promise still uses them. S remains protected by any exact canonical result that retains it (`inventory.rs:1011-1046`). Shared files retained by a partial rewrite stay protected.

Pass exact obsolete `ObjectRef`s from retained typed creation evidence to `plan_removal`. Its current plan expands the root inventory, identifies native tables touching candidates and records exact registration identities (`removal.rs:180-231,866-974`). Execution intersects the old plan with fresh unprotected candidates (`removal.rs:369-400`), reserves object unavailability and intent, and detaches native registrations by UUID and metadata location (`removal.rs:437-450,796-823`). Preserve that path. An old native registration referencing deleted files cannot remain a silently dangling head.

Retaining a logical metadata object does not require retaining its native registration. Static pin opening already reads an exact metadata file without a catalog head (`src/storage.rs:710-732`). Native metadata-log names are historical identity, not selected snapshots or recursively retained payload (`src/storage/tables.rs:51-78`; `src/storage.rs:531-540`). Nevertheless retained native snapshots, source-schema dependencies and restore capture can independently retain their complete closure; use inventory outcomes rather than inferring eligibility from these facts.

This increment does not retire the latest representation key for an old logical digest, collect arbitrary unfinished files lacking typed creation identities, sweep attempt directories, expire grants automatically, or establish remote cleanup. Those are distinct promises or unqualified effects. The retained latest keys and any old broad owner bindings must be reported honestly as blockers. No measured time, memory, disk reduction or full R6 completion follows from this static review.

## User value, alternatives and qualification

The benefit is actual removal of superseded compaction payload through the qualified removal path while current reads and admitted recovery remain correct. Conceptual debt stays small: one retirement scope and collector, one root distinction, a retry fence, exact attempt ownership and narrower selection grants. Reusing the common retirement/removal machinery pays down duplicated decisions.

The opposite decision—leave all recovery roots forever—avoids new retirement semantics but cannot deliver the planned retained-storage control. The smaller key-prefix-only proposal is refuted by F1 and F2. A separate compaction garbage collector would duplicate inventory, ownership and destructive-effect rules. A generic per-phase policy engine or automatic sweep is optional sophistication without a present acceptance need.

The design fails its value claim if A-only bytes still cannot be removed after every intentional independent promise ends, if B changes after retirement, or if interrupted retirement/retry resurrects a disposed operation. Removing the new scope leaves no safe public way to end compaction recovery; removing the selected root distinction leaves predecessor retention indefinitely. The likely future criticism is claiming all obsolete representations are collectible when historical latest keys or foreign grants remain protected; keep those boundaries visible.

Implementation qualification should include:

- S → A → B with real exact removal of A-only files, native registration detachment, unchanged selected B and equal values/occurrences;
- the currently selected compaction's recovery retirement, followed by new managed reads and later compaction;
- request-only, post-rewrite, post-verification and stale-selection interruption, followed by explicit retirement, reopen, and refusal before retry work;
- a new phase/attempt between planning and execution, conflicting/reordered/wrong-generation plans, malformed markers, wrong purposes and same-ID normal-operation/compaction isolation;
- a held Engine stream, an admitted update capturing A, a partial rewrite sharing files, and independent retained result/phase/grant/restore blockers;
- source logical metadata still present after physical predecessor removal, typed creation evidence still usable, and foreign grant scope preserved only for required shared objects;
- lost retirement responses and a cold retry returning the same retirement receipt without enlarging its scope.

**Final assessment:** Intent and shape match if F1–F3 and the retry/validation rules are implemented. The user-value claim is supported by a concrete removal path, not yet by execution evidence. The design honors the accepted lifecycle commitments and reuses sibling services. Confidence is high for the local state/ownership reasoning, medium for end-to-end qualification until the implementation and its targeted/full gates run.
