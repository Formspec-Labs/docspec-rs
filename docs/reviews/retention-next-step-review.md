# Next R6 step: retire terminal operation recovery roots

## Findings and decision

**RECONSIDER a result/phase-only reclamation claim. Implement explicit terminal-operation recovery retirement next, including its attempt reservations.** This is the smallest extension that can make unused local objects cease to be protected. Foreign grants and some descendant dependencies remain separate blockers; report them rather than claiming that logical retirement has freed disk.

Static architecture review on 2026-10-09 against `4fd1ca6` and the uncommitted result-availability retirement changes. This reviews implementation sequencing, not the current patch's execution results. No Rust changes, Cargo commands, cleanup or provider calls were performed.

### F1 — CONCERN: terminal operations still retain recovery inputs and entire attempt directories

`operation/`, `request-dependencies/` and `backfill/` expand their captured inputs even after completion or abandonment. Every attempt begins with `retired: false`; inventory protects its entire directory. Result and phase retirement alone cannot end these roots. The current retirement scope does not accept their record families (`src/engine/retention/retirement.rs:30`, `src/model.rs:507`).

**RESHAPE:** add an operation-recovery scope using the existing exact-generation retirement mechanism. End these recovery claims and their blanket attempt protection explicitly, while keeping independently retained phases and results alive. Do not treat abandonment itself as phase expiry.

### F2 — CONCERN: foreign grants outlive their claims; naive token retirement would permit stale reuse

Inventory expands every `owner-protection/` grant independently of consumer claims (`src/engine/retention/inventory.rs:187`). Active `reference-binding/` records also contribute tokens until their exact claim generation retires (`:196`). Retiring the original producer or consumer operation does not end a token still used by another live phase/result.

The current propagation API explicitly assumes tokens cannot retire (`src/engine/retention/owners.rs:398`). `acquire_owner` returns an existing stable-key grant without a retirement check (`:633`), and `local_binding` only handles the earlier-generation retirement case (`:456`).

**RESHAPE before foreign grant retirement:** classify claims by exact live generation; refuse propagation from retired claims; release only token generations with no active consumer binding; and make acquisition create a new granted generation when the old one retired. A token identifies one consumer, so a bounded scan of that consumer's bindings is sufficient under owned-host maintenance exclusion. Unknown or inconsistent bindings remain blockers. This is a concrete additional requirement, not part of the current availability scope.

### F3 — CONCERN: completed Backfill planning still opens recovery inputs

`plan_backfill_admitted` resolves the old base, opens native metadata, compiles expressions and opens an occurrence scan before detecting its completed result (`src/engine/backfill.rs:210`, `:231`, `:233`). The run path already checks request identity and returns the completed result first (`:407`).

**RESHAPE with operation retirement:** after availability and exact request-fingerprint checks, return a completed plan from the retained result before opening original recovery inputs. Report completed status and retained row metadata; do not invent file-level diagnostics that require reopening retired inputs. A retired availability promise must still refuse; an available completed result must never silently recompute or reselect. Keep the ordinary plan's existing retained-readback fast path (`src/engine.rs:308`).

## Exact root trace

| Remaining root | Current retention effect | Required disposition |
| --- | --- | --- |
| `operation/<id>` | Recursively retains exact base release, even when terminal (`inventory.rs:960`). | End recovery contribution through the operation scope; retain identity/fingerprint and original record revision. |
| `request-dependencies/<id>` | Retains admitted inputs, base and related coverage (`inventory.rs:974`). | Same operation scope; a separately live completion must protect the dependencies it actually uses. |
| `request/<id>` | Direct content expansion stops after result/abandonment, but its binding remains active (`inventory.rs:985`, `:196`). | Include exact claim retirement so its tokens stop contributing recovery protection. |
| `backfill/<id>` | Retains captured base, target schema/binding and related evidence (`inventory.rs:935`). | Same operation scope; completed Backfill replay must not depend on reopening it. |
| `protection/<attempt>` | Protects all candidate paths under each active reservation (`inventory.rs:1085`). | End blanket protection only for attempts demonstrably owned by the terminal operation. Keep the reservation as creator evidence. |
| Material phase/group completions | Retain exact source/output dependencies independently (`inventory.rs:867`, `:917`, `:1000`). | Existing phase scope, explicitly and independently selected. Never cascade from parent retirement. |
| `reference-binding/` and `owner-protection/` | Active claims and all owner grants retain foreign objects (`inventory.rs:187`, `:196`). | Exact-generation grant retirement and active-binding checks; no implicit release from elapsed time or parent status. |
| Legacy succession/progress bindings | New ordering records do not promise payload availability, but old bindings/grants can persist (`history.rs:227`, `inventory.rs:1082`). | Explicitly retire their obsolete content claims using known metadata-only semantics, then release unused grants. Unknown scopes remain conservative. |
| `generation/<id>` and coverage seals | Each seal expands every member release, including members not consumed as input (`inventory.rs:782`, `:792`, `:1009`). | Still blocks coupled-data reclamation. Separate immutable admissibility evidence from a content promise before claiming that case complete; retain exact consumed inputs/evidence. Sealing currently acquires member grants (`inputs.rs:86`). |
| Selected/live descendant results | Follow parent manifests, computed bases, change bases and actual related inputs (`inventory.rs:799`, `:809`, `:817`). | Keep required evidence and interpretation. Some edges currently expand whole ancestor releases; a future edge-specific lifetime qualification is necessary before claiming routine update history can be reclaimed. Retiring the ancestor's standalone availability alone is insufficient. |

Table citations abbreviate paths under `src/engine/retention/`, except `history.rs` and `inputs.rs`, which are under `src/engine/`.

## Minimum implementation shape

Add `RetirementScope::OperationRecovery { operation }`. Reuse `RetentionPlan`, `RetainedGeneration`, marker/receipt validation and lost-response reconciliation; do not introduce another retirement service (`src/model.rs:499`, `src/engine/retention/retirement.rs:296`).

1. Collect the operation's exact operation/request/dependency/capture generations and owned attempt reservations. Require a matching completed result or valid explicit abandonment. Validate native revisions and request identity. A selected result may stay available: its result promise is independent of operation recovery.
2. Execute through `maintain_work`, then the existing dataset lock and a fresh lifecycle view. Recollect the exact set, refuse a stale or expanded plan, and append scoped retirement markers/receipt. This drains admitted operations and excludes new ones for the whole owned task, including caller cancellation (`src/engine/worker.rs:110`, `:135`).
3. Use one shared exact-generation liveness decision for both root expansion and binding contribution. Extend the existing retirement receipt pass rather than decoding each receipt repeatedly or building a new index (`inventory.rs:147`). A retired claim must not provide propagation scope later. Preserve raw records as receipts; availability/reuse decisions consult the relevant promise.
4. Stop the matching attempts' blanket directory protection. Both producers record the operation in `Protection.purpose` (`src/engine.rs:1678`, `src/engine/backfill/preparation.rs:305`). Validate that canonical association plus dataset/key/path, or make the association typed; do not infer it from directory age. Prefer exact retirement markers as the decision authority rather than a second independently mutable retirement flag.

Keep `check_object_owner` able to read a closed reservation. Its current checks deliberately validate creator identity and path without requiring `retired == false` (`owners.rs:701`). Completed phases or other results may still retain files created in that directory. Ending blanket protection is not erasing creation evidence.

For owner grants, use the same scoped plan/receipt machinery with an explicit grant scope when that extension is implemented. Batch candidate tokens by consumer and inspect each consumer's active bindings once under `maintain_work`; lock affected datasets consistently. Retire consumer claims before unused owner grants so interruption leaves excess protection, never missing protection. Stable grant keys exclude revision (`owners.rs:196`), so reacquisition must issue a new native generation and preserve the old retirement marker. Existing live claims keep their exact token generation. Do not add owner lookups to ordinary reads.

## Lineage, boundaries and invariants

| Source / seam | Relationship and commitment |
| --- | --- |
| `docs/requirements/06-maintenance-retention.md:9`, `:23`, `:27` | Parent requirement: provenance, content availability and recovery have independent lifetimes; completed phases survive parent failure. |
| `docs/requirements/06-maintenance-retention.md:29`, `:31` | Fresh eligibility can shrink an exact plan; logical retirement is not physical deletion authority. |
| `docs/PLAN.md:41` and `AGENTS.md:29` | Keep routine reads and retries free of repeated payload/graph validation. Perform maintenance scans only for explicit maintenance. |
| `docs/reviews/history-metadata-design.md` and `4fd1ca6` | Retain ordering/observation evidence independently of payload availability. Do not recreate observed-head payload roots. |
| `retirement.rs:296` → `worker.rs:135` | Retention service owns logical decisions; the owned host supplies exclusive admitted-work custody. This does not qualify distributed maintenance. |
| `inventory.rs:187` → `owners.rs:611` | Consumer claims and owner tokens jointly govern foreign protection; token retirement requires acquisition/reuse changes. |

**Preserve:** the original immutable result and succession rows and their selection revisions. `history::check_link_receipt` requires their equal original native revision (`src/engine/history.rs:327`). Retirement appends evidence instead of rewriting receipts.

**Preserve:** distinguish a public/fresh availability lookup from raw receipt access for already admitted dependencies. A live descendant or phase may continue using its independently retained dependency after the source's standalone availability retires. That does not authorize admitting new work from a retired source.

**Rely on:** independently reused phases already retain their own exact dependencies. Qualification must prove this, including transferred owner tokens; parent recovery retirement cannot compensate for incomplete phase dependency records by silently deleting content.

**Keep out of this increment:** physical removal and restore execution. `read_group` streams without acquiring the host maintenance lock (`src/engine/read.rs:11`, `:38`); the writer barrier alone is not a reader-lifetime guarantee. Actual removal still needs supported reader protection, unavailable/reserved exact objects, durable deletion intent and outcome reconciliation required by R6. An unblocked inspection candidate is not deletion authorization (`src/model.rs:550`).

## Required cases and user-value test

- Build independent A, then replace it with independent unbased B. Retire A's availability and explicitly chosen phases, then A's operation recovery. An unused exact A object must become unblocked; B and any retained A phase remain protected. This is the positive local reclamation-eligibility witness.
- Reuse a completed A phase in B. Retire A's operation and close its attempt. B's exact files/dependencies remain readable; the creator reservation remains valid evidence. Repeat with a foreign token before enabling grant retirement.
- Keep A's result available while retiring only recovery. Exact completed run and Backfill plan retries succeed without opening removed recovery-only artifacts; conflicting operation fingerprints still refuse. Retired result availability still refuses rather than recomputing.
- Preview, then add/change a claim or reservation: stale execution cannot expand scope. Exercise a caller-disconnected worker, commit-response loss and repeated retirement identity. Preserve independently completed phases and exact receipt revisions.
- For grant release: one surviving phase/result binding blocks release; no active bindings permits exact-generation retirement; subsequent acquisition cannot revive the retired token. Unknown legacy scope refuses or remains protected.
- Show seals and required descendant dependencies as remaining blockers. Do not label their bytes reclaimed, and do not use their presence to weaken promised historical interpretation or evidence.

The beneficiary is the operator controlling retained disk after corrections and failed attempts. This adds a scoped decision to an existing service and pays down permanent-root debt; it requires no scheduler, normal-read scan or new catalog backend. No speed, memory or reclaimed-byte improvement has been measured here. Maintenance work scales with the explicitly inspected metadata/bindings and remains bounded by the existing allowance.

## Counterfactual and verdict

Keeping all terminal recovery roots would preserve safety but defeat the positive unused-object witness. Automatically retiring every phase with its parent would simplify cleanup while violating R6's independent recovery promise. A new garbage collector would duplicate existing exact plans and inventory without fixing either lifetime error.

**Verdict: RECONSIDER the next step if it only adds another availability marker. Proceed with terminal-operation recovery retirement and attempt closure as the next coherent implementation.** Intent and requirements align after that reshape; shared retirement mechanics cover most of the design. Confidence is high for the static root/replay trace. The decisive failure criterion is an unused independent A object remaining protected solely by its terminal operation/attempt after all explicitly selected promises retire. Foreign-grant, seal, descendant-lifetime and physical-removal qualification remain named work, not implied completion.
