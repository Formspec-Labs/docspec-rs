# Owner-side protection: bounded design review

**Recommendation: KEEP dataset-scoped lifecycle authority; add idempotent owner protection before the first durable consumer reference.** Make acquisition and removal reservation use the same guarded owner table. Commit owner protection first, then commit the consumer's exact token bindings. This permits a recoverable two-step handoff without another database, global reverse index, worker registry or payload scan. Removal and restore remain unfinished.

Static architecture review of `e2d7015`. Applied repository/parent instructions and the Rust and semi-formal architecture-review skills. Read the authoritative user attachment, `retention-execution-next.md`, and the actual admission, sealing, selection, verification and inventory paths. No production/test edits, Cargo, corpus reads or deletion. This document recommends a boundary; it does not qualify an implementation.

## Findings that determine the design

| Finding | Evidence | Required shape |
| --- | --- | --- |
| **Selection-only acquisition is too late.** | Active Refresh `request/` records are already inventory roots; admission records are independently reusable (`src/engine.rs:987`; `src/engine/retention/inventory.rs:542`; `src/engine/refresh.rs:53`). | Acquire before committing the request/admission that first promises the foreign dependency. Later processing, evidence, readback and selection preserve the committed token identity; they must not silently infer protection from a current source head. |
| **Seals are foreign consumers too.** | `seal_generation_admitted` commits member `VersionRef`s in the family lifecycle; inventory expands every member manifest (`src/engine/inputs.rs:26`; `src/engine/retention/inventory.rs:355`). | Treat the family as the consumer and protect every member dependency before writing the seal. Retiring a processing phase cannot release a token still needed by a seal. |
| **Existing semantic verification is not the complete protection closure.** | `verification::walk` deliberately follows semantic related-input edges only; inventory also follows composition parents, changes, computed-source interpretation and seals (`src/engine/verification.rs:285`; `src/engine/retention/inventory.rs:348`, `:363`, `:383`). | Reuse/extract the inventory's typed metadata traversal for an explicit set of supplied roots. Do not use `available_graph` or `release_objects` alone as complete ownership evidence, and do not scan all lifecycle namespaces on ordinary admission. |
| **A manifest's owner is not automatically the owner of its transitive objects.** | `ObjectRef` carries its own owner; `TablePin` retains exact inventories (`src/model.rs:64`, `:73`). Compact native expansion in inventory can return paths without original owner/digest evidence (`src/engine/retention/inventory.rs:623`). | Group verified references by each object's actual owner, preserving conflicting identity refusal. Use complete retained pins and already-bound upstream tokens. Do not relabel foreign files to the outer release owner or invent exact owner identities from native paths alone. |
| **Adding owner locks beneath existing consumer locks can deadlock.** | Capture/sealing use sorted multi-dataset locks; `complete` and selection already hold consumer locks (`src/engine/inputs.rs:162`; `src/engine.rs:1503`; `src/engine/history.rs:160`). | Resolve the bounded lock set before acquiring it, or finish owner acquisitions before entering the consumer lock. Never discover and lock another owner while holding an arbitrary consumer lock. Recheck mutable eligibility at the existing consumer commit. |
| **The local maintenance gate cannot stand in for durable owner authority.** | Ordinary services use readers and maintenance uses an owned writer (`src/engine/worker.rs:110`, `:135`); these disappear with the process. | Both reference acquisition and removal reservation must read their predicate and commit through the same owner's guarded `Lifecycle::commit`. Race-test that boundary directly so the global gate cannot hide a missing durable conflict. |

## Decision lineage and affected seams

The authoritative attachment's **Object protection**, lines 31–37, requires one owner, committed foreign protection before selection, and acquisition/removal conflicts in the same guarded state. Its **Recovery and reuse** section requires durable protection at independent completion boundaries. R6 **Retention and deletion** requires fresh eligibility, durable unavailability and exact effect reconciliation. `retention-execution-next.md`, **Blocking seams** and **Smallest concrete implementation**, identify the same missing owner authority. This checkpoint implements that prerequisite; it does not narrow the parent requirements.

The attachment is `/tmp/codex-remote-attachments/01a116b5-a9a6-7363-acec-cc4bcb984f9a/68780234-c3d4-40d0-9d50-bf5e235c2883/1-codex-plan-01a11900-ac52-77c2-81e4-70cf250b7570-01a11900-ac.md`.

| Seam | Current owner and substrate | Consumer that must bind protection |
| --- | --- | --- |
| Source release or native snapshot → retained foreign reference | Actual `ObjectRef.owner`, existing lifecycle table and guarded commit | Refresh request/admission, family seal, later independent phase or release |
| Acquisition → consumer acknowledgment | Owner token commit precedes consumer commit; no cross-table transaction is claimed | Consumer records exact immutable token references atomically with its root |
| Token → inventory | Same shared typed root traversal | Read-only protection inspection and future exact removal decision |
| Acquisition → removal reservation | Same owner view, identity predicate and guarded commit | Future removal intent; provider effects remain disabled |
| Completion reuse → new consumer promise | Existing exact-generation reuse and retained provenance | New acknowledgment checks or acquires the required active owner tokens without rewriting old completion history |

## Smallest enforceable record and transaction shape

**Reservation-ancestry refinement:** the exact retained production reservation is sufficient evidence of the owner for an already-qualified immutable reference. `reserve_attempt` and `reserve_group` commit owner/id/prefix before creating the attempt directory (`src/engine.rs:1468`; `src/engine/backfill/preparation.rs:231`). `storage::pin` hashes newly created objects and preserves inherited object owners (`src/storage.rs:471`). Validate the reservation's dataset, key/id and exact managed prefix against `workspace/objects/<owner>/<attempt>/`, plus regular-file identity, size, unavailable state and conflicting typed identities. A new ownership registry or repeat payload hash is unnecessary under this local immutable-path boundary.

The reservation proves production ownership, not successful immutable creation. A concrete distinction exists today: `copy_verified` leaves its destination after `input_digest_mismatch`, and native production can leave provisional files before pinning (`src/storage.rs:317`). Those files can satisfy ancestry, type and size without supporting a supplied digest or a completed writer. A foreign reference must therefore originate from producer-anchored retained typed creation/completion evidence, not merely a parseable or hash-valid caller-authored manifest. Path-only managed input locators must resolve to that existing evidence or refuse. An explicit test should leave a failed-copy/provisional file in a valid reservation and prove that ancestry alone cannot issue an authoritative immutable reference.

Use typed, bounded rows in the existing owner lifecycle. An immutable protection record needs a stable token identity, owner, consumer, stable consumer claim identity, and exact owned dependency scope. Scope can use retained release/snapshot identities plus their qualified inventory; it need not duplicate every object in every phase or create one transaction per file. An ordinary consumer token reference must bind the owner table identity, token identity and exact committed generation/content, not just a token name.

Choose deterministic identity from the complete owner/consumer/claim/dependency tuple or accept a stable caller identity with exact request equality checks. Deduplicate identical acquisitions and batch roots belonging to one owner in one guarded commit. A retry under the same identity with different dependencies refuses. Owner-side state is authoritative; a consumer's copied token JSON is not proof that the owner committed it.

The acquisition sequence is:

1. Resolve the explicit bounded metadata roots and real owners. Reuse complete pin inventories and existing committed dependency bindings. Reject missing/conflicting ownership evidence; do not read payloads or infer ownership from a locator alone.
2. Under an owner view, validate the exact target identity and absence of unavailable/removing state. Reuse a matching active token or append a new one through `Lifecycle::commit`. That commit uses the same owner authority the future removal reservation will use. On a stale view, recompute the decision; do not blindly replay it.
3. Reconcile an unknown response by reading the token's exact identity/content from the owner. Do not acknowledge a consumer root until that committed record is observed. If one owner succeeds and another refuses, keep the successful token; no compensating release is safe merely because the consumer attempt failed.
4. Enter the consumer's existing guarded commit with all needed tokens already durable. Record the exact token bindings atomically with the request, phase, seal or selected root they protect. Reuse a common check/helper across these paths. Token checks supplement existing eligibility and captured-input checks; they do not replace them.

Tokens should remain immutable and active in this checkpoint. Existing phase retirement must not release them automatically: a result, readback, seal, another completed phase or another consumer can still depend on the same bytes. Explicit token retirement later needs proof that all promises using that token were retired or reconciled. A token acquired before a failed consumer commit conservatively retains bytes; expose it as retained protection, not as an orphan inferred from age.

For removal, the owner's guarded decision must inspect active token protection and durably reserve the exact target as unavailable in the same state transition that later authorizes effects. Acquisition must read that exact unavailable state. If both decisions start from one owner revision, only one guarded commit may win; the loser rereads and refuses its now-invalid predicate. A mere in-memory check or an independently stored removal flag is insufficient. Defining/testing this predicate is necessary now; implementing deletion, outcomes and recovery remains separate unfinished work.

## Integration rules and invariant checks

- **Before request/phase commitment:** Refresh's first retained foreign input must obtain protection before `request/<operation>` and admission become durable. Generic completion and readback paths check bindings, including first-active-completion reuse. Do not rely on selection as the only enforcement point.
- **Before family seal commitment:** acquire as the family consumer before `generation/<id>` is written, retaining sorted lock order. Family membership validation remains unchanged.
- **At final selection:** `select_with_progress` is the shared guarded result-acknowledgment seam (`src/engine/history.rs:160`). Assert the selected release's required token bindings there. `select_refresh` retains its semantic/eligibility graph checks; protection is a separate obligation (`src/engine/refresh.rs:202`).
- **Public capture remains observation:** `capture_inputs` lacks a durable consumer identity and currently returns coverage without committing it (`src/engine/inputs.rs:88`). Keep that behavior. Use an internal acquisition operation when durable consumer admission occurs; do not create unexplained permanent tokens for every plan or capture call.
- **Transitive ownership is explicit:** protecting release C at C's authority does not by itself create protection for A's objects referenced through C. Either C already has durable upstream owner tokens that remain active with the protected root, or resolve/acquire those actual owner dependencies. Legacy references remain protected by the full inventory; their absence of tokens never proves removal eligibility.
- **Unknown history stays unknown:** newly emitted foreign roots must carry validated protection. Existing immutable manifests and phase provenance are not rewritten to fabricate past acquisition. An adoption can add new protection with its own current acknowledgment. Untyped external file locators are not proof of a managed owner; retain existing conservative inventory behavior until the removal boundary can qualify them.
- **Budget and growth:** bound combined request, decoded metadata, pending owners, tokens and token references before commits. Use the existing shared Budget and lifecycle history limits. Charge graph state once, deduplicate by exact identity, and reuse committed results. State growth is proportional to distinct retained claims/dependency sets; measure it before claiming scale. No routine full-catalog scan, payload hash or second persistent index is justified by this checkpoint.

## Adversarial checks and kill criteria

1. Race owner acquisition against owner removal reservation using two stale views directly: exactly one commit wins. On retry, acquisition refuses unavailable state, or removal refuses the active token. Repeat with unchanged owner-selected output so only lifecycle authority can fence the decision.
2. Lose owner commit acknowledgment; retry observes one identical token and one native revision. Change dependency, consumer or owner under the same identity and require refusal. Forge a consumer token reference without an owner record, with a wrong owner table UUID, altered scope, wrong generation or unknown fields; acknowledgment must refuse.
3. Stop after each owner commit and before consumer acknowledgment, including partial acquisition from several owners. Reopen: extra tokens survive; no incomplete consumer result is selected. Retrying does not add duplicate tokens or repeat payload verification.
4. Exercise Refresh request/admission, reusable processing/evidence/readback, family seals and final selection. Direct source and transitive dependency invalidation remain correctly guarded; protection never makes ineligible inputs eligible.
5. Build a dependency graph with foreign semantic inputs, composition/history/change references and shared objects. Remove one consumer or retire one phase; every independent surviving owner claim continues protecting the exact bytes. Unknown legacy references stay conservative.
6. Exercise opposing consumers and owners with reversed IDs while a maintenance writer queues. No nested shared admission or dataset-lock inversion; independent owners remain concurrent.
7. Hold data fixed while varying consumers and exact dependency sets. Record owner commits, retry commits, metadata bytes/requests and admitted memory. Identical retry should do no new owner commit; large batches refuse before partial consumer acknowledgment. Zero per-row transactions and zero payload scans are explicit acceptance checks.

**Kill criterion:** reject the implementation if either stale decision can acknowledge a reference after removal wins, if selection accepts a consumer-supplied token without owner evidence, or if foreign owner closure requires a global catalog/payload scan for every ordinary admission. Those failures invalidate the proposed safety or resource benefit rather than merely suggesting a later optimization.

## Verdict and limits

**APPROVE this owner-first, guarded two-step design for implementation, subject to the checks above.** The user-visible benefit is that future cleanup cannot invalidate an already acknowledged cross-dataset dependency; interrupted acquisition loses capacity temporarily rather than losing retained work. The concrete simplification is reuse of the existing lifecycle guard, metadata inventories and shared budget, with no new transaction coordinator or registry.

No new implementation has been qualified by this document. It does not complete removal intent/unavailability execution, exact deletion outcomes, reservation closure, token retirement, release expiry, compaction, provider fencing, restore, metadata-growth qualification, the Comments performance target or full R1–R7. The existing global inventory remains necessary for legacy/shared roots and independent protection; owner tokens supply durable acquisition authority rather than replacing that inventory.

## Next integration, grounded in the implemented kernel

The acquisition/seal checkpoint is reviewed separately in
[owner-protection-review.md](owner-protection-review.md). Follow-up source tracing
identified these requirements for completing foreign-reference integration:

- Acquire before the initial operation/request commit in `prepare_and_select`
  and before operation/request/capture in `prepare_backfill`. `admit_refresh` is
  too late. Reconcile existing work first, acquire outside every consumer/source
  lock, then reacquire the existing lock set and repeat admission guards.
- Include every based operation's exact base, direct refresh inputs and every
  member of captured producer seals. `captured_manifest` alone omits based Build
  and Update. Inherited computed groups, old coverage and history belong to that
  closure. Reuse captured coverage during admission.
- Propagate exact existing tokens into independently retained completions and
  readbacks, preserving the original binding of a reused or concurrently winning
  generation. Extend binding references from one token per owner to sorted,
  deduplicated `(owner, token)` pairs, and check required roots against their
  union. Group reads by owner; avoid minting a union grant or walking dependencies
  again at every phase.
- A newly selected sibling composition, the actual predecessor at selection, or
  `input-progress.observed_head` can add dependencies after admission. Inventory
  currently retains `prior`, `target` and `observed_head` manifests. Acquire the
  missing protection outside locks and recheck the exact final selection after
  relocking; admission's token set cannot silently stand in for these later roots.
- Managed `InputSpec.path` retains a locator before copying but has no owner or
  byte count. Resolve and protect its qualified managed-object identity before
  request admission, or refuse that managed-reference case. A path and supplied
  digest alone do not establish ownership or completed creation. External input
  locations remain outside managed owner-token authority.

These are remaining implementation obligations, not guarantees established by
the current seal-only integration. Keep root pins and copied token references
charged for their actual lifetime and retain conservative legacy handling.
