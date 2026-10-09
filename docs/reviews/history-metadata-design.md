# Keep ordering evidence separate from retained content

## Decision and correction

Do not acquire complete release payload closures merely because a succession or
input-progress record names a version. Preserve exact ordering and observation
metadata in the catalog. Protect content through the independent promises that
actually require it to remain readable or reusable.

This requirement-grounded correction **supersedes the earlier guidance to acquire
newly observed head closures before acknowledging input progress**, including the
observed-head acquisition section of
[selection-protection-design.md](selection-protection-design.md). It also
supersedes treating succession bindings as a required permanent content root.
The earlier guidance followed the existing inventory expansion without first
challenging its retention semantics. The checked authority-to-immutable-result
relationship and propagation of actual result protection remain useful.

This is a static design review against baseline `7e94506` and the current working
tree. No implementation edits, Cargo commands or runtime qualification were
performed for this review. File references describe the inspected implementations;
the in-progress source may move their line numbers.

## Requirements and present behavior

- [R6](../requirements/06-maintenance-retention.md:9) gives historical provenance,
  retained content, optional acceleration and recovery support distinct
  lifetimes. Its retention section says history is not automatically permanent
  and requires explicit policies and fresh eligibility before deletion
  (`:23`, `:29`).
- [R4](../requirements/04-change-tracking.md:27) describes accepted succession as
  a separate, minimal catalog relationship. Retain enough evidence to establish
  order for selected coverage.
- [R5](../requirements/05-dependent-refresh.md:29) requires accepted source
  history for nonregression and exact covered inputs with observed lag (`:31`).
  It does not require the payload of every intermediate or observed version.
- `history::follows` reads one retained lifecycle view, checks accepted revision
  ordering, and returns endpoint identities and counts (`src/engine/history.rs:44`).
  `read_link` checks the succession/result records and their common native
  revision (`src/engine/history.rs:325`). Neither opens external release JSON or
  data objects. The public proof documentation explicitly excludes payload
  availability (`src/engine/history.rs:9`).
- The existing inventory expands `succession` and `input-progress` references
  through `self.release`, which recursively discovers release content. That is
  stronger than the ordering and observation records require. Permanent owner
  grants make the resulting over-retention persist beyond the observation.

Full-closure protection is conservative while removal is disabled. It is not a
sound final lifetime rule: an ordering chain can remain necessary while its
intermediate payload versions are eligible for retirement. Requiring that chain
to retain all their payloads defeats the independent retention windows in R6.

## Which promise retains which objects

| Record or obligation | What must remain available |
| --- | --- |
| Selected or explicitly retained result; retained phase; published availability window; reader or restore point | Its promised content, interpretation and required evidence, including exact dependencies. Existing content protection remains in force. |
| Succession relationship | Exact selected/predecessor identities and the catalog evidence establishing accepted order. Naming a manifest does not by itself require opening or retaining that manifest's content. |
| Input-progress observation | Exact prior/target/observed-head identities, observation revision and reported ordering/lag facts. Actual consumed inputs remain protected by the retained result or phase that requires them. |

The same version can have both kinds of obligation. Removing the ordering record
from payload reachability does not remove a separately retained result's promise.
Likewise, this decision does not weaken required source or contributor evidence,
schema interpretation, scalar binding history or supported recovery.

## Smallest complete lifecycle path

1. Stop recursively expanding succession/progress endpoint references into
   payload roots and stop creating new payload grants solely for those records.
   Keep the metadata records in their existing guarded selection commit.
2. Continue binding actual retained results to their consumed protection scopes.
   Resolve mutable authority through its exact immutable result. No second
   mutable cursor, observation scheduler or durable progress-acquisition phase is
   needed.
3. Implement explicit retirement of result/readback availability separately from
   ordering evidence before enabling removal. Initially retain the existing
   immutable result rows for lightweight identity decoding, and record retirement
   separately. This preserves their original native revision for `read_link`;
   there is no need to invent a new receipt store now. Historical reads must
   expose retirement instead of implying that content remains available.
4. Retire owner grants only when every live content obligation permits it. Old
   conservative grants are not deletion authority and must not be silently
   removed. Shared references, selected descendants, phase recovery, publication
   and restore promises still block reclamation. Keep physical removal disabled
   until this and the existing reservation/reconciliation protocol are qualified.

The catalog's own metadata durability and required ordering-record lifetime remain
obligations. This correction does not authorize catalog history truncation.

## Minimum metadata integrity checks

Use cached lifecycle rows and small typed decoding. Share local predicates with
`read_link` where practical; do not call `prove_succession` for every inventory
record or add a second historical traversal. Do not resolve external manifests,
hash payloads or acquire owners for these checks.

For `succession/<version>`:

- The key, selected dataset/version, manifest owner and containing lifecycle table
  agree. Validate the recorded object identity's shape without opening its path.
- The matching immutable result receipt exists in the same local view and native
  revision. Its release/version/manifest identity, verified flag, boundary and
  eligibility agree with the succession record. These are the existing
  `read_link` checks, with the inventory's key/table identity added explicitly.
- A predecessor, when present, belongs to the same dataset and is not the selected
  version. Equal dataset/version identities cannot name conflicting manifests.
- Native record revisions remain positive and bounded by the inspected view, as
  the inventory caller already checks. Full ancestry order, missing links, branch
  membership and cycle detection remain the job of `follows` when order is
  actually needed. An explicit inventory may check a direct predecessor receipt
  through keyed lookup, but it must not walk the whole chain separately per row.

For `input-progress/<operation>`:

- The key names a related result committed in the same consumer revision. Its
  dataset/version identity agrees with the containing table. The progress input
  keys and exact targets equal that result's captured input coverage.
- Each prior, target and observed head has the map entry's dataset and a matching
  manifest owner. A reused dataset/version identity must have the same full
  `VersionRef`, including manifest identity.
- `observed_revision` is positive. It belongs to the **source** lifecycle: never
  compare it with the consumer's selection revision or require it to equal the
  source's later current revision.
- Zero advancement agrees with identical prior/target identities; zero lag agrees
  with identical target/observed-head identities. Use checked arithmetic if
  combining the counts. These are consistency checks, not an independent proof of
  the stored counts or of historical selected-head membership.
- Preserve the existing creation-time guarded `follows` proof. If prior coverage
  is cross-checked locally, use the operation's actual rule: Refresh compares
  previous selected coverage, while the current related Backfill path proves
  target-to-target. Do not invent a stricter universal prior rule.

A deliberately forged but internally consistent observation cannot be disproved
by local shape checks alone. It also cannot authorize payload deletion, revive a
retired result or replace the live eligibility checks. Re-proving external history
belongs to explicit diagnosis or an operation that needs that proof, not routine
inventory parsing.

## Decisive qualification

Retain A1 for a dependent result, advance the producer through A2 to A3, and
explicitly retire only A2's content. A1-to-A3 ordering must remain provable; A1's
required inputs/evidence and A3's selected data remain readable; unused A2 bytes
become reclaimable; an A2 historical read reports explicit retirement. An observed
lag reference alone must not prevent that outcome.

Before removal exists, narrow tests can establish the metadata boundary: ordering
proofs and progress parsing do not open external release/payload paths; malformed
keys, identities, revisions and inconsistent counts refuse; independent retained
content roots still protect their objects. These tests must not be presented as
completed physical-retirement qualification.
