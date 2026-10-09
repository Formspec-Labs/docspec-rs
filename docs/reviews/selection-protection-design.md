# Selection protection: bounded design review

Static review, 2026-10-08. No Cargo commands or runtime qualification were run
for these notes. References below were checked against the working tree while
the implementation and tests were in progress.

**Decision: approve carrying retained protection into immutable selected results
and succession records. Resolve authority through its exact immutable result;
reject a separately stored authority binding as duplicate work. The authority
relationship checks and input-progress acquisition remain unfinished. This
decision does not establish full selection or removal readiness.**

The user benefit is preserving exact reusable results and the current sibling
composition during backfill. This increment extends the existing phase protection
mechanism; it adds no owner acquisition, payload verification, or dependency walk
at selection. The governing retention requirements are the shared inventory and
prevention of new references to removing objects in
[R6](../requirements/06-maintenance-retention.md#retention-and-deletion).

## Keep the immutable increment small

- Carry `Retained<ReleasePin>` through selection instead of discarding the actual
  readback's scope: `src/engine.rs:1510`, `src/engine.rs:1514`.
- Bind `result/<id>` using that readback scope. Bind `succession/<id>` separately
  using the result scope plus the actual selected predecessor's scope from the
  same guarded lifecycle view: `src/engine/history.rs:199`,
  `src/engine/history.rs:244`, `src/engine/history.rs:251`.
- Check a bound predecessor's retained result against the selected release before
  inheriting its scope: `src/engine/history.rs:204`. Unknown predecessor provenance
  leaves succession protection unknown; it does not erase known result scope.
- Keep the predecessor-only scope out of the result binding. Otherwise later
  results recursively inherit unrelated prior selections' grants, increasing
  metadata and retention without improving their own material dependencies.
- Use the existing atomic selection commit and completion-binding helper.
  Backfill already reads the current result's scope and combines it with the
  prepared target, so bound results complete that handoff:
  `src/engine/backfill.rs:585`, `src/engine/backfill.rs:603`.

## Resolve authority through its immutable result

`authority` changes during admission and invalidation as well as selection:
`src/engine.rs:679`, `src/engine.rs:1109`, `src/engine/history.rs:243`.
Existing binding validation requires an exact claim revision and digest:
`src/engine/retention/owners.rs:252`, `src/engine/retention/owners.rs:256`.

Adding a binding only at selection would leave it stale after the next authority
rewrite. Replacing a known selection with unknown protection would also need a
way to supersede its old binding. Those problems come from duplicating the same
protection promise: authority carries the exact release already stored in the
immutable result, and selection writes them together.

The smaller design treats authority as a checked reference to that result. Within
one lifecycle view, require all of the following for a selected release:

- `authority.release` equals the full `result/<release.id>` value, and
  `authority.selected` equals that result's dataset.
- The result's native record revision equals `authority.selection_revision`.
- That positive selection revision is no newer than the authority record's native
  revision, which is no newer than the inspected view.
- Dataset, version, owner, and verified-release identities agree. An empty
  selection has no selected dataset, release, or selection revision.

Admission and invalidation may advance the authority record while preserving
these relationships. They need no new protection record. A newly selected result
with unknown protection remains unknown without inheriting its predecessor's
binding or writing a tombstone. Missing or contradictory result identity cannot
qualify authority through some other result; retain conservative handling or
refuse malformed metadata.

Enforce the relationship in the existing authority branch of the shared root
walker, `src/engine/retention/inventory.rs:852`. It currently checks only agreement
between authority's own selected dataset and release before expanding the
release. Both full inspection and shallow root discovery use this walker
(`src/engine/retention/inventory.rs:230`,
`src/engine/retention/inventory.rs:285`). The snapshot revision bounds already
checked by their callers remain necessary.

Full inspection separately validates stored result bindings and deduplicates
their owner scopes (`src/engine/retention/inventory.rs:179`,
`src/engine/retention/inventory.rs:395`). Reuse that path; do not synthesize another
binding under the authority key or reopen owners for its duplicate release.
If an application later needs the selected protection scope directly, resolve
this same checked relationship and call `retained_protections` for the result
key. A missing result binding returns unknown through the existing API
(`src/engine/retention/owners.rs:408`).

Future result/token retirement must preserve this invariant: a selected result
and its required grants remain live while authority references that exact
generation. Current phase retirement does not include result records
(`src/engine/retention/phases.rs:19`). R6 requires protected roots and deduplicated
references; it does not require one durable binding for each duplicate copy of
the same selected release.

## Observed input heads need owner-first acquisition

Captured refresh can observe a producer head newer than its target:
`src/engine/refresh.rs:299`, `src/engine/history.rs:117`. Inventory follows the
prior, target, and observed-head manifests from the progress record:
`src/engine/retention/inventory.rs:932`. Captured request protection alone cannot
certify a newly observed head's dependencies. The current increment leaves
`input-progress/<id>` unbound: `src/engine/history.rs:258`.

Future acquisition must occur outside consumer/input locks, followed by a fresh
guarded decision. Compare the selected identities, eligibility, and required
family membership/seals; take the final observation revision from the guarded
state. Do not treat an unchanged producer lifecycle revision as the definition
of an unchanged selected head: creating an owner grant itself advances that
revision (`src/engine/retention/owners.rs:625`,
`src/engine/retention/owners.rs:657`). An exact-revision comparison therefore
invalidates its own preparation and creates avoidable retries. Identical grants
are reusable (`src/engine/retention/owners.rs:633`), so this does not by itself
prove an infinite retry loop.

## Qualification and next work

Exercise atomic result/succession bindings, an intervening actual predecessor,
known result with unknown predecessor, current sibling backfill composition,
lost-response/reopen reuse, and admission/invalidation after selection. Check
that propagation introduces no owner commits or payload work. These are required
checks for the implementation review, not results claimed by these notes.

The current source contains result/succession binding cases in
`src/engine/retention/owners/phase_tests.rs:252` and `:287`, an unknown predecessor
case at `:353`, and selected-history corruption cases in `tests/succession.rs`.
Those do not establish the proposed authority relationship in retention
inspection. Add focused cases for a selected bound result after admission and
invalidation rewrites; a known-to-unknown selected result; missing/mismatched
result content; and a result revision different from `selection_revision`.
Inspection must retain its existing no-payload-read behavior, demonstrated by
`tests/retention_inspection.rs:37`. No test results are claimed by this design
follow-up.

Then implement the authority relationship checks and actual progress-root
acquisition. Token retirement, removal reservations, exact deletion/reconciliation,
and their failure qualification remain separate unfinished requirements. Keep
physical removal and automatic expiration disabled.
