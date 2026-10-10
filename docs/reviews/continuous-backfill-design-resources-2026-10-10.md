# Continuous backfill: resource and recovery design review

**Keep exact captured backfills and their shared file completions. Add a focused
catch-up service with finite rounds and a durable, generation-checked admission
fence. Qualify successful cutover and bounded refusal separately.** A retry loop
alone cannot guarantee convergence; a deadline alone cannot authorize selection.

This static review covers R3 continuous ingestion at
`8566235f32abc90ea21cfbd4b75f610766cfdc2d`, dated 2026-10-10. It applies the
semi-formal architecture review and Rust guidance. The working proposal is a
continuous service that retains exact child backfills, reuses completed files,
tries finite catch-up rounds, and then fences new source admission for a bounded
attempt. Existing source requests may finish. Any unresolved accepted source
request blocks selection; expiry preserves completed work and releases admission.
No source edits, builds, tests, benchmarks, or corpus scans were performed.

## Findings

### F1 — Require a complete pending-input proof, not a maximum sequence check

**BLOCKER for a design using only the admission high-water mark; RESHAPE as proposed.**
Category: intent versus implementation; location: admission and final cutover.

`authority.boundary` advances when the request is registered
(`src/engine.rs:1114–1150`). Input copying, hashing, and retained admission completion
happen afterward (`src/engine.rs:801–875`). A source update also binds its original
base and definition (`src/model.rs:817–827`), and retry checks those before resuming
the operation (`src/engine.rs:1025–1034`). A selected larger sequence does not prove
that every earlier request was incorporated, and a registration is not proof that
input bytes are durable.

At fence capture and final selection, classify every retained source operation
from the same catalog view: selected, explicitly abandoned, or unresolved. Do
not inspect only the gap above selected coverage. Preserve and report an unresolved
operation even when its sequence is below the latest selected sequence. Validate
the operation/request identity and terminal decision binding, using the existing
abandonment proof as a model (`src/engine/retention.rs:23–104`). Source operations
include every request kind that can change source state, not just `UpdateSpec`.

Unresolved requests block cutover until they complete or their owner explicitly
resolves them. Do not automatically abandon them, rewrite their base intent, or
change their definition. The fence may end with a structured refusal naming the
blocking operations. Reject new source requests before registration while the
fence is active; a refused request is not accepted work. Retained pre-fence inputs
and completions remain under the existing protection rules.

The proposed all-outstanding scan meets this requirement. It has O(H) metadata
work for H retained operations, not constant-time authority checking. Iterate the
existing `SnapshotRows::prefix` without cloning the map; decode one admitted record
at a time and bound diagnostics (`src/catalog.rs:62–98`). If this exceeds the
declared fence budget, fail the attempt with retained progress; do not introduce
another pending-work database to disguise the cost.

### F2 — Bound admission delay with a durable fence identity and terminal behavior

**CONCERN; RESHAPE the fence into an explicit guarded state transition.**
Category: lifecycle seam; location: source admission, cutover, timeout, and restart.

The current backfill guard intentionally requires the exact source, source
boundary, target group and captured admission boundary
(`src/engine/backfill.rs:1027–1073`). The required replacement is not a mutex held
while processing. Preserve short dataset locks and conditional native commits.

Retain one active fence record with its owner operation, unique attempt generation,
captured authority epoch and expected definition, deadline, and cutoff admission
boundary. Every new source admission checks it before adding operation metadata.
Every final cutover checks the same unexpired generation in the same guarded
selection commit. Expiry, cancellation, conflict and restart leave the candidate
recoverable and make future admission possible. An expired worker cannot select,
clear a successor fence, or renew itself indefinitely. A lost selection response
first resolves the retained result rather than acquiring another fence.

Use the existing single local workspace owner and dataset commit machinery
(`src/budget.rs:173–199`; `src/catalog.rs:342–408`). A clock timeout is a trigger
for a guarded transition, not sole ownership proof; R1 expressly requires this
distinction (`docs/requirements/01-durable-storage.md:19`). Define restart and
clock-discontinuity behavior so an old timestamp cannot silently extend a pause.
Ensure metadata exhaustion cannot leave an active fence that requires an
unavailable cleanup commit before admission can resume.

The service should not wait for blocked input until after the entire pause budget
has been spent. Check unresolved input first, check elapsed budget between
bounded work units, and retain an actionable refusal. Existing native I/O is not
forcibly terminated by ending a fence. Actual foreground delay therefore includes
lock scheduling and in-flight commit latency; measure it before claiming a hard
wall-clock ceiling.

### F3 — Finite safe refusal is valid; unconditional convergence is not established

**OBSERVATION; KEEP the bounded completion-or-refusal policy.**
Category: user value and scope; location: catch-up control flow.

Before fencing, a candidate advances only when its effective processing rate
exceeds the rate at which ingestion invalidates or adds required work. Faster
arrival, repeated corrections to a hot file, or an unresolved conflicting update
can prevent convergence. This follows directly from the strict capture guard and
file-level preparation (`src/engine/backfill.rs:1050–1058`;
`src/engine/backfill/preparation.rs:148–254`).

Finite rounds plus a bounded fence can guarantee a bounded attempt and preservation
of acknowledged work. It cannot guarantee successful cutover for arbitrary
unfinishable input. This is consistent with R3's prohibition on indefinite locks
and dropped accepted work (`docs/requirements/03-schema-backfill.md:43–47`), provided
qualification also demonstrates a successful cutover with finite, drainable input
while new requests continuously attempt admission. A fixture that only refuses
safely does not establish the user outcome.

### F4 — Exact-file reuse is implemented; rewritten-file semantic reuse is not

**CONCERN for a claim of complete selective R3 reuse; KEEP conservative behavior
and state the boundary.** Category: material dependencies and scope.

The material key includes the immutable source `ObjectRef` plus expression
dependencies (`src/engine/backfill.rs:1127–1131`). Expression dependencies use
consumed stable field IDs and their interpretation, remove display names and future
write defaults, and include schema documentation when the expression observes it
(`src/engine/backfill/expressions.rs:136–142`, `:211–225`). This correctly reuses
unchanged files across unrelated schema changes and new source selections.

It does not reuse computation when a physical file is rewritten, even if every
consumed value is equal. `prepare_group` recomputes each missing file completion
(`src/engine/backfill/preparation.rs:148–254`). Existing supplemental dependency
qualification requires the original source objects to remain present and exact
(`src/engine/backfill/origins/checks.rs:92–123`); it is not a general cross-file
correspondence proof. Stable occurrence fields survive supported physical rewrites,
but identity alone does not prove material values stayed equal
(`src/storage/occurrences.rs:19–22`, `:177–201`).

Do not replace `file_key` with a business key, row count or unaudited value hash.
If semantic reuse is added, retain an explicit association from new source coverage
to the original execution and prove consumed-field interpretation, typed/null-aware
values, occurrence membership and multiplicity. Reuse the existing origins and
qualification seam; keep bulk correspondence in a bounded native relation. A changed
material input must miss this proof. New occurrences with equal displayed values
require a new binding; they do not inherit an old occurrence identity.

For this continuous-control increment, conservative recomputation is the simpler
safe choice. Its concrete constraint is physical file granularity, so this increment
does not close R3-T04's broader “only semantically affected work” acceptance. A
measurement must compare correspondence-read/join/storage cost with avoided scalar
evaluation before adding a more expensive reuse path for cheap scalar expressions.

### F5 — Keep bounded native preparation; do not replicate population-sized state per round

**CONCERN; RESHAPE only the new coordination metadata.** Category: resource use.

Existing preparation first resolves a whole prepared group, then resolves exact
file completions before reading missing source files
(`src/engine/backfill/preparation.rs:89–113`, `:148–170`). Canonical acknowledgments
return the winning generation and its protections, not the losing attempt
(`src/engine/backfill/preparation.rs:491–581`). Assembly shares output objects and
creates a native table once (`:288–350`). These are the right reusable primitives.

Keep child requests immutable and store compact references to their captured source
and completed result in the continuous operation. Do not copy full inventories,
group pins, pending rows or original file declarations into every control record.
Bound both catch-up rounds and retained control-history growth. Repeated rounds
still perform O(F) metadata inventory/assembly work for F source files, and repeated
lifecycle commits clone the current metadata map (`src/catalog.rs:349–359`). Bundled
file acknowledgments reduce commit frequency; they do not remove that growth.

`compute_file` currently reads all source columns through `read_file`
(`src/engine/backfill/preparation.rs:607–612`). The maintained adapter already has
`read_projected_file` (`src/storage/occurrences.rs:354–401`). A small follow-up can
compile expressions against the consumed-field projection and use that reader,
preserving stable occurrence columns and field-ID interpretation. Do not add a
parallel Parquet reader or claim narrow payload reads from the dependency key alone.

## Artifact summary and traced lineage

The beneficiary is an operator adding or replacing a computed group while source
ingestion continues. Input is an exact selected source, immutable group definition,
and already accepted source operations. Output is a complete selected combination
with the new group definition, or a bounded recoverable refusal. This is a product
capability supported by lifecycle and qualification work.

| Prior artifact | Relationship | Evidence |
| --- | --- | --- |
| Accepted plan | One native catalog authority, reusable completed phases, bounded processing | `docs/PLAN.md:23–33`, `:44–57`, `:69–87` |
| R1 | Defines durable acknowledgment, exact retry, shared phases and protection | `docs/requirements/01-durable-storage.md:9–23`, `:27–42` |
| R3 | Defines candidate coverage, catch-up, strict latest cutover, accepted later input and bounded fence | `docs/requirements/03-schema-backfill.md:33–47`, `:53–65` |
| R4 | Supplies optional certified changes; invalid present evidence refuses, missing acceleration falls back | `docs/requirements/04-change-tracking.md:9–27` |
| Q2, Q4, Q7 | Require concurrent groups, corrected dependencies and history/resource scaling | `docs/requirements/07-operations-qualification.md:93–115`, `:133–157` |
| Earlier resource design | Established per-file identities, bundled completion and no cumulative pin per file | `docs/reviews/backfill-design-resources.md:15–67` |
| Current status | Explicitly leaves continuous catch-up/cutover and broader selective reuse unfinished | `docs/STATUS.md:1938–1944` |

No separate ratified continuous-ingestion design was found. This review refines
the existing R3 requirement and the working proposal; it does not redefine R3 or
approve a completed implementation.

| Component / seam | Owner | Existing mechanism and consumer |
| --- | --- | --- |
| Source admission | Shared engine service | Exact request/base/definition plus retained input phase; new fence check belongs before registration (`src/engine.rs:990–1150`, `:801–875`). |
| Catch-up rounds | Focused backfill service | Exact child `BackfillSpec`, shared `prepare_group`, retained validation/readback; continuous API drives these services. |
| Group preparation | Shared scalar service | Same helper runs standalone backfill and coordinated source updates (`src/engine.rs:1441–1484`). |
| Change discovery | Shared R4 service | `change_chain` already bounds links and metadata; existing refresh consumes old and new raw keys (`src/engine/changes/chain.rs:44–202`; `src/engine/incremental/history.rs:59–85`). |
| Final selection | Shared lifecycle/history | Guarded selection preserves a complete source/group combination; schema writer-definition change occurs in that transaction (`src/engine/history.rs:142–238`). |
| Recovery/retention | Shared phase and protection services | Retained generations and exact owner protections remain valid after the control attempt fails (`src/engine/backfill/preparation.rs:515–581`). |
| Consumer verification | Shared native/combined readers | Routine metadata checks versus explicit full reconstruction remain distinct (`src/engine/backfill/verification.rs:522–553`). |

## Smallest complete path

1. Admit one immutable continuous intent: target group, initial expected group
   definition, source writer definition, dependency eligibility, finite catch-up
   policy and fence budget. Resolve its retained result before doing new work.
2. Capture the latest eligible selected source. Retain an exact child request with
   a stable retry identity before computation. Reuse `prepare_group` and existing
   acknowledgments; missing files alone execute. A full endpoint file inventory is
   a valid conservative catch-up path, including deletion and one-to-many shrinkage.
   It does not need a second change queue. Use the existing qualified R4 reader only
   when it avoids measured work; missing optional changes do not make state unreadable.
3. After child completion, attempt existing strict promotion. A source advance
   creates the next exact child within the round limit, preserving prior completions.
   A competing change to the target's definition, dependency eligibility or writer
   definition refuses. Source refresh of the same old group definition is distinct
   from a competing definition change; do not blindly reset that expectation every round.
4. If needed, acquire one bounded durable admission fence. New source requests
   receive an explicit refusal before acceptance. Allow admitted work to finish
   under existing authority checks, then classify every outstanding source request.
   A blocked request produces recoverable refusal rather than hidden rebasing.
5. Capture the final drained source and run the exact final child with retained
   reuse. At selection, atomically recheck fence generation/deadline, source and
   required upstream heads, writer definition, target expectation, complete coverage,
   dependency corrections, and the absence of unresolved source requests. Select
   the complete group combination and its definition, and end the fence together.
6. The next source write uses the selected computed definition through the existing
   coordinated preparation path. Old prepared source operations fail their exact
   base/definition checks. Source-schema edits additionally use the existing atomic
   `next_definition` behavior; group changes need no second writer registry.
7. On any exit, retry resolves the recorded child/result identities. Expired or
   ended fence attempts cannot mutate a successor. Input admission resumes without
   deleting candidate files, source admissions, readbacks or their protections.

For a group over related output, do not weaken required upstream latest-head and
eligibility guards. A fence on the output dataset does not stop upstream producers;
those producers may force a bounded refusal. Successful multi-dataset convergence
requires a declared coordinated boundary, not an accidental claim from a local
mutex (`docs/requirements/03-schema-backfill.md:45`; existing dependency locks and
ancestor checks at `src/engine/backfill.rs:873–902`).

## Resource model and tradeoffs

These are source-derived cost bounds and explicit unknowns, not measurements.
Let F be live source files, U be files missing qualified completions, H be retained
source-operation records, C be catalog records, R be the bounded number of rounds,
B be `max_batch_bytes`, and M be `max_metadata_bytes`.

| Stage | Runtime/I/O | Memory and disk consequence |
| --- | --- | --- |
| Exact-file catch-up | O(F) metadata traversal plus read/compute/write for U files; no payload read for a qualified exact-file hit | Per-round native metadata and origins are O(F), bounded by M. Payload output is proportional to newly computed files, not all source files. |
| Repeated catch-up | At most R endpoint traversals; worst-case rewritten hot files execute each round | Old and new completed generations coexist. Finite R bounds one invocation, not indefinite repeated operator retries. |
| Pending-input proof | O(H) metadata classification per fresh proof | Iterate admitted records; no row payload, input copying, or new persistent index. Fence latency can grow with H. |
| File acknowledgment | Bounded bundles, currently `COMPLETION_BUNDLE = 16` or M/8 serialized pending metadata | Each catalog commit clones O(C) current metadata and appends lifecycle history; file output is shared (`src/engine/backfill.rs:20–22`; `src/engine/backfill/preparation.rs:254–272`; `src/catalog.rs:349–401`). |
| Scalar execution | Stream one source file in batches; current path decodes its whole schema | Explicit simultaneous reservations include 2B expression allowance, 2B reader allowance, 4B writer allowance and M writer metadata, plus preparation/verification metadata. This is admission accounting, not aggregate RSS (`preparation.rs:607`; `occurrences.rs:397`; `storage.rs:245–246`). |
| Runtime sharing | Uses existing DataFusion runtime and blocking-worker permits | Runtime gets half the configured memory; application reservations get half; scratch has the existing cap (`src/budget.rs:85–96`, `:130–169`). No independent backfill thread pool. |
| Validation/readback | New output creation checks and retained validation; routine assembled checks stop before full value reconstruction | Keep explicit deep verification outside the production fence; it has full source/group read and reconstruction costs (`src/engine/backfill/verification.rs:531–553`). |

The scalar payload path alone therefore reserves 8B plus writer metadata while
reader and writer overlap; assembly separately reserves 2M
(`src/engine/backfill/preparation.rs:132–134`). Do not multiply a guessed per-worker
RSS by thread count and call it a process bound. Native decoder, compression,
expression expansion, retained backing arrays and catalog caches still need live
whole-process measurement.

Peak additional disk is the sum of newly retained input copies, new scalar output
across completed and in-flight rounds, source rewrite/change tables from concurrent
ingestion, new native/catalog/origins metadata, and scratch/spill. Existing source
and group files remain protected while new files coexist. Exact-file reuse adds
references, not another copy of their bytes. Current per-file preflight is twice
source bytes plus declared scratch (`src/engine/backfill/preparation.rs:596–605`);
that is not a hard upper bound for expanding expressions. Measure and check live
free space during writing. Report retained bytes and peak allocated bytes separately
from volume-wide free-space change. Do not use retirement to erase failed-attempt
evidence or promised completed work merely to make amplification look smaller.

## Invariants and commitments

| Invariant | Status | Evidence and failure mode |
| --- | --- | --- |
| One catalog selects exact source, schema, evidence and group definitions | RELIED UPON | Plan `:23–33`; separate candidate head selection would expose mixed coverage. |
| Acknowledged completion survives failed parent/selection | PRESERVED by existing shared machinery | R1 `:29–40`; replacing child results loses recoverable work. |
| Admission sequence is not source coverage | RELIED UPON | Registration precedes input retention; treating a high-water mark as consumed input loses requests. |
| Old-definition workers cannot select after cutover | RELIED UPON and checked anew | R3 `:45`; existing base/definition/epoch guards at `src/engine.rs:1945–1971`. |
| Complete coverage includes null/empty outcomes and excludes deleted occurrences | PRESERVED only with exact final child | R3 `:39–45`; final group must bind the actual target source, not a prior child. |
| Reuse preserves original execution and adequate material dependencies | PRESERVED for exact-file reuse; broader reuse UNQUALIFIED | R3 `:15–19`, `:33`; changing completion identity would falsify provenance. |
| Foreground blocking ends within the declared supported budget | NEWLY INTRODUCED behavior requiring measurement | R3 `:45`, `:65`; unguarded expiry or restart can create indefinite denial or stale selection. |
| R5 captured refresh remains distinct from strict R3 cutover | PRESERVED | R3 `:47`; sharing preparation does not justify imposing latest-head policy on routine refresh. |

## User value and counterfactuals

The proposed addition pays down operator coordination work: one intent advances
through captured sources, reuses expensive work, and either yields a complete
group or identifies the exact input preventing cutover. It adds only focused
control state to the existing lifecycle. A new scheduler, per-row catalog journal,
SQL index, or separately managed queue would add authority and recovery seams
without being needed for the bounded-refusal behavior.

The counter-decision is manual exact child retries. It remains correct but leaves
convergence, pause limits and blocking-input diagnosis to every caller. Removing
the fence leaves correctness intact while allowing repeated starvation under
continuous admission. Removing retained child/file results first breaks worker-loss
recovery and makes each retry more expensive. Existing preparation and catalog
services already subsume the data work; they do not subsume the continuous intent
or bounded admission decision.

Kill this design if qualification shows that an accepted input disappears, a stale
fence owner selects, source admission remains denied after the attempt ends, or
fixed-size catch-up repeatedly decodes unchanged payloads. A likely future concern
is O(RF) origins/inventory metadata and O(H) pending-operation classification. Measure
those growth dimensions before adding indexes or general workflow infrastructure.

## Meaningful qualification evidence

Existing test bodies establish useful seams, not new execution evidence from this
review: `tests/backfill.rs:328–473` retains file generations across a stale capture
and exact catch-up; `:172–325` covers retained/reopened and killed-worker handoffs;
`src/engine/backfill/file_completion_tests.rs:201` covers canonical acknowledgment
and winner protection; `tests/backfill_dependencies.rs:175–191` covers empty and
legacy recovery boundaries. None establishes sustained-ingestion cutover.

Required focused tests should prove these externally observable outcomes:

- Continuous attempted writes with a finite drainable accepted set: final combined
  values, occurrence membership, null/empty outcomes, evidence and group definition
  equal an independent reconstruction of the exact cutover input.
- An earlier unresolved admission below a later selected sequence blocks cutover;
  timeout preserves its retained input and original request, releases the fence,
  and returns the exact blocker. Also distinguish registered-but-not-retained input.
- Inserts, corrections, removals, one-to-many shrinkage and reversions between
  rounds: no stale rows or lost members. A same-group competing definition refuses;
  compatible siblings and receipt selections remain composed.
- Pause at fence acquisition, child file acknowledgment, validation, readback and
  final selection; lose responses and kill/reopen. Every retry returns its original
  selected result or reuses qualified retained work without duplicate computation.
- Expiry races with new admission and final selection; a prior fence worker cannot
  clear or select under a successor generation. Include cancellation, capacity
  exhaustion, restart and deliberately slow commit/read paths.
- Dependency correction after reuse lookup refuses stale adoption. Exact unchanged
  files keep their completion generation and have zero scalar execution; renamed
  fields keep stable binding. Physically rewritten files are either explicitly
  recomputed or covered by a separately qualified correspondence proof.
- A retained optional change chain is missing, over limit or corrupt: correct
  endpoint fallback for the first cases, explicit refusal for corrupt present
  evidence. Qualification should not require an accelerator for correctness.

Before performance collection, freeze ingestion rate, round/fence limits, workload,
background load, cold/warm conditions and the foreground target. Hold update size
fixed while independently growing F and H. Record attempted versus accepted
requests, cutover success/refusal, blocked time, catch-up lag, source bytes decoded,
computed/reused files, metadata requests/bytes, catalog commits, peak RSS, scratch,
peak additional allocated disk, and retained disk. Include the cost of old/new
coexistence and failed rounds. The full gate remains required for implementation;
passing these fixtures does not by itself qualify the broader R3/Q2 performance scope.

## Verdict

**APPROVE the focused service shape with the guards above; do not claim completed
R3 or unconditional convergence from the design.**

- Intent and shape match when successful cutover and bounded refusal are both explicit.
- User value is supported by existing recoverable file preparation and strict selection.
- Commitments remain honored only with the all-outstanding-input proof, exact final
  child, guarded fence expiry, and retained input/result protection.
- Conceptual debt is small: existing native catalog, preparation, reader and writer
  services subsume the expensive work. No additional general queue or scheduler is needed.
- Confidence is high in the static control/recovery assessment and moderate in the
  resource assessment. No runtime, memory, disk or latency claim was measured here.
