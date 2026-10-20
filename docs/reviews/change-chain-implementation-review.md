# Bounded forward change comparison: implementation review

**Verdict: APPROVE the narrow implementation below.** Reuse the accepted-selection
walk, existing applied change tables, native readers and DataFusion aggregation.
No new event system, persistent index, storage backend or release-schema redesign
is needed to accelerate the existing value comparison. Completing that slice does
not by itself complete R4's occurrence-addressed change output, independently
retirable accelerators, or portable certificate admission.

This is an independent static architecture review on 2026-10-10. The reviewer read
the workspace/repository instructions, PLAN, STATUS R4, R4 requirements, research
requirements and retained practitioner findings, then traced production paths and
test assertions. No code, tests, builds, benchmarks or payload hashes were run.
References below describe the inspected working tree, not a released version.

## Findings and required decisions

### F1 — Reuse one accepted-history walk; do not turn comparison into repeated verification

**CONCERN; debt and resource use. Recommendation: KEEP the history rules and
RESHAPE only their internal reuse.** `follows` already captures one immutable
`SnapshotRows`, verifies the current-head anchor, exact endpoint identities,
co-committed result/link revisions and strictly decreasing predecessor revisions.
It accounts for serialized history and link count and yields cooperatively
(`src/engine/history.rs:44-138`, `297-367`). Calling that method and then walking
history again to collect changes duplicates discovery. Calling the deep change
verifier for every link would scan before/after tables and defeat the feature
(`src/engine/changes.rs:101-105`, `150-200`).

Factor the existing traversal so comparison can collect a bounded set of candidate
change references during the same traversal. Preserve the inexpensive proof-only
consumer. Retain the captured rows and their reservation until discovery finishes;
do not reacquire the single cache on every step. Ordinary assembly can reuse
`Checks::composition` and shared metadata checks; independent deep reconstruction
stays in qualification (`src/engine/verification.rs:29-35`; `docs/PLAN.md:44-65`).

### F2 — Value cancellation must not claim occurrence or evidence equality

**CONCERN; intent versus shape. Recommendation: RESHAPE the comparison response,
without changing stored row schemas.** Current `Comparison` reports only schema,
input identity and added/removed value counts (`src/model.rs:689-696`). Its reader
excludes stored source origins and, for related source comparisons, contributor
helper fields (`src/engine.rs:578-580`, `609-618`;
`src/storage/identity.rs:19-29`). Thus equal public values can cancel while source
occurrences or receipts differ. R4 explicitly requires these distinctions and
before/after references (`docs/requirements/04-change-tracking.md:9-23`).

Keep current counts explicitly **value-multiset counts**: duplicate equal rows
count separately, but their physical/source addresses are not part of that
equality. Add exact endpoint references, requested group, chosen path, fallback
reason and separately named schema/definition/coverage/evidence results. A metadata
identity difference establishes changed identity, not necessarily unequal receipt
values. Do not label an uncomputed evidence comparison as equal.

For occurrence-addressed R4 output, expose a bounded native/Arrow relation from the
same comparison plan, retaining first-old/final-new addresses for each supported
identity. Do not pack rows into JSON or invent a one-row-per-key map. This is a
consumer-surface extension; it need not alter `ChangePin` or source tables. Until
that output and its identity semantics are implemented, report the narrower
value-comparison scope rather than mark all of R4 complete.

### F3 — Missing optional changes and invalid retained evidence need different outcomes

**CONCERN; correctness. Recommendation: RESHAPE discovery into explicit eligible,
fallback and refusal outcomes.** A missing optional accelerator is a reason to
compare readable endpoints in full. An invalid present descriptor, wrong manifest,
wrong schema, inconsistent counts or unreadable/corrupt present native evidence
must propagate a refusal. A broad `Err => full_compare` would erase that distinction
(`docs/requirements/04-change-tracking.md:23-27`, `35-38`).

Required succession is not optional acceleration: missing result/link evidence,
rebound revisions or a cycle on the selected chain remain refusals. A valid reverse
comparison, cross-dataset comparison, or retained but unselected readback takes the
explicit full path. `check_read_release` intentionally admits exact completed
readbacks as well as selected results (`src/engine/read.rs:78-103`), while succession
requires membership on the observed selected chain, including equal endpoints
(`src/engine/history.rs:95-130`). Do not weaken the latter to accommodate the former.

### F4 — Independent accelerator retirement remains a real R4/R6 seam

**OBSERVATION; scope. Recommendation: KEEP this separate from the bounded comparator
and retain it as required unfinished work.** Current retention follows a release's
`changes` into its base manifest, endpoint metadata, removed table and added table
(`src/engine/retention/inventory.rs:911-917`, `935-947`). There is no demonstrated
independent optional-history retirement state in this seam. A comparator can handle
absent optional files, but that alone does not prove safe retirement or authorize
deleting them. R4-T05 still requires exact retirement/protection integration.

The necessary future change is a narrow exact accelerator-retirement binding in
the existing lifecycle/protection path, with reader and required-consumer
protection. It does not justify a new retention framework or another latest pointer.

## Artifact summary and lineage

The reviewed decision is R4-T04 bounded forward composition, constrained by
R4-T01–T03 identity/trust and R4-T05 optional-history semantics. It is a product
capability supported by proof infrastructure: a small update should tell a consumer
what changed without repeatedly reading the whole population. STATUS correctly
leaves composition, lineage certificates and explicit unavailable-history fallback
unfinished (`docs/STATUS.md:1125`).

| Prior artifact | Relationship and practical constraint |
| --- | --- |
| `docs/PLAN.md:11-40` | One embedded engine and thin transports; reuse Iceberg, Arrow and DataFusion; catalog selection identifies the complete result. |
| `docs/PLAN.md:44-65`, `69-82` | Resolve qualified work early; validate new material once; preserve phases; derive changes from applied writes. |
| `docs/requirements/04-change-tracking.md:13-27` | Exact typed endpoints, independent change domains, forward continuity, explicit fallback, no inferred branch order. |
| `docs/requirements/RESEARCH.md:26-33`, `43-47` | Shared selected input/decoding, material dependency separation, bounded metadata, exact duplicates/nulls, no second cache or scheduler. |
| `docs/STATUS.md:1446-1476` | Accepted order exists independently of optional changes; one retained cache handle avoids repeated history replay. |

The retained practitioner findings were read at
`/Users/mikewolfd/Work/corpora/session-reports-2026-10-07/data-processing-interviews/FINDINGS.md`.
Their concrete concern is repeated staging/decoding and unjustified index cost, not
proof that a particular new backend is faster. The live code below is the evidence
for present implementation behavior.

| Component or seam | Owner and current role | Reuse in this change |
| --- | --- | --- |
| `Engine::compare`, `src/engine.rs:536-665` | Engine admits endpoints, checks schemas, executes full duplicate-aware value aggregation. | Own selection of fast/full path and one response shape. |
| `history::follows`, `src/engine/history.rs:44-138` | Engine proves accepted order over catalog metadata. | Share traversal; ordering semantics stay unchanged for R5. |
| `ChangePin`, `src/model.rs:950-960` | Model identifies exact base/result native states and applied removed/added tables. | Reuse as typed inputs, not as permission to trust arbitrary caller files. |
| `replace_keys`, `src/engine/write.rs:383-627` | Writer replaces complete key groups; one affected-row decode supplies kept/removed rows; added files are shared. | Already supplies the necessary applied multiset, including shrinkage and duplicates. |
| `verify_changes`, `src/engine/changes.rs:19-75`, `118-185` | Engine checks operation/base/schema/count binding; deep mode reconstructs membership. | Share metadata checks; do not invoke deep mode in ordinary comparison. |
| `query::table` / `occurrences`, `src/query.rs:17-44`, `125-137` | Shared maintained native readers. | Read selected change tables through the same budget and field interpretation. |
| `EvidenceResult`, `src/evidence.rs:35-46` | Receipt changes are separate from value and contributor evidence. | Report/compose separately; value equality cannot hide receipt updates. |
| `Budget`, `src/budget.rs:83-94`; `admit_work`, `src/engine/worker.rs:111-133` | Shared spill/memory accounting and maintenance exclusion. | Hold protections for the whole comparison, including cancellation cleanup. |

## Invariants and concrete implementation

**I1: exact accepted endpoints. RELIED UPON.** Use full `VersionRef` identities
(dataset, version, manifest identity), never operation names, wall-clock order,
row counts or `parent_manifest` alone. The selection code takes the actual guarded
predecessor from current authority (`src/engine/history.rs:167-200`); the composition
parent and an operation's captured base can express different relationships.

**I2: a selected source edge is either identity-preserving or has complete applied
changes. NEWLY ENFORCED by the comparator.** For each consecutive accepted pair in
the requested interval:

1. Resolve only the needed release metadata under the already captured lifecycle
   snapshot. Reuse the endpoint objects already admitted. Account for serialized
   bytes and decoded pin inventories before retaining them; do not accumulate full
   `ReleasePin` objects for every history entry without a reservation.
2. If the source's exact qualified logical identity and interpretation are unchanged,
   classify the edge as zero source-value change. Backfill explicitly clears source
   and receipt changes (`src/engine/backfill.rs:694-701`); evidence-only selection
   clears source changes (`src/engine.rs:1602-1606`). `None` here is not a missing
   required source delta. Still inspect/report other changed domains.
   A present descriptor must pass its metadata/binding checks even on a proposed
   zero-change shortcut; neither equal endpoint identities nor declared zero rows
   excuses corrupt present evidence.
3. Otherwise require a supported present `ChangePin`. Validate its exact base
   version/manifest, before/after metadata, typed key IDs, schema interpretation and
   checked row-count arithmetic. Match the operation producing it and the committed
   result. Current checks are in `src/engine/changes.rs:19-75`, `108-164`.
4. The simplest initial accepted case binds the change base to the immediate
   accepted predecessor. If the stored base is an earlier accepted version, bridge
   only when every intervening requested-group identity is provably unchanged and
   the explicit change endpoint is that same logical state. Otherwise return an
   explicit unsupported-path fallback, unless the descriptor contradicts its own
   declared base/result, which is corruption and refuses. Never apply a descriptor
   twice merely because several releases expose equivalent state.
5. Require compatible interpretation throughout the accelerated interval. A valid
   schema edit or unsupported computed-group delta uses the full/schema-result path;
   it does not become an empty change. `process_schema` currently produces no value
   delta (`src/engine/schema.rs:170-198`). Stable field IDs distinguish renames from
   different fields, but this comparator need not invent a cross-schema cast policy.

**I3: exact multiset and null semantics. PRESERVED.** For current value counts, the
smallest correct composition is signed aggregation over **only** applied rows:
each added row contributes `+1`; each removed row contributes `-1`; group by every
compared typed value and sum. Positive residuals count additions, negative residuals
count removals. Since each qualified edge satisfies
`after = before - removed + added`, intermediate states cancel exactly.

Extract the existing SQL construction/aggregate decoding into one small shared
helper used by full endpoint comparison and change-table comparison. Use positional
aliases for arbitrary names and checked count conversion. Do not use `DISTINCT`,
ordinary anti-join set subtraction, per-key overwrite maps or `EXCEPT ALL`: the
current implementation documents the duplicate problem and uses signed counts
(`src/engine.rs:619-665`). Null-safe group-key joins already use
`IsNotDistinctFrom` (`src/engine/write.rs:650-662`).

For a first-old/final-new **key-group relation**, preserve the whole multiset at
each key, not one representative row. All current source replacements are complete
key-group replacements (`src/engine/write.rs:337-359`, `383-408`). The next removal
for a touched key must equal that key's preceding addition under the selected
identity rules; omitted keys carry through. Use the maintained grouped signed
comparison over change rows for this continuity check. An insert followed by delete
can cancel; delete/reinsert of equal visible values cancels only in value mode.
Occurrence mode includes stable origins where present. A removed table's physical
row location is not automatically the source occurrence location; legacy tables
without stored origins currently receive value-only verification
(`src/engine/changes.rs:128-139`). Do not claim occurrence output for those tables
without an exact mapping or a separately qualified full occurrence comparison.

**I4: trust belongs to creation/admission, not apparent file shape. RELIED UPON.**
Only compose descriptors obtained from the exact locally retained selection, with
their existing completion/trust scope. Current `verified` means routine validation,
not a complete cryptographic/value proof (`src/model.rs:168-186`); metadata is
digest-checked by `open_pin` (`src/storage.rs:677-695`). The ordinary path must not
retroactively call every old payload deeply verified. A present descriptor that
fails a check refuses. Arbitrary same-shape payload tampering is not proven absent
by schema/count/continuity checks; explicit deep verification remains the integrity
investigation path, and external certificate admission requires its own qualified
trust boundary. This follows the plan's immutable-object and verification rules,
not a new permission to trust caller-supplied history.

**I5: optional acceleration cannot become required state. REQUIRED; partly open.**
Classify absent accelerator metadata or an actually absent optional accelerator
object as a specific full-fallback reason, after validating any descriptor already
present. Do not treat permission errors, digest mismatches, decoding errors,
contradictory fields, missing required succession, or missing endpoint data as
ordinary history absence. Full fallback reads the admitted endpoints and reports
its path/reason. Required-consumer retention remains governed by R6, as F4 states.

**I6: bounded work and protection. PRESERVED and extended.** Metadata work is
proportional to observed history plus selected descriptors; changed-value work is
proportional to applied rows across the interval, including repeated edits of hot
keys. It is not necessarily proportional to the final net delta. Cold lifecycle
replay still grows with retained history (`src/catalog.rs:203-280`).

Charge retained descriptor bytes, native metadata and DataFusion plan size; bound
the number of retained tables/union inputs as well as serialized bytes. Build a
bounded or balanced union rather than a deeply nested plan with one level per old
version. Use existing spill support for grouped rows and the existing maintenance
read guard throughout. A declared optional-chain planning limit may select full
comparison before payload work starts; exhausted shared admission or corrupt
required history still refuses. Do not retry full comparison automatically after
arbitrary execution/resource failure. No unbounded Rust map of decoded rows, new
cache, new scheduler or per-row lifecycle writes are necessary.

**I7: field impact must be conservative. NEWLY ENFORCED in reported changes.**
Current `ChangePin` identifies key fields, not a complete changed-column bitmap
(`src/model.rs:952-960`). Report the union of affected groups along the chain; when
exact field impact is unestablished, mark all fields in that affected group as
potentially changed. Keep definition, schema and evidence impact even if final
value counts cancel. Never interpret key field IDs as the only changed fields.

## Narrow tasks and decisive correctness cases

Implement these in the existing engine/model/CLI seams, with a thin HTTP route if
comparison is exposed there. The CLI already calls `Engine::compare`
(`src/main.rs:661-673`). Keep normal response metadata bounded and use Arrow/native
streaming for row-level output.

| Task | Concrete result |
| --- | --- |
| Shared discovery | Reuse history traversal and admission; collect candidate descriptors under one observed revision, with explicit fallback/refusal outcomes. |
| Shared aggregation | Move existing full multiset aggregation into a small common helper; feed either endpoints or qualified applied rows. Keep force-full mode as the independent value oracle. |
| Result meaning | Add exact endpoints/group, value semantics, path and fallback reason; report schema, definition, coverage and evidence independently. Expose first-old/final-new native row references when the richer change relation is implemented. |
| Trust and continuity | Share current endpoint/schema checks; qualify local applied-write composition against independent fixtures; keep portable admission and unsupported identity modes explicit. |
| Protection and limits | Reuse operation admission, metadata reservations and spill; no new persisted index. Track independent accelerator retirement as R4-T05/R6 work until its exact lifecycle binding exists. |

The following cases decide correctness, not just coverage of new branches:

| Case | Required outcome |
| --- | --- |
| A→B→C insert, replacement and deletion; key edited repeatedly | Accelerated and force-full value counts agree with an independent typed-row multiset oracle. Addresses identify first old/final new where the chosen mode supports them. |
| Duplicate shrinkage 3→1, repeated null rows, partial-null composite keys, empty string versus null, nested values, quoted field names | Preserve multiplicity and native type/null semantics; no distinct/set subtraction or positional receipt inheritance. |
| A→B→A values; insert→delete; delete→equal-value reinsert | Value reversion cancels correctly. Different occurrence identities remain changes in occurrence mode; receipt changes stay visible. |
| No-op update, source-identical backfill, evidence-only selection, equivalent physical compaction | Zero source-value delta without endpoint payload scans when exact qualified identities establish it; retain separate evidence/schema/definition/coverage meaning. |
| Backfill admitted before another sibling but selected after it | Walk actual accepted predecessors; never infer order from captured base or parent name. The schema/group outcome remains visible. |
| Wrong base digest/version, wrong endpoint metadata, changed key IDs, invalid count arithmetic, conflicting schema, wrong native snapshot, malformed metadata, inconsistent key-group continuity | Refuse present invalid evidence; never return empty changes or silently retry full. |
| Optional change absent, including a missing optional object with readable endpoints | Explicit full fallback; endpoint reads still work; succession proof still works. Missing required ordering evidence remains a refusal. |
| Reverse, different datasets, valid unselected readback, equal accepted endpoints | Explicit correct full/identity path as applicable; equal endpoints do not certify membership on an orphan branch. |
| A gap from a full rebuild or unsupported group/schema change | Full comparison/schema result with a concrete reason; do not infer omitted changes. |
| Historical target behind current head; concurrent later selection; cache replaced by another dataset | One observed snapshot and immutable endpoints determine the answer. Never replay lifecycle per link or change target mid-comparison. |
| Long chains, hot keys, many tiny tables, wide rows, constrained metadata/memory/scratch | Bound discovery/plan growth; spill or choose the declared full path; return structured capacity errors; release reservations and maintenance protection on completion/cancellation. |
| Removal requested while comparison is admitted | Existing maintenance exclusion prevents deletion until comparison's worker is finished; caller cancellation alone does not release its custody. |

Qualification must count endpoint payload opens, change payload opens, metadata
decodes and verification passes. For the eligible path, assert **no endpoint payload
scan**, not merely a small returned row count. Reuse the full comparator as a
differential check, but also retain an independent Rust typed-row oracle so shared
SQL bugs cannot make both arms agree incorrectly. Existing fixtures provide useful
starting points: duplicate comparison (`tests/lifecycle.rs:810-835`), actual
predecessors (`tests/succession.rs:72`), orphan identity (`240`), corrupt history
(`322`) and metadata limits (`501`). These are static test references, not newly
executed results.

## User value and counterfactuals

The direct benefit is avoiding complete endpoint decoding when a consumer compares
two versions connected by small, retained, qualified changes. Reusing the signed
aggregation and accepted-history walk pays down duplication. The response makes
fallback cost explainable and avoids false evidence equality. No speedup is claimed
by this review; total changed rows, history size, files and cold replay can still
dominate. R4 requires complete-path and growth-axis measurement
(`docs/requirements/04-change-tracking.md:40-48`).

The counter-decision is to retain full comparison only. It remains simple and
correct for existing value semantics but fails the small-update user outcome by
continuing to decode whole endpoints (`src/engine.rs:602-640`). Removing succession
would break nonregression proof before it breaks comparison. Removing optional
accelerators should only make comparison slower. Existing components therefore
partially subsume the feature, while no existing function composes the chain.

The kill criterion is decisive: if acceleration needs endpoint payload scans on
every eligible request, or cannot preserve the force-full multiset result while
keeping occurrence/evidence distinctions explicit, this implementation has not
delivered its promised value. The likely future-review failure would be calling
an exact value-count optimization a complete R4 change certificate. Keep the
response semantics and STATUS wording precise.

## Verdict

**APPROVE** the shared bounded local comparison design. Intent matches the accepted
small-update outcome; user value is supported by the current full-scan path;
required accepted-order, duplicate/null, immutable-selection and shared-budget
commitments remain intact. The design pays down comparison duplication and reuses
existing maintained execution/storage components. Confidence is **high** in this
static implementation direction, with runtime correctness and resource behavior
still to qualify.

No fundamental storage or execution blocker requires a broader redesign. The
remaining blockers to declaring **all R4 complete** are concrete, narrower work:
fully specified occurrence/value relation output, external certificate trust, and
independent accelerator retirement with consumer protection. Preserve those
requirements instead of silently shrinking completion to accelerated counts.
