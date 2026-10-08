# Backfill design review: simple recovery and bounded resource use

**Keep the native file-and-ordinal reader. Reshape the persistence and commit
granularity before implementation.** Compact per-file reuse records are suitable;
full cumulative pins or an unconditional transaction for every file would make
metadata cost dominate this design. No row-index SQL database is needed.

This is a static architecture review on 2026-10-08 against repository HEAD
`ac4064b`, using the semi-formal-architecture-review and applicable Rust guidance.
The design under review is the implementation task's backfill brief, reproduced
below. No implementation or tests were changed or executed by this reviewer.

## Findings

### BF1 — Compact file identity must be separate from whole-source adoption

**Severity: CONCERN. Category: intent versus shape / material dependencies.**

**Location:** design brief, “exact source TablePin” and “catch-up reuses unchanged
file phases”; `src/model.rs:62–68`; `docs/requirements/03-schema-backfill.md:15–19`.

A `TablePin` includes the entire object inventory. If its changing snapshot or
complete serialized value enters every per-file reuse key, a source append makes
every old file miss the cache. If the entire pin is copied into every completion,
an inventory of F files can produce O(F²) retained descriptor bytes. Neither is
required to bind the selected result to its exact source.

**RESHAPE:** use a compact immutable file-occurrence identity plus adequate material
schema/decoder/expression dependencies for file computation reuse. Record the
complete source pin once in the operation or selected group manifest. On adoption,
explicitly verify that each reused file remains a member of the new exact source
pin and that its interpretation and material dependencies still agree. Preserve
the original computation identity. A same-content copy at a different source
occurrence must not silently alias another occurrence; path, owner and digest
binding already exist in `ObjectRef` (`src/model.rs:51–58`).

This separation follows the required distinction between immutable values,
processing definitions, source occurrences and dataset versions. It does not
authorize reuse from business-key equality or content equality alone.

### BF2 — One lifecycle commit per file has quadratic cache-copy cost

**Severity: CONCERN. Category: resource efficiency / debt accrual.**

**Location:** design brief, “per-file completed phases”; `src/catalog.rs:248–270`.

Even with constant-size completion records, every lifecycle commit clones the
entire current `BTreeMap` (`src/catalog.rs:259`). Adding F new file completions one
commit at a time therefore copies a growing map repeatedly: O(F²) cumulative
record copying, plus repeated copying of pre-existing history. Each commit also
writes a native lifecycle file and snapshot. The finite history limit prevents
unbounded retained metadata; it does not make this operation sequence efficient.

**RESHAPE:** keep per-file reuse identity, but acknowledge a bounded bundle of file
completions through the existing `commit(Vec<records>)`. Bound the bundle by bytes
and a recovery/latency policy. Files are durably complete only when that guarded
commit succeeds; unacknowledged work remains an attempt. This reduces commit
frequency without a new queue, journal or scheduler. It reduces the coefficient,
not the asymptotic cost of the existing cache implementation; cap and measure
supported file count until lifecycle compaction or another measured improvement
is delivered.

Do not append each file to one cumulative group table and then persist that
growing full `TablePin` as every completion. Retain independent bounded output
fragments, record their exact output/coverage together, and assemble the selected
group through maintained native commits once. Use bounded coverage rows to name
fragments; do not put row-sized progress in lifecycle JSON.

### BF3 — Row-local semantics are required for safe file reuse

**Severity: CONCERN. Category: unstated assumption / correctness.**

**Location:** design brief, “deterministic scalar-expression field group”;
`src/engine.rs:1043–1077`.

Deterministic scalar output alone does not imply independence from other rows.
For example, a scalar subquery reading a maximum from `input` can return one value
per source row while depending on another file. Reusing an unchanged file after
that other file changes would be wrong. The existing general query validator
allows immutable, order-insensitive aggregates and traverses subqueries; it is
not a row-local expression validator.

**RESHAPE:** require expressions built from input columns, literals, casts,
conditionals and supported immutable scalar functions. Refuse aggregates, windows,
subqueries, joins, unnest/table functions and cardinality-changing plans. Preserve
one output occurrence for every input, including successful null/empty values.
Compile expressions once for the captured schema with maintained DataFusion
facilities and reuse that plan across bounded batches; do not parse/plan SQL anew
for each batch. A processing error fails that file's completion rather than
silently dropping the row or treating it as a successful null.

### BF4 — Native scan tasks are runtime objects, not durable descriptors

**Severity: CONCERN. Category: dependency seam / persistence.**

**Location:** design brief, “materialize batch descriptors”; pinned
`iceberg-0.10.1/src/scan/task.rs:55–120`.

`FileScanTask` has serde derives, but its partition, partition-spec and name-mapping
fields deliberately use serialization/deserialization functions that return
“not implemented” (`scan/task.rs:34–49`, `:89–117`). Persisting arbitrary native
tasks would therefore introduce an unsupported format boundary and duplicate
schema state.

**RESHAPE:** retain a compact project-owned descriptor containing immutable source
file identity, whole-file row count, material interpretation reference and output
references. Reconstruct native tasks from the captured source pin. Use a bounded
native descriptor/coverage table when file count exceeds the small operation
metadata allowance. Do not serialize native reader objects or collect all task
payloads without admission.

### BF5 — Scope the physical locator and the first consumer honestly

**Severity: OBSERVATION. Category: scope / user value.**

**Location:** `docs/requirements/03-schema-backfill.md:33–47`;
`docs/requirements/07-operations-qualification.md:93–99`;
`src/engine/write.rs:91–96`.

File identity plus row ordinal is an immutable physical occurrence locator.
Rewriting or compacting a file changes that locator even when values are equal.
The proposed conservative recomputation of rewritten files is correct; it does
not qualify stable logical-occurrence reuse across compaction. The current source
update path refuses datasets with dependent groups, which correctly prevents a
new source from silently retaining stale enrichments.

**KEEP** those boundaries until explicitly implemented and qualified. An initial
backfill may catch up to source changes made while its candidate was unselected;
that is distinct from ongoing ingestion after a group has been selected. A useful
field-group capability must expose a pinned combined read or materialization with
exact locator correspondence. A separately readable group table alone does not
satisfy the R3 consumer outcome. Do not remove the current source-update refusal
without selecting a consistent source-and-groups combination.

## Artifact summary and decision frame

The reviewed brief proposes:

1. Scan bounded, serial native `FileScanTask`s and assign an ordinal within each
   source file before filtering or relational processing. Refuse split scans,
   delete-bearing tasks and predicates; verify the native whole-file row count.
2. Bind derived rows to immutable source file identity and ordinal, and bind the
   selected field group to the exact source `TablePin`.
3. Support a deterministic scalar-expression field group. Store output and bulk
   coverage in native tables; recover completed file phases through the existing
   lifecycle rather than a per-row journal or SQL index.
4. Select a group only under exact source/boundary and expected target-group
   guards, preserving independent sibling groups. Catch-up reuses qualified
   unchanged files and computes new or rewritten files.
5. Retain the existing finite metadata budget pending lifecycle compaction.

The beneficiary is the dataset operator adding or improving a computed field.
The user-visible outcome is new fields without rewriting unrelated source data,
with recoverable progress and complete, correctly attached results. This is a
product capability supported by proof and operational infrastructure.

The exact native occurrence probe is visible in `src/query.rs`'s uncommitted
`native_occurrence_locator_probe` and `/tmp/docspec-occurrence-probe.log`. The
combined `id`, `_file`, `_pos` scan failed with “field not found”. This is evidence
that the attempted pinned-client path did not qualify, not proof about every
Iceberg implementation or future release. The narrower per-file fallback needs
its own positive qualification.

## Lineage and relationships

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| Greenfield plan | Requires independently computed groups, native bulk coverage, immutable input retention and one guarded lifecycle authority | `docs/PLAN.md:23–33`, `:44–62` |
| R3 schema/backfill requirement | Requires exact occurrence bindings, material dependency reuse, completed batches, complete cutover and a composed consumer result | `docs/requirements/03-schema-backfill.md:15–47` |
| Research acceptance | Requires early reuse lookup, bounded metadata, visible pressure, preserved completion and complete-path measurement | `docs/requirements/RESEARCH.md:26–33` |
| Existing native reader | Supplies serial decoding, batch admission and byte checks; can be reused rather than replaced | `src/storage.rs:68–95`; `src/query.rs:45–55` |
| Existing lifecycle | Supplies guarded multi-record commits and bounded replay, but clones current cached records per commit | `src/catalog.rs:192–270` |
| Q2 and Q7 | Require concurrency/correspondence and metadata-growth qualification; separate columns alone do not establish the guarantees | `docs/requirements/07-operations-qualification.md:93–99`, section Q7 |

| Component / seam | Owner | Depends on | Consumer / declared boundary |
| --- | --- | --- | --- |
| File occurrence reader | Storage/query adapter | Native task planning, exact file identity and native schema interpretation | Backfill expressions and combined reads; proposed seam needs the guards below. |
| Row-local computation | Focused backfill service | Captured schema, immutable expression dependencies, admitted Arrow batches | File result and coverage; R3 owns semantics. |
| File completion | Shared lifecycle service | Protected output bytes and matching complete coverage | Resume/catch-up; R1 semantics must remain the authority. |
| Group adoption and selection | Application selection service | Exact source, target group expectation, complete file coverage and active definition | New complete dataset selection; R3 strict cutover differs from routine R5 refresh. |
| Combined consumer read | Shared reader service | Selected source/group pins, locator join and schema | CLI/API/export; R3 explicitly requires this boundary. |
| Retention | Existing protection inventory | Source, group, phase and output references | Future cleanup; remains disabled until R6 qualification. |

## Invariants and commitments

- **Every locator denotes one source occurrence. NEWLY INTRODUCED.** Require a
  qualified whole-file task: `start == 0`, a full-file length (or the library's
  documented `0/0` whole-file form), known `record_count`, no predicate, and no
  deletes. Refuse duplicate tasks for one live file. Native task fields expose
  these facts (`iceberg-0.10.1/src/scan/task.rs:55–87`); native reading applies
  ranges, predicates and deletes before emitting batches
  (`src/arrow/reader/pipeline.rs:267–389`). Without these guards, a dense counter
  after reading can identify the wrong original row.
- **Ordinal generation precedes transforms. NEWLY INTRODUCED.** Reset only at a
  new immutable file, use checked arithmetic, and compare the final decoded count
  with the admitted native count before recording completion. Different batch
  sizes must produce the same locator/value pairs. Never infer correspondence
  from separate stream positions or business keys.
- **Coverage means complete computation. PRESERVED if explicit.** For a strict
  one-output-per-input scalar group, coverage can be a completed file range with
  row count and outcome, rather than duplicated per-row status. Missing completion
  remains pending/failed; a completed null is a value. Output and its matching
  coverage become authoritative together (`03-schema-backfill.md:39–41`).
- **Reuse identity differs from adoption. RELIED UPON.** Unchanged file phases
  retain their original generation; a new exact source selection records their
  membership and eligibility rather than manufacturing a new computation
  (`03-schema-backfill.md:15–19`, `:33`).
- **Independent group commits preserve siblings. RELIED UPON.** Reload the current
  selected combination, check the exact source and source boundary plus the
  expected target group and definition, merge that group, and commit once. Current
  generic selection replaces the whole dataset (`src/engine.rs:729–748`), so it
  cannot be reused unchanged for sibling-group adoption. A group-only operation
  must not invent a new source-admission boundary.
- **Bounded metadata remains readable after restart. PRESERVED.** Warm and cold
  lifecycle paths count serialized history and refuse excess
  (`src/catalog.rs:163–177`, `:250–263`). Estimate descriptor/final-manifest capacity
  before expensive processing; refusal preserves completed work. No silent budget
  increase, unbounded descriptor vector or extra status database is justified.
- **Source-byte and output checks retain their purpose. RELIED UPON.** Resolve
  qualified file phases before reopening source payloads. Required integrity and
  independent readback checks remain explicit; report their bytes separately from
  avoided computation. Reused computation does not imply zero I/O.

## User value and smaller maintained facilities

The design avoids rewriting broad source rows just to add a narrow field and
preserves work when a later file or cutover fails. That value supports a small
file-locator adapter and explicit coverage. It does not justify a second SQL
database, generalized plugin architecture or scheduler.

Reuse maintained task planning and `ArrowReaderBuilder` for schema-aware decoding;
keep the serial/batch settings and shared admission of the existing reader. Use
DataFusion expression planning/evaluation or a validated projection over the
existing streaming provider. Use existing Iceberg writers and multi-record
lifecycle commits. Hold reader/output admission through consumption and close;
do not introduce an unbounded `collect()` as an intermediate representation.

The main added debt is the physical-locator seam and a compact durable file-result
format. Explicit limitations prevent that narrow format from becoming an accidental
universal row-identity or native-task serialization standard. Repeating long paths
or complete pins in every derived row is unnecessary; use a compact file reference
with a retained mapping and an ordinal. Measure any further encoding optimization
against the actual output, rather than introducing a custom binary format.

The value claim is falsified if adding a cheap narrow field spends most of its time
rewriting source-width payloads, serially committing metadata, or materializing a
wide consumer result on every read. R3 requires measuring composition and readback
alongside avoided writes (`03-schema-backfill.md:35`, `:63`).

## Counterfactuals and qualification tests

- **Kill criterion:** unchanged physical file plus identical material interpretation
  produces different locator/value pairs across batch sizes or retries. Stop
  selection; repair or replace the reader seam before promising reuse.
- **Opposite design:** a row-index SQL database adds durable synchronization and
  cleanup obligations while still needing a qualified mapping from native rows.
  It does not solve the failed position-column probe by itself.
- **Removal probe:** without backfill, the current full-query build remains
  available, but operators must redo unrelated work and cannot recover file-level
  computation independently. Without the compact completion layer, recovery value
  disappears first; without occurrence binding, correctness disappears first.
- **Sibling subsumption:** native readers/writers and shared lifecycle already
  provide most machinery. They do not yet supply exact row locators, row-local
  expression validation, group coverage or independent merge selection.
- **Future criticism to prevent:** “per-file completion” quietly became a catalog
  transaction and full source-pin copy for every file, then small-file workloads
  exceeded history limits before finishing an otherwise modest dataset.

Before claiming this feature qualified, freeze targeted cases that check:

1. A multi-row-group file read with different batch sizes produces identical
   identities, duplicates, null/empty values and derived results. A split,
   predicate, delete-bearing task, duplicate task or wrong count refuses.
2. An interrupted bounded completion bundle distinguishes acknowledged file
   results from unfinished attempts; resume neither reruns committed computation
   nor invents coverage from readable orphan files.
3. A source append reuses existing file computations; a rewritten/removed file
   cannot keep stale locators. Schema/decoder/expression changes invalidate only
   phases whose adequacy is established; unknown adequacy recomputes.
4. Two different groups select without losing each other's result, same-group
   conflict refuses, source movement defeats stale cutover, and no-op group work
   does not move the source boundary.
5. Fixed data/update size with increasing file count and retained history measures
   descriptor bytes, lifecycle commit count/copy cost, native objects, restart
   lookup, retained bytes and complete consumer cost. Do not infer scale from row
   count alone. Include wide input slices and expanding scalar outputs under the
   existing resource-refusal policy.

## Verdict

**VERDICT: RECONSIDER the persistence and acknowledgment shape; KEEP the bounded
native file-and-ordinal mechanism.** Specifically: compact file descriptors,
separate reuse from source adoption, row-local expressions, and bounded completion
bundles through the existing lifecycle API. These are refinements of the proposed
mechanism, not a request for another backend or approval flow.

- Intent versus shape: **MATCHES** after the explicit bounds and reuse rules above.
- User-value claim: **SUPPORTED**; complete consumer composition remains part of
  delivery, not a result established by the reader probe.
- Commitments: **HONORED** by the narrow mechanism; full R3/Q2 is not yet qualified.
- Conceptual debt: **NEUTRAL** with compact descriptors and shared primitives;
  cumulative pins or a row-index database would add avoidable debt.
- Sibling subsumption: **PARTIAL**; reuse existing readers, writers and lifecycle.
- Confidence: **HIGH** in the code-backed metadata and serialization risks;
  **MEDIUM** in the proposed ordinal path pending positive native qualification.
