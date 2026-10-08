# Backfill design: consistency review

**Verdict: RECONSIDER the reuse and selection details; KEEP the native-reader
remedy.** Serial, complete-file decoding with an ordinal assigned before any
relational processing is a reasonable minimal fallback. It does not fundamentally
obstruct the full plan. Before wiring it into the engine, define exact file reuse,
separate completion from promotion guards, and assemble the final release from
the latest compatible group selection. Keep physical location distinct from the
logical occurrence identity required for future compaction.

This is a static architecture review on 2026-10-08 of the design supplied in the
review request, against the engine at `ac4064b` and the retained R1–R7 requirements.
The review request is the design artifact; no separate backfill implementation
document or completed implementation was available. The reviewer read the engine,
model, catalog, native-reader dependencies and relevant requirements. No tests,
probes or benchmarks were executed. The reported `_pos` failure is implementer
evidence, not a reviewer-run result; the in-progress probe is visible in
`src/query.rs:native_occurrence_locator_probe`.

## Findings and decisions

### F1 — A content digest alone cannot identify occurrence-bound reusable work

- **Severity:** CONCERN
- **Category:** unstated-assumption
- **Location:** review request, “completed files reusable by file digest and
  processing definition.”
- **Evidence chain:** The proposed output binds `(ObjectRef, ordinal)`.
  `ObjectRef` includes owner, path, digest and length (`src/model.rs:51-58`). Two
  distinct source files can contain identical bytes. Reusing the first file's
  output for the second by digest alone would retain the first locator or collapse
  distinct occurrences. Native field interpretation and relevant decoder rules
  can also change while file bytes do not. R3 requires exact input correspondence
  and material schema/decoder dependencies (`docs/requirements/03-schema-backfill.md:15-19`).
- **Recommendation: RESHAPE.** For this checkpoint, key completed work by the exact
  source object identity, the consumed native field interpretation and the
  material processing definition, with validator/eligibility rules explicit.
  Reusing the same unchanged file across source snapshots remains possible. Do
  not include the whole source snapshot as a material cache key when only the file
  was consumed. Reuse across a different locator requires an explicit verified
  rebinding; omit that optimization initially. Use R1's phase representation, not
  another cache authority.

### F2 — Complete-result readback must describe the actual concurrent group merge

- **Severity:** CONCERN
- **Category:** named-seam
- **Location:** review request, “selection merges one independent group.”
- **Evidence chain:** The current engine constructs a whole `DatasetPin` and its
  release before taking the final selection lock (`src/engine.rs:666-718`), then
  installs that entire value (`:720-749`). If two backfills capture source S with
  no groups, A may select group A while B still holds an earlier whole-result pin.
  B must not replace A with a result containing only B, or select a combination
  that differs from its independently verified release. The plan makes one
  catalog selection authoritative (`docs/PLAN.md:Architecture`), and R3 requires a
  pinned combined view with exact relationships (`03-schema-backfill.md:33-35`).
- **Recommendation: RESHAPE.** Commit prepared group values independently. At
  promotion, load the latest selection, recheck the exact source and expected
  target-group selection, then replace only that group while preserving compatible
  siblings. Build and verify the immutable release for this actual combination.
  A catalog conflict reloads the selection and re-evaluates eligibility; it may
  repeat composition/manifest work while retaining qualified file computation.
  Do not hold a dataset lock throughout backfill, and do not acknowledge a
  combination that differs from the readback pin. A completed operation retry
  still returns its originally committed release.

### F3 — Source coverage, admission fencing and computation completion need distinct checks

- **Severity:** CONCERN
- **Category:** named-seam
- **Location:** review request, source boundary and stale promotion rules.
- **Evidence chain:** Existing `phase`, `reserve_attempt` and `complete` all call
  the same authority guard (`src/engine.rs:767-861`), which checks the selected
  base (`:885-918`). Reusing that guard unchanged would stop durable per-file
  completion after source advancement, contrary to the proposed retention behavior
  and R3's recovery requirements (`03-schema-backfill.md:39-43`). Also, the current
  `authority.boundary` increments at operation registration before input admission
  (`src/engine.rs:515-552`). It is not by itself proof that those inputs are included
  in the selected source.
- **Failure sequences:** A file finishes after another operation advances the
  source and cannot record its otherwise valid retained completion; or a source
  operation registers, then fails input admission. Treating its sequence as covered
  falsely includes its input, while requiring selected coverage to catch that
  sequence without a terminal policy can indefinitely block promotion.
- **Recommendation: RESHAPE.** Name the captured source pin/covered boundary
  separately from the observed source-admission fence. A file may complete against
  its protected captured input when the head changes; its original definition and
  execution evidence remain unchanged. Reuse eligibility and final promotion must
  recheck relevant invalidation. Strict promotion checks the source relationship,
  current definition, target-group selection and admission policy. State how
  pending, failed or explicitly abandoned source attempts affect the fence.
  Backfill and unrelated group selection do not advance source coverage. No new
  general workflow system is needed for these distinct predicates.

### F4 — File ordinals are physical locators, not the final compaction identity

- **Severity:** CONCERN
- **Category:** commitment-violation if promoted to permanent identity
- **Location:** review request, “occurrences” and compaction scope disclaimer.
- **Evidence chain:** File rewriting changes both object identity and row ordinal,
  even when values are equivalent. R6 requires compaction to preserve logical
  occurrence/value identities and generation history
  (`docs/requirements/06-maintenance-retention.md:13-17`). R3 separately names
  business keys, source occurrences, values, definitions and dataset versions
  (`03-schema-backfill.md:15`). The review request expressly defers stable identity
  across compaction, so there is no current claim that this already satisfies R6.
- **Recommendation: KEEP with a named limit.** Serialize and describe this tuple
  as a source-pinned physical locator. Do not expose it as a permanent logical
  occurrence ID or derive all value identities from it. Future compaction can
  retain origin identity columns or a verified one-to-one correspondence while
  changing physical locations. Keep old objects protected until their bindings
  are no longer promised. That evolution is possible without replacing Iceberg
  or this reader; it must precede enabling compaction, not be hidden inside it.

### O1 — Exact coverage and scalar processing should be construction rules

- **Severity:** OBSERVATION
- **Category:** scope
- **Location:** review request, one row per source occurrence and complete coverage.
- **Recommendation: KEEP and make executable.** Compile a row-preserving scalar
  projection over the captured input. Carry internal locator columns unchanged;
  refuse expressions/plans that can filter, join, aggregate, unnest, contain a
  subquery, change cardinality or substitute identity columns. Existing general
  query qualification allows some aggregates, so it is not sufficient unchanged
  (`src/engine.rs:1043-1081`). Capture the group output schema once, including a
  stable group/field identity namespace; independent native tables commonly begin
  their field IDs from the same range. A display-name change is not a new identity.

  Coverage means exactly one accepted result for every expected locator, with no
  extras or duplicates. Equal counts alone are not that proof. Establish it by the
  row-preserving construction, structural checks and independent value/locator
  fixtures; do not add an unconditional extra population scan solely for status.
  Successful null remains a completed output row. Missing coverage is pending,
  an execution failure remains failed, and an empty source has complete empty
  coverage without fabricated rows (`03-schema-backfill.md:9`, `39-45`, `54`).

### O2 — Existing dependent-group refusal should remain until source advancement is coherent

- **Severity:** OBSERVATION
- **Category:** sibling-conflict
- **Location:** `src/engine/write.rs:92-96`.
- **Recommendation: KEEP.** Current updates refuse once dependent groups exist.
  Backfill preparation can still race a source update before promotion. Do not
  remove this refusal merely to demonstrate continued ingestion after selection:
  a newly selected source must not silently inherit old enrichment as complete.
  Later source advancement needs explicit stale/pending group coverage or
  coordinated refresh. Label this checkpoint as independent group preparation and
  guarded selection, not completed R3/R5.

## Artifact summary

- **Problem:** The pinned native reader's `_pos` path reportedly fails. Backfill
  needs exact duplicate-safe source correspondence without a business-key join or
  rewriting the base dataset.
- **Decision under review:** Read maintained whole-file native tasks serially;
  assign zero-based file-local positions; reject deletes, filtering and splits;
  bind results to source pins; retain per-file computation and coverage; merge a
  completed independent group under source and same-group guards.
- **Category:** Product capability supported by proof infrastructure. The user can
  compute a new field group, resume completed files and keep reading the prior
  selected result while work is pending.
- **Subsystems:** Native decoding (`src/storage.rs:68-95`), query execution
  (`src/query.rs:18-55`), phase retention and selection (`src/engine.rs:491-861`),
  common model (`src/model.rs:60-118`) and guarded lifecycle commits
  (`src/catalog.rs:241-280`).
- **Status:** Pre-implementation design. The `_pos` probe is not a completed
  backfill API or an execution certificate for the proposed ordinal fallback.

## Lineage and relationships

| Prior artifact | Relation | Constraint |
| --- | --- | --- |
| `docs/PLAN.md:Architecture and Lifecycle` | Parent implementation scope | Independent field groups, one authoritative selection, maintained components and exact readback. |
| `docs/requirements/RESEARCH.md:Requirements and task traceability` | User research requirements | UR-01/02/04/05 require retained expensive work, exact selected inputs, material invalidation and shared decoding. |
| `docs/requirements/01-durable-storage.md:27-42` | Common phase owner | Defines durable completion/reuse; forbids a second cache or job authority. |
| `docs/requirements/03-schema-backfill.md:11-47` | Direct feature requirements | Stable identities, complete coverage, independent groups, catch-up and strict cutover. |
| `docs/requirements/05-dependent-refresh.md:Design and Shared promotion and recovery` | Sibling | Uses the same result/coverage selection mechanics with a distinct captured-version policy. |
| `docs/requirements/06-maintenance-retention.md:13-27` | Future consumer and retention constraint | Compaction preserves logical identities; source bindings and completed phases remain protection roots. |
| `docs/requirements/07-operations-qualification.md:Q2 and Q4` | Acceptance | Concurrent groups and source corrections preserve correspondence; newly discovered dependencies invalidate stale adoption. |
| `docs/reviews/query-reader-consistency.md:Closure inspection` | Existing reader qualification boundary | Current native schema versus snapshot schema must be qualified before schema-only evolution. |

| Component / seam | Owner | Depends on | Consumers and seam status |
| --- | --- | --- | --- |
| Physical occurrence reader | Storage adapter | Exact native file task, native schema interpretation, immutable object and shared budget | Backfill and eventual combined reads; proposed here, needs an explicit narrow reader interface. |
| Group definition and coverage | R3 using R1 records | Stable group/field IDs, scalar expression definition, exact source scope | Plan/resume/promotion and readers; requirements name the semantics, concrete types remain to be written. |
| File completion and reuse | Shared engine/R1 | Protected output, material dependencies, validation scope | Backfill and catch-up; reuse existing lifecycle ownership, with F1/F3 refinements. |
| Combined release selection | Engine/R1/R3 | Complete compatible group pins and current authority | CLI/HTTP/readers/publication; F2 identifies the new merge step. |
| Retention and compaction | R6 | Source/group/phase roots and future correspondence | Every historical or in-flight reader; destructive behavior remains disabled. |

## Invariants and commitments

| Invariant | Status | Evidence and failure mode |
| --- | --- | --- |
| Every output refers to exactly one captured source occurrence | RELIED-UPON | R3 `:15`, `:33-35`; complete-file ordinals can provide local correspondence only when the task visits all rows exactly once. Filtering/deletes/splits before numbering break it. |
| Completed computation survives later source movement | PRESERVED by stated intent; requires F3 | R1 `:29-40` and R3 `:39-43`; using current source eligibility for durable completion discards useful qualified work. |
| Reuse binds exact material dependencies | RELIED-UPON; requires F1 | R1 `:31-40` and R3 `:17-19`; digest-only reuse can attach one file's rows or obsolete schema interpretation to another. |
| One catalog selection defines the full result | PRESERVED if F2 is applied | `docs/PLAN.md:Architecture`; readback and selection must refer to the same merged combination. |
| A group update preserves independently selected siblings | NEWLY INTRODUCED at this engine seam | R3 independent grouping and Q2; whole-map replacement after another group commit loses valid user work. |
| Source coverage does not advance because an enrichment ran | PRESERVED by proposed rule | R3 cutover and R5 coverage selection; a registration counter must not be presented as covered source input. |
| Compaction preserves logical identities | DEFERRED, not weakened | R6 `:13-17`; F4 prevents physical ordinals from becoming an incompatible permanent promise. |
| Coverage and schema are part of the consumer result | RELIED-UPON | R3 `:35`, `:45`; source and field-group tables alone do not establish a correct combined reader. |
| Stable operation retry returns the recorded result | PRESERVED requirement | Current reconciliation at `src/engine.rs:459-488`; retry must not re-merge a completed old operation into a newer selection and fabricate a new result. |

The native implementation supports the fallback's stated restrictions:
`FileScanTask` exposes byte start/length, full-file record count, native schema,
predicate and deletes (`iceberg-0.10.1/src/scan/task.rs:55-87`). The serial reader
uses sequential task processing and flattening rather than unordered task streams
(`iceberg-0.10.1/src/arrow/reader/pipeline.rs:64-84`). It applies split, predicate and
delete selection before emitted batches (`:283-400`). Refusing those inputs is
therefore necessary, not just defensive documentation. Verify the planned task
set equals the expected current file set exactly once; unknown record counts or
incomplete reads cannot authorize coverage.

## User-value analysis

The practical value is adding independently computed fields without rewriting
unchanged comments and retaining completed computation when a worker or final
promotion fails. Duplicate and null business keys remain separate observations.
This directly supports R3's user outcome and UR-01/02. The useful proof is exact
combined-reader values plus visible reuse after interruption, not merely new
Parquet files or a lower rewrite count.

The proposed adapter adds a small amount of project-owned work: validate native
task shape, maintain a checked ordinal counter and bind it to the exact file. It
reuses decoding and relational expression machinery. It avoids a custom Parquet
decoder, a global occurrence index and a general incremental query compiler. This
is proportionate to the missing native `_pos` primitive.

Keep per-file progress bounded and share source-scope metadata. Repeating an entire
source snapshot inventory inside every file completion would multiply retained
metadata unnecessarily; use ordinary immutable references. The current lifecycle
already refuses excessive serialized history (`src/catalog.rs:248-263`), which is
a safety limit rather than a scaling qualification. No performance gain is claimed
until complete preparation, validation, composition and consumer read costs are
measured.

The value claim is refuted if adding a small group still requires a wide source
rewrite, unchanged completed files must be recomputed after ordinary source
advancement, or consumers cannot retrieve exact source-plus-group values without
inventing their own identity join.

## Counterfactual and qualification analysis

| Case | Required observation or decision |
| --- | --- |
| Same logical file across batch sizes and row groups | A fixture with distinctive row values receives identical file-local locators across repeated native reads. Counts alone are insufficient. |
| Distinct files with identical bytes | Both occurrence populations survive; a phase cannot silently bind the wrong object. |
| Split task, predicate, deletes, missing count, repeated task, missing file | Refuse before declaring file coverage. These are outside the narrow ordinal method. |
| Null/duplicate business keys and nested/null/empty values | Backfill and combined reader match an independent occurrence-level oracle exactly. |
| Empty source | Select a typed complete-empty group without fake data rows. |
| Interrupted file and post-file failure | Uncommitted partial work is not complete; committed files remain reusable and protected. |
| Source changes one file during preparation | Old-source preparation remains inspectable; stale promotion refuses; unchanged exact files are reused in catch-up and changed/deleted occurrences are reconciled. |
| Two different groups race | Both survive in the final selected release, with exact same-source bindings and unchanged source coverage. |
| Two writers replace the same group | Only the expected-target winner selects; the loser retains useful prepared work and receives a structured conflict. |
| Source registration fails before admission | No false coverage and no unexplained permanent promotion barrier; exercise the F3 terminal policy. |
| Definition/dependency adequacy changes after reuse lookup | Final selection rechecks current eligibility; a stale phase lookup cannot authorize adoption (Q4). |
| Lost successful promotion response | Exact operation reconciliation returns its original committed release; no new group merge occurs. |
| Compaction changes locations | Remains explicitly refused/unqualified until identity preservation or correspondence is implemented and tested. |

**Kill criterion:** If unfiltered complete-file native reads do not retain exact
file order, or cannot expose a trustworthy one-task-per-file boundary, ordinal
assignment after decoding cannot establish identity. Repair/replace that adapter
or use carried origin IDs; do not infer order from equal counts.

**Counter-decision:** Requiring carried origin IDs in every base file now would
give a more direct future compaction path but changes source-writing semantics and
rewrites or maps existing development data. It is not necessary for this explicitly
source-pinned precursor. Waiting for native `_pos` support avoids the fallback but
blocks the user-visible group capability without evidence that such waiting pays.

**Removal probe:** Without an exact occurrence reader, backfill must either refuse
duplicates/null keys, collapse them through business keys, or rewrite the full
source with carried identity. The current read/query adapters decode values but do
not already solve this correspondence, so sibling subsumption is partial only.

**Six-month critic:** The likely criticism is promoting a physical locator into a
permanent logical ID, or leaving group composition to each caller. Name the
locator now, centralize the combined-read relationship, and keep the future
compaction guarantee explicit.

## Verdict

**VERDICT: RECONSIDER** the narrow reuse/selection shape before implementation;
retain the proposed native-reader strategy.

- **Intent vs. shape:** MATCHES for source-pinned independent backfill, after F1–F3
  make exact reuse, completion and concurrent release composition explicit.
- **User-value claim:** SUPPORTED by R3 and the retained research requirements.
- **Commitment status:** HONORED if physical locator and logical identity remain
  separate and strict complete coverage is maintained.
- **Conceptual debt delta:** NEUTRAL for the small adapter; avoids a larger custom
  reader/index framework. F1–F3 prevent avoidable reuse and concurrency debt.
- **Sibling subsumption:** PARTIAL — reuse existing decoding, phases and catalog
  commits; add only occurrence binding and group-specific selection semantics.
- **Confidence:** HIGH in the requirement and current-engine mismatches; MEDIUM
  in the proposed fallback until its independent order/correspondence probe passes.

No reviewed choice forces a future storage-engine rewrite. Approval of a concrete
implementation should depend on the targeted cases above, not on declaring full
R3, R5 or R6 complete.
