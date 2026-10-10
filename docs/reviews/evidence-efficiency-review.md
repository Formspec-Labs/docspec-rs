# Receipt-only updates: static efficiency review

**Final execution disposition, recorded by the primary agent on 2026-10-10:**
The reviewed source passed the [complete related-refresh suite](evidence-related-final.log)
and [`./scripts/check`](evidence-updates-final-check.log), both with exit status
zero. The final gate covers formatting, strict linting and all library,
integration and documentation tests, including the receipt suite after the last
future correction. The primary agent ran these checks with unchanged stack and
resource limits. This resolves the pending runtime checks recorded below without
changing the independent static approval. It does not establish throughput,
whole-process peak memory, remote behavior or complete-plan acceptance.

Review scope: the new receipt modules and tests, plus their unstaged integration
changes, against committed compacted-source base `a903c75`. The compacted-source
implementation is context, not this review's patch; its formatting drift has been
removed. This review uses the repository instructions, R3/R4/R5, the research
requirements, the Rust
performance/universal guidelines, and the semi-formal code-review workflow.

No builds, tests, benchmarks, corpus reads, or hashes were run by this reviewer.
The initial test table gives static expectations; retained runtime results are
identified separately. Initial-trace locations refer to the source before the
author's final formatting pass. The
repair disposition and final-review additions use the formatted source.

**Final verdict: APPROVE for this review scope.** F1 was repaired and independently
reread, including the final shared future and pin boxing. The reviewer read the
parent's retained
[focused test result](evidence-updates-focused-final.log): all evidence-update
cases pass, including the revised many-file fixture and receipt-phase recovery.
This focused pass predates the last verification-boxing change. The subsequent
[incremental oracle result](evidence-related-stack-verified.log) confirms the
original stack-failing case passes after that change. The parent reports unchanged
budgets and default stack. Related-suite and full-gate acceptance remain pending
at this checkpoint. This reviewer executed no tests.

## Findings

### F1 — Bound source-reader concurrency independently of file count — RESOLVED

- Original severity: **WARNING**; category: performance/resource use.
- Initial location before repair/formatting: `src/engine/evidence.rs:452–471`.
- Trigger: apply even a small receipt change after a source has stored occurrence
  origins and its snapshot contains many files.
- Initial evidence: `associate` created a separate `query::file` provider for every
  file, then unioned the providers. Each provider had one streaming partition
  (`src/query.rs:49–79`). DataFusion 53.1.0's physical planner constructs
  `UnionExec` for this plan (`physical_planner.rs:1711`); `UnionExec` sums the input
  partition counts (`datafusion-physical-plan/src/union.rs:180–185`). Its
  coalescing and repartitioning implementations start work for each input
  partition (`coalesce_partitions.rs:213–223`, `repartition/mod.rs:361–403`). Each
  native file reader immediately reserves twice `max_batch_bytes`
  (`src/storage/occurrences.rs:352–361`), and admission refuses rather than waits
  (`src/budget.rs:138–153`). `compute_threads` sets target partitions; it does not
  cap these file-provider partitions (`src/budget.rs:98–109`).
- Impact: reader-buffer demand can grow with source file count, independent of
  the number of changed receipt keys. A source that fits sequential reading can
  refuse a small receipt update under the shared memory allowance. This is a
  static execution-path finding; no failure threshold or measured peak is claimed.
- Fix: expose the captured `OccurrenceScan` through one sequential projected
  stream, or a fixed bounded number of streams. Reuse the existing projected
  file reader and shared admission rather than creating a cache or another
  receipt representation. This also removes the left-deep per-file union plan.
- Check: keep receipt scope fixed and grow source file count under a small
  `compute_threads` setting and fixed memory budget. Exercise both successful
  membership and an absent key after a value rewrite/compaction. Assert exact
  receipt associations and bounded active readers. Existing tests do not cover
  this file-count dimension (`tests/evidence_updates.rs:48–55,143–162`).

The upstream evidence above was read from the installed, pinned 53.1.0 sources in
the local Cargo registry, not inferred from API names.

**Repair disposition (formatted source):** `associate` now registers one
`query::files` provider for the captured origin columns
(`src/engine/evidence.rs:803–815`). The shared adapter exposes one partition
(`src/query.rs:67–97`); its `map(...).try_flatten()` stream opens the next file
after the preceding reader finishes (`src/query.rs:166–174`). Copied paths have
a metadata limit and retained memory permit (`src/query.rs:150–165,169–171`).
Existing single-file callers use the same adapter with `Some(path)`
(`src/query.rs:49–55,138–144`). The repair removes the active-reader file-count
multiplier and per-file union plan without a cache or extra source pass. This is
a structural improvement, not a speed or whole-process peak-memory measurement.

The final `one_receipt_on_many_origin_files_uses_a_fixed_reader_budget`
(`tests/evidence_updates.rs:700`) now uses the ordinary 512 MiB total/4 MiB batch
allowances throughout, with `batch_rows: 1` (`tests/evidence_updates.rs:34–41,740–746`).
It creates small physical files through maintained compaction with an 8 KiB
target (`tests/evidence_updates.rs:760–770`), rather than lowering lifecycle
metadata admission. It counts `compacted.output_files`, not preserved logical
occurrence identities, and asserts that simultaneous reader reservations would
exceed the fixed memory allowance (`tests/evidence_updates.rs:771–776`).

The append captures the selected compaction through `representation_pin`
(`src/engine.rs:1118–1156`, `src/engine/compaction.rs:52–83`); all existing keys are
outside the appended key 999, so the shared writer preserves the compacted files.
The fixture checks those file paths are retained by the appended source and its
schema contains origin fields before the receipt mutation
(`tests/evidence_updates.rs:782–813`). `process_receipts` then passes that source
to `check_associations`, selecting the sequential stored-origin branch. Both the
valid one-receipt replacement and an absent row `i64::MAX` are asserted, followed
by deep verification (`tests/evidence_updates.rs:814–838`). This reaches the
intended path by static trace, and the retained focused result confirms the
fixture executed successfully. Its hostile file-count assertion and successful
execution establish this case under configured resource admission. They are not
measurements of peak resident memory, file-descriptor peak, or throughput, and no
old-versus-new benchmark was run.

Earlier fixture attempts failed during setup, first on input row-group admission
and then on a lifecycle metadata row under the 8 KiB batch allowance. These were
reported by the parent and did not execute the intended fanout case. They are not
performance evidence for either reader implementation.

### Follow-up — Receipt completion classification — RESOLVED

The parent reported inheritance failures caused by the omitted `/receipts`
completion suffix. Independent tracing confirms `active_completion` rejects keys
outside `completion_key` before querying retained rows
(`src/engine/retention/retirement.rs:288–303`). The correction adds `/receipts` to
that shared classification (`src/engine/retention/retirement.rs:19–34`), which also
feeds phase-recovery scope and `retained_key` (lines 39 and 57–58). It therefore
repairs both reuse lookup and retention classification without adding another
record format or lookup path. The existing receipt-deletion killed-worker test
exercises this completion after a source update
(`tests/evidence_updates.rs:901`); it passes in the retained focused test result.

### O1 — Processing metrics omit source-association reads

- Severity: **OBSERVATION**; category: performance evidence.
- Location: `src/engine/evidence.rs:190,210,225,424–471`.
- `check_associations` may read every source file's two origin columns. Its
  return value carries no scan metrics. `process_receipts` returns the metrics
  from `replace_keys`, whose `files_scanned` counts receipt-file probes and
  receipt-file rewrites (`src/engine/write.rs:509–514`).
- The reported writer counters therefore do not establish complete receipt-update
  read cost. Keep them labeled as writer metrics, or add source-association
  file/byte counters when qualifying the complete flow. Do not add an extra scan
  to obtain counters. The dependent-refresh test's zero counters refer to its
  separate no-value-change shortcut, not to receipt attachment
  (`tests/evidence_updates.rs:232–246`).

## Patch summary

`EvidenceSpec` accepts complete replacement receipt groups and optional deletions,
keyed by stable source file and row (`src/evidence.rs:13–33`). The engine retains
the incoming Parquet, checks key membership, and reuses the existing native
replacement writer. `EvidenceResult` binds the receipt table and applied changes
to the exact selected source (`src/evidence.rs:35–47`). Receipt-only selections
adopt source, computed values, contributor evidence, and source boundary unchanged
while retaining the new receipt inputs (`src/engine.rs:1285–1311,1409–1445`).

Value updates retain receipts for surviving occurrences and delete associations
for removed occurrences. The first such value rewrite adds origin fields through
native metadata so surviving rows retain their old identities
(`src/engine/write.rs:194–220`, `src/engine/evidence.rs:66–100,228–310`).
Incremental dependent refresh can adopt exact value and contributor pins while
advancing captured coverage (`src/engine/incremental.rs:59–82,440–444`).

## Function trace

| Function or boundary | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| Request helpers | `src/model.rs:847–947` | Evidence request → dataset, operation, definition, base, inputs | Uses the existing operation identity and per-operation input names. |
| `validate`, `check_schema`, key helpers | `src/engine/evidence.rs:9–46` | Request/schema → acceptance or refusal | Requires a distinct base, definition, digest shape, and exact key/payload column shape. |
| `plan_evidence`, `update_evidence`, `evidence_until` | `src/engine/evidence.rs:106–128` | Bounded request → existing plan/run lifecycle | Reserves request memory and delegates to shared services. |
| `prepare_source` | `src/engine.rs:871–1020` | Retained request/input → durable processing result | Reuses admission/processing; dispatches receipt processing through the same completion records. |
| `process_receipts` | `src/engine/evidence.rs:163–225` | Replacement/deletion Parquet plus base → receipt table and changes | Checks overlap/membership/schema, then records actual removed/added rows. |
| `check_associations` | `src/engine/evidence.rs:49–63` | Distinct affected keys → membership proof | Rejects null/empty/negative keys and keys absent from the captured source. |
| `associate` | `src/engine/evidence.rs:427–472` | Receipt relation and source → semi/anti join | Uses file row ranges for original sources; two origin columns after rewrites. F1 applies to the latter branch. |
| `replace_keys` | `src/engine/write.rs:374–613` | Prior table, scope, replacement → native table/change relations | Materializes keys once, probes key columns, shares untouched files, decodes changed files once into kept/removed rows, and shares added files. |
| `receipt_replacement`, `source_with_origins` | `src/engine/evidence.rs:66–100` | Source and replacement → compatible write source/rows | Adds origin schema metadata only when needed and retains the prior logical source identity. |
| Value-update integration | `src/engine/write.rs:194–220` | Captured source → identity-preserving write source | Preserves canonical change endpoints while using the admitted representation. |
| Join/aggregate integration | `src/engine/incremental.rs:241–252`, `src/engine/aggregation.rs:256–268` | Derived replacement → identity-compatible replacement | Reuses the same origin preparation before replacing rows. |
| `inherit_receipts` | `src/engine/evidence.rs:229–310` | Source processing plus prior receipts → retained receipt result | Reuses exact receipts for unchanged/schema/insert-only sources; otherwise deletes removed occurrence groups in a separately recoverable phase. |
| Main result assembly | `src/engine.rs:1277–1563` | Processing/receipt completions → new combined selection | Adopts unchanged source and computed pins for evidence-only work; validates new receipts and component relationships. |
| `process_incremental` | `src/engine/incremental.rs:38–93` | Prior and current captured inputs → output table/change relation | Equal logical source identities preserve output and emit empty native changes without scanning value files. |
| `qualify` / `same_definition` | `src/engine/incremental.rs:511–543`, `src/model.rs:386–400` | Old/new refresh definitions → eligibility | Fences the shortcut by dataset roles, transform definition, eligibility, and public schema. |
| `incremental_evidence` | `src/engine/incremental.rs:419–445` | Empty applied value changes → prior contributors | Adopts the exact contributor table. |
| `prepare_evidence` | `src/engine/refresh.rs:131–219` | New captured coverage plus values → related result | Retains the adopted contributor table with the new request/coverage. |
| `verify`, `verify_result`, `verify_inheritance` | `src/engine/evidence.rs:334–422` | Retained selection → receipt checks | Routine mode checks assembly/schema/endpoints; deep mode checks complete membership and inherited multisets. |
| `verify_native_changes` | `src/engine/changes.rs:79–193` | Before/after/removed/added → applied-write proof | Shared source/receipt verifier; deep mode aggregates signed rows once per relation and preserves duplicate multiplicity. |
| `Checks::trust`, `Checks::table` | `src/engine/verification.rs:45–149` | Captured base/native outputs → bounded verification state | Includes prior receipt files in trusted immutable files; composition skips repeated data inspection. |
| `compatible_input_history`, receipt binding branch | `src/engine/backfill/verification.rs:244–291,380–389` | Backfill base/current parent → compatible input history | Permits exact receipt additions and independent backfills while checking unchanged source/definitions/related data. |
| `History::read` | `src/engine/backfill/verification.rs:313–356` | Manifest reference → shared bounded decoded manifest | Caches within the check, bounds cumulative bytes, and rejects reference conflicts. |
| Backfill composition/recovery | `src/engine/backfill.rs:694–717,875–913` | Prepared values/current selection → inherited receipts and values | Clears obsolete receipt change records, verifies compatible parent history, and preserves prepared values. |
| Receipt/occurrence readers | `src/engine/evidence.rs:134–161,327–331` | Exact release → Arrow stream | Reuses host stream admission and exact release resolution; payloads stream through maintained native readers. |
| CLI/HTTP adapters | `src/main.rs:188–210,303–324,556–596`, `src/http.rs:37–38,108–109,460–465,593–614` | JSON request or release → shared engine result/IPC stream | Thin adapters; HTTP body limit remains in the existing router. |
| Retention integration | `src/engine/retention.rs:73–82`, `src/engine/retention/inventory.rs:935–945,1136–1145`, `src/engine.rs:2347–2359` | Receipt pins/phases → retained object roots | Includes source identity, receipt tables, deltas, and the new phase in shared protection accounting. |

## Data flow, invariants, and cost dimensions

- **Input and receipt identity:** request input digests enter shared admission,
  survive as original input objects, and enter processing dependencies
  (`src/engine.rs:886–945,1000–1020`). Distinct key scope is derived from those
  retained inputs; receipt rows themselves are not deduplicated
  (`src/engine/evidence.rs:173–209`). This preserves duplicate/null receipt values.
- **Value adoption:** an evidence-only selection copies exact groups and computed
  bindings (`src/engine.rs:1439–1443`), and its verifier requires equality with the
  parent (`src/engine/backfill/verification.rs:192–209`). Incremental refresh
  consumes source tables, not receipt payloads or computed groups
  (`src/engine/refresh.rs:506–515`, `src/engine/aggregation.rs:65–81`). Its unchanged
  logical-source shortcut is therefore adequate for the currently supported
  transforms. A future evidence-dependent transform must add a material dependency;
  the present API does not claim one.
- **Source membership:** initial metadata ranges prove occurrence membership
  without source payload reads; rewritten sources need actual origin membership
  because deleted rows can leave gaps (`src/engine/evidence.rs:424–471`). The full
  narrow scan is a real correctness boundary, not evidence that full source values
  must be decoded. F1 concerns concurrency, not removal of that proof.
- **Recovery:** the shared completion lookup precedes original input copy/receipt
  computation (`src/engine.rs:864–878,949–979`). Source-driven receipt deletion has
  its own completion with exact source and previous receipt dependencies
  (`src/engine/evidence.rs:249–309`). Ordinary receipt reads do not redo membership
  proof (`src/engine/evidence.rs:134–141,327–330`).
- **Growth:** let U be changed receipt rows, S source rows, Fs source files, Fr
  receipt files, H retained selection depth, and G computed groups. Scope checks
  and materialization revisit U a fixed number of times. Initial association uses
  O(Fs) metadata; stored-origin association scans O(S) narrow values. The shared
  writer probes Fr files and can reread materialized keys per file, then rewrites
  affected files. This is not a delta-proportional write claim; R4 explicitly
  permits measured key scans (`docs/requirements/04-change-tracking.md:40–46`).
- **History:** a request-local bounded manifest cache prevents repeated disk
  reads across groups, but binding validation still traverses O(G × H) steps
  (`src/engine/backfill/verification.rs:213–226,313–389`). Cumulative manifests are
  bounded by `read_manifest` admission (`src/engine/inputs.rs:399–440`). Long
  histories can refuse at that bound; no claim of history-independent updates is
  warranted. No new cross-operation cache is justified by this static review.
- **Verification reuse:** new receipt outputs use the common bounded routine
  verifier, prior receipt files enter the existing trusted inventory, and deep
  membership/multiset reconstruction remains explicit
  (`src/engine/verification.rs:45–149`, `src/engine/evidence.rs:376–400`).

## Final future and pin allocation review

**Disposition: APPROVE for this correction.** The final boxes move large future
state and one optional result record behind pointers at shared boundaries. They
do not change receipt payload processing or create work per receipt row.

| Boundary | Current location | Allocation and behavior |
| --- | --- | --- |
| `receipt_replacement` and `source_with_origins` | `src/engine/evidence.rs:105–185` | One boxed future per helper invocation. Join and aggregate callers await the same helper before `replace_keys` (`src/engine/incremental.rs:266–279`, `src/engine/aggregation.rs:259–272`); the source-update caller awaits `source_with_origins` only when receipts exist and origin fields are absent (`src/engine/write.rs:199–221`). |
| `process_receipts` and `inherit_receipts` | `src/engine/evidence.rs:275–281,382–390` | One boxed future per processing/inheritance invocation, including an inheritance no-op. Main callers await them directly (`src/engine.rs:991,1345`); no redundant caller box remains. Completed receipt phases are still checked before reconstruction (`src/engine/evidence.rs:417–425`). |
| Shared `replace_keys` | `src/engine/write.rs:385–389` | One box per replacement operation. The whitespace-insensitive diff against the committed base shows the existing writer body enclosed in `Box::pin(async move { ... })`; its row-processing logic is unchanged. |
| `verify` and `verify_result` | `src/engine/evidence.rs:558–624` | One boxed future per invocation, including early-return checks. Composition and release verification await `verify` directly (`src/engine.rs:1621–1626,2499`); validation awaits `verify_result` directly (`src/engine.rs:1393–1404`). `verify` awaits the same shared `verify_result` at line 600. These allocations occur per checked selection, not per table row or batch. |
| Optional receipt result inside `DatasetPin` | `src/model.rs:140–154`, `src/engine.rs:1601` | `Option<Box<EvidenceResult>>` adds one allocation when the result is present, also when such a pin is cloned or deserialized. Assembly moves the retained result into the box; it does not copy receipt rows. `EvidenceResult` contains table/change references, not decoded receipt batches (`src/evidence.rs:43–46`). |

Origin preparation remains a metadata operation. It shares `source.objects`,
preserves row count, and restores the original logical identity
(`src/engine/evidence.rs:175–184`). The shared pin constructor checks existing
objects and reuses their identities instead of hashing them again
(`src/storage.rs:608–639`). The replacement DataFrame remains a lazy value; any
required origin-schema normalization follows the existing schema path
(`src/engine/evidence.rs:134–146`). No source-row collection was introduced.

The writer still sequences changed-file streams with
`iter(changed).then(execute).try_flatten()` and splits each batch between the kept
and removed writers through a channel of capacity one
(`src/engine/write.rs:534–567`). The separate repaired origin reader remains one
sequential file stream with its copied-path permit retained until drop
(`src/query.rs:145–174`). The new future boxes wrap these flows; they do not spawn
tasks, prefetch files, duplicate batch buffers, or alter cancellation ownership.
Each future retains its original borrowed inputs and mutable `Checks` lifetime.

Verification still checks source binding, table/schema, applied changes, and exact
unchanged receipt-table equality (`src/engine/evidence.rs:625–674`). Complete
association work is inside `checks.deep()` at line 676; inherited multiset work
is inside the deep, non-evidence-only branch at lines 609–610. Boxing introduces
no extra call to either proof and no relaxation of those checks. Serde 1.0.229,
the locked version, serializes `Box<T>` by dereferencing it and deserializes `T`
before boxing it (installed `serde/src/core/ser/impls.rs:460–490` and
`serde/src/core/de/impls.rs:1953–1958`). Thus the box itself adds no serialized
wrapper to `DatasetPin.evidence`; the field's default/omission behavior remains
explicit in `src/model.rs:153`.

The tradeoff is a fixed-size future allocation and indirect polling per invoked
helper, plus a box for each retained receipt result. Those allocations are not
charged as row buffers, and this review does not infer total heap or stack bytes
from source. The number of verification allocations can still grow with the
number of retained selections checked; file inventories and receipt-result pins
still grow with physical layout. Boxing does not remove the O(S) origin scan,
receipt-file probes, materialized-key rereads, or bounded history traversal
described above. It addresses the observed default-stack failure without a new
cache, another representation, or larger configured limits. No speed improvement
or total-memory reduction is claimed.

The reviewer read the parent's
[post-correction oracle result](evidence-related-stack-verified.log), which ends
with the original failing incremental case passing in 63.82 seconds. That is a
correctness/default-stack regression result, not a throughput benchmark. The
parent reports the original stack and resource settings; complete related-suite
and repository acceptance remain pending.

## Test behavior and gaps

The following expectations come from tracing assertions; execution belongs to the
parent's gate.

| Test | Location | Static expected behavior and what it checks |
| --- | --- | --- |
| `receipt_replacement_preserves_duplicates_nulls_source_and_computed_pins` | `tests/evidence_updates.rs:109` | Expected pass: exact adopted pins, distinct equal-value occurrences, null/empty/duplicate receipts, group replacement/deletion counts, retained retry, and subsequent computed group. |
| `first_value_update_preserves_receipts_for_unrelated_rows_in_the_same_file` | `tests/evidence_updates.rs:143` | Expected pass: first rewrite retains survivor origins, removes receipts for both replaced duplicate-key rows, keeps receipt objects protected, and admits a restore point. |
| `receipt_associations_refuse_unknown_rows_overlap_and_conflicting_retries` | `tests/evidence_updates.rs:170` | Expected pass: initial row-range proof rejects an absent row; overlap, conflicting operation identity, and stale base refuse. |
| `nested_receipt_lists_preserve_order_empty_and_null_distinctions` | `tests/evidence_updates.rs:189` | Expected pass: nested order, child null, empty list, and null list survive native storage and deep verification. |
| `evidence_only_refresh_reuses_values_and_contributors_with_new_coverage` | `tests/evidence_updates.rs:232` | Expected pass: aggregate values/contributors remain exact while coverage points at the new source receipt selection; receipts can attach to the derived result. |
| `active_backfill_adopts_new_receipts_without_recomputing_values` | `tests/evidence_updates.rs:256` | Expected pass with `test-util`: receipt selection between processing/readback and resume keeps prepared scalar records and current receipt table. |
| `receipt_processing_survives_process_loss_without_original_input` | `tests/evidence_updates.rs:283` | Expected pass with `test-util`: killed worker resumes durable processing without original input, then exact retry returns the retained result. |

The initial gaps in many-file membership and receipt-phase process loss were
addressed in the final patch by the tests below. No new HTTP receipt-route test
was found. A first-attachment nested receipt test does not establish every native
type works in deep grouped comparison; no broader type claim is made here.

## Final static pass after the repair

| Added or changed path | Formatted location | Verified behavior |
| --- | --- | --- |
| `query::files` / `projected_files` / sequential `NativePartition::execute` | `src/query.rs:59,67,125` | One provider partition retains bounded copied paths and opens only one projected native reader at a time; single-file callers share the same implementation. |
| Stored-origin `associate` branch | `src/engine/evidence.rs:803` | Uses that provider, projects only original file/row, and retains the same semi/anti-join membership semantics. |
| Shared `adopts_receipts` predicate | `src/engine/evidence.rs:81`, `src/engine.rs:397` | Plan and execution use the same completed-source evidence to omit a receipt phase for unchanged/schema/insert-only work. |
| Many-file fixed-budget fixture | `tests/evidence_updates.rs:700` | Maintained compaction creates the hostile physical file count; append retains that layout and origin schema; valid and absent-key receipt checks reach stored-origin association. Passed in the parent's retained focused log. |
| `adopted_receipts_do_not_appear_as_pending_after_cold_validation_recovery` | `tests/evidence_updates.rs:843` | With `test-util`, cold status omits nonexistent receipt work, and resume repeats no completed phases. |
| `inherited_receipt_deletions_survive_process_loss_without_repeating_work` | `tests/evidence_updates.rs:901` | With `test-util`, retained receipt deletion table and applied counts survive process loss without repeated admission/processing/receipt work. |
| Receipt completion classification | `src/engine/retention/retirement.rs:19` | `/receipts` participates in shared active completion, phase recovery, and retained-key classification. |

The final test assertions were read independently. Remaining resource-test limits
include empty stored-origin sources, large receipt/history populations, and
measured complete-path memory/I/O. These limits are not newly established defects.
The parent separately reported default-stack failures. The retained focused log
shows the receipt cases passing after the first corrections; the later oracle
log confirms the original incremental failure passes after the final shared
verification boxing. This report's original expected-pass table describes static
reasoning; the linked logs supply their respective runtime results. A completed
full repository gate is not claimed here.

## Review reasoning and conclusion

H1: receipt-only updates might recompute values. **Refuted** for the supported
operation path: exact pins are adopted and independent parent equality is checked.
H2: origin membership might decode complete source values. **Refuted**: physical
projection reaches the maintained native task; the initial per-file partition
layout created F1, now resolved by the sequential reader. H3: phase recovery might
reopen original receipt inputs. **Refuted**
for retained processing: shared completion lookup precedes copy/processing and the
killed-worker fixture asserts this boundary. H4: history validation might retain
unbounded decoded manifests. **Refuted as an unbounded-memory claim**: cumulative
admission bounds it; traversal and capacity sensitivity remain stated limits.

VERDICT: **APPROVE** for the final static review scope.

The receipt/value separation and unchanged-value adoption are supported by static
evidence. F1 is resolved by the shared sequential projected reader. Coverage of
ordinary semantics and the repaired path is **ADEQUATE for this slice**, supported
by static traces and the parent's retained focused pass. The final boxing review
found no new resource-scaling or payload-semantic defect; the previously failing
oracle also passes. The broader repository gate remains pending. Confidence is
**HIGH** in the reviewed behavior; complete resource scaling remains unqualified.
This review does not establish throughput,
whole-process peak memory, or the 10-million-Comments acceptance target.
