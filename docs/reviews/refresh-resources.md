# Coordinated scalar refresh: resource and reuse review

Review scope: uncommitted changes relative to `9e10497`. This is an independent
static review using the semi-formal code-review and applicable Microsoft Rust
guidelines. The reviewer did not execute tests or edit implementation files.

## Findings

### RR1 — Preserve original request provenance on equivalent retries

**Severity:** WARNING; correctness/recovery. **Status:** closed.

The initial retained-readback equality check compared the
entire `DatasetPin`. Its request contains caller input paths, while
`fingerprint` deliberately uses input digests rather than paths. A build or
enriched update interrupted after readback
can therefore reject an equivalent retry with a relocated input as
`corrupt_phase`, despite passing operation identity validation and reusing all
completed processing. Previously the retained readback returned its original
request. The repair reads the admitted `request/{operation}`
(`src/engine.rs:594`) and retains it in release and group provenance
(`:747`, `:788`), while allowing new caller locations for unfinished admission.
The strict source/group/readback comparison remains (`:799`). The enriched
update regression deletes both original inputs, supplies different nonexistent
locations with the same digests, and asserts that the resumed release exactly
equals the retained readback (`tests/refresh.rs:432`).
The build regression applies the same relocation and original-release equality
assertion (`tests/lifecycle.rs:688`).

### RR2 — Reuse the common checked refresh base during verification

**Severity:** WARNING; performance. **Status:** closed by static call and ownership
trace; no elapsed-time claim.

Initially, `verify_groups` read the update base manifest, then `verify_group`
hashed and deserialized it again for every refreshed group. Because it contains
all group inventories, this repeats work proportional to group count times
manifest size. The repair holds one checked manifest
(`src/engine/backfill/verification.rs:12`, `:51`) and reuses it while the exact
`ObjectRef` remains equal (`:111`). It drops the old value before reading a
different base and verifies each binding's dataset/version even on reuse
(`:164`). A coordinated update therefore reads its common base once within
`verify_groups`; separate change verification still has its own checked read.
There is no global cache or recursive history traversal.

## Patch summary

The patch prepares the source update and all existing scalar groups before one
guarded dataset selection. It separates a material `GroupDefinition` from the
operation that adopts it, records the exact composition parent, and reuses one
group-preparation implementation for standalone backfill and coordinated update.
It also strengthens retained readback checks and rejects incremental reuse of a
source whose dependency eligibility has changed.

The scope is row-local immutable scalar expressions. Broader related-input joins,
affected-group refresh, schema migration, retention, and complete-path performance
qualification are not implemented by this change.

## Function trace

| Function or type | Location | Input → output and verified behavior |
| --- | --- | --- |
| `DatasetPin`, `GroupDefinition`, `ComputedGroup` | `src/model.rs:73`, `:197`, `:216` | Exact source/group pins plus separate material definition, adoption request and composition parent. No recursive embedded history or extra index is introduced. |
| `BackfillSpec::group_definition` | `src/model.rs:205` | Request → owned material definition; omits operation/base identity so unchanged file work can remain reusable. |
| `plan_request` | `src/engine.rs:163` | Request and lifecycle → reusable/required phases; enriched updates include a group-completion marker and check current eligibility. |
| `run_internal` | `src/engine.rs:483` | Public build/update calls → heap-pinned preparation or reconciled exact-operation result after an ordinary competing-attempt error. |
| `prepare_and_select` | `src/engine.rs:515` | Admitted inputs → retained source processing → each selected group's preparation → exact composition → readback → one guarded selection. Every group is prepared before authority changes. |
| `check_update_eligibility`, `check_authority`, `check_base` | `src/engine.rs:1046`, `:1025`, `:1061` | Fresh authority and captured operation → refusal on changed dependency eligibility, execution epoch, definition or selected base. |
| `process_update` | `src/engine/write.rs:85` | Exact base and admitted replacements/deletions → source candidate and actual added/removed rows. Shared source files keep their identities; the obsolete enriched-source refusal is removed. Its new zero-applied-row branch (`:372`) preserves the exact previous source pin. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Request, operation, source and material definition → verified native group and logical/native field mapping. Resolves prepared work, then per-file completions, before source decoding. |
| `reserve_group` | `src/engine/backfill/preparation.rs:180` | Operation/purpose → protected unique directory; durable protection precedes creation. |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:218` | Pending file completions → guarded bundled lifecycle records; clears pending entries after successful acknowledgment. |
| `compute_file` | `src/engine/backfill/preparation.rs:246` | One immutable source file → scalar output and maintained native Avro descriptors. Hashing uses the existing bounded blocking worker; source decoding uses the admitted selective occurrence stream. |
| `file_key`, `read_completion`, `charge_assembly` | `src/engine/backfill.rs:564`, `:584`, `:650` | Exact object/schema/material definition/eligibility → reuse key; retained descriptor → verified native files; aggregate descriptor/reference bytes → bounded assembly charge. |
| `prepare_backfill` | `src/engine/backfill.rs:288` | Captured base → shared preparation → fresh sibling composition and exact parent reference. Compatible sibling changes retry only composition; source/same-group changes refuse. |
| `Capture`, `promotion`, `plan_backfill` | `src/engine/backfill.rs:19`, `:508`, `:138` | Captures both target metadata and adoption binding. Plan and selection compare both, so reselecting identical table bytes still invalidates an old same-group writer. Pending admitted source work prevents strict backfill cutover (`:531`). |
| `validate`, `validate_definition` | `src/engine/backfill.rs:45`, `:55` | Operation constraints remain separate from reusable expression-definition checks. |
| `Expressions::compile`, `evaluate` | `src/engine/backfill/expressions.rs:26`, `:117` | Material definition → maintained DataFusion physical expressions; one derived row per source occurrence with post-evaluation byte checking. |
| `read_combined` | `src/engine/backfill.rs:87` | Verified release → native source/group streams joined by file and ordinal through the shared DataFusion runtime; field names now come from the material definition. |
| `release_objects` | `src/engine.rs:1295` | Release → deduplicated immediate required objects, including parent/base manifests and actual-change tables, for cached-phase availability checking. |
| `verify_release_with_budget`, `verify_changes` | `src/engine.rs:1325`, `:1376` | Serialized format-4 release → request, evidence, source, change-endpoint, group-schema and occurrence checks. Old history is read only where directly referenced. |
| `verify_groups`, `read_manifest`, `read_base`, `verify_group`, `verify_group_data` | `src/engine/backfill/verification.rs:6`, `:134`, `:149`, `:157`, `:200` | Exact composition and adoption bindings → shallow parent/base checks and independent native group coverage. No recursive call to release verification is added. |
| Existing public/transport callers | `src/engine.rs:449`, `:468`; `src/http.rs:65`, `:77`; `src/engine/backfill.rs:241` | CLI/HTTP/application requests use the same services; the refresh policy is not duplicated in transports. |

## Data flow and invariants

| State | Creation, use and lifetime | Property and failure boundary |
| --- | --- | --- |
| Source candidate | Existing update writer → retained processing phase → shared group preparation | Unchanged files retain exact object identity, allowing existing file-completion keys to match. Rewritten files acquire new identities and are recomputed. |
| Material group definition | Initially admitted backfill → `ComputedGroup.definition` → each later update | Adopted operation identity changes without changing expression dependencies. Fields, definition, source schema, source object and eligibility remain part of reuse identity. |
| Prepared group | Captured source metadata and definition → eligibility-scoped prepared/validation keys | Whole-group reuse requires the exact candidate source metadata. Per-file reuse can cross candidate snapshots without weakening final source binding. |
| Resource custody | Selective native scan, assembly permit, expression/batch permits and blocking-worker permits | Extraction preserves serial file execution and existing ownership. Aggregate descriptors are charged before vector extension and retain admission through native append/pin. Cancellation cannot release a detached hash worker's own job/memory permits. |
| Completion history | File bundles and prepared/validation records → existing lifecycle map | The same cumulative serialized-history cap applies before acknowledgment and during cold replay (`src/catalog.rs:139–184`, `:248–275`). Bundling reduces commits but does not establish scalable unbounded history. |
| Candidate composition | All prepared groups plus source → one `DatasetPin` | No source-only publication occurs while an existing group is incomplete. The group marker reports preparation; only the final authority/result commit selects the combination. |
| Parent and group bases | Exact selected parent and each group's adoption base → independent verification | Immediate referenced manifests preserve composition evidence without recursively decoding every ancestor. The parent and unchanged sibling pins are checked during standalone backfill. |
| Cached readback | Retained release → strict candidate equality → immediate-object availability | Availability checking does not rehash previously verified immutable bytes. Missing referenced files refuse before selection. RR1 concerns canonical request identity at this boundary. |
| Retry authority | Dataset lock + fresh view + exact operation/base guards | A competing different selection refuses stale work; identical operation attempts reconcile one durable result. Completed group/file work remains retained. |

Review hypotheses: extraction might release an assembly permit before native
append/pin (refuted by the owning scope); update might publish before all groups
finish (refuted by the serial preparation and final commit); file keys might
include the new adoption request and defeat reuse (refuted); full request equality
might conflict with material operation identity (confirmed, RR1); shallow checks
might nevertheless repeat large manifests per group (confirmed, RR2).

## Tests and execution evidence

The existing backfill test fixtures are moved into `tests/support/scalar.rs`.
Their Parquet input generation, direct Rust expected values, native combined
reader consumption and null/duplicate/Unicode cases remain shared rather than
copied into the new refresh suite. `tests/backfill.rs` retains the full-build
refusal case while dropping the now-obsolete refusal of supported scalar updates.

Inspected refresh assertions:

| Test / location | Expected behavior and inspected evidence |
| --- | --- |
| `refresh_reuses_unchanged_file_computation_and_selects_all_groups_together`, `tests/refresh.rs:151` | Actual child-process work markers equal new source files times group count; old shared files cause no new computation. Deletes external inputs after source processing, compares both computed fields with complete direct Rust reconstruction, preserves old release and exact completion records, and checks reused output membership. |
| `noop_and_delete_all_keep_complete_group_semantics_without_recomputing_unchanged_files`, `:257` | No-op preserves exact source and group pins with no file computation; delete-all retains typed complete empty groups and readable history. |
| `refresh_resumes_after_partial_groups_readback_and_lost_selection_response`, `:286` | SIGKILL after first group, all groups, readback or selection; old selection remains before cutover, already completed work is not repeated, readback/selection recovery emits no work markers, and final values match Rust reconstruction. |
| `independent_refresh_verifier_rejects_missing_groups_inputs_and_false_source_interpretations`, `:338` | Rewrites and rehashes deliberately malformed release JSON so missing group, altered recipe/request, omitted/rebound inherited input, parent omission, stale boundary and changed source definition must reach semantic refusal. |
| `invalidated_source_refuses_incremental_admission_without_changing_authority`, `:410` | Plan and run return `stale_eligibility`; exact lifecycle state remains unchanged after rejected admission. |
| `relocated_retry_preserves_original_request_and_readback`, `:432` | Exercises RR1 after enriched readback with original input files removed; requires exact retained release rather than a replacement upload/readback. |
| `relocated_build_retry_keeps_the_original_readback_request`, `tests/lifecycle.rs:688` | Covers RR1 on the shared build path with the same exact-readback assertion after removing and changing the input locator. |
| `noop_target_reselection_is_a_stale_group_conflict_and_inherited_siblings_are_exact`, `tests/refresh.rs:463` | Same bytes under a new adoption request still cause `stale_group`; a later field addition preserves exact refreshed siblings, and rehashed forged omissions/bindings/pins/parent references refuse. |
| `refresh_and_backfill_races_preserve_captured_source_and_selected_groups`, `:550` | Admitted source changes block strict backfill cutover; routine refresh can select its complete capture while later work is admitted. Stale later updates refuse; subsequent catch-up reuses completed file work. |
| `failed_group_keeps_prepared_source_and_other_groups_without_partial_selection`, `:618` | Runtime scalar failure leaves the old release selected. Source and preceding group completions survive and need no new acknowledgment when resumed to that successful group boundary. |

These assertion paths support the repaired behavior. The reviewer inspected the
owner's successful full gate output in `docs/reviews/refresh-check.log`: lint,
unit, backfill, lifecycle, refresh, update and documentation test sections pass,
including the build relocation regression. The final additional lint pass also
finishes successfully. The original no-op failure is retained in
`docs/reviews/refresh-noop-failed.log`. This is inspected owner execution evidence;
no tests were run by this reviewer.

The no-op repair makes a concrete reduction: after native writers report zero
actual additions and removals, it skips candidate append/pin and preserves the
source metadata identity (`src/engine/write.rs:346`, `:372`). Prepared group keys
then resolve existing tables. Empty added/removed tables and exact change endpoints
still exist (`:378–398`). Schema/key checks and actual writes precede this branch;
equal total row counts alone never qualify a nonempty replacement as unchanged.
The test verifies exact pins and absence of file computation; it is not a timing
benchmark. Source discovery and empty native table creation remain.

## Performance and qualification boundaries

The implementation reuses completed computation before opening source payloads,
but source update discovery and independent full release verification still read
population-dependent data. Serial group preparation bounds simultaneous work; it
does not establish total elapsed-time performance. Metadata admission is a finite
serialized-size policy, not measured whole-process resident-memory accounting.
Scalar functions and native codecs can allocate before post-operation checks.
Compressed-byte disk estimates remain estimates for expanding expressions.

## Conclusion

**VERDICT: APPROVE.** RR1 and RR2 are closed, and the inspected owner gate passes.
The shared preparation and selection paths preserve the previous resource and
custody invariants while removing duplicated preparation and redundant no-op
metadata writes. Changed-path
coverage is **ADEQUATE** for this scalar checkpoint; confidence is **HIGH** in the
static traces. This does not qualify broader dependent joins, full resident-memory
limits, destructive maintenance, hosted operation or the Comments performance target.
