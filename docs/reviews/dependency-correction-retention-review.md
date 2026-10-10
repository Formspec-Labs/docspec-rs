# Dependency correction: retention and restoration review

The static paths preserve the original execution references and exact target snapshot, and correction authority cannot retire through the phase-retirement API. I found no demonstrated correctness defect. One required preservation case lacks an isolated test: a standalone qualification must remain sufficient after overlapping scalar roots disappear or are replaced. The new owner-creation fallback for that case also lacks direct coverage.

**Verdict: APPROVE with the focused coverage warning below.** No correctness failure or architecture change is inferred from the missing fixture. Isolated standalone ownership remains unqualified and belongs in the broader retention-lifetime work.

Reviewed the working tree based on `5508a11`, with scope limited to the dependency-correction retention, restoration, immutable proof, and ordinary-read paths. This review used static inspection only. No builds, tests, formatting, benchmarks, commits, or source edits were performed. The implementing agent owns gate execution.

## Finding

### F1 — Exercise standalone qualification retention without overlapping roots

- **Severity:** WARNING / P2
- **Category:** test-coverage
- **Locations:** `tests/dependency_correction_lifecycle.rs:178`, `src/engine/retention/inventory.rs:1078`, `src/engine/retention/owners.rs:856`
- **Description:** The new restoration fixture selects a qualified sibling result and retires the standalone qualification together with the old phases. This proves the selected result continues to protect evidence, but it does not prove a live standalone qualification independently retains its origins, target native closure, or owner-creation evidence. All original result/readback rows also remain present. The owner fallback's new qualification branch is not reached by an isolated assertion.
- **Evidence chain:** The fixture selects `sibling` at `tests/dependency_correction_lifecycle.rs:186`, includes `dependency-qualification/` in the retired keys at line 247, and accepts any blocker at line 274. `Inventory::record` has a distinct live-qualification branch at `src/engine/retention/inventory.rs:1078`; the corresponding unit test calls only shallow `reference_roots` and explicitly requires no queued reads at lines 2004–2033. The existing owner test `validation_only_origins_qualify_original_creations_after_cache_replacement` at `src/engine/retention/owners/tests.rs:990` deliberately isolates a validation root, not a qualification root. Therefore it does not exercise the added dispatch at `owners.rs:856`.
- **Concrete next step:** Add a fixture that retains an actual successful qualification before result selection, retires or replaces the overlapping scalar file/prepared/validation roots, and verifies no result/readback or old direct-creation row masks the qualification. Assert that inspection names the qualification as the blocker for the origins and a target-only native dependency; capture and restore those exact objects. Use the same isolated state to acquire a foreign owner's original descriptor/output through the existing creation-witness path and show an active qualification binding keeps its inherited owner grant in use. The existing validation-only fixture provides a suitable pattern. This is a bounded regression test for the added root family, not a new retention mechanism.

## Patch summary

`DependencyQualification` adds immutable supplemental evidence beside the original SQL declaration. Its value binds the checker, exact definition scope, cumulative omitted field IDs, first-correction revision, immutable origins object, and target native metadata/snapshot/schema (`src/model.rs:871`). `ComputedGroup` embeds it without changing the original request or input bindings (`src/model.rs:851`).

`qualify_corrections` writes the same proof as an independently retained completion and propagates existing owner witnesses (`src/engine/corrections.rs:137`). The shared inventory follows proof dependencies both from computed groups and from standalone qualification records (`src/engine/retention/inventory.rs:910`, `:1078`). Explicit phase retirement can end qualification recovery, while immutable decisions and current requirements stay outside retirement scope (`src/engine/retention/retirement.rs:19`).

Important callers are ordinary operation composition (`src/engine.rs:1679`), backfill composition (`src/engine/backfill.rs:761`), immutable group verification (`src/engine/backfill/verification.rs:430`), retention inspection (`src/engine/retention/inventory.rs:115`), restore dependency capture (`:148`), and owner acquisition (`src/engine/retention/owners.rs:714`).

## Function trace

| Function | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `validate_qualification` | `src/engine/corrections.rs:10` | Inline proof | Success/refusal | Checks checker, scope format, positive cutoff/fields, and common origins/target owner. |
| `qualification_key` | `src/engine/corrections.rs:31` | Valid proof | Digest-addressed key | Hashes every serialized proof field, including exact immutable references. |
| `check_association` | `src/engine/corrections.rs:39` | Proof, binding, source | Success/refusal | Requires the original origins, exact definition scope, exact target, and producing dataset to agree. |
| `expected` / `candidate` | `src/engine/corrections.rs:58`, `:66` | Current requirements, original binding, target | Immutable proof | Copies the cumulative fields and original cutoff; absent origins remain unknown. |
| `inherits` / `inherits_groups` | `src/engine/corrections.rs:82`, `:107` | Child and parent bindings | Boolean | Preserves original execution association; allows added proof or cumulative fields without removing previous proof. |
| `check_prepared_correction` | `src/engine/corrections.rs:120` | Captured view, originals, source, definition | Success/refusal | Resolves applicable requirements and checks metadata correspondence before reuse proceeds. |
| `qualify_corrections` | `src/engine/corrections.rs:137` | Operation, candidate dataset, witnesses, request-local checks | Updated candidate and retained completion | Reuses exact active proof content and its witnesses; otherwise checks, locks, reopens the view, and commits proof plus binding. |
| `guard_corrections` | `src/engine/corrections.rs:214` | Guarded lifecycle view and candidate | Success/refusal | Reconstructs expected proof from current authority and compares exact inline value. Performs no file or row work. |
| `scope_key` | `src/engine/corrections/records.rs:21` | Exact group definition | Scope key | Includes the scalar processor and full definition. |
| `correct_dependencies` / admitted body | `src/engine/corrections/records.rs:42`, `:54` | Correction request | Immutable decision | Uses dataset lock, union of fields, original first revision, one guarded decision/requirements commit, and lost-response reconciliation. |
| `requirements` | `src/engine/corrections/records.rs:114` | View and definition | Current typed requirements | Checks scope content and native revision; no history traversal or payload scan. |
| `correction_decision` | `src/engine/corrections/records.rs:141` | View and retry request | Original decision/refusal | Requires exact request content and actual retained revision. |
| `validate` | `src/engine/corrections/records.rs:169` | Correction | Success/refusal | Requires a valid scalar definition, reason, and positive omitted field IDs. |
| `Checks::qualification` | `src/engine/backfill/origins/checks.rs:51` | Proof, binding, source/group pins | Success/refusal | Combines original association validation with supplemental correspondence. |
| `Checks::correspondence` | `src/engine/backfill/origins/checks.rs:73` | Proof and target | Cached success/refusal | Requires nonempty originals, exact source object identities, exact file set/size/rows, and supported field interpretation. |
| `Checks::correction_file` | `src/engine/backfill/origins/checks.rs:129` | Original execution, target schema, fields | Success/refusal | Requires pre-correction acknowledged generation, original declaration/metadata, known processor, and equal primitive-field interpretation. |
| `Checks::source_files` | `src/engine/backfill/origins/checks.rs:257` | Resolved native table | Shared file facts | Caches path/size/row facts by immutable metadata location and charges retained metadata. |
| `Checks::check` | `src/engine/backfill/origins/checks.rs:370` | Original descriptor and source/group association | Success/refusal | Checks original/output association and original file generations; shares the new source-file cache at line 494. |
| `completion_records` | `src/engine.rs:108` | Completion and existing witnesses | Atomic record pair | Stores completion and exact content/revision-bound reference binding together. |
| `Inventory::computed` / `qualification` | `src/engine/retention/inventory.rs:910`, `:921` | Inline group/proof | Enqueued immutable roots | Follows original executions and the exact target snapshot even if the qualification's lifecycle completion retired. |
| `Inventory::record` | `src/engine/retention/inventory.rs:1026` | Typed lifecycle row | Roots/refusal | Validates correction/requirements identities; live qualification dispatch binds owner, key, cutoff, origins, and exact target. |
| `Inventory::file_completion` | `src/engine/retention/inventory.rs:931` | Original file completion | Evidence roots | Retains source, original schema metadata leaf, descriptor, outputs, and original declaration. |
| `Inventory::snapshot` / `expand` | `src/engine/retention/inventory.rs:858`, `:1482` | Immutable root queue | Native/evidence closure | Checks target snapshot/schema on native expansion; reads manifests and origin/declaration metadata, without decoding payload rows. |
| `collect_inventory` | `src/engine/retention/inventory.rs:174` | Catalog and inventory | Boundaries/closure | Validates retirements before skipping exact retired generations, then verifies live owner bindings. |
| `restore_dependencies` | `src/engine/retention/inventory.rs:139` | Restore owner | Exact object set | Reuses common active-root traversal and retains native catalog registrations; resolves still-missing native identities under exclusive maintenance. |
| `completion_key` / `scope_key` | `src/engine/retention/retirement.rs:19`, `:38` | Logical record key | Retirement membership | Admits qualification recovery, excludes correction/requirements authority. |
| `confirm_creation` | `src/engine/retention/owners.rs:784` | Exact requested objects | Success/refusal | Searches direct typed creation evidence, then immutable original-execution descriptors for missing identities. |
| `visit_original_creation` | `src/engine/retention/owners.rs:828` | Owner rows and missing identities | Original creation observations | Adds qualification roots; ignores foreign anchors, checks availability, and observes only actual declaration/descriptor/output creations. |
| `ReferenceProtections::inherit` | `src/engine/retention/owners.rs:83` | Existing/incoming witnesses | Combined or unknown scope | Preserves unknown provenance instead of upgrading it to a qualified empty scope. |
| `retained_protections` / `local_binding` | `src/engine/retention/owners.rs:439`, `:457` | Exact retained claim | Existing owner witnesses | Verifies content/revision binding and rejects retired generations; avoids reopening owners for propagation. |
| `validate_binding` | `src/engine/retention/owners.rs:236` | Claim and witness record | Direct foreign references | Uses the shared shallow root decoder to validate the bound claim. |
| `Engine::read_group` / `check_read_release` | `src/engine/read.rs:13`, `:53` | Exact retained release and group | Stream | Resolves retained immutable selection and reads the requested pin. Does not call qualification, current requirements, or dependency traversal. |

## Data flow and invariants

1. **Original execution identity remains immutable.** `candidate` copies the existing origins reference (`corrections.rs:76`); qualification mutates only the newly assembled binding (`:168`, `:171`). `inherits` compares original request, origins, definition, SQL bindings, native IDs, source metadata, and base manifest before allowing a supplemental change (`:82`). The original file generations and declarations remain in the immutable origins descriptor, expanded by `inventory.rs:1505`.

2. **Correction authority and recovery have separate lifetimes.** A decision and cumulative requirements commit together (`records.rs:94`). The first correction revision survives subsequent unions (`:75`). Qualification recovery is a phase key (`retirement.rs:34`), while decisions and requirements match no retirement scope (`:38`). Inventory still validates both authority families (`inventory.rs:1034`, `:1046`). A forged retirement receipt naming an unsupported scope key fails validation (`retirement.rs:134`).

3. **An inline proof remains a physical retention root after standalone recovery retires.** `computed` always visits an embedded proof (`inventory.rs:915`). Retirement filtering acts on the exact lifecycle generation (`:352`), not on immutable references embedded in a retained result. Qualification traversal enqueues both origins and exact native target (`:926`). It does not substitute the current source head.

4. **Original schema context does not retain unrelated old payloads.** A file completion keeps its `source_metadata` as a leaf (`inventory.rs:933`), while group dependencies and the qualification target retain their exact snapshot closure (`:952`, `:928`). This preserves the original read interpretation without expanding each original file's unrelated historic table contents.

5. **Foreign ownership uses existing witnesses.** Composition combines prepared and selected-result witnesses before qualification (`backfill.rs:739`). A retained proof inherits its same-generation witness (`corrections.rs:164`); a new proof is committed with the candidate's already acquired scope (`:200`). `completion_records` writes the proof/binding atomically (`engine.rs:108`). Grant retirement rejects any live binding that still names that grant (`retention/grants.rs:125`, `:152`). No new owner-grant format or authority is introduced. Isolated qualification coverage remains F1.

6. **Ordinary reads do not revalidate corrections.** Managed reads use exact result/readback equality (`read.rs:83`) and then `read_pin` (`:151`). Catalog-free reads resolve the manifest and read the selected pin (`:120`). Supplemental checking runs at preparation/composition/explicit verification boundaries (`corrections.rs:120`, `:137`; `backfill/verification.rs:430`), not per decoded row. The correspondence loops scale with original files and omitted field IDs (`origins/checks.rs:110`, `:152`). No measured throughput or memory improvement is claimed.

## Tests and concrete edge cases

These are static predictions from the assertions and implementation, not executed results.

| Test / scenario | Evidence | Expected behavior and limits |
| --- | --- | --- |
| `qualified_inherited_group_restores_original_evidence_after_recovery_retirement` | `tests/dependency_correction_lifecycle.rs:178` | Expected to preserve the original request/bindings and table, retain explicit evidence candidates after phase retirement, restore missing objects, read exact values, and retain correction authority. The fixture is masked by surviving results for standalone-root qualification. |
| `correction_registry_is_typed_object_free_and_revision_bound` | `src/engine/retention/inventory.rs:1910` | Expected to reject wrong decision revision/key/unknown fields, keep correction authority object-free, and expose proof origins/target metadata as shallow references. It does not expand a standalone proof's native closure. |
| `qualification_recovery_can_retire_without_retiring_correction_authority` | `src/engine/retention/inventory.rs:2047` | Expected to classify qualification as phase recovery and exclude correction/requirements keys. Directly covers the changed dispatch predicate. |
| `cached_originals_reuse_metadata_and_recheck_caller_association` | `src/engine/backfill/origins/checks.rs:603` | Expected to reuse the same source-file cache allocation without another metadata charge (`:683`) and reject changed interpretation. This is an empty-file cache fixture, not a positive correction-correspondence test. |
| `inherited_sibling_selection_adds_cumulative_proof_without_rewriting_values` | `tests/dependency_corrections.rs:234` | Expected to keep original bindings, outputs, and file-completion records while adding cumulative proof fields. |
| `correction_after_readback_requires_new_operation_and_reuses_originals` | `tests/dependency_corrections.rs:298` | Expected to refuse an altered old readback, reuse original values for a fresh operation, and retain the old readback bytes/content. |
| `validation_only_origins_qualify_original_creations_after_cache_replacement` | `src/engine/retention/owners/tests.rs:990` | Expected to qualify original creations through a validation-only anchor after direct creation rows are replaced, then acquire an owner grant. Useful existing primitive coverage; it does not exercise the new qualification dispatch. |
| `retry_conflict_and_cumulative_corrections_preserve_authority` | `src/engine/corrections/records.rs:228` | Expected to preserve head/global authority, reject changed retry content, union omitted fields, and retain the first cutoff. |
| `concurrent_corrections_preserve_both_fields_and_retry_generations` | `src/engine/corrections/records.rs:301` | Expected to serialize through the dataset lock and retain both requirement additions and original retry responses. |
| `lost_commit_response_reconciles_actual_native_generation` | `src/engine/corrections/records.rs:336` | Expected to reconcile the immutable decision after commit-response loss without a second decision. |
| Qualification completion retires but selected proof remains | `inventory.rs:352`, `:915` | Correct by static trace; covered by the lifecycle fixture. |
| Qualification is the sole remaining original-evidence root, target has distinct metadata, old cache rows are replaced | `inventory.rs:1078`, `owners.rs:856` | Static implementation appears sound; direct end-to-end coverage missing. This is F1. |
| Legacy origins or post-cutoff original acknowledgments | `origins/checks.rs:85`, `:137`, `:141` | Empty/absent historical evidence refuses as unknown; late generations refuse as mismatch. No unsafe fabricated declaration. |

## Exploration record

- **H1:** Retiring the standalone completion might drop evidence still embedded in a result. **Refuted:** typed computed-group traversal independently follows the inline proof (`inventory.rs:915`).
- **H2:** Qualification might retain only target metadata, losing the selected native files. **Refuted by static trace:** `snapshot` preserves snapshot/schema and native expansion follows its manifest inventory (`inventory.rs:858`, `:1540`). End-to-end standalone evidence remains unqualified by tests.
- **H3:** The new root might fail closed as an unknown record in owner-witness validation or restore. **Refuted by static trace:** both use the common typed inventory; its added qualification branch is visible to shallow claims and full restore traversal (`inventory.rs:438`, `:1078`).
- **H4:** Retention could mistake correction authority for disposable completed recovery. **Refuted:** only qualification keys enter completion/retirement scope (`retirement.rs:19`).
- **H5:** Ordinary reads might inherit new historical revalidation. **Refuted:** managed and catalog-free read entry points do not invoke supplemental qualification (`read.rs:13`, `:120`).

## Conclusion

**VERDICT: APPROVE**

The inspected implementation preserves the required immutable references, separates correction authority from phase recovery, and reuses the existing ownership machinery without adding row-level read work. No concrete source-code defect was established. The selected inline-proof lifecycle is exercised by the new integration fixture. Track F1 in the broader retention-lifetime qualification work and avoid claiming that isolated standalone ownership has already been exercised.

- **Coverage of changed paths:** INSUFFICIENT for the standalone qualification retention and creation-witness path; existing selected-result and primitive coverage is useful.
- **Confidence:** HIGH in the cited static traces and coverage gap; MEDIUM in end-to-end standalone behavior because it was neither directly covered nor executed in this review.
