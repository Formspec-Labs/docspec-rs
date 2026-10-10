# Scalar original-execution verification review

Current static disposition: **APPROVE**. The final disposition below closes the
findings retained here as review history; execution evidence belongs to the
implementation checkpoint.

Review basis: uncommitted changes against `8cb752574288e9830252c4f8cb7d439ae7311548`, inspected on 2026-10-10. This is a blind static review. No builds, tests, benchmarks, or corpus reads were run. The implementation owner owns execution evidence. Concurrent retention-owner changes are outside this review.

## Findings

### F1 — Recheck material semantics before returning a cached group

**Severity:** WARNING / P2. **Category:** correctness. **Location:** `src/engine/backfill/origins/checks.rs:259`.

The warm group-cache path can accept a source association that the cold path rejects. `check` recompiles the new source association, but the cached-group return compares only row count and output objects. The comparison between captured and current material dependency digests occurs afterward, at `src/engine/backfill/origins/checks.rs:268`.

Evidence chain:

- `check_group_dependencies` accepts a different physical source metadata object when its logical marker matches the captured logical source (`src/engine/backfill/preparation.rs:615`). This is needed for compaction.
- The association cache uses both the originals path and the current source metadata path, so a new source is compiled (`src/engine/backfill/origins/checks.rs:205`).
- `Expressions::compile` includes consumed fields' initial defaults, requiredness, type, and observed documentation in `dependencies`; it does not put those source semantics in the generated output schema (`src/engine/backfill/expressions.rs:136`, `src/engine/backfill/expressions.rs:158`).
- A second source metadata object can therefore change an initial read default, or documentation consumed by `arrow_metadata`, while retaining the same field IDs, output type, output mapping, row count, and logical marker. The early checks pass. A previously verified originals path then returns success before checking the changed dependency digest. A fresh `Checks` rejects it at the captured/current comparison.

Supported writers preserve these semantics; this is a malformed-association validation gap, not evidence of a valid writer producing wrong values. It still violates the intended rule that caching may skip immutable parsing but may not skip caller-specific association checks.

**Suggested correction:** retain the captured material dependency digest in `CheckedGroup` and compare it with every current association before the warm return, or move the existing comparison above that return while reusing compiled facts. Add a cold/warm regression using the same output type with different initial defaults or observed documentation.

**Disposition:** open at initial review.

### F2 — Carry the cache through retained-readback recovery

**Severity:** WARNING / P2. **Category:** performance. **Location:** `src/engine/backfill.rs:937`.

A resumed backfill still repeats the new original-execution work within one request. `retained_backfill_readback` constructs a fresh composition `Checks` for every retained readback. The caller then creates its request-local `Checks` only after that helper returns (`src/engine/backfill.rs:613`). The composition loop calls `verify_groups` again (`src/engine/backfill.rs:731`), and the recorded-result branch calls `check_release_available` (`src/engine/backfill.rs:775`), whose `verify_binding_dependencies` creates another fresh `Checks` (`src/engine/backfill/verification.rs:15`).

Each cold scalar check reads originals, declaration/schema JSON, source/output native inventories, and file descriptors. For multiple retained sibling-composition readbacks, this repeats work proportional to retained readbacks times origin files. These are immutable facts from the same request; no new payload or authority boundary requires the repeated qualification. This conflicts with the requested reuse across validation, readback, and retry. No latency or throughput claim is made from this static trace.

**Suggested correction:** create request check state before retained-readback lookup and pass it through composition and availability checks, preserving the availability checks and caller-specific comparisons. Extend the existing cache-read regression or instrumentation to prove resumed readback does not reopen the same scalar metadata.

**Disposition:** open at initial review. The concurrent assembly change already routes `FileAssembly::append` through `Checks::scalar_file`, which removes the separate assembly-to-origin descriptor reread (`src/engine/backfill/preparation.rs:46`, `src/engine/verification.rs:45`). That improvement is independent of this remaining recovery path.

### F3 — Preserve the deep-value regression's intended execution path

**Severity:** WARNING / P2. **Category:** test-coverage. **Location:** `tests/backfill.rs:860`.

`native_malformed_group_coverage_and_field_mapping_refuse` expects the `values` case to return `invalid_group_values`. It replaces the group with a table owned by an alternate dataset while leaving the original scalar origins attached (`tests/backfill.rs:823`, `tests/backfill.rs:846`). The new `scalar_origins` call precedes `verify_group_data` (`src/engine/backfill/verification.rs:239`) and rejects the owner/output association as `invalid_group` (`src/engine/backfill/origins/checks.rs:188`). The test will fail its error-code assertion and no longer reach its intended deep value reconstruction. This conclusion is static, not an observed test result.

**Suggested correction:** retain an explicit association-mismatch case, and make the payload-corruption cases internally consistent with the new metadata or explicitly use a legacy binding with absent origins. Preserve the `invalid_group_values` assertion for the deep-value case rather than weakening it to any refusal.

**Disposition:** open at initial review.

## Patch summary and scope

The patch retains the original declaration and winning file-completion generations in immutable JSON descriptors. Prepared completions, validation completions, and computed-group bindings share one origins reference. Metadata verification associates these records with the selected source and output without turning absent legacy records into newly invented provenance.

The reviewed production scope is `src/engine/backfill/origins.rs`, `src/engine/backfill/origins/checks.rs`, `src/engine/backfill/preparation.rs`, `src/engine/backfill.rs`, `src/engine/backfill/verification.rs`, `src/engine/verification.rs`, the affected creation/readback paths in `src/engine.rs`, and the additive `ComputedGroup` field in `src/model.rs`. Relevant changed tests and unchanged schema/compaction/read callers were read. Retention inventory and owner changes were inspected only where needed to understand the descriptor boundary; their full review belongs to the separate retention review.

Requirements used: `AGENTS.md`, `docs/dependency-corrections.md`, R3 schema/backfill requirements, and Q4/Q7 in the operations qualification requirements. This increment supplies original identity; it does not implement or qualify scoped dependency correction itself.

## Function trace

| Function / method | Location | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `origins::write` | `src/engine/backfill/origins.rs:50` | Serializable record → immutable `ObjectRef` | Counts JSON before allocation, reserves its buffer, writes once, and hashes the serialized bytes. |
| `origins::read` | `src/engine/backfill/origins.rs:72` | Object identity, budget → typed record | Delegates to the existing bounded, hash-bound manifest reader. |
| `origins::Checks::identity` | `src/engine/backfill/origins/checks.rs:40` | Object → remembered identity | Refuses conflicting owner/digest/size for the same path within the request. |
| `admit` | `src/engine/backfill/origins/checks.rs:51` | Metadata bytes → retained permit | Bounds cumulative serialized metadata and reserves an expansion allowance. |
| `original`, `declaration`, `metadata` | `src/engine/backfill/origins/checks.rs:64`, `:75`, `:86` | Object → shared decoded record | Checks exact identity before returning a cached value; cold reads retain worker custody. |
| `table` | `src/engine/backfill/origins/checks.rs:97` | Native snapshot reference → read-only table | Requires current schema and snapshot IDs to match the retained reference. |
| `compile` | `src/engine/backfill/origins/checks.rs:114` | Declaration and schema → compiled material facts | Requires the scalar processor and recompiles retained aliases against stable IDs; caches by both exact object paths. |
| `file` | `src/engine/backfill/origins/checks.rs:146` | Completion and schema → shared native descriptors | Rechecks all completion fields and schema on cache hits; cold reads use `read_completion`. |
| `check` | `src/engine/backfill/origins/checks.rs:176` | Origins plus caller source/group/definition/bindings → success/refusal | Checks owner, definition, SQL binding, source/output pins, object membership, native schemas, file keys, positive generations, exact output descriptors, and complete source/output coverage. F1 limits the warm semantic-association check. |
| `Checks::compose` | `src/engine/verification.rs:40` | Existing check state → composition scope | Disables table payload inspection while preserving immutable scalar metadata caches. |
| `Checks::scalar_file`, `scalar_origins` | `src/engine/verification.rs:45`, `:56` | Completion or optional origins → qualified metadata | Lazily allocates the boxed shared scalar checker; absent origins remain optional. |
| `FileAssembly::append` | `src/engine/backfill/preparation.rs:25` | Acknowledged winners → files, protections, origins | Checks the expected file key and retains the canonical returned generation/completion; uses the same descriptor cache as qualification. |
| `prepare_group` | `src/engine/backfill/preparation.rs:64` | Captured request/source/group → `PreparedGroup` | Uses original winners, emits one origins descriptor for a new preparation, verifies it before validation, and binds the same reference into validation and output. |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:441` | Pending files → canonical winners | Returns existing retained winners; new winners receive the actual committed revision before assembly. |
| `read_completion` | `src/engine/backfill.rs:1084` | Completion/schema → native descriptors | Hashes and decodes the same bounded buffer; refuses duplicate listed outputs, descriptor mismatch, row overflow, and output membership/size differences. |
| `charge_assembly` | `src/engine/backfill.rs:1175` | Current size and completion → admitted size | Includes the completion's new provenance fields and descriptor/object allowance. |
| `check_group_dependencies`, `check_validation` | `src/engine/backfill/preparation.rs:598`, `:632` | Captured/current references → success/refusal | Permits the established logical compaction identity; validation also requires exact origins equality. |
| `verify_binding_dependencies` | `src/engine/backfill/verification.rs:7` | Dataset → metadata qualification | Rechecks binding history and origins without value decoding. F2 limits cache lifetime. |
| `verify_groups` | `src/engine/backfill/verification.rs:28` | Dataset/shared checks → qualified composition | Checks operation-specific inheritance, then history, scalar origins, and existing group-data verification. |
| `verify_group_data` | `src/engine/backfill/verification.rs:494` | Source/output/binding → routine or deep result | Retains type/mapping checks; only deep mode reconstructs coverage and values. |
| Standalone backfill composition | `src/engine/backfill.rs:606` | Prepared or retained readback → selected release | Reuses check state through the new composition loop; the earlier retained-readback helper remains outside it. |
| Coordinated source/group preparation | `src/engine.rs:1355`, `src/engine.rs:1518`, `src/engine.rs:1621` | Source operation plus inherited groups → release | Shares state across preparation and composition; writes returned origins into each computed binding. |
| `release_objects`, `check_release_available` | `src/engine.rs:2387`, `src/engine.rs:2445` | Release → availability checks | Adds direct origins references; nonrelated cached recovery also checks binding dependencies. |
| Hosted and catalog-free reads | `src/engine/read.rs:13`, `src/engine/read.rs:120`, `src/engine.rs:2455` | Exact release/group → stream | Hosted reads match the retained release; catalog-free reads resolve its immutable manifest. Neither invokes scalar origins verification. |

## Data flow and invariants

- **Original declaration:** created from the actually compiled `GroupDefinition` and exact inferred SQL field map before file execution. Newly computed completions reference it and the original operation; reused completions keep the returned winner unchanged (`src/engine/backfill/preparation.rs:133`, `:195`, `:465`). The descriptor escapes through immutable file and group records, not through an adoption chain.
- **Generation and output association:** assembly uses the acknowledgment's retained completion and revision, then writes that exact record into `ScalarOrigins.files`. Verification associates every listed source path with the captured native source, consumes every selected output descriptor exactly once, and requires no remaining source or output (`src/engine/backfill/origins/checks.rs:293`). This is metadata correspondence; routine scope does not prove every output value.
- **Legacy unknowns:** `execution` and `origins` deserialize as `None`; an absent descriptor skips only the new origin check. A present origins descriptor containing an old completion still checks file/output association but does not synthesize an execution declaration (`src/engine/backfill.rs:42`, `src/model.rs:831`, `src/engine/backfill/origins/checks.rs:305`). This does not qualify future supplemental dependencies.
- **Schema meaning:** compilation binds retained aliases to stable IDs. It normalizes display names and future write defaults while retaining initial defaults, types, requiredness, and documentation observed by SQL (`src/engine/backfill/expressions.rs:39`, `:136`). Renames and reorder can reuse material work; removed IDs refuse. F1 is the exception when a changed source association hits a warm group cache.
- **Compaction:** the origins descriptor remains bound to its originally consumed physical source, while `check_group_dependencies` permits a verified replacement with the same logical metadata. The original file set is checked against the captured source, not wrongly against rewritten physical files (`src/engine/backfill/preparation.rs:604`, `src/engine/backfill/origins/checks.rs:243`, `:267`).
- **Bounded work:** JSON and Avro inputs are bounded before reads; metadata caches accumulate a request allowance; native inventories use existing bounded storage helpers; original-file loops yield (`src/engine/inputs.rs:399`, `src/engine/backfill.rs:1089`, `src/engine/backfill/origins/checks.rs:51`, `:294`, `src/storage.rs:482`). The expansion reservation is an accounting allowance, not measured whole-process memory evidence. F2 identifies avoidable repeated cold work.
- **Ordinary reads:** the new metadata graph is used by preparation, explicit verification, and cached-result adoption. The hosted and catalog-free data read paths retain their separate immutable-selection checks (`src/engine/read.rs:53`, `src/engine.rs:2455`).

## Test behavior and edge cases

The following assessments are based on source traces; none is an execution result.

| Test | Location | Relevant behavior and expected effect |
| --- | --- | --- |
| `cached_originals_reuse_metadata_and_recheck_caller_association` | `src/engine/backfill/origins/checks.rs:360` | Exercises an empty origin set; deletes previously parsed metadata and expects warm success without additional charge; then checks changed rows, output identity, and native mapping refuse. It does not test changed material source interpretation, leaving F1 uncovered. |
| `completion_descriptors_refuse_duplicate_outputs_and_changed_bytes` | `src/engine/backfill/file_completion_tests.rs:155` | Duplicate outputs should reach the new uniqueness check; changed digest/size should fail the same-buffer hash check. |
| `concurrent_acknowledgments_assemble_one_winner_with_its_own_scope` | `src/engine/backfill/file_completion_tests.rs:183` | Both attempts must assemble the returned winner and preserve its generation, execution, output identity, and protection, even after the loser's descriptor disappears. |
| `stale_source_retains_files_and_catchup_reuses_unchanged_objects` | `tests/backfill.rs:314` | Captures original lifecycle records, forces both reused and new files, compares descriptor records and generations to retained winners, reopens, and requires the same origins object through validation and selection. |
| `empty_source_completions_retain_dependencies_without_file_work` | `tests/backfill_dependencies.rs:167` | Uses worker-stop/resume preparation and validation cases; empty origins must be known-empty, and the same descriptor must survive recovery. |
| `legacy_group_completions_resume_and_keep_unknown_dependencies` | `tests/backfill_dependencies.rs:175` | Removes additive metadata after retiring modern fixture generations; output reuse must preserve `origins=None` without rewriting old records. |
| `legacy_file_reuse_keeps_original_execution_unknown` | `tests/backfill_dependencies.rs:183` | Removes original execution and schema metadata from retained files, resumes reuse, and asserts the descriptor contains null unknowns rather than the adopting operation. |
| `rename_reorder_keeps_source_and_computed_files_and_old_reads` | `tests/schema.rs:93` | Supported stable-ID rename/reorder should preserve file identity, computed values, and old reads; explicit deep verification also exercises origins against the evolved binding. |
| `repeated_compaction_noop_updates_reuse_groups_first_computed_on_a_representation` | `tests/compaction.rs:179` | Distinct physical source metadata share one logical state; the prepared group and original completion records remain unchanged, while deep verification checks the final result. |
| `rename_reorder_and_name_reuse_preserve_bound_values_and_material_identity` | `src/engine/backfill/expressions/tests.rs:46` | Stable IDs retain the original SQL meaning; a same-name replacement, absent original ID, and extra input binding do not silently reinterpret it. |
| `constants_ignore_schema_but_consumed_types_and_metadata_observers_invalidate` | `src/engine/backfill/expressions/tests.rs:108` | Establishes the material-digest distinction used in F1, including observable documentation changes. |
| `native_malformed_group_coverage_and_field_mapping_refuse` | `tests/backfill.rs:721` | Existing forged payload tests now encounter origin association checks before value reconstruction; the `values` assertion has the static failure described in F3. |

Direct negative tests for forged nonempty origins—wrong declaration, mismatched original source interpretation, wrong retained file key/revision, and omitted/duplicated origins—were not found in this scope. Existing lifecycle tests exercise the positive mixed-generation path, and the native malformed-group fixture exercises caller association rejection. Add focused negative coverage where fixing F1/F3 exposes those paths; do not treat the positive fixtures as proof of every refusal branch.

## Conclusion

VERDICT: REQUEST CHANGES

The patch preserves original execution records and separates metadata verification from ordinary reads, but the warm semantic-association gap and retained-readback cache lifetime remain open. The deep-value regression also needs repair so it still tests value verification. The shared assembly descriptor cache is a concrete reduction in repeated reads, without a measured speed claim.

Coverage of changed paths: INSUFFICIENT for the warm semantic mismatch and intended deep-value regression. Confidence: HIGH in the static traces; runtime qualification remains with the implementation owner.

## Follow-up review — finding dispositions

The original findings and initial verdict above are retained as review history. This section records a static reread of the implementation owner's corrections on 2026-10-10. Original line references describe the initial patch; the references below describe the corrected source. No build or test execution was performed by this reviewer.

### F1 — Resolved in source

`CheckedGroup` now retains the material dependency digest (`src/engine/backfill/origins/checks.rs:19`). Every warm association must match that digest before returning success (`src/engine/backfill/origins/checks.rs:312`). The cold path still checks the captured source and every known file execution before retaining the digest, and the added string is included in metadata admission (`src/engine/backfill/origins/checks.rs:416`). This closes the cold/warm discrepancy without reopening already parsed metadata.

The updated `cached_originals_reuse_metadata_and_recheck_caller_association` fixture changes the consumed field's initial default while retaining the same logical source marker (`src/engine/backfill/origins/checks.rs:538`). It expects both the warmed checker and a fresh checker to refuse, then confirms the original association still reuses its cached facts after the original JSON files are removed (`src/engine/backfill/origins/checks.rs:550`, `:566`, `:583`). This directly covers F1; test execution remains pending in the implementation owner's evidence.

### F2 — Resolved for the scoped scalar recovery path

The request creates `Checks` before calling `retained_backfill_readback` (`src/engine/backfill.rs:619`). The helper receives that instance, switches to composition only after finding retained readbacks, and passes it through all retained candidates (`src/engine/backfill.rs:872`, `:885`, `:937`). An empty lookup therefore leaves routine preparation checks enabled. The same state reaches the outer composition loop and nonrelated cached availability through `check_release_available_with_checks` (`src/engine.rs:2441`); `verify_binding_dependencies` no longer creates another scalar checker (`src/engine/backfill/verification.rs:7`).

Together with assembly's shared `scalar_file` call (`src/engine/backfill/preparation.rs:46`), the corrected direct path reuses parsed descriptor facts across assembly, original-execution qualification, retained readback candidates, composition, and cached availability. This is a static reduction in metadata opens, not a measured speedup or memory result.

The related-dataset branch still enters the existing independent dependency graph (`src/engine.rs:2446`). Its walker creates a separate composition checker for each node when no explicit verification state is supplied (`src/engine/verification.rs:362`). This preserves the established graph/availability boundary and keeps graph state local. It can still reopen scalar origins when a related result crosses that boundary. No correctness gap was found there, and this review does not require a broader walker refactor for the original-identity increment. An end-to-end claim of reading each immutable scalar record only once across related dependency traversal would be unsupported; measure that path before making such a claim or expanding the refactor.

### F3 — Deep cases repaired; new association case needs its match arm

The payload-corruption, mapping, and binding cases now explicitly clear origins, preserving the preexisting legacy deep-verification paths (`tests/backfill.rs:820`). The `values` case retains its `invalid_group_values` assertion (`tests/backfill.rs:919`). A new `origins` case preserves the descriptor so the alternate output must fail association.

On the reread, however, `origins` entered the payload setup branch and reached `_ => unreachable!()` because the no-mutation arm covered only `values` (`tests/backfill.rs:844`). Add `origins` to that no-mutation arm so the new case reaches the verifier. This residual fixture issue was reported immediately to the implementation owner.

Follow-up verdict at this point: **REQUEST CHANGES**, limited to the remaining F3 fixture arm. F1 and the scoped F2 correction are closed by static evidence. Functional coverage for those corrections is present in source; runtime results remain outside this review.

## Final static disposition

F3 is resolved: the fixture now has `"values" | "origins" => {}` at `tests/backfill.rs:852`. The origins case reaches its alternate-output association check, while the values case still removes origins and reaches the deep reconstruction assertion. The final reread found no remaining material issue in this review's scope.

VERDICT: APPROVE

The reviewed corrections close F1, the scoped direct-recovery F2, and F3 without adding a second verifier or cache. The unchanged related-dependency graph boundary retains the efficiency limitation documented above; approval does not establish graph-wide read-once behavior, whole-process memory bounds, or performance qualification.

Coverage of changed paths: ADEQUATE for the identified corrections and the inspected lifecycle cases. The earlier note about unexercised nonempty-origin refusal branches remains a coverage limit. Confidence: HIGH in the static dispositions. This approval is a source-review result; compilation, targeted tests, and the repository gate require the implementation owner's retained execution evidence.

## Additional fixture correction — scalar binding value comparison

The implementation owner reported that the gate exposed the same F3 short-circuit in `tests/scalar_bindings.rs::opposite_boolean_outputs_cannot_hide_each_other_in_value_comparison`. A static reread confirms that `forge_values` now clears only the cloned forged binding's origins before rehashing its manifest (`tests/scalar_bindings.rs:112`, `:119`). The helper still preserves occurrence columns and substitutes the requested value columns (`tests/scalar_bindings.rs:103`). It does not change the valid release or production verification.

This restores the boolean regression's intended legacy deep-value path: the test still swaps opposite boolean outputs and specifically requires `invalid_group_values` (`tests/scalar_bindings.rs:148`, `:158`). The helper's other caller still changes the ancestor binding and requires `invalid_group` (`tests/scalar_bindings.rs:249`, `:261`); production binding-history verification still runs before the optional origins check (`src/engine/backfill/verification.rs:244`). Modern origin/output association rejection remains covered by the separate origins case in `tests/backfill.rs:817`.

Disposition: **APPROVE** this fixture correction; the final static review verdict remains **APPROVE**. No additional material finding. This reviewer did not execute or inspect the owner's gate logs, and does not treat reported partial gate progress as a completed gate.
