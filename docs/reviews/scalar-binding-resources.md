# Scalar field bindings: resource and reuse review

Review scope: uncommitted checkpoint against `59cc143`, covering source, tests and
documentation for retained scalar input bindings. Static review using the
semi-formal-code-review and ms-rust guidance. The reviewer ran no builds or tests
and changed only this report. Execution evidence below was produced by the owner.

## Findings and dispositions

No unresolved blocking finding in the reviewed implementation.

| Finding | Severity / category | Evidence and disposition |
| --- | --- | --- |
| Documentation can affect an immutable expression | WARNING / correctness, closed | `Expressions::compile` at `src/engine/backfill/expressions.rs:82` now detects `arrow_metadata` and retains root documentation in its material dependency description at line 139. Pinned DataFusion's `datafusion-functions-53.1.0/src/core/arrow_metadata.rs:125` reads argument-field metadata; Iceberg's `iceberg-0.10.1/src/arrow/schema.rs:527` includes the native field's documentation there. Clearing it unconditionally could reuse stale computed values, followed by repeated validation refusal. The expression fixture checks both changed dependency identity and changed actual output. |
| Coverage admission overlapped the new value comparison unnecessarily | WARNING / resources, closed | `src/engine/backfill/verification.rs:384` confines the source inventory, coverage map and group reader to a block ending before the relational comparison. Their permits no longer overlap the next occurrence inventory simply because of lexical lifetime. |
| Historical parsing repeated hashing and synchronous unbuffered reads | WARNING / performance and cancellation, closed | `History::read` at `src/engine/backfill/verification.rs:241` uses the existing `inputs::read_manifest` at `src/engine/inputs.rs:286`. The helper admits a fixed buffer, checks actual size, hashes and parses the same bytes, and transfers both buffer and shared metadata custody to the blocking worker. No second parsing implementation remains. |
| Cache-key equality alone did not prove retained native-file reuse | WARNING / test coverage, closed | `src/engine/backfill.rs:706` now runs actual preparation before and after a native metadata fork. It checks unchanged completion records and identical computed Parquet objects, a different assembled metadata object, and distinct fresh-name results. `scalar-binding-reuse.log` records success. |
| Remaining broad work | OBSERVATION / performance | `verify_group_data` at `src/engine/backfill/verification.rs:355` verifies coverage and reconstructs current scalar values. Retained file computation avoids evaluation and writing for those files; it does not avoid full current-value verification. Native schema opens during history checks remain outside the manifest-byte accounting, as described below. These are qualification limits, not a demonstrated throughput improvement. |

## Patch summary

`ComputedGroup.input_fields` (`src/model.rs:504`) records each SQL input name's
native root field ID. Retained SQL therefore keeps its original meaning across a
rename, reorder or later reuse of the old name. Full source reconstruction has an
explicit correspondence rule: previous ID to previous current name to new ID.
Ordinary updates retain IDs.

Per-file reuse now depends on the exact immutable source object and consumed field
interpretations. Whole-table preparation still binds exact source metadata and
the input mapping. This separates computation reuse from current composition
without a second lifecycle store. Dataset format and processor advance at
`src/engine.rs:81`; the scalar processor advances at `src/engine/backfill.rs:16`.
Old formats are not implicitly upgraded.

Independent verification adds scalar value reconstruction and validates binding
provenance through actual owning selections. The request-local `History` cache
shares parsed manifests across groups. It is not a durable index or cache service.

## Function trace

| Function / location | Inputs → outputs | Verified callers, callees and behavior |
| --- | --- | --- |
| `Expressions::compile`, `src/engine/backfill/expressions.rs:29` | Native schema, definition, optional input map → physical/logical expressions, native output schema, consumed map and dependency digest | Called by backfill planning, shared preparation and independent verification. Parses against original aliases, resolves actual columns through IDs, refuses missing/extra bindings, and preserves consumed type/default/nested interpretation. `root_index` at line 221 performs native-ID lookup. |
| `Expressions::evaluate`, `src/engine/backfill/expressions.rs:196` | Source batch → locator-bearing computed batch | Existing per-file worker evaluates maintained physical expressions; cumulative output array bytes remain checked. Retained logical expressions are used separately by verification. |
| `inherited_inputs`, `src/engine/backfill.rs:572` | Actual operation, prior binding and before/after source pins → current alias-to-ID map | Called from coordinated preparation at `src/engine.rs:795` and history verification. Build/full refresh resolves correspondence by prior current names; update/incremental retains IDs. Missing correspondence refuses. |
| `file_key`, `src/engine/backfill.rs:613` | Source `ObjectRef`, compiled dependencies, eligibility → lifecycle phase key | Backfill planning and preparation use the same helper. Source path/owner/digest/size remain material; root presentation names and irrelevant fields do not enter the interpretation hash. |
| `prepare_group`, `src/engine/backfill/preparation.rs:6` | Request, operation, exact source, definition, optional map → `PreparedGroup` | Used by standalone backfill and source refresh. Checks completion keys before `compute_file`, validates retained source/count, loads retained native descriptors, uses existing bounded assembly, then validates the assembled table. Its prepared/validation digest at line 24 includes exact source metadata and input bindings. |
| `verify_groups`, `src/engine/backfill/verification.rs:21` | Dataset pin → accepted composition or refusal | Checks actual Build/Update/Backfill/Refresh composition, then invokes history and current-value verification per group. No recursive historical payload reconstruction. |
| `History::{new,read,base}`, `src/engine/backfill/verification.rs:233` | Exact manifest reference → shared parsed dataset | One admission reservation and cumulative serialized-byte allowance per invocation. Exact repeated references return the cached `Arc`; conflicting references to a path refuse. `base` also validates dataset/version. |
| `verify_binding_history`, `src/engine/backfill/verification.rs:272` | Owning dataset, group name, shared history → provenance acceptance | Iteratively follows actual owners, detects cycles, verifies source/binding identity, preserves sibling tables and bindings, checks operation-appropriate correspondence, and ends at the authored backfill's independently compiled input map. Cooperatively yields between hops. |
| `verify_binding_dependencies`, `src/engine/backfill/verification.rs:7` | Cached result → metadata availability/provenance check | Called in both ordinary cached readback (`src/engine.rs:1499`) and related dependency traversal (`src/engine/verification.rs:145`). Empty computed sets return before reserving history memory. |
| `verify_group_data`, `src/engine/backfill/verification.rs:355` | Source/group pins, definition, input/output maps → complete coverage and values or refusal | Checks native schema/mapping and exact occurrence coverage. Then uses `Budget::context` (`src/budget.rs:98`) and shared native providers (`src/query.rs:18`) for a separate expected-versus-actual join. Parenthesized `IS DISTINCT FROM` predicates include successful nulls. Only a mismatch marker is collected. |

## Data flow and invariants

1. **Identity precedes name resolution.** The authored compile records only consumed
   aliases. Bound compilation resolves those aliases by root ID before planning
   actual column access (`expressions.rs:41`, `:107`). The exact consumed-map check
   at line 130 prevents an incomplete or silently expanded dependency claim.
   Reusing an old display name under another ID cannot redirect retained SQL.

2. **Reuse includes material interpretation.** The per-file digest includes the
   definition, scalar processor, alias map and consumed native fields
   (`expressions.rs:133`). It retains type, requiredness, defaults and nested
   structure; root documentation remains material when observed. Constant
   expressions have no source-field dependencies. Whole prepared results include
   the exact source metadata and binding, so two meanings of the same SQL cannot
   collide at assembly or validation (`preparation.rs:24`).

3. **Origin claims are checked against owners.** History follows parent selections,
   not a binding's self-declared operation kind (`verification.rs:272`). A sibling
   backfill must inherit the same source, binding and value table; source operations
   must match the owner's request and parent. Update/incremental hops additionally
   require the same native schema. At the authored backfill, compiling without a
   supplied map must reproduce the retained map. Missing historical manifests or
   origin schemas refuse cached readback as well as full verification.

4. **Manifest custody survives cancellation.** `History` retains parsed entries and
   its shared permit through verification. The common reader moves cloned custody
   and its separate buffer permit into `Budget::blocking` (`inputs.rs:299`), whose
   worker also holds the existing I/O/job allowance (`budget.rs:114`). Dropping the
   async caller does not admit replacement work while that worker still owns its
   resources. Cumulative bytes cap distinct loaded manifests; repeated exact loads
   do not consume the allowance again.

5. **Coverage precedes value comparison.** Exact one-to-one source occurrence
   coverage makes the subsequent inner join sufficient to compare values. Duplicate,
   missing, foreign, null or out-of-range locators refuse before that join. The
   coverage structures drop before current-value reconstruction
   (`verification.rs:384`). Both production and independent contexts share native
   execution memory/spill accounting; no unrestricted default context was added.

The manifest allowance is not a whole-process memory or total history-I/O bound.
`inherited_inputs` and history schema checks call `storage::open_pin` at
`src/storage.rs:532`, which hashes and loads native metadata separately. These
schema opens can repeat across groups and history steps; their bytes are not charged
to `History.bytes`. Parsed representation overhead and upstream allocations also
remain outside exact RSS accounting. The existing native all-fields-absent decode
and DataFusion spill-overrun limitations remain relevant. No performance result is
inferred from bounded admission or exact file reuse.

## Test behavior and evidence

The reviewer read these assertion paths; the linked logs are owner execution evidence.

| Fixture | Assertions and disposition |
| --- | --- |
| `rename_reorder_and_name_reuse_preserve_bound_values_and_material_identity`, `src/engine/backfill/expressions/tests.rs:46` | Bound quoted aliases retain ID and value after reorder/rename; a fresh compile uses the new ID; missing/extra bindings refuse. Passed in `scalar-binding-expressions.log`. |
| `constants_ignore_schema_but_consumed_types_and_metadata_observers_invalidate`, `src/engine/backfill/expressions/tests.rs:108` | Constant dependency identity survives unrelated schema changes; consumed type changes invalidate; documentation affects `arrow_metadata` identity and actual output. Passed in the same log. |
| `native_rename_reuses_file_completions_but_name_reuse_changes_input_meaning`, `src/engine/backfill.rs:706` | Actual native descriptor reuse preserves completion records and Parquet objects while assembling new metadata. Fresh name resolution creates different objects and a null value. Passed in `scalar-binding-reuse.log`. |
| `reordered_full_rebuild_rebinds_computed_inputs_by_prior_current_names`, `tests/rebuild.rs:45` | Reordered full rebuild changes native ID assignments while preserving both scalar values against a Rust oracle; forged old mapping refuses. Passed in `scalar-binding-rebuild.log`. |
| `native_malformed_group_coverage_and_field_mapping_refuse`, `tests/backfill.rs:630` | Existing native coverage corruption cases plus wrong binding and wrong scalar values refuse, despite otherwise valid files. Passed in `scalar-binding-backfill.log`. |
| `tests/scalar_bindings.rs:121`, `:177`, `:268` | Opposite Boolean mismatches cannot mask one another; rehashed ancestor redirection and forged operation kinds refuse; deleting an older binding manifest or origin schema blocks cached selection, and restoring it permits resume. Passed in the complete gate. |
| `history_reuses_exact_manifests_and_bounds_combined_admission`, `src/engine/backfill/verification.rs:541` | Exact repeated reference shares the same `Arc` after file removal; conflicting reference refuses; cumulative overflow refuses before opening the next missing file; dropping retained values/cache releases admission. Passed in `scalar-binding-final-check.log`. |

The complete owner `./scripts/check` run passed in
[scalar-binding-check.log](scalar-binding-check.log). The direct cache test was
added afterward; the follow-up formatting, all-target/all-feature Clippy and
all-feature library test run also passed and is recorded separately in
[scalar-binding-final-check.log](scalar-binding-final-check.log). Earlier compilation
and Clippy failures remain retained with the checkpoint logs. No performance
benchmark, whole-process memory measurement or public schema-operation qualification
was executed by this review.

## Conclusion

VERDICT: APPROVE

The patch makes retained scalar computation depend on consumed native field
identity, proves actual file reuse across presentation changes, and adds independent
value/provenance checks through existing execution and lifecycle facilities.
The implementation adds a bounded local manifest cache rather than another durable
subsystem. The remaining full reads and repeated native-schema opens are explicit
costs requiring measurement before broader performance claims.

Coverage of changed paths: ADEQUATE. Confidence: HIGH for the scoped cache,
binding, custody and verifier behavior; performance and whole-process memory remain
unqualified. Public schema mutation, schema cutover and complete R3 delivery remain
outside this checkpoint.

The updated `README.md:96` and `docs/STATUS.md:734` distinguish this implemented
binding/reuse prerequisite from those unfinished lifecycle features and state the
native-metadata and full-verification limitations above.
