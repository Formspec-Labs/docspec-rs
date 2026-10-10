# Native column selection: implementation review

**Static decision: approve the semantic implementation checkpoint.** No
unresolved production correctness defect was found after the corrections below.
The complete design's metrics and measurement work remains outstanding. The
final repository gate passed separately, as recorded in the retained execution log.

Final static recheck on 2026-10-10 against
`05ea3a1996c56a470f9777e6db35a22ebdff8572` and the frozen working files.
The reviewer ran no build, test, formatter, benchmark, or corpus scan and changed
no production or test code. The MS Rust and semi-formal code-review workflows
informed the review. Unrelated change-chain and qualification-root implementation
was excluded; the main task's shared full gate includes that concurrent work.

## Findings and disposition

| Severity | Finding | Disposition |
| --- | --- | --- |
| WARNING, execution qualification | The [second repository gate](native-column-selection-full-check-2.log) aborted with stack overflow in `incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile`. | **RESOLVED.** The retained [debugger trace](native-column-selection-stack-backtrace.log) reproduced failure in SQL parsing. A [single-boundary intervention](../../src/storage/schema.rs#L101) boxes the maintained file-planning future. Static inspection confirms unchanged scan configuration, borrow lifetime, task stream and ID mapping. The [same normal-stack fixture](native-column-selection-stack-boxed.log) and the [final full gate](native-column-selection-full-check-3.log) pass. Numerical stack reduction has not been measured. |
| WARNING, resource correctness | The initial query-plan reservation covered the index vector but omitted the projected Arrow schema and copied metadata. Pinned Arrow `Schema::project` allocates field handles and clones schema metadata; large repeated projections could exceed the declared metadata allowance. | **RESOLVED.** Both [query callers](../../src/query.rs#L101) and [storage selection](../../src/storage/selection.rs#L54) now use shared [reservation accounting](../../src/storage/selection.rs#L154). The [revised fixture](../../src/query/selection_tests.rs#L279) checks repeated handles and metadata-heavy empty projection rather than copying the reservation formula. |
| OBSERVATION, measurement | [Ordinary](../../src/storage.rs#L124) and [per-file](../../src/storage/occurrences.rs#L237) readers consume `ScanResult::stream()` without retaining its metrics handle. The proposed requested-scan-byte hook and frozen comparative experiment are absent. | Requested-byte, elapsed-time, and whole-process memory improvement remain unqualified. The design's complete measurement slice is not complete. Capture the maintained metrics before consuming the stream and run the controlled comparison before making those claims. |
| WARNING, test coverage | The initial damaged-page fixture invoked `TableProvider::scan` directly and did not establish SQL optimizer pushdown. | **RESOLVED.** [Actual SQL queries](../../src/storage/occurrences/tests/selection.rs#L332) now read the key successfully and fail on the damaged payload across ordinary, located and captured providers. The retained targeted rerun passes. |
| WARNING, test coverage | The design proposes a broader selected task-refusal and composition matrix than these focused fixtures execute. | Existing common [task checks](../../src/storage/occurrences.rs#L406) remain unchanged and execute before selected/full decoding; [zero-column count refusal](../../src/storage/occurrences/tests/selection.rs#L105) is directly covered. The earlier nested-root gap is resolved by the explicit [Struct fixture](../../src/storage/occurrences/tests/nested_selection.rs#L8) and its [passing check](native-column-selection-nested-check-4.log). Neither result establishes every proposed combination, all nested types, or nested-leaf pruning. |

## Patch summary

The patch passes DataFusion's provider-relative column indices into the existing
native readers. A shared storage selection maps current-schema root positions
to unique Iceberg field IDs, reads necessary origin pairs, and builds the exact
requested Arrow output with repetitions and zero-column row counts preserved.
Ordinary values retain their existing read path and raw origin values. Located
and captured reads retain their complete-file occurrence checks.

The change removes the adapter-created `ProjectionExec` for index-only source
selection, replaces the captured reader's separate field-ID translation with
the shared selection, and reuses maintained Iceberg decoding and Arrow arrays.
These are verified structural changes, not measured throughput improvements.

Frozen reviewed production blobs, recorded with `git hash-object`:

| File | Git blob identity |
| --- | --- |
| `src/query.rs` | `7573db6f741c668da0749c926918471c31bcef52` |
| `src/storage.rs` | `da5d4c061953968d07cceb7b2f01cb0a544cdd61` |
| `src/storage/schema.rs` | `ce4e18f4fba9dc77fb550dd0b9b0a8834cc83dcc` |
| `src/storage/selection.rs` | `b54daf70a56125da2f931230d16d23e19bdffba1` |
| `src/storage/occurrences.rs` | `07bded939b3f8dd76a7bc066bc26e9f85b74416d` |

## Exploration record

| Hypothesis | Evidence examined | Result |
| --- | --- | --- |
| H1: SQL indices must compose with a captured provider's existing selection. | `NativeTable::scan`, `NativePartition::project`, `projected_files`, and the captured composition fixture. | Confirmed: Arrow validates against the advertised schema before composition with the immutable source mapping. |
| H2: Native deduplication must remain separate from output order. | `Selection::new`, `Selection::apply`, pinned Iceberg transformer and Arrow constructors. | Confirmed: dependencies use current root order; the result map preserves requested order and duplicate array references. |
| H3: Stored origins need a paired dependency even when one member is returned. | `Selection::new`, `Occurrences::next`, `identity::materialize`, `location_columns`. | Confirmed: selected stored or synthetic origin members add both native fields before existing paired validation and fallback. |
| H4: Empty selection must still consume physical rows for occurrence coverage. | `plan_selected_files`, `strip_file_column`, `Occurrences::next`, pinned Iceberg projection mask, zero-column fixture. | Confirmed: the transient field remains, offsets advance by decoded batch length, and EOF checks remain. Empty or all-absent native selections can still decode all physical columns. |
| H5: Newly retained query metadata must participate in admission. | Initial and revised `reserve_projection`, pinned Arrow `Schema::project`, ownership fixture. | Initial hypothesis refuted; final shared helper reserves schema and mapping before allocation. Static recheck confirms both query and storage callers use it. |

## Function trace

These functions and important direct callers/callees were read, rather than
inferred from names. References target the final production implementation.

| Function | Inputs and outputs | Verified behavior |
| --- | --- | --- |
| [`provider`, `table`, `occurrences`](../../src/query.rs#L24) | Immutable native table, budget, row hint → provider | Advertises native or located schema; retains an unselected immutable source. |
| [`file`, `files`, `projected_files`](../../src/query.rs#L68) | Captured scan, optional path/names → provider | Resolves initial names to captured-schema indices; preserves exact captured row hints. |
| [`NativeTable::scan`](../../src/query.rs#L157) | Provider-relative indices, limit → plan | Applies source selection; leaves expressions/filters to DataFusion and optional limit above the source. |
| [`reserve_projection`](../../src/storage/selection.rs#L154) | Schema, requested width, map size, budget → permit | Reserves mapping, schema handles and copied metadata before `Schema::project`. |
| [`NativePartition::project`](../../src/query.rs#L204) | Advertised indices → new partition | Checks indices via Arrow; composes existing mapping; retains row hint and selected schema. |
| [`NativePartition::execute`](../../src/query.rs#L317) | Immutable source → lazy stream | Dispatches ordinary/located/captured reads; captured multi-file reads open serially and retain the source owner. |
| [`partition_statistics`](../../src/query.rs#L280) | Selected schema and row hint → statistics | Selected column statistics remain unknown; row hint remains `Inexact`. |
| [`read_batches`, `read_projected_batches`](../../src/storage.rs#L88) | Table and optional source indices → values stream | Full-schema default admission, shared selection, serial native reader, transient-column check and stream-owned permits. |
| [`plan_files`, `plan_selected_files`](../../src/storage/schema.rs#L78) | Current schema/IDs → tasks | Keeps exact native table selection; overwrites task schema with retained current schema and field IDs. Boxes the maintained planning future at the common seam, preserving its inputs and output stream. |
| [`Selection::new`](../../src/storage/selection.rs#L30) | Qualified schema, mode, requested indices → dependency/output map | Validates indices, deduplicates IDs in root order, includes necessary origin pair and appends the reserved file ID once. |
| [`Selection::apply`](../../src/storage/selection.rs#L110) | Decoded batch and optional physical location → selected batch | Shares native and locator arrays; constructs with explicit row count; checks logical output size including repeated columns. |
| [`read_occurrences`, `read_projected_occurrences`](../../src/storage/occurrences.rs#L58) | Table and optional indices → located stream | Captures native inventory and delegates to the same checked occurrence state. |
| [`Occurrences::open`, `capture`, `capture_projected`](../../src/storage/occurrences.rs#L99) | Table/inventory, located flag, optional indices → state | Preserves default/schema qualification, inventory permit, reader batch reservation, and disabled row filtering/selection. |
| [`Occurrences::next`](../../src/storage/occurrences.rs#L174) | Checked tasks and decoded batches → next values/located batch | Removes each expected task once, materializes origins, advances physical offset, checks exact EOF counts and missing tasks. |
| [`OccurrenceScan::open`, `values`, `capture`](../../src/storage/occurrences.rs#L271) | Qualified occurrence state → reusable captured tasks | Checks complete task membership and transfers shared inventory ownership. |
| [`OccurrenceScan::read_file`, `read_projected_file`](../../src/storage/occurrences.rs#L354) | Captured path and optional indices → checked stream | Clones only the requested task, changes that clone's IDs, and retains the original scan for subsequent reads. |
| [`check_task`](../../src/storage/occurrences.rs#L406) | Task and captured file → acceptance/refusal | Requires full Parquet file, exact size/count, no predicate and no deletes. |
| [`append_locations`, `location_columns`](../../src/storage/occurrences.rs#L424) | Materialized origins or physical path/offset → locator arrays | Reuses stored origins; allocates only requested synthetic arrays; checks physical-locator allocation before building. |
| [`identity::materialize`](../../src/storage/identity.rs#L129) | Native origin pair and physical fallback → validated pair | Existing checks reject mismatched nulls, empty origin and negative positions; null pairs use physical identity. |
| [`strip_file_column`](../../src/storage/schema.rs#L114) | Native batch → value columns | Checks size before removing the final reserved-ID run-end-encoded field; a user field named `_file` is not stripped by name. |

Important consumers include [join input registration](../../src/engine/refresh.rs#L503),
[captured-origin membership](../../src/engine/evidence.rs#L816),
[replacement-key reads](../../src/engine/write.rs#L451), and
[selected exports](../../src/engine/export.rs#L464). These remain on their existing
providers/readers.

## Data flow and invariants

The query mapping is created before plan construction, composed against the
advertised schema, and owned by `NativeSource` through plan/stream lifetime.
Storage creates a separate admitted `Selection`. Its ordered `columns` map
escapes into the stream; temporary dependency flags and root positions do not.
Task field IDs are cloned from unique dependencies and always end in the reserved
file ID. Captured source tasks remain immutable.

Physical offsets originate at zero for each checked file, advance over every
decoded row, and are consumed before DataFusion filtering or limiting. Stored
origins override those offsets only after existing pair validation. Successful
EOF establishes full-file coverage; a consumer that drops early establishes only
the portion read.

Default interpretation stays in the pinned native transformer. Full-schema
`defaults::check_expansion` remains conservative for narrow reads. Current-schema
IDs preserve names/order independently of physical files. Repeated output columns
share array handles but each contributes to the existing logical output byte
check. Reader permits do not establish a bound on Parquet row-group page buffers
or whole-process resident memory.

## Pinned dependency evidence

The inspected lockfile pins DataFusion 53.1.0, Iceberg 0.10.1 and Arrow/Parquet
58.4.0. The local Cargo registry source was inspected at those exact versions:

- DataFusion `catalog/src/table.rs:93` defines projection against provider schema,
  in the requested order, and leaves filters unsupported by default.
- Arrow `arrow-schema/src/schema.rs:251` shares field objects but allocates their
  handle sequence and copies schema metadata.
- Arrow `arrow-array/src/record_batch.rs:455` preserves row count when projecting
  no columns; `try_new_with_options` accepts the explicit row count used here.
- Iceberg `arrow/reader/pipeline.rs:214` excludes virtual metadata IDs from the
  physical mask and passes the full requested ID list into its transformer.
- Iceberg `arrow/reader/projection.rs:118` and `:211` use all physical columns for
  empty and wholly absent field selections. This is a disclosed fallback, not
  evidence of zero-column I/O savings.
- Iceberg `arrow/reader/projection.rs:65` recursively includes a selected root's
  leaves. `arrow/schema.rs:527` preserves child field IDs and `doc` metadata.
- Iceberg `arrow/record_batch_transformer.rs:347` orders target fields by requested
  IDs and applies defaults and native identity mapping.
- Iceberg `arrow/scan_metrics.rs:44` increments requested range lengths before
  I/O, and `:88` consumes the result into its stream. Any later hook must capture
  `metrics().clone()` first and label the value requested scan bytes.

## Test behavior and edge cases

The reviewer read test bodies and retained logs but did not execute tests. The
[targeted rerun](native-column-selection-targeted-2.log) passes the new selection
fixtures. The [initial failures](native-column-selection-targeted-1.log) remain
retained: default scratch exceeded free space; the small DataFusion pool could
not reserve an unnecessary SQL sort; the DocSpec writer normalized null origins
before the fallback assertion. Final fixtures use zero scratch for the tiny
query source, compare sorted results in the test, and write raw Parquet origins
to exercise reader fallback. Those changes preserve production admission.

| Test / fixture | Trace and asserted behavior |
| --- | --- |
| [`direct_table_scan_preserves_repeats_empty_rows_and_statistics`](../../src/query/selection_tests.rs#L93) | Direct scan returns independently specified repeated/reordered arrays, retains nonzero row count with no columns, refuses an invalid index, honors a limit and keeps selected-schema statistics. Targeted rerun passes. |
| [`captured_provider_composes_indices_and_keeps_sql_behavior`](../../src/query/selection_tests.rs#L142) | Initial `[c,a]` composes with `[1,0,1]`; exact values/nulls and empty-output rows survive; SQL aliases, filters, constants and count match expected values. Targeted rerun passes. |
| [`captured_occurrence_selection_composes_synthetic_row_indices`](../../src/query/selection_tests.rs#L215) | Synthetic source-row index composes with native columns and repetitions. Targeted rerun passes. |
| [`projected_mapping_admission_is_owned_by_plan_and_stream`](../../src/query/selection_tests.rs#L237) and [`empty_projection_admits_cloned_schema_metadata_before_allocation`](../../src/query/selection_tests.rs#L279) | Dropping the plan retains active stream admission; stream drop releases it; exhausted capacity and metadata allowances refuse; empty selection charges metadata. Targeted rerun passes. |
| [`selected_locations_keep_original_offsets_across_files_and_empty_columns`](../../src/storage/occurrences/tests/selection.rs#L18) | File-only, row-only, mixed repeated and empty selection retain independent physical offsets across fixture files/batches; SQL filtering keeps original offsets; wrong zero-column decoded count refuses. Targeted rerun passes. |
| [`selected_origins_keep_pairs_fallback_and_validation`](../../src/storage/occurrences/tests/selection.rs#L151) | Raw stored origin and null pair produce `[42,1]`; repeated origin-row selection still includes its partner; synthetic locators preserve birth identity/fallback; malformed pairs refuse. Targeted rerun passes. |
| [`sql_native_and_captured_reads_skip_a_damaged_unselected_page`](../../src/storage/occurrences/tests/selection.rs#L281) | SQL returns exact keys despite damaged unused payload and fails when payload is selected across ordinary, located and captured providers. Targeted rerun passes. |
| [`current_schema_batches`](../../src/storage/occurrences/tests/schema.rs#L6), used by independent rename/reorder, nullable addition, same-name/new-ID and user `_file` cases | Compares selected output to full-read projection for empty, single, reversed and repeated requests. Independent full-value assertions remain. Added checks should preserve those exact values; repository-gate execution is separate evidence. |
| [`selected_defaults`](../../src/storage/occurrences/tests/defaults.rs#L135), called by [`native_v3_defaults_and_presence_survive_upgrade_append_rewrite_and_reopen`](../../src/storage/occurrences/tests/defaults.rs#L165) | Repeated default value plus supplied marker match independently specified rows for absent-only, mixed append and rewritten/reopened states. Initial/write defaults and explicit null/zero remain distinct. |
| [`selected_nested_roots_preserve_children_repeats_and_occurrence_rows`](../../src/storage/occurrences/tests/nested_selection.rs#L8) | Independently specifies sparse parent/child IDs and child documentation; distinguishes parent null, child null and empty text; checks repeated Struct roots, scalar reorder, empty source and captured occurrence-row offsets across batches. [Focused check passes](native-column-selection-nested-check-4.log). This does not qualify nested-leaf pruning or all nested types. |
| [`value_only_key_probes_skip_wide_fields_and_keep_reader_admission`](../../src/engine/write/tests.rs#L546) | Leaves one KiB for new selection/schema maps under the unchanged process ceiling. The same second reader still must refuse; dropping the first releases its batch allowance. |
| Existing [reader lifetime](../../src/query.rs#L520), [oversized values and tiny-budget count](../../src/query.rs#L621), [filtered counts](../../src/query.rs#L648), and [join statistics](../../src/query.rs#L693) | Keep batch/drop/EOF admission, tiny-budget `COUNT(*)` refusal, filtered `LIMIT/OFFSET` counts and smaller-build hints. Positive query fixtures lower their batch cap under the unchanged memory ceiling to admit the new mapping state. |

The full-read wrappers still cover reserved metadata-field checks, unqualified
schema refusals and malformed task inventories. Selected reading intentionally
does not prove the integrity of unused values: the damaged-page fixture is the
concrete distinction. Successful EOF proves full occurrence coverage; dropping a
stream early does not.

The nested fixture retains its failed setup attempts:
[new-table ID reassignment](native-column-selection-nested-check.log),
[invalid metadata filename](native-column-selection-nested-check-2.log), and
[unsupported nested evolution](native-column-selection-nested-check-3.log).
Its final setup asserts no snapshot exists and removes only the unreferenced
original schema from fresh fixture metadata before registering the explicit sparse
schema. Production evolution qualification is unchanged, and the test makes no
claim that adding nested roots over old files is supported.

Remaining coverage is narrower than the design's complete matrix. The existing
malformed/repeated/deleted/predicated task fixtures still use the full path; the
new selected fixture adds a short-decoded-count refusal with zero columns.
Captured composition is directly checked on a reordered provider, while repeated
initial captured schemas and every multi-file composition variant are not isolated
as separate fixtures. The shared checked paths and selected-file tests support
the implementation, but should not be described as exhaustive qualification.

The first [repository gate](native-column-selection-full-check-1.log) stopped on
strict Clippy's needless borrow at `query.rs:344`. The implementer removed that
borrow without changing behavior. Its [rerun](native-column-selection-full-check-2.log)
then aborted on the incremental-refresh disk-refusal fixture's stack overflow.
The [bounded diagnosis](native-column-selection-stack-probe.md) retains the
failure and keeps the normal stack/resource profile. The
[exact failing fixture passes after common planner boxing](native-column-selection-stack-boxed.log).
The [final full gate](native-column-selection-full-check-3.log) completed with
exit 0. Inspection of that retained log and [gate script](../../scripts/check)
confirms formatting, strict Clippy, all-feature unit and integration tests, and
the doc-test check passed. The log includes the former stack-overflow fixture,
the nested Struct fixture, and the concurrent qualification-root tests. These
are the implementer's execution results, inspected by this static reviewer.

## Conclusion

VERDICT: APPROVE for the semantic implementation checkpoint.

The patch connects the intended existing seams, preserves the traced field,
default, occurrence and ownership rules, and resolves the material metadata
admission finding. Targeted execution supports actual SQL column skipping and
the explicit nested Struct case. The normal-stack regression fixture now passes
after the shared planner-future correction, without changing query semantics,
resource limits, or concurrency.

Coverage of changed core paths: ADEQUATE for this checkpoint. The complete proposed
matrix is not exhausted, as described above. Confidence: HIGH for the traced
scalar, Struct and occurrence paths; MEDIUM for broader design acceptance.

The full repository gate passed. Requested scan bytes, timing and whole-process memory improvement remain
unmeasured. The design's metrics hook and controlled experiment are outstanding.
