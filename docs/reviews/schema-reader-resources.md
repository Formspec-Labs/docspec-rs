# Native schema reader resource review

No actionable implementation finding remains in the inspected reader checkpoint. Native task normalization, transient-column validation and admission are shared by ordinary and occurrence reads. The unnecessary quadratic schema check is removed. The complete regression gate passes, followed by final-source formatting, Clippy and library tests after the last guard and test additions. **VERDICT: APPROVE** for this reader prerequisite.

This is a static semi-formal review of the uncommitted changes against `90dfd5b`. The reviewer applied the semi-formal code-review skill and Rust performance, universal, library and resilience guidance, inspected production/test code and pinned dependency implementations, and did not run tests or change implementation. Scope is the native reader prerequisite for schema evolution. Public schema mutation, replacement writers after evolution, computed dependency reuse and complete lifecycle/performance qualification remain separate work.

## Findings

### SR-R1 — Native equal-type reorder silently relabeled values — CLOSED

- **Severity/category:** BLOCKER / correctness.
- **Evidence:** In Iceberg 0.10.1, `src/arrow/record_batch_transformer.rs:429–463` compares names, types and nullability without comparing field IDs. Equal-type reorder selects the name-only shortcut, which keeps positional arrays (`:314–323`). The owner-produced `schema-native-reorder-failed.log` records the concrete result: the first reordered `b` value is 11 instead of 22.
- **Resolution:** `src/storage/schema.rs::plan_files` sets each native task's schema and requested IDs from the exact table metadata's current schema, then appends maintained `RESERVED_FIELD_ID_FILE`. The virtual column has `RunEndEncoded<Int32, Utf8>` type. Parquet 58.4.0 refuses writing that type (`src/arrow/schema/mod.rs:857`), so native-generated physical data cannot match the augmented target schema. Iceberg takes its general ID-aware transformation instead. `strip_file_column` validates the trailing column's ID and type before removing it. There is no vendored transformer or duplicate value/default implementation.
- **Qualification:** The equal-type reorder oracle, same-name/new-ID test and all-requested-fields-absent equal-count branch exercise the original failure and the workaround's nontrivial boundary. The latter would defeat an argument based only on differing field counts.

### SR-R2 — Schema admission repeated linear root searches — CLOSED

- **Severity/category:** WARNING / performance.
- **Location:** `src/storage/schema.rs::read_schema`.
- **Scenario:** The initial helper scanned all old top-level fields for every current field, including the current schema itself. That added quadratic column-count work to ordinary readers as well as evolved tables.
- **Resolution:** The current schema is skipped by shared-Arc identity. Each historical schema builds one root-ID set and uses indexed field lookup plus set membership. This removes the repeated linear search without introducing a cache or another schema authority. No elapsed-time improvement is claimed.

### SR-R3 — Temporary-column accounting and refusal lacked direct coverage — CLOSED

- **Severity/category:** WARNING / test coverage.
- **Location:** `src/storage/occurrences/tests/schema.rs::transient_native_column_is_validated_and_charged_before_removal`.
- **Resolution:** The new test obtains a real augmented native batch. It accepts its measured byte allowance, rejects the smaller stripped-only allowance, refuses an ordinary user batch lacking the sentinel, and refuses a forged sentinel ID attached to the wrong type. This establishes both the resource boundary and protection against silently dropping a user column. It passes in `schema-reader-final-unit.log`.

### SR-R4 — Existing combined-output fixture measured stripped inputs — CLOSED

- **Severity/category:** WARNING / qualification fixture.
- **Location:** `tests/backfill.rs::joined_output_respects_a_limit_that_each_individual_input_fits`.
- **Evidence:** `schema-reader-check-failed.log` records refusal during independent native input verification, before the expected combined-result refusal. Its measured allowance omitted the newly admitted temporary column.
- **Resolution:** The fixture now measures the maintained reader's actual `_file`-augmented input batches and includes the source occurrence allowance. It still requires an `oversized_batch` error identifying the final combined result. Production admission was not weakened to make the fixture pass. It passes in the complete `schema-reader-check.log` gate.

### SR-R5 — All requested fields absent still decodes physical columns — ACCEPTED LIMITATION

- **Severity/category:** OBSERVATION / performance and resource qualification.
- **Evidence:** Iceberg 0.10.1 `src/arrow/reader/projection.rs:211–215` selects all physical columns when no requested field exists. The subsequent transformation can return only small null/default arrays and the file sentinel.
- **Disposition:** Do not claim pruning or an exact native-allocation/RSS bound for this case. Large hidden physical fields may still consume I/O and temporary decoder memory before the transformed batch reaches application admission. The implementation retains the existing admission boundary and does not add a parallel decoder, synthetic row generator or durable index to conceal this maintained-reader limitation.

## Patch summary and function trace

The patch replaces default snapshot-schema interpretation with exact pinned current-schema interpretation for a qualified set of top-level changes. Existing files, values and physical occurrence positions remain intact. The public API does not yet expose a schema operation.

| Function | Location | Inputs → outputs | Inspected behavior |
| --- | --- | --- | --- |
| `Budget::create` | `src/budget.rs:66` | Limits → admitted execution budget | Rejects `batch_rows > i32::MAX`, matching the native sentinel's signed run-end representation. |
| `read_schema` | `src/storage/schema.rs:17` | Native table → current Arrow schema or refusal | Rejects top-level reserved IDs, the file sentinel's ID at any nesting level, changed types/requiredness/initial defaults, nested moves and unqualified additions; allows top-level rename/reorder/removal and optional primitive additions. |
| `plan_files` | `src/storage/schema.rs:74` | Native table → lazy native task stream | Validates schema, uses one native planning lane, shares the current schema Arc, and replaces each task's requested field IDs with current order plus the transient file field. |
| `strip_file_column` | `src/storage/schema.rs:102` | Augmented batch + byte limit → visible batch | Counts augmented array bytes before stripping; checks trailing native ID/type; uses Arrow's batch projection, preserving row count and sharing value arrays. |
| `read_batches` | `src/storage.rs:74` | Native table + Budget → admitted batch stream | Holds the existing two-batch reservation through stream completion/drop/error; configures native file concurrency and row batch size; strips each validated sentinel. |
| `occurrence_schema` | `src/storage/occurrences.rs:29` | Native table → schema with physical locators | Uses the same schema interpretation, then rejects application locator-name collisions and adds locator fields. |
| `Occurrences::open` / `next` | `src/storage/occurrences.rs:87`, `:117` | Table → serial whole-file occurrence batches | Retains inventory admission, checks tasks/counts, strips native sentinel before adding the independent physical file/ordinal columns. Ordinals still advance before relational filtering. |
| `OccurrenceScan::open` / `read_file` | `src/storage/occurrences.rs:196`, `:244` | Table/tasks → selected-file streams | Captures normalized native tasks once. Its task-byte accounting includes the extra projected ID. Stream-owned batch admission and shared inventory admission survive dropping the scan handle. |
| `NativePartition::execute` | `src/query.rs:59` | Native table → DataFusion stream | Existing lazy provider consumes these readers; its declared current schema now agrees with actual successful read batches. |
| Independent `read_pin` | `src/engine/read.rs:66` | Verified table pin → streaming reader | Declares the same current schema and forwards typed native/admission errors. |
| Lifecycle `rows` | `src/catalog.rs:140`, reader call `:164` | Exact lifecycle view → cached rows | Uses the shared ordinary reader, so the new admission and transformation also apply to lifecycle replay. |

Pinned dependency references above resolve under the local Cargo registry to `iceberg-0.10.1`, `parquet-58.4.0` and `arrow-array-58.4.0`; the reviewer read those implementations directly.

## Data flow and invariants

1. **One schema authority.** The exact opened native metadata supplies current schema and field IDs. Task-local normalization is ephemeral interpretation, not a second persisted schema. Old table objects/pins continue using their old metadata. The patch neither changes a selected lifecycle result nor edits files.
2. **Values follow IDs rather than names or order.** Native pipeline `src/arrow/reader/pipeline.rs:247` installs the requested file constant; the transformer creates its special type at `:372`, chooses the general path at `:411–421`, then resolves source fields by ID at `:526`. Same-name replacement with a new ID yields null for the qualified missing optional primitive, rather than reading the old value.
3. **The forcing column cannot escape to consumers.** The final native column must have the exact reserved ID and expected run-end/value types. Only then is it removed. Arrow `RecordBatch::project` retains `row_count` even with no selected value columns (`arrow-array-58.4.0/src/record_batch.rs:455–478`). Existing value arrays are shared, not copied by this removal.
4. **The extra native column is admitted.** Its encoded representation holds one path value and one run end per batch; it is not expanded into row-count copies of the path. Application byte validation happens while it remains present. The independent occurrence locator allocation retains its existing separate preallocation/final checks.
5. **Capacity ownership stays attached to work.** No new producer task or detached blocking worker is introduced. Ordinary stream state retains its reservation; occurrence stream state retains both batch and native inventory custody. Native planner/reader cancellation quiescence and all decoder allocation are not newly guaranteed by those wrapper permits.
6. **Metadata growth remains finite but not free.** Task schemas share an Arc; requested-ID vectors remain task-owned and are included in selective-scan metadata accounting. Historical schema validation uses temporary per-schema root-ID sets rather than a persistent cache. Very wide/history-heavy metadata remains subject to existing upstream/admission limitations; this checkpoint does not qualify arbitrary metadata growth.
7. **Scope stays narrow.** Nested field structures may pass through unchanged; nested evolution and type/default transitions are not introduced. Metadata-only source selection, stable-ID replacement writers, scalar dependency binding and schema-aware incremental input changes remain unimplemented by this patch.

## Test behavior and evidence

All execution evidence is owner-produced. The reviewer inspected assertions and logs but ran no tests. `schema-native-reorder-failed.log` retains the native failure; earlier setup/compile/capacity failures are also retained. `schema-native-probe.log` records the initial workaround probe succeeding. `schema-reader-targeted.log` records successful targeted reader/schema qualification. After the fixture correction, `schema-reader-check.log` records the complete successful `scripts/check` gate, including the existing integration, recovery and nested-value suites. The last changes added the nested file-ID collision guard and stronger unit assertions. After those changes, `schema-reader-final-unit.log` records successful formatting, all-target/all-feature Clippy with warnings denied, and all-feature library tests. The complete integration gate therefore precedes that narrow final guard; it was not rerun after that guard. No no-default-feature result is claimed for this checkpoint.

| Test / location | Actual assertion and trace | Assessment |
| --- | --- | --- |
| `native_current_schema_projection_preserves_field_ids_nullable_additions_and_historical_files`, `src/storage/occurrences/tests/schema.rs:17` | Forks native metadata under a distinct UUID/location, preserves source IDs and snapshot, reads nullable additions/rename/reorder, appends new native files, compares the complete mixed-row multiset through ordinary and occurrence reads, distinguishes null/zero/empty, checks old file hashes and modification times, reads original historical schema. | Exercises native registration and mixed old/new physical files without rewriting prior payloads; strengthened assertions pass in the final library suite. |
| `native_current_schema_projection_reorders_equal_types_by_id`, `:210` | Asserts independently written Int64 values swap by stable ID, including nulls; DataFusion reads equal ordinary reader results. A second schema requests only a new same-named ID while physical and augmented field counts coincide. | Direct original-bug oracle and the forcing mechanism's equal-count boundary; passes. |
| `new_id_with_reused_name_never_reads_old_values`, `:344` | New ID 9 retains name `id`; results are null with ID 9. Whole and selective occurrence scans retain exact original file/ordinal membership. | Checks semantic field identity and physical evidence identity separately; passes. |
| `unqualified_schema_interpretations_refuse_before_reading_payload`, `:418` | Type, requiredness, non-null initial default and reserved-ID cases refuse during reader setup or occurrence-schema construction. | Changed failure paths refuse before payload-stream consumption; passes. |
| `transient_native_column_is_validated_and_charged_before_removal`, `:476` | Real augmented batch exceeds stripped-only allowance; missing sentinel and forged wrong-type sentinel refuse. | Direct SR-R3 regression passes in the final library suite. |
| `all_fields_hidden_and_a_user_file_name_preserve_values_and_occurrences`, `:549` | Zero visible columns preserve row counts and unique occurrence locations; a physical field renamed `_file` retains its actual Int64 values despite the virtual field having the same name. | Confirms stripping is by verified ID/position, not name, and empty visible schema retains rows; passes in the final library suite. |
| `native_virtual_run_end_limit_refuses_oversized_batches`, `src/budget.rs:313` | Accepts exactly `i32::MAX`, refuses the next configured batch size without allocating giant arrays. | Matches the actual native cast boundary. |
| Locator/ownership cases, `src/storage/occurrences/tests.rs:102`, `:157`, `:188`, `:219` | Independent Parquet row oracle, byte-identical files with distinct paths, batch sizes, empty source, capacity through partial read/drop/EOF/error, locator allocation and selective skipped-payload behavior. | Existing resource and occurrence properties remain exercised through the changed shared task path. |
| `joined_output_respects_a_limit_that_each_individual_input_fits`, `tests/backfill.rs:543` | Measures augmented input and occurrence capacity, then requires failure specifically from the combined result. | Corrected measurement preserves the original end-consumer admission check; passes in the complete gate. |
| `nested_replacements_preserve_values_and_native_field_ids`, `tests/updates.rs:580` | Independently inspects retained struct/list values and field IDs, with null/empty nested distinctions. | Existing unchanged-nested-value regression passes in the complete gate; nested schema evolution is not claimed. |

`docs/STATUS.md:660–689` accurately describes the reader prerequisite, admitted temporary column, all-fields-absent fallback and unimplemented public lifecycle pieces. Its qualification claims do not imply general column pruning or a whole-process memory result.

## Conclusion

**VERDICT: APPROVE.** The native workaround, shared interpretation, byte custody and refusal guards have concrete source traces and passing owner-run qualification. The identified resource inefficiency and fixture/coverage gaps are closed. The stated execution chronology preserves the distinction between the complete integration gate and the final-source library/Clippy checks.

- Coverage of changed paths: **ADEQUATE** for this reader checkpoint.
- Confidence: **HIGH** for the scoped mechanism; no throughput, pruning in the all-fields-absent case, hard native-allocation bound or public schema-operation completion is claimed.
