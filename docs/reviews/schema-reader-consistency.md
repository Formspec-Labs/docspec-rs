# Pinned-schema reader: consistency review

**Static verdict: APPROVE for the shared native reader prerequisite.** No
unresolved value, field-identity or occurrence-correspondence defect was found.
The mixed-file coverage gap identified during review is closed in the final test
source. This approval does not establish a public schema-evolution operation.

Reviewed against `90dfd5b` on 2026-10-08 using the semi-formal code-review and
Rust skills. This reviewer read the changed source, callers, maintained
Iceberg/Arrow implementations, test assertions and implementer-generated logs.
The reviewer did not execute tests or modify production code.

## Findings and qualification boundaries

### F1 — Mixed-schema assertions initially omitted inherited rows — closed

- **Severity:** WARNING, test coverage.
- **Location:** `src/storage/occurrences/tests/schema.rs:149`.
- **Initial evidence:** The first version filtered the post-append result to new
  rows whose key was at least 100. That assertion could pass even if every old
  file was omitted or its values were misbound after the append.
- **Closure:** The final fixture compares the complete old-plus-new typed
  multiset, preserving duplicate and null old rows and explicit null, zero and
  empty new values (`:149-161`). It repeats that value comparison through the
  occurrence reader and rejects duplicate physical locations (`:162-186`).
  Additional tests exercise an entirely hidden schema and a user field named
  `_file` (`:549`). These close the concrete review counterexamples without
  changing the production design.

### O1 — The workaround repairs an observed dependency defect

- **Severity:** OBSERVATION, correctness.
- **Location:** `src/storage/schema.rs:74`.
- **Evidence:** Iceberg 0.10.1 chooses a schema-only transformation when column
  count, types and nullability match; that comparison does not examine field IDs
  (`iceberg-0.10.1/src/arrow/record_batch_transformer.rs:437-469`). The retained
  `schema-native-reorder-failed.log` shows the resulting value swap: requested
  `b` received the old `a` values. This is distinct from the metadata-filename
  fixture failure present in the same log.
- **Disposition:** The added native metadata column has a run-end-encoded type
  that stored Iceberg fields do not have, forcing the maintained ID-based branch.
  Its removal checks both its reserved ID and exact native type
  (`schema.rs:102-129`). Keep the focused reorder and name-reuse tests when
  upgrading Iceberg; do not remove the workaround based only on an API change.

### O2 — Reader interpretation is narrower than lifecycle evolution

- **Severity:** OBSERVATION, scope.
- **Location:** `docs/STATUS.md`, “Pinned-schema reader checkpoint”;
  `src/storage/schema.rs:17`.
- **Evidence:** The patch introduces no new operation, selection record, schema
  authority, expression rebinding or source-update writer. It qualifies current
  native metadata against retained schema history and delegates values to the
  maintained reader. Native metadata remains the schema authority.
- **Disposition:** Public evolution still requires ID-preserving replacement
  candidates, ID-bound consumers, durable absence/default semantics and guarded
  selection. Direct Parquet update discovery is not made schema-evolution-safe
  merely because shared native reads now understand these changes. The updated
  status document states that boundary explicitly.

### O3 — Output admission is not a decoded-memory or throughput result

- **Severity:** OBSERVATION, performance and resources.
- **Location:** `src/storage/schema.rs:102`, `src/budget.rs:71`.
- **Evidence:** The augmented native batch is checked before stripping `_file`.
  The configured row limit cannot exceed the native signed-32-bit run end. When
  every requested ordinary field is absent, Iceberg's projection fallback still
  reads physical columns before constructing the requested nulls
  (`iceberg-0.10.1/src/arrow/reader/projection.rs:99-145`, `:155-219`).
- **Disposition:** Preserve the documented temporary-decoding limitation. The
  code retains shared admission and removes duplicate schema interpretation; it
  does not demonstrate a speedup, physical column pruning or a process memory
  ceiling. Resource qualification is reviewed separately.

## Patch summary

`src/storage/schema.rs` centralizes the interpretation used by ordinary native
reads, full occurrence reads and selected-file occurrence reads. It accepts
top-level rename, reorder and removal, plus optional primitive additions without
an initial default. It refuses unqualified changes to type, requiredness, default
interpretation or nesting. The exact retained metadata supplies the target schema;
the exact selected snapshot still supplies the physical files.

The adapter replaces the task's schema and requested field IDs, appends Iceberg's
transient file-path field and removes that field after the maintained reader has
mapped values by ID. It does not implement a decoder, write a second schema map
or change the captured file population. `Budget` rejects batch sizes outside the
native run-end representation. Existing resource fixtures account for the
transient field so they continue testing their intended refusal boundary.

Important callers include independent release reads
(`src/engine/read.rs:66`), native query partitions (`src/query.rs:59`), lifecycle
catalog replay (`src/catalog.rs:164`) and selected-file backfill/incremental scans
through `OccurrenceScan` (`src/storage/occurrences.rs:195`). None resolves a newer
catalog table head inside this reader.

## Function trace

| Function | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `read_schema` | `src/storage/schema.rs:17` | Captured table → Arrow schema | Rejects the virtual file ID anywhere in the current schema and reserved top-level IDs. Compares each current root field with retained schemas; unchanged field interpretation may have a different name/order. Missing historical fields must be optional primitives without initial defaults. |
| `unsupported` | `src/storage/schema.rs:67` | None → refusal | Supplies the stable `unsupported_schema_evolution` code. |
| `plan_files` | `src/storage/schema.rs:74` | Captured table → native task stream | Qualifies first, plans the same snapshot files serially, then sets each task's exact current schema and ordered IDs plus the virtual file ID. Other task fields remain unchanged. |
| `strip_file_column` | `src/storage/schema.rs:102` | Augmented batch and allowance → application batch | Checks all native array bytes, verifies the last field's ID/type and projects away only that column. Missing or malformed metadata fields refuse. |
| `read_batches` | `src/storage.rs:74` | Table/budget → fallible batch stream | Reserves capacity, configures the serial native reader and row bound, then strips each augmented batch. The stream owns the reservation until EOF, error or drop. |
| `occurrence_schema` | `src/storage/occurrences.rs:30` | Table → source fields plus locations | Uses shared schema qualification and preserves the existing case-insensitive reserved occurrence-name refusal. |
| `Occurrences::open` | `src/storage/occurrences.rs:88` | Table/budget → stream state | Captures exact inventory, shared task planning and serial unfiltered native reader; retains batch and inventory admission. |
| `Occurrences::next` | `src/storage/occurrences.rs:118` | Stream state → next located batch | Strips native metadata before assigning positions; checks exact per-file counts and complete task membership. |
| `OccurrenceScan::open` / `read_file` | `src/storage/occurrences.rs:196`, `:244` | Table, then exact file path → selected-file stream | Retains validated tasks from the same shared planner, bounds task metadata and reuses the same location/stripping path. Unknown requested files refuse. |
| `check_task` / `append_locations` | `src/storage/occurrences.rs:268`, `:286` | Native task/batch → validation/located batch | Still requires whole unfiltered Parquet files without deletes; assigns consecutive ordinals and checks final application batch bytes. |
| `Budget::create` | `src/budget.rs:66` | Limits → shared execution budget | Now rejects `batch_rows > i32::MAX`, matching the maintained virtual-column implementation. |
| `open_pin` | `src/storage.rs:530` | Exact pin → static native table | Verifies metadata bytes and exact snapshot/current-schema IDs without the producing catalog. |
| `NativePartition::execute` | `src/query.rs:59` | Captured table/budget → query stream | Lazily calls these shared readers; its declared schema already comes from the same current native metadata. |
| Maintained reader pipeline | `iceberg-0.10.1/src/arrow/reader/pipeline.rs:215-261` | Modified task → transformed batches | Removes virtual IDs from physical selection, adds a constant file field and passes requested IDs/current schema to the maintained transformer. |
| Maintained transformer | `iceberg-0.10.1/src/arrow/record_batch_transformer.rs:346-569` | Physical schema plus requested IDs → typed arrays | The forced full branch maps source and target by native ID and constructs missing primitive values from default/null rules. |

## Data flow and invariants

1. **Exact schema and file selection stay separate.** `open_pin` validates both
   identifiers in retained metadata. Native scan planning originally uses the
   snapshot's schema (`iceberg-0.10.1/src/scan/mod.rs:221`); `plan_files` replaces
   only the task interpretation and requested IDs (`schema.rs:94-98`). It does
   not change snapshot membership, file sizes, row counts or delete/predicate
   descriptors. Occurrence scans retain their stronger whole-file restrictions.
2. **IDs determine value identity.** Current field order creates the request
   vector (`schema.rs:77-87`). The virtual run-end-encoded field prevents the
   native name-only shortcut. The full native transformation resolves each
   requested ID against physical IDs. Reusing a name with a new ID therefore
   produces null for old files rather than copying the retired field's values.
3. **Interpretation changes fail before payload consumption.** Qualification runs
   before `plan_files` starts native planning (`schema.rs:75`). Existing root
   fields must preserve type, requiredness and initial default; a nested field
   cannot be promoted to a root merely by retaining its ID (`:38-60`). The guard
   examines every retained schema, intentionally refusing some cases a more
   sophisticated per-file proof might allow.
4. **Transient metadata cannot become user data or erase a same-named field.**
   Stripping is positional and verifies reserved ID/type, not a name search
   (`schema.rs:110-129`). A user field named `_file` with an ordinary native ID
   remains among the requested ordinary fields. A nested field with the virtual
   ID refuses before planning (`:19-24`).
5. **Empty visible schemas preserve occurrences.** Arrow `RecordBatch::project`
   retains `row_count` even with no columns
   (`arrow-array-58.4.0/src/record_batch.rs:454-478`). The occurrence path appends
   locations after stripping, so hiding every value field cannot erase rows or
   alter file-local ordinals.
6. **Historical reads remain immutable.** The fixture forks metadata through the
   maintained builder/register API, keeps old file objects and reads the original
   table afterward. The adapter owns/clones schema references and task values;
   it does not mutate native metadata or Parquet bytes.
7. **Admission covers the extra returned array.** The augmented batch passes its
   byte check before projection; final occurrence batches pass a second check.
   Reservations remain owned by the same fallible stream states. The row cap
   protects the maintained `num_rows as i32` conversion
   (`record_batch_transformer.rs:632-635`), without claiming to bound allocations
   inside the native decoder.

## Test behavior and edge cases

The following assertions were inspected, not executed by this reviewer.

| Test | Location | Expected assertion behavior |
| --- | --- | --- |
| `native_current_schema_projection_preserves_field_ids_nullable_additions_and_historical_files` | `src/storage/occurrences/tests/schema.rs:17` | Preserves stable ID and snapshot under metadata fork, checks renamed/reordered values and missing-field nulls, appends explicit null/zero/empty values, compares the complete mixed-file multiset through both readers, and checks original bytes/mtime/history. |
| `native_current_schema_projection_reorders_equal_types_by_id` | `src/storage/occurrences/tests/schema.rs:210` | Direct and DataFusion reads return `b` and `a` in the requested order with independently specified values. Also tests an all-absent field where physical and augmented target column counts coincide. |
| `new_id_with_reused_name_never_reads_old_values` | `src/storage/occurrences/tests/schema.rs:344` | Requires the new ID, all-null values and exact source locations through ordinary, full-occurrence and selected-file paths. |
| `unqualified_schema_interpretations_refuse_before_reading_payload` | `src/storage/occurrences/tests/schema.rs:418` | Requires early structured refusal for type, requiredness, initial-default and reserved-ID cases. |
| `transient_native_column_is_validated_and_charged_before_removal` | `src/storage/occurrences/tests/schema.rs:476` | Actual augmented bytes exceed stripped bytes; an allowance covering only user values refuses. Missing metadata and a forged reserved ID with the wrong type refuse. |
| `all_fields_hidden_and_a_user_file_name_preserve_values_and_occurrences` | `src/storage/occurrences/tests/schema.rs:549` | All-hidden reads retain every row and unique occurrence; an ordinary `_file` value field retains its values and does not disappear during stripping. |
| `native_virtual_run_end_limit_refuses_oversized_batches` | `src/budget.rs:313` | Rejects the first unrepresentable row limit and accepts the representable boundary without allocating such a batch. |
| Existing occurrence suite | `src/storage/occurrences/tests.rs:102` onward | Independent Parquet order, duplicate/null locations, malformed/missing/repeated tasks, selective payload opening, capacity lifetime and locator-byte refusal continue through the shared planner. |
| `joined_output_respects_a_limit_that_each_individual_input_fits` | `tests/backfill.rs:543` | Measures native values plus the transient field and source locators, then still requires the larger combined result to refuse. This fixture adjustment preserves its original purpose. |

These fixtures do not qualify all Iceberg primitive types, arbitrary imported
files, partition/schema evolution, delete-bearing occurrence scans or nested
structural edits. Nested-to-root rejection is supported by the inspected guard;
a direct regression fixture for that branch was not present at this review pass.
No such feature is inferred from the scalar rename/addition fixtures.

## Inspected execution evidence

- `docs/reviews/schema-native-reorder-failed.log` preserves the original equal-type
  reorder failure and a separate fixture metadata-filename error.
- `docs/reviews/schema-reader-targeted.log` records successful targeted native and
  occurrence qualification before the final strengthened assertions.
- `docs/reviews/schema-reader-check-failed.log` preserves the old combined-output
  fixture failing because its allowance omitted transient native bytes. The
  revised fixture measures those bytes instead of weakening production checks.
- `docs/reviews/schema-reader-check.log` records the completed owner-run
  `scripts/check` gate: formatting, Clippy with warnings denied, application and
  integration tests, and doc tests. The final mixed-file/full-hide/name-collision
  assertions and virtual-column-specific fixture were added after that run was
  compiled.
- `docs/reviews/schema-reader-final-unit.log` records the subsequent successful
  final-source Clippy and all-feature library test run. The owner also reports
  successful formatting in that command sequence. This run includes the final
  complete mixed-file, all-hidden, name-collision and transient-column assertions,
  plus the late nested reserved-file-ID guard in the compiled source. The full
  integration suite was not rerun after those final narrow changes. No test result
  here is reviewer-generated.

## Exploration notes

- **H1 — Pinned snapshot planning could ignore the current schema. Confirmed and
  addressed:** maintained planning chooses snapshot schema; the shared adapter
  explicitly replaces task interpretation with exact current metadata.
- **H2 — Changing task order alone could misbind equal-typed values. Confirmed
  upstream, closed here:** dependency source and retained failure establish the
  shortcut; the virtual-column transformation and value assertions close it.
- **H3 — Added metadata could alter row identity or leak into consumers. Refuted
  statically:** stripping precedes locations, validates ID/type and preserves row
  count; all reader paths use the same functions.
- **H4 — Mixed-file tests might validate only newly written rows. Confirmed in
  the first fixture, closed by the complete multiset and occurrence assertions.**

## Conclusion

**VERDICT: APPROVE** for the pinned-schema reader prerequisite.

- The patch achieves the stated reader behavior through exact native metadata
  and maintained ID-based decoding, with explicit refusals outside that subset.
- Coverage of changed paths: **ADEQUATE for this checkpoint**, with successful
  final-source unit evidence and the full preceding integration gate. Broader
  type/evolution cases remain explicitly outside qualification.
- Confidence: **HIGH** in the inspected identity and value path; no performance,
  whole-process resource or complete R3 claim is made.
