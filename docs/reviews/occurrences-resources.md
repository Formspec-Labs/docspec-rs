# Source occurrence reader: resource and simplicity review

**VERDICT: APPROVE the physical locator stream checkpoint.** No actionable
implementation defect remains in the inspected source. The requested test that
isolates locator-column memory was added and its successful execution appears in
the implementation owner's retained log.

Static review on 2026-10-08 of the working tree relative to `ac4064b`, applying the
semi-formal-code-review skill and relevant Rust guidelines. Scope:
`src/storage/occurrences.rs`, its tests and reexport, the native `_pos` capability
assertion, and the status claims. The reviewer did not execute tests, modify
implementation files, or commit. Backfill and field-group selection are outside
this checkpoint.

## Findings

No blocker, warning, or requested source change was found after tracing the
current implementation. The following scope observations are relevant to future
integration; they are not defects in the promised physical stream.

- **Resource qualification:** application permits and configured batch size do
  not establish a whole-process resident-memory ceiling. Native metadata and
  Parquet decoding allocate before the application checks decoded bytes
  (`src/storage.rs:376`; `src/storage/occurrences.rs:135–153`). Status states this
  limitation explicitly (`docs/STATUS.md:153–157`).
- **Future reuse:** the stream opens every live file after inventory discovery
  (`src/storage/occurrences.rs:165–187`). Backfill must resolve reusable file work
  before creating that file's native reader. Filtering emitted rows would still
  read and decode unchanged files. The current API does not claim selective
  backfill or catch-up performance.
- **Metadata work:** startup uses the existing complete retained-snapshot
  inventory and then native current-snapshot planning
  (`src/storage/occurrences.rs:100–112`; `src/storage.rs:302–338`). This is simple
  reuse of qualified facilities, but entails overlapping metadata traversal and
  startup work dependent on retained history. No reduced metadata-request count
  or large-history timing is established.

## Patch summary

The new reader adds `__docspec_file` and `__docspec_row` to each source batch.
Their values identify an exact native file path and zero-based position inside
that file. Each file is read whole and without filtering before positions are
assigned. Complete coverage requires successful end-of-stream consumption;
consumers must bind the paths to verified immutable objects in their source pin
(`src/storage/occurrences.rs:19–26`, `:59–75`).

The implementation uses maintained native planning and Arrow decoding. It adds
one small stream state machine, with no durable row index, custom task
serialization, detached application worker, or separate scheduler. Public
registration is a reexport (`src/storage.rs:3–4`). Production callers are **none
found** in this checkpoint; the new tests call the API directly. Existing plain
reads and comparison behavior are unchanged.

## Function trace

| Function / method | File:line | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `occurrence_schema` | `src/storage/occurrences.rs:30` | Native table → augmented Arrow schema | Refuses snapshot/current schema disagreement and reserved names, then adds nonnullable path and position fields. Preserves schema metadata. |
| `read_occurrences` | `src/storage/occurrences.rs:68` | Exact native table and shared budget → owned stream | Opens admitted state, then transfers ownership through `try_unfold`. Failure and exhaustion drop that state. |
| `Occurrences::open` | `src/storage/occurrences.rs:97` | Table and budget → reader state | Reserves two batch allowances, acquires bounded native inventory, retains its reservation, sets native planning and data-file concurrency to one, and sets explicit batch rows. Disables row filtering/selection. |
| `snapshot_files` | `src/storage.rs:288` | Native table and budget → retained object inventory and live descriptors | Applies existing finite metadata admission, deduplicates historical manifest traversal, refuses repeated live files and current native delete files. |
| `Occurrences::next` | `src/storage/occurrences.rs:132` | Mutable admitted state → next augmented batch or end/error | Keeps one active file reader, advances checked row counts, verifies exact file completion, rejects omitted or repeated tasks, and yields cooperatively between files. |
| `check_task` | `src/storage/occurrences.rs:200` | Native task and expected file → accept/refusal | Requires complete Parquet-file range and exact size/count; rejects predicates, delete files and unsupported ordinal counts. |
| `append_locations` | `src/storage/occurrences.rs:218` | Source batch, path, ordinal range, schema, limit → augmented batch | Estimates extra locator buffers before allocation, shares existing column buffers, creates locator arrays, then checks the actual augmented footprint. |
| `oversized` | `src/storage/occurrences.rs:250` | None → typed error | Returns stable `oversized_batch` refusal identifying source values plus occurrence locations. |
| Native capability assertion | `src/query.rs:263` | Maintained native scan requesting `_pos` → expected read error | Retains a test of the pinned dependency gap that motivates this adapter. It does not change the query provider. |

Pinned dependency source was read under the local Cargo registry:
`iceberg-0.10.1/src/scan/context.rs:114–142` creates full-file tasks with captured
size/count; `src/scan/mod.rs:135–141` sets planner concurrency;
`src/arrow/reader/pipeline.rs:64–74` uses serial file-stream flattening at
concurrency one. The decoder/transformer path preserves rows while adapting
columns (`src/arrow/reader/pipeline.rs:390–405`). Native planning separately
spawns work (`src/scan/mod.rs:401–477`); application stream drop does not prove
instant termination of every upstream I/O operation.

## Data flow and invariants

| State | Creation, mutation and use | Invariant / failure boundary |
| --- | --- | --- |
| Batch and metadata permits | Acquired in `open`; moved into owned fields (`:99–128`) | Held through partial consumption. Dropping the wrapper or completing with end/error drops its owned permits. A failure during preparation also drops already-acquired locals. |
| Expected file set | Live inventory descriptors become a path-keyed map (`:102–106`); each task removes one entry (`:174`) | No task can repeat a path or introduce an unknown file. Successful stream completion requires an empty map (`:165–179`). Paths distinguish byte-identical files. |
| Active file | One native task becomes one `FileRows` (`:181–193`); replaced only after EOF/count validation (`:157–163`) | The reader never advances to a different file while the current file remains active. It does not collect payloads or all native tasks. |
| Position | Starts at zero for each file (`:191`); checked addition for each batch (`:136–145`) | Batch boundaries do not change file-relative position. Overflow, decoded overrun and decoded underrun refuse. Position fits the public signed integer type. |
| Locator buffers | Calculated and allocated in `append_locations` (`:228–245`) | Variable path bytes and ordinals participate in admission. Source columns remain shared; the final check covers actual augmented Arrow memory. |
| Coverage | Defined by successful complete consumption (`:63`, `:157–179`) | A consumer must not treat an early batch as a completed file or dataset. Errors after partial delivery invalidate completion, while previously delivered batches remain provisional. |
| Schema | Captured once (`:98`); reused in each output batch (`:151`, `:243`) | Reserved fields cannot silently overwrite source values. Unqualified snapshot/current schema differences refuse before admission. |

Admission bounds application activity, not arbitrary retention of returned
batches by a consumer. Path strings are repeated per row and checked as such.
Serial decoding and this straightforward representation are reasonable for the
current prerequisite; dictionary encodings or parallel file scheduling would
need measured benefit and separate qualification.

Review hypotheses were refined as follows: a permit could be released before
consumption (refuted by owned state); changing batch sizes could change physical
positions (refuted by per-file cumulative counts and independent correspondence
tests); extra locator columns might escape the batch check (refuted by both
checks and the added isolated test); and application cancellation might imply
native planner quiescence (not established, now explicitly excluded).

## Test behavior and concrete edge cases

| Test | Evidence and expected behavior from the trace |
| --- | --- |
| `locators_match_independent_parquet_order_across_files_row_groups_and_batch_sizes`, `src/storage/occurrences/tests.rs:102` | Builds its expected map with a direct independent Parquet reader, crosses row groups and batch sizes, preserves duplicate/null values, and confirms byte-identical files at distinct paths remain separate occurrences. Also checks empty input and its schema. Assertions agree with the file-local counter and complete-task handling. |
| `reservations_survive_partial_reads_and_release_on_drop_eof_and_error`, `:157` | Uses a budget sized to one active reader. Competing admission refuses; drop, complete consumption and oversized failure permit reacquisition. Matches `try_unfold` ownership. |
| `locator_columns_participate_in_batch_admission`, `:188` | Measures source-only batch memory, proves the ordinary reader succeeds at that allowance, then requires the locator reader to refuse and release capacity. This specifically covers the additional allocation instead of merely forcing an already-oversized source batch. |
| `malformed_incomplete_and_repeated_native_tasks_cannot_establish_coverage`, `:214` | Mutates task range, size, count, predicate, format and deletes; injects omitted/repeated task sets; changes expected decoded counts both ways. Each path requires a typed refusal instead of completed coverage. |
| `reserved_names_and_unqualified_schema_changes_refuse`, `:285` | Uses native metadata construction to create schema disagreement and reserved names, including case variation. Requires the corresponding schema refusal codes. |
| `pinned_native_position_column_is_unavailable`, `src/query.rs:263` | Asserts the maintained scan currently fails when `_pos` is selected. The original failure is retained in `docs/reviews/native-position-probe.log`; a future dependency change requires revisiting this fallback. |

The reviewer read owner-produced `docs/reviews/occurrences-check.log`. At review
completion it contains successful targeted correspondence and locator-memory
tests, and a successful build-check section. This is owner execution evidence,
not reviewer execution; this review does not independently assert that the final
whole-repository gate has completed.

Small-file populations and retained-history growth remain performance
qualification cases. Real task omission and row-count mismatch are covered with
controlled task/state substitution; this is not a claim that these faults were
induced in an external catalog service. Native reader failures propagate through
the same state-owning error path, but upstream cancellation timing and process
termination are outside these tests.

## Conclusion

**VERDICT: APPROVE.** The implementation achieves its stated physical locator
and application-admission intent with a small maintained-reader adapter. Changed
path coverage is **ADEQUATE**; confidence is **HIGH** in the static ownership,
count and byte-admission traces. Approval does not establish completed backfill,
stable logical identity across rewriting, selective reuse, aggregate memory
bounds, or end-to-end throughput.
