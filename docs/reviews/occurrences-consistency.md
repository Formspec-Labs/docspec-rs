# Physical occurrence reader: consistency review

**Verdict: APPROVE for the physical source-pinned stream.** No actionable
row-correspondence or task-completeness defect was found in the reviewed source.
The implementation preserves the narrow design boundary: its locators identify
rows inside exact retained files; they are not permanent logical identities.

This is a static semi-formal review of the uncommitted checkpoint against
`ac4064b`, reviewed on 2026-10-08. The reviewer read the new reader and tests,
storage exports, capability probe, native inventory implementation and maintained
Iceberg reader/task implementations. No tests or benchmarks were executed by this
reviewer. The retained `_pos` failure log is implementer-run evidence, not a
reviewer-run result.

## Findings and scope observations

### O1 — The immutable-source binding is a caller precondition

- **Severity:** OBSERVATION
- **Category:** correctness boundary
- **Location:** `src/storage/occurrences.rs:59-71`.
- **Evidence:** `read_occurrences` accepts a native `Table` and `Budget`, not a
  `TablePin` or verified object inventory. It verifies native file membership,
  task shape and decoded counts. It does not independently hash every source
  object. Its public documentation expressly requires the caller to bind paths
  to verified immutable objects (`:62`). `ObjectRef` carries that identity through
  owner/path/digest/length (`src/model.rs:51-58`).
- **Disposition:** Keep this lower-level boundary. A later backfill/combined-read
  caller must establish the exact source pin and qualified object identities
  before using these locators as evidence. Equal file contents or business keys
  cannot substitute for that binding. No such production caller is introduced in
  this checkpoint.

### O2 — Permit release is qualified; upstream-task quiescence is not

- **Severity:** OBSERVATION
- **Category:** resource qualification
- **Location:** `src/storage/occurrences.rs:85-94`, `107-112`.
- **Evidence:** The wrapper owns its admission permits until drop, terminal error
  or successful end of stream. The maintained `plan_files` implementation launches
  background manifest tasks and returns their receiver
  (`iceberg-0.10.1/src/scan/mod.rs:400-480`). Dropping that receiver is not evidence
  that every in-progress upstream read has already finished.
- **Disposition:** The inspected tests establish wrapper permit lifetime. Keep
  whole-process cancellation and resource ceilings unqualified until observed
  against actual upstream activity. This is not a row-membership defect or a
  request for a custom task scheduler.

### O3 — Backfill lifecycle findings remain outside this storage checkpoint

- **Severity:** OBSERVATION
- **Category:** scope
- **Location:** `docs/reviews/backfill-design-consistency.md:F1-F4`.
- **Evidence:** This patch adds no file-phase reuse keys, group definition,
  coverage commit, catch-up planner or group-selection operation. It therefore
  neither implements nor contradicts the design refinements for exact-object
  reuse, completion versus promotion guards, source-admission fences and merged
  release readback.
- **Disposition:** Preserve those requirements when wiring the reader into
  backfill. Approving this primitive does not approve completed R3/R5/R6.

## Patch summary

`src/storage/occurrences.rs` introduces a maintained native-file reader wrapper
that appends `__docspec_file` and `__docspec_row` before any relational operation.
It validates native snapshot/current-schema agreement, reserved field names,
exactly-once whole-file tasks and file-local decoded counts. It retains shared
batch and inventory admission while streaming. `src/storage.rs:3-4` privately
registers the module and publicly exports its narrow API and field constants.

`src/query.rs:261-282` turns the observed `_pos` capability failure into a pinned
dependency regression probe. The saved failure at
`docs/reviews/native-position-probe.log` shows the original native scan returning
`Unexpected => field not found`. The new probe asserts that specific missing
capability; it does not silently substitute a different interpretation of `_pos`.

The independent test oracle reads retained Parquet files directly and enumerates
each file's rows. This avoids using the new ordinal-assignment code to construct
its own expected results.

## Function trace

| Function / method | Location | Inputs | Output and verified behavior |
| --- | --- | --- | --- |
| `occurrence_schema` | `src/storage/occurrences.rs:30` | Captured native table | Refuses different snapshot/current schemas and case-insensitive reserved top-level names; preserves native source fields and appends nonnull path/Int64 position fields. |
| `read_occurrences` | `src/storage/occurrences.rs:68` | Native table and shared budget | Opens owned state, then exposes a fallible stream. Successful coverage requires consuming through terminal `None`. |
| `Occurrences::open` | `src/storage/occurrences.rs:97` | Table/budget | Reserves batch capacity; loads the maintained exact snapshot inventory; retains the inventory permit; plans native tasks with serial limits and constructs an unfiltered serial reader. |
| `Occurrences::next` | `src/storage/occurrences.rs:132` | Owned task/file state | Reads one file at a time, assigns checked consecutive ordinals, checks exact count at file EOF, rejects unknown/repeated paths and refuses final EOF with missing expected files. |
| `check_task` | `src/storage/occurrences.rs:200` | Planned task and corresponding native descriptor | Requires start zero, full file length/size, exact known row count representable by the locator, Parquet format, no predicate and no deletes. |
| `append_locations` | `src/storage/occurrences.rs:218` | Native batch, file path and checked ordinal range | Estimates added allocation before creating locator arrays, appends them to unchanged source arrays and enforces the final Arrow memory bound. |
| `oversized` | `src/storage/occurrences.rs:250` | None | Returns the stable `oversized_batch` refusal. |
| `snapshot_files` | `src/storage.rs:288` | Captured native metadata and budget | Reads exact native manifests, rejects duplicate live files/current delete content and returns current live data descriptors plus retained inventory admission. |
| Maintained `ArrowReader::read` | `iceberg-0.10.1/src/arrow/reader/pipeline.rs:48` | One task stream | The concurrency-one path sequentially prepares and flattens file streams (`:64-74`). The wrapper passes one validated task at a time. |
| Maintained task decoding | `iceberg-0.10.1/src/arrow/reader/pipeline.rs:210-405` | Exact file task | Uses native field-ID projection/transformations and the maintained Parquet batch stream. The wrapper rejects the predicate/delete/split inputs that would omit rows before numbering. |
| Test callers | `src/storage/occurrences/tests.rs:102`, `152`, `183`, `254` | Native fixtures and malformed state | Exercise public stream/schema functions plus private task validation. No production engine caller was found in this checkpoint. |

The task planner's default column list follows the captured native schema's field
order (`iceberg-0.10.1/src/scan/mod.rs:239-276`). The native transformer maps projected
field IDs to that ordered schema (`iceberg-0.10.1/src/arrow/record_batch_transformer.rs:347-408`).
The wrapper delegates field interpretation to those maintained components and
does not reconstruct Parquet values itself.

## Data flow and invariants

1. **Exact task population:** `snapshot_files` rejects duplicate live paths before
   they enter `expected` (`src/storage.rs:349-357`). `Occurrences::next` removes a
   path only once and refuses absent entries (`occurrences.rs:174-179`). Terminal
   task EOF requires an empty expected map (`:165-172`). A skipped, repeated or
   extra task cannot produce a successfully completed stream.
2. **One full physical file:** Each task must cover byte start zero and exactly
   the native descriptor's length/size, with the same known row count and no
   predicate/deletes (`:200-215`). The maintained reader can otherwise apply
   selection before output (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:283-389`),
   so these checks preserve the meaning of emitted ordinals.
3. **File-local ordinal:** `FileRows.next` begins at zero for each newly accepted
   path (`occurrences.rs:188-193`). A batch advances it only after checked addition,
   count/range validation and successful locator construction (`:136-155`). Positions
   therefore span consecutive physical rows rather than a result-wide sequence.
   The file task order may vary without changing `(path, ordinal)` identity.
4. **Exact file completion:** Too many rows refuse before the excess batch is
   returned (`:136-145`). Too few rows refuse when the file stream ends (`:157-162`).
   A caller may observe earlier valid batches before that refusal; it must not
   equate partial consumption with complete coverage, as the public API documents
   (`:63`).
5. **Multiplicity and nulls:** Source arrays are retained unchanged; the added
   nonnull locator does not depend on source values (`:236-243`). Equal or null
   business values receive distinct positions within each file. Distinct paths
   remain distinct even when bytes happen to match; no digest-based deduplication
   exists in this wrapper.
6. **Schema boundary:** Snapshot/current-schema disagreement refuses before any
   inventory/task work (`:30-39`). Existing source names cannot collide with the
   internal locator columns (`:40-48`). This does not establish general schema
   evolution or arbitrary native type support.
7. **Owned admission:** `Occurrences` owns both permits (`:92-93`), including the
   reservation originally returned by the inventory (`:128`). The fallible unfold
   owns the entire state (`:72-75`). Drop, terminal error and successful EOF destroy
   it; no application-level producer task is detached by this wrapper. O2 limits
   what that proves about upstream background tasks.
8. **Byte bound:** The locator allocation estimate uses saturating arithmetic and
   includes repeated path bytes, string offsets and Int64 positions (`:228-235`).
   The final check includes actual Arrow buffer accounting (`:243-245`). The limit
   is a reader admission rule, not measured whole-process memory usage.

## Test behavior and edge cases

These are static assessments of assertions, not reviewer-run results.

| Test | Location | Expected behavior and assertion trace |
| --- | --- | --- |
| `locators_match_independent_parquet_order_across_files_row_groups_and_batch_sizes` | `src/storage/occurrences/tests.rs:102` | Builds expected `(path, position) -> value` mappings using direct Parquet reads. Requires multiple row groups, preserves duplicate/null values, and compares exact mappings across different emitted batch sizes. Insertion rejects duplicate locators. An empty native table preserves the augmented schema and reaches clean EOF. |
| `reservations_survive_partial_reads_and_release_on_drop_eof_and_error` | `src/storage/occurrences/tests.rs:152` | After a partial read, further admission refuses. Dropping the stream releases all wrapper capacity. Full consumption and an oversized-batch error also release capacity, with the expected stable refusal code. |
| `malformed_incomplete_and_repeated_native_tasks_cannot_establish_coverage` | `src/storage/occurrences/tests.rs:183` | Mutates start, length, unknown/wrong count, size, predicate, file format and delete references; requires `unsupported_occurrence_scan`. Empty/repeated task streams must end in `occurrence_tasks`, never successful EOF. Altered expected decoded counts in both directions must end in `occurrence_count`. |
| `reserved_names_and_unqualified_schema_changes_refuse` | `src/storage/occurrences/tests.rs:254` | Constructs metadata with a current schema different from the existing snapshot and requires `unsupported_schema_evolution`. Reserved path/position names, including uppercase spelling, refuse with `reserved_field`. |
| `pinned_native_position_column_is_unavailable` | `src/query.rs:263` | Uses the pinned native API with `_file`/`_pos` and requires its first yielded error to contain `field not found`. A future dependency repair changes this qualification result rather than silently changing the wrapper. |

Concrete qualification limits remain: the value/order fixture uses nullable Int64
source values; it is not exhaustive native type qualification. The oracle uses the
maintained Parquet decoder directly, independent of the new task/locator code but
not an independently implemented Parquet format. No end-to-end backfill phase or
promotion test is introduced here because those operations are outside this patch.

## Relation to the backfill design review

The implementation fulfills the narrow reader recommendations: maintained decode,
whole-file restrictions, exactly-once inventory/task checks, independent physical
order expectations, counts checked through EOF, reserved locator names, and an
explicit statement that physical locations are not stable logical IDs across
rewrites (`occurrences.rs:19-29`).

It deliberately stops before the design's remaining selection decisions. A later
consumer must retain exact immutable object identity alongside emitted paths,
preserve original group generation on reuse, distinguish completed computation
from promotion eligibility, and verify the actual merged release. Those are not
implied by a successful occurrence scan.

## Exploration notes

- **H1:** Native file-task order could change locator identity. **Refuted:** each
  task names a file and starts its own counter; global task order is irrelevant.
- **H2:** Filtered/split/deleted tasks could produce shifted ordinals. **Prevented
  in this scope:** task checks refuse these inputs before file decode.
- **H3:** Matching total counts could hide missing/repeated files. **Refuted:**
  path membership is checked independently of counts.
- **H4:** Early stream termination could be mistaken for completed coverage.
  **Remaining caller rule, explicitly documented:** only successful terminal EOF
  establishes the wrapper's completeness checks.
- **H5:** This solves permanent occurrence identity and backfill selection.
  **Refuted by scope and code:** it produces source-pinned physical locators only.

## Conclusion

**VERDICT: APPROVE** for the physical source-pinned occurrence reader.

- The patch achieves its stated narrow intent through inspected task membership,
  order, count and schema checks, with a direct independent locator/value oracle.
- **Coverage of changed paths: ADEQUATE** for this local reader primitive. The
  source identity precondition, type/cancellation limits and incomplete backfill
  lifecycle are explicit.
- **Confidence: HIGH** in the reviewed static invariants. No execution,
  performance, full process resource ceiling or completed R3/R5/R6 claim is made.
