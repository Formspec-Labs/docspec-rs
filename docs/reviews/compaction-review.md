# Local compaction: independent code review

Verdict: **APPROVE for the limited local compaction increment**, following static
re-review of the fixes below. The identified recovery and metadata-growth
blockers are resolved in code and covered by focused test cases. The implementation
owner must supply the test/gate results. Writer adoption and broader qualification
remain unfinished; this approval does not establish full R6 completion.

Reviewed on 2026-10-09 against uncommitted changes on
`codex/greenfield-lifecycle`, with base commit
`d57042e00a43b821b9730ad688d94ad6d2e011f4`. This is an independent static review;
no Cargo command, build, formatter, test or benchmark was run by this reviewer.
The implementation owner owns execution evidence. The working tree changed
during review; the shared-source representation lookup already uses logical
metadata and does not require the creating version to equal the reading version.

## Findings

### F1 — Resolved: source retirement prevented admitted recovery

- **Severity:** BLOCKER; correctness and recovery.
- **Original location:** `src/engine/compaction.rs:109`, `:237`, `:319` before
  the review fixes. The evidence below records that initial failure.
- **Evidence:** `compact_admitted` checks the availability of `spec.version`
  before loading either `compaction-result` or `compaction-request`. It repeats
  that check before reserving output and before representation selection. The
  request separately retains the source pin, release manifest and owner grants
  (`src/engine/compaction.rs:157`, `src/engine/compaction.rs:163`,
  `src/engine/compaction.rs:203`). The root inventory protects those dependencies
  and both source/replacement pins (`src/engine/retention/inventory.rs:972`,
  `src/engine/retention/inventory.rs:1017`).
- **Counterexample:** Start compaction on a base release and stop after its
  rewrite. Select a sibling backfill, which retains the same logical source.
  Retire the historical base's standalone availability. Retrying the admitted
  compaction now returns `availability_retired` before consulting its saved
  rewrite. Even a successfully completed compaction cannot replay its retained
  result after its original version is retired. This differs from backfill,
  which checks base availability only for a new admission
  (`src/engine/backfill.rs:288`, `src/engine/backfill.rs:490`).
- **Fix:** Gate source availability only while accepting a new compaction.
  Recover an existing request/result through its exact retained identity and
  protections, and continue to check representation freshness before selection.
  Add a test that retires a historical base between rewrite and selection, plus
  replay after successful completion.
- **Resolution verified:** Retained results and captures now resolve before
  source availability checks. The check remains at fresh capture
  (`src/engine/compaction.rs:109`, `:120`, `:143`). Local maintenance excludes
  availability retirement while admitted work is running
  (`src/engine/worker.rs:130`, `src/engine/retention/retirement.rs:465`).
  Rewrite reservation and final selection use the admitted capture without
  revoking it when standalone source availability ends (`:232`, `:313`).
  `admitted_compaction_survives_retired_historical_result_availability`
  (`tests/compaction.rs:434`) checks retained rewrite reuse, completed replay,
  unchanged sibling selection/values, and refusal of a fresh post-retirement
  request. This reviewer read the assertions; execution belongs to the owner.

### F2 — Resolved: configurable targets allowed unbounded metadata growth

- **Severity:** BLOCKER for the stated bounded-compaction requirement;
  performance and resource correctness.
- **Original location:** `src/storage.rs:193`, `:201`, `:210`, `:269` before
  the review fixes.
- **Evidence:** The writer accepts any positive `target_file_bytes` and retains
  one fixed reservation of four maximum batches. The maintained Iceberg rolling
  writer stores all closed `DataFileBuilder` values until `close`: see
  `iceberg-0.10.1/src/writer/file_writer/rolling_writer.rs:125`, `:200`, `:231`
  in the local Cargo registry. A tiny positive target can roll after every
  batch. A single captured large input file can thus produce an increasing
  descriptor population without an output-metadata check while writing.
  Conversely, a very large target permits increasing row-group metadata within
  one file: the maintained Parquet writer retains `row_groups`, column indexes
  and offset indexes until close
  (`parquet-58.4.0/src/file/writer.rs:156`, `:263`, `:272`).
- **Implication:** Input inventory bounds do not bound output file count or
  row-group count. The subsequent append/pin occurs after the entire output
  vector exists (`src/engine/compaction.rs:440`, `:454`, `:455`), so a later
  refusal does not bound the allocation or wasted physical work. This is a
  static growth finding, not a measured RSS or throughput result.
- **Fix:** Bound/admit retained writer metadata, including both row groups and
  completed file descriptors, before accumulation. A conservative bound derived
  from captured rows and schema is acceptable for this local increment if
  unsupported requests refuse before rewriting. Include focused capacity cases
  for very small and very large targets.
- **Resolution verified:** The writer now reserves metadata separately, includes
  nested schema field paths in its conservative charge, truncates chunk
  statistics and disables page/offset-index growth (`src/storage.rs:202`, `:207`,
  `:220`). It periodically closes the maintained writer to release footer state,
  separately charges retained file descriptors and split offsets, and refuses
  another batch when the allowance is exhausted (`:270`, `:281`, `:299`).
  This avoids charging all historical row groups against one population-wide
  ceiling. Restarted writers share the maintained filename generator's
  `Arc<AtomicU64>` counter, so closure/restart does not reuse file names
  (`iceberg-0.10.1/src/writer/file_writer/location_generator.rs:105`, `:131`).
  `tests/compaction.rs:509` covers refusal before selecting partial tiny-target
  output and later retry with more capacity; `tests/compaction.rs:588` checks
  that a huge target closes bounded windows and preserves the entire population.
  These are admission safeguards, not proof of aggregate RSS or throughput.

### F3 — Resolved: quadratic file-membership validation

- **Severity:** WARNING; performance and simplicity.
- **Location:** `src/engine/compaction.rs:391`.
- **Evidence:** Each selected path restarts `scan.files().any(...)`. When an
  empty request list means all files, this performs O(F²) path comparisons for
  F live files before opening payloads. `OccurrenceScan` already owns a
  `BTreeMap` keyed by path (`src/storage/occurrences.rs:209`, `:314`).
- **Fix:** Add a path-membership method to the existing scan or construct one
  path set once. This removes repeated iteration without new infrastructure.
  No elapsed-time improvement is claimed by this review.
- **Resolution verified:** Validation calls `OccurrenceScan::contains_file`
  (`src/engine/compaction.rs:387`), which uses the existing map's membership
  lookup (`src/storage/occurrences.rs:296`). The validation now requires
  O(F log F) map lookups for an all-file request.

### F4 — Compaction savings do not yet carry forward through source updates

- **Severity:** WARNING; scope and performance.
- **Location:** `src/engine/write.rs:188`, `:347`, `:394`.
- **Evidence:** New source updates load the canonical source pin from the
  immutable base release and share/rewrite its original file inventory.
  Representation selection is currently consumed by managed group/combined
  readers and later compactions (`src/engine/read.rs:26`,
  `src/engine/backfill.rs:148`, `src/engine/compaction.rs:152`). A new update can
  therefore restore the old fragmented layout in its new logical version.
- **Action:** Keep this limitation explicit. Qualify writer adoption of selected
  representations before claiming maintenance savings survive ongoing ingestion.
  That change also needs origin-aware update normalization: the current update
  normalization branch checks supplied/default state (`src/engine/write.rs:204`).

### F5 — Negative equivalence and broader race qualification remain open

- **Severity:** WARNING; test coverage.
- **Location:** `src/engine/compaction.rs:540`, `:557`; `tests/compaction.rs:118`.
- **Evidence:** The reviewed tests demonstrate exact successful rewrites and
  recovery/refusal boundaries. They do not inject a missing, duplicated, altered
  or invented occurrence into the prepared replacement, nor exercise malformed
  stored origins, cleanup against active compaction roots or process termination
  during acknowledged compaction handoffs.
- **Action:** Retain these cases in R6 qualification. The comparison and origin
  validation branches were traced statically; this review does not claim those
  failure cases have been executed.

## Patch summary and scope

The patch adds explicit file-group compaction, durable request/rewrite/verified/
selected records, logical-state metadata on `TablePin`, stable source occurrence
fields, and replacement selection for managed group/combined readers.
The CLI and HTTP entry points call the same `Engine::compact` service
(`src/main.rs:596`, `src/http.rs:419`).

Reviewed the compaction changes in `src/model.rs`, `src/storage.rs`,
`src/storage/identity.rs`, `src/storage/occurrences.rs`,
`src/engine/compaction.rs`, managed reads, schema/write/incremental/backfill
integration, compaction retention/creation records, and `tests/compaction.rs`.
The export and restore implementations in the same working tree are outside
this review, except their shared entry-point or retention interactions read here.

The governing requirements are exact typed equivalence and stable occurrences,
protected old readers/writers, guarded representation selection, and retained
interrupted work (`docs/requirements/06-maintenance-retention.md:13`, `:15`,
`:17`, `:25`, `:44`). The accepted full plan remains unfinished
(`docs/PLAN.md:108`).

## Function trace

| Function or path | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `TablePin::logical_metadata` | `src/model.rs:86` | Pin → original metadata reference | Falls back to physical metadata for older pins. |
| `Engine::compact`, `compact_until` | `src/engine/compaction.rs:24`, `:38` | Request → result/refusal | Shared owned work admission; test-only interruption after durable boundaries. |
| `compact_admitted` | `src/engine/compaction.rs:87` | Request → captured/prepared/verified/selected records | Retains admission first, reuses prepared output and verification, preserves admitted recovery after source retirement, compares current representation before selection. |
| `representation_pin` | `src/engine/compaction.rs:52` | Logical group + lifecycle view → physical pin | Uses logical metadata digest/group; requires verified equivalent row count and metadata identity. |
| `rewrite_compaction` | `src/engine/compaction.rs:370` | Captured pin + file list → replacement | Shares untouched files, rewrites selected files, retains original logical identity; checks live disk reserve between batches. |
| `capture_scan`, `selected_stream`, `rewrite_batch` | `src/engine/compaction.rs:483`, `:500`, `:511` | Native files → typed batches | Uses maintained complete-file reader and writer; maps occurrence columns into stored origin fields for source groups. |
| `verify_equivalence`, `comparable`, `compare_streams` | `src/engine/compaction.rs:540`, `:528`, `:557` | Selected old/new streams → equality/refusal | Compares public values plus stable occurrence columns in order, across differing batch boundaries; checks total rows and EOF on both sides. |
| `check_result` | `src/engine/compaction.rs:612` | Capture + saved result → validation | Binds request, source, logical identity and row count before reuse. |
| `identity::{has_origins, with_origins, materialize}` | `src/storage/identity.rs:44`, `:69`, `:100` | Native schema/batch → stable origins | Adds fresh native field IDs; preserves stored origins; validates paired nulls, nonempty origin and nonnegative row; fills legacy physical origins. |
| `identity::{public_schema, public_batch, hidden}` | `src/storage/identity.rs:19`, `:32`, `:15` | Internal schema/batch → public fields | Hides only stored origin fields. |
| `Occurrences::next`, `append_locations` | `src/storage/occurrences.rs:133`, `:387` | Complete physical rows → occurrence batches | Uses stored origin fields when present; otherwise initial file path/position; complete-file row-count checks remain. |
| `write_batches_sized` | `src/storage.rs:184` | Batches + target → native files | Uses maintained typed writer, materializes null writer origins, closes bounded footer windows and caps retained descriptors. |
| Managed `read_group`, `read_pin`, `read_compaction` | `src/engine/read.rs:13`, `:151`, `:166` | Available release/result → stream | Resolves current representation for managed reads, hides origin storage, leaves catalog-free callers responsible for file lifetime. |
| Managed `read_combined`, `combined_dataset` | `src/engine/backfill.rs:135`, `:177` | Release groups → combined stream | Resolves all equivalent groups from one lifecycle view and joins computed values by stable occurrences. |
| `prepare_group`, `check_group_dependencies` | `src/engine/backfill/preparation.rs:10`, `:443` | Source + definition → reusable group | Uses logical source metadata for whole-group reuse; accepts retained canonical source dependency for an equivalent representation. |
| `Expressions::compile` | `src/engine/backfill/expressions.rs:29` | Source schema + expressions → typed expressions | Refuses direct access to internal stored origin fields. |
| `verify_group_data` | `src/engine/backfill/verification.rs:376` | Source + computed group → deep verification | For stored origins, uses a full identity join and exact equal row counts instead of physical-file intervals. Relies on source-origin uniqueness. |
| Incremental key discovery / `qualify` | `src/engine/incremental.rs:251`, `:479` | Old/new source → work keys/qualification | Equivalent logical metadata yields no affected keys; schema comparison excludes stored origins. |
| `schema::{normalize, fields}` / source write | `src/engine/schema.rs:249`, `:427`; `src/engine/write.rs:102` | New data/schema edit → native schema/batches | Internal fields cannot be supplied as source values or update keys; schema edits preserve existing stored origins. |
| Retention `table`, compaction record collection / `visit_creation` | `src/engine/retention/inventory.rs:827`, `:972`; `src/engine/retention/owners.rs:795` | Lifecycle records → protected objects | Preserves logical metadata, capture dependencies, old/new pins and prepared output creation evidence. |

## Data flow and invariants

- A new compaction captures an available immutable release and the current
  equivalent group. Owner grants precede retained request admission. Production
  begins only after a protected output directory is recorded
  (`src/engine/compaction.rs:142`, `:163`, `:234`).
- Native replacement metadata preserves field meaning and IDs while dropping
  old snapshots from the new representation. Untouched files remain shared and
  selected rewritten files replace only their captured members
  (`src/storage/tables.rs:37`, `src/engine/compaction.rs:453`).
- Existing stored origins survive repeated compaction. Newly introduced origin
  columns receive the original physical file/row tuple, and equivalence checks
  compare the resulting occurrence columns as well as values
  (`src/storage/occurrences.rs:135`, `:395`; `src/engine/compaction.rs:511`,
  `:528`). Source-origin uniqueness follows from supported admission and exact
  rewrites; this review does not establish uniqueness for arbitrary externally
  fabricated pins.
- Representation selection changes one lifecycle key, while the canonical
  `ReleasePin` and logical version remain unchanged. Competing rewrites must
  still match their captured representation before selecting
  (`src/engine/compaction.rs:325`, `:336`, `:345`).
- Rewrite and verification completion records remain recovery roots. Current
  retention has no compaction retirement scope, so this increment retains these
  roots conservatively. It does not establish physical reclamation
  (`src/engine/retention/inventory.rs:972`,
  `src/engine/retention/retirement.rs:30`).
- Delete-bearing snapshots and partitioned/sorted/encrypted layouts refuse
  through the existing native adapters. Effective-delete compaction therefore
  remains outside the implemented subset
  (`src/storage.rs:517`, `src/storage/occurrences.rs:369`,
  `src/storage/tables.rs:41`).

## Test behavior and edge cases

Tests were read, not executed. The successful paths below have supporting code
traces; this review does not claim a passing test run.

| Test | Evidence and expected behavior from the trace |
| --- | --- |
| `source_compaction_preserves_duplicates_backfill_identity_and_logical_selection` (`tests/compaction.rs:118`) | Checks occurrence/value equality, managed combined reads, an already-open reader, unchanged latest, retry, repeated source compaction and computed-group compaction. Expected to pass the traced identity/selection paths. |
| `empty_source_and_partial_file_groups_preserve_exact_membership` (`tests/compaction.rs:162`) | Checks empty and partial rewrite, untouched-object sharing, exact retry and conflicting request refusal. Expected to pass the traced membership paths. |
| `native_nested_decimal_time_and_null_values_survive_typed_rewrite` (`tests/compaction.rs:195`) | Independently compares typed Arrow arrays for lists, decimals, time and nulls after rewrite. Expected to preserve native values. |
| `schema_defaults_and_supplied_state_remain_native_and_exact` (`tests/compaction.rs:267`) | Checks defaulted values, occurrence equality and all preexisting native fields after schema reorder/addition. Expected to preserve typed field meaning. |
| `saved_rewrites_and_verification_resume_after_reopen_without_replacement` (`tests/compaction.rs:334`) | Stops at rewrite/verification/selection, reopens, and checks exact saved records and occurrence equality. Covers unchanged availability only; F1 is not exercised. |
| `competing_representation_keeps_stale_completed_work_without_reselection` (`tests/compaction.rs:367`) | Selects a competitor after a saved rewrite, expects stale refusal and retained verified work. Expected to preserve the winner. |
| `sibling_backfill_keeps_the_shared_equivalent_source_representation` (`tests/compaction.rs:399`) | Backfills during stopped rewrite, selects the rewrite, reads the sibling and compacts from it again. Covers shared logical-source selection; it does not retire the creating version. |
| Transport integration (`tests/exports.rs:518`, `:569`) | HTTP route and CLI dispatch reach the shared service; HTTP response replays to the same result, and CLI output is verified. |
| `admitted_compaction_survives_retired_historical_result_availability` (`tests/compaction.rs:434`) | Exercises F1's concrete scenario; checks continued admitted recovery/replay and refused fresh admission. |
| `tiny_targets_refuse_bounded_descriptor_inventory_without_selecting_partial_files` (`tests/compaction.rs:509`) | Exercises F2's completed-descriptor dimension; requires early writer refusal, preserved selection and later successful retry. |
| `huge_target_rotates_bounded_footer_windows_without_limiting_total_rows` (`tests/compaction.rs:588`) | Exercises F2's open-footer dimension; requires multiple files under an unreachable target and exact complete occurrence equality. |

Missing qualification cases in the final reviewed snapshot: negative
equivalence cases that introduce missing, duplicate, altered or invented
occurrences; malformed stored origins; cleanup attempted against retained
compaction roots; process termination during compaction's acknowledged handoffs.
These gaps do not imply observed corruption. The full concurrent-writer,
delete-semantics and provider matrix remains accepted R6 work.

## Conclusion

The increment preserves typed values and source identity in the traced supported
paths, reuses existing native reader/writer and lifecycle facilities, and fixes
the material recovery/resource findings. The additional writer adapter remains
small and caps growth through maintained writer closure and retained descriptor
admission. The disk preflight remains an estimate, now backed by a free-space
check before each rewrite batch (`src/engine/compaction.rs:397`, `:429`). This
does not reserve disk against unrelated writers or establish measured peak space.

**VERDICT: APPROVE for this limited increment**

- Coverage of changed paths: **ADEQUATE** for the limited local increment after
  the added focused cases; F4/F5 remain explicit limitations and future work.
- Confidence: **HIGH** in the static evidence chains; runtime behavior and
  aggregate resource use were not measured here.
- This review does not complete R6/R7, qualify distributed execution or restore,
  or certify the full Comments throughput target.
