# Schema-preserving writer: consistency review

**Static verdict: APPROVE for this writer prerequisite.** No unresolved
field-identity, value-replacement, change-membership or selection defect was
found. The requested same-name/new-ID discovery regression is present and passed
in the inspected library-test evidence.

Reviewed against `c0d575c` on 2026-10-08 using the semi-formal code-review and
Rust skills. This reviewer inspected the changed source, its callers, maintained
Iceberg/DataFusion implementations, test assertions and owner-generated logs.
The reviewer did not execute tests or change implementation code.

## Findings and boundaries

- **Closed test gap — key discovery must use native identity.** The original
  rename/reorder fixture did not falsify a key name being reused with a different
  field ID. The added test at `src/engine/write/tests.rs:636` assigns that name
  ID 70 while old files still contain ID 10. It requires all old keys to read as
  absent, shares those files when inserting a non-null key, removes their exact
  rows through a null-key deletion, and checks no-op and empty-result pins.
  This exercises the new selected-key provider, not just the prior full reader.
- **Closed qualification gap — a capacity error alone did not prove paired-writer
  failure.** The original recovery fixture reached readback and then refused a
  decoded batch, so merely accepting another error message would weaken its
  claimed boundary. The revised fixture at `tests/updates.rs:435` supplies a
  64 KiB affected value under the unchanged 16 KiB update allowance. It requires
  the full native read to refuse, processing to remain incomplete, admission to
  remain reusable and retry to reconstruct the exact result after deleting the
  original input files. The focused owner run passed in
  `schema-write-update-wide.log`; no production limit was relaxed.
- **Observation — historical metadata is not candidate selection.** The native
  fork deliberately retains schema history, the highest allocated ID and existing
  metadata-log entries (`src/storage/tables.rs:55`). It clears snapshots,
  references and both statistics kinds. Native builders offer no metadata-log
  removal operation; the log does not supply the candidate's selected snapshot
  or current read dependencies. Its retention is explicit in `docs/STATUS.md`.
- **Scope — this is an R3 prerequisite.** Public schema operations, ID-bound
  computed dependencies, durable absence/default semantics and guarded schema
  cutover remain unfinished. Native schema-preserving writes do not establish
  those lifecycle behaviors. The status document states this limitation.

## Patch summary and function trace

The patch uses maintained metadata builders and registration to preserve field
identity. Replacements fork exact schema history instead of creating a new table
through the ID-reassigning native creation API. One captured native inventory now
serves selected-key discovery and complete-value rewrites. The existing shared
writer still produces the candidate and actual removed/added tables.

| Function | Location | Trace and result |
| --- | --- | --- |
| `fresh_metadata` | `src/storage/tables.rs:11` | Builds native metadata, reinstalls the supplied field IDs, then removes only the unused provisional schema before files exist. Normal inferred sequential schemas retain their previous interpretation. |
| `replacement_metadata` | `src/storage/tables.rs:37` | Clones exact base metadata, refuses unsupported layouts, assigns a new UUID/location, removes `main` and all snapshots, clears statistics and escaping writer paths, and disables destructive native cleanup. It retains schema IDs and the allocation high-water mark. |
| `register_candidate` / `bounded_json` | `src/storage/tables.rs:81`, `:107` | Reserves shared capacity; prepares and size-bounds native JSON in the blocking worker; writes with the existing create-new/fsync helper; registers that immutable metadata through the catalog. Cancellation retains worker admission. |
| `create_output` / `replacement_output` | `src/engine/write.rs:36`, `:51` | Resolve the dataset's native namespace and route fresh/forked metadata through the same registration helper. Production callers already reserve a protected attempt directory before writing. |
| `replace_keys` | `src/engine/write.rs:282` | Verifies every base object and its inventory, checks complete replacement schema, prepares key scope once, probes captured files, rewrites affected files and shares the rest. All three result tables fork the same exact base. |
| `query::file` / `NativePartition::execute` | `src/query.rs:49`, `:107` | Capture a file, selected root indices and shared scan; advertise the matching Arrow schema; open the same captured native task when executed. Existing whole-table and occurrence providers remain on their shared readers. |
| `OccurrenceScan::values` / `capture` | `src/storage/occurrences.rs:234`, `:250` | Transfer the already admitted inventory into a value-only scan; verify native task membership and shape once and retain its admission. No second inventory or raw historical Parquet name interpretation is introduced. |
| `read_projected_file` / `Occurrences::next` | `src/storage/occurrences.rs:306`, `:133` | Convert selected root indices to exact native IDs, retain the transient native file column required for ID-aware mapping, strip it before returning values and enforce decoded row counts. Value-only mode adds no occurrence columns. |
| Shared callers and verification | `src/engine/changes.rs:84`; `src/engine/incremental.rs:236`, `:444`; `src/engine/aggregation.rs:259`, `:331` | Source updates, incremental joins, summaries and their evidence use the same replacement primitive. Independent change verification still requires exact native schemas and the removed/added multiset equation. |

Small accompanying changes remove an unused creation import and correct the
computed-field comment: inferred outputs use sequential native IDs, whereas
replacement outputs now preserve their existing IDs. No lifecycle authority,
operation fingerprint, completion key or selection rule changes in this patch.

## Data flow and invariants

1. **Native field identity survives writes.** All candidate, removed and added
   tables derive from the same base metadata (`write.rs:396-404`). Native
   `add_current_schema` retains supplied field IDs; the maintained builder's
   schema removal does not reduce `last_column_id`. Forking preserves the exact
   schema ID needed by existing independent change verification. Fresh generic
   outputs guarantee supplied field IDs, not an arbitrary caller's schema ID.
2. **New metadata cannot select old candidate contents.** Clearing `main` is
   necessary because native snapshot removal alone does not clear the current
   snapshot ID. The explicit removal followed by snapshot/statistics cleanup
   leaves an empty candidate (`tables.rs:60-74`). Native writer location
   overrides are removed so new files remain in the protected attempt. Old data
   enters only through explicitly shared descriptors and inherited object refs.
3. **Discovery and rewriting interpret the same exact files.** Base hashing
   precedes decoding (`write.rs:293`); inventory validation precedes transfer.
   Key probes choose field IDs from each retained task's exact current schema
   (`occurrences.rs:324-341`). Affected-file plans use the full schema from that
   same scan. The maintained transformer builds output in requested-ID order;
   the transient file column preserves its ID-aware branch. No newer catalog
   head is resolved during either read.
4. **Actual writes remain the change authority.** The existing single joined
   stream feeds kept and removed writers (`write.rs:411-445`); replacement rows
   become added files. Counts come from completed native file descriptors. The
   zero-removal/zero-addition branch returns the exact old pin (`:493`), retaining
   computed-group reuse. Candidate pinning checks shared ownership; historical
   files and the old table remain unchanged.
5. **Resource cleanup preserves lazy execution.** Deregistering a processed
   per-file table (`write.rs:382-384`) does not invalidate a saved DataFrame:
   DataFusion's logical `TableScan` owns its `Arc<dyn TableSource>`. Each later
   execution owns a fresh native stream and its permits. Capacity errors remain
   fallible, including while the paired writers are active. This removes idle
   context entries without adding an execution framework.

## Inspected tests and execution evidence

The assertions below were read statically; execution evidence comes from the
owner's retained logs.

| Test | Location | Falsification covered |
| --- | --- | --- |
| Rename/reorder replacement | `src/engine/write/tests.rs:253` | Independent complete Rust values and removed/added membership; duplicates and nulls; sparse/nested IDs; exact schema equality; unchanged file digest and modification time. A retired ID 900 is absent from retained schemas yet the next native allocation is 901. |
| Empty native fork | `src/engine/write/tests.rs:389` | Fresh UUID, exact schema history/high-water, no snapshots/reference/statistics, preserved metadata log, disabled cleanup and unchanged source metadata. |
| Key-only provider/admission | `src/engine/write/tests.rs:451` | Requested keys fit where full values refuse; an ordinary locator-like source name remains usable; simultaneous readers retain capacity until drop; corrupting an unselected payload page leaves keys readable and makes the full read fail. |
| Metadata output limit | `src/engine/write/tests.rs:603`; `src/storage/tables.rs:167` | Oversized metadata refuses before creating its directory; exact JSON limit succeeds and the next byte refuses without growing past the declared allowance. |
| Reused name/new ID | `src/engine/write/tests.rs:636` | The closed identity test gap described above, including exact source schema/high-water on empty data and change tables. |
| Canceled metadata worker | `src/storage/tables.rs:204` | An actually paused blocking preparation retains all admission after caller cancellation, finishes a readable immutable metadata object and does not register a canceled candidate. |
| Paired-writer failure/resume | `tests/updates.rs:435` | An affected value larger than the unchanged update allowance fails in the full native read before the downstream joined-batch check. The fixture retains its timeout, structured error, old selected result, pending processing, reusable admission, deleted-input retry and exact reconstruction assertions. |

`schema-write-native.log` and `schema-write-unit.log` record successful targeted
qualification, including the new name-reuse case. The retained compile-failure
logs concern earlier fixture compilation. `schema-write-updates-failed.log` and
`schema-write-check-update-failed.log` preserve the old error-text expectations;
`schema-write-update-diagnostic.log` establishes that the original fixture reached
readback. `schema-write-update-wide.log` records the successful focused run with
the deterministic affected-value fixture. `schema-write-check-failed.log`
preserves an under-budgeted cancellation fixture that failed before entering its
worker. The final fixture includes the existing blocking-worker allowance.

`schema-write-check-update-failed.log` records the full owner-run `scripts/check`
attempt: formatting, all-target/all-feature Clippy, library and CLI tests, and
every integration suite before updates passed. Its only failure was the recovery
fixture described above. No production source changed after that run.

After changing only `tests/updates.rs`, the owner repeated formatting, Clippy with
warnings denied, the complete updates suite and doc tests. All passed with exit
status 0 in `schema-write-check.log`. Together these retained runs cover the
repository gate on the final source. A second full-suite run was not performed;
this review does not imply one or a new no-default-features check.

## Conclusion

**VERDICT: APPROVE, static consistency.** The implementation retains exact native
identity while reusing maintained construction, decoding and replacement paths.
The tests address the concrete schema/history and wrong-key counterexamples.
Coverage is adequate for the stated prerequisite. The full preceding run and the
successful changed-suite follow-up cover the final source as described above.
No consistency blocker remains for committing this checkpoint.

Key-only decoding and a transferred inventory remove specific unnecessary work,
but every base object is still hashed and the native all-requested-fields-absent
fallback can decode physical columns. The selected-file streaming provider also
does not preserve the prior raw Parquet provider's predicate/statistics pushdown;
the complete-path tradeoff remains unmeasured and is documented in `STATUS.md`.
This review establishes no latency, whole-process memory or complete R3 result.
