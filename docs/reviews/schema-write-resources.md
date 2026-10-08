# Schema-preserving writer resource review

**VERDICT: APPROVE for this writer prerequisite.** No actionable production finding remains. The serialized-buffer limit is now enforced before growth or durable output, and the corrected cancellation and paired-writer fixtures pass. The adapter preserves native field identity while reusing the existing inventory, reader and replacement writer. Its complete-path performance remains unmeasured.

This static semi-formal review covers the uncommitted changes against `c0d575c` on 2026-10-08. The reviewer applied the semi-formal code-review skill and Rust performance, universal, library and resilience guidance, inspected changed code, callers, tests and pinned dependency implementations, and ran no tests or builds. Only this report was edited. Execution evidence below belongs to the implementation owner.

## Findings

### SW-R1 — Candidate serialization exceeded its allowance before refusal — CLOSED

- **Severity/category:** WARNING / resource correctness.
- **Original location:** `src/storage/tables.rs::register_candidate`, formerly the complete `serde_json::to_vec` call followed by a length check.
- **Scenario and evidence:** An over-budget schema, property or retained history allocated its entire encoded representation before returning `metadata_capacity`. Reserving twice the metadata allowance did not bound that buffer.
- **Resolution:** `bounded_json` (`src/storage/tables.rs:107`) streams the maintained serializer into `BoundedJson`. Its `Write::write` implementation (`:130`) checks addition and the byte ceiling before extending the vector. Requested geometric growth stops at that ceiling. Capacity refusal remains typed; other serialization errors retain their cause. Registration calls `write_new` only after successful bounded encoding (`:98`).
- **Qualification:** The exact-limit, one-byte-short, zero-limit and repeated small-write assertions at `:167` pass. The native candidate fixture at `src/engine/write/tests.rs:603` supplies oversized field documentation, requires `metadata_capacity`, and confirms that the output directory was never created.

### SW-R2 — Cancellation fixture could not admit its blocking worker — CLOSED

- **Severity/category:** WARNING / qualification fixture.
- **Evidence:** The initial test allocated the entire wrapper pool to candidate metadata, leaving no space for `Budget::blocking`'s existing one-MiB reservation (`src/budget.rs:114`). Its entry signal therefore failed before exercising cancellation. The failure is retained in `schema-write-check-failed.log`.
- **Resolution:** The fixture now supplies exactly the candidate allowance plus the worker allowance in the wrapper half of the budget (`src/storage/tables.rs:213`). No production allowance was relaxed.
- **Qualification:** The current-thread test at `:204` blocks the actual preparation closure, aborts its caller, requires further admission to fail, releases the worker, then reacquires the entire wrapper allowance. It verifies a readable durable metadata file and absence of catalog registration after caller cancellation. The corrected test passes in `schema-write-check-update-failed.log`.

### SW-R3 — Existing update fixture no longer proved writer-time refusal — CLOSED

- **Severity/category:** WARNING / qualification fixture.
- **Evidence:** `schema-write-update-diagnostic.log` records processing and validation finishing before the old fixture reached a later readback limit. Merely accepting any `oversized_batch` message would have lost the intended paired-writer recovery test.
- **Resolution:** `tests/updates.rs:435` now uses an affected 64 KiB value with duplicate and null keys. Initial creation uses the normal allowance; the update retains its 16 KiB allowance. Key discovery fits, while the complete affected-file read necessarily refuses inside the paired writers.
- **Qualification:** The fixture still requires a native-reader refusal, completion within its timeout, unchanged selection, reusable admission, incomplete processing, and successful resume after deleting both caller input files (`:474`). The focused `schema-write-update-wide.log` and complete final updates suite pass. Production checks were unchanged.

### SW-R4 — Native allocation and scan-cost limits remain explicit — ACCEPTED LIMITATION

- **Severity/category:** OBSERVATION / performance qualification.
- The finite encoded-output check does not bound all native metadata allocations. Iceberg 0.10.1's `TableMetadata::serialize` clones its metadata before serializing (`src/spec/table_metadata.rs:894`), and the SQL catalog reparses the retained file during registration (`src/catalog.rs:944`). The shared permit governs admission, not an exact process-memory ceiling.
- The file provider explicitly selects key field IDs, but maintained `StreamingTable::scan` ignores filters and has no equivalent raw-Parquet statistics interface (`datafusion-catalog-53.1.0/src/streaming.rs:96`). Key-column skipping is tested; predicate/statistics pushdown and equivalent join planning are not claimed.
- When all requested key IDs are absent, Iceberg's existing fallback can decode all physical columns before producing null values. The previous schema-reader qualification and `docs/STATUS.md:680` describe this temporary-allocation and I/O limit. No replacement decoder or index is introduced here.
- Hashing and independent verification still cover more than the changed rows. File reuse does not establish update-size-proportional latency. Additional provider machinery should follow measured need.

## Patch summary and function trace

Fresh native output metadata now retains supplied field IDs. Replacement candidates additionally preserve schema history and the highest allocated ID while clearing old selected contents. Selected-file reads use the same maintained native interpretation as ordinary reads, replacing raw historical Parquet-name interpretation. Existing lifecycle authority and completion rules are unchanged.

| Function or caller | Location | Inputs → outputs and verified behavior |
| --- | --- | --- |
| `create_output` / `replacement_output` / `register_output` | `src/engine/write.rs:36`, `:51`, `:66` | Dataset namespace plus schema or exact source table → native candidate. Fresh and replacement paths share registration; source table metadata is shared until preparation clones it inside the admitted worker. |
| `fresh_metadata` | `src/storage/tables.rs:11` | Supplied schema → empty v2 metadata. Reinstalls actual IDs after the native constructor's reassignment, then removes the unused provisional schema before files exist. This preserves field IDs, not an arbitrary supplied schema-version ID. |
| `replacement_metadata` | `src/storage/tables.rs:37` | Exact base metadata → empty independent candidate. Refuses unsupported layout, changes UUID/location, clears `main`, snapshots and both statistics kinds, removes writer-location overrides and keeps destructive cleanup disabled. Schema history and `last_column_id` survive. |
| `register_candidate` / `bounded_json` | `src/storage/tables.rs:81`, `:107` | Preparation closure plus shared budget → durably written and catalog-registered table. A shared permit spans worker and catalog stages; serialization checks precede durable output. |
| `Budget::blocking` / `write_new` | `src/budget.rs:114`; `src/storage.rs:271` | Worker admission → synchronous operation; bytes → create-new file and fsync. The worker owns its job and buffer permits even after caller cancellation. |
| `replace_keys` | `src/engine/write.rs:282` | Verified base, complete key scope and replacement stream → candidate plus actual removed/added tables. Verifies retained objects and inventory, prepares keys once, probes files serially, saves affected-file plans and shares unaffected descriptors. |
| `OccurrenceScan::values` / shared capture | `src/storage/occurrences.rs:234`, `:250` | Already checked `SnapshotFiles` → one captured task map. Moves existing inventory admission, checks complete native task membership and bounds retained paths/ID vectors; does not load a second inventory. |
| `Occurrences::capture` / `next` | `src/storage/occurrences.rs:94`, `:133` | Native tasks → serial admitted batches. Uses native concurrency one and configured batch rows; validates the transient native column and decoded row counts. Value-only mode skips per-row locator allocation. |
| `OccurrenceScan::read_projected_file` | `src/storage/occurrences.rs:306` | Captured path and optional root indices → owning file stream. Maps indices to stable native IDs, retains the transient file sentinel, and owns batch and shared metadata permits. Located scans refuse value-only index changes. |
| `query::file` / `NativePartition::execute` | `src/query.rs:49`, `:107` | Shared scan/path/optional columns → lazy DataFusion provider. Provider construction stores metadata only; execution obtains a fresh admitted native stream. Whole-table and occurrence variants retain their existing readers. |
| Paired rewrite and added-row output | `src/engine/write.rs:413`, `:442`, `:446` | Affected-file plans execute serially; one joined stream supplies kept/removed writers through the existing capacity-one channel. Replacement rows execute once. No second per-file rewrite or producer task is added. |
| Shared replacement callers | `src/engine/write.rs:245`; `src/engine/incremental.rs:236`, `:444`; `src/engine/aggregation.rs:259`, `:331` | Source updates, incremental joins, summaries and evidence continue using this same primitive. |
| Computed output caller | `src/engine/backfill/preparation.rs:41`; `src/engine/backfill/expressions.rs:91` | Existing inferred sequential native schemas retain their explicit logical-to-native mapping. The comment now distinguishes inference from ID-preserving replacement. |

Pinned implementation references resolve under the local Cargo registry. Besides the serializer and catalog, the reviewer inspected Iceberg's metadata builder and `DefaultLocationGenerator`. The builder preserves the highest allocated field ID; removing snapshots alone does not clear `current_snapshot_id`, which explains the explicit `main` removal. The location generator consults exactly the two writer-location overrides removed by this patch. Existing metadata-log entries are deliberately retained; no custom metadata parser or second schema authority is added.

## Data flow and invariants

1. **Exact native identity governs writes.** Candidate, removed and added tables all derive from the same exact source metadata (`write.rs:396`). The native builder has no public direct setter for the allocation high-water mark; cloning and clearing its selected contents preserves retired IDs without synthetic fields or JSON surgery. Unchanged files enter through shared descriptors and inherited object references, not through inherited snapshots.
2. **Discovery and rewriting use one interpretation.** `replace_keys` checks object identity and the native inventory before transferring it (`write.rs:293`). The shared scan captures current-schema tasks once. Key probes read selected IDs; affected-file plans read complete current values from those same tasks. Same-name/new-ID fields therefore remain absent in old files instead of reading retired values by name.
3. **Reservations follow active work.** The metadata worker owns an Arc clone of the candidate permit; the caller retains its clone through catalog registration (`tables.rs:87`). Per-file streams own batch admission and an Arc to inventory admission (`occurrences.rs:346`). Dropping the scan or provider cannot revoke permits from an active stream. Native planner quiescence and every upstream decoder allocation remain outside this wrapper guarantee.
4. **Idle provider entries do not accumulate.** Each file's context registration is removed after probing or capturing its DataFrame (`write.rs:382`). Affected plans retain the provider they need. All views share the single task inventory; payload decoding remains lazy and serial. Retained plans/descriptors still scale with the finite file inventory, without a claimed exact memory bound for DataFusion plan objects.
5. **Failure does not fabricate completed output.** Bounded serialization refuses before creating its metadata object. Cancellation during durable preparation may leave an unregistered file in the reserved attempt, but cannot release its worker allowance early or select a result. Writer/read errors still propagate through the existing phase recovery path. The change does not add selection, retention or cleanup behavior.
6. **Maintenance history has a cost.** Native schema history and metadata-log entries remain in candidate metadata and count toward encoded capacity. Maintained snapshot removal uses slice membership checks (`iceberg-0.10.1/src/spec/table_metadata_builder.rs:487`); arbitrary snapshot-heavy metadata has no new CPU-cost qualification. The implementation avoids inventing another history representation to conceal this limit.

## Test behavior and execution evidence

All execution was performed by the owner. The reviewer inspected the following assertions and retained outputs.

| Test / location | Assertion and expected trace | Evidence / disposition |
| --- | --- | --- |
| `renamed_reordered_replacements_preserve_ids_highwater_rows_and_shared_files`, `src/engine/write/tests.rs:253` | Native source with sparse/nested IDs, rename/reorder and nullable addition; compares complete candidate and actual change multisets with a Rust oracle, including duplicate/null/empty values. Checks unchanged hash/mtime and inherited ownership; native schema addition after replacement allocates ID 901 beyond retired ID 900. | Pass in the library suite; direct identity and physical-reuse qualification. |
| `empty_candidate_clears_snapshot_state_but_preserves_native_schema_history`, `:389` | New UUID; same schema/high-water mark; no snapshots, history, audit ref or statistics; retained metadata log and disabled destructive cleanup; source remains unchanged. | Pass; validates the native builder's actual cleanup rather than inferring it from method names. |
| `value_only_key_probes_skip_wide_fields_and_keep_reader_admission`, `:451` | Ordinary field uses an occurrence-reserved name. Key-only provider fits a 2 KiB allowance while full values refuse. A live reader excludes another and releases capacity on drop. Corrupting only an unselected wide payload still permits exact key decoding and breaks full decoding. | Pass; proves column skipping and resource custody, without a throughput claim. |
| `reused_key_name_with_new_id_matches_absence_and_empty_noop_results`, `:636` | Reused key name gets ID 70 while old files carry ID 10. Non-null insertion shares old files, null-key deletion removes their exact rows, no-op preserves the exact pin, and delete-all keeps schema/high-water state. | Pass; covers selected-key interpretation where raw names would be wrong, plus empty/no-op paths. |
| `metadata_serialization_stops_at_exact_byte_limit`, `src/storage/tables.rs:167`; `oversized_candidate_metadata_refuses_before_writing_any_object`, `src/engine/write/tests.rs:603` | Exact JSON bytes succeed; insufficient allowance refuses before append/write; repeated token growth stays within the requested cap; oversized native metadata creates no candidate directory. | Pass; closes SW-R1. |
| `canceled_metadata_worker_keeps_admission_until_durable_write_finishes`, `src/storage/tables.rs:204` | Abort the caller while the actual worker is stalled; capacity remains held until it finishes; durable output is readable and catalog entry absent. | Corrected fixture passes; closes SW-R2. Catalog-stage permit lifetime is additionally supported by static ownership tracing, not a separate paused-catalog experiment. |
| Existing occurrence lifetime tests | `src/storage/occurrences/tests.rs:157`, `:219` | Partial read, EOF, error and drop release permits correctly; selected file keeps admission and reads after its scan handle is dropped and another file is removed. | Pass in the regression library suite; the value-only branch uses the same stream ownership. |
| `paired_writer_refusal_retains_admission_and_can_resume`, `tests/updates.rs:435` | Deterministic oversized affected value refuses during processing; selection stays unchanged and retained admission supports resume without original inputs. | Final focused and complete updates suite pass; closes SW-R3. |

Execution chronology is material here. `schema-write-check-update-failed.log` contains a full `scripts/check` run that passed formatting, all-target/all-feature Clippy, library/CLI tests and every integration suite preceding updates, including the corrected metadata cancellation test. Its updates failure was the obsolete fixture described in SW-R3. No production source changed afterward. The final `schema-write-check.log` records formatting, all-target/all-feature Clippy with warnings denied, the complete corrected updates suite and doc tests, with exit status zero. Together those runs cover the repository checks; a second complete invocation was not performed. Initial compile and fixture failures remain retained. This review does not claim a no-default-features run, benchmark, process-memory ceiling or remote-provider qualification.

## Conclusion

**VERDICT: APPROVE.** The patch achieves its stated native writer prerequisite: stable field identity, schema-aware file replacement, shared reader admission and bounded metadata output. It removes raw historical name interpretation and duplicate inventory ownership while retaining maintained execution components. Coverage of the scoped changed paths is **ADEQUATE**; confidence is **HIGH** for those paths. Unsupported-layout refusal is statically traced, not an exhaustive native-layout test matrix.

Public schema operations, ID-bound computed dependencies, durable absence semantics and guarded cutover remain unfinished, as stated in `docs/STATUS.md:687`. The tested column skipping and shared files justify those specific implementation claims; broader performance improvements require controlled measurements.
